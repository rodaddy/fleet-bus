"""``python -m fleet_orchestrator`` — the Phase-9 orchestrator hub entrypoint.

Runs the manager's coordination run-loop (:class:`~fleet_orchestrator.loop.OrchestratorHub`)
as a long-lived service on fleet-hub (fleet-hub): connect to NATS with a recognizable
connection name, load the subject catalog once (fail-open), subscribe the intake +
heartbeat lanes, and drive coordination until SIGINT/SIGTERM.

Modes:

* default   — the durable service; serves until signalled.
* ``--once`` — start ONE session (from ``--task-id``/``--channel``) and exit when it
  reaches DONE (a bounded shutdown timeout guards against a worker that never
  finishes). For live smoke-testing the loop against real workers. The dispatched
  prompt comes from ``--task-text`` when given, else the card's title+body fetched
  from k-board by ``--task-id`` (fleet-bus #52 — a dispatch must carry real text).
* ``--dry-run`` — coordinate with NO narrator wired, so no beats reach the
  manager's Discord inbox. Combine with ``--once`` to prove a single session
  end-to-end without touching Discord.

Configuration is entirely environment-driven and validated fail-loud at startup
(:meth:`~fleet_orchestrator.config.HubConfig.from_env`) — the service refuses to
start on a missing/typo'd value rather than discovering it at traffic time.
"""

from __future__ import annotations

import argparse
import asyncio
import os
import signal
import sys
import time
from dataclasses import replace
from datetime import UTC, datetime
from uuid import uuid4

from loguru import logger

from fleet_orchestrator import trace
from fleet_orchestrator.behaviors import IdClock
from fleet_orchestrator.config import HubConfig
from fleet_orchestrator.intake import card_text
from fleet_orchestrator.kboard import KboardClient
from fleet_orchestrator.loop import IntakeTask, build_hub

# The connection name pattern the NATS /connz probe can grep for this process.
# BusConfig.name defaults to fleet-nats-<agent>; the hub is a distinct process
# from the manager's subscriber, so it carries its own recognizable name.
_HUB_CONNECTION_NAME = "fleet-orchestrator-hub"
# A --once session that never completes must not hang the smoke test forever.
_ONCE_TIMEOUT_S = 120.0
# How often --once polls the session state for DONE.
_ONCE_POLL_S = 0.5


def _real_clock() -> IdClock:
    """The production id/timestamp source (uuid4 + UTC ISO)."""
    return IdClock(new_id=lambda: uuid4().hex, now=lambda: datetime.now(UTC).isoformat())


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="python -m fleet_orchestrator",
        description="The Phase-9 orchestrator coordination hub.",
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Start one session and exit when it reaches DONE (smoke test).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Wire no narrator — coordinate without posting beats to Discord.",
    )
    parser.add_argument(
        "--task-id",
        default=None,
        help="Task id for --once (required with --once).",
    )
    parser.add_argument(
        "--channel",
        default=None,
        help="Discord channel slug for the --once session (optional).",
    )
    parser.add_argument(
        "--task-text",
        default=None,
        help=(
            "Explicit task prompt for --once. When omitted, the card's title+body "
            "is fetched from k-board by --task-id. Overrides the fetched text."
        ),
    )
    return parser.parse_args(argv)


async def _run_service(config: HubConfig, *, dry_run: bool) -> None:
    """The durable path: serve until SIGINT/SIGTERM."""
    hub, bus, kboard = await build_hub(
        config, clock=_real_clock(), monotonic=time.monotonic, dry_run=dry_run
    )
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)
    try:
        await hub.start()
        logger.info("orchestrator.hub | serving (Ctrl-C / SIGTERM to stop)")
        await stop.wait()
    finally:
        hub.stop()
        await hub.aclose()
        # Drain any batching trace sink (Langfuse queues spans on a background
        # thread) BEFORE the bus closes, so the tail of the run is not lost on
        # SIGTERM. No-op when tracing is off or the sink is eager (JSONL).
        trace.flush()
        await bus.close()
        await kboard.close()
    logger.info("orchestrator.hub | service stopped")


async def _resolve_once_text(kboard: KboardClient, *, task_id: str, task_text: str | None) -> str:
    """Resolve the --once prompt: explicit override, else fetch the card by id.

    An explicit ``--task-text`` wins unconditionally (an operator smoke-testing a
    synthetic prompt should not need a real card). Otherwise the card's title+body
    is fetched from k-board and assembled via :func:`card_text`. A fetch failure is
    logged loud and degrades to ``""`` (the hub WARNs at dispatch) rather than
    aborting — --once must exercise the path even against a degraded board.
    """
    if task_text is not None:
        logger.info("orchestrator.hub | --once using explicit --task-text len={}", len(task_text))
        return task_text
    try:
        task = await kboard.get_task(task_id)
    except Exception as exc:  # noqa: BLE001 — fetch degrades loud, never aborts the smoke run
        logger.error(
            "orchestrator.hub | --once could not fetch card task={} ({}); dispatching empty text",
            task_id,
            exc,
        )
        return ""
    text = card_text(task.title, task.body)
    logger.info("orchestrator.hub | --once fetched card text task={} len={}", task_id, len(text))
    return text


async def _run_once(
    config: HubConfig,
    *,
    task_id: str,
    channel: str | None,
    task_text: str | None,
    dry_run: bool,
) -> int:
    """The --once path: start one session, wait for DONE (bounded), then exit.

    The dispatched turn must carry a real prompt or the worker subscriber drops it
    (fleet-bus #52), so --once resolves the task text before dispatch: an explicit
    ``task_text`` wins; otherwise the card's title+body is fetched from k-board by
    ``task_id``. A fetch failure is logged loud and falls through to an empty prompt
    (the hub then WARNs at dispatch) rather than aborting the smoke run — the point
    of --once is to exercise the path even when the board is degraded.

    Returns:
        Process exit code — 0 if the session reached DONE within the timeout,
        1 if it timed out (the worker never completed).
    """
    hub, bus, kboard = await build_hub(
        config, clock=_real_clock(), monotonic=time.monotonic, dry_run=dry_run
    )
    try:
        await hub.start()
        text = await _resolve_once_text(kboard, task_id=task_id, task_text=task_text)
        intake = IntakeTask(task_id=task_id, channel=channel, workers=config.workers, text=text)
        await hub.start_task(intake)
        logger.info("orchestrator.hub | --once started task={} waiting for DONE", task_id)
        deadline = time.monotonic() + _ONCE_TIMEOUT_S
        while time.monotonic() < deadline:
            # The task is dropped from tracking only when its terminal DONE state
            # is durably persisted — the authoritative completion signal (a cell
            # state read could be advanced by a foreign done the hub ignored).
            if not hub.is_tracking(task_id):
                logger.info("orchestrator.hub | --once task={} reached DONE", task_id)
                return 0
            await asyncio.sleep(_ONCE_POLL_S)
        logger.error(
            "orchestrator.hub | --once task={} did not complete within {}s",
            task_id,
            _ONCE_TIMEOUT_S,
        )
        return 1
    finally:
        hub.stop()
        await hub.aclose()
        # Drain the batching trace sink before the bus closes (see _run_service).
        trace.flush()
        await bus.close()
        await kboard.close()


async def _amain(args: argparse.Namespace) -> int:
    """Validate config fail-loud, then dispatch to the service or --once path."""
    # --once drives ONE explicit task and never runs the intake poller (see below),
    # so its posture is poller-off. Disable the poller in the environment BEFORE
    # HubConfig.from_env reads it, so the fleet-bus #54 fail-closed guard (an
    # enabled poller REQUIRES FLEET_INTAKE_LABEL) does not trip a --once smoke run
    # that legitimately has no card-scope marker (codex review, MAJOR). The service
    # path is unaffected: it still fails closed on an enabled-but-unmarked poller.
    if args.once:
        os.environ["FLEET_INTAKE_POLL_ENABLED"] = "false"
    config = HubConfig.from_env()
    # The hub is a distinct process from the manager's subscriber, so it takes a
    # recognizable connection name for the /connz probe — but never override a
    # FLEET_NATS_NAME the operator deliberately set. from_env sets the name to
    # fleet-nats-<agent> when the var is unset; only that default gets replaced.
    if _HUB_CONNECTION_NAME and os.environ.get("FLEET_NATS_NAME") is None:
        config = replace(config, bus=replace(config.bus, name=_HUB_CONNECTION_NAME))
    if args.once:
        if not args.task_id:
            logger.error("orchestrator.hub | --once requires --task-id")
            return 2
        # --once drives ONE explicit task and exits; the intake poller must NOT
        # run in this posture or it would auto-open every other `todo` card on the
        # board and break the single-session smoke isolation (codex review, MINOR).
        # Already forced off in the environment above (so the #54 label guard does
        # not trip); re-assert on the config for defence in depth and clarity.
        once_config = replace(config, intake_poll_enabled=False)
        return await _run_once(
            once_config,
            task_id=args.task_id,
            channel=args.channel,
            task_text=args.task_text,
            dry_run=args.dry_run,
        )
    await _run_service(config, dry_run=args.dry_run)
    return 0


def main(argv: list[str] | None = None) -> int:
    """Sync entrypoint (``python -m fleet_orchestrator``)."""
    args = _parse_args(argv)
    try:
        return asyncio.run(_amain(args))
    except ValueError as exc:
        # Fail-loud config validation: name the bad value and exit non-zero.
        logger.error("orchestrator.hub | config error: {}", exc)
        return 2


if __name__ == "__main__":
    sys.exit(main())
