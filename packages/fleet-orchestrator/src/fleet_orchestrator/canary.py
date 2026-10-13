"""Nightly organic canary for the fleet-bus trio (fleet-bus #89).

A once-a-night liveness proof that the WHOLE production intake lane works — not a
harness frame, but one real ``[fleet]`` card driven the production way:

    create ONE ``[fleet]`` todo card  →  the live intake poller auto-opens a
    session  →  the trio coordinates  →  the card reaches ``done``.

A card reaching ``done`` within the timeout is **GREEN**: log + exit 0 SILENTLY —
no channel post, nothing in front of people on a healthy night. A card that goes
``blocked`` or never reaches ``done`` within the window is **RED**: publish exactly
ONE narrator headline to the intake channel (the same PR-#85 channel-carrying beat
path fleet-liveness uses) and exit 1. That is the goal-brief contract verbatim:
*"one ``[fleet]`` card/night, narrator headline ONLY on failure."*

Reuse (no hand-rolled HTTP or Discord — REUSE LAW):
  * :meth:`~fleet_orchestrator.kboard.KboardClient.create_task` — the create verb
    on the client that already owns the tenant param + error mapping (the proven
    graduation-harness recipe: ``t_``-prefixed id, ``status:todo``, ``[fleet]``
    title so the poller opens it).
  * :meth:`~fleet_orchestrator.kboard.KboardClient.get_task` — the poll read.
  * :class:`~fleet_orchestrator.governor.Narrator` ``narrate_task(..., channel=...)``
    — the failure headline, on the manager's inbox, carrying the intake channel so
    ``envelope_to_message`` routes it (the session-less-beat channel fix, #85).
  * :func:`~fleet_orchestrator.config._float_env` — the fail-loud env pattern for
    the one new knob (``FLEET_CANARY_TIMEOUT_SECONDS``).

The core :func:`run_canary` takes injected collaborators (kboard, optional
narrator, a monotonic clock, an async sleep) so the green / timeout / blocked paths
are each drivable deterministically under test, mirroring the injected-collaborator
style of :meth:`~fleet_orchestrator.loop.OrchestratorHub.poll_fleet_liveness_once`.
The :func:`main` entry is the only place that reads a real clock and wires the live
bus + narrator + client.

No secrets are logged — ``task_id`` / status / reason are coordination metadata.
"""

from __future__ import annotations

import argparse
import asyncio
import sys
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from uuid import uuid4

from fleet_nats import BusConfig, FleetBus
from loguru import logger

from fleet_orchestrator.config import _ENV_INTAKE_CHANNEL, _ENV_MANAGER, _float_env
from fleet_orchestrator.governor import Narrator
from fleet_orchestrator.intake_poller import intake_label_from_env
from fleet_orchestrator.kboard import KboardClient, KboardError

# The one canary-owned env knob. Read through config._float_env so a typo'd /
# non-positive value refuses to start rather than silently defaulting — the same
# fail-loud posture as every other fleet env knob.
_ENV_TIMEOUT = "FLEET_CANARY_TIMEOUT_SECONDS"
_DEFAULT_TIMEOUT_S = 900.0

# How often the canary re-reads the card while waiting for a terminal status.
# Independent of the timeout; a small fixed cadence (a night-long window polled
# every ~15s is ~60 reads at the default, negligible board load).
_ENV_POLL = "FLEET_CANARY_POLL_SECONDS"
_DEFAULT_POLL_S = 15.0

# The real, tiny task text the canary card carries. It MUST be real work (the
# goal brief: "not a harness frame") — a worker replies with a one-line status,
# which drives the todo→running→done lifecycle the canary measures.
#
# The WIRE PROTOCOL block is load-bearing, not boilerplate: the agents do not
# emit machine completion/stance signals naturally, so the card text carries the
# protocol (the same idiom every honest-completion card uses). The first live
# canary run (2026-07-09 03:32 ET, t_canaryeeec5efc83204230) omitted it — workers
# replied with bare acks, every turn counted unsigned, the relay echo exhausted
# the budget, and the card BLOCKED: a false RED from card wording, not fleet
# health.
#
# Completion and stance are TYPED turn fields, not text trailers (fleet-bus #205
# removed the legacy ``[[done]]`` / ``[[stance:...]]`` parsers). A worker signs a
# turn by making its whole reply a JSON ``Turn`` object whose named fields carry
# the signals — a literal trailer in prose is inert.
_CANARY_TASK_TEXT = (
    "Canary: nightly liveness check (report-only). Each worker: reply with ONE "
    "final message that is a JSON object (a typed turn), for example: "
    '{"visible_text": "@bilby skippy on <host>, gateway up", "stance": '
    '"agree", "done": true, "outcome": "success"}. Put your one line — your '
    "agent id, your host, and whether your Hermes gateway is up — in "
    "visible_text, and mention your peer there with an @ sigil (Skippy writes "
    "@bilby, Bilby writes @skippy). Nagatha: one synthesis turn with "
    '{"visible_text": "<synthesis>", "done": true, "outcome": "success"}. '
    "REPORT ONLY — change nothing."
)

# The narrator beat published on failure (defined in governor._TEMPLATES).
_CANARY_FAILED_BEAT = "fleet_canary_failed"
# The synthetic task id the fleet-level (session-less) beat is keyed by, matching
# the fleet-liveness beats — the beat is hub-level, not the canary card's session.
_FLEET_TASK_ID = "fleet"


class CanaryOutcome(StrEnum):
    """The terminal outcome of one canary run."""

    GREEN = "green"  # card reached done within the window — silent, exit 0
    TIMEOUT = "timeout"  # never reached done within the window — headline, exit 1
    BLOCKED = "blocked"  # card went blocked — headline, exit 1
    ERROR = "error"  # create/poll raised (board unreachable) — headline, exit 1


# Terminal (non-GREEN) outcomes that must narrate a failure headline.
_FAILURE_OUTCOMES = frozenset({CanaryOutcome.TIMEOUT, CanaryOutcome.BLOCKED, CanaryOutcome.ERROR})


@dataclass(frozen=True)
class CanaryResult:
    """The result of one canary run — the outcome and the (short) reason.

    Attributes:
        outcome: The terminal :class:`CanaryOutcome`.
        task_id: The canary card id (for the log / failure headline target).
        reason: A short, log/board-readable reason (coordination metadata only).
    """

    outcome: CanaryOutcome
    task_id: str
    reason: str

    @property
    def is_green(self) -> bool:
        """True iff the canary passed (card reached ``done`` within the window)."""
        return self.outcome is CanaryOutcome.GREEN

    @property
    def exit_code(self) -> int:
        """0 on GREEN, 1 on any failure (the process exit code)."""
        return 0 if self.is_green else 1


def _new_task_id() -> str:
    """A unique ``t_``-prefixed canary card id (the proven k-board id shape).

    ``t_canary`` + a random uuid4 hex keeps the id unmistakably the canary's and
    collision-free across reboots. A monotonic-derived stamp is reboot-unsafe —
    the monotonic clock resets on restart, so two nights could mint the same id
    and collide on the board (a false RED); uuid4 makes each run's card unique.
    """
    return f"t_canary{uuid4().hex[:16]}"


async def run_canary(
    *,
    kboard: KboardClient,
    intake_label: str,
    timeout_s: float,
    poll_s: float,
    narrator: Narrator | None,
    channel: str | None,
    monotonic: Callable[[], float],
    sleep: Callable[[float], Awaitable[None]],
) -> CanaryResult:
    """Run one nightly canary: create a ``[fleet]`` card, poll to done/blocked/timeout.

    Deterministic under test: every clock/sleep is injected. Creates ONE real
    ``[fleet]`` card, then polls its status every ``poll_s`` until it reaches
    ``done`` (GREEN), goes ``blocked`` (RED), or the ``timeout_s`` window elapses
    (RED). On any non-GREEN outcome — including a board error — it narrates exactly
    ONE failure headline via the injected ``narrator`` (when wired) and returns a
    RED result; GREEN returns silently.

    Args:
        kboard: The k-board client (create + poll).
        intake_label: The card-scope marker the poller opens on (e.g. ``[fleet]``);
            the canary title MUST start with it or the poller ignores the card.
        timeout_s: The GREEN window — how long to wait for ``done`` before RED.
        poll_s: How often to re-read the card while waiting.
        narrator: The failure narrator (``None`` in a dry run — no beat surfaced).
        channel: The intake Discord channel the failure headline carries (so the
            session-less beat routes; ``None`` preserves the unconfigured no-op).
        monotonic: Injected monotonic seconds source (the ONE clock read).
        sleep: Injected async sleep (``asyncio.sleep`` in production).

    Returns:
        The :class:`CanaryResult` (its ``exit_code`` is the process exit code).
    """
    task_id = _new_task_id()
    title = f"{intake_label} nightly canary"

    # ---- create the card (the production intake trigger) --------------------
    try:
        await kboard.create_task(task_id=task_id, title=title, body=_CANARY_TASK_TEXT)
    except KboardError as exc:
        reason = f"card create failed: {exc}"
        logger.error("canary | create failed task={} ({})", task_id, exc)
        result = CanaryResult(CanaryOutcome.ERROR, task_id, reason)
        await _narrate_failure(narrator, result, channel=channel)
        return result

    logger.info(
        "canary | card created task={} label={!r} timeout_s={} poll_s={}",
        task_id,
        intake_label,
        timeout_s,
        poll_s,
    )

    # ---- poll to a terminal status or the timeout ---------------------------
    deadline = monotonic() + timeout_s
    while True:
        try:
            task = await kboard.get_task(task_id)
        except KboardError as exc:
            # A transient read failure mid-window is not fatal on its own — but if
            # the whole window elapses in read errors, the timeout branch below
            # surfaces it. Log and keep polling until the deadline.
            logger.warning("canary | poll read failed task={} ({}) — retrying", task_id, exc)
            task = None

        if task is not None:
            if task.status == "done":
                logger.info("canary | GREEN task={} reached done", task_id)
                return CanaryResult(CanaryOutcome.GREEN, task_id, "reached done")
            if task.status == "blocked":
                reason = "card went blocked (worker offline / escalation exhausted)"
                logger.error("canary | RED task={} blocked", task_id)
                result = CanaryResult(CanaryOutcome.BLOCKED, task_id, reason)
                await _narrate_failure(narrator, result, channel=channel)
                return result

        if monotonic() >= deadline:
            reason = f"did not reach done within {int(timeout_s)}s"
            logger.error("canary | RED task={} timed out ({})", task_id, reason)
            result = CanaryResult(CanaryOutcome.TIMEOUT, task_id, reason)
            await _narrate_failure(narrator, result, channel=channel)
            return result

        await sleep(poll_s)


async def _narrate_failure(
    narrator: Narrator | None, result: CanaryResult, *, channel: str | None
) -> None:
    """Publish the ONE failure headline (no-op on GREEN or when no narrator).

    Reuses the session-less :meth:`~fleet_orchestrator.governor.Narrator.narrate_task`
    with the intake ``channel`` — the exact PR-#85 path fleet-liveness uses so the
    beat reaches Discord. ``narrate_task`` never raises (it logs a failed publish),
    so a dead bus at 4am cannot mask the non-zero exit.
    """
    if result.outcome not in _FAILURE_OUTCOMES:
        return  # GREEN — silent by contract.
    if narrator is None:
        return  # dry run — the RED result still returns, but no beat is surfaced.
    await narrator.narrate_task(
        _FLEET_TASK_ID,
        _CANARY_FAILED_BEAT,
        channel=channel,
        target=result.task_id,
        detail=result.reason,
    )


# ---- entrypoint: wire the real bus + narrator + client ----------------------


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="python -m fleet_orchestrator.canary",
        description="Run one nightly organic canary against the live fleet-bus intake lane.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Create + poll the card but wire NO narrator (no Discord beat on failure).",
    )
    return parser.parse_args(argv)


async def _amain(args: argparse.Namespace) -> int:
    """Wire the live collaborators, run one canary, return its exit code.

    Fail-loud config (env) is read here; a missing manager id or typo'd timeout
    refuses to start (ValueError → exit 2 in :func:`main`).
    """
    import os

    manager = os.environ.get(_ENV_MANAGER, "").strip()
    if not manager:
        raise ValueError(f"{_ENV_MANAGER} is required (the manager agent id, e.g. 'nagatha')")
    intake_label = intake_label_from_env()
    if not intake_label:
        raise ValueError(
            "FLEET_INTAKE_LABEL is required (the card-scope marker the poller opens on, "
            "e.g. '[fleet]'); the canary card must carry it or the intake poller ignores it."
        )
    channel = os.environ.get(_ENV_INTAKE_CHANNEL, "").strip() or None
    if channel is None and not args.dry_run:
        # A live (narrating) run with no channel fail-OPENS: the RED headline would
        # be published with channel=None and never route to Discord (the #85
        # channel-carry path needs it), so a real failure would go unseen. Refuse
        # to start — same fail-loud posture as the FLEET_MANAGER_ID guard above.
        # (--dry-run wires no narrator, so an unset channel is harmless there.)
        raise ValueError(
            f"{_ENV_INTAKE_CHANNEL} is required (the Discord channel the failure "
            "headline carries; without it a RED canary would narrate to nobody). "
            "Use --dry-run to create + poll without narrating."
        )
    timeout_s = _float_env(_ENV_TIMEOUT, _DEFAULT_TIMEOUT_S)
    poll_s = _float_env(_ENV_POLL, _DEFAULT_POLL_S)

    bus_config = BusConfig.from_env()
    bus = FleetBus(bus_config)
    await bus.connect()
    kboard = KboardClient()
    try:
        narrator = (
            None
            if args.dry_run
            else Narrator(
                bus,
                env=bus_config.env,
                manager=manager,
                new_id=lambda: uuid4().hex,
                now=lambda: datetime.now(UTC).isoformat(),
            )
        )
        result = await run_canary(
            kboard=kboard,
            intake_label=intake_label,
            timeout_s=timeout_s,
            poll_s=poll_s,
            narrator=narrator,
            channel=channel,
            monotonic=time.monotonic,
            sleep=asyncio.sleep,
        )
    finally:
        await bus.close()
        await kboard.close()

    logger.info(
        "canary | done outcome={} task={} exit={}",
        result.outcome.value,
        result.task_id,
        result.exit_code,
    )
    return result.exit_code


def main(argv: list[str] | None = None) -> int:
    """Sync entrypoint (``python -m fleet_orchestrator.canary`` / console script).

    Exit codes: 0 GREEN, 1 canary failure (timeout/blocked/error), 2 config error.
    """
    args = _parse_args(argv)
    try:
        return asyncio.run(_amain(args))
    except ValueError as exc:
        logger.error("canary | config error: {}", exc)
        return 2


if __name__ == "__main__":
    sys.exit(main())
