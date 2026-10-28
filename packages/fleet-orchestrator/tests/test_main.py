"""Entrypoint (`python -m fleet_orchestrator`) dispatch tests.

Proves the CLI wiring the hub depends on WITHOUT connecting to NATS/k-board:
``build_hub`` is never reached because ``_run_once``/``_run_service`` are patched
to capture the config they receive. The point under test is #44's follow-up
(codex review, MINOR): the ``--once`` posture must disable the intake poller so a
single-session smoke run does not auto-open every other ``todo`` card.
"""

from __future__ import annotations

from typing import Any

import pytest

from fleet_orchestrator import __main__ as entry


@pytest.fixture
def _env(monkeypatch: Any) -> None:
    monkeypatch.setenv("FLEET_MANAGER_ID", "heron")
    monkeypatch.setenv("FLEET_WORKERS", "wren,finch")
    monkeypatch.setenv("FLEET_AGENT_ID", "heron")
    monkeypatch.delenv("FLEET_NATS_NAME", raising=False)


async def test_once_disables_the_intake_poller(monkeypatch: Any, _env: None) -> None:
    # --once forces intake_poll_enabled=False regardless of the env toggle, so a
    # single-session smoke run never auto-opens unrelated todo cards.
    monkeypatch.setenv("FLEET_INTAKE_POLL_ENABLED", "true")
    captured: dict[str, Any] = {}

    async def _fake_run_once(config: Any, **kwargs: Any) -> int:
        captured["config"] = config
        captured["kwargs"] = kwargs
        return 0

    monkeypatch.setattr(entry, "_run_once", _fake_run_once)
    args = entry._parse_args(["--once", "--task-id", "t-1", "--channel", "arena"])
    rc = await entry._amain(args)

    assert rc == 0
    assert captured["config"].intake_poll_enabled is False
    assert captured["kwargs"]["task_id"] == "t-1"


async def test_once_starts_without_a_card_scope_label(monkeypatch: Any, _env: None) -> None:
    # fleet-bus #54 (codex review, MAJOR): --once disables the poller, so it must NOT
    # require FLEET_INTAKE_LABEL — a single-session smoke run with no marker set must
    # still start (the fail-closed guard is for the ENABLED service posture only).
    monkeypatch.delenv("FLEET_INTAKE_LABEL", raising=False)
    monkeypatch.delenv("FLEET_INTAKE_POLL_ENABLED", raising=False)  # poller defaults ON
    captured: dict[str, Any] = {}

    async def _fake_run_once(config: Any, **kwargs: Any) -> int:
        captured["config"] = config
        return 0

    monkeypatch.setattr(entry, "_run_once", _fake_run_once)
    args = entry._parse_args(["--once", "--task-id", "t-1"])
    rc = await entry._amain(args)

    assert rc == 0
    assert captured["config"].intake_poll_enabled is False


async def test_service_path_fails_closed_without_a_card_scope_label(
    monkeypatch: Any, _env: None
) -> None:
    # fleet-bus #54: the durable service path leaves the poller ON, so a missing
    # card-scope marker must refuse to start (never open the whole board).
    monkeypatch.delenv("FLEET_INTAKE_LABEL", raising=False)
    monkeypatch.delenv("FLEET_INTAKE_POLL_ENABLED", raising=False)  # poller defaults ON

    args = entry._parse_args([])
    with pytest.raises(ValueError, match="FLEET_INTAKE_LABEL"):
        await entry._amain(args)


async def test_once_threads_task_text_through_to_run_once(monkeypatch: Any, _env: None) -> None:
    # fleet-bus #52: --task-text must reach _run_once so --once can dispatch a real
    # prompt (else the worker gets an empty turn).
    captured: dict[str, Any] = {}

    async def _fake_run_once(config: Any, **kwargs: Any) -> int:
        captured["kwargs"] = kwargs
        return 0

    monkeypatch.setattr(entry, "_run_once", _fake_run_once)
    args = entry._parse_args(["--once", "--task-id", "t-1", "--task-text", "do the thing"])
    rc = await entry._amain(args)

    assert rc == 0
    assert captured["kwargs"]["task_text"] == "do the thing"


async def test_resolve_once_text_prefers_explicit_task_text() -> None:
    # An explicit --task-text wins unconditionally (no k-board fetch needed).
    class BoomKboard:
        async def get_task(self, task_id: str) -> Any:
            raise AssertionError("must not fetch when --task-text is given")

    text = await entry._resolve_once_text(
        BoomKboard(),  # type: ignore[arg-type]
        task_id="t-1",
        task_text="explicit prompt",
    )
    assert text == "explicit prompt"


async def test_resolve_once_text_fetches_card_title_and_body() -> None:
    # Without --task-text the card is fetched and its title+body assembled.
    from fleet_orchestrator.kboard import KboardTask

    class CardKboard:
        async def get_task(self, task_id: str) -> KboardTask:
            return KboardTask(
                task_id=task_id,
                status="todo",
                sync_version=1,
                raw={},
                title="Do it",
                body="With detail.",
            )

    text = await entry._resolve_once_text(
        CardKboard(),  # type: ignore[arg-type]
        task_id="t-1",
        task_text=None,
    )
    assert text == "Do it\n\nWith detail."


async def test_resolve_once_text_degrades_to_empty_on_fetch_failure() -> None:
    # A board outage must not abort the smoke run — it degrades to "" (the hub then
    # WARNs at dispatch), never raises.
    class RaisingKboard:
        async def get_task(self, task_id: str) -> Any:
            raise RuntimeError("kboard: 503")

    text = await entry._resolve_once_text(
        RaisingKboard(),  # type: ignore[arg-type]
        task_id="t-1",
        task_text=None,
    )
    assert text == ""


async def test_service_path_keeps_the_intake_poller_enabled(monkeypatch: Any, _env: None) -> None:
    # The durable service path leaves the poller on (default) — it is the only
    # shipped created publisher.
    monkeypatch.delenv("FLEET_INTAKE_POLL_ENABLED", raising=False)
    captured: dict[str, Any] = {}

    async def _fake_run_service(config: Any, **kwargs: Any) -> None:
        captured["config"] = config

    monkeypatch.setattr(entry, "_run_service", _fake_run_service)
    args = entry._parse_args([])
    rc = await entry._amain(args)

    assert rc == 0
    assert captured["config"].intake_poll_enabled is True


# --- shutdown flush: the trace sink drains before the bus closes -----------------
#
# The review MAJOR: LangfuseTraceSink.flush() was dead code (no caller), so
# batched tail spans are lost on SIGTERM. These tests patch build_hub with fakes
# and prove _run_once / _run_service call trace.flush() in the finally path,
# ordered BEFORE bus.close() (a flush after the transport closes is useless).


class _FakeHub:
    """A hub double: satisfies the lifecycle + --once tracking probe."""

    async def start(self) -> None: ...

    def stop(self) -> None: ...

    async def aclose(self) -> None: ...

    def is_tracking(self, task_id: str) -> bool:
        return False  # --once sees DONE immediately

    async def start_task(self, intake: Any) -> Any: ...


class _FakeBus:
    """A bus double recording when close() ran, to order-check flush-before-close."""

    def __init__(self, order: list[str]) -> None:
        self._order = order

    async def close(self) -> None:
        self._order.append("bus.close")


class _FakeKboard:
    async def close(self) -> None: ...

    async def get_task(self, task_id: str) -> Any:
        raise AssertionError("not reached: --once uses explicit task_text")


def _patch_shutdown_probes(monkeypatch: Any, order: list[str]) -> None:
    """Patch build_hub → fakes and trace.flush → order-recorder."""

    async def _fake_build_hub(config: Any, **kwargs: Any) -> Any:
        return _FakeHub(), _FakeBus(order), _FakeKboard()

    monkeypatch.setattr(entry, "build_hub", _fake_build_hub)

    def _fake_flush() -> None:
        order.append("trace.flush")

    # Patch the flush the entrypoint calls (entry imports the trace module).
    monkeypatch.setattr("fleet_orchestrator.trace.flush", _fake_flush)


async def test_run_once_flushes_trace_before_bus_close(monkeypatch: Any, _env: None) -> None:
    order: list[str] = []
    _patch_shutdown_probes(monkeypatch, order)
    from fleet_orchestrator.config import HubConfig

    monkeypatch.setenv("FLEET_INTAKE_POLL_ENABLED", "false")
    config = HubConfig.from_env()

    rc = await entry._run_once(
        config,
        task_id="t-1",
        channel="arena",
        task_text="explicit prompt",  # avoids the kboard fetch
        dry_run=True,
    )

    assert rc == 0
    # flush ran, and ran BEFORE the bus closed (a post-close flush is useless)
    assert "trace.flush" in order
    assert order.index("trace.flush") < order.index("bus.close")


async def test_run_service_flushes_trace_before_bus_close(monkeypatch: Any, _env: None) -> None:
    import asyncio
    import os
    import signal

    order: list[str] = []
    _patch_shutdown_probes(monkeypatch, order)
    from fleet_orchestrator.config import HubConfig

    monkeypatch.setenv("FLEET_INTAKE_LABEL", "[fleet]")
    config = HubConfig.from_env()

    # _run_service blocks on stop.wait() after installing SIGINT/SIGTERM handlers.
    # Deliver a real SIGINT once it is serving so it reaches the finally path
    # deterministically — exercises the actual signal-driven shutdown, not a stub.
    async def _signal_after_start() -> None:
        await asyncio.sleep(0.05)
        os.kill(os.getpid(), signal.SIGINT)

    async with asyncio.timeout(5):
        await asyncio.gather(entry._run_service(config, dry_run=True), _signal_after_start())

    assert "trace.flush" in order
    assert order.index("trace.flush") < order.index("bus.close")
