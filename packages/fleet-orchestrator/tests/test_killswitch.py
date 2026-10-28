"""Hub kill authority behavior and flooded-bus acceptance proof (#107)."""

from __future__ import annotations

import asyncio
import threading
import time
from typing import cast

import fleet_nats.killprobe as probe_module
import pytest
from fleet_nats import BusConfig, FleetKV, KVEntry, StaleRevisionError
from fleet_nats.killflag import FleetKVKillAuthority, KillAuthorityConfig
from fleet_nats.killprobe import StoreKillProbe
from fleet_subscriber.killwatch import WorkerKillWatch

import fleet_orchestrator.kill as kill_module
from fleet_orchestrator.governor import Budget, Governor
from fleet_orchestrator.killswitch import KillSwitch, kill_poll_from_env
from fleet_orchestrator.state import CoordinationSession, State
from fleet_orchestrator.transitions import Action, ActionKind


def _session(task_id: str) -> CoordinationSession:
    return CoordinationSession(
        task_id=task_id,
        channel="ops",
        manager="heron",
        workers=("wren",),
        state=State.WORKING,
    )


def test_unknown_authority_fails_closed() -> None:
    governor = Governor(Budget())
    watch = KillSwitch(governor, poll_s=0.01, probe=lambda: None)
    assert watch.check_once() is True
    assert governor.is_all_paused()
    assert watch.fail_closed


def test_authoritative_release_is_the_only_resume_edge() -> None:
    readings = iter((None, False))
    governor = Governor(Budget())
    watch = KillSwitch(governor, poll_s=0.01, probe=lambda: next(readings))
    assert watch.check_once() is True
    assert watch.check_once() is False
    assert not governor.is_all_paused()


def test_poll_config_rejects_unbounded_latency(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("FLEET_KILL_SWITCH_POLL_SECONDS", "5")
    with pytest.raises(ValueError, match="\\(0, 4\\]"):
        kill_poll_from_env()


class _KV:
    def __init__(self) -> None:
        self.value: bytes | None = None
        self.revision = 0

    async def get(self, key: str) -> KVEntry | None:
        if self.value is None:
            return None
        return KVEntry(key=key, value=self.value, revision=self.revision)

    async def create(self, key: str, value: bytes) -> int:
        if self.value is not None:
            raise StaleRevisionError("kill", key, 0)
        self.value = value
        self.revision = 1
        return self.revision

    async def update(self, key: str, value: bytes, expected_revision: int) -> int:
        if expected_revision != self.revision:
            raise StaleRevisionError("kill", key, expected_revision)
        self.value = value
        self.revision += 1
        return self.revision


class _AuthorityTransport:
    """A separately-owned control transport, never the saturated work bus."""

    opened = 0
    closed = 0

    def __init__(self, config: BusConfig) -> None:
        self.config = config

    async def connect(self) -> None:
        type(self).opened += 1

    async def close(self) -> None:
        type(self).closed += 1


def _authority() -> FleetKVKillAuthority:
    return FleetKVKillAuthority(
        cast(FleetKV, _KV()), KillAuthorityConfig(bucket="fleet-kill-authority", key="fleet")
    )


def test_flooded_bus_cannot_delay_store_authority_kill(monkeypatch: pytest.MonkeyPatch) -> None:
    """A busy hub callback backlog cannot delay gate or real worker cancellation."""
    authority = _authority()
    asyncio.run(authority.initialize_released())

    async def _open(bus: _AuthorityTransport) -> FleetKVKillAuthority:
        return authority

    _AuthorityTransport.opened = 0
    _AuthorityTransport.closed = 0
    monkeypatch.setattr(probe_module, "FleetBus", _AuthorityTransport)
    monkeypatch.setattr(probe_module, "open_kill_authority", _open)
    monkeypatch.setattr(kill_module, "FleetBus", _AuthorityTransport)
    monkeypatch.setattr(kill_module, "open_kill_authority", _open)

    hub_ready = threading.Event()
    hub_gated = threading.Event()
    worker_cancelled = threading.Event()
    stop = threading.Event()
    flood_thread = threading.Thread(
        target=lambda: asyncio.run(
            _busy_hub_and_worker(
                hub_ready=hub_ready,
                hub_gated=hub_gated,
                worker_cancelled=worker_cancelled,
                stop=stop,
            )
        ),
        name="busy-hub-worker-callbacks",
        daemon=True,
    )
    flood_thread.start()
    assert hub_ready.wait(timeout=1), "the busy hub/worker harness never began"
    try:
        started = time.monotonic()
        assert kill_module.main(["arm"]) == 0  # actual operator CAS write
        deadline = time.monotonic() + 5
        while (
            not (hub_gated.is_set() and worker_cancelled.is_set()) and time.monotonic() < deadline
        ):
            time.sleep(0.01)
        assert hub_gated.is_set(), "a queued hub callback was not gated"
        assert worker_cancelled.is_set(), "the live worker callback was not cancelled"
        assert time.monotonic() - started <= 5
        assert _AuthorityTransport.opened >= 3, (
            "operator and both enforcers need separate control links"
        )
    finally:
        stop.set()
        flood_thread.join(timeout=1)
        assert not flood_thread.is_alive(), "the busy callback harness must be cleaned up"


def test_store_bus_failure_fails_closed_through_the_real_probe(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A hung control-store connection reaches the same fail-closed hub gate."""
    import fleet_nats.killprobe as probe_module

    class _HungBus(_AuthorityTransport):
        async def connect(self) -> None:
            await asyncio.Event().wait()

    monkeypatch.setattr(probe_module, "FleetBus", _HungBus)
    probe = StoreKillProbe(BusConfig(agent_id="ops", env="dev"), timeout_s=0.02)
    governor = Governor(Budget())
    watch = KillSwitch(governor, poll_s=0.01, probe=probe.read, store_probe=probe)
    watch.start()
    try:
        assert governor.is_all_paused()
    finally:
        watch.stop()


async def _busy_hub_and_worker(
    *,
    hub_ready: threading.Event,
    hub_gated: threading.Event,
    worker_cancelled: threading.Event,
    stop: threading.Event,
) -> None:
    """Run ordinary coordination callbacks and an in-flight worker on one busy loop."""
    governor = Governor(Budget())
    config = BusConfig(agent_id="ops", env="dev")
    hub_probe = StoreKillProbe(config, timeout_s=0.05)
    worker_probe = StoreKillProbe(config, timeout_s=0.05)
    hub_watch = KillSwitch(governor, poll_s=0.01, probe=hub_probe.read, store_probe=hub_probe)
    loop = asyncio.get_running_loop()

    worker_backlog = 64
    cancelled_workers = 0

    async def _worker_callback() -> None:
        nonlocal cancelled_workers
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            cancelled_workers += 1
            if cancelled_workers == worker_backlog:
                worker_cancelled.set()
            raise

    worker_tasks = [asyncio.create_task(_worker_callback()) for _ in range(worker_backlog)]

    def _cancel_worker() -> None:
        loop.call_soon_threadsafe(_cancel_worker_backlog)

    def _cancel_worker_backlog() -> None:
        for task in worker_tasks:
            task.cancel()

    worker_watch = WorkerKillWatch(
        poll_s=0.01,
        probe=worker_probe.read,
        store_probe=worker_probe,
        on_arm=_cancel_worker,
    )

    def _hub_callback() -> None:
        """The queued coordination callback uses the real fleet-wide Governor gate."""
        action = Action(kind=ActionKind.DISPATCH, session=_session("backlog"), target="wren")
        if governor.gate(action).kind is ActionKind.NOOP:
            hub_gated.set()
        if not stop.is_set():
            loop.call_soon(_hub_callback)

    hub_watch.start()
    worker_watch.start()
    loop.call_soon(_hub_callback)
    hub_ready.set()
    try:
        await asyncio.to_thread(stop.wait)
    finally:
        hub_watch.stop()
        worker_watch.stop()
        for task in worker_tasks:
            task.cancel()
        assert all(
            isinstance(result, asyncio.CancelledError)
            for result in await asyncio.gather(*worker_tasks, return_exceptions=True)
        )
