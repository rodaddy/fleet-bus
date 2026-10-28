"""Live round-trip of the orchestrator behaviors against the real bus (fleet-hub).

Proves real transport: an orchestrator dispatch and relay actually land on the
bus and are received by a subscriber. Auto-skips when fleet-hub is unreachable so the
suite stays green off-network (mirrors ``fleet_nats/tests/test_bus_live.py``).
"""

from __future__ import annotations

import asyncio
import itertools
import socket
from urllib.parse import urlsplit
from uuid import uuid4

import pytest
from fleet_nats import BusConfig, Envelope, FleetBus, subjects

from fleet_orchestrator import CoordinationSession, Event, EventKind, IdClock, Orchestrator, State

_URL = BusConfig.from_env("pytest").url


def _server_reachable() -> bool:
    parts = urlsplit(_URL)
    host, port = parts.hostname or "", parts.port or 4222
    try:
        with socket.create_connection((host, port), timeout=2):
            return True
    except OSError:
        return False


pytestmark = pytest.mark.skipif(
    not _server_reachable(), reason="fleet NATS server (fleet-hub) not reachable"
)


def _counter_clock() -> IdClock:
    ids = (f"live-{i}" for i in itertools.count())
    return IdClock(new_id=lambda: next(ids), now=lambda: "2026-07-07T00:00:00Z")


def _run_token() -> str:
    """Unique subject suffix for live-bus isolation across concurrent runs."""
    return uuid4().hex[:8]


async def test_live_dispatch_status_relay_roundtrip() -> None:
    # Proves the REAL closed loop over live transport: manager dispatches, a worker
    # publishes a genuine agent_status with no @-mention of the peer, and the
    # orchestrator's own detector fires the relay — no fabricated HANDOFF_MISSING.
    # Unique subjects per run so parallel/re-runs don't cross-talk on the shared bus.
    run = _run_token()
    channel = f"pytest-orch-live-{run}"
    workers = (f"pytest-wren-{run}", f"pytest-finch-{run}")
    quiet_worker, relay_peer = workers
    task_id = f"live-t-{run}"

    received_dispatch: list[Envelope] = []
    received_relay: list[Envelope] = []
    got_dispatch = asyncio.Event()
    got_relay = asyncio.Event()

    async def on_dispatch(env: Envelope) -> None:
        received_dispatch.append(env)
        got_dispatch.set()

    async def on_relay(env: Envelope) -> None:
        received_relay.append(env)
        got_relay.set()

    mgr_cfg = BusConfig(agent_id=f"pytest-heron-{run}", url=_URL, env="dev")
    async with (
        FleetBus(mgr_cfg) as mgr_bus,
        FleetBus(BusConfig(agent_id="w", url=_URL)) as sub_bus,
    ):
        await sub_bus.subscribe(
            subjects.agent_dispatch("dev", quiet_worker, channel=channel), on_dispatch
        )
        # The relay is per-channel (#117), same as the dispatch above — subscribe
        # the scoped form the session's channel actually publishes on.
        await sub_bus.subscribe(subjects.agent_relay("dev", relay_peer, channel=channel), on_relay)

        orch = Orchestrator(mgr_bus, _counter_clock())
        session = CoordinationSession(
            task_id=task_id, channel=channel, manager=f"pytest-heron-{run}", workers=workers
        )
        # Manager watches worker status. Settle so every subscription (dispatch,
        # relay, and the manager's status watch) is registered on the server
        # BEFORE anything is published — otherwise an immediate publish races the
        # subscription and is silently dropped (core NATS has no backlog).
        await orch.watch_worker_status(session.advanced_to(State.WORKING))
        await asyncio.sleep(0.3)

        await orch.dispatch_task(session, Event(kind=EventKind.DISPATCH), worker=quiet_worker)

        # The worker replies on agent_status WITHOUT mentioning the peer → the
        # orchestrator must detect the missing handoff and relay.
        await sub_bus.publish(
            subjects.agent_status("dev", task_id),
            Envelope.new(
                msg_id=f"ws1-{run}",
                ts="t",
                sender=quiet_worker,
                kind="status",
                task_id=task_id,
                payload={"text": "I finished my part of the analysis."},
            ),
        )

        await asyncio.wait_for(got_dispatch.wait(), timeout=5)
        await asyncio.wait_for(got_relay.wait(), timeout=5)

    assert len(received_dispatch) == 1
    assert received_dispatch[0].kind == "dispatch"
    assert received_dispatch[0].to == quiet_worker
    assert len(received_relay) == 1
    assert received_relay[0].to == relay_peer
    # The relay carried the missed worker text the detector captured.
    assert "finished my part" in received_relay[0].payload["text"]


async def test_live_done_sentinel_advances_to_done() -> None:
    # A real kind="done" sentinel published on the task's status lane advances the
    # orchestrator to DONE over live transport (issue #6).
    from fleet_orchestrator import State, done_envelope

    run = _run_token()
    task_id = f"pytest-done-live-{run}"
    mgr_cfg = BusConfig(agent_id=f"pytest-heron-{run}", url=_URL, env="dev")
    async with (
        FleetBus(mgr_cfg) as mgr_bus,
        FleetBus(BusConfig(agent_id="worker", url=_URL)) as worker_bus,
    ):
        orch = Orchestrator(mgr_bus, _counter_clock())
        session = CoordinationSession(
            task_id=task_id,
            channel=f"pytest-done-{run}",
            manager=f"pytest-heron-{run}",
            workers=(f"w-{run}",),
        )
        _, current = await orch.watch_worker_status(session.advanced_to(State.WORKING))
        await asyncio.sleep(0.3)

        await worker_bus.publish(
            subjects.agent_status("dev", task_id),
            done_envelope(
                msg_id=f"dn1-{run}",
                ts="t",
                sender=f"w-{run}",
                task_id=task_id,
                outcome="completed",
            ),
        )
        # Poll until the sentinel is processed (or fail the deadline).
        for _ in range(50):
            if current().state is State.DONE:
                break
            await asyncio.sleep(0.1)

    assert current().state is State.DONE
    assert current().outcome == "completed"


async def test_live_arbitrate_request_reply() -> None:
    run = _run_token()
    topic = f"pytest-who-goes-first-{run}"
    mgr_cfg = BusConfig(agent_id=f"pytest-heron-{run}", url=_URL, env="dev")
    async with (
        FleetBus(mgr_cfg) as mgr_bus,
        FleetBus(BusConfig(agent_id="asker", url=_URL)) as ask_bus,
    ):
        orch = Orchestrator(mgr_bus, _counter_clock())
        await orch.serve_arbitration(topic, ("wren", "finch"))
        await asyncio.sleep(0.3)

        reply = await ask_bus.request(
            subjects.agent_arbitrate("dev", topic),
            Envelope.new(msg_id="q", ts="t", sender="asker", kind="arbitrate", topic=topic),
            timeout_s=5,
        )

    assert reply is not None
    assert reply.payload["decision"] == "finch"
    assert reply.correlation_id == "q"
