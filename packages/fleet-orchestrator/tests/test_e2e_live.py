"""The live coordination integration test (#9) — the automatable half of the gate.

Scripts one moderate multi-step task through the full coordination closed loop
over the real fleet-hub bus: the manager dispatches to *both* workers, a worker
finishes WITHOUT handing off so the orchestrator's own detector fires the relay,
the peer picks it up, and a completion sentinel advances the orchestrator to DONE.
Auto-skips off-network (mirrors ``test_behaviors_live.py`` /
``fleet_nats/tests/test_bus_live.py``) so CI stays green.

**Scope — what this proves and what it does NOT.** This exercises the orchestrator
coordination brain over *real transport*, but the "workers" are pytest publishers
(``sub_bus.publish(agent_status…)``), NOT the real Wren/Finch agents running
their Hermes gateways. So it proves *dispatch/relay/done coordination works on the
live bus* — genuinely live, and the load-bearing half of #9 — but it is an
**integration test with simulated workers**, not the full real-agent proving run.

The remaining half of #9 — the **real agents coordinating and posting their own
words in Discord**, plus the k-board ``status=done`` durable-record check on a
purpose-made proving task — is the captured-evidence live run against the deployed
trio (the maintainer-authorized window, same as the Phase-7 enable). A bus test
cannot see an agent's Discord post; that half is verified by hand there. Both
halves are required to call #9 fully done — hence the plan marks Phase 9 ``[wip]``.
"""

from __future__ import annotations

import asyncio
import itertools
import socket
from urllib.parse import urlsplit
from uuid import uuid4

from fleet_nats import BusConfig, Envelope, FleetBus, done_envelope, subjects

from fleet_orchestrator import (
    CoordinationSession,
    Event,
    EventKind,
    IdClock,
    Orchestrator,
    State,
)

_URL = BusConfig.from_env("pytest").url


def _server_reachable() -> bool:
    parts = urlsplit(_URL)
    host, port = parts.hostname or "", parts.port or 4222
    try:
        with socket.create_connection((host, port), timeout=2):
            return True
    except OSError:
        return False


# NOTE: the test skips itself at runtime (pytest.skip inside the test) when fleet-hub
# is unreachable, rather than a module-level skipif. Both produce "1 skipped" and
# pytest exit 0 off-network; the runtime form keeps the skip reason next to the
# reachability check. The plan checker parses the pytest OUTCOME ("N passed"→PASS,
# "N skipped"→SKIP), not the exit code alone — because a skipped-only run and a
# real pass both exit 0 — so an off-network skip is honestly SKIP, never a false
# PASS on the acceptance gate.


def _counter_clock() -> IdClock:
    ids = (f"e2e-{i}" for i in itertools.count())
    return IdClock(new_id=lambda: next(ids), now=lambda: "2026-07-07T00:00:00Z")


def _run_token() -> str:
    """Unique subject suffix for live-bus isolation across concurrent runs."""
    return uuid4().hex[:8]


async def test_e2e_dispatch_relay_done_proving_run() -> None:
    """The full coordination loop on live transport: dispatch→relay→done.

    Skips cleanly when fleet-hub is unreachable (keeps CI green off-network).
    """
    import pytest

    if not _server_reachable():
        pytest.skip("fleet NATS server (fleet-hub) not reachable — live proving run skipped")

    run = _run_token()
    channel = f"pytest-e2e-proving-{run}"
    workers = (f"pytest-wren-{run}", f"pytest-finch-{run}")
    quiet_worker, relay_peer = workers
    task_id = f"e2e-proving-task-{run}"

    # Both workers' dispatch lanes + the relay lane are observed, so we can assert
    # dispatch reached BOTH workers and the relay fired to the un-mentioned peer.
    dispatched_to: list[str] = []
    relayed_to: list[Envelope] = []
    got_both_dispatch = asyncio.Event()
    got_relay = asyncio.Event()

    async def on_dispatch(env: Envelope) -> None:
        dispatched_to.append(env.to or "")
        if len(dispatched_to) >= len(workers):
            got_both_dispatch.set()

    async def on_relay(env: Envelope) -> None:
        relayed_to.append(env)
        got_relay.set()

    mgr_cfg = BusConfig(agent_id=f"pytest-heron-{run}", url=_URL, env="dev")
    async with (
        FleetBus(mgr_cfg) as mgr_bus,
        FleetBus(BusConfig(agent_id="pytest-workers", url=_URL)) as sub_bus,
    ):
        # Subscribe every worker's dispatch lane + the peer's relay lane.
        for worker in workers:
            await sub_bus.subscribe(
                subjects.agent_dispatch("dev", worker, channel=channel), on_dispatch
            )
        # Per-channel relay (#117), matching the scoped dispatch subscribed above.
        await sub_bus.subscribe(subjects.agent_relay("dev", relay_peer, channel=channel), on_relay)

        orch = Orchestrator(mgr_bus, _counter_clock())
        session = CoordinationSession(
            task_id=task_id, channel=channel, manager="pytest-heron", workers=workers
        )
        # Manager watches worker status. Settle so every subscription is registered
        # on the server BEFORE any publish — core NATS has no backlog, so an
        # immediate publish would race the subscription and be dropped.
        _, current = await orch.watch_worker_status(session.advanced_to(State.WORKING))
        await asyncio.sleep(0.3)

        # Beat 1 — dispatch fans out to BOTH workers on their per-channel lanes.
        await orch.apply(session, Event(kind=EventKind.DISPATCH))
        await asyncio.wait_for(got_both_dispatch.wait(), timeout=5)

        # Beat 2 — the quiet worker finishes WITHOUT @-mentioning the peer, so the
        # orchestrator's own detector must fire the relay (no fabricated event).
        await sub_bus.publish(
            subjects.agent_status("dev", task_id),
            Envelope.new(
                msg_id="e2e-ws1",
                ts="t",
                sender=quiet_worker,
                kind="status",
                task_id=task_id,
                payload={"text": "I finished my half of the proving task."},
            ),
        )
        await asyncio.wait_for(got_relay.wait(), timeout=5)

        # Beat 3 — a completion sentinel advances the orchestrator to DONE.
        await sub_bus.publish(
            subjects.agent_status("dev", task_id),
            done_envelope(
                msg_id="e2e-dn1", ts="t", sender=relay_peer, task_id=task_id, outcome="completed"
            ),
        )
        for _ in range(50):
            if current().state is State.DONE:
                break
            await asyncio.sleep(0.1)

    # Dispatch reached BOTH workers (distinct recipients).
    assert set(dispatched_to) == set(workers)
    # The relay fired exactly once, to the un-mentioned peer, carrying the context.
    assert len(relayed_to) == 1
    assert relayed_to[0].to == relay_peer
    assert "finished my half" in relayed_to[0].payload["text"]
    # The sentinel advanced the orchestrator to DONE with the outcome.
    assert current().state is State.DONE
    assert current().outcome == "completed"
