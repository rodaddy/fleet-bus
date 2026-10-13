"""Live functional tests against the real fleet NATS server (fleet-hub).

These prove the actual wire behaviour: connect, pub/sub round-trip, and
request/reply. Skipped automatically if the server is unreachable so the suite
stays green off-network. Set FLEET_NATS_URL to point elsewhere.
"""

from __future__ import annotations

import asyncio
import socket
from urllib.parse import urlsplit

import pytest

from fleet_nats import BusConfig, Envelope, FleetBus, subjects

# Single source of truth: the URL comes from config (FLEET_NATS_URL env, or the
# sanctioned default in config.py). Do NOT re-bake the IP here — if the server
# moves, config follows and this test follows with it (the "never baked
# elsewhere" invariant).
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


async def test_pub_sub_roundtrip() -> None:
    cfg = BusConfig(agent_id="pytest", url=_URL)
    received: list[Envelope] = []
    done = asyncio.Event()

    async def handler(env: Envelope) -> None:
        received.append(env)
        done.set()

    subject = subjects.direct("dev", "pytest-roundtrip")
    async with FleetBus(cfg) as bus:
        await bus.subscribe(subject, handler)
        await bus.publish(
            subject,
            Envelope.new(msg_id="m1", ts="t", sender="pytest", kind="ping", payload={"n": 1}),
        )
        await asyncio.wait_for(done.wait(), timeout=5)

    assert len(received) == 1
    assert received[0].kind == "ping"
    assert received[0].payload == {"n": 1}


async def test_request_reply() -> None:
    cfg = BusConfig(agent_id="pytest", url=_URL)
    subject = subjects.agent_arbitrate("dev", "pytest-decision")

    async def responder_bus() -> None:
        async with FleetBus(BusConfig(agent_id="responder", url=_URL)) as bus:
            got = asyncio.Event()

            async def _on(msg):  # type: ignore[no-untyped-def]
                env = Envelope.from_bytes(msg.data)
                await bus._nc.publish(  # replying via raw reply subject
                    msg.reply,
                    Envelope.new(
                        msg_id="r1",
                        ts="t",
                        sender="responder",
                        kind="decision",
                        payload={"answer": env.payload.get("q")},
                        correlation_id=env.id,
                    ).to_bytes(),
                )
                got.set()

            await bus._nc.subscribe(subject, cb=_on)
            await asyncio.wait_for(got.wait(), timeout=5)

    responder = asyncio.create_task(responder_bus())
    await asyncio.sleep(0.3)  # let responder subscribe

    async with FleetBus(cfg) as bus:
        reply = await bus.request(
            subject,
            Envelope.new(msg_id="q1", ts="t", sender="pytest", kind="ask", payload={"q": 42}),
            timeout_s=5,
        )

    await responder
    assert reply is not None
    assert reply.kind == "decision"
    assert reply.payload == {"answer": 42}
    assert reply.correlation_id == "q1"


async def test_request_no_responder_returns_none() -> None:
    cfg = BusConfig(agent_id="pytest", url=_URL)
    async with FleetBus(cfg) as bus:
        reply = await bus.request(
            subjects.agent_arbitrate("dev", "nobody-home-xyz"),
            Envelope.new(msg_id="q", ts="t", sender="pytest", kind="ask"),
            timeout_s=1,
        )
    assert reply is None
