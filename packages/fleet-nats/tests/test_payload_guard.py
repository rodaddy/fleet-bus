"""Pre-publish payload-size guard tests (fleet-bus #108).

NATS enforces a server-side ``max_payload`` (1 MiB default); a publish over it is
silently rejected server-side after the client fires, so an oversized dispatch
never starts and nothing surfaces to the caller. The bus adds a fail-loud
pre-publish guard that raises :class:`PayloadTooLargeError` — naming the actual
size, the limit, and an explicit truncation policy (NONE) — instead of dropping.

These are unit tests: a fake NATS client (with a ``max_payload`` attribute and an
async ``publish``) stands in for the wire so the guard is exercised off-network,
including an ACTUAL >1 MiB payload driven through it.
"""

from __future__ import annotations

from collections.abc import Iterator
from unittest.mock import AsyncMock, patch

import pytest
from fleet_core.transport import FleetBus as CoreFleetBus
from loguru import logger

from fleet_nats import BusConfig, Envelope, FleetBus, PayloadTooLargeError
from fleet_nats.bus import DeliveryMode, DurableNotProvisionedError

# NATS's default max_payload — the value the fake advertises, matching the real
# server default the guard protects against.
_MAX_PAYLOAD = 1_048_576


class _FakeReply:
    """A minimal reply Msg double carrying decodable envelope bytes."""

    def __init__(self, data: bytes) -> None:
        self.data = data


class _FakeNats:
    """Minimal NATS client double: advertises max_payload, records publishes."""

    def __init__(self, max_payload: int = _MAX_PAYLOAD) -> None:
        self.max_payload = max_payload
        self.published: list[tuple[str, bytes]] = []
        self.requested: list[tuple[str, bytes]] = []
        self.subscribed: list[tuple[str, object]] = []

    async def publish(self, subject: str, data: bytes) -> None:
        self.published.append((subject, data))

    async def subscribe(self, subject: str, cb: object) -> object:
        self.subscribed.append((subject, cb))
        return object()

    async def request(self, subject: str, data: bytes, timeout: float = 5.0) -> _FakeReply:
        self.requested.append((subject, data))
        # Echo a valid ping envelope back so request() can decode a reply.
        reply = Envelope.new(msg_id="r", ts="t", sender="peer", kind="ping", payload={})
        return _FakeReply(reply.to_bytes())


class _FakeMsg:
    """A raw request Msg double: carries a reply subject for FleetBus.reply."""

    def __init__(self, reply: str) -> None:
        self.reply = reply


def _bus_with_fake(max_payload: int = _MAX_PAYLOAD) -> tuple[FleetBus, _FakeNats]:
    """A FleetBus with its NATS client swapped for a fake (no real connection)."""
    bus = FleetBus(BusConfig(agent_id="pytest"))
    fake = _FakeNats(max_payload=max_payload)
    bus._nc = fake  # type: ignore[assignment]  # inject the wire double
    return bus, fake


def _envelope_with_payload_bytes(target_bytes: int) -> Envelope:
    """Build an envelope whose ENCODED (to_bytes) form is at least ``target_bytes``."""
    # One padding char ≈ one encoded byte inside the JSON string; overshoot so the
    # full encoded envelope (with the framing keys) clears the target.
    blob = "x" * target_bytes
    return Envelope.new(
        msg_id="m-big",
        ts="2026-07-07T00:00:00Z",
        sender="pytest",
        kind="dispatch",
        payload={"blob": blob},
    )


def test_fleet_nats_bus_is_core_transport_wrapper() -> None:
    """The compatibility surface is a thin subclass of the fleet-core transport."""
    bus = FleetBus(BusConfig(agent_id="pytest"))
    assert isinstance(bus, CoreFleetBus)


async def test_under_limit_publishes_normally() -> None:
    """A small envelope passes the guard and reaches the wire unchanged."""
    bus, fake = _bus_with_fake()
    env = Envelope.new(msg_id="m1", ts="t", sender="pytest", kind="ping", payload={"n": 1})
    await bus.publish("dev.direct.pytest", env)
    assert len(fake.published) == 1
    assert fake.published[0][0] == "dev.direct.pytest"
    assert fake.published[0][1] == env.to_bytes()


async def test_legacy_publish_default_stays_core_nats() -> None:
    """Legacy fleet-nats publish callers do not silently switch to JetStream."""
    bus, fake = _bus_with_fake()
    env = Envelope.new(msg_id="m1", ts="t", sender="pytest", kind="dispatch", payload={})

    with patch.object(bus, "_publish_durable", new=AsyncMock()) as durable_publish:
        await bus.publish("dev.direct.pytest", env)

    durable_publish.assert_not_awaited()
    assert fake.published == [("dev.direct.pytest", env.to_bytes())]


async def test_explicit_core_publish_uses_core_nats() -> None:
    """Callers can still select core explicitly without touching the durable path."""
    bus, fake = _bus_with_fake()
    env = Envelope.new(msg_id="m1", ts="t", sender="pytest", kind="dispatch", payload={})

    with patch.object(bus, "_publish_durable", new=AsyncMock()) as durable_publish:
        await bus.publish("dev.direct.pytest", env, mode=DeliveryMode.CORE)

    durable_publish.assert_not_awaited()
    assert fake.published == [("dev.direct.pytest", env.to_bytes())]


async def test_explicit_durable_publish_delegates_without_core_fallback() -> None:
    """Explicit durable publish stays fail-loud when durable is not provisioned."""
    bus, fake = _bus_with_fake()
    env = Envelope.new(msg_id="m1", ts="t", sender="pytest", kind="dispatch", payload={})
    expected = DurableNotProvisionedError(
        subject="dev.direct.pytest",
        operation="publish",
        reason="missing stream",
    )

    with patch.object(bus, "_publish_durable", new=AsyncMock(side_effect=expected)):
        with pytest.raises(DurableNotProvisionedError) as excinfo:
            await bus.publish("dev.direct.pytest", env, mode=DeliveryMode.DURABLE)

    assert excinfo.value is expected
    assert fake.published == []


async def test_legacy_subscribe_default_stays_core_nats() -> None:
    """Legacy fleet-nats subscribe callers bind a core subscription by default."""
    bus, fake = _bus_with_fake()

    async def handler(envelope: Envelope) -> None:
        _ = envelope

    with patch.object(bus, "_subscribe_durable", new=AsyncMock()) as durable_subscribe:
        sub = await bus.subscribe("dev.direct.pytest", handler)

    durable_subscribe.assert_not_awaited()
    assert sub is not None
    assert len(fake.subscribed) == 1
    assert fake.subscribed[0][0] == "dev.direct.pytest"


async def test_explicit_durable_subscribe_requires_preprovisioned_consumer() -> None:
    """Explicit durable subscribe delegates to fleet-core and fails before fallback."""
    bus, fake = _bus_with_fake()

    async def handler(envelope: Envelope) -> None:
        _ = envelope

    with pytest.raises(DurableNotProvisionedError) as excinfo:
        await bus.subscribe("dev.direct.pytest", handler, mode=DeliveryMode.DURABLE)

    assert excinfo.value.operation == "subscribe"
    assert "consumer identity" in str(excinfo.value)
    assert fake.subscribed == []


async def test_legacy_raw_subscribe_default_stays_core_nats() -> None:
    """Legacy raw subscribers keep request/reply-compatible core NATS behavior."""
    bus, fake = _bus_with_fake()

    async def handler(msg: object) -> None:
        _ = msg

    with patch.object(bus, "_raw_subscribe_durable", new=AsyncMock()) as durable_subscribe:
        sub = await bus.raw_subscribe("dev.direct.pytest", handler)

    durable_subscribe.assert_not_awaited()
    assert sub is not None
    assert len(fake.subscribed) == 1
    assert fake.subscribed[0][0] == "dev.direct.pytest"


async def test_explicit_durable_raw_subscribe_requires_preprovisioned_consumer() -> None:
    """Explicit durable raw subscribe is fail-loud and never falls back to core."""
    bus, fake = _bus_with_fake()

    async def handler(msg: object) -> None:
        _ = msg

    with pytest.raises(DurableNotProvisionedError) as excinfo:
        await bus.raw_subscribe("dev.direct.pytest", handler, mode=DeliveryMode.DURABLE)

    assert excinfo.value.operation == "raw_subscribe"
    assert "consumer identity" in str(excinfo.value)
    assert fake.subscribed == []


async def test_over_1mb_payload_raises_fail_loud() -> None:
    """An ACTUAL >1 MiB encoded envelope raises PayloadTooLargeError, not a silent drop."""
    bus, fake = _bus_with_fake()
    env = _envelope_with_payload_bytes(_MAX_PAYLOAD + 1024)
    size = len(env.to_bytes())
    assert size > _MAX_PAYLOAD  # the payload really is oversized

    with pytest.raises(PayloadTooLargeError) as excinfo:
        await bus.publish("dev.direct.pytest", env)

    err = excinfo.value
    assert err.subject == "dev.direct.pytest"
    assert err.size == size
    assert err.limit == _MAX_PAYLOAD
    # Fail-loud message names the actual size, the limit, and the truncation policy.
    msg = str(err)
    assert str(size) in msg
    assert str(_MAX_PAYLOAD) in msg
    assert "Truncation policy: NONE" in msg
    # NOTHING reached the wire — the guard fired before the publish.
    assert fake.published == []


async def test_over_limit_is_a_valueerror_subclass() -> None:
    """PayloadTooLargeError is a ValueError so existing except-ValueError callers catch it."""
    bus, _ = _bus_with_fake()
    env = _envelope_with_payload_bytes(_MAX_PAYLOAD + 1024)
    with pytest.raises(ValueError):
        await bus.publish("dev.direct.pytest", env)


def test_check_payload_size_raises_without_publishing() -> None:
    """check_payload_size (round 3) size-checks an envelope and NEVER publishes.

    The pre-ownership guard start_task runs before any side effect: an oversized
    envelope raises PayloadTooLargeError; nothing reaches the wire either way.
    """
    small_limit = 4096
    bus, fake = _bus_with_fake(max_payload=small_limit)
    big = _envelope_with_payload_bytes(small_limit + 512)
    with pytest.raises(PayloadTooLargeError) as excinfo:
        bus.check_payload_size("dev.direct.pytest", big)
    assert excinfo.value.limit == small_limit
    assert fake.published == []  # a size-check never publishes

    # A fitting envelope passes the check silently, still without publishing.
    small = Envelope.new(msg_id="m", ts="t", sender="pytest", kind="ping", payload={"n": 1})
    bus.check_payload_size("dev.direct.pytest", small)
    assert fake.published == []


async def test_guard_uses_server_negotiated_limit() -> None:
    """The guard reads the client's advertised max_payload, not a hardcoded 1 MiB.

    A server that advertises a SMALLER max_payload must be respected — an envelope
    under 1 MiB but over the negotiated limit is still refused.
    """
    small_limit = 4096
    bus, fake = _bus_with_fake(max_payload=small_limit)
    env = _envelope_with_payload_bytes(small_limit + 512)
    size = len(env.to_bytes())
    assert size < _MAX_PAYLOAD  # under the 1 MiB default...
    assert size > small_limit  # ...but over THIS server's advertised limit

    with pytest.raises(PayloadTooLargeError) as excinfo:
        await bus.publish("dev.direct.pytest", env)
    assert excinfo.value.limit == small_limit
    assert fake.published == []


async def test_exactly_at_limit_publishes() -> None:
    """A payload exactly AT the limit is allowed (the reject is strictly over)."""
    small_limit = 4096
    bus, fake = _bus_with_fake(max_payload=small_limit)
    # Pad an envelope up to exactly the limit.
    base = Envelope.new(msg_id="m", ts="t", sender="pytest", kind="ping", payload={})
    overhead = len(base.to_bytes())
    # payload {"blob": "..."} adds ~ len('{"blob":""}') minus the empty-payload {}.
    pad = small_limit - overhead - len('"blob":""')
    env = Envelope.new(
        msg_id="m", ts="t", sender="pytest", kind="ping", payload={"blob": "x" * pad}
    )
    data = env.to_bytes()
    assert len(data) <= small_limit  # constructed at-or-below the limit
    await bus.publish("dev.direct.pytest", env)
    assert len(fake.published) == 1


# --- exactly-one-over boundary (review P2-8) ----------------------------------


async def test_exactly_one_over_limit_raises() -> None:
    """A payload at limit+1 is refused — the strict `>` boundary, off-by-one pinned."""
    small_limit = 4096
    bus, fake = _bus_with_fake(max_payload=small_limit)
    base = Envelope.new(msg_id="m", ts="t", sender="pytest", kind="ping", payload={})
    overhead = len(base.to_bytes())
    # Build exactly limit+1 encoded bytes: pad to fill to the limit, then +1 char.
    pad = small_limit - overhead - len('"blob":""') + 1
    env = Envelope.new(
        msg_id="m", ts="t", sender="pytest", kind="ping", payload={"blob": "x" * pad}
    )
    size = len(env.to_bytes())
    assert size == small_limit + 1  # exactly one over
    with pytest.raises(PayloadTooLargeError) as excinfo:
        await bus.publish("dev.direct.pytest", env)
    assert excinfo.value.size == small_limit + 1
    assert excinfo.value.limit == small_limit
    assert fake.published == []


# --- fail-loud TRIPPED log line (review P1-3) ---------------------------------


@pytest.fixture
def captured_logs() -> Iterator[list[str]]:
    """Capture loguru messages via an added sink (repo convention, not caplog)."""
    logged: list[str] = []
    sink_id = logger.add(lambda m: logged.append(str(m)), level="DEBUG", format="{message}")
    try:
        yield logged
    finally:
        logger.remove(sink_id)


async def test_guard_tripped_logs_subject_size_limit(captured_logs: list[str]) -> None:
    """The guard logs `payload guard TRIPPED` with subject/size/limit BEFORE the raise."""
    small_limit = 4096
    bus, _ = _bus_with_fake(max_payload=small_limit)
    env = _envelope_with_payload_bytes(small_limit + 512)
    size = len(env.to_bytes())
    with pytest.raises(PayloadTooLargeError):
        await bus.publish("dev.direct.pytest", env)
    assert any(
        "payload guard TRIPPED" in line
        and "dev.direct.pytest" in line
        and f"size={size}" in line
        and f"limit={small_limit}" in line
        for line in captured_logs
    ), captured_logs


# --- request()/reply() route through the guard (review P2-6) ------------------


async def test_request_guards_oversized_payload() -> None:
    """request() fails loud on an oversized envelope instead of masking as a timeout."""
    small_limit = 4096
    bus, fake = _bus_with_fake(max_payload=small_limit)
    env = _envelope_with_payload_bytes(small_limit + 512)
    with pytest.raises(PayloadTooLargeError) as excinfo:
        await bus.request("dev.arbitrate.pytest", env)
    assert excinfo.value.subject == "dev.arbitrate.pytest"
    assert fake.requested == []  # nothing reached the wire


async def test_request_under_limit_round_trips() -> None:
    """A small request passes the guard and reaches the wire (decodes a reply)."""
    bus, fake = _bus_with_fake()
    env = Envelope.new(msg_id="m", ts="t", sender="pytest", kind="ping", payload={"n": 1})
    reply = await bus.request("dev.arbitrate.pytest", env)
    assert reply is not None
    assert len(fake.requested) == 1


async def test_reply_guards_oversized_payload() -> None:
    """reply() guards the encoded bytes using msg.reply as the subject (P2-6)."""
    small_limit = 4096
    bus, fake = _bus_with_fake(max_payload=small_limit)
    env = _envelope_with_payload_bytes(small_limit + 512)
    msg = _FakeMsg(reply="dev.reply.pytest")
    with pytest.raises(PayloadTooLargeError) as excinfo:
        await bus.reply(msg, env)  # type: ignore[arg-type]
    assert excinfo.value.subject == "dev.reply.pytest"
    assert fake.published == []  # nothing reached the wire


async def test_reply_under_limit_publishes() -> None:
    """A small reply passes the guard and reaches the wire on msg.reply."""
    bus, fake = _bus_with_fake()
    env = Envelope.new(msg_id="m", ts="t", sender="pytest", kind="ping", payload={})
    msg = _FakeMsg(reply="dev.reply.pytest")
    await bus.reply(msg, env)  # type: ignore[arg-type]
    assert fake.published == [("dev.reply.pytest", env.to_bytes())]


# --- functional: guard reads a REAL nats-py client's max_payload (F4) ---------


async def test_guard_reads_real_nats_client_max_payload() -> None:
    """The guard negotiates against a REAL nats-py Client's ``max_payload`` attr.

    Every other test here stubs ``max_payload`` on a hand-rolled fake, so a nats-py
    change that renames the attribute or changes its pre-connect default would slip
    through silently. This binds the guard to the actual ``nats.aio.client.Client``
    object path: an unconnected real Client exposes the 1 MiB default at exactly the
    attribute the guard reads (``self._nc.max_payload``). If nats-py drops/renames
    that attribute or shifts the default, this test breaks — the intended tripwire.

    The oversized path is used deliberately: the guard fires BEFORE any wire call,
    so no real connection is needed (and none is made).
    """
    from nats.aio.client import Client

    real = Client()
    # Sanity: the real client exposes the attribute the guard depends on, as an int
    # at nats-py's documented 1 MiB pre-connect default. This is the tripwire.
    assert isinstance(real.max_payload, int)
    assert real.max_payload == _MAX_PAYLOAD

    bus = FleetBus(BusConfig(agent_id="pytest"))
    bus._nc = real  # inject the REAL nats-py client (unconnected)

    env = _envelope_with_payload_bytes(_MAX_PAYLOAD + 1024)
    size = len(env.to_bytes())
    with pytest.raises(PayloadTooLargeError) as excinfo:
        await bus.publish("dev.direct.pytest", env)
    # The limit in the error came from the real client's negotiated attribute,
    # proving the guard read Client.max_payload (not a stubbed value).
    assert excinfo.value.limit == real.max_payload
    assert excinfo.value.size == size
