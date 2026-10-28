"""Contract tests for fleet_core.transport."""

from __future__ import annotations

import asyncio
import shutil
import socket
import subprocess
import time
from collections.abc import Awaitable, Callable
from pathlib import Path
from types import SimpleNamespace
from typing import Any, ClassVar, cast

import pytest
from nats.aio.client import Client as RealNATS
from nats.aio.msg import Msg
from nats.errors import NoRespondersError
from nats.errors import TimeoutError as NatsTimeoutError
from nats.js.api import AckPolicy, ConsumerConfig, StreamConfig
from nats.js.errors import NotFoundError

from fleet_core import (
    BusConfig,
    DeliveryMode,
    Done,
    DurableNotProvisionedError,
    Envelope,
    FleetBus,
    FleetTransport,
    Heartbeat,
    PayloadTooLargeError,
    Presence,
)
from fleet_core.transport import _is_auth_rejection


def _env(
    kind: str = "status",
    *,
    payload: dict[str, Any] | None = None,
    typed_signal: object | None = None,
) -> Envelope:
    return Envelope.new(
        msg_id=f"msg-{kind}",
        ts="2026-07-11T00:00:00Z",
        sender="pytest",
        kind=kind,
        payload=payload or {},
        typed_signal=typed_signal,
    )


def _heartbeat(agent: str = "heron") -> Heartbeat:
    return Heartbeat(agent_id=agent, role="agent", lanes=("dev.all",), seq=1)


def _presence(agent: str = "heron") -> Presence:
    return Presence(agent_id=agent, role="agent", lanes=("dev.all",), seq=1)


class _Msg:
    def __init__(self, data: bytes = b"", reply: str = "") -> None:
        self.data = data
        self.reply = reply
        self.acked = False
        self.nak_delay: float | None = None
        self.termed = False

    async def ack(self) -> None:
        self.acked = True

    async def nak(self, delay: int | float | None = None) -> None:
        self.nak_delay = None if delay is None else float(delay)

    async def term(self) -> None:
        self.termed = True


class _Subscription:
    pass


class _PubAck:
    def __init__(self, *, stream: str, seq: int) -> None:
        self.stream = stream
        self.seq = seq


class _StubJetStream:
    def __init__(self) -> None:
        self.stream_for_subject: str | BaseException = "FLEET_CORE"
        self.consumer_result: object | BaseException = SimpleNamespace(
            config=SimpleNamespace(max_deliver=3)
        )
        self.published: list[tuple[str, bytes, str | None]] = []
        self.subscriptions: list[
            tuple[str, Callable[[_Msg], Awaitable[None]], str | None, str | None, bool]
        ] = []

    async def find_stream_name_by_subject(self, subject: str) -> str:
        if isinstance(self.stream_for_subject, BaseException):
            raise self.stream_for_subject
        return self.stream_for_subject

    async def publish(self, subject: str, payload: bytes, *, stream: str | None = None) -> _PubAck:
        self.published.append((subject, payload, stream))
        return _PubAck(stream=stream or "FLEET_CORE", seq=len(self.published))

    async def consumer_info(self, stream: str, consumer: str) -> object:
        _ = stream, consumer
        if isinstance(self.consumer_result, BaseException):
            raise self.consumer_result
        return self.consumer_result

    async def subscribe(
        self,
        subject: str,
        *,
        cb: Callable[[_Msg], Awaitable[None]],
        durable: str | None = None,
        stream: str | None = None,
        manual_ack: bool = False,
    ) -> _Subscription:
        self.subscriptions.append((subject, cb, durable, stream, manual_ack))
        return _Subscription()


class _StubNATS:
    instances: ClassVar[list[_StubNATS]] = []

    def __init__(self) -> None:
        self.is_connected = False
        self.max_payload = 1_048_576
        self.last_error: BaseException | None = None
        self.connect_kwargs: dict[str, Any] | None = None
        self.published: list[tuple[str, bytes]] = []
        self.subscriptions: list[tuple[str, Callable[[_Msg], Awaitable[None]]]] = []
        self.requests: list[tuple[str, bytes, float]] = []
        self.request_reply: _Msg | BaseException = _Msg(_env("reply").to_bytes())
        self.drained = False
        self.closed = False
        self.jetstream_context = _StubJetStream()
        _StubNATS.instances.append(self)

    async def connect(self, **kwargs: Any) -> None:
        self.connect_kwargs = kwargs
        self.is_connected = True

    async def drain(self) -> None:
        self.drained = True
        self.is_connected = False

    async def close(self) -> None:
        self.closed = True
        self.is_connected = False

    async def publish(self, subject: str, data: bytes) -> None:
        self.published.append((subject, data))

    async def subscribe(self, subject: str, cb: Callable[[_Msg], Awaitable[None]]) -> _Subscription:
        self.subscriptions.append((subject, cb))
        return _Subscription()

    async def request(self, subject: str, data: bytes, *, timeout: float) -> _Msg:
        self.requests.append((subject, data, timeout))
        if isinstance(self.request_reply, BaseException):
            raise self.request_reply
        return self.request_reply

    def jetstream(self, **opts: object) -> _StubJetStream:
        _ = opts
        return self.jetstream_context


@pytest.fixture(autouse=True)
def stub_nats(monkeypatch: pytest.MonkeyPatch) -> None:
    _StubNATS.instances.clear()
    monkeypatch.setattr("fleet_core.transport.NATS", _StubNATS)


def test_exports_transport_api() -> None:
    assert FleetTransport is FleetBus
    assert DeliveryMode.DURABLE.value == "durable"
    assert DurableNotProvisionedError(subject="s", operation="publish").subject == "s"


async def test_connect_passes_single_url_auth_and_reconnect_kwargs(tmp_path: Any) -> None:
    creds = tmp_path / "agent.creds"
    creds.write_text("placeholder", encoding="utf-8")
    cfg = BusConfig(
        agent_id="heron",
        url="nats://user:secret@192.0.2.99:4222",
        env="dev",
        name="fleet-nats-heron",
        connect_timeout_s=1,
        max_reconnect_attempts=3,
        reconnect_wait_s=4,
        nats_user="ignored",
        nats_password="ignored-secret",
        nats_creds_file=str(creds),
    )

    bus = FleetBus(cfg)
    await bus.connect()

    kwargs = _StubNATS.instances[0].connect_kwargs
    assert kwargs is not None
    assert kwargs["servers"] == ["nats://user:secret@192.0.2.99:4222"]
    assert kwargs["name"] == "fleet-nats-heron"
    assert kwargs["connect_timeout"] == 1
    assert kwargs["max_reconnect_attempts"] == 3
    assert kwargs["reconnect_time_wait"] == 4
    assert kwargs["user"] is None
    assert kwargs["password"] is None
    assert kwargs["user_credentials"] == str(creds)
    assert kwargs["inbox_prefix"] == "_INBOX.heron"
    assert callable(kwargs["disconnected_cb"])
    assert callable(kwargs["reconnected_cb"])
    assert callable(kwargs["closed_cb"])
    assert callable(kwargs["error_cb"])


async def test_error_cb_reports_authz_refusal_as_a_dropped_message() -> None:
    """An async permissions violation must say WHAT was refused and that it DROPPED.

    NATS core publish is fire-and-forget, so a server-side permissions violation
    arrives on ``error_cb`` — never as a raise from ``publish()``. Logged as a
    generic "client error" it reads as transient reconnect noise, so a principal
    whose grant is wrong watches its messages vanish with no stated reason. That
    is worst for a publish-only principal (#222) whose entire job is pushing to
    one narrow subject.
    """
    from loguru import logger

    records: list[tuple[str, str]] = []
    sink_id = logger.add(
        lambda message: records.append((message.record["level"].name, message.record["message"])),
        level="DEBUG",
    )
    try:
        bus = FleetBus(BusConfig(agent_id="fleet-runner"))
        await bus.connect()
        kwargs = _StubNATS.instances[-1].connect_kwargs
        assert kwargs is not None
        error_cb = kwargs["error_cb"]

        records.clear()
        await error_cb(
            Exception('nats: permissions violation for publish to "dev.agent.dispatch.wren"')
        )
        assert any(
            level == "ERROR" and "REFUSED" in message and "DROPPED" in message
            for level, message in records
        ), records

        # A non-auth client error must still take the ordinary warning path —
        # the authz branch returns early, so it must not swallow everything.
        records.clear()
        await error_cb(Exception("nats: slow consumer detected"))
        assert any(
            level == "WARNING" and "client error" in message for level, message in records
        ), records
        assert not any("REFUSED" in message for _, message in records), records
    finally:
        logger.remove(sink_id)


async def test_close_drains_connected_client() -> None:
    bus = FleetBus(BusConfig(agent_id="heron"))
    await bus.connect()
    await bus.close()

    client = _StubNATS.instances[0]
    assert client.drained is True
    assert client.closed is False


def test_auth_rejection_marker_matches_nats_initial_connect_errors() -> None:
    assert _is_auth_rejection(RuntimeError("nats: Authorization Violation")) is True
    assert _is_auth_rejection(RuntimeError("socket closed")) is False


@pytest.mark.parametrize(
    "agent_id",
    ["Wren Smith", "finch.v2"],
)
def test_config_rejects_lossy_reply_inbox_identities(agent_id: str) -> None:
    with pytest.raises(ValueError, match="canonical NATS identity token"):
        BusConfig(agent_id=agent_id)


def test_config_prefers_authenticated_user_for_reply_inbox() -> None:
    config = BusConfig(agent_id="spoofable-label", nats_user="wren", nats_password="test-only")
    assert config.inbox_prefix == "_INBOX.wren"


def test_creds_file_uses_configured_identity_for_fail_closed_server_grant(tmp_path: Path) -> None:
    creds = tmp_path / "agent.creds"
    creds.write_text("placeholder", encoding="utf-8")
    config = BusConfig(agent_id="finch", nats_creds_file=str(creds))
    assert config.inbox_prefix == "_INBOX.finch"


def test_policy_defaults_durable_with_typed_heartbeat_as_sole_core_exception() -> None:
    """Delivery is chosen from the typed signal model, not the wire kind string.

    Durable by default; the ONLY implicit core exception is a bound typed
    ``Heartbeat``/``Presence`` whose own model owns ``jetstream=False``.
    """
    bus = FleetBus(BusConfig(agent_id="heron"))

    # No typed signal, any kind -> durable.
    assert bus.delivery_mode_for("dev.any", _env("status")) is DeliveryMode.DURABLE
    assert bus.delivery_mode_for("dev.any", _env("heartbeat")) is DeliveryMode.DURABLE
    # Typed Heartbeat / Presence signal -> core (the sole exception).
    assert (
        bus.delivery_mode_for("dev.any", _env("heartbeat", typed_signal=_heartbeat()))
        is DeliveryMode.CORE
    )
    assert (
        bus.delivery_mode_for("dev.any", _env("presence", typed_signal=_presence()))
        is DeliveryMode.CORE
    )


def test_policy_ignores_kind_string_and_payload_keys_reads_only_typed_signal() -> None:
    """Neither a spoofed ``kind`` nor a payload ``jetstream`` key moves delivery."""
    bus = FleetBus(BusConfig(agent_id="heron"))

    # A kind="heartbeat" WITHOUT a typed signal is NOT the exception -> durable.
    assert bus.delivery_mode_for("dev.any", _env("heartbeat")) is DeliveryMode.DURABLE
    # An opaque payload {"jetstream": False} is not a typed signal -> durable.
    assert (
        bus.delivery_mode_for("dev.any", _env("status", payload={"jetstream": False}))
        is DeliveryMode.DURABLE
    )
    # A typed heartbeat whose kind was spoofed to "status" STILL routes core,
    # because the decision reads the typed model, not the kind string.
    assert (
        bus.delivery_mode_for("dev.any", _env("status", typed_signal=_heartbeat()))
        is DeliveryMode.CORE
    )


def test_policy_explicit_mode_overrides_typed_signal_unambiguously() -> None:
    """An explicit ``mode=`` wins over the typed-signal policy, in both directions."""
    bus = FleetBus(BusConfig(agent_id="heron"))

    heartbeat = _env("heartbeat", typed_signal=_heartbeat())
    ordinary = _env("status")
    # Explicit override is honored in publish's mode-resolution seam.
    assert bus._selected_mode(subject="s", envelope=heartbeat, mode=DeliveryMode.DURABLE) is (
        DeliveryMode.DURABLE
    )
    assert bus._selected_mode(subject="s", envelope=ordinary, mode=DeliveryMode.CORE) is (
        DeliveryMode.CORE
    )
    # With no explicit mode, the typed-signal policy applies.
    assert bus._selected_mode(subject="s", envelope=heartbeat, mode=None) is DeliveryMode.CORE
    assert bus._selected_mode(subject="s", envelope=ordinary, mode=None) is DeliveryMode.DURABLE


async def test_publish_ordinary_envelope_uses_jetstream_with_prevalidated_stream() -> None:
    bus = FleetBus(BusConfig(agent_id="heron"))
    await bus.connect()
    envelope = _env("status")

    await bus.publish("dev.status", envelope)

    client = _StubNATS.instances[0]
    assert client.published == []
    assert client.jetstream_context.published == [("dev.status", envelope.to_bytes(), "FLEET_CORE")]


@pytest.mark.parametrize(
    "subject",
    [
        "dev.agent.*.status.task-1",  # a hostile channel="*" builds exactly this
        "dev.agent.status.*",
        "dev.agent.>",
        "dev.agent.*.relay.wren",
    ],
)
async def test_publish_refuses_a_wildcard_subject_instead_of_dropping_it(subject: str) -> None:
    """A wildcard publish subject must raise here, not vanish on the wire.

    NATS rejects a wildcard-bearing publish ASYNCHRONOUSLY via ``error_cb``, so
    the publish call returns cleanly and the message is simply never delivered —
    the failure surfaces far away as a task that hangs to timeout. The channel
    token is publisher-controlled and fleet-bus #117 routes it into the status/
    relay/wake/ack/telemetry forms, so ``channel="*"`` is reachable from real
    inbound traffic, not just misuse.
    """
    bus = FleetBus(BusConfig(agent_id="heron"))
    await bus.connect()

    with pytest.raises(ValueError, match="wildcard-bearing subject"):
        await bus.publish(subject, _env("status"), mode=DeliveryMode.CORE)

    client = _StubNATS.instances[0]
    # Nothing reached the wire on either path.
    assert client.published == []
    assert client.jetstream_context.published == []


async def test_publish_still_allows_concrete_scoped_subjects() -> None:
    """The guard must not touch the legitimate scoped form #117 introduced."""
    bus = FleetBus(BusConfig(agent_id="heron"))
    await bus.connect()
    envelope = _env("status")

    await bus.publish("dev.agent.dash-dev.status.task-1", envelope, mode=DeliveryMode.CORE)

    client = _StubNATS.instances[0]
    assert client.published == [("dev.agent.dash-dev.status.task-1", envelope.to_bytes())]


async def test_publish_fails_loud_when_durable_stream_is_missing() -> None:
    bus = FleetBus(BusConfig(agent_id="heron"))
    await bus.connect()
    client = _StubNATS.instances[0]
    client.jetstream_context.stream_for_subject = NotFoundError(description="no stream")

    with pytest.raises(DurableNotProvisionedError, match="no core-NATS fallback"):
        await bus.publish("dev.status", _env("status"))

    assert client.published == []
    assert client.jetstream_context.published == []


async def test_publish_fails_loud_when_jetstream_capability_is_missing() -> None:
    bus = FleetBus(BusConfig(agent_id="heron"))
    await bus.connect()
    client = _StubNATS.instances[0]
    client.jetstream_context.stream_for_subject = NoRespondersError()

    with pytest.raises(DurableNotProvisionedError, match="no core-NATS fallback"):
        await bus.publish("dev.status", _env("status"))

    assert client.published == []
    assert client.jetstream_context.published == []


async def test_publish_typed_heartbeat_and_presence_and_explicit_core_use_core_nats() -> None:
    """Typed Heartbeat and Presence publish over core NATS; so does explicit CORE."""
    bus = FleetBus(BusConfig(agent_id="heron"))
    await bus.connect()

    heartbeat = _env("heartbeat", typed_signal=_heartbeat())
    presence = _env("presence", typed_signal=_presence())
    ordinary = _env("status")
    await bus.publish("dev.heartbeat", heartbeat)
    await bus.publish("dev.presence", presence)
    await bus.publish("dev.status", ordinary, mode=DeliveryMode.CORE)

    client = _StubNATS.instances[0]
    assert client.published == [
        ("dev.heartbeat", heartbeat.to_bytes()),
        ("dev.presence", presence.to_bytes()),
        ("dev.status", ordinary.to_bytes()),
    ]
    # None of the ephemeral signals touched JetStream.
    assert client.jetstream_context.published == []


async def test_publish_roundtripped_typed_heartbeat_still_uses_core_nats() -> None:
    """Replay/forward of a typed heartbeat preserves the core routing policy."""
    bus = FleetBus(BusConfig(agent_id="heron"))
    await bus.connect()
    heartbeat = Envelope.from_bytes(_env("heartbeat", typed_signal=_heartbeat()).to_bytes())

    await bus.publish("dev.heartbeat", heartbeat)

    client = _StubNATS.instances[0]
    assert client.published == [("dev.heartbeat", heartbeat.to_bytes())]
    assert client.jetstream_context.published == []


async def test_publish_regular_typed_signal_stays_durable() -> None:
    """A typed Done/Stance signal has no jetstream=False field -> durable by default."""
    bus = FleetBus(BusConfig(agent_id="heron"))
    await bus.connect()

    done = _env("done", typed_signal=Done(task_id="t1"))
    await bus.publish("dev.status", done)

    client = _StubNATS.instances[0]
    # Regular typed signal went durable (JetStream), not core.
    assert client.published == []
    assert client.jetstream_context.published == [("dev.status", done.to_bytes(), "FLEET_CORE")]


async def test_no_stream_or_consumer_is_created_by_the_policy_path() -> None:
    """The heartbeat/durable policy path never provisions streams or consumers.

    Publishing a typed heartbeat (core) and an ordinary envelope (durable) must
    only bind to the PRE-existing stream via find/publish — never create one.
    """
    bus = FleetBus(BusConfig(agent_id="heron"))
    await bus.connect()
    client = _StubNATS.instances[0]

    await bus.publish("dev.heartbeat", _env("heartbeat", typed_signal=_heartbeat()))
    await bus.publish("dev.status", _env("status"))

    js = client.jetstream_context
    # The stub JetStream exposes only find/publish/consumer_info/subscribe — there
    # is no add_stream / add_consumer call anywhere in the policy path.
    assert not hasattr(js, "add_stream")
    assert not hasattr(js, "add_consumer")
    # The durable publish bound to the pre-validated stream name, created nothing.
    assert js.published == [("dev.status", _env("status").to_bytes(), "FLEET_CORE")]


async def test_subscribe_defaults_durable_but_core_opt_out_decodes_and_survives_errors() -> None:
    seen: list[str] = []

    async def handler(envelope: Envelope) -> None:
        seen.append(envelope.kind)
        if envelope.kind == "boom":
            raise RuntimeError("handler failed")

    bus = FleetBus(BusConfig(agent_id="heron"))
    await bus.connect()

    with pytest.raises(DurableNotProvisionedError):
        await bus.subscribe("dev.events", handler)

    sub = await bus.subscribe("dev.events", handler, mode=DeliveryMode.CORE)
    assert isinstance(sub, _Subscription)
    callback = _StubNATS.instances[0].subscriptions[0][1]
    await callback(_Msg(b"not json"))
    await callback(_Msg(_env("ok").to_bytes()))
    await callback(_Msg(_env("boom").to_bytes()))

    assert seen == ["ok", "boom"]


async def test_subscribe_typed_heartbeat_and_presence_use_core_without_subject_inference() -> None:
    seen: list[str] = []

    async def handler(envelope: Envelope) -> None:
        seen.append(envelope.kind)

    bus = FleetBus(BusConfig(agent_id="heron"))
    await bus.connect()

    heartbeat_sub = await bus.subscribe("dev.anything", handler, typed_signal=_heartbeat())
    presence_sub = await bus.subscribe("dev.not-heartbeat", handler, typed_signal=_presence())

    assert isinstance(heartbeat_sub, _Subscription)
    assert isinstance(presence_sub, _Subscription)
    client = _StubNATS.instances[0]
    assert [subject for subject, _ in client.subscriptions] == ["dev.anything", "dev.not-heartbeat"]
    assert client.jetstream_context.subscriptions == []

    await client.subscriptions[0][1](_Msg(_env("heartbeat", typed_signal=_heartbeat()).to_bytes()))
    await client.subscriptions[1][1](_Msg(_env("presence", typed_signal=_presence()).to_bytes()))

    assert seen == ["heartbeat", "presence"]


async def test_subscribe_no_or_non_ephemeral_typed_signal_stays_durable() -> None:
    async def handler(envelope: Envelope) -> None:
        _ = envelope

    bus = FleetBus(BusConfig(agent_id="heron"))
    await bus.connect()

    with pytest.raises(DurableNotProvisionedError):
        await bus.subscribe("dev.heartbeat", handler)
    with pytest.raises(DurableNotProvisionedError):
        await bus.subscribe("dev.done", handler, typed_signal=Done(task_id="t1"))

    client = _StubNATS.instances[0]
    assert client.subscriptions == []
    assert client.jetstream_context.subscriptions == []


async def test_subscribe_explicit_mode_overrides_typed_signal() -> None:
    async def handler(envelope: Envelope) -> None:
        _ = envelope

    bus = FleetBus(BusConfig(agent_id="heron"))
    await bus.connect()

    with pytest.raises(DurableNotProvisionedError):
        await bus.subscribe(
            "dev.heartbeat", handler, typed_signal=_heartbeat(), mode=DeliveryMode.DURABLE
        )

    sub = await bus.subscribe(
        "dev.done", handler, typed_signal=Done(task_id="t1"), mode=DeliveryMode.CORE
    )

    assert isinstance(sub, _Subscription)
    client = _StubNATS.instances[0]
    assert [subject for subject, _ in client.subscriptions] == ["dev.done"]
    assert client.jetstream_context.subscriptions == []


async def test_durable_subscribe_requires_caller_supplied_consumer() -> None:
    bus = FleetBus(BusConfig(agent_id="heron"))
    await bus.connect()

    async def handler(envelope: Envelope) -> None:
        _ = envelope

    with pytest.raises(DurableNotProvisionedError, match="consumer identity"):
        await bus.subscribe("dev.events", handler)

    client = _StubNATS.instances[0]
    assert client.subscriptions == []
    assert client.jetstream_context.subscriptions == []


async def test_durable_subscribe_fails_loud_when_consumer_is_missing() -> None:
    bus = FleetBus(BusConfig(agent_id="heron"))
    await bus.connect()
    client = _StubNATS.instances[0]
    client.jetstream_context.consumer_result = NotFoundError(description="no consumer")

    async def handler(envelope: Envelope) -> None:
        _ = envelope

    with pytest.raises(DurableNotProvisionedError, match="no core-NATS fallback"):
        await bus.subscribe("dev.events", handler, consumer="worker-a")

    assert client.subscriptions == []
    assert client.jetstream_context.subscriptions == []


async def test_durable_subscribe_rejects_unbounded_consumer_max_deliver() -> None:
    bus = FleetBus(BusConfig(agent_id="heron"))
    await bus.connect()
    client = _StubNATS.instances[0]
    client.jetstream_context.consumer_result = SimpleNamespace(
        config=SimpleNamespace(max_deliver=-1)
    )

    async def handler(envelope: Envelope) -> None:
        _ = envelope

    with pytest.raises(DurableNotProvisionedError, match="finite MaxDeliver"):
        await bus.subscribe("dev.events", handler, consumer="worker-a")

    assert client.subscriptions == []
    assert client.jetstream_context.subscriptions == []


async def test_durable_subscribe_terms_poison_naks_failure_and_acks_success() -> None:
    seen: list[str] = []

    async def handler(envelope: Envelope) -> None:
        seen.append(envelope.kind)
        if envelope.kind == "boom":
            raise RuntimeError("handler failed")

    bus = FleetBus(BusConfig(agent_id="heron"))
    await bus.connect()

    await bus.subscribe("dev.events", handler, consumer="worker-a")

    client = _StubNATS.instances[0]
    assert client.subscriptions == []
    assert client.jetstream_context.subscriptions[0][:3] == (
        "dev.events",
        client.jetstream_context.subscriptions[0][1],
        "worker-a",
    )
    assert client.jetstream_context.subscriptions[0][3:] == ("FLEET_CORE", True)
    callback = client.jetstream_context.subscriptions[0][1]
    poison = _Msg(b"not json")
    ok = _Msg(_env("ok").to_bytes())
    failed = _Msg(_env("boom").to_bytes())

    await callback(poison)
    await callback(ok)
    await callback(failed)

    assert seen == ["ok", "boom"]
    assert poison.termed is True
    assert poison.acked is False
    assert poison.nak_delay is None
    assert ok.acked is True
    assert ok.termed is False
    assert ok.nak_delay is None
    assert failed.acked is False
    assert failed.termed is False
    assert failed.nak_delay == 1.0


async def test_raw_subscribe_core_opt_out_and_durable_caller_owned_ack() -> None:
    seen: list[bytes] = []

    async def handler(msg: _Msg) -> None:
        seen.append(msg.data)
        if msg.data == b"durable-raw":
            await msg.ack()

    bus = FleetBus(BusConfig(agent_id="heron"))
    await bus.connect()

    with pytest.raises(DurableNotProvisionedError):
        await bus.raw_subscribe("dev.raw", cast(Callable[[Msg], Awaitable[None]], handler))

    await bus.raw_subscribe(
        "dev.raw", cast(Callable[[Msg], Awaitable[None]], handler), consumer="raw-worker"
    )
    js_callback = _StubNATS.instances[0].jetstream_context.subscriptions[0][1]
    durable = _Msg(b"durable-raw")
    await js_callback(durable)

    await bus.raw_subscribe(
        "dev.raw", cast(Callable[[Msg], Awaitable[None]], handler), mode=DeliveryMode.CORE
    )
    callback = _StubNATS.instances[0].subscriptions[0][1]
    await callback(_Msg(b"raw"))
    assert seen == [b"durable-raw", b"raw"]
    assert durable.acked is True
    assert durable.nak_delay is None
    assert durable.termed is False


async def test_raw_subscribe_typed_heartbeat_uses_core_and_non_ephemeral_stays_durable() -> None:
    seen: list[bytes] = []

    async def handler(msg: _Msg) -> None:
        seen.append(msg.data)

    bus = FleetBus(BusConfig(agent_id="heron"))
    await bus.connect()

    core_sub = await bus.raw_subscribe(
        "dev.heartbeat.raw",
        cast(Callable[[Msg], Awaitable[None]], handler),
        typed_signal=_heartbeat(),
    )
    assert isinstance(core_sub, _Subscription)

    with pytest.raises(DurableNotProvisionedError):
        await bus.raw_subscribe(
            "dev.done.raw",
            cast(Callable[[Msg], Awaitable[None]], handler),
            typed_signal=Done(task_id="t1"),
        )

    client = _StubNATS.instances[0]
    assert [subject for subject, _ in client.subscriptions] == ["dev.heartbeat.raw"]
    assert client.jetstream_context.subscriptions == []

    await client.subscriptions[0][1](_Msg(b"beat"))
    assert seen == [b"beat"]


async def test_raw_subscribe_explicit_mode_overrides_typed_signal() -> None:
    async def handler(msg: _Msg) -> None:
        _ = msg

    bus = FleetBus(BusConfig(agent_id="heron"))
    await bus.connect()

    with pytest.raises(DurableNotProvisionedError):
        await bus.raw_subscribe(
            "dev.heartbeat.raw",
            cast(Callable[[Msg], Awaitable[None]], handler),
            typed_signal=_heartbeat(),
            mode=DeliveryMode.DURABLE,
        )

    sub = await bus.raw_subscribe(
        "dev.done.raw",
        cast(Callable[[Msg], Awaitable[None]], handler),
        typed_signal=Done(task_id="t1"),
        mode=DeliveryMode.CORE,
    )

    assert isinstance(sub, _Subscription)
    client = _StubNATS.instances[0]
    assert [subject for subject, _ in client.subscriptions] == ["dev.done.raw"]
    assert client.jetstream_context.subscriptions == []


async def test_raw_durable_subscribe_rejects_unbounded_consumer_max_deliver() -> None:
    bus = FleetBus(BusConfig(agent_id="heron"))
    await bus.connect()
    client = _StubNATS.instances[0]
    client.jetstream_context.consumer_result = SimpleNamespace(
        config=SimpleNamespace(max_deliver=None)
    )

    async def handler(msg: Msg) -> None:
        _ = msg

    with pytest.raises(DurableNotProvisionedError, match="finite MaxDeliver"):
        await bus.raw_subscribe("dev.raw", handler, consumer="raw-worker")

    assert client.subscriptions == []
    assert client.jetstream_context.subscriptions == []


async def test_subclass_policy_seam_still_runs_payload_guard() -> None:
    class CoreTransport(FleetBus):
        def delivery_mode_for(self, subject: str, envelope: Envelope) -> DeliveryMode:
            return DeliveryMode.CORE

    bus = CoreTransport(BusConfig(agent_id="heron"))
    await bus.connect()
    _StubNATS.instances[0].max_payload = 10

    with pytest.raises(PayloadTooLargeError):
        await bus.publish("dev.too-large", _env("status", payload={"body": "x" * 100}))

    assert _StubNATS.instances[0].published == []


async def test_payload_guard_covers_check_publish_request_and_reply() -> None:
    bus = FleetBus(BusConfig(agent_id="heron"))
    await bus.connect()
    _StubNATS.instances[0].max_payload = 10
    envelope = _env("heartbeat", payload={"body": "x" * 100})

    with pytest.raises(PayloadTooLargeError):
        bus.check_payload_size("dev.big", envelope)
    with pytest.raises(PayloadTooLargeError):
        await bus.publish("dev.big", envelope)
    with pytest.raises(PayloadTooLargeError):
        await bus.request("dev.big", envelope)
    with pytest.raises(PayloadTooLargeError):
        await bus.reply(cast(Msg, _Msg(reply="reply.to")), envelope)


async def test_request_preserves_none_semantics_and_raises_other_failures() -> None:
    bus = FleetBus(BusConfig(agent_id="heron"))
    await bus.connect()
    client = _StubNATS.instances[0]

    client.request_reply = _Msg(_env("reply").to_bytes())
    reply = await bus.request("dev.rpc", _env("request"), timeout_s=1.5)
    assert reply is not None
    assert reply.kind == "reply"
    assert client.requests[-1][2] == 1.5

    client.request_reply = NoRespondersError()
    assert await bus.request("dev.rpc", _env("request")) is None

    client.request_reply = NatsTimeoutError()
    assert await bus.request("dev.rpc", _env("request")) is None

    client.request_reply = _Msg(b"not json")
    assert await bus.request("dev.rpc", _env("request")) is None

    client.request_reply = RuntimeError("boom")
    with pytest.raises(RuntimeError, match="boom"):
        await bus.request("dev.rpc", _env("request"))


async def test_reply_without_reply_subject_drops_without_publish() -> None:
    bus = FleetBus(BusConfig(agent_id="heron"))
    await bus.connect()

    await bus.reply(cast(Msg, _Msg(reply="")), _env("heartbeat"))

    assert _StubNATS.instances[0].published == []


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _wait_for_port(port: int, *, timeout_s: float = 5.0) -> None:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.settimeout(0.2)
            if sock.connect_ex(("127.0.0.1", port)) == 0:
                return
        time.sleep(0.05)
    raise TimeoutError(f"nats-server did not open port {port} within {timeout_s}s")


@pytest.mark.skipif(
    shutil.which("nats-server") is None,
    reason="nats-server binary not on PATH - JetStream publish proof needs a real server",
)
async def test_durable_publish_stores_message_in_preprovisioned_jetstream_stream(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr("fleet_core.transport.NATS", RealNATS)
    port = _free_port()
    store = tmp_path / "js-store"
    proc = subprocess.Popen(
        [
            "nats-server",
            "-js",
            "-p",
            str(port),
            "-sd",
            str(store),
        ],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    try:
        _wait_for_port(port)
        cfg = BusConfig(
            agent_id="pytest-js",
            url=f"nats://127.0.0.1:{port}",
            env="dev",
            connect_timeout_s=1,
        )
        async with FleetBus(cfg) as bus:
            js = bus.jetstream()
            await js.add_stream(StreamConfig(name="FLEET_CORE_TEST", subjects=["dev.js.>"]))
            envelope = _env("status", payload={"proof": "jetstream"})

            await bus.publish("dev.js.status", envelope)

            stored = await js.get_last_msg("FLEET_CORE_TEST", "dev.js.status")
            assert stored.data is not None
            assert Envelope.from_bytes(stored.data) == envelope
    finally:
        proc.terminate()
        try:
            await asyncio.wait_for(asyncio.to_thread(proc.wait), timeout=5)
        except TimeoutError:
            proc.kill()
            await asyncio.to_thread(proc.wait)


@pytest.mark.skipif(
    shutil.which("nats-server") is None,
    reason="nats-server binary not on PATH - JetStream subscribe proof needs a real server",
)
async def test_durable_subscribe_binds_preprovisioned_consumer_and_acks_success_only(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr("fleet_core.transport.NATS", RealNATS)
    port = _free_port()
    store = tmp_path / "js-sub-store"
    proc = subprocess.Popen(
        [
            "nats-server",
            "-js",
            "-p",
            str(port),
            "-sd",
            str(store),
        ],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    try:
        _wait_for_port(port)
        cfg = BusConfig(
            agent_id="pytest-js",
            url=f"nats://127.0.0.1:{port}",
            env="dev",
            connect_timeout_s=1,
        )
        async with FleetBus(cfg) as bus:
            js = bus.jetstream()
            await js.add_stream(StreamConfig(name="FLEET_CORE_SUB_TEST", subjects=["dev.sub.>"]))
            await js.add_consumer(
                "FLEET_CORE_SUB_TEST",
                ConsumerConfig(
                    durable_name="worker-a",
                    deliver_subject="deliver.worker-a",
                    filter_subject="dev.sub.events",
                    ack_policy=AckPolicy.EXPLICIT,
                    ack_wait=30.0,
                    max_deliver=3,
                ),
            )
            seen: list[str] = []

            async def handler(envelope: Envelope) -> None:
                seen.append(envelope.kind)
                if envelope.kind == "boom":
                    raise RuntimeError("handler failed")

            sub = await bus.subscribe("dev.sub.events", handler, consumer="worker-a")
            await js.publish("dev.sub.events", _env("ok").to_bytes(), stream="FLEET_CORE_SUB_TEST")
            await js.publish("dev.sub.events", b"not json", stream="FLEET_CORE_SUB_TEST")
            await js.publish(
                "dev.sub.events", _env("boom").to_bytes(), stream="FLEET_CORE_SUB_TEST"
            )

            for _ in range(30):
                if seen == ["ok", "boom"]:
                    break
                await asyncio.sleep(0.1)

            await sub.unsubscribe()
            info = await js.consumer_info("FLEET_CORE_SUB_TEST", "worker-a")
            assert seen == ["ok", "boom"]
            assert info.num_ack_pending == 1
    finally:
        proc.terminate()
        try:
            await asyncio.wait_for(asyncio.to_thread(proc.wait), timeout=5)
        except TimeoutError:
            proc.kill()
            await asyncio.to_thread(proc.wait)


@pytest.mark.skipif(
    shutil.which("nats-server") is None,
    reason="nats-server binary not on PATH - JetStream poison proof needs a real server",
)
async def test_durable_subscribe_terms_poison_on_embedded_jetstream(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr("fleet_core.transport.NATS", RealNATS)
    port = _free_port()
    store = tmp_path / "js-poison-store"
    proc = subprocess.Popen(
        ["nats-server", "-js", "-p", str(port), "-sd", str(store)],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    try:
        _wait_for_port(port)
        cfg = BusConfig(
            agent_id="pytest-js",
            url=f"nats://127.0.0.1:{port}",
            env="dev",
            connect_timeout_s=1,
        )
        async with FleetBus(cfg) as bus:
            js = bus.jetstream()
            await js.add_stream(
                StreamConfig(name="FLEET_CORE_POISON_TEST", subjects=["dev.poison.>"])
            )
            await js.add_consumer(
                "FLEET_CORE_POISON_TEST",
                ConsumerConfig(
                    durable_name="worker-poison",
                    deliver_subject="deliver.worker-poison",
                    filter_subject="dev.poison.events",
                    ack_policy=AckPolicy.EXPLICIT,
                    ack_wait=0.5,
                    max_deliver=3,
                ),
            )
            seen: list[str] = []

            async def handler(envelope: Envelope) -> None:
                seen.append(envelope.kind)

            sub = await bus.subscribe("dev.poison.events", handler, consumer="worker-poison")
            await js.publish("dev.poison.events", b"not json", stream="FLEET_CORE_POISON_TEST")
            await asyncio.sleep(1.2)

            await sub.unsubscribe()
            info = await js.consumer_info("FLEET_CORE_POISON_TEST", "worker-poison")
            assert seen == []
            assert info.num_ack_pending == 0
            assert info.num_redelivered == 0
    finally:
        proc.terminate()
        try:
            await asyncio.wait_for(asyncio.to_thread(proc.wait), timeout=5)
        except TimeoutError:
            proc.kill()
            await asyncio.to_thread(proc.wait)


@pytest.mark.skipif(
    shutil.which("nats-server") is None,
    reason="nats-server binary not on PATH - JetStream retry proof needs a real server",
)
async def test_durable_subscribe_naks_handler_failure_then_acks_retry(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr("fleet_core.transport.NATS", RealNATS)
    port = _free_port()
    store = tmp_path / "js-retry-store"
    proc = subprocess.Popen(
        ["nats-server", "-js", "-p", str(port), "-sd", str(store)],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    try:
        _wait_for_port(port)
        cfg = BusConfig(
            agent_id="pytest-js",
            url=f"nats://127.0.0.1:{port}",
            env="dev",
            connect_timeout_s=1,
        )
        async with FleetBus(cfg) as bus:
            js = bus.jetstream()
            await js.add_stream(
                StreamConfig(name="FLEET_CORE_RETRY_TEST", subjects=["dev.retry.>"])
            )
            await js.add_consumer(
                "FLEET_CORE_RETRY_TEST",
                ConsumerConfig(
                    durable_name="worker-retry",
                    deliver_subject="deliver.worker-retry",
                    filter_subject="dev.retry.events",
                    ack_policy=AckPolicy.EXPLICIT,
                    ack_wait=0.5,
                    max_deliver=2,
                ),
            )
            attempts: list[str] = []

            async def handler(envelope: Envelope) -> None:
                attempts.append(envelope.kind)
                if len(attempts) == 1:
                    raise RuntimeError("first attempt fails")

            sub = await bus.subscribe("dev.retry.events", handler, consumer="worker-retry")
            await js.publish(
                "dev.retry.events", _env("retry").to_bytes(), stream="FLEET_CORE_RETRY_TEST"
            )

            for _ in range(30):
                if attempts == ["retry", "retry"]:
                    break
                await asyncio.sleep(0.1)

            await sub.unsubscribe()
            info = await js.consumer_info("FLEET_CORE_RETRY_TEST", "worker-retry")
            assert attempts == ["retry", "retry"]
            assert info.num_ack_pending == 0
    finally:
        proc.terminate()
        try:
            await asyncio.wait_for(asyncio.to_thread(proc.wait), timeout=5)
        except TimeoutError:
            proc.kill()
            await asyncio.to_thread(proc.wait)


@pytest.mark.skipif(
    shutil.which("nats-server") is None,
    reason="nats-server binary not on PATH - raw durable proof needs a real server",
)
async def test_raw_durable_subscribe_caller_ack_and_no_ack_bound(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr("fleet_core.transport.NATS", RealNATS)
    port = _free_port()
    store = tmp_path / "js-raw-store"
    proc = subprocess.Popen(
        ["nats-server", "-js", "-p", str(port), "-sd", str(store)],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    try:
        _wait_for_port(port)
        cfg = BusConfig(
            agent_id="pytest-js",
            url=f"nats://127.0.0.1:{port}",
            env="dev",
            connect_timeout_s=1,
        )
        async with FleetBus(cfg) as bus:
            js = bus.jetstream()
            await js.add_stream(StreamConfig(name="FLEET_CORE_RAW_TEST", subjects=["dev.raw.>"]))
            await js.add_consumer(
                "FLEET_CORE_RAW_TEST",
                ConsumerConfig(
                    durable_name="worker-raw-ack",
                    deliver_subject="deliver.worker-raw-ack",
                    filter_subject="dev.raw.ack",
                    ack_policy=AckPolicy.EXPLICIT,
                    ack_wait=0.5,
                    max_deliver=2,
                ),
            )
            await js.add_consumer(
                "FLEET_CORE_RAW_TEST",
                ConsumerConfig(
                    durable_name="worker-raw-noack",
                    deliver_subject="deliver.worker-raw-noack",
                    filter_subject="dev.raw.noack",
                    ack_policy=AckPolicy.EXPLICIT,
                    ack_wait=0.5,
                    max_deliver=2,
                ),
            )
            ack_seen: list[bytes] = []
            no_ack_seen: list[bytes] = []

            async def ack_handler(msg: Msg) -> None:
                ack_seen.append(msg.data)
                await msg.ack()

            async def no_ack_handler(msg: Msg) -> None:
                no_ack_seen.append(msg.data)

            ack_sub = await bus.raw_subscribe("dev.raw.ack", ack_handler, consumer="worker-raw-ack")
            no_ack_sub = await bus.raw_subscribe(
                "dev.raw.noack", no_ack_handler, consumer="worker-raw-noack"
            )
            await js.publish("dev.raw.ack", b"ack", stream="FLEET_CORE_RAW_TEST")
            await js.publish("dev.raw.noack", b"noack", stream="FLEET_CORE_RAW_TEST")

            for _ in range(30):
                if ack_seen == [b"ack"] and len(no_ack_seen) >= 2:
                    break
                await asyncio.sleep(0.1)
            await asyncio.sleep(0.8)

            await ack_sub.unsubscribe()
            await no_ack_sub.unsubscribe()
            ack_info = await js.consumer_info("FLEET_CORE_RAW_TEST", "worker-raw-ack")
            no_ack_info = await js.consumer_info("FLEET_CORE_RAW_TEST", "worker-raw-noack")
            assert ack_seen == [b"ack"]
            assert len(no_ack_seen) == 2
            assert ack_info.num_ack_pending == 0
            assert no_ack_info.config.max_deliver == 2
    finally:
        proc.terminate()
        try:
            await asyncio.wait_for(asyncio.to_thread(proc.wait), timeout=5)
        except TimeoutError:
            proc.kill()
            await asyncio.to_thread(proc.wait)
