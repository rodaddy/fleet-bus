"""Unit tests for the NatsAdapter against a gateway stub + fake bus.

No real Hermes gateway and no network: a stub gateway API (satisfying the shape
the adapter uses) and a fake FleetBus prove the subscribe wiring, the
envelope→MessageEvent forging (internal=True), and the send()→publish path.
"""

from __future__ import annotations

import json
import threading
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import urlparse

import pytest
from fleet_nats import BusConfig, Done, Envelope, Stance, StancePosition, subjects

from fleet_subscriber import build_adapter_class

# --- Gateway stub: the slice of the Hermes runtime the adapter touches --------


@dataclass
class StubSessionSource:
    platform: Any
    chat_id: str
    chat_type: str = "channel"
    thread_id: str | None = None
    user_id: str | None = None


@dataclass
class StubMessageEvent:
    text: str
    message_type: Any = None
    source: Any = None
    internal: bool = False


@dataclass
class StubSendResult:
    success: bool
    message_id: str | None = None


class StubMessageType:
    TEXT = "text"


class StubPlatform:
    def __init__(self, name: str) -> None:
        self.name = name

    def __eq__(self, other: object) -> bool:
        return isinstance(other, StubPlatform) and other.name == self.name

    def __hash__(self) -> int:
        return hash(self.name)


class StubBasePlatformAdapter:
    """Stand-in for Hermes BasePlatformAdapter — records forged events."""

    def __init__(self, config: Any, platform: Any) -> None:
        self.config = config
        self.platform = platform
        self._running = False
        self._background_tasks: set[Any] = set()
        self.handled: list[Any] = []

    async def handle_message(self, event: Any) -> None:
        self.handled.append(event)

    async def cancel_background_tasks(self) -> None:
        self._background_tasks.clear()

    def is_connected(self) -> bool:
        return self._running


class StubApi:
    BasePlatformAdapter = StubBasePlatformAdapter
    MessageEvent = StubMessageEvent
    MessageType = StubMessageType
    SendResult = StubSendResult
    SessionSource = StubSessionSource
    Platform = StubPlatform


# --- Fake bus -----------------------------------------------------------------


@dataclass
class FakeBus:
    config: BusConfig
    subscriptions: dict[str, Any] = field(default_factory=dict)
    published: list[tuple[str, Envelope]] = field(default_factory=list)
    connected: bool = False
    created_buses: list[FakeBus] = field(default_factory=list, repr=False)

    async def connect(self) -> None:
        self.connected = True

    async def close(self) -> None:
        self.connected = False
        self.subscriptions.clear()

    async def subscribe(self, subject: str, handler: Any) -> Any:
        self.subscriptions[subject] = handler
        return object()

    async def publish(self, subject: str, envelope: Envelope) -> None:
        self.published.append((subject, envelope))


@dataclass
class StubConfig:
    home_channel: str | None = None


def _assert_noise_only(
    fake: FakeBus, *, task_id: str = "task-1", channel: str = "dash-dev"
) -> None:
    """Assert the send produced ONLY noise-lane traffic — no coordination at all.

    The invariant these call sites have always asserted is "this non-actionable
    send does NOT reach the status lane and does NOT complete the turn". Before
    #116 that was expressed as ``fake.published == []`` because the send was
    DISCARDED. It is now recorded on its own ack/telemetry lane instead, so the
    assertion is stated directly: nothing on ``status``, no ``done`` sentinel,
    and exactly one envelope, on a noise subject.
    """
    noise_subjects = {
        subjects.agent_ack("dev", task_id, channel=channel),
        subjects.agent_telemetry("dev", task_id, channel=channel),
    }
    published = [(subject, env) for subject, env in fake.published]
    assert [subject for subject, _ in published] and all(
        subject in noise_subjects for subject, _ in published
    ), published
    # The coordination contract is untouched: no status envelope, no completion.
    assert all(env.kind in {"ack", "telemetry"} for _, env in published), published
    assert not any(".status." in subject for subject, _ in published), published


def _adapter(
    monkeypatch: pytest.MonkeyPatch,
    *,
    agent: str = "wren",
    role: str | None = None,
    home_channel: str | None = "wren-home",
) -> tuple[Any, FakeBus]:
    monkeypatch.setenv("FLEET_AGENT_ID", agent)
    monkeypatch.setenv("FLEET_ENV", "dev")
    monkeypatch.setenv("FLEET_NATS_URL", "nats://192.0.2.74:4222")
    if role is not None:
        monkeypatch.setenv("FLEET_ROLE", role)
    else:
        monkeypatch.delenv("FLEET_ROLE", raising=False)

    cls = build_adapter_class(StubApi())
    # Default keeps every existing test on the explicit-home path; passing None
    # reproduces the real Hermes PlatformConfig, which has no home_channel field.
    adapter = cls(StubConfig(home_channel=home_channel))

    fake = FakeBus(config=BusConfig.from_env(agent))
    fake.created_buses.append(fake)
    # Swap the real FleetBus the adapter builds in connect() for the fake.
    import fleet_subscriber.adapter as adapter_mod

    first_issued = False

    def _new_fake_bus(config: BusConfig) -> FakeBus:
        nonlocal first_issued
        if not first_issued:
            first_issued = True
            return fake
        replacement = FakeBus(config=config, created_buses=fake.created_buses)
        fake.created_buses.append(replacement)
        return replacement

    monkeypatch.setattr(adapter_mod, "FleetBus", _new_fake_bus)

    # Adapter behavior tests do not provision a real JetStream store.  Keep the
    # transport fixture released; dedicated kill-watch tests exercise the actual
    # fail-closed source and cancellation boundary.
    class _ReleasedWatch:
        def start(self) -> None:
            return None

        def stop(self) -> None:
            return None

        def armed(self) -> bool:
            return False

    monkeypatch.setattr(
        adapter_mod,
        "build_worker_kill_watch",
        lambda config, *, poll_s, on_arm: _ReleasedWatch(),
    )
    return adapter, fake


async def test_worker_connect_subscribes_to_all_four_lanes(monkeypatch: pytest.MonkeyPatch) -> None:
    # Default role is WORKER (FLEET_ROLE unset) → the four work lanes. Every
    # CHANNEL-SCOPABLE lane needs BOTH forms: the orchestrator publishes on the
    # per-channel subject, which the bare form can never match (per-token NATS
    # matching). dispatch has always been scoped; relay and wake joined it in
    # #117, so they carry wildcards too. inbox addresses an AGENT, not a
    # channel's deliberation, so it is deliberately single-form.
    adapter, fake = _adapter(monkeypatch, agent="wren")
    ok = await adapter.connect()
    assert ok is True
    assert adapter.is_connected() is True
    assert set(fake.subscriptions) == {
        subjects.agent_dispatch("dev", "wren"),
        subjects.agent_dispatch_wildcard("dev", "wren"),
        subjects.agent_relay("dev", "wren"),
        subjects.agent_relay_wildcard("dev", "wren"),
        subjects.agent_wake("dev", "wren"),
        subjects.agent_wake_wildcard("dev", "wren"),
        subjects.agent_inbox("dev", "wren"),
    }


async def test_manager_connect_subscribes_to_inbox_only(monkeypatch: pytest.MonkeyPatch) -> None:
    # FLEET_ROLE=manager (Heron) → adapter subscribes only to its own inbox; the
    # task-keyed status/arbitrate lanes are the orchestrator's, not the adapter's.
    adapter, fake = _adapter(monkeypatch, agent="heron", role="manager")
    ok = await adapter.connect()
    assert ok is True
    assert set(fake.subscriptions) == {subjects.agent_inbox("dev", "heron")}
    # A manager must NOT be subscribed to worker dispatch/relay/wake lanes.
    assert subjects.agent_dispatch("dev", "heron") not in fake.subscriptions
    assert subjects.agent_relay("dev", "heron") not in fake.subscriptions
    assert subjects.agent_wake("dev", "heron") not in fake.subscriptions


async def test_connect_failure_returns_false(monkeypatch: pytest.MonkeyPatch) -> None:
    adapter, fake = _adapter(monkeypatch)

    async def boom() -> None:
        raise ConnectionError("bus down")

    fake.connect = boom  # type: ignore[method-assign]
    assert await adapter.connect() is False
    assert adapter.is_connected() is False


async def test_initial_connect_without_reconnect_keyword_uses_normal_path(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter, fake = _adapter(monkeypatch)

    assert await adapter.connect() is True
    assert adapter.is_connected() is True
    assert fake.connected is True

    await adapter.disconnect()


async def test_connect_accepts_hermes_reconnect_keyword(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter, fake = _adapter(monkeypatch)
    assert await adapter.connect() is True
    first_task = adapter._heartbeat_task
    assert first_task is not None

    assert await adapter.connect(is_reconnect=True) is True
    replacement = fake.created_buses[1]
    assert adapter.is_connected() is True
    assert fake.connected is False
    assert replacement.connected is True
    assert adapter._bus is replacement
    assert adapter._heartbeat_task is not first_task
    assert first_task.cancelled() or first_task.done()

    await adapter.disconnect()


async def test_reconnect_never_orphans_the_previous_heartbeat_task(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # A second connect() must tear down the first emitter — an orphaned task
    # would beat against the stale bus forever, unreachable via disconnect().
    adapter, fake = _adapter(monkeypatch)
    assert await adapter.connect() is True
    first_task = adapter._heartbeat_task
    first_emitter = adapter._heartbeat
    assert first_task is not None and first_emitter is not None

    assert await adapter.connect() is True
    assert adapter._heartbeat_task is not first_task
    assert first_task.cancelled() or first_task.done()

    await adapter.disconnect()
    assert adapter._heartbeat_task is None


async def test_failed_reconnect_closes_transports_and_reports_disconnected(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter, first = _adapter(monkeypatch)
    assert await adapter.connect() is True

    import fleet_subscriber.adapter as adapter_mod

    replacement = FakeBus(config=first.config)

    async def fail_after_open() -> None:
        replacement.connected = True
        raise ConnectionError("replacement bus down")

    replacement.connect = fail_after_open  # type: ignore[method-assign]
    monkeypatch.setattr(adapter_mod, "FleetBus", lambda config: replacement)

    assert await adapter.connect(is_reconnect=True) is False
    assert adapter.is_connected() is False
    assert adapter._bus is None
    assert adapter._heartbeat is None
    assert adapter._heartbeat_task is None
    assert adapter._kill_watch is None
    assert adapter._kill_loop is None
    assert first.connected is False
    assert replacement.connected is False
    assert first.subscriptions == {}
    assert replacement.subscriptions == {}


async def test_connect_bad_heartbeat_interval_returns_false_before_bus(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # connect()'s contract is False + log on ANY failure. A typo'd interval must
    # fail BEFORE the bus opens — never raise mid-wiring with live subscriptions.
    adapter, fake = _adapter(monkeypatch)
    monkeypatch.setenv("FLEET_HEARTBEAT_SECONDS", "soon")
    assert await adapter.connect() is False
    assert adapter.is_connected() is False
    assert fake.subscriptions == {}


async def test_inbound_envelope_forges_internal_message(monkeypatch: pytest.MonkeyPatch) -> None:
    adapter, fake = _adapter(monkeypatch, agent="wren")
    await adapter.connect()
    handler = fake.subscriptions[subjects.agent_dispatch("dev", "wren")]

    env = Envelope.new(
        msg_id="d1",
        ts="t",
        sender="heron",
        kind="dispatch",
        task_id="task-1",
        channel="dash-dev",
        payload={"text": "Do the thing."},
    )
    await handler(env)

    assert len(adapter.handled) == 1
    event = adapter.handled[0]
    assert event.internal is True  # bypasses mention-gate + auth
    assert event.text == "Do the thing."
    assert event.source.chat_id == "dash-dev"
    assert event.source.platform == StubPlatform("nats")
    assert event.source.user_id == "wren"


async def test_dedup_duplicate_dispatch_forges_one_turn(monkeypatch: pytest.MonkeyPatch) -> None:
    # App-level duplicate (transport is at-most-once, C5): the same envelope id
    # must never run twice.
    adapter, fake = _adapter(monkeypatch, agent="wren")
    await adapter.connect()
    handler = fake.subscriptions[subjects.agent_dispatch("dev", "wren")]
    env = Envelope.new(
        msg_id="dup-1",
        ts="t",
        sender="heron",
        kind="dispatch",
        task_id="task-1",
        channel="dash-dev",
        payload={"text": "Do the thing."},
    )
    await handler(env)
    await handler(env)  # duplicate → logged no-op
    assert len(adapter.handled) == 1


async def test_dead_letter_poison_envelope_keeps_subscriber_up(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Unprocessable input → sanitized record on the dead-letter lane; alive after.
    adapter, fake = _adapter(monkeypatch, agent="wren")
    await adapter.connect()
    import fleet_subscriber.adapter as adapter_mod

    def _boom(
        envelope: Envelope,
        *,
        channels: object = None,
        home_channel: str | None = None,
        synthetic_user_id: str = "fleet-nats",
    ) -> None:
        raise ValueError("unmappable for the test")

    monkeypatch.setattr(adapter_mod, "envelope_to_message", _boom)
    handler = fake.subscriptions[subjects.agent_dispatch("dev", "wren")]
    poison = Envelope.new(
        msg_id="poison-1", ts="t", sender="heron", kind="dispatch", payload={"text": "x"}
    )
    await handler(poison)  # must not raise

    dead = [(s, e) for s, e in fake.published if s == subjects.dead_letter("dev")]
    assert len(dead) == 1
    _, record = dead[0]
    assert record.kind == "dead_letter"
    assert record.payload["source_id"] == "poison-1"
    assert record.payload["source_subject"] == subjects.agent_dispatch("dev", "wren")
    assert "text" not in str(record.payload)  # never the raw payload
    # Reason carries the exception CLASS ONLY — downstream raisers embed the
    # offending input (the turn text) in str(exc), which must never leak here.
    assert record.payload["reason"] == "ValueError"
    assert "unmappable" not in str(record.payload)
    assert adapter.is_connected() is True
    assert adapter.handled == []


async def test_unactionable_envelope_is_skipped(monkeypatch: pytest.MonkeyPatch) -> None:
    adapter, fake = _adapter(monkeypatch, agent="wren")
    await adapter.connect()
    handler = fake.subscriptions[subjects.agent_relay("dev", "wren")]
    # No text → nothing to run.
    env = Envelope.new(
        msg_id="r1", ts="t", sender="heron", kind="relay", channel="dash-dev", payload={}
    )
    await handler(env)
    assert adapter.handled == []


async def test_send_without_known_task_falls_back_to_channel_key(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # A bare send() with no prior inbound (no task correlation) keys on the channel.
    adapter, fake = _adapter(monkeypatch, agent="wren")
    await adapter.connect()
    result = await adapter.send("dash-dev", "the agent's reply", metadata={"thread_id": "th-1"})
    assert result.success is True
    subject, env = fake.published[0]
    assert subject == subjects.agent_status("dev", "dash-dev", channel="dash-dev")
    assert env.payload["text"] == "the agent's reply"
    assert env.payload["thread_id"] == "th-1"
    assert env.ts != ""  # a real wire timestamp, not the empty-string bug


async def test_reply_routes_to_the_task_status_subject(monkeypatch: pytest.MonkeyPatch) -> None:
    # THE loop: an inbound dispatch for task-1 → forge turn → send() must publish
    # the reply on agent_status(env, "task-1") — the SAME subject the orchestrator
    # watches — not agent_status(env, <channel>). Regression test for the bug where
    # the reply was keyed on the channel and the manager would never receive it.
    adapter, fake = _adapter(monkeypatch, agent="wren")
    await adapter.connect()
    handler = fake.subscriptions[subjects.agent_dispatch("dev", "wren")]
    await handler(
        Envelope.new(
            msg_id="d1",
            ts="t",
            sender="heron",
            kind="dispatch",
            task_id="task-1",
            channel="dash-dev",
            payload={"text": "do it"},
        )
    )
    # The gateway would call send() with the channel as chat_id; the adapter must
    # recover task-1 from its correlation map.
    await adapter.send("dash-dev", _turn("done, here's the answer", done=True))
    subject, env = fake.published[0]
    assert subject == subjects.agent_status("dev", "task-1", channel="dash-dev")
    assert env.task_id == "task-1"


@pytest.mark.parametrize("hostile_channel", ["*", ">", "a.*"])
async def test_wildcard_inbound_channel_replies_on_a_deliverable_subject(
    monkeypatch: pytest.MonkeyPatch, hostile_channel: str
) -> None:
    """A publisher-controlled wildcard channel must not poison the reply subject.

    ``mapping.chat_id = envelope.channel``, so the dispatcher chooses this token.
    ``subjects.slug`` deliberately permits ``*`` (authz grant patterns need it),
    so ``channel="*"`` would build ``dev.agent.*.status.task-1`` — byte-identical
    to the hub's ``agent_status_wildcard`` subscription and INVALID as a publish
    subject. NATS rejects a wildcard publish asynchronously, so the reply would
    silently vanish and the task would hang to timeout with no error at the call
    site. The reply must fall back to the bare form, which the hub also
    subscribes, so the status and the done sentinel still land.
    """
    adapter, fake = _adapter(monkeypatch, agent="wren")
    await adapter.connect()
    handler = fake.subscriptions[subjects.agent_dispatch("dev", "wren")]
    await handler(
        Envelope.new(
            msg_id="d1",
            ts="t",
            sender="heron",
            kind="dispatch",
            task_id="task-1",
            channel=hostile_channel,
            payload={"text": "do it"},
        )
    )

    await adapter.send(hostile_channel, _turn("done, here's the answer", done=True))

    assert fake.published, "the reply must still be published, not dropped"
    for subject, _ in fake.published:
        assert "*" not in subject and ">" not in subject, subject
        # Deliverable AND still the subject the hub watches for this task.
        assert subject == subjects.agent_status("dev", "task-1")
    assert subjects.agent_status_wildcard("dev", "task-1") not in [s for s, _ in fake.published]


async def _dispatch(handler: Any, *, task_id: str, channel: str, text: str = "do it") -> None:
    await handler(
        Envelope.new(
            msg_id=f"d-{task_id}",
            ts="t",
            sender="heron",
            kind="dispatch",
            task_id=task_id,
            channel=channel,
            payload={"text": text},
        )
    )


def _turn(
    visible_text: str,
    *,
    done: bool = False,
    outcome: str | None = None,
    stance: str | None = None,
    fact_pull: bool = False,
    reason: str | None = None,
) -> str:
    payload: dict[str, Any] = {"visible_text": visible_text}
    if done:
        payload["done"] = done
    if outcome is not None:
        payload["outcome"] = outcome
    if stance is not None:
        payload["stance"] = stance
        payload["fact_pull"] = fact_pull
    if reason is not None:
        payload["reason"] = reason
    return json.dumps(payload)


async def test_correlated_turn_publishes_only_the_completed_send(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Ack/progress sends lack the explicit completion contract and must reach
    # neither the STATUS lane nor the Discord tee. The completed reply publishes
    # once, followed only by the existing control-plane done sentinel.
    #
    # #116 adds the other half of the contract: the noise is no longer DISCARDED,
    # it is recorded on its own lane. So this test now pins both directions —
    # the coordination lane sees exactly two envelopes, and the two noise sends
    # are still fully recoverable from the ack/telemetry lanes.
    adapter, fake = _adapter(monkeypatch, agent="wren")
    await adapter.connect()
    handler = fake.subscriptions[subjects.agent_dispatch("dev", "wren")]
    await _dispatch(handler, task_id="task-1", channel="dash-dev")

    await adapter.send("dash-dev", "ack: on it")
    await adapter.send("dash-dev", "progress: halfway")
    await adapter.send("dash-dev", _turn("the real answer", done=True))

    task_subject = subjects.agent_status("dev", "task-1", channel="dash-dev")
    status_lane = [(s, e) for s, e in fake.published if s == task_subject]
    assert [s for s, _ in status_lane] == [task_subject, task_subject]
    assert all(env.task_id == "task-1" for _, env in status_lane)
    assert [env.kind for _, env in status_lane] == ["status", "done"]
    assert status_lane[0][1].payload["text"] == "the real answer"

    # The two noise sends landed on the ack lane — logged and mineable, and NOT on
    # the status lane the orchestrator and the Discord bridge consume.
    ack_subject = subjects.agent_ack("dev", "task-1", channel="dash-dev")
    noise = [(s, e) for s, e in fake.published if s == ack_subject]
    assert [e.payload["text"] for _, e in noise] == ["ack: on it", "progress: halfway"]
    assert all(e.kind == "ack" for _, e in noise)
    # Nothing else was published anywhere.
    assert len(fake.published) == len(status_lane) + len(noise)


async def test_map_bounded_by_channel_new_dispatch_overwrites(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # A new dispatch on the same channel supersedes the old correlation — the map
    # holds at most one entry per channel (bounded growth), and sends after the new
    # dispatch route to the new task.
    adapter, fake = _adapter(monkeypatch, agent="wren")
    await adapter.connect()
    handler = fake.subscriptions[subjects.agent_dispatch("dev", "wren")]
    await _dispatch(handler, task_id="task-1", channel="dash-dev")
    await _dispatch(handler, task_id="task-2", channel="dash-dev")  # supersedes task-1
    await adapter.send("dash-dev", _turn("reply", done=True))
    subject, env = fake.published[0]
    assert subject == subjects.agent_status("dev", "task-2", channel="dash-dev")
    assert env.task_id == "task-2"
    assert adapter._channel_task == {}  # completed correlation is released


async def test_send_without_bus_returns_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    # The no-bus drop path returns SendResult(success=False), not a crash.
    adapter, _ = _adapter(monkeypatch, agent="wren")
    # send() before connect() → self._bus is None.
    result = await adapter.send("dash-dev", "reply with no bus")
    assert result.success is False


async def test_typed_completion_emits_done_sentinel(monkeypatch: pytest.MonkeyPatch) -> None:
    # A typed completion posts the visible reply then emits a kind="done" sentinel
    # on the task's status subject (issue #6), and frees the correlation.
    adapter, fake = _adapter(monkeypatch, agent="wren")
    await adapter.connect()
    handler = fake.subscriptions[subjects.agent_dispatch("dev", "wren")]
    await _dispatch(handler, task_id="task-1", channel="dash-dev")

    await adapter.send(
        "dash-dev", _turn("Here is the final answer.", done=True, outcome="completed")
    )

    task_subject = subjects.agent_status("dev", "task-1", channel="dash-dev")
    # Two publishes: the clean status reply, then the done sentinel.
    assert len(fake.published) == 2
    status_subj, status_env = fake.published[0]
    done_subj, done_env = fake.published[1]
    assert status_subj == task_subject
    assert status_env.kind == "status"
    assert status_env.payload["text"] == "Here is the final answer."
    assert done_subj == task_subject
    assert done_env.kind == "done"
    assert done_env.task_id == "task-1"
    assert done_env.payload == {
        "header": {"actor": "wren"},
        "data": {"task_id": "task-1", "outcome": "completed"},
        "extras": {},
        "footer": {},
    }
    # Turn over → correlation dropped.
    assert "dash-dev" not in adapter._channel_task


async def test_intermediate_correlated_reply_emits_nothing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter, fake = _adapter(monkeypatch, agent="wren")
    await adapter.connect()
    handler = fake.subscriptions[subjects.agent_dispatch("dev", "wren")]
    await _dispatch(handler, task_id="task-1", channel="dash-dev")
    await adapter.send("dash-dev", "still working on it...")  # no trailer
    # No status and no done sentinel; correlation remains for the final send.
    # The non-actionable send is recorded on its own noise lane instead (#116).
    _assert_noise_only(fake)
    assert adapter._channel_task["dash-dev"] == "task-1"


async def test_typed_stance_rides_payload_meta(monkeypatch: pytest.MonkeyPatch) -> None:
    # the agent runtime: a typed stance rides {"stance", "fact_pull"} on
    # payload.meta — the EXACT shape the orchestrator's parse_turn_signal consumes.
    adapter, fake = _adapter(monkeypatch, agent="wren")
    await adapter.connect()
    handler = fake.subscriptions[subjects.agent_dispatch("dev", "wren")]
    await _dispatch(handler, task_id="task-1", channel="dash-dev")

    await adapter.send(
        "dash-dev",
        _turn("The metric regressed.", done=True, stance="dissent", fact_pull=True),
    )

    subject, env = fake.published[0]
    assert subject == subjects.agent_status("dev", "task-1", channel="dash-dev")
    assert env.kind == "status"
    assert env.payload["text"] == "The metric regressed."
    assert env.payload["meta"] == {"stance": "dissent", "fact_pull": True}


async def test_stance_meta_matches_parse_turn_signal_wire_shape(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Contract test: the meta this subscriber emits organically must be EXACTLY the
    # shape the orchestrator's fleet_orchestrator.score.parse_turn_signal consumes
    # (the harness-injected shape from rung8 inject-signed-turns.py, now organic):
    # payload.meta = {"stance": <one of agree/dissent/neutral>, "fact_pull": bool},
    # with BOTH keys present (parse_turn_signal rejects a half-signed meta). Asserted
    # against the wire dict directly so this suite has no cross-package dep.
    adapter, fake = _adapter(monkeypatch, agent="finch")
    await adapter.connect()
    handler = fake.subscriptions[subjects.agent_dispatch("dev", "finch")]
    await _dispatch(handler, task_id="task-1", channel="dash-dev")
    await adapter.send(
        "dash-dev",
        _turn("I dissent and pulled a fact.", done=True, stance="dissent", fact_pull=True),
    )

    _, env = fake.published[0]
    meta = env.payload["meta"]
    assert set(meta) >= {"stance", "fact_pull"}  # both present → not half-signed
    assert meta["stance"] in {"agree", "dissent", "neutral"}
    assert isinstance(meta["fact_pull"], bool)
    assert env.sender == "finch"  # parse_turn_signal reads agent_id off the sender
    assert env.task_id == "task-1"


async def test_stance_reason_rides_meta_but_orchestrator_ignores_it(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter, fake = _adapter(monkeypatch, agent="wren")
    await adapter.connect()
    handler = fake.subscriptions[subjects.agent_dispatch("dev", "wren")]
    await _dispatch(handler, task_id="task-1", channel="dash-dev")
    await adapter.send(
        "dash-dev",
        _turn("See the log.", done=True, stance="agree", reason="logs confirm it"),
    )
    _, env = fake.published[0]
    assert env.payload["meta"] == {
        "stance": "agree",
        "fact_pull": False,
        "reason": "logs confirm it",
    }


async def test_no_stance_trailer_emits_no_meta(monkeypatch: pytest.MonkeyPatch) -> None:
    # Absence: an ordinary reply carries NO meta key (an UNSIGNED turn — the normal
    # case), so the orchestrator's parse_turn_signal returns None, not a bad signal.
    adapter, fake = _adapter(monkeypatch, agent="wren")
    await adapter.connect()
    handler = fake.subscriptions[subjects.agent_dispatch("dev", "wren")]
    await _dispatch(handler, task_id="task-1", channel="dash-dev")
    await adapter.send("dash-dev", _turn("just my thoughts, no stance signed", done=True))
    _, env = fake.published[0]
    assert "meta" not in env.payload


async def test_malformed_typed_stance_leaves_turn_unsigned(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # A malformed typed stance must not emit a stance the orchestrator would reject.
    # parse_turn rejects the JSON object, so the coordinated send has no signal.
    adapter, fake = _adapter(monkeypatch, agent="wren")
    await adapter.connect()
    handler = fake.subscriptions[subjects.agent_dispatch("dev", "wren")]
    await _dispatch(handler, task_id="task-1", channel="dash-dev")
    await adapter.send("dash-dev", _turn("hmm", done=True, stance="maybe"))
    _assert_noise_only(fake)


async def test_typed_stance_and_done_coexist(monkeypatch: pytest.MonkeyPatch) -> None:
    # A typed turn can BOTH sign a stance and complete; the stance rides meta and
    # the done sentinel is still emitted.
    adapter, fake = _adapter(monkeypatch, agent="wren")
    await adapter.connect()
    handler = fake.subscriptions[subjects.agent_dispatch("dev", "wren")]
    await _dispatch(handler, task_id="task-1", channel="dash-dev")

    await adapter.send(
        "dash-dev",
        _turn("Final call: agree.", done=True, outcome="completed", stance="agree", fact_pull=True),
    )

    assert len(fake.published) == 2
    _, status_env = fake.published[0]
    _, done_env = fake.published[1]
    assert status_env.kind == "status"
    assert status_env.payload["text"] == "Final call: agree."
    assert status_env.payload["meta"] == {"stance": "agree", "fact_pull": True}
    assert done_env.kind == "done"
    assert done_env.payload["data"]["outcome"] == "completed"


async def test_get_chat_info_returns_channel_shape(monkeypatch: pytest.MonkeyPatch) -> None:
    adapter, _ = _adapter(monkeypatch, agent="wren")
    info = await adapter.get_chat_info("dash-dev")
    assert info == {"id": "dash-dev", "type": "channel"}


async def test_disconnect_closes_bus(monkeypatch: pytest.MonkeyPatch) -> None:
    adapter, fake = _adapter(monkeypatch, agent="wren")
    await adapter.connect()
    assert fake.connected is True
    await adapter.disconnect()
    assert fake.connected is False
    assert adapter.is_connected() is False


async def test_connect_subscribe_failure_returns_false_and_closes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # connect()'s contract is return-False-with-log on ANY failure. A subscribe
    # raising mid-loop must not escape or leak an open, partially-subscribed bus.
    adapter, fake = _adapter(monkeypatch, agent="wren")

    async def boom(subject: str, handler: Any) -> Any:
        raise RuntimeError("connection dropped mid-subscribe")

    fake.subscribe = boom  # type: ignore[method-assign]
    ok = await adapter.connect()
    assert ok is False
    assert fake.connected is False  # the half-open bus was closed, not leaked


def _narration(task_id: str = "task-1") -> Envelope:
    return Envelope.new(
        msg_id="n1",
        ts="t",
        sender="heron",
        kind="narration",
        task_id=task_id,
        channel="dash-dev",
        payload={"text": f"[{task_id}] dispatched to finch — coordination started."},
    )


async def test_worker_never_voices_a_narration_envelope(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Single-throat guard (#13): a narration beat misrouted onto a WORKER lane
    # is dropped, never forged into a turn — only the manager narrates.
    adapter, fake = _adapter(monkeypatch, agent="wren")
    await adapter.connect()
    handler = fake.subscriptions[subjects.agent_inbox("dev", "wren")]
    await handler(_narration())
    assert adapter.handled == []


async def test_manager_forges_narration_as_an_announce_turn(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter, fake = _adapter(monkeypatch, agent="heron", role="manager")
    await adapter.connect()
    handler = fake.subscriptions[subjects.agent_inbox("dev", "heron")]
    await handler(_narration())
    assert len(adapter.handled) == 1
    assert "Announce this status" in adapter.handled[0].text


async def test_registry_lanes_are_honored_when_buildable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # The k-board registry's role→lanes map is authoritative for now: a
    # fetched catalog trimming a worker to inbox-only drives the subscriptions.
    from fleet_nats import SubjectCatalog

    import fleet_subscriber.adapter as adapter_mod

    async def fetched_catalog() -> SubjectCatalog:
        return SubjectCatalog(
            subjects={"agent.inbox": "{env}.agent.inbox.{agent}"},
            role_lanes={"worker": ("inbox",), "manager": ("inbox",)},
            source="kboard",
            version=2,
        )

    adapter, fake = _adapter(monkeypatch, agent="wren")
    monkeypatch.setattr(adapter_mod, "connect_catalog", fetched_catalog)
    assert await adapter.connect() is True
    assert set(fake.subscriptions) == {subjects.agent_inbox("dev", "wren")}


async def test_fetched_catalog_with_unknown_lane_name_refuses_to_start(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Re-check residual 1: a FETCHED (source=kboard) catalog naming a lane this
    # adapter cannot build is NOT fail-open. Silently subscribing every hard-coded
    # default would ignore a catalog the client just fetched and enforced — the
    # enforce-then-ignore split F1 exists to prevent. It must FAIL LOUD like a
    # missing template: connect() returns False, the bus is closed, and NOTHING is
    # subscribed (never the code defaults the catalog's lane set disagrees with).
    from fleet_nats import SubjectCatalog

    import fleet_subscriber.adapter as adapter_mod

    async def bogus_catalog() -> SubjectCatalog:
        return SubjectCatalog(
            subjects={"agent.inbox": "{env}.agent.inbox.{agent}"},
            role_lanes={"worker": ("dispatch", "bogus-lane"), "manager": ("inbox",)},
            source="kboard",
            version=2,
        )

    adapter, fake = _adapter(monkeypatch, agent="wren")
    monkeypatch.setattr(adapter_mod, "connect_catalog", bogus_catalog)
    assert await adapter.connect() is False
    assert adapter.is_connected() is False
    assert fake.subscriptions == {}
    assert fake.connected is False


async def test_publish_only_runner_subscribes_to_nothing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # #222: a runner box connects to publish telemetry and must attach ZERO
    # subscriptions. Connect still succeeds — publish-only is a working state,
    # not an error.
    adapter, fake = _adapter(monkeypatch, agent="runner-box", role="runner")
    assert await adapter.connect() is True
    assert fake.subscriptions == {}
    # ...and it must not PUBLISH on a lane its credential does not grant either.
    # The heartbeat emitter targets {env}.agent.heartbeat.{agent}; the runner
    # credential permits only {env}.factory.runner.*, so an emitter here would
    # publish a refused message every interval forever — dropped server-side,
    # logged as an authz refusal client-side, never seen by the liveness
    # consumer. Connect must still report connected.
    assert adapter._heartbeat is None
    assert adapter._heartbeat_task is None
    assert adapter.is_connected() is True


async def test_catalog_cannot_grant_lanes_to_a_publish_only_role(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # #222, the load-bearing case: the fetched catalog's role→lanes map is
    # normally AUTHORITATIVE, so a drifted or tampered k-board row assigning
    # `dispatch` to the runner would otherwise turn a publish-only principal
    # into one that RECEIVES instructions — the single thing its threat model
    # forbids (it runs a coding agent with bypassPermissions over untrusted
    # repo content). The client must refuse: connect False, bus closed, nothing
    # subscribed. Note `dispatch` IS a buildable lane, so the pre-existing
    # unknown-lane guard would NOT have caught this.
    from fleet_nats import SubjectCatalog

    import fleet_subscriber.adapter as adapter_mod

    async def hostile_catalog() -> SubjectCatalog:
        return SubjectCatalog(
            subjects={
                "agent.dispatch": "{env}.agent.dispatch.{worker}",
                "agent.dispatch.wildcard": "{env}.agent.*.dispatch.{worker}",
            },
            role_lanes={"runner": ("dispatch",)},
            source="kboard",
            version=2,
        )

    adapter, fake = _adapter(monkeypatch, agent="runner-box", role="runner")
    monkeypatch.setattr(adapter_mod, "connect_catalog", hostile_catalog)
    assert await adapter.connect() is False
    assert adapter.is_connected() is False
    assert fake.subscriptions == {}
    assert fake.connected is False


async def test_connect_refuses_to_start_below_catalog_floor(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A configured version floor the SERVED catalog can't meet must REFUSE TO
    START fail-loud (#103): CatalogVersionTooLowError propagates out of connect(),
    it is NOT swallowed into the soft return-False path — the process must exit.

    A registry URL is set (a floor with no URL is now a config error, worker-A's
    F2 fix), and the served catalog (version 7) sits below the floor (9), so the
    real fetch+enforce path raises the below-floor error out of connect()."""
    from fleet_nats import CatalogVersionTooLowError

    with _CatalogServer(
        _catalog_payload(dispatch_template="{env}.agent.dispatch.{worker}")
    ) as base_url:
        monkeypatch.setenv("FLEET_SUBJECT_REGISTRY_URL", base_url)
        monkeypatch.setenv("FLEET_SUBJECT_CATALOG_MIN_VERSION", "9")  # served v7 < 9
        adapter, fake = _adapter(monkeypatch, agent="wren")
        with pytest.raises(CatalogVersionTooLowError) as exc:
            await adapter.connect()

    assert exc.value.have_version == 7 and exc.value.min_version == 9
    # Refuse-to-start happens BEFORE the bus opens: assert the bus was never
    # CONNECTED (pin the ordering directly, not just the empty-subscriptions
    # proxy — connect_catalog runs before FleetBus.connect()) (#103 P2-8).
    assert fake.connected is False
    assert set(fake.subscriptions) == set()


# --- Voice tee (#55): adapter-level wiring ------------------------------------


async def test_voice_tee_off_by_default_no_discord_call(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Tee env unset → the adapter never calls the tee, and the bus path is intact.
    monkeypatch.delenv("FLEET_VOICE_TEE_ENABLED", raising=False)
    monkeypatch.delenv("FLEET_VOICE_TEE_CHANNEL_ID", raising=False)
    adapter, fake = _adapter(monkeypatch, agent="wren")
    await adapter.connect()
    import fleet_subscriber.adapter as adapter_mod

    calls: list[dict[str, Any]] = []

    async def spy(**kwargs: Any) -> None:
        calls.append(kwargs)

    monkeypatch.setattr(adapter_mod, "tee_to_discord", spy)
    result = await adapter.send("dash-dev", "a reply")
    assert result.success is True
    assert calls == []  # no tee attempted
    assert len(fake.published) == 1  # bus publish still happened


async def test_correlated_intermediate_send_skips_voice_tee(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("FLEET_VOICE_TEE_ENABLED", "true")
    # #118: the tee resolves the TURN's channel through the persisted portal
    # map. "dash-dev" is the channel these turns run in.
    monkeypatch.setenv("FLEET_CHANNEL_MAP", "dash-dev=discord-123")
    adapter, fake = _adapter(monkeypatch, agent="wren")
    await adapter.connect()
    handler = fake.subscriptions[subjects.agent_dispatch("dev", "wren")]
    await _dispatch(handler, task_id="task-1", channel="dash-dev")

    import fleet_subscriber.adapter as adapter_mod

    calls: list[dict[str, Any]] = []

    async def spy(**kwargs: Any) -> None:
        calls.append(kwargs)

    monkeypatch.setattr(adapter_mod, "tee_to_discord", spy)
    result = await adapter.send("dash-dev", "progress: halfway")

    assert result.success is True
    _assert_noise_only(fake)
    assert calls == []
    assert adapter._channel_task["dash-dev"] == "task-1"


async def test_correlated_stance_without_completion_publishes_signal_not_voice(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("FLEET_VOICE_TEE_ENABLED", "true")
    # #118: the tee resolves the TURN's channel through the persisted portal
    # map. "dash-dev" is the channel these turns run in.
    monkeypatch.setenv("FLEET_CHANNEL_MAP", "dash-dev=discord-123")
    adapter, fake = _adapter(monkeypatch, agent="wren")
    await adapter.connect()
    handler = fake.subscriptions[subjects.agent_dispatch("dev", "wren")]
    await _dispatch(handler, task_id="task-1", channel="dash-dev")

    import fleet_subscriber.adapter as adapter_mod

    calls: list[dict[str, Any]] = []

    async def spy(**kwargs: Any) -> None:
        calls.append(kwargs)

    monkeypatch.setattr(adapter_mod, "tee_to_discord", spy)
    result = await adapter.send("dash-dev", _turn("Agreed.", stance="agree"))

    assert result.success is True
    assert len(fake.published) == 1
    subject, envelope = fake.published[0]
    assert subject == subjects.agent_status("dev", "task-1", channel="dash-dev")
    assert envelope.kind == "status"
    assert envelope.task_id == "task-1"
    assert envelope.payload["text"] == "Agreed."
    assert envelope.payload["meta"] == {"stance": "agree", "fact_pull": False}
    assert calls == []
    assert adapter._channel_task["dash-dev"] == "task-1"


async def test_voice_tee_on_posts_stripped_text(monkeypatch: pytest.MonkeyPatch) -> None:
    # Enabled + a channel → the tee gets the VISIBLE (trailer-stripped) text and
    # the configured channel; the bus + done sentinel behavior is unchanged.
    monkeypatch.setenv("FLEET_VOICE_TEE_ENABLED", "true")
    # #118: the tee resolves the TURN's channel through the persisted portal
    # map. "dash-dev" is the channel these turns run in.
    monkeypatch.setenv("FLEET_CHANNEL_MAP", "dash-dev=discord-123")
    adapter, fake = _adapter(monkeypatch, agent="wren")
    await adapter.connect()
    handler = fake.subscriptions[subjects.agent_dispatch("dev", "wren")]
    await _dispatch(handler, task_id="task-1", channel="dash-dev")

    import fleet_subscriber.adapter as adapter_mod

    calls: list[dict[str, Any]] = []

    async def spy(**kwargs: Any) -> None:
        calls.append(kwargs)

    monkeypatch.setattr(adapter_mod, "tee_to_discord", spy)
    await adapter.send("dash-dev", _turn("The final answer.", done=True, outcome="completed"))

    # The tee copy receives the typed turn's visible text.
    assert len(calls) == 1
    assert calls[0]["channel_id"] == "discord-123"
    assert calls[0]["text"] == "The final answer."
    # Bus path unchanged: clean status reply + done sentinel.
    assert len(fake.published) == 2
    assert fake.published[0][1].payload["text"] == "The final answer."
    assert fake.published[1][1].kind == "done"


# --- #118: the tee target follows the TURN's channel --------------------------


async def test_voice_tee_mirrors_each_channel_to_its_own_room(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Two tasks in two channels mirror to two DIFFERENT Discord rooms.

    This is the #118 defect end-to-end. Before the portal map the tee read one
    ``FLEET_VOICE_TEE_CHANNEL_ID`` and ignored the turn's channel, so BOTH of
    these replies went to the same room — a ``fleet-ops`` answer landed in
    ``arena``. It now resolves per turn, so a host bridges N channels correctly.
    """
    monkeypatch.setenv("FLEET_VOICE_TEE_ENABLED", "true")
    monkeypatch.setenv("FLEET_CHANNEL_MAP", "arena=discord-arena,fleet-ops=discord-ops")
    adapter, fake = _adapter(monkeypatch, agent="wren")
    await adapter.connect()
    handler = fake.subscriptions[subjects.agent_dispatch("dev", "wren")]
    await _dispatch(handler, task_id="task-a", channel="arena")
    await _dispatch(handler, task_id="task-b", channel="fleet-ops")

    import fleet_subscriber.adapter as adapter_mod

    calls: list[dict[str, Any]] = []

    async def spy(**kwargs: Any) -> None:
        calls.append(kwargs)

    monkeypatch.setattr(adapter_mod, "tee_to_discord", spy)
    await adapter.send("arena", _turn("arena answer", done=True, outcome="completed"))
    await adapter.send("fleet-ops", _turn("ops answer", done=True, outcome="completed"))

    assert len(calls) == 2
    assert calls[0]["channel_id"] == "discord-arena"
    assert calls[0]["text"] == "arena answer"
    assert calls[1]["channel_id"] == "discord-ops"
    assert calls[1]["text"] == "ops answer"
    # The two turns went to DIFFERENT rooms — the assertion a single default
    # channel can never satisfy.
    assert calls[0]["channel_id"] != calls[1]["channel_id"]


async def test_voice_tee_skips_an_unmapped_channel_rather_than_misrouting(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An unmapped channel is NOT mirrored — and never to some other room.

    The bus path is untouched: the status reply and the done sentinel still
    publish, so a missing portal costs visibility, never coordination.

    ``FLEET_VOICE_TEE_CHANNEL_ID`` is set DELIBERATELY: without it the pre-#118
    code computed ``active = enabled and bool(channel_id)`` = False, so the tee
    never fired and ``calls == []`` held for the trivial reason "the tee was off"
    — a test that passes against the unfixed code and proves nothing. With the
    legacy id set, the old code WOULD have mirrored this orphan reply into
    ``discord-legacy``, so the empty-calls assertion is now a real assertion that
    the mirror was REFUSED for an unmapped channel.
    """
    monkeypatch.setenv("FLEET_VOICE_TEE_ENABLED", "true")
    monkeypatch.setenv("FLEET_VOICE_TEE_CHANNEL_ID", "discord-legacy")
    monkeypatch.setenv("FLEET_CHANNEL_MAP", "arena=discord-arena")
    adapter, fake = _adapter(monkeypatch, agent="wren")
    await adapter.connect()
    handler = fake.subscriptions[subjects.agent_dispatch("dev", "wren")]
    await _dispatch(handler, task_id="task-x", channel="not-mapped")

    import fleet_subscriber.adapter as adapter_mod

    calls: list[dict[str, Any]] = []

    async def spy(**kwargs: Any) -> None:
        calls.append(kwargs)

    monkeypatch.setattr(adapter_mod, "tee_to_discord", spy)
    await adapter.send("not-mapped", _turn("orphan answer", done=True, outcome="completed"))

    assert calls == []  # never mirrored into discord-arena
    assert len(fake.published) == 2  # status + done still published
    assert fake.published[1][1].kind == "done"


async def test_a_new_channel_needs_only_a_map_row(monkeypatch: pytest.MonkeyPatch) -> None:
    """The #118 acceptance criterion, at the adapter: a row, not a code change.

    Identical adapter code; the ONLY difference from the test above is one more
    entry in the persisted map.
    """
    monkeypatch.setenv("FLEET_VOICE_TEE_ENABLED", "true")
    monkeypatch.setenv("FLEET_CHANNEL_MAP", "arena=discord-arena,not-mapped=discord-new")
    adapter, fake = _adapter(monkeypatch, agent="wren")
    await adapter.connect()
    handler = fake.subscriptions[subjects.agent_dispatch("dev", "wren")]
    await _dispatch(handler, task_id="task-x", channel="not-mapped")

    import fleet_subscriber.adapter as adapter_mod

    calls: list[dict[str, Any]] = []

    async def spy(**kwargs: Any) -> None:
        calls.append(kwargs)

    monkeypatch.setattr(adapter_mod, "tee_to_discord", spy)
    await adapter.send("not-mapped", _turn("now visible", done=True, outcome="completed"))

    assert len(calls) == 1
    assert calls[0]["channel_id"] == "discord-new"


async def test_voice_tee_failure_never_breaks_bus_publish(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # End-to-end isolation with the REAL tee: a live gateway whose Discord adapter
    # raises must not break the bus publish or the done sentinel.
    import sys
    import types

    monkeypatch.setenv("FLEET_VOICE_TEE_ENABLED", "true")
    # #118: the tee resolves the TURN's channel through the persisted portal
    # map. "dash-dev" is the channel these turns run in.
    monkeypatch.setenv("FLEET_CHANNEL_MAP", "dash-dev=discord-123")

    class ExplodingDiscord:
        async def send(self, *, chat_id: str, content: str) -> Any:
            raise RuntimeError(f"discord down; reply was {content!r}")

    class _Runner:
        adapters = {StubPlatform("discord"): ExplodingDiscord()}

    gw = types.ModuleType("gateway")
    gw_run = types.ModuleType("gateway.run")
    gw_run._gateway_runner_ref = lambda: _Runner()  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "gateway", gw)
    monkeypatch.setitem(sys.modules, "gateway.run", gw_run)

    adapter, fake = _adapter(monkeypatch, agent="wren")
    await adapter.connect()
    handler = fake.subscriptions[subjects.agent_dispatch("dev", "wren")]
    await _dispatch(handler, task_id="task-1", channel="dash-dev")

    # send() must succeed and publish on the bus even though the tee blew up.
    result = await adapter.send("dash-dev", _turn("answer", done=True))
    assert result.success is True
    # The clean status reply + the done sentinel are both on the wire.
    assert len(fake.published) == 2
    assert fake.published[0][1].payload["text"] == "answer"
    assert fake.published[1][1].kind == "done"


# --- Catalog-consumed subjects: functional I/O over a REAL registry server ----
#
# F1/#103: a FETCHED catalog must DRIVE the concrete subscription subjects, not
# just be enforced-then-ignored. These tests stand up a real HTTP server that
# serves the k-board /api/nats-subjects contract (so the real connect_catalog →
# httpx fetch → parse path runs — no monkeypatching of the registry) whose
# ``agent.dispatch`` template DIFFERS from the code default, then drive the real
# adapter.connect() against a fake bus and assert the CATALOG's subject — not the
# hard-coded builder's — was subscribed. On the pre-fix code (which subscribed via
# _LANE_BUILDERS regardless of the fetched catalog) these fail.


def _catalog_payload(*, dispatch_template: str, drop_lane: str | None = None) -> dict[str, Any]:
    """A registry catalog whose dispatch template differs from the code default.

    ``drop_lane`` (optional) omits a worker lane's subject template to exercise
    the fail-loud path (a lane the code subscribes but the fetched catalog does
    not template). role_lanes still lists the full worker set, so the missing
    template surfaces only when the adapter resolves subjects THROUGH the catalog.
    """
    subjects_map = {
        "agent.dispatch": dispatch_template,
        "agent.dispatch.wildcard": "{env}.agent.*.dispatch.{worker}",
        "agent.relay": "{env}.agent.relay.{to}",
        # relay/wake are channel-scopable since #117, so each needs its wildcard
        # template too — a catalog missing one is the fail-loud case below.
        "agent.relay.wildcard": "{env}.agent.*.relay.{to}",
        "agent.wake": "{env}.agent.wake.{worker}",
        "agent.wake.wildcard": "{env}.agent.*.wake.{worker}",
        "agent.inbox": "{env}.agent.inbox.{agent}",
    }
    if drop_lane is not None:
        # Drop the catalog KEY(s) that back this lane so its template is absent.
        for key in list(subjects_map):
            if key == f"agent.{drop_lane}" or key.startswith(f"agent.{drop_lane}."):
                subjects_map.pop(key)
    return {
        "version": 7,
        "subjects": subjects_map,
        "role_lanes": {
            "worker": ["dispatch", "relay", "wake", "inbox"],
            "manager": ["inbox"],
        },
    }


class _CatalogServer:
    """A throwaway HTTP server serving one catalog payload on /api/nats-subjects."""

    def __init__(self, payload: dict[str, Any]) -> None:
        body = json.dumps(payload).encode()

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self) -> None:  # noqa: N802 — BaseHTTPRequestHandler contract
                if urlparse(self.path).path != "/api/nats-subjects":
                    self.send_error(404)
                    return
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, *_args: Any) -> None:  # silence test-server noise
                return

        self._server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)

    def __enter__(self) -> str:
        self._thread.start()
        # Bound to 127.0.0.1 above; only the OS-assigned port varies (an int for
        # AF_INET). server_address[0] is typed str|bytes in the stdlib stubs, so
        # use the known host literal rather than format a possibly-bytes value.
        port = self._server.server_address[1]
        return f"http://127.0.0.1:{port}"

    def __exit__(self, *_exc: Any) -> None:
        self._server.shutdown()
        self._server.server_close()
        self._thread.join(timeout=2)


async def test_worker_subscribes_the_fetched_catalog_dispatch_subject(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # The fetched catalog's dispatch template differs from the code default; the
    # real connect() must subscribe the CATALOG's subject, never the builder's.
    # (Pre-fix: subscribes subjects.agent_dispatch("dev","wren") and the catalog
    # subject is absent → this assertion fails.)
    dispatch_template = "{env}.agent.dispatch-v2.{worker}"
    with _CatalogServer(_catalog_payload(dispatch_template=dispatch_template)) as base_url:
        monkeypatch.setenv("FLEET_SUBJECT_REGISTRY_URL", base_url)
        monkeypatch.delenv("FLEET_SUBJECT_CATALOG_MIN_VERSION", raising=False)
        adapter, fake = _adapter(monkeypatch, agent="wren")
        ok = await adapter.connect()

    assert ok is True
    catalog_dispatch = dispatch_template.format(env="dev", worker="wren")
    assert catalog_dispatch == "dev.agent.dispatch-v2.wren"
    # The catalog subject is subscribed; the hard-coded builder's is NOT.
    assert catalog_dispatch in fake.subscriptions
    assert subjects.agent_dispatch("dev", "wren") not in fake.subscriptions
    # The other lanes (whose catalog templates match code) resolve identically —
    # the whole worker lane set is present, sourced from the catalog.
    assert set(fake.subscriptions) == {
        catalog_dispatch,
        subjects.agent_dispatch_wildcard("dev", "wren"),
        subjects.agent_relay("dev", "wren"),
        subjects.agent_relay_wildcard("dev", "wren"),
        subjects.agent_wake("dev", "wren"),
        subjects.agent_wake_wildcard("dev", "wren"),
        subjects.agent_inbox("dev", "wren"),
    }


async def test_connect_fails_loud_when_catalog_missing_a_lane_template(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Negative case (F1): the fetched catalog role_lanes names the wake lane, but
    # its subject template is absent. Enforcement-that-consumes must REFUSE to
    # subscribe on a code default the catalog does not name — connect() returns
    # False (loud), the half-open bus is closed, nothing partially subscribed.
    payload = _catalog_payload(dispatch_template="{env}.agent.dispatch.{worker}", drop_lane="wake")
    with _CatalogServer(payload) as base_url:
        monkeypatch.setenv("FLEET_SUBJECT_REGISTRY_URL", base_url)
        monkeypatch.delenv("FLEET_SUBJECT_CATALOG_MIN_VERSION", raising=False)
        adapter, fake = _adapter(monkeypatch, agent="wren")
        ok = await adapter.connect()

    assert ok is False
    assert adapter.is_connected() is False
    # Resolution fails BEFORE any subscribe, and the half-open bus is closed.
    assert fake.subscriptions == {}
    assert fake.connected is False


# --- Typed Turn deserialization boundary (#77) --------------------------------
#
# The reply's structure now comes from a TYPED, JSON-deserialized Turn's named
# fields — not from a pattern scraped over prose. A producer emitting a JSON turn
# drives completion / stance / visible text off fields. Legacy text trailers are
# compatibility parser helpers only; they are no longer called by the active send
# path. The spoof class the footer/trailer heuristics fought is dead: a [[done]] /
# footer string in visible_text or raw coordinated text is inert text.


async def test_json_turn_completes_and_emits_done_sentinel(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # A typed Turn with done=True posts the clean visible_text as a status reply
    # then emits the done sentinel — the SAME wire result as a [[done]] text reply,
    # but driven off the named field, not a trailer scrape.
    adapter, fake = _adapter(monkeypatch, agent="wren")
    await adapter.connect()
    handler = fake.subscriptions[subjects.agent_dispatch("dev", "wren")]
    await _dispatch(handler, task_id="task-1", channel="dash-dev")

    await adapter.send(
        "dash-dev",
        json.dumps(
            {"visible_text": "Here is the final answer.", "done": True, "outcome": "shipped"}
        ),
    )

    task_subject = subjects.agent_status("dev", "task-1", channel="dash-dev")
    assert len(fake.published) == 2
    status_subj, status_env = fake.published[0]
    done_subj, done_env = fake.published[1]
    assert status_subj == task_subject
    assert status_env.kind == "status"
    assert status_env.payload["text"] == "Here is the final answer."
    assert done_subj == task_subject
    assert done_env.kind == "done"
    assert done_env.payload == {
        "header": {"actor": "wren"},
        "data": {"task_id": "task-1", "outcome": "shipped"},
        "extras": {},
        "footer": {},
    }
    assert isinstance(done_env.typed_signal, Done)
    assert done_env.typed_signal.task_id == "task-1"
    assert done_env.typed_signal.outcome == "shipped"
    assert "dash-dev" not in adapter._channel_task  # correlation released


async def test_json_stance_only_turn_binds_typed_stance(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # A stance-only typed Turn (done=False) publishes the scorekeeper status signal
    # with a typed fleet-core Stance bound as typed_signal (C7: typed frame fields
    # are truth) and payload.meta kept only as a compatibility mirror.
    adapter, fake = _adapter(monkeypatch, agent="wren")
    await adapter.connect()
    handler = fake.subscriptions[subjects.agent_dispatch("dev", "wren")]
    await _dispatch(handler, task_id="task-1", channel="dash-dev")

    await adapter.send(
        "dash-dev",
        json.dumps(
            {
                "visible_text": "The metric regressed.",
                "stance": "dissent",
                "fact_pull": True,
                "reason": "p99 doubled",
            }
        ),
    )

    assert len(fake.published) == 1  # status signal only; not done → no sentinel
    _, env = fake.published[0]
    assert env.kind == "status"
    assert env.payload["text"] == "The metric regressed."
    # The typed signal is the truth the orchestrator reads first.
    assert isinstance(env.typed_signal, Stance)
    assert env.typed_signal.position is StancePosition.DISSENT
    assert env.typed_signal.fact_pull is True
    assert env.typed_signal.because == "p99 doubled"
    # The typed stance survives the real wire boundary — after a bus
    # serialize/deserialize the received envelope still carries the typed truth.
    received = Envelope.from_bytes(env.to_bytes())
    assert isinstance(received.typed_signal, Stance)
    assert received.typed_signal.position is StancePosition.DISSENT
    assert received.typed_signal.fact_pull is True
    # payload.meta stays only as the compatibility MIRROR of the typed signal.
    assert env.payload["meta"] == {
        "stance": "dissent",
        "fact_pull": True,
        "reason": "p99 doubled",
    }
    assert adapter._channel_task["dash-dev"] == "task-1"  # correlation kept


async def test_json_turn_both_signs_stance_and_completes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # done + stance in one typed turn: status carries meta, then the done sentinel.
    adapter, fake = _adapter(monkeypatch, agent="wren")
    await adapter.connect()
    handler = fake.subscriptions[subjects.agent_dispatch("dev", "wren")]
    await _dispatch(handler, task_id="task-1", channel="dash-dev")

    await adapter.send(
        "dash-dev",
        json.dumps(
            {
                "visible_text": "Final call: agree.",
                "done": True,
                "stance": "agree",
                "fact_pull": True,
            }
        ),
    )

    assert len(fake.published) == 2
    _, status_env = fake.published[0]
    _, done_env = fake.published[1]
    assert status_env.payload["text"] == "Final call: agree."
    # Typed truth on the status envelope, mirroring the Done.typed_signal checks.
    assert isinstance(status_env.typed_signal, Stance)
    assert status_env.typed_signal.position is StancePosition.AGREE
    assert status_env.typed_signal.fact_pull is True
    assert status_env.typed_signal.because is None
    # meta is only the compatibility mirror of the typed stance.
    assert status_env.payload["meta"] == {"stance": "agree", "fact_pull": True}
    assert done_env.kind == "done"
    assert done_env.payload["data"]["outcome"] == "completed"  # default when done, no outcome
    assert isinstance(done_env.typed_signal, Done)
    assert done_env.typed_signal.outcome == "completed"


async def test_raw_completion_trailer_does_not_complete_coordinated_turn(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Completion is a typed turn field, not a prose token (fleet-bus #205 removed
    # the text-trailer parsers entirely). A raw visible-text reply ending in
    # [[done]] is not a typed completion and must not emit status or a sentinel.
    adapter, fake = _adapter(monkeypatch, agent="wren")
    await adapter.connect()
    handler = fake.subscriptions[subjects.agent_dispatch("dev", "wren")]
    await _dispatch(handler, task_id="task-1", channel="dash-dev")

    await adapter.send("dash-dev", "the real answer\n[[done]]")

    _assert_noise_only(fake)
    assert adapter._channel_task["dash-dev"] == "task-1"


async def test_raw_stance_trailer_does_not_sign_coordinated_turn(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Same spoof class for stance: raw visible text is not typed structure. There
    # is no text-trailer parser anymore (fleet-bus #205); a coordinated send signs
    # only from a typed Turn's named fields.
    adapter, fake = _adapter(monkeypatch, agent="wren")
    await adapter.connect()
    handler = fake.subscriptions[subjects.agent_dispatch("dev", "wren")]
    await _dispatch(handler, task_id="task-1", channel="dash-dev")

    await adapter.send("dash-dev", "Agreed.\n[[stance:agree]]")

    _assert_noise_only(fake)
    assert adapter._channel_task["dash-dev"] == "task-1"


async def test_json_turn_visible_text_with_done_marker_does_not_complete(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # SPOOF CLASS DEAD: a JSON turn whose visible_text CONTAINS the literal [[done]]
    # marker but has done=False (the default) does NOT complete. The marker is inert
    # field text — completion comes from the named `done` field, never a scrape.
    adapter, fake = _adapter(monkeypatch, agent="wren")
    await adapter.connect()
    handler = fake.subscriptions[subjects.agent_dispatch("dev", "wren")]
    await _dispatch(handler, task_id="task-1", channel="dash-dev")

    await adapter.send(
        "dash-dev",
        json.dumps({"visible_text": "still thinking, will say [[done]] later"}),
    )

    # No completion and no stance on a correlated turn → no status, no done
    # sentinel (it rides the noise lane, #116). The literal [[done]] did NOT end
    # the turn.
    _assert_noise_only(fake)
    assert adapter._channel_task["dash-dev"] == "task-1"  # turn still open


async def test_json_turn_visible_text_with_stance_marker_does_not_sign(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # SPOOF CLASS DEAD: a JSON turn whose visible_text CONTAINS a stance trailer
    # marker but has no typed stance field does NOT sign. The marker is inert
    # field text — stance comes from the named `stance` field, never a scrape.
    adapter, fake = _adapter(monkeypatch, agent="wren")
    await adapter.connect()
    handler = fake.subscriptions[subjects.agent_dispatch("dev", "wren")]
    await _dispatch(handler, task_id="task-1", channel="dash-dev")

    await adapter.send(
        "dash-dev",
        json.dumps({"visible_text": "still thinking, might later say [[stance:agree]]"}),
    )

    _assert_noise_only(fake)
    assert adapter._channel_task["dash-dev"] == "task-1"


async def test_json_turn_visible_text_with_footer_string_does_not_complete(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # SPOOF CLASS DEAD (footer variant): the gateway footer shape
    # (`model · context% · cwd`) sitting inside visible_text is inert text on the
    # typed path — it is neither carried as a field nor stripped, and it does not
    # complete or sign the turn. The footer heuristic the 4 rounds fought is gone.
    adapter, fake = _adapter(monkeypatch, agent="wren")
    await adapter.connect()
    handler = fake.subscriptions[subjects.agent_dispatch("dev", "wren")]
    await _dispatch(handler, task_id="task-1", channel="dash-dev")

    await adapter.send(
        "dash-dev",
        json.dumps({"visible_text": "working... co-op · 5% · ~/repo"}),
    )

    _assert_noise_only(fake)  # not done, not signed → noise lane, never status
    assert adapter._channel_task["dash-dev"] == "task-1"


async def test_json_turn_footer_string_stays_verbatim_when_completing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Positive complement: a footer-looking substring in a DONE turn's visible_text
    # is preserved verbatim (never stripped) — attribution/footer stripping is not
    # this envelope's job (it lives in Langfuse traces), so the field rides as-is.
    adapter, fake = _adapter(monkeypatch, agent="wren")
    await adapter.connect()
    handler = fake.subscriptions[subjects.agent_dispatch("dev", "wren")]
    await _dispatch(handler, task_id="task-1", channel="dash-dev")

    await adapter.send(
        "dash-dev",
        json.dumps({"visible_text": "Answer. co-op · 5% · ~/repo", "done": True}),
    )

    _, status_env = fake.published[0]
    assert status_env.payload["text"] == "Answer. co-op · 5% · ~/repo"  # verbatim, not stripped


async def test_json_turn_with_unknown_extra_field_still_completes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # extra="allow": a done JSON turn carrying a field this consumer
    # does not know about must STILL complete — the unknown field is preserved, the
    # turn is NOT rejected into the text fallback (where the trailer-less JSON would
    # be suppressed and the correlation would wedge).
    adapter, fake = _adapter(monkeypatch, agent="wren")
    await adapter.connect()
    handler = fake.subscriptions[subjects.agent_dispatch("dev", "wren")]
    await _dispatch(handler, task_id="task-1", channel="dash-dev")

    await adapter.send(
        "dash-dev",
        json.dumps({"visible_text": "final", "done": True, "footer": "x"}),
    )

    assert len(fake.published) == 2  # status + done sentinel, NOT suppressed
    _, status_env = fake.published[0]
    _, done_env = fake.published[1]
    assert status_env.payload["text"] == "final"
    assert done_env.kind == "done"
    assert "dash-dev" not in adapter._channel_task  # correlation released, not wedged


async def test_json_turn_capitalised_stance_completes_and_signs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Case-insensitive stance (codex delta): a done turn signing "Agree" must
    # complete AND ride stance "agree" on payload.meta — a capitalised stance must
    # not fail the whole turn and drop the completion.
    adapter, fake = _adapter(monkeypatch, agent="wren")
    await adapter.connect()
    handler = fake.subscriptions[subjects.agent_dispatch("dev", "wren")]
    await _dispatch(handler, task_id="task-1", channel="dash-dev")

    await adapter.send(
        "dash-dev",
        json.dumps({"visible_text": "final", "done": True, "stance": "Agree"}),
    )

    assert len(fake.published) == 2  # status (with meta) + done sentinel
    _, status_env = fake.published[0]
    _, done_env = fake.published[1]
    assert status_env.payload["text"] == "final"
    assert status_env.payload["meta"] == {"stance": "agree", "fact_pull": False}
    assert done_env.kind == "done"


# --- review: the #118 cutover must not take the live tee dark ------------------


async def test_the_legacy_tee_env_keeps_mirroring_when_no_map_is_deployed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The live agent-host posture still mirrors after this change merges.

    An agent host today sets FLEET_SUBJECT_REGISTRY_URL and
    FLEET_VOICE_TEE_CHANNEL_ID but NOT FLEET_CHANNEL_MAP, and /api/channel-map
    404s — so the load ladder lands on the EMPTY map. Retiring the legacy env to
    warn-only makes ``active`` False and the reply reaches NO Discord room at all,
    a regression the diff ships with no unit-file change to compensate. The legacy
    id is bridged onto the agent's home channel instead, so the mirror survives
    the cutover and an operator migrates on their own schedule.
    """
    monkeypatch.setenv("FLEET_VOICE_TEE_ENABLED", "true")
    monkeypatch.setenv("FLEET_VOICE_TEE_CHANNEL_ID", "discord-legacy")
    monkeypatch.delenv("FLEET_CHANNEL_MAP", raising=False)
    monkeypatch.delenv("FLEET_SUBJECT_REGISTRY_URL", raising=False)
    adapter, fake = _adapter(monkeypatch, agent="wren")
    await adapter.connect()

    assert adapter._voice_tee.active is True

    handler = fake.subscriptions[subjects.agent_dispatch("dev", "wren")]
    # "wren-home" is StubConfig's home_channel — the slug the bridge anchors to.
    await _dispatch(handler, task_id="task-1", channel="wren-home")

    import fleet_subscriber.adapter as adapter_mod

    calls: list[dict[str, Any]] = []

    async def spy(**kwargs: Any) -> None:
        calls.append(kwargs)

    monkeypatch.setattr(adapter_mod, "tee_to_discord", spy)
    await adapter.send("wren-home", _turn("still visible", done=True, outcome="completed"))

    assert len(calls) == 1
    assert calls[0]["channel_id"] == "discord-legacy"


async def test_the_legacy_bridge_anchors_to_agent_id_when_config_has_no_home_channel(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The bridge must survive a config that carries NO home_channel.

    This reproduces the LIVE agent hosts, and it is the case every other test in
    this file misses: ``_adapter()`` builds ``StubConfig(home_channel=...)``, but
    the real Hermes ``PlatformConfig`` is constructed from this plugin's
    ``required_env`` (FLEET_NATS_URL, FLEET_AGENT_ID) and never carries a home
    channel at all. So on wren and finch -- both of which have
    FLEET_VOICE_TEE_ENABLED and FLEET_VOICE_TEE_CHANNEL_ID and neither of which
    has FLEET_CHANNEL_MAP -- ``home`` was empty, ``VoiceTeeConfig.from_env`` took
    the retire-and-skip branch, and the tee went from mirroring one room to
    mirroring NO room on the next gateway restart. An outage on the
    agent-visible path, delivered by an atomic code cutover.

    Anchoring to ``agent_id`` (required env, always present) keeps the bridge
    alive without resurrecting the #118 defect: it still maps exactly ONE slug.
    """
    monkeypatch.setenv("FLEET_VOICE_TEE_ENABLED", "1")
    monkeypatch.setenv("FLEET_VOICE_TEE_CHANNEL_ID", "discord-legacy")
    monkeypatch.delenv("FLEET_CHANNEL_MAP", raising=False)
    monkeypatch.delenv("FLEET_SUBJECT_REGISTRY_URL", raising=False)
    adapter, fake = _adapter(monkeypatch, agent="wren", home_channel=None)
    await adapter.connect()

    # Without the agent_id fallback this is False: no home slug, so the legacy
    # id is retired and the mirror is silently dead.
    assert adapter._voice_tee.active is True

    handler = fake.subscriptions[subjects.agent_dispatch("dev", "wren")]
    await _dispatch(handler, task_id="task-1", channel="wren")

    import fleet_subscriber.adapter as adapter_mod

    calls: list[dict[str, Any]] = []

    async def spy(**kwargs: Any) -> None:
        calls.append(kwargs)

    monkeypatch.setattr(adapter_mod, "tee_to_discord", spy)
    await adapter.send("wren", _turn("still visible", done=True, outcome="completed"))

    assert len(calls) == 1
    assert calls[0]["channel_id"] == "discord-legacy"


async def test_the_legacy_bridge_does_not_mirror_another_channel(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The bridge maps ONE slug, so it cannot restore one-room-for-every-task.

    Pre-#118 this turn — a channel the map does not know — would have posted into
    the single env room. Under the bridge it resolves to nothing and is skipped.
    """
    monkeypatch.setenv("FLEET_VOICE_TEE_ENABLED", "true")
    monkeypatch.setenv("FLEET_VOICE_TEE_CHANNEL_ID", "discord-legacy")
    monkeypatch.delenv("FLEET_CHANNEL_MAP", raising=False)
    monkeypatch.delenv("FLEET_SUBJECT_REGISTRY_URL", raising=False)
    adapter, fake = _adapter(monkeypatch, agent="wren")
    await adapter.connect()
    handler = fake.subscriptions[subjects.agent_dispatch("dev", "wren")]
    await _dispatch(handler, task_id="task-x", channel="somewhere-else")

    import fleet_subscriber.adapter as adapter_mod

    calls: list[dict[str, Any]] = []

    async def spy(**kwargs: Any) -> None:
        calls.append(kwargs)

    monkeypatch.setattr(adapter_mod, "tee_to_discord", spy)
    await adapter.send("somewhere-else", _turn("orphan", done=True, outcome="completed"))

    assert calls == []  # never mirrored into discord-legacy


async def test_a_changed_channel_map_drops_stale_chat_task_correlations(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A reconnect that loads a DIFFERENT map must not leave a mis-keyed correlation.

    ``_channel_task`` is keyed on the RESOLVED chat_id. The same room keys as the
    raw spelling under an empty map and as the canonical slug under a loaded one,
    so a map flip across a reconnect leaves a correlation no future reply can
    match: the reply's status then falls back to the CHANNEL subject, which
    ``watch_worker_status`` is not listening on, and the task's completion is never
    seen. Dropping the stale entries degrades one in-flight reply to the
    pre-existing uncorrelated path instead.
    """
    monkeypatch.delenv("FLEET_CHANNEL_MAP", raising=False)
    monkeypatch.delenv("FLEET_SUBJECT_REGISTRY_URL", raising=False)
    adapter, fake = _adapter(monkeypatch, agent="wren")
    await adapter.connect()
    handler = fake.subscriptions[subjects.agent_dispatch("dev", "wren")]
    await _dispatch(handler, task_id="task-1", channel="Arena")

    # Empty map → the raw spelling is the correlation key.
    assert adapter._channel_task == {"Arena": "task-1"}

    # The k-board table lands; the reconnect loads a real map, under which this
    # same room canonicalises to "arena" — a key the stale entry cannot produce.
    monkeypatch.setenv("FLEET_CHANNEL_MAP", "arena=discord-arena")
    await adapter.connect(is_reconnect=True)

    assert adapter._channels.source == "env"
    assert adapter._channel_task == {}


# --- noise lanes: needed-but-noisy traffic is recorded, never voiced (#116) ----


async def test_noise_send_is_recorded_not_discarded(monkeypatch: pytest.MonkeyPatch) -> None:
    """A non-actionable correlated send LANDS on the noise lane.

    Before #116 this send was dropped on the floor: the ack was neither delivered
    nor recorded anywhere, so "fully logged and mineable" was simply false. This
    is the test that fails against the unfixed adapter (which published nothing).
    """
    adapter, fake = _adapter(monkeypatch, agent="wren")
    await adapter.connect()
    handler = fake.subscriptions[subjects.agent_dispatch("dev", "wren")]
    await _dispatch(handler, task_id="task-1", channel="dash-dev")

    result = await adapter.send("dash-dev", "ack: on it")

    assert result.success is True
    assert len(fake.published) == 1
    subject, env = fake.published[0]
    assert subject == subjects.agent_ack("dev", "task-1", channel="dash-dev")
    assert env.kind == "ack"
    assert env.task_id == "task-1"
    assert env.channel == "dash-dev"
    # The content is preserved verbatim — mineable, not summarised away.
    assert env.payload["text"] == "ack: on it"


async def test_long_noise_send_goes_to_the_telemetry_lane(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Tool echo / progress markers ride ``telemetry``, not the low-volume ack lane."""
    adapter, fake = _adapter(monkeypatch, agent="wren")
    await adapter.connect()
    handler = fake.subscriptions[subjects.agent_dispatch("dev", "wren")]
    await _dispatch(handler, task_id="task-1", channel="dash-dev")

    tool_echo = "⏳ running: " + "x" * 200
    await adapter.send("dash-dev", tool_echo)

    subject, env = fake.published[0]
    assert subject == subjects.agent_telemetry("dev", "task-1", channel="dash-dev")
    assert env.kind == "telemetry"
    assert env.payload["text"] == tool_echo


@pytest.mark.parametrize("marker_send", ["⏳", "💾 saving", "⏳ still working", "... thinking"])
async def test_short_progress_marker_goes_to_telemetry_not_the_ack_lane(
    monkeypatch: pytest.MonkeyPatch, marker_send: str
) -> None:
    """A SHORT progress marker is telemetry — length alone would misfile it.

    The markers #116 names are the shortest sends on the wire, so a pure length
    rule files the highest-volume traffic onto ``ack``, whose contracted value
    (C3 row 13) is "one message per dispatch". A worker emitting a marker every
    few seconds would then put unbounded traffic on the low-volume lane and
    defeat the split entirely.
    """
    adapter, fake = _adapter(monkeypatch, agent="wren")
    await adapter.connect()
    handler = fake.subscriptions[subjects.agent_dispatch("dev", "wren")]
    await _dispatch(handler, task_id="task-1", channel="dash-dev")

    assert len(marker_send) <= 80, "the point of this test is that it is SHORT"
    await adapter.send("dash-dev", marker_send)

    subject, env = fake.published[0]
    assert subject == subjects.agent_telemetry("dev", "task-1", channel="dash-dev")
    assert env.kind == "telemetry"
    assert env.payload["text"] == marker_send


async def test_a_plain_short_ack_still_rides_the_ack_lane(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The marker rule must not swallow the ack lane it is meant to protect."""
    adapter, fake = _adapter(monkeypatch, agent="wren")
    await adapter.connect()
    handler = fake.subscriptions[subjects.agent_dispatch("dev", "wren")]
    await _dispatch(handler, task_id="task-1", channel="dash-dev")

    await adapter.send("dash-dev", "ack: on it")

    subject, env = fake.published[0]
    assert subject == subjects.agent_ack("dev", "task-1", channel="dash-dev")
    assert env.kind == "ack"


async def test_noise_never_reaches_the_status_lane_or_the_tee(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """ZERO noise on the coordination lane and ZERO Discord calls (#116 acceptance).

    The tee spy proves the "never costs an LLM turn / never lands in the parent
    channel" half; the subject assertion proves the orchestrator's status watcher
    cannot see it either.
    """
    monkeypatch.setenv("FLEET_VOICE_TEE_ENABLED", "true")
    monkeypatch.setenv("FLEET_VOICE_TEE_CHANNEL_ID", "chan-1")
    adapter, fake = _adapter(monkeypatch, agent="wren")
    await adapter.connect()
    handler = fake.subscriptions[subjects.agent_dispatch("dev", "wren")]
    await _dispatch(handler, task_id="task-1", channel="dash-dev")

    teed: list[dict[str, Any]] = []

    async def spy(**kwargs: Any) -> None:
        teed.append(kwargs)

    import fleet_subscriber.adapter as adapter_mod

    monkeypatch.setattr(adapter_mod, "tee_to_discord", spy)

    await adapter.send("dash-dev", "ack: on it")
    await adapter.send("dash-dev", "⏳ still working " + "y" * 200)

    assert teed == []  # nothing was ever offered to Discord
    status = subjects.agent_status("dev", "task-1", channel="dash-dev")
    assert not any(s == status for s, _ in fake.published)
    assert not any(env.kind in {"status", "done"} for _, env in fake.published)


async def test_noise_publish_failure_never_fails_the_send(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The noise lane is observability — a bus failure must not fail the turn.

    Same fail-open contract the voice tee has: losing an ack is a recording gap,
    not a coordination fault, so ``send`` still reports success.
    """
    adapter, fake = _adapter(monkeypatch, agent="wren")
    await adapter.connect()
    handler = fake.subscriptions[subjects.agent_dispatch("dev", "wren")]
    await _dispatch(handler, task_id="task-1", channel="dash-dev")

    async def boom(subject: str, envelope: Envelope) -> None:
        raise ConnectionError("bus down")

    fake.publish = boom  # type: ignore[method-assign]

    result = await adapter.send("dash-dev", "ack: on it")
    assert result.success is True  # swallowed, logged, turn unaffected


async def test_uncorrelated_send_is_not_noise(monkeypatch: pytest.MonkeyPatch) -> None:
    """An UNCORRELATED send still publishes normally — it is not noise.

    Noise routing is scoped to a correlated coordination turn (which has a
    completion contract). A standalone send has no such contract, so it must keep
    reaching the status lane rather than being quietly reclassified as noise and
    disappearing from the coordination path.
    """
    adapter, fake = _adapter(monkeypatch, agent="wren")
    await adapter.connect()

    await adapter.send("dash-dev", "a standalone reply")

    subject, env = fake.published[0]
    assert subject == subjects.agent_status("dev", "dash-dev", channel="dash-dev")
    assert env.kind == "status"


# --- channel scoping on the reply path (#117) ---------------------------------


async def test_reply_is_scoped_to_the_channel_it_answers(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Replies for different channels land on DIFFERENT subjects (#117 acceptance).

    Two dispatches for the same worker in two channels must produce two distinct
    scoped status subjects, so a channel-scoped observer of one never sees the
    other's traffic.
    """
    adapter, fake = _adapter(monkeypatch, agent="wren")
    await adapter.connect()
    handler = fake.subscriptions[subjects.agent_dispatch("dev", "wren")]

    await _dispatch(handler, task_id="task-a", channel="dash-dev")
    await adapter.send("dash-dev", _turn("answer a", done=True))
    await _dispatch(handler, task_id="task-b", channel="dash-prod")
    await adapter.send("dash-prod", _turn("answer b", done=True))

    published = [s for s, _ in fake.published]
    assert subjects.agent_status("dev", "task-a", channel="dash-dev") in published
    assert subjects.agent_status("dev", "task-b", channel="dash-prod") in published
    # No cross-channel bleed: neither task's reply used the other's channel token.
    assert subjects.agent_status("dev", "task-a", channel="dash-prod") not in published
    assert subjects.agent_status("dev", "task-b", channel="dash-dev") not in published


async def test_worker_receives_a_channel_scoped_relay(monkeypatch: pytest.MonkeyPatch) -> None:
    """A worker is subscribed to the SCOPED relay/wake forms the hub publishes (#117).

    Scoping relay/wake without pairing the wildcard subscription would be silent
    message loss (NATS core has no backlog), so this pins that the scoped subject
    the orchestrator now builds is one the worker actually attached to.
    """
    adapter, fake = _adapter(monkeypatch, agent="wren")
    await adapter.connect()

    scoped_relay = subjects.agent_relay_wildcard("dev", "wren")
    scoped_wake = subjects.agent_wake_wildcard("dev", "wren")
    assert scoped_relay in fake.subscriptions
    assert scoped_wake in fake.subscriptions

    # The concrete subject the hub publishes matches the wildcard subscribed.
    def _matches(pattern: str, subject: str) -> bool:
        pt, st = pattern.split("."), subject.split(".")
        return len(pt) == len(st) and all(p in ("*", s) for p, s in zip(pt, st, strict=True))

    assert _matches(scoped_relay, subjects.agent_relay("dev", "wren", channel="dash-dev"))
    assert _matches(scoped_wake, subjects.agent_wake("dev", "wren", channel="dash-dev"))
