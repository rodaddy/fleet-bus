"""Unit tests for the bus-wired behaviors, against a fake in-memory bus.

No network: a :class:`FakeBus` records published envelopes and lets tests drive
request/reply so dispatch fan-out, relay-on-missing-mention, arbitrate req→reply,
and wake are all provable deterministically. The live round-trip against fleet-hub
lives in ``test_behaviors_live.py`` (auto-skips off-network).
"""

from __future__ import annotations

import itertools
from typing import Any

import pytest
from fleet_nats import BusConfig, Envelope, subjects

from fleet_orchestrator import (
    ActionKind,
    CoordinationSession,
    Event,
    EventKind,
    IdClock,
    Orchestrator,
    State,
    done_envelope,
)
from fleet_orchestrator.governor import Budget, Governor
from fleet_orchestrator.score import Scorekeeper


class FakeMsg:
    """Minimal stand-in for a NATS ``Msg`` carrying a reply subject."""

    def __init__(self, data: bytes, reply: str) -> None:
        self.data = data
        self.reply = reply
        self.replied: Envelope | None = None


class _FakeSubscription:
    """A cancellable subscription handle, so a leak is observable in a test."""

    def __init__(self, bus: FakeBus, subject: str) -> None:
        self._bus = bus
        self.subject = subject

    async def unsubscribe(self) -> None:
        self._bus.unsubscribed.append(self.subject)


class FakeBus:
    """In-memory FleetBus double: records publishes, routes raw subscriptions."""

    def __init__(self, config: BusConfig) -> None:
        self._config = config
        self.published: list[tuple[str, Envelope]] = []
        self.raw_handlers: dict[str, Any] = {}
        self.handlers: dict[str, Any] = {}
        self.replies: list[tuple[FakeMsg, Envelope]] = []
        # Cross-runtime review: subscription handles used to be bare object()s,
        # so no test could observe whether a handle was ever cancelled. These
        # two knobs make a partial-subscribe failure expressible.
        self.subscribed: list[str] = []
        self.unsubscribed: list[str] = []
        self.fail_subscribe_after: int | None = None

    @property
    def config(self) -> BusConfig:
        return self._config

    async def publish(self, subject: str, envelope: Envelope) -> None:
        self.published.append((subject, envelope))

    async def raw_subscribe(self, subject: str, handler: Any) -> Any:
        self.raw_handlers[subject] = handler
        return object()  # a dummy subscription handle

    async def subscribe(self, subject: str, handler: Any) -> Any:
        if (
            self.fail_subscribe_after is not None
            and len(self.subscribed) >= self.fail_subscribe_after
        ):
            raise RuntimeError(f"subscribe failed for {subject}")
        self.handlers[subject] = handler
        self.subscribed.append(subject)
        return _FakeSubscription(self, subject)

    async def reply(self, msg: FakeMsg, envelope: Envelope) -> None:
        msg.replied = envelope
        self.replies.append((msg, envelope))


def _counter_clock() -> IdClock:
    ids = (f"m{i}" for i in itertools.count())
    return IdClock(new_id=lambda: next(ids), now=lambda: "2026-07-07T00:00:00Z")


def _orchestrator() -> tuple[Orchestrator, FakeBus]:
    cfg = BusConfig(agent_id="heron", env="dev")
    bus = FakeBus(cfg)
    # FakeBus duck-types the subset of FleetBus the Orchestrator uses; the cast is
    # test-only wiring, not production code.
    orch = Orchestrator(bus, _counter_clock())  # type: ignore[arg-type]
    return orch, bus


def _orchestrator_with_governor(governor: Any) -> tuple[Orchestrator, FakeBus]:
    """An Orchestrator wired with a governor (for the durable-persist tests)."""
    cfg = BusConfig(agent_id="heron", env="dev")
    bus = FakeBus(cfg)
    orch = Orchestrator(bus, _counter_clock(), governor=governor)  # type: ignore[arg-type]
    return orch, bus


def _session(state: State = State.DISPATCHED) -> CoordinationSession:
    return CoordinationSession(
        task_id="t-1",
        channel="dash-dev",
        manager="heron",
        workers=("wren", "finch"),
        state=state,
    )


async def test_dispatch_fans_out_to_all_workers() -> None:
    orch, bus = _orchestrator()
    action = await orch.apply(_session(), Event(kind=EventKind.DISPATCH))
    assert action.kind is ActionKind.DISPATCH
    assert action.session.state is State.WORKING
    # Two workers → two dispatch publishes, on per-channel subjects.
    assert len(bus.published) == 2
    dispatched_subjects = {s for s, _ in bus.published}
    assert dispatched_subjects == {
        subjects.agent_dispatch("dev", "wren", channel="dash-dev"),
        subjects.agent_dispatch("dev", "finch", channel="dash-dev"),
    }


async def test_dispatch_fanout_envelopes_carry_their_worker_in_to() -> None:
    # Each fan-out envelope's `to` must name its concrete worker (action.target is
    # None in fan-out — without the per-worker override, `to` would be lost).
    orch, bus = _orchestrator()
    await orch.apply(_session(), Event(kind=EventKind.DISPATCH))
    to_by_subject = {s: env.to for s, env in bus.published}
    assert to_by_subject[subjects.agent_dispatch("dev", "wren", channel="dash-dev")] == "wren"
    assert to_by_subject[subjects.agent_dispatch("dev", "finch", channel="dash-dev")] == "finch"


async def test_dispatch_to_single_worker_hint() -> None:
    orch, bus = _orchestrator()
    await orch.dispatch_task(_session(), Event(kind=EventKind.DISPATCH), worker="wren")
    assert len(bus.published) == 1
    subject, env = bus.published[0]
    assert subject == subjects.agent_dispatch("dev", "wren", channel="dash-dev")
    assert env.to == "wren"


async def test_relay_publishes_to_missed_peer() -> None:
    orch, bus = _orchestrator()
    event = Event(
        kind=EventKind.HANDOFF_MISSING,
        actor="wren",
        relay_to="finch",
        context={"text": "wren finished but never handed off"},
    )
    action = await orch.apply(_session(State.WORKING), event)
    assert action.kind is ActionKind.RELAY
    assert len(bus.published) == 1
    subject, env = bus.published[0]
    # Per-channel (#117), mirroring dispatch: the session's channel scopes the relay.
    assert subject == subjects.agent_relay("dev", "finch", channel="dash-dev")
    assert env.to == "finch"
    assert env.payload["text"] == "wren finished but never handed off"


async def test_wake_publishes_to_target() -> None:
    orch, bus = _orchestrator()
    # Wake is a manager-invoked behavior (no state transition), driven via the
    # public wake_worker() API — not a hand-built private call.
    await orch.wake_worker(_session(State.WORKING), "wren")
    assert len(bus.published) == 1
    subject, env = bus.published[0]
    assert subject == subjects.agent_wake("dev", "wren", channel="dash-dev")
    assert env.to == "wren"
    assert env.kind == "wake"


async def test_arbitrate_request_gets_deterministic_reply() -> None:
    orch, bus = _orchestrator()
    sub = await orch.serve_arbitration("who-goes-first", ("wren", "finch"))
    assert sub is not None
    subject = subjects.agent_arbitrate("dev", "who-goes-first")
    handler = bus.raw_handlers[subject]

    request = Envelope.new(
        msg_id="q1", ts="t", sender="wren", kind="arbitrate", topic="who-goes-first"
    )
    msg = FakeMsg(data=request.to_bytes(), reply="_INBOX.reply")
    await handler(msg)

    assert msg.replied is not None
    assert msg.replied.payload["decision"] == "finch"
    assert msg.replied.correlation_id == "q1"


async def test_serve_arbitration_empty_workers_raises_at_setup() -> None:
    orch, _ = _orchestrator()
    with pytest.raises(ValueError, match="no workers"):
        await orch.serve_arbitration("topic", ())


async def test_watch_status_fires_relay_on_missing_handoff() -> None:
    # The closed loop: a worker status with no @-mention of the peer drives the
    # detector → relay, over the (fake) bus, with no fabricated event.
    orch, bus = _orchestrator()
    sub, current = await orch.watch_worker_status(_session(State.WORKING))
    assert sub is not None
    status_subject = subjects.agent_status("dev", "t-1")
    handler = bus.handlers[status_subject]

    status = Envelope.new(
        msg_id="s1",
        ts="t",
        sender="wren",
        kind="status",
        task_id="t-1",
        payload={"text": "I finished my part."},  # no @finch mention
    )
    await handler(status)

    # A relay was published to the un-mentioned peer, and the session advanced.
    relay_subject = subjects.agent_relay("dev", "finch", channel="dash-dev")
    assert any(s == relay_subject for s, _ in bus.published)
    assert current().state is State.RELAY_NEEDED


async def test_watch_status_stops_rapid_alternating_relay_before_budget() -> None:
    """Three callback-level missing-handoff edges stop at the relay control.

    The stance metadata is realistic post-#100 input but inert here: subscriber
    gating is tested in fleet-nats; this test begins at the status callback.
    """

    class RecordingNarrator:
        def __init__(self) -> None:
            self.calls: list[tuple[str, str, str | None, str]] = []

        async def narrate_task(
            self, task_id: str, beat: str, *, channel: str | None = None, detail: str = ""
        ) -> None:
            self.calls.append((task_id, beat, channel, detail))

    class RecordingKboard:
        def __init__(self) -> None:
            self.events: list[tuple[str, str, str, str]] = []

        async def post_event(self, task_id: str, event: str, *, actor: str, body: str) -> None:
            self.events.append((task_id, event, actor, body))

    cfg = BusConfig(agent_id="heron", env="dev")
    bus = FakeBus(cfg)
    narrator = RecordingNarrator()
    kboard = RecordingKboard()
    times = iter((0.0, 1.0, 2.0))
    governor = Governor(Budget(max_msgs=100, max_tokens=100_000))
    orch = Orchestrator(
        bus,  # type: ignore[arg-type]
        _counter_clock(),
        governor=governor,
        narrator=narrator,  # type: ignore[arg-type]
        kboard=kboard,  # type: ignore[arg-type]
        monotonic=lambda: next(times),
    )
    await orch.watch_worker_status(_session(State.WORKING))
    handler = bus.handlers[subjects.agent_status("dev", "t-1")]

    for msg_id, sender in (("s1", "wren"), ("s2", "finch"), ("s3", "wren")):
        await handler(
            Envelope.new(
                msg_id=msg_id,
                ts="t",
                sender=sender,
                kind="status",
                task_id="t-1",
                payload={
                    "text": "Progress update.",
                    "meta": {"stance": "agree", "fact_pull": False},
                },
            )
        )

    relays = [(subject, env) for subject, env in bus.published if ".relay." in subject]
    assert [env.to for _subject, env in relays] == ["finch", "wren"]
    assert governor.is_paused("t-1")
    assert narrator.calls == [
        (
            "t-1",
            "relay_loop",
            "dash-dev",
            "relay loop detected: 3 alternating edges (threshold 3) within 5.0s",
        )
    ]
    assert len(kboard.events) == 1
    assert kboard.events[0][:3] == ("t-1", "comment", "heron")
    assert kboard.events[0][3].startswith("PAUSED by relay-loop reliability control:")
    assert "over budget" not in kboard.events[0][3]


async def test_watch_status_valid_handoff_resets_alternating_relay_history() -> None:
    """A valid callback-level handoff breaks an in-progress relay run."""
    cfg = BusConfig(agent_id="heron", env="dev")
    bus = FakeBus(cfg)
    times = iter((0.0, 1.0, 2.0, 3.0, 4.0))
    governor = Governor(Budget(max_msgs=100, max_tokens=100_000))
    orch = Orchestrator(
        bus,  # type: ignore[arg-type]
        _counter_clock(),
        governor=governor,
        monotonic=lambda: next(times),
    )
    await orch.watch_worker_status(_session(State.WORKING))
    handler = bus.handlers[subjects.agent_status("dev", "t-1")]

    async def status(msg_id: str, sender: str, text: str = "Progress update.") -> None:
        await handler(
            Envelope.new(
                msg_id=msg_id,
                ts="t",
                sender=sender,
                kind="status",
                task_id="t-1",
                payload={"text": text},
            )
        )

    await status("s1", "wren")
    await status("s2", "finch")
    await status("handoff", "wren", "Over to you @finch")
    await status("s3", "wren")
    await status("s4", "finch")

    assert governor.is_paused("t-1") is False
    relays = [(subject, env) for subject, env in bus.published if ".relay." in subject]
    assert [env.to for _subject, env in relays] == ["finch", "wren", "finch", "wren"]

    await status("s5", "wren")
    assert governor.is_paused("t-1") is True
    relays = [(subject, env) for subject, env in bus.published if ".relay." in subject]
    assert [env.to for _subject, env in relays] == ["finch", "wren", "finch", "wren"]


async def test_watch_status_no_relay_when_handoff_present() -> None:
    orch, bus = _orchestrator()
    _, current = await orch.watch_worker_status(_session(State.WORKING))
    handler = bus.handlers[subjects.agent_status("dev", "t-1")]
    status = Envelope.new(
        msg_id="s1",
        ts="t",
        sender="wren",
        kind="status",
        task_id="t-1",
        payload={"text": "over to you @finch"},
    )
    await handler(status)
    assert bus.published == []
    assert current().state is State.WORKING


async def test_watch_status_rejects_foreign_done_before_observer() -> None:
    orch, bus = _orchestrator()
    observed: list[str] = []

    async def observe(status: Envelope) -> None:
        observed.append(status.id)

    _, current = await orch.watch_worker_status(_session(State.WORKING), observe)
    handler = bus.handlers[subjects.agent_status("dev", "t-1")]
    await handler(
        done_envelope(
            msg_id="s1",
            ts="t",
            sender="wren",
            task_id="OTHER",
            outcome="completed",
        )
    )

    assert observed == []
    assert bus.published == []
    assert current().state is State.WORKING


async def test_watch_status_rejects_foreign_progress_before_all_mutation() -> None:
    orch, bus = _orchestrator()
    keeper = Scorekeeper()

    async def observe(status: Envelope) -> None:
        keeper.observe(status)

    _, current = await orch.watch_worker_status(_session(State.WORKING), observe)
    handler = bus.handlers[subjects.agent_status("dev", "t-1")]
    await handler(
        Envelope.new(
            msg_id="foreign-progress",
            ts="t",
            sender="wren",
            kind="status",
            task_id="OTHER",
            payload={"text": "I finished my part."},
        )
    )

    assert keeper.score_for("OTHER") is None
    assert bus.published == []
    assert current().state is State.WORKING


async def test_watch_status_rejects_forged_done_before_observer() -> None:
    orch, bus = _orchestrator()
    keeper = Scorekeeper()

    async def observe(status: Envelope) -> None:
        keeper.observe(status)

    _, current = await orch.watch_worker_status(_session(State.WORKING), observe)
    handler = bus.handlers[subjects.agent_status("dev", "t-1")]
    await handler(
        done_envelope(
            msg_id="forged-done",
            ts="t",
            sender="mallory",
            task_id="t-1",
            outcome="completed",
        )
    )

    assert keeper.score_for("t-1") is None
    assert bus.published == []
    assert current().state is State.WORKING


async def test_watch_status_payload_only_done_from_worker_advances() -> None:
    orch, bus = _orchestrator()
    _, current = await orch.watch_worker_status(_session(State.WORKING))
    handler = bus.handlers[subjects.agent_status("dev", "t-1")]
    await handler(
        Envelope.new(
            msg_id="payload-only-done",
            ts="t",
            sender="wren",
            kind="done",
            payload={"task_id": "t-1", "outcome": "completed"},
        )
    )

    assert bus.published == []
    assert current().state is State.DONE
    assert current().outcome == "completed"


async def test_watch_status_rejects_same_task_forged_progress_before_all_mutation() -> None:
    orch, bus = _orchestrator()
    keeper = Scorekeeper()

    async def observe(status: Envelope) -> None:
        keeper.observe(status)

    _, current = await orch.watch_worker_status(_session(State.WORKING), observe)
    handler = bus.handlers[subjects.agent_status("dev", "t-1")]
    await handler(
        Envelope.new(
            msg_id="forged-progress",
            ts="t",
            sender="mallory",
            kind="status",
            task_id="t-1",
            payload={"text": "I finished my part."},
        )
    )

    assert keeper.score_for("t-1") is None
    assert bus.published == []
    assert current().state is State.WORKING


async def test_watch_status_rejects_slug_collision_done_sender() -> None:
    orch, bus = _orchestrator()
    session = CoordinationSession(
        task_id="t-1",
        channel="dash-dev",
        manager="heron",
        workers=("foo bar", "finch"),
        state=State.WORKING,
    )
    _, current = await orch.watch_worker_status(session)
    handler = bus.handlers[subjects.agent_status("dev", "t-1")]
    await handler(
        done_envelope(
            msg_id="slug-collision",
            ts="t",
            sender="foo.bar",
            task_id="t-1",
            outcome="completed",
        )
    )

    assert bus.published == []
    assert current().state is State.WORKING


async def test_watch_status_observer_failure_still_fires_one_relay() -> None:
    orch, bus = _orchestrator()

    async def fail(_status: Envelope) -> None:
        raise RuntimeError("private observer detail")

    _, current = await orch.watch_worker_status(_session(State.WORKING), fail)
    handler = bus.handlers[subjects.agent_status("dev", "t-1")]
    await handler(
        Envelope.new(
            msg_id="s1",
            ts="t",
            sender="wren",
            kind="status",
            task_id="t-1",
            payload={"text": "I finished my part."},
        )
    )

    relays = [subject for subject, _env in bus.published if ".relay." in subject]
    assert relays == [subjects.agent_relay("dev", "finch", channel="dash-dev")]
    assert current().state is State.RELAY_NEEDED


async def test_watch_status_observer_failure_still_advances_done() -> None:
    orch, bus = _orchestrator()

    async def fail(_status: Envelope) -> None:
        raise RuntimeError("private observer detail")

    _, current = await orch.watch_worker_status(_session(State.WORKING), fail)
    handler = bus.handlers[subjects.agent_status("dev", "t-1")]
    await handler(
        done_envelope(
            msg_id="d1",
            ts="t",
            sender="wren",
            task_id="t-1",
            outcome="completed",
        )
    )

    assert current().state is State.DONE
    assert current().outcome == "completed"
    assert bus.published == []


async def test_watch_status_done_sentinel_advances_to_done() -> None:
    # A kind="done" sentinel on the status lane deterministically advances the
    # session to DONE (issue #6), NOT run through missing-handoff detection.
    orch, bus = _orchestrator()
    _, current = await orch.watch_worker_status(_session(State.WORKING))
    handler = bus.handlers[subjects.agent_status("dev", "t-1")]
    await handler(
        done_envelope(msg_id="d1", ts="t", sender="wren", task_id="t-1", outcome="completed")
    )
    assert current().state is State.DONE
    assert current().outcome == "completed"
    assert bus.published == []  # COMPLETE performs no bus publish


async def test_arbitrate_bad_request_is_dropped_no_reply() -> None:
    orch, bus = _orchestrator()
    await orch.serve_arbitration("topic", ("wren", "finch"))
    handler = bus.raw_handlers[subjects.agent_arbitrate("dev", "topic")]
    msg = FakeMsg(data=b"not json", reply="_INBOX.reply")
    await handler(msg)
    assert msg.replied is None  # poison request never produces a reply


async def test_noop_performs_no_publish() -> None:
    orch, bus = _orchestrator()
    action = await orch.apply(_session(State.WORKING), Event(kind=EventKind.WORKER_PROGRESS))
    assert action.kind is ActionKind.NOOP
    assert bus.published == []


async def test_complete_performs_no_publish() -> None:
    # WORKING + DONE → COMPLETE: advances to DONE, logs, publishes nothing.
    orch, bus = _orchestrator()
    action = await orch.apply(_session(State.WORKING), Event(kind=EventKind.DONE, outcome="ok"))
    assert action.kind is ActionKind.COMPLETE
    assert action.session.state is State.DONE
    assert bus.published == []


async def test_relay_no_target_guard_refuses_to_publish() -> None:
    # Defensive guard: a targetless RELAY action never publishes to a None subject.
    orch, bus = _orchestrator()
    from fleet_orchestrator.transitions import Action

    await orch._perform(
        Action(kind=ActionKind.RELAY, session=_session(State.RELAY_NEEDED), target=None)
    )
    assert bus.published == []


async def test_wake_no_target_guard_refuses_to_publish() -> None:
    orch, bus = _orchestrator()
    from fleet_orchestrator.transitions import Action

    await orch._perform(Action(kind=ActionKind.WAKE, session=_session(State.WORKING), target=None))
    assert bus.published == []


async def test_persist_transition_writes_kboard_then_emits_event() -> None:
    # A DONE transition must CAS-write status=done to k-board (the durable record)
    # AND emit a kboard_event(completed) on the bus (delivery) — with the durable
    # write STRICTLY BEFORE the emit. A shared timeline (the mock transport and the
    # bus both append to one list in call order) proves the ordering, not just that
    # both happened: reordering persist_transition to emit-first would fail here.
    import httpx

    from fleet_orchestrator import KboardClient

    timeline: list[str] = []

    def handle(request: httpx.Request) -> httpx.Response:
        timeline.append(request.method)
        if request.method == "GET":
            return httpx.Response(
                200, json={"task_id": "t-1", "status": "running", "sync_version": 3}
            )
        return httpx.Response(200, json={"task_id": "t-1", "status": "done", "sync_version": 4})

    orch, bus = _orchestrator()

    # Wrap the fake bus publish so an emit stamps the same timeline the k-board
    # transport does — making the relative order observable in one sequence.
    original_publish = bus.publish

    async def _timed_publish(subject: str, envelope: Envelope) -> None:
        timeline.append("EMIT")
        await original_publish(subject, envelope)

    bus.publish = _timed_publish  # type: ignore[method-assign]

    done_session = _session(State.WORKING).advanced_to(State.DONE, outcome="completed")
    async with KboardClient(client=httpx.AsyncClient(transport=httpx.MockTransport(handle))) as kb:
        await orch.persist_transition(done_session, kb)

    # The durable CAS write (GET→PATCH) completes BEFORE the bus emit.
    assert timeline == ["GET", "PATCH", "EMIT"]
    assert len(bus.published) == 1
    subject, env = bus.published[0]
    assert subject == subjects.kboard_event("dev", "completed", "t-1")
    assert env.kind == "kboard_event"
    assert env.payload == {"event": "completed", "status": "done"}


async def test_offline_decisions_drive_wake_redispatch_and_blocked_event() -> None:
    """apply_offline_decision maps each rung onto its concrete behavior (#25)."""
    import json

    import httpx

    from fleet_orchestrator import KboardClient
    from fleet_orchestrator.offline import Escalation, OfflineDecision

    orch, bus = _orchestrator()
    session = _session(State.WORKING)
    calls: list[dict[str, str]] = []

    def handle(request: httpx.Request) -> httpx.Response:
        calls.append(
            {"method": request.method, "url": str(request.url), "body": request.content.decode()}
        )
        # update_status GETs the current row (for its CAS version) then PATCHes;
        # give it a well-formed row so from_row parses a sync_version.
        if request.method == "GET":
            return httpx.Response(
                200, json={"task_id": "t-1", "status": "running", "sync_version": 7}
            )
        return httpx.Response(200, json={"task_id": "t-1", "status": "blocked", "sync_version": 8})

    async with KboardClient(client=httpx.AsyncClient(transport=httpx.MockTransport(handle))) as kb:
        await orch.apply_offline_decision(
            session, OfflineDecision("t-1", "finch", Escalation.WAKE, "silent"), kb
        )
        await orch.apply_offline_decision(
            session, OfflineDecision("t-1", "finch", Escalation.REDISPATCH, "still silent"), kb
        )
        await orch.apply_offline_decision(
            session, OfflineDecision("t-1", "finch", Escalation.BLOCKED, "exhausted"), kb
        )

    subjects_hit = [s for s, _ in bus.published]
    # Wake and dispatch are both per-channel (#117) — same session, same slice.
    assert subjects.agent_wake("dev", "finch", channel="dash-dev") in subjects_hit
    assert subjects.agent_dispatch("dev", "finch", channel="dash-dev") in subjects_hit
    # A redispatch carries a FRESH envelope id by design (C5: it is meant to run
    # again; the subscriber's Envelope.id dedup does not collapse it).
    ids = [e.id for _, e in bus.published]
    assert len(ids) == len(set(ids))
    # BLOCKED is durable k-board state: the card status FLIPS to blocked (a
    # GET→PATCH CAS write) AND a comment records the reason — NOT a bus publish.
    # Zombie-card guard (fleet-bus #73): without the status flip the card sits at
    # "running" forever.
    patches = [c for c in calls if c["method"] == "PATCH"]
    assert len(patches) == 1, "BLOCKED must flip the card status exactly once"
    assert "/api/tasks/t-1" in patches[0]["url"]
    assert json.loads(patches[0]["body"])["status"] == "blocked"
    events = [c for c in calls if "/api/tasks/t-1/events" in c["url"]]
    assert len(events) == 1
    assert "BLOCKED" in str(events[0]["body"])
    assert "finch" in str(events[0]["body"])
    # Ordering: the durable status flip (PATCH) commits STRICTLY BEFORE the visible
    # comment (events POST) — same "durable write first" contract persist_transition
    # holds. Assert on positions in the single call timeline, not just presence.
    patch_idx = next(i for i, c in enumerate(calls) if c["method"] == "PATCH")
    event_idx = next(i for i, c in enumerate(calls) if "/api/tasks/t-1/events" in c["url"])
    assert patch_idx < event_idx, "status flip must precede the comment"


async def test_blocked_status_flip_failure_still_posts_comment() -> None:
    """A failed status flip must NOT swallow the comment (BLOCKED is fire-once).

    The detector pops the watch before this runs, so there is no retry: if
    update_status raises, the comment is the only durable trace left — it must
    still post, apply_offline_decision must not raise, and the failure logs loud.
    """
    import httpx
    from loguru import logger

    from fleet_orchestrator import KboardClient
    from fleet_orchestrator.offline import Escalation, OfflineDecision

    orch, _ = _orchestrator()
    session = _session(State.WORKING)
    events: list[dict[str, str]] = []

    def handle(request: httpx.Request) -> httpx.Response:
        # update_status GETs the version, then PATCHes — fail the PATCH (500) so
        # the flip raises KboardError. The comment POST still succeeds.
        if request.method == "GET":
            return httpx.Response(
                200, json={"task_id": "t-1", "status": "running", "sync_version": 7}
            )
        if request.method == "PATCH":
            return httpx.Response(500, json={})
        events.append({"url": str(request.url), "body": request.content.decode()})
        return httpx.Response(200, json={})

    # Repo convention (test_loop.py): capture loguru via an added sink, not caplog.
    logged: list[str] = []
    sink_id = logger.add(lambda m: logged.append(str(m)), level="ERROR", format="{message}")
    try:
        async with KboardClient(
            client=httpx.AsyncClient(transport=httpx.MockTransport(handle))
        ) as kb:
            # Must NOT raise — the flip is best-effort, the comment is guaranteed.
            await orch.apply_offline_decision(
                session, OfflineDecision("t-1", "finch", Escalation.BLOCKED, "exhausted"), kb
            )
    finally:
        logger.remove(sink_id)

    # The comment still posted despite the failed flip.
    assert len(events) == 1
    assert "/api/tasks/t-1/events" in events[0]["url"]
    assert "BLOCKED" in str(events[0]["body"])
    # The flip failure was logged loud (task id + the failure), no secrets.
    assert any("status flip to 'blocked' failed" in line and "t-1" in line for line in logged), (
        logged
    )
    assert any("comment will still be attempted" in line for line in logged), logged
    assert not any("comment still posted" in line for line in logged), logged
    assert any("BLOCKED comment posted" in line for line in logged), logged
    assert not any("status=blocked surfaced" in line for line in logged), logged


@pytest.mark.parametrize("conflict_before_done", [False, True])
async def test_blocked_escalation_preserves_done_without_comment_or_success_log(
    conflict_before_done: bool,
) -> None:
    """A done card wins both the initial read and a conflict refetch race."""
    import httpx
    from loguru import logger

    from fleet_orchestrator import KboardClient
    from fleet_orchestrator.offline import Escalation, OfflineDecision

    orch, _ = _orchestrator()
    session = _session(State.WORKING)
    calls: list[str] = []

    def handle(request: httpx.Request) -> httpx.Response:
        calls.append(request.method)
        if request.method == "POST":
            raise AssertionError("a preserved done task must not receive a BLOCKED comment")
        if request.method == "GET":
            status = "running" if conflict_before_done and calls.count("GET") == 1 else "done"
            version = 7 if status == "running" else 8
            return httpx.Response(
                200, json={"task_id": "t-1", "status": status, "sync_version": version}
            )
        if request.method == "PATCH" and conflict_before_done:
            return httpx.Response(409, json={"error": "stale"})
        raise AssertionError(f"unexpected {request.method}")

    logged: list[str] = []
    sink_id = logger.add(lambda m: logged.append(str(m)), format="{message}")
    try:
        async with KboardClient(
            client=httpx.AsyncClient(transport=httpx.MockTransport(handle))
        ) as kb:
            await orch.apply_offline_decision(
                session, OfflineDecision("t-1", "finch", Escalation.BLOCKED, "exhausted"), kb
            )
    finally:
        logger.remove(sink_id)

    assert calls == (["GET", "PATCH", "GET"] if conflict_before_done else ["GET"])
    assert not any("status=blocked surfaced" in line for line in logged), logged
    assert any("durable status=done" in line and "no BLOCKED comment" in line for line in logged)


async def test_blocked_comment_failure_after_status_write_logs_loudly() -> None:
    """A fire-once blocked reason must not disappear silently after the status commits."""
    import httpx
    from loguru import logger

    from fleet_orchestrator import KboardClient
    from fleet_orchestrator.offline import Escalation, OfflineDecision

    orch, _ = _orchestrator()
    session = _session(State.WORKING)
    calls: list[str] = []

    def handle(request: httpx.Request) -> httpx.Response:
        calls.append(request.method)
        if request.method == "GET":
            return httpx.Response(
                200, json={"task_id": "t-1", "status": "running", "sync_version": 7}
            )
        if request.method == "PATCH":
            return httpx.Response(
                200, json={"task_id": "t-1", "status": "blocked", "sync_version": 8}
            )
        return httpx.Response(500, json={})

    logged: list[str] = []
    sink_id = logger.add(lambda m: logged.append(str(m)), level="ERROR", format="{message}")
    try:
        async with KboardClient(
            client=httpx.AsyncClient(transport=httpx.MockTransport(handle))
        ) as kb:
            await orch.apply_offline_decision(
                session, OfflineDecision("t-1", "finch", Escalation.BLOCKED, "exhausted"), kb
            )
    finally:
        logger.remove(sink_id)

    assert calls == ["GET", "PATCH", "POST"]
    assert any(
        "comment failed after status=blocked" in line and "t-1" in line for line in logged
    ), logged


async def test_blocked_status_and_comment_failures_claim_neither_write_succeeded() -> None:
    """Best-effort failure stays non-fatal and diagnostics remain truthful."""
    import httpx
    from loguru import logger

    from fleet_orchestrator import KboardClient
    from fleet_orchestrator.offline import Escalation, OfflineDecision

    orch, _ = _orchestrator()
    calls: list[str] = []

    def handle(request: httpx.Request) -> httpx.Response:
        calls.append(request.method)
        if request.method == "GET":
            return httpx.Response(
                200, json={"task_id": "t-1", "status": "running", "sync_version": 7}
            )
        return httpx.Response(500, json={})

    logged: list[str] = []
    sink_id = logger.add(lambda m: logged.append(str(m)), level="ERROR", format="{message}")
    try:
        async with KboardClient(
            client=httpx.AsyncClient(transport=httpx.MockTransport(handle))
        ) as kb:
            await orch.apply_offline_decision(
                _session(State.WORKING),
                OfflineDecision("t-1", "finch", Escalation.BLOCKED, "exhausted"),
                kb,
            )
    finally:
        logger.remove(sink_id)

    assert calls == ["GET", "PATCH", "POST"]
    assert any("status flip to 'blocked' failed" in line for line in logged), logged
    assert any("status=blocked was not proven" in line for line in logged), logged
    assert not any("status=blocked surfaced" in line for line in logged), logged
    assert not any("comment still posted" in line for line in logged), logged


async def test_persist_transition_suppresses_event_when_durable_status_disagrees() -> None:
    """The bus must not announce a lifecycle state the durable task rejected."""
    from fleet_orchestrator.kboard import KboardTask

    class DoneKboard:
        async def update_status(self, task_id: str, status: str) -> KboardTask:
            return KboardTask(task_id, "done", 9, {})

    orch, bus = _orchestrator()
    await orch.persist_transition(_session(State.WORKING), DoneKboard())  # type: ignore[arg-type]

    assert bus.published == []


# ---- Durable governor persist is CAUSAL with the count (fleet-bus #106) ------
# codex P1: persisting from a SEPARATE status subscription raced the counting
# callback — a snapshot could capture msgs=0 before the count landed. The persist
# now lives in the counting handler (watch_worker_status → apply → persist), so
# the durable snapshot always observes the post-increment count. These tests drive
# the REAL registered status handler and assert the persisted count, which the
# pre-fix (separate-subscription) code could not guarantee.


class _RecordingStore:
    """A GovernorStore that records every persisted snapshot in order."""

    def __init__(self) -> None:
        self.persisted: list[dict[str, Any]] = []

    async def persist(self, task_id: str, snapshot: dict[str, Any]) -> None:
        self.persisted.append(dict(snapshot))

    async def load(self, task_id: str) -> dict[str, Any] | None:
        return self.persisted[-1] if self.persisted else None


def _status_env(sender: str, text: str, task_id: str = "t-1") -> Envelope:
    return Envelope.new(
        msg_id="s1",
        ts="2026-07-10T00:00:00Z",
        sender=sender,
        kind="status",
        task_id=task_id,
        channel="dash-dev",
        payload={"text": text},
    )


async def test_status_handler_persists_the_post_increment_count() -> None:
    # A missing-handoff status counts (note_message) AND persists in the SAME
    # handler. The persisted snapshot must reflect the incremented count — proving
    # persist is causally after the count, not a racing separate subscription.
    from fleet_orchestrator.governor import Budget, Governor

    store = _RecordingStore()
    gov = Governor(Budget(max_msgs=100, max_tokens=100_000), store=store)
    orch, bus = _orchestrator_with_governor(gov)

    session = CoordinationSession(
        task_id="t-1",
        channel="dash-dev",
        manager="heron",
        workers=("wren", "finch"),
        state=State.WORKING,
    )
    sub, accessor = await orch.watch_worker_status(session)
    subject = subjects.agent_status("dev", "t-1")
    handler = bus.handlers[subject]

    # A status with no handoff mention triggers a relay → apply → note_message.
    await handler(_status_env(sender="wren", text="done, no mention of anyone"))

    assert store.persisted, "the counting handler must have persisted"
    # The persisted snapshot observed the incremented count (>= 1), never msgs=0.
    assert store.persisted[-1]["msgs"] >= 1
    assert gov.snapshot("t-1")["msgs"] == store.persisted[-1]["msgs"]


async def test_a_failed_second_status_subscribe_does_not_leak_the_first() -> None:
    """Cross-runtime review: the two status subscriptions were a tuple literal.

    A tuple literal evaluates left to right, so when the SECOND subscribe raises
    (server hiccup, permissions denial) the FIRST is already live on the
    connection while its handle is discarded with the half-built tuple. The
    caller never receives it, so nothing can ever cancel it: that subscription
    keeps delivering into ``_on_status`` for the life of the process, and a
    retry then has two handlers observing the same task through two different
    session cells.
    """
    orch, bus = _orchestrator()
    session = CoordinationSession(
        task_id="t-1",
        channel="dash-dev",
        manager="heron",
        workers=("wren", "finch"),
        state=State.WORKING,
    )
    bus.fail_subscribe_after = 1  # the bare form lands, the scoped one fails

    with pytest.raises(RuntimeError):
        await orch.watch_worker_status(session)

    assert bus.subscribed, "the first subscribe must have happened for this to be a leak test"
    assert bus.unsubscribed == bus.subscribed, (
        f"leaked subscription(s): opened {bus.subscribed}, cancelled {bus.unsubscribed}"
    )


async def test_status_watch_task_does_not_double_persist() -> None:
    # The separate hub observer (_watch_task) must NOT persist any more — only the
    # counting handler does. Driving only the counting handler yields exactly the
    # persists from that handler, one per counted message, with a strictly
    # increasing ``msgs`` count (the anti-backward-roll guard is now the SERVER's
    # KV revision-CAS, not an application-layer ``version`` field on the snapshot).
    from fleet_orchestrator.governor import Budget, Governor

    store = _RecordingStore()
    gov = Governor(Budget(max_msgs=100, max_tokens=100_000), store=store)
    orch, bus = _orchestrator_with_governor(gov)
    session = CoordinationSession(
        task_id="t-1",
        channel="dash-dev",
        manager="heron",
        workers=("wren", "finch"),
        state=State.WORKING,
    )
    await orch.watch_worker_status(session)
    handler = bus.handlers[subjects.agent_status("dev", "t-1")]

    await handler(_status_env(sender="wren", text="no mention one"))
    await handler(_status_env(sender="finch", text="no mention two"))

    counts = [snap["msgs"] for snap in store.persisted]
    assert counts == [1, 2]  # one persist per counted message; monotonic, no double-persist
