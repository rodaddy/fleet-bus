"""Unit tests for the Phase-9 orchestrator run-loop (the hub), against a fake bus.

No network. A :class:`FakeBus` records publishes and delivers through the ONE
handler subscribed on a task status subject (relay/done, score, and offline are
composed behind that callback). A :class:`FakeKboard` records durable
writes. A controllable monotonic clock drives the offline ladder deterministically.

The live end-to-end proof over fleet-hub stays in ``test_e2e_live.py`` (auto-skips
off-network); this file proves the wiring deterministically.
"""

from __future__ import annotations

import asyncio
import itertools
import json
import threading
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest
from fleet_nats import (
    BusConfig,
    Envelope,
    PayloadTooLargeError,
    done_envelope,
    heartbeat_envelope,
    subjects,
)

from fleet_orchestrator import (
    HubConfig,
    IdClock,
    IntakeTask,
    OrchestratorHub,
    State,
)
from fleet_orchestrator.costfeed import CostLedger
from fleet_orchestrator.governor import Budget, Governor
from fleet_orchestrator.intake_poller import _REPUBLISH_AFTER_TICKS
from fleet_orchestrator.killswitch import KillSwitch
from fleet_orchestrator.loop import parse_intake

# ---- fakes -----------------------------------------------------------------


class FakeSub:
    """A subscription handle that records unsubscribe (teardown assertions)."""

    def __init__(self, subject: str) -> None:
        self.subject = subject
        self.cancelled = False

    async def unsubscribe(self) -> None:
        self.cancelled = True


class FakeMsg:
    """A raw NATS ``Msg`` double carrying the request bytes + a reply subject.

    ``serve_arbitration`` subscribes RAW (the reply subject lives on the ``Msg``,
    which ``FleetBus.subscribe`` decodes away) and decodes the request itself via
    ``Envelope.from_bytes(msg.data)``, so a raw responder test must deliver a
    ``Msg``-shaped object, not a decoded ``Envelope``.
    """

    def __init__(self, subject: str, data: bytes, reply: str) -> None:
        self.subject = subject
        self.data = data
        self.reply = reply


class FakeBus:
    """In-memory FleetBus double retaining every handler per subject."""

    def __init__(self, config: BusConfig, *, max_payload: int = 1_048_576) -> None:
        self._config = config
        self.published: list[tuple[str, Envelope]] = []
        self.handlers: dict[str, list[Any]] = {}
        self.raw_handlers: dict[str, list[Any]] = {}
        self.replies: list[tuple[str, Envelope]] = []
        self.subs: list[FakeSub] = []
        # Mirror the real bus max_payload ceiling so the size guard exercises real
        # encoded-byte sizing (a small value lets a modest payload trip it).
        self.max_payload = max_payload
        # Every (subject, encoded-bytes) the size guard checked — for round-4 parity
        # assertions (the bytes sized must equal the bytes published, per worker).
        self.checked: list[tuple[str, bytes]] = []

    @property
    def config(self) -> BusConfig:
        return self._config

    def check_payload_size(self, subject: str, envelope: Envelope) -> None:
        # Mirrors FleetBus.check_payload_size: raise on an oversized ENCODED envelope
        # without publishing (the pre-ownership guard start_task runs).
        data = envelope.to_bytes()
        self.checked.append((subject, data))
        if len(data) > self.max_payload:
            raise PayloadTooLargeError(subject, size=len(data), limit=self.max_payload)

    async def publish(self, subject: str, envelope: Envelope) -> None:
        self.published.append((subject, envelope))

    async def subscribe(self, subject: str, handler: Any) -> FakeSub:
        self.handlers.setdefault(subject, []).append(handler)
        sub = FakeSub(subject)
        self.subs.append(sub)
        return sub

    async def raw_subscribe(self, subject: str, handler: Any) -> FakeSub:
        # Raw handlers take a Msg, not an Envelope — track them separately so a
        # raw delivery reaches the arbitration responder with the right shape.
        self.raw_handlers.setdefault(subject, []).append(handler)
        sub = FakeSub(subject)
        self.subs.append(sub)
        return sub

    async def reply(self, msg: Any, envelope: Envelope) -> None:
        # Record the reply keyed by the request's reply subject (what the asker
        # is blocking on), mirroring FleetBus.reply publishing to msg.reply.
        self.replies.append((msg.reply, envelope))

    async def deliver(self, subject: str, envelope: Envelope) -> None:
        """Fan an envelope out to every handler subscribed on ``subject``."""
        for handler in list(self.handlers.get(subject, [])):
            await handler(envelope)

    async def deliver_raw(self, subject: str, msg: FakeMsg) -> None:
        """Fan a raw ``Msg`` out to every RAW handler subscribed on ``subject``."""
        for handler in list(self.raw_handlers.get(subject, [])):
            await handler(msg)


class FakeKboard:
    """Records durable writes without touching the network."""

    def __init__(self) -> None:
        self.statuses: list[tuple[str, str]] = []
        self.events: list[tuple[str, str, str]] = []

    async def list_tasks(self) -> list[Any]:
        # No cards by default — the intake poller sees an empty board. Poller
        # behavior over a populated board is proven in test_intake_poller.py.
        return []

    async def update_status(self, task_id: str, status: str) -> Any:
        from fleet_orchestrator.kboard import KboardTask

        self.statuses.append((task_id, status))
        return KboardTask(task_id, status, len(self.statuses), {})

    async def post_event(
        self, task_id: str, event_type: str, actor: str, body: str | None = None
    ) -> None:
        self.events.append((task_id, event_type, actor))

    async def close(self) -> None:  # pragma: no cover - lifecycle parity
        pass


class FakeGovernorStore:
    """In-memory GovernorStore double for hub durable-state lifecycle tests."""

    def __init__(self) -> None:
        self.saved: dict[str, dict[str, Any]] = {}
        self.deleted: list[str] = []

    async def persist(self, task_id: str, snapshot: dict[str, Any]) -> None:
        self.saved[task_id] = dict(snapshot)

    async def load(self, task_id: str) -> dict[str, Any] | None:
        return self.saved.get(task_id)

    async def delete(self, task_id: str) -> bool:
        self.deleted.append(task_id)
        return self.saved.pop(task_id, None) is not None


class Clock:
    """A controllable monotonic clock for the offline ladder."""

    def __init__(self) -> None:
        self.t = 0.0

    def __call__(self) -> float:
        return self.t

    def advance(self, seconds: float) -> None:
        self.t += seconds


def _counter_clock() -> IdClock:
    ids = (f"h{i}" for i in itertools.count())
    return IdClock(new_id=lambda: next(ids), now=lambda: "2026-07-08T00:00:00Z")


def _config(**overrides: Any) -> HubConfig:
    base: dict[str, Any] = dict(
        bus=BusConfig(agent_id="heron", env="dev", url="nats://127.0.0.1:4222"),
        manager="heron",
        workers=("wren", "finch"),
        budget=Budget(max_msgs=200, max_tokens=150_000),
        dispatch_timeout_s=300.0,
        offline_poll_s=1.0,
        heartbeat_interval_s=30.0,
        circlejerk_n=4,
        arbitration_enabled=True,
        arbitrate_topics=(),
        # Off by default in these wiring tests: the poller runs a background
        # k-board list loop and the FakeKboard here has no list_tasks; the poller's
        # own behavior (publish once / dedup / disabled) is proven in
        # test_intake_poller.py. Tests that need it on pass intake_poll_enabled=True.
        intake_poll_enabled=False,
        intake_poll_s=15.0,
        intake_channel="",
        # Empty by default (matches the disabled poller); tests that enable the
        # poller pass a marker (fleet-bus #54 requires one when enabled).
        intake_label="",
    )
    base.update(overrides)
    return HubConfig(**base)


def _hub(
    *,
    dry_run: bool = True,
    kboard: FakeKboard | None = None,
    bus_max_payload: int = 1_048_576,
    **config_overrides: Any,
) -> tuple[OrchestratorHub, FakeBus, FakeKboard, Clock]:
    from fleet_nats.registry import code_catalog

    config = _config(**config_overrides)
    bus = FakeBus(config.bus, max_payload=bus_max_payload)
    kboard = kboard if kboard is not None else FakeKboard()
    clock = Clock()
    governor = Governor(config.budget, ledger=CostLedger())
    kill_switch = KillSwitch(governor, poll_s=config.kill_switch_poll_s, probe=lambda: False)
    hub = OrchestratorHub(
        config,
        bus,  # type: ignore[arg-type]  # duck-typed subset the hub uses
        kboard,  # type: ignore[arg-type]
        clock=_counter_clock(),
        monotonic=clock,
        catalog=code_catalog(),
        dry_run=dry_run,
        governor=governor,
        kill_switch=kill_switch,
    )
    return hub, bus, kboard, clock


# ---- intake parsing --------------------------------------------------------


def _created(task_id: str | None, **payload: Any) -> Envelope:
    return Envelope.new(
        msg_id="i1",
        ts="t",
        sender="kboard",
        kind="kboard_event",
        task_id=task_id,
        channel=payload.pop("channel_field", None),
        payload=payload,
    )


def test_parse_intake_uses_default_workers_when_absent() -> None:
    intake = parse_intake(_created("t-1"), default_workers=("wren", "finch"))
    assert intake is not None
    assert intake.task_id == "t-1"
    assert intake.workers == ("wren", "finch")


def test_parse_intake_honors_payload_workers() -> None:
    env = _created("t-1", workers=["wren"], channel="dash-dev")
    intake = parse_intake(env, default_workers=("wren", "finch"))
    assert intake is not None
    assert intake.workers == ("wren",)
    assert intake.channel == "dash-dev"


def test_parse_intake_rejects_bad_worker_list() -> None:
    # A malformed workers list falls back to the configured default (never crashes).
    env = _created("t-1", workers=["wren", 42])
    intake = parse_intake(env, default_workers=("wren", "finch"))
    assert intake is not None
    assert intake.workers == ("wren", "finch")


def test_parse_intake_without_task_id_is_none() -> None:
    assert parse_intake(_created(None), default_workers=("wren",)) is None


def test_parse_intake_carries_task_text() -> None:
    # fleet-bus #52: the intake must carry the prompt text off the created payload.
    env = _created("t-1", text="do the thing", channel="dash-dev")
    intake = parse_intake(env, default_workers=("wren",))
    assert intake is not None
    assert intake.text == "do the thing"


def test_parse_intake_text_defaults_empty_when_absent() -> None:
    intake = parse_intake(_created("t-1"), default_workers=("wren",))
    assert intake is not None
    assert intake.text == ""


def test_parse_intake_non_string_text_becomes_empty() -> None:
    # A bogus non-string text must not reach the dispatch body — coerced to "".
    intake = parse_intake(_created("t-1", text=42), default_workers=("wren",))
    assert intake is not None
    assert intake.text == ""


# ---- start_task: dispatch + persist ----------------------------------------


async def test_start_task_dispatches_and_persists_working() -> None:
    hub, bus, kboard, _clock = _hub()
    await hub.start()
    session = await hub.start_task(IntakeTask("t-1", "dash-dev", ("wren", "finch")))
    assert session.state is State.WORKING
    # Dispatch fanned out to both workers on per-channel subjects.
    dispatch_subjects = {s for s, _ in bus.published if ".dispatch." in s}
    assert dispatch_subjects == {
        subjects.agent_dispatch("dev", "wren", channel="dash-dev"),
        subjects.agent_dispatch("dev", "finch", channel="dash-dev"),
    }
    # The WORKING transition was persisted durably to k-board (running status).
    assert ("t-1", "running") in kboard.statuses
    await hub.aclose()


def _dispatch_envelopes(bus: FakeBus) -> list[Envelope]:
    return [env for s, env in bus.published if ".dispatch." in s]


async def test_start_task_dispatch_envelope_carries_payload_text() -> None:
    # fleet-bus #52 core fix: the dispatch fanned out to each worker MUST carry the
    # task prompt as payload["text"] — the empty payload was why the shipped path
    # never delivered a prompt to any worker.
    hub, bus, _kboard, _clock = _hub()
    await hub.start()
    await hub.start_task(IntakeTask("t-1", "dash-dev", ("wren", "finch"), text="build the widget"))
    dispatches = _dispatch_envelopes(bus)
    assert len(dispatches) == 2  # one per worker
    assert all(env.payload.get("text") == "build the widget" for env in dispatches)
    # The identity keys ride along too (channel + task_id in the body).
    assert all(env.payload.get("task_id") == "t-1" for env in dispatches)
    assert all(env.payload.get("channel") == "dash-dev" for env in dispatches)
    await hub.aclose()


async def test_start_task_warns_on_empty_text_but_still_dispatches() -> None:
    # An empty prompt must WARN loudly (never silent — the exact silent failure that
    # shipped) yet still dispatch (the coordination session is real).
    from loguru import logger

    warnings: list[str] = []
    sink_id = logger.add(
        lambda m: warnings.append(m.record["message"]),
        level="WARNING",
        format="{message}",
    )
    try:
        hub, bus, _kboard, _clock = _hub()
        await hub.start()
        await hub.start_task(IntakeTask("t-1", "dash-dev", ("wren",), text=""))
        await hub.aclose()
    finally:
        logger.remove(sink_id)
    assert any("empty text" in w and "t-1" in w for w in warnings), warnings
    # It still dispatched — the empty prompt does not abort coordination.
    assert _dispatch_envelopes(bus), "an empty-text task should still dispatch"


async def test_start_task_warns_on_whitespace_only_text() -> None:
    # A whitespace-only prompt is what the subscriber drops (`not text.strip()`),
    # so it must WARN like an empty prompt, not slip through as "present" (codex
    # review, MAJOR).
    from loguru import logger

    warnings: list[str] = []
    sink_id = logger.add(
        lambda m: warnings.append(m.record["message"]), level="WARNING", format="{message}"
    )
    try:
        hub, _bus, _kboard, _clock = _hub()
        await hub.start()
        await hub.start_task(IntakeTask("t-1", "dash-dev", ("wren",), text="   \n\t "))
        await hub.aclose()
    finally:
        logger.remove(sink_id)
    assert any("empty text" in w and "t-1" in w for w in warnings), warnings


async def test_poller_intake_dispatch_flows_text_end_to_end() -> None:
    # The full shipped path, deterministically: the poller lists a card, publishes
    # created WITH the card's title+body as text, the hub's intake handler parses it
    # and dispatches — and the worker's dispatch envelope carries the real prompt.
    from fleet_orchestrator.kboard import KboardTask

    class CardKboard(FakeKboard):
        async def list_tasks(self) -> list[Any]:
            return [
                KboardTask(
                    task_id="t-1",
                    status="todo",
                    sync_version=1,
                    raw={},
                    title="[fleet] Ship the fix",
                    body="Carry the prompt to the worker.",
                )
            ]

    hub, bus, _kboard, _clock = _hub(
        kboard=CardKboard(),
        intake_poll_enabled=True,
        intake_channel="dash-dev",
        intake_label="[fleet]",
    )
    await hub.start()
    assert hub._intake_poller is not None  # noqa: SLF001 - poller is enabled
    # One poll publishes created (against the card-returning board wired above);
    # then deliver it to the hub's intake subscription so the session opens (the
    # FakeBus does not auto-route publishes to subscribers).
    await hub._intake_poller.poll_once()  # noqa: SLF001 - driving one deterministic tick
    created_subject = subjects.kboard_event("dev", "created", "t-1")
    created = next(env for s, env in bus.published if s == created_subject)
    assert created.payload["text"] == "[fleet] Ship the fix\n\nCarry the prompt to the worker."
    # Route the created event through the intake lane the hub actually subscribed
    # (the wildcard), exercising _on_intake → parse_intake → start_task for real.
    intake_lane = subjects.kboard_event("dev", "created", subjects.STAR)
    await bus.deliver(intake_lane, created)
    dispatches = _dispatch_envelopes(bus)
    assert dispatches, "the created card should have dispatched to the worker"
    assert all(
        env.payload.get("text") == "[fleet] Ship the fix\n\nCarry the prompt to the worker."
        for env in dispatches
    )
    await hub.aclose()


async def test_start_task_is_idempotent_for_a_tracked_task() -> None:
    hub, bus, _kboard, _clock = _hub()
    await hub.start()
    await hub.start_task(IntakeTask("t-1", "dash-dev", ("wren", "finch")))
    first = len(bus.published)
    await hub.start_task(IntakeTask("t-1", "dash-dev", ("wren", "finch")))
    # Second intake for the same task publishes nothing new.
    assert len(bus.published) == first
    await hub.aclose()


async def test_intake_without_task_id_is_dropped() -> None:
    hub, bus, _kboard, _clock = _hub()
    await hub.start()
    await hub._on_intake(_created(None))  # noqa: SLF001 - exercising the intake callback
    assert not any(".dispatch." in s for s, _ in bus.published)
    await hub.aclose()


# ---- relay: a worker finishes without handing off --------------------------


async def test_missing_handoff_fires_relay() -> None:
    hub, bus, _kboard, _clock = _hub()
    await hub.start()
    await hub.start_task(IntakeTask("t-1", "dash-dev", ("wren", "finch")))
    status_subject = subjects.agent_status("dev", "t-1")
    assert len(bus.handlers[status_subject]) == 1
    # wren finishes without @-mentioning finch → the orchestrator must relay.
    await bus.deliver(
        status_subject,
        Envelope.new(
            msg_id="ws1",
            ts="t",
            sender="wren",
            kind="status",
            task_id="t-1",
            payload={"text": "done my half"},
        ),
    )
    relayed = [
        s for s, _ in bus.published if s == subjects.agent_relay("dev", "finch", channel="dash-dev")
    ]
    assert len(relayed) == 1, "one delivery must fire at most one relay"
    await hub.aclose()


# ---- done sentinel: teardown + persist DONE --------------------------------


async def test_done_sentinel_persists_done_and_tears_down() -> None:
    hub, bus, kboard, _clock = _hub()
    await hub.start()
    await hub.start_task(IntakeTask("t-1", "dash-dev", ("wren", "finch")))
    status_subject = subjects.agent_status("dev", "t-1")
    await bus.deliver(
        status_subject,
        done_envelope(msg_id="dn1", ts="t", sender="finch", task_id="t-1", outcome="completed"),
    )
    # The task is no longer tracked, and DONE was persisted durably.
    assert not hub.is_tracking("t-1")
    assert ("t-1", "done") in kboard.statuses
    # Every per-task subscription was unsubscribed on teardown.
    task_subs = [s for s in bus.subs if s.subject == status_subject]
    assert len(task_subs) == 1
    assert task_subs[0].cancelled
    await hub.aclose()


async def test_hydrated_paused_task_is_held_without_working_or_dispatch() -> None:
    store = FakeGovernorStore()
    store.saved["t-1"] = {
        "task_id": "t-1",
        "msgs": 4,
        "paused": True,
        "breach_kind": "msgs",
        "breach_observed": 4,
        "breach_cap": 3,
        "cap_override": None,
        "tokens": None,
    }
    hub, bus, kboard, _clock = _hub(dry_run=False)
    hub._governor.attach_store(store)  # noqa: SLF001 - durable hydrate seam

    session = await hub.start_task(IntakeTask("t-1", "dash-dev", ("wren", "finch")))

    assert session.state is State.DISPATCHED
    assert hub.is_tracking("t-1")
    assert ("t-1", "running") not in kboard.statuses
    assert not any(".dispatch." in subject for subject, _env in bus.published)
    inbox = subjects.agent_inbox("dev", "heron")
    assert not any(subject == inbox for subject, _env in bus.published)
    await hub.aclose()


async def test_hydrated_nonterminal_task_resumes_counters_and_dispatches() -> None:
    store = FakeGovernorStore()
    store.saved["t-1"] = {
        "task_id": "t-1",
        "msgs": 2,
        "paused": False,
        "breach_kind": None,
        "breach_observed": None,
        "breach_cap": None,
        "cap_override": None,
        "tokens": None,
    }
    hub, bus, kboard, _clock = _hub()
    hub._governor.attach_store(store)  # noqa: SLF001 - durable hydrate seam

    session = await hub.start_task(IntakeTask("t-1", "dash-dev", ("wren", "finch")))

    assert session.state is State.WORKING
    assert hub._governor.snapshot("t-1")["msgs"] == 3  # noqa: SLF001
    assert ("t-1", "running") in kboard.statuses
    assert any(".dispatch." in subject for subject, _env in bus.published)
    await hub.aclose()


async def test_done_cleans_governor_state_before_teardown() -> None:
    store = FakeGovernorStore()
    store.saved["t-1"] = {
        "task_id": "t-1",
        "msgs": 2,
        "paused": False,
        "breach_kind": None,
        "breach_observed": None,
        "breach_cap": None,
        "cap_override": None,
        "tokens": None,
    }
    hub, bus, _kboard, _clock = _hub()
    hub._governor.attach_store(store)  # noqa: SLF001 - durable hydrate seam
    await hub.start_task(IntakeTask("t-1", "dash-dev", ("wren",)))

    await bus.deliver(
        subjects.agent_status("dev", "t-1"),
        done_envelope(msg_id="dn1", ts="t", sender="wren", task_id="t-1", outcome="completed"),
    )

    assert not hub.is_tracking("t-1")
    assert store.deleted == ["t-1"]
    assert store.saved.get("t-1") is None
    assert hub._governor.snapshot("t-1")["msgs"] == 0  # noqa: SLF001
    await hub.aclose()


async def test_same_task_id_after_done_starts_with_clean_governor_state() -> None:
    store = FakeGovernorStore()
    store.saved["t-1"] = {
        "task_id": "t-1",
        "msgs": 2,
        "paused": False,
        "breach_kind": None,
        "breach_observed": None,
        "breach_cap": None,
        "cap_override": None,
        "tokens": None,
    }
    hub, bus, _kboard, _clock = _hub()
    hub._governor.attach_store(store)  # noqa: SLF001 - durable hydrate seam
    await hub.start_task(IntakeTask("t-1", "dash-dev", ("wren",)))
    await bus.deliver(
        subjects.agent_status("dev", "t-1"),
        done_envelope(msg_id="dn1", ts="t", sender="wren", task_id="t-1", outcome="completed"),
    )

    restarted, bus_b, _kboard_b, _clock_b = _hub()
    restarted._governor.attach_store(store)  # noqa: SLF001 - durable hydrate seam
    await restarted.start_task(IntakeTask("t-1", "dash-dev", ("wren",)))

    assert restarted._governor.snapshot("t-1")["msgs"] == 1  # noqa: SLF001
    assert restarted._governor.is_paused("t-1") is False  # noqa: SLF001
    assert any(".dispatch." in subject for subject, _env in bus_b.published)
    await restarted.aclose()


async def test_done_persist_uses_sentinel_outcome_not_a_cell() -> None:
    # The terminal session is built from the sentinel OUTCOME by the hub's own
    # observer, not read from the cell watch_worker_status advances — so the DONE
    # persist can never be skipped by cross-subscription delivery order (the race
    # the earlier split design had). A non-default outcome proves it flows through.
    hub, bus, kboard, _clock = _hub(dry_run=False)
    await hub.start()
    await hub.start_task(IntakeTask("t-1", "dash-dev", ("wren", "finch")))
    await bus.deliver(
        subjects.agent_status("dev", "t-1"),
        done_envelope(msg_id="dn1", ts="t", sender="finch", task_id="t-1", outcome="abandoned"),
    )
    assert ("t-1", "done") in kboard.statuses
    # The done narration beat carries the sentinel's own outcome word.
    inbox = subjects.agent_inbox("dev", "heron")
    done_beats = [
        env for s, env in bus.published if s == inbox and "abandoned" in env.payload.get("text", "")
    ]
    assert done_beats, "the done beat should carry the sentinel outcome (abandoned)"
    await hub.aclose()


# ---- offline ladder --------------------------------------------------------


async def test_offline_poll_fires_wake_after_silence() -> None:
    # Large poll interval so the background loop never auto-ticks; drive one tick
    # deterministically via poll_offline_once (no sleep race).
    hub, bus, _kboard, clock = _hub(dispatch_timeout_s=10.0, offline_poll_s=3600.0)
    await hub.start()
    await hub.start_task(IntakeTask("t-1", "dash-dev", ("wren",)))
    clock.advance(50.0)  # no worker signal, well past the dispatch timeout
    await hub.poll_offline_once()
    wake_subjects = [
        s for s, _ in bus.published if s == subjects.agent_wake("dev", "wren", channel="dash-dev")
    ]
    assert wake_subjects, "the offline ladder should have woken the silent worker"
    await hub.aclose()


async def test_offline_watches_each_worker_independently() -> None:
    # A multi-worker task must watch EACH worker for silence. wren answers,
    # finch stays silent → only finch is woken. Proves the per-worker offline key
    # (without it, note_dispatch overwrites and only the last worker is watched).
    hub, bus, _kboard, clock = _hub(dispatch_timeout_s=10.0, offline_poll_s=3600.0)
    await hub.start()
    await hub.start_task(IntakeTask("t-1", "dash-dev", ("wren", "finch")))
    status_subject = subjects.agent_status("dev", "t-1")
    assert len(bus.handlers[status_subject]) == 1
    # Time passes, THEN wren signals — resetting only wren's ladder to a fresh
    # timestamp; finch never signals and stays anchored at dispatch time.
    clock.advance(45.0)
    await bus.deliver(
        status_subject,
        Envelope.new(
            msg_id="ws1",
            ts="t",
            sender="wren",
            kind="status",
            task_id="t-1",
            payload={"text": "@finch your turn"},  # mentions peer → no relay
        ),
    )
    clock.advance(5.0)  # finch is 50s silent (past window); wren only 5s since reset
    await hub.poll_offline_once()
    woke = {s for s, _ in bus.published if ".wake." in s}
    assert subjects.agent_wake("dev", "finch", channel="dash-dev") in woke, (
        "the silent worker (finch) must be woken"
    )
    assert subjects.agent_wake("dev", "wren", channel="dash-dev") not in woke, (
        "wren answered — must NOT be woken"
    )
    await hub.aclose()


# ---- dry-run: no narration -------------------------------------------------


async def test_dry_run_publishes_no_narration_beat() -> None:
    hub, bus, _kboard, _clock = _hub(dry_run=True)
    await hub.start()
    await hub.start_task(IntakeTask("t-1", "dash-dev", ("wren", "finch")))
    # In dry-run no narrator is wired, so nothing lands on the manager's inbox.
    inbox = subjects.agent_inbox("dev", "heron")
    assert not any(s == inbox for s, _ in bus.published)
    await hub.aclose()


async def test_narration_beat_lands_on_manager_inbox_when_wired() -> None:
    hub, bus, _kboard, _clock = _hub(dry_run=False)
    await hub.start()
    await hub.start_task(IntakeTask("t-1", "dash-dev", ("wren", "finch")))
    inbox = subjects.agent_inbox("dev", "heron")
    beats = [env for s, env in bus.published if s == inbox]
    assert beats, "a dispatched beat should land on the manager's inbox"
    # Narration carries no task prose — only the templated beat text.
    assert all("text" in env.payload for env in beats)
    await hub.aclose()


# ---- k-board resilience ----------------------------------------------------


async def test_kboard_failure_on_start_does_not_crash_the_hub() -> None:
    # A raising k-board (task not on the board / outage) must not take start_task
    # down — the dispatch still delivers over the bus; k-board reconciles later.
    hub, bus, _kboard, _clock = _hub()

    class RaisingKboard(FakeKboard):
        async def update_status(self, task_id: str, status: str) -> Any:
            raise RuntimeError("kboard: 404 (task not on board)")

    hub._kboard = RaisingKboard()  # type: ignore[assignment]  # noqa: SLF001 - fault injection
    await hub.start()
    session = await hub.start_task(IntakeTask("t-1", "dash-dev", ("wren", "finch")))
    # The task still coordinated: dispatch fanned out despite the durable-write fail.
    assert session.state is State.WORKING
    assert any(".dispatch." in s for s, _ in bus.published)
    await hub.aclose()


async def test_terminal_persist_failure_keeps_task_tracked_for_retry() -> None:
    # If the durable DONE write fails, the task must stay tracked (subs live) so a
    # re-emitted done retries (app-level; transport is at-most-once, C5) — the
    # card can't be silently left running forever.
    hub, bus, _kboard, _clock = _hub()

    class FlakyKboard(FakeKboard):
        def __init__(self) -> None:
            super().__init__()
            self.calls = 0

        async def update_status(self, task_id: str, status: str) -> Any:
            self.calls += 1
            # Fail the FIRST done write; succeed on the retry.
            if status == "done" and self.calls <= 2:
                raise RuntimeError("kboard: 503")
            return await super().update_status(task_id, status)

    hub._kboard = FlakyKboard()  # type: ignore[assignment]  # noqa: SLF001 - fault injection
    await hub.start()
    await hub.start_task(IntakeTask("t-1", "dash-dev", ("wren",)))
    done = done_envelope(msg_id="dn1", ts="t", sender="wren", task_id="t-1", outcome="completed")
    await bus.deliver(subjects.agent_status("dev", "t-1"), done)
    # First done write failed → task STILL tracked (retry window kept open).
    assert hub.is_tracking("t-1")
    # A re-emitted done retries the persist; this time it lands and finishes.
    await bus.deliver(subjects.agent_status("dev", "t-1"), done)
    assert not hub.is_tracking("t-1")
    await hub.aclose()


# ---- oversized fan-out: size-guard BEFORE ownership (round 3 controller ruling) --


def _oversized_intake_kboard(status_flip: Callable[[str], None] | None = None) -> FakeKboard:
    """A k-board whose todo card carries a body large enough to blow the fan-out.

    The dispatch envelope embeds the card body as payload["text"], so a big body
    makes the ENCODED dispatch envelope exceed the (small) bus max_payload the test
    configures — a real oversized fan-out, driven through the production intake path.
    """

    class _Card(FakeKboard):
        async def list_tasks(self) -> list[Any]:
            from fleet_orchestrator.kboard import KboardTask

            return [
                KboardTask(
                    task_id="t-big",
                    status="todo",
                    sync_version=1,
                    raw={},
                    title="[fleet] Oversized",
                    body="X" * 20_000,  # blows a small max_payload once encoded
                )
            ]

        async def update_status(self, task_id: str, status: str) -> Any:
            if status_flip is not None:
                status_flip(status)
            return await super().update_status(task_id, status)

    return _Card()


async def _drive_intake_once(hub: OrchestratorHub, bus: FakeBus) -> None:
    """Run one production intake cycle: poll a todo card, deliver its created event.

    Polls up to the republish grace window so a card still ``todo`` after a failed
    open (its ``created`` never landed a session) is re-published exactly as the
    live poller does — no test-only shortcut. Delivers the newest ``created``.
    """
    assert hub._intake_poller is not None  # noqa: SLF001 - poller enabled by the test
    created_subject = subjects.kboard_event("dev", "created", "t-big")
    before = sum(1 for s, _ in bus.published if s == created_subject)
    for _ in range(_REPUBLISH_AFTER_TICKS + 1):
        await hub._intake_poller.poll_once()  # noqa: SLF001 - deterministic ticks
        if sum(1 for s, _ in bus.published if s == created_subject) > before:
            break
    created = next(env for s, env in reversed(bus.published) if s == created_subject)
    intake_lane = subjects.kboard_event("dev", "created", subjects.STAR)
    await bus.deliver(intake_lane, created)  # _on_intake -> parse_intake -> start_task


async def test_oversized_fanout_aborts_before_ownership_and_parks_blocked() -> None:
    # Round 3: one oversized fan-out envelope aborts start_task BEFORE any publish,
    # WORKING persistence, or ownership — the card is flipped blocked while still
    # todo. Zero dispatches, task never tracked, correctly parked.
    kboard = _oversized_intake_kboard()
    hub, bus, _kb, _clock = _hub(
        kboard=kboard,
        bus_max_payload=4096,  # small ceiling so the 20k-body dispatch is oversized
        intake_poll_enabled=True,
        intake_channel="dash-dev",
        intake_label="[fleet]",
    )
    await hub.start()
    await _drive_intake_once(hub, bus)

    assert _dispatch_envelopes(bus) == []  # NO partial fan-out — nothing dispatched
    assert not hub.is_tracking("t-big")  # never owned
    assert ("t-big", "blocked") in kboard.statuses  # parked off todo -> blocked
    # No WORKING persistence: the only status write is the dead-letter blocked flip.
    assert all(status != "running" for _tid, status in kboard.statuses)
    await hub.aclose()


async def test_oversized_fanout_failed_flip_leaves_card_todo_for_next_poll() -> None:
    # Round 3: if the blocked flip FAILS, the card is untouched on todo (never owned,
    # never falsely DONE). The next poll cycle re-picks it and IS the retry owner —
    # once k-board heals the flip lands and the card parks blocked. No extra machinery.
    state = {"fail": True}

    def _maybe_fail(status: str) -> None:
        if status == "blocked" and state["fail"]:
            raise RuntimeError("kboard: 503 (blocked flip failed)")

    kboard = _oversized_intake_kboard(status_flip=_maybe_fail)
    hub, bus, _kb, _clock = _hub(
        kboard=kboard,
        bus_max_payload=4096,
        intake_poll_enabled=True,
        intake_channel="dash-dev",
        intake_label="[fleet]",
    )
    await hub.start()

    # First poll cycle: oversized + flip fails → nothing owned, nothing dispatched,
    # nothing durably parked; the card stays todo (untouched).
    await _drive_intake_once(hub, bus)
    assert _dispatch_envelopes(bus) == []
    assert not hub.is_tracking("t-big")
    assert ("t-big", "blocked") not in kboard.statuses

    # k-board heals; the NEXT poll cycle re-picks the still-todo card and retries.
    state["fail"] = False
    await _drive_intake_once(hub, bus)
    assert ("t-big", "blocked") in kboard.statuses  # now durably parked
    assert not hub.is_tracking("t-big")
    assert _dispatch_envelopes(bus) == []  # still never dispatched
    await hub.aclose()


async def test_oversized_fanout_does_not_report_blocked_when_board_returns_done() -> None:
    """A concurrent done task is not relabeled or reported as blocked."""
    from loguru import logger

    from fleet_orchestrator.kboard import KboardTask

    hub, _, kboard, _ = _hub(bus_max_payload=1)

    async def preserve_done(task_id: str, status: str) -> KboardTask:
        kboard.statuses.append((task_id, status))
        return KboardTask(task_id, "done", 9, {})

    kboard.update_status = preserve_done  # type: ignore[method-assign]
    logs: list[str] = []
    sink_id = logger.add(
        lambda message: logs.append(message.record["message"]),
        level="ERROR",
        format="{message}",
    )
    try:
        result = await hub.start_task(
            IntakeTask(task_id="t-done", channel="dash-dev", workers=("finch",), text="too big")
        )
    finally:
        logger.remove(sink_id)

    assert result.outcome != "blocked"
    assert not hub.is_tracking("t-done")
    outer_failure = [message for message in logs if "blocked NOT committed/reported" in message]
    assert outer_failure
    assert all("todo" not in message for message in outer_failure)
    assert all(
        "durable state may be unchanged or concurrently terminal" in message
        for message in outer_failure
    )
    await hub.aclose()


async def test_dispatch_publishes_the_prechecked_envelopes_verbatim() -> None:
    # Round 4 parity: the bytes the size guard checked must be the bytes published,
    # per worker. render_and_check_dispatch renders each fan-out envelope ONCE
    # (minting its id/timestamp) and start_task threads those exact artifacts to the
    # publish — so a counter-based id/timestamp re-render can never slip a
    # boundary-case envelope past the check. Multi-worker fan-out, all-fit path.
    class _Card(FakeKboard):
        async def list_tasks(self) -> list[Any]:
            from fleet_orchestrator.kboard import KboardTask

            return [
                KboardTask(
                    task_id="t-big",
                    status="todo",
                    sync_version=1,
                    raw={},
                    title="[fleet] Parity",
                    body="carry the prompt to every worker",
                )
            ]

    hub, bus, kboard, _clock = _hub(
        kboard=_Card(),
        intake_poll_enabled=True,
        intake_channel="dash-dev",
        intake_label="[fleet]",
    )
    await hub.start()
    await _drive_intake_once(hub, bus)

    # Fan-out went to BOTH default workers (wren, finch).
    dispatched = [(s, e) for s, e in bus.published if ".dispatch." in s]
    assert len(dispatched) == 2, dispatched
    # Every dispatch subject that was published was also size-checked, and the
    # checked bytes are byte-identical to the published bytes — per worker.
    checked = {s: data for s, data in bus.checked if ".dispatch." in s}
    assert set(checked) == {s for s, _ in dispatched}, "each dispatch was size-checked"
    for subject, envelope in dispatched:
        assert checked[subject] == envelope.to_bytes(), f"byte parity for {subject}"
    await hub.aclose()


async def test_done_for_foreign_task_id_is_dropped() -> None:
    # A done sentinel carrying a DIFFERENT task_id than the lane's must NOT finish
    # this task (parse_done only checks a task_id exists, not that it matches).
    hub, bus, kboard, _clock = _hub()
    await hub.start()
    await hub.start_task(IntakeTask("t-1", "dash-dev", ("wren",)))
    await bus.deliver(
        subjects.agent_status("dev", "t-1"),
        done_envelope(msg_id="dn1", ts="t", sender="wren", task_id="OTHER", outcome="completed"),
    )
    # The task is still tracked; no DONE was persisted for it.
    assert hub.is_tracking("t-1")
    assert ("t-1", "done") not in kboard.statuses
    await hub.aclose()


async def test_synthesize_decision_dispatches_to_manager_inbox() -> None:
    # A circlejerk trip with no evidenced winner → SYNTHESIZE → a synthesis TURN
    # dispatched to the manager's own inbox (shipped assign_synthesis behavior).
    # Runs in dry_run: synthesis is a coordination dispatch, not a narration beat.
    hub, bus, _kboard, _clock = _hub(dry_run=True, circlejerk_n=2)
    await hub.start()
    await hub.start_task(IntakeTask("t-1", "dash-dev", ("wren", "finch")))
    status = subjects.agent_status("dev", "t-1")
    assert len(bus.handlers[status]) == 1
    # Two consecutive agree turns (no dissent, no fact-pull) trip the circlejerk;
    # with no evidenced position the decider returns SYNTHESIZE.
    for i, sender in enumerate(("wren", "finch")):
        await bus.deliver(
            status,
            Envelope.new(
                msg_id=f"a{i}",
                ts="t",
                sender=sender,
                kind="status",
                task_id="t-1",
                payload={"text": "agreed", "meta": {"stance": "agree", "fact_pull": False}},
            ),
        )
        if i == 0:
            assert not any(
                subject == subjects.agent_inbox("dev", "heron") for subject, _env in bus.published
            ), "one delivery must count as one score observation, below the threshold"
    inbox = subjects.agent_inbox("dev", "heron")
    synth = [env for s, env in bus.published if s == inbox]
    assert len(synth) == 1, "two deliveries must trigger exactly one synthesis"
    await hub.aclose()


# ---- arbitration responder (#43) -------------------------------------------


async def test_arbitration_responder_answers_a_request_end_to_end() -> None:
    # A worker publishes an arbitrate request on the topic subject and blocks on a
    # reply; the wired responder must decide deterministically and reply on the
    # request's reply subject with the chosen worker (sorted-first = finch).
    hub, bus, _kboard, _clock = _hub(arbitrate_topics=("who-first",))
    await hub.start()
    subject = subjects.agent_arbitrate("dev", "who-first")
    request = Envelope.new(
        msg_id="req1", ts="t", sender="wren", kind="arbitrate", topic="who-first"
    )
    await bus.deliver_raw(subject, FakeMsg(subject, request.to_bytes(), reply="_INBOX.reply1"))
    # One reply landed on the asker's reply subject, correlated to the request,
    # carrying the deterministic decision (sorted-first of wren,finch = finch).
    replies = [env for reply_subj, env in bus.replies if reply_subj == "_INBOX.reply1"]
    assert replies, "the responder should have replied on the request's reply subject"
    reply = replies[0]
    assert reply.correlation_id == "req1"
    assert reply.payload["decision"] == "finch"
    await hub.aclose()


async def test_arbitration_disabled_wires_no_responder() -> None:
    # With arbitration disabled, no responder is stood up even if topics are set —
    # a request gets no reply (the pre-#43 behavior, opt-out honored).
    hub, bus, _kboard, _clock = _hub(arbitration_enabled=False, arbitrate_topics=("who-first",))
    await hub.start()
    subject = subjects.agent_arbitrate("dev", "who-first")
    assert subject not in bus.raw_handlers, "no arbitration responder should be subscribed"
    request = Envelope.new(
        msg_id="req1", ts="t", sender="wren", kind="arbitrate", topic="who-first"
    )
    await bus.deliver_raw(subject, FakeMsg(subject, request.to_bytes(), reply="_INBOX.reply1"))
    assert not bus.replies, "a disabled responder must not reply"
    await hub.aclose()


async def test_arbitration_enabled_but_no_topics_wires_no_responder() -> None:
    # Enabled (the default) with no configured topics is a no-op: arbitration is
    # opt-in per topic, so a hub with none behaves exactly as before.
    hub, bus, _kboard, _clock = _hub(arbitration_enabled=True, arbitrate_topics=())
    await hub.start()
    assert subjects.agent_arbitrate("dev", "who-first") not in bus.raw_handlers
    assert subjects.hub_show_work("dev") in bus.raw_handlers
    await hub.aclose()


async def test_arbitration_responder_torn_down_on_aclose() -> None:
    # The responder is a service-level subscription and must be cancelled on close.
    hub, bus, _kboard, _clock = _hub(arbitrate_topics=("who-first",))
    await hub.start()
    subject = subjects.agent_arbitrate("dev", "who-first")
    arb_subs = [s for s in bus.subs if s.subject == subject]
    assert arb_subs, "the arbitration responder should have registered a subscription"
    await hub.aclose()
    assert all(s.cancelled for s in arb_subs), "arbitration sub must be cancelled on aclose"


# ---- show_work responder (#64) ---------------------------------------------


async def test_show_work_responder_reconstructs_redacted_trail(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    journal = tmp_path / "trail.jsonl"
    journal.write_text(
        json.dumps(
            {
                "task_id": "task-64",
                "kind": "status",
                "sender": "wren",
                "text_preview": "token sk-ant-abcdefghij0123456789",
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("FLEET_TRACE_PATH", str(journal))
    hub, bus, _kboard, _clock = _hub()
    await hub.start()
    subject = subjects.hub_show_work("dev")
    request = Envelope.new(
        msg_id="show-1", ts="t", sender="wren", kind="show_work", payload={"task_id": "task-64"}
    )
    await bus.deliver_raw(subject, FakeMsg(subject, request.to_bytes(), reply="_INBOX.show"))
    reply = next(reply for destination, reply in bus.replies if destination == "_INBOX.show")
    assert reply.correlation_id == request.id
    assert "[REDACTED]" in str(reply.payload["markdown"])
    assert "sk-ant-abcdefghij" not in str(reply.payload["markdown"])
    await hub.aclose()


async def test_show_work_responder_rejects_malformed_and_missing_task() -> None:
    hub, bus, _kboard, _clock = _hub()
    await hub.start()
    subject = subjects.hub_show_work("dev")
    malformed = Envelope.new(msg_id="bad", ts="t", sender="wren", kind="show_work", payload={})
    await bus.deliver_raw(subject, FakeMsg(subject, malformed.to_bytes(), reply="_INBOX.bad"))
    assert bus.replies[-1][1].payload == {"error": "show_work: invalid request"}
    oversized = Envelope.new(
        msg_id="large",
        ts="t",
        sender="wren",
        kind="show_work",
        payload={"task_id": "x" * 257},
    )
    await bus.deliver_raw(subject, FakeMsg(subject, oversized.to_bytes(), reply="_INBOX.large"))
    assert bus.replies[-1][1].payload == {"error": "show_work: invalid request"}
    missing = Envelope.new(
        msg_id="missing", ts="t", sender="wren", kind="show_work", payload={"task_id": "missing"}
    )
    await bus.deliver_raw(subject, FakeMsg(subject, missing.to_bytes(), reply="_INBOX.missing"))
    assert bus.replies[-1][1].payload == {"error": "show_work: response unavailable"}
    await hub.aclose()


async def test_show_work_responder_preserves_bounded_truncation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from fleet_orchestrator import show_work

    journal = tmp_path / "trail.jsonl"
    journal.write_text(
        "\n".join(
            json.dumps(
                {
                    "task_id": "task-64",
                    "kind": "status",
                    "sender": "wren",
                    "text_preview": "token sk-ant-abcdefghij0123456789 " + "x" * 80,
                }
            )
            for index in range(8)
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("FLEET_TRACE_PATH", str(journal))
    monkeypatch.setattr(show_work, "_MAX_RESPONSE_BYTES", 500)
    monkeypatch.setattr(show_work, "_ARTIFACT_DIR", tmp_path / "artifacts")
    hub, bus, _kboard, _clock = _hub()
    await hub.start()
    request = Envelope.new(
        msg_id="truncated",
        ts="t",
        sender="wren",
        kind="show_work",
        payload={"task_id": "task-64"},
    )
    await bus.deliver_raw(
        subjects.hub_show_work("dev"), FakeMsg("show", request.to_bytes(), reply="_INBOX.truncated")
    )
    markdown = str(bus.replies[-1][1].payload["markdown"])
    assert len(markdown.encode("utf-8")) <= 500
    assert "truncated" in markdown.lower()
    assert "sk-ant-abcdefghij" not in markdown
    assert "[REDACTED]" in markdown
    await hub.aclose()


async def test_show_work_responder_lifecycle_is_hub_owned() -> None:
    hub, bus, _kboard, _clock = _hub()
    await hub.start()
    subject = subjects.hub_show_work("dev")
    responder_subs = [sub for sub in bus.subs if sub.subject == subject]
    assert responder_subs
    await hub.aclose()
    assert all(sub.cancelled for sub in responder_subs)


def test_show_work_clean_default_matches_production_trace_sink(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from fleet_orchestrator import show_work
    from fleet_orchestrator.trace import PRODUCTION_TRACE_PATH

    monkeypatch.delenv("FLEET_TRACE_PATH", raising=False)
    assert show_work._journal_path() == Path(PRODUCTION_TRACE_PATH)


async def test_show_work_rejects_missing_reply_before_journal_retrieval(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from fleet_orchestrator import show_work

    called = False

    def unexpected_render(_task_id: str) -> str:
        nonlocal called
        called = True
        raise AssertionError("missing reply must reject before retrieval")

    monkeypatch.setattr(show_work, "_render", unexpected_render)
    hub, bus, _kboard, _clock = _hub()
    await hub.start()
    request = Envelope.new(
        msg_id="no-reply", ts="t", sender="wren", kind="show_work", payload={"task_id": "task-64"}
    )
    await bus.deliver_raw(
        subjects.hub_show_work("dev"), FakeMsg("show", request.to_bytes(), reply="")
    )
    assert called is False
    assert not bus.replies
    await hub.aclose()


async def test_show_work_keeps_hub_loop_responsive_and_rejects_parallel_retrieval(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from fleet_orchestrator import show_work

    entered = threading.Event()
    release = threading.Event()

    def slow_render(_task_id: str) -> str:
        entered.set()
        assert release.wait(timeout=2)
        return "## Summary\ncomplete"

    monkeypatch.setattr(show_work, "_render", slow_render)
    hub, bus, _kboard, _clock = _hub()
    await hub.start()
    subject = subjects.hub_show_work("dev")
    first = Envelope.new(
        msg_id="first", ts="t", sender="wren", kind="show_work", payload={"task_id": "task-1"}
    )
    first_delivery = asyncio.create_task(
        bus.deliver_raw(subject, FakeMsg(subject, first.to_bytes(), reply="_INBOX.first"))
    )
    assert await asyncio.to_thread(entered.wait, 1)

    second = Envelope.new(
        msg_id="second", ts="t", sender="finch", kind="show_work", payload={"task_id": "task-2"}
    )
    await bus.deliver_raw(subject, FakeMsg(subject, second.to_bytes(), reply="_INBOX.second"))
    assert bus.replies[-1][1].payload == {"error": "show_work: busy"}

    control = Envelope.new(msg_id="control", ts="t", sender="rico", kind="control", payload={})
    await asyncio.wait_for(bus.deliver(subjects.hub_control_wildcard("dev"), control), timeout=0.1)
    release.set()
    await first_delivery
    assert bus.replies[-1][1].payload == {"markdown": "## Summary\ncomplete"}
    await hub.aclose()


def test_show_work_scans_past_unrelated_bytes_and_enforces_artifact_retention(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from fleet_orchestrator import show_work

    journal = tmp_path / "coordination.jsonl"
    unrelated = json.dumps({"task_id": "other", "kind": "status", "text_preview": "x" * 12_000})
    target = json.dumps({"task_id": "task-64", "kind": "status", "text_preview": "found"})
    journal.write_text("\n".join([unrelated] * 90 + [target]), encoding="utf-8")
    monkeypatch.setenv("FLEET_TRACE_PATH", str(journal))
    assert "found" in show_work._render("task-64")

    monkeypatch.setattr(show_work, "_ARTIFACT_DIR", tmp_path / "artifacts")
    monkeypatch.setattr(show_work, "_MAX_ARTIFACT_COUNT", 2)
    monkeypatch.setattr(show_work, "_MAX_ARTIFACT_TOTAL_BYTES", 1_000)
    for body in ("first", "second", "third"):
        assert show_work._write_full("task-64", body) is not None
    artifacts = sorted((tmp_path / "artifacts").glob("show-work-*.md"))
    assert len(artifacts) == 2
    assert sum(path.stat().st_size for path in artifacts) <= 1_000


def test_show_work_skips_oversized_unrelated_trace_but_refuses_target_trace(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from fleet_orchestrator import show_work

    monkeypatch.setattr(show_work, "_MAX_LINE_BYTES", 128)
    journal = tmp_path / "coordination.jsonl"
    unrelated = json.dumps({"action": "x" * 512, "task_id": "other", "ts": "t"})
    target = json.dumps({"action": "status", "task_id": "task-64", "ts": "t"})
    journal.write_text(f"{unrelated}\n{target}\n", encoding="utf-8")
    monkeypatch.setenv("FLEET_TRACE_PATH", str(journal))
    assert "task-64" in show_work._render("task-64")

    oversized_target = json.dumps({"action": "x" * 512, "task_id": "task-64", "ts": "t"})
    journal.write_text(f"{oversized_target}\n", encoding="utf-8")
    with pytest.raises(show_work.ShowWorkLimitError, match="oversized requested trace frame"):
        show_work._render("task-64")


def test_show_work_rebuilds_corrupt_existing_artifact_atomically(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from fleet_orchestrator import show_work

    monkeypatch.setattr(show_work, "_ARTIFACT_DIR", tmp_path / "artifacts")
    artifact_name = show_work._artifact_name("task-64", "complete artifact")
    artifact_dir = tmp_path / "artifacts"
    artifact_dir.mkdir(mode=0o700)
    (artifact_dir / artifact_name).write_text("partial", encoding="utf-8")

    assert show_work._write_full("task-64", "complete artifact") == f"artifact:{artifact_name}"
    assert (artifact_dir / artifact_name).read_bytes() == b"complete artifact"
    assert not list(artifact_dir.glob("*.tmp"))


async def test_show_work_refuses_symlinked_journal_and_artifact_paths(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from fleet_orchestrator import show_work

    journal_target = tmp_path / "target.jsonl"
    journal_target.write_text(
        json.dumps({"task_id": "task-64", "kind": "status"}), encoding="utf-8"
    )
    journal_link = tmp_path / "journal-link.jsonl"
    journal_link.symlink_to(journal_target)
    monkeypatch.setenv("FLEET_TRACE_PATH", str(journal_link))
    monkeypatch.setattr(show_work, "_ARTIFACT_DIR", tmp_path / "artifact-link")
    (tmp_path / "artifact-link").symlink_to(tmp_path / "outside", target_is_directory=True)
    with pytest.raises(show_work.ShowWorkLimitError):
        show_work._write_full("task-64", "full artifact")
    hub, bus, _kboard, _clock = _hub()
    await hub.start()
    request = Envelope.new(
        msg_id="hostile", ts="t", sender="wren", kind="show_work", payload={"task_id": "task-64"}
    )
    await bus.deliver_raw(
        subjects.hub_show_work("dev"), FakeMsg("show", request.to_bytes(), reply="_INBOX.hostile")
    )
    assert bus.replies[-1][1].payload == {"error": "show_work: response unavailable"}
    await hub.aclose()


async def test_show_work_registration_failure_unwinds_prior_hub_resources(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from fleet_orchestrator import loop

    async def fail_registration(*_args: object, **_kwargs: object) -> FakeSub:
        raise RuntimeError("show-work registration failed")

    monkeypatch.setattr(loop, "serve_show_work", fail_registration)
    hub, bus, _kboard, _clock = _hub()
    with pytest.raises(RuntimeError, match="show-work registration failed"):
        await hub.start()
    assert bus.subs and all(sub.cancelled for sub in bus.subs)


# ---- HubConfig.from_env fail-loud ------------------------------------------


def test_config_from_env_defaults_agent_id_to_manager(monkeypatch: Any) -> None:
    # FLEET_AGENT_ID unset must NOT leave the hub's bus identity as "unknown"
    # (which would stamp live envelopes) — it defaults to the manager id.
    monkeypatch.delenv("FLEET_AGENT_ID", raising=False)
    monkeypatch.setenv("FLEET_MANAGER_ID", "heron")
    monkeypatch.setenv("FLEET_WORKERS", "wren,finch")
    config = HubConfig.from_env()
    assert config.bus.agent_id == "heron"


def test_config_from_env_rejects_worker_with_separator(monkeypatch: Any) -> None:
    # A worker id containing the offline-key separator must fail loud at config.
    monkeypatch.setenv("FLEET_MANAGER_ID", "heron")
    monkeypatch.setenv("FLEET_WORKERS", "wren,bi::lby")
    import pytest

    with pytest.raises(ValueError, match="must not contain"):
        HubConfig.from_env()


def test_config_from_env_requires_manager(monkeypatch: Any) -> None:
    monkeypatch.delenv("FLEET_MANAGER_ID", raising=False)
    monkeypatch.setenv("FLEET_WORKERS", "wren")
    import pytest

    with pytest.raises(ValueError, match="FLEET_MANAGER_ID"):
        HubConfig.from_env()


def test_config_from_env_requires_workers(monkeypatch: Any) -> None:
    monkeypatch.setenv("FLEET_MANAGER_ID", "heron")
    monkeypatch.delenv("FLEET_WORKERS", raising=False)
    import pytest

    with pytest.raises(ValueError, match="FLEET_WORKERS"):
        HubConfig.from_env()


def test_config_from_env_arbitration_defaults_on_no_topics(monkeypatch: Any) -> None:
    # Arbitration defaults ON but with no topics configured (opt-in per topic).
    monkeypatch.setenv("FLEET_MANAGER_ID", "heron")
    monkeypatch.setenv("FLEET_WORKERS", "wren,finch")
    monkeypatch.delenv("FLEET_ARBITRATION_ENABLED", raising=False)
    monkeypatch.delenv("FLEET_ARBITRATE_TOPICS", raising=False)
    config = HubConfig.from_env()
    assert config.arbitration_enabled is True
    assert config.arbitrate_topics == ()


def test_config_from_env_parses_arbitrate_topics(monkeypatch: Any) -> None:
    monkeypatch.setenv("FLEET_MANAGER_ID", "heron")
    monkeypatch.setenv("FLEET_WORKERS", "wren,finch")
    monkeypatch.setenv("FLEET_ARBITRATE_TOPICS", " who-first , ownership ")
    config = HubConfig.from_env()
    assert config.arbitrate_topics == ("who-first", "ownership")


def test_config_from_env_arbitration_disabled_by_flag(monkeypatch: Any) -> None:
    monkeypatch.setenv("FLEET_MANAGER_ID", "heron")
    monkeypatch.setenv("FLEET_WORKERS", "wren,finch")
    monkeypatch.setenv("FLEET_ARBITRATION_ENABLED", "false")
    config = HubConfig.from_env()
    assert config.arbitration_enabled is False


def test_config_from_env_rejects_bad_arbitration_bool(monkeypatch: Any) -> None:
    # A typo'd bool must fail loud, not silently default arbitration on.
    monkeypatch.setenv("FLEET_MANAGER_ID", "heron")
    monkeypatch.setenv("FLEET_WORKERS", "wren,finch")
    monkeypatch.setenv("FLEET_ARBITRATION_ENABLED", "flase")
    import pytest

    with pytest.raises(ValueError, match="FLEET_ARBITRATION_ENABLED"):
        HubConfig.from_env()


def test_config_from_env_enabled_poller_requires_label(monkeypatch: Any) -> None:
    # fleet-bus #54, fail closed: an enabled poller with no card-scope marker would
    # open every card on the board (the fleet-hub first-enable grabbed 8 unrelated
    # cards). Refuse to start rather than run unscoped.
    monkeypatch.setenv("FLEET_MANAGER_ID", "heron")
    monkeypatch.setenv("FLEET_WORKERS", "wren,finch")
    monkeypatch.setenv("FLEET_INTAKE_POLL_ENABLED", "true")
    monkeypatch.delenv("FLEET_INTAKE_LABEL", raising=False)
    import pytest

    with pytest.raises(ValueError, match="FLEET_INTAKE_LABEL"):
        HubConfig.from_env()


def test_config_from_env_enabled_poller_with_label_starts(monkeypatch: Any) -> None:
    monkeypatch.setenv("FLEET_MANAGER_ID", "heron")
    monkeypatch.setenv("FLEET_WORKERS", "wren,finch")
    monkeypatch.setenv("FLEET_INTAKE_POLL_ENABLED", "true")
    monkeypatch.setenv("FLEET_INTAKE_LABEL", "[fleet]")
    config = HubConfig.from_env()
    assert config.intake_poll_enabled is True
    assert config.intake_label == "[fleet]"


def test_config_from_env_disabled_poller_needs_no_label(monkeypatch: Any) -> None:
    # A disabled poller opens nothing, so no marker is required.
    monkeypatch.setenv("FLEET_MANAGER_ID", "heron")
    monkeypatch.setenv("FLEET_WORKERS", "wren,finch")
    monkeypatch.setenv("FLEET_INTAKE_POLL_ENABLED", "false")
    monkeypatch.delenv("FLEET_INTAKE_LABEL", raising=False)
    config = HubConfig.from_env()
    assert config.intake_poll_enabled is False
    assert config.intake_label == ""


# ---- lifecycle -------------------------------------------------------------


async def test_start_then_aclose_cancels_service_subscriptions() -> None:
    hub, bus, _kboard, _clock = _hub()
    await hub.start()
    await hub.aclose()
    # Service subscriptions (intake, heartbeat, token-cost) were all cancelled.
    assert bus.subs and all(s.cancelled for s in bus.subs)


async def test_intake_poller_started_when_enabled_and_stopped_on_aclose() -> None:
    # When enabled, start() spins up the intake poller background task; aclose()
    # (via the shared _stop event + cancel) tears it down cleanly.
    hub, _bus, _kboard, _clock = _hub(intake_poll_enabled=True, intake_label="[fleet]")
    await hub.start()
    assert hub._intake_task is not None  # noqa: SLF001 - lifecycle assertion
    task = hub._intake_task  # noqa: SLF001
    await hub.aclose()
    assert task.done()
    assert hub._intake_task is None  # noqa: SLF001 - cleared on teardown


async def test_intake_poller_not_started_when_disabled() -> None:
    # Disabled config stands up no poller — no created publisher is wired.
    hub, _bus, _kboard, _clock = _hub(intake_poll_enabled=False)
    await hub.start()
    assert hub._intake_task is None  # noqa: SLF001 - lifecycle assertion
    await hub.aclose()


# ---- control plane (#59): external resume of a paused governor --------------


def _control(task_id: str, op: str = "resume", **payload: Any) -> Envelope:
    body: dict[str, Any] = {"op": op, "task_id": task_id, **payload}
    return Envelope.new(
        msg_id="c1", ts="t", sender="rico", kind="control", task_id=task_id, payload=body
    )


async def test_control_subscribed_on_the_hub_control_wildcard() -> None:
    hub, bus, _kboard, _clock = _hub()
    await hub.start()
    assert subjects.hub_control_wildcard("dev") in bus.handlers
    await hub.aclose()


async def test_control_resume_unblocks_a_paused_task_and_wake_proceeds() -> None:
    # The graduation-run gap (#59): a PAUSED task gates its offline wake forever; a
    # control resume frame reaches the LIVE governor and the next offline tick then
    # fires the wake to the silent worker. Two separate hubs so the gated case does
    # not consume the offline rung the resumed case then observes (the ladder
    # advances on check() whether or not the escalation was gated).

    # (a) PAUSED → the offline tick performs NO bus behavior for the worker.
    hub_a, bus_a, _kb_a, clock_a = _hub(dispatch_timeout_s=10.0, offline_poll_s=3600.0)
    await hub_a.start()
    await hub_a.start_task(IntakeTask("t-1", "dash-dev", ("wren",)))
    hub_a._governor.pause("t-1")  # noqa: SLF001 - stand in for a live over-budget pause
    baseline = len(bus_a.published)
    clock_a.advance(50.0)
    await hub_a.poll_offline_once()
    assert bus_a.published[baseline:] == [], "a paused task must perform NO bus behavior"
    await hub_a.aclose()

    # (b) PAUSED then RESUMED via a control frame → the same offline tick wakes.
    hub_b, bus_b, _kb_b, clock_b = _hub(dispatch_timeout_s=10.0, offline_poll_s=3600.0)
    await hub_b.start()
    await hub_b.start_task(IntakeTask("t-1", "dash-dev", ("wren",)))
    hub_b._governor.pause("t-1")  # noqa: SLF001
    await bus_b.deliver(subjects.hub_control_wildcard("dev"), _control("t-1"))
    assert hub_b._governor.is_paused("t-1") is False  # noqa: SLF001  # resume reached the governor
    after_resume = len(bus_b.published)
    clock_b.advance(50.0)
    await hub_b.poll_offline_once()
    wake = subjects.agent_wake("dev", "wren", channel="dash-dev")
    assert wake in {s for s, _ in bus_b.published[after_resume:]}, (
        "resume must let the gated wake proceed"
    )
    await hub_b.aclose()


async def test_control_resume_applies_bounded_budget_add() -> None:
    hub, bus, _kboard, _clock = _hub()
    await hub.start()
    hub._governor.pause("t-1")  # noqa: SLF001
    await bus.deliver(subjects.hub_control_wildcard("dev"), _control("t-1", budget_msgs_add=25))
    assert hub._governor.is_paused("t-1") is False  # noqa: SLF001
    # The per-task cap was raised: default 200 + 25 for t-1 only.
    for _ in range(225):
        assert hub._governor.note_message("t-1") is None  # noqa: SLF001
    assert hub._governor.note_message("t-1") is not None  # noqa: SLF001  # 226 > 225
    await hub.aclose()


async def test_control_pause_op_gates_a_task() -> None:
    hub, bus, _kboard, _clock = _hub()
    await hub.start()
    assert hub._governor.is_paused("t-1") is False  # noqa: SLF001
    await bus.deliver(subjects.hub_control_wildcard("dev"), _control("t-1", op="pause"))
    assert hub._governor.is_paused("t-1") is True  # noqa: SLF001
    await hub.aclose()


async def test_control_unknown_op_is_ignored_and_does_not_crash() -> None:
    hub, bus, _kboard, _clock = _hub()
    await hub.start()
    hub._governor.pause("t-1")  # noqa: SLF001
    # An unknown op must be a loud no-op: the pause is untouched, no crash.
    await bus.deliver(subjects.hub_control_wildcard("dev"), _control("t-1", op="detonate"))
    assert hub._governor.is_paused("t-1") is True  # noqa: SLF001 - still paused
    await hub.aclose()


async def test_control_malformed_frame_is_safe() -> None:
    hub, bus, _kboard, _clock = _hub()
    await hub.start()
    hub._governor.pause("t-1")  # noqa: SLF001
    # A control frame with no op key at all — dropped, no crash, pause intact.
    bad = Envelope.new(
        msg_id="c1", ts="t", sender="rico", kind="control", task_id="t-1", payload={}
    )
    await bus.deliver(subjects.hub_control_wildcard("dev"), bad)
    assert hub._governor.is_paused("t-1") is True  # noqa: SLF001
    # A malformed budget bump must not sink the resume that unblocks the lane.
    await bus.deliver(subjects.hub_control_wildcard("dev"), _control("t-1", budget_msgs_add="lots"))
    assert hub._governor.is_paused("t-1") is False  # noqa: SLF001 - resume still applied
    await hub.aclose()


# ---- fleet-liveness roster sweep (fleet-bus #78) ---------------------------


def _beat(agent: str, *, seq: int = 1) -> Envelope:
    return heartbeat_envelope(
        msg_id=f"hb-{agent}-{seq}",
        ts="t",
        sender=agent,
        role="worker",
        lanes=("inbox",),
        seq=seq,
        last_turn_ts=None,
    )


async def _feed_beat(bus: FakeBus, agent: str, *, seq: int = 1) -> None:
    # watch_heartbeats subscribes on the wildcard subject; the tracker keys per
    # agent from the envelope sender, so a beat delivered on the wildcard lane
    # feeds the roster sweep's is_stale cross-check.
    await bus.deliver(subjects.agent_heartbeat_wildcard("dev"), _beat(agent, seq=seq))


def _liveness_hub(
    *, dry_run: bool = False, **overrides: Any
) -> tuple[OrchestratorHub, FakeBus, FakeKboard, Clock]:
    # heartbeat_interval_s=10 so a beat at now=T is stale once (now-T) > 30s.
    # Overrides win over these defaults (a caller may widen the roster or shorten
    # the sweep), so build the kwargs then let overrides replace any key.
    kwargs: dict[str, Any] = dict(
        fleet_liveness_enabled=True,
        fleet_expected=("finch",),
        fleet_liveness_consecutive=2,
        fleet_liveness_sweep_s=3600.0,  # never auto-tick; drive poll manually
        heartbeat_interval_s=10.0,
    )
    kwargs.update(overrides)
    return _hub(dry_run=dry_run, **kwargs)


async def test_fleet_liveness_sweep_started_when_enabled_and_stopped_on_aclose() -> None:
    hub, _bus, _kboard, _clock = _liveness_hub()
    await hub.start()
    assert hub._fleet_liveness_task is not None  # noqa: SLF001 - lifecycle assertion
    task = hub._fleet_liveness_task  # noqa: SLF001
    await hub.aclose()
    assert task.done()
    assert hub._fleet_liveness_task is None  # noqa: SLF001 - cleared on teardown


async def test_fleet_liveness_sweep_not_started_when_disabled() -> None:
    hub, _bus, _kboard, _clock = _hub(dry_run=False, fleet_liveness_enabled=False)
    await hub.start()
    assert hub._fleet_liveness_task is None  # noqa: SLF001 - lifecycle assertion
    await hub.aclose()


async def test_fleet_liveness_below_threshold_is_silent() -> None:
    hub, bus, _kboard, clock = _liveness_hub()
    await hub.start()
    await _feed_beat(bus, "finch")  # last_seen = 0.0
    inbox = subjects.agent_inbox("dev", "heron")
    baseline = len(bus.published)
    clock.advance(40.0)  # stale (past 30s window)
    await hub.poll_fleet_liveness_once()  # 1 silent sweep — below threshold of 2
    assert not [s for s, _ in bus.published[baseline:] if s == inbox]
    await hub.aclose()


async def test_fleet_liveness_fires_one_missing_beat_after_n_stale() -> None:
    # Configure the hub's fleet channel: the session-less beat MUST carry it on the
    # envelope, else the manager's subscriber drops it channel-less (fleet-bus #72).
    hub, bus, _kboard, clock = _liveness_hub(intake_channel="dash-fleet")
    await hub.start()
    await _feed_beat(bus, "finch")
    inbox = subjects.agent_inbox("dev", "heron")
    clock.advance(40.0)
    await hub.poll_fleet_liveness_once()  # 1 silent
    await hub.poll_fleet_liveness_once()  # 2 silent -> exactly one missing beat
    missing = [
        env
        for s, env in bus.published
        if s == inbox and "missing from the bus" in env.payload.get("text", "")
    ]
    assert len(missing) == 1, "one missing beat on the manager inbox"
    assert "finch" in missing[0].payload["text"]
    assert missing[0].task_id == "fleet"
    # The beat carries the hub's fleet channel so envelope_to_message can resolve a
    # chat_id — a channel-less beat is silently dropped by the subscriber (#72).
    assert missing[0].channel == "dash-fleet", "session-less beat must carry the fleet channel"
    # Latched: another silent sweep does NOT re-narrate (no repeat spam).
    await hub.poll_fleet_liveness_once()
    still_one = [
        env
        for s, env in bus.published
        if s == inbox and "missing from the bus" in env.payload.get("text", "")
    ]
    assert len(still_one) == 1, "still exactly one — no repeat missing spam while silent"
    await hub.aclose()


async def test_fleet_liveness_narrates_recovery_after_missing() -> None:
    hub, bus, _kboard, clock = _liveness_hub()
    await hub.start()
    await _feed_beat(bus, "finch")
    inbox = subjects.agent_inbox("dev", "heron")
    clock.advance(40.0)
    await hub.poll_fleet_liveness_once()  # 1 silent
    await hub.poll_fleet_liveness_once()  # 2 silent -> missing
    # finch beats again — the tracker re-sees it at the current clock.
    await _feed_beat(bus, "finch", seq=2)
    await hub.poll_fleet_liveness_once()  # fresh -> exactly one recovered
    recovered = [
        env
        for s, env in bus.published
        if s == inbox and "back on the bus" in env.payload.get("text", "")
    ]
    assert len(recovered) == 1
    assert "finch" in recovered[0].payload["text"]
    await hub.aclose()


async def test_fleet_liveness_dry_run_sweeps_but_narrates_nothing() -> None:
    hub, bus, _kboard, clock = _liveness_hub(dry_run=True)
    await hub.start()
    await _feed_beat(bus, "finch")
    inbox = subjects.agent_inbox("dev", "heron")
    clock.advance(40.0)
    await hub.poll_fleet_liveness_once()
    await hub.poll_fleet_liveness_once()  # would fire missing, but no narrator
    assert not [s for s, _ in bus.published if s == inbox]
    await hub.aclose()


class _RaisingNarrator:
    """A narrator whose narrate_task ALWAYS raises — proves the sweep's per-beat
    guard: one bad narration must not propagate, skip peers, or kill the loop."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, str]] = []  # (beat, target) per attempt

    async def narrate_task(
        self, task_id: str, beat: str, *, channel: str | None = None, **fields: str
    ) -> None:
        self.calls.append((beat, fields.get("target", "")))
        raise RuntimeError("narrate boom")


async def test_fleet_liveness_narrate_failure_does_not_kill_the_sweep() -> None:
    # A raising narrator on EVERY fired transition must not (a) propagate out of
    # poll_fleet_liveness_once, (b) let one bad beat skip a peer transition in the
    # same tick, or (c) stop later ticks from firing (recovery still attempted).
    hub, bus, _kboard, clock = _liveness_hub(
        fleet_expected=("finch", "wren"),  # two agents → two transitions per tick
        fleet_liveness_consecutive=2,
    )
    await hub.start()
    boom = _RaisingNarrator()
    hub._narrator = boom  # type: ignore[assignment]  # noqa: SLF001 - inject the raising throat

    await _feed_beat(bus, "finch")
    await _feed_beat(bus, "wren")
    clock.advance(40.0)  # both agents now stale (past the 30s window)

    await hub.poll_fleet_liveness_once()  # 1 silent — no transition, no narrate
    assert boom.calls == []
    # 2 silent → BOTH agents fire a missing transition this tick. The first
    # narrate raises; the guard must swallow it AND still attempt the second.
    await hub.poll_fleet_liveness_once()  # must NOT raise
    missing_targets = {t for b, t in boom.calls if b == "fleet_silent"}
    assert missing_targets == {"finch", "wren"}, "a bad beat must not skip its peer"

    # A later tick still fires: finch recovers → a recovery beat is attempted even
    # though every prior narrate raised (the loop is not wedged by past failures).
    before = len(boom.calls)
    await _feed_beat(bus, "finch", seq=2)
    await hub.poll_fleet_liveness_once()  # must NOT raise
    assert ("fleet_present", "finch") in boom.calls[before:], (
        "recovery still attempted after failures"
    )

    # The background sweep task is alive and healthy (never crashed on an exception).
    assert hub._fleet_liveness_task is not None  # noqa: SLF001
    assert not hub._fleet_liveness_task.done()  # noqa: SLF001 - no exception killed it
    await hub.aclose()  # clean teardown despite the failing narrator
    assert hub._fleet_liveness_task is None  # noqa: SLF001


async def test_fleet_liveness_background_task_survives_a_narrate_failure_tick() -> None:
    # Drive the REAL background loop (not just poll_once): a short sweep interval so
    # the _fleet_liveness_poll task ticks on its own, hits the raising narrator, and
    # must keep running — the task stays alive, not .done() with a captured exception.
    hub, bus, _kboard, clock = _liveness_hub(fleet_liveness_sweep_s=0.01)
    await hub.start()
    boom = _RaisingNarrator()
    hub._narrator = boom  # type: ignore[assignment]  # noqa: SLF001
    await _feed_beat(bus, "finch")
    clock.advance(40.0)  # stale

    # Let the background loop tick several times through the failing narrator.
    import asyncio

    for _ in range(10):
        await asyncio.sleep(0)
        if any(b == "fleet_silent" for b, _ in boom.calls):
            break
    await asyncio.sleep(0.05)  # a few more real ticks past the failure
    assert any(b == "fleet_silent" for b, _ in boom.calls), "the loop must have fired a beat"
    assert hub._fleet_liveness_task is not None  # noqa: SLF001
    assert not hub._fleet_liveness_task.done()  # noqa: SLF001 - the tick failure did not kill it
    await hub.aclose()


async def test_fleet_liveness_never_seen_agent_narrated_through_the_loop() -> None:
    # An expected agent that NEVER beat (is_stale → None) is silent from tick one
    # and must be narrated missing through the real hub wiring, not just the unit
    # reporter — no heartbeat is ever fed for "ghost".
    hub, bus, _kboard, _clock = _liveness_hub(fleet_expected=("ghost",))
    await hub.start()
    inbox = subjects.agent_inbox("dev", "heron")
    await hub.poll_fleet_liveness_once()  # 1 silent (never seen)
    await hub.poll_fleet_liveness_once()  # 2 silent → missing
    missing = [
        env
        for s, env in bus.published
        if s == inbox and "missing from the bus" in env.payload.get("text", "")
    ]
    assert len(missing) == 1
    assert "ghost" in missing[0].payload["text"]
    await hub.aclose()


# ---- fleet-liveness config (fleet-bus #78) ---------------------------------


def test_config_from_env_enabled_sweep_requires_roster(monkeypatch: Any) -> None:
    import pytest

    monkeypatch.setenv("FLEET_MANAGER_ID", "heron")
    monkeypatch.setenv("FLEET_WORKERS", "wren,finch")
    monkeypatch.setenv("FLEET_LIVENESS_ENABLED", "true")
    monkeypatch.delenv("FLEET_EXPECTED_AGENTS", raising=False)
    with pytest.raises(ValueError, match="FLEET_EXPECTED_AGENTS is required"):
        HubConfig.from_env()


def test_config_from_env_enabled_sweep_with_roster_starts(monkeypatch: Any) -> None:
    monkeypatch.setenv("FLEET_MANAGER_ID", "heron")
    monkeypatch.setenv("FLEET_WORKERS", "wren,finch")
    monkeypatch.setenv("FLEET_LIVENESS_ENABLED", "true")
    monkeypatch.setenv("FLEET_EXPECTED_AGENTS", "wren,finch,heron")
    config = HubConfig.from_env()
    assert config.fleet_liveness_enabled is True
    assert config.fleet_expected == ("wren", "finch", "heron")


def test_config_from_env_disabled_sweep_needs_no_roster(monkeypatch: Any) -> None:
    monkeypatch.setenv("FLEET_MANAGER_ID", "heron")
    monkeypatch.setenv("FLEET_WORKERS", "wren,finch")
    monkeypatch.delenv("FLEET_LIVENESS_ENABLED", raising=False)
    monkeypatch.delenv("FLEET_EXPECTED_AGENTS", raising=False)
    config = HubConfig.from_env()
    assert config.fleet_liveness_enabled is False
    assert config.fleet_expected == ()


# ---- build_hub refuse-to-start (#103 P2-8) ---------------------------------


async def test_build_hub_refuses_to_start_below_floor_before_bus_connect(
    monkeypatch: Any,
) -> None:
    """build_hub enforces the catalog floor BEFORE opening the bus (#103 P2-8):
    a floor set + an unreachable registry raises CatalogVersionTooLowError, and
    the bus's connect() is NEVER called. Pin the ORDERING with a real fake bus
    that records connect(), not a proxy — connect_catalog runs before
    FleetBus.connect()."""
    from fleet_nats import CatalogVersionTooLowError, RegistryUnreachableError

    import fleet_orchestrator.loop as loop_mod

    connect_calls: list[str] = []

    class _ConnectRecordingBus:
        def __init__(self, config: Any) -> None:
            self._config = config

        async def connect(self) -> None:
            connect_calls.append("connect")

    monkeypatch.setattr(loop_mod, "FleetBus", _ConnectRecordingBus)
    # Floor set + a configured-but-unreachable registry → code catalog (v0) is
    # below the floor → refuse to start (as a registry OUTAGE, the distinct type).
    monkeypatch.setenv("FLEET_SUBJECT_CATALOG_MIN_VERSION", "3")
    monkeypatch.setenv("FLEET_SUBJECT_REGISTRY_URL", "http://127.0.0.1:1")  # nothing listening

    config = _config()
    with pytest.raises(CatalogVersionTooLowError) as exc:
        await loop_mod.build_hub(config, clock=_counter_clock(), monotonic=Clock(), dry_run=True)
    # An outage under a floor is the DISTINCT-and-loud subtype (k-board down),
    # not a stale-client error.
    assert isinstance(exc.value, RegistryUnreachableError)
    # The bus was never connected — enforcement is strictly pre-bus.
    assert connect_calls == []


async def test_build_hub_starts_when_floor_disabled(monkeypatch: Any) -> None:
    """No floor set → build_hub connects normally even on the code fallback
    (fail-open preserved): the bus connect() DOES run."""
    import fleet_orchestrator.loop as loop_mod

    connect_calls: list[str] = []

    class _FakeBusOK:
        def __init__(self, config: Any) -> None:
            self._config = config

        async def connect(self) -> None:
            connect_calls.append("connect")

    class _FakeKboardOK:
        pass

    monkeypatch.setattr(loop_mod, "FleetBus", _FakeBusOK)
    monkeypatch.setattr(loop_mod, "KboardClient", _FakeKboardOK)
    monkeypatch.delenv("FLEET_SUBJECT_CATALOG_MIN_VERSION", raising=False)
    monkeypatch.delenv("FLEET_SUBJECT_REGISTRY_URL", raising=False)

    config = _config()
    hub, bus, kboard = await loop_mod.build_hub(
        config, clock=_counter_clock(), monotonic=Clock(), dry_run=True
    )
    assert connect_calls == ["connect"]
    assert hub is not None


# ---- durable store kill authority wiring (#107) -----------------------------


def test_config_from_env_kill_authority_poll_defaults(monkeypatch: Any) -> None:
    monkeypatch.setenv("FLEET_MANAGER_ID", "heron")
    monkeypatch.setenv("FLEET_WORKERS", "wren,finch")
    monkeypatch.delenv("FLEET_KILL_SWITCH_POLL_SECONDS", raising=False)
    config = HubConfig.from_env()
    assert config.kill_switch_poll_s == 1.0
