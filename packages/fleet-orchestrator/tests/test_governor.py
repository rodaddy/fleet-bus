"""Governor tests (#13): deterministic cap breach → forced surface + pause."""

from __future__ import annotations

import itertools
from typing import Any

import pytest
from fleet_nats import NARRATION_KIND, BusConfig, Envelope, subjects, token_cost_envelope

from fleet_orchestrator.behaviors import IdClock, Orchestrator
from fleet_orchestrator.costfeed import CostLedger
from fleet_orchestrator.governor import Breach, BreachKind, Budget, Governor, Narrator
from fleet_orchestrator.state import CoordinationSession, Event, EventKind, State
from fleet_orchestrator.transitions import Action, ActionKind


def _session(task_id: str = "t-1") -> CoordinationSession:
    return CoordinationSession(
        task_id=task_id,
        channel="dash-dev",
        manager="nagatha",
        workers=("skippy", "bilby"),
        state=State.WORKING,
    )


def _cost(task_id: str, turn: str, total: int) -> Envelope:
    return token_cost_envelope(
        msg_id=f"tc-{turn}",
        ts="2026-07-08T00:00:00Z",
        sender="bilby",
        task_id=task_id,
        turn_id=turn,
        prompt_tokens=total,
        completion_tokens=0,
        total_tokens=total,
        model="claude-fable-5",
    )


def test_governor_message_cap_breaches_past_boundary_not_at_it() -> None:
    gov = Governor(Budget(max_msgs=3, max_tokens=10_000))
    assert gov.note_message("t-1") is None  # 1
    assert gov.note_message("t-1") is None  # 2
    assert gov.note_message("t-1") is None  # 3 == cap: NOT a breach
    breach = gov.note_message("t-1")  # 4 > cap
    assert isinstance(breach, Breach)
    assert breach.kind is BreachKind.MSGS
    assert (breach.observed, breach.cap) == (4, 3)
    assert gov.is_paused("t-1") is True


def test_governor_token_cap_breaches_from_the_feed() -> None:
    ledger = CostLedger()
    gov = Governor(Budget(max_msgs=1000, max_tokens=100), ledger=ledger)
    ledger.observe(_cost("t-1", "turn-1", 60))
    assert gov.check_tokens("t-1") is None  # 60 <= 100
    ledger.observe(_cost("t-1", "turn-2", 40))
    assert gov.check_tokens("t-1") is None  # 100 == cap: NOT a breach
    ledger.observe(_cost("t-1", "turn-3", 1))
    breach = gov.check_tokens("t-1")  # 101 > cap
    assert breach is not None and breach.kind is BreachKind.TOKENS
    assert gov.is_paused("t-1") is True


def test_governor_double_breach_never_double_pauses() -> None:
    gov = Governor(Budget(max_msgs=1, max_tokens=10))
    assert gov.note_message("t-1") is None
    assert gov.note_message("t-1") is not None  # the breach
    assert gov.note_message("t-1") is None  # already paused → no second breach
    assert gov.breach_for("t-1") is not None


def test_governor_absent_token_feed_still_enforces_message_cap() -> None:
    # Degraded but never blind: no ledger wired → the message cap alone governs.
    gov = Governor(Budget(max_msgs=2, max_tokens=1), ledger=None)
    assert gov.note_message("t-1") is None
    assert gov.note_message("t-1") is None
    assert gov.note_message("t-1") is not None


def test_governor_gate_noops_bus_behavior_for_paused_task_only() -> None:
    gov = Governor(Budget(max_msgs=1, max_tokens=10))
    gov.note_message("t-1")
    gov.note_message("t-1")  # breach → paused
    paused_action = Action(kind=ActionKind.DISPATCH, session=_session("t-1"), target="bilby")
    gated = gov.gate(paused_action)
    assert gated.kind is ActionKind.NOOP
    assert "paused by governor (msgs)" in gated.reason
    assert gated.session is paused_action.session  # persistence unaffected

    other = Action(kind=ActionKind.DISPATCH, session=_session("t-2"), target="bilby")
    assert gov.gate(other) is other  # unpaused tasks flow through untouched


def test_governor_resume_is_explicit_and_resets_the_counter() -> None:
    gov = Governor(Budget(max_msgs=1, max_tokens=10))
    gov.note_message("t-1")
    gov.note_message("t-1")
    assert gov.is_paused("t-1") is True
    assert gov.resume("t-1") is True
    assert gov.is_paused("t-1") is False
    assert gov.resume("t-1") is False  # not paused → nothing to resume
    assert gov.note_message("t-1") is None  # counter reset — coordination flows


def test_governor_budget_from_env_default_and_failloud(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("FLEET_BUDGET_MSGS", raising=False)
    monkeypatch.delenv("FLEET_BUDGET_TOKENS", raising=False)
    budget = Budget.from_env()
    assert budget.max_msgs == 200
    assert budget.max_tokens == 150_000
    monkeypatch.setenv("FLEET_BUDGET_MSGS", "50")
    monkeypatch.setenv("FLEET_BUDGET_TOKENS", "9000")
    budget = Budget.from_env()
    assert (budget.max_msgs, budget.max_tokens) == (50, 9000)
    monkeypatch.setenv("FLEET_BUDGET_MSGS", "lots")
    with pytest.raises(ValueError, match="must be an integer"):
        Budget.from_env()
    monkeypatch.setenv("FLEET_BUDGET_MSGS", "0")
    with pytest.raises(ValueError, match="must be positive"):
        Budget.from_env()
    with pytest.raises(ValueError, match="must be positive"):
        Budget(max_msgs=0)


def test_governor_resume_is_idempotent_for_an_unpaused_task() -> None:
    # Nagatha requirement #1 (#59): a resume for a task that isn't paused is a
    # loud no-op returning False — never an error/crash (safe for a duplicate
    # control frame; app-level, transport is at-most-once, C5).
    gov = Governor(Budget(max_msgs=5, max_tokens=10))
    assert gov.is_paused("t-1") is False
    assert gov.resume("t-1") is False  # never paused
    assert gov.resume("t-1") is False  # still safe on repeat


def test_governor_resume_with_bounded_budget_add_raises_only_this_task() -> None:
    # Nagatha requirement #2 (#59): budget_msgs_add is a BOUNDED per-task add — it
    # raises this task's cap by exactly N and touches no other task/the shared cap.
    gov = Governor(Budget(max_msgs=2, max_tokens=10_000))
    gov.note_message("t-1")
    gov.note_message("t-1")
    gov.note_message("t-1")  # 3 > cap 2 → paused
    assert gov.is_paused("t-1") is True
    assert gov.resume("t-1", budget_msgs_add=3) is True  # cap now 2+3 = 5 for t-1
    assert gov.resume("t-1", budget_msgs_add=3) is False  # replay does not raise it again
    # t-1 now tolerates up to 5 before a re-breach; t-2 still uses the shared cap 2.
    for _ in range(5):
        assert gov.note_message("t-1") is None
    assert gov.note_message("t-1") is not None  # 6 > raised cap 5
    gov.note_message("t-2")
    gov.note_message("t-2")
    assert gov.note_message("t-2") is not None  # 3 > shared cap 2 (unaffected)


@pytest.mark.parametrize("budget_msgs_add", [0, -1])
def test_governor_resume_rejects_non_positive_budget_add(budget_msgs_add: int) -> None:
    gov = Governor(Budget(max_msgs=1, max_tokens=10))
    gov.note_message("t-1")
    gov.note_message("t-1")  # paused
    with pytest.raises(ValueError, match="must be positive"):
        gov.resume("t-1", budget_msgs_add=budget_msgs_add)
    assert gov.is_paused("t-1") is True


def test_governor_resume_budget_add_does_not_mutate_an_unpaused_task() -> None:
    gov = Governor(Budget(max_msgs=2, max_tokens=10_000))
    assert gov.resume("t-1", budget_msgs_add=10) is False  # not paused → False
    assert gov.note_message("t-1") is None
    assert gov.note_message("t-1") is None
    assert gov.note_message("t-1") is not None  # 3 > unchanged shared cap 2


def test_governor_manual_pause_gates_bus_behavior_and_is_idempotent() -> None:
    # The control-plane pause op (#59): gate a task with no budget breach.
    gov = Governor(Budget(max_msgs=100, max_tokens=10_000))
    assert gov.is_paused("t-1") is False
    assert gov.pause("t-1") is True
    assert gov.is_paused("t-1") is True
    assert gov.pause("t-1") is False  # already paused → no-op
    action = Action(kind=ActionKind.DISPATCH, session=_session("t-1"), target="bilby")
    assert gov.gate(action).kind is ActionKind.NOOP
    assert gov.resume("t-1") is True  # symmetric resume clears the manual pause
    assert gov.is_paused("t-1") is False


# ---- relay-loop reliability control (#96) ---------------------------------


def test_governor_relay_loop_trips_once_on_third_alternating_edge() -> None:
    gov = Governor(Budget(max_msgs=100, max_tokens=10_000))
    assert gov.note_relay("t-1", "e1", "skippy", "bilby", 0.0) is None
    assert gov.note_relay("t-1", "e2", "bilby", "skippy", 1.0) is None
    breach = gov.note_relay("t-1", "e3", "skippy", "bilby", 2.0)
    assert breach is not None
    assert breach.kind is BreachKind.RELAY_LOOP
    assert (breach.observed, breach.cap) == (3, 3)
    assert gov.is_paused("t-1") is True
    assert gov.note_relay("t-1", "e4", "bilby", "skippy", 3.0) is None


def test_governor_relay_loop_dedups_envelope_ids() -> None:
    gov = Governor(Budget(max_msgs=100, max_tokens=10_000))
    assert gov.note_relay("t-1", "e1", "skippy", "bilby", 0.0) is None
    assert gov.note_relay("t-1", "e2", "bilby", "skippy", 1.0) is None
    assert gov.note_relay("t-1", "e2", "bilby", "skippy", 1.0) is None
    assert gov.is_paused("t-1") is False
    assert gov.note_relay("t-1", "e3", "skippy", "bilby", 2.0) is not None


def test_governor_relay_loop_self_edges_never_advance_alternation() -> None:
    gov = Governor(Budget(max_msgs=100, max_tokens=10_000))
    for index in range(3):
        assert gov.note_relay("t-1", f"self-{index}", "skippy", "skippy", float(index)) is None
    assert gov.is_paused("t-1") is False

    assert gov.note_relay("t-1", "pair-1", "skippy", "bilby", 3.0) is None
    assert gov.note_relay("t-1", "pair-2", "bilby", "skippy", 4.0) is None
    breach = gov.note_relay("t-1", "pair-3", "skippy", "bilby", 5.0)
    assert breach is not None
    assert breach.kind is BreachKind.RELAY_LOOP


def test_governor_relay_loop_isolates_tasks_and_resets_on_pair_break() -> None:
    gov = Governor(Budget(max_msgs=100, max_tokens=10_000))
    assert gov.note_relay("t-1", "a1", "skippy", "bilby", 0.0) is None
    assert gov.note_relay("t-2", "b1", "skippy", "bilby", 0.0) is None
    assert gov.note_relay("t-1", "a2", "bilby", "clarisa", 1.0) is None
    assert gov.note_relay("t-2", "b2", "bilby", "skippy", 1.0) is None
    assert gov.note_relay("t-1", "a3", "clarisa", "bilby", 2.0) is None
    assert gov.note_relay("t-2", "b3", "skippy", "bilby", 2.0) is not None
    assert gov.is_paused("t-1") is False


def test_governor_relay_loop_resets_after_window() -> None:
    gov = Governor(Budget(max_msgs=100, max_tokens=10_000))
    assert gov.note_relay("t-1", "e1", "skippy", "bilby", 0.0) is None
    assert gov.note_relay("t-1", "e2", "bilby", "skippy", 1.0) is None
    assert gov.note_relay("t-1", "e3", "skippy", "bilby", 7.0) is None
    assert gov.note_relay("t-1", "e4", "bilby", "skippy", 8.0) is None
    assert gov.note_relay("t-1", "e5", "skippy", "bilby", 9.0) is not None


def test_governor_relay_loop_resume_and_completion_clear_state() -> None:
    gov = Governor(Budget(max_msgs=100, max_tokens=10_000))
    for envelope_id, sender, target, now in (
        ("e1", "skippy", "bilby", 0.0),
        ("e2", "bilby", "skippy", 1.0),
        ("e3", "skippy", "bilby", 2.0),
    ):
        gov.note_relay("t-1", envelope_id, sender, target, now)
    assert gov.resume("t-1") is True
    assert gov.note_relay("t-1", "e4", "bilby", "skippy", 3.0) is None
    assert gov.note_relay("t-1", "e5", "skippy", "bilby", 4.0) is None

    gov.complete_task("t-1")
    assert gov.is_paused("t-1") is False
    assert gov.note_relay("t-1", "e1", "skippy", "bilby", 5.0) is None
    assert gov.note_relay("t-1", "e2", "bilby", "skippy", 6.0) is None


def test_governor_unpaused_resume_preserves_relay_run() -> None:
    gov = Governor(Budget(max_msgs=100, max_tokens=10_000))
    assert gov.note_relay("t-1", "e1", "skippy", "bilby", 0.0) is None
    assert gov.note_relay("t-1", "e2", "bilby", "skippy", 1.0) is None

    assert gov.resume("t-1", budget_msgs_add=10) is False

    breach = gov.note_relay("t-1", "e3", "skippy", "bilby", 2.0)
    assert breach is not None
    assert breach.kind is BreachKind.RELAY_LOOP


# ---- Orchestrator wiring: the gate is a live control, not a seam -----------


class _Bus:
    """Duck-typed FleetBus double: records publishes, captures subscriptions."""

    def __init__(self) -> None:
        self.published: list[tuple[str, Envelope]] = []
        self.subscriptions: dict[str, Any] = {}

    @property
    def config(self) -> BusConfig:
        return BusConfig(agent_id="nagatha", env="dev")

    async def publish(self, subject: str, envelope: Envelope) -> None:
        self.published.append((subject, envelope))

    async def subscribe(self, subject: str, handler: Any) -> Any:
        self.subscriptions[subject] = handler
        return object()


class _Kboard:
    """Records the durable pause events the runbook's resume procedure reads."""

    def __init__(self) -> None:
        self.events: list[tuple[str, str, str, str]] = []

    async def post_event(self, task_id: str, event_type: str, *, actor: str, body: str) -> None:
        self.events.append((task_id, event_type, actor, body))


class _RecordingStore:
    """A GovernorStore double that records snapshots written by durable mode."""

    def __init__(self) -> None:
        self.persisted: list[dict[str, Any]] = []

    async def persist(self, task_id: str, snapshot: dict[str, Any]) -> None:
        self.persisted.append(dict(snapshot))

    async def load(self, task_id: str) -> dict[str, Any] | None:
        return self.persisted[-1] if self.persisted else None

    async def delete(self, task_id: str) -> bool:
        return False


def _wired(
    max_msgs: int, *, max_tokens: int = 10_000, ledger: CostLedger | None = None
) -> tuple[Orchestrator, _Bus, Governor, _Kboard]:
    bus = _Bus()
    kboard = _Kboard()
    ids = (f"g{i}" for i in itertools.count())
    clock = IdClock(new_id=lambda: next(ids), now=lambda: "2026-07-08T00:00:00Z")
    governor = Governor(Budget(max_msgs=max_msgs, max_tokens=max_tokens), ledger=ledger)
    narrator = Narrator(bus, env="dev", manager="nagatha", new_id=clock.new_id, now=clock.now)
    # _Bus/_Kboard duck-type the subsets the Orchestrator uses (test-only casts).
    orch = Orchestrator(
        bus,  # type: ignore[arg-type]
        clock,
        governor=governor,
        narrator=narrator,
        kboard=kboard,  # type: ignore[arg-type]
    )
    return orch, bus, governor, kboard


def _dispatched(task_id: str = "t-1") -> CoordinationSession:
    return CoordinationSession(
        task_id=task_id,
        channel="dash-dev",
        manager="nagatha",
        workers=("skippy", "bilby"),
        state=State.DISPATCHED,
    )


async def test_governor_wired_breach_gates_the_breaching_action_and_narrates() -> None:
    orch, bus, governor, _kb = _wired(max_msgs=1)
    first = await orch.apply(_dispatched(), Event(kind=EventKind.DISPATCH))
    assert first.kind is ActionKind.DISPATCH
    dispatches_before = len(bus.published)
    assert dispatches_before == 2  # fan-out to both workers

    # The second event trips the cap: its OWN action is gated to NOOP — the
    # only publish it produces is the paused narration beat, on the manager's
    # inbox, through the single throat.
    second = await orch.apply(_dispatched(), Event(kind=EventKind.DISPATCH))
    assert second.kind is ActionKind.NOOP
    assert "paused by governor (msgs)" in second.reason
    new = bus.published[dispatches_before:]
    assert [env.kind for _, env in new] == [NARRATION_KIND]
    assert new[0][0] == subjects.agent_inbox("dev", "nagatha")
    assert governor.is_paused("t-1") is True


async def test_governor_wired_wake_and_redispatch_honor_the_pause() -> None:
    orch, bus, governor, _kb = _wired(max_msgs=1)
    governor.note_message("t-1")
    governor.note_message("t-1")  # breach → paused
    session = _dispatched()
    before = len(bus.published)
    await orch.wake_worker(session, "skippy")
    await orch.redispatch(session, "skippy")
    assert bus.published[before:] == []  # a paused task performs NO bus behavior

    assert governor.resume("t-1") is True
    await orch.wake_worker(session, "skippy")
    assert [env.kind for _, env in bus.published[before:]] == ["wake"]


async def test_governor_wired_wake_and_redispatch_honor_the_fleet_kill() -> None:
    """P1-3: an ARMED fleet kill short-circuits the direct-invoke wake/redispatch
    paths too — the offline ladder publishes NOTHING to the bus under the kill.

    Without this the offline poll loop keeps firing ``agent_wake``/``agent_dispatch``
    while the fleet is supposed to be halted, escaping the gate that ``apply()``'s
    path honors ("gates EVERY dispatch/relay/wake").
    """
    orch, bus, governor, _kb = _wired(max_msgs=200)  # no task is over budget
    assert governor.is_paused("t-1") is False  # NOT a per-task pause — the fleet kill
    governor.pause_all()  # arm the out-of-band fleet kill
    session = _dispatched()
    before = len(bus.published)
    await orch.wake_worker(session, "skippy")
    await orch.redispatch(session, "skippy")
    assert bus.published[before:] == [], "an armed fleet kill must halt wake AND redispatch"

    # Releasing the fleet kill lets the offline wake flow again (orthogonal to budget).
    assert governor.resume_all() is True
    await orch.wake_worker(session, "skippy")
    assert [env.kind for _, env in bus.published[before:]] == ["wake"]


async def test_governor_wired_other_tasks_flow_while_one_is_paused() -> None:
    orch, bus, governor, _kb = _wired(max_msgs=1)
    governor.note_message("t-1")
    governor.note_message("t-1")  # t-1 paused
    action = await orch.apply(_dispatched("t-2"), Event(kind=EventKind.DISPATCH))
    assert action.kind is ActionKind.DISPATCH
    assert {env.task_id for _, env in bus.published} == {"t-2"}


def test_governor_gate_lets_a_paused_task_complete() -> None:
    # COMPLETE performs no bus behavior; a finished task must be allowed to
    # finish — never mislabeled as a budget NOOP (fable cross-review, #38).
    gov = Governor(Budget(max_msgs=1, max_tokens=10))
    gov.note_message("t-1")
    gov.note_message("t-1")  # breach → paused
    complete = Action(kind=ActionKind.COMPLETE, session=_session("t-1"))
    assert gov.gate(complete) is complete


async def test_governor_done_event_never_counts_against_the_budget() -> None:
    # Completion is the budget's GOAL: with the counter AT the cap, the done
    # sentinel still completes the task — no breach, no false "PAUSED" beat.
    orch, bus, governor, kboard = _wired(max_msgs=1)
    governor.note_message("t-1")  # counter == cap
    action = await orch.apply(_session(), Event(kind=EventKind.DONE, outcome="completed"))
    assert action.kind is ActionKind.COMPLETE
    assert action.session.state is State.DONE
    assert governor.is_paused("t-1") is False
    assert bus.published == []  # no narration, no bus behavior
    assert kboard.events == []


async def test_governor_token_cap_is_enforced_live_by_the_cost_watch() -> None:
    orch, bus, governor, kboard = _wired(max_msgs=1000, max_tokens=100, ledger=CostLedger())
    await orch.watch_token_costs()
    on_cost = bus.subscriptions[subjects.token_cost_wildcard("dev")]

    await on_cost(_cost("t-1", "turn-1", 100))  # == cap: not a breach
    assert governor.is_paused("t-1") is False
    await on_cost(_cost("t-1", "turn-2", 1))  # 101 > cap → live pause
    assert governor.is_paused("t-1") is True
    # Surfaced both ways: the narration beat AND the durable k-board record.
    narrations = [env for _, env in bus.published if env.kind == NARRATION_KIND]
    assert len(narrations) == 1
    assert "PAUSED over budget" in str(narrations[0].payload["text"])
    assert len(kboard.events) == 1
    assert kboard.events[0][0] == "t-1"
    assert "human resume required" in kboard.events[0][3]


async def test_governor_token_cost_watch_persists_spend_and_breach_state() -> None:
    bus = _Bus()
    kboard = _Kboard()
    store = _RecordingStore()
    ids = (f"g{i}" for i in itertools.count())
    clock = IdClock(new_id=lambda: next(ids), now=lambda: "2026-07-08T00:00:00Z")
    governor = Governor(
        Budget(max_msgs=1000, max_tokens=100),
        ledger=CostLedger(),
        store=store,
    )
    narrator = Narrator(bus, env="dev", manager="nagatha", new_id=clock.new_id, now=clock.now)
    orch = Orchestrator(
        bus,  # type: ignore[arg-type]
        clock,
        governor=governor,
        narrator=narrator,
        kboard=kboard,  # type: ignore[arg-type]
    )
    await orch.watch_token_costs()
    on_cost = bus.subscriptions[subjects.token_cost_wildcard("dev")]

    await on_cost(_cost("t-1", "turn-1", 100))
    assert store.persisted[-1]["tokens"]["tokens"] == 100
    assert store.persisted[-1]["paused"] is False

    await on_cost(_cost("t-1", "turn-2", 1))
    assert governor.is_paused("t-1") is True
    assert store.persisted[-1]["tokens"]["tokens"] == 101
    assert store.persisted[-1]["paused"] is True
    assert store.persisted[-1]["breach_kind"] == BreachKind.TOKENS.value
    narrations = [env for _, env in bus.published if env.kind == NARRATION_KIND]
    assert len(narrations) == 1
    assert len(kboard.events) == 1


async def test_governor_cost_watch_dedups_duplicate_turns() -> None:
    # An app-level duplicate of the same turn (transport is at-most-once, C5) must
    # never double-count spend into a phantom breach.
    orch, bus, governor, _kb = _wired(max_msgs=1000, max_tokens=100, ledger=CostLedger())
    await orch.watch_token_costs()
    on_cost = bus.subscriptions[subjects.token_cost_wildcard("dev")]
    await on_cost(_cost("t-1", "turn-1", 60))
    await on_cost(_cost("t-1", "turn-1", 60))  # duplicate — a logged no-op
    assert governor.is_paused("t-1") is False


async def test_governor_cost_watch_requires_a_ledger() -> None:
    orch, _bus, _gov, _kb = _wired(max_msgs=10)  # governor without a ledger
    with pytest.raises(ValueError, match="CostLedger"):
        await orch.watch_token_costs()


# ---- fleet-wide kill switch gate (#107) ------------------------------------


def test_governor_pause_all_gates_every_task_without_a_task_id() -> None:
    """pause_all NOOPs EVERY task's bus behavior — the 'stop everything' lever."""
    gov = Governor(Budget(max_msgs=200, max_tokens=150_000))
    # No task is over budget; nothing is per-task paused.
    assert gov.is_paused("t-1") is False
    assert gov.is_paused("t-2") is False

    assert gov.pause_all() is True  # armed
    assert gov.is_all_paused() is True

    for task_id in ("t-1", "t-2", "t-freshly-created"):
        action = Action(kind=ActionKind.DISPATCH, session=_session(task_id), target="bilby")
        gated = gov.gate(action)
        assert gated.kind is ActionKind.NOOP
        assert "fleet kill switch armed" in gated.reason
        assert gated.session is action.session  # persistence unaffected


@pytest.mark.parametrize(
    "kind",
    [ActionKind.DISPATCH, ActionKind.RELAY, ActionKind.WAKE, ActionKind.ARBITRATE],
)
def test_governor_kill_noops_every_non_terminal_kind(kind: ActionKind) -> None:
    """P2-8: the fleet kill gates EVERY non-terminal action kind to NOOP, not just
    DISPATCH — RELAY/WAKE/ARBITRATE are bus behaviors too and must all halt.

    (ARBITRATE is request/reply: NOOPing it means the responder produces no reply
    under an armed kill. That is the intended halt — a caller blocking on an
    arbitrate reply times out on its own request path, which is correct: the fleet
    is stopped, the decision is not made.)
    """
    gov = Governor(Budget(max_msgs=200, max_tokens=150_000))
    gov.pause_all()
    action = Action(kind=kind, session=_session("t-1"), target="bilby")
    gated = gov.gate(action)
    assert gated.kind is ActionKind.NOOP
    assert "fleet kill switch armed" in gated.reason


@pytest.mark.parametrize("kind", [ActionKind.COMPLETE, ActionKind.NOOP])
def test_governor_kill_passes_terminal_kinds_untouched(kind: ActionKind) -> None:
    """P2-8: COMPLETE/NOOP pass the fleet kill unchanged — a terminating task finishes."""
    gov = Governor(Budget(max_msgs=200, max_tokens=150_000))
    gov.pause_all()
    action = Action(kind=kind, session=_session("t-1"))
    assert gov.gate(action) is action


def test_governor_pause_all_is_idempotent() -> None:
    gov = Governor(Budget(max_msgs=1, max_tokens=10))
    assert gov.pause_all() is True
    assert gov.pause_all() is False  # already armed
    assert gov.resume_all() is True
    assert gov.resume_all() is False  # already released


def test_governor_resume_all_restores_prior_per_task_posture() -> None:
    """Releasing the fleet kill leaves a budget-paused task still paused (orthogonal)."""
    gov = Governor(Budget(max_msgs=1, max_tokens=10))
    gov.note_message("t-paused")
    gov.note_message("t-paused")  # breach → per-task paused
    assert gov.is_paused("t-paused") is True

    gov.pause_all()  # arm the fleet kill on top
    # Under the kill, EVEN a healthy task is gated.
    healthy = Action(kind=ActionKind.DISPATCH, session=_session("t-healthy"), target="bilby")
    assert gov.gate(healthy).kind is ActionKind.NOOP

    gov.resume_all()  # release the fleet kill
    # The healthy task flows again...
    assert gov.gate(healthy) is healthy
    # ...but the budget-paused task is STILL paused (kill was orthogonal).
    assert gov.is_paused("t-paused") is True
    paused = Action(kind=ActionKind.DISPATCH, session=_session("t-paused"), target="bilby")
    assert gov.gate(paused).kind is ActionKind.NOOP
    assert "paused by governor (msgs)" in gov.gate(paused).reason


def test_governor_kill_lets_terminal_actions_through() -> None:
    """A COMPLETE/NOOP is never gated by the fleet kill — a terminating task finishes."""
    gov = Governor(Budget(max_msgs=200, max_tokens=150_000))
    gov.pause_all()
    complete = Action(kind=ActionKind.COMPLETE, session=_session("t-1"))
    assert gov.gate(complete) is complete
    noop = Action(kind=ActionKind.NOOP, session=_session("t-1"))
    assert gov.gate(noop) is noop
