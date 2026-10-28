"""Exhaustive unit tests over the pure state machine — zero NATS, zero LLM.

Every (state × event) pair is exercised: the nominal forward path, the
missing-handoff → RELAY_NEEDED core, arbitration, the done sentinel from every
state, terminal idempotency, and that unhandled pairs degrade to a logged NOOP
rather than raising. The machine is provable here before Phase 2 wires it.
"""

from __future__ import annotations

import itertools

import pytest

from fleet_orchestrator import (
    Action,
    ActionKind,
    CoordinationSession,
    Event,
    EventKind,
    State,
    next_action,
)


def _session(state: State = State.DISPATCHED) -> CoordinationSession:
    return CoordinationSession(
        task_id="t-1",
        channel="dash-dev",
        manager="heron",
        workers=("wren", "finch"),
        state=state,
    )


# --- Nominal forward path -------------------------------------------------


def test_dispatch_moves_to_working_and_emits_dispatch() -> None:
    action = next_action(_session(State.DISPATCHED), Event(kind=EventKind.DISPATCH, actor="wren"))
    assert action.kind is ActionKind.DISPATCH
    assert action.session.state is State.WORKING
    assert action.target == "wren"


def test_worker_progress_stays_working_noop() -> None:
    action = next_action(_session(State.WORKING), Event(kind=EventKind.WORKER_PROGRESS))
    assert action.kind is ActionKind.NOOP
    assert action.session.state is State.WORKING


def test_worker_started_from_dispatched_advances_to_working() -> None:
    action = next_action(_session(State.DISPATCHED), Event(kind=EventKind.WORKER_STARTED))
    # NOOP kind (no bus behavior) but the session still advances — the returned
    # session is authoritative and callers persist it regardless of kind.
    assert action.kind is ActionKind.NOOP
    assert action.session.state is State.WORKING


# --- The human-replacement core: missing handoff -> RELAY_NEEDED ----------


def test_missing_handoff_triggers_relay() -> None:
    action = next_action(
        _session(State.WORKING),
        Event(
            kind=EventKind.HANDOFF_MISSING,
            actor="wren",
            relay_to="finch",
            context={"missed": "wren forgot to hand off to finch"},
        ),
    )
    assert action.kind is ActionKind.RELAY
    assert action.session.state is State.RELAY_NEEDED
    assert action.session.relay_target == "finch"
    assert action.session.relay_context == {"missed": "wren forgot to hand off to finch"}
    assert action.target == "finch"


def test_relay_sent_returns_to_working_and_clears_target() -> None:
    src = _session(State.RELAY_NEEDED).advanced_to(
        State.RELAY_NEEDED, relay_target="finch", relay_context={"missed": "x"}
    )
    action = next_action(src, Event(kind=EventKind.RELAY_SENT))
    assert action.session.state is State.WORKING
    assert action.session.relay_target is None
    assert action.session.relay_context == {}


def test_missing_handoff_without_target_is_noop_not_targetless_relay() -> None:
    # A relay to nobody is meaningless — must NOOP (logged), never emit RELAY(None).
    action = next_action(_session(State.WORKING), Event(kind=EventKind.HANDOFF_MISSING))
    assert action.kind is ActionKind.NOOP
    assert action.session.state is State.WORKING
    assert "nothing to relay" in action.reason


def test_relay_re_request_without_target_is_noop() -> None:
    # In RELAY_NEEDED with no session target and no event target → NOOP, not RELAY(None).
    src = _session(State.RELAY_NEEDED)
    action = next_action(src, Event(kind=EventKind.HANDOFF_MISSING))
    assert action.kind is ActionKind.NOOP
    assert "nothing to relay" in action.reason


def test_relay_re_requested_before_first_lands_keeps_target() -> None:
    src = _session(State.RELAY_NEEDED).advanced_to(
        State.RELAY_NEEDED, relay_target="finch", relay_context={"missed": "x"}
    )
    action = next_action(src, Event(kind=EventKind.HANDOFF_MISSING))
    assert action.kind is ActionKind.RELAY
    assert action.session.state is State.RELAY_NEEDED
    assert action.session.relay_target == "finch"
    assert action.session.relay_context == {"missed": "x"}


# --- Arbitration ----------------------------------------------------------


def test_arbitrate_requested_moves_to_arbitrating() -> None:
    action = next_action(
        _session(State.WORKING),
        Event(kind=EventKind.ARBITRATE_REQUESTED, actor="wren", topic="who-goes-first"),
    )
    assert action.kind is ActionKind.ARBITRATE
    assert action.session.state is State.ARBITRATING
    assert action.session.arbitrate_topic == "who-goes-first"


def test_arbitrate_request_without_topic_is_noop() -> None:
    # A topicless arbitrate request can't be decided — NOOP, not ARBITRATE(None).
    action = next_action(_session(State.WORKING), Event(kind=EventKind.ARBITRATE_REQUESTED))
    assert action.kind is ActionKind.NOOP
    assert action.session.state is State.WORKING
    assert "nothing to decide" in action.reason


def test_arbitrated_returns_to_working() -> None:
    src = _session(State.ARBITRATING).advanced_to(State.ARBITRATING, arbitrate_topic="who")
    action = next_action(src, Event(kind=EventKind.ARBITRATED))
    assert action.session.state is State.WORKING
    assert action.session.arbitrate_topic is None


# --- Done sentinel from every non-terminal state --------------------------


@pytest.mark.parametrize(
    "state",
    [State.DISPATCHED, State.WORKING, State.RELAY_NEEDED, State.ARBITRATING],
)
def test_done_from_any_state_completes(state: State) -> None:
    action = next_action(_session(state), Event(kind=EventKind.DONE, outcome="completed"))
    assert action.kind is ActionKind.COMPLETE
    assert action.session.state is State.DONE
    assert action.session.outcome == "completed"


def test_done_defaults_outcome() -> None:
    action = next_action(_session(State.WORKING), Event(kind=EventKind.DONE))
    assert action.session.outcome == "completed"


# --- Terminal idempotency -------------------------------------------------


@pytest.mark.parametrize("kind", list(EventKind))
def test_done_is_terminal_every_event_is_noop(kind: EventKind) -> None:
    action = next_action(_session(State.DONE), Event(kind=kind))
    assert action.kind is ActionKind.NOOP
    assert action.session.state is State.DONE


# --- Totality: every (state, event) pair resolves to exactly one Action ----


@pytest.mark.parametrize(
    ("state", "kind"),
    list(itertools.product(list(State), list(EventKind))),
)
def test_every_pair_is_total_and_returns_an_action(state: State, kind: EventKind) -> None:
    action = next_action(_session(state), Event(kind=kind))
    assert isinstance(action, Action)
    assert isinstance(action.kind, ActionKind)
    # The returned session is always a valid State (never None / partial).
    assert isinstance(action.session.state, State)


def test_unhandled_pair_degrades_to_noop_not_raise() -> None:
    # RELAY_SENT is meaningless in DISPATCHED — must NOOP, not raise.
    action = next_action(_session(State.DISPATCHED), Event(kind=EventKind.RELAY_SENT))
    assert action.kind is ActionKind.NOOP
    assert "no transition" in action.reason


def test_session_is_immutable() -> None:
    session = _session(State.WORKING)
    next_action(session, Event(kind=EventKind.DONE, outcome="completed"))
    # Original snapshot is untouched — transitions return new sessions.
    assert session.state is State.WORKING
    assert session.outcome is None


def test_relay_context_snapshot_is_detached_from_caller_dict() -> None:
    # Mutating the dict the caller passed into the event must NOT rewrite the
    # stored session snapshot — the model copies+freezes on the way in.
    caller_dict = {"missed": "original"}
    action = next_action(
        _session(State.WORKING),
        Event(kind=EventKind.HANDOFF_MISSING, relay_to="finch", context=caller_dict),
    )
    caller_dict["missed"] = "TAMPERED"
    caller_dict["injected"] = "new key"
    assert action.session.relay_context == {"missed": "original"}


def test_event_context_is_read_only() -> None:
    # The carried map is a read-only proxy — attempting to mutate it raises.
    event = Event(kind=EventKind.HANDOFF_MISSING, relay_to="finch", context={"k": "v"})
    with pytest.raises(TypeError):
        event.context["k"] = "mutated"  # type: ignore[index]


def test_relay_context_is_read_only() -> None:
    session = CoordinationSession(
        task_id="t",
        channel="c",
        manager="n",
        workers=("a", "b"),
        state=State.RELAY_NEEDED,
        relay_context={"k": "v"},
    )
    with pytest.raises(TypeError):
        session.relay_context["k"] = "mutated"  # type: ignore[index]
