"""The transition heart: ``next_action(state, event) -> Action``.

A pure case-statement — the deterministic replacement for the failed
human-in-the-loop DAP. Given the current :class:`~fleet_orchestrator.state.State`
and an inbound :class:`~fleet_orchestrator.state.Event`, it returns exactly one
:class:`Action`: what the orchestrator should do and which state to move to.

No LLM, no I/O, no clock, no random. Every branch is explicit; an unexpected
(state, event) pair returns a ``NOOP`` action carrying a reason rather than
raising, so a stray event never crashes the control loop — but it is logged.

Each transition logs *which state, which event, which transition* (a
reconstructable decision) per ``docs/logging.md`` — written in as the module is
built, not bolted on later.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum

from loguru import logger

from fleet_orchestrator.state import CoordinationSession, Event, EventKind, State


class ActionKind(StrEnum):
    """What the orchestrator should physically do for a transition.

    The bus-wiring layer (:mod:`fleet_orchestrator.behaviors`, Phase 2) maps each
    of these onto a concrete NATS behavior. Phase 1 only decides *which* action;
    it never performs one.

    Attributes:
        DISPATCH: Fan the task out to the target worker(s).
        RELAY: Relay missed context to ``relay_target`` (the human-replacement
            core).
        ARBITRATE: Return a deterministic decision on ``arbitrate_topic``.
        WAKE: Nudge a worker that has gone quiet.
        COMPLETE: The turn is done; persist and stop.
        NOOP: **No bus behavior** to perform for this event. The session may
            still have advanced (e.g. a worker-started ping moves DISPATCHED →
            WORKING with nothing to publish). ``kind`` describes the *bus
            action*, not whether state changed — callers therefore ALWAYS
            persist ``action.session``, regardless of kind, and only touch the
            bus for the non-NOOP kinds. Carries a ``reason``.
    """

    DISPATCH = "dispatch"
    RELAY = "relay"
    ARBITRATE = "arbitrate"
    WAKE = "wake"
    COMPLETE = "complete"
    NOOP = "noop"


@dataclass(frozen=True)
class Action:
    """The single decision returned by :func:`next_action`.

    Attributes:
        kind: Which :class:`ActionKind` to perform.
        session: The resulting session snapshot (already advanced to its next
            state). Callers persist / act on this.
        target: The ``agent_id`` this action is directed at, when applicable
            (dispatch/relay/wake target).
        reason: Human/log-readable reason, always set for ``NOOP`` and carried on
            every action for the transition log line.
        context: Optional carried payload (e.g. the relay text). Pure data.
    """

    kind: ActionKind
    session: CoordinationSession
    target: str | None = None
    reason: str = ""
    context: dict[str, str] = field(default_factory=dict)


def _noop(session: CoordinationSession, event: Event, reason: str) -> Action:
    """Build a logged NOOP for an event that does not apply in ``session.state``.

    A stray or re-delivered event must not crash the control loop, but it must
    never pass silently either — log it, then return an idempotent NOOP that
    leaves the session unchanged.
    """
    logger.debug(
        "orchestrator.transition | state={} event={} -> NOOP ({}) task={}",
        session.state.value,
        event.kind.value,
        reason,
        session.task_id,
    )
    return Action(kind=ActionKind.NOOP, session=session, reason=reason)


def next_action(session: CoordinationSession, event: Event) -> Action:
    """Decide the single next action for ``session`` given ``event``.

    Pure and total: every (state, event) pair resolves to exactly one
    :class:`Action`. Unhandled pairs fall through to a logged ``NOOP`` — the
    control loop stays alive on unexpected input.

    Args:
        session: The current coordination session (immutable snapshot).
        event: The inbound event to apply.

    Returns:
        The :class:`Action` to perform, carrying the already-advanced session.
        ``Action.session`` is always authoritative — persist it for *every*
        action, including ``NOOP`` (a NOOP means "no bus behavior", not "no
        state change"; see :class:`ActionKind`). Only the non-NOOP kinds imply a
        bus behavior to run.
    """
    state = session.state
    kind = event.kind

    # Terminal: once DONE, nothing re-opens the session. Every event is a NOOP.
    if state is State.DONE:
        return _noop(session, event, "session already DONE")

    match (state, kind):
        # --- DISPATCHED: the task has been handed to the workers -------------
        case (State.DISPATCHED, EventKind.DISPATCH):
            # (Re)dispatch is idempotent AT THE STATE-MACHINE LEVEL only: re-applying
            # DISPATCH when already DISPATCHED lands in the same WORKING state. The
            # WIRE fan-out it triggers is NOT idempotent — each dispatch mints a
            # fresh Envelope.id and the worker runs again by design (C5; see
            # behaviors.redispatch). The pure core only decides "DISPATCH, move to
            # WORKING"; the actual fan-out to session.workers is Phase 2's bus
            # behavior. target carries an optional single-worker hint from the
            # event (None = fan out to all session.workers, resolved downstream).
            nxt = session.advanced_to(State.WORKING)
            # Carry the event's context (the task prompt {text,task_id,channel})
            # onto the action so the bus layer's _envelope stamps it as the dispatch
            # body — without this the fan-out reaches the worker EMPTY and the
            # subscriber drops the turn (fleet-bus #52). Pure copy: the machine
            # never reads the prose, it only forwards it to the behavior layer.
            return _emit(
                ActionKind.DISPATCH,
                nxt,
                event,
                target=event.actor,
                reason="dispatched to workers",
                context=dict(event.context),
            )
        case (State.DISPATCHED, EventKind.WORKER_STARTED):
            nxt = session.advanced_to(State.WORKING)
            return _emit(ActionKind.NOOP, nxt, event, reason="worker started")

        # --- WORKING: at least one worker is active on the task -------------
        case (State.WORKING, EventKind.WORKER_STARTED | EventKind.WORKER_PROGRESS):
            # Still working; nothing to do but stay observable.
            return _emit(ActionKind.NOOP, session, event, reason="worker progress")
        case (State.WORKING, EventKind.HANDOFF_MISSING):
            # The human-replacement core: a worker finished without handing off.
            # A relay to nobody is meaningless — a HANDOFF_MISSING with no peer to
            # relay to is malformed; NOOP it (logged) rather than emit a targetless
            # RELAY that would blow up in subjects.agent_relay(env, None) downstream.
            if not event.relay_to:
                return _noop(session, event, "handoff-missing without relay_to; nothing to relay")
            nxt = session.advanced_to(
                State.RELAY_NEEDED,
                relay_target=event.relay_to,
                relay_context=event.context,
            )
            return _emit(
                ActionKind.RELAY,
                nxt,
                event,
                target=event.relay_to,
                reason="worker omitted handoff; relay needed",
            )
        case (State.WORKING, EventKind.ARBITRATE_REQUESTED):
            # Symmetric with the relay guard: an arbitrate request with no topic
            # can't be decided or keyed onto subjects.agent_arbitrate(env, None)
            # downstream — NOOP it (logged) rather than enter ARBITRATING with a
            # null topic the manager can't act on.
            if not event.topic:
                return _noop(session, event, "arbitrate request without a topic; nothing to decide")
            nxt = session.advanced_to(State.ARBITRATING, arbitrate_topic=event.topic)
            return _emit(
                ActionKind.ARBITRATE, nxt, event, target=event.actor, reason="arbitration requested"
            )
        case (State.WORKING, EventKind.DONE):
            nxt = session.advanced_to(State.DONE, outcome=event.outcome or "completed")
            return _emit(ActionKind.COMPLETE, nxt, event, reason="done signal received")

        # --- RELAY_NEEDED: orchestrator must relay the missed context -------
        case (State.RELAY_NEEDED, EventKind.RELAY_SENT):
            # Relay delivered; the workers are working again.
            nxt = session.advanced_to(State.WORKING)
            return _emit(ActionKind.NOOP, nxt, event, reason="relay delivered; back to working")
        case (State.RELAY_NEEDED, EventKind.HANDOFF_MISSING):
            # Re-notified before the first relay landed; re-emit to the same peer.
            # Fall back to the target already on the session; if neither the event
            # nor the session names a peer there is nothing to relay to — NOOP.
            target = event.relay_to or session.relay_target
            if not target:
                return _noop(session, event, "relay re-request without a target; nothing to relay")
            nxt = session.advanced_to(
                State.RELAY_NEEDED,
                relay_target=target,
                relay_context=event.context or session.relay_context,
            )
            return _emit(
                ActionKind.RELAY,
                nxt,
                event,
                target=target,
                reason="relay re-requested",
            )

        # --- ARBITRATING: manager owes a deterministic decision ------------
        case (State.ARBITRATING, EventKind.ARBITRATED):
            # Decision returned; coordination resumes.
            nxt = session.advanced_to(State.WORKING)
            return _emit(ActionKind.NOOP, nxt, event, reason="arbitrated; back to working")

        # --- DONE from any non-terminal state (a late/direct done sentinel) -
        case (_, EventKind.DONE):
            nxt = session.advanced_to(State.DONE, outcome=event.outcome or "completed")
            return _emit(ActionKind.COMPLETE, nxt, event, reason=f"done signal in {state.value}")

    return _noop(session, event, f"no transition for ({state.value}, {kind.value})")


def _emit(
    kind: ActionKind,
    session: CoordinationSession,
    event: Event,
    *,
    target: str | None = None,
    reason: str,
    context: dict[str, str] | None = None,
) -> Action:
    """Build and log a firing transition (which state, which event, which action).

    The log line is the reconstructable decision record required by
    ``docs/logging.md``: state, event, resulting action, task id. No secrets,
    member ids, or webhook urls ever enter it — only coordination metadata.
    """
    logger.info(
        "orchestrator.transition | -> state={} event={} action={} target={} task={} ({})",
        session.state.value,
        event.kind.value,
        kind.value,
        target,
        session.task_id,
        reason,
    )
    return Action(
        kind=kind,
        session=session,
        target=target,
        reason=reason,
        context=context if context is not None else {},
    )
