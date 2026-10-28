"""The coordination state model — pure data, no I/O, no clock, no random.

A :class:`CoordinationSession` is one task worked by the trio. It moves through
an explicit :class:`State` machine driven by :class:`Event`\\ s. This module is
the provable heart of the DAP: it holds *only* data and enums so the transition
function (:mod:`fleet_orchestrator.transitions`) can be exhaustively unit-tested
in isolation, with zero NATS and zero LLM.

Callers supply ids and timestamps (mirroring ``Envelope.new``) — nothing here
touches ``time`` or ``random`` at import, so sessions are deterministic under
test.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field, replace
from enum import StrEnum
from types import MappingProxyType


def _frozen_map(value: Mapping[str, str]) -> Mapping[str, str]:
    """Return a read-only, caller-independent copy of a string map.

    Frozen dataclasses only block *rebinding* a field, not mutation of a mutable
    value bound to it. A plain ``dict`` field therefore leaks two ways: a caller
    can mutate the dict it passed in (and thus the "immutable" snapshot), and a
    stored-by-reference dict can be rewritten after the fact. Copy into a
    ``MappingProxyType`` so the stored map is both detached from the caller's
    object and read-only.
    """
    return MappingProxyType(dict(value))


class State(StrEnum):
    """The five coordination states, in nominal forward order.

    ``DISPATCHED → WORKING → RELAY_NEEDED → ARBITRATING → DONE``. The
    ``RELAY_NEEDED`` state is the human-replacement core: a worker finished a
    turn without handing off to the peer it needed, so the orchestrator must
    relay the missed context before work can continue.
    """

    DISPATCHED = "dispatched"
    WORKING = "working"
    RELAY_NEEDED = "relay_needed"
    ARBITRATING = "arbitrating"
    DONE = "done"


class EventKind(StrEnum):
    """The inbound events that drive transitions.

    These map onto envelope kinds / bus signals but are declared here as pure
    enums so the transition function never imports the transport.

    Attributes:
        DISPATCH: A task was dispatched to the worker(s).
        WORKER_STARTED: A worker acknowledged / began working.
        WORKER_PROGRESS: A worker emitted intermediate status (still working).
        HANDOFF_MISSING: A worker finished its turn but omitted the handoff to
            the peer it needed — the trigger into ``RELAY_NEEDED``.
        RELAY_SENT: The orchestrator relayed the missed context to the peer.
        ARBITRATE_REQUESTED: A worker asked the manager to decide (who-goes-first
            / conflict) — the trigger into ``ARBITRATING``.
        ARBITRATED: The manager returned a deterministic decision.
        DONE: An unambiguous completion signal (``kind="done"`` envelope).
    """

    DISPATCH = "dispatch"
    WORKER_STARTED = "worker_started"
    WORKER_PROGRESS = "worker_progress"
    HANDOFF_MISSING = "handoff_missing"
    RELAY_SENT = "relay_sent"
    ARBITRATE_REQUESTED = "arbitrate_requested"
    ARBITRATED = "arbitrated"
    DONE = "done"


@dataclass(frozen=True)
class Event:
    """A single inbound event applied to a session.

    Attributes:
        kind: Which :class:`EventKind` occurred.
        actor: The ``agent_id`` this event is about (worker that finished, worker
            that asked to arbitrate, ...). Optional for task-level events.
        relay_to: For ``HANDOFF_MISSING``, the peer the missed context must be
            relayed to (the worker that was *not* mentioned).
        topic: For ``ARBITRATE_REQUESTED``, the free-form decision topic.
        outcome: For ``DONE``, the terminal outcome string (e.g. ``completed``).
        context: Optional carried payload (e.g. the missed handoff text to relay);
            pure data, never interpreted here.
    """

    kind: EventKind
    actor: str | None = None
    relay_to: str | None = None
    topic: str | None = None
    outcome: str | None = None
    context: Mapping[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        # Detach + freeze the carried map so the "immutable event" contract holds
        # even against a caller that keeps and mutates the dict it passed in.
        object.__setattr__(self, "context", _frozen_map(self.context))


@dataclass(frozen=True)
class CoordinationSession:
    """One task worked by the trio, plus the coordination state around it.

    Frozen: transitions return a *new* session via :meth:`advanced_to` rather
    than mutating in place, so a session's history is a value sequence and tests
    can assert on immutable snapshots.

    Attributes:
        task_id: The k-board task id this session coordinates.
        channel: Originating Discord channel slug (per-channel isolation).
        manager: The manager ``agent_id`` (e.g. ``heron``).
        workers: The worker ``agent_id``\\ s on this task (e.g. wren, finch).
        state: Current :class:`State`.
        relay_target: When in ``RELAY_NEEDED``, the peer awaiting the relay.
        relay_context: The missed-handoff payload to relay to ``relay_target``.
        arbitrate_topic: When in ``ARBITRATING``, the topic under decision.
        outcome: Terminal outcome, set on reaching ``DONE``.
    """

    task_id: str
    channel: str
    manager: str
    workers: tuple[str, ...]
    state: State = State.DISPATCHED
    relay_target: str | None = None
    relay_context: Mapping[str, str] = field(default_factory=dict)
    arbitrate_topic: str | None = None
    outcome: str | None = None

    def __post_init__(self) -> None:
        # Same detach+freeze as Event.context: the stored relay map must not alias
        # the caller's (or the event's) dict, so a later mutation there can't
        # rewrite this snapshot. replace() re-runs __post_init__, so advanced_to()
        # inherits this protection for free.
        object.__setattr__(self, "relay_context", _frozen_map(self.relay_context))

    def advanced_to(
        self,
        state: State,
        *,
        relay_target: str | None = None,
        relay_context: Mapping[str, str] | None = None,
        arbitrate_topic: str | None = None,
        outcome: str | None = None,
    ) -> CoordinationSession:
        """Return a new session in ``state`` with the given coordination fields.

        Fields not passed are reset to their neutral default so a stale
        ``relay_target`` (say) never lingers past the state it belonged to — the
        session snapshot always reflects exactly the state it is in.
        """
        return replace(
            self,
            state=state,
            relay_target=relay_target,
            relay_context=relay_context if relay_context is not None else {},
            arbitrate_topic=arbitrate_topic,
            outcome=outcome,
        )
