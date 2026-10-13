"""Consume the completion sentinel — parse a ``kind="done"`` envelope into an Event.

The sentinel *wire shape* (``kind="done"``, ``payload={"task_id", "outcome"}``,
the :func:`~fleet_nats.done_envelope` builder) is defined once in the shared
transport layer (:mod:`fleet_nats.sentinel`), so the emitter (subscriber) and
consumer (this package) cannot drift. This module owns only the *orchestrator
semantics*: turning a received sentinel into a coordination :class:`Event` that
advances the state machine to ``DONE`` (issue #6).

``done_envelope`` / ``is_done_envelope`` / ``DONE_KIND`` / ``DEFAULT_OUTCOME`` are
re-exported here for callers that build sentinels alongside consuming them.
"""

from __future__ import annotations

from fleet_nats import DEFAULT_OUTCOME, DONE_KIND, Done, Envelope, done_envelope, is_done_envelope
from fleet_nats.sentinel import OUTCOME_KEY, TASK_ID_KEY

from fleet_orchestrator.state import Event, EventKind

__all__ = [
    "DEFAULT_OUTCOME",
    "DONE_KIND",
    "done_envelope",
    "done_payload_field",
    "is_done_envelope",
    "parse_done",
]


def done_payload_field(payload: dict[str, object], key: str) -> str | None:
    """Read a string done body field, tolerating wrapped and flat wire shapes.

    Current sentinels wrap the ``Done`` body in the shared four-slot
    :class:`~fleet_core.contract.Payload`, so ``task_id`` / ``outcome`` live under
    ``payload["data"]``. A mid-deploy old-format sentinel still carries them flat
    at the top level. Prefer the nested slot; fall back to the flat key so both
    parse. Returns ``None`` when the field is absent or empty.
    """
    data = payload.get("data")
    raw: object | None
    if isinstance(data, dict) and key in data:
        raw = data[key]
    else:
        raw = payload.get(key)
    if raw is None:
        return None
    text = str(raw)
    return text or None


def parse_done(envelope: Envelope) -> Event | None:
    """Parse a received sentinel into a ``DONE`` :class:`Event`, or ``None``.

    Returns ``None`` when the envelope is not ``kind="done"`` (the caller ignores
    it) or when a ``done`` sentinel names no task — a done-signal that identifies
    no task cannot advance any session, so it is rejected rather than advancing
    the wrong one.

    Args:
        envelope: A received envelope off the status lane.

    Returns:
        A ``DONE`` event carrying the outcome, or ``None`` to ignore.
    """
    if not is_done_envelope(envelope):
        return None
    typed_signal = envelope.typed_signal
    typed_done = typed_signal if isinstance(typed_signal, Done) else None
    # Prefer the envelope task_id, then the typed Done signal, then the payload
    # copy for hand-built legacy sentinels. A sentinel that names no task cannot
    # advance a session.
    task_id = envelope.task_id or (typed_done.task_id if typed_done is not None else None)
    task_id = task_id or done_payload_field(envelope.payload, TASK_ID_KEY)
    if not task_id:
        return None
    outcome = (
        typed_done.outcome
        if typed_done is not None
        else done_payload_field(envelope.payload, OUTCOME_KEY) or DEFAULT_OUTCOME
    )
    return Event(kind=EventKind.DONE, actor=envelope.sender, outcome=str(outcome))
