"""The completion-sentinel wire shape — the one shared ``kind="done"`` contract.

The done signal is an :class:`~fleet_nats.envelope.Envelope` with ``kind="done"``
carrying the typed :class:`~fleet_core.spec.Done` in the ``data`` slot of the
shared :class:`~fleet_core.contract.Payload` four-slot body
(``{"header": {"actor"}, "data": {"task_id", "outcome"}, "extras": {},
"footer": {}}``), published on the task's status subject. Both the *emitter* (the
per-agent subscriber) and the *consumer* (the orchestrator) must agree on this
shape exactly — so it lives here, in the shared transport layer both already
import, with a single builder. Neither side hand-rolls the dict; that is how the
shape silently drifts. Coordination still rides ``typed_signal`` on the envelope,
unaffected by the body wrapping.

Reading a received sentinel into a coordination *event* is orchestrator
semantics, so that lives in the orchestrator — this module owns only the wire
shape and its construction/recognition.
"""

from __future__ import annotations

from fleet_core import FooterBase, HeaderBase, Payload
from fleet_core.spec import Done

from fleet_nats.envelope import Envelope

# The done payload body wrapped in the shared four-slot contract. The ``data``
# slot IS the typed ``Done`` model; done needs no header/footer fields beyond the
# bases, so the concrete type parametrizes over the base classes directly.
DonePayload = Payload[HeaderBase, Done, FooterBase]

# The sentinel's kind + payload keys — the shared contract. One definition.
DONE_KIND = "done"
TASK_ID_KEY = "task_id"
OUTCOME_KEY = "outcome"

# The terminal outcome when a turn finishes without naming one.
DEFAULT_OUTCOME = "completed"


def done_envelope(
    *,
    msg_id: str,
    ts: str,
    sender: str,
    task_id: str,
    outcome: str = DEFAULT_OUTCOME,
    channel: str | None = None,
) -> Envelope:
    """Build the completion-sentinel envelope a finished turn emits.

    Publish on ``subjects.agent_status(env, task_id)`` — the task's status lane,
    the subject the orchestrator watches. Caller supplies id/ts (mirrors
    :meth:`Envelope.new`) so it stays deterministic under test.

    Args:
        msg_id: Unique message id.
        ts: ISO-8601 UTC timestamp.
        sender: The finishing agent's id.
        task_id: The task this turn completed.
        outcome: Terminal outcome (e.g. ``completed`` / ``failed``).
        channel: Optional originating channel.

    Returns:
        The ``kind="done"`` :class:`Envelope`.
    """
    done = Done(task_id=task_id, outcome=outcome)
    payload_model = DonePayload(
        header=HeaderBase(actor=sender),
        data=done,
        footer=FooterBase(),
    )
    return Envelope.new(
        msg_id=msg_id,
        ts=ts,
        sender=sender,
        kind=DONE_KIND,
        task_id=task_id,
        channel=channel,
        payload=payload_model.model_dump(mode="json", exclude_none=True),
        typed_signal=done,
    )


def is_done_envelope(envelope: Envelope) -> bool:
    """Whether ``envelope`` is a completion sentinel (``kind="done"``)."""
    return envelope.kind == DONE_KIND
