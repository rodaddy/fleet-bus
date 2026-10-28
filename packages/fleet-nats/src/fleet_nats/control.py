"""The control-plane frame — the shared ``kind="control"`` op contract (#59).

The manager run-loop (the hub) owns a task's live governor state in-process, so
a paused (over-budget) task is only resumable by that live instance. There is no
external lever otherwise — a human steer saying "resume, and give it more budget"
has nowhere to land. This module defines the tiny out-of-band frame that reaches
it: a :class:`~fleet_nats.envelope.Envelope` with ``kind="control"`` carrying
``payload={"op", "task_id"}`` (+ an optional BOUNDED per-task ``budget_msgs_add``
bump, + an optional audited ``reason``), published on
``subjects.hub_control(env, task_id)``.

The frame also carries an optional audited ``reason`` (the WHY of a steer; the
WHO is ``envelope.sender``). It is length-capped and included only when present,
so a frame without it is byte-identical to a pre-``reason`` frame — existing
publishers and the hub consumer are unaffected (#65 review). Making the audit
record ride the frame is the point: WHO + WHY must be hub-inspectable, not only
in the publishing tool's local log.

Both the *publisher* (a harness / a Heron-side steer tool) and the *consumer*
(the hub) must agree on this shape exactly — so it lives here, in the shared
transport layer both import, with a single builder and a single parser. Neither
side hand-rolls the dict; that is how the shape silently drifts (same discipline
as :mod:`fleet_nats.sentinel`).

The budget bump is ADD-semantics on purpose (Heron ratified, #59): a frame
raises THIS task's cap by exactly N — never a global/unbounded set — so a steer's
grant is bounded and per-task by construction.

Ops are a CLOSED set. An unknown op is not improvised at build time (it raises)
and is a loud no-op at consume time — the hub must never crash on, or silently
swallow, a control frame.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

from loguru import logger

import fleet_nats.subjects as subjects
from fleet_nats.envelope import Envelope
from fleet_nats.tracing import traced

# ``import fleet_nats.subjects`` binds the SUBMODULE directly (subjects has no
# fleet_nats-package imports of its own), so it cannot re-enter a half-built
# fleet_nats/__init__ mid-import — no cycle even though __init__ imports control.

# The control frame's kind + payload keys — the shared contract. One definition.
CONTROL_KIND = "control"
OP_KEY = "op"
TASK_ID_KEY = "task_id"
# ADD-semantics: raise THIS task's cap by N (bounded, per-task); never a set.
BUDGET_MSGS_ADD_KEY = "budget_msgs_add"
# The audited WHY of a steer (who = envelope.sender; why = this). Optional and
# length-capped: the audit record of a control op must be hub-inspectable, not
# only visible in the publisher's local log.
REASON_KEY = "reason"
# Cap the reason so a control frame can never carry an unbounded blob — the field
# is an audit note, not a payload channel. A longer reason is truncated (audit
# still lands), never rejected (a fat note must not sink the resume).
MAX_REASON_LEN = 500

# The closed set of ops the control plane understands.
OP_RESUME = "resume"
OP_PAUSE = "pause"
_KNOWN_OPS = frozenset({OP_RESUME, OP_PAUSE})


@dataclass(frozen=True)
class ControlOp:
    """A parsed, recognised control op — the hub acts on this, never raw payload.

    Attributes:
        op: The op word (``resume`` / ``pause``) — always in the known set.
        task_id: The task the op targets.
        budget_msgs_add: Optional BOUNDED per-task message-cap increase to apply
            on a ``resume`` (add exactly this many to THIS task's cap); ``None``
            when no bump was asked for or the bump was malformed.
        reason: Optional audited WHY of the steer (the operator's note). ``None``
            when the frame carried no reason — full backward compat with frames
            (and callers) that predate the field.
    """

    op: str
    task_id: str
    budget_msgs_add: int | None = None
    reason: str | None = None


@traced
def control_envelope(
    *,
    msg_id: str,
    ts: str,
    sender: str,
    task_id: str,
    op: str = OP_RESUME,
    budget_msgs_add: int | None = None,
    reason: str | None = None,
    channel: str | None = None,
) -> Envelope:
    """Build a control-plane frame a harness/steer tool publishes at the hub.

    Publish on ``subjects.hub_control(env, task_id)`` — the task's control lane,
    the wildcard the hub watches. Caller supplies id/ts (mirrors
    :meth:`Envelope.new` and :func:`~fleet_nats.sentinel.done_envelope`) so it
    stays deterministic under test.

    Args:
        msg_id: Unique message id.
        ts: ISO-8601 UTC timestamp.
        sender: The operator/tool identity issuing the op (recorded in hub logs).
        task_id: The task the op targets.
        op: The op word — one of ``resume`` / ``pause``.
        budget_msgs_add: Optional BOUNDED per-task cap increase to request on a
            ``resume`` (add N messages to THIS task's cap). Must be a positive int
            when given; a non-positive add is a caller error.
        reason: Optional audited WHY of the steer. Included in the payload only
            when a non-empty note is given, truncated to :data:`MAX_REASON_LEN`
            (an audit note, not a payload channel). Omitting it keeps the frame
            byte-identical to a pre-``reason`` frame — existing hub/harness
            callers and consumers are unaffected.
        channel: Optional originating channel (for narration routing/parity).

    Returns:
        The ``kind="control"`` :class:`Envelope`.

    Raises:
        ValueError: On an unknown ``op`` (the op set is closed — an unknown op is
            a programming error at the publisher, not content to improvise) or a
            non-positive ``budget_msgs_add``.
    """
    if op not in _KNOWN_OPS:
        raise ValueError(f"control_envelope: unknown op {op!r}; known: {sorted(_KNOWN_OPS)}")
    if budget_msgs_add is not None and budget_msgs_add <= 0:
        raise ValueError(
            f"control_envelope: budget_msgs_add must be positive, got {budget_msgs_add}"
        )
    payload: dict[str, object] = {OP_KEY: op, TASK_ID_KEY: task_id}
    if budget_msgs_add is not None:
        payload[BUDGET_MSGS_ADD_KEY] = budget_msgs_add
    # Include reason only when a non-empty note is present (a blank/whitespace
    # reason adds no audit value and would only bloat the frame). Truncate to the
    # cap so the field can never carry an unbounded blob.
    if reason is not None:
        trimmed = reason.strip()
        if trimmed:
            payload[REASON_KEY] = trimmed[:MAX_REASON_LEN]
    return Envelope.new(
        msg_id=msg_id,
        ts=ts,
        sender=sender,
        kind=CONTROL_KIND,
        task_id=task_id,
        channel=channel,
        payload=payload,
    )


def is_control_envelope(envelope: Envelope) -> bool:
    """Whether ``envelope`` is a control frame (``kind="control"``)."""
    return envelope.kind == CONTROL_KIND


@traced
def parse_control(envelope: Envelope) -> ControlOp | None:
    """Read a received control frame into a recognised :class:`ControlOp`.

    The consumer-side counterpart to :func:`control_envelope`: it validates the
    frame is a known, well-formed control op and never raises on a malformed one
    — a bad frame must be a loud DROP at the hub, not a crash. The hub logs the
    ``None`` cases with the sender; this parser stays silent (no logger) so the
    wire layer holds no policy.

    Args:
        envelope: The received envelope (already decoded by the bus).

    Returns:
        The parsed :class:`ControlOp` when the frame is a known op with a
        task_id; ``None`` when it is not a control frame, carries no/blank op,
        has no task_id, or names an op outside the closed set. A malformed
        ``budget_msgs_add`` is dropped (set to ``None``) rather than failing the
        whole op — the resume still unblocks the lane, the bogus bump is ignored.
        A missing/blank/non-string ``reason`` surfaces as ``None`` (full backward
        compat with frames that predate the field).
    """
    if not is_control_envelope(envelope):
        return None
    payload = envelope.payload
    op = payload.get(OP_KEY)
    if not isinstance(op, str) or op not in _KNOWN_OPS:
        return None
    # Prefer the payload task_id; fall back to the envelope field (the publisher
    # sets both, but the subject already scoped delivery to one task's lane).
    raw_task = payload.get(TASK_ID_KEY) or envelope.task_id
    if not isinstance(raw_task, str) or not raw_task:
        return None
    budget_msgs_add = _coerce_budget(payload.get(BUDGET_MSGS_ADD_KEY))
    reason = _coerce_reason(payload.get(REASON_KEY))
    return ControlOp(op=op, task_id=raw_task, budget_msgs_add=budget_msgs_add, reason=reason)


class _PublishBus(Protocol):
    """The one bus capability the control publisher needs."""

    async def publish(self, subject: str, envelope: Envelope) -> None:
        """Publish an envelope."""
        ...


@traced
async def publish_control(
    bus: _PublishBus,
    *,
    env: str,
    task_id: str,
    sender: str,
    op: str = OP_RESUME,
    budget_msgs_add: int | None = None,
    reason: str | None = None,
    msg_id: str,
    ts: str,
    channel: str | None = None,
) -> Envelope:
    """Build and publish a control frame on the hub's control lane (#59).

    The one call a harness or a Heron-side steer tool makes to unpause a lane:
    it builds the frame via :func:`control_envelope` and publishes it on
    ``subjects.hub_control(env, task_id)`` — the lane the hub watches. Mirrors
    :func:`~fleet_nats.sentinel.done_envelope`'s caller-supplies-id/ts contract so
    the publish stays deterministic under test.

    Args:
        bus: The connected bus (publish capability only).
        env: Subject env prefix.
        task_id: The task the op targets.
        sender: The operator/tool identity (recorded in the hub's log).
        op: One of ``resume`` / ``pause``.
        budget_msgs_add: Optional bounded per-task cap increase on a ``resume``.
        reason: Optional audited WHY of the steer — rides the payload so the
            audit (who=sender, why=reason) is hub-inspectable, not only in the
            publisher's local log. Omitting it keeps the frame byte-identical to
            a pre-``reason`` frame.
        msg_id: Unique message id.
        ts: ISO-8601 UTC timestamp.
        channel: Optional originating channel.

    Returns:
        The published :class:`Envelope` (for the caller to log/inspect).

    Raises:
        ValueError: On an unknown ``op`` or non-positive ``budget_msgs_add``
            (raised by :func:`control_envelope` before anything is published).
    """
    # Sanitized boundary logging (scout p4-logging-langfuse-scout.md §1): this was
    # the ONE control-plane edge with no entry/exit/failure line and no downstream
    # coverage. Log the COORDINATION shape only — subject + op + task_id — never the
    # payload (reason prose, sender identity, budget bump ride in the frame and are
    # not echoed here; docs/logging.md never-log list).
    subject = subjects.hub_control(env, task_id)
    logger.debug("control.publish | subject={} op={} task_id={}", subject, op, task_id)
    envelope = control_envelope(
        msg_id=msg_id,
        ts=ts,
        sender=sender,
        task_id=task_id,
        op=op,
        budget_msgs_add=budget_msgs_add,
        reason=reason,
        channel=channel,
    )
    try:
        await bus.publish(subject, envelope)
    except Exception as exc:
        logger.error("control.publish | subject={} op={} failed: {}", subject, op, exc)
        raise
    logger.debug("control.publish | subject={} op={} published", subject, op)
    return envelope


def _coerce_budget(raw: object) -> int | None:
    """Coerce a payload ``budget_msgs_add`` to a positive int, else ``None``.

    A missing/None/malformed/non-positive bump is dropped (not fatal): the op is
    a resume first, a budget bump second — a junk bump must never sink the resume
    that unblocks the lane.
    """
    if raw is None:
        return None
    try:
        value = int(raw)  # type: ignore[call-overload]  # guarded by except below
    except (TypeError, ValueError):
        return None
    return value if value > 0 else None


def _coerce_reason(raw: object) -> str | None:
    """Coerce a payload ``reason`` to a non-empty, capped str, else ``None``.

    A missing/None/non-string/blank reason surfaces as ``None`` — the audit note
    is optional and a bogus one must never fail the op. The length cap is
    re-applied on read so an oversized reason from any publisher (not just this
    library's builder) can never balloon the parsed op.
    """
    if not isinstance(raw, str):
        return None
    trimmed = raw.strip()
    if not trimmed:
        return None
    return trimmed[:MAX_REASON_LEN]
