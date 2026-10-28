"""The ``fleet_control`` tool — the manager's steer→control-plane bridge (#65).

The #61 governor control plane already exists in the shared transport
(:mod:`fleet_nats.control`): a ``kind="control"`` frame on
``subjects.hub_control(env, task_id)`` carrying ``{op, task_id, budget_msgs_add?}``
that the running hub consumes and turns into a live ``Governor.resume`` /
``Governor.pause``. What was missing is the *human* end of that wire: during
graduation runs a Discord steer ("resume the run and raise the budget") only
landed because a harness injected the control frame
(``injectors/governor-resume.py``). In production **nothing** maps a manager's
steer to a control op — the manager (Heron) sees the PAUSED narration beat in
Discord but has no lever to pull.

This tool is that lever. It publishes via the EXISTING
:func:`fleet_nats.control.publish_control` — no new frame format, no second
contract to drift from :func:`fleet_nats.control.parse_control`. The hub's
``dev.hub.control.*`` subscription does the rest.

Role gate (mirrors ``fleet_send``, #62): registered ONLY when
``FLEET_ROLE=manager``. Workers do not steer the governor; a non-manager load
skips registration with a debug log.

Task-id resolution (never guess — #65 requirement): the tool resolves the target
task from, in order:

1. an explicit ``task_id`` arg (the id the manager read off the PAUSED beat, e.g.
   ``[t_42] PAUSED over budget ...`` — the beat carries the id in-band), else
2. the current bus turn's task via :data:`fleet_subscriber.fleet_send.current_bus_task_id`
   (an in-turn steer inherits the task it is steering, exactly like ``fleet_send``).

If neither resolves the tool does NOT publish — it returns a structured error
telling the human to supply the id, rather than steering a guessed task. The
subscriber runs in a different process from the hub and cannot enumerate the
hub's in-memory paused-task set, so these two in-process signals are the honest
resolution universe.

Audit (WHO + WHY): the frame's ``sender`` is this agent's ``FLEET_AGENT_ID``
(WHO) and the ``reason`` rides the frame's payload (WHY) — so the audit record is
**hub-inspectable**, not only in this tool's local log. The hub logs both on the
resume/pause (``orchestrator.governor | resumed via control op ... sender=...
reason=...``). ``reason`` is length-capped by the wire builder
(:data:`fleet_nats.control.MAX_REASON_LEN`) and omitted from the frame when
blank, so a reason-less steer stays byte-identical to a pre-``reason`` frame.

Failure contract (mirrors ``fleet_send``): the handler NEVER raises out to the
gateway. Every failure — bad env, bad op, out-of-bounds budget, bad task_id,
publish error — returns a ``tool_error(...)`` string the model can read. Logging
is loud and sanitized: subjects, ids, ops, task, sender, and the audited
``reason`` are logged; nothing secret rides these fields.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

from fleet_nats import BusConfig, FleetBus, control, subjects
from loguru import logger

from fleet_subscriber.fleet_send import FLEET_SEND_TOOLSET, current_bus_task_id
from fleet_subscriber.role import SubscriberRole

# One toolset name for both bus tools (fleet_send + fleet_control) — the shared
# literal lives in one place (fleet_send) so the two tools can never drift onto
# different toolset names (#66 review nit).
FLEET_CONTROL_TOOLSET = FLEET_SEND_TOOLSET

# Tool-level upper bound on a single steer's budget bump. control.py enforces
# only positivity on the wire (a bump must be > 0); the TOOL is the human-facing
# surface, so it also caps the top — a model (or a fat-fingered steer) must not
# grant an effectively unbounded cap increase in one call. The wire stays the
# shared contract; this is the steer surface's own guardrail.
MAX_BUDGET_MSGS_ADD = 500

_KNOWN_OPS = (control.OP_RESUME, control.OP_PAUSE)

FLEET_CONTROL_SCHEMA: dict[str, Any] = {
    "name": "fleet_control",
    "description": (
        "Steer the fleet governor over the control plane: resume or pause a task, "
        "optionally raising THIS task's message budget. Use when a human in Discord "
        "asks you to resume/pause a paused run or grant more budget — this is the "
        "lever that makes that steer real. Manager-only."
    ),
    "parameters": {
        "type": "object",
        "properties": {
            "op": {
                "type": "string",
                "enum": list(_KNOWN_OPS),
                "description": "The control op: 'resume' (clear a pause) or 'pause' (gate a task).",
            },
            "task_id": {
                "type": "string",
                "description": (
                    "The task to steer (the id in the PAUSED beat, e.g. 't_42'). "
                    "Defaults to the current bus turn's task when omitted; if neither "
                    "resolves the tool refuses rather than guessing."
                ),
            },
            "budget_msgs_add": {
                "type": "integer",
                "description": (
                    "Optional message-budget increase on a resume — add exactly this "
                    f"many messages to THIS task's cap (positive, max {MAX_BUDGET_MSGS_ADD}). "
                    "Bounded and per-task; never touches the shared cap or another task."
                ),
            },
            "reason": {
                "type": "string",
                "description": (
                    "Why you are steering (e.g. 'per Rico'). Rides the control frame and "
                    "is audited hub-side alongside your identity (the frame sender)."
                ),
            },
        },
        "required": ["op"],
    },
}


def _tool_error(message: str) -> str:
    """Return a sanitized tool-error string the model can read.

    Mirrors the host ``tools.registry.tool_error`` shape (a JSON object with an
    ``error`` key) without importing the gateway, so this module stays
    host-import free and unit-testable off-host.
    """
    return json.dumps({"error": message})


def _now() -> str:
    """ISO-8601 UTC timestamp for the outbound control frame (live wire time)."""
    return datetime.now(UTC).isoformat()


def _new_id() -> str:
    """A fresh message id for the outbound control envelope."""
    return uuid4().hex


def _resolve_task_id(explicit: str | None) -> str | None:
    """Resolve the target task: explicit → current bus turn; never guess.

    Returns the resolved task id, or ``None`` when nothing resolved — the caller
    then refuses rather than steering a guess. The subscriber cannot read the
    hub's paused-task set (different process), so these two in-process signals are
    the honest resolution universe.
    """
    if explicit and explicit.strip():
        return explicit.strip()
    inherited = current_bus_task_id.get()
    if inherited:
        return inherited
    return None


def _validate_budget(raw: Any) -> tuple[int | None, str | None]:
    """Validate an optional ``budget_msgs_add`` against the tool's bounds.

    Returns ``(value, error)``. ``value`` is the accepted positive int (or
    ``None`` when omitted); ``error`` is a message when the input is present but
    out of bounds — the caller refuses without publishing rather than clamping
    silently (a steer's grant must be exactly what was asked or an explicit
    refusal, never a quietly-different number).
    """
    if raw is None:
        return None, None
    try:
        value = int(raw)
    except (TypeError, ValueError):
        return None, f"budget_msgs_add must be an integer, got {raw!r}"
    if value <= 0:
        return None, f"budget_msgs_add must be positive, got {value}"
    if value > MAX_BUDGET_MSGS_ADD:
        return None, f"budget_msgs_add must be <= {MAX_BUDGET_MSGS_ADD}, got {value}"
    return value, None


async def _fleet_control(args: dict[str, Any], **_kwargs: Any) -> str:
    """The ``fleet_control`` tool handler (async).

    Publishes a ``kind="control"`` frame via :func:`fleet_nats.control.publish_control`
    on the task's control lane; the hub resumes/pauses the live governor. Returns
    a human/model-readable receipt string; never raises out to the gateway.
    """
    op = str(args.get("op") or "").strip().lower()
    if op not in _KNOWN_OPS:
        return _tool_error(f"fleet_control: op must be one of {list(_KNOWN_OPS)}, got {op!r}")

    budget_msgs_add, budget_err = _validate_budget(args.get("budget_msgs_add"))
    if budget_err is not None:
        return _tool_error(f"fleet_control: {budget_err}")
    if budget_msgs_add is not None and op != control.OP_RESUME:
        return _tool_error("fleet_control: budget_msgs_add only applies to a 'resume' op")

    resolved = _resolve_task_id(args.get("task_id"))
    if resolved is None:
        # Never guess: refuse and tell the human how to name the task.
        return _tool_error(
            "fleet_control: no task to steer — pass task_id (the id in the PAUSED beat), "
            "or call this during the bus turn of the task you mean to steer"
        )

    # Slug the id ONCE, up front, so the subject (subjects.hub_control slugs it
    # internally), the payload task_id, and the receipt are all the SAME value —
    # a mixed-case/dotted id (e.g. "T_42") must not key the subject as "t_42"
    # while the payload/receipt carry "T_42" (hub-keying mismatch, #66 review).
    try:
        task_id = subjects.slug(resolved)
    except ValueError as exc:
        logger.error("fleet_control | task_id normalises to empty: {!r}", resolved)
        return _tool_error(f"fleet_control: invalid task_id {resolved!r}: {exc}")

    reason = str(args.get("reason") or "").strip()

    # Identity is non-negotiable — the frame's sender is this agent's
    # FLEET_AGENT_ID and is what the hub audits. A manager steering as "unknown"
    # is a misdeploy to catch loudly, not to let ride onto the control plane.
    try:
        bus_config = BusConfig.from_env()
    except ValueError as exc:
        logger.error("fleet_control | bad bus config: {}", exc)
        return _tool_error(f"fleet_control: bad bus config: {exc}")
    sender = bus_config.agent_id
    if not sender or sender == "unknown":
        logger.error("fleet_control | FLEET_AGENT_ID unset (sender resolved to {!r})", sender)
        return _tool_error(
            "fleet_control: FLEET_AGENT_ID is not set — cannot steer without an identity"
        )
    env = bus_config.env

    logger.info(
        "fleet_control | publishing subject={} from={} op={} task={} budget_add={} reason={!r}",
        subjects.hub_control(env, task_id),
        sender,
        op,
        task_id,
        budget_msgs_add,
        reason,
    )

    bus = FleetBus(bus_config)
    try:
        await bus.connect()
        # The EXISTING publisher — one frame, the shared contract parse_control
        # reads. No second frame format lives here.
        await control.publish_control(
            bus,
            env=env,
            task_id=task_id,
            sender=sender,
            op=op,
            budget_msgs_add=budget_msgs_add,
            reason=reason or None,
            msg_id=_new_id(),
            ts=_now(),
        )
    except Exception as exc:  # noqa: BLE001 — a bus fault must not crash the gateway
        logger.error("fleet_control | publish failed op={} task={}: {}", op, task_id, exc)
        return _tool_error(f"fleet_control: {op} on {task_id} failed: {type(exc).__name__}: {exc}")
    finally:
        try:
            await bus.close()
        except Exception as exc:  # noqa: BLE001
            logger.warning("fleet_control | bus close failed task={}: {}", task_id, exc)

    logger.info(
        "fleet_control | sent op={} task={} budget_add={} sender={}",
        op,
        task_id,
        budget_msgs_add,
        sender,
    )
    return _receipt(op=op, task_id=task_id, budget_msgs_add=budget_msgs_add, reason=reason)


def _receipt(*, op: str, task_id: str, budget_msgs_add: int | None, reason: str) -> str:
    """A human/model-readable steer receipt for the manager to voice in Discord.

    Reports what was PUBLISHED, not what the hub then did — the frame is
    fire-and-forget on the control lane (the hub logs the resume/pause and whether
    the task was actually paused). The receipt names op, task, the budget delta,
    and echoes the audited reason so the manager can post one clean line.
    """
    verb = "resumed" if op == control.OP_RESUME else "paused"
    parts = [f"{verb} {task_id}"]
    if budget_msgs_add is not None:
        parts.append(f"budget +{budget_msgs_add}")
    line = ", ".join(parts)
    if reason:
        line = f"{line} — {reason}"
    return f"Control frame published: {line}."


def register_fleet_control(ctx: Any) -> bool:
    """Register the ``fleet_control`` tool — MANAGER role only.

    Called from the plugin's ``register()``. Reads ``FLEET_ROLE``; when the agent
    is not a manager the tool is NOT registered and the reason is logged at debug
    (workers do not steer the governor — #65, mirrors ``fleet_send`` #60).

    Returns:
        True if the tool was registered, False if suppressed for role.
    """
    role = SubscriberRole.from_env()
    if role is not SubscriberRole.MANAGER:
        logger.debug("fleet_control | not registering — role={} (manager-only tool)", role.value)
        return False

    ctx.register_tool(
        name="fleet_control",
        toolset=FLEET_CONTROL_TOOLSET,
        schema=FLEET_CONTROL_SCHEMA,
        handler=_fleet_control,
        is_async=True,
        description=FLEET_CONTROL_SCHEMA["description"],
        emoji="🎛️",
    )
    logger.info("fleet_control | registered (manager role)")
    return True
