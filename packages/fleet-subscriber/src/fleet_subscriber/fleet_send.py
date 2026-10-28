"""The ``fleet_send`` tool — the manager's agent-invocable bus-send lever.

Deliberation between fleet agents happens over NATS, not Discord (Rico's
ratified model). The subscriber already handles the *receive* side (dispatch →
agent turn) and the *reply* side (turn text → status lane). What was missing is a
way for the **manager** (Heron) to actively *say* something on the bus —
summon a worker into a deliberation, pose a question, send steering — without a
Discord mention.

This tool publishes a ``kind="dispatch"`` :class:`~fleet_nats.Envelope` on the
target worker's dispatch subject (:func:`fleet_nats.subjects.agent_dispatch`).
That is the exact lane a worker's subscriber consumes and forges into a real
agent turn (``adapter._on_envelope`` → ``_forge_turn`` →
:func:`fleet_subscriber.mapping.envelope_to_message`): the mapping requires a
``payload.text`` and a resolvable channel, both of which this tool sets — so a
``fleet_send`` call becomes a live turn on the target worker.

Role gate (issue #60): the tool is registered ONLY when ``FLEET_ROLE=manager``.
Workers reply naturally through the existing reply path; they do NOT get a
free-send tool for now (prevents cross-talk storms). When a non-manager loads the
plugin the registration is skipped with a debug log.

Task-id inheritance: when the current turn is itself bus-driven, the adapter
stashes the inbound envelope's ``task_id`` in a contextvar
(:data:`current_bus_task_id`) for the duration of the turn. ``fleet_send``
inherits that id when the caller omits ``task_id``, so a summoned deliberation
stays keyed to the same k-board task. Off a bus turn (or when the var is unset)
it mints a fresh ``t_chat<ts>`` id.

Failure contract: the handler NEVER raises out to the gateway. Every failure —
missing identity, bad env, publish error — returns a ``tool_error(...)`` string
the model can read. Logging is loud and sanitized: subjects, ids, kinds, and
the sender/target are logged; the message ``text`` (agent prose) is NEVER logged.
"""

from __future__ import annotations

import time
from contextvars import ContextVar
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

from fleet_nats import BusConfig, Envelope, FleetBus, subjects
from fleet_nats.tracing import traced
from loguru import logger

from fleet_subscriber.role import SubscriberRole

# Set by the adapter for the duration of a bus-driven turn (see
# adapter._forge_turn). Holds the inbound envelope's task_id so an in-turn
# fleet_send inherits it. Default None → the tool mints a fresh chat task id.
current_bus_task_id: ContextVar[str | None] = ContextVar(
    "fleet_subscriber_current_bus_task_id", default=None
)

# Default deliberation channel when the caller omits one. Matches the arena
# channel the fleet uses for open cross-agent deliberation.
_DEFAULT_CHANNEL = "arena"

FLEET_SEND_TOOLSET = "fleet_bus"

FLEET_SEND_SCHEMA: dict[str, Any] = {
    "name": "fleet_send",
    "description": (
        "Send a message to another fleet agent over the NATS bus, starting a real "
        "turn for them. Use this to summon a worker into a deliberation, pose a "
        "question, or send steering — bus-native, no Discord mention needed. "
        "Manager-only."
    ),
    "parameters": {
        "type": "object",
        "properties": {
            "to": {
                "type": "string",
                "description": "The target agent's id/name (e.g. 'finch', 'wren').",
            },
            "text": {
                "type": "string",
                "description": "The message to send — it becomes the target agent's turn prompt.",
            },
            "channel": {
                "type": "string",
                "description": (
                    "Deliberation channel the exchange rides on (default 'arena'). "
                    "The target's reply posts back to this channel."
                ),
            },
            "task_id": {
                "type": "string",
                "description": (
                    "k-board task id to key this exchange to. Defaults to the current "
                    "bus turn's task when omitted, else a fresh chat id is minted."
                ),
            },
        },
        "required": ["to", "text"],
    },
}


def _tool_error(message: str) -> str:
    """Return a sanitized tool-error string the model can read.

    Mirrors the host ``tools.registry.tool_error`` shape (a JSON object with an
    ``error`` key) without importing the gateway — this module stays host-import
    free so it is unit-testable off-host, exactly like the rest of the plugin.
    """
    import json

    return json.dumps({"error": message})


def _now() -> str:
    """ISO-8601 UTC timestamp for the outbound envelope (live wire time)."""
    return datetime.now(UTC).isoformat()


def _new_id() -> str:
    """A fresh message id for the outbound dispatch envelope."""
    return uuid4().hex


def _resolve_task_id(explicit: str | None) -> str:
    """Pick the task id: explicit → current bus turn → a fresh chat id.

    A summoned deliberation should stay keyed to the task that summoned it, so an
    omitted task_id inherits the in-turn bus context. Off a bus turn we mint a
    ``t_chat<epoch_ms>`` id — a distinct, sortable namespace so a chat-initiated
    thread never collides with a real k-board task id.
    """
    if explicit and explicit.strip():
        return explicit.strip()
    inherited = current_bus_task_id.get()
    if inherited:
        return inherited
    return f"t_chat{int(time.time() * 1000)}"


@traced
async def _fleet_send(args: dict[str, Any], **_kwargs: Any) -> str:
    """The ``fleet_send`` tool handler (async).

    Publishes a ``kind="dispatch"`` envelope to the target's dispatch subject so
    the target's subscriber forges it into a turn. Returns a human/model-readable
    result string; never raises out to the gateway.
    """
    to = str(args.get("to") or "").strip()
    text = str(args.get("text") or "").strip()
    channel = str(args.get("channel") or "").strip() or _DEFAULT_CHANNEL
    if not to:
        return _tool_error("fleet_send: 'to' (target agent) is required")
    if not text:
        return _tool_error("fleet_send: 'text' (message) is required")

    # Identity is non-negotiable: the envelope's sender is this agent's
    # FLEET_AGENT_ID. BusConfig.from_env() falls back to the literal "unknown"
    # when the var is unset — a manager publishing as "unknown" is a misdeploy we
    # must catch loudly, not let ride onto the bus.
    try:
        bus_config = BusConfig.from_env()
    except ValueError as exc:
        logger.error("fleet_send | bad bus config: {}", exc)
        return _tool_error(f"fleet_send: bad bus config: {exc}")
    sender = bus_config.agent_id
    if not sender or sender == "unknown":
        logger.error("fleet_send | FLEET_AGENT_ID unset (sender resolved to {!r})", sender)
        return _tool_error(
            "fleet_send: FLEET_AGENT_ID is not set — cannot publish without an identity"
        )

    task_id = _resolve_task_id(args.get("task_id"))
    env = bus_config.env

    try:
        subject = subjects.agent_dispatch(env, to, channel=channel)
    except ValueError as exc:
        # slug() rejects an empty/whitespace target or channel.
        logger.error("fleet_send | bad subject token to={!r} channel={!r}: {}", to, channel, exc)
        return _tool_error(f"fleet_send: invalid target or channel: {exc}")

    # dispatch is the lane a WORKER's subscriber consumes and forges into a turn.
    # The envelope MUST carry channel + payload.text: envelope_to_message reads
    # chat_id from envelope.channel and the turn prompt from payload.text, and
    # returns None (skip) without both.
    envelope = Envelope.new(
        msg_id=_new_id(),
        ts=_now(),
        sender=sender,
        kind="dispatch",
        to=to,
        task_id=task_id,
        channel=channel,
        payload={"text": text},
    )

    logger.info(
        "fleet_send | publishing subject={} from={} to={} channel={} task={} kind=dispatch",
        subject,
        sender,
        to,
        channel,
        task_id,
    )

    bus = FleetBus(bus_config)
    try:
        await bus.connect()
        await bus.publish(subject, envelope)
    except Exception as exc:  # noqa: BLE001 — a bus fault must not crash the gateway
        logger.error("fleet_send | publish failed subject={} to={}: {}", subject, to, exc)
        return _tool_error(f"fleet_send: publish to {to} failed: {type(exc).__name__}: {exc}")
    finally:
        # Always tear the short-lived connection down — never leak a socket/task.
        try:
            await bus.close()
        except Exception as exc:  # noqa: BLE001
            logger.warning("fleet_send | bus close failed to={}: {}", to, exc)

    logger.info("fleet_send | sent subject={} to={} task={}", subject, to, task_id)
    return (
        f"Sent to {to} on channel '{channel}' (task {task_id}). "
        f"Their reply will come back on the bus."
    )


@traced
def register_fleet_send(ctx: Any) -> bool:
    """Register the ``fleet_send`` tool — MANAGER role only.

    Called from the plugin's ``register()``. Reads ``FLEET_ROLE``; when the agent
    is not a manager the tool is NOT registered and the reason is logged at debug
    (workers reply through the existing path; they do not get a free-send tool —
    issue #60, prevents cross-talk storms).

    Returns:
        True if the tool was registered, False if suppressed for role.
    """
    role = SubscriberRole.from_env()
    if role is not SubscriberRole.MANAGER:
        logger.debug("fleet_send | not registering — role={} (manager-only tool)", role.value)
        return False

    ctx.register_tool(
        name="fleet_send",
        toolset=FLEET_SEND_TOOLSET,
        schema=FLEET_SEND_SCHEMA,
        handler=_fleet_send,
        is_async=True,
        description=FLEET_SEND_SCHEMA["description"],
        emoji="📣",
    )
    logger.info("fleet_send | registered (manager role)")
    return True
