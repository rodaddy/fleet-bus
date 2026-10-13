"""All-role client for the hub-owned, pull-only ``show_work`` service (#64)."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

from fleet_nats import BusConfig, Envelope, FleetBus, subjects
from loguru import logger

from fleet_subscriber.fleet_send import FLEET_SEND_TOOLSET

_MAX_TASK_ID_CHARS = 256
_MAX_REQUEST_BYTES = 512
_MAX_RESPONSE_BYTES = 12_000
_MAX_RESPONSE_ENVELOPE_BYTES = 64 * 1024
_TIMEOUT_S = 5.0

SHOW_WORK_TOOLSET = FLEET_SEND_TOOLSET
SHOW_WORK_SCHEMA: dict[str, Any] = {
    "name": "show_work",
    "description": "Retrieve one hub-rendered, redacted fleet work trail (read-only).",
    "parameters": {
        "type": "object",
        "properties": {"task_id": {"type": "string", "description": "Task id to retrieve."}},
        "required": ["task_id"],
        "additionalProperties": False,
    },
}


def _tool_error(message: str) -> str:
    rendered = json.dumps({"error": message})
    if len(rendered.encode("utf-8", "strict")) <= _MAX_RESPONSE_BYTES:
        return rendered
    return json.dumps({"error": "show_work: response unavailable"})


def _bounded_utf8(value: str) -> str | None:
    """Return strict UTF-8 text only when it fits the response budget."""
    try:
        encoded = value.encode("utf-8", "strict")
    except UnicodeError:
        return None
    if len(encoded) > _MAX_RESPONSE_BYTES:
        return None
    return encoded.decode("utf-8", "strict")


def _request(task_id: str, sender: str) -> Envelope:
    return Envelope.new(
        msg_id=uuid4().hex,
        ts=datetime.now(UTC).isoformat(),
        sender=sender,
        kind="show_work",
        payload={"task_id": task_id},
    )


async def _show_work(args: dict[str, Any], **_kwargs: Any) -> str:
    """Request a bounded result; never expose journal or transport internals."""
    raw_task_id = args.get("task_id")
    if not isinstance(raw_task_id, str) or not raw_task_id.strip():
        return _tool_error("show_work: 'task_id' is required")
    task_id = raw_task_id.strip()
    if len(task_id) > _MAX_TASK_ID_CHARS or set(args) != {"task_id"}:
        return _tool_error("show_work: invalid request")
    try:
        config = BusConfig.from_env()
        if not config.agent_id or config.agent_id == "unknown":
            raise ValueError
        request = _request(task_id, config.agent_id)
        if len(request.to_bytes()) > _MAX_REQUEST_BYTES:
            return _tool_error("show_work: invalid request")
        bus = FleetBus(config)
        try:
            await bus.connect()
            response = await bus.request(
                subjects.hub_show_work(config.env), request, timeout_s=_TIMEOUT_S
            )
        finally:
            await bus.close()
    except Exception as exc:  # noqa: BLE001 - gateway clients receive no transport internals
        logger.warning("show_work | request unavailable class={}", type(exc).__name__)
        return _tool_error("show_work: service unavailable")
    if (
        response is None
        or response.model_extra
        or response.extra
        or response.kind != "show_work"
        or response.correlation_id != request.id
        or any(
            field is not None
            for field in (
                response.to,
                response.task_id,
                response.channel,
                response.topic,
                response.act,
                response.state,
                response.typed_signal,
            )
        )
    ):
        return _tool_error("show_work: service unavailable")
    try:
        response_bytes = response.to_bytes()
    except (TypeError, UnicodeError, ValueError):
        return _tool_error("show_work: response unavailable")
    if len(response_bytes) > _MAX_RESPONSE_ENVELOPE_BYTES:
        return _tool_error("show_work: response unavailable")
    if set(response.payload) == {"error"} and isinstance(response.payload["error"], str):
        error = _bounded_utf8(response.payload["error"])
        if error is not None:
            return _tool_error(error)
        return _tool_error("show_work: response unavailable")
    if set(response.payload) != {"markdown"} or not isinstance(response.payload["markdown"], str):
        return _tool_error("show_work: response unavailable")
    markdown = _bounded_utf8(response.payload["markdown"])
    if markdown is None:
        return _tool_error("show_work: response unavailable")
    return markdown


def register_show_work(ctx: Any) -> bool:
    """Register the read-only tool for every supported subscriber role."""
    ctx.register_tool(
        name="show_work",
        toolset=SHOW_WORK_TOOLSET,
        schema=SHOW_WORK_SCHEMA,
        handler=_show_work,
        is_async=True,
        description=SHOW_WORK_SCHEMA["description"],
        emoji="🧾",
    )
    logger.info("show_work | registered (all roles, hub request/reply)")
    return True
