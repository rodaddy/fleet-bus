"""Opt-in, sanitized function-boundary tracing for fleet packages."""

from __future__ import annotations

import inspect
import os
import re
from collections.abc import Callable, Mapping, Sequence
from functools import wraps
from pathlib import Path
from threading import Lock
from typing import Any, cast

from loguru import logger

from fleet_nats.redact import redact_text

TRACE_ENABLED = os.getenv("FLEET_TRACE") == "1"
_MAX_VALUE_CHARS = 300
_MAX_MAPPING_ITEMS = 20
_MAX_KEY_CHARS = 80
_PRIVATE_KEYS = frozenset(
    {"auth", "authorization", "key", "password", "secret", "token", "url", "webhook"}
)
_PROSE_KEYS = frozenset(
    {"body", "content", "message", "payload", "prompt", "reason", "text", "title"}
)
_SERIALIZED_PROSE_KEY = re.compile(
    r"^\s*(?:body|content|message|payload|prompt|reason|text|title)\s*(?:=|:|\[)",
    re.IGNORECASE,
)
_REDACTED = "[REDACTED]"
_DROPPED = "[DROPPED]"
_UNAVAILABLE = "[UNAVAILABLE]"
_TRACE_EXTRA_KEY = "fleet_trace"
if "_file_sink_lock" not in globals():
    _file_sink_lock = Lock()
_file_sink_configured = False
if "_file_sink_id" not in globals():
    _file_sink_id: int | None = None
if "_file_sink_path" not in globals():
    _file_sink_path: str | None = None


def _shorten(value: str) -> str:
    """Bound a rendered trace value after credential-shape redaction."""
    scrubbed = redact_text(value)
    if len(scrubbed) <= _MAX_VALUE_CHARS:
        return scrubbed
    return f"{scrubbed[:_MAX_VALUE_CHARS]}...[truncated]"


def _normalized_key(key: object) -> str | None:
    """Normalize a mapping key without allowing hostile key rendering to escape."""
    try:
        rendered = str(key)
    except Exception:  # noqa: BLE001 - trace rendering must not break application behavior
        return None
    camel_split = re.sub(r"(?<=[a-z0-9])(?=[A-Z])", "_", rendered)
    return re.sub(r"[^a-z0-9]+", "_", camel_split.lower()).strip("_")


def _safe_key(key: object) -> str:
    """Render a bounded mapping key, failing closed when it cannot be rendered."""
    try:
        rendered = str(key)
    except Exception:  # noqa: BLE001 - trace rendering must not break application behavior
        return _UNAVAILABLE
    camel_split = re.sub(r"(?<=[a-z0-9])(?=[A-Z])", "_", rendered)
    normalized = re.sub(r"[^a-z0-9]+", "_", camel_split.lower()).strip("_")
    if _is_private_key(normalized):
        return _REDACTED
    if normalized in _PROSE_KEYS or _SERIALIZED_PROSE_KEY.match(rendered):
        return _DROPPED
    return _shorten(rendered)[:_MAX_KEY_CHARS]


def _is_private_key(normalized: str) -> bool:
    return bool(set(normalized.split("_")) & _PRIVATE_KEYS)


def sanitize(value: object, *, key: object | None = None) -> object:
    """Return a bounded, never-payload trace representation of ``value``."""
    if key is not None:
        normalized = _normalized_key(key)
        if normalized is None:
            return _UNAVAILABLE
        if normalized in _PROSE_KEYS:
            return _DROPPED
        if _is_private_key(normalized):
            return _REDACTED
    if value is None or isinstance(value, (bool, int, float)):
        return value
    if isinstance(value, str):
        return _shorten(value)
    if isinstance(value, bytes):
        return f"<bytes:{len(value)}>"
    if value.__class__.__name__ == "Envelope":
        return {
            "kind": sanitize(getattr(value, "kind", None)),
            "task_id": sanitize(getattr(value, "task_id", None)),
            "sender": sanitize(getattr(value, "sender", None)),
            "to": sanitize(getattr(value, "to", None)),
        }
    if isinstance(value, Mapping):
        sanitized: dict[str, object] = {}
        for _, (item_key, item) in zip(range(_MAX_MAPPING_ITEMS), value.items(), strict=False):
            safe_key = _safe_key(item_key)
            sanitized[safe_key] = (
                safe_key
                if safe_key in {_DROPPED, _REDACTED, _UNAVAILABLE}
                else sanitize(item, key=item_key)
            )
        return sanitized
    if isinstance(value, Sequence):
        return [sanitize(item) for item in value[:_MAX_MAPPING_ITEMS]]
    if isinstance(value, Path):
        return _shorten(str(value))
    return f"<{type(value).__name__}>"


def _safe_sanitize(value: object) -> object:
    """Sanitize without allowing a malformed runtime value to break its caller."""
    try:
        return sanitize(value)
    except Exception as exc:  # noqa: BLE001 - tracing must never change application behavior
        try:
            logger.warning(
                "fleet.trace | sanitization failed value_type={} error_type={}",
                type(value).__name__,
                type(exc).__name__,
            )
        except Exception:  # noqa: BLE001 - no safe recovery remains if the logger itself failed
            pass
        return _UNAVAILABLE


def _safe_exception_message(exc: Exception) -> object:
    """Render an exception message without replacing the application exception."""
    try:
        return _safe_sanitize(str(exc))
    except BaseException:  # noqa: B036 - exception rendering must not change application behavior
        return _UNAVAILABLE


def _emit(level: str, event: str, function: str, value: object) -> None:
    """Emit a trace record without allowing observability to break the caller."""
    try:
        logger.bind(**{_TRACE_EXTRA_KEY: True}).log(
            level, "fleet.trace | event={} function={} value={}", event, function, value
        )
    except Exception as exc:  # noqa: BLE001 - tracing must never change application behavior
        try:
            logger.warning(
                "fleet.trace | emission failed function={} error_type={}",
                function,
                type(exc).__name__,
            )
        except Exception:  # noqa: BLE001 - no safe recovery remains if the logger itself failed
            pass


def _arguments(
    function: Callable[..., object], args: tuple[object, ...], kwargs: dict[str, object]
) -> object:
    """Bind and sanitize call arguments, excluding conventional instance receivers."""
    try:
        bound = inspect.signature(function).bind_partial(*args, **kwargs)
        values = dict(bound.arguments)
    except (TypeError, ValueError):
        values = {"args": args, "kwargs": kwargs}
    values.pop("self", None)
    values.pop("cls", None)
    return _safe_sanitize(values)


def traced[**P, R](function: Callable[P, R]) -> Callable[P, R]:
    """Trace sync or async call boundaries when ``FLEET_TRACE=1`` at startup."""
    function_name = function.__qualname__
    if inspect.iscoroutinefunction(function):

        @wraps(function)
        async def async_wrapper(*args: P.args, **kwargs: P.kwargs) -> Any:
            if not TRACE_ENABLED:
                return await function(*args, **kwargs)
            _emit("DEBUG", "in", function_name, _arguments(function, args, kwargs))
            try:
                result = await function(*args, **kwargs)
            except Exception as exc:
                _emit(
                    "ERROR",
                    "error",
                    function_name,
                    {"type": type(exc).__name__, "message": _safe_exception_message(exc)},
                )
                raise
            _emit("DEBUG", "out", function_name, _safe_sanitize(result))
            return result

        return cast(Callable[P, R], async_wrapper)

    @wraps(function)
    def sync_wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
        if not TRACE_ENABLED:
            return function(*args, **kwargs)
        _emit("DEBUG", "in", function_name, _arguments(function, args, kwargs))
        try:
            result = function(*args, **kwargs)
        except Exception as exc:
            _emit(
                "ERROR",
                "error",
                function_name,
                {"type": type(exc).__name__, "message": _safe_exception_message(exc)},
            )
            raise
        _emit("DEBUG", "out", function_name, _safe_sanitize(result))
        return result

    return sync_wrapper


def _is_trace_record(record: Any) -> bool:
    """Keep the optional incident file limited to explicitly bound trace records."""
    return record["extra"].get(_TRACE_EXTRA_KEY) is True


def _configure_file_sink() -> None:
    """Add the optional rotated trace sink, degrading visibly on failure."""
    global _file_sink_configured, _file_sink_id, _file_sink_path
    path = os.getenv("FLEET_TRACE_FILE") if TRACE_ENABLED else None
    with _file_sink_lock:
        if _file_sink_id is not None and _file_sink_path == path:
            _file_sink_configured = True
            return
        if _file_sink_id is not None:
            try:
                logger.remove(_file_sink_id)
            except Exception as exc:  # noqa: BLE001 - optional observability must not stop startup
                try:
                    logger.warning(
                        "fleet.trace | file sink cleanup failed path={} error_type={}",
                        _file_sink_path,
                        type(exc).__name__,
                    )
                except Exception:  # noqa: BLE001 - no safe recovery remains if the logger itself failed
                    pass
                return
            _file_sink_id = None
            _file_sink_path = None
            _file_sink_configured = False
        if path is None:
            return
        try:
            _file_sink_id = logger.add(
                path,
                filter=_is_trace_record,
                rotation="50 MB",
                retention="7 days",
                compression="zip",
                enqueue=True,
            )
        except Exception as exc:  # noqa: BLE001 - optional observability must not stop startup
            try:
                logger.warning(
                    "fleet.trace | file sink unavailable path={} error_type={}",
                    path,
                    type(exc).__name__,
                )
            except Exception:  # noqa: BLE001 - no safe recovery remains if the logger itself failed
                pass
        else:
            _file_sink_path = path
            _file_sink_configured = True


_configure_file_sink()
