"""The one fleet-bus logging convention, importable from fleet-core.

Structured, sanitized logging is a standing fleet requirement (``docs/logging.md``),
not a per-package feature. Every fleet package logs the same way:

    logger.<level>("<module>.<operation> | key={} key={} ...", value, value, ...)

Loguru is the single logging dependency (``from loguru import logger``). This
module does NOT wrap or replace ``logger`` — call sites keep importing loguru's
``logger`` directly. What it provides is the one place a process configures the
sink: level, format, and JSON-vs-human rendering, resolved from the environment
so every service is configured the same way instead of each re-implementing
``logger.add`` / ``logger.remove``.

The never-log list (secrets, webhook URLs, Discord/Slack member ids, envelope
payload contents) and the required log points live in ``docs/logging.md`` and are
enforced per-phase in review; this helper only owns sink CONFIGURATION, never what
a call site chooses to log.
"""

from __future__ import annotations

import os
import sys

from loguru import logger

# The human-readable line format. The message itself already carries the
# ``<module>.<operation> | ...`` convention (docs/logging.md); the sink adds a
# timestamp, level, and source location around it. Kept close to loguru's default
# so a developer reading a terminal sees familiar output.
_HUMAN_FORMAT = (
    "<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | "
    "<level>{level: <8}</level> | "
    "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> | "
    "<level>{message}</level>"
)

# Env knobs — resolved here so every service configures logging identically.
_LEVEL_ENV = "FLEET_LOG_LEVEL"
_JSON_ENV = "FLEET_LOG_JSON"
_DEFAULT_LEVEL = "INFO"

# Accepted (case-insensitive) truthy/falsy spellings for FLEET_LOG_JSON — the
# same vocabulary the config env helpers accept, so an operator learns one set.
_TRUE_VALUES = frozenset({"1", "true", "yes", "on"})
_FALSE_VALUES = frozenset({"0", "false", "no", "off"})

# The valid loguru severity names, upper-cased. A typo in FLEET_LOG_LEVEL must
# fail loud (a mistyped level that silently defaulted could hide a service
# running far more or far less verbose than the operator asked for).
_VALID_LEVELS = frozenset({"TRACE", "DEBUG", "INFO", "SUCCESS", "WARNING", "ERROR", "CRITICAL"})


def _resolve_level(level: str | None) -> str:
    """Resolve the effective sink level, failing loud on an unknown name.

    Precedence: an explicit ``level`` argument wins, then ``FLEET_LOG_LEVEL``,
    then the ``INFO`` default. The result is validated against loguru's severity
    names so a typo (``INFOO``) raises instead of silently defaulting.
    """
    raw = level if level is not None else os.getenv(_LEVEL_ENV)
    if raw is None or not raw.strip():
        return _DEFAULT_LEVEL
    normalized = raw.strip().upper()
    if normalized not in _VALID_LEVELS:
        raise ValueError(f"{_LEVEL_ENV} must be one of {sorted(_VALID_LEVELS)}, got {raw!r}")
    return normalized


def _resolve_json(as_json: bool | None) -> bool:
    """Resolve JSON-vs-human rendering, failing loud on junk env values.

    Precedence: an explicit ``as_json`` argument wins, then ``FLEET_LOG_JSON``,
    then ``False`` (human-readable). A typo in the env value raises rather than
    silently choosing a format.
    """
    if as_json is not None:
        return as_json
    raw = os.getenv(_JSON_ENV)
    if raw is None:
        return False
    normalized = raw.strip().lower()
    if normalized == "":
        return False
    if normalized in _TRUE_VALUES:
        return True
    if normalized in _FALSE_VALUES:
        return False
    raise ValueError(
        f"{_JSON_ENV} must be one of "
        f"{sorted(_TRUE_VALUES)} (true) or {sorted(_FALSE_VALUES)} (false), "
        f"got {raw!r}"
    )


def configure_logging(*, level: str | None = None, as_json: bool | None = None) -> None:
    """Install the standard fleet-bus loguru sink on stderr (idempotent).

    This is the one place a fleet process configures logging. It removes loguru's
    default handler and installs a single stderr sink at the resolved level, in
    either the human format (default, dev) or newline-delimited JSON
    (``FLEET_LOG_JSON`` / ``as_json=True``, for a log pipeline).

    It configures the sink only; it does NOT change what call sites log. Call
    sites keep using ``from loguru import logger`` with the
    ``"<module>.<operation> | key={} ..."`` convention (``docs/logging.md``). The
    never-log list and required log points are review-enforced, not enforced here.

    Args:
        level: Explicit minimum level (loguru severity name, case-insensitive).
            Falls back to ``FLEET_LOG_LEVEL`` then ``INFO``.
        as_json: Force JSON (``True``) or human (``False``) rendering. Falls back
            to ``FLEET_LOG_JSON`` then human-readable.

    Raises:
        ValueError: If ``FLEET_LOG_LEVEL``/``level`` names an unknown level or
            ``FLEET_LOG_JSON`` holds a value that is neither truthy nor falsy.

    Idempotent: calling it again re-applies the (possibly re-resolved) config by
    removing existing handlers and re-adding the single sink, so a re-init in a
    test or a re-configured service never stacks duplicate sinks.
    """
    resolved_level = _resolve_level(level)
    resolved_json = _resolve_json(as_json)

    # Replace whatever is installed (loguru's default handler on first call, or a
    # prior configure_logging sink on a re-init) with exactly one sink — never
    # stack duplicates.
    logger.remove()
    if resolved_json:
        # serialize=True emits one JSON object per record; the format string is
        # ignored for the serialized payload, so we omit it.
        logger.add(sys.stderr, level=resolved_level, serialize=True, backtrace=False)
    else:
        logger.add(
            sys.stderr,
            level=resolved_level,
            format=_HUMAN_FORMAT,
            backtrace=False,
        )
    logger.debug(
        "fleet_core.logging.configure | level={} json={}",
        resolved_level,
        resolved_json,
    )
