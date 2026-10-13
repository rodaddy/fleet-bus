"""The Hermes gateway import seam — the one place the host runtime is imported.

The subscriber runs *inside* the Hermes gateway on an agent host, so it imports
the gateway's own modules (``gateway.platforms.base``, ``gateway.session``,
``gateway.config``) at runtime. Those packages exist only on the agent host, not
in this repo's venv — so every gateway import is funnelled through this module.

Under test (and in CI, off-host) the gateway is absent; tests install a stub into
:data:`gateway` before importing the adapter, exercising the envelope→MessageEvent
mapping and subscribe wiring without a real gateway. This keeps the sanctioned
``register_platform`` lever the only integration point (never the standalone
"pipeline" dead-end the plan warns against).

Verified against canonical Hermes v0.17.0
(``ai-agents/platforms/hermes/v0_17_0``): ``BasePlatformAdapter`` (base.py:1803),
``MessageEvent`` with ``internal: bool = False`` (base.py:1422/1474),
``SessionSource`` (session.py:71), ``Platform`` dynamic enum (config.py:136),
``register_platform`` (hermes_cli/plugins.py:770). v0.18 drift is non-breaking
(only ``SessionSource.guild_id → scope_id``, aliased).
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

from fleet_nats.tracing import traced


@runtime_checkable
class GatewayApi(Protocol):
    """The slice of the Hermes gateway runtime the adapter depends on.

    A structural type over the real ``gateway`` modules so the adapter can be
    unit-tested against a stub that satisfies this shape. The real objects
    (``BasePlatformAdapter``, ``MessageEvent``, ``SessionSource``, ``MessageType``,
    ``SendResult``, ``Platform``) are provided by the host at runtime.
    """

    BasePlatformAdapter: type[Any]
    MessageEvent: type[Any]
    MessageType: type[Any]
    SendResult: type[Any]
    SessionSource: type[Any]
    Platform: type[Any]


@traced
def load_gateway() -> GatewayApi:
    """Import and return the live Hermes gateway API objects.

    Raises:
        RuntimeError: If the gateway runtime is not importable — i.e. this is
            being run outside a Hermes agent host without a test stub installed.
            Fail loud (never a silent fallback) so a misdeploy is obvious.
    """
    try:
        from gateway.config import Platform  # type: ignore[import-not-found]
        from gateway.platforms.base import (  # type: ignore[import-not-found]
            BasePlatformAdapter,
            MessageEvent,
            MessageType,
            SendResult,
        )
        from gateway.session import SessionSource  # type: ignore[import-not-found]
    except ImportError as exc:  # pragma: no cover - host-only import
        raise RuntimeError(
            "fleet_subscriber.hermes.load_gateway: the Hermes gateway runtime is not "
            "importable. This plugin runs inside a Hermes agent host; off-host, tests "
            "must install a gateway stub. Original import error: " + str(exc)
        ) from exc

    class _Api:
        pass

    api = _Api()
    api.BasePlatformAdapter = BasePlatformAdapter  # type: ignore[attr-defined]
    api.MessageEvent = MessageEvent  # type: ignore[attr-defined]
    api.MessageType = MessageType  # type: ignore[attr-defined]
    api.SendResult = SendResult  # type: ignore[attr-defined]
    api.SessionSource = SessionSource  # type: ignore[attr-defined]
    api.Platform = Platform  # type: ignore[attr-defined]
    return api  # type: ignore[return-value]


if TYPE_CHECKING:
    # For type-checking only: the gateway types are Any (host-provided).
    pass
