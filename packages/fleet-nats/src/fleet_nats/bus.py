"""fleet-nats compatibility wrapper over the fleet-core transport boundary."""

from __future__ import annotations

from typing import TYPE_CHECKING, Self

from fleet_core.config import redact_url
from fleet_core.transport import (
    DeliveryMode,
    DurableNotProvisionedError,
    EnvelopeHandler,
    PayloadTooLargeError,
    RawMessageHandler,
)
from fleet_core.transport import (
    FleetBus as FleetTransport,
)
from nats.aio.client import Client as NATS

from fleet_nats.config import BusConfig
from fleet_nats.envelope import Envelope
from fleet_nats.tracing import traced

if TYPE_CHECKING:
    from nats.aio.subscription import Subscription


class FleetBus(FleetTransport):
    """Backwards-compatible fleet-nats bus surface.

    ``fleet_core.transport.FleetBus`` owns lifecycle, auth, payload guarding,
    request/reply, reconnect, JetStream access, and durable fail-loud behavior.
    Legacy ``fleet_nats`` callers historically used core NATS for ordinary
    publish/subscribe/raw-subscribe calls, so this wrapper preserves that
    compatibility default unless a caller explicitly selects durable mode.
    Explicit ``mode=DeliveryMode.DURABLE`` is delegated without catching
    ``DurableNotProvisionedError`` or attempting a core fallback.
    """

    def __init__(self, config: BusConfig) -> None:
        """Store config while preserving the legacy ``fleet_nats.bus.NATS`` seam."""
        self._config = config
        self._nc = NATS()
        self._closing = False

    @traced
    async def __aenter__(self) -> Self:
        """Connect and return the fleet-nats wrapper type."""
        await self.connect()
        return self

    @traced
    async def publish(
        self,
        subject: str,
        envelope: Envelope,
        *,
        mode: DeliveryMode | None = None,
    ) -> None:
        """Publish an envelope, defaulting legacy fleet-nats callers to core NATS."""
        await super().publish(
            subject,
            envelope,
            mode=DeliveryMode.CORE if mode is None else mode,
        )

    @traced
    async def subscribe(
        self,
        subject: str,
        handler: EnvelopeHandler,
        *,
        mode: DeliveryMode | None = None,
        consumer: str | None = None,
        typed_signal: object | None = None,
    ) -> Subscription:
        """Subscribe to decoded envelopes, defaulting legacy callers to core NATS."""
        return await super().subscribe(
            subject,
            handler,
            mode=DeliveryMode.CORE if mode is None else mode,
            consumer=consumer,
            typed_signal=typed_signal,
        )

    @traced
    async def raw_subscribe(
        self,
        subject: str,
        handler: RawMessageHandler,
        *,
        mode: DeliveryMode | None = None,
        consumer: str | None = None,
        typed_signal: object | None = None,
    ) -> Subscription:
        """Subscribe to raw messages, defaulting legacy callers to core NATS."""
        return await super().raw_subscribe(
            subject,
            handler,
            mode=DeliveryMode.CORE if mode is None else mode,
            consumer=consumer,
            typed_signal=typed_signal,
        )


__all__ = [
    "DeliveryMode",
    "DurableNotProvisionedError",
    "EnvelopeHandler",
    "FleetBus",
    "FleetTransport",
    "PayloadTooLargeError",
    "RawMessageHandler",
    "redact_url",
]
