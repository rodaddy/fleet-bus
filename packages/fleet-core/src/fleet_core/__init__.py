"""fleet-core base package for fleet-bus clients.

This package is intentionally skeletal in C1. Later slices fill the typed spec,
configuration, logging, and transport boundaries without requiring downstream
clients to invent their own bus contract.
"""

from __future__ import annotations

from importlib.metadata import version

from fleet_core.config import (
    AuthCredentials,
    BusConfig,
    CredentialSource,
)
from fleet_core.contract import (
    FooterBase,
    HeaderBase,
    Payload,
)
from fleet_core.logging import configure_logging
from fleet_core.spec import (
    ENVELOPE_VERSION,
    Act,
    Completion,
    Done,
    Envelope,
    Heartbeat,
    Presence,
    Stance,
    StancePosition,
    State,
)
from fleet_core.transport import (
    DeliveryMode,
    DurableNotProvisionedError,
    EnvelopeHandler,
    FleetBus,
    FleetTransport,
    PayloadTooLargeError,
)

__version__ = version("fleet-core")

__all__ = [
    "Act",
    "AuthCredentials",
    "BusConfig",
    "Completion",
    "CredentialSource",
    "Done",
    "DurableNotProvisionedError",
    "ENVELOPE_VERSION",
    "DeliveryMode",
    "Envelope",
    "EnvelopeHandler",
    "FleetBus",
    "FleetTransport",
    "FooterBase",
    "HeaderBase",
    "Heartbeat",
    "Payload",
    "PayloadTooLargeError",
    "Presence",
    "Stance",
    "StancePosition",
    "State",
    "__version__",
    "configure_logging",
]
