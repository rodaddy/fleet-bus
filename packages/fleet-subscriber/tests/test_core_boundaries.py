"""Compatibility boundaries that must resolve through fleet-core-backed shims."""

from __future__ import annotations

from fleet_core.config import AuthCredentials as CoreAuthCredentials
from fleet_core.config import BusConfig as CoreBusConfig
from fleet_core.config import CredentialSource as CoreCredentialSource
from fleet_core.spec import Envelope as CoreEnvelope
from fleet_core.spec import Heartbeat as CoreHeartbeat
from fleet_nats import BusConfig as NatsBusConfig
from fleet_nats import CredentialSource as NatsCredentialSource
from fleet_nats import Envelope as NatsEnvelope
from fleet_nats import Heartbeat as NatsHeartbeat

from fleet_subscriber import (
    AuthCredentials,
    BusConfig,
    CredentialSource,
    Envelope,
)


def test_subscriber_reexports_fleet_core_backed_envelope() -> None:
    assert Envelope is NatsEnvelope
    assert Envelope is CoreEnvelope


def test_subscriber_reexports_fleet_core_backed_config() -> None:
    assert BusConfig is NatsBusConfig
    assert BusConfig is CoreBusConfig
    assert AuthCredentials is CoreAuthCredentials
    assert CredentialSource is NatsCredentialSource
    assert CredentialSource is CoreCredentialSource


def test_subscriber_heartbeat_uses_fleet_core_model() -> None:
    assert NatsHeartbeat is CoreHeartbeat
