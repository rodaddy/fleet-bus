"""Package-root compatibility import tests."""

from __future__ import annotations

import fleet_core.spec as core_spec
import fleet_core.transport as core_transport

import fleet_nats
import fleet_nats.heartbeat as heartbeat_module
import fleet_nats.subjects as subjects_module
from fleet_nats.bus import FleetBus as CompatFleetBus


def test_package_root_preserves_subjects_module() -> None:
    """``from fleet_nats import subjects`` still exposes fleet-nats subjects."""
    assert fleet_nats.subjects is subjects_module


def test_package_root_reexports_core_spec_types() -> None:
    """C7 can import shared typed message names through fleet-nats compatibility."""
    assert fleet_nats.Act is core_spec.Act
    assert fleet_nats.State is core_spec.State
    assert fleet_nats.Stance is core_spec.Stance
    assert fleet_nats.StancePosition is core_spec.StancePosition
    assert fleet_nats.Done is core_spec.Done
    assert fleet_nats.Completion is core_spec.Completion
    assert fleet_nats.Presence is core_spec.Presence
    assert fleet_nats.Heartbeat is core_spec.Heartbeat
    assert heartbeat_module.Heartbeat is core_spec.Heartbeat
    assert fleet_nats.Envelope is core_spec.Envelope


def test_package_root_reexports_transport_types() -> None:
    """FleetBus stays the compatibility subclass; shared transport names are core."""
    assert fleet_nats.FleetBus is CompatFleetBus
    assert issubclass(fleet_nats.FleetBus, core_transport.FleetBus)
    assert fleet_nats.FleetTransport is core_transport.FleetBus
    assert fleet_nats.DeliveryMode is core_transport.DeliveryMode
    assert fleet_nats.DurableNotProvisionedError is core_transport.DurableNotProvisionedError
    assert fleet_nats.EnvelopeHandler is core_transport.EnvelopeHandler
