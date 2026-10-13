"""fleet-subscriber — the per-agent Hermes NATS ingress plugin.

Turns an inbound fleet-bus envelope into a real agent turn whose own words post
back out, using the sanctioned Hermes ``register_platform`` lever (never the
standalone-pipeline dead-end the plan warns against).

Public surface:
    SyntheticMessage / envelope_to_message  — the pure, gateway-free mapping.
    build_adapter_class                      — build the NatsAdapter against a
                                               loaded (real or stub) gateway API.
    register                                 — the Hermes plugin entry point.
    load_gateway / GatewayApi                — the host-runtime import seam.
"""

from __future__ import annotations

from fleet_subscriber.adapter import build_adapter_class, register
from fleet_subscriber.completion import Completion
from fleet_subscriber.config import AuthCredentials, BusConfig, CredentialSource
from fleet_subscriber.envelope import Envelope
from fleet_subscriber.fleet_send import (
    FLEET_SEND_SCHEMA,
    current_bus_task_id,
    register_fleet_send,
)
from fleet_subscriber.hermes import GatewayApi, load_gateway
from fleet_subscriber.mapping import SyntheticMessage, envelope_to_message
from fleet_subscriber.role import SubscriberRole, lanes_for_role
from fleet_subscriber.stance import StanceSignal
from fleet_subscriber.turn import Turn, parse_turn
from fleet_subscriber.voice_tee import VoiceTeeConfig, tee_to_discord

__all__ = [
    "FLEET_SEND_SCHEMA",
    "AuthCredentials",
    "BusConfig",
    "Completion",
    "CredentialSource",
    "Envelope",
    "GatewayApi",
    "StanceSignal",
    "SubscriberRole",
    "SyntheticMessage",
    "Turn",
    "VoiceTeeConfig",
    "build_adapter_class",
    "current_bus_task_id",
    "envelope_to_message",
    "lanes_for_role",
    "load_gateway",
    "parse_turn",
    "register",
    "register_fleet_send",
    "tee_to_discord",
]

__version__ = "0.1.0"
