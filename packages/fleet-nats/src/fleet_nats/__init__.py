"""fleet-nats — canonical NATS transport + coordination library for the fleet.

One library, many thin consumers: trio coordination, OB memory transport,
k-board events, per-agent inboxes, local dev. Mirrors the Dash TS contract in
``@example/core-nats`` so both languages agree on subjects and envelopes.

Public surface:
    Envelope       — the typed message contract.
    BusConfig      — connection config (url from env, not baked).
    FleetBus       — connect / publish / subscribe / request over NATS core.
    subjects       — subject-building helpers (the naming convention).
    Payload        — the standard four-slot ``Envelope.payload`` body contract.
    HeaderBase     — base for a message type's payload header (``actor`` required).
    FooterBase     — base for a message type's payload footer (all-optional meta).
"""

from __future__ import annotations

from fleet_core import FooterBase, HeaderBase, Payload
from fleet_core.spec import Act, Completion, Done, Presence, Stance, StancePosition, State

from fleet_nats.bus import (
    DeliveryMode,
    DurableNotProvisionedError,
    EnvelopeHandler,
    FleetBus,
    FleetTransport,
    PayloadTooLargeError,
)
from fleet_nats.channel_map import (
    ChannelMap,
    ChannelMapError,
    ChannelPortal,
    empty_map,
    fetch_channel_map,
    is_subject_safe_slug,
    load_channel_map,
    parse_portals,
    parse_static_spec,
)
from fleet_nats.config import AuthCredentials, BusConfig, CredentialSource
from fleet_nats.control import (
    CONTROL_KIND,
    OP_PAUSE,
    OP_RESUME,
    ControlOp,
    control_envelope,
    is_control_envelope,
    parse_control,
    publish_control,
)
from fleet_nats.envelope import Envelope
from fleet_nats.heartbeat import (
    HEARTBEAT_KIND,
    Heartbeat,
    heartbeat_envelope,
    parse_heartbeat,
)
from fleet_nats.jetstream import (
    FleetKV,
    Incarnation,
    KeyWatch,
    KVEntry,
    ReplayMessage,
    ReplayStream,
    StaleRevisionError,
    create_kv,
    ensure_replay_stream,
    open_kv,
)
from fleet_nats.killflag import (
    DEFAULT_BUCKET,
    DEFAULT_KEY,
    FleetKVKillAuthority,
    KillAuthorityConfig,
    KillAuthorityError,
    open_kill_authority,
)
from fleet_nats.killprobe import StoreKillProbe
from fleet_nats.narration import NARRATION_KIND
from fleet_nats.registry import (
    CatalogVersionTooLowError,
    LoadedCatalog,
    RegistryUnreachableError,
    SubjectCatalog,
    code_catalog,
    connect_catalog,
    diff_catalogs,
    enforce_min_version,
    fetch_catalog,
    load_catalog,
    load_catalog_detailed,
)
from fleet_nats.sentinel import (
    DEFAULT_OUTCOME,
    DONE_KIND,
    done_envelope,
    is_done_envelope,
)
from fleet_nats.showwork import (
    Frame,
    RenderedTranscript,
    Summary,
    reconstruct,
    reconstruct_json,
    render_markdown,
)
from fleet_nats.tokencost import (
    TOKEN_COST_KIND,
    TokenCost,
    parse_token_cost,
    token_cost_envelope,
)

__all__ = [
    "CONTROL_KIND",
    "DEFAULT_OUTCOME",
    "DONE_KIND",
    "OP_PAUSE",
    "OP_RESUME",
    "Act",
    "AuthCredentials",
    "BusConfig",
    "CatalogVersionTooLowError",
    "ChannelMap",
    "ChannelMapError",
    "ChannelPortal",
    "Completion",
    "ControlOp",
    "CredentialSource",
    "DEFAULT_BUCKET",
    "DEFAULT_KEY",
    "DeliveryMode",
    "Done",
    "DurableNotProvisionedError",
    "Envelope",
    "EnvelopeHandler",
    "FleetBus",
    "FleetKV",
    "FleetKVKillAuthority",
    "HeaderBase",
    "FleetTransport",
    "FooterBase",
    "Frame",
    "Incarnation",
    "KVEntry",
    "KillAuthorityConfig",
    "KillAuthorityError",
    "KeyWatch",
    "Payload",
    "PayloadTooLargeError",
    "HEARTBEAT_KIND",
    "LoadedCatalog",
    "NARRATION_KIND",
    "Presence",
    "Heartbeat",
    "RegistryUnreachableError",
    "RenderedTranscript",
    "ReplayMessage",
    "ReplayStream",
    "StaleRevisionError",
    "StoreKillProbe",
    "Stance",
    "StancePosition",
    "State",
    "SubjectCatalog",
    "Summary",
    "TOKEN_COST_KIND",
    "TokenCost",
    "code_catalog",
    "connect_catalog",
    "control_envelope",
    "create_kv",
    "diff_catalogs",
    "done_envelope",
    "empty_map",
    "ensure_replay_stream",
    "enforce_min_version",
    "open_kv",
    "open_kill_authority",
    "fetch_catalog",
    "fetch_channel_map",
    "is_subject_safe_slug",
    "load_catalog",
    "load_channel_map",
    "parse_portals",
    "parse_static_spec",
    "load_catalog_detailed",
    "heartbeat_envelope",
    "is_control_envelope",
    "parse_control",
    "publish_control",
    "parse_heartbeat",
    "parse_token_cost",
    "reconstruct",
    "reconstruct_json",
    "render_markdown",
    "token_cost_envelope",
    "is_done_envelope",
]

__version__ = "0.1.0"
