"""The heartbeat wire shape — the one shared ``kind="heartbeat"`` contract (#26).

Each subscriber emits a bounded-interval heartbeat on its
``subjects.agent_heartbeat(env, agent)`` lane; the orchestrator (and, when it
unparks, the dashboard) derives per-agent liveness/topology from it. Emitter
(fleet-subscriber) and consumer (fleet-orchestrator) live in different
packages — exactly the drift risk that produced the dispatch-subject mismatch —
so the shape lives here, in the transport layer both import, with one builder
and one strict parser. Neither side hand-rolls the dict.

The heartbeat carries identity/liveness metadata ONLY: agent id, role, the
effective subscribed lanes, a monotonic sequence counter, and the timestamp of
the last agent turn (nullable). Never message bodies, secrets, or member ids —
the ``docs/logging.md`` never-log list applies to this payload verbatim.
"""

from __future__ import annotations

from typing import Any

from fleet_core.spec import Heartbeat as Heartbeat

from fleet_nats.envelope import Envelope
from fleet_nats.tracing import traced

# The heartbeat's kind + payload keys — the shared contract. One definition.
HEARTBEAT_KIND = "heartbeat"
AGENT_ID_KEY = "agent_id"
ROLE_KEY = "role"
LANES_KEY = "lanes"
SEQ_KEY = "seq"
LAST_TURN_TS_KEY = "last_turn_ts"
# The effective subject-catalog version the emitter connected with (#103). This
# is how version skew becomes observable on the bus: the orchestrator reads it
# off every beat and can see mixed-version populations. Nullable so beats from
# clients that predate this field parse cleanly as "unknown version" rather than
# being rejected — never let a schema add-on take a live agent's liveness offline.
CATALOG_VERSION_KEY = "catalog_version"
# The installed fleet-package version the emitter runs (#203). This is how
# *install* drift — distinct from *catalog* skew — becomes observable on the bus:
# the orchestrator reads it off every beat and can see mixed-install populations.
# Nullable so beats from clients that predate this field parse cleanly as
# "unknown version" rather than being rejected — never let a schema add-on take a
# live agent's liveness offline.
PACKAGE_VERSION_KEY = "package_version"


@traced
def heartbeat_envelope(
    *,
    msg_id: str,
    ts: str,
    sender: str,
    role: str,
    lanes: tuple[str, ...],
    seq: int,
    last_turn_ts: str | None,
    catalog_version: int | None = None,
    package_version: str | None = None,
) -> Envelope:
    """Build the heartbeat envelope a subscriber emits each interval.

    Publish on ``subjects.agent_heartbeat(env, sender)``. Caller supplies id/ts
    (mirrors :meth:`Envelope.new`) so beats stay deterministic under test.

    Args:
        msg_id: Unique message id.
        ts: ISO-8601 UTC timestamp of this beat.
        sender: The emitting agent's id (also carried in the payload).
        role: The subscriber role (``worker``/``manager``).
        lanes: The effective subscribed lane names.
        seq: Monotonic beat counter.
        last_turn_ts: ISO timestamp of the last agent turn, or ``None``.
        catalog_version: The subject-catalog version the emitter connected with
            (#103); ``None`` when the emitter has no version to report. Carried
            so the orchestrator can observe version skew across the fleet.
        package_version: The installed fleet-package version the emitter runs
            (#203); ``None`` when the emitter has no version to report. Carried
            so the orchestrator can observe install drift across the fleet.

    Returns:
        The ``kind="heartbeat"`` :class:`Envelope`.
    """
    heartbeat = Heartbeat(
        agent_id=sender,
        role=role,
        lanes=lanes,
        seq=seq,
        last_turn_ts=last_turn_ts,
        catalog_version=catalog_version,
        package_version=package_version,
        ts=ts,
    )
    return Envelope.new(
        msg_id=msg_id,
        ts=ts,
        sender=sender,
        kind=HEARTBEAT_KIND,
        payload=_legacy_payload(heartbeat),
        typed_signal=heartbeat,
    )


@traced
def parse_heartbeat(envelope: Envelope) -> Heartbeat:
    """Parse a received heartbeat strictly. Fail loud on a malformed one.

    Args:
        envelope: A received envelope from the heartbeat lane.

    Returns:
        The parsed :class:`Heartbeat`.

    Raises:
        ValueError: If the envelope is not a heartbeat or its payload is
            malformed (wrong types / missing keys). Callers log and skip —
            a bad beat must be visible, never silently counted as liveness.
    """
    if envelope.kind != HEARTBEAT_KIND:
        raise ValueError(f"not a heartbeat envelope: kind={envelope.kind!r}")
    body = envelope.payload
    agent_id = body.get(AGENT_ID_KEY)
    if not isinstance(agent_id, str) or not agent_id:
        raise ValueError("heartbeat payload missing/invalid agent_id")
    if agent_id != envelope.sender:
        # One identity per beat: a payload/sender split means emitter drift (or
        # forgery) — reject rather than silently letting the weaker one win.
        raise ValueError(
            f"heartbeat identity mismatch: sender={envelope.sender!r} agent_id={agent_id!r}"
        )
    return Heartbeat.model_validate(
        {
            AGENT_ID_KEY: agent_id,
            ROLE_KEY: body.get(ROLE_KEY),
            LANES_KEY: body.get(LANES_KEY),
            SEQ_KEY: body.get(SEQ_KEY),
            LAST_TURN_TS_KEY: body.get(LAST_TURN_TS_KEY),
            CATALOG_VERSION_KEY: body.get(CATALOG_VERSION_KEY),
            PACKAGE_VERSION_KEY: body.get(PACKAGE_VERSION_KEY),
            "ts": envelope.ts,
        }
    )


def _legacy_payload(heartbeat: Heartbeat) -> dict[str, Any]:
    """Return the pre-C6 heartbeat payload while binding typed_signal separately."""
    return heartbeat.model_dump(mode="json", exclude={"jetstream", "ts"})


__all__ = [
    "AGENT_ID_KEY",
    "CATALOG_VERSION_KEY",
    "HEARTBEAT_KIND",
    "Heartbeat",
    "LANES_KEY",
    "LAST_TURN_TS_KEY",
    "PACKAGE_VERSION_KEY",
    "ROLE_KEY",
    "SEQ_KEY",
    "heartbeat_envelope",
    "parse_heartbeat",
]
