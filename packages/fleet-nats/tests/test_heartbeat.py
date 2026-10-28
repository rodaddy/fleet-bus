"""Wire-shape tests for the shared heartbeat contract (#26)."""

from __future__ import annotations

import pytest
from fleet_core.spec import Heartbeat as CoreHeartbeat

from fleet_nats import Envelope, Heartbeat, heartbeat_envelope, parse_heartbeat, subjects


def test_heartbeat_import_path_reexports_fleet_core_contract() -> None:
    assert Heartbeat is CoreHeartbeat


def _beat() -> Envelope:
    return heartbeat_envelope(
        msg_id="m1",
        ts="2026-07-08T00:00:00Z",
        sender="finch",
        role="worker",
        lanes=("dispatch", "relay", "wake", "inbox"),
        seq=7,
        last_turn_ts="2026-07-07T23:59:00Z",
        catalog_version=5,
        package_version="0.3.1",
    )


def test_wire_round_trip() -> None:
    """builder -> bytes -> Envelope -> parse yields the identical beat."""
    env = Envelope.from_bytes(_beat().to_bytes())
    hb = parse_heartbeat(env)
    assert isinstance(hb, CoreHeartbeat)
    assert hb.agent_id == "finch"
    assert hb.role == "worker"
    assert hb.lanes == ("dispatch", "relay", "wake", "inbox")
    assert hb.seq == 7
    assert hb.last_turn_ts == "2026-07-07T23:59:00Z"
    assert hb.catalog_version == 5
    assert hb.package_version == "0.3.1"
    assert hb.ts == "2026-07-08T00:00:00Z"


def test_typed_heartbeat_roundtrip_prefers_core_delivery() -> None:
    """A helper-built heartbeat binds the typed signal that owns core delivery."""
    env = Envelope.from_bytes(_beat().to_bytes())
    assert isinstance(env.typed_signal, CoreHeartbeat)
    assert env.prefers_core_delivery() is True
    assert env.typed_signal.agent_id == "finch"
    assert env.typed_signal.catalog_version == 5
    assert env.typed_signal.package_version == "0.3.1"
    assert env.typed_signal.jetstream is False


def test_heartbeat_payload_stays_legacy_shape() -> None:
    """Payload consumers still see the pre-typed-signal heartbeat shape."""
    env = _beat()
    assert env.payload == {
        "agent_id": "finch",
        "role": "worker",
        "lanes": ["dispatch", "relay", "wake", "inbox"],
        "seq": 7,
        "last_turn_ts": "2026-07-07T23:59:00Z",
        "catalog_version": 5,
        "package_version": "0.3.1",
    }


def test_opaque_heartbeat_spoof_remains_durable() -> None:
    """kind/payload claims do not get the heartbeat core exception."""
    env = Envelope.new(
        msg_id="m",
        ts="t",
        sender="finch",
        kind="heartbeat",
        payload={
            "agent_id": "finch",
            "role": "worker",
            "lanes": ["inbox"],
            "seq": 1,
            "last_turn_ts": None,
            "jetstream": False,
        },
    )
    assert env.typed_signal is None
    assert env.prefers_core_delivery() is False
    assert parse_heartbeat(env).agent_id == "finch"


def test_catalog_version_absent_parses_as_none() -> None:
    """A beat from a client that predates the field (key absent) parses as
    unknown version — a schema add-on must never reject older live beats."""
    env = Envelope.new(
        msg_id="m",
        ts="t",
        sender="finch",
        kind="heartbeat",
        payload={
            "agent_id": "finch",
            "role": "worker",
            "lanes": ["inbox"],
            "seq": 1,
            "last_turn_ts": None,
            # catalog_version key deliberately omitted (old emitter)
        },
    )
    assert parse_heartbeat(env).catalog_version is None


def test_package_version_absent_parses_as_none() -> None:
    """A beat from a client that predates the field (key absent) parses as
    unknown install version — a schema add-on must never reject older live beats."""
    env = Envelope.new(
        msg_id="m",
        ts="t",
        sender="finch",
        kind="heartbeat",
        payload={
            "agent_id": "finch",
            "role": "worker",
            "lanes": ["inbox"],
            "seq": 1,
            "last_turn_ts": None,
            # package_version key deliberately omitted (old emitter)
        },
    )
    assert parse_heartbeat(env).package_version is None


def test_null_package_version_is_valid() -> None:
    env = heartbeat_envelope(
        msg_id="m4",
        ts="t",
        sender="wren",
        role="worker",
        lanes=("inbox",),
        seq=0,
        last_turn_ts=None,
        package_version=None,
    )
    assert parse_heartbeat(env).package_version is None


def test_package_version_round_trips_through_wire() -> None:
    """A package_version survives builder -> typed_signal AND to_bytes/from_bytes
    -> parse_heartbeat — install drift stays observable end to end (#203)."""
    env = heartbeat_envelope(
        msg_id="m5",
        ts="t",
        sender="wren",
        role="worker",
        lanes=("inbox",),
        seq=1,
        last_turn_ts=None,
        package_version="1.2.3",
    )
    assert isinstance(env.typed_signal, CoreHeartbeat)
    assert env.typed_signal.package_version == "1.2.3"
    round_tripped = Envelope.from_bytes(env.to_bytes())
    assert parse_heartbeat(round_tripped).package_version == "1.2.3"


def test_null_catalog_version_is_valid() -> None:
    env = heartbeat_envelope(
        msg_id="m3",
        ts="t",
        sender="wren",
        role="worker",
        lanes=("inbox",),
        seq=0,
        last_turn_ts=None,
        catalog_version=None,
    )
    assert parse_heartbeat(env).catalog_version is None


def test_null_last_turn_ts_is_valid() -> None:
    env = heartbeat_envelope(
        msg_id="m2",
        ts="t",
        sender="wren",
        role="worker",
        lanes=("inbox",),
        seq=0,
        last_turn_ts=None,
    )
    assert parse_heartbeat(env).last_turn_ts is None


def test_wrong_kind_rejected() -> None:
    env = Envelope.new(msg_id="m", ts="t", sender="x", kind="status", payload={})
    with pytest.raises(ValueError, match="not a heartbeat"):
        parse_heartbeat(env)


@pytest.mark.parametrize(
    "corrupt",
    [
        {"agent_id": ""},
        {"agent_id": 3},
        {"role": None},
        {"lanes": "dispatch"},
        {"lanes": [""]},
        {"lanes": [1, 2]},
        {"seq": -1},
        {"seq": "7"},
        {"seq": True},
        {"last_turn_ts": 12},
        {"catalog_version": -1},
        {"catalog_version": "5"},
        {"catalog_version": True},
        # Identity split: payload agent_id disagreeing with envelope.sender is
        # emitter drift or forgery — never let the weaker identity win silently.
        {"agent_id": "impostor"},
    ],
)
def test_malformed_payload_fails_loud(corrupt: dict[str, object]) -> None:
    """Every malformed field raises — a bad beat never counts as liveness."""
    env = _beat()
    bad = Envelope.new(
        msg_id="m",
        ts="t",
        sender="finch",
        kind="heartbeat",
        payload={**env.payload, **corrupt},
    )
    with pytest.raises(ValueError):
        parse_heartbeat(bad)


def test_wildcard_subject_matches_per_agent_form() -> None:
    """The consumer wildcard and the emitter subject agree token-for-token."""
    concrete = subjects.agent_heartbeat("dev", "finch")
    wild = subjects.agent_heartbeat_wildcard("dev")
    assert concrete.split(".")[:-1] == wild.split(".")[:-1]
    assert wild.endswith(".*")
    assert concrete == "dev.agent.heartbeat.finch"
