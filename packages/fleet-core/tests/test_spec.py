"""Functional tests for the fleet-core message spec."""

from __future__ import annotations

import json

import pytest
from pydantic import ValidationError

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
    signal_prefers_core,
)


def _sample() -> Envelope:
    return Envelope.new(
        msg_id="abc123",
        ts="2026-07-07T00:00:00Z",
        sender="heron",
        kind="dispatch",
        payload={"task": "review PR", "steps": [1, 2, 3]},
        to="finch",
        task_id="t_deadbeef",
        channel="all-agents",
        topic="review",
        correlation_id="corr-1",
        act=Act.REQUEST,
        state=State.SUBMITTED,
    )


def test_roundtrip_preserves_all_wire_fields() -> None:
    env = _sample()

    restored = Envelope.from_bytes(env.to_bytes())

    assert restored == env
    assert restored.act is Act.REQUEST
    assert restored.state is State.SUBMITTED


def test_wire_uses_from_key_not_sender() -> None:
    raw = _sample().to_bytes()

    assert b'"from":"heron"' in raw
    assert b'"sender"' not in raw


def test_act_and_state_default_to_none_for_backward_compatibility() -> None:
    env = Envelope.new(msg_id="x", ts="t", sender="wren", kind="status")
    restored = Envelope.from_bytes(env.to_bytes())

    assert restored.act is None
    assert restored.state is None
    assert restored.version == ENVELOPE_VERSION


def test_act_subset_and_state_values_are_exact() -> None:
    assert [item.value for item in Act] == [
        "request",
        "inform",
        "propose",
        "agree",
        "refuse",
        "failure",
    ]
    assert [item.value for item in State] == [
        "submitted",
        "working",
        "input-required",
        "completed",
        "failed",
    ]


@pytest.mark.parametrize("field", ["act", "state"])
def test_typed_fields_preserve_future_same_major_string_values(field: str) -> None:
    body = {"id": "x", "ts": "t", "from": "s", "kind": "k", field: "future-value"}

    env = Envelope.from_bytes(json.dumps(body).encode("utf-8"))
    re_decoded = json.loads(env.to_bytes().decode("utf-8"))

    assert getattr(env, field).value == "future-value"
    assert re_decoded[field] == "future-value"


@pytest.mark.parametrize(
    ("field", "enum_cls", "known_value"),
    [
        ("act", Act, Act.REQUEST.value),
        ("state", State, State.SUBMITTED.value),
    ],
)
def test_unknown_typed_fields_do_not_grow_enum_maps(
    field: str,
    enum_cls: type[Act] | type[State],
    known_value: str,
) -> None:
    value_map_size = len(enum_cls._value2member_map_)
    member_map_size = len(enum_cls._member_map_)
    canonical_values = [item.value for item in enum_cls]

    for index in range(500):
        future_value = f"future-{field}-{index}"
        body = {"id": "x", "ts": "t", "from": "s", "kind": "k", field: future_value}

        env = Envelope.from_bytes(json.dumps(body).encode("utf-8"))
        re_decoded = json.loads(env.to_bytes().decode("utf-8"))
        typed_value = getattr(env, field)

        assert isinstance(typed_value, enum_cls)
        assert typed_value.value == future_value
        assert re_decoded[field] == future_value

    assert len(enum_cls._value2member_map_) == value_map_size
    assert len(enum_cls._member_map_) == member_map_size
    assert [item.value for item in enum_cls] == canonical_values
    assert enum_cls(known_value) is enum_cls._value2member_map_[known_value]


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("act", 123),
        ("state", {"name": "future"}),
    ],
)
def test_typed_fields_reject_malformed_non_string_values(field: str, value: object) -> None:
    body = {"id": "x", "ts": "t", "from": "s", "kind": "k", field: value}

    with pytest.raises(ValidationError):
        Envelope.from_bytes(json.dumps(body).encode("utf-8"))


def test_unknown_fields_survive_deserialize_serialize() -> None:
    raw = (
        b'{"id":"x","ts":"t","from":"s","kind":"k",'
        b'"active_turn":"turn-1","role":"reviewer","future_obj":{"n":1}}'
    )

    env = Envelope.from_bytes(raw)
    re_decoded = json.loads(env.to_bytes().decode("utf-8"))

    assert env.extra == {"active_turn": "turn-1", "role": "reviewer", "future_obj": {"n": 1}}
    assert re_decoded["active_turn"] == "turn-1"
    assert re_decoded["role"] == "reviewer"
    assert re_decoded["future_obj"] == {"n": 1}


def test_pydantic_allowed_extras_roundtrip_without_legacy_extra() -> None:
    env = Envelope.model_validate(
        {
            "id": "x",
            "ts": "t",
            "sender": "s",
            "kind": "k",
            "payload": {},
            "active_turn": "turn-1",
            "lanes": ["core"],
        }
    )

    re_decoded = json.loads(env.to_bytes().decode("utf-8"))

    assert re_decoded["active_turn"] == "turn-1"
    assert re_decoded["lanes"] == ["core"]


def test_legacy_extra_and_pydantic_extra_collision_fails_loud() -> None:
    env = Envelope.model_validate(
        {
            "id": "x",
            "ts": "t",
            "sender": "s",
            "kind": "k",
            "payload": {},
            "extra": {"role": "reviewer"},
            "role": "implementer",
        }
    )

    with pytest.raises(ValueError, match="conflicting unknown extra fields: role"):
        env.to_bytes()


def test_extra_cannot_overwrite_known_fields() -> None:
    env = Envelope.new(msg_id="real", ts="t", sender="s", kind="k")
    env.extra["id"] = "spoofed"
    env.extra["note"] = "keep"

    body = json.loads(env.to_bytes().decode("utf-8"))

    assert body["id"] == "real"
    assert body["note"] == "keep"


def test_from_bytes_rejects_newer_version(caplog: pytest.LogCaptureFixture) -> None:
    raw = b'{"id":"x","ts":"t","from":"s","kind":"k","version":99}'

    caplog.set_level("WARNING", logger="fleet_core.spec")
    with pytest.raises(ValueError, match="refusing message version 99"):
        Envelope.from_bytes(raw)
    assert "refusing message version 99 > supported" in caplog.text


@pytest.mark.parametrize("sneaky_version", [b"1.9", b"2.0", b"true", b"false"])
def test_from_bytes_version_gate_no_float_or_bool_bypass(sneaky_version: bytes) -> None:
    raw = b'{"id":"x","ts":"t","from":"s","kind":"k","version":' + sneaky_version + b"}"

    with pytest.raises(ValueError, match="version must be a JSON integer"):
        Envelope.from_bytes(raw)


@pytest.mark.parametrize("token", [b"NaN", b"Infinity", b"-Infinity"])
def test_from_bytes_rejects_non_finite_constants(token: bytes) -> None:
    raw = b'{"id":"x","ts":"t","from":"s","kind":"k","weird":' + token + b"}"

    with pytest.raises(ValueError, match="non-finite JSON constant"):
        Envelope.from_bytes(raw)


@pytest.mark.parametrize("literal", [b"1e400", b"-1e400", b"1E400", b"1.5e400"])
def test_from_bytes_rejects_overflow_float(literal: bytes) -> None:
    raw = b'{"id":"x","ts":"t","from":"s","kind":"k","weird":' + literal + b"}"

    with pytest.raises(ValueError, match="overflows to a"):
        Envelope.from_bytes(raw)


@pytest.mark.parametrize("literal", [b"1e-4000", b"-1e-4000", b"2.5e-350"])
def test_from_bytes_rejects_underflow_float(literal: bytes) -> None:
    raw = b'{"id":"x","ts":"t","from":"s","kind":"k","tiny":' + literal + b"}"

    with pytest.raises(ValueError, match="underflows to 0.0"):
        Envelope.from_bytes(raw)


def test_from_bytes_rejects_high_precision_decimal() -> None:
    raw = (
        b'{"id":"x","ts":"t","from":"s","kind":"k","precise":1.2345678901234567890123456789012345}'
    )

    with pytest.raises(ValueError, match="loses precision"):
        Envelope.from_bytes(raw)


def test_from_bytes_rejects_non_object_payload() -> None:
    raw = b'{"id":"x","ts":"t","from":"s","kind":"k","payload":[1,2,3]}'

    with pytest.raises(ValueError, match="payload must be a JSON object"):
        Envelope.from_bytes(raw)


def test_to_bytes_rejects_non_finite_payload() -> None:
    env = Envelope.new(msg_id="x", ts="t", sender="s", kind="k", payload={"n": float("nan")})

    with pytest.raises(ValueError, match="Out of range float"):
        env.to_bytes()


def test_heartbeat_fields_match_live_identity_liveness_shape() -> None:
    beat = Heartbeat.model_validate(
        {
            "agent_id": "finch",
            "role": "worker",
            "lanes": ["dispatch", "status"],
            "seq": 7,
            "last_turn_ts": None,
            "catalog_version": 5,
            "ts": "2026-07-11T00:00:00Z",
            "active_turn": "turn-1",
        }
    )

    dumped = beat.model_dump(mode="json")

    assert dumped["agent_id"] == "finch"
    assert dumped["role"] == "worker"
    assert dumped["lanes"] == ["dispatch", "status"]
    assert dumped["seq"] == 7
    assert dumped["last_turn_ts"] is None
    assert dumped["catalog_version"] == 5
    assert dumped["jetstream"] is False
    assert dumped["active_turn"] == "turn-1"


def test_heartbeat_jetstream_true_fails_validation() -> None:
    with pytest.raises(ValidationError):
        Heartbeat.model_validate(
            {
                "agent_id": "finch",
                "role": "worker",
                "lanes": ["dispatch"],
                "seq": 1,
                "jetstream": True,
            }
        )


@pytest.mark.parametrize(
    "payload",
    [
        {"agent_id": "", "role": "worker", "lanes": ["dispatch"], "seq": 1},
        {"agent_id": "finch", "role": "", "lanes": ["dispatch"], "seq": 1},
        {"agent_id": "finch", "role": "worker", "lanes": [""], "seq": 1},
        {"agent_id": "finch", "role": "worker", "lanes": ["dispatch"], "seq": -1},
        {
            "agent_id": "finch",
            "role": "worker",
            "lanes": ["dispatch"],
            "seq": 1,
            "catalog_version": -1,
        },
    ],
)
def test_heartbeat_rejects_malformed_hard_fields(payload: dict[str, object]) -> None:
    with pytest.raises(ValidationError):
        Heartbeat.model_validate(payload)


def test_presence_serializes_explicit_heartbeat_shape() -> None:
    presence = Presence.model_validate(
        {"agent_id": "wren", "role": "manager", "lanes": ["control"], "seq": 0}
    )

    dumped = presence.model_dump(mode="json")

    assert dumped["agent_id"] == "wren"
    assert dumped["role"] == "manager"
    assert dumped["lanes"] == ["control"]
    assert dumped["jetstream"] is False


def test_stance_position_vocabulary_and_aliases() -> None:
    stance = Stance.model_validate(
        {
            "stance": "DISSENT",
            "fact_pull": True,
            "reason": "metric was stale",
            "needs_from": ["wren"],
            "trace_id": "tr-1",
        }
    )

    dumped = stance.model_dump(mode="json")

    assert [item.value for item in StancePosition] == ["agree", "dissent", "neutral"]
    assert stance.position is StancePosition.DISSENT
    assert stance.because == "metric was stale"
    assert "position" not in dumped
    assert "because" not in dumped
    assert dumped["stance"] == "dissent"
    assert dumped["fact_pull"] is True
    assert dumped["reason"] == "metric was stale"
    assert dumped["needs_from"] == ["wren"]
    assert dumped["trace_id"] == "tr-1"


@pytest.mark.parametrize(
    "payload",
    [
        {"stance": "maybe", "fact_pull": False},
        {"stance": "agree", "fact_pull": "yes"},
        {"stance": "agree", "needs_from": [""]},
    ],
)
def test_stance_rejects_malformed_hard_fields(payload: dict[str, object]) -> None:
    with pytest.raises(ValidationError):
        Stance.model_validate(payload)


def test_done_and_completion_alias_model_terminal_signal() -> None:
    done = Done.model_validate({"task_id": "task-1", "extra_note": "kept"})
    completion = Completion.model_validate({"task_id": "task-2", "outcome": "failed"})

    assert done.outcome == "completed"
    assert done.model_dump(mode="json")["extra_note"] == "kept"
    assert completion.task_id == "task-2"
    assert completion.outcome == "failed"


@pytest.mark.parametrize("payload", [{"task_id": ""}, {"task_id": "task-1", "outcome": ""}])
def test_done_rejects_empty_hard_fields(payload: dict[str, object]) -> None:
    with pytest.raises(ValidationError):
        Done.model_validate(payload)


# --- typed durability policy (C5 / #170) --------------------------------------


def test_heartbeat_and_presence_own_the_jetstream_false_field() -> None:
    """The ephemeral exception is a typed field on the signal model, not prose."""
    hb = Heartbeat(agent_id="heron", role="agent", lanes=("dev.all",), seq=0)
    pr = Presence(agent_id="heron", role="agent", lanes=("dev.all",), seq=0)
    assert hb.jetstream is False
    assert pr.jetstream is False


def test_heartbeat_jetstream_field_is_literal_false_only() -> None:
    """The heartbeat durability field cannot be flipped to True (Literal[False])."""
    with pytest.raises(ValidationError):
        Heartbeat.model_validate(
            {"agent_id": "n", "role": "agent", "lanes": ["dev.all"], "seq": 0, "jetstream": True}
        )


def test_signal_prefers_core_only_for_heartbeat_and_presence() -> None:
    """Only Heartbeat/Presence prefer core; every other signal (or non-signal) is durable."""
    assert signal_prefers_core(Heartbeat(agent_id="n", role="agent", lanes=("l",), seq=0)) is True
    assert signal_prefers_core(Presence(agent_id="n", role="agent", lanes=("l",), seq=0)) is True
    assert signal_prefers_core(Done(task_id="t1")) is False
    assert signal_prefers_core(Stance.model_validate({"stance": "agree"})) is False
    # A non-signal (None, dict, str) is never the exception.
    assert signal_prefers_core(None) is False
    assert signal_prefers_core({"jetstream": False}) is False
    assert signal_prefers_core("heartbeat") is False


def test_envelope_prefers_core_reads_bound_typed_signal_not_kind() -> None:
    """Envelope.prefers_core_delivery is driven by the bound typed signal, not kind."""
    hb = Heartbeat(agent_id="n", role="agent", lanes=("l",), seq=0)
    # kind="status" but bound to a heartbeat -> prefers core (reads the model).
    spoofed = Envelope.new(
        msg_id="m1", ts="2026-07-11T00:00:00Z", sender="n", kind="status", typed_signal=hb
    )
    assert spoofed.prefers_core_delivery() is True
    # kind="heartbeat" but no bound typed signal -> durable (no string inference).
    bare = Envelope.new(msg_id="m2", ts="2026-07-11T00:00:00Z", sender="n", kind="heartbeat")
    assert bare.prefers_core_delivery() is False


def test_typed_heartbeat_signal_survives_envelope_wire_roundtrip() -> None:
    """Heartbeat's typed durability identity survives serialization."""
    hb = Heartbeat(agent_id="n", role="agent", lanes=("l",), seq=0)
    env = Envelope.new(
        msg_id="m3", ts="2026-07-11T00:00:00Z", sender="n", kind="heartbeat", typed_signal=hb
    )
    raw = env.to_bytes()

    assert b'"typed_signal"' in raw
    decoded = json.loads(raw.decode("utf-8"))
    assert decoded["typed_signal"]["type"] == "heartbeat"
    assert decoded["typed_signal"]["data"]["jetstream"] is False

    restored = Envelope.from_bytes(raw)
    assert isinstance(restored.typed_signal, Heartbeat)
    assert restored.prefers_core_delivery() is True


def test_replay_forward_preserves_heartbeat_core_policy() -> None:
    hb = Heartbeat(agent_id="n", role="agent", lanes=("l",), seq=1)
    first = Envelope.new(
        msg_id="m4", ts="2026-07-11T00:00:00Z", sender="n", kind="status", typed_signal=hb
    )

    replayed = Envelope.from_bytes(Envelope.from_bytes(first.to_bytes()).to_bytes())

    assert isinstance(replayed.typed_signal, Heartbeat)
    assert replayed.prefers_core_delivery() is True


def test_presence_typed_signal_survives_wire_roundtrip() -> None:
    presence = Presence(agent_id="n", role="agent", lanes=("l",), seq=2)
    env = Envelope.new(
        msg_id="m5",
        ts="2026-07-11T00:00:00Z",
        sender="n",
        kind="presence",
        typed_signal=presence,
    )

    restored = Envelope.from_bytes(env.to_bytes())

    assert isinstance(restored.typed_signal, Presence)
    assert restored.prefers_core_delivery() is True


def test_normal_typed_signal_survives_wire_but_remains_durable() -> None:
    env = Envelope.new(
        msg_id="m6",
        ts="2026-07-11T00:00:00Z",
        sender="n",
        kind="done",
        typed_signal=Done(task_id="t1"),
    )

    restored = Envelope.from_bytes(env.to_bytes())

    assert isinstance(restored.typed_signal, Done)
    assert restored.prefers_core_delivery() is False


def test_unknown_typed_signal_declaration_is_preserved_but_durable() -> None:
    raw = (
        b'{"id":"x","ts":"t","from":"s","kind":"future",'
        b'"typed_signal":{"type":"future-signal","data":{"jetstream":false,"n":1}}}'
    )

    env = Envelope.from_bytes(raw)
    re_decoded = json.loads(env.to_bytes().decode("utf-8"))

    assert env.typed_signal is None
    assert env.prefers_core_delivery() is False
    assert re_decoded["typed_signal"] == {
        "type": "future-signal",
        "data": {"jetstream": False, "n": 1},
    }


@pytest.mark.parametrize(
    "typed_signal",
    [
        "heartbeat",
        {"type": "", "data": {}},
        {"type": "heartbeat", "data": "not-an-object"},
        {
            "type": "heartbeat",
            "data": {"agent_id": "n", "role": "agent", "lanes": ["l"], "seq": 0, "jetstream": True},
        },
    ],
)
def test_malformed_typed_signal_declaration_fails_loud(typed_signal: object) -> None:
    raw = json.dumps(
        {"id": "x", "ts": "t", "from": "s", "kind": "heartbeat", "typed_signal": typed_signal}
    ).encode("utf-8")

    with pytest.raises((ValueError, ValidationError)):
        Envelope.from_bytes(raw)


def test_legacy_heartbeat_envelope_without_typed_signal_remains_durable() -> None:
    raw = b'{"id":"x","ts":"t","from":"s","kind":"heartbeat","payload":{"jetstream":false}}'

    env = Envelope.from_bytes(raw)

    assert env.typed_signal is None
    assert env.prefers_core_delivery() is False
