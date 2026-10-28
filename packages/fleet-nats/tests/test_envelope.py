"""Functional tests for the Envelope contract — round-trip and validation."""

from __future__ import annotations

import json

import pytest
from fleet_core.spec import ENVELOPE_VERSION as CORE_ENVELOPE_VERSION
from fleet_core.spec import Envelope as CoreEnvelope

from fleet_nats.envelope import ENVELOPE_VERSION, Envelope


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
    )


def test_roundtrip_preserves_all_fields() -> None:
    env = _sample()
    restored = Envelope.from_bytes(env.to_bytes())
    assert restored == env


def test_fleet_nats_envelope_reexports_fleet_core_contract() -> None:
    assert Envelope is CoreEnvelope
    assert ENVELOPE_VERSION == CORE_ENVELOPE_VERSION


def test_wire_uses_from_key_not_sender() -> None:
    # The wire field is `from` (Python keyword) mapped to .sender.
    raw = _sample().to_bytes()
    assert b'"from":"heron"' in raw
    assert b'"sender"' not in raw


def test_roundtrip_minimal_optionals_none() -> None:
    env = Envelope.new(msg_id="x", ts="t", sender="wren", kind="status")
    restored = Envelope.from_bytes(env.to_bytes())
    assert restored.to is None
    assert restored.task_id is None
    assert restored.payload == {}
    assert restored.version == ENVELOPE_VERSION


@pytest.mark.parametrize(
    "field,value",
    [("id", ""), ("sender", ""), ("kind", "")],
)
def test_required_fields_rejected_empty(field: str, value: str) -> None:
    kwargs = {"id": "x", "ts": "t", "sender": "s", "kind": "k"}
    kwargs[field] = value
    with pytest.raises(ValueError):
        Envelope(**kwargs)  # type: ignore[arg-type]


def test_from_bytes_rejects_garbage() -> None:
    with pytest.raises(ValueError):
        Envelope.from_bytes(b"not json at all {{{")


def test_from_bytes_rejects_non_object() -> None:
    with pytest.raises(ValueError):
        Envelope.from_bytes(b"[1, 2, 3]")


@pytest.mark.parametrize(
    "bad_payload",
    [b'"a string"', b"[1,2,3]", b"42", b"true"],
)
def test_from_bytes_rejects_non_dict_payload(bad_payload: bytes) -> None:
    # A non-object payload would decode fine but break every handler's
    # payload.get(...) with an AttributeError (not a ValueError), slipping past
    # the subscribe() decode guard. It must be rejected at parse time.
    raw = b'{"id":"x","ts":"t","from":"s","kind":"k","payload":' + bad_payload + b"}"
    with pytest.raises(ValueError, match="payload must be a JSON object"):
        Envelope.from_bytes(raw)


def test_from_bytes_null_payload_becomes_empty_dict() -> None:
    raw = b'{"id":"x","ts":"t","from":"s","kind":"k","payload":null}'
    assert Envelope.from_bytes(raw).payload == {}


def test_from_bytes_coerces_scalar_fields_to_str() -> None:
    # A numeric "to" must not survive as an int despite the str | None annotation.
    raw = b'{"id":"x","ts":"t","from":"s","kind":"k","to":123,"task_id":456}'
    env = Envelope.from_bytes(raw)
    assert env.to == "123"
    assert isinstance(env.to, str)
    assert env.task_id == "456"


def test_from_bytes_refuses_newer_version() -> None:
    # C2 rule 3 — fail LOUD on a newer major. A message declaring a schema
    # version this build cannot honour is REFUSED with ValueError (routed to the
    # subscribe() decode guard), never accepted with fields silently dropped.
    raw = b'{"id":"x","ts":"t","from":"s","kind":"k","version":99}'
    with pytest.raises(ValueError, match="refusing message version 99"):
        Envelope.from_bytes(raw)


def test_from_bytes_version_gate_has_no_silent_path() -> None:
    # Prove there is NO silent acceptance: every version strictly greater than
    # ENVELOPE_VERSION must raise. (No return-with-warning branch survives.)
    for bump in (1, 2, 5, 99):
        bad = ENVELOPE_VERSION + bump
        raw = b'{"id":"x","ts":"t","from":"s","kind":"k","version":' + str(bad).encode() + b"}"
        with pytest.raises(ValueError, match="refusing message version"):
            Envelope.from_bytes(raw)


@pytest.mark.parametrize(
    "sneaky_version",
    [
        b"1.9",  # float would int()-truncate to 1 and SLIP the > gate
        b"2.0",  # float form of a newer major
        b"true",  # int(True)==1 would masquerade as current
        b"false",  # int(False)==0
    ],
)
def test_from_bytes_version_gate_no_float_or_bool_bypass(sneaky_version: bytes) -> None:
    # Regression (swarm antagonist): a float/bool version must NOT int()-coerce
    # past the gate. It is refused as a non-integer version — no silent accept,
    # no truncation that would also corrupt version on re-encode.
    raw = b'{"id":"x","ts":"t","from":"s","kind":"k","version":' + sneaky_version + b"}"
    with pytest.raises(ValueError, match="version must be a JSON integer"):
        Envelope.from_bytes(raw)


@pytest.mark.parametrize("bad", [b"0", b"-1", b"-99"])
def test_from_bytes_rejects_non_positive_version(bad: bytes) -> None:
    # Regression (swarm antagonist): the gate had no lower bound, so a zero/
    # negative version was accepted verbatim. Versions are >= 1.
    raw = b'{"id":"x","ts":"t","from":"s","kind":"k","version":' + bad + b"}"
    with pytest.raises(ValueError, match="version must be >= 1"):
        Envelope.from_bytes(raw)


@pytest.mark.parametrize("token", [b"NaN", b"Infinity", b"-Infinity"])
def test_from_bytes_rejects_non_finite_constants(token: bytes) -> None:
    # Regression (swarm antagonist): json accepts NaN/Infinity by default and
    # would re-emit the bare token, producing bytes strict parsers (Dash TS
    # JSON.parse) reject. Refuse them at decode so extra can't ferry poison bytes.
    raw = b'{"id":"x","ts":"t","from":"s","kind":"k","weird":' + token + b"}"
    with pytest.raises(ValueError, match="non-finite JSON constant"):
        Envelope.from_bytes(raw)


@pytest.mark.parametrize(
    "literal",
    # Floating-point literals (have '.'/'e') that overflow the IEEE-754 double to
    # +/-inf on parse. NB a huge *integer* literal (no '.'/'e') is NOT included:
    # json parses it via parse_int to an arbitrary-precision Python int, which is
    # finite and round-trips — only floats can overflow to inf.
    [b"1e400", b"-1e400", b"1E400", b"1.5e400"],
)
def test_from_bytes_rejects_overflow_float_in_unknown_field(literal: bytes) -> None:
    # Regression (terminal codex review MAJOR-1): a spec-valid numeric literal
    # that overflows to float('inf') is NOT a NaN/Infinity token, so it slips
    # parse_constant and Python decodes it to inf. It must be refused at DECODE
    # (parse_float), else it is accepted into extra and only blows up later at
    # to_bytes() with a raise that escapes the decode poison guard.
    raw = b'{"id":"x","ts":"t","from":"s","kind":"k","weird":' + literal + b"}"
    with pytest.raises(ValueError, match="overflows to a"):
        Envelope.from_bytes(raw)


def test_from_bytes_rejects_overflow_float_in_payload() -> None:
    # Same overflow nested inside payload (a JSON object) — must also be caught
    # at decode, not deferred to to_bytes.
    raw = b'{"id":"x","ts":"t","from":"s","kind":"k","payload":{"cost":1e400}}'
    with pytest.raises(ValueError, match="overflows to a"):
        Envelope.from_bytes(raw)


def test_from_bytes_accepts_finite_floats() -> None:
    # The parse_float guard must NOT reject ordinary finite floats.
    raw = b'{"id":"x","ts":"t","from":"s","kind":"k","score":3.14,"payload":{"n":-2.5e10}}'
    env = Envelope.from_bytes(raw)
    assert env.extra["score"] == 3.14
    assert env.payload["n"] == -2.5e10
    # And they round-trip.
    assert json.loads(env.to_bytes().decode("utf-8"))["score"] == 3.14


def test_from_bytes_accepts_huge_integer() -> None:
    # A giant integer literal (no '.'/'e') is arbitrary-precision and finite in
    # Python — it is NOT an overflow float and must be accepted + round-tripped.
    big = b"9" * 400
    raw = b'{"id":"x","ts":"t","from":"s","kind":"k","big":' + big + b"}"
    env = Envelope.from_bytes(raw)
    assert env.extra["big"] == int(big)
    assert json.loads(env.to_bytes().decode("utf-8"))["big"] == int(big)


def test_overflow_float_repro_no_longer_reaches_to_bytes() -> None:
    # The exact terminal-review repro: decode must raise, so to_bytes() is never
    # reached with a non-finite value (accept-set == re-emittable-set).
    repro = b'{"id":"x","ts":"t","from":"s","kind":"k","weird":1e400}'
    with pytest.raises(ValueError, match="overflows to a"):
        Envelope.from_bytes(repro)


def test_to_bytes_refuses_non_finite_payload() -> None:
    # allow_nan=False: a constructor-supplied non-finite value must not serialise
    # to poison bytes; the publisher gets a ValueError instead.
    env = Envelope.new(msg_id="x", ts="t", sender="s", kind="k", payload={"n": float("nan")})
    with pytest.raises(ValueError, match="Out of range float"):
        env.to_bytes()


@pytest.mark.parametrize("literal", [b"1e-4000", b"-1e-4000", b"2.5e-350"])
def test_from_bytes_rejects_underflow_float_in_unknown_field(literal: bytes) -> None:
    # Regression (terminal recheck NEW MAJOR): a spec-valid tiny-exponent literal
    # underflows to 0.0 through float, silently ZEROING a non-zero producer value.
    # rule 1 says unknown values survive UNCHANGED, so this is refused at DECODE
    # (accept-set == re-emittable-set) and routed to the subscribe() poison guard.
    raw = b'{"id":"x","ts":"t","from":"s","kind":"k","tiny":' + literal + b"}"
    with pytest.raises(ValueError, match="underflows to 0.0"):
        Envelope.from_bytes(raw)


def test_from_bytes_rejects_underflow_float_in_payload() -> None:
    # Same underflow nested inside payload — caught at decode, not silently zeroed
    # for a payload consumer.
    raw = b'{"id":"x","ts":"t","from":"s","kind":"k","payload":{"cost":1e-4000}}'
    with pytest.raises(ValueError, match="underflows to 0.0"):
        Envelope.from_bytes(raw)


def test_from_bytes_rejects_high_precision_decimal() -> None:
    # Regression (terminal recheck NEW MAJOR): a 35-significant-digit decimal is
    # rounded to the nearest double, so the re-emitted value differs from the wire
    # — a silent value change rule 1 forbids. Refused at DECODE.
    lit = b"1.2345678901234567890123456789012345"  # 35 significant digits
    raw = b'{"id":"x","ts":"t","from":"s","kind":"k","precise":' + lit + b"}"
    with pytest.raises(ValueError, match="loses precision"):
        Envelope.from_bytes(raw)


def test_from_bytes_accepts_big_finite_float_and_roundtrips() -> None:
    # A large-but-finite, precision-preserving float (1e300) is NOT lossy: it does
    # not overflow, underflow, or drop digits. It must be accepted and round-trip
    # to the exact same value (guards against the lossy check over-rejecting).
    raw = b'{"id":"x","ts":"t","from":"s","kind":"k","big":1e300,"payload":{"n":1e300}}'
    env = Envelope.from_bytes(raw)
    assert env.extra["big"] == 1e300
    assert env.payload["n"] == 1e300
    re_decoded = json.loads(env.to_bytes().decode("utf-8"))
    assert re_decoded["big"] == 1e300
    assert re_decoded["payload"]["n"] == 1e300


def test_from_bytes_preserves_unknown_float_value_on_roundtrip() -> None:
    # Positive round-trip: an accepted unknown float in extra re-emits byte-equal
    # in VALUE (the whole point — accepted numbers survive unchanged, per rule 1).
    raw = b'{"id":"x","ts":"t","from":"s","kind":"k","ratio":3.140625}'
    env = Envelope.from_bytes(raw)
    from decimal import Decimal

    before = Decimal("3.140625")
    after = Decimal(str(json.loads(env.to_bytes().decode("utf-8"))["ratio"]))
    assert before == after


def test_from_bytes_accepts_current_and_equal_version() -> None:
    # The gate is strictly `>`: the supported version itself is accepted.
    raw = (
        b'{"id":"x","ts":"t","from":"s","kind":"k","version":'
        + str(ENVELOPE_VERSION).encode()
        + b"}"
    )
    assert Envelope.from_bytes(raw).version == ENVELOPE_VERSION


def test_roundtrip_preserves_unknown_fields() -> None:
    # C2 rule 1 — an unknown top-level field (a newer same-major producer's
    # addition) must survive decode→encode intact. act/state are now typed
    # fleet-core fields, not opaque extras.
    raw = (
        b'{"id":"x","ts":"t","from":"s","kind":"k",'
        b'"act":"claim","state":"open","future_obj":{"n":1}}'
    )
    env = Envelope.from_bytes(raw)
    assert env.act == "claim"
    assert env.state == "open"
    assert env.extra == {"future_obj": {"n": 1}}
    # Re-encoded with the unknown keys byte-content-equal on decode.
    re_decoded = json.loads(env.to_bytes().decode("utf-8"))
    assert re_decoded["act"] == "claim"
    assert re_decoded["state"] == "open"
    assert re_decoded["future_obj"] == {"n": 1}


def test_extra_cannot_overwrite_known_fields() -> None:
    # Known fields win on collision: a stray "id" injected into extra must NOT
    # overwrite the typed id on the wire (defence against smuggled shadow fields).
    env = Envelope.new(msg_id="real", ts="t", sender="s", kind="k", act="request")
    env.extra = {"id": "spoofed", "act": "spoofed", "note": "keep"}
    body = json.loads(env.to_bytes().decode("utf-8"))
    assert body["id"] == "real"
    assert body["act"] == "request"
    assert body["note"] == "keep"


def test_act_state_survive_new_and_bytes_roundtrip() -> None:
    env = Envelope.new(
        msg_id="x",
        ts="t",
        sender="s",
        kind="status",
        act="request",
        state="working",
    )

    body = json.loads(env.to_bytes().decode("utf-8"))
    assert body["act"] == "request"
    assert body["state"] == "working"

    restored = Envelope.from_bytes(env.to_bytes())
    assert restored.act == "request"
    assert restored.state == "working"


def test_roundtrip_no_unknown_fields_has_empty_extra() -> None:
    # A message carrying only known fields round-trips with an empty extra and
    # byte-identical to the pre-C2 shape (no accidental extra key emitted).
    restored = Envelope.from_bytes(_sample().to_bytes())
    assert restored.extra == {}
    assert restored == _sample()


@pytest.mark.parametrize("bad_version", [b"null", b'"one"', b"[]"])
def test_from_bytes_rejects_non_int_version(bad_version: bytes) -> None:
    # A null/string/array version is not a JSON integer; must raise ValueError so
    # it hits the subscribe() decode guard rather than escaping later.
    raw = b'{"id":"x","ts":"t","from":"s","kind":"k","version":' + bad_version + b"}"
    with pytest.raises(ValueError):
        Envelope.from_bytes(raw)


@pytest.mark.parametrize("field", ["id", "ts", "from", "kind"])
def test_from_bytes_rejects_null_required_field(field: str) -> None:
    # A null required field must NOT become the string "None" (which would pass
    # the non-empty check as a bogus real value).
    body = {"id": "x", "ts": "t", "from": "s", "kind": "k"}
    body[field] = None  # type: ignore[assignment]
    import json as _json

    with pytest.raises(ValueError, match="non-empty string"):
        Envelope.from_bytes(_json.dumps(body).encode("utf-8"))


def test_from_bytes_rejects_missing_required_field() -> None:
    # Missing id entirely (not just null) is also rejected.
    with pytest.raises(ValueError, match="non-empty string"):
        Envelope.from_bytes(b'{"ts":"t","from":"s","kind":"k"}')
