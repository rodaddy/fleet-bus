"""Functional tests for the shared payload contract (header/data/extras/footer).

These pin the three base-package invariants: every slot is dict-typed in shape,
only ``extras`` may be empty, and a wrong-for-the-type payload fails loud with a
reason (a ``ValidationError`` naming the offending field).
"""

from __future__ import annotations

import json

import pytest
from pydantic import BaseModel, ConfigDict, Field, ValidationError

from fleet_core.contract import FooterBase, HeaderBase, Payload


# ---- a representative message type (models OB's agent_context_pack shape) ----
class _PackHeader(HeaderBase):
    session_key: str
    platform: str


class _PackData(BaseModel):
    model_config = ConfigDict(extra="forbid")  # owner chooses strict

    operation: str = "agent_context_pack"
    query: str | None = None
    requested_sections: list[str] = Field(default_factory=list)


class _PackFooter(FooterBase):
    trace_id: str  # this type makes trace_id mandatory (narrows the optional base)


_PackPayload = Payload[_PackHeader, _PackData, _PackFooter]


def _valid() -> _PackPayload:
    return _PackPayload(
        header=_PackHeader(actor="discord", session_key="s1", platform="discord"),
        data=_PackData(query="who am i"),
        footer=_PackFooter(trace_id="t-123"),
    )


def test_valid_payload_round_trips_with_extras_as_empty_dict() -> None:
    dumped = json.loads(_valid().model_dump_json(exclude_none=True))
    assert dumped["extras"] == {}
    assert dumped["header"]["actor"] == "discord"
    assert dumped["data"]["operation"] == "agent_context_pack"
    assert dumped["footer"]["trace_id"] == "t-123"


def test_all_four_slots_are_dicts_in_shape() -> None:
    dumped = json.loads(_valid().model_dump_json(exclude_none=True))
    for slot in ("header", "data", "extras", "footer"):
        assert isinstance(dumped[slot], dict), slot


# ---- invariant 2: only extras may be empty ----
def test_empty_header_is_rejected() -> None:
    with pytest.raises(ValidationError) as exc:
        _PackPayload(header={}, data=_PackData(), footer=_PackFooter(trace_id="t"))  # type: ignore[arg-type]  # intentional: empty header
    assert exc.value.errors()[0]["loc"] == ("header", "actor")


def test_missing_required_header_field_is_rejected() -> None:
    with pytest.raises(ValidationError) as exc:
        _PackPayload(
            header={"actor": "x", "platform": "discord"},  # type: ignore[arg-type]  # intentional: missing session_key
            data=_PackData(),
            footer=_PackFooter(trace_id="t"),
        )
    assert exc.value.errors()[0]["loc"] == ("header", "session_key")


def test_empty_footer_is_rejected_when_type_requires_a_field() -> None:
    with pytest.raises(ValidationError) as exc:
        _PackPayload(
            header=_PackHeader(actor="x", session_key="s", platform="d"),
            data=_PackData(),
            footer={},  # type: ignore[arg-type]  # intentional: empty footer must reject
        )
    assert exc.value.errors()[0]["loc"] == ("footer", "trace_id")


def test_data_is_required_with_no_default() -> None:
    with pytest.raises(ValidationError) as exc:
        _PackPayload(  # type: ignore[call-arg]  # intentional: omitted required data must reject
            header=_PackHeader(actor="x", session_key="s", platform="d"),
            footer=_PackFooter(trace_id="t"),
        )
    assert exc.value.errors()[0]["loc"] == ("data",)


# ---- invariant 3: fail-loud with a reason ----
def test_mistyped_data_field_fails_loud_naming_the_field() -> None:
    with pytest.raises(ValidationError) as exc:
        _PackPayload(
            header=_PackHeader(actor="x", session_key="s", platform="d"),
            data={"requested_sections": "not-a-list"},  # type: ignore[arg-type]  # intentional: mistyped data
            footer=_PackFooter(trace_id="t"),
        )
    err = exc.value.errors()[0]
    assert err["loc"] == ("data", "requested_sections")
    assert "list" in err["msg"].lower()


def test_unknown_key_in_strict_data_is_rejected() -> None:
    with pytest.raises(ValidationError) as exc:
        _PackPayload(
            header=_PackHeader(actor="x", session_key="s", platform="d"),
            data={"bogus": 1},  # type: ignore[arg-type]  # intentional: unknown key in strict data
            footer=_PackFooter(trace_id="t"),
        )
    assert exc.value.errors()[0]["loc"] == ("data", "bogus")


def test_fifth_top_level_slot_is_rejected() -> None:
    with pytest.raises(ValidationError) as exc:
        _PackPayload(
            header=_PackHeader(actor="x", session_key="s", platform="d"),
            data=_PackData(),
            footer=_PackFooter(trace_id="t"),
            meta={"x": 1},  # type: ignore[call-arg]  # intentional: fifth slot must reject
        )
    assert exc.value.errors()[0]["loc"] == ("meta",)


# ---- invariant 1: extras shape coercion ----
def test_extras_none_coerces_to_empty_dict() -> None:
    payload = _PackPayload(
        header=_PackHeader(actor="x", session_key="s", platform="d"),
        data=_PackData(),
        footer=_PackFooter(trace_id="t"),
        extras=None,  # type: ignore[arg-type]  # intentional: None must coerce to {}
    )
    assert payload.extras == {}


@pytest.mark.parametrize("bad", [[], "x", 3, 1.5, True])
def test_non_dict_extras_is_rejected(bad: object) -> None:
    with pytest.raises(ValidationError) as exc:
        _PackPayload(
            header=_PackHeader(actor="x", session_key="s", platform="d"),
            data=_PackData(),
            footer=_PackFooter(trace_id="t"),
            extras=bad,  # type: ignore[arg-type]  # intentional: non-dict extras must reject
        )
    assert exc.value.errors()[0]["loc"] == ("extras",)


@pytest.mark.parametrize("slot", ["header", "data", "footer"])
@pytest.mark.parametrize("bad", [[], "x", 3, None])
def test_non_dict_header_data_footer_is_rejected(slot: str, bad: object) -> None:
    kwargs: dict[str, object] = {
        "header": _PackHeader(actor="x", session_key="s", platform="d"),
        "data": _PackData(),
        "footer": _PackFooter(trace_id="t"),
    }
    kwargs[slot] = bad
    with pytest.raises(ValidationError) as exc:
        _PackPayload(**kwargs)  # type: ignore[arg-type]  # intentional: non-dict slot must reject
    assert exc.value.errors()[0]["loc"][0] == slot


def test_wire_path_round_trip_validates_nested_models() -> None:
    wire = _valid().model_dump_json(exclude_none=True)
    parsed = _PackPayload.model_validate_json(wire)
    assert parsed.header.session_key == "s1"
    assert parsed.data.operation == "agent_context_pack"


def test_wire_path_rejects_wrong_for_type_body() -> None:
    bad_wire = json.dumps(
        {
            "header": {"actor": "x", "platform": "d"},  # missing session_key
            "data": {"operation": "agent_context_pack"},
            "footer": {"trace_id": "t"},
        }
    )
    with pytest.raises(ValidationError) as exc:
        _PackPayload.model_validate_json(bad_wire)
    assert exc.value.errors()[0]["loc"] == ("header", "session_key")


def test_post_construction_mutation_cannot_violate_the_contract() -> None:
    payload = _valid()
    with pytest.raises(ValidationError) as exc:
        payload.extras = [1, 2]  # type: ignore[assignment]  # intentional: mutation must re-validate
    assert exc.value.errors()[0]["loc"] == ("extras",)


def test_extras_accepts_arbitrary_dict_content() -> None:
    payload = _PackPayload(
        header=_PackHeader(actor="x", session_key="s", platform="d"),
        data=_PackData(),
        footer=_PackFooter(trace_id="t"),
        extras={"candidate_memory": ["a", "b"], "nested": {"k": 1}},
    )
    assert payload.extras["nested"] == {"k": 1}


def test_contract_exports_are_public() -> None:
    import fleet_core

    assert fleet_core.Payload is Payload
    assert fleet_core.HeaderBase is HeaderBase
    assert fleet_core.FooterBase is FooterBase
