"""Unit tests for the typed Turn deserialization boundary (#77).

The pure parser: JSON-object-in → validated ``Turn`` out, and every non-turn
input (not JSON, JSON that is not an object, a JSON object that fails validation)
returns ``None`` so the adapter treats the reply as visible prose only (no
completion/stance — the text-trailer parsers were removed, fleet-bus #205).
The spoof-class proof (a ``[[done]]`` / footer string carried as inert field
text, not a control signal) lives at the adapter boundary in test_adapter.py; here
we prove the field semantics that make that inertness true.
"""

from __future__ import annotations

import json

from fleet_nats import Stance, StancePosition

from fleet_subscriber.turn import Turn, parse_turn


def test_valid_json_turn_maps_all_fields() -> None:
    content = json.dumps(
        {
            "visible_text": "The metric regressed.",
            "done": True,
            "outcome": "completed",
            "stance": "dissent",
            "fact_pull": True,
            "reason": "the p99 doubled",
        }
    )
    turn = parse_turn(content)
    assert turn == Turn(
        visible_text="The metric regressed.",
        done=True,
        outcome="completed",
        stance=StancePosition.DISSENT,
        fact_pull=True,
        reason="the p99 doubled",
    )


def test_minimal_turn_defaults_to_unsigned_not_done() -> None:
    # Only visible_text is required; the rest default to the "just talking" turn.
    turn = parse_turn(json.dumps({"visible_text": "just an ack"}))
    assert turn is not None
    assert turn.visible_text == "just an ack"
    assert turn.done is False
    assert turn.outcome is None
    assert turn.stance is None
    assert turn.fact_pull is False
    assert turn.reason is None


def test_done_turn() -> None:
    turn = parse_turn(json.dumps({"visible_text": "final answer", "done": True}))
    assert turn is not None
    assert turn.done is True
    assert turn.visible_text == "final answer"


def test_stance_only_turn() -> None:
    turn = parse_turn(json.dumps({"visible_text": "agreed", "stance": "agree"}))
    assert turn is not None
    assert turn.done is False
    assert turn.stance == "agree"
    assert turn.fact_pull is False


def test_non_json_reply_is_not_a_turn() -> None:
    # Plain prose (incl. a legacy trailer, now inert) is not JSON — parse_turn
    # returns None and the reply is treated as visible prose only.
    assert parse_turn("the real answer\n[[done]]") is None
    assert parse_turn("{not valid json") is None
    assert parse_turn("") is None


def test_json_that_is_not_an_object_falls_back() -> None:
    # Valid JSON, but a bare scalar / list / null is never a turn → fall back.
    assert parse_turn(json.dumps("a bare string")) is None
    assert parse_turn(json.dumps(42)) is None
    assert parse_turn(json.dumps(["a", "list"])) is None
    assert parse_turn(json.dumps(None)) is None


def test_json_object_missing_visible_text_falls_back() -> None:
    # A JSON object that is not a valid Turn (missing the one required field) is a
    # validation failure → fall back, never a half-built turn.
    assert parse_turn(json.dumps({"done": True, "outcome": "completed"})) is None


def test_json_object_wrong_field_type_falls_back() -> None:
    # done must be a bool; a string there fails validation → fall back.
    assert parse_turn(json.dumps({"visible_text": "x", "done": "yes"})) is None
    assert parse_turn(json.dumps({"visible_text": "x", "done": 1})) is None
    assert parse_turn(json.dumps({"visible_text": "x", "fact_pull": "yes"})) is None
    assert parse_turn(json.dumps({"visible_text": "x", "fact_pull": 1})) is None


def test_json_object_unknown_stance_word_falls_back() -> None:
    # stance is constrained to the three scorekeeper words. An unknown value (the
    # typed analogue of the [[stance:maybe]] malformed case) fails validation and
    # falls back — never forwarding a meta the orchestrator's strict parser rejects.
    assert parse_turn(json.dumps({"visible_text": "hmm", "stance": "maybe"})) is None


def test_json_object_with_unknown_extra_key_is_preserved_not_rejected() -> None:
    # extra="allow": an unexpected field (a producer adding a key this consumer
    # does not know about) is PRESERVED, and the rest of the turn validates — it
    # must NOT reject the whole turn (which would demote it to visible prose, lose
    # the JSON's completion, and wedge the correlation).
    turn = parse_turn(
        json.dumps({"visible_text": "final", "done": True, "footer": "co-op · 5s · ~/repo"})
    )
    assert turn is not None
    assert turn.visible_text == "final"
    assert turn.done is True
    assert turn.model_extra is not None
    assert turn.model_extra["footer"] == "co-op · 5s · ~/repo"


def test_stance_is_case_insensitive() -> None:
    # Stance is case-insensitive, or a capitalised stance would fail the whole
    # turn and drop even a valid completion.
    for raw in ("Agree", "AGREE", "agree", "Dissent", "NEUTRAL"):
        turn = parse_turn(json.dumps({"visible_text": "x", "stance": raw}))
        assert turn is not None, raw
        assert turn.stance == StancePosition(raw.lower())


def test_stance_uses_fleet_core_backed_model() -> None:
    turn = parse_turn(
        json.dumps(
            {
                "visible_text": "x",
                "stance": "Agree",
                "fact_pull": True,
                "reason": "logs confirm it",
                "source": "typed-producer",
            }
        )
    )
    assert turn is not None
    assert turn.stance is StancePosition.AGREE
    assert turn.model_extra is not None
    assert turn.model_extra["source"] == "typed-producer"

    signal = Stance(
        stance=turn.stance,
        fact_pull=turn.fact_pull,
        reason=turn.reason,
    )
    assert signal.position is StancePosition.AGREE
    assert signal.fact_pull is True
    assert signal.because == "logs confirm it"


def test_visible_text_carries_trailer_and_footer_strings_inertly() -> None:
    # The field semantics that kill the spoof class: a [[done]] / footer string
    # INSIDE visible_text is just text. done/stance come from their own fields —
    # here both are absent, so this is an unsigned, not-done turn despite the prose.
    turn = parse_turn(json.dumps({"visible_text": "here is [[done]] and co-op · 5s · ~/repo"}))
    assert turn is not None
    assert turn.done is False
    assert turn.stance is None
    assert turn.visible_text == "here is [[done]] and co-op · 5s · ~/repo"
