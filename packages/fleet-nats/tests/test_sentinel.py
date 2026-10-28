"""Unit tests for the shared completion-sentinel wire shape."""

from __future__ import annotations

import json

from fleet_core.spec import Done

from fleet_nats import DONE_KIND, Envelope, done_envelope, is_done_envelope


def test_done_envelope_shape() -> None:
    env = done_envelope(msg_id="m1", ts="t", sender="wren", task_id="task-1", outcome="ok")
    assert env.kind == DONE_KIND == "done"
    assert env.task_id == "task-1"
    # The Done body now rides the shared four-slot Payload contract: the typed
    # Done is the `data` slot, the sender is the header `actor`, extras is the
    # only empty-legal slot, and the footer is empty (all-optional base).
    assert env.payload == {
        "header": {"actor": "wren"},
        "data": {"task_id": "task-1", "outcome": "ok"},
        "extras": {},
        "footer": {},
    }
    assert env.payload["data"] == {"task_id": "task-1", "outcome": "ok"}
    assert env.payload["header"]["actor"] == "wren"
    assert env.payload["extras"] == {}
    assert env.sender == "wren"
    # typed_signal is unchanged — coordination is unaffected by the body wrapping.
    assert isinstance(env.typed_signal, Done)
    assert env.typed_signal.task_id == "task-1"
    assert env.typed_signal.outcome == "ok"


def test_done_envelope_default_outcome() -> None:
    env = done_envelope(msg_id="m1", ts="t", sender="wren", task_id="task-1")
    assert env.payload["data"]["outcome"] == "completed"


def test_done_envelope_survives_the_wire() -> None:
    # The shape must round-trip through to_bytes/from_bytes unchanged — this is the
    # contract the orchestrator's parse_done relies on.
    env = done_envelope(msg_id="m1", ts="t", sender="wren", task_id="task-1", outcome="failed")
    back = Envelope.from_bytes(env.to_bytes())
    assert is_done_envelope(back)
    assert back.task_id == "task-1"
    assert back.payload == {
        "header": {"actor": "wren"},
        "data": {"task_id": "task-1", "outcome": "failed"},
        "extras": {},
        "footer": {},
    }
    assert isinstance(back.typed_signal, Done)
    assert back.typed_signal.task_id == "task-1"
    assert back.typed_signal.outcome == "failed"
    assert back.prefers_core_delivery() is False


def test_done_envelope_emits_typed_signal_declaration() -> None:
    env = done_envelope(msg_id="m1", ts="t", sender="wren", task_id="task-1")
    body = json.loads(env.to_bytes().decode("utf-8"))
    assert body["typed_signal"] == {
        "type": "done",
        "data": {"task_id": "task-1", "outcome": "completed"},
    }


def test_legacy_kind_based_done_recognition_still_works() -> None:
    env = Envelope.new(
        msg_id="m1",
        ts="t",
        sender="wren",
        kind=DONE_KIND,
        task_id="task-1",
        payload={"task_id": "task-1", "outcome": "ok"},
    )
    assert is_done_envelope(env)
    assert env.typed_signal is None
    assert env.prefers_core_delivery() is False


def test_is_done_envelope_false_for_status() -> None:
    env = Envelope.new(msg_id="m1", ts="t", sender="wren", kind="status")
    assert not is_done_envelope(env)
