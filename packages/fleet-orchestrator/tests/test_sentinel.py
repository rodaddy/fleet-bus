"""Unit tests for the completion sentinel (build + parse) — pure, no bus."""

from __future__ import annotations

from fleet_nats import Done, Envelope

from fleet_orchestrator import EventKind, done_envelope, is_done_envelope, parse_done


def test_done_envelope_shape() -> None:
    env = done_envelope(msg_id="m1", ts="t", sender="skippy", task_id="task-1", outcome="completed")
    assert env.kind == "done"
    assert env.task_id == "task-1"
    assert env.payload == {
        "header": {"actor": "skippy"},
        "data": {"task_id": "task-1", "outcome": "completed"},
        "extras": {},
        "footer": {},
    }
    assert isinstance(env.typed_signal, Done)
    assert env.typed_signal.task_id == "task-1"
    assert env.typed_signal.outcome == "completed"
    assert is_done_envelope(env)


def test_done_envelope_default_outcome() -> None:
    env = done_envelope(msg_id="m1", ts="t", sender="skippy", task_id="task-1")
    assert env.payload["data"]["outcome"] == "completed"


def test_parse_done_returns_done_event() -> None:
    env = done_envelope(msg_id="m1", ts="t", sender="skippy", task_id="task-1", outcome="ok")
    event = parse_done(env)
    assert event is not None
    assert event.kind is EventKind.DONE
    assert event.actor == "skippy"
    assert event.outcome == "ok"


def test_parse_done_reads_typed_done_after_wire_round_trip() -> None:
    env = done_envelope(msg_id="m1", ts="t", sender="skippy", task_id="task-1", outcome="ok")
    event = parse_done(Envelope.from_bytes(env.to_bytes()))
    assert event is not None
    assert event.kind is EventKind.DONE
    assert event.actor == "skippy"
    assert event.outcome == "ok"


def test_parse_done_prefers_typed_done_outcome() -> None:
    env = Envelope.new(
        msg_id="m1",
        ts="t",
        sender="skippy",
        kind="done",
        payload={"task_id": "payload-task", "outcome": "payload-outcome"},
        typed_signal=Done(task_id="typed-task", outcome="typed-outcome"),
    )
    event = parse_done(env)
    assert event is not None
    assert event.outcome == "typed-outcome"


def test_parse_done_ignores_non_done_envelope() -> None:
    env = Envelope.new(msg_id="m1", ts="t", sender="skippy", kind="status", task_id="t1")
    assert parse_done(env) is None


def test_parse_done_rejects_sentinel_without_task_id() -> None:
    # A done signal that names no task can't advance any session — reject it.
    env = Envelope.new(msg_id="m1", ts="t", sender="skippy", kind="done", payload={})
    assert parse_done(env) is None


def test_parse_done_recovers_task_id_from_nested_data_slot() -> None:
    # Typed signal absent; task_id/outcome live under the wrapped `data` slot of
    # the four-slot Payload contract. The fallback reads the nested shape.
    env = Envelope.new(
        msg_id="m1",
        ts="t",
        sender="skippy",
        kind="done",
        payload={
            "header": {"actor": "skippy"},
            "data": {"task_id": "t9", "outcome": "ok"},
            "extras": {},
            "footer": {},
        },
    )
    event = parse_done(env)
    assert event is not None
    assert event.outcome == "ok"


def test_parse_done_recovers_task_id_from_flat_payload_back_compat() -> None:
    # Back-compat: a mid-deploy old-format sentinel carries task_id/outcome flat
    # at the top level (pre-Payload wrapping). The fallback still parses it.
    env = Envelope.new(msg_id="m1", ts="t", sender="skippy", kind="done", payload={"task_id": "t9"})
    event = parse_done(env)
    assert event is not None
    assert event.outcome == "completed"
