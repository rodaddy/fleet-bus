"""Unit tests for the control-plane frame — the shared ``kind="control"`` op (#59)."""

from __future__ import annotations

import pytest

from fleet_nats import (
    CONTROL_KIND,
    OP_PAUSE,
    OP_RESUME,
    ControlOp,
    Envelope,
    control_envelope,
    is_control_envelope,
    parse_control,
    publish_control,
    subjects,
)


def test_control_envelope_resume_shape() -> None:
    env = control_envelope(msg_id="m1", ts="t", sender="rico", task_id="task-1")
    assert env.kind == CONTROL_KIND == "control"
    assert env.task_id == "task-1"
    assert env.sender == "rico"
    assert env.payload == {"op": "resume", "task_id": "task-1"}


def test_control_envelope_defaults_to_resume() -> None:
    env = control_envelope(msg_id="m1", ts="t", sender="rico", task_id="task-1")
    assert env.payload["op"] == OP_RESUME


def test_control_envelope_carries_bounded_budget_add() -> None:
    env = control_envelope(msg_id="m1", ts="t", sender="rico", task_id="task-1", budget_msgs_add=50)
    assert env.payload["budget_msgs_add"] == 50


def test_control_envelope_pause_op() -> None:
    env = control_envelope(msg_id="m1", ts="t", sender="rico", task_id="task-1", op=OP_PAUSE)
    assert env.payload["op"] == OP_PAUSE


def test_control_envelope_rejects_unknown_op() -> None:
    with pytest.raises(ValueError, match="unknown op"):
        control_envelope(msg_id="m1", ts="t", sender="rico", task_id="task-1", op="detonate")


def test_control_envelope_rejects_non_positive_budget_add() -> None:
    with pytest.raises(ValueError, match="must be positive"):
        control_envelope(msg_id="m1", ts="t", sender="rico", task_id="task-1", budget_msgs_add=0)


def test_control_frame_survives_the_wire() -> None:
    # The contract parse_control relies on must round-trip through to/from_bytes.
    env = control_envelope(msg_id="m1", ts="t", sender="rico", task_id="task-1", budget_msgs_add=25)
    back = Envelope.from_bytes(env.to_bytes())
    assert is_control_envelope(back)
    op = parse_control(back)
    assert op == ControlOp(op="resume", task_id="task-1", budget_msgs_add=25)


def test_parse_control_resume_without_bump() -> None:
    env = control_envelope(msg_id="m1", ts="t", sender="rico", task_id="task-1")
    op = parse_control(env)
    assert op is not None
    assert (op.op, op.task_id, op.budget_msgs_add) == ("resume", "task-1", None)


def test_parse_control_none_for_non_control_kind() -> None:
    env = Envelope.new(msg_id="m1", ts="t", sender="rico", kind="status", task_id="task-1")
    assert parse_control(env) is None


def test_parse_control_none_for_unknown_op() -> None:
    # A hand-rolled control frame naming an op outside the closed set is dropped.
    env = Envelope.new(
        msg_id="m1",
        ts="t",
        sender="rico",
        kind=CONTROL_KIND,
        task_id="task-1",
        payload={"op": "explode", "task_id": "task-1"},
    )
    assert parse_control(env) is None


def test_parse_control_none_for_missing_op() -> None:
    env = Envelope.new(
        msg_id="m1", ts="t", sender="rico", kind=CONTROL_KIND, task_id="task-1", payload={}
    )
    assert parse_control(env) is None


def test_parse_control_none_when_no_task_id_anywhere() -> None:
    # No payload task_id and no envelope task_id — nothing to target, dropped.
    env = Envelope.new(
        msg_id="m1", ts="t", sender="rico", kind=CONTROL_KIND, payload={"op": "resume"}
    )
    assert parse_control(env) is None


def test_parse_control_falls_back_to_envelope_task_id() -> None:
    # Payload omits task_id but the envelope field carries it (subject-scoped).
    env = Envelope.new(
        msg_id="m1",
        ts="t",
        sender="rico",
        kind=CONTROL_KIND,
        task_id="task-1",
        payload={"op": "resume"},
    )
    op = parse_control(env)
    assert op is not None
    assert op.task_id == "task-1"


def test_parse_control_drops_malformed_budget_but_keeps_the_resume() -> None:
    # A junk bump must never sink the resume that unblocks the lane.
    env = Envelope.new(
        msg_id="m1",
        ts="t",
        sender="rico",
        kind=CONTROL_KIND,
        task_id="task-1",
        payload={"op": "resume", "task_id": "task-1", "budget_msgs_add": "lots"},
    )
    op = parse_control(env)
    assert op is not None
    assert op.op == "resume"
    assert op.budget_msgs_add is None  # dropped, not fatal


def test_parse_control_drops_non_positive_budget() -> None:
    env = Envelope.new(
        msg_id="m1",
        ts="t",
        sender="rico",
        kind=CONTROL_KIND,
        task_id="task-1",
        payload={"op": "resume", "task_id": "task-1", "budget_msgs_add": -5},
    )
    op = parse_control(env)
    assert op is not None and op.budget_msgs_add is None


class _Bus:
    """Records publishes for the publisher-helper test."""

    def __init__(self) -> None:
        self.published: list[tuple[str, Envelope]] = []

    async def publish(self, subject: str, envelope: Envelope) -> None:
        self.published.append((subject, envelope))


async def test_publish_control_targets_the_hub_control_lane() -> None:
    bus = _Bus()
    env = await publish_control(
        bus,
        env="dev",
        task_id="task-1",
        sender="rico",
        budget_msgs_add=10,
        msg_id="m1",
        ts="t",
    )
    assert len(bus.published) == 1
    subject, published = bus.published[0]
    assert subject == subjects.hub_control("dev", "task-1")
    assert published is env
    assert parse_control(published) == ControlOp("resume", "task-1", 10)


async def test_publish_control_raises_before_publishing_on_bad_op() -> None:
    bus = _Bus()
    with pytest.raises(ValueError, match="unknown op"):
        await publish_control(
            bus, env="dev", task_id="task-1", sender="rico", op="nuke", msg_id="m1", ts="t"
        )
    assert bus.published == []  # nothing hit the wire


# --- reason: the audited WHY (#65 review) --------------------------------------


def test_control_envelope_carries_reason_when_given() -> None:
    env = control_envelope(msg_id="m1", ts="t", sender="rico", task_id="task-1", reason="per Rico")
    assert env.payload["reason"] == "per Rico"


def test_control_envelope_omits_reason_when_absent() -> None:
    # A reason-less frame is byte-identical to a pre-reason frame (backward compat).
    env = control_envelope(msg_id="m1", ts="t", sender="rico", task_id="task-1")
    assert "reason" not in env.payload
    assert env.payload == {"op": "resume", "task_id": "task-1"}


def test_control_envelope_omits_blank_reason() -> None:
    # A whitespace-only reason adds no audit value — dropped, not carried blank.
    env = control_envelope(msg_id="m1", ts="t", sender="rico", task_id="task-1", reason="   ")
    assert "reason" not in env.payload


def test_control_envelope_truncates_reason_to_cap() -> None:
    from fleet_nats.control import MAX_REASON_LEN

    env = control_envelope(
        msg_id="m1", ts="t", sender="rico", task_id="task-1", reason="x" * (MAX_REASON_LEN + 50)
    )
    assert len(env.payload["reason"]) == MAX_REASON_LEN


def test_reason_round_trips_via_parse_control() -> None:
    env = control_envelope(
        msg_id="m1", ts="t", sender="rico", task_id="task-1", budget_msgs_add=4, reason="per Rico"
    )
    back = Envelope.from_bytes(env.to_bytes())
    op = parse_control(back)
    assert op == ControlOp(op="resume", task_id="task-1", budget_msgs_add=4, reason="per Rico")


def test_parse_control_reason_none_when_absent() -> None:
    # A frame with no reason parses with reason=None — full backward compat with
    # existing hub/harness callers that never set the field.
    env = control_envelope(msg_id="m1", ts="t", sender="rico", task_id="task-1")
    op = parse_control(env)
    assert op is not None and op.reason is None


def test_parse_control_drops_non_string_reason() -> None:
    # A bogus reason must never fail the op — surfaces as None, resume still lands.
    env = Envelope.new(
        msg_id="m1",
        ts="t",
        sender="rico",
        kind=CONTROL_KIND,
        task_id="task-1",
        payload={"op": "resume", "task_id": "task-1", "reason": 123},
    )
    op = parse_control(env)
    assert op is not None and op.reason is None
    assert op.op == "resume"


def test_parse_control_recaps_oversized_reason_on_read() -> None:
    from fleet_nats.control import MAX_REASON_LEN

    # An oversized reason from any publisher (not just this builder) is re-capped
    # on read, so the parsed op can never balloon.
    env = Envelope.new(
        msg_id="m1",
        ts="t",
        sender="rico",
        kind=CONTROL_KIND,
        task_id="task-1",
        payload={"op": "resume", "task_id": "task-1", "reason": "y" * (MAX_REASON_LEN + 10)},
    )
    op = parse_control(env)
    assert op is not None and op.reason is not None and len(op.reason) == MAX_REASON_LEN


async def test_publish_control_threads_reason() -> None:
    bus = _Bus()
    await publish_control(
        bus,
        env="dev",
        task_id="task-1",
        sender="rico",
        reason="per Rico",
        msg_id="m1",
        ts="t",
    )
    _, published = bus.published[0]
    assert parse_control(published) == ControlOp("resume", "task-1", None, "per Rico")


# --- publish_control boundary logging (scout p4-logging-langfuse-scout.md §1) ---
#
# publish_control was the ONE control-plane edge with no entry/exit/failure log
# and no downstream coverage. The lines log the COORDINATION shape only —
# subject + op + task_id — never the payload (reason prose, sender identity).


class _ExplodingBus:
    """A bus whose publish always raises — exercises the failure log branch."""

    async def publish(self, subject: str, envelope: Envelope) -> None:
        raise ConnectionError("bus down")


async def test_publish_control_logs_entry_and_success() -> None:
    from loguru import logger

    lines: list[str] = []
    handle = logger.add(lines.append, level="DEBUG", format="{message}")
    try:
        await publish_control(
            bus=_Bus(),
            env="dev",
            task_id="task-1",
            sender="rico",
            op=OP_RESUME,
            reason="secret-why-must-not-leak",
            msg_id="m1",
            ts="t",
        )
    finally:
        logger.remove(handle)
    joined = "\n".join(lines)
    # entry + exit boundary lines present, carrying subject + op + task_id
    assert "control.publish" in joined
    assert "published" in joined  # the success/exit line
    assert "op=resume" in joined
    assert subjects.hub_control("dev", "task-1") in joined
    # the audited reason prose must NEVER reach the log
    assert "secret-why-must-not-leak" not in joined


async def test_publish_control_logs_failure_and_reraises() -> None:
    from loguru import logger

    lines: list[str] = []
    handle = logger.add(lines.append, level="DEBUG", format="{message}")
    try:
        with pytest.raises(ConnectionError):
            await publish_control(
                bus=_ExplodingBus(),
                env="dev",
                task_id="task-1",
                sender="rico",
                msg_id="m1",
                ts="t",
            )
    finally:
        logger.remove(handle)
    joined = "\n".join(lines)
    assert "control.publish" in joined
    assert "failed" in joined
    assert "op=resume" in joined
