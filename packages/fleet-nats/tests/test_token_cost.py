"""Wire-shape tests for the shared token-cost contract (#27)."""

from __future__ import annotations

import pytest

from fleet_nats import Envelope, parse_token_cost, subjects, token_cost_envelope


def _cost() -> Envelope:
    return token_cost_envelope(
        msg_id="c1",
        ts="2026-07-08T00:00:00Z",
        sender="finch",
        task_id="task-42",
        turn_id="turn-3",
        prompt_tokens=120,
        completion_tokens=80,
        total_tokens=200,
        model="claude-opus-4-8",
    )


def test_wire_round_trip() -> None:
    """builder -> bytes -> Envelope -> parse yields the identical event."""
    env = Envelope.from_bytes(_cost().to_bytes())
    tc = parse_token_cost(env)
    assert tc.agent_id == "finch"
    assert tc.task_id == "task-42"
    assert tc.turn_id == "turn-3"
    assert tc.prompt_tokens == 120
    assert tc.completion_tokens == 80
    assert tc.total_tokens == 200
    assert tc.model == "claude-opus-4-8"
    assert tc.ts == "2026-07-08T00:00:00Z"


def test_zero_token_turn_is_valid() -> None:
    """A turn that spends nothing is a real, valid event — not a malformed one."""
    env = token_cost_envelope(
        msg_id="c0",
        ts="t",
        sender="wren",
        task_id="task-1",
        turn_id="turn-0",
        prompt_tokens=0,
        completion_tokens=0,
        total_tokens=0,
        model="claude-haiku",
    )
    tc = parse_token_cost(env)
    assert tc.total_tokens == 0


def test_wrong_kind_rejected() -> None:
    env = Envelope.new(msg_id="m", ts="t", sender="x", kind="status", payload={})
    with pytest.raises(ValueError, match="not a token_cost"):
        parse_token_cost(env)


@pytest.mark.parametrize(
    "corrupt",
    [
        {"agent_id": ""},
        {"agent_id": 3},
        {"task_id": ""},
        {"task_id": None},
        {"turn_id": ""},
        {"model": ""},
        {"model": 7},
        {"prompt_tokens": -1},
        {"completion_tokens": "80"},
        # bool is an int subclass — a stray True must not be counted as a token.
        {"prompt_tokens": True},
        {"total_tokens": -5},
        # Inconsistent accounting: total != prompt + completion must fail loud.
        {"total_tokens": 999},
        # Identity split: payload agent_id disagreeing with envelope.sender is
        # emitter drift or forgery — never credit spend to the wrong agent.
        {"agent_id": "impostor"},
    ],
)
def test_malformed_payload_fails_loud(corrupt: dict[str, object]) -> None:
    """Every malformed field raises — a bad event never sums into a budget."""
    env = _cost()
    bad = Envelope.new(
        msg_id="m",
        ts="t",
        sender="finch",
        kind="token_cost",
        payload={**env.payload, **corrupt},
    )
    with pytest.raises(ValueError):
        parse_token_cost(bad)


def test_wildcard_subject_matches_per_agent_form() -> None:
    """The consumer wildcard and the emitter subject agree token-for-token."""
    concrete = subjects.token_cost("dev", "finch")
    wild = subjects.token_cost_wildcard("dev")
    assert concrete.split(".")[:-1] == wild.split(".")[:-1]
    assert wild.endswith(".*")
    assert concrete == "dev.agent.token_cost.finch"
