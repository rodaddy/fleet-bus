"""Tests for token-cost consumption and per-task summing (#27)."""

from __future__ import annotations

import pytest
from fleet_nats import Envelope, parse_token_cost, token_cost_envelope

from fleet_orchestrator.costfeed import CostLedger, sum_task_cost


def _cost(
    *,
    agent_id: str = "finch",
    task_id: str = "task-42",
    turn_id: str = "turn-1",
    prompt_tokens: int = 100,
    completion_tokens: int = 50,
) -> Envelope:
    return token_cost_envelope(
        msg_id=f"c-{turn_id}",
        ts="2026-07-08T00:00:00Z",
        sender=agent_id,
        task_id=task_id,
        turn_id=turn_id,
        prompt_tokens=prompt_tokens,
        completion_tokens=completion_tokens,
        total_tokens=prompt_tokens + completion_tokens,
        model="claude-opus-4-8",
    )


def test_costfeed_two_turns_same_task_accumulate() -> None:
    ledger = CostLedger()
    assert ledger.observe(_cost(turn_id="turn-1", prompt_tokens=100, completion_tokens=50))
    assert ledger.observe(_cost(turn_id="turn-2", prompt_tokens=80, completion_tokens=20))

    assert ledger.task_total("task-42") == 250
    assert ledger.agent_total("finch") == 250


def test_costfeed_other_tasks_excluded_from_total() -> None:
    ledger = CostLedger()
    ledger.observe(_cost(task_id="task-42", turn_id="a", prompt_tokens=100, completion_tokens=0))
    ledger.observe(_cost(task_id="task-99", turn_id="b", prompt_tokens=30, completion_tokens=0))

    assert ledger.task_total("task-42") == 100
    assert ledger.task_total("task-99") == 30
    # An unseen task has spent nothing — a real answer, not an error.
    assert ledger.task_total("task-unseen") == 0


def test_costfeed_malformed_event_logged_and_skipped() -> None:
    ledger = CostLedger()
    malformed = Envelope.new(msg_id="bad", ts="t", sender="finch", kind="status", payload={})

    assert ledger.observe(malformed) is None
    assert ledger.task_total("task-42") == 0
    assert ledger.agent_total("finch") == 0


def test_costfeed_duplicate_turn_is_summed_once() -> None:
    # App-level duplicate (transport is at-most-once, C5): the SAME turn observed
    # twice (reconnect re-send / emitter retry) must sum exactly once, or the #13
    # governor pauses on phantom spend.
    ledger = CostLedger()
    env = _cost(turn_id="turn-1", prompt_tokens=100, completion_tokens=50)

    assert ledger.observe(env) is not None  # first sight: parsed + summed
    assert ledger.observe(env) is None  # duplicate: parsed-fine no-op, not summed
    assert ledger.task_total("task-42") == 150
    assert ledger.agent_total("finch") == 150


def test_costfeed_distinct_turns_same_task_are_not_deduped() -> None:
    # Dedup keys on (slug(agent_id), task_id, turn_id) — distinct turns of one
    # task are real, separate spend and both accumulate.
    ledger = CostLedger()
    assert ledger.observe(_cost(turn_id="turn-1", prompt_tokens=100, completion_tokens=0))
    assert ledger.observe(_cost(turn_id="turn-2", prompt_tokens=40, completion_tokens=0))

    assert ledger.task_total("task-42") == 140


def test_costfeed_dedup_window_bound_holds() -> None:
    # The seen-set is a bounded LRU: past the window the oldest turn evicts, so a
    # duplicate of an evicted turn is treated as new (the k-board record is the
    # documented backstop for that rare double-count).
    ledger = CostLedger(dedup_window=2)
    ledger.observe(_cost(turn_id="a", prompt_tokens=1, completion_tokens=0))
    ledger.observe(_cost(turn_id="b", prompt_tokens=1, completion_tokens=0))
    ledger.observe(_cost(turn_id="c", prompt_tokens=1, completion_tokens=0))  # evicts "a"

    # "a" aged out of the window → re-observed as new, summed again.
    assert ledger.observe(_cost(turn_id="a", prompt_tokens=1, completion_tokens=0)) is not None
    # "c" is still in the window → deduped.
    assert ledger.observe(_cost(turn_id="c", prompt_tokens=1, completion_tokens=0)) is None
    assert ledger.task_total("task-42") == 4


def test_costfeed_dedup_window_must_be_positive() -> None:
    with pytest.raises(ValueError, match="dedup_window must be positive"):
        CostLedger(dedup_window=0)


def test_sum_task_cost_from_parsed_feed_events_alone() -> None:
    # sum_task_cost is the stateless seam: parse the feed, then sum by task with
    # no ledger — the exact math the governor (#13) budgets against.
    raw = [
        _cost(task_id="task-42", turn_id="t1", prompt_tokens=100, completion_tokens=50),
        _cost(task_id="task-42", turn_id="t2", prompt_tokens=10, completion_tokens=5),
        _cost(task_id="task-99", turn_id="t3", prompt_tokens=200, completion_tokens=0),
    ]
    events = [parse_token_cost(env) for env in raw]
    assert sum_task_cost(events, "task-42") == 165
    assert sum_task_cost(events, "task-99") == 200
    assert sum_task_cost(events, "task-unseen") == 0


def test_ledger_keys_agents_by_subject_slug_normalization() -> None:
    # The LivenessTracker precedent: a dashboard joining liveness and cost by
    # agent id must see one identity — the slugged one the lanes route by.
    ledger = CostLedger()
    env = token_cost_envelope(
        msg_id="tc-slug",
        ts="2026-07-08T00:00:00Z",
        sender="Foo Bar",
        task_id="t-9",
        turn_id="turn-1",
        prompt_tokens=10,
        completion_tokens=5,
        total_tokens=15,
        model="claude-fable-5",
    )
    assert ledger.observe(env) is not None
    assert ledger.agent_total("foo-bar") == 15
    assert ledger.agent_total("Foo Bar") == 15


# ---- Durable per-task snapshot / restore (fleet-bus #106) ------------------


def test_task_snapshot_serializes_spend_and_seen_turns() -> None:
    ledger = CostLedger()
    ledger.observe(
        _cost(task_id="task-42", turn_id="turn-1", prompt_tokens=100, completion_tokens=50)
    )
    ledger.observe(
        _cost(task_id="task-42", turn_id="turn-2", prompt_tokens=80, completion_tokens=20)
    )
    ledger.observe(_cost(task_id="other", turn_id="turn-x", prompt_tokens=10, completion_tokens=5))

    snap = ledger.task_snapshot("task-42")
    assert snap["tokens"] == 250
    # (agent_key, turn_id) pairs for THIS task only — carries the agent so a
    # restored dedup key matches a live agent-scoped redelivery.
    assert snap["seen_turns"] == [["finch", "turn-1"], ["finch", "turn-2"]]


def test_task_snapshot_empty_task_is_wellformed() -> None:
    assert CostLedger().task_snapshot("never-seen") == {"tokens": 0, "seen_turns": []}


def test_restore_task_resumes_spend_across_a_simulated_restart() -> None:
    # A hub accumulates spend, "crashes", and a FRESH ledger restores — the token
    # total resumes instead of resetting to zero (crash-near-cap does not hand the
    # task a whole second token budget).
    crashed = CostLedger()
    crashed.observe(
        _cost(task_id="task-42", turn_id="turn-1", prompt_tokens=600, completion_tokens=300)
    )
    snap = crashed.task_snapshot("task-42")
    assert snap["tokens"] == 900

    restarted = CostLedger()
    restarted.restore_task("task-42", snap)
    assert restarted.task_total("task-42") == 900


def test_restore_task_seeds_dedup_so_redelivery_after_restart_is_not_double_counted() -> None:
    # THE redelivery guard survives a restart: a turn summed BEFORE the crash must
    # not be summed AGAIN when the bus redelivers it after resume.
    crashed = CostLedger()
    crashed.observe(
        _cost(task_id="task-42", turn_id="turn-1", prompt_tokens=600, completion_tokens=300)
    )
    snap = crashed.task_snapshot("task-42")

    restarted = CostLedger()
    restarted.restore_task("task-42", snap)
    # The bus redelivers turn-1 after the restart — it must be a no-op, not +900.
    assert (
        restarted.observe(
            _cost(task_id="task-42", turn_id="turn-1", prompt_tokens=600, completion_tokens=300)
        )
        is None
    )
    assert restarted.task_total("task-42") == 900  # NOT doubled to 1800
    # A genuinely new turn still counts normally.
    assert (
        restarted.observe(
            _cost(task_id="task-42", turn_id="turn-2", prompt_tokens=100, completion_tokens=0)
        )
        is not None
    )
    assert restarted.task_total("task-42") == 1000


def test_restore_task_rejects_negative_tokens() -> None:
    with pytest.raises(ValueError, match="tokens must be >= 0"):
        CostLedger().restore_task("task-42", {"tokens": -1})


@pytest.mark.parametrize("tokens", ["900", True, 1.5])
def test_restore_task_rejects_wrong_typed_tokens_without_partial_mutation(tokens: object) -> None:
    ledger = CostLedger()
    ledger.restore_task("task-42", {"tokens": 100, "seen_turns": [["finch", "turn-1"]]})

    with pytest.raises(TypeError, match="tokens must be an int"):
        ledger.restore_task("task-42", {"tokens": tokens, "seen_turns": [["finch", "turn-2"]]})

    assert ledger.task_total("task-42") == 100
    assert ledger.task_snapshot("task-42")["seen_turns"] == [["finch", "turn-1"]]


@pytest.mark.parametrize(
    "seen_turns",
    [
        [[123, "turn-1"]],
        [["finch", 123]],
        [["finch"]],
        [["finch", "turn-1", "extra"]],
        [123],
        [True],
    ],
)
def test_restore_task_rejects_malformed_seen_turns_without_partial_mutation(
    seen_turns: object,
) -> None:
    ledger = CostLedger()
    ledger.restore_task("task-42", {"tokens": 100, "seen_turns": [["finch", "turn-1"]]})

    with pytest.raises(TypeError, match="seen_turns"):
        ledger.restore_task("task-42", {"tokens": 200, "seen_turns": seen_turns})

    assert ledger.task_total("task-42") == 100
    assert ledger.task_snapshot("task-42")["seen_turns"] == [["finch", "turn-1"]]


def test_restore_task_preserves_legacy_bare_turn_id_strings() -> None:
    ledger = CostLedger()
    ledger.restore_task("task-42", {"tokens": 900, "seen_turns": ["turn-1"]})

    assert ledger.task_total("task-42") == 900
    assert ledger.task_snapshot("task-42")["seen_turns"] == [["\x00restored", "turn-1"]]
