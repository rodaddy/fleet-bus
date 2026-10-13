"""Token-cost consumer — the per-task spend the governor budgets against (#27).

Consumes the shared :mod:`fleet_nats.tokencost` wire shape from the
``token_cost`` wildcard lane (emitted per turn by the agent runtime's post-turn hook) and answers
the one question Phase 5's governor (#13) asks: how many tokens has this task
spent so far. Pure and deterministic — the ledger reads no clock, keys nothing
by wall time, and never carries what a turn read or wrote, only its size.

:func:`sum_task_cost` is the stateless seam: given already-parsed events, sum a
task's total. :class:`CostLedger` is the streaming face of the same math —
:meth:`~CostLedger.observe` parses one envelope (logging and skipping a malformed
one exactly like :class:`~fleet_orchestrator.liveness.LivenessTracker`), and the
totals accumulate as events arrive.

**Duplicates mean dedup.** The transport is at-most-once today (core NATS, no
JetStream anywhere — a message published while a subscriber is down is LOST, no
backlog), so redelivery is NOT a transport property. Duplicates still arise
APP-side (#25, C5): a reconnect re-send or an emitter retry can publish the same
turn's ``token_cost`` twice. A blind sum would inflate a task's spend and make
the #13 governor pause on phantom cost — so ``turn_id`` exists in the schema
precisely to make spend idempotent. :class:`CostLedger` dedups built-in on the
``(subjects.slug(agent_id), task_id, turn_id)`` idempotency key (agent id
slug-normalized to match the routing lanes) with a bounded LRU; a duplicate turn
is a logged no-op and is summed exactly once. See
``docs/contracts/C5-ordering-retry-semantics.md``.
"""

from __future__ import annotations

from collections import OrderedDict
from collections.abc import Iterable
from typing import Any

from fleet_nats import Envelope, TokenCost, parse_token_cost, subjects
from fleet_nats.tracing import traced
from loguru import logger

# Turns remembered for dedup; the oldest evicts first. A window this size covers
# far more in-flight turns than any reconnect re-send / emitter-retry burst
# duplicates, so a real duplicate never ages out before its original is still
# summable.
_DEFAULT_DEDUP_WINDOW = 4096

# Placeholder agent id stamped on dedup keys re-seeded by restore_task (#106).
# The original agent id is not carried in the durable snapshot — dedup only needs
# to recognise a (task, turn) pair as already-summed, so a fixed placeholder is
# sufficient and cannot collide with a live slugged agent id (leading marker).
_RESTORED_AGENT = "\x00restored"


def _require_snapshot_int(field: str, value: Any) -> int:
    """Return an exact non-bool integer snapshot field."""
    if type(value) is not int:
        raise TypeError(f"restore_task: {field} must be an int, got {type(value).__name__}")
    return value


def _restore_seen_entry(entry: Any) -> tuple[str, str]:
    """Validate one durable seen-turn entry before mutating the ledger."""
    if isinstance(entry, list) and len(entry) == 2:
        agent_key, turn_id = entry
        if not isinstance(agent_key, str) or not isinstance(turn_id, str):
            raise TypeError("restore_task: seen_turns pair entries must be strings")
        return agent_key, turn_id
    if isinstance(entry, str):
        return _RESTORED_AGENT, entry
    raise TypeError(
        "restore_task: seen_turns entries must be [agent_key, turn_id] string pairs "
        "or legacy bare turn_id strings"
    )


def sum_task_cost(events: Iterable[TokenCost], task_id: str) -> int:
    """Sum ``total_tokens`` across every event belonging to ``task_id``.

    The CALLER owns dedup of its event list: this is a stateless sum, so a
    duplicate ``turn_id`` present twice in ``events`` is counted twice.
    Deduplicate on ``(agent_id, task_id, turn_id)`` before summing — the caller
    owns id normalization here, so raw ``agent_id`` is fine; :class:`CostLedger`
    (which dedups built-in) additionally slug-normalizes ``agent_id`` to match the
    routing lanes.

    Args:
        events: Already-parsed token-cost events (from the feed).
        task_id: The k-board task to total.

    Returns:
        The task's total token spend — ``0`` when no event matches (an unseen
        task has spent nothing, which is a real answer, not an error).
    """
    return sum(event.total_tokens for event in events if event.task_id == task_id)


class CostLedger:
    """Accumulates per-task / per-agent token spend from observed events.

    Deterministic by construction: no clock, no randomness — the running totals
    depend only on the events fed in, so the governor's budget check is provable
    in isolation. Idempotent against app-level duplicates (emitter retry /
    reconnect re-send — the transport is at-most-once today, C5): a duplicate turn
    (same ``(subjects.slug(agent_id), task_id, turn_id)`` idempotency key — agent
    id slug-normalized to match the routing lanes) is summed exactly once, tracked
    by a bounded LRU so memory stays flat under load.

    Args:
        dedup_window: Maximum turns remembered for dedup; the oldest evicts
            first. A duplicate that slips past the window is reconciled at the
            record — k-board is the source of truth, keyed by ``task_id``.
    """

    def __init__(self, *, dedup_window: int = _DEFAULT_DEDUP_WINDOW) -> None:
        if dedup_window <= 0:
            raise ValueError(f"CostLedger dedup_window must be positive, got {dedup_window}")
        self._window = dedup_window
        self._by_task: dict[str, int] = {}
        self._by_agent: dict[str, int] = {}
        # Keyed on (slug(agent_id), task_id, turn_id): the tuple that makes one
        # turn's spend idempotent (agent_id slug-normalized at insert, see below).
        # Value is unused — an OrderedDict is the LRU set.
        self._seen: OrderedDict[tuple[str, str, str], None] = OrderedDict()

    @traced
    def observe(self, envelope: Envelope) -> TokenCost | None:
        """Record one received token-cost envelope.

        Args:
            envelope: The received envelope from the token-cost lane.

        Returns:
            The parsed :class:`~fleet_nats.TokenCost` on a first-seen turn, or
            ``None`` when the envelope is malformed (logged and skipped) OR is a
            duplicate turn already summed. A duplicate turn (app-level retry /
            reconnect re-send) parses fine but must NOT be summed twice, so it is
            a logged no-op returning ``None`` — the same "parsed but not counted"
            contract a malformed event returns, distinguished only by the log.
        """
        try:
            cost = parse_token_cost(envelope)
        except ValueError as exc:
            logger.warning(
                "orchestrator.costfeed | malformed token_cost sender={} dropped: {}",
                envelope.sender,
                exc,
            )
            return None
        # Key the agent by the SAME normalization the routing lanes use — the
        # LivenessTracker precedent: a raw-id key would diverge from the slugged
        # subject identity, and a #14 dashboard joining liveness and cost by
        # agent would read 0 for any id the lane normalizes.
        agent_key = subjects.slug(cost.agent_id)
        key = (agent_key, cost.task_id, cost.turn_id)
        if key in self._seen:
            # Refresh to most-recent (true LRU) so a hot duplicate can't age out
            # while it is still arriving, then no-op — never sum a turn twice.
            self._seen.move_to_end(key)
            logger.info(
                "orchestrator.costfeed | duplicate token_cost agent={} task={} turn={} — no-op",
                cost.agent_id,
                cost.task_id,
                cost.turn_id,
            )
            return None
        self._seen[key] = None
        if len(self._seen) > self._window:
            self._seen.popitem(last=False)
        self._by_task[cost.task_id] = self._by_task.get(cost.task_id, 0) + cost.total_tokens
        self._by_agent[agent_key] = self._by_agent.get(agent_key, 0) + cost.total_tokens
        logger.debug(
            "orchestrator.costfeed | cost agent={} task={} turn={} total={}",
            cost.agent_id,
            cost.task_id,
            cost.turn_id,
            cost.total_tokens,
        )
        return cost

    def task_total(self, task_id: str) -> int:
        """Total token spend accumulated for ``task_id`` (``0`` if unseen)."""
        return self._by_task.get(task_id, 0)

    def task_snapshot(self, task_id: str) -> dict[str, Any]:
        """Serialize the per-task spend + dedup keys for durable resume (#106).

        The governor persists this alongside its message counters so a crash near
        the token cap does NOT reset spend to zero (which would let a runaway task
        spend a whole second budget before the cap re-tripped). Scoped to ONE
        ``task_id``: only that task's total and its already-summed dedup turn keys
        travel, so the snapshot stays bounded by the task's own turn count, never
        the full LRU window.

        The dedup keys are the ``(agent_key, turn_id)`` pairs already counted for
        this task; on :meth:`restore_task` they rebuild the EXACT dedup tuple the
        live :meth:`observe` path forms (``(agent_key, task_id, turn_id)``), so a
        redelivered ``token_cost`` for a turn summed before the crash is a no-op
        after resume — the at-least-once redelivery guard survives the restart. The
        agent id is carried (not dropped) precisely so the restored key matches a
        live redelivery, whose key is agent-scoped.

        Returns:
            ``{"tokens": <int>, "seen_turns": [[<agent_key>, <turn_id>], ...]}`` —
            JSON-able. A task with no observed spend yields
            ``{"tokens": 0, "seen_turns": []}``.
        """
        seen_turns = sorted(
            [agent_key, turn_id]
            for (agent_key, seen_task, turn_id) in self._seen
            if seen_task == task_id
        )
        return {"tokens": self._by_task.get(task_id, 0), "seen_turns": seen_turns}

    def restore_task(self, task_id: str, snapshot: dict[str, Any]) -> None:
        """Re-seed a task's spend + dedup keys from :meth:`task_snapshot` (#106).

        Idempotent and additive-safe: the restored total REPLACES the in-memory
        per-task total (a fresh ledger starts at zero, so this seeds it), and each
        dedup key is rebuilt as the exact ``(agent_key, task_id, turn_id)`` tuple
        the live :meth:`observe` path forms — so a post-restart redelivery of an
        already-counted turn is recognised and NOT double-counted. A genuinely new
        turn carries a different ``turn_id`` and is counted normally.

        Backward-tolerant: a legacy snapshot whose ``seen_turns`` are bare
        ``turn_id`` strings (pre-agent-key shape) is accepted with a placeholder
        agent — the spend total still resumes; only that legacy shape's dedup is
        agent-blind, which is strictly better than losing the guard.

        Args:
            task_id: The task whose spend is being resumed.
            snapshot: A dict from :meth:`task_snapshot`.
        """
        if not isinstance(snapshot, dict):
            raise TypeError(f"restore_task: snapshot must be a dict, got {type(snapshot).__name__}")
        tokens = _require_snapshot_int("tokens", snapshot.get("tokens", 0))
        if tokens < 0:
            raise ValueError(f"restore_task: tokens must be >= 0, got {tokens}")
        seen_turns = snapshot.get("seen_turns", [])
        if not isinstance(seen_turns, list):
            raise TypeError(
                f"restore_task: seen_turns must be a list, got {type(seen_turns).__name__}"
            )
        restored_seen = [_restore_seen_entry(entry) for entry in seen_turns]

        self._by_task[task_id] = tokens
        for agent_key, turn_id in restored_seen:
            key = (agent_key, task_id, turn_id)
            if key not in self._seen:
                self._seen[key] = None
        # Keep the LRU bounded even after a bulk restore.
        while len(self._seen) > self._window:
            self._seen.popitem(last=False)
        logger.info(
            "orchestrator.costfeed | restored task={} tokens={} seen_turns={}",
            task_id,
            tokens,
            len(seen_turns),
        )

    def agent_total(self, agent_id: str) -> int:
        """Total spend for ``agent_id`` (slug-normalized; ``0`` if unseen)."""
        return self._by_agent.get(subjects.slug(agent_id), 0)
