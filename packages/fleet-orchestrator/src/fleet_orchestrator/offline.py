"""Agent-offline dispatch handling (#25, Phase 3.4).

A dispatch to a worker that never answers must not be lost — and must not
silently wait in JetStream either (JS is invisible to agents; the k-board is
where a blocked task is durable and visible). The contract:

    silence past ``FLEET_DISPATCH_TIMEOUT_SECONDS``
        → WAKE the worker
    still silent one more window            → REDISPATCH once (fires at most once
                                              per window; the redispatch itself is
                                              NON-idempotent — fresh Envelope.id,
                                              a deliberate re-run, C5)
    still silent one more window            → BLOCKED: k-board event + log
                                              (narration arrives with #13)

The Phase-2 heartbeat cross-check: when the worker's heartbeat is already
STALE at a rung, waking a dead agent is a wasted window — the ladder skips
straight to the next rung, with the staleness recorded in the reason.

Deterministic by construction: the detector never reads a clock — callers
supply ``now`` as monotonic seconds (no ``time``/``datetime`` imports here),
and the escalation ladder is a pure case-progression per watched task.
"""

from __future__ import annotations

import math
import os
from collections.abc import Callable
from dataclasses import dataclass
from enum import StrEnum

from loguru import logger

_ENV_TIMEOUT = "FLEET_DISPATCH_TIMEOUT_SECONDS"
_DEFAULT_TIMEOUT_S = 300.0


def timeout_from_env() -> float:
    """Read ``FLEET_DISPATCH_TIMEOUT_SECONDS`` (default 300). Fail loud on junk.

    Raises:
        ValueError: If set non-numeric, non-finite, or non-positive — a typo'd
            timeout must not silently become the default.
    """
    raw = os.environ.get(_ENV_TIMEOUT)
    if raw is None:
        return _DEFAULT_TIMEOUT_S
    try:
        value = float(raw)
    except ValueError as exc:
        raise ValueError(f"{_ENV_TIMEOUT} must be a number, got {raw!r}") from exc
    if not math.isfinite(value):
        raise ValueError(f"{_ENV_TIMEOUT} must be finite, got {value}")
    if value <= 0:
        raise ValueError(f"{_ENV_TIMEOUT} must be positive, got {value}")
    return value


class Escalation(StrEnum):
    """The rung the offline ladder fires for a silent dispatch."""

    WAKE = "wake"
    REDISPATCH = "redispatch"
    BLOCKED = "blocked"


@dataclass(frozen=True)
class OfflineDecision:
    """One escalation decision for one watched task.

    Attributes:
        task_id: The silent task.
        worker: The unresponsive worker.
        escalation: Which rung fired.
        reason: Log/board-readable why (includes heartbeat staleness when it
            short-circuited a rung). Coordination metadata only.
    """

    task_id: str
    worker: str
    escalation: Escalation
    reason: str


@dataclass
class _Watch:
    """Mutable per-task watch state (internal)."""

    worker: str
    last_signal: float  # dispatch or most recent worker status, monotonic s
    woke: bool = False
    redispatched: bool = False


class OfflineDetector:
    """Watches dispatched tasks for silence and walks the escalation ladder.

    Args:
        timeout_s: Silence window per rung (``FLEET_DISPATCH_TIMEOUT_SECONDS``).
    """

    def __init__(self, *, timeout_s: float) -> None:
        if not timeout_s > 0:
            raise ValueError(f"OfflineDetector timeout_s must be positive, got {timeout_s}")
        self._timeout_s = timeout_s
        self._watches: dict[str, _Watch] = {}

    def note_dispatch(self, task_id: str, worker: str, *, now: float) -> None:
        """Start (or restart) watching a dispatched task."""
        self._watches[task_id] = _Watch(worker=worker, last_signal=now)

    def note_status(self, task_id: str, *, now: float) -> None:
        """A worker signal arrived — the task is alive; reset its ladder."""
        watch = self._watches.get(task_id)
        if watch is None:
            return
        watch.last_signal = now
        watch.woke = False
        watch.redispatched = False

    def note_done(self, task_id: str) -> None:
        """The task completed; stop watching it."""
        self._watches.pop(task_id, None)

    def __len__(self) -> int:
        return len(self._watches)

    def check(
        self,
        *,
        now: float,
        is_stale: Callable[[str], bool | None],
    ) -> list[OfflineDecision]:
        """Advance the ladder for every watched task silent past the window.

        Args:
            now: Caller-supplied monotonic seconds.
            is_stale: The Phase-2 liveness cross-check
                (:meth:`~fleet_orchestrator.liveness.LivenessTracker.is_stale`
                partial'd with now/interval) — ``True`` means the worker's
                heartbeat is already stale, ``None`` means never seen.

        Returns:
            The decisions that fired this check (at most one rung per task per
            call — the ladder never skips a rung silently; staleness skips are
            explicit in the reason).
        """
        decisions: list[OfflineDecision] = []
        for task_id, watch in self._watches.items():
            if (now - watch.last_signal) <= self._timeout_s:
                continue
            stale = is_stale(watch.worker)
            silence = now - watch.last_signal
            base = f"no worker signal for {silence:.0f}s (window {self._timeout_s:.0f}s)"
            if not watch.woke and stale is not True:
                watch.woke = True
                watch.last_signal = now  # next rung gets a fresh window
                decisions.append(OfflineDecision(task_id, watch.worker, Escalation.WAKE, base))
            elif not watch.redispatched:
                if not watch.woke:
                    # Stale heartbeat: waking a dead agent wastes a window.
                    watch.woke = True
                    base += "; heartbeat stale — wake rung skipped"
                watch.redispatched = True
                watch.last_signal = now
                decisions.append(
                    OfflineDecision(task_id, watch.worker, Escalation.REDISPATCH, base)
                )
            else:
                decisions.append(
                    OfflineDecision(
                        task_id,
                        watch.worker,
                        Escalation.BLOCKED,
                        base + "; wake + redispatch exhausted",
                    )
                )
        # BLOCKED is terminal — reclaim the watch (an un-reclaimed dict would
        # grow forever against a chronically-offline worker). A later dispatch
        # of the same task re-watches it from scratch via note_dispatch.
        for decision in decisions:
            if decision.escalation is Escalation.BLOCKED:
                self._watches.pop(decision.task_id, None)
        for decision in decisions:
            logger.warning(
                "orchestrator.offline | task={} worker={} escalation={} ({})",
                decision.task_id,
                decision.worker,
                decision.escalation.value,
                decision.reason,
            )
        return decisions
