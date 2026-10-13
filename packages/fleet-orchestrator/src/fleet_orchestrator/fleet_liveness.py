"""Task-independent fleet-liveness roster sweep (fleet-bus #78).

The shipped :class:`~fleet_orchestrator.offline.OfflineDetector` only watches a
*dispatched* task's worker — an **idle** agent going dark is invisible to it.
The heartbeat :class:`~fleet_orchestrator.liveness.LivenessTracker` (fed hub-wide
by :func:`~fleet_orchestrator.liveness.watch_heartbeats`) already carries the
continuous presence signal; what was missing is a hub-wide, task-independent
sweep over an *expected roster* that narrates when an expected agent has been
silent for N consecutive sweeps, and again when it recovers.

Report-only by construction: this module decides NOTHING about wake/redispatch/
mutation — it only reports transitions the loop narrates. It reuses the heartbeat
tracker rather than adding a second liveness source (no ``/connz`` poll, no
watchdog wire frames — reuse-law redo of the rejected PR #68).

Flap suppression mirrors :class:`~fleet_orchestrator.offline._Watch`'s one-alarm-
per-episode discipline: an agent is reported MISSING once, on the sweep its
consecutive-silent counter crosses ``consecutive``; it is reported RECOVERED once,
on the first fresh beat after having been reported missing. A fresh beat below the
threshold resets the counter silently (a flap under threshold narrates nothing).

Deterministic by construction: the reporter never reads a clock — callers supply
``now`` as monotonic seconds (no ``time``/``datetime`` imports here), and the
per-agent state machine is a pure progression. ``is_stale`` returning ``None``
("never seen") is treated as silent — an expected agent that never emitted a first
beat is exactly the "absent from the bus" case the roster check is for.
"""

from __future__ import annotations

import math
import os
from collections.abc import Callable
from dataclasses import dataclass

from loguru import logger

from fleet_orchestrator.liveness import DEFAULT_MISSED_INTERVALS

_ENV_ENABLED = "FLEET_LIVENESS_ENABLED"
_ENV_EXPECTED = "FLEET_EXPECTED_AGENTS"
_ENV_CONSECUTIVE = "FLEET_LIVENESS_CONSECUTIVE"
_ENV_SWEEP = "FLEET_LIVENESS_SWEEP_SECONDS"

# Off first-deploy: an unconfigured hub is a no-op (the sweep never runs). A roster
# is REQUIRED when enabled — an enabled sweep with no expected agents has nothing
# to check and is a config error (fail-closed in HubConfig, mirroring the intake
# label at config.py).
_DEFAULT_ENABLED = False
# Alarm after this many consecutive silent sweeps — flap suppression: a single
# missed sweep does not narrate, only a sustained silence does.
_DEFAULT_CONSECUTIVE = 3
# How often the loop advances the sweep. Independent of the offline poll cadence.
_DEFAULT_SWEEP_S = 30.0

# Recognised truthy/falsy spellings for the on/off switch. Anything else is a typo
# and refuses to start (a policy switch must never silently default the wrong way),
# matching config._bool_env / intake_poller.
_TRUE_TOKENS = frozenset({"1", "true", "yes", "on"})
_FALSE_TOKENS = frozenset({"0", "false", "no", "off"})


def enabled_from_env() -> bool:
    """Read ``FLEET_LIVENESS_ENABLED`` (default ``False``). Fail loud on junk.

    Off by first-deploy default so an unconfigured hub never sweeps. A typo'd
    toggle must not silently pick a side.

    Raises:
        ValueError: If set to anything outside the recognised truthy/falsy set.
    """
    raw = os.environ.get(_ENV_ENABLED)
    if raw is None:
        return _DEFAULT_ENABLED
    token = raw.strip().lower()
    if token in _TRUE_TOKENS:
        return True
    if token in _FALSE_TOKENS:
        return False
    raise ValueError(
        f"{_ENV_ENABLED} must be one of {sorted(_TRUE_TOKENS | _FALSE_TOKENS)}, got {raw!r}"
    )


def expected_from_env() -> tuple[str, ...]:
    """Read ``FLEET_EXPECTED_AGENTS`` — the comma roster; empty/unset yields none.

    Optional here (returns ``()`` when unset); the caller
    (:meth:`~fleet_orchestrator.config.HubConfig.from_env` /
    :meth:`~fleet_orchestrator.config.HubConfig.__post_init__`) is what refuses to
    start an ENABLED sweep with no roster, so a disabled sweep need not carry one —
    the same fail-closed shape as the intake label (fleet-bus #54).

    Returns:
        The trimmed roster agent ids (``()`` when the env var is unset or blank).
    """
    raw = os.environ.get(_ENV_EXPECTED)
    if not raw:
        return ()
    return tuple(a.strip() for a in raw.split(",") if a.strip())


def consecutive_from_env() -> int:
    """Read ``FLEET_LIVENESS_CONSECUTIVE`` (default 3). Fail loud on junk.

    Raises:
        ValueError: If set non-integer or non-positive — a typo'd threshold must
            not silently become the default (zero/negative would alarm instantly).
    """
    raw = os.environ.get(_ENV_CONSECUTIVE)
    if raw is None:
        return _DEFAULT_CONSECUTIVE
    try:
        value = int(raw)
    except ValueError as exc:
        raise ValueError(f"{_ENV_CONSECUTIVE} must be an integer, got {raw!r}") from exc
    if value <= 0:
        raise ValueError(f"{_ENV_CONSECUTIVE} must be positive, got {value}")
    return value


def sweep_seconds_from_env() -> float:
    """Read ``FLEET_LIVENESS_SWEEP_SECONDS`` (default 30). Fail loud on junk.

    Raises:
        ValueError: If set non-numeric, non-finite, or non-positive — a typo'd
            cadence must not silently become the default.
    """
    raw = os.environ.get(_ENV_SWEEP)
    if raw is None:
        return _DEFAULT_SWEEP_S
    try:
        value = float(raw)
    except ValueError as exc:
        raise ValueError(f"{_ENV_SWEEP} must be a number, got {raw!r}") from exc
    if not math.isfinite(value):
        raise ValueError(f"{_ENV_SWEEP} must be finite, got {value}")
    if value <= 0:
        raise ValueError(f"{_ENV_SWEEP} must be positive, got {value}")
    return value


class TransitionKind:
    """The two report-only fleet-liveness transitions (string tags)."""

    MISSING = "missing"
    RECOVERED = "recovered"


@dataclass(frozen=True)
class FleetSilenceTransition:
    """One report-only fleet-liveness transition for one expected agent.

    Attributes:
        agent: The expected roster agent the transition concerns.
        kind: ``missing`` (crossed the consecutive-silent threshold) or
            ``recovered`` (first fresh beat after having been reported missing).
        silent_sweeps: Consecutive silent sweeps at the transition (the crossing
            count for ``missing``; the count at recovery for ``recovered``).
        reason: Board/log-readable one-liner (coordination metadata only).
    """

    agent: str
    kind: str
    silent_sweeps: int
    reason: str


@dataclass
class _AgentState:
    """Mutable per-agent sweep state (internal)."""

    silent_sweeps: int = 0
    reported_missing: bool = False


# The is_stale cross-check the sweep consumes: agent -> True (stale) / False
# (fresh) / None (never seen). Both True and None count as silent.
_StaleCheck = Callable[[str], "bool | None"]


class FleetLivenessReporter:
    """Sweeps an expected roster against the heartbeat tracker (report-only).

    Owns no coordination logic and mutates nothing on the bus: :meth:`sweep`
    returns the transitions the loop narrates. Reuses the shipped heartbeat
    presence signal (via the injected ``is_stale`` callable partial'd over the
    :class:`~fleet_orchestrator.liveness.LivenessTracker`) rather than adding a
    second liveness source.

    Args:
        expected: The expected roster agent ids swept every tick.
        interval_s: The heartbeat cadence for the staleness math (matches the
            emitter cadence, as the offline ladder's cross-check does).
        consecutive: Consecutive silent sweeps before an agent is reported
            MISSING (flap suppression).
        missed: Missed heartbeat intervals that count as stale
            (``LivenessTracker.is_stale`` ``missed``).
    """

    def __init__(
        self,
        *,
        expected: tuple[str, ...],
        interval_s: float,
        consecutive: int,
        missed: int = DEFAULT_MISSED_INTERVALS,
    ) -> None:
        if not expected:
            raise ValueError(
                "FleetLivenessReporter requires a non-empty expected roster "
                "(an enabled sweep with no agents has nothing to check; fleet-bus #78)"
            )
        if consecutive <= 0:
            raise ValueError(
                f"FleetLivenessReporter consecutive must be positive, got {consecutive}"
            )
        if not interval_s > 0:
            raise ValueError(f"FleetLivenessReporter interval_s must be positive, got {interval_s}")
        self._expected = expected
        self._interval_s = interval_s
        self._consecutive = consecutive
        self._missed = missed
        self._state: dict[str, _AgentState] = {agent: _AgentState() for agent in expected}

    def sweep(self, *, now: float, is_stale: _StaleCheck) -> list[FleetSilenceTransition]:
        """Advance the roster sweep ONE tick; return the transitions that fired.

        Pure and deterministic. For each expected agent it reads ``is_stale``:
        ``True``/``None`` (stale or never-seen) is a silent tick that climbs the
        per-agent counter — the crossing of ``consecutive`` fires exactly one
        ``missing`` transition and latches (no repeat spam while still silent).
        ``False`` (a fresh beat) resets the counter; if the agent had been reported
        missing, that reset fires exactly one ``recovered`` transition and clears
        the latch. A fresh beat that was never reported missing narrates nothing
        (a flap under threshold is silent).

        Args:
            now: Caller-supplied monotonic seconds.
            is_stale: The heartbeat cross-check (agent -> ``True``/``False``/
                ``None``), partial'd over the tracker with ``now``/``interval``.

        Returns:
            The transitions that fired this sweep (at most one per agent).
        """
        transitions: list[FleetSilenceTransition] = []
        for agent in self._expected:
            state = self._state[agent]
            stale = is_stale(agent)
            if stale is False:
                # A fresh beat: reset the counter. Recovery narrates only if the
                # agent had been reported missing (else a sub-threshold flap is
                # silent — same discipline as the offline ladder's one-per-episode).
                if state.reported_missing:
                    transitions.append(
                        FleetSilenceTransition(
                            agent=agent,
                            kind=TransitionKind.RECOVERED,
                            silent_sweeps=state.silent_sweeps,
                            reason="fresh heartbeat after being reported missing",
                        )
                    )
                state.silent_sweeps = 0
                state.reported_missing = False
                continue
            # stale is True or None (never seen) — a silent tick.
            state.silent_sweeps += 1
            if not state.reported_missing and state.silent_sweeps >= self._consecutive:
                state.reported_missing = True
                never_seen = stale is None
                reason = f"no heartbeat for {state.silent_sweeps} consecutive sweeps" + (
                    " (never seen on the bus)" if never_seen else ""
                )
                transitions.append(
                    FleetSilenceTransition(
                        agent=agent,
                        kind=TransitionKind.MISSING,
                        silent_sweeps=state.silent_sweeps,
                        reason=reason,
                    )
                )
        for transition in transitions:
            level = logger.warning if transition.kind == TransitionKind.MISSING else logger.info
            level(
                "orchestrator.fleet_liveness | agent={} kind={} silent_sweeps={} ({}) now={}",
                transition.agent,
                transition.kind,
                transition.silent_sweeps,
                transition.reason,
                now,
            )
        return transitions
