"""Tests for the task-independent fleet-liveness roster sweep (fleet-bus #78).

The reporter is a pure, injected-clock state machine over a REAL
:class:`~fleet_orchestrator.liveness.LivenessTracker` fed synthetic heartbeat
envelopes — no fakes beyond the tracker the design already reuses. Covers the
flap-suppression discipline: silent below threshold narrates nothing, N
consecutive silent sweeps fire exactly ONE ``missing`` (no repeat spam while
still silent), a fresh beat after missing fires exactly one ``recovered``, and a
never-seen expected agent is treated as silent.
"""

from __future__ import annotations

from collections.abc import Callable

import pytest
from fleet_nats import Envelope, heartbeat_envelope

from fleet_orchestrator.fleet_liveness import (
    FleetLivenessReporter,
    FleetSilenceTransition,
    TransitionKind,
    consecutive_from_env,
    enabled_from_env,
    expected_from_env,
    sweep_seconds_from_env,
)
from fleet_orchestrator.liveness import LivenessTracker

_INTERVAL_S = 10.0
_MISSED = 3
# A beat at last_seen=T is stale once (now - T) > interval*missed = 30s.
_STALE_AFTER = _INTERVAL_S * _MISSED


def _heartbeat(agent_id: str, *, seq: int = 1) -> Envelope:
    return heartbeat_envelope(
        msg_id=f"hb-{agent_id}-{seq}",
        ts="2026-07-09T00:00:00Z",
        sender=agent_id,
        role="worker",
        lanes=("inbox",),
        seq=seq,
        last_turn_ts=None,
    )


def _reporter(*, expected: tuple[str, ...], consecutive: int) -> FleetLivenessReporter:
    return FleetLivenessReporter(
        expected=expected,
        interval_s=_INTERVAL_S,
        consecutive=consecutive,
        missed=_MISSED,
    )


def _stale_check(tracker: LivenessTracker, now: float) -> Callable[[str], bool | None]:
    def _check(agent: str) -> bool | None:
        return tracker.is_stale(agent, now=now, interval_s=_INTERVAL_S, missed=_MISSED)

    return _check


# ---- constructor guards ----------------------------------------------------


def test_reporter_rejects_empty_roster() -> None:
    with pytest.raises(ValueError, match="non-empty expected roster"):
        _reporter(expected=(), consecutive=3)


def test_reporter_rejects_non_positive_consecutive() -> None:
    with pytest.raises(ValueError, match="consecutive must be positive"):
        _reporter(expected=("bilby",), consecutive=0)


def test_reporter_rejects_non_positive_interval() -> None:
    with pytest.raises(ValueError, match="interval_s must be positive"):
        FleetLivenessReporter(expected=("bilby",), interval_s=0.0, consecutive=3)


# ---- the sweep state machine -----------------------------------------------


def test_fresh_agent_never_narrates() -> None:
    tracker = LivenessTracker()
    tracker.observe(_heartbeat("bilby"), now=0.0)
    reporter = _reporter(expected=("bilby",), consecutive=3)

    # Still fresh across several sweeps — never crosses the staleness window.
    for now in (1.0, 5.0, 20.0):
        assert reporter.sweep(now=now, is_stale=_stale_check(tracker, now)) == []


def test_below_threshold_is_silent() -> None:
    tracker = LivenessTracker()
    tracker.observe(_heartbeat("bilby"), now=0.0)
    reporter = _reporter(expected=("bilby",), consecutive=3)

    # Two silent sweeps (past the staleness window) — under the threshold of 3.
    now = _STALE_AFTER + 1.0  # stale
    assert reporter.sweep(now=now, is_stale=_stale_check(tracker, now)) == []
    now += 1.0
    assert reporter.sweep(now=now, is_stale=_stale_check(tracker, now)) == []


def test_n_consecutive_stale_fires_exactly_one_missing() -> None:
    tracker = LivenessTracker()
    tracker.observe(_heartbeat("bilby"), now=0.0)
    reporter = _reporter(expected=("bilby",), consecutive=3)

    now = _STALE_AFTER + 1.0
    assert reporter.sweep(now=now, is_stale=_stale_check(tracker, now)) == []  # 1 silent
    now += 1.0
    assert reporter.sweep(now=now, is_stale=_stale_check(tracker, now)) == []  # 2 silent
    now += 1.0
    fired = reporter.sweep(now=now, is_stale=_stale_check(tracker, now))  # 3 silent -> alarm
    assert len(fired) == 1
    assert fired[0].agent == "bilby"
    assert fired[0].kind == TransitionKind.MISSING
    assert fired[0].silent_sweeps == 3

    # Latched: still silent on the next sweeps — NO repeat missing spam.
    now += 1.0
    assert reporter.sweep(now=now, is_stale=_stale_check(tracker, now)) == []
    now += 1.0
    assert reporter.sweep(now=now, is_stale=_stale_check(tracker, now)) == []


def test_recovery_after_missing_fires_exactly_one_recovered() -> None:
    tracker = LivenessTracker()
    tracker.observe(_heartbeat("bilby"), now=0.0)
    reporter = _reporter(expected=("bilby",), consecutive=2)

    now = _STALE_AFTER + 1.0
    reporter.sweep(now=now, is_stale=_stale_check(tracker, now))  # 1 silent
    now += 1.0
    fired = reporter.sweep(now=now, is_stale=_stale_check(tracker, now))  # 2 silent -> missing
    assert fired[0].kind == TransitionKind.MISSING

    # A fresh beat arrives — the tracker sees the agent again (last_seen=now).
    now += 1.0
    tracker.observe(_heartbeat("bilby", seq=2), now=now)
    recovered = reporter.sweep(now=now, is_stale=_stale_check(tracker, now))
    assert len(recovered) == 1
    assert recovered[0].agent == "bilby"
    assert recovered[0].kind == TransitionKind.RECOVERED

    # Latch cleared: a following fresh sweep narrates nothing (no repeat recovered).
    now += 1.0
    assert reporter.sweep(now=now, is_stale=_stale_check(tracker, now)) == []


def test_flap_under_threshold_never_narrates_recovery() -> None:
    tracker = LivenessTracker()
    tracker.observe(_heartbeat("bilby"), now=0.0)
    reporter = _reporter(expected=("bilby",), consecutive=3)

    # One silent sweep (below threshold) then a fresh beat: never reported missing,
    # so the recovery is silent too — a sub-threshold flap makes NO noise.
    now = _STALE_AFTER + 1.0
    assert reporter.sweep(now=now, is_stale=_stale_check(tracker, now)) == []
    now += 1.0
    tracker.observe(_heartbeat("bilby", seq=2), now=now)
    assert reporter.sweep(now=now, is_stale=_stale_check(tracker, now)) == []


def test_never_seen_expected_agent_is_treated_as_silent() -> None:
    # No heartbeat ever observed for "ghost": is_stale returns None, which the
    # sweep counts as a silent tick and alarms on after the threshold.
    tracker = LivenessTracker()
    reporter = _reporter(expected=("ghost",), consecutive=2)

    fired: list[FleetSilenceTransition] = []
    now = 1.0
    for _ in range(2):
        fired = reporter.sweep(now=now, is_stale=_stale_check(tracker, now))
        now += 1.0
    assert len(fired) == 1
    assert fired[0].agent == "ghost"
    assert fired[0].kind == TransitionKind.MISSING
    assert "never seen on the bus" in fired[0].reason


def test_sweep_is_per_agent_independent() -> None:
    tracker = LivenessTracker()
    tracker.observe(_heartbeat("bilby"), now=0.0)
    tracker.observe(_heartbeat("skippy"), now=0.0)
    reporter = _reporter(expected=("bilby", "skippy"), consecutive=2)

    # bilby keeps beating; skippy goes dark.
    now = _STALE_AFTER + 1.0
    tracker.observe(_heartbeat("bilby", seq=2), now=now)
    assert reporter.sweep(now=now, is_stale=_stale_check(tracker, now)) == []  # skippy 1 silent
    now += 1.0
    tracker.observe(_heartbeat("bilby", seq=3), now=now)
    fired = reporter.sweep(now=now, is_stale=_stale_check(tracker, now))  # skippy 2 -> missing
    assert len(fired) == 1
    assert fired[0].agent == "skippy"


# ---- env helpers (fail-loud) -----------------------------------------------


def test_enabled_from_env_default_off(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("FLEET_LIVENESS_ENABLED", raising=False)
    assert enabled_from_env() is False


def test_enabled_from_env_parses_truthy(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("FLEET_LIVENESS_ENABLED", "true")
    assert enabled_from_env() is True


def test_enabled_from_env_rejects_junk(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("FLEET_LIVENESS_ENABLED", "flase")
    with pytest.raises(ValueError, match="FLEET_LIVENESS_ENABLED"):
        enabled_from_env()


def test_expected_from_env_parses_roster(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("FLEET_EXPECTED_AGENTS", " skippy, bilby ,nagatha ")
    assert expected_from_env() == ("skippy", "bilby", "nagatha")


def test_expected_from_env_empty_when_unset(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("FLEET_EXPECTED_AGENTS", raising=False)
    assert expected_from_env() == ()


def test_consecutive_from_env_default_and_reject(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("FLEET_LIVENESS_CONSECUTIVE", raising=False)
    assert consecutive_from_env() == 3
    monkeypatch.setenv("FLEET_LIVENESS_CONSECUTIVE", "0")
    with pytest.raises(ValueError, match="positive"):
        consecutive_from_env()


def test_sweep_seconds_from_env_default_and_reject(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("FLEET_LIVENESS_SWEEP_SECONDS", raising=False)
    assert sweep_seconds_from_env() == 30.0
    monkeypatch.setenv("FLEET_LIVENESS_SWEEP_SECONDS", "nope")
    with pytest.raises(ValueError, match="must be a number"):
        sweep_seconds_from_env()
