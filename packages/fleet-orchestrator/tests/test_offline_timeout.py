"""Agent-offline escalation tests (#25, Phase 3.4): wake → redispatch → blocked."""

from __future__ import annotations

import pytest

from fleet_orchestrator.offline import Escalation, OfflineDetector, timeout_from_env


def _never_stale(_worker: str) -> bool | None:
    return False


def test_offline_ladder_wake_then_redispatch_then_blocked() -> None:
    detector = OfflineDetector(timeout_s=10.0)
    detector.note_dispatch("t1", "bilby", now=0.0)

    assert detector.check(now=10.0, is_stale=_never_stale) == []  # at the window, not past

    (wake,) = detector.check(now=10.5, is_stale=_never_stale)
    assert (wake.task_id, wake.worker, wake.escalation) == ("t1", "bilby", Escalation.WAKE)

    assert detector.check(now=15.0, is_stale=_never_stale) == []  # fresh window after wake
    (redispatch,) = detector.check(now=21.0, is_stale=_never_stale)
    assert redispatch.escalation is Escalation.REDISPATCH

    (blocked,) = detector.check(now=32.0, is_stale=_never_stale)
    assert blocked.escalation is Escalation.BLOCKED
    assert "exhausted" in blocked.reason

    # Terminal: the watch is reclaimed — no re-fire, no unbounded growth.
    assert len(detector) == 0
    assert detector.check(now=100.0, is_stale=_never_stale) == []


def test_offline_worker_signal_resets_the_ladder() -> None:
    detector = OfflineDetector(timeout_s=10.0)
    detector.note_dispatch("t1", "bilby", now=0.0)
    (wake,) = detector.check(now=11.0, is_stale=_never_stale)
    assert wake.escalation is Escalation.WAKE

    detector.note_status("t1", now=12.0)  # the worker answered — alive
    assert detector.check(now=21.0, is_stale=_never_stale) == []  # window restarts
    (wake_again,) = detector.check(now=23.0, is_stale=_never_stale)
    assert wake_again.escalation is Escalation.WAKE  # ladder restarted from rung one


def test_offline_stale_heartbeat_skips_the_wake_rung() -> None:
    # Phase-2 cross-check: waking an agent whose heartbeat is already stale
    # wastes a window — the ladder goes straight to redispatch, explicitly.
    detector = OfflineDetector(timeout_s=10.0)
    detector.note_dispatch("t1", "bilby", now=0.0)
    (decision,) = detector.check(now=11.0, is_stale=lambda _w: True)
    assert decision.escalation is Escalation.REDISPATCH
    assert "heartbeat stale" in decision.reason
    (blocked,) = detector.check(now=22.0, is_stale=lambda _w: True)
    assert blocked.escalation is Escalation.BLOCKED


def test_offline_done_task_is_unwatched_and_unknown_status_ignored() -> None:
    detector = OfflineDetector(timeout_s=10.0)
    detector.note_dispatch("t1", "bilby", now=0.0)
    detector.note_done("t1")
    detector.note_status("t-unknown", now=1.0)  # never dispatched — no crash
    assert detector.check(now=100.0, is_stale=_never_stale) == []


def test_offline_never_seen_heartbeat_still_walks_the_ladder() -> None:
    # is_stale None (never seen) is unknown, not dead — the wake rung stays.
    detector = OfflineDetector(timeout_s=10.0)
    detector.note_dispatch("t1", "bilby", now=0.0)
    (decision,) = detector.check(now=11.0, is_stale=lambda _w: None)
    assert decision.escalation is Escalation.WAKE


def test_dispatch_timeout_from_env_default_and_failloud(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("FLEET_DISPATCH_TIMEOUT_SECONDS", raising=False)
    assert timeout_from_env() == 300.0
    monkeypatch.setenv("FLEET_DISPATCH_TIMEOUT_SECONDS", "45")
    assert timeout_from_env() == 45.0
    monkeypatch.setenv("FLEET_DISPATCH_TIMEOUT_SECONDS", "later")
    with pytest.raises(ValueError, match="must be a number"):
        timeout_from_env()
    monkeypatch.setenv("FLEET_DISPATCH_TIMEOUT_SECONDS", "inf")
    with pytest.raises(ValueError, match="must be finite"):
        timeout_from_env()
    monkeypatch.setenv("FLEET_DISPATCH_TIMEOUT_SECONDS", "0")
    with pytest.raises(ValueError, match="must be positive"):
        timeout_from_env()
    with pytest.raises(ValueError, match="must be positive"):
        OfflineDetector(timeout_s=0)
