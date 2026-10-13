"""Unit tests for the deterministic detectors + arbitration policy.

Pure functions, no bus: prove the missing-handoff detection and the arbitration
decision are reproducible and correct in isolation.
"""

from __future__ import annotations

import pytest

from fleet_orchestrator import decide_arbitration, detect_missing_handoff
from fleet_orchestrator.state import EventKind

WORKERS = ("skippy", "bilby")


def test_missing_handoff_detected_when_no_peer_mentioned() -> None:
    event = detect_missing_handoff(
        sender="skippy", text="I finished my part of the analysis.", peers=WORKERS
    )
    assert event is not None
    assert event.kind is EventKind.HANDOFF_MISSING
    assert event.actor == "skippy"
    assert event.relay_to == "bilby"
    assert event.context["missed_from"] == "skippy"


def test_no_missing_handoff_when_peer_is_mentioned() -> None:
    event = detect_missing_handoff(
        sender="skippy", text="Done — over to you @bilby to wire it up.", peers=WORKERS
    )
    assert event is None


def test_mention_detection_is_case_insensitive() -> None:
    event = detect_missing_handoff(sender="skippy", text="handing to @BILBY now", peers=WORKERS)
    assert event is None


def test_mention_requires_token_boundary_not_substring() -> None:
    # @bilby must NOT match inside @bilbylicious — otherwise a real missing
    # handoff is suppressed (the false-negative the boundary guard fixes).
    event = detect_missing_handoff(
        sender="skippy", text="great work @bilbylicious keep going", peers=WORKERS
    )
    assert event is not None
    assert event.relay_to == "bilby"


def test_email_like_at_does_not_count_as_mention() -> None:
    # email@bilby.com is not a handoff to bilby — the @ is mid-token.
    event = detect_missing_handoff(
        sender="skippy", text="cc them at ops@bilby.example — done", peers=WORKERS
    )
    assert event is not None
    assert event.relay_to == "bilby"


def test_mention_with_trailing_punctuation_still_counts() -> None:
    # "@bilby," / "@bilby." are real mentions (punctuation is a boundary).
    for text in ("over to you, @bilby.", "@bilby, your turn", "handing off (@bilby)"):
        assert detect_missing_handoff(sender="skippy", text=text, peers=WORKERS) is None


def test_solo_worker_has_no_missing_handoff() -> None:
    event = detect_missing_handoff(sender="skippy", text="all done", peers=("skippy",))
    assert event is None


def test_relay_target_is_deterministic_first_unmentioned_peer() -> None:
    # With two un-mentioned peers, the sorted-first is chosen, reproducibly.
    trio = ("skippy", "bilby", "nagatha")
    event = detect_missing_handoff(sender="skippy", text="finished", peers=trio)
    assert event is not None
    assert event.relay_to == "bilby"  # sorted(["bilby", "nagatha"])[0]


def test_decide_arbitration_is_deterministic() -> None:
    assert decide_arbitration(topic="who-goes-first", workers=WORKERS) == "bilby"
    # Same inputs, same output — every time.
    assert decide_arbitration(topic="who-goes-first", workers=WORKERS) == "bilby"


def test_decide_arbitration_empty_workers_raises() -> None:
    with pytest.raises(ValueError, match="no workers"):
        decide_arbitration(topic="x", workers=())
