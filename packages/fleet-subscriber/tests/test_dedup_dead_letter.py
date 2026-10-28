"""Delivery-semantics guard tests (#25): dedup no-op, dead-letter, stay-up."""

from __future__ import annotations

import itertools

import pytest
from fleet_nats import Envelope, subjects

from fleet_subscriber.dead_letter import DeadLetterReporter, dead_letter_envelope
from fleet_subscriber.dedup import DedupGuard, window_from_env


class _RecordingBus:
    def __init__(self) -> None:
        self.published: list[tuple[str, Envelope]] = []

    async def publish(self, subject: str, envelope: Envelope) -> None:
        self.published.append((subject, envelope))


class _ExplodingBus:
    async def publish(self, subject: str, envelope: Envelope) -> None:
        raise ConnectionError("bus down")


def test_dedup_duplicate_is_noop_and_lru_is_bounded() -> None:
    guard = DedupGuard(window=3)
    assert guard.seen_before("a") is False
    assert guard.seen_before("b") is False
    assert guard.seen_before("a") is True  # duplicate → no-op signal, refreshes "a"
    assert guard.seen_before("c") is False
    assert len(guard) == 3  # order now b, a, c — "b" is oldest
    assert guard.seen_before("d") is False  # evicts "b"
    assert len(guard) == 3
    assert guard.seen_before("a") is True  # the refreshed hot id survived the window
    assert guard.seen_before("b") is False  # evicted → treated as new


def test_dedup_window_from_env_default_and_failloud(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("FLEET_DEDUP_WINDOW", raising=False)
    assert window_from_env() == 1024
    monkeypatch.setenv("FLEET_DEDUP_WINDOW", "64")
    assert window_from_env() == 64
    monkeypatch.setenv("FLEET_DEDUP_WINDOW", "many")
    with pytest.raises(ValueError, match="must be an integer"):
        window_from_env()
    monkeypatch.setenv("FLEET_DEDUP_WINDOW", "0")
    with pytest.raises(ValueError, match="must be positive"):
        window_from_env()
    with pytest.raises(ValueError, match="must be positive"):
        DedupGuard(window=0)


def _reporter(bus: _RecordingBus | _ExplodingBus) -> DeadLetterReporter:
    ids = (f"dl{i}" for i in itertools.count())
    times = (f"2026-07-08T00:00:{i:02d}Z" for i in itertools.count())
    return DeadLetterReporter(
        bus, env="dev", agent_id="finch", new_id=lambda: next(ids), now=lambda: next(times)
    )


def _poison() -> Envelope:
    return Envelope.new(
        msg_id="p1", ts="2026-07-08T00:00:00Z", sender="mystery", kind="dispatch", payload={}
    )


async def test_dead_letter_record_is_sanitized_and_on_the_dead_letter_lane() -> None:
    bus = _RecordingBus()
    await _reporter(bus).report(
        reason="ValueError: unmappable", source_subject="dev.agent.dispatch.finch", source=_poison()
    )
    (subject, record), *_ = bus.published
    assert subject == subjects.dead_letter("dev")
    assert record.kind == "dead_letter"
    # Sanitized: the closed metadata key set — never the source payload.
    assert set(record.payload) == {
        "reason",
        "source_subject",
        "source_id",
        "source_kind",
        "source_ts",
    }
    assert record.payload["source_id"] == "p1"
    assert record.payload["source_subject"] == "dev.agent.dispatch.finch"


async def test_dead_letter_publish_failure_is_swallowed() -> None:
    """The error path must never take the subscriber down — log and drop."""
    await _reporter(_ExplodingBus()).report(
        reason="boom", source_subject="dev.agent.dispatch.finch", source=_poison()
    )  # would raise if the reporter propagated


def test_dead_letter_envelope_round_trips() -> None:
    record = dead_letter_envelope(
        msg_id="dl1",
        ts="t",
        sender="finch",
        reason="KeyError: chat",
        source_subject="dev.agent.relay.finch",
        source_id="s1",
        source_kind="relay",
        source_ts="t0",
    )
    decoded = Envelope.from_bytes(record.to_bytes())
    assert decoded.kind == "dead_letter"
    assert decoded.payload["reason"] == "KeyError: chat"
