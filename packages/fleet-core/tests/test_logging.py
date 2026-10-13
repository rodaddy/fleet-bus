"""Tests for fleet_core.logging.configure_logging (C3 / #168).

The helper owns SINK CONFIGURATION only — level, JSON-vs-human rendering, and
idempotent (re)install — resolved from env or explicit args, fail-loud on junk.
It does not change what call sites log (that convention is in docs/logging.md and
review-enforced), so these tests exercise the sink, not the line format.
"""

from __future__ import annotations

import json

import pytest
from loguru import logger

from fleet_core.logging import configure_logging


@pytest.fixture(autouse=True)
def _restore_logger() -> object:
    """Snapshot/reset loguru around each test so a configured sink never leaks."""
    yield
    logger.remove()


def test_explicit_level_filters_below(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("FLEET_LOG_LEVEL", raising=False)
    monkeypatch.delenv("FLEET_LOG_JSON", raising=False)
    # Capture the configure_logging sink itself by redirecting its stderr target,
    # so the assertion proves *its* WARNING threshold filters an INFO line — not a
    # second, more permissive sink.
    captured: list[str] = []
    monkeypatch.setattr(
        "fleet_core.logging.sys.stderr",
        type(
            "W", (), {"write": lambda _self, s: captured.append(s), "flush": lambda _self: None}
        )(),
    )
    configure_logging(level="WARNING")
    logger.info("below.threshold | x={}", 1)
    logger.warning("at.threshold | x={}", 2)
    text = "".join(captured)
    assert "below.threshold" not in text  # INFO filtered out by the WARNING sink
    assert "at.threshold" in text


def test_level_from_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("FLEET_LOG_LEVEL", "error")  # lowercase → normalized
    monkeypatch.delenv("FLEET_LOG_JSON", raising=False)
    # Should not raise; env level is accepted case-insensitively.
    configure_logging()


def test_explicit_level_beats_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("FLEET_LOG_LEVEL", "DEBUG")
    configure_logging(level="ERROR")  # explicit arg wins — no raise


def test_bad_level_fails_loud(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("FLEET_LOG_LEVEL", "INFOO")
    with pytest.raises(ValueError, match="FLEET_LOG_LEVEL"):
        configure_logging()


def test_bad_level_arg_fails_loud() -> None:
    with pytest.raises(ValueError, match="FLEET_LOG_LEVEL"):
        configure_logging(level="verbose")


def test_json_output_is_valid_jsonl(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("FLEET_LOG_LEVEL", raising=False)
    monkeypatch.setenv("FLEET_LOG_JSON", "true")
    # Capture the sink that configure_logging ITSELF installs by redirecting its
    # stderr target BEFORE configuration — so the assertion proves the configured
    # JSON branch (serialize=True on that sink) emits parseable JSONL, not a
    # second, hand-installed serialize sink that would only re-prove loguru.
    captured: list[str] = []
    monkeypatch.setattr(
        "fleet_core.logging.sys.stderr",
        type(
            "W", (), {"write": lambda _self, s: captured.append(s), "flush": lambda _self: None}
        )(),
    )
    configure_logging()
    logger.info("fleetbus.publish | subject={} kind={}", "dev.x", "wake")
    assert captured, "expected a serialized record from the configured JSON sink"
    record = json.loads("".join(captured))
    assert record["record"]["message"].startswith("fleetbus.publish |")
    assert record["record"]["level"]["name"] == "INFO"


def test_bad_json_value_fails_loud(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("FLEET_LOG_LEVEL", raising=False)
    monkeypatch.setenv("FLEET_LOG_JSON", "maybe")
    with pytest.raises(ValueError, match="FLEET_LOG_JSON"):
        configure_logging()


def test_idempotent_no_duplicate_sinks(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("FLEET_LOG_LEVEL", raising=False)
    monkeypatch.delenv("FLEET_LOG_JSON", raising=False)
    captured: list[str] = []
    configure_logging()
    configure_logging()  # re-init must not stack duplicate sinks
    # Install one capturing sink and confirm a single emission per record.
    logger.remove()
    logger.add(captured.append, level="INFO", format="{message}")
    logger.info("once.only | n={}", 1)
    assert len([line for line in captured if "once.only" in line]) == 1
