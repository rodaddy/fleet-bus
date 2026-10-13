"""Subscriber-to-scorekeeper contract tests for organic stance turns."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any

import pytest
from fleet_nats import Envelope, Stance, StancePosition, subjects
from fleet_subscriber import build_adapter_class

from fleet_orchestrator.score import ScoreEventKind, Scorekeeper, parse_turn_signal


def _turn(visible_text: str, *, stance: str | None = None, fact_pull: bool = False) -> str:
    """Serialize a typed fleet-core Turn — the bus-first signal path (#77 / #172).

    A signed stance rides a typed ``stance`` field on a JSON Turn payload, NOT a
    ``[[stance:...]]`` prose trailer. Prose scraping was removed from the active
    path in #172; connectors carry no control signal, so the stance MUST be a
    field for the status lane and scorekeeper to see it.
    """
    payload: dict[str, Any] = {"visible_text": visible_text}
    if stance is not None:
        payload["stance"] = stance
        payload["fact_pull"] = fact_pull
    return json.dumps(payload)


@dataclass
class _SendResult:
    success: bool
    message_id: str | None = None


class _Platform:
    def __init__(self, name: str) -> None:
        self.name = name


class _BaseAdapter:
    def __init__(self, config: Any, platform: Any) -> None:
        self.config = config
        self.platform = platform


class _GatewayApi:
    BasePlatformAdapter = _BaseAdapter
    MessageEvent = object
    MessageType = object
    Platform = _Platform
    SendResult = _SendResult
    SessionSource = object


@dataclass
class _RecordingBus:
    published: list[tuple[str, Envelope]] = field(default_factory=list)

    async def publish(self, subject: str, envelope: Envelope) -> None:
        self.published.append((subject, envelope))


@dataclass
class _Config:
    home_channel: str | None = None


def _correlated_adapter(monkeypatch: pytest.MonkeyPatch, agent: str) -> tuple[Any, _RecordingBus]:
    monkeypatch.setenv("FLEET_AGENT_ID", agent)
    monkeypatch.setenv("FLEET_ENV", "dev")
    monkeypatch.setenv("FLEET_NATS_URL", "nats://127.0.0.1:4222")
    monkeypatch.delenv("FLEET_VOICE_TEE_ENABLED", raising=False)

    adapter = build_adapter_class(_GatewayApi())(_Config())
    bus = _RecordingBus()
    adapter._bus = bus
    adapter._channel_task["dash-dev"] = "task-1"
    return adapter, bus


async def test_stance_without_done_survives_gate_and_reaches_circlejerk_detector(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    skippy, skippy_bus = _correlated_adapter(monkeypatch, "skippy")
    bilby, bilby_bus = _correlated_adapter(monkeypatch, "bilby")

    # The bare ack no longer vanishes (#116): it lands on the ACK lane, which is
    # app-plane and which the scorekeeper below never reads. What matters for
    # scoring is unchanged — the ack contributes NO status envelope.
    await skippy.send("dash-dev", "ack: checking the evidence")
    ack_subject = subjects.agent_ack("dev", "task-1", channel="dash-dev")
    assert [subject for subject, _ in skippy_bus.published] == [ack_subject]
    skippy_bus.published.clear()

    await skippy.send("dash-dev", _turn("Agreed with the proposal.", stance="agree"))
    await bilby.send("dash-dev", _turn("I agree too.", stance="agree"))

    status_subject = subjects.agent_status("dev", "task-1", channel="dash-dev")
    assert [subject for subject, _ in skippy_bus.published] == [status_subject]
    assert [subject for subject, _ in bilby_bus.published] == [status_subject]

    keeper = Scorekeeper(circlejerk_n=2)
    assert keeper.observe(skippy_bus.published[0][1]) is None
    event = keeper.observe(bilby_bus.published[0][1])

    assert event is not None
    assert event.kind is ScoreEventKind.CIRCLEJERK_DETECTED
    assert event.run_length == 2
    assert event.agents == ("bilby", "skippy")


async def test_stance_rides_typed_signal_across_the_wire_boundary(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # The C7 truth path (#182): the subscriber binds a typed fleet-core Stance as
    # typed_signal on the status envelope, and that typed signal — not the
    # payload.meta compatibility mirror — is what survives the REAL bus
    # serialize/deserialize boundary and drives orchestrator scoring.
    skippy, skippy_bus = _correlated_adapter(monkeypatch, "skippy")

    await skippy.send(
        "dash-dev",
        _turn("The metric regressed.", stance="dissent", fact_pull=True),
    )

    assert len(skippy_bus.published) == 1
    subject, emitted = skippy_bus.published[0]
    assert subject == subjects.agent_status("dev", "task-1", channel="dash-dev")

    # Cross the real wire boundary: to_bytes → from_bytes, exactly what the bus
    # does. The typed stance must be reconstructed on the receiving side.
    received = Envelope.from_bytes(emitted.to_bytes())
    assert isinstance(received.typed_signal, Stance)
    assert received.typed_signal.position is StancePosition.DISSENT
    assert received.typed_signal.fact_pull is True

    # Scoring works from the typed signal ALONE: strip the payload.meta mirror so
    # the legacy fallback cannot answer, and parse_turn_signal must still sign.
    received.payload.pop("meta")
    signal = parse_turn_signal(received)
    assert signal is not None
    assert signal.agent_id == "skippy"
    assert signal.task_id == "task-1"
    assert signal.stance is StancePosition.DISSENT
    assert signal.fact_pull is True

    # And the scorekeeper consumes meta-stripped, wire-roundtripped envelopes
    # end-to-end — typed-signal-only circlejerk detection (agree runs trip it;
    # the dissent above would reset the run, so this leg uses agree turns).
    def _wire_agree(bus: _RecordingBus, index: int) -> Envelope:
        env = Envelope.from_bytes(bus.published[index][1].to_bytes())
        env.payload.pop("meta")
        return env

    skippy2, skippy2_bus = _correlated_adapter(monkeypatch, "skippy")
    bilby, bilby_bus = _correlated_adapter(monkeypatch, "bilby")
    await skippy2.send("dash-dev", _turn("Agreed.", stance="agree"))
    await bilby.send("dash-dev", _turn("I agree too.", stance="agree"))

    keeper = Scorekeeper(circlejerk_n=2)
    assert keeper.observe(_wire_agree(skippy2_bus, 0)) is None
    event = keeper.observe(_wire_agree(bilby_bus, 0))
    assert event is not None
    assert event.kind is ScoreEventKind.CIRCLEJERK_DETECTED
    assert event.agents == ("bilby", "skippy")
