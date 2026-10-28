"""Narration tests (#13): one throat, templated beats, worker silence."""

from __future__ import annotations

import itertools
from pathlib import Path

import pytest
from fleet_nats import Envelope, subjects

from fleet_orchestrator.governor import NARRATION_KIND, Narrator, render_narration
from fleet_orchestrator.state import CoordinationSession, State


class _RecordingBus:
    def __init__(self) -> None:
        self.published: list[tuple[str, Envelope]] = []

    async def publish(self, subject: str, envelope: Envelope) -> None:
        self.published.append((subject, envelope))


class _ExplodingBus:
    async def publish(self, subject: str, envelope: Envelope) -> None:
        raise ConnectionError("bus down")


def _session() -> CoordinationSession:
    return CoordinationSession(
        task_id="t-1",
        channel="dash-dev",
        manager="heron",
        workers=("wren", "finch"),
        state=State.WORKING,
    )


def _narrator(bus: _RecordingBus | _ExplodingBus) -> Narrator:
    ids = (f"n{i}" for i in itertools.count())
    times = (f"2026-07-08T00:00:{i:02d}Z" for i in itertools.count())
    return Narrator(
        bus, env="dev", manager="heron", new_id=lambda: next(ids), now=lambda: next(times)
    )


async def test_narration_publishes_only_on_the_manager_inbox_lane() -> None:
    bus = _RecordingBus()
    narrator = _narrator(bus)
    await narrator.narrate(_session(), "dispatched", target="finch")
    await narrator.narrate(_session(), "done", outcome="completed")
    lanes = {subject for subject, _ in bus.published}
    assert lanes == {subjects.agent_inbox("dev", "heron")}
    for _, env in bus.published:
        assert env.kind == NARRATION_KIND
        assert env.sender == "heron"
        assert env.channel == "dash-dev"


async def test_narration_text_is_templated_metadata_never_prose() -> None:
    bus = _RecordingBus()
    await _narrator(bus).narrate(_session(), "dispatched", target="finch")
    _, env = bus.published[0]
    assert env.payload == {"text": "[t-1] dispatched to finch — coordination started."}


def test_narration_templates_are_a_closed_set() -> None:
    line = render_narration("paused", task_id="t-1", detail="msgs 4 > cap 3")
    assert line == "[t-1] PAUSED over budget (msgs 4 > cap 3) — human resume required."
    with pytest.raises(KeyError):
        render_narration("improvised", task_id="t-1")


def test_fleet_liveness_beats_render() -> None:
    # The task-independent fleet-liveness keys (fleet-bus #78) render with the
    # synthetic "fleet" task id and the agent as target; the closed-set invariant
    # (above) still rejects an unknown beat.
    silent = render_narration("fleet_silent", task_id="fleet", target="finch", detail="3 sweeps")
    assert silent == "[fleet] finch missing from the bus — silent 3 sweeps."
    present = render_narration("fleet_present", task_id="fleet", target="finch")
    assert present == "[fleet] finch back on the bus."


def test_canary_failed_beat_renders_target_and_reason() -> None:
    # fleet-bus #89: the nightly-canary failure headline is a closed-set beat,
    # rendered with the synthetic "fleet" task id, the card id as target, and the
    # failure reason as detail. Only ever narrated on failure (green is silent).
    line = render_narration(
        "fleet_canary_failed", task_id="fleet", target="t_canary1", detail="timed out"
    )
    assert line == (
        "[fleet] nightly canary FAILED — card t_canary1 timed out. See the k-board record."
    )


async def test_narration_publish_failure_is_swallowed() -> None:
    """Narration must never take coordination down — log and continue."""
    await _narrator(_ExplodingBus()).narrate(_session(), "woken", target="finch")


def test_narration_is_never_originated_by_the_subscriber_package() -> None:
    """Honest structural claim: the subscriber never ORIGINATES narration.

    (It legitimately CONSUMES narration — the manager's own adapter maps a beat
    to an announce turn, and a worker-role adapter drops it — so a blanket
    "the string never appears" scan would prove nothing. What must hold is that
    no envelope the subscriber CONSTRUCTS carries the narration kind.)

    Mechanism: harvest every ``kind=...`` construction argument in the package
    sources — string literals and named constants — and assert narration is not
    among them. The harvest going empty would itself be a test bug, so it is
    asserted non-empty against the kinds the package is known to emit.
    """
    import re

    import fleet_subscriber

    pkg_dir = Path(next(iter(fleet_subscriber.__path__)))
    sources = "\n".join(p.read_text() for p in pkg_dir.glob("*.py"))
    literal_kinds = set(re.findall(r'kind="([a-z_-]+)"', sources))
    constant_kinds = set(re.findall(r"kind=([A-Z][A-Z_]*)", sources))
    assert "status" in literal_kinds  # the harvest works (send() emits status)
    assert NARRATION_KIND not in literal_kinds
    assert not any("NARRATION" in name for name in constant_kinds)
