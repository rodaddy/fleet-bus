"""Unit tests for the nightly organic canary (fleet-bus #89).

No network, no real clock: a fake k-board client scripts the create + poll
sequence and an injected monotonic/sleep drives the window deterministically. The
failure headline is asserted against the REAL :class:`~fleet_orchestrator.governor.Narrator`
over a recording bus (the same proven pattern as ``test_narration``), so the
"narrator headline ONLY on failure" contract is proved on the actual beat path,
not a mock.

Covered:
  * GREEN  — card reaches ``done`` within the window → no narration, exit 0.
  * TIMEOUT — never reaches ``done`` before the deadline → ONE headline, exit 1.
  * BLOCKED — card goes ``blocked`` → ONE headline, exit 1.
  * ERROR  — create raises (board unreachable) → ONE headline, exit 1.
  * dry run (no narrator) — RED result still returned, no beat surfaced.
"""

from __future__ import annotations

import itertools

import pytest
from fleet_nats import Envelope, subjects

from fleet_orchestrator import canary as canary_mod
from fleet_orchestrator.canary import CanaryOutcome, CanaryResult, main, run_canary
from fleet_orchestrator.governor import NARRATION_KIND, Narrator
from fleet_orchestrator.kboard import KboardError, KboardTask

_LABEL = "[fleet]"
_CHANNEL = "all-agents"


class _RecordingBus:
    """Captures every published (subject, envelope) — no network."""

    def __init__(self) -> None:
        self.published: list[tuple[str, Envelope]] = []

    async def publish(self, subject: str, envelope: Envelope) -> None:
        self.published.append((subject, envelope))


def _narrator(bus: _RecordingBus) -> Narrator:
    ids = (f"n{i}" for i in itertools.count())
    times = (f"2026-07-09T00:00:{i:02d}Z" for i in itertools.count())
    return Narrator(
        bus, env="dev", manager="nagatha", new_id=lambda: next(ids), now=lambda: next(times)
    )


def _task(task_id: str, status: str) -> KboardTask:
    return KboardTask.from_row({"task_id": task_id, "status": status, "sync_version": 1})


class _FakeKboard:
    """A scripted k-board double: records the created card, replays poll statuses.

    ``statuses`` is the sequence of statuses ``get_task`` returns on successive
    polls (the last value repeats if polling outlasts the script). ``create_error``
    /``get_error`` inject a :class:`KboardError` to exercise the degraded paths.
    """

    def __init__(
        self,
        *,
        statuses: list[str] | None = None,
        create_error: bool = False,
        get_error: bool = False,
    ) -> None:
        self._statuses = statuses or []
        self._create_error = create_error
        self._get_error = get_error
        self.created: dict[str, str] | None = None
        self._poll = 0

    async def create_task(
        self, *, task_id: str, title: str, body: str = "", status: str = "todo"
    ) -> KboardTask:
        if self._create_error:
            raise KboardError("board unreachable on create")
        self.created = {"task_id": task_id, "title": title, "body": body, "status": status}
        return _task(task_id, status)

    async def get_task(self, task_id: str) -> KboardTask:
        if self._get_error:
            raise KboardError("board unreachable on poll")
        idx = min(self._poll, len(self._statuses) - 1)
        self._poll += 1
        return _task(task_id, self._statuses[idx])


class _Clock:
    """A monotonic double that advances a fixed step on every injected sleep."""

    def __init__(self, *, step: float) -> None:
        self._t = 0.0
        self._step = step

    def now(self) -> float:
        return self._t

    async def sleep(self, _seconds: float) -> None:
        self._t += self._step


async def _run(
    kboard: _FakeKboard,
    *,
    narrator: Narrator | None,
    timeout_s: float = 100.0,
    poll_s: float = 10.0,
    step: float = 10.0,
) -> CanaryResult:
    clock = _Clock(step=step)
    return await run_canary(
        kboard=kboard,  # type: ignore[arg-type]  # structural double
        intake_label=_LABEL,
        timeout_s=timeout_s,
        poll_s=poll_s,
        narrator=narrator,
        channel=_CHANNEL,
        monotonic=clock.now,
        sleep=clock.sleep,
    )


async def test_green_reaches_done_and_narrates_nothing() -> None:
    # todo → running → done inside the window: GREEN, exit 0, no channel post.
    bus = _RecordingBus()
    kboard = _FakeKboard(statuses=["running", "running", "done"])
    result = await _run(kboard, narrator=_narrator(bus))

    assert result.outcome is CanaryOutcome.GREEN
    assert result.is_green is True
    assert result.exit_code == 0
    assert bus.published == []  # silent on a healthy night — the whole point.
    # The created card carried the [fleet] label + real (non-empty) task text.
    assert kboard.created is not None
    assert kboard.created["title"].startswith(_LABEL)
    assert kboard.created["status"] == "todo"
    assert len(kboard.created["body"]) > 0


async def test_timeout_narrates_one_failure_headline_and_exits_one() -> None:
    # Never reaches done: after the deadline elapses, exactly ONE headline fires.
    bus = _RecordingBus()
    kboard = _FakeKboard(statuses=["running"])  # stuck running forever
    result = await _run(kboard, narrator=_narrator(bus), timeout_s=30.0, poll_s=10.0, step=10.0)

    assert result.outcome is CanaryOutcome.TIMEOUT
    assert result.exit_code == 1
    assert len(bus.published) == 1  # exactly one headline
    subject, env = bus.published[0]
    assert subject == subjects.agent_inbox("dev", "nagatha")  # manager inbox lane
    assert env.kind == NARRATION_KIND
    assert env.channel == _CHANNEL  # carries the intake channel (PR #85 path)
    text = env.payload["text"]
    assert text.startswith("[fleet] nightly canary FAILED")
    assert result.task_id in text  # the card id is the headline target


async def test_blocked_narrates_one_failure_headline_and_exits_one() -> None:
    bus = _RecordingBus()
    kboard = _FakeKboard(statuses=["running", "blocked"])
    result = await _run(kboard, narrator=_narrator(bus))

    assert result.outcome is CanaryOutcome.BLOCKED
    assert result.exit_code == 1
    assert len(bus.published) == 1
    _, env = bus.published[0]
    assert env.payload["text"].startswith("[fleet] nightly canary FAILED")
    assert "blocked" in env.payload["text"]


async def test_create_error_narrates_and_exits_one() -> None:
    bus = _RecordingBus()
    kboard = _FakeKboard(create_error=True)
    result = await _run(kboard, narrator=_narrator(bus))

    assert result.outcome is CanaryOutcome.ERROR
    assert result.exit_code == 1
    assert len(bus.published) == 1  # a board-down create still surfaces one headline
    assert kboard.created is None  # nothing was created


async def test_persistent_poll_error_times_out_with_one_headline() -> None:
    # Create succeeds but every poll read fails: the window elapses in read errors
    # and the run ends as a TIMEOUT with exactly one headline (never silent).
    bus = _RecordingBus()
    kboard = _FakeKboard(get_error=True)
    result = await _run(kboard, narrator=_narrator(bus), timeout_s=30.0, poll_s=10.0, step=10.0)

    assert result.outcome is CanaryOutcome.TIMEOUT
    assert result.exit_code == 1
    assert len(bus.published) == 1


async def test_dry_run_returns_red_but_surfaces_no_beat() -> None:
    # No narrator wired (dry run): a failure still returns a RED result + exit 1,
    # but nothing is published (there is no bus to post to).
    kboard = _FakeKboard(statuses=["running", "blocked"])
    result = await _run(kboard, narrator=None)

    assert result.outcome is CanaryOutcome.BLOCKED
    assert result.exit_code == 1


async def test_transient_poll_error_then_done_is_green() -> None:
    # A single transient read failure mid-window must NOT be fatal: the next poll
    # sees `done` and the run is GREEN with no headline (proves the retry path).
    class _FlakyKboard(_FakeKboard):
        def __init__(self) -> None:
            super().__init__(statuses=["done"])
            self._first = True

        async def get_task(self, task_id: str) -> KboardTask:
            if self._first:
                self._first = False
                raise KboardError("transient blip on first poll")
            return await super().get_task(task_id)

    bus = _RecordingBus()
    result = await _run(_FlakyKboard(), narrator=_narrator(bus), timeout_s=100.0)

    assert result.outcome is CanaryOutcome.GREEN
    assert result.exit_code == 0
    assert bus.published == []  # recovered → silent


# ---- entrypoint fail-loud config (main() → exit 2 before any network) --------

_REQUIRED_ENV = ("FLEET_MANAGER_ID", "FLEET_INTAKE_LABEL", "FLEET_INTAKE_CHANNEL")


def _set_live_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Set every required live-run var so a guard under test fires in isolation."""
    monkeypatch.setenv("FLEET_MANAGER_ID", "nagatha")
    monkeypatch.setenv("FLEET_INTAKE_LABEL", "[fleet]")
    monkeypatch.setenv("FLEET_INTAKE_CHANNEL", "all-agents")


def _guard_no_network(monkeypatch: pytest.MonkeyPatch) -> None:
    """Trip-wire: if a config guard fails to short-circuit, the bus/kboard build
    raises loudly so a regression can't silently reach the network in a unit test."""

    def _boom(*_a: object, **_k: object) -> object:
        raise AssertionError("reached network setup — a config guard failed to fire")

    monkeypatch.setattr(canary_mod, "FleetBus", _boom)
    monkeypatch.setattr(canary_mod, "KboardClient", _boom)


def test_main_missing_manager_id_exits_two(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_live_env(monkeypatch)
    monkeypatch.delenv("FLEET_MANAGER_ID", raising=False)
    _guard_no_network(monkeypatch)
    assert main([]) == 2


def test_main_missing_intake_channel_exits_two(monkeypatch: pytest.MonkeyPatch) -> None:
    # The fail-loud fix: a live (narrating) run with no channel refuses to start,
    # since a RED headline with channel=None would route to nobody.
    _set_live_env(monkeypatch)
    monkeypatch.delenv("FLEET_INTAKE_CHANNEL", raising=False)
    _guard_no_network(monkeypatch)
    assert main([]) == 2


def test_main_bad_timeout_exits_two(monkeypatch: pytest.MonkeyPatch) -> None:
    # A typo'd FLEET_CANARY_TIMEOUT_SECONDS is a ValueError → exit 2 (not a
    # silent default). Requires the three string guards to pass first.
    _set_live_env(monkeypatch)
    monkeypatch.setenv("FLEET_CANARY_TIMEOUT_SECONDS", "not-a-number")
    _guard_no_network(monkeypatch)
    assert main([]) == 2


def test_main_dry_run_tolerates_missing_channel(monkeypatch: pytest.MonkeyPatch) -> None:
    # --dry-run wires no narrator, so an unset channel is NOT a config error there:
    # the run must get past the channel guard (and only then hit the network stub).
    monkeypatch.setenv("FLEET_MANAGER_ID", "nagatha")
    monkeypatch.setenv("FLEET_INTAKE_LABEL", "[fleet]")
    monkeypatch.delenv("FLEET_INTAKE_CHANNEL", raising=False)

    reached: dict[str, bool] = {"network": False}

    def _mark(*_a: object, **_k: object) -> object:
        reached["network"] = True
        raise KboardError("stub: past the channel guard")  # abort before real I/O

    monkeypatch.setattr(canary_mod, "FleetBus", _mark)
    # dry-run does not build a narrator; FleetBus is the first network construct.
    with pytest.raises(KboardError):
        main(["--dry-run"])
    assert reached["network"] is True  # got past the channel guard, as intended
