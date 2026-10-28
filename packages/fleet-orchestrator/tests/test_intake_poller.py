"""Unit tests for the k-board intake poller (fleet-bus #44).

The poller is the missing publisher of the ``created`` intake event: it lists the
board, and for each new ``todo`` card publishes ``dev.kboard.task.created.<id>``
exactly once. These tests prove — against in-memory doubles, no network —:

- a new ``todo`` card publishes ``created`` exactly once;
- a non-``todo`` (already-claimed/running) card is never published;
- a restart does not double-open (the durable ``todo`` status is the guard);
- the per-process ``seen`` set suppresses a re-publish across ticks;
- a card reset back to ``todo`` is re-openable;
- a k-board list failure is swallowed (the loop survives to the next tick);
- the env config helpers parse and fail loud.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Any

import pytest
from fleet_nats import (
    ChannelMap,
    Envelope,
    PayloadTooLargeError,
    empty_map,
    parse_portals,
    subjects,
)

from fleet_orchestrator.intake_poller import (
    IntakePoller,
    intake_enabled_from_env,
    intake_label_from_env,
    intake_poll_from_env,
)

# The card-scope marker (fleet-bus #54) the tests scope the poller to. FakeTask
# titles default to it so the status/id-focused cases still match the filter;
# the filter's own match/skip behavior is proven by the dedicated cases below.
_MARKER = "[fleet]"

# ---- doubles ----------------------------------------------------------------


@dataclass
class FakeTask:
    """The task fields the poller reads (a structural subset of KboardTask).

    ``title`` defaults to the fleet marker so existing status/id-focused cases pass
    the fleet-bus #54 scope filter without restating it; the filter's match/skip is
    covered by dedicated cases. ``body`` defaults empty; the text-carrying cases set
    both explicitly (fleet-bus #52).

    ``raw`` is the untyped k-board row the real ``KboardTask`` carries. The poller
    reads the card's own ``channel`` from it (fleet-bus #118), so a card may name
    the channel it belongs to instead of inheriting one board-wide default.
    """

    task_id: str
    status: str
    title: str = _MARKER
    body: str = ""
    raw: dict[str, Any] = field(default_factory=dict)


class FakeLister:
    """A k-board list side that returns a scriptable board (or raises)."""

    def __init__(self, boards: list[list[FakeTask]] | None = None) -> None:
        # Each poll_once consumes the next board snapshot; the last is reused once
        # exhausted so a steady-state board can be polled repeatedly.
        self._boards = boards or [[]]
        self._calls = 0
        self.raise_next = False

        # Records dead-letter (poison) status flips: (task_id, status).
        self.status_flips: list[tuple[str, str]] = []
        # When True, update_status raises (simulates k-board down during dead-letter).
        self.flip_raises = False
        self.flip_result_status: str | None = None

    async def list_tasks(self) -> list[FakeTask]:
        if self.raise_next:
            self.raise_next = False
            raise RuntimeError("kboard: 503 (board unreachable)")
        idx = min(self._calls, len(self._boards) - 1)
        self._calls += 1
        return self._boards[idx]

    async def update_status(self, task_id: str, status: str) -> Any:
        if self.flip_raises:
            raise RuntimeError("kboard: 503 (dead-letter flip failed)")
        self.status_flips.append((task_id, status))
        return FakeTask(task_id, self.flip_result_status or status)


class FakeBus:
    """Records published (subject, envelope) pairs — no network.

    ``fail_publishes`` counts down: while > 0 a publish raises (simulating a
    transient bus error) and is not recorded, then decrements.
    """

    def __init__(self, *, fail_publishes: int = 0, oversize: bool = False) -> None:
        self.published: list[tuple[str, Envelope]] = []
        self.fail_publishes = fail_publishes
        # When True, EVERY publish raises PayloadTooLargeError (a PERMANENT
        # failure — the created envelope can never fit the bus max_payload).
        self.oversize = oversize
        self.publish_attempts = 0

    async def publish(self, subject: str, envelope: Envelope) -> None:
        self.publish_attempts += 1
        if self.oversize:
            raise PayloadTooLargeError(subject, size=1_100_000, limit=1_048_576)
        if self.fail_publishes > 0:
            self.fail_publishes -= 1
            raise RuntimeError("bus: publish failed (transient)")
        self.published.append((subject, envelope))


def _poller(
    lister: FakeLister,
    bus: FakeBus,
    *,
    channel: str = "arena",
    workers: tuple[str, ...] = ("finch",),
    label: str = _MARKER,
    channels: ChannelMap | None = None,
) -> IntakePoller:
    ids = iter(f"m{i}" for i in range(1000))
    return IntakePoller(
        lister,  # type: ignore[arg-type]  # duck-typed subset the poller uses
        bus,  # duck-typed publisher subset
        env="dev",
        sender="heron",
        default_workers=workers,
        channels=channels if channels is not None else empty_map(),
        home_channel=channel,
        new_id=lambda: next(ids),
        now=lambda: "2026-07-08T00:00:00Z",
        poll_s=15.0,
        label=label,
    )


def _created_subjects(bus: FakeBus) -> list[str]:
    return [s for s, _ in bus.published]


# ---- poll_once behavior -----------------------------------------------------


async def test_new_todo_card_publishes_created_exactly_once() -> None:
    lister = FakeLister([[FakeTask("t-1", "todo")]])
    bus = FakeBus()
    poller = _poller(lister, bus)

    count = await poller.poll_once()

    assert count == 1
    subject = subjects.kboard_event("dev", "created", "t-1")
    assert _created_subjects(bus) == [subject]
    env = bus.published[0][1]
    assert env.task_id == "t-1"
    assert env.sender == "heron"
    assert env.kind == "kboard_event"
    assert env.channel == "arena"
    # The default FakeTask carries only the scope marker as its title (fleet-bus
    # #54), so the assembled prompt text is that marker; the key is always present.
    assert env.payload == {"channel": "arena", "workers": ["finch"], "text": _MARKER}


async def test_created_event_carries_card_title_and_body_as_text() -> None:
    # fleet-bus #52: the created event MUST carry the card's prompt (title+body) as
    # payload["text"] or the dispatch reaches the worker empty and is dropped.
    lister = FakeLister(
        [[FakeTask("t-1", "todo", title="[fleet] Fix the bug", body="It empties the payload.")]]
    )
    bus = FakeBus()
    poller = _poller(lister, bus)

    count = await poller.poll_once()

    assert count == 1
    env = bus.published[0][1]
    assert env.payload["text"] == "[fleet] Fix the bug\n\nIt empties the payload."


async def test_marker_only_card_publishes_with_marker_as_text() -> None:
    # A card whose title is only the scope marker (no extra prose, no body) is still
    # a task to open: it publishes carrying the marker as its text. (An unmarked or
    # truly empty-title card can never reach publish — it fails the scope filter —
    # so the empty-text WARN branch is now only reachable off the --once path.)
    lister = FakeLister([[FakeTask("t-1", "todo", title=_MARKER, body="")]])
    bus = FakeBus()
    poller = _poller(lister, bus)

    count = await poller.poll_once()

    assert count == 1
    assert bus.published[0][1].payload["text"] == _MARKER


async def test_already_claimed_card_is_ignored() -> None:
    # A running (already-picked-up) card must NOT get a created event.
    lister = FakeLister([[FakeTask("t-1", "running"), FakeTask("t-2", "done")]])
    bus = FakeBus()
    poller = _poller(lister, bus)

    count = await poller.poll_once()

    assert count == 0
    assert bus.published == []


async def test_second_tick_does_not_republish_same_todo_card() -> None:
    # The card is still todo on the next tick (the hub's status write hasn't
    # landed yet); the per-process seen set suppresses a duplicate created event.
    board = [FakeTask("t-1", "todo")]
    lister = FakeLister([board, board])  # same board both ticks
    bus = FakeBus()
    poller = _poller(lister, bus)

    first = await poller.poll_once()
    second = await poller.poll_once()

    assert (first, second) == (1, 0)
    assert len(bus.published) == 1


async def test_restart_does_not_reopen_a_card_the_hub_moved_off_todo() -> None:
    # Cross-restart guard: a fresh poller (empty seen set) polling a board where
    # the card has already advanced to running never re-publishes created.
    lister = FakeLister([[FakeTask("t-1", "running")]])
    bus = FakeBus()
    fresh_poller = _poller(lister, bus)  # simulates a restarted process

    count = await fresh_poller.poll_once()

    assert count == 0
    assert bus.published == []


async def test_card_reset_to_todo_is_reopenable() -> None:
    # todo -> (opened) -> running -> todo (reset) must re-publish once on the reset:
    # dropping a non-todo card from seen lets a genuine re-todo re-open it.
    lister = FakeLister(
        [
            [FakeTask("t-1", "todo")],  # tick 1: open
            [FakeTask("t-1", "running")],  # tick 2: picked up (drops from seen)
            [FakeTask("t-1", "todo")],  # tick 3: reset -> re-open
        ]
    )
    bus = FakeBus()
    poller = _poller(lister, bus)

    c1 = await poller.poll_once()
    c2 = await poller.poll_once()
    c3 = await poller.poll_once()

    assert (c1, c2, c3) == (1, 0, 1)
    assert len(bus.published) == 2


async def test_blank_task_id_row_is_skipped() -> None:
    lister = FakeLister([[FakeTask("", "todo"), FakeTask("t-2", "todo")]])
    bus = FakeBus()
    poller = _poller(lister, bus)

    count = await poller.poll_once()

    assert count == 1
    assert bus.published[0][1].task_id == "t-2"


async def test_list_failure_is_swallowed_and_returns_zero() -> None:
    lister = FakeLister([[FakeTask("t-1", "todo")]])
    lister.raise_next = True
    bus = FakeBus()
    poller = _poller(lister, bus)

    count = await poller.poll_once()

    # The failing tick published nothing and did not raise; the next tick recovers.
    assert count == 0
    assert bus.published == []
    recovered = await poller.poll_once()
    assert recovered == 1


async def test_still_todo_card_republishes_after_grace_window() -> None:
    # A card whose created event never opened a session stays todo. The poller must
    # NOT strand it forever: after the grace window it re-publishes so a silent open
    # failure self-heals (codex review, MAJOR). Poll a steady todo board and count
    # publishes across enough ticks to cross the window.
    from fleet_orchestrator.intake_poller import _REPUBLISH_AFTER_TICKS

    board = [FakeTask("t-1", "todo")]
    lister = FakeLister([board])  # same board reused every tick
    bus = FakeBus()
    poller = _poller(lister, bus)

    # Tick 1 publishes; ticks 2..N are suppressed until the window elapses, then a
    # second publish fires. Over (_REPUBLISH_AFTER_TICKS + 1) ticks we see exactly 2.
    counts = [await poller.poll_once() for _ in range(_REPUBLISH_AFTER_TICKS + 1)]
    assert sum(counts) == 2, counts
    assert len(bus.published) == 2


async def test_publish_failure_does_not_mark_seen_and_retries_next_tick() -> None:
    # A transient publish failure must not suppress the card (it never went out) and
    # must not escape poll_once to kill the loop (codex review, MAJOR).
    board = [FakeTask("t-1", "todo")]
    lister = FakeLister([board, board])
    bus = FakeBus(fail_publishes=1)  # first publish raises, then succeeds
    poller = _poller(lister, bus)

    first = await poller.poll_once()  # publish raises → swallowed, nothing marked
    second = await poller.poll_once()  # retries and succeeds

    assert first == 0
    assert second == 1
    assert len(bus.published) == 1


async def test_empty_channel_yields_unchannelled_envelope() -> None:
    lister = FakeLister([[FakeTask("t-1", "todo")]])
    bus = FakeBus()
    poller = _poller(lister, bus, channel="")

    await poller.poll_once()

    env = bus.published[0][1]
    assert env.channel is None
    assert env.payload == {"channel": "", "workers": ["finch"], "text": _MARKER}


# ---- card-scope filter (fleet-bus #54) --------------------------------------


async def test_marked_card_matches_filter_and_publishes() -> None:
    # A todo card whose title starts with the configured marker is in scope: it
    # publishes created exactly once.
    lister = FakeLister([[FakeTask("t-1", "todo", title="[fleet] do the thing")]])
    bus = FakeBus()
    poller = _poller(lister, bus, label="[fleet]")

    count = await poller.poll_once()

    assert count == 1
    assert bus.published[0][1].task_id == "t-1"


async def test_unmarked_card_is_skipped_not_published() -> None:
    # A todo card whose title lacks the marker is foreign backlog (the fleet-hub
    # first-enable grabbed 8 such cards): it must be skipped, never published.
    lister = FakeLister([[FakeTask("t-1", "todo", title="unrelated backlog card")]])
    bus = FakeBus()
    poller = _poller(lister, bus, label="[fleet]")

    count = await poller.poll_once()

    assert count == 0
    assert bus.published == []


async def test_filter_publishes_only_the_marked_card_on_a_mixed_board() -> None:
    # A realistic board: one marked card among foreign backlog. Only the marked one
    # opens.
    lister = FakeLister(
        [
            [
                FakeTask("keep", "todo", title="[fleet] real work"),
                FakeTask("skip-1", "todo", title="someone else's card"),
                FakeTask("skip-2", "todo", title="another backlog item"),
            ]
        ]
    )
    bus = FakeBus()
    poller = _poller(lister, bus, label="[fleet]")

    count = await poller.poll_once()

    assert count == 1
    assert bus.published[0][1].task_id == "keep"


async def test_leading_whitespace_title_still_matches_marker() -> None:
    # A card title with a leading space still matches (the filter strips before the
    # prefix check) so an accidental space does not defeat scoping.
    lister = FakeLister([[FakeTask("t-1", "todo", title="  [fleet] spaced")]])
    bus = FakeBus()
    poller = _poller(lister, bus, label="[fleet]")

    count = await poller.poll_once()

    assert count == 1


async def test_poller_rejects_empty_label() -> None:
    # Belt-and-suspenders: even if a caller bypasses HubConfig, constructing the
    # poller with an empty marker (which would open every card) fails loud.
    lister = FakeLister([[FakeTask("t-1", "todo")]])
    bus = FakeBus()
    with pytest.raises(ValueError, match="non-empty label"):
        _poller(lister, bus, label="")


# ---- env config helpers -----------------------------------------------------


def test_intake_poll_from_env_default(monkeypatch: Any) -> None:
    monkeypatch.delenv("FLEET_INTAKE_POLL_SECONDS", raising=False)
    assert intake_poll_from_env() == 15.0


def test_intake_poll_from_env_reads_value(monkeypatch: Any) -> None:
    monkeypatch.setenv("FLEET_INTAKE_POLL_SECONDS", "5")
    assert intake_poll_from_env() == 5.0


@pytest.mark.parametrize("bad", ["nope", "0", "-1", "inf", "nan"])
def test_intake_poll_from_env_fails_loud_on_junk(monkeypatch: Any, bad: str) -> None:
    monkeypatch.setenv("FLEET_INTAKE_POLL_SECONDS", bad)
    with pytest.raises(ValueError, match="FLEET_INTAKE_POLL_SECONDS"):
        intake_poll_from_env()


def test_intake_enabled_from_env_default_true(monkeypatch: Any) -> None:
    monkeypatch.delenv("FLEET_INTAKE_POLL_ENABLED", raising=False)
    assert intake_enabled_from_env() is True


@pytest.mark.parametrize("value", ["1", "true", "TRUE", "yes", "on"])
def test_intake_enabled_from_env_truthy(monkeypatch: Any, value: str) -> None:
    monkeypatch.setenv("FLEET_INTAKE_POLL_ENABLED", value)
    assert intake_enabled_from_env() is True


@pytest.mark.parametrize("value", ["0", "false", "FALSE", "no", "off"])
def test_intake_enabled_from_env_falsy(monkeypatch: Any, value: str) -> None:
    monkeypatch.setenv("FLEET_INTAKE_POLL_ENABLED", value)
    assert intake_enabled_from_env() is False


def test_intake_enabled_from_env_fails_loud_on_junk(monkeypatch: Any) -> None:
    monkeypatch.setenv("FLEET_INTAKE_POLL_ENABLED", "maybe")
    with pytest.raises(ValueError, match="FLEET_INTAKE_POLL_ENABLED"):
        intake_enabled_from_env()


def test_intake_label_from_env_unset_is_empty(monkeypatch: Any) -> None:
    # Unset yields "" here; the ENABLED-poller refusal lives in HubConfig.from_env.
    monkeypatch.delenv("FLEET_INTAKE_LABEL", raising=False)
    assert intake_label_from_env() == ""


def test_intake_label_from_env_reads_and_strips(monkeypatch: Any) -> None:
    monkeypatch.setenv("FLEET_INTAKE_LABEL", "  [fleet] ")
    assert intake_label_from_env() == "[fleet]"


# ---- run loop ---------------------------------------------------------------


async def test_run_stops_promptly_on_stop_event() -> None:
    lister = FakeLister([[]])
    bus = FakeBus()
    poller = _poller(lister, bus)
    stop = asyncio.Event()
    stop.set()  # already stopped → run returns without a tick

    await asyncio.wait_for(poller.run(stop), timeout=1.0)
    assert bus.published == []


# ---- oversized card is PERMANENT: dead-lettered, never loops (review P1-4) ----


async def test_oversized_card_does_not_loop() -> None:
    """One oversized card is attempted ONCE, dead-lettered, and never re-published.

    A PayloadTooLargeError is PERMANENT — no retry will ever fit the payload — so
    the poller must NOT re-emit the card every _REPUBLISH_AFTER_TICKS ticks. The
    card is poisoned (in-process skip) and dead-lettered on the board (flipped to
    ``blocked``, off ``todo``) so it drops out of the candidate set for good.
    """
    # A steady board that keeps returning the same todo card every tick.
    lister = FakeLister([[FakeTask("t-big", "todo")]])
    bus = FakeBus(oversize=True)
    poller = _poller(lister, bus)

    # Drive many ticks — far more than the republish grace window.
    for _ in range(20):
        await poller.poll_once()

    # The card was attempted EXACTLY ONCE (poison set stops every later tick).
    assert bus.publish_attempts == 1
    # Nothing was ever successfully published.
    assert bus.published == []
    # It was dead-lettered on the board (flipped off todo → blocked) exactly once.
    assert lister.status_flips == [("t-big", "blocked")]


async def test_oversized_card_not_poisoned_when_dead_letter_flip_fails() -> None:
    """F3: if the k-board flip FAILS the card is left un-poisoned and retries.

    Poisoning before the flip succeeded (old behavior) silently stranded the card
    as ``todo`` forever while the poison guard suppressed every retry. The card
    must instead stay un-poisoned so each later tick re-attempts the dead-letter
    flip until k-board heals.
    """
    lister = FakeLister([[FakeTask("t-big", "todo")]])
    lister.flip_raises = True  # k-board down for the whole run
    bus = FakeBus(oversize=True)
    poller = _poller(lister, bus)

    for _ in range(5):
        await poller.poll_once()

    # NOT poisoned: every tick re-publishes (oversized → flip attempted) again,
    # so the card is retried rather than silently suppressed.
    assert bus.publish_attempts == 5
    assert lister.status_flips == []  # the flip raised every time, so nothing recorded


async def test_dead_letter_timeout_does_not_claim_card_left_todo() -> None:
    """A timeout can follow a committed write, so the durable outcome is unknown."""
    from loguru import logger

    lister = FakeLister([[FakeTask("t-big", "todo")]])
    bus = FakeBus(oversize=True)
    poller = _poller(lister, bus)

    async def commit_then_timeout(task_id: str, status: str) -> Any:
        lister.status_flips.append((task_id, status))
        raise TimeoutError("response timed out after commit")

    lister.update_status = commit_then_timeout  # type: ignore[method-assign]
    logs: list[str] = []
    sink_id = logger.add(
        lambda message: logs.append(message.record["message"]),
        level="ERROR",
        format="{message}",
    )
    try:
        await poller.poll_once()
    finally:
        logger.remove(sink_id)

    assert lister.status_flips == [("t-big", "blocked")]
    failure_logs = [message for message in logs if "blocked write not proven" in message]
    assert failure_logs
    assert all("todo" not in message for message in failure_logs)
    assert all("card remains eligible for retry" in message for message in failure_logs)


async def test_oversized_card_not_poisoned_when_board_returns_done() -> None:
    """A concurrent done result must not be reported or cached as blocked."""
    lister = FakeLister([[FakeTask("t-big", "todo")]])
    lister.flip_result_status = "done"
    bus = FakeBus(oversize=True)
    poller = _poller(lister, bus)

    await poller.poll_once()
    await poller.poll_once()

    assert bus.publish_attempts == 2
    assert lister.status_flips == [("t-big", "blocked"), ("t-big", "blocked")]


async def test_oversized_card_poisons_once_flip_heals() -> None:
    """F3: once the k-board flip succeeds the card is poisoned and stops retrying."""
    lister = FakeLister([[FakeTask("t-big", "todo")]])
    lister.flip_raises = True  # k-board down at first
    bus = FakeBus(oversize=True)
    poller = _poller(lister, bus)

    await poller.poll_once()  # flip fails → not poisoned
    await poller.poll_once()  # flip fails → still retrying
    assert bus.publish_attempts == 2
    assert lister.status_flips == []

    from fleet_orchestrator.intake_poller import _POISON_STATUS

    lister.flip_raises = False  # k-board heals
    await poller.poll_once()  # flip succeeds → NOW poisoned
    assert lister.status_flips == [("t-big", _POISON_STATUS)]

    # Poisoned now: further ticks never re-attempt the publish.
    for _ in range(5):
        await poller.poll_once()
    assert bus.publish_attempts == 3


async def test_transient_publish_error_still_retries_next_tick() -> None:
    """A TRANSIENT publish error (not oversized) is NOT poisoned — it retries and heals."""
    lister = FakeLister([[FakeTask("t-1", "todo")]])
    bus = FakeBus(fail_publishes=1)  # first publish fails transiently, then succeeds
    poller = _poller(lister, bus)

    first = await poller.poll_once()  # transient failure → not published
    assert first == 0
    assert bus.published == []
    second = await poller.poll_once()  # retried and succeeded (not poisoned)
    assert second == 1
    assert len(bus.published) == 1
    assert lister.status_flips == []  # a transient error never dead-letters


# ---- fleet-bus #118: the channel comes from the CARD, via the portal map ------


def _portals() -> ChannelMap:
    return parse_portals(
        [
            {"slug": "arena", "discord_id": "1111"},
            {"slug": "fleet-ops", "discord_id": "2222"},
        ]
    )


async def test_each_card_opens_in_its_own_channel() -> None:
    """Two cards naming two channels open two DIFFERENT sessions' channels.

    This is the #118 defect on the inbound leg. Before the portal map every card
    was stamped with one ``FLEET_INTAKE_CHANNEL``, so a board serving two channels
    opened BOTH cards in whichever one that env named — and the only way to add a
    channel was an env change plus a hub redeploy.
    """
    lister = FakeLister(
        [
            [
                FakeTask("t-1", "todo", raw={"channel": "arena"}),
                FakeTask("t-2", "todo", raw={"channel": "fleet-ops"}),
            ]
        ]
    )
    bus = FakeBus()
    poller = _poller(lister, bus, channel="arena", channels=_portals())

    assert await poller.poll_once() == 2
    by_task = {e.task_id: e for _, e in bus.published}
    assert by_task["t-1"].channel == "arena"
    assert by_task["t-2"].channel == "fleet-ops"
    assert by_task["t-1"].payload["channel"] == "arena"
    assert by_task["t-2"].payload["channel"] == "fleet-ops"
    # Two different channels — the assertion one board-wide default cannot satisfy.
    assert by_task["t-1"].channel != by_task["t-2"].channel


async def test_a_card_channel_given_as_a_discord_id_canonicalises_to_its_slug() -> None:
    """A card naming a raw Discord id is stamped with the coordination SLUG.

    The coordination side keys subjects and sessions on slugs; the display side
    speaks ids. Canonicalising here is what keeps the two legs from drifting.
    """
    lister = FakeLister([[FakeTask("t-1", "todo", raw={"channel": "2222"})]])
    bus = FakeBus()
    poller = _poller(lister, bus, channels=_portals())

    assert await poller.poll_once() == 1
    _, env = bus.published[0]
    assert env.channel == "fleet-ops"
    assert env.payload["channel"] == "fleet-ops"


async def test_the_card_channel_wins_over_the_board_default() -> None:
    """A card that names its channel is never overridden by the board default."""
    lister = FakeLister([[FakeTask("t-1", "todo", raw={"channel": "fleet-ops"})]])
    bus = FakeBus()
    poller = _poller(lister, bus, channel="arena", channels=_portals())

    assert await poller.poll_once() == 1
    assert bus.published[0][1].channel == "fleet-ops"


async def test_a_card_without_a_channel_falls_back_to_the_board_default() -> None:
    """The home channel is the fallback ONLY — and it is canonicalised too."""
    lister = FakeLister([[FakeTask("t-1", "todo")]])
    bus = FakeBus()
    poller = _poller(lister, bus, channel="1111", channels=_portals())

    assert await poller.poll_once() == 1
    assert bus.published[0][1].channel == "arena"


async def test_a_new_channel_slug_opens_with_no_code_change() -> None:
    """The #118 acceptance criterion on the inbound leg: a row, not a code edit.

    Identical poller code and an identical card; the ONLY difference is one more
    entry in the persisted portal map.
    """
    card = FakeTask("t-1", "todo", raw={"channel": "skunkworks"})
    grown = parse_portals(
        [
            {"slug": "arena", "discord_id": "1111"},
            {"slug": "fleet-ops", "discord_id": "2222"},
            {"slug": "skunkworks", "discord_id": "3333"},
        ]
    )
    bus = FakeBus()
    poller = _poller(FakeLister([[card]]), bus, channels=grown)

    assert await poller.poll_once() == 1
    assert bus.published[0][1].channel == "skunkworks"


async def test_an_unmapped_card_channel_still_opens_the_card() -> None:
    """The map translates; it does not gate intake.

    An unreachable channel-map table (empty map) must never stop cards from
    opening — the OUTBOUND leg is where an unmapped channel refuses to post.
    """
    lister = FakeLister([[FakeTask("t-1", "todo", raw={"channel": "brand-new"})]])
    bus = FakeBus()
    poller = _poller(lister, bus, channels=empty_map())

    assert await poller.poll_once() == 1
    assert bus.published[0][1].channel == "brand-new"


# ---- review: a stamped channel becomes a SUBJECT TOKEN -----------------------


async def test_a_wildcard_card_channel_is_refused_not_stamped() -> None:
    """A ``*`` card channel must never reach ``subjects.agent_dispatch``.

    The stamped channel rides ``CoordinationSession.channel`` into
    ``subjects.agent_dispatch(env, worker, channel=session.channel)``, producing
    ``{env}.agent.*.dispatch.{worker}``. ``subjects.slug`` deliberately permits
    ``*`` (callers build authorization grant patterns from STAR), the manager's own
    publish grant is ``agent_dispatch(env, STAR, channel=STAR)`` so it ACCEPTS
    that subject, and nats-py never validates a publish subject client-side —
    ``publish`` is fire-and-forget, so the server's rejection arrives
    asynchronously on ``error_cb`` and never raises. Net result on the unfixed
    code: the card is marked coordinated, every dispatch silently vanishes, and
    the operator sees an ordinary INFO dispatch line. The card falls back to the
    board default instead.
    """
    lister = FakeLister([[FakeTask("t-1", "todo", raw={"channel": "*"})]])
    bus = FakeBus()
    poller = _poller(lister, bus, channel="arena", channels=_portals())

    assert await poller.poll_once() == 1
    _, env = bus.published[0]
    assert env.channel == "arena"  # the board default, never "*"
    assert env.payload["channel"] == "arena"
    assert subjects.agent_dispatch("dev", "wren", channel=env.channel) == (
        "dev.agent.arena.dispatch.wren"
    )


async def test_a_multi_token_wildcard_card_channel_is_refused() -> None:
    """``>`` makes ``subjects.slug`` RAISE inside the publish path.

    That ValueError is swallowed into a log line by the transport, so the card is
    re-polled and dropped forever with no dead-letter. Refuse it at the boundary.
    """
    lister = FakeLister([[FakeTask("t-1", "todo", raw={"channel": ">"})]])
    bus = FakeBus()
    poller = _poller(lister, bus, channel="arena", channels=_portals())

    assert await poller.poll_once() == 1
    assert bus.published[0][1].channel == "arena"


async def test_a_dotted_card_channel_is_refused_so_it_cannot_split_a_subject() -> None:
    """A ``.`` in a channel is a subject DELIMITER, not part of one token."""
    lister = FakeLister([[FakeTask("t-1", "todo", raw={"channel": "arena.evil"})]])
    bus = FakeBus()
    poller = _poller(lister, bus, channel="arena", channels=_portals())

    assert await poller.poll_once() == 1
    assert bus.published[0][1].channel == "arena"


async def test_an_unsafe_board_default_degrades_to_unchannelled() -> None:
    """With BOTH candidates unsafe the card opens unchannelled, never poisoned.

    Degrading to the pre-#118 unchannelled path keeps intake open (the map
    translates, it does not gate intake) while still refusing to feed the token to
    a subject builder.
    """
    lister = FakeLister([[FakeTask("t-1", "todo", raw={"channel": "*"})]])
    bus = FakeBus()
    poller = _poller(lister, bus, channel=">", channels=empty_map())

    assert await poller.poll_once() == 1
    _, env = bus.published[0]
    assert env.channel is None
    assert env.payload["channel"] == ""


async def test_an_ordinary_unmapped_channel_is_still_stamped_through() -> None:
    """The safety rule must not break the documented fail-open degrade.

    This is the LIVE posture: ``/api/channel-map`` 404s, so the map is empty and
    every card takes the unresolved path. An ordinary slug must still be stamped —
    only subject-UNSAFE values are refused.
    """
    lister = FakeLister([[FakeTask("t-1", "todo", raw={"channel": "skunkworks"})]])
    bus = FakeBus()
    poller = _poller(lister, bus, channel="arena", channels=empty_map())

    assert await poller.poll_once() == 1
    assert bus.published[0][1].channel == "skunkworks"
