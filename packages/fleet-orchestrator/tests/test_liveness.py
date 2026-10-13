"""Tests for heartbeat liveness derivation and the cross-package wire contract (#26)."""

from __future__ import annotations

from collections.abc import Awaitable, Callable

from fleet_nats import Envelope, heartbeat_envelope, subjects
from fleet_subscriber.heartbeat import HeartbeatEmitter

from fleet_orchestrator.liveness import LivenessTracker, watch_heartbeats


class _RecordingBus:
    def __init__(self) -> None:
        self.published: list[tuple[str, Envelope]] = []
        self.subscriptions: list[tuple[str, Callable[[Envelope], Awaitable[None]]]] = []

    async def publish(self, subject: str, envelope: Envelope) -> None:
        self.published.append((subject, envelope))

    async def subscribe(
        self, subject: str, handler: Callable[[Envelope], Awaitable[None]]
    ) -> object:
        self.subscriptions.append((subject, handler))
        return "subscription"


def _heartbeat(
    *,
    agent_id: str = "bilby",
    seq: int = 1,
    last_turn_ts: str | None = "2026-07-08T00:00:01Z",
    catalog_version: int | None = None,
) -> Envelope:
    return heartbeat_envelope(
        msg_id=f"hb-{seq}",
        ts="2026-07-08T00:00:00Z",
        sender=agent_id,
        role="worker",
        lanes=("dispatch", "relay", "wake", "inbox"),
        seq=seq,
        last_turn_ts=last_turn_ts,
        catalog_version=catalog_version,
    )


def test_observe_records_per_agent_view_and_effective_topology() -> None:
    tracker = LivenessTracker()
    beat = tracker.observe(_heartbeat(), now=10.0)

    assert beat is not None
    assert tracker.view("bilby") is not None
    assert tracker.is_stale("bilby", now=39.0, interval_s=10.0, missed=3) is False
    topology = tracker.effective_topology(now=39.0, interval_s=10.0, missed=3)
    assert topology == {
        "bilby": {
            "connected": True,
            "role": "worker",
            "lanes": ["dispatch", "relay", "wake", "inbox"],
            "seq": 1,
            "last_seen": 10.0,
            "last_turn_ts": "2026-07-08T00:00:01Z",
            "catalog_version": None,
            "working": True,
            "stale": False,
        }
    }


def test_staleness_is_false_at_threshold_and_true_after_n_missed_intervals() -> None:
    tracker = LivenessTracker()
    tracker.observe(_heartbeat(last_turn_ts=None), now=100.0)

    assert tracker.is_stale("bilby", now=130.0, interval_s=10.0, missed=3) is False
    assert tracker.is_stale("bilby", now=130.001, interval_s=10.0, missed=3) is True
    assert tracker.effective_topology(now=130.001, interval_s=10.0, missed=3)["bilby"] == {
        "connected": False,
        "role": "worker",
        "lanes": ["dispatch", "relay", "wake", "inbox"],
        "seq": 1,
        "last_seen": 100.0,
        "last_turn_ts": None,
        "catalog_version": None,
        "working": False,
        "stale": True,
    }


def test_malformed_heartbeat_is_logged_and_not_counted() -> None:
    tracker = LivenessTracker()
    malformed = Envelope.new(msg_id="bad", ts="t", sender="bilby", kind="status", payload={})

    assert tracker.observe(malformed, now=1.0) is None
    assert tracker.view("bilby") is None
    assert tracker.is_stale("bilby", now=1.0, interval_s=10.0) is None


def test_tracker_keys_by_subject_slug_normalization() -> None:
    # The routing lane slugs the id ("Foo Bar" -> "foo-bar"); the tracker must
    # key the same way or lookups by lane identity miss the agent.
    tracker = LivenessTracker()
    env = heartbeat_envelope(
        msg_id="hb-slug",
        ts="2026-07-08T00:00:00Z",
        sender="Foo Bar",
        role="worker",
        lanes=("inbox",),
        seq=1,
        last_turn_ts=None,
    )
    assert tracker.observe(env, now=1.0) is not None
    assert tracker.view("foo-bar") is not None
    assert tracker.view("Foo Bar") is not None
    assert tracker.is_stale("foo-bar", now=1.0, interval_s=10.0) is False
    assert list(tracker.effective_topology(now=1.0, interval_s=10.0)) == ["foo-bar"]


def test_seq_reset_is_accepted_as_a_fresh_beat() -> None:
    tracker = LivenessTracker()
    tracker.observe(_heartbeat(seq=5), now=10.0)
    tracker.observe(_heartbeat(seq=1), now=11.0)

    view = tracker.view("bilby")
    assert view is not None
    assert view.beat.seq == 1
    assert view.last_seen == 11.0


def test_effective_topology_surfaces_catalog_version() -> None:
    """Version skew is observable on the bus (#103): the version each agent
    connected with appears in the topology snapshot operators read."""
    tracker = LivenessTracker()
    tracker.observe(_heartbeat(agent_id="bilby", catalog_version=7), now=10.0)
    topo = tracker.effective_topology(now=11.0, interval_s=10.0)
    assert topo["bilby"]["catalog_version"] == 7


def test_version_skew_across_agents_logs_a_warning() -> None:
    """A beat whose catalog version differs from an already-observed agent's is
    a mixed-taxonomy hazard — surfaced loudly the moment it's observable."""
    from loguru import logger

    records: list[tuple[str, str]] = []
    sink_id = logger.add(
        lambda m: records.append((m.record["level"].name, m.record["message"])),
        level="DEBUG",
        filter="fleet_orchestrator.liveness",
    )
    try:
        tracker = LivenessTracker()
        tracker.observe(_heartbeat(agent_id="bilby", catalog_version=7), now=10.0)
        tracker.observe(_heartbeat(agent_id="skippy", catalog_version=6), now=11.0)
        assert any(
            level == "WARNING" and "VERSION SKEW" in message for level, message in records
        ), records
    finally:
        logger.remove(sink_id)


def test_matching_versions_do_not_warn() -> None:
    from loguru import logger

    records: list[tuple[str, str]] = []
    sink_id = logger.add(
        lambda m: records.append((m.record["level"].name, m.record["message"])),
        level="DEBUG",
        filter="fleet_orchestrator.liveness",
    )
    try:
        tracker = LivenessTracker()
        tracker.observe(_heartbeat(agent_id="bilby", catalog_version=7), now=10.0)
        tracker.observe(_heartbeat(agent_id="skippy", catalog_version=7), now=11.0)
        assert not any("VERSION SKEW" in message for _, message in records), records
    finally:
        logger.remove(sink_id)


def _skew_sink() -> tuple[list[tuple[str, str]], int]:
    from loguru import logger

    records: list[tuple[str, str]] = []
    sink_id = logger.add(
        lambda m: records.append((m.record["level"].name, m.record["message"])),
        level="DEBUG",
        filter="fleet_orchestrator.liveness",
    )
    return records, sink_id


def _skew_warnings(records: list[tuple[str, str]]) -> list[str]:
    return [msg for level, msg in records if level == "WARNING" and "VERSION SKEW" in msg]


def test_sustained_skew_warns_once_not_per_beat() -> None:
    """A fleet is ALWAYS skewed mid-rollout: warning per heartbeat is a log storm
    (#103 P2-5). Sustained skew must warn ONCE, not on every beat."""
    from loguru import logger

    records, sink_id = _skew_sink()
    try:
        tracker = LivenessTracker()
        tracker.observe(_heartbeat(agent_id="bilby", catalog_version=7), now=10.0)
        # skippy on v6 diverges: ONE warning. Then many more beats at the SAME
        # skew-set must NOT re-warn.
        for i, now in enumerate((11.0, 12.0, 13.0, 14.0, 15.0)):
            agent = "skippy" if i % 2 == 0 else "bilby"
            version = 6 if agent == "skippy" else 7
            tracker.observe(
                _heartbeat(agent_id=agent, seq=10 + i, catalog_version=version), now=now
            )
        assert len(_skew_warnings(records)) == 1, records
    finally:
        logger.remove(sink_id)


def test_skew_re_warns_on_a_new_divergent_version_then_stays_quiet() -> None:
    """A CHANGE in the skew-set re-arms the warning (a genuinely new divergence),
    but the new steady-state stays quiet again (#103 P2-5)."""
    from loguru import logger

    records, sink_id = _skew_sink()
    try:
        tracker = LivenessTracker()
        tracker.observe(_heartbeat(agent_id="bilby", catalog_version=7), now=10.0)
        tracker.observe(_heartbeat(agent_id="skippy", catalog_version=6), now=11.0)  # warn 1
        # A THIRD agent introduces version 5 — a NEW skew-set → warn again.
        tracker.observe(_heartbeat(agent_id="nagatha", catalog_version=5), now=12.0)  # warn 2
        # Re-beat the same three: same skew-set → no more warnings.
        tracker.observe(_heartbeat(agent_id="bilby", seq=2, catalog_version=7), now=13.0)
        tracker.observe(_heartbeat(agent_id="skippy", seq=2, catalog_version=6), now=14.0)
        assert len(_skew_warnings(records)) == 2, records
    finally:
        logger.remove(sink_id)


def test_return_to_consensus_then_new_version_re_arms_the_skew_warning() -> None:
    """After the fleet converges and later diverges at a NEW version, the warning
    re-fires — the version-carrying skew marker clears on consensus (#103 P2-5).
    A flap between two DISTINCT skewing values never converges and stays bounded —
    see ``test_flap_between_two_distinct_skewing_values_is_bounded_to_two``."""
    from loguru import logger

    records, sink_id = _skew_sink()
    try:
        tracker = LivenessTracker()
        tracker.observe(_heartbeat(agent_id="bilby", catalog_version=7), now=10.0)
        tracker.observe(_heartbeat(agent_id="skippy", catalog_version=6), now=11.0)  # warn 1
        # skippy upgrades to 7 → consensus (one version) → skew marker clears.
        tracker.observe(_heartbeat(agent_id="skippy", seq=2, catalog_version=7), now=12.0)
        # A fresh divergence at a NEW version (skippy has never been warned at 5)
        # after consensus must warn again.
        tracker.observe(_heartbeat(agent_id="skippy", seq=3, catalog_version=5), now=13.0)  # warn 2
        assert len(_skew_warnings(records)) == 2, records
    finally:
        logger.remove(sink_id)


def test_flapping_through_the_anchor_re_warns_each_re_divergence() -> None:
    """An emitter flapping THROUGH the anchor value (6↔7 while bilby=7) hits
    consensus (single version = 7) on every 7-beat. Terminal finding F4 requires
    the per-agent warned-version dedup to CLEAR on convergence so a later renewed
    skew warns again — which necessarily means each re-divergence back to 6 after
    a consensus-touch re-warns.

    This SUPERSEDES the earlier once-per-distinct-value flap bound for the
    through-anchor case (the old contract kept 6 in skippy's warned set forever
    and stayed silent). The tradeoff is accepted deliberately: a genuinely
    renewed skew after the fleet went uniform MUST be visible, and an agent that
    keeps dipping to consensus and re-diverging IS repeatedly renewing a skew.
    The distinct-value flap bound WITHIN an unresolved episode still holds — see
    ``test_flap_between_two_distinct_skewing_values_is_bounded_to_two`` (a flapper
    between two skewing values never converges, so its dedup never clears)."""
    from loguru import logger

    records, sink_id = _skew_sink()
    try:
        tracker = LivenessTracker()
        tracker.observe(_heartbeat(agent_id="bilby", catalog_version=7), now=10.0)
        # skippy flaps 6,7,6,7,6,7. Each 6 diverges from bilby's 7 (warn); each 7
        # is consensus (dedup clears). Three 6-beats → three warnings.
        for i, version in enumerate((6, 7, 6, 7, 6, 7)):
            tracker.observe(
                _heartbeat(agent_id="skippy", seq=2 + i, catalog_version=version),
                now=11.0 + i,
            )
        assert len(_skew_warnings(records)) == 3, records
    finally:
        logger.remove(sink_id)


def test_flap_between_two_distinct_skewing_values_is_bounded_to_two() -> None:
    """Regression (#103 P3): an emitter flapping between TWO DISTINCT values that
    BOTH skew against an anchor (skippy 6↔5 while bilby stays 7 — 6≠7 AND 5≠7, so
    NEITHER flip is consensus) must warn at most TWICE total (once per distinct
    value), NOT once per beat.

    A last-value ``dict[str, int]`` bound was insufficient here: each flip
    overwrote the map to the OTHER value, so the flap-back always looked "newly
    divergent" and re-warned every beat (8 beats → 8 warnings, empirically). The
    per-agent version SET fixes it. A genuinely new THIRD distinct value still
    warns once."""
    from loguru import logger

    records, sink_id = _skew_sink()
    try:
        tracker = LivenessTracker()
        tracker.observe(_heartbeat(agent_id="bilby", catalog_version=7), now=10.0)
        # skippy flaps 6,5,6,5,6,5,6,5 — every value skews vs bilby's anchored 7,
        # so there is NO consensus dip. Only the FIRST 6 and the FIRST 5 are new
        # distinct divergent values; every later flip is a value skippy was already
        # warned at → silent. Two warnings total, not eight.
        now = 11.0
        for i in range(8):
            version = 6 if i % 2 == 0 else 5
            tracker.observe(
                _heartbeat(agent_id="skippy", seq=2 + i, catalog_version=version), now=now
            )
            # keep bilby fresh so it never goes stale and drops from the compare
            now += 0.5
            tracker.observe(_heartbeat(agent_id="bilby", seq=2 + i, catalog_version=7), now=now)
            now += 0.5
        assert len(_skew_warnings(records)) == 2, records
        # A genuinely-new THIRD distinct skewing value warns exactly once more.
        tracker.observe(_heartbeat(agent_id="skippy", seq=99, catalog_version=4), now=now)
        assert len(_skew_warnings(records)) == 3, records
    finally:
        logger.remove(sink_id)


def test_skew_ignores_stale_dead_agents_ghost_version() -> None:
    """A dead agent's last version must NOT fire phantom skew after the live fleet
    is uniform (#103 P2-6) — the staleness filter excludes it."""
    from loguru import logger

    records, sink_id = _skew_sink()
    try:
        tracker = LivenessTracker()
        # bilby beats v6 at t=10 then goes silent (dies).
        tracker.observe(
            _heartbeat(agent_id="bilby", catalog_version=6),
            now=10.0,
            interval_s=10.0,
            missed=3,
        )
        # Much later, skippy beats v7. bilby is now stale (>3*10s since its beat),
        # so its ghost v6 must be excluded — the LIVE fleet is uniform (just skippy).
        tracker.observe(
            _heartbeat(agent_id="skippy", catalog_version=7),
            now=100.0,
            interval_s=10.0,
            missed=3,
        )
        assert _skew_warnings(records) == [], records
    finally:
        logger.remove(sink_id)


def test_skew_does_not_self_trigger_on_a_lone_agent_bump() -> None:
    """A lone agent upgrading 6→7 must NOT warn against its own prior value
    (#103 P2-7) — skew needs two DISTINCT live agents."""
    from loguru import logger

    records, sink_id = _skew_sink()
    try:
        tracker = LivenessTracker()
        tracker.observe(_heartbeat(agent_id="bilby", seq=1, catalog_version=6), now=10.0)
        tracker.observe(_heartbeat(agent_id="bilby", seq=2, catalog_version=7), now=11.0)
        assert _skew_warnings(records) == [], records
    finally:
        logger.remove(sink_id)


def test_skew_none_and_int_coexist_no_false_skew_either_ordering() -> None:
    """Mixed None/int coexistence is the real rollout (an old beat with no version
    field alongside a new one carrying it): a None never triggers skew against an
    int, in either arrival order (#103 P3-10)."""
    from loguru import logger

    # int first, then None.
    records, sink_id = _skew_sink()
    try:
        tracker = LivenessTracker()
        tracker.observe(_heartbeat(agent_id="bilby", catalog_version=7), now=10.0)
        tracker.observe(_heartbeat(agent_id="skippy", catalog_version=None), now=11.0)
        assert _skew_warnings(records) == [], records
    finally:
        logger.remove(sink_id)

    # None first, then int.
    records, sink_id = _skew_sink()
    try:
        tracker = LivenessTracker()
        tracker.observe(_heartbeat(agent_id="bilby", catalog_version=None), now=10.0)
        tracker.observe(_heartbeat(agent_id="skippy", catalog_version=7), now=11.0)
        assert _skew_warnings(records) == [], records
    finally:
        logger.remove(sink_id)


def _coverage_warnings(records: list[tuple[str, str]]) -> list[str]:
    return [msg for level, msg in records if level == "WARNING" and "COVERAGE INCOMPLETE" in msg]


def test_coverage_incomplete_signal_fires_for_none_plus_versioned_fleet() -> None:
    """The crux of P2: version-only skew is BLIND during the #103 rollout — a
    pre-#103 emitter (catalog_version=None) alongside a versioned agent produces
    no VERSION SKEW, so the blind spot must SELF-ANNOUNCE via a distinct one-shot
    'coverage incomplete' signal (once, deduped), in either arrival order."""
    from loguru import logger

    # versioned first, then None.
    records, sink_id = _skew_sink()
    try:
        tracker = LivenessTracker()
        tracker.observe(_heartbeat(agent_id="bilby", catalog_version=7), now=10.0, interval_s=10.0)
        tracker.observe(
            _heartbeat(agent_id="skippy", catalog_version=None), now=11.0, interval_s=10.0
        )
        assert _skew_warnings(records) == [], records  # NOT a version skew
        assert len(_coverage_warnings(records)) == 1, records  # but coverage IS incomplete
    finally:
        logger.remove(sink_id)

    # None first, then versioned — same outcome regardless of order.
    records, sink_id = _skew_sink()
    try:
        tracker = LivenessTracker()
        tracker.observe(
            _heartbeat(agent_id="bilby", catalog_version=None), now=10.0, interval_s=10.0
        )
        tracker.observe(_heartbeat(agent_id="skippy", catalog_version=7), now=11.0, interval_s=10.0)
        assert _skew_warnings(records) == [], records
        assert len(_coverage_warnings(records)) == 1, records
    finally:
        logger.remove(sink_id)


def test_coverage_incomplete_signal_is_deduped_then_re_arms() -> None:
    """The coverage signal fires ONCE while the None+versioned condition holds
    (not per beat), then re-arms once every live agent carries a version so a
    later recurrence re-announces (#103 P2)."""
    from loguru import logger

    records, sink_id = _skew_sink()
    try:
        tracker = LivenessTracker()
        tracker.observe(_heartbeat(agent_id="bilby", catalog_version=7), now=10.0, interval_s=10.0)
        # skippy beats None twice: ONE coverage warning, not per beat.
        tracker.observe(
            _heartbeat(agent_id="skippy", seq=1, catalog_version=None), now=11.0, interval_s=10.0
        )
        tracker.observe(
            _heartbeat(agent_id="skippy", seq=2, catalog_version=None), now=12.0, interval_s=10.0
        )
        assert len(_coverage_warnings(records)) == 1, records
        # skippy upgrades to a version → coverage now complete → marker re-arms.
        tracker.observe(
            _heartbeat(agent_id="skippy", seq=3, catalog_version=7), now=13.0, interval_s=10.0
        )
        # A NEW pre-#103 agent appears → coverage incomplete again → re-announce.
        tracker.observe(
            _heartbeat(agent_id="nagatha", catalog_version=None), now=14.0, interval_s=10.0
        )
        assert len(_coverage_warnings(records)) == 2, records
    finally:
        logger.remove(sink_id)


def test_no_coverage_signal_when_every_live_agent_carries_a_version() -> None:
    """A fleet where every live agent carries a version has COMPLETE coverage —
    no coverage-incomplete signal (only real skew, if versions differ) (#103 P2)."""
    from loguru import logger

    records, sink_id = _skew_sink()
    try:
        tracker = LivenessTracker()
        tracker.observe(_heartbeat(agent_id="bilby", catalog_version=7), now=10.0, interval_s=10.0)
        tracker.observe(_heartbeat(agent_id="skippy", catalog_version=6), now=11.0, interval_s=10.0)
        assert _coverage_warnings(records) == [], records
        assert len(_skew_warnings(records)) == 1, records  # a real version skew instead
    finally:
        logger.remove(sink_id)


async def test_heartbeat_contract_real_emitter_real_consumer_one_bus() -> None:
    bus = _RecordingBus()
    ids = iter(["h1"])
    times = iter(["2026-07-08T00:00:00Z"])
    emitter = HeartbeatEmitter(
        bus,
        agent_id="skippy",
        env="dev",
        role="worker",
        lanes=("dispatch", "relay", "wake", "inbox"),
        new_id=lambda: next(ids),
        now=lambda: next(times),
    )
    tracker = LivenessTracker()

    await emitter.beat()
    subject, envelope = bus.published[0]
    assert subject == subjects.agent_heartbeat("dev", "skippy")
    assert tracker.observe(envelope, now=5.0) is not None
    assert tracker.effective_topology(now=5.0, interval_s=30.0) == {
        "skippy": {
            "connected": True,
            "role": "worker",
            "lanes": ["dispatch", "relay", "wake", "inbox"],
            "seq": 1,
            "last_seen": 5.0,
            "last_turn_ts": None,
            "catalog_version": None,
            "working": False,
            "stale": False,
        }
    }


async def test_watch_heartbeats_subscribes_wildcard_and_feeds_tracker() -> None:
    bus = _RecordingBus()
    tracker = LivenessTracker()

    handle = await watch_heartbeats(bus, tracker, env="dev", now=lambda: 42.0)
    assert handle == "subscription"
    subject, handler = bus.subscriptions[0]
    assert subject == subjects.agent_heartbeat_wildcard("dev")

    await handler(_heartbeat(agent_id="nagatha", seq=2))
    view = tracker.view("nagatha")
    assert view is not None
    assert view.last_seen == 42.0
    assert view.beat.seq == 2


def test_convergence_clears_dedup_so_a_renewed_same_value_skew_re_warns() -> None:
    """Terminal finding F4: after the fleet CONVERGES, the per-agent warned-version
    dedup is cleared, so a LATER divergence at the SAME version an agent was warned
    at pre-convergence warns AGAIN — not silent forever.

    This is the case the pre-existing flap bound (P3) deliberately suppressed:
    without the convergence-clear, ``skippy@6`` warns once, and after skippy
    upgrades to 7 (consensus) and LATER regresses to 6, the old dedup kept 6 in
    skippy's warned set and stayed silent — masking a genuine renewed skew."""
    from loguru import logger

    records, sink_id = _skew_sink()
    try:
        tracker = LivenessTracker()
        tracker.observe(_heartbeat(agent_id="bilby", catalog_version=7), now=10.0)
        tracker.observe(_heartbeat(agent_id="skippy", catalog_version=6), now=11.0)  # warn 1
        # Fleet converges: skippy upgrades to 7 (single version → consensus).
        tracker.observe(_heartbeat(agent_id="skippy", seq=2, catalog_version=7), now=12.0)
        # keep bilby fresh so it stays in the compare.
        tracker.observe(_heartbeat(agent_id="bilby", seq=2, catalog_version=7), now=13.0)
        # LATER, skippy regresses to the SAME value (6) it was warned at before.
        # Post-convergence the dedup is clear → this renewed skew warns AGAIN.
        tracker.observe(_heartbeat(agent_id="skippy", seq=3, catalog_version=6), now=14.0)  # warn 2
        assert len(_skew_warnings(records)) == 2, records
    finally:
        logger.remove(sink_id)
