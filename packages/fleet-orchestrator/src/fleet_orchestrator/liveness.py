"""Heartbeat consumer — per-agent liveness + effective topology (#26).

Consumes the shared :mod:`fleet_nats.heartbeat` wire shape from the
``agent_heartbeat`` wildcard lane and answers the four questions ``/connz``
cannot: is the agent *connected* (beats arriving), *subscribed* (which lanes,
per its role map — effective state, not config assumption), *working* (a
last-turn timestamp), or *dead/stalled* (no beat for N intervals).

Deterministic by construction: the tracker never reads a clock — callers
supply ``now`` as monotonic seconds (the injected-clock invariant; this module
imports no ``time``/``datetime``). The deploy-time ``/connz`` probe remains the
install gate; this is the continuous runtime signal it cannot provide.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Protocol

from fleet_nats import Envelope, Heartbeat, parse_heartbeat, subjects
from loguru import logger

# An agent is STALE after this many missed intervals (env-tunable by callers).
DEFAULT_MISSED_INTERVALS = 3


@dataclass(frozen=True, slots=True)
class AgentView:
    """The latest known liveness view of one agent.

    Attributes:
        beat: The last parsed heartbeat (identity, role, lanes, seq, last turn).
        last_seen: Caller-supplied monotonic seconds when that beat arrived.
    """

    beat: Heartbeat
    last_seen: float

    def is_stale(self, *, now: float, interval_s: float, missed: int) -> bool:
        """Whether the agent has missed ``missed`` full intervals at ``now``."""
        return (now - self.last_seen) > (interval_s * missed)


class LivenessTracker:
    """Derives per-agent liveness from observed heartbeats alone."""

    def __init__(self) -> None:
        self._agents: dict[str, AgentView] = {}
        # The catalog-version skew-set most recently observed (#103 P2-5). A
        # fleet is ALWAYS skewed mid-rollout, so a per-beat warn is a log storm
        # every heartbeat; this tracks the last version-carrying skew-set and is
        # re-armed (set to ``None``) on a return to consensus. The ACTUAL warn
        # dedup is the per-agent map below — return-to-consensus re-warns only
        # when an agent diverges at a version it was NOT already warned at, so a
        # flap (P3) can't re-arm the warning every cycle. ``None`` = "never seen".
        self._skew_warned: frozenset[int] | None = None
        # Per-agent SET of every catalog version we WARNED about in a skew (#103
        # P3): the flap bound. A flapping emitter dips to consensus every other
        # beat and would re-arm ``_skew_warned`` every cycle — so the
        # once-per-skew-set dedup alone lets it warn once per flap-cycle. Keying
        # dedup on the (agent, version) pairings that make up a skew means a value
        # an agent has ALREADY been warned at is silent on its next flap back.
        # A SET (not a last-value scalar) is required: an emitter oscillating
        # between TWO distinct skewing values (wren 6↔5 vs an anchored finch 7 —
        # BOTH skew) would, with a last-value map, look "newly divergent" on every
        # flip (the map was just overwritten to the other value), re-warning each
        # beat. Accumulating every warned version per agent bounds such a flap to
        # at most once per DISTINCT value it ever holds, matching the docstring.
        # Cleared on fleet CONVERGENCE (all version-carrying agents agree) so a
        # renewed skew after the fleet went uniform warns again (F4) — the flap
        # bound only needs to hold WITHIN an unresolved skew episode.
        self._agent_warned_version: dict[str, set[int]] = {}
        # One-shot marker for the "skew coverage incomplete" rollout signal
        # (#103 P2): live version-carrying beats coexisting with live beats that
        # carry NO catalog version (pre-#103 emitters) — the transition the skew
        # feature exists to observe, where version-only comparison is blind. Set
        # while that condition holds so it announces once, not every beat; cleared
        # when it no longer holds so a later recurrence re-announces.
        self._skew_coverage_incomplete_warned: bool = False

    def observe(
        self,
        envelope: Envelope,
        *,
        now: float,
        interval_s: float | None = None,
        missed: int = DEFAULT_MISSED_INTERVALS,
    ) -> Heartbeat | None:
        """Record one received heartbeat envelope.

        Args:
            envelope: The received envelope from the heartbeat lane.
            now: Caller-supplied monotonic seconds.
            interval_s: The heartbeat cadence, used to exclude STALE agents from
                the skew comparison (#103 P2-6) — a dead agent's ghost version
                must not fire phantom skew after the live fleet is uniform. When
                ``None`` no staleness filter is applied (every tracked view
                counts) — the back-compatible default for callers that don't
                thread staleness.
            missed: Missed-interval threshold for the staleness filter above.

        Returns:
            The parsed :class:`~fleet_nats.Heartbeat`, or ``None`` when the
            envelope is malformed — logged and skipped, never counted as
            liveness and never allowed to crash the consumer.
        """
        try:
            beat = parse_heartbeat(envelope)
        except ValueError as exc:
            logger.warning(
                "orchestrator.liveness | malformed heartbeat sender={} dropped: {}",
                envelope.sender,
                exc,
            )
            return None
        # Key by the SAME normalization the routing subjects use — a raw-id key
        # would let tracker lookups miss agents whose ids the lane slugs.
        key = subjects.slug(beat.agent_id)
        prior = self._agents.get(key)
        if prior is not None and beat.seq < prior.beat.seq:
            # A seq reset means the agent restarted — worth a line, not an error.
            logger.info(
                "orchestrator.liveness | seq reset agent={} {}->{} (restart?)",
                beat.agent_id,
                prior.beat.seq,
                beat.seq,
            )
        self._agents[key] = AgentView(beat=beat, last_seen=now)
        self._observe_skew(beat, key, now=now, interval_s=interval_s, missed=missed)
        logger.debug(
            "orchestrator.liveness | beat agent={} role={} seq={} catalog_version={}",
            beat.agent_id,
            beat.role,
            beat.seq,
            beat.catalog_version,
        )
        return beat

    def _observe_skew(
        self,
        beat: Heartbeat,
        key: str,
        *,
        now: float,
        interval_s: float | None,
        missed: int,
    ) -> None:
        """Surface catalog-version skew across the live fleet (#103 P2-5/6/7 + P2/P3).

        Version skew is a fleet-split hazard: two LIVE agents on different
        catalog versions run mixed taxonomies. This surfaces it loudly, but:

        - **Deduped, flap-bounded** (P2-5 / P3): a fleet is always skewed
          mid-rollout, so warning every beat is a ~30s log storm. Dedup is keyed
          on the per-agent SET of versions each agent has already been warned at
          in a skew — an agent is warned once per DISTINCT version it holds in a
          skew. So an emitter flapping between two distinct skewing values
          (wren 6↔5 while finch is anchored at 7 — BOTH 6 and 5 skew vs 7)
          warns at most twice total (once per value), never once per flap-cycle.
          A genuinely new third distinct skewing value still warns once.
        - **Non-stale only** (P2-6): a dead agent's ghost version is excluded via
          the staleness filter, so it can't fire phantom skew after the live
          fleet went uniform. No filter is applied when ``interval_s is None``.
        - **Self-excluded** (P2-7): the current agent's own PRIOR view is dropped
          from the comparison — only its CURRENT beat's version counts — so a
          lone agent bumping its own version (6→7) diverges from nothing (there
          is no second live agent) and does not warn against its own prior value.
        - **Rollout blind-spot self-announces** (P2): version-only comparison is
          blind to pre-#103 emitters (``catalog_version=None``) — exactly the
          mixed-taxonomy transition this feature exists to observe. When live
          version-carrying beats coexist with live None-carrying beats, a
          distinct one-shot signal fires so the incomplete coverage announces
          itself rather than presenting a false all-clear.
        """
        # The OTHER live agents (self dropped, P2-7; stale dropped, P2-6 when an
        # interval is threaded), split by whether they carry a catalog version.
        other_versions: dict[str, int] = {}
        none_carrying: list[str] = []
        for other_key, view in self._agents.items():
            if other_key == key:
                continue
            if interval_s is not None and view.is_stale(
                now=now, interval_s=interval_s, missed=missed
            ):
                continue
            if view.beat.catalog_version is None:
                none_carrying.append(other_key)
            else:
                other_versions[other_key] = view.beat.catalog_version

        # The combined live version-carrying set = the OTHER version-carrying
        # agents + the current beat's own version (if it carries one).
        versioned = dict(other_versions)
        if beat.catalog_version is not None:
            versioned[key] = beat.catalog_version

        # --- P2: rollout coverage-incomplete signal --------------------------
        # If at least one live agent carries a version AND at least one live agent
        # carries none, version-only skew comparison cannot see the whole fleet.
        # Announce that once (deduped on the condition), so the blind spot during
        # the #103 rollout is self-evident instead of a silent false all-clear.
        current_is_none = beat.catalog_version is None
        any_none = bool(none_carrying) or current_is_none
        coverage_incomplete = bool(versioned) and any_none
        if coverage_incomplete and not self._skew_coverage_incomplete_warned:
            self._skew_coverage_incomplete_warned = True
            n_none = len(none_carrying) + (1 if current_is_none else 0)
            logger.warning(
                "orchestrator.liveness | catalog SKEW COVERAGE INCOMPLETE: {} live "
                "agent(s) report NO catalog version alongside {} versioned agent(s) — "
                "skew detection is blind to pre-#103 emitters during rollout; "
                "'all agents carry a catalog version' is the precondition for "
                "trusting the SKEW signal",
                n_none,
                len(versioned),
            )
        elif not coverage_incomplete:
            # Condition cleared (every live agent now carries a version, or the
            # versioned side emptied) — re-arm so a later recurrence re-announces.
            self._skew_coverage_incomplete_warned = False

        # --- P2-5/6/7 + P3: version-carrying skew ----------------------------
        version_set = frozenset(versioned.values())
        if len(version_set) < 2:
            # Consensus among version-carrying agents → the fleet has CONVERGED
            # to a single version. Re-arm the skew marker AND clear the per-agent
            # warned-version dedup, so a LATER, genuinely-new skew (a renewed
            # divergence after the fleet went uniform) WARNS AGAIN instead of
            # staying silent forever on a value it warned at pre-convergence
            # (terminal finding F4). The flap bound (P3) still holds WITHIN a skew
            # episode: dedup only clears once EVERY version-carrying agent agrees,
            # which a flapper oscillating between two distinct skewing values
            # never reaches (it keeps ≥2 live versions and never converges).
            self._skew_warned = None
            self._agent_warned_version.clear()
            return
        # A genuinely-new skew requires a (agent, version) pairing we have NOT yet
        # warned about — this is the flap bound (P3): a value an agent already
        # warned at stays silent on its flap back. It also subsumes the
        # once-per-skew-set dedup (P2-5): a steady skew re-beats the same
        # pairings and stays quiet.
        newly_divergent = {
            agent_key: version
            for agent_key, version in versioned.items()
            if version not in self._agent_warned_version.get(agent_key, frozenset())
        }
        if not newly_divergent:
            self._skew_warned = version_set
            return
        # Warn, and record the pairings so their flap-back / re-beat is silent.
        # Accumulate into each agent's warned-version SET (never overwrite) so a
        # flap between two distinct skewing values is bounded to one warn each.
        self._skew_warned = version_set
        for agent_key, version in versioned.items():
            self._agent_warned_version.setdefault(agent_key, set()).add(version)
        per_agent: dict[str, int | None] = dict(sorted(other_versions.items()))
        per_agent[beat.agent_id] = beat.catalog_version
        logger.warning(
            "orchestrator.liveness | catalog VERSION SKEW: live agents report {} — "
            "mixed-taxonomy fleet (per-agent: {})",
            sorted(version_set),
            per_agent,
        )

    def view(self, agent_id: str) -> AgentView | None:
        """The latest view of ``agent_id`` (slug-normalized), or ``None``."""
        return self._agents.get(subjects.slug(agent_id))

    def is_stale(
        self,
        agent_id: str,
        *,
        now: float,
        interval_s: float,
        missed: int = DEFAULT_MISSED_INTERVALS,
    ) -> bool | None:
        """Staleness of one agent (``None`` = never seen — unknown, not alive)."""
        agent = self._agents.get(subjects.slug(agent_id))
        if agent is None:
            return None
        return agent.is_stale(now=now, interval_s=interval_s, missed=missed)

    def effective_topology(
        self,
        *,
        now: float,
        interval_s: float,
        missed: int = DEFAULT_MISSED_INTERVALS,
    ) -> dict[str, dict[str, object]]:
        """Who is alive, with which role/lanes — derived from heartbeats alone.

        Returns:
            Per-agent (keyed by slug-normalized id): ``connected`` (not stale),
            ``role``, ``lanes``, ``seq``, ``last_seen``, ``last_turn_ts``,
            ``working`` (has reported at least one turn since start AND is not
            stale — an idle-but-beating agent stays ``working``; beat ``now`` is
            monotonic while turn timestamps are ISO strings, so "recent turn" is
            deliberately not computable here), and ``stale``. The manager loop
            and the parked dashboard (#14) both read this shape.
        """
        return {
            agent_id: {
                "connected": not view.is_stale(now=now, interval_s=interval_s, missed=missed),
                "role": view.beat.role,
                "lanes": list(view.beat.lanes),
                "seq": view.beat.seq,
                "last_seen": view.last_seen,
                "last_turn_ts": view.beat.last_turn_ts,
                "catalog_version": view.beat.catalog_version,
                "working": view.beat.last_turn_ts is not None
                and not view.is_stale(now=now, interval_s=interval_s, missed=missed),
                "stale": view.is_stale(now=now, interval_s=interval_s, missed=missed),
            }
            for agent_id, view in self._agents.items()
        }


class _SubscribeBus(Protocol):
    """The one bus capability the liveness watcher needs."""

    async def subscribe(
        self, subject: str, handler: Callable[[Envelope], Awaitable[None]]
    ) -> object:
        """Subscribe a decoded-envelope handler."""
        ...


async def watch_heartbeats(
    bus: _SubscribeBus,
    tracker: LivenessTracker,
    *,
    env: str,
    now: Callable[[], float],
    interval_s: float | None = None,
    missed: int = DEFAULT_MISSED_INTERVALS,
) -> object:
    """Subscribe to every agent heartbeat and feed ``tracker``.

    Args:
        bus: Connected bus exposing ``subscribe``.
        tracker: The tracker to update from received beats.
        env: Fleet environment prefix.
        now: Caller-supplied monotonic clock.
        interval_s: The heartbeat cadence, threaded into ``observe`` so the skew
            detector can exclude STALE agents' ghost versions (#103 P2-6). When
            ``None`` no staleness filter is applied (back-compat).
        missed: Missed-interval staleness threshold for the filter above.

    Returns:
        The underlying subscription handle returned by the bus.
    """
    subject = subjects.agent_heartbeat_wildcard(env)

    async def _on_heartbeat(envelope: Envelope) -> None:
        tracker.observe(envelope, now=now(), interval_s=interval_s, missed=missed)

    logger.info("orchestrator.liveness | watching subject={}", subject)
    return await bus.subscribe(subject, _on_heartbeat)
