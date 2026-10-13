"""Service configuration for the Phase-9 orchestrator hub — fail-loud, env-driven.

The run-loop (:mod:`fleet_orchestrator.loop`) is a long-lived service, not a
pure module, so it is the one place that reads a wall/monotonic clock and
aggregates the env knobs the shipped modules already own individually
(:func:`~fleet_orchestrator.governor.Budget.from_env`,
:func:`~fleet_orchestrator.offline.timeout_from_env`,
:func:`~fleet_orchestrator.score.circlejerk_n_from_env`,
:func:`~fleet_nats.BusConfig.from_env`).

This module adds ONLY the service-level knobs the loop itself needs and does the
fail-loud validation at startup (the coding-standard: a service must not start
with missing/typo'd config and discover it at traffic time). It introduces no
new coordination behavior — every value flows into an existing shipped module.
"""

from __future__ import annotations

import math
import os
from dataclasses import dataclass, replace

from fleet_nats import BusConfig

from fleet_orchestrator.fleet_liveness import (
    consecutive_from_env,
    enabled_from_env,
    expected_from_env,
    sweep_seconds_from_env,
)
from fleet_orchestrator.governor import Budget
from fleet_orchestrator.governor_store import durable_enabled_from_env
from fleet_orchestrator.intake import OFFLINE_KEY_SEP
from fleet_orchestrator.intake_poller import (
    intake_enabled_from_env,
    intake_label_from_env,
    intake_poll_from_env,
)
from fleet_orchestrator.killswitch import kill_poll_from_env
from fleet_orchestrator.offline import timeout_from_env
from fleet_orchestrator.score import circlejerk_n_from_env

# Service-level env knobs the loop owns (the per-module knobs live in their own
# modules and are read via their own from_env helpers — never duplicated here).
_ENV_MANAGER = "FLEET_MANAGER_ID"
_ENV_WORKERS = "FLEET_WORKERS"
_ENV_OFFLINE_POLL = "FLEET_OFFLINE_POLL_SECONDS"
_ENV_HEARTBEAT_INTERVAL = "FLEET_HEARTBEAT_INTERVAL_SECONDS"
_ENV_ARBITRATION_ENABLED = "FLEET_ARBITRATION_ENABLED"
_ENV_ARBITRATE_TOPICS = "FLEET_ARBITRATE_TOPICS"
# The Discord channel slug the intake poller stamps on a card it opens. The
# poller sees only board rows, not a per-card channel, so a single configured
# default carries onto every poller-opened session. Optional — an empty default
# yields an unchannelled session, matching the pre-poller --once path.
_ENV_INTAKE_CHANNEL = "FLEET_INTAKE_CHANNEL"
# The env var name of the required card-scope marker (fleet-bus #54). Owned/read
# by intake_poller.intake_label_from_env; named here only for the fail-closed
# error message so an operator sees exactly what to set.
_ENV_INTAKE_LABEL = "FLEET_INTAKE_LABEL"
# The fleet-liveness roster env names (fleet-bus #78). The values are owned/read
# by fleet_liveness.*_from_env; named here only for the fail-closed error message
# (an enabled sweep with no roster) so an operator sees exactly what to set.
_ENV_LIVENESS_ENABLED = "FLEET_LIVENESS_ENABLED"
_ENV_EXPECTED_AGENTS = "FLEET_EXPECTED_AGENTS"

# Arbitration responder is ON by default (Rung 8's arbitrate leg must be
# service-coverable, not hand-served). It only stands up a responder per topic
# named in FLEET_ARBITRATE_TOPICS, so an enabled hub with no topics is a no-op —
# opt-in per topic, but the switch fails closed only when explicitly disabled.
_DEFAULT_ARBITRATION_ENABLED = True

# Recognised truthy/falsy spellings for a bool env var. Anything else is a typo
# and refuses to start (a policy switch must never silently default the wrong way).
_TRUE_TOKENS = frozenset({"1", "true", "yes", "on"})
_FALSE_TOKENS = frozenset({"0", "false", "no", "off"})

# The offline detector's ladder is only advanced when the loop calls check(); the
# poll cadence is how often it does so. Default well under the dispatch timeout so
# a rung fires within one window of the silence crossing it.
_DEFAULT_OFFLINE_POLL_S = 30.0
# Must match the subscriber's heartbeat emitter cadence so staleness math lines up
# (fleet_subscriber.heartbeat.interval_from_env default). Read here for the
# is_stale cross-check the offline ladder consumes.
_DEFAULT_HEARTBEAT_INTERVAL_S = 30.0


def _float_env(var: str, default: float) -> float:
    """Read a positive-finite float env var; fail loud on junk (a typo is policy-breaking)."""
    raw = os.environ.get(var)
    if raw is None:
        return default
    try:
        value = float(raw)
    except ValueError as exc:
        raise ValueError(f"{var} must be a number, got {raw!r}") from exc
    if not math.isfinite(value):
        raise ValueError(f"{var} must be finite, got {value}")
    if value <= 0:
        raise ValueError(f"{var} must be positive, got {value}")
    return value


def _bool_env(var: str, default: bool) -> bool:
    """Read a bool env var; fail loud on an unrecognised spelling (a typo is policy-breaking).

    A policy switch (arbitration on/off) must never silently coerce a typo to a
    default — ``FLEET_ARBITRATION_ENABLED=flase`` should refuse to start, not
    quietly leave arbitration on. Only the known truthy/falsy tokens are accepted.
    """
    raw = os.environ.get(var)
    if raw is None:
        return default
    token = raw.strip().lower()
    if token in _TRUE_TOKENS:
        return True
    if token in _FALSE_TOKENS:
        return False
    raise ValueError(f"{var} must be one of {sorted(_TRUE_TOKENS | _FALSE_TOKENS)}, got {raw!r}")


def _topics_env(var: str) -> tuple[str, ...]:
    """Read a comma-separated arbitration-topic list; empty/unset yields no topics.

    Unlike the worker list this is optional (arbitration is opt-in per topic), so
    an unset var is not an error — it just stands up no responders.
    """
    raw = os.environ.get(var)
    if not raw:
        return ()
    return tuple(t.strip() for t in raw.split(",") if t.strip())


def _workers_env(var: str) -> tuple[str, ...]:
    """Read a comma-separated worker list; fail loud when empty (no one to dispatch to)."""
    raw = os.environ.get(var)
    if not raw:
        raise ValueError(
            f"{var} is required (comma-separated worker agent ids, e.g. 'skippy,bilby')"
        )
    workers = tuple(w.strip() for w in raw.split(",") if w.strip())
    if not workers:
        raise ValueError(f"{var} must name at least one worker, got {raw!r}")
    # A worker id containing the offline-key separator would corrupt the
    # per-worker offline watch key (unkey mis-splits it). Reject fail-loud at the
    # config boundary rather than silently mis-key a watch (codex cross-review #4).
    bad = [w for w in workers if OFFLINE_KEY_SEP in w]
    if bad:
        raise ValueError(f"{var} worker ids must not contain {OFFLINE_KEY_SEP!r}, got {bad}")
    return workers


@dataclass(frozen=True)
class HubConfig:
    """Validated configuration for one orchestrator hub process.

    Attributes:
        bus: The fleet bus connection config (identity, url, env, connection
            name) — from :func:`~fleet_nats.BusConfig.from_env`.
        manager: The manager agent id whose identity the hub coordinates as and
            whose inbox the single narrator posts to (e.g. ``nagatha``).
        workers: The worker agent ids a new task fans out to (e.g. skippy, bilby).
        budget: The per-task back-channel caps (governor).
        dispatch_timeout_s: The offline-ladder silence window per rung.
        offline_poll_s: How often the loop advances the offline ladder.
        heartbeat_interval_s: The heartbeat cadence used for staleness math.
        circlejerk_n: The scorekeeper's consecutive-agreement threshold.
        arbitration_enabled: Whether the hub stands up arbitration responders at
            start (default on; a responder is only served per configured topic).
        arbitrate_topics: The arbitration topics the hub serves a responder for.
            Empty means "no responder" even when arbitration is enabled (opt-in
            per topic).
        intake_poll_enabled: Whether the k-board intake poller runs. It is the
            only shipped publisher of the ``created`` event, so with it off the
            durable service never auto-opens a session from a new card (fleet-bus
            #44); an operator disables it only when a real k-board webhook
            publishes ``created`` directly, or for a ``--once``-only smoke posture.
        intake_poll_s: How often the intake poller lists the board for new cards.
        intake_channel: The Discord channel slug stamped on a poller-opened card
            (empty → unchannelled, matching the pre-poller ``--once`` path).
        intake_label: The card-scope marker (fleet-bus #54) — the poller opens
            ONLY ``todo`` cards whose title starts with it, so a board holding
            foreign backlog is safe to poll. REQUIRED whenever the poller is
            enabled: an enabled-but-unmarked poller refuses to start (fail closed)
            rather than grabbing every card, as the fleet-hub first-enable did. Empty
            when the poller is disabled (the marker is then irrelevant).
        fleet_liveness_enabled: Whether the task-independent fleet-liveness roster
            sweep runs (fleet-bus #78). Off by first-deploy default — an
            unconfigured hub never sweeps. The sweep reuses the shipped heartbeat
            :class:`~fleet_orchestrator.liveness.LivenessTracker` to narrate an
            IDLE expected agent going dark (the task-scoped offline ladder cannot
            see it). Report-only: no wake/redispatch.
        fleet_expected: The expected roster the sweep checks (e.g.
            skippy,bilby,nagatha). REQUIRED whenever the sweep is enabled: an
            enabled-but-rosterless sweep refuses to start (fail closed), mirroring
            the intake label. Empty when the sweep is disabled.
        fleet_liveness_consecutive: Consecutive silent sweeps before an expected
            agent is reported MISSING (flap suppression).
        fleet_liveness_sweep_s: How often the loop advances the roster sweep.
        kill_switch_poll_s: How often the kill-switch watcher checks the durable store. The
            kill lands within one interval of the file appearing, so this bounds
            kill latency (default well under the #107 5-second target).
        governor_durable: Whether the governor persists/resumes its budget counters
            via the fleet-nats JetStream KV CAS store (fleet-bus #106). Default
            ``False`` = today's in-memory behavior. IDENTITY-GATED — enabling it is
            blocked on identity stabilization (T5 + u7/#99) and is an operator, not
            a merge, decision.
    """

    bus: BusConfig
    manager: str
    workers: tuple[str, ...]
    budget: Budget
    dispatch_timeout_s: float
    offline_poll_s: float
    heartbeat_interval_s: float
    circlejerk_n: int
    arbitration_enabled: bool
    arbitrate_topics: tuple[str, ...]
    intake_poll_enabled: bool
    intake_poll_s: float
    intake_channel: str
    intake_label: str
    # Fleet-liveness sweep (fleet-bus #78). Defaulted so an unconfigured hub — and
    # existing test/config constructions that predate the sweep — are a no-op.
    fleet_liveness_enabled: bool = False
    fleet_expected: tuple[str, ...] = ()
    fleet_liveness_consecutive: int = 3
    fleet_liveness_sweep_s: float = 30.0
    # Fleet-wide durable-store kill authority (fleet-bus #107). Every component
    # resolves the same FleetKV key; no host-local fallback exists.
    kill_switch_poll_s: float = 1.0
    # Durable governor state (fleet-bus #106). Defaulted OFF so an unconfigured hub
    # — and every test/config construction that predates it — is byte-for-byte
    # today's in-memory governor. Enabling the cutover is IDENTITY-GATED (blocked on
    # T5 + u7/#99): state persisted against unstable identities is corrupt state, so
    # turning this on is an operator decision made only once identity is stable, NOT
    # a merge decision. See fleet_orchestrator.governor_store.
    governor_durable: bool = False

    def __post_init__(self) -> None:
        if not self.manager:
            raise ValueError("HubConfig.manager must be a non-empty agent id")
        if not self.workers:
            raise ValueError("HubConfig.workers must name at least one worker")
        # Fail closed (fleet-bus #54): an ENABLED poller with no card-scope marker
        # would open every ``todo`` card on the board (the fleet-hub first-enable
        # grabbed 8 unrelated backlog cards). Refuse to start rather than run
        # unscoped. A disabled poller needs no marker.
        if self.intake_poll_enabled and not self.intake_label:
            raise ValueError(
                f"{_ENV_INTAKE_LABEL} is required when the intake poller is enabled "
                f"(the card-scope marker; an unscoped poller would open every card on the "
                f"board — fleet-bus #54). Set it to a title prefix (e.g. '[fleet]'), or "
                f"disable the poller with FLEET_INTAKE_POLL_ENABLED=false."
            )
        # Fail closed (fleet-bus #78): an ENABLED fleet-liveness sweep with no
        # expected roster has nothing to check — refuse to start rather than run a
        # useless no-op sweep, mirroring the intake-label fail-closed above.
        if self.fleet_liveness_enabled and not self.fleet_expected:
            raise ValueError(
                f"{_ENV_EXPECTED_AGENTS} is required when the fleet-liveness sweep is enabled "
                f"(the expected roster; an enabled sweep with no agents has nothing to check "
                f"— fleet-bus #78). Set it to a comma list (e.g. 'skippy,bilby,nagatha'), or "
                f"disable the sweep with {_ENV_LIVENESS_ENABLED}=false."
            )

    @classmethod
    def from_env(cls) -> HubConfig:
        """Build the hub config from the environment, fail-loud on any bad value.

        Every knob is read through its owning module's helper (or validated here
        for the service-level knobs) so a missing/typo'd value refuses to start
        the service rather than silently defaulting at traffic time.

        Raises:
            ValueError: On any missing-required or malformed env value — the
                aggregate of every module's own fail-loud validation, surfaced at
                startup where the operator can see it.
        """
        bus = BusConfig.from_env()
        manager = os.environ.get(_ENV_MANAGER, "").strip()
        if not manager:
            raise ValueError(f"{_ENV_MANAGER} is required (the manager agent id, e.g. 'nagatha')")
        # BusConfig.from_env defaults FLEET_AGENT_ID to the literal "unknown" when
        # unset; the hub stamps every coordination envelope's sender from this id,
        # so a missing var would put "unknown" on live traffic. Default the hub's
        # bus identity to the manager id instead (codex cross-review, #7).
        if bus.agent_id == "unknown":
            bus = replace(bus, agent_id=manager)
        return cls(
            bus=bus,
            manager=manager,
            workers=_workers_env(_ENV_WORKERS),
            budget=Budget.from_env(),
            dispatch_timeout_s=timeout_from_env(),
            offline_poll_s=_float_env(_ENV_OFFLINE_POLL, _DEFAULT_OFFLINE_POLL_S),
            heartbeat_interval_s=_float_env(_ENV_HEARTBEAT_INTERVAL, _DEFAULT_HEARTBEAT_INTERVAL_S),
            circlejerk_n=circlejerk_n_from_env(),
            arbitration_enabled=_bool_env(_ENV_ARBITRATION_ENABLED, _DEFAULT_ARBITRATION_ENABLED),
            arbitrate_topics=_topics_env(_ENV_ARBITRATE_TOPICS),
            intake_poll_enabled=intake_enabled_from_env(),
            intake_poll_s=intake_poll_from_env(),
            intake_channel=os.environ.get(_ENV_INTAKE_CHANNEL, "").strip(),
            intake_label=intake_label_from_env(),
            fleet_liveness_enabled=enabled_from_env(),
            fleet_expected=expected_from_env(),
            fleet_liveness_consecutive=consecutive_from_env(),
            fleet_liveness_sweep_s=sweep_seconds_from_env(),
            kill_switch_poll_s=kill_poll_from_env(),
            governor_durable=durable_enabled_from_env(),
        )
