"""The NATS platform adapter — the ingress that turns bus envelopes into turns.

A Hermes ``BasePlatformAdapter`` (the sanctioned ``register_platform`` lever, NOT
the standalone-pipeline dead-end the plan warns against) that:

1. On :meth:`connect`, opens a :class:`~fleet_nats.FleetBus` and subscribes to
   *this* agent's coordination subjects (dispatch / relay / wake / inbox), all
   built via :mod:`fleet_nats.subjects`. The bus owns the subscription lifecycle
   (``FleetBus.close`` drains it on :meth:`disconnect`); per-message agent turns
   are the base class's tracked ``_background_tasks``.
2. On each envelope, maps it (:func:`~fleet_subscriber.mapping.envelope_to_message`)
   to a synthetic internal ``MessageEvent(internal=True)`` and hands it to the
   gateway via ``handle_message`` — which runs a full agent turn. ``internal=True``
   bypasses the mention-gate and user-auth (verified: gateway/run.py:7170).
3. The gateway's pipeline posts the agent's own reply back out via this adapter's
   :meth:`send`, which publishes it onto the bus (delivery only; the reply is the
   agent's own words).

The adapter subclasses a *host-provided* base class, so it is constructed through
:func:`build_adapter_class`, which is handed the loaded gateway API (real on the
host, a stub under test). Nothing here imports the gateway directly.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from datetime import UTC, datetime
from functools import partial
from typing import Any
from uuid import uuid4

from fleet_nats import (
    NARRATION_KIND,
    BusConfig,
    ChannelMap,
    Envelope,
    FleetBus,
    Stance,
    StancePosition,
    SubjectCatalog,
    connect_catalog,
    done_envelope,
    empty_map,
    load_channel_map,
    subjects,
)
from fleet_nats.tracing import traced
from loguru import logger

from fleet_subscriber.completion import DEFAULT_OUTCOME, Completion
from fleet_subscriber.dead_letter import DeadLetterReporter
from fleet_subscriber.dedup import DedupGuard, window_from_env
from fleet_subscriber.fleet_control import register_fleet_control
from fleet_subscriber.fleet_send import current_bus_task_id, register_fleet_send
from fleet_subscriber.heartbeat import (
    HeartbeatEmitter,
    installed_package_version,
    interval_from_env,
)
from fleet_subscriber.hermes import GatewayApi, load_gateway
from fleet_subscriber.killwatch import WorkerKillWatch, build_worker_kill_watch
from fleet_subscriber.mapping import SyntheticMessage, envelope_to_message, reply_channel
from fleet_subscriber.role import SubscriberRole, is_publish_only, lanes_for_role
from fleet_subscriber.show_work import register_show_work
from fleet_subscriber.stance import StanceSignal
from fleet_subscriber.turn import Turn, parse_turn
from fleet_subscriber.voice_tee import VoiceTeeConfig, tee_to_discord

# Map a role lane name to the fleet_nats.subjects builders that name this agent's
# concrete subscription subjects for it — the ONLY place lane names become
# subjects (never hand-formatted). Managers and workers both draw their lanes
# from role.py. Each builder is used as (env, agent) -> str.
#
# Every CHANNEL-SCOPABLE lane needs BOTH forms: the orchestrator publishes on the
# PER-CHANNEL subject ({env}.agent.{channel}.{lane}.{agent}), which has one more
# token than the bare form — NATS wildcards match per-token, so a bare-form
# subscription alone would NEVER receive scoped traffic (a latent
# no-dispatch-delivered bug caught by the fleet-hub dummy round-trip work).
#
# dispatch has been scoped since the beginning; relay and wake gained scoping in
# fleet-bus #117 and MUST be paired with their wildcards in the same change. A
# scoped publisher plus a bare-only subscriber is silent message loss, not a
# degraded mode: NATS core has no backlog, so the relay/wake simply never arrives
# and the worker waits forever.
#
# ``inbox`` is deliberately NOT scoped: it addresses an AGENT, not a channel's
# deliberation, and a manager has exactly one inbox regardless of how many
# channels it coordinates.
_LANE_BUILDERS: dict[str, tuple[Callable[[str, str], str], ...]] = {
    "dispatch": (subjects.agent_dispatch, subjects.agent_dispatch_wildcard),
    "relay": (subjects.agent_relay, subjects.agent_relay_wildcard),
    "wake": (subjects.agent_wake, subjects.agent_wake_wildcard),
    "inbox": (subjects.agent_inbox,),
}

# When a catalog was FETCHED from the registry (source="kboard"), the fetched
# catalog — not the code builders — is authoritative for the concrete subjects a
# lane subscribes to (the k-board registry endpoint / #103). Enforcement without consumption is
# theater: a client that fetches a catalog then subscribes via hard-coded builders
# would split the fleet the moment the registry's templates diverge from code.
#
# Each lane maps to the catalog subject KEYS (in CODE_SUBJECTS terms) it must
# subscribe. The dispatch lane needs BOTH the bare form and the per-channel
# wildcard (per-token NATS matching — see _LANE_BUILDERS above). A lane whose
# catalog key is absent from the fetched catalog is a HARD failure at subscribe
# time (named lane + version), never a silent fall back to the code builder.
_LANE_CATALOG_KEYS: dict[str, tuple[str, ...]] = {
    "dispatch": ("agent.dispatch", "agent.dispatch.wildcard"),
    "relay": ("agent.relay", "agent.relay.wildcard"),
    "wake": ("agent.wake", "agent.wake.wildcard"),
    "inbox": ("agent.inbox",),
}

# The concrete token every catalog subject template placeholder resolves to for
# THIS agent's own subscription. ``*`` placeholders in a template (the NATS
# wildcard tokens, e.g. the dispatch channel) are literals in the template and
# carry no ``{...}`` marker, so they survive .format() untouched. Every non-star
# placeholder addresses this one agent, so they all resolve to its (slugged) id.
_CATALOG_PLACEHOLDERS: tuple[str, ...] = ("worker", "agent", "to")

# Length ceiling below which a non-actionable send is filed as an ``ack`` rather
# than ``telemetry`` (#116). Both lanes are app-plane and neither reaches
# Discord, so this only decides WHICH mineable lane records the message — never
# whether it is recorded. Sized to admit a bare "ack"/"on it"/"starting" while
# excluding a tool-output echo, and biased toward telemetry on a tie (see
# ``NatsAdapter._noise_lane``).
_ACK_MAX_CHARS = 80

# Leading markers that make a send telemetry REGARDLESS of length (#116).
#
# The progress markers the issue enumerates are the shortest sends on the wire
# (a bare "⏳" is one character), so the length rule alone would file the
# highest-volume traffic onto the ack lane — the exact opposite of the split's
# purpose, since ack is contracted as one message per dispatch (C3 row 13).
# Matched as a PREFIX on the stripped text: these markers lead a progress line,
# and prefix-matching cannot reclassify a real ack that merely mentions one.
_PROGRESS_MARKERS: tuple[str, ...] = (
    "⏳",  # hourglass — the progress class named in #116
    "⌛",
    "💾",  # floppy disk — the saving class named in #116
    "🔧",
    "🔨",
    "🔍",
    "…",
    "...",
)


def _render_catalog_subject(template: str, *, env: str, agent: str) -> str:
    """Render a catalog subject template into this agent's concrete subject.

    Substitutes ``{env}`` and this-agent placeholders (``{worker}``/``{agent}``/
    ``{to}``) with the (slugged) agent id — mirroring the ``fleet_nats.subjects``
    builders so a fetched catalog and the code builders agree token-for-token.

    Raises:
        KeyError: If the template names a placeholder outside the known set (a
            malformed catalog row must fail loud, never render a wrong subject).
    """
    slugged = subjects.slug(agent)
    values = {"env": env, **{name: slugged for name in _CATALOG_PLACEHOLDERS}}
    return template.format_map(values)


def _resolve_lane_subjects(
    catalog: SubjectCatalog,
    lanes: tuple[str, ...] | list[str],
    *,
    env: str,
    agent: str,
    use_catalog: bool,
) -> list[str]:
    """Concrete subscription subjects for ``lanes``, catalog-first when fetched.

    When ``use_catalog`` is True (a registry-served catalog whose lane names this
    adapter can build), every subject comes from the catalog's ``subjects``
    templates via a FAIL-LOUD lookup: a lane whose catalog key is missing raises
    (naming the lane and catalog version) rather than silently reverting to the
    code builder — enforcement must consume the catalog it enforces. When False
    (no registry, or the unknown-lane code fallback), the code builders are the
    explicit, sanctioned source.

    Raises:
        KeyError: A lane's required catalog subject template is absent from the
            fetched catalog (lane + version named) — a HARD subscribe-time refuse.
    """
    resolved: list[str] = []
    for lane in lanes:
        if use_catalog:
            for key in _LANE_CATALOG_KEYS[lane]:
                template = catalog.subjects.get(key)
                if template is None:
                    raise KeyError(
                        f"catalog (version {catalog.version}, source {catalog.source}) is "
                        f"missing subject template {key!r} for lane {lane!r} — refusing to "
                        f"subscribe on a code default the fetched catalog does not name"
                    )
                resolved.append(_render_catalog_subject(template, env=env, agent=agent))
        else:
            for builder in _LANE_BUILDERS[lane]:
                resolved.append(builder(env, agent))
    return resolved


@traced
def build_adapter_class(api: GatewayApi) -> type[Any]:
    """Build the ``NatsAdapter`` class bound to a concrete gateway API.

    The base class (``BasePlatformAdapter``) is provided by the host runtime, so
    the subclass is created here once the API is loaded. On the host, call with
    :func:`~fleet_subscriber.hermes.load_gateway`; under test, pass a stub API.

    Args:
        api: The loaded gateway API (real or stub).

    Returns:
        The ``NatsAdapter`` class, ready to instantiate with a ``PlatformConfig``.
    """

    class NatsAdapter(api.BasePlatformAdapter):  # type: ignore[misc, name-defined]
        """Fleet NATS ingress adapter for one agent identity."""

        def __init__(self, config: Any) -> None:
            super().__init__(config, api.Platform("nats"))
            self._bus_config = BusConfig.from_env()
            # Role (worker|manager) selects this agent's subscription lanes; from
            # FLEET_ROLE, defaulting to WORKER (safe least-capable — a misconfigured
            # agent listens to its own work, never assumes the coordinator identity).
            self._role = SubscriberRole.from_env()
            self._bus: FleetBus | None = None
            # The subject catalog, fetched ONCE at connect (k-board registry,
            # the k-board registry endpoint) and cached — never a hot-path read.
            self._catalog: SubjectCatalog | None = None
            self._heartbeat: HeartbeatEmitter | None = None
            self._heartbeat_task: asyncio.Task[None] | None = None
            # Delivery-semantics guards (#25, C5): app-level duplicates (retry /
            # reconnect re-send; transport is at-most-once) need an idempotent
            # consumer; unprocessable input needs a visible destination.
            self._dedup: DedupGuard | None = None
            self._dead_letter: DeadLetterReporter | None = None
            # The persisted channel portal map (fleet-bus #118) — slug ↔ Discord
            # id, k-board-backed, loaded at connect(). Empty until then so a
            # pre-connect send resolves nothing rather than guessing a channel.
            self._channels: ChannelMap = empty_map()
            # The agent's home channel: the slug this agent's own turns belong to
            # when an envelope omits one. It is a SLUG (an identity), resolved to a
            # Discord id through the map — never a raw channel id, and never the
            # tee's target for other channels' turns (that was the #118 defect).
            # Fall back to the agent's own id when the host config carries no
            # home_channel. The real Hermes ``PlatformConfig`` never sets one --
            # it is built from this plugin's ``required_env`` (FLEET_NATS_URL,
            # FLEET_AGENT_ID) -- so reading config alone yields None on EVERY
            # live agent host, which silently disables the legacy tee bridge
            # below and takes the mirror from "one room" to "no room" on the
            # next gateway restart. ``agent_id`` is required env, so it is always
            # present, and an agent's home channel is its own name by convention
            # (skippy -> #skippy). Verified against the running skippy/bilby
            # gateways: both have FLEET_VOICE_TEE_ENABLED and
            # FLEET_VOICE_TEE_CHANNEL_ID, neither has FLEET_CHANNEL_MAP.
            self._home_channel = getattr(config, "home_channel", None) or self._bus_config.agent_id
            # Optional voice tee (#55): mirror the visible reply to a real Discord
            # channel through the gateway's own in-process Discord adapter. OFF by
            # default; a missing/false flag means no Discord call is ever made.
            # Rebuilt at connect() once the portal map is loaded.
            self._voice_tee = VoiceTeeConfig.from_env(
                self._channels, home_channel=self._home_channel
            )
            # Correlate an outbound reply back to the task it answers. The gateway's
            # send() only hands us (chat_id, content, metadata) — the task_id from
            # the inbound envelope is gone by then — so we stash chat_id -> task_id
            # when forging the turn and look it up in send() to key the reply's
            # agent_status subject on the SAME task_id the orchestrator watches.
            self._channel_task: dict[str, str] = {}
            self._kill_watch: WorkerKillWatch | None = None
            self._kill_loop: asyncio.AbstractEventLoop | None = None

        # ---- BasePlatformAdapter abstract surface -----------------------------

        async def _stop_heartbeat(self) -> None:
            """Stop and await the heartbeat task; idempotent, safe when absent.

            Shared by ``disconnect()`` and ``connect()`` (a re-connect must not
            orphan the previous emitter — an unreachable task beating against a
            stale bus forever).
            """
            if self._heartbeat is not None:
                self._heartbeat.stop()
            if self._heartbeat_task is not None:
                self._heartbeat_task.cancel()
                try:
                    await self._heartbeat_task
                except asyncio.CancelledError:
                    logger.debug("fleet_subscriber.heartbeat | task cancelled")
            self._heartbeat_task = None
            self._heartbeat = None

        async def _stop_kill_watch(self) -> None:
            """Join the worker authority watch without blocking the gateway loop."""
            watch = self._kill_watch
            self._kill_watch = None
            self._kill_loop = None
            if watch is not None:
                await asyncio.get_running_loop().run_in_executor(None, watch.stop)

        def _cancel_for_kill(self) -> None:
            """Schedule real gateway-task cancellation from the watcher thread."""
            loop = self._kill_loop
            if loop is None or loop.is_closed():
                return
            loop.call_soon_threadsafe(lambda: asyncio.create_task(self.cancel_background_tasks()))

        async def connect(self, *, is_reconnect: bool = False) -> bool:
            """Open the bus and subscribe to this agent's coordination subjects.

            ``is_reconnect`` is the Hermes platform-adapter reconnect signal. Both
            call modes use the same guarded teardown and rebuild path below.
            """
            agent = self._bus_config.agent_id
            env = self._bus_config.env
            # A reconnect replaces the whole transport lifecycle.  Mark the
            # adapter down before teardown so any failure below cannot leave the
            # previous successful state reporting connected.
            self._running = False
            await self._stop_heartbeat()
            await self._stop_kill_watch()
            self._dedup = None
            self._dead_letter = None
            if self._bus is not None:
                previous_bus = self._bus
                self._bus = None
                await previous_bus.close()
            self._kill_loop = asyncio.get_running_loop()
            self._kill_watch = build_worker_kill_watch(
                self._bus_config,
                poll_s=1.0,
                on_arm=self._cancel_for_kill,
            )
            # Tick-zero is intentionally before catalog and FleetBus.connect().
            self._kill_watch.start()
            # Validate the heartbeat interval BEFORE any side effect: a typo'd
            # env var must fail this connect loudly (contract: False + log),
            # not raise mid-wiring and leave an open, subscribed bus behind.
            try:
                heartbeat_interval_s = interval_from_env()
                dedup_window = window_from_env()
            except ValueError as exc:
                logger.error("fleet_subscriber.connect | bad env config agent={}: {}", agent, exc)
                await self._stop_kill_watch()
                return False
            # Subject registry (the k-board registry endpoint / #103): learn the catalog ONCE
            # at connect AND enforce the configured version floor BEFORE opening
            # the bus — a below-floor client speaks an obsolete taxonomy and
            # splits the fleet, so it must refuse to start rather than come up
            # deaf. connect_catalog falls open to the code catalog only when NO
            # floor is configured; with a floor set, a registry outage (code
            # catalog, version 0) fails the floor and raises. That raise is a
            # HARD refuse-to-start — it deliberately propagates out of connect()
            # (not the soft return-False path) so the process exits loud.
            catalog = await connect_catalog()
            self._catalog = catalog
            # Channel portal map (fleet-bus #118): the persisted slug ↔ Discord-id
            # table BOTH bridge legs resolve through, fetched ONCE here beside the
            # catalog (never on the hot path) and re-fetched on every reconnect so
            # a channel added to k-board lands without restarting the agent.
            # load_channel_map never raises — an unreachable table yields the empty
            # map and the tee then skips (loudly) rather than mis-routing.
            previous_channels = self._channels
            self._channels = await load_channel_map()
            # A reconnect can load a DIFFERENT map (the k-board table goes from 404
            # to live, or the reverse). ``_channel_task`` is keyed on the resolved
            # chat_id, so the same room can key as "Arena" under an empty map and
            # "arena" under a loaded one — a reply then never finds the task opened
            # under the other spelling and its status falls back to the CHANNEL
            # subject, which watch_worker_status() is not listening on. Drop the
            # stale correlations when the map identity changes: a lost correlation
            # degrades one in-flight reply to the channel-keyed subject (the
            # pre-existing uncorrelated path), whereas a stale one silently
            # mis-keys every subsequent reply for that room.
            if previous_channels.portals != self._channels.portals and self._channel_task:
                logger.warning(
                    "fleet_subscriber.connect | channel map changed ({} -> {} portals, "
                    "source {} -> {}) — dropping {} stale chat->task correlation(s) so a "
                    "reply cannot be keyed on a spelling the new map no longer produces",
                    len(previous_channels),
                    len(self._channels),
                    previous_channels.source,
                    self._channels.source,
                    len(self._channel_task),
                )
                self._channel_task.clear()
            self._voice_tee = VoiceTeeConfig.from_env(
                self._channels, home_channel=self._home_channel
            )
            # Log ``active``, not ``enabled``: ``enabled`` is just the env flag and
            # is True whenever the deploy sets it, so it is structurally incapable
            # of showing the one failure an operator needs to see here — a tee that
            # is switched on but can never fire because no portal resolved. The
            # WARNING below is that signal, and it names the reason.
            logger.info(
                "fleet_subscriber.connect | channel map source={} portals={} "
                "tee_enabled={} tee_active={}",
                self._voice_tee.channels.source,
                len(self._voice_tee.channels),
                self._voice_tee.enabled,
                self._voice_tee.active,
            )
            if self._voice_tee.enabled and not self._voice_tee.active:
                logger.warning(
                    "fleet_subscriber.connect | voice tee is ENABLED but INERT — the "
                    "channel map holds no portals (source={}), so NO reply will be "
                    "mirrored to Discord. Deploy the k-board channel-map table or set "
                    "FLEET_CHANNEL_MAP.",
                    self._voice_tee.channels.source,
                )
            bus = FleetBus(self._bus_config)
            try:
                await bus.connect()
            except Exception as exc:
                # Log before returning a falsy result — never a silent failure.
                logger.error(
                    "fleet_subscriber.connect | bus connect failed agent={}: {}",
                    agent,
                    exc,
                )
                await bus.close()
                await self._stop_kill_watch()
                return False
            self._bus = bus

            # Subscribe to this agent's role lanes (worker: dispatch/relay/wake/
            # inbox; manager: inbox — the task-keyed status/arbitrate lanes are
            # owned by the orchestrator, not the adapter). Every subject is built
            # via the sanctioned builders, never hand-formatted.
            # connect()'s contract is return-False-with-log on ANY failure. A
            # subscribe raising mid-loop (connection dropped) must not escape
            # and leave an open, partially-subscribed bus behind — close it and
            # honor the bool contract.
            # The registry's role→lanes map (loaded above) is authoritative for
            # now. A FETCHED catalog (source="kboard") is authoritative for the
            # concrete subjects too: its subject templates — not the hard-coded
            # builders — name the lanes this agent subscribes (F1). The code
            # builders are the explicit no-registry (source="code") fallback.
            lanes = catalog.role_lanes.get(self._role.value, lanes_for_role(self._role))
            if is_publish_only(self._role) and lanes:
                # A publish-only role subscribes to NOTHING (#222), and the
                # registry does not get to override that. The role→lanes map is
                # normally authoritative from the fetched catalog, so without this
                # a drifted or tampered k-board row could hand a runner box the
                # `dispatch` lane — turning a publish-only principal into one that
                # RECEIVES instructions, which is the single thing its threat
                # model forbids (it runs a coding agent with bypassPermissions
                # over untrusted repo content). Fail loud and subscribe nothing.
                logger.error(
                    "fleet_subscriber.connect | catalog (version {}, source {}) assigns "
                    "lane(s) {} to publish-only role={} — refusing; a publish-only "
                    "principal must never subscribe",
                    catalog.version,
                    catalog.source,
                    list(lanes),
                    self._role.value,
                )
                await bus.close()
                self._bus = None
                await self._stop_kill_watch()
                return False
            use_catalog = catalog.source == "kboard"
            unknown_lanes = [lane for lane in lanes if lane not in _LANE_BUILDERS]
            if unknown_lanes:
                if use_catalog:
                    # A FETCHED catalog naming a lane this adapter cannot build is
                    # NOT a fail-open case (re-check residual 1): silently
                    # subscribing every hard-coded default would ignore a catalog
                    # the client just fetched and enforced — the exact
                    # enforce-then-ignore split F1 exists to prevent. Fail loud,
                    # like a missing template: refuse to start, name the lane(s)
                    # and catalog version, close the bus, subscribe nothing.
                    logger.error(
                        "fleet_subscriber.connect | fetched catalog (version {}, source {}) "
                        "role={} names unbuildable lane(s) {} — refusing to subscribe on "
                        "code defaults the catalog's lane set does not agree with",
                        catalog.version,
                        catalog.source,
                        self._role.value,
                        unknown_lanes,
                    )
                    await bus.close()
                    self._bus = None
                    await self._stop_kill_watch()
                    return False
                # A CODE-source catalog (no registry) has all-buildable lanes by
                # construction, but keep the defensive fallback: an unknown lane in
                # the bootstrap posture reverts to the code-default lane set +
                # builders rather than crash a no-registry client.
                logger.error(
                    "fleet_subscriber.connect | code-catalog lanes {} for role={} contain "
                    "unknown names — using code-default lanes",
                    lanes,
                    self._role.value,
                )
                lanes = lanes_for_role(self._role)
            # Resolve every concrete subject up front so a fetched catalog missing a
            # lane's template FAILS LOUD here (naming lane + version) instead of
            # silently subscribing via a code default the catalog does not name.
            try:
                lane_subjects = [
                    (lane, subject)
                    for lane in lanes
                    for subject in _resolve_lane_subjects(
                        catalog, [lane], env=env, agent=agent, use_catalog=use_catalog
                    )
                ]
            except KeyError as exc:
                logger.error(
                    "fleet_subscriber.connect | catalog/lane mismatch agent={} role={}: {}",
                    agent,
                    self._role.value,
                    exc,
                )
                await bus.close()
                self._bus = None
                await self._stop_kill_watch()
                return False
            try:
                for lane, subject in lane_subjects:
                    # Bind the concrete lane so a dead-letter record can name
                    # the subject the unprocessable envelope arrived on.
                    await bus.subscribe(subject, partial(self._on_envelope, source_subject=subject))
                    logger.info(
                        "fleet_subscriber.connect | subscribed "
                        "subject={} lane={} agent={} role={} catalog_version={} source={}",
                        subject,
                        lane,
                        agent,
                        self._role.value,
                        catalog.version,
                        catalog.source,
                    )
            except Exception as exc:
                logger.error(
                    "fleet_subscriber.connect | subscribe failed agent={}: {}",
                    agent,
                    exc,
                )
                await bus.close()
                self._bus = None
                await self._stop_kill_watch()
                return False

            # Heartbeat (#26): identity/liveness only — agent id, role, the
            # EFFECTIVE lanes just subscribed, seq, last-turn ts. Started after
            # the subscriptions so a beat never advertises lanes that failed.
            #
            # A publish-only principal does NOT beat (#222). Its credential
            # permits exactly one subject, and `{env}.agent.heartbeat.{agent}` is
            # not it — so an emitter here would publish a refused message every
            # interval forever: dropped by the server, logged as an authz refusal
            # by the client, and never seen by the liveness consumer. There is
            # also nothing to advertise (lanes is empty by construction) and no
            # fleet-liveness question to answer: a runner box is disposable and
            # its run state lives in the job row, not in bus presence.
            if is_publish_only(self._role):
                logger.debug(
                    "fleet_subscriber.connect | role={} is publish-only — no heartbeat "
                    "(its credential grants no heartbeat subject)",
                    self._role.value,
                )
            else:
                self._heartbeat = HeartbeatEmitter(
                    bus,
                    agent_id=agent,
                    env=env,
                    role=self._role.value,
                    lanes=lanes,
                    new_id=self._new_id,
                    now=self._now,
                    catalog_version=catalog.version,
                    package_version=installed_package_version(),
                    interval_s=heartbeat_interval_s,
                )
                self._heartbeat_task = asyncio.create_task(self._heartbeat.run())

            # Delivery-semantics guards (#25, C5): dedup makes the consumer
            # idempotent against app-level duplicates (transport is at-most-once);
            # the reporter gives unprocessable input a visible lane.
            self._dedup = DedupGuard(window=dedup_window)
            self._dead_letter = DeadLetterReporter(
                bus, env=env, agent_id=agent, new_id=self._new_id, now=self._now
            )

            self._running = True  # drives BasePlatformAdapter.is_connected()
            logger.info(
                "fleet_subscriber.connect | ready agent={} env={} role={}",
                agent,
                env,
                self._role.value,
            )
            return True

        async def disconnect(self) -> None:
            """Stop the subscriber and drain the bus.

            FleetBus owns the NATS subscription lifecycle; ``close()`` drains and
            tears down the subscriptions (and nats-py's background reader/ping
            tasks), so there is no separate listener task for the adapter to hold.
            ``cancel_background_tasks()`` cancels any per-message turns the base
            class spawned.
            """
            self._running = False
            await self._stop_heartbeat()
            await self._stop_kill_watch()
            # The guards hold the closing bus; drop them with it.
            self._dedup = None
            self._dead_letter = None
            await self.cancel_background_tasks()
            if self._bus is not None:
                await self._bus.close()
                self._bus = None
            logger.info(
                "fleet_subscriber.disconnect | closed agent={}",
                self._bus_config.agent_id,
            )

        @staticmethod
        def _recover_turn_signals(
            content: str,
        ) -> tuple[Completion | None, StanceSignal | None, str]:
            """Recover ``(completion, stance, visible)`` from a reply's content.

            The single deserialization boundary (#77). Completion and stance are
            TYPED signals: a coordinated reply is a JSON object validated into a
            :class:`~fleet_subscriber.turn.Turn`, whose NAMED fields carry them —
            so a literal ``[[done]]`` / ``[[stance:...]]`` token or a gateway
            footer string inside ``visible_text`` is inert display prose, never a
            control signal (fleet-bus #205 removed the legacy text-trailer parsers
            entirely). Content that is not a typed ``Turn`` is visible prose only:
            it may still publish on an uncorrelated send, but it cannot complete or
            sign a coordinated turn.

            Returns the types the send() logic consumes: a :class:`Completion` (or
            ``None``), a :class:`StanceSignal` (or ``None``), and the ``visible``
            text — so the gate / meta / done-sentinel / voice-tee logic downstream
            is untouched.

            Args:
                content: The raw reply string handed to ``send()``.

            Returns:
                ``(completion, stance, visible)`` — signals present only for a
                typed ``Turn``; otherwise ``(None, None, content)``.
            """
            turn = parse_turn(content)
            if turn is not None:
                return NatsAdapter._signals_from_turn(turn)
            return None, None, content

        @staticmethod
        def _signals_from_turn(
            turn: Turn,
        ) -> tuple[Completion | None, StanceSignal | None, str]:
            """Map a typed :class:`Turn` onto the (completion, stance, visible) locals.

            ``done`` → a :class:`Completion` (outcome defaulting to
            ``DEFAULT_OUTCOME``); a non-``None`` ``stance`` → a
            :class:`StanceSignal`. ``visible`` is the named ``visible_text`` field
            verbatim — completion/stance are typed fields, never text, so there is
            nothing to strip.
            """
            completion = (
                Completion(
                    visible_text=turn.visible_text,
                    outcome=turn.outcome if turn.outcome else DEFAULT_OUTCOME,
                )
                if turn.done
                else None
            )
            stance = (
                StanceSignal(
                    visible_text=turn.visible_text,
                    stance=turn.stance.value,
                    fact_pull=turn.fact_pull,
                    reason=turn.reason,
                )
                if turn.stance is not None
                else None
            )
            return completion, stance, turn.visible_text

        def _reply_channel(self, chat_id: str) -> str | None:
            """The channel token to scope an outbound subject with (#117), or None.

            The gateway's ``chat_id`` IS the Discord channel the turn ran in, so
            it is the channel axis for the reply. Delegates to the SHARED
            :func:`fleet_subscriber.mapping.reply_channel` so the dummy's
            install-proof round-trip and this production path can never drift on
            the scoping rule (empty and wildcard both fall back to the bare form).
            """
            return reply_channel(chat_id)

        @staticmethod
        def _noise_lane(text: str) -> str:
            """Classify one noise send as ``ack`` or ``telemetry`` (#116).

            The split is by VOLUME and consumer, not by importance (both lanes are
            app-plane and neither reaches Discord):

            - ``ack`` — a short bare acknowledgement, one per dispatch. A miner
              that only wants "did the dispatch land" reads this lane alone.
            - ``telemetry`` — tool echo and progress markers (the hourglass and
              floppy-disk classes), unbounded per turn.

            The heuristic is deliberately crude and deliberately BIASED toward
            ``telemetry``: misfiling telemetry as an ack would pollute the
            low-volume lane whose whole value is that it stays small, whereas
            misfiling an ack as telemetry costs nothing but a slightly noisier
            firehose. Both are recorded either way, so no classification error
            here can lose a message — which is why a crude rule is acceptable.

            Length ALONE cannot carry that bias, though: the progress markers #116
            names (the hourglass and floppy-disk classes) are the SHORTEST sends
            on the wire — ``"⏳"`` is one character — so a pure ``len()`` rule files
            the highest-volume traffic onto the lane contracted as "one message per
            dispatch" (C3 row 13), defeating the split it exists to make. A marker
            prefix therefore decides FIRST, and only an unmarked short send is an
            ack.
            """
            stripped = text.strip()
            if stripped.startswith(_PROGRESS_MARKERS):
                return "telemetry"
            return "ack" if len(stripped) <= _ACK_MAX_CHARS else "telemetry"

        async def _publish_noise(
            self,
            *,
            task_id: str,
            chat_id: str,
            text: str,
            thread_id: Any,
        ) -> None:
            """Record one non-actionable send on its own noise lane (#116).

            Never raises and never affects the caller's ``SendResult``: this lane
            is observability. A bus failure while recording an ack must not turn a
            healthy turn into a failed send, so every error is logged and
            swallowed — the same fail-open contract the voice tee has.

            Never logs ``text`` (agent prose — ``docs/logging.md`` never-log list).
            """
            if self._bus is None:
                logger.error(
                    "fleet_subscriber.noise | no bus; dropping noise chat={} task={}",
                    chat_id,
                    task_id,
                )
                return
            lane = self._noise_lane(text)
            builder = subjects.agent_ack if lane == "ack" else subjects.agent_telemetry
            try:
                subject = builder(
                    self._bus_config.env, task_id, channel=self._reply_channel(chat_id)
                )
                envelope = Envelope.new(
                    msg_id=self._new_id(),
                    ts=self._now(),
                    sender=self._bus_config.agent_id,
                    kind=lane,
                    task_id=task_id,
                    channel=chat_id,
                    payload={
                        "text": text,
                        **({"thread_id": thread_id} if thread_id else {}),
                    },
                )
                await self._bus.publish(subject, envelope)
            except Exception as exc:  # noqa: BLE001 — noise recording is best-effort
                logger.warning(
                    "fleet_subscriber.noise | publish failed lane={} chat={} task={}: {}",
                    lane,
                    chat_id,
                    task_id,
                    type(exc).__name__,
                )
                return
            logger.info(
                "fleet_subscriber.noise | recorded subject={} lane={} chat={} task={} chars={}",
                subject,
                lane,
                chat_id,
                task_id,
                len(text),
            )

        async def send(
            self,
            chat_id: str,
            content: str,
            reply_to: str | None = None,
            metadata: dict[str, Any] | None = None,
        ) -> Any:
            """Publish the agent's final reply back onto the bus.

            The reply's STRUCTURE (done / visible text / stance) is recovered at a
            single deserialization boundary (#77): a typed, JSON-deserialized
            :class:`~fleet_subscriber.turn.Turn` when the producer emits one. For a
            correlated coordination turn, a typed completion signal identifies the
            final visible reply.
            A structural stance is published as a status signal for scoring but is
            never voice-teed; other intermediate sends are suppressed. An
            uncorrelated send has no completion contract and publishes normally so
            legitimate standalone output is never silently lost. Never logs
            ``content`` (agent prose).
            """
            thread_id = (metadata or {}).get("thread_id")
            # Key the reply's status subject on the TASK the turn answered — that is
            # the subject the orchestrator's watch_worker_status() listens on
            # (agent_status(env, task_id)). Fall back to the channel only when the
            # inbound had no task_id (a bare inbox turn, not a coordinated task).
            #
            # get() (peek, NOT pop): the gateway calls send() *many* times per turn.
            # The correlation must survive suppressed intermediate sends so the
            # explicitly completed reply still reaches the task-keyed subject.
            # Growth is bounded instead by keying on channel and OVERWRITING the
            # entry on each new dispatch (_on_envelope), so the map holds at most one
            # entry per active channel — a small, fixed set — not one per turn.
            task_id = self._channel_task.get(chat_id)
            status_key = task_id if task_id else chat_id
            # Channel-scoped (#117): the reply rides the SAME channel slice as the
            # dispatch it answers, so a channel-scoped observer hears only its own
            # traffic. The hub subscribes both forms, so this is safe against a
            # hub/subscriber version skew in either direction.
            subject = subjects.agent_status(
                self._bus_config.env, status_key, channel=self._reply_channel(chat_id)
            )
            logger.info(
                "fleet_subscriber.send | subject={} chat={} task={} thread={}",
                subject,
                chat_id,
                task_id,
                thread_id,
            )
            if self._bus is None:
                logger.error("fleet_subscriber.send | no bus; dropping reply chat={}", chat_id)
                return api.SendResult(success=False, message_id=None)

            # Recover the turn's STRUCTURE (done + visible text + stance) from the
            # content. The primary path (#77) is a TYPED, deserialized Turn: a JSON
            # object whose named fields carry the signals, so a literal [[done]] or
            # a gateway footer sitting inside visible_text is inert text, never a
            # control signal. Non-typed prose normalises to no signal, so legacy
            # trailers can no longer complete or sign a coordinated turn.
            completion, stance, visible = self._recover_turn_signals(content)
            if task_id and completion is None and stance is None:
                # Coordinated turns are noisy at the gateway boundary: ack,
                # progress, and tool-output sends all pass through this method.
                # Without a completion or valid structural stance signal there is
                # nothing for the STATUS lane to consume, and nothing that may
                # reach Discord — but the noise is still worth keeping (#116).
                #
                # Before #116 this branch DISCARDED the send outright, which lost
                # every ack and progress marker: "needed but noisy" traffic was
                # neither delivered nor recorded. Now it is republished onto its
                # OWN lane ({env}.agent.ack.* / {env}.agent.telemetry.*), which is
                # app-plane and which the Discord bridge does not forward. The
                # noise stays fully logged and mineable and still costs ZERO LLM
                # turns, because nothing subscribes these lanes for coordination.
                #
                # A publish failure here must NOT change the send's outcome: the
                # noise lane is observability, and losing an ack must never fail a
                # turn that the coordination path considers fine.
                await self._publish_noise(
                    task_id=task_id,
                    chat_id=chat_id,
                    text=visible,
                    thread_id=thread_id,
                )
                logger.warning(
                    "fleet_subscriber.send | correlated send lacks completion or stance; "
                    "routed to the noise lane, suppressing status and voice tee "
                    "chat={} task={} thread={}",
                    chat_id,
                    task_id,
                    thread_id,
                )
                return api.SendResult(success=True, message_id=None)

            # The stance typed Turn field is the ORGANIC turn-signal
            # (the agent runtime). The typed fleet-core ``Stance`` bound as
            # ``typed_signal`` is the truth the orchestrator's parse_turn_signal
            # reads first (C7: typed frame fields are truth); ``payload.meta``
            # {"stance", "fact_pull"[, "reason"]} is kept only as a compatibility
            # MIRROR for pre-core consumers of the legacy meta fallback.
            # ``visible`` was already resolved (trailers stripped on the text
            # path; the named visible_text field on the typed path) above.
            meta: dict[str, Any] | None = None
            typed_stance: Stance | None = None
            if stance is not None:
                typed_stance = Stance(
                    stance=StancePosition(stance.stance),
                    fact_pull=stance.fact_pull,
                    reason=stance.reason,
                )
                # Both fields are always emitted together — parse_turn_signal
                # rejects a half-signed meta (stance without fact_pull), so a
                # signed turn must carry both.
                meta = {"stance": stance.stance, "fact_pull": stance.fact_pull}
                if stance.reason is not None:
                    meta["reason"] = stance.reason
                logger.info(
                    "fleet_subscriber.send | signed stance subject={} task={} "
                    "stance={} fact_pull={} has_reason={}",
                    subject,
                    task_id,
                    stance.stance,
                    stance.fact_pull,
                    stance.reason is not None,
                )

            reply = Envelope.new(
                msg_id=self._new_id(),
                ts=self._now(),
                sender=self._bus_config.agent_id,
                kind="status",
                task_id=task_id,
                channel=chat_id,
                payload={
                    "text": visible,
                    **({"meta": meta} if meta is not None else {}),
                    **({"thread_id": thread_id} if thread_id else {}),
                },
                typed_signal=typed_stance,
            )
            await self._bus.publish(subject, reply)

            # Optional voice tee (#55): mirror only the final visible reply. A
            # stance-only status is structural scorekeeper input, not user-facing
            # prose. tee_to_discord() never raises, so the bus path and done
            # sentinel remain isolated from Discord failures.
            if self._voice_tee.active and (completion is not None or task_id is None):
                # #118: the mirror goes to THIS turn's channel, resolved through
                # the persisted portal map — not to one env-configured room for
                # every task. An unmapped channel skips the mirror loudly; it must
                # never fall back to another channel.
                tee_target = self._voice_tee.target_for(chat_id)
                if tee_target is None:
                    logger.warning(
                        "fleet_subscriber.send | channel {} has no portal in the channel "
                        "map (source={} portals={}) — reply NOT mirrored task={}",
                        chat_id,
                        self._voice_tee.channels.source,
                        len(self._voice_tee.channels),
                        task_id,
                    )
                else:
                    await tee_to_discord(
                        config=self._voice_tee,
                        platform_factory=api.Platform,
                        channel_id=tee_target,
                        text=visible,
                        # Declare the lane so the bridge's own subject guard (#116)
                        # can refuse noise independently of this call site's logic.
                        # Kept from #116 alongside #118's per-channel target: the
                        # two guards are orthogonal — this one refuses the wrong
                        # LANE, target_for refuses the wrong ROOM.
                        source_subject=subject,
                    )

            if completion is not None and task_id:
                # Emit the un-spoofable done sentinel on the SAME status subject the
                # orchestrator watches, via the SHARED builder (fleet_nats.sentinel)
                # so the emitter and consumer can never drift on the wire shape.
                done = done_envelope(
                    msg_id=self._new_id(),
                    ts=self._now(),
                    sender=self._bus_config.agent_id,
                    task_id=task_id,
                    outcome=completion.outcome,
                    channel=chat_id,
                )
                logger.info(
                    "fleet_subscriber.send | done sentinel subject={} task={} outcome={}",
                    subject,
                    task_id,
                    completion.outcome,
                )
                await self._bus.publish(subject, done)
                # The turn is over — drop the correlation so the channel is free for
                # the next task (bounded growth; the reply already routed correctly).
                self._channel_task.pop(chat_id, None)

            return api.SendResult(success=True, message_id=None)

        async def get_chat_info(self, chat_id: str) -> dict[str, Any]:
            """Minimal chat info — the bus carries channels, not rich chat metadata."""
            return {"id": chat_id, "type": "channel"}

        @staticmethod
        def _now() -> str:
            """ISO-8601 UTC timestamp for an outbound envelope (live wire time)."""
            return datetime.now(UTC).isoformat()

        def _new_id(self) -> str:
            """A fresh message id for an outbound reply envelope."""
            return uuid4().hex

        # ---- ingress ----------------------------------------------------------

        async def _on_envelope(
            self, envelope: Envelope, *, source_subject: str = "unknown"
        ) -> None:
            """Forge an inbound envelope into an internal agent turn.

            Delivery semantics (#25): a duplicate id is a logged no-op (never a
            second turn); anything that raises out of mapping/forging becomes a
            sanitized dead-letter record — the subscriber stays up either way.
            """
            if self._kill_watch is None or self._kill_watch.armed():
                logger.warning(
                    "fleet_subscriber.ingress | kill authority armed/uncertain; refusing id={}",
                    envelope.id,
                )
                return
            if self._dedup is not None and self._dedup.seen_before(envelope.id):
                return
            try:
                await self._forge_turn(envelope)
            except Exception as exc:  # noqa: BLE001 — one poison envelope must not kill ingress
                # Reason is the exception CLASS ONLY: downstream raisers (gateway
                # validation errors) commonly embed the offending input in str(exc),
                # which here can be the turn text — payload the dead-letter lane and
                # the log line must never carry (docs/logging.md never-log list).
                reason = type(exc).__name__
                logger.error(
                    "fleet_subscriber.ingress | unprocessable envelope id={} kind={}: {}",
                    envelope.id,
                    envelope.kind,
                    reason,
                )
                if self._dead_letter is not None:
                    await self._dead_letter.report(
                        reason=reason, source_subject=source_subject, source=envelope
                    )

        async def _forge_turn(self, envelope: Envelope) -> None:
            """The (unguarded) ingress path: map, stamp, forge the agent turn."""
            # Single-throat guard (#13): only the MANAGER voices narration. The
            # orchestrator only ever publishes beats on the manager's inbox, but a
            # misrouted/spoofed narration envelope must never make a worker speak.
            if envelope.kind == NARRATION_KIND and self._role is not SubscriberRole.MANAGER:
                logger.warning(
                    "fleet_subscriber.ingress | narration on a {} lane — dropped (task={})",
                    self._role.value,
                    envelope.task_id,
                )
                return
            # Attribute the synthetic turn to the receiving agent so gateway
            # beat acceptance and audit trails retain the real adapter identity.
            # This is attribution only, not authentication or authorization;
            # authenticated per-agent identity belongs to fleet-bus#104 (F2).
            # #118: the target channel is resolved through the persisted portal
            # map. The envelope's own channel wins; the agent's home channel is
            # only the fallback for an envelope that names none, and BOTH are
            # canonicalised through the same table so a slug and an id cannot
            # produce two different destinations for one room.
            mapped = envelope_to_message(
                envelope,
                channels=self._channels,
                home_channel=self._home_channel,
                synthetic_user_id=self._bus_config.agent_id,
            )
            if mapped is None:
                logger.warning(
                    "fleet_subscriber.ingress | unactionable envelope kind={} task={} — skipped",
                    envelope.kind,
                    envelope.task_id,
                )
                return
            # Remember which task this channel's turn answers, so send() can key the
            # reply back onto the task's status subject (the gateway strips task_id).
            if mapped.task_id:
                self._channel_task[mapped.chat_id] = mapped.task_id
            # Expose the inbound task_id to any in-turn fleet_send call (manager
            # summoning a worker mid-deliberation), so a summoned exchange stays
            # keyed to the SAME task. Set before handle_message spawns the turn:
            # asyncio.create_task snapshots the current contextvars context, so the
            # background turn — and the tool handler it runs — inherit this value.
            if mapped.task_id:
                current_bus_task_id.set(mapped.task_id)
            if self._heartbeat is not None:
                self._heartbeat.note_turn()
            event = self._make_event(mapped)
            logger.info(
                "fleet_subscriber.ingress | forging turn kind={} chat={} task={}",
                mapped.kind,
                mapped.chat_id,
                mapped.task_id,
            )
            # handle_message returns None (spawns the turn as a background task);
            # the agent's reply comes back out via send() (verified base.py:3926).
            await self.handle_message(event)

        def _make_event(self, mapped: SyntheticMessage) -> Any:
            """Build the gateway MessageEvent(internal=True) from a mapped message."""
            source = api.SessionSource(
                platform=api.Platform("nats"),
                chat_id=mapped.chat_id,
                chat_type=mapped.chat_type,
                thread_id=mapped.thread_id,
                user_id=mapped.user_id,
            )
            return api.MessageEvent(
                text=mapped.text,
                message_type=api.MessageType.TEXT,
                source=source,
                internal=True,  # bypass the mention-gate + user-auth (host-verified)
            )

    return NatsAdapter


@traced
def register(ctx: Any) -> None:
    """Plugin entry point — called by the Hermes plugin system.

    Registers the NATS platform via the sanctioned ``register_platform`` lever.
    The adapter class is built against the live gateway API at registration time.

    Args:
        ctx: The Hermes ``PluginContext``.
    """
    api = load_gateway()
    adapter_cls = build_adapter_class(api)

    def _build_adapter(config: Any) -> Any:
        return adapter_cls(config)

    def _check() -> bool:
        # The transport dep must be importable for the adapter to connect.
        try:
            import nats  # noqa: F401

            return True
        except ImportError:
            logger.error("fleet_subscriber.check | nats-py not importable")
            return False

    ctx.register_platform(
        name="nats",
        label="NATS",
        adapter_factory=_build_adapter,
        check_fn=_check,
        required_env=["FLEET_NATS_URL", "FLEET_AGENT_ID"],
        install_hint="uv add nats-py",
        emoji="🛰️",
    )
    logger.info("fleet_subscriber.register | NATS platform registered")

    # The manager's agent-invocable bus-send lever (issue #60). Role-gated inside
    # register_fleet_send: registered only when FLEET_ROLE=manager, so workers
    # never get a free-send tool (they reply through the existing path). A tool
    # registration failure must not abort the platform registration above, so it
    # is guarded — the ingress path stays live even if the tool can't register.
    try:
        register_fleet_send(ctx)
    except Exception as exc:  # noqa: BLE001 — tool wiring must not break the platform
        logger.error("fleet_subscriber.register | fleet_send registration failed: {}", exc)

    # The manager's steer→control-plane lever (issue #65). Role-gated inside
    # register_fleet_control (manager-only, mirrors fleet_send): maps a human
    # Discord steer (resume/pause/budget) onto the existing #61 control plane so
    # the steer is a real lever pull, not theater. Guarded for the same reason —
    # a tool wiring fault must not take the platform registration down.
    try:
        register_fleet_control(ctx)
    except Exception as exc:  # noqa: BLE001 — tool wiring must not break the platform
        logger.error("fleet_subscriber.register | fleet_control registration failed: {}", exc)

    # Pull-only deliberation receipts (#64). This is intentionally available to
    # every role; unlike fleet_send/fleet_control it cannot publish or steer.
    try:
        register_show_work(ctx)
    except Exception as exc:  # noqa: BLE001 — tool wiring must not break the platform
        logger.error("fleet_subscriber.register | show_work registration failed: {}", exc)
