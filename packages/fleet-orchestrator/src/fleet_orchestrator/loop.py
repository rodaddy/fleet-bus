"""The Phase-9 orchestrator run-loop — wires the shipped pieces into a service.

Phases 1-6 built the coordination brain: a pure state machine
(:mod:`fleet_orchestrator.transitions`), bus-wired behaviors
(:class:`~fleet_orchestrator.behaviors.Orchestrator`), a governor + single
narrator (:mod:`fleet_orchestrator.governor`), a scorekeeper/decider
(:mod:`fleet_orchestrator.score`), liveness + an offline ladder
(:mod:`fleet_orchestrator.liveness` / :mod:`fleet_orchestrator.offline`), a
k-board client (:mod:`fleet_orchestrator.kboard`), and a JSONL trace
(:mod:`fleet_orchestrator.trace`). Each was unit-proven and one live e2e wired
them BY HAND (``tests/test_e2e_live.py``).

This module is the missing durable service that does that wiring automatically
and long-lived — the "manager run-loop that Phase 9 builds" the shipped
docstrings already point at (``behaviors.py`` ``persist_transition``,
``docs/runbook.md``). It adds **no new coordination behavior**: it introduces
one intake seam (a k-board ``created`` event opens a coordination session) and
otherwise only starts/stops the existing watches per task.

Design invariants held: subjects only via :mod:`fleet_nats.subjects`; ids and
timestamps injected via :class:`~fleet_orchestrator.behaviors.IdClock` so the
control loop stays deterministic under test; no LLM anywhere; k-board is the
durable source of truth (NATS delivers); Discord is display-only (only the
manager's narrator surfaces beats, on her own inbox). The one clock this service
legitimately reads is the monotonic poll clock for the offline ladder — injected
at construction so tests drive it with a fake.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from dataclasses import dataclass, field, replace
from typing import Protocol, cast

from fleet_nats import (
    ChannelMap,
    Envelope,
    FleetBus,
    PayloadTooLargeError,
    StoreKillProbe,
    empty_map,
    load_channel_map,
    subjects,
)
from fleet_nats.control import OP_PAUSE, OP_RESUME, parse_control
from fleet_nats.registry import SubjectCatalog, connect_catalog
from fleet_nats.tracing import traced
from loguru import logger

from fleet_orchestrator import trace
from fleet_orchestrator.behaviors import IdClock, Orchestrator
from fleet_orchestrator.config import HubConfig
from fleet_orchestrator.costfeed import CostLedger
from fleet_orchestrator.fleet_liveness import FleetLivenessReporter, TransitionKind
from fleet_orchestrator.governor import Governor, Narrator
from fleet_orchestrator.governor_store import FleetKVGovernorStore, open_governor_store
from fleet_orchestrator.intake import INTAKE_EVENT, IntakeTask, offline_key, parse_intake, unkey
from fleet_orchestrator.intake_poller import IntakePoller
from fleet_orchestrator.kboard import KboardClient
from fleet_orchestrator.killswitch import KillSwitch
from fleet_orchestrator.liveness import LivenessTracker, watch_heartbeats
from fleet_orchestrator.offline import OfflineDetector
from fleet_orchestrator.score import (
    DecisionKind,
    Score,
    Scorekeeper,
    assign_synthesis,
    decide,
    narrate_decision,
    narrate_score_event,
)
from fleet_orchestrator.sentinel import parse_done
from fleet_orchestrator.show_work import serve_show_work
from fleet_orchestrator.state import CoordinationSession, Event, EventKind, State


class _Unsubscribable(Protocol):
    """The one capability the hub needs of a subscription handle to tear it down.

    ``FleetBus.subscribe``/``raw_subscribe`` and ``watch_token_costs`` return a
    concrete ``nats`` ``Subscription``; ``watch_heartbeats`` returns it typed as
    ``object`` (its Protocol widens the return). Both satisfy this narrow
    unsubscribe protocol, so the hub holds handles as ``_Unsubscribable`` rather
    than importing the nats type just for teardown.
    """

    async def unsubscribe(self) -> None:
        """Cancel this subscription."""
        ...


@dataclass
class _TaskWatches:
    """The live subscriptions + accessor for one coordinated task (internal)."""

    session_ref: Callable[[], CoordinationSession]
    subs: list[_Unsubscribable] = field(default_factory=list)


class OrchestratorHub:
    """The long-lived Phase-9 service: intake → coordinate → persist → done.

    Owns the connected bus, the manager's :class:`Orchestrator`, the governor +
    narrator, the scorekeeper, the liveness tracker + offline ladder, and the
    k-board client. On start it subscribes the intake lane (k-board ``created``
    events) and the shared heartbeat lane, and begins the offline poll. Each
    intake opens a per-task coordination session and starts the shipped watches
    for it; a ``done`` sentinel tears the session's watches down.

    The hub performs **no new coordination logic** — every decision still comes
    from the pure state machine and the deterministic detectors.
    """

    def __init__(
        self,
        config: HubConfig,
        bus: FleetBus,
        kboard: KboardClient,
        *,
        clock: IdClock,
        monotonic: Callable[[], float],
        catalog: SubjectCatalog,
        dry_run: bool = False,
        governor: Governor | None = None,
        kill_switch: KillSwitch | None = None,
    ) -> None:
        """Wire the hub from its config and the connected collaborators.

        Args:
            config: The validated service config.
            bus: The (connected) fleet bus.
            kboard: The k-board client (durable source of truth).
            clock: Injected id/timestamp factories (determinism seam).
            monotonic: Injected monotonic clock for the offline poll (the one
                runtime clock the service reads; injected so tests fake it).
            catalog: The subject catalog loaded once at connect (fail-open to
                code defaults). Held for observability/drift; the loop still
                builds concrete subjects via :mod:`fleet_nats.subjects`.
            dry_run: When ``True`` the hub coordinates normally EXCEPT it wires
                no narrator — no beats reach the manager's inbox, so a single
                session can be driven end-to-end without touching Discord.
        """
        self._config = config
        self._bus = bus
        self._kboard = kboard
        self._clock = clock
        self._monotonic = monotonic
        self._catalog = catalog
        self._dry_run = dry_run
        # The persisted channel portal map (fleet-bus #118). Empty until start()
        # loads it: a hub that has not started yet must resolve nothing rather
        # than guess a channel.
        self._channels: ChannelMap = empty_map()

        # Wire the #27 cost ledger into the governor so the token cap is a LIVE
        # control, not a documented intention (docs/runbook.md: the token cap is
        # inert unless watch_token_costs runs — started in start()).
        #
        # Durable governor state (fleet-bus #106) is FLAG-GATED and OFF by default:
        # a store is wired ONLY when FLEET_GOVERNOR_DURABLE is on. With it off (the
        # default) the governor is byte-for-byte today's in-memory behavior — no KV
        # dependency on the counter path. Enabling it is IDENTITY-GATED (blocked on
        # T5 + u7/#99) and is an operator, not a merge, decision.
        #
        # The durable store is a JetStream KV over the bus, which must be opened
        # ASYNCHRONOUSLY. This __init__ is sync, so the governor is built store-less
        # here and the opened store is attached in start() (before intake begins)
        # via Governor.attach_store when the flag is on.
        self._governor = governor or Governor(config.budget, ledger=CostLedger(), store=None)
        self._governor_durable = config.governor_durable
        self._narrator: Narrator | None = (
            None
            if dry_run
            else Narrator(
                bus,
                env=config.bus.env,
                manager=config.manager,
                new_id=clock.new_id,
                now=clock.now,
            )
        )
        self._orchestrator = Orchestrator(
            bus,
            clock,
            governor=self._governor,
            narrator=self._narrator,
            kboard=kboard,
        )
        self._scorekeeper = Scorekeeper(circlejerk_n=config.circlejerk_n)
        self._liveness = LivenessTracker()
        self._offline = OfflineDetector(timeout_s=config.dispatch_timeout_s)
        # The task-independent fleet-liveness roster sweep (fleet-bus #78). Built
        # only when enabled (config fails closed if enabled with no roster), so a
        # disabled hub carries no reporter and starts no sweep task. Reuses the
        # heartbeat tracker above — no second liveness source.
        self._fleet_liveness: FleetLivenessReporter | None = (
            FleetLivenessReporter(
                expected=config.fleet_expected,
                interval_s=config.heartbeat_interval_s,
                consecutive=config.fleet_liveness_consecutive,
            )
            if config.fleet_liveness_enabled
            else None
        )
        self._fleet_liveness_task: asyncio.Task[None] | None = None

        # The fleet-wide FleetKV kill authority (#107): a dedicated control-store
        # probe, polled outside the ordinary work callbacks. An armed or uncertain
        # record pauses every task (all actions -> NOOP); only an authoritative
        # released read resumes it. Built here so enforcement is never opt-in.
        #
        # P1-2 (out-of-LOOP, not just out-of-bus): the watcher runs on a DEDICATED OS
        # THREAD, not an asyncio task. The poll would otherwise share the hub's single
        # event loop with every NATS callback, so a real coordination storm (thousands
        # of ready callbacks) could starve the tick behind the backlog and blow the
        # ≤5s budget — a NATS flood IS event-loop work. On its own thread the
        # store-read + pause_all() loop is immune to loop saturation. The
        # governor kill flag is a single bool: the thread's write and the loop's read
        # (in gate) are each one atomic CPython bytecode op under the GIL, so the flip
        # needs no lock. The thread starts in start() and stops via a threading.Event
        # in aclose() (joined cleanly, no leaked thread).
        if kill_switch is None:
            store_probe = StoreKillProbe(config.bus)
            self._kill_switch = KillSwitch(
                self._governor,
                poll_s=config.kill_switch_poll_s,
                probe=store_probe.read,
                store_probe=store_probe,
            )
        else:
            self._kill_switch = kill_switch

        self._tasks: dict[str, _TaskWatches] = {}
        self._service_subs: list[_Unsubscribable] = []
        self._offline_task: asyncio.Task[None] | None = None
        # The intake poller: the only shipped publisher of the ``created`` event
        # the intake lane above consumes. Built in start() (when enabled) so the
        # durable service auto-opens a session from a new k-board card (#44); its
        # background task is stopped via the shared _stop event in aclose().
        self._intake_poller: IntakePoller | None = None
        self._intake_task: asyncio.Task[None] | None = None
        self._stop = asyncio.Event()

    @property
    def env(self) -> str:
        """The subject env prefix this hub coordinates under."""
        return self._config.bus.env

    def is_tracking(self, task_id: str) -> bool:
        """Whether the hub is still actively coordinating ``task_id``.

        A task is dropped from tracking only when its terminal state is durably
        persisted (see :meth:`_finish_task`), so ``True`` means "still coordinated"
        — the authoritative completion signal, independent of any subscription's
        cell (which may advance to DONE on a done envelope the hub then refuses to
        act on, e.g. a foreign task_id).
        """
        return task_id in self._tasks

    def session_state(self, task_id: str) -> State | None:
        """The tracked coordination cell state, or ``None`` when not tracked.

        Prefer :meth:`is_tracking` for "is it done" — the cell is advanced by
        ``watch_worker_status`` and may read DONE for a task the hub kept tracked
        (a foreign-id done). Kept for the ``--once`` smoke path, which treats
        untracked (``None``) OR DONE as completion.
        """
        watches = self._tasks.get(task_id)
        return watches.session_ref().state if watches is not None else None

    @traced
    async def start(self) -> None:
        """Subscribe the intake + heartbeat lanes and begin the offline poll.

        The intake lane is the k-board ``created`` wildcard; the heartbeat lane
        feeds the liveness tracker the offline ladder cross-checks. Startup is
        idempotent per hub instance (called once by :meth:`run`).

        When durable governor state is enabled (``FLEET_GOVERNOR_DURABLE`` on) the
        JetStream KV store is opened over the bus and attached to the governor HERE
        — before any intake subscribes, so a resumed task hydrates against a live
        store. The KV open is async, which is why it lives in ``start()`` rather
        than the sync ``__init__``.
        """
        # Channel portal map (fleet-bus #118): the persisted slug ↔ Discord-id
        # table the intake leg stamps cards through, loaded ONCE here — the same
        # posture the subscriber's outbound leg uses, so both legs read the same
        # k-board table and a new channel is a row, not a redeploy. Never raises;
        # an unreachable table yields the empty map and intake stamps channels
        # through unresolved rather than refusing to open cards.
        self._channels = await load_channel_map()
        logger.info(
            "orchestrator.hub | channel map source={} portals={} slugs={}",
            self._channels.source,
            len(self._channels),
            ",".join(self._channels.slugs),
        )
        if self._governor_durable:
            store: FleetKVGovernorStore = await open_governor_store(self._bus)
            self._governor.attach_store(store)
            logger.info(
                "orchestrator.hub | durable governor store attached (fleet-bus #106); durable={}",
                self._governor.durable,
            )
        intake_subject = subjects.kboard_event(self.env, INTAKE_EVENT, subjects.STAR)
        self._service_subs.append(await self._bus.subscribe(intake_subject, self._on_intake))
        logger.info(
            "orchestrator.hub | intake subscribed subject={} manager={} workers={}",
            intake_subject,
            self._config.manager,
            ",".join(self._config.workers),
        )
        # watch_heartbeats types its return as ``object`` (its bus Protocol widens
        # it); the concrete handle is a nats Subscription with unsubscribe(), so
        # narrow it to the teardown protocol the hub holds.
        # Thread the heartbeat cadence so the skew detector excludes stale/dead
        # agents' ghost catalog versions (#103 P2-6) — a dead agent's last
        # version must not fire phantom skew after the live fleet is uniform.
        heartbeat_sub = await watch_heartbeats(
            self._bus,
            self._liveness,
            env=self.env,
            now=self._monotonic,
            interval_s=self._config.heartbeat_interval_s,
        )
        self._service_subs.append(cast("_Unsubscribable", heartbeat_sub))
        # Feed the #27 token-cost lane into the governor's ledger so the token cap
        # enforces (else it is inert — docs/runbook.md governor section).
        self._service_subs.append(await self._orchestrator.watch_token_costs())
        # Subscribe the control plane (#59): the ONE external lever on the live
        # governor. A paused (over-budget) task is only resumable in-process, so a
        # human steer's "resume, raise the budget" lands here — the hub owns the
        # live Governor instance and calls its resume/pause. One wildcard for the
        # whole hub (task ids unknown at subscribe time; a frame may target a task
        # created later), mirroring the heartbeat/token-cost wildcard pattern.
        control_subject = subjects.hub_control_wildcard(self.env)
        self._service_subs.append(await self._bus.subscribe(control_subject, self._on_control))
        logger.info("orchestrator.hub | control plane subscribed subject={}", control_subject)
        # Stand up the arbitration responder(s) so Rung 8's arbitrate leg has a
        # server in the durable service (#43): without this a worker publishing on
        # dev.agent.arbitrate.<topic> and blocking on the reply gets no responder.
        await self._serve_arbitration()
        try:
            show_work_sub = await serve_show_work(
                self._bus,
                env=self.env,
                sender=self._config.manager,
                new_id=self._clock.new_id,
                now=self._clock.now,
            )
        except Exception:
            await self.aclose()
            raise
        self._service_subs.append(cast("_Unsubscribable", show_work_sub))
        self._offline_task = asyncio.create_task(self._offline_poll())
        # The FleetKV kill-authority watcher (#107): its OWN dedicated OS thread,
        # NOT an asyncio task — so a work-callback flood cannot starve the poll
        # cadence and blow the ≤5s budget. It starts alongside (not behind) normal
        # bus subscriptions and joins cleanly in aclose().
        self._kill_switch.start()
        logger.info(
            "orchestrator.hub | kill authority watch active poll={}s "
            "(dedicated store + watcher threads)",
            self._config.kill_switch_poll_s,
        )
        # The fleet-liveness roster sweep runs hub-wide (task-independent) only
        # when enabled — off first-deploy, so an unconfigured hub starts nothing.
        if self._fleet_liveness is not None:
            self._fleet_liveness_task = asyncio.create_task(self._fleet_liveness_poll())
            logger.info(
                "orchestrator.hub | fleet-liveness sweep started roster={} interval={}s",
                ",".join(self._config.fleet_expected),
                self._config.fleet_liveness_sweep_s,
            )
        self._start_intake_poller()
        logger.info(
            "orchestrator.hub | ready connection={} env={} dry_run={}",
            self._config.bus.name,
            self.env,
            self._dry_run,
        )

    async def _serve_arbitration(self) -> None:
        """Stand up one arbitration responder per configured topic (#43).

        Arbitrate is request/reply: a worker publishes on
        ``dev.agent.arbitrate.<topic>`` and blocks on a reply. The shipped
        :meth:`Orchestrator.serve_arbitration` answers deterministically via
        :func:`~fleet_orchestrator.detect.decide_arbitration` over the hub's
        configured workers and replies on the request's reply subject. Held as a
        service-level subscription (hub-wide, not per-task) and torn down in
        :meth:`aclose`.

        Opt-in per topic: disabled (``arbitration_enabled=False``) or with no
        configured topics stands up nothing and the hub behaves exactly as before.
        """
        if not self._config.arbitration_enabled:
            logger.info("orchestrator.hub | arbitration disabled; no responder wired")
            return
        if not self._config.arbitrate_topics:
            logger.info(
                "orchestrator.hub | arbitration enabled but no topics configured;"
                " no responder wired"
            )
            return
        for topic in self._config.arbitrate_topics:
            sub = await self._orchestrator.serve_arbitration(topic, self._config.workers)
            self._service_subs.append(cast("_Unsubscribable", sub))
            logger.info(
                "orchestrator.hub | arbitration responder wired topic={} workers={}",
                topic,
                ",".join(self._config.workers),
            )

    def _start_intake_poller(self) -> None:
        """Start the k-board intake poller unless disabled by config.

        The poller is the only shipped publisher of the ``created`` event the
        intake lane consumes, so without it the durable service never auto-opens a
        session from a new card (#44). Disabled (``intake_poll_enabled=False``)
        stands up nothing and the hub behaves as before — intake then only reaches
        it via an external ``created`` publisher (a real k-board webhook) or the
        ``--once`` path.
        """
        if not self._config.intake_poll_enabled:
            logger.info("orchestrator.hub | intake poller disabled; no created publisher wired")
            return
        self._intake_poller = IntakePoller(
            self._kboard,
            self._bus,
            env=self.env,
            sender=self._config.manager,
            default_workers=self._config.workers,
            channels=self._channels,
            home_channel=self._config.intake_channel,
            new_id=self._clock.new_id,
            now=self._clock.now,
            poll_s=self._config.intake_poll_s,
            label=self._config.intake_label,
        )
        self._intake_task = asyncio.create_task(self._intake_poller.run(self._stop))

    async def _on_intake(self, envelope: Envelope) -> None:
        """Open a coordination session for a newly-created k-board task."""
        intake = parse_intake(envelope, default_workers=self._config.workers)
        if intake is None:
            logger.warning(
                "orchestrator.hub | intake without task_id sender={}; dropped", envelope.sender
            )
            return
        await self.start_task(intake)

    async def _persist_governor(self, task_id: str) -> None:
        """Durably persist the governor counters for ``task_id`` if durable is on.

        Thin gate around :meth:`Governor.persist_state` so control-op handlers do
        not repeat the durable check. No-op when durable mode is off = today's
        byte-for-byte behavior (no k-board call).
        """
        if self._governor.durable:
            await self._governor.persist_state(task_id)

    async def _on_control(self, envelope: Envelope) -> None:
        """Apply an out-of-band control op to the LIVE governor (#59).

        The one external lever on the running governor: a paused (over-budget)
        task is only resumable by this in-process instance, so a harness or a
        Nagatha-side steer publishes a ``control`` frame on the hub's control
        lane and it lands here. The op set is closed; anything else — a malformed
        frame, an unknown op — is a LOUD no-op with the sender recorded, never a
        crash and never a silent swallow (fleet-bus #59 fail-safe).

        Ops:
            resume: clear the governor pause (+ optional ``budget_msgs`` bump) so
                gated wake/dispatch/relay flow again.
            pause: gate the task's bus behavior on operator demand (symmetry).
        """
        op = parse_control(envelope)
        if op is None:
            # Not a known/well-formed control op — record the sender + kind so an
            # operator can see the rejected frame, then drop it.
            logger.warning(
                "orchestrator.control | ignored malformed/unknown control frame"
                " sender={} kind={} task={} payload_op={!r}",
                envelope.sender,
                envelope.kind,
                envelope.task_id,
                envelope.payload.get("op"),
            )
            return
        if op.op == OP_RESUME:
            resumed = self._governor.resume(op.task_id, budget_msgs_add=op.budget_msgs_add)
            logger.info(
                "orchestrator.governor | resumed via control op task={} sender={}"
                " budget_msgs_add={} was_paused={} reason={!r}",
                op.task_id,
                envelope.sender,
                op.budget_msgs_add,
                resumed,
                op.reason,
            )
            # Persist the operator decision (fleet-bus #106): a resume + cap bump is
            # a durable steer — a crash right after must not discard it and re-pause
            # the task. No-op when durable mode is off = today's behavior.
            await self._persist_governor(op.task_id)
        elif op.op == OP_PAUSE:
            paused = self._governor.pause(op.task_id)
            logger.info(
                "orchestrator.governor | paused via control op task={} sender={}"
                " took_effect={} reason={!r}",
                op.task_id,
                envelope.sender,
                paused,
                op.reason,
            )
            # Persist the operator pause so a crash does not resume a task the
            # operator deliberately halted (fleet-bus #106). No-op when flag off.
            await self._persist_governor(op.task_id)
        else:  # pragma: no cover - parse_control only returns known ops
            logger.warning(
                "orchestrator.control | unhandled control op={} task={} sender={}; no-op",
                op.op,
                op.task_id,
                envelope.sender,
            )

    @traced
    async def start_task(self, intake: IntakeTask) -> CoordinationSession:
        """Begin coordinating one task: dispatch, then watch until done.

        Guards on ``task_id`` against a duplicate intake for a task already
        coordinated (a duplicate ``created`` from an app-level re-publish —
        intake_poller re-emit / reconnect re-send; the transport is at-most-once,
        C5, not a redelivering bus). Dispatches the task to the
        workers, then starts the per-task watches (status/relay/done +
        scorekeeper + offline). The session snapshot is returned for callers
        (the ``--once`` path) that want to await completion.

        Args:
            intake: The parsed intake identity.

        Returns:
            The dispatched session (state WORKING).
        """
        if intake.task_id in self._tasks:
            logger.info(
                "orchestrator.hub | intake for already-coordinated task={}; ignored",
                intake.task_id,
            )
            return self._tasks[intake.task_id].session_ref()

        session = CoordinationSession(
            task_id=intake.task_id,
            channel=intake.channel or "",
            manager=self._config.manager,
            workers=intake.workers,
        )
        logger.info(
            "orchestrator.hub | start task={} channel={} workers={}",
            session.task_id,
            session.channel,
            ",".join(session.workers),
        )

        # Build the dispatch event ONCE: it is both size-prechecked (below) and
        # published (via dispatch_task). The prompt text rides as payload["text"]
        # (the whole point of #52). An empty/whitespace-only prompt still opens the
        # coordination session but WARNs loudly — a worker cannot act on an empty
        # turn and the subscriber drops it, so a silent empty dispatch is the exact
        # failure that shipped (codex review, MAJOR: decide "present" on stripped text).
        text = intake.text
        if text.strip():
            logger.info(
                "orchestrator.dispatch | task={} text present len={}", session.task_id, len(text)
            )
            logger.debug(
                "orchestrator.dispatch | task={} text preview={!r}", session.task_id, text[:80]
            )
        else:
            logger.warning(
                "orchestrator.dispatch | WARNING empty text task={} channel={};"
                " worker will receive no prompt",
                session.task_id,
                session.channel,
            )
        dispatch_event = Event(
            kind=EventKind.DISPATCH,
            context={
                "text": text,
                "task_id": session.task_id,
                "channel": session.channel,
            },
        )

        # SIZE-GUARD BEFORE OWNERSHIP (round 3 controller ruling): render + size-check
        # EVERY fan-out envelope before persisting WORKING, before any publish, and
        # before adding to _tasks. An oversized dispatch is PERMANENT (no retry ever
        # fits it), so it must abort with zero side effects — no partial fan-out, no
        # WORKING persistence, no ownership. While the card is still ``todo`` we
        # dead-letter it to ``blocked``; if that flip fails we leave it ``todo`` and
        # walk away — the next poller cycle re-picks it and IS the retry owner (no
        # extra machinery). Never falsely DONE.
        #
        # render_and_check_dispatch renders the envelopes ONCE and returns them; the
        # real dispatch below publishes those EXACT artifacts verbatim, so the bytes
        # sized here are the bytes published — no id/timestamp re-render can slip a
        # boundary-case envelope past the check (round 4 parity).
        try:
            prerendered = self._orchestrator.render_and_check_dispatch(session, dispatch_event)
        except PayloadTooLargeError as exc:
            logger.error(
                "orchestrator.hub | dispatch PERMANENTLY undispatchable (oversized) "
                "task={} size={} limit={}; NOT dispatched, NOT owned; dead-lettering "
                "while still todo",
                intake.task_id,
                exc.size,
                exc.limit,
            )
            if await self._dead_letter_card(intake.task_id):
                # Parked off ``todo`` -> ``blocked``. Terminal, correctly reported.
                return session.advanced_to(State.DONE, outcome="blocked")
            # Flip failed (k-board down): the card is untouched on ``todo``, so the
            # next intake poll re-picks it and retries this exact path once k-board
            # heals. Nothing is owned and nothing is falsely DONE — return the
            # pre-dispatch snapshot (state DISPATCHED, not DONE).
            logger.error(
                "orchestrator.hub | oversized card blocked NOT committed/reported "
                "task={}; durable state may be unchanged or concurrently terminal",
                intake.task_id,
            )
            return session

        # Reserve the task key SYNCHRONOUSLY, before any await, so a second intake
        # for the same task_id (a duplicate created from an app-level re-publish —
        # transport is at-most-once, C5) can never race
        # past the duplicate guard while the first is awaiting its subscribe
        # (TOCTOU). The accessor starts as the WORKING session and is replaced by
        # watch_worker_status's cell accessor once subscribed.
        working_session = session.advanced_to(State.WORKING)
        watches = _TaskWatches(session_ref=lambda: working_session)
        self._tasks[intake.task_id] = watches

        # Durable governor RESUME (fleet-bus #106): if durable mode is on, rehydrate
        # this task's budget/pause counters from k-board BEFORE any coordination runs
        # — a hub that crashed mid-task continues its counters instead of resetting
        # them (the crash-loop-spends-unbounded failure). No-op (returns False,
        # zero cost, no k-board call) when the flag is off = today's behavior.
        if self._governor.durable:
            resumed = await self._governor.hydrate(intake.task_id)
            logger.info(
                "orchestrator.hub | durable governor hydrate task={} resumed={}",
                intake.task_id,
                resumed,
            )
            if self._governor.is_paused(intake.task_id):
                logger.warning(
                    "orchestrator.hub | durable governor hydrate held task={} paused; "
                    "NOT persisting WORKING, narrating dispatched, or dispatching work",
                    intake.task_id,
                )
                return session

        # Any failure during setup/dispatch must ROLL BACK the reservation and
        # cancel whatever was already subscribed — otherwise the reserved key
        # would make every future intake for this task a permanent no-op, and a
        # half-registered subscription would leak (codex cross-review, #3).
        try:
            # Subscribe ONCE before dispatch so no status is missed (core NATS has
            # no backlog). watch_worker_status composes the hub observer with its
            # relay/done state machine behind this one physical callback.
            status_subs, session_ref = await self._orchestrator.watch_worker_status(
                working_session,
                lambda env: self._observe_task_status(session, env),
            )
            watches.session_ref = session_ref
            # BOTH status forms (#117): bare and channel-scoped. Every handle must
            # be tracked here or the rollback/teardown path leaks a subscription.
            watches.subs.extend(status_subs)

            # Persist the WORKING transition durably (k-board first, then emit) and
            # narrate the dispatch beat, then fan the task out to the workers. The
            # persist is best-effort (see _persist). Payload size was already proven
            # to fit by render_and_check_dispatch above, and its exact rendered
            # envelopes are published verbatim here, so dispatch cannot raise
            # PayloadTooLargeError; any other failure rolls back.
            working = session.advanced_to(State.WORKING)
            await self._persist(working)
            if self._narrator is not None:
                await self._narrator.narrate(
                    working, "dispatched", target=",".join(session.workers)
                )
            await self._orchestrator.dispatch_task(
                session, dispatch_event, prerendered_dispatch=prerendered
            )
            # Watch EACH worker for silence independently. The shipped
            # OfflineDetector keys a watch by its task_id argument, so a per-worker
            # composite key gives every fan-out target its own ladder — without it
            # note_dispatch would overwrite and only the last worker be watched.
            for worker in session.workers:
                self._offline.note_dispatch(
                    offline_key(session.task_id, worker), worker, now=self._monotonic()
                )
            return working_session
        except Exception:
            logger.error("orchestrator.hub | start failed task={}; rolling back", intake.task_id)
            self._tasks.pop(intake.task_id, None)
            for sub in watches.subs:
                await _cancel_sub(sub)
            for worker in session.workers:
                self._offline.note_done(offline_key(session.task_id, worker))
            raise
        return working

    async def _observe_task_status(self, session: CoordinationSession, env: Envelope) -> None:
        """Observe one status delivery for score, offline state, and durable done.

        Composed inside ``watch_worker_status``'s one physical subscription. On
        each status envelope it, in order: (1) feeds
        the scorekeeper and narrates any circlejerk/decision, (2) on a ``done``
        sentinel persists the terminal state (built from the sentinel outcome, not
        from a cell) and tears the task down, else (3) resets that worker's
        offline ladder. ``watch_worker_status`` rejects a foreign done before
        invoking this observer.
        """
        await self._observe_score(session, env)
        # Durable governor PERSIST is NOT done here (fleet-bus #106, codex terminal
        # review): this observer is score/offline/terminal only. Persisting here
        # would race the counting callback (watch_worker_status → Orchestrator.apply
        # → note_message) — the snapshot could capture msgs=0 before the count
        # landed, and a restart would hydrate a count LOWER than processed. The
        # persist lives CAUSALLY in the counting handler (behaviors.Orchestrator,
        # right after apply), so it always observes the post-increment count.
        done = parse_done(env)
        if done is not None:
            final = session.advanced_to(State.DONE, outcome=done.outcome)
            for worker in session.workers:
                self._offline.note_done(offline_key(session.task_id, worker))
            self._scorekeeper.note_done(session.task_id)
            await self._finish_task(session.task_id, final)
            return
        if env.sender:
            self._offline.note_status(
                offline_key(session.task_id, env.sender), now=self._monotonic()
            )

    async def _observe_score(self, session: CoordinationSession, env: Envelope) -> None:
        """Feed the scorekeeper; on a circlejerk trip, decide and act on it.

        On a trip the deterministic decider runs: a ``DECIDE`` verdict narrates
        the evidenced winner; a ``SYNTHESIZE`` verdict dispatches a synthesis TURN
        to the manager's own inbox via the shipped :func:`assign_synthesis` (the
        manager arbitrates by taking a turn — she never rewrites worker output).
        Read-only w.r.t. the coordination cell.
        """
        event = self._scorekeeper.observe(env)
        if event is None:
            return
        score = self._scorekeeper.score_for(session.task_id)
        # observe() records the topic before it can trip an event, so score_for is
        # always populated here; the fallback keeps the type checker honest.
        decision = decide(score if score is not None else Score(task_id=session.task_id))
        logger.info(
            "orchestrator.hub | score decision task={} kind={} winner={}",
            session.task_id,
            decision.kind.value,
            decision.winner,
        )
        if self._narrator is not None:
            await narrate_score_event(self._narrator, session, event, now=self._clock.now())
            await narrate_decision(self._narrator, session, decision, now=self._clock.now())
        # A SYNTHESIZE verdict is NOT gate logic: it hands the manager a normal
        # turn on her own inbox (shipped behavior). This runs even in dry-run — it
        # is a coordination dispatch, not a Discord narration beat.
        if decision.kind is DecisionKind.SYNTHESIZE:
            await assign_synthesis(
                self._bus,
                session,
                env=self.env,
                new_id=self._clock.new_id(),
                now=self._clock.now(),
            )

    async def _dead_letter_card(self, task_id: str) -> bool:
        """Flip a permanently-undispatchable (oversized) card to ``blocked``.

        Called from :meth:`start_task`'s pre-ownership size guard (round 3): an
        oversized dispatch is caught BEFORE any publish/persist/ownership while the
        card is still ``todo``. Flipping it ``todo`` -> ``blocked`` durably parks it
        so the intake poller stops re-picking it.

        Returns ``True`` iff the flip durably succeeded. On failure it is logged
        loudly and ``False`` is returned — the caller must NOT report the task
        terminally ``blocked`` (claiming DONE would be a lie) and instead leaves the
        card on ``todo`` so the next poll cycle re-picks and retries. It never
        re-raises; the caller owns the surface-and-leave-on-todo decision.
        """
        try:
            updated = await self._kboard.update_status(task_id, "blocked")
        except Exception as exc:  # noqa: BLE001 — surfaced via the return, never swallowed
            logger.error(
                "orchestrator.hub | dead-letter flip failed task={} status=blocked: {}",
                task_id,
                exc,
            )
            return False
        if updated.status != "blocked":
            logger.warning(
                "orchestrator.hub | dead-letter request suppressed task={} "
                "durable_status={} (not reporting blocked)",
                task_id,
                updated.status,
            )
            return False
        logger.warning(
            "orchestrator.hub | dead-lettered oversized card task={} -> blocked", task_id
        )
        return True

    async def _persist(self, session: CoordinationSession) -> None:
        """Best-effort durable persist of a transition — log-loud, never fatal.

        k-board is the source of truth WHEN reachable, but a single task's
        persist failure (an outage, a task not yet on the board) must not take the
        hub down: coordination still runs over the bus and the next successful
        write reconciles the card. This is the log-before-degrade discipline
        (docs/coding-standards.md — no silent swallow, no fatal escalation).

        Note the wording: ``persist_transition`` writes k-board FIRST and then
        emits the ephemeral kboard_event, and it deliberately lets an *emit*
        failure raise AFTER the durable write committed (behaviors.py). So a raise
        here does not always mean the durable write failed — it may mean the write
        landed but the bus emit didn't. The log says "persist-or-emit failed" so
        it never falsely asserts the durable write was lost.
        """
        try:
            await self._orchestrator.persist_transition(session, self._kboard)
        except Exception as exc:  # noqa: BLE001 — durable path degrades loudly, never fatally
            logger.error(
                "orchestrator.hub | persist-or-emit failed task={} state={}: {}",
                session.task_id,
                session.state.value,
                exc,
            )

    async def _finish_task(self, task_id: str, final: CoordinationSession) -> None:
        """Persist the terminal state, THEN tear down a completed task's watches.

        ``final`` is the DONE session built from the sentinel outcome by the
        caller — NOT read from a cell another subscription is racing to advance,
        so the durable ``done`` write cannot be skipped by delivery ordering.

        Ordering (codex cross-review, #2): persist FIRST while the task is still
        tracked, and only untrack + tear down on a SUCCESSFUL durable write. On
        failure the task stays tracked and its subscriptions stay live, so that IF
        another ``done`` for this task arrives it will retry the persist — keyed on
        ``task_id`` (tracked-map presence), and the CAS write is idempotent, so a
        second ``done`` after a SUCCESSFUL finish finds the task already untracked
        and returns without a second persist/teardown.

        GAP (recorded on #114): keeping subscriptions live does NOT by itself
        produce that retry. The shipped ``done`` producer
        (:mod:`fleet_subscriber` ``send``) emits exactly ONE ``done`` sentinel and
        has no re-emit / reconnect retry loop, and the transport is at-most-once
        (C5) — it will not re-deliver the ``done`` for us. So absent a second,
        independent agent completion, a persist failure here can leave the card
        ``running`` indefinitely. Closing this needs an explicit orchestrator-side
        retry/reconciliation of failed terminal writes (NOT shipped); this method
        only holds the door open for one, it does not guarantee one.
        """
        watches = self._tasks.get(task_id)
        if watches is None:
            return  # already finished (duplicate done) — idempotent no-op
        try:
            await self._orchestrator.persist_transition(final, self._kboard)
        except Exception as exc:  # noqa: BLE001 — hold the task tracked (see GAP), log loud
            logger.error(
                "orchestrator.hub | terminal persist failed task={} (kept tracked; needs a "
                "further done or reconciliation to retry — see #114): {}",
                task_id,
                exc,
            )
            return
        if not await self._governor.clear_state(task_id):
            logger.error(
                "orchestrator.hub | terminal governor cleanup failed task={} (kept tracked; "
                "needs a further done or reconciliation to retry)",
                task_id,
            )
            return
        # Durable DONE committed — now it is safe to untrack and tear down.
        self._tasks.pop(task_id, None)
        if self._narrator is not None:
            await self._narrator.narrate(final, "done", outcome=final.outcome or "completed")
        for sub in watches.subs:
            await _cancel_sub(sub)
        logger.info("orchestrator.hub | finished task={} state={}", task_id, final.state.value)

    @traced
    async def poll_offline_once(self) -> None:
        """Advance the offline ladder ONE tick and apply any fired escalations.

        Cross-checks liveness (a stale heartbeat skips the wake rung) and applies
        each fired escalation through the shipped
        :meth:`Orchestrator.apply_offline_decision`. Split out from the poll loop
        so a single tick is drivable deterministically under test (no sleep race).
        """

        def _is_stale(worker: str) -> bool | None:
            return self._liveness.is_stale(
                worker,
                now=self._monotonic(),
                interval_s=self._config.heartbeat_interval_s,
            )

        decisions = self._offline.check(now=self._monotonic(), is_stale=_is_stale)
        for decision in decisions:
            # decision.task_id is the per-worker composite key; unmap to the real
            # task id for the task lookup and the k-board BLOCKED record.
            real_task_id, _ = unkey(decision.task_id)
            watches = self._tasks.get(real_task_id)
            if watches is None:
                continue  # task finished between check() and apply
            session = watches.session_ref()
            real_decision = replace(decision, task_id=real_task_id)
            try:
                await self._orchestrator.apply_offline_decision(
                    session, real_decision, self._kboard
                )
            except Exception as exc:  # noqa: BLE001 — one bad rung must not stop the poll
                logger.error(
                    "orchestrator.hub | offline escalation failed task={} worker={}: {}",
                    real_task_id,
                    decision.worker,
                    exc,
                )

    async def _offline_poll(self) -> None:
        """Run :meth:`poll_offline_once` every poll interval until stopped.

        The one runtime clock the service reads (monotonic, injected).
        """
        interval = self._config.offline_poll_s
        while not self._stop.is_set():
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=interval)
                return  # stop signalled during the wait
            except TimeoutError:
                pass  # a normal poll tick
            await self.poll_offline_once()

    @traced
    async def poll_fleet_liveness_once(self) -> None:
        """Advance the fleet-liveness roster sweep ONE tick and narrate transitions.

        Report-only (fleet-bus #78): each fired transition is narrated on the
        manager's inbox via the EXISTING session-less narrator entry point
        (``narrate_task``, keyed by the synthetic ``"fleet"`` task id) and nothing
        else — no wake, no redispatch, no cell mutation. Split out from the poll
        loop so a single tick is drivable deterministically under test, mirroring
        :meth:`poll_offline_once`. A no-op when the sweep is disabled (no reporter).
        """
        if self._fleet_liveness is None:
            return

        def _is_stale(agent: str) -> bool | None:
            return self._liveness.is_stale(
                agent,
                now=self._monotonic(),
                interval_s=self._config.heartbeat_interval_s,
            )

        transitions = self._fleet_liveness.sweep(now=self._monotonic(), is_stale=_is_stale)
        if self._narrator is None:
            return  # dry-run: the sweep still ran + logged, but no beat is surfaced
        for transition in transitions:
            beat = "fleet_silent" if transition.kind == TransitionKind.MISSING else "fleet_present"
            # narrate_task never raises (it logs a failed publish), so one bad beat
            # cannot stop the sweep — but keep the loop honest with the same
            # per-item guard the offline poll uses.
            try:
                await self._narrator.narrate_task(
                    "fleet",
                    beat,
                    # The hub's fleet channel is the honest target for a hub-level
                    # (session-less) beat: without it the envelope reaches the
                    # manager's subscriber channel-less and envelope_to_message
                    # drops it (no chat_id) — the Phase-1 session-less-beat drop
                    # (fleet-bus #72). ``or None`` preserves the unconfigured no-op.
                    channel=self._config.intake_channel or None,
                    target=transition.agent,
                    detail=f"{transition.silent_sweeps} sweeps",
                )
            except Exception as exc:  # noqa: BLE001 — one bad beat must not stop the sweep
                logger.error(
                    "orchestrator.hub | fleet-liveness narrate failed agent={} kind={}: {}",
                    transition.agent,
                    transition.kind,
                    exc,
                )

    async def _fleet_liveness_poll(self) -> None:
        """Run :meth:`poll_fleet_liveness_once` every sweep interval until stopped.

        Mirrors :meth:`_offline_poll`: wait on the stop event with a timeout so a
        stop signalled mid-interval returns promptly, and run one sweep per tick.
        """
        interval = self._config.fleet_liveness_sweep_s
        while not self._stop.is_set():
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=interval)
                return  # stop signalled during the wait
            except TimeoutError:
                pass  # a normal sweep tick
            await self.poll_fleet_liveness_once()

    @traced
    async def run(self) -> None:
        """Start the hub and serve until :meth:`stop` (SIGINT/SIGTERM wired by the entrypoint)."""
        await self.start()
        try:
            await self._stop.wait()
        finally:
            await self.aclose()

    def stop(self) -> None:
        """Signal the hub to shut down (safe to call from a signal handler)."""
        self._stop.set()

    @traced
    async def aclose(self) -> None:
        """Cancel the offline poll and every subscription (idempotent)."""
        self._stop.set()
        if self._offline_task is not None:
            self._offline_task.cancel()
            try:
                await self._offline_task
            except asyncio.CancelledError:
                logger.debug("orchestrator.hub | offline poll cancelled")
            self._offline_task = None
        if self._fleet_liveness_task is not None:
            self._fleet_liveness_task.cancel()
            try:
                await self._fleet_liveness_task
            except asyncio.CancelledError:
                logger.debug("orchestrator.hub | fleet-liveness sweep cancelled")
            self._fleet_liveness_task = None
        # The kill-switch watcher runs on a dedicated OS thread (P1-2). Signal its
        # threading.Event and JOIN it so teardown leaves no leaked thread. The
        # watcher's stop.wait(timeout) returns the instant the event is set, so the
        # join returns promptly (a short bounded timeout guards the pathological case
        # of a stat wedged on a dead mount — the process still tears down, it just
        # logs that the watcher thread outlived the join). The join is offloaded so
        # aclose never blocks the event loop on a thread.
        await asyncio.get_running_loop().run_in_executor(None, self._kill_switch.stop)
        # The intake poller's run loop returns promptly on the _stop set above; the
        # cancel is belt-and-suspenders for a task mid-tick (a k-board list in
        # flight), so teardown never blocks on a slow board response.
        if self._intake_task is not None:
            self._intake_task.cancel()
            try:
                await self._intake_task
            except asyncio.CancelledError:
                logger.debug("orchestrator.hub | intake poller cancelled")
            self._intake_task = None
        self._intake_poller = None
        service_subs = self._service_subs
        self._service_subs = []
        for sub in service_subs:
            await _cancel_sub(sub)
        # Snapshot + clear BEFORE awaiting: _cancel_sub awaits, during which a
        # still-live status callback could reach _finish_task and pop _tasks,
        # mutating the dict mid-iteration (codex cross-review, #6).
        task_watches = list(self._tasks.values())
        self._tasks.clear()
        for watches in task_watches:
            for sub in watches.subs:
                await _cancel_sub(sub)
        logger.info("orchestrator.hub | closed manager={}", self._config.manager)


async def _cancel_sub(sub: _Unsubscribable) -> None:
    """Unsubscribe one NATS subscription, tolerating an already-drained bus."""
    try:
        await sub.unsubscribe()
    except Exception as exc:  # noqa: BLE001 — teardown must not raise past shutdown
        logger.debug("orchestrator.hub | unsubscribe ignored: {}", exc)


@traced
async def build_hub(
    config: HubConfig,
    *,
    clock: IdClock,
    monotonic: Callable[[], float],
    dry_run: bool = False,
) -> tuple[OrchestratorHub, FleetBus, KboardClient]:
    """Connect the bus + k-board, load the subject catalog, and wire a hub.

    The subject catalog is loaded ONCE here (at connect) via ``connect_catalog``,
    which enforces the configured version floor (#103). With no floor set
    (``FLEET_SUBJECT_CATALOG_MIN_VERSION`` unset/0) it fails open to the baked-in
    code defaults with a loud log when the registry is unreachable — the bus must
    never fail closed on Postgres (fleet-board #15). With a floor set, a
    below-floor catalog (including the version-0 code fallback on a registry
    outage) raises ``CatalogVersionTooLowError`` and the hub refuses to start —
    a below-floor orchestrator speaks an obsolete taxonomy and splits the fleet.

    Args:
        config: The validated service config.
        clock: Injected id/timestamp factories.
        monotonic: Injected monotonic clock for the offline poll.
        dry_run: Pass through to the hub (no narrator when true).

    Returns:
        ``(hub, bus, kboard)`` — the caller owns closing the bus and k-board
        (the hub's ``aclose`` cancels subscriptions but does not own the bus
        connection, mirroring the injected-collaborators pattern).
    """
    # Tick-zero kill enforcement is deliberately FIRST.  The isolated store probe
    # fails closed while unavailable, so neither a catalog outage nor a saturated
    # coordination bus can make this process accept fleet work before authority is
    # known.
    governor = Governor(config.budget, ledger=CostLedger(), store=None)
    store_probe = StoreKillProbe(config.bus)
    kill_switch = KillSwitch(
        governor,
        poll_s=config.kill_switch_poll_s,
        probe=store_probe.read,
        store_probe=store_probe,
    )
    kill_switch.start()
    # Enforce the catalog version floor BEFORE opening the coordination bus (#103): a
    # below-floor hub must refuse to start, not connect and coordinate on an
    # obsolete taxonomy. connect_catalog raises CatalogVersionTooLowError when a
    # floor is configured and unmet; that propagates out of hub setup by design.
    try:
        catalog = await connect_catalog()
        bus = FleetBus(config.bus)
        await bus.connect()
        kboard = KboardClient()
    except Exception:
        kill_switch.stop()
        raise
    logger.info(
        "orchestrator.hub | catalog source={} version={} subjects={}",
        catalog.source,
        catalog.version,
        len(catalog.subjects),
    )
    trace.set_sink(trace.sink_from_env())
    hub = OrchestratorHub(
        config,
        bus,
        kboard,
        clock=clock,
        monotonic=monotonic,
        catalog=catalog,
        dry_run=dry_run,
        governor=governor,
        kill_switch=kill_switch,
    )
    return hub, bus, kboard


__all__ = [
    "IntakeTask",
    "OrchestratorHub",
    "build_hub",
    "parse_intake",
]
