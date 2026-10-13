"""Bus-wired coordination behaviors — the state machine, connected to NATS.

Phase 1 gave us a pure ``next_action(session, event) -> Action``. This module
performs the actions over a live :class:`~fleet_nats.bus.FleetBus`: it maps each
:class:`~fleet_orchestrator.transitions.ActionKind` onto the concrete NATS
behavior issue #4 calls for — **dispatch**, **relay**, **arbitrate**, **wake** —
building every subject *only* through :mod:`fleet_nats.subjects`.

The orchestrator stays deterministic: it consults the pure state machine and the
pure detectors (:mod:`fleet_orchestrator.detect`); NATS is delivery, never
decision. No LLM anywhere in this path.

Ids and timestamps are injected (an ``IdClock``) so every published envelope is
reproducible under test, mirroring ``Envelope.new``'s caller-supplied id/ts
contract.
"""

from __future__ import annotations

import time
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING

from fleet_nats import Envelope, FleetBus, subjects
from fleet_nats.sentinel import TASK_ID_KEY
from fleet_nats.tracing import traced
from loguru import logger

from fleet_orchestrator import trace
from fleet_orchestrator.detect import decide_arbitration, detect_missing_handoff
from fleet_orchestrator.governor import Breach, BreachKind, Governor, Narrator
from fleet_orchestrator.kboard import KboardClient, event_for_state, status_for_state
from fleet_orchestrator.offline import Escalation, OfflineDecision
from fleet_orchestrator.sentinel import done_payload_field, parse_done
from fleet_orchestrator.state import CoordinationSession, Event, EventKind
from fleet_orchestrator.transitions import Action, ActionKind, next_action

if TYPE_CHECKING:
    from nats.aio.msg import Msg
    from nats.aio.subscription import Subscription


@dataclass(frozen=True)
class IdClock:
    """Injected id + timestamp sources so published envelopes stay deterministic.

    The bus layer must stamp every envelope with an id and timestamp, but calling
    ``uuid4``/``now`` inline would make published output non-reproducible under
    test (the exact failure the envelope contract avoids by taking caller ids).
    Callers pass factories; tests pass counters.

    Attributes:
        new_id: Returns a fresh message id.
        now: Returns an ISO-8601 UTC timestamp string.
    """

    new_id: Callable[[], str]
    now: Callable[[], str]


@dataclass(frozen=True)
class _RelayContext:
    """Identity and timing for one inbound status that decided a relay."""

    envelope_id: str
    sender: str
    now: float


class Orchestrator:
    """Drives coordination for one manager identity over the bus.

    Owns a :class:`FleetBus` and, per task, a :class:`CoordinationSession`. Each
    inbound :class:`Event` is run through the pure state machine; the returned
    :class:`Action` is performed as a NATS behavior. The session is *always*
    persisted from ``action.session`` (a NOOP means "no bus behavior", not "no
    state change" — see :class:`ActionKind`).

    Relay-loop reliability is owned at this behavior boundary:
    :meth:`watch_worker_status` identifies actual missing-handoff relay edges,
    :class:`Governor` tracks their lifecycle and pauses a rapid alternating run,
    and terminal completion releases the retained per-task governor state.
    """

    def __init__(
        self,
        bus: FleetBus,
        clock: IdClock,
        *,
        governor: Governor | None = None,
        narrator: Narrator | None = None,
        kboard: KboardClient | None = None,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        """Store the bus, id/clock sources, and the optional #13 controls.

        Args:
            bus: The (not yet connected) fleet bus.
            clock: Injected id/timestamp factories.
            governor: The budget governor. When present, every non-terminal
                event counts against the task's budget and every decided action
                passes through :meth:`Governor.gate` before it touches the bus —
                a paused task performs no bus behavior (``wake_worker`` and
                ``redispatch`` honor the pause too). ``None`` = ungoverned.
            narrator: The single-throat narrator. When present alongside the
                governor, a stop condition immediately narrates its typed pause
                beat.
            kboard: The k-board client. When present, a stop condition also
                writes the durable pause event the runbook's resume procedure
                reads (the BLOCKED-record precedent).
            monotonic: Injected monotonic clock used only to timestamp observed
                relay edges for deterministic window evaluation.
        """
        self._bus = bus
        self._clock = clock
        self._governor = governor
        self._narrator = narrator
        self._kboard = kboard
        self._monotonic = monotonic

    @property
    def env(self) -> str:
        """The environment prefix all this orchestrator's subjects carry."""
        return self._bus.config.env

    @property
    def sender(self) -> str:
        """This orchestrator's ``agent_id`` (stamped on every envelope's from)."""
        return self._bus.config.agent_id

    def _envelope(
        self,
        session: CoordinationSession,
        action: Action,
        *,
        to: str | None = None,
        payload: Mapping[str, str] | None = None,
    ) -> Envelope:
        """Build the outbound envelope for an action (kind = the action kind).

        ``to`` overrides the envelope's target; when omitted it defaults to
        ``action.target``. Fan-out dispatch passes the concrete ``worker`` per
        envelope, because ``action.target`` is ``None`` in the fan-out case (the
        worker lives in the subject) and the envelope's ``to`` must still name the
        actual recipient, not ``None``.

        ``payload`` overrides the body; when omitted it defaults to the action's
        own carried context. The relay behavior passes ``session.relay_context``
        explicitly, because the *missed handoff text* the transition stashed lives
        on the advanced session (via ``advanced_to``), not on ``action.context``.
        """
        body = payload if payload is not None else action.context
        return Envelope.new(
            msg_id=self._clock.new_id(),
            ts=self._clock.now(),
            sender=self.sender,
            kind=action.kind.value,
            task_id=session.task_id,
            channel=session.channel,
            to=to if to is not None else action.target,
            payload=dict(body),
        )

    @traced
    async def apply(
        self,
        session: CoordinationSession,
        event: Event,
        *,
        relay_context: _RelayContext | None = None,
        prerendered_dispatch: list[tuple[str, Envelope]] | None = None,
    ) -> Action:
        """Run one event through the machine and perform its bus behavior.

        Args:
            session: The current session snapshot for the task.
            event: The inbound event.
            relay_context: Internal status-ingress metadata for an actual relay
                edge. :meth:`watch_worker_status` supplies it; general callers
                should omit it.
            prerendered_dispatch: When this event produces a DISPATCH action, the
                exact ``(subject, envelope)`` pairs already rendered + size-checked
                by :meth:`render_and_check_dispatch`, published verbatim so no
                re-render can change a byte between the size check and the wire
                (round 4). ``None`` for every non-precheck path (renders on the spot).

        Returns:
            The :class:`Action` taken. ``action.session`` is the new snapshot the
            caller must persist (always, regardless of kind).
        """
        # Governor wiring (#13): the event counts against the task's budget
        # BEFORE the action is decided, and the decided action passes through
        # the pause gate before any bus behavior. A breach on THIS event pauses
        # the task immediately — the very action it produced is already gated.
        # A DONE sentinel is exempt: completion is the budget's GOAL, not
        # back-channel chatter — a task must never be "paused" by finishing.
        breach = (
            self._governor.note_message(session.task_id)
            if self._governor is not None and event.kind is not EventKind.DONE
            else None
        )
        action = next_action(session, event)
        if self._governor is not None:
            if event.kind is EventKind.DONE:
                self._governor.complete_task(session.task_id)
            elif (
                action.kind is ActionKind.RELAY
                and action.target is not None
                and relay_context is not None
            ):
                # A budget breach already pauses this turn; preserve that single
                # controlling reason instead of surfacing two pauses at once.
                if breach is None:
                    breach = self._governor.note_relay(
                        action.session.task_id,
                        relay_context.envelope_id,
                        relay_context.sender,
                        action.target,
                        relay_context.now,
                    )
            elif action.kind is not ActionKind.RELAY:
                self._governor.reset_relay(action.session.task_id)
            action = self._governor.gate(action)
        await self._perform(action, prerendered_dispatch=prerendered_dispatch)
        if breach is not None:
            await self._surface_breach(
                breach, task_id=action.session.task_id, channel=action.session.channel
            )
        # The machine-readable twin of the transition log line (docs/logging.md /
        # #22): state+event+action metadata only, never envelope payloads. Emitted
        # AFTER _perform so a failed publish never leaves a decision record whose
        # state the caller then never persisted — a replayer reading the JSONL
        # alone must not reconstruct a state path the session never took.
        trace.emit(
            trace.TraceRecord(
                ts=self._clock.now(),
                task_id=action.session.task_id,
                state=action.session.state.value,
                event=event.kind.value,
                action=action.kind.value,
                target=action.target,
            )
        )
        return action

    async def _perform(
        self,
        action: Action,
        *,
        prerendered_dispatch: list[tuple[str, Envelope]] | None = None,
    ) -> None:
        """Perform the NATS behavior for ``action``. NOOP does nothing on the bus.

        Every branch builds its subject via :mod:`fleet_nats.subjects` — never a
        hand-formatted string — and logs a sanitized line (subject/kind/target/
        task_id only; never payload contents, which may carry agent prose).

        ``prerendered_dispatch`` (round 4) is only consulted on a DISPATCH action —
        it carries the exact envelopes already size-checked, published verbatim.
        """
        session = action.session
        match action.kind:
            case ActionKind.DISPATCH:
                await self._dispatch(session, action, prerendered_dispatch)
            case ActionKind.RELAY:
                await self._relay(session, action)
            case ActionKind.ARBITRATE:
                # Arbitration is request/reply, not fire-and-forget: the reply is
                # sent by the serve_arbitration() responder (which owns the reply
                # subject on the request Msg). apply()'s ARBITRATE action only
                # records the state transition into ARBITRATING; there is no
                # publish to perform here. The two are wired together by the
                # manager loop (Phase 9), where the same orchestrator both serves
                # arbitration and advances the session.
                logger.debug(
                    "orchestrator.behavior | arbitrate pending task={} topic={}",
                    session.task_id,
                    session.arbitrate_topic,
                )
            case ActionKind.WAKE:
                await self._wake(session, action)
            case ActionKind.COMPLETE:
                logger.info("orchestrator.behavior | complete task={}", session.task_id)
            case ActionKind.NOOP:
                logger.debug("orchestrator.behavior | noop ({})", action.reason)

    def _render_dispatch(
        self, session: CoordinationSession, action: Action
    ) -> list[tuple[str, Envelope]]:
        """Render the per-worker ``(subject, envelope)`` fan-out pairs ONCE.

        A single-target hint on the action goes only to that worker; otherwise the
        task fans out to every worker on the session. Subjects are per-channel so
        dispatches in different Discord channels never cross-contaminate. Each
        envelope's ``to`` is stamped with the concrete worker (in fan-out
        ``action.target`` is ``None``, so the recipient would otherwise be lost).

        Rendering is the ONLY place a dispatch envelope's id/timestamp is minted, so
        rendering exactly once — here — and reusing the artifacts for both the
        pre-ownership size check and the real publish guarantees byte parity (round
        4): the bytes sized are the bytes published.
        """
        targets = (action.target,) if action.target else session.workers
        rendered: list[tuple[str, Envelope]] = []
        for worker in targets:
            subject = subjects.agent_dispatch(self.env, worker, channel=session.channel)
            rendered.append((subject, self._envelope(session, action, to=worker)))
        return rendered

    async def _dispatch(
        self,
        session: CoordinationSession,
        action: Action,
        prerendered: list[tuple[str, Envelope]] | None = None,
    ) -> None:
        """Fan the task out to the worker(s), publishing the rendered envelopes.

        ``prerendered`` carries the exact ``(subject, envelope)`` pairs already
        built (and size-checked) by :meth:`render_and_check_dispatch`; they are
        published VERBATIM so an id/timestamp re-render can never change a byte
        between the size check and the wire (round 4 parity). When absent (any
        non-precheck dispatch path — wake/redispatch reuse), the pairs are rendered
        here on the spot.
        """
        pairs = prerendered if prerendered is not None else self._render_dispatch(session, action)
        for subject, envelope in pairs:
            logger.info(
                "orchestrator.dispatch | subject={} worker={} task={}",
                subject,
                envelope.to,
                session.task_id,
            )
            await self._bus.publish(subject, envelope)
            # Trace AFTER the publish so a record never claims an op an exception
            # prevented (the persist_transition log-ordering precedent).
            trace.emit(
                trace.TraceRecord(
                    ts=self._clock.now(),
                    task_id=session.task_id,
                    action="dispatch",
                    target=envelope.to,
                    subject=subject,
                )
            )

    def render_and_check_dispatch(
        self, session: CoordinationSession, event: Event
    ) -> list[tuple[str, Envelope]]:
        """Render every fan-out dispatch envelope, size-check it, and RETURN them.

        Runs the pure ``next_action`` for the DISPATCH ``event``, renders the exact
        per-worker envelopes ONCE via :meth:`_render_dispatch`, and size-checks each
        against the bus max_payload WITHOUT publishing, persisting, or taking
        ownership. The caller runs this BEFORE any of those side effects so an
        oversized dispatch aborts cleanly — no partial fan-out, no WORKING
        persistence, no ``_tasks`` entry — then threads the RETURNED pairs into the
        real dispatch so the bytes sized here are the bytes published (round 4).

        Returns:
            The rendered ``(subject, envelope)`` pairs (empty if the event does not
            produce a DISPATCH action).

        Raises:
            PayloadTooLargeError: If ANY fan-out envelope exceeds max_payload. The
                first offender is named; nothing is published.
        """
        action = next_action(session, event)
        if action.kind is not ActionKind.DISPATCH:
            return []
        pairs = self._render_dispatch(action.session, action)
        for subject, envelope in pairs:
            self._bus.check_payload_size(subject, envelope)
        return pairs

    async def _relay(self, session: CoordinationSession, action: Action) -> None:
        """Relay the missed context to the peer that was not handed off to."""
        target = action.target
        if not target:
            # The pure core guards against this (targetless RELAY never fires), but
            # never publish to a None subject if a caller somehow bypasses it.
            logger.error(
                "orchestrator.relay | no target; refusing to relay task={}", session.task_id
            )
            return
        # Per-channel (#117), mirroring _render_dispatch: a relay carries context
        # missed in THIS channel's deliberation, so it rides this channel's slice.
        # A channel-less session keeps the bare form (the builder's own fallback),
        # so an unchannelled `--once` run is unchanged.
        subject = subjects.agent_relay(self.env, target, channel=session.channel)
        logger.info(
            "orchestrator.relay | subject={} to={} task={}", subject, target, session.task_id
        )
        # The missed-handoff context lives on the advanced session, not the action.
        await self._bus.publish(
            subject, self._envelope(session, action, payload=session.relay_context)
        )
        trace.emit(
            trace.TraceRecord(
                ts=self._clock.now(),
                task_id=session.task_id,
                action="relay",
                target=target,
                subject=subject,
            )
        )

    async def _wake(self, session: CoordinationSession, action: Action) -> None:
        """Wake a worker that has gone quiet."""
        target = action.target
        if not target:
            logger.error("orchestrator.wake | no target; refusing to wake task={}", session.task_id)
            return
        # Per-channel (#117), same rule as dispatch/relay: a wake nudges a worker
        # that went quiet on THIS channel's task. Channel-less falls back to bare.
        subject = subjects.agent_wake(self.env, target, channel=session.channel)
        logger.info(
            "orchestrator.wake | subject={} worker={} task={}", subject, target, session.task_id
        )
        await self._bus.publish(subject, self._envelope(session, action))
        trace.emit(
            trace.TraceRecord(
                ts=self._clock.now(),
                task_id=session.task_id,
                action="wake",
                target=target,
                subject=subject,
            )
        )

    async def wake_worker(self, session: CoordinationSession, worker: str) -> None:
        """Public API: wake a worker that has gone quiet on this task.

        A worker "going quiet" is an operator/manager judgement (or a Phase-7
        deployment timeout), not a pure state transition — so wake is invoked
        directly here rather than emitted by ``next_action``. It publishes an
        ``agent_wake`` nudge without changing the session state.

        Args:
            session: The task's session (unchanged by a wake).
            worker: The worker id to nudge.
        """
        if self._paused_skip(session, "wake"):
            return
        await self._wake(session, Action(kind=ActionKind.WAKE, session=session, target=worker))

    async def _surface_breach(self, breach: Breach, *, task_id: str, channel: str | None) -> None:
        """Surface a pause (#13): the narration beat + the durable k-board record.

        Best-effort by design: the pause itself already lives in the governor
        (the enforcement is done); a surface failure logs loudly but must never
        take coordination down. The k-board write follows the BLOCKED-record
        precedent — it is what the runbook's resume procedure reads.
        """
        if self._narrator is not None:
            beat = (
                "relay_loop"
                if breach.kind is BreachKind.RELAY_LOOP
                else "at_risk"
                if breach.kind is BreachKind.AT_RISK
                else "paused"
            )
            await self._narrator.narrate_task(task_id, beat, channel=channel, detail=breach.reason)
        if self._kboard is None:
            return
        try:
            await self._kboard.post_event(
                task_id,
                "comment",
                actor=self.sender,
                body=(
                    f"PAUSED by relay-loop reliability control: {breach.reason} — "
                    "human resume required"
                    if breach.kind is BreachKind.RELAY_LOOP
                    else f"PAUSED over budget: {breach.reason} — human resume required"
                ),
            )
        except Exception as exc:  # noqa: BLE001 — the durable surface degrades loudly, not fatally
            logger.error(
                "orchestrator.governor | k-board pause record failed task={}: {}", task_id, exc
            )

    @traced
    async def watch_token_costs(self) -> Subscription:
        """Feed the #27 cost lane into the governor and enforce the token cap.

        Subscribes the env-wide ``token_cost`` wildcard, observes every event
        into the governor's ledger (idempotent against app-level duplicates —
        emitter retry / reconnect re-send; transport is at-most-once, C5 — the
        ledger dedups on the ``(slug(agent_id), task_id, turn_id)`` key), and
        re-checks the task's token cap after each
        first-seen turn. A breach pauses the task and surfaces exactly like a
        message-cap breach. This is what makes ``FLEET_BUDGET_TOKENS`` a live
        control rather than a documented intention.

        Returns:
            The subscription handle (cancel on shutdown).

        Raises:
            ValueError: When no governor with a :class:`CostLedger` is wired —
                a cost watch with nowhere to account spend is a misconfiguration
                to surface at setup, not silently at traffic time.
        """
        governor = self._governor
        ledger = governor.ledger if governor is not None else None
        if governor is None or ledger is None:
            raise ValueError("watch_token_costs requires a governor wired with a CostLedger")
        subject = subjects.token_cost_wildcard(self.env)

        async def _on_cost(env: Envelope) -> None:
            cost = ledger.observe(env)
            if cost is None:
                return  # malformed (logged) or a duplicate turn (deduped)
            breach = governor.check_tokens(cost.task_id)
            await self._persist_governor(cost.task_id)
            if breach is not None:
                await self._surface_breach(breach, task_id=cost.task_id, channel=env.channel)

        logger.info("orchestrator.costfeed | watching subject={}", subject)
        return await self._bus.subscribe(subject, _on_cost)

    def _paused_skip(self, session: CoordinationSession, behavior: str) -> bool:
        """True (and logged) when the governor gates this task's bus behavior.

        The direct-invoke behaviors (wake/redispatch) do not flow through
        ``apply()``'s gate, so they check the governor themselves — a gated task
        performs NO bus behavior, however it is reached. Two orthogonal gates,
        checked in the same order :meth:`Governor.gate` uses:

        * FLEET-WIDE kill (#107, P1-3). ``is_all_paused()`` — the out-of-band
          "stop everything" lever — MUST short-circuit wake/redispatch too, else the
          offline poll loop would keep publishing ``agent_wake``/``agent_dispatch``
          onto the bus under an armed kill, escaping the fleet gate that
          ``apply()``'s path already honors. Checked FIRST: it needs no task_id.
        * Per-task budget pause (#13). ``is_paused(task_id)`` — the original gate.
        """
        if self._governor is None:
            return False
        if self._governor.is_all_paused():
            logger.warning(
                "orchestrator.governor | skipped {} task={} (FLEET KILL SWITCH armed)",
                behavior,
                session.task_id,
            )
            return True
        if not self._governor.is_paused(session.task_id):
            return False
        logger.warning(
            "orchestrator.governor | skipped {} task={} (paused over budget)",
            behavior,
            session.task_id,
        )
        return True

    async def redispatch(self, session: CoordinationSession, worker: str) -> None:
        """Public API: re-dispatch a task to one silent worker (#25, offline).

        NON-idempotent by design — there is NO dedup key for this path (C5). This
        publishes a FRESH envelope (new ``Envelope.id`` via ``_dispatch`` →
        ``_envelope`` → ``new_id``), and the subscriber only dedups on exact
        ``Envelope.id``, so a redispatch is deliberately a distinct turn that
        RUNS: the worker is meant to run the task again (the original evidently
        never ran or died), and idempotency would defeat the recovery. Duplicate
        execution is bounded not by a key but by the offline detector firing the
        rung at most once per silence window, with ``task_id`` reconciliation at
        the k-board record catching a genuine double-run after the fact.
        (Transport is at-most-once, C5; this re-dispatch is an app-level retry,
        not transport redelivery.)

        Args:
            session: The task's session (unchanged by a redispatch).
            worker: The single worker to re-target.
        """
        if self._paused_skip(session, "redispatch"):
            return
        await self._dispatch(
            session,
            Action(
                kind=ActionKind.DISPATCH,
                session=session,
                target=worker,
                reason="offline redispatch",
            ),
        )

    async def apply_offline_decision(
        self, session: CoordinationSession, decision: OfflineDecision, kboard: KboardClient
    ) -> None:
        """Run one offline-escalation decision (#25, Phase 3.4).

        WAKE and REDISPATCH are bus behaviors; BLOCKED is durable k-board state:
        the card's status flips to ``blocked`` (the honest lifecycle state — its
        worker is blocked, not still running) AND a comment records the reason
        (narration arrives with #13). Each worker has its own offline watch, but
        the first worker whose ladder exhausts blocks the shared task; this does
        not wait for every worker to exhaust. The envelope does NOT wait in
        JetStream: k-board is where a blocked task lives.

        Args:
            session: The silent task's session.
            decision: The rung fired by :class:`~fleet_orchestrator.offline.OfflineDetector`.
            kboard: The k-board client for the BLOCKED record.
        """
        match decision.escalation:
            case Escalation.WAKE:
                await self.wake_worker(session, decision.worker)
            case Escalation.REDISPATCH:
                await self.redispatch(session, decision.worker)
            case Escalation.BLOCKED:
                # Flip the card's durable status FIRST (the honest state: this
                # task's worker is blocked, not still "running"), then the
                # human-readable comment. This mirrors persist_transition's
                # "durable write before the visible signal" ordering. Without the
                # status flip the card is a zombie — comment posted, status stuck
                # at "running" forever (fleet-bus #73; observed live on fleet-hub).
                #
                # BLOCKED is fire-once-terminal: the detector pops the watch BEFORE
                # this runs (offline.py), so the branch never re-fires for this
                # rung. The status flip is therefore BEST-EFFORT — if it raises
                # (PATCH rejected / CAS exhaustion / transport timeout) we must NOT
                # lose the comment too (that would be strictly worse than the
                # pre-#73 unconditional comment, with no retry to recover it). Log
                # loud and fall through to the guaranteed comment.
                durable_status: str | None = None
                try:
                    updated = await kboard.update_status(decision.task_id, "blocked")
                except Exception as exc:  # noqa: BLE001 — best-effort flip; comment must still post
                    logger.error(
                        "orchestrator.offline | task={} status flip to 'blocked' failed "
                        "(BLOCKED comment will still be attempted): {}",
                        decision.task_id,
                        exc,
                    )
                else:
                    durable_status = updated.status
                    if durable_status != "blocked":
                        logger.warning(
                            "orchestrator.offline | task={} blocked escalation suppressed; "
                            "durable status={} (no BLOCKED comment)",
                            decision.task_id,
                            durable_status,
                        )
                        return
                try:
                    await kboard.post_event(
                        decision.task_id,
                        "comment",
                        actor=self.sender,
                        body=f"BLOCKED: worker {decision.worker} offline — {decision.reason}",
                    )
                except Exception as exc:  # noqa: BLE001 — fire-once reason must fail loud
                    if durable_status == "blocked":
                        logger.error(
                            "orchestrator.offline | task={} BLOCKED comment failed "
                            "after status=blocked; fire-once reason was not recorded: {}",
                            decision.task_id,
                            exc,
                        )
                    else:
                        logger.error(
                            "orchestrator.offline | task={} BLOCKED comment failed; "
                            "status=blocked was not proven and reason was not recorded: {}",
                            decision.task_id,
                            exc,
                        )
                else:
                    if durable_status == "blocked":
                        logger.error(
                            "orchestrator.offline | task={} BLOCKED worker={} "
                            "status=blocked surfaced to k-board",
                            decision.task_id,
                            decision.worker,
                        )
                    else:
                        logger.error(
                            "orchestrator.offline | task={} BLOCKED comment posted for worker={}; "
                            "status=blocked was not proven",
                            decision.task_id,
                            decision.worker,
                        )

    @traced
    async def serve_arbitration(self, topic: str, workers: tuple[str, ...]) -> Subscription:
        """Subscribe as the arbitration responder for ``topic``.

        Arbitrate is request/reply: a worker sends ``agent_arbitrate(env, topic)``
        and blocks on a reply. The manager answers deterministically via
        :func:`~fleet_orchestrator.detect.decide_arbitration` (a case-statement,
        never an LLM) and replies on the request's reply subject.

        Uses the raw NATS client to subscribe because the reply subject lives on
        the underlying ``Msg`` (``FleetBus.subscribe`` decodes to an Envelope and
        drops it); the reply itself still goes through ``FleetBus.reply`` so the
        envelope contract holds.

        Args:
            topic: The arbitration topic to serve.
            workers: The candidate workers the decision picks among.

        Returns:
            The NATS subscription handle (cancel on shutdown).
        """
        # Reject an empty candidate set up front — otherwise every request would
        # raise ValueError deep inside the raw callback (which has no reply on the
        # failure path), leaving the asker to hang until timeout. Fail loud at
        # setup, where the caller can see it, rather than silently at request time.
        if not workers:
            raise ValueError(f"serve_arbitration: no workers to arbitrate topic {topic!r}")

        subject = subjects.agent_arbitrate(self.env, topic)

        async def _on_request(msg: Msg) -> None:
            try:
                request = Envelope.from_bytes(msg.data)
            except ValueError as exc:
                logger.warning("orchestrator.arbitrate | bad request on {}: {}", subject, exc)
                return
            # raw_subscribe has no post-decode guard (unlike subscribe) — wrap the
            # decide+reply body so a policy error can't escape to the nats callback
            # and kill the subscription; log before returning (no silent pass).
            try:
                decision = decide_arbitration(topic=topic, workers=workers)
                logger.info(
                    "orchestrator.arbitrate | subject={} topic={} decision={} corr={}",
                    subject,
                    topic,
                    decision,
                    request.id,
                )
                reply = Envelope.new(
                    msg_id=self._clock.new_id(),
                    ts=self._clock.now(),
                    sender=self.sender,
                    kind="arbitrate",
                    topic=topic,
                    correlation_id=request.id,
                    payload={"decision": decision},
                )
                await self._bus.reply(msg, reply)
            except Exception as exc:
                logger.error(
                    "orchestrator.arbitrate | failed to answer on {} corr={}: {}",
                    subject,
                    request.id,
                    exc,
                )

        logger.info("orchestrator.arbitrate | serving subject={} topic={}", subject, topic)
        return await self._bus.raw_subscribe(subject, _on_request)

    @traced
    async def watch_worker_status(
        self,
        session: CoordinationSession,
        observe_status: Callable[[Envelope], Awaitable[None]] | None = None,
    ) -> tuple[tuple[Subscription, ...], Callable[[], CoordinationSession]]:
        """Subscribe to worker status and fire a relay when a handoff is missing.

        This is the closed loop issue #4 calls for: worker output arrives on
        ``agent_status``, the deterministic :func:`detect_missing_handoff` decides
        whether the worker forgot to hand off, and if so :meth:`apply` runs the
        relay behavior over the bus — no human, no LLM, no fabricated event.

        The session advances across callbacks (WORKING → RELAY_NEEDED → …), so it
        is held in a mutable cell; the returned accessor exposes the current
        snapshot for the caller to persist (Phase 5 wires that to k-board).

        A status envelope carries the worker's turn output in ``payload["text"]``
        and its sender in ``Envelope.sender``. Only workers *other* than the
        sender are handoff candidates (see :func:`detect_missing_handoff`).
        Missing-handoff decisions contribute one relay edge to the governor;
        valid handoffs reset an in-progress run, while DONE processing releases
        all retained governor state for the task.

        Args:
            session: The task's starting session (typically WORKING).
            observe_status: Optional owning-hub observer run before coordination
                behavior, behind this method's physical subscription.

        Returns:
            ``(subscriptions, current_session_accessor)``. The tuple holds BOTH
            status forms (bare and channel-scoped, #117) — cancel every one on
            shutdown; call the accessor to read the latest session snapshot.
        """
        # Mutable single-element cell so the callback can advance the session and
        # the caller can read the latest snapshot without a shared class field
        # (keeps the Orchestrator itself stateless across tasks).
        cell = [session]
        # BOTH forms (#117). The status lane is a contract between two SEPARATELY
        # deployed processes: the hub subscribes here, the per-agent subscriber
        # publishes. A subscriber can be running an older or newer build, so the
        # hub must hear a bare-form publish AND a channel-scoped one — otherwise a
        # version skew is silent message loss (NATS core has no backlog, so the
        # reply is simply never delivered and the task hangs to timeout).
        #
        # Subscribing both cannot double-process: a publisher emits exactly ONE
        # form per message, and the two subjects differ in token count so no
        # single message matches both.
        subject = subjects.agent_status(self.env, session.task_id)
        scoped_subject = subjects.agent_status_wildcard(self.env, session.task_id)

        async def _observe(env: Envelope) -> None:
            if observe_status is None:
                return
            try:
                await observe_status(env)
            except Exception as exc:  # noqa: BLE001 - observer failure must not block coordination
                logger.error(
                    "orchestrator.status | observer failed task={} sender={} kind={} "
                    "error_type={}; coordination continues",
                    cell[0].task_id,
                    env.sender,
                    env.kind,
                    type(exc).__name__,
                )

        async def _on_status(env: Envelope) -> None:
            active = cell[0]
            # Parse first because hand-built DONE sentinels may carry their task
            # identity only in the canonical payload field.
            done = parse_done(env)
            payload_task_id = done_payload_field(env.payload, TASK_ID_KEY)
            effective_done_task_id = (
                env.task_id
                if isinstance(env.task_id, str) and env.task_id
                else payload_task_id
                if isinstance(payload_task_id, str) and payload_task_id
                else None
            )
            status_task_id = effective_done_task_id if done is not None else env.task_id
            if status_task_id != active.task_id:
                logger.warning(
                    "orchestrator.status | foreign task={} on lane task={}; dropped before "
                    "observer",
                    status_task_id,
                    active.task_id,
                )
                return

            if env.sender not in active.workers:
                logger.warning(
                    "orchestrator.status | non-worker sender={} task={}; dropped before observer",
                    subjects.slug(env.sender),
                    active.task_id,
                )
                return

            # Once identity guards pass, a completion sentinel deterministically
            # advances the session to DONE and never reaches handoff detection.
            if done is not None:
                await _observe(env)
                logger.info(
                    "orchestrator.status | done sentinel sender={} outcome={} task={}",
                    env.sender,
                    done.outcome,
                    cell[0].task_id,
                )
                action = await self.apply(cell[0], done)
                cell[0] = action.session
                await self._persist_governor(cell[0].task_id)
                return

            await _observe(env)
            text = env.payload.get("text", "")
            detected = detect_missing_handoff(
                sender=env.sender, text=str(text), peers=cell[0].workers
            )
            if detected is None:
                if self._governor is not None:
                    self._governor.reset_relay(active.task_id)
                logger.debug(
                    "orchestrator.status | handoff present sender={} task={}",
                    env.sender,
                    cell[0].task_id,
                )
                return
            logger.info(
                "orchestrator.status | missing handoff sender={} relay_to={} task={}",
                env.sender,
                detected.relay_to,
                cell[0].task_id,
            )
            action = await self.apply(
                cell[0],
                detected,
                relay_context=_RelayContext(env.id, env.sender, self._monotonic()),
            )
            cell[0] = action.session  # persist the advanced snapshot for next event
            await self._persist_governor(cell[0].task_id)

        logger.info(
            "orchestrator.status | watching subject={} scoped={} task={}",
            subject,
            scoped_subject,
            session.task_id,
        )
        # Build incrementally and unwind on failure. A tuple literal evaluates
        # left to right, so if the SECOND subscribe raises (a server hiccup, a
        # permissions denial) the first is already live on the connection while
        # its handle is discarded with the half-built tuple — the caller never
        # receives it, so nothing can ever cancel it. That leaked subscription
        # keeps delivering into _on_status for the life of the process, which on
        # a retry means the task's status is observed twice by two handlers over
        # two different session cells.
        opened: list[Subscription] = []
        try:
            for subj in (subject, scoped_subject):
                opened.append(await self._bus.subscribe(subj, _on_status))
        except BaseException:
            for sub in opened:
                try:
                    await sub.unsubscribe()
                except Exception as exc:  # noqa: BLE001 - cleanup must not mask the original
                    logger.warning(
                        "orchestrator.status | could not unwind a partial status "
                        "subscription task={} error={}",
                        session.task_id,
                        exc,
                    )
            raise
        return tuple(opened), lambda: cell[0]

    async def _persist_governor(self, task_id: str) -> None:
        """Durably persist the governor counters for ``task_id`` (fleet-bus #106).

        CAUSAL ORDERING (codex terminal review): the durable snapshot MUST observe
        the count this same handler just incremented via :meth:`apply` →
        ``note_message``. Persisting from a SEPARATE status subscription raced the
        increment — the snapshot could capture msgs=0 before the counting callback
        ran, and a restart would hydrate a count LOWER than what was processed. By
        persisting HERE, right after ``apply`` in the counting handler, the write is
        strictly after the increment: a restart can never hydrate a count below
        what was actually processed. No-op (returns immediately) when no governor is
        wired or durable mode is off = today's byte-for-byte behavior.
        """
        if self._governor is not None and self._governor.durable:
            await self._governor.persist_state(task_id)

    async def persist_transition(self, session: CoordinationSession, kboard: KboardClient) -> None:
        """Persist a coordination transition to k-board and emit a kboard_event.

        k-board is the durable source of truth (design invariant); NATS delivers.
        On each transition the orchestrator: (1) CAS-updates the task's k-board
        status for the new state, then (2) emits the matching ``kboard_event`` on
        the bus so live consumers observe the lifecycle change.

        Ordering is deliberate: the durable write happens *first*, then the bus
        emit. If the bus emit fails (outage), the CAS write already committed, so
        the record is authoritative and a consumer that missed the ephemeral
        signal can re-derive truth from k-board — the emit is best-effort. The
        reverse order would let a consumer act on a lifecycle change that never
        persisted. The emit failure is *not* swallowed here: a bus outage raises
        after the successful write, and the caller must tolerate the
        "write-committed, emit-raised" outcome (the record is still correct).

        Wiring: this method is the k-board bridge (issue #7). It is invoked by the
        manager run-loop that Phase 9 builds (which owns a live ``KboardClient``);
        it is deliberately *not* on the ``apply()``/``_perform()`` hot path yet, so
        the pure state machine stays free of durable I/O until the loop wires it.

        Args:
            session: The advanced session (the state to persist).
            kboard: The CAS-safe k-board client.
        """
        status = status_for_state(session.state)
        event = event_for_state(session.state)
        # Durable write first (source of truth), then the ephemeral bus signal.
        # Each side logs *after* it succeeds so a log line never claims an action
        # (persist / emit) that a subsequent exception actually prevented.
        updated = await kboard.update_status(session.task_id, status)
        if updated.status != status:
            logger.warning(
                "orchestrator.kboard | task={} requested state={} status={} but durable "
                "status={}; lifecycle event suppressed",
                session.task_id,
                session.state.value,
                status,
                updated.status,
            )
            return
        logger.info(
            "orchestrator.kboard | persisted task={} state={} status={}",
            session.task_id,
            session.state.value,
            status,
        )
        subject = subjects.kboard_event(self.env, event, session.task_id)
        envelope = Envelope.new(
            msg_id=self._clock.new_id(),
            ts=self._clock.now(),
            sender=self.sender,
            kind="kboard_event",
            task_id=session.task_id,
            channel=session.channel,
            payload={"event": event, "status": status},
        )
        await self._bus.publish(subject, envelope)
        logger.info(
            "orchestrator.kboard | emitted task={} event={} subject={}",
            session.task_id,
            event,
            subject,
        )

    @traced
    async def dispatch_task(
        self,
        session: CoordinationSession,
        event: Event,
        *,
        worker: str | None = None,
        prerendered_dispatch: list[tuple[str, Envelope]] | None = None,
    ) -> Action:
        """Convenience: apply a dispatch event, optionally to a single worker.

        Args:
            session: The (typically DISPATCHED) session.
            event: A ``DISPATCH`` event.
            worker: Optional single-worker hint; ``None`` fans out to all workers.
            prerendered_dispatch: The exact envelopes from
                :meth:`render_and_check_dispatch`, published verbatim (round 4).

        Returns:
            The resulting :class:`Action` (session advanced to WORKING).
        """
        # Thread the single-worker hint through the event's actor field so the
        # pure machine records it and _dispatch honours it.
        if worker is not None:
            event = Event(
                kind=event.kind,
                actor=worker,
                relay_to=event.relay_to,
                topic=event.topic,
                outcome=event.outcome,
                context=event.context,
            )
        return await self.apply(session, event, prerendered_dispatch=prerendered_dispatch)
