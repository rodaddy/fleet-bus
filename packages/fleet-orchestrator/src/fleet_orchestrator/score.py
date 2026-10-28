"""Cross-agent scorekeeper + decider — the anti-circlejerk half (#17, Phase 6).

The orchestrator sees ALL turns on the shared bus, so it — and only it — can
count the back-and-forth between Wren and Finch deterministically. A single
agent gate (the agent runtime's turn-signal hook) sees only its own turns; the cross-agent count is
real *here*. This module keeps that score and, on non-convergence, decides.

Three parts, all pure case-statement logic — **no LLM, no heuristic, no clock,
no randomness anywhere**:

1. **Turn-signal contract** (the agent runtime's turn-signal hook seam). Agents ride two
   structured fields on the EXISTING ``agent_status`` envelope's ``payload.meta``:
   ``stance`` (``"agree"`` / ``"dissent"`` / ``"neutral"``) and ``fact_pull``
   (bool — did the agent pull a fact / break the circle). :func:`parse_turn_signal`
   parses them STRICTLY: present-and-valid → a frozen :class:`TurnSignal`;
   present-but-wrong-type/value → ``ValueError`` (fail loud, a typo is not a
   policy); simply absent → ``None`` (an UNSIGNED turn). An unsigned turn is
   counted and logged, **never** guessed from prose — the stance is not inferred
   from what the worker said, only from what it signed.

2. **Scorekeeper.** :class:`Score` tallies per topic (keyed by ``task_id`` so
   interleaved topics never cross-contaminate): the consecutive-agreement run,
   dissent/fact-pull counts, per-agent turn counts, and the unsigned count.
   :meth:`Scorekeeper.observe` runs the case-statement update and trips a
   ``circlejerk_detected`` :class:`ScoreEvent` EXACTLY ONCE when the run reaches
   ``FLEET_CIRCLEJERK_N`` consecutive agree-stance turns with zero dissent and
   zero fact-pull in that run. A dissent or a fact-pull resets the run and
   re-arms the detector.

3. **Decider.** :func:`decide` picks the evidenced position deterministically
   (dissent-backed AND fact-pulled wins), or returns a ``SYNTHESIZE`` decision
   when the score cannot decide. The synthesize path is NOT gate logic: the
   caller dispatches a NORMAL agent turn to the manager's own inbox
   (:func:`assign_synthesis`) — Heron arbitrates by taking a turn, never by
   rewriting worker output.
"""

from __future__ import annotations

import os
from collections import OrderedDict
from dataclasses import dataclass, field, replace
from enum import StrEnum
from typing import Any, Protocol

from fleet_nats import Envelope, StancePosition, subjects
from fleet_nats import Stance as CoreStance
from loguru import logger

from fleet_orchestrator import trace
from fleet_orchestrator.governor import Narrator
from fleet_orchestrator.state import CoordinationSession

_ENV_CIRCLEJERK_N = "FLEET_CIRCLEJERK_N"
# Conservative default — tune from observed traffic via env, like the governor's
# budget caps. Four consecutive unbroken agreements is the shortest run that
# reads as a rubber-stamp rather than genuine early alignment.
_DEFAULT_CIRCLEJERK_N = 4
# Status envelopes remembered for at-least-once delivery deduplication. This is
# deliberately the same bounded LRU-wheel size as CostLedger: large enough for
# reconnect/retry bursts while keeping memory flat under sustained traffic.
_DEFAULT_DEDUP_WINDOW = 4096


def _int_env(var: str, default: int) -> int:
    """Read a positive-int env var; fail loud on junk (a typo is not a policy).

    Mirrors ``governor._int_env`` exactly: unset → default, non-int → ValueError,
    non-positive → ValueError. A circlejerk threshold of 0 or -1 is meaningless,
    so it is rejected at the boundary rather than silently clamped.
    """
    raw = os.environ.get(var)
    if raw is None:
        return default
    try:
        value = int(raw)
    except ValueError as exc:
        raise ValueError(f"{var} must be an integer, got {raw!r}") from exc
    if value <= 0:
        raise ValueError(f"{var} must be positive, got {value}")
    return value


def circlejerk_n_from_env() -> int:
    """The consecutive-agreement threshold from ``FLEET_CIRCLEJERK_N`` (default 4)."""
    return _int_env(_ENV_CIRCLEJERK_N, _DEFAULT_CIRCLEJERK_N)


# ---- Turn-signal contract (the agent runtime's turn-signal hook seam) ---------------------


Stance = StancePosition
"""Shared stance vocabulary re-exported for orchestrator callers."""


@dataclass(frozen=True)
class TurnSignal:
    """A parsed, signed turn — the structured turn-signal reflex on an ``agent_status``.

    Attributes:
        agent_id: The signing agent (the envelope ``sender``).
        task_id: The topic this turn belongs to (the envelope ``task_id``).
        stance: The declared shared :class:`fleet_nats.StancePosition`.
        fact_pull: Whether the agent pulled a fact this turn (broke the circle).
    """

    agent_id: str
    task_id: str
    stance: Stance
    fact_pull: bool


def _require_stance(value: Any) -> Stance:
    """Coerce a wire ``stance`` value to :class:`Stance` or raise ``ValueError``.

    A present ``stance`` field that is not one of the three declared words is a
    malformed signal — fail loud rather than silently treat it as neutral or
    unsigned (both would corrupt the count).
    """
    if not isinstance(value, str):
        raise ValueError(f"turn signal 'stance' must be a string, got {value!r}")
    try:
        return StancePosition(value)
    except ValueError as exc:
        allowed = ", ".join(s.value for s in StancePosition)
        raise ValueError(f"turn signal 'stance' must be one of ({allowed}), got {value!r}") from exc


def _require_fact_pull(value: Any) -> bool:
    """Coerce a wire ``fact_pull`` value to ``bool`` or raise ``ValueError``.

    Strictly a JSON bool — ``0``/``1``/``"true"`` are rejected. An ambiguous
    truthiness coercion here would let a malformed signal quietly re-arm or trip
    the detector, so the type is enforced at the boundary.
    """
    if not isinstance(value, bool):
        raise ValueError(f"turn signal 'fact_pull' must be a bool, got {value!r}")
    return value


def parse_turn_signal(envelope: Envelope) -> TurnSignal | None:
    """Parse the turn signal off an ``agent_status`` envelope.

    The signal rides ``payload.meta`` as ``{"stance": ..., "fact_pull": ...}``.

    Args:
        envelope: The status envelope carrying the worker's turn.

    Returns:
        A frozen :class:`TurnSignal` when BOTH meta fields are present and valid;
        ``None`` when the meta fields are simply absent (an UNSIGNED turn — it is
        counted and logged, never guessed from the turn's prose).

    Raises:
        ValueError: When ``meta`` is present but malformed — a non-mapping
            ``meta``, an invalid ``stance`` word, a non-bool ``fact_pull``, or
            exactly one of the two fields present (a half-signed turn is a
            contract violation, not an unsigned turn). Fail loud.
    """
    typed_signal = envelope.typed_signal
    if isinstance(typed_signal, CoreStance):
        if not envelope.task_id:
            raise ValueError("turn signal requires a task_id on the envelope")
        return TurnSignal(
            agent_id=envelope.sender,
            task_id=envelope.task_id,
            stance=typed_signal.position,
            fact_pull=typed_signal.fact_pull,
        )

    meta = envelope.payload.get("meta")
    if meta is None:
        return None  # unsigned turn — counted, not guessed
    if not isinstance(meta, dict):
        raise ValueError(f"turn signal 'meta' must be a mapping, got {meta!r}")

    has_stance = "stance" in meta
    has_fact_pull = "fact_pull" in meta
    if not has_stance and not has_fact_pull:
        return None  # empty meta present but no signal fields → still unsigned
    if has_stance != has_fact_pull:
        missing = "fact_pull" if has_stance else "stance"
        raise ValueError(f"turn signal is half-signed: missing {missing!r} in meta")

    stance = _require_stance(meta["stance"])
    fact_pull = _require_fact_pull(meta["fact_pull"])
    if not envelope.task_id:
        raise ValueError("turn signal requires a task_id on the envelope")
    return TurnSignal(
        agent_id=envelope.sender,
        task_id=envelope.task_id,
        stance=stance,
        fact_pull=fact_pull,
    )


# ---- Scorekeeper -----------------------------------------------------------


@dataclass(frozen=True)
class Score:
    """The per-topic tally, keyed by ``task_id``.

    Frozen: :meth:`Scorekeeper.observe` returns a NEW score per turn (a value
    sequence), so a topic's history can be asserted on immutable snapshots — the
    same discipline :class:`CoordinationSession` uses.

    Attributes:
        task_id: The topic this score belongs to.
        agree_run: Consecutive agree-stance turns since the last reset (dissent
            or fact-pull resets it to 0).
        dissents: Total dissent-stance turns on the topic.
        fact_pulls: Total fact-pull turns on the topic.
        unsigned: Total unsigned turns on the topic (counted, never guessed).
        turns_by_agent: Per-agent turn counts (every turn, signed or not).
        dissents_by_agent: Per-agent dissent counts — who supplied the pushback.
        fact_pulls_by_agent: Per-agent fact-pull counts — who supplied the
            evidence. The per-agent split is what makes the decider able to
            name the evidenced agent even when peers took neutral turns.
        tripped: Whether the current run has already emitted a circlejerk trip
            (re-armed to ``False`` on the next dissent / fact-pull reset). Guards
            the once-per-run guarantee.
    """

    task_id: str
    agree_run: int = 0
    dissents: int = 0
    fact_pulls: int = 0
    unsigned: int = 0
    turns_by_agent: dict[str, int] = field(default_factory=dict)
    dissents_by_agent: dict[str, int] = field(default_factory=dict)
    fact_pulls_by_agent: dict[str, int] = field(default_factory=dict)
    tripped: bool = False

    def _with_turn(self, agent_id: str, **changes: Any) -> Score:
        """Return a new score with ``agent_id``'s turn counted and ``changes`` applied.

        The per-agent map is copied (frozen-dataclass fields block rebinding, not
        mutation of a shared dict), so no snapshot ever aliases another's tally.
        """
        counts = dict(self.turns_by_agent)
        counts[agent_id] = counts.get(agent_id, 0) + 1
        return replace(self, turns_by_agent=counts, **changes)


class ScoreEventKind(StrEnum):
    """What a :class:`ScoreEvent` reports.

    Attributes:
        CIRCLEJERK_DETECTED: N consecutive unbroken agreements tripped the
            detector (emitted exactly once per run).
    """

    CIRCLEJERK_DETECTED = "circlejerk_detected"


@dataclass(frozen=True)
class ScoreEvent:
    """A deterministic scorekeeper outcome — reported, not advisory.

    Attributes:
        kind: Which :class:`ScoreEventKind` occurred.
        task_id: The topic it concerns.
        run_length: The consecutive-agreement run length at the trip.
        agents: The agents that took part in the tripping run (sorted).
    """

    kind: ScoreEventKind
    task_id: str
    run_length: int
    agents: tuple[str, ...]

    @property
    def reason(self) -> str:
        """Board/log-readable one-liner (coordination metadata only)."""
        return (
            f"circlejerk: {self.run_length} consecutive agreements, "
            f"no dissent or fact-pull ({', '.join(self.agents)})"
        )


class Scorekeeper:
    """Deterministic cross-agent scorekeeping over the shared turn stream.

    Keeps a :class:`Score` per topic (``task_id``). :meth:`observe` parses the
    turn signal off each ``agent_status`` envelope and applies a pure
    case-statement update; a malformed signal raises out of :func:`parse_turn_signal`
    and is logged + skipped at THIS boundary (the observe call) so one bad
    envelope never corrupts the tally or crashes the loop.

    Args:
        circlejerk_n: Consecutive-agreement threshold to trip (from
            :func:`circlejerk_n_from_env` by default).
        dedup_window: Maximum envelope identities remembered for at-least-once
            delivery deduplication; the least-recently seen entry evicts first.
    """

    def __init__(
        self,
        *,
        circlejerk_n: int | None = None,
        dedup_window: int = _DEFAULT_DEDUP_WINDOW,
    ) -> None:
        self._n = circlejerk_n if circlejerk_n is not None else circlejerk_n_from_env()
        if self._n <= 0:
            raise ValueError(f"circlejerk_n must be positive, got {self._n}")
        if dedup_window <= 0:
            raise ValueError(f"Scorekeeper dedup_window must be positive, got {dedup_window}")
        self._window = dedup_window
        self._scores: dict[str, Score] = {}
        self._seen: OrderedDict[tuple[str, str, str], None] = OrderedDict()

    def score_for(self, task_id: str) -> Score | None:
        """The current score snapshot for ``task_id``, or ``None`` if unseen."""
        return self._scores.get(task_id)

    def note_done(self, task_id: str) -> None:
        """End scoring for ``task_id`` and clear the topic.

        A ``done`` mid-exchange ends the debate: the tally is dropped so a later,
        unrelated task reusing the id starts clean, and no stale run can trip.
        """
        score_removed = self._scores.pop(task_id, None) is not None
        seen_keys = [key for key in self._seen if key[1] == task_id]
        for key in seen_keys:
            del self._seen[key]
        if score_removed or seen_keys:
            logger.info("orchestrator.score | done, scoring ended task={}", task_id)

    def observe(self, envelope: Envelope) -> ScoreEvent | None:
        """Observe one turn; update the topic tally; maybe trip a circlejerk.

        Args:
            envelope: An ``agent_status`` envelope carrying a (possibly unsigned)
                turn.

        Returns:
            A ``circlejerk_detected`` :class:`ScoreEvent` when THIS turn tripped
            the detector (first trip of the run only), else ``None``.
        """
        try:
            signal = parse_turn_signal(envelope)
        except ValueError as exc:
            # Malformed signal — logged and skipped at the boundary. One bad
            # envelope must not corrupt the tally or take the loop down.
            logger.warning(
                "orchestrator.score | malformed turn signal sender={} task={}: {}",
                envelope.sender,
                envelope.task_id,
                exc,
            )
            return None

        task_id = signal.task_id if signal is not None else envelope.task_id
        if not task_id:
            logger.warning(
                "orchestrator.score | unsigned turn without task_id sender={}; dropped",
                envelope.sender,
            )
            return None

        key = (subjects.slug(envelope.sender), task_id, envelope.id)
        if key in self._seen:
            self._seen.move_to_end(key)
            logger.info(
                "orchestrator.score | duplicate turn sender={} task={} id={} — no-op",
                envelope.sender,
                task_id,
                envelope.id,
            )
            return None
        self._seen[key] = None
        if len(self._seen) > self._window:
            self._seen.popitem(last=False)

        if signal is None:
            return self._observe_unsigned(envelope)
        return self._observe_signal(signal)

    def _observe_unsigned(self, envelope: Envelope) -> ScoreEvent | None:
        """Count an unsigned turn (no stance, no guess) — it never trips a run.

        An unsigned turn is not an agreement: it neither extends nor resets the
        agree run, it is simply recorded. (An absent task_id can't be keyed, so
        it is logged and dropped — there is no topic to attribute it to.)
        """
        task_id = envelope.task_id
        if not task_id:
            logger.warning(
                "orchestrator.score | unsigned turn without task_id sender={}; dropped",
                envelope.sender,
            )
            return None
        score = self._scores.get(task_id, Score(task_id=task_id))
        self._scores[task_id] = score._with_turn(envelope.sender, unsigned=score.unsigned + 1)
        logger.info(
            "orchestrator.score | unsigned turn sender={} task={} (counted, not guessed)",
            envelope.sender,
            task_id,
        )
        return None

    def _observe_signal(self, signal: TurnSignal) -> ScoreEvent | None:
        """Apply a signed turn: the pure case-statement at the heart of scoring.

        A fact-pull resets the run (and re-arms) regardless of stance — pulling a
        fact IS the break-the-circle act. A DISSENT stance also resets + re-arms.
        A single turn that BOTH dissents and fact-pulls counts as both (it is the
        strongest evidenced turn — the decider's "dissented AND fact-pulled"
        candidate can be earned in one turn). AGREE extends the run (and may
        trip); NEUTRAL leaves the run untouched (neither agreement nor dissent).
        """
        task_id = signal.task_id
        score = self._scores.get(task_id, Score(task_id=task_id))

        if signal.fact_pull or signal.stance is Stance.DISSENT:
            # The break-the-circle turn: reset the run and re-arm; tally every
            # kind of evidence THIS turn carried, per-topic AND per-agent (the
            # per-agent split is what the decider names a winner from).
            changes: dict[str, Any] = {"agree_run": 0, "tripped": False}
            broke = []
            if signal.fact_pull:
                pulls = dict(score.fact_pulls_by_agent)
                pulls[signal.agent_id] = pulls.get(signal.agent_id, 0) + 1
                changes["fact_pulls"] = score.fact_pulls + 1
                changes["fact_pulls_by_agent"] = pulls
                broke.append("fact-pull")
            if signal.stance is Stance.DISSENT:
                dissents = dict(score.dissents_by_agent)
                dissents[signal.agent_id] = dissents.get(signal.agent_id, 0) + 1
                changes["dissents"] = score.dissents + 1
                changes["dissents_by_agent"] = dissents
                broke.append("dissent")
            self._scores[task_id] = score._with_turn(signal.agent_id, **changes)
            logger.info(
                "orchestrator.score | {} resets run sender={} task={}",
                "+".join(broke),
                signal.agent_id,
                task_id,
            )
            return None

        match signal.stance:
            case Stance.NEUTRAL:
                self._scores[task_id] = score._with_turn(signal.agent_id)
                logger.debug(
                    "orchestrator.score | neutral turn sender={} task={}",
                    signal.agent_id,
                    task_id,
                )
                return None
            case Stance.AGREE:
                run = score.agree_run + 1
                updated = score._with_turn(signal.agent_id, agree_run=run)
                if run >= self._n and not updated.tripped:
                    tripped = replace(updated, tripped=True)
                    self._scores[task_id] = tripped
                    event = ScoreEvent(
                        kind=ScoreEventKind.CIRCLEJERK_DETECTED,
                        task_id=task_id,
                        run_length=run,
                        agents=tuple(sorted(tripped.turns_by_agent)),
                    )
                    logger.warning("orchestrator.score | {} task={}", event.reason, task_id)
                    return event
                self._scores[task_id] = updated
                logger.info(
                    "orchestrator.score | agree run={} sender={} task={}",
                    run,
                    signal.agent_id,
                    task_id,
                )
                return None


# ---- Decider ---------------------------------------------------------------


class DecisionKind(StrEnum):
    """What the decider concluded.

    Attributes:
        DECIDE: One agent's evidenced position wins — ``winner`` names it.
        SYNTHESIZE: The score cannot decide (all unsigned, or a true tie);
            Heron takes a synthesis TURN (never gate logic — the caller
            dispatches a normal agent turn to the manager's own inbox).
    """

    DECIDE = "decide"
    SYNTHESIZE = "synthesize"


@dataclass(frozen=True)
class Decision:
    """The decider's deterministic verdict on a topic.

    Attributes:
        kind: :class:`DecisionKind` — decide vs synthesize.
        task_id: The topic decided.
        winner: The winning agent id, set iff ``kind`` is ``DECIDE``.
        reason: Log/board-readable rationale (coordination metadata only).
    """

    kind: DecisionKind
    task_id: str
    winner: str | None = None
    reason: str = ""


@dataclass(frozen=True)
class _Candidate:
    """One agent's evidence bundle, for the decider's tiebreak chain."""

    agent_id: str
    fact_pulls: int
    dissents: int


def decide(score: Score) -> Decision:
    """Decide a topic deterministically from the kept score.

    An evidenced position is one an agent *both* dissented on AND backed with a
    fact-pull — pushback without evidence is noise, evidence without pushback is
    agreement. Only agents who did both are candidates.

    Tiebreak (THE documented order), applied when more than one candidate
    qualifies: **most fact-pulls, then most dissents, then lexicographically
    smallest agent id**. The lexicographic final tier makes the outcome fully
    deterministic — never clock- or insertion-order-dependent.

    Evidence is tallied PER AGENT (``Score.dissents_by_agent`` /
    ``fact_pulls_by_agent``), so a peer taking neutral/agree/unsigned turns
    never blocks a decidable topic — only who supplied the evidence matters.
    When the evidence is genuinely split (one agent dissented, a different one
    fact-pulled, nobody did both), absent, or the topic is all-unsigned, no
    single position is backed → ``SYNTHESIZE``.

    Args:
        score: The topic's kept score.

    Returns:
        A ``DECIDE`` :class:`Decision` naming the evidenced winner, or a
        ``SYNTHESIZE`` decision when the score cannot decide.
    """
    # No evidence at all (all unsigned / all neutral / all agree) → synthesize.
    if score.dissents == 0 or score.fact_pulls == 0:
        return Decision(
            kind=DecisionKind.SYNTHESIZE,
            task_id=score.task_id,
            reason="no evidenced position (missing dissent or fact-pull) — synthesize",
        )

    candidates = _evidenced_candidates(score)
    if not candidates:
        return Decision(
            kind=DecisionKind.SYNTHESIZE,
            task_id=score.task_id,
            reason="evidence split across agents; no single backed position — synthesize",
        )

    winner = _tiebreak(candidates)
    return Decision(
        kind=DecisionKind.DECIDE,
        task_id=score.task_id,
        winner=winner.agent_id,
        reason=(
            f"evidenced position wins: {winner.agent_id} "
            f"(fact_pulls={winner.fact_pulls}, dissents={winner.dissents})"
        ),
    )


def _evidenced_candidates(score: Score) -> tuple[_Candidate, ...]:
    """The agents who both dissented AND fact-pulled on the topic.

    Per-agent tallies make this exact: an agent qualifies iff ITS OWN counts
    carry at least one dissent and at least one fact-pull — a peer's neutral or
    agree turns are irrelevant. Two agents can both qualify; the tiebreak chain
    then orders them. Nobody qualifying (evidence split or absent) returns
    empty (→ synthesize).
    """
    evidenced = sorted(set(score.dissents_by_agent) & set(score.fact_pulls_by_agent))
    return tuple(
        _Candidate(
            agent_id=agent_id,
            fact_pulls=score.fact_pulls_by_agent[agent_id],
            dissents=score.dissents_by_agent[agent_id],
        )
        for agent_id in evidenced
    )


def _tiebreak(candidates: tuple[_Candidate, ...]) -> _Candidate:
    """Apply THE documented tiebreak: fact_pulls, then dissents, then agent id.

    Sorts descending on fact_pulls, then dissents, and ascending (lexicographic)
    on agent id as the final deterministic tier.
    """
    return sorted(candidates, key=lambda c: (-c.fact_pulls, -c.dissents, c.agent_id))[0]


# ---- Synthesis carve-out (an agent turn, never gate logic) -----------------

SYNTHESIS_KIND = "dispatch"
# TEMPLATED instruction — ids/state-words only, never worker payload prose. The
# manager receives a NORMAL turn on her own inbox; she synthesizes and assigns.
_SYNTHESIS_INSTRUCTION = (
    "[{task_id}] synthesize: the workers did not converge — take both positions, "
    "decide the right thing, and assign it."
)


class _PublishBus(Protocol):
    """The one bus capability :func:`assign_synthesis` needs."""

    async def publish(self, subject: str, envelope: Envelope) -> None:
        """Publish an envelope."""
        ...


async def assign_synthesis(
    bus: _PublishBus,
    session: CoordinationSession,
    *,
    env: str,
    new_id: str,
    now: str,
) -> None:
    """Dispatch a synthesis TURN to the manager's own inbox (not gate logic).

    On a ``SYNTHESIZE`` decision Heron does not rewrite worker output; she takes
    a turn. This publishes a dispatch envelope carrying a TEMPLATED instruction
    (ids/state-words only, no worker prose) to ``agent_inbox(env, manager)``, so
    the manager runs the synthesis as a normal agent turn through her own gateway.

    Args:
        bus: The connected bus (publish capability only).
        session: The topic's session (task_id + manager + channel routing).
        env: The subject env prefix (from the manager's bus config).
        new_id: A fresh message id (injected — the envelope contract owns its id).
        now: An ISO-8601 timestamp (injected — this module never reads a clock).
    """
    subject = subjects.agent_inbox(env, session.manager)
    text = _SYNTHESIS_INSTRUCTION.format(task_id=session.task_id)
    envelope = Envelope.new(
        msg_id=new_id,
        ts=now,
        sender=session.manager,
        kind=SYNTHESIS_KIND,
        task_id=session.task_id,
        channel=session.channel,
        to=session.manager,
        payload={"text": text},
    )
    await bus.publish(subject, envelope)
    logger.info(
        "orchestrator.synthesis | assigned to manager={} task={} lane={}",
        session.manager,
        session.task_id,
        subject,
    )


# ---- Narration + trace for score-based decisions ---------------------------


async def narrate_score_event(
    narrator: Narrator, session: CoordinationSession, event: ScoreEvent, *, now: str
) -> None:
    """Narrate a circlejerk trip via the Phase-5 single narrator + trace it.

    Args:
        narrator: The manager's single :class:`Narrator`.
        session: The topic's session.
        event: The tripped :class:`ScoreEvent`.
        now: Injected ISO-8601 timestamp for the trace record (no clock here).
    """
    await narrator.narrate(session, "circlejerk")
    trace.emit(
        trace.TraceRecord(
            ts=now,
            task_id=event.task_id,
            action=event.kind.value,
        )
    )


async def narrate_decision(
    narrator: Narrator, session: CoordinationSession, decision: Decision, *, now: str
) -> None:
    """Narrate a score-based decision via the Phase-5 single narrator + trace it.

    A ``SYNTHESIZE`` decision is NOT narrated as a verdict here — the synthesis
    turn is a normal agent dispatch (:func:`assign_synthesis`) that narrates
    itself through the manager's own path. Only a ``DECIDE`` verdict emits a
    ``decided`` beat.

    Args:
        narrator: The manager's single :class:`Narrator`.
        session: The topic's session.
        decision: The decider's verdict.
        now: Injected ISO-8601 timestamp for the trace record (no clock here).
    """
    if decision.kind is DecisionKind.DECIDE:
        await narrator.narrate(session, "decided", target=decision.winner or "")
    trace.emit(
        trace.TraceRecord(
            ts=now,
            task_id=decision.task_id,
            action=decision.kind.value,
            target=decision.winner,
        )
    )
