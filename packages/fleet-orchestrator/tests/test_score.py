"""Scorekeeper + decider tests (#17): cross-agent anti-circlejerk.

Scripted Wren+Finch exchanges on a fake stream drive the deterministic
scorekeeper: the circlejerk trips at EXACTLY ``N`` (never at ``N-1``), a dissent
or fact-pull resets the run and re-arms the detector, the trip fires once per
run, unsigned turns are counted (never guessed from prose), interleaved topics
stay isolated, and ``done`` ends scoring. The decider picks the evidenced
position, applies THE documented tiebreak chain, and synthesizes when it cannot
decide. A malformed signal is logged + skipped at the observe boundary.
"""

from __future__ import annotations

import itertools

from fleet_nats import Envelope, StancePosition, subjects
from fleet_nats import Stance as CoreStance

from fleet_orchestrator.governor import Narrator
from fleet_orchestrator.score import (
    Decision,
    DecisionKind,
    Score,
    ScoreEvent,
    ScoreEventKind,
    Scorekeeper,
    Stance,
    TurnSignal,
    assign_synthesis,
    decide,
    narrate_decision,
    narrate_score_event,
    parse_turn_signal,
)
from fleet_orchestrator.state import CoordinationSession, State

# ---- Fixtures / helpers ----------------------------------------------------


class _RecordingBus:
    def __init__(self) -> None:
        self.published: list[tuple[str, Envelope]] = []

    async def publish(self, subject: str, envelope: Envelope) -> None:
        self.published.append((subject, envelope))


_ids = itertools.count()


def _turn(
    sender: str,
    task_id: str,
    *,
    stance: str | None = None,
    fact_pull: bool | None = None,
    meta: dict[str, object] | None = None,
    unsigned: bool = False,
    msg_id: str | None = None,
    typed_signal: object | None = None,
) -> Envelope:
    """Build an ``agent_status`` envelope, optionally carrying a turn signal.

    ``unsigned=True`` omits ``meta`` entirely (an unsigned turn). Otherwise the
    stance/fact_pull ride ``payload.meta``; ``meta=`` overrides for malformed
    cases.
    """
    payload: dict[str, object] = {"text": "worker turn prose (never parsed for stance)"}
    if not unsigned:
        if meta is not None:
            payload["meta"] = meta
        elif stance is not None or fact_pull is not None:
            payload["meta"] = {"stance": stance, "fact_pull": fact_pull}
    return Envelope.new(
        msg_id=msg_id if msg_id is not None else f"m-{next(_ids)}",
        ts="2026-07-08T00:00:00Z",
        sender=sender,
        kind="status",
        task_id=task_id,
        channel="dash-dev",
        payload=payload,
        typed_signal=typed_signal,
    )


def _session(task_id: str = "t-1") -> CoordinationSession:
    return CoordinationSession(
        task_id=task_id,
        channel="dash-dev",
        manager="heron",
        workers=("wren", "finch"),
        state=State.WORKING,
    )


# ---- Turn-signal contract --------------------------------------------------


def test_score_parse_turn_signal_valid() -> None:
    env = _turn("wren", "t-1", stance="dissent", fact_pull=True)
    sig = parse_turn_signal(env)
    assert sig == TurnSignal(agent_id="wren", task_id="t-1", stance=Stance.DISSENT, fact_pull=True)


def test_score_stance_uses_shared_fleet_core_vocabulary() -> None:
    assert Stance is StancePosition
    assert {stance.value for stance in Stance} == {"agree", "dissent", "neutral"}


def test_score_parse_turn_signal_prefers_typed_stance_signal() -> None:
    env = _turn(
        "wren",
        "t-1",
        meta={"stance": "bogus", "fact_pull": "bad"},
        typed_signal=CoreStance(stance=StancePosition.DISSENT, fact_pull=True, reason="logs"),
    )
    sig = parse_turn_signal(env)
    assert sig == TurnSignal(agent_id="wren", task_id="t-1", stance=Stance.DISSENT, fact_pull=True)


def test_score_typed_stance_signal_requires_task_id() -> None:
    import pytest

    env = Envelope.new(
        msg_id="m-typed-no-task",
        ts="2026-07-08T00:00:00Z",
        sender="wren",
        kind="status",
        payload={"text": "typed stance without topic"},
        typed_signal=CoreStance(stance=StancePosition.AGREE),
    )
    with pytest.raises(ValueError, match="task_id"):
        parse_turn_signal(env)


def test_score_unsigned_turn_parses_to_none() -> None:
    assert parse_turn_signal(_turn("wren", "t-1", unsigned=True)) is None


def test_score_empty_meta_is_unsigned_not_error() -> None:
    assert parse_turn_signal(_turn("wren", "t-1", meta={})) is None


def test_score_malformed_signal_raises_valueerror() -> None:
    import pytest

    # bad stance word
    with pytest.raises(ValueError, match="stance"):
        parse_turn_signal(_turn("wren", "t-1", meta={"stance": "yes", "fact_pull": True}))
    # non-bool fact_pull
    with pytest.raises(ValueError, match="fact_pull"):
        parse_turn_signal(_turn("wren", "t-1", meta={"stance": "agree", "fact_pull": "yes"}))
    # half-signed
    with pytest.raises(ValueError, match="half-signed"):
        parse_turn_signal(_turn("wren", "t-1", meta={"stance": "agree"}))
    # non-mapping meta
    with pytest.raises(ValueError, match="mapping"):
        parse_turn_signal(_turn("wren", "t-1", meta=["nope"]))  # type: ignore[arg-type]


# ---- Scorekeeper: circlejerk trip ------------------------------------------


def test_circlejerk_trips_at_exactly_n_not_before() -> None:
    keeper = Scorekeeper(circlejerk_n=4)
    # Wren + Finch rubber-stamping each other.
    speakers = itertools.cycle(("wren", "finch"))
    events: list[ScoreEvent | None] = []
    for _ in range(3):  # N-1 = 3 agreements: no trip
        events.append(keeper.observe(_turn(next(speakers), "t-1", stance="agree", fact_pull=False)))
    assert all(e is None for e in events), "N-1 agreements must NOT trip"
    trip = keeper.observe(_turn(next(speakers), "t-1", stance="agree", fact_pull=False))
    assert isinstance(trip, ScoreEvent)
    assert trip.kind is ScoreEventKind.CIRCLEJERK_DETECTED
    assert trip.run_length == 4
    assert trip.agents == ("finch", "wren")


def test_circlejerk_fires_once_per_run_then_stays_quiet() -> None:
    keeper = Scorekeeper(circlejerk_n=2)
    assert keeper.observe(_turn("wren", "t-1", stance="agree", fact_pull=False)) is None
    assert keeper.observe(_turn("finch", "t-1", stance="agree", fact_pull=False)) is not None
    # Still agreeing past the trip: no second event for the same run.
    assert keeper.observe(_turn("wren", "t-1", stance="agree", fact_pull=False)) is None
    assert keeper.observe(_turn("finch", "t-1", stance="agree", fact_pull=False)) is None


def test_circlejerk_dissent_resets_and_rearms() -> None:
    keeper = Scorekeeper(circlejerk_n=3)
    keeper.observe(_turn("wren", "t-1", stance="agree", fact_pull=False))
    keeper.observe(_turn("finch", "t-1", stance="agree", fact_pull=False))
    # Dissent resets the run.
    keeper.observe(_turn("wren", "t-1", stance="dissent", fact_pull=False))
    score = keeper.score_for("t-1")
    assert score is not None and score.agree_run == 0 and score.dissents == 1
    # Re-armed: it takes a full fresh N agreements to trip again.
    assert keeper.observe(_turn("finch", "t-1", stance="agree", fact_pull=False)) is None
    assert keeper.observe(_turn("wren", "t-1", stance="agree", fact_pull=False)) is None
    assert keeper.observe(_turn("finch", "t-1", stance="agree", fact_pull=False)) is not None


def test_circlejerk_fact_pull_resets_and_rearms() -> None:
    keeper = Scorekeeper(circlejerk_n=3)
    keeper.observe(_turn("wren", "t-1", stance="agree", fact_pull=False))
    keeper.observe(_turn("finch", "t-1", stance="agree", fact_pull=False))
    # A fact-pull (even on an agree stance) breaks the circle: reset + re-arm.
    keeper.observe(_turn("wren", "t-1", stance="agree", fact_pull=True))
    score = keeper.score_for("t-1")
    assert score is not None and score.agree_run == 0 and score.fact_pulls == 1
    assert keeper.observe(_turn("finch", "t-1", stance="agree", fact_pull=False)) is None
    assert keeper.observe(_turn("wren", "t-1", stance="agree", fact_pull=False)) is None
    assert keeper.observe(_turn("finch", "t-1", stance="agree", fact_pull=False)) is not None


def test_score_unsigned_turn_counted_not_guessed() -> None:
    keeper = Scorekeeper(circlejerk_n=2)
    # Two agreements would trip at N=2 — but an unsigned turn between them is
    # NOT counted as agreement (no guess from prose): the run does not advance.
    keeper.observe(_turn("wren", "t-1", stance="agree", fact_pull=False))
    assert keeper.observe(_turn("finch", "t-1", unsigned=True)) is None
    score = keeper.score_for("t-1")
    assert score is not None
    assert score.unsigned == 1
    assert score.agree_run == 1, "unsigned turn must not extend the agree run"


def test_score_interleaved_tasks_do_not_cross_contaminate() -> None:
    keeper = Scorekeeper(circlejerk_n=2)
    # Interleave two topics; only t-1 reaches N agreements.
    assert keeper.observe(_turn("wren", "t-1", stance="agree", fact_pull=False)) is None
    assert keeper.observe(_turn("wren", "t-2", stance="agree", fact_pull=False)) is None
    assert keeper.observe(_turn("finch", "t-2", stance="dissent", fact_pull=False)) is None
    trip = keeper.observe(_turn("finch", "t-1", stance="agree", fact_pull=False))
    assert trip is not None and trip.task_id == "t-1"
    s2 = keeper.score_for("t-2")
    assert s2 is not None and s2.agree_run == 0 and s2.dissents == 1


def test_score_done_ends_scoring_and_clears_topic() -> None:
    keeper = Scorekeeper(circlejerk_n=2)
    keeper.observe(_turn("wren", "t-1", stance="agree", fact_pull=False))
    keeper.note_done("t-1")
    assert keeper.score_for("t-1") is None
    # Reusing the id starts clean: one agreement is not enough to trip at N=2.
    assert keeper.observe(_turn("finch", "t-1", stance="agree", fact_pull=False)) is None


def test_score_done_clears_dedup_identity_for_reused_task() -> None:
    keeper = Scorekeeper(circlejerk_n=2)
    turn = _turn(
        "Foo Bar",
        "t-1",
        stance="agree",
        fact_pull=False,
        msg_id="reused-identity",
    )

    assert keeper.observe(turn) is None
    keeper.note_done("t-1")
    assert keeper.observe(turn) is None

    score = keeper.score_for("t-1")
    assert score is not None
    assert score.agree_run == 1
    assert score.turns_by_agent == {"Foo Bar": 1}


def test_score_malformed_signal_logged_and_skipped_at_observe_boundary() -> None:
    keeper = Scorekeeper(circlejerk_n=2)
    keeper.observe(_turn("wren", "t-1", stance="agree", fact_pull=False))
    # A malformed envelope must not raise out of observe, nor corrupt the tally.
    assert (
        keeper.observe(_turn("finch", "t-1", meta={"stance": "bogus", "fact_pull": True})) is None
    )
    score = keeper.score_for("t-1")
    assert score is not None
    assert score.agree_run == 1, "malformed turn must not touch the run"
    assert score.dissents == 0 and score.fact_pulls == 0 and score.unsigned == 0


def test_score_redelivered_signed_turn_is_counted_once() -> None:
    keeper = Scorekeeper(circlejerk_n=2)
    turn = _turn("wren", "t-1", stance="agree", fact_pull=False, msg_id="same")

    assert keeper.observe(turn) is None
    assert keeper.observe(turn) is None

    score = keeper.score_for("t-1")
    assert score is not None
    assert score.agree_run == 1
    assert score.turns_by_agent == {"wren": 1}


def test_score_redelivered_unsigned_turn_is_counted_once() -> None:
    keeper = Scorekeeper()
    turn = _turn("wren", "t-1", unsigned=True, msg_id="same")

    keeper.observe(turn)
    keeper.observe(turn)

    score = keeper.score_for("t-1")
    assert score is not None
    assert score.unsigned == 1
    assert score.turns_by_agent == {"wren": 1}


def test_score_distinct_envelope_ids_are_distinct_turns() -> None:
    keeper = Scorekeeper(circlejerk_n=2)

    keeper.observe(_turn("wren", "t-1", stance="agree", fact_pull=False, msg_id="a"))
    event = keeper.observe(_turn("wren", "t-1", stance="agree", fact_pull=False, msg_id="b"))

    assert event is not None
    score = keeper.score_for("t-1")
    assert score is not None and score.agree_run == 2


def test_score_same_envelope_id_isolated_by_agent_and_task() -> None:
    keeper = Scorekeeper(circlejerk_n=4)

    keeper.observe(_turn("Foo Bar", "t-1", unsigned=True, msg_id="same"))
    keeper.observe(_turn("finch", "t-1", unsigned=True, msg_id="same"))
    keeper.observe(_turn("Foo Bar", "t-2", unsigned=True, msg_id="same"))
    # Sender normalization makes this a redelivery of Foo Bar's first turn.
    keeper.observe(_turn("foo-bar", "t-1", unsigned=True, msg_id="same"))

    first = keeper.score_for("t-1")
    second = keeper.score_for("t-2")
    assert first is not None and first.unsigned == 2
    assert first.turns_by_agent == {"Foo Bar": 1, "finch": 1}
    assert second is not None and second.unsigned == 1


def test_score_dedup_window_refreshes_hot_duplicate_before_eviction() -> None:
    keeper = Scorekeeper(dedup_window=2)
    a = _turn("wren", "t-1", unsigned=True, msg_id="a")
    b = _turn("wren", "t-1", unsigned=True, msg_id="b")
    c = _turn("wren", "t-1", unsigned=True, msg_id="c")

    keeper.observe(a)
    keeper.observe(b)
    keeper.observe(a)  # refreshes a, making b the oldest
    keeper.observe(c)  # evicts b
    keeper.observe(a)  # still remembered: no-op
    keeper.observe(b)  # evicted: counted again

    score = keeper.score_for("t-1")
    assert score is not None
    assert score.unsigned == 4


def test_score_dedup_window_must_be_positive() -> None:
    import pytest

    with pytest.raises(ValueError, match="dedup_window must be positive"):
        Scorekeeper(dedup_window=0)


def test_score_malformed_envelope_does_not_poison_dedup_wheel() -> None:
    keeper = Scorekeeper()
    malformed = _turn(
        "wren",
        "t-1",
        meta={"stance": "bogus", "fact_pull": False},
        msg_id="retry",
    )
    valid = _turn("wren", "t-1", stance="agree", fact_pull=False, msg_id="retry")

    assert keeper.observe(malformed) is None
    assert keeper.observe(valid) is None

    score = keeper.score_for("t-1")
    assert score is not None and score.agree_run == 1


def test_score_replaying_same_journal_is_idempotent() -> None:
    journal = (
        _turn("wren", "t-1", stance="agree", fact_pull=False, msg_id="a"),
        _turn("finch", "t-1", unsigned=True, msg_id="b"),
        _turn("wren", "t-1", stance="dissent", fact_pull=True, msg_id="c"),
    )
    replayed = Scorekeeper()
    baseline = Scorekeeper()

    for envelope in journal:
        baseline.observe(envelope)
        replayed.observe(envelope)
    for envelope in journal:
        replayed.observe(envelope)

    assert replayed.score_for("t-1") == baseline.score_for("t-1")


# ---- Decider ---------------------------------------------------------------


def _score(
    task_id: str = "t-1",
    *,
    dissents: int = 0,
    fact_pulls: int = 0,
    turns_by_agent: dict[str, int] | None = None,
    dissents_by_agent: dict[str, int] | None = None,
    fact_pulls_by_agent: dict[str, int] | None = None,
    unsigned: int = 0,
) -> Score:
    return Score(
        task_id=task_id,
        dissents=dissents,
        fact_pulls=fact_pulls,
        unsigned=unsigned,
        turns_by_agent=turns_by_agent if turns_by_agent is not None else {},
        dissents_by_agent=dissents_by_agent if dissents_by_agent is not None else {},
        fact_pulls_by_agent=fact_pulls_by_agent if fact_pulls_by_agent is not None else {},
    )


def test_decider_picks_the_evidenced_position() -> None:
    # Wren alone dissented AND pulled a fact — the evidenced position wins.
    score = _score(
        dissents=2,
        fact_pulls=1,
        turns_by_agent={"wren": 3},
        dissents_by_agent={"wren": 2},
        fact_pulls_by_agent={"wren": 1},
    )
    d = decide(score)
    assert d == Decision(
        kind=DecisionKind.DECIDE,
        task_id="t-1",
        winner="wren",
        reason=d.reason,
    )
    assert d.winner == "wren"


def test_decider_neutral_peer_turns_never_block_the_evidenced_position() -> None:
    # Wren supplied ALL the evidence; Finch only took a neutral turn (a
    # report). The topic IS decidable — a peer's evidence-free turns are
    # irrelevant (swarm-review MED on #39: sole evidence-supplier, not sole
    # turn-taker, is the decidable condition).
    score = _score(
        dissents=1,
        fact_pulls=1,
        turns_by_agent={"wren": 2, "finch": 1},
        dissents_by_agent={"wren": 1},
        fact_pulls_by_agent={"wren": 1},
    )
    d = decide(score)
    assert d.kind is DecisionKind.DECIDE
    assert d.winner == "wren"


def test_decider_two_evidenced_candidates_resolve_through_the_tiebreak() -> None:
    # Both agents dissented AND fact-pulled: the documented tiebreak chain is
    # live on the real decide() path — most fact-pulls wins here.
    score = _score(
        dissents=3,
        fact_pulls=3,
        turns_by_agent={"wren": 3, "finch": 3},
        dissents_by_agent={"wren": 1, "finch": 2},
        fact_pulls_by_agent={"wren": 2, "finch": 1},
    )
    d = decide(score)
    assert d.kind is DecisionKind.DECIDE
    assert d.winner == "wren"


def test_decider_synthesizes_when_no_evidence() -> None:
    # All agreements / no dissent or fact-pull → cannot decide → synthesize.
    d = decide(_score(dissents=0, fact_pulls=0, turns_by_agent={"wren": 2, "finch": 2}))
    assert d.kind is DecisionKind.SYNTHESIZE
    assert d.winner is None


def test_decider_synthesizes_on_all_unsigned() -> None:
    d = decide(_score(unsigned=5, turns_by_agent={"wren": 3, "finch": 2}))
    assert d.kind is DecisionKind.SYNTHESIZE


def test_decider_synthesizes_when_evidence_split_across_agents() -> None:
    # A GENUINE split: Wren dissented, Finch pulled the fact — neither agent
    # holds both halves, so no single backed position exists → synthesize.
    d = decide(
        _score(
            dissents=1,
            fact_pulls=1,
            turns_by_agent={"wren": 1, "finch": 1},
            dissents_by_agent={"wren": 1},
            fact_pulls_by_agent={"finch": 1},
        )
    )
    assert d.kind is DecisionKind.SYNTHESIZE


def test_decider_tiebreak_chain_is_deterministic() -> None:
    from fleet_orchestrator.score import _Candidate, _tiebreak

    # fact_pulls dominates.
    cands = (
        _Candidate("finch", fact_pulls=1, dissents=9),
        _Candidate("wren", fact_pulls=2, dissents=1),
    )
    assert _tiebreak(cands).agent_id == "wren"
    # equal fact_pulls → most dissents.
    cands = (
        _Candidate("finch", fact_pulls=2, dissents=1),
        _Candidate("wren", fact_pulls=2, dissents=5),
    )
    assert _tiebreak(cands).agent_id == "wren"
    # equal fact_pulls AND dissents → lexicographically smallest agent id.
    cands = (
        _Candidate("wren", fact_pulls=2, dissents=2),
        _Candidate("finch", fact_pulls=2, dissents=2),
    )
    assert _tiebreak(cands).agent_id == "finch"


# ---- Synthesis carve-out + narration ---------------------------------------


async def test_score_synthesize_dispatches_agent_turn_to_manager_inbox() -> None:
    bus = _RecordingBus()
    await assign_synthesis(
        bus, _session("t-9"), env="dev", new_id="syn-1", now="2026-07-08T00:00:00Z"
    )
    assert len(bus.published) == 1
    subject, env = bus.published[0]
    assert subject == subjects.agent_inbox("dev", "heron")
    assert env.to == "heron" and env.sender == "heron"
    # Templated instruction — ids/state-words only, no worker prose.
    assert env.payload["text"] == (
        "[t-9] synthesize: the workers did not converge — take both positions, "
        "decide the right thing, and assign it."
    )


async def test_score_decision_narrates_via_single_narrator() -> None:
    bus = _RecordingBus()
    narrator = Narrator(
        bus, env="dev", manager="heron", new_id=lambda: "n1", now=lambda: "2026-07-08T00:00:00Z"
    )
    decision = Decision(kind=DecisionKind.DECIDE, task_id="t-1", winner="wren")
    await narrate_decision(narrator, _session(), decision, now="2026-07-08T00:00:00Z")
    assert len(bus.published) == 1
    subject, env = bus.published[0]
    assert subject == subjects.agent_inbox("dev", "heron")
    assert env.payload["text"] == "[t-1] decided for wren (evidenced position)."


async def test_circlejerk_trip_narrates_via_single_narrator() -> None:
    bus = _RecordingBus()
    narrator = Narrator(
        bus, env="dev", manager="heron", new_id=lambda: "n1", now=lambda: "2026-07-08T00:00:00Z"
    )
    event = ScoreEvent(
        kind=ScoreEventKind.CIRCLEJERK_DETECTED,
        task_id="t-1",
        run_length=4,
        agents=("finch", "wren"),
    )
    await narrate_score_event(narrator, _session(), event, now="2026-07-08T00:00:00Z")
    assert len(bus.published) == 1
    _, env = bus.published[0]
    assert env.payload["text"] == (
        "[t-1] circlejerk detected — agreements looping, no dissent or fact-pull."
    )


def test_decider_single_dissent_fact_pull_turn_earns_candidacy_end_to_end() -> None:
    # Through the real stream: one turn signed dissent + fact_pull counts as
    # BOTH kinds of evidence, so its agent is decidable immediately — even with
    # a neutral peer turn in the mix.
    keeper = Scorekeeper(circlejerk_n=4)
    keeper.observe(_turn("finch", "t-9", stance="neutral", fact_pull=False))
    keeper.observe(_turn("wren", "t-9", stance="dissent", fact_pull=True))
    score = keeper.score_for("t-9")
    assert score is not None
    assert score.dissents_by_agent == {"wren": 1}
    assert score.fact_pulls_by_agent == {"wren": 1}
    d = decide(score)
    assert d.kind is DecisionKind.DECIDE
    assert d.winner == "wren"
