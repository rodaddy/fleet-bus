"""Deterministic detectors + policies that feed the state machine.

The state machine (:mod:`fleet_orchestrator.transitions`) decides *what to do*
given an :class:`~fleet_orchestrator.state.Event`. This module is the
deterministic glue that turns raw worker output into those events and turns an
arbitration request into a decision — the parts issue #4 calls out as "explicit
code, testable without a live agent":

    detect_missing_handoff(...)  raw worker status -> HANDOFF_MISSING event | None
    decide_arbitration(...)      an arbitrate request -> a deterministic answer

No LLM, no I/O, no clock, no random. Pure functions over pure data so both the
"did a worker forget to hand off" and "who goes first" logic are unit-testable in
isolation, exactly like the transition core.
"""

from __future__ import annotations

import re

from fleet_orchestrator.state import Event, EventKind


def _is_mentioned(peer: str, lowered_text: str, mention_token: str) -> bool:
    """Whether ``peer`` is @-mentioned in ``lowered_text`` on a token boundary.

    A plain substring test would falsely count ``@finchlicious`` or
    ``email@finch.com`` as a mention of ``finch`` and suppress a real
    missing-handoff (a false negative → the coordination stall this whole path
    exists to catch). We require the mention sigil to start a token (not be
    preceded by a word char, so ``email@finch`` does not count) and the peer name
    to end on a boundary (not be followed by a word char, so ``@finchlicious``
    does not count).
    """
    pattern = rf"(?<!\w){re.escape(mention_token)}{re.escape(peer.lower())}(?!\w)"
    return re.search(pattern, lowered_text) is not None


def detect_missing_handoff(
    *,
    sender: str,
    text: str,
    peers: tuple[str, ...],
    mention_token: str = "@",
) -> Event | None:
    """Detect a worker finishing a turn without handing off to the peer it needs.

    The failure this replaces (the June-5 DAP): worker A does its part and stops,
    never mentioning worker B, so B never learns it's their turn and coordination
    stalls with no human to nudge it. We detect it deterministically: among the
    *other* workers on the task (``peers`` minus ``sender``), if the worker's
    output mentions none of them, a handoff is missing and the orchestrator must
    relay to the first un-mentioned peer.

    A mention is ``{mention_token}{peer}`` (case-insensitive), matching how agents
    address each other in Discord. The check is intentionally simple and
    explicit — no NLP, no model — so it is fully reproducible under test.

    Args:
        sender: The worker whose output this is.
        text: The worker's raw turn output.
        peers: All workers on the task (including ``sender``).
        mention_token: The mention sigil (default ``@``).

    Returns:
        A ``HANDOFF_MISSING`` :class:`Event` naming the peer to relay to, or
        ``None`` when the worker mentioned a peer (handoff present) or has no
        peers to hand off to.
    """
    others = [p for p in peers if p != sender]
    if not others:
        # Solo worker — there is no peer a handoff could be missing to.
        return None

    lowered = text.lower()
    if any(_is_mentioned(peer, lowered, mention_token) for peer in others):
        # At least one peer was addressed; the handoff is present.
        return None

    # Nobody was mentioned — relay to the first peer deterministically (sorted so
    # the choice is stable and reproducible, never dict/order-dependent).
    relay_to = sorted(others)[0]
    return Event(
        kind=EventKind.HANDOFF_MISSING,
        actor=sender,
        relay_to=relay_to,
        context={"missed_from": sender, "text": text},
    )


def decide_arbitration(*, topic: str, workers: tuple[str, ...]) -> str:
    """Return a deterministic arbitration decision — never an LLM.

    The manager is asked "who goes first?" / "which of us owns this?" and must
    answer the same way every time for the same inputs. Policy: pick the
    lexicographically-first worker id. Trivial, but the point is *determinism and
    auditability*, not cleverness — a case-statement the whole fleet can reason
    about, replacing the non-deterministic June-5 free-for-all.

    Args:
        topic: The decision topic (carried for logging/audit; the policy is
            worker-ordering based, so the topic does not change the outcome
            today — but it is part of the signature so a richer per-topic policy
            can slot in without changing callers).
        workers: The candidate workers.

    Returns:
        The chosen worker id.

    Raises:
        ValueError: If ``workers`` is empty — there is nobody to choose, and
            silently returning "" would let a bad decision ride. Fail loud.
    """
    if not workers:
        raise ValueError(f"decide_arbitration: no workers to arbitrate topic {topic!r}")
    return sorted(workers)[0]
