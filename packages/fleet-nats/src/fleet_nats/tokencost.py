"""The token-cost wire shape — the one shared ``kind="token_cost"`` contract (#27).

After each agent turn, the Hermes post-turn hook (rtech-hermes #457) emits one
token-cost event on its ``subjects.token_cost(env, agent)`` lane; the governor
(#13) sums per-task spend and enforces a budget, and the dashboard (#14) renders
it. Emitter (rtech-hermes) and consumers (fleet-orchestrator) live in different
repos — exactly the drift risk that produced the dispatch-subject mismatch — so
the shape lives here, in the transport layer both import, with one builder and
one strict parser. Neither side hand-rolls the dict.

**Counts, never bodies.** This payload carries measured token ACCOUNTING only:
agent id, task/turn ids, prompt/completion/total token counts, the model name,
and the timestamp. It NEVER carries the words a turn read or wrote, nor any body,
secret, or member id — the ``docs/logging.md`` never-log list applies to this
payload verbatim. A governor needs the size of a turn, not what was in it.
"""

from __future__ import annotations

from dataclasses import dataclass

from fleet_nats.envelope import Envelope

# The event's kind + payload keys — the shared contract. One definition.
TOKEN_COST_KIND = "token_cost"
AGENT_ID_KEY = "agent_id"
TASK_ID_KEY = "task_id"
TURN_ID_KEY = "turn_id"
PROMPT_TOKENS_KEY = "prompt_tokens"
COMPLETION_TOKENS_KEY = "completion_tokens"
TOTAL_TOKENS_KEY = "total_tokens"
MODEL_KEY = "model"


@dataclass(frozen=True, slots=True)
class TokenCost:
    """A parsed token-cost event — the consumer-side view of one turn's spend.

    Attributes:
        agent_id: The emitting agent (identity of who spent the tokens).
        task_id: The k-board task the turn belonged to (the governor sums by this).
        turn_id: The specific agent turn that was measured.
        prompt_tokens: Input tokens the turn consumed (>= 0).
        completion_tokens: Output tokens the turn produced (>= 0).
        total_tokens: prompt + completion — carried explicitly and checked, so an
            inconsistent emitter fails loud rather than under-counting a budget.
        model: The model name the turn ran against (e.g. for per-model cost).
        ts: The event's ISO timestamp (the envelope's ``ts``).
    """

    agent_id: str
    task_id: str
    turn_id: str
    prompt_tokens: int
    completion_tokens: int
    total_tokens: int
    model: str
    ts: str


def token_cost_envelope(
    *,
    msg_id: str,
    ts: str,
    sender: str,
    task_id: str,
    turn_id: str,
    prompt_tokens: int,
    completion_tokens: int,
    total_tokens: int,
    model: str,
) -> Envelope:
    """Build the token-cost envelope the Hermes post-turn hook emits (#457).

    Publish on ``subjects.token_cost(env, sender)``. Caller supplies id/ts
    (mirrors :meth:`Envelope.new`) so events stay deterministic under test.

    Args:
        msg_id: Unique message id.
        ts: ISO-8601 UTC timestamp of this event.
        sender: The emitting agent's id (also carried in the payload as agent_id).
        task_id: The k-board task the turn belonged to.
        turn_id: The measured agent turn.
        prompt_tokens: Input tokens consumed (>= 0).
        completion_tokens: Output tokens produced (>= 0).
        total_tokens: Must equal prompt + completion.
        model: The model name the turn ran against.

    Returns:
        The ``kind="token_cost"`` :class:`Envelope`.
    """
    return Envelope.new(
        msg_id=msg_id,
        ts=ts,
        sender=sender,
        kind=TOKEN_COST_KIND,
        payload={
            AGENT_ID_KEY: sender,
            TASK_ID_KEY: task_id,
            TURN_ID_KEY: turn_id,
            PROMPT_TOKENS_KEY: prompt_tokens,
            COMPLETION_TOKENS_KEY: completion_tokens,
            TOTAL_TOKENS_KEY: total_tokens,
            MODEL_KEY: model,
        },
    )


def _require_str(body: dict[str, object], key: str) -> str:
    """Return a required payload field as a non-empty ``str`` or raise ``ValueError``."""
    value = body.get(key)
    if not isinstance(value, str) or not value:
        raise ValueError(f"token_cost payload missing/invalid {key}")
    return value


def _require_count(body: dict[str, object], key: str) -> int:
    """Return a required payload field as a non-negative ``int`` or raise ``ValueError``.

    ``bool`` is a subclass of ``int`` — reject it explicitly so a stray ``True``
    can never be counted as one token.
    """
    value = body.get(key)
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise ValueError(f"token_cost payload missing/invalid {key}")
    return value


def parse_token_cost(envelope: Envelope) -> TokenCost:
    """Parse a received token-cost event strictly. Fail loud on a malformed one.

    Args:
        envelope: A received envelope from the token-cost lane.

    Returns:
        The parsed :class:`TokenCost`.

    Raises:
        ValueError: If the envelope is not a token-cost event or its payload is
            malformed — wrong kind, identity split (payload agent_id disagreeing
            with the envelope sender), missing/empty strings, non-int/bool/
            negative counts, an inconsistent total (total != prompt + completion),
            or a non-string model. Callers log and skip — a bad event must be
            visible, never silently summed into a budget. A zero-token turn is
            valid (a turn can legitimately spend nothing).
    """
    if envelope.kind != TOKEN_COST_KIND:
        raise ValueError(f"not a token_cost envelope: kind={envelope.kind!r}")
    body = envelope.payload
    agent_id = _require_str(body, AGENT_ID_KEY)
    if agent_id != envelope.sender:
        # One identity per event: a payload/sender split means emitter drift (or
        # forgery) — reject rather than crediting spend to the wrong agent.
        raise ValueError(
            f"token_cost identity mismatch: sender={envelope.sender!r} agent_id={agent_id!r}"
        )
    task_id = _require_str(body, TASK_ID_KEY)
    turn_id = _require_str(body, TURN_ID_KEY)
    prompt_tokens = _require_count(body, PROMPT_TOKENS_KEY)
    completion_tokens = _require_count(body, COMPLETION_TOKENS_KEY)
    total_tokens = _require_count(body, TOTAL_TOKENS_KEY)
    if total_tokens != prompt_tokens + completion_tokens:
        # Inconsistent accounting: a governor summing totals would silently under-
        # or over-count a budget. Fail loud so the emitter bug is visible.
        raise ValueError(
            f"token_cost total mismatch: total={total_tokens} "
            f"prompt={prompt_tokens} completion={completion_tokens}"
        )
    model = _require_str(body, MODEL_KEY)
    return TokenCost(
        agent_id=agent_id,
        task_id=task_id,
        turn_id=turn_id,
        prompt_tokens=prompt_tokens,
        completion_tokens=completion_tokens,
        total_tokens=total_tokens,
        model=model,
        ts=envelope.ts,
    )
