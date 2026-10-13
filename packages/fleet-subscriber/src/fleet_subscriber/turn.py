"""The typed agent turn — a deserialized coordination structure, not scraped prose.

Issue #77. The subscriber used to recover a turn's *structure* (is it done, what
did the agent say, what stance did it sign) by REGEX-SCRAPING the agent's
free-form reply text: a ``[[done]]`` / ``[[stance:...]]`` trailer match plus a
shape-heuristic that tried to tell a gateway footer (``model · context% · cwd``)
apart from ordinary prose. Shape-guessing prose-versus-footer is structurally
unwinnable — it was spoofed four review rounds running.

The fix removes the guessing entirely: the agent turn becomes a TYPED, explicitly
deserialized structure. A producer emits its turn as a JSON object; this module
validates it into a :class:`Turn` at the single deserialization boundary
(:meth:`~fleet_subscriber.adapter.NatsAdapter.send`). Structure now comes from
NAMED FIELDS, never from a pattern scanned over prose:

* ``visible_text`` is exactly what the agent said — a field, so a literal
  ``[[done]]`` or a ``co-op · 5s · ~/repo`` footer string sitting INSIDE it is
  just text. It cannot complete a turn or sign a stance; the spoof class the
  trailer/footer heuristics fought is simply gone on this path.
* ``done`` / ``outcome`` carry the completion signal that ``[[done:outcome]]``
  used to encode.
* ``stance`` / ``fact_pull`` / ``reason`` carry the signed stance that
  ``[[stance:...]]`` used to encode.

There is deliberately NO footer / attribution / model / token field: turn
attribution lives in Langfuse traces, not the coordination envelope, so the
footer is not carried here and never needs stripping.

The completion/stance TEXT trailer parsers are GONE (fleet-bus #205): a reply
that is not a typed :class:`Turn` carries no completion/stance — it is visible
prose only. :mod:`~fleet_subscriber.completion` and
:mod:`~fleet_subscriber.stance` now hold only the ``Completion`` / ``StanceSignal``
value types, not parsers.

Parsing is pure and lives here, separate from the adapter, so it is unit tested
without a gateway or a bus.
"""

from __future__ import annotations

import json

from fleet_nats import Completion as CoreCompletion
from fleet_nats import Stance as CoreStance
from fleet_nats import StancePosition
from loguru import logger
from pydantic import BaseModel, ConfigDict, ValidationError, field_validator, model_validator

_CORE_DONE_TASK_ID = "typed-turn"
_DEFAULT_OUTCOME = "completed"


class Turn(BaseModel):
    """One deserialized agent coordination turn.

    Fields mirror exactly what the adapter consumes today from the two text
    parsers — no more. ``visible_text`` is the only required field (a turn must
    say *something*); the rest default to the "unsigned, not done" turn.

    Attributes:
        visible_text: What the agent actually said — safe to surface to humans.
            A field, not a scrape: any trailer/footer-looking substring in here
            is inert text, never a control signal.
        done: Whether this turn completes the task (the old ``[[done]]``).
        outcome: The terminal outcome when ``done`` (the old ``[[done:outcome]]``);
            meaningful only when ``done`` is true, ``None`` otherwise.
        stance: The signed stance — ``agree`` / ``dissent`` / ``neutral`` — or
            ``None`` for an unsigned turn (the old ``[[stance:<kw>]]``).
            Case-insensitive on input (``Agree`` → ``agree``).
        fact_pull: Whether the agent pulled a fact this turn (old ``fact_pull``).
        reason: Optional freeform stance rationale (old ``reason=...``), or ``None``.
    """

    # ``extra="allow"``: an unexpected key is accepted and PRESERVED. Fleet-core's
    # typed signal models use the same forward-compatible posture: agreed fields
    # validate hard, while producer-specific metadata can ride the typed object
    # without forcing downstream packages to rev in lockstep.
    #
    # ``strict=True`` (kept): a JSON bool must be a bool, never a coerced
    # ``"true"`` / ``"yes"`` / ``1`` — mirroring the orchestrator's own strict wire
    # posture (fleet_orchestrator.score._require_fact_pull, which rejects
    # ``0``/``1``/``"true"``). An ambiguous truthiness coercion at this boundary
    # could flip ``done`` / ``fact_pull`` on a malformed turn; a mistyped field
    # fails validation and the reply is treated as visible prose only instead.
    model_config = ConfigDict(extra="allow", strict=True)

    visible_text: str
    done: bool = False
    outcome: str | None = None
    # Constrained to the three scorekeeper words (or absent) — a stance value
    # outside them is a malformed turn (fails validation → visible prose only),
    # never a meta the orchestrator's strict parse_turn_signal would reject.
    stance: StancePosition | None = None
    fact_pull: bool = False
    reason: str | None = None

    @field_validator("stance", mode="before")
    @classmethod
    def _normalize_stance_case(cls, value: object) -> object:
        """Lowercase a string ``stance`` before the Literal check (case-insensitive).

        A stance is case-insensitive — ``Agree`` / ``AGREE`` / ``agree`` all mean
        ``agree``. Without this, the case-sensitive ``Literal`` would fail the
        WHOLE turn on a capitalised stance and drop even a valid completion. Only
        ``str`` inputs are touched; ``None`` (and any non-str, left for the Literal
        to reject under strict) passes through unchanged.
        """
        if isinstance(value, str):
            return StancePosition(value.lower())
        return value

    @model_validator(mode="after")
    def _validate_with_fleet_core_signals(self) -> Turn:
        """Validate agreed turn fields with the fleet-core-backed signal models."""
        if self.done:
            CoreCompletion(
                task_id=_CORE_DONE_TASK_ID,
                outcome=self.outcome if self.outcome else _DEFAULT_OUTCOME,
            )
        if self.stance is not None:
            CoreStance(
                stance=self.stance,
                fact_pull=self.fact_pull,
                reason=self.reason,
            )
        return self


def parse_turn(content: str) -> Turn | None:
    """Deserialize ``content`` into a :class:`Turn`, or ``None`` to fall back.

    The single JSON deserialization boundary. Returns a :class:`Turn` only when
    ``content`` is a JSON OBJECT that validates against the model. Returns
    ``None`` — signalling the caller to treat the reply as visible prose only
    (no completion/stance) — for anything else:

    * not JSON at all (an ordinary text+trailer reply);
    * JSON that is not an object (a bare string / number / list / ``null``);
    * a JSON object that fails validation (missing ``visible_text``, a wrong
      field type, or an unknown/forbidden extra key).

    Never logs ``content`` (agent prose): only the failure class / kind is logged,
    per the never-log list.

    Args:
        content: The raw reply string handed to the adapter's ``send()``.

    Returns:
        A validated :class:`Turn` on the typed path, else ``None``.
    """
    try:
        parsed = json.loads(content)
    except (json.JSONDecodeError, ValueError):
        # Plain prose — not a typed turn. Not an error: the reply is visible text
        # only (no completion/stance). Debug-level so it never floods.
        logger.debug("fleet_subscriber.turn | content is not JSON — visible prose only")
        return None

    if not isinstance(parsed, dict):
        # Valid JSON but not an object (bare string/number/list/null) — a producer
        # never emits a turn as those, so treat as visible prose rather than guess.
        logger.debug(
            "fleet_subscriber.turn | JSON is not an object (type={}) — visible prose only",
            type(parsed).__name__,
        )
        return None

    try:
        return Turn.model_validate(parsed)
    except ValidationError as exc:
        # A JSON object that is NOT a valid turn (missing visible_text, bad type,
        # forbidden extra key). Log the error COUNT/shape, never the offending
        # values (which are agent prose). Treated as visible prose only — no
        # completion/stance signal is recovered.
        logger.warning(
            "fleet_subscriber.turn | JSON object failed Turn validation "
            "({} error(s)) — visible prose only",
            exc.error_count(),
        )
        return None
