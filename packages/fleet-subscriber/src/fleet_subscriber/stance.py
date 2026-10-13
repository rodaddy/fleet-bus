"""The parsed-stance value type carried by a coordinated turn.

Stance is a *typed* signal on the message envelope (``fleet_core.spec.Stance``
via ``Envelope.typed_signal``, mirrored onto ``payload.meta`` for pre-core
consumers), not a token in reply prose. The adapter's coordinated send path
derives a :class:`StanceSignal` only from a typed
:class:`~fleet_subscriber.turn.Turn` (its named ``stance`` / ``fact_pull`` /
``reason`` fields).

The legacy ``[[stance:agree|dissent|neutral ...]]`` reply-text trailer parser
lived here; it was removed (fleet-bus #205) because a signed stance is an
envelope field, never text — a literal token inside ``visible_text`` is inert
display prose. The three declared stance words remain the shared contract with
the orchestrator's scorekeeper (``fleet_orchestrator.score``), verified by test.
This module now carries only the value type.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class StanceSignal:
    """A coordinated turn's stance, recovered from the typed turn's fields.

    Attributes:
        visible_text: The agent's reply text, safe to surface to humans.
        stance: The declared stance word (``agree`` / ``dissent`` / ``neutral``),
            the canonical wire form.
        fact_pull: Whether the agent declared a fact-pull this turn.
        reason: An optional freeform rationale, or ``None``.
    """

    visible_text: str
    stance: str
    fact_pull: bool
    reason: str | None
