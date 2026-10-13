"""The parsed-completion value type carried by a coordinated turn.

Completion is a *typed* signal on the message envelope (``fleet_core.spec.Done``
via ``Envelope.typed_signal``), not a token in reply prose. The adapter's
coordinated send path derives a :class:`Completion` only from a typed
:class:`~fleet_subscriber.turn.Turn` (its named ``done`` field) and emits it as
the un-spoofable ``done`` sentinel via ``fleet_nats.sentinel.done_envelope``.

The legacy ``[[done]]`` / ``[[done:outcome]]`` reply-text trailer parser lived
here; it was removed (fleet-bus #205) because completion is an envelope field,
never text — a literal token inside ``visible_text`` is inert display prose. This
module now carries only the value type and the default outcome the typed path
uses.
"""

from __future__ import annotations

from dataclasses import dataclass

DEFAULT_OUTCOME = "completed"


@dataclass(frozen=True)
class Completion:
    """A coordinated turn's completion, recovered from the typed turn's ``done``.

    Attributes:
        visible_text: The agent's reply text, safe to surface to humans.
        outcome: The terminal outcome (defaults to ``completed``).
    """

    visible_text: str
    outcome: str
