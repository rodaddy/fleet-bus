"""The shared payload contract — the standard shape of ``Envelope.payload``.

The envelope (``fleet_core.spec.Envelope``) is the coordination frame the bus
reads to route, dedupe, and derive completion; its ``payload`` is OPAQUE to the
bus (C6). This module gives the *body* a shared, owner-extensible shape so every
actor on the bus — Open Brain, the agent fleet, the k-board, the Dash apps —
carries its data the same way without re-encoding anything NATS already owns
(routing rides the subject, request/reply correlation rides the reply-to inbox,
integrity/size/dedup ride NATS/JetStream, and the transport bearer rides NATS
headers).

The shape is four slots — ``header · data · extras · footer`` — and only four
(the model is ``extra="forbid"``; you cannot invent a fifth top-level slot):

* ``header`` — app-level identity/scope the *receiver* resolves (NOT the
  transport bearer). Per message type via ``HeaderBase`` subclass; ``actor`` is
  always required.
* ``data`` — the typed, source-described content. Owner-specced: strict
  (``extra="forbid"``) or open (``extra="allow"``) is the owner's call.
* ``extras`` — the universal open bag; the ONLY slot that may legitimately be
  ``{}``. Adjacent, non-authoritative, forward-compat context.
* ``footer`` — observability/meta NATS does not carry (trace ids, client
  version), destined for logging → Langfuse. Per message type via
  ``FooterBase`` subclass.

Three invariants (the base-package rule for message bodies):

1. **Every slot is dict-typed in shape** — never a list/scalar. ``extras`` given
   ``None`` coerces to ``{}`` (it is the empty-legal slot); ``header`` / ``data``
   / ``footer`` given ``None`` or a list/scalar are a hard reject, so a receiver
   can always treat a slot as a mapping.
2. **Only ``extras`` may be empty.** ``header`` / ``data`` / ``footer`` carry the
   fields their message type requires, so an empty one is rejected unless that
   type genuinely declares no required fields.
3. **Fail-loud with a reason.** A payload whose info is wrong for its message
   type raises a :class:`pydantic.ValidationError` naming the offending field —
   never silent coercion or best-effort acceptance. This is the same fail-loud
   posture as ``Envelope.from_bytes`` (C2): a malformed message is a poison
   message, not a half-valid one.
"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator

__all__ = [
    "FooterBase",
    "HeaderBase",
    "Payload",
]


class HeaderBase(BaseModel):
    """Shared base for a message type's header.

    App-level identity/scope the *receiver* resolves — not transport auth (the
    bearer rides NATS headers; routing rides the subject). A message type
    subclasses this to add the identity fields THAT type requires. ``actor`` is
    the one field every type carries, so an empty header is already rejected.
    ``extra="allow"`` lets a type add identity fields without a subclass when it
    wants to stay loose.
    """

    model_config = ConfigDict(extra="allow")

    actor: str
    """Who/what this payload is FROM at the app layer (not the transport principal)."""


class FooterBase(BaseModel):
    """Shared base for a message type's footer.

    Observability/meta NATS does not carry, destined for logging → Langfuse.
    NOT coordination (that is ``typed_signal`` on the envelope). All base fields
    are optional; a message type subclasses this to make the telemetry IT
    requires mandatory (e.g. an observable op requiring ``trace_id``).
    """

    model_config = ConfigDict(extra="allow")

    trace_id: str | None = None
    client: str | None = None
    client_version: str | None = None


class Payload[HeaderT: HeaderBase, DataT: BaseModel, FooterT: FooterBase](BaseModel):
    """The standard shape of what rides inside ``Envelope.payload``.

    ``header`` / ``data`` / ``footer`` are each owner-typed per message type via
    the type parameters — each carries the fields ITS type requires, so an empty
    one is rejected unless that type declares none. ``extras`` is the ONLY slot
    that legitimately defaults to ``{}``. The four slots are fixed
    (``extra="forbid"``): nothing else lives at this level.

    Fail-loud: a message whose header/data/footer is missing a required field or
    mistyped raises a :class:`pydantic.ValidationError` naming the field. No
    default papers over a wrong-for-the-type payload. ``validate_assignment``
    keeps that guarantee after construction — a sender cannot mutate a slot into
    a contract-violating shape and then serialize it onto the wire.
    """

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    header: HeaderT
    data: DataT
    extras: dict[str, Any] = Field(default_factory=dict)
    footer: FooterT

    @field_validator("extras", mode="before")
    @classmethod
    def _extras_is_dict(cls, value: Any) -> dict[str, Any]:
        """Coerce a missing ``extras`` (``None``) to ``{}``; reject non-dicts.

        ``extras`` is the one slot that may be empty, but it must still be a
        mapping in shape — a list/scalar is a hard reject with a reason, never
        coerced.
        """
        if value is None:
            return {}
        if not isinstance(value, dict):
            raise ValueError("extras must be a dict")
        return value
