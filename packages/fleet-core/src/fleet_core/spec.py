"""Typed fleet-bus message spec.

The C2-a slice promotes the existing ``fleet_nats.envelope`` wire contract into
fleet-core without changing the bytes callers already exchange. The FIPA-ACL
performative subset is intentionally small and grounded in current fleet plan
language: request, inform, propose, agree, refuse, and failure.

Envelope vs. message body (the base-package rule — see
``docs/contracts/C6-envelope-vs-message-body.md``): every message is one JSON
object with two layers. The ENVELOPE — the known wire keys plus ``typed_signal``
(``heartbeat`` / ``presence`` / ``stance`` / ``done``) — is the coordination
contract the bus reads to route, dedupe, decide durability, and derive
completion/stance. The message body (``payload``) is OPAQUE to the bus: it is
validated only to *be* a JSON object and is never inspected for a coordination
signal. A receiver may use anything in ``payload``, but nothing in it — and no
token inside body text — can drive coordination. The split is code-enforced:
``to_bytes`` writes typed fields last (body cannot spoof them), and
``signal_prefers_core`` reads the typed model, never the body or the subject.
"""

from __future__ import annotations

import json
import logging
import math
from decimal import Decimal
from enum import StrEnum
from typing import Any, Literal, Self

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    StrictBool,
    StrictInt,
    field_validator,
    model_validator,
)

ENVELOPE_VERSION = 1
logger = logging.getLogger(__name__)

_KNOWN_WIRE_KEYS: frozenset[str] = frozenset(
    {
        "id",
        "ts",
        "from",
        "kind",
        "payload",
        "to",
        "task_id",
        "channel",
        "topic",
        "correlation_id",
        "version",
        "act",
        "state",
    }
)


class Act(StrEnum):
    """Small fleet-owned FIPA-ACL performative subset."""

    REQUEST = "request"
    INFORM = "inform"
    PROPOSE = "propose"
    AGREE = "agree"
    REFUSE = "refuse"
    FAILURE = "failure"

    @classmethod
    def _missing_(cls, value: object) -> Act | None:
        """Preserve same-major future performatives as open enum values."""
        if not isinstance(value, str):
            return None
        member = str.__new__(cls, value)
        member._name_ = "UNKNOWN"
        member._value_ = value
        return member


class State(StrEnum):
    """A2A task states carried on every envelope when known."""

    SUBMITTED = "submitted"
    WORKING = "working"
    INPUT_REQUIRED = "input-required"
    COMPLETED = "completed"
    FAILED = "failed"

    @classmethod
    def _missing_(cls, value: object) -> State | None:
        """Preserve same-major future task states as open enum values."""
        if not isinstance(value, str):
            return None
        member = str.__new__(cls, value)
        member._name_ = "UNKNOWN"
        member._value_ = value
        return member


class StancePosition(StrEnum):
    """The single fleet-core stance vocabulary."""

    AGREE = "agree"
    DISSENT = "dissent"
    NEUTRAL = "neutral"


def _reject_lossy_float(token: str) -> float:
    """Reject JSON floats whose Python decode cannot round-trip unchanged."""
    value = float(token)
    if not math.isfinite(value):
        raise ValueError(
            f"Envelope.from_bytes: numeric literal {token!r} overflows to a "
            "non-finite float; not allowed"
        )
    exact = Decimal(token)
    if value == 0.0 and exact != 0:
        raise ValueError(
            f"Envelope.from_bytes: numeric literal {token!r} underflows to 0.0; "
            "its value would not survive the round-trip"
        )
    if exact != Decimal(repr(value)):
        raise ValueError(
            f"Envelope.from_bytes: numeric literal {token!r} loses precision as a "
            "float; its value would not survive the round-trip"
        )
    return value


def _reject_non_finite(token: str) -> Any:
    """Reject non-finite JSON constants accepted by Python's JSON decoder."""
    raise ValueError(f"Envelope.from_bytes: non-finite JSON constant {token!r} not allowed")


def _opt_str(value: Any) -> str | None:
    """Coerce optional scalar wire fields to ``str | None``."""
    return None if value is None else str(value)


def _require_str(field: str, value: Any) -> str:
    """Return a required wire field as a non-empty string or raise."""
    if not isinstance(value, str) or not value:
        raise ValueError(
            f"Envelope.from_bytes: required field {field!r} must be a non-empty string"
        )
    return value


class Envelope(BaseModel):
    """A single fleet-bus message.

    Unknown top-level wire keys are carried in ``extra`` by ``from_bytes`` and
    unknown constructor keys are carried by Pydantic's ``extra="allow"`` path.
    ``to_bytes`` merges both carriers back onto the wire, then writes known
    fields last so typed fields cannot be spoofed by opaque data.
    """

    model_config = ConfigDict(extra="allow", populate_by_name=True)

    id: str
    ts: str
    sender: str = Field(alias="from")
    kind: str
    # The message BODY — opaque to the bus (C6). Free-form per-``kind`` JSON the
    # receiving consumer owns; validated only to *be* a JSON object (``from_bytes``
    # rejects a non-object). The bus NEVER reads a ``payload`` key for routing,
    # dedup, durability, or completion/stance — those come from typed envelope
    # fields (the known wire keys and ``typed_signal``) only. A control-looking
    # string inside the body is inert data, not a signal.
    payload: dict[str, Any] = Field(default_factory=dict)
    to: str | None = None
    task_id: str | None = None
    channel: str | None = None
    topic: str | None = None
    correlation_id: str | None = None
    version: StrictInt = ENVELOPE_VERSION
    act: Act | None = None
    state: State | None = None
    extra: dict[str, Any] = Field(default_factory=dict, exclude=True)
    # The typed built-in signal this envelope carries — the ENVELOPE side of the
    # C6 split, and the ONLY place a coordination signal (completion, stance,
    # liveness) lives. ``to_bytes`` serializes a validated declaration for known
    # built-ins, and ``from_bytes`` reconstructs only those known types. Unknown
    # declarations remain additive wire data and do not gain the heartbeat/presence
    # core exception. Completion/stance are read from here, never from body text
    # (the ``[[done]]``/``[[stance]]`` trailer path was removed, fleet-bus #205).
    typed_signal: Any = Field(default=None, exclude=True)

    @field_validator("id", "ts", "sender", "kind")
    @classmethod
    def _non_empty_required(cls, value: str, info: Any) -> str:
        """Reject empty required strings."""
        if not value:
            raise ValueError(f"Envelope.{info.field_name} must be non-empty")
        return value

    @model_validator(mode="after")
    def _valid_version(self) -> Self:
        """Keep constructor callers behind the same fail-loud version gate."""
        if isinstance(self.version, bool) or not isinstance(self.version, int):
            raise ValueError(f"Envelope.version must be a JSON integer, got {self.version!r}")
        if self.version < 1:
            raise ValueError(f"Envelope.version must be >= 1, got {self.version}")
        if self.version > ENVELOPE_VERSION:
            raise ValueError(
                f"Envelope.version refuses message version {self.version} > "
                f"supported {ENVELOPE_VERSION}"
            )
        return self

    @classmethod
    def new(
        cls,
        *,
        msg_id: str,
        ts: str,
        sender: str,
        kind: str,
        payload: dict[str, Any] | None = None,
        to: str | None = None,
        task_id: str | None = None,
        channel: str | None = None,
        topic: str | None = None,
        correlation_id: str | None = None,
        act: Act | str | None = None,
        state: State | str | None = None,
        typed_signal: object | None = None,
    ) -> Envelope:
        """Construct an envelope with caller-supplied id and timestamp.

        ``typed_signal`` binds the typed built-in signal (``Heartbeat`` /
        ``Presence`` / ``Stance`` / ``Done``) the envelope carries. Known
        built-ins serialize as an explicit typed-signal declaration so replay and
        forwarding preserve the durability policy without reading subjects,
        bare ``kind`` strings, or opaque payload keys.
        """
        return cls.model_validate(
            {
                "id": msg_id,
                "ts": ts,
                "sender": sender,
                "kind": kind,
                "payload": payload or {},
                "to": to,
                "task_id": task_id,
                "channel": channel,
                "topic": topic,
                "correlation_id": correlation_id,
                "act": act,
                "state": state,
                "typed_signal": typed_signal,
            }
        )

    def prefers_core_delivery(self) -> bool:
        """Whether this envelope's bound typed signal owns the core exception.

        True only when a typed built-in signal is bound and its model declares
        ``jetstream is False`` (``Heartbeat`` / ``Presence``). Read from the typed
        model, never from ``kind`` or the subject. An envelope with no bound
        typed signal — or one bound to a durable signal — is durable by default.
        """
        return signal_prefers_core(self.typed_signal)

    def to_bytes(self) -> bytes:
        """Serialize to UTF-8 JSON using the stable fleet-bus wire shape."""
        body: dict[str, Any] = dict(self.extra)
        pydantic_extra = self.model_extra or {}
        collisions = {
            key
            for key, value in pydantic_extra.items()
            if key in body and body[key] != value and key not in _KNOWN_WIRE_KEYS
        }
        if collisions:
            joined = ", ".join(sorted(collisions))
            raise ValueError(f"Envelope.to_bytes: conflicting unknown extra fields: {joined}")
        body.update(pydantic_extra)
        body.update(
            {
                "id": self.id,
                "ts": self.ts,
                "from": self.sender,
                "kind": self.kind,
                "payload": self.payload,
                "to": self.to,
                "task_id": self.task_id,
                "channel": self.channel,
                "topic": self.topic,
                "correlation_id": self.correlation_id,
                "version": self.version,
                "act": self.act,
                "state": self.state,
            }
        )
        serialized_signal = typed_signal_to_wire(self.typed_signal)
        if serialized_signal is not None:
            body["typed_signal"] = serialized_signal
        return json.dumps(body, separators=(",", ":"), allow_nan=False).encode("utf-8")

    @classmethod
    def from_bytes(cls, raw: bytes) -> Envelope:
        """Parse an envelope from wire bytes.

        Raises:
            ValueError: If JSON decoding, required fields, payload shape, numeric
                preservation, typed enum validation, or version gates fail.
        """
        try:
            body = json.loads(
                raw.decode("utf-8"),
                parse_constant=_reject_non_finite,
                parse_float=_reject_lossy_float,
            )
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            raise ValueError(f"Envelope.from_bytes: undecodable message: {exc}") from exc
        if not isinstance(body, dict):
            raise ValueError("Envelope.from_bytes: message is not a JSON object")

        raw_payload = body.get("payload")
        if raw_payload is None:
            payload: dict[str, Any] = {}
        elif isinstance(raw_payload, dict):
            payload = raw_payload
        else:
            got = type(raw_payload).__name__
            raise ValueError(f"Envelope.from_bytes: payload must be a JSON object, got {got}")

        raw_version = body.get("version", ENVELOPE_VERSION)
        if isinstance(raw_version, bool) or not isinstance(raw_version, int):
            raise ValueError(
                f"Envelope.from_bytes: version must be a JSON integer, got {raw_version!r}"
            )
        version = raw_version
        if version < 1:
            raise ValueError(f"Envelope.from_bytes: version must be >= 1, got {version}")
        if version > ENVELOPE_VERSION:
            logger.warning(
                "Envelope.from_bytes: refusing message version %s > supported %s "
                "(fail-loud version gate; newer major may be misread)",
                version,
                ENVELOPE_VERSION,
            )
            raise ValueError(
                f"Envelope.from_bytes: refusing message version {version} > "
                f"supported {ENVELOPE_VERSION}"
            )

        typed_signal = typed_signal_from_wire(body.get("typed_signal"))
        extra = {key: value for key, value in body.items() if key not in _KNOWN_WIRE_KEYS}
        return cls.model_validate(
            {
                "id": _require_str("id", body.get("id")),
                "ts": _require_str("ts", body.get("ts")),
                "sender": _require_str("from", body.get("from")),
                "kind": _require_str("kind", body.get("kind")),
                "payload": payload,
                "to": _opt_str(body.get("to")),
                "task_id": _opt_str(body.get("task_id")),
                "channel": _opt_str(body.get("channel")),
                "topic": _opt_str(body.get("topic")),
                "correlation_id": _opt_str(body.get("correlation_id")),
                "version": version,
                "act": body.get("act"),
                "state": body.get("state"),
                "extra": extra,
                "typed_signal": typed_signal,
            }
        )


class Heartbeat(BaseModel):
    """Identity/liveness signal emitted on the heartbeat lane."""

    model_config = ConfigDict(extra="allow")

    agent_id: str
    role: str
    lanes: tuple[str, ...]
    seq: StrictInt
    last_turn_ts: str | None = None
    catalog_version: StrictInt | None = None
    # The installed fleet-package version the emitter runs (#203). This is how
    # install drift — distinct from subject-catalog skew — becomes observable on
    # the bus. Nullable so beats from clients that predate this field parse
    # cleanly as "unknown version" rather than being rejected.
    package_version: str | None = None
    ts: str | None = None
    jetstream: Literal[False] = False

    @field_validator("agent_id", "role")
    @classmethod
    def _required_non_empty(cls, value: str, info: Any) -> str:
        """Reject empty identity fields."""
        if not value:
            raise ValueError(f"Heartbeat.{info.field_name} must be non-empty")
        return value

    @field_validator("lanes")
    @classmethod
    def _lanes_non_empty_strings(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        """Reject malformed lane lists."""
        if not all(isinstance(item, str) and item for item in value):
            raise ValueError("Heartbeat.lanes must contain only non-empty strings")
        return value

    @model_validator(mode="after")
    def _valid_liveness_numbers(self) -> Self:
        """Reject bool/negative liveness counters."""
        if self.seq < 0:
            raise ValueError(f"Heartbeat.seq must be >= 0, got {self.seq}")
        if self.catalog_version is not None and self.catalog_version < 0:
            raise ValueError(f"Heartbeat.catalog_version must be >= 0, got {self.catalog_version}")
        return self


class Presence(Heartbeat):
    """Explicit presence signal using the same identity/liveness wire fields."""


class Stance(BaseModel):
    """Typed stance signal replacing ``[[stance:...]]`` trailers."""

    model_config = ConfigDict(extra="allow", populate_by_name=True, serialize_by_alias=True)

    position: StancePosition = Field(alias="stance")
    fact_pull: StrictBool = False
    because: str | None = Field(default=None, alias="reason")
    needs_from: tuple[str, ...] = Field(default_factory=tuple)

    @field_validator("position", mode="before")
    @classmethod
    def _normalize_position(cls, value: object) -> object:
        """Preserve legacy case-insensitive stance input."""
        if isinstance(value, str):
            return value.lower()
        return value

    @field_validator("needs_from")
    @classmethod
    def _needs_from_strings(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        """Reject malformed dependency identities."""
        if not all(isinstance(item, str) and item for item in value):
            raise ValueError("Stance.needs_from must contain only non-empty strings")
        return value


class Done(BaseModel):
    """Typed completion signal replacing agent-emitted ``[[done]]`` trailers."""

    model_config = ConfigDict(extra="allow")

    task_id: str
    outcome: str = "completed"

    @field_validator("task_id", "outcome")
    @classmethod
    def _non_empty_done_fields(cls, value: str, info: Any) -> str:
        """Reject empty completion fields."""
        if not value:
            raise ValueError(f"Done.{info.field_name} must be non-empty")
        return value


Completion = Done


# The typed built-in signals whose model owns the durability policy. A signal
# that declares ``jetstream = False`` on its own model is the intentional
# ephemeral exception (``Heartbeat`` and its ``Presence`` subclass); every other
# signal is durable by default. The transport reads this typed field, never the
# wire ``kind`` string or the subject, to decide delivery.
BuiltinSignal = Heartbeat | Presence | Stance | Done


_TYPED_SIGNAL_DATA_KEY = "data"
_TYPED_SIGNAL_TYPE_KEY = "type"


def typed_signal_to_wire(signal: object) -> dict[str, Any] | None:
    """Serialize a known built-in signal declaration for the envelope wire."""
    if isinstance(signal, Presence):
        return {
            _TYPED_SIGNAL_TYPE_KEY: "presence",
            _TYPED_SIGNAL_DATA_KEY: signal.model_dump(mode="json"),
        }
    if isinstance(signal, Heartbeat):
        return {
            _TYPED_SIGNAL_TYPE_KEY: "heartbeat",
            _TYPED_SIGNAL_DATA_KEY: signal.model_dump(mode="json"),
        }
    if isinstance(signal, Stance):
        return {
            _TYPED_SIGNAL_TYPE_KEY: "stance",
            _TYPED_SIGNAL_DATA_KEY: signal.model_dump(mode="json", by_alias=True),
        }
    if isinstance(signal, Done):
        return {
            _TYPED_SIGNAL_TYPE_KEY: "done",
            _TYPED_SIGNAL_DATA_KEY: signal.model_dump(mode="json"),
        }
    return None


def typed_signal_from_wire(value: object) -> BuiltinSignal | None:
    """Validate a known typed-signal wire declaration.

    Unknown valid declarations are preserved as ordinary extra data by
    ``Envelope.from_bytes`` and remain durable. Malformed declarations fail loud
    so they cannot accidentally acquire the heartbeat/presence opt-out.
    """
    if value is None:
        return None
    if not isinstance(value, dict):
        raise ValueError("Envelope.from_bytes: typed_signal must be a JSON object")

    signal_type = value.get(_TYPED_SIGNAL_TYPE_KEY)
    if not isinstance(signal_type, str) or not signal_type:
        raise ValueError("Envelope.from_bytes: typed_signal.type must be a non-empty string")

    data = value.get(_TYPED_SIGNAL_DATA_KEY)
    if not isinstance(data, dict):
        raise ValueError("Envelope.from_bytes: typed_signal.data must be a JSON object")

    if signal_type == "heartbeat":
        return Heartbeat.model_validate(data)
    if signal_type == "presence":
        return Presence.model_validate(data)
    if signal_type == "stance":
        return Stance.model_validate(data)
    if signal_type == "done":
        return Done.model_validate(data)
    return None


def signal_prefers_core(signal: object) -> bool:
    """Whether a typed signal's own model declares the ephemeral (core) exception.

    True only when the signal model carries ``jetstream is False`` as a typed
    field it owns (``Heartbeat`` / ``Presence``). This is the SOLE durable-default
    exception and it is read from the model, not inferred from a ``kind`` string,
    subject, or an opaque payload dict. Any signal (or non-signal) that does not
    declare a literal ``jetstream=False`` field routes durable.
    """
    return isinstance(signal, Heartbeat) and signal.jetstream is False


__all__ = [
    "Act",
    "BuiltinSignal",
    "Completion",
    "Done",
    "ENVELOPE_VERSION",
    "Envelope",
    "Heartbeat",
    "Presence",
    "Stance",
    "StancePosition",
    "State",
    "signal_prefers_core",
    "typed_signal_from_wire",
    "typed_signal_to_wire",
]
