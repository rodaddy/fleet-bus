"""Pure envelope → synthetic-message mapping — no gateway, fully testable.

The subscriber's job is to turn an inbound fleet-bus :class:`~fleet_nats.Envelope`
into a synthetic internal message the Hermes gateway can run as a full agent turn.
The *shape* of that mapping (what text, which target channel/thread, which
identity) is pure data logic, so it lives here — free of any gateway import — and
is unit-tested directly. The adapter (:mod:`fleet_subscriber.adapter`) then feeds
this into the real ``MessageEvent``/``SessionSource`` on the host.

The mapping is deliberately conservative: it reads only the typed envelope fields
and the sanctioned payload keys, never eval/exec, so a malformed publisher cannot
smuggle behavior into the agent turn.
"""

from __future__ import annotations

from dataclasses import dataclass

from fleet_nats import NARRATION_KIND, ChannelMap, Envelope
from fleet_nats.tracing import traced
from loguru import logger


@dataclass(frozen=True)
class SyntheticMessage:
    """The gateway-agnostic description of the agent turn to run.

    The adapter maps this onto ``MessageEvent(text=..., source=SessionSource(...),
    internal=True)`` on the host. Kept separate so the decision logic is proven
    without a gateway.

    Attributes:
        text: The message text to run as the agent's turn (the dispatch/relay
            prompt the manager sent).
        chat_id: The target Discord channel id the agent's reply should post to.
        thread_id: Optional thread id for threaded replies.
        chat_type: ``"channel"`` / ``"thread"`` / ``"dm"``.
        user_id: The synthetic sender identity (for audit; ``internal=True`` means
            auth is bypassed regardless).
        task_id: The k-board task id this turn relates to (carried for logging).
        kind: The originating envelope kind (``dispatch`` / ``relay`` / ``wake`` /
            ``inbox``) — drives nothing here, carried for observability.
    """

    text: str
    chat_id: str
    thread_id: str | None
    chat_type: str
    user_id: str
    task_id: str | None
    kind: str


# Payload keys the mapping honours. A publisher supplies the turn text under
# "text"; the target channel/thread come from the envelope's channel field and an
# optional payload "thread_id". Everything else on the payload is ignored.
_TEXT_KEY = "text"
_THREAD_KEY = "thread_id"

# A narration envelope (#13) is a manager-only status BEAT, not a prompt. Fed to
# the gateway raw, the LLM would *react* to the beat line instead of announcing
# it — so the mapping wraps it in a fixed announce instruction. The wrapper is
# a closed template (the beat itself is already templated metadata); Discord
# still gets the manager's own words, per the display invariant.
_NARRATION_INSTRUCTION = (
    "Fleet coordination status beat (not a request): {beat}\n"
    "Announce this status to the channel in one brief line of your own words. "
    "Do not ask questions or start new work."
)


def _resolve_channel(channel: str | None, channels: ChannelMap | None) -> str | None:
    """Canonicalise ``channel`` through the portal map, or pass it through.

    Two spellings of one room (the slug ``arena`` and its Discord id) must produce
    ONE ``chat_id``, because the adapter keys its ``chat_id -> task_id``
    correlation on that value: two spellings would mean a reply routed under one
    spelling never finds the task opened under the other.

    Pass-through (not drop) for an unknown channel is deliberate — see
    :func:`envelope_to_message` step 3.

    Args:
        channel: A coordination slug or a Discord channel id.
        channels: The portal map, or ``None`` when none is loaded.

    Returns:
        The canonical slug when the map knows this channel; the stripped input
        when it does not; ``None`` for a blank/absent channel.
    """
    if not channel or not channel.strip():
        return None
    stripped = channel.strip()
    if channels is None:
        return stripped
    return channels.resolve_slug(stripped) or stripped


def reply_channel(chat_id: str | None) -> str | None:
    """The channel token to scope an outbound subject with (#117), or ``None``.

    ``None`` selects the BARE subject form. Two cases degrade to it:

    - **empty** — an empty token would make :func:`fleet_nats.subjects.slug`
      raise and take down a send that is perfectly deliverable on the bare form.
    - **wildcard** — ``chat_id`` traces back to ``envelope.channel``, a token the
      PUBLISHER of the inbound message controls, and ``slug`` deliberately
      permits ``*`` because authorization grant builders need it. A hostile
      ``channel="*"`` would build ``{env}.agent.*.status.{task}``, which is
      byte-identical to the hub's own subscription wildcard and invalid as a
      publish subject — NATS drops such a publish asynchronously, so the reply
      would vanish and the task would hang to timeout with no error at the call
      site. The bare form keeps it deliverable (the hub subscribes both forms).

    Shared by the real adapter and the dummy so the install-proof round-trip
    cannot drift from the subject shape production actually publishes.
    """
    if not chat_id:
        return None
    if "*" in chat_id or ">" in chat_id:
        logger.warning(
            "fleet_subscriber.reply_channel | wildcard channel; using the BARE "
            "subject form instead of a scoped one channel={}",
            chat_id,
        )
        return None
    return chat_id


@traced
def envelope_to_message(
    envelope: Envelope,
    *,
    channels: ChannelMap | None = None,
    home_channel: str | None = None,
    synthetic_user_id: str = "fleet-nats",
) -> SyntheticMessage | None:
    """Map an inbound envelope to a :class:`SyntheticMessage`, or ``None`` to skip.

    Returns ``None`` (skip, logged by the caller) when the envelope carries no
    turn text or no resolvable target channel — a message the agent cannot act on
    should never be forged into a turn.

    Channel resolution (fleet-bus #118) runs through the persisted portal map, so
    the coordination side and the display side cannot drift on a channel's name:

    1. The envelope's own ``channel`` — canonicalised through the map, so a slug
       and the equivalent Discord id both land on ONE canonical slug.
    2. ``home_channel`` (this agent's own channel slug), likewise canonicalised —
       the fallback for an envelope that names no channel at all.
    3. A channel the map does not know is passed through UNCHANGED rather than
       dropped: the map is authoritative for *translation*, not for permission,
       and an unreachable k-board (empty map) must not stop an explicitly
       addressed turn from running. The outbound leg is where an unmapped channel
       is refused, because that is the leg that would otherwise post into the
       wrong room.

    Args:
        envelope: The inbound fleet-bus envelope (dispatch/relay/wake/inbox).
        channels: The persisted portal map. ``None`` (or empty) means no
            translation is available — resolution degrades to pass-through.
        home_channel: This agent's own channel slug, used only when the envelope
            names no channel. ``None`` means no fallback.
        synthetic_user_id: Identity stamped on the synthetic message.

    Returns:
        The :class:`SyntheticMessage` to run, or ``None`` to skip this envelope.
    """
    text = envelope.payload.get(_TEXT_KEY)
    if not isinstance(text, str) or not text.strip():
        # No prompt to run — nothing to turn into an agent turn.
        return None

    chat_id = _resolve_channel(envelope.channel, channels) or _resolve_channel(
        home_channel, channels
    )
    if not chat_id:
        # No target channel — the reply would have nowhere to go.
        return None

    if envelope.kind == NARRATION_KIND:
        # A beat is announced, never executed (see _NARRATION_INSTRUCTION).
        text = _NARRATION_INSTRUCTION.format(beat=text)

    raw_thread = envelope.payload.get(_THREAD_KEY)
    thread_id = str(raw_thread) if raw_thread is not None else None
    chat_type = "thread" if thread_id else "channel"

    return SyntheticMessage(
        text=text,
        chat_id=chat_id,
        thread_id=thread_id,
        chat_type=chat_type,
        user_id=synthetic_user_id,
        task_id=envelope.task_id,
        kind=envelope.kind,
    )
