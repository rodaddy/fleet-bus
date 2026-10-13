"""The optional Discord voice tee — mirror a bus-driven reply to a real channel.

The subscriber's :meth:`send` publishes the agent's reply onto the NATS
``agent_status`` lane (delivery to the orchestrator). That lane is *coordination*
plumbing — the agent's actual voice never reaches a human-visible Discord
channel. The interim workaround is task-text telling the agent to post via its
own toolset; this module is the real lever.

When enabled (``FLEET_VOICE_TEE_ENABLED``), the adapter ALSO delivers the visible
reply text to a Discord channel — through the gateway's *own* in-process Discord
adapter, via the sanctioned ``gateway.run._gateway_runner_ref()`` →
``runner.adapters.get(Platform.DISCORD)`` → ``adapter.send(...)`` path (the same
seam ``tools/send_message_tool.py`` uses). No new HTTP client, no webhooks.

Per-channel targeting (fleet-bus #118)
--------------------------------------

The tee used to post EVERY reply to one id from ``FLEET_VOICE_TEE_CHANNEL_ID``,
ignoring the turn's own channel — so a task coordinated in ``fleet-ops`` and a
task coordinated in ``arena`` both landed in whichever single room that env named,
and a host could bridge exactly one channel. The target is now resolved PER TURN
from the turn's channel through the persisted portal map
(:class:`fleet_nats.ChannelMap`, the Matrix ``RoomBridgeStore`` pattern), so a new
channel is a k-board row, not a redeploy.

``FLEET_VOICE_TEE_CHANNEL_ID`` is RETIRED, but not merely warned about: on a host
whose map is empty (the live posture — ``/api/channel-map`` is not deployed and
agent units do not set ``FLEET_CHANNEL_MAP``) a warn-only retirement would take
the tee from "mirrors to one room" to "mirrors to NO room" at merge. It is
therefore honoured as a single home-channel portal while the map is empty. See
:meth:`VoiceTeeConfig.from_env`.

An unmapped channel SKIPS the mirror (loudly). It never falls back to another
channel — mirroring a task's output into a room it does not belong to is worse
than not mirroring it.

Design contract (issue #55):

- Off by default: a missing/false ``FLEET_VOICE_TEE_ENABLED`` means no Discord
  call is ever made.
- The tee copy is the *visible* reply — the ``visible_text`` field of the typed
  turn, which carries no control signals to strip (completion/stance are typed
  envelope fields, not text; fleet-bus #205).
- The tee is best-effort and MUST NEVER break the bus path: every failure is a
  loud ``WARNING`` and swallowed. The caller publishes on the bus (and emits the
  done sentinel) regardless of the tee's outcome.
- Sanitized logging only: entry/exit/failure name the channel and outcome, never
  the reply prose.

TLDR channel/thread split (fleet-bus #160)
------------------------------------------

The teed copy previously did a single raw ``adapter.send`` — a long reply then
got hacked into a 5-part wall by Discord's native >2000-char chunker. The guard's
"channel = headline, thread = substance" rule (the agent runtime) never ran on
the tee copy because that rule lives in the gateway's ``transform_llm_output``
hook chain, which a subscriber-side ``adapter.send`` bypasses.

The gateway's TLDR split has two halves (verified against source, not docs):

- a **pure** decision — ``rtech_hermes_runtime.tldr_split.split_for_tldr(text,
  limit)`` -> a ``SplitDecision`` (``should_split``, ``headline``, ``remainder``,
  ``thread_name``). No I/O, no gateway/session state.
- a **spill** — ``adapter.create_handoff_thread(parent, name)`` then
  ``adapter.send(chat_id, remainder, metadata={"thread_id": id})``. The gateway
  plugin schedules this onto its loop via a stash armed at ``pre_gateway_dispatch``
  ingress — machinery the tee path never traverses.

The tee does NOT need that stash. It already holds the live Discord ``adapter``
and it is already ``await``-ed on the gateway loop (it is called from an async
``adapter.send``), so it can call the *same* pure decision and the *same*
create-then-post spill pair directly. We reuse ``split_for_tldr`` (the existing
wheel) and replicate the tiny spill I/O the guard's ``_tldr_spill_coro`` uses.

What we deliberately do NOT do: leak any completion/stance signal onto the teed
copy. Those are typed envelope fields on the NATS-wire path (``fleet_core.spec``
``Done`` / ``Stance`` via ``typed_signal``); the *visible* reply handed to this
module is the plain ``visible_text``, carrying no control tokens by design. The
wire path owns completion, not the tee.

Fail-open throughout: if the split module is unavailable (off-host, or an import
error) or the split is disabled/under-limit, the tee falls back to the single
in-channel send. A thread-spill failure posts only a compact parent-channel
notice; it never dumps the full remainder wall into the parent channel. The
original content remains at its source. Nothing here raises into the bus path.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any

from fleet_nats import ChannelMap, ChannelPortal
from loguru import logger

_ENV_ENABLED = "FLEET_VOICE_TEE_ENABLED"
# RETIRED by fleet-bus #118 — read only to WARN that a deploy still sets it. The
# tee's target now comes from the persisted channel map, per turn.
_ENV_CHANNEL_ID = "FLEET_VOICE_TEE_CHANNEL_ID"

# The lane tokens the bridge NEVER forwards to Discord (fleet-bus #116).
#
# These are the needed-but-noisy classes — acks, tool echo, and the hourglass /
# floppy-disk progress markers. They have their own subjects precisely so this
# decision is structural: the bridge reads the LANE out of the subject and
# refuses, instead of pattern-matching prose (which eventually misclassifies a
# real reply as noise, or worse, the reverse).
#
# Belt-and-braces, and deliberately so. Nothing in the subscriber currently tees
# a noise envelope — ``adapter.send`` routes noise to ``_publish_noise`` and
# returns before reaching the tee. This guard is the SECOND barrier so that a
# future caller wiring a bridge onto a wildcard subscription cannot leak noise
# into a human channel just by forgetting the plane rule.
_NEVER_TEED_LANES = frozenset({"ack", "telemetry"})
_THREAD_FALLBACK_NOTICE = (
    "Full response could not be mirrored to a Discord detail thread. "
    "See the source conversation for the original content."
)

# Truthy spellings for the enable flag — mirror the fleet convention (a typo like
# "yes"/"1"/"on" reads as on; anything else, including unset, reads as off).
_TRUE_VALUES = frozenset({"1", "true", "yes", "on"})


@dataclass(frozen=True)
class VoiceTeeConfig:
    """Resolved voice-tee configuration.

    Attributes:
        enabled: Whether the tee is switched on (``FLEET_VOICE_TEE_ENABLED``).
        channels: The persisted slug ↔ Discord-id portal map (fleet-bus #118).
            This is what makes the tee per-channel: the turn's OWN channel slug
            picks the target room. Empty when no map is reachable.
    """

    enabled: bool
    channels: ChannelMap

    @property
    def active(self) -> bool:
        """Whether the tee may fire at all — enabled AND at least one known portal.

        Deliberately NOT "a channel id is configured": the target is now resolved
        per turn from the envelope's channel (:meth:`target_for`), so the only
        process-wide precondition is that some portal exists to resolve against.
        """
        return self.enabled and len(self.channels) > 0

    def target_for(self, channel: str | None) -> str | None:
        """The Discord channel id this turn's reply mirrors to, or ``None`` to skip.

        The #118 fix in one method. Previously every reply from this process went
        to one env-configured id, so a task coordinated in ``fleet-ops`` posted
        into ``arena``. Now the turn's own channel (``Envelope.channel``, carried
        through :class:`~fleet_subscriber.mapping.SyntheticMessage` and back into
        ``send``) is resolved through the persisted portal map.

        ``channel`` may already be a Discord id (a genuine Discord-origin turn), so
        resolution goes through :meth:`~fleet_nats.ChannelMap.resolve_discord_id`,
        which accepts EITHER spelling. That is deliberately the same
        :meth:`~fleet_nats.ChannelMap.portal_for` lookup the inbound leg's
        ``resolve_slug`` uses: this method previously searched the id index first
        while ``resolve_slug`` searched the slug index first, so a numeric slug that
        equalled another portal's Discord id sent the two legs to two DIFFERENT
        rooms. One precedence rule, one table, one answer.

        Returns ``None`` for an unmapped channel. The caller SKIPS the mirror; it
        must never fall back to "some other channel" — posting a task's output
        into a room it does not belong to is the exact defect #118 removes.

        Args:
            channel: The turn's channel — a coordination slug or a Discord id.

        Returns:
            The Discord channel id to post to, or ``None`` when unmapped.
        """
        return self.channels.resolve_discord_id(channel)

    @classmethod
    def from_env(cls, channels: ChannelMap, *, home_channel: str | None = None) -> VoiceTeeConfig:
        """Read the tee's on/off flag; take the portal map from the caller.

        Fail-safe by design: the tee is an opt-in mirror, so a missing or junk
        flag reads as disabled rather than raising — a misconfigured tee must
        never take down the bus path at connect/send time.

        The map is a PARAMETER, not another env read, because loading it is an
        async one-shot the adapter already performs at connect (alongside the
        subject catalog) — a per-process fetch, never a hot-path call.

        Legacy migration bridge (``FLEET_VOICE_TEE_CHANNEL_ID``)
        -------------------------------------------------------

        #118 changes the tee's process-wide precondition from "an env id is set"
        to "the map has at least one portal". On the live fleet TODAY those are
        not the same condition: agent hosts set ``FLEET_SUBJECT_REGISTRY_URL`` and
        ``FLEET_VOICE_TEE_CHANNEL_ID`` but NOT ``FLEET_CHANNEL_MAP``, and
        ``/api/channel-map`` still 404s, so the load ladder lands on the empty map
        and a purely warn-only retirement would take the tee from "mirrors to one
        room" to "mirrors to NO room" the moment this merges — a regression
        delivered by an atomic code cutover that requires a manual env edit on
        every host to undo.

        So the legacy id is honoured as a ONE-PORTAL BRIDGE, and only when there is
        nothing better: THIS AGENT'S HOME CHANNEL must have no portal of its own
        AND a home channel must be known, in which case the legacy id is APPENDED
        as that home channel's portal. It is loud (``WARNING``) and strictly
        transitional — it can never resurrect the #118 defect, because it maps
        exactly one extra slug (this agent's own home channel). Every other
        unmapped channel still resolves to ``None`` and is skipped rather than
        mis-routed into the legacy room. Once the home channel has a real row the
        bridge is dead.

        The retire test is deliberately "does *home* have a portal", not "is the
        map non-empty". A rolling seed that adds an unrelated row would satisfy
        the latter while leaving ``home`` unmapped, silently taking a working
        mirror dark because some other channel was added. For the same reason the
        bridge appends to the loaded portals instead of replacing them.

        Args:
            channels: The loaded portal map (:func:`fleet_nats.load_channel_map`).
            home_channel: This agent's own channel slug, used only to anchor the
                legacy bridge portal. ``None`` disables the bridge.
        """
        raw_enabled = os.environ.get(_ENV_ENABLED, "").strip().lower()
        enabled = raw_enabled in _TRUE_VALUES
        legacy_channel = os.environ.get(_ENV_CHANNEL_ID, "").strip()
        if not legacy_channel:
            return cls(enabled=enabled, channels=channels)

        home = (home_channel or "").strip()
        # Retire the legacy id only when THIS agent's home channel actually has a
        # portal — not merely when the map is non-empty. A rolling seed that adds
        # an unrelated row (FLEET_CHANNEL_MAP=arena=111) would otherwise discard
        # the bridge while `home` still has no portal, so target_for(home) starts
        # returning None and a previously-mirrored channel silently goes dark
        # because some OTHER channel was added.
        home_has_portal = bool(home) and channels.resolve_discord_id(home) is not None
        if home_has_portal or not home:
            # A real portal for this agent's own channel wins outright; with no
            # home channel there is nothing to anchor the bridge to, so the id is
            # simply retired.
            logger.warning(
                "fleet_subscriber.voice_tee | {} is set but RETIRED (fleet-bus #118): "
                "the tee now resolves each turn's channel through the persisted "
                "channel map ({} portals). Remove it from the deploy env.",
                _ENV_CHANNEL_ID,
                len(channels),
            )
            return cls(enabled=enabled, channels=channels)

        logger.warning(
            "fleet_subscriber.voice_tee | {} is RETIRED (fleet-bus #118) but NO channel "
            "map is loaded — bridging it as a single portal {}={} so the mirror keeps "
            "working. Only this agent's home channel mirrors; every other channel is "
            "skipped. Deploy the k-board channel-map table or set FLEET_CHANNEL_MAP, "
            "then remove {}.",
            _ENV_CHANNEL_ID,
            home,
            legacy_channel,
            _ENV_CHANNEL_ID,
        )
        # APPEND the home portal to whatever was loaded; do not replace the map.
        # During a rolling seed the map may already carry unrelated rows (e.g.
        # 'arena') while this agent's own channel has none. Replacing would drop
        # those rows and un-mirror a channel that was working a moment ago.
        bridged = ChannelMap(
            portals=(*channels.portals, ChannelPortal(slug=home, discord_id=legacy_channel)),
            source="legacy-env",
        )
        return cls(enabled=enabled, channels=bridged)


def lane_of(subject: str) -> str | None:
    """The agent-lane token in ``subject``, or ``None`` if it names no agent lane.

    Both agent subject shapes put the lane in a fixed position relative to the
    ``agent`` layer token (``fleet_nats.subjects._agent_lane``)::

        {env}.agent.{lane}.{tail}              -> lane at index 2
        {env}.agent.{channel}.{lane}.{tail}    -> lane at index 3

    The two are told apart by TOKEN COUNT, exactly as NATS itself matches, so a
    channel whose name collides with a lane word cannot fool the reader.

    Args:
        subject: A fully built subject string.

    Returns:
        The lane token, or ``None`` for a non-agent subject or an unrecognised
        shape (the caller treats ``None`` as "not a known noise lane").
    """
    tokens = subject.split(".")
    if len(tokens) < 4 or tokens[1] != "agent":
        return None
    if len(tokens) == 4:
        return tokens[2]
    if len(tokens) == 5:
        return tokens[3]
    return None


def is_forwardable(subject: str) -> bool:
    """Whether a bridge may forward ``subject``'s traffic to a human channel.

    ``False`` for the noise lanes (#116) — acks and telemetry are app-plane and
    must reach zero Discord channels. ``True`` for everything else, including a
    subject this function cannot parse: the bridge's existing plane discipline
    already governs those, and failing OPEN here keeps an unparseable subject
    behaving exactly as it did before this guard existed rather than silently
    muting a real reply.
    """
    return lane_of(subject) not in _NEVER_TEED_LANES


def _resolve_split_decision(text: str) -> Any | None:
    """The TLDR split decision for ``text``, or ``None`` to send whole. Fail open.

    Reuses the gateway guard's *pure* split wheel
    (``rtech_hermes_runtime.tldr_split.split_for_tldr``) with the same env-driven
    config (``RTECH_TLDR_SPLIT_ENABLED`` / ``RTECH_TLDR_LIMIT``) so the teed copy
    obeys the identical "channel = headline, thread = substance" contract the
    genuine ``platform=discord`` turns already do.

    The module lives in the Hermes host venv the subscriber is installed into (not
    a pip dep here — see ``pyproject.toml``), so the import is deferred and
    guarded: off-host / import error / disabled / under-limit all return ``None``
    and the caller sends the whole reply in-channel unchanged.

    Returns:
        A ``SplitDecision`` with ``should_split`` True, or ``None`` (send whole).
    """
    try:
        from rtech_hermes_runtime.tldr_split import (  # type: ignore[import-not-found]
            TldrSplitConfig,
            split_for_tldr,
        )

        cfg = TldrSplitConfig.from_env()
        if not cfg.enabled:
            return None
        # ``text`` is statically ``str`` (the tee's own visible reply), so no
        # isinstance guard is needed here — a redundant one is dead code Pyright
        # flags as unreachable. ``split_for_tldr`` also handles empty/short text.
        decision = split_for_tldr(text, cfg.limit)
        return decision if getattr(decision, "should_split", False) else None
    except Exception:  # noqa: BLE001 — split is best-effort; never break the tee
        # Off-host the module simply is not importable; on-host a config/split
        # error must not take down the mirror. Fall back to a whole in-channel send.
        logger.opt(exception=True).debug(
            "fleet_subscriber.voice_tee | tldr split unavailable — sending whole"
        )
        return None


async def _spill_remainder_to_thread(
    *,
    adapter: Any,
    channel_id: str,
    thread_name: str,
    remainder: str,
) -> bool:
    """Open a detail thread off ``channel_id`` and post ``remainder`` into it.

    Mirrors the gateway guard's ``_tldr_spill_coro`` create-then-post pair: use
    the adapter's contract-level ``create_handoff_thread`` to open a thread, then
    ``send(..., metadata={"thread_id": id})`` to post the detail there. If the
    adapter cannot thread (no ``create_handoff_thread``, or it fails/returns
    falsy), the parent channel gets only a compact failure notice.

    The remainder is the *substance* of the reply. If it cannot land in the
    thread, do not dump it back into the parent channel as a >2000-char wall.
    EVERY failure mode of the thread spill funnels to ONE terminal compact notice
    (fail-loud WARNING/ERROR):

    - ``create_handoff_thread`` raises or returns falsy → notice.
    - the threaded ``send`` returns ``SendResult.success is False`` (Discord's
      create-then-post cache race returns a falsy result WITHOUT raising) → notice.
    - the threaded ``send`` RAISES → caught here (not left to the caller, which
      would swallow it AFTER the headline already landed) → notice.

    Returns:
        True only when the remainder was delivered to the thread. False when the
        thread spill failed, regardless of whether the compact parent notice was
        delivered. Never raises out; the caller's ``tee_to_discord`` contract
        keeps the bus path untouched regardless. Sanitized logging only
        (channel/thread ids, lengths, never the prose).
    """
    thread_id: str | None = None
    try:
        create = getattr(adapter, "create_handoff_thread", None)
        if callable(create):
            # ``adapter`` is Any (a duck-typed platform adapter resolved at
            # runtime), so ``create`` is an untyped callable; Pyright cannot prove
            # its result is awaitable. It is — the contract is
            # ``async def create_handoff_thread(...) -> str | None`` (discord
            # adapter.py:4602). Narrow the awaited value to ``object`` for the guard.
            raw: object = await create(channel_id, thread_name)  # pyright: ignore[reportGeneralTypeIssues]
            thread_id = str(raw) if raw else None
    except Exception as exc:  # noqa: BLE001 — thread creation is best-effort
        logger.warning(
            "fleet_subscriber.voice_tee | create_handoff_thread failed channel={}: {}",
            channel_id,
            type(exc).__name__,
        )
        thread_id = None

    # Try the thread spill as ONE guarded unit: create + threaded-send. A success
    # returns; ANY failure (raise, or success=False) falls through to the single
    # terminal compact notice below. There is no third parallel branch.
    if thread_id:
        try:
            result = await adapter.send(
                chat_id=channel_id, content=remainder, metadata={"thread_id": thread_id}
            )
            if bool(getattr(result, "success", False)):
                logger.info(
                    "fleet_subscriber.voice_tee | remainder spilled to thread channel={} thread={}",
                    channel_id,
                    thread_id,
                )
                return True
            # Returned failure (no raise): the cache race. Fall through to notice.
            logger.warning(
                "fleet_subscriber.voice_tee | thread remainder send unsuccessful — "
                "posting compact parent notice channel={} thread={} remainder_len={}",
                channel_id,
                thread_id,
                len(remainder),
            )
        except Exception as exc:  # noqa: BLE001 — a RAISE must fall back, not escape
            # If this escaped, the caller would swallow it after the headline
            # already posted. Catch it HERE so it reaches the same terminal notice
            # as the success=False path.
            logger.warning(
                "fleet_subscriber.voice_tee | thread remainder send raised — "
                "posting compact parent notice channel={} thread={} remainder_len={}: {}",
                channel_id,
                thread_id,
                len(remainder),
                type(exc).__name__,
            )
    else:
        # No thread available. The headline already went out; do not dump the
        # full detail back into the parent channel as a wall.
        logger.warning(
            "fleet_subscriber.voice_tee | no detail thread — posting compact parent "
            "notice channel={} remainder_len={}",
            channel_id,
            len(remainder),
        )

    await _send_thread_failure_notice(
        adapter=adapter,
        channel_id=channel_id,
        remainder_len=len(remainder),
    )
    return False


async def _send_thread_failure_notice(
    *,
    adapter: Any,
    channel_id: str,
    remainder_len: int,
) -> bool:
    """Post the compact parent-channel notice and inspect its delivery result."""
    try:
        result = await adapter.send(chat_id=channel_id, content=_THREAD_FALLBACK_NOTICE)
    except Exception as exc:  # noqa: BLE001 — notice fallback is best-effort
        logger.error(
            "fleet_subscriber.voice_tee | thread failure parent notice send failed "
            "channel={} error={} remainder_len={}",
            channel_id,
            type(exc).__name__,
            remainder_len,
        )
        return False
    if not bool(getattr(result, "success", False)):
        logger.error(
            "fleet_subscriber.voice_tee | thread failure parent notice send unsuccessful "
            "channel={} error={} remainder_len={}",
            channel_id,
            getattr(result, "error", None),
            remainder_len,
        )
        return False
    return True


def _resolve_discord_adapter(platform_factory: Any) -> Any | None:
    """Find the live in-process Discord adapter, or ``None`` if unreachable.

    Uses the sanctioned in-process seam (``gateway.run._gateway_runner_ref``) —
    the same one ``tools/send_message_tool.py`` uses for plugin-platform
    delivery. Off-host / out-of-process (cron, tests without a runner) the
    weakref returns ``None`` and there is simply no adapter to tee through.

    Args:
        platform_factory: The gateway ``Platform`` enum factory (``api.Platform``)
            — called as ``platform_factory("discord")`` to name the target.

    Returns:
        The Discord adapter instance, or ``None`` when no live gateway/Discord
        adapter is present.
    """
    try:
        from gateway.run import _gateway_runner_ref  # type: ignore[import-not-found]

        runner = _gateway_runner_ref()
    except Exception:  # noqa: BLE001 — off-host import/lookup: no live runner
        return None
    if runner is None:
        return None
    try:
        return runner.adapters.get(platform_factory("discord"))
    except Exception:  # noqa: BLE001 — a malformed runner must not break the tee
        return None


async def tee_to_discord(
    *,
    config: VoiceTeeConfig,
    platform_factory: Any,
    channel_id: str,
    text: str,
    source_subject: str | None = None,
) -> None:
    """Best-effort mirror of a visible reply to a Discord channel.

    NEVER raises: any failure (no live gateway, no Discord adapter, send error)
    is logged at ``WARNING`` and swallowed so the bus path is untouched. The
    caller has already stripped the completion trailer; ``text`` is the visible
    reply verbatim.

    Args:
        config: The resolved tee config (only used for its ``enabled`` flag in
            the log line; the caller has already gated on ``config.active``).
        platform_factory: The gateway ``Platform`` enum factory (``api.Platform``).
        channel_id: The Discord channel id to post to.
        text: The visible reply text (trailer already stripped).
        source_subject: The bus subject this reply rides, when known. A noise
            lane (#116) is refused outright — ZERO ack/telemetry traffic may land
            in a human channel. ``None`` (a caller with no subject to declare)
            keeps the pre-#116 behavior and forwards.
    """
    if source_subject is not None and not is_forwardable(source_subject):
        # Structural refusal, by subject, before any Discord call is made.
        logger.info(
            "fleet_subscriber.voice_tee | noise lane not forwarded subject={} channel={}",
            source_subject,
            channel_id,
        )
        return
    logger.info(
        "fleet_subscriber.voice_tee | teeing reply channel={} enabled={}",
        channel_id,
        config.enabled,
    )
    try:
        adapter = _resolve_discord_adapter(platform_factory)
        if adapter is None:
            # Not an error: off-host, or Discord simply isn't connected on this
            # gateway. Loud enough to diagnose a misconfigured tee, never fatal.
            logger.warning(
                "fleet_subscriber.voice_tee | no live Discord adapter — reply not mirrored "
                "channel={}",
                channel_id,
            )
            return
        # "channel = headline, thread = substance" (#160): a long reply becomes a
        # short in-channel headline plus a detail thread instead of a raw 5-part
        # wall. The decision is the guard's pure wheel; fail-open to a whole send.
        decision = _resolve_split_decision(text)
        headline = getattr(decision, "headline", text) if decision is not None else text

        result = await adapter.send(chat_id=channel_id, content=headline)
        success = bool(getattr(result, "success", False))
        if not success:
            logger.warning(
                "fleet_subscriber.voice_tee | Discord adapter send unsuccessful channel={}",
                channel_id,
            )
            return

        if decision is not None:
            # The headline is in-channel; spill the substance to its detail thread.
            # A spill failure must NOT unwind the mirror — the headline already
            # posted successfully — so isolate it in its own guard.
            try:
                remainder_delivered = await _spill_remainder_to_thread(
                    adapter=adapter,
                    channel_id=channel_id,
                    thread_name=decision.thread_name,
                    remainder=decision.remainder,
                )
            except Exception as exc:  # noqa: BLE001 — spill is best-effort
                logger.warning(
                    "fleet_subscriber.voice_tee | remainder spill failed channel={}: {}",
                    channel_id,
                    type(exc).__name__,
                )
                remainder_delivered = False
            if remainder_delivered:
                logger.info("fleet_subscriber.voice_tee | mirrored (split) channel={}", channel_id)
            else:
                logger.warning(
                    "fleet_subscriber.voice_tee | split mirror incomplete channel={}",
                    channel_id,
                )
            return

        logger.info("fleet_subscriber.voice_tee | mirrored channel={}", channel_id)
    except Exception as exc:  # noqa: BLE001 — tee must NEVER break the bus path
        # Class only + channel: an adapter send error can embed the reply text in
        # str(exc); never log prose (docs/logging.md never-log list).
        logger.warning(
            "fleet_subscriber.voice_tee | tee failed channel={}: {}",
            channel_id,
            type(exc).__name__,
        )
