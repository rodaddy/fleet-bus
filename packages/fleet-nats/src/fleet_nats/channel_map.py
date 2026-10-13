"""The channel portal map — slug ↔ Discord id, persisted in k-board (fleet-bus #118).

Both bridge legs used to resolve "where does this land in Discord?" from a single
env var, and the two legs did not even agree on which one:

- **outbound** (:mod:`fleet_subscriber.voice_tee`) mirrored EVERY reply, for every
  task, to the one id in ``FLEET_VOICE_TEE_CHANNEL_ID`` — the envelope's own
  ``channel`` was ignored entirely, so two tasks in two different channels both
  posted into the same room.
- **inbound** (:mod:`fleet_orchestrator.intake_poller`) stamped the one slug in
  ``FLEET_INTAKE_CHANNEL`` onto every card it opened.

A single id per process cannot express "this task belongs to that channel", so
adding a channel meant an env change plus a redeploy per host — and adding a
*second* channel to one host was not expressible at all. That is the constraint
#129 (one task = one visible working thread) has to break before it can put a
task's thread in the task's OWN channel.

The model is Matrix's ``RoomBridgeStore`` **portal** pattern: a bridge does not
hardcode a room, it keeps a persisted table of portals (remote room ↔ matrix
room) and looks the pairing up per message. Here a portal is
``(slug, discord_id)``:

    arena       ↔ 1234567890123456789
    fleet-ops   ↔ 9876543210987654321

The **slug is the coordination-side name** (it appears in ``Envelope.channel``,
in subjects via :func:`fleet_nats.subjects.slug`, and on k-board cards); the
**Discord id is the display-side address** (what an adapter's ``send`` needs).
Neither leg should know the other side's spelling, so both call this map.

Authority and failure posture mirror the ratified subject registry
(:mod:`fleet_nats.registry`, ``AUTHORITY = "kboard"``) deliberately — a second
persisted-map module with a *different* failure posture would be a trap:

- k-board Postgres is authoritative. The table is fetched ONCE at connect and
  cached; resolution is a dict lookup, never a hot-path HTTP call.
- Any failure to reach it — unset URL, transport error, 404 not-yet-deployed,
  malformed payload — falls back LOUDLY to an EMPTY map, never to a stale
  guess. An empty map is honest ("I know of no portals"), and each leg then
  applies its own documented degrade (see :class:`ChannelMap` below).
- The registry's URL env var is reused (``FLEET_SUBJECT_REGISTRY_URL``): both
  tables live in the same k-board, and a second URL knob would be one more thing
  to get out of sync across three hosts for no gain.

**Adding a channel requires no code change** (the #118 acceptance criterion): a
row in the k-board table is enough. The seed/bootstrap posture for a fleet that
has not deployed the table yet is :envvar:`FLEET_CHANNEL_MAP` — a static
``slug=id,slug=id`` env — which is deliberately NOT a per-process default
channel: it is the same many-portal map, just supplied statically, so a host can
carry N channels and the read API is identical either way.
"""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass, field

import httpx
from loguru import logger

from fleet_nats.registry import _safe_exc, _safe_registry_url
from fleet_nats.subjects import STAR
from fleet_nats.tracing import traced

# Characters that must never appear in a slug, because a slug becomes a NATS
# subject TOKEN on the dispatch path (``{env}.agent.{channel}.dispatch.{worker}``
# via :func:`fleet_nats.subjects.slug`). ``subjects.slug`` rejects only ``>``; it
# deliberately permits ``*`` so callers can build authorization grant patterns
# from :data:`~fleet_nats.subjects.STAR`. A *portal* is never a grant pattern, so
# the map is the right place to refuse both wildcards outright — otherwise an
# operator-typed (or k-board-sourced) row of ``*`` publishes onto
# ``{env}.agent.*.dispatch.{worker}``, which the manager's own publish grant
# ACCEPTS and which nats-py never validates client-side, so every dispatch for
# that card vanishes into a wildcard subject with no error anywhere.
# NOTE: whitespace is checked with ``str.isspace()``, NOT by listing " " here.
# A literal-space entry catches only U+0020 and lets a tab, newline, or carriage
# return through — and NATS treats those as PUB-command separators, so a slug of
# ``arena\tops`` yields a malformed PUB that nats-py queues without client-side
# validation and the dispatch is lost asynchronously with no error anywhere.
_FORBIDDEN_SLUG_CHARS = (STAR, ">", ".")

# The k-board table endpoint. Same base URL as the subject registry (one k-board,
# one URL knob) — see the module docstring for why a second knob is a liability.
_ENV_URL = "FLEET_SUBJECT_REGISTRY_URL"
_ENV_STATIC = "FLEET_CHANNEL_MAP"
_API_PATH = "/api/channel-map"
_TENANT = "pg"
_DEFAULT_TIMEOUT_S = 5.0


class ChannelMapError(ValueError):
    """A channel-map payload/spec was malformed (the caller decides to fall back)."""


@dataclass(frozen=True)
class ChannelPortal:
    """One persisted portal: a coordination slug paired with a Discord channel id.

    Attributes:
        slug: The coordination-side channel name (``Envelope.channel``, k-board
            card channel, subject token). Lowercase by convention; matching is
            case-insensitive so a card typed ``Arena`` still resolves.
        discord_id: The display-side Discord channel id (the snowflake an
            adapter's ``send`` addresses). Kept as ``str``: Discord snowflakes
            exceed 53-bit float precision and are strings everywhere on the wire.
    """

    slug: str
    discord_id: str


@dataclass(frozen=True)
class ChannelMap:
    """A bidirectional slug ↔ Discord-id portal map. Immutable; safe to cache.

    This is the read API BOTH bridge legs resolve through, and the one #129
    builds on, so it answers the question in whichever direction the caller has:

    - outbound leg holds a slug (from ``Envelope.channel``) and needs an id →
      :meth:`discord_id_for`.
    - inbound leg holds a Discord id (from a real message/card) and needs the
      coordination slug to stamp → :meth:`slug_for`.

    Both resolvers return ``None`` rather than a fallback. A map that quietly
    substituted "some other channel" would recreate exactly the #118 defect it
    exists to remove — posting a task's output into a room it does not belong to
    is worse than not posting it. Each leg logs and degrades on ``None``.

    Attributes:
        portals: The portals in this map, in load order.
        source: ``"kboard"`` (fetched), ``"env"`` (static bootstrap), or
            ``"empty"`` (nothing loaded — a loud fallback, not a silent default).
    """

    portals: tuple[ChannelPortal, ...]
    source: str
    # Derived lookup indexes, built once at construction. Case-folded keys so a
    # card titled with "Arena" resolves the "arena" portal.
    _by_slug: Mapping[str, ChannelPortal] = field(init=False, repr=False, compare=False)
    _by_id: Mapping[str, ChannelPortal] = field(init=False, repr=False, compare=False)

    def __post_init__(self) -> None:
        """Build the two lookup indexes (frozen dataclass → ``object.__setattr__``)."""
        by_slug: dict[str, ChannelPortal] = {}
        by_id: dict[str, ChannelPortal] = {}
        for portal in self.portals:
            by_slug[portal.slug.casefold()] = portal
            by_id[portal.discord_id] = portal
        object.__setattr__(self, "_by_slug", by_slug)
        object.__setattr__(self, "_by_id", by_id)

    def __len__(self) -> int:
        """The number of portals — ``if not channel_map:`` reads as "no portals"."""
        return len(self.portals)

    @property
    def slugs(self) -> tuple[str, ...]:
        """Every known slug, in load order (diagnostics + operator log lines)."""
        return tuple(portal.slug for portal in self.portals)

    def discord_id_for(self, slug: str | None) -> str | None:
        """The Discord channel id for ``slug``, or ``None`` if this map has no portal.

        The OUTBOUND resolver (voice tee): the envelope names a coordination slug;
        Discord needs the snowflake.

        Args:
            slug: The coordination-side channel slug (``None``/blank → ``None``).

        Returns:
            The Discord channel id, or ``None`` when the slug has no portal.
        """
        if not slug or not slug.strip():
            return None
        portal = self._by_slug.get(slug.strip().casefold())
        return portal.discord_id if portal is not None else None

    def slug_for(self, discord_id: str | None) -> str | None:
        """The coordination slug for a Discord channel id, or ``None`` if unmapped.

        The INBOUND resolver: a real Discord id (a card's channel, a message's
        origin) has to become the slug the coordination side stamps on envelopes
        and subjects.

        Args:
            discord_id: The Discord channel id (``None``/blank → ``None``).

        Returns:
            The coordination slug, or ``None`` when the id has no portal.
        """
        if not discord_id or not discord_id.strip():
            return None
        portal = self._by_id.get(discord_id.strip())
        return portal.slug if portal is not None else None

    def portal_for(self, channel: str | None) -> ChannelPortal | None:
        """The portal ``channel`` names, whether it is spelled as a slug or an id.

        THE single precedence rule. Every "which room is this?" answer in the fleet
        resolves here, so no two callers can disagree about one channel: the slug
        index is searched first, then the id index. ``parse_portals`` additionally
        refuses a map where those two namespaces overlap, so in a validated map the
        order cannot matter — this method is what makes that also true for a map
        built by hand.

        Args:
            channel: A slug or a Discord channel id.

        Returns:
            The matching portal, or ``None`` when nothing matches.
        """
        if not channel or not channel.strip():
            return None
        needle = channel.strip()
        return self._by_slug.get(needle.casefold()) or self._by_id.get(needle)

    def resolve_slug(self, channel: str | None) -> str | None:
        """Normalize ``channel`` — a slug OR a Discord id — to its canonical slug.

        The legs receive a "channel" from places that do not agree on which
        spelling they use (an env var, a k-board card field, an envelope from
        another fleet component). Accepting either and canonicalising through the
        SAME persisted table is what stops the two legs from drifting apart again.
        A slug already in the map canonicalises to its stored casing.

        Args:
            channel: A slug or a Discord channel id.

        Returns:
            The canonical slug, or ``None`` when ``channel`` matches no portal.
        """
        portal = self.portal_for(channel)
        return portal.slug if portal is not None else None

    def resolve_discord_id(self, channel: str | None) -> str | None:
        """The Discord id for ``channel``, whether it is spelled as a slug or an id.

        The outbound mirror's resolver. It shares :meth:`portal_for` with
        :meth:`resolve_slug` on purpose: the tee used to search the id index first
        while ``resolve_slug`` searched the slug index first, so a slug that
        equalled another portal's id sent the two legs to two different rooms.

        Args:
            channel: A slug or a Discord channel id.

        Returns:
            The Discord channel id, or ``None`` when nothing matches.
        """
        portal = self.portal_for(channel)
        return portal.discord_id if portal is not None else None


def empty_map() -> ChannelMap:
    """The empty map — the loud fallback when no persisted map is reachable."""
    return ChannelMap(portals=(), source="empty")


def is_subject_safe_slug(slug: str) -> bool:
    """Whether ``slug`` is safe to use as a NATS subject token.

    A channel slug does not stay data: the hub feeds it to
    :func:`fleet_nats.subjects.agent_dispatch` as a subject TOKEN. A token
    carrying a wildcard (``*`` or ``>``) or a delimiter (``.``) either widens the
    subject or splits it, and both failures are SILENT — nats-py does not
    validate a publish subject client-side and ``publish`` is fire-and-forget, so
    the server's rejection surfaces asynchronously on ``error_cb`` while the card
    has already been marked coordinated.

    ``subjects.slug`` cannot enforce this itself: it must keep accepting
    :data:`~fleet_nats.subjects.STAR` so callers can build authorization grant
    patterns. A portal is never a grant pattern, so the map enforces the stricter
    rule at the boundary where operator/k-board-controlled text enters.

    Args:
        slug: The candidate coordination slug (already stripped).

    Returns:
        True when the slug may safely become a subject token.
    """
    if not slug.strip():
        return False
    if any(ch.isspace() for ch in slug):
        return False
    return not any(bad in slug for bad in _FORBIDDEN_SLUG_CHARS)


def parse_portals(payload: object) -> ChannelMap:
    """Validate a k-board channel-map response strictly into a :class:`ChannelMap`.

    Strict on purpose, mirroring :func:`fleet_nats.registry._parse_catalog`: a
    malformed table is an ERROR, not a half-loaded map. Half a portal table is
    the worst outcome available — it routes some channels correctly and silently
    drops others, which is indistinguishable from "that channel has no portal".

    Accepts either the object form (the endpoint contract)::

        {"version": 3, "channels": [{"slug": "arena", "discord_id": "123"}]}

    or a bare list of the same portal objects, so the endpoint may return the
    rows directly (the tasks API already does exactly this — see
    ``KboardClient.list_tasks``).

    Args:
        payload: The decoded JSON body.

    Returns:
        The parsed map (``source="kboard"``).

    Raises:
        ChannelMapError: On any malformed payload/row, a duplicate slug, or a
            duplicate Discord id.
    """
    if isinstance(payload, dict):
        rows = payload.get("channels")
    elif isinstance(payload, list):
        rows = payload
    else:
        raise ChannelMapError(
            f"channel-map payload must be an object or list, got {type(payload).__name__}"
        )
    if not isinstance(rows, list):
        raise ChannelMapError("channel-map 'channels' must be a list")

    portals: list[ChannelPortal] = []
    seen_slugs: dict[str, str] = {}
    seen_ids: dict[str, str] = {}
    for row in rows:
        if not isinstance(row, dict):
            raise ChannelMapError(f"channel-map row must be an object, got {type(row).__name__}")
        slug = row.get("slug")
        discord_id = row.get("discord_id")
        if not isinstance(slug, str) or not slug.strip():
            raise ChannelMapError(f"channel-map row has an invalid slug: {slug!r}")
        # Coerce an int id to str: a snowflake that arrives as a JSON number has
        # already lost precision in a float decoder, so accept only an int (exact
        # in Python) and reject a float loudly rather than route to a wrong room.
        if isinstance(discord_id, bool) or not isinstance(discord_id, str | int):
            raise ChannelMapError(
                f"channel-map row {slug!r} has an invalid discord_id: {discord_id!r}"
            )
        discord_id = str(discord_id).strip()
        if not discord_id:
            raise ChannelMapError(f"channel-map row {slug!r} has an empty discord_id")
        slug = slug.strip()
        # The slug becomes a NATS subject token on the dispatch path, so a
        # wildcard/delimiter in it is subject injection, not a naming quirk. Refuse
        # the ROW rather than sanitising it: a silently rewritten slug would no
        # longer match the k-board row an operator is looking at.
        if not is_subject_safe_slug(slug):
            raise ChannelMapError(
                f"channel-map row has a slug that is not subject-safe: {slug!r} — a slug "
                f"becomes a NATS subject token and may not be blank, contain whitespace "
                f"(including tab/newline/carriage return), or contain any of "
                f"{''.join(_FORBIDDEN_SLUG_CHARS)!r}"
            )
        folded = slug.casefold()
        if folded in seen_slugs:
            raise ChannelMapError(
                f"channel-map has duplicate slug {slug!r} (already mapped to "
                f"{seen_slugs[folded]}) — a slug must name exactly one channel"
            )
        if discord_id in seen_ids:
            raise ChannelMapError(
                f"channel-map has duplicate discord_id {discord_id} (slugs "
                f"{seen_ids[discord_id]!r} and {slug!r}) — an id must name exactly one slug"
            )
        seen_slugs[folded] = discord_id
        seen_ids[discord_id] = slug
        portals.append(ChannelPortal(slug=slug, discord_id=discord_id))

    # Cross-namespace disjointness. Uniqueness WITHIN each namespace is not enough:
    # the two resolvers search the two indexes in opposite order
    # (:meth:`ChannelMap.resolve_slug` slug-first, the tee's ``target_for`` id-first),
    # so a slug that equals a DIFFERENT portal's discord_id makes the two legs
    # disagree about which room one channel names — the exact "post into a room it
    # does not belong to" failure this module exists to prevent. Both rows are
    # individually valid, so this has to be its own rule.
    for portal in portals:
        folded = portal.slug.casefold()
        colliding = seen_ids.get(portal.slug)
        if colliding is not None and colliding.casefold() != folded:
            raise ChannelMapError(
                f"channel-map slug {portal.slug!r} collides with the discord_id of slug "
                f"{colliding!r} — the slug and Discord-id namespaces must be disjoint or "
                f"the two bridge legs resolve one channel to two different rooms"
            )
    return ChannelMap(portals=tuple(portals), source="kboard")


def parse_static_spec(spec: str) -> ChannelMap:
    """Parse the ``FLEET_CHANNEL_MAP`` bootstrap spec ``slug=id,slug=id``.

    The bootstrap posture for a fleet whose k-board channel-map table is not
    deployed yet. It is explicitly NOT a revival of ``default_channel``: it holds
    MANY portals, so a host still carries every channel it bridges and the read
    API is byte-identical to the fetched path. Blank entries and surrounding
    whitespace are tolerated (an operator-typed env), but a malformed entry fails
    loud rather than silently dropping a portal.

    Args:
        spec: The raw env value, e.g. ``"arena=123,fleet-ops=456"``.

    Returns:
        The parsed map (``source="env"``); an empty spec yields an empty map.

    Raises:
        ChannelMapError: On an entry without exactly one ``=``, a blank side, or
            a duplicate slug/id.
    """
    rows: list[dict[str, str]] = []
    for entry in spec.split(","):
        item = entry.strip()
        if not item:
            continue
        # partition() splits on the FIRST '=' only, so 'a=123=456' would yield a
        # discord_id of '123=456' — accepted as an active portal and then sent to
        # Discord as an invalid target, silently killing that channel's mirror.
        # The docstring promises exactly one '='; enforce it rather than imply it.
        equals = item.count("=")
        if equals == 0:
            raise ChannelMapError(
                f"{_ENV_STATIC} entry {item!r} is not 'slug=discord_id' (missing '=')"
            )
        if equals > 1:
            raise ChannelMapError(
                f"{_ENV_STATIC} entry {item!r} is not 'slug=discord_id' "
                f"(expected exactly one '=', found {equals})"
            )
        slug, _sep, discord_id = item.partition("=")
        rows.append({"slug": slug.strip(), "discord_id": discord_id.strip()})
    parsed = parse_portals(rows)
    return ChannelMap(portals=parsed.portals, source="env")


@traced
async def fetch_channel_map(
    base_url: str,
    *,
    timeout_s: float = _DEFAULT_TIMEOUT_S,
    client: httpx.AsyncClient | None = None,
) -> ChannelMap:
    """Fetch the persisted channel map from k-board.

    Args:
        base_url: The k-board base URL (no trailing path).
        timeout_s: Request timeout for the one-shot client. An injected ``client``
            owns its own timeout config and this parameter is ignored on that path
            (same contract as :func:`fleet_nats.registry.fetch_catalog`).
        client: Injected client (tests); when ``None`` a one-shot client is used.

    Returns:
        The fetched, strictly-validated map (``source="kboard"``).

    Raises:
        httpx.HTTPError: On any transport/status failure.
        ChannelMapError: On a malformed payload.
    """
    url = f"{base_url.rstrip('/')}{_API_PATH}"
    if client is not None:
        resp = await client.get(url, params={"tenant": _TENANT})
        resp.raise_for_status()
        return parse_portals(resp.json())
    async with httpx.AsyncClient(timeout=timeout_s) as one_shot:
        resp = await one_shot.get(url, params={"tenant": _TENANT})
        resp.raise_for_status()
        return parse_portals(resp.json())


@traced
async def load_channel_map(*, client: httpx.AsyncClient | None = None) -> ChannelMap:
    """Load the active channel map ONCE (call at connect; cache the result).

    Never raises — a bridge leg must not fail to start because the portal table
    is unreachable. Resolution order:

    1. **k-board** when ``FLEET_SUBJECT_REGISTRY_URL`` is set. Authoritative.
       Any failure (transport, non-2xx, malformed) logs an ERROR and falls
       through to (2) — fail-open, never fail-closed on Postgres, exactly as the
       subject registry does.
    2. **``FLEET_CHANNEL_MAP``** static bootstrap spec, when set. A malformed
       spec logs an ERROR and falls through to (3) rather than taking the leg
       down.
    3. **The empty map**, logged loudly. Not a default channel: each leg's own
       documented degrade applies (the tee suppresses the mirror; intake stamps
       no channel).

    Args:
        client: Injected ``httpx.AsyncClient`` (tests); ``None`` uses a one-shot.

    Returns:
        The loaded map — ``source`` names which of the three rungs answered.
    """
    base_url = os.environ.get(_ENV_URL)
    if base_url:
        try:
            fetched = await fetch_channel_map(base_url, client=client)
        except Exception as exc:  # noqa: BLE001 — the fallback IS the contract
            # Sanitize BOTH url and exception: an httpx status error's str embeds
            # the full requested URL (any query token with it). Same rule as the
            # registry's fetch-failure log line.
            logger.error(
                "fleet_nats.channel_map | fetch failed url={} ({}) — falling back to "
                "{} / empty map",
                _safe_registry_url(base_url),
                _safe_exc(exc),
                _ENV_STATIC,
            )
        else:
            logger.info(
                "fleet_nats.channel_map | loaded source=kboard portals={} slugs={}",
                len(fetched),
                ",".join(fetched.slugs),
            )
            return fetched
    else:
        logger.debug("fleet_nats.channel_map | {} unset — trying {}", _ENV_URL, _ENV_STATIC)

    static_spec = os.environ.get(_ENV_STATIC, "").strip()
    if static_spec:
        try:
            parsed = parse_static_spec(static_spec)
        except ChannelMapError as exc:
            # Never echo the spec itself in full — it is operator-typed config, and
            # the exception already names the offending entry.
            logger.error(
                "fleet_nats.channel_map | {} is malformed ({}) — falling back to the "
                "EMPTY map; no channel resolves until it is fixed",
                _ENV_STATIC,
                exc,
            )
        else:
            logger.info(
                "fleet_nats.channel_map | loaded source=env portals={} slugs={}",
                len(parsed),
                ",".join(parsed.slugs),
            )
            return parsed

    logger.warning(
        "fleet_nats.channel_map | NO channel map available (registry url and {} both "
        "unusable) — every channel resolution will return None",
        _ENV_STATIC,
    )
    return empty_map()
