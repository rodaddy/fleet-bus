"""Channel portal map tests (fleet-bus #118): parse, resolve both ways, fail open.

The map replaces a per-process ``default_channel``, so the tests that matter are
the ones a single-default implementation would FAIL:

- a map holds MANY portals and resolves each to its own id (a default cannot);
- resolution works in BOTH directions (slug→id and id→slug);
- a slug the map does not know returns ``None`` rather than some other channel;
- a new slug is added by DATA (a k-board row / an env entry), never by code.
"""

from __future__ import annotations

import json

import httpx
import pytest

from fleet_nats import (
    ChannelMap,
    ChannelMapError,
    ChannelPortal,
    empty_map,
    fetch_channel_map,
    is_subject_safe_slug,
    load_channel_map,
    parse_portals,
    parse_static_spec,
)

_ENV_URL = "FLEET_SUBJECT_REGISTRY_URL"
_ENV_STATIC = "FLEET_CHANNEL_MAP"

_ROWS = [
    {"slug": "arena", "discord_id": "1111111111111111111"},
    {"slug": "fleet-ops", "discord_id": "2222222222222222222"},
]


def _transport(payload: object, status: int = 200) -> httpx.AsyncClient:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(status, content=json.dumps(payload), headers={"x-t": "1"})

    return httpx.AsyncClient(transport=httpx.MockTransport(handler))


# ---- the core defect: many channels, not one default -------------------------


def test_map_resolves_each_slug_to_its_own_channel() -> None:
    """Two slugs resolve to two DIFFERENT ids — the whole point of #118.

    A single ``default_channel`` returns one id for every input, so it fails this
    outright: it cannot express "arena is over there, fleet-ops is over here".
    """
    channels = parse_portals(_ROWS)
    assert channels.discord_id_for("arena") == "1111111111111111111"
    assert channels.discord_id_for("fleet-ops") == "2222222222222222222"
    assert channels.discord_id_for("arena") != channels.discord_id_for("fleet-ops")


def test_map_resolves_id_back_to_slug() -> None:
    """The inbound direction: a Discord id canonicalises to its coordination slug."""
    channels = parse_portals(_ROWS)
    assert channels.slug_for("2222222222222222222") == "fleet-ops"
    assert channels.slug_for("1111111111111111111") == "arena"


def test_unmapped_channel_resolves_to_none_not_a_fallback() -> None:
    """An unknown slug is ``None`` — never "some other channel".

    Substituting a fallback would recreate the exact defect #118 removes: posting
    a task's output into a room it does not belong to.
    """
    channels = parse_portals(_ROWS)
    assert channels.discord_id_for("does-not-exist") is None
    assert channels.slug_for("9999999999999999999") is None
    assert channels.discord_id_for(None) is None
    assert channels.discord_id_for("   ") is None
    assert channels.slug_for(None) is None


def test_new_slug_needs_only_data_no_code_change() -> None:
    """The acceptance criterion: adding a channel is a ROW, not a code edit.

    The same ``parse_portals`` call — unchanged — resolves a slug it has never
    seen, purely because the payload grew one entry.
    """
    before = parse_portals(_ROWS)
    assert before.discord_id_for("skunkworks") is None

    after = parse_portals([*_ROWS, {"slug": "skunkworks", "discord_id": "3333333333333333333"}])
    assert after.discord_id_for("skunkworks") == "3333333333333333333"
    # The pre-existing portals are untouched by the addition.
    assert after.discord_id_for("arena") == "1111111111111111111"


def test_resolve_slug_canonicalises_either_spelling_to_one_slug() -> None:
    """A slug and its Discord id both canonicalise to ONE slug.

    Two spellings of one room must not produce two chat ids — the subscriber keys
    its ``chat_id -> task_id`` correlation on this value.
    """
    channels = parse_portals(_ROWS)
    assert channels.resolve_slug("arena") == "arena"
    assert channels.resolve_slug("1111111111111111111") == "arena"
    assert channels.resolve_slug("ARENA") == "arena"  # case-insensitive
    assert channels.resolve_slug("unknown") is None


def test_slug_matching_is_case_insensitive_and_trimmed() -> None:
    """A card typed ``  Arena `` still resolves — operator input is forgiving."""
    channels = parse_portals(_ROWS)
    assert channels.discord_id_for("  Arena ") == "1111111111111111111"


def test_len_and_slugs_expose_the_map_for_diagnostics() -> None:
    channels = parse_portals(_ROWS)
    assert len(channels) == 2
    assert channels.slugs == ("arena", "fleet-ops")
    assert len(empty_map()) == 0
    assert empty_map().source == "empty"


def test_a_directly_constructed_map_builds_its_indexes() -> None:
    """``ChannelMap`` is usable without going through a parser (the #129 seam).

    The next PR builds on this read API, so constructing a map from portals in
    code — not just from a fetched payload — has to index both directions.
    """
    channels = ChannelMap(portals=(ChannelPortal(slug="arena", discord_id="111"),), source="test")
    assert channels.discord_id_for("arena") == "111"
    assert channels.slug_for("111") == "arena"
    assert channels.resolve_slug("111") == "arena"


# ---- strict parsing: a half-loaded portal table is the worst outcome ----------


def test_parse_accepts_the_object_form_with_version() -> None:
    channels = parse_portals({"version": 3, "channels": _ROWS})
    assert channels.source == "kboard"
    assert len(channels) == 2


def test_parse_coerces_an_integer_discord_id() -> None:
    """A snowflake sent as a JSON int is exact in Python; keep it as a string."""
    channels = parse_portals([{"slug": "arena", "discord_id": 1111111111111111111}])
    assert channels.discord_id_for("arena") == "1111111111111111111"


@pytest.mark.parametrize(
    "payload",
    [
        "not-an-object",
        {"channels": "not-a-list"},
        [{"slug": "", "discord_id": "1"}],
        [{"slug": "arena"}],
        [{"slug": "arena", "discord_id": ""}],
        [{"slug": "arena", "discord_id": 1.5}],
        [{"slug": "arena", "discord_id": True}],
        ["not-an-object-row"],
    ],
)
def test_parse_rejects_malformed_payloads(payload: object) -> None:
    with pytest.raises(ChannelMapError):
        parse_portals(payload)


def test_parse_rejects_a_duplicate_slug() -> None:
    """One slug must name exactly one channel — a dupe is silent misrouting."""
    with pytest.raises(ChannelMapError, match="duplicate slug"):
        parse_portals([{"slug": "arena", "discord_id": "1"}, {"slug": "Arena", "discord_id": "2"}])


def test_parse_rejects_a_duplicate_discord_id() -> None:
    """One id must name exactly one slug, or the reverse lookup is ambiguous."""
    with pytest.raises(ChannelMapError, match="duplicate discord_id"):
        parse_portals([{"slug": "a", "discord_id": "1"}, {"slug": "b", "discord_id": "1"}])


# ---- the static bootstrap spec ----------------------------------------------


def test_static_spec_parses_many_portals() -> None:
    """``FLEET_CHANNEL_MAP`` is a MANY-portal map, not a revived default channel."""
    channels = parse_static_spec("arena=111, fleet-ops=222 ,")
    assert channels.source == "env"
    assert channels.discord_id_for("arena") == "111"
    assert channels.discord_id_for("fleet-ops") == "222"


def test_static_spec_rejects_an_entry_without_an_equals() -> None:
    with pytest.raises(ChannelMapError, match="missing '='"):
        parse_static_spec("arena")


def test_static_spec_rejects_a_blank_side() -> None:
    with pytest.raises(ChannelMapError):
        parse_static_spec("arena=")


def test_empty_static_spec_is_an_empty_map() -> None:
    assert len(parse_static_spec("  ,  ")) == 0


# ---- fetch + load: fail open, loudly, to an EMPTY map (never a stale guess) ---


@pytest.mark.asyncio
async def test_fetch_returns_the_served_map() -> None:
    async with _transport({"version": 1, "channels": _ROWS}) as client:
        channels = await fetch_channel_map("https://k-board.example", client=client)
    assert channels.discord_id_for("fleet-ops") == "2222222222222222222"


@pytest.mark.asyncio
async def test_fetch_raises_on_a_non_2xx() -> None:
    async with _transport({}, status=500) as client:
        with pytest.raises(httpx.HTTPStatusError):
            await fetch_channel_map("https://k-board.example", client=client)


@pytest.mark.asyncio
async def test_load_prefers_kboard_over_the_static_spec(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """k-board is authoritative; the env spec is only a bootstrap."""
    monkeypatch.setenv(_ENV_URL, "https://k-board.example")
    monkeypatch.setenv(_ENV_STATIC, "arena=999")
    async with _transport({"version": 1, "channels": _ROWS}) as client:
        channels = await load_channel_map(client=client)
    assert channels.source == "kboard"
    assert channels.discord_id_for("arena") == "1111111111111111111"


@pytest.mark.asyncio
async def test_load_falls_back_to_the_static_spec_when_kboard_is_down(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A 500 from k-board must not take a bridge leg down — fail open to the env."""
    monkeypatch.setenv(_ENV_URL, "https://k-board.example")
    monkeypatch.setenv(_ENV_STATIC, "arena=999,fleet-ops=888")
    async with _transport({}, status=500) as client:
        channels = await load_channel_map(client=client)
    assert channels.source == "env"
    assert channels.discord_id_for("arena") == "999"
    assert channels.discord_id_for("fleet-ops") == "888"


@pytest.mark.asyncio
async def test_load_falls_back_to_the_empty_map_when_nothing_is_configured(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """No URL, no spec → the EMPTY map. Honest, never a silent default channel."""
    monkeypatch.delenv(_ENV_URL, raising=False)
    monkeypatch.delenv(_ENV_STATIC, raising=False)
    channels = await load_channel_map()
    assert channels.source == "empty"
    assert channels.discord_id_for("arena") is None


@pytest.mark.asyncio
async def test_load_falls_back_to_empty_on_a_malformed_static_spec(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A typo'd env spec degrades loudly to empty rather than raising at connect."""
    monkeypatch.delenv(_ENV_URL, raising=False)
    monkeypatch.setenv(_ENV_STATIC, "arena")
    channels = await load_channel_map()
    assert channels.source == "empty"


@pytest.mark.asyncio
async def test_load_falls_back_to_empty_on_a_malformed_kboard_payload(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A malformed table is never half-loaded — it is no map at all."""
    monkeypatch.setenv(_ENV_URL, "https://k-board.example")
    monkeypatch.delenv(_ENV_STATIC, raising=False)
    async with _transport([{"slug": "arena"}]) as client:
        channels = await load_channel_map(client=client)
    assert channels.source == "empty"


# --- review: a slug becomes a SUBJECT TOKEN, so the map is the validation boundary


@pytest.mark.parametrize(
    "bad",
    [
        "*",
        ">",
        "a.b",
        "two words",
        "arena*",
        "*.*",
        # Cross-runtime review: the guard listed a literal " ", which catches only
        # U+0020. NATS treats tab/newline/CR as PUB-command separators, so these
        # produce a malformed PUB that nats-py queues without client-side
        # validation — the dispatch is lost asynchronously with no error anywhere.
        "arena\tops",
        "arena\nops",
        "arena\rops",
        "arena\x0bops",
        "arena\x0cops",
        "arena ops",
    ],
)
def test_a_slug_that_is_not_subject_safe_is_refused(bad: str) -> None:
    """A wildcard/delimiter slug is rejected at parse, not discovered on the wire.

    A stamped slug becomes a NATS subject TOKEN
    (``{env}.agent.{channel}.dispatch.{worker}``). ``subjects.slug`` cannot refuse
    ``*`` — it must keep accepting STAR so callers can build authorization grant
    patterns — so an accepted ``*`` portal publishes every dispatch onto
    ``{env}.agent.*.dispatch.{worker}``, which the manager's OWN publish grant
    accepts and nats-py never validates client-side: the card is marked
    coordinated and the dispatch silently vanishes. ``>`` instead makes
    ``subjects.slug`` raise inside the publish path, which is swallowed into a log
    line, so the card is re-polled and dropped forever. Both are silent; the map
    is the only place that sees the value before it becomes a subject.
    """
    with pytest.raises(ChannelMapError, match="subject-safe"):
        parse_portals([{"slug": bad, "discord_id": "999"}])


def test_the_static_spec_refuses_a_wildcard_slug_too() -> None:
    """The env bootstrap runs through the SAME validation as the fetched table."""
    with pytest.raises(ChannelMapError, match="subject-safe"):
        parse_static_spec("*=999")


def test_the_static_spec_refuses_an_entry_with_two_equals() -> None:
    """Cross-runtime review: the docstring promised exactly one ``=``; enforce it.

    ``partition("=")`` splits on the FIRST ``=`` only, so ``all-agents=123=456``
    parsed to ``discord_id="123=456"`` and was accepted as an ACTIVE portal. A
    non-empty map then retires the legacy bridge, so every ``all-agents`` reply
    was sent to the invalid Discord target ``123=456`` and silently not mirrored
    — a typo in one env var takes a channel dark with no error.
    """
    with pytest.raises(ChannelMapError, match="exactly one"):
        parse_static_spec("all-agents=123=456")


def test_is_subject_safe_slug_accepts_ordinary_channel_names() -> None:
    """The guard must not reject the names operators actually type."""
    for good in ("arena", "fleet-ops", "all-agents", "Arena", "chan_1", "1234567890123456789"):
        assert is_subject_safe_slug(good) is True


def test_a_slug_colliding_with_another_rows_discord_id_is_refused() -> None:
    """Cross-namespace collision is refused — uniqueness WITHIN each is not enough.

    Both rows here are individually valid: no duplicate slug, no duplicate id. But
    the slug ``1111`` is also ``arena``'s Discord id, and the two legs used to
    search the two indexes in OPPOSITE order — the inbound resolver slug-first, the
    outbound tee id-first — so one channel resolved to two different rooms. A reply
    addressed to the ``1111`` slug (real room ``aaa``) mirrored into Discord channel
    ``1111``, which is ``arena``'s room: posting a task's output into a room it does
    not belong to.
    """
    with pytest.raises(ChannelMapError, match="collides with the discord_id"):
        parse_portals(
            [
                {"slug": "1111", "discord_id": "aaa"},
                {"slug": "arena", "discord_id": "1111"},
            ]
        )


def test_a_slug_equal_to_its_OWN_discord_id_is_still_allowed() -> None:
    """The disjointness rule targets CROSS-row collisions, not a self-referential row.

    A portal whose slug IS its own Discord id (a channel named by its snowflake) is
    unambiguous — both indexes point at the same portal — so it must keep working.
    """
    parsed = parse_portals([{"slug": "1111", "discord_id": "1111"}])
    assert parsed.resolve_slug("1111") == "1111"
    assert parsed.resolve_discord_id("1111") == "1111"


def test_both_resolvers_share_one_precedence_rule() -> None:
    """``resolve_slug`` and ``resolve_discord_id`` answer from the SAME lookup.

    Even for a hand-built map that bypasses ``parse_portals`` validation, the two
    directions must never disagree about which portal a channel names — that
    disagreement is what routed a reply into the wrong room.
    """
    hand_built = ChannelMap(
        portals=(
            ChannelPortal(slug="1111", discord_id="aaa"),
            ChannelPortal(slug="arena", discord_id="1111"),
        ),
        source="kboard",
    )
    assert hand_built.portal_for("1111") is hand_built.portals[0]
    assert hand_built.resolve_slug("1111") == "1111"
    # The id-first precedence the tee used to have would answer "1111" here.
    assert hand_built.resolve_discord_id("1111") == "aaa"
