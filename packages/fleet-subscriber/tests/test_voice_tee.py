"""Unit tests for the optional Discord voice tee (#55).

Two layers: the pure ``VoiceTeeConfig.from_env`` reader, and the best-effort
``tee_to_discord`` mirror against a fake gateway runner. No real Hermes gateway
and no network — the sanctioned ``gateway.run._gateway_runner_ref`` seam is
patched via ``sys.modules`` (the module does not exist off-host).
"""

from __future__ import annotations

import sys
import types
from dataclasses import dataclass
from typing import Any

import pytest
from fleet_nats import ChannelMap, ChannelPortal, empty_map, parse_portals, subjects

import fleet_subscriber.voice_tee as voice_tee_mod
from fleet_subscriber.voice_tee import (
    VoiceTeeConfig,
    is_forwardable,
    lane_of,
    tee_to_discord,
)


def _tee_map() -> ChannelMap:
    """The portal map the tee tests resolve through (fleet-bus #118).

    ``chan-1`` keeps every pre-existing assertion in this file valid: the tests
    below still mirror to ``chan-1``, they just reach it by resolving the turn's
    ``arena`` slug instead of reading one env var.
    """
    return parse_portals(
        [
            {"slug": "arena", "discord_id": "chan-1"},
            {"slug": "fleet-ops", "discord_id": "chan-2"},
        ]
    )


# --- Platform factory stub: the adapter passes api.Platform("discord") ---------


class StubPlatform:
    def __init__(self, name: str) -> None:
        self.name = name

    def __eq__(self, other: object) -> bool:
        return isinstance(other, StubPlatform) and other.name == self.name

    def __hash__(self) -> int:
        return hash(self.name)


@dataclass
class StubSendResult:
    success: bool
    error: str | None = None


class RecordingDiscordAdapter:
    """Records send() calls; configurable success/raise for the failure test.

    Also models the TLDR thread-spill seam (#160): ``create_handoff_thread``
    returns a stub thread id (or ``None`` when ``can_thread=False``), and
    ``send`` accepts the optional ``metadata`` the spill passes.
    """

    def __init__(
        self,
        *,
        success: bool = True,
        raises: bool = False,
        can_thread: bool = True,
        thread_send_fails: bool = False,
        thread_send_raises: bool = False,
        final_send_fails: bool = False,
        final_send_raises: bool = False,
    ) -> None:
        self._success = success
        self._raises = raises
        self._can_thread = can_thread
        # Models Discord's create-then-post cache race (#160 codex HIGH): a send
        # INTO a thread returns success=False WITHOUT raising, while in-channel
        # sends succeed. Used to prove the remainder falls back in-channel.
        self._thread_send_fails = thread_send_fails
        # Sibling failure mode (#160 codex round-2 HIGH): the send INTO a thread
        # RAISES, while in-channel sends succeed. Proves a raised threaded send is
        # caught in the spill and falls back in-channel (not left to escape).
        self._thread_send_raises = thread_send_raises
        self._final_send_fails = final_send_fails
        self._final_send_raises = final_send_raises
        self.calls: list[dict[str, Any]] = []
        self.threads: list[dict[str, str]] = []

    async def send(
        self,
        *,
        chat_id: str,
        content: str,
        metadata: dict[str, Any] | None = None,
    ) -> StubSendResult:
        self.calls.append({"chat_id": chat_id, "content": content, "metadata": metadata})
        if self._raises:
            raise RuntimeError(f"discord down; reply was {content!r}")
        is_thread_send = bool(metadata and metadata.get("thread_id"))
        if is_thread_send and self._thread_send_raises:
            raise RuntimeError(f"thread post race; reply was {content!r}")
        if is_thread_send and self._thread_send_fails:
            return StubSendResult(success=False)
        is_terminal_fallback = not is_thread_send and len(self.calls) > 1
        if is_terminal_fallback and self._final_send_raises:
            raise RuntimeError(f"final channel post failed; reply was {content!r}")
        if is_terminal_fallback and self._final_send_fails:
            return StubSendResult(success=False, error="final channel post failed")
        return StubSendResult(success=self._success)

    async def create_handoff_thread(self, parent_chat_id: str, name: str) -> str | None:
        if not self._can_thread:
            return None
        self.threads.append({"parent": parent_chat_id, "name": name})
        return "thread-99"


class FakeRunner:
    def __init__(self, adapters: dict[Any, Any]) -> None:
        self.adapters = adapters


class RecordingLogger:
    """Tiny loguru-shaped logger for asserting sanitized voice-tee outcomes."""

    def __init__(self) -> None:
        self.records: list[tuple[str, str]] = []

    def opt(self, **_: Any) -> RecordingLogger:
        return self

    def debug(self, message: str, *args: Any) -> None:
        self._record("DEBUG", message, *args)

    def info(self, message: str, *args: Any) -> None:
        self._record("INFO", message, *args)

    def warning(self, message: str, *args: Any) -> None:
        self._record("WARNING", message, *args)

    def error(self, message: str, *args: Any) -> None:
        self._record("ERROR", message, *args)

    def _record(self, level: str, message: str, *args: Any) -> None:
        self.records.append((level, message.format(*args)))

    def messages(self, level: str | None = None) -> list[str]:
        return [message for record_level, message in self.records if level in {None, record_level}]


def _install_runner(monkeypatch: pytest.MonkeyPatch, runner: Any) -> None:
    """Install a fake ``gateway.run`` module exposing ``_gateway_runner_ref``."""
    module = types.ModuleType("gateway.run")
    module._gateway_runner_ref = lambda: runner  # type: ignore[attr-defined]
    pkg = types.ModuleType("gateway")
    monkeypatch.setitem(sys.modules, "gateway", pkg)
    monkeypatch.setitem(sys.modules, "gateway.run", module)


# --- rtech_hermes_runtime.tldr_split stub (#160) ------------------------------
#
# The real split module lives in the Hermes host venv, not a pip dep here (see
# pyproject.toml), so the tee imports it deferred + guarded. Under test we install
# a faithful stand-in via sys.modules: a config that reads the SAME env vars and a
# split_for_tldr that reproduces the real contract (split iff body > limit; the
# headline is the first ``limit`` chars, the remainder is the rest, both non-empty
# to count as a split). This lets us assert the tee's headline/thread behavior
# without vendoring rtech_hermes_runtime.


@dataclass
class _StubSplitDecision:
    should_split: bool
    headline: str
    remainder: str = ""
    thread_name: str = "Details"


@dataclass
class _StubSplitConfig:
    enabled: bool
    limit: int

    @classmethod
    def from_env(cls) -> _StubSplitConfig:
        import os

        raw = os.environ.get("RTECH_TLDR_SPLIT_ENABLED", "").strip().lower()
        enabled = raw in {"1", "true", "yes", "on"}
        limit = int(os.environ.get("RTECH_TLDR_LIMIT", "400") or "400")
        return cls(enabled=enabled, limit=limit)


def _stub_split_for_tldr(text: str, limit: int) -> _StubSplitDecision:
    if not text or len(text) <= limit:
        return _StubSplitDecision(should_split=False, headline=text, remainder="")
    headline = text[:limit].rstrip()
    remainder = text[limit:].lstrip()
    if not headline or not remainder:
        return _StubSplitDecision(should_split=False, headline=text, remainder="")
    return _StubSplitDecision(
        should_split=True,
        headline=headline,
        remainder=remainder,
        thread_name=headline.splitlines()[0][:60] or "Details",
    )


def _install_split_module(monkeypatch: pytest.MonkeyPatch) -> None:
    """Install a fake ``rtech_hermes_runtime.tldr_split`` faithful to the contract."""
    module = types.ModuleType("rtech_hermes_runtime.tldr_split")
    module.TldrSplitConfig = _StubSplitConfig  # type: ignore[attr-defined]
    module.split_for_tldr = _stub_split_for_tldr  # type: ignore[attr-defined]
    pkg = types.ModuleType("rtech_hermes_runtime")
    monkeypatch.setitem(sys.modules, "rtech_hermes_runtime", pkg)
    monkeypatch.setitem(sys.modules, "rtech_hermes_runtime.tldr_split", module)


# --- VoiceTeeConfig.from_env --------------------------------------------------


def test_config_default_off(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("FLEET_VOICE_TEE_ENABLED", raising=False)
    monkeypatch.delenv("FLEET_VOICE_TEE_CHANNEL_ID", raising=False)
    cfg = VoiceTeeConfig.from_env(_tee_map())
    assert cfg.enabled is False
    assert cfg.active is False


@pytest.mark.parametrize("raw", ["1", "true", "TRUE", "Yes", "on"])
def test_config_truthy_spellings(monkeypatch: pytest.MonkeyPatch, raw: str) -> None:
    monkeypatch.setenv("FLEET_VOICE_TEE_ENABLED", raw)
    cfg = VoiceTeeConfig.from_env(_tee_map())
    assert cfg.enabled is True
    assert cfg.active is True


@pytest.mark.parametrize("raw", ["0", "false", "no", "", "nonsense"])
def test_config_non_truthy_reads_off(monkeypatch: pytest.MonkeyPatch, raw: str) -> None:
    monkeypatch.setenv("FLEET_VOICE_TEE_ENABLED", raw)
    cfg = VoiceTeeConfig.from_env(_tee_map())
    assert cfg.enabled is False
    assert cfg.active is False


def test_config_enabled_with_no_portals_is_inactive(monkeypatch: pytest.MonkeyPatch) -> None:
    """Enabled with an EMPTY map is a no-op: there is no room to resolve to."""
    monkeypatch.setenv("FLEET_VOICE_TEE_ENABLED", "true")
    cfg = VoiceTeeConfig.from_env(empty_map())
    assert cfg.enabled is True
    assert cfg.active is False


# --- #118: the target is the TURN's channel, not one env-configured room ------


def test_each_channel_mirrors_to_its_own_room() -> None:
    """Two turns in two channels resolve to two DIFFERENT Discord ids.

    This is the #118 defect, stated as a test. The old tee took its target from
    ``FLEET_VOICE_TEE_CHANNEL_ID`` and ignored the turn's channel entirely, so
    both of these returned the SAME id — an ``arena`` reply posted into
    ``fleet-ops`` (or vice versa) depending only on which host it ran on.
    """
    cfg = VoiceTeeConfig(enabled=True, channels=_tee_map())
    assert cfg.target_for("arena") == "chan-1"
    assert cfg.target_for("fleet-ops") == "chan-2"
    assert cfg.target_for("arena") != cfg.target_for("fleet-ops")


def test_unmapped_channel_has_no_target_so_the_mirror_is_skipped() -> None:
    """An unmapped channel is ``None`` — never a fallback to another room.

    Posting a task's output into a room it does not belong to is worse than not
    posting it, so the tee refuses rather than substitutes.
    """
    cfg = VoiceTeeConfig(enabled=True, channels=_tee_map())
    assert cfg.target_for("nowhere") is None
    assert cfg.target_for(None) is None
    assert cfg.target_for("  ") is None


def test_a_discord_id_channel_resolves_to_itself() -> None:
    """A turn whose channel is already a Discord id posts to that id."""
    cfg = VoiceTeeConfig(enabled=True, channels=_tee_map())
    assert cfg.target_for("chan-2") == "chan-2"


def test_adding_a_channel_needs_no_code_change() -> None:
    """The acceptance criterion on the outbound leg: a new portal is DATA."""
    before = VoiceTeeConfig(enabled=True, channels=_tee_map())
    assert before.target_for("skunkworks") is None
    after = VoiceTeeConfig(
        enabled=True,
        channels=parse_portals(
            [
                {"slug": "arena", "discord_id": "chan-1"},
                {"slug": "fleet-ops", "discord_id": "chan-2"},
                {"slug": "skunkworks", "discord_id": "chan-3"},
            ]
        ),
    )
    assert after.target_for("skunkworks") == "chan-3"


def test_retired_single_channel_env_is_not_honoured(monkeypatch: pytest.MonkeyPatch) -> None:
    """``FLEET_VOICE_TEE_CHANNEL_ID`` no longer routes anything (fleet-bus #118).

    A deploy that still sets it must not resurrect the one-room-for-every-task
    behavior: an unmapped channel stays unmapped regardless of the retired env.
    """
    monkeypatch.setenv("FLEET_VOICE_TEE_ENABLED", "true")
    monkeypatch.setenv("FLEET_VOICE_TEE_CHANNEL_ID", "legacy-room")
    cfg = VoiceTeeConfig.from_env(_tee_map(), home_channel="arena")
    assert cfg.target_for("nowhere") is None
    assert cfg.target_for("arena") == "chan-1"


# --- review: the legacy env is a migration BRIDGE, not a silent kill switch ----


def test_the_legacy_env_bridges_the_home_channel_when_no_map_is_loaded(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An enabled tee must not go DARK on the live deploy the moment #118 merges.

    Live posture on an agent host today: ``FLEET_SUBJECT_REGISTRY_URL`` and
    ``FLEET_VOICE_TEE_CHANNEL_ID`` are set, ``FLEET_CHANNEL_MAP`` is not, and
    ``/api/channel-map`` 404s — so the load ladder lands on the EMPTY map. With a
    warn-only retirement ``active`` is False and the mirror stops reaching Discord
    entirely: a regression from "mirrors to one room" to "mirrors to NO room",
    delivered by an atomic code cutover that needs a manual env edit on every host
    to undo. The legacy id is therefore bridged as ONE home-channel portal.
    """
    monkeypatch.setenv("FLEET_VOICE_TEE_ENABLED", "true")
    monkeypatch.setenv("FLEET_VOICE_TEE_CHANNEL_ID", "legacy-room")
    cfg = VoiceTeeConfig.from_env(empty_map(), home_channel="all-agents")

    assert cfg.active is True
    assert cfg.target_for("all-agents") == "legacy-room"
    assert cfg.channels.source == "legacy-env"


def test_the_legacy_bridge_still_refuses_every_other_channel(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The bridge cannot resurrect the #118 defect: it maps exactly ONE slug.

    Pre-#118 the tee mirrored EVERY channel's reply into the single env room. The
    bridge maps only this agent's home channel, so another channel's turn resolves
    to ``None`` and is skipped rather than mis-routed into the legacy room.
    """
    monkeypatch.setenv("FLEET_VOICE_TEE_ENABLED", "true")
    monkeypatch.setenv("FLEET_VOICE_TEE_CHANNEL_ID", "legacy-room")
    cfg = VoiceTeeConfig.from_env(empty_map(), home_channel="all-agents")

    assert cfg.target_for("arena") is None
    assert cfg.target_for("fleet-ops") is None


def test_a_real_map_beats_the_legacy_env_outright(monkeypatch: pytest.MonkeyPatch) -> None:
    """Once a map is loaded the bridge is dead — the legacy id routes nothing."""
    monkeypatch.setenv("FLEET_VOICE_TEE_ENABLED", "true")
    monkeypatch.setenv("FLEET_VOICE_TEE_CHANNEL_ID", "legacy-room")
    cfg = VoiceTeeConfig.from_env(_tee_map(), home_channel="arena")

    assert cfg.channels.source == "kboard"
    assert cfg.target_for("arena") == "chan-1"
    # The home channel resolves to its PORTAL's room, never the legacy room.
    assert cfg.target_for("arena") != "legacy-room"


def test_a_partial_map_rollout_does_not_drop_the_home_channel_bridge(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Cross-runtime review: retiring on ``len(channels) > 0`` un-mirrors a live channel.

    A rolling seed adds ONE unrelated row while this agent's own channel still
    has none. The old test was "is the map non-empty", so the bridge was
    discarded, ``target_for('all-agents')`` returned ``None``, and a channel that
    was mirroring a moment ago went dark purely because some OTHER channel was
    added. The retire test must be "does HOME have a portal".
    """
    monkeypatch.setenv("FLEET_VOICE_TEE_ENABLED", "true")
    monkeypatch.setenv("FLEET_VOICE_TEE_CHANNEL_ID", "legacy-room")
    partial = parse_portals([{"slug": "arena", "discord_id": "chan-1"}])

    cfg = VoiceTeeConfig.from_env(partial, home_channel="all-agents")

    # The home channel keeps mirroring through the legacy id...
    assert cfg.target_for("all-agents") == "legacy-room"
    # ...and the unrelated row that triggered the rollout is NOT discarded.
    assert cfg.target_for("arena") == "chan-1"
    # Still no invention: an unmapped channel is skipped, never mis-routed.
    assert cfg.target_for("fleet-ops") is None


def test_the_bridge_retires_once_the_home_channel_has_its_own_portal(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The bridge dies on the row that matters — home's — not on any row at all."""
    monkeypatch.setenv("FLEET_VOICE_TEE_ENABLED", "true")
    monkeypatch.setenv("FLEET_VOICE_TEE_CHANNEL_ID", "legacy-room")

    cfg = VoiceTeeConfig.from_env(_tee_map(), home_channel="arena")

    assert cfg.target_for("arena") == "chan-1"
    assert cfg.target_for("arena") != "legacy-room"


def test_no_legacy_env_and_no_map_leaves_the_tee_inert(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The bridge is opt-in on the legacy env; it never invents a portal."""
    monkeypatch.setenv("FLEET_VOICE_TEE_ENABLED", "true")
    monkeypatch.delenv("FLEET_VOICE_TEE_CHANNEL_ID", raising=False)
    cfg = VoiceTeeConfig.from_env(empty_map(), home_channel="all-agents")

    assert cfg.enabled is True
    assert cfg.active is False


def test_the_two_resolvers_cannot_disagree_about_one_channel() -> None:
    """``target_for`` shares ONE precedence rule with ``ChannelMap.resolve_slug``.

    ``target_for`` used to search the id index first while ``resolve_slug``
    searched the slug index first, so a numeric slug equal to a DIFFERENT portal's
    Discord id sent the two legs to two different rooms: a turn addressed to the
    ``1111`` slug (whose real room is ``aaa``) mirrored into Discord channel
    ``1111`` — ``arena``'s room.
    """
    hand_built = ChannelMap(
        portals=(
            ChannelPortal(slug="1111", discord_id="aaa"),
            ChannelPortal(slug="arena", discord_id="1111"),
        ),
        source="kboard",
    )
    cfg = VoiceTeeConfig(enabled=True, channels=hand_built)

    assert cfg.target_for("1111") == "aaa"
    assert hand_built.resolve_slug("1111") == "1111"


# --- tee_to_discord -----------------------------------------------------------


async def test_tee_posts_to_the_discord_adapter(monkeypatch: pytest.MonkeyPatch) -> None:
    discord = RecordingDiscordAdapter(success=True)
    _install_runner(monkeypatch, FakeRunner({StubPlatform("discord"): discord}))

    await tee_to_discord(
        config=VoiceTeeConfig(enabled=True, channels=_tee_map()),
        platform_factory=StubPlatform,
        channel_id="chan-1",
        text="the visible reply",
    )
    assert discord.calls == [
        {"chat_id": "chan-1", "content": "the visible reply", "metadata": None}
    ]


async def test_tee_no_runner_is_a_noop(monkeypatch: pytest.MonkeyPatch) -> None:
    # Off-host / out-of-process: the weakref returns None. No crash, no call.
    _install_runner(monkeypatch, None)
    await tee_to_discord(
        config=VoiceTeeConfig(enabled=True, channels=_tee_map()),
        platform_factory=StubPlatform,
        channel_id="chan-1",
        text="hi",
    )  # must simply return


async def test_tee_no_discord_adapter_is_a_noop(monkeypatch: pytest.MonkeyPatch) -> None:
    # Live runner but Discord not connected on this gateway → WARN + return.
    _install_runner(monkeypatch, FakeRunner({}))
    await tee_to_discord(
        config=VoiceTeeConfig(enabled=True, channels=_tee_map()),
        platform_factory=StubPlatform,
        channel_id="chan-1",
        text="hi",
    )


async def test_tee_send_failure_never_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    # An adapter that raises must be swallowed (the bus path depends on it).
    discord = RecordingDiscordAdapter(raises=True)
    _install_runner(monkeypatch, FakeRunner({StubPlatform("discord"): discord}))
    await tee_to_discord(  # must not raise
        config=VoiceTeeConfig(enabled=True, channels=_tee_map()),
        platform_factory=StubPlatform,
        channel_id="chan-1",
        text="secret reply prose",
    )
    assert len(discord.calls) == 1  # it was attempted


async def test_tee_unsuccessful_result_is_swallowed(monkeypatch: pytest.MonkeyPatch) -> None:
    discord = RecordingDiscordAdapter(success=False)
    _install_runner(monkeypatch, FakeRunner({StubPlatform("discord"): discord}))
    await tee_to_discord(  # must not raise on a falsy SendResult
        config=VoiceTeeConfig(enabled=True, channels=_tee_map()),
        platform_factory=StubPlatform,
        channel_id="chan-1",
        text="hi",
    )
    assert len(discord.calls) == 1


# --- TLDR channel/thread split on the teed copy (#160) ------------------------


async def test_tee_long_reply_splits_headline_and_thread(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A >400-char teed reply → a short in-channel headline + a detail THREAD.

    This is the #160 regression: the old raw send let a 7916-char reply become a
    5-part in-channel wall (Discord's native chunker). With the split it is ONE
    headline post in-channel plus the remainder posted into a created thread.
    """
    monkeypatch.setenv("RTECH_TLDR_SPLIT_ENABLED", "true")
    monkeypatch.setenv("RTECH_TLDR_LIMIT", "400")
    _install_split_module(monkeypatch)
    discord = RecordingDiscordAdapter(success=True)
    _install_runner(monkeypatch, FakeRunner({StubPlatform("discord"): discord}))

    long_reply = "A" * 500 + " " + "B" * 500  # body well over the 400 limit

    await tee_to_discord(
        config=VoiceTeeConfig(enabled=True, channels=_tee_map()),
        platform_factory=StubPlatform,
        channel_id="chan-1",
        text=long_reply,
    )

    # Exactly two sends: the headline in-channel (no thread metadata) and the
    # remainder into the created thread — NOT a multi-part in-channel wall.
    assert len(discord.calls) == 2
    headline_call, remainder_call = discord.calls
    assert headline_call["metadata"] is None
    assert len(headline_call["content"]) <= 400
    assert remainder_call["metadata"] == {"thread_id": "thread-99"}
    # A thread was actually opened off the parent channel.
    assert discord.threads == [{"parent": "chan-1", "name": discord.threads[0]["name"]}]
    # Headline + remainder together preserve the substance (no detail dropped).
    assert headline_call["content"] + remainder_call["content"] == long_reply


async def test_tee_short_reply_is_unchanged_single_send(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A <400-char teed reply is a single in-channel send, no thread (unchanged)."""
    monkeypatch.setenv("RTECH_TLDR_SPLIT_ENABLED", "true")
    monkeypatch.setenv("RTECH_TLDR_LIMIT", "400")
    _install_split_module(monkeypatch)
    discord = RecordingDiscordAdapter(success=True)
    _install_runner(monkeypatch, FakeRunner({StubPlatform("discord"): discord}))

    await tee_to_discord(
        config=VoiceTeeConfig(enabled=True, channels=_tee_map()),
        platform_factory=StubPlatform,
        channel_id="chan-1",
        text="a short reply",
    )
    assert discord.calls == [{"chat_id": "chan-1", "content": "a short reply", "metadata": None}]
    assert discord.threads == []


async def test_tee_split_disabled_sends_whole_even_when_long(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Split OFF (default) → the whole long reply goes in-channel, no thread.

    Preserves the pre-#160 behavior when the split flag is not set: fail-open to a
    single send (Discord's own chunker still applies, but we never split).
    """
    monkeypatch.delenv("RTECH_TLDR_SPLIT_ENABLED", raising=False)
    _install_split_module(monkeypatch)
    discord = RecordingDiscordAdapter(success=True)
    _install_runner(monkeypatch, FakeRunner({StubPlatform("discord"): discord}))

    long_reply = "A" * 900
    await tee_to_discord(
        config=VoiceTeeConfig(enabled=True, channels=_tee_map()),
        platform_factory=StubPlatform,
        channel_id="chan-1",
        text=long_reply,
    )
    assert discord.calls == [{"chat_id": "chan-1", "content": long_reply, "metadata": None}]
    assert discord.threads == []


async def test_tee_split_module_absent_falls_back_to_whole_send(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Off-host (rtech_hermes_runtime not importable) → whole send, never raises."""
    monkeypatch.setenv("RTECH_TLDR_SPLIT_ENABLED", "true")
    # Deliberately do NOT install the split module: the deferred import fails.
    monkeypatch.delitem(sys.modules, "rtech_hermes_runtime.tldr_split", raising=False)
    monkeypatch.delitem(sys.modules, "rtech_hermes_runtime", raising=False)
    discord = RecordingDiscordAdapter(success=True)
    _install_runner(monkeypatch, FakeRunner({StubPlatform("discord"): discord}))

    long_reply = "A" * 900
    await tee_to_discord(  # must not raise despite the missing module
        config=VoiceTeeConfig(enabled=True, channels=_tee_map()),
        platform_factory=StubPlatform,
        channel_id="chan-1",
        text=long_reply,
    )
    assert discord.calls == [{"chat_id": "chan-1", "content": long_reply, "metadata": None}]
    assert discord.threads == []


async def test_tee_split_thread_creation_unavailable_posts_compact_notice(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When threading is unsupported, parent gets only a compact failure notice."""
    monkeypatch.setenv("RTECH_TLDR_SPLIT_ENABLED", "true")
    monkeypatch.setenv("RTECH_TLDR_LIMIT", "400")
    _install_split_module(monkeypatch)
    logger = RecordingLogger()
    monkeypatch.setattr(voice_tee_mod, "logger", logger)
    discord = RecordingDiscordAdapter(success=True, can_thread=False)
    _install_runner(monkeypatch, FakeRunner({StubPlatform("discord"): discord}))

    long_reply = "A" * 500 + " " + "B" * 500
    await tee_to_discord(
        config=VoiceTeeConfig(enabled=True, channels=_tee_map()),
        platform_factory=StubPlatform,
        channel_id="chan-1",
        text=long_reply,
    )
    # Two sends, both in-channel: headline then compact notice. The full remainder
    # stays at the source; the tee must not dump it as a parent-channel wall.
    assert len(discord.calls) == 2
    assert all(c["metadata"] is None for c in discord.calls)
    assert discord.calls[1]["content"] != long_reply[400:].lstrip()
    assert len(discord.calls[1]["content"]) < 2000
    assert "could not be mirrored" in discord.calls[1]["content"]
    assert any("split mirror incomplete" in msg for msg in logger.messages("WARNING"))
    assert not any("mirrored (split)" in msg for msg in logger.messages("INFO"))


async def test_tee_headline_send_failure_skips_spill(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """If the headline send is unsuccessful, do NOT open a thread or spill."""
    monkeypatch.setenv("RTECH_TLDR_SPLIT_ENABLED", "true")
    monkeypatch.setenv("RTECH_TLDR_LIMIT", "400")
    _install_split_module(monkeypatch)
    discord = RecordingDiscordAdapter(success=False)  # headline send reports failure
    _install_runner(monkeypatch, FakeRunner({StubPlatform("discord"): discord}))

    long_reply = "A" * 500 + " " + "B" * 500
    await tee_to_discord(
        config=VoiceTeeConfig(enabled=True, channels=_tee_map()),
        platform_factory=StubPlatform,
        channel_id="chan-1",
        text=long_reply,
    )
    assert len(discord.calls) == 1  # only the headline attempt
    assert discord.threads == []  # no thread opened


async def test_tee_thread_remainder_send_failure_posts_compact_notice(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Threaded remainder send returns success=False (no raise) → compact notice.

    Discord's create-then-post cache race (#160 codex HIGH) returns a falsy
    ``SendResult`` WITHOUT raising. The old code only guarded a *raised* exception,
    so the remainder (the substance of the reply) was silently dropped and only
    the headline landed. The fix checks ``.success`` and emits a compact fail-loud
    parent notice instead of dumping the full remainder wall.
    """
    monkeypatch.setenv("RTECH_TLDR_SPLIT_ENABLED", "true")
    monkeypatch.setenv("RTECH_TLDR_LIMIT", "400")
    _install_split_module(monkeypatch)
    logger = RecordingLogger()
    monkeypatch.setattr(voice_tee_mod, "logger", logger)
    # Thread creation works, but the send INTO the thread returns success=False.
    discord = RecordingDiscordAdapter(success=True, thread_send_fails=True)
    _install_runner(monkeypatch, FakeRunner({StubPlatform("discord"): discord}))

    long_reply = "A" * 500 + " " + "B" * 500
    await tee_to_discord(  # must not raise
        config=VoiceTeeConfig(enabled=True, channels=_tee_map()),
        platform_factory=StubPlatform,
        channel_id="chan-1",
        text=long_reply,
    )

    # Three sends: headline in-channel, the FAILED threaded remainder, then the
    # compact parent notice. The full remainder is not dumped in-channel.
    assert len(discord.calls) == 3
    headline_call, failed_thread_call, notice_call = discord.calls
    assert headline_call["metadata"] is None
    assert failed_thread_call["metadata"] == {"thread_id": "thread-99"}
    assert notice_call["metadata"] is None
    assert failed_thread_call["content"] != notice_call["content"]
    assert len(notice_call["content"]) < 2000
    assert "could not be mirrored" in notice_call["content"]
    assert not any("mirrored (split)" in msg for msg in logger.messages("INFO"))


async def test_tee_thread_remainder_send_raise_posts_compact_notice(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Threaded remainder send RAISES (not returns False) → compact notice.

    The sibling gap (#160 codex round-2 HIGH): the threaded ``adapter.send`` was
    OUTSIDE any local try, so a raise escaped to the caller's guard, which swallows
    it AFTER the headline already posted — silently dropping the remainder. The
    structural fix wraps the threaded send in its own try/except so a raise reaches
    the SAME terminal compact-notice path as the success=False path. Must not raise.
    """
    monkeypatch.setenv("RTECH_TLDR_SPLIT_ENABLED", "true")
    monkeypatch.setenv("RTECH_TLDR_LIMIT", "400")
    _install_split_module(monkeypatch)
    logger = RecordingLogger()
    monkeypatch.setattr(voice_tee_mod, "logger", logger)
    # Thread creation works, but the send INTO the thread RAISES (in-channel is OK).
    discord = RecordingDiscordAdapter(success=True, thread_send_raises=True)
    _install_runner(monkeypatch, FakeRunner({StubPlatform("discord"): discord}))

    long_reply = "A" * 500 + " " + "B" * 500
    await tee_to_discord(  # must not raise despite the threaded send raising
        config=VoiceTeeConfig(enabled=True, channels=_tee_map()),
        platform_factory=StubPlatform,
        channel_id="chan-1",
        text=long_reply,
    )

    # Three sends: headline in-channel, the RAISED threaded remainder attempt, then
    # the compact parent notice — the raise did NOT dump the body as a wall.
    assert len(discord.calls) == 3
    headline_call, raised_thread_call, notice_call = discord.calls
    assert headline_call["metadata"] is None
    assert raised_thread_call["metadata"] == {"thread_id": "thread-99"}
    assert notice_call["metadata"] is None
    assert raised_thread_call["content"] != notice_call["content"]
    assert len(notice_call["content"]) < 2000
    assert "could not be mirrored" in notice_call["content"]
    assert not any("mirrored (split)" in msg for msg in logger.messages("INFO"))


async def test_tee_thread_failure_notice_send_failure_logs_error_without_mirrored_success(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Compact parent notice returns success=False → fail loud, no success log."""
    monkeypatch.setenv("RTECH_TLDR_SPLIT_ENABLED", "true")
    monkeypatch.setenv("RTECH_TLDR_LIMIT", "400")
    _install_split_module(monkeypatch)
    logger = RecordingLogger()
    monkeypatch.setattr(voice_tee_mod, "logger", logger)
    discord = RecordingDiscordAdapter(
        success=True,
        thread_send_fails=True,
        final_send_fails=True,
    )
    _install_runner(monkeypatch, FakeRunner({StubPlatform("discord"): discord}))

    long_reply = "A" * 500 + " " + "B" * 500
    await tee_to_discord(
        config=VoiceTeeConfig(enabled=True, channels=_tee_map()),
        platform_factory=StubPlatform,
        channel_id="chan-1",
        text=long_reply,
    )

    assert len(discord.calls) == 3
    assert any("thread failure parent notice send unsuccessful" in msg for msg in logger.messages())
    assert any("split mirror incomplete" in msg for msg in logger.messages("WARNING"))
    assert not any("mirrored (split)" in msg for msg in logger.messages("INFO"))


async def test_tee_thread_failure_notice_send_raise_logs_error_without_mirrored_success(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Compact parent notice raises → fail loud, no success log."""
    monkeypatch.setenv("RTECH_TLDR_SPLIT_ENABLED", "true")
    monkeypatch.setenv("RTECH_TLDR_LIMIT", "400")
    _install_split_module(monkeypatch)
    logger = RecordingLogger()
    monkeypatch.setattr(voice_tee_mod, "logger", logger)
    discord = RecordingDiscordAdapter(
        success=True,
        thread_send_fails=True,
        final_send_raises=True,
    )
    _install_runner(monkeypatch, FakeRunner({StubPlatform("discord"): discord}))

    long_reply = "A" * 500 + " " + "B" * 500
    await tee_to_discord(
        config=VoiceTeeConfig(enabled=True, channels=_tee_map()),
        platform_factory=StubPlatform,
        channel_id="chan-1",
        text=long_reply,
    )

    assert len(discord.calls) == 3
    assert any("thread failure parent notice send failed" in msg for msg in logger.messages())
    assert any("split mirror incomplete" in msg for msg in logger.messages("WARNING"))
    assert not any("mirrored (split)" in msg for msg in logger.messages("INFO"))


async def test_tee_thread_failure_with_large_remainder_does_not_dump_parent_wall(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A >2000-char failed thread remainder becomes one compact parent notice."""
    monkeypatch.setenv("RTECH_TLDR_SPLIT_ENABLED", "true")
    monkeypatch.setenv("RTECH_TLDR_LIMIT", "400")
    _install_split_module(monkeypatch)
    logger = RecordingLogger()
    monkeypatch.setattr(voice_tee_mod, "logger", logger)
    discord = RecordingDiscordAdapter(success=True, thread_send_fails=True)
    _install_runner(monkeypatch, FakeRunner({StubPlatform("discord"): discord}))

    long_reply = "A" * 500 + " " + "B" * 2500
    await tee_to_discord(
        config=VoiceTeeConfig(enabled=True, channels=_tee_map()),
        platform_factory=StubPlatform,
        channel_id="chan-1",
        text=long_reply,
    )

    assert len(discord.calls) == 3
    _, failed_thread_call, notice_call = discord.calls
    assert len(failed_thread_call["content"]) > 2000
    assert notice_call["metadata"] is None
    assert len(notice_call["content"]) < 2000
    assert "could not be mirrored" in notice_call["content"]
    assert "B" * 100 not in notice_call["content"]
    assert any("split mirror incomplete" in msg for msg in logger.messages("WARNING"))
    assert not any("mirrored (split)" in msg for msg in logger.messages("INFO"))


# --- the bridge's noise-lane refusal (fleet-bus #116) -------------------------


def test_lane_of_reads_both_subject_shapes() -> None:
    """The lane token is found in BOTH the bare and channel-scoped shapes.

    Told apart by token COUNT, exactly as NATS matches — so a channel named after
    a lane word cannot fool the reader.
    """
    assert lane_of(subjects.agent_status("dev", "t-1")) == "status"
    assert lane_of(subjects.agent_status("dev", "t-1", channel="dash-dev")) == "status"
    assert lane_of(subjects.agent_ack("dev", "t-1")) == "ack"
    assert lane_of(subjects.agent_ack("dev", "t-1", channel="dash-dev")) == "ack"
    assert lane_of(subjects.agent_telemetry("dev", "t-1", channel="dash-dev")) == "telemetry"
    # A channel literally NAMED "ack" does not turn a status subject into an ack.
    assert lane_of(subjects.agent_status("dev", "t-1", channel="ack")) == "status"


def test_lane_of_returns_none_for_non_agent_subjects() -> None:
    """Non-agent layers name no agent lane — the caller treats that as forwardable."""
    assert lane_of(subjects.hub_control("dev", "t-1")) is None
    assert lane_of(subjects.factory_runner("dev", "job-1")) is None
    assert lane_of(subjects.ob_context_pack("dev")) is None
    assert lane_of("garbage") is None


def test_noise_lanes_are_never_forwardable() -> None:
    """THE #116 acceptance criterion at the bridge boundary.

    Every ack/telemetry subject form — bare and channel-scoped — is refused;
    every coordination lane is still allowed.
    """
    for channel in (None, "dash-dev"):
        assert not is_forwardable(subjects.agent_ack("dev", "t-1", channel=channel))
        assert not is_forwardable(subjects.agent_telemetry("dev", "t-1", channel=channel))
        assert is_forwardable(subjects.agent_status("dev", "t-1", channel=channel))
        assert is_forwardable(subjects.agent_dispatch("dev", "wren", channel=channel))


def test_unparseable_subject_fails_open_and_forwards() -> None:
    """An unrecognised subject keeps its pre-#116 behavior rather than muting a reply.

    Failing CLOSED here would silently swallow real agent output the moment a new
    subject shape appeared — a worse failure than forwarding, which the existing
    plane discipline already governs.
    """
    assert is_forwardable("weird")
    assert is_forwardable("a.b.c.d.e.f.g")


async def test_tee_refuses_a_noise_subject_without_touching_discord(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """ZERO ack traffic reaches the parent Discord channel (#116 acceptance).

    The refusal happens BEFORE the adapter is even resolved, so there is no
    Discord call to fail — the assertion is on ``discord.calls``, i.e. what a
    human would actually see in the channel.
    """
    discord = RecordingDiscordAdapter(success=True)
    _install_runner(monkeypatch, FakeRunner({StubPlatform("discord"): discord}))

    for noise_subject in (
        subjects.agent_ack("dev", "t-1", channel="dash-dev"),
        subjects.agent_telemetry("dev", "t-1", channel="dash-dev"),
        subjects.agent_ack("dev", "t-1"),
    ):
        await tee_to_discord(
            config=VoiceTeeConfig(enabled=True, channels=_tee_map()),
            platform_factory=StubPlatform,
            channel_id="chan-1",
            text="ack: on it",
            source_subject=noise_subject,
        )

    assert discord.calls == []  # nothing landed in the human channel


async def test_tee_still_forwards_a_status_subject(monkeypatch: pytest.MonkeyPatch) -> None:
    """The guard is surgical: a real status reply still mirrors, scoped or not."""
    discord = RecordingDiscordAdapter(success=True)
    _install_runner(monkeypatch, FakeRunner({StubPlatform("discord"): discord}))

    await tee_to_discord(
        config=VoiceTeeConfig(enabled=True, channels=_tee_map()),
        platform_factory=StubPlatform,
        channel_id="chan-1",
        text="the visible reply",
        source_subject=subjects.agent_status("dev", "t-1", channel="dash-dev"),
    )

    assert [call["content"] for call in discord.calls] == ["the visible reply"]
