"""Unit tests for the pure envelope → synthetic-message mapping (no gateway)."""

from __future__ import annotations

from fleet_nats import ChannelMap, Envelope, empty_map, parse_portals

from fleet_subscriber import SyntheticMessage, envelope_to_message


def _env(**kw: object) -> Envelope:
    base: dict[str, object] = dict(
        msg_id="m1", ts="t", sender="nagatha", kind="dispatch", task_id="task-1"
    )
    base.update(kw)
    return Envelope.new(**base)  # type: ignore[arg-type]


def test_dispatch_envelope_maps_to_message() -> None:
    env = _env(channel="dash-dev", payload={"text": "Investigate the fleet-hub outage."})
    msg = envelope_to_message(env)
    assert isinstance(msg, SyntheticMessage)
    assert msg.text == "Investigate the fleet-hub outage."
    assert msg.chat_id == "dash-dev"
    assert msg.chat_type == "channel"
    assert msg.thread_id is None
    assert msg.task_id == "task-1"
    assert msg.kind == "dispatch"
    assert msg.user_id == "fleet-nats"


def test_thread_id_in_payload_makes_a_thread_message() -> None:
    env = _env(channel="dash-dev", payload={"text": "reply here", "thread_id": "th-9"})
    msg = envelope_to_message(env)
    assert msg is not None
    assert msg.thread_id == "th-9"
    assert msg.chat_type == "thread"


def test_missing_text_is_skipped() -> None:
    env = _env(channel="dash-dev", payload={"no_text": "x"})
    assert envelope_to_message(env) is None


def test_blank_text_is_skipped() -> None:
    env = _env(channel="dash-dev", payload={"text": "   "})
    assert envelope_to_message(env) is None


def test_missing_channel_without_default_is_skipped() -> None:
    env = _env(payload={"text": "no channel here"})
    assert envelope_to_message(env) is None


def test_missing_channel_falls_back_to_the_home_channel() -> None:
    env = _env(payload={"text": "use my home channel"})
    msg = envelope_to_message(env, home_channel="agent-home")
    assert msg is not None
    assert msg.chat_id == "agent-home"


def test_envelope_channel_wins_over_the_home_channel() -> None:
    env = _env(channel="explicit", payload={"text": "hi"})
    msg = envelope_to_message(env, home_channel="fallback")
    assert msg is not None
    assert msg.chat_id == "explicit"


# --- #118: channel resolution runs through the persisted portal map -----------


def _channels() -> ChannelMap:
    return parse_portals(
        [
            {"slug": "arena", "discord_id": "1111"},
            {"slug": "fleet-ops", "discord_id": "2222"},
        ]
    )


def test_each_envelope_keeps_its_own_channel() -> None:
    """Two envelopes in two channels forge turns in two DIFFERENT chats.

    A per-process default cannot express this; the map can.
    """
    a = envelope_to_message(_env(channel="arena", payload={"text": "a"}), channels=_channels())
    b = envelope_to_message(_env(channel="fleet-ops", payload={"text": "b"}), channels=_channels())
    assert a is not None and b is not None
    assert a.chat_id == "arena"
    assert b.chat_id == "fleet-ops"


def test_a_discord_id_canonicalises_to_its_slug() -> None:
    """One room must yield ONE chat_id whichever spelling arrives.

    The adapter keys its ``chat_id -> task_id`` correlation on this value, so a
    dispatch stamped with the id and a reply routed by the slug have to agree.
    Without the map both spellings pass through untouched and the correlation is
    lost — which is exactly what happened before #118.
    """
    by_slug = envelope_to_message(
        _env(channel="arena", payload={"text": "x"}), channels=_channels()
    )
    by_id = envelope_to_message(_env(channel="1111", payload={"text": "x"}), channels=_channels())
    assert by_slug is not None and by_id is not None
    assert by_slug.chat_id == by_id.chat_id == "arena"


def test_the_home_channel_is_canonicalised_through_the_map_too() -> None:
    """A home channel configured as a raw id still resolves to the slug."""
    msg = envelope_to_message(
        _env(payload={"text": "no channel"}), channels=_channels(), home_channel="2222"
    )
    assert msg is not None
    assert msg.chat_id == "fleet-ops"


def test_an_unmapped_channel_passes_through_rather_than_dropping_the_turn() -> None:
    """The map translates; it does not gate intake.

    An unreachable k-board (empty map) must not stop an explicitly addressed turn
    from running — the OUTBOUND leg is where an unmapped channel is refused.
    """
    msg = envelope_to_message(
        _env(channel="brand-new", payload={"text": "still runs"}), channels=empty_map()
    )
    assert msg is not None
    assert msg.chat_id == "brand-new"


def test_custom_synthetic_user_id() -> None:
    env = _env(channel="c", payload={"text": "hi"})
    msg = envelope_to_message(env, synthetic_user_id="bus-forge")
    assert msg is not None
    assert msg.user_id == "bus-forge"


def test_narration_envelope_maps_to_an_announce_instruction_not_a_bare_prompt() -> None:
    # A #13 narration beat must never reach the LLM as a raw string it would
    # *react* to — the mapping wraps it in the fixed announce instruction.
    beat = "[task-1] PAUSED over budget (msgs 4 > cap 3) — human resume required."
    env = _env(kind="narration", channel="dash-dev", payload={"text": beat})
    msg = envelope_to_message(env)
    assert msg is not None
    assert msg.text != beat  # wrapped, never the bare beat
    assert beat in msg.text
    assert "Announce this status" in msg.text
    assert msg.kind == "narration"


def test_non_narration_kinds_are_never_wrapped() -> None:
    env = _env(kind="dispatch", channel="dash-dev", payload={"text": "Do the thing."})
    msg = envelope_to_message(env)
    assert msg is not None
    assert msg.text == "Do the thing."
