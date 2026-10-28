"""Functional tests for the show-your-work reconstruction engine (#64).

Two journal shapes are exercised:

1. The REAL thin ``TraceRecord`` shape the deployed hub actually writes
   (``fleet_orchestrator.trace.JsonlTraceSink.as_json``): coordination metadata
   ONLY — ``ts``/``task_id``/``state``/``event``/``action``/``target``/``subject``,
   with NO ``kind``/``sender``/``text_preview``/``meta``. This is the Phase-5
   reconciliation the scout flagged: PR #67's reader required a ``kind`` field
   the real record never carries, so it would have dropped 100% of real lines.
   The reader now DERIVES kind from action/event/subject.
2. A richer observer/raw shape (``text_preview``/``meta``/per-kind scalars) kept
   for forward-compat and to lock the redaction acceptance criteria from #67's
   review (BLOCKER-1/2, HIGH-1/2, MEDIUM).

Timestamps are UTC in the journal and must render in ET.
"""

from __future__ import annotations

import json

import pytest

from fleet_nats.showwork import (
    _kind_from_subject,
    parse_frame,
    reconstruct,
    reconstruct_json,
    render_markdown,
    scope_to_task,
)

TASK = "t_grad51783562393"


@pytest.mark.parametrize("env", ["dev", "prod", "staging"])
@pytest.mark.parametrize(
    ("suffix", "expected"),
    [
        ("agent.dispatch.wren", "dispatch"),
        ("agent.arbitrate.grad-5", "arbitrate"),
        ("kboard.task.created.t_1", "kboard_event"),
    ],
)
def test_kind_from_subject_env_is_opaque(env: str, suffix: str, expected: str) -> None:
    # fleet-bus #105 P3-6: env token (parts[0]) is opaque — staging/prod subjects
    # infer kind exactly like dev, so a staging trace is not blind to kind.
    assert _kind_from_subject(f"{env}.{suffix}") == expected


# ---------------------------------------------------------------------------
# REAL thin TraceRecord shape (the deployed hub's actual JSONL sink output)
# ---------------------------------------------------------------------------

_REAL_TRACE_FRAMES: list[dict[str, object]] = [
    # A dispatch bus-op record: action + subject + target, NO kind/sender/text.
    {
        "ts": "2026-07-09T02:00:09.199679+00:00",
        "task_id": TASK,
        "action": "dispatch",
        "target": "wren",
        "subject": "dev.agent.dispatch.wren",
    },
    {
        "ts": "2026-07-09T02:00:09.201000+00:00",
        "task_id": TASK,
        "action": "dispatch",
        "target": "finch",
        "subject": "dev.agent.dispatch.finch",
    },
    # A decision record: state + event + action, no subject (real thin shape).
    {
        "ts": "2026-07-09T02:00:12.976249+00:00",
        "task_id": TASK,
        "state": "coordinating",
        "event": "worker_status",
        "action": "advance",
    },
    # A relay bus-op.
    {
        "ts": "2026-07-09T02:00:12.977438+00:00",
        "task_id": TASK,
        "action": "relay",
        "target": "wren",
        "subject": "dev.agent.relay.wren",
    },
    # A wake bus-op.
    {
        "ts": "2026-07-09T02:00:29.079468+00:00",
        "task_id": TASK,
        "action": "wake",
        "target": "wren",
        "subject": "dev.agent.wake.wren",
    },
    # A DECIDE decision record: winner in target (this is how the real trace
    # records an arbitration winner — no `decision` scalar).
    {
        "ts": "2026-07-09T02:00:36.500000+00:00",
        "task_id": TASK,
        "action": "decide",
        "target": "finch",
    },
    # A NOOP decision record — must be dropped as trail noise.
    {
        "ts": "2026-07-09T02:00:40.000000+00:00",
        "task_id": TASK,
        "action": "noop",
    },
    # A complete action → outcome "completed".
    {
        "ts": "2026-07-09T02:01:14.039736+00:00",
        "task_id": TASK,
        "state": "done",
        "action": "complete",
    },
    # Off-task noise (different task) — excluded.
    {
        "ts": "2026-07-09T02:00:10.000000+00:00",
        "task_id": "t_other",
        "action": "dispatch",
        "target": "wren",
        "subject": "dev.agent.dispatch.wren",
    },
]


def _real_lines() -> list[str]:
    return [json.dumps(f) for f in _REAL_TRACE_FRAMES]


def test_real_trace_shape_parses_without_kind_field() -> None:
    # The reconciliation seam: a real TraceRecord has NO `kind`. parse_frame must
    # still yield a frame and DERIVE the kind from `action`.
    frame = parse_frame(json.dumps(_REAL_TRACE_FRAMES[0]))
    assert frame is not None
    assert frame.kind == "dispatch"  # derived from action
    assert frame.target == "wren"
    assert frame.text == ""  # the real trace carries no body — normal, not a crash


def test_real_trace_scopes_and_drops_noop_and_offtask() -> None:
    summary, frames = reconstruct(_real_lines(), TASK)
    kinds = [f.kind for f in frames]
    assert "noop" not in kinds  # NOOP dropped as trail noise
    assert all(f.task_id == TASK for f in frames)  # off-task excluded
    ts_list = [f.ts for f in frames if f.ts]
    assert ts_list == sorted(ts_list)


def test_real_trace_reconstructs_participants_decision_outcome() -> None:
    summary, _ = reconstruct(_real_lines(), TASK)
    # Participants come off the dispatch/relay/wake/decide targets (no sender).
    assert "wren" in summary.participants
    assert "finch" in summary.participants
    # DECIDE winner is recorded in the action="decide" record's target.
    assert summary.decision == "finch"
    # complete action → outcome.
    assert summary.outcome == "completed"


def test_real_trace_renders_et_and_coordination_trail() -> None:
    summary, frames = reconstruct(_real_lines(), TASK)
    md = render_markdown(summary, frames, max_chars=None).markdown
    assert md.index("## Summary") < md.index("## Transcript")
    assert "2026-07-08 22:00:09 EDT" in md  # UTC 02:00 → ET 22:00 prior day
    assert "**dispatch**" in md
    assert "**decide**" in md and "finch" in md
    assert "outcome: completed" in md


# ---------------------------------------------------------------------------
# Richer observer/raw shape — locks the #67 review acceptance criteria
# ---------------------------------------------------------------------------

_FIXTURE_FRAMES: list[dict[str, object]] = [
    {  # heartbeat — always dropped (liveness noise)
        "ts": "2026-07-09T01:59:58.716895+00:00",
        "subject": "dev.agent.heartbeat.finch",
        "kind": "heartbeat",
        "task_id": None,
        "sender": "finch",
        "id": "hb1",
    },
    {  # off-task frame — different task, excluded
        "ts": "2026-07-09T01:59:59.000000+00:00",
        "subject": "dev.agent.dispatch.wren",
        "kind": "dispatch",
        "task_id": "t_other",
        "sender": "heron",
        "id": "off1",
        "text_preview": "unrelated task",
    },
    {
        "ts": "2026-07-09T02:00:09.148173+00:00",
        "subject": f"dev.kboard.task.created.{TASK}",
        "kind": "kboard_event",
        "task_id": TASK,
        "sender": None,
        "id": "kb1",
        "event": "created",
        "text_preview": "[fleet] Fleet config-drift audit across the three agent repos.",
    },
    {
        "ts": "2026-07-09T02:00:09.199656+00:00",
        "subject": "dev.agent.inbox.heron",
        "kind": "narration",
        "task_id": TASK,
        "sender": "heron",
        "id": "n1",
        "text_preview": f"[{TASK}] dispatched to wren,finch - coordination started.",
    },
    {
        "ts": "2026-07-09T02:00:09.199679+00:00",
        "subject": "dev.agent.dispatch.wren",
        "kind": "dispatch",
        "task_id": TASK,
        "sender": "heron",
        "id": "d1",
        "text_preview": "[fleet] Fleet config-drift audit across the three agent repos.",
    },
    {
        "ts": "2026-07-09T02:00:12.977438+00:00",
        "subject": "dev.agent.relay.wren",
        "kind": "relay",
        "task_id": TASK,
        "sender": "heron",
        "id": "r1",
        "missed_from": "finch",
        "text_preview": "finished my step, results attached",
    },
    {  # a status frame carrying a SIGNED stance (dissent + fact_pull)
        "ts": "2026-07-09T02:00:35.688909+00:00",
        "subject": f"dev.agent.status.{TASK}",
        "kind": "status",
        "task_id": TASK,
        "sender": "finch",
        "id": "s1",
        "meta": {"stance": "dissent", "fact_pull": True},
        "text_preview": "signed: I dissent and I pulled a fact to back it",
    },
    {  # an agree stance
        "ts": "2026-07-09T02:00:36.189737+00:00",
        "subject": f"dev.agent.status.{TASK}",
        "kind": "status",
        "task_id": TASK,
        "sender": "wren",
        "id": "s2",
        "meta": {"stance": "agree", "fact_pull": False},
        "text_preview": "signed: I agree with the consensus",
    },
    {  # a wake ping (empty payload)
        "ts": "2026-07-09T02:00:29.079468+00:00",
        "subject": "dev.agent.wake.wren",
        "kind": "wake",
        "task_id": TASK,
        "sender": "heron",
        "id": "w1",
    },
    {  # topic-scoped arbitrate frame — no task_id, tied by topic
        "ts": "2026-07-09T02:00:13.303225+00:00",
        "subject": "dev.agent.arbitrate.grad-5",
        "kind": "arbitrate",
        "task_id": None,
        "sender": "heron",
        "id": "arb1",
        "topic": "grad-5",
        "decision": "finch",
    },
    {  # a control:pause with sender + reason
        "ts": "2026-07-09T02:00:40.000000+00:00",
        "subject": f"dev.hub.control.{TASK}",
        "kind": "control",
        "task_id": TASK,
        "sender": "rico",
        "id": "c1",
        "op": "pause",
        "text_preview": "steering: hold while I check the audit scope",
    },
    {  # a control:resume
        "ts": "2026-07-09T02:00:50.000000+00:00",
        "subject": f"dev.hub.control.{TASK}",
        "kind": "control",
        "task_id": TASK,
        "sender": "rico",
        "id": "c2",
        "op": "resume",
        "text_preview": "steering: resume, scope is fine",
    },
    {
        "ts": "2026-07-09T02:01:14.039736+00:00",
        "subject": f"dev.agent.status.{TASK}",
        "kind": "done",
        "task_id": TASK,
        "sender": "heron",
        "id": "done1",
        "outcome": "completed",
    },
]


def _lines() -> list[str]:
    return [json.dumps(f) for f in _FIXTURE_FRAMES]


# ---- reconstruction ---------------------------------------------------------


def test_reconstruct_scopes_and_orders() -> None:
    _, frames = reconstruct(_lines(), TASK, topic="grad-5")
    kinds = [f.kind for f in frames]
    assert "heartbeat" not in kinds
    assert all(f.task_id in (TASK, None) for f in frames)
    assert "arbitrate" in kinds
    ts_list = [f.ts for f in frames if f.ts]
    assert ts_list == sorted(ts_list)


def test_summary_participants_and_decision() -> None:
    summary, _ = reconstruct(_lines(), TASK, topic="grad-5")
    assert "wren" in summary.participants
    assert "finch" in summary.participants
    assert "heron" in summary.participants
    assert summary.decision == "finch"
    assert summary.outcome == "completed"


def test_stance_parsing_from_meta() -> None:
    summary, _ = reconstruct(_lines(), TASK, topic="grad-5")
    assert ("finch", "dissent", True) in summary.stances
    assert ("wren", "agree", False) in summary.stances


def test_control_ops_captured_with_sender_and_reason() -> None:
    summary, _ = reconstruct(_lines(), TASK, topic="grad-5")
    ops = summary.control_ops
    assert ("pause", "rico", "steering: hold while I check the audit scope") in ops
    assert ("resume", "rico", "steering: resume, scope is fine") in ops


def test_arbitrate_excluded_without_topic() -> None:
    _, frames = reconstruct(_lines(), TASK)  # no topic
    assert all(f.kind != "arbitrate" for f in frames)


# ---- rendering: summary first, ET timestamps --------------------------------


def test_markdown_summary_first_transcript_second_et() -> None:
    summary, frames = reconstruct(_lines(), TASK, topic="grad-5")
    md = render_markdown(summary, frames, max_chars=None).markdown
    assert md.index("## Summary") < md.index("## Transcript")
    assert "2026-07-08 22:00:09 EDT" in md
    assert "[[stance:dissent +fact_pull]]" in md
    assert "control:pause" in md
    assert "arbitrate" in md and "decision=finch" in md
    assert "outcome: completed" in md


def test_control_op_rendering_shows_who_and_why() -> None:
    summary, frames = reconstruct(_lines(), TASK, topic="grad-5")
    md = render_markdown(summary, frames, max_chars=None).markdown
    assert "control:pause** by rico" in md
    assert "hold while I check the audit scope" in md


# ---- redaction (fail-closed) ------------------------------------------------


def test_redaction_scrubs_secrets_in_render() -> None:
    frame = {
        "ts": "2026-07-09T02:00:09.000000+00:00",
        "subject": f"dev.agent.status.{TASK}",
        "kind": "status",
        "task_id": TASK,
        "sender": "wren",
        "id": "sx",
        "text_preview": (
            "here is the webhook https://discord.com/api/webhooks/123/abcDEF "
            "and token sk-ant-abcdefghijklmnop0123456789 done"
        ),
    }
    summary, frames = reconstruct([json.dumps(frame)], TASK)
    md = render_markdown(summary, frames, max_chars=None).markdown
    assert "discord.com/api/webhooks" not in md
    assert "sk-ant-abcdefghijklmnop" not in md
    assert "[REDACTED]" in md


def test_json_surface_also_redacts() -> None:
    frame = {
        "ts": "2026-07-09T02:00:09.000000+00:00",
        "subject": f"dev.agent.status.{TASK}",
        "kind": "status",
        "task_id": TASK,
        "sender": "wren",
        "id": "sx",
        "text_preview": "token=ghp_0123456789abcdefghijklmnopqrstuv secret",
    }
    summary, frames = reconstruct([json.dumps(frame)], TASK)
    doc = reconstruct_json(summary, frames)
    assert "ghp_0123456789" not in json.dumps(doc)
    assert "[REDACTED]" in json.dumps(doc)


# ---- errors + robustness ----------------------------------------------------


def test_missing_task_id_raises() -> None:
    with pytest.raises(ValueError, match="task_id must be a non-empty string"):
        scope_to_task([], "")


def test_unusable_lines_skipped_not_fatal() -> None:
    lines = ["", "not json", "[]", "{}", json.dumps(_FIXTURE_FRAMES[4])]
    summary, frames = reconstruct(lines, TASK)
    # only the one usable in-scope frame (index 4 → the dispatch to wren).
    assert summary.frame_count == 1
    assert frames[0].kind == "dispatch"


def test_parse_frame_reads_raw_payload_shape() -> None:
    # A RAW journal (full payload, not observer-redacted) must parse too.
    raw = {
        "ts": "2026-07-09T02:00:09.000000+00:00",
        "subject": f"dev.agent.status.{TASK}",
        "kind": "status",
        "task_id": TASK,
        "from": "finch",
        "id": "raw1",
        "payload": {"text": "raw body here", "meta": {"stance": "neutral", "fact_pull": False}},
    }
    frame = parse_frame(json.dumps(raw))
    assert frame is not None
    assert frame.sender == "finch"
    assert frame.text == "raw body here"
    assert frame.meta.get("stance") == "neutral"


def test_line_with_neither_task_id_nor_subject_skipped() -> None:
    # A record with no task_id and no subject cannot be placed — skipped.
    assert parse_frame(json.dumps({"ts": "2026-07-09T02:00:00+00:00", "action": "noop"})) is None


# ---- truncation -------------------------------------------------------------


def test_truncation_notes_and_pointer() -> None:
    many = []
    for i in range(200):
        many.append(
            {
                "ts": f"2026-07-09T02:{i // 60:02d}:{i % 60:02d}.000000+00:00",
                "subject": f"dev.agent.status.{TASK}",
                "kind": "status",
                "task_id": TASK,
                "sender": "wren",
                "id": f"m{i}",
                "text_preview": f"status update number {i} with some padding text here",
            }
        )
    summary, frames = reconstruct([json.dumps(f) for f in many], TASK)
    rendered = render_markdown(summary, frames, max_chars=1500, full_output_path="/tmp/full.md")
    assert rendered.truncated is True
    assert rendered.full_len > 1500
    assert len(rendered.markdown) <= 1500
    assert "truncated" in rendered.markdown.lower()
    assert "/tmp/full.md" in rendered.markdown


def test_no_truncation_under_cap() -> None:
    summary, frames = reconstruct(_lines(), TASK, topic="grad-5")
    rendered = render_markdown(summary, frames, max_chars=100_000)
    assert rendered.truncated is False
    assert "truncated" not in rendered.markdown.lower()


# ---- HIGH-1: absolute cap floor ---------------------------------------------


def test_tiny_cap_honored_absolutely() -> None:
    # A cap smaller than the fixed header+note overhead must STILL be honored —
    # the returned markdown never exceeds max_chars (correctness/test-cov HIGH-1).
    summary, frames = reconstruct(_lines(), TASK, topic="grad-5")
    for cap in (10, 50, 120, 300):
        rendered = render_markdown(summary, frames, max_chars=cap, full_output_path="/tmp/f.md")
        assert len(rendered.markdown) <= cap, f"cap {cap} violated: {len(rendered.markdown)}"
        assert rendered.truncated is True


# ---- HIGH-2: fail-closed redaction is FORCED --------------------------------


def _raise(_text: str) -> str:
    raise RuntimeError("boom")


def test_fail_closed_marker_in_markdown(monkeypatch: pytest.MonkeyPatch) -> None:
    import fleet_nats.showwork as sw

    monkeypatch.setattr(sw, "redact_text", _raise)
    summary, frames = reconstruct(_lines(), TASK, topic="grad-5")
    md = render_markdown(summary, frames, max_chars=None).markdown
    # The raw dissent/agree prose must NOT appear; the marker must.
    assert "I dissent and I pulled a fact" not in md
    assert "[redacted-frame]" in md


def test_fail_closed_marker_in_json(monkeypatch: pytest.MonkeyPatch) -> None:
    import fleet_nats.showwork as sw

    monkeypatch.setattr(sw, "redact_text", _raise)
    summary, frames = reconstruct(_lines(), TASK, topic="grad-5")
    doc = reconstruct_json(summary, frames)
    blob = json.dumps(doc)
    assert "I dissent and I pulled a fact" not in blob
    assert "[redacted-frame]" in blob


def test_fail_closed_control_reason(monkeypatch: pytest.MonkeyPatch) -> None:
    import fleet_nats.showwork as sw

    monkeypatch.setattr(sw, "redact_text", _raise)
    summary, frames = reconstruct(_lines(), TASK, topic="grad-5")
    doc = reconstruct_json(summary, frames)
    reasons = [op["reason"] for op in doc["summary"]["control_ops"]]  # type: ignore[index]
    assert "steering: hold while I check the audit scope" not in reasons
    assert all(r == "[redacted-frame]" for r in reasons if r is not None)


# ---- BLOCKER-1: extra scalars redacted in markdown --------------------------


def test_extra_scalar_leak_redacted_markdown() -> None:
    frame = {
        "ts": "2026-07-09T02:00:09.000000+00:00",
        "subject": f"dev.agent.status.{TASK}",
        "kind": "done",
        "task_id": TASK,
        "sender": "heron",
        "id": "leak1",
        "outcome": "completed https://discord.com/api/webhooks/1/secretHook",
    }
    summary, frames = reconstruct([json.dumps(frame)], TASK)
    md = render_markdown(summary, frames, max_chars=None).markdown
    assert "discord.com/api/webhooks" not in md
    assert "[REDACTED]" in md


# ---- BLOCKER-2: meta/extra redacted in json ---------------------------------


def test_meta_token_leak_redacted_json() -> None:
    frame = {
        "ts": "2026-07-09T02:00:09.000000+00:00",
        "subject": f"dev.agent.status.{TASK}",
        "kind": "status",
        "task_id": TASK,
        "sender": "finch",
        "id": "leak2",
        "meta": {
            "stance": "dissent",
            "fact_pull": True,
            "note": "key sk-ant-abcdef0123456789ghijkl",
        },
    }
    summary, frames = reconstruct([json.dumps(frame)], TASK)
    blob = json.dumps(reconstruct_json(summary, frames))
    assert "sk-ant-abcdef0123456789" not in blob
    assert "[REDACTED]" in blob


def test_extra_webhook_leak_redacted_json() -> None:
    frame = {
        "ts": "2026-07-09T02:00:09.000000+00:00",
        "subject": f"dev.agent.status.{TASK}",
        "kind": "done",
        "task_id": TASK,
        "sender": "heron",
        "id": "leak3",
        "outcome": "done https://hooks.slack.com/services/T/B/XXXX",
    }
    summary, frames = reconstruct([json.dumps(frame)], TASK)
    blob = json.dumps(reconstruct_json(summary, frames))
    assert "hooks.slack.com/services" not in blob
    assert "[REDACTED]" in blob


def test_fail_closed_meta_structure(monkeypatch: pytest.MonkeyPatch) -> None:
    import fleet_nats.showwork as sw

    monkeypatch.setattr(sw, "redact_text", _raise)
    frame = {
        "ts": "2026-07-09T02:00:09.000000+00:00",
        "subject": f"dev.agent.status.{TASK}",
        "kind": "status",
        "task_id": TASK,
        "sender": "finch",
        "id": "leak4",
        "meta": {"stance": "dissent", "note": "raw token abcXYZ123"},
    }
    summary, frames = reconstruct([json.dumps(frame)], TASK)
    blob = json.dumps(reconstruct_json(summary, frames))
    assert "raw token abcXYZ123" not in blob
    assert "[redacted-frame]" in blob


# ---- MEDIUMs ----------------------------------------------------------------


def test_unknown_kind_renders_via_catch_all() -> None:
    frame = {
        "ts": "2026-07-09T02:00:09.000000+00:00",
        "subject": f"dev.agent.mystery.{TASK}",
        "kind": "mystery_kind",
        "task_id": TASK,
        "sender": "wren",
        "id": "u1",
        "text_preview": "some future frame",
    }
    summary, frames = reconstruct([json.dumps(frame)], TASK)
    md = render_markdown(summary, frames, max_chars=None).markdown
    assert "mystery_kind" in md
    assert "some future frame" in md


def test_wrong_topic_excludes_other_topic_arbitrate() -> None:
    # A run-9 pull must NOT bleed in a grad-5 arbitrate frame (cross-run guard).
    _, frames = reconstruct(_lines(), TASK, topic="grad-9")
    assert all(f.kind != "arbitrate" for f in frames)


def test_empty_summary_no_indexerror() -> None:
    from fleet_nats.showwork import build_summary

    summary = build_summary(TASK, [])
    assert summary.frame_count == 0
    assert summary.started_et is None
    assert summary.ended_et is None
    assert summary.participants == ()


# ---- LOWs -------------------------------------------------------------------


def test_est_winter_render() -> None:
    from fleet_nats.showwork import _to_et

    # January → EST (-05:00). 15:00 UTC → 10:00 EST.
    assert _to_et("2026-01-15T15:00:00+00:00") == "2026-01-15 10:00:00 EST"


def test_naive_ts_assumed_utc() -> None:
    from fleet_nats.showwork import _to_et

    # A naive ts (no offset) is treated as UTC, not machine-local. 02:00 → 22:00
    # EDT the prior day — deterministic regardless of the reader's box TZ.
    assert _to_et("2026-07-09T02:00:09") == "2026-07-08 22:00:09 EDT"


def test_offset_ts_from_real_hub_journal() -> None:
    from fleet_nats.showwork import _to_et

    # Tonight's real hub journal writes ET-offset timestamps (`-04:00`). A ts that
    # already carries -04:00 renders at that wall-clock in ET.
    assert _to_et("2026-07-09T01:15:12-04:00") == "2026-07-09 01:15:12 EDT"


def test_decision_narration_scrape_fallback() -> None:
    # No arbitrate.decision extra — only a narration "decided for X" beat.
    frames = [
        {
            "ts": "2026-07-09T02:00:09.000000+00:00",
            "subject": "dev.agent.inbox.heron",
            "kind": "narration",
            "task_id": TASK,
            "sender": "heron",
            "id": "nd1",
            "text_preview": f"[{TASK}] decided for wren (evidenced position).",
        }
    ]
    summary, _ = reconstruct([json.dumps(f) for f in frames], TASK)
    assert summary.decision == "wren"


# ---- Golden fixture (REAL hub-journal shape drift anchor) --------------------


def test_golden_fixture_reconstructs() -> None:
    from pathlib import Path

    fixture = Path(__file__).parent / "fixtures" / "golden-bus-frames.jsonl"
    lines = fixture.read_text(encoding="utf-8").splitlines()
    summary, frames = reconstruct(lines, "t_grad51783562393")
    assert summary.frame_count > 0
    kinds = {f.kind for f in frames}
    # The golden fixture is the REAL thin TraceRecord shape (action-derived kinds).
    assert {"dispatch", "relay", "wake", "arbitrate", "decide"} <= kinds
    arbitrate_line = next(line for line in lines if json.loads(line).get("action") == "arbitrate")
    arbitrate = parse_frame(arbitrate_line)
    assert arbitrate is not None
    assert arbitrate.kind == "arbitrate"
    assert arbitrate.task_id == "t_grad51783562393"
    assert arbitrate.subject == ""
    assert arbitrate.extra == {
        "event": "arbitrate_requested",
        "state": "arbitrating",
        "action": "arbitrate",
    }
    assert summary.decision == "finch"
    assert summary.outcome == "completed"
    md = render_markdown(summary, frames, max_chars=None).markdown
    assert md.index("## Summary") < md.index("## Transcript")
    assert "EDT" in md
    # The run/task ids are the receipt's value — they must SURVIVE redaction.
    assert "t_grad51783562393" in md
