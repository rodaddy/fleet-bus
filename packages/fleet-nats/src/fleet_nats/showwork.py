"""show-your-work — scoped deliberation-transcript reconstruction (#64).

Pull-only, read-only reconstruction of a single task's coordination trail from
the hub's per-task frame journal. The fleet's ratified model (2026-07-08, all
three agents unanimous): "digest for the room, transcript for the referee" — the
answer to *why-did-you-do-that* is *because the work is available, ask for it*.

This module is the pure engine. It never touches the network and never calls a
wall clock for reconstruction (timestamps come from the journal). It DISPLAYS a
coordination record; :mod:`fleet_nats.redact` scrubs credential-shaped substrings
from every rendered surface so a trail is safe to share.

The journal it actually reads
-----------------------------
The hub's JSONL trace sink (``fleet_orchestrator.trace.JsonlTraceSink``, enabled
by ``FLEET_TRACE_PATH``) appends one ``TraceRecord`` per coordination decision
and per bus operation. That record is DELIBERATELY THIN — coordination metadata
ONLY, never message bodies/secrets/webhook URLs (``trace.py:9-11`` negative
space). Its on-disk fields (``None`` omitted) are::

    ts        ISO-8601 timestamp (injected clock)
    task_id   the coordination task
    state     resulting session state       (decision records)
    event     inbound event kind            (decision records)
    action    action kind decided/performed (dispatch|relay|wake|complete|
                                              decide|synthesize|noop|...)
    target    the agent the action is directed at, when applicable
    subject   the NATS subject              (bus-operation records)

CRITICAL reconciliation (2026-07-09, real gate1/gate2 journals): the deployed
``TraceRecord`` is far thinner than a rich "observer" frame. It carries NO
``kind`` field, NO ``sender``, NO ``text_preview``/body, NO ``meta``, and no
per-kind scalars (``outcome``/``topic``/``decision``/``missed_from``). So this
reader:

* DERIVES ``kind`` — if a line has no ``kind`` it is synthesized from ``action``
  (a bus op) else from ``event`` (a decision record) else ``"event"``. A
  ``kboard_event`` envelope (``kind`` present) is honored as-is.
* renders a COORDINATION trail (who/what/when/decision/outcome) from ``action``/
  ``event``/``target``/``state`` — it does NOT require a message body, because
  the real trace never writes one. When a richer journal DOES carry
  ``text_preview``/``meta`` (an observer-side journal, or a raw full-payload
  journal), those are used and redacted; but their absence is normal, not an
  error.

Scoping
-------
A frame belongs to the trail when its ``task_id`` matches, OR it is a
topic-scoped arbitrate frame whose ``topic`` matches the run topic the caller
supplied (an arbitrate frame may carry no task_id — it is topic-scoped).
Unlinked arbitrate frames are surfaced only when the caller supplies the run's
topic; proximity is never used. Heartbeats are always dropped (liveness noise).
"""

from __future__ import annotations

import json
from collections.abc import Iterable, Iterator
from dataclasses import dataclass, field
from datetime import UTC, datetime
from zoneinfo import ZoneInfo

from loguru import logger

from fleet_nats.redact import redact_text

# Rico reads ET only — every rendered timestamp is America/New_York.
_ET = ZoneInfo("America/New_York")

# Frame kinds that never belong in a deliberation trail (liveness noise).
_DROP_KINDS = frozenset({"heartbeat"})

# Action kinds that are pure no-ops on the trail (a NOOP decision records a
# non-action; surfacing it as a transcript line is noise, not signal).
_DROP_ACTIONS = frozenset({"noop"})


@dataclass(frozen=True, slots=True)
class Frame:
    """One parsed journal frame, normalized across thin-trace/observer/raw shapes.

    Attributes:
        ts: ISO-8601 timestamp string as written (``None`` on legacy lines).
        subject: The NATS subject the frame rode (``""`` when the record is a
            decision record that carries no subject — real thin trace).
        kind: The frame kind (DERIVED from ``action``/``event`` when the record
            carries no explicit ``kind`` — the real thin trace shape).
        task_id: The task this frame belongs to (``None`` for topic/heartbeat).
        sender: Publisher agent id (``None`` when the journal did not record it —
            the real thin trace never does).
        msg_id: The message id (``None`` when absent).
        text: The rendered text (preview when present, else full payload.text),
            BEFORE redaction. Empty string when the frame carries no text — the
            NORMAL case for the real thin trace.
        meta: The stance/decision meta dict (empty when absent — the normal case).
        extra: Kind-specific scalars kept verbatim (``missed_from``, ``outcome``,
            ``topic``, ``decision``, ``event``, ``status``, ``op``, ``state``,
            ``action``, ``target``).
    """

    ts: str | None
    subject: str
    kind: str
    task_id: str | None
    sender: str | None
    msg_id: str | None
    text: str
    meta: dict[str, object] = field(default_factory=dict)
    extra: dict[str, object] = field(default_factory=dict)

    @property
    def target(self) -> str | None:
        """The agent the frame is directed at.

        Prefer an explicit ``target`` scalar (the real thin trace records it on
        dispatch/relay/wake/decide). Otherwise read it off the subject's last
        token for the directed bus kinds. Status frames' last subject token is a
        task id, not an agent, so those fall through to ``None``.
        """
        explicit = self.extra.get("target")
        if isinstance(explicit, str) and explicit:
            return explicit
        if self.kind in ("dispatch", "relay", "wake") and self.subject:
            return self.subject.rsplit(".", 1)[-1]
        return None


def _extract_text(obj: dict[str, object]) -> str:
    """Pull a renderable text out of an observer-shaped OR raw-shaped frame.

    Prefer the observer's ``text_preview`` (already truncated); fall back to a raw
    ``payload.text`` when the journal kept the full payload. The real thin trace
    carries NEITHER — it returns ``""`` (no body), which is the normal case, not
    a crash.
    """
    preview = obj.get("text_preview")
    if isinstance(preview, str):
        return preview
    payload = obj.get("payload")
    if isinstance(payload, dict):
        text = payload.get("text")
        if isinstance(text, str):
            return text
    return ""


def _extract_meta(obj: dict[str, object]) -> dict[str, object]:
    """Read the stance/decision meta from either shape (top-level or payload)."""
    meta = obj.get("meta")
    if isinstance(meta, dict):
        return dict(meta)
    payload = obj.get("payload")
    if isinstance(payload, dict):
        pmeta = payload.get("meta")
        if isinstance(pmeta, dict):
            return dict(pmeta)
    return {}


# Kind-specific scalar fields lifted into ``extra`` verbatim (never freeform
# text). Includes the real thin-trace scalars ``state``/``action``/``target``
# alongside the richer observer scalars.
_EXTRA_KEYS = (
    "missed_from",
    "outcome",
    "topic",
    "decision",
    "event",
    "status",
    "op",
    "state",
    "action",
    "target",
)


def _extract_extra(obj: dict[str, object]) -> dict[str, object]:
    """Lift the kind-specific scalar fields (never freeform text) into extra."""
    extra: dict[str, object] = {}
    for key in _EXTRA_KEYS:
        if key in obj and obj[key] is not None:
            extra[key] = obj[key]
    payload = obj.get("payload")
    if isinstance(payload, dict):
        for key in _EXTRA_KEYS:
            if key not in extra and payload.get(key) is not None:
                extra[key] = payload[key]
    return extra


# Map a bus/decision subject prefix onto a frame kind, when no explicit kind and
# no action is present (belt-and-braces for a subject-only bus-op line).
def _kind_from_subject(subject: str) -> str | None:
    """Best-effort kind from a NATS subject's shape (``{env}.agent.<kind>.*``)."""
    parts = subject.split(".")
    # {env}.agent.dispatch.skippy -> "dispatch"; {env}.kboard.task.created.* ->
    # "kboard_event"; {env}.agent.arbitrate.<topic> -> "arbitrate".
    # The env token (parts[0]) is OPAQUE — dev/prod/staging all key the same way,
    # so a staging subject is not blind to kind inference (fleet-bus #105 P3-6).
    if len(parts) >= 3:
        if parts[1] == "agent" and parts[2] in (
            "dispatch",
            "relay",
            "wake",
            "arbitrate",
            "status",
            "control",
        ):
            return parts[2]
        if parts[1] == "kboard":
            return "kboard_event"
    return None


def _derive_kind(obj: dict[str, object], subject: str) -> str:
    """Derive the frame kind for a record that carries no explicit ``kind``.

    Precedence: explicit ``kind`` > ``action`` (a bus-op / decision) > the
    subject's shape > ``event`` (a decision record's inbound event) > ``"event"``.
    This is the reconciliation seam: the real ``TraceRecord`` never writes
    ``kind``, so the reader synthesizes it from ``action``/``event``/subject.
    """
    kind = obj.get("kind")
    if isinstance(kind, str) and kind:
        return kind
    action = obj.get("action")
    if isinstance(action, str) and action:
        return action
    if subject:
        by_subject = _kind_from_subject(subject)
        if by_subject:
            return by_subject
    event = obj.get("event")
    if isinstance(event, str) and event:
        return event
    return "event"


def parse_frame(line: str) -> Frame | None:
    """Parse one journal line into a :class:`Frame`, or ``None`` if unusable.

    A blank line, a non-JSON line, or a JSON value that is not an object is
    skipped with a debug log (a corrupt journal line must never sink the whole
    reconstruction — the journal is append-only and may hold a torn final line).

    A usable record needs a ``task_id`` OR a ``subject`` to place it on a trail
    (the real thin ``TraceRecord`` always carries ``task_id`` and often no
    ``subject``; a richer observer frame always carries ``subject``). The
    ``kind`` is DERIVED when absent (real trace) rather than required — requiring
    ``kind`` was the #67 reader's fatal mismatch: it would drop 100% of real
    ``TraceRecord`` lines.
    """
    stripped = line.strip()
    if not stripped:
        return None
    try:
        obj = json.loads(stripped)
    except json.JSONDecodeError as exc:
        logger.debug("showwork.parse | skipping unparseable journal line: {}", exc)
        return None
    if not isinstance(obj, dict):
        logger.debug("showwork.parse | skipping non-object journal line")
        return None
    subject_raw = obj.get("subject")
    subject = subject_raw if isinstance(subject_raw, str) else ""
    task_id_raw = obj.get("task_id")
    task_id = task_id_raw if isinstance(task_id_raw, str) else None
    if not subject and task_id is None:
        logger.debug("showwork.parse | skipping frame with no task_id and no subject")
        return None
    kind = _derive_kind(obj, subject)
    sender = obj.get("sender") or obj.get("from")
    return Frame(
        ts=obj.get("ts") if isinstance(obj.get("ts"), str) else None,
        subject=subject,
        kind=kind,
        task_id=task_id,
        sender=sender if isinstance(sender, str) else None,
        msg_id=obj.get("id") if isinstance(obj.get("id"), str) else None,
        text=_extract_text(obj),
        meta=_extract_meta(obj),
        extra=_extract_extra(obj),
    )


def iter_frames(lines: Iterable[str]) -> Iterator[Frame]:
    """Parse an iterable of journal lines, skipping the unusable ones."""
    for line in lines:
        frame = parse_frame(line)
        if frame is not None:
            yield frame


def scope_to_task(
    frames: Iterable[Frame], task_id: str, *, topic: str | None = None
) -> list[Frame]:
    """Filter frames down to a single task's deliberation trail.

    A frame is in-scope when its ``task_id`` matches. Topic-scoped arbitrate
    frames may carry no task_id; they are included ONLY when the caller supplies
    the run ``topic`` and the frame's ``extra['topic']`` matches it — never by
    proximity guessing. Heartbeats and NOOP-action records are always dropped.

    The result is chronological by ``ts`` (frames with no ts sort last, stable).

    Raises:
        ValueError: If ``task_id`` is empty/whitespace.
    """
    if not task_id or not task_id.strip():
        raise ValueError("scope_to_task: task_id must be a non-empty string")
    task_id = task_id.strip()
    kept: list[Frame] = []
    for frame in frames:
        if frame.kind in _DROP_KINDS:
            continue
        if frame.extra.get("action") in _DROP_ACTIONS or frame.kind in _DROP_ACTIONS:
            continue
        if frame.task_id == task_id:
            kept.append(frame)
        elif topic and frame.kind == "arbitrate" and frame.extra.get("topic") == topic:
            kept.append(frame)
    # ts is ISO-8601 so a lexical sort == chronological. Missing ts sorts last via
    # a high sentinel; stable so equal-ts frames keep journal order.
    kept.sort(key=lambda f: (f.ts is None, f.ts or ""))
    return kept


# ---- Summary reconstruction -------------------------------------------------


@dataclass(frozen=True, slots=True)
class Summary:
    """The who/what/when/decision header block (rendered first).

    Attributes:
        task_id: The task the trail belongs to.
        participants: Sorted agent ids that dispatched-to / sent / were relayed.
        started_et: First frame's timestamp in ET (``None`` if none).
        ended_et: Last frame's timestamp in ET (``None`` if none).
        frame_count: Number of in-scope frames.
        decision: The DECIDE winner (``None`` when the task never arbitrated).
        stances: Ordered (sender, stance, fact_pull) tuples parsed from meta.
        outcome: The done-sentinel outcome (``None`` when unfinished).
        control_ops: Ordered (op, sender, reason) control ops (pauses/resumes).
    """

    task_id: str
    participants: tuple[str, ...]
    started_et: str | None
    ended_et: str | None
    frame_count: int
    decision: str | None
    stances: tuple[tuple[str, str, bool], ...]
    outcome: str | None
    control_ops: tuple[tuple[str, str | None, str | None], ...]


def _participants(frames: list[Frame]) -> tuple[str, ...]:
    """Every agent that took part — dispatch/relay/wake targets + frame senders."""
    seen: set[str] = set()
    for frame in frames:
        target = frame.target
        if target:
            seen.add(target)
        if frame.sender:
            seen.add(frame.sender)
    seen.discard("")
    return tuple(sorted(seen))


def _decision(frames: list[Frame]) -> str | None:
    """The arbitration/DECIDE winner (last decision wins), else ``None``.

    Read off (a) an explicit ``decision`` extra (observer arbitrate frame), (b) a
    real thin-trace ``decide``-action record's ``target`` (the winner the hub
    recorded), or (c) a narration beat ``decided for X (evidenced position).``.
    """
    winner: str | None = None
    for frame in frames:
        decided = frame.extra.get("decision")
        if isinstance(decided, str) and decided:
            winner = decided
        # Real thin trace: a DECIDE decision record is action="decide" with the
        # winner in target.
        if frame.extra.get("action") == "decide":
            tgt = frame.extra.get("target")
            if isinstance(tgt, str) and tgt:
                winner = tgt
        if frame.kind == "narration" and "decided for " in frame.text:
            tail = frame.text.split("decided for ", 1)[1]
            name = tail.split(" ", 1)[0].strip().rstrip(".")
            if name:
                winner = name
    return winner


def _stances(frames: list[Frame]) -> tuple[tuple[str, str, bool], ...]:
    """Ordered signed stances parsed off status-frame meta (sender, stance, fp).

    The real thin trace carries no ``meta``, so this is empty for a pure-trace
    journal; it populates only when a richer observer/raw journal is read.
    """
    out: list[tuple[str, str, bool]] = []
    for frame in frames:
        stance = frame.meta.get("stance")
        if isinstance(stance, str) and stance:
            fact_pull = bool(frame.meta.get("fact_pull"))
            out.append((frame.sender or "?", stance, fact_pull))
    return tuple(out)


def _outcome(frames: list[Frame]) -> str | None:
    """The task outcome (last-wins), from a done frame or a complete action.

    Recognizes (a) an observer ``done`` frame's ``outcome`` extra and (b) the
    real thin trace's ``complete`` action / done sentinel. Absent → ``None``.
    """
    outcome: str | None = None
    for frame in frames:
        if frame.kind == "done":
            got = frame.extra.get("outcome")
            outcome = got if isinstance(got, str) and got else "completed"
        elif frame.extra.get("action") == "complete" or frame.kind == "complete":
            outcome = "completed"
    return outcome


def _control_ops(frames: list[Frame]) -> tuple[tuple[str, str | None, str | None], ...]:
    """Ordered control ops (op, sender, reason) — pauses/resumes with who/why."""
    out: list[tuple[str, str | None, str | None]] = []
    for frame in frames:
        if frame.kind != "control":
            continue
        op = frame.extra.get("op")
        op_str = op if isinstance(op, str) and op else "control"
        reason = frame.text or None
        out.append((op_str, frame.sender, reason))
    return tuple(out)


def _to_et(ts: str | None) -> str | None:
    """Render an ISO-8601 timestamp in ET; ``None``/unparseable → ``None``.

    A NAIVE timestamp (no offset — some legacy journal lines) is treated as UTC
    EXPLICITLY, not machine-local: the journal is written with UTC clocks on the
    hub, so a bare ``astimezone`` would silently reinterpret it against whatever
    TZ the reader's box happens to run in and shift the whole trail. Assuming UTC
    keeps the render deterministic regardless of where ``show_work`` is invoked.
    A ts that already carries an offset (e.g. the hub's ``-04:00`` gate journals)
    is converted honestly.

    Never raises — a malformed ts degrades to ``None`` rather than sinking the
    render (the trail is more valuable than one clean cell).
    """
    if not ts:
        return None
    try:
        dt = datetime.fromisoformat(ts)
    except ValueError:
        logger.debug("showwork.to_et | unparseable ts {!r}", ts)
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    et = dt.astimezone(_ET)
    return et.strftime("%Y-%m-%d %H:%M:%S %Z")


def build_summary(task_id: str, frames: list[Frame]) -> Summary:
    """Reconstruct the who/what/when/decision header from the in-scope frames."""
    started = frames[0].ts if frames else None
    ended = frames[-1].ts if frames else None
    return Summary(
        task_id=task_id,
        participants=_participants(frames),
        started_et=_to_et(started),
        ended_et=_to_et(ended),
        frame_count=len(frames),
        decision=_decision(frames),
        stances=_stances(frames),
        outcome=_outcome(frames),
        control_ops=_control_ops(frames),
    )


# ---- Rendering --------------------------------------------------------------


_REDACTED_MARKER = "[redacted-frame]"


def _redact(text: str) -> str:
    """Fail-closed redaction wrapper: on ANY scrub error, drop the whole text.

    The contract (issue #64): never leak. A scrub that raises must NOT fall
    through to the raw text — it returns a ``[redacted-frame]`` marker so the
    value is omitted rather than rendered unscrubbed.
    """
    try:
        return redact_text(text)
    except Exception as exc:  # noqa: BLE001 — fail closed: a scrub error must never leak
        logger.warning("showwork.redact | scrub failed, omitting value: {}", exc)
        return _REDACTED_MARKER


def _r(value: object) -> str:
    """Coerce ANY interpolated scalar to a redacted string, fail-closed.

    Every non-text field that reaches a rendered surface — a done ``outcome``, an
    arbitrate ``topic``/``decision``, a relay ``missed_from``, a control op word,
    a kboard ``event``, a ``target`` agent — is trusted-agent-authored and could
    carry a leaked credential exactly like ``frame.text``. It must go through the
    SAME scrub, not be interpolated raw (correctness-reviewer BLOCKER-1). ``None``
    renders empty.
    """
    if value is None:
        return ""
    try:
        return redact_text(str(value))
    except Exception as exc:  # noqa: BLE001 — fail closed on the scalar path too
        logger.warning("showwork.redact | scalar scrub failed, omitting: {}", exc)
        return _REDACTED_MARKER


def _redact_obj(value: object) -> object:
    """Recursively scrub every string in a meta/extra structure, fail-closed.

    ``reconstruct_json`` emits ``meta`` and ``extra`` — a ``meta.note`` or a
    nested ``extra`` value could carry a token (correctness-reviewer BLOCKER-2:
    the JSON surface must never be a redaction bypass). Walk dicts/lists and scrub
    every string leaf through the fail-closed wrapper; non-strings pass through.
    On any structural error the WHOLE value collapses to the marker rather than
    leak a half-scrubbed structure.
    """
    try:
        if isinstance(value, str):
            return _redact(value)
        if isinstance(value, dict):
            return {k: _redact_obj(v) for k, v in value.items()}
        if isinstance(value, list):
            return [_redact_obj(v) for v in value]
        return value
    except Exception as exc:  # noqa: BLE001 — fail closed: never emit a partial scrub
        logger.warning("showwork.redact | structure scrub failed, omitting: {}", exc)
        return _REDACTED_MARKER


def _render_frame_line(frame: Frame) -> str:
    """One transcript line for a frame — coordination facts + redacted values.

    EVERY interpolated value that could be agent-authored — the text body (often
    empty for the real thin trace), plus the extra scalars (``missed_from``,
    ``topic``, ``decision``, ``outcome``, ``event``, ``op``, ``target``) and the
    stance meta — is scrubbed through the fail-closed wrapper (``_r`` / ``_redact``).
    ``et`` and the frame kind are engine-controlled and safe.
    """
    et = _to_et(frame.ts) or "??"
    who = _r(frame.sender) or "?"
    target = frame.target
    arrow = f" -> {_r(target)}" if target else ""
    body = _redact(frame.text) if frame.text else ""

    if frame.kind == "dispatch":
        return f"- `{et}` **dispatch** {who}{arrow}: {body}".rstrip()
    if frame.kind == "relay":
        missed = frame.extra.get("missed_from")
        miss = f" (missed handoff from {_r(missed)})" if missed else ""
        return f"- `{et}` **relay**{arrow}{miss}: {body}".rstrip()
    if frame.kind == "wake":
        return f"- `{et}` **wake**{arrow} (quiet too long)"
    if frame.kind == "status":
        stance = frame.meta.get("stance")
        sig = ""
        if isinstance(stance, str) and stance:
            fp = " +fact_pull" if frame.meta.get("fact_pull") else ""
            sig = f" [[stance:{_r(stance)}{fp}]]"
        return f"- `{et}` **status** {who}{sig}: {body}".rstrip()
    if frame.kind == "narration":
        return f"- `{et}` **narration**: {body}".rstrip()
    if frame.kind == "arbitrate":
        topic = frame.extra.get("topic")
        decided = frame.extra.get("decision")
        parts = [f"- `{et}` **arbitrate**"]
        if topic:
            parts.append(f"topic={_r(topic)}")
        if decided:
            parts.append(f"decision={_r(decided)}")
        if body:
            parts.append(f"— {body}")
        return " ".join(parts)
    if frame.kind in ("decide", "synthesize"):
        # Real thin-trace decision records: action word + winner in target.
        parts = [f"- `{et}` **{_r(frame.kind)}**"]
        if target:
            parts.append(f"-> {_r(target)}")
        if body:
            parts.append(f": {body}")
        return " ".join(parts).rstrip()
    if frame.kind == "control":
        op = frame.extra.get("op") or "control"
        reason = f" — {body}" if body else ""
        return f"- `{et}` **control:{_r(op)}** by {who}{reason}".rstrip()
    if frame.kind in ("done", "complete"):
        outcome = frame.extra.get("outcome") or "completed"
        return f"- `{et}` **done** — outcome: {_r(outcome)}"
    if frame.kind == "kboard_event":
        event = frame.extra.get("event") or frame.extra.get("status") or "event"
        return f"- `{et}` **kboard** {_r(event)}: {body}".rstrip()
    # Unknown kind — surface it, never drop silently. Kind is engine-safe; the
    # rest is scrubbed.
    return f"- `{et}` **{_r(frame.kind)}** {who}{arrow}: {body}".rstrip()


def render_summary_md(summary: Summary) -> str:
    """Render the who/what/when/decision header block as markdown.

    Every agent-authored field (task id, participants, decision, stance
    who/keyword, control op/sender, outcome) is scrubbed fail-closed — the header
    is a rendered surface exactly like the transcript, not a redaction bypass.
    """
    participants = ", ".join(_r(p) for p in summary.participants) or "(none recorded)"
    lines = [
        f"# Deliberation trail — `{_r(summary.task_id)}`",
        "",
        "## Summary",
        f"- **Participants:** {participants}",
        f"- **Window (ET):** {summary.started_et or '?'} -> {summary.ended_et or '?'}",
        f"- **Frames:** {summary.frame_count}",
    ]
    if summary.decision:
        lines.append(f"- **Decision:** arbitrated for **{_r(summary.decision)}**")
    if summary.stances:
        signed = ", ".join(
            f"{_r(who)}={_r(stance)}{'+fp' if fp else ''}" for who, stance, fp in summary.stances
        )
        lines.append(f"- **Signed stances:** {signed}")
    if summary.control_ops:
        ops = ", ".join(
            f"{_r(op)} by {_r(sender) or '?'}" for op, sender, _reason in summary.control_ops
        )
        lines.append(f"- **Control ops:** {ops}")
    lines.append(
        f"- **Outcome:** {_r(summary.outcome)}"
        if summary.outcome
        else "- **Outcome:** (unfinished)"
    )
    return "\n".join(lines)


@dataclass(frozen=True, slots=True)
class RenderedTranscript:
    """A rendered deliberation trail (summary header + transcript body).

    Attributes:
        markdown: The full markdown (summary first, transcript second), already
            redacted and — for the tool surface — truncated to the cap.
        truncated: Whether the transcript body was cut to the size cap.
        full_len: The full (untruncated) markdown length in characters.
        frame_count: The number of in-scope frames.
    """

    markdown: str
    truncated: bool
    full_len: int
    frame_count: int


def reconstruct(
    lines: Iterable[str],
    task_id: str,
    *,
    topic: str | None = None,
) -> tuple[Summary, list[Frame]]:
    """The one entry point: journal lines + task_id → (summary, chronological frames).

    Raises:
        ValueError: If ``task_id`` is empty (propagated from :func:`scope_to_task`).
    """
    frames = scope_to_task(iter_frames(lines), task_id, topic=topic)
    summary = build_summary(task_id, frames)
    return summary, frames


def render_markdown(
    summary: Summary,
    frames: list[Frame],
    *,
    max_chars: int | None = None,
    full_output_path: str | None = None,
) -> RenderedTranscript:
    """Render summary-header-first, transcript-second markdown.

    Args:
        summary: The reconstructed header block.
        frames: The chronological in-scope frames.
        max_chars: Optional size cap (the tool surface passes a sane cap; the CLI
            passes ``None`` for the full document). When the rendered document
            exceeds the cap the transcript body is cut at a frame boundary and a
            truncation note + pointer to ``full_output_path`` is appended.
        full_output_path: Where the full (untruncated) document lives, cited in
            the truncation note. Ignored when nothing is truncated.

    Returns:
        The :class:`RenderedTranscript`.
    """
    header = render_summary_md(summary)
    body_lines = [_render_frame_line(f) for f in frames]
    transcript = "\n".join(["", "## Transcript", "", *body_lines])
    full = header + "\n" + transcript
    full_len = len(full)

    if max_chars is None or full_len <= max_chars:
        return RenderedTranscript(
            markdown=full, truncated=False, full_len=full_len, frame_count=len(frames)
        )

    # Truncate at a frame (line) boundary so we never cut mid-line and leak half a
    # redacted body. Keep the header intact; trim transcript lines until it fits
    # with room for the note.
    note_ptr = f" See the full trail at `{full_output_path}`." if full_output_path else ""
    note = f"\n\n> _Transcript truncated to fit ({full_len} chars -> cap {max_chars}).{note_ptr}_"
    budget = max_chars - len(header) - len("\n\n## Transcript\n\n") - len(note)
    kept: list[str] = []
    used = 0
    for line in body_lines:
        if used + len(line) + 1 > budget:
            break
        kept.append(line)
        used += len(line) + 1
    truncated_body = "\n".join(["", "## Transcript", "", *kept])
    markdown = header + "\n" + truncated_body + note

    # Honor the cap ABSOLUTELY. When even the fixed overhead (header + note) alone
    # exceeds max_chars, the frame-boundary trim above cannot get under the cap —
    # so hard-slice the final string as the last resort. A model context budget is
    # a hard ceiling, not a target; the returned markdown must NEVER exceed it.
    if len(markdown) > max_chars:
        markdown = markdown[:max_chars]
    return RenderedTranscript(
        markdown=markdown, truncated=True, full_len=full_len, frame_count=len(frames)
    )


def reconstruct_json(summary: Summary, frames: list[Frame]) -> dict[str, object]:
    """The ``--format json`` shape: summary + one FULLY-redacted record per frame.

    EVERY string value on this surface — text, sender, target, and recursively
    every string leaf of ``meta`` and ``extra`` — runs through the fail-closed
    redaction scrub, exactly like the markdown path. The JSON surface must never
    be a redaction bypass (correctness-reviewer BLOCKER-2: a ``meta.note`` or a
    nested ``extra`` value could otherwise carry a token verbatim). ``ts_et``,
    ``kind``, ``frame_count``, ``fact_pull`` are engine-controlled and safe.
    """
    return {
        "task_id": _r(summary.task_id),
        "summary": {
            "participants": [_r(p) for p in summary.participants],
            "started_et": summary.started_et,
            "ended_et": summary.ended_et,
            "frame_count": summary.frame_count,
            "decision": _r(summary.decision) if summary.decision else None,
            "stances": [
                {"sender": _r(who), "stance": _r(stance), "fact_pull": fp}
                for who, stance, fp in summary.stances
            ],
            "outcome": _r(summary.outcome) if summary.outcome else None,
            "control_ops": [
                {
                    "op": _r(op),
                    "sender": _r(sender) if sender else None,
                    "reason": _redact(reason) if reason else None,
                }
                for op, sender, reason in summary.control_ops
            ],
        },
        "frames": [
            {
                "ts_et": _to_et(f.ts),
                "kind": f.kind,
                "sender": _r(f.sender) if f.sender else None,
                "target": _r(f.target) if f.target else None,
                "text": _redact(f.text) if f.text else "",
                "meta": _redact_obj(f.meta),
                "extra": _redact_obj(f.extra),
            }
            for f in frames
        ],
    }
