"""Optional JSONL coordination trace — the replay/backtesting foundation (#22).

Off by default in dev; ON by default in prod (``FLEET_ENV=prod``) so an incident
always leaves a replay trace — the backtesting/incident foundation must never be
silently off in prod (#108). An explicit ``FLEET_TRACE_PATH`` names the file to
append to and overrides the prod default in either env; with neither set in dev
the sink is a no-op (no file is created, no I/O happens). One structured record
per coordination decision and per bus operation — the same decision points the
loguru lines cover, in a machine-readable form.

Records carry coordination metadata ONLY: timestamp, state, event, action,
task id, target agent, subject. Never message bodies, secrets, webhook URLs,
or Discord member ids (the ``docs/logging.md`` never-log list is this record's
negative space). The record shape is a closed dataclass so nothing else can
sneak in.

Timestamps are caller-supplied (the orchestrator's injected ``IdClock``) — this
module never reads a wall clock, keeping traced runs reproducible under test.
"""

from __future__ import annotations

import json
import os
import re
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Final, Protocol

from loguru import logger

# Exact FLEET_ENV values, matching fleet_nats.config.BusConfig's strict contract
# (frozenset({"dev","prod"}), case-sensitive). trace.py and BusConfig are the two
# consumers of FLEET_ENV and MUST agree, or `FLEET_ENV=Prod` turns the trace ON
# here while BusConfig raises at connect (SME review, P2). We deliberately do NOT
# lower()/strip(): an exact non-match (e.g. "Prod", " prod ") is NOT prod for
# either consumer — the trace stays off, matching what BusConfig would reject.
_VALID_FLEET_ENVS = frozenset({"dev", "prod"})

# Soft import: the Langfuse SDK is an OPTIONAL dependency (extra ``[langfuse]``).
# An absent SDK must NEVER crash coordination — sink_from_env simply omits the
# Langfuse sink and the JSONL sink continues alone. Mirrors the fail-open
# soft-import in the in-tree Hermes plugin
# (ai-agents/platforms/hermes/v0_18_0/plugins/observability/langfuse/__init__.py:36-40).
# Typed Any so mypy is clean whether or not the extra is installed.
_Langfuse: Any
try:  # pragma: no cover - exercised via monkeypatch, not a real absent SDK
    from langfuse import Langfuse

    _Langfuse = Langfuse
except Exception:  # pragma: no cover - fail-open when optional dep is missing
    _Langfuse = None

_ENV_FLAG = "FLEET_TRACE_PATH"

# The subject-env knob (fleet_nats.BusConfig reads the same var). In prod the
# replay trace is the backtesting/incident foundation, so it MUST be on even when
# no explicit FLEET_TRACE_PATH is set — an incident that captures nothing is the
# exact failure #108 fixes. dev stays off-by-default (no accidental file I/O in
# unit runs), but an explicit FLEET_TRACE_PATH still wins in either env.
_ENV_FLEET_ENV = "FLEET_ENV"
_PROD_ENV = "prod"

# Where the prod trace lands when FLEET_TRACE_PATH is not set. Under the hub's
# runtime dir on fleet-hub (fleet-hub); JsonlTraceSink mkdirs the parent lazily. An
# operator can still override with an explicit FLEET_TRACE_PATH.
PRODUCTION_TRACE_PATH: Final = "/var/lib/fleet-orchestrator/trace/coordination.jsonl"

# Self-bounding rotation (review P2 #133): the prod-default trace is ON and
# append-only, so without a cap it grows UNBOUNDED and eventually fills the
# StateDirectory volume, degrading every other writer on it. JsonlTraceSink rotates
# in-sink (no dependency on deploy-side logrotate/tmpfiles hygiene): when the active
# file reaches _MAX_TRACE_BYTES it is renamed through a ring of _MAX_TRACE_BACKUPS
# numbered files (coordination.jsonl.1 … .5) — the RotatingFileHandler naming
# convention — and a fresh file is opened. The oldest backup is discarded on
# rollover, so total on-disk trace is hard-bounded at
# ~_MAX_TRACE_BYTES * (_MAX_TRACE_BACKUPS + 1) (~768 MiB at the defaults). Both are
# monkeypatchable (tests drive tiny caps). A rotation error is swallowed+logged
# exactly like an emit write failure — it must never crash the coordination path.
_MAX_TRACE_BYTES = 128 * 1024 * 1024  # 128 MiB per file
_MAX_TRACE_BACKUPS = 5  # keep .1 … .5; total bound ~768 MiB

# Standard Langfuse SDK env names (the Hermes plugin also accepts HERMES_-prefixed
# variants; the fleet hub sets the plain names in the unit env — scout §3). Creds
# come from Vaultwarden item ``Langfuse - MAE`` at deploy time; NEVER hardcoded.
_LF_PUBLIC_KEY_ENV = "LANGFUSE_PUBLIC_KEY"
_LF_SECRET_KEY_ENV = "LANGFUSE_SECRET_KEY"
_LF_BASE_URL_ENV = "LANGFUSE_BASE_URL"

# Langfuse-issued keys carry these prefixes (cloud or self-hosted). A placeholder
# ("your-key", "test") is accepted at construction but drops every trace at flush
# time — reuse the Hermes plugin's prefix guard (init.py:70-80, ref #23823) to
# fail-open loudly ONCE instead of shipping a silently-broken sink.
_LF_PUBLIC_KEY_PREFIX = "pk-lf-"
_LF_SECRET_KEY_PREFIX = "sk-lf-"

# Bound live trace state so a run that never emits a terminal ``done`` for a
# task cannot leak root-trace ids forever. Reuses the evict-on-cap idea from the
# Hermes plugin's ``_MAX_TRACE_STATE`` (init.py:66). Far above any realistic
# concurrent-live-task working set; it only bounds the non-finalizing leak.
_LF_MAX_TASKS = 512
_LF_NAME_COMPONENT_MAX = 48
_LF_NAME_MAX = 128


@dataclass(frozen=True, slots=True)
class TraceRecord:
    """One sanitized coordination decision or bus operation.

    Attributes:
        ts: Caller-supplied ISO-8601 timestamp (injected clock, never local).
        task_id: The coordination task this record belongs to.
        state: Resulting session state, for decision records.
        event: Inbound event kind, for decision records.
        action: Action kind decided / performed.
        target: The ``agent_id`` the action is directed at, when applicable.
        subject: The NATS subject, for bus-operation records.
    """

    ts: str
    task_id: str
    state: str | None = None
    event: str | None = None
    action: str | None = None
    target: str | None = None
    subject: str | None = None

    def as_json(self) -> str:
        """Render the record as one compact JSON line (``None`` fields omitted)."""
        fields = {k: v for k, v in asdict(self).items() if v is not None}
        return json.dumps(fields, sort_keys=True)


class TraceSink(Protocol):
    """Anything that can accept trace records (file sink, test capture, no-op)."""

    def emit(self, record: TraceRecord) -> None:
        """Accept one record."""
        ...

    def flush(self) -> None:
        """Flush any buffered records at shutdown (no-op for eager sinks).

        Batching sinks (Langfuse queues spans on a background thread) MUST drain
        here or the tail of a run is lost on SIGTERM. Eager sinks (JSONL writes
        each line immediately) implement this as a no-op. Called from the hub's
        shutdown/finally path before the bus closes.
        """
        ...


class NullTraceSink:
    """The flag-off sink: does nothing, touches nothing."""

    def emit(self, record: TraceRecord) -> None:
        """Discard the record (tracing is disabled)."""

    def flush(self) -> None:
        """Nothing is buffered (tracing is disabled)."""


class JsonlTraceSink:
    """Append-only JSONL file sink, fsync'd per line, size-rotated to a hard bound.

    A write failure is logged and swallowed — tracing is observability, and it
    must never take the control loop down with it. The first failure logs at
    ERROR (prod now defaults the trace ON and depends on it — a prod hub that
    silently stops capturing is the exact #108 failure; an operator MUST see the
    first failure loudly, P1-2); repeats drop to debug so a dead disk cannot
    flood the log.

    Durability (P2-9): each line is flushed to the OS and ``os.fsync``'d before
    ``emit`` returns, so a record that emit() accepted survives an OOM-kill /
    SIGKILL / power-loss immediately after — the incident classes replay exists
    to reconstruct. fsync is per-line (each record is a discrete coordination
    decision, not a hot inner loop), so the durability cost is bounded.

    Retention (review P2 #133): the prod-default trace is ON and append-only, so
    the sink SELF-BOUNDS on disk — it does not depend on deploy-side
    logrotate/tmpfiles. Before appending, if the active file has reached
    :data:`_MAX_TRACE_BYTES` it is rotated through a ring of
    :data:`_MAX_TRACE_BACKUPS` numbered backups (``…​.jsonl.1`` … ``.5``, the
    stdlib RotatingFileHandler convention), discarding the oldest — so total
    on-disk trace is hard-bounded regardless of uptime. A rotation error degrades
    exactly like a write failure (swallowed + logged), never crashing the loop.
    """

    def __init__(self, path: Path) -> None:
        """Remember the target path. The file is created lazily on first emit."""
        self._path = path
        self._warned = False

    def _rotate_if_needed(self) -> None:
        """Rotate the active file through the backup ring if it hit the size cap.

        Called under ``emit``'s try, BEFORE the append, so the check reflects the
        size the previous emits already wrote. When the active file is at/over
        :data:`_MAX_TRACE_BYTES`, shift ``.{K-1}`` → ``.{K}`` (dropping the old
        ``.{K}``) down to base → ``.1``, leaving base absent so the append opens a
        fresh file. A partial rotation (some renames done, one raised) still leaves
        a coherent set — the append then continues on whatever base is now present;
        the caller's except turns any raise into the standard degrade path.
        """
        try:
            size = self._path.stat().st_size
        except FileNotFoundError:
            return  # no active file yet → nothing to rotate
        if size < _MAX_TRACE_BYTES:
            return
        # Drop the oldest, then cascade .{i} → .{i+1} down to base → .1.
        oldest = self._path.with_name(f"{self._path.name}.{_MAX_TRACE_BACKUPS}")
        if oldest.exists():
            oldest.unlink()
        for i in range(_MAX_TRACE_BACKUPS - 1, 0, -1):
            src = self._path.with_name(f"{self._path.name}.{i}")
            if src.exists():
                src.rename(self._path.with_name(f"{self._path.name}.{i + 1}"))
        self._path.rename(self._path.with_name(f"{self._path.name}.1"))

    def emit(self, record: TraceRecord) -> None:
        """Append one JSON line (rotated + fsync'd), logging (never raising) on failure.

        The catch is deliberately broad: serialization of a runtime-bad field
        value (``TypeError`` from ``json.dumps``) must degrade exactly like a
        disk or rotation failure — the contract is that tracing NEVER takes the
        control loop down, and every failure is logged before being swallowed.
        """
        try:
            line = record.as_json()  # serialize BEFORE touching the filesystem
            self._path.parent.mkdir(parents=True, exist_ok=True)
            self._rotate_if_needed()  # self-bound on disk before appending
            with self._path.open("a", encoding="utf-8") as fh:
                fh.write(line + "\n")
                # Push the buffered write to the OS, then fsync so the record is
                # on stable storage before emit() returns (P2-9 durability).
                fh.flush()
                os.fsync(fh.fileno())
        except Exception as exc:  # noqa: BLE001 — degrade-never-raise contract
            if not self._warned:
                # ERROR, not warning: prod defaults the trace ON and depends on
                # it — the first capture failure must be loud (P1-2).
                logger.error("trace.emit | sink write failed, tracing degraded: {}", exc)
                self._warned = True
            else:
                logger.debug("trace.emit | sink still failing: {}", exc)

    def flush(self) -> None:
        """No-op: each line is written and fsync'd eagerly on emit, nothing buffered."""


class TeeTraceSink:
    """Fan one record to several sinks; one sink's failure never blocks the rest.

    Composes the JSONL sink (replay/backtesting foundation, #22) with the
    Langfuse sink so a traced run lands in BOTH — JSONL replay is preserved AND
    Langfuse gets spans from the identical, already-sanitized record flow. Each
    child sink already owns its own degrade-never-raise contract; the tee adds a
    belt-and-braces guard so a child that somehow raises cannot starve its
    siblings or take the control loop down.
    """

    def __init__(self, *sinks: TraceSink) -> None:
        """Remember the child sinks, in emit order."""
        self._sinks = sinks

    def emit(self, record: TraceRecord) -> None:
        """Fan ``record`` to every child; log-and-continue on any child failure."""
        for sink in self._sinks:
            try:
                sink.emit(record)
            except Exception as exc:  # noqa: BLE001 — degrade-never-raise contract
                logger.error("trace.tee | child sink {} failed: {}", type(sink).__name__, exc)

    def flush(self) -> None:
        """Fan the shutdown flush to every child; one child's failure never blocks the rest."""
        for sink in self._sinks:
            try:
                sink.flush()
            except Exception as exc:  # noqa: BLE001 — degrade-never-raise contract
                logger.error("trace.tee | child sink {} flush failed: {}", type(sink).__name__, exc)


class LangfuseTraceSink:
    """Map sanitized :class:`TraceRecord`s to Langfuse traces/spans.

    Model (scout §2): one Langfuse **trace** per ``task_id`` (a deterministic id
    seeded from the task id, memoized so every record for a task threads onto the
    same trace), one **span** per record keyed by the record's ``action``
    (dispatch / relay / arbitration / done / …).

    REUSE of the in-tree Hermes plugin
    (ai-agents/platforms/hermes/v0_18_0/plugins/observability/langfuse/__init__.py):
      * client build + fail-open + placeholder-key guard (``_get_langfuse``,
        init.py:149-232, ref #23823);
      * SDK-v3 ``create_trace_id(seed=...)`` + ``start_observation(trace_context=...)``
        + ``observation.update(...)/.end()`` + ``client.flush()`` (init.py:606-699);
      * evict-on-cap trace-state bounding (``_MAX_TRACE_STATE``, init.py:66).

    DIVERGENCE (deliberate): the Hermes plugin wraps a live call stack with
    ``start_as_current_observation(...).__enter__()`` — that needs an enclosing
    scope to close. This sink receives DISCRETE ``emit()`` calls with no enclosing
    frame, so it uses standalone ``start_observation`` + immediate ``.end()`` per
    record instead of a current-context root span. No dangling context is left.

    Fail-open everywhere (same contract as :class:`JsonlTraceSink`): a missing
    SDK, bad creds, or any SDK error logs ONCE then degrades to a no-op — tracing
    is observability and must NEVER take coordination down.

    The ``task_id`` mapping and terminal tags below are metadata-only
    addressability for beat attribution and acceptance. They are not an
    authorization or spoofing control; authenticated per-agent principals land
    with F2 (#104).
    """

    def __init__(self, client: Any) -> None:
        """Wrap an already-built Langfuse client (see :func:`_build_langfuse`)."""
        self._client = client
        self._trace_ids: dict[str, str] = {}
        self._degraded = False

    def _trace_id_for(self, task_id: str) -> str:
        """Return the memoized trace id for ``task_id``, minting one on first use.

        Bounds live state: over the cap, drop the oldest tracked task id (dict is
        insertion-ordered) before inserting — the Hermes plugin's evict-on-cap
        idea, adapted to a plain id map (no root span to end here).
        """
        existing = self._trace_ids.get(task_id)
        if existing is not None:
            return existing
        if len(self._trace_ids) >= _LF_MAX_TASKS:
            oldest = next(iter(self._trace_ids))
            del self._trace_ids[oldest]
        # str() pins the return type: the SDK client is dynamically typed (Any),
        # and create_trace_id is documented to return a str id.
        trace_id = str(self._client.create_trace_id(seed=f"fleet-task::{task_id}"))
        self._trace_ids[task_id] = trace_id
        return trace_id

    def emit(self, record: TraceRecord) -> None:
        """Emit one span under the task's trace, logging (never raising) on failure."""
        if self._degraded:
            return
        try:
            trace_id = self._trace_id_for(record.task_id)
            # Only sanctioned, already-sanitized coordination metadata rides the
            # span — the SAME closed field set the JSONL line carries. No bodies,
            # no secrets (TraceRecord's negative space is the never-log list).
            metadata = {
                k: v
                for k, v in (
                    ("state", record.state),
                    ("event", record.event),
                    ("target", record.target),
                    ("subject", record.subject),
                )
                if v is not None
            }
            metadata["task_id"] = record.task_id
            terminal = _terminal_tag(record)
            if terminal is not None:
                metadata["terminal_state"] = terminal
            operation = _trace_operation(record)
            # Names are an observability label only: bound and sanitize the
            # untrusted identifiers to prevent control characters/cardinality
            # pollution. Raw task_id remains in metadata for exact lookup;
            # authentication/authorization lands with F2 (#104).
            safe_task_id = _safe_langfuse_name_component(record.task_id)
            safe_operation = _safe_langfuse_name_component(operation)
            observation = self._client.start_observation(
                trace_context={"trace_id": trace_id},
                name=f"fleet.task.{safe_task_id}.{safe_operation}"[:_LF_NAME_MAX],
                as_type="span",
                metadata=metadata,
                tags=[terminal] if terminal is not None else [],
            )
            observation.update(metadata={"ts": record.ts})
            observation.end()
        except Exception as exc:  # noqa: BLE001 — degrade-never-raise contract
            # Log ONCE then go inert: a broken Langfuse edge must not flood the
            # log or slow the loop with a per-record retry.
            logger.warning("trace.langfuse | sink failed, Langfuse tracing degraded: {}", exc)
            self._degraded = True

    def flush(self) -> None:
        """Best-effort flush of the client's background queue (never raises)."""
        try:
            self._client.flush()
        except Exception as exc:  # noqa: BLE001 — degrade-never-raise contract
            logger.debug("trace.langfuse | flush failed: {}", exc)


def _trace_operation(record: TraceRecord) -> str:
    """Choose a meaningful operation name without exposing payload content."""
    action = (record.action or "").strip().lower()
    if action and action != "noop":
        return action
    event = (record.event or "").strip().lower()
    return event or "coordination_event"


def _safe_langfuse_name_component(value: str) -> str:
    """Return a bounded, control-character-free component for a span name."""
    normalized = re.sub(r"[^A-Za-z0-9_.-]+", "-", value).strip("-._")
    return (normalized or "unknown")[:_LF_NAME_COMPONENT_MAX]


def _terminal_tag(record: TraceRecord) -> str | None:
    """Return an honest terminal tag derivable from the closed record fields."""
    values = {
        value.strip().lower()
        for value in (record.state, record.action, record.event)
        if value is not None
    }
    if "done" in values or "complete" in values:
        return "done"
    if "blocked" in values:
        return "blocked"
    if "red" in values:
        return "RED"
    return None


def _valid_key(value: str, prefix: str) -> bool:
    """True when ``value`` looks like a real Langfuse key (not a placeholder)."""
    return value.startswith(prefix)


def _build_langfuse() -> Any | None:
    """Build a Langfuse client from the env, or ``None`` when unavailable.

    Fail-open gate (reused from the Hermes plugin's ``_get_langfuse``): returns
    ``None`` — logging at most once — when the SDK is absent, creds are missing,
    creds look like placeholders, or the client constructor raises. A ``None``
    result means "no Langfuse sink"; the JSONL sink then runs alone.
    """
    if _Langfuse is None:
        logger.info("trace.langfuse | SDK not installed; Langfuse tracing off (JSONL only)")
        return None
    public_key = os.environ.get(_LF_PUBLIC_KEY_ENV, "").strip()
    secret_key = os.environ.get(_LF_SECRET_KEY_ENV, "").strip()
    base_url = os.environ.get(_LF_BASE_URL_ENV, "").strip()
    if not (public_key and secret_key and base_url):
        return None
    if not _valid_key(public_key, _LF_PUBLIC_KEY_PREFIX) or not _valid_key(
        secret_key, _LF_SECRET_KEY_PREFIX
    ):
        # Placeholder creds construct fine but drop every trace at flush (#23823).
        logger.warning(
            "trace.langfuse | credentials look like placeholders "
            "(expected pk-lf-/sk-lf- prefixes); Langfuse tracing off (JSONL only)"
        )
        return None
    try:
        client = _Langfuse(public_key=public_key, secret_key=secret_key, base_url=base_url)
    except Exception as exc:  # noqa: BLE001 — fail-open: never crash coordination
        logger.warning("trace.langfuse | could not initialize client: {} (JSONL only)", exc)
        return None
    logger.info("trace.langfuse | Langfuse tracing enabled base_url={}", base_url)
    return client


def _resolve_trace_path() -> str | None:
    """Resolve the JSONL trace path from the env, defaulting ON in prod.

    Precedence:
      1. An explicit ``FLEET_TRACE_PATH`` always wins (either env).
      2. Otherwise, in ``FLEET_ENV=prod`` the trace defaults ON at
         :data:`PRODUCTION_TRACE_PATH` — the backtesting/incident foundation
         must never be silently off in prod (#108).
      3. Otherwise (dev, unset path) tracing is off — ``None``.

    Returns:
        The JSONL destination path, or ``None`` when tracing is off.
    """
    explicit = os.environ.get(_ENV_FLAG)
    if explicit:
        return explicit
    # EXACT match, matching BusConfig's strict, case-sensitive contract — no
    # strip()/lower(). `FLEET_ENV=Prod`/` prod ` is NOT prod for BusConfig (it
    # raises at connect), so it must not silently arm the trace here either
    # (SME review, P2 — the two FLEET_ENV consumers must agree).
    fleet_env = os.environ.get(_ENV_FLEET_ENV, "")
    if fleet_env == _PROD_ENV:
        return PRODUCTION_TRACE_PATH
    return None


def _probe_writable(path: str) -> str | None:
    """Return ``None`` if ``path``'s parent dir is writable, else a reason string.

    Wiring-time writability probe (P1-2): the JsonlTraceSink creates the file
    lazily on first emit and SWALLOWS a write failure, so on a clean host with an
    uncreatable/unwritable trace dir the ``ENABLED`` startup line would lie — it
    claims capture while every emit silently degrades. Probing here (create the
    parent, then open the file for append) surfaces the failure at startup, next
    to the posture line, so an operator sees a loud ERROR instead of a silent gap.

    The probe is non-destructive: it only ensures the parent exists and the file
    is openable for append (touch semantics), never writing a record.
    """
    try:
        target = Path(path)
        target.parent.mkdir(parents=True, exist_ok=True)
        # Open for append (touch): proves both the dir and the file are writable
        # without emitting a record. Closes immediately.
        with target.open("a", encoding="utf-8"):
            pass
    except Exception as exc:  # noqa: BLE001 — a probe failure is reported, never raised
        return str(exc)
    return None


def sink_from_env() -> TraceSink:
    """Build the sink the environment asks for, defaulting ON in prod (#108).

    The JSONL replay trace is enabled when ``FLEET_TRACE_PATH`` is set OR
    ``FLEET_ENV=prod`` (a prod hub captures its coordination trace by default;
    an explicit path overrides the prod default). When enabled AND Langfuse creds
    are present, TEE the JSONL sink with a Langfuse sink so replay and Langfuse
    both see the identical record flow. Missing SDK/creds → JSONL sink alone
    (never crashes).

    A fail-loud startup line ALWAYS states the trace posture (on/off + the
    destination path) so an operator can see at a glance whether an incident will
    be captured — the exact gap #108 closes.
    """
    path = _resolve_trace_path()
    if not path:
        # Fail-loud on the OFF case too: an operator must be able to see that a
        # (non-prod) hub is running with no replay capture, not infer it silently.
        logger.info(
            "trace.configure | JSONL trace DISABLED (set {} or run with {}=prod to enable)",
            _ENV_FLAG,
            _ENV_FLEET_ENV,
        )
        return NullTraceSink()
    # Fail-loud at WIRING time if the destination is not writable, so the ENABLED
    # posture line can never lie about capture (P1-2).
    #
    # In prod the trace is the backtesting/incident foundation and must never be
    # silently absent: an unwritable destination REFUSES startup (raises) so the
    # hub cannot come up believing it is capturing when it is not. In non-prod the
    # trace is a convenience — warn loudly and degrade rather than block a dev run.
    unwritable = _probe_writable(path)
    if unwritable is not None:
        if os.environ.get(_ENV_FLEET_ENV, "") == _PROD_ENV:
            raise RuntimeError(
                "JSONL trace path NOT WRITABLE in prod "
                f"path={path} reason={unwritable} — refusing startup; the "
                "coordination trace is the incident/backtesting foundation and "
                "must not be silently absent (check StateDirectory/perms on the "
                "hub unit)"
            )
        logger.error(
            "trace.configure | JSONL trace path NOT WRITABLE path={} reason={} — "
            "capture will silently degrade until this is fixed (check StateDirectory/"
            "perms on the hub unit)",
            path,
            unwritable,
        )
    else:
        logger.info("trace.configure | JSONL trace ENABLED path={}", path)
    jsonl = JsonlTraceSink(Path(path))
    client = _build_langfuse()
    if client is None:
        return jsonl
    return TeeTraceSink(jsonl, LangfuseTraceSink(client))


_active: TraceSink | None = None


def set_sink(sink: TraceSink | None) -> None:
    """Install ``sink`` explicitly (tests), or ``None`` to re-read the env lazily."""
    global _active
    _active = sink


def emit(record: TraceRecord) -> None:
    """Emit ``record`` to the active sink (resolved from the env on first use)."""
    global _active
    if _active is None:
        _active = sink_from_env()
    _active.emit(record)


def flush() -> None:
    """Flush the active sink at shutdown so a batching sink drains its tail.

    Only flushes an ALREADY-resolved sink — a run that never emitted has no
    active sink and nothing to drain, so shutdown must not lazily build one (that
    would needlessly re-init Langfuse just to flush an empty queue). Called from
    the hub's shutdown/finally path before the bus closes.
    """
    if _active is not None:
        _active.flush()
