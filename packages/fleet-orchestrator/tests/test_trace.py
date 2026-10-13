"""Tests for the optional JSONL coordination trace (#22).

Covers the acceptance gates: flag off → no file and zero artifacts; flag on →
one valid, sanitized JSONL record per decision; a driven transition sequence is
replayable from the trace alone; a sink write failure degrades (logged) without
crashing the control loop. The orchestrator wiring is tested through the same
FakeBus double the behaviors tests use.
"""

from __future__ import annotations

import itertools
import json
import os
from collections.abc import Iterator
from pathlib import Path

import pytest
from fleet_nats import BusConfig, Envelope
from loguru import logger

from fleet_orchestrator import trace
from fleet_orchestrator.behaviors import IdClock, Orchestrator
from fleet_orchestrator.state import CoordinationSession, Event, EventKind, State
from fleet_orchestrator.trace import (
    JsonlTraceSink,
    LangfuseTraceSink,
    NullTraceSink,
    TeeTraceSink,
    TraceRecord,
    sink_from_env,
)

# Sink isolation is the conftest autouse fixture; tests here install their own
# sinks explicitly via trace.set_sink().


class _CaptureSink:
    """Test sink capturing records in memory."""

    def __init__(self) -> None:
        self.records: list[TraceRecord] = []
        self.flushed = 0

    def emit(self, record: TraceRecord) -> None:
        self.records.append(record)

    def flush(self) -> None:
        self.flushed += 1


class _FakeBus:
    """Minimal FleetBus double: records publishes."""

    def __init__(self, config: BusConfig) -> None:
        self._config = config
        self.published: list[tuple[str, Envelope]] = []

    @property
    def config(self) -> BusConfig:
        return self._config

    async def publish(self, subject: str, envelope: Envelope) -> None:
        self.published.append((subject, envelope))


def _orchestrator() -> tuple[Orchestrator, _FakeBus]:
    cfg = BusConfig(agent_id="nagatha", env="dev")
    bus = _FakeBus(cfg)
    ids = (f"m{i}" for i in itertools.count())
    clock = IdClock(new_id=lambda: next(ids), now=lambda: "2026-07-07T00:00:00Z")
    return Orchestrator(bus, clock), bus  # type: ignore[arg-type]


def _session() -> CoordinationSession:
    return CoordinationSession(
        task_id="t-trace",
        channel="dash-dev",
        manager="nagatha",
        workers=("bilby", "skippy"),
        state=State.DISPATCHED,
    )


# --- flag off: zero artifacts -------------------------------------------------


def test_flag_off_creates_no_file(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Unset flag (dev) → the LAZY emit() path resolves NullTraceSink; nothing on disk."""
    monkeypatch.delenv("FLEET_TRACE_PATH", raising=False)
    monkeypatch.delenv("FLEET_ENV", raising=False)  # dev: off-by-default (#108)
    monkeypatch.chdir(tmp_path)
    assert isinstance(sink_from_env(), NullTraceSink)
    # exercise emit()'s _active-is-None branch, not a pre-installed sink
    trace.set_sink(None)
    trace.emit(TraceRecord(ts="2026-07-07T00:00:00Z", task_id="t-off"))
    assert list(tmp_path.iterdir()) == []


def test_flag_on_builds_jsonl_sink(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Set flag → the LAZY emit() path resolves a JsonlTraceSink at that path."""
    target = tmp_path / "coord.jsonl"
    monkeypatch.setenv("FLEET_TRACE_PATH", str(target))
    assert isinstance(sink_from_env(), JsonlTraceSink)
    # exercise the emit() -> sink_from_env() wiring end to end
    trace.set_sink(None)
    trace.emit(TraceRecord(ts="2026-07-07T00:00:00Z", task_id="t-on"))
    lines = target.read_text(encoding="utf-8").splitlines()
    assert len(lines) == 1
    assert json.loads(lines[0]) == {"ts": "2026-07-07T00:00:00Z", "task_id": "t-on"}


# --- prod defaults the trace ON (#108) ----------------------------------------


def test_prod_env_defaults_trace_on(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """FLEET_ENV=prod with NO explicit path → JSONL sink at the prod default path.

    The acceptance gate for #108(a): a prod hub captures a replay trace by default
    so an incident is never silently un-captured.
    """
    default_target = tmp_path / "prod-trace" / "coordination.jsonl"
    monkeypatch.delenv("FLEET_TRACE_PATH", raising=False)
    monkeypatch.setenv("FLEET_ENV", "prod")
    # Redirect the prod default to a writable tmp path — the real /var/lib default
    # is unwritable in the sandbox and prod now REFUSES an unwritable dest (F1).
    monkeypatch.setattr(trace, "PRODUCTION_TRACE_PATH", str(default_target))
    sink = sink_from_env()
    assert isinstance(sink, JsonlTraceSink)
    assert str(sink._path) == trace.PRODUCTION_TRACE_PATH


def test_prod_env_writes_jsonl_at_default_path(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A prod hub with no explicit path still writes a JSONL record (a task today writes)."""
    default_target = tmp_path / "prod-trace" / "coordination.jsonl"
    monkeypatch.delenv("FLEET_TRACE_PATH", raising=False)
    monkeypatch.setenv("FLEET_ENV", "prod")
    monkeypatch.setattr(trace, "PRODUCTION_TRACE_PATH", str(default_target))
    trace.set_sink(None)
    trace.emit(TraceRecord(ts="2026-07-07T00:00:00Z", task_id="t-prod"))
    lines = default_target.read_text(encoding="utf-8").splitlines()
    assert len(lines) == 1
    assert json.loads(lines[0]) == {"ts": "2026-07-07T00:00:00Z", "task_id": "t-prod"}


def test_explicit_path_overrides_prod_default(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """An explicit FLEET_TRACE_PATH wins over the prod default (operator override)."""
    explicit = tmp_path / "explicit.jsonl"
    monkeypatch.setenv("FLEET_ENV", "prod")
    monkeypatch.setenv("FLEET_TRACE_PATH", str(explicit))
    sink = sink_from_env()
    assert isinstance(sink, JsonlTraceSink)
    assert str(sink._path) == str(explicit)


def test_dev_env_stays_off_when_unset(monkeypatch: pytest.MonkeyPatch) -> None:
    """FLEET_ENV=dev (or unset) with no explicit path → NullTraceSink (no file I/O)."""
    monkeypatch.delenv("FLEET_TRACE_PATH", raising=False)
    monkeypatch.setenv("FLEET_ENV", "dev")
    assert isinstance(sink_from_env(), NullTraceSink)


def _unwritable_trace_path(tmp_path: Path) -> Path:
    """A trace path whose parent CANNOT be created: an ancestor is a real file.

    ``_probe_writable`` calls ``parent.mkdir(parents=True)``; when an ancestor
    component is a regular file that raises (NotADirectoryError), which is the
    portable, non-root-dependent way to force an unwritable destination.
    """
    blocker = tmp_path / "blocker"
    blocker.write_text("i am a file, not a directory", encoding="utf-8")
    return blocker / "trace" / "coordination.jsonl"


def test_prod_unwritable_trace_path_refuses_startup(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """F1: in prod an unwritable trace destination REFUSES startup (raises).

    The coordination trace is the incident/backtesting foundation; a prod hub
    must not come up believing it is capturing when the destination is unwritable.
    """
    unwritable = _unwritable_trace_path(tmp_path)
    monkeypatch.setenv("FLEET_ENV", "prod")
    monkeypatch.setenv("FLEET_TRACE_PATH", str(unwritable))
    with pytest.raises(RuntimeError, match="NOT WRITABLE in prod"):
        sink_from_env()


def test_nonprod_unwritable_trace_path_degrades(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """F1: outside prod an unwritable path warns + degrades, never blocks a dev run."""
    unwritable = _unwritable_trace_path(tmp_path)
    monkeypatch.setenv("FLEET_ENV", "dev")
    monkeypatch.setenv("FLEET_TRACE_PATH", str(unwritable))
    # Returns a sink (degrade-never-raise in non-prod), does NOT raise.
    assert isinstance(sink_from_env(), JsonlTraceSink)


# --- record shape: sanitized, closed ------------------------------------------


def test_record_json_omits_none_and_carries_only_sanctioned_keys() -> None:
    """Every emitted key is from the closed metadata set; None fields vanish."""
    rec = TraceRecord(
        ts="2026-07-07T00:00:00Z",
        task_id="t-keys",
        state="working",
        event="dispatch",
        action="dispatch",
        target="bilby",
        subject="dev.agent.dispatch.dash-dev.bilby",
    )
    data = json.loads(rec.as_json())
    assert set(data) == {"ts", "task_id", "state", "event", "action", "target", "subject"}
    partial = json.loads(TraceRecord(ts="x", task_id="t").as_json())
    assert set(partial) == {"ts", "task_id"}


# --- write failure: degrade, never raise --------------------------------------


def test_sink_write_failure_never_raises(tmp_path: Path) -> None:
    """A sink pointed at an unwritable path logs and swallows, twice."""
    # the path IS a directory -> open("a") raises IsADirectoryError (an OSError)
    sink = JsonlTraceSink(tmp_path)
    rec = TraceRecord(ts="2026-07-07T00:00:00Z", task_id="t-fail")
    sink.emit(rec)  # first failure: error path (prod depends on the trace, P1-2)
    sink.emit(rec)  # repeat failure: debug path


def test_bad_record_value_degrades_never_raises(tmp_path: Path) -> None:
    """A runtime type violation in a record field degrades like a disk failure."""
    sink = JsonlTraceSink(tmp_path / "coord.jsonl")
    bad = TraceRecord(ts="2026-07-07T00:00:00Z", task_id=object())  # type: ignore[arg-type]
    sink.emit(bad)  # TypeError from json.dumps -> logged, swallowed
    sink.emit(bad)
    assert not (tmp_path / "coord.jsonl").exists()


def test_sink_creates_missing_parent_dirs(tmp_path: Path) -> None:
    """The documented /var/log-style example works: parents are created."""
    target = tmp_path / "nested" / "dirs" / "coord.jsonl"
    JsonlTraceSink(target).emit(TraceRecord(ts="2026-07-07T00:00:00Z", task_id="t-mkdir"))
    assert target.read_text(encoding="utf-8").count("\n") == 1


# --- orchestrator wiring: decision + bus-op records ----------------------------


@pytest.mark.asyncio
async def test_apply_emits_decision_and_dispatch_records() -> None:
    """One decision record per apply(), one op record per publish, subjects real."""
    capture = _CaptureSink()
    trace.set_sink(capture)
    orch, bus = _orchestrator()
    action = await orch.apply(_session(), Event(kind=EventKind.DISPATCH))
    assert action.session.state is State.WORKING

    decisions = [r for r in capture.records if r.event is not None]
    ops = [r for r in capture.records if r.subject is not None]
    assert len(decisions) == 1
    assert decisions[0].state == "working"
    assert decisions[0].event == "dispatch"
    assert decisions[0].action == "dispatch"
    assert decisions[0].task_id == "t-trace"
    # fan-out to both workers → two op records whose subjects match the publishes
    assert [r.subject for r in ops] == [subject for subject, _ in bus.published]
    assert {r.target for r in ops} == {"bilby", "skippy"}


@pytest.mark.asyncio
async def test_trace_replays_to_the_same_state_path(tmp_path: Path) -> None:
    """A driven sequence is reconstructable from the JSONL trace alone."""
    target = tmp_path / "replay.jsonl"
    trace.set_sink(JsonlTraceSink(target))
    orch, _bus = _orchestrator()

    session = _session()
    for event in (
        Event(kind=EventKind.DISPATCH),
        Event(kind=EventKind.HANDOFF_MISSING, relay_to="skippy", context={"note": "ctx"}),
        Event(kind=EventKind.RELAY_SENT),
        Event(kind=EventKind.DONE, outcome="completed"),
    ):
        session = (await orch.apply(session, event)).session

    states = [
        json.loads(line)["state"]
        for line in target.read_text(encoding="utf-8").splitlines()
        if "state" in json.loads(line)
    ]
    assert states == ["working", "relay_needed", "working", "done"]
    assert session.state is State.DONE


@pytest.mark.asyncio
async def test_no_message_bodies_in_trace(tmp_path: Path) -> None:
    """The relay's carried prose never appears in any trace record."""
    target = tmp_path / "sanitized.jsonl"
    trace.set_sink(JsonlTraceSink(target))
    orch, _bus = _orchestrator()
    secret_prose = "the-missed-handoff-prose-must-not-leak"
    session = (await orch.apply(_session(), Event(kind=EventKind.DISPATCH))).session
    await orch.apply(
        session,
        Event(kind=EventKind.HANDOFF_MISSING, relay_to="skippy", context={"note": secret_prose}),
    )
    assert secret_prose not in target.read_text(encoding="utf-8")


def test_module_capture_via_set_sink() -> None:
    """set_sink() routes module-level emit() to the injected sink."""
    capture = _CaptureSink()
    trace.set_sink(capture)
    trace.emit(TraceRecord(ts="x", task_id="t-inject"))
    assert [r.task_id for r in capture.records] == ["t-inject"]


class _ExplodingBus:
    """Bus double whose publish always raises — for trace-ordering proof."""

    def __init__(self, config: BusConfig) -> None:
        self._config = config

    @property
    def config(self) -> BusConfig:
        return self._config

    async def publish(self, subject: str, envelope: Envelope) -> None:
        raise ConnectionError("bus down")


@pytest.mark.asyncio
async def test_failed_publish_leaves_no_op_record() -> None:
    """A bus-op trace record must never claim a publish an exception prevented."""
    capture = _CaptureSink()
    trace.set_sink(capture)
    cfg = BusConfig(agent_id="nagatha", env="dev")
    ids = itertools.count()
    clock = IdClock(new_id=lambda: f"m{next(ids)}", now=lambda: "2026-07-07T00:00:00Z")
    orch = Orchestrator(_ExplodingBus(cfg), clock)  # type: ignore[arg-type]
    with pytest.raises(ConnectionError):
        await orch.apply(_session(), Event(kind=EventKind.DISPATCH))
    # NO records at all: neither an op record (publish failed) nor a decision
    # record (the caller never persisted that state — replay must not see it).
    assert capture.records == []


# --- Langfuse sink: one trace per task, one span per record, fail-open ---------
#
# A FAKE Langfuse client (no network): records create_trace_id / start_observation
# calls so the mapping — one memoized trace id per task_id, one span per record —
# is asserted without a live server.


class _FakeObservation:
    """A fake span: records update()/end() and its construction kwargs."""

    def __init__(self, log: list[dict[str, object]], kwargs: dict[str, object]) -> None:
        self._log = log
        self.kwargs = kwargs
        self.updated: dict[str, object] = {}
        self.ended = False

    def update(self, **kwargs: object) -> None:
        self.updated.update(kwargs)

    def end(self) -> None:
        self.ended = True


class _FakeLangfuse:
    """A fake SDK-v3 client. Deterministic trace ids; records every observation.

    Accepts the same constructor kwargs the real ``Langfuse`` takes so it can
    stand in for the class ``_build_langfuse`` instantiates.
    """

    def __init__(self, **_kwargs: object) -> None:
        self.seeds: list[str] = []
        self.observations: list[_FakeObservation] = []
        self.flushed = 0

    def create_trace_id(self, *, seed: str) -> str:
        self.seeds.append(seed)
        return f"trace-{seed}"

    def start_observation(self, **kwargs: object) -> _FakeObservation:
        obs = _FakeObservation(self.observations, kwargs)  # type: ignore[arg-type]
        self.observations.append(obs)
        return obs

    def flush(self) -> None:
        self.flushed += 1


class _ExplodingLangfuse:
    """A fake client whose start_observation always raises — for fail-open proof."""

    def create_trace_id(self, *, seed: str) -> str:
        return f"trace-{seed}"

    def start_observation(self, **kwargs: object) -> object:
        raise RuntimeError("langfuse down")

    def flush(self) -> None:
        raise RuntimeError("flush down")


def _rec(task_id: str, action: str, **kw: object) -> TraceRecord:
    return TraceRecord(ts="2026-07-09T00:00:00Z", task_id=task_id, action=action, **kw)  # type: ignore[arg-type]


def test_langfuse_one_trace_per_task_span_per_record() -> None:
    """Records sharing a task_id reuse ONE trace id; each record makes one span."""
    client = _FakeLangfuse()
    sink = LangfuseTraceSink(client)
    sink.emit(_rec("t-1", "dispatch", target="bilby", subject="dev.x"))
    sink.emit(_rec("t-1", "relay", target="skippy"))
    sink.emit(_rec("t-1", "done", state="done"))
    # one trace id minted for the task (seeded from task_id), reused across records
    assert client.seeds == ["fleet-task::t-1"]
    assert len(client.observations) == 3
    # every span threads onto the same trace context
    ctxs = [o.kwargs["trace_context"] for o in client.observations]
    assert ctxs == [{"trace_id": "trace-fleet-task::t-1"}] * 3
    # names are task-addressable and operation-specific; each span is closed
    assert [o.kwargs["name"] for o in client.observations] == [
        "fleet.task.t-1.dispatch",
        "fleet.task.t-1.relay",
        "fleet.task.t-1.done",
    ]
    assert all(o.ended for o in client.observations)


def test_langfuse_distinct_tasks_get_distinct_traces() -> None:
    """Two task_ids → two distinct memoized trace ids."""
    client = _FakeLangfuse()
    sink = LangfuseTraceSink(client)
    sink.emit(_rec("t-a", "dispatch"))
    sink.emit(_rec("t-b", "dispatch"))
    assert client.seeds == ["fleet-task::t-a", "fleet-task::t-b"]


def test_langfuse_span_metadata_is_sanitized_only() -> None:
    """Only the closed coordination field set reaches the span — never a body."""
    client = _FakeLangfuse()
    LangfuseTraceSink(client).emit(
        _rec("t-1", "dispatch", state="working", event="dispatch", target="bilby", subject="dev.s")
    )
    meta = client.observations[0].kwargs["metadata"]
    assert meta == {
        "task_id": "t-1",
        "state": "working",
        "event": "dispatch",
        "target": "bilby",
        "subject": "dev.s",
    }
    observation = client.observations[0]
    assert observation.kwargs["name"] == "fleet.task.t-1.dispatch"
    assert observation.kwargs["tags"] == []


def test_langfuse_span_name_is_bounded_and_metadata_keeps_raw_task_id() -> None:
    """Name labels are safe/bounded while metadata preserves exact attribution."""
    task_id = "task\n\t" + ("x" * 200) + "\u0000"
    operation = "op\r\n" + ("y" * 200)
    client = _FakeLangfuse()
    LangfuseTraceSink(client).emit(_rec(task_id, operation))
    observation = client.observations[0]
    name = observation.kwargs["name"]
    assert isinstance(name, str)
    assert len(name) <= 128
    assert all(char.isprintable() for char in name)
    metadata = observation.kwargs["metadata"]
    assert isinstance(metadata, dict)
    assert metadata["task_id"] == task_id


@pytest.mark.parametrize(
    ("record", "tag"),
    [
        (_rec("t-done", "complete", state="done"), "done"),
        (_rec("t-blocked", "status", state="blocked"), "blocked"),
        (_rec("t-red", "RED"), "RED"),
    ],
)
def test_langfuse_terminal_tags_are_metadata_only(record: TraceRecord, tag: str) -> None:
    """Terminal tags and task addressability contain no payload/body fields."""
    client = _FakeLangfuse()
    LangfuseTraceSink(client).emit(record)
    observation = client.observations[0]
    assert observation.kwargs["metadata"] == {
        "task_id": record.task_id,
        **({"state": record.state} if record.state is not None else {}),
        "terminal_state": tag,
    }
    assert observation.kwargs["tags"] == [tag]
    assert "body" not in observation.kwargs["metadata"]


def test_langfuse_emit_failure_degrades_never_raises() -> None:
    """A failing SDK call logs once, goes inert, and never raises into the loop."""
    sink = LangfuseTraceSink(_ExplodingLangfuse())
    sink.emit(_rec("t-1", "dispatch"))  # first failure → warn + degrade
    sink.emit(_rec("t-1", "relay"))  # already degraded → silent no-op
    sink.flush()  # flush failure is swallowed too


def test_langfuse_trace_state_is_bounded(monkeypatch: pytest.MonkeyPatch) -> None:
    """Over the cap, the oldest task id is evicted so live state cannot leak."""
    monkeypatch.setattr(trace, "_LF_MAX_TASKS", 2)
    client = _FakeLangfuse()
    sink = LangfuseTraceSink(client)
    sink.emit(_rec("t-1", "dispatch"))
    sink.emit(_rec("t-2", "dispatch"))
    sink.emit(_rec("t-3", "dispatch"))  # evicts t-1
    # t-1 re-mints a NEW trace id (its memo was evicted): seed seen twice for t-1
    sink.emit(_rec("t-1", "relay"))
    assert client.seeds.count("fleet-task::t-1") == 2


# --- sink_from_env wiring: tee on, JSONL-only fallbacks ------------------------


def test_sink_from_env_langfuse_absent_returns_jsonl_only(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """SDK absent → sink_from_env returns the JSONL sink alone (no crash, no tee)."""
    monkeypatch.setenv("FLEET_TRACE_PATH", str(tmp_path / "coord.jsonl"))
    monkeypatch.setattr(trace, "_Langfuse", None)
    sink = sink_from_env()
    assert isinstance(sink, JsonlTraceSink)


def test_sink_from_env_missing_creds_returns_jsonl_only(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """SDK present but creds unset → JSONL only (Langfuse silently skipped)."""
    monkeypatch.setenv("FLEET_TRACE_PATH", str(tmp_path / "coord.jsonl"))
    monkeypatch.setattr(trace, "_Langfuse", _FakeLangfuse)
    for var in ("LANGFUSE_PUBLIC_KEY", "LANGFUSE_SECRET_KEY", "LANGFUSE_BASE_URL"):
        monkeypatch.delenv(var, raising=False)
    assert isinstance(sink_from_env(), JsonlTraceSink)


def test_sink_from_env_placeholder_creds_returns_jsonl_only(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Placeholder creds (wrong prefix) → JSONL only; the #23823 silent-drop guard."""
    monkeypatch.setenv("FLEET_TRACE_PATH", str(tmp_path / "coord.jsonl"))
    monkeypatch.setattr(trace, "_Langfuse", _FakeLangfuse)
    monkeypatch.setenv("LANGFUSE_PUBLIC_KEY", "your-key-here")
    monkeypatch.setenv("LANGFUSE_SECRET_KEY", "your-secret-here")
    monkeypatch.setenv("LANGFUSE_BASE_URL", "http://192.0.2.73:3000")
    assert isinstance(sink_from_env(), JsonlTraceSink)


def test_sink_from_env_valid_creds_tees_jsonl_and_langfuse(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """SDK + real-looking creds → a tee of JSONL and Langfuse over one record flow."""
    target = tmp_path / "coord.jsonl"
    monkeypatch.setenv("FLEET_TRACE_PATH", str(target))
    monkeypatch.setattr(trace, "_Langfuse", _FakeLangfuse)
    monkeypatch.setenv("LANGFUSE_PUBLIC_KEY", "pk-lf-abc")
    monkeypatch.setenv("LANGFUSE_SECRET_KEY", "sk-lf-xyz")
    monkeypatch.setenv("LANGFUSE_BASE_URL", "http://192.0.2.73:3000")
    sink = sink_from_env()
    assert isinstance(sink, TeeTraceSink)
    # one emit lands in BOTH children from the identical record
    sink.emit(_rec("t-1", "dispatch", subject="dev.x"))
    assert target.read_text(encoding="utf-8").count("\n") == 1


def test_tee_child_failure_does_not_block_siblings(tmp_path: Path) -> None:
    """A raising child sink is logged and the other child still gets the record."""

    class _Boom:
        def emit(self, record: TraceRecord) -> None:
            raise RuntimeError("boom")

        def flush(self) -> None: ...

    target = tmp_path / "coord.jsonl"
    tee = TeeTraceSink(_Boom(), JsonlTraceSink(target))
    tee.emit(_rec("t-1", "dispatch"))  # first child raises, second still writes
    assert target.read_text(encoding="utf-8").count("\n") == 1


# --- flush at shutdown: batching sinks must drain their tail ---------------------
#
# The Langfuse SDK queues spans on a background thread; without a shutdown flush
# the tail of a run is lost on SIGTERM. The Protocol carries flush() (no-op on the
# eager sinks); the Langfuse sink drains via client.flush(); the tee fans out; and
# the module-level flush() routes to the active sink.


def test_langfuse_flush_drains_the_client() -> None:
    """LangfuseTraceSink.flush() calls the SDK client's flush (drains the queue)."""
    client = _FakeLangfuse()
    LangfuseTraceSink(client).flush()
    assert client.flushed == 1


def test_module_flush_routes_to_the_active_sink() -> None:
    """trace.flush() drains the ALREADY-active sink (the shutdown-path call)."""
    client = _FakeLangfuse()
    trace.set_sink(LangfuseTraceSink(client))
    trace.flush()
    assert client.flushed == 1


def test_module_flush_is_a_noop_when_no_sink_resolved() -> None:
    """A run that never emitted has no active sink: flush() must NOT build one."""
    trace.set_sink(None)  # _active is None → nothing resolved yet
    trace.flush()  # must not raise and must not lazily build/re-init a sink
    # emit() is what lazily resolves; flush() must have left _active untouched
    assert trace._active is None


def test_tee_flush_fans_out_to_children(tmp_path: Path) -> None:
    """TeeTraceSink.flush() drains every child (JSONL no-op + Langfuse drain)."""
    client = _FakeLangfuse()
    tee = TeeTraceSink(JsonlTraceSink(tmp_path / "c.jsonl"), LangfuseTraceSink(client))
    tee.flush()
    assert client.flushed == 1


def test_tee_flush_child_failure_does_not_block_siblings() -> None:
    """A child whose flush raises is logged; the other child still drains."""

    class _BoomFlush:
        def emit(self, record: TraceRecord) -> None: ...

        def flush(self) -> None:
            raise RuntimeError("flush boom")

    client = _FakeLangfuse()
    TeeTraceSink(_BoomFlush(), LangfuseTraceSink(client)).flush()
    assert client.flushed == 1


def test_null_and_jsonl_flush_are_noops(tmp_path: Path) -> None:
    """The eager sinks implement flush() as a harmless no-op (nothing buffered)."""
    NullTraceSink().flush()
    JsonlTraceSink(tmp_path / "c.jsonl").flush()  # must not create the file
    assert not (tmp_path / "c.jsonl").exists()


# --- fail-loud log lines: the "loud" half of fail-loud is pinned (review P1-3) --
#
# Repo convention (test_behaviors.py:415, test_loop.py): capture loguru via an
# added sink, not caplog. These pin the operator-visibility lines the PR's thesis
# rests on — deleting the logger calls must turn a test red.


@pytest.fixture
def captured_logs() -> Iterator[list[str]]:
    """Yield a growing list of formatted loguru messages (all levels)."""
    logged: list[str] = []
    sink_id = logger.add(lambda m: logged.append(str(m)), level="DEBUG", format="{message}")
    try:
        yield logged
    finally:
        logger.remove(sink_id)


def test_enabled_line_fires_with_resolved_path(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, captured_logs: list[str]
) -> None:
    """sink_from_env logs `JSONL trace ENABLED path=<resolved>` naming the path."""
    target = tmp_path / "coord.jsonl"
    monkeypatch.setenv("FLEET_TRACE_PATH", str(target))
    sink_from_env()
    assert any("JSONL trace ENABLED" in line and str(target) in line for line in captured_logs), (
        captured_logs
    )


def test_disabled_line_fires_when_off(
    monkeypatch: pytest.MonkeyPatch, captured_logs: list[str]
) -> None:
    """sink_from_env logs `JSONL trace DISABLED` when neither path nor prod is set."""
    monkeypatch.delenv("FLEET_TRACE_PATH", raising=False)
    monkeypatch.setenv("FLEET_ENV", "dev")
    sink_from_env()
    assert any("JSONL trace DISABLED" in line for line in captured_logs), captured_logs


def test_prod_default_unwritable_refuses_startup(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, captured_logs: list[str]
) -> None:
    """F1: an unwritable prod-default path REFUSES startup (raises), never ENABLED.

    Wiring-time writability probe (P1-2) tightened for prod: the prod trace is the
    incident/backtesting foundation, so an unwritable destination must take the hub
    down at startup rather than silently degrade behind an ENABLED-looking posture.
    """
    # Point the prod default at a path whose parent is a FILE (mkdir -> NotADirectory).
    blocker = tmp_path / "blocker"
    blocker.write_text("i am a file, not a dir", encoding="utf-8")
    unwritable = blocker / "trace" / "coordination.jsonl"
    monkeypatch.delenv("FLEET_TRACE_PATH", raising=False)
    monkeypatch.setenv("FLEET_ENV", "prod")
    monkeypatch.setattr(trace, "PRODUCTION_TRACE_PATH", str(unwritable))
    with pytest.raises(RuntimeError, match="NOT WRITABLE in prod"):
        sink_from_env()
    assert not any("JSONL trace ENABLED" in line for line in captured_logs), captured_logs


def test_write_failure_first_log_is_error(tmp_path: Path, captured_logs: list[str]) -> None:
    """The FIRST JsonlTraceSink write failure logs at ERROR (prod depends on it, P1-2)."""
    sink = JsonlTraceSink(tmp_path)  # path IS a dir → open('a') raises IsADirectoryError
    sink.emit(TraceRecord(ts="2026-07-07T00:00:00Z", task_id="t-fail"))
    assert any(
        "sink write failed" in line and "tracing degraded" in line for line in captured_logs
    ), captured_logs


# --- FLEET_ENV exact-match parity with BusConfig (review P2-7) -----------------


@pytest.mark.parametrize("value", ["Prod", " prod ", "PROD", "prod\n", "production", ""])
def test_non_exact_prod_env_leaves_trace_off(value: str, monkeypatch: pytest.MonkeyPatch) -> None:
    """Only the EXACT string 'prod' arms the trace — matching BusConfig's contract.

    'Prod'/' prod '/etc are NOT prod for BusConfig (it raises at connect), so they
    must not silently arm the trace here either (the two consumers must agree).
    """
    monkeypatch.delenv("FLEET_TRACE_PATH", raising=False)
    monkeypatch.setenv("FLEET_ENV", value)
    assert isinstance(sink_from_env(), NullTraceSink)


def test_exact_prod_env_arms_trace(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """The exact string 'prod' (and only it) arms the prod-default trace."""
    monkeypatch.delenv("FLEET_TRACE_PATH", raising=False)
    monkeypatch.setenv("FLEET_ENV", "prod")
    # Writable prod default: the real /var/lib path is unwritable in the sandbox and
    # prod refuses an unwritable dest (F1); this test only asserts the arm-on-prod rule.
    monkeypatch.setattr(
        trace, "PRODUCTION_TRACE_PATH", str(tmp_path / "prod" / "coordination.jsonl")
    )
    assert isinstance(sink_from_env(), JsonlTraceSink)


def test_empty_trace_path_falls_through_to_env(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """An empty-string FLEET_TRACE_PATH is falsy → falls through to the FLEET_ENV rule (P3)."""
    monkeypatch.setenv("FLEET_TRACE_PATH", "")
    monkeypatch.setenv("FLEET_ENV", "dev")
    assert isinstance(sink_from_env(), NullTraceSink)
    monkeypatch.setenv("FLEET_ENV", "prod")
    # Writable prod default so the prod-refuse guard (F1) doesn't fire on /var/lib.
    monkeypatch.setattr(
        trace, "PRODUCTION_TRACE_PATH", str(tmp_path / "prod" / "coordination.jsonl")
    )
    assert isinstance(sink_from_env(), JsonlTraceSink)  # empty path ⇒ prod default


# --- fsync durability (review P2-9) -------------------------------------------


def test_emit_fsyncs_each_line(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Each emit fsyncs the file descriptor so an accepted record is on stable storage."""
    real_fsync = os.fsync
    synced: list[int] = []

    def _recording_fsync(fd: int) -> None:
        synced.append(fd)
        real_fsync(fd)

    # Patch the fsync the sink actually calls (trace.py does `os.fsync(...)`).
    monkeypatch.setattr("fleet_orchestrator.trace.os.fsync", _recording_fsync)
    sink = JsonlTraceSink(tmp_path / "coord.jsonl")
    sink.emit(TraceRecord(ts="2026-07-07T00:00:00Z", task_id="t-fsync"))
    assert len(synced) == 1  # exactly one fsync for the one record


# --- size rotation: the prod-default trace is hard-bounded (review P2 #133) ----


def _size_rec(i: int) -> TraceRecord:
    """A trace record whose task id makes each JSON line comfortably multi-byte."""
    return TraceRecord(ts="2026-07-07T00:00:00Z", task_id=f"task-{i:06d}-padding")


def test_trace_rotates_and_bounds_total_files(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Writing far past the cap keeps at most base + _MAX_TRACE_BACKUPS files.

    Tiny cap so a handful of lines rolls over; the bound must hold no matter how
    many records are written — the oldest backup is discarded on each rollover.
    """
    monkeypatch.setattr(trace, "_MAX_TRACE_BYTES", 200)  # tiny cap → rolls fast
    monkeypatch.setattr(trace, "_MAX_TRACE_BACKUPS", 3)
    base = tmp_path / "coord.jsonl"
    sink = JsonlTraceSink(base)

    for i in range(500):  # far more than fits under the cap
        sink.emit(_size_rec(i))

    files = sorted(p.name for p in tmp_path.iterdir())
    # At most base + 3 backups — never unbounded growth.
    assert files == ["coord.jsonl", "coord.jsonl.1", "coord.jsonl.2", "coord.jsonl.3"], files
    # Every retained file is at/under the cap plus at most one line's overshoot
    # (rotation happens BEFORE the append, so the active file can carry one line
    # over the threshold before the next emit rolls it).
    for p in tmp_path.iterdir():
        assert p.stat().st_size <= trace._MAX_TRACE_BYTES + 256, (p.name, p.stat().st_size)


def test_trace_rotation_preserves_newest_records(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """After rollover the ACTIVE file holds the most-recent records (oldest rotated out)."""
    monkeypatch.setattr(trace, "_MAX_TRACE_BYTES", 200)
    monkeypatch.setattr(trace, "_MAX_TRACE_BACKUPS", 2)
    base = tmp_path / "coord.jsonl"
    sink = JsonlTraceSink(base)
    for i in range(200):
        sink.emit(_size_rec(i))
    # The last record written must be in the active (base) file — newest survives.
    active = base.read_text(encoding="utf-8")
    assert "task-000199-padding" in active


def test_trace_rotation_error_degrades_never_raises(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A rotation failure is swallowed+logged like a write failure — never crashes."""
    monkeypatch.setattr(trace, "_MAX_TRACE_BYTES", 100)

    def _boom(self: JsonlTraceSink) -> None:
        raise OSError("rotation failed (disk gone)")

    sink = JsonlTraceSink(tmp_path / "coord.jsonl")
    sink.emit(_size_rec(0))  # seed a file so the next emit tries to rotate
    # Force the file over the cap, then make rotation raise.
    (tmp_path / "coord.jsonl").write_text("x" * 200, encoding="utf-8")
    monkeypatch.setattr(JsonlTraceSink, "_rotate_if_needed", _boom)
    sink.emit(_size_rec(1))  # must NOT raise — degrade path swallows the rotation error
    sink.emit(_size_rec(2))  # repeat failure stays on the debug path, still no raise
