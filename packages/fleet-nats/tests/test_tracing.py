"""Behavior tests for opt-in fleet boundary tracing."""

from __future__ import annotations

import importlib
import inspect
from collections.abc import Iterator, Mapping
from threading import Event, Thread

import pytest
from loguru import logger

import fleet_nats.tracing as tracing
from fleet_nats import Envelope


@pytest.fixture(autouse=True)
def isolate_trace_environment(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    """Keep ordinary tracing reloads from inheriting persistent incident sinks."""
    monkeypatch.delenv("FLEET_TRACE", raising=False)
    monkeypatch.delenv("FLEET_TRACE_FILE", raising=False)
    importlib.reload(tracing)
    yield
    monkeypatch.delenv("FLEET_TRACE", raising=False)
    monkeypatch.delenv("FLEET_TRACE_FILE", raising=False)
    importlib.reload(tracing)


@pytest.fixture
def trace_enabled(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    """Reload tracing enabled, then restore its disabled process default."""
    monkeypatch.setenv("FLEET_TRACE", "1")
    monkeypatch.delenv("FLEET_TRACE_FILE", raising=False)
    importlib.reload(tracing)
    yield
    monkeypatch.delenv("FLEET_TRACE", raising=False)
    monkeypatch.delenv("FLEET_TRACE_FILE", raising=False)
    importlib.reload(tracing)


def _records() -> tuple[list[str], int]:
    records: list[str] = []
    sink = logger.add(
        lambda message: records.append(str(message)), level="DEBUG", format="{message}"
    )
    return records, sink


def test_traced_logs_sync_in_out_and_redacts(trace_enabled: None) -> None:
    records, sink = _records()
    envelope = Envelope.new(
        msg_id="m1",
        ts="now",
        sender="worker",
        kind="status",
        task_id="t1",
        payload={"text": "private agent prose"},
    )

    @tracing.traced
    def boundary(item: Envelope, *, api_key: str, details: dict[str, str]) -> str:
        return f"ok {api_key}"

    try:
        boundary(envelope, api_key="sk-abcdefghijklmnop1234", details={"message": "private"})
    finally:
        logger.remove(sink)

    output = "\n".join(records)
    assert "event=in" in output
    assert "event=out" in output
    assert "task_id" in output and "t1" in output
    assert "private agent prose" not in output
    assert "sk-abcdefghijklmnop1234" not in output
    assert "private" not in output


def test_traced_logs_error_and_reraises_unchanged(trace_enabled: None) -> None:
    records, sink = _records()
    expected = ValueError("bad request")

    @tracing.traced
    def boundary() -> None:
        raise expected

    try:
        with pytest.raises(ValueError) as caught:
            boundary()
    finally:
        logger.remove(sink)
    assert caught.value is expected
    assert "event=error" in "\n".join(records)


def test_traced_preserves_sync_exception_when_rendering_raises_keyboard_interrupt(
    trace_enabled: None,
) -> None:
    class UnrenderableError(Exception):
        def __str__(self) -> str:
            raise KeyboardInterrupt("message rendering interrupted")

    expected = UnrenderableError()

    @tracing.traced
    def boundary() -> None:
        raise expected

    with pytest.raises(UnrenderableError) as caught:
        boundary()

    assert caught.value is expected


@pytest.mark.asyncio
async def test_traced_preserves_async_exception_when_rendering_raises_keyboard_interrupt(
    trace_enabled: None,
) -> None:
    class UnrenderableError(Exception):
        def __str__(self) -> str:
            raise KeyboardInterrupt("message rendering interrupted")

    expected = UnrenderableError()

    @tracing.traced
    async def boundary() -> None:
        raise expected

    with pytest.raises(UnrenderableError) as caught:
        await boundary()

    assert caught.value is expected


@pytest.mark.asyncio
async def test_traced_supports_async_boundaries(trace_enabled: None) -> None:
    @tracing.traced
    async def boundary(value: int) -> int:
        return value + 1

    assert await boundary(2) == 3
    assert boundary.__name__ == "boundary"


def test_disabled_mode_skips_sanitizer(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("FLEET_TRACE", raising=False)
    importlib.reload(tracing)
    calls = 0

    def counting_sanitize(value: object, *, key: object | None = None) -> object:
        nonlocal calls
        calls += 1
        return value

    monkeypatch.setattr(tracing, "sanitize", counting_sanitize)

    @tracing.traced
    def boundary(value: int) -> int:
        return value + 1

    assert boundary(4) == 5
    assert calls == 0


@pytest.mark.asyncio
async def test_disabled_mode_bypasses_trace_work_for_async_and_sync(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("FLEET_TRACE", raising=False)
    importlib.reload(tracing)

    def unexpected(*args: object, **kwargs: object) -> object:
        raise AssertionError("disabled tracing performed trace work")

    monkeypatch.setattr(tracing, "_arguments", unexpected)
    monkeypatch.setattr(tracing, "_emit", unexpected)
    monkeypatch.setattr(inspect, "signature", unexpected)

    @tracing.traced
    def sync_boundary(value: int) -> int:
        return value + 1

    @tracing.traced
    async def async_boundary(value: int) -> int:
        return value + 1

    assert sync_boundary(4) == 5
    assert await async_boundary(5) == 6


def test_sanitizer_failure_is_visible_and_non_fatal(
    trace_enabled: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    records, sink = _records()

    def broken_sanitize(value: object, *, key: object | None = None) -> object:
        raise RuntimeError("broken")

    monkeypatch.setattr(tracing, "sanitize", broken_sanitize)

    @tracing.traced
    def boundary(value: object) -> str:
        return "ok"

    try:
        assert boundary(object()) == "ok"
    finally:
        logger.remove(sink)
    assert "sanitization failed" in "\n".join(records)


@pytest.mark.parametrize(
    ("key", "canary"),
    [
        ("clientSecret", "opaque-client-secret-canary"),
        ("apiKey", "opaque-api-key-canary"),
        ("title", "private-title-canary"),
        ("body", "private-body-canary"),
        ("reason", "private-reason-canary"),
        ("baseUrl", "https://user:password@registry.example/catalog?token=url-canary"),
    ],
)
def test_traced_nested_private_values_never_reach_records(
    trace_enabled: None, key: str, canary: str
) -> None:
    records, sink = _records()

    @tracing.traced
    def boundary(details: dict[str, object]) -> None:
        return None

    try:
        boundary({"outer": {key: canary}})
    finally:
        logger.remove(sink)

    assert canary not in "\n".join(records)


def test_sanitize_bounds_and_redacts_strings_bytes_sequences_and_mappings() -> None:
    credential = "sk-abcdefghijklmnop1234"
    long_value = "v" * (tracing._MAX_VALUE_CHARS + 1)
    oversized_mapping = {f"field-{item}": item for item in range(tracing._MAX_MAPPING_ITEMS + 5)}
    oversized_sequence = list(range(tracing._MAX_MAPPING_ITEMS + 5))

    assert credential not in str(tracing.sanitize(credential))
    assert str(tracing.sanitize(long_value)).endswith("...[truncated]")
    assert tracing.sanitize(b"x" * 1000) == "<bytes:1000>"
    sanitized_sequence = tracing.sanitize(oversized_sequence)
    sanitized_mapping = tracing.sanitize(oversized_mapping)
    assert isinstance(sanitized_sequence, list)
    assert isinstance(sanitized_mapping, dict)
    assert len(sanitized_sequence) == tracing._MAX_MAPPING_ITEMS
    assert len(sanitized_mapping) == tracing._MAX_MAPPING_ITEMS


def test_sanitize_bounds_mapping_keys_and_contains_hostile_key_rendering() -> None:
    class HostileKey:
        def __str__(self) -> str:
            raise RuntimeError("must not render")

    key = "key-" + "x" * (tracing._MAX_KEY_CHARS + 100)
    sanitized = tracing.sanitize(
        {
            key: "value",
            "clientSecret=opaque-key-canary": "value",
            "body=prose-key-canary": "value",
            HostileKey(): "hostile-value-canary",
        }
    )

    assert isinstance(sanitized, dict)
    assert all(len(item_key) <= tracing._MAX_KEY_CHARS for item_key in sanitized)
    assert "opaque-key-canary" not in repr(sanitized)
    assert "prose-key-canary" not in repr(sanitized)
    assert "hostile-value-canary" not in repr(sanitized)
    assert sanitized[tracing._UNAVAILABLE] == tracing._UNAVAILABLE


def test_sanitize_mapping_consumes_no_more_than_its_item_budget() -> None:
    class CollidingProseMapping(Mapping[str, int]):
        def __init__(self, count: int) -> None:
            self.count = count
            self.consumed = 0

        def __getitem__(self, key: str) -> int:
            return int(key.rsplit("=", maxsplit=1)[1])

        def __iter__(self) -> Iterator[str]:
            for index in range(self.count):
                self.consumed += 1
                yield f"body={index}"

        def __len__(self) -> int:
            return self.count

    values = CollidingProseMapping(tracing._MAX_MAPPING_ITEMS + 21)
    sanitized = tracing.sanitize(values)

    assert values.consumed == tracing._MAX_MAPPING_ITEMS
    assert sanitized == {tracing._DROPPED: tracing._DROPPED}


def test_sanitize_preserves_metadata_keys_but_drops_serialized_prose_keys() -> None:
    private_body = "private-body-canary"
    private_reason = "private-reason-canary"
    sanitized = tracing.sanitize(
        {
            "message_id": "m-123",
            "reason_code": "retryable",
            "title_format": "[fleet] {task}",
            "body": private_body,
            f"body={private_body}": "ignored",
            f"reason:{private_reason}": "ignored",
        }
    )

    assert isinstance(sanitized, dict)
    assert sanitized["message_id"] == "m-123"
    assert sanitized["reason_code"] == "retryable"
    assert sanitized["title_format"] == "[fleet] {task}"
    assert sanitized[tracing._DROPPED] == tracing._DROPPED
    assert private_body not in repr(sanitized)
    assert private_reason not in repr(sanitized)


def test_emit_failures_are_non_fatal_for_sync_boundaries(
    trace_enabled: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    class BrokenTraceLogger:
        def log(self, *args: object, **kwargs: object) -> None:
            raise RuntimeError("sink unavailable")

    monkeypatch.setattr(logger, "bind", lambda **kwargs: BrokenTraceLogger())

    @tracing.traced
    def boundary(value: int) -> int:
        return value + 1

    assert boundary(1) == 2


@pytest.mark.asyncio
async def test_emit_failures_are_non_fatal_for_async_boundaries(
    trace_enabled: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    class BrokenTraceLogger:
        def log(self, *args: object, **kwargs: object) -> None:
            raise RuntimeError("sink unavailable")

    monkeypatch.setattr(logger, "bind", lambda **kwargs: BrokenTraceLogger())

    @tracing.traced
    async def boundary(value: int) -> int:
        return value + 1

    assert await boundary(1) == 2


def test_trace_file_filter_excludes_ordinary_records(trace_enabled: None) -> None:
    records: list[str] = []
    sink = logger.add(
        lambda message: records.append(str(message)),
        filter=tracing._is_trace_record,
        format="{message}",
    )
    try:
        logger.info("ordinary record canary")
        tracing._emit("DEBUG", "in", "boundary", {"ok": True})
    finally:
        logger.remove(sink)

    output = "\n".join(records)
    assert "ordinary record canary" not in output
    assert "fleet.trace | event=in" in output


def test_file_sink_is_trace_only_bounded_and_configured_once(
    trace_enabled: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls: list[tuple[object, dict[str, object]]] = []
    removals: list[int] = []
    monkeypatch.setenv("FLEET_TRACE_FILE", "/trace/boundary.log")
    monkeypatch.setattr(tracing, "_file_sink_configured", False)
    monkeypatch.setattr(tracing, "_file_sink_id", None)
    monkeypatch.setattr(tracing, "_file_sink_path", None)

    def add(path: object, **kwargs: object) -> int:
        calls.append((path, kwargs))
        return 1

    monkeypatch.setattr(logger, "add", add)
    monkeypatch.setattr(logger, "remove", removals.append)
    tracing._configure_file_sink()
    tracing._configure_file_sink()

    assert len(calls) == 1
    path, options = calls[0]
    assert path == "/trace/boundary.log"
    assert options["filter"] is tracing._is_trace_record
    assert options["rotation"] == "50 MB"
    assert options["retention"] == "7 days"
    assert options["compression"] == "zip"
    assert options["enqueue"] is True
    monkeypatch.delenv("FLEET_TRACE_FILE", raising=False)
    tracing._configure_file_sink()
    assert removals == [1]


def test_file_sink_configuration_serializes_concurrent_callers(
    trace_enabled: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls = 0
    removals: list[int] = []
    first_add_started = Event()
    release_first_add = Event()
    second_add_started = Event()
    monkeypatch.setenv("FLEET_TRACE_FILE", "/trace/boundary.log")
    monkeypatch.setattr(tracing, "_file_sink_configured", False)
    monkeypatch.setattr(tracing, "_file_sink_id", None)
    monkeypatch.setattr(tracing, "_file_sink_path", None)

    def add(*args: object, **kwargs: object) -> int:
        nonlocal calls
        calls += 1
        if calls == 1:
            first_add_started.set()
            assert release_first_add.wait(timeout=1)
        else:
            second_add_started.set()
        return 1

    monkeypatch.setattr(logger, "add", add)
    monkeypatch.setattr(logger, "remove", removals.append)
    first = Thread(target=tracing._configure_file_sink)
    second = Thread(target=tracing._configure_file_sink)
    first.start()
    assert first_add_started.wait(timeout=1)
    second.start()
    assert not second_add_started.wait(timeout=0.1)
    release_first_add.set()
    first.join(timeout=1)
    second.join(timeout=1)

    assert not first.is_alive()
    assert not second.is_alive()
    assert calls == 1
    monkeypatch.delenv("FLEET_TRACE_FILE", raising=False)
    tracing._configure_file_sink()
    assert removals == [1]


def test_reload_does_not_accumulate_persistent_file_sinks(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    additions: list[object] = []
    removals: list[int] = []
    monkeypatch.setenv("FLEET_TRACE", "1")
    monkeypatch.setenv("FLEET_TRACE_FILE", "/trace/reload.log")
    monkeypatch.setattr(tracing, "_file_sink_id", None)
    monkeypatch.setattr(tracing, "_file_sink_path", None)

    def add(path: object, **kwargs: object) -> int:
        additions.append(path)
        return 101

    monkeypatch.setattr(logger, "add", add)
    monkeypatch.setattr(logger, "remove", removals.append)

    importlib.reload(tracing)
    importlib.reload(tracing)

    assert additions == ["/trace/reload.log"]
    monkeypatch.delenv("FLEET_TRACE", raising=False)
    monkeypatch.delenv("FLEET_TRACE_FILE", raising=False)
    importlib.reload(tracing)
    assert removals == [101]


def test_reload_preserves_file_sink_lock_during_blocked_configuration(
    trace_enabled: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    additions: list[int] = []
    removals: list[int] = []
    first_add_started = Event()
    release_first_add = Event()
    second_add_started = Event()
    monkeypatch.setenv("FLEET_TRACE", "1")
    monkeypatch.setenv("FLEET_TRACE_FILE", "/trace/reload-race.log")
    monkeypatch.setattr(tracing, "_file_sink_configured", False)
    monkeypatch.setattr(tracing, "_file_sink_id", None)
    monkeypatch.setattr(tracing, "_file_sink_path", None)

    def add(*args: object, **kwargs: object) -> int:
        sink_id = len(additions) + 1
        additions.append(sink_id)
        if sink_id == 1:
            first_add_started.set()
            assert release_first_add.wait(timeout=1)
        else:
            second_add_started.set()
        return sink_id

    monkeypatch.setattr(logger, "add", add)
    monkeypatch.setattr(logger, "remove", removals.append)
    configure = Thread(target=tracing._configure_file_sink)
    reload = Thread(target=importlib.reload, args=(tracing,))
    configure.start()
    assert first_add_started.wait(timeout=1)
    reload.start()
    assert not second_add_started.wait(timeout=0.1)
    release_first_add.set()
    configure.join(timeout=1)
    reload.join(timeout=1)

    assert not configure.is_alive()
    assert not reload.is_alive()
    assert additions == [1]
    monkeypatch.delenv("FLEET_TRACE_FILE", raising=False)
    tracing._configure_file_sink()
    assert removals == additions


def test_file_sink_add_failure_is_visible_and_non_fatal(
    trace_enabled: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    records, sink = _records()
    monkeypatch.setenv("FLEET_TRACE_FILE", "/trace/unavailable.log")
    monkeypatch.setattr(tracing, "_file_sink_configured", False)

    def broken_add(*args: object, **kwargs: object) -> int:
        raise OSError("unavailable")

    monkeypatch.setattr(logger, "add", broken_add)
    try:
        tracing._configure_file_sink()
    finally:
        logger.remove(sink)

    assert "file sink unavailable" in "\n".join(records)
