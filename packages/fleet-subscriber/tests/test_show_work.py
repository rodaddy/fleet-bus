"""Focused client tests for the hub-owned ``show_work`` tool (#64)."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any

import pytest
from fleet_nats import Envelope, subjects

from fleet_subscriber import show_work


@dataclass
class StubContext:
    tools: list[dict[str, Any]] = field(default_factory=list)

    def register_tool(self, **kwargs: Any) -> None:
        self.tools.append(kwargs)


@pytest.mark.parametrize("role", ["worker", "manager"])
def test_registers_for_every_role(monkeypatch: pytest.MonkeyPatch, role: str) -> None:
    monkeypatch.setenv("FLEET_ROLE", role)
    context = StubContext()
    assert show_work.register_show_work(context) is True
    assert context.tools[0]["name"] == "show_work"
    assert context.tools[0]["schema"]["parameters"]["additionalProperties"] is False


class FakeBus:
    response: Envelope | None = None
    subject = ""
    last_request: Envelope | None = None
    timeout = 0.0

    def __init__(self, _config: Any) -> None:
        pass

    async def connect(self) -> None:
        pass

    async def close(self) -> None:
        pass

    async def request(
        self, subject: str, envelope: Envelope, *, timeout_s: float
    ) -> Envelope | None:
        type(self).subject = subject
        type(self).last_request = envelope
        type(self).timeout = timeout_s
        return type(self).response


def _reply(request: Envelope, payload: dict[str, str]) -> Envelope:
    return Envelope.new(
        msg_id="reply",
        ts="t",
        sender="nagatha",
        kind="show_work",
        correlation_id=request.id,
        payload=payload,
    )


async def test_requests_sanctioned_subject_and_payload(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("FLEET_AGENT_ID", "skippy")
    monkeypatch.setenv("FLEET_NATS_URL", "nats://127.0.0.1:4222")
    monkeypatch.setenv("FLEET_ENV", "dev")
    monkeypatch.setattr(show_work, "FleetBus", FakeBus)
    original = show_work._request("task-1", "skippy")
    FakeBus.response = _reply(original, {"markdown": "## Summary\nredacted"})

    # The client generates a fresh id; mirror it in the fake response at request time.
    async def request(
        self: FakeBus, subject: str, envelope: Envelope, *, timeout_s: float
    ) -> Envelope:
        FakeBus.subject, FakeBus.last_request, FakeBus.timeout = subject, envelope, timeout_s
        return _reply(envelope, {"markdown": "## Summary\nredacted"})

    monkeypatch.setattr(FakeBus, "request", request)
    assert await show_work._show_work({"task_id": "task-1"}) == "## Summary\nredacted"
    assert FakeBus.subject == subjects.hub_show_work("dev")
    assert FakeBus.last_request is not None and FakeBus.last_request.payload == {
        "task_id": "task-1"
    }
    assert FakeBus.timeout == show_work._TIMEOUT_S


@pytest.mark.parametrize(
    "args", [{}, {"task_id": " "}, {"task_id": "x" * 257}, {"task_id": "x", "topic": "no"}]
)
async def test_invalid_input_never_opens_transport(args: dict[str, str]) -> None:
    output = json.loads(await show_work._show_work(args))
    assert output["error"] in {"show_work: 'task_id' is required", "show_work: invalid request"}


async def test_timeout_or_no_responder_is_safe(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("FLEET_AGENT_ID", "skippy")
    monkeypatch.setenv("FLEET_NATS_URL", "nats://127.0.0.1:4222")
    monkeypatch.setenv("FLEET_ENV", "dev")
    monkeypatch.setattr(show_work, "FleetBus", FakeBus)
    FakeBus.response = None
    assert (
        json.loads(await show_work._show_work({"task_id": "task"}))["error"]
        == "show_work: service unavailable"
    )


async def test_invalid_utf8_or_oversized_response_is_refused(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("FLEET_AGENT_ID", "skippy")
    monkeypatch.setenv("FLEET_NATS_URL", "nats://127.0.0.1:4222")
    monkeypatch.setenv("FLEET_ENV", "dev")
    monkeypatch.setattr(show_work, "FleetBus", FakeBus)

    async def bad_response(
        self: FakeBus, _subject: str, envelope: Envelope, *, timeout_s: float
    ) -> Envelope:
        return _reply(envelope, {"markdown": "\ud800"})

    monkeypatch.setattr(FakeBus, "request", bad_response)
    assert (
        json.loads(await show_work._show_work({"task_id": "task"}))["error"]
        == "show_work: response unavailable"
    )


async def test_markdown_budget_does_not_include_envelope_overhead(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("FLEET_AGENT_ID", "skippy")
    monkeypatch.setenv("FLEET_NATS_URL", "nats://127.0.0.1:4222")
    monkeypatch.setenv("FLEET_ENV", "dev")
    monkeypatch.setattr(show_work, "FleetBus", FakeBus)
    markdown = "x" * 11_990

    async def bounded_response(
        self: FakeBus, _subject: str, envelope: Envelope, *, timeout_s: float
    ) -> Envelope:
        response = _reply(envelope, {"markdown": markdown})
        assert len(response.to_bytes()) > show_work._MAX_RESPONSE_BYTES
        assert len(response.to_bytes()) <= show_work._MAX_RESPONSE_ENVELOPE_BYTES
        return response

    monkeypatch.setattr(FakeBus, "request", bounded_response)
    assert await show_work._show_work({"task_id": "task"}) == markdown


async def test_accepts_full_budget_unicode_markdown(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("FLEET_AGENT_ID", "skippy")
    monkeypatch.setenv("FLEET_NATS_URL", "nats://127.0.0.1:4222")
    monkeypatch.setenv("FLEET_ENV", "dev")
    monkeypatch.setattr(show_work, "FleetBus", FakeBus)
    markdown = "😀" * 3_000
    assert len(markdown.encode("utf-8")) == show_work._MAX_RESPONSE_BYTES

    async def bounded_response(
        self: FakeBus, _subject: str, envelope: Envelope, *, timeout_s: float
    ) -> Envelope:
        response = _reply(envelope, {"markdown": markdown})
        assert len(response.to_bytes()) <= show_work._MAX_RESPONSE_ENVELOPE_BYTES
        return response

    monkeypatch.setattr(FakeBus, "request", bounded_response)
    assert await show_work._show_work({"task_id": "task"}) == markdown


async def test_response_requires_exact_envelope_and_payload_shapes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("FLEET_AGENT_ID", "skippy")
    monkeypatch.setenv("FLEET_NATS_URL", "nats://127.0.0.1:4222")
    monkeypatch.setenv("FLEET_ENV", "dev")
    monkeypatch.setattr(show_work, "FleetBus", FakeBus)

    async def extra_envelope(
        self: FakeBus, _subject: str, envelope: Envelope, *, timeout_s: float
    ) -> Envelope:
        response = _reply(envelope, {"markdown": "ok"})
        response.extra["unexpected"] = "no"
        return response

    monkeypatch.setattr(FakeBus, "request", extra_envelope)
    assert json.loads(await show_work._show_work({"task_id": "task"}))["error"] == (
        "show_work: service unavailable"
    )

    async def populated_known_field(
        self: FakeBus, _subject: str, envelope: Envelope, *, timeout_s: float
    ) -> Envelope:
        response = _reply(envelope, {"markdown": "ok"})
        response.task_id = "unexpected-task"
        return response

    monkeypatch.setattr(FakeBus, "request", populated_known_field)
    assert json.loads(await show_work._show_work({"task_id": "task"}))["error"] == (
        "show_work: service unavailable"
    )

    async def extra_payload(
        self: FakeBus, _subject: str, envelope: Envelope, *, timeout_s: float
    ) -> Envelope:
        return _reply(envelope, {"markdown": "ok", "error": "not permitted"})

    monkeypatch.setattr(FakeBus, "request", extra_payload)
    assert json.loads(await show_work._show_work({"task_id": "task"}))["error"] == (
        "show_work: response unavailable"
    )

    async def oversized_envelope(
        self: FakeBus, _subject: str, envelope: Envelope, *, timeout_s: float
    ) -> Envelope:
        response = _reply(envelope, {"markdown": "ok"})
        response.id = "x" * show_work._MAX_RESPONSE_ENVELOPE_BYTES
        return response

    monkeypatch.setattr(FakeBus, "request", oversized_envelope)
    assert json.loads(await show_work._show_work({"task_id": "task"}))["error"] == (
        "show_work: response unavailable"
    )

    async def huge_response(
        self: FakeBus, _subject: str, envelope: Envelope, *, timeout_s: float
    ) -> Envelope:
        return _reply(envelope, {"markdown": "x" * (show_work._MAX_RESPONSE_BYTES + 1)})

    monkeypatch.setattr(FakeBus, "request", huge_response)
    assert (
        json.loads(await show_work._show_work({"task_id": "task"}))["error"]
        == "show_work: response unavailable"
    )
