"""Unit tests for the fleet_send tool (issue #60).

No real gateway, no network: a fake FleetBus proves the publish shape (envelope
fields + subject), a stub PluginContext proves the manager-only role gate, and
the contextvar proves task_id inheritance. Env is monkeypatch-scoped so no state
leaks across tests.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any

import pytest
from fleet_nats import BusConfig, Envelope, subjects

import fleet_subscriber.fleet_send as fs
from fleet_subscriber.fleet_send import (
    current_bus_task_id,
    register_fleet_send,
)

# --- Fakes --------------------------------------------------------------------


@dataclass
class FakeBus:
    config: BusConfig
    published: list[tuple[str, Envelope]] = field(default_factory=list)
    connected: bool = False
    closed: bool = False
    connect_error: Exception | None = None
    publish_error: Exception | None = None

    async def connect(self) -> None:
        if self.connect_error is not None:
            raise self.connect_error
        self.connected = True

    async def publish(self, subject: str, envelope: Envelope) -> None:
        if self.publish_error is not None:
            raise self.publish_error
        self.published.append((subject, envelope))

    async def close(self) -> None:
        self.closed = True


@dataclass
class StubCtx:
    """Records register_tool calls from the plugin register path."""

    tools: list[dict[str, Any]] = field(default_factory=list)

    def register_tool(self, **kwargs: Any) -> None:
        self.tools.append(kwargs)


def _install_fake_bus(monkeypatch: pytest.MonkeyPatch, fake: FakeBus) -> None:
    monkeypatch.setattr(fs, "FleetBus", lambda cfg: fake)


def _manager_env(monkeypatch: pytest.MonkeyPatch, *, agent: str = "heron") -> None:
    monkeypatch.setenv("FLEET_AGENT_ID", agent)
    monkeypatch.setenv("FLEET_ENV", "dev")
    monkeypatch.setenv("FLEET_NATS_URL", "nats://192.0.2.74:4222")
    monkeypatch.setenv("FLEET_ROLE", "manager")


# --- Role gate ----------------------------------------------------------------


def test_manager_registers_the_tool(monkeypatch: pytest.MonkeyPatch) -> None:
    _manager_env(monkeypatch)
    ctx = StubCtx()
    registered = register_fleet_send(ctx)
    assert registered is True
    assert len(ctx.tools) == 1
    tool = ctx.tools[0]
    assert tool["name"] == "fleet_send"
    assert tool["is_async"] is True
    assert tool["toolset"] == fs.FLEET_SEND_TOOLSET
    # Schema exposes the documented args and requires to + text.
    params = tool["schema"]["parameters"]
    assert set(params["properties"]) == {"to", "text", "channel", "task_id"}
    assert params["required"] == ["to", "text"]


def test_worker_role_gets_no_tool(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("FLEET_AGENT_ID", "wren")
    monkeypatch.setenv("FLEET_ROLE", "worker")
    ctx = StubCtx()
    registered = register_fleet_send(ctx)
    assert registered is False
    assert ctx.tools == []


def test_unset_role_defaults_to_worker_no_tool(monkeypatch: pytest.MonkeyPatch) -> None:
    # Unset FLEET_ROLE → worker (least-capable default) → no free-send tool.
    monkeypatch.setenv("FLEET_AGENT_ID", "wren")
    monkeypatch.delenv("FLEET_ROLE", raising=False)
    ctx = StubCtx()
    assert register_fleet_send(ctx) is False
    assert ctx.tools == []


# --- Publish shape ------------------------------------------------------------


async def test_publish_shape_correct(monkeypatch: pytest.MonkeyPatch) -> None:
    _manager_env(monkeypatch, agent="heron")
    fake = FakeBus(config=BusConfig.from_env("heron"))
    _install_fake_bus(monkeypatch, fake)

    result = await fs._fleet_send(
        {"to": "finch", "text": "join the arena deliberation", "channel": "arena"}
    )

    assert fake.connected is True
    assert fake.closed is True
    assert len(fake.published) == 1
    subject, env = fake.published[0]
    # dispatch is the lane a worker's subscriber forges into a turn.
    assert subject == subjects.agent_dispatch("dev", "finch", channel="arena")
    assert env.kind == "dispatch"
    assert env.sender == "heron"
    assert env.to == "finch"
    assert env.channel == "arena"
    # payload.text is what envelope_to_message turns into the worker's turn prompt.
    assert env.payload["text"] == "join the arena deliberation"
    # A minted (non-inherited) task id lands in the t_chat namespace.
    assert env.task_id is not None and env.task_id.startswith("t_chat")
    # Human/model-readable success string, not an error.
    assert "error" not in result
    assert "finch" in result


async def test_channel_defaults_to_arena(monkeypatch: pytest.MonkeyPatch) -> None:
    _manager_env(monkeypatch)
    fake = FakeBus(config=BusConfig.from_env("heron"))
    _install_fake_bus(monkeypatch, fake)

    await fs._fleet_send({"to": "wren", "text": "steering note"})

    subject, env = fake.published[0]
    assert subject == subjects.agent_dispatch("dev", "wren", channel="arena")
    assert env.channel == "arena"


# --- Identity guard -----------------------------------------------------------


async def test_missing_agent_id_fails_loud(monkeypatch: pytest.MonkeyPatch) -> None:
    # FLEET_AGENT_ID unset → BusConfig resolves sender to "unknown"; the tool must
    # refuse rather than publish onto the bus under a bogus identity.
    monkeypatch.delenv("FLEET_AGENT_ID", raising=False)
    monkeypatch.setenv("FLEET_ENV", "dev")
    monkeypatch.setenv("FLEET_NATS_URL", "nats://192.0.2.74:4222")
    fake = FakeBus(config=BusConfig.from_env())
    _install_fake_bus(monkeypatch, fake)

    result = await fs._fleet_send({"to": "finch", "text": "hi"})
    payload = json.loads(result)
    assert "FLEET_AGENT_ID" in payload["error"]
    # Nothing published under a bogus identity.
    assert fake.published == []
    assert fake.connected is False


# --- Arg validation -----------------------------------------------------------


async def test_missing_to_returns_error(monkeypatch: pytest.MonkeyPatch) -> None:
    _manager_env(monkeypatch)
    fake = FakeBus(config=BusConfig.from_env("heron"))
    _install_fake_bus(monkeypatch, fake)
    result = await fs._fleet_send({"text": "no target"})
    assert "'to'" in json.loads(result)["error"]
    assert fake.published == []


async def test_missing_text_returns_error(monkeypatch: pytest.MonkeyPatch) -> None:
    _manager_env(monkeypatch)
    fake = FakeBus(config=BusConfig.from_env("heron"))
    _install_fake_bus(monkeypatch, fake)
    result = await fs._fleet_send({"to": "finch"})
    assert "'text'" in json.loads(result)["error"]
    assert fake.published == []


# --- task_id inheritance ------------------------------------------------------


async def test_task_id_inherited_from_bus_context(monkeypatch: pytest.MonkeyPatch) -> None:
    _manager_env(monkeypatch)
    fake = FakeBus(config=BusConfig.from_env("heron"))
    _install_fake_bus(monkeypatch, fake)

    token = current_bus_task_id.set("k123")
    try:
        await fs._fleet_send({"to": "finch", "text": "deliberate on k123"})
    finally:
        current_bus_task_id.reset(token)

    _, env = fake.published[0]
    # The summoned exchange stays keyed to the same k-board task.
    assert env.task_id == "k123"


async def test_explicit_task_id_overrides_context(monkeypatch: pytest.MonkeyPatch) -> None:
    _manager_env(monkeypatch)
    fake = FakeBus(config=BusConfig.from_env("heron"))
    _install_fake_bus(monkeypatch, fake)

    token = current_bus_task_id.set("k123")
    try:
        await fs._fleet_send({"to": "finch", "text": "hi", "task_id": "k999"})
    finally:
        current_bus_task_id.reset(token)

    _, env = fake.published[0]
    assert env.task_id == "k999"


async def test_task_id_minted_when_no_context(monkeypatch: pytest.MonkeyPatch) -> None:
    _manager_env(monkeypatch)
    fake = FakeBus(config=BusConfig.from_env("heron"))
    _install_fake_bus(monkeypatch, fake)
    # No contextvar set (default None) → fresh chat id.
    assert current_bus_task_id.get() is None
    await fs._fleet_send({"to": "finch", "text": "cold start"})
    _, env = fake.published[0]
    assert env.task_id is not None and env.task_id.startswith("t_chat")


# --- Failure never crashes ----------------------------------------------------


async def test_publish_failure_returns_error_not_raise(monkeypatch: pytest.MonkeyPatch) -> None:
    _manager_env(monkeypatch)
    fake = FakeBus(
        config=BusConfig.from_env("heron"),
        publish_error=RuntimeError("bus wedged"),
    )
    _install_fake_bus(monkeypatch, fake)
    result = await fs._fleet_send({"to": "finch", "text": "hi"})
    assert "publish to finch failed" in json.loads(result)["error"]
    # The connection is still torn down on the failure path.
    assert fake.closed is True


async def test_connect_failure_returns_error_not_raise(monkeypatch: pytest.MonkeyPatch) -> None:
    _manager_env(monkeypatch)
    fake = FakeBus(
        config=BusConfig.from_env("heron"),
        connect_error=TimeoutError("server down"),
    )
    _install_fake_bus(monkeypatch, fake)
    result = await fs._fleet_send({"to": "finch", "text": "hi"})
    assert "failed" in json.loads(result)["error"]
    assert fake.closed is True
