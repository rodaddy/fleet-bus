"""Unit tests for the fleet_control tool (issue #65).

No real gateway, no network: a fake FleetBus captures the published control
frame (so we can round-trip it through ``fleet_nats.control.parse_control`` and
prove the shape matches the shared contract exactly), a stub PluginContext proves
the manager-only role gate, and the contextvar proves task_id inheritance. Env is
monkeypatch-scoped so no state leaks across tests.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any

import pytest
from fleet_nats import BusConfig, Envelope, control, subjects

import fleet_subscriber.fleet_control as fc
from fleet_subscriber.fleet_control import (
    MAX_BUDGET_MSGS_ADD,
    register_fleet_control,
)
from fleet_subscriber.fleet_send import current_bus_task_id

# --- Fakes --------------------------------------------------------------------


@dataclass
class FakeBus:
    config: BusConfig
    published: list[tuple[str, Envelope]] = field(default_factory=list)
    connected: bool = False
    closed: bool = False
    connect_error: Exception | None = None
    publish_error: Exception | None = None
    close_error: Exception | None = None

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
        if self.close_error is not None:
            raise self.close_error


@dataclass
class StubCtx:
    """Records register_tool calls from the plugin register path."""

    tools: list[dict[str, Any]] = field(default_factory=list)

    def register_tool(self, **kwargs: Any) -> None:
        self.tools.append(kwargs)


@dataclass
class _RegisterCtx(StubCtx):
    """A StubCtx that also records register_platform (for the register() guard)."""

    platforms: list[str] = field(default_factory=list)

    def register_platform(self, **kwargs: Any) -> None:
        self.platforms.append(kwargs["name"])


def _raise(exc: Exception) -> Any:
    """A callable that raises ``exc`` (to stand in for a faulting register step)."""

    def _f(_ctx: Any) -> bool:
        raise exc

    return _f


def _install_fake_bus(monkeypatch: pytest.MonkeyPatch, fake: FakeBus) -> None:
    monkeypatch.setattr(fc, "FleetBus", lambda cfg: fake)


def _manager_env(monkeypatch: pytest.MonkeyPatch, *, agent: str = "heron") -> None:
    monkeypatch.setenv("FLEET_AGENT_ID", agent)
    monkeypatch.setenv("FLEET_ENV", "dev")
    monkeypatch.setenv("FLEET_NATS_URL", "nats://192.0.2.74:4222")
    monkeypatch.setenv("FLEET_ROLE", "manager")


# --- Role gate ----------------------------------------------------------------


def test_manager_registers_the_tool(monkeypatch: pytest.MonkeyPatch) -> None:
    _manager_env(monkeypatch)
    ctx = StubCtx()
    registered = register_fleet_control(ctx)
    assert registered is True
    assert len(ctx.tools) == 1
    tool = ctx.tools[0]
    assert tool["name"] == "fleet_control"
    assert tool["is_async"] is True
    assert tool["toolset"] == fc.FLEET_CONTROL_TOOLSET
    # The registered handler and emoji are the module's own (no accidental wiring).
    assert tool["handler"] is fc._fleet_control
    assert tool["emoji"] == "🎛️"
    params = tool["schema"]["parameters"]
    assert set(params["properties"]) == {"op", "task_id", "budget_msgs_add", "reason"}
    assert params["required"] == ["op"]
    # The op enum is the control plane's closed set — not improvised here.
    assert set(params["properties"]["op"]["enum"]) == {
        control.OP_RESUME,
        control.OP_PAUSE,
    }


def test_worker_role_gets_no_tool(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("FLEET_AGENT_ID", "wren")
    monkeypatch.setenv("FLEET_ROLE", "worker")
    ctx = StubCtx()
    registered = register_fleet_control(ctx)
    assert registered is False
    assert ctx.tools == []


def test_unset_role_defaults_to_worker_no_tool(monkeypatch: pytest.MonkeyPatch) -> None:
    # Unset FLEET_ROLE → worker (least-capable default) → no governor lever.
    monkeypatch.setenv("FLEET_AGENT_ID", "wren")
    monkeypatch.delenv("FLEET_ROLE", raising=False)
    ctx = StubCtx()
    assert register_fleet_control(ctx) is False
    assert ctx.tools == []


# --- Publish shape matches the shared control contract ------------------------


async def test_publish_shape_round_trips_through_parse_control(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _manager_env(monkeypatch, agent="heron")
    fake = FakeBus(config=BusConfig.from_env("heron"))
    _install_fake_bus(monkeypatch, fake)

    result = await fc._fleet_control(
        {"op": "resume", "task_id": "t_42", "budget_msgs_add": 4, "reason": "per Rico"}
    )

    assert fake.connected is True
    assert fake.closed is True
    assert len(fake.published) == 1
    subject, env = fake.published[0]
    # Published on the task's control lane the hub watches.
    assert subject == subjects.hub_control("dev", "t_42")
    assert env.kind == control.CONTROL_KIND
    assert env.sender == "heron"
    # The frame parses back into a ControlOp — same contract the hub reads.
    parsed = control.parse_control(env)
    assert parsed is not None
    assert parsed.op == control.OP_RESUME
    assert parsed.task_id == "t_42"
    assert parsed.budget_msgs_add == 4
    # The audited WHY rides the frame (hub-inspectable), not only the local log.
    assert parsed.reason == "per Rico"
    assert env.payload["reason"] == "per Rico"
    # Human/model receipt, not an error — carries the audited reason.
    assert "error" not in result
    assert "t_42" in result and "budget +4" in result and "per Rico" in result


async def test_pause_op_publishes(monkeypatch: pytest.MonkeyPatch) -> None:
    _manager_env(monkeypatch)
    fake = FakeBus(config=BusConfig.from_env("heron"))
    _install_fake_bus(monkeypatch, fake)

    result = await fc._fleet_control({"op": "pause", "task_id": "t_9"})
    _, env = fake.published[0]
    parsed = control.parse_control(env)
    assert parsed is not None and parsed.op == control.OP_PAUSE
    assert parsed.budget_msgs_add is None
    assert "paused t_9" in result


# --- Budget bounds ------------------------------------------------------------


async def test_budget_zero_refused(monkeypatch: pytest.MonkeyPatch) -> None:
    _manager_env(monkeypatch)
    fake = FakeBus(config=BusConfig.from_env("heron"))
    _install_fake_bus(monkeypatch, fake)
    result = await fc._fleet_control({"op": "resume", "task_id": "t_1", "budget_msgs_add": 0})
    assert "positive" in json.loads(result)["error"]
    assert fake.published == []


async def test_budget_over_max_refused(monkeypatch: pytest.MonkeyPatch) -> None:
    _manager_env(monkeypatch)
    fake = FakeBus(config=BusConfig.from_env("heron"))
    _install_fake_bus(monkeypatch, fake)
    result = await fc._fleet_control(
        {"op": "resume", "task_id": "t_1", "budget_msgs_add": MAX_BUDGET_MSGS_ADD + 1}
    )
    assert str(MAX_BUDGET_MSGS_ADD) in json.loads(result)["error"]
    assert fake.published == []


async def test_budget_non_integer_refused(monkeypatch: pytest.MonkeyPatch) -> None:
    _manager_env(monkeypatch)
    fake = FakeBus(config=BusConfig.from_env("heron"))
    _install_fake_bus(monkeypatch, fake)
    result = await fc._fleet_control({"op": "resume", "task_id": "t_1", "budget_msgs_add": "lots"})
    assert "integer" in json.loads(result)["error"]
    assert fake.published == []


async def test_budget_on_pause_refused(monkeypatch: pytest.MonkeyPatch) -> None:
    # A budget bump is a resume-only concept; asking for it on a pause is a
    # caller error, refused before any frame is published.
    _manager_env(monkeypatch)
    fake = FakeBus(config=BusConfig.from_env("heron"))
    _install_fake_bus(monkeypatch, fake)
    result = await fc._fleet_control({"op": "pause", "task_id": "t_1", "budget_msgs_add": 5})
    assert "resume" in json.loads(result)["error"]
    assert fake.published == []


async def test_budget_at_max_accepted(monkeypatch: pytest.MonkeyPatch) -> None:
    # The boundary is inclusive: exactly MAX is allowed.
    _manager_env(monkeypatch)
    fake = FakeBus(config=BusConfig.from_env("heron"))
    _install_fake_bus(monkeypatch, fake)
    await fc._fleet_control(
        {"op": "resume", "task_id": "t_1", "budget_msgs_add": MAX_BUDGET_MSGS_ADD}
    )
    _, env = fake.published[0]
    parsed = control.parse_control(env)
    assert parsed is not None and parsed.budget_msgs_add == MAX_BUDGET_MSGS_ADD


# --- Op validation ------------------------------------------------------------


async def test_unknown_op_refused(monkeypatch: pytest.MonkeyPatch) -> None:
    _manager_env(monkeypatch)
    fake = FakeBus(config=BusConfig.from_env("heron"))
    _install_fake_bus(monkeypatch, fake)
    result = await fc._fleet_control({"op": "yeet", "task_id": "t_1"})
    assert "op must be one of" in json.loads(result)["error"]
    assert fake.published == []


# --- Task resolution: never guess ---------------------------------------------


async def test_no_task_and_no_context_refuses_without_publishing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # No explicit task_id and no bus-turn context → structured refusal, no frame.
    _manager_env(monkeypatch)
    fake = FakeBus(config=BusConfig.from_env("heron"))
    _install_fake_bus(monkeypatch, fake)
    assert current_bus_task_id.get() is None
    result = await fc._fleet_control({"op": "resume"})
    err = json.loads(result)["error"]
    assert "no task to steer" in err
    assert fake.published == []
    # Refused before touching the bus.
    assert fake.connected is False


async def test_task_id_inherited_from_bus_context(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _manager_env(monkeypatch)
    fake = FakeBus(config=BusConfig.from_env("heron"))
    _install_fake_bus(monkeypatch, fake)

    token = current_bus_task_id.set("t_ctx")
    try:
        result = await fc._fleet_control({"op": "resume"})
    finally:
        current_bus_task_id.reset(token)

    _, env = fake.published[0]
    parsed = control.parse_control(env)
    assert parsed is not None and parsed.task_id == "t_ctx"
    assert "t_ctx" in result


async def test_explicit_task_id_overrides_context(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _manager_env(monkeypatch)
    fake = FakeBus(config=BusConfig.from_env("heron"))
    _install_fake_bus(monkeypatch, fake)

    token = current_bus_task_id.set("t_ctx")
    try:
        await fc._fleet_control({"op": "pause", "task_id": "t_explicit"})
    finally:
        current_bus_task_id.reset(token)

    _, env = fake.published[0]
    parsed = control.parse_control(env)
    assert parsed is not None and parsed.task_id == "t_explicit"


# --- Identity guard -----------------------------------------------------------


async def test_missing_agent_id_fails_loud(monkeypatch: pytest.MonkeyPatch) -> None:
    # FLEET_AGENT_ID unset → sender resolves to "unknown"; refuse rather than
    # steer the governor under a bogus identity (the hub audits sender).
    monkeypatch.delenv("FLEET_AGENT_ID", raising=False)
    monkeypatch.setenv("FLEET_ENV", "dev")
    monkeypatch.setenv("FLEET_NATS_URL", "nats://192.0.2.74:4222")
    fake = FakeBus(config=BusConfig.from_env())
    _install_fake_bus(monkeypatch, fake)

    result = await fc._fleet_control({"op": "resume", "task_id": "t_1"})
    assert "FLEET_AGENT_ID" in json.loads(result)["error"]
    assert fake.published == []
    assert fake.connected is False


# --- Failure never crashes ----------------------------------------------------


async def test_publish_failure_returns_error_not_raise(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _manager_env(monkeypatch)
    fake = FakeBus(
        config=BusConfig.from_env("heron"),
        publish_error=RuntimeError("bus wedged"),
    )
    _install_fake_bus(monkeypatch, fake)
    result = await fc._fleet_control({"op": "resume", "task_id": "t_1"})
    assert "failed" in json.loads(result)["error"]
    # The connection is still torn down on the failure path.
    assert fake.closed is True


async def test_connect_failure_returns_error_not_raise(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _manager_env(monkeypatch)
    fake = FakeBus(
        config=BusConfig.from_env("heron"),
        connect_error=TimeoutError("server down"),
    )
    _install_fake_bus(monkeypatch, fake)
    result = await fc._fleet_control({"op": "resume", "task_id": "t_1"})
    assert "failed" in json.loads(result)["error"]
    assert fake.closed is True


async def test_close_failure_still_returns_receipt(monkeypatch: pytest.MonkeyPatch) -> None:
    # A bus-close fault after a good publish must not lose the receipt or crash —
    # it is logged (warning) and the successful steer still reports.
    _manager_env(monkeypatch)
    fake = FakeBus(
        config=BusConfig.from_env("heron"),
        close_error=RuntimeError("socket already gone"),
    )
    _install_fake_bus(monkeypatch, fake)
    result = await fc._fleet_control({"op": "resume", "task_id": "t_1"})
    # The publish happened; the receipt (not an error) is returned despite close.
    assert len(fake.published) == 1
    assert "error" not in result
    assert "resumed t_1" in result


# --- reason: audited WHY rides the frame (#65 review) --------------------------


async def test_reason_omitted_from_payload_when_absent(monkeypatch: pytest.MonkeyPatch) -> None:
    # No reason → the frame is byte-identical to a pre-reason frame (backward compat).
    _manager_env(monkeypatch)
    fake = FakeBus(config=BusConfig.from_env("heron"))
    _install_fake_bus(monkeypatch, fake)
    await fc._fleet_control({"op": "resume", "task_id": "t_1"})
    _, env = fake.published[0]
    assert "reason" not in env.payload
    parsed = control.parse_control(env)
    assert parsed is not None and parsed.reason is None


# --- task_id slug: subject/payload/receipt agree (#66 review) ------------------


async def test_mixed_case_dotted_task_id_slugged_consistently(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # A mixed-case/dotted id must key the subject, payload, and receipt as ONE
    # slugged value — never subject=t-42 while payload/receipt carry "T.42".
    _manager_env(monkeypatch)
    fake = FakeBus(config=BusConfig.from_env("heron"))
    _install_fake_bus(monkeypatch, fake)
    result = await fc._fleet_control({"op": "resume", "task_id": "T.42"})
    subject, env = fake.published[0]
    slugged = subjects.slug("T.42")  # "t-42"
    assert subject == subjects.hub_control("dev", slugged)
    parsed = control.parse_control(env)
    assert parsed is not None and parsed.task_id == slugged
    assert env.task_id == slugged
    assert slugged in result


# --- shared toolset constant (#66 review nit) ---------------------------------


def test_toolset_shared_with_fleet_send() -> None:
    from fleet_subscriber.fleet_send import FLEET_SEND_TOOLSET

    assert fc.FLEET_CONTROL_TOOLSET == FLEET_SEND_TOOLSET == "fleet_bus"


# --- registration guard: a raising register must not break the platform -------


def test_register_raises_on_bad_role(monkeypatch: pytest.MonkeyPatch) -> None:
    # A typo'd FLEET_ROLE makes SubscriberRole.from_env raise — register_fleet_control
    # surfaces it (the adapter's register() catches it; see the guard test below).
    monkeypatch.setenv("FLEET_AGENT_ID", "heron")
    monkeypatch.setenv("FLEET_ROLE", "boss")  # not worker/manager
    ctx = StubCtx()
    with pytest.raises(ValueError, match="FLEET_ROLE"):
        register_fleet_control(ctx)
    assert ctx.tools == []


def test_adapter_register_survives_fleet_control_raising(monkeypatch: pytest.MonkeyPatch) -> None:
    # The adapter's register() guards register_fleet_control: a raise there must
    # NOT abort register() — fleet_send (registered just before it) still lands.
    from fleet_subscriber import adapter as adapter_mod

    monkeypatch.setenv("FLEET_AGENT_ID", "heron")
    monkeypatch.setenv("FLEET_ROLE", "manager")
    monkeypatch.setenv("FLEET_NATS_URL", "nats://192.0.2.74:4222")

    # Neutralize the platform registration (needs the live gateway) — we only
    # exercise the tool-registration guards here.
    monkeypatch.setattr(adapter_mod, "load_gateway", lambda: object())
    monkeypatch.setattr(adapter_mod, "build_adapter_class", lambda _api: object)
    # Force the fleet_control registration to blow up.
    monkeypatch.setattr(
        adapter_mod,
        "register_fleet_control",
        _raise(RuntimeError("wiring fault")),
    )

    ctx = _RegisterCtx()
    # Must not raise despite the wiring fault.
    adapter_mod.register(ctx)
    names = [t["name"] for t in ctx.tools]
    assert "fleet_send" in names  # survived the fleet_control fault
    assert "fleet_control" not in names  # the faulting one did not register
    assert ctx.platforms == ["nats"]  # platform registration also unaffected


# --- bad bus config (#66 review minor) ----------------------------------------


async def test_bad_bus_config_returns_error(monkeypatch: pytest.MonkeyPatch) -> None:
    # An invalid FLEET_ENV makes BusConfig.from_env raise → "bad bus config",
    # nothing published.
    monkeypatch.setenv("FLEET_AGENT_ID", "heron")
    monkeypatch.setenv("FLEET_ROLE", "manager")
    monkeypatch.setenv("FLEET_NATS_URL", "nats://192.0.2.74:4222")
    monkeypatch.setenv("FLEET_ENV", "staging")  # not dev/prod
    # The tool's own BusConfig.from_env() will raise on the bad env; the fake's
    # config just needs to be a valid one (env defaults to dev).
    fake = FakeBus(config=BusConfig(agent_id="heron"))
    _install_fake_bus(monkeypatch, fake)
    result = await fc._fleet_control({"op": "resume", "task_id": "t_1"})
    assert "bad bus config" in json.loads(result)["error"]
    assert fake.published == []
    assert fake.connected is False


# --- no task_id stash leaks between turns (#66 review nit) ---------------------


async def test_two_sequential_calls_do_not_stash_task_id(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Off any bus turn, a first explicit-id call must not leave state that a second
    # (no-id) call inherits — the second must refuse, not reuse the first's id.
    _manager_env(monkeypatch)
    fake = FakeBus(config=BusConfig.from_env("heron"))
    _install_fake_bus(monkeypatch, fake)
    assert current_bus_task_id.get() is None

    first = await fc._fleet_control({"op": "resume", "task_id": "t_first"})
    assert "resumed t_first" in first
    assert len(fake.published) == 1

    second = await fc._fleet_control({"op": "resume"})
    assert "no task to steer" in json.loads(second)["error"]
    # No second frame — the first call's id did not leak.
    assert len(fake.published) == 1
