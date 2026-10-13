"""Operator command tests for the shared durable authority (#107)."""

from __future__ import annotations

import asyncio
import time
from typing import Any

from fleet_orchestrator.kill import main


class _Authority:
    def __init__(self, armed: bool = False) -> None:
        self.value = armed

    async def armed(self) -> bool:
        return self.value

    async def set_armed(self, armed: bool) -> bool:
        self.value = armed
        return armed

    async def initialize_released(self) -> bool:
        self.value = False
        return False


class _Bus:
    async def connect(self) -> None:
        return None

    async def close(self) -> None:
        return None


def _wire(monkeypatch: Any, authority: _Authority) -> None:
    import fleet_orchestrator.kill as module

    monkeypatch.setattr(module, "FleetBus", lambda config: _Bus())

    async def _open(bus: _Bus) -> _Authority:
        return authority

    monkeypatch.setattr(module, "open_kill_authority", _open)


def test_arm_release_and_status_use_the_same_authority(monkeypatch: Any) -> None:
    authority = _Authority()
    _wire(monkeypatch, authority)
    assert main(["initialize"]) == 0
    assert main(["status"]) == 1
    assert main([]) == 0
    assert authority.value is True
    assert main(["status"]) == 0
    assert main(["release"]) == 0
    assert authority.value is False


def test_store_failure_never_claims_a_success(monkeypatch: Any) -> None:
    import fleet_orchestrator.kill as module

    monkeypatch.setattr(module, "FleetBus", lambda config: _Bus())

    async def _broken(bus: _Bus) -> _Authority:
        raise RuntimeError("store unavailable")

    monkeypatch.setattr(module, "open_kill_authority", _broken)
    assert main(["arm"]) == 2


def test_hung_connect_respects_the_hard_aggregate_deadline(monkeypatch: Any) -> None:
    import fleet_orchestrator.kill as module

    class _HungBus(_Bus):
        async def connect(self) -> None:
            await asyncio.Event().wait()

    monkeypatch.setenv("FLEET_KILL_SWITCH_DEADLINE_SECONDS", "0.05")
    monkeypatch.setattr(module, "FleetBus", lambda config: _HungBus())
    started = time.monotonic()
    assert main(["arm"]) == 2
    assert time.monotonic() - started < 0.25


def test_hung_store_action_respects_the_same_deadline(monkeypatch: Any) -> None:
    class _HungAuthority(_Authority):
        async def set_armed(self, armed: bool) -> bool:
            await asyncio.Event().wait()
            raise AssertionError("unreachable after intentional hung action")

    authority = _HungAuthority()
    _wire(monkeypatch, authority)
    monkeypatch.setenv("FLEET_KILL_SWITCH_DEADLINE_SECONDS", "0.05")
    started = time.monotonic()
    assert main(["arm"]) == 2
    assert time.monotonic() - started < 0.25
