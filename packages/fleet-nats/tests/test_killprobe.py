"""Recovery behavior for the isolated durable kill-authority probe (#107)."""

from __future__ import annotations

import asyncio
import time
from typing import cast

import pytest

import fleet_nats.killprobe as probe_module
from fleet_nats import BusConfig
from fleet_nats.killflag import FleetKVKillAuthority
from fleet_nats.killprobe import StoreKillProbe


class _AuthorityBus:
    """Transport-faithful control connection, intentionally separate from workers."""

    opened = 0
    closed = 0

    def __init__(self, config: BusConfig) -> None:
        self.config = config

    async def connect(self) -> None:
        type(self).opened += 1

    async def close(self) -> None:
        type(self).closed += 1


class _HungAuthority:
    async def armed(self) -> bool:
        await asyncio.Event().wait()
        raise AssertionError("unreachable")


class _ReleasedAuthority:
    async def armed(self) -> bool:
        return False


def test_post_connect_read_timeout_reconnects_and_observes_release(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A poisoned KV read is discarded; the replacement connection sees release."""
    opened: list[object] = [_HungAuthority(), _ReleasedAuthority()]

    async def _open(_: _AuthorityBus) -> FleetKVKillAuthority:
        return cast(FleetKVKillAuthority, opened.pop(0))

    _AuthorityBus.opened = 0
    _AuthorityBus.closed = 0
    monkeypatch.setattr(probe_module, "FleetBus", _AuthorityBus)
    monkeypatch.setattr(probe_module, "open_kill_authority", _open)
    probe = StoreKillProbe(BusConfig(agent_id="ops", env="dev"), timeout_s=0.02)
    probe.start()
    try:
        assert probe.read() is None
        deadline = time.monotonic() + 1
        while _AuthorityBus.opened < 2 and time.monotonic() < deadline:
            time.sleep(0.005)
        assert _AuthorityBus.opened >= 2
        assert probe.read() is False
    finally:
        probe.stop()
    assert _AuthorityBus.closed >= 2
