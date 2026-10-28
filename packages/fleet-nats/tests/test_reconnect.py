"""Reconnect/resubscribe contract tests (#25, Phase 3.1).

Two layers:
- Config-knob unit tests (always run): the env-tunable reconnect policy is
  parsed strictly and fails loud on junk.
- A live kill/restart test against a LOCAL throwaway ``nats-server``
  subprocess (auto-skips when the binary is absent): proves core-NATS
  subscriptions ride nats-py's client subscription table across a reconnect —
  delivery resumes with no re-subscribe call from us. This is the local twin
  of the fleet-hub-restart runbook drill.
"""

from __future__ import annotations

import asyncio
import shutil
import socket
import subprocess
import time

import pytest

from fleet_nats import BusConfig, Envelope, FleetBus

_NATS_SERVER = shutil.which("nats-server")


def test_reconnect_knobs_from_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("FLEET_RECONNECT_MAX", raising=False)
    monkeypatch.delenv("FLEET_RECONNECT_WAIT_SECONDS", raising=False)
    cfg = BusConfig.from_env("finch")
    assert cfg.max_reconnect_attempts == -1  # unlimited-with-backoff default
    assert cfg.reconnect_wait_s == 2.0

    monkeypatch.setenv("FLEET_RECONNECT_MAX", "17")
    monkeypatch.setenv("FLEET_RECONNECT_WAIT_SECONDS", "5")
    cfg = BusConfig.from_env("finch")
    assert cfg.max_reconnect_attempts == 17
    assert cfg.reconnect_wait_s == 5


def test_reconnect_knobs_fail_loud_on_junk(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("FLEET_RECONNECT_MAX", "lots")
    with pytest.raises(ValueError, match="FLEET_RECONNECT_MAX must be an integer"):
        BusConfig.from_env("finch")
    monkeypatch.delenv("FLEET_RECONNECT_MAX", raising=False)

    monkeypatch.setenv("FLEET_RECONNECT_WAIT_SECONDS", "soon")
    with pytest.raises(ValueError, match="FLEET_RECONNECT_WAIT_SECONDS must be an integer"):
        BusConfig.from_env("finch")


def test_reconnect_knob_bounds_validated() -> None:
    with pytest.raises(ValueError, match="max_reconnect_attempts"):
        BusConfig(agent_id="finch", max_reconnect_attempts=-2)
    with pytest.raises(ValueError, match="reconnect_wait_s"):
        BusConfig(agent_id="finch", reconnect_wait_s=0)


@pytest.mark.asyncio
async def test_bus_wraps_single_url_no_comma_split() -> None:
    """Pin the guard/dialer coupling (fleet-bus #105 P3-7).

    The staging prod-URL guard (config.py) vets ONLY ``self.url`` as a single
    endpoint. bus.connect must therefore hand nats-py exactly ``[self.url]`` — a
    single wrapped URL — and never split it on commas. If a future change did
    ``servers=url.split(",")``, the guard would vet only the first entry and a
    prod endpoint could ride in on a later comma. This test fails the moment that
    coupling is broken.
    """
    from unittest.mock import AsyncMock, patch

    multi = "nats://192.0.2.99:4222,nats://192.0.2.98:4222"
    bus = FleetBus(BusConfig(agent_id="pin", url=multi, env="staging"))
    with patch.object(bus._nc, "connect", new=AsyncMock()) as mock_connect:
        await bus.connect()
    # Exactly one server, and it is the whole URL verbatim — no comma split.
    assert mock_connect.await_args is not None
    assert mock_connect.await_args.kwargs["servers"] == [multi]


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _start_server(port: int) -> subprocess.Popen[bytes]:
    proc = subprocess.Popen(  # noqa: S603 — fixed argv, local test binary
        [str(_NATS_SERVER), "-a", "127.0.0.1", "-p", str(port)],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.2):
                return proc
        except OSError:
            time.sleep(0.05)
    proc.kill()
    raise RuntimeError(f"local nats-server did not listen on {port} within 10s")


async def _wait_until(predicate, timeout_s: float, what: str) -> None:  # type: ignore[no-untyped-def]
    deadline = asyncio.get_running_loop().time() + timeout_s
    while asyncio.get_running_loop().time() < deadline:
        if predicate():
            return
        await asyncio.sleep(0.05)
    raise AssertionError(f"timed out waiting for {what}")


@pytest.mark.skipif(_NATS_SERVER is None, reason="no local nats-server binary")
async def test_reconnect_clean_close_never_logs_a_give_up() -> None:
    """nats-py fires disconnected/closed cbs on graceful close — a clean
    shutdown must not read like a reconnect give-up (false ERROR = alert
    fatigue against the runbook's real give-up signal)."""
    from loguru import logger

    port = _free_port()
    server = _start_server(port)
    captured: list[str] = []
    sink_id = logger.add(
        lambda message: captured.append(message.record["level"].name),
        filter=lambda record: "fleetbus.reconnect" in record["message"],
    )
    try:
        config = BusConfig(url=f"nats://127.0.0.1:{port}", agent_id="clean-close", env="dev")
        bus = FleetBus(config)
        await bus.connect()
        await bus.close()
        await asyncio.sleep(0.2)  # let nats-py run its callbacks
        assert "ERROR" not in captured
        assert "WARNING" not in captured
    finally:
        logger.remove(sink_id)
        server.kill()
        server.wait(timeout=10)


@pytest.mark.skipif(_NATS_SERVER is None, reason="no local nats-server binary")
async def test_reconnect_resubscribes_across_server_restart() -> None:
    """Kill the server mid-subscription; delivery must resume after restart."""
    port = _free_port()
    server = _start_server(port)
    received: list[Envelope] = []

    async def _handler(envelope: Envelope) -> None:
        received.append(envelope)

    config = BusConfig(
        url=f"nats://127.0.0.1:{port}",
        agent_id="reconnect-proof",
        env="dev",
        reconnect_wait_s=1,
    )
    bus = FleetBus(config)
    try:
        await bus.connect()
        await bus.subscribe("dev.agent.inbox.reconnect-proof", _handler)

        first = Envelope.new(msg_id="r1", ts="t1", sender="probe", kind="status", payload={})
        await bus.publish("dev.agent.inbox.reconnect-proof", first)
        await _wait_until(lambda: len(received) == 1, 5, "pre-restart delivery")

        # Kill and restart the server on the SAME port — the client must
        # reconnect on its own and its subscription table must re-establish.
        server.kill()
        server.wait(timeout=10)
        await _wait_until(lambda: not bus.is_connected, 10, "client to notice the drop")
        server = _start_server(port)
        await _wait_until(lambda: bus.is_connected, 15, "client reconnect")

        second = Envelope.new(msg_id="r2", ts="t2", sender="probe", kind="status", payload={})
        await bus.publish("dev.agent.inbox.reconnect-proof", second)
        await _wait_until(lambda: len(received) == 2, 5, "post-restart delivery")
        assert [env.id for env in received] == ["r1", "r2"]
    finally:
        await bus.close()
        server.kill()
        server.wait(timeout=10)
