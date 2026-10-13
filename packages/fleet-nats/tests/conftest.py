"""Shared fixtures for fleet-nats tests.

The JetStream fixtures here boot a REAL embedded ``nats-server`` with JetStream
enabled (``-js``) on an ephemeral port + temp store dir, and hand back a
connected :class:`~fleet_nats.bus.FleetBus`. This is deliberate: the CAS
guarantee #146/#150 depend on is a SERVER-side property (the JetStream sequence
is the revision), so proving it against a mock that fakes ``revision`` would
prove nothing. If ``nats-server`` is not on PATH the JetStream tests skip (they
cannot be faked), so the pure-Python suite still runs everywhere.
"""

from __future__ import annotations

import shutil
import socket
import subprocess
import tempfile
import time
from collections.abc import AsyncIterator, Iterator

import pytest
import pytest_asyncio

from fleet_nats.bus import FleetBus
from fleet_nats.config import BusConfig

_NATS_SERVER = shutil.which("nats-server")

# Module-usable skip marker: JetStream tests are meaningless without a real
# server, and there is no honest mock for a server-assigned revision.
requires_nats_server = pytest.mark.skipif(
    _NATS_SERVER is None,
    reason="nats-server binary not on PATH — JetStream CAS/replay cannot be faked; "
    "install nats-server to run these (brew install nats-server).",
)


def _free_port() -> int:
    """Return an unused localhost TCP port for the embedded server."""
    sock = socket.socket()
    try:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])
    finally:
        sock.close()


def _wait_for_port(port: int, *, timeout_s: float = 10.0) -> None:
    """Block until ``port`` accepts a connection, or raise on timeout."""
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        try:
            socket.create_connection(("127.0.0.1", port), timeout=0.25).close()
            return
        except OSError:
            time.sleep(0.1)
    raise RuntimeError(f"embedded nats-server did not open port {port} within {timeout_s}s")


@pytest.fixture
def jetstream_server() -> Iterator[str]:
    """Boot a real embedded ``nats-server -js`` on an ephemeral port; yield its URL.

    Tears the process down and removes the temp store dir on exit. Skips the
    whole test if ``nats-server`` is not installed.
    """
    if _NATS_SERVER is None:
        pytest.skip("nats-server not installed")
    port = _free_port()
    store = tempfile.mkdtemp(prefix="fleet-nats-js-")
    proc = subprocess.Popen(  # noqa: S603 - fixed argv, no shell, test-only
        [_NATS_SERVER, "-p", str(port), "-js", "-sd", store, "-a", "127.0.0.1"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    try:
        _wait_for_port(port)
        yield f"nats://127.0.0.1:{port}"
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait()
        shutil.rmtree(store, ignore_errors=True)


@pytest_asyncio.fixture
async def js_bus(jetstream_server: str) -> AsyncIterator[FleetBus]:
    """A connected :class:`FleetBus` pointed at the embedded JetStream server."""
    config = BusConfig(url=jetstream_server, agent_id="pytest-js", env="dev")
    async with FleetBus(config) as bus:
        yield bus
