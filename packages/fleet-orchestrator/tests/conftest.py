"""Shared test fixtures for fleet-orchestrator.

The trace sink is module-level state resolved lazily from the environment; a
developer or CI env with ``FLEET_TRACE_PATH`` set must never make unrelated
tests write a real trace file. Force the no-op sink around every test; tests
that want tracing install their own sink explicitly via ``trace.set_sink``.
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
from fleet_nats import BusConfig, FleetBus

from fleet_orchestrator import trace

# ---- Real embedded nats-server -js fixtures (fleet-bus #106 KV store tests) --
# Replicated faithfully from packages/fleet-nats/tests/conftest.py: the durable
# governor store's CAS guarantee is a SERVER-side property (the JetStream sequence
# IS the revision), so proving it against a mock that fakes ``revision`` would
# prove nothing. The orchestrator package cannot import fleet-nats' conftest, so
# the fixtures live here too. If ``nats-server`` is not on PATH these tests skip.

_NATS_SERVER = shutil.which("nats-server")

# Module-usable skip marker: KV/CAS tests are meaningless without a real server,
# and there is no honest mock for a server-assigned revision.
requires_nats_server = pytest.mark.skipif(
    _NATS_SERVER is None,
    reason="nats-server binary not on PATH — JetStream KV CAS cannot be faked; "
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
    store = tempfile.mkdtemp(prefix="fleet-orch-js-")
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
    config = BusConfig(url=jetstream_server, agent_id="pytest-orch-js", env="dev")
    async with FleetBus(config) as bus:
        yield bus


@pytest.fixture(autouse=True)
def _isolate_trace_sink() -> Iterator[None]:
    trace.set_sink(trace.NullTraceSink())
    yield
    trace.set_sink(None)


@pytest.fixture(autouse=True)
def _default_intake_label(monkeypatch: pytest.MonkeyPatch) -> None:
    """Provide a safe default FLEET_INTAKE_LABEL for HubConfig.from_env tests.

    The intake poller defaults ON (it is the only shipped ``created`` publisher),
    and fleet-bus #54 makes a card-scope marker REQUIRED whenever the poller is
    enabled — so a bare ``from_env`` with only manager/workers set would now refuse
    to start. Seed a harmless marker here so success-path config tests that do not
    care about intake scope still boot; a test asserting the fail-closed refusal
    overrides this by ``delenv``-ing the var itself.
    """
    monkeypatch.setenv("FLEET_INTAKE_LABEL", "[fleet]")
