"""URL-userinfo redaction tests (#48).

A NATS URL may carry credentials (``nats://user:pass@host:4222``). Every
connect-path log line and the connect-timeout exception message echo the URL,
so the password must never reach the logs. Two layers:

- ``redact_url`` unit tests (always run): the helper strips the password from
  every userinfo shape and leaves credential-free URLs untouched.
- Connect-path tests: the *failure* path (timeout against a dead port, always
  runs) and the *success* path (against a LOCAL throwaway ``nats-server``,
  auto-skips when the binary is absent) both capture loguru output and assert
  the password never appears — in a log line or the raised error message.
"""

from __future__ import annotations

import asyncio
import shutil
import socket
import subprocess
import time

import pytest
from loguru import logger

from fleet_nats import BusConfig, FleetBus
from fleet_nats.bus import redact_url

_NATS_SERVER = shutil.which("nats-server")
_SECRET = "s3cr3t-pw"  # noqa: S105 — test fixture password, not a real credential


# --------------------------------------------------------------------------- #
# redact_url unit tests
# --------------------------------------------------------------------------- #


def test_redact_url_strips_password() -> None:
    out = redact_url("nats://user:s3cr3t-pw@host.example:4222")
    assert "s3cr3t-pw" not in out
    assert out == "nats://user:***@host.example:4222"


def test_redact_url_keeps_bare_username() -> None:
    # A username with no password has nothing secret to hide.
    assert redact_url("nats://user@host:4222") == "nats://user@host:4222"


def test_redact_url_passthrough_no_userinfo() -> None:
    assert redact_url("nats://host:4222") == "nats://host:4222"
    assert redact_url("nats://127.0.0.1:4222") == "nats://127.0.0.1:4222"


def test_redact_url_empty_username_with_password() -> None:
    out = redact_url("nats://:s3cr3t-pw@host:4222")
    assert "s3cr3t-pw" not in out
    assert out == "nats://:***@host:4222"


def test_redact_url_password_with_special_chars() -> None:
    # A password containing '@' / ':' must still be fully removed.
    out = redact_url("nats://user:p@ss:word@host:4222")
    assert "p@ss:word" not in out
    assert "***@host:4222" in out


def test_redact_url_password_with_raw_slash() -> None:
    # Regression: a raw (un-percent-encoded) '/' in the password makes
    # urlsplit().password read None, which would fail OPEN and leak. The
    # string-authority helper must still redact it.
    out = redact_url("nats://user:p/a:ss@host:4222")
    assert "p/a:ss" not in out
    assert out == "nats://user:***@host:4222"


def test_redact_url_preserves_ipv6_brackets() -> None:
    out = redact_url("nats://user:s3cr3t-pw@[::1]:4222")
    assert "s3cr3t-pw" not in out
    assert out == "nats://user:***@[::1]:4222"


def test_redact_url_no_scheme_passthrough() -> None:
    # Not a scheme'd URL → nothing to slice; returned unchanged.
    assert redact_url("host:4222") == "host:4222"


def test_redact_url_preserves_path_and_query() -> None:
    out = redact_url("nats://user:s3cr3t-pw@host:4222/path?opt=1")
    assert "s3cr3t-pw" not in out
    assert out == "nats://user:***@host:4222/path?opt=1"


# --------------------------------------------------------------------------- #
# connect-path: failure (timeout) — always runs, no server needed
# --------------------------------------------------------------------------- #


def _dead_port() -> int:
    """Bind then release a port so it is (very likely) refusing/timing out."""
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(("127.0.0.1", 0))
    port = int(s.getsockname()[1])
    s.close()
    return port


def test_connect_failure_never_logs_password(caplog: pytest.LogCaptureFixture) -> None:
    port = _dead_port()
    url = f"nats://user:{_SECRET}@127.0.0.1:{port}"
    cfg = BusConfig(url=url, agent_id="bilby", connect_timeout_s=1)
    bus = FleetBus(cfg)

    sink_lines: list[str] = []
    handle = logger.add(sink_lines.append, level="DEBUG", format="{message}")
    try:
        # A dead port yields the connect-timeout branch (bounded by our budget).
        with pytest.raises(TimeoutError) as excinfo:
            asyncio.run(bus.connect())
    finally:
        logger.remove(handle)

    captured = "\n".join(sink_lines)
    # Password must not appear in ANY captured connect-path log line...
    assert _SECRET not in captured
    # ...nor in the raised error message (the timeout path embeds the URL).
    assert _SECRET not in str(excinfo.value)
    # And the redacted marker should be present, proving the URL was logged safely.
    assert "***@127.0.0.1" in captured


def test_connect_generic_failure_never_logs_password(
    caplog: pytest.LogCaptureFixture,
) -> None:
    # An unresolvable host raises a connect error BEFORE the timeout budget,
    # exercising the generic ``except Exception`` branch (not the timeout one).
    url = f"nats://user:{_SECRET}@nonexistent.invalid:4222"
    cfg = BusConfig(url=url, agent_id="bilby", connect_timeout_s=1)
    bus = FleetBus(cfg)

    sink_lines: list[str] = []
    handle = logger.add(sink_lines.append, level="DEBUG", format="{message}")
    try:
        with pytest.raises(Exception) as excinfo:  # noqa: PT011,B017 — nats-py raises varied types
            asyncio.run(bus.connect())
    finally:
        logger.remove(handle)

    captured = "\n".join(sink_lines)
    assert _SECRET not in captured
    assert _SECRET not in str(excinfo.value)
    # The generic-failure log line ("failed url=...") must carry the redacted URL.
    assert "***@nonexistent.invalid" in captured


# --------------------------------------------------------------------------- #
# connect-path: success — LOCAL nats-server, auto-skips when absent
# --------------------------------------------------------------------------- #


def _free_port() -> int:
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(("127.0.0.1", 0))
    port = int(s.getsockname()[1])
    s.close()
    return port


def _wait_listening(port: int, timeout_s: float = 5.0) -> bool:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.settimeout(0.2)
            if sock.connect_ex(("127.0.0.1", port)) == 0:
                return True
        time.sleep(0.05)
    return False


@pytest.mark.skipif(_NATS_SERVER is None, reason="nats-server binary not installed")
def test_connect_success_never_logs_password() -> None:
    port = _free_port()
    proc = subprocess.Popen(  # noqa: S603
        [str(_NATS_SERVER), "-p", str(port), "-a", "127.0.0.1"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    try:
        assert _wait_listening(port), "throwaway nats-server did not come up"
        # Unauthenticated throwaway server ignores the userinfo; connect still
        # succeeds and the debug/info connect lines fire with the URL.
        url = f"nats://user:{_SECRET}@127.0.0.1:{port}"
        cfg = BusConfig(url=url, agent_id="bilby", connect_timeout_s=2)
        bus = FleetBus(cfg)

        sink_lines: list[str] = []
        handle = logger.add(sink_lines.append, level="DEBUG", format="{message}")

        async def _run() -> None:
            await bus.connect()
            assert bus.is_connected
            await bus.close()

        try:
            asyncio.run(_run())
        finally:
            logger.remove(handle)

        captured = "\n".join(sink_lines)
        assert _SECRET not in captured
        assert "***@127.0.0.1" in captured
    finally:
        proc.terminate()
        proc.wait(timeout=5)
