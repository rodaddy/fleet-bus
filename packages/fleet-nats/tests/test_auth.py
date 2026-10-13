"""Client connect-path credential tests (F2 authn, fleet-bus #104).

The client HALF of F2: a bus participant presents its own per-agent NATS
credentials at connect (user/password or a ``.creds`` file), the source is
logged but never the value, and the absent path stays permissive but announces
itself loudly. The server-enforced authz half (per-subject allow-lists on
fleet-hub) is out of scope here — see ``docs/f2-nats-auth-posture.md``.

Three layers, all off-network (no live server needed):
- Config: credential_source resolution, both-or-neither user/password guard,
  empty-env normalisation, and auth_credentials() shape.
- Connect wiring: the built kwargs are splatted into ``nc.connect`` (asserted
  via a monkeypatched connect that captures its kwargs).
- No-secret-in-logs: a full connect with a real secret value never emits that
  value on any log line.
"""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any

import pytest
from loguru import logger
from nats.errors import Error as NatsError
from nats.errors import NoServersError

from fleet_nats import BusConfig, CredentialSource, FleetBus
from fleet_nats.config import AuthCredentials

# A deliberately LOW-entropy, obviously-fake sentinel — real enough to assert
# "this value never appears in a log line", fake enough that a secret scanner
# (ggshield, pre-push) does not flag it. Never a real credential.
_SECRET_PASSWORD = "FAKE-PASSWORD-DO-NOT-LOG"  # noqa: S105 — test sentinel, not a cred


@pytest.fixture
def creds_path(tmp_path: Path) -> str:
    """A real, readable .creds file path.

    BusConfig pre-validates the creds file at construction (isfile + R_OK) so
    the path must actually exist and be readable; a fake string path now (by
    design) fails loud. Contents are irrelevant off-network — nats-py never
    opens it in these stubbed connects. This must NOT be a real credential.
    """
    p = tmp_path / "agent-nagatha.creds"
    p.write_text("-- NOT A REAL CREDENTIAL --\n")
    return str(p)


# ---- config: credential_source resolution --------------------------------


def test_credential_source_none_by_default() -> None:
    cfg = BusConfig(agent_id="nagatha")
    assert cfg.credential_source is CredentialSource.NONE
    assert cfg.auth_credentials() == AuthCredentials()


def test_credential_source_user_password() -> None:
    cfg = BusConfig(agent_id="nagatha", nats_user="nagatha", nats_password=_SECRET_PASSWORD)
    assert cfg.credential_source is CredentialSource.USER_PASSWORD
    assert cfg.auth_credentials() == AuthCredentials(user="nagatha", password=_SECRET_PASSWORD)


def test_credential_source_creds_file(creds_path: str) -> None:
    cfg = BusConfig(agent_id="nagatha", nats_creds_file=creds_path)
    assert cfg.credential_source is CredentialSource.CREDS_FILE
    assert cfg.auth_credentials() == AuthCredentials(user_credentials=creds_path)


def test_creds_file_wins_over_user_password(creds_path: str) -> None:
    # A deployment with a .creds file must not silently downgrade to a shared
    # user/password if both are somehow present.
    cfg = BusConfig(
        agent_id="nagatha",
        nats_user="nagatha",
        nats_password=_SECRET_PASSWORD,
        nats_creds_file=creds_path,
    )
    assert cfg.credential_source is CredentialSource.CREDS_FILE
    assert cfg.auth_credentials() == AuthCredentials(user_credentials=creds_path)


def test_user_without_password_fails_loud() -> None:
    with pytest.raises(ValueError, match="both nats_user and nats_password"):
        BusConfig(agent_id="nagatha", nats_user="nagatha")


def test_password_without_user_fails_loud() -> None:
    with pytest.raises(ValueError, match="both nats_user and nats_password"):
        BusConfig(agent_id="nagatha", nats_password=_SECRET_PASSWORD)


# ---- config: from_env wiring + empty-value normalisation -----------------


def test_from_env_reads_user_password(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("FLEET_NATS_USER", "bilby")
    monkeypatch.setenv("FLEET_NATS_PASSWORD", _SECRET_PASSWORD)
    cfg = BusConfig.from_env("bilby")
    assert cfg.credential_source is CredentialSource.USER_PASSWORD
    assert cfg.nats_user == "bilby"
    assert cfg.nats_password == _SECRET_PASSWORD


def test_from_env_reads_creds_file(monkeypatch: pytest.MonkeyPatch, creds_path: str) -> None:
    monkeypatch.setenv("FLEET_NATS_CREDS", creds_path)
    cfg = BusConfig.from_env("bilby")
    assert cfg.credential_source is CredentialSource.CREDS_FILE
    assert cfg.nats_creds_file == creds_path


def test_from_env_absent_is_none(monkeypatch: pytest.MonkeyPatch) -> None:
    for var in ("FLEET_NATS_USER", "FLEET_NATS_PASSWORD", "FLEET_NATS_CREDS"):
        monkeypatch.delenv(var, raising=False)
    cfg = BusConfig.from_env("bilby")
    assert cfg.credential_source is CredentialSource.NONE


def test_from_env_blank_export_normalises_to_none(monkeypatch: pytest.MonkeyPatch) -> None:
    # A blank/whitespace export must mean "unset", not "empty username" — else it
    # would trip the both-or-neither guard for a common deploy artifact.
    monkeypatch.setenv("FLEET_NATS_USER", "")
    monkeypatch.setenv("FLEET_NATS_PASSWORD", "   ")
    monkeypatch.setenv("FLEET_NATS_CREDS", "")
    cfg = BusConfig.from_env("bilby")
    assert cfg.credential_source is CredentialSource.NONE


# ---- connect wiring: kwargs splatted into nc.connect ---------------------


class _CapturingNATS:
    """Stand-in for the nats-py client that records connect kwargs.

    Only the surface FleetBus.connect touches is implemented; connect returns
    immediately (no real socket), so these tests run fully off-network.
    """

    def __init__(self) -> None:
        self.connect_kwargs: dict[str, Any] = {}
        self.is_connected = False
        # Mirrors nats-py's Client.last_error property — the last error it
        # recorded on a failed connect attempt. None on the happy path.
        self.last_error: BaseException | None = None

    async def connect(self, **kwargs: Any) -> None:
        self.connect_kwargs = kwargs
        self.is_connected = True


@pytest.fixture
def _patched_bus(monkeypatch: pytest.MonkeyPatch) -> type[_CapturingNATS]:
    """Patch the NATS client FleetBus instantiates with the capturing stub."""
    monkeypatch.setattr("fleet_nats.bus.NATS", _CapturingNATS)
    return _CapturingNATS


async def test_connect_passes_user_password_kwargs(
    _patched_bus: type[_CapturingNATS],
) -> None:
    cfg = BusConfig(agent_id="nagatha", nats_user="nagatha", nats_password=_SECRET_PASSWORD)
    bus = FleetBus(cfg)
    await bus.connect()
    nc: _CapturingNATS = bus._nc  # type: ignore[assignment]
    assert nc.connect_kwargs["user"] == "nagatha"
    assert nc.connect_kwargs["password"] == _SECRET_PASSWORD
    # creds-file arg is passed as None (nats-py treats None as "not provided").
    assert nc.connect_kwargs["user_credentials"] is None


async def test_connect_passes_creds_file_kwarg(
    _patched_bus: type[_CapturingNATS],
    creds_path: str,
) -> None:
    cfg = BusConfig(agent_id="nagatha", nats_creds_file=creds_path)
    bus = FleetBus(cfg)
    await bus.connect()
    nc: _CapturingNATS = bus._nc  # type: ignore[assignment]
    assert nc.connect_kwargs["user_credentials"] == creds_path
    assert nc.connect_kwargs["user"] is None
    assert nc.connect_kwargs["password"] is None


async def test_connect_no_creds_passes_no_auth_kwargs(
    _patched_bus: type[_CapturingNATS],
) -> None:
    # Absence path: current behaviour preserved — every auth arg is None, which
    # nats-py treats as "not provided" (i.e. an anonymous connect).
    cfg = BusConfig(agent_id="nagatha")
    bus = FleetBus(cfg)
    await bus.connect()
    nc: _CapturingNATS = bus._nc  # type: ignore[assignment]
    assert nc.connect_kwargs["user"] is None
    assert nc.connect_kwargs["password"] is None
    assert nc.connect_kwargs["user_credentials"] is None


# ---- no-secret-in-logs ---------------------------------------------------


async def test_connect_never_logs_credential_values(
    _patched_bus: type[_CapturingNATS],
    creds_path: str,
) -> None:
    """A full connect with a real secret value must never emit it on any log
    line — the source is logged, the value is not."""
    captured: list[str] = []
    sink_id = logger.add(lambda message: captured.append(str(message)), level="DEBUG")
    try:
        cfg = BusConfig(
            agent_id="nagatha",
            nats_user="nagatha",
            nats_password=_SECRET_PASSWORD,
            nats_creds_file=creds_path,
        )
        bus = FleetBus(cfg)
        await bus.connect()
    finally:
        logger.remove(sink_id)

    joined = "\n".join(captured)
    assert _SECRET_PASSWORD not in joined
    assert creds_path not in joined  # even the creds PATH is not logged
    # ...but the SOURCE must be visible so the authed state is auditable.
    assert "creds-file" in joined


async def test_connect_without_creds_warns_loudly(
    _patched_bus: type[_CapturingNATS],
) -> None:
    """The unauthenticated path must announce itself at WARNING — an anonymous
    connection is never silent."""
    captured: list[tuple[str, str]] = []
    sink_id = logger.add(
        lambda message: captured.append((message.record["level"].name, message.record["message"])),
        level="DEBUG",
    )
    try:
        bus = FleetBus(BusConfig(agent_id="nagatha"))
        await bus.connect()
    finally:
        logger.remove(sink_id)

    warnings = [msg for level, msg in captured if level == "WARNING"]
    assert any("WITHOUT credentials" in msg for msg in warnings)


# ---- P1: BusConfig repr/str never leaks the secret or the creds path ------


def test_repr_hides_password_and_creds_path(creds_path: str) -> None:
    """repr(cfg)/str(cfg) must NOT contain the password value or the creds path
    (repr=False on both fields), while credential_source stays visible so the
    mechanism is still auditable. This is the P1 blocker: the object's own repr
    was the leak, not the connect-path logging."""
    cfg = BusConfig(
        agent_id="nagatha",
        nats_user="nagatha",
        nats_password=_SECRET_PASSWORD,
        nats_creds_file=creds_path,
    )
    r = repr(cfg)
    s = str(cfg)
    f = f"cfg={cfg}"
    for rendered in (r, s, f):
        assert _SECRET_PASSWORD not in rendered
        assert creds_path not in rendered
    # The mechanism (source) is not secret and MUST stay observable.
    assert cfg.credential_source.value == "creds-file"


def test_repr_still_shows_nonsecret_fields() -> None:
    """The redaction is surgical: only the secret value + creds path drop out;
    ordinary fields (agent_id, url) remain in the repr for debuggability."""
    cfg = BusConfig(agent_id="nagatha", nats_user="nagatha", nats_password=_SECRET_PASSWORD)
    r = repr(cfg)
    assert "nagatha" in r  # agent_id / user still visible
    assert _SECRET_PASSWORD not in r
    assert "nats://" in r  # url still visible


# ---- P2: creds-file pre-validation names the ENV VAR, not the path --------


def test_missing_creds_file_fails_loud_naming_env_var(tmp_path: Path) -> None:
    """A missing creds path fails at construction with a ValueError that names
    FLEET_NATS_CREDS and does NOT contain the resolved path (path-disclosure is
    the leak vector — the path encodes deploy layout / agent identity)."""
    missing = str(tmp_path / "does-not-exist.creds")
    with pytest.raises(ValueError, match="FLEET_NATS_CREDS") as excinfo:
        BusConfig(agent_id="nagatha", nats_creds_file=missing)
    assert missing not in str(excinfo.value)


def test_unreadable_creds_file_fails_loud(tmp_path: Path) -> None:
    """An existing-but-unreadable creds file also fails loud without the path."""
    p = tmp_path / "locked.creds"
    p.write_text("x")
    p.chmod(0o000)
    try:
        with pytest.raises(ValueError, match="FLEET_NATS_CREDS") as excinfo:
            BusConfig(agent_id="nagatha", nats_creds_file=str(p))
        assert str(p) not in str(excinfo.value)
    finally:
        p.chmod(0o600)  # restore so tmp cleanup can remove it


# ---- P2-B: FLEET_NATS_REQUIRE_AUTH opt-in fail-closed ---------------------


async def test_require_auth_no_creds_raises(
    _patched_bus: type[_CapturingNATS],
) -> None:
    """require_auth + no creds → hard error at connect (opt-in fail-closed)."""
    bus = FleetBus(BusConfig(agent_id="nagatha", require_auth=True))
    with pytest.raises(ValueError, match="FLEET_NATS_REQUIRE_AUTH"):
        await bus.connect()


async def test_no_require_auth_no_creds_warns_and_connects(
    _patched_bus: type[_CapturingNATS],
) -> None:
    """Default (require_auth unset) + no creds → warns + connects (today's
    permissive rollout behaviour, unchanged)."""
    bus = FleetBus(BusConfig(agent_id="nagatha"))
    await bus.connect()  # must not raise
    assert bus._nc.is_connected


def test_require_auth_from_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("FLEET_NATS_REQUIRE_AUTH", "1")
    assert BusConfig.from_env("bilby").require_auth is True
    monkeypatch.setenv("FLEET_NATS_REQUIRE_AUTH", "false")
    assert BusConfig.from_env("bilby").require_auth is False


# ---- P3-C: both .creds and user/password set → visible override WARNING ----


async def test_both_creds_and_userpass_warns_override(
    _patched_bus: type[_CapturingNATS],
    creds_path: str,
) -> None:
    captured: list[tuple[str, str]] = []
    sink_id = logger.add(
        lambda message: captured.append((message.record["level"].name, message.record["message"])),
        level="DEBUG",
    )
    try:
        cfg = BusConfig(
            agent_id="nagatha",
            nats_user="nagatha",
            nats_password=_SECRET_PASSWORD,
            nats_creds_file=creds_path,
        )
        await FleetBus(cfg).connect()
    finally:
        logger.remove(sink_id)
    warnings = [msg for level, msg in captured if level == "WARNING"]
    assert any("ignoring user/password" in msg for msg in warnings)


# ---- P2-A: post-flip auth rejection is self-diagnosing --------------------
#
# These stubs reproduce REAL nats-py initial-connect behaviour (verified against
# the installed client.py), NOT a convenient fiction:
#   - nats-py raises a GENERIC ``errors.Error('nats: Authorization Violation')``
#     on an initial-connect auth reject — never an ``AuthorizationError`` (that
#     class is only built in its read-loop). So we classify by MESSAGE marker.
#   - Under the fleet default ``max_reconnect_attempts=-1`` nats-py never discards
#     the auth-rejecting server, swallows the error into an infinite retry loop,
#     and records it on ``last_error``. FleetBus's own ``wait_for`` budget then
#     fires a ``TimeoutError`` first — so the reject arrives as a TIMEOUT with
#     ``last_error`` set, and we must recover the real cause from there.
#   - Under a FINITE ``max_reconnect_attempts`` nats-py does NOT re-raise the
#     auth error: it records the violation on ``last_error`` and, once its bounded
#     attempts are spent, raises a generic ``NoServersError``. The actionable
#     cause therefore lives on ``last_error`` in this path too — FleetBus must
#     recover it from there, not from the raised ``NoServersError``.

_AUTH_VIOLATION = NatsError("nats: Authorization Violation")


class _AuthRejectInfiniteRetryNATS(_CapturingNATS):
    """Real -1 behaviour: hang (as nats-py's infinite retry loop would) with
    last_error set, so FleetBus's wait_for budget times out."""

    async def connect(self, **kwargs: Any) -> None:
        self.last_error = _AUTH_VIOLATION
        await asyncio.sleep(3600)  # never returns — mirrors the infinite loop


_NO_SERVERS = NoServersError()


class _AuthRejectFiniteNATS(_CapturingNATS):
    """Real finite-reconnect behaviour: nats-py records the auth violation on
    ``last_error`` and, once its bounded attempts are exhausted, raises a generic
    ``NoServersError`` — NOT the auth error itself. The actionable cause is only
    recoverable from ``last_error``."""

    async def connect(self, **kwargs: Any) -> None:
        self.last_error = _AUTH_VIOLATION
        raise _NO_SERVERS


async def test_anonymous_reject_infinite_retry_surfaces_as_actionable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Default -1 reconnect: the auth reject presents as a wait_for TIMEOUT with
    last_error set; FleetBus recovers the real cause and raises an actionable,
    secret-free ConnectionError instead of a misleading 'server unreachable'."""
    monkeypatch.setattr("fleet_nats.bus.NATS", _AuthRejectInfiniteRetryNATS)
    # connect_timeout_s=0 → budget=2s so the test does not wait the full window.
    bus = FleetBus(BusConfig(agent_id="nagatha", connect_timeout_s=0))
    with pytest.raises(ConnectionError, match="none configured") as excinfo:
        await bus.connect()
    msg = str(excinfo.value)
    assert "FLEET_NATS_CREDS" in msg
    assert "FLEET_NATS_USER" in msg
    # It must NOT masquerade as an unreachable-server timeout.
    assert "unreachable" not in msg
    assert isinstance(excinfo.value.__cause__, TimeoutError)


async def test_anonymous_reject_finite_reconnect_surfaces_as_actionable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Finite reconnect: nats-py raises a generic NoServersError with the auth
    violation on last_error; FleetBus recovers the real cause from last_error and
    raises the actionable ConnectionError (never a bare 'no servers')."""
    monkeypatch.setattr("fleet_nats.bus.NATS", _AuthRejectFiniteNATS)
    bus = FleetBus(BusConfig(agent_id="nagatha", max_reconnect_attempts=3))
    with pytest.raises(ConnectionError, match="none configured") as excinfo:
        await bus.connect()
    assert "FLEET_NATS_CREDS" in str(excinfo.value)
    # The raised NoServersError is the __cause__; the auth marker came from
    # last_error, which is what made the message actionable.
    assert excinfo.value.__cause__ is _NO_SERVERS


async def test_authed_reject_finite_reconnect_actionable_source(
    monkeypatch: pytest.MonkeyPatch,
    creds_path: str,
) -> None:
    """When creds WERE presented and rejected, the message reports the source
    (creds-file) — never the value or path — not a 'none configured' message."""
    monkeypatch.setattr("fleet_nats.bus.NATS", _AuthRejectFiniteNATS)
    bus = FleetBus(BusConfig(agent_id="nagatha", nats_creds_file=creds_path))
    with pytest.raises(ConnectionError, match="refused this agent's credentials") as excinfo:
        await bus.connect()
    msg = str(excinfo.value)
    assert "creds-file" in msg
    assert creds_path not in msg  # path never leaks
    assert "source=creds-file" in msg


async def test_non_auth_timeout_still_reports_unreachable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A genuine unreachable server (timeout with NO auth marker on last_error)
    must still report 'unreachable' — the auth path must not swallow it."""

    class _HangNoError(_CapturingNATS):
        async def connect(self, **kwargs: Any) -> None:
            await asyncio.sleep(3600)  # last_error stays None

    monkeypatch.setattr("fleet_nats.bus.NATS", _HangNoError)
    bus = FleetBus(BusConfig(agent_id="nagatha", connect_timeout_s=0))
    with pytest.raises(TimeoutError, match="unreachable"):
        await bus.connect()


# ---- P2-C: TOCTOU creds-file open failure at connect is path-sanitized -----


async def test_connect_creds_file_open_error_sanitizes_path(
    monkeypatch: pytest.MonkeyPatch,
    creds_path: str,
) -> None:
    """BusConfig pre-validates the creds file, but nats-py opens it lazily during
    connect. If it is deleted / made unreadable in the TOCTOU window, that open
    raises an OSError whose ``str`` carries the creds-file PATH. The raised error
    and every log line must name FLEET_NATS_CREDS and the errno class — never the
    path string."""

    class _LazyOpenFailsNATS(_CapturingNATS):
        async def connect(self, **kwargs: Any) -> None:
            # nats-py surfaces the path in the OSError message; e.g.
            # "[Errno 13] Permission denied: '/vault/agent.creds'".
            raise PermissionError(13, "Permission denied", creds_path)

    monkeypatch.setattr("fleet_nats.bus.NATS", _LazyOpenFailsNATS)

    captured: list[str] = []
    sink_id = logger.add(lambda message: captured.append(str(message)), level="DEBUG")
    try:
        bus = FleetBus(BusConfig(agent_id="nagatha", nats_creds_file=creds_path))
        with pytest.raises(ConnectionError, match="FLEET_NATS_CREDS") as excinfo:
            await bus.connect()
    finally:
        logger.remove(sink_id)

    msg = str(excinfo.value)
    # Actionable: names the env var, the errno class, and the agent — never the path.
    assert "FLEET_NATS_CREDS" in msg
    assert "nagatha" in msg
    assert "Permission denied" in msg
    assert creds_path not in msg

    joined = "\n".join(captured)
    assert "FLEET_NATS_CREDS" in joined
    assert creds_path not in joined  # the ERROR log line is path-free too
