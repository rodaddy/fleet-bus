"""Tests for the heartbeat emitter (#26): cadence, sanitization, resilience."""

from __future__ import annotations

import itertools
from importlib.metadata import PackageNotFoundError
from pathlib import Path

import pytest
from fleet_core.spec import Heartbeat
from fleet_nats import Envelope, parse_heartbeat
from loguru import logger

from fleet_subscriber.heartbeat import (
    HeartbeatEmitter,
    _release_sha,
    installed_package_version,
    interval_from_env,
)


class _RecordingBus:
    def __init__(self) -> None:
        self.published: list[tuple[str, Envelope]] = []

    async def publish(self, subject: str, envelope: Envelope) -> None:
        self.published.append((subject, envelope))


class _ExplodingBus:
    async def publish(self, subject: str, envelope: Envelope) -> None:
        raise ConnectionError("bus down")


class _PathStub:
    """Stands in for ``pathlib.Path`` so the receipt walk starts under tmp_path.

    ``_release_sha`` resolves the receipt relative to ``__file__``, which under
    test is the real installed module. Patching the ``Path`` name lets a test
    point that walk at a temp tree instead of the developer's filesystem — where
    a stray ``.fleet-release-sha`` anywhere above the repo would otherwise make
    the result depend on the machine running the suite.
    """

    def __init__(self, target: Path) -> None:
        self._target = target

    def __call__(self, _file: str) -> Path:
        return self._target

    def resolve(self) -> Path:
        return self._target


class _StepSleeper:
    """Controllable sleeper: records requested delays, stops the loop after N."""

    def __init__(self, emitter_holder: list[HeartbeatEmitter], stop_after: int) -> None:
        self.delays: list[float] = []
        self._holder = emitter_holder
        self._stop_after = stop_after

    async def __call__(self, seconds: float) -> None:
        self.delays.append(seconds)
        if len(self.delays) >= self._stop_after:
            self._holder[0].stop()


def _emitter(
    bus: _RecordingBus | _ExplodingBus,
    *,
    interval_s: float = 30.0,
    sleeper: _StepSleeper | None = None,
    catalog_version: int | None = None,
    package_version: str | None = None,
) -> HeartbeatEmitter:
    ids = (f"h{i}" for i in itertools.count())
    times = (f"2026-07-08T00:00:{i:02d}Z" for i in itertools.count())
    kwargs = {}
    if sleeper is not None:
        kwargs["sleep"] = sleeper
    return HeartbeatEmitter(
        bus,
        agent_id="bilby",
        env="dev",
        role="worker",
        lanes=("dispatch", "relay", "wake", "inbox"),
        new_id=lambda: next(ids),
        now=lambda: next(times),
        catalog_version=catalog_version,
        package_version=package_version,
        interval_s=interval_s,
        **kwargs,
    )


@pytest.mark.asyncio
async def test_beat_publishes_parseable_heartbeat_on_the_agent_lane() -> None:
    bus = _RecordingBus()
    emitter = _emitter(bus)
    await emitter.beat()
    (subject, env), *_ = bus.published
    assert subject == "dev.agent.heartbeat.bilby"
    hb = parse_heartbeat(env)
    assert isinstance(env, Envelope)
    assert env.typed_signal is not None
    assert isinstance(env.typed_signal, Heartbeat)
    assert env.typed_signal.jetstream is False
    assert hb.agent_id == "bilby"
    assert hb.role == "worker"
    assert hb.lanes == ("dispatch", "relay", "wake", "inbox")
    assert hb.seq == 1
    assert hb.last_turn_ts is None


@pytest.mark.asyncio
async def test_seq_is_monotonic_and_note_turn_stamps() -> None:
    bus = _RecordingBus()
    emitter = _emitter(bus)
    await emitter.beat()
    emitter.note_turn()
    await emitter.beat()
    first = parse_heartbeat(bus.published[0][1])
    second = parse_heartbeat(bus.published[1][1])
    assert (first.seq, second.seq) == (1, 2)
    assert first.last_turn_ts is None
    assert second.last_turn_ts is not None


@pytest.mark.asyncio
async def test_payload_carries_only_sanctioned_keys() -> None:
    """The beat is identity/liveness only — the closed key set, nothing else."""
    bus = _RecordingBus()
    await _emitter(bus).beat()
    _, env = bus.published[0]
    assert set(env.payload) == {
        "agent_id",
        "role",
        "lanes",
        "seq",
        "last_turn_ts",
        "catalog_version",
        "package_version",
    }


@pytest.mark.asyncio
async def test_beat_carries_the_connected_catalog_version() -> None:
    """Version skew is observable on the bus (#103): the emitter stamps the
    catalog version it connected with onto every beat."""
    bus = _RecordingBus()
    await _emitter(bus, catalog_version=7).beat()
    hb = parse_heartbeat(bus.published[0][1])
    assert hb.catalog_version == 7


@pytest.mark.asyncio
async def test_beat_catalog_version_defaults_to_none() -> None:
    bus = _RecordingBus()
    await _emitter(bus).beat()
    assert parse_heartbeat(bus.published[0][1]).catalog_version is None


@pytest.mark.asyncio
async def test_beat_carries_the_installed_package_version() -> None:
    """Install drift is observable on the bus (#203): the emitter threads the
    installed fleet-package version it was given onto every beat."""
    bus = _RecordingBus()
    await _emitter(bus, package_version="0.3.1").beat()
    hb = parse_heartbeat(bus.published[0][1])
    assert hb.package_version == "0.3.1"


@pytest.mark.asyncio
async def test_beat_package_version_defaults_to_none() -> None:
    bus = _RecordingBus()
    await _emitter(bus).beat()
    assert parse_heartbeat(bus.published[0][1]).package_version is None


@pytest.mark.asyncio
async def test_run_loop_beats_each_interval_and_stops() -> None:
    bus = _RecordingBus()
    holder: list[HeartbeatEmitter] = []
    sleeper = _StepSleeper(holder, stop_after=3)
    emitter = _emitter(bus, interval_s=30.0, sleeper=sleeper)
    holder.append(emitter)
    await emitter.run()
    assert len(bus.published) == 3
    assert sleeper.delays == [30.0, 30.0, 30.0]


@pytest.mark.asyncio
async def test_beat_failure_is_swallowed_and_loop_continues() -> None:
    """A publish failure degrades loudly (logged) — the loop keeps beating."""
    holder: list[HeartbeatEmitter] = []
    sleeper = _StepSleeper(holder, stop_after=2)
    emitter = _emitter(_ExplodingBus(), sleeper=sleeper)
    holder.append(emitter)
    await emitter.run()  # would raise if beat() propagated
    assert emitter.seq == 2


def test_interval_from_env_default_and_failloud(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("FLEET_HEARTBEAT_SECONDS", raising=False)
    assert interval_from_env() == 30.0
    monkeypatch.setenv("FLEET_HEARTBEAT_SECONDS", "12.5")
    assert interval_from_env() == 12.5
    monkeypatch.setenv("FLEET_HEARTBEAT_SECONDS", "soon")
    with pytest.raises(ValueError, match="must be a number"):
        interval_from_env()
    monkeypatch.setenv("FLEET_HEARTBEAT_SECONDS", "0")
    with pytest.raises(ValueError, match="must be positive"):
        interval_from_env()


def test_installed_package_version_differs_between_shas(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """THE #225 acceptance test: two hosts on different shas must report
    different values.

    This is the test #218 lacked. Its tests asserted the field round-trips and
    matched distribution metadata — both true of a hardcoded constant, which is
    exactly what shipped. A drift signal that cannot vary cannot show drift, so
    assert variation directly.
    """
    monkeypatch.setenv("FLEET_RELEASE_SHA", "aaaaaaa")
    first = installed_package_version()
    monkeypatch.setenv("FLEET_RELEASE_SHA", "bbbbbbb")
    second = installed_package_version()

    assert first is not None and second is not None
    assert first != second, "package_version must vary with the deployed sha"
    assert first.endswith("+aaaaaaa")
    assert second.endswith("+bbbbbbb")


def test_installed_package_version_is_none_when_sha_unknown(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """An undeployed checkout reports None, not a bare dist version.

    Absent reads as "I do not know". A bare ``0.1.0`` would read as a real
    answer and make a stale host indistinguishable from a current one.
    """
    monkeypatch.delenv("FLEET_RELEASE_SHA", raising=False)
    monkeypatch.setattr("fleet_subscriber.heartbeat.Path", _PathStub(tmp_path))
    assert installed_package_version() is None


def test_installed_package_version_reads_the_release_receipt(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Falls back to the ``.fleet-release-sha`` receipt that
    ``deploy-hub-release.sh`` already writes into every release dir."""
    monkeypatch.delenv("FLEET_RELEASE_SHA", raising=False)
    (tmp_path / ".fleet-release-sha").write_text("c0ffee1\n", encoding="utf-8")
    monkeypatch.setattr("fleet_subscriber.heartbeat.Path", _PathStub(tmp_path / "pkg" / "mod.py"))
    result = installed_package_version()
    assert result is not None
    assert result.endswith("+c0ffee1"), "trailing newline must be stripped"


def test_installed_package_version_ignores_an_empty_receipt(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """An empty/whitespace receipt is unknown, not an empty-suffix version."""
    monkeypatch.delenv("FLEET_RELEASE_SHA", raising=False)
    (tmp_path / ".fleet-release-sha").write_text("   \n", encoding="utf-8")
    monkeypatch.setattr("fleet_subscriber.heartbeat.Path", _PathStub(tmp_path / "pkg" / "mod.py"))
    assert installed_package_version() is None


def test_installed_package_version_survives_a_missing_dist(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A missing dist name never throws; the sha alone still identifies the host."""

    def _boom(_name: str) -> str:
        raise PackageNotFoundError("fleet-subscriber")

    monkeypatch.setenv("FLEET_RELEASE_SHA", "d00dfee")
    monkeypatch.setattr("fleet_subscriber.heartbeat.version", _boom)
    assert installed_package_version() == "d00dfee"


def _write_module(pkg_dir: Path) -> Path:
    """Materialize a fake installed ``fleet_subscriber/heartbeat.py``."""
    pkg_dir.mkdir(parents=True)
    module = pkg_dir / "heartbeat.py"
    module.write_text("# stand-in for the installed module\n", encoding="utf-8")
    return module


@pytest.mark.parametrize(
    ("layout", "levels"),
    [
        # The sealed hub install: releases/<sha>/.venv/lib/python3.13/
        # site-packages/fleet_subscriber/ — receipt at the release root.
        ("hub", (".venv", "lib", "python3.13", "site-packages", "fleet_subscriber")),
        # The editable agent-host install: <src>/packages/fleet-subscriber/src/
        # fleet_subscriber/ — receipt at the synced tree root.
        ("agent-host", ("packages", "fleet-subscriber", "src", "fleet_subscriber")),
    ],
)
def test_release_receipt_is_found_through_the_real_path_walk(
    layout: str,
    levels: tuple[str, ...],
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Both REAL deployment layouts resolve, without stubbing ``Path``.

    The other receipt tests monkeypatch the module-global ``Path``, so they
    never exercise the actual ``Path(__file__).resolve()`` walk at the actual
    depth. This one builds the true directory trees under ``tmp_path`` and
    drives ``_release_sha`` with ``__file__`` pointed into them, so the bounded
    walk depth is covered for the layouts that ship.
    """
    monkeypatch.delenv("FLEET_RELEASE_SHA", raising=False)
    root = tmp_path / layout
    module = _write_module(root.joinpath(*levels))
    (root / ".fleet-release-sha").write_text("d00dfee\n", encoding="utf-8")

    monkeypatch.setattr("fleet_subscriber.heartbeat.__file__", str(module))
    assert _release_sha() == "d00dfee"


def test_release_receipt_walk_is_bounded_and_ignores_distant_files(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """A receipt far above the install is NOT this agent's identity.

    An unbounded walk runs to ``/``, so a stray ``.fleet-release-sha`` in
    ``/opt`` or a developer's home dir would be reported as the running
    release. Placing one deeper than the bound must read as unknown.
    """
    monkeypatch.delenv("FLEET_RELEASE_SHA", raising=False)
    deep = tmp_path.joinpath(*(f"lvl{i}" for i in range(8)))
    module = _write_module(deep / "fleet_subscriber")
    (tmp_path / ".fleet-release-sha").write_text("faceb00\n", encoding="utf-8")

    monkeypatch.setattr("fleet_subscriber.heartbeat.__file__", str(module))
    assert _release_sha() is None


def test_release_receipt_walk_continues_past_an_unreadable_level(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """One unreadable ancestor must not suppress a good receipt above it.

    ``is_file()`` on a level the service user cannot stat raises ``OSError``.
    Aborting the walk there turns a recoverable per-level error into total loss
    of the signal, even though the real receipt sits one level higher.
    """
    monkeypatch.delenv("FLEET_RELEASE_SHA", raising=False)
    module = _write_module(tmp_path / "release" / "inner" / "fleet_subscriber")
    (tmp_path / "release" / ".fleet-release-sha").write_text("beefca7\n", encoding="utf-8")
    blocked = tmp_path / "release" / "inner" / ".fleet-release-sha"

    real_is_file = Path.is_file

    def _flaky_is_file(self: Path) -> bool:
        if self == blocked:
            raise PermissionError(f"cannot stat {self}")
        return real_is_file(self)

    monkeypatch.setattr(Path, "is_file", _flaky_is_file)
    monkeypatch.setattr("fleet_subscriber.heartbeat.__file__", str(module))
    assert _release_sha() == "beefca7"


@pytest.mark.parametrize(
    "junk",
    [
        "abc123\nrm -rf /\nmore",  # multi-line: rides onto the wire verbatim
        "x" * 200_000,  # unbounded: a 200KB beat every 30s per agent
        "feature/my-branch",  # a branch name, not a sha
        "$(whoami)",  # an unexpanded command substitution
        "ABC1234",  # uppercase — the hub enforces lowercase hex
        "abc12",  # too short to identify a commit
    ],
)
def test_release_sha_rejects_malformed_receipt_content(
    junk: str, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Junk in the receipt reads as unknown, never as a version.

    The value is stamped verbatim onto every heartbeat and ``spec.py`` types
    ``package_version`` as a bare ``str`` with no validator, so an unvalidated
    receipt either publishes a plausible-looking wrong identity or floods the
    lane. Only a commit sha is a commit sha.
    """
    monkeypatch.delenv("FLEET_RELEASE_SHA", raising=False)
    module = _write_module(tmp_path / "fleet_subscriber")
    (tmp_path / ".fleet-release-sha").write_text(junk, encoding="utf-8")

    monkeypatch.setattr("fleet_subscriber.heartbeat.__file__", str(module))
    assert _release_sha() is None
    assert installed_package_version() is None


@pytest.mark.parametrize("junk", ["feature/branch", "$(whoami)", "not a sha", "x" * 100_000])
def test_release_sha_rejects_malformed_env_var(junk: str, monkeypatch: pytest.MonkeyPatch) -> None:
    """``FLEET_RELEASE_SHA`` is operator-supplied and equally untrusted."""
    monkeypatch.setenv("FLEET_RELEASE_SHA", junk)
    assert _release_sha() is None
    assert installed_package_version() is None


def test_unknown_version_is_logged_not_silent(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """The absence of package_version must leave a greppable trace.

    The adapter computes this once at connect and then publishes ``null`` every
    30s for the process lifetime. An operator asking "why is package_version
    null on all six agents" needs a log line to find, per the repo rule that a
    fallback is logged before it is returned.
    """
    monkeypatch.delenv("FLEET_RELEASE_SHA", raising=False)
    module = _write_module(tmp_path / "fleet_subscriber")
    monkeypatch.setattr("fleet_subscriber.heartbeat.__file__", str(module))

    records: list[str] = []
    sink_id = logger.add(lambda msg: records.append(msg.record["message"]), level="INFO")
    try:
        assert installed_package_version() is None
    finally:
        logger.remove(sink_id)

    assert any("package_version will report unknown" in line for line in records), records


@pytest.mark.parametrize("junk", ["nan", "inf", "-inf"])
def test_interval_from_env_rejects_non_finite(junk: str, monkeypatch: pytest.MonkeyPatch) -> None:
    """nan compares False against <= 0 — non-finite values need their own gate."""
    monkeypatch.setenv("FLEET_HEARTBEAT_SECONDS", junk)
    with pytest.raises(ValueError, match="must be (finite|positive)"):
        interval_from_env()
