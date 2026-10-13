"""The per-agent heartbeat emitter (#26).

A background loop that publishes one :func:`fleet_nats.heartbeat_envelope` per
interval on this agent's ``subjects.agent_heartbeat(env, agent)`` lane, carrying
identity/liveness fields only: agent id, role, the EFFECTIVE subscribed lanes
(from the role map at connect time), a monotonic ``seq``, and the timestamp of
the last agent turn. The consumer (orchestrator liveness / future dashboard)
derives connected / subscribed / working / dead from these beats — the runtime
complement to the deploy-time ``/connz`` probe.

Determinism under test: id/timestamp/sleep are all injected (the ``IdClock``
precedent); the loop's cadence is observable without wall-clock waits. A beat
failure is logged and the loop continues — liveness reporting must degrade
loudly, never die silently or take the adapter down.
"""

from __future__ import annotations

import asyncio
import math
import os
import re
from collections.abc import Awaitable, Callable
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path
from typing import Protocol

from fleet_nats import Envelope, heartbeat_envelope, subjects
from fleet_nats.tracing import traced
from loguru import logger

_ENV_INTERVAL = "FLEET_HEARTBEAT_SECONDS"
_DEFAULT_INTERVAL_S = 30.0


_ENV_RELEASE_SHA = "FLEET_RELEASE_SHA"
_RELEASE_SHA_FILENAME = ".fleet-release-sha"

# A commit sha and nothing else. An unvalidated receipt is stamped verbatim onto
# every beat on the wire, so a truncated write, a stray editor file, a branch
# name, or an unexpanded `$(...)` would either publish a plausible-looking wrong
# identity or flood the heartbeat lane with a multi-hundred-KB beat every 30s
# per agent (spec.py types package_version as a bare str with no validator).
# Same shape the hub already enforces in deploy-hub-release.sh /
# verify-hub-release.py, so both deploy paths agree on what a sha looks like.
_SHA_PATTERN = re.compile(r"^[0-9a-f]{7,40}$")

# How far above the installed package to look for the receipt. The two real
# layouts are the deepest cases: the sealed hub install is 5 levels up
# (``<release>/.venv/lib/python3.13/site-packages/fleet_subscriber``) and the
# editable agent-host install is 4 (``<src>/packages/fleet-subscriber/src/
# fleet_subscriber``). Bounding at 6 covers both with slack while refusing to
# scan to ``/``, where a stray ``.fleet-release-sha`` in ``/opt`` or a
# developer's home dir would otherwise be reported as this agent's identity.
_RECEIPT_SEARCH_LEVELS = 6


def _release_sha() -> str | None:
    """Return the deployed release sha, or ``None`` when not deployed.

    Two sources, in order. ``FLEET_RELEASE_SHA`` wins so a host can state its
    own identity (and so tests can drive this without a filesystem). Otherwise
    read ``.fleet-release-sha``, the receipt ``deploy-hub-release.sh`` and
    ``install-agent.sh`` both write; we resolve it relative to the installed
    package so it is found through the ``current`` symlink without hardcoding
    ``/opt/fleet-bus``.

    A malformed, empty, or out-of-shape receipt returns ``None`` rather than a
    junk version — an unknown version must read as unknown, never as a
    plausible-looking one. Every ``None`` here is logged: the absence of this
    field is a real operational signal, and an operator asking "why is
    package_version null on all six agents" needs something to grep for.
    """
    env_sha = os.environ.get(_ENV_RELEASE_SHA, "").strip()
    if env_sha:
        if _SHA_PATTERN.fullmatch(env_sha):
            return env_sha
        logger.warning(
            "fleet_subscriber.heartbeat | ignoring malformed {}={!r} "
            "(want a 7-40 char hex commit sha); package_version will report unknown",
            _ENV_RELEASE_SHA,
            env_sha[:80],
        )
        return None

    # Walk up from this module toward the release root, bounded (see above).
    here = Path(__file__).resolve()
    for parent in list(here.parents)[:_RECEIPT_SEARCH_LEVELS]:
        receipt = parent / _RELEASE_SHA_FILENAME
        try:
            if not receipt.is_file():
                continue
            raw = receipt.read_text(encoding="utf-8").strip()
        except OSError as exc:
            # Keep looking upward rather than giving up: one unreadable
            # intermediate directory (root-owned 0700 while the subscriber runs
            # as a service user) must not suppress a perfectly good receipt one
            # level higher. A recoverable per-level error is not a total loss.
            logger.warning(
                "fleet_subscriber.heartbeat | unreadable release receipt {}: {}",
                receipt,
                exc,
            )
            continue
        if _SHA_PATTERN.fullmatch(raw):
            return raw
        logger.warning(
            "fleet_subscriber.heartbeat | ignoring malformed release receipt {} "
            "(want a 7-40 char hex commit sha, got {!r}); "
            "package_version will report unknown",
            receipt,
            raw[:80],
        )
        return None
    logger.info(
        "fleet_subscriber.heartbeat | no release receipt ({}) within {} levels above {} "
        "and {} unset; package_version will report unknown "
        "(install drift is NOT observable on this host)",
        _RELEASE_SHA_FILENAME,
        _RECEIPT_SEARCH_LEVELS,
        here.parent,
        _ENV_RELEASE_SHA,
    )
    return None


def installed_package_version() -> str | None:
    """Return this agent's installed fleet-package version, or ``None``.

    This is the installed agent-host package whose drift #203 is about — the
    emitter stamps it on every beat so install drift becomes observable on the
    bus.

    The value is ``{dist_version}+{release_sha}`` when the host is running a
    deployed release, and ``None`` when no release sha can be determined.

    #225: the dist version ALONE cannot answer the question this field exists
    to answer. All four fleet packages carry a hardcoded ``version = "0.1.0"``,
    so ``version("fleet-subscriber")`` returns the same string on a current host
    and a months-stale one — a drift signal that is structurally incapable of
    showing drift, which reads as "fleet is consistent" precisely when it is
    not. The sha is the part that varies; the dist version is kept as a prefix
    only so the field stays human-readable.

    Returning ``None`` when the sha is unknown is deliberate: a consumer seeing
    an absent version knows it does not know, whereas a bare ``0.1.0`` looks
    like a real answer. Never raises — construction must not throw.
    """
    sha = _release_sha()
    if sha is None:
        # _release_sha already logged the specific reason (malformed value, no
        # receipt found, unreadable levels). Nothing more to add — but never
        # make this branch itself silent: the adapter calls this once at connect
        # and then publishes the result every 30s for the process lifetime.
        return None
    try:
        dist_version = version("fleet-subscriber")
    except PackageNotFoundError:
        logger.warning(
            "fleet_subscriber.heartbeat | fleet-subscriber dist metadata not found; "
            "package_version reports the bare sha {}",
            sha,
        )
        return sha
    return f"{dist_version}+{sha}"


class _PublishBus(Protocol):
    """The one bus capability the emitter needs."""

    async def publish(self, subject: str, envelope: Envelope) -> None:
        """Publish an envelope."""
        ...


def interval_from_env() -> float:
    """Read ``FLEET_HEARTBEAT_SECONDS`` (default 30). Fail loud on junk.

    Raises:
        ValueError: If the variable is set to a non-positive or non-numeric
            value — a typo'd interval must not silently become the default.
    """
    raw = os.environ.get(_ENV_INTERVAL)
    if raw is None:
        return _DEFAULT_INTERVAL_S
    try:
        value = float(raw)
    except ValueError as exc:
        raise ValueError(f"{_ENV_INTERVAL} must be a number, got {raw!r}") from exc
    if not math.isfinite(value):
        raise ValueError(f"{_ENV_INTERVAL} must be finite, got {value}")
    if value <= 0:
        raise ValueError(f"{_ENV_INTERVAL} must be positive, got {value}")
    return value


class HeartbeatEmitter:
    """Emits this agent's heartbeat at a bounded interval.

    Args:
        bus: The connected bus (publish capability only).
        agent_id: This agent's identity.
        env: Subject env prefix (``dev``/``prod``).
        role: The subscriber role value (``worker``/``manager``).
        lanes: The lane names ACTUALLY subscribed at connect time.
        catalog_version: The subject-catalog version this agent connected with
            (#103), carried on every beat so the orchestrator can observe
            version skew across the fleet. ``None`` when unknown.
        package_version: The installed fleet-package version this agent runs
            (#203), carried on every beat so the orchestrator can observe
            install drift across the fleet. ``None`` when unknown.
        new_id: Message-id factory (injected).
        now: ISO-8601 timestamp factory (injected).
        interval_s: Seconds between beats.
        sleep: Injected async sleeper (tests pass a controllable fake).
    """

    def __init__(
        self,
        bus: _PublishBus,
        *,
        agent_id: str,
        env: str,
        role: str,
        lanes: tuple[str, ...],
        new_id: Callable[[], str],
        now: Callable[[], str],
        catalog_version: int | None = None,
        package_version: str | None = None,
        interval_s: float = _DEFAULT_INTERVAL_S,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        self._bus = bus
        self._agent_id = agent_id
        self._env = env
        self._role = role
        self._lanes = lanes
        self._catalog_version = catalog_version
        self._package_version = package_version
        self._new_id = new_id
        self._now = now
        self._interval_s = interval_s
        self._sleep = sleep
        self._seq = 0
        self._last_turn_ts: str | None = None
        self._stopped = False

    @property
    def seq(self) -> int:
        """Beats emitted so far (monotonic, per process)."""
        return self._seq

    def note_turn(self) -> None:
        """Stamp the last-turn timestamp (call when a turn starts/ends)."""
        self._last_turn_ts = self._now()

    def stop(self) -> None:
        """Ask the run loop to exit after the current beat/sleep."""
        self._stopped = True

    @traced
    async def beat(self) -> None:
        """Emit one heartbeat. A failure is logged; it never raises."""
        self._seq += 1
        subject = subjects.agent_heartbeat(self._env, self._agent_id)
        beat = heartbeat_envelope(
            msg_id=self._new_id(),
            ts=self._now(),
            sender=self._agent_id,
            role=self._role,
            lanes=self._lanes,
            seq=self._seq,
            last_turn_ts=self._last_turn_ts,
            catalog_version=self._catalog_version,
            package_version=self._package_version,
        )
        try:
            await self._bus.publish(subject, beat)
            logger.debug(
                "fleet_subscriber.heartbeat | subject={} agent={} seq={}",
                subject,
                self._agent_id,
                self._seq,
            )
        except Exception as exc:  # noqa: BLE001 — liveness must degrade loudly, not die
            logger.warning(
                "fleet_subscriber.heartbeat | beat failed agent={} seq={}: {}",
                self._agent_id,
                self._seq,
                exc,
            )

    @traced
    async def run(self) -> None:
        """Beat every ``interval_s`` seconds until :meth:`stop` (or cancel)."""
        logger.info(
            "fleet_subscriber.heartbeat | started agent={} role={} interval_s={}",
            self._agent_id,
            self._role,
            self._interval_s,
        )
        while not self._stopped:
            await self.beat()
            await self._sleep(self._interval_s)
        logger.info("fleet_subscriber.heartbeat | stopped agent={}", self._agent_id)
