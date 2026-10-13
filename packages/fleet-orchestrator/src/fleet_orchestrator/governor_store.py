"""Durable governor-counter persistence (fleet-bus #106) — FLAG-GATED, OFF by default.

The governor's budget/pause counters live in-process on one hub (governor.py).
A crash resets them — and those counters were the ONLY terminator of the 67%
relay storm (u1/#96). This module makes the counters durable so a hub restart
can *resume* a task's counters instead of resetting them to zero.

**Substrate: JetStream KV with server-authoritative revision-CAS.** Durable
governor counters persist to a JetStream KV bucket via the fleet-nats
:class:`~fleet_nats.FleetKV` wrapper (fleet-bus #124). One KV key per task holds
that task's latest counter snapshot as a JSON blob. Each write carries the key's
current SERVER revision as the compare-and-set token: the JetStream stream
sequence the server assigns the key — an authoritative, server-side monotonic
value, NOT a process-local counter. A stale write from an old hub is rejected by
the SERVER atomically (:class:`~fleet_nats.StaleRevisionError`), across every
process, so the counters can never roll backward. This REPLACES the old k-board
append-only event log + application-layer ``version`` scheme, which had NO
server-side CAS and enforced monotonicity only in the client (a delayed/reordered
append could still slip a backward roll past the guard).

**IDENTITY-GATED — blocked on T5 + u7/#99.** Per the ratified execution order,
F4 is sequenced AFTER identity stabilizes: state persisted against an unstable
``task_id``/identity is corrupt state. Therefore this module ships the durable
MECHANISM but the cutover is gated behind ``FLEET_GOVERNOR_DURABLE`` (default
``false``). With the flag OFF the hub's governor is byte-for-byte today's
in-memory behavior — no read, no write, no KV dependency on the hot path.
Enabling it is an OPERATOR decision made only once identity is stable, NOT a
merge decision.

**Write failure policy (runaway-terminator counter).** If a durable write fails
while the flag is ON — including a CAS loss where another hub won the race — we
FAIL LOUD (WARNING) but keep the IN-MEMORY counter authoritative for the live
process and do NOT halt the hub. Rationale: the in-memory governor still
terminates the storm *this* run (its counters never depend on the KV). Halting
mid-storm would remove the very terminator we are protecting. On a CAS loss the
store first RE-READS to refresh its cached revision so the next persist uses the
fresh revision, then RE-RAISES so :meth:`Governor.persist_state` applies the
WARNING-and-keep policy. Loud, never silent (we never lose a counter to a
swallowed exception); never fatal.

**Read failure policy (fail CLOSED).** A load READ failure or a MALFORMED durable
record does NOT fall open to zero. Zeroing on restart would reset the SOLE
runaway terminator during a brief KV outage. :meth:`FleetKV.get` fails closed by
construction (only a genuine key-miss on the same store incarnation returns
``None``; any other failure raises), and this store re-raises on a corrupt record
so the governor's hydrate pauses the task (assume at-risk) rather than crashing.
"""

from __future__ import annotations

import json
import os
from typing import Any, Protocol, runtime_checkable

from fleet_nats import FleetBus, FleetKV, StaleRevisionError, create_kv
from loguru import logger

# The env flag that gates the whole durable-governor cutover. Default OFF: an
# unset/false flag yields NO store, so the governor is byte-for-byte in-memory.
_ENV_DURABLE = "FLEET_GOVERNOR_DURABLE"

# Recognised bool spellings (kept local so this module has no config.py import
# cycle; mirrors config._bool_env semantics — a typo refuses to start).
_TRUE_TOKENS = frozenset({"1", "true", "yes", "on"})
_FALSE_TOKENS = frozenset({"0", "false", "no", "off"})

# The default KV bucket the durable governor state lives in. One bucket for the
# whole hub; one key per task_id inside it.
_DEFAULT_BUCKET = "fleet-governor-state"


def durable_enabled_from_env() -> bool:
    """Whether durable governor state is enabled (``FLEET_GOVERNOR_DURABLE``).

    Default ``False`` (identity-gated cutover — see the module docstring). Fails
    loud on an unrecognised spelling: a policy switch must never silently coerce a
    typo to the wrong default.

    Raises:
        ValueError: On an unrecognised (typo'd) truthy/falsy spelling.
    """
    raw = os.environ.get(_ENV_DURABLE)
    if raw is None:
        return False
    token = raw.strip().lower()
    if token in _TRUE_TOKENS:
        return True
    if token in _FALSE_TOKENS:
        return False
    raise ValueError(
        f"{_ENV_DURABLE} must be one of {sorted(_TRUE_TOKENS | _FALSE_TOKENS)}, got {raw!r}"
    )


@runtime_checkable
class GovernorStore(Protocol):
    """The durable persistence a :class:`~fleet_orchestrator.governor.Governor` uses.

    Two verbs, both keyed by ``task_id``: persist a counter snapshot, and load the
    latest persisted snapshot (``None`` when a task has never been persisted). The
    snapshot is a plain JSON-able dict owned by the governor (its serialized
    counters) — the store is agnostic to its shape.
    """

    async def persist(self, task_id: str, snapshot: dict[str, Any]) -> None:
        """Durably record ``snapshot`` for ``task_id`` (server-CAS write)."""
        ...

    async def load(self, task_id: str) -> dict[str, Any] | None:
        """Return the latest persisted snapshot for ``task_id``, or ``None``."""
        ...


class FleetKVGovernorStore:
    """A :class:`GovernorStore` over a JetStream KV bucket with server-CAS.

    Each task's counter snapshot is one KV key (the task_id) holding a JSON blob.
    Writes go through the server-authoritative revision-CAS of
    :class:`~fleet_nats.FleetKV`: the first write is :meth:`FleetKV.create`
    (fails if the key already exists), and subsequent writes are
    :meth:`FleetKV.update` against the last observed revision. A stale write from
    an old hub is rejected by the SERVER (:class:`~fleet_nats.StaleRevisionError`),
    so the counters can never roll backward — that guard is enforced server-side,
    NOT by an application-layer version number.

    Args:
        kv: An already-opened :class:`~fleet_nats.FleetKV` handle (injected — this
            store never dials its own connection; use :func:`open_governor_store`
            to build one over a connected bus via the ABA-safe ``create_kv`` path).
    """

    def __init__(self, kv: FleetKV) -> None:
        self._kv = kv
        # The LAST SERVER revision observed per task_id, learned from THIS store's
        # own load()/persist() calls. This is NOT a process-local version counter:
        # it is the SERVER's authoritative revision (the JetStream stream sequence
        # for the key) cached so the next CAS update() knows its expected_revision.
        # A CAS loss refreshes it via a re-read so the next persist uses the fresh
        # revision.
        self._revisions: dict[str, int] = {}

    async def persist(self, task_id: str, snapshot: dict[str, Any]) -> None:
        """Write ``snapshot`` for ``task_id`` under server-CAS.

        The KV key IS the ``task_id`` (card ids are already KV-safe, so no
        sanitization is applied). First write for a task uses
        :meth:`FleetKV.create`; subsequent writes use the CAS :meth:`FleetKV.update`
        against the last observed revision.

        On a CAS loss (:class:`~fleet_nats.StaleRevisionError` — another hub won the
        race) the store RE-READS to refresh its cached revision so a later retry
        uses the fresh revision, logs a WARNING naming the task, then RE-RAISES so
        :meth:`Governor.persist_state` keeps the in-memory counter authoritative
        (fail-loud, non-fatal). It does NOT retry-loop here: the governor's next
        envelope persist uses the refreshed revision.

        Raises:
            StaleRevisionError: On a CAS loss (re-raised after refreshing the
                cached revision).
        """
        body = json.dumps(snapshot, separators=(",", ":"), sort_keys=True).encode()
        if task_id not in self._revisions:
            try:
                rev = await self._kv.create(task_id, body)
            except StaleRevisionError:
                # The key already exists (another hub — or a prior incarnation —
                # created it first). Learn its current revision and CAS-update
                # against it, so the first write from THIS store still lands under
                # server-CAS rather than blindly overwriting.
                rev = await self._update_against_current(task_id, body)
        else:
            try:
                rev = await self._kv.update(
                    task_id, body, expected_revision=self._revisions[task_id]
                )
            except StaleRevisionError:
                # CAS LOSS: another hub committed first. Refresh the cached revision
                # from the server so the next persist uses the fresh one, then
                # re-raise for the governor's WARNING-and-keep policy.
                await self._refresh_revision(task_id)
                logger.warning(
                    "governor_store.persist | task={} CAS LOSS (another hub won the race); "
                    "cached revision refreshed, in-memory kept authoritative",
                    task_id,
                )
                raise
        self._revisions[task_id] = rev
        logger.debug(
            "governor_store.persist | task={} revision={} snapshot_keys={}",
            task_id,
            rev,
            sorted(snapshot),
        )

    async def _update_against_current(self, task_id: str, body: bytes) -> int:
        """CAS-update a key that already exists, learning its revision first.

        The create()-lost path: the key was created by another writer/incarnation
        before this store's first write. Re-read to learn the current revision,
        then CAS-update against it. A CAS loss here (a third writer commits between
        the read and the update) refreshes the cache and re-raises, matching the
        update-path policy.
        """
        current = await self._kv.get(task_id)
        if current is None:
            # Raced: the key vanished (deleted) between the create-loss and this
            # read. Treat as a fresh key and create again; a second create-loss
            # would re-raise StaleRevisionError to the caller (fail-loud).
            return await self._kv.create(task_id, body)
        try:
            return await self._kv.update(task_id, body, expected_revision=current.revision)
        except StaleRevisionError:
            await self._refresh_revision(task_id)
            logger.warning(
                "governor_store.persist | task={} CAS LOSS on first-write reconcile; "
                "cached revision refreshed, in-memory kept authoritative",
                task_id,
            )
            raise

    async def _refresh_revision(self, task_id: str) -> None:
        """Re-read the key to refresh the cached server revision after a CAS loss.

        So a subsequent :meth:`persist` retry CAS-updates against the fresh
        revision instead of the stale one. A genuinely-absent key drops the cached
        revision so the next persist re-creates.
        """
        entry = await self._kv.get(task_id)
        if entry is None:
            self._revisions.pop(task_id, None)
            return
        self._revisions[task_id] = entry.revision

    async def load(self, task_id: str) -> dict[str, Any] | None:
        """Read the latest durable snapshot for ``task_id``, or ``None`` if fresh.

        Reads the task's KV key. ``None`` (genuine key-miss on the same store
        incarnation) is the normal "fresh task, start from zero" signal, not an
        error. On a hit, caches the SERVER revision so a later :meth:`persist`
        CAS-updates against it, then decodes the JSON snapshot.

        Fails CLOSED on corruption: an unparseable JSON value, or a decoded value
        that is not a dict, RAISES rather than returning ``None`` — the governor's
        hydrate then pauses the task (assume at-risk) instead of silently zeroing
        the sole runaway terminator. (:meth:`FleetKV.get` itself already fails
        closed on any non-key-miss failure.)

        Raises:
            ValueError: On a corrupt durable record (unparseable JSON, or a decoded
                value that is not a dict).
        """
        entry = await self._kv.get(task_id)
        if entry is None:
            logger.debug(
                "governor_store.load | task={} no durable snapshot (start from zero)", task_id
            )
            return None
        try:
            loaded = json.loads(entry.value)
        except (json.JSONDecodeError, TypeError) as exc:
            logger.error(
                "governor_store.load | task={} CORRUPT durable record (unparseable JSON) — "
                "failing CLOSED (not returning None): {}",
                task_id,
                exc,
            )
            raise ValueError(f"corrupt governor snapshot for task {task_id!r}: {exc}") from exc
        if not isinstance(loaded, dict):
            logger.error(
                "governor_store.load | task={} CORRUPT durable record (not a dict: {}) — "
                "failing CLOSED",
                task_id,
                type(loaded).__name__,
            )
            raise ValueError(
                f"corrupt governor snapshot for task {task_id!r}: expected a JSON object, "
                f"got {type(loaded).__name__}"
            )
        self._revisions[task_id] = entry.revision
        logger.info(
            "governor_store.load | task={} resumed revision={} snapshot_keys={}",
            task_id,
            entry.revision,
            sorted(loaded),
        )
        return loaded

    async def delete(self, task_id: str) -> bool:
        """Delete/tombstone ``task_id`` and evict its cached server revision.

        Completion cleanup must remove the durable snapshot so a later card that
        reuses the same ``task_id`` cannot hydrate stale pause/spend state. The
        delete is CAS-guarded when this store has observed a revision; otherwise it
        first reads the key to learn the current server revision. A genuine miss is
        already clean and returns ``False``.
        """
        revision = self._revisions.get(task_id)
        if revision is None:
            entry = await self._kv.get(task_id)
            if entry is None:
                logger.debug("governor_store.delete | task={} no durable snapshot", task_id)
                return False
            revision = entry.revision
        try:
            deleted = await self._kv.delete(task_id, expected_revision=revision)
        except StaleRevisionError:
            await self._refresh_revision(task_id)
            logger.warning(
                "governor_store.delete | task={} CAS LOSS during cleanup; cached revision "
                "refreshed",
                task_id,
            )
            raise
        self._revisions.pop(task_id, None)
        logger.info("governor_store.delete | task={} deleted={}", task_id, deleted)
        return deleted


async def open_governor_store(bus: FleetBus, bucket: str = _DEFAULT_BUCKET) -> FleetKVGovernorStore:
    """Open (create-or-bind) the durable governor KV bucket and wrap it in a store.

    Uses the :func:`~fleet_nats.create_kv` path, which stamps a per-incarnation
    UUID into the bucket for ABA protection BY CONSTRUCTION — the airtight path a
    durable state store must take.

    Args:
        bus: A connected fleet bus (reuses its connection).
        bucket: The KV bucket name (default ``fleet-governor-state``).

    Returns:
        A :class:`FleetKVGovernorStore` bound to the bucket.
    """
    kv = await create_kv(
        bus,
        bucket,
        description="fleet governor durable counter state (fleet-bus #106)",
    )
    logger.info("governor_store.open | durable governor store bound bucket={}", bucket)
    return FleetKVGovernorStore(kv)


__all__ = [
    "FleetKVGovernorStore",
    "GovernorStore",
    "durable_enabled_from_env",
    "open_governor_store",
]
