"""JetStream wrappers — replay stream (J3) + KV with native revision-CAS.

The opt-in JetStream surface for the fleet bus (fleet-bus #124 / J3). Two thin,
typed wrappers over nats-py's JetStream API, both built on an already-connected
:class:`~fleet_nats.bus.FleetBus` (they reuse its one connection via
:meth:`FleetBus.jetstream` — they never dial their own client):

* :class:`FleetKV` — a JetStream **KeyValue** store exposing the server's
  **native per-key revision** as an atomic compare-and-swap (CAS) primitive.
  This is the load-bearing piece: the durable governor state (#146) and the
  fleet-wide kill flag (#150) rebuild on this. The revision is the JetStream
  message sequence for the key — an authoritative, server-side monotonic value,
  NOT a process-local counter — so a stale-revision write is rejected by the
  SERVER, atomically, even across processes. :meth:`FleetKV.update` is the CAS
  write; a mismatch raises :class:`StaleRevisionError`.

* :class:`ReplayStream` — the J3 read-only replay: a JetStream stream that
  durably captures a set of bus subjects so a task's transcript can be replayed
  after the fact (the backtesting foundation). Read-only w.r.t. live flows — it
  only mirrors subjects already flowing on the bus.

Neither wrapper is imported by coordination code. JetStream stays an infra
concern; core publish/subscribe/request never touch it.
"""

from __future__ import annotations

import asyncio
import uuid
from collections.abc import AsyncGenerator, AsyncIterator
from dataclasses import dataclass
from typing import TYPE_CHECKING

from loguru import logger
from nats.errors import TimeoutError as NatsTimeoutError
from nats.js.api import AckPolicy, ConsumerConfig, DeliverPolicy, KeyValueConfig, StreamConfig
from nats.js.errors import (
    BadRequestError,
    BucketNotFoundError,
    KeyNotFoundError,
    KeyWrongLastSequenceError,
    NotFoundError,
)

# JetStream API error codes for "wrong last sequence" — the CAS-conflict signal.
# nats-py maps these to KeyWrongLastSequenceError on create/update, but its KV
# delete path publishes raw and surfaces the generic BadRequestError carrying one
# of these err_codes, so a CAS delete must classify by the code, not only the
# type. Both codes mean the same wrong-last-sequence rejection (verified against
# nats-py kv.py update(), which treats them identically):
#   10071 = JSStreamWrongLastSequenceErrF
#   10164 = JSStreamWrongLastSequenceConstantErr
_WRONG_LAST_SEQUENCE_ERR_CODES = frozenset({10071, 10164})

# Stream-metadata key holding a per-incarnation UUID stamped at create_kv. This
# is the UNIQUE incarnation identity (finding #4, round 3) that get() compares —
# NOT the stream `created` timestamp, which nats-py truncates to microseconds and
# so is NOT unique across two incarnations minted in the same µs. A delete+recreate
# mints a fresh UUID (or, for a foreign/legacy bucket, has none), so a mismatch or
# absence is a positive ABA signal that fails CLOSED.
_INCARNATION_META_KEY = "fleet_incarnation"

if TYPE_CHECKING:
    from datetime import datetime

    from nats.js import JetStreamContext
    from nats.js.kv import KeyValue

    from fleet_nats.bus import FleetBus


@dataclass(frozen=True, slots=True)
class Incarnation:
    """A KV store's unique incarnation identity, captured when a handle opens.

    Used by :meth:`FleetKV.get` to detect a delete-and-recreate (ABA) between a
    read and its fail-closed reprobe. Two fields:

    * ``stamp`` — a per-incarnation UUID stamped into the stream metadata at
      :func:`create_kv`. This is the AUTHORITATIVE unique identity: a recreated
      bucket gets a fresh UUID (or, for a foreign/legacy bucket created outside
      this library, ``None``). Any change — or the disappearance of a stamp that
      was present at open — is a positive "recreated" signal.
    * ``created`` — the stream ``created`` timestamp, kept as a SECONDARY signal
      only. It is NOT unique on its own (µs truncation), so it never stands alone:
      it is compared as a tiebreak/corroborator, never as the sole marker.

    The reprobe (:meth:`same_incarnation`) is deliberately conservative: it treats
    "cannot positively confirm the SAME incarnation" as a mismatch, so the
    ambiguous case fails CLOSED rather than reading a recreated store as "key
    legitimately missing".
    """

    stamp: str | None
    created: datetime | None

    def same_incarnation(self, other: Incarnation) -> bool:
        """True only if ``other`` is PROVABLY the same store incarnation as self.

        Fail-closed logic: returns ``True`` only when we can positively confirm
        identity, ``False`` (a mismatch → the caller raises) otherwise.

        * If this handle captured a UUID ``stamp`` at open, the current store MUST
          present the identical stamp — a different or absent stamp means the
          store was recreated (the stamp does not survive delete+recreate).
        * If this handle had NO stamp (a foreign/legacy bucket), we cannot rely on
          a UUID, so we fall back to the ``created`` timestamp AND require the
          current store to ALSO be stamp-less: a store that has GAINED a stamp is a
          different incarnation. Because ``created`` is not µs-unique, this foreign
          path is best-effort — the #146/#150 stores always go through
          :func:`create_kv` and so always carry a stamp (the airtight path).
        """
        if self.stamp is not None:
            # Authoritative: the unique UUID must be present and identical.
            return other.stamp == self.stamp
        # No stamp captured (foreign bucket): a store that now HAS a stamp is a
        # different incarnation; otherwise corroborate with created (best-effort).
        if other.stamp is not None:
            return False
        return self.created is not None and other.created == self.created


class StaleRevisionError(Exception):
    """A CAS write was rejected because the caller's expected revision was stale.

    Raised by :meth:`FleetKV.update` (and :meth:`FleetKV.delete` when passed an
    expected revision) when the key's CURRENT server-side revision no longer
    matches the ``expected_revision`` the caller presented — i.e. another writer
    won the race and committed a newer value first. This is the fail-loud CAS
    signal #146/#150 depend on: the losing writer MUST re-read and retry, never
    blindly overwrite. It wraps nats-py's ``KeyWrongLastSequenceError`` into a
    typed, fleet-owned error that names the bucket, key, and expected revision
    (nats-py's carries only the wrong-sequence protocol text).

    Attributes:
        bucket: The KV bucket the write targeted.
        key: The key whose revision was stale.
        expected_revision: The revision the caller believed was current.
    """

    def __init__(self, bucket: str, key: str, expected_revision: int) -> None:
        """Build the fail-loud CAS-conflict message naming bucket, key, revision."""
        self.bucket = bucket
        self.key = key
        self.expected_revision = expected_revision
        super().__init__(
            f"CAS conflict on {bucket!r}/{key!r}: expected revision "
            f"{expected_revision}, but the key was modified by another writer "
            "first. Re-read the current value+revision and retry; never blindly "
            "overwrite (fleet-bus #146/#150 depend on this rejection)."
        )


@dataclass(frozen=True, slots=True)
class KVEntry:
    """One key's current value and its authoritative server-side revision.

    A trimmed, typed view of nats-py's ``KeyValue.Entry`` carrying only what a
    CAS consumer needs: the ``value`` bytes and the ``revision`` to pass back as
    ``expected_revision`` on the next :meth:`FleetKV.update`. The revision is the
    JetStream message sequence for the key — monotonic and server-authoritative.

    Attributes:
        key: The key this entry is for.
        value: The stored bytes (callers own decoding).
        revision: The current server-side revision (pass to CAS update/delete).
    """

    key: str
    value: bytes
    revision: int


class FleetKV:
    """A JetStream KeyValue store with native revision-CAS (fleet-bus #124).

    Wraps a nats-py ``KeyValue`` handle and exposes a small, typed surface whose
    writes carry the server's native per-key revision. The revision is NOT a
    process-local version — it is the JetStream stream sequence the server
    assigns each write — so :meth:`update` is a real atomic CAS enforced by the
    server across ALL writers and processes.

    Build via :func:`open_kv` (bind an existing bucket) or :func:`create_kv`
    (create-or-bind), both of which take a connected :class:`FleetBus`.

    Values are ``bytes`` in and out — this wrapper is store-shape-agnostic; the
    consumer (governor/kill-switch) owns JSON/msgpack encoding of its state.
    """

    def __init__(self, bucket: str, kv: KeyValue, incarnation: Incarnation) -> None:
        """Store the bucket name, bound nats-py KV handle, and store incarnation.

        Prefer :func:`open_kv` / :func:`create_kv` over calling this directly;
        they own acquiring the handle from a connected bus AND capturing the
        :class:`Incarnation` marker that :meth:`get` uses to detect a
        delete-and-recreate (ABA) between a read and its fail-closed reprobe.
        """
        self._bucket = bucket
        self._kv = kv
        # Incarnation marker (finding #4, round 3): a UNIQUE store identity, not a
        # truncatable timestamp. `created` alone is NOT unique — nats-py truncates
        # it to microseconds, so two incarnations minted in the same µs compare
        # equal and an ABA'd empty store reads as "key missing" (fail-open). The
        # marker instead carries a per-incarnation UUID stamped into the stream's
        # metadata at create_kv; :meth:`get`'s reprobe compares that.
        self._incarnation = incarnation

    @property
    def bucket(self) -> str:
        """The KV bucket name this handle is bound to."""
        return self._bucket

    async def create(self, key: str, value: bytes) -> int:
        """Create ``key`` iff it does not exist; return its first revision.

        The CAS-safe way to establish a key exactly once: if another writer
        created it first, nats-py raises ``KeyWrongLastSequenceError`` (create is
        an ``update`` against revision 0 under the hood), which we surface as a
        :class:`StaleRevisionError` with ``expected_revision=0`` — the loser must
        :meth:`get` and use CAS :meth:`update`, not overwrite.

        Args:
            key: The key to create.
            value: The initial value bytes.

        Returns:
            The new key's revision (pass to a later :meth:`update` as
            ``expected_revision``).

        Raises:
            StaleRevisionError: If the key already exists (another writer won).
        """
        logger.debug("fleetkv.create | bucket={} key={} bytes={}", self._bucket, key, len(value))
        try:
            return await self._kv.create(key, value)
        except KeyWrongLastSequenceError as exc:
            logger.warning(
                "fleetkv.create | bucket={} key={} already exists (CAS lost)",
                self._bucket,
                key,
            )
            raise StaleRevisionError(self._bucket, key, 0) from exc

    async def get(self, key: str) -> KVEntry | None:
        """Return the current value+revision for ``key``, or ``None`` if absent.

        Fail-CLOSED contract for the caller: a missing KEY is ``None`` (a real,
        expected state for governor/kill-switch cold-start), but any OTHER
        JetStream failure — including the BUCKET/STREAM being gone, connection
        dead, corrupt read — propagates. It is NEVER swallowed into ``None``, so a
        read failure can never be mistaken for "key not set" (the exact fail-OPEN
        failure #150 must not make: an armed kill flag whose store was deleted
        must NOT read back as "unset").

        The bucket-gone case needs an explicit guard: nats-py collapses BOTH
        "no message for this subject" (key missing — legitimate) AND
        "stream/bucket does not exist" (store broken — must fail closed) into the
        SAME ``KeyNotFoundError``. So on a not-found we re-probe the bucket via
        :meth:`KeyValue.status` (a ``stream_info`` call) and require POSITIVE
        proof it is the SAME store incarnation captured at open:

        * reprobe raises (store gone / unreachable) -> fail CLOSED (raise);
        * reprobe succeeds but the ``created`` incarnation DIFFERS from the one
          captured at open -> the store was deleted-and-recreated (ABA) between
          the read and the reprobe -> fail CLOSED (raise). A recreated-empty
          store must NOT read as "key legitimately missing" (finding #5, round 2:
          this is the exact ABA that would let an armed #150 kill flag read UNSET);
        * reprobe succeeds AND the incarnation matches -> the key is genuinely
          absent on the SAME live store -> ``None``.

        Args:
            key: The key to read.

        Returns:
            A :class:`KVEntry`, or ``None`` only when the key genuinely does not
            exist on the SAME store incarnation opened. A delete/purge tombstone
            is also reported as ``None``.

        Raises:
            Exception: Any error other than a genuine key-miss on the SAME intact
                store — bucket/stream gone, RECREATED (ABA), connection dead,
                corrupt read — is re-raised (fail-closed; never degraded to
                ``None``).
        """
        try:
            entry = await self._kv.get(key)
        except KeyNotFoundError:
            # Require POSITIVE proof the store is the SAME incarnation. status()
            # does a stream_info; it raises if the stream is gone/unreachable.
            try:
                status = await self._kv.status()
            except Exception as store_exc:
                logger.error(
                    "fleetkv.get | bucket={} key={} store MISSING/unreachable — failing "
                    "CLOSED (not reporting 'unset'): {}",
                    self._bucket,
                    key,
                    store_exc,
                )
                raise
            current_incarnation = _incarnation_from_status(status)
            if not self._incarnation.same_incarnation(current_incarnation):
                logger.error(
                    "fleetkv.get | bucket={} key={} store was RECREATED (incarnation "
                    "changed {} -> {}) between read and reprobe — failing CLOSED "
                    "(an ABA'd empty store must not read as 'unset')",
                    self._bucket,
                    key,
                    self._incarnation,
                    current_incarnation,
                )
                raise RuntimeError(
                    f"FleetKV.get: bucket {self._bucket!r} was deleted and recreated "
                    "under this handle (incarnation changed). Refusing to report the "
                    "key as 'unset' — the store you opened is gone. Re-open the bucket "
                    "before reading again."
                ) from None
            logger.debug(
                "fleetkv.get | bucket={} key={} not found (same store incarnation)",
                self._bucket,
                key,
            )
            return None
        # A delete/purge tombstone reads back with value=None; treat as absent so
        # a consumer never CAS-updates against a tombstone's revision by mistake.
        if entry.value is None or entry.revision is None:
            logger.debug("fleetkv.get | bucket={} key={} tombstone/no-rev", self._bucket, key)
            return None
        return KVEntry(key=key, value=entry.value, revision=entry.revision)

    async def update(self, key: str, value: bytes, expected_revision: int) -> int:
        """CAS write: set ``key`` to ``value`` iff its revision == ``expected_revision``.

        The atomic compare-and-swap the whole wrapper exists for. The server
        checks ``expected_revision`` against the key's current sequence and
        commits ONLY if they match; if another writer committed first, the server
        rejects the write and nats-py raises ``KeyWrongLastSequenceError``, which
        we surface as :class:`StaleRevisionError`. This is atomic and enforced
        server-side across every process — not a read-then-write window in this
        client — so two racing writers cannot both succeed.

        Args:
            key: The key to update.
            value: The new value bytes.
            expected_revision: The revision the caller last observed (from
                :meth:`get`/:meth:`create`). The write commits only if it still
                matches.

        Returns:
            The new revision after a successful commit.

        Raises:
            StaleRevisionError: If ``expected_revision`` no longer matches the
                key's current server-side revision (another writer won the race).
        """
        logger.debug(
            "fleetkv.update | bucket={} key={} expected_rev={} bytes={}",
            self._bucket,
            key,
            expected_revision,
            len(value),
        )
        try:
            return await self._kv.update(key, value, last=expected_revision)
        except KeyWrongLastSequenceError as exc:
            logger.warning(
                "fleetkv.update | bucket={} key={} CAS REJECTED expected_rev={} "
                "(a newer write won) — caller must re-read and retry",
                self._bucket,
                key,
                expected_revision,
            )
            raise StaleRevisionError(self._bucket, key, expected_revision) from exc

    async def put(self, key: str, value: bytes) -> int:
        """Unconditional write (last-writer-wins); return the new revision.

        Escape hatch for the rare write that legitimately has no CAS precondition
        (e.g. a one-writer cold-start seed). Prefer :meth:`create` (first write)
        or :meth:`update` (subsequent writes) so concurrent writers cannot clobber
        each other — ``put`` deliberately does NOT protect against a lost update.

        Args:
            key: The key to write.
            value: The value bytes.

        Returns:
            The new revision.
        """
        logger.debug("fleetkv.put | bucket={} key={} bytes={}", self._bucket, key, len(value))
        return await self._kv.put(key, value)

    async def delete(self, key: str, *, expected_revision: int | None = None) -> bool:
        """Delete ``key``; optionally CAS-guarded on ``expected_revision``.

        With ``expected_revision`` set, the delete is itself a CAS operation — it
        commits only if the key's current revision matches, else raises
        :class:`StaleRevisionError` (so a kill-switch clear cannot race a
        concurrent re-arm). Without it, the delete is unconditional.

        Args:
            key: The key to delete. Pass ``expected_revision`` to make it a CAS
                delete.
            expected_revision: If given, delete only when the current revision
                matches (CAS delete). MUST be ``>= 1`` — a live key's first
                revision is 1, and nats-py silently DROPS the CAS header for a
                ``last <= 0`` value (``if last and last > 0``), turning a
                "CAS delete" into an UNCONDITIONAL one. A non-positive expected
                revision is therefore a caller bug (a stale/zero-initialised
                revision), not a valid CAS target, and is rejected loud rather
                than silently downgraded to an unconditional delete.

        Returns:
            ``True`` on a committed delete.

        Raises:
            ValueError: If ``expected_revision`` is given and is ``< 1`` (would
                bypass CAS — see above).
            StaleRevisionError: If ``expected_revision`` was given and no longer
                matches (another writer changed the key first).
        """
        # Reject the CAS-bypass footgun: nats-py only attaches the
        # EXPECTED_LAST_SUBJECT_SEQUENCE header when last > 0, so a CAS delete at
        # revision 0 (or negative) would silently become UNCONDITIONAL and clobber
        # a concurrently re-armed key. A live key is never at revision < 1, so a
        # sub-1 expected revision is always a bug — fail loud.
        if expected_revision is not None and expected_revision < 1:
            logger.error(
                "fleetkv.delete | bucket={} key={} rejected expected_rev={} (< 1 would bypass CAS)",
                self._bucket,
                key,
                expected_revision,
            )
            raise ValueError(
                f"FleetKV.delete: expected_revision must be >= 1 for a CAS delete "
                f"(got {expected_revision}) — nats-py drops the CAS header for a "
                "non-positive last-revision, which would make this delete "
                "unconditional and let it clobber a concurrent re-arm. A live key's "
                "revision is always >= 1; pass the revision from get()/create(), or "
                "omit expected_revision for an intentionally unconditional delete."
            )
        logger.debug(
            "fleetkv.delete | bucket={} key={} expected_rev={}",
            self._bucket,
            key,
            expected_revision,
        )
        try:
            return await self._kv.delete(key, last=expected_revision)
        except BadRequestError as exc:
            # nats-py's KV delete publishes raw, so a wrong-last-sequence conflict
            # surfaces as a generic BadRequestError (err_code 10071 or 10164), NOT
            # the KeyWrongLastSequenceError create/update raise. Classify by the
            # code: those two codes are our CAS conflict; any other BadRequestError
            # is a real bug and must propagate (fail-loud, never swallowed as a
            # CAS loss).
            if getattr(exc, "err_code", None) not in _WRONG_LAST_SEQUENCE_ERR_CODES:
                logger.error(
                    "fleetkv.delete | bucket={} key={} unexpected bad-request: {}",
                    self._bucket,
                    key,
                    exc,
                )
                raise
            logger.warning(
                "fleetkv.delete | bucket={} key={} CAS REJECTED expected_rev={}",
                self._bucket,
                key,
                expected_revision,
            )
            raise StaleRevisionError(self._bucket, key, expected_revision or 0) from exc

    async def watch(self, key: str) -> KeyWatch:
        """Watch a single key; yields a :class:`KVEntry` per update.

        The early-start watch #150 needs: a consumer subscribes to the kill flag
        and reacts to every change (including the current value on start). Deletes
        surface as an entry with ``value``/``revision`` filtered — a delete yields
        ``None`` through :meth:`KeyWatch.updates`.

        Args:
            key: The key to watch.

        Returns:
            A :class:`KeyWatch` async-iterable of :class:`KVEntry` (or ``None``
            for a delete/tombstone update).
        """
        logger.debug("fleetkv.watch | bucket={} key={}", self._bucket, key)
        watcher = await self._kv.watch(key)
        return KeyWatch(self._bucket, key, watcher)


class KeyWatch:
    """Async-iterable view over a nats-py ``KeyWatcher`` for one key.

    Yields a :class:`KVEntry` for each set, and ``None`` for a delete/purge
    (tombstone) update, so a kill-switch watcher can react to "flag cleared" as
    cleanly as "flag set". nats-py's watcher also emits a ``None`` sentinel to
    mark the end of the initial-values replay; that sentinel is filtered out here
    (it is not a key update). The generator runs until :meth:`stop` is called,
    which it detects and ends cleanly.
    """

    def __init__(self, bucket: str, key: str, watcher: KeyValue.KeyWatcher) -> None:
        """Store the bucket/key context and the underlying nats-py watcher."""
        self._bucket = bucket
        self._key = key
        self._watcher = watcher

    async def updates(self) -> AsyncIterator[KVEntry | None]:
        """Yield each key update: a :class:`KVEntry`, or ``None`` for a delete.

        Blocks INDEFINITELY between updates — a kill-switch watch (#150) must
        never self-terminate on idle. nats-py's ``KeyWatcher.updates`` defaults to
        a 5s timeout that RAISES ``TimeoutError`` on an idle watch (which would
        silently kill the watch), so this passes ``timeout=None`` to wait forever.

        Filters nats-py's end-of-initial-values ``None`` sentinel (not a real
        update). A tombstone (deleted/purged) update yields ``None``. Ends the
        generator cleanly when :meth:`stop` enqueues the ``STOP_ITER`` sentinel.
        """
        stop_iter = self._watcher.STOP_ITER
        while True:
            # timeout=None → wait forever; an idle kill-switch watch must not die.
            # nats-py's KeyWatcher.updates is untyped, hence the ignore.
            update = await self._watcher.updates(timeout=None)  # type: ignore[no-untyped-call]
            # stop() enqueues STOP_ITER — end the generator instead of treating
            # the sentinel as an entry (which would AttributeError on .operation).
            if update is stop_iter:
                logger.debug("fleetkv.watch | bucket={} key={} stopped", self._bucket, self._key)
                return
            if update is None:
                # End-of-initial-values marker from nats-py — not a key change.
                continue
            # A delete/purge surfaces with operation="DEL"/"PURGE" and an EMPTY
            # value (b""), not value=None — so classify by operation, not by a
            # falsy value (a legit set could be empty bytes too). A tombstone
            # yields None so a kill-switch watcher reacts to "flag cleared".
            if update.operation in ("DEL", "PURGE") or update.revision is None:
                logger.debug(
                    "fleetkv.watch | bucket={} key={} delete/tombstone update op={}",
                    self._bucket,
                    self._key,
                    update.operation,
                )
                yield None
                continue
            yield KVEntry(key=self._key, value=update.value or b"", revision=update.revision)

    async def stop(self) -> None:
        """Stop the underlying watcher and GUARANTEE the updates loop unblocks.

        nats-py's ``KeyWatcher.stop`` unsubscribes and THEN enqueues its
        ``STOP_ITER`` sentinel — so if the unsubscribe raises, the sentinel is
        never enqueued and a ``timeout=None`` :meth:`updates` blocks FOREVER
        (finding round 2: the leak the idle-watch fix introduced). To make the
        unblock unconditional we enqueue ``STOP_ITER`` ourselves in a ``finally``,
        regardless of whether nats-py's ``stop`` succeeded — the ``updates`` loop
        keys off the sentinel, not the subscription, so it always terminates.
        """
        try:
            await self._watcher.stop()  # type: ignore[no-untyped-call]
        except Exception as exc:
            logger.warning(
                "fleetkv.watch | bucket={} key={} underlying stop failed: {}",
                self._bucket,
                self._key,
                exc,
            )
        finally:
            # Unconditionally unblock updates(): put STOP_ITER on the watcher's own
            # queue even if nats-py's stop() raised before it could. put_nowait is
            # safe (unbounded queue); a QueueFull is defensively drained-and-retried
            # so the sentinel is never lost.
            try:
                self._watcher._updates.put_nowait(self._watcher.STOP_ITER)
            except asyncio.QueueFull:  # pragma: no cover - defensive
                try:
                    self._watcher._updates.get_nowait()
                except asyncio.QueueEmpty:
                    pass
                self._watcher._updates.put_nowait(self._watcher.STOP_ITER)


def _incarnation_from_status(status: object) -> Incarnation:
    """Build an :class:`Incarnation` from a KV ``status()`` result.

    Reads the per-incarnation UUID from the stream metadata (``None`` for a
    foreign/legacy bucket that was never stamped) plus the ``created`` timestamp
    as a secondary corroborator.
    """
    stream_info = status.stream_info  # type: ignore[attr-defined]
    metadata = stream_info.config.metadata or {}
    stamp = metadata.get(_INCARNATION_META_KEY)
    return Incarnation(stamp=stamp, created=stream_info.created)


async def _capture_incarnation(kv: KeyValue) -> Incarnation:
    """Capture the store's :class:`Incarnation` at open (for ABA detection).

    Used by both factories; :meth:`FleetKV.get` compares against this to detect a
    delete-and-recreate of the same bucket name (finding #4).
    """
    return _incarnation_from_status(await kv.status())


async def _stamp_incarnation(js: JetStreamContext, bucket: str) -> None:
    """Stamp a fresh per-incarnation UUID into the KV bucket's stream metadata.

    Gives the store a UNIQUE identity (finding #4, round 3) that survives across
    reads but NOT across a delete+recreate — so :meth:`FleetKV.get` can detect an
    ABA even when two incarnations share a µs-truncated ``created`` timestamp.
    Idempotent per incarnation: only stamps when no stamp is present, so a re-bind
    of an already-stamped bucket keeps its identity stable.
    """
    stream_name = f"KV_{bucket}"
    info = await js.stream_info(stream_name)
    config = info.config
    existing = (config.metadata or {}).get(_INCARNATION_META_KEY)
    if existing is not None:
        # Already stamped this incarnation — keep it stable across re-binds.
        return
    config.metadata = dict(config.metadata or {})
    config.metadata[_INCARNATION_META_KEY] = uuid.uuid4().hex
    await js.update_stream(config=config)


async def open_kv(bus: FleetBus, bucket: str) -> FleetKV:
    """Bind an EXISTING KV bucket on a connected bus; fail loud if absent.

    Read/write path for a bucket a deploy already provisioned. Does not create —
    a missing bucket is an operator error (the deploy did not run), not something
    to silently create at runtime with default (possibly wrong) limits.

    Args:
        bus: A connected :class:`FleetBus` (reuses its connection).
        bucket: The KV bucket name.

    Returns:
        A :class:`FleetKV` bound to the bucket.

    Raises:
        BucketNotFoundError: If the bucket does not exist (via nats-py).
    """
    js = bus.jetstream()
    try:
        kv = await js.key_value(bucket)
    except (BucketNotFoundError, NotFoundError):
        logger.error("fleetkv.open | bucket={} does not exist — was it provisioned?", bucket)
        raise
    incarnation = await _capture_incarnation(kv)
    logger.info("fleetkv.open | bound bucket={} incarnation={}", bucket, incarnation)
    return FleetKV(bucket, kv, incarnation)


async def create_kv(
    bus: FleetBus,
    bucket: str,
    *,
    history: int = 1,
    ttl_s: float | None = None,
    description: str = "",
) -> FleetKV:
    """Create-or-bind a KV bucket on a connected bus; return a :class:`FleetKV`.

    Idempotent: nats-py's ``create_key_value`` binds an existing bucket with a
    matching config rather than erroring, so this is safe to call at each
    consumer start. Keep ``history`` small (default 1) for a state/flag store —
    CAS needs only the current revision, and a large history is wasted storage.

    Args:
        bus: A connected :class:`FleetBus`.
        bucket: The KV bucket name.
        history: Revisions retained per key (default 1 — current only).
        ttl_s: Optional per-key TTL in seconds (``None`` = no expiry).
        description: Optional human description stored on the bucket.

    Returns:
        A :class:`FleetKV` bound to the created/existing bucket.
    """
    js = bus.jetstream()
    config = KeyValueConfig(
        bucket=bucket,
        history=history,
        ttl=ttl_s,
        description=description or f"fleet KV bucket {bucket}",
    )
    kv = await js.create_key_value(config=config)
    # Stamp a unique incarnation UUID into the stream metadata BEFORE capturing the
    # incarnation, so this handle (and any later re-bind) carries the airtight
    # identity get() needs to detect an ABA (finding #4). This is the path #146/#150
    # always take, so their kill-flag/governor stores are ABA-safe by construction.
    await _stamp_incarnation(js, bucket)
    incarnation = await _capture_incarnation(kv)
    logger.info(
        "fleetkv.create | bucket={} history={} ttl_s={} incarnation_stamp={}",
        bucket,
        history,
        ttl_s,
        incarnation.stamp,
    )
    return FleetKV(bucket, kv, incarnation)


@dataclass(frozen=True, slots=True)
class ReplayMessage:
    """One replayed message from the J3 replay stream.

    The durable record of a message that flowed on the bus: its ``subject``, the
    raw ``data`` bytes (decode with ``Envelope.from_bytes`` if it is an envelope),
    and its stream ``sequence`` (monotonic per stream — the replay order).

    Attributes:
        subject: The subject the message was published to.
        data: The raw payload bytes as captured.
        sequence: The stream sequence number (replay ordering).
    """

    subject: str
    data: bytes
    sequence: int


class ReplayStream:
    """J3 read-only replay stream (fleet-bus #124).

    A JetStream stream that durably mirrors a set of bus subjects so a task's
    transcript can be replayed after it ran — the backtesting foundation. It is
    read-only w.r.t. live coordination: it only captures subjects already flowing
    on the core bus (agents publish to core NATS exactly as before; JetStream
    stores a durable copy). Nothing here publishes onto the coordination lanes.

    Build via :func:`ensure_replay_stream` on a connected :class:`FleetBus`.
    """

    def __init__(self, name: str, subjects: tuple[str, ...], js: JetStreamContext) -> None:
        """Store the stream name, captured subjects, and JetStream context."""
        self._name = name
        self._subjects = subjects
        self._js = js

    @property
    def name(self) -> str:
        """The JetStream stream name."""
        return self._name

    @property
    def subjects(self) -> tuple[str, ...]:
        """The subject filters this stream captures."""
        return self._subjects

    async def replay(
        self, subject_filter: str | None = None, *, start_sequence: int = 1
    ) -> AsyncGenerator[ReplayMessage]:
        """Replay stored messages in stream order (oldest first).

        Reads durably from the stream via a short-lived ephemeral consumer —
        purely a READ; it does not ack-consume the live flow or mutate the
        stream. Yields each stored message as a :class:`ReplayMessage`. The
        server-side consumer is DELETED on teardown so no consumer leaks
        (finding #5).

        Cleanup timing (important): the consumer delete runs in this generator's
        ``finally``. Python runs an async-generator ``finally`` when the generator
        is CLOSED — which happens automatically when an ``async for`` loop runs to
        completion, but NOT synchronously if the loop body raises and propagates.
        For guaranteed, immediate cleanup on an early break or exception, wrap the
        iteration in :func:`contextlib.aclosing`::

            async with aclosing(stream.replay()) as it:
                async for msg in it:
                    ...  # break / raise here still tears the consumer down now

        On normal completion (the common case) the consumer is deleted
        deterministically without ``aclosing``.

        Args:
            subject_filter: If given, replay only messages on this subject (a
                concrete subject or a wildcard within the captured set). ``None``
                replays every captured subject.
            start_sequence: Stream sequence to start from (default 1 = beginning).

        Yields:
            :class:`ReplayMessage` for each stored message, oldest first.
        """
        logger.info(
            "replaystream.replay | stream={} filter={} start_seq={}",
            self._name,
            subject_filter,
            start_sequence,
        )
        # SNAPSHOT bound: capture last_seq ONCE up front. Replay is a one-shot READ
        # of what was stored at this instant — anything published AFTER this
        # snapshot (a live message still flowing onto the mirrored subject) is NOT
        # part of this replay and is excluded by the `seq > last_seq` guard below
        # (finding #9). The consumer starts AT start_sequence via the server-side
        # BY_START_SEQUENCE policy.
        info = await self._js.stream_info(self._name)
        last_seq = info.state.last_seq
        if last_seq < start_sequence:
            logger.debug("replaystream.replay | stream={} empty/beyond range", self._name)
            return
        # PULL consumer with explicit ack + bounded fetch batches (round-2 fix).
        #
        # The previous PUSH + AckPolicy.NONE approach had NO flow control: the
        # server firehoses all matches and a slow async consumer overruns nats-py's
        # client buffers, DROPPING messages while server num_pending still hits 0 —
        # a silently TRUNCATED replay (worse than the original ack-stall). A PULL
        # consumer only delivers what we fetch(); we ack each message to advance the
        # consumer, and re-fetch until the consumer reports num_pending==0. This is
        # the correct bounded, complete replay pattern.
        config = ConsumerConfig(
            deliver_policy=DeliverPolicy.BY_START_SEQUENCE,
            opt_start_seq=start_sequence,
            ack_policy=AckPolicy.EXPLICIT,
        )
        psub = await self._js.pull_subscribe(
            subject_filter or ">", stream=self._name, config=config
        )
        # Capture the server-side consumer name up front so teardown can DELETE it.
        # unsubscribe() only tears down the client inbox subscription — it leaves
        # the durable-style consumer resident on the server until its inactivity
        # reap, so repeated replays accumulate consumers (finding #5, round 3). We
        # delete it explicitly in the finally, on BOTH completion and error.
        consumer_name: str | None = None
        try:
            consumer_name = (await psub.consumer_info()).name
        except Exception as exc:  # pragma: no cover - defensive
            logger.warning(
                "replaystream.replay | could not read consumer name for cleanup: {}", exc
            )
        fetched_any = False
        try:
            while True:
                # Have we drained? A consumer_info() error is a HARD failure — it
                # must RAISE, never be treated as "drained" (round-2 fix): returning
                # an incomplete replay as SUCCESS would silently corrupt a replay.
                cinfo = await psub.consumer_info()
                if cinfo.num_pending == 0 and fetched_any:
                    # Nothing left AND we have fetched at least once — genuinely
                    # done. (fetched_any guards against a spurious "num_pending==0"
                    # before the consumer has been asked for anything.)
                    logger.debug(
                        "replaystream.replay | stream={} drained (num_pending=0)",
                        self._name,
                    )
                    break
                try:
                    batch = await psub.fetch(batch=256, timeout=5.0)
                except NatsTimeoutError:
                    # A fetch timeout is only benign if the consumer confirms it is
                    # drained; re-check. If num_pending is still > 0 here, the server
                    # said there is a backlog but sent nothing within the window —
                    # surface it rather than hang or silently truncate.
                    cinfo = await psub.consumer_info()
                    if cinfo.num_pending == 0:
                        logger.debug(
                            "replaystream.replay | stream={} drained after fetch timeout",
                            self._name,
                        )
                        break
                    raise RuntimeError(
                        f"replaystream.replay: stream {self._name!r} reports "
                        f"{cinfo.num_pending} pending message(s) but fetch delivered "
                        "none within the window — refusing to return a truncated "
                        "replay as success."
                    ) from None
                fetched_any = True
                for msg in batch:
                    seq = msg.metadata.sequence.stream
                    # Snapshot bound (finding #9): never yield a message published
                    # AFTER the replay began. Sequences are monotonic, so once we
                    # pass the snapshot we ack-and-stop (ack keeps the ephemeral
                    # consumer tidy; we simply do not yield past the bound).
                    if seq > last_seq:
                        await msg.ack()
                        logger.debug(
                            "replaystream.replay | stream={} reached snapshot bound "
                            "(seq={} > last_seq={}) — stopping",
                            self._name,
                            seq,
                            last_seq,
                        )
                        return
                    yield ReplayMessage(subject=msg.subject, data=msg.data, sequence=seq)
                    # Ack advances the pull consumer so the next fetch returns the
                    # following messages (no max_ack_pending stall, no push-drop).
                    await msg.ack()
        finally:
            # Tear down the client subscription, THEN delete the server-side
            # consumer so none is left resident (finding #5). Both run regardless
            # of how the loop exited (completion, snapshot-bound return, or error).
            try:
                await psub.unsubscribe()
            except Exception as exc:
                logger.warning("replaystream.replay | unsubscribe failed: {}", exc)
            if consumer_name is not None:
                try:
                    await self._js.delete_consumer(self._name, consumer_name)
                    logger.debug(
                        "replaystream.replay | stream={} deleted consumer={}",
                        self._name,
                        consumer_name,
                    )
                except NotFoundError:
                    # Already gone (unsubscribe of an ephemeral may have reaped it)
                    # — the goal state (no resident consumer) is achieved.
                    logger.debug(
                        "replaystream.replay | stream={} consumer={} already gone",
                        self._name,
                        consumer_name,
                    )
                except Exception as exc:
                    logger.warning(
                        "replaystream.replay | failed to delete consumer={}: {}",
                        consumer_name,
                        exc,
                    )


async def ensure_replay_stream(
    bus: FleetBus,
    name: str,
    subjects: tuple[str, ...],
    *,
    max_age_s: float | None = None,
    max_bytes: int = -1,
) -> ReplayStream:
    """Create-or-update the J3 replay stream capturing ``subjects``.

    Idempotent: uses nats-py ``add_stream`` when absent and ``update_stream``
    when the stream already exists with different subjects/limits, so a deploy
    can re-run it safely. The stream is a durable mirror — agents keep publishing
    to core NATS; JetStream stores a copy for replay.

    Args:
        bus: A connected :class:`FleetBus`.
        name: The stream name.
        subjects: The subject filters to capture (e.g. ``("dev.agent.>",)``).
        max_age_s: Optional retention age in seconds (``None`` = keep until other
            limits bite).
        max_bytes: Optional total-size cap in bytes (``-1`` = unbounded).

    Returns:
        A :class:`ReplayStream` bound to the stream.
    """
    js = bus.jetstream()
    config = StreamConfig(
        name=name,
        subjects=list(subjects),
        max_age=max_age_s,
        max_bytes=max_bytes,
    )
    try:
        await js.add_stream(config=config)
        logger.info("replaystream.ensure | created stream={} subjects={}", name, subjects)
    except Exception as exc:
        # Already exists (or config drift) — reconcile via update rather than
        # failing the deploy. nats-py raises a generic error on a duplicate name;
        # update_stream is the idempotent reconcile.
        logger.debug("replaystream.ensure | add failed ({}) — updating stream={}", exc, name)
        await js.update_stream(config=config)
        logger.info("replaystream.ensure | updated stream={} subjects={}", name, subjects)
    return ReplayStream(name, subjects, js)
