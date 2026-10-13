"""Functional tests for the JetStream KV (CAS) + replay wrappers (fleet-bus #124).

Every test here runs against a REAL embedded ``nats-server -js`` (the ``js_bus``
fixture in conftest.py) — the CAS guarantee is a server-side property, so there
is no honest way to fake it. The load-bearing test is
``test_update_rejects_stale_revision``: it proves a stale-revision write is
rejected, which is the exact guarantee #146 (durable governor) and #150
(kill-switch) rebuild on.
"""

from __future__ import annotations

import asyncio
import shutil
from collections.abc import Awaitable, Callable
from contextlib import aclosing

import pytest
from nats.js.api import KeyValueConfig

from fleet_nats.bus import FleetBus
from fleet_nats.config import BusConfig
from fleet_nats.jetstream import (
    FleetKV,
    Incarnation,
    KVEntry,
    StaleRevisionError,
    create_kv,
    ensure_replay_stream,
    open_kv,
)

# JetStream tests are meaningless without a real server (the CAS revision is a
# server-side property — there is no honest mock). Skip the module when
# nats-server is absent; the fixtures in conftest.py do the same.
pytestmark = pytest.mark.skipif(
    shutil.which("nats-server") is None,
    reason="nats-server binary not on PATH — JetStream CAS/replay cannot be faked; "
    "install nats-server to run these (brew install nats-server).",
)


# --------------------------------------------------------------------------- #
# KV — native revision-CAS (the load-bearing primitive)
# --------------------------------------------------------------------------- #


async def test_create_returns_first_revision(js_bus: FleetBus) -> None:
    """create() establishes a key and returns its first server-side revision."""
    kv = await create_kv(js_bus, "gov-state")
    rev = await kv.create("task-1", b'{"count": 0}')
    assert rev >= 1
    entry = await kv.get("task-1")
    assert entry is not None
    assert entry.value == b'{"count": 0}'
    assert entry.revision == rev


async def test_create_twice_is_rejected(js_bus: FleetBus) -> None:
    """A second create() on an existing key is a CAS loss (StaleRevisionError)."""
    kv = await create_kv(js_bus, "gov-state")
    await kv.create("dup", b"first")
    with pytest.raises(StaleRevisionError) as ei:
        await kv.create("dup", b"second")
    assert ei.value.key == "dup"
    # The stored value must be the FIRST write — the losing create never applied.
    entry = await kv.get("dup")
    assert entry is not None
    assert entry.value == b"first"


async def test_update_happy_path_advances_revision(js_bus: FleetBus) -> None:
    """update() with the current revision commits and returns a new revision."""
    kv = await create_kv(js_bus, "gov-state")
    rev1 = await kv.create("k", b"v1")
    rev2 = await kv.update("k", b"v2", expected_revision=rev1)
    assert rev2 > rev1
    entry = await kv.get("k")
    assert entry is not None
    assert entry.value == b"v2"
    assert entry.revision == rev2


async def test_update_rejects_stale_revision(js_bus: FleetBus) -> None:
    """THE load-bearing test: a stale-revision update is rejected, value unchanged.

    This is the exact guarantee #146/#150 depend on — a writer holding an old
    revision cannot clobber a value another writer already advanced.
    """
    kv = await create_kv(js_bus, "gov-state")
    rev1 = await kv.create("k", b"v1")
    # A concurrent writer advances the key to v2 (revision now rev2).
    rev2 = await kv.update("k", b"v2", expected_revision=rev1)
    assert rev2 > rev1

    # The first writer, still holding the STALE rev1, tries to write v3.
    with pytest.raises(StaleRevisionError) as ei:
        await kv.update("k", b"v3-from-stale-writer", expected_revision=rev1)
    assert ei.value.key == "k"
    assert ei.value.expected_revision == rev1
    assert ei.value.bucket == "gov-state"

    # The stale write MUST NOT have applied — value is still v2, revision rev2.
    entry = await kv.get("k")
    assert entry is not None
    assert entry.value == b"v2"
    assert entry.revision == rev2


class _NaiveClientSideCAS:
    """A DELIBERATELY WRONG CAS: read-compare-write in the client, no atomicity.

    Models the process-local version scheme codex says the server-CAS test must
    exclude. Under a concurrent race where both writers read the same revision
    BEFORE either writes, this lets BOTH "win" (a lost update) — exactly the bug
    a real server-side CAS prevents. Used by the test below to PROVE the race
    harness actually discriminates: the naive impl double-wins, the real KV does
    not.
    """

    def __init__(self, revision: int) -> None:
        self.revision = revision
        self.value: bytes | None = None

    async def update(self, value: bytes, expected_revision: int) -> int:
        current = self.revision  # read
        await asyncio.sleep(0)  # yield → the other writer interleaves here
        if current != expected_revision:  # compare
            raise StaleRevisionError("naive", "k", expected_revision)
        await asyncio.sleep(0)
        self.revision = current + 1  # write
        self.value = value
        return self.revision


async def _run_barriered_race(
    write_a: Callable[[], Awaitable[int]],
    write_b: Callable[[], Awaitable[int]],
) -> list[object]:
    """Run two pre-bound update() calls that hold at a barrier, then fire together.

    Both callables have ALREADY captured their expected revision (the caller binds
    it). The barrier guarantees both writers reach their update() at the same
    instant AFTER both have been scheduled — the condition that separates a real
    server CAS (one winner) from a naive client compare (both win). Returns
    [result_a, result_b], each an ``int`` (won) or a ``StaleRevisionError`` (lost).
    """
    barrier = asyncio.Event()
    ready = asyncio.Event()
    pending = {"n": 2}

    async def writer(write: Callable[[], Awaitable[int]]) -> object:
        pending["n"] -= 1
        if pending["n"] == 0:
            ready.set()
        await barrier.wait()  # both released at once
        try:
            return await write()
        except StaleRevisionError as exc:
            return exc

    t_a = asyncio.create_task(writer(write_a))
    t_b = asyncio.create_task(writer(write_b))
    await asyncio.wait_for(ready.wait(), timeout=5)  # both scheduled + at barrier
    barrier.set()
    return list(await asyncio.gather(t_a, t_b))


async def test_naive_client_side_cas_double_wins_under_barriered_race() -> None:
    """CONTROL: the naive client-side CAS lets BOTH writers win (finding #2/#3).

    Proves the barriered-race harness actually discriminates: against a
    process-local read-compare-write, both writers committed the SAME revision —
    a lost update. The real-KV test below runs the IDENTICAL harness and must NOT
    double-win. Without this control, "one winner" could be an artifact of
    serialization rather than server atomicity.
    """
    naive = _NaiveClientSideCAS(revision=1)  # one in-process store, two callers
    rev = naive.revision  # both writers captured the SAME revision before racing
    results = await _run_barriered_race(
        lambda: naive.update(b"from-a", expected_revision=rev),
        lambda: naive.update(b"from-b", expected_revision=rev),
    )
    winners = [r for r in results if isinstance(r, int)]
    # The naive impl double-wins: both saw rev 1, both wrote — a lost update.
    assert len(winners) == 2, f"naive CAS should double-win, got {results!r}"


async def test_cas_is_server_side_concurrent_race(js_bus: FleetBus, jetstream_server: str) -> None:
    """CAS is server-atomic under a barriered CONCURRENT race (finding #2/#3).

    Runs the SAME barriered-race harness as the naive control above — both
    writers demonstrably read the SAME revision BEFORE either writes (the barrier
    guarantees it), then both attempt update() at once over two SEPARATE bus
    connections. The naive client-side CAS double-wins under this exact harness
    (see control test); the real server-side CAS must yield EXACTLY ONE winner and
    one rejection. So a pass here is server atomicity, not serialization luck — a
    process-local version scheme would double-win and FAIL this assertion.
    """
    kv_a = await create_kv(js_bus, "race")
    rev = await kv_a.create("k", b"seed")

    other = BusConfig(url=jetstream_server, agent_id="pytest-js-2", env="dev")
    async with FleetBus(other) as bus_b:
        kv_b = await open_kv(bus_b, "race")

        results = await _run_barriered_race(
            lambda: kv_a.update("k", b"from-a", expected_revision=rev),
            lambda: kv_b.update("k", b"from-b", expected_revision=rev),
        )
        winners = [r for r in results if isinstance(r, int)]
        losers = [r for r in results if isinstance(r, StaleRevisionError)]
        unexpected = [r for r in results if not isinstance(r, (int, StaleRevisionError))]
        assert unexpected == [], f"unexpected results: {unexpected}"
        # Server-side CAS: EXACTLY one winner, one rejection — NOT a double-win.
        assert len(winners) == 1, f"expected 1 server winner (not a double-win), got {results!r}"
        assert len(losers) == 1, f"expected 1 CAS-rejected loser, got {results!r}"

        # The committed value is whichever writer won; the loser NEVER applied.
        entry = await kv_b.get("k")
        assert entry is not None
        assert entry.value in (b"from-a", b"from-b")
        assert entry.revision == winners[0]


async def test_get_missing_key_is_none_not_error(js_bus: FleetBus) -> None:
    """A missing key reads back as None (a real cold-start state), never raises."""
    kv = await create_kv(js_bus, "gov-state")
    assert await kv.get("never-written") is None


async def test_delete_with_matching_revision_commits(js_bus: FleetBus) -> None:
    """A CAS delete with the current revision commits; the key reads back None."""
    kv = await create_kv(js_bus, "flags")
    rev = await kv.create("kill", b"1")
    assert await kv.delete("kill", expected_revision=rev) is True
    assert await kv.get("kill") is None


async def test_delete_with_stale_revision_is_rejected(js_bus: FleetBus) -> None:
    """A CAS delete with a stale revision is rejected — value survives.

    Guards the kill-switch clear against racing a concurrent re-arm (#150).
    """
    kv = await create_kv(js_bus, "flags")
    rev1 = await kv.create("kill", b"1")
    rev2 = await kv.update("kill", b"2", expected_revision=rev1)
    assert rev2 > rev1
    with pytest.raises(StaleRevisionError):
        await kv.delete("kill", expected_revision=rev1)
    entry = await kv.get("kill")
    assert entry is not None
    assert entry.value == b"2"


async def test_delete_with_zero_revision_is_rejected(js_bus: FleetBus) -> None:
    """CAS delete with expected_revision < 1 is a misuse and is REJECTED (finding #1).

    nats-py drops the CAS header for last <= 0, silently making the delete
    UNCONDITIONAL. A live key at revision 1 must NOT be deletable via
    ``expected_revision=0``; the wrapper rejects the call with ValueError, and
    the key survives. This test FAILS against the pre-fix code (which passed
    last=0 straight through and deleted the key unconditionally).
    """
    kv = await create_kv(js_bus, "flags")
    rev = await kv.create("kill", b"armed")
    assert rev == 1
    with pytest.raises(ValueError, match="expected_revision must be >= 1"):
        await kv.delete("kill", expected_revision=0)
    # The key MUST survive — the bypassed CAS never fired.
    entry = await kv.get("kill")
    assert entry is not None
    assert entry.value == b"armed"


async def test_get_fails_closed_when_bucket_deleted(
    js_bus: FleetBus, jetstream_server: str
) -> None:
    """get() FAILS CLOSED when the whole bucket/store is gone (finding #5).

    An armed kill flag whose KV store was deleted must NOT read back as "unset"
    (fail-open). nats-py collapses bucket-gone into the same KeyNotFoundError as
    a missing key, so the wrapper re-probes the store: a genuine key-miss on an
    intact bucket -> None, but a MISSING BUCKET -> raise. This test FAILS against
    the pre-fix code (which returned None for a deleted bucket).
    """
    kv = await create_kv(js_bus, "kill-store")
    await kv.create("kill", b"armed")
    # Nuke the whole bucket out from under the reader.
    js = js_bus.jetstream()
    await js.delete_key_value("kill-store")

    with pytest.raises(Exception) as ei:  # noqa: PT011 - broad by design (fail-closed)
        await kv.get("kill")
    # It must NOT be swallowed to None — a raise of any kind is the fail-closed
    # contract. (A genuine key-miss path is covered by the None tests above.)
    assert ei.value is not None


async def test_get_fails_closed_on_bucket_delete_recreate_aba(js_bus: FleetBus) -> None:
    """get() FAILS CLOSED when the bucket is deleted AND RECREATED (finding #5, round 2).

    The ABA the round-1 fix missed: if the bucket is deleted and recreated
    (empty) between the read and the fail-closed reprobe, a plain "does the store
    exist now?" reprobe PASSES — and the destroyed armed store reads back as
    "unset" (fail-open, breaks #150's kill flag). The wrapper captures the store
    incarnation (stream ``created``) at open and refuses if it changed. Here we
    simulate the ABA by deleting and recreating the SAME bucket name under the
    handle, then reading a key that never existed on the NEW incarnation. It must
    RAISE (incarnation changed), not return None. FAILS against the round-1 fix,
    which returned None because the recreated store exists.
    """
    kv = await create_kv(js_bus, "aba-store")
    await kv.create("kill", b"armed")
    js = js_bus.jetstream()
    # ABA: destroy the armed store and recreate an EMPTY one with the same name.
    await js.delete_key_value("aba-store")
    await js.create_key_value(config=KeyValueConfig(bucket="aba-store", history=1))

    # The handle still points at the (now recreated) bucket. Reading the key must
    # NOT report "unset" — the store it was opened against is gone.
    with pytest.raises(RuntimeError, match="recreated"):
        await kv.get("kill")


async def test_get_aba_detected_when_created_timestamps_collide(js_bus: FleetBus) -> None:
    """ABA is caught even when the recreated bucket shares the SAME µs ``created``.

    Finding #4 (round 3): the incarnation marker must be a UNIQUE id, not the
    ``created`` timestamp — nats-py truncates ``created`` to microseconds, so two
    incarnations minted in the same µs compare EQUAL and the timestamp-only guard
    fails open. This test forces that exact collision: it captures the handle's
    incarnation, deletes+recreates the bucket, then OVERRIDES the handle's captured
    ``created`` to equal the recreated store's ``created`` (simulating a µs match).
    A timestamp-only guard would then see equal ``created`` and return None
    (fail-open); the UUID-stamp guard still detects the different incarnation and
    RAISES. FAILS against a ``created``-only implementation.
    """
    kv = await create_kv(js_bus, "aba-collide")
    await kv.create("kill", b"armed")
    original = kv._incarnation
    assert original.stamp is not None  # create_kv always stamps a UUID

    js = js_bus.jetstream()
    await js.delete_key_value("aba-collide")
    await js.create_key_value(config=KeyValueConfig(bucket="aba-collide", history=1))

    # Force the µs collision: make the handle's captured `created` identical to the
    # recreated store's `created`, so ONLY the UUID stamp can distinguish them.
    status = await kv._kv.status()
    recreated_created = status.stream_info.created
    kv._incarnation = Incarnation(stamp=original.stamp, created=recreated_created)

    # The recreated store has a DIFFERENT stamp (or none) despite the equal
    # timestamp — the guard must still raise, not return None.
    with pytest.raises(RuntimeError, match="recreated"):
        await kv.get("kill")


async def test_watch_yields_updates_and_delete(js_bus: FleetBus) -> None:
    """watch() yields a KVEntry per set and None for a delete (early-start #150)."""
    kv = await create_kv(js_bus, "watched")
    watch = await kv.watch("flag")
    updates = watch.updates()

    rev = await kv.create("flag", b"on")
    first = await asyncio.wait_for(updates.__anext__(), timeout=5)
    assert first is not None
    assert first.value == b"on"
    assert first.revision == rev

    await kv.delete("flag")
    second = await asyncio.wait_for(updates.__anext__(), timeout=5)
    assert second is None  # delete/tombstone surfaces as None
    await watch.stop()


async def test_watch_survives_long_idle(js_bus: FleetBus) -> None:
    """An idle watch does NOT self-terminate after nats-py's 5s default (finding #6).

    A kill-switch watch must stay alive indefinitely. The critical detail (round-2
    fix): the consumer must already be BLOCKED INSIDE ``updates()`` during the
    idle window, so nats-py's 5s default timeout would ACTUALLY fire against the
    pre-fix code. A background task starts iterating immediately and is parked in
    the underlying ``KeyWatcher.updates`` await before the idle; we then idle past
    5s and set the key. With ``timeout=None`` the same blocked call resumes and
    delivers; with the pre-fix ``updates()`` (default 5s) the underlying call
    would have raised TimeoutError at 5s while parked, killing the generator and
    the background task before the key was ever set — so the received value would
    never arrive.
    """
    kv = await create_kv(js_bus, "idle-watch")
    watch = await kv.watch("flag")

    received: list[object] = []

    async def consume() -> None:
        # Actively iterate from the START — this parks inside updates() well
        # before the key is set, exposing the 5s idle-timeout in pre-fix code.
        async for update in watch.updates():
            received.append(update)
            return  # one delivery is enough for the assertion

    task = asyncio.create_task(consume())
    # Give the task time to reach the blocking await, THEN idle past the 5s window
    # while it is parked inside updates().
    await asyncio.sleep(6.0)
    assert not task.done(), "watch task ended during idle — the 5s timeout fired (bug)"

    rev = await kv.create("flag", b"late")
    await asyncio.wait_for(task, timeout=5)
    assert received
    entry = received[0]
    assert isinstance(entry, KVEntry)
    assert entry.value == b"late"
    assert entry.revision == rev
    await watch.stop()


async def test_watch_stop_ends_generator_cleanly(js_bus: FleetBus) -> None:
    """watch.stop() ends the updates generator cleanly, no sentinel leak (finding #10).

    nats-py's stop() enqueues a STOP_ITER sentinel; iterating past it must end the
    generator (StopAsyncIteration), NOT try to read .operation on the sentinel
    (AttributeError). We stop while a consumer is blocked in __anext__ and assert
    the loop terminates cleanly. This test FAILS against the pre-fix code, which
    treated the sentinel as an entry and raised AttributeError.
    """
    kv = await create_kv(js_bus, "stopwatch")
    watch = await kv.watch("flag")

    collected: list[object] = []

    async def consume() -> None:
        async for update in watch.updates():
            collected.append(update)

    task = asyncio.create_task(consume())
    await asyncio.sleep(0.2)  # let it reach the blocking __anext__
    await watch.stop()
    # The generator must terminate cleanly (no AttributeError from the sentinel).
    await asyncio.wait_for(task, timeout=5)
    assert task.exception() is None


async def test_watch_stop_unblocks_even_if_unsubscribe_fails(js_bus: FleetBus) -> None:
    """stop() unblocks a timeout=None watch EVEN IF unsubscribe raises (round-2 leak).

    The idle-watch fix (timeout=None) introduced a leak: nats-py's stop()
    unsubscribes and THEN enqueues STOP_ITER, so if unsubscribe raises, the
    sentinel is never enqueued and the blocked updates() await hangs FOREVER. The
    fix enqueues STOP_ITER in a finally regardless. We force unsubscribe to raise
    and assert the blocked consumer still terminates. FAILS against the round-1
    code (the consume task would never complete → wait_for TimeoutError).
    """
    kv = await create_kv(js_bus, "leakwatch")
    watch = await kv.watch("flag")

    async def consume() -> None:
        async for _update in watch.updates():
            return

    task = asyncio.create_task(consume())
    await asyncio.sleep(0.2)  # park inside the timeout=None updates() await

    # Force the underlying stop() to raise BEFORE it can enqueue STOP_ITER.
    async def _boom() -> None:
        raise RuntimeError("simulated unsubscribe failure")

    watch._watcher.stop = _boom  # type: ignore[method-assign]

    await watch.stop()  # must still enqueue STOP_ITER via the finally
    # The blocked consumer must be released, not hang forever.
    await asyncio.wait_for(task, timeout=5)


# --------------------------------------------------------------------------- #
# Replay stream (J3) — read-only durable replay
# --------------------------------------------------------------------------- #


async def test_replay_captures_and_replays_in_order(js_bus: FleetBus) -> None:
    """The replay stream durably captures published subjects and replays in order."""
    stream = await ensure_replay_stream(js_bus, "REPLAY_A", ("dev.ev.>",))
    js = js_bus.jetstream()
    await js.publish("dev.ev.a", b"m1")
    await js.publish("dev.ev.b", b"m2")
    await js.publish("dev.ev.a", b"m3")

    replayed = [m async for m in stream.replay()]
    assert [m.data for m in replayed] == [b"m1", b"m2", b"m3"]
    assert [m.subject for m in replayed] == ["dev.ev.a", "dev.ev.b", "dev.ev.a"]
    # Sequences are monotonic (replay order).
    seqs = [m.sequence for m in replayed]
    assert seqs == sorted(seqs)


async def test_replay_subject_filter(js_bus: FleetBus) -> None:
    """replay(subject_filter=...) returns only the matching captured subject."""
    stream = await ensure_replay_stream(js_bus, "REPLAY_B", ("dev.task.>",))
    js = js_bus.jetstream()
    await js.publish("dev.task.a", b"a1")
    await js.publish("dev.task.b", b"b1")
    await js.publish("dev.task.a", b"a2")

    only_a = [m.data async for m in stream.replay("dev.task.a")]
    assert only_a == [b"a1", b"a2"]


async def test_replay_start_sequence(js_bus: FleetBus) -> None:
    """replay(start_sequence=N) skips messages before N."""
    stream = await ensure_replay_stream(js_bus, "REPLAY_C", ("dev.log.>",))
    js = js_bus.jetstream()
    for i in range(1, 5):
        await js.publish("dev.log.x", f"m{i}".encode())

    tail = [m.data async for m in stream.replay(start_sequence=3)]
    assert tail == [b"m3", b"m4"]


async def test_ensure_replay_stream_is_idempotent(js_bus: FleetBus) -> None:
    """ensure_replay_stream re-runs cleanly (deploy safety) and preserves data."""
    s1 = await ensure_replay_stream(js_bus, "REPLAY_D", ("dev.z.>",))
    js = js_bus.jetstream()
    await js.publish("dev.z.a", b"keep")
    # Re-run with the same subjects — must not error or wipe the stream.
    s2 = await ensure_replay_stream(js_bus, "REPLAY_D", ("dev.z.>",))
    assert s2.name == s1.name
    replayed = [m.data async for m in s2.replay()]
    assert replayed == [b"keep"]


async def test_replay_deletes_consumer_no_leak(js_bus: FleetBus) -> None:
    """Each completed replay leaves NO resident server-side consumer (finding #5).

    ``unsubscribe()`` tears down only the client inbox subscription; the pull
    consumer stays resident on the server until its inactivity reap, so repeated
    replays accumulate consumers. The wrapper now deletes the consumer explicitly
    on teardown. We run several replays and assert the stream's consumer_count is
    back to 0 after each — FAILS against the pre-fix code (count climbs 1, 2, 3…).
    """
    stream = await ensure_replay_stream(js_bus, "REPLAY_LEAK", ("dev.leak.>",))
    js = js_bus.jetstream()
    for i in range(1, 6):
        await js.publish("dev.leak.x", str(i).encode())

    for _ in range(3):
        replayed = [m.data async for m in stream.replay()]
        assert len(replayed) == 5
        # After the replay completes, no consumer should remain on the stream.
        info = await js.stream_info("REPLAY_LEAK")
        assert info.state.consumer_count == 0, (
            f"replay leaked a consumer: consumer_count={info.state.consumer_count}"
        )


async def test_replay_deletes_consumer_on_error(js_bus: FleetBus) -> None:
    """The consumer is deleted when the replay ERRORS mid-iteration (finding #5).

    Cleanup runs in the generator's finally on the error path too. Per the replay
    contract, deterministic cleanup on an early break/exception requires closing
    the generator (``contextlib.aclosing``), which an ``async for`` does NOT do
    synchronously when its body raises. Using ``aclosing`` here proves the finally
    tears the consumer down immediately on error. FAILS against a replay whose
    finally omits the consumer delete (the consumer would remain resident).
    """
    stream = await ensure_replay_stream(js_bus, "REPLAY_LEAK_ERR", ("dev.lerr.>",))
    js = js_bus.jetstream()
    for i in range(1, 6):
        await js.publish("dev.lerr.x", str(i).encode())

    with pytest.raises(RuntimeError, match="boom"):
        async with aclosing(stream.replay()) as it:
            async for _msg in it:
                raise RuntimeError("boom")  # abort mid-iteration

    info = await js.stream_info("REPLAY_LEAK_ERR")
    assert info.state.consumer_count == 0, (
        f"errored replay leaked a consumer: consumer_count={info.state.consumer_count}"
    )


async def test_replay_delivers_full_large_backlog_no_ack_stall(js_bus: FleetBus) -> None:
    """Replay delivers EVERY stored message past the ack-pending window (finding #7).

    A push+no-ack replay either stalls at max_ack_pending (explicit ack) or drops
    messages with no flow control (AckPolicy.NONE) — either way a 1105-message
    backlog silently truncates. The pull consumer with explicit ack + bounded
    fetch batches delivers all 1105 in order. Publishes deliberately past nats-py's
    default max_ack_pending of 1000 so a stall/drop would show as a short count.
    """
    stream = await ensure_replay_stream(js_bus, "REPLAY_BIG", ("dev.big.>",))
    js = js_bus.jetstream()
    total = 1105  # deliberately past nats-py's default max_ack_pending of 1000
    for i in range(1, total + 1):
        await js.publish("dev.big.x", str(i).encode())

    replayed = [m async for m in stream.replay()]
    assert len(replayed) == total, f"expected {total}, got {len(replayed)}"
    # Contiguous and in order — no gaps, no truncation.
    assert [m.data for m in replayed] == [str(i).encode() for i in range(1, total + 1)]


async def test_replay_excludes_messages_after_snapshot(js_bus: FleetBus) -> None:
    """Replay never includes messages published AFTER the replay began (finding #9).

    ``replay`` snapshots last_seq at call time; the mirrored subject keeps flowing.
    A MATCHING message published mid-iteration has a stream seq ABOVE the snapshot
    and must be excluded. The round-2 design detail: enough pre-snapshot backlog
    that iteration is still ongoing when the post-snapshot message lands, and the
    post message is on the SAME captured subject — so a boundless replay (no
    ``seq > last_seq`` guard) WOULD keep pulling and include it. We publish the
    post-snapshot message after the FIRST delivered message (iteration far from
    done) and assert it never appears and the count equals the snapshot size. This
    FAILS against pre-fix code that yielded without enforcing the snapshot bound.
    """
    stream = await ensure_replay_stream(js_bus, "REPLAY_SNAP", ("dev.snap.>",))
    js = js_bus.jetstream()
    pre_count = 50  # a real backlog: iteration is nowhere near done at msg #1
    for i in range(1, pre_count + 1):
        await js.publish("dev.snap.x", f"pre{i}".encode())

    collected: list[bytes] = []
    posted = False
    async for msg in stream.replay():
        collected.append(msg.data)
        if not posted:
            # Publish a MATCHING message right after the first delivery, while the
            # replay is still mid-iteration. Its seq is above the snapshot; a
            # boundless replay would keep pulling and include it.
            await js.publish("dev.snap.x", b"post-snapshot")
            posted = True

    assert b"post-snapshot" not in collected, "post-snapshot message leaked into replay"
    # Exactly the snapshot contents, in order — nothing published mid-iteration.
    assert collected == [f"pre{i}".encode() for i in range(1, pre_count + 1)]


async def test_replay_raises_on_consumer_info_failure(js_bus: FleetBus) -> None:
    """A consumer_info() failure RAISES, never silently 'drained' (finding #7 pt 2).

    A transient consumer_info() error must fail the replay loudly — returning an
    INCOMPLETE replay as SUCCESS would silently corrupt the backtest. We publish a
    backlog, then patch the pull-subscription's consumer_info to raise mid-replay,
    and assert the replay propagates the error instead of stopping and reporting a
    partial result as complete. FAILS against the round-1 code, which caught the
    consumer_info error and `break`ed (returning a truncated replay as success).
    """
    stream = await ensure_replay_stream(js_bus, "REPLAY_CIERR", ("dev.cierr.>",))
    js = js_bus.jetstream()
    for i in range(1, 11):
        await js.publish("dev.cierr.x", f"m{i}".encode())

    # Patch the STREAM'S OWN JetStream context (the one replay() uses): wrap
    # pull_subscribe so the returned subscription's consumer_info raises after the
    # first successful call (a transient server/consumer error mid-replay).
    real_pull_subscribe = stream._js.pull_subscribe
    call_state = {"info_calls": 0}

    async def _patched_pull_subscribe(*args: object, **kwargs: object):  # type: ignore[no-untyped-def]
        psub = await real_pull_subscribe(*args, **kwargs)  # type: ignore[arg-type]
        real_info = psub.consumer_info

        async def _flaky_info():  # type: ignore[no-untyped-def]
            call_state["info_calls"] += 1
            if call_state["info_calls"] >= 2:
                raise RuntimeError("simulated transient consumer_info failure")
            return await real_info()

        psub.consumer_info = _flaky_info  # type: ignore[method-assign]
        return psub

    stream._js.pull_subscribe = _patched_pull_subscribe  # type: ignore[method-assign]

    with pytest.raises(RuntimeError, match="consumer_info"):
        _ = [m async for m in stream.replay()]


# --------------------------------------------------------------------------- #
# jetstream() accessor guard
# --------------------------------------------------------------------------- #


async def test_jetstream_accessor_requires_connection(jetstream_server: str) -> None:
    """FleetBus.jetstream() fails loud before connect (no opaque later failure)."""
    bus = FleetBus(BusConfig(url=jetstream_server, agent_id="pytest-js", env="dev"))
    with pytest.raises(RuntimeError, match="requires a connected bus"):
        bus.jetstream()


async def test_open_kv_missing_bucket_raises(js_bus: FleetBus) -> None:
    """open_kv on a bucket no deploy created fails loud (not silent-create)."""
    from nats.js.errors import BucketNotFoundError, NotFoundError

    with pytest.raises((BucketNotFoundError, NotFoundError)):
        await open_kv(js_bus, "never-provisioned")


async def test_fleetkv_bucket_property(js_bus: FleetBus) -> None:
    """FleetKV exposes its bucket name."""
    kv: FleetKV = await create_kv(js_bus, "named-bucket")
    assert kv.bucket == "named-bucket"
