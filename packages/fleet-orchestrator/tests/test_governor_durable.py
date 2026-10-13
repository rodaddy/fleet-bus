"""Durable governor-state tests (fleet-bus #106) — FLAG-GATED persistence.

Proves the MECHANISM (the flag-ON path) with functional tests, and proves the
flag-OFF default is an EXACT in-memory no-op. Two layers:

1. :class:`Governor` snapshot/restore/persist_state/hydrate against an in-memory
   fake store and a failing store — the counter round-trip, the simulated-restart
   resume (a FRESH Governor reading persisted state), and the write-failure policy
   (WARNING + in-memory kept authoritative, never halt).
2. :class:`FleetKVGovernorStore` against a REAL embedded ``nats-server -js`` — the
   JetStream KV seam with server-authoritative revision-CAS. The CAS guarantee is
   a server-side property (the JetStream sequence IS the revision), so it is
   proven against a real server (skipped if ``nats-server`` is not on PATH), never
   a mock. The barriered concurrent-race test + its naive client-side control are
   the discrimination proof: a process-local version scheme would double-win.

The flag-OFF proof is the crux: with no store wired, every durable method is a
no-op that returns ``False`` and touches nothing, so the governor is byte-for-byte
today's behavior.
"""

from __future__ import annotations

import asyncio
import json
import shutil
from collections.abc import Awaitable, Callable
from typing import Any

import pytest
from fleet_nats import BusConfig, FleetBus, StaleRevisionError, create_kv, open_kv

from fleet_orchestrator import (
    Budget,
    FleetKVGovernorStore,
    Governor,
    durable_enabled_from_env,
    open_governor_store,
)
from fleet_orchestrator.governor import BreachKind
from fleet_orchestrator.governor_store import GovernorStore

# The KV/CAS tests use the ``js_bus`` / ``jetstream_server`` fixtures in
# conftest.py (which boot a real embedded nats-server). This marker mirrors the
# conftest skip: KV/CAS is a server-side property with no honest mock. Defined
# here (not imported from conftest — tests/ is not a package) so it can decorate.
requires_nats_server = pytest.mark.skipif(
    shutil.which("nats-server") is None,
    reason="nats-server binary not on PATH — JetStream KV CAS cannot be faked; "
    "install nats-server to run these (brew install nats-server).",
)


class _FakeStore:
    """An in-memory GovernorStore: last-write-wins per task, records calls."""

    def __init__(self) -> None:
        self.saved: dict[str, dict[str, Any]] = {}
        self.persist_calls = 0
        self.load_calls = 0
        self.delete_calls = 0

    async def persist(self, task_id: str, snapshot: dict[str, Any]) -> None:
        self.persist_calls += 1
        # Round-trip through JSON so the fake matches the real store's serialization
        # (a snapshot that isn't JSON-able would pass the fake but fail the KV).
        self.saved[task_id] = json.loads(json.dumps(snapshot))

    async def load(self, task_id: str) -> dict[str, Any] | None:
        self.load_calls += 1
        return self.saved.get(task_id)

    async def delete(self, task_id: str) -> bool:
        self.delete_calls += 1
        return self.saved.pop(task_id, None) is not None


class _FailingStore:
    """A GovernorStore whose every op raises — for the failure-policy tests."""

    async def persist(self, task_id: str, snapshot: dict[str, Any]) -> None:
        raise RuntimeError("simulated durable write failure")

    async def load(self, task_id: str) -> dict[str, Any] | None:
        raise RuntimeError("simulated durable read failure")

    async def delete(self, task_id: str) -> bool:
        raise RuntimeError("simulated durable delete failure")


class _SnapshotStore:
    """A GovernorStore that returns one supplied snapshot."""

    def __init__(self, snapshot: dict[str, Any]) -> None:
        self.snapshot = snapshot

    async def persist(self, task_id: str, snapshot: dict[str, Any]) -> None:  # pragma: no cover
        raise AssertionError("not used")

    async def load(self, task_id: str) -> dict[str, Any] | None:
        return self.snapshot

    async def delete(self, task_id: str) -> bool:
        return False


def _budget() -> Budget:
    return Budget(max_msgs=3, max_tokens=1000)


# ---- Flag-OFF is an EXACT in-memory no-op (the crux) -----------------------


def test_governor_without_store_is_not_durable() -> None:
    gov = Governor(_budget())
    assert gov.durable is False


async def test_flag_off_persist_and_hydrate_are_no_ops() -> None:
    # No store wired: persist_state / hydrate return False and touch nothing —
    # byte-for-byte today's behavior.
    gov = Governor(_budget())
    gov.note_message("t-1")
    gov.note_message("t-1")
    assert await gov.persist_state("t-1") is False
    assert await gov.hydrate("t-1") is False
    # In-memory counters are untouched by the (skipped) durable path.
    snap = gov.snapshot("t-1")
    assert snap["msgs"] == 2
    assert snap["paused"] is False


async def test_flag_off_counting_matches_a_bare_governor_exactly() -> None:
    # Two governors, one with the durable API exercised (but no store) and one
    # never touched by it, must reach identical state — proving the flag-off path
    # adds no behavior.
    a = Governor(_budget())
    b = Governor(_budget())
    for _ in range(2):
        a.note_message("t-9")
        b.note_message("t-9")
    await a.persist_state("t-9")  # no-op
    await a.hydrate("t-9")  # no-op
    assert a.snapshot("t-9") == b.snapshot("t-9")


# ---- snapshot / restore round-trip (pure) ----------------------------------


def test_snapshot_zero_state_is_wellformed() -> None:
    gov = Governor(_budget())
    snap = gov.snapshot("t-new")
    assert snap == {
        "task_id": "t-new",
        "msgs": 0,
        "paused": False,
        "breach_kind": None,
        "breach_observed": None,
        "breach_cap": None,
        "cap_override": None,
        "tokens": None,  # no ledger wired
    }


def test_snapshot_restore_roundtrips_counters_and_pause() -> None:
    # Drive a governor over its cap so it pauses, then restore a FRESH governor
    # from the snapshot and prove identical observable state.
    gov = Governor(_budget())
    breach = None
    for _ in range(4):  # cap is 3; the 4th message breaches
        breach = gov.note_message("t-1") or breach
    assert breach is not None
    assert gov.is_paused("t-1")

    snap = gov.snapshot("t-1")
    fresh = Governor(_budget())
    fresh.restore(snap)

    assert fresh.is_paused("t-1") is True
    assert fresh.snapshot("t-1") == snap
    fresh_breach = fresh.breach_for("t-1")
    assert fresh_breach is not None
    assert fresh_breach.kind is BreachKind.MSGS
    assert fresh_breach.observed == 4


def test_restore_reinstates_cap_override() -> None:
    gov = Governor(_budget())
    gov.note_message("t-1")
    gov.pause("t-1")
    gov.resume("t-1", budget_msgs_add=10)  # raises this task's effective cap
    snap = gov.snapshot("t-1")
    assert snap["cap_override"] is not None

    fresh = Governor(_budget())
    fresh.restore(snap)
    assert fresh.snapshot("t-1")["cap_override"] == snap["cap_override"]


def test_restore_malformed_snapshot_raises() -> None:
    gov = Governor(_budget())
    with pytest.raises(KeyError):
        gov.restore({"msgs": 5})  # missing task_id


def test_restore_always_applies_the_snapshot() -> None:
    # The anti-backward-roll guard is NO LONGER at this layer (it is the SERVER's
    # revision-CAS in FleetKVGovernorStore). restore is a pure idempotent apply
    # that ALWAYS returns True — even re-applying a "lower" snapshot succeeds,
    # because the server would never have let a backward-rolling write be written.
    gov = Governor(_budget())
    assert gov.restore({"task_id": "t-1", "msgs": 4, "tokens": None}) is True
    assert gov.snapshot("t-1")["msgs"] == 4
    assert gov.restore({"task_id": "t-1", "msgs": 1, "tokens": None}) is True
    assert gov.snapshot("t-1")["msgs"] == 1  # applied — no client-side guard


# ---- Durable persist / hydrate MECHANISM (flag ON, fake store) --------------


async def test_persist_writes_snapshot_to_store() -> None:
    store = _FakeStore()
    gov = Governor(_budget(), store=store)
    assert gov.durable is True
    gov.note_message("t-1")
    gov.note_message("t-1")
    assert await gov.persist_state("t-1") is True
    assert store.persist_calls == 1
    assert store.saved["t-1"]["msgs"] == 2


async def test_hydrate_resumes_counters_across_a_simulated_restart() -> None:
    # THE #106 ACCEPTANCE MECHANISM: a hub counts messages, persists, "crashes",
    # and a FRESH Governor (the restarted hub) hydrates — counters resume from the
    # store instead of resetting to zero.
    store = _FakeStore()
    crashed = Governor(_budget(), store=store)
    crashed.note_message("t-1")
    crashed.note_message("t-1")
    await crashed.persist_state("t-1")

    # Simulated restart: a brand-new Governor sharing the same durable store.
    restarted = Governor(_budget(), store=store)
    assert restarted.snapshot("t-1")["msgs"] == 0  # in-memory starts at zero
    resumed = await restarted.hydrate("t-1")
    assert resumed is True
    assert restarted.snapshot("t-1")["msgs"] == 2  # resumed, NOT reset

    # And the resumed counter keeps enforcing: the cap is 3, two are already
    # spent, so the SECOND further message breaches — the terminator survives.
    assert restarted.note_message("t-1") is None  # count 3 == cap, not over
    breach = restarted.note_message("t-1")  # count 4 > cap 3
    assert breach is not None
    assert breach.kind is BreachKind.MSGS


async def test_hydrate_resumes_a_paused_task_so_the_terminator_survives() -> None:
    # The runaway-terminator case: a task that PAUSED over budget must come back
    # PAUSED after a restart — otherwise the crash-loop resets the only stop.
    store = _FakeStore()
    crashed = Governor(_budget(), store=store)
    for _ in range(4):  # breach the cap
        crashed.note_message("t-1")
    assert crashed.is_paused("t-1")
    await crashed.persist_state("t-1")

    restarted = Governor(_budget(), store=store)
    assert await restarted.hydrate("t-1") is True
    assert restarted.is_paused("t-1") is True  # still paused after "restart"


async def test_clear_state_deletes_durable_snapshot_and_resets_local_state() -> None:
    from fleet_orchestrator.costfeed import CostLedger

    store = _FakeStore()
    ledger = CostLedger()
    ledger.restore_task("t-1", {"tokens": 900, "seen_turns": [["bilby", "turn-a"]]})
    gov = Governor(_budget(), ledger=ledger, store=store)
    for _ in range(4):
        gov.note_message("t-1")
    assert gov.is_paused("t-1") is True
    assert await gov.persist_state("t-1") is True

    assert await gov.clear_state("t-1") is True
    assert store.delete_calls == 1
    assert await store.load("t-1") is None
    assert gov.snapshot("t-1")["msgs"] == 0
    assert gov.is_paused("t-1") is False
    assert ledger.task_total("t-1") == 0


async def test_same_task_id_after_completion_starts_clean_after_clear_state() -> None:
    store = _FakeStore()
    crashed = Governor(_budget(), store=store)
    for _ in range(4):
        crashed.note_message("t-1")
    await crashed.persist_state("t-1")
    assert await crashed.clear_state("t-1") is True

    restarted = Governor(_budget(), store=store)
    assert await restarted.hydrate("t-1") is False
    assert restarted.snapshot("t-1")["msgs"] == 0
    assert restarted.is_paused("t-1") is False


async def test_hydrate_missing_snapshot_returns_false_and_starts_from_zero() -> None:
    store = _FakeStore()
    gov = Governor(_budget(), store=store)
    assert await gov.hydrate("t-never-persisted") is False
    assert gov.snapshot("t-never-persisted")["msgs"] == 0


# ---- write failure policy (fail loud, keep in-memory, never halt) -----------


async def test_persist_failure_is_loud_but_non_fatal_and_keeps_counter() -> None:
    # A write failure must NOT raise into the hot path or lose the in-memory
    # counter — the storm still terminates this run. persist_state returns False
    # (write lost) but the governor keeps counting.
    gov = Governor(_budget(), store=_FailingStore())
    gov.note_message("t-1")
    gov.note_message("t-1")
    assert await gov.persist_state("t-1") is False  # write failed, no raise
    # In-memory counter survives and still enforces.
    assert gov.snapshot("t-1")["msgs"] == 2
    gov.note_message("t-1")  # count 3 == cap
    breach = gov.note_message("t-1")  # count 4 > cap 3 — terminator still fires
    assert breach is not None


async def test_hydrate_read_failure_fails_closed_and_pauses() -> None:
    # fleet-bus #106: a read failure on restart must FAIL CLOSED, not fall open to
    # zero. Zeroing would reset the sole runaway terminator during a brief KV
    # outage. hydrate returns False, the task is PAUSED (assume at-risk), and an
    # operator must resume once the KV is readable again.
    gov = Governor(_budget(), store=_FailingStore())
    assert await gov.hydrate("t-1") is False
    assert gov.is_paused("t-1") is True  # FAIL CLOSED, not open-to-zero
    # And the pause actually gates: a gated action becomes a NOOP.
    breach = gov.breach_for("t-1")
    assert breach is not None


async def test_hydrate_malformed_snapshot_fails_closed_and_pauses() -> None:
    class _CorruptStore:
        async def persist(self, task_id: str, snapshot: dict[str, Any]) -> None:  # pragma: no cover
            raise AssertionError("not used")

        async def load(self, task_id: str) -> dict[str, Any] | None:
            return {"msgs": 5}  # missing task_id — malformed

        async def delete(self, task_id: str) -> bool:
            return False

    gov = Governor(_budget(), store=_CorruptStore())
    # A malformed durable record must not crash the hub, and must NOT fall open to
    # zero — it fails CLOSED (task paused), assume at-risk (fleet-bus #106).
    assert await gov.hydrate("t-1") is False
    assert gov.is_paused("t-1") is True


async def test_hydrate_malformed_token_snapshot_fails_closed_and_labels_at_risk() -> None:
    class _CorruptStore:
        async def persist(self, task_id: str, snapshot: dict[str, Any]) -> None:  # pragma: no cover
            raise AssertionError("not used")

        async def load(self, task_id: str) -> dict[str, Any] | None:
            return {"task_id": task_id, "msgs": 2, "tokens": ["not", "a", "dict"]}

        async def delete(self, task_id: str) -> bool:
            return False

    from fleet_orchestrator.costfeed import CostLedger

    gov = Governor(_budget(), ledger=CostLedger(), store=_CorruptStore())
    assert await gov.hydrate("t-1") is False
    breach = gov.breach_for("t-1")
    assert breach is not None
    assert breach.kind is BreachKind.AT_RISK
    assert "durable governor state unavailable/corrupt" in breach.reason
    assert "msgs" not in breach.reason


@pytest.mark.parametrize(
    "snapshot",
    [
        {"task_id": "t-1", "msgs": "0", "tokens": None},
        {"task_id": "t-1", "msgs": True, "tokens": None},
        {"task_id": "t-1", "msgs": 1.5, "tokens": None},
        {"task_id": "t-1", "msgs": 0, "cap_override": "7", "tokens": None},
        {"task_id": "t-1", "msgs": 0, "cap_override": True, "tokens": None},
        {
            "task_id": "t-1",
            "msgs": 0,
            "paused": True,
            "breach_kind": "msgs",
            "breach_observed": "4",
            "breach_cap": 3,
            "tokens": None,
        },
        {
            "task_id": "t-1",
            "msgs": 0,
            "paused": True,
            "breach_kind": "msgs",
            "breach_observed": True,
            "breach_cap": 3,
            "tokens": None,
        },
        {
            "task_id": "t-1",
            "msgs": 0,
            "paused": True,
            "breach_kind": "msgs",
            "breach_observed": 4,
            "breach_cap": 3.5,
            "tokens": None,
        },
    ],
)
async def test_hydrate_wrong_typed_governor_numeric_snapshot_fails_closed_without_partial_mutation(
    snapshot: dict[str, Any],
) -> None:
    from fleet_orchestrator.costfeed import CostLedger

    ledger = CostLedger()
    ledger.restore_task("t-1", {"tokens": 100, "seen_turns": [["bilby", "turn-a"]]})
    gov = Governor(_budget(), ledger=ledger, store=_SnapshotStore(snapshot))
    gov.note_message("t-1")

    assert await gov.hydrate("t-1") is False

    breach = gov.breach_for("t-1")
    assert breach is not None
    assert breach.kind is BreachKind.AT_RISK
    assert gov.snapshot("t-1")["msgs"] == 1
    assert ledger.task_total("t-1") == 100
    assert ledger.task_snapshot("t-1")["seen_turns"] == [["bilby", "turn-a"]]


@pytest.mark.parametrize(
    "token_snapshot",
    [
        {"tokens": "900", "seen_turns": []},
        {"tokens": True, "seen_turns": []},
        {"tokens": 1.5, "seen_turns": []},
        {"tokens": 900, "seen_turns": [[123, "turn-b"]]},
        {"tokens": 900, "seen_turns": [["bilby", 123]]},
        {"tokens": 900, "seen_turns": [123]},
    ],
)
async def test_hydrate_wrong_typed_token_snapshot_fails_closed_without_partial_mutation(
    token_snapshot: dict[str, Any],
) -> None:
    from fleet_orchestrator.costfeed import CostLedger

    ledger = CostLedger()
    ledger.restore_task("t-1", {"tokens": 100, "seen_turns": [["bilby", "turn-a"]]})
    gov = Governor(
        _budget(),
        ledger=ledger,
        store=_SnapshotStore({"task_id": "t-1", "msgs": 0, "tokens": token_snapshot}),
    )
    gov.note_message("t-1")

    assert await gov.hydrate("t-1") is False

    breach = gov.breach_for("t-1")
    assert breach is not None
    assert breach.kind is BreachKind.AT_RISK
    assert gov.snapshot("t-1")["msgs"] == 1
    assert ledger.task_total("t-1") == 100
    assert ledger.task_snapshot("t-1")["seen_turns"] == [["bilby", "turn-a"]]


async def test_hydrate_missing_snapshot_does_not_fail_closed() -> None:
    # A NONE snapshot (task never persisted) is the normal fresh-start signal, NOT
    # a failure: hydrate returns False WITHOUT pausing — only real read errors /
    # corrupt records fail closed.
    store = _FakeStore()
    gov = Governor(_budget(), store=store)
    assert await gov.hydrate("t-fresh") is False
    assert gov.is_paused("t-fresh") is False  # fresh task is not paused


def test_fake_store_satisfies_the_protocol() -> None:
    assert isinstance(_FakeStore(), GovernorStore)


# ---- Token-spend ledger survives a crash near the cap (fleet-bus #106) -------


async def test_snapshot_includes_token_ledger_and_restore_resumes_spend() -> None:
    from fleet_orchestrator.costfeed import CostLedger

    ledger = CostLedger()
    # Seed the ledger directly via its task snapshot round-trip (the observe path
    # is covered in the costfeed tests; here we assert the governor carries it).
    seed = {"tokens": 900, "seen_turns": [["bilby", "turn-a"], ["bilby", "turn-b"]]}
    ledger.restore_task("t-1", seed)
    gov = Governor(Budget(max_msgs=3, max_tokens=1000), ledger=ledger)
    snap = gov.snapshot("t-1")
    assert snap["tokens"] == seed

    # A FRESH governor+ledger (restarted hub) resumes the spend, not zero — so a
    # crash near the cap does not hand the task a whole second token budget.
    fresh_ledger = CostLedger()
    fresh = Governor(Budget(max_msgs=3, max_tokens=1000), ledger=fresh_ledger)
    fresh.restore(snap)
    assert fresh_ledger.task_total("t-1") == 900  # spend resumed, NOT reset


async def test_restore_without_ledger_tolerates_token_field() -> None:
    # A snapshot carrying a token sub-snapshot restored into a governor with NO
    # ledger wired (degraded, message-cap-only) must not crash — the token field
    # is simply ignored.
    gov = Governor(_budget())  # no ledger
    snap = {
        "task_id": "t-1",
        "msgs": 2,
        "tokens": {"tokens": 500, "seen_turns": ["x"]},
    }
    assert gov.restore(snap) is True
    assert gov.snapshot("t-1")["msgs"] == 2


# ---- The env flag itself ----------------------------------------------------


def test_durable_flag_defaults_off(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("FLEET_GOVERNOR_DURABLE", raising=False)
    assert durable_enabled_from_env() is False


def test_durable_flag_reads_truthy(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("FLEET_GOVERNOR_DURABLE", "true")
    assert durable_enabled_from_env() is True


def test_durable_flag_rejects_typo(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("FLEET_GOVERNOR_DURABLE", "flase")
    with pytest.raises(ValueError, match="FLEET_GOVERNOR_DURABLE"):
        durable_enabled_from_env()


# ---- FleetKVGovernorStore against a REAL nats-server -js (server CAS) --------
# The durable substrate: JetStream KV with server-authoritative revision-CAS.
# These run against a real embedded server (skipped if nats-server is absent) —
# a mock cannot fake the server-assigned revision the whole feature rebuilds on.

_BUCKET = "fleet-governor-state"


@requires_nats_server
async def test_fleetkv_store_persist_then_load_roundtrips(js_bus: FleetBus) -> None:
    store = await open_governor_store(js_bus)
    snap: dict[str, Any] = {
        "task_id": "t-1",
        "msgs": 7,
        "paused": False,
        "cap_override": None,
        "tokens": None,
    }
    await store.persist("t-1", snap)
    loaded = await store.load("t-1")
    assert loaded == snap  # JSON round-trip preserves the snapshot exactly


@requires_nats_server
async def test_fleetkv_store_load_missing_is_none(js_bus: FleetBus) -> None:
    store = await open_governor_store(js_bus)
    assert await store.load("t-never-persisted") is None


@requires_nats_server
async def test_governor_over_fleetkv_survives_a_simulated_restart(
    js_bus: FleetBus, jetstream_server: str
) -> None:
    # crash+restart resume from the KV: a "crashed" Governor over one bus persists
    # its counters; a FRESH Governor over a SECOND bus connection to the SAME
    # server+bucket hydrates and RESUMES the counter (msgs continues, not reset).
    crashed_store = await open_governor_store(js_bus)
    crashed = Governor(_budget(), store=crashed_store)
    crashed.note_message("t-1")
    crashed.note_message("t-1")
    assert await crashed.persist_state("t-1") is True

    # Restart: a brand-new store over a SECOND connection to the same server. The
    # bucket already exists (the crashed hub created it), so the restarted hub
    # BINDS it via open_kv rather than re-creating — mirroring the reference
    # create-on-A / open-on-B pattern (a second create_kv would collide on the
    # incarnation-stamped stream config). The store is the SAME class either way.
    other = BusConfig(url=jetstream_server, agent_id="pytest-orch-js-2", env="dev")
    async with FleetBus(other) as bus_b:
        restarted_store = FleetKVGovernorStore(await open_kv(bus_b, _BUCKET))
        restarted = Governor(_budget(), store=restarted_store)
        assert restarted.snapshot("t-1")["msgs"] == 0  # in-memory starts at zero
        assert await restarted.hydrate("t-1") is True
        assert restarted.snapshot("t-1")["msgs"] == 2  # resumed from the KV, NOT reset


async def test_naive_client_side_cas_double_wins_under_barriered_race() -> None:
    """CONTROL: a naive client-side CAS lets BOTH writers win (a lost update).

    The revert→fail discrimination proof: models the process-local version scheme
    the server-CAS test must exclude. Under a barriered race where both writers
    read the SAME revision BEFORE either writes, this double-wins — exactly the bug
    a real server-side CAS prevents. The real-KV test below runs the IDENTICAL
    barrier and must NOT double-win, so a pass there is server atomicity, not
    serialization luck.
    """
    naive = _NaiveClientSideCAS(revision=1)
    rev = naive.revision  # both writers captured the SAME revision before racing
    results = await _run_barriered_race(
        lambda: naive.update(b"from-a", expected_revision=rev),
        lambda: naive.update(b"from-b", expected_revision=rev),
    )
    winners = [r for r in results if isinstance(r, int)]
    assert len(winners) == 2, f"naive CAS should double-win, got {results!r}"


@requires_nats_server
async def test_concurrent_hub_persists_are_cas_arbitrated_exactly_one_winner(
    js_bus: FleetBus, jetstream_server: str
) -> None:
    """THE discrimination test: two hubs' concurrent CAS persists — one winner.

    Two FleetKVGovernorStore instances over two SEPARATE bus connections to the
    same server+bucket, both seeded to the SAME server revision for a key (both
    load the same task so both cache the same revision), then both persist (a CAS
    ``update``) concurrently under a barrier. Server-side CAS yields EXACTLY ONE
    winner + one StaleRevisionError. The naive control above double-wins under the
    identical barrier, so this pass is server atomicity, not luck — a process-local
    version scheme would double-win and FAIL this assertion.
    """
    # store_a creates the bucket + seeds the key (caching revision 1). store_b
    # BINDS the same bucket over a second connection (open_kv, not a second
    # create_kv — that would collide on the incarnation-stamped stream config) and
    # loads the key, caching the SAME revision 1. So both stores race an update()
    # against the identical expected_revision — the barrier condition.
    store_a = await open_governor_store(js_bus)
    await store_a.persist("t-race", {"task_id": "t-race", "msgs": 0})  # create → caches rev 1

    other = BusConfig(url=jetstream_server, agent_id="pytest-orch-js-race", env="dev")
    async with FleetBus(other) as bus_b:
        store_b = FleetKVGovernorStore(await open_kv(bus_b, _BUCKET))
        await store_b.load("t-race")  # caches the SAME revision 1

        results = await _run_barriered_race(
            lambda: store_a.persist("t-race", {"task_id": "t-race", "msgs": 1}),
            lambda: store_b.persist("t-race", {"task_id": "t-race", "msgs": 2}),
        )
        winners = [r for r in results if r is None]  # persist returns None on success
        losers = [r for r in results if isinstance(r, StaleRevisionError)]
        unexpected = [r for r in results if r is not None and not isinstance(r, StaleRevisionError)]
        assert unexpected == [], f"unexpected results: {unexpected!r}"
        assert len(winners) == 1, f"expected exactly 1 CAS winner, got {results!r}"
        assert len(losers) == 1, f"expected exactly 1 StaleRevisionError loser, got {results!r}"


@requires_nats_server
async def test_fleetkv_store_load_corrupt_record_raises(js_bus: FleetBus) -> None:
    # A non-JSON value in the key must fail the load CLOSED (raise), NOT return
    # None — so the governor's hydrate pauses (assume at-risk) rather than zeroing
    # the sole runaway terminator on a corrupt record. Write raw bytes via a
    # FleetKV.put handle over the same bucket, then load through the store.
    kv = await create_kv(js_bus, _BUCKET)
    await kv.put("t-corrupt", b"not json")
    store = FleetKVGovernorStore(kv)
    with pytest.raises(ValueError, match="corrupt governor snapshot"):
        await store.load("t-corrupt")


# ---- barriered-race harness (inlined from fleet-nats' test_jetstream) -------


class _NaiveClientSideCAS:
    """A DELIBERATELY WRONG CAS: read-compare-write in the client, no atomicity.

    Models the process-local version scheme the server-CAS test must exclude.
    Under a concurrent race where both writers read the same revision BEFORE either
    writes, this lets BOTH "win" (a lost update). Used by the control test to PROVE
    the race harness discriminates.
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
    write_a: Callable[[], Awaitable[object]],
    write_b: Callable[[], Awaitable[object]],
) -> list[object]:
    """Run two pre-bound calls that hold at a barrier, then fire together.

    The barrier guarantees both writers reach their write at the same instant
    AFTER both have been scheduled — the condition that separates a real server CAS
    (one winner) from a naive client compare (both win). Returns [result_a,
    result_b]; a StaleRevisionError is returned (not raised) so the caller can
    count winners vs losers.
    """
    barrier = asyncio.Event()
    ready = asyncio.Event()
    pending = {"n": 2}

    async def writer(write: Callable[[], Awaitable[object]]) -> object:
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
