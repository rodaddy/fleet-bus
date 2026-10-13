"""The back-channel governor + single-narrator hooks (#13, Phase 5).

Two coupled controls, both deterministic orchestrator control-flow:

1. **Hard per-task budget.** Every coordination envelope the orchestrator sees
   on a task counts against ``FLEET_BUDGET_MSGS``; the task's token spend (the
   #27 feed via :class:`~fleet_orchestrator.costfeed.CostLedger`) counts
   against ``FLEET_BUDGET_TOKENS``. Exceeding either cap is a case-statement
   outcome, not advisory logging: the governor reports a :class:`Breach`
   exactly once, the task is PAUSED (further dispatch/relay/wake gated to
   NOOP), and the pause is surfaced — a narration beat plus a durable k-board
   event. Resume is an explicit human act (see the runbook), never automatic.

2. **Relay-loop reliability control.** Missing-handoff relay edges are tracked
   per task. Three rapid alternating edges for the same worker pair pause the
   task before the third relay is published. Envelope ids are retained in a
   bounded LRU for idempotency; resume, non-relay progress, and task completion
   reset the applicable run state.

3. **One narrator.** Only the manager surfaces coordination to Discord. The
   :class:`Narrator` renders deterministic TEMPLATED beat summaries —
   state/event/action words and ids only, never envelope payload prose — and
   publishes them on the MANAGER's own inbox lane, so they reach Discord
   through her gateway path (her adapter → her post). No worker-side code path
   exists here; the renderer is manager-only by construction.

The five-state pure machine is NOT modified: the pause gate lives beside it,
exactly as ``persist_transition`` does. Deterministic by construction — relay
timestamps are supplied by the caller, with no governor-owned clock or
randomness; caps, thresholds, and counters are fixed values.

Cap defaults are conservative guesses on purpose: the REAL numbers come from
observed traffic once the dashboard (#14) unparks — measure-first is honored by
making both caps env-tunable.
"""

from __future__ import annotations

import os
from collections import OrderedDict
from collections.abc import Callable
from dataclasses import dataclass
from enum import StrEnum
from typing import TYPE_CHECKING, Any, Protocol

from fleet_nats import NARRATION_KIND, Envelope, subjects
from fleet_nats.tracing import traced
from loguru import logger

from fleet_orchestrator.costfeed import CostLedger
from fleet_orchestrator.state import CoordinationSession
from fleet_orchestrator.transitions import Action, ActionKind

if TYPE_CHECKING:
    from fleet_orchestrator.governor_store import GovernorStore

_ENV_MSGS = "FLEET_BUDGET_MSGS"
_ENV_TOKENS = "FLEET_BUDGET_TOKENS"
# Conservative defaults — tune from observed traffic (#14), via env, per deploy.
_DEFAULT_MSGS = 200
_DEFAULT_TOKENS = 150_000
_RELAY_LOOP_THRESHOLD = 3
_RELAY_LOOP_WINDOW_S = 5.0
_RELAY_ENVELOPE_LRU_SIZE = 4096


def _require_snapshot_int(field: str, value: Any) -> int:
    """Return an exact non-bool integer from a durable governor snapshot."""
    if type(value) is not int:
        raise TypeError(f"restore: {field} must be an int, got {type(value).__name__}")
    return value


def _require_optional_snapshot_int(field: str, value: Any) -> int | None:
    """Return an optional exact non-bool integer from a durable governor snapshot."""
    if value is None:
        return None
    return _require_snapshot_int(field, value)


def _require_snapshot_str(field: str, value: Any) -> str:
    """Return a string from a durable governor snapshot."""
    if not isinstance(value, str):
        raise TypeError(f"restore: {field} must be a str, got {type(value).__name__}")
    return value


def _int_env(var: str, default: int) -> int:
    """Read a positive-int env var; fail loud on junk (a typo is not a policy)."""
    raw = os.environ.get(var)
    if raw is None:
        return default
    try:
        value = int(raw)
    except ValueError as exc:
        raise ValueError(f"{var} must be an integer, got {raw!r}") from exc
    if value <= 0:
        raise ValueError(f"{var} must be positive, got {value}")
    return value


@dataclass(frozen=True)
class Budget:
    """The per-task back-channel caps.

    Attributes:
        max_msgs: Coordination envelopes allowed per task before breach.
        max_tokens: Token spend (from the #27 feed) allowed per task.
    """

    max_msgs: int = _DEFAULT_MSGS
    max_tokens: int = _DEFAULT_TOKENS

    def __post_init__(self) -> None:
        if self.max_msgs <= 0 or self.max_tokens <= 0:
            raise ValueError(f"Budget caps must be positive, got {self}")

    @classmethod
    def from_env(cls) -> Budget:
        """Build caps from ``FLEET_BUDGET_MSGS`` / ``FLEET_BUDGET_TOKENS``."""
        return cls(
            max_msgs=_int_env(_ENV_MSGS, _DEFAULT_MSGS),
            max_tokens=_int_env(_ENV_TOKENS, _DEFAULT_TOKENS),
        )


class BreachKind(StrEnum):
    """Which governor stop condition a task reached."""

    MSGS = "msgs"
    TOKENS = "tokens"
    RELAY_LOOP = "relay_loop"
    AT_RISK = "at_risk"


@dataclass(frozen=True)
class Breach:
    """A single governor stop condition, reported once when reached.

    Attributes:
        task_id: The task stopped by the governor.
        kind: Which stop condition was reached.
        observed: The observed count when the condition was reached.
        cap: The configured budget cap or reliability-control threshold.
    """

    task_id: str
    kind: BreachKind
    observed: int
    cap: int

    @property
    def reason(self) -> str:
        """Board/log-readable one-liner (coordination metadata only)."""
        if self.kind is BreachKind.AT_RISK:
            return "durable governor state unavailable/corrupt; task paused fail-closed"
        if self.kind is BreachKind.RELAY_LOOP:
            return (
                f"relay loop detected: {self.observed} alternating edges "
                f"(threshold {self.cap}) within {_RELAY_LOOP_WINDOW_S}s"
            )
        return f"back-channel budget breach: {self.kind.value} {self.observed} > cap {self.cap}"


@dataclass(frozen=True)
class _RelayRun:
    sender: str
    target: str
    at: float
    count: int


class Governor:
    """Deterministic per-task budget and relay-loop enforcement + pause gate.

    Args:
        budget: The caps to enforce.
        ledger: The #27 token-cost ledger. Optional — when the feed is absent
            the message cap still enforces (degraded, never blind).
        store: The durable counter store (fleet-bus #106). ``None`` (the default)
            is today's byte-for-byte in-memory governor: no persist, no hydrate,
            no KV dependency. A store is wired ONLY when the
            ``FLEET_GOVERNOR_DURABLE`` flag is on — an operator decision gated on
            identity stabilization (T5 + u7/#99), NOT a merge decision. The store
            is a JetStream KV backed
            :class:`~fleet_orchestrator.governor_store.FleetKVGovernorStore`; it
            can also be attached after construction via :meth:`attach_store` (the
            hub opens the KV asynchronously in its ``start()``, after the sync
            ``__init__``). See :mod:`fleet_orchestrator.governor_store`.
    """

    def __init__(
        self,
        budget: Budget,
        *,
        ledger: CostLedger | None = None,
        store: GovernorStore | None = None,
    ) -> None:
        self._budget = budget
        self._ledger = ledger
        self._store = store
        self._msgs: dict[str, int] = {}
        self._paused: dict[str, Breach] = {}
        # Per-task message-cap overrides (a steer's "resume, raise the budget";
        # #59). Empty by default — a task with no override uses the shared cap, so
        # the common path is unchanged. Only ever RAISES the effective cap.
        self._msgs_cap_override: dict[str, int] = {}
        # The FLEET-WIDE kill flag (#107). When armed it gates EVERY task's bus
        # behavior to NOOP in :meth:`gate` — the "stop everything" lever that needs
        # no task_id. Orthogonal to the per-task ``_paused`` map: arming it does not
        # touch per-task pause state, so releasing it restores exactly the prior
        # per-task posture (a task paused over budget stays paused). Armed/released
        # by the out-of-band :class:`~fleet_orchestrator.killswitch.KillSwitch`,
        # never by a bus frame.
        self._all_paused = False
        self._relay_runs: dict[str, _RelayRun] = {}
        self._relay_envelopes: OrderedDict[tuple[str, str], None] = OrderedDict()

    @property
    def durable(self) -> bool:
        """Whether durable counter persistence is wired (fleet-bus #106).

        ``False`` (no store) is today's in-memory behavior. When ``False`` every
        durable method below is a no-op, so the common path is unchanged.
        """
        return self._store is not None

    def attach_store(self, store: GovernorStore) -> None:
        """Attach the durable store after construction (fleet-bus #106).

        The hub builds the :class:`Governor` synchronously in its ``__init__`` but
        the JetStream KV store must be opened asynchronously (over the connected
        bus, in ``start()``). So the hub constructs the governor store-less and
        attaches the opened store here before intake begins. After this,
        :attr:`durable` is ``True`` and persist/hydrate are live.

        Guard: only attaches when no store is already wired — a governor
        constructed WITH a store (the direct-injection path, e.g. tests) is not
        silently overwritten. A second attach is a loud no-op.

        Args:
            store: The opened durable store to wire.
        """
        if self._store is not None:
            logger.warning(
                "orchestrator.governor | attach_store ignored — a store is already wired"
            )
            return
        self._store = store
        logger.warning(
            "orchestrator.governor | DURABLE governor state ENABLED (fleet-bus #106) — counters "
            "persist/resume via JetStream KV CAS. IDENTITY-GATED: only safe once identity is "
            "stable (T5 + u7/#99)."
        )

    @property
    def ledger(self) -> CostLedger | None:
        """The wired #27 cost ledger (``None`` = degraded, message cap only)."""
        return self._ledger

    @traced
    def note_message(self, task_id: str) -> Breach | None:
        """Count one coordination envelope against ``task_id`` and check caps.

        Returns:
            The :class:`Breach` if THIS message tripped a cap (first breach
            only — an already-paused task never re-breaches), else ``None``.
        """
        self._msgs[task_id] = self._msgs.get(task_id, 0) + 1
        return self._check(task_id)

    def check_tokens(self, task_id: str) -> Breach | None:
        """Re-check the token cap (call after the ledger observed new cost)."""
        return self._check(task_id)

    def note_relay(
        self,
        task_id: str,
        envelope_id: str,
        sender: str,
        target: str,
        now: float,
    ) -> Breach | None:
        """Observe one relay edge and pause a rapid three-edge alternating run.

        Envelope ids are idempotent within a bounded LRU. A run advances only
        when the same worker pair reverses direction within five seconds.
        """
        key = (task_id, envelope_id)
        if key in self._relay_envelopes:
            self._relay_envelopes.move_to_end(key)
            return None
        self._relay_envelopes[key] = None
        if len(self._relay_envelopes) > _RELAY_ENVELOPE_LRU_SIZE:
            self._relay_envelopes.popitem(last=False)

        previous = self._relay_runs.get(task_id)
        alternates = (
            previous is not None
            and sender != target
            and sender == previous.target
            and target == previous.sender
            and 0 <= now - previous.at <= _RELAY_LOOP_WINDOW_S
        )
        if alternates and previous is not None:
            count = previous.count + 1
        else:
            count = 1
        self._relay_runs[task_id] = _RelayRun(sender, target, now, count)
        if count < _RELAY_LOOP_THRESHOLD or task_id in self._paused:
            return None
        return self._pause(Breach(task_id, BreachKind.RELAY_LOOP, count, _RELAY_LOOP_THRESHOLD))

    def reset_relay(self, task_id: str) -> None:
        """Clear an in-progress relay run after non-relay progress."""
        self._relay_runs.pop(task_id, None)

    def complete_task(self, task_id: str) -> None:
        """Release governor state retained for a completed task."""
        self._relay_runs.pop(task_id, None)
        self._paused.pop(task_id, None)
        self._msgs.pop(task_id, None)
        self._msgs_cap_override.pop(task_id, None)
        for relay_key in tuple(self._relay_envelopes):
            if relay_key[0] == task_id:
                del self._relay_envelopes[relay_key]
        if self._ledger is not None:
            self._ledger._by_task.pop(task_id, None)
            for seen_key in tuple(self._ledger._seen):
                if seen_key[1] == task_id:
                    del self._ledger._seen[seen_key]

    def _msgs_cap(self, task_id: str) -> int:
        """The effective message cap for a task (a #59 override, else the shared cap)."""
        return self._msgs_cap_override.get(task_id, self._budget.max_msgs)

    def _check(self, task_id: str) -> Breach | None:
        if task_id in self._paused:
            return None  # double-breach never double-pauses
        msgs = self._msgs.get(task_id, 0)
        cap = self._msgs_cap(task_id)
        if msgs > cap:
            return self._pause(Breach(task_id, BreachKind.MSGS, msgs, cap))
        if self._ledger is not None:
            tokens = self._ledger.task_total(task_id)
            if tokens > self._budget.max_tokens:
                return self._pause(
                    Breach(task_id, BreachKind.TOKENS, tokens, self._budget.max_tokens)
                )
        return None

    def _pause(self, breach: Breach) -> Breach:
        self._paused[breach.task_id] = breach
        # Journal checkpoint line: task_id + which cap + the count-vs-cap at the
        # breach, so the pause is diagnosable from the journal alone (Nagatha
        # requirement #3). msgs is the live message counter for this task.
        logger.error(
            "orchestrator.governor | PAUSED task={} kind={} count={} cap={} reason={}",
            breach.task_id,
            breach.kind.value,
            breach.observed,
            breach.cap,
            breach.reason,
        )
        return breach

    def is_paused(self, task_id: str) -> bool:
        """Whether coordination for ``task_id`` is paused over budget."""
        return task_id in self._paused

    def breach_for(self, task_id: str) -> Breach | None:
        """The breach that paused ``task_id``, if any."""
        return self._paused.get(task_id)

    @traced
    def gate(self, action: Action) -> Action:
        """The manager-loop gate: a paused task performs NO bus behavior.

        Args:
            action: The action the pure machine decided.

        Returns:
            The action unchanged when the task is not paused; otherwise a
            ``NOOP`` carrying the pause reason — the session snapshot is kept
            (state persistence is unaffected; only bus behavior stops).
        """
        # COMPLETE / NOOP pass through unconditionally: they perform no bus
        # behavior, and a finished task must be allowed to finish — gating a
        # COMPLETE would only mislabel the terminal transition as a NOOP (fable
        # cross-review, #38). This holds even under the fleet-wide kill: the point
        # is to stop NEW work (dispatch/relay/wake), not to strand a task that is
        # already terminating.
        if action.kind in (ActionKind.NOOP, ActionKind.COMPLETE):
            return action
        # FLEET-WIDE kill (#107) is checked BEFORE the per-task pause: it is the
        # "stop everything" lever and needs no task_id. When armed, EVERY non-
        # terminal action gates to NOOP regardless of per-task state.
        if self._all_paused:
            logger.warning(
                "orchestrator.governor | gated action={} task={} (FLEET KILL SWITCH armed)",
                action.kind.value,
                action.session.task_id,
            )
            return Action(
                kind=ActionKind.NOOP,
                session=action.session,
                reason="fleet kill switch armed; operator release required",
            )
        breach = self._paused.get(action.session.task_id)
        if breach is None:
            return action
        logger.warning(
            "orchestrator.governor | gated action={} task={} (paused: {})",
            action.kind.value,
            action.session.task_id,
            breach.reason,
        )
        return Action(
            kind=ActionKind.NOOP,
            session=action.session,
            reason=f"paused by governor ({breach.kind.value}); human resume required",
        )

    @traced
    def pause(self, task_id: str) -> bool:
        """Explicit external pause (a control-plane ``pause`` op; #59).

        Symmetric to :meth:`resume`: gate a task's bus behavior on operator
        demand without a budget breach. Records a synthetic MSGS breach at the
        current counter so :meth:`gate` / :meth:`is_paused` treat it exactly like
        a budget pause. Idempotent — pausing an already-paused task is a no-op.

        Returns:
            ``True`` if this call paused the task, ``False`` if it was already
            paused.
        """
        if task_id in self._paused:
            return False
        observed = self._msgs.get(task_id, 0)
        self._paused[task_id] = Breach(task_id, BreachKind.MSGS, observed, self._msgs_cap(task_id))
        logger.warning("orchestrator.governor | PAUSED task={} (external control op)", task_id)
        return True

    @traced
    def resume(self, task_id: str, *, budget_msgs_add: int | None = None) -> bool:
        """Explicit human resume: clear the pause and reset the message counter.

        Idempotent by task pause state: a duplicate resume control frame
        (app-level retry / reconnect re-send; transport is at-most-once, C5) for
        an already-resumed task is a no-op that returns ``False``. This includes
        budget-carrying resumes: the per-task cap is raised only when this call
        actually clears a pause (Nagatha requirement #1, fleet-bus #59).

        Args:
            task_id: The task to unpause.
            budget_msgs_add: Optional BOUNDED per-task cap increase — add exactly
                this many messages to THIS task's effective cap (a steer's
                "resume, and give it N more"; Nagatha requirement #2). Bounded and
                per-task by construction: it never touches the shared cap or any
                other task, and it can only ever RAISE this task's cap. Must be a
                positive int; a non-positive add is a caller error. The bump is
                secondary — it never blocks the resume, which the counter reset
                alone accomplishes.

        Returns:
            ``True`` if the task was paused (and is now resumed), else ``False``.

        Raises:
            ValueError: If ``budget_msgs_add`` is given and not positive.
        """
        if budget_msgs_add is not None:
            if budget_msgs_add <= 0:
                raise ValueError(f"resume: budget_msgs_add must be positive, got {budget_msgs_add}")
        breach = self._paused.pop(task_id, None)
        if breach is None:
            return False
        self.reset_relay(task_id)
        if budget_msgs_add is not None:
            current = self._msgs_cap(task_id)
            raised = current + budget_msgs_add
            self._msgs_cap_override[task_id] = raised
            logger.info(
                "orchestrator.governor | msg cap raised task={} {} +{} -> {}",
                task_id,
                current,
                budget_msgs_add,
                raised,
            )
        self._msgs[task_id] = 0
        logger.info("orchestrator.governor | RESUMED task={} (was: {})", task_id, breach.reason)
        return True

    def pause_all(self) -> bool:
        """Arm the FLEET-WIDE kill: gate EVERY task's bus behavior to NOOP (#107).

        The "stop everything" lever — no task_id needed. Idempotent: arming an
        already-armed kill is a no-op returning ``False``. Does NOT touch the
        per-task ``_paused`` map, so :meth:`resume_all` restores exactly the prior
        posture. Armed by the out-of-band
        :class:`~fleet_orchestrator.killswitch.KillSwitch`, which watches a local
        file on its own timer — so this lands even while the bus is saturated.

        Returns:
            ``True`` if this call armed the kill, ``False`` if it was already armed.
        """
        if self._all_paused:
            return False
        self._all_paused = True
        logger.error("orchestrator.governor | FLEET KILL SWITCH ARMED — all tasks gated to NOOP")
        return True

    def resume_all(self) -> bool:
        """Release the FLEET-WIDE kill (#107): tasks resume their prior posture.

        Idempotent: releasing an unarmed kill is a no-op returning ``False``.
        Releasing does NOT clear any per-task budget pause — a task paused over
        budget before/while the kill was armed stays paused and still needs its
        own resume (the two levers are orthogonal by construction).

        Returns:
            ``True`` if this call released an armed kill, else ``False``.
        """
        if not self._all_paused:
            return False
        self._all_paused = False
        logger.warning(
            "orchestrator.governor | FLEET KILL SWITCH RELEASED — per-task posture restored"
        )
        return True

    def is_all_paused(self) -> bool:
        """Whether the fleet-wide kill switch is currently armed (#107)."""
        return self._all_paused

    # ---- Durable counter persistence (fleet-bus #106, FLAG-GATED) -----------
    # These are no-ops when no store is wired (durable flag OFF = today's
    # behavior). The snapshot/restore pair is pure in-memory (JSON-able) counter
    # serialization; persist_state/hydrate are the async JetStream KV round-trips.

    def snapshot(self, task_id: str) -> dict[str, Any]:
        """Serialize a task's governor counters to a JSON-able dict (pure).

        The durable twin of the in-memory per-task state: the message counter, any
        pause breach, any raised cap override, and the task's token-spend ledger
        sub-snapshot (spend + dedup turn keys). Pure and side-effect-free — used
        both to persist (write to the store) and, symmetrically, restored by
        :meth:`restore`. A task with no state yields the zero snapshot (msgs 0, not
        paused, no override, zero tokens), so a snapshot is always well-formed.

        There is no application-layer ``version`` on the snapshot: the
        anti-backward-roll CAS token is the SERVER's KV revision, held by
        :class:`~fleet_orchestrator.governor_store.FleetKVGovernorStore`, not a
        field the governor stamps.
        """
        breach = self._paused.get(task_id)
        return {
            "task_id": task_id,
            "msgs": self._msgs.get(task_id, 0),
            "paused": breach is not None,
            "breach_kind": breach.kind.value if breach is not None else None,
            "breach_observed": breach.observed if breach is not None else None,
            "breach_cap": breach.cap if breach is not None else None,
            "cap_override": self._msgs_cap_override.get(task_id),
            # Token-spend ledger (fleet-bus #106 finding: spend must survive a crash
            # near the cap, else a runaway re-spends a whole budget). ``None`` when
            # no ledger is wired (message-cap-only degraded mode) — restore tolerates.
            "tokens": self._ledger.task_snapshot(task_id) if self._ledger is not None else None,
        }

    def restore(self, snapshot: dict[str, Any]) -> bool:
        """Load a task's counters from a :meth:`snapshot` dict (pure, in-memory).

        The inverse of :meth:`snapshot`. Rebuilds the message counter, the pause
        breach (if any), the cap override, and the token-spend ledger. Idempotent —
        restoring the same snapshot twice yields the same state. This is the RESUME
        primitive: after a restart, hydrate reads the durable snapshot and calls
        this so the counters continue from where the crashed hub left them instead
        of resetting to zero.

        **No anti-backward-roll guard here.** The stale-write guard is NO LONGER at
        this layer: it is enforced by the SERVER's revision-CAS in
        :meth:`~fleet_orchestrator.governor_store.FleetKVGovernorStore.persist`
        (a stale hub's ``update`` is rejected server-side, so a backward-rolling
        snapshot can never be written in the first place). ``restore`` is therefore
        a pure, idempotent apply that ALWAYS applies the snapshot and returns
        ``True`` (the ``-> bool`` return is kept for API stability).

        Args:
            snapshot: A dict produced by :meth:`snapshot`.

        Returns:
            ``True`` — the snapshot is always applied (kept for API stability).

        Raises:
            KeyError: If the snapshot is missing the required ``task_id``/``msgs``
                keys — a malformed durable record is a fail-loud error, not silently
                coerced (the caller decides whether to fall back to zero).
        """
        task_id = _require_snapshot_str("task_id", snapshot["task_id"])
        msgs = _require_snapshot_int("msgs", snapshot["msgs"])
        override = _require_optional_snapshot_int("cap_override", snapshot.get("cap_override"))
        paused = snapshot.get("paused", False)
        if type(paused) is not bool:
            raise TypeError(f"restore: paused must be a bool, got {type(paused).__name__}")
        breach: Breach | None = None
        if paused:
            kind = BreachKind(_require_snapshot_str("breach_kind", snapshot["breach_kind"]))
            breach = Breach(
                task_id=task_id,
                kind=kind,
                observed=_require_snapshot_int("breach_observed", snapshot["breach_observed"]),
                cap=_require_snapshot_int("breach_cap", snapshot["breach_cap"]),
            )
        else:
            _require_optional_snapshot_int("breach_observed", snapshot.get("breach_observed"))
            _require_optional_snapshot_int("breach_cap", snapshot.get("breach_cap"))
        tokens = snapshot.get("tokens")
        if tokens is not None and self._ledger is not None:
            self._ledger.restore_task(task_id, tokens)

        self._msgs[task_id] = msgs
        if override is None:
            self._msgs_cap_override.pop(task_id, None)
        else:
            self._msgs_cap_override[task_id] = override
        if breach is None:
            self._paused.pop(task_id, None)
        else:
            self._paused[task_id] = breach
        logger.info(
            "orchestrator.governor | RESTORED task={} msgs={} paused={}",
            task_id,
            self._msgs[task_id],
            task_id in self._paused,
        )
        return True

    @traced
    async def persist_state(self, task_id: str) -> bool:
        """Durably persist a task's counters to the store (no-op when flag OFF).

        When no store is wired (durable flag OFF) this returns ``False`` without
        touching the network — today's byte-for-byte behavior. When a store IS
        wired, it writes the :meth:`snapshot`.

        **Write failure policy (runaway-terminator counter).** A write failure is
        FAIL-LOUD (WARNING) but NON-FATAL: the in-memory counter stays authoritative
        for this process (the storm still terminates this run — the counters never
        depend on the KV), and we return ``False`` rather than raising up into the
        hot coordination path or halting the hub mid-storm. The only thing a failed
        write costs is *cross-restart* resume, which is exactly the capability
        gated behind identity stabilization anyway. We never swallow silently — the
        WARNING carries the task and the error. A
        :class:`~fleet_nats.StaleRevisionError` means another hub won the CAS race;
        the store has already refreshed its cached revision, so the in-memory
        counter stays authoritative here and the NEXT envelope's persist uses the
        fresh revision. It is caught by the same ``except Exception`` below.

        Returns:
            ``True`` if the snapshot was durably written, ``False`` if durable mode
            is off OR the write failed (with a logged WARNING in the failure case).
        """
        if self._store is None:
            return False
        if not self._has_task_state(task_id):
            logger.debug(
                "orchestrator.governor | durable persist skipped task={} (no retained state)",
                task_id,
            )
            return False
        snapshot = self.snapshot(task_id)
        try:
            await self._store.persist(task_id, snapshot)
        except Exception as exc:  # noqa: BLE001 — fail loud, keep in-memory, never halt mid-storm
            logger.warning(
                "orchestrator.governor | durable persist FAILED task={} (in-memory kept "
                "authoritative; cross-restart resume lost this write): {}",
                task_id,
                exc,
            )
            return False
        logger.info(
            "orchestrator.governor | durable persist task={} msgs={}", task_id, snapshot["msgs"]
        )
        return True

    def _has_task_state(self, task_id: str) -> bool:
        """Whether there is non-empty local state worth persisting for ``task_id``."""
        if task_id in self._msgs or task_id in self._paused or task_id in self._msgs_cap_override:
            return True
        if any(key[0] == task_id for key in self._relay_envelopes):
            return True
        if self._ledger is None:
            return False
        return self._ledger.task_total(task_id) > 0 or any(
            seen_task == task_id for _agent, seen_task, _turn in self._ledger._seen
        )

    async def clear_state(self, task_id: str) -> bool:
        """Clear local and durable state for a completed task.

        Completion makes the task's governor state obsolete. Clear local counters
        first, then delete/tombstone the KV key when durable mode is on so a
        later card reusing the same ``task_id`` starts clean instead of hydrating
        stale pause/spend/revision state.

        Returns:
            ``True`` when cleanup is complete or durable mode is off; ``False`` if
            a durable delete failed (with a logged ERROR).
        """
        self.complete_task(task_id)
        if self._store is None:
            return True
        delete = getattr(self._store, "delete", None)
        if delete is None:
            logger.error(
                "orchestrator.governor | durable cleanup FAILED task={} (store has no delete API)",
                task_id,
            )
            return False
        try:
            await delete(task_id)
        except Exception as exc:  # noqa: BLE001 — cleanup retry is owned by caller
            logger.error(
                "orchestrator.governor | durable cleanup FAILED task={} (local cleared; "
                "durable snapshot may resurrect on reuse/restart): {}",
                task_id,
                exc,
            )
            return False
        return True

    @traced
    async def hydrate(self, task_id: str) -> bool:
        """Resume a task's counters from the store on restart (no-op when flag OFF).

        When no store is wired (durable flag OFF) this returns ``False`` without a
        network call — today's behavior (counters start at zero). When a store IS
        wired, it reads the latest durable snapshot and :meth:`restore`s it, so a
        restarted hub continues the task's counters instead of resetting them — the
        #106 acceptance MECHANISM.

        **Failure policy — FAIL CLOSED (fleet-bus #106).** A store READ failure or a
        MALFORMED durable record does NOT fall open to zero. Zeroing on restart
        would reset the SOLE runaway terminator during a brief KV outage — the
        exact runaway the durable state exists to survive. Instead we treat unknown
        durable state as "assume at-risk": the task is PAUSED (a synthetic breach so
        :meth:`gate` NOOPs its bus behavior) and the error is surfaced LOUD (ERROR),
        so an operator resumes deliberately once the KV is readable again. Never
        silent, never a fatal crash — the hub keeps running, that ONE task holds.

        A missing snapshot (``None`` — task never persisted) is NOT a failure: it is
        the normal "fresh task, start from zero" signal and returns ``False`` without
        pausing. Only an actual read error or a corrupt record fails closed.

        Returns:
            ``True`` if counters were resumed from a durable snapshot, ``False`` if
            durable mode is off, no snapshot exists, or the load failed closed (the
            task is paused and an ERROR is logged in the fail-closed case).
        """
        if self._store is None:
            return False
        try:
            snapshot = await self._store.load(task_id)
        except Exception as exc:  # noqa: BLE001 — fail loud + CLOSED, never silently zero
            logger.error(
                "orchestrator.governor | durable hydrate READ FAILED task={} — FAILING CLOSED "
                "(task PAUSED, assume at-risk; operator resume required once the KV readable): {}",
                task_id,
                exc,
            )
            self._pause_at_risk(task_id)
            return False
        if snapshot is None:
            return False
        try:
            self.restore(snapshot)
        except (KeyError, TypeError, ValueError) as exc:
            logger.error(
                "orchestrator.governor | durable hydrate MALFORMED snapshot task={} — FAILING "
                "CLOSED (task PAUSED, assume at-risk; operator resume required): {}",
                task_id,
                exc,
            )
            self._pause_at_risk(task_id)
            return False
        return True

    def _pause_at_risk(self, task_id: str) -> None:
        """Pause a task whose durable state could not be trusted (fail-closed).

        Records a typed AT_RISK breach so :meth:`gate` / :meth:`is_paused` block
        bus behavior without pretending a normal message or token cap was crossed.
        Idempotent: pausing an already paused task is a no-op (never overwrites a
        real budget breach).
        """
        if task_id in self._paused:
            return
        self._paused[task_id] = Breach(task_id, BreachKind.AT_RISK, 0, 0)


# ---- Narration (single throat) ---------------------------------------------
# NARRATION_KIND is the shared wire constant (fleet_nats.narration): the
# subscriber's mapping recognizes the same value, so emitter and consumer can
# never drift. Re-exported here (and from the package root) for callers.

__all__ = [
    "NARRATION_KIND",
    "Breach",
    "BreachKind",
    "Budget",
    "Governor",
    "Narrator",
    "render_narration",
]


class _PublishBus(Protocol):
    """The one bus capability the narrator needs."""

    async def publish(self, subject: str, envelope: Envelope) -> None:
        """Publish an envelope."""
        ...


# Deterministic beat templates — state/event/action WORDS and ids only. Never
# envelope prose: Discord is treated as leaked, and narration must not become
# a second copy of the coordination content.
_TEMPLATES = {
    "dispatched": "[{task_id}] dispatched to {target} — coordination started.",
    "relayed": "[{task_id}] relay sent to {target} (a handoff was missing).",
    "arbitrated": "[{task_id}] arbitration decided for {target}.",
    "woken": "[{task_id}] {target} nudged (quiet too long).",
    "redispatched": "[{task_id}] redispatched to {target} (still silent).",
    "blocked": "[{task_id}] BLOCKED — {target} offline; see the k-board record.",
    "done": "[{task_id}] done — outcome: {outcome}.",
    "paused": "[{task_id}] PAUSED over budget ({detail}) — human resume required.",
    "at_risk": "[{task_id}] PAUSED fail-closed ({detail}) — human resume required.",
    "relay_loop": (
        "[{task_id}] PAUSED by relay-loop reliability control ({detail}) — human resume required."
    ),
    "circlejerk": "[{task_id}] circlejerk detected — agreements looping, no dissent or fact-pull.",
    "decided": "[{task_id}] decided for {target} (evidenced position).",
    # Task-independent fleet-liveness beats (fleet-bus #78). Not task-scoped —
    # rendered with a synthetic task_id ("fleet") and the agent id as target. The
    # roster sweep (fleet_liveness.FleetLivenessReporter) narrates an idle agent
    # going dark, which the task-scoped offline ladder ("blocked") cannot see.
    "fleet_silent": "[fleet] {target} missing from the bus — silent {detail}.",
    "fleet_present": "[fleet] {target} back on the bus.",
    # Nightly organic canary (fleet-bus #89). Rendered with the synthetic "fleet"
    # task_id; ``target`` is the canary card id and ``detail`` the failure reason
    # (timeout / blocked). Only ever narrated on FAILURE — a green canary is
    # silent, so this beat IS the "narrator headline ONLY on failure" contract.
    "fleet_canary_failed": (
        "[fleet] nightly canary FAILED — card {target} {detail}. See the k-board record."
    ),
}


def render_narration(
    beat: str, *, task_id: str, target: str = "", outcome: str = "", detail: str = ""
) -> str:
    """Render one templated narration line.

    Args:
        beat: One of the template keys (``dispatched``/``relayed``/...).
        task_id: The task the beat belongs to.
        target: The agent id the beat concerns, when applicable.
        outcome: The done outcome word, for ``done``.
        detail: The breach one-liner, for ``paused``.

    Returns:
        The rendered line.

    Raises:
        KeyError: On an unknown beat — narration templates are a closed set;
            an unknown beat is a programming error, not content to improvise.
    """
    return _TEMPLATES[beat].format(task_id=task_id, target=target, outcome=outcome, detail=detail)


class Narrator:
    """Publishes narration beats on the MANAGER's own inbox lane.

    One narrator, one throat: only the manager's identity ever narrates. The
    beat reaches Discord through the manager's gateway (inbox → adapter → her
    own post) — fleet-bus's job ends at the bus.

    Args:
        bus: The connected bus (publish capability only).
        env: Subject env prefix.
        manager: The manager agent id (the ONLY narration target).
        new_id: Message-id factory (injected).
        now: ISO-8601 timestamp factory (injected).
    """

    def __init__(
        self,
        bus: _PublishBus,
        *,
        env: str,
        manager: str,
        new_id: Callable[[], str],
        now: Callable[[], str],
    ) -> None:
        self._bus = bus
        self._env = env
        self._manager = manager
        self._new_id = new_id
        self._now = now

    async def narrate(self, session: CoordinationSession, beat: str, **fields: str) -> None:
        """Render and publish one beat for a session (task_id + channel routing).

        Args:
            session: The task's session.
            beat: The template key.
            **fields: Template fields (``target``/``outcome``/``detail``).
        """
        await self.narrate_task(session.task_id, beat, channel=session.channel, **fields)

    async def narrate_task(
        self, task_id: str, beat: str, *, channel: str | None = None, **fields: str
    ) -> None:
        """Render and publish one beat by task id; a failure logs — narration
        must never take coordination down.

        The session-less entry point: callers observing a lane keyed only by
        task (the #27 cost watch) have no session snapshot to hand over.

        Args:
            task_id: The task the beat belongs to.
            beat: The template key.
            channel: The Discord channel the beat concerns, when known.
            **fields: Template fields (``target``/``outcome``/``detail``).
        """
        text = render_narration(beat, task_id=task_id, **fields)
        subject = subjects.agent_inbox(self._env, self._manager)
        envelope = Envelope.new(
            msg_id=self._new_id(),
            ts=self._now(),
            sender=self._manager,
            kind=NARRATION_KIND,
            task_id=task_id,
            channel=channel,
            payload={"text": text},
        )
        try:
            await self._bus.publish(subject, envelope)
            logger.info(
                "orchestrator.narrate | beat={} task={} target_lane={}",
                beat,
                task_id,
                subject,
            )
        except Exception as exc:  # noqa: BLE001 — narration must degrade loudly, not fatally
            logger.warning(
                "orchestrator.narrate | publish failed beat={} task={}: {}",
                beat,
                task_id,
                exc,
            )
