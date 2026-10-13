# C5 — Ordering & Retry Semantics (honest contract)

**Status:** ratified. **Issue:** #114 (epic #109).
**Ratified decision (Rico, 2026-07-10):** the issue #114 decision comment

> Honest ordering + retry semantics — per-`(channel, task)` ordering ONLY (no
> global ordering promise); at-least-once + idempotent consumers on JetStream
> lanes; at-**most**-once stated plainly everywhere else; every retry path names
> its idempotency key; overclaiming docstrings corrected. No amendments.

This contract is the single source of truth for what the fleet-bus transport
promises about **message ordering** and **redelivery/retry semantics**. It exists
because the codebase carried docstrings claiming "the bus is at-least-once" — an
overclaim the deployed transport does not honor today. C5 replaces those claims
with the truth and pins the rule that keeps future docstrings honest.

---

## (a) Ordering promise — per-`(channel, task)` ONLY

The bus promises ordering **only within a single `(channel, task)` pair**: two
messages published to the same channel for the same task, by the same producer,
over the same live connection, are delivered to a given subscriber in publish
order (this is core NATS per-subject, per-publisher FIFO).

The bus makes **NO** ordering promise beyond that scope. Explicitly disclaimed:

- **No global ordering.** Messages across different channels or different tasks
  have no defined relative order.
- **No cross-task ordering.** Task A's `done` and task B's `done` may arrive in
  either order.
- **No cross-producer ordering** within a channel/task — two producers racing
  the same lane are not serialized by the bus.
- **No ordering across a reconnect gap.** A reconnect is a new connection; the
  ordering guarantee is per live connection only.

Consumers that need a total order derive it from **k-board Postgres** (the handoff
source of truth), never from bus arrival order. NATS delivers; it is not the
record.

---

## (b) Delivery semantics TODAY — at-**most**-once

**JetStream is enabled NOWHERE in the fleet today.** Every lane runs on core
NATS. Therefore the transport is **at-most-once, fleet-wide**:

- **No backlog. A message published while its subscriber is down is LOST.** Core
  NATS is fire-and-forget: there is no store, no replay, no redelivery. A
  subscriber that is offline when a message is published never sees that message.
- The transport delivers a message **zero or one** times — never more, on its own.

**But duplicates still genuinely arise** — from **application-level** behavior,
not transport redelivery:

- **App-level emitter retries.** A producer that republishes after a failed or
  uncertain publish (e.g. the intake poller re-emitting `created` on the next
  tick) sends a second copy.
- **Reconnect re-sends.** An app that republishes across a reconnect gap (because
  the first publish may have been dropped mid-flight) sends a second copy.

Because those duplicates are real, **consumers must be idempotent**. The dedup
guards in the codebase are correct and stay. What changed is the *attribution*:
duplicates come from app-level retries and reconnect re-sends, **not** from
JetStream/transport redelivery (which does not exist today). Every consumer that
dedups keys on an explicit idempotency key — see the registry in section (d).

---

## (c) Future JetStream rule — composes with the C3 plane/QoS table

When (and only when) a lane adopts JetStream, its delivery semantics become
**at-least-once**: JetStream stores and *redelivers* until the consumer acks, so
the same envelope can legitimately be seen more than once at the transport level.

**Rule:** any lane that adopts JetStream **MUST** have an idempotent consumer with
a named idempotency key before it ships. At-least-once without an idempotent
consumer is a double-execution bug.

This rule composes with the ratified **C3 plane/QoS table** (issue #112), under
which every lane declares four fields at birth — **plane · delivery goal ·
ordering scope · retention**. C5 governs the *delivery-goal* choice: a lane whose
delivery goal is "at-most-once ok" stays on core NATS (accept loss, cheap); a lane
whose delivery goal is "needs redelivery" adopts JetStream (at-least-once,
durable) and is only valid paired with a registered idempotency key here. C5 does
not introduce a separate lane taxonomy — it is the delivery-goal half of C3's
per-lane declaration.

JetStream remains **infra, invisible to agents**: the agent-facing API stays core
NATS. Adopting JetStream on a lane changes durability/redelivery, not the surfaced
API.

---

## (d) Idempotency-key registry

Every retry / re-send / redelivery path in the transport, and the key that makes
it safe against duplicates. (Today all "redelivery" triggers are app-level; the
JetStream rule of section (c) will later make transport redelivery a fourth
trigger — the same keys below cover it.)

Honesty caveat: two rows have **no** idempotency key on purpose. *Offline
redispatch* is non-idempotent by design (it exists to re-run a worker). *Hub
`_finish_task`* holds a failed terminal write open but cannot itself force the
retry — a documented **GAP** (#114), not a satisfied key. Both are named as such
below rather than papered over, per the section (e) honesty rule.

| Path | Location | Trigger (today = app-level) | Idempotency key |
|---|---|---|---|
| Bus reconnect | `packages/fleet-nats/src/fleet_nats/bus.py` (nats-py auto-reconnect, `config.py` `max_reconnect_attempts` / `reconnect_time_wait`) | TCP disconnect → client reconnects. The bus performs no transport-level store-and-replay/redelivery; nats-py (>=2.15) does keep a client-side outbound *pending* buffer that it flushes on reconnect, so a message enqueued just before the drop may be sent post-reconnect — a duplicate here is still an *app*/client re-send across the gap, not a subscriber-side transport redelivery | none at bus layer; safety is downstream (subscriber `Envelope.id`) |
| Intake `created` re-publish | `intake_poller.py` (`_publish_created`; re-emit next tick if card still `todo`) | Publish failed / open never completed → next poll tick re-emits `created` | `task_id` — k-board status is the durable ledger; `start_task` dedups a tracked task |
| Hub `start_task` dedup | `loop.py` `start_task` (+ synchronous key reservation before the first await) | Duplicate `created` intake for an already-coordinated task | `task_id` (in-memory `self._tasks`; key reserved before any await to close the TOCTOU) |
| Hub `_finish_task` persist-hold (see GAP note) | `loop.py` `_finish_task` (kept-tracked on failure; dup no-op on untracked) | Durable `done` write fails → task kept tracked (subs live) | `task_id` (tracked-map presence) makes the persist idempotent-on-retry: the CAS write is idempotent, and a second `done` after a SUCCESSFUL finish finds the task untracked → no-op. **GAP (recorded on #114):** keeping subscriptions live does NOT itself produce a retry — the shipped `done` producer (`fleet-subscriber/adapter.py` `send`) emits exactly ONE `done` and has no re-emit/reconnect retry loop. So absent a second, independent agent completion, a persist failure can leave the card `running` indefinitely. Closing this needs an explicit orchestrator-side retry/reconciliation of failed terminal writes (not shipped) — at-most-once transport does not re-deliver the `done` for us |
| k-board CAS refetch-retry | `kboard.py` (retry loop, budget `_MAX_CAS_RETRIES`) | 409/412 stale-version conflict on PATCH → refetch version, retry | `expected_sync_version` (optimistic CAS token) — pure HTTP CAS, not a bus concern |
| Token-cost ledger dedup | `costfeed.py` `CostLedger.observe`; wired `behaviors.py` `watch_token_costs` | Duplicate `token_cost` envelope (app emitter retry / reconnect re-send) | `(subjects.slug(agent_id), task_id, turn_id)` — agent id is slug-normalized to match the routing lanes; bounded LRU; a re-seen turn is a logged no-op, summed exactly once |
| Governor resume dedup | `governor.py` `resume` | Duplicate resume control frame | `task_id` in `self._paused` — **plain resume is idempotent** (resume of a non-paused task is a `False` no-op). **SHARP EDGE: the `budget_msgs_add` path is NOT idempotent** — each duplicate re-adds the cap bump (N→2N→3N…) while still returning `False`, so a budget-carrying resume must be delivered exactly once (deliberate human steer, not an auto-retried frame) |
| Subscriber dispatch/relay/wake dedup | `dedup.py` (`DedupGuard`); wired in `adapter.py` at connect | Duplicate dispatch/relay/wake envelope (retry ABOVE the bus — orchestrator wake/redispatch — or reconnect re-send) | `Envelope.id` (caller-supplied, unique; bounded LRU, `FLEET_DEDUP_WINDOW`). Backstop: `task_id` at the k-board record |
| Offline redispatch | `behaviors.py` `redispatch` | Silent worker → re-dispatch | **No dedup key — NON-idempotent by design.** Each redispatch mints a **FRESH** `Envelope.id` (`behaviors.py` `_dispatch` → `_envelope` → `new_id`), and the subscriber only dedups on exact `Envelope.id`, so two redispatch attempts are two distinct turns that both run. This is intentional: the worker is *meant* to run again (the original evidently never ran or died), so idempotency would defeat the recovery. What bounds duplicate execution is not a key but (a) the offline-detector firing the rung at most once per silence window and (b) `task_id` reconciliation at the k-board record after the fact — a genuine double-run is visible and correctable there, not prevented |
| Canary poll retry | `canary.py` (poll loop) | Transient `KboardError` on `get_task` → keep polling until deadline | `task_id` — read-only status poll, no publish, no duplicate risk |
| Trace sink | `trace.py` | (no retry — goes inert `_degraded=True` after first sink failure) | N/A — does not retry |

---

## (e) Docstring honesty rule

**No docstring, comment, or doc may promise delivery semantics the deployed
transport does not provide.**

- Do **not** write "the bus is at-least-once" while JetStream is enabled nowhere.
  Today the transport is at-most-once. Attribute duplicates to their real source —
  **app-level emitter retries and reconnect re-sends** — not transport
  redelivery.
- State plainly, where relevant, that a message published while a subscriber is
  down is **lost** (no backlog).
- When a lane adopts JetStream, then and only then may its consumer's docstring
  say "at-least-once" — and it must, in the same breath, name the idempotency key
  (section d) that makes it safe.
- Every retry/dedup path's docstring must name its idempotency key.

The corrections landed under #114 brought the existing docstrings into line with
this rule; the registry above is the running inventory to keep them honest.
