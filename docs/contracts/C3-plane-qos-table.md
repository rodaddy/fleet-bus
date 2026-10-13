# C3 — Plane / QoS Table (ratified contract)

**Status:** RATIFIED (Rico, 2026-07-10). No amendments.
**Ratification:** the issue #112 decision comment
— *"Yes — ratify table + checklist"*, seeded from T4's traffic matrix, with every
NEW lane required to declare four fields at birth: **plane · delivery goal · ordering
scope · retention**.
**Seed provenance:** `wave1-research/fleetbus-traffic-matrix.md` (complete publish/subscribe
inventory with `file:line` receipts) built at commit `060d71f` — the current `origin/main`
tip these receipts are verified against.
**Companion contracts:** C5 (ordering scope: **per-`(channel, task)` only**). Plane
classification narrative and the app↔token seam live in T4 / issue #119 →
`docs/fleetbus-traffic-matrix.md` (PR
#130). This file is the ratified
**contract table + new-lane checklist**; it does not restate T4's prose and does not
edit `docs/fleetbus-traffic-matrix.md`.

---

## 0. Honest QoS baseline (read before the table)

The transport is **NATS core only**. JetStream is deliberately not surfaced; durability
is an infra concern (`bus.py:8-10`). Therefore, for **every lane today**:

- **Delivery = at-most-once.** No server-side backlog: a message published before its
  subscriber attaches is lost (the reason `watch_worker_status` subscribes *before*
  dispatch, `loop.py:445-451`). The "needs-redelivery" lanes below name a *goal the
  transport does not yet meet* — they are the future-JetStream candidates, not a promise
  the current bus keeps.
- **Retention = none.** No stream, no replay, no dead-letter durability at the transport.
  `agent.dead_letter` is an *application* record published like any other core message; it
  is not a durable queue.
- **Ordering scope = per-`(channel, task)` only** (C5). NATS core preserves per-subject
  publish order from a single publisher on a single connection; it makes **no** cross-subject,
  cross-publisher, or cross-connection ordering guarantee. This is the guarantee ceiling for
  **every** lane: the contract commits to ordering no wider than per-`(channel, task)`.
  A lane's subject key may name a *different* axis (per-agent, per-task, per-env, per-request) —
  those axes span more messages than a single `(channel, task)` and are therefore **not**
  subsets of the cap. The Ordering-scope column below reads `per-(channel, task)` uniformly (the
  contracted ceiling); the actual subject-key axis is described in each row's receipt text. Where
  a subject spans a wider axis, the contract still only guarantees ordering **within** each
  `(channel, task)` slice of that subject — never across the whole per-agent or per-task stream.

"Delivery goal" columns carry a single value per lane: `at-most-once ok` = the lane is correct
as-is on NATS core; `needs redelivery` = the lane *wants* a guarantee the core bus cannot give
and is a JetStream-future candidate. The two are mutually exclusive — a lane is one or the
other, and the **actual** delivery for *every* lane today is at-most-once regardless.

---

## 1. The ratified table (every current lane)

Lanes are the collapsed subject/registry keys. Each row declares the four ratified fields.
`Plane`: **token** = enters an LLM agent's context as a turn (costs tokens); **app** =
consumed by deterministic code only. The token/app boundary is physically clean in code —
only the subscriber's `_forge_turn` crosses into an LLM turn (`adapter.py:454-496`); the
orchestrator is all app-plane.

| # | Lane (registry key) | Subject builder | Plane | Delivery goal | Ordering scope | Retention | Receipt |
|---|---------------------|-----------------|-------|---------------|----------------|-----------|---------|
| 1 | `agent.dispatch[.channel]` | `agent_dispatch` (+`_wildcard`) | **token** | **needs redelivery** (task-start; if lost, task never begins — actual today is at-most-once) | per-`(channel, task)` | none | `subjects.py:47-63`, `behaviors.py:241`, `fleet_send.py:207`, `adapter.py:203,466-496` |
| 2 | `agent.relay` | `agent_relay` | **token** | at-most-once ok | per-`(channel, task)` | none | `subjects.py:71-73`, `behaviors.py:269`, `adapter.py:203` |
| 3 | `agent.wake` | `agent_wake` | **token** | at-most-once ok | per-`(channel, task)` | none | `subjects.py:76-78`, `behaviors.py:292`, `mapping.py:91-99` |
| 4 | `agent.inbox` | `agent_inbox` | **token** | at-most-once ok | per-`(channel, task)` | none | `subjects.py:91-93`, `governor.py:436`, `score.py:611`, `adapter.py:203` |
| 5 | `agent.status` | `agent_status` | **app** | at-most-once ok | per-`(channel, task)` | none | `subjects.py:66-68`, `adapter.py:357-402`, `behaviors.py:614`, `loop.py:566` |
| 6 | `agent.heartbeat` | `agent_heartbeat` (+`_wildcard`) | **app** | at-most-once ok (liveness is inherently lossy; a missed beat is re-emitted next interval) | per-`(channel, task)` | none | subject axis is **per-agent** (`{env}.agent.heartbeat.{agent}`); order guaranteed only within each `(channel, task)` slice. `subjects.py:96-105`, `heartbeat.py:129`, `liveness.py:179` |
| 7 | `agent.token_cost` | `token_cost` (+`_wildcard`) | **app** | at-most-once ok (dedup on `(agent, task, turn)` at consumer) | per-`(channel, task)` | none | subject axis is **per-agent** (`{env}.agent.token_cost.{agent}`); ordering not relied on (consumer dedups). `subjects.py:119-133`, `behaviors.py:378`, `costfeed.py:79-124` |
| 8 | `agent.arbitrate` | `agent_arbitrate` | **app** | at-most-once ok (request/reply; a lost request simply times out) | per-`(channel, task)` | none | subject axis is **per-request** (request/reply, one exchange). `subjects.py:86-88`, `behaviors.py:544`, `bus.py:293-338` |
| 9 | `agent.dead_letter` | `dead_letter` | **app** | **needs redelivery** (a lost dead-letter loses the only record of an unprocessable envelope — actual today is at-most-once) | per-`(channel, task)` | none | subject axis is a **single per-env** subject (`{env}.agent.dead-letter`); order guaranteed only within each `(channel, task)` slice. `subjects.py:110-116`, `dead_letter.py:96-127` |
| 10 | `kboard.event` | `kboard_event` | **app** | at-most-once ok (k-board Postgres is the source of truth; the bus event is a nudge, poller re-scans) | per-`(channel, task)` | none (record lives in k-board) | subject axis is **per-task** (`{env}.kboard.task.{event}.{task_id}`); order guaranteed only within each `(channel, task)` slice. `subjects.py:145-147`, `behaviors.py:665`, `intake_poller.py:400`, `loop.py:223` |
| 11 | `hub.control` | `hub_control` (+`_wildcard`) | **app** | at-most-once ok (operator steer; if lost, operator re-issues) | per-`(channel, task)` | none | subject axis is **per-task** (`{env}.hub.control.{task_id}`); order guaranteed only within each `(channel, task)` slice. `subjects.py:150-167`, `control.py:270`, `loop.py:247` |
| 12 | `factory.runner` | `factory_runner` (+`_wildcard`) | **app** | at-most-once ok (a dropped progress ping is cosmetic; the durable record is the runner result file on the collab NFS share plus the k-board event log) | per-`(channel, task)` | none (record lives in the runner result file + k-board) | subject axis is **per-job** (`{env}.factory.runner.{job_id}`); order guaranteed only within each `(channel, task)` slice. **Publish-only lane** — the `fleet-runner` principal has `subscribe { deny = [">"] }` (`deploy/nats/fleet-prod-nats-server.conf`), and no role subscribes it (`CODE_PUBLISH_ONLY_ROLES`). fleet-bus #222, consumed by a software-factory dashboard. `subjects.py:180-207`, `registry.py:118-119` |

### Lanes added after ratification (each declares the four fields at birth)

| # | Lane (registry key) | Subject builder | Plane | Delivery goal | Ordering scope | Retention | Receipt |
|---|---------------------|-----------------|-------|---------------|----------------|-----------|---------|
| 13 | `agent.ack` | `agent_ack` (+`.channel`/`_wildcard`) | **app** | at-most-once ok (an ack is a convenience signal, not the record; the durable proof a dispatch landed is the worker's subsequent status/done and the k-board row. A lost ack costs an operator one line of observability, nothing else) | per-`(channel, task)` | none (the mineable record is the log/JetStream-future; the bus is not the record) | fleet-bus #116. Publish-only from the agent — **no role subscribes it** (deliberately absent from `CODE_ROLE_LANES`). `subjects.py` `agent_ack`, `registry.py` `CODE_SUBJECTS["agent.ack*"]`, `adapter.py` `_publish_noise`. "ZERO land in the parent Discord channel" is enforced by `adapter.send` returning after `_publish_noise`, BEFORE the tee; `voice_tee.is_forwardable` is a deliberate second barrier for future wildcard-subscribing bridges and cannot fire on today's call sites |
| 14 | `agent.telemetry` | `agent_telemetry` (+`.channel`/`_wildcard`) | **app** | at-most-once ok (a dropped progress marker is cosmetic — identical rationale to `factory.runner`; the turn's real outcome rides `status`) | per-`(channel, task)` | none | fleet-bus #116. Same publish-only/no-subscriber/no-forward contract as `agent.ack`; split from it by VOLUME (unbounded per turn vs one per dispatch) so a delivery-confirmation miner need not filter a firehose. `subjects.py` `agent_telemetry`, `adapter.py` `_noise_lane` |
| 15 | `ob.context_pack` | `ob_context_pack` | **app** | at-most-once ok (request/reply; a lost request simply times out and the caller retries — same shape as `agent.arbitrate`) | per-`(channel, task)` | none (the durable record is Open Brain's own Postgres, which the worker answers from; the bus is not the record) | fleet-bus #221. Subject axis is a **single per-env service endpoint** (`{env}.ob.memory.context_pack`); order guaranteed only within each `(channel, task)` slice. Consumer is OB's deterministic context-pack worker — receipt creates no LLM turn. Server grant `{env}.ob.>` (`deploy/nats/fleet-prod-nats-server.conf`, pinned by `test_authz_conf.py`). **Provisioned, not yet live** — the worker exists in OB's code but every deployed OB worker pins `OPENBRAIN_NATS_ENABLE_BRIDGE=false`, so no traffic rides this lane today (see `docs/consumers.md`); the row declares the four fields at birth per C3 so the lane is contracted before it carries traffic. The Python builder is the source of truth the TS mirror must match |

**Channel scoping (#117) is not a new lane.** Extending the `{channel}` token to
`status`/`relay`/`wake` (and the noise lanes) adds a routing axis to lanes 1-5,
not new lanes: each keeps its ratified plane, delivery goal, and retention. The
ordering scope is *unchanged and unaffected* — the contracted ceiling was already
per-`(channel, task)`, so making the channel explicit in the subject brings the
subject key **closer** to the contracted axis rather than widening it. No row
above needed a QoS revision as a result.

### Judgment-call lanes (controller: verify these)

Two lanes are **defined in source** (builder + catalog) but have **no orchestrator/subscriber
call site** at `060d71f`. They are not in the task's 11-lane list. Classified conservatively and
flagged rather than silently dropped:

| # | Lane (registry key) | Subject builder | Plane | Delivery goal | Ordering scope | Retention | Receipt / judgment |
|---|---------------------|-----------------|-------|---------------|----------------|-----------|--------------------|
| J1 | `agent.claim` | `agent_claim` | **app** | at-most-once ok (request/reply worker-claim; lost claim → retry) | per-`(channel, task)` | none | subject axis is **per-request** (`{env}.agent.claim.{task_id}`, request/reply). `subjects.py:81-83`, `registry.py:45`. Builder exists; **no publish/subscribe call site** in `packages/*/src` at `060d71f`. Classified app (request/reply, code-only) by analogy to `agent.arbitrate`. |
| J2 | `direct` | `direct` | **token** (intended) | at-most-once ok | per-`(channel, task)` | none | subject axis is **per-agent** (`{env}.direct.{agent}`); order guaranteed only within each `(channel, task)` slice. `subjects.py:9,140-142`, `registry.py:58`. Builder + catalog entry ("local/human agents → trio, no Discord overhead"); **no call site** at `060d71f`. Intended as a direct-to-agent inbox-style lane → token by intent, but unwired. |

Both also appear in T4's `docs/fleetbus-traffic-matrix.md` reconciliation (PR #130). When
either gains a real call site, it re-enters via the new-lane checklist below.

---

## 2. New-lane checklist (mandatory at birth)

**Before any new subject is added via `fleet_nats.subjects`, the lane MUST declare all four
ratified fields in this contract.** A subject builder without these four declarations is not a
ratified lane and must not be merged.

- [ ] **Plane** — `app` (consumed by deterministic code only) or `token` (enters an LLM
      agent's context as a turn). A token-plane lane costs tokens per subscriber; justify it.
- [ ] **Delivery goal** — `at-most-once ok` (correct as-is on NATS core) **or** `needs
      redelivery` (wants a guarantee the core bus cannot give → JetStream-future candidate;
      say so explicitly, do not imply the current bus delivers it).
- [ ] **Ordering scope** — must be **per-`(channel, task)` or narrower** (C5). A wider scope
      claims a guarantee NATS core does not provide and is rejected.
- [ ] **Retention** — `none` today (no JetStream stream). If the lane needs a durable record,
      name where the record actually lives (e.g. k-board Postgres for `kboard.event`); the bus
      is not the record (design invariant).

Wiring reminder (from the seed §7 registry mechanics): adding a lane touches
`subjects.py` (builder) → `registry.py` `CODE_SUBJECTS` (+ `CODE_ROLE_LANES` if a role gains
the lane) → subscriber `adapter.py` `_LANE_BUILDERS` (if role-subscribed) → `registry_sync`
(PUT to k-board) → the twin-lock tests (`tests/test_registry.py`). The four declarations above
are the *contract* gate on top of that mechanical wiring.

---

## 3. Provenance and authority

- **Ratified decision:** issue #112 comment
  the issue #112 decision comment (Rico, 2026-07-10).
- **Seed matrix:** `wave1-research/fleetbus-traffic-matrix.md` @ commit `060d71f` — the P1–P15
  publish and S1–S8 subscribe inventory this table collapses to lanes.
- **Live-source verification:** every receipt above re-checked against this worktree at `060d71f`
  (`packages/fleet-nats/src/fleet_nats/{subjects,registry,bus}.py`,
  `packages/fleet-orchestrator/src/fleet_orchestrator/*`,
  `packages/fleet-subscriber/src/fleet_subscriber/*`).
- **T4 coordination:** plane-classification prose contract is issue #119 →
  `docs/fleetbus-traffic-matrix.md` (PR #130), a distinct file. This contract references it;
  the two lanes do not collide.
