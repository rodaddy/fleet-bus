# fleet-bus architecture (operator/system doc)

The end-to-end architecture of the fleet message + coordination bus: how a task
moves from a human post in Discord, through deterministic NATS coordination, into
the k-board record, and back to Discord as the agents' own words.

This is the operator/architecture reference (issue #12, Phase 8). For deploying
and verifying the subscriber, see [`runbook.md`](./runbook.md). For the logging
convention, see [`logging.md`](./logging.md). This doc does not repeat either;
it explains the system they operate.

---

## 1. Coordination architecture — four planes, one job

fleet-bus separates *display* from *coordination* from *record*. Each plane has
exactly one job, and the design invariants forbid collapsing them.

| Plane | Medium | Role | What it is NOT |
|---|---|---|---|
| Human display | Discord | Where humans read what agents say. Agents post their own words. | NOT where coordination happens. No case-statements, no state machine lives here. |
| Control plane | fleet-core `FleetBus` over NATS/JetStream | The DAP control plane — deterministic dispatch/relay/arbitrate driven by a pure state machine and a shared transport contract. Ordinary coordination publish/subscribe is durable by default through pre-provisioned JetStream streams/consumers. | NOT an LLM. No model sits in the control path; every decision is a case-statement. |
| Source of truth | k-board (Postgres, tenant `pg`) | The durable handoff record — task lifecycle status. | NOT the broker. The bus delivers; k-board *is* the lifecycle record. |
| Durable delivery substrate | JetStream behind fleet-core | Replay, ack/redelivery, durable consumers, and broker dedup where configured. Durable paths bind only to pre-provisioned streams/consumers and fail loud when provisioning is missing. | NOT a second task database, not consumer-owned policy, and not a production cutover before F3/#105 proves provisioning/load. |

The load-bearing consequences:

- **Discord = human display.** Coordination does not live in Discord. We do not
  ride Discord's mention-gate or thread mechanics to decide who works next. The
  agent's reply that a human reads in Discord is the agent's own prose — the bus
  carries it, it does not author it.
- **fleet-core FleetBus = the DAP control plane transport.** The "who goes next, relay this, arbitrate
  that" logic is deterministic code we own (`fleet_orchestrator.transitions`),
  off Discord. There is **no LLM in the control path** — `next_action(state,
  event)` is a pure, exhaustively-testable case-statement with no I/O, no clock,
  no random, no model.
- **k-board Postgres = handoff source of truth.** When the orchestrator advances
  a task, it writes the durable status to k-board *first*, then emits the
  bus signal. A consumer can always re-derive lifecycle truth from k-board. The
  broker is delivery, never the lifecycle record.
- **Durable by default, with explicit exceptions.** Ordinary coordination
  publish/subscribe uses fleet-core durable mode. Missing JetStream capability,
  missing stream/consumer, or ack mismatch fails loud with **no silent core
  fallback**. Heartbeat/Presence is the sole implicit ephemeral exception because
  absence is the signal; stale redelivery would lie. Other core-NATS use must be
  an explicit `DeliveryMode.CORE` opt-out.

**Current rollout status:** C1-C7 are landed on `main`: fleet-core is the base
package, typed specs are on the wire, ordinary coordination is durable by
default, and typed Heartbeat/Presence are the only automatic ephemeral
exceptions. `fleet-nats` imports/re-exports the shared fleet-core
spec/config/transport surface behind explicit legacy core compatibility, and the
C7 downstream cutover has landed: fleet-orchestrator reads the typed fleet-core
Stance/Done signals and fleet-subscriber emits typed turn fields on its active
send path (the prose-trailer parsers survive only as legacy compatibility, not
the coordinated path). C8 (this change) is the doc reconciliation for that base.
F3/#105 remains the gate before any production fleet-wide default cutover.

---

## 2. The subject tree

Subjects are built through the shared fleet bus library surface, with
`fleet_nats.subjects` still owning the subject builders. The fleet-nats registry
and KV helpers also remain fleet-nats-owned compatibility surfaces; fleet-core
owns the shared spec/config/transport base. Nothing downstream hand-formats a
subject string. The tree mirrors the live Dash
convention `{env}.{domain}.{...}` (e.g. `prod.signal.momentum.CL`):
dot-delimited, env-prefixed, hierarchical. Building on the same shape means the
fleet's JS/TS nodes (the Dash mesh's own language) buttress the Dash mesh rather
than fork it. The parity is in the *subject convention* and typed envelope
compatibility; delivery mode is fleet-core policy.

The first token after `{env}` is the **interaction layer**, so a flood or failure
in one layer cannot contaminate another and each is independently observable:

| Layer | Shape | Purpose |
|---|---|---|
| `agent` | `{env}.agent.<...>` | Crew coordination (Discord-originated trio DAP) |
| `direct` | `{env}.direct.<agent>` | Local/human agent → fleet agent, off-Discord, no Discord overhead |
| `human` | `{env}.human.<...>` | Human / local-agent topics |
| `kboard` | `{env}.kboard.task.<event>.<task>` | k-board task lifecycle events |
| `hub` | `{env}.hub.<...>` | Out-of-band control plane for the manager run-loop |
| `factory` | `{env}.factory.runner.<job_id>` | Publish-only telemetry from disposable runner boxes (#222). The one layer with an **untrusted publisher**: app-plane only, never routed into an LLM turn |

Within the `agent` layer, the builders (all in `fleet_nats/subjects.py`):

| Builder | Subject | Direction |
|---|---|---|
| `agent_dispatch(env, worker, channel=…)` | `{env}.agent.{channel}.dispatch.{worker}` (or `{env}.agent.dispatch.{worker}` when no channel) | manager → worker: hand off a task |
| `agent_dispatch_wildcard(env, worker)` | `{env}.agent.*.dispatch.{worker}` | worker subscription: match per-channel dispatches. NATS matches per-token, so the bare form alone never receives a per-channel dispatch — subscribers need both (the adapter's dispatch lane subscribes both) |
| `agent_status(env, task_id)` | `{env}.agent.status.{task_id}` | worker → manager: progress / heartbeat / complete |
| `agent_relay(env, to)` | `{env}.agent.relay.{to}` | manager → peer: relay missed context |
| `agent_wake(env, worker)` | `{env}.agent.wake.{worker}` | manager → worker: nudge a quiet worker |
| `agent_claim(env, task_id)` | `{env}.agent.claim.{task_id}` | worker claims a task (request/reply) |
| `agent_arbitrate(env, topic)` | `{env}.agent.arbitrate.{topic}` | worker → manager: ask for a decision (request/reply) |
| `agent_inbox(env, agent)` | `{env}.agent.inbox.{agent}` | per-agent direct inbox |
| `agent_heartbeat(env, agent)` | `{env}.agent.heartbeat.{agent}` | per-agent liveness |
| `agent_heartbeat_wildcard(env)` | `{env}.agent.heartbeat.*` | consumer subscription: every agent's liveness |
| `token_cost(env, agent)` | `{env}.agent.token_cost.{agent}` | per-agent per-turn token-cost feed (#27) |
| `token_cost_wildcard(env)` | `{env}.agent.token_cost.*` | consumer subscription: every agent's token cost |
| `direct(env, agent)` | `{env}.direct.{agent}` | off-Discord direct lane |
| `kboard_event(env, event, task_id)` | `{env}.kboard.task.{event}.{task_id}` | lifecycle event (`created`/`claimed`/`status`/`comment`/`completed`) |
| `wildcard(env, layer)` | `{env}.{layer}.>` | subscribe across an entire layer |

**Per-channel isolation.** Dispatch subjects carry the originating Discord
channel slug (`agent_dispatch(env, worker, channel=…)`), so tasks in different
channels never cross-contaminate — a dispatch in one channel lands on a distinct
subject from the same worker's dispatch in another. All tokens are normalized by
`_slug()` (lowercased, spaces/dots → hyphens) so an ordinary channel or agent name
becomes a clean subject token. (`_slug` normalizes dots/spaces/case; it does not
strip NATS wildcard tokens like `*`/`>` — fleet agent/channel ids don't contain
those, so this is a naming convention, not an injection guard.)

**Dash parity.** Because subjects are `{env}.{domain}.{...}` like Dash's
`@example/core-nats`, the fleet's JS/TS mesh nodes share one subject
convention with Dash rather than forking it. Delivery mode is separate:
fleet-core ordinary coordination defaults durable, with explicit core opt-out
and the heartbeat exception. Do not invent a subject by string concatenation; add
or use a builder in the shared fleet bus subject surface instead.

---

## 3. The coordination state machine

The heart of the DAP is a pure function: `next_action(session, event) -> Action`
in `fleet_orchestrator/transitions.py`. It is the deterministic replacement for
the failed human-in-the-loop coordination — no LLM, no I/O, no clock, no random.
Given the current state and an inbound event, it returns exactly one action and
the already-advanced session snapshot.

### States

`CoordinationSession.state` is one of five (`fleet_orchestrator/state.py`), in
nominal forward order:

```
DISPATCHED → WORKING → RELAY_NEEDED → ARBITRATING → DONE
```

- **DISPATCHED** — the task has been handed to the workers.
- **WORKING** — at least one worker is active on the task.
- **RELAY_NEEDED** — the human-replacement core: a worker finished a turn without
  handing off to the peer it needed; the orchestrator must relay the missed
  context before work continues.
- **ARBITRATING** — a worker asked the manager to decide (who-goes-first /
  conflict); the manager owes a deterministic decision.
- **DONE** — terminal. Once DONE, nothing re-opens the session; every further
  event is a logged NOOP.

A `CoordinationSession` is frozen — transitions return a *new* session via
`advanced_to(...)`, so a task's history is a value sequence and every snapshot
reflects exactly the state it is in (stale `relay_target`/`arbitrate_topic` never
linger past their state).

### Transitions

Each row is a `(state, event) → (action, next state)` branch of the
case-statement. `NOOP` means **no bus behavior**, not "no state change" — callers
persist `action.session` for *every* action, and only touch the bus for the
non-NOOP kinds.

| From state | Event | Action | To state | Note |
|---|---|---|---|---|
| DISPATCHED | `DISPATCH` | `DISPATCH` | WORKING | Fan out to workers. Re-dispatch is idempotent *at the state level* (same target state); the wire fan-out is not (fresh envelope, deliberate re-run — see C5). |
| DISPATCHED | `WORKER_STARTED` | `NOOP` | WORKING | Worker acknowledged. |
| WORKING | `WORKER_STARTED` / `WORKER_PROGRESS` | `NOOP` | WORKING | Still working; stay observable. |
| WORKING | `HANDOFF_MISSING` (with `relay_to`) | `RELAY` | RELAY_NEEDED | **The relay.** Worker forgot to hand off → relay missed context to the peer. |
| WORKING | `HANDOFF_MISSING` (no `relay_to`) | `NOOP` (guard) | WORKING | A relay to nobody is malformed — logged, refused. |
| WORKING | `ARBITRATE_REQUESTED` (with `topic`) | `ARBITRATE` | ARBITRATING | Manager owes a decision. |
| WORKING | `ARBITRATE_REQUESTED` (no `topic`) | `NOOP` (guard) | WORKING | Nothing to decide — logged, refused. |
| WORKING | `DONE` | `COMPLETE` | DONE | Unambiguous completion sentinel. |
| RELAY_NEEDED | `RELAY_SENT` | `NOOP` | WORKING | Relay delivered; back to working. |
| RELAY_NEEDED | `HANDOFF_MISSING` (has a target — `relay_to` or the session's `relay_target`) | `RELAY` | RELAY_NEEDED | Re-notified before first relay landed; re-emit to the same peer. |
| RELAY_NEEDED | `HANDOFF_MISSING` (no target either) | `NOOP` (guard) | RELAY_NEEDED | A relay re-request with no target is refused, same as the WORKING guard. |
| ARBITRATING | `ARBITRATED` | `NOOP` | WORKING | Decision returned; coordination resumes. |
| any non-terminal | `DONE` | `COMPLETE` | DONE | Late/direct done sentinel from any state. |
| DONE | any | `NOOP` | DONE | Terminal. |
| any | unhandled `(state, event)` | `NOOP` (logged) | unchanged | A stray event never crashes the control loop. |

### The relay behavior (the human-replacement core)

The relay is why this bus exists. In the old human-in-the-loop DAP, when a worker
finished its turn but forgot to hand off to the peer it needed, a *human* noticed
and nudged the other agent. `RELAY_NEEDED` automates exactly that:

1. Worker output arrives on `agent_status` (`watch_worker_status` in
   `behaviors.py`).
2. The deterministic `detect_missing_handoff(sender, text, peers)` decides
   whether the worker forgot to hand off — no LLM, a pure detector.
3. If a handoff is missing, a `HANDOFF_MISSING` event (carrying `relay_to` = the
   peer that was *not* mentioned, and the missed context) is applied.
4. `WORKING + HANDOFF_MISSING → RELAY`, moving the session to `RELAY_NEEDED`.
5. `Orchestrator._relay(...)` publishes the missed context to
   `agent_relay(env, target)` — the peer now has what it needed and work
   continues. No human, no LLM, no fabricated event.

The missed-handoff text lives on the *advanced session*
(`session.relay_context`), not on the action, so `_relay` passes it explicitly
into the outbound envelope.

### Arbitrate

Arbitration is **request/reply**, not fire-and-forget. A worker sends on
`agent_arbitrate(env, topic)` and blocks on a reply. `Orchestrator.serve_arbitration(topic, workers)`
subscribes as the responder, answers deterministically via
`decide_arbitration(topic, workers)` (a case-statement, never an LLM), and replies
on the request's reply subject with a `correlation_id` echoing the request id. An
empty candidate set is rejected at setup (fail loud) so a request never hangs.

---

## 4. The agent-subscriber mechanism

An agent participates in the bus through the NATS platform adapter in
`fleet_subscriber/adapter.py`. It is a **Hermes `BasePlatformAdapter`** registered
via the sanctioned `register_platform` lever — not a standalone pipeline. The flow
turns a bus envelope into a full agent turn and the agent's own reply back onto
the bus.

```
dispatch/relay/wake/inbox envelope
        │
        ▼
_on_envelope  ──►  envelope_to_message  ──►  MessageEvent(internal=True)
        │                                            │
        │                                            ▼
        │                                     handle_message  ──►  full agent turn
        │                                            │
        └── stash chat_id → task_id                  ▼
                                              send(chat_id, content, …)
                                                     │
                             ┌───────────────────────┤
                             ▼                        ▼
                  publish reply on            if typed done signal:
                  agent_status(env, task_id)  publish done sentinel
                  (the agent's own words)     on the same status subject
```

Step by step:

1. **Subscribe.** On `connect()`, the adapter opens a `FleetBus` and subscribes to
   *this* agent's role lanes — a worker gets `dispatch`/`relay`/`wake`/`inbox`; a
   manager gets `inbox` only (the task-keyed `status`/`arbitrate` lanes belong to
   the orchestrator, not the adapter). Every subject is built via
   `fleet_nats.subjects`.
2. **Ingress → internal turn.** `_on_envelope` maps the envelope to a synthetic
   internal `MessageEvent(internal=True)` and calls `handle_message`.
   `internal=True` **bypasses the mention-gate and user-auth** (host-verified) —
   the whole point of coordinating off Discord's mention mechanics. The adapter
   stashes `chat_id → task_id` so the eventual reply can be correlated back to the
   task.
3. **The agent's own reply out.** The gateway runs a full turn and posts the
   agent's reply back through the adapter's `send()`. `send()` publishes the reply
   text on `agent_status(env, task_id)` — the exact subject the orchestrator's
   `watch_worker_status` listens on. The reply is the **agent's own words**; the
   bus only delivers them. `send()` uses `get()` (peek, not pop) on the
   correlation map because the gateway calls `send()` many times per turn (ack,
   progress, footer, the real reply) — popping would misroute later sends.
4. **Done sentinel + correlation.** Since C7, the completion signal rides a
   **typed turn field**, not a stripped prose trailer: `send()` recovers
   `(completion, stance, visible)` at a single deserialization boundary
   (`_recover_turn_signals`) that prefers a JSON `Turn` whose named `done`/
   `outcome`/`stance` fields carry the control signal — so a literal `[[done]]`
   or gateway footer inside `visible_text` is inert text, never a signal. On a
   typed `done`, `send()` publishes the clean visible reply, then emits the
   un-spoofable **done sentinel** via the shared `done_envelope` builder on the
   *same* status subject. (The legacy `[[done]]`/`[[done:outcome]]` and
   `[[stance:…]]` text-trailer parsers were **removed** in fleet-bus
   #205; `completion.py` /
   `stance.py` now hold only the value types. A reply that is not a typed turn is
   visible prose only.) On the orchestrator side, `parse_done` turns
   that `kind="done"` envelope into a `DONE` event — checked **first**, before
   missing-handoff detection, so completion is deterministic and never mistaken
   for a forgotten handoff. A sentinel that names no task is rejected (it can't
   advance any session). The correlation is then dropped so the channel is free
   for the next task.

The done sentinel's wire shape (`kind="done"`, `payload={"task_id","outcome"}`) is
defined once in `fleet_nats.sentinel` and re-exported through
`fleet_orchestrator.sentinel`, so emitter and consumer can never drift.

The done sentinel is the clearest instance of the base-package rule that
completion — like all coordination — rides a **typed envelope signal**, never
body text: the authoritative `Done` is on `typed_signal`, and the
`payload={"task_id","outcome"}` is only a mirror for pre-typed consumers. The
bus never routes or completes on a payload key. See
[C6 — Envelope vs. Message Body](contracts/C6-envelope-vs-message-body.md) for
the full contract.

---

## 5. Per-agent enable + verification

Enabling and verifying the subscriber across the trio (Nagatha the manager,
Skippy and Bilby the workers) is a maintainer-gated, front-door deploy — see
**[`runbook.md`](./runbook.md)** for the full procedure. Do not duplicate it. The
operator-critical points it covers:

- **Verify by effective state, never by config file.** Probe the running agent
  (`specs/probe-trio-subscribers.sh` against the NATS `/connz` monitor). A `SKIP`
  line is *not* acceptance — it means the monitor was unreachable.
- **`allow_bots` is env-only and never `none`.** A worker needs at least
  `mentions`; Nagatha may be `all`. It is set directly in the agent's environment,
  never bridged from config, so a config restore can't silently drop it to `none`.
- **Role-aware `FLEET_ROLE`.** `worker` (default, least-capable) subscribes the
  four work lanes; `manager` subscribes `inbox` only. An unset role fails *safe*
  (worker), a missing `FLEET_ROLE=manager` fails *silent* (wrong topology) — so
  Nagatha's **effective** role is checked explicitly from her `connect` log.

---

## 6. End-to-end walkthrough — one task, human post → dispatch → relay → done → Discord

This is the traceable narrative: follow a single task through every plane. This
scenario's ordering is **dispatch → relay → done** — the relay beat is
*conditional* (it fires only when a worker omits the handoff; a clean handoff
skips it), and `DONE` can arrive from any non-terminal state (see the transition
table). Each beat below names the subject and code that carries it.

> **Design vs. live status.** This walkthrough is the *design* each tested piece
> implements — the state machine, the fan-out, the relay detector, the done
> sentinel, and the k-board bridge all exist and are unit-proven. The standing
> **manager run-loop** that wires them into a live end-to-end flow (the loop that
> holds the per-task `agent_status`/`agent_arbitrate` subscriptions) is **Phase 9**;
> the end-to-end live proof against the real trio is captured there. So read the
> beats below as "how the pieces compose," proven live in Phase 9 — not as a flow
> a freshly-enabled subscriber runs on its own today.

**Beat 0 — human post (Discord).** A human posts a task in a Discord channel:
"Skippy and Bilby, ship the X change." This is the only human-authored moment.
Discord is display; the human post is what *starts* coordination, but coordination
itself now leaves Discord.

**Beat 1 — dispatch (NATS control plane).** The manager (Nagatha) opens a
`CoordinationSession` in state `DISPATCHED` for the task's k-board `task_id`,
carrying the originating channel slug. A `DISPATCH` event runs through
`next_action`: `DISPATCHED + DISPATCH → DISPATCH action, state WORKING`.
`Orchestrator._dispatch` fans the task out, publishing an envelope per worker on
`agent_dispatch(env, worker, channel=…)` — per-channel isolated. Each worker's
subscriber picks it up (`_on_envelope`), forges a `MessageEvent(internal=True)`
(mention-gate + auth bypassed), and `handle_message` runs a full agent turn.

**Beat 2 — working + the relay (NATS control plane).** Skippy finishes its turn
and replies; the subscriber's `send()` publishes Skippy's own words on
`agent_status(env, task_id)`. The orchestrator's `watch_worker_status` receives it
and runs `detect_missing_handoff` — and finds Skippy forgot to hand off to Bilby.
This fires the **relay**: `HANDOFF_MISSING(relay_to=bilby)` →
`WORKING + HANDOFF_MISSING → RELAY action, state RELAY_NEEDED`.
`Orchestrator._relay` publishes the missed context to `agent_relay(env, bilby)` —
the human-replacement relay, no human in the loop. Bilby's subscriber receives the
relay, runs its turn, and on `RELAY_SENT` the session returns to `WORKING`. (If a
worker had instead asked the manager to decide something, this is where
`ARBITRATE_REQUESTED` would move the session through `ARBITRATING` and back.)

**Beat 3 — done (NATS control plane + k-board record).** A worker finishes with a
typed `done` turn signal (the C7 typed path; a legacy `[[done]]` trailer no
longer completes a coordinated turn). Its `send()` posts the clean reply, then
emits the done sentinel on `agent_status(env, task_id)`. `watch_worker_status` checks `parse_done`
*first*, gets a `DONE` event, and `WORKING + DONE → COMPLETE action, state DONE`.
The session is terminal. On the transition, `persist_transition` writes the
durable status to **k-board first** (`update_status(task_id, "done")`, CAS-safe),
*then* emits the `kboard_event(env, "completed", task_id)` on the bus — record
before signal, so truth survives a bus outage.

**Beat 4 — back to Discord (human display).** Precise boundary: this repo's
subscriber `send()` publishes the agent's reply onto the bus (`agent_status`),
not to Discord directly. What surfaces those words in Discord is the agent's *own
Hermes gateway* (its normal Discord adapter) — outside fleet-bus. So the human
reads the agents' own words in Discord, but fleet-bus's job ends at the bus; it
does not own the Discord write. Discord displayed the human post that started it
and (via each agent's gateway) the agents' own replies that resolved it; the
coordination that connected them — dispatch, relay, done — happened
deterministically on NATS and was recorded in k-board, never on Discord.

**The trace, in one line:** human post (Discord) → **dispatch** on
`agent_dispatch` → worker turn on `agent_status` → **relay** on `agent_relay` →
typed `done` sentinel → **done**/`COMPLETE` + k-board `completed` → agents' own words
shown in Discord.

---

## 7. Delivery semantics — the reliability contract (#25)

The bus is load-bearing: a lost dispatch is a lost task. This section is
**normative** — consumers code against it. Four failure modes, four defined
behaviors; nothing vanishes silently.

### 7.1 Reconnect / resubscribe

`FleetBus` surfaces nats-py's reconnect machinery through `BusConfig`:
`FLEET_RECONNECT_MAX` (default `-1`, retry forever) and
`FLEET_RECONNECT_WAIT_SECONDS` (default `2`), both validated fail-loud. Every
connection-state transition is logged (`fleetbus.reconnect | …`); a connection
that exhausts its budget logs an **error** line — give-up is never silent.
Core-NATS subscriptions ride the client's subscription table, so they
re-establish across a reconnect with no re-subscribe call from the consumer —
verified by `packages/fleet-nats/tests/test_reconnect.py`, which kills and
restarts a local throwaway `nats-server` mid-subscription and asserts delivery
resumes (the local twin of the fleet-hub-restart runbook drill).

### 7.2 Delivery semantics: durable-default transport + idempotent consumers

**Decision (see `docs/contracts/C5-ordering-retry-semantics.md`, the canonical
contract):** historical production deployments may still be core-only until
F3/#105 proves provisioning and load, but the fleet-core C4/C5 target contract is
durable-by-default for ordinary coordination traffic. Durable publish/subscribe
binds to pre-provisioned JetStream streams/consumers, validates ack/bind state,
and fails loud with no silent core fallback when provisioning is missing.
Heartbeat/Presence is the sole implicit ephemeral exception; declared
`DeliveryMode.CORE` remains an explicit opt-out.

Dispatch / relay / wake can still see **duplicates**. The duplicate source may be
app-level retry, reconnect republish, or durable redelivery depending on the
lane's provisioning state. Losing a dispatch is worse than repeating one, so
consumers must be idempotent regardless of whether the broker is currently core
or durable.

Duplicates require idempotent consumers. The subscriber keys a bounded LRU
seen-set on `Envelope.id` (`fleet_subscriber.dedup.DedupGuard`,
`FLEET_DEDUP_WINDOW`, default 1024): a duplicate id is a logged no-op, never a
second agent turn. A duplicate that slips past the window is reconciled at the
record — **k-board is the source of truth**, keyed by `task_id`; a double-run
is visible and correctable there.

### 7.3 Dead-letter

An envelope the subscriber cannot process (mapping crash, malformed fields —
anything that raises out of ingress) is published as a **sanitized** record on
`subjects.dead_letter(env)` (`{env}.agent.dead-letter`) and logged; the
subscriber stays up. The record carries `reason`, `source_subject`,
`source_id`, `source_kind`, `source_ts` — **never the raw payload** (the
`logging.md` never-log list). If the dead-letter publish itself fails, the
reporter logs and drops — the error path can never take the subscriber down.
Watch it with `nats sub 'dev.agent.dead-letter'`; a spike means a schema drift
or a misbehaving publisher (see the runbook troubleshooting entry).

### 7.4 Agent-offline dispatch

**Decision:** a dispatch to an offline worker is detected by the orchestrator —
no worker signal within `FLEET_DISPATCH_TIMEOUT_SECONDS` (default 300),
cross-checked against Phase-2 heartbeat staleness — and walks a deterministic
ladder (`fleet_orchestrator.offline.OfflineDetector`, injected clock):

1. **WAKE** the worker (`agent_wake`);
2. one more silent window → **REDISPATCH once** (a fresh envelope id — so it is
   deliberately NOT collapsed by §7.2's `Envelope.id` dedup: the worker re-runs
   by design. Duplicate execution is bounded by the detector firing this rung at
   most once per window, reconciled at the k-board `task_id` record — see C5);
3. one more silent window → **BLOCKED**: a durable k-board event surfaces the
   task (narration arrives with the #13 governor). A stale heartbeat skips the
   wake rung explicitly (waking a dead agent wastes a window; the skip is in
   the decision's reason).

For lanes not yet backed by explicit WorkQueue/durable consumers, k-board is
where a blocked task is durable and visible. After J2/#127 and the C6/C7 consumer
migration work land, dispatch/claim durability belongs to explicit
pre-provisioned WorkQueue streams/consumers, not accidental hidden waiting.
## 8. Token-cost feed (#27)

The governor (#13) and the cost dashboard (#14) both need one number per turn:
how many tokens it spent. This section defines the **subject and schema** on the
bus side so the emitter and the consumers agree exactly. The wire shape lives
once in `fleet_nats.tokencost` (builder + strict parser); no side hand-rolls the
dict.

### Subject

Published on `token_cost(env, agent)` → `{env}.agent.token_cost.{agent}`.
Consumers subscribe `token_cost_wildcard(env)` → `{env}.agent.token_cost.*` to
sum spend across every agent whose id is unknown at subscribe time. Both are
built only via `fleet_nats.subjects` — mirrors the `agent_heartbeat` pair.

### Schema

Envelope `kind="token_cost"`; payload (all keys defined in
`fleet_nats.tokencost`):

| Field | Type | Meaning |
|---|---|---|
| `agent_id` | non-empty str | Who spent the tokens. MUST equal the envelope `sender` (identity rule). |
| `task_id` | non-empty str | The k-board task the turn belonged to — the key the governor sums by. |
| `turn_id` | non-empty str | The specific measured agent turn. |
| `prompt_tokens` | int ≥ 0 | Input tokens the turn consumed. |
| `completion_tokens` | int ≥ 0 | Output tokens the turn produced. |
| `total_tokens` | int ≥ 0 | Carried explicitly; MUST equal `prompt_tokens + completion_tokens`. |
| `model` | non-empty str | The model the turn ran against (per-model cost). |
| `ts` | ISO-8601 str | The envelope `ts` — when the turn was measured. |

`parse_token_cost` rejects (raises `ValueError`, consumer logs + skips): wrong
kind, an `agent_id`/`sender` split, any missing/empty string, a non-`int` or
`bool` or negative count, a `total_tokens` that disagrees with
`prompt + completion` (**inconsistent accounting fails loud** — a governor must
never silently under-count), or a non-`str` model. A **zero-token turn is
valid** (a turn can legitimately spend nothing).

### Emission point

The emitter is **the agent runtime's post-turn hook** (Hermes-side), which
measures tokens at the agent-turn level and publishes one `token_cost` event per
turn against this exact schema. This issue (fleet-bus #27) owns only the SUBJECT
+ SCHEMA on the bus side so both ends match; the agent runtime's post-turn hook owns emitting it.

### DECISION — its own subject, NOT ridden on the #26 heartbeat

Token cost gets its **own** `token_cost` lane rather than folding a cost field
into the heartbeat (#26). The heartbeat is **bounded-interval** liveness; a
token-cost event is **per-turn** accounting. Their cadences differ, and a
governor summing spend must not miss a turn that happens between two beats — nor
have cost tied to a liveness cadence that changes for unrelated reasons. Separate
lanes also keep each independently observable: a flood or failure of one cannot
contaminate the other.

### App-level duplicates → dedup on turn_id (idempotency)

Token-cost remains its own per-turn lane. Under the fleet-core durable-default
model it should be durable unless deliberately classified otherwise; J1/#125
still owns broker `Nats-Msg-Id` dedup on scored lanes and remains open. A
reconnect re-send, emitter retry, or durable redelivery can republish the same
turn's `token_cost` event. A consumer that *blindly sums* the feed would
double-count a duplicate turn, inflating a task's spend and pausing the #13
governor on phantom cost. `turn_id` is in the schema precisely so a consumer can
make spend idempotent: dedup on `(agent_id, task_id, turn_id)` before summing.
`fleet_orchestrator.costfeed`'s `CostLedger` does this **built-in** (a bounded
LRU seen-set; a duplicate turn is a logged no-op, summed exactly once). The
stateless `sum_task_cost` helper does not — its docstring states the caller owns
dedup of the event list it passes in. A duplicate that slips past the LRU window
is reconciled at the record: k-board is the source of truth, keyed by `task_id`.

### Counts, never content

The payload carries measured token **accounting only** — counts, ids, model,
timestamp. It NEVER carries what a turn read or wrote, nor any body, secret, or
member id: the `docs/logging.md` never-log list applies verbatim. A governor
needs the *size* of a turn, not what was in it. The module enforces the rule at
the source (a checker greps `fleet_nats/tokencost.py` for content-bearing field
names and fails the build if any appear).

---

## 9. Turn signals + scorekeeper (#17)

Nagatha keeps score in real time as Skippy and Bilby work a topic, and on
non-convergence she decides. The orchestrator sees **all** turns on the shared
bus, so the cross-agent count is real *here* in a way a single agent gate cannot
achieve — a lone Hermes reflex (the agent runtime's turn-signal hook) sees only its own turns. All
of this is **deterministic case-statement logic**: no LLM, no heuristic, no
clock, no randomness. The stance of a turn is never guessed from its prose — only
read from what the agent *signed*.

> **Wiring status (current shipped path).** The hub's
> `watch_worker_status` callback feeds each status envelope through
> `_observe_task_status` to `_observe_score`, which calls
> `Scorekeeper.observe()`. The parser reads the **typed fleet-core `Stance`
> signal** (`envelope.typed_signal`) first, with the legacy `payload.meta` shape
> as a compatibility fallback. Gateways still own emitting a signed signal;
> absent fields remain an unsigned, counted turn. Decision execution is wired
> but remains circlejerk-trip gated: `_observe_score` returns when `observe()`
> emits no event and runs `decide()` only after a trip.

### The turn-signal contract (the agent runtime's turn-signal hook seam)

Every worker turn already rides the `agent_status` envelope. Since C7 the signal
is carried as the envelope's **typed fleet-core `Stance`** (`typed_signal`), with
the legacy **`payload.meta`** shape parsed as a compatibility fallback. Two
structured fields carry it either way:

| Field | Type | Meaning |
|---|---|---|
| `stance` | `"agree"` \| `"dissent"` \| `"neutral"` | What the agent DECLARED about the peer's position — `dissent` is the break-the-circle pushback signal. |
| `fact_pull` | bool | Whether the agent pulled a fact this turn (the other break-the-circle act; ties to the agent runtime's fact-check). |

`parse_turn_signal(envelope)` (`fleet_orchestrator.score`) is strict — it prefers
the typed `Stance` signal, then falls back to `payload.meta`:

- **Present and valid** → a frozen `TurnSignal(agent_id, task_id, stance, fact_pull)`.
- **Present but malformed** (non-mapping `meta`, an invalid `stance` word, a
  non-bool `fact_pull`, or a *half-signed* turn with only one of the two fields)
  → `ValueError`. Fail loud — a typo is not a policy.
- **Simply absent** → `None`, an **UNSIGNED** turn. An unsigned turn is *counted
  and logged*, never inferred from the turn's prose. It is not agreement: it
  neither extends nor resets the agreement run.

The emitter of these fields is **the agent runtime's turn-signal hook** — the per-agent reflex hook
that decides whether it checked / dissented and stamps `meta` before the turn
goes on the bus. This issue (fleet-bus #17) owns only the parse + score + decide
on the orchestrator side; the agent runtime's turn-signal hook owns emitting the signal.

### Scorekeeping + the circlejerk threshold

`Scorekeeper` keeps a `Score` **per topic, keyed by `task_id`**, so interleaved
topics never cross-contaminate: the consecutive-agreement run, dissent count,
fact-pull count, per-agent turn counts, and the unsigned count. `observe(envelope)`
runs a pure case-statement update and returns a `circlejerk_detected` `ScoreEvent`
**exactly once** when the run reaches `FLEET_CIRCLEJERK_N` (**default 4**,
positive-int, fail-loud env parse) consecutive **agree**-stance turns with **zero
dissent and zero fact-pull** in that run.

**Reset + re-arm rule:** a `dissent` stance *or* a `fact_pull` (whatever the
stance) resets the run to 0 and re-arms the detector — it then takes a full fresh
`N` unbroken agreements to trip again. The once-per-run guarantee is a `tripped`
latch on the score, cleared by the same reset. A `note_done(task_id)` mid-exchange
ends scoring and clears the topic (a later task reusing the id starts clean). A
malformed signal is logged and **skipped at the `observe` boundary** — one bad
envelope never corrupts the tally or crashes the loop.

### Decider policy + tiebreak

`decide(score) -> Decision` is deterministic. An **evidenced position** is one an
agent *both* dissented on AND backed with a fact-pull (pushback without evidence
is noise; evidence without pushback is agreement). Evidence is tallied **per
agent** (`dissents_by_agent` / `fact_pulls_by_agent`), so a peer taking neutral,
agree, or unsigned turns never blocks a decidable topic. A single turn signed
`dissent` with `fact_pull: true` earns candidacy on its own (it counts as both).
The evidenced agent wins.

**THE documented tiebreak**, applied when more than one candidate qualifies:
**most fact-pulls, then most dissents, then the lexicographically smallest agent
id**. The lexicographic final tier makes the outcome fully deterministic — never
clock- or insertion-order-dependent.

When the score **cannot decide** — all-unsigned, no evidence, or a true tie /
evidence split across agents so no single position is backed — the decision is
`Decision(kind=SYNTHESIZE)`.

#### Shipped run-loop proving sequence

The live hub evaluates `decide(score)` only when `Scorekeeper.observe()` emits a
`circlejerk_detected` event. A normal status turn that does not trip the detector
returns from the run-loop's score observer without producing either `DECIDE` or
`SYNTHESIZE`. This is the shipped reachability contract, not a claim that every
completed debate produces a verdict.

That gate fixes the order of a proving run. To prove `DECIDE` on one fresh
`task_id`, first send the candidate's signed `dissent` with `fact_pull: true`,
then send a full uninterrupted run of `FLEET_CIRCLEJERK_N` signed `agree` turns
with `fact_pull: false`. The evidenced turn resets the agreement run, so placing
it inside or after those agreements prevents that run from tripping. On the
final agreement the detector trips; the earlier per-agent evidence remains in
the topic score, and the decider can name the evidenced candidate. To prove
`SYNTHESIZE`, use a separate fresh `task_id` and trip the detector with the same
uninterrupted agreement run but without a single agent holding both dissent and
fact-pull evidence. Keep the two outcomes on separate tasks so their cumulative
scores cannot contaminate one another.

### The synthesis carve-out — an agent turn, NEVER gate logic

`SYNTHESIZE` is **not** gate logic and does not rewrite worker output. Nagatha
arbitrates by *taking a turn*: `assign_synthesis(bus, session, env, new_id, now)`
publishes a normal dispatch envelope carrying a **templated** instruction
(ids/state-words only, no worker payload prose) to `agent_inbox(env, manager)` —
the manager's own inbox lane. She runs the synthesis as an ordinary agent turn
through her own gateway and assigns the result. Score-based decisions and
circlejerk trips narrate via the Phase-5 single `Narrator` (`decided` /
`circlejerk` beats — ids and state-words only) and trace via the Phase-1
`trace.emit` with the injected clock.

### Dashboard readiness (#14, Phase 7 — parked)

The circlejerk trips, score tallies, and decisions this phase produces are the
first real feed for the cost/coordination **dashboard (#14)**. That work is
deliberately **parked**: #14 stays OPEN and no `fleet-dashboard` package is built
until observed traffic (post-#17) justifies the shape — measure first. This
scorekeeper is the readiness record for it; the dashboard consumes these signals
when it unparks.

---

## 10. Subject registry — k-board `nats_subjects`, authoritative for now (the k-board registry endpoint)


**The decision (2026-07-08).** While the fleet's non-Dash subjects are being
locked down, the subject catalog (templates + role→lanes map) is registered in
a k-board Postgres table, and **that table is authoritative**. It is fully
separate from Dash's own subject set and database. The schema/API belong to
the k-board service repo (the k-board registry endpoint); this repo owns the client and the
seed/sync tool.

**How clients learn subjects.** `fleet_nats.registry.load_catalog()` — called
ONCE by the subscriber adapter at `connect()` and cached; never a hot-path
read:

- `FLEET_SUBJECT_REGISTRY_URL` unset → the baked-in code catalog
  (`registry.code_catalog()`, the documentation-of-record twin of
  `fleet_nats.subjects`, locked together by tests).
- Set → `GET {base}/api/nats-subjects?tenant=pg`; the fetched catalog wins.
  (Today the role→lanes map is what drives behavior; fetched subject
  TEMPLATES are validated for drift but not yet rendered into subscriptions —
  the `subjects.py` builders remain the render path.)
  ANY failure — unreachable, 404 (endpoint not deployed yet), malformed —
  logs a loud ERROR and falls back to the code catalog. **Fail-open by
  design: Postgres being down must never take an agent off the bus.**
- Drift between the fetched catalog and the code defaults is logged as an
  ERROR listing the drifted keys — the signal to reconcile `subjects.py` or
  the table. A live drift gate (`test_registry_live.py`) passes-or-skips
  off-network.
- The adapter applies the registry's role→lanes map only when every lane name
  is buildable; unknown names fall back to the code lanes (a bad registry row
  must never silence a worker's dispatch lane).

**Seeding.** `python -m fleet_nats.registry_sync` PUTs the code catalog to the
table (seed + reconcile after a `subjects.py` change). The version is
**client-owned**: `registry_sync` sends it, the endpoint stores what it is sent
and `409`s only on a stale write (it does **not** auto-bump). The code catalog's
version is `0` (the "unset / fallback" sentinel the endpoint validator rejects as
`< 1`), so the seed clamps up and sends version `1` for the first real stored
catalog (#155). It exits non-zero with a clear message until the k-board registry endpoint
deploys the endpoint — contract-first.

**Catalog-version enforcement at connect (#103).** Fail-open alone splits the
fleet: with the sync endpoint undeployed, every client silently runs the
version-0 code catalog, a taxonomy write is a no-op, and a partial rollout
leaves mutually-deaf populations. So a **minimum catalog version** is enforced
at connect via `registry.connect_catalog()` (which the subscriber adapter and
the orchestrator hub call instead of the bare `load_catalog()`):

- The floor is `FLEET_SUBJECT_CATALOG_MIN_VERSION` (`config.min_catalog_version()`).
  **`0` (the default) DISABLES enforcement** — the bootstrap posture, so a
  version-0 code-catalog fleet still starts before k-board serves a versioned
  catalog. The floor is never baked in; the operator turns it on at rollout.
- With a **positive floor set**, the effective catalog version MUST meet it.
  A registry outage falls back to the code catalog (version 0), which is below
  any positive floor, so the client **refuses to start** —
  `CatalogVersionTooLowError`, fail-loud, naming have-version vs min-version.
  Refuse-to-start is the enforced default whenever a floor is configured;
  falling back is thus a loud, explicit, logged decision, never a silent
  downgrade. Enforcement runs BEFORE the bus opens (no partial subscriptions),
  and the error propagates out of `connect()` (a HARD exit, not the soft
  return-False path).
- **Rollout ordering.** Deploy the k-board endpoint and seed the table first;
  confirm `GET /api/nats-subjects` returns 200 with the intended `version`;
  then raise `FLEET_SUBJECT_CATALOG_MIN_VERSION` to that version across the
  fleet. Because the floor is opt-in, a fleet mid-rollout (some hosts with the
  floor, some without) never bricks — the un-floored hosts keep failing open.
- **Version skew is observable on the bus.** Every heartbeat carries
  `catalog_version` (the version the emitter connected with; nullable so older
  beats parse as unknown). The orchestrator's liveness tracker surfaces it in
  `effective_topology()` and logs a `VERSION SKEW` WARNING the moment two live
  agents report different versions — the operator signal for a mixed-taxonomy
  fleet.

> **Cross-repo dependency (scoped out of this repo).** The sync/read endpoint
> `PUT|GET https://kboard.example.internal/api/nats-subjects?tenant=pg` is served
> by the **k-board service repo (the k-board registry endpoint)**, NOT fleet-bus. Until
> k-board deploys it, `registry_sync` exits 3 on 404 and `connect_catalog`
> falls open (or refuses to start, if a floor is configured). fleet-bus owns
> only the client contract + enforcement; deploying the endpoint is k-board's.
> Required there: a `version`-bumping table + a handler returning
> `{"version": int, "subjects": {..}, "role_lanes": {..}}` matching the shape
> `registry._parse_catalog` validates and `registry_sync.sync` PUTs.

**The flip seam.** When subjects are locked down, authority flips to
code-canonical (`registry.AUTHORITY`), the table becomes a mirror, and the
sync tool's direction is unchanged — only who is *believed* changes. No
client changes.

---

## Cross-references

- Deploy & verify the subscriber: [`runbook.md`](./runbook.md)
- Logging convention (what is/never logged): [`logging.md`](./logging.md)
- Coding standards (shared with rtech-agents): [`coding-standards.md`](./coding-standards.md)
