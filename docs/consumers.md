# fleet-core consumers — the canonical-library adapter pattern (#11)

`fleet-core` is the base package for the fleet message spec, config/env/auth,
logging, and default transport. Consumers are **thin adapters** over that shared
surface — they import the base `FleetBus` / `Envelope` contract where available,
use the shared subject builders, never reimplement transport, and never fork the
subject convention. `fleet-nats` remains the subject/helper compatibility package
and now imports/re-exports the shared fleet-core spec/config/transport surface
behind explicit legacy core compatibility.

C1-C7 are landed on `main`: the fleet-core base, typed wire spec, durable-default
transport, and typed Heartbeat/Presence ephemeral exception are the current base
contract; the fleet-nats compatibility/re-export shim is in place; and the C7
downstream cutover has landed — fleet-orchestrator reads the typed fleet-core
Stance/Done signals and fleet-subscriber emits typed turn fields on its active
send path (the prose-trailer parsers survive only as legacy compatibility). C8
(this change) reconciles the docs to that base. F3/#105 remains the gate before
any production fleet-wide default cutover.

> **The rule:** if a consumer needs a capability the shared fleet bus surface
> lacks, **file it back to fleet-core/fleet-nats ownership** and add it to the
> shared library — do not fork the transport, hand-format a subject, or create a
> consumer-local delivery policy. One base contract, many thin adapters.

## The public surface (all a consumer needs)

```python
from fleet_core import BusConfig, DeliveryMode, Envelope, FleetBus
from fleet_nats import subjects  # subjects/registry/KV remain fleet-nats-owned.
```

- `BusConfig.from_env(agent_id=...)` — connection config from the environment
  (`FLEET_NATS_URL` / `FLEET_AGENT_ID` / `FLEET_ENV`); never bake a URL in.
- `FleetBus(config)` — `connect()` / `publish(subject, envelope)` /
  `subscribe(subject, handler)` / `request(...)` / `reply(...)` / `close()` over
  the fleet-core transport. Ordinary coordination publish/subscribe is durable
  by default through pre-provisioned JetStream streams/consumers; missing durable
  provisioning fails loud with no silent core fallback. `DeliveryMode.CORE` is an
  explicit opt-out, and Heartbeat/Presence is the sole implicit ephemeral
  exception.
- `Envelope.new(msg_id=, ts=, sender=, kind=, payload=, task_id=, channel=, …)` —
  the one typed message contract; `to_bytes()` / `from_bytes()` on the wire.
- `subjects.*` — the **only** sanctioned subject builders (see below). Never
  hand-format a subject string. They live in `fleet_nats` alongside the
  registry/KV helpers and should be treated as the shared fleet bus subject
  surface.

## Subject convention — Dash parity (stated + checked)

The subject tree mirrors the live Dash convention `{env}.{domain}.{...}`
(`@example/core-nats`) — dot-delimited, env-prefixed, hierarchical — so the
fleet's Python nodes and the Dash TS nodes agree on the wire and the fleet JS
nodes **buttress** the Dash mesh rather than forking it. The layers:

| Layer                    | Builder(s)                                             | Use |
|--------------------------|--------------------------------------------------------|-----|
| `{env}.agent.<…>`        | `agent_dispatch/status/relay/wake/arbitrate/inbox`     | crew coordination (Discord-originated trio DAP) |
| `{env}.direct.<…>`       | `direct(env, agent)`                                   | local/human agents → trio, no Discord overhead |
| `{env}.human.<…>`        | (human/local-agent topics)                             | human-facing topics |
| `{env}.kboard.<…>`       | `kboard_event(env, event, task_id)`                    | k-board task lifecycle events |
| `{env}.hub.<…>`          | `hub_control(env, task_id)`, `hub_show_work(env)`      | out-of-band control plane for the manager run-loop |
| `{env}.factory.<…>`      | `factory_runner(env, job_id)`                          | publish-only telemetry from disposable runner boxes (#222). **Untrusted publisher** — app-plane only, never routed into an LLM turn (`docs/fleetbus-traffic-matrix.md`) |
| `{env}.ob.<…>`           | `ob_context_pack(env)`                                 | Open Brain memory transport (#221). OB-owned domain, granted `{env}.ob.>`; app-plane request/reply |

Two axes ride these subjects and must not be conflated (#117):

- **lane = channel scoping.** `dispatch`, `status`, `relay`, `wake`, `ack`, and
  `telemetry` each take an optional `channel=` argument, inserting the channel
  token between `agent` and the lane name. It narrows WHICH traffic a subscriber
  attaches to. Because a scoped subject has one MORE token than its bare form and
  NATS matches per-token, a subscriber that wants every channel's slice must
  attach the paired `*_wildcard` builder **as well as** the bare form — attaching
  only one is silent message loss, not a degraded mode.
- **namespace = access control.** WHO may publish/subscribe is the server-side
  credential grant. Channel scoping is **not** an authorization boundary: the
  per-channel wildcard the live grants use (`dev.agent.*.dispatch.*`) still
  matches every channel's slice.

The single-source gate applies to the whole fleet bus base, not just subject
strings: typed Pydantic spec, env/auth config, logging/redaction, default
transport, and subject builders all belong in fleet-core/fleet-nats ownership.
The `no hand-formatted subjects` check still greps that no consumer hand-formats
a `"{env}.{layer}."` string, so **every** subject goes through the shared
builders. That makes parity *maintainable in one place*, but note what it does
and does not prove: it guarantees the Python side is internally consistent; it
does **not** verify that the builders match Dash's TS `@example/core-nats`
definitions. Dash↔Python parity is a **convention kept in lockstep by hand** (a
cross-language parity check does not exist yet — worth a follow-up). If Dash adds
a domain, add the builder in the shared surface; if the two ever diverge, that is
a real drift the grep will not catch — reconcile at the convention level.

## Adapter pattern — one minimal example per consumer class

### 1. Trio coordination (the manager/worker agents)

The trio consumes fleet-nats through two packages already in this repo:
`fleet-orchestrator` (the deterministic DAP brain, driving `FleetBus`) and
`fleet-subscriber` (the per-agent Hermes ingress adapter). Neither reimplements
transport — both are adapters over `FleetBus`/`subjects`. See `docs/architecture.md`.

### 2. Open Brain (memory transport — open-brain #223)

OB has **shipped the client code and holds the grant, but no deployed worker has
the bridge enabled** — it is a provisioned consumer, not yet a live one. Stated
precisely, because this repo has been burned by repeating stale issue text:

- **Shipped.** OB's context-pack request/reply worker and its TS subject mirror
  exist (`open-brain/src/nats-worker.ts`, `open-brain/src/nats-subjects.ts`).
- **Granted.** The `open-brain` principal holds a server-side `{env}.ob.>`
  publish+subscribe grant (`deploy/nats/fleet-prod-nats-server.conf`, pinned by
  `tests/test_authz_conf.py::test_open_brain_grants_exact`).
- **Not running.** `open-brain/scripts/worker-env.ts` pins
  `OPENBRAIN_TRANSPORT: "http"` and `OPENBRAIN_NATS_ENABLE_BRIDGE: "false"`, and
  `open-brain/src/nats-runtime.ts` makes the NATS runtime available only when
  BOTH are flipped. Neither `com.rico.open-brain-nats-worker` nor the
  `com.rico.open-brain-nats` broker that
  `open-brain/docs/node-c-nats-worker-runbook.md` requires is loaded on any host,
  and that runbook still says "install only from a release that has passed the
  validation gate". No OB traffic has crossed the fleet bus.

Issue #221's body says "the NATS worker and the lane are live"; that is the stale
source and it is **not** what the deployed code does. What #221 actually closes
is the missing *builder* — the subject was being hand-formatted in TypeScript
with a `TODO(fleet-nats)` asking for a Python owner. That is fixed here. Treat
the lane as reserved-and-provisioned until a worker ships with the bridge on.

Build that subject with `subjects.ob_context_pack(env)` — **never** hand-format
`f"{env}.ob.…"`. The builder is the single source of truth; OB's TypeScript
mirror must track it, not the other way round (see the parity note below). The
pattern for any additional OB memory event:

```python
from fleet_core import BusConfig, Envelope, FleetBus
from fleet_nats import subjects

async def publish_memory_event(kind: str, task_id: str, payload: dict) -> None:
    cfg = BusConfig.from_env(agent_id="open-brain")
    bus = FleetBus(cfg)
    await bus.connect()
    try:
        subject = subjects.kboard_event(cfg.env, kind, task_id)  # or an OB-domain builder
        env = Envelope.new(
            msg_id=new_id(), ts=now_iso(), sender="open-brain",
            kind=kind, task_id=task_id, payload=payload,
        )
        await bus.publish(subject, env)
    finally:
        await bus.close()
```

If OB needs another `ob.*` subject, file a `fleet-nats` issue to add the `ob_*`
builder here — don't hand-format `f"{env}.ob.…"`. Cross-ref: **open-brain #223**.

**Python↔TypeScript parity for the `ob` layer.** OB is a TS consumer, so it keeps
a hand-maintained mirror (`open-brain/src/nats-subjects.ts`). That mirror existed
*because* this library had no `ob_*` builder to import, and it reimplemented the
slug rule to compensate — exactly the fork this document forbids. The builder now
exists and is authoritative; the mirror must match its **output** byte-for-byte.

Parity is on the produced subject, and it is **not** total on validation. Today
the two sides diverge on one rule: `subjects.slug` rejects a token containing the
multi-token wildcard `>`, while the TS mirror's `slugSubjectToken`
(`open-brain/src/nats-subjects.ts`) rejects only the empty token. So
`obContextPackSubject(">")` returns `">.ob.memory.context_pack"` in TypeScript
where `subjects.ob_context_pack(">")` raises `ValueError` in Python. Nothing
breaks today — `env` is not caller-controlled and `BusConfig` constrains it to
`dev`/`prod`/`staging` — but the `>` guard is Python-only, and closing it is
OB-side work (fleet-bus cannot enforce a rule inside OB's repo). Do not read the
"byte-for-byte" obligation as "the two validators already agree".

One parity detail is load-bearing: `ob_context_pack` **slugs the `env` token**,
unlike the `{env}.agent.…` builders, which interpolate `env` raw. That is not an
inconsistency to "fix" — it matches the live TS mirror, which slugs env. Changing
either side alone puts the two languages on different wires. The mismatch is
harmless today because `BusConfig` constrains `env` to `dev`/`prod`/`staging`,
all slug-identical. `tests/test_config_and_subjects.py` pins the whole contract
(shape, env slugging, empty-token rejection, and the `{env}.ob.>` grant prefix).

### 3. k-board (task lifecycle events)

The k-board bridge already ships in `fleet-orchestrator` (`kboard.py` +
`Orchestrator.persist_transition`): it CAS-writes the durable task status to
k-board and emits a `kboard_event(env, event, task_id)` on the bus. A standalone
k-board publisher (server-side) is the same adapter shape — build the subject via
`subjects.kboard_event`, send an `Envelope`. Cross-ref the k-board NATS bridge
cards. k-board Postgres is the source of truth; NATS delivers, it is not the record.

### 4. Dash apps (TS mesh)

Dash consumes the **same** subject convention from the TS side
(`@example/core-nats`). There is no Python adapter to write for Dash — the
contract is the shared subject/envelope shape. The obligation is **parity**: keep
`subjects.py` and the Dash TS contract in lockstep (same `{env}.{domain}.{...}`),
so a Python publisher and a TS subscriber interoperate on the wire.

### 5. Local dev (Mac / a human or local agent on the `direct.*` lane)

The Mac already talks to the bus. The formalized local-dev helper uses the
`direct(env, agent)` lane — human/local-agent → trio, with no Discord overhead:

```python
from fleet_core import BusConfig, Envelope, FleetBus
from fleet_nats import subjects

async def direct_send(to_agent: str, text: str) -> None:
    """Publish a direct message to an agent's direct lane from local dev."""
    cfg = BusConfig.from_env(agent_id="local-dev")
    bus = FleetBus(cfg)
    await bus.connect()
    try:
        subject = subjects.direct(cfg.env, to_agent)
        env = Envelope.new(
            msg_id=new_id(), ts=now_iso(), sender="local-dev",
            kind="direct", to=to_agent, payload={"text": text},
        )
        await bus.publish(subject, env)
    finally:
        await bus.close()

async def direct_listen(agent: str, handler) -> None:
    """Subscribe to this local identity's direct lane."""
    cfg = BusConfig.from_env(agent_id=agent)
    bus = FleetBus(cfg)
    await bus.connect()
    await bus.subscribe(subjects.direct(cfg.env, agent), handler)
```

(`new_id`/`now_iso` are the caller's id/timestamp sources — the `Envelope`
contract takes caller-supplied ids so published output is reproducible; see how
`fleet-orchestrator`'s `IdClock` injects them under test.)

### 6. Token-cost feed consumer (governor #13 / dashboard #14)

The per-turn token-cost feed (#27) is emitted by rtech-hermes #457 on the
`token_cost` lane. A consumer subscribes the wildcard, parses each event with the
shared strict parser, and sums per-task spend — the number the governor budgets
against. `CostLedger` does the work — parse, **dedup on `turn_id`** (app-level
retries, reconnect re-sends, or durable redelivery can still duplicate; J1/#125
owns broker `Nats-Msg-Id` dedup on scored lanes and remains open), and accumulate
— the consumer never hand-rolls the payload dict or sums blindly.

```python
from fleet_core import Envelope
from fleet_nats import subjects
from fleet_orchestrator.costfeed import CostLedger

async def watch_token_cost(bus, cfg, task_id: str) -> None:
    """Subscribe every agent's token_cost lane; track this task's spend."""
    ledger = CostLedger()  # dedups duplicate turns built-in

    async def _on_cost(env: Envelope) -> None:
        if ledger.observe(env) is None:
            return  # malformed (logged) or duplicate turn (logged)
        spent = ledger.task_total(task_id)  # total_tokens across this task
        # ... governor: compare `spent` to the budget, pause on breach

    await bus.subscribe(subjects.token_cost_wildcard(cfg.env), _on_cost)
```

The streaming face of the same math is `fleet_orchestrator.costfeed.CostLedger`
(`observe(env)` → parse/log-and-skip, `task_total`/`agent_total`) — pure and
deterministic, no clock. The payload carries **counts, never content** (see the
token-cost feed contract in `docs/architecture.md`).

## Why thin adapters (not forks)

- **One wire.** A single `Envelope` + one subject convention means any consumer
  interoperates with any other and with the Dash TS mesh — no drift.
- **One place to fix.** Spec/config/logging/transport bugs are fixed once in
  `fleet-core`; subject builders, registry helpers, and KV helpers remain in
  `fleet-nats`. Consumers inherit fixes instead of carrying forks.
- **One delivery seam.** Consumers select or override delivery mode only through
  fleet-core transport APIs. Ordinary coordination is durable by default;
  heartbeat/presence is the sole implicit ephemeral exception; explicit core
  opt-out is visible in code review.

New consumer? Import the fleet-core base, build subjects via the shared subject
helpers, send `Envelope`s, and file a library issue for any missing capability.
Never fork.
