# F2 — NATS identity & authorization posture (ratification brief)

**Status:** PROPOSED — awaiting Rico's GO. Issue: rodaddy/fleet-bus#104
(Part of Wave 0 — Foundations, #102).

This is the artifact Rico ratifies **before** the server half lands. It proposes
the fleet's per-agent-credentials + deny-by-default per-subject allow-list
posture. The client half (a bus participant presenting its own credentials at
connect) ships in the PR that carries this doc; **nothing server-side changes
until this posture is ratified.**

---

## 0. What is and is NOT in the PR that carries this brief

**IN (client half, shipped):**

- `BusConfig` gains optional per-agent credential fields
  (`FLEET_NATS_USER` / `FLEET_NATS_PASSWORD`, or a `FLEET_NATS_CREDS` `.creds`
  file path). Creds-file wins when both are set; user-without-password (or vice
  versa) fails loud at construction.
- `FleetBus.connect()` passes the resolved credentials to nats-py when
  configured, and logs the **credential source** (`none` / `creds-file` /
  `user-password`) — never the value, never the creds path.
- The unauthenticated path stays **permissive** by default (it does not fail
  closed — see §6 rollout) but logs a loud `WARNING` at every anonymous connect
  so the spoofable state is never silent. An opt-in `FLEET_NATS_REQUIRE_AUTH`
  (default off) lets a post-rollout deployment fail *closed* by choice: with it
  set, an anonymous connect raises at connect instead of warning, closing the
  "operator ships an unauthenticated agent to prod, only a WARNING reveals it"
  gap.
- `BusConfig`'s `nats_password` and `nats_creds_file` are `repr=False`, so
  `repr(cfg)`/`str(cfg)` never spills a secret value or the identity-encoding
  creds path. The creds-file path is pre-validated at construction (isfile +
  readable); a missing/unreadable path fails loud naming the **env var**, never
  the resolved path.
- Post-flip clarity: a server auth reject (surfaced by nats-py as a generic
  `errors.Error`, and — under the default `-1` reconnect — as a swallowed
  `last_error` behind FleetBus's connect timeout) is classified by message
  marker and re-raised as an actionable, secret-free `ConnectionError` naming
  the vars to set (see §6 step 5 for the finite-`FLEET_RECONNECT_MAX` caveat
  that makes it surface immediately). When both `.creds` and user/password are
  configured, a WARNING announces that creds-file wins.
- Functional tests: source resolution, kwargs threaded to connect, absence path,
  loud-warn on anonymous connect, no-secret-in-logs, repr redaction, creds-file
  pre-validation, `require_auth` fail-closed, and the override/rejection warnings.

**NOT in this PR (gated on this brief's ratification, then separate work):**

- **No NATS server config on fleet-hub** — accounts, users, per-subject
  permissions, `$SYS` operator context. (Server half of #104.)
- **No credential minting** — no `.creds` files, no passwords, no nkeys are
  generated, printed, committed, or vaulted here. This doc references creds by
  vault path / env var only.
- **No enforcement** — until the server config lands, an agent physically *can*
  still publish/subscribe anywhere. The client half is inert-but-ready: it will
  present creds the moment the server starts requiring them.
- Issue #104 does **not** close on this PR. It closes when the server-enforced
  allow-list is live and a cross-list subscribe is refused by the server.

---

## 1. The problem (receipts)

- `bus.py` `connect()` passed **no credentials**. Any process that can reach
  `nats://192.0.2.74:4222` (fleet-hub) could publish as any agent (spoof + a
  context-injection surface onto the token plane) and subscribe to everything.
- Identity today is **self-asserted**: the envelope `from` field + the display
  connection `name` (`config.py`). Neither is enforced by anything.
- No `$SYS` account ⇒ no bus observability (`connz` → HTTP 400; CLI → "ensure
  the account used has system privileges") during exactly the floods F-series is
  fixing.

## 2. Proposed posture (the two phases)

**Phase 1 — authn: dedicated per-agent NATS credentials.** Each principal that
connects to the bus (orchestrator/manager, each worker, OB, k-board publisher,
local-dev) gets its **own** vaulted NATS credential. Recommended mechanism:
per-agent `.creds` files (NATS-native JWT + nkey seed) referenced by
`FLEET_NATS_CREDS`; user/password (`FLEET_NATS_USER`/`FLEET_NATS_PASSWORD`) is
supported as the lighter-weight alternative for principals where a full operator
JWT hierarchy is overkill. **Recommended default = dedicated `.creds`.**

**Phase 2 — authz (the real lever): deny-by-default per-subject allow-lists.**
Each NATS user carries server-enforced `publish`/`subscribe` permission lists
scoped to exactly the subjects that principal's role uses. Default deny: a
subject not on a user's allow-list is refused by the server — an agent
*physically cannot* subscribe or publish outside its lane. Plus a **`$SYS`
operator context** on fleet-hub so an operator can read `connz` for bus
observability.

## 3. `$SYS` operator context on fleet-hub

- Add a `$SYS`-account operator user on fleet-hub with system privileges, so
  `connz` / `subsz` / account stats are readable during floods. Credential
  vaulted like every other; used by the operator + the fleet dashboard, never by
  an agent.
- This is **the** observability gap today (probes returned HTTP 400 / "account
  ... system privileges"). It is independent of the allow-list work and can land
  with the first server-config change.
- Out of scope for the client-half PR; it is a server-side account.

## 4. Per-agent subject allow/deny matrix (deny-by-default)

Derived from `fleet_nats.subjects` usage per component
(`docs/architecture.md` §§2–4, `docs/consumers.md`). `{env}` = `dev`|`prod`.
Everything **not** listed as allow is **denied** (server default-deny). `{self}`
= that principal's own agent id.

### 4a. Worker agent (e.g. Skippy, Bilby-as-worker) — least-capable

| Direction | Subject pattern | Builder | Why |
|---|---|---|---|
| SUB | `{env}.agent.*.dispatch.{self}` + `{env}.agent.dispatch.{self}` | `agent_dispatch_wildcard` / `agent_dispatch` | receive its own dispatches (per-channel + bare; both forms needed) |
| SUB | `{env}.agent.relay.{self}` | `agent_relay` | receive relayed context |
| SUB | `{env}.agent.wake.{self}` | `agent_wake` | receive wake nudges |
| SUB | `{env}.agent.inbox.{self}` | `agent_inbox` | direct inbox |
| SUB | `{env}.direct.{self}` | `direct` | local/human → this worker |
| SUB | `_INBOX.{self}.>` | `BusConfig.inbox_prefix` | **requester side of request/reply**: receive `claim`/`arbitrate`/`show_work` replies only under the authenticated principal's deterministic prefix |
| PUB | `{env}.agent.status.*` | `agent_status` | publish progress/complete + done sentinel (task-keyed; worker does not know task ids at grant time → the `status` prefix, not per-task) |
| PUB | `{env}.agent.claim.*` | `agent_claim` | claim a task (request/reply) |
| PUB | `{env}.agent.arbitrate.*` | `agent_arbitrate` | ask the manager to decide (request/reply) |
| PUB | `{env}.hub.show-work` | `hub_show_work` | request one redacted, pull-only work trail |
| PUB | `{env}.agent.heartbeat.{self}` | `agent_heartbeat` | own liveness only |
| PUB | `{env}.agent.token_cost.{self}` | `token_cost` | own per-turn cost only |
| **DENY** | `{env}.agent.dispatch.>` (publish) | — | a worker must never dispatch as the manager |
| **DENY** | `{env}.agent.relay.>` / `wake.>` (publish) | — | manager-only lanes |
| **DENY** | other agents' `heartbeat.{peer}` / `token_cost.{peer}` (publish) | — | no spoofing a peer's liveness/spend |
| **DENY** | `{env}.hub.control.>` (both) | — | hub-only control plane |

> **Request/reply inbox boundary.** FleetBus derives and passes
> `inbox_prefix="_INBOX.<canonical-principal>"` from the authenticated NATS user
> (or the configured canonical agent identity for creds-file deployments); nats-py adds its
> per-request suffix below that prefix. Each requester therefore subscribes only to
> `_INBOX.{self}.>`, and the committed NATS grants match that exact prefix. Identity
> values are never normalized: a creds-file deployment whose configured identity does
> not match its server grant receives a NATS authorization failure. The
> manager still uses `allow_responses: true`: the server grants one reply publish
> only to the reply subject carried by the request, including the worker's
> principal-scoped inbox.

### 4b. Manager / orchestrator (Nagatha)

| Direction | Subject pattern | Builder | Why |
|---|---|---|---|
| SUB | `{env}.agent.inbox.{self}` | `agent_inbox` | manager's own inbox |
| SUB | `{env}.agent.status.>` | `agent_status` | watch every worker's status (`watch_worker_status`) |
| SUB | `{env}.agent.claim.>` | `agent_claim` | responder side of claims |
| SUB | `{env}.agent.arbitrate.>` | `agent_arbitrate` | responder side of arbitration |
| SUB | `{env}.agent.heartbeat.*` | `agent_heartbeat_wildcard` | fleet liveness / topology |
| SUB | `{env}.agent.token_cost.*` | `token_cost_wildcard` | governor: sum spend across all agents |
| SUB | `{env}.hub.control.*` | `hub_control_wildcard` | receive out-of-band control frames (pause/resume) |
| SUB | `{env}.hub.show-work` | `hub_show_work` | hub-owned responder for redacted work trails |
| SUB | `{env}.factory.runner.*` | `factory_runner_wildcard` | observe software-factory run progress (#222). Subscribe-only and deliberately absent from the PUB list below: a manager reads runner telemetry, it never forges it. See §4h for the publishing side and the plane caveat |
| PUB | `{env}.agent.*.dispatch.*` + `{env}.agent.dispatch.*` | `agent_dispatch` | fan tasks out to workers |
| PUB | `{env}.agent.relay.*` | `agent_relay` | relay missed context (the reason the bus exists) |
| PUB | `{env}.agent.wake.*` | `agent_wake` | nudge quiet workers |
| PUB | `{env}.agent.dead-letter` | `dead_letter` | sanitized unprocessable-envelope records |
| PUB | `{env}.kboard.task.*.*` | `kboard_event` | k-board lifecycle emit (orchestrator kboard bridge) |
| PUB | `{env}.agent.heartbeat.{self}` / `token_cost.{self}` | — | own liveness/cost |
| PUB | `allow_responses: true` (server permission, not a subject) | — (nats-py) | **responder side of request/reply**: lets the manager reply to a worker's `claim`/`arbitrate` on the requester's dynamic `_INBOX` reply subject (`bus.py` `reply()` → `nc.publish(msg.reply, …)`). Preferred over a static `_INBOX.>` publish grant — the server auto-scopes a one-shot publish to exactly the reply subject of a request it received |
| **DENY** | `{env}.agent.status.{task}` (publish) | — | status is the worker's voice; manager reads, never writes it |

### 4c. Hub / control-plane publisher (harness, Nagatha-side steer — #59)

| Direction | Subject pattern | Builder | Why |
|---|---|---|---|
| PUB | `{env}.hub.control.*` | `hub_control` | publish pause/resume control frames |
| **DENY** | everything else | — | control-only principal |

### 4d. Open Brain (memory transport — open-brain #223, not yet a live consumer)

| Direction | Subject pattern | Builder | Why |
|---|---|---|---|
| PUB/SUB | its own `ob.*` (or `kboard`) memory lanes | `kboard_event` / a future `ob_*` builder | memory events only |
| **DENY** | `{env}.agent.*` coordination lanes | — | OB is not a coordination participant |

### 4e. k-board publisher (server-side)

| Direction | Subject pattern | Builder | Why |
|---|---|---|---|
| PUB | `{env}.kboard.task.*.*` | `kboard_event` | task lifecycle events |
| **DENY** | `{env}.agent.*` coordination lanes | — | k-board delivers events; it is not a coordinator |

### 4h. Software-factory runner boxes (`fleet-runner`, #222)

| Direction | Subject pattern | Builder | Why |
|---|---|---|---|
| PUB | `{env}.factory.runner.*` | `factory_runner` | live run progress from a runner box |
| **DENY** (explicit `>`) | everything else, both accounts | — | a runner must never RECEIVE an instruction from the bus |

The deny here is **explicit**, not implicit. A runner box is a disposable
privileged LXC clone running a coding agent with `bypassPermissions` /
`sandbox_mode=danger-full-access` over untrusted repository content, so it is
the one principal most likely to try. Two consequences:

- **The server-side deny is the boundary.** `fleet_subscriber`'s
  `CODE_PUBLISH_ONLY_ROLES` / `is_publish_only()` stop *our* subscriber from
  attaching lanes, but cannot stop a process that holds a connection from
  calling `nc.subscribe()` itself. Only the NATS permission does.
- **An omitted `subscribe` direction is ALLOW-ALL in NATS**, i.e. silently the
  exact opposite of the intent — the same gotcha called out for `fleet-hub-control`
  and `fleet-kboard`. `tests/test_authz_conf.py::test_runner_grants_exact` pins
  `sub_deny={">"}`, and the live-server test proves a real runner credential is
  refused on dispatch/relay/wake/inbox/token_cost/hub.control/`>`.

One shared principal, not one per job: runner boxes are created and destroyed
per job, so a per-job credential would mean minting on the hot path. Confinement
does not depend on job identity — every runner is equally untrusted.

Dispatch reaches a runner over the durable Postgres path (job row + atomic claim
+ `mcp2cli runner_run`), never over NATS: per C3 the core bus is at-most-once
with no retention, so a dispatch published before a subscriber attached is
silently lost.

### 4f. Local-dev (Mac / human on the `direct.*` lane)

| Direction | Subject pattern | Builder | Why |
|---|---|---|---|
| PUB | `{env}.direct.*` | `direct` | human/local → an agent's direct lane |
| SUB | `{env}.direct.{self}` | `direct` | replies to this local identity |
| **DENY** | `{env}.agent.dispatch/relay/wake/status.>` | — | local-dev is not the manager and not a worker |

### 4g. `$SYS` operator

| Direction | Subject pattern | Why |
|---|---|---|
| SUB | `$SYS.>` | read `connz`/`subsz`/account events for observability |
| **DENY** | all `{env}.*` app subjects | operator watches the system account, never the app plane |

## 5. Rotation story

- **Storage:** all credentials in Vaultwarden (fleet convention); injected as env
  at runtime (`FLEET_NATS_CREDS` path or `FLEET_NATS_USER`/`_PASSWORD`). Never
  baked into the library, an image, or git.
- **Rotation unit = one principal.** Because every agent has its *own* credential
  (this is the whole point vs. a shared token), a compromised or expired
  credential is rotated for that one agent without touching any other.
  **Exception — `fleet-runner` (§4h) is one principal shared by every runner
  box**, because boxes are created and destroyed per job and a per-job
  credential would mean minting on the hot path. So rotating it cycles the
  credential for *all* runners at once, and a single compromised box burns it
  for every other. That is an accepted trade: confinement does not depend on
  job identity (every runner is equally untrusted and equally publish-only),
  and the seat can publish exactly one subject. The cost is no per-job
  attribution from the credential — attribution comes from `job_id` in the
  subject, which is **not** authenticated.
- **On-host `.creds` path convention:** the canonical location is
  `/etc/fleet-bus/creds/<agent>.creds` (e.g. `/etc/fleet-bus/creds/nagatha.creds`),
  owner = the agent's service user, **mode `0600`** (readable only by that user).
  `FLEET_NATS_CREDS` points at exactly this path. Keeping every agent's creds at a
  stable, per-agent path is what makes the "swap at the same path" hot-swap below
  work without touching the unit file.
- **`.creds` (JWT + nkey) rotation:** re-issue the user JWT (short-to-medium TTL),
  update the vault entry, redeploy that agent's env. `user_credentials` is a
  file path, so a hot-swap is "write new file at the same path (see convention
  above), reconnect."
- **user/password rotation:** update the server user + the vault entry, redeploy
  the agent. Coarser than JWT; that is why `.creds` is the recommended default.
  **Caution — rotate only during a restart window:** nats-py captures the
  user/password *once at connect* and reuses it verbatim on every auto-reconnect,
  so a server-side password rotation while the agent is up causes its next
  reconnect to fail authorization until the agent is redeployed with the new
  value. `.creds` does not have this hazard — nats-py re-reads the file per
  handshake, so a same-path swap survives a live reconnect. This asymmetry is a
  further reason `.creds` is the recommended default.
- **No dual-domain coupling:** because these are dedicated NATS credentials (not
  the OB token — see §7), a NATS rotation is a NATS-only event. It cannot break
  or expose OB, and an OB rotation cannot break the bus.

## 6. Rollout order (mixed-fleet safe, fail-closed)

The fleet is mixed-version (per `pyproject` notes: Nagatha on 3.13, Bilby/Skippy
rollout pending). Enforcement must not take the fleet offline. Mixed-fleet
safety comes from order, never an anonymous server seat. Ordered:

1. **Ship the client half (shipped in PR #140).** Every `FleetBus` can present creds but the
   server does not require them yet. No behavior change: absent creds → anonymous
   connect + loud WARNING. Fully backward compatible.
2. **Mint + vault per-agent creds and distribute them to every live agent.** No server
   enforcement yet; distribute `FLEET_NATS_CREDS` to each agent's env. Agents now
   connect *with* creds against a server that still allows anonymous — still
   mixed-safe.
3. **Add the `$SYS` operator context** on fleet-hub (independent; unblocks
   observability immediately).
4. **Install the committed fail-closed config** with
   `deploy/nats/provision-fleet-auth.sh install` only after every live agent is
   confirmed credentialed. The materialized EnvironmentFile records each
   expected public username beside its password; check/install re-fetch the
   referenced Vault item and validate each pair as one unit, rejecting stale
   password-only files, username drift, or password drift.
   Before mutation, install also proves the unit's effective `ExecStart`
   references the configured destination; after restart, a mandatory `curl`
   health probe must pass or the prior config and drop-in are restored.
   There is no `no_auth_user` or default-permit account. This install is the
   enforcement moment; verify every principal and grant via `$SYS` `connz`.

   > **Post-flip failure legibility — reconnect caveat.** nats-py surfaces an
   > *initial-connect* auth reject as a generic `errors.Error("nats:
   > Authorization Violation")`, never a typed `AuthorizationError` (that type is
   > only raised in its read loop). Under the fleet default
   > `max_reconnect_attempts=-1` (`FLEET_RECONNECT_MAX`), nats-py never discards
   > the rejecting server: it swallows the error into an infinite connect-retry
   > loop, so `FleetBus.connect()`'s own timeout budget fires first and the reject
   > would otherwise present as a spurious "server unreachable" timeout.
   > `FleetBus` compensates by classifying the reject from `nc.last_error`
   > (message-marker match) and raising an actionable, secret-free
   > `ConnectionError` — but for the reject to surface *immediately* rather than
   > after the timeout budget, set a **finite `FLEET_RECONNECT_MAX`** on agents
   > during the flip window. Note nats-py does **not** re-raise the auth error
   > under a finite limit either: it records the violation on `nc.last_error` and,
   > once its bounded attempts are spent, raises a generic `NoServersError`. So
   > `FleetBus` classifies from `nc.last_error` on *both* paths — the `-1` timeout
   > path and the finite `NoServersError` path — and raises the same actionable
   > `ConnectionError`; the finite limit's benefit is that the error surfaces at
   > once (after N attempts) instead of waiting the full connect-timeout budget.
   > A finite `FLEET_RECONNECT_MAX` during rollout also stops an agent from
   > silently hammering a now-rejecting server forever.
5. **Close #104** when a cross-list subscribe is server-refused and `$SYS` reads
   `connz`.

The client-half decision to fail *permissive-but-loud* (not fail-closed) is
deliberate: it lets steps 1–3 run against a mixed fleet without an outage, and
the loud WARNING guarantees the pre-enforcement window is never invisible.

## 7. OB-token reuse — REJECTED (reasons)

The operator-suggested stopgap of **reusing the Open Brain token** as the NATS
credential is rejected (codex review, carried in #104). Reasons:

1. **Couples two security domains.** A leak or rotation in *either* OB or the bus
   would silently break or expose the *other*. Two systems that must be
   independently rotatable would become entangled.
2. **No per-agent identity.** A single shared token is **one principal** — it
   defeats the entire point of F2, which is that each agent has its own
   enforceable identity and allow-list. With a shared token there is nothing to
   scope a per-subject allow-list *to*.
3. **Zero effort saved.** Dedicated per-agent NATS creds are the *same* effort to
   provision as wiring the OB token through — minting a NATS user is cheap — but
   without the coupling or the loss of identity.

Therefore: **dedicated per-agent NATS credentials only.** OB-token reuse is not
implemented, not offered as a fallback, and must not be added later.

---

## What Rico ratifies

Ratifying this brief = GO on: (a) **dedicated per-agent NATS credentials** (default
`.creds`, user/password allowed), (b) **deny-by-default per-subject allow-lists**
shaped per §4, (c) a **`$SYS` operator context** on fleet-hub, and (d) the
**mixed-fleet-safe rollout order** in §6 (credential distribution before the
single fail-closed install; no anonymous seat). It also affirms **OB-token reuse
stays rejected** (§7). The committed server config and provisioning tooling are
the reproducible server half; credential minting, distribution, live install,
and acceptance remain separate maintainer-gated operations.
