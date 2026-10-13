# Deploy runbook — enable the subscriber across the trio (#8)

> **Show-work service.** `show_work` is an authenticated NATS request/reply tool:
> subscribers never read or mount the hub journal. The hub responder renders a
> redacted bounded trail; applying the updated NATS authorization/deploy config
> to a live hub remains a separate, explicitly authorized operation.

How to enable the fleet-bus NATS subscriber on each trio agent — **Nagatha**
(manager), **Skippy** and **Bilby** (workers) — with the right identity, role,
and subjects, **without regressing safety**. Verification is by **effective
state** (probe the running agent), never by trusting a config file.

> **Live-deploy authorization.** Enabling the subscriber changes config on shared
> agent hosts (bilby-host / skippy-host / nagatha-host) and restarts the gateway. That is a
> maintainer-gated action: it goes through the front door
> (`restore-customizations --agent <name> --apply`), with a **backup + ledger
> entry before any live change**, and never raw `ssh` config edits.
> **Provisioning is decoupled from this gate**: installing the packages
> (self-provisioning section below) may happen any time, on any host, because
> it changes no agent config and restarts nothing. Only the *enable* — config
> + gateway restart — is maintainer-authorized. Do not run the apply step
> below without that authorization.

## Host table (confirm live before deploy — tables drift)

| Agent   | Host  | Role    | Subscribed lanes (effective)                    |
|---------|-------|---------|-------------------------------------------------|
| Bilby   | bilby-host | worker  | `dispatch` / `relay` / `wake` / `inbox`         |
| Skippy  | skippy-host | worker  | `dispatch` / `relay` / `wake` / `inbox`         |
| Nagatha | nagatha-host | manager | `inbox` (adapter); status/arbitrate via the orchestrator |

All three are on Python 3.13 with `nats-py>=2.15` already (fleet standard,
rtech-hermes #338) — **no venv work needed** to enable the subscriber.

## Per-agent config surface (env-driven)

The subscriber reads its identity and role entirely from the environment
(`fleet_nats.config.BusConfig.from_env` + `fleet_subscriber.role.SubscriberRole`):

| Env var          | Meaning                                  | Bilby         | Skippy        | Nagatha       |
|------------------|------------------------------------------|---------------|---------------|---------------|
| `FLEET_NATS_URL` | NATS server                              | `nats://192.0.2.74:4222` | (same) | (same) |
| `FLEET_AGENT_ID` | this agent's identity                    | `bilby`       | `skippy`      | `nagatha`     |
| `FLEET_ENV`      | subject env prefix (`dev`/`prod`)        | `dev`         | `dev`         | `dev`         |
| `FLEET_ROLE`     | `worker` \| `manager` (unset ⇒ `worker`) | `worker`      | `worker`      | `manager`     |
| `FLEET_SUBJECT_CATALOG_MIN_VERSION` | subject-catalog floor; **refuses to start below it** (`0`/unset ⇒ enforcement OFF, fail-open) — see [Catalog-version enforcement](#catalog-version-enforcement-floor--refuse-to-start-103) | `0` | `0` | `0` |

**`FLEET_SUBJECT_CATALOG_MIN_VERSION` must be consistent on the agent hosts AND
the hub (fleet-hub).** The subscriber reads this floor too, so a floored hub with
un-floored workers (or vice-versa) is a **half-enforced** state: one population
refuses a stale catalog while the other silently accepts it — exactly the
mixed-taxonomy split the floor exists to prevent. Set the same value everywhere,
and only after the endpoint serves it (see the enforcement section below). Until
then, leave it `0` on all three agents and the hub.

**Do not set `FLEET_NATS_NAME`.** The connection name defaults to
`fleet-nats-<agent>`, which the effective-state probe greps for on `/connz`.
Overriding `FLEET_NATS_NAME` would rename the connection and make the probe miss
it (a false FAIL). Leave it unset on all three agents.

`FLEET_ROLE` selects the subscription lanes (`worker` → the four work lanes;
`manager` → `inbox` only, because the task-keyed `status`/`arbitrate` lanes are
owned by the orchestrator, not the adapter). An unknown `FLEET_ROLE` **fails
loud** — it never silently falls back. `FLEET_ROLE` unset defaults to `worker`
(least-capable): a missed var can never silently promote a worker to coordinator.
The symmetric risk — Nagatha coming up as a `worker` if her `FLEET_ROLE=manager`
is missed — fails *safe* (no privilege gain) but *silent* (wrong topology), so
the verification below explicitly checks Nagatha's **effective** role.

> **Enabling the subscriber ≠ Nagatha coordinating.** Enabling the subscriber
> makes an agent's *ingress* live (it receives its lanes). For Nagatha the
> manager, the coordination behavior — handling worker `status` and serving
> `arbitrate` — is owned by the **orchestrator run-loop**, which subscribes those
> task-keyed lanes per task. That loop is **Phase 9**, not Phase 7. So enabling
> Nagatha's subscriber alone makes her reachable, but she is not a functioning
> coordinator until the Phase-9 orchestrator loop runs. Do not read a connected
> Nagatha subscriber as "manager working."

### `FLEET_VOICE_TEE_*` — optional Discord voice tee (#55, default OFF)

By default the subscriber publishes a bus-driven turn's reply **only** onto the
NATS `agent_status` lane (coordination plumbing); the agent's visible voice never
reaches a real Discord channel. The **voice tee** is an opt-in mirror that also
delivers the visible reply text to a Discord channel through the gateway's *own*
in-process Discord adapter (the sanctioned `gateway.run._gateway_runner_ref` →
`runner.adapters.get(Platform.DISCORD)` → `adapter.send` seam — no new HTTP
client, no webhooks).

| Env var                     | Meaning                                            | Default   |
|-----------------------------|----------------------------------------------------|-----------|
| `FLEET_VOICE_TEE_ENABLED`   | switch the tee on (`1`/`true`/`yes`/`on`)          | off       |
| `FLEET_VOICE_TEE_CHANNEL_ID`| **RETIRED (fleet-bus #118)**, kept as a transitional bridge — see below. Remove it once `FLEET_CHANNEL_MAP` (or the k-board table) is live. | (unset) |
| `FLEET_CHANNEL_MAP`         | the portal map the tee resolves through — see [Channel map](#channel-map-slug--discord-id-118) | (unset) |

- **The mirror target is the TURN's channel, resolved through the persisted
  channel map** (fleet-bus #118), not one env-configured room. Before #118 the tee
  posted every reply to `FLEET_VOICE_TEE_CHANNEL_ID`, so a host bridged exactly
  one channel and an `arena` reply could land in `fleet-ops`. See
  [Channel map](#channel-map-slug--discord-id-118).
- The tee fires only when **enabled AND the channel map holds at least one
  portal** (`active`). Missing or junk `FLEET_VOICE_TEE_ENABLED` reads as **off** —
  a misconfigured tee never makes a Discord call. The connect line logs
  `tee_active=` alongside `tee_enabled=`, and an enabled-but-inert tee emits a
  distinct `WARNING` naming the map source, so "switched on but can never fire"
  is visible at startup rather than as silence.
- **Migration bridge.** Because the load ladder lands on the empty map on a host
  that sets only `FLEET_SUBJECT_REGISTRY_URL` (the live posture — `/api/channel-map`
  404s), a still-set `FLEET_VOICE_TEE_CHANNEL_ID` is honoured as a **single
  portal for the agent's own home channel** when — and only when — the loaded map
  is empty. That keeps the mirror working across the cutover instead of silently
  going dark. It cannot resurrect the #118 defect: exactly one slug maps, so every
  OTHER channel still resolves to nothing and is skipped rather than mis-routed
  into the legacy room. It is a loud `WARNING`, and a non-empty map wins outright.
- A channel with **no portal is NOT mirrored** (a loud `WARNING`); the tee never
  falls back to another room. The bus path — status reply and done sentinel — is
  unaffected, so a missing portal costs visibility, never coordination.
- The tee copy is the **visible** reply: the `[[done:…]]` completion trailer is
  stripped from the Discord copy (as it is from the bus copy). Nothing else is
  stripped.
- The tee is **best-effort and never breaks the bus path**: any failure (no live
  gateway, Discord not connected, a send error) is a loud `WARNING` and swallowed;
  the bus publish and the `done` sentinel are emitted regardless.

### `allow_bots` — the safety line (env-only, never `none`)

- `allow_bots` stays **env-only** and is **never `none`**: a worker needs at
  least `mentions`; Nagatha (manager) may be `all`.
- **Do NOT** put `allow_bots` into the config→env bridge — it is set directly in
  the agent's environment, not derived from config, so a config restore can never
  silently drop it to `none`.

## Self-provisioning install (agents install what they need, when they need it)

`scripts/install-agent.sh` provisions everything the subscriber needs on a host,
idempotently, with no interactive steps — a dedicated venv at
`$FLEET_BUS_HOME/.venv` (default `/opt/fleet-bus`) with `fleet-nats` +
`fleet-subscriber` installed and import-verified. Re-running upgrades in place.

Source resolution (first match wins): `FLEET_BUS_SRC` → the collab mount
(`/srv/shared/fleet-bus`) → the checkout the script runs from.
Hosts without GitHub credentials or a collab mount get the tree shipped as a
tarball over ssh stdin (never nested-quoted commands):

```
sha="$(git rev-parse HEAD)"          # run from a CLEAN checkout — see below
printf '%s\n' "$sha" > .fleet-release-sha    # ships the identity WITH the bytes
tar czf - --exclude '.venv' --exclude '__pycache__' --exclude '.git' \
    packages scripts pyproject.toml .fleet-release-sha \
  | ssh root@<host> 'rm -rf /opt/fleet-bus/src.new && mkdir -p /opt/fleet-bus/src.new \
      && tar xzf - -C /opt/fleet-bus/src.new \
      && rm -rf /opt/fleet-bus/src.prev \
      && { [ -d /opt/fleet-bus/src ] && mv /opt/fleet-bus/src /opt/fleet-bus/src.prev || true; } \
      && mv /opt/fleet-bus/src.new /opt/fleet-bus/src'
ssh root@<host> "PATH=\$HOME/.local/bin:\$PATH FLEET_RELEASE_SHA=$sha FLEET_BUS_SRC=/opt/fleet-bus/src bash /opt/fleet-bus/src/scripts/install-agent.sh"
```

> **Extract into a NEW directory, then swap — never `tar x` over the live tree.**
> `tar` extracts *over* an existing directory without deleting paths the new
> release dropped, and because `FLEET_BUS_SRC` then equals the install dest, the
> installer takes its `source already in place (skipping sync)` branch and its
> `rsync --delete` never runs. Ship release B over release A that way and a file
> B deleted is still sitting there, still importable by the editable install —
> while the receipt says B. That is a confident identity for bytes that are not
> that release: worse than reporting nothing, and exactly the drift #203/#225
> exist to expose. The swap above is what makes the extracted tree the WHOLE
> tree. (`src.prev` is kept for one cycle as a rollback; it costs one tree.)
>
> The `.fleet-release-sha` line is not redundant with `FLEET_RELEASE_SHA` below:
> the env var identifies THIS install run, the shipped receipt survives on the
> host so a later re-run of the installer with no env var still reports the right
> sha instead of degrading to `null`.

**`FLEET_RELEASE_SHA` is not optional bookkeeping.** The tarball excludes
`.git`, so `/opt/fleet-bus/src` on the host has no git metadata and the
installer cannot derive the sha itself. Without it the install still succeeds,
but it logs `WARNING: no usable sha` and the agent's heartbeat reports
`package_version: null` forever — install drift across the fleet becomes
invisible, which is the exact failure #203/#225 exist to prevent. Pass the sha
of the checkout you tarred, and tar only a clean checkout: the tarball ships
your **working tree**, so a sha from a dirty tree describes bytes the host is
not running. "Clean" here means **including untracked files** — `git status`
without `--untracked-files=no`. An untracked `.py` under `packages/` is copied
by the installer and shipped by the tarball, but contributes nothing to HEAD, so
a tree that is "clean" only in the tracked sense still stamps a sha that does
not describe the deployed bytes.

The installer resolves the sha from, first match wins: `FLEET_RELEASE_SHA` → a
`.fleet-release-sha` file already in the source tree → the git HEAD of
`$FLEET_BUS_SRC`, and only when that path is the work-tree **root** and clean.

> The in-tree receipt only reaches the host if you **name it in the tar**, as
> the command above does. It is at the tree ROOT, and the documented archive
> selects `packages scripts pyproject.toml` — a root dotfile is not inside any
> of those, so the older command silently shipped no receipt and the host
> depended entirely on the env var. Verified by running the documented archive:
> `ROOT_RECEIPT_SURVIVED_DOCUMENTED_TAR=no`.

It writes the sha to
`/opt/fleet-bus/src/.fleet-release-sha`, which the subscriber reads at connect.
Verify after installing:

```
ssh root@<host> 'cat /opt/fleet-bus/src/.fleet-release-sha'
```

Hub releases need none of this: `deploy-hub-release.sh` writes the same receipt
into each immutable release dir and the runtime finds it through `current`.

Installing is NOT enabling: the install puts the packages on disk; an agent's
gateway starts coordinating only via the enable procedure below (config +
front-door apply). The two are deliberately decoupled so provisioning can happen
any time without touching agent behavior.

### Headless test dummy (install verification without a gateway)

`python -m fleet_subscriber.dummy` runs a gateway-free echo subscriber under the
installed venv: it connects as `FLEET_AGENT_ID`, subscribes the same role lanes
as the real adapter, echoes every envelope as a `status` reply, and completes a
`dispatch` with the real done sentinel. Use it to prove an install end-to-end on
any host (it is a test instrument, not an agent — no turns, no Discord):

```
FLEET_NATS_URL=nats://192.0.2.74:4222 FLEET_AGENT_ID=dummy-<host> FLEET_ENV=dev \
    /opt/fleet-bus/.venv/bin/python -m fleet_subscriber.dummy
```

Verify: `fleet-nats-dummy-<host>` appears on the monitor's `/connz`
(`http://192.0.2.74:8223` — see Troubleshooting for the 8222-vs-8223 trap), and
a dispatch published to its lane comes back as echo + done. Verified live on
fleet-hub (fleet-hub) 2026-07-07.

## Enable procedure (per agent — front door only)

For each agent, in the maintainer-authorized deploy window:

1. **Back up + ledger** the agent's current customizations before any change
   (per rtech-hermes `docs/coding-standards.md`). No change without a backup and
   a ledger entry.
2. Set the agent's env: `FLEET_NATS_URL`, `FLEET_AGENT_ID`, `FLEET_ENV`,
   `FLEET_ROLE` (table above), and set `allow_bots` to `mentions` (or `all` for
   Nagatha), **not** `none`. (Verification of the *effective* value is step 6 —
   setting it here is not proof it took.)
3. Register the plugin (already shipped: `fleet_subscriber` exposes `register`
   via its `plugin.yaml`) so the gateway loads the `nats` platform on start.
4. Apply through the **front door**, never raw ssh:
   ```
   restore-customizations --agent <bilby|skippy|nagatha> --apply
   ```
5. Restart/reload the gateway per the front-door apply's own procedure (not a
   hand-rolled process kill).
6. **Verify effective state** (next section) — a config file present is not
   "enabled"; the running agent must report the subscriber connected, its
   effective `allow_bots` ≠ `none`, and (Nagatha) its effective role = `manager`.

## Verification — effective state, not config assumption

Three effective-state checks, each read from the **running system**, never from
the env you just set (setting a var is not proof it took):

**1. Subscriber connected (bus probe).** Run from a host that can reach the NATS
monitor:

```
bash specs/probe-trio-subscribers.sh
```

It queries the NATS monitor `/connz` for each `fleet-nats-<agent>` connection and
prints one line per agent. **A `SKIP` line is NOT acceptance** — it means the
monitor was unreachable (off-network / not deployed), so the probe could not
verify anything. Phase-7 live evidence is a **`PASS`** line for all three agents
from an **on-network** run; a `SKIP` from the build host proves nothing and must
not be recorded as the gate passing.

**2. Effective `allow_bots` ≠ `none` (per-host gateway read).** A bus probe cannot
see a gateway's `allow_bots` — it is the gateway's resolved config. Read the
**running gateway's effective** `allow_bots` on each host (the gateway's own
resolved-config surface, not the raw env), and confirm it is `mentions` (or `all`
for Nagatha), never `none`. Reading the env you set in step 2 is config-assumption
and does not count.

**3. Effective role = `manager` (Nagatha only).** Confirm Nagatha came up as the
manager, not a silently-defaulted worker: grep her subscriber's `connect` log for
the resolved role line — the adapter logs one `subscribed subject=… lane=… role=…`
line per lane plus `ready agent=nagatha … role=manager`. A manager logs a single
`lane=inbox` subscription and `role=manager`; a mis-defaulted worker would log
`lane=dispatch`/`relay`/`wake` and `role=worker`. (The connection log is the
authoritative per-lane evidence; NATS `/connz` does **not** return per-connection
subscription subjects without `?subs=1`, so the default probe response cannot show
lanes — use the log.)

All three must hold for the trio to be considered enabled.

Until the live enable is authorized and applied, the probe reports the trio as
**not yet enabled** (a truthful non-pass), never a false green.

## Rollback

If an agent misbehaves after enable:

1. **Restore from the pre-change backup** taken in step 1 (front door), and add a
   ledger entry noting the rollback and why.
2. Re-apply via `restore-customizations --agent <name> --apply` to return the
   agent to its backed-up state; restart/reload per the front-door procedure.
3. Re-verify the agent's effective state with **all three** checks from the
   Verification section: the probe (`specs/probe-trio-subscribers.sh`) for the
   subscriber connection, the per-host gateway read for `allow_bots ≠ none`, and
   (Nagatha) the log check for `role=manager`. The probe alone does **not** cover
   `allow_bots` — it only sees the bus connection — so the gateway read is a
   separate, required step on rollback too.

## Update survival

The subscriber is built for **update survival**: config lives in the agent's env
plus the backed-up customizations, so a gateway update does not silently drop it.
After any gateway update, re-apply through the front door
(`restore-customizations --agent <name> --apply`) to restore the enabled state
deterministically, then re-run `specs/probe-trio-subscribers.sh` to confirm the
subscriber reconnected. Nothing about the enabled state depends on a hand-edit
that an update would clobber — the update-survival property is the whole reason
config is env + front-door, not ad-hoc.

## Troubleshooting

Symptom → likely cause → what to check. All checks are effective-state reads of
the running system, never config-file assumptions.

### Bus unreachable

An agent logs `fleetbus.connect | timed out` / `unreachable`, or the probe SKIPs.

- Confirm the NATS server is up and reachable: the client port
  `nats://192.0.2.74:4222` (fleet-hub) and the monitor `http://192.0.2.74:8223`
  (`http_port` in the server config — 8222 on this server is the WEBSOCKET
  listener, which answers HTTP with `400 Bad Request`; don't probe it).
- From the agent host, confirm network reachability to that IP/port (the fleet
  segment) — the agent may be on a segment that can't route to fleet-hub.
- Check `FLEET_NATS_URL` on the agent points at the live server (not a stale/dev
  URL). The library retries reconnect forever after a first connect, but the
  *initial* connect is time-bounded and will fail loud if the server is down.

### Subscriber not connecting

`connect()` returns false / no `fleet_subscriber.connect | ready` log line.

- Grep the agent's log for `fleet_subscriber.connect | bus connect failed` — the
  reason is logged (never silent).
- Confirm the plugin registered: the gateway should log
  `fleet_subscriber.register | NATS platform registered` on start. If absent, the
  plugin wasn't loaded — re-check the front-door apply and the `plugin.yaml`.
- Confirm `FLEET_AGENT_ID` / `FLEET_ENV` are set — a wrong id subscribes the wrong
  lanes; a wrong env prefixes the wrong subjects.
- Verify on the bus: `specs/probe-trio-subscribers.sh` (a PASS line, not a SKIP).

### Relay not firing (a worker forgot to hand off but no relay went out)

The orchestrator's `watch_worker_status` fires a relay only when a worker's status
has no `@peer` mention. If relays aren't firing:

- Confirm the manager's orchestrator loop is running and subscribed to
  `agent_status(env, task_id)` for the task — the relay is driven from that
  subscription, and (as of Phase 7) that loop is Phase 9, so an
  ingress-only-enabled Nagatha will not relay yet.
- Confirm the worker's status actually omits the peer mention — a status that
  *does* `@peer` correctly suppresses the relay (that is the intended behavior,
  not a bug).
- Check the orchestrator log for `orchestrator.relay | subject=…` (fired) vs a
  `no target; refusing to relay` guard line.

### CAS conflicts on k-board

`kboard.update_status | CAS conflict … refetching`, or a `KboardConflictError`.

- A single logged conflict that resolves on refetch is **normal** — another writer
  moved the task first; the client refetches the new `sync_version` and retries.
- A `KboardConflictError` after the retry budget means *sustained* contention on
  one task — check whether two writers are updating the same task in a tight loop.
- Correctness depends on the k-board server honoring `expected_sync_version`
  (rejecting a stale PATCH). If lost updates appear despite no error, verify the
  server is enforcing the CAS check, not ignoring the field.

### Dead-letter spike (`dev.agent.dead-letter` filling up)

Watch the lane: `nats sub 'dev.agent.dead-letter'` (or `prod.…`). Each record is
sanitized metadata only — `reason`, `source_subject`, `source_id`, `source_kind`,
`source_ts` — never the raw payload.

- **A trickle is signal working as designed:** one malformed envelope was
  refused and recorded; the subscriber stayed up. Check the `reason` and fix
  the publisher.
- **A spike from one `source_subject`** means a publisher is emitting a shape
  the consumer can't map — usually schema drift after a partial deploy. Compare
  the two sides' `fleet-nats` versions first.
- **A spike across many subjects** points at the subscriber host (bad deploy,
  broken venv) — re-run the verification section's effective-state checks.
- The reporter never retries: a dead-letter record that failed to publish is in
  the agent's log (`fleet_subscriber.dead_letter | publish failed`), so a quiet
  lane with loud logs means the bus itself was down at fault time.

### Task BLOCKED by the offline ladder

`orchestrator.offline | task=… BLOCKED worker=… surfaced to k-board` — the
worker stayed silent through wake → redispatch → blocked
(`FLEET_DISPATCH_TIMEOUT_SECONDS` per rung, default 300).

Each worker has an independent offline ladder. The **first** worker to exhaust
its ladder blocks the shared task immediately; the orchestrator does not wait
for every assigned worker to exhaust before writing `blocked`.

1. Check the worker's heartbeat lane (`dev.agent.heartbeat.<agent>`): beating
   but not working points at its gateway turn path; not beating at all points
   at the subscriber/host (probe + front-door checks per Verification).
2. The task's k-board card carries the BLOCKED event with the full reason —
   after fixing the worker, redispatch by re-posting the dispatch (a fresh
   envelope id passes the dedup guard by design).

## Governor — back-channel budget tuning + resume (#13)

The orchestrator pauses a task whose coordination chatter exceeds either cap:

| Env var | Meaning | Default |
|---|---|---|
| `FLEET_BUDGET_MSGS` | coordination envelopes per task | `200` |
| `FLEET_BUDGET_TOKENS` | token spend per task (#27 feed) | `150000` |

Both are validated fail-loud (a typo'd cap refuses to start, never silently
becomes the default). The defaults are conservative guesses — tune them from
**observed traffic** once the dashboard (#14) unparks; do not hand-wave them up.

The message cap counts inside `Orchestrator.apply()` (a task's own `done`
sentinel is exempt — finishing is the budget's goal). The token cap enforces
via `Orchestrator.watch_token_costs()`, which feeds the #27 cost lane into the
governor's ledger (turn_id-deduped) and re-checks after every first-seen turn —
start that watch alongside the status watch or the token cap is inert.

**On breach** the task is PAUSED deterministically: further dispatch/relay/wake
for that task gate to NOOP, a narration beat surfaces the pause through the
manager, and the k-board card gets a paused-over-budget event (the durable
"why did coordination stop" record). Sample narration line (templated, no
payload prose): `[t-42] PAUSED over budget (msgs 201 > cap 200) — human resume
required.`

**Resume procedure (explicit human act):**
1. Read the k-board card's pause event and decide: raise the cap (env change +
   restart), split the task, or intervene in the coordination.
2. Resume the LIVE governor over the CONTROL PLANE (#59) — no restart, no host
   shell needed. Publish a `control` frame on `dev.hub.control.<task_id>`
   (build it with `fleet_nats.control.control_envelope`, or one call via
   `fleet_nats.control.publish_control(bus, env=..., task_id=..., sender=...)`).
   The hub subscribes `dev.hub.control.*` and calls the running
   `Governor.resume` on the frame. Ops (closed set):
   - `{"op":"resume","task_id":...}` — clear the pause; gated wake/dispatch/relay
     flow again. Optional `"budget_msgs_add": N` raises THIS task's message cap
     by exactly N (bounded, per-task — never the shared cap or another task).
   - `{"op":"pause","task_id":...}` — gate a task on demand (symmetry).
   - Optional `"reason":"..."` on either op — the audited WHY of the steer,
     length-capped and omitted when blank (a reason-less frame is byte-identical
     to a pre-`reason` frame — full backward compat).
   An unknown op or malformed frame is a loud no-op in the hub log
   (`orchestrator.control | ignored ...`), never a crash. The resume/pause is
   logged with the frame's `sender` (WHO) **and** `reason` (WHY) for
   auditability — the audit record is hub-inspectable, not only in the
   publisher's local log
   (`orchestrator.governor | resumed via control op ... sender=... reason=...`).
3. Falling back: `Governor.resume(task_id)` in-process, or restart the
   orchestrator (pause state is in-memory by design; the durable record lives on
   the k-board). The control frame is the no-restart path and the only one a
   Nagatha-side steer tool can use.
4. The message counter resets on resume; the token ledger does NOT (spend is
   history — raise `FLEET_BUDGET_TOKENS` if the task legitimately needs more).

### Durable governor state (#106) — FLAG-GATED, OFF by default

> **IDENTITY-GATED: enabling durable mode is BLOCKED on identity stabilization
> (T5 act/state enum + u7/#99).** State persisted against unstable identities is
> corrupt state. Turning this on is an **operator decision made only once
> identity is stable**, not a merge decision. Default is OFF and the merged code
> is byte-for-byte today's in-memory governor.

The governor's budget/pause counters are in-process on one hub. A crash resets
them — and those counters were the ONLY terminator of the 67% relay storm
(`msgs 31 > cap 30`, u1/#96); a crash-loop mid-storm would spend unbounded.
Issue #106 ships the MECHANISM to make the counters durable so a hub restart
*resumes* a task's counters from a JetStream KV store instead of resetting them.

| Env var | Meaning | Default |
|---|---|---|
| `FLEET_GOVERNOR_DURABLE` | persist/resume governor counters via JetStream KV (server-CAS) | `false` (OFF) |

- **OFF (default):** no store is wired; `persist_state`/`hydrate` are no-ops that
  make zero KV calls. The governor is byte-for-byte today's in-memory
  behavior — this is what merges and what runs until identity is stable.
- **ON (operator, post-identity-gate):** the hub opens a `FleetKVGovernorStore`
  over the bus in `start()` (before intake) via `open_governor_store` and attaches
  it to the governor. On task open it `hydrate`s the task's counters from the KV
  (resume, not reset); the snapshot is `persist_state`d **causally from the
  counting handler** (right after `note_message`), so the durable write always
  observes the count it just incremented — never a racing separate subscription
  that could snapshot `msgs=0` before the count landed.

**Substrate: JetStream KV with server-authoritative revision-CAS (not a new wheel).**
The store is the fleet-nats `FleetKV` wrapper (fleet-bus #124/#153), one KV key per
task holding the latest counter snapshot as a JSON blob. It is opened via the
`create_kv` path, which stamps a per-incarnation UUID into the bucket for ABA
protection by construction. This REPLACES the earlier k-board append-only event-log
design: that had no server-side compare-and-set on the counter blob, so its
monotonicity was enforced only in the client (a delayed/reordered append could
still slip a backward roll past the guard).

**Server-side CAS (NOT a client version number).** Each write carries the key's
current SERVER revision (the JetStream stream sequence the server assigns the key)
as the compare-and-set token: `FleetKV.create` establishes the key exactly once,
and subsequent writes are `FleetKV.update(key, blob, expected_revision)`. A **stale
write from an old hub** — the delayed/reordered write that would roll counters
*backward* — is rejected by the SERVER atomically with `StaleRevisionError`, across
every process. The losing hub re-reads the fresh revision and keeps its in-memory
counter authoritative; the counter can never roll backward. There is no
application-layer `version` field any more — the guard is the server revision.

**Token-spend ledger is persisted too.** The snapshot carries the task's token
spend and its dedup turn keys (`(agent, turn_id)` pairs), so a crash near the
token cap resumes spend instead of resetting it (which would hand a runaway a
whole second token budget), and a redelivered `token_cost` for a turn summed
before the crash is not double-counted after resume.

**KV WRITE failure policy (runaway-terminator counter).** If a durable write fails
while ON — including a CAS loss where another hub won the race — the governor FAILS
LOUD (WARNING) but keeps the **in-memory counter authoritative** and does **not**
halt the hub. The in-memory governor still terminates the storm *this* run (its
counters never depend on the KV); halting mid-storm would remove the very
terminator we are protecting. The only thing a failed write costs is *cross-restart*
resume. Never silent, never fatal.

**KV READ failure policy on restart — FAIL CLOSED.** A `hydrate` READ failure or a
MALFORMED durable record does **not** fall open to zero. Zeroing on restart would
reset the sole runaway terminator during a brief KV outage — the exact runaway the
durable state exists to survive. `FleetKV.get` fails closed by construction (only a
genuine key-miss on the same store incarnation returns `None`; any other failure —
bucket gone, recreated/ABA, connection dead — raises), and the store re-raises on a
corrupt record, so the task is **PAUSED** (assume at-risk) and the error is logged
at ERROR; an operator resumes it deliberately over the control plane once the KV is
readable again. A *missing* snapshot (task never persisted) is the normal
fresh-start signal and does NOT pause. Recovery: confirm the KV/JetStream is
healthy, then `fleet_control(op="resume", task_id=...)` (see below).

### `fleet_control` — the manager's steer→control lever (#65)

Step 2 above is a *control frame*, not a shell command — and in production the
only thing that publishes one is the **`fleet_control`** tool, registered on the
manager's subscriber (`FLEET_ROLE=manager` only; workers do not steer the
governor). It is the human end of the #61 wire: it calls the existing
`fleet_nats.control.publish_control` — no new frame format — so the frame Nagatha
sends is byte-for-byte the one the hub's `parse_control` reads.

**WHEN Nagatha uses it:** a human in Discord steers the run — "resume the run",
"pause t_42", "resume and give it more budget". Nagatha reads the target task id
straight off the PAUSED beat she posted (`[t_42] PAUSED over budget ...`), then
pulls the lever:

- `fleet_control(op="resume", task_id="t_42", budget_msgs_add=4, reason="per Rico")`
  → publishes the resume frame, returns a receipt she voices back:
  `resumed t_42, budget +4 — per Rico`.
- `task_id` defaults to the current bus turn's task when omitted (an in-turn
  steer inherits the task it is steering). If neither an explicit id nor a
  bus-turn task resolves, the tool **refuses** and asks for the id — it never
  guesses which task to steer. (The subscriber runs in a different process from
  the hub and cannot enumerate the hub's paused-task set; the id must come from
  the beat or the turn context.)
- `op` is the closed set `resume` / `pause`; `budget_msgs_add` is bounded
  (positive, and tool-capped) and applies to `resume` only. A mixed-case/dotted
  `task_id` is slugged once so the subject, payload, and receipt are one value
  (no hub-keying mismatch).
- **Audit (WHO + WHY):** the frame's `sender` is Nagatha's `FLEET_AGENT_ID` (WHO)
  and the `reason` rides the frame's payload (WHY). Both are logged **hub-side**
  on the resume/pause — the audit record is inspectable at the hub, not only in
  the tool's local log. That is the point of threading `reason` onto the frame:
  the WHO+WHY of a steer must be hub-inspectable.

**Why the lever matters, in Nagatha's words:** a steer that only *looks* like a
command is theater. *"If it cannot be inspected, it is not a command; it is
theater."* Posting "resuming the run" in Discord without publishing a control
frame changes nothing — the paused governor stays paused. `fleet_control` is what
makes the steer real: an inspectable frame on `dev.hub.control.<task_id>` that the
hub acts on and logs (with who + why). No lever pull, no command.

## Catalog-version enforcement (floor + refuse-to-start) (#103)

The subject catalog (templates + role→lanes map) is authoritative in the k-board
`nats_subjects` table (fleet-board #15). Clients learn it once at connect and
cache it; a registry outage falls **open** to the baked-in version-`0` code
catalog. Fail-open alone is a fleet-split hazard: with the endpoint undeployed,
every client silently runs the version-`0` catalog, a taxonomy write is a no-op,
and a partial rollout leaves mutually-deaf populations. So an OPT-IN **minimum
catalog version** is enforced at connect (`registry.connect_catalog()`, called by
BOTH the subscriber adapter and the orchestrator hub).

**The floor: `FLEET_SUBJECT_CATALOG_MIN_VERSION`.**

- `0` (default / unset) **DISABLES** enforcement — the bootstrap posture. A
  version-`0` code-catalog fleet still starts. Fail-open is preserved.
- A **positive** floor is enforced: a below-floor catalog makes the client
  **REFUSE TO START** — `CatalogVersionTooLowError`, fail-loud, before the bus
  opens (no partial subscriptions), propagating as a hard error (not the soft
  return-False path). The process exits loud.
- The floor is **never baked in**; the operator turns it on at rollout, and only
  after the endpoint serves that version. It is read on **both** the agent hosts
  (the subscriber enforces it too) and the hub (fleet-hub) — keep the value
  identical everywhere. A floored hub + un-floored workers (or vice-versa) is a
  **half-enforced split**: one population refuses a stale catalog while the other
  accepts it. That is the exact mixed-taxonomy state the floor prevents.

**Two distinct below-floor failure modes — read the error, don't guess.** A
version-`0` code catalog under a positive floor can mean two very different
things, and the enforcement makes them DISTINCT and loud so an operator can tell
"my client is stale" from "k-board is down":

| Error (log + exception type) | Meaning | What to do |
|---|---|---|
| `CatalogVersionTooLowError` — "…is below the configured floor … (registry answered; this is a STALE CLIENT)" | The registry ANSWERED and its served version is below the floor. | Update this client, or lower the floor. The fleet is genuinely ahead of it. |
| `RegistryUnreachableError` — "registry unreachable, cannot verify catalog floor …" | A configured registry URL FAILED (connection error / 404-not-deployed / malformed), so the client fell back to the version-`0` code catalog under an OUTAGE. The client may already satisfy the floor. | Restore k-board (or drop the floor until it is back). Do NOT chase a phantom client-version mismatch. |

`RegistryUnreachableError` subclasses `CatalogVersionTooLowError`, so existing
`except CatalogVersionTooLowError` handling and the refuse-to-start posture are
unchanged; only the message and type differ so the two modes are distinguishable.

> **Residual (known, by design).** There is **no on-disk last-known-good
> catalog cache**. So a k-board blip during a *floored* rolling restart still
> refuses to start the agents that are mid-restart — even ones that previously
> satisfied the floor — because the client cannot re-verify the floor while
> k-board is down. This is the safe default (a truly-below-floor client must not
> come up), and it is now **loud and correctly diagnosed** (`RegistryUnreachableError`
> naming "registry unreachable", not a misleading stale-client error). The
> mitigations are operational, not code: (a) keep the floor `0` until the
> endpoint is proven live and stable, and (b) never restart a floored fleet while
> k-board is down. A last-known-good on-disk cache is a future enhancement, not
> shipped here.

**Rollout ordering (mixed-fleet-safe — do these IN ORDER).** Because the floor is
opt-in, a fleet mid-rollout (some hosts floored, some not) never bricks — the
un-floored hosts keep failing open. Never raise the floor above what the live
endpoint serves.

1. **Deploy the k-board endpoint** (fleet-board #15 — a different repo). Nothing
   in fleet-bus deploys it; this repo owns only the client contract + the seed
   tool.
2. **Seed the table** from the code catalog:
   ```
   /opt/fleet-bus/.venv/bin/python -m fleet_nats.registry_sync \
       [--base-url https://kboard.example.internal]
   ```
   The tool's exit code is the gate (see the table below): **exit `0` = seeded,
   safe to proceed**; exit `3` = the endpoint is not deployed yet, so **do NOT
   raise the floor**. The seed PUTs a **client-owned** version: the version is
   sent by `registry_sync`, not assigned by the server — the endpoint stores what
   it is sent and `409`s only on a stale write (it does **not** auto-bump). The
   code catalog's version is `0` (the "unset / fallback / bootstrap" sentinel,
   which the endpoint validator rejects as `< 1`), so the seed clamps up and
   sends version **`1`** for the first real stored catalog (#155).
3. **Curl-verify the endpoint** returns 200 with the intended version:
   ```
   curl -s 'https://kboard.example.internal/api/nats-subjects?tenant=pg' | jq '.version'
   ```
   Confirm it is a 200 and the `version` is the one you intend to enforce (the
   first seed serves `1`).
4. **THEN raise the floor** to that version, fleet-wide, on the agent hosts AND
   the hub — `FLEET_SUBJECT_CATALOG_MIN_VERSION=<version>` — and re-apply /
   restart through the front door. Set it to the SAME value everywhere.

**`registry_sync` exit codes** (`python -m fleet_nats.registry_sync`) — exit `0`
is the "safe to raise the floor" gate:

| Exit | Meaning | Floor action |
|---|---|---|
| `0` | Seeded — a clean `HTTP 200` whose echoed catalog round-trips the seed. | **Safe to raise the floor** (after the curl-verify in step 3). |
| `2` | Registry **unreachable** (connection/transport error). | Do NOT raise the floor. Fix connectivity to k-board. |
| `3` | Endpoint **not deployed yet** (404 — fleet-board #15). | Do NOT raise the floor. The endpoint half is not shipped. |
| `4` | Not a confirmed store — any non-200 (a `4xx`/`5xx`, incl. a `400` from the endpoint validator, or a `3xx`/`204`). | Do NOT raise the floor. Read the logged status. |
| `5` | `200` but the body did not validate as a catalog. | Do NOT raise the floor. The endpoint returned an unexpected body. |
| `6` | `200` but the stored catalog drifted from what was sent. | Do NOT raise the floor. Reconcile `subjects.py` / the table. |

**Version skew is observable on the bus.** Every heartbeat carries
`catalog_version` (the version the emitter connected with; nullable so older
beats parse as unknown). The orchestrator's liveness tracker surfaces it in
`effective_topology()` and logs a `VERSION SKEW` WARNING when two live agents
diverge. The warning is **bounded**, NOT one-per-heartbeat (a mid-rollout fleet
is always skewed; a per-beat warning would be a log storm):

- It warns once per NEW `(agent, version)` divergence and re-arms only for a
  version an agent has not yet been warned at. Stale/dead agents' ghost versions
  are excluded, and a lone agent bumping its own version does not self-trigger.
- A **flapping** emitter (e.g. oscillating `6↔7` every beat) is bounded to at
  most one warning per DISTINCT version it holds — NOT one per flap-cycle. A
  catalog version that flaps is abnormal emitter behavior; the bounded warnings
  are the signal to inspect that emitter.
- **Coverage caveat during rollout.** Version-only comparison is blind to
  pre-#103 emitters that beat with NO `catalog_version` (they parse as unknown,
  by design — back-compat). When live version-carrying beats coexist with live
  version-less beats, the tracker emits a distinct one-shot `SKEW COVERAGE
  INCOMPLETE` WARNING. **"All agents carry a catalog version" is the precondition
  for trusting the `VERSION SKEW` signal** — until every live agent reports a
  version, a clean skew log is not a clean-fleet guarantee. Finish the rollout
  (all agents on a `catalog_version`-carrying build), confirm the coverage signal
  clears, then trust the skew log.

The WARNING is the operator signal for a mixed-taxonomy fleet: finish the
rollout, then confirm the skew clears.

## Channel map (slug ↔ Discord id) (#118)

The persisted **portal map** both bridge legs resolve channels through — the
Matrix `RoomBridgeStore` pattern. It replaces the per-process single channel
(`FLEET_VOICE_TEE_CHANNEL_ID` outbound, `FLEET_INTAKE_CHANNEL` as the only
inbound value), so **a host bridges N channels and a new channel is a row, not a
redeploy**.

| Env var | Meaning | Default |
|---|---|---|
| `FLEET_SUBJECT_REGISTRY_URL` | k-board base URL. The map is fetched from `/api/channel-map?tenant=pg` — the **same** URL as the subject registry (one k-board, one knob). | unset |
| `FLEET_CHANNEL_MAP` | bootstrap spec `slug=id,slug=id` used when the k-board table is unreachable/undeployed. **Many** portals, not a default channel. | unset |

**Load order** (once at connect/start, never on the hot path; never raises):

1. **k-board** when the registry URL is set — authoritative. Any failure
   (transport, non-2xx, malformed) logs an `ERROR` and falls through.
2. **`FLEET_CHANNEL_MAP`** — the static bootstrap. A malformed spec logs an
   `ERROR` and falls through.
3. **The empty map**, logged loudly. Not a default channel.

**Degrade, per leg** — the two are deliberately different:

- **Outbound** (subscriber voice tee): an unmapped channel is **not mirrored**
  (`WARNING`), never redirected to another room. The bus path is untouched.
- **Inbound** (hub intake poller): an unmapped channel is **stamped through
  unresolved** (`WARNING`) — the map translates, it does not gate intake, so an
  unreachable k-board never stops cards from opening.

A card names its own channel via the k-board row's `channel` field (a slug **or**
a Discord id — both canonicalise to the slug). `FLEET_INTAKE_CHANNEL` remains the
board-wide fallback for a card that names none.

An unresolvable channel is stamped through only when it is **subject-safe**. The
stamped slug becomes a NATS subject token
(`{env}.agent.{channel}.dispatch.{worker}`), so a value containing `*`, `>`, `.`
or a space is refused with an `ERROR` and the card falls back to
`FLEET_INTAKE_CHANNEL`, then to unchannelled. `parse_portals` applies the same
rule to every map row, and additionally rejects a map whose slug and Discord-id
namespaces overlap.

> **Inbound leg is wired but not yet exercised.** The `channel` field is READ by
> the intake poller; nothing in the fleet writes it today — `KboardClient.create_task`
> sets only `task_id`/`title`/`body`/`status`, and the live board could not be
> inspected to confirm the column exists (`GET /api/tasks?tenant=pg` returns 500).
> Until a writer lands, **every live card takes the `FLEET_INTAKE_CHANNEL`
> fallback**, which is the pre-#118 behavior. Only the outbound leg is genuinely
> per-channel today. Treat "a card names its own channel" as the implemented read
> contract, not as observed live behavior.

> **Not deployed yet.** `/api/channel-map` returns 404 on
> `kboard.example.internal` today (verified), so the live posture is rung 2
> (`FLEET_CHANNEL_MAP`) until the k-board table lands. The fail-open ladder means
> no agent goes down waiting for it.
>
> **Agent hosts must set `FLEET_CHANNEL_MAP`.** An agent host that sets
> `FLEET_SUBJECT_REGISTRY_URL` but not `FLEET_CHANNEL_MAP` loads the EMPTY map, and
> an enabled tee is then inert. `FLEET_VOICE_TEE_CHANNEL_ID` is bridged as a
> single home-channel portal so the mirror keeps working through the cutover (see
> below), but that bridge mirrors ONLY the agent's home channel. Set
> `FLEET_CHANNEL_MAP` to get the full per-channel behavior.

## Fleet-wide FleetKV kill authority — "stop everything" (#107)

`fleet_control` is per-task and in-band. The fleet-wide stop is one **JetStream
KV record**, read by the hub and every worker through independent, dedicated
control-plane connections. The work bus may be saturated; it is never the source
of truth for this decision.

### Scope and fail-closed behavior

The record `{ "armed": true | false }` gates every new hub dispatch/relay/wake to
`NOOP` and rejects worker ingress. An arm also cancels a worker's live gateway
background turns. Each watcher starts before its ordinary bus connection and
polls on its own OS thread. A missing key, malformed value, store error, or read
timeout is **unknown**, therefore armed: no error may silently release the fleet.

The authority is exactly one bucket/key pair shared by the CLI, hub, and workers:

| Setting | Default | Meaning |
|---|---|---|
| `FLEET_KILL_SWITCH_BUCKET` | `fleet-kill-authority` | Pre-provisioned JetStream KV bucket |
| `FLEET_KILL_SWITCH_KEY` | `fleet` | The single durable authority record |
| `FLEET_KILL_SWITCH_POLL_SECONDS` | `1` | Watcher interval; must be at most four seconds |
| `FLEET_KILL_SWITCH_DEADLINE_SECONDS` | `5` | CLI aggregate deadline; must be in `(0, 5]` |

### Provision once, then initialize once

Before any hub or worker is started, an operator with JetStream administration
credentials must provision the bucket. Use the deployed NATS administration
procedure (for example, `nats kv add fleet-kill-authority --history 1`), with the
same NATS URL and credentials that the fleet uses. Do not depend on automatic
creation: a missing bucket/key intentionally blocks enforcement rather than
creating an ambiguous release.

After the bucket exists, run the safe one-time initializer from the installed
orchestrator venv:

```bash
# FLEET_NATS_URL and its configured credentials must be present in this shell.
/opt/fleet-bus/current/.venv/bin/fleet-kill initialize
```

`initialize` creates only a previously absent key in the **released** state. It
refuses an existing key, including an armed key; it never clears an authority
that another operator has set. Use `status` to confirm the persisted value before
starting fleet processes.

### Incident operation

The command opens its own FleetBus/KV connection and confirms the store action
with a compare-and-set write. Its one hard deadline covers connect, KV open,
read/write, and close reporting; it never falls back to a file or a bus frame.

```bash
/opt/fleet-bus/current/.venv/bin/fleet-kill arm
/opt/fleet-bus/current/.venv/bin/fleet-kill status
/opt/fleet-bus/current/.venv/bin/fleet-kill release
```

Use positional actions exactly as shown. The old `--release`, `--status`, and
`FLEET_KILL_SWITCH_PATH` contracts are removed and have no effect.

- `arm`, `release`, and successful `initialize` exit `0`.
- `status` exits `0` when the durable record is armed and `1` when released.
- A configuration, connection, missing-key, CAS, or aggregate-deadline failure
  exits `2`; that is not a successful release or arm.

`status` is the store confirmation, not a claim about a particular hub process.
After an arm, it must report armed; then inspect hub/worker logs for the watcher
edge if process-level confirmation is required. A recovered probe tears down its
old control connection and reconnects, so a later authoritative release is able
to resume the hub and workers.
## Orchestrator hub — the Phase-9 run-loop service (#9)

Enabling Nagatha's subscriber makes her *reachable* (ingress); it does NOT make
her coordinate. Coordination — handling worker `status`, firing relays, walking
the offline ladder, persisting to k-board, narrating — is owned by the
**orchestrator run-loop**, a distinct long-lived service. That service is
`fleet_orchestrator`'s hub (`python -m fleet_orchestrator`, console-script
`fleet-orchestrator`). It runs on **fleet-hub (fleet-hub)**, not on an agent host: it
is the manager's coordination brain as its own process, so it outlives any single
agent turn and survives agent restarts.

### What it does on start

1. Connect to NATS with the connection name **`fleet-orchestrator-hub`** (the
   `/connz` probe greps this — distinct from the manager subscriber's
   `fleet-nats-<agent>`). Set a `FLEET_NATS_NAME` only to override deliberately.
2. Load the subject catalog once (fleet-board #15): reads
   `FLEET_SUBJECT_REGISTRY_URL` and fetches the k-board `nats_subjects` table,
   **failing open loudly to the baked-in code defaults** when the registry is
   unreachable. The registry endpoint is not deployed yet, so the code-defaults
   path is the live path today — that is expected, not a fault. If
   `FLEET_SUBJECT_CATALOG_MIN_VERSION` is set to a positive floor, this step also
   **enforces it before the bus opens** and the hub **refuses to start** on a
   below-floor catalog — see
   [Catalog-version enforcement](#catalog-version-enforcement-floor--refuse-to-start-103)
   for the floor, the distinct outage-vs-stale errors, and the rollout ordering.
3. Subscribe the intake lane `{env}.kboard.task.created.*` (a new k-board task
   card opens a coordination session), the shared heartbeat wildcard (liveness),
   and the #27 token-cost feed (so the token cap is a live control, not inert).
4. Per task: dispatch to the workers, then watch `agent_status.{task_id}` for
   relay/done, layer the scorekeeper + offline observers on the same lane, and
   drive dispatch → relay/wake/redispatch/offline → done, persisting each
   transition to k-board (best-effort: a durable-write failure logs loudly and
   coordination continues over the bus — one task's k-board outage never takes
   the hub down).

### Env surface (all fail-loud at startup)

| Env var | Meaning | Default |
|---|---|---|
| `FLEET_NATS_URL` | NATS server | `nats://192.0.2.74:4222` |
| `FLEET_AGENT_ID` | the hub's own bus identity (usually the manager id) | `unknown` |
| `FLEET_ENV` | subject env prefix — exactly `dev` or `prod`, case-sensitive (matches `BusConfig`). The **prod hub MUST set exactly `prod`**: it selects the prod subject prefix AND arms the default-on replay trace (#108). Code default is `dev` (off-by-default trace); a prod unit left at `dev` runs with capture silently off (review P1-1). | `dev` (prod unit sets `prod`) |
| `FLEET_MANAGER_ID` | **required** — the manager id the hub narrates as | — |
| `FLEET_WORKERS` | **required** — comma-separated default worker ids (e.g. `skippy,bilby`) | — |
| `FLEET_BUDGET_MSGS` / `FLEET_BUDGET_TOKENS` | governor caps (above) | `200` / `150000` |
| `FLEET_GOVERNOR_DURABLE` | persist/resume governor counters via JetStream KV server-CAS — **IDENTITY-GATED, operator-only** (above) | `false` |
| `FLEET_DISPATCH_TIMEOUT_SECONDS` | offline-ladder silence window per rung | `300` |
| `FLEET_OFFLINE_POLL_SECONDS` | how often the ladder is advanced | `30` |
| `FLEET_HEARTBEAT_INTERVAL_SECONDS` | heartbeat cadence for staleness math (match the subscriber emitter) | `30` |
| `FLEET_CIRCLEJERK_N` | scorekeeper consecutive-agreement threshold | `4` |
| `FLEET_SUBJECT_REGISTRY_URL` | subject-registry base URL (unset ⇒ code defaults). Also the [channel map](#channel-map-slug--discord-id-118) source. | unset |
| `FLEET_CHANNEL_MAP` | channel-portal bootstrap spec `slug=id,slug=id`, used when the k-board channel-map table is unreachable — see [Channel map](#channel-map-slug--discord-id-118) | unset |
| `FLEET_SUBJECT_CATALOG_MIN_VERSION` | subject-catalog floor; the hub **refuses to start** on a below-floor catalog (`0`/unset ⇒ OFF, fail-open). Must match the agent hosts — see [Catalog-version enforcement](#catalog-version-enforcement-floor--refuse-to-start-103) | `0` |
| `FLEET_TRACE_PATH` | JSONL coordination trace file. In **prod** the trace is **ON by default** at `/var/lib/fleet-orchestrator/trace/coordination.jsonl` even when unset (an incident must always leave a replay trace — #108); an explicit path overrides that default. In **dev** an unset path ⇒ no-op sink. | unset (prod defaults on) |
| `FLEET_NATS_NAME` | override the `fleet-orchestrator-hub` connection name | (do not set) |

The **arena narration channel** (`<arena-channel-id>`) is the Discord channel
Nagatha's own gateway posts her narration beats into — the hub only publishes
beats to her `agent_inbox`; her adapter → her gateway → her post is what reaches
that channel. The channel id is not the hub's config (it never touches Discord).

### Modes (for testing without the full deploy)

- `python -m fleet_orchestrator` — the durable service; serves until SIGINT/SIGTERM.
- `python -m fleet_orchestrator --once --task-id <id> [--channel <slug>]` — start
  one session and exit when it reaches DONE (bounded timeout). Live smoke.
- `python -m fleet_orchestrator --dry-run` — coordinate with NO narrator wired,
  so no beats reach Discord. Combine with `--once` to prove a session end-to-end
  off Discord.

### Deploy on fleet-hub (maintainer-gated — do NOT run without authorization)

The normal repo-owned path starts with the **Request fleet hub deploy** GitHub
Actions workflow. It is operator-triggered (`workflow_dispatch`), and its
GitHub-hosted validation job fails any ref other than exactly `main`; it never
runs automatically after a merge. A successful main request triggers the
default-branch **Deploy fleet hub** workflow through `workflow_run`. This keeps
branch-editable workflow code off a fleet runner; the trusted continuation binds checkout
and deployment to the successful request's exact SHA. The dedicated runner must
carry the registered `[self-hosted, fleet-bus]` labels and
have `git`, `sha256sum`, `scp`, and `ssh`. It must reach fleet-hub through the SSH
host alias `fleet-hub-deploy`, which is provisioned as user `rico` by the infra
lane. Do not route around a missing runner or SSH prerequisite. The deploy names
the `fleet-bus-production` GitHub Environment. Required reviewers/protection on
that Environment are an external GitHub configuration gate; this repository
does not create or weaken them.

The workflow proves its credential-free checkout is clean at the selected full
SHA, creates a complete Git bundle for that exact commit under `RUNNER_TEMP`,
hashes it, and transfers it to a unique `/opt/fleet-bus/incoming/` path. The
bounded helper runs only on fleet-hub: it verifies the hash, complete bundle, and
advertised SHA; atomically creates or reuses a clean detached checkout at
`/opt/fleet-bus/sources/<full-sha>/`; and quarantines dirty or corrupt reuse.
Incoming and staging artifacts are cleaned. Checkout credentials never cross
the SSH boundary.

fleet-hub—not the runner—must have `git`, `uv`, Python 3.13, `rsync`, `curl`, `jq`,
`sha256sum`, `realpath`, and `flock`. The remote helper calls the existing
release script with the exact source checkout, which builds that commit once at
its final path, `/opt/fleet-bus/releases/<full-sha>/`: it copies the committed
workspace `uv.lock`, exports locked, hash-checked third-party requirements, then
installs all four local packages (`fleet-core`, `fleet-nats`,
`fleet-subscriber`, and `fleet-orchestrator`) non-editably at the final `.venv`
path. The root lock also contains the build backend, closing an implicit
build-time resolution path. Each sealed release records a distribution/content
`release-manifest.json` and its digest `release-seal`; reuse is allowed only
after the complete manifest schema, full content map, lock digest, marker,
executable, seal, and all four final-venv distributions verify. Derived CPython
bytecode (`__pycache__`/`*.pyc`) is deliberately excluded from the manifest on
both the seal and verify sides (#208): the running service writes fresh `.pyc`
into the venv on import (its unit does not set `PYTHONDONTWRITEBYTECODE`), which
would otherwise mutate the sealed release and fail the next deploy. This is safe
because the authoritative `.py` source is sealed and CPython never trusts a
`.pyc` whose source hash/mtime does not match. An incomplete/corrupt pre-existing
release is quarantined while the deploy lock is held, then rebuilt from the
selected SHA.

Activation is an atomic `/opt/fleet-bus/current` symlink replacement. Before
activation or restart, the script reads effective systemd `ExecStart` and
requires the exact `/opt/fleet-bus/current/.venv/bin/fleet-orchestrator` path.
Readiness requires consecutive `systemctl is-active` checks and JSON-parsed
`/connz` responses whose connection `name` equals the configured name exactly.
The hub unit must set `FLEET_NATS_NAME=fleet-orchestrator-hub`; an explicitly set
`FLEET_READY_NAME` must be the same string. This is one config contract, not two
independently tuned names.

A managed prior release requires coherent `current`, marker, manifest/seal, and
`DEPLOYED_SHA`. The only legacy-bootstrap state is `current -> /opt/fleet-bus`
with a working legacy venv and no receipt. Failed readiness restores the old
symlink and re-proves readiness before an old receipt is restored. If rollback
cannot prove the old release, the receipt is removed and the script emits
`CRITICAL: ROLLBACK FAILED` rather than falsely claiming the old SHA.

One-time migration before the first workflow run is maintainer/root work, not a
workflow step. The preconditions below must run **before** any release-path,
unit, or sudoers mutation. The remote deploy operator is `rico`; the hub service
continues to run under the service account declared by its unit:

```sh
set -euo pipefail
deploy_user=rico
home=/opt/fleet-bus
legacy_exec="$home/.venv/bin/fleet-orchestrator"

id "$deploy_user" >/dev/null
[ -x "$legacy_exec" ] || { echo "legacy fleet-orchestrator venv is required" >&2; exit 1; }
"$legacy_exec" --help >/dev/null
if [ -e "$home/current" ] || [ -L "$home/current" ]; then
  echo "current already exists; this migration is first-run only" >&2
  exit 1
fi
if [ -e "$home/DEPLOYED_SHA" ] || [ -L "$home/DEPLOYED_SHA" ]; then
  echo "DEPLOYED_SHA already exists; this migration is first-run only" >&2
  exit 1
fi

sudo install -d -o "$deploy_user" -g "$deploy_user" -m 0750 \
  "$home/releases" "$home/incoming" "$home/sources"
sudo chown -R "$deploy_user:$deploy_user" "$home"
sudo ln -s "$home" "$home/current"
sudo install -d -o root -g "$deploy_user" -m 0750 /etc/fleet-bus
sudo mkdir -p /etc/systemd/system/fleet-orchestrator.service.d
sudo tee /etc/systemd/system/fleet-orchestrator.service.d/release-path.conf >/dev/null <<'EOF'
[Service]
EnvironmentFile=/etc/fleet-bus/orchestrator.env
Environment=FLEET_NATS_NAME=fleet-orchestrator-hub
ExecStart=
ExecStart=/opt/fleet-bus/current/.venv/bin/fleet-orchestrator
EOF
sudo systemctl daemon-reload
sudo systemctl show fleet-orchestrator --property=ExecStart --value
# The output must contain: path=/opt/fleet-bus/current/.venv/bin/fleet-orchestrator ;
# Register the runner with labels: self-hosted,fleet-bus and provision its
# fleet-hub-deploy SSH alias for rico@fleet-hub.
printf '%s ALL=(root) NOPASSWD: /usr/bin/systemctl show fleet-orchestrator --property=ExecStart --value, /usr/bin/systemctl restart fleet-orchestrator, /usr/bin/systemctl is-active fleet-orchestrator\n' "$deploy_user" \
  | sudo tee /etc/sudoers.d/fleet-bus-deploy >/dev/null
sudo chmod 0440 /etc/sudoers.d/fleet-bus-deploy
sudo visudo -cf /etc/sudoers.d/fleet-bus-deploy
```

The remote deploy operator owns the managed `/opt/fleet-bus` paths but does not
run the hub as root. Repository token permissions are `contents: read`; checkout
is pinned to an immutable action commit with credential persistence disabled.
No deploy secret, SSH key, or registration token belongs in the repository.
Before the first authorized dispatch, confirm the runner is online with both
labels, its `fleet-hub-deploy` alias works, and the protected Environment has
its required reviewers.

1. Legacy bootstrap is only the pre-workflow migration above. After the first
   release, use `/opt/fleet-bus/current/.venv/bin/...` paths exclusively; do not
   install an editable hub venv in place.
2. Write the systemd unit (env in the unit or an `EnvironmentFile`, never inline
   secrets — the hub reads no secrets, but keep the pattern):
   ```
   # /etc/systemd/system/fleet-orchestrator.service
   [Unit]
   Description=Fleet orchestrator hub (Phase-9 coordination run-loop)
   After=network-online.target
   Wants=network-online.target

   [Service]
   User=fleetdeploy
   Group=fleetdeploy
   Environment=FLEET_NATS_URL=nats://192.0.2.74:4222
   Environment=FLEET_ENV=prod
   Environment=FLEET_AGENT_ID=nagatha
   Environment=FLEET_MANAGER_ID=nagatha
   Environment=FLEET_WORKERS=skippy,bilby
   # Must exactly match the release deploy probe contract.
   Environment=FLEET_NATS_NAME=fleet-orchestrator-hub
   # Required while the default-on intake poller is enabled: every organic
   # dispatch and narration beat must carry its destination channel.
   Environment=FLEET_INTAKE_CHANNEL=all-agents
   # Channel portal map (#118). REQUIRED on every hub and agent host until the
   # k-board /api/channel-map table is deployed (it 404s today), because the load
   # ladder otherwise lands on the EMPTY map: intake then stamps channels
   # unresolved and an enabled voice tee is inert. Many portals, slug=discord_id,
   # comma-separated — NOT a default channel. Every slug listed here must be
   # subject-safe (no '*', '>', '.' or spaces) and must not equal any other row's
   # Discord id. Replace with this fleet's real rows:
   #   Environment=FLEET_CHANNEL_MAP=all-agents=<all-agents-channel-id>,arena=...
   Environment=FLEET_CHANNEL_MAP=all-agents=REPLACE_WITH_DISCORD_CHANNEL_ID
   # Real-run controls. Keep the source defaults here; reduced test-window
   # values can strand normal turns before workers have time to respond.
   Environment=FLEET_BUDGET_MSGS=200
   Environment=FLEET_DISPATCH_TIMEOUT_SECONDS=300
   Environment=FLEET_OFFLINE_POLL_SECONDS=30
   # Subject-catalog floor. Leave OFF (0 / omit) until the k-board endpoint
   # serves a versioned catalog and every agent host is raised to the SAME
   # value (a floored hub + un-floored workers is a half-enforced split). Raise
   # it ONLY after the rollout gate below passes. Prefer an EnvironmentFile so
   # the hub and the agent hosts share one source. Example once live:
   #   Environment=FLEET_SUBJECT_CATALOG_MIN_VERSION=3
   Environment=FLEET_SUBJECT_CATALOG_MIN_VERSION=0
   # FleetKV kill authority (#107). Supply the same NATS URL and credential
   # source used by fleet-kill, the hub, and workers; the authority bucket/key
   # must already be provisioned and initialized as documented above.
   # Environment=FLEET_KILL_SWITCH_BUCKET=fleet-kill-authority
   # Environment=FLEET_KILL_SWITCH_KEY=fleet
   # systemd creates/owns /var/lib/fleet-orchestrator (0700, the unit's User) on
   # EVERY start, so the prod-default replay trace path
   # /var/lib/fleet-orchestrator/trace/coordination.jsonl is always writable —
   # without this the trace's lazy mkdir/open raises PermissionError on a clean
   # host and capture silently degrades (#108, review P1-2). The hub also fails
   # LOUD at startup if the trace path is not writable, so a broken perms setup
   # surfaces as an ERROR next to the posture line rather than a silent gap.
   StateDirectory=fleet-orchestrator
   ExecStart=/opt/fleet-bus/current/.venv/bin/fleet-orchestrator
   Restart=on-failure
   RestartSec=5
   KillSignal=SIGTERM

   [Install]
   WantedBy=multi-user.target
   ```
   > **`FLEET_ENV=prod` is load-bearing here (review P1-1).** The prod-default
   > replay trace (#108) arms ONLY on the exact string `FLEET_ENV=prod` — the
   > same strict, case-sensitive contract `BusConfig` enforces (`Prod`/` prod `
   > are NOT prod; they leave the trace OFF *and* make `BusConfig` raise at
   > connect). A hub unit left at `FLEET_ENV=dev` runs with capture silently
   > off, so the F6 acceptance ("a prod hub captures a replay trace") fails
   > quietly. The prod hub MUST set exactly `prod`.
   >
   > **Perms prerequisite (review P1-2).** `StateDirectory=fleet-orchestrator`
   > makes systemd create + chown `/var/lib/fleet-orchestrator` to the unit's
   > `User` on every start. If you run the hub as a non-default user or without
   > `StateDirectory`, pre-create the dir writable by that user
   > (`install -d -o <user> -m 0700 /var/lib/fleet-orchestrator`) or the trace
   > sink degrades. An explicit `FLEET_TRACE_PATH` overrides the default and must
   > point at a dir the unit's user can write.
   >
   > **Retention — the trace is self-bounded (review P2 #133).** The append-only
   > trace does NOT grow unbounded: `JsonlTraceSink` rotates in-sink at **128 MiB**
   > per file, keeping **5** numbered backups (`coordination.jsonl.1` … `.5`), so
   > total on-disk trace is hard-capped at **~768 MiB** — no logrotate/tmpfiles
   > fragment is required. Size the `StateDirectory` volume with headroom over that
   > bound. Monitoring: alert if `du -sh /var/lib/fleet-orchestrator/trace`
   > materially exceeds ~768 MiB (would indicate rotation is failing — check the
   > `trace.emit | sink write failed` ERROR line) or if the volume's free space
   > drops low. The bound holds regardless of hub uptime.
3. `systemctl daemon-reload && systemctl enable --now fleet-orchestrator`.
4. Verify effective state (never trust the unit file):
   - `journalctl -u fleet-orchestrator` shows `orchestrator.hub | ready
     connection=fleet-orchestrator-hub` and the catalog-load line.
   - **Kill-authority watch is live:** the startup log shows
     `orchestrator.hub | kill authority watch active`. Confirm the durable state
     separately with `/opt/fleet-bus/current/.venv/bin/fleet-kill status`; a missing key
     or unavailable store is fail-closed, not a release.
   - **Replay trace armed (#108, review P1-1/P1-2).** `journalctl -u
     fleet-orchestrator | grep 'trace.configure'` shows exactly
     `trace.configure | JSONL trace ENABLED path=/var/lib/fleet-orchestrator/trace/coordination.jsonl`
     — NOT `DISABLED` (means `FLEET_ENV` isn't the exact string `prod`) and NOT
     `NOT WRITABLE` (means the StateDirectory/perms prerequisite failed). Then
     confirm the file GROWS under load: `wc -l
     /var/lib/fleet-orchestrator/trace/coordination.jsonl` before and after a
     proving task increases (a prod hub that never writes a line is the silent
     failure #108 exists to close).
   - The NATS monitor `/connz` (`http://192.0.2.74:8223`) shows the
     `fleet-orchestrator-hub` connection.
   - A `--dry-run --once` smoke (or a real proving task) drives dispatch → relay
     → done; the k-board card for the task moves `running` → `done`.

### Graduated proving evidence contract (#9, #46, #47)

Use a fresh `task_id` for every rung. Capture the NATS monitor's server-wide
`in_msgs` and `out_msgs` from `/varz` immediately before and after the rung, then
retain the task's trace records, hub log lines, arena posts, and k-board state in
one evidence bundle.

Core NATS does **not** expose per-subject or per-lane counters. `/connz` proves
connection liveness, while `/varz` provides only a gross server-wide traffic
delta. Therefore a published coordination operation is proven by both of these
exact-subject records:

1. the task's trace record carrying the expected full `subject`; and
2. the matching `orchestrator.* | subject=...` hub log line.

The before/after `/varz` delta supplements that pair as gross broker-throughput
evidence; it cannot attribute traffic to a subject. Do not require or report a
per-lane broker counter, and do not score a rung as failed because that
nonexistent metric is absent.

Apply that contract to the graduated ladder:

| Rung | Exact-subject proof | Other required proof |
|---|---|---|
| 3 | matching dispatch trace record and `orchestrator.dispatch` log line | worker's own-words arena post, done sentinel, and k-board `status=done` |
| 5 | matching relay and wake trace records plus `orchestrator.relay` and `orchestrator.wake` log lines | induced missing handoff and silence, peer's own-words arena post, and k-board `status=done` |
| 8 | matching dispatch trace/log pair plus the exact-subject `orchestrator.arbitrate` request/reply log | deterministic reply, score/reset/trip and decision trace records, narration or synthesis turn, and k-board `status=done` |

Rung 8 must follow the shipped trip gate documented in
`docs/architecture.md`: for `DECIDE`, send the evidenced
`dissent`+`fact_pull` turn first, then the uninterrupted
`FLEET_CIRCLEJERK_N` agreement run. Use a separate fresh task with no single
evidenced candidate to prove `SYNTHESIZE`. A dissent or fact-pull inserted into
the agreement run resets it, and a completed topic that never trips the
circlejerk detector produces no decision on the shipped run-loop path.

First-dispatch acceptance is: workflow validation passed on `main`, the named
Environment approved it, `DEPLOYED_SHA` equals the selected full SHA, the release
manifest/seal verifies, `/connz` has the exact configured connection name, and a
real smoke succeeds. Preserve that receipt with the workflow run URL and SHA.

Workflow rollback is automatic. For break-glass rollback, choose a known-good
sealed release and preserve the atomic/ready/receipt ordering; do not write a
receipt before readiness is proven:

```sh
set -euo pipefail
home=/opt/fleet-bus
sha='<known-good-full-sha>'
source="$home/sources/$sha" # clean detached managed source; never a release directory
trusted_python=/usr/bin/python3 # root-owned operator interpreter; never $target/.venv/bin/python
[[ "$sha" =~ ^[0-9a-f]{40}$ ]] || { echo "full lowercase SHA required" >&2; exit 1; }
target="$home/releases/$sha"
receipt="$home/DEPLOYED_SHA"
next="$home/.current-next-break-glass"
activated=0
receipt_written=0

cleanup() {
  status=$?
  trap - EXIT
  if [ "$status" -ne 0 ] && [ "$activated" -eq 1 ] && [ "$receipt_written" -ne 1 ]; then
    unlink "$receipt" 2>/dev/null || true
    echo "break-glass failed after activation; DEPLOYED_SHA invalidated" >&2
  fi
  exit "$status"
}
trap cleanup EXIT

# This is the same complete, side-effect-free verifier used before a deploy
# reuses a sealed release. The interpreter and verifier implementation come
# from the trusted operator/source boundary, never from $target. The source
# checkout must be the clean selected SHA so the verifier can reject altered
# copied source, changed symlinks, non-regular release entries, and missing or
# altered installed distribution metadata/module paths. The verifier only
# inspects the candidate site-packages filesystem; it never imports or executes
# candidate release code.
[ "$(git -C "$source" rev-parse HEAD)" = "$sha" ]
[ -z "$(git -C "$source" status --porcelain=v1 --untracked-files=all)" ]
[ -x "$trusted_python" ] && [ -f "$source/scripts/verify-hub-release.py" ] && \
  [ ! -L "$source/scripts/verify-hub-release.py" ]
PYTHONDONTWRITEBYTECODE=1 "$trusted_python" \
  "$source/scripts/verify-hub-release.py" --source "$source" --release "$target" --sha "$sha"

ln -s "$target" "$next"
activated=1
mv -Tf "$next" "$home/current"
sudo systemctl restart fleet-orchestrator
sudo systemctl is-active fleet-orchestrator
curl -fsS http://127.0.0.1:8223/connz | jq -e --arg name fleet-orchestrator-hub \
  '(.connections | type == "array") and any(.connections[]?; .name? == $name)' >/dev/null
printf '%s\n' "$sha" >"$home/.DEPLOYED_SHA-break-glass"
chmod 0444 "$home/.DEPLOYED_SHA-break-glass"
mv -f "$home/.DEPLOYED_SHA-break-glass" "$receipt"
receipt_written=1
```

If the symlink, restart, or readiness step fails, remove `DEPLOYED_SHA`, preserve
the failing evidence, and escalate to the maintainer; do not claim rollback
completed. Disabling the service remains the stop-only fallback. The hub holds
no durable state of its own (session state is in-memory; the k-board card is the
record), so stopping it pauses coordination without losing task history.

## Nightly organic canary (#89)

A once-a-night liveness proof of the **whole production intake lane** — not a
harness frame. `python -m fleet_orchestrator.canary` (console script
`fleet-canary`) creates ONE real `[fleet]` k-board card, then polls it:

- the live intake poller auto-opens a session from the `todo` card (its title
  carries `FLEET_INTAKE_LABEL`, so it rides the exact production path);
- the card reaching `done` within `FLEET_CANARY_TIMEOUT_SECONDS` is **GREEN** —
  the canary logs and exits `0` **silently** (no channel post; a healthy night is
  quiet);
- a `blocked` card, a create/poll error, or the timeout elapsing is **RED** — the
  canary publishes **exactly one** narrator headline to `FLEET_INTAKE_CHANNEL`
  (the same PR-#85 channel-carrying beat path fleet-liveness uses) and exits `1`.

This is the goal-brief contract: *one `[fleet]` card/night, narrator headline
ONLY on failure.*

### Env surface (all fail-loud at startup)

| Env var | Meaning | Default |
|---|---|---|
| `FLEET_MANAGER_ID` | **required** — the manager id the failure headline narrates as | — |
| `FLEET_INTAKE_LABEL` | **required** — the card-scope marker the poller opens on (must be `[fleet]` to match the live poller) | — |
| `FLEET_INTAKE_CHANNEL` | **required** (live runs) — the Discord channel the failure headline carries; unset/empty is a config error (exit 2) in a narrating run, since a RED headline with no channel would route to nobody. Only optional under `--dry-run` (no narrator wired). | — |
| `FLEET_CANARY_TIMEOUT_SECONDS` | GREEN window — how long to wait for `done` before RED | `900` |
| `FLEET_CANARY_POLL_SECONDS` | how often the card is re-read while waiting | `15` |
| `FLEET_NATS_URL` / `FLEET_ENV` / `FLEET_AGENT_ID` | bus connection (same as the hub) | as above |

Exit codes: `0` GREEN, `1` canary failure (timeout/blocked/error), `2` config error.

### Modes (for testing without the full deploy)

- `python -m fleet_orchestrator.canary` — one real run against the live board +
  bus; narrates on failure.
- `python -m fleet_orchestrator.canary --dry-run` — create + poll the card but
  wire NO narrator (a failure still exits `1` but posts nothing to Discord).

### Deploy on fleet-hub (maintainer-gated — do NOT run without authorization)

A **systemd timer** on the hub fires the canary once a night. Same venv +
`EnvironmentFile` convention as the hub service; the canary reuses the hub's env
(`FLEET_MANAGER_ID`, `FLEET_INTAKE_LABEL`, `FLEET_INTAKE_CHANNEL`, the bus vars),
so point its unit at the same `EnvironmentFile`.

1. The console script is in the active immutable release:
   `/opt/fleet-bus/current/.venv/bin/fleet-canary`.
2. Write the oneshot service + timer:
   ```
   # /etc/systemd/system/fleet-canary.service
   [Unit]
   Description=Nightly organic canary for the fleet-bus trio (fleet-bus #89)
   After=network-online.target fleet-orchestrator.service
   Wants=network-online.target

   [Service]
   Type=oneshot
   # Reuse the hub's env (bus vars + FLEET_MANAGER_ID + FLEET_INTAKE_LABEL +
   # FLEET_INTAKE_CHANNEL); add the canary knobs there or inline below.
   EnvironmentFile=/etc/fleet-bus/orchestrator.env
   Environment=FLEET_CANARY_TIMEOUT_SECONDS=900
   ExecStart=/opt/fleet-bus/current/.venv/bin/fleet-canary
   ```
   ```
   # /etc/systemd/system/fleet-canary.timer
   [Unit]
   Description=Fire the nightly fleet canary once a night

   [Timer]
   # 09:07 UTC ≈ 04:07–05:07 ET (EST/EDT) — a quiet pre-dawn slot. The 30-min
   # randomized delay avoids a fixed-second thundering herd on the board.
   OnCalendar=*-*-* 09:07:00 UTC
   RandomizedDelaySec=1800
   Persistent=true

   [Install]
   WantedBy=timers.target
   ```
   Crontab equivalent (if a timer is not used), env sourced from the same file:
   ```
   # m h dom mon dow  — 09:07 UTC nightly
7 9 * * *  set -a; . /etc/fleet-bus/orchestrator.env; set +a; /opt/fleet-bus/current/.venv/bin/fleet-canary >> /var/log/fleet-canary.log 2>&1
   ```
3. `systemctl daemon-reload && systemctl enable --now fleet-canary.timer`.
4. Verify effective state (never trust the unit file):
   - `systemctl list-timers fleet-canary.timer` shows the next fire time.
   - A manual `systemctl start fleet-canary.service` (or a direct
     `fleet-canary` run) drives ONE card `todo` → `running` → `done`;
     `journalctl -u fleet-canary` shows `canary | GREEN task=… reached done`
     and **no** narration beat. GATE 6 requires this once-green run.
   - Force a RED (e.g. a tiny `FLEET_CANARY_TIMEOUT_SECONDS=1`) and confirm ONE
     `[fleet] nightly canary FAILED …` headline reaches `FLEET_INTAKE_CHANNEL`.

Rollback: `systemctl disable --now fleet-canary.timer`. The canary holds no
durable state; a stray canary card is a normal `[fleet]` card the trio completes.
