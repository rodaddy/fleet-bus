# F3 — Staging env / shadow hub (separate server) — deploy runbook (#105)

> **SUPERSEDED IN PART (2026-07-28) — read this first.**
>
> #105 did **not** close against this document's separate-staging-host design.
> Its acceptance receipt says so explicitly: *"This closes against the
> superseding all-dev controller decision in the prior comment, not the obsolete
> prod/staging/new-LXC wording."* What actually shipped was a **3-node all-dev
> coordination cluster on existing nodes** — Bilby `192.0.2.71`, Dash
> `192.0.2.62`, node-c `192.0.2.21`, on dedicated ports `4223/6223/8223`, with
> the existing `4222/8222` services left untouched.
>
> **Still true and still enforced** — do not delete these:
> - The `BusConfig` staging tripwire is live and tested. It moved packages:
>   it is now `fleet_core.config` (`_STAGING_ENV`, `_PROD_ENDPOINTS_RAW`,
>   `_is_known_prod_url`), re-exported through `fleet_nats.config`. The private
>   `_PROD_ENDPOINTS_RAW` symbol is **not** re-exported — reference it as
>   `fleet_core.config._PROD_ENDPOINTS_RAW`.
> - Everything this doc says about *why* an IP guard is not an isolation
>   boundary, and about credentials being the real boundary, holds.
> - The DNS-coupling and localhost-tunnel caveats hold.
>
> **No longer the plan** — §1's "pick a separate staging host" and the GO-gated
> host stand-up. There is one cluster; both `dev.` and `prod.` subject prefixes
> are granted per-user on it (see `/etc/fleet-js/nats-server.conf`, rendered by
> rtech-infra `fleet-js-cluster.yml`, **not** by this repo).
>
> Treat the sections below as the design rationale of record, not as an
> executable host-provisioning runbook.

How to stand up a **separate staging NATS server + shadow orchestrator hub** so
every bus/topology change is proven on a testbed with **zero prod traffic**,
before it touches the live fleet. This is also the **JetStream pilot ground**
(JetStream is a server-level enable you cannot safely stage on the prod box).

> **Isolation = a separate server WITH DISTINCT CREDENTIALS, NOT a subject
> prefix and NOT the IP guard.** `FLEET_ENV=staging` on the *same* server was
> rejected on codex review: it shares prod's failure domain, config, load, and
> slow-consumer behaviour, and JetStream enablement is itself a server-level
> change you can't stage on the box you're protecting. The `{env}=staging` token
> is a **labeling convenience only**.
>
> **What actually enforces isolation is distinct staging CREDENTIALS/identity**
> (§2.1): staging mints its own account/creds (and preferably a TLS server
> identity) so **prod credentials are INVALID on staging and staging credentials
> INVALID on prod**. That is the fail-closed boundary. A separate host with no
> auth is *not* isolated — any client that resolves to the staging IP (including
> a prod-labeled one) could connect, and a staging client that resolves a DNS
> alias to the prod IP could reach prod.
>
> The library's `BusConfig` prod-URL guard is **defense-in-depth against the
> common literal-IP typo — it is NOT the isolation boundary.** It fails loud if
> `FLEET_ENV=staging` is combined with a known prod *IP literal*
> (`fleet_core.config._PROD_ENDPOINTS_RAW`; parses+canonicalizes the URL). It
> **cannot** catch a DNS hostname that resolves to the prod IP (a record added
> outside this repo; the guard never resolves DNS — see the guard note below), so
> it is only a secondary tripwire. Credentials, not this guard, are what keep
> staging and prod apart.

> **GO-gate.** Everything that provisions a host or starts a service on it is
> **operator-GO-gated (Rico)** and is NOT performed by this PR. This runbook is
> the design + repo-side prep; the host stand-up is the deploy step. The
> GO-gated items are listed explicitly in [GO-gated remainder](#go-gated-remainder).

---

## What already works (repo-side, this PR)

- `FLEET_ENV=staging` is a first-class env (`fleet_core.config._VALID_ENVS`).
- `{env}` flows cleanly through `subjects.py` and the `registry.py` templates
  (`CODE_SUBJECTS`, every entry keyed on `{env}`) — a staging client builds
  `staging.agent.…` subjects with **no code change** to the subject builders.
- The **prod-URL tripwire** (defense-in-depth, NOT isolation): constructing a
  `staging` `BusConfig` against a known prod *IP literal* raises `ValueError`. It
  is a **canonicalizing** check, not a string blocklist: it parses the URL,
  defaults the missing port to `4222` (as nats-py does before dialing), folds
  every numeric IP spelling (decimal-dword / hex / dotted-hex via the libc parser,
  IPv4-mapped IPv6 down to its v4 form), canonicalizes IPv6 (incl. `[::1]`
  loopback), strips userinfo, and compares the `(host, port)` tuple — so portless,
  numeric, mapped-v6, and FQDN-dot rewrites of a prod IP are all caught, not just
  the literal typo. A dotted-octet form with a **leading-zero octet**
  (`0192.0.2.74`) is deliberately **rejected as malformed** rather than folded,
  because its octal-vs-decimal reading diverges across platforms (glibc/BSD read
  octal; some stacks read decimal) and the guard must not fold to a value the
  dialer might disagree with.

  > **This tripwire is not fail-closed isolation.** It only catches a prod *IP
  > literal* spelled into the URL. It **cannot** catch a DNS hostname that
  > resolves to the prod IP — `FLEET_ENV=staging FLEET_NATS_URL=nats://fleet-hub.internal:4222`
  > constructs fine whenever that name resolves to `192.0.2.74`, because the
  > guard never resolves DNS (doing so would hang config construction) and the DNS
  > record lives outside this repo. **Distinct staging credentials (§2.1) are the
  > real boundary**; this guard is a secondary tripwire for the common typo only.

  > **DNS coupling (adopt-a-name checklist).** If a prod **DNS name** is ever
  > adopted for the hub, adding it in **two** places narrows the tripwire's blind
  > spot (it does NOT make the tripwire fail-closed): (1) `_PROD_ENDPOINTS_RAW` in
  > `packages/fleet-nats/src/fleet_nats/config.py`, and (2) the `case` prod-URL
  > list in `specs/staging-isolation-drill.sh`. Credentials remain the boundary
  > regardless.

  > **Localhost-tunnel caveat (P3-8).** The guard treats `localhost:4222` /
  > `127.0.0.1:4222` / `[::1]:4222` as **prod** (they're the on-hub SSH-tunnel
  > shorthands that land on the prod server). So an operator tunneling to the
  > **staging** box from their workstation must bind the local end to a
  > **non-4222** port — e.g. `ssh -L 4223:localhost:4222 staging-host` and set
  > `FLEET_NATS_URL=nats://localhost:4223`. A local `localhost:4222` forward is
  > indistinguishable from the prod tunnel and is refused under `FLEET_ENV=staging`.
- **Fail-loud on missing `FLEET_ENV` with an overridden URL** (MED-4): if
  `FLEET_NATS_URL` is set to a non-default (staging) value but `FLEET_ENV` is
  unset or blank, `BusConfig.from_env()` **raises** (naming `FLEET_ENV`) instead
  of silently defaulting to `dev`. So a staging `EnvironmentFile` that forgets the
  `FLEET_ENV=staging` line **fails to start** rather than quietly running on
  `dev.*` subjects — ambiguous deployment state is refused, not masked.
- `fleet_orchestrator` reads its bus config via `BusConfig.from_env`, so the
  shadow hub inherits staging support, the tripwire, and the missing-env fail-loud
  with no orchestrator code change — you point it at the staging URL by env alone.
- `specs/staging-isolation-drill.sh` — the end-to-end isolation drill (below).

So a client/hub runs **wholly against staging** by setting exactly two env vars:
`FLEET_NATS_URL=<staging box>` and `FLEET_ENV=staging`.

---

## Current deploy mechanic (established from repo docs/CI — cite)

The runbook's deploy steps mirror the mechanics already documented in-repo, so
the staging stand-up is the SAME shape the fleet already uses, not a new one:

- **Recent deploys were pull + service restart on the hub.** The
  fleet-orchestrator hub and the nightly canary both run as **systemd units on
  fleet-hub (fleet-hub)** — `docs/runbook.md` §"Orchestrator hub — the Phase-9
  run-loop service (#9)" and §"Nightly organic canary (#89)". The install is a
  **provision (pull) + `systemctl enable --now`** cycle; rollback is
  `systemctl disable --now`.
- **Provisioning is decoupled from enabling.** `scripts/install-agent.sh`
  idempotently provisions a dedicated venv at `/opt/fleet-bus/.venv`
  (`FLEET_BUS_HOME` default `/opt/fleet-bus`) with `fleet-nats` +
  `fleet-subscriber` installed and import-verified; the orchestrator adds
  `pip install -e …/packages/fleet-orchestrator`. Source resolution:
  `FLEET_BUS_SRC` → the collab mount → the checkout it runs from
  (`docs/runbook.md` §"Self-provisioning install").
- **Effective-state verification, never config-file trust.** Every enable in the
  repo is verified by probing the *running* system (`/connz`, `journalctl`),
  never by trusting the unit file — mirrored below.
- **NATS server config lives on the host.** The prod server config is
  `/home/fleet/nats/nats-server.conf` on fleet-hub, with the monitor on `http_port`
  **8223** (8222 is that server's WEBSOCKET listener — see
  `specs/probe-trio-subscribers.sh` and `docs/runbook.md` Troubleshooting). The
  staging server gets its own config on its own host, below.

The one delta staging adds: a **separate host** (the prod deploys all landed on
fleet-hub). Host choice + provisioning is the GO-gated step.

---

## 1. Proposed staging host requirements (do NOT pick the IP here)

Leave the host choice + provisioning to the GO-gated deploy step. Requirements:

| Requirement | Value / note |
|---|---|
| Kind | Its **own** LXC / VM — a **separate failure domain** from fleet-hub. Not a container on the same host, not a second process on fleet-hub. |
| Network | On the fleet segment, reachable from the agent hosts and the hub, on a **distinct IP** from `192.0.2.74`. |
| NATS | `nats-server` ≥ the prod version, **JetStream enabled** (the pilot; prod is core-only today). |
| Ports | client `4222`, monitor `http_port` (use **8223** to match the prod convention and the probes). |
| Python | 3.13 (fleet standard) for the shadow hub venv. |
| Disk | JetStream file store needs disk — size for the pilot's streams (small; it's a testbed). |
| DNS (optional) | Raw IP is the default for the pilot. A fleet-DNS name is optional; if adopted, add the Pi-hole record via the **deploy-service** / **pihole** skill AND register the name in the guard (see the adopt-a-name checklist above). |

> **No Caddy / no HTTP proxy.** Staging exposes **raw NATS ports** (`4222`
> client, `8223` monitor), NOT HTTP services, so Caddy is **intentionally not**
> in this stand-up — the fleet's usual reverse-proxy step does not apply. The
> `8223` monitor is a plain HTTP endpoint you hit directly by IP for the drill's
> `/connz` / `/jsz` checks; it is not fronted by a proxy.

The IP/hostname is **deliberately unspecified** — it is chosen and provisioned at
GO time (deploy-service / Ansible / Pi-hole per the fleet infra conventions),
then written into the staging `EnvironmentFile` below.

---

## 2. NATS server config sketch (JetStream enabled — the pilot)

On the staging host, `/home/fleet/nats/nats-server.conf`. The **deltas from prod
are (1) the `jetstream {}` block** (why this must be a separate box — you can't
enable it on prod to test it) **and (2) a DISTINCT staging account/credentials**
(§2.1 — the real isolation boundary; the credential *value* is minted at GO time,
NOT in this repo):

```
# staging fleet NATS — SEPARATE host from fleet-hub (fleet-bus #105)
listen: 0.0.0.0:4222

# Monitor on 8223 to match the prod convention + the repo probes
# (specs/probe-trio-subscribers.sh, docs/runbook.md). Do NOT use 8222.
http_port: 8223

server_name: fleet-staging

# --- AUTH: the isolation boundary (fleet-bus #105 HIGH-2) --------------------
# A staging server MUST require auth, or a prod-labeled client that resolves to
# the staging IP connects freely (and the reverse). The staging credential is a
# DISTINCT identity from prod so that prod creds are INVALID here and staging
# creds INVALID on prod. Mint the value at GO time via the approved secret path
# (Vaultwarden), never commit it. Two acceptable shapes — pick one and keep it
# a twin of prod's auth model:
#
#   authorization {
#       # token or user/pass — the VALUE comes from the secret store at deploy,
#       # not this file in git. Placeholder shown; never a real secret in-repo.
#       user: "fleet-staging"
#       password: "$STAGING_NATS_PASSWORD"   # injected at provision, not committed
#   }
#
#   # OR (preferred, matches an nkey/creds fleet): an accounts + user block whose
#   # nkey/creds seed is minted for STAGING only. prod's creds do not authenticate
#   # against this account.
#
# TLS (preferred): give the staging server its own TLS identity so clients also
# verify they reached the staging box, not a look-alike:
#   tls {
#       cert_file: "/home/fleet/nats/tls/staging-server-cert.pem"
#       key_file:  "/home/fleet/nats/tls/staging-server-key.pem"
#   }

# JetStream PILOT — the whole reason staging is a separate server. Never enabled
# on prod until proven here.
jetstream {
    store_dir: /home/fleet/nats/jetstream
    max_memory_store: 256MB
    max_file_store: 2GB
}
```

> Keep the staging config a **twin** of prod except the JetStream block and the
> DISTINCT staging credential/identity, so a topology change proven on staging
> transfers to prod without config surprises. When the JetStream pilot graduates,
> the same block is what lands on prod — but the credentials stay separate.

**JetStream `store_dir` must be writable by the `nats-server` run user.**
JetStream silently degrades / fails to enable if it can't write its store, and a
`/connz` that answers does **not** prove JetStream came up. The prod NATS config
lives under `/home/fleet/nats/` on fleet-hub (§"Current deploy mechanic"), i.e.
`nats-server` runs as the **`fleet`** user, so before the first start create the
store dir owned by that user:

```
# as root on the staging host, BEFORE first `systemctl start nats-server`
install -d -o fleet -g fleet -m 0750 /home/fleet/nats/jetstream
```

> If your staging unit runs `nats-server` as a different user, substitute it in
> both `-o`/`-g` **and** confirm the unit's `User=` matches — a store dir owned
> by the wrong user is the most common "JetStream didn't enable" failure.

**GO verify — prove JetStream is actually enabled, not just that NATS answers.**
`/connz` responding only proves the client listener is up. Confirm JetStream via
the dedicated monitor endpoints:

```
# jetstream must report enabled + the configured store dir
curl -fsS http://<STAGING_HOST>:8223/jsz | grep -E '"config"|"store_dir"|"memory"'
# /varz also carries a jetstream block when enabled
curl -fsS http://<STAGING_HOST>:8223/varz | grep -A2 '"jetstream"'
```

A `/jsz` that 404s or a `/varz` with no `jetstream` block means JetStream did
**not** enable (usually the `store_dir` ownership above) — do not proceed to the
pilot until `/jsz` reports the store.

---

## 2.1 Distinct staging credentials — the REAL isolation boundary (HIGH-2)

**This is what actually keeps staging and prod apart. The separate host and the
`BusConfig` IP tripwire are necessary but NOT sufficient.** Without credentials,
the staging server above listens on `0.0.0.0:4222` with no authorization: any
fleet client that receives the staging IP connects freely — **including a
prod-labeled client** — and, via a DNS alias that resolves to the prod IP, a
staging client can reach prod. Isolation would be only an operator convention.

**Requirement (GO-gated — the credential VALUE is minted outside this repo and is
NEVER committed):**

1. **Mint a DISTINCT staging account/credential** (token, user/pass, or —
   preferred — an nkey/creds identity) such that:
   - **prod credentials are INVALID on staging**, and
   - **staging credentials are INVALID on prod.**
   Do not reuse the prod credential or the prod account. Reusing it re-collapses
   the failure domain the separate host was meant to split.
2. **Preferably give the staging server a TLS identity** (`tls {}` above) so a
   client also verifies it reached the *staging* box and not a look-alike on the
   staging IP.
3. **Deliver the credential via the approved secret path only:** store it in
   **Vaultwarden**, materialize it into the client `EnvironmentFile` at provision
   time (mode `chmod 600`, root-owned), and reference it by env var. **Never**
   inline the secret as `Environment=` in a committed unit, never in this repo,
   never in a log or PR.
4. **Client mechanism = F2 (#104) `FLEET_NATS_CREDS`.** The staging client
   (subscriber/hub) presents its staging credential through the F2 client creds
   mechanism (`FLEET_NATS_CREDS`, delivered from Vaultwarden → `EnvironmentFile`).
   That is how a staging client authenticates to the staging account and — because
   the credential is staging-only — is rejected by prod. The client
   `EnvironmentFile` in §3 therefore MUST carry `FLEET_NATS_CREDS` pointing at the
   deployed staging creds file; a client with no credential, or with the prod
   credential, must **fail to authenticate** against the staging server.

> **Bottom line:** stand up the staging server *with* its distinct account/creds
> (and ideally TLS), or you have a second unlocked door, not an isolated testbed.
> The `EnvironmentFile` below is incomplete until it carries the staging
> `FLEET_NATS_CREDS`.

---

## 3. Shadow-hub deployment (fleet-orchestrator pointed at staging)

The shadow hub is `fleet_orchestrator` — **the same code as prod**, pointed at
the staging server by env only. Same venv convention as prod
(`docs/runbook.md` §"Deploy on fleet-hub"): provision, systemd unit, verify
effective state. It can run **on the staging host itself** (keeps the whole
testbed in one failure domain) or on any host that can reach the staging URL.

`/etc/fleet-bus/orchestrator-staging.env` (the `EnvironmentFile`; mode `600`,
root-owned). The staging URL + `FLEET_ENV=staging` label it staging, but
**`FLEET_NATS_CREDS` is what makes it ISOLATED** — it points at the deployed
staging creds file (from Vaultwarden, §2.1), so this hub authenticates to the
staging account only and is rejected by prod. Omitting `FLEET_ENV` here now
**fails loud** (MED-4), and omitting `FLEET_NATS_CREDS` against an auth-required
staging server means the hub **cannot connect** — both are intentional:

```
FLEET_NATS_URL=nats://<STAGING_HOST>:4222   # the SEPARATE box — never fleet-hub
FLEET_ENV=staging                           # REQUIRED (from_env fails loud if missing)
FLEET_NATS_CREDS=/etc/fleet-bus/staging.creds  # F2 (#104); deployed from Vaultwarden, chmod 600
FLEET_AGENT_ID=nagatha
FLEET_MANAGER_ID=nagatha
FLEET_WORKERS=skippy,bilby
```

> **`FLEET_NATS_CREDS` value is a secret path, deployed at provision time — never
> commit the creds file, and never inline the credential as `Environment=`.** The
> path shown is illustrative; the real file is materialized from Vaultwarden with
> `chmod 600`.

`/etc/systemd/system/fleet-orchestrator-staging.service` (a distinct unit name +
connection name so it can never be mistaken for the prod hub on a `/connz`):

```
[Unit]
Description=Fleet SHADOW orchestrator hub (staging — fleet-bus #105)
After=network-online.target
Wants=network-online.target

[Service]
EnvironmentFile=/etc/fleet-bus/orchestrator-staging.env
Environment=FLEET_NATS_NAME=fleet-orchestrator-hub-staging
ExecStart=/opt/fleet-bus/.venv/bin/fleet-orchestrator
Restart=on-failure
RestartSec=5
KillSignal=SIGTERM

[Install]
WantedBy=multi-user.target
```

Provision + enable (GO-gated):

```
# FLEET_RELEASE_SHA=<sha of the checkout that was shipped here>: /opt/fleet-bus/src
# has no .git, so without it package_version reports null. See docs/runbook.md.
FLEET_RELEASE_SHA=<sha> FLEET_BUS_SRC=/opt/fleet-bus/src bash /opt/fleet-bus/src/scripts/install-agent.sh
/opt/fleet-bus/.venv/bin/pip install -e /opt/fleet-bus/src/packages/fleet-orchestrator
systemctl daemon-reload && systemctl enable --now fleet-orchestrator-staging
```

Verify **effective state** (never trust the unit file):

- `journalctl -u fleet-orchestrator-staging` shows
  `orchestrator.hub | ready connection=fleet-orchestrator-hub-staging`.
- The **staging** monitor `/connz` (`http://<STAGING_HOST>:8223`) shows the
  `fleet-orchestrator-hub-staging` connection.
- The **prod** monitor `/connz` (`http://192.0.2.74:8223`) does **NOT** show it
  — the shadow hub never touched prod.

---

## 4. End-to-end topology drill (proves isolation — ZERO prod traffic)

`specs/staging-isolation-drill.sh` is the acceptance drill. It drives a **REAL
end-to-end orchestrator session** against staging and audits prod **across the
whole drill window**, so it can no longer PASS with the shadow hub stopped or
prove-nothing on a single snapshot (fleet-bus #105 HIGH-3):

| Assertion | What it proves | How |
|---|---|---|
| **GUARD** | a misconfigured drill can't publish to a prod IP literal | refuses if `FLEET_STAGING_NATS_URL` is a known prod URL, *and* `BusConfig` refuses `staging`+prod-IP in-process. (Defense-in-depth, not isolation — §2.1 creds are the boundary.) |
| **E2E** | the shadow hub actually **ran the topology** on staging | starts a **worker dummy** on staging, then drives `fleet-orchestrator --once --dry-run --task-id <drill> --task-text …`; PASS **requires** the hub log line `task=<drill> reached DONE` — i.e. a real dispatch → status/relay → done round-trip on `staging.*`. No DONE ⇒ **FAIL** (hub down / worker absent / timed out). Connection-presence alone is **not** sufficient. |
| **POSITIVE** | the staging bus carried the exercise | the drill's `fleet-orchestrator-hub-<drill>` / `fleet-nats-<drill>-<worker>` connections appear on the **staging** `/connz`. |
| **NEGATIVE (durable)** | **prod was untouched across the window** | a background poller reads the **prod** `/connz` on a ~1s cadence for the entire drill and **FAILs** if either drill connection name shows up at ANY poll — narrowing the connected-then-disconnected blind spot a single snapshot misses. |

Why the durable prod audit: a single `/connz` snapshot misses a connection that
touched prod and disconnected before the snapshot, and cannot see a connection
under another name. Polling the prod monitor for the whole window — and treating
any **gap** in prod reachability as a **SKIP, not a pass** (you cannot prove a
negative through a blind spot) — is what lets a PASS actually mean "zero prod
traffic". The drill also **SKIPs** (never passes) if the prod baseline read or
the staging monitor is unreachable at the start.

Run (GO-gated — needs the staging box up **with its staging creds**, §2.1):

```
FLEET_STAGING_NATS_URL=nats://<STAGING_HOST>:4222 \
FLEET_STAGING_MONITOR_URL=http://<STAGING_HOST>:8223 \
FLEET_PROD_MONITOR_URL=http://192.0.2.74:8223 \
FLEET_DRILL_PYTHON=/opt/fleet-bus/.venv/bin/python \
FLEET_DRILL_WORKER=skippy \
FLEET_DRILL_ID=staging-drill \
    sh specs/staging-isolation-drill.sh
```

Exit codes: `0` PASS (real e2e ran on staging, prod clean across the window —
**#105 acceptance**), `2` SKIP (a monitor/tool unreachable/absent, or prod went
unreachable mid-window — cannot verify), `1` FAIL (leaked to prod, or the hub
never reached DONE so the exercise did not actually run).

> **Note on `--once`:** the orchestrator's `--once` **requires** `--task-id`, and
> resolves the dispatch prompt from either an explicit `--task-text` (what the
> drill uses — no k-board card needed) or, if omitted, the k-board card fetched by
> `--task-id`. So a card-free dry smoke is
> `fleet-orchestrator --once --dry-run --task-id <id> --task-text "<prompt>"`;
> use a real `--task-id` **without** `--task-text` only when you have first created
> a staging k-board card for `<id>`. The isolation drill wraps this with the
> worker + two-ended durable proof.

---

## 5. Rollback

The staging server + shadow hub hold **no prod state** — rollback is local to
the testbed and never touches prod:

1. **Shadow hub:** `systemctl disable --now fleet-orchestrator-staging`. The hub
   holds no durable state of its own (session state is in-memory; any k-board
   card is the record), so stopping it just pauses the testbed.
2. **Staging server:** stop `nats-server` on the staging host. Because it is a
   **separate box**, stopping it has **zero** effect on prod (that separation is
   the entire point of #105). The JetStream file store under
   `/home/fleet/nats/jetstream` can be wiped between pilots — it's a testbed.
3. **Decommission (if abandoning the testbed):** tear down the LXC/VM via the
   fleet infra path (remove-service / Ansible / Pi-hole cleanup). No prod
   config, DNS, or unit is touched.

There is **no rollback path that can affect prod**, by construction — staging
never had a hand on the prod server.

---

## GO-gated remainder

Exactly what needs **Rico's GO** (new-host provisioning + service start on it);
none of it is in this PR:

> **ISOLATION IS NOT ACHIEVED UNTIL THE STAGING CREDENTIALS EXIST (step 2b).**
> Standing up a separate staging host **without** distinct staging
> account/credentials does **NOT** isolate staging from prod — a prod-labeled
> client that resolves to the staging IP would connect, and a staging client that
> resolves a DNS alias to the prod IP could reach prod. The separate host and the
> `BusConfig` IP tripwire are **necessary but not sufficient**; the tripwire is a
> secondary check for the literal-IP typo, not the boundary. Do not treat "the
> staging box is up" as "staging is isolated" until steps 2b + 3 (creds minted,
> distinct from prod, delivered via Vaultwarden → `EnvironmentFile`) are done and
> a **credential-required** drill PASS (step 4) is recorded.

1. **Provision the staging host** — choose the IP/hostname and stand up the
   separate LXC/VM on the fleet segment (deploy-service / Ansible / Pi-hole per
   fleet infra conventions).
2. **Install + configure `nats-server` with JetStream** on that host
   (§2 config sketch) and start it.
2b. **Mint DISTINCT staging credentials + (preferably) a TLS server identity**
   (§2.1) — a staging account/creds such that **prod creds are INVALID on staging
   and staging creds INVALID on prod**. Store the value in **Vaultwarden**; it is
   minted outside this repo and is **never committed**. This is the actual
   isolation boundary; the separate host without it is an unlocked second door.
3. **Provision + start the shadow hub** — `install-agent.sh` +
   `pip install -e …/fleet-orchestrator`, write the staging `EnvironmentFile`
   (including `FLEET_NATS_CREDS` from Vaultwarden, `chmod 600`; and
   `FLEET_ENV=staging`, which now fails loud if omitted) and unit (§3),
   `systemctl enable --now fleet-orchestrator-staging`.
4. **Run the isolation drill** (§4) from an on-network host **with the staging
   creds in place** and record the **PASS** — a real e2e session ran on staging
   and prod stayed clean across the window. That PASS is what closes #105 (a SKIP
   is not acceptance; a PASS against an auth-less staging server does not prove
   isolation, only that the drill's own connection did not appear).
