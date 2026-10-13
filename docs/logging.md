# Logging convention (fleet-bus)

Structured, sanitized logging is a **standing fleet requirement**, not a feature —
it makes coordination observable and is the seed of the replay/backtesting
capability. It is written *into* each module as the module is built (a Phase 1–5
acceptance criterion), never bolted on afterward. This doc is the shared
convention every package follows, plus the audit gate that proves they do.

Logger: **`loguru`** (`from loguru import logger`) — the one fleet-bus logging dep.

## Incident boundary tracing

Set `FLEET_TRACE=1` before starting one fleet process to emit sanitized `in`,
`out`, and `error` records for its decorated public boundaries. Tracing is off by
default and must be enabled only on the host/process being investigated. Set
`FLEET_TRACE_FILE=/var/log/fleet-bus/boundary-trace.log` to tee only those
explicitly tagged trace records (never the process's ordinary Loguru records) to
a 50 MB rotating Loguru file sink. Rotated files are ZIP-compressed and retained
for seven days; without the variable, records use the process's normal Loguru
sinks.

```zsh
FLEET_TRACE=1 FLEET_TRACE_FILE=/var/log/fleet-bus/boundary-trace.log <service command>
```

Restart the process after changing either variable, reproduce the incident, then
collect the normal service journal and optional trace file. Remove both variables
and restart when collection is complete. Arguments and results are bounded and
credential-shape redacted; envelope payloads and prose-bearing mapping fields are
never rendered. Tracing failures are logged and swallowed so observability cannot
take down the operation.

This boundary trace is separate from the orchestrator's machine-readable
`FLEET_TRACE_PATH` coordination-decision JSONL described below.

## The never-log list (hard rule)

**Never** put any of these in a log statement — treat Discord as leaked:

- secrets, API tokens, passwords, `Bearer` tokens
- Discord/Slack webhook URLs
- Discord member IDs (and, by extension, any raw Discord snowflake that identifies
  a human)
- **envelope payload contents** — agent prose, task text, user messages. The
  payload can carry anything; it is never logged.

**Safe to log** (coordination metadata, carries no secrets):

- `task_id`, `topic`, `correlation_id`
- NATS `subject`s (built only via `fleet_nats.subjects`)
- envelope `kind`, `to`, `sender`/`agent_id`
- coordination `state`, `event`, `action`, `status`
- error *reasons* (`exc` string) at failure points — an exception message from the
  transport, not a payload dump

Rationale: `task_id`/subjects/`kind`/`agent_id` are the fleet's own coordination
identifiers; they are what you need to reconstruct a decision and nothing a
customer or credential leaks through. Payloads and Discord identifiers are where
secrets and PII live, so they never appear.

## Line format

One convention, everywhere:

```
logger.<level>("<module>.<operation> | key={} key={} ...", value, value, ...)
```

- **`<module>.<operation>`** — a stable prefix so a grep finds every line for a
  behavior (e.g. `fleetbus.publish`, `orchestrator.dispatch`,
  `fleet_subscriber.send`, `kboard.update_status`).
- **`|`** separates the prefix from the sanitized key/value context.
- Use loguru's `{}` binding (`logger.info("... task={}", task_id)`) — never
  f-strings — so values are structured, not pre-interpolated.

Examples from the codebase:

```python
logger.debug("fleetbus.publish | subject={} kind={}", subject, envelope.kind)
logger.info("orchestrator.dispatch | subject={} worker={} task={}", subject, worker, session.task_id)
logger.info("kboard.update_status | task={} status={} version={}->{} attempt={}", ...)
```

## Where a log line is required

This is the **convention P1–P5 are built to meet**, enforced per-phase in each
phase's review/gauntlet (logging is a standing acceptance criterion), not by a
single coverage gate — a grep can prove *absence* of forbidden patterns but not
*presence* of a log line at every future branch. As of Phase 6 every point below
is covered; new code must keep it that way.

Log at these five points (rtech-hermes coding standard — entry/exit/failure/
fallback/guard):

1. **Every coordination transition** — the orchestrator logs *which state, which
   event, which transition/action* so the decision is reconstructable. This is the
   replay seed: `next_action` is pure and every `_perform` branch logs its action.
2. **Every bus publish / subscribe / request / reply** — a sanitized line naming
   the subject and kind (see `fleet_nats.bus`).
3. **Every subscriber ingress and reply** — envelope in, reply out, both by
   subject/task, never payload (see `fleet_subscriber.adapter`).
4. **Every failure / fallback** — log *before* returning a fallback or re-raising.
   There is **no silent `except: pass`** anywhere; a caught exception is always
   logged with its reason first (`docs/coding-standards.md`).
5. **Every guard trigger** — when a defensive guard refuses an action (a
   targetless RELAY/WAKE, a poison request dropped, a bad envelope), it logs why
   at `warning`/`error` before returning.

## Levels

- `debug` — high-frequency, low-signal (per-publish, per-message decode).
- `info` — a coordination event worth seeing in normal operation (connect,
  dispatch, relay, arbitrate decision, persist, done).
- `warning` — a recoverable anomaly (CAS conflict → refetch, bad request dropped,
  no responders, timeout).
- `error` — a failure that breaks the operation (publish failed, request failed,
  k-board non-2xx) — always logged before the raise/fallback.

## JSONL coordination trace (#22 — shipped)

The optional machine-readable JSONL coordination trace — deferred out of #10,
now shipped as `fleet_orchestrator.trace`. **Off by default in dev; ON by
default in prod** (`FLEET_ENV=prod`): a prod hub captures its replay trace at
`/var/lib/fleet-orchestrator/trace/coordination.jsonl` even when
`FLEET_TRACE_PATH` is unset, so an incident always leaves a trace (#108). An
explicit `FLEET_TRACE_PATH` names the file to append to and overrides the prod
default in either env; in dev an unset path means the no-op sink (no file is
ever created, zero I/O).

- One record per coordination **decision** (`apply()`: ts, state, event,
  action, task_id, target) and one per **bus operation**
  (dispatch/relay/wake: ts, action, task_id, target, subject) — the
  transition and dispatch/relay/wake log points in machine-readable form for
  replay/backtesting. The **arbitration reply is logged but not yet traced**:
  `serve_arbitration` is topic-keyed with no task binding at reply time; its
  trace record lands with the Phase-5/6 manager-loop work that binds topics
  to tasks (recorded on #13/#17, not a silent gap).
- The record shape is a closed dataclass (`TraceRecord`); the never-log list
  above is its negative space — message bodies, secrets, webhook URLs, and
  member ids cannot enter it.
- Timestamps come from the orchestrator's injected `IdClock`, never a wall
  clock, so traced runs stay reproducible under test.
- A sink write failure is logged and swallowed (first at **error** — prod
  defaults the trace ON and depends on it, so the first failure must be loud;
  repeats at debug) — tracing never takes the control loop down. `sink_from_env`
  also probes the destination for writability at wiring time and logs a loud
  `trace.configure | JSONL trace path NOT WRITABLE` ERROR at startup if it can't
  write, so the `ENABLED` posture line can never lie about capture (#108).
- Durability: each JSONL line is flushed and `os.fsync`'d before `emit` returns
  (one fsync per coordination record — not a hot loop), so a record `emit`
  accepted survives an OOM-kill / SIGKILL / power-loss immediately after. The
  final line may still be a trailing partial write if the process is killed
  *mid-write* (before fsync); replay must tolerate a single trailing partial JSON
  line at EOF. (The `os.fsync` is a synchronous call on the coordination path; on
  local persistent disk at coordination cadence this is an intentional durability
  trade — if a degraded disk ever stalls the hub loop, the documented mitigation
  is to offload the write to a thread/queue.)
- **Retention — the trace is self-bounded (review P2 #133).** The prod-default
  trace is ON and append-only, so `JsonlTraceSink` rotates **in-sink** and does
  not depend on deploy-side logrotate/tmpfiles. When the active file reaches
  `_MAX_TRACE_BYTES` (128 MiB) it is renamed through a ring of `_MAX_TRACE_BACKUPS`
  (5) numbered backups (`coordination.jsonl.1` … `.5`, the stdlib
  RotatingFileHandler convention), discarding the oldest — so total on-disk trace
  is hard-bounded at ~768 MiB regardless of uptime. Replay reads newest-first:
  the active file, then `.1` … `.5`. A rotation error degrades exactly like a
  write failure (logged, swallowed) and never crashes coordination.
- The sink is resolved from the env at **first emit** and then cached for the
  process lifetime: set `FLEET_TRACE_PATH` before the service starts, and
  restart to enable/disable/redirect it. (Startup wiring may pin it explicitly
  with `trace.set_sink(trace.sink_from_env())` after config is loaded; the
  fail-loud `trace.configure | JSONL trace ENABLED path=…` / `… DISABLED` info
  line always states the trace posture at startup so an operator can see at a
  glance whether an incident will be captured.)

```bash
# dev: opt in explicitly (prod is on by default at the path below)
FLEET_TRACE_PATH=/var/log/fleet-bus/coordination.jsonl
# prod: on by default even when FLEET_TRACE_PATH is unset —
#   /var/lib/fleet-orchestrator/trace/coordination.jsonl
```

### Payload guard (#108)

The publish path fails loud BEFORE the wire call when an encoded envelope exceeds
the NATS `max_payload` (server-negotiated, 1 MiB default): `FleetBus.publish`
(and `request`/`reply`) raise `PayloadTooLargeError` (a `ValueError` subclass)
naming the subject, the actual size, the limit, and the **truncation policy:
NONE**.

**What this actually replaces.** nats-py (>=2.15) already rejects an over-limit
publish *client-side, before any bytes hit the socket* — `Client.publish` raises
`nats.errors.MaxPayloadError`. So this is NOT a silent server-side drop; the
caller already gets a synchronous exception. The problem the guard solves is that
`MaxPayloadError` is **opaque**: it names neither the offending subject, the
actual size, the limit, nor the truncation policy, so it reads as an unexplained
failure in an incident review. `PayloadTooLargeError` replaces that opaque error
with a typed, policy-naming one plus a fail-loud log. The bus never truncates;
the caller must split the work, offload a large body to a side channel and
publish a reference, or raise the server `max_payload`. The guard logs
`fleetbus.publish | payload guard TRIPPED` at error before raising.

The measured size is payload-only (the server also counts subject + protocol
framing), matching nats-py's own payload-only pre-check — a deliberate slight
under-count, not exact wire accounting. Pre-connect the limit is nats-py's 1 MiB
default (not server-negotiated until connect); the normal flow connects first so
the authoritative limit is in force by the time anything publishes.

**Poison handling (an oversized card must not loop).** A `PayloadTooLargeError`
is PERMANENT — no retry will ever fit the payload. The intake poller
(`_publish_created`) and the hub's `start_task` rollback catch it *distinctly*
from a transient publish error: the card is dead-lettered on the board (flipped
to `blocked`, off `todo`/`running`) and the poller adds it to an in-process
poison set, so one 1.1 MiB card is attempted once and never re-picked — it does
not re-publish every few ticks forever.

## Audit gate

Two greps enforce this convention and run in the plan checker
(`specs/fleet-bus-nats-coordination.verify.sh`):

```bash
# No secret / webhook / member-id patterns in any log statement:
! grep -rnE 'logger\.(info|debug|warning|error).*(webhook|discord.*id|token|secret|bearer)' \
    packages/fleet-orchestrator/src packages/fleet-subscriber/src packages/fleet-nats/src

# No silent excepts (log-before-fallback enforced):
! grep -rnE 'except[^:]*:[[:space:]]*pass' \
    packages/fleet-orchestrator/src packages/fleet-subscriber/src packages/fleet-nats/src
```

A clean tree → grep finds nothing → grep exits non-zero → the leading `!` inverts
that to exit 0, a passing gate. (A match → grep exits 0 → `!` → non-zero → fail.)

**What the gate does and does not prove.** It is a **regression backstop**, and a
deliberately narrow one — it catches a forbidden pattern
(`token`/`secret`/`webhook`/`discord…id`/`bearer`) appearing *inline* (on the same
physical line) in a log statement. It does **not** catch:

- a secret laundered through an intermediate variable
  (`x = user.discord_id; logger.info("id={}", x)`);
- a forbidden token on a *different* line of a multi-line `logger.<level>(` call;
- the two-line silent-except form (`except Exception:` / `pass` on the next line —
  the regex is single-line);
- member-id shapes other than the literal `discord…id` (`member_id`, `user_id`,
  `snowflake`).

The real guarantee is **structural** — payloads and PII are never passed to a
logger in the first place: every log line takes coordination metadata
(`task_id`/`subject`/`kind`/`state`/…), and "no payload/PII in logs" plus
"log-before-fallback, no silent except" are checked in **each phase's code
review**. The greps guard against a *future regression* of that discipline; they
are not the primary defense, and they do not prove positive log-line coverage
(a grep proves absence of a pattern, not presence of a log line at every branch).
