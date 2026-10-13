# Subscriber live verification — NATS dispatch → agent's own words in Discord

The `fleet-subscriber` plugin's unit tests prove the envelope→`MessageEvent`
mapping and the subscribe/`send` wiring against a gateway stub. Unit tests are
**necessary but not sufficient** (repeated fleet lesson): the plugin is only
proven when a real NATS dispatch produces the **agent's own words** in the target
Discord channel, on a real Hermes agent host.

This document is the reproducible procedure for that live verification. The
**evidence capture** step runs during the Phase 7 deploy (`#8`) against a real
agent on the trio — it cannot run from the build host, because it needs the
gateway runtime installed and a live agent turn. Until then, the procedure below
is the source-of-truth for how the check is performed and what counts as pass.

## What is being verified

1. The plugin registers as a Hermes platform (`register_platform(name="nats")`)
   and its adapter `connect()` subscribes to the agent's coordination subjects.
2. A NATS dispatch envelope — bare (`{env}.agent.dispatch.{agent}`) or
   per-channel (`{env}.agent.{channel}.dispatch.{agent}`; the adapter subscribes
   both forms) — is forged into a synthetic `MessageEvent(internal=True)` and
   run as a **full agent turn**.
3. The agent's reply is the agent's **own words** (its voice), posted to the
   target Discord channel/thread — not a canned echo, not the orchestrator's
   words.
4. `internal=True` bypasses the mention-gate and user-auth (so the dispatch runs
   without an @-mention), while `DISCORD_ALLOW_BOTS != none` stays in effect on
   the receiving side (safety preserved — see the runbook).

## Preconditions (on the agent host)

- Hermes gateway v0.17+ running (`hermes-gateway.service`).
- The `fleet-subscriber` plugin installed into the gateway's plugin dir
  (`~/.hermes/hermes-agent/plugins/`), with its `plugin.yaml` + `__init__.py`
  exporting `register`.
- Env set for the plugin: `FLEET_NATS_URL`, `FLEET_AGENT_ID`, `FLEET_ENV`.
- `nats-py` importable in the gateway venv (the plugin's `check_fn`).
- The fleet NATS server reachable (`nats://192.0.2.74:4222`, fleet-hub).

## Procedure

1. **Confirm the platform registered.** In the gateway logs after start, look for
   `fleet_subscriber.register | NATS platform registered` and, on connect,
   `fleet_subscriber.connect | subscribed subject=...` for the agent's
   dispatch/relay/wake/inbox lanes. (Effective-state, not config assumption.)

2. **Dispatch a task over NATS.** From a bus client (the local `direct.*` dev
   lane, or a manager), publish a dispatch envelope built via
   `fleet_nats.subjects.agent_dispatch(env, agent, channel=<target>)` with a
   payload carrying the prompt text:

   ```python
   from fleet_nats import BusConfig, Envelope, FleetBus, subjects
   cfg = BusConfig.from_env("dispatcher")
   async with FleetBus(cfg) as bus:
       subj = subjects.agent_dispatch(cfg.env, "<agent>", channel="<target-channel>")
       await bus.publish(subj, Envelope.new(
           msg_id="verify-1", ts="<iso>", sender="verify",
           kind="dispatch", task_id="verify-task",
           channel="<target-channel>",
           payload={"text": "In one sentence, confirm you received this over NATS."},
       ))
   ```

3. **Observe the agent's own words in Discord.** Within the turn budget, the
   agent posts its reply — in its own voice — to `<target-channel>` (or the
   thread if `thread_id` was set). This is the pass condition: a real agent turn
   surfaced as the agent's own words, triggered by a NATS dispatch with no
   @-mention.

4. **Capture evidence.** Record: the gateway log lines
   (`fleet_subscriber.ingress | forging turn ...` → `fleet_subscriber.send | ...`),
   the Discord message permalink/transcript ref, and the dispatch `task_id`.
   Attach these to the Phase 7 deploy PR / k-board card. **Do not mark the phase
   done on "it compiles" or unit tests alone.**

## Pass / fail

- **Pass:** the agent's own-words reply appears in the target channel, caused by
  the NATS dispatch, with the log trail `ingress → handle_message → send`
  captured.
- **Fail:** no reply (subscriber not connected / wrong subject / `check_fn`
  false), a canned echo instead of an agent turn (wrong injection path), or the
  reply blocked by `allow_bots=none` on the receiving side.

## Completion sentinel — the `[[done]]` trailer (#6)

The orchestrator needs a deterministic "this turn is done" signal, not a heuristic
over prose. Convention: an agent ends a coordination turn by appending a fixed
trailer to its final reply — **`[[done]]`** or **`[[done:<outcome>]]`** (e.g.
`[[done:failed]]`). It must be at the very *end* of the reply (a mid-prose mention
does not count — the match is end-anchored, case-insensitive).

On seeing the trailer, the subscriber:
1. strips it from the visible reply (humans see the clean text), and
2. emits a `kind="done"` **completion sentinel** on the task's status subject —
   `payload={"task_id", "outcome"}`, built via the shared
   `fleet_nats.done_envelope()` so the emitter and the orchestrator's
   `parse_done()` consumer can never drift on the wire shape.

The orchestrator's `watch_worker_status()` checks for the sentinel **first** (a
`done` envelope has no reply text, so it must not be run through missing-handoff
detection) and advances the session `WORKING → DONE`. The token is a
**trusted-agent convention**: like `internal=True` below, it rides the trusted
NATS control plane — a hostile publisher could forge a `done`, so the NATS ACLs
are the perimeter.

## Security boundary — `internal=True` means bus access is agent-turn authority

The subscriber forges every inbound envelope into a `MessageEvent(internal=True)`,
which (verified: gateway/base.py bypasses the mention-gate and user-auth when
`internal` is set) runs a **full agent turn with the publisher's prompt text and
no authentication**. This is architecturally intended — NATS is the trusted DAP
control plane — but it makes an explicit trust assumption:

> **Anyone who can publish to `{env}.agent.{id}.{dispatch,relay,wake,inbox}` can
> run an unauthenticated agent turn on that agent.** The auth perimeter is the
> NATS server's ACLs, not the plugin.

The mapping is conservative (typed envelope fields only, no `eval`/`exec` — see
`mapping.py`), so this is not code injection, but it *is* a prompt-injection /
unauthorized-turn surface. **Deploy consequence:** the fleet NATS server must
restrict publish access to these subjects to trusted fleet participants
(per-service NATS credentials, not an open bus). Confirm this at the Phase 7
deploy.

## Stub-vs-host note (for maintainers)

The unit tests exercise the adapter against a **stub** gateway API, so they prove
the envelope→turn→reply *logic* but cannot catch a future Hermes version
renaming or adding an abstract method (the stub is a plain class, not an `ABC`).
The authoritative check that `NatsAdapter` still satisfies `BasePlatformAdapter`
is **instantiating it on-host** — which the Phase 7 deploy does (a plugin that
fails the abstract-method contract raises `TypeError` at construction). Treat a
clean on-host `connect()` as the contract-conformance proof.

## Evidence

- Unit-level proof (this build): `packages/fleet-subscriber/tests/` — 14 tests,
  green, covering the four-lane subscribe wiring, `internal=True` forging, the
  unactionable-envelope skip, and the `send()`→publish path.
- Live-level proof (Phase 7 deploy): _pending the trio deploy — evidence
  (Discord permalink + gateway log trail + task_id) will be captured here and on
  the Phase 7 card when the plugin is enabled on a real agent._
