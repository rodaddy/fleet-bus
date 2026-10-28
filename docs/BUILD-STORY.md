# How fleet-bus was built

This repo was published on 2026-10-13 as a single scrubbed commit, so the
public git log is short. The full history lives in the author's private
repository: 161 non-merge commits from 2026-04-27 to 2026-10-27, plus the
contracts, plans and runbooks under `docs/` and `specs/`. This document is
taken from that commit history and from those docs. Issue and PR numbers
below refer to the private tracker and are given only to show sequence.

## The problem and the first version

The author runs a small group of long-running AI agents that work together on
tasks while people watch and steer in a chat channel. At first the agents
coordinated by talking to each other in chat. That failed in predictable ways:
mentions were missed, two agents answered the same message, hand-offs were
lost, and afterwards nobody could say who had owned a task.

The repository began on 2026-04-27 as a small A2A (agent-to-agent protocol)
server for infrastructure monitoring. On 2026-07-07 it was repurposed as
fleet-bus, with a new `fleet-nats` package: agent coordination would move off
the chat surface onto NATS, and chat would become display only.

## Timeline

| Dates | Phase | What landed |
| --- | --- | --- |
| 2026-04-27 | Predecessor | An A2A protocol server with a README; later repurposed. |
| 2026-07-07 | Plan and core build | A master plan with an executable checker (`specs/*.verify.sh`), the `fleet-nats` package, a pure state machine for the orchestrator, dispatch/relay/arbitrate/wake behaviours, a per-agent subscriber plugin, a completion sentinel, a task-board bridge with compare-and-set writes, the logging convention, and a CI gate that mirrors the pre-push hook. |
| 2026-07-08 to 07-09 | Delivery and governance | Reconnect, at-least-once de-duplication, dead-letter and an offline ladder; heartbeats and liveness; a back-channel governor with a single narrator; a scorekeeper that stops agents from simply agreeing with each other; Langfuse traces; a nightly canary run. |
| 2026-07-10 | Contracts | Six written contracts (C1-C6 in `docs/contracts/`): token-cost baseline, envelope compatibility, plane and QoS table, owner map, ordering and retry, envelope vs message body. Also a fleet-wide kill switch, staging guardrails, catalog-version enforcement, JetStream replay and a KV store with revision compare-and-set. |
| 2026-07-11 to 07-12 | `fleet-core` | A base client package extracted with its own config contract, logging, typed signals and transport policy. `fleet-nats`, the subscriber and the orchestrator were repointed onto it, and the old prose trailers parsed out of agent replies were replaced by typed signals. |
| 2026-07-13 to 07-15 | Release and observability | Immutable, sealed hub releases with remote deploy, an authenticated `show_work` trail reader, a shared header/data/extras/footer payload contract, package version stamped into heartbeats, a glossary. |
| 2026-07-27 to 07-28 | Lanes and channels | A publish-only telemetry lane with a scoped role, CI on self-hosted runners, subject lanes for noisy traffic, and a persisted channel map so adding a channel is a data row, not a redeploy. |
| 2026-09 to 10-27 | Maintenance | Moved to a self-hosted Forgejo instance, affected-test selection on PRs, CI clean-up, and the public release. |

## Key design decisions

- **Chat displays, code decides.** Agents still post in chat so people can read
  them, but nothing reads chat to decide control flow. Dispatch, relay, wake
  and control messages ride a typed `Envelope` on NATS.
- **A pure state machine as orchestrator.** `fleet_orchestrator.transitions`
  is a `next_action(state, event)` function with no model, clock or I/O. Every
  transition is a case in code with a unit test, and no language model sits in
  the coordination path.
- **Write the board first, then signal.** Task state lives on an external task
  board with compare-and-set updates. The orchestrator writes the board first
  and then emits the bus signal, so the record of who owns a task never lags
  the message that announces it.
- **Promise only what the transport does (C5).** The code once carried
  docstrings that said "the bus is at-least-once", which the deployed
  transport did not honour. Contract C5, ratified 2026-07-10, states the real
  promise: ordering only within one `(channel, task)` pair, at-least-once with
  idempotent consumers on JetStream lanes, at-most-once stated plainly
  everywhere else, and an idempotency key named for every retry path.
- **Typed signals instead of parsed text.** Early subscribers scraped
  `[[done]]` and `[[stance]]` markers out of agent replies. On 2026-07-12 and
  07-14 those were replaced by typed fields on the envelope and the text
  trailers were removed.
- **Unknown fields are preserved (C2).** The envelope keeps fields it does not
  recognise and fails loudly on an unsupported version, so a newer producer
  cannot be silently truncated by an older consumer.

## How AI models and agents were used

- The build ran as planned goal runs: a written master plan with numbered
  issues, an executable checker that tests whether each plan item is really
  done, and a runbook for the autonomous build run (2026-07-07).
- Agents wrote the code in parallel lanes; the commit log names the review
  steps. A "pre-merge gauntlet" ran in phases, and commits on 2026-07-07
  address findings from a review swarm and from a cross-review by a second
  model family (Codex), including a fix to freeze shared maps against caller
  aliasing and a regex denial-of-service fix in the completion-trailer parser.
- A CI gate (2026-07-09) requires each PR body to record a critical review
  pass before merge.
- The agents being coordinated were themselves the test subjects: the nightly
  canary and the live coordination integration test drive real agent turns
  over the bus.

## How it is tested

- Four packages, each gated in CI on `ruff check`, `ruff format --check`,
  `mypy --strict` and `pytest`. On 2026-10-27: `fleet-core` 181 tests,
  `fleet-nats` 577 (4 skipped), `fleet-orchestrator` 568 (4 skipped),
  `fleet-subscriber` 262, all passing.
- The JetStream tests run against a real `nats-server` when it is on PATH.
- A golden fixture of recorded bus frames
  (`packages/fleet-nats/tests/fixtures/golden-bus-frames.jsonl`) pins the wire
  format and drives the `fleet-show-work` output shown in the README.
- `specs/*.verify.sh` are executable checkers for the plans; on 2026-07-12
  several commits hardened them to fail closed instead of passing on scanner
  errors.
- Shell tests under `tests/` cover the hub release and agent install scripts.

## What did not work, and what changed

- **Over-claimed delivery guarantees.** Fixed by contract C5 and by correcting
  the docstrings, rather than by pretending the transport did more.
- **Relay loops.** Agents could relay a task back and forth rapidly; a fix on
  2026-07-10 stops them, and the governor caps messages per task.
- **Prose scraping.** Parsing completion markers out of free text was fragile
  (it needed a regex denial-of-service fix on 2026-07-07). Typed signals
  replaced it within a week.
- **Credentials in logs.** NATS URLs with user info reached log lines; four
  fixes on 2026-07-08 and 07-11 redact URL credentials, including URLs without
  a scheme.
- **Version drift on hosts.** Heartbeats first stamped a package version that
  did not change between deploys of the same version; a fix on 2026-07-28 ties
  it to the deployed commit so install drift is visible.

## Current state

Experimental and published as a reference implementation. The author ran it
for a three-agent fleet on a private network in 2026. The task board service,
the agent runtime and agent personas are not part of this repo.
