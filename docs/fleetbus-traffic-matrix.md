# Fleet Bus Traffic Matrix

Status: **ratified contract** for the current subject catalog (fleet-bus #119).

This document classifies every current fleet-bus traffic lane into one of two
planes. The classification follows the consumer, not the subject prefix or the
envelope's prose content:

- **Token-plane** traffic is delivered to an agent gateway and creates an LLM
  turn. Every delivery costs context tokens per subscriber, so the message must
  be correct, direct, actionable, and necessary—or absent.
- **App-plane** traffic is consumed by deterministic code. It may contain agent
  output for recording, scoring, display, or state transitions, but consuming
  it does not itself create another LLM turn.

"NATS control plane" in the architecture documentation describes NATS's role in
coordination. It is not a third traffic plane and does not override this table.

## Ratified classification

| Logical lane | Current subject template(s) | Plane | Why |
| --- | --- | --- | --- |
| dispatch | `{env}.agent.dispatch.{worker}`; `{env}.agent.{channel}.dispatch.{worker}` | token-plane | The worker subscriber turns the task instruction into an agent turn. |
| relay | `{env}.agent.relay.{to}`; `{env}.agent.{channel}.relay.{to}` | token-plane | The target subscriber turns relayed context into an agent turn. Channel-scoped since #117 — a relay carries context missed in ONE channel's deliberation. |
| wake | `{env}.agent.wake.{worker}`; `{env}.agent.{channel}.wake.{worker}` | token-plane | The target subscriber handles the wake as an agent turn. Keep wakes minimal. Channel-scoped since #117. |
| inbox | `{env}.agent.inbox.{agent}` | token-plane | The addressed subscriber creates an agent turn. This includes orchestrator narration and manager synthesis dispatches. Deliberately NOT channel-scoped: it addresses an agent, not a channel's deliberation. |
| direct (reserved/unwired) | `{env}.direct.{agent}` | token-plane | The intended contract is a direct local/human-to-agent turn without Discord overhead. No current runtime subscriber consumes this subject, so it must remain unwired until a token-plane consumer lands with the required safeguards. |
| status | `{env}.agent.status.{task_id}`; `{env}.agent.{channel}.status.{task_id}` | app-plane | Orchestrator/scorer code observes progress and agent output; receipt does not prompt another model. Human display bridges are sinks, not LLM subscribers. Channel-scoped since #117; the hub subscribes BOTH forms so a hub/subscriber version skew is not silent message loss. |
| done | `{env}.agent.status.{task_id}` (either form) with `kind="done"` | app-plane | The completion sentinel shares the status subject and drives a deterministic state transition. |
| ack | `{env}.agent.ack.{task_id}`; `{env}.agent.{channel}.ack.{task_id}` | app-plane | **Noise lane (#116).** A worker's bare acknowledgement that a dispatch landed. Consumed only by operators/miners reading the log; NO role subscribes it. The enforcement that zero noise reaches the parent Discord channel is the adapter's early return — `adapter.send` routes a non-actionable send to `_publish_noise` and returns BEFORE the tee. `voice_tee.is_forwardable` is a deliberate SECOND barrier that reads the lane out of the subject; on today's call sites it can never fire, because the only subject reaching the tee is a `status` one. It exists so a future caller wiring a bridge onto a wildcard subscription cannot leak noise by forgetting the plane rule. Needed but non-actionable, so it must cost zero LLM turns while staying fully recorded. |
| telemetry | `{env}.agent.telemetry.{task_id}`; `{env}.agent.{channel}.telemetry.{task_id}` | app-plane | **Noise lane (#116).** Tool echo and progress markers (the hourglass / floppy-disk classes). Same contract and same second-barrier bridge refusal as `ack`; split from it purely by volume — an ack is one message per dispatch, telemetry is unbounded per turn, so a delivery-confirmation miner must not have to filter a firehose. Classification is by leading progress marker FIRST and length second (`adapter._noise_lane`): the markers named here are the shortest sends on the wire, so a length-only rule would file the highest-volume traffic onto `ack`. |
| ob.context_pack | `{env}.ob.memory.context_pack` | app-plane | Open Brain's context-pack request/reply lane (#221). The consumer is OB's deterministic worker answering from Postgres; receipt creates no LLM turn. **Provisioned, not yet live** — the worker exists in OB's code but every deployed OB worker pins `OPENBRAIN_NATS_ENABLE_BRIDGE=false`, so no traffic rides this lane today (`docs/consumers.md`). The `ob` domain is OB-owned and server-granted (`{env}.ob.>`). A single fixed subject, not channel-scoped: it is a service endpoint, not per-channel coordination. |
| heartbeat | `{env}.agent.heartbeat.{agent}` | app-plane | Liveness code consumes identity, sequence, role, lanes, and timestamps. |
| token_cost | `{env}.agent.token_cost.{agent}` | app-plane | Governor/accounting code consumes measured token usage. |
| kboard.event | `{env}.kboard.task.{event}.{task_id}` | app-plane | Intake and persistence code consume durable task lifecycle events. |
| control | `{env}.hub.control.{task_id}` | app-plane | The hub run-loop validates and applies operator control operations in code. |
| arbitrate | `{env}.agent.arbitrate.{topic}` | app-plane | The orchestrator's request/reply arbitration handler chooses deterministically; manager synthesis, when required, crosses separately through `inbox`. |
| claim | `{env}.agent.claim.{task_id}` | app-plane | The request/reply claim contract is machine coordination and must not create an LLM turn. |
| dead-letter | `{env}.agent.dead-letter` | app-plane | Operators and diagnostics consume sanitized failure records; they are never agent prompts. |
| factory.runner | `{env}.factory.runner.{job_id}` | app-plane | The software-factory watcher consumes run progress as deterministic board/UI state; receipt must not prompt a model. **This is the only lane whose publisher is untrusted** — a runner box runs a coding agent with `bypassPermissions` over untrusted repository content — so the app-plane classification is a hard constraint on consumers, not a description of current traffic. See the plane-crossing note below. |

The per-channel wildcards (`{env}.agent.*.{lane}.{token}` for dispatch, status,
relay, wake, ack, and telemetry), heartbeat and token-cost wildcards, hub-control
wildcard, and generic layer wildcard are subscription selectors—not additional
publishable lanes. They inherit the plane of each concrete subject they match. A
wildcard subscriber must not erase plane boundaries by feeding matched app-plane
traffic into an LLM.

The `human` subject layer mentioned in the subject-tree architecture has no
current builder or catalog entry, so it is reserved rather than a current lane.

### The noise lanes — why `ack`/`telemetry` are separate subjects (#116)

Acks, tool echo, and the hourglass/floppy-disk progress markers are *needed but
noisy*: an operator wants them, an LLM must never pay for them.

Before #116 the subscriber **discarded** these sends outright
(`adapter.send` returned early without publishing), so the noise cost zero turns
but was also unrecoverable — "fully logged and mineable" was simply false. They
now publish on their own subjects instead.

The load-bearing property is that the split is **structural**, not textual. A
Discord bridge decides what to forward by reading the LANE out of the subject
(`voice_tee.lane_of` / `is_forwardable`), so it never has to guess from prose
whether a message is an ack or a real reply — a heuristic that would eventually
misclassify a genuine agent reply as noise, or worse, leak noise as a reply.

Consequences that reviewers must preserve:

- **No role subscribes them.** They are absent from `CODE_ROLE_LANES` on purpose;
  adding them would make every agent attach to its own noise, re-creating the
  per-turn cost the lanes exist to remove.
- **Zero Discord delivery.** `is_forwardable` refuses both lanes in both subject
  forms, and the refusal happens before any adapter call is made.
- **Recording is fail-open.** A publish failure on a noise lane is logged and
  swallowed; it must never turn a healthy turn into a failed send.
- **The coordination contract is unchanged.** A non-actionable send still reaches
  neither the `status` lane nor the done sentinel.

### `factory.runner` — plane constraint and the heron grant (#222)

This lane is answered here rather than left to the checklist because it is the
only lane whose **publisher is untrusted by design**.

- **Concrete consumer:** a software-factory dashboard,
  which renders live run progress. Receipt is
  deterministic board/UI state and **must not create an LLM turn**.
- **No in-repo subscriber today.** `factory_runner_wildcard` has no non-test
  call site, so nothing currently consumes this lane. The constraint below binds
  the first consumer that does.
- **NO LLM-adjacent consumer may subscribe `factory.runner.*`.** Feeding this
  lane into a model is a prompt-injection path from untrusted repository content
  straight into a fleet agent's context. Relabelling it telemetry does not make
  it safe (see the plane-boundary rule above).
- **Why heron holds a subscribe grant anyway.** `deploy/nats/fleet-prod-nats-server.conf`
  §4b grants the manager `{env}.factory.runner.*` **subscribe-only** so the
  hub can observe factory progress, and the grant is deliberately absent from
  its publish list (pinned by `test_authz_conf.py::test_manager_grants_exact`)
  so a manager can never forge runner telemetry. Heron is also the principal
  that runs `_forge_turn`, so this grant is the one seam where an app-plane
  lane meets an LLM-hosting principal. It is safe **only** while no code routes
  a `factory.runner.*` envelope into a turn. Any change that subscribes this
  lane in the orchestrator must state, in the PR, which deterministic handler
  consumes it — or drop the grant.
- **C6 bounds the blast radius but does not remove the constraint.** The body is
  opaque to the bus and cannot drive coordination, so a hostile payload cannot
  complete a task, sign a stance, or route a message. The residual risk is
  purely "bytes reach a model's context", which is exactly what the rule above
  forbids.
Its first concrete lane must follow the birth checklist below.

## The crossing seam

The orchestrator is deterministic code. It may read app-plane status, cost,
liveness, k-board, control, and arbitration traffic without spending model
context. Exactly one transition crosses from an app-plane decision into model
work: code publishes a deliberately constructed envelope onto a token-plane
lane handled by the subscriber gateway.

Today the clearest carve-out is arbitration synthesis: scoring code decides
that synthesis is required, then publishes one templated `dispatch` envelope to
the manager's token-plane `inbox`. Narration uses the same inbox boundary. The
model never makes the gate decision, and app-plane payload prose must not be
copied across implicitly. A new crossing requires an explicit formatter or
promotion gate that selects the minimum actionable content, plus tests on both
sides of the seam.

Traffic does not change planes merely because it is mirrored to logs, storage,
metrics, or Discord. It changes planes only when a consumer uses it to create an
LLM turn. Conversely, relabeling a token-plane subject as telemetry does not
make it app-plane if a gateway still prompts a model from it.

## New-lane birth PR-review gate

Every new publishable lane must declare its plane in the same change that adds
its subject builder and catalog entry. This checklist is a mandatory
PR-review/ratification gate owned by this T4 contract; it is not currently
machine-enforced. C3 (#112) owns codifying the plane/QoS table and its drift
checks. Until that machinery lands, reviewers must reject a new lane unless all
boxes are answered:

- [ ] Name the logical lane and every publish and subscription subject form.
- [ ] Declare exactly one plane: `app-plane` or `token-plane`.
- [ ] Name the concrete consumer and state whether receipt creates an LLM turn.
- [ ] For token-plane lanes, justify why a new turn is necessary; define the
      minimum actionable payload, subscriber fan-out, deduplication, and a
      negative test proving noise/non-actionable traffic creates zero turns.
- [ ] For app-plane lanes, test that every consumer remains deterministic and
      cannot route the envelope into an LLM gateway implicitly.
- [ ] If traffic crosses planes, name the single owning formatter/promotion
      gate, its rejection rules, provenance, and tests for both accepted and
      rejected inputs.
- [ ] Update this matrix, `fleet_nats.subjects`, `registry.CODE_SUBJECTS`, the
      relevant role/subscriber mapping, and their drift tests together.
- [ ] Confirm wildcard subscriptions preserve the declared plane and cannot
      broaden delivery across the seam.

## Source reconciliation

This table was reconciled against `fleet_nats.subjects`,
`registry.CODE_SUBJECTS`, subscriber role mappings, and orchestrator publishers.
The issue summary's lane list omitted two current subject definitions:
`agent.claim` and reserved/unwired `direct`. They are classified above rather
than silently left outside the contract. The latter has no current runtime
subscriber, so its token-plane assignment is the required contract for future
wiring rather than a claim about live delivery. No source contradicts the
ratified plane assignments;
`agent.status` carrying agent-authored text is intentionally app-plane because
its receiving path is deterministic code/display, not a new model turn.
