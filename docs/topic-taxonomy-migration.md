# Topic-taxonomy migration and rollback design

- Status: **design complete; staging proof blocked by F3 (#105)**
- Owner: T6 / fleet-bus #121
- Ordering contract: C5 / fleet-bus #114

This runbook defines how a subject lane moves from the legacy taxonomy to the
two-plane taxonomy without making a flag-day change. It is an operational
design, not evidence that the migration works: F3 is still open, so none of the
commands or configuration fragments below have been applied to staging or
production.

## Safety contract

- Run the drill only after F1 (#103), F2 (#104), and F3 (#105) are closed and
  their effective state has been verified. Production cutover also requires the
  normal operator GO.
- Treat k-board, keyed by `task_id`, as the durable record. Core NATS remains
  delivery, not the record.
- Build application subjects with `fleet_nats.subjects`; keep the migration map
  as an infrastructure manifest. Do not hand-format subjects in consumers.
- Never infer success from a valid config file. Validate the config, reload the
  server, inspect the effective mapping, and prove message behavior.
- Preserve `Envelope.id` across the legacy and new copies. Every consumer that
  can see both taxonomies must apply the durable dedup and side-effect protocol
  below before causing a side effect.
- Do not combine application dual-publish and a server mapping that redirects
  the same source copy. That creates an unbounded duplicate path.

## Ordering promise

The fleet promises ordering only within one `(channel, task_id)` stream. It
does **not** promise ordering across channels, tasks, publishers, subjects,
reconnects, or the legacy/new copies during dual-publish. Core NATS remains
at-most-once; future JetStream lanes must separately declare at-least-once
delivery and an idempotency key under C5.

For an ordered lane, one publisher connection serializes a task's envelopes and
one selected taxonomy is authoritative for side effects. A dual-subscribed
consumer may observe both copies, but accepts the first `Envelope.id` once and
must not use arrival order between the two subjects as task order. Sequence
validation uses the lane's task transition/sequence field once C5 ratifies it;
until then, k-board state is the reconciliation authority and any ambiguous
ordering is a stop condition.

Weighted subject mappings choose a destination per message. They are therefore
**not sticky by `(channel, task_id)`** and must not split an ordered task or a
token-plane conversation. Percentage weights are allowed only for a lane whose
ratified matrix says it is order-insensitive and idempotent. Ordered and
token-plane lanes use a named canary cohort selected before publication (for
example, a dedicated canary ingress subject or an explicit channel/task allow
list), then map that cohort 100% to the new subject.

## Per-lane migration manifest

Before moving a lane, add one reviewed manifest row to the change ticket:

| Required field | Meaning |
|---|---|
| legacy subject pattern | Exact current source pattern |
| new subject pattern | Exact app-plane or token-plane destination |
| plane | `app` or `token`, matching the ratified traffic matrix |
| publisher and consumers | Every process that publishes or subscribes |
| ordering key | `channel + task_id`, or `none` with justification |
| delivery and idempotency | Core at-most-once, or JetStream policy plus key |
| durable dedup | Store/table, unique key scope, retention, and cleanup owner |
| side-effect atomicity | Transaction boundary or downstream idempotency mechanism |
| canary selector | Named cohort; never an implicit random cohort for ordered traffic |
| observable gate | Queries/counters and the accepted baseline or threshold |
| rollback owner | Operator authorized to restore the legacy route |

No wildcard map may be broader than one reviewed row. In particular, do not
map `*.agent.>` as a shortcut: app-plane and token-plane lanes have different
cost and correctness boundaries.

## Durable dedup and side-effect atomicity

First-seen process memory is not sufficient. Before dual-publish, every lane
must choose and prove one of these mechanisms:

1. **Transactional inbox.** In the same durable database transaction, insert a
   unique inbox key and commit the local side effect. The key is
   `(lane_id, Envelope.id)`, where `lane_id` is the exact manifest row; this
   prevents an id collision or intentional reuse on one lane from suppressing a
   different lane. A unique-key conflict means the side effect already won and
   the duplicate is acknowledged without running it again.
2. **Idempotent downstream.** When the side effect belongs to another durable
   system and cannot share the inbox transaction, call that system with
   `(lane_id, Envelope.id)` as its enforced idempotency key. Persist the durable
   inbox/result before reporting completion. A log-only key, check-then-act
   sequence, or best-effort cache does not qualify.

The manifest retention must be at least:

`dual-publish window + rollback window + maximum replay/redelivery horizon`.

If a lane has no bounded replay horizon, retain its keys for the life of the
affected durable record (for a task lane, through task archival). Cleanup must
be transactional/batched and must not remove keys while either taxonomy can
still deliver or be replayed.

Before opening dual-publish, the F3 staging drill must deliver the same key on
both legs, crash/restart the consumer at both failure seams (after durable key
claim but before completion, and after side-effect commit but before completion
is observed), then replay/redeliver the envelope. Acceptance is one durable
side effect, one committed dedup row/result, and duplicate counters for every
other delivery. If atomicity cannot be demonstrated, that lane cannot migrate.

## Core NATS quiescence barrier

Core NATS has no durable consumer position or broker acknowledgement. A client
`flush` proves the server processed that connection's buffered publishes; it
does not prove subscriber callbacks or side effects completed. Use this full
barrier before changing a lane's publishing authority or claiming it drained:

1. Close the lane's ingress gate so no new application publish can start. Give
   the gate a migration epoch and reject/queue late calls visibly.
2. For **every** publisher connection, wait until its application publish
   counter is zero, then call the client flush operation with a bounded timeout.
   A timeout is a failed barrier. For a transport with publisher
   acknowledgements, also wait through the last captured acknowledgement.
3. From each flushed publisher connection, publish a unique barrier envelope on
   every explicit leg it used, then flush again. Barrier envelopes have no
   business side effect and carry the migration epoch, publisher id, leg, and
   barrier id.
4. Each consumer records, per `(migration epoch, publisher, leg)`, callback
   `received`, durable `completed`, and `in_flight` counters. Wait until every
   expected barrier has been received, every pre-barrier callback is durably
   completed, `failed == 0`, `in_flight == 0`, and `received == completed`.
   Business counters exclude the no-side-effect barrier envelopes; barrier
   receipt is tracked separately. Aggregate counters across every active
   consumer instance, including queue-group members.
5. Sample the counters for two full, predeclared monitoring intervals. The
   checkpoint passes only if they remain unchanged and every durable side effect
   through each barrier is committed. Save the counter vector as evidence.

All publishers must participate; a barrier from one connection says nothing
about another connection's messages. After the checkpoint, keep ingress closed
until the new authority is effective, then reopen it under a new epoch.

Where a lane later uses a durable transport, the manifest may replace steps
3–5 with a captured durable position: stop publishers, flush/ack through the
last sequence, and prove every consumer acknowledgement floor reached that
sequence with zero pending/in-flight messages. Core NATS lanes must use the
barrier above.

## Server mapping shape (illustrative only)

NATS mappings are account-scoped and take effect after a configuration reload.
The checked-in deployment/Ansible source, introduced by the F3 owner, must hold
the real stanza. Validate the owning full config, not this fragment.

```conf
# Example token substitution only. Replace with a reviewed manifest row.
mappings: {
  "staging.migrate.canary.agent.*.dispatch.*": [
    { destination: "staging.agent.$1.dispatch.$2", weight: 100% }
  ]
}
```

For an order-insensitive, idempotent app-plane lane only, a reviewed step may
temporarily use weights:

```conf
mappings: {
  "staging.migrate.telemetry.*": [
    { destination: "staging.legacy.telemetry.$1", weight: 90% },
    { destination: "staging.agent.telemetry.$1", weight: 10% }
  ]
}
```

Weights distribute messages; they do not duplicate them. Dual-publish is an
application migration phase in which the publisher intentionally emits the
same envelope bytes (including `Envelope.id`) to both explicit subjects.

### Mandatory pre-reload disjointness gate

Before loading or reloading any mapping, attach machine-checked subject-match
evidence proving the mapping source is a dedicated ingress pattern disjoint from
**both** explicit dual-publish legs. The manifest must name all three patterns;
an exact wildcard-pattern intersection check must fail if any subject can match
both the mapping source and either legacy or new pattern. Representative
wildcard captures and traces of one ingress canary plus one publish on each
explicit leg supplement that exhaustive check by proving only ingress is
transformed. A config parse alone is not this proof.

If that disjointness cannot be proved, the mapping must not be reloaded while
dual-publish is open. Close dual-publish first using the Core NATS quiescence
barrier, switch publishers to the dedicated ingress source while ingress remains
closed, then validate/reload the mapping and reopen. This is mandatory even
when the intended mapping destination looks harmless.

## Observable gates

Capture a timestamped evidence bundle for every phase and lane. It must identify
the server/config revision, mapping revision, publisher and consumer revisions,
subject patterns, canary cohort, and task ids. Payload text and credentials do
not belong in the bundle.

The following are hard holds:

- zero unauthorized, cross-channel, or wrong-plane delivery;
- zero missing task transitions after reconciliation with k-board;
- zero duplicate side effects after deduplication by `Envelope.id`;
- zero ordering violations within the selected `(channel, task_id)` canary;
- token-plane fan-out and token cost do not exceed the lane's pre-cutover
  baseline/approved threshold;
- legacy and new publish/receive counts reconcile after accounting for the
  declared dual-publish and dedup paths;
- no slow-consumer, permission, mapping, decode, or dead-letter error attributable
  to the step;
- latency and error rate remain within the lane's predeclared threshold.

If a threshold has not been declared, the lane is not ready to move. A server
reload, process health check, or connected subscriber count alone is not proof.

## Staging sequence (F3-gated)

Each step stops for evidence review. Do not batch reloads or advance on a red
gate.

1. **Freeze the manifest.** Verify F1/F2/F3 closed, C5 compatibility recorded,
   all publishers/consumers inventoried, and the rollback owner present.
2. **Baseline old-only.** Run representative `(channel, task_id)` flows on the
   separate staging server and save the observable gates above.
3. **Deploy read compatibility.** New consumers subscribe to both explicit
   taxonomies and deduplicate `Envelope.id`; deliveries from the new subject run
   in shadow/no-side-effect mode, so the legacy subject remains the only
   side-effect authority. Prove the new subscription is idle and harmless.
4. **Prove durable dedup.** Run the duplicate/restart/redelivery drill for every
   lane and retain its dedup-store, retention, and atomicity evidence. No lane
   advances on an in-memory or check-then-act implementation.
5. **Open the dual-publish window.** One publisher emits byte-equivalent
   envelopes to legacy then new subjects, from the same serialized task loop.
   Consumers continue accepting one id once. Reconcile both copies and k-board.
6. **Pass the pre-reload gate.** Prove the mapping ingress is disjoint from both
   explicit dual-publish legs. If it is not, close dual-publish with the Core
   NATS quiescence barrier before changing publishers or reloading.
7. **Install the canary map.** Validate the full candidate server config with
   `nats-server -t -c <candidate-config>`. Reload through the F3-owned service
   procedure, inspect effective mapping/trace evidence, and route one named
   canary cohort 100% to the new taxonomy.
8. **Expand deliberately.** Add named channels/tasks in bounded cohorts. A
   percentage-weight step is optional and permitted only for a manifest row
   explicitly classified order-insensitive and idempotent. Never weight an
   ordered or token-plane stream.
9. **Make new authoritative.** Route all eligible ingress 100% to new subjects.
   Keep dual subscriptions and dedup active through the observation window.
10. **Close dual-publish.** Close ingress, execute the Core NATS quiescence
    barrier across every publisher and both explicit legs, switch publishers to
    new-only, then reopen ingress under a new epoch. Retain the tested rollback
    mapping, durable dedup, and legacy read compatibility until the rollback
    window expires.
11. **Staging rollback drill.** Execute the rollback below, prove old-only
   behavior, then repeat the forward cutover. This successful drill is the
   missing F3-gated half of #121 acceptance.

## Rollback after both taxonomies carry traffic

Rollback is a forward configuration change, not a deletion of the new
taxonomy. Keep consumers dual-compatible while restoring the single legacy
authority.

1. Close ingress, stop expansion, and record the last accepted
   `(channel, task_id)` plus the mapping and application revisions.
2. Execute the Core NATS quiescence barrier across every publisher and active
   leg. Do not change mapping or publisher authority until the stable counter
   checkpoint is saved.
3. Restore the pre-reviewed mapping that sends the dedicated ingress 100% to the
   legacy destination. Validate the full config, re-prove source/leg
   disjointness, reload through the owning service procedure, and confirm the
   effective mapping.
4. Switch applications to the dedicated ingress (or legacy-only when no
   disjoint ingress exists), flush the publisher connections, then reopen
   ingress under a new epoch.
5. Keep dual subscriptions and durable `(lane_id, Envelope.id)` dedup active.
   Any late new-subject message is completed without a second side effect.
6. Reconcile every in-flight task against k-board. Resume an ambiguous task from
   its durable state; do not guess from which subject arrived last.
7. Re-run all observable gates on legacy-only traffic. If any integrity gate is
   red, stop processing the affected lane and escalate to the rollback owner.
8. Preserve the new taxonomy, evidence, and failure reason for diagnosis. Remove
   compatibility only in a later reviewed change after the rollback window.

## Validation receipts still required

Design completion is proven by this reviewed document and repository doc gates.
Issue #121 is **not acceptance-complete** until the F3-owned separate staging
server exists and the following receipts are attached to the issue/PR:

- candidate and rollback full-config validation output;
- mapping-source/dual-leg disjointness proof for every reload;
- durable dedup atomicity plus crash/restart/redelivery proof per lane;
- Core NATS barrier counter vectors for authority changes and rollback;
- effective reload/mapping evidence from staging;
- old-only baseline, dual-publish, named-canary, new-only, rollback, and
  forward-again evidence bundles;
- k-board reconciliation for the canary task ids;
- explicit statement that production and live agent config were untouched.

## NATS references

- [Configuring subject mapping](https://docs.nats.io/running-a-nats-service/configuration/configuring_subject_mapping)
- [NATS server configuration and reload](https://docs.nats.io/running-a-nats-service/configuration)
- [Subject mapping and partitioning](https://docs.nats.io/nats-concepts/subject_mapping)
