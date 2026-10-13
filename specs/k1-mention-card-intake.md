# K1: Human mention to tracked k-board card intake

Status: Phase A design only

Build gate: **BLOCKED until every C-contract issue #110, #111, #112, #113, and #114 is closed**

## Scope lock

This document defines the contract that turns a human order directed at an agent into a durable k-board card before that order may be acknowledged. It does not implement the contract.

Phase A may land this design without changing runtime behavior. Phase-B **coding** may begin only after the controller verifies that all five C-contracts are closed and reads their ratified artifacts, identifies the concrete gateway acknowledgment owner and k-board persistence owner, and records accepted companion-contract shapes with those owners in linked issues. Coding may define the fleet-bus side against those accepted shapes; it does not wait for the owner implementations to land.

Cross-boundary integration, enablement, and deployment have a later gate: every required owner-side change must be landed, its contract suite must pass, and the real gateway-to-fleet-bus-to-k-board boundary tests must be green. This separation avoids making fleet-bus implementation depend on its own downstream integration result while still prohibiting premature enablement. This design PR neither authorizes cross-repository changes nor substitutes for owner acceptance. Phase B must adapt this design to the final contracts rather than treating this draft as permission to pre-empt them.

The owning boundary is:

1. a transport adapter recognizes a human order and emits a normalized `HumanOrder`;
2. fleet-bus intake creates or resolves exactly one durable card through the existing `KboardClient` create boundary;
3. the existing k-board poller publishes the card's `created` event;
4. the existing hub and offline ladder dispatch and chase the card;
5. the caller may send a human-visible acknowledgment only after intake returns a durable card binding.

No Discord SDK type, webhook implementation, agent-host configuration, or new task lifecycle belongs in this change.

## Evidence and confirmed current flow

The plan and issue record the live failure: a Discord message reported that an acknowledged order had sat unexecuted for 2h20m; message `1524852175547662586` asked for the protocol that turns acknowledgment into action. The order never entered the tracked-card lifecycle.

Current source confirms why:

- `KboardClient.create_task` already owns `POST /api/tasks`, tenant handling, status validation, logging, and conversion of the response to `KboardTask` (`packages/fleet-orchestrator/src/fleet_orchestrator/kboard.py:186-240`). It currently accepts only caller-supplied `task_id`, `title`, `body`, and `status`.
- Open issue #45 is stale as written: it predates the create verb added for closed issue #89. Phase B must reuse the shipped method rather than reopening or duplicating that solved HTTP boundary.
- `IntakePoller` is a **card-to-created-event** adapter, not a human-message reader. It lists k-board rows, selects scoped `todo` cards, and publishes the existing intake event (`packages/fleet-orchestrator/src/fleet_orchestrator/intake_poller.py:260-409`). Its durable restart guard is the card status; its short per-process suppression is only a race/noise guard (`intake_poller.py:19-36,248-258`).
- The hub subscribes to the k-board `created` wildcard and starts the poller inside the existing service (`packages/fleet-orchestrator/src/fleet_orchestrator/loop.py:215-264,303-338`).
- `OrchestratorHub.start_task` ignores an already-coordinated task id before opening another session (`loop.py:397-417`).
- `OfflineDetector` already owns the silence ladder: wake, one redispatch, then durable blocked (`packages/fleet-orchestrator/src/fleet_orchestrator/offline.py:1-20,94-190`).
- The reused path is not privacy-clean today: `IntakePoller` and `OrchestratorHub.start_task` log `text[:80]` previews (`intake_poller.py:374-380`; `loop.py:477-488`). Phase B must replace these with metadata-only logging before human prose enters the path.
- The fleet-bus repo contains no Discord message consumer. Discord remains the human display boundary; the producer that can see a Discord mention must normalize it before calling fleet-bus.

Therefore the minimum correct Phase-B change is a create-or-resolve seam ahead of the existing poller. The poller, hub intake subscription, dispatch, status watches, and offline ladder remain the lifecycle.

## Transport-neutral interface

The contract is expressed in domain values. Names are illustrative; Phase B should follow the ratified C-contract vocabulary.

```python
@dataclass(frozen=True)
class HumanOrder:
    transport: str                 # e.g. "discord"; not a Discord SDK object
    source_scope: str              # stable channel/room identity, not display text
    source_message_id: str         # transport-stable immutable message id
    target_agent: str              # canonical fleet agent id
    title: str
    body: str
    provenance: ProvenanceToken    # opaque issuer-minted evidence, not caller claims
    source_url: str | None = None  # display/audit pointer, not identity

@dataclass(frozen=True)
class IntakeReceipt:
    card_id: str
    idempotency_key: str
    disposition: Literal["created", "existing"]
    durable: Literal[True]

class HumanOrderIntake(Protocol):
    async def create_or_resolve(self, order: HumanOrder) -> IntakeReceipt: ...

AckCapability = NewType("AckCapability", str)

# The string is only an opaque, unguessable handle. AckIssuer retains the
# authoritative immutable binding in a private/server-side registry and
# atomically consumes it under the accepted replay policy. AckEmitter trusts no
# caller-readable or caller-writable fields on the handle. An equivalent
# implementation may use a MAC over the complete binding, provided the MAC key
# is secret and absent from the token and verification still atomically enforces
# expiry/replay state.

class AckEmitter(Protocol):
    async def capability_for(
        self, order: HumanOrder, receipt: IntakeReceipt
    ) -> AckCapability: ...
    async def emit_success(self, capability: AckCapability) -> None: ...
```

`ProvenanceToken` is not an ordinary caller-populated dataclass. It is either cryptographically authenticated evidence with an allowlisted issuer/audience/key id and replay protection, or an opaque handle minted over a mutually authenticated trusted channel by a controller-verified gateway issuer. Its authoritative immutable binding covers every security-relevant input: canonical transport, `source_scope`, `source_message_id`, tenant, reporter identity/classification, and the intended target or complete authorized-target set. fleet-bus verifies/resolves that binding at ingress, derives those values from it (or compares each normalized caller-facing value to it), and rejects every missing or mismatched value before persistence or capability minting. It never accepts tenant, reporter, source, transport, or target authority from parallel caller fields or human text. It then authorizes the bound principal for the bound tenant and intended target. Invalid issuer, signature/MAC, audience, expiry, nonce/replay state, unresolved opaque handle, unauthorized target, or any binding mismatch fails closed. Cross-tenant lookup, replay, persistence, capability minting, or acknowledgment fails closed.

Input validation is fail-closed. Blank source identity, reporter, target, or actionable text is rejected before any acknowledgment. The intake operation returns only after the k-board has confirmed the row or an existing row with the same idempotency identity has been read back and verified. Timeouts, non-2xx responses, malformed responses, authorization failures, or conflicting identity bindings are errors, not success-shaped fallbacks.

Transport adapters own mention parsing and human/bot classification. fleet-bus owns normalization validation, durable identity, card creation/resolution, and the receipt. This keeps the core reusable for another human transport without importing transport-specific types.

## Card schema: current versus required

| Concern | Current proven create/read shape | Required for human-order intake |
|---|---|---|
| Card identity | Caller supplies `task_id`; server echoes it | Stable `card_id` bound to the idempotency identity |
| Lifecycle | `status`, initially `todo` | `todo`; unchanged so the existing poller sees it |
| Poller scope | Title begins with configured `FLEET_INTAKE_LABEL` | Preserve the configured prefix; never bypass poller scoping |
| Work text | `title`, `body` | Sanitized human order without losing actionable body text |
| Tenant | Client supplies current tenant in query and body | Authenticated canonical tenant in query, body, identity, lookup, and authorization; no second HTTP implementation |
| CAS/read model | `sync_version`; raw server row retained | Same, plus enough durable metadata to verify identity on replay |
| Reporter | Not represented by the typed create API | `reporter_kind=human` and stable `reporter_id` |
| Source | Not represented by the typed create API | `source_transport`, `source_scope`, `source_message_id`, optional `source_url` |
| Target | Poller currently uses configured default workers | Durable canonical `target_agent` or a ratified mapping that the poller can consume |
| Idempotency | Unique caller-chosen `task_id` only | Durable unique `idempotency_key` with conflict/read-back semantics |
| Audit time | Server row may contain fields, but client does not require them | Server-owned creation timestamp; transport timestamp may be optional evidence only |

Phase B must not hide required metadata in prose if k-board exposes a structured metadata surface. If the ratified/create API cannot persist and query the source identity atomically, the build remains blocked and the controller must open/route the owning k-board contract change. Encoding JSON into `body` is not an acceptable substitute because workers consume it as the task prompt.

## Durable idempotency

The identity of one intake attempt is a canonical tuple:

```text
human-order:v1 / tenant_id / transport / source_scope / source_message_id / target_agent
```

Every tuple component is canonicalized before hashing: versioned UTF-8 encoding; Unicode NFC; transport and canonical agent identifiers lowercased; tenant, scope, and message identifiers preserved according to their owning contract; and explicit length-prefix separators to prevent concatenation ambiguity. Opaque identifiers are not trimmed or case-folded unless their owning contract requires it. The stored `idempotency_key` is a bounded SHA-256 representation of the canonical bytes. The unhashed source fields remain separately auditable. Display names, message text, timestamps, retries, and acknowledgment ids are not part of identity. All create and lookup operations are tenant-scoped, and a key found under another tenant is never disclosed as an existing receipt.

Required behavior:

1. The first call atomically creates one `todo` card and binds the key.
2. A retry with the same key and the same normalized identity returns `disposition="existing"` and the same card id.
3. The same key resolving to different source or target fields is a hard conflict and guard-triggering failure.
4. Concurrent callers cannot create two cards for one key. This requires a server-enforced unique constraint or an equivalent atomic create-if-absent operation; a process-local set is insufficient.
5. A transport redelivery after process or hub restart resolves the existing durable card.
6. Card creation success followed by response loss is recovered by lookup using the key; callers do not blindly create a new random id.
7. Poller re-publication is intentionally separate. Duplicate `created` delivery is already tolerated by hub task-id dedup, while the card row/status remains the cross-restart ledger.

Whether the card id itself is derived from the key or the server stores a separate unique key is an unresolved Phase-B choice. Correctness requires atomic uniqueness and read-back verification either way.

## Acknowledgment-without-card guard

An acknowledgment is any human-visible response that asserts receipt/ownership of an actionable `HumanOrder`. It is legal only through `AckEmitter.emit_success` with an opaque, issuer-bound `AckCapability` minted from a verified `IntakeReceipt` for the same authenticated tenant and source identity. A raw receipt, boolean, or card id is not an acknowledgment capability. Capability minting re-verifies the complete provenance binding (transport, source scope/message, tenant, reporter, and intended/authorized target), canonical idempotency key, card id, and durable marker. The capability is only an unguessable handle into an issuer-controlled authoritative immutable binding with atomic consumption, or a secret-key MAC token with equivalent complete-binding and replay guarantees; verification never trusts caller-writable fields. `emit_success` resolves/verifies that binding, atomically applies the accepted consumption policy, and derives the displayed/structured card id only from the verified binding. It accepts no separate card id that could disagree with it.

The adapter calls intake before emitting the acknowledgment:

```text
authenticate + authorize -> normalize order -> create_or_resolve -> mint AckCapability -> emit_success(capability)
```

The guard contract is:

- no receipt, non-durable receipt, mismatched tenant/idempotency/source binding, missing card id, or absent/invalid capability raises `AckWithoutCardViolation` (final name follows the ratified taxonomy);
- the violation carries safe coordination metadata: transport, source scope, source message id, target agent, and error code; it never logs raw order prose;
- the guard logs only safe identifiers and an error code, and returns/raises a typed failure that the transport adapter cannot mistake for a successful acknowledgment;
- if card creation fails, the adapter may send a truthful failure message (for example, “I could not track this order”), but it must not claim acknowledgment/ownership;
- an existing-card receipt is valid: a redelivered mention may acknowledge the already-bound card without creating another;
- guard evaluation is synchronous with the acknowledgment decision. A later reconciliation job cannot legitimize an earlier ack-without-card;
- there is no unguarded success-ack sink in the adapter-facing API. Truthful failure messaging uses a separate capability/path that cannot express ownership.

The guard protects the human-facing edge, while fleet-bus owns the predicate and typed violation so every transport applies identical semantics. Phase B must include entry, success, failure, fallback, and guard-trigger logs, with no silent catches. Logs and traces must never include raw order title/body, message excerpts, reusable auth material, or unredacted source URLs; existing raw-preview logging on any changed path must be removed rather than copied forward.

## Content, privacy, and deterministic card projection

Human text and URLs are untrusted input. Phase B must enforce bounded UTF-8 fields, reject control and bidi-spoofing characters under the ratified text policy, escape content at every rendering boundary, and never fetch or unfurl `source_url`. Only approved `https` transport permalinks may be retained; credentials, query secrets, fragments, and disallowed hosts are rejected or redacted before persistence. Reporter identifiers and URLs follow the retention/redaction policy selected by the owning contracts, and deletion requests must be enforceable without weakening the durable identity ledger.

Projection from a normalized order to a card is deterministic and testable: the title is exactly `<FLEET_INTAKE_LABEL> <normalized title>` with one separator and a configured maximum; overflow moves to the body without semantic loss. The body contains normalized actionable text only, not hidden JSON or auth/provenance. Structured target/provenance fields are derived from authenticated tenant/transport/target values, never from arbitrary human text. `actionable` is a transport-owned explicit classification value with a versioned rule id; missing, ambiguous, or conversational input is rejected, never guessed. Identical normalized input must produce byte-identical title, body, target, provenance, and identity fields.

## Reuse of the poller and chase wheel

The new ingress stops after a confirmed `todo` card exists. It does **not** publish dispatch/wake/blocked frames directly and does not maintain timers.

The existing path remains:

```text
durable scoped todo card
  -> IntakePoller publishes kboard created
  -> OrchestratorHub.start_task dispatches and installs watches
  -> worker status resets the offline detector
  -> silence causes wake -> redispatch -> blocked
```

This deliberately preserves the poller's fail-closed `[fleet]` scope, retry window, hub task-id dedup, governor behavior, status persistence, and offline escalation. A Phase-B proposal that adds a second mention queue, direct dispatch, a mention-specific retry timer, or a parallel blocked state violates this design.

## Cross-boundary ownership

| Boundary | Owner | Contract |
|---|---|---|
| Detect agent mention, exclude bot/self messages, classify actionable human order | Transport/gateway integration outside fleet-bus | Produce one validated `HumanOrder`; no SDK objects cross the boundary |
| Normalize and validate intake; derive durable identity; create-or-resolve card | fleet-bus #128 | Return verified `IntakeReceipt` or fail loud |
| Persist source metadata and enforce atomic uniqueness | k-board API/storage owner | Durable key lookup/create semantics; any required server change is explicit |
| Turn scoped `todo` card into `created` event | Existing fleet-bus `IntakePoller` | Reuse unchanged unless ratified schema requires a narrow read extension |
| Dispatch, watch, wake, redispatch, blocked | Existing fleet-bus hub/offline machinery | Reuse unchanged except narrowly required target metadata consumption |
| Emit human-visible acknowledgment | Controller-verified gateway owner; companion issue required | Use guarded `AckEmitter` with a matching durable receipt; expose card id where UX permits |
| Persist/query source identity atomically | Controller-verified k-board owner; companion issue required | Accept the tenant-scoped uniqueness/read-back contract before fleet-bus coding; land implementation and tests before integration/enablement/deployment |

Any Phase-B need to edit another repository or agent-host configuration is a stop and coordination point, not permission for this issue to cross repositories. Before Phase-B coding, the controller must verify the owner, create/link the companion issue, and record that owner's accepted contract shape. The owner implementation and its tests need not land before fleet-bus codes against that accepted shape, but they must land before cross-boundary integration, enablement, or deployment.

## Hard dependency gate

Phase B is prohibited until the controller verifies all of these issues are closed and their accepted contracts are reflected here:

- #110 C1 token-cost baseline
- #111 C2 envelope schema compatibility
- #112 C3 plane/QoS table
- #113 C4 owner-map contract
- #114 C5 ordering, retry, and idempotency semantics

At Phase-A authoring time, all five issues were open. In particular, #112 decides the lane/QoS declaration, #113 constrains ownership/visible acknowledgment, and #114 governs retry and idempotency claims. No code, wire subject, or persistence promise in this draft overrides those decisions.

After those five closures, Phase-B coding remains blocked until the controller records concrete gateway-ack and k-board-persistence owners plus linked companion issues whose contract shapes those owners have accepted. A repository lacking an accepted companion contract is a hard stop, not an invitation to infer ownership. Integration, enablement, and deployment remain separately blocked until the required owner-side implementations land and cross-boundary contract tests pass.

## Non-goals

- #129 / K3 / T8: creating one visible Discord working thread per task.
- #118 / T3: the persisted channel-slug to Discord-id map that gates #129.
- Owner-map generation and visible-ack presentation in any external repository unless separately accepted by its verified owner.
- Direct Discord SDK/webhook integration in fleet-bus.
- Replacing the k-board poller, hub intake subscription, dispatch path, governor, status watches, or offline ladder.
- A `default_channel` workaround, per-message stub threads, or thread routing.
- Deploying or changing hub/agent-host configuration.
- Multi-agent fan-out policy. The one-message/multiple-mentioned-agent rule remains an explicit decision below.

## Functional I/O matrix

| Case | Input | Durable/card output | Human-edge output | Required proof |
|---|---|---|---|---|
| New valid order | Human, fleet scope, one target, stable message id, actionable text | One scoped `todo` card; receipt `created` | Ack may include card id | Real-both-ends test: normalized input -> create request/response -> poller-created event -> hub tracked task |
| Duplicate delivery | Same canonical identity | Same card; receipt `existing` | Ack may be replayed/updated, never a new ownership claim | Restart and concurrent-delivery tests prove one card id |
| Conflicting replay | Same key, different source/target binding | No mutation | No success ack; typed conflict | Conflict test and guard log assertion |
| Card API unavailable/timeout | Valid order, persistence fails | No confirmed card | Truthful failure only; no ack | Failure injection proves fail-loud and zero acknowledgment calls |
| Response lost after create | Create may have committed, client times out | Lookup resolves same card by key | Ack only after verified read-back | Scripted timeout/read-back test |
| Malformed/untrusted event | Missing identity, target, or actionable text | No card | No ack | Boundary validation table tests |
| Bot/self message | Non-human reporter classification | No card | No intake ack | Adapter test; classification remains transport-owned |
| Existing card reaches poller | Scoped `todo` row | Existing `created` event path | Not applicable | Existing poller tests remain green |
| Worker stays silent | Tracked card dispatched, no worker signal | Status progresses to `blocked` through existing persistence | Existing narration behavior only | Existing offline ladder functional test, no mention-specific timer |
| Ack attempted without receipt | Any ack call lacking matching durable receipt | No change | `AckWithoutCardViolation` | Guard test asserts ack sink was not called and safe log fired |
| Forged provenance | Caller-built, altered, wrong-audience, expired, replayed, unknown-issuer, or unresolved token/handle; any transport/source/tenant/reporter/target mismatch | No card | No ack; typed authentication failure | Boundary tests prove every security-relevant value is derived from or exactly matches the authoritative binding and every forgery fails before persistence/capability minting |
| Forged/mutated ack token | Caller-built object/handle, `object.__new__` instance, copied or mutated value, guessed nonce/handle, altered binding/card id, or replay contrary to policy | No change | `AckWithoutCardViolation`; no success sink call | Guard tests prove authoritative registry/MAC verification, atomic consumption, capability-derived card id, and replay semantics |

## Phase-B validation plan

Phase B is not complete with unit tests alone. The implementation PR must include functional tests at every changed seam and preserve existing package gates.

1. Contract tests for normalization validation and canonical idempotency identity.
2. `httpx.MockTransport` tests for create, duplicate/read-back, conflict, timeout-after-commit recovery, malformed response, and safe logging.
3. Concurrency test proving simultaneous identical intake attempts produce one durable card (against the owning persistence boundary, not a fake process-local lock).
4. Provenance tests proving arbitrary caller objects, copied/altered claims, invalid cryptographic evidence, unknown issuers, wrong audience, expiry, replay, and unresolved opaque handles cannot reach persistence or capability minting. The matrix must independently mismatch transport, source scope, source message id, tenant, reporter, intended target, and authorized-target set, and prove each is rejected before either boundary.
5. Guard/capability tests proving no success-ack sink is reachable without a matching authenticated, tenant-scoped durable receipt. `object.__new__` fabrication, ordinary construction/subclassing where applicable, copied or mutated objects/values, nonce or handle guessing, issuer/binding/card substitution, and concurrent or sequential disallowed replay must fail. Tests must prove authoritative immutable registry binding plus atomic consumption (or equivalent secret-MAC verification), show that verification trusts no caller-writable field, confirm `emit_success` has no card-id parameter and uses the verified capability-bound card id, and accept an existing-card receipt under the ratified replay policy.
6. Poller integration test proving the created human-order card satisfies scope and produces the existing `kboard_event(created)` payload with actionable text and target/channel data resolved per the ratified contracts.
7. Hub functional test proving that event enters `start_task`, is deduplicated by card id, and installs the existing offline watch.
8. Offline functional test proving silence follows wake -> one redispatch -> blocked with no mention-specific branch.
9. Restart/redelivery test proving the same source message resolves the same card after intake and hub process restart.
10. Package gates: Ruff check and format, strict mypy, and pytest for every touched package; `scripts/check-docs.sh`; repository verification scripts where applicable.
11. Conditional cross-owner validation: before integration or enablement, required linked owner changes, their landed contract suites, and an integration test across the real boundary are mandatory evidence. If no owner-side change is required, the controller records the accepted existing API/version and evidence that it satisfies authentication, tenant-scoped atomic persistence, lookup, and guarded acknowledgment requirements.
12. Live proof after merge and operator deployment GO: a test human mention in the approved fleet bench creates one visible k-board card, the card id is present in the intake/ack receipt, the existing chase path observes it, and an injected ack-without-receipt attempt raises the guard violation. Never use `dash-dev` for the probe.

Reviews occur only at the PR boundary. The implementation PR must follow the full pre-merge gauntlet required by the goal. This Phase-A artifact does not authorize a review swarm before a PR exists.

## Unresolved decisions for Phase B

These must be resolved from the closed C-contracts and owning APIs before coding:

1. Which k-board structured metadata field(s) store reporter, source, target, and idempotency key, and whether a server migration/API change is required.
2. Whether the server supports an atomic idempotency-key upsert/read endpoint or whether deterministic `task_id` plus conflict read-back is the ratified mechanism.
3. The exact subject/plane and delivery guarantee for transport-neutral `HumanOrder`, if it crosses a bus at all. Direct in-process invocation is preferable unless #112 requires a lane.
4. The canonical error/guard taxonomy and whether `AckWithoutCardViolation` is an exception, result variant, or both across the transport boundary.
5. How `target_agent` is persisted and consumed without falling back to the poller's current all-default-workers behavior.
6. Whether one message mentioning multiple agents is rejected, creates one manager-owned card, or creates one card per target. It must never silently fan out duplicate cards.
7. What text constitutes an actionable order versus a conversational mention. Classification remains transport-owned but needs a deterministic, testable contract.
8. Retention/redaction policy for reporter id and source URL. Raw message prose must not enter logs.
9. Whether a success acknowledgment must always display the card id or may carry a stable link while retaining the card id in structured metadata.
10. Which repository and accountable owner own each required gateway adapter, acknowledgment call-site, and k-board persistence change; the controller must create/link accepted companion contracts before Phase-B coding and verify their implementations land before integration/enablement.
11. The retention/redaction rules and approved permalink hosts for reporter/source data, as ratified by the owning privacy and gateway contracts.

Until these are resolved, #110-#114 are closed, and both concrete owners and accepted companion-contract shapes are linked, the correct next action is design review and dependency tracking—not Phase-B coding. Even after coding begins, integration/enablement must stop until required owner-side changes have landed and the cross-boundary tests pass.
