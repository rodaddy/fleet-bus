# C4 — Owner-map contract (contract-net award shape)

**Status:** RATIFIED (paper contract). **Issue:** [#113](https://github.com/rodaddy/fleet-bus/issues/113).
**Epic:** Contracts (#109). **Implemented by:** [rodaddy/rtech-hermes#545](https://github.com/rodaddy/rtech-hermes/issues/545) (kernel + hub narration).

> **Ratified decision (Rico, 2026-07-10):** the owner-map is a **contract-net award shape** —
> the manager posts the owner-map FIRST; non-owners stay silent in-channel until they return a
> lane result; silence is measurable — implemented identically by KERNEL text and hub narration.
> Verbatim: *"Yes — contract-net shape (Recommended)"*, no amendments.
> Recorded at
> <https://github.com/rodaddy/fleet-bus/issues/113#issuecomment-4932337584>.

This document defines the **machine-readable shape** only. It does not implement kernel prompt
text or hub narration — rtech-hermes#545 owns implementation and MUST derive both from this schema.

## Motivating failure

On a fan-out order, all three agents re-stated the same rule instead of one owning the lane and
the others staying silent — channel noise with no owner (rtech-hermes#545 receipts: three
identical rule re-statements on a single order, 11 identical Acks in 90s, 20 tool-spew posts).
The owner-map turns "who does what" from an emergent race into a manager-declared award.

Example channel rendering from the plan of record:

```
@Nagatha: clones · @Skippy: NATS brief · @Bilby: deploy diag
```

---

## (a) SHAPE

The owner-map is a per-task award: `task_id → [ lane ]`, where each lane names **exactly one**
owner agent and a scope description. It is a **contract-net award** — the manager (the
auctioneer) has already decided the allocation; this artifact publishes the awards, it is not a
call for bids.

### Machine-readable schema (JSON)

```json
{
  "kind": "owner_map",
  "version": 1,
  "task_id": "string",
  "award_id": "string (unique per award generation for this task)",
  "channel": "string | null",
  "manager": "string (agent_id of the poster)",
  "awarded_at": "string (ISO-8601 timestamp)",
  "award_message_id": "string (id of the channel post that projects this award)",
  "lanes": [
    {
      "lane_id": "string (stable, unique within this task)",
      "owner": "string (agent_id — exactly one)",
      "scope": "string (human-readable lane/scope description)"
    }
  ]
}
```

Field semantics and invariants:

| Field | Type | Rule |
|---|---|---|
| `kind` | `"owner_map"` | Literal discriminator; distinguishes this envelope from other coordination kinds. |
| `version` | int | Schema version. Start at `1`; bump on breaking shape changes. |
| `task_id` | string | The task this award applies to. A task may have multiple awards over time (award generations, see **(c)**). |
| `award_id` | string | Unique id for this **award generation** of the task. A re-award mints a new `award_id`; measurement is per award generation (see **(e)**). |
| `channel` | string \| null | The Discord channel the award is routed to. `null` = the task's default channel (see **(d)**). Silence is measured only in this channel. |
| `manager` | string | `agent_id` of the poster. MUST be the task manager (see **(b)**). Not required to appear in any lane. |
| `awarded_at` | ISO-8601 | The award timestamp; the `t0` for this generation's silence window (see **(e)**). |
| `award_message_id` | string | Id of the channel post that projects this award. The award post is exempt from the silence metric by this identity (see **(e)**), not by timestamp. |
| `lanes` | array | One entry per lane. MUST be non-empty. |
| `lanes[].lane_id` | string | Stable id, unique within the task. Referenced by the returned lane result. |
| `lanes[].owner` | string | **Exactly one** `agent_id`. No lane has two owners. |
| `lanes[].scope` | string | Human-readable scope (e.g. `"NATS brief"`). Renders in the channel post. |

**One-owner-per-lane** is a hard invariant: within a single owner-map, every `lane_id` maps to
exactly one `owner`. A given agent MAY own more than one lane; a lane MUST NOT be co-owned.

### Channel rendering

The visible channel post is a deterministic render of the same object. The rendering is
**normative** so both implementers (kernel text and hub narration) emit a byte-identical string:

- Render one segment per lane as `@<owner>: <scope>`, in **`lanes[]` array order** (no sorting,
  no dedup, no reordering).
- Join segments with ` · ` (space, U+00B7 middle dot, space). No leading/trailing separator.
- `@<owner>` is the literal `@` followed by the lane's `owner` `agent_id` verbatim. `<scope>` is
  the lane's `scope` verbatim.

```
@Nagatha: clones · @Skippy: NATS brief · @Bilby: deploy diag
```

The rendering is a **projection** of the schema, never an independent source of allocation. The
rendered segment sequence MUST equal `lanes[]` in order and cardinality.

---

## (b) WHO POSTS

- **Only the manager** posts the owner-map (single-narrator invariant — the manager is the one
  throat that surfaces coordination; matches the existing hub `Narrator`, which narrates only on
  the manager's identity, `packages/fleet-orchestrator/src/fleet_orchestrator/governor.py:369`).
- The owner-map MUST be the **FIRST coordination turn** for the task in-channel — the
  contract-net award opens the task. Nothing coordination-bearing precedes it for that `task_id`.

An owner-map posted by a non-manager, or a non-owner-map coordination post appearing in-channel
before the award for that task, is a protocol violation.

---

## (c) WHERE IT LIVES

The owner-map exists in two places, with a clear source of truth:

1. **Durable record — the k-board task (source of truth).** Per repo invariant *"k-board
   Postgres = handoff source of truth. NATS delivers; it is not the record"* (CLAUDE.md), the
   authoritative owner-map is persisted **on the k-board task** under the canonical field
   **`task.owner_map`** — the JSON object of shape **(a)**. `task.owner_map` holds the **current**
   (latest) award generation. A re-award mints a new `award_id` and a new `awarded_at`, replaces
   `task.owner_map`, and MUST append the superseded generation to **`task.owner_map_history`** (an
   append-only list of prior award objects, oldest→newest). Audits, the silence measurement, and
   replay MUST read `task.owner_map` (current) and `task.owner_map_history` (prior generations); no
   other field or store is authoritative. (rtech-hermes#545 owns the DB migration that adds the
   columns; this contract fixes the field names so two implementers cannot diverge on where they
   live.)

   **Award generations.** Each award — the initial one plus every re-award — is a distinct
   **generation** keyed by `award_id`. Generations do not overwrite each other's *identity*; only
   the "current" pointer moves. This is what makes measurement per-generation well-defined in
   **(e)** rather than lost on overwrite.
2. **Visible channel post.** The rendered `@owner: scope` line the manager posts in-channel so
   humans and agents see the award. This is a **projection** of the durable record, not a second
   source of truth.

The NATS envelope that carries the award between the manager and the hub/k-board is transport,
not the record — consistent with *"NATS delivers; it is not the record."* If the durable record
and the channel post disagree, the **k-board task wins**.

> **Judgment call (durable home).** The ratified decision and #113 acceptance require the schema
> to name *where it lives* but did not pin the exact storage. Placing the authoritative copy on
> the **k-board task** (not NATS, not Discord) is the only choice consistent with the repo's
> stated handoff-source-of-truth invariant. The channel post and the NATS envelope are
> explicitly demoted to projection/transport. rtech-hermes#545 implements the persistence; this
> contract fixes *which* store is authoritative.

---

## (d) NON-OWNER SILENCE

For a given task, an agent that is **not** listed as an `owner` in that task's owner-map MUST
post **nothing in-channel** for that task from the moment of the award until it has a lane result
to return. "Returning a lane result" is the only event that ends silence for a non-owner, and by
definition an agent only returns a result for a lane it owns — so a non-owner has no compliant
in-channel post for the task.

**Silence applies to the channel, not to work.** Non-owners may still do thread-only conduct
(acks, tool-echo, 💾/⏳ status) per rtech-hermes#545's thread-routing; those do not count against
silence because they are not in-channel. Silence is specifically about keeping the **channel**
carrying the owner-map plus substance, not ack/tool/ping spam.

> **Ratified human-address exception (Rico, 2026-07-10,
> <https://github.com/rodaddy/fleet-bus/issues/113#issuecomment-4932935474>).** A non-owner's in-channel post
> that is a **direct reply to a human address/@-mention** is **outside** the non-owner silence rule
> during the award window. When a human addresses an agent directly — by @-mentioning that agent, or
> in a message the agent's post replies to — that agent MAY answer in-channel without violating
> silence. The exception covers **only** the direct reply to the human; it does not license the agent
> to then re-state task rules, narrate its lane, or otherwise coordinate in-channel. **Task-attributed
> spam remains banned:** a post carrying the `task_id` that is not itself a direct human-address reply
> is still a violation. The exception is identified structurally (see **(e)**): the post either
> **reply-references a human-authored message** or the **immediately preceding message @-mentions the
> agent by a human**.

**Violation** = any post in the award's **routed channel** (see **(e)** for `null` resolution),
for that `task_id`, by an agent not in the current award generation's owner set, inside that
generation's window (**(e)**) — excluding the manager's own award post **and any direct reply to a
human address/@-mention** (the ratified human-address exception above). Because a non-owner never
returns a lane result, *every* in-channel post by a non-owner in that channel/window — other than a
direct human-address reply — is a violation. The manager's award post itself is never a violation
(excluded by message identity).
The canonical failure this closes: three agents re-stating the same rule when only one was awarded
the lane.

---

## (e) HOW SILENCE IS MEASURED

The measurement is a concrete, drillable count that gives u8's drill its assertion. It is defined
**per award generation** `g` (identified by `award_id`), then summed over all generations of the
task.

**Per-generation window.** For award generation `g` of task `T`:

- **Opens** strictly *after* `g`'s award post — the message identified by `g.award_message_id`.
  Ordering is by the channel's message sequence, not by wall-clock `ts` (timestamps may tie); the
  award post itself is excluded by **identity**, so the required manager award never counts as a
  violation even though the manager need not be an owner.
- **Closes** at the earliest of: (i) the next generation's award post (the re-award that
  supersedes `g`), or (ii) `end_of_task(T)` (below). The close bound is **exclusive of the
  boundary post** — the re-award post belongs to the next generation, and the terminal event
  message itself (see below) is not counted.

**Terminal event.** `end_of_task(T)` is the **k-board task status transition into a terminal
state** (the k-board task is the source of truth per **(c)**; terminal states are the k-board's
own done/failed/cancelled set). The transition message/record is the boundary and is **not**
counted. Non-owners never return a lane result, so their exposure runs to this terminal
transition.

**Censored / never-terminating tasks.** If `T` has no terminal transition at audit time, the
window for the current generation closes at **audit time (`now`)** and the task is reported
`open` (not `compliant` and not `violating` as final) — the count so far still stands, but the
verdict is provisional until the task terminates. Measurement never blocks or fails on a task that
has not ended.

**Channel scope.** Silence is measured **only in the award's routed channel**: a post counts only
if `m.channel == g.channel`. When `g.channel` is `null`, it resolves to the task's default channel
(the single channel the task is conducted in); posts in any *other* channel are out of scope for
this metric. A post carrying `task_id == T` in a different channel is not a silence violation here.

**Metric.** For award generation `g` and each agent `A` **not** in `owners(g)`, count in-channel
messages authored by `A` for `task_id == T` in `g`'s window and channel, **excluding any post that
is a direct reply to a human address/@-mention** (the ratified human-address exception in **(d)**):

**Human-address predicate.** `is_human_address_reply(m, A)` is true iff either (i) `m` carries a
**reply-reference to a human-authored message** (`m.reply_to` resolves to a message whose author is a
human, not an agent), or (ii) the **immediately preceding message in the channel** `@-mentions` `A`
and is authored by a human. Only the direct reply is exempt; a subsequent post by `A` that does not
independently satisfy this predicate is not covered.

```
window(g)      = ( after g.award_message_id , before close(g) )         # by channel message order
close(g)       = min( next_award_post(T, g) , terminal_transition(T) , now_if_open )
owners(g)      = { lane.owner : lane ∈ g.lanes }

violations(A, g) = | { m : m.channel_post
                          ∧ m.task_id == T
                          ∧ m.channel == resolve_channel(g.channel, T)
                          ∧ m.author == A
                          ∧ A ∉ owners(g)
                          ∧ m ∈ window(g)
                          ∧ m.id ≠ g.award_message_id
                          ∧ ¬ is_human_address_reply(m, A) } |

silence_compliant(T) = ( Σ over g ∈ generations(T)
                           Σ over A ∉ owners(g)  violations(A, g)  == 0 )
```

- **0 = compliant.** `T` is silence-compliant iff the summed non-owner in-channel count across
  **all** award generations is 0. Tasks still `open` report provisionally.
- Any positive count is the violation total and identifies the offending agents and generation.

This is the assertion u8's drill hangs on: replay the task, read `task.owner_map` +
`task.owner_map_history` from the k-board for each generation's `owners(g)`, count non-owner
in-channel posts per generation window (excluding the award post by id, excluding direct
human-address replies per `is_human_address_reply`, scoped to the routed channel), assert the sum
`== 0`.

---

## (f) IDENTICAL IMPLEMENTATION (invariant)

**KERNEL text and hub narration MUST both derive from THIS schema; neither restates the
allocation independently.**

- The manager's **kernel** produces the owner-map object (the JSON shape in **(a)**) as the
  contract-net award.
- The **hub narration** (`Narrator`, `render_narration`,
  `packages/fleet-orchestrator/src/fleet_orchestrator/governor.py:369` /
  `:424`) renders the channel post as a **projection** of that same object.
- There is exactly one allocation of owners→lanes per task, and it lives in the durable record
  (**(c)**). Both surfaces read it; neither invents it. A kernel that lists one owner set and a
  narration that renders a different one is a contract breach.

This is what "implemented identically by KERNEL text and hub narration" (the ratified decision)
means in machine terms: **one schema, two projections, zero independent restatements.**

---

## References

- Ratified decision comment:
  <https://github.com/rodaddy/fleet-bus/issues/113#issuecomment-4932337584>
- Ratified human-address exception (Rico, 2026-07-10): latest comment on
  <https://github.com/rodaddy/fleet-bus/issues/113#issuecomment-4932935474>. **Origin:** the KERNEL
  (rodaddy/rtech-hermes#565) **references** this exception and MUST cite C4 — it never restates it.
  C4 is the single source; #565 derives.
- Implementing issue (kernel + hub narration): rodaddy/rtech-hermes#545
- Contract issue: rodaddy/fleet-bus#113 · Epic: #109
- Related contracts: T5 (act/state enum, future), K4 (visible-ack handoffs)
- Repo invariants: `CLAUDE.md` — *k-board = handoff source of truth; NATS delivers, is not the
  record; Discord = human display.*
