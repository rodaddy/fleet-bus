# C1 — Modeled coordination-span index (pre-de-noise)

**Status:** Ratified & codified (remediated). This document codifies the C1
decision Rico ratified on 2026-07-10 (issue #110), recorded at
the issue #110 decision comment:

> publish the token-cost baseline — N-subscribers × M-messages × context-growth
> for one realistic day, derived from the existing `token_cost` lane + Langfuse
> — as **the** pre-de-noise number every reduction claim (u1/#96, u5/#100,
> topic-split T1) must cite.

It was **reopened by the controller** at
the issue #110 decision comment
after the merged v1 was found to present a model output as an observed baseline.
This revision executes **option (b)** from that comment: rename the artifact as a
**modeled coordination-span index**, label every constant MEASURED or
ASSUMPTION, and define a **workload-controlled** comparison. Part of the
Contracts epic (#109).

> **What this document is — and is not.**
> It is a **modeled index**: a reproducible function of the *measured* span
> structure and a set of *assumed* per-message token constants. It is **not**
> observed token spend. No real `token_cost` per-turn count feeds any number
> here — the `token_cost` lane is consumed in-process and never persisted (see
> Methodology → "Why the token counts are modeled"). Every headline is the
> output of the model in [§ Formula](#formula), never a measurement of billed
> tokens.

---

## Headline: the coordination-span index

> **CSI(2026-07-09) ≈ 1,863,300 index-tokens** at the assumed constants
> (`N_read=2, BASE=2000, GROWTH=350, COMPLETION=250`) —
> **a modeled index, not observed spend**, for the fleet coordination layer on
> one realistic day (**2026-07-09**, the overnight Wave-1 + live-fire window).

"Index-tokens" is a **unit of the model**, deliberately named apart from
"tokens billed" so nothing downstream can quote 1,863,300 as measured spend.

**Evidence class: MIXED (measured structure × assumed price).**
- The **structure** — span/relay/dispatch counts, per-task fan-out, the
  relay-storm shape — is **MEASURED** from live Langfuse traces (project MAE,
  93 coordination spans across 5 tasks on 2026-07-09).
- Every **token-bearing constant** (`N_read`, `BASE`, `GROWTH`, `COMPLETION`)
  is an **ASSUMPTION**. None is calibrated from a measured token count; the
  `token_cost` lane contributes **zero** numeric input (Methodology → "Why the
  token counts are modeled"). The index therefore inherits a **±50% band** — see
  [§ Sensitivity](#sensitivity-how-much-the-assumptions-move-the-index).

**~89% of the index (1,650,600 index-tok at baseline) is a single relay-storm
task** — the skippy↔bilby ping-pong of #96. This concentration is **rooted in
the measured structure** (58 of 72 relays on one task) but is **not** independent
of the assumed constants: because the index is convex in M, varying the
dominant `GROWTH` constant ±50% moves the headline share (arithmetic below, from
[§ Formula](#formula), N cancels):

| `GROWTH` | storm-task index-tok (N=1) | day total (N=1) | storm share |
|---|---|---|---|
| 175 (−50%) | 483,525 | 570,450 | **84.8%** |
| 350 (baseline) | 825,300 | 931,650 | **88.6% (≈89%)** |
| 525 (+50%) | 1,167,075 | 1,292,850 | **90.3%** |

The share stays high (84.8%–90.3%) across the plausible band, so the
relay-storm dominance is a **robust** claim — but it is a robust *range*, not a
constant-independent fact. It is the signal the de-noise work exists to kill.

---

## Methodology

### Formula

```
CSI(day) = Σ_tasks  Σ_{k=1..M_task}  ( BASE + GROWTH·(k−1) + COMPLETION ) · N_read
```

- **M** (messages) — per-task coordination span count (**MEASURED** per task).
- **N_read** (billable readers) — LLM turns forged per coordination message
  (**ASSUMPTION** — see [§ N_read](#n_read-what-the-topology-actually-proves)).
- **context-growth** — the `GROWTH·(k−1)` term: the k-th message on a task
  re-includes the accumulated transcript, so modeled cost grows with position.
  This is the term de-noise attacks.

> ⚠️ **The index is convex in M.** For a task,
> `Σ_{k=1..M}(BASE+GROWTH·(k−1)+COMPLETION)` grows **faster than linearly** in
> M (the `GROWTH·(k−1)` sum is `GROWTH·M(M−1)/2`). This is the reason the
> comparison rules in [§ How to re-measure](#how-to-re-measure-workload-controlled)
> **forbid** comparing raw index — or index-per-span — across different task
> **shapes**: convexity lets a pure redistribution fake a reduction. See the
> [phantom-reduction counterexample](#the-phantom-reduction-counterexample-mandatory-guard).

### Day chosen

**2026-07-09 UTC** — the overnight Wave-1 run + live-fire day named in the plan.
All 93 traced coordination spans fall in the **06:58–09:24 UTC** window; a
±1-day widen (07-08 → 07-10) returns the identical 93, so the day is
self-contained.

### Sources (priority order per the plan)

1. **Langfuse (live) — USED, structure only.** Project MAE, `env=default`, host
   `192.0.2.73:3000` (langfuse-host). The fleet-hub `TeeTraceSink` /
   `LangfuseTraceSink` (`packages/fleet-orchestrator/src/fleet_orchestrator/trace.py`)
   was live on 2026-07-09 and wrote one trace per `task_id` with one span per
   coordination action. Source of the structural inputs below. **Receipt
   status:** only the two top-line counts are backed by a committed reproducible
   receipt — `observations` `meta.totalItems = 93` and `traces` `meta.totalItems
   = 5` from the two calls in [§ Queries used](#queries-used). The finer
   breakdowns (by action, by target, per-task M) require grouping the full
   paginated `observations` payload by `name`/`metadata.target`/`traceId`; the
   exact aggregation command used at authoring time was **not preserved**, so
   those rows are labeled **MODELED-FROM-OBSERVED** (derived from the observed
   93-span payload, but without a committed re-runnable aggregation) rather than
   MEASURED. A re-measure MUST regenerate them via the pagination + group-by
   steps in [§ How to re-measure](#how-to-re-measure-workload-controlled) and
   commit the frozen aggregate. **Langfuse spans carry no token/usage fields**
   (`TraceRecord` is a closed dataclass with no token fields; all 93 observations
   return zero usage) — so Langfuse contributes structure, never price.
2. **`token_cost` lane — NOT USABLE for a past day.** Consumed in-process by the
   orchestrator `CostLedger`; **no disk, no Langfuse persistence** (verified,
   this session). Contributes no numeric input.
3. **Hermes agent-host logs — CHECKED, no usable per-turn usage.** On the agent
   host (nagatha, `192.0.2.77`) `~/.hermes/logs/agent.log` covers
   2026-06-23 → 2026-07-10 (1,653 lines on 2026-07-09) but carries **zero**
   `prompt_tokens`/`completion_tokens`/`total_tokens` records. The only
   token-shaped lines are `agent.conversation_compression` heuristics
   (`rough_tokens=~N … awaiting_real_usage=true`) — an *estimated* context size,
   explicitly not real usage, none on 2026-07-09. Gateway/error logs: none
   either. **No measured per-turn calibration data exists**, so a full option
   (a) real-data baseline is impossible for this window; option (b) stands with
   the constants labeled ASSUMPTION.

### Queries used

Langfuse public API, basic auth (creds from Vaultwarden item `Langfuse - MAE`;
**never** committed):

```bash
# ILLUSTRATIVE — not a runnable line. Set env vars from Vaultwarden via your own
# secret-fetch step; never commit values. Paginate with limit=100 & page loop.
curl -u "$LANGFUSE_PUBLIC_KEY:$LANGFUSE_SECRET_KEY" \
  "$LANGFUSE_HOST/api/public/observations?fromStartTime=2026-07-09T00:00:00Z&toStartTime=2026-07-10T00:00:00Z&limit=100"
# → meta.totalItems = 93
curl -u "$LANGFUSE_PUBLIC_KEY:$LANGFUSE_SECRET_KEY" \
  "$LANGFUSE_HOST/api/public/traces?fromTimestamp=2026-07-09T00:00:00Z&toTimestamp=2026-07-10T00:00:00Z&limit=100"
# → meta.totalItems = 5
```

### Why the token counts are modeled

The `token_cost` wire shape (`packages/fleet-nats/src/fleet_nats/tokencost.py`)
carries real per-turn `prompt_tokens` / `completion_tokens`, but on the consumer
side it flows only into the orchestrator's in-process `CostLedger` — **nothing
persists those counts to disk or to Langfuse**, and the emitter is the
the agent runtime post-turn hook, not a durable sink. The Langfuse
`TraceRecord` is a closed dataclass with **no token fields**; all 93 observations
return **zero** usage. The agent-host logs carry no per-turn usage either (see
Sources #3). So for a past day the *structure* is measurable but the per-message
*price* is not — it is **assumed** and labeled as such throughout.

---

## Inputs

### Structure (Langfuse, 2026-07-09)

Only the first two rows carry a committed receipt (`meta.totalItems`) and are
**MEASURED**. The remaining rows are **MODELED-FROM-OBSERVED**: derived by
grouping the observed 93-span payload, but the exact aggregation command was not
preserved (see Sources #1). A re-measure regenerates them via the group-by steps
in [§ How to re-measure](#how-to-re-measure-workload-controlled).

| Input | Value | Label | Source |
|---|---|---|---|
| Coordination spans (messages), M | **93** | MEASURED | `observations` meta.totalItems |
| Distinct tasks (traces) | **5** | MEASURED | `traces` meta.totalItems |
| By action | relay **72**, dispatch **15**, complete **4**, noop **2** | MODELED-FROM-OBSERVED | group observed payload by span `name` |
| By target | bilby **51**, skippy **31**, (unaddressed) **11** | MODELED-FROM-OBSERVED | group observed payload by `metadata.target` |
| Window | 06:58:29 – 09:24:08 UTC | MODELED-FROM-OBSERVED | span `startTime` min/max over observed payload |

Per-task message counts (**MODELED-FROM-OBSERVED** — grouped from the observed
payload by `traceId`, no committed aggregation command):

| task (traceId prefix) | M | breakdown |
|---|---|---|
| `3923dafc37` (relay storm) | **63** | dispatch 3, relay **58**, noop 2 |
| `7027950070` | 12 | dispatch 3, relay 8, complete 1 |
| `ccb0b30404` | 6 | dispatch 3, relay 2, complete 1 |
| `63b5d1fe7e` | 6 | dispatch 3, relay 2, complete 1 |
| `536d40996d` | 6 | dispatch 3, relay 2, complete 1 |

### ASSUMPTION constants (uncalibrated — no measured token input)

Every value below is an **ASSUMPTION**. There is **no measured source or
calibration date** for any of them; they are documented-reference guesses, not
observations. Sensitivity of the index to each is in
[§ Sensitivity](#sensitivity-how-much-the-assumptions-move-the-index).

| Constant | Value | Label | Basis (and why it is only an assumption) |
|---|---|---|---|
| `N_read` | **2** | **ASSUMPTION (scenario)** | "addressed reader + a second observer." The topology **does not prove 2** — most spans forge **1** billable turn (see below). Kept at 2 as an explicit *scenario ceiling*; the **N=1 topology-floor index is 931,650** index-tok. |
| `BASE` | **2,000 tok** | **ASSUMPTION** | system + task framing re-sent each turn. No measured turn confirms this figure; no calibration date. |
| `GROWTH` | **350 tok/msg** | **ASSUMPTION** | accumulated-transcript re-read per prior message. Uncalibrated; the index is **most sensitive to this constant** (±50% → ±39% index). |
| `COMPLETION` | **250 tok** | **ASSUMPTION** | typical coordination-turn output. Uncalibrated. |

**Weak sanity anchor (not a calibration):** the agent-host compression heuristic
logged `rough_tokens≈11,213` for a ~46-message accumulated context. That is an
*estimated context size for one agent's whole transcript*, `awaiting_real_usage`,
so it can only order-of-magnitude sanity-check that a multi-message task context
is in the ten-thousands of tokens — it does **not** calibrate BASE/GROWTH per
turn and is **not** treated as a measured input.

Cross-check constants (documented, `governor.py`): `FLEET_BUDGET_MSGS = 200`/task,
`FLEET_BUDGET_TOKENS = 150,000`/task. These are **caps**, not calibrations.

### N_read: what the topology actually proves

The reopen requires each span type mapped to **actual billable readers** from
the registry + subscriber adapter, or marked a scenario assumption. Mapping:

- **Who forges an LLM turn.** A billable turn is forged only in
  `fleet_subscriber.adapter._forge_turn`
  (`packages/fleet-subscriber/src/fleet_subscriber/adapter.py:454`), reached from
  `_on_envelope` (`:424`) for an envelope delivered to a lane **this agent
  subscribes**. Lanes come from `registry.CODE_ROLE_LANES`
  (`packages/fleet-nats/src/fleet_nats/registry.py:67`): **worker** →
  `dispatch/relay/wake/inbox`; **manager** → `inbox` only. The concrete
  subscriptions are built in `_LANE_BUILDERS` (`adapter.py:67`).
- **Per span type → billable readers:**

  | Span (action) | Subject lane | Who subscribes | Billable turns forged |
  |---|---|---|---|
  | `dispatch.{worker}` | `dispatch` | the **one** target worker | **1** |
  | `relay.{to}` | `relay` | the **one** target agent | **1** |
  | `wake.{worker}` | `wake` | the one target worker | **1** (often a no-op map) |
  | `inbox.{agent}` | `inbox` | the one target (worker or manager) | **1** |
  | `noop` | — | (unactionable → `envelope_to_message` returns None, `adapter.py:475`) | **0** |
  | narration to a non-manager | any | dropped at `adapter.py:459` | **0** |

  > **Span-priced, not turn-priced (deliberate over-count).** The index is a
  > *coordination-**span** index*: it prices **every measured span uniformly**,
  > so the 2 `noop` spans in the storm task's M=63 are each priced as a full
  > `BASE+GROWTH+COMPLETION` message even though the table above rates them at
  > **0** billable turns. This is an intentional, disclosed over-count
  > (`+95,100` index-tok on the storm task at N=2; 21 of 93 spans total across
  > dispatch/complete/noop that don't map cleanly to one full turn). It is kept
  > — not corrected — but its cancellation in a delta is **conditional, not
  > automatic**: the uniform-span over-count cancels **only if the action mix is
  > unchanged** within each compared stratum. Uniform span pricing is convex in
  > position and blind to action type, so a re-measure that shifts the mix (e.g.
  > fewer noops, or relays moved to earlier positions) changes the over-count
  > asymmetrically and the residual does **not** cancel. Therefore a re-measure
  > MUST report the **per-stratum action mix** (relay/dispatch/complete/noop
  > counts) alongside the delta, and a stratum whose action mix has shifted is
  > **not** directly comparable — treat mix shift as invalidating the direct
  > comparison, not as noise that averages out. The per-span reader table is the
  > *turn-accounting truth*; the index's uniform span pricing is a *comparison
  > convention* that is only valid under held-constant action mix.

- **The "manager as a second reader" premise is false for billing.** The manager
  subscribes only `inbox`; its ledger/observer accounting a message is the
  **in-process `CostLedger`**, **not** a gateway turn — no second LLM call. A
  message is read by a second *LLM* turn only if it is *also* delivered to
  another subscribed lane (e.g. a relay fan-out addressing two agents), which the
  measured 2026-07-09 structure shows is the *exception*, not the rule (targets
  are single-addressed: bilby 51, skippy 31).

**Conclusion:** the topology proves **N_read = 1** for the overwhelming majority
of the 93 spans. `N_read = 2` is therefore **NOT** a subscriber measurement; it
is a **scenario assumption** (a ceiling covering fan-out / a hypothetical
dashboard reader). Both are published as **bracketing scenarios**, not as a
proven floor and ceiling:

- **Reference scenario (N=1):** **931,650 index-tok/day** — the lower bracket,
  the most topology-defensible N. It is **not** a proven floor: the index still
  over-counts the **21 of 93 spans** (dispatch/complete/noop) that don't map to a
  full billable turn — noops in particular map to **zero** turns (see the
  span-priced over-count note above) — so a turn-accurate count would sit below
  it.
- **Illustrative upper scenario (N=2):** **1,863,300 index-tok/day** — the
  headline and upper bracket, explicitly a scenario covering fan-out / an extra
  reader, **not** a proven ceiling and not a topology fact.

Both brackets inherit the same uniform-span over-count; they bound N, not the
true turn cost. A reduction claim MUST state which N it used and hold it fixed
across baseline and re-measure.

---

## Arithmetic

Per task, index = `Σ_{k=1..M} (BASE + GROWTH·(k−1) + COMPLETION) · N_read`.

**Scenario N=2 (headline):**

| task | M | index-tok |
|---|---|---|
| `3923dafc37` | 63 | **1,650,600** |
| `7027950070` | 12 | 100,200 |
| `ccb0b30404` | 6 | 37,500 |
| `63b5d1fe7e` | 6 | 37,500 |
| `536d40996d` | 6 | 37,500 |
| **day total** | **93** | **1,863,300** |

**Reference scenario N=1:** day total **931,650** index-tok (exactly ½; N is
linear). Relay-storm task = 1,650,600 (N=2) / 825,300 (N=1) = **~89%** of the day
either way — **N-invariant** (N cancels), but **not** constant-invariant: the
share moves to 84.8%–90.3% under `GROWTH` ±50% (see
[§ Headline](#headline-the-coordination-span-index)).

---

## Sensitivity (how much the assumptions move the index)

Because every constant is an ASSUMPTION, the index carries a band. Moving each
constant ±50% from its assumed value (N=2 shape held):

| Constant | −50% | assumed | +50% | Index swing |
|---|---|---|---|---|
| `BASE` (1000 / 2000 / 3000) | 1,677,300 | 1,863,300 | 2,049,300 | **±10%** |
| `GROWTH` (175 / 350 / 525) | 1,140,900 | 1,863,300 | 2,585,700 | **±39%** |
| `COMPLETION` (125 / 250 / 375) | 1,840,050 | 1,863,300 | 1,886,550 | **±1%** |
| `N_read` (1 / 2 / 3) | 931,650 | 1,863,300 | 2,794,950 | **±50%** |

Takeaways: the index is **dominated by `GROWTH` and `N_read`**, both
uncalibrated. A plausible band spans roughly **0.9M–2.8M** index-tok. This is
why the number is an **index for comparison**, never a spend figure — and why
comparisons must hold every constant fixed.

---

## Validation (cross-checks against documented receipts)

- **Relay storm is real, not a modeling artifact.** #96 reports 116/172 relays
  were skippy↔bilby bounces; Langfuse independently shows one task with 58
  relays (of 72 total) targeting exactly bilby (51) and skippy (31). The measured
  structure corroborates the receipt; this holds at any N.
- **Budget-cap consistency (#96).** The storm task's modeled per-reader index
  (825,300 index-tok) is the model applied to the **63 spans actually observed**
  for that task — the modeled index of the *observed pre-termination structure*,
  nothing more. It is **not** an estimate of any unobserved trajectory. Its
  exceeding the 150,000 `FLEET_BUDGET_TOKENS` cap is consistent with a
  cap-terminated run.
- **u5/#100 observer receipts** (1,419 unsigned-turn lines, 1,332 duplicates)
  concern the status/observation lane — a **different stream** with
  **incompatible units** (coordination *spans* vs observation *lines*). See
  [§ Exclusions](#exclusions-what-this-index-does-not-count) and the u5 fold-in
  rule below. Never summed with the index.

### u5 fold-in scope (unchanged, restated)

- **In scope:** any u5 change that reduces the count/context-growth of the 93
  **coordination spans** re-measures through the formula and cites a delta
  against the index (at the same N).
- **Out of scope (report separately, own unit):** u5's observer-idempotency win
  on the status/observation lane — reported as the reduction in duplicate
  observation lines (target 1,419 → ≤ the 87 real unsigned turns), per the #100
  acceptance test. **Never** a term in this formula; the two units are never
  summed.

---

## Exclusions (what this index does NOT count)

The index prices **only** the 93 measured coordination spans of 2026-07-09 under
the assumed constants. Explicitly **excluded**:

1. **Agent work/tool turns** — reasoning, tool calls, code edits an agent runs
   inside a turn. The index counts coordination *messages*, not intra-turn work.
2. **Non-coordination traffic** — heartbeats, status/observation lines, dead
   letters, arbitrate/claim lanes; any subject outside the measured coordination
   span set.
3. **Readers beyond the assumed N** — if a real dashboard/observer ingests
   messages as additional LLM turns, that spend is not in the N=1 or N=2 index.
4. **Untraced activity** — anything not written to Langfuse as a coordination
   span on the day (the index is exactly as complete as the trace set).
5. **Real `token_cost` events outside the chosen span set** — the in-process
   ledger's counts for turns not represented in the 93 spans (and, being
   unpersisted, none of them regardless).
6. **The observation/status lane** (u5's #100 domain) — different units, reported
   separately, never summed.

---

## Citation contract (for u1/#96, u5/#100, T1)

Any reduction claim citing this index MUST pin **all** of:

1. **Doc revision** — the exact git SHA / revision of this file used for the
   baseline (not "C1").
2. **UTC window** — exact `from`/`to`, same **calendar duration** as the baseline
   day.
3. **Query output** — the **complete paginated** `observations` result (all
   `page=1..totalPages`) or a **frozen aggregate artifact** committed alongside
   the claim.
4. **Per-task M distribution + action mix** — the `traceId → M` table and the
   `name`/`metadata.target` breakdown for the compared window.
5. **Formula + constants** — the exact `BASE/GROWTH/COMPLETION/N_read` used
   (must equal the baseline's), stated inline.
6. **Raw-vs-controlled label** — every number tagged as **raw** (uncontrolled)
   or **controlled** (task-stratified / fixed-replay). A raw cross-shape delta is
   **not** a valid reduction claim (see guard below).
7. **Stable T1 identifier** — cite T1 by its issue/spec number, never the bare
   label `T1`.

---

## How to re-measure (workload-controlled)

A reduction is a claim about **the same workload costing less**, not about a
quieter or differently shaped day. Because the index is **convex in M**
(§ Formula), you may **NOT** compare raw index — or index-per-span — across
different task shapes.

### The phantom-reduction counterexample (mandatory guard)

Hold the **same** 93 spans, 5 tasks, and constants; merely **redistribute** the
per-task message counts from the measured `[63,12,6,6,6]` to `[20,20,20,20,13]`:

| Shape | total index (N=2) | index/span | Δ vs baseline |
|---|---|---|---|
| measured `[63,12,6,6,6]` | 1,863,300 | 20,035 | — |
| redistributed `[20,20,20,20,13]` | 1,005,100 | 10,808 | **−46%** |

**Same total spans, same tasks, zero real change — yet a 46% "reduction".** Note
the drop is **46% in BOTH raw total AND index-per-span**: normalizing by span
count does **NOT** rescue the comparison, because index/span is itself convex in
the shape. This is why the v1 "report tokens/span" rule was insufficient and is
**replaced** by the workload-controlled procedure below.

### Required method — one of these two

**A. Task-stratified paired comparison (default).**
Bin tasks by **M-strata** (e.g. `M∈[1–8]`, `[9–20]`, `[21+]`). Compare the index
**within the same stratum** between baseline and post-change (index-per-task
averaged inside a stratum, or per-matched-task where task identity is stable).
Report a per-stratum delta; a fleet delta only as a stratum-weighted roll-up
using the **baseline** stratum weights (so a change in task-size mix cannot move
the headline). **Never** compare a `[21+]` task against an `[1–8]` task. Report
the **per-stratum action mix** (relay/dispatch/complete/noop) for both baseline
and post-change: the uniform-span over-count cancels only under unchanged mix, so
a stratum whose action mix shifted is **not** directly comparable and its delta
is invalid (see the span-priced over-count note in § N_read).

**B. Fixed-replay / canonical workload (preferred when available).**
When F3/J3 staging lands, replay an **identical, canned** task set (same task
count, same per-task action inputs) through baseline vs post-change wiring and
compare the index directly — the shape is held constant by construction, so any
delta is a real per-turn/fan-out change. Until staging exists, use method A.

### Steps

1. **Pull Langfuse observations for the window; paginate.** `limit=100` per page;
   read `meta.totalPages` and loop `page=1..totalPages` or the count is silently
   truncated (a busy window exceeds 100).
2. **Group by `traceId`; count every span of every `name`** (relay + dispatch +
   complete + noop) — apples-to-apples with the baseline, which priced all 93.
3. **Stratify by M** (method A) or confirm identical inputs (method B).
4. **Apply the exact constants** `BASE/GROWTH/COMPLETION/N_read` — held fixed and
   equal to the baseline's; state which N. If a constant must change, re-state
   the baseline at the new constant side by side.
5. **Report the controlled delta**, labeled controlled, with the full citation
   contract above. A raw cross-shape total may appear only as clearly-labeled
   context, never as the claim.
6. **Optional upgrade (option (a) path):** when the `token_cost` lane gains a
   persistent sink, replace the ASSUMPTION constants with MEASURED per-turn
   `total_tokens` summed by task and re-publish as a real baseline. The
   comparison protocol above is unchanged; only the price term upgrades.

---

*Codifies the ratified C1 decision (issue #110),
remediated per the controller's reopen.
A modeled coordination-span index — not observed spend. Feeds workload-controlled
acceptance numbers for u1/#96, u5/#100, and topic-split T1.*
