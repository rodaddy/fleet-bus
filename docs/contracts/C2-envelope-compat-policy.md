# C2 — Envelope Compatibility Policy

**Contract:** C2 (issue [#111](https://github.com/rodaddy/fleet-bus/issues/111), epic
[#109](https://github.com/rodaddy/fleet-bus/issues/109))
**Status:** Ratified (Rico, 2026-07-10) —
[decision comment](https://github.com/rodaddy/fleet-bus/issues/111#issuecomment-4932337486)
**Scope:** `packages/fleet-nats/src/fleet_nats/envelope.py` — the `Envelope` wire contract
every consumer shares.

This is the contract that governs how the fleet-bus envelope evolves without breaking the
producers and consumers already on the wire. It is the only C-series contract that
materially changes `envelope.py` behavior. It blocks T5 ([#120](https://github.com/rodaddy/fleet-bus/issues/120)),
whose motivating change is an additive `act`/`state` enum field on the envelope.

---

## The three ratified rules

Per the ratified decision the envelope compatibility policy is:

1. **Unknown fields are preserved on round-trip.** A field on the wire that this build does
   not know about MUST survive `from_bytes` → `to_bytes` unchanged. Decode never silently
   drops data it did not recognise; a relay/forwarder never truncates a newer producer's
   message down to the fields this build happens to know.

2. **Additive-only changes within a major version.** Inside a single major version, the wire
   shape may only *grow* — new optional fields, new enum members. It may never rename,
   remove, retype, or change the meaning of an existing field. Any of those is a major bump.

3. **Explicit version gate that fails LOUD.** When a message declares a schema version this
   build cannot honour (`version > ENVELOPE_VERSION`), the envelope layer refuses or degrades
   — it NEVER accepts silently. The gate is unmissable: it is a hard, logged failure, not a
   best-effort log line the caller can ignore.

---

## What "additive-only within a major" means

`ENVELOPE_VERSION` is the major. Within one major, evolution is monotonic *growth* of the
wire shape. The typed dataclass fields are the "known set"; everything else on the wire is
carried opaquely (see round-trip guarantee).

**T5 is the motivating case.** T5 adds an `act`/`state` enum to the envelope. That is the
canonical *additive* change: a new optional field with new enum members, no existing field
touched.

### Do (additive — same major, no version bump)

- Add a new **optional** top-level field (defaulted; absence is valid). T5's `act`/`state`
  land here.
- Add a new **enum member** to an existing open enum (e.g. a new `kind`). Consumers that
  don't recognise it route it as unknown; they do not crash.
- Add a new key inside `payload` (payload is already free-form JSON per kind).

Old consumers ignore the new field (or carry it opaquely via the unknown-field mechanism);
new consumers read it. No wire break.

### Don't (breaking — requires a major bump of `ENVELOPE_VERSION`)

- **Remove or rename** an existing field (`task_id` → `taskId`, dropping `topic`).
- **Retype** an existing field (`version: int` → `version: str`; `to` from string to object).
- **Make an existing optional field required**, or tighten its accepted domain so previously
  valid messages now fail validation.
- **Change the meaning** of an existing field while keeping its name/type (semantic break —
  the worst kind, because it type-checks and silently misbehaves).
- **Change the wire encoding** (key casing, the `from`↔`sender` mapping, JSON→something else).

Any "don't" ships behind an incremented `ENVELOPE_VERSION`, at which point the version gate
(rule 3) protects older builds from the newer shape.

---

## Version-gate behavior (the concrete choice)

Two cases, both driven by the ratified rules:

### Same major, unknown fields present — PRESERVE and round-trip

`version <= ENVELOPE_VERSION` (additive-only guarantee holds): the message is accepted. Any
top-level key not in the known field set is captured into `Envelope.extra` and merged back on
`to_bytes`. Known fields win on collision (see round-trip guarantee). This is rule 1 + rule 2
in force: a new field this build hasn't learned yet rides through untouched.

### Newer major — FAIL LOUD by refusing with `ValueError`

`version > ENVELOPE_VERSION`: **`from_bytes` raises `ValueError`.**

This is the ratified rule-3 "refuse" branch, chosen over "degrade-with-warning" because it is
the cleanest genuinely-loud gate available in this codebase:

- `bus.py`'s `subscribe` decode guard (`packages/fleet-nats/src/fleet_nats/bus.py:250-255`)
  and `request` reply guard (`bus.py:318-322`) already catch `ValueError` from
  `Envelope.from_bytes`, log it at WARNING as a poison message, and drop it — the
  subscription survives. Raising `ValueError` therefore routes straight into the existing
  poison-message path: **loud (logged), safe (one bad message never kills the subscription),
  and non-silent (the message is refused, never half-accepted with dropped fields).**
- The previous behavior — a lone `logger.warning` while still constructing the envelope and
  dropping unknown fields — is exactly the "silent-ish degrade" rule 3 forbids: the fields
  were lost, the caller got a valid-looking `Envelope`, and only a log line (easily missed)
  recorded the loss. That is removed.

`from_bytes` logs the guard trigger at WARNING before raising, so the refusal is visible at
the envelope layer itself (`Envelope.from_bytes: refusing message version N > supported M`),
independent of whatever the caller's decode guard logs.

**The `version` field is a JSON integer ≥ 1 — no coercion.** `from_bytes` does NOT
`int()`-coerce the wire value: a float (`1.9`) would truncate to `1`, *slip* the `>` gate, and
corrupt the version on re-encode; a bool (`int(True) == 1`) would masquerade as the current
major; a zero/negative version is nonsensical. All three are refused with `ValueError` (routed
to the decode guard) before the `>` gate runs, so the fail-loud gate cannot be bypassed by a
non-integer version. This mirrors `registry.py`'s `isinstance(int)` + not-`bool` check.

**Why refuse and not degrade:** the ratified text permits "degrade-with-loud-warning" *only
if the warning is unmissable AND fields are still preserved.* A `logger.warning` inside a
library decode path is eminently missable, and the old code did not even preserve the fields.
Refusing with `ValueError` is unambiguously loud (it aborts the decode and hits the poison
guard) and cannot be mistaken for success. A newer *major* is, by definition (rule 2), a wire
shape this build may misread — refusing is the correct fail-closed posture. Forward-compat for
*additive* change is already delivered by the same-major preserve path; it does not require
accepting an unknown major.

### Supported-version range and rolling out a major bump

The supported version range is a **single major**: this build accepts exactly
`1 <= version <= ENVELOPE_VERSION` (currently `ENVELOPE_VERSION = 1`, i.e. only version 1).
There is no per-major decode branch and, deliberately, no multi-major decoder: within the one
supported major every change is additive (rule 2), so one decoder suffices, and anything above
the current major is refused by the fail-loud gate (rule 3). `version < 1` and non-integer
versions are rejected as malformed (above).

A **non-additive change** (any "Don't" above — rename/remove/retype/re-mean a field, or change
the encoding) requires **incrementing `ENVELOPE_VERSION`**, and that is a fleet-wide event, not
a single-service deploy. Because a bare bump makes every not-yet-upgraded consumer *refuse*
newer-major traffic (fail-loud, by design), a major bump MUST follow a rollout order — one of:

1. **Consumers-before-producers.** Deploy the new `ENVELOPE_VERSION` to every *consumer* first
   (so they accept the new major), then flip *producers* to emit it. No producer emits major
   N+1 until all consumers accept it. Simplest; requires a full consumer fleet upgrade before
   any producer flips.
2. **Dual-decode window.** Ship a transitional build that decodes BOTH the old and the new
   major (widen the accepted range to `{N, N+1}` for one release), roll it fleet-wide, then
   flip producers to N+1, then in a later release drop N. Needed when consumers and producers
   cannot be ordered (bidirectional traffic, shared subjects).

This ordering is exactly the cost the **additive-only rule exists to avoid**: additive changes
need none of the above (old consumers ignore/carry the new field, rule 1), so `ENVELOPE_VERSION`
should stay put and evolution should stay additive for as long as possible. A major bump is a
last resort with a coordinated rollout, tracked as its own fleet change — never smuggled into a
feature PR. When `ENVELOPE_VERSION` moves, update this section's concrete range and the chosen
rollout mechanic for that bump.

---

## Round-trip guarantee

For any wire bytes `raw` that `from_bytes` accepts (i.e. `version <= ENVELOPE_VERSION`,
required fields present, `payload` a JSON object):

> `Envelope.from_bytes(raw).to_bytes()` reproduces every top-level key present in `raw`,
> including keys this build does not know about, with their original values.

Mechanism: `from_bytes` collects every top-level key that is **not** in the known field set
into `Envelope.extra: dict[str, Any]` (the known set is
`{id, ts, from, kind, payload, to, task_id, channel, topic, correlation_id, version}`).
`to_bytes` seeds the output dict from `extra` FIRST, then `.update()`s the known fields on top
— so on a same-key collision the **known field is written last and wins.** So `extra` can only
ever *add* keys the schema doesn't own; it can never overwrite or corrupt a typed field (a
malicious/buggy producer cannot smuggle a second `id` through `extra`, because the typed `id`
is applied last in the merge order that favors known keys).

`extra` defaults to an empty dict and the dataclass stays `frozen`. A same-version message
with no unknown fields has `extra == {}` and round-trips byte-identically to today.

*Shallow-frozen sharp edge:* `frozen=True` blocks rebinding attributes but does NOT deep-copy
the `payload`/`extra` dicts — they are held by reference and are mutable in place (same for the
pre-existing `payload`). `from_bytes` always constructs a fresh `extra`, so decoded envelopes
never alias external state; direct constructor callers must pass a dict they will not later
mutate. Both dicts are read-only after construction by convention (documented on
`Envelope.extra`), not enforced.

**Bounds on the guarantee (deliberate):**

- *Lossy JSON numbers are rejected, not silently mangled (accepted numeric range).* Rule 1
  promises an unknown value survives the round-trip **unchanged**. `json.loads` decodes every
  fractional/exponent literal through `float` (an IEEE-754 double), which can silently change a
  value in three ways — all refused at DECODE via `parse_float` (`_reject_lossy_float`) so the
  accept-set stays exactly the re-emittable-set (the failure routes to the `subscribe()` poison
  guard, never an escaping encode-side raise), plus the two `NaN`/`Infinity` token forms refused
  via `parse_constant`:
    1. **Overflow** — a huge-exponent literal (`1e400`) decodes to `float('inf')`; refused
       (`math.isfinite` check).
    2. **Underflow** — a tiny-exponent literal (`1e-4000`) decodes to `0.0`, zeroing a non-zero
       producer value; refused (`value == 0.0 and Decimal(token) != 0`).
    3. **Precision loss** — a high-significant-digit decimal (roughly 30+ digits, e.g.
       `1.2345678901234567890123456789012345`) is rounded to the nearest double, so the
       re-emitted value differs from the wire; refused (`Decimal(token) != Decimal(repr(value))`,
       where `repr(float)` is the shortest round-tripping string, so equality means every written
       digit survived — ordinary finite floats like `3.14` pass).
    4. The `NaN`/`Infinity`/`-Infinity` extension *tokens* — refused via `parse_constant` (strict
       parsers such as Dash TS `JSON.parse` reject them).

  **Accepted numeric range, stated honestly:** an *integer* literal (no `.`/`e`) is parsed by
  `parse_int` to an arbitrary-precision Python `int` — always exact, always accepted (huge
  integers included). A *float* literal is accepted iff its `float` decode is finite, non-zero
  unless it is literally zero, and preserves every significant digit at double precision — i.e.
  the double reproduces the producer's exact written value. A float outside that range (overflow,
  underflow, or > double precision) is **refused with `ValueError` at `from_bytes`**, not
  silently rounded, so no accepted message can round-trip to a different value. `to_bytes`
  additionally sets `allow_nan=False` as a belt-and-suspenders guard against a
  constructor-supplied non-finite value. Trade-off: the guarantee is exact-value round-trip over
  the accepted range at the cost of refusing (fail-loud) the sub-double-precision fringe — chosen
  over the alternative (preserve raw numeric lexemes via `Decimal`), which would either leak a
  `Decimal` into `payload` consumers that expect `float` (a behavior change rule 2 forbids within
  a major) or require a custom `json.dumps` encoder; rejecting lossy parses keeps known-field and
  `payload` decoding byte-for-byte identical to pre-C2 and needs no encoder change.
- *Known-field value coercion is out of scope of the round-trip guarantee.* The guarantee
  covers unknown keys (carried raw in `extra`) losslessly. Known optional string fields are
  still coerced (`"to": 123` → `"123"`) by the pre-existing `_opt_str` — that is a typed-field
  normalisation, not part of the unknown-field preservation contract.
- *Alias fields are preserved opaquely, not reconciled.* A stray non-canonical duplicate of a
  known field (e.g. a top-level `"sender"`, which is NOT the canonical wire key — the wire key
  is `"from"`) is treated as an unknown field: preserved in `extra` and re-emitted alongside
  the canonical field. `from` remains authoritative on decode. This is faithful to rule 1
  (preserve what you don't own); producers must use canonical wire keys, and consumers must
  read the canonical field, never an alias.
