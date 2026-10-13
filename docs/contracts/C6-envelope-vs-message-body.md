# C6 — Envelope vs. Message Body

**Contract:** C6
**Status:** Default in force (code-enforced by `fleet_core.spec`).
**Scope:** `packages/fleet-core/src/fleet_core/spec.py` — the `Envelope` every
fleet-bus producer and consumer shares.

This is the base-package rule that every fleet-bus client is built on:

> **The envelope is the coordination contract. The message body is opaque to the
> bus.** The bus routes, dedupes, decides durability, and derives completion /
> stance **only** from typed envelope fields. Everything a receiver puts in the
> message body (`payload`) is free for that receiver to use, but it is **not part
> of the envelope** and **nothing in it can drive coordination.**

It is the companion to [C2](C2-envelope-compat-policy.md) (how the envelope wire
shape *evolves*); C6 says what the envelope *is* versus what it *carries*.

---

## The two layers

Every message on the bus is one JSON object with exactly two kinds of data.

### 1. The envelope — the coordination contract (typed, the bus reads it)

The envelope is the fixed, typed, versioned contract the control plane acts on.
It is the set of known top-level wire keys plus the typed signal:

| Layer | Fields | Who reads them |
|---|---|---|
| Known wire keys | `id`, `ts`, `from`, `kind`, `to`, `task_id`, `channel`, `topic`, `correlation_id`, `version`, `act`, `state` | The bus (routing, dedup, versioning, ACL/plane) |
| Typed signal | `typed_signal` — one of `heartbeat` / `presence` / `stance` / `done` | The bus (durability, presence, scoring, completion) |

These are what the bus is *allowed* to act on. Concretely, the bus derives from
typed fields only:

- **Routing / correlation** — from `to` / `topic` / `channel` / `correlation_id` /
  `task_id`, never from the body.
- **Dedup** — from `id` (`fleet_subscriber.dedup.DedupGuard`), never from body content.
- **Durability (JetStream vs. core)** — from the typed signal's own model via
  `Envelope.prefers_core_delivery()` → `signal_prefers_core()`. This reads
  `jetstream is False` off the typed `Heartbeat` / `Presence` model — **never**
  from the `kind` string, the subject, or a payload key.
- **Completion** — from a typed `Done` signal (`typed_signal`), surfaced as the
  un-spoofable done sentinel. **Not** from any text in the body.
- **Stance** — from a typed `Stance` signal (`typed_signal`). **Not** from any
  text in the body.

### 2. The message body — opaque payload (the bus does NOT read it)

`payload` is a free-form JSON object. The envelope layer validates exactly one
thing about it: that it **is** a JSON object (`from_bytes` rejects a non-object
`payload` with `ValueError`; absence defaults to `{}`). Beyond that shape check,
the bus never inspects it.

- The **receiver** may put anything it likes in `payload` and read anything it
  likes out of it. That is a private contract between a producer and the specific
  consumer kind — per-`kind` free-form JSON.
- Nothing the bus does depends on a `payload` key. A body key is never a routing
  hint, never a durability flag, never a completion or stance signal.
- A control-signal-*looking* string sitting inside the body — a literal
  `[[done]]`, a `co-op · 5s · ~/repo` footer, a `"stance": "agree"` key — is
  **inert display text/data.** It cannot complete a turn or sign a stance,
  because completion and stance are typed envelope fields, not body content.

> Some `kind`s do carry a small, *defined* payload schema (e.g. the done
> sentinel's `payload={"task_id","outcome"}`, or `token_cost` — see
> [architecture.md](../architecture.md)). Those are per-`kind` receiver
> contracts. They are still **body**, not envelope: the authoritative typed
> signal rides `typed_signal`; the payload mirror exists only for pre-typed
> consumers and is never the thing the bus routes or completes on.

---

## Why the split is un-spoofable

The separation is not a convention — the code makes body data structurally
unable to impersonate the envelope:

1. **Known fields are written last.** `Envelope.to_bytes` seeds the wire dict
   from unknown/extra data first, then `.update()`s the known fields and the
   serialized `typed_signal` on top. A body (or extra) key named `id`,
   `typed_signal`, `version`, … cannot overwrite the real typed field — the
   typed value always wins the merge (see C2's round-trip guarantee).

2. **Durability reads the typed model, not the wire.** `signal_prefers_core`
   returns `True` **only** for a bound `Heartbeat`/`Presence` whose own model
   declares `jetstream is False`. A payload key `{"jetstream": false}` does
   nothing — the function type-checks the signal object, it does not scan the
   body. Everything without that typed opt-out is durable by default.

3. **Completion / stance are typed-only.** The legacy `[[done]]` / `[[stance:…]]`
   reply-text trailer parsers were **removed** (fleet-bus
   [#205](https://github.com/rodaddy/fleet-bus/issues/205)). A reply that is not a
   typed turn carries no completion/stance — it is visible prose only. There is
   exactly one path to "done" or a signed stance: a typed `Done` / `Stance` on
   `typed_signal`. Body text has no second path.

---

## What this means for producers and consumers

**Producers MUST:**

- Put coordination intent in typed envelope fields — `act` / `state`,
  `to` / `topic` / `correlation_id` / `task_id`, and a `typed_signal`
  (`Done` / `Stance` / `Heartbeat` / `Presence`) when signalling completion,
  stance, or liveness.
- Treat `payload` as receiver-facing data only. Never encode a coordination
  signal as a payload key or as a token inside body text and expect the bus to
  act on it — it will not.

**Consumers MUST:**

- Derive coordination from the typed envelope (`typed_signal`, `act`/`state`,
  the routing keys), never by scraping the body.
- Read `payload` only as the private per-`kind` contract it is. Treat any
  control-looking string inside the body as inert data.

**The bus WILL NOT:**

- Route, dedupe, decide durability, or derive completion/stance from any
  `payload` key or any body text. If it isn't a typed envelope field, the bus
  does not act on it.

---

## Relationship to the other contracts

- **[C2 — Envelope Compatibility Policy](C2-envelope-compat-policy.md):** how the
  envelope's *known set* evolves (additive-only within a major, version gate,
  unknown-field round-trip). C6 defines the envelope/body boundary; C2 governs
  changes to the envelope side of it. Adding a key **inside `payload`** is a body
  change and needs no version bump (C2 rule "add a new key inside `payload`").
  Adding or changing an **envelope** field is governed by C2.
- **fleet-bus [#205](https://github.com/rodaddy/fleet-bus/issues/205):** removed
  the text-trailer completion/stance path, making "typed signal only" the sole
  path — the concrete enforcement of C6's completion/stance rule.
