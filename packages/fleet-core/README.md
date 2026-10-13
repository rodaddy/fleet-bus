# fleet-core

Base package for fleet-bus clients. It owns the typed message spec
(`fleet_core.spec`), config, and transport contract every fleet-bus producer and
consumer shares.

## The core rule: envelope vs. message body

Every fleet-bus message is one JSON object with two layers, and the split is the
foundation everything else is built on:

> **The envelope is the coordination contract. The message body is opaque to the
> bus.** The bus routes, dedupes, decides durability, and derives completion /
> stance **only** from typed envelope fields (`fleet_core.spec.Envelope` — its
> known wire keys and its `typed_signal`: `heartbeat` / `presence` / `stance` /
> `done`). Everything in the message body (`payload`) is free for the receiving
> consumer to use, but it is **not part of the envelope** and **nothing in it can
> drive coordination.**

A producer signals completion, stance, or liveness with a **typed signal**
(`Done` / `Stance` / `Heartbeat` / `Presence`) on the envelope — never with a
payload key or a token inside body text. A control-looking string in the body
(a literal `[[done]]`, a `"stance"` key, a footer) is inert data: the bus does
not act on it. This is code-enforced, not a convention — see
[`spec.py`](src/fleet_core/spec.py) (`to_bytes` writes typed fields last;
`signal_prefers_core` reads the typed model, never the body) and the
[C6 contract](../../docs/contracts/C6-envelope-vs-message-body.md) for the full
statement and rationale. The legacy `[[done]]`/`[[stance]]` text-trailer path was
removed in fleet-bus
[#205](https://github.com/rodaddy/fleet-bus/issues/205), leaving typed signals as
the only path.

For how the envelope wire shape *evolves* (additive-only within a major, version
gate, unknown-field round-trip), see the
[C2 compatibility policy](../../docs/contracts/C2-envelope-compat-policy.md).

## Version discipline

Downstream clients should pin private GitHub refs to immutable release tags once
fleet-core releases begin. Package version metadata lives in `pyproject.toml`;
`fleet_core.__version__` reads the installed distribution metadata so editable,
workspace, and Git ref installs expose the same version string.

Release tags use `fleet-core/v{project.version}`. For the current `0.1.0`
package version, downstream clients install the monorepo subdirectory with:

```sh
uv add "fleet-core @ git+ssh://git@github.com/rodaddy/fleet-bus.git@fleet-core/v0.1.0#subdirectory=packages/fleet-core"
```

See [RELEASING.md](RELEASING.md) for the immutable tag checklist and manual
Git-subdirectory install proof.
