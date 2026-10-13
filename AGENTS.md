# fleet-bus -- agent instructions

Instructions for coding agents (and humans) working in this repo.

## Coding standards (MANDATORY)

**Read `docs/coding-standards.md` at the start of every session before touching
code.** It is shared with rtech-agents. Everything below is a
fleet-bus delta on top of it, not a replacement.

Key points that apply here (Python):

- **`uv` only** — `uv add` / `uv run` / `uv sync`. Never pip/pip3/conda/pipx.
- **Never work on `main`.** Feature branches only: `<type>/<description>`. Merge via PR.
- **Stage files by name** — never `git add .` / `-A`. Never commit `.env`/secrets.
- **Pre-push hooks MUST check the same paths as CI** — `ruff check src/ tests/`,
  not just `src/`. A hook that skips `tests/` is a gap.
- **Error handling** — no silent `except: pass`; log before returning a fallback.
- **Shell shebangs** — `#!/usr/bin/env bash|zsh|sh`, never `#!/bin/bash`.
- **No complex quoting over SSH** — pipe scripts via stdin, don't nest Python/JSON.

## What this repo is

The fleet message + coordination bus. Canonical NATS transport and (soon) the
DAP coordination layer for the agent fleet, Open Brain memory, and the k-board.
Repurposed from the retired `bilby-a2a` A2A server (2026-07-07).

## Stack

| Layer | Tool |
|---|---|
| Language | Python **>=3.13** (fleet standard, rtech-hermes #338) |
| Package manager | `uv` (never pip) |
| Lint / format | `ruff` |
| Types | `mypy --strict` |
| Tests | `pytest` (+ `pytest-asyncio`) |
| Transport | `nats-py>=2.15` (NATS core; JetStream is infra, not surfaced) |
| Logging | `loguru` |

## Layout

```
fleet-bus/
├── docs/coding-standards.md      ← shared coding standards (read first)
├── pyproject.toml                ← uv workspace (members = packages/*)
└── packages/
    └── fleet-nats/               ← the shared NATS library (the foundation)
        ├── src/fleet_nats/       config · envelope · subjects · bus
        └── tests/                unit + live (auto-skip off-network)
```

## Design invariants (do not violate)

- **Discord = human display.** Agents post their own words. Coordination does NOT
  live in Discord.
- **NATS = the DAP control plane** — deterministic code (case-statements), our own,
  off Discord's mention-gate/thread mechanics.
- **k-board Postgres = handoff source of truth.** NATS delivers; it is not the record.
- **JetStream is infra** — durability/observability, invisible to agents. The
  agent-facing API is NATS core only.
- **Subjects mirror Dash** `{env}.{domain}.{...}` (`@example/core-nats`) so the
  fleet JS nodes buttress the Dash mesh, not fork it. Build subjects only via
  `fleet_nats.subjects`.
- **Connection config from env** (`FLEET_NATS_URL`, `FLEET_AGENT_ID`, `FLEET_ENV`) —
  never baked into the library.

## Dev / gates

```bash
cd packages/fleet-nats
uv venv --python 3.13 --seed .venv
uv pip install --python .venv/bin/python -e ".[dev]"
.venv/bin/ruff check src/ tests/
.venv/bin/ruff format --check src/ tests/
.venv/bin/mypy src/
.venv/bin/python -m pytest tests/ -q
```

Fleet NATS server: `nats://192.0.2.74:4222` (fleet-hub). Live bus tests auto-skip
when it is unreachable.

## Corrections

<!-- Add gotchas discovered during development here -->
