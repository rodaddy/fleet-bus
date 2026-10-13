# Coding standards

Adapted from rtech-hermes. This repo is a from-scratch agent product. Code here
must be small, targeted, testable, and typed.

## Session start requirement

Read this document into context at the start of every working session before
touching code.

## Root instruction files: CLAUDE.md is canonical

`CLAUDE.md` and `AGENTS.md` are two byte-identical real files, NOT symlinks.

- **Edit only `CLAUDE.md`.** The `.githooks/pre-commit` hook copies it to
  `AGENTS.md` and stages the copy, so both land in the same commit.
- Direct edits to `AGENTS.md` are blocked by the hook when the files differ.
- Symlinks are banned in this repo.

## Branch discipline: never work in main

`main` is pass-through only — merge, pull, leave. After any merge or pull that
lands you on main, immediately switch to a work branch. Branches describe the
work, not the agent (`feat/walking-skeleton`, `fix/composer-slot-order`, never
`bilby/*` or `agent/*`). All changes merge via PR.

## Source layout

`src`-layout uv packages under `packages/`, separated by responsibility. Do not
build one giant file forced into every runtime.

```text
packages/rtech-persona/
  pyproject.toml
  src/rtech_persona/
    __init__.py
    loader.py
    composer.py
  tests/
    __init__.py
    test_loader.py
    test_composer.py
```

- `config.py` for typed, immutable configuration dataclasses.
- `_<name>_utils.py` for pure utility functions with no side effects.
- Domain modules for business logic.

## Python style

### Module header

Every `.py` file must begin with a short imperative docstring followed by:

```python
from __future__ import annotations
```

`from __future__ import annotations` is mandatory in every module.

### Imports

Relative imports for sibling modules within a package; absolute imports for
cross-package/external. Never parent-relative (`from ..x import y`). Use the
`TYPE_CHECKING` guard for import-time-only type imports.

### Type annotations

Every function has fully typed parameters and a return type. No exceptions. Use
`str | None`, never `Optional[str]`.

### Logging

Use Loguru exclusively. Never stdlib `logging`. Never `print()`.

Log at: entry, exit, failure, fallback, and guard-trigger points. Include
agent/path/profile context when it helps identify the affected runtime.

Never log: secrets, tokens, auth JSON, raw session DBs, full memory dumps, or
private user content. Log sanitized summaries only.

### Docstrings

Google-style. Imperative first sentence. `Args:` / `Returns:` / `Raises:` blocks
when the signature alone is not self-documenting.

### Configuration

Typed, immutable frozen dataclasses with `__post_init__` validation. Constants
belong in frozen dataclasses or module-level `UPPER_CASE`. Never inline magic
numbers or magic strings.

### Control flow

Prefer guard clauses and early returns for invalid or unsupported states. Use
dispatch maps instead of long `if`/`elif` chains.

### Error handling

`try/except/finally` is mandatory around all external calls, file operations, and
hook invocations. Never bare `except:`. Never `except Exception: pass`. Every
exception is logged at the appropriate level. Fail-open must be documented and
intentional at the site.

### File size limit

750 lines hard maximum per file. Split proactively at ~600 lines.

## Toolchain

- `uv` only. Never `pip`.
- Ruff for formatting and linting.
- mypy for type checking.
- pytest for tests.

Quality gates (must all pass before a PR is mergeable):

```bash
uv run ruff check .
uv run ruff format --check .
uv run mypy packages/
uv run pytest -q
```

## Testing requirements

Each feature needs tests before it is claimed done. Testing philosophy:
functional I/O coverage — does the function produce correct output for a given
input? — not line-coverage metrics.

Minimum per module:

- Tests for valid input across all significant branches.
- Tests for invalid input, edge cases, and known bypass patterns.
- An import test confirming the package entrypoint loads without error.

## Pre-commit / pre-push hooks

Hooks are repo-controlled and installable by script (`scripts/install-hooks.sh`).
Pushes cannot bypass quality gates. The secret scan (gitleaks, run by the
machine-wide hooks in `~/.config/git/hooks`) fails closed if gitleaks is
missing, misconfigured, or reports findings.

## Security rules

- Never log secrets, tokens, auth JSON, raw session DBs, or private user content.
- Scrub credentials from variables immediately after use.
- Fail closed on missing auth or guard config.
- Never `exec()` user-controlled input.
- Resolve paths with `Path.resolve()` before any read/write; reject paths that
  escape the expected root.

## Commit and PR hygiene

Stage files explicitly by name — never `git add .`. Commits use conventional
style: `feat:`, `fix:`, `chore:`, `docs:`, `test:`. Do not commit secrets,
`.env`, raw logs, session DBs, or unrelated files.
