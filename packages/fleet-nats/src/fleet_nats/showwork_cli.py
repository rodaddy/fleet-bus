"""``fleet-show-work <task_id> [--format md|json]`` — the hub-side CLI (#64).

Pull-only, read-only: reads the hub's per-task frame journal and prints the
scoped deliberation trail for one task — summary header first, transcript second,
timestamps in ET, credential-shaped text redacted (fail-closed).

The journal path comes from ``--journal`` or ``$FLEET_JOURNAL_PATH`` (default
``/opt/fleet-bus/trace`` — a directory is scanned for ``*.jsonl``; a file is read
directly). The CLI does the I/O; the reconstruction is the pure
:mod:`fleet_nats.showwork` engine, so the exact same trail renders under test.

NOTE (Phase 5): this CLI is NOT wired to any live display path yet. The
Discord-thread display command lands with the deploy phase; this ships the read
engine + CLI + tests only.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from collections.abc import Iterator
from pathlib import Path

from loguru import logger

from fleet_nats.showwork import (
    reconstruct,
    reconstruct_json,
    render_markdown,
)

_ENV_JOURNAL = "FLEET_JOURNAL_PATH"
_DEFAULT_JOURNAL = "/opt/fleet-bus/trace"


def _iter_journal_lines(path: Path) -> Iterator[str]:
    """Yield journal lines from a file, or from every ``*.jsonl`` in a directory.

    Streaming, one line at a time — never materializes the whole journal. The
    reconstruction engine scopes to the task AS IT READS, so peak memory is
    O(matching frames), not O(directory) — a multi-GB trace dir must never be
    slurped just to pull one task's trail.

    Raises:
        FileNotFoundError: If the path does not exist (fail loud — a missing
            journal is a misconfig, not an empty trail).
    """
    if not path.exists():
        raise FileNotFoundError(f"journal path not found: {path}")
    files = sorted(path.glob("*.jsonl")) if path.is_dir() else [path]
    for jf in files:
        with jf.open(encoding="utf-8") as fh:
            yield from fh


def _resolve_journal(explicit: str | None) -> Path:
    """Pick the journal path: ``--journal`` → ``$FLEET_JOURNAL_PATH`` → default."""
    return Path(explicit or os.environ.get(_ENV_JOURNAL) or _DEFAULT_JOURNAL)


def main(argv: list[str] | None = None) -> int:
    """CLI entry point. Returns a process exit code (0 ok, 2 no such task/journal)."""
    parser = argparse.ArgumentParser(
        prog="fleet-show-work",
        description="Reconstruct one task's scoped deliberation trail (pull-only, read-only).",
    )
    parser.add_argument("task_id", help="The k-board task id to reconstruct.")
    parser.add_argument(
        "--format", choices=("md", "json"), default="md", help="Output format (default md)."
    )
    parser.add_argument(
        "--journal",
        default=None,
        help=f"Journal file or dir (default ${_ENV_JOURNAL} or {_DEFAULT_JOURNAL}).",
    )
    parser.add_argument(
        "--topic",
        default=None,
        help="Run arbitration topic, to pull in topic-scoped arbitrate frames.",
    )
    args = parser.parse_args(argv)

    journal = _resolve_journal(args.journal)
    # Stream the journal through the engine, which scopes to the task AS IT READS
    # — peak memory is O(matching frames), not O(directory). The generator checks
    # existence eagerly, so a missing journal still fails loud here.
    try:
        summary, frames = reconstruct(_iter_journal_lines(journal), args.task_id, topic=args.topic)
    except FileNotFoundError as exc:
        logger.error("fleet-show-work | {}", exc)
        print(f"error: {exc}", file=sys.stderr)
        return 2
    if summary.frame_count == 0:
        msg = f"no frames found for task {args.task_id!r} in {journal}"
        logger.warning("fleet-show-work | {}", msg)
        print(f"error: {msg}", file=sys.stderr)
        return 2

    if args.format == "json":
        print(json.dumps(reconstruct_json(summary, frames), indent=2, sort_keys=True))
    else:
        # CLI renders the FULL document (no cap) — the tool surface is the capped one.
        print(render_markdown(summary, frames, max_chars=None).markdown)
    return 0


if __name__ == "__main__":  # pragma: no cover — thin process shim
    raise SystemExit(main())
