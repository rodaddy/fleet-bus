#!/usr/bin/env bash
# Checker for specs/fleet-bus-nats-coordination.html.
# Runs every phase's machine-checkable gate, prints per-box PASS/FAIL, and exits
# non-zero if any gate fails. The Build workflow sets plan markers from THIS exit
# code — the agent's self-report is non-authoritative.
#
# Gates for phases not yet built are reported SKIP (the artifact doesn't exist
# yet) so this can run at any point and only assert what currently should hold.
set -uo pipefail

repo_root="$(cd "$(dirname "$0")/.." && pwd)"
cd "$repo_root" || exit

pass=0
fail=0
skip=0

# check <label> <command...> : run a gate, print result, tally.
check() {
    local label="$1"; shift
    if "$@" >/dev/null 2>&1; then
        printf 'PASS  %s\n' "$label"
        pass=$((pass + 1))
    else
        printf 'FAIL  %s\n' "$label"
        fail=$((fail + 1))
    fi
}

# check_if <guard-file> <label> <command...> : SKIP when the artifact isn't built yet.
check_if() {
    local guard="$1"; shift
    local label="$1"; shift
    if [ ! -e "$guard" ]; then
        printf 'SKIP  %s (not built: %s)\n' "$label" "$guard"
        skip=$((skip + 1))
        return
    fi
    check "$label" "$@"
}

package_gate() {
    local pkg="$1"
    local d="packages/$pkg"
    if [ -x "$d/.venv/bin/ruff" ]; then
        (cd "$d" && .venv/bin/ruff check src/ tests/ && .venv/bin/ruff format --check src/ tests/ && .venv/bin/mypy src/ tests/ && .venv/bin/python -m pytest tests/ -q)
    else
        (cd "$d" && uv run --extra dev ruff check src/ tests/ && uv run --extra dev ruff format --check src/ tests/ && uv run --extra dev mypy src/ tests/ && uv run --extra dev python -m pytest tests/ -q)
    fi
}

grep_python_code() {
    local pattern="$1"; shift
    uv run --quiet python - "$pattern" "$@" <<'PY'
from __future__ import annotations

import io
import re
import sys
import tokenize
import ast

pattern = re.compile(sys.argv[1])


def blank_line_segment(line: str, start_col: int, end_col: int) -> str:
    line_end = len(line.rstrip("\r\n"))
    safe_end = min(end_col, line_end)
    return line[:start_col] + (" " * max(0, safe_end - start_col)) + line[safe_end:]


def blank_span(lines: list[str], start: tuple[int, int], end: tuple[int, int]) -> None:
    start_line, start_col = start
    end_line, end_col = end
    if start_line == end_line:
        lines[start_line - 1] = blank_line_segment(lines[start_line - 1], start_col, end_col)
        return
    lines[start_line - 1] = blank_line_segment(lines[start_line - 1], start_col, len(lines[start_line - 1]))
    for line_no in range(start_line + 1, end_line):
        lines[line_no - 1] = blank_line_segment(lines[line_no - 1], 0, len(lines[line_no - 1]))
    lines[end_line - 1] = blank_line_segment(lines[end_line - 1], 0, end_col)


def strip_comments_and_docstrings(source: str, tree: ast.AST) -> tuple[str, set[int]]:
    lines = source.splitlines(keepends=True)
    docstrings: set[int] = set()
    docstring_spans: list[tuple[tuple[int, int], tuple[int, int]]] = []

    def character_position(line_no: int, byte_col: int) -> tuple[int, int]:
        byte_prefix = lines[line_no - 1].encode("utf-8")[:byte_col]
        return line_no, len(byte_prefix.decode("utf-8"))

    for node in ast.walk(tree):
        body = getattr(node, "body", None)
        if not isinstance(body, list) or not body:
            continue
        first = body[0]
        if (
            isinstance(first, ast.Expr)
            and isinstance(first.value, ast.Constant)
            and isinstance(first.value.value, str)
        ):
            docstrings.add(id(first.value))
            # AST columns are UTF-8 byte offsets; tokenize columns are character
            # offsets. Convert both ends so every token in a concatenated
            # docstring is blanked without touching following executable code.
            docstring_spans.append(
                (
                    character_position(first.lineno, first.col_offset),
                    character_position(first.end_lineno, first.end_col_offset),
                )
            )
    for token in tokenize.generate_tokens(io.StringIO(source).readline):
        if token.type == tokenize.STRING and any(
            start <= token.start and token.end <= end for start, end in docstring_spans
        ):
            blank_span(lines, token.start, token.end)
        elif token.type == tokenize.COMMENT:
            blank_span(lines, token.start, token.end)
    return "".join(lines), docstrings


def effective_string(node: ast.expr) -> str | None:
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    if isinstance(node, ast.JoinedStr):
        parts: list[str] = []
        for value in node.values:
            if isinstance(value, ast.Constant) and isinstance(value.value, str):
                parts.append(value.value)
            elif isinstance(value, ast.FormattedValue):
                parts.append("{" + ast.unparse(value.value) + "}")
        return "".join(parts)
    return None


found = False
for path in sys.argv[2:]:
    try:
        with open(path, encoding="utf-8") as handle:
            source = handle.read()
        tree = ast.parse(source, filename=path)
        code, docstrings = strip_comments_and_docstrings(source, tree)
        candidates = [
            value
            for node in ast.walk(tree)
            if id(node) not in docstrings
            if (value := effective_string(node)) is not None
        ]
    except Exception as exc:
        print(f"{path}: scanner error ({type(exc).__name__})", file=sys.stderr)
        sys.exit(2)
    if pattern.search(code) or any(pattern.search(repr(candidate)) for candidate in candidates):
        print(path)
        found = True
sys.exit(0 if found else 1)
PY
}

# grep_python_code uses grep-like statuses: 0=match, 1=no match, >=2=error.
# Only a proven no-match is a passing negative gate.
no_python_match() {
    grep_python_code "$@"
    local result=$?
    [ "$result" -eq 1 ]
}

# Only the audited defensive empty handlers listed below are allowed:
# - packages/fleet-core/src/fleet_core/config.py catches OSError to fall through
#   from IPv4 normalization into IPv6/hostname normalization.
# - packages/fleet-nats/src/fleet_nats/jetstream.py catches asyncio.QueueEmpty
#   in a defensive STOP_ITER enqueue race cleanup.
# - fleet-orchestrator poll loops catch TimeoutError as the normal tick/sweep edge.
no_silent_except_pass() {
    uv run --quiet python - "$@" <<'PY'
from __future__ import annotations

import ast
import sys

allowed = {
    ("packages/fleet-core/src/fleet_core/config.py", 109, "OSError"),
    ("packages/fleet-nats/src/fleet_nats/jetstream.py", 580, "asyncio.QueueEmpty"),
    ("packages/fleet-orchestrator/src/fleet_orchestrator/killswitch.py", 263, "TimeoutError"),
    ("packages/fleet-orchestrator/src/fleet_orchestrator/intake_poller.py", 506, "TimeoutError"),
    ("packages/fleet-orchestrator/src/fleet_orchestrator/loop.py", 900, "TimeoutError"),
    ("packages/fleet-orchestrator/src/fleet_orchestrator/loop.py", 964, "TimeoutError"),
}


def exc_name(node: ast.expr | None) -> str:
    if node is None:
        return "bare"
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        base = exc_name(node.value)
        return f"{base}.{node.attr}" if base else node.attr
    return ast.unparse(node)


violations: list[str] = []
for path in sys.argv[1:]:
    tree = ast.parse(open(path, encoding="utf-8").read(), filename=path)
    for node in ast.walk(tree):
        if not isinstance(node, ast.ExceptHandler):
            continue
        if len(node.body) == 1 and isinstance(node.body[0], ast.Pass):
            name = exc_name(node.type)
            if (path, node.lineno, name) not in allowed:
                violations.append(f"{path}:{node.lineno}: except {name}: pass")

if violations:
    print("\n".join(violations))
    sys.exit(1)
PY
}

grep_tracked_production_secrets() {
    uv run --quiet python - <<'PY'
from __future__ import annotations

import io
import re
import subprocess
import sys
import tokenize

pattern = re.compile(r"(discord\.com/api/webhooks|xoxb-|Bearer [A-Za-z0-9])")
excluded_exact = {"packages/fleet-nats/src/fleet_nats/redaction_patterns.toml"}
excluded_parts = {"/tests/", "/.venv/", "/__pycache__/", "/.pytest_cache/", "/.mypy_cache/", "/.ruff_cache/"}

paths = subprocess.check_output(["git", "ls-files", "packages", "docs"], text=True).splitlines()
violations: list[str] = []
for path in paths:
    if path in excluded_exact or any(part in f"/{path}" for part in excluded_parts):
        continue
    try:
        raw = open(path, "rb").read()
    except OSError as exc:
        print(f"{path}: unreadable tracked production file ({type(exc).__name__})", file=sys.stderr)
        sys.exit(2)
    if path.endswith(".py"):
        tokens = tokenize.tokenize(io.BytesIO(raw).readline)
        text = " ".join(
            token.string
            for token in tokens
            if token.type
            not in {
                tokenize.ENCODING,
                tokenize.COMMENT,
                tokenize.NL,
                tokenize.NEWLINE,
                tokenize.INDENT,
                tokenize.DEDENT,
                tokenize.ENDMARKER,
            }
        )
    else:
        text = raw.decode("utf-8", errors="ignore")
    if pattern.search(text):
        violations.append(path)

if violations:
    print("\n".join(violations))
    sys.exit(1)
PY
}

package_source_files_except() {
    find packages -path '*/src/*.py' "$@" -print
}
export -f grep_python_code no_silent_except_pass grep_tracked_production_secrets package_source_files_except

echo "== Phase 0: foundation landed =="
# P0 asserts the MERGE happened — only checkable AFTER merge. Pre-merge these
# can't be true yet, so report SKIP (not-yet), not FAIL: gate them on the
# post-merge signal (fleet-nats present on origin/main). This keeps the checker
# honest — green-or-skip — instead of red for the whole life of the PR it gates.
if git ls-tree -r origin/main --name-only 2>/dev/null | grep -q 'fleet_nats/bus.py'; then
    # shellcheck disable=SC2016
    check "P0 PR #2 merged" bash -c \
        'test "$(gh pr view 2 --json state -q .state 2>/dev/null)" = "MERGED"'
    check "P0 fleet-nats on main, no finch_a2a" bash -c \
        'test -f packages/fleet-nats/src/fleet_nats/bus.py && ! git ls-tree -r origin/main --name-only 2>/dev/null | grep -q finch_a2a'
else
    echo "SKIP  P0 foundation on main (not merged yet — pre-merge)"
    skip=$((skip + 2))
fi

echo "== Cross-cutting: package gates =="
for p in fleet-nats fleet-orchestrator fleet-subscriber; do
    d="packages/$p"
    # Not built yet → legitimate SKIP. But a BUILT package (has src/) with no
    # .venv can't be gated — that's a FAIL, not a silent skip, else the checker
    # reports "0 fail" having run nothing (the false-green Codex flagged).
    if [ ! -d "$d/src" ]; then
        printf 'SKIP  %s four gates (not built: %s/src)\n' "$p" "$d"
        skip=$((skip + 1))
        continue
    fi
    check "$p four gates (ruff/format/mypy/pytest on src+tests)" package_gate "$p"
done

echo "== Cross-cutting: hygiene greps =="
# shellcheck disable=SC2046
check_if "packages/fleet-orchestrator/src" "no hand-formatted subjects" \
    no_python_match "([\"\047](dev|prod)|\\{[a-zA-Z_.]*env\\})\\.(agent|direct|human|kboard)\\." \
    $(package_source_files_except ! -name subjects.py ! -name registry.py)
# shellcheck disable=SC2016
check_if "packages/fleet-orchestrator/src" "no silent excepts" bash -c \
    'no_silent_except_pass $(find packages -path "*/src/*.py" -print)'
# Scan all tracked production files under packages/ and docs/. Tests, caches,
# venvs, and the exact redaction-pattern catalog are excluded because they
# intentionally contain secret-shaped fixtures or generated artifacts.
check "no secrets/webhooks in code+docs" bash -c \
    'grep_tracked_production_secrets'

echo "== Phase 3/4: subscriber + sentinel =="
check_if "docs/subscriber-live-verify.md" "subscriber live-verify documented" bash -c \
    'grep -qi discord docs/subscriber-live-verify.md'
check_if "packages/fleet-subscriber/src" "teams_pipeline dead-end not copied" bash -c \
    '! grep -rn teams_pipeline packages/fleet-subscriber/src 2>/dev/null'

echo "== Phase 6: observability — logging convention + audit =="
check_if "docs/logging.md" "logging convention documented" bash -c \
    'grep -qi "never-log" docs/logging.md && grep -qi "task_id" docs/logging.md'
# No secret/webhook/member-id patterns inside any log statement. Scans all three
# src trees (fleet-nats has the bus publish/subscribe log lines too), stricter
# than the plan's orchestrator+subscriber-only scope.
check "no secrets in log statements" bash -c \
    '! grep -rnE "logger\.(info|debug|warning|error).*(webhook|discord.*id|secret|bearer|token[[:space:]]*(=|:|\\{|%|\\$))" packages/*/src --include="*.py" 2>/dev/null'

echo "== Phase 7/8: deploy + docs =="
# The probe is a Phase-7 DEPLOY gate — it must SKIP until the subscriber is
# deployed to the trio, not merely built. Guarding on packages/fleet-subscriber/src
# was wrong: that dir exists the moment Phase 3 lands (subscriber BUILT), which
# flipped the stub from SKIP to a false FAIL two phases early. Gate instead on the
# deploy artifact docs/runbook.md (produced by Phase 7/8, carrying the enable
# procedure the probe verifies). Until the runbook exists, the deploy hasn't
# happened, so the probe is an unbuilt gate → SKIP (never a false PASS: once the
# runbook lands, the real probe must actually pass).
# The probe distinguishes SKIP (exit 2, monitor unreachable — off-network/not
# deployed) from PASS (exit 0, all connected) and FAIL (exit 1, reachable but an
# agent missing). check() only knows PASS/FAIL, so a plain check_if would score a
# SKIP as PASS — the exact false-green this checker guards against elsewhere. Map
# the three codes explicitly: only a real exit-0 PASS tallies as pass.
# A probe FAIL (monitor reachable, agent missing) is a REGRESSION only when the
# plan claims Phase 7 done. While the plan itself marks Phase 7 [wip] (live
# enable deferred to the maintainer window / agent self-install), trio-not-
# connected is the DECLARED state — reported as SKIP with the reason, not a red
# gate. The moment the HTML claims "[x] Phase 7", a failing probe becomes a
# claim-vs-reality mismatch and fails the run. This gates the marker on the
# plan's own claim — overstating the marker is exactly what gets caught.
if [ ! -e "docs/runbook.md" ]; then
    printf 'SKIP  trio effective-state probe (not built: docs/runbook.md)\n'
    skip=$((skip + 1))
else
    bash specs/probe-trio-subscribers.sh >/dev/null 2>&1
    probe_rc=$?
    # Anchor on the phase H3 itself so an amendment QUOTING the marker string
    # can't flip the claim, and a checklist-item [x] can't either — the heading
    # is the one authoritative phase marker.
    phase7_claimed_done=false
    if grep -qE '<h3><code class="status">\[x\]</code> Phase 7:' \
        specs/fleet-bus-nats-coordination.html; then
        phase7_claimed_done=true
    fi
    if [ "$probe_rc" -eq 0 ]; then
        printf 'PASS  trio effective-state probe\n'; pass=$((pass + 1))
    elif [ "$probe_rc" -eq 2 ]; then
        printf 'SKIP  trio effective-state probe (monitor unreachable — off-network/not deployed)\n'
        skip=$((skip + 1))
    elif [ "$probe_rc" -ne 1 ]; then
        # Not one of the probe's three documented codes (127 = script missing, a
        # crash, …) — a BROKEN probe must never masquerade as the declared state.
        printf 'FAIL  trio effective-state probe (probe itself broken: exit %s)\n' "$probe_rc"
        fail=$((fail + 1))
    elif [ "$phase7_claimed_done" = false ]; then
        printf 'SKIP  trio effective-state probe (trio not connected — Phase 7 still [wip], live enable deferred)\n'
        skip=$((skip + 1))
    else
        printf 'FAIL  trio effective-state probe (plan claims Phase 7 [x] but an agent is not connected)\n'
        fail=$((fail + 1))
    fi
fi
check_if "docs/runbook.md" "runbook has rollback + update-survival" bash -c \
    'grep -qi rollback docs/runbook.md && grep -qiE "update.survival" docs/runbook.md'
for f in docs/consumers.md docs/architecture.md docs/runbook.md; do
    check_if "$f" "doc present: $f" test -f "$f"
done

echo "== Phase 9: e2e live proving run =="
# The live proof (#9). Runs against fleet-hub and real agents; auto-SKIPs off-network
# via the pytest live gate, so a green-or-skip run keeps CI clean. Gated on the
# orchestrator package existing (its tests hold the e2e marker).
#
# pytest exit codes / outcomes matter here, and exit code ALONE is not enough: a
# run where the only collected test SKIPS (fleet-hub unreachable off-network) returns
# exit 0 — identical to a real PASS. Scoring that as PASS would be a false green on
# the acceptance gate (nothing was actually proven). So we parse the outcome:
#   - exit 5             = "no tests collected" (e2e not built yet)     -> SKIP
#   - output "N skipped" and "0 passed" (test ran but skipped, off-net) -> SKIP
#   - output "N passed"  (test ran and passed live)                     -> PASS
#   - anything else (a real failure)                                    -> FAIL
# Only a genuine on-network PASS tallies as pass; a skip is never a false green.
if [ ! -d "packages/fleet-orchestrator/src" ]; then
    echo "SKIP  e2e live trio proving run (pass-or-skip) (not built: packages/fleet-orchestrator/src)"
    skip=$((skip + 1))
else
    e2e_out="$(cd packages/fleet-orchestrator && FLEET_NATS_URL=nats://192.0.2.74:4222 \
        uv run --extra dev python -m pytest tests -q -k e2e 2>&1)"
    e2e_rc=$?
    if [ "$e2e_rc" -eq 5 ]; then
        echo "SKIP  e2e live trio proving run (pass-or-skip) (no e2e test yet — Phase 9)"
        skip=$((skip + 1))
    elif printf '%s' "$e2e_out" | grep -qE '[1-9][0-9]* (failed|error)'; then
        # Any failure OR collection/fixture error is a FAIL, even alongside a
        # passed/skipped test — checked FIRST so a mixed "1 failed, 1 passed" (or
        # "errors during collection") can't be masked by the passed/skipped greps.
        echo "FAIL  e2e live trio proving run (pass-or-skip)"
        fail=$((fail + 1))
    elif [ "$e2e_rc" -eq 0 ] && printf '%s' "$e2e_out" | grep -qE '[1-9][0-9]* passed'; then
        # A clean run (exit 0) with at least one passing e2e test — live on-network.
        echo "PASS  e2e live trio proving run (pass-or-skip)"
        pass=$((pass + 1))
    elif printf '%s' "$e2e_out" | grep -qE '[1-9][0-9]* skipped'; then
        # Collected but skipped (fleet-hub unreachable) — nothing proven, so SKIP not PASS.
        echo "SKIP  e2e live trio proving run (pass-or-skip) (fleet-hub unreachable — off-network)"
        skip=$((skip + 1))
    else
        echo "FAIL  e2e live trio proving run (pass-or-skip)"
        fail=$((fail + 1))
    fi
fi

echo
printf 'SUMMARY: %d pass, %d fail, %d skip\n' "$pass" "$fail" "$skip"
[ "$fail" -eq 0 ]
