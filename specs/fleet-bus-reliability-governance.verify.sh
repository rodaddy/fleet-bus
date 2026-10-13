#!/usr/bin/env bash
# Checker for specs/fleet-bus-reliability-governance.html (issues #13 #14 #17 #22 #25 #26 #27 #28).
# Runs every phase's machine-checkable gate, prints per-box PASS/FAIL/SKIP, and
# exits non-zero if any gate fails. The Build workflow sets plan markers from
# THIS exit code — the agent's self-report is non-authoritative.
#
# Gates for phases not yet built report SKIP (the artifact doesn't exist yet) so
# this can run at any point and only assert what currently should hold. A skip
# is an unbuilt gate, never a pass.
set -uo pipefail

# Two modes (codex cross-review, C1):
#   default        — exit 0 iff no runnable gate FAILed (skips allowed: dev loop)
#   STRICT=1       — exit nonzero if ANY gate skipped or failed. Plan-completion
#                    verification and the Build workflow's final marker pass MUST
#                    use STRICT=1; a fresh checkout full of SKIPs is not green.
STRICT="${STRICT:-0}"

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

# check_if <guard-path> <label> <command...> : SKIP when the artifact isn't built yet.
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

fleet_orchestrator_service_unit() {
    local runbook="$1"
    awk '
        $0 == "   # /etc/systemd/system/fleet-orchestrator.service" && previous == "   ```" {
            marker_count++
            capture = 1
        }
        capture && $0 == "   ```" {
            closed = 1
            capture = 0
            next
        }
        capture { print }
        { previous = $0 }
        END {
            if (marker_count != 1 || !closed) {
                exit 1
            }
        }
    ' "$runbook"
}

fleet_orchestrator_service_unit_has_production_env() {
    local runbook="$1"
    local unit
    unit="$(fleet_orchestrator_service_unit "$runbook")" || return 1

    grep -q "^   Environment=FLEET_INTAKE_CHANNEL=all-agents$" <<<"$unit" &&
        grep -q "^   Environment=FLEET_BUDGET_MSGS=200$" <<<"$unit" &&
        grep -q "^   Environment=FLEET_DISPATCH_TIMEOUT_SECONDS=300$" <<<"$unit" &&
        grep -q "^   Environment=FLEET_OFFLINE_POLL_SECONDS=30$" <<<"$unit"
}

runbook_has_production_unit_env_and_resume() {
    local runbook="$1"
    fleet_orchestrator_service_unit_has_production_env "$runbook" && grep -qi resume "$runbook"
}

rejects_service_env_outside_unit() {
    local fixture
    fixture=$'   ```\n   # /etc/systemd/system/fleet-orchestrator.service\n   [Service]\n   Environment=FLEET_INTAKE_CHANNEL=all-agents\n   Environment=FLEET_BUDGET_MSGS=200\n   Environment=FLEET_DISPATCH_TIMEOUT_SECONDS=300\n   ```\n   Environment=FLEET_OFFLINE_POLL_SECONDS=30\n'

    ! fleet_orchestrator_service_unit_has_production_env /dev/stdin <<<"$fixture"
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

pytest_out() {
    local venv="$1"; shift
    local pkg="$1"; shift
    local kexpr="$1"; shift
    if [ -x "$venv" ]; then
        (cd "$pkg" && ".venv/bin/python" -m pytest tests/ -q -k "$kexpr" 2>&1)
    else
        (cd "$pkg" && uv run --extra dev python -m pytest tests/ -q -k "$kexpr" 2>&1)
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

# check_pytest <guard-venv> <label> <pkg-dir> <-k expr> : outcome-parsed pytest.
# Exit code alone is not enough: a run whose only tests SKIP returns 0, identical
# to a real pass — the false green this checker exists to prevent. Parse outcomes:
#   exit 5 / "no tests ran"       -> SKIP (tests not written yet)
#   any "N failed|error"          -> FAIL (checked first, can't be masked)
#   exit 0 + "N passed"           -> PASS
#   collected-but-skipped only    -> SKIP (nothing proven)
check_pytest() {
    local venv="$1"; shift
    local label="$1"; shift
    local pkg="$1"; shift
    local kexpr="$1"; shift
    if [ ! -d "$pkg/src" ]; then
        printf 'SKIP  %s (not built: %s/src)\n' "$label" "$pkg"
        skip=$((skip + 1))
        return
    fi
    local out rc
    out="$(pytest_out "$venv" "$pkg" "$kexpr")"
    rc=$?
    if [ "$rc" -eq 5 ]; then
        printf 'SKIP  %s (no matching tests yet)\n' "$label"
        skip=$((skip + 1))
    elif printf '%s' "$out" | grep -qE '[1-9][0-9]* (failed|error)'; then
        printf 'FAIL  %s\n' "$label"
        fail=$((fail + 1))
    elif [ "$rc" -eq 0 ] && printf '%s' "$out" | grep -qE '[1-9][0-9]* passed'; then
        # Annotate partial runs (codex C2): a pass alongside skips proved only the
        # local half — the label must say so, never read as full acceptance.
        local skipped
        skipped="$(printf '%s' "$out" | grep -oE '[1-9][0-9]* skipped' | head -1)"
        if [ -n "$skipped" ]; then
            printf 'PASS  %s (partial: %s)\n' "$label" "$skipped"
        else
            printf 'PASS  %s\n' "$label"
        fi
        pass=$((pass + 1))
    elif [ "$rc" -eq 0 ] && printf '%s' "$out" | grep -qE '[1-9][0-9]* skipped'; then
        printf 'SKIP  %s (collected but skipped — env-bound)\n' "$label"
        skip=$((skip + 1))
    else
        printf 'FAIL  %s\n' "$label"
        fail=$((fail + 1))
    fi
}

# no_banned_words <file> <ERE> : gate that FILE contains no banned word OUTSIDE
# comments. Comments are stripped positionally (sed 's/#.*//'), never by dropping
# whole lines — a trailing comment cannot launder a real code token past the gate
# (swarm finding F1). Patterns use word boundaries so `context`/`read_text` never
# false-match `text`.
no_banned_words() {
    local file="$1"; shift
    local pattern="$1"; shift
    no_python_match "$pattern" "$file"
}

echo "== Phase 0 (#28): env-matrix reconciliation =="
check_if "docs/env-matrix-reconciliation.md" "P0 record mentions all four contract checks (doc-presence gate)" bash -c \
    'grep -q FLEET_NATS_NAME docs/env-matrix-reconciliation.md && grep -qi allow_bots docs/env-matrix-reconciliation.md && grep -q FLEET_ROLE docs/env-matrix-reconciliation.md && grep -qi collision docs/env-matrix-reconciliation.md'
check_if "docs/env-matrix-reconciliation.md" "P0 record present + #464 cross-linked (doc-presence gate, not verification)" bash -c \
    'grep -qE "(rtech-hermes.*#464|#464.*rtech-hermes)" docs/env-matrix-reconciliation.md && grep -qiE "sign.?off" docs/env-matrix-reconciliation.md'
check_if "docs/env-matrix-reconciliation.md" "P0 no secret values in the record" bash -c \
    '! grep -rnE "(discord\.com/api/webhooks|xoxb-|Bearer [A-Za-z0-9])" docs/env-matrix-reconciliation.md'

echo "== Phase 1 (#22): JSONL coordination trace =="
check_pytest "packages/fleet-orchestrator/.venv/bin/python" \
    "P1 trace tests (off=no-file, on=valid sanitized JSONL)" \
    "packages/fleet-orchestrator" "trace"
check_if "packages/fleet-orchestrator/src/fleet_orchestrator/trace.py" "P1 flag name matches docs<->code" bash -c \
    'grep -q FLEET_TRACE_PATH docs/logging.md && grep -q FLEET_TRACE_PATH packages/fleet-orchestrator/src/fleet_orchestrator/trace.py'
if [ -f "packages/fleet-orchestrator/src/fleet_orchestrator/trace.py" ]; then
    check "P1 trace record fields are the closed sanctioned set" \
        bash -c \
        "cd packages/fleet-orchestrator && uv run --extra dev python -c \"from fleet_orchestrator.trace import TraceRecord; assert set(TraceRecord.__dataclass_fields__) == {'ts','task_id','state','event','action','target','subject'}\""
else
    printf 'SKIP  P1 trace record closed-set check (not built)\n'; skip=$((skip + 1))
fi
check_if "packages/fleet-orchestrator/src/fleet_orchestrator/trace.py" "P1 no payload/content field in trace records" \
    no_banned_words packages/fleet-orchestrator/src/fleet_orchestrator/trace.py "\\b(payload|content|text)\\b"

echo "== Phase 2 (#26): heartbeat + liveness =="
check_pytest "packages/fleet-subscriber/.venv/bin/python" \
    "P2 heartbeat emitter tests" "packages/fleet-subscriber" "heartbeat"
check_pytest "packages/fleet-orchestrator/.venv/bin/python" \
    "P2 liveness/staleness tests" "packages/fleet-orchestrator" "liveness"
check_pytest "packages/fleet-orchestrator/.venv/bin/python" \
    "P2 cross-package wire-contract (real emitter + real consumer, one bus)" \
    "packages/fleet-orchestrator" "heartbeat and contract"
check_if "packages/fleet-subscriber/src/fleet_subscriber/heartbeat.py" \
    "P2 contract test imports the REAL emitter (no same-named stub)" bash -c \
    'grep -rqE "from fleet_subscriber(\\.heartbeat)? import" packages/fleet-orchestrator/tests/'
check_if "packages/fleet-subscriber/src/fleet_subscriber/heartbeat.py" "P2 heartbeat payload identity-only" \
    no_banned_words packages/fleet-subscriber/src/fleet_subscriber/heartbeat.py "payload\\[|\\b(content|text|member)\\b"

echo "== Phase 3 (#25): delivery semantics =="
check_pytest "packages/fleet-subscriber/.venv/bin/python" \
    "P3 dedup + dead-letter tests" "packages/fleet-subscriber" "dedup or dead_letter"
check_pytest "packages/fleet-orchestrator/.venv/bin/python" \
    "P3 agent-offline timeout tests" "packages/fleet-orchestrator" "offline or timeout"
check_pytest "packages/fleet-nats/.venv/bin/python" \
    "P3 reconnect/resubscribe tests (pass-or-skip without nats-server)" \
    "packages/fleet-nats" "reconnect"
# C5/#114: architecture.md now states honest delivery semantics (at-most-once
# transport + idempotent consumers); the old "at-most-once" grep was the exact
# overclaim C4/C5 corrected, so prove the corrected contract instead: durable by
# default AND the fail-loud/no-silent-core-fallback guarantee that replaced it,
# without weakening the idempotent/dead-letter/offline acceptance.
check_if "packages/fleet-subscriber/src/fleet_subscriber/dedup.py" "P3 delivery-semantics contract documented" bash -c \
    'grep -qi "delivery semantics" docs/architecture.md && grep -qi "durable by default" docs/architecture.md && grep -qi "no silent core fallback" docs/architecture.md && grep -qi "idempotent" docs/architecture.md && grep -qiE "dead.letter" docs/architecture.md && grep -qi offline docs/architecture.md'

echo "== Phase 4 (#27): token-cost feed contract =="
check_pytest "packages/fleet-nats/.venv/bin/python" \
    "P4 token-cost wire round-trip + strict parse" "packages/fleet-nats" "token_cost"
check_pytest "packages/fleet-orchestrator/.venv/bin/python" \
    "P4 consumer sums per-task cost from feed" "packages/fleet-orchestrator" "sum_task_cost or costfeed"
check_if "packages/fleet-nats/src/fleet_nats/tokencost.py" "P4 contract documented both sides + #457 cross-link" bash -c \
    'grep -q token_cost docs/architecture.md && grep -q token_cost docs/consumers.md && grep -qE "#457" docs/architecture.md'
check_if "packages/fleet-nats/src/fleet_nats/tokencost.py" "P4 schema carries counts, never content" \
    no_banned_words packages/fleet-nats/src/fleet_nats/tokencost.py "\\b(prompt_text|completion_text|content)\\b"

echo "== Phase 5 (#13): governor + Nagatha narrates =="
check_pytest "packages/fleet-orchestrator/.venv/bin/python" \
    "P5 budget breach -> forced surface + pause" "packages/fleet-orchestrator" "governor"
check_pytest "packages/fleet-orchestrator/.venv/bin/python" \
    "P5 manager-only narration + worker silence" "packages/fleet-orchestrator" "narrat"
check_if "packages/fleet-orchestrator/src/fleet_orchestrator/governor.py" "P5 narration never interpolates payload prose" \
    no_banned_words packages/fleet-orchestrator/src/fleet_orchestrator/governor.py "payload\\[|\\b(reply_text|member)\\b"
check_if "packages/fleet-orchestrator/src/fleet_orchestrator/governor.py" "P5 runbook: production unit env + resume procedure" \
    runbook_has_production_unit_env_and_resume docs/runbook.md
check "P5 verifier rejects production env outside fenced service unit" rejects_service_env_outside_unit

echo "== Phase 6 (#17): scorekeeper + decider =="
check_pytest "packages/fleet-orchestrator/.venv/bin/python" \
    "P6 score/circlejerk/decider tests" "packages/fleet-orchestrator" "score or circlejerk or decider"
check_if "packages/fleet-orchestrator/src/fleet_orchestrator/score.py" "P6 turn-signal contract documented + #442 cross-link" bash -c \
    'grep -q fact_pull docs/architecture.md && grep -qE "#442" docs/architecture.md'
check "P6 no LLM in the control path" bash -c \
    '! grep -rnE "^[[:space:]]*(import|from)[[:space:]]+(openai|anthropic|litellm)\\b" packages/fleet-orchestrator/src 2>/dev/null'

echo "== Phase 7 (#14): dashboard park gate =="
check_if "packages/fleet-orchestrator/src/fleet_orchestrator/score.py" "P7 readiness record documented (after P6)" bash -c \
    'grep -qi "dashboard readiness" docs/architecture.md && grep -qi parked docs/architecture.md'
check "P7 park held: no dashboard built" bash -c \
    '! test -d packages/fleet-dashboard'
# Network-bound: gh may be unauthenticated/offline — SKIP, never a false verdict.
if gh issue view 14 --json state -q .state >/dev/null 2>&1; then
    # shellcheck disable=SC2016
    check "P7 #14 still OPEN (parked, not closed-as-built)" bash -c \
        'test "$(gh issue view 14 --json state -q .state)" = "OPEN"'
else
    echo "SKIP  P7 #14 still OPEN (gh unreachable/unauthenticated)"
    skip=$((skip + 1))
fi

echo "== Cross-cutting: package gates =="
for p in fleet-nats fleet-orchestrator fleet-subscriber; do
    d="packages/$p"
    if [ ! -d "$d/src" ]; then
        printf 'SKIP  %s four gates (not built: %s/src)\n' "$p" "$d"
        skip=$((skip + 1))
        continue
    fi
    check "$p four gates (ruff/format/mypy/pytest on src+tests)" package_gate "$p"
done

echo "== Cross-cutting: hygiene greps =="
# fleet_nats/subjects.py IS the sanctioned builder — the one legitimate home for
# these f-strings — so it is excluded, as is registry.py (the sanctioned catalog
# of subject TEMPLATES, locked to the builders by test_registry). Everything
# else interpolating {env}.<domain>. or quoting "dev./prod." is a violation.
# shellcheck disable=SC2046
check "no hand-formatted subjects" \
    no_python_match "([\"\047](dev|prod)|\\{[a-zA-Z_.]*env\\})\\.(agent|direct|human|kboard)\\." \
    $(package_source_files_except ! -name subjects.py ! -name registry.py)
# shellcheck disable=SC2016
check "no silent excepts" bash -c \
    'no_silent_except_pass $(find packages -path "*/src/*.py" -print)'
# Scan all tracked production files under packages/ and docs/. Tests, caches,
# venvs, and the exact redaction-pattern catalog are excluded because they
# intentionally contain secret-shaped fixtures or generated artifacts.
check "no secrets/webhooks in code+docs" bash -c \
    'grep_tracked_production_secrets'
# Injected-clock invariant on the new deterministic modules (only the ones built so far).
clock_files=""
for f in trace liveness governor score; do
    p="packages/fleet-orchestrator/src/fleet_orchestrator/$f.py"
    [ -f "$p" ] && clock_files="$clock_files $p"
done
if [ -n "$clock_files" ]; then
    # shellcheck disable=SC2086
    check "no time/random/datetime imports in new deterministic modules" bash -c \
        "! grep -rnE '^[[:space:]]*(import|from) (time|random|datetime)\\b' $clock_files"
else
    echo "SKIP  no time/random at import (no new deterministic modules built yet)"
    skip=$((skip + 1))
fi

echo
printf 'SUMMARY: %d pass, %d fail, %d skip\n' "$pass" "$fail" "$skip"
if [ "$STRICT" = "1" ]; then
    [ "$fail" -eq 0 ] && [ "$skip" -eq 0 ]
else
    [ "$fail" -eq 0 ]
fi
