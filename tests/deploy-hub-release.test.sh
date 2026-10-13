#!/usr/bin/env bash
# Regression for manual release deployment; all disposable state stays in the
# configured fleet-bus temp workspace, never /tmp.
set -euo pipefail

repo_root="$(cd "$(dirname "$0")/.." && pwd)"
subject="$repo_root/scripts/deploy-hub-release.sh"
verifier="$repo_root/scripts/verify-hub-release.py"
request_workflow="$repo_root/.github/workflows/deploy-request.yml"
deploy_workflow="$repo_root/.github/workflows/deploy.yml"
runbook="$repo_root/docs/runbook.md"
test_root="${FLEET_BUS_TEST_TMP:-$HOME/.cache/fleet-bus-tests/deploy-hub-release-tests}"
runner_temp="${RUNNER_TEMP:-}"
case "$test_root" in
    "$HOME"/.cache/fleet-bus-tests/*) ;;
    "$runner_temp"/fleet-bus/*) [ "${FLEET_BUS_TEST_ALLOW_RUNNER_TEMP:-0}" = 1 ] ;;
    *) echo "FAIL: FLEET_BUS_TEST_TMP must be inside the configured fleet-bus temp workspace"; exit 1 ;;
esac
mkdir -p "$test_root/_archive"
work="$test_root/run-$$"
home="$work/home"
fake_bin="$work/bin"
real_mv="$(command -v mv)"
real_uv="$(command -v uv)"
trusted_python="$(command -v python3)"
old_sha="1111111111111111111111111111111111111111"
new_sha="2222222222222222222222222222222222222222"
retry_sha="3333333333333333333333333333333333333333"
restart_sha="4444444444444444444444444444444444444444"
source_venv="$repo_root/packages/fleet-core/.venv"
source_venv_fixture="$source_venv/deploy-hub-release-regression-$$"
source_venv_created=0

cleanup() {
    if [ "$source_venv_created" -eq 1 ] && [ -d "$source_venv" ]; then
        mv "$source_venv" "$work/source-venv-fixture" 2>/dev/null || true
    elif [ -d "$source_venv_fixture" ]; then
        mv "$source_venv_fixture" "$work/source-venv-fixture" 2>/dev/null || true
    fi
    chmod -R u+w "$work" 2>/dev/null || true
    mv "$work" "$test_root/_archive/run-$(date -u +%Y%m%dT%H%M%SZ)-$$" 2>/dev/null || true
}
trap cleanup EXIT

# Workflow contract: an editable dispatch ref can only reach GitHub-hosted
# validation. The trusted default-branch workflow_run continuation is the sole
# path to the self-hosted runner and binds checkout/deploy to the request SHA.
grep -Fx '  workflow_dispatch:' "$request_workflow" >/dev/null
grep -Fx '    runs-on: ubuntu-latest' "$request_workflow" >/dev/null
grep -F "test \"\$SELECTED_REF\" = refs/heads/main" "$request_workflow" >/dev/null
if grep -F 'self-hosted' "$request_workflow" >/dev/null; then
    echo "FAIL: branch-editable dispatch workflow targets a self-hosted runner"
    exit 1
fi
grep -Fx '  workflow_run:' "$deploy_workflow" >/dev/null
grep -Fx '    workflows: [Request fleet hub deploy]' "$deploy_workflow" >/dev/null
grep -Fx '    runs-on: [self-hosted, fleet-bus]' "$deploy_workflow" >/dev/null
grep -F "github.event.workflow_run.conclusion == 'success'" "$deploy_workflow" >/dev/null
grep -F "github.event.workflow_run.head_branch == 'main'" "$deploy_workflow" >/dev/null
grep -F 'github.event.workflow_run.head_repository.full_name == github.repository' "$deploy_workflow" >/dev/null
grep -Fx '      name: fleet-bus-production' "$deploy_workflow" >/dev/null
grep -F "ref: \${{ github.event.workflow_run.head_sha }}" "$deploy_workflow" >/dev/null
grep -F "DEPLOY_SHA: \${{ github.event.workflow_run.head_sha }}" "$deploy_workflow" >/dev/null
grep -Fx '  contents: read' "$deploy_workflow" >/dev/null
grep -Eq 'uses: actions/checkout@[0-9a-f]{40}' "$deploy_workflow"
if grep -Eq '^    if: github.ref == ' "$request_workflow"; then
    echo "FAIL: dispatch validation must fail rather than skip"
    exit 1
fi

mkdir -p "$home/.venv/bin" "$fake_bin"
printf '#!/usr/bin/env bash\nexit 0\n' >"$home/.venv/bin/fleet-orchestrator"
chmod +x "$home/.venv/bin/fleet-orchestrator"
ln -s "$home" "$home/current" # the only accepted receipt-free legacy bootstrap

cat >"$fake_bin/sudo" <<'EOF'
#!/usr/bin/env bash
exec "$@"
EOF
cat >"$fake_bin/systemctl" <<'EOF'
#!/usr/bin/env bash
case "$1" in
    show)
        expected="${FLEET_BUS_HOME}/current/.venv/bin/fleet-orchestrator"
        case "${TEST_EXECSTART_MODE:-good}" in
            good) printf '{ path=%s ; argv[]=%s ; }\n' "$expected" "$expected" ;;
            substring) printf '{ path=%s-evil ; argv[]=%s ; }\n' "$expected" "$expected" ;;
            extra-argv) printf '{ path=%s ; argv[]=%s --unsafe ; }\n' "$expected" "$expected" ;;
            *) exit 2 ;;
        esac
        ;;
    restart)
        current="$(readlink "$FLEET_BUS_HOME/current")"
        printf 'restart %s\n' "$current" >>"$TEST_RESTART_LOG"
        [ "${TEST_FAIL_RESTART_TARGET:-}" != "$current" ] || exit 1
        ;;
    is-active) exit 0 ;;
    *) exit 2 ;;
esac
EOF
cat >"$fake_bin/curl" <<'EOF'
#!/usr/bin/env bash
current="$(readlink "$FLEET_BUS_HOME/current")"
printf '%s\n' "$current" >>"$TEST_CURL_LOG"
if [ "${TEST_FAIL_READY_TARGET:-}" = "$current" ]; then
    printf '%s\n' "$TEST_FAIL_READY_JSON"
    exit 0
fi
printf '%s\n' "$TEST_READY_JSON"
EOF
cat >"$fake_bin/git" <<'EOF'
#!/usr/bin/env bash
if [ "${1:-}" = '-C' ]; then
    source_root="$2"
    shift 2
else
    exit 2
fi
[ "$source_root" = "$TEST_GIT_SOURCE" ] || exit 2
case "${1:-}" in
    rev-parse)
        case "${2:-}" in
            --is-inside-work-tree) printf '%s\n' true ;;
            HEAD) printf '%s\n' "${TEST_GIT_HEAD:-$DEPLOY_SHA}" ;;
            *) exit 2 ;;
        esac
        ;;
    status)
        if [ "${TEST_GIT_DIRTY:-0}" = 1 ]; then
            printf '%s\n' ' M dirty-file'
        fi
        ;;
    *) exit 2 ;;
esac
EOF
cat >"$fake_bin/mv" <<'EOF'
#!/usr/bin/env bash
last="${!#}"
if [ "${TEST_FAIL_RECEIPT:-0}" = 1 ] && [ "$last" = "$FLEET_BUS_HOME/DEPLOYED_SHA" ]; then
    exit 1
fi
exec "$TEST_REAL_MV" "$@"
EOF
cat >"$fake_bin/uv" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail

python_path=""
installing_release_packages=0
previous=""
for argument in "$@"; do
    if [ "$previous" = --python ]; then
        python_path="$argument"
    fi
    case "$argument" in
        */src/packages/fleet-orchestrator) installing_release_packages=1 ;;
    esac
    previous="$argument"
done

"$TEST_REAL_UV" "$@"

if [ -n "${TEST_CANDIDATE_STARTUP_MARKER:-}" ] && [ "$installing_release_packages" -eq 1 ]; then
    venv_root="$(dirname "$(dirname "$python_path")")"
    site_packages="$(find "$venv_root/lib" -type d -path '*/site-packages' -print -quit)"
    [ -n "$site_packages" ] || exit 1
    cat >"$site_packages/sitecustomize.py" <<'PY'
from pathlib import Path
import os

marker = os.environ.get("TEST_CANDIDATE_STARTUP_MARKER")
if marker:
    Path(marker).write_text("candidate interpreter started\n")
PY
fi
EOF
chmod +x "$fake_bin"/*

run_deploy() {
    PATH="$fake_bin:$PATH" \
    FLEET_BUS_HOME="$home" \
    FLEET_BUS_SRC="$repo_root" \
    DEPLOY_SHA="$1" \
    FLEET_NATS_NAME='hub.*[literal]' \
    FLEET_READY_NAME='hub.*[literal]' \
    FLEET_READY_ATTEMPTS=1 \
    FLEET_READY_SUCCESSES=1 \
    FLEET_READY_DELAY_SECONDS=0 \
    TEST_RESTART_LOG="$work/restarts.log" \
    TEST_CURL_LOG="$work/curl.log" \
    TEST_GIT_SOURCE="$repo_root" \
    TEST_REAL_MV="$real_mv" \
    TEST_REAL_UV="$real_uv" \
    FLEET_HUB_VERIFIER_PYTHON="${FLEET_HUB_VERIFIER_PYTHON_OVERRIDE:-$trusted_python}" \
    TEST_CANDIDATE_STARTUP_MARKER="${TEST_CANDIDATE_STARTUP_MARKER:-}" \
    TEST_READY_JSON="${TEST_READY_JSON:?set TEST_READY_JSON before run_deploy}" \
    bash "$subject"
}

run_break_glass_verifier() {
    local release="$1"
    local sha="$2"
    PYTHONDONTWRITEBYTECODE=1 "$trusted_python" "$verifier" \
        --source "$repo_root" --release "$release" --sha "$sha"
}

assert_break_glass_rejects() {
    local label="$1"
    local release="$2"
    local sha="$3"
    local expected_error="$4"
    local status
    set +e
    run_break_glass_verifier "$release" "$sha" >"$work/break-glass-$label.log" 2>&1
    status=$?
    set -e
    [ "$status" -ne 0 ] || { echo "FAIL: break-glass verifier accepted $label release"; exit 1; }
    grep -F "$expected_error" "$work/break-glass-$label.log" >/dev/null
}

assert_deploy_rebuilds_rejected_release() {
    local label="$1"
    local sha="$2"
    TEST_READY_JSON='{"connections":[{"name":"hub.*[literal]"}]}' \
    run_deploy "$sha" >"$work/deploy-$label.log" 2>&1
    grep -F 'quarantined unsealed release' "$work/deploy-$label.log" >/dev/null
    if grep -F 'reusing verified sealed release' "$work/deploy-$label.log" >/dev/null; then
        echo "FAIL: deploy reused hostile $label release"
        exit 1
    fi
}

reseal_release() {
    local release="$1"
    PYTHONDONTWRITEBYTECODE=1 "$trusted_python" - "$release" <<'PY'
import hashlib
import json
import os
import stat
import sys
from pathlib import Path

release = Path(sys.argv[1])
manifest_path = release / "release-manifest.json"
manifest = json.loads(manifest_path.read_text())
files: dict[str, str] = {}
symlinks: dict[str, str] = {}
for path in sorted(release.rglob("*")):
    relative = path.relative_to(release).as_posix()
    mode = path.lstat().st_mode
    if stat.S_ISDIR(mode):
        continue
    # Match the real seal/verify (#208): derived CPython bytecode is not sealed.
    if "__pycache__" in path.parts or relative.endswith(".pyc"):
        continue
    if stat.S_ISLNK(mode):
        symlinks[relative] = os.readlink(path)
        continue
    if not stat.S_ISREG(mode):
        raise SystemExit(f"non-regular release entry: {relative}")
    if relative not in {"release-manifest.json", "release-seal"}:
        files[relative] = hashlib.sha256(path.read_bytes()).hexdigest()
manifest["files"] = files
manifest["symlinks"] = symlinks
manifest_path.write_text(json.dumps(manifest, sort_keys=True, separators=(",", ":")) + "\n")
(release / "release-seal").write_text(
    hashlib.sha256(manifest_path.read_bytes()).hexdigest() + "  release-manifest.json\n"
)
PY
}

# CI package gates create ignored package-local virtualenvs before deployment.
# The trusted source inventory must exclude the same .venv tree as rsync.
if [ ! -d "$source_venv" ]; then
    source_venv_created=1
fi
mkdir -p "$source_venv_fixture"
printf '%s\n' 'ignored package-local virtualenv file' >"$source_venv_fixture/ignored.py"

# The source must be the exact, clean selected checkout before any release work.
set +e
TEST_READY_JSON='{"connections":[{"name":"hub.*[literal]"}]}' \
TEST_GIT_HEAD="$old_sha" run_deploy "$retry_sha" >"$work/source-head.log" 2>&1
status=$?
set -e
[ "$status" -ne 0 ] || { echo "FAIL: mismatched source HEAD passed"; exit 1; }
grep -F 'FLEET_BUS_SRC HEAD does not match DEPLOY_SHA' "$work/source-head.log" >/dev/null

set +e
TEST_READY_JSON='{"connections":[{"name":"hub.*[literal]"}]}' \
TEST_GIT_DIRTY=1 run_deploy "$retry_sha" >"$work/source-dirty.log" 2>&1
status=$?
set -e
[ "$status" -ne 0 ] || { echo "FAIL: dirty source checkout passed"; exit 1; }
grep -F 'FLEET_BUS_SRC must be clean at DEPLOY_SHA' "$work/source-dirty.log" >/dev/null

# ExecStart fields are parsed as exact path and argv[0], not substrings.
for execstart_mode in substring extra-argv; do
    set +e
    TEST_READY_JSON='{"connections":[{"name":"hub.*[literal]"}]}' \
    TEST_EXECSTART_MODE="$execstart_mode" run_deploy "$new_sha" >"$work/execstart-$execstart_mode.log" 2>&1
    status=$?
    set -e
    [ "$status" -ne 0 ] || { echo "FAIL: ExecStart $execstart_mode bypass passed"; exit 1; }
    grep -F 'effective ExecStart path and argv[0] must exactly equal' "$work/execstart-$execstart_mode.log" >/dev/null
done

# Exact JSON readiness rejects missing, malformed, and regex-lookalike names.
set +e
TEST_READY_JSON='{"connections":[{"name":"hub.*[literal]"}]}' \
TEST_FAIL_READY_TARGET="$home/releases/$new_sha" \
TEST_FAIL_READY_JSON='{"connections":[{"name":"hubXliteral"}]}' \
TEST_CANDIDATE_STARTUP_MARKER="$work/candidate-interpreter-started" \
run_deploy "$new_sha" >"$work/not-ready.log" 2>&1
status=$?
set -e
[ "$status" -ne 0 ] || { echo "FAIL: regex-lookalike readiness passed"; exit 1; }
[ "$(readlink "$home/current")" = "$home" ] || { echo "FAIL: legacy rollback target was not restored"; exit 1; }
[ ! -e "$home/DEPLOYED_SHA" ] || { echo "FAIL: legacy rollback created a receipt"; exit 1; }
[ -f "$home/releases/$new_sha/release-manifest.json" ] || { echo "FAIL: sealed failed release missing manifest"; exit 1; }
[ ! -e "$work/candidate-interpreter-started" ] || { echo "FAIL: sealing executed candidate startup code"; exit 1; }

# The failed but sealed release is reusable only after manifest/content checks.
TEST_READY_JSON='{"connections":[{"name":"hub.*[literal]"}]}' \
TEST_FAIL_READY_TARGET='' run_deploy "$new_sha" >"$work/success.log" 2>&1
[ "$(cat "$home/DEPLOYED_SHA")" = "$new_sha" ] || { echo "FAIL: receipt did not advance"; exit 1; }
grep -F "reusing verified sealed release $home/releases/$new_sha" "$work/success.log" >/dev/null
grep -F '"fleet-core":"fleet_core"' "$home/releases/$new_sha/release-manifest.json" >/dev/null
grep -F '"fleet-orchestrator":"fleet_orchestrator"' "$home/releases/$new_sha/release-manifest.json" >/dev/null
run_break_glass_verifier "$home/releases/$new_sha" "$new_sha"

# #208 regression: the running service writes fresh bytecode into the sealed
# venv on import (its unit does not set PYTHONDONTWRITEBYTECODE). Derived
# __pycache__/*.pyc is excluded from the seal, so a .pyc appearing (or changing)
# after sealing must NOT invalidate the release — otherwise the next deploy dies.
release="$home/releases/$new_sha"
site_packages="$(find "$release/.venv/lib" -type d -path '*/site-packages' -print -quit)"
[ -n "$site_packages" ] || { echo "FAIL: candidate site-packages missing for pyc test"; exit 1; }
chmod -R u+w "$release"
mkdir -p "$site_packages/fleet_nats/__pycache__"
printf 'simulated fresh bytecode from a running import\n' \
    >"$site_packages/fleet_nats/__pycache__/__init__.cpython-313.pyc"
printf 'top-level stray bytecode\n' >"$release/stray.pyc"
if ! run_break_glass_verifier "$release" "$new_sha" >"$work/pyc-after-seal.log" 2>&1; then
    echo "FAIL: post-seal bytecode invalidated the release (#208 regression)"
    cat "$work/pyc-after-seal.log"
    exit 1
fi
rm -f "$release/stray.pyc"
rm -rf "$site_packages/fleet_nats/__pycache__"

# A restart failure must return immediately even when restart_and_wait is called
# under ||; readiness for the failed target must never run and receipt stays old.
: >"$work/curl.log"
set +e
TEST_READY_JSON='{"connections":[{"name":"hub.*[literal]"}]}' \
TEST_FAIL_RESTART_TARGET="$home/releases/$restart_sha" \
run_deploy "$restart_sha" >"$work/restart-failed.log" 2>&1
status=$?
set -e
[ "$status" -ne 0 ] || { echo "FAIL: restart failure passed"; exit 1; }
grep -F 'restart failed for fleet-orchestrator' "$work/restart-failed.log" >/dev/null
[ "$(readlink "$home/current")" = "$home/releases/$new_sha" ] || { echo "FAIL: restart failure did not roll back"; exit 1; }
[ "$(cat "$home/DEPLOYED_SHA")" = "$new_sha" ] || { echo "FAIL: restart failure advanced receipt"; exit 1; }
if grep -F "$home/releases/$restart_sha" "$work/curl.log" >/dev/null; then
    echo "FAIL: readiness ran after restart failure"
    exit 1
fi

# Existing incomplete directories are quarantined and rebuilt under the lock.
mkdir -p "$home/releases/$retry_sha"
printf '%s\n' "$retry_sha" >"$home/releases/$retry_sha/.fleet-release-sha"
set +e
TEST_READY_JSON='{"connections":[{"name":"hub.*[literal]"}]}' \
TEST_FAIL_READY_TARGET="$home/releases/$retry_sha" \
TEST_FAIL_READY_JSON='not-json' run_deploy "$retry_sha" >"$work/retry.log" 2>&1
status=$?
set -e
[ "$status" -ne 0 ] || { echo "FAIL: malformed readiness JSON passed"; exit 1; }
grep -F 'quarantined unsealed release' "$work/retry.log" >/dev/null
[ -f "$home/releases/$retry_sha/release-seal" ] || { echo "FAIL: incomplete release was not rebuilt and sealed"; exit 1; }
[ "$(cat "$home/DEPLOYED_SHA")" = "$new_sha" ] || { echo "FAIL: failed activation changed old receipt"; exit 1; }

# A bad prior receipt fails before it can activate another release.
chmod u+w "$home/DEPLOYED_SHA"
printf '%s\n' "$old_sha" >"$home/DEPLOYED_SHA"
set +e
TEST_READY_JSON='{"connections":[{"name":"hub.*[literal]"}]}' \
TEST_FAIL_READY_TARGET='' run_deploy "$retry_sha" >"$work/mismatch.log" 2>&1
status=$?
set -e
[ "$status" -ne 0 ] || { echo "FAIL: mismatched managed receipt passed"; exit 1; }
grep -F 'DEPLOYED_SHA does not match managed current' "$work/mismatch.log" >/dev/null
printf '%s\n' "$new_sha" >"$home/DEPLOYED_SHA"
chmod 0444 "$home/DEPLOYED_SHA"

# Both deployment reuse and the break-glass path invoke the same complete
# verifier. A valid seal only authenticates its manifest, so every hostile
# self-sealed release must fail before either caller can activate it.
release="$home/releases/$restart_sha"
chmod -R u+w "$release"
"$release/.venv/bin/python" - "$release" <<'PY'
import hashlib
import json
import sys
from pathlib import Path

release = Path(sys.argv[1])
manifest_path = release / "release-manifest.json"
manifest = json.loads(manifest_path.read_text())
manifest["files"] = {}
manifest_path.write_text(json.dumps(manifest, sort_keys=True, separators=(",", ":")) + "\n")
(release / "release-seal").write_text(hashlib.sha256(manifest_path.read_bytes()).hexdigest() + "  release-manifest.json\n")
PY
assert_break_glass_rejects empty-files "$release" "$restart_sha" 'manifest file set is empty or invalid'
assert_deploy_rebuilds_rejected_release empty-files "$restart_sha"

release="$home/releases/$new_sha"
chmod -R u+w "$release"
"$release/.venv/bin/python" - "$release" <<'PY'
import hashlib
import json
import sys
from pathlib import Path

release = Path(sys.argv[1])
manifest_path = release / "release-manifest.json"
manifest = json.loads(manifest_path.read_text())
del manifest["files"]["src/uv.lock"]
manifest_path.write_text(json.dumps(manifest, sort_keys=True, separators=(",", ":")) + "\n")
(release / "release-seal").write_text(hashlib.sha256(manifest_path.read_bytes()).hexdigest() + "  release-manifest.json\n")
PY
assert_break_glass_rejects partial-files "$release" "$new_sha" 'manifest file set is incomplete or content mismatched'
assert_deploy_rebuilds_rejected_release partial-files "$new_sha"

release="$home/releases/$restart_sha"
chmod -R u+w "$release"
printf '%s\n' hostile >"$release/unlisted-hostile-file"
assert_break_glass_rejects unlisted-file "$release" "$restart_sha" 'manifest file set is incomplete or content mismatched'
assert_deploy_rebuilds_rejected_release unlisted-file "$restart_sha"

release="$home/releases/$new_sha"
chmod -R u+w "$release"
"$release/.venv/bin/python" - "$release" <<'PY'
import hashlib
import json
import sys
from pathlib import Path

release = Path(sys.argv[1])
manifest_path = release / "release-manifest.json"
manifest = json.loads(manifest_path.read_text())
manifest["distributions"]["fleet-nats"] = "wrong_module"
manifest_path.write_text(json.dumps(manifest, sort_keys=True, separators=(",", ":")) + "\n")
(release / "release-seal").write_text(hashlib.sha256(manifest_path.read_bytes()).hexdigest() + "  release-manifest.json\n")
PY
assert_break_glass_rejects wrong-distribution "$release" "$new_sha" 'manifest distribution set is incomplete or mismatched'
assert_deploy_rebuilds_rejected_release wrong-distribution "$new_sha"

# Trust decisions must never execute a candidate interpreter. Repointing it to
# true, adding an unlisted symlink, or adding a non-regular entry rejects both
# the break-glass and deploy-reuse paths.
release="$home/releases/$restart_sha"
chmod -R u+w "$release"
ln -s /usr/bin/true "$release/unlisted-hostile-link"
assert_break_glass_rejects unlisted-symlink "$release" "$restart_sha" 'manifest symlink set is incomplete or mismatched'
assert_deploy_rebuilds_rejected_release unlisted-symlink "$restart_sha"

release="$home/releases/$new_sha"
chmod -R u+w "$release"
mkfifo "$release/unlisted-hostile-fifo"
assert_break_glass_rejects non-regular "$release" "$new_sha" 'release entry is not regular or a documented symlink'
assert_deploy_rebuilds_rejected_release non-regular "$new_sha"

release="$home/releases/$restart_sha"
chmod -R u+w "$release"
unlink "$release/.venv/bin/python"
ln -s /usr/bin/true "$release/.venv/bin/python"
assert_break_glass_rejects interpreter-repoint "$release" "$restart_sha" 'manifest symlink set is incomplete or mismatched'
assert_deploy_rebuilds_rejected_release interpreter-repoint "$restart_sha"

# Required distributions are proven from the candidate site-packages tree, not
# from the self-declared manifest. Both callers reject a hostile reseal that
# removes the required metadata or installed top-level package.
release="$home/releases/$new_sha"
chmod -R u+w "$release"
site_packages="$(find "$release/.venv/lib" -type d -path '*/site-packages' -print -quit)"
[ -n "$site_packages" ] || { echo "FAIL: candidate site-packages missing"; exit 1; }
metadata_path="$(find "$site_packages" -maxdepth 2 -type f -path '*/fleet_nats-*.dist-info/METADATA' -print -quit)"
[ -n "$metadata_path" ] || { echo "FAIL: fleet-nats metadata missing"; exit 1; }
mv "$metadata_path" "$metadata_path.disabled"
reseal_release "$release"
assert_break_glass_rejects missing-dist-metadata "$release" "$new_sha" 'candidate distribution metadata is missing or not regular: fleet_nats-'
assert_deploy_rebuilds_rejected_release missing-dist-metadata "$new_sha"

release="$home/releases/$restart_sha"
chmod -R u+w "$release"
site_packages="$(find "$release/.venv/lib" -type d -path '*/site-packages' -print -quit)"
[ -d "$site_packages/fleet_nats" ] || { echo "FAIL: fleet-nats module missing"; exit 1; }
mv "$site_packages/fleet_nats" "$site_packages/fleet_nats.disabled"
reseal_release "$release"
assert_break_glass_rejects missing-module "$release" "$restart_sha" 'candidate module path missing or not regular: fleet-nats'
assert_deploy_rebuilds_rejected_release missing-module "$restart_sha"

# The trusted verifier's canonical path cannot point inside managed releases.
cp "$(realpath "$trusted_python")" "$home/releases/trusted-python"
set +e
TEST_READY_JSON='{"connections":[{"name":"hub.*[literal]"}]}' \
FLEET_HUB_VERIFIER_PYTHON_OVERRIDE="$home/releases/trusted-python" \
run_deploy "$retry_sha" >"$work/trusted-python-inside-release.log" 2>&1
status=$?
set -e
[ "$status" -ne 0 ] || { echo "FAIL: release-local trusted verifier Python passed"; exit 1; }
grep -F 'trusted verifier Python must be outside managed releases' "$work/trusted-python-inside-release.log" >/dev/null

# Static guards cover the remaining rollback, source, and runbook contracts
# without a host.
grep -F 'effective ExecStart path and argv[0] must exactly equal' "$subject" >/dev/null
grep -F 'receipt invalidated' "$subject" >/dev/null
grep -F 'FLEET_READY_NAME must exactly match FLEET_NATS_NAME' "$subject" >/dev/null
grep -F 'another hub deployment holds' "$subject" >/dev/null
grep -F 'jq -e --arg expected' "$subject" >/dev/null
grep -F 'verify-hub-release.py' "$subject" >/dev/null
grep -F 'FLEET_HUB_VERIFIER_PYTHON' "$subject" >/dev/null
grep -F "\"\$TRUSTED_VERIFIER_PYTHON\" - \"\$release_dir\" \"\$DEPLOY_SHA\"" "$subject" >/dev/null
if grep -F "\"\$release_dir/.venv/bin/python\" - \"\$release_dir\" \"\$DEPLOY_SHA\"" "$subject" >/dev/null; then
    echo "FAIL: sealing executed the untrusted candidate interpreter"
    exit 1
fi
grep -F 'managed_releases_realpath' "$subject" >/dev/null
grep -F 'FLEET_BUS_SRC must be clean at DEPLOY_SHA' "$subject" >/dev/null
test -f "$verifier"
grep -F 'set -euo pipefail' "$runbook" >/dev/null
grep -F 'legacy fleet-orchestrator venv is required' "$runbook" >/dev/null
grep -F 'break-glass failed after activation; DEPLOYED_SHA invalidated' "$runbook" >/dev/null
grep -F 'verify-hub-release.py' "$runbook" >/dev/null
grep -F 'trusted_python=/usr/bin/python3' "$runbook" >/dev/null
grep -F 'installed distribution metadata' "$runbook" >/dev/null
grep -F "\"\$home/releases\" \"\$home/incoming\" \"\$home/sources\"" "$runbook" >/dev/null

echo "deploy-hub-release.test: PASS — dispatch, immutable lock/release, exact readiness, rollback, receipt, and retry contracts"
