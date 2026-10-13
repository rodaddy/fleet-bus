#!/usr/bin/env bash
# Manual, immutable release-directory deploy for the fleet orchestrator hub (#157).
set -euo pipefail

FLEET_BUS_HOME="${FLEET_BUS_HOME:-/opt/fleet-bus}"
FLEET_BUS_SRC="${FLEET_BUS_SRC:-}"
DEPLOY_SHA="${DEPLOY_SHA:-}"
SERVICE_NAME="${FLEET_SERVICE_NAME:-fleet-orchestrator}"
READY_URL="${FLEET_READY_URL:-http://127.0.0.1:8223/connz}"
FLEET_NATS_NAME="${FLEET_NATS_NAME:-fleet-orchestrator-hub}"
READY_NAME="${FLEET_READY_NAME:-$FLEET_NATS_NAME}"
READY_ATTEMPTS="${FLEET_READY_ATTEMPTS:-30}"
READY_DELAY_SECONDS="${FLEET_READY_DELAY_SECONDS:-2}"
READY_SUCCESSES="${FLEET_READY_SUCCESSES:-2}"
TRUSTED_VERIFIER_PYTHON="${FLEET_HUB_VERIFIER_PYTHON:-/usr/bin/python3}"

releases_dir="$FLEET_BUS_HOME/releases"
release_dir="$releases_dir/$DEPLOY_SHA"
current_link="$FLEET_BUS_HOME/current"
receipt="$FLEET_BUS_HOME/DEPLOYED_SHA"
lock_file="$FLEET_BUS_HOME/.deploy.lock"
next_link="$FLEET_BUS_HOME/.current-next-$$"
receipt_tmp="$FLEET_BUS_HOME/.DEPLOYED_SHA-$$"

created_release=0
release_complete=0
activated=0
succeeded=0
prior_target=""
prior_receipt=""
prior_receipt_present=0
prior_state=""

log() { printf 'deploy-hub-release | %s\n' "$*"; }
die() { log "ERROR: $*" >&2; exit 1; }

validate_inputs() {
    [[ "$DEPLOY_SHA" =~ ^[0-9a-f]{40}$ ]] || die "DEPLOY_SHA must be a full lowercase commit SHA"
    [ -n "$FLEET_BUS_SRC" ] || die "FLEET_BUS_SRC is required"
    command -v git >/dev/null 2>&1 || die "git is required to verify FLEET_BUS_SRC"
    command -v realpath >/dev/null 2>&1 || die "realpath is required to establish the trusted verification boundary"
    FLEET_BUS_SRC="$(realpath "$FLEET_BUS_SRC")" || die "could not resolve FLEET_BUS_SRC"
    local trusted_verifier_realpath managed_releases_realpath
    managed_releases_realpath="$(realpath -m "$releases_dir")" || \
        die "could not resolve managed releases directory"
    trusted_verifier_realpath="$(realpath "$TRUSTED_VERIFIER_PYTHON")" || \
        die "could not resolve trusted verifier Python"
    case "$FLEET_BUS_SRC" in
        "$managed_releases_realpath"|"$managed_releases_realpath"/*)
            die "FLEET_BUS_SRC must be outside managed releases"
            ;;
    esac
    [ "$(git -C "$FLEET_BUS_SRC" rev-parse --is-inside-work-tree 2>/dev/null)" = "true" ] || \
        die "FLEET_BUS_SRC must be a Git checkout"
    local source_head source_status
    source_head="$(git -C "$FLEET_BUS_SRC" rev-parse HEAD 2>/dev/null)" || die "could not read FLEET_BUS_SRC HEAD"
    [ "$source_head" = "$DEPLOY_SHA" ] || die "FLEET_BUS_SRC HEAD does not match DEPLOY_SHA"
    source_status="$(git -C "$FLEET_BUS_SRC" status --porcelain=v1 --untracked-files=all 2>/dev/null)" || \
        die "could not verify FLEET_BUS_SRC cleanliness"
    [ -z "$source_status" ] || die "FLEET_BUS_SRC must be clean at DEPLOY_SHA"
    for package in fleet-core fleet-nats fleet-subscriber fleet-orchestrator; do
        [ -d "$FLEET_BUS_SRC/packages/$package" ] || die "source is missing packages/$package"
    done
    [ -f "$FLEET_BUS_SRC/pyproject.toml" ] || die "source is missing pyproject.toml"
    [ -f "$FLEET_BUS_SRC/uv.lock" ] || die "source is missing committed uv.lock"
    [ -f "$FLEET_BUS_SRC/scripts/verify-hub-release.py" ] || die "source is missing trusted release verifier"
    [ ! -L "$FLEET_BUS_SRC/scripts/verify-hub-release.py" ] || die "trusted release verifier must not be a symlink"
    [[ "$TRUSTED_VERIFIER_PYTHON" = /* ]] || die "FLEET_HUB_VERIFIER_PYTHON must be an absolute trusted operator path"
    [ -x "$TRUSTED_VERIFIER_PYTHON" ] || die "trusted verifier Python is not executable: $TRUSTED_VERIFIER_PYTHON"
    case "$trusted_verifier_realpath" in
        "$managed_releases_realpath"|"$managed_releases_realpath"/*)
            die "trusted verifier Python must be outside managed releases"
            ;;
    esac
    [[ "$READY_ATTEMPTS" =~ ^[1-9][0-9]*$ ]] || die "FLEET_READY_ATTEMPTS must be a positive integer"
    [[ "$READY_SUCCESSES" =~ ^[1-9][0-9]*$ ]] || die "FLEET_READY_SUCCESSES must be a positive integer"
    [ "$READY_SUCCESSES" -le "$READY_ATTEMPTS" ] || die "FLEET_READY_SUCCESSES cannot exceed FLEET_READY_ATTEMPTS"
    [[ "$READY_DELAY_SECONDS" =~ ^[0-9]+$ ]] || die "FLEET_READY_DELAY_SECONDS must be a non-negative integer"
    [[ "$SERVICE_NAME" =~ ^[A-Za-z0-9_.@-]+$ ]] || die "invalid service name"
    [ -n "$FLEET_NATS_NAME" ] || die "FLEET_NATS_NAME must not be empty"
    [ "$READY_NAME" = "$FLEET_NATS_NAME" ] || die "FLEET_READY_NAME must exactly match FLEET_NATS_NAME"
}

atomic_link() {
    local target="$1"
    unlink "$next_link" 2>/dev/null || true
    ln -s "$target" "$next_link"
    # -T prevents mv from following an existing current->directory symlink.
    mv -Tf "$next_link" "$current_link"
}

require_effective_execstart() {
    local expected="$FLEET_BUS_HOME/current/.venv/bin/fleet-orchestrator"
    local effective field path_value="" argv_value="" path_count=0 argv_count=0
    local -a fields
    effective="$(sudo systemctl show "$SERVICE_NAME" --property=ExecStart --value)"
    IFS=';' read -r -a fields <<<"$effective"
    for field in "${fields[@]}"; do
        # systemd separates command fields with semicolons and wraps each
        # command in braces. Trim only that structural whitespace/wrapping.
        field="$(printf '%s' "$field" | sed -E 's/^[[:space:]{]+//; s/[[:space:]}]+$//')"
        case "$field" in
            path=*)
                path_count=$((path_count + 1))
                path_value="${field#path=}"
                ;;
            argv\[\]=*)
                argv_count=$((argv_count + 1))
                argv_value="${field#argv[]=}"
                ;;
        esac
    done
    if [ "$path_count" -ne 1 ] || [ "$argv_count" -ne 1 ] || \
        [ "$path_value" != "$expected" ] || [ "$argv_value" != "$expected" ]; then
        die "effective ExecStart path and argv[0] must exactly equal $expected"
    fi
}

service_ready() {
    sudo systemctl is-active "$SERVICE_NAME" >/dev/null 2>&1 || return 1
    curl -fsS --connect-timeout 2 --max-time 5 "$READY_URL" |
        jq -e --arg expected "$READY_NAME" \
            '(.connections | type == "array") and any(.connections[]?; type == "object" and .name? == $expected)' \
            >/dev/null
}

wait_ready() {
    local attempt consecutive=0
    for ((attempt = 1; attempt <= READY_ATTEMPTS; attempt++)); do
        if service_ready; then
            consecutive=$((consecutive + 1))
            if [ "$consecutive" -ge "$READY_SUCCESSES" ]; then
                log "readiness proved service=$SERVICE_NAME connection=$READY_NAME consecutive=$consecutive attempt=$attempt"
                return 0
            fi
        else
            consecutive=0
        fi
        log "readiness pending attempt=$attempt/$READY_ATTEMPTS consecutive=$consecutive/$READY_SUCCESSES"
        sleep "$READY_DELAY_SECONDS"
    done
    return 1
}

restart_and_wait() {
    require_effective_execstart
    if ! sudo systemctl restart "$SERVICE_NAME"; then
        log "restart failed for $SERVICE_NAME" >&2
        return 1
    fi
    if ! wait_ready; then
        return 1
    fi
}

write_receipt() {
    local value="$1"
    printf '%s\n' "$value" >"$receipt_tmp"
    chmod 0444 "$receipt_tmp"
    mv -f "$receipt_tmp" "$receipt"
}

invalidate_receipt() {
    unlink "$receipt" 2>/dev/null || log "CRITICAL: could not invalidate $receipt" >&2
}

restore_receipt() {
    if [ "$prior_receipt_present" -eq 1 ]; then
        write_receipt "$prior_receipt"
    else
        invalidate_receipt
    fi
}

verify_release() {
    local candidate="$1"
    local expected_sha="$2"
    PYTHONDONTWRITEBYTECODE=1 "$TRUSTED_VERIFIER_PYTHON" \
        "$FLEET_BUS_SRC/scripts/verify-hub-release.py" \
        --source "$FLEET_BUS_SRC" --release "$candidate" --sha "$expected_sha"
}

seal_release() {
    PYTHONDONTWRITEBYTECODE=1 "$TRUSTED_VERIFIER_PYTHON" - "$release_dir" "$DEPLOY_SHA" <<'PY'
from __future__ import annotations

import hashlib
import json
import os
import stat
import sys
from pathlib import Path

release = Path(sys.argv[1]).resolve()
sha = sys.argv[2]
files: dict[str, str] = {}
symlinks: dict[str, str] = {}
for path in sorted(release.rglob("*")):
    relative = path.relative_to(release).as_posix()
    mode = path.lstat().st_mode
    if stat.S_ISDIR(mode):
        continue
    # Exclude derived CPython bytecode (__pycache__/*.pyc) from the sealed
    # manifest. The running orchestrator writes fresh .pyc into the venv on
    # import (its unit does not set PYTHONDONTWRITEBYTECODE), which would mutate
    # the sealed release out from under this manifest and fail the next deploy
    # (#208). Safe because the .py source IS sealed and CPython never trusts a
    # .pyc whose source hash/mtime does not match. This exclusion is kept
    # byte-for-byte identical to verify-hub-release.py:is_python_bytecode.
    if "__pycache__" in path.parts or relative.endswith(".pyc"):
        continue
    if stat.S_ISLNK(mode):
        symlinks[relative] = os.readlink(path)
        continue
    if not stat.S_ISREG(mode):
        raise SystemExit(f"non-regular release entry: {relative}")
    if relative not in {"release-manifest.json", "release-seal"}:
        files[relative] = hashlib.sha256(path.read_bytes()).hexdigest()
distributions = {
    "fleet-core": "fleet_core",
    "fleet-nats": "fleet_nats",
    "fleet-subscriber": "fleet_subscriber",
    "fleet-orchestrator": "fleet_orchestrator",
}
manifest = {
    "schema_version": 2,
    "sha": sha,
    "lock_sha256": hashlib.sha256((release / "src" / "uv.lock").read_bytes()).hexdigest(),
    "distributions": distributions,
    "files": files,
    "symlinks": symlinks,
}
(release / "release-manifest.json").write_text(json.dumps(manifest, sort_keys=True, separators=(",", ":")) + "\n")
PY
    ( cd "$release_dir" && sha256sum release-manifest.json > release-seal )
    chmod -R a-w "$release_dir"
}

quarantine_release() {
    local candidate="$1"
    local quarantined
    quarantined="$releases_dir/.quarantine-$DEPLOY_SHA-$(date -u +%Y%m%dT%H%M%SZ)-$$"
    mv "$candidate" "$quarantined"
    log "quarantined unsealed release $candidate as $quarantined"
}

build_release() {
    if [ -L "$release_dir" ]; then
        die "release path must not be a symlink: $release_dir"
    fi
    if [ -e "$release_dir" ]; then
        if verify_release "$release_dir" "$DEPLOY_SHA"; then
            log "reusing verified sealed release $release_dir"
            release_complete=1
            return
        fi
        quarantine_release "$release_dir" || die "could not quarantine incomplete release $release_dir"
    fi

    mkdir "$release_dir"
    created_release=1
    mkdir "$release_dir/src"
    rsync -a --delete \
        --exclude '.git' --exclude '.venv' --exclude '__pycache__' \
        "$FLEET_BUS_SRC/packages" "$FLEET_BUS_SRC/scripts" "$release_dir/src/"
    rsync -a "$FLEET_BUS_SRC/pyproject.toml" "$FLEET_BUS_SRC/uv.lock" "$release_dir/src/"
    (
        cd "$release_dir/src"
        uv export --frozen --all-packages --no-dev --no-emit-workspace \
            --format requirements.txt --output-file "$release_dir/third-party.requirements"
    )
    uv venv --python 3.13 "$release_dir/.venv"
    uv pip install --python "$release_dir/.venv/bin/python" --require-hashes --no-deps \
        -r "$release_dir/third-party.requirements"
    uv pip install --no-editable --no-deps --no-build-isolation \
        --python "$release_dir/.venv/bin/python" \
        "$release_dir/src/packages/fleet-core" \
        "$release_dir/src/packages/fleet-nats" \
        "$release_dir/src/packages/fleet-subscriber" \
        "$release_dir/src/packages/fleet-orchestrator"
    printf '%s\n' "$DEPLOY_SHA" >"$release_dir/.fleet-release-sha"
    seal_release
    verify_release "$release_dir" "$DEPLOY_SHA" || die "new release failed sealed-manifest verification"
    release_complete=1
    log "built and sealed release $release_dir"
}

read_prior_state() {
    if [ ! -L "$current_link" ]; then
        [ ! -e "$current_link" ] || die "$current_link exists but is not a symlink"
        die "current is required; only the explicit legacy-bootstrap state is allowed before first deploy"
    fi
    prior_target="$(readlink "$current_link")"
    if [ "$prior_target" = "$FLEET_BUS_HOME" ]; then
        prior_state="legacy-bootstrap"
        [ -x "$prior_target/.venv/bin/fleet-orchestrator" ] || die "legacy bootstrap lacks fleet-orchestrator"
        [ ! -e "$receipt" ] || die "legacy bootstrap must not have DEPLOYED_SHA"
        log "accepted explicit legacy-bootstrap current=$prior_target without receipt"
        return
    fi
    case "$prior_target" in
        "$releases_dir"/*)
            local prior_sha="${prior_target#"$releases_dir"/}"
            [[ "$prior_sha" =~ ^[0-9a-f]{40}$ ]] || die "current points at an invalid release path: $prior_target"
            verify_release "$prior_target" "$prior_sha" || die "current managed release is not sealed and coherent: $prior_target"
            [ -f "$receipt" ] || die "managed current requires DEPLOYED_SHA"
            prior_receipt="$(cat "$receipt")"
            [ "$prior_receipt" = "$prior_sha" ] || die "DEPLOYED_SHA does not match managed current"
            prior_receipt_present=1
            prior_state="managed"
            ;;
        *) die "current points outside the managed release layout: $prior_target" ;;
    esac
}

rollback() {
    local rollback_ok=1
    log "deployment failed after activation; restoring previous $prior_state release"
    atomic_link "$prior_target" || rollback_ok=0
    if [ "$rollback_ok" -eq 1 ]; then
        restart_and_wait || rollback_ok=0
    fi
    if [ "$rollback_ok" -eq 1 ]; then
        restore_receipt || rollback_ok=0
    else
        # A receipt can never claim the prior SHA until its current target has
        # been restored and readiness has been re-proven.
        invalidate_receipt
    fi
    if [ "$rollback_ok" -ne 1 ]; then
        log "CRITICAL: ROLLBACK FAILED; receipt invalidated; inspect $current_link and $SERVICE_NAME" >&2
        return 1
    fi
    log "rollback restored current=$prior_target and coherent receipt state"
}

cleanup() {
    local status=$?
    trap - EXIT INT TERM
    unlink "$next_link" 2>/dev/null || true
    unlink "$receipt_tmp" 2>/dev/null || true
    if [ "$status" -ne 0 ] && [ "$activated" -eq 1 ] && [ "$succeeded" -ne 1 ]; then
        rollback || status=1
    fi
    if [ "$status" -ne 0 ] && [ "$created_release" -eq 1 ] && [ "$release_complete" -ne 1 ] && [ -e "$release_dir" ]; then
        quarantine_release "$release_dir" || log "WARNING: could not quarantine incomplete release $release_dir"
    fi
    exit "$status"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

validate_inputs
for command in flock rsync uv curl jq sha256sum; do
    command -v "$command" >/dev/null 2>&1 || die "$command is required"
done

mkdir -p "$releases_dir"
exec 9>"$lock_file"
flock -n 9 || die "another hub deployment holds $lock_file"

require_effective_execstart
read_prior_state
build_release
activated=1
atomic_link "$release_dir"
restart_and_wait || die "new release did not become ready"
write_receipt "$DEPLOY_SHA"
succeeded=1
log "deployed sha=$DEPLOY_SHA current=$release_dir receipt=$receipt"
