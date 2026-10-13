#!/usr/bin/env bash
# Transfer an exact commit to fleet-hub and deploy it without runner credentials.
set -euo pipefail

log() { printf 'deploy-hub-remote | %s\n' "$*"; }
die() { log "ERROR: $*" >&2; exit 1; }

# EXIT traps run after function-local state unwinds, so cleanup state must remain
# script-scoped for set -e failures as well as normal returns.
cleanup_mode=""
cleanup_ref_created=0
cleanup_bundle_ref=""
cleanup_bundle_path=""
cleanup_sources_dir=""
cleanup_deploy_sha=""
cleanup_verify_dir=""
cleanup_stage_dir=""
cleanup_incoming_bundle=""
cleanup_incoming_validated=0

cleanup_remote_tree() {
    local path="$1"
    [ -n "$path" ] || return 0
    case "$path" in
        "$cleanup_sources_dir/.verify-$cleanup_deploy_sha-"*|"$cleanup_sources_dir/.staging-$cleanup_deploy_sha-"*) ;;
        *) log "WARNING: refused cleanup outside bounded staging layout: $path"; return 1 ;;
    esac
    if [ -e "$path" ] || [ -L "$path" ]; then
        find "$path" -depth -delete
    fi
}

cleanup() {
    local status=$?
    trap - EXIT INT TERM
    case "$cleanup_mode" in
        runner)
            if [ "$cleanup_ref_created" -eq 1 ]; then
                git update-ref -d "$cleanup_bundle_ref" >/dev/null 2>&1 || {
                    log "WARNING: could not remove temporary bundle ref $cleanup_bundle_ref"
                    status=1
                }
            fi
            if [ -f "$cleanup_bundle_path" ] && [ ! -L "$cleanup_bundle_path" ]; then
                unlink "$cleanup_bundle_path" || {
                    log "WARNING: could not remove runner bundle $cleanup_bundle_path"
                    status=1
                }
            fi
            ;;
        remote)
            cleanup_remote_tree "$cleanup_verify_dir" || status=1
            cleanup_remote_tree "$cleanup_stage_dir" || status=1
            if [ "$cleanup_incoming_validated" -eq 1 ] && \
                { [ -e "$cleanup_incoming_bundle" ] || [ -L "$cleanup_incoming_bundle" ]; }; then
                unlink "$cleanup_incoming_bundle" || {
                    log "WARNING: could not remove incoming bundle $cleanup_incoming_bundle"
                    status=1
                }
            fi
            ;;
    esac
    exit "$status"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

validate_sha() {
    [[ "$1" =~ ^[0-9a-f]{40}$ ]] || die "DEPLOY_SHA must be a full lowercase commit SHA"
}

runner_main() {
    local deploy_sha="${DEPLOY_SHA:-}"
    local runner_temp="${RUNNER_TEMP:-}"
    local run_id="${GITHUB_RUN_ID:-}"
    local run_attempt="${GITHUB_RUN_ATTEMPT:-}"
    local deploy_host="fleet-hub-deploy"
    local remote_home="/opt/fleet-bus"
    local bundle_dir bundle_path bundle_sha256 remote_bundle bundle_ref

    validate_sha "$deploy_sha"
    [[ "$runner_temp" = /* ]] || die "RUNNER_TEMP must be an absolute path"
    [[ "$run_id" =~ ^[1-9][0-9]*$ ]] || die "GITHUB_RUN_ID must be a positive integer"
    [[ "$run_attempt" =~ ^[1-9][0-9]*$ ]] || die "GITHUB_RUN_ATTEMPT must be a positive integer"
    for command in git sha256sum scp ssh unlink; do
        command -v "$command" >/dev/null 2>&1 || die "$command is required"
    done

    [ "$(git rev-parse HEAD)" = "$deploy_sha" ] || die "checkout HEAD does not match DEPLOY_SHA"
    [ -z "$(git status --porcelain=v1 --untracked-files=all)" ] || die "checkout must be clean at DEPLOY_SHA"

    bundle_dir="$runner_temp/fleet-bus"
    bundle_path="$bundle_dir/${deploy_sha}-${run_id}-${run_attempt}.bundle"
    remote_bundle="$remote_home/incoming/${deploy_sha}-${run_id}-${run_attempt}.bundle"
    # git bundle refuses an otherwise-unreferenced raw SHA as an empty bundle.
    bundle_ref="refs/fleet-bus/deploy"
    cleanup_mode="runner"
    cleanup_bundle_path="$bundle_path"
    cleanup_bundle_ref="$bundle_ref"
    mkdir -p "$bundle_dir"
    if [ ! -d "$bundle_dir" ] || [ -L "$bundle_dir" ]; then
        die "runner bundle directory is unsafe"
    fi
    if [ -e "$bundle_path" ] || [ -L "$bundle_path" ]; then
        die "runner bundle path already exists"
    fi

    git update-ref "$bundle_ref" "$deploy_sha" "" || die "temporary bundle ref already exists"
    cleanup_ref_created=1
    git bundle create "$bundle_path" "$bundle_ref"
    git update-ref -d "$bundle_ref"
    cleanup_ref_created=0
    bundle_sha256="$(sha256sum "$bundle_path" | awk '{print $1}')"
    [[ "$bundle_sha256" =~ ^[0-9a-f]{64}$ ]] || die "could not hash deployment bundle"

    if ! scp -o BatchMode=yes -- "$bundle_path" "$deploy_host:$remote_bundle"; then
        if ! ssh -o BatchMode=yes "$deploy_host" \
            bash -s -- --cleanup-incoming "$deploy_sha" "$remote_bundle" <"$0"; then
            log "WARNING: could not clean failed transfer $remote_bundle"
        fi
        die "bundle transfer failed"
    fi
    if ! ssh -o BatchMode=yes "$deploy_host" \
        bash -s -- --remote "$deploy_sha" "$bundle_sha256" "$remote_bundle" <"$0"; then
        if ! ssh -o BatchMode=yes "$deploy_host" \
            bash -s -- --cleanup-incoming "$deploy_sha" "$remote_bundle" <"$0"; then
            log "WARNING: could not clean failed handoff $remote_bundle"
        fi
        die "remote deploy failed"
    fi
    log "remote deploy completed sha=$deploy_sha host=$deploy_host"
}

cleanup_incoming_main() {
    [ "$#" -eq 2 ] || die "cleanup mode requires DEPLOY_SHA and incoming path"
    local deploy_sha="$1"
    local incoming_bundle="$2"
    local home="${FLEET_BUS_HOME:-/opt/fleet-bus}"
    local incoming_dir bundle_name

    validate_sha "$deploy_sha"
    [[ "$home" = /* ]] || die "FLEET_BUS_HOME must be an absolute path"
    command -v realpath >/dev/null 2>&1 || die "realpath is required"
    command -v unlink >/dev/null 2>&1 || die "unlink is required"
    home="$(realpath -m "$home")"
    incoming_dir="$home/incoming"
    if [ ! -d "$incoming_dir" ] || [ -L "$incoming_dir" ]; then
        die "incoming directory is missing or unsafe: $incoming_dir"
    fi
    [ "$(dirname "$incoming_bundle")" = "$incoming_dir" ] || \
        die "incoming bundle path is outside the bounded deployment layout"
    bundle_name="$(basename "$incoming_bundle")"
    [[ "$bundle_name" =~ ^${deploy_sha}-[1-9][0-9]*-[1-9][0-9]*\.bundle$ ]] || \
        die "incoming bundle name is outside the bounded deployment layout"
    if [ -e "$incoming_bundle" ] || [ -L "$incoming_bundle" ]; then
        unlink "$incoming_bundle"
        log "cleaned failed transfer $incoming_bundle"
    fi
}

remote_main() {
    [ "$#" -eq 3 ] || die "remote mode requires DEPLOY_SHA, bundle SHA-256, and incoming path"
    local deploy_sha="$1"
    local expected_hash="$2"
    local incoming_bundle="$3"
    local home="${FLEET_BUS_HOME:-/opt/fleet-bus}"
    local incoming_dir sources_dir source_dir verify_dir stage_dir quarantine_path
    local incoming_real incoming_parent bundle_name actual_hash bundle_heads source_real

    validate_sha "$deploy_sha"
    [[ "$expected_hash" =~ ^[0-9a-f]{64}$ ]] || die "bundle SHA-256 must be 64 lowercase hex characters"
    [[ "$home" = /* ]] || die "FLEET_BUS_HOME must be an absolute path"
    for command in git sha256sum realpath flock find mv unlink bash; do
        command -v "$command" >/dev/null 2>&1 || die "$command is required"
    done

    home="$(realpath -m "$home")"
    incoming_dir="$home/incoming"
    sources_dir="$home/sources"
    source_dir="$sources_dir/$deploy_sha"
    verify_dir="$sources_dir/.verify-$deploy_sha-$$"
    stage_dir="$sources_dir/.staging-$deploy_sha-$$"
    quarantine_path=""

    if [ ! -d "$incoming_dir" ] || [ -L "$incoming_dir" ]; then
        die "incoming directory is missing or unsafe: $incoming_dir"
    fi
    [ "$(dirname "$incoming_bundle")" = "$incoming_dir" ] || \
        die "incoming bundle path is outside the bounded deployment layout"
    bundle_name="$(basename "$incoming_bundle")"
    [[ "$bundle_name" =~ ^${deploy_sha}-[1-9][0-9]*-[1-9][0-9]*\.bundle$ ]] || \
        die "incoming bundle name is outside the bounded deployment layout"
    if [ ! -f "$incoming_bundle" ] || [ -L "$incoming_bundle" ]; then
        die "incoming bundle is missing or unsafe"
    fi
    incoming_real="$(realpath "$incoming_bundle")" || die "could not resolve incoming bundle"
    incoming_parent="$(dirname "$incoming_real")"
    [ "$incoming_parent" = "$(realpath "$incoming_dir")" ] || \
        die "incoming bundle resolves outside the bounded deployment layout"
    cleanup_mode="remote"
    cleanup_sources_dir="$sources_dir"
    cleanup_deploy_sha="$deploy_sha"
    cleanup_verify_dir="$verify_dir"
    cleanup_stage_dir="$stage_dir"
    cleanup_incoming_bundle="$incoming_bundle"
    cleanup_incoming_validated=1

    actual_hash="$(sha256sum "$incoming_bundle" | awk '{print $1}')"
    [ "$actual_hash" = "$expected_hash" ] || die "incoming bundle SHA-256 mismatch"

    mkdir -p "$sources_dir"
    if [ ! -d "$sources_dir" ] || [ -L "$sources_dir" ] || \
        [ "$(realpath "$sources_dir")" != "$sources_dir" ]; then
        die "sources directory is unsafe: $sources_dir"
    fi
    exec 9>"$sources_dir/.materialize.lock"
    flock -n 9 || die "another source materialization holds $sources_dir/.materialize.lock"

    git init --bare -q "$verify_dir"
    git -C "$verify_dir" bundle verify "$incoming_bundle" >/dev/null
    bundle_heads="$(git bundle list-heads "$incoming_bundle")"
    [ "$bundle_heads" = "$deploy_sha refs/fleet-bus/deploy" ] || \
        die "bundle does not advertise exactly DEPLOY_SHA"
    cleanup_remote_tree "$verify_dir"
    cleanup_verify_dir=""

    source_is_valid() {
        local candidate="$1"
        [ -d "$candidate" ] && [ ! -L "$candidate" ] || return 1
        [ -d "$candidate/.git" ] && [ ! -L "$candidate/.git" ] || return 1
        source_real="$(realpath "$candidate")" || return 1
        [ "$source_real" = "$candidate" ] || return 1
        [ "$(git -C "$candidate" rev-parse --is-inside-work-tree 2>/dev/null)" = true ] || return 1
        [ "$(git -C "$candidate" rev-parse HEAD 2>/dev/null)" = "$deploy_sha" ] || return 1
        ! git -C "$candidate" symbolic-ref -q HEAD >/dev/null 2>&1 || return 1
        [ -z "$(git -C "$candidate" status --porcelain=v1 --untracked-files=all 2>/dev/null)" ] || return 1
        git -C "$candidate" fsck --full --strict >/dev/null 2>&1 || return 1
    }

    if [ -e "$source_dir" ] || [ -L "$source_dir" ]; then
        if source_is_valid "$source_dir"; then
            log "reusing clean detached source $source_dir"
        else
            quarantine_path="$sources_dir/.quarantine-$deploy_sha-$(date -u +%Y%m%dT%H%M%SZ)-$$"
            mv -T "$source_dir" "$quarantine_path"
            log "quarantined invalid source $source_dir as $quarantine_path"
        fi
    fi

    if [ ! -e "$source_dir" ] && [ ! -L "$source_dir" ]; then
        git init -q "$stage_dir"
        git -C "$stage_dir" fetch --no-tags "$incoming_bundle" refs/fleet-bus/deploy >/dev/null
        git -C "$stage_dir" checkout --detach FETCH_HEAD >/dev/null
        source_is_valid "$stage_dir" || die "materialized source failed exact-SHA validation"
        mv -T "$stage_dir" "$source_dir"
        cleanup_stage_dir=""
        log "materialized clean detached source $source_dir"
    fi

    source_is_valid "$source_dir" || die "source checkout failed final validation"
    if [ ! -f "$source_dir/scripts/deploy-hub-release.sh" ] || \
        [ -L "$source_dir/scripts/deploy-hub-release.sh" ]; then
        die "exact source is missing a safe deploy-hub-release.sh"
    fi
    DEPLOY_SHA="$deploy_sha" FLEET_BUS_SRC="$source_dir" \
        bash "$source_dir/scripts/deploy-hub-release.sh"
}

case "${1:-}" in
    --remote)
        shift
        remote_main "$@"
        ;;
    --cleanup-incoming)
        shift
        cleanup_incoming_main "$@"
        ;;
    *)
        [ "$#" -eq 0 ] || die "unexpected arguments"
        runner_main
        ;;
esac
