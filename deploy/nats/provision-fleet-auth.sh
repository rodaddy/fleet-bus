#!/usr/bin/env bash
# provision-fleet-auth.sh — materialize the fleet-hub NATS auth EnvironmentFile
# from Vaultwarden and install the committed config on the hub (F2 server half,
# fleet-bus #104; posture docs/f2-nats-auth-posture.md §5–§6).
#
# This script holds Vaultwarden ITEM REFS only. Secret VALUES exist in exactly
# two places: the Vaultwarden items, and the 0600 EnvironmentFile this script
# writes on the hub host. Nothing is ever echoed, logged, or committed.
#
# Secret values are NEVER evaluated as shell code: every fetched value must
# match SECRET_RE (single-line, base64 alphabet only — the mint format), and
# `check`/`install` pass variables to nats-server via env(1) after per-line
# validation instead of dot-sourcing the EnvironmentFile.
#
# Usage:
#   provision-fleet-auth.sh mint          # create any missing Vaultwarden items
#                                         # with fresh random values (idempotent:
#                                         # existing items are kept, but their
#                                         # value format and login.username are
#                                         # verified — fail closed on drift)
#   provision-fleet-auth.sh materialize   # fetch all values from Vaultwarden and
#                                         # write ENV_FILE (default
#                                         # /home/fleet/nats/fleet-auth.env, 0600);
#                                         # re-verifies each item's value format
#                                         # AND login.username — fail closed on
#                                         # drift, mint is not a prerequisite gate
#   provision-fleet-auth.sh check         # validate the committed server config
#                                         # (nats-server -t) against ENV_FILE
#   provision-fleet-auth.sh install       # check, then install the config at
#                                         # CONF_DEST, wire EnvironmentFile= into
#                                         # the NATS unit via a systemd drop-in,
#                                         # restart, and verify the daemon is up.
#                                         # Failure-atomic: artifacts are staged
#                                         # and renamed into place, and any
#                                         # restart/health failure restores the
#                                         # pre-install config + drop-in and
#                                         # restarts the prior state
#
# Requires an unlocked `bw` (Bitwarden/Vaultwarden CLI) session for mint and
# materialize: export BW_SESSION first. Fails closed on any missing tool,
# locked vault, absent item, malformed value, or username drift — a partial
# EnvironmentFile is never left behind.

set -euo pipefail
umask 077

ENV_FILE="${FLEET_NATS_AUTH_ENV_FILE:-/home/fleet/nats/fleet-auth.env}"
CONF_FILE="$(cd "$(dirname "$0")" && pwd)/fleet-prod-nats-server.conf"
CONF_DEST="${FLEET_NATS_CONF_DEST:-/home/fleet/nats/nats-server.conf}"
UNIT_NAME="${FLEET_NATS_UNIT:-nats-server.service}"
DROPIN_DIR="${FLEET_NATS_DROPIN_DIR:-/etc/systemd/system/${UNIT_NAME}.d}"
MONITOR_URL="${FLEET_NATS_MONITOR_URL:-http://127.0.0.1:8223/healthz}"

# Every secret value must be a single line drawn from the base64 alphabet AND
# start with a LETTER. The leading-letter rule is a NATS parser constraint, not
# cosmetics: nats-server runs env-substituted values through its config parser,
# and a digit-led value can parse as a sized number ("0K8…" → 0 kilobytes) or
# scientific float ("7E1J…" → 7e1) and crash the daemon at startup. Rotated or
# imported values MUST conform — anything else (whitespace, quotes, $(),
# backticks, newlines, digit-led) is rejected before it can reach a file or a
# process environment.
SECRET_RE='^[A-Za-z][A-Za-z0-9+/=]{31,127}$'

# Vaultwarden ref convention: one item per principal (posture §5 — the rotation
# unit is one principal). ENV_VAR=vaultwarden-item-name=nats-username. The item
# name is the vault ref; login.username is the NATS username the server
# authenticates (fleet-prod-nats-server.conf `user:` entries) so any consumer
# of the login record can authenticate as-is. These are REFS, not secrets.
# Item names are VAULT_ITEM_PREFIX + principal; set FLEET_NATS_VAULT_ITEM_PREFIX
# to match your own vault naming.
VAULT_ITEM_PREFIX="${FLEET_NATS_VAULT_ITEM_PREFIX:-fleet-nats-}"
PRINCIPALS=(
    "FLEET_NATS_PW_NAGATHA=${VAULT_ITEM_PREFIX}nagatha=nagatha"
    "FLEET_NATS_PW_SKIPPY=${VAULT_ITEM_PREFIX}skippy=skippy"
    "FLEET_NATS_PW_BILBY=${VAULT_ITEM_PREFIX}bilby=bilby"
    "FLEET_NATS_PW_HUB_CONTROL=${VAULT_ITEM_PREFIX}hub-control=fleet-hub-control"
    "FLEET_NATS_PW_OPEN_BRAIN=${VAULT_ITEM_PREFIX}open-brain=open-brain"
    "FLEET_NATS_PW_KBOARD=${VAULT_ITEM_PREFIX}kboard=fleet-kboard"
    "FLEET_NATS_PW_RUNNER=${VAULT_ITEM_PREFIX}runner=fleet-runner"
    "FLEET_NATS_PW_LOCAL_DEV=${VAULT_ITEM_PREFIX}local-dev=local-dev"
    "FLEET_NATS_PW_SYS_OPERATOR=${VAULT_ITEM_PREFIX}sys-operator=fleet-sys-operator"
)

die() {
    printf 'provision-fleet-auth: ERROR: %s\n' "$1" >&2
    exit 1
}

[[ "$VAULT_ITEM_PREFIX" != *=* ]] || die "FLEET_NATS_VAULT_ITEM_PREFIX must not contain '='"

require_bw() {
    command -v bw >/dev/null 2>&1 || die "bw CLI not found (Vaultwarden access required)"
    [[ -n "${BW_SESSION:-}" ]] || die "BW_SESSION not set — unlock the vault first (bw unlock)"
    local status
    status="$(bw status 2>/dev/null | grep -o '"status":"[a-z]*"' || true)"
    [[ "$status" == '"status":"unlocked"' ]] || die "vault is not unlocked (bw status: ${status:-unknown})"
}

# Fetch one item's password to stdout. Callers must capture into a variable —
# never pipe onward or echo.
vault_get() {
    bw get password "$1" 2>/dev/null
}

# Reject any secret value that is not a single base64-alphabet line. The value
# itself is never printed — only the item ref.
validate_secret() {
    local item="$1" value="$2"
    [[ "$value" =~ $SECRET_RE ]] || die \
        "vault item $item: value is not single-line base64-safe (mint format) — rotate it to a conforming value (posture §5)"
}

# The login record must authenticate as-is: its username has to be the NATS
# username the server's `user:` entry carries, not the item ref (PR #183
# round-1 finding 3, round-2 finding 1). Enforced on EVERY fetch — mint-time
# validation alone can be skipped (operator never runs mint) or drift after
# minting, so materialize/check/install must fail closed on mismatch too.
validate_username() {
    local item="$1" nats_user="$2" vault_user
    vault_user="$(bw get username "$item" 2>/dev/null || true)"
    [[ "$vault_user" == "$nats_user" ]] || die \
        "vault item $item: login.username is '${vault_user:-<unset>}' but the NATS server authenticates '$nats_user' — fix the item's username, then re-run"
}

cmd_mint() {
    require_bw
    local entry var item nats_user created=0 kept=0
    for entry in "${PRINCIPALS[@]}"; do
        var="${entry%%=*}"
        item="${entry#*=}"; nats_user="${item#*=}"; item="${item%%=*}"
        : "$var"
        local existing
        if existing="$(vault_get "$item")"; then
            validate_secret "$item" "$existing"
            existing=""
            validate_username "$item" "$nats_user"
            kept=$((kept + 1))
            printf 'provision-fleet-auth: %s exists — kept (rotate explicitly, posture §5)\n' "$item"
            continue
        fi
        # 32 random bytes, base64 — held only in this shell variable, stored
        # straight into the vault item, never printed. base64 alphabet needs no
        # JSON escaping; redraw until the value satisfies SECRET_RE (leading
        # letter — see the SECRET_RE comment for why digit-led values are
        # unsafe for the NATS config parser).
        local secret
        while :; do
            secret="$(openssl rand -base64 32)" || die "openssl rand failed"
            [[ "$secret" =~ $SECRET_RE ]] && break
        done
        printf '{"type":1,"name":"%s","notes":"fleet-bus NATS credential; rotation unit = this principal only","login":{"username":"%s","password":"%s"}}' \
            "$item" "$nats_user" "$secret" | bw encode | bw create item >/dev/null \
            || die "failed to create vault item $item"
        secret=""
        created=$((created + 1))
        printf 'provision-fleet-auth: minted %s (username %s)\n' "$item" "$nats_user"
    done
    bw sync >/dev/null 2>&1 || true
    printf 'provision-fleet-auth: mint done (%d created, %d kept)\n' "$created" "$kept"
}

cmd_materialize() {
    require_bw
    local tmp entry var item nats_user value
    tmp="$(mktemp "${ENV_FILE}.XXXXXX")"
    trap 'rm -f "$tmp"' EXIT
    {
        printf '# fleet-hub NATS auth env — MATERIALIZED from Vaultwarden by\n'
        printf '# provision-fleet-auth.sh. Do not edit; do not copy; 0600 only.\n'
    } >"$tmp"
    for entry in "${PRINCIPALS[@]}"; do
        var="${entry%%=*}"
        item="${entry#*=}"; nats_user="${item#*=}"; item="${item%%=*}"
        value="$(vault_get "$item")" || die "vault item missing: $item — run 'mint' first (fail closed, no partial env file)"
        [[ -n "$value" ]] || die "vault item empty: $item (fail closed)"
        # A value that fails this gate can never reach the file, so nothing
        # downstream ever needs to shell-evaluate untrusted bytes.
        validate_secret "$item" "$value"
        # Username drift fails the materialize itself, so every install path
        # (materialize → check → install) is covered even when mint is skipped.
        validate_username "$item" "$nats_user"
        # Persist the public identity beside its secret.  Downstream validation
        # treats these two lines as one credential-material unit, so an old or
        # hand-written password-only file cannot pass check/install.
        printf '%s_USER=%s\n' "$var" "$nats_user" >>"$tmp"
        printf '%s=%s\n' "$var" "$value" >>"$tmp"
        value=""
    done
    chmod 0600 "$tmp"
    mv "$tmp" "$ENV_FILE"
    trap - EXIT
    printf 'provision-fleet-auth: wrote %s (0600, %d principals)\n' "$ENV_FILE" "${#PRINCIPALS[@]}"
}

# Read ENV_FILE without sourcing it: every non-comment line must be exactly
# VAR=<base64 value>. Populates the caller's named array with validated
# VAR=value strings for env(1). Error messages carry line numbers only — never
# line content (it is secret material).
read_env_file() {
    local -n out_ref="$1"
    out_ref=()
    require_bw
    [[ -f "$ENV_FILE" ]] || die "env file not found: $ENV_FILE — run 'materialize' first"
    local line lineno=0 entry var item expected_user vault_user vault_password
    local -A values=()
    while IFS= read -r line || [[ -n "$line" ]]; do
        lineno=$((lineno + 1))
        [[ -z "$line" || "$line" == \#* ]] && continue
        [[ "$line" =~ ^(FLEET_NATS_PW_[A-Z_]+(_USER)?)=([^[:space:]]+)$ ]] || die \
            "malformed entry at $ENV_FILE line $lineno — re-run 'materialize'"
        var="${BASH_REMATCH[1]}"
        [[ -z "${values[$var]+set}" ]] || die \
            "duplicate credential field at $ENV_FILE line $lineno — re-run 'materialize'"
        values[$var]="${BASH_REMATCH[3]}"
    done <"$ENV_FILE"
    [[ ${#values[@]} -eq $((${#PRINCIPALS[@]} * 2)) ]] || die \
        "$ENV_FILE does not contain one username/password unit for every principal — re-run 'materialize'"
    for entry in "${PRINCIPALS[@]}"; do
        var="${entry%%=*}"
        item="${entry#*=}"; expected_user="${item#*=}"; item="${item%%=*}"
        [[ "${values[${var}_USER]:-}" == "$expected_user" ]] || die \
            "$ENV_FILE identity mismatch for $var — re-run 'materialize'"
        [[ "${values[$var]:-}" =~ $SECRET_RE ]] || die \
            "$ENV_FILE has invalid password material for $var — re-run 'materialize'"
        vault_user="$(bw get username "$item" 2>/dev/null || true)"
        [[ "$vault_user" == "${values[${var}_USER]}" ]] || die \
            "vault identity mismatch for $item — re-run 'materialize'"
        vault_password="$(vault_get "$item")" || die \
            "vault item missing: $item — re-run 'materialize'"
        validate_secret "$item" "$vault_password"
        [[ "$vault_password" == "${values[$var]}" ]] || die \
            "vault credential mismatch for $item — re-run 'materialize'"
        out_ref+=("$var=${values[$var]}")
        vault_password=""
    done
}

cmd_check() {
    command -v nats-server >/dev/null 2>&1 || die "nats-server not found — cannot validate config"
    [[ -f "$CONF_FILE" ]] || die "config not found: $CONF_FILE"
    local -a auth_env
    read_env_file auth_env
    # env(1) sets the variables for the validator only — the values are never
    # interpreted by a shell.
    env "${auth_env[@]}" nats-server -t -c "$CONF_FILE" \
        || die "nats-server config validation FAILED"
    printf 'provision-fleet-auth: config OK: %s\n' "$CONF_FILE"
}

# Roll the live host back to the pre-install artifacts after a failed
# restart/probe: restore (or remove, if they did not exist) the config and the
# systemd drop-in, then daemon-reload + restart so the service runs the prior
# state again. Best-effort by design — it runs inside a failure path.
DROPIN_FILE=""
CONF_BAK=""
DROPIN_BAK=""
rollback_install() {
    printf 'provision-fleet-auth: ROLLBACK: restoring pre-install config and drop-in\n' >&2
    if [[ -n "$CONF_BAK" ]]; then
        mv -f "$CONF_BAK" "$CONF_DEST" || printf 'provision-fleet-auth: ROLLBACK: failed to restore %s\n' "$CONF_DEST" >&2
    else
        rm -f "$CONF_DEST"
    fi
    if [[ -n "$DROPIN_BAK" ]]; then
        mv -f "$DROPIN_BAK" "$DROPIN_FILE" || printf 'provision-fleet-auth: ROLLBACK: failed to restore %s\n' "$DROPIN_FILE" >&2
    else
        rm -f "$DROPIN_FILE"
    fi
    systemctl daemon-reload || true
    systemctl restart "$UNIT_NAME" \
        || printf 'provision-fleet-auth: ROLLBACK: restart of %s failed — service needs manual attention\n' "$UNIT_NAME" >&2
}

# Fail the install AND roll the host back — never leave the new artifacts
# installed after a failed restart/health-check (PR #183 round-2 finding 4).
die_rollback() {
    rollback_install
    die "$1 — live host rolled back to pre-install state"
}

cmd_install() {
    # Fail closed before touching the live host: the committed config must
    # validate against the materialized env file first.
    cmd_check
    command -v systemctl >/dev/null 2>&1 || die "systemctl not found — install runs on the hub host (fleet-hub)"
    command -v curl >/dev/null 2>&1 || die "curl not found — cannot verify the installed daemon"
    local effective_exec
    effective_exec="$(systemctl show --property=ExecStart --value "$UNIT_NAME")" \
        || die "failed to inspect effective ExecStart for $UNIT_NAME"
    if ! tr ' ;{}' '\n' <<<"$effective_exec" | grep -Fx -- "$CONF_DEST" >/dev/null; then
        die "$UNIT_NAME effective ExecStart does not reference $CONF_DEST — refusing install"
    fi
    mkdir -p "$DROPIN_DIR" || die "failed to create $DROPIN_DIR"
    DROPIN_FILE="$DROPIN_DIR/10-fleet-auth.conf"
    # Snapshot the pre-install state so any failure past this point can restore
    # it exactly (absent file → absent file, not an empty leftover).
    if [[ -f "$CONF_DEST" ]]; then
        CONF_BAK="$(mktemp "${CONF_DEST}.bak.XXXXXX")" || die "failed to create config backup"
        cp -p "$CONF_DEST" "$CONF_BAK" || die "failed to back up $CONF_DEST"
    fi
    if [[ -f "$DROPIN_FILE" ]]; then
        DROPIN_BAK="$(mktemp "${DROPIN_FILE}.bak.XXXXXX")" || die "failed to create drop-in backup"
        cp -p "$DROPIN_FILE" "$DROPIN_BAK" || die "failed to back up $DROPIN_FILE"
    fi
    # Stage each artifact next to its destination and rename into place —
    # rename(2) is atomic, so the live path never holds a partial write.
    local conf_stage dropin_stage
    conf_stage="$(mktemp "${CONF_DEST}.stage.XXXXXX")" || die "failed to stage config"
    if ! cp "$CONF_FILE" "$conf_stage" || ! chmod 0644 "$conf_stage"; then
        rm -f "$conf_stage"
        die "failed to stage config at $conf_stage"
    fi
    dropin_stage="$(mktemp "${DROPIN_FILE}.stage.XXXXXX")" || { rm -f "$conf_stage"; die "failed to stage drop-in"; }
    if ! printf '# Wired by provision-fleet-auth.sh — NATS auth EnvironmentFile (fleet-bus #104).\n[Service]\nEnvironmentFile=%s\n' \
        "$ENV_FILE" >"$dropin_stage" || ! chmod 0644 "$dropin_stage"; then
        rm -f "$conf_stage" "$dropin_stage"
        die "failed to stage $DROPIN_FILE"
    fi
    mv -f "$conf_stage" "$CONF_DEST" || { rm -f "$conf_stage" "$dropin_stage"; die "failed to install config at $CONF_DEST"; }
    mv -f "$dropin_stage" "$DROPIN_FILE" || { rm -f "$dropin_stage"; die_rollback "failed to install $DROPIN_FILE"; }
    systemctl daemon-reload || die_rollback "systemctl daemon-reload failed"
    systemctl restart "$UNIT_NAME" || die_rollback "restart of $UNIT_NAME failed — inspect 'journalctl -u $UNIT_NAME'"
    systemctl is-active --quiet "$UNIT_NAME" || die_rollback "$UNIT_NAME is not active after restart"
    # Verify the running daemon, not just the unit state.
    curl -fsS --max-time 5 "$MONITOR_URL" >/dev/null \
        || die_rollback "monitor probe failed: $MONITOR_URL — daemon restarted but is not answering"
    rm -f "$CONF_BAK" "$DROPIN_BAK"
    printf 'provision-fleet-auth: installed %s, wired EnvironmentFile into %s, restarted, verified\n' \
        "$CONF_DEST" "$UNIT_NAME"
}

case "${1:-}" in
    mint) cmd_mint ;;
    materialize) cmd_materialize ;;
    check) cmd_check ;;
    install) cmd_install ;;
    *)
        printf 'usage: %s mint|materialize|check|install\n' "$0" >&2
        exit 2
        ;;
esac
