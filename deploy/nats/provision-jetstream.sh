#!/usr/bin/env bash
# Provision + verify the JetStream pilot on the fleet STAGING NATS host
# (fleet-bus #124 / J3, #123). Run this ON the staging host as root, AFTER
# placing deploy/nats/fleet-staging-nats-server.conf at the staging config path
# and setting $STAGING_NATS_PASSWORD in the unit's EnvironmentFile.
#
# It is idempotent (safe to re-run) and it FAILS LOUD: JetStream silently
# degrades if its store dir is unwritable, and a NATS that answers /connz does
# NOT prove JetStream came up — so this creates the store dir with the correct
# owner, (re)starts nats-server, and then PROVES JetStream is enabled via /jsz
# before returning success.
#
# ── PROD SAFETY (fleet-bus #124 codex finding #8, round 2 — FAIL CLOSED) ──────
# This must NEVER run against prod (fleet-hub) — prod is deliberately core-NATS-only
# until the pilot graduates, and this script does `systemctl restart`. The guard
# requires POSITIVE PROOF of staging; the ABSENCE of prod-proof is NOT enough
# (round 1's mistake: a prod host outside the hostname glob, or an unreachable
# monitor, slipped through to the restart). The guard is STRUCTURAL and CLOSED:
#   1. Refuses to run unless FLEET_STAGING=1 / --staging is passed.
#   2. NO prod-matching defaults: store dir, run user/group, systemd unit, and
#      monitor URL are all REQUIRED env vars — a missing one aborts pre-restart.
#   3. Before restarting, it demands the CURRENTLY-RUNNING server prove it is
#      staging: it queries /varz on the REAL monitor port (8223, the fleet
#      convention) and REQUIRES server_name == the expected staging name
#      (FLEET_EXPECT_SERVER_NAME, default "fleet-staging"). Any of:
#         - monitor unreachable / no answer,
#         - /varz missing a server_name,
#         - server_name != the expected staging name (incl. "fleet-hub"),
#      is an UNCONFIRMED identity and the script REFUSES (never restarts). A
#      single missed signal fails CLOSED. The only escape for a genuinely
#      first-boot staging host with no server yet running is an explicit
#      FLEET_ALLOW_NO_RUNNING_SERVER=1, which the operator sets knowingly.
# So a prod (or unknown) host can never reach `systemctl restart` by default.

set -euo pipefail

log() { printf '[provision-jetstream] %s\n' "$*" >&2; }
die() { log "ERROR: $*"; exit 1; }

# --- 0. explicit staging confirmation ----------------------------------------
# Accept either FLEET_STAGING=1 or a --staging flag. No other path proceeds.
for arg in "$@"; do
    case "$arg" in
        --staging) FLEET_STAGING=1 ;;
        *) die "unknown argument: $arg (only --staging is accepted)" ;;
    esac
done
if [ "${FLEET_STAGING:-0}" != "1" ]; then
    die "refusing to run without explicit staging confirmation. This script does a \
systemctl restart and must NEVER touch prod (fleet-hub). Set FLEET_STAGING=1 (or pass \
--staging) ONLY on the staging/shadow-hub host."
fi

# --- 0b. REQUIRED env vars — no prod-matching defaults -----------------------
# Every host-specific value must be supplied explicitly. A missing one aborts
# before any side effect, so a bare invocation can never land on prod's paths,
# user, unit, or monitor.
: "${FLEET_JS_STORE_DIR:?set FLEET_JS_STORE_DIR to the staging JetStream store dir (no default)}"
: "${FLEET_NATS_USER:?set FLEET_NATS_USER to the staging nats-server run user (no default)}"
: "${FLEET_NATS_GROUP:?set FLEET_NATS_GROUP to the staging nats-server run group (no default)}"
: "${FLEET_NATS_UNIT:?set FLEET_NATS_UNIT to the staging systemd unit name (no default)}"
: "${FLEET_NATS_MONITOR_URL:?set FLEET_NATS_MONITOR_URL to the staging monitor URL, e.g. http://127.0.0.1:8223 (no default)}"

STORE_DIR="$FLEET_JS_STORE_DIR"
RUN_USER="$FLEET_NATS_USER"
RUN_GROUP="$FLEET_NATS_GROUP"
NATS_UNIT="$FLEET_NATS_UNIT"
MONITOR_URL="$FLEET_NATS_MONITOR_URL"
# The staging server_name the running server MUST report to prove it is staging.
EXPECT_SERVER_NAME="${FLEET_EXPECT_SERVER_NAME:-fleet-staging}"

# curl timeouts (finding #11): a stalled monitor must never hang the verifier.
CURL_TIMEOUTS=(--connect-timeout 3 --max-time 5)

# --- 0c. POSITIVE staging proof — fail CLOSED (round-2 fix, finding #8) -------
# Require the currently-running server to PROVE it is staging BEFORE we restart
# it. Absence of prod-proof is not enough: an unreachable monitor, a missing
# server_name, or any non-staging name all mean "identity UNCONFIRMED" and we
# REFUSE. The prod hub (server_name=fleet-hub) fails this by definition.
extract_server_name() {
    # Parse "server_name":"<name>" from /varz JSON without a JSON dep.
    printf '%s' "$1" | grep -o '"server_name" *: *"[^"]*"' | head -n1 \
        | sed 's/.*: *"\([^"]*\)".*/\1/'
}

if varz="$(curl -fsS "${CURL_TIMEOUTS[@]}" "$MONITOR_URL/varz" 2>/dev/null)"; then
    running_name="$(extract_server_name "$varz")"
    if [ -z "$running_name" ]; then
        die "the NATS server at $MONITOR_URL answered /varz but reported NO \
server_name — cannot confirm this is staging. Refusing (fail-closed: identity \
unconfirmed)."
    fi
    if [ "$running_name" != "$EXPECT_SERVER_NAME" ]; then
        die "the NATS server at $MONITOR_URL reports server_name='$running_name', \
not the expected staging name '$EXPECT_SERVER_NAME'. This is NOT the staging \
server (it may be prod, e.g. server_name=fleet-hub). Refusing (fail-closed)."
    fi
    log "staging identity CONFIRMED: running server_name='$running_name'"
else
    # No running server answered the monitor. That is only acceptable on a
    # genuine first boot, and only when the operator has EXPLICITLY opted in —
    # otherwise an unreachable monitor on a prod box (wrong port, firewall) would
    # otherwise slip through. Fail CLOSED by default.
    if [ "${FLEET_ALLOW_NO_RUNNING_SERVER:-0}" != "1" ]; then
        die "no NATS server answered $MONITOR_URL/varz, so this host's identity \
is UNCONFIRMED. Refusing to restart (fail-closed): an unreachable monitor must \
not be read as 'safe'. If this is a genuine first-boot staging host with no \
server yet running, set FLEET_ALLOW_NO_RUNNING_SERVER=1 to proceed knowingly."
    fi
    log "no running server; FLEET_ALLOW_NO_RUNNING_SERVER=1 set — proceeding (first boot)"
fi

# --- 1. store dir, owned by the nats-server run user -------------------------
# A store dir owned by the wrong user is the most common "JetStream didn't
# enable" failure — install -d is idempotent and fixes ownership on re-run.
if ! id -u "$RUN_USER" >/dev/null 2>&1; then
    die "run user '$RUN_USER' does not exist — set FLEET_NATS_USER to the nats-server User="
fi
log "ensuring store dir $STORE_DIR owned by $RUN_USER:$RUN_GROUP"
install -d -o "$RUN_USER" -g "$RUN_GROUP" -m 0750 "$STORE_DIR" \
    || die "could not create/own store dir $STORE_DIR"

# --- 2. (re)start nats-server ------------------------------------------------
log "restarting nats-server unit=$NATS_UNIT"
systemctl restart "$NATS_UNIT" \
    || die "nats-server ($NATS_UNIT) failed to (re)start — check journalctl -u $NATS_UNIT"

# --- 3. PROVE JetStream is enabled (not just that NATS answers) --------------
# /connz answering only proves the client listener is up. /jsz carries the
# JetStream config+store only when JetStream actually enabled; a 404/empty means
# it did not come up (usually store-dir ownership above). curl carries explicit
# connect/overall timeouts so a stalled monitor fails loud instead of hanging.
log "verifying JetStream via $MONITOR_URL/jsz"
for attempt in 1 2 3 4 5; do
    if jsz="$(curl -fsS "${CURL_TIMEOUTS[@]}" "$MONITOR_URL/jsz" 2>/dev/null)" \
        && printf '%s' "$jsz" | grep -q '"config"'; then
        store="$(printf '%s' "$jsz" | grep -o '"store_dir":[^,}]*' || true)"
        log "JetStream ENABLED — $store"
        exit 0
    fi
    log "JetStream not ready yet (attempt $attempt/5) — retrying"
    sleep 2
done
die "JetStream did NOT enable — /jsz never reported a config block. Most likely the store dir ($STORE_DIR) is not writable by the nats-server run user, or the jetstream {} block is missing from the staging config. Check: journalctl -u $NATS_UNIT, and ls -la $STORE_DIR"
