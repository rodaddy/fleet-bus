#!/usr/bin/env sh
# F3 staging-isolation drill (fleet-bus #105).
#
# Proves a REAL end-to-end topology exercise runs against the SEPARATE staging
# NATS server with ZERO prod traffic touched — the issue's acceptance gate. It is
# an effective-state probe of the RUNNING systems (both monitors + the hub's own
# lifecycle log), never a config assumption and never a mere connection snapshot.
#
# The prior version PASSED with the shadow hub STOPPED: it only started one dummy
# subscriber and checked one connection name at a single instant, so it proved
# neither that the topology ran nor that prod stayed quiet across the window.
# This drill fixes both halves (HIGH-3):
#
#   POSITIVE / e2e (staging carried a REAL exercise):
#     - start a WORKER dummy on staging, then drive ONE orchestrator session with
#       `fleet-orchestrator --once --dry-run` pointed at staging. The hub only
#       logs `--once task=<DRILL_ID> reached DONE` after a real
#       dispatch -> status/relay -> done round-trip on `staging.*` subjects, so
#       that line is the proof the topology actually ran. NO such line -> FAIL
#       (the shadow hub was not running the exercise, or the worker never
#       responded). A connection merely being present is NOT sufficient.
#     - the drill's connections (`fleet-*-<DRILL_ID>`) also appear on the STAGING
#       monitor's /connz. Belt-and-suspenders on the positive side.
#
#   NEGATIVE / durable prod audit (prod was UNTOUCHED across the whole window):
#     - the prod monitor is polled REPEATEDLY across the entire drill window
#       (not one snapshot). If the drill connection name appears on prod at ANY
#       poll -> FAIL (leak). A single snapshot misses a connection that touched
#       prod and disconnected between snapshots — the repeated poll narrows that.
#     - the prod audit MUST be establishable: if the prod monitor is unreachable
#       at the START (baseline cannot be taken) or goes unreachable DURING the
#       window (a gap the leak could hide in), the drill SKIPs, never PASSes.
#       Proving a negative requires actually looking, for the whole window.
#
#   GUARD:
#     - the drill env (FLEET_ENV=staging + the staging URL) is refused by
#       BusConfig if the URL is a known prod IP literal, so a misconfigured drill
#       fails loud BEFORE it can publish (the #105 literal-IP tripwire). This is
#       DEFENSE-IN-DEPTH, not the isolation boundary — true isolation is distinct
#       staging credentials (see docs/f3-staging-shadow-hub.md).
#
# Exit semantics — SKIP is a DISTINCT code so a caller never mistakes "couldn't
# verify" for "verified":
#   0  e2e ran on staging + prod clean across the window   -> PASS
#   2  a required monitor/tool unreachable/absent          -> SKIP (cannot verify)
#   1  reachable but isolation VIOLATED or e2e did NOT run -> FAIL
#
# Required env (no defaults for the servers — a wrong default could point the
# drill at prod, which is the whole thing we are guarding against):
#   FLEET_STAGING_NATS_URL      nats:// URL of the SEPARATE staging server
#   FLEET_STAGING_MONITOR_URL   http:// monitor (/connz) of the staging server
#   FLEET_PROD_MONITOR_URL      http:// monitor (/connz) of the PROD server (fleet-hub)
# Optional:
#   FLEET_DRILL_ID              drill connection id suffix (default: staging-drill)
#   FLEET_DRILL_PYTHON          python that has fleet-nats + fleet-orchestrator +
#                               fleet-subscriber installed (default:
#                               /opt/fleet-bus/.venv/bin/python)
#   FLEET_DRILL_WORKER          worker agent id the dummy answers as (default: wren)
#   FLEET_DRILL_TIMEOUT         seconds to wait for the hub to reach DONE (default: 60)
set -u

STAGING_URL="${FLEET_STAGING_NATS_URL:-}"
STAGING_MONITOR="${FLEET_STAGING_MONITOR_URL:-}"
PROD_MONITOR="${FLEET_PROD_MONITOR_URL:-http://192.0.2.74:8223}"
DRILL_ID="${FLEET_DRILL_ID:-staging-drill}"
PYTHON_BIN="${FLEET_DRILL_PYTHON:-/opt/fleet-bus/.venv/bin/python}"
WORKER_ID="${FLEET_DRILL_WORKER:-wren}"
DRILL_TIMEOUT="${FLEET_DRILL_TIMEOUT:-60}"

# Connection names the exercise puts on the bus, all keyed by the drill id so the
# prod audit can look for THIS drill specifically (not just "any" connection).
HUB_NAME="fleet-orchestrator-hub-${DRILL_ID}"
WORKER_NAME="fleet-nats-${DRILL_ID}-${WORKER_ID}"

fail_config() { echo "CONFIG $1" >&2; exit 2; }
[ -n "$STAGING_URL" ] || fail_config "FLEET_STAGING_NATS_URL unset — refusing to guess (could be prod)."
[ -n "$STAGING_MONITOR" ] || fail_config "FLEET_STAGING_MONITOR_URL unset — cannot verify staging carried the drill."
[ -n "$PROD_MONITOR" ] || fail_config "FLEET_PROD_MONITOR_URL unset — cannot audit prod (a negative needs looking)."

# Known prod endpoints, mirrored from fleet_nats.config._PROD_ENDPOINTS_RAW. A
# staging URL that is actually a prod URL is the rejected label-without-isolation
# anti-pattern — refuse before publishing a single frame. This shell case is a
# fast pre-check on the COMMON literal + portless spellings; the canonicalizing
# BusConfig guard (config.py) is the real backstop and also catches the exotic
# numeric spellings (dword/hex/mapped-v6) that are impractical to enumerate here.
#
# ADOPT-A-NAME: if a prod DNS name is ever added to config.py's
# _PROD_ENDPOINTS_RAW, add it to this case list too. Note: neither guard closes
# the DNS-alias-to-prod case — that is the job of distinct staging credentials
# (docs/f3-staging-shadow-hub.md); these are literal-IP tripwires only.
case "${STAGING_URL%/}" in
    nats://192.0.2.74:4222 | nats://192.0.2.74 \
    | nats://127.0.0.1:4222 | nats://127.0.0.1 \
    | nats://localhost:4222 | nats://localhost \
    | "nats://[::1]:4222" | "nats://[::1]")
        echo "FAIL  FLEET_STAGING_NATS_URL=${STAGING_URL} is a KNOWN PROD URL — that is"
        echo "      label-without-isolation (fleet-bus #105). Point it at the staging box."
        exit 1
        ;;
esac

if [ ! -x "$PYTHON_BIN" ]; then
    echo "SKIP  drill python not found at ${PYTHON_BIN} (set FLEET_DRILL_PYTHON) — cannot run the"
    echo "      live exercise here. Provision per docs/f3-staging-shadow-hub.md, then re-run."
    exit 2
fi

echo "staging-isolation-drill: staging=${STAGING_URL}"
echo "  staging monitor=${STAGING_MONITOR}  prod monitor=${PROD_MONITOR}"
echo "  hub=${HUB_NAME}  worker=${WORKER_NAME}  timeout=${DRILL_TIMEOUT}s"

# curl the connz JSON; empty stdout + nonzero rc means unreachable (curl -f).
probe_connz() { curl -fsS --max-time 5 "$1/connz?limit=1024" 2>/dev/null; }

# grep a connz body for one of the drill's connection names.
connz_has_name() {
    # $1 = connz body, $2 = connection name
    printf '%s' "$1" | grep -qE "\"name\":[[:space:]]*\"$2\""
}

# Scratch files (some hosts lack mktemp -> fall back to a pid-suffixed name).
mk_tmp() { mktemp 2>/dev/null || echo "/tmp/fleet-drill-$1.$$"; }
DUMMY_LOG="$(mk_tmp dummy)"
HUB_LOG="$(mk_tmp hub)"
AUDIT_SENTINEL="$(mk_tmp sentinel)"
PROD_LEAK_FLAG="$(mk_tmp leak)"
PROD_GAP_FLAG="$(mk_tmp gap)"
rm -f "$PROD_LEAK_FLAG" "$PROD_GAP_FLAG"
export AUDIT_SENTINEL PROD_MONITOR HUB_NAME WORKER_NAME PROD_LEAK_FLAG PROD_GAP_FLAG

dummy_pid=""
audit_pid=""
# shellcheck disable=SC2329  # invoked indirectly via `trap ... EXIT INT TERM`
cleanup() {
    rm -f "$AUDIT_SENTINEL" 2>/dev/null
    [ -n "$audit_pid" ] && { kill "$audit_pid" 2>/dev/null; wait "$audit_pid" 2>/dev/null; }
    [ -n "$dummy_pid" ] && { kill "$dummy_pid" 2>/dev/null; wait "$dummy_pid" 2>/dev/null; }
    rm -f "$DUMMY_LOG" "$HUB_LOG" "$PROD_LEAK_FLAG" "$PROD_GAP_FLAG" 2>/dev/null
}
trap 'cleanup' EXIT INT TERM

# --- baseline: the prod audit MUST be establishable up front ----------------
# Without a prod baseline read we cannot prove absence — SKIP, never PASS.
if ! probe_connz "$PROD_MONITOR" >/dev/null; then
    echo "SKIP  prod monitor unreachable at ${PROD_MONITOR} — cannot PROVE zero prod traffic."
    echo "      Absence-of-leak is unverifiable without reading prod; refusing to pass."
    exit 2
fi
if ! probe_connz "$STAGING_MONITOR" >/dev/null; then
    echo "SKIP  staging monitor unreachable at ${STAGING_MONITOR} — cannot verify (off-network/not up)."
    exit 2
fi

# --- start a WORKER dummy on staging ----------------------------------------
# The worker answers dispatches so the hub's session can reach DONE. It is
# FLEET_ENV=staging, so BusConfig refuses to construct against a prod URL (the
# #105 guard) — this line is itself an isolation assertion. FLEET_ROLE=worker is
# explicit (worker is also the default) so it subscribes its dispatch/relay lanes.
FLEET_NATS_URL="$STAGING_URL" FLEET_ENV=staging \
    FLEET_AGENT_ID="$WORKER_ID" FLEET_NATS_NAME="$WORKER_NAME" \
    FLEET_ROLE=worker \
    "$PYTHON_BIN" -m fleet_subscriber.dummy >"$DUMMY_LOG" 2>&1 &
dummy_pid=$!

# --- durable prod audit: poll prod ACROSS the whole window ------------------
# A background poller reads prod /connz on a tight cadence for the drill window
# and records a violation the instant the drill name shows up, so a connection
# that touches prod and disconnects between snapshots is far less likely to hide.
: >"$AUDIT_SENTINEL"
export CONNZ_LIMIT="1024"
(
    while [ -f "$AUDIT_SENTINEL" ]; do
        body="$(curl -fsS --max-time 5 "${PROD_MONITOR}/connz?limit=${CONNZ_LIMIT}" 2>/dev/null)" || {
            # Prod went unreachable mid-window: the leak could hide in this gap.
            echo "gap" >"$PROD_GAP_FLAG"
            sleep 1
            continue
        }
        # Fail CLOSED on page overflow: NATS /connz reports "total" = ALL
        # connections on the server matching the filter, and "num_connections" =
        # how many were actually RETURNED in this (paginated) response. If
        # returned < total the page truncated and the drill connection could be
        # on an unseen page — we CANNOT prove absence, so record a gap
        # (audit → SKIP), never a clean pass. A returned count equal to the limit
        # is also treated as a possible truncation.
        total="$(printf '%s' "$body" | grep -oE "\"total\":[[:space:]]*[0-9]+" | grep -oE "[0-9]+" | head -n1)"
        returned="$(printf '%s' "$body" | grep -oE "\"num_connections\":[[:space:]]*[0-9]+" | grep -oE "[0-9]+" | head -n1)"
        if [ -n "$total" ] && [ -n "$returned" ] && [ "$returned" -lt "$total" ]; then
            echo "overflow total=${total} returned=${returned} limit=${CONNZ_LIMIT}" >"$PROD_GAP_FLAG"
        elif [ -n "$returned" ] && [ "$returned" -ge "$CONNZ_LIMIT" ]; then
            echo "overflow page-full returned=${returned} limit=${CONNZ_LIMIT}" >"$PROD_GAP_FLAG"
        fi
        # Match ANY returned connection whose name carries the drill id, not just
        # the two hub/worker spellings — a differently-named drill connection that
        # leaked to prod must still be caught.
        if printf '%s' "$body" | grep -qE "\"name\":[[:space:]]*\"[^\"]*${DRILL_ID}[^\"]*\""; then
            echo "leak" >"$PROD_LEAK_FLAG"
        fi
        sleep 1
    done
) &
audit_pid=$!

# Give the worker a moment to connect + subscribe its staging lanes.
sleep 4

# --- drive ONE real orchestrator session against staging --------------------
# --once + --task-text needs no k-board card; the hub dispatches to the worker,
# the worker relays status + the done sentinel, and the hub logs `reached DONE`.
# --dry-run keeps beats off Discord. FLEET_WORKERS scopes the session to our
# worker so the dispatch lands on the dummy. The outer `timeout` bounds the run
# (the hub also self-bounds its own --once DONE wait).
echo "staging-isolation-drill: driving one --once session (up to ${DRILL_TIMEOUT}s) ..."
FLEET_NATS_URL="$STAGING_URL" FLEET_ENV=staging \
    FLEET_AGENT_ID=heron FLEET_MANAGER_ID=heron \
    FLEET_WORKERS="$WORKER_ID" FLEET_NATS_NAME="$HUB_NAME" \
    timeout "$DRILL_TIMEOUT" \
    "$PYTHON_BIN" -m fleet_orchestrator --once --dry-run \
        --task-id "$DRILL_ID" \
        --task-text "staging isolation drill ${DRILL_ID} — no-op probe" \
    >"$HUB_LOG" 2>&1
hub_rc=$?

# Positive staging read while things may still be settling: a staging /connz that
# should show the drill connections.
staging_connz="$(probe_connz "$STAGING_MONITOR")" || staging_connz=""

# Let the audit loop take a final couple of polls, then stop it.
sleep 2
rm -f "$AUDIT_SENTINEL" 2>/dev/null
kill "$audit_pid" 2>/dev/null
wait "$audit_pid" 2>/dev/null
audit_pid=""

# --- evaluate ---------------------------------------------------------------
rc=0

# (a) e2e: the hub must have reached DONE on staging subjects.
if grep -qE "task=${DRILL_ID} reached DONE" "$HUB_LOG"; then
    echo "PASS  hub reached DONE for task=${DRILL_ID} — dispatch/relay/done ran on staging.*"
else
    echo "FAIL  hub did NOT reach DONE (rc=${hub_rc}) — the shadow hub did not run the"
    echo "      end-to-end exercise (hub down, worker absent, or session timed out)."
    echo "      --- last hub log lines ---"
    tail -n 15 "$HUB_LOG" 2>/dev/null | sed 's/^/      /'
    rc=1
fi

# (b) positive staging presence (belt-and-suspenders).
if connz_has_name "$staging_connz" "$HUB_NAME" || connz_has_name "$staging_connz" "$WORKER_NAME"; then
    echo "PASS  drill connection present on STAGING /connz — staging bus carried the drill."
else
    echo "WARN  drill connections not seen on STAGING /connz snapshot (may have already"
    echo "      closed after DONE); the reached-DONE proof above is authoritative."
fi

# (c) durable prod audit result.
if [ -f "$PROD_GAP_FLAG" ]; then
    echo "SKIP  prod audit had a GAP during the window — a leak could hide in it."
    echo "      $(cat "$PROD_GAP_FLAG" 2>/dev/null)"
    echo "      Either prod went unreachable or /connz overflowed its page (returned <"
    echo "      total, or page full at limit): cannot prove zero prod traffic; refusing to pass."
    exit 2
fi
if [ -f "$PROD_LEAK_FLAG" ]; then
    echo "FAIL  drill connection FOUND on PROD /connz during the window — LEAKED to prod."
    echo "      ISOLATION BROKEN."
    rc=1
else
    echo "PASS  drill ABSENT from PROD /connz across the whole window — zero prod traffic."
fi

if [ "$rc" -eq 0 ]; then
    echo "staging-isolation-drill: PASS — real exercise ran on staging, prod untouched (#105 acceptance)."
fi
exit "$rc"
