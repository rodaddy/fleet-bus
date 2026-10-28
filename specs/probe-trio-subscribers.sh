#!/usr/bin/env bash
# Phase 7 (#8) effective-state probe: confirm each trio agent's NATS subscriber
# is actually CONNECTED to the bus — verified by probing the RUNNING system, not
# by trusting a config file.
#
# It queries the NATS server's monitoring endpoint (/connz) for each agent's
# connection name (fleet-nats-<agent>, set by BusConfig.from_env) and reports
# per-agent connected / missing. A connected subscriber is proof of effective
# state; a config file on disk is not.
#
# The allow_bots != none half of the check is a per-host effective-state read
# (the running gateway's resolved config), which requires host access through the
# front door — it is NOT asserted from here (a bus probe cannot see a gateway's
# allow_bots). This script asserts the bus-connection half; the runbook's enable
# procedure covers the allow_bots verification at apply time. See docs/runbook.md.
#
# Exit semantics — SKIP is a DISTINCT code so a caller/checker never mistakes
# "couldn't verify" for "verified" (a SKIP must never tally as PASS):
#   0  all three subscribers connected              -> PASS
#   2  monitoring endpoint unreachable              -> SKIP (off-network/not deployed)
#   1  endpoint reachable but >=1 agent NOT connected -> FAIL (real regression)
#
# Host table (confirm live before deploy — tables drift):
#   Finch  finch-host   (worker)
#   Wren wren-host   (worker)
#   Heron heron-host  (manager)
set -uo pipefail

# Monitoring endpoint: NATS exposes /connz on its HTTP monitor port. On fleet-hub
# that is 8223 (http_port in /home/fleet/nats/nats-server.conf) — NOT the
# conventional 8222, which this server binds as its WEBSOCKET listener. Probing
# 8222 gets HTTP 400 "Bad Request" from the ws handshake, which curl -f treats
# as failure -> a permanent false SKIP. Verified live on the host 2026-07-07.
NATS_MONITOR_URL="${FLEET_NATS_MONITOR_URL:-http://192.0.2.74:8223}"
AGENTS=(finch wren heron)

echo "probe-trio-subscribers: querying ${NATS_MONITOR_URL}/connz"

# Pull the current connections. If the monitor is unreachable, this is off-network
# or not-yet-deployed -> SKIP (exit 2, per the header table), not a false FAIL.
connz="$(curl -fsS --max-time 5 "${NATS_MONITOR_URL}/connz?limit=1024" 2>/dev/null)" || {
    echo "SKIP  NATS monitor unreachable at ${NATS_MONITOR_URL} (off-network or not deployed yet)."
    echo "      Effective-state probe cannot run here; enable per docs/runbook.md, then re-run."
    echo "      (exit 2 = SKIP, NOT acceptance — only an exit-0 PASS is Phase-7 live evidence.)"
    exit 2
}

missing=0
for agent in "${AGENTS[@]}"; do
    # The subscriber's connection name is fleet-nats-<agent> (BusConfig.from_env,
    # the default — do NOT set FLEET_NATS_NAME, which would defeat this match).
    # Anchor on the JSON "name":"fleet-nats-<agent>" field so an unrelated field
    # (client IP, subject) that happened to contain the string can't false-match,
    # and a sibling like fleet-nats-<agent>2 can't substring-match this agent.
    # Here-string, NOT `printf | grep -q`: under pipefail, grep -q exiting at
    # first match can SIGPIPE the printf (rc 141) and score a CONNECTED agent
    # as missing once /connz outgrows the pipe buffer.
    if grep -qE "\"name\":[[:space:]]*\"fleet-nats-${agent}\"" <<<"$connz"; then
        echo "PASS  ${agent}: subscriber connected (fleet-nats-${agent} present on the bus)."
    else
        echo "FAIL  ${agent}: no fleet-nats-${agent} connection on the bus (not enabled)."
        missing=$((missing + 1))
    fi
done

if [ "$missing" -ne 0 ]; then
    echo "probe-trio-subscribers: ${missing} agent(s) not connected — trio NOT fully enabled."
    exit 1
fi
echo "probe-trio-subscribers: all three subscribers connected."
exit 0
