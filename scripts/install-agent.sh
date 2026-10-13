#!/usr/bin/env bash
# Self-provisioning installer for a fleet-bus agent host (#8).
#
# "Agents install what they need when they need it": run this ON the agent host
# (or ship it there) and it provisions everything the subscriber needs — a
# dedicated venv with fleet-nats + fleet-subscriber installed — idempotently,
# with zero interactive steps. Re-running upgrades in place (update-survival).
#
# Source resolution, in order (first match wins):
#   1. FLEET_BUS_SRC        — explicit path to a fleet-bus source tree
#   2. /srv/shared/fleet-bus — the canonical shared-mount location
#   3. the repo this script lives in       — running from a checkout
#
# Install target: FLEET_BUS_HOME (default /opt/fleet-bus)
#   $FLEET_BUS_HOME/src    — the synced source tree
#   $FLEET_BUS_HOME/.venv  — the uv venv with both packages installed
#
# Requirements on the host: python3.13, uv, rsync-or-tar. No network needed when
# installing from a local source tree (private repo, no GitHub creds required).
# No secrets touched; nothing here reads or writes credentials.
set -euo pipefail

FLEET_BUS_HOME="${FLEET_BUS_HOME:-/opt/fleet-bus}"
CANONICAL_SRC="/srv/shared/fleet-bus"

log() { printf 'install-agent | %s\n' "$*"; }

# ---- resolve the source tree -------------------------------------------------
script_dir="$(cd "$(dirname "$0")" && pwd)"
repo_of_script="$(cd "$script_dir/.." && pwd)"

if [ -n "${FLEET_BUS_SRC:-}" ]; then
    src="$FLEET_BUS_SRC"
elif [ -d "$CANONICAL_SRC/packages/fleet-nats" ]; then
    src="$CANONICAL_SRC"
elif [ -d "$repo_of_script/packages/fleet-nats" ]; then
    src="$repo_of_script"
else
    log "ERROR: no fleet-bus source found (set FLEET_BUS_SRC, populate $CANONICAL_SRC, or run from a checkout)"
    exit 1
fi
# Validate the chosen source is COMPLETE before any copying starts — everything
# the sync needs must exist, or we fail here with a clear message instead of
# aborting mid-rsync/tar with a cryptic error and a partial dest.
for need in packages/fleet-nats packages/fleet-subscriber scripts pyproject.toml; do
    if [ ! -e "$src/$need" ]; then
        log "ERROR: source $src is missing $need — not a usable fleet-bus tree"
        exit 1
    fi
done
log "source: $src"

# ---- prerequisites (fail loud, name the fix) ----------------------------------
command -v uv >/dev/null 2>&1 || { log "ERROR: uv not found — install uv first"; exit 1; }
uv python find 3.13 >/dev/null 2>&1 || { log "ERROR: python 3.13 not available to uv"; exit 1; }

# ---- sync source into FLEET_BUS_HOME/src (skip when installing in place) ------
mkdir -p "$FLEET_BUS_HOME"
dest="$FLEET_BUS_HOME/src"
# Compare RESOLVED paths: a trailing slash or symlink alias of dest must hit the
# skip too, or the tar branch would extract over the very tree it is archiving.
src="$(cd "$src" && pwd)"
mkdir -p "$dest"
if [ "$src" = "$(cd "$dest" && pwd)" ]; then
    log "source already in place at $dest (skipping sync)"
else
    # The root pyproject.toml MUST ride along: the packages declare
    # fleet-nats = { workspace = true }, which only resolves when the uv
    # workspace root sits above them in the synced tree.
    if command -v rsync >/dev/null 2>&1; then
        rsync -a --delete \
            --exclude '.venv' --exclude '__pycache__' --exclude '.git' \
            "$src/packages" "$src/scripts" "$dest/"
        rsync -a "$src/pyproject.toml" "$dest/"
    else
        # tar pipe, not cp -R: the source tree may hold per-package .venv dirs
        # (a dev checkout) — copying those would drag gigabytes onto the host.
        # Match rsync --delete semantics: drop the previous synced dirs first so
        # an upstream-removed module cannot survive an "upgrade" as a stale file
        # the editable install would still import.
        rm -rf "$dest/packages" "$dest/scripts"
        (cd "$src" && tar -cf - --exclude '.venv' --exclude '__pycache__' --exclude '.git' \
            packages scripts pyproject.toml) | tar -xf - -C "$dest"
    fi
    log "synced source -> $dest"
fi

# ---- release receipt (#225) ---------------------------------------------------
# Stamp the installed sha where the runtime can read it. The heartbeat's
# package_version is built from this (fleet_subscriber/heartbeat.py); WITHOUT it
# the field is None, and with a WRONG one it is worse than None — a confident
# identity that does not describe the bytes on disk. #218 shipped a constant
# 0.1.0 that could not show drift; a mis-stamped sha would show fake drift.
#
# Mirrors the hub's own receipt name so both deploy paths agree; the runtime
# resolves it by walking up from the installed package, so it must sit at the
# tree root the packages were installed from.
#
# Three sha sources, first match wins. Order matters: the two that survive a
# `--exclude '.git'` tarball come FIRST, because that tarball is the documented
# agent-host path (docs/runbook.md) and a git-only source would make the receipt
# unstampable on exactly the hosts #225 exists to fix.
#   1. FLEET_RELEASE_SHA        — the shipping side states the identity
#   2. $src/.fleet-release-sha  — a receipt already in the tree (rides the tar)
#   3. git HEAD of $src         — a live checkout, and only a CONTAINED one
sha=""
sha_source=""

# --- 1. explicit, from the operator/shipper ---
if [ -n "${FLEET_RELEASE_SHA:-}" ]; then
    sha="$FLEET_RELEASE_SHA"
    sha_source="FLEET_RELEASE_SHA"
# --- 2. a receipt shipped inside the source tree ---
elif [ -f "$src/.fleet-release-sha" ]; then
    sha="$(tr -d '[:space:]' <"$src/.fleet-release-sha")"
    sha_source="$src/.fleet-release-sha"
# --- 3. a live git checkout ---
# Containment matters: bare `git rev-parse HEAD` walks up to ANY ancestor .git,
# so an untar into a path that merely sits under some other repo would stamp
# that foreign repo's HEAD as the fleet-bus release. Require $src to be the
# work-tree ROOT, mirroring deploy-hub-release.sh's --is-inside-work-tree guard.
elif [ "$(git -C "$src" rev-parse --show-toplevel 2>/dev/null)" = "$src" ]; then
    # A dirty tree does not describe the installed bytes: the sync below copies
    # the WORKING tree, not `git archive`, so two hosts installed from the same
    # checkout at different dirty states would report an identical sha while
    # running different code — the drift this field exists to expose, hidden.
    # deploy-hub-release.sh dies on dirty; the agent path degrades to unknown.
    #
    # UNTRACKED FILES COUNT. The sync above copies the working tree wholesale,
    # so an untracked module under packages/ is deployed and imported by the
    # editable install while contributing nothing to HEAD. Ignoring untracked
    # here would stamp HEAD onto bytes HEAD does not describe — the same
    # confident-but-wrong identity the tracked-dirty check already refuses.
    # Matches deploy-hub-release.sh's --untracked-files=all cleanliness gate.
    if [ -n "$(git -C "$src" status --porcelain=v1 --untracked-files=all 2>/dev/null)" ]; then
        log "WARNING: $src is a dirty checkout (tracked or untracked changes) — refusing to stamp a sha that does not describe it"
    else
        sha="$(git -C "$src" rev-parse HEAD 2>/dev/null || true)"
        sha_source="git HEAD of $src"
    fi
fi

# A junk sha is worse than no sha: it looks like a real release identity and a
# drift dashboard cannot tell it from genuine drift. Enforce the same shape the
# hub already enforces (deploy-hub-release.sh, verify-hub-release.py).
if [ -n "$sha" ] && ! printf '%s' "$sha" | grep -Eq '^[0-9a-f]{7,40}$'; then
    log "WARNING: ignoring malformed sha from $sha_source (not a 7-40 char hex commit sha)"
    sha=""
fi

receipt="$dest/.fleet-release-sha"
if [ -n "$sha" ]; then
    printf '%s\n' "$sha" >"$receipt"
    log "stamped release receipt $receipt ($sha, from $sha_source)"
else
    # Best-effort by design: a tree with no usable sha still installs and
    # reports an unknown version rather than a fake one. `rm -f` does NOT
    # suppress permission errors, so under `set -e` a stale unremovable receipt
    # would abort the install BEFORE the venv/pip upgrade below — turning a
    # cosmetic cleanup into a hard failure that leaves the host on old code.
    if ! rm -f "$receipt" 2>/dev/null; then
        log "WARNING: could not remove stale receipt $receipt — package_version may report a stale sha"
    fi
    log "WARNING: no usable sha for $src — package_version will report unknown."
    log "         Set FLEET_RELEASE_SHA=<sha> (see docs/runbook.md) to make install drift observable."
fi

# ---- venv + editable installs (idempotent; re-run = upgrade) -------------------
venv="$FLEET_BUS_HOME/.venv"
if [ ! -x "$venv/bin/python" ]; then
    # Crash recovery: a dir left by an interrupted run (no bin/python) would make
    # `uv venv` refuse forever — clear it before creating. Scope-guarded: only
    # ever removes $FLEET_BUS_HOME/.venv, and only when it is NOT a working venv.
    [ -d "$venv" ] && rm -rf "$venv"
    uv venv --python 3.13 "$venv"
    log "created venv $venv"
fi
uv pip install --python "$venv/bin/python" -q \
    -e "$dest/packages/fleet-nats" \
    -e "$dest/packages/fleet-subscriber"
log "installed fleet-nats + fleet-subscriber into $venv"

# ---- verify (effective state, not assumption) ----------------------------------
# Verifies the INSTALL (both packages import). Role display is best-effort: a
# bad FLEET_ROLE in the host env must not fail a successful install — the role
# is a runtime concern, validated (loudly) when the subscriber actually starts.
"$venv/bin/python" - <<'PY'
import fleet_nats, fleet_subscriber
from fleet_subscriber import SubscriberRole
try:
    role = SubscriberRole.from_env().value
except ValueError as exc:
    role = f"INVALID ({exc})"
print(f"install-agent | verified fleet_nats {fleet_nats.__version__} "
      f"fleet_subscriber {fleet_subscriber.__version__} "
      f"default_role={role}")
PY
log "done. Set FLEET_NATS_URL / FLEET_AGENT_ID / FLEET_ENV / FLEET_ROLE per docs/runbook.md."
