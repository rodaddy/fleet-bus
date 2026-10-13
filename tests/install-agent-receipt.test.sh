#!/usr/bin/env bash
# Regression for the install-agent release receipt (#225).
#
# The receipt is what makes package_version vary across hosts; without it the
# heartbeat reports null and install drift is invisible — the exact failure
# #203/#218/#225 exist to prevent. These cases pin the sha RESOLUTION contract,
# which is where the review found the defects:
#
#   1. the documented tarball path (no .git) must still stamp a receipt
#   2. a foreign ancestor repo's HEAD must never be stamped
#   3. a dirty checkout must not claim a clean sha
#   4. an unremovable stale receipt must not abort the install
#
# The installer's venv/pip work is NOT exercised: this drives only the receipt
# block, so the test needs no network, no uv and no python 3.13. All disposable
# state stays in the configured fleet-bus temp workspace, never /tmp.
set -euo pipefail

repo_root="$(cd "$(dirname "$0")/.." && pwd)"
subject="$repo_root/scripts/install-agent.sh"
runbook="$repo_root/docs/runbook.md"
test_root="${FLEET_BUS_TEST_TMP:-$HOME/.cache/fleet-bus-tests/install-agent-receipt-tests}"
# Two sanctioned roots: the local temp workspace, and — on a CI runner — a
# subdir of $RUNNER_TEMP, which IS the runner's disposable workspace. Guard the
# runner arm on a NON-EMPTY $RUNNER_TEMP: with it unset the pattern would
# collapse to `/fleet-bus/*` and quietly bless a literal /fleet-bus path outside
# any workspace. Each arm must end in a command that cannot fail under `set -e`
# — a bare `[ ... ]` as the last command of a case arm exits the script with
# status 1 and PRINTS NOTHING, which is a CI failure with no diagnostic.
runner_temp="${RUNNER_TEMP:-}"
case "$test_root" in
    "$HOME"/.cache/fleet-bus-tests/*) ;;
    *)
        if [ -n "$runner_temp" ] && [ "${test_root#"$runner_temp"/fleet-bus/}" != "$test_root" ]; then
            :
        else
            echo "FAIL: FLEET_BUS_TEST_TMP ($test_root) must be inside the configured"
            echo "      fleet-bus temp workspace ($HOME/.cache/fleet-bus-tests/*)"
            echo "      or, on a CI runner, \$RUNNER_TEMP/fleet-bus/*"
            exit 1
        fi
        ;;
esac
mkdir -p "$test_root/_archive"
work="$test_root/run-$$"
mkdir -p "$work"

cleanup() {
    chmod -R u+w "$work" 2>/dev/null || true
    mv "$work" "$test_root/_archive/run-$(date -u +%Y%m%dT%H%M%SZ)-$$" 2>/dev/null || true
}
trap cleanup EXIT

fail() { echo "FAIL: $*"; exit 1; }

# Extract the receipt block from the real installer and run it in isolation,
# so this tests the SHIPPED code rather than a copy that can drift from it.
receipt_block="$work/receipt-block.sh"
{
    printf '%s\n' '#!/usr/bin/env bash'
    printf '%s\n' 'set -euo pipefail'
    printf '%s\n' 'log() { printf "install-agent | %s\n" "$*"; }'
    # shellcheck disable=SC2016  # emitting shell source, not expanding here
    printf '%s\n' 'src="$1"; dest="$2"'
    awk '/^# ---- release receipt \(#225\)/,/^# ---- venv \+ editable installs/' "$subject" \
        | sed '$d'
} >"$receipt_block"
grep -F '.fleet-release-sha' "$receipt_block" >/dev/null \
    || fail "could not extract the receipt block from $subject"

# A fleet-bus-shaped source tree, with NO .git — exactly what the documented
# tarball (docs/runbook.md, --exclude '.git') lands on an agent host.
make_src() {
    local dir="$1"
    mkdir -p "$dir/packages/fleet-nats" "$dir/packages/fleet-subscriber" "$dir/scripts"
    : >"$dir/pyproject.toml"
}

run_receipt() {
    local src="$1" dest="$2"
    shift 2
    mkdir -p "$dest"
    env "$@" bash "$receipt_block" "$src" "$dest" 2>&1
}

# --- 1. the documented agent-host path: no .git, sha supplied by the shipper ---
# THE #225 case. Before the fix this branch could only consult `git rev-parse`
# in a tree that by construction has no git metadata, so every agent host
# stamped nothing and reported package_version=null.
sha1="a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0"
src1="$work/tarball-src"; dest1="$work/tarball-dest"
make_src "$src1"
out="$(run_receipt "$src1" "$dest1" FLEET_RELEASE_SHA="$sha1")"
[ -f "$dest1/.fleet-release-sha" ] \
    || fail "no receipt written on the documented tarball path: $out"
[ "$(cat "$dest1/.fleet-release-sha")" = "$sha1" ] \
    || fail "receipt content wrong on the tarball path: $(cat "$dest1/.fleet-release-sha")"

# --- 1b. a receipt already in the tree survives the tarball and is honored ----
sha1b="0f1e2d3c4b5a60718293a4b5c6d7e8f90a1b2c3d"
src1b="$work/shipped-src"; dest1b="$work/shipped-dest"
make_src "$src1b"
printf '%s\n' "$sha1b" >"$src1b/.fleet-release-sha"
out="$(run_receipt "$src1b" "$dest1b")"
[ "$(cat "$dest1b/.fleet-release-sha")" = "$sha1b" ] \
    || fail "an in-tree receipt was not carried to the install: $out"

# --- 1c. the DOCUMENTED tar command must actually carry the root receipt ------
# Cross-runtime review: 1b creates the receipt directly in the post-tar src, so
# it proves the installer HONORS an in-tree receipt but never that the receipt
# survives TRANSPORT. The documented archive selects `packages scripts
# pyproject.toml`; a root dotfile is inside none of them, so the old command
# shipped no receipt at all and the host silently depended on FLEET_RELEASE_SHA.
# This runs the real archive selection from docs/runbook.md.
sha1c="1a2b3c4d5e6f708192a3b4c5d6e7f8091a2b3c4d"
src1c="$work/tar-transport-src"; hop1c="$work/tar-transport-hop"
make_src "$src1c"
printf '%s\n' "$sha1c" >"$src1c/.fleet-release-sha"
mkdir -p "$hop1c"
( cd "$src1c" && tar czf - --exclude '.venv' --exclude '__pycache__' --exclude '.git' \
    packages scripts pyproject.toml .fleet-release-sha ) | tar xzf - -C "$hop1c"
[ -f "$hop1c/.fleet-release-sha" ] \
    || fail "the documented tar did not carry the root receipt — package_version degrades to null on the host"
[ "$(cat "$hop1c/.fleet-release-sha")" = "$sha1c" ] \
    || fail "the receipt was corrupted in transport: $(cat "$hop1c/.fleet-release-sha")"
# ...and the installer honors what actually arrived, with NO env override.
dest1c="$work/tar-transport-dest"
out="$(run_receipt "$hop1c" "$dest1c")"
[ "$(cat "$dest1c/.fleet-release-sha")" = "$sha1c" ] \
    || fail "the transported receipt was not honored by the install: $out"

# --- 2. a foreign ancestor repo must never be stamped -------------------------
# `git rev-parse HEAD` walks up to ANY ancestor .git, so untarring under some
# unrelated checkout would publish that repo's HEAD as the fleet-bus release —
# a confident, wrong identity that a drift dashboard cannot distinguish from
# real drift.
outer="$work/outer-repo"
mkdir -p "$outer"
git -C "$outer" init -q .
git -C "$outer" config user.email fleet-bus-test@example.invalid
git -C "$outer" config user.name "fleet-bus test"
: >"$outer/unrelated.txt"
git -C "$outer" add unrelated.txt
git -C "$outer" -c commit.gpgsign=false commit -q -m "unrelated outer repo"
foreign_sha="$(git -C "$outer" rev-parse HEAD)"
src2="$outer/nested/fleet-bus-src"; dest2="$work/foreign-dest"
make_src "$src2"
out="$(run_receipt "$src2" "$dest2")"
if [ -f "$dest2/.fleet-release-sha" ]; then
    got="$(cat "$dest2/.fleet-release-sha")"
    [ "$got" != "$foreign_sha" ] \
        || fail "stamped an unrelated ancestor repo's HEAD as the fleet-bus release ($got)"
    fail "stamped a receipt from a non-fleet-bus repo: $got"
fi
printf '%s' "$out" | grep -F 'no usable sha' >/dev/null \
    || fail "no warning when the sha was unresolvable: $out"

# --- 3. a dirty checkout must not claim a clean sha ---------------------------
# The installer syncs the WORKING tree, not `git archive`, so a dirty checkout's
# HEAD does not describe the installed bytes: two hosts installed from the same
# checkout at different dirty states would report an identical package_version
# while running different code.
src3="$work/dirty-src"; dest3="$work/dirty-dest"
make_src "$src3"
git -C "$src3" init -q .
git -C "$src3" config user.email fleet-bus-test@example.invalid
git -C "$src3" config user.name "fleet-bus test"
git -C "$src3" add pyproject.toml
git -C "$src3" -c commit.gpgsign=false commit -q -m "clean baseline"
clean_sha="$(git -C "$src3" rev-parse HEAD)"

out="$(run_receipt "$src3" "$dest3")"
[ "$(cat "$dest3/.fleet-release-sha")" = "$clean_sha" ] \
    || fail "a clean checkout must stamp its HEAD: $out"

printf 'uncommitted drift\n' >"$src3/pyproject.toml"
dest3b="$work/dirty-dest-2"
out="$(run_receipt "$src3" "$dest3b")"
if [ -f "$dest3b/.fleet-release-sha" ]; then
    fail "a dirty checkout stamped $(cat "$dest3b/.fleet-release-sha") — it does not describe the tree"
fi
printf '%s' "$out" | grep -F 'dirty checkout' >/dev/null \
    || fail "no dirty-checkout warning: $out"

# --- 3c. UNTRACKED source bytes make the tree dirty too -----------------------
# Cross-runtime review: the gate used --untracked-files=no while the sync copies
# the working tree wholesale. So an untracked module under packages/ was
# DEPLOYED and imported by the editable install, contributed nothing to HEAD,
# and the installer still stamped HEAD — a confident identity for bytes HEAD
# does not describe, which is the exact drift #225 exists to expose.
src3c="$work/untracked-src"; dest3c="$work/untracked-dest"
make_src "$src3c"
git -C "$src3c" init -q .
git -C "$src3c" -c user.email=t@t -c user.name=t add -A
git -C "$src3c" -c user.email=t@t -c user.name=t -c commit.gpgsign=false \
    commit -q -m "clean baseline"
# An untracked module the installer WILL copy onto the host.
printf 'print("shipped but not committed")\n' \
    >"$src3c/packages/fleet-subscriber/untracked_module.py"
out="$(run_receipt "$src3c" "$dest3c")"
if [ -f "$dest3c/.fleet-release-sha" ]; then
    fail "an untracked-dirty checkout stamped $(cat "$dest3c/.fleet-release-sha") — it does not describe the deployed bytes"
fi
printf '%s' "$out" | grep -F 'dirty checkout' >/dev/null \
    || fail "no dirty-checkout warning for untracked source bytes: $out"

# --- 3b. a malformed sha is refused rather than published ---------------------
for junk in "feature/my-branch" "not-a-sha" "ABCDEF1" "abc12"; do
    dest_j="$work/junk-dest-$(printf '%s' "$junk" | tr -c 'a-zA-Z0-9' '_')"
    src_j="$work/junk-src"
    make_src "$src_j"
    out="$(run_receipt "$src_j" "$dest_j" FLEET_RELEASE_SHA="$junk")"
    [ ! -f "$dest_j/.fleet-release-sha" ] \
        || fail "stamped malformed sha '$junk' as a release identity"
    printf '%s' "$out" | grep -F 'malformed sha' >/dev/null \
        || fail "no malformed-sha warning for '$junk': $out"
done

# --- 4. an unremovable stale receipt must not abort the install ---------------
# `rm -f` does NOT suppress permission errors, so under `set -euo pipefail` this
# cleanup would kill the run BEFORE the venv/pip upgrade — leaving the host on
# old code because a cosmetic step failed.
src4="$work/rm-src"; dest4="$work/rm-dest"
make_src "$src4"
mkdir -p "$dest4"
printf 'staleaa\n' >"$dest4/.fleet-release-sha"
chmod a-w "$dest4"
set +e
out="$(run_receipt "$src4" "$dest4" 2>&1)"
rc=$?
set -e
chmod u+w "$dest4"
[ "$rc" -eq 0 ] \
    || fail "an unremovable stale receipt aborted the install (exit $rc): $out"
printf '%s' "$out" | grep -F 'could not remove stale receipt' >/dev/null \
    || fail "no warning for the unremovable receipt: $out"

# --- documentation contract ---------------------------------------------------
# The one documented agent-host path must actually carry the sha, or operators
# reproduce finding 1 by following the runbook verbatim.
grep -F 'FLEET_RELEASE_SHA' "$runbook" >/dev/null \
    || fail "docs/runbook.md never mentions FLEET_RELEASE_SHA"
# shellcheck disable=SC2016  # a literal the runbook must contain, not an expansion
grep -F 'FLEET_RELEASE_SHA=$sha FLEET_BUS_SRC=/opt/fleet-bus/src' "$runbook" >/dev/null \
    || fail "the runbook install command does not thread FLEET_RELEASE_SHA"
grep -F '.fleet-release-sha' "$runbook" >/dev/null \
    || fail "the runbook never names the receipt file"

echo "install-agent-receipt.test: PASS — tarball path stamps, foreign/dirty/malformed shas refused, cleanup non-fatal, runbook threads the sha"
