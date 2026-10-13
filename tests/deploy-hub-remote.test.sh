#!/usr/bin/env bash
# Functional boundary tests for runner-to-fleet-hub immutable source transfer.
set -euo pipefail

repo_root="$(cd "$(dirname "$0")/.." && pwd)"
subject="$repo_root/scripts/deploy-hub-remote.sh"
test_root="${FLEET_BUS_TEST_TMP:-$HOME/.cache/fleet-bus-tests/deploy-hub-remote-tests}"
runner_temp_env="${RUNNER_TEMP:-}"
case "$test_root" in
    "$HOME"/.cache/fleet-bus-tests/*) ;;
    "$runner_temp_env"/fleet-bus/*) [ "${FLEET_BUS_TEST_ALLOW_RUNNER_TEMP:-0}" = 1 ] ;;
    *) echo "FAIL: FLEET_BUS_TEST_TMP must be inside the configured fleet-bus temp workspace"; exit 1 ;;
esac

mkdir -p "$test_root/_archive"
work="$test_root/run-$$"
fixture="$work/source"
home="$work/remote-home"
runner_temp="$work/runner-temp"
fake_bin="$work/bin"
mkdir -p "$fixture/scripts" "$home/incoming" "$runner_temp" "$fake_bin" "$work/no-hooks"

cleanup() {
    chmod -R u+w "$work" 2>/dev/null || true
    mv "$work" "$test_root/_archive/run-$(date -u +%Y%m%dT%H%M%SZ)-$$" 2>/dev/null || true
}
trap cleanup EXIT

cat >"$fixture/scripts/deploy-hub-release.sh" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
[ "${TEST_REMOTE_CONTEXT:-0}" = 1 ] || { echo "release executed outside remote context" >&2; exit 1; }
head="$(git -C "$FLEET_BUS_SRC" rev-parse HEAD)"
status="$(git -C "$FLEET_BUS_SRC" status --porcelain=v1 --untracked-files=all)"
[ -z "$status" ] || { echo "release received dirty source" >&2; exit 1; }
printf '%s\n%s\n%s\nremote\n' "$DEPLOY_SHA" "$head" "$FLEET_BUS_SRC" >"$FLEET_BUS_HOME/release-call"
EOF
chmod +x "$fixture/scripts/deploy-hub-release.sh"
git -C "$fixture" init -q
git -C "$fixture" config user.name 'fleet-bus test'
git -C "$fixture" config user.email 'fleet-bus-test@example.invalid'
git -C "$fixture" config core.hooksPath "$work/no-hooks"
git -C "$fixture" config commit.gpgsign false
git -C "$fixture" add scripts/deploy-hub-release.sh
git -C "$fixture" commit -qm 'fixture release'
deploy_sha="$(git -C "$fixture" rev-parse HEAD)"
wrong_sha="1111111111111111111111111111111111111111"

cat >"$fake_bin/scp" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
[ "${1:-}" = -o ] || exit 2
[ "${2:-}" = BatchMode=yes ] || exit 2
shift 2
[ "${1:-}" = -- ] && shift
[ "$#" -eq 2 ] || exit 2
source_path="$1"
case "$2" in
    fleet-hub-deploy:/opt/fleet-bus/incoming/*)
        destination="$TEST_REMOTE_HOME/incoming/${2##*/}"
        ;;
    *) exit 2 ;;
esac
cp "$source_path" "$destination"
[ "${TEST_SCP_FAIL_AFTER_COPY:-0}" != 1 ] || exit 1
EOF
cat >"$fake_bin/ssh" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
[ "${1:-}" = -o ] || exit 2
[ "${2:-}" = BatchMode=yes ] || exit 2
shift 2
host="$1"
shift
[ "$host" = fleet-hub-deploy ] || exit 2
printf '%s\n' "$host" >>"$TEST_SSH_LOG"
args=("$@")
last_index=$((${#args[@]} - 1))
case "${args[$last_index]}" in
    /opt/fleet-bus/incoming/*)
        args[$last_index]="$TEST_REMOTE_HOME/incoming/${args[$last_index]##*/}"
        ;;
    *) exit 2 ;;
esac
if [ "${TEST_SSH_FAIL_BEFORE_EXEC:-0}" = 1 ]; then
    for argument in "${args[@]}"; do
        [ "$argument" != --remote ] || exit 1
    done
fi
TEST_REMOTE_CONTEXT=1 FLEET_BUS_HOME="$TEST_REMOTE_HOME" "${args[@]}"
EOF
chmod +x "$fake_bin/scp" "$fake_bin/ssh"

assert_clean_artifacts() {
    if find "$home/incoming" -mindepth 1 -print -quit | grep -q .; then
        echo "FAIL: incoming bundle was not cleaned"
        exit 1
    fi
    if find "$home/sources" -maxdepth 1 \( -name '.staging-*' -o -name '.verify-*' \) -print -quit | grep -q .; then
        echo "FAIL: staging artifact was not cleaned"
        exit 1
    fi
}

make_bundle() {
    local path="$1"
    git -C "$fixture" update-ref refs/fleet-bus/deploy "$deploy_sha" ""
    git -C "$fixture" bundle create "$path" refs/fleet-bus/deploy
    git -C "$fixture" update-ref -d refs/fleet-bus/deploy
}

# Runner mode crosses the fake SSH boundary. The fixture release refuses to run
# without the remote-only context, making local runner execution observable.
(
    cd "$fixture"
    PATH="$fake_bin:$PATH" \
    DEPLOY_SHA="$deploy_sha" \
    RUNNER_TEMP="$runner_temp" \
    GITHUB_RUN_ID=15701 \
    GITHUB_RUN_ATTEMPT=1 \
    TEST_REMOTE_HOME="$home" \
    TEST_SSH_LOG="$work/ssh.log" \
    bash "$subject"
)
[ "$(cat "$work/ssh.log")" = fleet-hub-deploy ] || { echo "FAIL: remote host alias was not used"; exit 1; }
mapfile -t release_call <"$home/release-call"
[ "${release_call[0]}" = "$deploy_sha" ] || { echo "FAIL: release received wrong DEPLOY_SHA"; exit 1; }
[ "${release_call[1]}" = "$deploy_sha" ] || { echo "FAIL: release source HEAD was not exact SHA"; exit 1; }
[ "${release_call[2]}" = "$home/sources/$deploy_sha" ] || { echo "FAIL: release source was not remote managed source"; exit 1; }
[ "${release_call[3]}" = remote ] || { echo "FAIL: release did not execute in remote context"; exit 1; }
[ ! -e "$runner_temp/fleet-bus/$deploy_sha-15701-1.bundle" ] || { echo "FAIL: runner bundle was not cleaned"; exit 1; }
[ "$(git -C "$home/sources/$deploy_sha" rev-parse HEAD)" = "$deploy_sha" ]
if git -C "$home/sources/$deploy_sha" symbolic-ref -q HEAD >/dev/null 2>&1; then
    echo "FAIL: source checkout is not detached"
    exit 1
fi
[ -z "$(git -C "$home/sources/$deploy_sha" status --porcelain=v1 --untracked-files=all)" ]
assert_clean_artifacts

# A second exact deployment reuses the clean detached source rather than
# quarantining or rematerializing it.
(
    cd "$fixture"
    PATH="$fake_bin:$PATH" \
    DEPLOY_SHA="$deploy_sha" \
    RUNNER_TEMP="$runner_temp" \
    GITHUB_RUN_ID=15707 \
    GITHUB_RUN_ATTEMPT=1 \
    TEST_REMOTE_HOME="$home" \
    TEST_SSH_LOG="$work/ssh.log" \
    bash "$subject"
) >"$work/clean-reuse.log" 2>&1
grep -F "reusing clean detached source $home/sources/$deploy_sha" "$work/clean-reuse.log" >/dev/null
if find "$home/sources" -maxdepth 1 -name ".quarantine-$deploy_sha-*" -print -quit | grep -q .; then
    echo "FAIL: clean source reuse was quarantined"
    exit 1
fi
assert_clean_artifacts

# A transport error reported after the copy triggers bounded remote cleanup;
# neither the remote release nor either side's bundle artifact may remain.
before_call="$(cat "$home/release-call")"
set +e
(
    cd "$fixture"
    PATH="$fake_bin:$PATH" \
    DEPLOY_SHA="$deploy_sha" \
    RUNNER_TEMP="$runner_temp" \
    GITHUB_RUN_ID=15706 \
    GITHUB_RUN_ATTEMPT=1 \
    TEST_REMOTE_HOME="$home" \
    TEST_SSH_LOG="$work/ssh.log" \
    TEST_SCP_FAIL_AFTER_COPY=1 \
    bash "$subject"
) >"$work/scp-failure.log" 2>&1
status=$?
set -e
[ "$status" -ne 0 ] || { echo "FAIL: post-copy scp failure passed"; exit 1; }
grep -F 'bundle transfer failed' "$work/scp-failure.log" >/dev/null
[ "$(cat "$home/release-call")" = "$before_call" ] || { echo "FAIL: failed transfer executed release"; exit 1; }
[ ! -e "$runner_temp/fleet-bus/$deploy_sha-15706-1.bundle" ] || { echo "FAIL: failed runner bundle was not cleaned"; exit 1; }
assert_clean_artifacts

# A successful copy followed by SSH failure before remote_main starts still
# invokes the bounded cleanup mode and does not execute the release.
before_call="$(cat "$home/release-call")"
set +e
(
    cd "$fixture"
    PATH="$fake_bin:$PATH" \
    DEPLOY_SHA="$deploy_sha" \
    RUNNER_TEMP="$runner_temp" \
    GITHUB_RUN_ID=15708 \
    GITHUB_RUN_ATTEMPT=1 \
    TEST_REMOTE_HOME="$home" \
    TEST_SSH_LOG="$work/ssh.log" \
    TEST_SSH_FAIL_BEFORE_EXEC=1 \
    bash "$subject"
) >"$work/ssh-failure.log" 2>&1
status=$?
set -e
[ "$status" -ne 0 ] || { echo "FAIL: pre-execution SSH failure passed"; exit 1; }
grep -F 'remote deploy failed' "$work/ssh-failure.log" >/dev/null
[ "$(cat "$home/release-call")" = "$before_call" ] || { echo "FAIL: failed SSH handoff executed release"; exit 1; }
[ ! -e "$runner_temp/fleet-bus/$deploy_sha-15708-1.bundle" ] || { echo "FAIL: failed SSH runner bundle was not cleaned"; exit 1; }
assert_clean_artifacts

# Hash mismatch fails before release execution and still removes the incoming file.
hash_bundle="$home/incoming/$deploy_sha-15702-1.bundle"
make_bundle "$hash_bundle"
before_call="$(cat "$home/release-call")"
set +e
TEST_REMOTE_CONTEXT=1 FLEET_BUS_HOME="$home" \
    bash "$subject" --remote "$deploy_sha" "$(printf '0%.0s' {1..64})" "$hash_bundle" \
    >"$work/hash-reject.log" 2>&1
status=$?
set -e
[ "$status" -ne 0 ] || { echo "FAIL: wrong bundle hash passed"; exit 1; }
grep -F 'incoming bundle SHA-256 mismatch' "$work/hash-reject.log" >/dev/null
[ "$(cat "$home/release-call")" = "$before_call" ] || { echo "FAIL: hash rejection executed release"; exit 1; }
assert_clean_artifacts

# A complete bundle for a different advertised SHA is rejected and cleaned.
wrong_bundle="$home/incoming/$wrong_sha-15703-1.bundle"
make_bundle "$wrong_bundle"
wrong_hash="$(sha256sum "$wrong_bundle" | awk '{print $1}')"
set +e
TEST_REMOTE_CONTEXT=1 FLEET_BUS_HOME="$home" \
    bash "$subject" --remote "$wrong_sha" "$wrong_hash" "$wrong_bundle" \
    >"$work/wrong-sha.log" 2>&1
status=$?
set -e
[ "$status" -ne 0 ] || { echo "FAIL: bundle advertising another SHA passed"; exit 1; }
grep -F 'bundle does not advertise exactly DEPLOY_SHA' "$work/wrong-sha.log" >/dev/null
assert_clean_artifacts

# Dirty reuse is quarantined, then rebuilt from the verified bundle.
printf '%s\n' hostile >>"$home/sources/$deploy_sha/scripts/deploy-hub-release.sh"
dirty_bundle="$home/incoming/$deploy_sha-15704-1.bundle"
make_bundle "$dirty_bundle"
dirty_hash="$(sha256sum "$dirty_bundle" | awk '{print $1}')"
TEST_REMOTE_CONTEXT=1 FLEET_BUS_HOME="$home" \
    bash "$subject" --remote "$deploy_sha" "$dirty_hash" "$dirty_bundle" \
    >"$work/dirty-reuse.log" 2>&1
grep -F 'quarantined invalid source' "$work/dirty-reuse.log" >/dev/null
grep -R -F 'hostile' "$home/sources/.quarantine-$deploy_sha-"* >/dev/null
[ -z "$(git -C "$home/sources/$deploy_sha" status --porcelain=v1 --untracked-files=all)" ]
assert_clean_artifacts

# Corrupt Git metadata is also quarantined rather than reused.
printf '%s\n' 'ref: refs/heads/missing' >"$home/sources/$deploy_sha/.git/HEAD"
corrupt_bundle="$home/incoming/$deploy_sha-15705-1.bundle"
make_bundle "$corrupt_bundle"
corrupt_hash="$(sha256sum "$corrupt_bundle" | awk '{print $1}')"
TEST_REMOTE_CONTEXT=1 FLEET_BUS_HOME="$home" \
    bash "$subject" --remote "$deploy_sha" "$corrupt_hash" "$corrupt_bundle" \
    >"$work/corrupt-reuse.log" 2>&1
grep -F 'quarantined invalid source' "$work/corrupt-reuse.log" >/dev/null
[ "$(git -C "$home/sources/$deploy_sha" rev-parse HEAD)" = "$deploy_sha" ]
[ -z "$(git -C "$home/sources/$deploy_sha" status --porcelain=v1 --untracked-files=all)" ]
assert_clean_artifacts

echo "deploy-hub-remote.test: PASS — remote-only execution, exact SHA, hash rejection, quarantine, and cleanup"
