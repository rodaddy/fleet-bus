#!/usr/bin/env bash
# Docs-drift gate (docs/coding-standards.md "Enforcement").
#
# Blocks when Python source under a package changed but that package's docs/api/
# was not updated in the same change. Runs both as a pre-push hook and in CI.
#
# GRACEFUL BOOTSTRAP: this repo has no docs/api/ layout yet — it arrives with the
# build phases (issues #3-#12) that add modules. Until a package actually has a
# docs/api/ directory, this check no-ops for that package rather than false-fail
# the foundation. Once docs/api/ exists, drift on that package is enforced.
set -euo pipefail

repo_root="$(cd "$(dirname "$0")/.." && pwd)"
cd "$repo_root"

# Pick a base ref to diff against: origin/main in CI, the upstream/main locally.
base=""
for cand in "origin/main" "main" "@{upstream}"; do
    if git rev-parse --verify --quiet "$cand" >/dev/null 2>&1; then
        base="$cand"
        break
    fi
done
if [ -z "$base" ]; then
    echo "check-docs: no base ref (origin/main|main|@{upstream}) — skipping."
    exit 0
fi

# Files changed vs base. Fail LOUD if the diff itself errors (e.g. no merge base
# on an odd/shallow checkout) rather than swallowing it into an empty result —
# an empty "changed" would otherwise read as "no drift, pass" and silently bypass
# the gate. A failed diff computation is a hard error, not a pass.
if ! changed="$(git diff --name-only "$base...HEAD" 2>&1)"; then
    echo "check-docs: git diff against $base failed — refusing to pass:" >&2
    echo "$changed" | sed 's/^/  /' >&2
    exit 1
fi
[ -n "$changed" ] || { echo "check-docs: no changed files vs $base — OK."; exit 0; }

fail=0
# For each package, if its source changed AND it has a docs/api/, require a
# docs/api/ change too. Packages without docs/api/ are in bootstrap — skipped.
for pkg in packages/*/; do
    name="$(basename "$pkg")"
    api_dir="${pkg}docs/api"

    src_changed="$(printf '%s\n' "$changed" | grep -E "^${pkg}src/.*\.py$" || true)"
    [ -n "$src_changed" ] || continue

    if [ ! -d "$api_dir" ]; then
        echo "check-docs: $name — source changed, no docs/api/ yet (bootstrap, skipped)."
        continue
    fi

    docs_changed="$(printf '%s\n' "$changed" | grep -E "^${pkg}docs/api/.*\.md$" || true)"
    if [ -z "$docs_changed" ]; then
        echo "check-docs: FAIL $name — source under ${pkg}src/ changed but ${api_dir}/ was not updated."
        echo "  changed source:"
        printf '%s\n' "$src_changed" | sed 's/^/    /'
        fail=1
    else
        echo "check-docs: $name — source + docs/api/ both updated. OK."
    fi
done

if [ "$fail" -ne 0 ]; then
    echo "check-docs: FAILED — update the package's docs/api/<module>.md in the same change."
    exit 1
fi
echo "check-docs: OK."
