#!/usr/bin/env bash
# Point git at the tracked .githooks/ dir. Run once after clone.
set -euo pipefail
repo_root="$(cd "$(dirname "$0")/.." && pwd)"
git -C "$repo_root" config core.hooksPath .githooks

# Make hooks executable. git won't fire a non-executable hook, so a masked chmod
# failure would leave the gate silently disabled — verify instead of swallowing.
chmod +x "$repo_root"/.githooks/pre-push
if [ ! -x "$repo_root/.githooks/pre-push" ]; then
    echo "install-hooks: FAILED to make .githooks/pre-push executable" >&2
    exit 1
fi
echo "hooks installed: core.hooksPath -> .githooks"
