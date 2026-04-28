#!/usr/bin/env bash
set -euo pipefail

if command -v acd >/dev/null 2>&1; then
  acd stop --all || true
fi

rm -f "$HOME/.local/bin/acd"
rm -rf "$HOME/.local/share/acd" "$HOME/.local/state/acd" "$HOME/.config/acd"

echo "acd uninstalled."
echo "Per-repo state at <repo>/.git/acd/ is left untouched. Remove manually if desired."
