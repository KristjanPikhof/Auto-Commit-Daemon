#!/usr/bin/env bash
set -euo pipefail

if ! command -v acd >/dev/null 2>&1; then
  echo "ACD was not found. No files were removed." >&2
  exit 1
fi

# The CLI reviews owned paths and preserves saved work by default.
exec acd uninstall "$@"
