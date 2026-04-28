#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../.."

# Phase 0 placeholder.
# Phase 1+ will exercise: cold-start latency, single-file commit latency,
# steady-state CPU/RSS on a 5k-file repo, 50k-file initial bootstrap.
echo "benchmark suite not yet implemented (Phase 0 placeholder)"
exit 0
