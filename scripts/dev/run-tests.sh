#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../.."

go vet ./...
gofmt -l .
scripts/dev/test.sh
