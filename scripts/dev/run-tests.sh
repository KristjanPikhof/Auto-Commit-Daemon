#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../.."

go vet ./...
gofmt -l .
go test ./... -race -count=1
