#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../.."

shard_count=${ACD_TEST_SHARDS:-2}
package_parallelism=${ACD_TEST_PACKAGE_PARALLELISM:-2}
output_root=$(mktemp -d "${TMPDIR:-/tmp}/acd-tests.XXXXXX")
cleanup() {
  rm -rf "$output_root"
}
trap cleanup EXIT

packages=()
package_list=$(go list ./...)
while IFS= read -r package; do
  case "$package" in
    */internal/cli | */internal/daemon)
      ;;
    *)
      packages[${#packages[@]}]=$package
      ;;
  esac
done <<<"$package_list"

scripts/dev/test-package-shards.sh ./internal/cli "$shard_count" \
  -race -count=1 >"$output_root/cli.log" 2>&1 &
cli_pid=$!
scripts/dev/test-package-shards.sh ./internal/daemon "$shard_count" \
  -race -count=1 >"$output_root/daemon.log" 2>&1 &
daemon_pid=$!
go test -p "$package_parallelism" "${packages[@]}" \
  -race -count=1 >"$output_root/packages.log" 2>&1 &
packages_pid=$!

status=0
for job in cli daemon packages; do
  pid_name="${job}_pid"
  if ! wait "${!pid_name}"; then
    status=1
  fi
  cat "$output_root/$job.log"
done
exit "$status"
