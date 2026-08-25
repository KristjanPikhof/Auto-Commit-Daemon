#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 2 ]]; then
  echo "usage: $0 <package> <shard-count> [go-test-args...]" >&2
  exit 2
fi

package=$1
shard_count=$2
shift 2

if ! [[ "$shard_count" =~ ^[1-9][0-9]*$ ]]; then
  echo "invalid shard count: $shard_count" >&2
  exit 2
fi

test_names=()
while IFS= read -r name; do
  test_names[${#test_names[@]}]=$name
done < <(go test "$package" -list '^(Test|Example|Fuzz)' |
  awk '/^(Test|Example|Fuzz)/ { print }')

test_count=${#test_names[@]}
if [[ "$test_count" -eq 0 ]]; then
  echo "$package: no tests, examples, or fuzz targets found" >&2
  exit 1
fi
if [[ "$shard_count" -gt "$test_count" ]]; then
  shard_count=$test_count
fi

output_root=$(mktemp -d "${TMPDIR:-/tmp}/acd-test-shards.XXXXXX")
cleanup() {
  rm -rf "$output_root"
}
trap cleanup EXIT

pids=()
outputs=()
for ((shard = 0; shard < shard_count; shard++)); do
  pattern=""
  for ((index = shard; index < test_count; index += shard_count)); do
    if [[ -n "$pattern" ]]; then
      pattern+="|"
    fi
    pattern+="${test_names[$index]}"
  done
  output="$output_root/shard-$shard.log"
  outputs[$shard]=$output
  go test "$package" "$@" -run "^($pattern)$" >"$output" 2>&1 &
  pids[$shard]=$!
done

echo "$package: running $test_count top-level tests across $shard_count shards"
status=0
for ((shard = 0; shard < shard_count; shard++)); do
  if ! wait "${pids[$shard]}"; then
    status=1
  fi
  cat "${outputs[$shard]}"
done
exit "$status"
