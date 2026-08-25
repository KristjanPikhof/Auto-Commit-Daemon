#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 2 ]]; then
  echo "usage: $0 <package> <shard-count> [go-test-args...]" >&2
  exit 2
fi

package=$1
shard_count=$2
shift 2
requested_shard=${ACD_TEST_SHARD_INDEX:-}

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
if [[ -n "$requested_shard" ]] &&
  { ! [[ "$requested_shard" =~ ^[0-9]+$ ]] || [[ "$requested_shard" -ge "$shard_count" ]]; }; then
  echo "invalid shard index $requested_shard for $shard_count shards" >&2
  exit 2
fi

output_root=$(mktemp -d "${TMPDIR:-/tmp}/acd-test-shards.XXXXXX")
cleanup() {
  rm -rf "$output_root"
}
trap cleanup EXIT

pids=()
outputs=()
first_shard=0
last_shard=$shard_count
if [[ -n "$requested_shard" ]]; then
  first_shard=$requested_shard
  last_shard=$((requested_shard + 1))
fi
for ((shard = first_shard; shard < last_shard; shard++)); do
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

if [[ -n "$requested_shard" ]]; then
  echo "$package: running shard $((requested_shard + 1))/$shard_count of $test_count top-level tests"
else
  echo "$package: running $test_count top-level tests across $shard_count shards"
fi
status=0
for ((shard = first_shard; shard < last_shard; shard++)); do
  if ! wait "${pids[$shard]}"; then
    status=1
  fi
  cat "${outputs[$shard]}"
done
exit "$status"
