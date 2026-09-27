#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 2 ]]; then
  echo "usage: $0 <package> <shard-count> [go-test-args...]" >&2
  exit 2
fi
started_seconds=$SECONDS
package=$1
package_slug=${package#./}
package_slug=${package_slug//\//_}
shard_count=$2
shift 2
requested_shard=${ACD_TEST_SHARD_INDEX:-}
if ! [[ "$shard_count" =~ ^[1-9][0-9]*$ ]] ||
  { [[ -n "$requested_shard" ]] && { ! [[ "$requested_shard" =~ ^[0-9]+$ ]] || ((requested_shard >= shard_count)); }; }; then
  echo "invalid shard count/index: $shard_count/$requested_shard" >&2
  exit 2
fi
output_root=$(mktemp -d "${TMPDIR:-/tmp}/acd-test-shards.XXXXXX")
trap 'rm -rf "$output_root"' EXIT
# Capture discovery separately: a compile failure must fail the lane rather
# than disappearing in a process substitution. Use identical build flags.
go test "$package" "$@" -list '^(Test|Example|Fuzz)' >"$output_root/discovery.log"
awk '/^(Test|Example|Fuzz)[[:alnum:]_]*$/ { print }' "$output_root/discovery.log" >"$output_root/names"
if [[ ! -s "$output_root/names" ]]; then
  cat "$output_root/discovery.log" >&2
  echo "$package: no tests, examples, or fuzz targets found" >&2
  exit 1
fi
python3 scripts/dev/test-manifest.py balance "$package" "$shard_count" "$output_root/names" >"$output_root/manifest.json"
first=0
last=$shard_count
if [[ -n "$requested_shard" ]]; then
  first=$requested_shard
  last=$((requested_shard + 1))
fi
if [[ -n "${ACD_TEST_RESULTS_DIR:-}" ]]; then
  mkdir -p "$ACD_TEST_RESULTS_DIR"
fi
pids=()
status=0
for ((shard = first; shard < last; shard++)); do
  pattern=$(python3 scripts/dev/test-manifest.py pattern "$output_root/manifest.json" "$shard")
  result="$output_root/shard-$shard.jsonl"
  if [[ -n "${ACD_TEST_RESULTS_DIR:-}" ]]; then
    name="$package_slug-$shard"
    result="$ACD_TEST_RESULTS_DIR/$name.jsonl"
    cp "$output_root/manifest.json" "$ACD_TEST_RESULTS_DIR/$name.manifest.json"
  fi
  echo "$package: running shard $((shard + 1))/$shard_count"
  go test "$package" "$@" -json -run "$pattern" >"$result" 2>&1 &
  pids[$shard]=$!
  outputs[$shard]=$result
done
for ((shard = first; shard < last; shard++)); do
  if ! wait "${pids[$shard]}"; then status=1; fi
  python3 - "${outputs[$shard]}" <<'PY'
import json, sys
records = []
for line in open(sys.argv[1]):
    try:
        records.append(json.loads(line))
    except json.JSONDecodeError:
        print(line, end='')
failed = {r.get('Test', '').split('/')[0] for r in records if r.get('Action') == 'fail'}
for event in records:
    test = event.get('Test', '')
    if event.get('Action') == 'output' and (not test or test.split('/')[0] in failed):
        output = event.get('Output', '')
        if not output.startswith(('=== RUN', '=== PAUSE', '=== CONT', '=== NAME')):
            print(output, end='')
PY
done
if [[ -n "${ACD_TEST_RESULTS_DIR:-}" ]]; then
  python3 - "$ACD_TEST_RESULTS_DIR/${package_slug}-${requested_shard:-all}.summary.json" "$((SECONDS - started_seconds))" "$status" <<'PY_SUMMARY'
import json, pathlib, sys
pathlib.Path(sys.argv[1]).write_text(json.dumps({'wall_seconds': int(sys.argv[2]), 'exit_code': int(sys.argv[3])}) + '\n')
PY_SUMMARY
fi
exit "$status"
