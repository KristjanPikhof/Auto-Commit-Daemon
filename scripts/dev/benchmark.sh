#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../.."

# Artifacts live outside the checkout by default: a benchmark must never cause
# the installed worker to capture its own output.
results=${ACD_BENCHMARK_RESULTS:-$(mktemp -d "${TMPDIR:-/tmp}/acd-benchmark.XXXXXX")}
mkdir -p "$results"
start=$SECONDS
status=0
ACD_BENCHMARK=1 go test ./test/integration -tags=integration -race \
  -run '^TestProductionMeasurements$' -count=1 -parallel=1 -timeout=2m \
  -json >"$results/production.jsonl" 2>&1 || status=$?
python3 - "$results" "$((SECONDS - start))" "$status" <<'PY'
import json, pathlib, sys
root = pathlib.Path(sys.argv[1])
summary = {'suite_seconds': int(sys.argv[2]), 'exit_code': int(sys.argv[3]),
           'scope': 'production measurement scenario; not the complete suite'}
for line in (root / 'production.jsonl').read_text().splitlines():
    try:
        event = json.loads(line)
    except json.JSONDecodeError:
        print(line)
        continue
    output = event.get('Output', '')
    if 'ACD_MEASUREMENT ' in output:
        summary.update(json.loads(output.split('ACD_MEASUREMENT ', 1)[1]))
    elif event.get('Action') == 'output':
        print(output, end='')
(root / 'summary.json').write_text(json.dumps(summary, indent=2) + '\n')
print(json.dumps(summary, indent=2))
print('Measurement artifacts:', root)
PY
exit "$status"
