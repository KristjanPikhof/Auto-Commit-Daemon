#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../.."

shard_count=${ACD_TEST_SHARDS:-2}
package_parallelism=${ACD_TEST_PACKAGE_PARALLELISM:-2}
test_timeout=${ACD_TEST_TIMEOUT:-4m15s}
timing_sensitive_daemon_tests='^(TestRun_(FsnotifyDrivesWake|LifecycleHappyPath|WakeBurstCoalesced|RealSIGUSR1|RepeatedEditsToSameFile_OrderedCommits|SelfTerminateNoClients)|TestReplay_IntentSingletonSupersededProbeTimeoutSettlesEvent)$'
output_root=

cleanup() {
  if [[ -n "$output_root" ]]; then
    rm -rf "$output_root"
  fi
}
trap cleanup EXIT

usage() {
  cat >&2 <<'EOF'
usage: scripts/dev/test.sh
       scripts/dev/test.sh core <shard-count> <shard-index>
       scripts/dev/test.sh support
       scripts/dev/test.sh sensitive
       scripts/dev/test.sh stress-daemon <shard-count> <shard-index>
       scripts/dev/test.sh stress-support <shard-count> <shard-index>
EOF
}

validate_shard() {
  local count=$1
  local index=$2

  case "$count" in
    '' | *[!0-9]*)
      echo "test.sh: shard count must be a positive integer" >&2
      return 2
      ;;
  esac
  case "$index" in
    '' | *[!0-9]*)
      echo "test.sh: shard index must be a non-negative integer" >&2
      return 2
      ;;
  esac
  if ((count < 1)); then
    echo "test.sh: shard count must be greater than zero" >&2
    return 2
  fi
  if ((index >= count)); then
    echo "test.sh: shard index must be less than shard count" >&2
    return 2
  fi
}

run_package_shard() {
  local package=$1
  local count=$2
  local index=$3
  shift 3

  ACD_TEST_SHARD_INDEX=$index scripts/dev/test-package-shards.sh \
    "$package" "$count" "$@"
}

run_core() {
  local count=$1
  local index=$2
  local cli_pid
  local daemon_pid
  local status=0

  validate_shard "$count" "$index"
  run_package_shard ./internal/cli "$count" "$index" \
    -race -count=1 -timeout "$test_timeout" &
  cli_pid=$!
  run_package_shard ./internal/daemon "$count" "$index" \
    -race -count=1 -timeout "$test_timeout" \
    -skip "$timing_sensitive_daemon_tests" &
  daemon_pid=$!

  if ! wait "$cli_pid"; then
    status=1
  fi
  if ! wait "$daemon_pid"; then
    status=1
  fi
  return "$status"
}

run_support() {
  local package_list
  local package
  local packages=()

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

  go test -p "$package_parallelism" "${packages[@]}" \
    -race -count=1 -timeout "$test_timeout"
}

run_sensitive() {
  go test ./internal/daemon -race -count=1 -timeout "$test_timeout" \
    -run "$timing_sensitive_daemon_tests"
}

run_stress_daemon() {
  local count=$1
  local index=$2

  validate_shard "$count" "$index"
  run_package_shard ./internal/daemon "$count" "$index" \
    -race -count=3 -timeout "$test_timeout" -failfast \
    -skip "$timing_sensitive_daemon_tests"
}

run_stress_support() {
  local count=$1
  local index=$2

  validate_shard "$count" "$index"
  run_package_shard ./internal/git "$count" "$index" \
    -race -count=3 -timeout "$test_timeout" -failfast
  run_package_shard ./internal/state "$count" "$index" \
    -race -count=3 -timeout "$test_timeout" -failfast
}

run_all() {
  local status=0
  local index
  local support_pid
  local -a core_pids=()

  validate_shard "$shard_count" 0
  output_root=$(mktemp -d "${TMPDIR:-/tmp}/acd-tests.XXXXXX")

  for ((index = 0; index < shard_count; index++)); do
    run_core "$shard_count" "$index" \
      >"$output_root/core-$index.log" 2>&1 &
    core_pids[$index]=$!
  done
  run_support >"$output_root/support.log" 2>&1 &
  support_pid=$!

  for ((index = 0; index < shard_count; index++)); do
    if ! wait "${core_pids[$index]}"; then
      status=1
    fi
    cat "$output_root/core-$index.log"
  done
  if ! wait "$support_pid"; then
    status=1
  fi
  cat "$output_root/support.log"

  if ! run_sensitive >"$output_root/sensitive.log" 2>&1; then
    status=1
  fi
  cat "$output_root/sensitive.log"
  return "$status"
}

case "${1:-}" in
  '')
    run_all
    ;;
  core)
    if (($# != 3)); then
      usage
      exit 2
    fi
    run_core "$2" "$3"
    ;;
  support)
    if (($# != 1)); then
      usage
      exit 2
    fi
    run_support
    ;;
  sensitive)
    if (($# != 1)); then
      usage
      exit 2
    fi
    run_sensitive
    ;;
  stress-daemon)
    if (($# != 3)); then
      usage
      exit 2
    fi
    run_stress_daemon "$2" "$3"
    ;;
  stress-support)
    if (($# != 3)); then
      usage
      exit 2
    fi
    run_stress_support "$2" "$3"
    ;;
  *)
    usage
    exit 2
    ;;
esac
