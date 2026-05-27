#!/usr/bin/env bash
# Cursor agent lifecycle helper (see templates/cursor/README.md).
set -euo pipefail

LOG="${XDG_STATE_HOME:-$HOME/.local/state}/acd/cursor-hook.log"
LOG_DIR="$(dirname "$LOG")"
[ -d "$LOG_DIR" ] || mkdir -p "$LOG_DIR" 2>/dev/null || true

usage() {
  printf 'usage: %s start|wake|flush|stop\n' "$(basename "$0")" >&2
  exit 2
}

cursor_extract() {
  local out rc
  out=$(acd hook-cursor-extract <&0 2>>"$LOG") || {
    rc=$?
    printf '[%s] hook-cursor-extract failed exit=%d\n' "$(date +%FT%T%z)" "$rc" >>"$LOG"
    return "$rc"
  }
  SESSION_ID=$(printf '%s\n' "$out" | sed -n '1p')
  REPO=$(printf '%s\n' "$out" | sed -n '2p')
}

cmd_start() {
  cursor_extract || exit 1
  acd start \
    --harness cursor \
    --session-id "$SESSION_ID" \
    --watch-pid 0 \
    --repo "$REPO" >/dev/null 2>>"$LOG" || {
    rc=$?
    printf '[%s] session-start failed exit=%d cmd=acd-start\n' "$(date +%FT%T%z)" "$rc" >>"$LOG"
    exit 1
  }
}

cmd_wake() {
  cursor_extract || exit 1
  { acd start \
      --harness cursor \
      --session-id "$SESSION_ID" \
      --watch-pid 0 \
      --repo "$REPO" >/dev/null \
    && acd wake \
      --session-id "$SESSION_ID" \
      --repo "$REPO" >/dev/null ; } 2>>"$LOG" || {
    rc=$?
    printf '[%s] active hook failed exit=%d cmd=acd-start-wake\n' "$(date +%FT%T%z)" "$rc" >>"$LOG"
    exit 1
  }
}

cmd_flush() {
  cursor_extract || exit 1
  acd flush --logical \
    --session-id "$SESSION_ID" \
    --repo "$REPO" >/dev/null 2>>"$LOG" || {
    rc=$?
    printf '[%s] flush failed exit=%d cmd=acd-flush-logical\n' "$(date +%FT%T%z)" "$rc" >>"$LOG"
    exit 1
  }
}

cmd_stop() {
  cursor_extract || exit 0
  acd stop \
    --session-id "$SESSION_ID" \
    --repo "$REPO" >/dev/null 2>>"$LOG" || true
}

case "${1:-}" in
  start) cmd_start ;;
  wake) cmd_wake ;;
  flush) cmd_flush ;;
  stop) cmd_stop ;;
  *) usage ;;
esac
