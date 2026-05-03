# Changelog

## Unreleased

### Fixed

- Same-branch fast-forwards, such as `git checkout main && git pull`, now
  refresh ACD's shadow baseline from the new `HEAD` instead of replaying stale
  work captured before the pull.
- Manual pause/resume now preserves self-heal behavior when an external commit
  lands during the pause, so the resumed daemon can mark matching work as
  already published instead of treating it like upstream-only content.

## v2026-05-03

### Added

- `acd recover --auto` can repair stale live-index entries left by older
  ACD-published commits, and `acd doctor` can report repair candidates.
- Generated dependency and cache directories such as `node_modules/`,
  `target/`, virtualenvs, and common tool caches are ignored by default during
  capture and watcher walks.
- `acd start` now works without `--session-id` for manual current-repo use.
  It registers a stable human client for the repo, so repeated manual starts
  refresh the same row instead of creating a pile of stale clients.
- `acd stop` now works without `--session-id` for manual current-repo use.
  It stops the resolved repo daemon directly.
- `acd list --watch` refreshes the daemon table until Ctrl-C.
- `acd list --watch --interval <duration>` sets the refresh rate.
- `acd logs` tails the current repo's daemon log as raw JSONL.
- `acd logs --lines N` changes the initial tail length.
- `acd logs --follow` streams new daemon log lines as they arrive.

### Changed

- Replay now reconciles the live Git index after publishing commits, guarded by
  path-scoped before-state checks so user-staged changes are not overwritten.
- Root `acd --help` is now compact and grouped by workflow.
- User-facing commands now include more practical help text and examples.
- `acd stop --session-id <id>` is now documented as the harness/refcount path:
  it deregisters one client and stops the daemon only when no peers remain.
- Harness templates keep passing explicit `--session-id`; the new no-flag
  start/stop defaults are for humans at a terminal.
- Updated README and troubleshooting docs with examples for watch mode and
  log tailing, live-index recovery, safe-ignore defaults, and the simpler
  current-repo start/stop flow.

### Fixed

- Daemon runs now wire the per-repo JSONL file logger through the same canonical
  repo hash used by `acd logs` and central stats.
- Published replay events no longer leave the live index stale after ACD moves
  `HEAD`.
- Generated dependency/cache trees no longer show up as capture events or
  watcher load when a repo forgot to gitignore them.
- `acd logs --follow` no longer misses lines appended while switching from
  the initial tail read to follow mode.

## v2026-05-02

### Breaking changes

- Removed `ACD_AI_SEND_DIFF`. Diff egress is now off by default. Set
  `ACD_AI_DIFF_EGRESS=1` to allow network or subprocess AI providers to
  receive redacted diffs.

### Added

- `acd diagnose`, `acd recover`, `acd pause`, `acd resume`, and
  `acd purge-events` give operators first-class recovery controls for replay
  blockers, branch incidents, and manual pause state.
- Recursive fsnotify watching can drive daemon wakeups when enabled.
- Best-effort JSONL trace files record capture, replay, branch-token, pause,
  and daemon-transition decisions.

### Changed

- Replay, fsnotify, git ignore checks, log rotation, and provider shutdown are
  more aggressively bounded so the daemon is less likely to hang.
- Git diff/blob rendering now has stronger caps for large files.
- Process checks use pinned system `ps` paths on macOS and Linux.
- Schema v4 adds faster flush-request lookup and read-heavy state paths use the
  read pool where possible.
- Docs now cover AI diff egress, branch-token handling, recovery workflows, and
  daemon troubleshooting.

### Fixed

- Fixed several edge cases around ambiguous refs, SQLite lock handling,
  rewind grace, malformed pause markers, detached-to-attached branch recovery,
  shadow bootstrap atomicity, and git-operation marker stat errors.

## v2026-04-28

### Added

- Initial public release of `acd`, a per-repo auto-commit daemon for macOS and
  Linux.
- Added daemon lifecycle commands: `start`, `stop`, `wake`, `touch`, and
  `daemon run`.
- Added operator commands: `status`, `list`, `stats`, `doctor`, `diagnose`,
  `recover`, `pause`, `resume`, `purge-events`, `gc`, and `init`.
- Added capture and replay backed by SQLite state, shadow paths, publish state,
  flush requests, daemon metadata, rollups, and the central registry.
- Added commit-message providers: deterministic, OpenAI-compatible, and
  subprocess.
- Added harness setup snippets for Claude Code, Codex, OpenCode, Pi, shell,
  and direnv.
- Added JSONL daemon logs with rotation, XDG paths, repo hashing, process
  fingerprinting, trace support, and install/uninstall scripts.

### Changed

- Pinned Go 1.22 dependencies, including `modernc.org/sqlite v1.36.0`.
- Release packaging is set up. Homebrew publishing remains skipped until tap
  credentials exist.
