# Changelog

## Unreleased

### Added

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

- Root `acd --help` is now compact and grouped by workflow.
- User-facing commands now include more practical help text and examples.
- `acd stop --session-id <id>` is now documented as the harness/refcount path:
  it deregisters one client and stops the daemon only when no peers remain.
- Harness templates keep passing explicit `--session-id`; the new no-flag
  start/stop defaults are for humans at a terminal.
- Updated README and troubleshooting docs with examples for watch mode and
  log tailing, plus the simpler current-repo start/stop flow.

### Fixed

- `acd logs --follow` no longer misses lines appended while switching from
  the initial tail read to follow mode.

## v2026-05-02

### Breaking changes

- Removed `ACD_AI_SEND_DIFF`. Diff egress is now off by default. Set
  `ACD_AI_DIFF_EGRESS=1` to allow network or subprocess AI providers to
  receive redacted diffs.

### Added

- `acd recover --auto` can repair old live-index drift after ACD-published
  commits.
- Generated directories such as `node_modules/`, `target/`, virtualenvs, and
  common cache folders are ignored by default during capture and watcher walks.
- `acd doctor` includes watcher diagnostics and active ignore settings in
  reports and bundles.

### Changed

- Replay, fsnotify, git ignore checks, log rotation, and provider shutdown are
  more aggressively bounded so the daemon is less likely to hang.
- Git diff/blob rendering now has stronger caps for large files.
- Process checks use pinned system `ps` paths on macOS and Linux.
- Docs now cover AI diff egress, live-index recovery, safe-ignore defaults, and
  daemon troubleshooting.

### Fixed

- Fixed stale live-index state after replay publishes a commit.
- Fixed generated ignored trees showing up as capture events or watcher load.
- Fixed several edge cases around ambiguous refs, SQLite lock handling,
  rewind grace, malformed pause markers, and git-operation marker stat errors.

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
