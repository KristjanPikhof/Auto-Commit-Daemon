# Changelog

## Unreleased

### Changed

- **Codex hooks v2 (breaking).** `acd setup codex` now emits
  `~/.codex/hooks.json` instead of the legacy `[[hooks.*]]` TOML snippet,
  matching the order Codex now uses for hook discovery (`hooks.json` wins
  over `config.toml`). All five Codex hook events are wired:
  `SessionStart -> acd start` (timeout 15s), `UserPromptSubmit -> idempotent
  acd start, then acd wake` (15s), `PreToolUse` and `PostToolUse -> idempotent
  acd start, then acd wake` (matcher `apply_patch|Edit|Write|Bash`, 15s each),
  and `Stop -> acd touch` (5s)
  mirroring the claude-code pattern so the daemon survives end-of-turn
  while replay drains. `_acd_managed: true` at the top level is the
  managed-install marker. `cwd` is sourced from the JSON `cwd` field on
  stdin via `acd hook-stdin-extract session_id cwd? <&0`;
  `CODEX_PROJECT_DIR` is no longer required, and `printf "{}\n"` is gone.
  Adapter detection now matches per path (JSON markers for `hooks.json`,
  TOML markers for `config.toml`) across user-scoped Codex config paths. `acd doctor`
  warns when both `~/.codex/hooks.json` and a legacy Codex TOML config carry
  acd markers. Codex actually merges every hook source it finds, so
  leaving both files installed fires every event twice; `acd doctor`
  surfaces this as "Codex merges all hook sources and will fire each
  event twice (doubled acd start/wake/touch)." **Migration:** run
  `acd setup codex --raw > ~/.codex/hooks.json` (the new `--raw` flag
  emits the snippet without `// `-wrapped instructions, which JSON does
  not allow), delete the `# acd-managed: true` block from
  `~/.codex/config.toml`, then run `/hooks` inside Codex to approve
  the five newly-installed hook entries — Codex now flags every
  newly-added hook as "review required" and refuses to run them until
  the user approves. Codex also deprecated
  `[features].codex_hooks = true` in favor of `[features].hooks = true`;
  `hooks.json` does not need a `[features]` block, but legacy TOML
  users should rename the flag.
  `acd hook-stdin-extract` now accepts multiple field arguments, emits one scalar
  per line in argument order, and supports optional fields with a `?` suffix; the
  single-arg form is unchanged.
- Claude Code, OpenCode, and Pi active hooks now match Codex's self-healing
  pattern: tool/prompt activity runs idempotent `acd start` before `acd wake`,
  so ACD restarts after a manual `acd stop` without waiting for a brand-new
  harness session. Their end-session hooks still deregister with
  `acd stop --session-id`; Codex keeps `Stop -> acd touch` to avoid stopping
  mid-replay drain.
- `acd status`, `acd diagnose`, and `acd doctor` now report the *effective*
  commit strategy by resolving daemon `commit.strategy` meta first, then the
  `ACD_COMMIT_STRATEGY` env, then the canonical default. Unrecognized meta
  values no longer leak into the report; they emit a slog warning and the
  env-derived strategy is shown instead. New helper
  `cli.ResolveEffectiveCommitStrategy` centralizes this resolution so
  `commit-all`, intent observability, and any future read-only consumers
  agree on what is active.

### Fixed

- `acd commit-all` now force-reseeds `shadow_paths` from `HEAD` and drops
  stale `pending` capture events for the active `(branch_ref,
  branch_generation)` before capturing. Previously, when a prior daemon
  session had absorbed worktree edits into shadow without successfully
  replaying them, the bootstrap marker was already set; `commit-all` saw a
  shadow that mirrored live state, captured zero events, and reported
  `Commits: 0; no pending events; worktree already clean` while the
  worktree was still dirty. The fix exposes a new
  `state.DeletePendingForBranchGeneration` helper, switches `commit-all`
  to `daemon.ReseedShadowFromHead`, and surfaces a `dropped_stale_pending`
  count plus a `shadow reseeded from HEAD` note in the JSON and human
  output.

### Added

- `acd commit-all`: one-shot command that captures every uncommitted file in
  the worktree and replays them as commits without starting the persistent
  daemon. Useful for cold starts, dirty repos after the daemon was off, and
  onboarding an existing worktree into ACD history. Files are sorted
  lexicographically by path so sibling files cluster together in the commit
  sequence and the intent planner sees coherent windows of related siblings.
  The active commit strategy is read from existing config; there is no
  `--strategy` override. Flags: `--dry-run` (plan without committing),
  `--yes` (skip confirmation), `--json` (machine-readable output, requires
  `--yes`), `--repo`. Refuses to run on detached HEAD, during active git
  operations (rebase, merge, cherry-pick, bisect), while a manual pause marker
  is present, or while the per-repo daemon is running.

## v2026-05-06

### Added

- AI intent commit strategy: `ACD_COMMIT_STRATEGY=intent` can ask the
  configured AI provider to group related pending captures into one reviewable
  commit while keeping the existing `event` strategy as the default.
- Intent planning controls: `ACD_INTENT_WINDOW`,
  `ACD_INTENT_MIN_PENDING`, `ACD_INTENT_MAX_PENDING_AGE`,
  `ACD_INTENT_RECENT_COMMITS`, and `ACD_INTENT_DEFER_LIMIT` tune planner
  context, offered capture windows, sparse-queue waits, and starvation
  protection for deferred work.
- Opt-in prompt tracing: `ACD_AI_PROMPT_TRACE=1` plus `acd prompt` lets
  operators inspect local event and intent-planner request diagnostics after
  redaction/truncation, with explicit privacy warnings because traces may still
  contain source code.
- Intent grouping observability in `status`, `diagnose`, `doctor`, `events`,
  and `explain`, including grouped event sequences, deferral reasons,
  forced-aging decisions, and planner validation failures.
- Explainable ACD history: `acd events`, `acd explain`, and richer
  `status`/`diagnose`/`doctor` output now show why work was captured,
  committed, skipped, protected, blocked, or handled by external history.
- `acd fix` can plan and apply safe recovery for blocked or failed replay rows,
  stale external-work cases, expired manual pause markers, and drained
  backpressure.

### Changed

- Renamed `acd init <harness>` to `acd setup <harness>` for clarity. `acd init` kept as hidden alias for one release with stderr deprecation warning; reserved `acd init` for future repo-state initialization.
- Claude Code harness snippet now uses the canonical nested hook schema
  (`hooks: [{type: "command", command: "..."}]` per event, with `matcher: ""`
  on `PreToolUse`/`PostToolUse`). Required by the current Claude Code hooks
  engine; the previous flat `command` shape no longer registers. Re-run
  `acd setup claude-code` and replace the old `hooks` block in
  `~/.claude/settings.json`.
- `acd wake` and `acd touch` now exit cleanly when another short-lived control
  caller already holds `control.lock`, instead of failing the harness hook.
  JSON output adds `skipped: true` and `skipped_reason: "control_lock_held"`
  so callers can distinguish a no-op from a real heartbeat or signal. The
  in-flight caller does the equivalent work and the daemon reconciles on its
  next tick.
- AI provider docs now include setup profiles for compatibility mode,
  reviewer-friendly intent grouping, metadata-only private repos, self-hosted
  providers, explicit diff egress, and subprocess intent-planner plugins.
- Read-only observability commands no longer migrate old repo databases just to
  inspect them. If a decision ledger is missing, they return an empty history
  instead of changing the DB.
- `acd events --watch` now follows decisions appended after watch starts unless
  `--since` is provided.
- Docs now cover explainable history, failed replay barriers, safe-ignore
  restart requirements, intent batch-wait troubleshooting, prompt-trace
  provider setup, local retention behavior, and the current status JSON fields.

### Fixed

- Intent strategy falls back to event-style publishing when configuration is
  invalid or planner output cannot be trusted, so replay safety stays under ACD
  control.
- Intent grouping now treats nested paths as ordered dependencies, so a later
  child path cannot publish ahead of an earlier parent-path capture.
- Git history and supersede probes now use literal pathspecs, so paths
  containing characters such as `*`, `[`, or `:` cannot match unrelated files
  while proving planner context or external supersede history.
- Planner failures from OpenAI-compatible and subprocess providers are recorded
  as `intent_planner_error` decisions before deterministic fallback, and
  planner reason text is normalized before it reaches diagnostics.
- v6 to v7 state database migration now adds `planner_state` without rebuilding
  the existing decision ledger, and intent deferral summaries ignore pending
  rows hidden behind terminal replay barriers.
- Fast-forward during rewind grace now has integration coverage that preserves
  the fast-forwarded worktree before checking for phantom capture events.
- Integration SQLite setup now waits for transient daemon DB locks before
  injecting PID-reuse test rows.
- Decision records keep their original event sequence after old capture events
  are pruned, so historical explanations stay useful.
- Replay no longer marks work as `superseded_external` unless history, `HEAD`,
  and the live worktree prove the queued change is already obsolete. The
  history probe is bounded by the per-event timeout.
- Deleted tracked files under skipped generated or gitignored directories are
  captured as deletes instead of being hidden by the parent directory skip.
- Same-branch fast-forwards, such as `git checkout main && git pull`, now
  refresh ACD's shadow baseline from the new `HEAD` instead of replaying stale
  work captured before the pull.
- Manual pause/resume now preserves self-heal behavior when an external commit
  lands during the pause, so the resumed daemon can mark matching work as
  already published instead of treating it like upstream-only content.
- Branch-transition settling avoids treating a ref move and its worktree update
  as separate local edits.

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
