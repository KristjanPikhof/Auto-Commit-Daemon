# Changelog

## Unreleased

### Added

- Explicit repo lifecycle commands: `acd repo init`, `acd repo list`, and
  `acd repo remove`. Autodiscovery stays on by default, but can be disabled
  with `~/.config/acd/config.json` or `ACD_REPO_AUTODISCOVERY`.
- Singleton intent windows now use the per-event commit-message provider,
  keeping one-capture commits cheaper while still emitting
  `replay.intent.singleton_shortcircuit` traces.

### Changed

- AI-generated commit messages now use a semantic subject plus wrapped `- `
  context bullets.
- Plain `acd wake` now only refreshes heartbeat and nudges capture/replay. Use
  `acd flush --logical` to force the current intent window through the batch
  gate.
- Hook wake acknowledgements moved to debug logs with periodic summaries.
- Human `acd repo list` output hides `STATE_DB`; `--json` still includes
  `state_db` for scripts.

### Fixed

- Planner overlap between `selected_seqs` and `deferred_seqs` is now rejected
  reliably, retried with a typed error, then safely falls back with reject
  logging.
- Intent grouping rationale stays in `grouping_reason` instead of leaking into
  commit bodies.
- Planner reject logging is wired at daemon startup, including 5 MiB rotation.

## v2026-05-16

Intent-planner atomicity: same-path coalesce, validation retry, planner-rejects forensics, Stop-hook rewire.

### Added

- `acd flush --logical`: explicit drain entrypoint for harness Stop / idle
  hooks. Refreshes heartbeat, enqueues a `flush_logical` request, signals the
  daemon to evaluate the pending window without waiting for `MIN_PENDING` or
  `MAX_PENDING_AGE`. Refuses on detached HEAD, in-progress git ops, or
  manual pause (heartbeat still runs). `--logical` requires an existing
  registered session; heartbeat-only mode keeps `acd touch` semantics.
- Stop-hook rewire for Claude Code, OpenCode `session.idle`, and Pi
  `session.idle`: now call `acd flush --logical` instead of `acd touch`.
  Partial work commits at agent-reply boundary, not the 5-minute age
  trigger. Codex Stop stays on `acd touch` (its Stop fires per tool turn).
  **Migration:** re-run `acd setup <harness>`; `acd doctor` flags drift.
- Same-path coalesce: replay folds runs of consecutive single-path captures
  into one planner offer with squashed before/after state. Stops at branch
  transitions, multi-path interleave, and barriers. On commit, every
  original seq publishes with the same `commit_oid` and gets its own
  `decision_records` row. Coalesced deferrals advance `defer_count` for
  every covered seq so external publish of the representative cannot reset
  the forced-aging clock. `ACD_INTENT_PATH_COALESCE` (default ON) toggles.
- Composed planner retry-on-invalid: `composed.PlanIntent` retries the
  primary once on typed `*IntentPlanValidationError`, quoting the validator
  message into a new `RetryCorrection` field. Skips retry on transport
  errors and on validator codes the normalizer already heals, so retries
  do not double-bill the provider. `ACD_INTENT_RETRY_ON_INVALID` (default
  ON) toggles.
- Forced-aging singleton fast path: when the offered window is length 1
  and the capture is overdue, the daemon synthesizes the plan locally
  with no provider call. Subject comes from a new diff-aware fallback
  (Go func, TS class/function, Python def, Markdown heading; basename
  verb form otherwise). `validateIntentSelectionSafety` and
  `ValidateIntentPlan` still run; subject extraction is bounded by a
  1-second budget.
- `<gitDir>/acd/planner-rejects.jsonl`: rotating forensic log of
  validator-rejected planner responses. 5 MiB per file, 2 retained. **Raw
  model output is redacted by default** (only code, message, offered
  seqs, size, sha256, parsed-plan summary persisted). Set
  `ACD_INTENT_REJECTS_RAW=1` to opt into verbatim capture (one-shot
  startup warning emitted). Treat the file as sensitive either way.
- `ACD_PATH_QUIESCENCE_SECONDS` (default `0`, off): defers planner-offer
  for path P until P has been quiet that long. Capture rows persist
  immediately. FIFO-preserving (gated head blocks the batch) and
  multi-op aware (gates on union of all touched op paths). Tracker
  bounded at 4096 entries with `2 * window` eviction; pays zero cost
  when disabled. `path_quiescence_gated_events` surfaces in `acd
  status`. Restart daemon to apply.
- `ACD_RECENT_COMMIT_AFFINITY_SECONDS` (default `0`, off): when `>0` and
  the most recent HEAD commit touched an offered path within the
  window, planner request carries a `path_recent_commits` hint
  suggesting `"extend or wait"`. Hint-only; no amend implemented.
- Intent system prompt adds three guarantees: same-path causality
  (deferring path P forces every later P seq to also defer), defer_count
  guidance (prefer captures with `defer_count >= 1`), forced_aging
  singleton rule restated explicitly. Worked example included.
- `ValidateIntentPlan` returns typed `*IntentPlanValidationError` with new
  codes (`IntentPlanValidationShape`, `IntentPlanValidationOfferedWindow`,
  `IntentPlanValidationDeferredReasonMissing`). Callers can `errors.As`
  to inspect code and seq. Existing untyped error paths unchanged.
- `acd status --json` and `acd diagnose --json` `intent_strategy` block
  gain `planner_error_rate_recent`, `singleton_commit_rate_recent`,
  `intent_stage_diff_cap`, and `path_quiescence_gated_events`. Both
  rate denominators fixed at 100 decisions; warn fires only once the
  ledger reaches a full window. `acd diagnose` surfaces a remediation
  hint above 5% planner-error rate, pointing at the rejects log.
- New integration tests in `test/integration/`:
  `intent_atomicity_test.go` (grouped commit + deferred-middle split),
  `intent_planner_recovery_test.go` (retry absorbs validation error +
  forced-singleton skips provider), `intent_flush_test.go` (`acd flush
  --logical` 2s budget + quiescence release timing),
  `flush_logical_test.go` (openai-compat flush coverage). Run under
  `-tags=integration`; default `make test` cycle unaffected.

### Changed

- `ACD_INTENT_DEFER_LIMIT` default `1` (was `2`). With the retry loop
  and forensic rejects log in place, a single deferral is more often
  planner churn than a real "wait for related work" signal. Set `=2`
  to restore prior tolerance.
- Intent planner stage uses a dedicated 16 KiB diff cap
  (`ai.IntentStageDiffCap`) per offered capture instead of the legacy
  4 KiB `ai.DiffCap`. Per-event commit-message path unchanged. New
  `BuildOpsDiffWithCap(ctx, repoRoot, ops, cap)` wraps the legacy
  `BuildOpsDiff` and threads the cap through `cappedDiffBuffer` so
  the larger budget actually applies on the wire.
- `NormalizeIntentPlanDeferredReasons` synthesizes a `DeferredReason`
  entry for any deferred seq the planner omitted, using the marker
  `IntentPlanReasonMarker = "planner omitted reason"`. Round-trips
  into `decision_records.reason` so operators see a non-blank
  explanation. Returns `(IntentPlan, dropped []int64, synthesized
  []int64)`; callers emit a single `slog.Warn` naming both lists.
  The `openai-compat` and `subprocess` providers also drop spurious
  entries; `composed.PlanIntent` runs the same normalize on primary
  and fallback paths.

### Fixed

- Path-quiescence FIFO ordering: removed in-place compaction of gated
  rows that could publish later events ahead of gated earlier events.
  The gate now skips the entire batch when the head is gated,
  preserving capture-seq as the canonical happened-before relation.
- Path-quiescence multi-op gap: gate previously checked only the
  header path. Now folds `touchedPaths(ops)` into the decision so all
  op paths must be quiescent.
- Unbounded `pathQuiescenceWrites` map growth. Now gated behind a
  startup-resolved atomic bool (zero cost when disabled), bounded at
  4096 entries with `2 * window` eviction when active.
- `persistPathQuiescenceSnapshot` write amplification: two `daemon_meta`
  writes per replay pass even when disabled. Now skips entirely when
  `pathQuiescence == 0` and only writes when the gated count differs
  from the last persisted value.
- `IntentStageDiffCap` was effectively dead before this release —
  `cappedDiffBuffer` hardcoded `ai.DiffCap` (4 KiB). The new
  `BuildOpsDiffWithCap` makes the 16 KiB budget actually apply.
- `acd flush --logical` authorization: previously lazy-registered any
  session-id and let any same-user process force commit boundaries.
  Now requires an existing registered client; unknown sessions return
  `refused_reason=unknown_session` with no enqueue.
- `acd flush` JSON `last_seen_ts` no longer serializes as `0` on the
  control-lock-held skip path. JSON tag now `omitempty`.
- Planner-rejects raw response is redacted by default. Models can echo
  prompt content (paths, diff text); verbatim persistence leaks via
  repo handoff, backups, or support bundles. Opt back in with
  `ACD_INTENT_REJECTS_RAW=1`.
- Planner-rejects rotation is now atomic via a single `os.Rename`. The
  prior `os.Remove` + `os.Rename` window could lose the `.1` archive
  on a crash between the two calls.
- Forced-aging singleton fast path now runs `validateIntentSelectionSafety`
  and `ValidateIntentPlan` on the synthesized plan; failures fall
  through to the deterministic slow path with `intent_planner_error`
  recorded.
- `runPrimaryWithRetry` no longer shadows the outer `plan` variable
  inside the success block.
- `acd status` no longer over-subtracts the path-quiescence gated
  count from `visible_pending_events` when the daemon is dead or the
  snapshot is stale (>30s old).
- `planner_error_rate_recent_warn` no longer fires during the first
  100 decisions: dilution against the fixed-100 denominator could
  trip the threshold on 5 errors in 5 rows. Warn requires a full
  window.
- `subject_fallback`: skips Go `func main(` as a low-value symbol;
  `tsMethodRE` now requires at least one modifier prefix
  (`public|private|protected|static|async`) so call expressions do
  not hijack the subject.
- Setup tests now assert format-specific managed markers
  (`_acd_managed: true` in JSON, `# acd-managed: true` in YAML) so a
  future template edit cannot silently break `acd doctor` drift
  detection.
- `acd commit-all` error messages point at `acd fix --clear-pause`
  instead of deprecated `acd recover --auto`.
- Self-heal for blocked replay barriers: the daemon probes
  `blocked_conflict` rows before draining pending captures and
  promotes any row whose captured after-state already matches HEAD.
  No new commit minted; ledger records `handled_external_after_block`.
- Codex install detection recognizes repo-local `.codex/hooks.json`
  and `.codex/config.toml` from the current Git worktree root,
  alongside user-level paths.
- `internal/git/history.go`: replaced custom `parseInt64` with
  `strconv.ParseInt`.

### Recovery

- `acd fix` is the single recovery entrypoint. Planner covers
  resolve-already-landed-barrier, retarget-stale-anchor,
  delete-obsolete-barrier, mark-external-published,
  clear-expired-manual-pause, clear-drained-backpressure under
  `--yes`. Add `--force` to also purge blocked barriers with pending
  successors. `state.db` is backed up before any mutation; refuses
  while a live daemon owns the database.
- `acd diagnose --json` reports `auto_resolvable_blocked_count` and
  `barrier_with_successors_count`. Human output points at the right
  `acd fix` invocation.

### Deprecated

- `acd recover` and `acd purge-events` remain deprecated and hidden
  from help. Both print a one-line deprecation warning to stderr and
  keep working. Switch to `acd fix` (and `acd fix --force` for
  purge). Hard removal is tracked separately and will land on a
  dedicated branch on top of the next release tag.

### Docs

- README rewritten for clarity. Side-by-side recommended configs
  (deterministic event vs intent gpt-5.4-mini), "When commits stop
  appearing" recovery flow, "Migrating from prior releases" section,
  full env table covering every new var.

## v2026-05-13

### Added

- `acd start`, `acd status`, `acd diagnose`, `acd events`, `acd logs`,
  `acd prompt`, `acd recover`, and `acd daemon run` now resolve subdirectories
  to the canonical Git worktree root via a shared `git.ResolveWorktree` helper.
  Starting ACD from `repo/subdir` refreshes the existing repo daemon and
  central-registry row instead of creating a duplicate subdirectory entry.
  Non-Git directories are refused with a clear error (`ErrNotWorktree`).
- `acd start` gains a registry-backed early short-circuit that resolves
  canonical repo identity from a fresh central-registry row plus the session's
  start-cache before invoking `git rev-parse`. Existing path-vs-hash lookup
  also prefers exact path matches so stale legacy duplicates no longer shadow
  the canonical row.
- `acd gc` now merges legacy duplicate registry rows that share a Git
  toplevel or `state.db` before pruning. `acd gc --json` adds a `merged[]`
  array of `{kept_path, dropped_path, reason}` entries alongside the existing
  `dropped[]` and `kept` fields.
- `acd setup opencode` and `acd setup pi` now print the canonical default hook
  paths (`~/.config/opencode/hook/hooks.yaml` and
  `~/.pi/agent/hook/hooks.yaml`). `acd doctor` detects acd-managed hooks at
  those paths and points remediation guidance at them. Older hook paths
  (`~/.config/opencode/hooks.yaml`, `~/.pi/hook/hooks.yaml`) remain detected
  as a secondary fallback during migration.
- `acd doctor` now surfaces the actual file that carries the acd marker when
  it differs from the canonical primary. JSON adds a `matched_path` field on
  each harness report; human output prints a `marker found at` line. Drift
  scans run against the matched file, and remediation switches to a
  merge-only nudge toward the canonical path so users on a legacy fallback
  are never told to overwrite a non-canonical file they hand-authored.
- `README.md`, `templates/opencode/README.md`, and `templates/pi/README.md`
  now document the `# acd-managed: true` marker requirement and explain how
  to recover when `acd doctor` reports detection as `no` despite the hook
  file existing. The README harness table splits "hook support" (native vs
  external engine) from install location for clearer onboarding.

## v2026-05-10

### Added

- `acd diagnose` now reports the most recent dead-branch cleanup. JSON output
  includes `dead_branch_prune_last_run_ts`,
  `dead_branch_prune_last_count`, and `dead_branch_prune_last_refs`; human
  output prints a `Dead-branch prune:` row after a cleanup removes rows.

### Fixed

- Deleted feature branches no longer leave phantom replay blockers behind.
  ACD prunes stale `pending`, `blocked_conflict`, and `failed` rows for dead
  branch refs during runtime branch changes and daemon startup. Paused repos
  are left untouched, and `ACD_KEEP_DEAD_BRANCH_BARRIERS=1` keeps the old rows
  for forensic inspection.

## v2026-05-08

### Added

- `acd commit-all` can capture and replay a dirty worktree without starting the
  persistent daemon. It is meant for cold starts, repos where the daemon was off,
  and onboarding existing work into ACD history. It supports `--dry-run`, `--yes`,
  `--json`, and `--repo`, refuses unsafe git states, and uses the active commit
  strategy instead of adding a separate strategy flag.
- `acd start` short-circuits repeated active-hook calls through a per-session
  cache file at `<gitDir>/acd/start-cache-<sessionhash>.json`. The hot path
  returns in roughly 50 ms instead of the cold-path budget of 1 s, while the
  cold path is unchanged. The cache stores a daemon fingerprint (start time
  plus argv hash) so PID reuse on long-running boxes cannot serve a stale fast
  path for an unrelated process. Each hot-path call also refreshes
  `daemon_clients.last_seen_ts` so the refcount sweeper does not evict a
  session that lives entirely on the fast path.
- `acd doctor` flags installed snippet drift. It warns when active hook bodies
  in `~/.claude/settings.json`, `~/.codex/hooks.json`,
  `~/.config/opencode/hooks.yaml`, or `~/.pi/hook/hooks.yaml` lack both the
  `acd start` and `acd wake` calls or the log fallback, and prints
  per-harness remediation pointing at `acd setup <harness>` with merge
  instructions.
- `acd doctor` tails the Codex hook log (last 50 lines or 5 minutes) and
  surfaces the error count plus the first failing line. Output also reports
  config read errors separately from marker-missing on EACCES/EIO via a new
  `config_read_error` field in the JSON view.
- `acd setup <harness> --raw` validates JSON for `.json` snippet targets
  before emitting. Invalid templates exit non-zero with the byte offset
  instead of letting users redirect malformed JSON into
  `~/.codex/hooks.json`.

### Changed

- **Codex hooks v2 is a breaking setup change.** `acd setup codex` now writes
  `~/.codex/hooks.json` instead of the legacy TOML hook snippet. To migrate,
  run `acd setup codex` and merge the printed JSON into `~/.codex/hooks.json`
  manually. If the file contains only the acd block you can redirect directly:
  `acd setup codex --raw > ~/.codex/hooks.json`. **Warning:** the redirect
  replaces the entire file. If you have custom non-acd hooks in
  `~/.codex/hooks.json`, back up the file first and merge by hand instead of
  using `>`. After writing the file, remove the old `# acd-managed: true` block
  from `~/.codex/config.toml`, then approve the new hooks with `/hooks` inside
  Codex. Codex re-flags all hook entries as review-required after every
  `hooks.json` content change, so re-run `/hooks` after any re-install too.
  `acd doctor` now warns when both old and new Codex hook configs are installed,
  because Codex will run both. Codex hooks are enabled by default; if
  `~/.codex/config.toml` pins feature flags, keep lifecycle hooks enabled with
  `[features].hooks = true` (`codex_hooks` is only a deprecated alias). See
  [templates/codex/README.md](templates/codex/README.md) for full details.
- Codex hooks now read `cwd` from hook stdin, no longer require
  `CODEX_PROJECT_DIR`, and use `acd hook-stdin-extract session_id cwd?` for the
  hook payload. The helper also supports multiple fields and optional fields.
  Codex hook bodies capture helper exit explicitly: a failing
  `acd hook-stdin-extract` now logs `[ts] active hook failed exit=N
  cmd=acd-hook-stdin-extract` to the hook log instead of being swallowed by a
  trailing `|| exit 0`.
- Claude Code, Codex, OpenCode, and Pi active hooks now run idempotent
  `acd start` before `acd wake`. ACD can recover after a manual `acd stop`
  without waiting for a brand-new harness session. **Migration:** if you ran
  `acd stop --all` and the daemon does not restart automatically when the next
  prompt or tool event fires, your installed snippet is stale. Re-run
  `acd setup <harness>` and merge the updated hooks block into your installed
  config. **Warning:** redirecting `acd setup <harness> --raw > <config-path>`
  replaces the entire file. Back it up first if you have custom non-acd
  entries. See the per-harness READMEs
  ([claude-code](templates/claude-code/README.md),
  [codex](templates/codex/README.md),
  [opencode](templates/opencode/README.md),
  [pi](templates/pi/README.md)) for merge instructions. Run `acd doctor` to
  identify which harness needs updating.
- Pi SID fallback is now per-process unique (`pi-$$-$(date +%s)`) instead of
  a shared `unknown`. Multiple Pi sessions without `PI_SESSION_ID` no longer
  collapse onto the same client, so the first `session.deleted` does not tear
  down the daemon while other sessions are still active.
- `acd doctor` drift remediation suggests `acd setup <harness>` (merge into
  config) by default, with the `--raw` redirect form only after backing up
  the target file. README and per-harness READMEs for claude-code, opencode,
  and pi now carry the same overwrite warning the codex template already
  shipped.
- `templates/codex/uninstall.md` documents surgical removal of the five acd
  hook entries and the `_acd_managed` flag instead of deleting
  `~/.codex/hooks.json` outright. The previous wording destroyed merged
  custom hooks.
- `acd setup shell --raw` writes a blank line between the direnv and zshrc
  snippets so callers can split them into separate files cleanly.
- `acd status`, `acd diagnose`, and `acd doctor` now report the effective commit
  strategy from daemon metadata first, then `ACD_COMMIT_STRATEGY`, then the
  default. Unknown daemon values fall back to the environment-derived strategy
  and emit a warning instead of leaking into user output.

### Fixed

- `acd commit-all` now force-reseeds `shadow_paths` from `HEAD` and drops
  stale pending capture events before capture. It no longer reports a clean
  worktree when an earlier daemon session had absorbed unreplayed edits into
  shadow state. Human and JSON output now show the reseed note and
  `dropped_stale_pending` count.
- `acd doctor` YAML drift detection now actually fires for OpenCode and Pi
  configs. The previous parser misread nested `actions` items as new
  top-level hook entries, dropped the parent association, and silently
  emitted zero drift for every real config.
- `acd doctor` `parseLogTimestamp` now matches the bracketed timestamp shape
  hook templates write (`[2026-05-08T12:34:56-0700] ...`). The five-minute
  recency window was effectively disabled before because every line fell
  through to the no-timestamp branch and counted as recent.
- `acd doctor` no longer flags JSONL info lines that contain the substring
  `failed` (for example `failed_blocking_pending=0`). Only wrapper-printf
  failure lines and JSONL `level=error|fatal` are counted as hook errors.
- `acd doctor` distinguishes "config unreadable" from "marker missing" on
  EACCES/EIO. JSON consumers can now read `config_read_error` to tell the
  two cases apart.
- The YAML `acd-managed` marker now requires the `# ` comment prefix. A
  hand-edited config containing bare `acd-managed: true` is no longer
  detected as an acd install, and the marker no longer collides with the
  TOML form as a substring.
- `acd start` short-circuit cache writes use unique temp filenames. Two
  concurrent active hooks no longer collide on a shared
  `start-cache.json.tmp` and corrupt the cache file.
- `acd stop` invalidates the start cache on the `Deferred` path and after a
  failed force-stop escalation. A deferred stop no longer leaves a fresh
  cache for a session that was just deregistered, and a daemon that
  survived `--force` no longer leaves stale caches behind.
- Codex `SessionStart` no longer swallows helper failures. A missing `acd`
  binary, oversized stdin, or malformed JSON now surfaces as a logged
  failure with the real exit code instead of a silent skip.

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
