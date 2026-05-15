# Changelog

## Unreleased

## v2026-05-16

The intent-planner atomicity epic. Same-path coalesce, validation retry, planner-rejects forensics, Stop-hook rewire, plus a 7-way code review pass that hardened the FIFO and multi-op semantics of the new path-quiescence gate before tagging.

### Added

- **`acd flush --logical`** explicit drain entrypoint for harness Stop / idle
  hooks. Refreshes the heartbeat, enqueues a `flush_logical` flush_request,
  and signals the daemon so the next replay tick evaluates the visible
  pending window without waiting for `ACD_INTENT_MIN_PENDING` or
  `ACD_INTENT_MAX_PENDING_AGE`. Refuses on detached HEAD, in-progress git
  operations (rebase/merge/cherry-pick/bisect), and manual pause markers
  while still refreshing the heartbeat. Refusals surface `refused_reason`
  in JSON output without blocking the harness hook. Without `--logical` the
  command degrades to `acd touch` semantics. **`--logical` requires an
  existing registered session**; lazy registration only happens on the
  heartbeat-only path so a stray local process cannot force commit
  boundaries.

- **Stop-hook rewire** for Claude Code, OpenCode `session.idle`, and Pi
  `session.idle` from `acd touch` to `acd flush --logical`. Partial work
  now commits when an agent finishes a reply rather than waiting up to 5
  minutes for the age trigger. Codex Stop deliberately stays on `acd touch`
  because its Stop event fires per tool turn (a logical flush per Stop
  would commit per tool run). **Migration:** existing snippets keep working
  but lose the new commit boundary. Re-run `acd setup <harness>` to opt
  in; `acd doctor` flags the drift.

- **Same-path pre-coalesce** of consecutive captures for intent planning.
  The replay window builder folds runs of consecutive captures touching
  exactly one shared path into a single offered planner entry. The
  squashed entry carries the run's first observed `BeforeOID`/`BeforeMode`
  and the run's last observed `AfterOID`/`AfterMode`, so write-tree builds
  the same final blob as the unmerged chain. Renames, deletes, multi-path
  captures, and any divergence in branch_token (`branch_ref`,
  `branch_generation`, `base_head`) close a run; terminal barriers never
  appear in the input because `state.PendingEvents` already filters them.
  On commit success every original seq absorbed by the representative is
  marked published with the same `commit_oid` and gets its own
  `decision_records` row, so `acd events --json grouped_seqs` reports the
  full coverage. Also: when the planner defers a coalesced offer,
  `defer_count` now advances for every covered seq (not just the
  representative), so external publish of the representative cannot reset
  the forced-aging clock for the rest of the run. `ACD_INTENT_PATH_COALESCE`
  controls the feature; default ON. Set to `0|false|no|off` to opt out
  (restart the daemon to apply).

- **Composed planner retry-on-invalid** loop. `composed.PlanIntent` retries
  the primary planner once when `ValidateIntentPlan` returns a typed
  `*IntentPlanValidationError`. The retry quotes the validator message
  verbatim into a new `IntentPlanRequest.RetryCorrection` field that
  providers fold into the user prompt as a "your previous tool call failed
  validation; correct it" follow-up. Transport errors (timeouts, HTTP
  errors, context cancellation, network failures), untyped errors, and
  validator codes the normalizer already heals
  (`IntentPlanValidationDeferredReasonNotDeferred`,
  `IntentPlanValidationDeferredReasonMissing`) skip the retry to avoid
  double-billing the provider. Capped at one attempt; a second invalid
  response surfaces the typed error so replay records
  `intent_planner_error` and runs the deterministic fallback.
  `ACD_INTENT_RETRY_ON_INVALID` (default ON) toggles the loop globally.

- **Forced-aging singleton fast path.** When the offered window has length
  1 and the capture is overdue (`defer_count >= ACD_INTENT_DEFER_LIMIT`),
  the daemon synthesizes the plan locally and publishes without calling
  the provider. Subject comes from a new diff-aware fallback
  (`internal/ai/subject_fallback.go`) extracting the touched Go func, TS
  class/function, Python def, or Markdown heading; falls back to
  `Add/Update/Remove <basename>` per op kind. Both
  `validateIntentSelectionSafety` and `ai.ValidateIntentPlan` still run on
  the synthesized plan so the safety invariants the slow path enforces
  hold on the fast path too. The diff-aware subject extraction runs under
  a 1-second budget — on timeout the path falls back to the cheap
  basename-verb form so forced-aging always makes progress.

- **`<gitDir>/acd/planner-rejects.jsonl`** rotating forensic log of
  validator-rejected planner responses. 5 MiB per file, 2 files retained
  (current + `.1`); rotation uses an atomic `os.Rename` so the prior
  archive never disappears mid-rotation. **Raw model output is redacted
  by default**: only the validation code, message, offered seqs, response
  size, sha256, and parsed-plan summary
  (`{selected_count, deferred_count}`) are persisted. Set
  `ACD_INTENT_REJECTS_RAW=1` to opt into verbatim capture for offline
  debugging; the daemon emits a one-shot `slog.Warn` at startup when
  verbatim mode is on. Treat the file as sensitive either way — even
  redacted records can leak via repo handoff or backups. Writes are
  best-effort behind a mutex; failures surface as `slog.Warn` and never
  block the planner path.

- **Per-path quiescence gate** `ACD_PATH_QUIESCENCE_SECONDS` (default
  `0`, off). When `>0`, defers offering pending captures for path P to
  the planner until P has been quiet for the configured number of
  seconds. The capture row itself persists immediately for durability;
  only the planner-offer is gated. The gate is **FIFO-preserving**: if
  the head pending event is gated, the entire batch waits — no
  compaction around the head, so cross-path causality cannot be
  reordered. The gate is **multi-op aware**: it loads `capture_ops` and
  checks the union of all touched paths plus `OldPath`, so a multi-op
  event whose header path is quiet but whose second op was just written
  is held back. The tracker is bounded to 4096 entries with eviction at
  `2 * window` age. Stamping is gated behind a startup-resolved atomic
  bool so a daemon with the feature disabled pays zero per-capture
  bookkeeping cost. The daemon stamps
  `daemon_meta.path_quiescence.{gated_count, updated_at}` on every
  replay pass (only when the value changes); `acd status --json` reads
  both keys, surfaces `path_quiescence_gated_events`, and only subtracts
  from `visible_pending_events` when the snapshot is fresh AND the
  daemon is alive — a stale or dead-daemon stamp no longer
  over-subtracts the operator's view. `oldest_pending_age_seconds` stays
  anchored to the row's `captured_ts` and is never shifted by the gate.
  Restart the daemon for env changes to apply.

- **Prior-commit affinity hint** `ACD_RECENT_COMMIT_AFFINITY_SECONDS`
  (default `0`, off). When `>0` AND the most recent HEAD commit
  reachable from the active branch touched an offered capture's path
  AND landed within the window, the planner request now carries a
  `path_recent_commits[i] = {path, oid, age_seconds, suggested_action}`
  entry. `suggested_action` is fixed at `"extend or wait"` in v1 — the
  hint is informational only; the daemon does NOT amend on the
  planner's behalf. The hint is forwarded through the composed primary
  + retry + fallback paths so every planner invocation sees consistent
  context. Default flipped from `120` to `0` because the lookup costs
  N `git log` per replay pass; opt back in for small repos or when
  prototyping amend flows. Follow-up: act on the hint with an actual
  amend path (tracked separately).

- **Intent planner system prompt** carries three additional guarantees:
  (1) **same-path causality** — deferring an offered seq for path P
  forces every later offered seq touching P to also be deferred, so a
  same-path chain never gets split; (2) **defer_count guidance** —
  captures whose `defer_count >= 1` should be preferred for inclusion
  when evidence permits, preventing endless churn; (3) the
  **forced_aging singleton rule** is restated explicitly with empty
  deferred lists. The worked example demonstrates the same-path
  scenario end to end. Network and plugin planner providers pick this
  up automatically through `IntentPlannerSystemPrompt()`.

- **`ValidateIntentPlan`** now returns `*IntentPlanValidationError` for
  every failure path, with new `IntentPlanValidationCode` values
  (`IntentPlanValidationShape`, `IntentPlanValidationOfferedWindow`,
  `IntentPlanValidationDeferredReasonMissing`) covering shape errors,
  out-of-window seqs, and missing deferred reasons. The existing
  `IntentPlanValidationDeferredReasonNotDeferred` code is unchanged.
  Callers that only check the error message continue to work; new
  callers can `errors.As` to inspect the typed code and seq. Used by
  the composed retry loop above to decide whether a retry is
  worthwhile.

- **Status / diagnose observability**. `acd status --json` and
  `acd diagnose --json` `intent_strategy` block gain
  `planner_error_rate_recent` (share of `intent_planner_error` rows
  over the most recent 100 decisions),
  `singleton_commit_rate_recent` (share of one-event commits over the
  most recent 100 distinct commit OIDs), `intent_stage_diff_cap`
  (active per-stage planner diff budget), and
  `path_quiescence_gated_events`. Both rate denominators are fixed at
  100 regardless of how many rows the ledger holds, so the rates
  dilute toward zero while the ledger fills. The
  `planner_error_rate_recent_warn` flag only fires once the ledger
  reaches the full 100-decision window; before that the threshold
  check is suppressed to avoid first-hour false alarms. `acd diagnose`
  surfaces a remediation hint when the rate exceeds 0.05 (5%),
  pointing at the rejects log. The status human renderer adds an
  `Intent rates (last 100): ...` summary line when either rate is
  non-zero.

- **Cross-cutting integration coverage** for the intent epic in
  `test/integration/`:
  - `intent_atomicity_test.go` — drives a four-file create batch
    through the real `acd` binary against a mock openai-compat server
    and asserts the daemon publishes one grouped commit covering
    every capture seq. Companion three-file scenario pins the
    at-least-two-commits negative arm: when the planner defers the
    middle seq, A+C land as one commit and the middle stays pending
    with `planner_state.defer_count >= 1`, no coalesce across the
    planner-drawn boundary.
  - `intent_planner_recovery_test.go` — proves the composed retry
    loop end-to-end (mock provider returns invalid first call, valid
    on retry; daemon publishes the grouped commit and records ZERO
    `intent_planner_error` decisions). Second test pins the
    forced-aging singleton fast path: planner defer + next replay
    publishes WITHOUT another planner call (provider hit count
    unchanged) using the diff-aware Go-symbol subject.
  - `intent_flush_test.go` — deterministic-provider `acd flush
    --logical` budget assertion (HEAD must advance within 2s) plus
    `ACD_PATH_QUIESCENCE_SECONDS=2` opt-in test proving two same-path
    saves 500ms apart are held back by the quiescence gate, surface
    as a single one-commit window once the quiet period elapses, and
    bump `daemon_meta.path_quiescence.gated_count`.
  - `flush_logical_test.go` — openai-compat coverage of the same
    flush budget against a mock network provider.
  These tests run under the existing `-tags=integration` build gate
  so the default `make test` cycle is unaffected.

### Changed

- **`ACD_INTENT_DEFER_LIMIT` default is now `1`** (was `2`). The Wave 2
  retry loop in `composed.PlanIntent` plus the new
  `<gitDir>/acd/planner-rejects.jsonl` forensic surface mean an event
  that has already been deferred once is overwhelmingly more likely to
  be planner churn than a legitimate "wait for related work" signal.
  Lowering the default forces the forced-aging singleton path sooner
  so deferred work lands promptly. Operators who want the historical
  behaviour can still set `ACD_INTENT_DEFER_LIMIT=2` explicitly.

- **Intent planner stage uses a dedicated 16 KiB diff cap** (the new
  `ai.IntentStageDiffCap` constant) per offered capture instead of the
  legacy 4 KiB `ai.DiffCap`. The per-event commit-message path
  (`Generate`) is unchanged — only the `PlanIntent` path uses the
  larger cap. Implementation: `BuildOpsDiff` now wraps a new
  `BuildOpsDiffWithCap(ctx, repoRoot, ops, cap)` that threads the cap
  through `cappedDiffBuffer` and the per-op git stdout limit, so the
  larger cap actually applies on the wire. Trace records and
  prompt-trace metadata report the stage cap (16000) on intent
  requests so operators inspecting per-stage payload sizes see the
  active budget. Total payload stays bounded by `ACD_INTENT_WINDOW`
  (default 10) × cap, comfortably under the openai-compat 1 MiB body
  limit.

- **`NormalizeIntentPlanDeferredReasons`** now also synthesizes a
  `DeferredReason` entry for any deferred seq the planner omitted,
  using the constant marker text `IntentPlanReasonMarker = "planner
  omitted reason"`. The marker round-trips into
  `decision_records.reason` so operators inspecting deferred captures
  see a non-blank explanation. The function returns
  `(IntentPlan, dropped []int64, synthesized []int64)`; callers emit
  a single `slog.Warn` (`"intent planner: normalized
  deferred_reasons"`) naming both lists, and the daemon's
  defense-in-depth re-normalization in `planIntentWithFallback`
  discards both return values so the second pass stays silent. The
  `openai-compat` and `subprocess` providers also drop entries whose
  seq is selected or non-offered. `composed.PlanIntent` runs the same
  normalize-then-warn on both primary and fallback paths so any
  third-party `IntentPlanner` wired through `Compose` is covered.
  Plans that still fail `ValidateIntentPlan` after normalization fall
  back to deterministic one-capture commits with an
  `intent_planner_error` ledger entry.

### Fixed

- **Path-quiescence FIFO ordering.** The first cut of
  `filterPendingByPathQuiescence` removed gated rows in place;
  `selectIntentWindow` then received a compacted slice that could
  start after the true FIFO head. An old pending event for path A
  being restamped while a later pending event for path B was already
  quiet would let B publish first. Cross-path dependent commits could
  split or land in reverse causal order. The gate now skips the
  entire batch when the head is gated, preserving capture-seq as the
  canonical happened-before relation.

- **Path-quiescence multi-op gap.** The gate previously checked only
  `CaptureEvent.Path` and `OldPath`. Multi-op captures whose header
  path was quiet but whose second op path had just been written
  slipped the gate. The check now folds `touchedPaths(ops)` into the
  decision so the full op-path set must be quiescent.

- **Unbounded `pathQuiescenceWrites` growth.** The package-level map
  was stamped on every captured op + every rename `OldPath`, with no
  eviction, even when the gate was disabled (default config). A
  long-running daemon on a large repo accrued one entry per
  ever-touched path. The map is now gated behind a startup-resolved
  atomic bool so it stays empty when the feature is off, bounded at
  4096 entries with `2 * window` eviction when on, and
  `PathQuiescenceTrackerSize()` is exposed for diagnose surfacing.

- **`persistPathQuiescenceSnapshot` write amplification.** Two
  `daemon_meta` writes fired per replay pass even with the gate
  disabled. Now skips entirely when `cfg.pathQuiescence == 0` and
  only writes when the gated count differs from the last persisted
  value. Removes 10–40 spurious SQLite UPDATEs/sec under fsnotify
  storms.

- **`IntentStageDiffCap` was effectively dead** before this release —
  `cappedDiffBuffer` hardcoded `ai.DiffCap` (4 KiB) so the planner
  truncated to 16 KiB after already seeing only 4 KiB. The new
  `BuildOpsDiffWithCap` wires the cap through to the buffer and the
  per-op git stdout limit so the 16 KiB intent-stage budget actually
  applies.

- **`acd flush --logical` authorization.** Previously lazy-registered
  any caller-supplied session-id with harness `"other"`, then
  enqueued the flush_logical request with `BypassMinPending=true`.
  Any same-user process could force planner drains at chosen
  boundaries. Now requires an existing registered client when
  `--logical` is set; unknown sessions return ok=true with
  `refused_reason=unknown_session` and no flush request enqueued.
  Heartbeat-only mode keeps lazy-register behavior.

- **`acd flush` JSON `last_seen_ts: 0` on skip path.** The
  control-lock-held branch returned `last_seen_ts: 0` (epoch 1970)
  because the field had no `omitempty` tag. Downstream parsers
  computing `now - last_seen_ts` saw a stale-by-decades signal. JSON
  tag now `omitempty`; absent field signals "not refreshed this
  call".

- **Planner-rejects log raw response default-redact** (security).
  Previously persisted up to 1 MiB of verbatim model output to
  `<gitDir>/acd/planner-rejects.jsonl`. Models occasionally echo
  prompt content (paths, captured diff text, subjects) so the file
  could accumulate sensitive data that propagates via repo handoff,
  backups, or support bundles. Now redacts by default — only code,
  message, offered seqs, response size, sha256, and parsed-plan
  summary are kept. `ACD_INTENT_REJECTS_RAW=1` opts back into
  verbatim capture with a one-shot startup warning.

- **Planner-rejects rotation atomicity.** Previous rotation did
  `os.Remove(rotated)` then `os.Rename(current, rotated)`; a crash
  between the two calls lost the prior `.1` archive. Now relies on
  POSIX `rename(2)` atomic overwrite so the archive is always
  present at any observable point.

- **Forced-aging singleton fast path skipped safety validators.**
  The first cut routed a synthesized plan straight to
  `publishIntentSelection` without running
  `validateIntentSelectionSafety` or `ai.ValidateIntentPlan`. Both
  now run on the fast path; on validation failure the daemon records
  `intent_planner_error` and falls through to the deterministic
  slow path.

- **`runPrimaryWithRetry` `plan` shadowing.** The inner
  `plan, dropped, synthesized := …` shadowed the outer `plan`, so a
  future maintainer pulling `return plan, nil` outside the inner
  block would silently return the un-normalized plan. Pre-declared
  `dropped, synthesized` once and switched the inner site to `=`.

- **`acd status` over-subtracted on dead daemon.** The
  path-quiescence gated-count meta read was applied unconditionally;
  if the daemon crashed mid-pass leaving `gated_count=5` the next
  status read reported pending=2 when the SQL truth was pending=7.
  Now requires a fresh `path_quiescence.updated_at` (within 30s)
  AND a live daemon before subtracting.

- **`PlannerErrorRateRecentWarn` first-hour false alarms.** With
  the fixed-100 denominator, 5 errors in the first 5 decisions
  computed to rate=0.05 = threshold and tripped the warn. The flag
  now only fires once `decision_records >= 100` so dilution never
  drives a false alarm during boot.

- **`subject_fallback` regex tightening.** Skips Go `func main(` as
  a low-value symbol; falls through to next match or basename verb
  form. `tsMethodRE` now requires at least one modifier prefix
  (`public|private|protected|static|async`) so arbitrary
  `someCallback(opts): void {` lines no longer hijack the subject.

- **Setup tests assert format-specific managed marker.** New Stop /
  idle hook tests now decode the JSON for Claude Code and assert
  `_acd_managed == true`, and grep YAML for literal
  `# acd-managed: true` for OpenCode and Pi. Without these
  assertions a future template edit could drop the marker, pass the
  body assertion, and silently break `acd doctor` drift detection.

- **`acd commit-all` error messages** now point at
  `acd fix --clear-pause` instead of the deprecated
  `acd recover --auto`. Hidden help and stderr deprecation banner
  are unchanged.

- **Self-heal for blocked replay barriers.** The daemon's replay
  pass probes `blocked_conflict` rows before draining pending
  captures and promotes any row whose captured after-state already
  matches HEAD. No new commit is minted; the trace record carries
  event class `replay.self_heal` and the decision ledger records
  `handled_external_after_block`. Rows that fail the predicate stay
  blocked for operator inspection. Triggered by a real incident
  where one stuck `blocked_conflict` hid 94 pending captures whose
  captured changes already existed at HEAD.

- **Codex install detection** now recognizes repo-local
  `.codex/hooks.json` and `.codex/config.toml` from the current Git
  worktree root, alongside the user-level paths. `acd setup` and
  `acd doctor` pick up project-local Codex installs without a
  user-level config. The user-scoped `~/.codex/hooks.json` stays
  the canonical `ConfigPath`; `MatchedPath` reports the repo-local
  path when only the project file carries the marker.

- **`internal/git/history.go` parseInt64** replaced with
  `strconv.ParseInt`. Behavioral parity, removes a small drift
  surface.

### Recovery

- **`acd fix` is the single recovery entrypoint.** The planner covers
  `resolve_already_landed_barrier`, `retarget_stale_anchor`,
  `delete_obsolete_barrier`, `mark_external_published`,
  `clear_expired_manual_pause`, and `clear_drained_backpressure`
  under `--yes`. Add `--force` to opt into
  `purge_barrier_with_successors` for blocked barriers that still
  hold pending rows behind them; combine with `--yes` to apply.
  `--force` without `--yes` stays dry-run. `state.db` is backed up
  before any mutation, and every action refuses while a live
  daemon owns the database.

- **`acd diagnose --json`** reports `auto_resolvable_blocked_count`
  and `barrier_with_successors_count`. The human output points
  operators at `acd fix --dry-run` or `acd fix --force --dry-run`
  based on which case applies.

### Deprecated

- **`acd recover` and `acd purge-events` remain deprecated** and
  hidden from help. Both print a one-line deprecation warning to
  stderr and keep working for one release. Switch to `acd fix` (and
  `acd fix --force` for purge). Hard removal is tracked separately
  and will land on a dedicated branch on top of the next release
  tag, not on this self-host branch.

### Docs

- **README rewritten** for clarity. New sections: side-by-side
  recommended configs (deterministic event vs intent gpt-5.4-mini),
  "When commits stop appearing" recovery flow, "Migrating from
  prior releases" with the Stop hook rewire and the
  planner-rejects.jsonl forensic surface. Env table now covers
  every Wave 2 var (`ACD_INTENT_PATH_COALESCE`,
  `ACD_INTENT_RETRY_ON_INVALID`, `ACD_INTENT_REJECTS_RAW`,
  `ACD_PATH_QUIESCENCE_SECONDS`, `ACD_RECENT_COMMIT_AFFINITY_SECONDS`)
  with defaults and rationale. Net length down from ~466 lines to
  ~250 while gaining migration coverage.

### Self-host smoke evidence

Recorded against the `feat/intent-planner-atomicity-fixes` branch
with the freshly-built intent-strategy daemon running throughout
the Wave 3 verification pass:

- **`planner_error_rate_recent` = 0.0** over the most recent
  100-decision window (target `< 0.03`).
- **71 multi-file intent groups** landed (size 2–10): 39×size-2,
  12×size-3, 9×size-4, 3×size-5, 3×size-6, 1×size-7, 2×size-8,
  2×size-10. Target was `>= 3` multi-file groups.

Reproduce on any repo:

~~~bash
acd diagnose --repo . --json | jq '.intent_strategy.planner_error_rate_recent'

sqlite3 .git/acd/state.db "
  SELECT cnt, COUNT(*)
  FROM (
    SELECT COUNT(*) cnt
    FROM decision_records
    WHERE kind='committed' AND commit_oid IS NOT NULL
    GROUP BY commit_oid
  )
  GROUP BY cnt;
"
~~~

A continuous fresh-start 30-minute smoke run remains a manual
operator gate before tagging; the in-session evidence above
demonstrates the daemon meets the metric thresholds the smoke is
designed to catch.

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
  because Codex will run both. Keep Codex lifecycle hooks enabled in
  `~/.codex/config.toml` with `[features].codex_hooks = true`. See
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
