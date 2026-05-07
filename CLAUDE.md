# Agent guide

## Basics

- `acd`: static Go CLI/daemon, MIT, macOS/Linux `arm64`/`amd64`; no Windows v1.
- Module `github.com/KristjanPikhof/Auto-Commit-Daemon`; Go `1.22`; `modernc.org/sqlite v1.36.0`; date tags `vYYYY-MM-DD`.

```bash
make build          # static bin/acd, CGO_ENABLED=0, -tags=netgo,osusergo
make test           # go test ./... -race -count=1
make lint           # go vet ./... + gofmt check
make fmt            # gofmt -w .
make tidy           # go mod tidy
./bin/acd version
```

Required before done/push/PR/final:

```bash
make lint
make test
go test ./test/integration/... -tags=integration -race -count=1 -timeout 5m
go test ./internal/daemon/... ./internal/git/... ./internal/state/... ./internal/pause/... ./internal/cli/... -race -count=3 -timeout 10m
```

Release smoke updates active local ACD. Ask before install:

```bash
make build && install -m 0755 ./bin/acd ~/.local/bin/acd
git tag v2026-MM-DD && git push origin v2026-MM-DD && gh run watch
gh release edit v2026-MM-DD --prerelease=false --latest
ACD_VERSION=v2026-MM-DD sh scripts/install.sh
```

## Map

- `cmd/acd/main.go`: entrypoint.
- `internal/cli`: Cobra commands.
- `internal/daemon`: run loop, capture/replay, intent grouping, branch tokens, shadow/bootstrap, fsnotify, refcount, live-index repair, trace.
- `internal/state`: schema v7, events/ops, `decision_records`, `planner_state`, shadow/meta/clients/flush/safe-ignore/sensitive matchers.
- `internal/git`: bounded refs/tree/diff/blob/scratch-index/history/ignore helpers.
- `internal/ai`: deterministic/OpenAI-compatible/subprocess providers; message + intent-planner contracts.
- `internal/adapter`: real harness detection; do not restore TODO stubs.
- `internal/central`, `internal/identity`, `internal/logger`, `internal/paths`, `internal/pause`, `internal/trace`: stats, fingerprints, logs, XDG, pause, trace.
- `templates/*`: harness snippets; keep `templates/embed.go` current.
- `test/integration`: build-tagged lifecycle/adapter/recovery/ignored-tree/fallback/AI/explainable/self-heal/intent tests.
- `README.md`, `docs/*`, `CHANGELOG.md`: user docs; use `~~~` for nested fences.

## Workflow

- Keep changes scoped; prefer `rg`; never revert unrelated work.
- After `git.Init`/`git init`, pin fixtures: `git symbolic-ref HEAD refs/heads/main`.
- Stubs must compile: `package <name>` plus `// TODO(phase N): <intent>`.
- Treat races, panics, nil pointers, ordering failures, and CI flakes as bugs; inspect/narrow before retrying.
- Timing failures: focused `-count=10`; `GOMAXPROCS=1 -count=50` for ordering hazards.
- Broad-run-sensitive: `TestRun_FsnotifyDrivesWake`, `TestRun_LifecycleHappyPath`, `TestRun_WakeBurstCoalesced`, `TestRun_RealSIGUSR1`, repeated edits, external fast-forward reseed, FF-in-grace self-heal.
- HEAD-transition tests usually wait for `waitForMetaValue(MetaKeyBranchHead, <sha>, 3s)`.
- CLI changes need Cobra help/examples/root help updates. Template changes must preserve embedded FS behavior.
- Changelog/release notes should describe user impact, not file diffs.

## State and branch model

- Repo DB: `<gitDir>/acd/state.db`; central registry/stats use XDG state/share paths.
- `SchemaVersion = 7`: v5 `decision_records`; v6 denormalized `decision_records.event_seq`; v7 `planner_state`.
- `shadow_paths` key: `(branch_ref, branch_generation, path)`; read-heavy code uses `state.DB.ReadSQL()`.
- Shadow bootstrap: 5000-row chunks; marker `shadow.bootstrapped:<branch_ref>:<generation>` only after all chunks commit; cleanup partial rows on failure.
- Reseed prunes old generations via `ACD_SHADOW_RETENTION_GENERATIONS` (default `1` prior generation). Empty active shadow with marker means delete marker and re-bootstrap.
- Branch tokens: attached `rev:<sha> <branch-ref>`; detached `rev:<sha>`; missing `missing <branch-ref>`. Fast-forward keeps generation; reset/rebase/switch/same-SHA ref switch bumps; legacy bare rev upgraded to attached forces Diverged.
- Detached HEAD pauses capture/replay; `acd start` refuses it. Never fall back to `refs/heads/main` when `git symbolic-ref` fails.
- Git-operation markers pause capture/replay: `rebase-merge`, `rebase-apply`, `MERGE_HEAD`, `CHERRY_PICK_HEAD`, `BISECT_LOG`; non-`ErrNotExist` stat errors fail open with warning.
- Same-branch rewinds set `daemon_meta.replay.paused_until = now + ACD_REWIND_GRACE_SECONDS`; `0` disables. Manual `<gitDir>/acd/paused` wins.
- SQLite read errors in `daemonPauseState` fail closed for that tick.
- Diverged drops stale `pending` rows for previous generation only; keep `published`, `failed`, `blocked_conflict`; attached-from-detached clears `MetaKeyDetachedHeadPaused` and rewind grace.

## Capture, fsnotify, ignore

- Capture compares live worktree to `shadow_paths`; stale/missing bootstrap can create phantom creates.
- `walkLive` BFSes by directory layer, batches ignore checks (`ignoreCheckBatchSize=1000`), prunes ignored/sensitive/safe-ignore dirs before readdir.
- `fsnotify_watcher.preWalk` mirrors `walkLive`; never prune worktree-rooted `acd/` (`.git/acd` is daemon state). Symlinks are mode `120000`; never descend.
- Empty `ACD_SENSITIVE_GLOBS` keeps defaults; typos must not disable defaults. Sensitive dir pruning uses literal dir names; wildcards are file-granular.
- Safe-ignore defaults include dependency/cache dirs. `ACD_SAFE_IGNORE=0|false|no|off` disables; `ACD_SAFE_IGNORE_EXTRA=dist/,build/` appends. Restart daemon for env changes.
- Safe-ignore dirs prune descendants, not same-named files. Use `SafeIgnoreMatcher.MatchFile` for files/symlinks and `MatchDirectory` for dirs.
- Protected skipped dirs mean dir exists, not every tracked child: `protectShadowFromSkippedPresent` must `Lstat` shadow children; `os.ErrNotExist` leaves row so delete classification emits delete.
- `IgnoreChecker.Check`: long-lived `git check-ignore --stdin -z --non-matching --verbose`; stream stdin from writer goroutine while reading stdout. One large `stdin.Write` deadlocks on macOS 16 KiB pipes. Invalidate before each capture pass and on `.gitignore` fsnotify events.
- `IgnoreChecker.Close`: non-blocking atomic cancel, `killLocked`, bounded `cmd.Wait` at 2s.

## Replay and intent grouping

- Default `ACD_COMMIT_STRATEGY=event`: one captured event per commit; must not call planner.
- `ACD_COMMIT_STRATEGY=intent`: offers pending captures to AI planner; capture durability unchanged.
- Intent envs: `ACD_INTENT_WINDOW=10`, `ACD_INTENT_RECENT_COMMITS=5`, `ACD_INTENT_DEFER_LIMIT=2`.
- Planner may select exactly one capture or any non-empty subset; every offered seq must be selected or deferred.
- Invalid/missing/unsafe planner output records `intent_planner_error` and falls back to safe deterministic one-item planning.
- Deferred captures stay pending in `planner_state` (`defer_count`, `last_planned_ts`, reason/error). At `defer_count >= ACD_INTENT_DEFER_LIMIT`, oldest overdue capture gets forced one-item planning.
- Grouped publish marks selected events `published` with same `commit_oid`; ledger records grouped seqs, deferrals, forced aging, planner errors.
- Per-pass scratch index `<gitDir>/acd/replay-*.index` is seeded from `cctx.BaseHead`; reads use `git.LsFilesIndex(ctx, repoDir, indexFile, paths...)`.
- CAS targets literal `HEAD` via `git.UpdateRef`; named refs still use `--no-deref`.
- `DefaultReplayLimit = 64`; query `Limit+1`, trim, set `ReplaySummary.HasMore`.
- `DefaultReplayPerEventTimeout = 60s`; timeout/cancel in heavy git work marks event `failed` and stops batch.
- `blocked_conflict` and `failed` are terminal seq barriers; `PendingEvents` hides later pending rows behind prior terminal rows for same branch/generation.
- Idempotent publish checks current `HEAD` before before-state blocking; if HEAD has desired final blob/mode/absence, mark published with `commit_oid=HEAD`.
- `superseded_external` requires bounded history proof, parent/base tree match, and live worktree before-state match. Incomplete proof means no supersede.
- Conflict metadata: `daemon_meta.last_replay_conflict`; legacy mirror `last_replay_conflict_legacy`.
- Live-index reconciliation is guarded/path-scoped; never overwrite user-staged changes. See `internal/git/tree.go`, `internal/daemon/replay.go`, `internal/daemon/live_index_repair.go`.
- `replay.live_index` traces are success records unless failed/blocked; `replayUpdateRefBackoffs` uses `math/rand/v2` jitter +-25%.

## Run loop and observability

- `processBranchTokenChange` runs before capture and after flush drain; do not collapse. Post-flush recheck handles git surgery outside `wakeCh`.
- Branch transitions set `branchTransitionSettleDelay = 100ms` so ref moves/worktree updates are not sampled as separate local edits.
- Flush drain bounded by `DefaultFlushLimit = 256`; check `ctx.Err()` and `shutdownCh`.
- Per-tick metadata writes batch via `state.MetaSetMany`; `MetaKeyBranchHead` keep-alive is guarded by `lastStampedBranchHead`.
- Daemon stamps `commit.strategy`, `intent.window`, `intent.recent_commits`, `intent.defer_limit`, `intent.diff_egress`; CLI reads daemon meta before current env.
- Startup sweeps `acknowledged` flush requests older than `OrphanFlushAckThreshold = 5m` to `failed`.
- Fingerprint warn LRU cap 1024; evict 256 oldest. Warn limiters and `ClampRewindGraceAtStartup` must handle backward NTP.
- fsnotify dispatch must not block: runtime creates use `rewalkCh`/`rewalkWorker`; diagnostics use `diagCh`; tail clamps at `MaxDebounceTail = 500ms`; ENOSPC -> `errBudgetExceeded`; `Stop(context.Context)` bounded.
- Logs: raw JSONL at `paths.Roots.RepoLogPath(repoHash)` (`~/.local/state/acd/<repo-hash>/daemon.log`) with rotation/compression.
- `acd logs --follow` streams from EOF reached by initial tail read; do not re-`Stat` after tailing.
- `acd list --watch --interval 2s` redraws table with timestamp; one-shot output unchanged; one-shot `--json` works; `--watch` rejects `--json`.
- `acd events --watch`: no `--since` starts at current ledger tail; with `--since`, resumes after cursor.
- `acd status`, `acd diagnose`, `acd doctor` show `failed_events`, `failed_blocking_pending`, intent summaries; guide to `acd fix --dry-run`.
- Probes: `acd status --repo .`; `acd events --watch`; `acd logs --repo . --lines 50 --follow`; `acd diagnose --repo . --json`; `acd doctor --repo . --json`; `git status --short --ignored`.

## CLI read-only UX

- `events`, `explain`, `doctor` read paths must not call `state.Open` or migrate DBs; use read-only SQLite projections (`openStateDBReadOnly` pattern).
- Missing `decision_records` or `planner_state`: empty summaries, clear human text, valid JSON, no table creation.
- `explain --since` summarizes newest post-cursor decision.
- Status JSON: `decision_counts`, `recent_decisions`, `decision_cursor`, `failed_events`, `failed_blocking_pending`, `intent_strategy`.

## Git, AI, trace

- `internal/git`: `RunOpts.Timeout`, `RunWithLimit`, `ErrStdoutOverflow`, `DefaultReadTimeout=30s`, `DefaultWriteTimeout=60s`, `git.DefaultDiffCap` (1 MiB). Ambiguous `RevParse` -> `git.ErrRefAmbiguous`.
- Pinned `ps`: `/bin/ps` on Darwin, `/usr/bin/ps` on Linux. Do not use `$PATH`. `isSQLiteLocked` must unwrap `*sqlite.Error` and compare typed code before substring fallback.
- AI providers declare `NeedsDiff`; network providers receive redacted diffs only when `NeedsDiff=true` and `ACD_AI_DIFF_EGRESS` is truthy. `DeterministicProvider` uses `NeedsDiff=false`.
- `BuildOpsDiff` caps rendered text at `ai.DiffCap`; per-op git diff uses `2 * ai.DiffCap` and 5s timeout. Redact + truncate before provider send.
- `ACD_AI_SEND_DIFF` was removed; if set, emit one startup deprecation warning.
- Generic messages like `Update PopupApp.tsx` are low-priority message-quality issues unless replay/state is wrong.
- `ACD_TRACE=1` writes best-effort JSONL to `<gitDir>/acd/trace/YYYY-MM-DD.jsonl`; `ACD_TRACE_DIR` overrides. Never block/abort. Verify classes with `rg -n "EventClass:" internal/`.

## Recovery

Prefer built-in recovery before SQLite edits:

```bash
acd diagnose --repo . --json
acd recover --repo . --auto --dry-run --json
acd recover --repo . --auto --yes
acd wake --repo . --session-id <session>
acd status --repo .
```

- `acd recover --auto` refuses while daemon PID is alive.
- It creates `.git/acd/state.db.recover-<timestamp>`, retargets pending/blocked rows to current branch/generation, resets blocked rows, clears replay/pause metadata, removes manual pause marker.
- Use `acd resume --yes` when only lifting manual pause.
- Manual cleanup: `acd pause --repo . --reason "manual reset" --yes`; `acd resume --repo . --yes`; `sqlite3 .git/acd/state.db "DELETE FROM capture_events WHERE state='blocked_conflict';"`.

## Harness/templates

- Codex template: `templates/codex/hooks.json` (v2). Codex now reads `~/.codex/hooks.json` before `~/.codex/config.toml`, so the JSON file wins discovery; the legacy TOML snippet was removed.
- Wired Codex events: `SessionStart` (`acd start`, timeout 15s), `UserPromptSubmit` (idempotent `acd start`, then `acd wake`, 15s), `PreToolUse` and `PostToolUse` (idempotent `acd start`, then `acd wake`, matcher `apply_patch|Edit|Write|Bash`, 15s), `Stop` (`acd touch`, 5s — mirrors claude-code so replay drain finishes before refcount sweep cleans up).
- Claude Code, OpenCode, and Pi active hooks also run idempotent `acd start` before `acd wake`; only their end-session hooks differ (`SessionEnd` / `session.deleted` use `acd stop --session-id`).
- Marker is `_acd_managed: true` at the JSON top level (parity with claude-code). Per-path adapter detection: hooks.json paths match JSON markers, config.toml paths match the legacy TOML comment marker; cross-format strings do not count.
- `acd doctor` warns when `~/.codex/hooks.json` and a legacy Codex TOML config both carry acd markers. Codex MERGES every hook source (it does not shadow), so leaving both installed fires every event twice. User must delete the TOML acd block.
- Codex flags every newly-added hook entry as "review required"; users must run `/hooks` inside Codex once after `acd setup codex --raw > ~/.codex/hooks.json` to approve all 5 entries before the daemon will start. Until approved, `SessionStart` never fires and `acd status` shows no Codex client. Documented in templates/codex/README.md and CHANGELOG.
- Legacy TOML users: Codex deprecated `[features].codex_hooks = true` for `[features].hooks = true`. New `hooks.json` install needs no `[features]` block.
- `cwd` comes from stdin (`acd hook-stdin-extract session_id cwd? <&0`); falls back to `$PWD` when Codex omits `cwd`. `CODEX_PROJECT_DIR` is no longer required. `printf "{}\n"` is no longer required; bash bodies rely on `exit 0` and use `|| exit 0` after the helper so a missing `acd` never blocks the hook.
- Templates use `acd hook-stdin-extract <field> [field...]` (multi-arg) instead of `jq`; keep `internal/cli/hookhelper.go` and AdapterE2E coverage.

## Env knobs

- Trace: `ACD_TRACE`; `ACD_TRACE_DIR` default `<gitDir>/acd/trace`.
- Shadow: `ACD_SHADOW_RETENTION_GENERATIONS=1`; rewind: `ACD_REWIND_GRACE_SECONDS=60`, `0` disables.
- Capture: `ACD_SENSITIVE_GLOBS`; `ACD_SAFE_IGNORE`; `ACD_SAFE_IGNORE_EXTRA`.
- AI: `ACD_AI_PROVIDER=deterministic|openai-compat|subprocess:<name>`; `ACD_AI_BASE_URL=https://api.openai.com/v1`; `ACD_AI_API_KEY`; `ACD_AI_MODEL=gpt-4o-mini`; `ACD_AI_TIMEOUT=30s`; `ACD_AI_CA_FILE`; `ACD_AI_DIFF_EGRESS`.
- Commit strategy: `ACD_COMMIT_STRATEGY=event|intent`; `ACD_INTENT_WINDOW=10`; `ACD_INTENT_RECENT_COMMITS=5`; `ACD_INTENT_DEFER_LIMIT=2`.
- Watcher/client: `ACD_FSNOTIFY_ENABLED`; `ACD_DISABLE_FSNOTIFY`; `ACD_MAX_INOTIFY_WATCHES`; `ACD_CLIENT_TTL_SECONDS`.

## Release notes

- `.goreleaser.yaml` hardcodes `prerelease: false`; date tags otherwise become pre-releases and `releases/latest` breaks.
- Brew publishing is gated behind `--skip=homebrew` until `HOMEBREW_TAP_TOKEN` and tap repo exist.
