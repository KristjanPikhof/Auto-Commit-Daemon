# Agent guide

## Identity/commands

- `acd`: static Go CLI/daemon, MIT, macOS/Linux `arm64`/`amd64`; no Windows v1.
- Module `github.com/KristjanPikhof/Auto-Commit-Daemon`; Go `1.22` and `modernc.org/sqlite v1.36.0` are pinned.
- Date tags: `vYYYY-MM-DD`; `make build` injects version + git SHA.

```bash
make build          # static bin/acd, CGO_ENABLED=0, -tags=netgo,osusergo
make test           # go test ./... -race -count=1
make lint           # go vet ./... + gofmt check
make fmt            # gofmt -w .
make tidy           # go mod tidy
./bin/acd version
```

Mandatory before done/push/PR/final branch handoff:

```bash
make lint
make test
go test ./test/integration/... -tags=integration -race -count=1 -timeout 5m
go test ./internal/daemon/... ./internal/git/... ./internal/state/... ./internal/pause/... ./internal/cli/... -race -count=3 -timeout 10m
```

Release smoke: `make build && install -m 0755 ./bin/acd ~/.local/bin/acd`; `git tag v2026-MM-DD && git push origin v2026-MM-DD && gh run watch`; `gh release edit v2026-MM-DD --prerelease=false --latest`; `ACD_VERSION=v2026-MM-DD sh scripts/install.sh`.

## Map

- `cmd/acd/main.go`: entrypoint.
- `internal/cli`: Cobra commands: start/stop/status/list/logs/events/explain/diagnose/doctor/fix/recover/pause/resume/init/hooks.
- `internal/daemon`: run loop, capture, replay, branch tokens, bootstrap/shadow, fsnotify, refcount, live-index repair, trace.
- `internal/state`: SQLite schema v6, events/ops, decision ledger, shadow/meta/clients/flush/safe-ignore/sensitive matchers.
- `internal/git`: bounded git refs/tree/diff/blob/scratch-index/ignore helpers.
- `internal/ai`: deterministic/OpenAI-compatible/subprocess providers; `internal/adapter`: harness detection; `internal/central`: registry/stats.
- `internal/identity/logger/paths/pause/trace`: fingerprints, pinned `ps`, JSONL rotation, XDG paths, pause marker, trace.
- `templates/*`: harness snippets; keep `templates/embed.go` current. `test/integration`: build-tagged lifecycle/adapter/recovery/ignored-tree/fallback/AI/explainable/self-heal tests.
- `README.md`, `docs/*`: user docs; use `~~~` for nested fences.

## Workflow

- Keep changes scoped; prefer `rg`; never revert unrelated work.
- After `git.Init`/`git init`, pin fixtures: `git symbolic-ref HEAD refs/heads/main`.
- Stubs must compile: `package <name>` plus `// TODO(phase N): <intent>`.
- Treat races, panics, nil pointers, ordering failures, and CI flakes as bugs; inspect/narrow before retrying.
- Timing failures: focused `-count=10`; `GOMAXPROCS=1 -count=50` for ordering hazards.
- Broad-run-sensitive: `TestRun_FsnotifyDrivesWake`, `TestRun_LifecycleHappyPath`, `TestRun_WakeBurstCoalesced`, `TestRun_RealSIGUSR1`, `TestRun_RepeatedEditsToSameFile_OrderedCommits`, `TestRun_ExternalFastForwardReseedsShadowWithoutCapturingUpstream`, `TestSelfHeal_FastForwardDuringRewindGrace_NoPhantoms`.
- Multi-phase HEAD-transition tests usually wait for `waitForMetaValue(MetaKeyBranchHead, <sha>, 3s)`.
- CLI changes need Cobra help/examples and compact root help updates. Template changes must preserve embedded FS behavior.

## State/branch model

- Repo DB: `<gitDir>/acd/state.db`; central registry/stats use XDG state/share paths.
- `SchemaVersion = 6`: v5 added `decision_records`; v6 makes `decision_records.event_seq` denormalized ledger data, not an FK nulled by `capture_events` pruning.
- `shadow_paths` key: `(branch_ref, branch_generation, path)`; read-heavy code uses `state.DB.ReadSQL()`.
- Shadow bootstrap: 5000-row chunks; marker `shadow.bootstrapped:<branch_ref>:<generation>` only after all chunks commit; delete partial rows on failure.
- Reseed prunes old generations via `ACD_SHADOW_RETENTION_GENERATIONS` (default `1` prior generation). Empty active shadow with marker means delete marker and re-bootstrap.
- Branch tokens: attached `rev:<sha> <branch-ref>`; detached `rev:<sha>`; missing `missing <branch-ref>`. Same-branch fast-forward keeps generation; reset/rebase/switch/same-SHA ref switch bumps. Legacy token without branch ref upgraded to attached token forces Diverged.
- Detached HEAD pauses capture/replay; `acd start` refuses it. Never fall back to `refs/heads/main` when `git symbolic-ref` fails.
- Git operation markers pause capture/replay: `rebase-merge`, `rebase-apply`, `MERGE_HEAD`, `CHERRY_PICK_HEAD`, `BISECT_LOG`. Non-`ErrNotExist` stat errors fail open with warning.
- Same-branch rewinds set `daemon_meta.replay.paused_until = now + ACD_REWIND_GRACE_SECONDS`; `0` disables. Manual `<gitDir>/acd/paused` wins; malformed/non-regular marker fails open.
- SQLite read errors in `daemonPauseState` fail closed for that tick.
- Diverged drops stale `pending` rows for previous generation only; keep `published`, `failed`, `blocked_conflict`; attached-from-detached clears `MetaKeyDetachedHeadPaused` and rewind grace metadata.

## Capture/fsnotify/ignore

- Capture compares live worktree to `shadow_paths`; stale/missing bootstrap can create phantom creates.
- `walkLive` BFSes by directory layer, batches ignore checks (`ignoreCheckBatchSize=1000`), and prunes ignored/sensitive/safe-ignore dirs before readdir.
- `fsnotify_watcher.preWalk` mirrors `walkLive`; never prune worktree-rooted `acd/` (`.git/acd` is daemon state). Symlinks are mode `120000`; never descend into symlinked dirs.
- Empty `ACD_SENSITIVE_GLOBS` keeps defaults; typos must not disable defaults. Sensitive dir pruning uses literal dir names; wildcards are file-granular.
- Safe-ignore defaults: `node_modules/`, `target/`, `.venv/`, `venv/`, `__pycache__/`, `.pytest_cache/`, `.mypy_cache/`, `.ruff_cache/`, `.gradle/`. `ACD_SAFE_IGNORE=0|false|no|off` disables; `ACD_SAFE_IGNORE_EXTRA=dist/,build/` appends valid patterns. Restart daemon for env changes.
- Safe-ignore dirs prune descendants, not same-named files. Use `SafeIgnoreMatcher.MatchFile` for files/symlinks and `MatchDirectory` for dirs.
- Protected skipped dirs mean dir exists, not every tracked child: `protectShadowFromSkippedPresent` must `Lstat` concrete shadow children; `os.ErrNotExist` leaves shadow row so delete classification emits delete.
- `IgnoreChecker.Check`: long-lived `git check-ignore --stdin -z --non-matching --verbose`; stream stdin from writer goroutine while reading stdout. One large `stdin.Write` deadlocks on macOS 16 KiB pipes. Invalidate before each capture pass and on `.gitignore` fsnotify events.
- `IgnoreChecker.Close`: non-blocking atomic cancel, `killLocked`, bounded `cmd.Wait` at 2s.

## Replay

- Per-pass scratch index `<gitDir>/acd/replay-*.index` seeded from `cctx.BaseHead`; reads use `git.LsFilesIndex(ctx, repoDir, indexFile, paths...)`.
- CAS targets literal `HEAD` via `git.UpdateRef`; named refs still use `--no-deref`.
- `DefaultReplayLimit = 64`; query `Limit+1`, trim, set `ReplaySummary.HasMore`.
- `DefaultReplayPerEventTimeout = 60s`; timeout/cancel in heavy git work marks event `failed` and stops batch.
- `blocked_conflict` and `failed` are terminal seq barriers; `PendingEvents` hides later pending rows behind prior terminal rows for same branch/generation.
- Idempotent publish checks current `HEAD` before before-state blocking; if HEAD has desired final blob/mode/absence, mark published with `commit_oid=HEAD`.
- `superseded_external` is conservative: bounded history probe (`diff --quiet` plus `rev-list --max-count=1`), parent/base trees must match captured before-state, and live worktree must match before-state. If proof is incomplete, do not supersede.
- Conflict metadata: `daemon_meta.last_replay_conflict`; legacy mirror `last_replay_conflict_legacy`.
- Live-index reconciliation after publish is guarded/path-scoped; never overwrite user-staged changes. See `internal/git/tree.go`, `internal/daemon/replay.go`, `internal/daemon/live_index_repair.go`. Doctor may report stale old published events.
- `replay.live_index` traces are success records unless failed/blocked; `replayUpdateRefBackoffs` uses `math/rand/v2` jitter +-25%.

## Run loop/observability

- `processBranchTokenChange` runs before capture and after flush drain; do not collapse. Post-flush recheck handles git surgery outside `wakeCh`.
- Any branch token transition sets `branchTransitionSettleDelay = 100ms` so ref moves and worktree updates are not sampled as local edits in separate ticks.
- Flush drain bounded by `DefaultFlushLimit = 256`; check `ctx.Err()` and `shutdownCh`.
- Per-tick metadata writes batch via `state.MetaSetMany`; `MetaKeyBranchHead` keep-alive is value-guarded by `lastStampedBranchHead`.
- Startup sweeps `acknowledged` flush requests older than `OrphanFlushAckThreshold = 5m` to `failed`.
- Fingerprint warn LRU cap 1024; evict 256 oldest. Warn limiters and `ClampRewindGraceAtStartup` must handle backward NTP.
- fsnotify dispatch must not block: runtime creates use `rewalkCh`/`rewalkWorker`; diagnostics use `diagCh`; trailing timer clamps at `MaxDebounceTail = 500ms`; ENOSPC -> `errBudgetExceeded`; `Stop(context.Context)` bounded.
- Logs: raw JSONL at `paths.Roots.RepoLogPath(repoHash)` (`~/.local/state/acd/<repo-hash>/daemon.log`) with rotation/compression.
- `acd logs --follow` streams from EOF reached by initial tail read; do not re-`Stat` after tailing.
- `acd list --watch --interval 2s` redraws table with timestamp; one-shot output unchanged; no `--json`.
- `acd events --watch`: with no `--since`, starts at current ledger tail; with `--since`, resumes after cursor.
- `acd status`, `acd diagnose`, `acd doctor` surface `failed_events` and `failed_blocking_pending`; guide to `acd fix --dry-run`.
- `acd doctor` tails logs best-effort, sanitizes `$HOME` to `~`, bundles logs, ignore patterns, fsnotify stats, state/meta JSON.

```bash
acd status --repo .
acd list --watch --interval 2s
acd events --watch
acd logs --repo . --lines 100
acd logs --repo . --lines 50 --follow
acd diagnose --repo . --json
acd doctor --repo . --json
sqlite3 .git/acd/state.db "SELECT state, COUNT(*) FROM capture_events GROUP BY state;"
sqlite3 .git/acd/state.db "SELECT seq, operation, path, substr(error,1,100) FROM capture_events WHERE state IN ('blocked_conflict','failed') ORDER BY seq DESC LIMIT 20;"
git status --short --ignored
```

## CLI read-only UX

- `events`, `explain`, and `doctor` read paths must not call `state.Open` or migrate old DBs; use read-only SQLite projections (`openStateDBReadOnly` pattern). Missing `decision_records`: empty ledger, clear human text, valid JSON, no table creation.
- `explain --since` summarizes newest post-cursor decision. Status JSON fields: `decision_counts`, `recent_decisions`, `decision_cursor`, `failed_events`, `failed_blocking_pending`.

## Git/AI/trace

- `internal/git`: `RunOpts.Timeout`, `RunWithLimit`, `ErrStdoutOverflow`, `DefaultReadTimeout=30s`, `DefaultWriteTimeout=60s`; diff/blob caps use `git.DefaultDiffCap` (1 MiB). `RevParse` ambiguous refs -> `git.ErrRefAmbiguous`.
- Pinned `ps`: `/bin/ps` on Darwin, `/usr/bin/ps` on Linux. Do not use `$PATH`. `isSQLiteLocked` must unwrap `*sqlite.Error` and compare typed code before substring fallback.
- AI providers declare `NeedsDiff`; network providers receive redacted diffs only when `NeedsDiff=true` and `ACD_AI_DIFF_EGRESS` is truthy. `DeterministicProvider` uses `NeedsDiff=false`.
- `BuildOpsDiff` uses `git.DiffBlobsLimited` / `git.CatFileBlobLimited`; no post-render trim. Per-op timeout 5s.
- `ACD_AI_SEND_DIFF` was removed; if set, emit one startup deprecation warning.
- Generic messages like `Update PopupApp.tsx` are low-priority message-quality issues unless replay/state is wrong.
- `ACD_TRACE=1` writes best-effort JSONL to `<gitDir>/acd/trace/YYYY-MM-DD.jsonl`; `ACD_TRACE_DIR` overrides; never block/abort. Verify event-class additions with `rg -n "EventClass:" internal/`.

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
- It creates `.git/acd/state.db.recover-<timestamp>`, retargets pending/blocked rows to current attached branch/generation, resets blocked rows, clears replay/pause metadata, removes manual pause marker.
- Use `acd resume --yes` when only lifting manual pause.
- Manual cleanup: `acd pause --repo . --reason "manual reset" --yes`; `acd resume --repo . --yes`; `sqlite3 .git/acd/state.db "DELETE FROM capture_events WHERE state='blocked_conflict';"`.

## Harness/templates

- Codex template: `templates/codex/config.snippet.toml`.
- Codex hooks require `[features] codex_hooks = true`, `[[hooks.<EventName>]]`, then nested `[[hooks.<EventName>.hooks]]`; flat `[[hooks]]` fails.
- Hook stdout must be valid JSON; snippet redirects `acd` output to `/dev/null` and emits `printf "{}\n"`. No `Stop` hook; it races replay drain.
- Codex can auto-load `~/.codex/hooks.json` and `~/.codex/config.toml`; delete old `hooks.json` after installing toml snippet.
- Templates use `acd hook-stdin-extract <field>` instead of `jq`; keep `internal/cli/hookhelper.go` and AdapterE2E coverage.
- `internal/adapter` is real harness config/marker detection code; do not restore old TODO stubs.

## Env knobs

- Trace: `ACD_TRACE`; `ACD_TRACE_DIR` default `<gitDir>/acd/trace`.
- Shadow: `ACD_SHADOW_RETENTION_GENERATIONS=1`; rewind: `ACD_REWIND_GRACE_SECONDS=60`, `0` disables.
- Capture: `ACD_SENSITIVE_GLOBS`; `ACD_SAFE_IGNORE`; `ACD_SAFE_IGNORE_EXTRA`.
- AI: `ACD_AI_PROVIDER=deterministic|openai-compat|subprocess:<name>`; `ACD_AI_BASE_URL=https://api.openai.com/v1` absolute HTTPS; missing `ACD_AI_API_KEY` degrades to deterministic; `ACD_AI_MODEL=gpt-4o-mini`; `ACD_AI_TIMEOUT=30s`; `ACD_AI_CA_FILE`; `ACD_AI_DIFF_EGRESS`.
- Watcher/client: `ACD_FSNOTIFY_ENABLED`; `ACD_DISABLE_FSNOTIFY`; `ACD_MAX_INOTIFY_WATCHES`; `ACD_CLIENT_TTL_SECONDS`.

## Release notes

- `.goreleaser.yaml` hardcodes `prerelease: false`; date tags otherwise become pre-releases and `releases/latest` breaks.
- Brew publishing is gated behind `--skip=homebrew` until `HOMEBREW_TAP_TOKEN` and tap repo exist.
