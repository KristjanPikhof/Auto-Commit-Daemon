# Agent guide

## Identity and commands

- `acd`: static Go CLI/daemon, MIT, macOS/Linux `arm64`/`amd64`; no Windows v1.
- Module `github.com/KristjanPikhof/Auto-Commit-Daemon`. Go `1.22` and `modernc.org/sqlite v1.36.0` are pinned; do not bump without approval.
- Date tags: `vYYYY-MM-DD`; `make build` injects version + git SHA.

```bash
make build          # static bin/acd, CGO_ENABLED=0, -tags=netgo,osusergo
make test           # go test ./... -race -count=1
make lint           # go vet ./... + gofmt check
make fmt            # gofmt -w .
make tidy           # go mod tidy
./bin/acd version
```

Mandatory before claiming done, pushing final branch work, or opening PR:

```bash
make lint
make test
go test ./test/integration/... -tags=integration -race -count=1 -timeout 5m
go test ./internal/daemon/... ./internal/git/... ./internal/state/... ./internal/pause/... ./internal/cli/... -race -count=3 -timeout 10m
```

Release smoke: `make build && install -m 0755 ./bin/acd ~/.local/bin/acd`; `git tag v2026-MM-DD && git push origin v2026-MM-DD && gh run watch`; `gh release edit v2026-MM-DD --prerelease=false --latest`; `ACD_VERSION=v2026-MM-DD sh scripts/install.sh`.

## Project map

- `cmd/acd/main.go`: CLI entrypoint.
- `internal/cli`: Cobra commands; `internal/daemon`: run loop/capture/replay/fsnotify/branch tokens/shadow/live-index repair.
- `internal/git`: bounded git/refs/tree/diff/blob/scratch-index/ignore helpers; `internal/state`: SQLite schema v4/events/shadow/meta/clients/flush/matchers.
- `internal/ai`: deterministic/OpenAI-compatible/subprocess providers; `internal/adapter`: harness detection; `internal/central`: registry/stats.
- `internal/identity`: fingerprints and pinned `ps`; `internal/logger`: JSONL rotation/compression; `internal/paths`: XDG roots/repo hash/log path.
- `internal/pause`: durable pause marker; `internal/trace`: best-effort trace.
- `templates/*`: harness snippets, keep `templates/embed.go` current; `test/integration`: build-tagged lifecycle/adapter/recovery/ignored-tree/fallback/AI tests.
- `docs/*`, `README.md`: user docs; use `~~~` for nested fences.

## Workflow rules

- Keep changes scoped. Prefer `rg`. Daemon tests are timing-sensitive; run focused tests before broad runs.
- Pin fixture branches after `git.Init`/`git init`: `git symbolic-ref HEAD refs/heads/main`.
- Stubs must compile: `package <name>` plus `// TODO(phase N): <intent>`.
- Treat data races, panics, nil pointers, ordering failures, and CI flakes as real bugs; do not retry first.
- Timing failures: focused `-count=10`; `GOMAXPROCS=1 -count=50` for ordering hazards.
- Multi-phase HEAD-transition tests usually wait for `waitForMetaValue(MetaKeyBranchHead, <sha>, 3s)`.
- Broad-run-sensitive tests: `TestRun_FsnotifyDrivesWake`, `TestRun_LifecycleHappyPath`, `TestRun_WakeBurstCoalesced`, `TestRun_RealSIGUSR1`, `TestRun_RepeatedEditsToSameFile_OrderedCommits`.
- Template edits must preserve embedded FS behavior.

## State and branch model

- Repo DB: `<gitDir>/acd/state.db`; central registry/stats use user XDG state/share paths; schema v4.
- `shadow_paths` key: `(branch_ref, branch_generation, path)`; read-heavy code uses `state.DB.ReadSQL()`.
- Shadow bootstrap writes 5000-row chunks; marker `shadow.bootstrapped:<branch_ref>:<generation>` only after all chunks commit. On failure delete partial branch/generation rows.
- Reseed prunes old generations via `ACD_SHADOW_RETENTION_GENERATIONS` (default `1` prior generation).
- Marker with empty active `shadow_paths` means delete marker and re-bootstrap.
- Branch tokens: attached `rev:<sha> <branch-ref>`; detached `rev:<sha>`; missing ref `missing <branch-ref>`.
- Fast-forward same branch keeps generation; reset/rebase/switch/same-SHA ref switch bumps generation.
- Legacy token without branch ref forces Diverged when upgraded to attached token, even if SHA unchanged.
- Detached HEAD pauses capture/replay; `acd start` refuses it. Never fall back to `refs/heads/main` when `git symbolic-ref` fails.
- Git operation markers pause capture/replay: `rebase-merge`, `rebase-apply`, `MERGE_HEAD`, `CHERRY_PICK_HEAD`, `BISECT_LOG`.
- `gitOperationInProgress` fails open on non-`ErrNotExist` stat errors: log and treat marker absent.
- Same-branch rewinds set `daemon_meta.replay.paused_until = now + ACD_REWIND_GRACE_SECONDS`; `0` disables.
- Manual marker `<gitDir>/acd/paused` wins over rewind grace. Malformed/non-regular marker fails open with warning.
- SQLite read errors in `daemonPauseState` fail closed for that tick.
- Diverged drops stale `pending` rows for previous generation only; keep `published`, `failed`, `blocked_conflict`.
- Diverged-attached-from-detached must clear `MetaKeyDetachedHeadPaused` and rewind grace metadata.
- `idx_flush_requests_status_id` keeps `ClaimNextFlushRequest` constant-time.

## Capture, fsnotify, ignore

- Capture compares live worktree to `shadow_paths`; stale/missing bootstrap can misclassify tracked files as phantom creates.
- `walkLive` BFSes by directory layer, batches ignore checks with `ignoreCheckBatchSize=1000`, and prunes ignored dirs before readdir.
- `fsnotify_watcher.preWalk` mirrors `walkLive` ignore/sensitive/safe-ignore pruning.
- Do not prune worktree-rooted `acd/`; daemon state is under `.git/acd`.
- Symlinks are mode `120000`; never descend into symlinked dirs.
- Empty `ACD_SENSITIVE_GLOBS` keeps defaults; never let typo/misconfig disable defaults. Sensitive dir pruning uses literal dir names only; wildcards are file-granular.
- Safe-ignore defaults: `node_modules/`, `target/`, `.venv/`, `venv/`, `__pycache__/`, `.pytest_cache/`, `.mypy_cache/`, `.ruff_cache/`, `.gradle/`.
- `ACD_SAFE_IGNORE=0|false|no|off` disables safe-ignore; `ACD_SAFE_IGNORE_EXTRA=dist/,build/` appends valid patterns. Invalid extras are ignored.
- Safe-ignore dir patterns prune dirs/descendants, not same-named files. Use `SafeIgnoreMatcher.MatchFile` for files/symlinks and `MatchDirectory` for dirs.
- `IgnoreChecker.Check` uses long-lived `git check-ignore --stdin -z --non-matching --verbose`.
- Stream stdin from a writer goroutine while reading stdout; one large `stdin.Write` deadlocks on macOS 16 KiB pipes.
- `IgnoreChecker.Close` is non-blocking: atomic cancel, `killLocked`, bounded `cmd.Wait` at 2s.
- `git check-ignore --stdin` does not reload `.gitignore`; invalidate before each capture pass and on `.gitignore` fsnotify events.
- Key tests: `TestIgnoreCheckerInvalidateReloadsGitignore`, `TestHandleEventInvalidatesIgnoreCheckerOnGitignoreChange`.

## Replay

- Replay uses per-pass scratch index `<gitDir>/acd/replay-*.index` seeded from `cctx.BaseHead`.
- Scratch-index reads use `git.LsFilesIndex(ctx, repoDir, indexFile, paths...)`.
- Replay CAS targets literal `HEAD` via `git.UpdateRef`; named refs still use `--no-deref`.
- `DefaultReplayLimit = 64`; query `Limit+1`, trim, set `ReplaySummary.HasMore`.
- Each event has 60s budget. Timeout marks blocked; `MarkEventBlocked` errors propagate.
- `blocked_conflict` is terminal/seq barrier; `PendingEvents` hides later pending rows behind prior `blocked_conflict`/`failed` for same branch/generation.
- Idempotent publish checks current `HEAD` before before-state blocking. If `HEAD` already has desired final blob/mode/absence, mark published with `commit_oid=HEAD`.
- Conflict metadata: JSON in `daemon_meta.last_replay_conflict`; legacy mirror `last_replay_conflict_legacy`.
- Live-index reconciliation after publish is guarded/path-scoped. Do not overwrite user-staged changes. See `internal/git/tree.go`, `internal/daemon/replay.go`, `internal/daemon/live_index_repair.go`.
- Startup/recover repair handles old published events whose live index stayed stale; doctor may report candidates.
- `replay.live_index` traces are success records unless failed/blocked; successful `applied` keeps `error` empty.
- `replayUpdateRefBackoffs` uses `math/rand/v2` jitter +-25%.

## Run loop and observability

- `processBranchTokenChange` runs before capture and after flush drain; do not collapse. Post-flush recheck handles git surgery outside `wakeCh`.
- Flush drain bounded by `DefaultFlushLimit = 256`; check `ctx.Err()` and `shutdownCh`.
- Per-tick metadata writes batch through `state.MetaSetMany`.
- `MetaKeyBranchHead` keep-alive is value-guarded by closure-scoped `lastStampedBranchHead`.
- Startup sweeps `acknowledged` flush requests older than `OrphanFlushAckThreshold = 5m` to `failed`.
- Fingerprint warn LRU cap 1024; evict 256 oldest.
- Warn limiters and `ClampRewindGraceAtStartup` must handle backward NTP steps.
- fsnotify dispatch must not block: runtime creates use `rewalkCh`/`rewalkWorker`; diagnostics use `diagCh`; trailing timer clamps at `MaxDebounceTail = 500ms`; ENOSPC -> `errBudgetExceeded`; `Stop(context.Context)` bounded.
- fsnotify CLI env: `ACD_FSNOTIFY_ENABLED` enables, `ACD_DISABLE_FSNOTIFY` forces poll-only, `ACD_MAX_INOTIFY_WATCHES` overrides Linux watch budget.
- Daemon logs are raw JSONL at `paths.Roots.RepoLogPath(repoHash)` (`~/.local/state/acd/<repo-hash>/daemon.log`) with rotation/compression.
- `acd logs` resolves current repo through central registry and prints raw JSONL. `--lines N` tails, `--follow` streams appended bytes, missing logs return actionable errors.
- `acd logs --follow` must follow from EOF reached by the initial tail read; do not re-`Stat` after tailing and skip bytes appended between tail/follow.
- `acd list --watch --interval 2s` redraws the existing table with an updated timestamp; one-shot `acd list` output must stay unchanged. `--watch` does not support `--json`.
- `acd doctor` tails logs best-effort, sanitizes `$HOME` to `~`, and bundles `daemon-tail.log`, `sensitive-globs.txt`, `safe-ignore-patterns.txt`, `fsnotify-stats.json`, state/meta JSON.

```bash
acd status --repo .
acd list --watch --interval 2s
acd logs --repo . --lines 100
acd logs --repo . --lines 50 --follow
acd diagnose --repo . --json
acd doctor --repo . --json
sqlite3 .git/acd/state.db "SELECT state, COUNT(*) FROM capture_events GROUP BY state;"
sqlite3 .git/acd/state.db "SELECT seq, operation, path, substr(error,1,100) FROM capture_events WHERE state='blocked_conflict' ORDER BY seq DESC LIMIT 20;"
git status --short --ignored
```

## Git, AI, trace

- `internal/git`: `RunOpts.Timeout`, `RunWithLimit`, `ErrStdoutOverflow`, `DefaultReadTimeout=30s`, `DefaultWriteTimeout=60s`; diff/blob caps use `git.DefaultDiffCap` (1 MiB).
- `RevParse` surfaces ambiguous refs as `git.ErrRefAmbiguous`; classify separately from missing ref.
- `ps` path pinned: `/bin/ps` on Darwin, `/usr/bin/ps` on Linux. Do not use `$PATH`.
- `isSQLiteLocked` must unwrap `*sqlite.Error` and compare typed code before substring fallback.
- AI providers declare `NeedsDiff`; network providers receive redacted diffs only when `NeedsDiff=true` and `ACD_AI_DIFF_EGRESS` is truthy.
- `DeterministicProvider` uses `NeedsDiff=false`; gets empty `DiffText`.
- `BuildOpsDiff` uses git-layer caps via `git.DiffBlobsLimited` / `git.CatFileBlobLimited`; no post-render trim. Per-op timeout 5s.
- `ACD_AI_SEND_DIFF` was removed; if set, emit one startup deprecation warning.
- Generic messages like `Update PopupApp.tsx` are low-priority message-quality issues unless replay/state is wrong.
- `ACD_TRACE=1` writes best-effort JSONL to `<gitDir>/acd/trace/YYYY-MM-DD.jsonl`; `ACD_TRACE_DIR` overrides; never block/abort on trace writes.
- Event classes: `bootstrap_shadow.reseed`, `capture.classify`, `capture.event`, `capture.pause`, `replay.commit`, `replay.conflict`, `replay.failed`, `replay.update_ref`, `replay.live_index`, `replay.pause`, `branch_token.transition`, `daemon.pause`.
- Verify trace additions with `rg -n "EventClass:" internal/`.

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
- Use `acd resume --yes` when only lifting a manual pause.
- Manual cleanup: `acd pause --repo . --reason "manual reset" --yes`; `acd resume --repo . --yes`; `sqlite3 .git/acd/state.db "DELETE FROM capture_events WHERE state='blocked_conflict';"`.

## Harness adapter gotchas

- Codex template: `templates/codex/config.snippet.toml`.
- Codex hooks require `[features] codex_hooks = true`, `[[hooks.<EventName>]]`, then nested `[[hooks.<EventName>.hooks]]`; flat `[[hooks]]` fails.
- Codex hook stdout must be valid JSON. Snippet redirects `acd` output to `/dev/null` and emits `printf "{}\n"`.
- No `Stop` hook in Codex snippet; it races replay drain. Cleanup uses `watch_pid` death plus refcount sweep.
- Codex can auto-load both `~/.codex/hooks.json` and `~/.codex/config.toml`; delete old `hooks.json` after installing toml snippet.
- Templates use `acd hook-stdin-extract <field>` instead of `jq`; keep helper in `internal/cli/hookhelper.go` and AdapterE2E coverage.
- `internal/adapter` is real code for harness config/marker detection; do not restore old TODO stubs.

## Environment knobs

- Trace: `ACD_TRACE` truthy enables JSONL; `ACD_TRACE_DIR` default `<gitDir>/acd/trace`.
- Shadow: `ACD_SHADOW_RETENTION_GENERATIONS=1`; rewind: `ACD_REWIND_GRACE_SECONDS=60`, `0` disables.
- Capture: `ACD_SENSITIVE_GLOBS` empty/whitespace keeps defaults; `ACD_SAFE_IGNORE` default enabled; `ACD_SAFE_IGNORE_EXTRA` appends comma-separated patterns.
- AI: `ACD_AI_PROVIDER=deterministic|openai-compat|subprocess:<name>`; `ACD_AI_BASE_URL=https://api.openai.com/v1` must be absolute HTTPS; missing `ACD_AI_API_KEY` degrades to deterministic; `ACD_AI_MODEL=gpt-4o-mini`; `ACD_AI_TIMEOUT=30s`; `ACD_AI_CA_FILE`; `ACD_AI_DIFF_EGRESS`.
- Watcher/client: `ACD_FSNOTIFY_ENABLED` truthy enables fsnotify in CLI; `ACD_DISABLE_FSNOTIFY` forces poll-only; `ACD_MAX_INOTIFY_WATCHES` overrides Linux watch budget; `ACD_CLIENT_TTL_SECONDS` overrides CLI heartbeat TTL.

## Release notes

- `.goreleaser.yaml` hardcodes `prerelease: false`; date tags otherwise become pre-releases and `releases/latest` breaks.
- Brew publishing is gated behind `--skip=homebrew` until `HOMEBREW_TAP_TOKEN` and tap repo exist.
