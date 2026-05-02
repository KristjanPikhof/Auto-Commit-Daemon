# Agent guide

## Identity / commands

- `acd`: static Go CLI/daemon, MIT, macOS/Linux `arm64`/`amd64`, no Windows v1.
- Module `github.com/KristjanPikhof/Auto-Commit-Daemon`; date tags `vYYYY-MM-DD`; `make build` injects version + git SHA.
- Go `1.22` and `modernc.org/sqlite v1.36.0` are pinned; do not bump without approval.

```bash
make build          # CGO_ENABLED=0, -tags=netgo,osusergo, output bin/acd
make test           # go test ./... -race -count=1
make lint           # go vet ./... + gofmt check
make fmt            # gofmt -w .
make tidy           # go mod tidy
./bin/acd version
go test ./test/integration/... -tags=integration -race -count=1 -timeout 5m
```

Mandatory before claiming done, pushing final branch work, or opening PR:

```bash
make lint
make test
go test ./test/integration/... -tags=integration -race -count=1 -timeout 5m
go test ./internal/daemon/... ./internal/git/... ./internal/state/... ./internal/pause/... ./internal/cli/... -race -count=3 -timeout 10m
```

Install/release smoke:

```bash
make build && install -m 0755 ./bin/acd ~/.local/bin/acd
git tag v2026-MM-DD && git push origin v2026-MM-DD && gh run watch
gh release edit v2026-MM-DD --prerelease=false --latest
ACD_VERSION=v2026-MM-DD sh scripts/install.sh
```

## Project map

| Path | Purpose |
|---|---|
| `cmd/acd/main.go` | CLI entrypoint. |
| `internal/cli` | Cobra commands. |
| `internal/daemon` | Run loop, capture/replay, fsnotify, branch tokens, shadow, live-index repair. |
| `internal/git` | Bounded git, refs, trees/diffs/blobs, scratch indexes, ignore checker. |
| `internal/state` | SQLite, events, shadow paths, meta, clients, flush requests, matchers. |
| `internal/ai` | Deterministic, OpenAI-compatible, subprocess providers. |
| `internal/adapter` | Harness detection for `acd init`/doctor. |
| `internal/central` | Registry/rollup stats DB. |
| `internal/identity` | Process fingerprints; pinned `ps`. |
| `internal/logger` | JSONL slog logger with rotation/compression. |
| `internal/paths` | XDG paths, repo hashes, per-repo log path. |
| `internal/pause` | Durable `<gitDir>/acd/paused` marker. |
| `internal/trace` | Best-effort JSONL trace. |
| `templates/*` | Harness snippets; after edits, keep `templates/embed.go` current. |
| `test/integration` | Build-tagged lifecycle/adapter/recovery/ignored-tree/fallback/AI tests. |
| `docs/*` | User docs. |

## General workflow

- Keep changes scoped; daemon tests are timing-sensitive. Prefer `rg`.
- Pin fixture branches after `git.Init`/`git init`: `git symbolic-ref HEAD refs/heads/main`.
- Stubs: `package <name>` plus `// TODO(phase N): <intent>`; must compile.
- README/adapter docs use `~~~` for nested fences.
- Replay queued events with scratch indexes only; never inspect live repo index.
- Treat CI data races, panics, nil pointers, or ordering failures as real bugs. Do not retry first.
- Timing failures: focused `-count=10`; `GOMAXPROCS=1 -count=50` for ordering hazards.
- Multi-phase HEAD-transition tests usually wait for `waitForMetaValue(MetaKeyBranchHead, <sha>, 3s)`.
- Broad-run-sensitive: `TestRun_FsnotifyDrivesWake`, `TestRun_LifecycleHappyPath`, `TestRun_WakeBurstCoalesced`, `TestRun_RealSIGUSR1`, `TestRun_RepeatedEditsToSameFile_OrderedCommits`.

## State and branch model

- Repo SQLite: `<gitDir>/acd/state.db`; central registry/stats use user state/share paths; schema v4.
- `idx_flush_requests_status_id` keeps `ClaimNextFlushRequest` constant-time.
- `shadow_paths` key: `(branch_ref, branch_generation, path)`; read-heavy code uses `state.DB.ReadSQL()`.
- Shadow bootstrap: 5000-row `state.AppendShadowBatch` chunks; marker `shadow.bootstrapped:<branch_ref>:<generation>`.
- Write completion marker only after all chunks commit; on failure delete partial branch/generation rows.
- Reseed prunes old generations with `ACD_SHADOW_RETENTION_GENERATIONS` (default `1` prior generation).
- Marker exists but active branch/generation has empty `shadow_paths`: delete marker to re-bootstrap.
- Branch tokens: attached `rev:<sha> <branch-ref>`; detached `rev:<sha>`; missing ref `missing <branch-ref>`.
- Fast-forward same branch keeps generation; reset/rebase/switch/same-SHA ref switch bumps generation.
- Legacy token without branch ref forces Diverged when upgraded to attached token, even if SHA unchanged.
- Detached HEAD pauses capture/replay; `acd start` refuses it. Never fall back to `refs/heads/main` when `git symbolic-ref` fails.
- Git operation markers pause capture/replay: `rebase-merge`, `rebase-apply`, `MERGE_HEAD`, `CHERRY_PICK_HEAD`, `BISECT_LOG`.
- `gitOperationInProgress` fails open on non-`ErrNotExist` stat errors: log and treat marker absent.
- Same-branch rewinds set `daemon_meta.replay.paused_until = now + ACD_REWIND_GRACE_SECONDS`; capture/replay pause during grace; `0` disables.
- Manual marker `<gitDir>/acd/paused` wins over rewind grace. Malformed/non-regular marker fails open with warning.
- SQLite read errors in `daemonPauseState` fail closed for that tick.
- Diverged drops stale `pending` rows for previous generation only; keep `published`, `failed`, `blocked_conflict`.
- Diverged-attached-from-detached must clear `MetaKeyDetachedHeadPaused` and rewind grace metadata.

## Capture and fsnotify

- Capture compares live worktree to `shadow_paths`; stale/missing bootstrap can misclassify tracked files as phantom creates.
- `walkLive` BFSes by directory layer, batches ignore checks with `ignoreCheckBatchSize=1000`, prunes ignored dirs before readdir.
- `fsnotify_watcher.preWalk` mirrors `walkLive` ignore/sensitive/safe-ignore pruning.
- Do not prune worktree-rooted `acd/`; daemon state is under `.git/acd`.
- Symlinks are mode `120000`; never descend into symlinked dirs.
- Empty `ACD_SENSITIVE_GLOBS` falls back to defaults; never let typo/misconfig disable defaults.
- Sensitive dir pruning uses only literal dir names; wildcards like `credentials*` are file-granular.
- Safe-ignore defaults: `node_modules/`, `target/`, `.venv/`, `venv/`, `__pycache__/`, `.pytest_cache/`, `.mypy_cache/`, `.ruff_cache/`, `.gradle/`.
- `ACD_SAFE_IGNORE=0|false|no|off` disables safe-ignore; `ACD_SAFE_IGNORE_EXTRA=dist/,build/` appends valid patterns. Invalid extras are ignored.
- Safe-ignore dir patterns prune dirs/descendants, not same-named files. Use `SafeIgnoreMatcher.MatchFile` for files/symlinks and `MatchDirectory` for dirs.
- `IgnoreChecker.Check` uses long-lived `git check-ignore --stdin -z --non-matching --verbose`.
- Stream stdin from a writer goroutine while reading stdout; one large `stdin.Write` deadlocks on macOS 16 KiB pipes.
- `IgnoreChecker.Close` is non-blocking: atomic cancel, `killLocked`, bounded `cmd.Wait` at 2s.
- `git check-ignore --stdin` does not reload `.gitignore`; invalidate before each capture pass and on `.gitignore` fsnotify events. Tests: `TestIgnoreCheckerInvalidateReloadsGitignore`, `TestHandleEventInvalidatesIgnoreCheckerOnGitignoreChange`.

## Replay

- Replay uses per-pass scratch index `<gitDir>/acd/replay-*.index` seeded from `cctx.BaseHead`.
- Scratch-index reads use `git.LsFilesIndex(ctx, repoDir, indexFile, paths...)`.
- Replay CAS targets literal `HEAD` via `git.UpdateRef`; named refs still use `--no-deref`.
- `DefaultReplayLimit = 64`; query `Limit+1`, trim, set `ReplaySummary.HasMore`.
- Each event has 60s budget. Timeout marks blocked; `MarkEventBlocked` errors propagate.
- `blocked_conflict` is terminal/seq barrier; `PendingEvents` hides later pending rows behind prior `blocked_conflict`/`failed` for same branch/generation.
- Idempotent publish checks current `HEAD` before before-state blocking. If `HEAD` has desired final blob/mode/absence, mark published with `commit_oid=HEAD`.
- Conflict metadata: JSON in `daemon_meta.last_replay_conflict`; legacy string mirror `last_replay_conflict_legacy`.
- Live-index reconciliation after publish is guarded/path-scoped. Do not overwrite user-staged changes. See `internal/git/tree.go`, `internal/daemon/replay.go`, `internal/daemon/live_index_repair.go`.
- Startup/recover repair handles old published events whose live index stayed stale; doctor may report candidates.
- `replay.live_index` traces are success records unless failed/blocked; successful `applied` keeps `error` empty.
- `replayUpdateRefBackoffs` uses `math/rand/v2` jitter +-25%.

## Run loop, watcher, logs

- `processBranchTokenChange` runs before capture and after flush drain. Do not collapse; post-flush recheck handles git surgery outside `wakeCh`.
- Flush drain bounded by `DefaultFlushLimit = 256`; check `ctx.Err()` and `shutdownCh`.
- Per-tick metadata writes batch through `state.MetaSetMany`.
- `MetaKeyBranchHead` keep-alive is value-guarded by closure-scoped `lastStampedBranchHead`.
- Startup sweeps `acknowledged` flush requests older than `OrphanFlushAckThreshold = 5m` to `failed`.
- Fingerprint warn LRU cap 1024; evict 256 oldest.
- Warn limiters and `ClampRewindGraceAtStartup` must handle backward NTP steps.
- fsnotify dispatch must not block: runtime creates use `rewalkCh`/`rewalkWorker`; diagnostics use `diagCh`; trailing timer clamps at `MaxDebounceTail = 500ms`; ENOSPC -> `errBudgetExceeded`; `Stop(context.Context)` bounded.
- fsnotify CLI env: `ACD_FSNOTIFY_ENABLED` enables, `ACD_DISABLE_FSNOTIFY` forces poll-only, `ACD_MAX_INOTIFY_WATCHES` overrides Linux watch budget.
- Daemon logs are JSONL at `paths.Roots.RepoLogPath(repoHash)` (`~/.local/state/acd/<repo-hash>/daemon.log`), with rotation/compression in `internal/logger`.
- `acd doctor` tails logs best-effort, sanitizes `$HOME` to `~`, and bundles `daemon-tail.log`, `sensitive-globs.txt`, `safe-ignore-patterns.txt`, `fsnotify-stats.json`, state/meta JSON.

## Git, AI, trace

- `internal/git`: `RunOpts.Timeout`, `RunWithLimit`, `ErrStdoutOverflow`, `DefaultReadTimeout=30s`, `DefaultWriteTimeout=60s`.
- Diff/blob helpers route `DefaultReadTimeout` and `git.DefaultDiffCap` (1 MiB) through `RunWithLimit`; overflow returns partial prefix + `ErrStdoutOverflow`.
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

Useful live checks:

```bash
acd status --repo .
acd diagnose --repo . --json
acd doctor --repo . --json
sqlite3 .git/acd/state.db "SELECT state, COUNT(*) FROM capture_events GROUP BY state;"
sqlite3 .git/acd/state.db "SELECT seq, operation, path, substr(error,1,100) FROM capture_events WHERE state='blocked_conflict' ORDER BY seq DESC LIMIT 20;"
sqlite3 .git/acd/state.db "SELECT COUNT(*) FROM capture_events WHERE path LIKE 'node_modules/%' OR path LIKE 'dist/%' OR path LIKE '.trekoon/%';"
git status --short --ignored
```

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
- It creates `.git/acd/state.db.recover-<timestamp>`, retargets pending/blocked rows to current attached branch/generation, resets blocked rows to pending, clears replay/pause metadata, removes manual pause marker.
- Use `acd resume --yes` when only lifting a manual pause.

Manual cleanup:

```bash
acd pause --repo . --reason "manual reset" --yes
acd resume --repo . --yes
sqlite3 .git/acd/state.db "DELETE FROM capture_events WHERE state='blocked_conflict';"
```

## Harness adapter gotchas

- Codex template: `templates/codex/config.snippet.toml`.
- Codex hooks require `[features] codex_hooks = true`, `[[hooks.<EventName>]]`, then nested `[[hooks.<EventName>.hooks]]`; flat `[[hooks]]` fails.
- Codex hook stdout must be valid JSON. Snippet redirects `acd` output to `/dev/null` and emits `printf "{}\n"`.
- No `Stop` hook in Codex snippet; it races replay drain. Cleanup uses `watch_pid` death plus refcount sweep.
- Codex can auto-load both `~/.codex/hooks.json` and `~/.codex/config.toml`; delete old `hooks.json` after installing toml snippet.
- Templates use `acd hook-stdin-extract <field>` instead of `jq`; keep helper in `internal/cli/root.go` and AdapterE2E coverage.
- `internal/adapter` is real code for harness config/marker detection; do not restore old TODO stubs.

## Environment knobs

- Trace: `ACD_TRACE` truthy enables JSONL; `ACD_TRACE_DIR` default `<gitDir>/acd/trace`.
- Shadow: `ACD_SHADOW_RETENTION_GENERATIONS=1`; rewind: `ACD_REWIND_GRACE_SECONDS=60`, `0` disables.
- Capture: `ACD_SENSITIVE_GLOBS` empty/whitespace keeps defaults; `ACD_SAFE_IGNORE` default enabled, `0|false|no|off` disables; `ACD_SAFE_IGNORE_EXTRA` appends comma-separated patterns.
- AI: `ACD_AI_PROVIDER=deterministic|openai-compat|subprocess:<name>`; `ACD_AI_BASE_URL=https://api.openai.com/v1` must be absolute HTTPS; `ACD_AI_API_KEY` missing degrades openai-compat to deterministic; `ACD_AI_MODEL=gpt-4o-mini`; `ACD_AI_TIMEOUT=30s`; `ACD_AI_CA_FILE`; `ACD_AI_DIFF_EGRESS` truthy opts in to redacted diff egress.
- Watcher/client: `ACD_FSNOTIFY_ENABLED` truthy enables fsnotify in CLI; `ACD_DISABLE_FSNOTIFY` forces poll-only; `ACD_MAX_INOTIFY_WATCHES` overrides Linux budget; `ACD_CLIENT_TTL_SECONDS` overrides CLI heartbeat TTL.

## Release notes

- `.goreleaser.yaml` hardcodes `prerelease: false`; date tags otherwise become pre-releases and `releases/latest` breaks.
- Brew publishing is gated behind `--skip=homebrew` until `HOMEBREW_TAP_TOKEN` and tap repo exist.
