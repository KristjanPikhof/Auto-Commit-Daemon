# Agent guide

## Identity

- Binary: `acd`, single static Go CLI/daemon.
- Module: `github.com/KristjanPikhof/Auto-Commit-Daemon`
- Versioning: date tags, `vYYYY-MM-DD`; `make build` injects version and git SHA.
- Platforms: macOS and Linux, `arm64`/`amd64`. No Windows in v1.
- License: MIT.

## Commands

```bash
make build          # CGO_ENABLED=0, -tags=netgo,osusergo
make test           # go test ./... -race -count=1
make lint           # go vet ./... plus gofmt check
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

Install current local build:

```bash
make build && install -m 0755 ./bin/acd ~/.local/bin/acd
```

Required after `templates/*` edits because snippets are embedded by `templates/embed.go`.

Release smoke:

```bash
git tag v2026-MM-DD && git push origin v2026-MM-DD && gh run watch
gh release edit v2026-MM-DD --prerelease=false --latest
ACD_VERSION=v2026-MM-DD sh scripts/install.sh
```

## Project map

| Path | Purpose |
|---|---|
| `cmd/acd/main.go` | CLI entrypoint. |
| `internal/cli` | Cobra commands: start/stop/status/diagnose/recover/init/hooks/etc. |
| `internal/daemon` | Run loop, capture/replay, fsnotify, branch tokens, shadow bootstrap, recovery repair. |
| `internal/git` | Bounded git helpers, refs, trees, diff/blob reads, live index reconciliation, ignore checker. |
| `internal/state` | SQLite schema/migrations, events, shadow paths, daemon meta, clients, flush requests. |
| `internal/ai` | Deterministic, OpenAI-compatible, and subprocess providers. |
| `internal/adapter` | Harness detection for `acd init` and `acd doctor`. |
| `internal/central` | Registry and rollup stats DB. |
| `internal/pause` | Durable pause marker under `<gitDir>/acd/paused`. |
| `internal/trace` | Best-effort JSONL trace writer. |
| `templates/*` | Installed harness snippets. Update `templates/embed.go` for new files. |
| `test/integration` | Build-tagged lifecycle, adapter, recovery, ignored-tree, fallback tests. |
| `docs/*` | User-facing architecture and provider docs. |

Go is pinned to `go 1.22`. `modernc.org/sqlite` is pinned at `v1.36.0`; do not bump Go or sqlite for platform issues without explicit approval.

## Workflows and conventions

- Keep changes scoped. This repo has many timing-sensitive daemon tests.
- Test fixtures must pin branch names after `git.Init` or `git init`:
  `git symbolic-ref HEAD refs/heads/main`. CI defaults can be `master`.
- Stub format: `package <name>` plus `// TODO(phase N): <intent>`. Stubs must compile.
- README and adapter docs use `~~~` when nesting fenced code.
- Prefer `rg` over `grep`; some old comments say grep but repo practice is `rg`.
- Never inspect the live repo index when replaying queued events. Replay uses scratch indexes only.
- If CI fails with `WARNING: DATA RACE`, panic, nil pointer, or ordering failure, assume real bug until proved otherwise. Do not retry first.
- For timing-sensitive `internal/daemon` failures, reproduce with `-count=10`; also try `GOMAXPROCS=1 -count=50` for ordering hazards.
- Multi-phase HEAD-transition tests must wait for phase observation, usually `waitForMetaValue(MetaKeyBranchHead, <sha>, 3s)`.

Known timing-sensitive tests under broad runs: `TestRun_FsnotifyDrivesWake`, `TestRun_LifecycleHappyPath`, `TestRun_WakeBurstCoalesced`, `TestRun_RealSIGUSR1`, `TestRun_RepeatedEditsToSameFile_OrderedCommits`. Prefer focused `-run` while diagnosing, then run the full mandatory suite.

## Core data model

- SQLite state lives under `<gitDir>/acd/state.db`; central registry/stats are under user state/share paths.
- Schema is v4. `idx_flush_requests_status_id` keeps `ClaimNextFlushRequest` constant-time.
- `shadow_paths` is keyed by `(branch_ref, branch_generation, path)`.
- Shadow bootstrap is chunked in 5000-row transactions via `state.AppendShadowBatch`.
- Bootstrap idempotency is `daemon_meta` marker `shadow.bootstrapped:<branch_ref>:<generation>` from `ShadowBootstrappedKey`.
- Completion marker is written only after all chunks commit. On failure, partial rows for the branch/generation are deleted.
- Successful reseed prunes old generations using `ACD_SHADOW_RETENTION_GENERATIONS` (default `1` prior generation).
- If a bootstrap marker exists but `shadow_paths` is empty for the active branch/generation, delete that marker to force a clean re-bootstrap.
- `state.DB.ReadSQL()` is the supported read-only handle for read-heavy queries and shadow loads.

## Branch and pause invariants

- Branch token formats:
  - attached: `rev:<sha> <branch-ref>`
  - detached: `rev:<sha>`
  - missing attached ref: `missing <branch-ref>`
- Fast-forward on same branch keeps generation. Diverged resets/rebases/branch-switches/same-SHA ref switches bump generation.
- Legacy tokens without a branch ref force Diverged when upgraded to an attached token, even if SHA is unchanged.
- Detached HEAD pauses capture/replay. `acd start` refuses detached HEAD. Never fall back to `refs/heads/main` when `git symbolic-ref` fails.
- Git operation markers pause capture/replay: `rebase-merge`, `rebase-apply`, `MERGE_HEAD`, `CHERRY_PICK_HEAD`, `BISECT_LOG`.
- `gitOperationInProgress` fails open on non-`ErrNotExist` stat errors; log and treat marker absent.
- Same-branch rewinds write `daemon_meta.replay.paused_until = now + ACD_REWIND_GRACE_SECONDS`; both capture and replay pause during grace. `0` disables grace.
- Manual replay pause marker is `<gitDir>/acd/paused`. Manual marker wins over rewind grace. Malformed/non-regular disk marker fails open with warning.
- SQLite read errors in `daemonPauseState` fail closed for that tick.
- Diverged drops stale `pending` rows for the previous generation only. Keep `published`, `failed`, and `blocked_conflict`.
- Diverged-attached-from-detached must clear `MetaKeyDetachedHeadPaused` and rewind grace metadata.

## Capture invariants

- Capture compares live worktree to `shadow_paths`; stale or missing bootstrap can classify tracked files as phantom creates.
- `walkLive` uses BFS by directory layer, batches ignore checks with `ignoreCheckBatchSize=1000`, and prunes ignored directories before readdir.
- `fsnotify_watcher.preWalk` must mirror `walkLive` ignore-prune behavior.
- Do not prune a worktree-rooted `acd/`; daemon state is under `.git/acd`, outside the worktree.
- Symlinks are captured as mode `120000`. Never descend into symlinked directories.
- Sensitive globs: empty `ACD_SENSITIVE_GLOBS` falls back to defaults. Never let typo/misconfig open the gate.
- Sensitive directory pruning only uses literal sensitive directory names. Wildcard patterns such as `credentials*` apply at file granularity.
- `IgnoreChecker.Check` uses long-lived `git check-ignore --stdin -z --non-matching --verbose`.
- `IgnoreChecker.Check` must stream stdin from a writer goroutine while reading stdout. A single large `stdin.Write` deadlocks on macOS 16 KiB pipes.
- `IgnoreChecker.Close` is non-blocking: atomic cancel, `killLocked`, bounded `cmd.Wait` at 2s.
- `git check-ignore --stdin` does not reload `.gitignore` during a session. Keep `IgnoreChecker.Invalidate` behavior:
  - run loop invalidates before each capture pass,
  - fsnotify invalidates on worktree `.gitignore` events.
  Regression: stale checker committed ignored `node_modules/` and `dist/` in a live test. Tests: `TestIgnoreCheckerInvalidateReloadsGitignore`, `TestHandleEventInvalidatesIgnoreCheckerOnGitignoreChange`.

## Replay invariants

- Replay uses isolated per-pass scratch index `<gitDir>/acd/replay-*.index` seeded from `cctx.BaseHead`.
- Use `git.LsFilesIndex(ctx, repoDir, indexFile, paths...)` for scratch-index reads.
- Replay CAS targets literal `HEAD` via `git.UpdateRef`; named refs still use `--no-deref`.
- `DefaultReplayLimit = 64`; query `Limit+1`, trim, set `ReplaySummary.HasMore`.
- Every replay event has a 60s budget. Timeout marks event blocked, and `MarkEventBlocked` errors propagate.
- `blocked_conflict` is terminal and a seq barrier. `PendingEvents` hides later pending rows behind prior `blocked_conflict` or `failed` rows for the branch/generation.
- Idempotent publish checks current `HEAD` before blocking on before-state mismatch. If `HEAD` already has desired final blob/mode or absence, mark published with `commit_oid=HEAD`.
- Replay conflict metadata is JSON in `daemon_meta.last_replay_conflict`; legacy string mirror is `last_replay_conflict_legacy`.
- Live-index reconciliation after publish is guarded and path-scoped. Do not overwrite user-staged changes. Related files: `internal/git/tree.go`, `internal/daemon/replay.go`, `internal/daemon/live_index_repair.go`.
- Startup/recover repair handles old published events whose live index stayed stale. Doctor may report repair candidates.
- `replay.live_index` traces are success records unless decision is failed/blocked; successful `applied` must keep `error` empty.
- `replayUpdateRefBackoffs` uses `math/rand/v2` jitter +-25% to avoid aligned concurrent retries.

## Run-loop invariants

- `processBranchTokenChange` runs twice per iteration: before capture and after flush drain. Do not collapse to one call.
- Post-flush re-check is load-bearing because operator git surgery is not serialized through `wakeCh`.
- Flush drain is bounded by `DefaultFlushLimit = 256` and must check both `ctx.Err()` and `shutdownCh` each iteration.
- Per-tick metadata writes batch through `state.MetaSetMany`.
- `MetaKeyBranchHead` keep-alive is value-guarded by closure-scoped `lastStampedBranchHead`.
- Startup runs `sweepOrphanAckedFlushRequests`: `acknowledged` rows older than `OrphanFlushAckThreshold = 5m` become `failed`.
- Fingerprint warn LRU is capped at 1024 entries, evicting 256 oldest on overflow.
- Warn limiters and `ClampRewindGraceAtStartup` must handle backward NTP steps.
- fsnotify dispatch must not block on slow helpers:
  - runtime creates go through buffered `rewalkCh` and `rewalkWorker`,
  - diagnostics go through `diagCh`,
  - wake coalescing uses leading-edge fire plus trailing timer clamped at `MaxDebounceTail = 500ms`,
  - ENOSPC normalizes to `errBudgetExceeded`,
  - `Stop(context.Context)` is bounded.

## Git and diff helpers

- `internal/git` exposes `RunOpts.Timeout`, `RunWithLimit`, `ErrStdoutOverflow`, `DefaultReadTimeout=30s`, `DefaultWriteTimeout=60s`.
- Diff/blob helpers route `DefaultReadTimeout` and `git.DefaultDiffCap` (1 MiB) through `RunWithLimit`.
- On overflow, return partial prefix plus `ErrStdoutOverflow`; truncation must be observable.
- `RevParse` surfaces ambiguous refs as `git.ErrRefAmbiguous`; classify separately from missing ref.
- `ps` path is pinned: `/bin/ps` on Darwin, `/usr/bin/ps` on Linux. Do not use `$PATH`.
- `isSQLiteLocked` must unwrap `*sqlite.Error` and compare typed code before substring fallback.

## AI and messages

- Providers declare capability with `NeedsDiff`.
- Network providers get redacted unified diffs when `NeedsDiff=true` and `ACD_AI_DIFF_EGRESS` is truthy.
- `DeterministicProvider` uses `NeedsDiff=false` and gets empty `DiffText`.
- `BuildOpsDiff` uses git-layer caps through `git.DiffBlobsLimited` / `git.CatFileBlobLimited`; no post-render trim.
- Per-op diff render has 5s timeout.
- `ACD_AI_SEND_DIFF` was removed; setting it emits one startup deprecation warning.
- Message quality can be weak for generic modify events, e.g. `Update PopupApp.tsx`; treat as low-priority message-quality issue unless replay/state is wrong.

## Trace and diagnostics

- `ACD_TRACE=1` writes JSONL to `<gitDir>/acd/trace/YYYY-MM-DD.jsonl`.
- `ACD_TRACE_DIR` overrides trace dir. Trace writes are best-effort and must never block/abort work.
- Known event classes include:
  - `bootstrap_shadow.reseed`
  - `capture.classify`
  - `capture.event`
  - `capture.pause`
  - `replay.commit`
  - `replay.conflict`
  - `replay.failed`
  - `replay.update_ref`
  - `replay.live_index`
  - `replay.pause`
  - `branch_token.transition`
  - `daemon.pause`
- Use `rg -n "EventClass:" internal/` or direct trace helpers to verify additions.

Useful live checks:

```bash
acd status --repo .
acd diagnose --repo . --json
sqlite3 .git/acd/state.db "SELECT state, COUNT(*) FROM capture_events GROUP BY state;"
sqlite3 .git/acd/state.db "SELECT seq, operation, path, substr(error,1,100) FROM capture_events WHERE state='blocked_conflict' ORDER BY seq DESC LIMIT 20;"
sqlite3 .git/acd/state.db "SELECT COUNT(*) FROM capture_events WHERE path LIKE 'node_modules/%' OR path LIKE 'dist/%' OR path LIKE '.trekoon/%';"
git status --short --ignored
```

## Recovery

Prefer built-in recovery before hand-editing SQLite:

```bash
acd diagnose --repo . --json
acd recover --repo . --auto --dry-run --json
acd recover --repo . --auto --yes
acd wake --repo . --session-id <session>
acd status --repo .
```

`acd recover --auto` refuses to run while daemon PID is alive, creates `.git/acd/state.db.recover-<timestamp>`, retargets pending/blocked rows to current attached branch/generation, resets blocked rows to pending, clears stale replay/pause metadata, and removes the on-disk manual pause marker. Use `acd resume --yes` when only lifting a manual pause.

Manual cleanup snippets:

```bash
acd pause --repo . --reason "manual reset" --yes
acd resume --repo . --yes
sqlite3 .git/acd/state.db "DELETE FROM capture_events WHERE state='blocked_conflict';"
```

## Harness adapter gotchas

- Codex template: `templates/codex/config.snippet.toml`.
- Codex hooks require `[features] codex_hooks = true`, then `[[hooks.<EventName>]]`, then nested `[[hooks.<EventName>.hooks]]`.
- Flat `[[hooks]]` arrays do not work.
- Codex hook stdout must be valid JSON. Snippet redirects `acd` output to `/dev/null` and emits `printf "{}\n"`.
- No `Stop` hook in Codex snippet; it races replay drain. Cleanup uses `watch_pid` death plus refcount sweep.
- Codex can auto-load both `~/.codex/hooks.json` and `~/.codex/config.toml`; delete old `hooks.json` after installing toml snippet.
- Templates use `acd hook-stdin-extract <field>` instead of `jq`; keep helper in `internal/cli/root.go` and AdapterE2E coverage.
- `internal/adapter` is real code for harness config and marker detection; do not restore old TODO stubs.

## Environment knobs

| Variable | Default | Effect |
|---|---:|---|
| `ACD_TRACE` | unset | `1`/`true`/`yes` enables trace JSONL. |
| `ACD_TRACE_DIR` | `<gitDir>/acd/trace` | Trace output override. |
| `ACD_SHADOW_RETENTION_GENERATIONS` | `1` | Prior shadow generations retained. |
| `ACD_SENSITIVE_GLOBS` | built-in defaults | Empty string falls back to defaults. |
| `ACD_REWIND_GRACE_SECONDS` | `60` | Same-branch rewind pause seconds; `0` disables. |
| `ACD_AI_DIFF_EGRESS` | unset | Opts in to sending reconstructed diffs to network AI providers. |

## Release notes

- `.goreleaser.yaml` hardcodes `prerelease: false`; date tags otherwise become pre-releases and `releases/latest` breaks.
- Brew publishing is gated behind `--skip=homebrew` until `HOMEBREW_TAP_TOKEN` and tap repo exist.
