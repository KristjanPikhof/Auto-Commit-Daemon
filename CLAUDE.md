# Agents Guide

## Identity

- **Binary**: `acd` (single static cross-platform Go binary)
- **Module**: `github.com/KristjanPikhof/Auto-Commit-Daemon`
- **License**: MIT
- **Versioning**: date-based, `vYYYY-MM-DD`
- **Platforms**: macOS (arm64, amd64), Linux (arm64, amd64). No Windows in v1.

## Build / test / verify

```bash
make build          # CGO_ENABLED=0, -tags=netgo,osusergo, ldflags-injected version
make test           # go test ./... -race -count=1
make lint           # go vet + gofmt -l (must be empty)
./bin/acd version

# Integration tests (build-tagged)
go test ./test/integration/... -tags=integration -race -count=1 -timeout 5m
```

## Pre-merge verification (mandatory)

Before declaring work done, before pushing the final commit on a branch, and before opening a PR for review, run the full suite locally with the race detector:

```bash
make lint
make test                                                                                                       # ./... -race -count=1
go test ./test/integration/... -tags=integration -race -count=1 -timeout 5m
go test ./internal/daemon/... ./internal/git/... ./internal/state/... ./internal/pause/... ./internal/cli/... -race -count=3 -timeout 10m
```

Why: Ubuntu CI has caught real races and ordering bugs that pass on a single macOS run because of timing differences. CI failure ≠ flake by default — assume race or ordering bug until ruled out. The `race-stress` lane in `.github/workflows/ci.yml` runs the broader `-count=3` set on every PR.

When CI fails on a previously-green branch:

1. Re-read failing test name + file:line from the log.
2. `WARNING: DATA RACE` or `panic: ... nil pointer` → real bug; fix root cause; do not retry.
3. Timing-sensitive `internal/daemon` failure → reproduce locally with `-count=10` (and `GOMAXPROCS=1 -count=50` to expose ordering hazards). Only retry CI if you cannot reproduce after both.
4. Cross-check macOS-only assumptions: fsnotify event ordering, process exec timing, `/tmp` semantics differ on Linux.

Common Linux-only failure modes seen on this codebase:

- **Test-design race against boot iteration.** Test stages multiple HEAD transitions but the daemon's boot iteration may observe phase 1 *or* phase 2 depending on scheduler. Fix: `waitForMetaValue(MetaKeyBranchHead, <expected>, 3s)` between phases so each phase is observed deterministically before moving on. Pattern in `TestRun_PostFlushBranchTokenReCheck`.
- **Real ordering bug masked by macOS scheduling.** Daemon iteration finishes before the test mutates state on macOS, hiding a missing meta-clear. On Linux the iteration races and exposes it. Don't relax the assertion — fix the production path. Example: Diverged-attached-from-detached must clear `MetaKeyDetachedHeadPaused` and `MetaKeyReplayPausedUntil` (`internal/daemon/daemon.go` Diverged branch); otherwise the dedicated reattach branch is bypassed forever once `cctx.BranchRef` is set.

## Refresh local install

```bash
make build && install -m 0755 ./bin/acd ~/.local/bin/acd
```

Required after any `templates/*` edit (templates baked at build time via `templates/embed.go`).

## Conventions

- **Stub format**: `package <name>` + `// TODO(phase N): <intent>`. Stubs must compile.
- **Markdown nested code**: README + adapter docs use `~~~` fences when nesting code blocks.
- **Embed**: `templates/embed.go` is the single embed point. Extend its `//go:embed` line for new harnesses.
- **Test fixtures must pin branch name**: after `git.Init` (or `git init`), call `git symbolic-ref HEAD refs/heads/main`. CI runners default to `master`.

## Architecture invariants

- **`shadow_paths` is keyed by `(branch_ref, branch_generation)`.** Whenever the generation bumps (Diverged transition) or branch ref changes, you MUST reseed via `BootstrapShadow(ctx, repoDir, db, cctx)` or the next capture pass classifies every tracked file as a phantom `create`. Idempotency is gated by a `daemon_meta` completion marker — `IsShadowBootstrapped` checks the per-(branch, generation) key formatted by `ShadowBootstrappedKey` (prefix `MetaKeyShadowBootstrappedPrefix = "shadow.bootstrapped:"`). The previous COUNT(*) gate was unsafe under partial bootstrap failure and has been removed. Successful reseeds prune old generations via `ACD_SHADOW_RETENTION_GENERATIONS` (default `1` prior generation).
- **Shadow bootstrap is chunked, transactional, and self-cleaning.** `BootstrapShadow` walks `HEAD` and feeds rows to `state.AppendShadowBatch` in 5000-row chunks (`bootstrapShadowChunkSize` in `internal/daemon/bootstrap.go`); each chunk is its own transaction. The completion marker is written ONLY after every chunk has committed; on any failure the partially-inserted rows for `(branch_ref, generation)` are deleted before returning the error so the next pass re-walks from a clean slate.
- **Branch-generation token**: format `rev:<sha> <branch-ref>` for an attached HEAD, `rev:<sha>` for detached HEAD, and `missing <branch-ref>` when an attached ref has no commit. Fast-forward (newHead descends from prevHead on the same branch ref) keeps generation; Diverged (reset/rebase/branch-switch/same-SHA ref switch) bumps it. Persisted in `daemon_meta` as `branch.generation` + `branch.head` + `branch_token`.
- **Legacy branch tokens without a ref name force Diverged on upgrade.** A persisted `rev:<sha>` or bare `missing` token followed by an attached `rev:<sha> <branch-ref>` or `missing <branch-ref>` is treated as Diverged even when the SHA is unchanged. This intentionally reseeds shadow state and avoids replaying stale queued rows onto a newly identified branch.
- **Detached HEAD pauses capture/replay.** `acd start` refuses to register on detached HEAD; the daemon stores `detached_head_paused` and leaves `CaptureContext.BranchRef` empty until reattached. Never fall back to `refs/heads/main` when `git symbolic-ref` fails.
- **Git operations pause capture/replay.** `rebase-merge`, `rebase-apply`, `MERGE_HEAD`, `CHERRY_PICK_HEAD`, and `BISECT_LOG` in the git dir set `operation_in_progress`; the daemon skips branch-token, capture, and replay work until the marker clears.
- **Replay uses an isolated per-pass scratch index** (`<gitDir>/acd/replay-*.index`) seeded from `cctx.BaseHead`. Helper: `git.LsFilesIndex(ctx, repoDir, indexFile, paths...)`. Never inspect the live repo index for queued history.
- **Idempotent publish handles parallel committers before blocking.** When the scratch-index before-state probe would otherwise produce `blocked_conflict`, replay checks the current `HEAD` tree for every op's desired final state. If `HEAD` already has the captured blob/mode, or the path is already absent for delete/rename cleanup, the event is marked `published` with `commit_oid=HEAD` and no new commit is created. This only narrows the before-state mismatch path; real mismatches still become terminal `blocked_conflict` rows.
- **Replay CAS targets literal `HEAD`.** The replay path calls `git update-ref HEAD <new> <old>` through `git.UpdateRef`; literal `HEAD` must dereference to the worktree's active branch, while named refs continue to use `--no-deref`. This keeps linked worktrees and same-SHA branch switches anchored to the current worktree.
- **Replay pause gate checks manual marker before rewind grace.** `gitDir/acd/paused` is a durable JSON marker owned by `acd pause` and `acd resume`; the daemon reads it once per replay pass and never deletes it. Malformed markers fail open with a warning. If no active manual marker exists, replay checks `daemon_meta.replay.paused_until`; a future timestamp skips the drain, and an expired timestamp is cleared. Rewind grace defaults to 60 seconds and is controlled by `ACD_REWIND_GRACE_SECONDS` (`0` disables it).
- **Same-branch rewinds pause BOTH capture and replay during the grace window.** When `newHead` is an ancestor of `prevHead` on the same branch ref, the daemon writes `daemon_meta.replay.paused_until = now + ACD_REWIND_GRACE_SECONDS`. During the grace window, BOTH capture and replay are paused so a transient revert+re-edit dance does not race the operator — fsnotify fires as untracked files reappear and a post-grace replay drain would otherwise resurrect work the operator just rewound. The marker is auto-cleared on expiry; capture rows are NOT created while the gate is active. Scope: same-ref rewinds only. Ref-switch divergences go through Diverged + `DeletePendingForGeneration`. Same-SHA branch switches also go through Diverged. Detached-HEAD transitions use `MetaKeyDetachedHeadPaused`.
- **`blocked_conflict` is terminal and forms a seq barrier.** Set via `state.MarkEventBlocked` (atomic update of `capture_events` + `publish_state`). Daemon never retries. `PendingEvents` hides later pending rows for the same `(branch_ref, branch_generation)` behind any earlier `blocked_conflict` or `failed` row, so downstream events do NOT leapfrog a broken predecessor across replay passes. Terminal rows older than retention are pruned only when they are no longer the active barrier.
- **Diverged drops stale pending rows only.** On Diverged, delete `pending` capture events for the previous branch generation. Do not delete `blocked_conflict`, `failed`, or `published` rows; those remain operator-visible.
- **Replay conflict metadata is structured.** `daemon_meta.last_replay_conflict` stores JSON with `ts`, `seq`, `error_class`, `expected_sha`, `actual_sha`, `ref`, `path`, and `message`. `last_replay_conflict_legacy` mirrors the old single-line string for backward-compatible tooling.
- **AI diff text follows provider capability.** Network providers declare `NeedsDiff=true` and receive a redacted unified diff built from `before_oid` and `after_oid` blobs (`internal/daemon/message.go::BuildOpsDiff`). `DeterministicProvider` declares `NeedsDiff=false` and receives an empty `DiffText`. Diff rendering is capped at `DiffCap` during construction.
- **Trace logging is opt-in and best-effort.** `ACD_TRACE=1` writes JSONL decision records to `<gitDir>/acd/trace/` or `ACD_TRACE_DIR`. Trace writes never block or abort capture/replay.
- **`walkLive` and `fsnotify_watcher.preWalk` both use BFS-by-layer ignore-prune.** Each directory layer is batch-classified via `IgnoreChecker.Check` with `ignoreCheckBatchSize=1000` paths per call before descending; ignored directories are pruned from the next frontier so subtrees like `build/`, `node_modules/`, `DerivedData/` are never readdir'd. The two paths are deliberately symmetric — divergence between them previously hid the v2026-05-01 P0 capture deadlock. Helper: `classifyIgnoredBatched` in `internal/daemon/capture.go`.
- **`IgnoreChecker.Check` stream-pumps stdin via a writer goroutine** before entering the read loop. Single `stdin.Write` of every path would deadlock against the macOS 16 KiB pipe buffer when the batch is large. On read error the subprocess is `killLocked` and `errCh` is drained; on read success deferred write errors surface via `errCh`. Do not refactor back to a synchronous write.

## Daemon run-loop invariants

- **`processBranchTokenChange` runs ONCE per Run iteration.** The double-call pattern (pre-capture and post-flush) was a v2026-05-01 bug; HEAD does not move between the two calls without an explicit wake.
- **Diverged-attached-from-detached must clear pause markers.** When `tokenBranchRef(oldToken) == ""` and the new token is attached, the Diverged branch in the run loop must `MetaDelete(MetaKeyDetachedHeadPaused)` and `clearRewindGraceMeta`. Otherwise the dedicated reattach branch (which fires only when `cctx.BranchRef == ""`) is bypassed forever once the Diverged path sets `cctx.BranchRef`.
- **Replay budget is bounded.** `DefaultReplayLimit = 64`; `Replay` queries `Limit+1` rows, trims the extra, and sets `ReplaySummary.HasMore` to cue the next-iteration decision (immediate re-wake vs. natural tick). The run loop wires `ReplayLimit: DefaultReplayLimit` on every call.
- **Flush drain is bounded.** `DefaultFlushLimit = 256`; the run loop resolves `flushLimit` from `opts.FlushLimit` falling back to the default. The inner drain loop checks `ctx.Err()` at the top of every iteration and breaks on cancel — SIGTERM mid-drain exits within ~100 ms.
- **`MetaKeyBranchHead` per-tick MetaSet is value-guarded.** A closure-scoped `lastStampedBranchHead` (seeded from `persistedHead`) skips the keep-alive write when `liveHead == lastStampedBranchHead`. Idle daemons do not churn the meta table every tick.
- **Startup orphan-acked sweep.** `sweepOrphanAckedFlushRequests` runs once at boot and marks `acknowledged` rows older than `OrphanFlushAckThreshold = 5 * time.Minute` as `failed`, so a crashed worker does not leave forever-stuck rows.
- **`daemonPauseState` fails open on non-regular markers.** `pause.Read` returning `ErrMalformed` *or* `ErrNonRegularSource` (FIFO/socket/dir/symlink at `<gitDir>/acd/paused`) logs a warning and treats the marker as absent. Stale FIFOs do not wedge replay.

## Known issues / flaky tests

- **Timing-sensitive in `internal/daemon` under broad package runs**: `TestRun_FsnotifyDrivesWake`, `TestRun_LifecycleHappyPath`, `TestRun_WakeBurstCoalesced`, `TestRun_RealSIGUSR1`, `TestRun_RepeatedEditsToSameFile_OrderedCommits`. Prefer focused `-run` verification when diagnosing unrelated lanes, then run the full suite before merge.
- **Multi-phase HEAD-transition tests must phase deterministically.** When a test stages two HEAD movements and asserts a transition was classified, insert `waitForMetaValue(MetaKeyBranchHead, <phase1HeadSha>, 3s)` between the phases so the daemon's boot iteration cannot race past phase 1 unobserved. Stabilization pattern applied to `TestRun_PostFlushBranchTokenReCheck` (commit `ab52b32`); skipping it produces 3-of-50 Linux flakes under `GOMAXPROCS=1 -count=50`.

## Gotchas

- **`modernc.org/sqlite`** drives the DB without cgo. Pinned at `v1.36.0` to keep the `go 1.22` directive (newer sqlite needs go ≥ 1.23). Platform breakage = STOP and surface options to the user; do not bump go or sqlite without explicit approval.
- **Symlinks**: always captured as mode `120000`. Never descend into symlinked directories. Fixture: `TestCapture_SymlinkToDirAsMode120000`.
- **Sensitive globs**: empty `ACD_SENSITIVE_GLOBS` falls back to defaults. Never let a typo open the gate.
- **Sensitive directory pruning**: fsnotify prunes only literal sensitive directory names. Wildcard file patterns like `credentials*` are applied at file granularity so ordinary directories such as `credentials_repo` are still watched.

## Harness adapter gotchas

- **Codex hooks** (`templates/codex/config.snippet.toml`) need 3-level schema: `[features] codex_hooks = true`, then `[[hooks.<EventName>]]` wrapping `[[hooks.<EventName>.hooks]]` (handler with `type = "command"` + `command`). Flat `[[hooks]]` arrays do NOT work.
- **Codex hook stdout must be valid JSON.** Snippet redirects `acd` output to `/dev/null` and emits `printf "{}\n"`.
- **No `Stop` hook in the Codex snippet** — races the replay drain. Cleanup via `watch_pid` death + refcount sweep instead.
- **Codex auto-loads both `~/.codex/hooks.json` and `~/.codex/config.toml`.** Delete the old hooks.json after installing the toml snippet.
- **Hook JSON extraction**: templates use `acd hook-stdin-extract <field>` instead of `jq`; keep that helper registered in `internal/cli/root.go` and covered by AdapterE2E.
- **Adapter package is real code.** `internal/adapter` detects installed harness config files and markers for `acd init` auto-detect and `acd doctor`; do not restore the old TODO-only stubs.

## Recovery / cleanup

```bash
# Inspect the current anchor, blocked histogram, and recent blockers
acd diagnose --repo .
acd diagnose --repo . --json

# Inspect event states
sqlite3 .git/acd/state.db "SELECT state, COUNT(*) FROM capture_events GROUP BY state;"

# Inspect blocked events with reasons
sqlite3 .git/acd/state.db "SELECT seq, operation, path, substr(error,1,100) FROM capture_events WHERE state='blocked_conflict' ORDER BY seq DESC LIMIT 20;"

# Pause replay while doing manual branch surgery, then resume explicitly
acd pause --repo . --reason "manual reset" --yes
acd resume --repo . --yes

# Drop blocked rows (terminal, safe to delete)
sqlite3 .git/acd/state.db "DELETE FROM capture_events WHERE state='blocked_conflict';"
```

### Incident recovery cookbook

Use the built-in recovery flow before editing SQLite by hand:

```bash
# 1. Confirm the current anchor and blocker shape
acd diagnose --repo . --json

# 2. Preview the recovery plan; this must not mutate state.db
acd recover --repo . --auto --dry-run --json

# 3. Apply only after reading the plan. A byte-for-byte backup is created as
#    .git/acd/state.db.recover-<timestamp>.
acd recover --repo . --auto --yes

# 4. Wake the daemon and inspect the queue
acd wake --repo . --session-id <session>
acd status --repo .
```

The original 145-event incident pattern is: `daemon_state.branch_ref` and queued `capture_events.branch_ref` point at a stale branch, while `git symbolic-ref HEAD` points at the active branch. `acd recover` retargets pending/blocked rows to the current attached branch and generation, resets `blocked_conflict` rows to `pending`, clears stale replay/pause metadata, and refuses to run while the daemon PID is alive. `acd recover --auto` now also clears `daemon_meta.replay.paused_until` and removes the on-disk manual pause marker. Run `acd resume --yes` instead when you only need to lift a manual pause without triggering the full recover flow.

## Environment knobs

| Variable | Default | Effect |
|---|---:|---|
| `ACD_TRACE` | unset | Truthy values `1`, `true`, `yes` enable JSONL trace logging. |
| `ACD_TRACE_DIR` | `<gitDir>/acd/trace` | Overrides the trace output directory. |
| `ACD_SHADOW_RETENTION_GENERATIONS` | `1` | Number of prior shadow generations retained after reseed. |
| `ACD_SENSITIVE_GLOBS` | built-in defaults | Empty string falls back to defaults. |
| `ACD_REWIND_GRACE_SECONDS` | `60` | Seconds to pause replay after same-branch rewind detection. `0` disables the grace. |
| `ACD_AI_DIFF_EGRESS` | unset | Truthy (`1`/`true`/`yes`) opts in to sending reconstructed diffs to network AI providers. Off by default; metadata-only payload otherwise. |

Diff-egress migration: `ACD_AI_SEND_DIFF` was removed. Setting it now emits a one-shot deprecation warn-log at daemon startup. See `docs/ai-providers.md` for the full opt-in semantics — the canonical source of truth for AI payload behavior.

### Trace log format

Trace files rotate daily as `YYYY-MM-DD.jsonl`. Every line is JSON:

```json
{"ts":"2026-04-29T12:34:56.000000789Z","repo":"/repo/acd","branch_ref":"refs/heads/main","head_sha":"dddddddddddddddddddddddddddddddddddddddd","event_class":"replay.commit","decision":"published","reason":"event published","input":{"operation":"create","path":"file.txt"},"output":{"commit":"eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee","parent":"dddddddddddddddddddddddddddddddddddddddd"},"error":"","seq":4,"generation":7}
```

Known `event_class` values (verify with `grep -rn "EventClass:" internal/`):

| `event_class` | When emitted | Notable `input`/`output` fields |
|---|---|---|
| `bootstrap_shadow.reseed` | Shadow state reseeded after Diverged or at startup | out: `rows` |
| `capture.classify` | Worktree vs. shadow diff computed | out: `ops`, `walked_files`, `oversize`, `errors` |
| `capture.event` | Op written to `capture_events` (`appended`) or dropped at cap (`dropped`) | in: `op`, `path`, `fidelity`; out: `seq` or `pending_depth`/`cap` |
| `capture.pause` | Capture skipped because replay is paused | out: `source`, `reason`, `set_at`, `expires_at`, `remaining_seconds` |
| `replay.commit` | Event published as a commit or idempotent HEAD match | in: `operation`, `path`; out: `commit`, `parent` |
| `replay.conflict` | Event becomes `blocked_conflict` | in: `operation`, `path`; out: `expected_sha`, `actual_sha`, `ref` |
| `replay.failed` | Event becomes `failed` (bad ops, ancestry, write-tree) | in: `operation`, `path` |
| `replay.update_ref` | Each `git update-ref` attempt (one record per retry) | out: `attempt`, `max_attempts`, `retry`, `ref`, `commit`, `expected_sha` |
| `replay.pause` | Replay drain skipped because paused | out: `source`, `reason`, `set_at`, `expires_at`, `remaining_seconds` |
| `branch_token.transition` | HEAD movement classified (startup or per-tick) | in: `previous`, `current`; out: `prev_generation`, `new_generation`, `dropped_pending` |
| `daemon.pause` | Git operation marker detected (`paused`) or cleared (`resumed`) | in: `operation` |

## Release one-liners

```bash
# Cut a new release
git tag v2026-MM-DD && git push origin v2026-MM-DD && gh run watch

# Fix a release auto-marked as pre-release
gh release edit v2026-MM-DD --prerelease=false --latest

# Smoke-test install.sh
ACD_VERSION=v2026-MM-DD sh scripts/install.sh
```

`.goreleaser.yaml` hardcodes `prerelease: false` (date tags would otherwise be auto-pre-released and `releases/latest` would return nothing). Brew step gated behind `--skip=homebrew` until `HOMEBREW_TAP_TOKEN` PAT + tap repo exist.
