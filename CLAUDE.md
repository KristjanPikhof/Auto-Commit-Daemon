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

- **`shadow_paths` is keyed by `(branch_ref, branch_generation)`.** Whenever the generation bumps (Diverged transition) or branch ref changes, you MUST reseed via `BootstrapShadow(ctx, repoDir, db, cctx)` or the next capture pass classifies every tracked file as a phantom `create`. Idempotent — guarded by COUNT(*) check. Successful reseeds prune old generations via `ACD_SHADOW_RETENTION_GENERATIONS` (default `1` prior generation).
- **Branch-generation token**: format `rev:<sha> <branch-ref>` for an attached HEAD, `rev:<sha>` for detached HEAD, and `missing <branch-ref>` when an attached ref has no commit. Fast-forward (newHead descends from prevHead on the same branch ref) keeps generation; Diverged (reset/rebase/branch-switch/same-SHA ref switch) bumps it. Persisted in `daemon_meta` as `branch.generation` + `branch.head` + `branch_token`.
- **Detached HEAD pauses capture/replay.** `acd start` refuses to register on detached HEAD; the daemon stores `detached_head_paused` and leaves `CaptureContext.BranchRef` empty until reattached. Never fall back to `refs/heads/main` when `git symbolic-ref` fails.
- **Replay uses an isolated per-pass scratch index** (`<gitDir>/acd/replay-*.index`) seeded from `cctx.BaseHead`. Helper: `git.LsFilesIndex(ctx, repoDir, indexFile, paths...)`. Never inspect the live repo index for queued history.
- **`blocked_conflict` is terminal and forms a seq barrier.** Set via `state.MarkEventBlocked` (atomic update of `capture_events` + `publish_state`). Daemon never retries. `PendingEvents` hides later pending rows for the same `(branch_ref, branch_generation)` behind any earlier `blocked_conflict` or `failed` row, so downstream events do NOT leapfrog a broken predecessor across replay passes. Terminal rows older than retention are pruned only when they are no longer the active barrier.
- **AI diff text is opt-in.** By default providers receive empty `DiffText`; `ACD_AI_SEND_DIFF=1` enables redacted captured diffs built from `before_oid`/`after_oid` blobs (`internal/daemon/message.go::BuildOpsDiff`). Deterministic provider declares `NeedsDiff=false` and skips reconstruction. Diff rendering is capped during construction at `DiffCap` and survives live worktree changes after capture.

## Known issues / flaky tests

- **Timing-sensitive in `internal/daemon` under broad package runs**: `TestRun_FsnotifyDrivesWake`, `TestRun_LifecycleHappyPath`, `TestRun_WakeBurstCoalesced`, `TestRun_RealSIGUSR1`, and `TestRun_RepeatedEditsToSameFile_OrderedCommits`. Prefer focused `-run` verification when diagnosing unrelated lanes, then run the full suite before merge.

## Gotchas

- **`modernc.org/sqlite`** drives the DB without cgo. Pinned at `v1.36.0` to keep the `go 1.22` directive (newer sqlite needs go ≥ 1.23). Platform breakage = §17.1 risk, STOP and surface options.
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
# Inspect event states
sqlite3 .git/acd/state.db "SELECT state, COUNT(*) FROM capture_events GROUP BY state;"

# Inspect blocked events with reasons
sqlite3 .git/acd/state.db "SELECT seq, operation, path, substr(error,1,100) FROM capture_events WHERE state='blocked_conflict' ORDER BY seq DESC LIMIT 20;"

# Drop blocked rows (terminal, safe to delete)
sqlite3 .git/acd/state.db "DELETE FROM capture_events WHERE state='blocked_conflict';"
```

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
