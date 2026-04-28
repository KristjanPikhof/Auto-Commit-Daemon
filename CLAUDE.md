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

- **`shadow_paths` is keyed by `(branch_ref, branch_generation)`.** Whenever the generation bumps (Diverged transition) or branch ref changes, you MUST reseed via `BootstrapShadow(ctx, repoDir, db, cctx)` or the next capture pass classifies every tracked file as a phantom `create`. Idempotent — guarded by COUNT(*) check.
- **Branch-generation token**: format `rev:<sha>` for an existing ref, `missing` otherwise. Fast-forward (newHead descends from prevHead) keeps generation; Diverged (reset/rebase/branch-switch) bumps it. Persisted in `daemon_meta` as `branch.generation` + `branch.head`.
- **Replay uses isolated scratch index** (`replay.index`) seeded from `cctx.BaseHead`. Helper: `git.LsFilesIndex(ctx, repoDir, indexFile, paths...)`. Never inspect the live repo index for queued history.
- **`blocked_conflict` is terminal.** Set via `state.MarkEventBlocked` (atomic update of `capture_events` + `publish_state`). Daemon never retries. Safe to bulk-DELETE blocked rows during cleanup. Replay batch halts after `recordConflict`, commit-build error, or `update-ref` CAS failure — later events do NOT leapfrog a broken predecessor.
- **AI diff text is built from captured `before_oid`/`after_oid` blobs** (`internal/daemon/message.go::BuildOpsDiff`), capped via `ai.Truncate(DiffCap)`. Survives live worktree changes after capture. `CommitContext` carries `DiffText`, `RepoRoot`, `Branch`, `Now`, `MultiOp`.

## Known issues / flaky tests

- **Flaky in `internal/daemon`** (pass in isolation, fail under load): `TestRun_FsnotifyDrivesWake`, `TestRun_LifecycleHappyPath`, `TestRun_WakeBurstCoalesced`, `TestFsnotify_BudgetExceededFallsBackToPoll` (integration).
- **Real bug**: `TestAI_DeterministicDefault` — deterministic fallback returns AI-flavored subjects ("Add deterministic text fixture") instead of plain `"Add deterministic.txt"`. Reproduces in isolation.

## Gotchas

- **`modernc.org/sqlite`** drives the DB without cgo. Pinned at `v1.36.0` to keep the `go 1.22` directive (newer sqlite needs go ≥ 1.23). Platform breakage = §17.1 risk, STOP and surface options.
- **Symlinks**: always captured as mode `120000`. Never descend into symlinked directories. Fixture: `TestCapture_SymlinkToDirAsMode120000`.
- **Sensitive globs**: empty `ACD_SENSITIVE_GLOBS` falls back to defaults. Never let a typo open the gate.

## Harness adapter gotchas

- **Codex hooks** (`templates/codex/config.snippet.toml`) need 3-level schema: `[features] codex_hooks = true`, then `[[hooks.<EventName>]]` wrapping `[[hooks.<EventName>.hooks]]` (handler with `type = "command"` + `command`). Flat `[[hooks]]` arrays do NOT work.
- **Codex hook stdout must be valid JSON.** Snippet redirects `acd` output to `/dev/null` and emits `printf "{}\n"`.
- **No `Stop` hook in the Codex snippet** — races the replay drain. Cleanup via `watch_pid` death + refcount sweep instead.
- **Codex auto-loads both `~/.codex/hooks.json` and `~/.codex/config.toml`.** Delete the old hooks.json after installing the toml snippet.

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
