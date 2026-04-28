# Agents Guide

## Identity

- **Binary**: `acd` (single static cross-platform Go binary)
- **Module**: `github.com/KristjanPikhof/Auto-Commit-Daemon`
- **License**: MIT
- **Versioning**: date-based, `vYYYY-MM-DD` (first tag: `v2026-04-28`)
- **Platforms**: macOS (arm64, amd64), Linux (arm64, amd64). No Windows in v1.

## Build / test / verify

```bash
make build          # CGO_ENABLED=0, -tags=netgo,osusergo, ldflags-injected version
make test           # go test ./... -race -count=1
make lint           # go vet + gofmt -l (must be empty)
make release-snapshot   # goreleaser local check
./bin/acd version
./bin/acd           # no args → help + "acd: no command provided", exit 1

# Integration tests (build-tagged)
go test ./test/integration/... -tags=integration -race -count=1 -timeout 5m
```

`internal/cli/stubs.go` only holds the unimplemented commands (post-Phase 6 there are essentially none — `init`, `start`, `stop`, `wake`, `touch`, `daemon run`, `list`, `status`, `stats`, `gc`, `doctor`, `version` are all wired).

## Install paths (dev + prod)

| Path | When to use |
|------|-------------|
| `make build && install -m 0755 ./bin/acd ~/.local/bin/acd` | Local iteration. Templates baked at build time, so any `templates/*` edit needs a rebuild before `acd init <harness>` reflects it. |
| `go install ./cmd/acd` | Quickest dev install. Version string will be `dev (unknown)` — no ldflags injection. |
| `GOPROXY=direct go install github.com/KristjanPikhof/Auto-Commit-Daemon/cmd/acd@<branch-or-sha>` | Test a branch from another machine. `direct` bypasses `proxy.golang.org` (which caches stale pseudo-versions for non-semver tags). |
| `curl -fsSL …/scripts/install.sh \| sh` | Production install from the latest non-prerelease GitHub release. Requires `acd_<VERSION_NUM>_<os>_<arch>.tar.gz` + `checksums.txt` to exist. |

## Conventions

- **Stub format**: `package <name>` + `// TODO(phase N): <intent>`. Stubs must compile (no unused imports).
- **Markdown nested code**: README + adapter docs use `~~~` fences when nesting code blocks inside other code blocks.
- **Embed**: `templates/embed.go` is the single embed point. Add new harness directories alongside existing ones and extend the `//go:embed` line.

## Release & install gotchas

  - Goreleaser's `release.prerelease: auto` marks date tags as pre-release → `releases/latest` API returns nothing → `install.sh` can't resolve a version. `.goreleaser.yaml` now hardcodes `prerelease: false`. Existing pre-release releases must be flipped manually: `gh release edit <tag> --prerelease=false --latest`.
- **Release workflow auth**: `.github/workflows/release.yml` uses `secrets.GITHUB_TOKEN` (built-in, auto-injected on every workflow run) for archive upload. The brew step needs `HOMEBREW_TAP_TOKEN` (PAT with repo scope on the tap repo) and is gated behind `--skip=homebrew` until that secret + tap repo exist.
- **install.sh quirks**:
  - Tag carries leading `v` (`v2026-04-28`); goreleaser archive names omit it (`acd_2026-04-28_*`). install.sh strips the prefix into `VERSION_NUM` for both URL and `sha256sum -c` grep.
  - The downloaded archive must be saved with its **original filename** because `sha256sum -c` opens the file by the name in `checksums.txt`. Saving as a renamed `acd.tar.gz` breaks verification.
  - `curl -fsSL <raw-url> | sh` sometimes 404s on `raw.githubusercontent.com` (HTTP/2 stream reset / CDN edge variance) while a plain `curl -o file <raw-url>` succeeds. Workaround: download to a file first, then `sh /path/to/file`. `--http1.1` also tends to dodge it.
- **Templates are baked at build time** via `templates/embed.go`. Editing `templates/<harness>/*.snippet.*` does not affect an already-installed binary. Either rebuild and reinstall (`make build && install …`), or republish the release (push commits, retag, wait for workflow).
- **Test fixtures must pin branch name**: `git init` honors host's `init.defaultBranch` (CI runners default to `master`). Tests pin `BranchRef = "refs/heads/main"`, so any new fixture that calls `git.Init` must follow with `git symbolic-ref HEAD refs/heads/main`. Existing fixtures that need this: `internal/daemon/{capture_test,daemon_test}.go::*Fixture` and `test/integration/helpers_test.go::gitInit`.

## Harness adapter gotchas

- **Codex hooks (`templates/codex/config.snippet.toml`)** must follow Codex's 3-level schema: `[features] codex_hooks = true`, then `[[hooks.<EventName>]]` (matcher group) wrapping `[[hooks.<EventName>.hooks]]` (handler with `type = "command"` + `command`). Flat `[[hooks]]` arrays with `event = "..."` fields are **not** valid and surface as `invalid type: map, expected a sequence in 'hooks'`.
- **Codex hook stdout must be valid JSON.** Codex parses each hook's stdout against its own schema (most strict on `Stop`). `acd start|wake|stop|touch` print their own `--json` shapes which Codex rejects. The snippet redirects `acd` output to `/dev/null` and emits `printf "{}\n"` on stdout. Apply this pattern to any new hook command.
- **No `Stop` hook in the Codex snippet.** Codex fires `Stop` immediately after the last `PostToolUse`, racing the daemon's capture+replay drain. Cleanup happens via `watch_pid` death + refcount sweep instead — Codex's PID dies when it exits, the daemon sweep notices via `kill(pid, 0)`, drops the client row, then self-terminates after `BootGrace` + 2 empty sweeps (~30–60s). The integration test for codex calls `acd stop --force` directly to avoid the wait.
- **OpenCode "database or disk is full"** comes from OpenCode's own bun-bundled SQLite (`bun:sqlite`), not from acd. `acd` writes to `<repo>/.git/acd/state.db`; OpenCode writes to its own state dir. If you see this, check `df -h ~` and OpenCode's state dir — it is unrelated to the hooks YAML.
- **Codex auto-loads both `~/.codex/hooks.json` and `~/.codex/config.toml`.** Codex prints a warning to use one. After installing the toml snippet, delete or rename any pre-existing `hooks.json` (`mv ~/.codex/hooks.json ~/.codex/hooks.json.bak`).

## Gotchas

- **`modernc.org/sqlite`** drives the DB without cgo. If a target platform breaks, that is a §17.1 risk → STOP and surface options. Pinned at `v1.36.0` to keep the `go 1.22` directive (newer sqlite needs go ≥ 1.23).
- **Symlinks**: always captured as mode `120000`. Never descend into a symlinked directory. The legacy daemon shipped a regression here; the Go port repeats the fix verbatim — fixture covers it (`TestCapture_SymlinkToDirAsMode120000`).
- **Sensitive globs**: empty `ACD_SENSITIVE_GLOBS` falls back to defaults (security: never let a typo open the gate).
- **Branch-generation token**: format `rev:<sha>` for an existing ref, `missing` otherwise. Same generation = fast-forward; bumped generation = force-push/reset.

## Useful one-liners

```bash

# Confirm binary still works
make build && ./bin/acd version

# Refresh local install with newest source (needed after any templates/* edit)
make build && install -m 0755 ./bin/acd ~/.local/bin/acd

# Inspect git auto-commit history
git log --oneline | head -30

# Cut a new release (assumes secrets + workflow already healthy)
git tag v2026-MM-DD && git push origin v2026-MM-DD && gh run watch

# Re-tag the same date (delete + recreate; overwrites release artifacts)
git tag -d v2026-04-28 && git push origin :v2026-04-28
git tag v2026-04-28 && git push origin v2026-04-28

# Fix a release that goreleaser auto-marked as pre-release
gh release edit v2026-04-28 --prerelease=false --latest

# Smoke test install.sh end-to-end (env override skips the releases/latest API)
ACD_VERSION=v2026-04-28 sh scripts/install.sh
```
