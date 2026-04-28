# CLAUDE.md — Auto-Commit-Daemon

Fast-access project brief for future Claude Code sessions. Read this first.

## Identity

- **Binary**: `acd` (single static cross-platform Go binary)
- **Module**: `github.com/KristjanPikhof/Auto-Commit-Daemon`
- **License**: MIT
- **Versioning**: date-based, `vYYYY-MM-DD` (first tag: `v2026-04-28`)
- **Platforms**: macOS (arm64, amd64), Linux (arm64, amd64). No Windows in v1.
- **Mission**: greenfield Go reimplementation of the Python `atomic-commit-worktree-daemon`. Watches a git worktree, captures every meaningful change, lands atomic commits per file. Multi-harness (Claude Code, Codex, OpenCode, Pi, shell).

## Source of truth

- **Plan**: `.plan/acd.md` (gitignored). 2114 lines. Single canonical spec — sections referenced as `§N` throughout the codebase.
- **Legacy reference**: `.plan/examples/atomic-commit-worktree-daemon/` (gitignored). Frozen Python implementation. Read-only museum exhibit; do not modify or attempt to run alongside `acd`.
- `.plan/` is in `.gitignore` — all plan/legacy material stays out of git history by design.

## Current state

- **Phase 0–6**: DONE (epic `7c06148c-cbc3-4d86-91a5-f952693c276d`, merged via PR #1).
- **First release published**: tag `v2026-04-28` on `main`; goreleaser workflow uploads 4 archives + `checksums.txt` (darwin/linux × amd64/arm64). Brew formula generation gated behind `--skip=homebrew` until tap repo + secrets exist.
- **Open Phase 6 work** (user-side, not codable here):
  - `KristjanPikhof/homebrew-tap` repo + `HOMEBREW_TAP_TOKEN` / `GH_PAT` secrets, then drop `--skip=homebrew` in `.github/workflows/release.yml`
  - Re-verify `install.sh` / `brew install acd` / `go install …@latest` on fresh macOS arm64 + Ubuntu 22.04 amd64
  - §1.4 success-criteria walkthrough on a fresh OS

Use Trekoon as the live progress source: `trekoon --toon session --epic 7c06148c-cbc3-4d86-91a5-f952693c276d`. Do not duplicate task tracking elsewhere.

## Locked decisions (do not renegotiate in v1)

| ID  | Decision                                                                  |
|-----|---------------------------------------------------------------------------|
| D1  | Go 1.22+                                                                  |
| D11 | fsnotify hybrid + poll fallback                                           |
| D13 | Single binary; `acd daemon run` is the long-running entry                 |
| D16 | SQLite driver: `modernc.org/sqlite` (pure Go, zero cgo)                   |
| D17 | Git: subprocess to system `git`. **Never use `go-git`.**                  |
| D18 | AI plugins: subprocess, JSON-over-stdin/stdout                            |
| D19 | Logging: `log/slog` JSONL, rotated by size+age                            |
| D20 | Heartbeat liveness primary; PID liveness fast-path when available         |
| D21 | Default heartbeat TTL: 30 minutes (env `ACD_CLIENT_TTL_SECONDS`)          |

Full list in `.plan/acd.md §2` (D1–D21).

## Repo layout (canonical paths)

```
cmd/acd/main.go                       — entry; calls internal/cli.Execute
internal/cli/                         — cobra root + subcommand stubs (Phase 0); replace stubRun in later phases
internal/{daemon,state,central,adapter,git,ai,identity,logger,paths}/
                                      — package stubs; each file has TODO(phase N) marker
internal/version/version.go           — ldflags-injected Version + GitSHA
templates/embed.go                    — package templates exposes embed.FS for adapter snippets
templates/{claude-code,codex,opencode,pi,shell}/  — drop-in snippets + READMEs + uninstall docs
scripts/install.sh, scripts/uninstall.sh, scripts/dev/*  — release + dev tooling
.github/workflows/{ci,release,codeql}.yml
.goreleaser.yaml                      — darwin+linux × amd64+arm64; brew tap auto-publish
Makefile                              — build/test/lint/release-snapshot
.plan/acd.md                          — spec (gitignored)
.plan/examples/atomic-commit-worktree-daemon/  — legacy Python (gitignored, read-only)
```

State lives **inside** `.git/`:
- Per-repo state DB: `<repo>/.git/acd/state.db` (greenfield, schema in `§6.1`)
- Per-repo locks: `<repo>/.git/acd/{daemon,control}.lock`
- Per-repo logs: `~/.local/state/acd/<repo-hash>/daemon.log`

Central state at `~/.local/share/acd/`:
- `registry.json` (atomic write + flock)
- `stats.db` (append-only schema only — `ALTER TABLE ADD COLUMN` exclusively)

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

- **Commits**: atomic-commit hook is configured globally; every Edit/Write tool call auto-commits the touched file. Expect dozens of commits per scaffolding pass. Do not amend. If `go.mod`/`go.sum` shows up untracked (because `go get` did not go through the tool), commit explicitly.
- **Stub format**: `package <name>` + `// TODO(phase N): <intent>`. Stubs must compile (no unused imports).
- **Plan references**: cite section numbers (`§7.2`, `§8.5`) so readers can find the spec quickly.
- **Markdown nested code**: README + adapter docs use `~~~` fences when nesting code blocks inside other code blocks.
- **Embed**: `templates/embed.go` is the single embed point. Add new harness directories alongside existing ones and extend the `//go:embed` line.
- **Caveman mode** is active in this user's environment for narration; code/commits/PRs/docs stay in normal English.

## Gotchas

- **`.plan/` is gitignored** — anything in there will never reach git. Do not put runtime artifacts there.
- **`modernc.org/sqlite`** drives the DB without cgo. Phase 1 will reintroduce it (Phase 0 trimmed it via `go mod tidy`). If a target platform breaks, that is a §17.1 risk → STOP and surface options.
- **Symlinks**: always captured as mode `120000`. Never descend into a symlinked directory. The legacy daemon shipped a regression here; the Go port must repeat the fix verbatim.
- **Sensitive globs**: empty `ACD_SENSITIVE_GLOBS` falls back to defaults (security: never let a typo open the gate).
- **Branch-generation token**: format `rev:<sha>` for an existing ref, `missing` otherwise. Same generation = fast-forward; bumped generation = force-push/reset.
- **Trekoon compact-spec pipes**: literal `|` inside a description must be `\|`. Only `\|`, `\\`, `\n`, `\r`, `\t` are valid escapes.
- **Phase 0 exit contract**: `make build` green, `make lint` clean, `make test` clean, `acd version` prints, `acd` no-args exits 1 with `acd: no command provided`. Any deviation = STOP.

## Stop conditions (escalate to user)

Per plan §"STOP CONDITIONS":
1. A locked decision (D1–D21) does not match implementation reality.
2. A regression test from the legacy daemon fails to port without behaviour change.
3. A risk in §17.1 actually fires.
4. The spec contradicts itself.
5. You are 30+ minutes deep on one bug with no progress.

In every stop condition: paste the failing command + last 50 lines of relevant output, propose 2–3 options, ask which path.

## Useful one-liners

```bash
# Trekoon orient
trekoon --toon session --epic 7c06148c-cbc3-4d86-91a5-f952693c276d

# Trekoon next ready
trekoon --toon task ready --epic 7c06148c-cbc3-4d86-91a5-f952693c276d --limit 5

# Confirm Phase 0 binary still works
make build && ./bin/acd version

# Inspect git auto-commit history
git log --oneline | head -30
```
