# Agent guide

## Basics

- Static Go CLI/daemon, MIT, macOS/Linux `arm64`/`amd64`; no Windows v1.
- Module `github.com/KristjanPikhof/Auto-Commit-Daemon`; Go 1.22; `modernc.org/sqlite v1.36.0`.
- Tags are `vYYYY-MM-DD`. As of 2026-05-10, `v2026-05-10` is published at `4d9dfef`; `main` may be newer. Do not move a published tag unless requested.
- `AGENTS.md -> CLAUDE.md`; edit `CLAUDE.md`, preserve the symlink.

```bash
make build      # static bin/acd, CGO_ENABLED=0, -tags=netgo,osusergo
make test       # go test ./... -race -count=1
make lint       # go vet ./... + gofmt check
make fmt        # gofmt -w .
make tidy       # go mod tidy
```

Pre-PR gate:

```bash
cleanenv() { env -u ACD_INTENT_MIN_PENDING -u ACD_INTENT_MAX_PENDING_AGE -u ACD_INTENT_WINDOW -u ACD_INTENT_RECENT_COMMITS -u ACD_INTENT_DEFER_LIMIT ACD_COMMIT_STRATEGY=event "$@"; }
cleanenv make lint
cleanenv make test
cleanenv go test ./test/integration/... -tags=integration -race -count=1 -timeout 5m
cleanenv go test ./internal/daemon/... ./internal/git/... ./internal/state/... ./internal/pause/... ./internal/cli/... -race -count=3 -timeout 10m
```

Release smoke, only after approval for install/tag:

```bash
make build && install -m 0755 ./bin/acd ~/.local/bin/acd
git tag v2026-MM-DD <commit> && git push origin v2026-MM-DD
gh run list --workflow release.yml --branch v2026-MM-DD --limit 1
gh run watch <run-id>
gh release view v2026-MM-DD --json isDraft,isPrerelease,isLatest,url,assets
ACD_VERSION=v2026-MM-DD sh scripts/install.sh
```

- `make build` uses `git describe`; before tagging it can show the prior tag plus commit count.
- Release workflow: GoReleaser `--skip=homebrew`, then 6 install-smoke retries for GitHub asset propagation. If smoke hits asset 404 after publish, verify release view, checksums/downloads, and local `ACD_VERSION=<tag> sh scripts/install.sh` before retagging. Reruns can fail because the release exists.
- `.goreleaser.yaml` sets `prerelease: false`; date tags otherwise break `releases/latest`. Brew stays skipped until tap credentials exist.

## Map

| Path | Purpose |
|---|---|
| `cmd/acd/main.go` | CLI entrypoint |
| `internal/cli` | Cobra commands, setup/hookhelper/status/diagnose/doctor/recover/commit-all/start cache |
| `internal/daemon` | run loop, capture/replay, intent, branch tokens, shadow/bootstrap, fsnotify, refcount, live-index repair, trace |
| `internal/state` | SQLite v7: events/ops, `decision_records`, `planner_state`, shadow/meta/clients/flush/safe-ignore/sensitive |
| `internal/git` | bounded refs/tree/diff/blob/scratch-index/history/ignore helpers |
| `internal/ai` | deterministic/openai-compat/subprocess providers; message + intent-planner contracts |
| `internal/adapter` | harness detection and markers; do not restore TODO stubs |
| `internal/{central,identity,logger,paths,pause,trace}` | registry/stats, fingerprints, logs, XDG, pause, trace |
| `templates/{claude-code,codex,opencode,pi,shell}` | harness snippets; Codex `hooks.json`; legacy TOML deleted |
| `test/integration` | build-tagged lifecycle/adapter/recovery/AI/explainable/self-heal/intent/latency-budget |
| `.github/workflows/{ci,codeql,release}.yml` | CI, CodeQL, tag release |
| `README.md`, `docs/*`, `CHANGELOG.md` | user docs; nested fences use `~~~` |

## Workflow

- Scope changes; prefer `rg`; never revert unrelated work.
- Docs are release contract. Update README/changelog/docs only when stale; release notes describe user impact.
- After `git.Init`/`git init` in tests: `git symbolic-ref HEAD refs/heads/main`.
- Stubs compile: `package <name>` plus `// TODO(phase N): <intent>`.
- Races, panics, nil pointers, ordering failures, CI flakes, and read-only path migrations are bugs. Inspect before retry.
- Timing: focused `-count=10`; ordering hazards `GOMAXPROCS=1 -count=50`.
- Broad-run-sensitive: `TestRun_FsnotifyDrivesWake`, `TestRun_LifecycleHappyPath`, `TestRun_WakeBurstCoalesced`, `TestRun_RealSIGUSR1`, repeated edits, external FF reseed, FF-in-grace self-heal.
- HEAD-transition: `waitForMetaValue(MetaKeyBranchHead, <sha>, 3s)`.
- CLI changes need Cobra help/examples; template changes need `internal/cli/setup_test.go` + AdapterE2E.
- Self-hosting: ACD may auto-commit this repo. Before destructive surgery: `acd pause --repo . --reason "..." --yes`; after: `acd resume --repo . --yes`.
- Intent-env failures: rerun with `cleanenv`; verify suspected main flakes on `main`.

## State, Branch, Capture

- Repo DB `<gitDir>/acd/state.db`; central registry/stats use XDG state/share.
- Start cache: per-session `<gitDir>/acd/start-cache-<sha256(session_id)[:16]>.json`, schema v2. Lock-free read at top of `runStart`; written after cold path; `acd stop` removes matching/all caches. Atomic tmp+rename prevents corruption.
- `SchemaVersion = 7`: v5 `decision_records`; v6 `decision_records.event_seq`; v7 `planner_state`.
- `shadow_paths` key `(branch_ref, branch_generation, path)`; read-heavy paths use `state.DB.ReadSQL()`.
- Shadow bootstrap: 5000-row chunks; marker `shadow.bootstrapped:<branch_ref>:<generation>` only after all chunks commit; clean partial rows on failure.
- Reseed prunes old generations via `ACD_SHADOW_RETENTION_GENERATIONS` (default 1). Empty active shadow with marker means delete marker and re-bootstrap.
- Branch tokens: attached `rev:<sha> <branch-ref>`; detached `rev:<sha>`; missing `missing <branch-ref>`. FF keeps generation; reset/rebase/switch/same-SHA ref switch bumps; legacy bare rev upgraded to attached forces Diverged.
- Detached HEAD pauses capture/replay and `acd start` refuses. Never fall back to `refs/heads/main` when `git symbolic-ref` fails.
- Git-op markers pause capture/replay: `rebase-merge`, `rebase-apply`, `MERGE_HEAD`, `CHERRY_PICK_HEAD`, `BISECT_LOG`. Non-`ErrNotExist` stat errors fail open.
- Same-branch rewinds set `daemon_meta.replay.paused_until = now + ACD_REWIND_GRACE_SECONDS`; `0` disables. Manual `<gitDir>/acd/paused` wins.
- Diverged drops stale `pending` for prior generation and keeps `published`. Dead prior refs trigger `state.PurgeUnpublishedForDeadBranch`: prune `pending` + terminal `blocked_conflict`/`failed`, lift `publish_state` only for deleted blocked row in that pair, clear breadcrumbs. Live refs preserve rows/blockers.
- Dead-branch startup sweep runs after running-mode publish, uses `git.LiveBranchSet`, honors manual pause and `ACD_KEEP_DEAD_BRANCH_BARRIERS=1`, stamps `dead_branch_prune.{last_run_ts,last_count,last_refs}`, surfaces in `acd diagnose --json`. SQLite pause-read errors fail closed.
- Capture compares live worktree to shadow; stale/missing bootstrap creates phantom creates.
- `walkLive` BFSes by directory; ignore checks batched (`ignoreCheckBatchSize=1000`); ignored/sensitive/safe-ignore dirs pruned before readdir.
- Never prune worktree-rooted `acd/`; `.git/acd` is daemon state. Symlink mode is `120000`; never descend.
- Empty `ACD_SENSITIVE_GLOBS` keeps defaults; typos must not disable defaults. Safe-ignore prunes descendants, not same-named files. `ACD_SAFE_IGNORE=0|false|no|off` disables; `ACD_SAFE_IGNORE_EXTRA=dist/,build/` appends. Restart daemon for env changes.
- `IgnoreChecker.Check`: long-lived `git check-ignore --stdin -z --non-matching --verbose`; stream stdin while reading stdout. One large `stdin.Write` deadlocks on macOS 16 KiB pipes. `Close` is non-blocking cancel + `killLocked` + bounded `cmd.Wait` 2s.

## Replay and Intent

- Default `ACD_COMMIT_STRATEGY=event`: one capture per commit; must not call planner.
- `ACD_COMMIT_STRATEGY=intent`: offers pending captures to AI planner; capture durability unchanged.
- Intent defaults: `ACD_INTENT_WINDOW=10`, `ACD_INTENT_MIN_PENDING=10`, `ACD_INTENT_MAX_PENDING_AGE=5m`, `ACD_INTENT_RECENT_COMMITS=5`, `ACD_INTENT_DEFER_LIMIT=2`.
- Planner selects one capture or any non-empty subset; every offered seq must be selected/deferred. Invalid/missing/unsafe output records `intent_planner_error` and falls back to deterministic one-item.
- Deferred stays pending in `planner_state`; at `defer_count >= ACD_INTENT_DEFER_LIMIT`, oldest overdue is forced one-item.
- Grouped publish marks selected events `published` with same `commit_oid`; ledger records grouped seqs, deferrals, forced aging, planner errors.
- Per-pass scratch index `<gitDir>/acd/replay-*.index` is seeded from `cctx.BaseHead`; reads via `git.LsFilesIndex(...)`.
- CAS targets literal `HEAD` via `git.UpdateRef`; named refs use `--no-deref`.
- `DefaultReplayLimit = 64`; query `Limit+1`, trim, set `ReplaySummary.HasMore`. `DefaultReplayPerEventTimeout = 60s`; timeout/cancel marks event `failed` and stops batch.
- `blocked_conflict`/`failed` are terminal seq barriers; `PendingEvents` hides later pending behind prior terminal rows for same branch/generation.
- Idempotent publish checks current `HEAD` before before-state blocking; matching desired blob/mode/absence means publish with `commit_oid=HEAD`.
- `superseded_external` requires bounded history proof, parent/base tree match, live worktree before-state match. Incomplete proof means no supersede.
- Live-index reconciliation is guarded/path-scoped and must not overwrite user-staged changes. See `internal/git/tree.go`, `internal/daemon/replay.go`, `internal/daemon/live_index_repair.go`.

## Run Loop and Observability

- `processBranchTokenChange` runs before capture and after flush drain; do not collapse. Post-flush recheck handles git surgery outside `wakeCh`.
- Branch settle `100ms`; flush drain `DefaultFlushLimit = 256`.
- Daemon stamps `commit.strategy`, `intent.{window,min_pending,max_pending_age,recent_commits,defer_limit,diff_egress}`; CLI reads meta before env.
- Startup sweeps `acknowledged` flush requests older than `OrphanFlushAckThreshold = 5m` to `failed`.
- fsnotify dispatch must not block: runtime creates use `rewalkCh`/`rewalkWorker`; diagnostics `diagCh`; tail clamps `MaxDebounceTail = 500ms`; ENOSPC -> `errBudgetExceeded`.
- Daemon log: `paths.Roots.RepoLogPath(repoHash)` (`~/.local/state/acd/<repo-hash>/daemon.log`) with rotation/compression. Hook log: `${XDG_STATE_HOME:-$HOME/.local/state}/acd/<harness>-hook.log`; doctor tails Codex logs.
- `acd logs --follow` streams from EOF reached by initial tail read; do not re-`Stat`. `acd list --watch` rejects `--json`. `acd events --watch` without `--since` starts at current ledger tail; with `--since` resumes after cursor.
- Probes: `acd status --repo .`; `acd events --watch`; `acd logs --repo . --lines 50 --follow`; `acd diagnose --repo . --json`; `acd doctor --repo . --json`; `git status --short --ignored`.

## CLI, Git, AI

- `events`/`explain`/`doctor` read paths must not call `state.Open` or migrate DBs; use read-only SQLite. Missing decision tables means empty summaries, clear text, valid JSON, no table creation.
- Status JSON: `decision_counts`, `recent_decisions`, `decision_cursor`, `failed_events`, `failed_blocking_pending`, `intent_strategy`; `explain --since` summarizes newest post-cursor decision.
- `acd doctor` detects drift when active hooks lack both `acd start` and `acd wake`, falls back on EACCES/EIO, tails Codex hook log, and surfaces error count + first line.
- `acd setup <harness> --raw` emits only snippet body. Default keeps comment-wrapped instructions/README. Shell `--raw` writes a `\n` separator between direnv and zshrc snippets.
- `acd commit-all`: one-shot capture+replay without persistent daemon. Refuses on detached HEAD, git-op markers, manual pause marker, running per-repo daemon. Force-reseeds shadow from HEAD; drops stale pending for active `(branch_ref, generation)`.
- `acd start` short-circuit: per-session start-cache + central registry; same `session_id` + fresh heartbeat + daemon fingerprint skips control.lock, SQLite migration, registry rewrite. Hot path ~50ms. Adapter PPID `kill(0)` probe near `RegisterClient`; ESRCH logs and continues.
- Registry-backed early short-circuit (`tryRegistryBackedShortCircuitStart`) runs before `git.ResolveWorktree`: matches `$PWD`-within-canonical-root, filters non-canonical/dead rows via `registryRecordLooksCanonical`, derives `gitDir` from `state_db`, cheap-checks attached HEAD via `.git/HEAD`, then evaluates start-cache. Skips Git rev-parse entirely on warm path. Cold path in `runStart` always uses `git.ResolveWorktree`.
- Path lookups canonicalize via `git.ResolveWorktree` (rev-parse `--show-toplevel` + `--absolute-git-dir`, EvalSymlinks both). `ErrNotWorktree` refuses non-Git paths. `lookupRegisteredRepo` is the shared read-only registry helper for `status`/`diagnose`/`events`/`logs`/`prompt`/`recover`; `daemon`/`start` use `ResolveWorktree` directly. Existing short-circuit prefers exact path match before `RepoHash` fallback so stale duplicates can't shadow canonical rows.
- Manual `acd start` without `--session-id` registers `human:<repoHash>`. Harness paths pass `--session-id`, `--harness`, usually `--watch-pid`. Subdir invocation refreshes the same canonical root row instead of creating a duplicate.
- `central.Registry.UpsertRepo` matches by canonical `state_db` or path; `CleanupLegacyDuplicates` (parallelized, `min(GOMAXPROCS,8)` workers under `WithLock`) merges rows sharing git-toplevel or state_db. `acd gc` runs cleanup before pruning and reports merges via `merged []LegacyDuplicateChange` in JSON.
- `internal/git`: `RunOpts.Timeout`, `RunWithLimit`, `ErrStdoutOverflow`, `DefaultReadTimeout=30s`, `DefaultWriteTimeout=60s`, `git.DefaultDiffCap` (1 MiB). Ambiguous `RevParse` means `git.ErrRefAmbiguous`.
- Pinned `ps`: `/bin/ps` Darwin, `/usr/bin/ps` Linux. Do not use `$PATH`. `isSQLiteLocked` must unwrap `*sqlite.Error` and compare typed code before substring fallback.
- `acd hook-stdin-extract <field> [field...]`: rejects CR/LF/NUL; empty required fields are field-not-found; required outputs flush after every required field resolves; 1 MiB stdin truncation is distinct. Optional fields use `?`.
- AI providers declare `NeedsDiff`; network providers receive redacted diffs only when `NeedsDiff=true` and `ACD_AI_DIFF_EGRESS` truthy. `DeterministicProvider` has `NeedsDiff=false`.
- `BuildOpsDiff` caps rendered text at `ai.DiffCap`; per-op git diff uses `2 * ai.DiffCap` and 5s timeout. Redact/truncate before provider send.
- `ACD_AI_SEND_DIFF` is removed; if set, emits one startup deprecation warning.
- `ACD_TRACE=1` writes best-effort JSONL `<gitDir>/acd/trace/YYYY-MM-DD.jsonl`; `ACD_TRACE_DIR` overrides. Never block/abort. Verify classes with `rg -n "EventClass:" internal/`.

## Recovery

```bash
acd diagnose --repo . --json
acd recover --repo . --auto --dry-run --json
acd recover --repo . --auto --yes
acd wake --repo . --session-id <session>
acd status --repo .
```

- `acd recover --auto` refuses while daemon PID lives; creates `.git/acd/state.db.recover-<timestamp>`, retargets pending/blocked rows, resets blocked, clears replay/pause meta, removes manual pause marker.
- `acd resume --yes` lifts only manual pause.
- Last-resort: pause/resume plus `sqlite3 .git/acd/state.db "DELETE FROM capture_events WHERE state='blocked_conflict';"`.

## Harness/Templates

| Harness | Start | Active hooks | End | Notes |
|---|---|---|---|---|
| Claude Code | `SessionStart -> acd start` fail-soft | `Pre/PostToolUse`: and-chain + log fallback | `Stop -> acd touch`; `SessionEnd -> acd stop --session-id` | `CLAUDE_PROJECT_DIR:-$PWD`; nested JSON |
| Codex | `SessionStart -> acd start` | `UserPromptSubmit`/`PreToolUse`/`PostToolUse`; matcher `apply_patch\|Edit\|Write\|Bash` on `PreToolUse` and `PostToolUse`; mkdir gated | `Stop -> acd touch` | `templates/codex/hooks.json`; active timeout 15s |
| OpenCode | `session.created -> acd start` | `tool.before.*`/`tool.after.*`: and-chain + log fallback | `session.idle -> acd touch`; `session.deleted -> acd stop --session-id` | `OPENCODE_SESSION_ID`/`OPENCODE_PROJECT_DIR`; `~/.config/opencode/hook/hooks.yaml` |
| Pi | `session.created -> acd start` | `tool.before.*`/`tool.after.*`: and-chain + log fallback | `session.idle -> acd touch`; `session.deleted -> acd stop --session-id` | `SID="${PI_SESSION_ID:-pi-$$-$(date +%s)}"`; no `uuidgen`; `~/.pi/agent/hook/hooks.yaml` |

Active-hook body:

```bash
LOG="${XDG_STATE_HOME:-$HOME/.local/state}/acd/<harness>-hook.log"
[ -d "$(dirname "$LOG")" ] || mkdir -p "$(dirname "$LOG")" 2>/dev/null || true
{ acd start --... && acd wake --... ; } 2>>"$LOG" || { printf '[%s] active hook failed exit=%d cmd=acd-start-wake\n' "$(date +%FT%T%z)" "$?" >>"$LOG"; exit 1; }
```

- `acd start` failure is no longer masked by wake; active hook exits nonzero. AdapterE2E covers stop-all self-heal and corrupt-DB negatives.
- Existing-user migration: re-run `acd setup <harness>` and replace installed hooks block. `acd doctor` flags drift.
- Markers are format-specific: TOML/YAML use leading `# acd-managed: true`; JSON detects `"_acd_managed": true` with/without space. Keep hookhelper, setup tests, templates, AdapterE2E in sync.
- Codex: `~/.codex/hooks.json` wins over `~/.codex/config.toml`; legacy TOML deleted; `_acd_managed: true` is top-level JSON; `acd doctor` warns when both Codex files carry acd markers because events double.
- Codex `/hooks` re-approval is required after every `~/.codex/hooks.json` change; until approved, `SessionStart` never fires. `acd setup codex --raw > ~/.codex/hooks.json` destroys non-acd entries; custom-hook users must merge manually.
- Codex hooks need the Codex feature flag in `~/.codex/config.toml`; official docs currently show `[features].codex_hooks = true`. `hooks.json` carries hook bodies, not feature flags.
- Codex `cwd` comes from stdin via `acd hook-stdin-extract session_id cwd? <&0`; missing `cwd` falls back to `$PWD`. `CODEX_PROJECT_DIR`/`printf "{}\n"` are gone. Bash bodies use `|| exit 0` after helper so missing `acd` does not block hook.

## Env

| Group | Vars |
|---|---|
| Trace | `ACD_TRACE`; `ACD_TRACE_DIR` default `<gitDir>/acd/trace` |
| Shadow/rewind | `ACD_SHADOW_RETENTION_GENERATIONS=1`; `ACD_REWIND_GRACE_SECONDS=60` (`0` disables); `ACD_KEEP_DEAD_BRANCH_BARRIERS` disables auto-prune |
| Capture | `ACD_SENSITIVE_GLOBS`; `ACD_SAFE_IGNORE`; `ACD_SAFE_IGNORE_EXTRA`; `ACD_MAX_PENDING_EVENTS` |
| AI | `ACD_AI_PROVIDER=deterministic|openai-compat|subprocess:<name>`; `ACD_AI_BASE_URL`; `ACD_AI_API_KEY`; `ACD_AI_MODEL`; `ACD_AI_TIMEOUT=30s`; `ACD_AI_CA_FILE`; `ACD_AI_DIFF_EGRESS` |
| Strategy | `ACD_COMMIT_STRATEGY=event|intent`; `ACD_INTENT_WINDOW=10`; `ACD_INTENT_MIN_PENDING=10`; `ACD_INTENT_MAX_PENDING_AGE=5m`; `ACD_INTENT_RECENT_COMMITS=5`; `ACD_INTENT_DEFER_LIMIT=2` |
| Watcher/client | `ACD_FSNOTIFY_ENABLED`; `ACD_DISABLE_FSNOTIFY`; `ACD_MAX_INOTIFY_WATCHES`; `ACD_CLIENT_TTL_SECONDS` |
