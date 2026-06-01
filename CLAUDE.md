# Agent guide

## Project

- Static Go CLI/daemon for macOS/Linux `arm64`/`amd64`; no Windows v1.
- Module `github.com/KristjanPikhof/Auto-Commit-Daemon`; Go 1.22; `modernc.org/sqlite v1.36.0`; MIT.
- Tags use `vYYYY-MM-DD`. Verify latest with `git tag --sort=-creatordate | head`; local latest at this refresh was `v2026-05-29`. `v2026-05-10` exists at `4d9dfef`. Do not move published tags unless requested.
- `AGENTS.md -> CLAUDE.md`; edit `CLAUDE.md`, preserve the symlink.

## Commands

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

Release smoke only after install/tag approval:

```bash
make build && install -m 0755 ./bin/acd ~/.local/bin/acd
git tag v2026-MM-DD <commit> && git push origin v2026-MM-DD
gh run list --workflow release.yml --branch v2026-MM-DD --limit 1
gh run watch <run-id>
gh release view v2026-MM-DD --json isDraft,isPrerelease,isLatest,url,assets
ACD_VERSION=v2026-MM-DD sh scripts/install.sh
```

- `make build` uses `git describe`; before tagging it can show the prior tag plus commit count.
- Release workflow uses GoReleaser `--skip=homebrew`, then 6 install-smoke retries for asset propagation. If smoke sees asset 404 after publish, verify release view, checksums/downloads, and `ACD_VERSION=<tag> sh scripts/install.sh` before retagging. Reruns can fail because the release exists.
- `.goreleaser.yaml` sets `prerelease: false`; date tags otherwise break `releases/latest`. Brew stays skipped until tap credentials exist.

## Map

| Path | Purpose |
|---|---|
| `cmd/acd/main.go` | CLI entrypoint |
| `internal/cli` | Cobra commands: setup, hooks, status, diagnose, doctor, fix, commit-all, start cache |
| `internal/daemon` | run loop, capture/replay, intent, branch tokens, bootstrap/shadow, fsnotify, refcount, live-index repair, trace |
| `internal/state` | SQLite v7: events/ops, decisions, planner_state, shadow/meta/clients/flush/safe-ignore/sensitive |
| `internal/git` | bounded refs/tree/diff/blob/scratch-index/history/ignore helpers |
| `internal/ai` | deterministic/openai-compat/subprocess providers, commit message prompts, intent planner |
| `internal/adapter` | harness detection and markers; do not restore TODO stubs |
| `internal/{central,identity,logger,paths,pause,trace}` | registry/stats, fingerprints, logs, XDG, pause markers, trace |
| `templates/{claude-code,codex,cursor,opencode,pi,shell}` | setup snippets; Codex and Cursor use `hooks.json`; legacy TOML removed |
| `test/integration` | build-tagged lifecycle, adapter, recovery, AI, explainable, self-heal, intent, latency-budget tests |
| `.github/workflows/{ci,codeql,release}.yml` | CI, CodeQL, tag release |
| `README.md`, `docs/*`, `CHANGELOG.md` | user contract; nested fences use `~~~` |

## Workflow

- Scope changes; prefer `rg`; never revert unrelated work.
- Docs are release contract. Update README/changelog/docs only when stale; release notes describe user impact.
- After `git.Init`/`git init` in tests: `git symbolic-ref HEAD refs/heads/main`.
- Stubs compile: `package <name>` plus `// TODO(phase N): <intent>`.
- Races, panics, nil pointers, ordering failures, CI flakes, and read-only path migrations are bugs. Inspect before retry.
- Timing: focused `-count=10`; ordering hazards `GOMAXPROCS=1 -count=50`.
- Broad-run-sensitive tests: `TestRun_FsnotifyDrivesWake`, `TestRun_LifecycleHappyPath`, `TestRun_WakeBurstCoalesced`, `TestRun_RealSIGUSR1`, repeated edits, external FF reseed, FF-in-grace self-heal.
- HEAD-transition tests: `waitForMetaValue(MetaKeyBranchHead, <sha>, 3s)`.
- CLI changes need Cobra help/examples. Template changes need `internal/cli/setup_test.go` plus AdapterE2E coverage.
- Self-hosting: ACD may auto-commit this repo. Pause only for branch/history surgery, recovery/state-db mutation, or tests that intentionally exercise daemon capture/replay against this repo; after: `acd resume --repo . --yes`.
- Source edits do not affect the already-running daemon. Ordinary implementation work does not require pausing; when testing daemon or prompt behavior, `make build`, install/use the new `bin/acd`, then restart the daemon or run the intended binary directly.
- Intent-env failures: rerun with `cleanenv`; verify suspected main flakes on `main`.

Commit message format expected from AI and manual fixes:

```text
Line 1: <imperative verb> <what changed>
- max 50 characters
- no trailing period

Line 2: blank

Line 3+: bullet list for why/context
- each bullet starts with "- "
- max 72 characters per line
- wrapped continuation lines must not start with "- "
```

- Line 1 starts with an imperative verb such as Add, Fix, Refactor, Remove, Rename, Simplify, Update, or Document.
- Describe the semantic change, not just the filename. Do not mention filenames in line 1 unless the change is specifically about that file itself.
- Body explains why, intent, impact, or context; do not restate the diff.
- Intent planner grouping rationale belongs in `grouping_reason`, never in commit body.
- Avoid generic messages: `Update file`, `WIP`, `changes`.
- Code facts: `ai.SubjectCap = 50`, `ai.BodyWrap = 72`, per-event `ai.DiffCap = 4000`, planner `ai.IntentStageDiffCap = 16000`.

## State, Branch, Capture

- Repo DB: `<gitDir>/acd/state.db`; central registry/stats use XDG state/share.
- Start cache: `<gitDir>/acd/start-cache-<sha256(session_id)[:16]>.json`, schema v2. `acd stop` removes matching/all caches. Atomic tmp+rename prevents corruption.
- `SchemaVersion = 7`: v5 `decision_records`; v6 `decision_records.event_seq`; v7 `planner_state`.
- `shadow_paths` key `(branch_ref, branch_generation, path)`; read-heavy paths use `state.DB.ReadSQL()`.
- Shadow bootstrap: 5000-row chunks; marker `shadow.bootstrapped:<branch_ref>:<generation>` only after all chunks commit; clean partial rows on failure. Empty active shadow with marker means delete marker and re-bootstrap.
- Reseed prunes old generations via `ACD_SHADOW_RETENTION_GENERATIONS` default `1`.
- Branch tokens: attached `rev:<sha> <branch-ref>`; detached `rev:<sha>`; missing `missing <branch-ref>`. FF keeps generation; reset/rebase/switch/same-SHA ref switch bumps; legacy bare rev upgraded to attached forces Diverged.
- Detached HEAD pauses capture/replay and `acd start` refuses. Never fall back to `refs/heads/main` when `git symbolic-ref` fails.
- Git-op markers pause capture/replay: `rebase-merge`, `rebase-apply`, `MERGE_HEAD`, `CHERRY_PICK_HEAD`, `BISECT_LOG`. Non-`ErrNotExist` stat errors fail open.
- Same-branch rewinds set `daemon_meta.replay.paused_until = now + ACD_REWIND_GRACE_SECONDS`; `0` disables. Manual `<gitDir>/acd/paused` wins.
- Diverged drops stale `pending` for prior generation, keeps `published`, and handles dead refs through `state.PurgeUnpublishedForDeadBranch`. Live refs preserve blockers.
- Dead-branch startup sweep runs after running-mode publish, honors manual pause and `ACD_KEEP_DEAD_BRANCH_BARRIERS=1`, stamps `dead_branch_prune.{last_run_ts,last_count,last_refs}`, and surfaces in `acd diagnose --json`. SQLite pause-read errors fail closed.
- Capture compares live worktree to shadow; stale/missing bootstrap creates phantom creates.
- `walkLive` BFSes by directory; ignore checks batched (`ignoreCheckBatchSize=1000`); ignored/sensitive/safe-ignore dirs pruned before readdir.
- Never prune worktree-rooted `acd/`; `.git/acd` is daemon state. Symlink mode is `120000`; never descend.
- Empty `ACD_SENSITIVE_GLOBS` keeps defaults; typos must not disable defaults. Safe-ignore prunes descendants, not same-named files. `ACD_SAFE_IGNORE=0|false|no|off` disables; `ACD_SAFE_IGNORE_EXTRA=dist/,build/` appends. Restart daemon for env changes.
- `IgnoreChecker.Check` uses long-lived `git check-ignore --stdin -z --non-matching --verbose`; stream stdin while reading stdout. One large `stdin.Write` deadlocks on macOS 16 KiB pipes. `Close` is non-blocking cancel + kill + bounded `cmd.Wait` 2s.

## Replay and Intent

- Default `ACD_COMMIT_STRATEGY=event`: one capture per commit; must not call planner.
- `ACD_COMMIT_STRATEGY=intent`: AI planner selects one capture or a non-empty subset; capture durability unchanged.
- Intent defaults: `ACD_INTENT_WINDOW=10`, `ACD_INTENT_MIN_PENDING=10`, `ACD_INTENT_MAX_PENDING_AGE=5m`, `ACD_INTENT_RECENT_COMMITS=5`, `ACD_INTENT_DEFER_LIMIT=1`, `ACD_INTENT_PATH_COALESCE=1`.
- Planner must classify every offered seq as selected or deferred. Invalid/missing/unsafe output records `intent_planner_error` and falls back to deterministic one-item.
- `deferred_reasons[i].seq` must appear in `deferred_seqs`. Providers normalize spurious deferred reasons before validation and warn with dropped seqs.
- Deferred stays pending in `planner_state`; at `defer_count >= ACD_INTENT_DEFER_LIMIT`, oldest overdue is forced one-item.
- Same-path coalesce folds consecutive captures touching one shared path into one planner entry. Boundaries: different path, multi-path, rename, delete, or `(branch_ref, branch_generation, base_head)` change. On success every original seq is marked published with the same `commit_oid`; `acd events --json grouped_seqs` shows the run.
- Planner rejects log: `<gitDir>/acd/planner-rejects.jsonl`, 5 MiB + `.1`, best effort. Fields include `ts`, `provider`, `offered_seqs`, `code`, `message`, raw response size, sha256, and parsed-plan summary. `raw_response` is redacted by default; verbatim only with truthy `ACD_INTENT_REJECTS_RAW`, which logs a startup warning.
- `acd status --json` and `acd diagnose --json` expose `intent_strategy`, including recent planner/singleton rates over fixed denominator 100. `diagnose` hints when `planner_error_rate_recent > 0.05`.
- Per-pass scratch index `<gitDir>/acd/replay-*.index` is seeded from `cctx.BaseHead`; reads via `git.LsFilesIndex(...)`.
- CAS targets literal `HEAD` via `git.UpdateRef`; named refs use `--no-deref`.
- `DefaultReplayLimit = 64`; query `Limit+1`, trim, set `ReplaySummary.HasMore`. `DefaultReplayPerEventTimeout = 60s`; timeout/cancel marks event `failed` and stops batch.
- `blocked_conflict`/`failed` are terminal seq barriers; `PendingEvents` hides later pending behind prior terminal rows for same branch/generation.
- Idempotent publish checks current `HEAD` before before-state blocking; matching desired blob/mode/absence means publish with `commit_oid=HEAD`.
- `superseded_external` requires bounded history proof, parent/base tree match, and live worktree before-state match. Incomplete proof means no supersede.
- Live-index reconciliation is guarded/path-scoped and must not overwrite user-staged changes. See `internal/git/tree.go`, `internal/daemon/replay.go`, `internal/daemon/live_index_repair.go`.

## Run Loop and Observability

- `processBranchTokenChange` runs before capture and after flush drain; do not collapse. Post-flush recheck handles git surgery outside `wakeCh`.
- Branch settle `100ms`; flush drain `DefaultFlushLimit = 256`.
- Daemon stamps `commit.strategy`, `intent.{window,min_pending,max_pending_age,recent_commits,defer_limit,diff_egress}`; CLI reads meta before env.
- Startup sweeps `acknowledged` flush requests older than `OrphanFlushAckThreshold = 5m` to `failed`.
- fsnotify dispatch must not block: runtime creates use `rewalkCh`/`rewalkWorker`; diagnostics `diagCh`; tail clamps `MaxDebounceTail = 500ms`; ENOSPC -> `errBudgetExceeded`.
- Daemon log: `paths.Roots.RepoLogPath(repoHash)` (`~/.local/state/acd/<repo-hash>/daemon.log`) with rotation/compression. Hook log: `${XDG_STATE_HOME:-$HOME/.local/state}/acd/<harness>-hook.log`.
- `acd logs --follow` streams from EOF reached by initial tail read; do not re-`Stat`. `acd list` on a TTY defaults to compact watch (`REPO`/`DAEMON`/`PEND`/`BLK`/`HEAD`/`STATUS`; two-segment `REPO`, `#hash` on collision; compact tokens `blk`/`wait`/`miss`/`bad`; disabled rows are hidden); non-TTY and `--once` one-shot compact; `--verbose` wide table; `--json` one-shot on TTY; explicit `--watch --json` errors; `--interactive` opens the repo manager. `acd rewrite-commits` prefers explicit selectors `--from-sha`, `--from-nr`, `--range-nr`, and `--range-sha`; compatibility `--from`/`--range` remain. `--progress auto|plain|json|off` writes only to stderr, and `--quiet` suppresses progress. `acd rewrite-commits --plan-only` ends with plan-saved + `Next:` apply commands; declined apply prints `No rewrite performed.`. `acd events --watch` without `--since` starts at current ledger tail; with `--since` resumes after cursor.
- Probes: `acd status --repo .`; `acd events --watch`; `acd logs --repo . --lines 50 --follow`; `acd diagnose --repo . --json`; `acd doctor --repo . --json`; `git status --short --ignored`.

## CLI, Git, AI

- `events`/`explain`/`doctor` read paths must not call `state.Open` or migrate DBs; use read-only SQLite. Missing decision tables mean empty summaries, clear text, valid JSON, no table creation.
- Status JSON includes `decision_counts`, `recent_decisions`, `decision_cursor`, `failed_events`, `failed_blocking_pending`, `intent_strategy`. `explain --since` summarizes newest post-cursor decision.
- `acd doctor` detects drift when active hooks lack both `acd start` and `acd wake`, falls back on EACCES/EIO, tails Codex hook log, and surfaces error count plus first line.
- `acd setup <harness> --raw` emits only snippet body. Default keeps comment-wrapped instructions/README. Shell `--raw` writes a `\n` separator between direnv and zshrc snippets.
- `acd commit-all`: one-shot capture+replay without persistent daemon. Refuses on detached HEAD, git-op markers, manual pause marker, or running per-repo daemon. Force-reseeds shadow from HEAD; drops stale pending for active `(branch_ref, generation)`.
- `acd start` short-circuit: start-cache + central registry can skip control.lock, SQLite migration, registry rewrite. Manual start without `--session-id` registers `human:<repoHash>`. Harness starts require `--session-id` when `--harness` is set.
- Per-repo disabled lifecycle state lives in the central registry. `acd repo disable` stops a live daemon, clears start caches, preserves `.git/acd/state.db`, and makes hook `start`/`wake`/`touch`/`flush` skip with `repo_disabled`; manual start reports `acd repo enable --repo <path>`. `acd repo enable` clears disabled state without starting the daemon. `acd repo manage` and `acd list --interactive` share the line-oriented manager (`t/e/d N`, `r`, `v`, `q`).
- Registry-backed early short-circuit runs before `git.ResolveWorktree`; cold path always uses `git.ResolveWorktree`.
- Path lookups canonicalize via `git.ResolveWorktree` (`rev-parse --show-toplevel` + `--absolute-git-dir`, EvalSymlinks both). `ErrNotWorktree` refuses non-Git paths. `lookupRegisteredRepo` is shared by read-only commands.
- `central.Registry.UpsertRepo` matches by canonical `state_db` or path. `CleanupLegacyDuplicates` runs under lock with up to 8 workers; `acd gc` reports `merged []LegacyDuplicateChange`.
- `internal/git`: `RunOpts.Timeout`, `RunWithLimit`, `ErrStdoutOverflow`, `DefaultReadTimeout=30s`, `DefaultWriteTimeout=60s`, `git.DefaultDiffCap` 1 MiB. Ambiguous `RevParse` means `git.ErrRefAmbiguous`.
- Pinned `ps`: `/bin/ps` Darwin, `/usr/bin/ps` Linux. `isSQLiteLocked` must unwrap `*sqlite.Error` and compare typed code before substring fallback.
- `acd hook-stdin-extract <field> [field...]`: rejects CR/LF/NUL; empty required fields are field-not-found; required outputs flush after every required field resolves; 1 MiB stdin truncation is distinct. Optional fields use `?`.
- AI providers declare `NeedsDiff`; network providers receive redacted diffs only when `NeedsDiff=true` and `ACD_AI_DIFF_EGRESS` is truthy. `DeterministicProvider` has `NeedsDiff=false`.
- `BuildOpsDiff` caps rendered text at `ai.DiffCap`; per-op git diff uses `2 * ai.DiffCap` and 5s timeout. Redact/truncate before provider send.
- `ACD_AI_SEND_DIFF` is removed; if set, emits one startup deprecation warning.
- `ACD_TRACE=1` writes best-effort JSONL `<gitDir>/acd/trace/YYYY-MM-DD.jsonl`; `ACD_TRACE_DIR` overrides. It never blocks/aborts. Verify classes with `rg -n "EventClass:" internal/`.

## Recovery

```bash
acd diagnose --repo . --json
acd fix --dry-run
acd fix --yes
acd fix --force --dry-run
acd fix --force --yes
acd wake --repo . --session-id <session>
acd status --repo .
```

- `acd fix` is the recovery entrypoint. Dry-run is default without `--yes`.
- `--yes` applies safe actions: resolve already-landed barriers, retarget stale anchors, delete obsolete barriers, mark externally-published rows, clear expired manual pauses, clear drained backpressure.
- `--force` also purges terminal replay barriers with pending successors, including failed rows; combine with `--yes` to apply.
- Fixes refuse while a live daemon owns the state DB; state.db is backed up before mutation.
- `acd resume --yes` lifts only manual pause.
- `acd recover` and `acd purge-events` are deprecated and hidden; use `acd fix`.
- Last resort: pause/resume plus `sqlite3 .git/acd/state.db "DELETE FROM capture_events WHERE state='blocked_conflict';"`.

## Harness Templates

| Harness | Start | Active hooks | End | Notes |
|---|---|---|---|---|
| Claude Code | `SessionStart -> acd start` fail-soft | `Pre/PostToolUse`: start+wake and-chain + log fallback | `Stop -> acd flush --logical`; `SessionEnd -> acd stop --session-id` | `CLAUDE_PROJECT_DIR:-$PWD`; nested JSON |
| Codex | `SessionStart -> acd start` | `UserPromptSubmit`/`PreToolUse`/`PostToolUse`; matcher `apply_patch\|Edit\|Write\|Bash` | `Stop -> acd touch` | `templates/codex/hooks.json`; active timeout 15s |
| Cursor | `sessionStart -> acd start` | `postToolUse`/`afterFileEdit` -> start+wake inline | `stop -> flush`; `sessionEnd -> stop` | User-global `~/.cursor/hooks.json`; `conversation_id`; `--watch-pid 0`; approve in Settings → Hooks |
| OpenCode | `session.created -> acd start` | `tool.before.*`/`tool.after.*`: start+wake and-chain + log fallback | `session.idle -> acd flush --logical`; `session.deleted -> acd stop --session-id` | `OPENCODE_SESSION_ID`/`OPENCODE_PROJECT_DIR`; `~/.config/opencode/hook/hooks.yaml` |
| Pi | `session.created -> acd start` | `tool.before.*`/`tool.after.*`: start+wake and-chain + log fallback | `session.idle -> acd flush --logical`; `session.deleted -> acd stop --session-id` | `SID="${PI_SESSION_ID:-pi-$$-$(date +%s)}"`; no `uuidgen`; `~/.pi/agent/hook/hooks.yaml` |

Active-hook body pattern:

```bash
LOG="${XDG_STATE_HOME:-$HOME/.local/state}/acd/<harness>-hook.log"
[ -d "$(dirname "$LOG")" ] || mkdir -p "$(dirname "$LOG")" 2>/dev/null || true
{ acd start --... && acd wake --... ; } 2>>"$LOG" || { printf '[%s] active hook failed exit=%d cmd=acd-start-wake\n' "$(date +%FT%T%z)" "$?" >>"$LOG"; exit 1; }
```

- `acd start` failure is no longer masked by wake; active hook exits nonzero. AdapterE2E covers stop-all self-heal and corrupt-DB negatives.
- Existing-user migration: rerun `acd setup <harness>` and replace installed hooks. `acd doctor` flags drift.
- Markers: TOML/YAML use leading `# acd-managed: true`; JSON detects `"_acd_managed": true`. Keep hookhelper, setup tests, templates, AdapterE2E in sync.
- Codex: `~/.codex/hooks.json` wins over `~/.codex/config.toml`; legacy TOML deleted; `_acd_managed: true` is top-level JSON; `acd doctor` warns when both Codex files carry acd markers because events double.
- Codex Stop deliberately stays on `acd touch`: Codex Stop fires on every assistant turn and overlaps tool runs, so `flush --logical` there would chain commits per tool turn. If Codex later adds a true session-idle event, mirror the Claude/OpenCode/Pi flush behavior.
- Codex `/hooks` re-approval is required after every `~/.codex/hooks.json` change; until approved, `SessionStart` never fires.
- `acd setup codex --raw > ~/.codex/hooks.json` destroys non-ACD entries; custom-hook users must merge manually.
- Cursor: user-global `~/.cursor/hooks.json` only (not repo `.cursor/hooks.json`); hook commands are inline in `templates/cursor/hooks.json`. `acd setup cursor --raw > ~/.cursor/hooks.json` replaces the entire file; merge the five lifecycle events manually when non-acd hooks exist. Approve in Settings → Hooks after install.
- Codex hooks are enabled by default. If `~/.codex/config.toml` pins feature flags, keep `[features].hooks = true`; `features.codex_hooks` is only a deprecated alias. `hooks.json` carries hook bodies, not feature flags.
- Codex `cwd` comes from stdin via `acd hook-stdin-extract session_id cwd? <&0`; missing `cwd` falls back to `$PWD`. `CODEX_PROJECT_DIR`/`printf "{}\n"` are gone. Bash bodies use `|| exit 0` after helper so missing `acd` does not block hook.
- `acd flush --logical` refreshes heartbeat, enqueues `flush_logical`, and SIGUSR1s the daemon. Only drained `flush_logical` sets `IntentBypassBatchWait`; plain `acd wake` only nudges capture/replay. Logical flush requires a registered active session id and reports refusals in JSON without blocking the harness hook.

## Env

| Group | Vars |
|---|---|
| Trace | `ACD_TRACE`; `ACD_TRACE_DIR` default `<gitDir>/acd/trace` |
| Shadow/rewind | `ACD_SHADOW_RETENTION_GENERATIONS=1`; `ACD_REWIND_GRACE_SECONDS=60` (`0` disables); `ACD_KEEP_DEAD_BRANCH_BARRIERS` disables auto-prune |
| Capture | `ACD_SENSITIVE_GLOBS`; `ACD_SAFE_IGNORE`; `ACD_SAFE_IGNORE_EXTRA`; `ACD_MAX_PENDING_EVENTS`; `ACD_PATH_QUIESCENCE_SECONDS=0` (off; restart to apply; capture remains durable, planner offer waits for quiet path) |
| AI | `ACD_AI_PROVIDER=deterministic|openai-compat|subprocess:<name>`; `ACD_AI_BASE_URL`; `ACD_AI_API_KEY`; `ACD_AI_MODEL`; `ACD_AI_TIMEOUT=30s`; `ACD_AI_CA_FILE`; `ACD_AI_DIFF_EGRESS`; `ACD_INTENT_REJECTS_RAW` |
| Strategy | `ACD_COMMIT_STRATEGY=event|intent`; `ACD_INTENT_WINDOW=10`; `ACD_INTENT_MIN_PENDING=10`; `ACD_INTENT_MAX_PENDING_AGE=5m`; `ACD_INTENT_RECENT_COMMITS=5`; `ACD_INTENT_DEFER_LIMIT=1`; `ACD_INTENT_PATH_COALESCE=1`; `ACD_RECENT_COMMIT_AFFINITY_SECONDS=0`; planner cap `ai.IntentStageDiffCap=16000` |
| Watcher/client | `ACD_FSNOTIFY_ENABLED`; `ACD_DISABLE_FSNOTIFY`; `ACD_MAX_INOTIFY_WATCHES`; `ACD_CLIENT_TTL_SECONDS` |
