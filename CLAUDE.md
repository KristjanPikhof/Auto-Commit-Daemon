# Agent guide

## Project

- Static Go CLI/daemon for macOS/Linux `arm64`/`amd64`; no Windows v1.
- Module `github.com/KristjanPikhof/Auto-Commit-Daemon`; Go 1.26.5; `modernc.org/sqlite v1.54.0`; MIT.
- Tags use `vYYYY-MM-DD`; find the latest with `git tag --sort=-creatordate | head`. Never move a published tag unless requested.
- `AGENTS.md -> CLAUDE.md`; edit `CLAUDE.md` and preserve the symlink.
- README/docs are product contracts. Keep details in canonical docs, not duplicated here; nested Markdown fences use `~~~`.

## Commands

~~~bash
make build      # static bin/acd; CGO_ENABLED=0; netgo,osusergo
make test       # go test ./... -race -count=1
make lint       # go vet ./... + gofmt check
make fmt        # gofmt -w .
make tidy       # go mod tidy
~~~

Verification cadence:

- Group implementation into coherent milestones. Do not run builds, tests, or linters after every file or small edit.
- During a milestone, run only the cheapest focused check for the code being changed, such as one package, one test, or `make build` when compilation is the risk.
- Run the full pre-PR gate once when the complete change is ready for handoff or review. Rerun only checks invalidated by later edits.
- For docs-only or agent-guidance changes, use `git diff --check` plus targeted link, command, or symlink checks. Do not build or run Go tests unless the documentation changes generated output or an executable contract.

Full pre-PR gate:

~~~bash
cleanenv() { env -u ACD_INTENT_MIN_PENDING -u ACD_INTENT_MAX_PENDING_AGE -u ACD_INTENT_SETTLE_WINDOW -u ACD_INTENT_WINDOW -u ACD_INTENT_RECENT_COMMITS -u ACD_INTENT_DEFER_LIMIT ACD_COMMIT_STRATEGY=event "$@"; }
cleanenv make lint
cleanenv make test
cleanenv go test ./test/integration/... -tags=integration -race -count=1 -timeout 5m
cleanenv go test ./internal/daemon/... ./internal/git/... ./internal/state/... ./internal/pause/... ./internal/cli/... -race -count=3 -timeout 10m
git diff --check
~~~

Focused stability: `-count=10`; ordering hazards: `GOMAXPROCS=1 -count=50`.

Release only after explicit install/tag approval:

~~~bash
make build && install -m 0755 ./bin/acd ~/.local/bin/acd
git tag v2026-MM-DD <commit> && git push origin v2026-MM-DD
gh run list --workflow release.yml --branch v2026-MM-DD --limit 1
gh run watch <run-id>
gh release view v2026-MM-DD --json isDraft,isPrerelease,isLatest,url,assets
ACD_VERSION=v2026-MM-DD sh scripts/install.sh
~~~

- `make build` uses `git describe`; an untagged build shows the prior tag plus commit count.
- Release runs `goreleaser release --clean`, publishes the Homebrew formula with `TAP_GITHUB_TOKEN`, then retries install smoke six times. `.goreleaser.yaml` uses `prerelease: false`.
- A release rerun may fail because the GitHub release exists. Inspect release assets/checksums, tap formula, and version-pinned installer before considering a new tag.

## Map

| Path | Purpose |
|---|---|
| `cmd/acd/main.go` | CLI entrypoint |
| `internal/cli` | Cobra commands, lifecycle/control, recovery, setup, start cache |
| `internal/daemon` | Run loop, capture/replay, self-publication, recovery, intent, shadow, fsnotify |
| `internal/state` | SQLite v19: events, settings, candidates, repairs, lineage, self-publication journal |
| `internal/git` | Bounded ref/tree/diff/blob/index/history/ignore helpers |
| `internal/ai` | Deterministic, OpenAI-compatible, subprocess providers; prompts/planner |
| `internal/{central,config,identity,logger,paths,pause,prompttrace,trace}` | Registry, config, process identity, XDG/logging, pause, diagnostics |
| `internal/adapter`, `templates/*` | Harness detection and install snippets; Codex/Cursor use `hooks.json` |
| `test/integration` | Build-tagged lifecycle, recovery, AI, intent, PTY, capacity and fault tests |
| `.github/workflows/{ci,codeql,release}.yml` | CI, CodeQL, tag release |
| `README.md`, `docs/*`, `CHANGELOG.md` | Product/release contracts |

## Workflow and test traps

- Scope changes; prefer `rg`; preserve unrelated work. Investigate before retrying: races, panics, ordering failures, flakes, and read-only migrations are bugs.
- ACD self-hosts this repo. Do not pause it for ordinary edits, builds, lint, or tests that use isolated repositories. Pause only before manual history shaping, branch surgery, recovery/DB mutation, or a test deliberately exercising capture/replay against this checkout: `acd pause --repo . --reason "..." --yes`; always finish with `acd resume --repo . --yes`.
- Source edits do not update the running daemon. For runtime/provider behavior, build and invoke `bin/acd`, or install/restart only with authorization.
- After test `git.Init`/`git init`, run `git symbolic-ref HEAD refs/heads/main`. HEAD-transition tests use `waitForMetaValue(MetaKeyBranchHead, <sha>, 3s)`.
- Broad-run-sensitive coverage includes fsnotify wake, lifecycle, wake coalescing, real SIGUSR1, repeated edits, external FF reseed, FF-in-rewind-grace, and `TestSquashBacklogRecovery_PreservesSixtyCapturesAndControls`.
- Settings integration selector: `go test ./test/integration/... -tags=integration -run 'Settings|RuntimeConfig|ConfigExperiment|SettingsTUI' -race -count=1 -timeout 5m`. Preserve isolated HOME/XDG, local HTTPS, real PTYs at wide/medium/narrow sizes, resize, keyboard-only, NO_COLOR, accessible, dirty-quit, terminal restoration, and static builds.
- Integration has a hard 5m package budget. Only isolated tests may use `t.Parallel`; never parallelize ancestors of `t.Setenv`. PTY tests must use distinct repo/home/PTY/context.
- Planner mocks must distinguish `capture_intent_plan_v2` from locked `commit_message` rewrites. A model-wide failure that also rejects the rewrite changes durable fallback/experiment behavior.
- `runStart` treats PID/heartbeat without the canonical lock as stale. Fake daemons must acquire/release `daemon.AcquireDaemonLock`; otherwise tests may call `defaultSpawnDaemon`, recursively execute `cli.test`, and leak process trees.
- Daemon package `TestMain` isolates HOME/XDG and stubs ordinary host owner discovery; lock tests retain real/injected ownership probes.
- CLI changes need Cobra help/examples. Template changes need `internal/cli/setup_test.go` and AdapterE2E coverage.
- Rerun environment-sensitive failures through `cleanenv`; verify suspected baseline flakes on `main`.

Commit messages:

| Mode | Contract |
|---|---|
| `imperative` (default) | Semantic imperative subject, max 50 chars, no period; blank line; `- ` why/context bullets wrapped at 72 |
| `conventional` | Scope-less `type: subject`; types `feat`, `fix`, `docs`, `refactor`, `test`, `build`, `ci`, `chore`, `perf`, `style`, `revert`; no scopes/breaking markers |

Avoid filename-only subjects, `Update file`, `WIP`, and `changes`. Planner rationale belongs in `grouping_reason`, never the commit body. Caps: `ai.SubjectCap=50`, `ai.BodyWrap=72`, `ai.DiffCap=4000`, `ai.IntentStageDiffCap=16000`.

## State, ownership, branches and capture

- Repo DB: `<gitDir>/acd/state.db`; central registry/stats use XDG paths. `SchemaVersion=19`: v17 candidate lineage; v18 immutable self-publication journal; v19 prepare-time completion semantics. See `internal/state/{schema.go,migrate.go,self_publication.go}`.
- Read-only CLI paths must not migrate. Pre-v14 returns empty settings; pre-v15 reports Intent v2 unavailable; pre-v18 reports self-publication unavailable.
- Canonical ownership is `<git-common-dir>/acd-daemon.lock`, shared by linked worktrees and outside movable `.git/acd`. A new daemon also locks every discovered legacy `<gitDir>/acd/daemon.lock` and probes old owners. Never delete a lock file or move `.git/acd` to bypass ownership.
- PID/heartbeat are supporting evidence, not ownership proof. Concurrent `acd start` waits within the existing 3s/5s start budget for a new lock owner to stamp live state, then rechecks the lock; an unknown owner remains fail-closed.
- Start cache: `<gitDir>/acd/start-cache-<sha256(session_id)[:16]>.json`, schema v2, atomic temp+rename. `acd stop` removes matching/all caches.
- Branch tokens: attached `rev:<sha> <branch-ref>`; detached `rev:<sha>`; missing `missing <branch-ref>`. Reset/rebase/switch/same-SHA ref switch bumps generation; ordinary FF keeps it, except FF during rewind grace bumps, reseeds, and clears grace.
- Reconcile the prior exact pair before accepting any non-unchanged token transition. Dead-ref sweeps skip live refs.
- Detached HEAD pauses capture/replay and `acd start` refuses; never fall back to `refs/heads/main` after symbolic-ref failure.
- Git-op markers: `rebase-merge`, `rebase-apply`, `MERGE_HEAD`, `CHERRY_PICK_HEAD`, `BISECT_LOG`. Log non-`ErrNotExist` stat errors but treat the marker absent for that tick.
- Same-branch rewind sets `daemon_meta.replay.paused_until`; manual `<gitDir>/acd/paused` wins.
- `shadow_paths` key: `(branch_ref, branch_generation, path)`. Bootstrap in 5000-row chunks; write `shadow.bootstrapped:<ref>:<generation>` only after completion. Stale/partial bootstrap causes phantom creates.
- `walkLive` is directory-BFS with 1000-path ignore batches. Prune ignored/sensitive/safe-ignore dirs before readdir. Worktree `acd/` is data; only `.git/acd` is state. Symlinks are mode `120000`, never descended.
- Empty `ACD_SENSITIVE_GLOBS` retains defaults. False-like `ACD_SAFE_IGNORE` disables; `ACD_SAFE_IGNORE_EXTRA=dist/,build/` appends.
- `IgnoreChecker` streams writes while reading persistent `git check-ignore --stdin -z --non-matching --verbose`; one large write can deadlock on macOS 16 KiB pipes. Close is cancel+kill with a 2s wait.

## Replay, self-publication and intent

- Event mode publishes one capture per commit. Intent v2 persists semantic candidates, builds hard/soft dependency DAGs, accepts proven non-contiguous membership, and publishes topologically after cohesion, completeness, separation, dependency, materialization, verification, and revertibility gates.
- Self-publication phases: `prepared` before ref CAS; `git_applied` after exact target observation; `completed` after atomic event/candidate/branch-token settlement; `abandoned` only when proof shows the ref never left the source. v19 locks completion semantics at prepare time.
- Startup and loop-boundary recovery prove literal ref, source/parent, target/tree, membership, and ref ownership. Ambiguity remains durable with `needs_attention`; never guess, purge, archive, or settle it.
- Run-loop adoption of a proved target preserves branch generation. Later movement follows normal external transition reconciliation.
- Journal preparation remains inside the 60s per-event budget. A canceled/deadline preparation transaction rolls back, marks the representative event terminal, and halts the pass; mark/complete/abandon use bounded non-canceled state contexts.
- Candidate limits: 256 captures, 128 open candidates, 4096 edges per exact pair; purpose 512 chars; missing-companion/atomicity 2048 chars; verification output 64 KiB. Candidate tables store no raw diffs.
- Hard edges: same-path order, rename chains, before/after objects, create/modify/delete, known generated dependencies. Soft evidence: directory proximity, source/test convention, symbols/hunks, references, roles, activity epochs. Time is weak evidence.
- Native v2 assigns every visible capture exactly once. Subprocess v1 adapts as `planner_protocol=v1_compat` and cannot claim native readiness. One unchanged fingerprint gets at most three remote attempts; valid groups are locked while unresolved captures are replanned. Message-only failure uses the locked rewrite path.
- Fallback: every Intent preset uses an evidence partition after its bounded attempts or no-progress detection. Time and directory proximity cannot merge alone; Quality keeps full verification and bounded private-history repair. No preset bypasses hard dependencies, materialization, or required verification.
- Candidate evaluation triggers on quiet time, soft boundary, logical flush, max age, or capacity. Boundaries never bypass safety gates.
- Balanced/Quality repair only a private contiguous ACD-owned first-parent suffix at exact HEAD. Reject merges, tags, other refs, Git operations, manual pause, staging overlap, or failed gates. Persist phase mappings, create `refs/acd/intent-repair/.../backup`, CAS literal ref, reconcile, reseed, preserve live index/worktree. Completed backups: 7 days, newest 50.
- Planner windows store privacy-safe summaries, not raw diffs. Rejects: `<gitDir>/acd/planner-rejects.jsonl` (5 MiB plus `.1`); raw response requires `ACD_INTENT_REJECTS_RAW`.
- Planner circuit: transport failure opens immediately; semantic validation findings do not affect transport health. Cooldowns are 30s, 2m, 10m with one half-open probe. Caller cancellation releases the probe without changing health.
- Scratch index `<gitDir>/acd/replay-*.index` starts at `cctx.BaseHead`. CAS literal `HEAD`; named refs use `--no-deref`.
- Event pass budget: 64 rows. `blocked_conflict`/`failed` are terminal barriers. Idempotent publish checks current HEAD first; `superseded_external` requires bounded ancestry/tree/live-state proof. Live-index repair is path-scoped and never overwrites user staging.

Regular configuration:

- `acd configure`: Everyday/Maximum speed/Strict review onboarding; adaptive prompts, one fingerprint-bound preview, synchronous provider test, one runtime revision, durable validation job, then `acd on`. `--wait` follows validation. Dry-run performs no provider call, command, write, start, or hook change.
- `acd settings`: advanced Bubble Tea v2.0.8/Bubbles v2.1.1/Lip Gloss v2.0.5/Huh v2.0.3 lab. Preserve static builds, responsive/accessibility/no-color, `DRAFT > TESTED > QUEUED > ACTIVE`.
- Resolution: experiment > repository > profile > global > environment > preset > default. Hot fields apply between passes; restart-required fields wait for daemon restart. Global saves do not fan out; stopped-daemon apply never starts it.
- Credentials: `${XDG_CONFIG_HOME:-$HOME/.config}/acd/credentials.json` schema v1, regular `0600` under owner-only `0700`; reject symlinks, wrong owner/mode, malformed/multiple/future JSON. Atomic same-directory write with fsync/rename/dir-fsync. `ACD_AI_API_KEY` wins. Never expose secrets in state, logs, traces, fingerprints, status, diagnostics, errors, or test output.
- Strict provider tests send one synthetic request without source. Endpoint credentials, subprocess execution, diff egress, and verification commands require operation-specific approval. Verification runs in bounded ephemeral detached candidate worktrees.

## Exact-chain recovery

~~~bash
acd fix --dry-run
acd fix --yes
acd fix --force --dry-run
acd fix --force --yes
~~~

- Reconcile from the earliest unpublished seq for one exact `(branch_ref,generation)`. Resolve transitive rename closure; strip seed-represented prefixes; fail safely above 4096 context events, 4096 ancestry commits, or 64 proof commits. Context is materialization-only.
- HEAD+ancestry+final-state proof marks the complete chain `published` under `/published`; otherwise archive immutable provenance under `/archive` and mark `recovered`. Missing objects, partial proof, collisions, or races make no DB transition.
- Recovery refs include a 96-bit SHA-256 target digest: archive hashes `baseHead + NUL + treeOID`; published hashes `commitOID`.
- Dead-ref recovery locks proof ref and expected-absent branch in one `git update-ref --stdin` transaction held through SQLite transition.
- Archive recovery invalidates the exact shadow pair, reseeds from HEAD, and recaptures dirty work. Reconciliation never changes live HEAD/index/worktree.
- `acd fix` defaults dry-run. Apply acquires daemon ownership after consent, rechecks daemon/HEAD/git-op/pause, refuses a live owner, then makes a WAL-consistent `VACUUM INTO` backup verified by `PRAGMA quick_check`.
- `--force` is archive-only exact-chain recovery, not captured-work purge. `commit-all` reconciles before reseed.
- Dry-run/decline/JSON without `--yes` must not open writable state, lock, create refs, capture, or build providers.
- `acd recover` and `acd purge-events` are hidden/deprecated. Only explicit purge `--all` delegates archive-only whole-repo recovery.
- Published pruning defaults to 7 days, preserves rename-aware recovery closure and unresolved terminals, and prunes recovered members only while their recovery ref is verified/locked.
- `acd resume --yes` removes only manual pause. Restore/archive runbooks: `docs/user-workflows.md`.

## CLI, observability and run loop

- Keep `processBranchTokenChange` before capture and after flush drain. Branch settle 100ms; flush drain 256; startup fails acknowledged flushes older than 5m.
- fsnotify is opt-in; poll is the safety net. Dispatch never blocks; rewalk/diagnostics use replaceable worker channels; debounce tail 500ms; budget exhaustion falls back to poll.
- Bare `acd` is read-only health. Active-tail terminal events mean `needs_attention`; historical terminals do not. `acd on` is idempotent but returns nonzero if still unhealthy. `acd off` disables/stops and preserves state.
- `events`, `explain`, `status`, `diagnose`, and `doctor` read paths use read-only SQLite and never migrate/create tables.
- Status/diagnose/doctor expose self-publication phase, canonical owners, remediation (`automatic_recovery`, `stop_old_owner`, `needs_attention`), intent windows/circuit/candidates/repair, and runtime revisions/experiments.
- Start cache/registry may bypass control lock, migration, and registry rewrite. Manual session: `human:<repoHash>`; harness starts require `--session-id`.
- Autodiscovery defaults enabled; override `${XDG_CONFIG_HOME:-$HOME/.config}/acd/config.json` or `ACD_REPO_AUTODISCOVERY`. Invalid policy skips hooks but errors for manual callers; `acd repo init` registers explicitly.
- `repo disable` stops, clears caches, preserves DB; hooks skip `repo_disabled`; `repo enable` only clears disabled state.
- Canonicalize with `git.ResolveWorktree`; reject non-worktrees. Git helper limits: read 30s, write 60s, diff 1 MiB. Ambiguous revision is `git.ErrRefAmbiguous`. Pin `ps` to `/bin/ps` on Darwin and `/usr/bin/ps` on Linux.
- Logs: `${XDG_STATE_HOME:-$HOME/.local/state}/acd/<repo-hash>/daemon.log`; hooks: `${XDG_STATE_HOME:-$HOME/.local/state}/acd/<harness>-hook.log`. `logs --follow` continues from initial-tail EOF.
- `events --watch` starts at ledger tail unless `--since`; rewrite progress uses stderr.
- `ACD_TRACE=1` writes best-effort JSONL to `<gitDir>/acd/trace/YYYY-MM-DD.jsonl` or `ACD_TRACE_DIR`; never block/abort.
- Providers declare `NeedsDiff`; network diff requires `NeedsDiff=true` and truthy `ACD_AI_DIFF_EGRESS`, after redaction/truncation. Removed `ACD_AI_SEND_DIFF` warns once.
- Probes: `acd status --repo .`, `acd events --watch`, `acd logs --repo . --lines 50 --follow`, `acd diagnose --repo . --json`, `acd doctor --repo . --json`, `git status --short --ignored`.

## Harness templates

| Harness | Start | Active | End |
|---|---|---|---|
| Claude Code | `SessionStart -> start` | `Pre/PostToolUse -> start && wake` | `Stop -> flush --logical`; `SessionEnd -> stop` |
| Codex | `SessionStart -> start` | `UserPromptSubmit/PreToolUse/PostToolUse -> start && wake` | `Stop -> touch --soft-boundary` |
| Cursor | `sessionStart -> start` | `postToolUse/afterFileEdit -> start && wake` | `stop -> flush`; `sessionEnd -> stop` |
| OpenCode/Pi | `session.created -> start` | `tool.before/after.* -> start && wake` | `session.idle -> flush --logical`; `session.deleted -> stop` |

- Templates are source of truth. Keep helper, setup tests, and AdapterE2E synchronized. Active hooks use `start && wake`, log failures, return nonzero.
- TOML/YAML/shell use leading `# acd-managed: true`. JSON is schema-clean and detected by command signatures; top-level `_acd_managed` is legacy-only.
- Codex uses `~/.codex/hooks.json` with only root `hooks`; legacy TOML duplicates events. Keep `[features].hooks=true` if pinned; `features.codex_hooks` is deprecated.
- Codex Stop uses `acd touch --soft-boundary`; changes require `/hooks` re-approval. `setup codex --raw > ~/.codex/hooks.json` replaces the file: merge custom hooks.
- Codex extracts `session_id cwd?` from stdin; missing cwd uses `$PWD`; missing `acd` is fail-soft.
- Cursor installs only `~/.cursor/hooks.json`; `--raw` replaces it, so merge custom hooks and re-approve in Settings -> Hooks.
- `flush --logical` refreshes heartbeat, queues `flush_logical`, signals SIGUSR1, and bypasses intent batch wait only after drain for a registered active session.

## Environment

`acd configure` owns regular setup; `acd settings` persists advanced non-secret values and shows environment shadowing. Hot settings apply at the next safe boundary; restart-required fields need daemon restart.

| Group | Variables/defaults |
|---|---|
| Repo | `ACD_REPO_AUTODISCOVERY=enabled`; durable `${XDG_CONFIG_HOME:-$HOME/.config}/acd/config.json` |
| Trace | `ACD_TRACE=off`; `ACD_TRACE_DIR=<gitDir>/acd/trace`; `ACD_AI_PROMPT_TRACE=off` (sensitive) |
| Recovery | `ACD_SHADOW_RETENTION_GENERATIONS=1`; `ACD_REWIND_GRACE_SECONDS=60`; `ACD_KEEP_DEAD_BRANCH_BARRIERS` |
| Capture | `ACD_MAX_FILE_BYTES=5 MiB`; `ACD_MAX_PENDING_EVENTS=50000`; `ACD_PATH_QUIESCENCE_SECONDS=0`; `ACD_EVENT_RETENTION_DAYS=7`; sensitive/safe-ignore variables |
| AI | provider `deterministic`, `openai-compat`, or `subprocess:<name>`; `ACD_AI_BASE_URL=https://api.openai.com/v1`; `ACD_AI_MODEL=gpt-5.4-mini`; `ACD_AI_TIMEOUT=5m`; `ACD_AI_API_KEY`; `ACD_AI_CA_FILE`; `ACD_AI_DIFF_EGRESS=off`; `ACD_COMMIT_FORMAT`; `ACD_INTENT_REJECTS_RAW=off` |
| Intent | `ACD_COMMIT_STRATEGY=event|intent`; `ACD_COMMIT_PRESET=fast|balanced|quality`; verification/repair variables; `ACD_INTENT_REPAIR_MAX_COMMITS<=5`; built-ins keep path coalescing off |
| Watch | `ACD_FSNOTIFY_ENABLED=off`; `ACD_DISABLE_FSNOTIFY`; `ACD_MAX_INOTIFY_WATCHES`; `ACD_CLIENT_TTL_SECONDS=1800` |

Canonical details: `docs/overview.md`, `docs/capture-replay.md`, `docs/settings.md`, `docs/intent-commit-flow.md`, `docs/ai-providers.md`, `docs/user-workflows.md`, `docs/rewrite-commits.md`.
