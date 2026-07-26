# Agent guide

## Project

- Static Go CLI/daemon for macOS/Linux `arm64`/`amd64`; no Windows v1.
- Module: `github.com/KristjanPikhof/Auto-Commit-Daemon`; Go 1.26.5; `modernc.org/sqlite v1.53.0`; MIT.
- Tags use `vYYYY-MM-DD`. Find the latest with `git tag --sort=-creatordate | head`; never move a published tag unless requested.
- `AGENTS.md -> CLAUDE.md`. Edit `CLAUDE.md` and preserve the symlink.
- README/docs are product contracts. Link canonical runbooks instead of copying them here; nested Markdown fences use `~~~`.

## Commands

~~~bash
make build      # static bin/acd; CGO_ENABLED=0; netgo,osusergo
make test       # go test ./... -race -count=1
make lint       # go vet ./... + gofmt check
make fmt        # gofmt -w .
make tidy       # go mod tidy
~~~

Pre-PR gate:

~~~bash
cleanenv() { env -u ACD_INTENT_MIN_PENDING -u ACD_INTENT_MAX_PENDING_AGE -u ACD_INTENT_SETTLE_WINDOW -u ACD_INTENT_WINDOW -u ACD_INTENT_RECENT_COMMITS -u ACD_INTENT_DEFER_LIMIT ACD_COMMIT_STRATEGY=event "$@"; }
cleanenv make lint
cleanenv make test
cleanenv go test ./test/integration/... -tags=integration -race -count=1 -timeout 5m
cleanenv go test ./internal/daemon/... ./internal/git/... ./internal/state/... ./internal/pause/... ./internal/cli/... -race -count=3 -timeout 10m
~~~

Release only after explicit install/tag approval:

~~~bash
make build && install -m 0755 ./bin/acd ~/.local/bin/acd
git tag v2026-MM-DD <commit> && git push origin v2026-MM-DD
gh run list --workflow release.yml --branch v2026-MM-DD --limit 1
gh run watch <run-id>
gh release view v2026-MM-DD --json isDraft,isPrerelease,isLatest,url,assets
ACD_VERSION=v2026-MM-DD sh scripts/install.sh
~~~

- `make build` uses `git describe`, so an untagged build shows the prior tag plus commit count.
- Release runs `goreleaser release --clean`, publishes the Homebrew formula with `TAP_GITHUB_TOKEN`, then retries install smoke 6 times for asset propagation. `.goreleaser.yaml` sets `prerelease: false` so date tags work with `releases/latest`.
- A rerun can fail because the GitHub release already exists. Inspect the release, checksums/downloads, tap formula, and version-pinned installer before considering any new tag.

## Map

| Path | Purpose |
|---|---|
| `cmd/acd/main.go` | CLI entrypoint |
| `internal/cli` | Cobra commands, health/control, recovery, setup, start cache |
| `internal/daemon` | Run loop, capture/replay, exact-chain reconciliation, intent circuit, shadow, fsnotify |
| `internal/state` | SQLite v14: events/ops, planner/runtime settings state, recovery snapshots, meta/clients/flush |
| `internal/git` | Bounded Git/ref/tree/diff/blob/index/history/ignore helpers |
| `internal/ai` | Deterministic, OpenAI-compatible, subprocess providers; prompts and planner |
| `internal/{central,config,identity,logger,paths,pause,prompttrace,trace}` | Registry/config, XDG/logging, pause and diagnostics |
| `internal/adapter` | Harness detection/markers; do not restore old TODO stubs |
| `templates/{claude-code,codex,cursor,opencode,pi,shell}` | Install snippets; Codex/Cursor use `hooks.json` |
| `test/integration` | Build-tagged lifecycle, adapter, recovery, AI, intent and latency tests |
| `.github/workflows/{ci,codeql,release}.yml` | CI, CodeQL and tag release |
| `README.md`, `docs/*`, `CHANGELOG.md` | User and release contracts |

## Workflow and tests

- Scope changes, prefer `rg`, preserve unrelated work and inspect failures before retrying. Races, panics, ordering failures, flakes and read-only migrations are bugs.
- After test `git.Init`/`git init`, run `git symbolic-ref HEAD refs/heads/main`. HEAD-transition tests use `waitForMetaValue(MetaKeyBranchHead, <sha>, 3s)`.
- Focused stability: `-count=10`. Ordering hazards: `GOMAXPROCS=1 -count=50`.
- Broad-run-sensitive coverage includes fsnotify wake, lifecycle, wake coalescing, real SIGUSR1, repeated edits, external FF reseed, FF-in-rewind-grace, and `TestSquashBacklogRecovery_PreservesSixtyCapturesAndControls`.
- Settings integration coverage: `go test ./test/integration/... -tags=integration -run 'Settings|RuntimeConfig|ConfigExperiment|SettingsTUI' -race -count=1 -timeout 5m`. Preserve isolated HOME/XDG roots, local HTTPS fixtures, real PTYs at wide/medium/narrow sizes, resize, keyboard-only, NO_COLOR, accessible, dirty-quit, terminal-restoration, and static-build checks.
- CLI changes need Cobra help/examples. Template changes need `internal/cli/setup_test.go` and AdapterE2E coverage.
- ACD self-hosts this repo. Pause only for branch/history surgery, recovery/DB mutation, or tests that deliberately run capture/replay against this checkout; resume with `acd resume --repo . --yes`.
- Source edits do not change the running daemon. For daemon/provider tests, build and install/use `bin/acd`, then restart or invoke it directly.
- Rerun environment-sensitive failures through `cleanenv` and verify suspected baseline flakes on `main`.

Commit messages:

| Mode | Contract |
|---|---|
| Default `imperative` | Semantic imperative subject, max 50 chars, no period; blank line; `- ` why/context bullets wrapped at 72 |
| Optional `conventional` | Scope-less `type: subject`; types: `feat`, `fix`, `docs`, `refactor`, `test`, `build`, `ci`, `chore`, `perf`, `style`, `revert`; no scopes/breaking markers |

- Avoid filename-only subjects, `Update file`, `WIP` and `changes`.
- Planner rationale belongs in `grouping_reason`, never the commit body.
- Caps: `ai.SubjectCap=50`, `ai.BodyWrap=72`, per-event `ai.DiffCap=4000`, planner `ai.IntentStageDiffCap=16000`.

## State, branches and capture

- Repo DB: `<gitDir>/acd/state.db`; central registry/stats use XDG paths. v12 adds immutable recovery snapshots, v13 adds same-base recovery-prefix retention, and `SchemaVersion=14` adds immutable runtime config revisions, activation requests, experiments, and planner/decision revision metadata; see `internal/state/migrate.go`.
- Start cache: `<gitDir>/acd/start-cache-<sha256(session_id)[:16]>.json`, schema v2, atomic temp+rename. `acd stop` removes matching/all caches.
- Branch tokens: attached `rev:<sha> <branch-ref>`; detached `rev:<sha>`; missing `missing <branch-ref>`. Reset/rebase/switch/same-SHA ref switch bumps generation; ordinary FF keeps it, except FF during rewind grace bumps, reseeds, and clears grace. Legacy bare rev upgrades as Diverged.
- Every non-unchanged token transition reconciles the prior exact pair before acceptance; dead-ref sweeps alone skip live refs.
- Detached HEAD pauses capture/replay and `acd start` refuses; never fall back to `refs/heads/main` after symbolic-ref failure.
- Git-op markers: `rebase-merge`, `rebase-apply`, `MERGE_HEAD`, `CHERRY_PICK_HEAD`, `BISECT_LOG`. Log non-`ErrNotExist` stat errors and treat the marker absent for that tick to avoid a permanent latch.
- Same-branch rewind sets `daemon_meta.replay.paused_until` using `ACD_REWIND_GRACE_SECONDS`. Manual `<gitDir>/acd/paused` always wins.
- `shadow_paths` is keyed by `(branch_ref, branch_generation, path)`. Bootstrap uses 5000-row chunks and writes `shadow.bootstrapped:<ref>:<generation>` only after completion; clean partial/empty-marked state before re-bootstrap.
- Capture compares live worktree to shadow; stale bootstrap causes phantom creates. Reseed keeps one old generation by default.
- `walkLive` is directory-BFS with 1000-path ignore batches; prune ignored/sensitive/safe-ignore dirs before readdir. Worktree `acd/` is data; only `.git/acd` is state. Symlinks are mode `120000` and never descended.
- Empty `ACD_SENSITIVE_GLOBS` retains defaults. Safe-ignore prunes descendants, not same-named files; false-like `ACD_SAFE_IGNORE` disables and `ACD_SAFE_IGNORE_EXTRA=dist/,build/` appends.
- `IgnoreChecker` keeps `git check-ignore --stdin -z --non-matching --verbose` alive. Stream writes while reading stdout; one large write can deadlock on macOS 16 KiB pipes. Close is cancel+kill with a 2s wait.

## Replay and intent

- `acd settings` is the Go-native configuration lab. It uses Bubble Tea v2.0.8, Bubbles v2.1.1, Lip Gloss v2.0.5, and Huh v2.0.3. Preserve static CGO-disabled builds, responsive/accessible/no-color behavior, and `DRAFT > TESTED > QUEUED > ACTIVE` text labels.
- Settings authoring precedence is repository > profile > global > environment > default. Active experiments are immutable runtime candidates outside those authoring layers. API keys remain environment only. Hot revisions activate between daemon passes; saved restart-required fields resolve when the next daemon starts and never enter a hot revision. Global saves do not fan out and stopped-daemon apply never starts it.
- Strict provider tests send one synthetic request without source data. Endpoint credentials and subprocess execution require operation-specific confirmation; diff egress is activation-only because the test never sends a diff. Rich and accessible modes resolve missing confirmations inside the current session. Accessible onboarding is action-first: Test current settings by default, Quick provider setup for provider essentials, and Advanced settings for the full non-sensitive catalog. Keep all interaction keyboard-only. Experiment comparisons are descriptive, not causal.

- Default `ACD_COMMIT_STRATEGY=event` publishes one capture per commit and never calls the planner. `intent` selects one capture or a non-empty subset; capture durability is unchanged.
- Every offered seq must be selected or deferred. Invalid/missing/unsafe output records `intent_planner_error` and falls back to a deterministic singleton.
- Ordered `commit_groups` partition and publish a visible window. `deferred_reasons[i].seq` must be in `deferred_seqs`; drop spurious reasons with a warning. At the defer limit, force the oldest overdue singleton.
- Same-path coalescing is legacy opt-in. Without it, consecutive captures stay visible; with it, different/multi-path, rename, delete, or branch/generation/base-head changes break the run and every original seq shares the commit.
- Planner windows persist privacy-safe summaries, not raw diffs. Rejects go to `<gitDir>/acd/planner-rejects.jsonl` (5 MiB plus `.1`); raw responses are redacted unless `ACD_INTENT_REJECTS_RAW` is truthy.
- Planner health is versioned JSON in `daemon_meta.intent.planner.health`. A transport failure opens the circuit immediately; 3 consecutive validation/safety failures open it; cooldowns are 30s, 2m, 10m; one half-open probe runs while other windows use deterministic fallback.
- Reuse one provider. Successfully received malformed/empty rewrites are `IntentMessageRewriteValidationError`; operational failures are transport. Preserve `MessageQualityError.Unwrap`; caller cancellation never mutates health.
- Per-pass scratch index `<gitDir>/acd/replay-*.index` starts from `cctx.BaseHead`. CAS updates literal `HEAD`; named refs use `--no-deref`.
- Event-mode run-loop budgeting is 64 rows; intent uses its planner gate/window. Per-event timeout is 60s and settles that event terminal before halting; caller cancellation propagates.
- `blocked_conflict`/`failed` are terminal barriers, so later pending rows for the exact pair stay hidden.
- Idempotent publish checks current HEAD before before-state blocking. `superseded_external` requires bounded history, parent/base-tree and live before-state proof. Live-index repair is path-scoped and never overwrites user staging.

## Recovery

~~~bash
acd fix --dry-run
acd fix --yes
acd fix --force --dry-run
acd fix --force --yes
~~~

- Reconcile from the earliest unpublished seq of one exact `(branch_ref,generation)` chain. Represented published context can include same-base prefixes, relevant interleaved events, and earlier captures published by commits that strictly descend the first recovery base. Resolve transitive rename dependencies with a reverse path-closure scan, strip the seed-represented operation prefix before proof, and fail safely above 4096 context events, 4096 traversed ancestry commits, or 64 remaining proof commits. Context is materialization-only and is never transitioned.
- HEAD+ancestry+final-state proof marks the complete unpublished chain `published` under a `/published` ref; otherwise archive reconstructs immutable provenance under `/archive` and marks it `recovered`. Missing objects, partial proof, collisions or races mean no DB transition.
- Recovery refs include a 96-bit SHA-256 target digest: archive hashes `baseHead + NUL + treeOID`; published hashes `commitOID`. This prevents selector reuse across linked worktrees/reset DBs.
- Dead-ref recovery locks/verifies the proof ref and expected-absent branch in one `git update-ref --stdin` transaction held through the SQLite transition. The dead-branch sweep proves or archives; legacy `dead_branch_prune.*` records only the last non-empty recovery.
- Archive recovery invalidates the exact shadow pair, then reseeds from HEAD and recaptures the dirty worktree. Reconciliation never changes live HEAD, index or worktree.
- `acd fix` is dry-run without `--yes`. Safe apply reconciles exact pairs and may clean explicitly selected protected-generated pending rows, expired manual pause and drained backpressure; exact-pair reconciliation never retargets or deletes captured chains.
- `--force` selects archive-only exact-chain recovery; it does not purge captured work. Fix acquires `daemon.lock` after consent, rechecks daemon/HEAD/git-op/pause safety, refuses a live owner, then creates a WAL-consistent `VACUUM INTO` backup verified with `PRAGMA quick_check` before migration/mutation.
- `commit-all` reconciles before reseed. Its dry-run, decline and JSON without `--yes` must not open writable state, lock, create refs, capture or build providers; incomplete recovery returns nonzero.
- `acd recover` and `acd purge-events` are deprecated/hidden. Purge selectors `--blocked|--pending|--failed` fail closed; only explicit `--all` delegates archive-only whole-repo recovery.
- Published-event pruning defaults to 7 days, preserves the same rename-aware recovery-context closure as reconciliation, and retains an exact pair without blocking unrelated pruning when that closure exceeds the execution cap. It never removes unresolved terminal rows and prunes recovered members only while their recovery ref is verified and locked.
- `acd resume --yes` lifts only manual pause. Restore/archive workflows are in `docs/user-workflows.md`.

## Run loop, CLI and observability

- Keep `processBranchTokenChange` before capture and after flush drain; the second catches Git surgery outside `wakeCh`. Branch settle is 100ms, flush drain 256, and startup fails acknowledged flushes older than 5m.
- fsnotify is opt-in; poll is the safety net. Dispatch never blocks: rewalk/diagnostics use replaceable worker channels, debounce tail is 500ms, and budget exhaustion falls back to poll.
- Bare `acd` is read-only health. Active-tail terminal events mean `needs_attention`; inactive historical terminals do not. `acd on` is idempotent but renders diagnostics and returns nonzero if still unhealthy. `acd off` idempotently disables/stops and preserves state.
- `events`, `explain` and `doctor` read paths must use read-only SQLite and never migrate/create tables. Missing decision tables yield empty valid output.
- `status --json`/`diagnose --json` expose intent windows, circuit, recovery, and additive runtime settings revision/experiment state. Pre-v14 read paths return an empty settings projection without migration. Open-circuit guidance is fallback plus automatic probe, not restart.
- Logs: `${XDG_STATE_HOME:-$HOME/.local/state}/acd/<repo-hash>/daemon.log` with rotation/compression; hooks use `${XDG_STATE_HOME:-$HOME/.local/state}/acd/<harness>-hook.log`. `acd logs --follow` continues from the EOF reached by initial tail.
- `events --watch` starts at ledger tail unless `--since` is supplied. Rewrite progress uses stderr; see `docs/rewrite-commits.md`.
- Start cache plus central registry may bypass control lock, DB migration and registry rewrite. Manual start uses `human:<repoHash>`; harness starts require `--session-id`.
- Repo autodiscovery defaults enabled. `${XDG_CONFIG_HOME:-$HOME/.config}/acd/config.json` or `ACD_REPO_AUTODISCOVERY` overrides it. Invalid policy fails closed/skips for hooks but errors for manual callers; `acd repo init` explicitly registers.
- Central lifecycle: `repo disable` stops, clears caches and preserves DB; hooks skip `repo_disabled`; `repo enable` only clears disabled state.
- Canonicalize with `git.ResolveWorktree` (toplevel, absolute Git dir, symlinks); reject non-worktrees. Registry upsert matches canonical DB/path.
- Git helpers use timeouts and bounded output: read 30s, write 60s, diff 1 MiB; ambiguous rev parse is `git.ErrRefAmbiguous`. Pin `ps` to `/bin/ps` on Darwin and `/usr/bin/ps` on Linux.
- AI providers declare `NeedsDiff`. Network diff egress requires both `NeedsDiff=true` and truthy `ACD_AI_DIFF_EGRESS`; redact/truncate before send. `ACD_AI_SEND_DIFF` is removed and warns once if set.
- `ACD_TRACE=1` writes best-effort JSONL to `<gitDir>/acd/trace/YYYY-MM-DD.jsonl` (or `ACD_TRACE_DIR`) and must never block/abort.
- Useful probes: `acd status --repo .`, `acd events --watch`, `acd logs --repo . --lines 50 --follow`, `acd diagnose --repo . --json`, `acd doctor --repo . --json`, `git status --short --ignored`.

## Harness templates

| Harness | Start | Active | End |
|---|---|---|---|
| Claude Code | `SessionStart -> start` | `Pre/PostToolUse -> start && wake` | `Stop -> flush --logical`; `SessionEnd -> stop` |
| Codex | `SessionStart -> start` | `UserPromptSubmit/PreToolUse/PostToolUse -> start && wake` | `Stop -> touch --soft-boundary` |
| Cursor | `sessionStart -> start` | `postToolUse/afterFileEdit -> start && wake` | `stop -> flush`; `sessionEnd -> stop` |
| OpenCode/Pi | `session.created -> start` | `tool.before/after.* -> start && wake` | `session.idle -> flush --logical`; `session.deleted -> stop` |

- Templates are source of truth. Keep hook helper, setup tests and AdapterE2E synchronized. Active hooks use `start && wake`, log failures and return nonzero; never mask start failures.
- TOML/YAML/shell use leading `# acd-managed: true`. JSON must be schema-clean and is detected by command signatures; top-level `_acd_managed` is legacy-only.
- Codex uses `~/.codex/hooks.json` with only root `hooks`; legacy TOML duplicates events. Keep `[features].hooks=true` if pinned; `features.codex_hooks` is deprecated.
- Codex Stop uses `acd touch --soft-boundary` because it fires every turn; the boundary triggers evaluation without bypassing safety gates. Changes require `/hooks` re-approval. `setup codex --raw > ~/.codex/hooks.json` replaces the file; merge custom hooks.
- Codex extracts `session_id cwd?` from stdin; missing cwd falls back to `$PWD`. Missing `acd` stays fail-soft.
- Cursor installs only to `~/.cursor/hooks.json`; `--raw` replaces it, so merge custom hooks and re-approve in Settings -> Hooks.
- `flush --logical` refreshes heartbeat, enqueues `flush_logical` and signals SIGUSR1. Only a drained logical flush bypasses intent batch wait; it requires a registered active session and reports refusal without blocking the harness.

## Environment

Environment variables remain compatible. `acd settings` can persist explicit
non-secret values and shows when they shadow environment values. Hot settings
activate at the next safe work boundary; fields labeled `restart required`
need an explicit daemon restart. API keys remain environment only.

| Group | Variables and defaults |
|---|---|
| Repo | `ACD_REPO_AUTODISCOVERY=enabled`; durable override `${XDG_CONFIG_HOME:-$HOME/.config}/acd/config.json` |
| Trace | `ACD_TRACE=off`; `ACD_TRACE_DIR=<gitDir>/acd/trace`; `ACD_AI_PROMPT_TRACE=off` (sensitive) |
| Shadow/recovery | `ACD_SHADOW_RETENTION_GENERATIONS=1`; `ACD_REWIND_GRACE_SECONDS=60`; `ACD_KEEP_DEAD_BRANCH_BARRIERS` disables dead-ref recovery sweep |
| Capture | `ACD_MAX_FILE_BYTES=5 MiB`; `ACD_MAX_PENDING_EVENTS=50000`; `ACD_PATH_QUIESCENCE_SECONDS=0`; `ACD_EVENT_RETENTION_DAYS=7`; `ACD_SENSITIVE_GLOBS`; `ACD_SAFE_IGNORE`; `ACD_SAFE_IGNORE_EXTRA` |
| AI | `ACD_AI_PROVIDER`: `deterministic`, `openai-compat` or `subprocess:<name>`; `ACD_AI_BASE_URL=https://api.openai.com/v1`; `ACD_AI_MODEL=gpt-5.4-mini`; `ACD_AI_TIMEOUT=30s`; `ACD_AI_API_KEY`; `ACD_AI_CA_FILE`; `ACD_AI_DIFF_EGRESS=off`; `ACD_COMMIT_FORMAT`: `imperative` or `conventional`; `ACD_INTENT_REJECTS_RAW=off` |
| Intent | `ACD_COMMIT_STRATEGY`: `event` or `intent`; `ACD_INTENT_WINDOW=10`; `ACD_INTENT_MIN_PENDING=10`; `ACD_INTENT_SETTLE_WINDOW=10s`; `ACD_INTENT_MAX_PENDING_AGE=5m`; `ACD_INTENT_RECENT_COMMITS=5`; `ACD_INTENT_DEFER_LIMIT=1`; `ACD_INTENT_RETRY_ON_INVALID=2`; `ACD_INTENT_PATH_COALESCE=off`; `ACD_RECENT_COMMIT_AFFINITY_SECONDS=0` |
| Watch/client | `ACD_FSNOTIFY_ENABLED=off`; `ACD_DISABLE_FSNOTIFY` forces poll; `ACD_MAX_INOTIFY_WATCHES`; `ACD_CLIENT_TTL_SECONDS=1800` |

Canonical details: `docs/overview.md`, `docs/capture-replay.md`, `docs/settings.md`, `docs/intent-commit-flow.md`, `docs/ai-providers.md`, `docs/user-workflows.md` and `docs/rewrite-commits.md`.
