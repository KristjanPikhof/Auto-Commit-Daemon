# Changelog

## Unreleased

## v2026-08-25

### Changed

- Race tests now split the two largest packages into isolated processes and
  spread repeated stress checks across CI runners. Timing-sensitive cases still
  run with the race detector, but without competing workloads.
- Intent planning now validates a local hard-dependency baseline before it
  reserves a provider attempt. Native v2 providers can refine that baseline,
  while invalid local state records a skipped call with its finding codes.
- Planner diagnostics now separate provider attempts from replay-error repeats
  and report preflight, cache rebuild, and provider-skip outcomes.
- Updated Lip Gloss to 2.0.6 and ANSI to 0.11.8 for terminal rendering fixes.
  SQLite 1.56.0 includes the upstream journal rollback correction, and CI and
  release workflows now use `actions/checkout` 7.0.1.
- Updated Bubbles to 2.2.0, Bubble Tea to 2.0.9, and SQLite to 1.57.0. The
  terminal updates fix screen cleanup and keyboard restoration, while the
  SQLite update adds optional connection hardening without changing existing
  DSNs.
- `acd setup` now installs or upgrades the shared runtime without enabling the
  current repository. Run `acd on` in each repository you want to protect; it
  registers, migrates, and enables only that repository. Other mutation
  commands stop and ask for setup when the managed runtime is out of date.

### Fixed

- Intent planning now removes dependency edges after their captures leave the
  active window. Long-lived branches no longer exhaust the 4,096-edge limit
  and stall `commit-all` on already-published history.
- Unordered soft dependency evidence is now normalized before it reaches the
  publication graph. References to earlier generated artifacts no longer stop
  `commit-all` with a reverse-edge error.
- Published-event retention now keeps captures while a restore checkpoint is
  live and retires their membership after its private ref is pruned. Cleanup
  no longer loops on foreign-key errors or leaves stale dependency history.
- Publication barriers now choose the newest eligible checkpoint even when a
  migration resets observation epoch numbering. An older high-numbered
  checkpoint can no longer cause a false `HEAD` mismatch.
- Forced `commit-all` no longer reuses a cached non-forced waiting plan. Plan
  identity now covers the complete planning snapshot, including forced aging
  and hashed captured diffs.
- Invalid planner output now keeps proven groups, repairs safe dependency and
  forced-aging findings locally, and returns to a validated local grouping
  after bounded corrections. Recoverable preflight failures enter local drain
  recovery instead of immediately requiring manual action.
- Publication barriers now require a new completed checkpoint for the exact
  worktree, branch, observation, and `HEAD`. A same-branch commit can no longer
  make stale checkpoint proof eligible for publication.
- ACD now completes stale publication runs when every frozen event is already
  published or safely recovered. Workers reconcile them during startup and
  normal recovery, while `acd support recover` provides the same repair when a
  worker cannot run.

## v2026-08-18

### Added

- `acd setup` now guides a fresh user through Everyday or Maximum Speed,
  imperative or Conventional Commit messages, and a local or OpenAI-compatible
  provider. It reviews and tests the exact provider with synthetic content
  before the rollback-safe install starts.
- First setup can store an OpenAI-compatible bearer token in the protected user
  credential file, honor `ACD_AI_API_KEY`, approve warned HTTP endpoints, and
  save fingerprint-bound global defaults for current and future repositories.
- ACD now creates rootless checkpoint commits and GC-safe private refs before
  work that can change repository or service state. It records checkpoint
  coverage, publication links, retention, and data needed for crash recovery.
- `acd restore` now starts with a mandatory preview. It records the filesystem
  exactly before and after the restore, leaves `HEAD` and the index alone, and
  supports undo, rollback, and forward repair.
- Setup, migration, and uninstall now run as transactions with rollback and an
  isolated self-test. Registry v2 identifies Git common directories and linked
  worktrees, while state schemas v20 through v23 store operations, checkpoints,
  publication drains, adaptive Intent planning runs, and resolved semantic
  plans. Uninstall keeps
  repository data unless the user separately confirms a purge.
- A single user-level supervisor now owns one worker per Git common directory.
  macOS terminals and agent applications share it through peer UID checks, with
  no Full Disk Access requirement. Linux continues to use systemd.
- Intent publication drains now survive disconnects and daemon restarts. They
  freeze the approved target, retain checkpoint membership, and stop only when
  publication finishes or reaches a proven safety block.
- Adaptive Intent planning now records attempts and outcomes across restarts.
  It keeps valid groups while unresolved captures are replanned and records a
  completed outcome even when provider cooldown prevents a remote call. A
  completed plan is revalidated and reused while its fingerprint is unchanged.

### Changed

- `acd list` is again the compact live dashboard for protection health and
  commit progress. It keeps repositories that need action or are working on
  screen, fills the five-row default view with recent activity, and uses paused
  repositories only when space remains. `SAFE`, `DRAIN`, `LEFT`, and `STATUS`
  replace the registration-oriented table; `--all` shows every enabled
  repository and `--verbose` adds operational details. `acd repo list` remains
  the static maintenance inventory.
- Dashboard collection now reads the registry and supervisor once per frame,
  then reads repository databases concurrently with strict time limits and no
  Git scans or migrations. Slow repositories cannot hold up the full view, and
  refresh timing starts after each completed frame. JSON output remains
  exhaustive and adds worker, operational, blocker, activity, and publication
  drain fields without removing existing fields.
- The public CLI now has ten root commands, five product states, one JSON result
  envelope, and hidden compatibility aliases. Help, configuration, status, and
  rewrite output now use direct wording and give the next safe action.
- `acd on` now checkpoints the repository, replaces the managed worker, waits
  for readiness, and runs safe exact-chain recovery. Forward schema upgrades
  must pass the checkpoint barrier first. `acd off` waits for the worker to
  stop, and doctor reports the worker error with the matching repair command.
- `acd commit-all --yes` now checkpoints staged state before using it, keeps the
  worktree unchanged, and follows one frozen target until it completes or hits
  a safety block. Background publication still never consumes staged changes.
- New repositories use deterministic Intent/Fast with structural verification.
  This default needs no provider credential or diff egress. The v19 to v20
  cutover preserves the effective publication settings of existing repos.
- Intent planning waits for the repository settle window, then allows one
  effective remote correction. It repairs only proven missing dependencies and
  falls back to validated local groups when the response remains invalid.
  Logical flushes, durable boundaries, forced aging, and maximum age can still
  release a window.
- Status, diagnose, and doctor now separate active recovery from a real safety
  block. They report checkpoint progress, publication drains, planner attempts,
  local fallback, staged consent, worker health, and sanitized errors. A fresh
  worker completing its checkpoint is shown as normal progress.
- Session-start and active integration hints now fail closed. Idle, stop, and
  close hints log the real failure but return success. Filesystem polling
  remains the protection path when a hint is missing.
- Linked worktrees now keep planner rejects in their own Git directories, and
  shared logs identify both the worktree and Git directory. Exact-chain repair
  checkpoints each enabled worktree, stops the shared worker for the repair,
  and restores protection afterward.

### Fixed

- Fixed deterministic Intent fallback collapsing valid published candidates
  when new captures had a same-file hard dependency. ACD now offers an eligible
  private suffix to one bounded semantic repair replan. If replanning fails, it
  preserves the existing commit OIDs and publishes only the new captures after
  generating a meaningful locked message. Provider outages leave the new
  candidate waiting across restarts instead of publishing `Update <path>`.
- Setup can reuse checkpoint ownership from an earlier attempt when the same
  event appears in a retained recovery snapshot. Re-running setup no longer
  fails because that event already belongs to a checkpoint.
- Compatible runtime replacement is journaled and bounded, so ordinary source
  rebuilds do not repeat every repository migration scan and self-test.
- Intent drains now distinguish queued work from real progress. An expired or
  unsafe history repair retires only the blocking candidate, keeps published
  commits unchanged, and releases its pending events for forward recovery.
  Large queues can no longer prevent recovery just because another replay pass
  is available.
- Forward recovery now returns to pending-only Intent planning after every
  local unlock. When planning stalls, ACD publishes the smallest safe hard
  dependency component, including a single event when needed, then replans the
  remaining target from the new `HEAD`. An unavailable provider keeps recovery
  local until its circuit allows another probe.
- Recovery now resumes blocked publication drains and follows recaptured events
  across rewritten bases. Repeated transitions that already reached their
  recorded result are accepted only after the existing proof checks pass.
- The supervisor now removes deleted worktrees only after proving that their
  ACD state has no unresolved protected work. Unsafe stale registrations are
  disabled instead of restarting missing workers, and manual registry cleanup
  uses the same proof.
- `acd history rewrite` now trusts the disabled repository lifecycle and the
  canonical writer lock instead of stale daemon modes or reused PIDs. Apply
  holds exclusive ownership through Git and state reconciliation, then repairs
  stale running metadata only after proving that ACD is off.

## v2026-08-07

### Fixed

- Fixed settings authoring and previews resolving repository, profile, and
  global scopes against the wrong lower-precedence layers. Persisted fields
  now share validation and encoding before they are saved.
- Fixed duplicate file signatures allowing more than one deletion to pair
  with the same rename target. Rename classification now remains deterministic
  without reusing a matched destination.
- Fixed sensitive-path checks using separate configuration paths for one-off
  and daemon matchers. A nonempty `ACD_SENSITIVE_GLOBS` value now consistently
  replaces the default deny and allow lists.
- Fixed `acd rewrite-commits` ignoring saved repository, profile, and global
  settings. Plan generation now uses the effective configured strategy,
  provider, model, format, timeout, and protected credential instead of reading
  only compatibility environment variables.
- Isolated verification subprocess environment directories from repository and
  Git ancestry. Temporary Git-negative tests can no longer discover the
  candidate repository through `TMPDIR`.
- Fixed `acd configure` appearing blocked after final approval. It now reports
  numbered progress, exposes sanitized verification failure output, and avoids
  delayed terminal capability replies corrupting short-terminal shell prompts.
- Removed the regular wizard's free-form verification command question.
  Everyday runs no project command; repository Strict Review detects a full
  command and asks only for exact approval in the final preview.
- Fixed Intent Everyday getting stuck in `needs_attention` after successful
  setup. Structural verification now relies on ACD's atomicity and
  materialization gates instead of waiting for a repository command.
- Fixed planner fallback repeatedly rejecting hard-linked work from an
  existing candidate. Balanced and Fast now continue or merge the persisted
  candidate without losing dependency order or leaving it permanently waiting.
- Fixed a hard dependency bridge joining two recent candidates. ACD now keeps
  one canonical candidate, records the merged lineage, and can rebuild a
  private suffix when one semantic candidate owns non-contiguous old commits.
  Reordered candidates must be visible in the current dependency evaluation,
  remain structurally independent, and pass exact pre-CAS verification.
- Fixed repeated replay failures appearing as active health. Status, diagnose,
  doctor, and events now report the bounded error, repeat count, blocked
  capture, candidate IDs, and fallback size. Identical log lines are rate
  limited and recovery is reported explicitly.
- Fixed Balanced fallback splitting a source change from its exact test or
  migration companion when the planner window ended between them. One-to-one
  persisted companions now rejoin the candidate and use bounded repair;
  ambiguous matches stay pending for planner review.

### Added

- Added global Everyday work and Maximum speed setup experiences with adaptive
  provider reuse, first-time endpoint and model prompts, and one
  fingerprint-bound approval. Repository setup additionally offers Strict
  Review and `--wait`.
- Added durable background configuration validation. Capture remains active
  while replay and repair wait, and failed checks preserve the desired
  revision, last-known-good runtime, sanitized output, and retry state.
- Added Intent v2 semantic candidates, dependency-aware non-contiguous
  grouping, explicit atomicity gates, exact candidate-tree verification, and
  preset-aware planner fallback.
- Added versioned Fast, Balanced, and Quality presets for Event and Intent
  strategies. Runtime revisions retain preset identity, version, and
  customization state.
- Added `acd configure` for guided strategy, preset, provider, credential,
  diff-consent, verification, repair, and activation setup. Dry-run previews
  the complete transaction without calls, commands, writes, starts, or hook
  changes.
- Added protected XDG credential storage and `acd auth set|status|remove`.
  `ACD_AI_API_KEY` remains higher priority, and secrets stay out of settings,
  SQLite, logs, traces, status, diagnostics, and fingerprints.
- Added bounded automatic repair for eligible private ACD commit suffixes.
  Repair uses backup refs, atomic ref compare-and-swap, durable old-to-new
  mappings, restart reconciliation, and strict staging and ref-containment
  checks.
- Added durable candidate lineage for dependency-driven merges. The v17 ledger
  keeps source candidate status and published commit identity without storing
  raw diffs.
- Added a crash-recoverable self-publication journal. ACD records exact event
  and candidate ownership before changing a branch, then can reconcile the
  `prepared`, `git_applied`, and `completed` phases after restart.
- Added one canonical-writer fence under the Git common directory. Linked
  worktrees and state-directory replacements cannot start a second ACD writer,
  and status, diagnose, and doctor report ownership, publication phase,
  heartbeat, wake progress, and non-destructive remediation.
- Added native Intent v2 protocols for OpenAI-compatible and capability-aware
  subprocess providers. Legacy subprocess results remain available through
  the reported `v1_compat` adapter.

### Changed

- Consolidated duplicated scheduling, capture, replay, AI response,
  verification, settings, and self-publication recovery paths so shared
  outcomes are finalized consistently. Added regression coverage for replay
  probe timeouts in Event and Intent modes.
- Made bare `acd configure` a one-time global setup with Everyday and Maximum
  Speed only. Everyday uses internal structural gates and never detects or
  runs project tests; Strict Review and full command validation require an
  explicit `--repo`.
- Added fingerprint-bound global setup approvals so untouched repositories can
  inherit reviewed provider, diff-egress, and repair permissions without
  globally authorizing repository-controlled shell commands.
- Existing Intent repositories migrate to `intent.balanced@3`. Missing
  provider, diff-context, credential, or approved verification prerequisites
  now stop replay with `needs_attention` while capture continues. ACD no longer
  silently resumes v1 or metadata-only Intent planning.
- Intent candidates now survive planner windows and publish in dependency
  order. Same-path and object dependencies remain ordered, while independent
  `A1, B1, A2` sequences can publish as `A=[1,3]` and `B=[2]` when scratch
  materialization proves independence.
- Balanced fallback now publishes only bounded hard-dependency components or
  an unambiguous test/source or migration companion. Known generated-output
  dependencies remain hard edges. Ambiguous groups, groups above 32 captures,
  and groups above 12 paths stay pending for planner review. Import, symbol,
  hunk, module, activity, and time similarity cannot merge fallback components.
- Increased the default per-request AI provider timeout from 30 seconds to
  5 minutes for slower approved endpoints. Correction retries remain bounded
  by `intent.retry_on_invalid`.
- Codex Stop now records `acd touch --soft-boundary`. Logical flushes remain
  hard evaluation boundaries; neither boundary bypasses atomicity,
  verification, branch, pause, Git-operation, conflict, or replay safety.
- Repository state is now SQLite `SchemaVersion=19`. Version 18 adds the
  immutable self-publication journal without backfilling historical commits.
  Version 19 persists prepare-time candidate completion semantics so restart
  recovery does not reinterpret a landed target from current settings.
  Pre-v18 status, diagnose, and doctor reads report self-publication as
  unavailable without migrating or writing the database.

## v2026-07-26

### Changed

- Reworked the README workflow diagram to explain capture, event and intent
  grouping, safe replay, and automatic recovery in one first-look flow. Added
  a matching standalone Mermaid source for reuse.
- Updated pinned `actions/setup-go` from `v6.5.0` to `v7.0.0` across CI,
  CodeQL, and release workflows.
- Updated maintenance dependencies: `modernc.org/sqlite` from `v1.53.0` to
  `v1.54.0` (SQLite 3.53.3), and `github.com/mattn/go-isatty` from `v0.0.22`
  to `v0.0.24`.

### Fixed

- Recovery now preserves relevant earlier captures published after the first
  unresolved base advanced, follows transitive rename dependencies, and
  strips the operation prefix already represented by the recovery seed.
  Context discovery is a linear path-closure scan bounded to 4,096 events,
  4,096 traversed ancestry commits, and 64 remaining tree proofs.
  Published-event pruning preserves the same closure while isolating
  oversized exact pairs. This prevents false before-state mismatches without
  correlated ledger scans or unbounded Git proof fan-out.

## v2026-07-24

### Added

- Added the Go-native `acd settings` configuration lab with rich and accessible
  terminal modes, repository/global/profile scopes, source and shadow labels,
  strict synthetic provider testing, and explicit risk confirmations. The TUI
  pins Bubble Tea 2.0.8, Bubbles 2.1.1, Lip Gloss 2.0.5, and Huh 2.0.3 while
  preserving static CGO-disabled builds.
- Added immutable desired/applied config revisions, safe-boundary activation,
  last-known-good revert, bounded experiments, and descriptive revision/profile
  comparisons. The runtime settings ledger uses SQLite `SchemaVersion=14`.

### Changed

- The settings lab now starts accessible sessions with **Test current
  settings**, offers a short **Quick provider setup** before **Advanced
  settings**, accepts both `t` and `T`, and resolves provider-risk confirmations
  inside the current keyboard-only session. Synthetic tests no longer require
  activation-only diff egress consent.
- `acd status` and `acd diagnose` now add saved generation, desired/applied/
  last-known-good revision, profile, activation failure, boundary, and
  experiment progress without changing existing fields or pre-v14 read-only
  behavior.
- Provider, model, commit, and intent settings can now be saved without shell
  sourcing. Existing environment variables remain compatible, and API keys
  remain environment only.

### Fixed

- No-session `acd stop` now reads daemon control state without opening the
  migration path, so an older binary can safely stop a newer daemon without
  altering its repository database. Session-aware stops remain schema-aware
  because they update client rows.
- Saved restart-required settings now resolve repository, profile, global, and
  environment precedence when the daemon starts, including raw sensitive path
  filters and repository-specific client TTL.
- Rejected runtime revisions now restore the last-known-good bundle after a
  daemon restart. Active experiments block unrelated apply/revert requests and
  fail atomically before queuing their baseline cleanup.
- The settings UI now tests cleared overrides with their inherited value,
  preserves unrelated dirty drafts, refreshes runtime state on a timer, and
  clears dirty state after a successful experiment start.
- Provider setup now falls back before loading a custom CA when the API key is
  missing, closes custom transports, and sanitizes wrapped cancellation and
  deadline errors without breaking `errors.Is`.
- Bare health keeps backpressure and terminal replay barriers visible during
  rewind grace; `waiting` now covers both intent batching and rewind grace.
- Recovery now limits published context to suffix paths and groups
  noncontiguous members by commit, preventing false provenance mismatches
  after intent-mode deferrals and branch rewinds.

## v2026-07-13

### Added

- Bare `acd` now gives a read-only health summary and one recommended next
  action. `acd on` and `acd off` provide idempotent everyday controls while
  preserving per-repo state.
- Intent mode now persists planner circuit health. Transport failures open the
  circuit immediately, repeated invalid plans open it after three failures,
  and deterministic planning keeps commits moving during 30-second, 2-minute,
  and 10-minute cooldowns.
- Recovery snapshots and hidden `refs/acd/recovery/*` refs now preserve exact
  unpublished branch-generation chains before ACD changes their queue state.

### Changed

- Replay, branch transitions, dead-branch cleanup, `acd fix`, and `commit-all`
  now reconcile whole unpublished chains. A stable exact `HEAD` match is proven
  as published; any unresolved chain is archived without changing the live
  worktree, index, or branch.
- `acd fix --force` now selects archive-only recovery. It no longer purges
  terminal barriers or retargets captures to a different branch generation.
- `acd status` and `acd diagnose` now show planner circuit state, failures,
  deterministic bypasses, and the next automatic provider probe.
- Updated pinned GitHub Actions dependencies: `actions/setup-go` from `v6.4.0`
  to `v6.5.0`, and `goreleaser/goreleaser-action` from `v7.2.2` to `v7.2.3`.
- Updated `golang.org/x/sys` from `v0.46.0` to `v0.47.0`.
- Raised the minimum Go toolchain to 1.26.5 to include the standard-library
  fix for GO-2026-5856.

### Fixed

- `commit-all --dry-run`, JSON consent refusal, and declined confirmation now
  leave ACD state and recovery refs unchanged and do not start the AI provider.
- `acd fix` backups now include committed SQLite WAL frames and pass
  `PRAGMA quick_check` before recovery mutation continues.
- `acd fix` and `commit-all` now recheck branch, Git-operation, and manual-pause
  safety at mutation boundaries instead of relying on an earlier plan snapshot.
- `commit-all` now exits non-zero when unpublished events remain, instead of
  reporting a partial drain as successful.
- Intent `commit-all` now reuses the planner circuit across replay passes, so a
  provider outage falls back once instead of repeating the remote timeout.
- Recovery now proves grouped same-path commit prefixes cumulatively and keeps
  each hidden evidence ref locked through its SQLite lifecycle transition.
- Published-event retention now preserves same-base recovery prefixes. Schema
  v13 adds a covering index so the safety check remains bounded at ledger cap.
- Recovery now treats captured filenames as literal Git pathspecs without
  trimming legal whitespace or interpreting pathspec-magic characters.
- Planner circuit state now initializes only after daemon-lock ownership,
  caller cancellation wins provider-error races, and persisted errors redact
  URL paths that may contain credentials.
- Daemon shutdown now cancels and joins the asynchronous startup recovery sweep
  before releasing the daemon lock or returning database ownership.

## v2026-06-26

### Added

- Intent mode now has a short `ACD_INTENT_SETTLE_WINDOW` burst delay after the
  pending-count gate, so rapid related edits can reach one planner-visible
  window before AI grouping runs.
- `acd events --json`, `acd status --json`, and `acd diagnose --json` now expose
  privacy-safe planner-window summaries showing offered, selected, deferred,
  forced, hidden, and grouped seqs without requiring prompt-trace parsing.

### Changed

- Intent planning can partition one visible window into multiple ordered commit
  groups, letting unrelated close-together changes and independent same-file
  edits publish as separate atomic commits.
- The intent planner prompt now explicitly asks for ordered `commit_groups`
  when one visible window contains multiple independent commit intents.
- Consecutive same-path captures are planner-visible by default. Set
  `ACD_INTENT_PATH_COALESCE=1` to restore the legacy folding behavior.
- Updated maintenance dependencies: `modernc.org/sqlite` from `v1.52.0` to
  `v1.53.0`, and pinned `actions/checkout` from `v6.0.3` to `v7.0.0`.

## v2026-06-21

### Changed

- Updated pinned GitHub Actions dependencies: `actions/checkout` from `v5.0.1`
  to `v6.0.3`, and `goreleaser/goreleaser-action` from `v6.4.0` to `v7.2.2`.
- Raised the minimum Go toolchain to 1.26.4 for building from source and
  updated pinned dependencies, including `modernc.org/sqlite v1.52.0`, as part
  of a security and toolchain maintenance bump.

### Fixed

- Codex 0.141+ rejects unknown top-level hook fields, so JSON hook templates for
  Codex, Claude Code, and Cursor no longer emit `_acd_managed`. ACD now detects
  JSON installs from ACD hook command signatures while still recognizing legacy
  `_acd_managed` files for migration and doctor guidance.
- `acd doctor` now reports schema-clean Codex and Claude Code installs as
  drifted when their active hooks are missing, instead of treating a
  `SessionStart`-only install as healthy.
- JSON harness uninstall docs now match all ACD lifecycle hooks, including
  stop/flush/touch commands that do not carry a `--harness` flag.
- The same-SHA rewind regression test now uses explicit daemon wakes instead of
  idle ticks, avoiding slow Ubuntu race-test timing failures while keeping the
  assertion focused on cross-tick rewind detection.

## v2026-06-06

### Added

- Optional `ACD_COMMIT_FORMAT=conventional` output for scope-less Conventional
  Commit subjects, while keeping the existing imperative format as the default.
- Commit format validation and deterministic fallback handling across event,
  intent, provider, subprocess, and rewrite-plan flows.

## v2026-06-05

### Added

- Per-repo lifecycle controls: `acd repo disable`, `acd repo enable`,
  `acd repo manage`, and `acd list --interactive`. Disabled repos preserve
  `.git/acd` state, are hidden from normal `acd list` snapshots, and make hook
  calls skip cleanly with `repo_disabled`.
- `acd diagnose` and `acd fix` now surface tracked generated-cache delete
  floods, clean the ACD queue without mutating Git, and print explicit
  `git add -u` plus `git commit` follow-up commands for the repository cleanup.

### Fixed

- Capture now protects deleted tracked files under safe-ignore generated trees
  instead of queuing delete floods, and reconciles shadow state so the same
  generated deletes do not reappear on the next scan.

## v2026-06-01

### Changed

- `acd rewrite-commits` now has explicit `--from-sha`, `--from-nr`,
  `--range-nr`, and `--range-sha` selectors, plus stderr-only progress modes
  for plan generation and apply.

## v2026-05-29

### Changed

- README and the docs folder were rewritten into shorter, task-focused guides
  with diagrams and clearer recovery paths.
- OpenAI-compatible provider defaults now use `gpt-5.4-mini`.
- `ACD_INTENT_RETRY_ON_INVALID` now accepts a retry count and defaults to `2`,
  giving invalid intent plans one more correction attempt before fallback.
- `acd list` now shows the intent batch-wait countdown in `wait` rows, so
  sparse queues show when the age trigger will commit without extra commands.

### Fixed

- `acd fix --force` now purges failed replay barriers with pending successors,
  so stuck queues can recover through the supported `acd fix` flow instead of
  the hidden `purge-events --failed` compatibility command.

## v2026-05-28

### Changed

- **`acd list` (breaking on TTY):** Interactive terminals default to a live
  compact dashboard (`REPO`/`DAEMON`/`PEND`/`BLK`/`HEAD`/`STATUS`). `REPO` is
  the last two path segments (e.g. `Development/Auto-Commit-Daemon`); duplicate
  labels get a `#` hash suffix. Compact status tokens include `blk`, `wait`,
  `miss`, `bad`. Pipes and non-TTY stdout get a one-shot compact table. Use
  `--once` for one snapshot on a TTY, `--verbose` for the wide table, `--json`
  for JSON (one-shot on TTY). `--watch` is an explicit alias. `--watch` with
  `--json` is rejected.
- **`acd rewrite-commits --plan-only`:** Ends with `Plan saved. Git history
  unchanged.` and a `Next:` block (`--show-plan`, `--apply-plan --dry-run`,
  `--apply-plan --yes`). Declining apply prints `No rewrite performed.`

### Added

- **Cursor harness:** `acd setup cursor` prints user-global install steps;
  `acd setup cursor --raw > ~/.cursor/hooks.json` writes strict JSON for a
  fresh install. Cursor hook commands are inline in `hooks.json`, so no helper
  script is required. Redirecting `--raw` output **replaces the entire hooks file**;
  merge the five lifecycle events manually when you already have custom hooks.
  Approve hooks in Cursor **Settings → Hooks** after install.

### Fixed

- Daemon shutdown now reports stopped-state persistence failures and lifecycle
  tests read stopped state through a fresh DB handle, reducing macOS CI flakes.

## v2026-05-25

### Added

- Homebrew tap support now allows installing `acd` with
  `brew install KristjanPikhof/tap/acd`.
- `acd rewrite-commits` now offers reviewable AI-assisted commit-message
  rewrites for current-branch linear history, with saved plans, dry runs,
  backup refs, and recovery guidance.

### Fixed

- OpenAI-compatible responses now reject unexpected tool calls instead of
  accepting the wrong function output.

## v2026-05-20

### Changed

- Recovery views now separate waiting queues from blocked repos, so `acd list`,
  `acd status`, and `acd diagnose` point to the right next command.
- Intent mode now rewrites generic planner commit messages while preserving the
  accepted grouping, and status/diagnose expose message-quality rewrite and
  fallback reasons.
- Forced-aging singleton intent windows now run through non-deterministic
  providers and the message-quality gate, falling back deterministically when
  rewrite or provider output is unsafe.
- Intent message-quality checks now catch token-only and filename-only subjects,
  and prompt tracing includes intent rewrite requests.

### Fixed

- `acd fix --force --yes` now self-verifies cleanup and reports remaining
  blockers instead of looking successful when a barrier still needs attention.

## v2026-05-18

### Added

- Explicit repo lifecycle commands: `acd repo init`, `acd repo list`, and
  `acd repo remove`. Autodiscovery stays on by default, but can be disabled
  with `~/.config/acd/config.json` or `ACD_REPO_AUTODISCOVERY`.
- Singleton intent windows now use the per-event commit-message provider,
  keeping one-capture commits cheaper while still emitting
  `replay.intent.singleton_shortcircuit` traces.

### Changed

- AI-generated commit messages now use a semantic subject plus wrapped `- `
  context bullets.
- Plain `acd wake` now only refreshes heartbeat and nudges capture/replay. Use
  `acd flush --logical` to force the current intent window through the batch
  gate.
- Hook wake acknowledgements moved to debug logs with periodic summaries.

### Fixed

- Planner overlap between `selected_seqs` and `deferred_seqs` is now rejected
  reliably, retried with a typed error, then safely falls back with reject
  logging.
- Intent grouping rationale stays in `grouping_reason` instead of leaking into
  commit bodies.
- Planner reject logging is wired at daemon startup, including 5 MiB rotation.

## v2026-05-16

Intent planner reliability, explicit flush boundaries, and safer recovery.

### Added

- `acd flush --logical` forces the current intent window through the batch
  gate for registered sessions. Claude Code, OpenCode, and Pi idle hooks now
  use it; Codex Stop stays on `acd touch`.
- Intent mode now handles same-path coalescing, retry-on-invalid planner
  output, forced singleton commits, and typed planner validation errors.
- Planner rejects are written to `<gitDir>/acd/planner-rejects.jsonl` with
  5 MiB rotation. Raw model output is redacted unless
  `ACD_INTENT_REJECTS_RAW=1`.
- New tuning knobs: `ACD_PATH_QUIESCENCE_SECONDS`,
  `ACD_RECENT_COMMIT_AFFINITY_SECONDS`, `ACD_INTENT_PATH_COALESCE`, and
  `ACD_INTENT_RETRY_ON_INVALID`.
- `acd status --json` and `acd diagnose --json` now report planner error
  rates, singleton commit rates, intent diff cap, and path-quiescence gates.

### Changed

- `ACD_INTENT_DEFER_LIMIT` now defaults to `1`; set it to `2` to restore the
  previous tolerance.
- Intent planner requests get a dedicated 16 KiB diff budget through
  `ai.IntentStageDiffCap`; per-event commit messages still use the smaller
  event diff cap.
- Missing deferred reasons are normalized into visible decision records, and
  spurious deferred-reason entries are dropped with one warning.

### Fixed

- Path quiescence now preserves FIFO ordering, checks every path in multi-op
  captures, avoids extra metadata writes, and keeps its tracker bounded.
- `acd flush --logical` now requires an existing registered client; unknown
  sessions return `refused_reason=unknown_session`.
- Planner-reject logging now redacts raw responses by default and rotates
  atomically.
- Forced singleton plans now run the same safety validation as planner output.
- `acd status` no longer over-subtracts stale path-quiescence counts, and
  planner-error warnings wait for a full 100-decision window.
- Blocked replay barriers self-heal when the captured after-state already
  matches `HEAD`, recording `handled_external_after_block`.
- Codex install detection now includes repo-local `.codex/hooks.json` and
  `.codex/config.toml`.

### Recovery

- `acd fix` is the recovery entrypoint. `--yes` applies safe repairs, and
  `--force` can purge replay barriers with pending successors. The command
  backs up `state.db` and refuses to run while a live daemon owns it.
- `acd diagnose --json` reports `auto_resolvable_blocked_count` and
  `barrier_with_successors_count`.

### Deprecated

- `acd recover` and `acd purge-events` remain hidden, deprecated aliases.
  Use `acd fix` and `acd fix --force` instead.

### Docs

- README now covers recommended configs, recovery, migration, and the new
  environment variables.

## v2026-05-13

### Added

- `acd start`, `acd status`, `acd diagnose`, `acd events`, `acd logs`,
  `acd prompt`, `acd recover`, and `acd daemon run` now resolve subdirectories
  to the canonical Git worktree root via a shared `git.ResolveWorktree` helper.
  Starting ACD from `repo/subdir` refreshes the existing repo daemon and
  central-registry row instead of creating a duplicate subdirectory entry.
  Non-Git directories are refused with a clear error (`ErrNotWorktree`).
- `acd start` gains a registry-backed early short-circuit that resolves
  canonical repo identity from a fresh central-registry row plus the session's
  start-cache before invoking `git rev-parse`. Existing path-vs-hash lookup
  also prefers exact path matches so stale legacy duplicates no longer shadow
  the canonical row.
- `acd gc` now merges legacy duplicate registry rows that share a Git
  toplevel or `state.db` before pruning. `acd gc --json` adds a `merged[]`
  array of `{kept_path, dropped_path, reason}` entries alongside the existing
  `dropped[]` and `kept` fields.
- `acd setup opencode` and `acd setup pi` now print the canonical default hook
  paths (`~/.config/opencode/hook/hooks.yaml` and
  `~/.pi/agent/hook/hooks.yaml`). `acd doctor` detects acd-managed hooks at
  those paths and points remediation guidance at them. Older hook paths
  (`~/.config/opencode/hooks.yaml`, `~/.pi/hook/hooks.yaml`) remain detected
  as a secondary fallback during migration.
- `acd doctor` now surfaces the actual file that carries the acd marker when
  it differs from the canonical primary. JSON adds a `matched_path` field on
  each harness report; human output prints a `marker found at` line. Drift
  scans run against the matched file, and remediation switches to a
  merge-only nudge toward the canonical path so users on a legacy fallback
  are never told to overwrite a non-canonical file they hand-authored.
- `README.md`, `templates/opencode/README.md`, and `templates/pi/README.md`
  now document the `# acd-managed: true` marker requirement and explain how
  to recover when `acd doctor` reports detection as `no` despite the hook
  file existing. The README harness table splits "hook support" (native vs
  external engine) from install location for clearer onboarding.

## v2026-05-10

### Added

- `acd diagnose` now reports the most recent dead-branch cleanup. JSON output
  includes `dead_branch_prune_last_run_ts`,
  `dead_branch_prune_last_count`, and `dead_branch_prune_last_refs`; human
  output prints a `Dead-branch prune:` row after a cleanup removes rows.

### Fixed

- Deleted feature branches no longer leave phantom replay blockers behind.
  ACD prunes stale `pending`, `blocked_conflict`, and `failed` rows for dead
  branch refs during runtime branch changes and daemon startup. Paused repos
  are left untouched, and `ACD_KEEP_DEAD_BRANCH_BARRIERS=1` keeps the old rows
  for forensic inspection.

## v2026-05-08

### Added

- `acd commit-all` can capture and replay a dirty worktree without starting the
  persistent daemon. It is meant for cold starts, repos where the daemon was off,
  and onboarding existing work into ACD history. It supports `--dry-run`, `--yes`,
  `--json`, and `--repo`, refuses unsafe git states, and uses the active commit
  strategy instead of adding a separate strategy flag.
- `acd start` short-circuits repeated active-hook calls through a per-session
  cache file at `<gitDir>/acd/start-cache-<sessionhash>.json`. The hot path
  returns in roughly 50 ms instead of the cold-path budget of 1 s, while the
  cold path is unchanged. The cache stores a daemon fingerprint (start time
  plus argv hash) so PID reuse on long-running boxes cannot serve a stale fast
  path for an unrelated process. Each hot-path call also refreshes
  `daemon_clients.last_seen_ts` so the refcount sweeper does not evict a
  session that lives entirely on the fast path.
- `acd doctor` flags installed snippet drift. It warns when active hook bodies
  in `~/.claude/settings.json`, `~/.codex/hooks.json`,
  `~/.config/opencode/hooks.yaml`, or `~/.pi/hook/hooks.yaml` lack both the
  `acd start` and `acd wake` calls or the log fallback, and prints
  per-harness remediation pointing at `acd setup <harness>` with merge
  instructions.
- `acd doctor` tails the Codex hook log (last 50 lines or 5 minutes) and
  surfaces the error count plus the first failing line. Output also reports
  config read errors separately from marker-missing on EACCES/EIO via a new
  `config_read_error` field in the JSON view.
- `acd setup <harness> --raw` validates JSON for `.json` snippet targets
  before emitting. Invalid templates exit non-zero with the byte offset
  instead of letting users redirect malformed JSON into
  `~/.codex/hooks.json`.

### Changed

- **Codex hooks v2 is a breaking setup change.** `acd setup codex` now writes
  `~/.codex/hooks.json` instead of the legacy TOML hook snippet. To migrate,
  run `acd setup codex` and merge the printed JSON into `~/.codex/hooks.json`
  manually. If the file contains only the acd block you can redirect directly:
  `acd setup codex --raw > ~/.codex/hooks.json`. **Warning:** the redirect
  replaces the entire file. If you have custom non-acd hooks in
  `~/.codex/hooks.json`, back up the file first and merge by hand instead of
  using `>`. After writing the file, remove the old `# acd-managed: true` block
  from `~/.codex/config.toml`, then approve the new hooks with `/hooks` inside
  Codex. Codex re-flags all hook entries as review-required after every
  `hooks.json` content change, so re-run `/hooks` after any re-install too.
  `acd doctor` now warns when both old and new Codex hook configs are installed,
  because Codex will run both. Codex hooks are enabled by default; if
  `~/.codex/config.toml` pins feature flags, keep lifecycle hooks enabled with
  `[features].hooks = true` (`codex_hooks` is only a deprecated alias). See
  [templates/codex/README.md](templates/codex/README.md) for full details.
- Codex hooks now read `cwd` from hook stdin, no longer require
  `CODEX_PROJECT_DIR`, and use `acd hook-stdin-extract session_id cwd?` for the
  hook payload. The helper also supports multiple fields and optional fields.
  Codex hook bodies capture helper exit explicitly: a failing
  `acd hook-stdin-extract` now logs `[ts] active hook failed exit=N
  cmd=acd-hook-stdin-extract` to the hook log instead of being swallowed by a
  trailing `|| exit 0`.
- Claude Code, Codex, OpenCode, and Pi active hooks now run idempotent
  `acd start` before `acd wake`. ACD can recover after a manual `acd stop`
  without waiting for a brand-new harness session. **Migration:** if you ran
  `acd stop --all` and the daemon does not restart automatically when the next
  prompt or tool event fires, your installed snippet is stale. Re-run
  `acd setup <harness>` and merge the updated hooks block into your installed
  config. **Warning:** redirecting `acd setup <harness> --raw > <config-path>`
  replaces the entire file. Back it up first if you have custom non-acd
  entries. See the per-harness READMEs
  ([claude-code](templates/claude-code/README.md),
  [codex](templates/codex/README.md),
  [opencode](templates/opencode/README.md),
  [pi](templates/pi/README.md)) for merge instructions. Run `acd doctor` to
  identify which harness needs updating.
- Pi SID fallback is now per-process unique (`pi-$$-$(date +%s)`) instead of
  a shared `unknown`. Multiple Pi sessions without `PI_SESSION_ID` no longer
  collapse onto the same client, so the first `session.deleted` does not tear
  down the daemon while other sessions are still active.
- `acd doctor` drift remediation suggests `acd setup <harness>` (merge into
  config) by default, with the `--raw` redirect form only after backing up
  the target file. README and per-harness READMEs for claude-code, opencode,
  and pi now carry the same overwrite warning the codex template already
  shipped.
- `templates/codex/uninstall.md` documents surgical removal of the five acd
  hook entries and the `_acd_managed` flag instead of deleting
  `~/.codex/hooks.json` outright. The previous wording destroyed merged
  custom hooks.
- `acd setup shell --raw` writes a blank line between the direnv and zshrc
  snippets so callers can split them into separate files cleanly.
- `acd status`, `acd diagnose`, and `acd doctor` now report the effective commit
  strategy from daemon metadata first, then `ACD_COMMIT_STRATEGY`, then the
  default. Unknown daemon values fall back to the environment-derived strategy
  and emit a warning instead of leaking into user output.

### Fixed

- `acd commit-all` now force-reseeds `shadow_paths` from `HEAD` and drops
  stale pending capture events before capture. It no longer reports a clean
  worktree when an earlier daemon session had absorbed unreplayed edits into
  shadow state. Human and JSON output now show the reseed note and
  `dropped_stale_pending` count.
- `acd doctor` YAML drift detection now actually fires for OpenCode and Pi
  configs. The previous parser misread nested `actions` items as new
  top-level hook entries, dropped the parent association, and silently
  emitted zero drift for every real config.
- `acd doctor` `parseLogTimestamp` now matches the bracketed timestamp shape
  hook templates write (`[2026-05-08T12:34:56-0700] ...`). The five-minute
  recency window was effectively disabled before because every line fell
  through to the no-timestamp branch and counted as recent.
- `acd doctor` no longer flags JSONL info lines that contain the substring
  `failed` (for example `failed_blocking_pending=0`). Only wrapper-printf
  failure lines and JSONL `level=error|fatal` are counted as hook errors.
- `acd doctor` distinguishes "config unreadable" from "marker missing" on
  EACCES/EIO. JSON consumers can now read `config_read_error` to tell the
  two cases apart.
- The YAML `acd-managed` marker now requires the `# ` comment prefix. A
  hand-edited config containing bare `acd-managed: true` is no longer
  detected as an acd install, and the marker no longer collides with the
  TOML form as a substring.
- `acd start` short-circuit cache writes use unique temp filenames. Two
  concurrent active hooks no longer collide on a shared
  `start-cache.json.tmp` and corrupt the cache file.
- `acd stop` invalidates the start cache on the `Deferred` path and after a
  failed force-stop escalation. A deferred stop no longer leaves a fresh
  cache for a session that was just deregistered, and a daemon that
  survived `--force` no longer leaves stale caches behind.
- Codex `SessionStart` no longer swallows helper failures. A missing `acd`
  binary, oversized stdin, or malformed JSON now surfaces as a logged
  failure with the real exit code instead of a silent skip.

## v2026-05-06

### Added

- AI intent commit strategy: `ACD_COMMIT_STRATEGY=intent` can ask the
  configured AI provider to group related pending captures into one reviewable
  commit while keeping the existing `event` strategy as the default.
- Intent planning controls: `ACD_INTENT_WINDOW`,
  `ACD_INTENT_MIN_PENDING`, `ACD_INTENT_MAX_PENDING_AGE`,
  `ACD_INTENT_RECENT_COMMITS`, and `ACD_INTENT_DEFER_LIMIT` tune planner
  context, offered capture windows, sparse-queue waits, and starvation
  protection for deferred work.
- Opt-in prompt tracing: `ACD_AI_PROMPT_TRACE=1` plus `acd prompt` lets
  operators inspect local event and intent-planner request diagnostics after
  redaction/truncation, with explicit privacy warnings because traces may still
  contain source code.
- Intent grouping observability in `status`, `diagnose`, `doctor`, `events`,
  and `explain`, including grouped event sequences, deferral reasons,
  forced-aging decisions, and planner validation failures.
- Explainable ACD history: `acd events`, `acd explain`, and richer
  `status`/`diagnose`/`doctor` output now show why work was captured,
  committed, skipped, protected, blocked, or handled by external history.
- `acd fix` can plan and apply safe recovery for blocked or failed replay rows,
  stale external-work cases, expired manual pause markers, and drained
  backpressure.

### Changed

- Renamed `acd init <harness>` to `acd setup <harness>` for clarity. `acd init` kept as hidden alias for one release with stderr deprecation warning; reserved `acd init` for future repo-state initialization.
- Claude Code harness snippet now uses the canonical nested hook schema
  (`hooks: [{type: "command", command: "..."}]` per event, with `matcher: ""`
  on `PreToolUse`/`PostToolUse`). Required by the current Claude Code hooks
  engine; the previous flat `command` shape no longer registers. Re-run
  `acd setup claude-code` and replace the old `hooks` block in
  `~/.claude/settings.json`.
- `acd wake` and `acd touch` now exit cleanly when another short-lived control
  caller already holds `control.lock`, instead of failing the harness hook.
  JSON output adds `skipped: true` and `skipped_reason: "control_lock_held"`
  so callers can distinguish a no-op from a real heartbeat or signal. The
  in-flight caller does the equivalent work and the daemon reconciles on its
  next tick.
- AI provider docs now include setup profiles for compatibility mode,
  reviewer-friendly intent grouping, metadata-only private repos, self-hosted
  providers, explicit diff egress, and subprocess intent-planner plugins.
- Read-only observability commands no longer migrate old repo databases just to
  inspect them. If a decision ledger is missing, they return an empty history
  instead of changing the DB.
- `acd events --watch` now follows decisions appended after watch starts unless
  `--since` is provided.
- Docs now cover explainable history, failed replay barriers, safe-ignore
  restart requirements, intent batch-wait troubleshooting, prompt-trace
  provider setup, local retention behavior, and the current status JSON fields.

### Fixed

- Intent strategy falls back to event-style publishing when configuration is
  invalid or planner output cannot be trusted, so replay safety stays under ACD
  control.
- Intent grouping now treats nested paths as ordered dependencies, so a later
  child path cannot publish ahead of an earlier parent-path capture.
- Git history and supersede probes now use literal pathspecs, so paths
  containing characters such as `*`, `[`, or `:` cannot match unrelated files
  while proving planner context or external supersede history.
- Planner failures from OpenAI-compatible and subprocess providers are recorded
  as `intent_planner_error` decisions before deterministic fallback, and
  planner reason text is normalized before it reaches diagnostics.
- v6 to v7 state database migration now adds `planner_state` without rebuilding
  the existing decision ledger, and intent deferral summaries ignore pending
  rows hidden behind terminal replay barriers.
- Fast-forward during rewind grace now has integration coverage that preserves
  the fast-forwarded worktree before checking for phantom capture events.
- Integration SQLite setup now waits for transient daemon DB locks before
  injecting PID-reuse test rows.
- Decision records keep their original event sequence after old capture events
  are pruned, so historical explanations stay useful.
- Replay no longer marks work as `superseded_external` unless history, `HEAD`,
  and the live worktree prove the queued change is already obsolete. The
  history probe is bounded by the per-event timeout.
- Deleted tracked files under skipped generated or gitignored directories are
  captured as deletes instead of being hidden by the parent directory skip.
- Same-branch fast-forwards, such as `git checkout main && git pull`, now
  refresh ACD's shadow baseline from the new `HEAD` instead of replaying stale
  work captured before the pull.
- Manual pause/resume now preserves self-heal behavior when an external commit
  lands during the pause, so the resumed daemon can mark matching work as
  already published instead of treating it like upstream-only content.
- Branch-transition settling avoids treating a ref move and its worktree update
  as separate local edits.

## v2026-05-03

### Added

- `acd recover --auto` can repair stale live-index entries left by older
  ACD-published commits, and `acd doctor` can report repair candidates.
- Generated dependency and cache directories such as `node_modules/`,
  `target/`, virtualenvs, and common tool caches are ignored by default during
  capture and watcher walks.
- `acd start` now works without `--session-id` for manual current-repo use.
  It registers a stable human client for the repo, so repeated manual starts
  refresh the same row instead of creating a pile of stale clients.
- `acd stop` now works without `--session-id` for manual current-repo use.
  It stops the resolved repo daemon directly.
- `acd list --watch` refreshes the daemon table until Ctrl-C.
- `acd list --watch --interval <duration>` sets the refresh rate.
- `acd logs` tails the current repo's daemon log as raw JSONL.
- `acd logs --lines N` changes the initial tail length.
- `acd logs --follow` streams new daemon log lines as they arrive.

### Changed

- Replay now reconciles the live Git index after publishing commits, guarded by
  path-scoped before-state checks so user-staged changes are not overwritten.
- Root `acd --help` is now compact and grouped by workflow.
- User-facing commands now include more practical help text and examples.
- `acd stop --session-id <id>` is now documented as the harness/refcount path:
  it deregisters one client and stops the daemon only when no peers remain.
- Harness templates keep passing explicit `--session-id`; the new no-flag
  start/stop defaults are for humans at a terminal.
- Updated README and troubleshooting docs with examples for watch mode and
  log tailing, live-index recovery, safe-ignore defaults, and the simpler
  current-repo start/stop flow.

### Fixed

- Daemon runs now wire the per-repo JSONL file logger through the same canonical
  repo hash used by `acd logs` and central stats.
- Published replay events no longer leave the live index stale after ACD moves
  `HEAD`.
- Generated dependency/cache trees no longer show up as capture events or
  watcher load when a repo forgot to gitignore them.
- `acd logs --follow` no longer misses lines appended while switching from
  the initial tail read to follow mode.

## v2026-05-02

### Breaking changes

- Removed `ACD_AI_SEND_DIFF`. Diff egress is now off by default. Set
  `ACD_AI_DIFF_EGRESS=1` to allow network or subprocess AI providers to
  receive redacted diffs.

### Added

- `acd diagnose`, `acd recover`, `acd pause`, `acd resume`, and
  `acd purge-events` give operators first-class recovery controls for replay
  blockers, branch incidents, and manual pause state.
- Recursive fsnotify watching can drive daemon wakeups when enabled.
- Best-effort JSONL trace files record capture, replay, branch-token, pause,
  and daemon-transition decisions.

### Changed

- Replay, fsnotify, git ignore checks, log rotation, and provider shutdown are
  more aggressively bounded so the daemon is less likely to hang.
- Git diff/blob rendering now has stronger caps for large files.
- Process checks use pinned system `ps` paths on macOS and Linux.
- Schema v4 adds faster flush-request lookup and read-heavy state paths use the
  read pool where possible.
- Docs now cover AI diff egress, branch-token handling, recovery workflows, and
  daemon troubleshooting.

### Fixed

- Fixed several edge cases around ambiguous refs, SQLite lock handling,
  rewind grace, malformed pause markers, detached-to-attached branch recovery,
  shadow bootstrap atomicity, and git-operation marker stat errors.

## v2026-04-28

### Added

- Initial public release of `acd`, a per-repo auto-commit daemon for macOS and
  Linux.
- Added daemon lifecycle commands: `start`, `stop`, `wake`, `touch`, and
  `daemon run`.
- Added operator commands: `status`, `list`, `stats`, `doctor`, `diagnose`,
  `recover`, `pause`, `resume`, `purge-events`, `gc`, and `init`.
- Added capture and replay backed by SQLite state, shadow paths, publish state,
  flush requests, daemon metadata, rollups, and the central registry.
- Added commit-message providers: deterministic, OpenAI-compatible, and
  subprocess.
- Added harness setup snippets for Claude Code, Codex, OpenCode, Pi, shell,
  and direnv.
- Added JSONL daemon logs with rotation, XDG paths, repo hashing, process
  fingerprinting, trace support, and install/uninstall scripts.

### Changed

- Pinned Go 1.22 dependencies, including `modernc.org/sqlite v1.36.0`.
- Release packaging is set up. Homebrew publishing remains skipped until tap
  credentials exist.
