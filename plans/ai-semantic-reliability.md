# Reliable AI semantic commits with simpler operation

Accepted implementation plan, September 15, 2026.

Branch: `feat/ai-semantic-reliability`.
Base: `3285f423`, matching refreshed `origin/main` with a clean worktree.
Execution and evidence: Trekoon epic `f3466c0f-27f2-4b67-8990-c9fd3757923d`.

This document describes the target behavior. README and user documentation
change alongside the corresponding implementation. Trekoon records progress.

## Goal and decisions

Users enable ACD, keep editing, and trust it to protect changes and create
coherent local commits without supervising a queue.

- Recommend AI semantic grouping for Everyday work.
- Continue checkpoint capture during provider or verification waits.
- Wait for AI during outages. Never silently publish deterministic messages
  for an AI-configured repository.
- Keep deterministic mode as an explicit offline alternative.
- Recover invalid AI grouping through validated local evidence when necessary;
  an AI-configured group still needs an AI-written message.
- Keep bounded repair of recent, private ACD commits enabled.
- Preserve existing provider, privacy, verification, and repository choices.
- Preserve the completed checkpoint-maintenance and dashboard-activity work.
- Remove obsolete implementations and tests without losing supported scenarios.

## User journeys

### Setup and upgrades

1. `acd setup` recommends AI semantic commits with a concrete source/test/docs
   grouping example. Local automatic commits explain their limited semantics.
2. Request credentials and source-sharing consent only for the selected
   provider. Advanced connection and verification settings remain optional.
3. Probe using synthetic content, then review the existing installation plan.
4. After successful setup, offer to enable the current repository through a
   separate consent step. Confirm protection only after its first checkpoint.
5. Preserve saved choices on upgrade. Fresh noninteractive setup requires an
   explicit provider; it never silently selects a network service.
6. Verify runtime convergence and report partial success honestly. Package
   installation stays with the package manager. Explain macOS session-owned
   startup; this change adds no launchd service.

### Daily work and recovery

Default status answers whether protection is on, current changes are saved,
work reached branch commits, and any recovery or action remains. Verbose and
JSON output retain queue, phase, provider, heartbeat, and retry detail.

| Situation | Behavior |
|---|---|
| Timeout, rate limit, temporary outage | Capture continues; persisted backoff and one recovery probe retry AI. |
| Invalid grouping | Keep valid groups; correct unresolved work; prove local fallback grouping. |
| Missing AI message | Keep the group protected and wait for its semantic message. |
| Bad credentials or source-sharing configuration | Preserve work; name one actionable configuration correction. |
| Verification failure | Bounded checkpoint recovery; independent safe work can proceed. |
| Missing objects or ambiguous Git ownership | Preserve evidence; require action only where safety cannot be proved. |
| Worker restart | Resume durable targets without duplicate publication. |

An explicit deterministic configuration change previews the active-target
effect and uses verified preservation/recapture when necessary. Return to AI
only after an explicit selection.

### Commit, restore, and configuration

- `commit-all` previews paths, queued work, staging scope and staging effects.
  Recheck before apply and refresh confirmation when scope materially changes.
  Freeze the target; later captures stay separate. Avoid double-counting paths
  and queue entries as distinct files.
- Bare `restore` opens a terminal checkpoint picker with time and changed
  files. The selected checkpoint uses the existing preview, overlap, apply,
  and undo protocol. ID-based scripts remain supported; nonterminal bare
  invocation gives usage and a history command without applying anything.
- Ordinary settings show effective values, their sources, and how to restore
  inheritance. Preserve advanced resolution layers and privacy boundaries.

## Implementation packages

### A. Baseline and obsolete code

Audit macOS/Linux reachability including supported build tags and compatibility
entrypoints. Classify retired implementations, adapters, platform code and
test helpers. Map removed tests to supported replacements or obsolete behavior.
Transfer useful scenarios before deleting old lifecycle, direct-daemon,
status/list, commit-all, and repository command implementations. Keep supported
aliases thin. Retain necessary state migrations and remove misleading historical
phase comments. Each removal is a coherent, independently reviewable change.

### B. Nonblocking evaluation

Retain one canonical worker owner. Split publication into worker-owned prepare,
bounded background evaluation, and worker-owned validation/application.

- One evaluation per worktree; coalesce pending requests.
- Pin completed checkpoint membership, branch generation, expected parent,
  candidate/config revisions, provider and verification identities.
- Background evaluation uses immutable inputs and isolated scratch resources.
  It cannot mutate live refs, worktree, index or candidate lifecycle records.
- Keep runtime resources leased; hold operation gates only for required short
  prepare/apply boundaries, never throughout provider or project-check waits.
- Continue watcher/hint processing and complete checkpoints while evaluating.
- Later captures do not invalidate or enlarge a frozen target.
- Reject results invalidated by branch movement, restore, candidate ownership
  or configuration changes; rebuild from proven state.
- Cancel and join jobs on shutdown; preserve restart evidence and checkpoint
  object reachability until active work resolves.
- Use typed internal job/request/result structures and existing durable plan,
  candidate and publication records rather than a general job framework.

### C. Recovery and AI waiting

Give the publication coordinator ownership of recovery decisions. Separate
typed reasons, retry scheduling, proof, and explanatory wording. Remove control
flow based on error text. Retain bounded corrections, preserved groups and
exact completed-plan reuse. Separate transport waits from semantic retry
exhaustion. Persist 30-second, two-minute, then ten-minute backoff with a single
probe, continuing at the cap during outages. Repeated temporary failure alone
must not become terminal or exhaust candidate identities.

Retain actionable credential/configuration and safety failures. Reuse exact
valid messages and verification results across restarts. Keep private repair
limits and independent-group progress. Explicit provider changes use the
existing durable transition protocol and preview their effects.

### D. Shared truth and Git proofs

Use one read-only typed outcome projection across status/list/doctor/diagnose/
history. Distinguish enabled protection, covered observation, branch publication,
separate recovery preservation, activity, waits and required action. Add explicit
JSON outcome/reason/retry fields while retaining deprecated fields' meanings.
Human output changes immediately; recovered history has a distinct outcome.

Preserve unknown values. Return typed service data instead of JSON round trips.
Share ancestry, tree, rename, mode and post-probe ref checks with explicit
caller policies. Divide rates by actual sample counts and report those counts;
handle low-sample warning confidence separately. Persistent additions use
additive migrations. Read-only commands never migrate or mutate state.

### E. CLI journeys

Implement the setup, compact/verbose status, settings inheritance, reviewed
commit scope and restore selection journeys above. Keep Everyday structural
verification and bounded repair, credential security, source-sharing approval,
accessible output, scripting interfaces, and noninteractive repository consent.

### F. Faster production verification

Record JSON test timings and wall-clock baseline. Preserve package races,
platform coverage and repeated daemon/Git/state stress. Make surviving tagged
integration scenarios required on Ubuntu and macOS and include all lanes in
the final CI result. Use measured duration for deterministic shard assignment
and prove complete, nonoverlapping selection, including examples/fuzz seeds.

Start with existing core/stress lane counts. Size integration shards against
a three-minute test budget and rebalance when measured leaf time exceeds
4m30s. Keep a hard five-minute job timeout and five-minute local/hosted critical
path target; report hosted queue delay separately. Reuse production binary and
repository fixtures, avoid discovery rebuilds, replace sleeps with readiness,
bound subprocess concurrency, and remove only duplicate/obsolete coverage.

Replace the placeholder benchmark with checkpoint latency, capture during AI
waits, idle resources, and suite-duration measurements. Integration runner race
instrumentation does not imply the release-style child binary is instrumented.

### G. Documentation

Update README, commands, workflows, providers, settings, protection, architecture,
Intent docs and the generated configuration reference with each behavior change.
Lead with AI and explicit local alternatives; explain waits, recovery, outcomes,
staging, restore undo, runtime convergence, private repair and current tests.
Keep aliases in one compatibility section and remove obsolete primary examples.
Preserve maintenance updates. Update CHANGELOG and agent guidance while keeping
`AGENTS.md` linked to `CLAUDE.md`. Apply the humanizer writing guidance.

## Acceptance and verification

Regression coverage must prove capture during blocked provider/verification;
frozen target progress under continuous editing; AI outage/restarts without
deterministic commits; bounded invalid-plan correction; canceled/stale jobs;
branch switch/reset/rebase/restore races; staging preservation and reviewed
consumption; explicit deterministic transition; consistent recovered outcomes;
setup/upgrade consent and version convergence; picker/ID restore parity and
undo; real metric denominators; populated-state migrations; and complete
surviving integration coverage after old tests are removed.

Mock providers cover related source/tests/docs, independent features, renames,
generated files and dependencies. Assert grouping and useful messages.
Run focused race tests and relevant isolated integration selectors during
development. At a code milestone run `make lint`, `make test`, and
`git diff --check`. Repeat stability-sensitive cases only when justified.
Documentation-only changes use docs checks and verify the AGENTS symlink.

## Delivery and boundaries

Deliver baseline/mapping, retired-code removal, shared proofs/status,
nonblocking evaluation, durable recovery, CLI journeys, required optimized
tests, then final documentation and independent review. Every behavior change
includes its regression tests and documentation. ACD handles all commits.

Completion requires all accepted findings addressed or verified obsolete;
capture during slow evaluation; no silent deterministic downgrade; preserved
supported coverage; required integration lanes; before/after timing evidence;
consistent docs/help/JSON/runtime contracts; and all scoped changes committed.

Do not install or restart the user's runtime, push, open/merge PRs, tag or
release without a later explicit request. Preserve unrelated concurrent work.
