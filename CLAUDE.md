# Auto-Commit-Daemon

ACD protects eligible file changes in private Git checkpoints while users work.
Users run `acd finish` when a change is ready. Ordinary protection never calls a
provider, reads provider credentials, runs project checks, or starts publication.
Startup may recover an already authorized prepared mutation from its exact saved
proof; it cannot start a new finish.

Fresh installations use offline Local mode and imperative messages. Local finish
creates one complete batch; optional Intent mode plans completed goals and safe
semantic boundaries. Both modes require explicit finish. ACD never pushes.

Keep the three workflows independent: `on` captures current changes, `finish`
creates commits from a fresh protected target, and `history rewrite` uses the
selected current Git history as its source of truth. Old archive records must
not gate these workflows or enter a new rewrite plan.

This file tells agents how to change ACD. README and `docs/` explain the product
to users. `AGENTS.md` is a symlink to this file, so edit `CLAUDE.md` and preserve
the symlink.

## What ACD must do

1. **Protect continuously.** Keep eligible work durable before any publication,
   including during provider failures, rejected plans, restarts, and branch movement.
2. **Publish on request.** Freeze a fresh complete checkpoint and attached HEAD,
   prepare and verify the whole private chain, then publish through one branch CAS.
3. **Keep later work moving.** Later edits stay protected for the next finish and
   never change the accepted request.
4. **Use purposeful boundaries.** Intent produces complete goals and valid
   intermediate commits. Local provides one honest offline batch.
5. **Recover from proof.** Interrupted planning becomes a retryable failure;
   prepared publication recovers only from exact saved branch/index evidence.
   Ambiguous evidence remains `needs_attention`.
6. **Stay quiet and truthful.** Avoid idle provider/check/commit work, busy loops,
   full scans without cause, and writes that do not represent progress. Distinguish
   protection, branch publication, recovery preservation, and required action.

Local and Intent are the supported strategies. Legacy Event settings translate
into Local batches during explicit upgrade. No continuous publication engine,
automatic history repair, or fallback mode remains.

## Terms

- **capture:** one observed file version retained as checkpoint or legacy evidence
- **checkpoint:** a Git snapshot proving eligible work is protected
- **publication:** applying a fully prepared finish chain to its branch
- **Intent:** grouping related captures into semantic commits
- **publication target:** the frozen complete tree and identity of an explicit request
- **recovered:** preserved in a protected recovery snapshot
- **resolved:** published normally or safely recovered
- **worker:** the process protecting one repository
- **supervisor:** the per-user process managing workers
- **runtime:** the installed CLI, supervisor, and active workers
- **source:** the code in this checkout, which may differ from the runtime

Use these terms in code, documentation, and status messages.

## How to work

Questions, reviews, research, and diagnosis are read-only unless the developer
asks for a change.

Before editing:

1. Read the relevant code and canonical documentation.
2. Inspect the current diff and nearby history.
3. Understand the scenario as the user experienced it.
4. Make the smallest complete change.
5. Add regression coverage for the original scenario.
6. Simplify recently changed code before handoff.
7. Verify only what the change invalidated.

Use `rg` to search if available. Preserve unrelated work.

ACD self-hosts this repository:

- Check installed runtime ownership once. An active older runtime still owns
  automatic commits until explicitly upgraded; warnings do not make it inactive.
- Do not run `git add` or `git commit`. Never force `acd commit-all` unless the
  user specifically requests it.
- With a verified finish-based runtime, use `acd finish` for authorized completed
  work. Finish the task and checks without waiting or polling for publication.
  Report failed or pending requests accurately.
- Do not pause ACD for ordinary edits, builds, or isolated tests. Pause only for
  history surgery, direct state recovery, or a test operating on this checkout.
- Source changes do not update the installed runtime. Install, restart, tag,
  push, or release only when explicitly requested.

## Safety rules

### Ownership and state

The canonical daemon lock proves ownership. PID files and heartbeats are only
supporting evidence. Never delete locks, move `.git/acd`, or remove state files
to bypass an unclear owner.

Read-only commands must not write state, migrate schemas, build providers, or
rewrite registry data.

Migrations and recovery must preserve captures and immutable provenance.
Ambiguous ref movement or publication remains `needs_attention`; never guess.

Publication and recovery must preserve the live worktree, index, and user
staging.

Unsafe Git states can prevent finish publication. Continuous checkpoint
protection remains active where eligible files are readable.

### Intent and recovery

Every changed entry in a frozen Intent target must belong to exactly one goal.

Hard dependencies, rename chains, create and delete order, and generated
relationships must remain safe. Time and directory proximity cannot be the only
reason for grouping unrelated work.

Plan useful completed steps within the overall goal before assigning changes.
A feature can contain a working refactor, its consumer, and a broader policy
decision. Dependencies determine order; they do not by themselves require one
commit. Keep implementation,
callers, tests, imports, generated output, and relevant documentation together
when they complete one goal. Available unpublished corrections belong with the
change they correct. Different files or screens do not justify a split.

Each split needs a useful review or revert boundary and a valid intermediate
state. Put prerequisites before consumers or in the same commit. Explain
the reason or practical impact in each generated message, not only internal
planning metadata. Explain broader defaults and behavior changes from evidence.
Intent requires approved redacted code diffs on every planning path; completion
context supplements that evidence. Local remains offline. Existing settings
retain their consent on upgrade. An exact `--message` requests one complete batch. Age and queue pressure
never waive completeness checks. Judge the proposed commit tree, not the final
worktree. Frozen request identity, author boundaries, and explicit history-rewrite safety
limits still apply.

An Intent attempt may review large evidence in at most 32 bounded calls, then
make one global planning call and at most one global correction within one persisted
deadline. Reviews cover every complete redacted diff, splitting large files
without dropping bytes. Review fragments are evidence, never commit boundaries.
An invalid review gets at most one correction; accepted reviews are not repeated.
Corrections consume the same 32-call budget and deadline. Their counts are
reserved durably and reported separately from global planning calls.
At most two immutable reviews run concurrently; budget reservations and state
writes stay on the owning worker. Small requests skip review. Messages belong to the global plan. Provider rejection,
timeout, unavailable evidence, or exhausted budget fails the explicit request;
there is no background semantic retry or silent Local fallback.

A valid grounded plan can become one whole-batch goal only when it passes the
same dependency, materialization, and verification checks. Never create generic
per-file commits to escape a rejected plan.

Client disconnect detaches from the accepted bounded job. Restart fails running
planning retryably and recovers prepared publication without AI only through exact
proof. Explicit retry preserves the frozen target and options; it cannot absorb
later edits, silently rebase, or restore over the user's worktree.

Keep legacy captures, journals, and immutable provenance readable for explicit
migration, history, restore, and retention. Legacy rows are not an active queue.

### Observability and performance

`acd status`, `acd list`, doctor, diagnose, and JSON output must agree.

Saved work awaiting finish is a healthy state. A responsive worker does not
prove a branch commit. Report worker liveness and explicit request progress
separately.

Human status separates branch commits from recovery preservation. Deprecated
JSON publication booleans retain their earlier resolved-work meaning; the shared
publication outcome reports branch commitment, recovery, saved work, and explicit
request progress. Legacy classification flags do not imply background planning.

Prefer event-driven wakeups. Polling is a safety net and should back off while
idle. Keep scans, explicit provider calls, logs, and database work bounded.

## Testing

Tests must represent scenarios users can actually encounter.

A good regression test recreates the original problem, including restart,
timeout, provider rejection, branch movement, queued captures, or recovery when
those details caused the defect.

Test the visible result as well as internal state. A progress fix should prove what
`acd status` and `acd list` tell the user.

Prefer observable milestones and durable state transitions over sleeps. Use
isolated HOME, XDG, repository, process, and PTY state. After `git init`, attach
`HEAD` to `refs/heads/main`.

Do not weaken assertions, remove real scenarios, or hide failures to make tests
faster.

### Keep feedback under five minutes

The default local gate and the complete hosted GitHub Actions workflow each
have a five-minute wall-clock target. The local gate uses three core shards
with at most two parallel tests per package process.

Hosted pull requests run four core shards, four production integration shards,
and one support lane on both Ubuntu and macOS. The support lane runs timing-sensitive tests only after the other
package tests finish. Repeated daemon stress uses six Ubuntu runners, while
Git and state stress use two. Each leaf job should finish within four minutes
and 30 seconds and has a hard five-minute timeout. If a lane exceeds that
target, split or rebalance it without dropping cases or repetitions.

When tests exceed that budget:

- shard independent packages
- improve fixtures and startup helpers
- remove duplicated setup
- replace sleeps with observable readiness
- run independent CI jobs in parallel
- measure where the time is spent

During development, run the smallest focused test that proves the change:

~~~bash
go test ./path/to/package -run '^TestRelevantScenario$' -race -count=1
~~~

Use `-count=10` for stability-sensitive behavior. Use
`GOMAXPROCS=1 -count=50` only for suspected ordering hazards.

Run the broad local gate once when a code milestone is ready:

~~~bash
make lint
make test
git diff --check
~~~

Run a focused integration selector when the change crosses real processes, Git
state, providers, PTYs, lifecycle, or recovery:

~~~bash
go test ./test/integration/... \
  -tags=integration \
  -run 'RelevantScenario' \
  -race \
  -count=1 \
  -timeout 5m
~~~

Hosted CI owns the complete integration and repeated stability lanes. Keep them
sharded instead of stacking every lane sequentially during local development.

### Documentation-only changes

Do not run Go builds, package tests, race tests, integration tests, or the full
gate after changing only documentation or agent guidance.

Run:

~~~bash
git diff --check
test -L AGENTS.md
test "$(readlink AGENTS.md)" = CLAUDE.md
~~~

Add only targeted checks for changed links, generated references, documented
commands, or release packaging.

## Where code lives

| Path | Purpose |
|---|---|
| `internal/cli` | Commands, lifecycle, status, recovery, and setup |
| `internal/daemon` | Capture, Intent, publication, recovery, and run loop |
| `internal/state` | SQLite state, checkpoints, plans, and migrations |
| `internal/git` | Bounded Git operations and proof helpers |
| `internal/ai` | Providers, prompts, messages, and planning |
| `internal/supervisor` | Shared worker management |
| `templates`, `internal/adapter` | Harness integrations |
| `test/integration` | Real lifecycle, recovery, Intent, and fault scenarios |

Templates are the source of truth for integrations. Keep templates, setup
tests, and adapter coverage synchronized.

README and docs are product contracts. Use the humanizer skill when writing
documentation. Update user docs and `CHANGELOG.md` when behavior changes.

## Canonical references

- Architecture: `docs/overview.md`
- Protection and publication: `docs/capture-replay.md`
- Intent publication: `docs/intent-commit-flow.md`
- Providers and privacy: `docs/ai-providers.md`
- Settings: `docs/settings.md`
- Commands: `docs/commands.md`
- User workflows: `docs/user-workflows.md`
- Harness integrations: `docs/multi-tool.md`
