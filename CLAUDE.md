# Auto-Commit-Daemon

ACD is a lightweight background tool that protects file changes and turns them
into useful local Git commits.

Intent mode is the main product experience. A user changes files, ACD captures
the work, groups related changes by meaning, and creates semantic commits. The
user should not need to stage files, choose commit boundaries, monitor a queue,
or think about committing.

This file tells agents how to change ACD. README and `docs/` explain the product
to users. `AGENTS.md` is a symlink to this file, so edit `CLAUDE.md` and preserve
the symlink.

## What ACD must do

1. **Create semantic history.** Intent mode should produce coherent commits,
   not generic filename commits or arbitrary time-based groups.

2. **Protect before publishing.** Provider failures, rejected plans, failed
   verification, restarts, branch movement, and timeouts must not put captured
   work at risk.

3. **Recover automatically.** Retry temporary failures, rebuild invalid plans,
   and restart from the last proven checkpoint state. Use `needs_attention`
   only when ACD cannot prove a safe automatic path.

4. **Keep new work moving.** Once publication starts, its target stays frozen.
   Later captures remain protected and become input for later semantic commits.

5. **Stay fast and quiet.** Avoid busy loops, frequent full-repository scans,
   repeated provider calls for unchanged evidence, and database writes that do
   not represent progress.

6. **Tell the truth.** Status and list must clearly distinguish protection,
   publication, waiting, active work, recovery, and required user action.

Event mode is useful for compatibility and testing, but Intent mode is the
experience we optimize for.

## Terms

- **capture:** one durable record of an observed file change
- **checkpoint:** a Git snapshot proving eligible work is protected
- **publication:** turning protected captures into branch commits
- **Intent:** grouping related captures into semantic commits
- **publication target:** the frozen captures currently being published
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

- Do not run `git add` or `git commit`.
- Do not pause ACD for ordinary edits, builds, or isolated tests.
- Pause only for history surgery, direct state recovery, or a test that
  deliberately operates on this checkout.
- Source changes do not update the installed runtime.
- Install, restart, tag, push, or release only when explicitly requested.

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

Unsafe Git states may pause classification or publication, but completed
checkpoint protection remains active.

### Intent and recovery

Every visible capture must belong to exactly one Intent group.

Hard dependencies, rename chains, create and delete order, and generated
relationships must remain safe. Time and directory proximity cannot be the only
reason for grouping unrelated work.

When a plan fails:

1. Keep valid groups when possible.
2. Retry or correct the unresolved part.
3. Rebuild stale plans.
4. Use safe evidence-based Intent recovery after bounded provider failures.
5. Keep later captures protected for the next plan.

Evidence-based recovery must still respect dependencies, materialization, and
verification. It must not become generic per-file commits.

Restart planning from the last proven checkpoint state when current publication
cannot continue. Do not restore files over the user's worktree.

A provider rejection, timeout, or worker restart should recover automatically.
It should not leave the repository permanently blocked.

### Observability and performance

`acd status`, `acd list`, doctor, diagnose, and JSON output must agree.

A healthy wait is not a stall. A responsive worker is not proof of queue
progress. Report worker liveness and queue movement separately.

`Published to Git: yes` means every protected change is resolved in branch
history or a protected recovery snapshot, with no active failed or blocked
capture.

Prefer event-driven wakeups. Polling is a safety net and should back off while
idle. Keep scans, provider calls, queues, logs, and database work bounded.

## Testing

Tests must represent scenarios users can actually encounter.

A good regression test recreates the original problem, including restart,
timeout, provider rejection, branch movement, queued captures, or recovery when
those details caused the defect.

Test the visible result as well as internal state. A queue fix should prove what
`acd status` and `acd list` tell the user.

Prefer observable milestones and durable state transitions over sleeps. Use
isolated HOME, XDG, repository, process, and PTY state. After `git init`, attach
`HEAD` to `refs/heads/main`.

Do not weaken assertions, remove real scenarios, or hide failures to make tests
faster.

### Keep feedback under five minutes

The default local gate and the complete hosted GitHub Actions workflow each
have a five-minute wall-clock target.

Hosted pull requests run four core shards and one support lane on both Ubuntu
and macOS. The support lane runs timing-sensitive tests only after the other
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
