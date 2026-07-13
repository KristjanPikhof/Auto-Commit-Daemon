# Command reference

Start with `acd`. It reads the current repository, tells you whether ACD is
healthy, and gives you one next action. Most people only need `acd`, `acd on`,
and `acd off` during normal work.

Commands use the Git worktree containing your current directory. Pass
`--repo /path/to/repo` when you want to target another worktree. Add `--json`
to commands that support structured output, and use
`acd <command> --help` for every flag and example.

## Daily control

| Command | Use it for | What it changes |
|---|---|---|
| `acd` | Check one repo and get a recommended next action. | Nothing. It reads health only. |
| `acd on` | Register or enable the repo and make sure its daemon is running. | Desired state, registration, client heartbeat, and daemon process as needed. |
| `acd off` | Stop ACD for this repo until you turn it on again. | Desired state and daemon process. Captured state stays in `.git/acd`. |
| `acd status` | Inspect the daemon, clients, queue, branch, pauses, and recent decisions. | Nothing. |
| `acd list` | Watch enabled repos from one terminal. | Nothing, unless you open the interactive manager. |

The short loop is:

~~~bash
acd
acd on
acd off
~~~

Bare `acd` reports one of these states:

| State | Meaning |
|---|---|
| `healthy` | The daemon is running and no action is needed. |
| `waiting` | Intent mode is waiting for its configured batch boundary. |
| `degraded` | ACD is still running, usually with deterministic planner fallback. |
| `needs_attention` | The daemon, pause state, or replay queue needs inspection. |
| `off` | This repo is disabled. |
| `not_a_repo` | The selected path is not inside a Git worktree. |

Bare `acd` returns its classification as information, including unhealthy
states. `acd on` returns a failure when it cannot leave the repo running in a
healthy state.

`acd status --watch` refreshes one repo. `acd list` behaves differently based
on its output:

| Command | Behavior |
|---|---|
| `acd list` in a terminal | Opens the compact live dashboard. |
| `acd list` in a pipe | Prints one compact snapshot. |
| `acd list --once` | Prints one snapshot even in a terminal. |
| `acd list --once --verbose` | Adds clients, last commit, full paths, and status notes. |
| `acd list --json` | Prints one machine-readable snapshot. |
| `acd list --interactive` | Opens the repo lifecycle manager. |

Watch mode and `--json` cannot be combined.

## Set up an AI tool

`acd setup [harness]` prints the hook snippet for one supported integration. It
does not edit the target configuration file.

~~~bash
acd setup claude-code
acd setup codex
acd setup cursor
acd setup opencode
acd setup pi
acd setup shell
~~~

Use `--raw` when you need only the snippet body:

~~~bash
acd setup codex --raw
~~~

Redirecting raw output with `>` replaces the destination file. Merge the
snippet by hand when that file already contains custom hooks or settings. Run
`acd doctor` after setup to check the installed hook against the current
template.

The adapter guides under [`templates/`](../templates/) contain the exact
configuration path, hook mapping, verification steps, and uninstall steps for
each harness.

## See what ACD is doing

| Command | What it answers | Notes |
|---|---|---|
| `acd status` | Is this repo running, waiting, paused, blocked, or degraded? | Use `--watch` for a live view or `--json` for automation. |
| `acd events` | What did ACD capture, group, publish, skip, or block? | `--watch` starts at the current activity tail unless `--since` is set. |
| `acd explain` | Why did one path or commit behave this way? | Use `--path FILE`, `--commit REV`, or `--last`. |
| `acd diagnose` | Which branch anchor, pause, queue barrier, or planner state needs attention? | Read-only. Start here before recovery. |
| `acd logs` | What did the daemon write to its raw JSONL log? | Use `--lines N` and `--follow`. |
| `acd prompt` | What did a traced AI request contain? | Traces exist only when `ACD_AI_PROMPT_TRACE=1` was set at daemon start. |
| `acd doctor` | Are the binary, hooks, provider settings, registry, and runtime healthy? | `--bundle` writes a sanitized zip for support. |

Useful examples:

~~~bash
acd events --watch
acd explain --path internal/state/schema.go
acd explain --commit HEAD
acd diagnose --json
acd logs --lines 100 --follow
acd prompt --last
acd doctor --bundle --output /tmp
~~~

`acd events` is the activity history intended for normal use. `acd logs` is
the lower-level daemon log. Prompt traces may contain source text, so review
them before sharing.

## Recover a repo

ACD normally resolves queued work by itself. If the branch already contains
every captured change, ACD marks that work published. Otherwise it saves the
captured chain under `refs/acd/recovery/*`, reseeds from the current branch,
and captures any work that is still present in the worktree.

Use this manual sequence when `acd` says the repo needs attention:

~~~bash
acd
acd diagnose --json
acd fix --dry-run
acd off
acd fix --yes
acd on
acd
~~~

`acd fix --dry-run` only prints the plan. `acd fix --yes` backs up the SQLite
state, reruns its safety checks, and applies safe recovery while the daemon is
off. It never changes the live worktree, index, or branch.

Use the force mode when you want ACD to save the captured chain without first
trying to prove that the branch already contains it:

~~~bash
acd fix --force --dry-run
acd off
acd fix --force --yes
acd on
~~~

Here, `--force` selects archive-only recovery. It does not authorize the write
by itself, and it does not delete captured work. `--yes` is still required.

| Command | Purpose |
|---|---|
| `acd pause --reason "branch surgery"` | Stop capture and replay while keeping the daemon registered. |
| `acd pause --ttl 1h --reason "maintenance"` | Add a pause that expires automatically. |
| `acd resume --yes` | Remove a manual pause marker. |
| `acd resume --accept-overflow` | Clear durable capture backpressure and accept that filesystem events may have been missed. |

Use `--accept-overflow` only after reading `acd diagnose`. It is separate from
`--yes` because it accepts a different risk.

See [user-workflows.md](user-workflows.md) for archived-work inspection,
generated-file cleanup, branch surgery, and symptom-based recovery.

## Commit work made while ACD was off

`acd commit-all` captures a dirty worktree and replays it with the configured
commit strategy. It first resolves any older unpublished chain, then reseeds
from `HEAD` before capturing the live files.

~~~bash
acd off
acd commit-all --dry-run
acd commit-all --yes
acd on
~~~

The command refuses detached HEAD, an in-progress Git operation, a manual
pause, and a live daemon when it reaches the write phase. `--json` requires
`--yes` for an apply run. If any captured work remains unpublished, the command
returns a failure instead of reporting a partial drain as success.

## Rewrite local commit messages

`acd rewrite-commits` creates a reviewable message-only plan for a linear range
on the current branch. The daemon never starts a rewrite on its own.

~~~bash
acd rewrite-commits --from-nr 5 --plan-out rewrite.json --plan-only
acd rewrite-commits --show-plan rewrite.json

acd off
acd rewrite-commits --apply-plan rewrite.json --dry-run
acd rewrite-commits --apply-plan rewrite.json --yes
acd on
~~~

Use `--from-sha`, `--from-nr`, `--range-sha`, `--range-nr`, or `--last` to
select commits. Creating a new plan requires intent mode and a working
non-deterministic provider. Showing, editing, or applying a saved plan does not
call the provider.

Apply verifies the branch, creates backup refs, recreates commits with the new
messages, and moves the branch only after the checks pass. File contents stay
the same. Merge commits, detached HEAD, a live daemon, pending captures, and a
branch that no longer matches the plan are refused.

For a fresh selection, `--json` prints the resolved selection and exits before
plan generation. Use normal text output when you want to generate a plan.

See [rewrite-commits.md](rewrite-commits.md) for selectors, plan editing,
progress output, and backup recovery.

## Manage registered repos

Most repos need no manual registration because harness hooks call `acd start`.
Use `acd on` and `acd off` for normal control. The `acd repo` commands are for
explicit registry administration.

| Command | What it does |
|---|---|
| `acd repo init` | Creates or opens `.git/acd/state.db` and registers an attached worktree without starting a daemon. |
| `acd repo disable` | Disables a registered repo, stops its daemon, clears start caches, and preserves state. |
| `acd repo enable` | Enables a registered repo without starting its daemon. |
| `acd repo list` | Lists every registry row, including disabled, missing, and state-database-missing repos. |
| `acd repo manage` | Opens the interactive lifecycle manager. |
| `acd repo remove --dry-run` | Previews registration removal. |
| `acd repo remove --yes` | Removes the registry row and start caches, but keeps `.git/acd`. |
| `acd repo remove --yes --purge-state` | Removes the registry row and deletes `.git/acd` state. |

`acd repo disable` and `acd repo enable` require an existing registry row. In
contrast, `acd off` records the disabled state even for a repo ACD has not seen
before, and `acd on` can register and start it in one command.

## Maintenance and build information

| Command | What it does |
|---|---|
| `acd stats` | Reports central commit, event, file, byte, and error totals for the last seven days. Use `--since 30d` to change the window. |
| `acd gc` | Merges duplicate registry rows and removes missing or long-dead entries. It does not delete repo state databases or captured events. |
| `acd version` | Prints the release tag, commit, Go version, operating system, and architecture available in the build. |

`acd stats` may initialize the central statistics database. `acd gc` changes
the central registry, so run `acd list` or `acd repo list` first when you want
to inspect what is registered.

## Hook protocol

Harness templates call these commands automatically. Use `acd setup` instead
of wiring them by hand unless you are building an adapter.

| Command | Hook contract |
|---|---|
| `acd start` | Register a client session and start or refresh the repo daemon. Harnesses pass `--session-id`, `--harness`, and sometimes `--watch-pid`. |
| `acd stop` | Stop one repo, deregister one session, or stop every daemon with `--all`. `--force` bypasses session refcounts. |
| `acd wake` | Refresh a session heartbeat and nudge the daemon. It requires `--session-id` and does not bypass intent batch waits. |
| `acd touch` | Refresh a session heartbeat without signaling the daemon. Codex uses this for its turn-level `Stop` event. |
| `acd flush` | Refresh a heartbeat. With `--logical`, ask the next replay pass to drain the visible intent window now. |

Logical flush requires an already registered session. It bypasses only the
intent batch wait. Detached HEAD, Git operations, manual pauses, validation,
and replay safety still apply.

