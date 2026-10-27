# Command reference

ACD saves eligible changes continuously to private checkpoints. Run `acd finish`
when a complete piece of work is ready for local Git commits. Commands never push.
Source changes do not update the installed CLI or workers; runtime upgrades use
an explicitly reviewed `acd setup` plan.

## Public commands

Bare `acd` is `acd status`. These are the public commands and the first-release
compatibility alias:

| Command | Purpose and mutation boundary |
|---|---|
| `acd config` | Review and save global or repository settings and permissions. |
| `acd setup` | Review installation, integrations, and an explicit runtime upgrade. |
| `acd setup agents` | Guide hook setup, agent instructions, and optional repository protection. |
| `acd status` | Read current protection, branch outcome, and explicit request state. |
| `acd on` | Enable this worktree and confirm a complete checkpoint. |
| `acd off` | Save a final checkpoint, disable protection, and stop its worker. |
| `acd list` | Read the global protection dashboard. |
| `acd finish [PURPOSE]` | Plan, check, and publish the frozen complete batch locally. |
| `acd commit-all` | First-release deprecated delegate to finish. |
| `acd history` | Read saved checkpoints; activity and rewrite are subcommands. |
| `acd restore [ID]` | Preview a checkpoint restore, then apply after confirmation. |
| `acd doctor` | Read problems and safe next steps; `--bundle` writes a sanitized archive. |
| `acd uninstall` | Review removal of owned runtime files; preserve data by default. |

`--repo PATH` resolves a directory inside the selected Git worktree to its root.
Linked worktrees have separate protection and request identity. Global operations
such as `list` reject `--repo`; they already operate across repositories.

## Setup, on, and off

~~~bash
acd setup --dry-run
acd setup
acd setup agents
acd on
acd off
~~~

Repeating `acd on` keeps a healthy worker running. It waits for the selected
worktree to become ready and confirms a complete checkpoint.

`on` saves current changes, `finish` creates branch commits from the current
protected work, and `history rewrite` takes its input from current Git commits.
Old ACD archive records do not block these commands.

Setup works outside Git. Fresh setup defaults to Local mode and imperative
messages, with no API key or connection test required. Intent setup asks for
its provider settings and any necessary permissions. Ordinary setup and reviewed settings apply do not
contact the provider. Any separate provider probe requires an explicit request;
it is not a prerequisite for saving.

For unattended installation, review and retain the exact plan ID first:

~~~bash
acd setup --dry-run --non-interactive --strategy local --json
acd setup --yes --non-interactive --expect-plan PLAN_ID --strategy local
~~~

Use `--integrations auto|none|NAME,...`, `--strategy local|intent`,
`--commit-format imperative|conventional`, and the provider's `--provider`,
`--base-url`, `--model`, and `--ca-file` options for the reviewed plan. Bearer
credentials come from `ACD_AI_API_KEY` or `--credential-stdin`, never a command-line
secret argument. Intent endpoint access and redacted code sharing
require their exact permissions. HTTP additionally needs
`--confirm-insecure-http`; URLs containing credentials, queries, or fragments
and provider redirects are refused.

Setup preview writes nothing and runs no provider, service, migration, or Git
mutation. In repositories created with the finish workflow, ordinary background
saves do not invalidate a reviewed setup plan. Changes to preserved legacy
evidence still require a new preview. Apply preserves database backups and private
provenance during an incompatible upgrade. Disabled repositories keep their old
state until explicitly activated. Ambiguous legacy publication can remain
`needs_attention` without blocking current protection, finish, or history rewrite. Current read-only
commands inspect old records without upgrading them.

Setup installation and repository enablement are separate consent steps.
For coding tools, run `acd setup agents`. It previews hook changes, prints the
copy-ready `AGENTS.md` section, and offers to add it to a chosen instruction
file. Use `--manual` for instructions and commands without changes, or
`--dry-run` to preview specific options. Non-terminal runs preview by default;
`--yes` applies the displayed options. Adding instructions and enabling
protection are explicit choices. See [the agent setup guide](multi-tool.md).
After installation, use `acd config` to change the finish mode, provider, or
credentials. Setup upgrades preserve those settings and reject first-setup
preference flags rather than ignoring them.
Compatibility `setup --repo PATH` retains global scope and prints the follow-up
`acd on --repo PATH`. Unattended setup does not grant repository consent.

On a fresh machine, `on` can bootstrap the missing runtime with offline Local
defaults, enable the selected worktree, and confirm protection. An existing
incompatible runtime requires an explicit reviewed `acd setup` upgrade; it is
not silently replaced. Normal protection uses no AI or project checks
and creates no new publication request. Startup can recover an already authorized
prepared publication only through its exact saved branch/index proof. Unknown or
disabled worktrees are never enabled by hooks. `off` waits for a final complete checkpoint before disabling; failure keeps
protection enabled. `off --force` explicitly accepts stopping without confirmed
current coverage. Saved history remains available.

## Console output

Normal output leads with current protection and Git commit status.
`Action required` marks a problem ACD cannot safely resolve on its own.
Suggestions such as finishing saved work or configuring hooks are labeled
`Optional`. ACD handles protection scans and automatic capture retries itself.
Creating commits and approving recovery remain explicit user decisions.
Use `status --verbose` or `--json` for saved request IDs, hashes, checks, and
background process details.
A saved checkpoint does not mean a Git commit was created.

An old failed finish does not require a retry when current work is already
committed. Normal status omits that failure; verbose output and JSON retain the
saved request. When current work is ready, a new `acd finish` uses current files.
Setup prints each phase once, with progress reminders only while a phase takes
time, instead of printing every repository check.

`list` prints a compact table once and exits, leaving terminal scrollback
available. Each row shows a repository's saved coverage, pending changes, mode,
and status. Repositories requiring action remain visible, with next steps below
the table. `--all` includes idle repositories; `--verbose` shows detailed
repository blocks. Use `list --watch` for live refresh until Ctrl-C. Redirected
watch output appends dated updates without terminal screen controls.
If a repository read times out, its row shows `checking protection`, with `-`
for saved coverage and `?` for pending changes. An incomplete read does not
require a repair.
`--once` remains an explicit alias for the default snapshot. `on --quiet` hides
the startup wait.

A failed finish can be retried after its cause is resolved. A finish needing
attention points to `acd doctor`; retry does not bypass missing safety evidence.
A retry can restart the saved request and always keeps its original changes.

When a finish remains interrupted after branch or index changes, doctor points
to `acd support recover --force --dry-run`. Review that plan, then use
`acd support recover --force --yes` to preserve the request and release its
block on new finishes. Recovery backs up the database and verifies the saved
private snapshots. It preserves the original request and publication evidence;
it does not claim completion or consume staging. Current files and branches
stay unchanged. Review `git diff` and `git diff --cached` before a new finish.
Missing or changed snapshot proof keeps the request blocked.

Check results say whether a project command ran, an earlier result was reused,
or no project checks were configured. Structure checks alone do not mean the
project's tests passed.

## Finish

~~~bash
acd finish "Add offline checkout"
acd finish --message "Update setup guidance"
acd finish --dry-run
acd finish --include-staged
acd finish --retry REQUEST_ID --timeout 15m
~~~

Finish itself authorizes local publication; it needs no `--yes`. Local mode
creates one complete batch commit without provider or network access. Intent
uses the approved provider to plan completed goals and valid intermediate trees.
A provider failure ends this request; it never switches to Local automatically.

The quoted purpose supplies context about what changed and why. Intent uses it
to identify useful steps and explain decisions. Intent requires approved
redacted code diffs; binary-only work also needs context or an exact message. `--message` supplies the complete
subject and optional body for one batch commit and disables automatic splitting.
Generated Intent messages include a short reason or practical impact in the body.
Both modes support imperative
subjects (`Preserve staged content`) and scope-free conventional subjects
(`fix: preserve staged content`). Change mode or style through reviewed settings.

Each request freezes a fresh complete checkpoint, expected branch/HEAD, target
tree, settings, and index identity. The entire chain must pass its checks before
one branch update. Later edits remain protected for a later finish. Detached
HEAD, conflicts, active Git operations, moved refs, and ambiguous same-branch
linked worktrees prevent publication without disabling protection. An empty
finish succeeds without creating a commit; an attached unborn branch can receive
its first commit.

Finish announces that later edits belong to the next finish before preparing
commits. If files still have uncommitted changes afterward, it shows their count
and the saved snapshot's time. Use `git status --short` to inspect them, then run
`acd finish` when that work is ready. Excluded files remain uncommitted.
When nothing remains, finish reports a clean working copy. Repeated project-check
results appear once; JSON keeps each commit's result.

Ordinary finish refuses staging. `--include-staged` approves only the exact
current selection, including staged-only bytes and deletions. Unstaged changes
on those paths remain live for later work. Excluded staged paths are refused.
Later staging is preserved, including when publication needs crash recovery.
Finish never checks out candidate trees over your live files.

`--dry-run` plans and checks without moving the branch or index. It can write
private checkpoints and request evidence and can contact the Intent provider.
To publish afterward, run a new ordinary finish, which freezes current work.

An identical active request reconnects. A different request reports busy.
Disconnecting the terminal leaves the bounded request running; status/list show
its durable result. A worker restart fails an unfinished planning/checking
attempt without an automatic provider retry. A prepared publication recovers
only through exact saved branch/index proof.

`--retry ID` retains the failed target and starts one bounded attempt. It accepts
`--dry-run`, `--repo`, `--json`, `--quiet`, and `--timeout`, but rejects a new purpose, message,
or staged selection. Changed branch/index ownership or approved settings require
a new finish; retry never absorbs later files. Credentials can be refreshed for
an explicit attempt with the same approved provider and privacy configuration.

`--timeout 15m` sets the planning budget for a new attempt (1 second through
1 hour); otherwise `ai.timeout` applies. Reconnecting keeps the existing
deadline. Validated evidence reviews are saved durably and reused by explicit
retry only when the exact frozen evidence, provider configuration, and review
instructions still match. Progress shows completed and reused fragments.
Successful retry previews retain this evidence for a later real retry of the
same ID. Each attempt still makes a fresh global plan.

Verification always proves structure and target equality. `none` and
`structural` do not imply that project tests ran. `fast` and `full` run the
approved command in isolated candidate trees, under one bounded chain deadline.
Output reports which checks actually ran. See the
[finish contract](finish-contract.md) for exact request and safety rules.

### Deprecated commit-all

For the first finish-based release only, `commit-all` remains visible and delegates
to finish. Bare `commit-all` directs you to finish; it requires `--dry-run` or
`--yes`. `commit-all --yes` preserves its explicit consent to include the current
staged selection. It has no separate drain, publication scheduler, or retry loop.

## Status and list

~~~bash
acd status
acd status --verbose
acd list --once
acd list --all --verbose
acd list --json
~~~

Status distinguishes enabled protection, complete eligible coverage, saved work
still awaiting finish, branch commits, explicit request results, and legacy
preservation. Protection and worker liveness are separate facts. A failed finish
can coexist with healthy protection and later saved edits.

List refreshes in a terminal; a pipe or `--once` gives one snapshot. It shows
recently active repositories and unfinished work; `--all` includes every enabled
repository. Ctrl-C leaves the last completed dashboard visible.
`--watch --interval 5s` explicitly streams human snapshots. JSON is
one-shot and rejects streaming combinations. A bounded read can report unknown
coverage rather than invent commitment. Historical resolved-work booleans keep
their deprecated meanings; use `publication_outcome` for current branch,
checkpoint, finish/history request, and legacy facts.

`acd repo list` is the static registration inventory, including disabled and
missing entries. Status, list, doctor, and diagnose do not migrate databases,
resolve credentials, probe providers, or modify registry data.

## Settings

~~~bash
acd config
acd config --repo /path/to/repo
acd config --accessible
acd config get commit.strategy
acd config set commit.strategy intent
acd config set commit.format conventional
acd config reset commit.format
acd config credentials status
~~~

`config` and `config edit` open the same editor, initially at global scope. Use
its scope menu, `--scope global|repo`, or `--repo PATH`. `--accessible` uses linear
prompts. The editor rejects `--json`.

`get`, `set`, and `reset` default to repository scope inside Git and global scope
outside. They accept `--scope repo|profile|global`; profiles use `--profile NAME`.
`get` shows resolved values and their sources without writing state. `set` and
`reset` save a draft; follow the returned command to review and apply that scope.

Saving in the editor validates the exact approved settings and permissions and
applies them synchronously. It runs no provider probe or project command.
A separate provider probe requires an explicit request. In-flight finish requests keep their
frozen approved revision; the next request uses newly applied settings.
Credential status never prints the token; `set` prompts securely and `remove`
removes the protected stored credential. Local mode needs no credential.

## History, rewrite, and restore

~~~bash
acd history
acd history activity
acd history explain
acd history rewrite --last 5 --plan-out goals.json
acd history rewrite --show-plan goals.json
acd history rewrite --apply goals.json --dry-run
acd history rewrite --apply goals.json --yes
acd restore CHECKPOINT_ID
acd restore CHECKPOINT_ID --yes
~~~

History reads completed checkpoints. Activity/explain also retain readable old
provenance. A saved snapshot is not evidence that its changes were committed.

Rewrite is explicit in both modes. Local uses deterministic existing order and
grouping; Intent uses bounded goal planning. Selection is contiguous linear
history with `--last`, `--from-sha`, `--from-nr`, `--range-sha`, or `--range-nr`.
`--messages-only` keeps one output per selected commit. `--new-branch NAME`
reconstructs onto a new branch while preserving the source. Saved-plan show
makes no provider call. Apply checks exact saved evidence, author boundaries,
per-commit trees, final tree, source backup, and native index ownership.
Plans are rejected when stale; preview never authorizes apply. See
[history rewrite](rewrite-commits.md) for plan editing and recovery limits.

Restore accepts a unique checkpoint prefix. Bare restore in a terminal opens a
picker; outside a terminal, supply an ID. Preview reports changed paths and
staged overlaps. Apply revalidates the preview, creates an undo checkpoint, and
preserves HEAD and native staging. It changes live files only after confirmation.
Restore never runs finish automatically. Partial snapshots cannot be restored
as complete worktree states.

## Doctor, support, and repository maintenance

~~~bash
acd doctor
acd doctor --bundle
acd support diagnose --json
acd support logs --lines 100
acd support bundle
acd support repair
acd support recover --dry-run
acd repo list
acd repo remove --dry-run
acd repo gc
acd repo stats --since 30d
~~~

Doctor reads saved configuration, worker and watcher facts, coverage, and request
failures and prints a safe next step. `support logs` is raw local JSONL; review
paths and contents before sharing. Use `support bundle` for a sanitized archive.

Support repair previews a provable interrupted restore; `--yes` completes its
checkpoint only when current files still match. Support recovery inspects
preserved legacy work and exact recorded proofs. Any apply is explicitly reviewed;
it cannot start the retired planner, infer a history rewrite, or replace live
files or staging to repair a finish failure. New finish retries use `finish
--retry ID`. Follow the reported remedy for ambiguous legacy attention.

## Uninstall and compatibility

`acd uninstall --dry-run` previews removal. Apply stops owned workers and removes
owned integrations, service/binary files, and desired enabled state while
preserving databases and private refs. Data removal additionally requires
`--purge-data` and `--confirm-purge-data`. Unattended apply needs
`--yes --non-interactive --expect-plan PLAN_ID`.

For a clean start in every registered worktree, preview
`acd uninstall --dry-run --purge-data`, then apply with
`acd uninstall --purge-data --confirm-purge-data`. This removes ACD directories
and databases under Git, all private `refs/acd/` checkpoints and archives, and
ACD's runtime and registrations. It preserves project branches, tags, commits,
the index, and working files. Unavailable worktrees are reported rather than
deleted through an unverified path.

Advanced `support` and `repo` namespaces are callable but hidden from root help.
Internal worker/supervisor/hint entrypoints serve installed integrations.
Earlier root aliases such as `start`, `stop`, `flush`, `configure`, `logs`, and
`rewrite-commits` are removed. Use current namespaces. The deprecated
`commit-all` delegate is the first-release compatibility window, not a second
engine. `acd --version` reports the CLI version.

## Flags, JSON, and exits

| Flag | Meaning |
|---|---|
| `--repo PATH` | Select a worktree; rejected by global-only commands. |
| `--json` | Emit one machine-readable envelope; rejected by interactive editors and incompatible streaming routes. |
| `--quiet` | Hide optional progress, preserve the final result. |
| `--log-level LEVEL` | Set invocation logging to debug, info, warn, or error. |

JSON uses `ok`, `state`, `changed`, ordered `actions`, `next_action`, `data`, and
`error`. Errors contain `code`, safe `message`, `retryable`, and redacted details.
Request `data.state` is separate from the top-level repository state. Finish
reports its request/attempt, frozen checkpoint and target, proven publication,
verification, and failure reason. A saved or completed preview never means
branch publication. A retryable error requires an explicit attempt.

| Exit | Meaning |
|---|---|
| `0` | Completed with no required action. |
| `1` | Unexpected internal failure. |
| `2` | Invalid command, arguments, or retry identity. |
| `3` | Valid result or refused mutation requiring user action. |
| `4` | Transient worker/provider unavailability or detached result waiting. |

See [settings](settings.md), [providers and privacy](ai-providers.md), and
[user workflows](user-workflows.md) for detailed examples.
