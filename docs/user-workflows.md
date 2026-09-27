# User workflows

## Install ACD

Homebrew is the shortest installation path:

~~~bash
brew install KristjanPikhof/tap/acd
~~~

The verified release installer is the alternative:

~~~bash
curl -fsSL \
  https://raw.githubusercontent.com/KristjanPikhof/Auto-Commit-Daemon/main/scripts/install.sh |
  bash
export PATH="$HOME/.local/bin:$PATH"
~~~

See the [README requirements](../README.md#requirements) before enabling your
first repository.

## Set up ACD once

~~~bash
acd setup
~~~

Setup configures the shared runtime, integrations, grouping, commit format,
repair limit, and provider. AI semantic commits with Everyday checks are
recommended. Local automatic commits are an explicit offline alternative with
more limited grouping and messages.

An OpenAI-compatible setup asks for its endpoint, model, and bearer token. It
sends one fixed synthetic request after you approve the plan, without sending
repository content during that test.

`acd setup` verifies the running installation before reporting readiness.
In a terminal it then offers to protect the current repository, with separate
consent. It confirms protection only after a completed checkpoint.

## Enable a repository

~~~bash
cd /path/to/repository
acd on
acd status
~~~

`acd on` registers and enables only that repository, then waits for a verified
checkpoint. `acd status` should report protection on, current changes
protected, and no action needed. Your setup choices become the defaults for
repositories you enable later.

## Daily check

~~~bash
acd
acd history
~~~

`Branch commits` waiting with `Current changes saved: yes` means work is safe and Git
publication is delayed. The final `Next:` line gives the exact command only
when action is necessary.

The human status output keeps the main questions separate:

| Output | Meaning |
|---|---|
| `Protection` | Whether background protection is on. |
| `Current changes saved` | Whether the latest observation has a completed durable checkpoint. |
| `Branch commits` | Committed work or the reason protected changes are waiting. |
| `Recovery` | Changes preserved separately from normal branch history. |
| `Next` | An actionable command, or `No action needed.` |

`acd status --verbose` also shows the provider, queue, active target, phase,
last queue movement, and worker heartbeat. A heartbeat shows liveness; it does
not prove publication progress.

These fields are independent. Current changes can remain protected while Git
publication is waiting or needs recovery.

For a quick check, run `acd list`. A changing `QUEUE` or `TARGET` proves
durable progress. If neither moves, `PHASE` shows whether ACD is waiting for a
provider retry, calling the provider, running verification, or recovering.
`acd status` gives the same state in full sentences and says whether any action
is actually required.

## Publication is delayed

Run `acd doctor`. ACD may be waiting to group related changes, for a Git
operation to finish, for a project check to pass, or for an AI provider to
recover. A failed project check normally starts automatic checkpoint
replanning; `Publication phase` says so while ACD keeps working. Doctor tells
you when a restart or another command is actually required. Do not delete ACD
databases, private refs, ownership locks, or sockets. Completed checkpoints
remain protected while publication waits.

For an explicit all-changes drain, run:

~~~bash
acd commit-all --yes
~~~

The command freezes one target, waits until it is published, and shows how many
protected changes remain and how many commits have been created. Closing the
terminal only detaches the display. The background worker keeps publishing,
and the next `acd commit-all --yes` reconnects to the same run.

If semantic planning cannot make progress, the worker recovers on its own. It
records a restart-safe transition, removes unsafe mixed membership, and asks
the Intent planner to regroup only the remaining target. If the new plan still
cannot move, ACD commits the smallest safe dependency group locally, even when
that group contains one change. It then tries Intent planning again from the
new `HEAD`. Existing commits are not rewritten.

If the configured semantic provider is unavailable, ACD keeps the selected
local group protected and retries its locked commit message. It does not
publish a generic filename-based message. Recovery also runs during normal
background publication. It does not require another `commit-all`, a database
purge, or a manual Git commit.

This command is also the normal way to let ACD include staged changes. ACD
verifies the full private checkpoint before clearing the staging area back to
`HEAD`. Working files do not change. The checkpoint still contains staged,
unstaged, and untracked content. Ordinary background publication never clears
staged changes.

Later edits are protected normally but do not expand or starve the frozen
target. Status reports `planning` during pending-only Intent replanning,
`event_fallback` during one local unlock, and `self_healing` while restoring a
restart-safe recovery stage. No action is needed in those states. ACD asks for
attention only when safety proof is impossible, such as exhausted verification
recovery, ambiguous self-publication, an unresolved conflict, a missing Git
object, an oversized or cyclic hard dependency component, or unsafe branch
ownership. Detached `HEAD`, a manual pause, and an active Git operation wait
without discarding protected work.

## Restore a checkpoint

~~~bash
acd restore                 # terminal picker
acd history                 # IDs for scripting
acd restore cp-...
acd restore cp-... --yes
~~~

The terminal picker shows retained completed checkpoints, then the selected
file preview and a separate apply confirmation. Outside a terminal, an ID is
required. Preview first. Resolve staged overlap before applying. Restore leaves `HEAD`
and the index unchanged and prints an undo command using the pre-restore
checkpoint.

If status reports an interrupted post-restore checkpoint:

~~~bash
acd support repair
acd support repair --yes
~~~

Repair applies only if the current protected tree still exactly matches the
restore target.

## Turn protection off and on

~~~bash
acd off
acd on
~~~

`acd off` takes a final checkpoint, disables the repository, and waits for its
managed worker to stop. If the checkpoint cannot be confirmed, ACD stays on.
Use `acd off --force` only when you accept that risk.

`acd on` registers an unknown repository or re-enables a disabled one. It
backs up and migrates only that repository when needed, replaces its managed
worker, and confirms a new checkpoint. If the shared runtime does not exactly
match the CLI, run `acd setup` first. If ACD cannot prove that activation or
recovery is safe, it leaves the repository unregistered or disabled when
possible and tells you what to run. Neither command deletes checkpoint history.

## Diagnose locally

~~~bash
acd doctor
acd support diagnose --json
acd support logs
acd support bundle
~~~

Start with `acd doctor`. It shows the worker state, the latest safe startup
error, and the command that matches the problem. Use `acd support diagnose
--json` when you need the full local report, and `acd support logs` when doctor
asks you to inspect startup details. Credentials, provider responses, and raw
source content are redacted by default.

ACD completes an old publication run automatically when every protected change
is already published or preserved in recovery. This also works after switching
branches or restarting the worker.

If ACD cannot prove that the run is complete, preview the remaining recovery:

~~~bash
acd support recover --dry-run
acd support recover --yes
~~~

Apply mode checkpoints every enabled linked worktree in the repository. It
then stops the shared repository worker, rechecks the plan, performs recovery,
and starts protection again. You do not need to switch to an old branch or run
`acd off` first. Other repositories keep running.

## Upgrade from v19

Run `acd setup`. The incompatible-upgrade plan covers enabled repositories
only. Disabled records remain disabled and their databases are migrated later,
one at a time, by `acd on`. Missing enabled worktrees with unresolved captured
state, failed SQLite checks, unsafe Git operations, missing objects,
insufficient disk, or ambiguous exact-chain proof block the global cutover.

Before global commit, any failure restores database, service, integration,
configuration, and registry preimages. A migration bridge ref containing an
otherwise unrepresented concurrent edit is retained with a recovery manifest
rather than deleted.

After commit, recovery is forward-only through `acd support repair`.

## Uninstall

~~~bash
acd uninstall --dry-run
acd uninstall
~~~

Default uninstall preserves repository databases and private refs. To remove
protected data, review the separate purge plan and provide the second explicit
confirmation. Never manually remove ownership locks or broad ACD directories.
