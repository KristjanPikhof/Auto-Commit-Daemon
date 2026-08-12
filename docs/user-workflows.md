# User workflows

## First setup

~~~bash
acd setup --dry-run
acd setup
acd
~~~

The final command should report enabled and protected. Fresh setup needs no API
credential and makes no network request.

## Daily check

~~~bash
acd
acd history
~~~

`waiting` with `Current changes protected: yes` means work is safe and Git
publication is delayed. The final `Next:` line gives the exact command only
when action is necessary.

The human status output keeps the main questions separate:

| Output | Meaning |
|---|---|
| `State` | Overall state: `off`, `protected`, `waiting`, `publishing`, or `needs_action`. |
| `ACD protection` | Whether background protection is on. |
| `Current changes protected` | Whether the latest eligible changes have a completed durable checkpoint. |
| `Published to Git` | Whether all protected changes are now in local Git commits. |
| `Action needed` | Whether you need to do anything now. |
| `Status` | What ACD is doing or why it stopped. |
| `Next` | The next command, or `No action needed.` |

These fields are independent. Current changes can remain protected while Git
publication is waiting or needs recovery.

## Publication is delayed

Run `acd doctor`. ACD may be waiting to group related changes, for a Git
operation to finish, for a project check to pass, or for an AI provider to
recover. It handles ordinary delays automatically. Doctor tells you when a
restart or another command is required. Do not delete ACD databases, private
refs, ownership locks, or sockets. Completed checkpoints remain protected
while publication waits.

For an explicit all-changes drain, run:

~~~bash
acd commit-all --yes
~~~

The command freezes one target, waits until it is published, and shows how many
protected changes remain and how many commits have been created. Closing the
terminal only detaches the display. The background worker keeps publishing,
and the next `acd commit-all --yes` reconnects to the same run.

This command is also the normal way to let ACD include staged changes. ACD
verifies the full private checkpoint before clearing the staging area back to
`HEAD`. Working files do not change. The checkpoint still contains staged,
unstaged, and untracked content. Ordinary background publication never clears
staged changes.

Later edits are protected normally but do not expand or starve the frozen
target. ACD asks for attention only when safety proof is impossible, such as a
detached `HEAD`, unresolved conflict, active Git operation, external `HEAD`
race, missing Git object, or terminal replay barrier.

## Restore a checkpoint

~~~bash
acd history
acd restore cp-...
acd restore cp-... --yes
~~~

Preview first. Resolve staged overlap before applying. Restore leaves `HEAD`
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

`acd on` always replaces the managed worker with a new one and confirms a new
checkpoint. Use it to enable ACD, restart a stuck worker, or finish a compatible
upgrade after installing a newer CLI binary. ACD backs up and applies safe
state migrations during that upgrade. If it cannot prove that an upgrade or
recovery is safe, it leaves the previous state intact and tells you what to run.
Neither command deletes checkpoint history.

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

If status reports a durable publication block, preview and apply exact-chain
recovery:

~~~bash
acd support recover --dry-run
acd support recover --yes
~~~

Apply mode checkpoints every enabled linked worktree in the repository. It
then stops the shared repository worker, rechecks the plan, performs recovery,
and starts protection again. You do not need to run `acd off` first. Other
repositories keep running.

## Upgrade from v19

Run `acd setup`. The plan covers every registered repository. Missing worktrees
with unresolved captured state, failed SQLite checks, unsafe Git operations,
missing objects, insufficient disk, or ambiguous exact-chain proof block the
whole cutover.

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
