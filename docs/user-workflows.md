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

`waiting` with `Protected: yes` means work is safe and publication is delayed.
The final `Next:` line gives the exact command only when action is necessary.

Status separates checkpoint protection from Git state and publication
provenance:

| Output | Meaning |
|---|---|
| `retrying` | Replay hit a recoverable error and will retry automatically. A completed checkpoint remains protected. |
| `self_healing` | ACD is normalizing or rebuilding the durable publication plan. No command is needed. |
| `waiting_for_provider` | The frozen publication target is waiting for its bounded semantic attempt. The drain survives restart. |
| `event_fallback` | Semantic recovery did not progress, so ACD is publishing local atomic dependency groups. |
| `needs_attention` | A durable publication block needs inspection with `acd doctor`. |
| `busy` with a fresh heartbeat | The worker is draining pending work or completing another bounded operation. |
| `All changes committed in Git: yes` | The worktree is clean. This does not claim that ACD created its commits. |
| `Protected: yes` | The latest observed eligible tree is covered by a completed checkpoint. |
| `Latest protection checkpoint published by ACD: yes` | No checkpoint or pending event remains unpublished behind that checkpoint. |

These fields are independent. A clean worktree can be committed in Git by a
person or another tool. A completed checkpoint can remain protected while Git
publication is waiting. The `Published by ACD` result reports ACD provenance
for the latest completed checkpoint, not the origin of every commit in Git.

## Publication is delayed

Run `acd doctor`. Common waiting causes include the settle window, unsafe Git
state, verification failure, provider retry, or worker version mismatch.
Retrying planner output, deterministic fallback, and provider cooldown recover
automatically. No restart or configuration change is required. Do not delete
state, private refs, ownership locks, or sockets. Provider and verification
failures do not invalidate completed checkpoints.

For an explicit all-changes drain, run:

~~~bash
acd commit-all --yes
~~~

The command freezes one target, waits until it is published, and shows the
phase, remaining events, commit count, fallback mode, and latest safe error.
Closing the terminal only detaches the display. The worker keeps publishing,
and the next `acd commit-all --yes` attaches to the same active drain.

This command is also the staging consent boundary. ACD verifies the full
private checkpoint before resetting the index to `HEAD`. The worktree does not
change, and staged, unstaged, and untracked content remains represented by the
checkpoint and frozen target. Ordinary background publication never consumes
staged content.

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
