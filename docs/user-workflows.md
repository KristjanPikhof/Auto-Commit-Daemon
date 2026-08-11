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

Off performs a final checkpoint barrier. If protection cannot be confirmed it
stays enabled; only `acd off --force` accepts that risk. Neither command deletes
checkpoint history.

## Diagnose locally

~~~bash
acd doctor
acd support diagnose --json
acd support logs
acd support bundle
~~~

Normal output contains privacy-safe counts. Support output stays local and
redacts credentials, provider responses, and raw source content by default.

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
