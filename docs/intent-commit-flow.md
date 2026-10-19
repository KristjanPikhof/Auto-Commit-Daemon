# Intent finish flow

Intent runs when you explicitly request `acd finish` under an approved Intent
configuration. Background protection needs no provider. Local is the offline
default; see [settings](settings.md) to select Intent.

~~~bash
acd finish "Add offline checkout"
acd finish --dry-run "Add offline checkout"
~~~

## Freeze the work

The worker obtains a fresh complete checkpoint and freezes the expected HEAD,
complete target tree, index identity, settings, and staged consent. The request
ID identifies that immutable input. Later saves receive their own checkpoints
and stay outside the target.

A dry run creates private planning and verification evidence and may call the
provider or approved project commands. It does not move the branch, index, or
working files. After a preview, an ordinary finish freezes a fresh target.

## Plan completed goals

The provider identifies completed goals before assigning changes. Each split
needs a useful review or revert boundary and a valid intermediate tree. Keep
implementation, callers, imports, tests, generated output, and relevant docs
together when they complete one goal. Put prerequisites before consumers or in
the same commit. Different files, directories, or save times do not justify a
split.

ACD accepts only exact recorded path versions. It rejects missing or duplicate
membership, unsafe dependencies, split rename groups, mixed author boundaries,
and fabricated intermediate content. Neutral recorded cycles can remain
provenance without creating a commit; a net-zero target creates no commits.
The final tree must equal the entire frozen target, including modes, symlinks,
binary objects, deletions, and unchanged entries.

## Bound the provider work

One attempt allows one planning exchange and at most one combined correction.
Both share the persisted `ai.timeout` deadline, five minutes by default. Messages
are part of the same plan; they do not get a separate retry loop.

The request contains bounded metadata, redacted supplied context, and approved
text diffs. ACD measures complete serialized input and fails oversized requests
instead of clipping evidence. Binary content is represented by metadata and
verified Git objects. See [providers and privacy](ai-providers.md).

Missing credentials, timeout, unavailable providers, or an invalid plan fail
the attempt. ACD keeps the work protected and does not switch to Local. A valid
plan whose splits cannot form safe complete trees can collapse to one checked
batch using the provider's grounded purpose and message.

`--message` preserves an exact valid user message. In Intent mode it still
requires provider planning; it is not an offline shortcut.

## Verify every tree

ACD materializes the complete proposed chain privately. Structural checks prove
recorded-version use, safe membership and ordering, and exact target equality.
They always run, including under `intent.verification=none`.

`fast` or `full` runs the approved configured project command for each candidate
in isolation. Missing commands are reported as missing. Cached evidence is
labelled reused and applies only to the identical tree, command, environment,
and policy. ACD does not claim that an imagined or unexecuted test passed.

## Publish once

After every candidate passes, ACD journals the complete chain and moves the
branch with one exact compare-and-swap. A failure cannot publish a partial
chain. The live worktree remains unchanged; staged content follows the explicit
[index consent rules](capture-replay.md#the-finish-target).

Closing the terminal detaches the display. Use the saved request ID to reconnect
or explicitly retry a failed attempt:

~~~bash
acd finish --retry REQUEST_ID
~~~

Retry keeps the original target and consent. Corrected credentials may be used
for the same approved provider; later file edits remain outside it. If the
branch or index proof no longer matches, ACD refuses publication. A worker
restart marks interrupted running work failed; exact prepared publication can
recover without a new provider call.

The [finish contract](finish-contract.md) specifies request states and results.
The former automatic queue, preset, retry, fallback, and repair design is
preserved in the [legacy Intent flow](legacy-intent-commit-flow.md).
