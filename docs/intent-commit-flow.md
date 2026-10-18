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

Intent also retains useful earlier checkpoint versions from the same worktree,
branch, and HEAD. ACD considers at most 256 retained current checkpoints; a
larger window reports that its evidence is too large instead of clipping it.
Restore, migration, branch movement, and legacy captures end that window.
Private finish refs keep the selected versions available across pruning and
explicit retries. Old requests without this evidence keep their original target.

Only paths changed in the final eligible target can supply intermediate versions.
Renames and deletions stay complete final transitions. Requests with staged
consent use only the frozen final selection, so earlier unstaged versions cannot
enter their commits. ACD never invents a missing intermediate file version.

A dry run creates private planning and verification evidence and may call the
provider or approved project commands. It does not move the branch, index, or
working files. After a preview, an ordinary finish freezes a fresh target.

## Plan completed goals

The provider identifies useful steps within the completed task before assigning
changes. A feature can contain a behavior-preserving refactor, the feature it
enables, and a broader policy change. Dependencies determine their order; they
do not automatically make them one commit. Each step needs a useful review or
revert boundary and a valid intermediate tree. Keep its callers, tests, imports,
generated output, and relevant docs together. File names, save times, and line
counts do not justify a split or a fixed commit count.

Generated messages name the change and include a short body explaining the
reason or concrete impact from the supplied evidence. Reasons must appear in
Git, not just internal planning metadata. Test counts and lists of components
do not explain a decision. Available corrections stay with the step they correct.

Supply completion context describing the problem, approach, and constraints.
When text diffs are not shared, a new Intent finish needs context or an exact
message; filenames alone cannot establish intent. A missing-context failure
needs a new finish with context, since retry preserves the original input.
Diff sharing remains an explicit settings choice. Previews identify plans made
without code diffs and show each proposed boundary.

The planner chooses which changes belong together. ACD orders the commits by
their version dependencies and replays selected versions in recorded order.
Conflicting groups need a corrected plan; ACD does not merge them. It accepts
only exact recorded path versions and rejects missing or duplicate membership,
unsafe dependencies, split rename groups, mixed author boundaries,
and fabricated intermediate content. Neutral recorded cycles can remain
provenance without creating a commit; a net-zero target creates no commits.
The final tree must equal the entire frozen target, including modes, symlinks,
binary objects, deletions, and unchanged entries.

## Bound the provider work

One attempt allows one planning exchange and at most one combined correction.
Both share the persisted `ai.timeout` deadline, five minutes by default. Messages
are part of the same plan; they do not get a separate retry loop. Correction
feedback includes all detected message and membership conflicts together.

The request contains bounded metadata, redacted supplied context, and approved
text diffs. ACD measures complete serialized input and fails oversized requests
instead of clipping evidence. Binary content is represented by metadata and
verified Git objects. See [providers and privacy](ai-providers.md).

Missing credentials, timeout, unavailable providers, or an invalid plan fail
the attempt. ACD keeps the work protected and does not switch to Local. A single
batch needs an explanation of why the recorded changes lack useful safe steps;
belonging to one feature is not enough. A failed intermediate check fails the
whole request rather than silently combining its commits.

`--message` requests one complete batch with that exact valid message and disables
automatic splitting. Use the quoted purpose for grouping context. In Intent
mode an exact message still requires provider planning.

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
