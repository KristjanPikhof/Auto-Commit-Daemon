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
enter their commits.

When no useful earlier versions exist, a new Intent request with approved `fast`
or `full` checks can split existing text files using exact lines from the frozen
base and target. Each insertion or deletion belongs to one goal. ACD constructs
the intermediate files locally; the provider cannot supply replacement code.
Go and Swift scope information prevents identical lines in different functions
from matching. Swift files require a working local Swift compiler. New files,
renames, binary files and mode changes remain whole transitions. Older requests,
exact messages and structural-only requests retain their existing version rules.

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

Keep a new field, type, or method with its consumers or before them. Reviews
retain those interface names so the planner can identify the dependency.
A workflow's reconnection and stale-callback guards usually complete that
workflow; separate test assertions alone do not justify separate commits.

Generated messages name the change and include a short body explaining the
reason or concrete impact from the supplied evidence. Reasons must appear in
Git, not just internal planning metadata. Test counts and lists of components
do not explain a decision. Available corrections stay with the step they correct.

Each generated goal also supplies a `reason`: the previous problem or constraint,
the chosen mechanism, and why it helps. ACD requires that explanation in the
published body, allowing line wrapping and bullet formatting. This prevents a
planner from leaving its explanation only in private metadata. The provider
still judges whether the explanation and boundary follow from the evidence;
ACD cannot prove a motivation from message wording alone.

ACD wraps generated body bullets at 72 characters without changing their words
or the subject. This leaves the provider's correction available for decisions
and dependencies. Invalid bullet structure and overlong subjects still fail
validation. Exact `--message` text must already meet the format rules.

For example, ignoring unchanged API-key writes prevents a focus change from
disconnecting a verified session. That bug fix has a separate purpose from a
Settings redesign, even when both were discovered during the same UI audit.
A shortcut recorder, its saved bindings, conflict checks and menu hints can
complete one feature.

When the available versions or source selections cannot separate independent
purposes safely, the provider can return `blocked_reason` with no goals. Finish fails with
the explanation and keeps the work protected. It does not combine unrelated
behavior just to produce a commit. Finish coherent, verified milestones while
working so their versions remain available for later planning. An unchanged
retry cannot supply missing versions or missing context.

Supply completion context describing the problem, approach, and constraints.
Intent requires approved redacted diffs for every recorded text change. Setup
includes sharing in its approval review. Existing installations must approve it
in `acd config`; upgrades and retries keep the original privacy settings.
Completion context supplements the code and is required for binary-only work
unless an exact batch message is supplied. Missing approval or context needs a
new finish after correcting the input, since retry preserves the original
settings and purpose. Previews show each proposed boundary.

The planner chooses which changes belong together. ACD orders recorded versions
by their dependencies. Source selections use the provider's prerequisite order.
Conflicting groups need a corrected plan; ACD does not merge them. It accepts
only recorded versions or verified selections of frozen source bytes. It rejects missing or duplicate membership,
unsafe dependencies, split rename groups, mixed author boundaries,
and provider-written replacement content. Neutral recorded cycles can remain
provenance without creating a commit; a net-zero target creates no commits.
The final tree must equal the entire frozen target, including modes, symlinks,
binary objects, deletions, and unchanged entries.

## Bound the provider work

Small requests use one planning exchange and at most one combined correction.
Large requests first review the complete redacted evidence in bounded batches,
then make one global plan across all recorded changes. At most 128 evidence
reviews are allowed, with at most four immutable provider exchanges at once.
The worker reserves each call before releasing its operation gate and continues
protecting later edits while reviews run. All calls share one persisted deadline, using `ai.timeout` (five minutes by
default) or an explicit `--timeout 15m` for the new attempt. Messages
are part of the same plan; they do not get a separate retry loop. Correction
feedback includes all detected message and membership conflicts together. A
source plan rejected by project checks can use that same correction. The
feedback identifies the failed step and diagnostic names already present in
approved redacted diffs. Raw command output stays local. Every corrected
candidate runs its checks again within the original deadline.

The request contains bounded metadata, redacted supplied context, and approved
text diffs. Each encoded review is bounded to 512 KiB. The global catalogue
and its correction can use up to 2 MiB; `ai.max_input_bytes` can lower either
limit. A large file is divided into UTF-8 fragments without
losing bytes. Every fragment gets a bounded factual observation before planning;
ACD validates complete fragment coverage and records content digests, summaries,
review-call counts, and attempted request sizes. Each review contains at most 32 fragments so its output stays manageable.
A malformed or overlong review can be corrected once within the same call
budget and deadline. ACD reserves context space for the feedback and retains
accepted reviews. Provider transport and protocol failures still end the attempt.
OpenAI-compatible requests enable strict schemas with a summary length pattern.
Each summary can use up to 4,096 characters for behavior, dependencies, and
other details needed by the global planner. Short notes are enough for simple
fragments. ACD checks the complete reply locally and never cuts a summary to
make it pass. Combined requests still obey the configured context limits.
The global planner receives all original change identities and observations
together, without local cache digests. Its larger catalogue and review allowance support thousands
of meaningful goals without enlarging raw-diff review calls. Commit counts come
from useful boundaries, not review batches. Provider context limits and the
attempt deadline still apply; a large batch may need an explicit longer timeout.
Fragments, files, and review calls do not prescribe commit boundaries. Raw aggregate evidence
still obeys `ai.max_input_bytes`; metadata or a global plan that cannot fit fails
instead of clipping evidence. Binary content is represented by metadata and
verified Git objects. See [providers and privacy](ai-providers.md).

Missing credentials, timeout, unavailable providers, or an invalid plan fail
the attempt. ACD keeps the work protected and does not switch to Local. A single
batch needs an explanation of why the recorded changes lack useful safe steps;
belonging to one feature is not enough. Failed intermediate checks never publish
a partial chain or silently combine commits. A source plan that still fails
after the available correction ends the request.

`--message` needs an explanatory body. It requests one complete batch with that
exact valid message and disables
automatic splitting. Use the quoted purpose for grouping context. In Intent
mode an exact message still requires provider planning.

## Verify every tree

ACD materializes the complete proposed chain privately. Structural checks prove
recorded versions or exact frozen-byte selections, safe membership and ordering,
and exact target equality.
They always run, including under `intent.verification=none`.

Structural verification does not compile code. To catch a consumer whose field
or method appears only in a later commit, configure an approved project command
under `fast` or `full`.

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

Validated reviews are saved after each accepted batch. An explicit retry reuses
them only when the frozen evidence, redacted fragments, provider configuration,
and review instructions still match. It reviews only missing evidence, then
creates a fresh global plan. Restart never starts that work automatically.
Reviews saved by older builds without this fingerprint cannot be reused.
Valid completed reviews survive a parallel peer's failure. A successful retry
preview also keeps its review evidence for a later publication attempt using
the same request ID; that attempt still creates a fresh global plan.

For a large batch, give a new attempt more planning time without changing its
saved target or provider settings:

~~~bash
acd finish --retry REQUEST_ID --timeout 15m
~~~

`--timeout` accepts 1 second through 1 hour. Reconnecting to a running attempt
keeps its existing deadline. Progress reports reviewed and reused fragments,
review calls, and time remaining. Call budgets apply to new exchanges in each
explicit attempt; reused reviews consume no calls.

When an attempt exhausts its planning time, finish reports `provider_deadline`
and suggests a longer bounded retry. Other provider failures keep their own
cause. The suggested command is optional and starts no background work.

Retry keeps the original target and consent. Corrected credentials may be used
for the same approved provider; later file edits remain outside it. If the
branch or index proof no longer matches, ACD refuses publication. A worker
restart marks interrupted running work failed; exact prepared publication can
recover without a new provider call.

The [finish contract](finish-contract.md) specifies request states and results.
The former automatic queue, preset, retry, fallback, and repair design is
preserved in the [legacy Intent flow](legacy-intent-commit-flow.md).
