# Protection and publication

## Observation and coverage

Every accepted watcher event, complete poll, or optional semantic hint advances
`observation_epoch` and immediately marks protection incomplete. A worker may
report `protected=true` only when:

- no scan or checkpoint is in flight;
- no eligible path failed reading or stabilization; and
- `covered_epoch == observation_epoch`.

When filesystem watching is enabled, its first event wakes the worker
immediately. More events in the same burst use a 100 ms trailing debounce with
a 500 ms hard limit, so continuous formatter activity cannot postpone
observation indefinitely. The complete safety poll starts at 750 ms after
activity and doubles while the repository stays idle, up to two minutes. It
repairs watcher loss and remains the coverage authority.

## Eligible scope

Git-ignored, sensitive, and generated cache paths remain outside the protected
scope. Regular files above `capture.max_file_bytes` stream into durable Git
objects with bounded memory. File size alone no longer stops capture. The
setting keeps its name for compatibility and now controls buffering.

The generated-cache guard also excludes `-Xcc/` compiler index directories,
including output accidentally written inside an Xcode project. Existing
captured work remains protected until publication or explicit recovery resolves
it; adding an exclusion does not discard queued captures or remove tracked files.

An unreadable or changing eligible path makes coverage incomplete. ACD still
saves readable files in a durable **partial checkpoint**, records the failed
eligible paths and reasons, and retries with backoff. It keeps the last complete
checkpoint. Partial checkpoints cannot satisfy a complete-checkpoint barrier
or be used for full-tree restore.

Independent documentation may publish when it neither touches nor references
a failed path. Missing content can hide dependencies, so source, configuration,
assets, and uncertain rename or delete companions wait for complete observation.
ACD keeps these captures protected; it never treats a failed read as a deletion.
Status reports incomplete coverage even when the pending queue is empty.
Sockets, FIFOs, and devices are outside Git's file model and are not captured.

The 50,000-event default backpressure limit bounds low-level publication work
for one branch generation. It does not cap checkpoint protection: a completed
checkpoint still records the full eligible worktree when the event queue is
full. Paths that do not fit in the event queue stay out of capture-event and
shadow ownership, then are classified again after publication drains below the
limit.

## Durable checkpoint completion

1. Scan and hash eligible files; record failed coverage explicitly.
2. Build a tree through a scratch index. Normal capture also appends low-level
   capture records; protection-only scans defer classification.
3. Write Git objects with supported fsync settings and reread them exactly.
4. Insert the operation, checkpoint, membership, exclusions, object IDs, and
   expected private ref as `prepared` in one full-synchronous SQLite
   transaction.
5. Create the private ref with create-only CAS.
6. Observe its exact target.
7. Atomically mark the checkpoint and operation `completed`.
8. Allow publication to consume its member changes.

On recovery, absent prepared refs are retryable, exact expected refs complete
forward, and a different target becomes durable `needs_action`. Recovery never
guesses or deletes an ambiguous ref.

During a slow provider or verifier call, the worker continues making durable
protection-only checkpoints. Their bytes are protected immediately; capture
classification waits until the active evaluation finishes. The durable
`protection.classification_pending` marker prevents status from claiming those
bytes are already committed. It clears only after a complete capture has
classified the observed work, including work held by the event-queue cap.
History labels snapshots without capture membership `saved`.

## Unsafe Git states

Protection continues during detached HEAD, conflicts, branch changes,
merge/rebase/cherry-pick/bisect markers, manual publication pause, or staging
that would be unsafe to publish. Publication resumes only after the existing
branch-token and Git safety gates prove a stable target.

## Publication behavior

Only completed-checkpoint members enter publication. Existing Event and Intent
semantics remain intact. Provider, planning, message, grouping, test, or
verification problems never undo the completed checkpoint or its `protected`
state. Status shows `waiting` while ACD pauses for a retry, `publishing` while
it is actively retrying or rebuilding the plan, and `needs_action` only when
bounded recovery is exhausted or ACD cannot prove a safe next step.

Publication writes a specialized prepared record before branch mutation,
uses a literal-ref compare-and-swap, observes the exact target, and atomically
settles member changes and checkpoint publication links. Startup recovery
proves source, parent, target, tree, membership, and ref ownership. Ambiguity
stays `needs_action`.

If every member of a stopped publication run is already `published` or
`recovered`, the worker completes the run during startup or the next recovery
pass. The proof uses the frozen membership in SQLite and does not change HEAD,
the index, the worktree, or another branch.

## Retention

ACD never prunes unpublished checkpoints, restore preimages, unresolved
operations, or the newest complete checkpoint, even when a newer partial
snapshot exists. Published checkpoints default to 30 days and at least 100
retained. A soft 5 GiB budget may prune published
checkpoints older than seven days but never below 100. Protected-only content
over budget is retained and reported, never discarded.

Expected private refs make retained objects survive `git gc --prune=now`.

Checkpoint maintenance normally runs hourly. Failed checks retry after one,
two, five, then fifteen minutes, with later retries fifteen minutes apart.
The retry schedule survives worker restarts. A successful check clears the
failure and returns to hourly maintenance.

A failed inventory is reported as a maintenance failure, not a storage-budget
warning. Status, list, doctor, and diagnose share the saved outcome. Detailed
reports include the error, next check, and last successful storage measurement.
Temporary failures retry automatically. An external prerequisite, such as the
Xcode license agreement, names the action needed and is rechecked automatically.
ACD does not accept licenses on the user's behalf.

Interrupted pruning retries through the recorded ref proof. A moved ref remains
untouched and requires attention until safe recovery can be proven. Independent
checkpoint protection and publication continue while maintenance is waiting.
`acd repo gc` cleans registration records; it does not clear maintenance errors
or prune protected checkpoints.

Capture health is shared by status, list, doctor, diagnose, and recovery.
Reports separate worker responsiveness, capture progress, failure onset, affected
paths, and the next retry. Brief stabilization failures show waiting; persistent
or unreadable-path failures need attention while retries continue. Repeated
unchanged failures reuse the partial checkpoint and do not repeat the same log
line on every scan. A responsive worker does not prove queue progress.

Published capture retention keeps records referenced by a publication drain,
including a completed drain. Pruning an unrelated old capture cannot break the
frozen membership ledger or its foreign keys.
