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

ACD reuses its bounded ignore, sensitive-path, symlink, file-size, and TOCTOU
checks. Git-ignored and configured sensitive paths are outside the contract.
Unreadable, unstable, or oversized eligible paths make the observation
unprotected until a later complete rescan succeeds.

The 50,000-event default backpressure limit bounds low-level publication work
for one branch generation. It does not cap checkpoint protection: a completed
checkpoint still records the full eligible worktree when the event queue is
full. Paths that do not fit in the event queue stay out of capture-event and
shadow ownership, then are classified again after publication drains below the
limit.

## Durable checkpoint completion

1. Scan and hash the complete eligible scope.
2. Append low-level capture records and build a tree through a scratch index.
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
operations, or the newest completed checkpoint. Published checkpoints default
to 30 days and at least 100 retained. A soft 5 GiB budget may prune published
checkpoints older than seven days but never below 100. Protected-only content
over budget is retained and reported, never discarded.

Expected private refs make retained objects survive `git gc --prune=now`.
