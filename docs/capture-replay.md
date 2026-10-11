# Protection and publication

## Observation and coverage

ACD saves eligible changes in private checkpoints while protection is on.
Watcher events and complete safety polls mark coverage incomplete until a scan
proves the latest observation. `protected=true` means the eligible scope has a
completed durable checkpoint and no eligible read or stabilization failure.
It does not mean the work has been committed to the branch.

Complete polling repairs missed watcher events. An idle worker backs off;
unchanged scans reuse existing protection evidence. Idle time, stop events,
and integration hints never trigger a finish.

## Eligible scope

Git-ignored, sensitive, and generated cache paths remain excluded. Regular
files, executable modes, symlink entries, deletions, and binary files retain
their Git representation. Reads stay anchored beneath the opened repository
directory and reject symlink ancestors, including directories replaced during
a scan. Gitlinks stay distinct from ordinary files.

`capture.max_file_bytes` controls buffering, not eligibility. Larger regular
files stream into durable objects. ACD caps buffering at 32 MiB. Sockets,
FIFOs, and devices are outside Git's file model.

An unreadable or changing eligible path makes coverage incomplete. ACD saves
readable files in a partial checkpoint, records the failure, and keeps the last
complete checkpoint. It never guesses that a failed read is a deletion.
Partial checkpoints cannot satisfy finish's fresh complete-checkpoint barrier
or a full-tree restore. Protection retries independently of publication.

## Durable checkpoint completion

ACD builds the protected tree with a scratch index, writes and verifies its Git
objects, then records a prepared checkpoint and expected private ref in SQLite.
It creates the ref with create-only compare-and-swap. Completion requires a
direct private ref at the exact target, held under Git's native ref lock while
SQLite records completion. The live index and worktree do not change.

Recovery completes an exact prepared ref forward. A symbolic ref or different
target requires attention and remains untouched. Private refs keep retained
objects reachable through Git garbage collection. Missing or malformed legacy
objects prevent completed cutover, while independently available valid captured
objects remain pinned for recovery. Legacy archive attention does not block
current checkpoint protection or finish. History rewrite reads the selected
current Git commits, independently of archived capture records.

Upgrades check legacy objects and private refs in bounded
batches. Each ref must still point directly to its recorded object; Git's native
locks protect the checks and completion record. Missing or malformed evidence
remains visible as attention and is checked again after the evidence changes.
Later checkpoints reuse the immutable pins for already retained commits instead
of creating another full archive. Each manifest keeps its own exact snapshot
and recorded pin mapping; older refs remain retained.

## The finish target

`acd finish` requests a fresh complete checkpoint and freezes one target. The
source checkpoint proves the saved worktree bytes. The publication target also
preserves unchanged base entries outside the protected scope, including
excluded paths and gitlinks. A change to an ignore rule does not silently delete
an existing branch entry.

Ordinary finish refuses staging. With `--include-staged`, only the frozen staged
selection uses its index versions or deletions; other eligible paths use the
frozen worktree versions. Unstaged differences on a selected path remain live
work for a later finish. Staged excluded or sensitive paths are rejected, even
with consent.

Later edits continue to receive checkpoints during planning and verification.
They do not expand the accepted request. A retry uses that same frozen target;
a new ordinary finish freezes current work again.

## Verification and publication

Every candidate must use recorded path versions and produce a complete tree.
ACD checks dependencies, rename groups, author boundaries, coverage, and exact
final target equality. These structural checks always run.

`none` and `structural` do not run project commands. `fast` and `full` use the
approved configured command in an isolated candidate tree. Results distinguish
executed checks, reused results, missing commands, and structural-only evidence.
A structural pass is not a claim that tests ran.

ACD builds and verifies the whole private commit chain before preparing its
publication journal. One exact branch compare-and-swap publishes the tip.
A failure before that mutation leaves the branch unchanged and the checkpoint
retained. After-CAS interruption reports the proven branch outcome separately
from any remaining index cleanup or journal completion. Finish never pushes.

Publication rechecks the branch and index. Approved staging is consumed through
Git's exclusive index lock using its frozen identity; new staging is never
silently overwritten. Working files stay unchanged. After a crash, exact
prepared proof can complete forward; ambiguity requires attention. A running
request interrupted before preparation becomes failed and needs explicit retry.

## Unsafe Git states

Protection continues during detached HEAD, conflicts, branch movement, and
merge, rebase, cherry-pick, or bisect operations. Finish requires an attached,
stable branch and safe Git state. Setup or restore can hold publication while
their own operation completes. Neither condition grants permission to discard
protected work.

## Retention

ACD retains unfinished finish targets, the newest complete checkpoint, restore
preimages, unresolved operations, and legacy provenance needed for recovery.
Referenced private objects survive `git gc --prune=now`.

Older published or superseded protection snapshots can expire after 30 days,
with at least 100 retained. The soft storage budget can prune eligible snapshots
after seven days. The latest complete checkpoint and snapshots referenced by
finish, restore, unresolved operations, or legacy recovery remain protected.
Required content over budget stays retained and is reported.

Maintenance retries interrupted work from exact ref proof. A moved ref remains
untouched. Status and diagnostics distinguish storage warnings, maintenance
failure, incomplete coverage, and worker responsiveness. A heartbeat alone
does not prove the latest file changes are protected.
