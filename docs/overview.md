# Architecture overview

ACD separates durable protection from Git publication.

~~~text
filesystem watch + complete poll + optional hints
                    |
                    v
          protection scheduler
                    |
        completed private checkpoint ref
                    |
                    v
          publication scheduler
     group -> verify -> message -> Git CAS
                    |
                    v
          ordinary local Git commit
~~~

Provider, grouping, test, or verification failures can delay the lower half;
they cannot prevent a successful protection scan from completing the upper
half.

## Process boundaries

One user-level supervisor owns desired state. It groups linked worktrees by
canonical Git common directory and runs one isolated worker for each group.
The supervisor never opens repository SQLite as a writer. Workers own their
repositories under the canonical common-directory lock.

Local newline-delimited JSON IPC uses schema v1, request IDs, strict method
validation, worktree identity, and bounded deadlines. Mutations fail closed if
the supervisor or worker is unavailable. Read-only status may use an existing
SQLite projection.

The supervisor restarts a crashed worker after 1, 2, 5, 10, then 30 seconds.
Backoff resets after five healthy minutes. Repeated crashes surface as
`needs_action` while bounded retries continue.

## Repository identity

Registry v2 identifies a repository by the first 16 lowercase hex characters
of SHA-256 over its canonical Git common directory. A worktree uses the same
construction over its canonical root. Old path hashes remain migration
metadata only.

## Checkpoint store

A checkpoint is a rootless Git commit whose tree contains the complete
eligible protected scope. A private ref makes its objects reachable:

~~~text
refs/acd/checkpoints/v1/<worktree-id>/cp-<milliseconds>-<random>
~~~

SQLite v20 records immutable operation identity, checkpoint phases, exact
capture-event membership, privacy-safe exclusion counts, publication links,
and restore relationships. The specialized publication record remains the
authoritative branch-ref CAS proof and links to the general operation.

## Publication

Event strategy retains one captured change per local commit. Intent strategy
can group changes across completed checkpoints, subject to existing dependency,
materialization, verification, revertibility, and exact-ref CAS gates. Only
events belonging to completed checkpoints are eligible.

Unsafe Git states suspend publication but never protection. A checkpoint is
published only when all member events have completed normal commit mappings.
No component pushes automatically.

## Restore

Restore computes a full-tree plan, checkpoints the current state, journals
filesystem preimages, applies same-directory atomic replacements, preserves
the index and `HEAD`, then checkpoints the result. The pre-restore checkpoint
is the undo target.

If files were applied but the final checkpoint failed, forward repair proves
the current protected tree equals the target before creating the missing
checkpoint and completing the operation.

## Global transactions

Setup, upgrade, uninstall, configuration, registry, integration, and service
mutations are recorded in the global operations database. Plans have immutable
SHA-256 digests. Every touched file has a type/mode/owner/digest preimage and
prior service desired state.

The v19 to v20 cutover spans every registered repository. A temporary
protection bridge covers concurrent edits, repository locks are acquired in
sorted common-directory order, and held v20 workers must prove current
coverage before global commit. Any ambiguous legacy proof aborts the entire
transaction.
