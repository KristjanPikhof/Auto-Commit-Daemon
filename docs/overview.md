# Architecture overview

ACD separates durable protection from Git publication.

You change files, ACD captures them, groups related changes by intent, creates
semantic local commits, and leaves you with a clean Git history.

![From file changes to clean Git history](assets/acd-workflow.png)

ACD validates every grouping plan before publication. If an AI-generated plan
is invalid or unsafe, ACD rejects it and retries or rebuilds the plan. The
completed checkpoint remains protected throughout that loop.

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

On macOS the supervisor and workers are descendants of the first authorized
Terminal or agent application that starts them. One owner-only socket is shared
by all processes for that user; the server verifies the peer UID before reading
a request, and repository IDs are still restricted by the registered allowlist.
ACD therefore needs no Full Disk Access and does not install a launchd service.
Mutating commands and supported integration hints start the supervisor when it
is absent; after logout or reboot, protection resumes on that first invocation.
Linux uses a persistent systemd user service.

Local newline-delimited JSON IPC uses schema v1, request IDs, strict method
validation, worktree identity, and bounded deadlines. Mutations fail closed if
the supervisor or worker is unavailable. Read-only status may use an existing
SQLite projection.

The supervisor restarts a crashed worker after 1, 2, 5, 10, then 30 seconds.
Backoff resets after five healthy minutes. Repeated crashes surface as
`needs_action` while bounded retries continue.

The supervisor advertises protocol, registry, repository-state, and integration
compatibility plus the exact managed-binary digest. Repository mutations and
hooks require that runtime to match the CLI exactly. A mismatch is a
fail-closed `acd setup` boundary; ordinary repository activity never replaces
the binary or upgrades unrelated repositories.

Integration hooks start or wake only repositories that are already registered
and enabled. Unknown and disabled repositories are skipped without creating
state; human-facing output points to `acd on`. Idle, stop, and session-close
boundary hints preserve and log the real command exit code but return success,
because they refine commit grouping and cleanup rather than establish current
protection.

## Repository identity

Registry v2 identifies a repository by the first 16 lowercase hex characters
of SHA-256 over its canonical Git common directory. A worktree uses the same
construction over its canonical root. Old path hashes remain migration
metadata only.

The worker daemon log is stored at
`${XDG_STATE_HOME:-$HOME/.local/state}/acd/<repo-hash>/daemon.log`. Linked
worktrees share that common-directory log. Each worktree record includes
`repo_hash`, `worktree`, and `git_dir`; attached-branch records also include
`branch_ref` and the known `branch_generation`. Planner reject logs are not
shared. They stay under the exact worktree Git directory described in
[AI providers](ai-providers.md#failure-behavior).

## Checkpoint store

A checkpoint is a rootless Git commit whose tree contains the complete
eligible protected scope. A private ref makes its objects reachable:

~~~text
refs/acd/checkpoints/v1/<worktree-id>/cp-<milliseconds>-<random>
~~~

SQLite v21 records immutable operation identity, checkpoint phases, exact
capture-event membership, privacy-safe exclusion counts, publication links,
restore relationships, and durable publication drains. A drain stores one
frozen target, progress counters, bounded semantic attempts, local fallback,
staged consent, and restart-safe phase state. The specialized publication
record remains the authoritative branch-ref CAS proof and links to the general
operation.

SQLite v24 also freezes the capture membership of each new Intent history
repair. Completed repairs and normal publications use the same durable branch
transition proof, so a worker can recover either ref move after a restart.

## Publication

Event strategy retains one captured change per local commit. Intent strategy
can group changes across completed checkpoints, subject to existing dependency,
materialization, verification, revertibility, and exact-ref CAS gates. Only
events belonging to completed checkpoints are eligible.

An active drain publishes only its immutable frozen target. Later captures get
normal checkpoint protection but wait outside that target. Planner failure can
move the drain through normalization to local atomic dependency groups.
Neither path bypasses materialization, verification, exact-ref CAS, or the
self-publication journal. Client cancellation stops only the wait display.

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

Setup, upgrade, uninstall, configuration, registry, integration, and platform
lifecycle mutations are recorded in the global operations database. Plans have
immutable SHA-256 digests. Every touched file has a type/mode/owner/digest
preimage and prior platform lifecycle state.

An incompatible setup cutover spans enabled repositories only. A temporary
protection bridge covers concurrent edits, repository locks are acquired in
sorted common-directory order, and held workers must prove current coverage
before global commit. Disabled records and database versions remain unchanged
until their next `acd on`. Any ambiguous legacy proof in an enabled repository
aborts the entire transaction.
