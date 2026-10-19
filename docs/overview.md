# Architecture overview

ACD protects eligible file changes continuously. It creates branch commits only
when you run `acd finish`. Saving files, leaving an editor, going idle, or
stopping a worker does not authorize a new publication. Startup can complete an
already authorized prepared publication from exact saved proof.

~~~text
file changes -> protection scan -> private checkpoint
                                      |
                                acd finish
                                      |
                         freeze -> plan -> verify
                                      |
                         one branch compare-and-swap
~~~

Fresh installations use offline Local mode with imperative messages. Intent
uses an approved AI provider to plan completed goals. Both modes use the same
checkpoint, tree, staging, and publication safety checks.

## Process boundaries

A per-user supervisor groups registered worktrees by canonical Git common
directory. One worker owns each group under the daemon lock. Linked worktrees
have separate identities, checkpoints, and settings. PID files and heartbeats
support diagnosis; they do not replace the ownership lock.

On macOS the supervisor and workers descend from the application that starts
them. ACD uses an owner-only socket and checks the peer UID and repository
allowlist. It does not install a launchd service. Linux uses a systemd user
service. Protection resumes through the matching installed runtime.

The supervisor checks the managed binary digest and protocol compatibility.
Repository commands refuse an incompatible runtime and direct you to
`acd setup`. Source changes in a checkout do not update an installed runtime.
An existing older runtime can keep making automatic commits until you
explicitly upgrade it.

Restarted workers belong to the supervisor and keep running after the request
that started them ends. They stop when the supervisor stops.

Local IPC uses bounded requests with repository and worktree identity. Read-only
status, list, doctor, and diagnose inspect existing evidence without building
providers, migrating databases, or starting publication. Mutations require the
canonical owner.

## Protection

The worker watches for changes and performs complete safety scans. A complete
checkpoint proves the current eligible scope; failed reads leave coverage
incomplete and retain readable content in a partial checkpoint. Ignored and
sensitive paths stay outside that scope. Large regular files stream into Git
objects with bounded memory.

A checkpoint is a rootless Git commit retained by a private ref:

~~~text
refs/acd/checkpoints/v1/<worktree-id>/cp-<milliseconds>-<random>
~~~

SQLite records its identity, tree, coverage, exclusions, and ref proof. A
completed checkpoint is protection evidence, not a branch commit. Repeated
unchanged scans do not create a publication queue or call AI.

State database paths preserve filesystem characters such as `?`, `#`, and `%`.
Git lookups treat captured filenames literally, including names that resemble
Git pathspec syntax.

See [protection and publication](capture-replay.md) for coverage and retention.

## Explicit publication

Finish obtains a fresh complete checkpoint and freezes the attached branch,
expected HEAD, target tree, index identity, mode, style, settings, and consent.
Later edits remain protected for another finish.

Local creates one complete batch commit without a provider. Intent plans useful
review or revert boundaries using exact recorded versions. ACD materializes
and checks every proposed tree privately, then prepares the whole commit chain
before one compare-and-swap moves the branch. A failed candidate cannot publish
part of that chain. The live worktree stays untouched.

Slow provider calls and approved project checks release the operation gate so
checkpoint protection can continue. The worker owns the accepted request;
closing the client detaches its display. Reconnecting uses the saved request
ID. Failed requests need an explicit retry; a restart does not start a new AI
attempt. Prepared publication resumes only from exact saved proof. Ambiguous
proof requires attention.

See [Intent finish flow](intent-commit-flow.md), [provider failure behavior](ai-providers.md#failure-behavior),
and the detailed [finish contract](finish-contract.md).

## Restore and history

Restore previews a retained complete checkpoint, saves current preimages, and
applies confirmed file replacements. It preserves HEAD and the index and
prints an undo checkpoint. Interrupted restores require exact forward proof.

History rewriting is a separate explicit selection, plan, preview, and apply
operation. It retains the original source history and checks exact branch and
index identity. It never runs because files were saved or an editor stopped.
See [history rewriting](rewrite-commits.md).

## Configuration and upgrades

Reviewed settings become an approved revision synchronously. Saving or applying
settings does not test a provider, run a project command, start a worker, or
publish work. A new finish uses the approved snapshot; an accepted finish keeps
its frozen settings.

Setup, upgrade, and uninstall use reviewed global plans with rollback preimages.
An incompatible upgrade covers enabled repositories; disabled repositories stay
disabled until their next explicit activation. Legacy checkpoints and immutable
publication evidence remain retained. Unprovable legacy mutations block
publication rather than restart the old planner.

The retired automatic publication design is preserved in the
[legacy Intent flow](legacy-intent-commit-flow.md).
