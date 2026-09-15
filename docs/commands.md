# Command reference

## Public root commands

`acd` with no subcommand is identical to `acd status`. Root help exposes only:

| Command | Mutates | Confirmation |
|---|---|---|
| `acd setup` | Shared binary, service, configuration, integrations, and enabled repository migrations during an incompatible upgrade | Shows one exact global plan and asks once. |
| `acd status` | Nothing | None. |
| `acd on` | Repository desired state through the supervisor | None; idempotent. |
| `acd off` | Final checkpoint and repository desired state | `--force` only when protection cannot be confirmed. |
| `acd list` | Nothing | None; live on a TTY and one-shot through a pipe or `--once`. |
| `acd commit-all` | Checkpoint and normal local Git publication | Preview unless `--yes` is supplied. |
| `acd history` | Nothing | None. |
| `acd restore [ID]` | Preview, or working tree after confirmation | Bare command opens a terminal picker; ID route supports scripts. |
| `acd doctor` | Nothing unless bundle output is requested | Bundle path is explicit. |
| `acd uninstall` | Supervisor process, Linux service, binary, owned integrations and desired state | Shows a plan; data purge needs a second confirmation. |

## Setup

~~~bash
acd setup
acd setup --dry-run
acd setup --integrations=auto
acd setup --integrations=none
acd setup --integrations=claude-code,codex
acd setup --yes --non-interactive --expect-plan sha256:...
acd setup --repo /path/to/repo
~~~

Setup is global and works outside Git. The compatibility `--repo` flag does
not change setup scope. It prints a warning and the exact follow-up command:
`acd on --repo /path/to/repo`.

`--dry-run` performs no file write, command execution, supervisor or service
action, provider call, integration change, migration, state open for writing,
Git ref creation, or secret prompt. Fresh setup asks for an experience, commit
format, and provider. An OpenAI-compatible provider also asks for the endpoint,
model, and masked bearer token. The token is tested only after the final review.

Fresh automation must first run `acd setup --dry-run --non-interactive --provider
deterministic --json` (or explicitly choose `openai-compat`), retain its digest, then
apply with `--yes --non-interactive --expect-plan`. Use `--experience`,
`--commit-format`, `--provider`, `--base-url`, `--model`, and `--ca-file` to set
the reviewed non-secret values. A bearer token may come only from
`ACD_AI_API_KEY` or `--credential-stdin`. Add the matching confirmation flags
for endpoint credentials, HTTP, diff egress, and Intent repair when the plan
requires them.

HTTP endpoints need `--confirm-insecure-http`. HTTP is not encrypted, so the
token and later requests can be read or changed in transit. Redirects and URLs
with embedded credentials, query strings, or fragments are refused.

Setup validates the OS, architecture, platform lifecycle, disk space,
configuration, and integration files. The installation transaction does not grant repository consent. After it
succeeds, terminal setup offers a separate repository enablement confirmation.
Unattended setup never grants this consent. During an incompatible upgrade, it checkpoints
and migrates enabled repositories before committing the global transaction.
Disabled repository records are preserved and their databases are left
unchanged until their next `acd on`.

The isolated setup self-test uses its own Git identity in a scratch repository.
Your global Git identity and repository settings are unchanged.

Existing installations skip first-run questions and keep their current
settings.

Compatible setup plans inspect only the shared runtime and integrations. They
do not scan repository databases, rerun migrations, or repeat the isolated
migration self-test.

## Status

Every human status answers:

1. Is ACD enabled?
2. Are current eligible changes protected?
3. Are they published to Git?
4. Is user action required?
5. What exact command should run next?

State priority is `off`, `needs_action`, `publishing`, `waiting`, `protected`.
The independent `protected` boolean may remain true in the middle three
publication/repair states.

Default status shows `Protection`, `Current changes saved`, `Branch commits`,
an optional `Recovery` count, and `Next`. `acd status --verbose` adds operational
detail. Branch publication and recovery preservation are distinct outcomes.

JSON adds `publication_outcome` with nullable `branch_committed`, counts for
`branch_changes`, `recovered_changes`, and `waiting_changes`, plus `reason_code`
and `retry_at` when known. `pending_classification` identifies saved bytes
waiting to enter the capture ledger. Null means unavailable. Existing `published` and
`checkpoint_published_by_acd` fields are deprecated compatibility fields: their
existing meaning still includes safely recovered work. History retains its
old `published` boolean and adds `outcome`, `retained`, and `recovered_events`.
Snapshots without capture membership report `saved`. Outcome counts for branch
publication and waiting use the current branch generation; recovery counts
retain historical preservation evidence. Exact current-branch tree proof permits
a recovered-and-recaptured snapshot to report commitment without erasing its
recovery history. If that proof is unavailable, `branch_committed` is null.

If Git changes while AI is evaluating a target, ACD rejects that stale result.
An exact match with externally committed work can resolve through the recovery
proof (`recovery_published`). A chain containing both applied and reverted work
may instead be preserved separately (`recovery_archived`). Neither outcome
creates a duplicate branch commit or restores files over your edits.

Read-only status falls back to existing v20 SQLite projections when the
supervisor is unavailable. Mutations never fall back to direct unsupervised
writes. On macOS, mutating commands first start or reuse the shared per-user
supervisor. The owner-only socket verifies the peer UID, and the process uses
the permissions inherited from the application that first started it. If the
CLI and managed runtime do not match exactly, mutations stop and ask you to run
`acd setup`.

## On and off

~~~bash
acd on
acd off
acd off --force
~~~

`on` first requires an installed, exactly matching managed runtime. It then
registers an unknown repository, upgrades only that repository when needed,
enables it, starts a new worker, and verifies a checkpoint before it succeeds.
It does not migrate or enable any other repository.

`off` requests a complete checkpoint before disabling, then waits for the
managed worker to stop. If the checkpoint fails, the repository stays enabled
and the command returns `needs_action`. `--force` accepts disabling without a
confirmed current checkpoint.

## History and restore

~~~bash
acd history
acd history --activity
acd history explain
acd history rewrite
acd restore cp-...
acd restore cp-... --yes
~~~

Creating a new rewrite plan requires Intent mode and an explicitly configured
non-deterministic AI provider that can plan commits. Showing, editing, or
applying an existing saved plan makes no new AI call.

History rewrite groups adjacent commits by intent unless you pass
`--messages-only`. Its preview shows the selected and resulting commit counts,
group membership, messages, and grouping reasons before any history changes.

Checkpoint prefixes are accepted only when unique. Restore is full-checkpoint
only. Preview reports create, modify, delete, mode, symlink, untracked-overwrite
and staged-overlap counts. Apply revalidates the plan digest, `HEAD` token,
worktree identity, index digest, and target ref.

Bare `acd restore` in a terminal lists completed checkpoints. Select one to see
changed files, confirm, and receive an undo checkpoint command. The picker and
ID route share preview, overlap, staging-preservation, and stale-plan checks.
Outside a terminal, supply an ID from `acd history`; no restore is applied.

## Repository dashboard and commit-all

~~~bash
acd list
acd list --once
acd list --all
acd list --verbose
acd list --watch --interval 5s
acd list --json
acd commit-all --dry-run
acd commit-all --yes
~~~

`list` shows enabled repositories with activity in the last hour, plus any
repository with unfinished work. Activity includes agent hooks, captured edits,
publication progress, commit-all requests, and applied history rewrites.
Worker heartbeats and background maintenance do not count as activity.

A repository with pending, blocked, stalled, or incompletely protected work
stays visible until that work is resolved. An otherwise idle repository drops
out after one hour, including repositories with maintenance warnings or a
manual pause. There is no fixed row limit or filler from older repositories.

The first snapshot sorts by recent activity. A terminal refreshes rows in place
until Ctrl-C and appends newly active repositories. Use `--once` for one snapshot,
`--all` for every enabled repository, and `--verbose` for worker, tool, blocker,
last commit, maintenance error, and recovery details. `list` works globally
from any directory. An empty compact view says `No active repositories`.

`SAFE` confirms that the latest checkpoint is complete. `MODE` shows the
configured commit strategy. `QUEUE` counts all pending work. `TARGET` appears
for the bounded remainder of `commit-all` or automatic Intent recovery; it does
not mean the strategy changed. `LAST MOVE` is the age of durable queue
progress, not the worker heartbeat. `PHASE` distinguishes an ordinary Intent
countdown from planning, publication, retry, and automatic recovery.
`provider-wait` includes the retry countdown, `provider-call` means the retry
request is in flight, and `verifying` means the approved repository check is
running. A dash means that field does not apply or could not be read during
that frame.

| Status | Meaning |
|---|---|
| `healthy` | Protection is complete and no work is pending. |
| `working` | ACD is checkpointing, planning, publishing, validating, starting, or retrying; `PHASE` gives the exact activity. |
| `waiting` | Protected work is waiting for the countdown or safe condition shown in `PHASE`. |
| `stalled` | The worker is responsive, but the queue has not made durable progress within the bounded threshold. ACD keeps retrying or recovering automatically; no action is needed unless the status changes to `needs action`. |
| `paused` | Protection and publication are manually paused. |
| `needs action` | A failure or safety block requires attention. |

JSON remains exhaustive regardless of the compact view. It keeps the existing
fields and adds `worker_state`, `operational_state`, `blocked_events`,
`last_activity_at`, `publication_drain`, `unfinished_work`, and
`checkpoint_maintenance`. Maintenance details distinguish a failed check from
measured storage use and include the next scheduled attempt.
A needs-action result is printed
before exit code 3 is returned. Human compact snapshots use only visible
repositories to decide that exit status; `--all` and JSON use the exhaustive
result. Hidden idle warnings do not fail a compact snapshot.

Disabled, missing, and stale registration records remain available under hidden
`acd repo list`. That command is the static maintenance inventory and does not
refresh automatically.

Before confirmation, `commit-all` lists changed paths, staged content, and
already queued paths. The lists may overlap. Included staging is consumed only
after checkpoint protection; ordinary background publication preserves staging.
Interactive approval is rechecked against paths, queued work, and staging. If
they change, review the refreshed preview; a later worker-side mismatch refuses
application. `--yes` accepts the current scope without an interactive preview.
Immediately before consuming staging, the worker locks the Git index and checks
its saved approval identity. If you staged something else while it waited, ACD
preserves that selection and asks you to review commit-all again. This check
also survives a worker restart. Older requests without a saved staging identity
require a new review before consuming staging.
A fresh approval preserves the old request's captured work separately and
recaptures the current files for a new target. The old staging approval stays
unchanged; it is never reused to consume a newer selection.

`commit-all` first completes a durable checkpoint, records the highest event
sequence covered by the barrier, and drains only that bounded target through
the managed worker. Later edits cannot extend the wait. Event mode may create
one commit per capture; Intent mode may create several semantically atomic
commits. The command never combines everything into one commit merely because
of its name. If the terminal disconnects or the worker restarts, publication
continues and the next `acd commit-all --yes` reconnects to the same drain.
Invalid Intent grouping can use the configured retry budget, capped at two
corrections after the first plan. Repeated no-progress state then enters
bounded replanning or a safe local unlock. A local group still requires a
semantic commit message before publication.

The barrier accepts only a completed checkpoint for the requested worktree,
branch, generation, and observation. A checkpoint from another branch or
linked worktree is ignored while the worker creates the matching checkpoint.

## Doctor and support

~~~bash
acd doctor
acd support diagnose
acd support logs
acd support repair
acd support repair --yes
acd support bundle
~~~

`doctor` shows the worker's current state, its latest safe error, and a command
that addresses that error. Start with `acd on` for a stopped or stale managed
worker. Use the support commands only when doctor asks for them.

Support repair previews a safely provable interrupted restore and, with
`--yes`, completes its post-restore checkpoint. It refuses if the working tree
no longer matches the interrupted restore target.

`acd support recover` proves or preserves unpublished work. It also completes
a stale publication run when every frozen member is already published or
recovered. Workers perform that completion automatically during startup and
normal branch recovery; the command is a fallback for a worker that cannot run.

## Uninstall

~~~bash
acd uninstall --dry-run
acd uninstall
acd uninstall --purge-data
~~~

Default uninstall completes checkpoint barriers, stops workers and the
supervisor, removes only verified owned integration entries, removes the
managed Linux service (when present) and binary, disables repositories, and
preserves every state database and private ref. macOS has no installed service
file in session mode.

Data purge requires `--purge-data` plus the second
`--confirm-purge-data` confirmation. Noninteractive apply also requires
`--yes --non-interactive --expect-plan <digest>`.

## Advanced namespaces

~~~text
acd config get|set|edit|reset|credentials
acd support diagnose|logs|repair|recover|prompt|bundle
acd repo list|remove|gc
acd history activity|explain|rewrite
~~~

They are callable but hidden from root help.

Configuration defaults to repository scope inside a worktree and global scope
outside one. `--scope repo|profile|global` is explicit. Interactive editors
reject `--json` rather than ignoring it.

## Persistent flags

| Flag | Contract |
|---|---|
| `--repo PATH` | Resolves a worktree target, or is rejected for global-only operations. |
| `--json` | Uses the common envelope, or is rejected for interactive TUI operations. |
| `--quiet` | Suppresses progress but never the final result. |
| `--log-level LEVEL` | Configures the CLI logger for that invocation only. |

No accepted persistent flag is silently ignored.

## JSON contract

Every JSON response is written once to stdout, including nonzero exits:

~~~json
{
  "ok": true,
  "state": "protected",
  "changed": false,
  "actions": [],
  "next_action": null,
  "data": {},
  "error": null
}
~~~

`ok` means the command executed; it does not mean the repository is healthy.
Errors use `code`, `message`, `retryable`, and redacted `details`. Actions are
ordered objects with `kind`, `status`, `target`, and `detail`.

| Exit | Meaning |
|---|---|
| `0` | Completed with no required action. |
| `1` | Unexpected internal failure. |
| `2` | Invalid command or flag combination. |
| `3` | Valid result that requires user action. |
| `4` | Transient supervisor or worker unavailability. |

## Compatibility aliases

Aliases are retained for this release and the next, hidden from help, and may
be removed no earlier than the third checkpoint-first release.

| Old name | Destination |
|---|---|
| `configure`, `settings` | `config edit` |
| `auth` | `config credentials` |
| `events` | `history activity` |
| `explain` | `history explain` |
| `rewrite-commits` | `history rewrite` |
| `diagnose`, `logs`, `fix`, `recover`, `prompt` | Matching `support` operation |
| `stats`, `gc` | Matching `repo` operation |
| `start`, `stop`, `wake`, `touch`, `flush` | Hidden internal session/hint protocol |
| `daemon run` | Hidden internal worker entrypoint |
| Hook extractors | Hidden internal integration helpers |
| `version` | `acd --version` |
| `setup <integration> --raw` | Hidden `setup integration <name> --print` route |

Manual compatibility calls warn on stderr. Recognized integration calls
suppress terminal warnings and emit only a rate-limited diagnostic.

Failed JSON recovery commands preserve the reviewed or partial plan in `data`,
with `ok: false` and a typed error, instead of discarding the plan on failure.
