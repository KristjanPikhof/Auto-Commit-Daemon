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
| `acd restore ID` | Nothing by default; working tree with `--yes` | Preview is mandatory. |
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

Automation must first run `acd setup --dry-run --json`, retain its digest, then
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
configuration, and integration files. Fresh setup creates no repository state
and registers no repository. During an incompatible upgrade, it checkpoints
and migrates enabled repositories before committing the global transaction.
Disabled repository records are preserved and their databases are left
unchanged until their next `acd on`.

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

Checkpoint prefixes are accepted only when unique. Restore is full-checkpoint
only. Preview reports create, modify, delete, mode, symlink, untracked-overwrite
and staged-overlap counts. Apply revalidates the plan digest, `HEAD` token,
worktree identity, index digest, and target ref.

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

`list` is the live overview for repository health and commit progress. It always
shows repositories that need action or are working. The default view fills the
remaining five-row budget with repositories where ACD most recently handled
changes. Paused repositories appear only when space remains after recent work.
Use `--all` for every enabled repository and `--verbose` for worker, tool,
safety, drain, blocker, last commit, and recovery details. A terminal refreshes
the same screen until Ctrl-C; `--once` prints one snapshot.

`SAFE` confirms that the latest checkpoint is complete. `DRAIN` shows published
and target event counts for an active `commit-all` run. `LEFT` shows the exact
events remaining in that drain, or ordinary pending events when no drain is
active. A dash under `SAFE` means the checkpoint state could not be read during
that frame.

| Status | Meaning |
|---|---|
| `healthy` | Protection is complete and no work is pending. |
| `working` | ACD is checkpointing, publishing, planning, validating, starting, or retrying. |
| `waiting` | Protected work is waiting for an Intent batch delay or rewind grace period. |
| `paused` | Protection and publication are manually paused. |
| `needs action` | A failure or safety block requires attention. |

JSON remains exhaustive regardless of the compact view. It keeps the existing
fields and adds `worker_state`, `operational_state`, `blocked_events`,
`last_activity_at`, and `publication_drain`. A needs-action result is printed
before `acd list` returns exit code 3.

Disabled, missing, and stale registration records remain available under hidden
`acd repo list`. That command is the static maintenance inventory and does not
refresh automatically.

`commit-all` first completes a durable checkpoint, records the highest event
sequence covered by the barrier, and drains only that bounded target through
the managed worker. Later edits cannot extend the wait. Event mode may create
one commit per capture; Intent mode may create several semantically atomic
commits. The command never combines everything into one commit merely because
of its name. If the terminal disconnects or the worker restarts, publication
continues and the next `acd commit-all --yes` reconnects to the same drain.
Invalid Intent grouping gets one bounded rebuild, followed by safe
local atomic dependency groups when needed.

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
