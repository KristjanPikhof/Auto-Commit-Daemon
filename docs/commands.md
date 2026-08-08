# Command reference

## Public root commands

`acd` with no subcommand is identical to `acd status`. Root help exposes only:

| Command | Mutates | Confirmation |
|---|---|---|
| `acd setup` | Binary, service, registry, configuration, integrations, repository schema and checkpoint refs | Shows one exact plan and asks once. |
| `acd status` | Nothing | None. |
| `acd on` | Repository desired state through the supervisor | None; idempotent. |
| `acd off` | Final checkpoint and repository desired state | `--force` only when protection cannot be confirmed. |
| `acd history` | Nothing | None. |
| `acd restore ID` | Nothing by default; working tree with `--yes` | Preview is mandatory. |
| `acd doctor` | Nothing unless bundle output is requested | Bundle path is explicit. |
| `acd uninstall` | Service, binary, owned integrations and desired state | Shows a plan; data purge needs a second confirmation. |

## Setup

~~~bash
acd setup
acd setup --dry-run
acd setup --integrations=auto
acd setup --integrations=none
acd setup --integrations=claude-code,codex
acd setup --yes --non-interactive --expect-plan sha256:...
~~~

`--dry-run` performs no file write, command execution, service action, provider
call, integration change, migration, state open for writing, or Git ref
creation. `--expect-plan` is required for noninteractive apply when existing
installation or v19 state is detected.

Setup validates the OS, architecture, Git durability support, repository,
service manager, disk space, configuration, and integration files. It backs up
every touched file, applies one all-repository v19 to v20 cutover, runs an
isolated self-test, and commits only after all held workers report complete
current coverage.

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
writes.

## On and off

~~~bash
acd on
acd off
acd off --force
~~~

`off` requests a complete checkpoint barrier before disabling. If that barrier
fails, the repository stays enabled and the command returns `needs_action`.
`--force` explicitly accepts disabling without confirmed current protection.

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

## Doctor and support

~~~bash
acd doctor
acd support diagnose
acd support logs
acd support repair
acd support repair --yes
acd support bundle
~~~

Support repair previews a safely provable interrupted restore and, with
`--yes`, completes its post-restore checkpoint. It refuses if the working tree
no longer matches the interrupted restore target.

## Uninstall

~~~bash
acd uninstall --dry-run
acd uninstall
acd uninstall --purge-data
~~~

Default uninstall completes checkpoint barriers, stops workers and the
supervisor, removes only verified owned integration entries, removes the
managed service and binary, disables repositories, and preserves every state
database and private ref.

Data purge requires `--purge-data` plus the second
`--confirm-purge-data` confirmation. Noninteractive apply also requires
`--yes --non-interactive --expect-plan <digest>`.

## Advanced namespaces

~~~text
acd config get|set|edit|reset|credentials
acd support diagnose|logs|repair|bundle
acd repo list|remove|gc
acd history rewrite
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
| `events` | `history --activity` |
| `explain` | `history explain` |
| `rewrite-commits` | `history rewrite` |
| `diagnose`, `logs`, `fix`, `recover`, `prompt` | Matching `support` operation |
| `list`, `stats`, `gc` | Matching `repo` operation |
| `start`, `stop`, `wake`, `touch`, `flush` | Hidden internal session/hint protocol |
| `daemon run` | Hidden internal worker entrypoint |
| Hook extractors | Hidden internal integration helpers |
| `version` | `acd --version` |
| `setup <integration> --raw` | Hidden `setup integration <name> --print` route |

Manual compatibility calls warn on stderr. Recognized integration calls
suppress terminal warnings and emit only a rate-limited diagnostic.
