# Migrate to Intent v2

Intent v2 is a cutover for repositories already using Intent mode. It does not
silently preserve the v1 planner path.

## Check the repository after upgrading

Build or install the new binary through your normal release process, then run:

~~~bash
acd status
acd diagnose
~~~

The daemon migrates the repository database from schema v14 to v15 on a
writable start. The migration is additive. It retains capture events, planner
windows, decisions, runtime revisions, experiments, and recovery data.
Read-only commands do not migrate the database.

| Existing repository | Migrated mode |
|---|---|
| Event without an explicit preset | `event.fast@2` |
| Event with explicit compatible settings | Event plus the inferred or selected preset |
| Intent | `intent.balanced@2` |

Existing provider, model, endpoint, timeout, commit format, and explicit v1
intent tuning remain advanced overrides. A differing preset-owned override
marks the result `Balanced (customized)`.

## Resolve migration attention

Intent v2 requires a tested provider, redacted diff context, and an approved
fast verification command for Balanced. If any prerequisite is missing, ACD
continues capturing changes but stops replay:

~~~text
needs_attention
run acd configure
~~~

Resolve the complete configuration in one guided transaction:

~~~bash
acd configure
~~~

The wizard shows the exact diff-egress policy and verification command before
activation. It tests both, stores an optional protected credential, writes one
immutable runtime revision, and enables the daemon.

Do not work around the gate by clearing diff consent or relying on v1
environment-only grouping. Metadata-only Intent v2 is unsupported in regular
presets, and the daemon will not fall back to it.

## Keep existing credentials

An existing `ACD_AI_API_KEY` continues to work and has priority over the new
protected file. To move the value into the XDG credential file:

~~~bash
printf '%s\n' "$ACD_AI_API_KEY" | acd auth set --stdin
acd auth status
~~~

Unset the environment variable only after `acd auth status` reports the file
source. ACD never copies a secret automatically.

## Understand repair activation

Migrated Balanced repositories enable automatic repair for at most three
eligible ACD commits within ten minutes. Repair still requires the strict
private-history, exact-HEAD, staging, Git-operation, atomicity, and verification
checks documented in [Intent commit flow](intent-commit-flow.md).

To disable automatic repair without weakening the other Balanced gates, use
the advanced settings editor and customize `intent.repair.enabled`. To remove
command verification, switch to Fast through `acd configure`; do not blank the
Balanced verification command.

## Roll back safely

Schema v15 is additive, but an older binary refuses a newer database instead of
guessing how to read it. Do not point a pre-v15 daemon at a repository after
migration.

If replay needs attention after upgrade, keep the v2 binary and run
`acd configure`. Captures are already durable, so replacing the database or
purging events is the wrong recovery path.

Intent repair backup refs use `refs/acd/intent-repair/.../backup`. A crash after
the Git ref update leaves the mapping recoverable on restart. Keep the backup
and follow `acd status`, `acd doctor`, or `acd fix --dry-run` guidance rather
than resetting or deleting refs by hand.

## Upgrade harness boundaries

Regenerate or merge the current harness snippet after upgrading:

~~~bash
acd setup codex
acd setup claude-code
acd setup cursor
acd setup opencode
acd setup pi
~~~

Codex Stop now records `acd touch --soft-boundary`. Other supported harnesses
retain logical flush boundaries. Setup prints snippets only; it does not edit
external hook files.

## Implementation archive

The implementation plan is archived as `acd-intent-v2-2026-07-26` in Trekoon
epic `f82f0e6b-601c-4019-b9bc-1af3b54fc4dd`.
