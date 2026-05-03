# User Workflows: Status, Events, Explain, Fix, and Support

Use this guide when commits do not look the way you expected. It starts with
the daily commands and only escalates to diagnostics when the repo is actually
stuck.

## Daily command loop

~~~bash
acd status
acd status --watch
acd events
acd events --watch
acd explain --path path/to/file
acd explain --commit HEAD
~~~

`acd status` is the current snapshot: daemon liveness, queue counts, pause
state, branch generation, active commit strategy, and recent decision counts.
`acd events` is the durable decision ledger. It tells you what ACD captured,
skipped, committed, deferred, grouped, blocked, or treated as already handled by
another committer. `acd explain` turns those decisions into a path or commit
answer.

`acd events --watch` follows new ledger rows. Without `--since`, it starts at
the current ledger tail and prints only decisions appended after watch starts.
Use `acd events --since <cursor>` when you want to resume from an older
decision cursor.

Reach for the raw log only when you need daemon internals:

~~~bash
acd logs --lines 200
acd logs --follow
~~~

For support reports, prefer `acd doctor --bundle`; it sanitizes and bundles the
useful diagnostics instead of asking someone to read raw JSONL.

## File was not committed

Start with the path, not SQLite:

~~~bash
acd status
acd explain --path path/to/file
acd events --path path/to/file
~~~

Common answers:

| What you see | Meaning | Next step |
|---|---|---|
| `captured` with action `queued` | ACD noticed the file and replay has not published it yet. | Wait one tick, or run `acd wake --session-id "$ACD_SESSION_ID"` from a harness shell. If pending grows, check `acd status`. |
| `committed` | ACD already made the commit. | Use `acd explain --commit HEAD` or `git log -- path/to/file`. |
| `protected` or `skipped` | ACD intentionally left the path uncommitted. | Check safe-ignore, sensitive globs, `.gitignore`, or whether the file is outside the repo. |
| No decision and no pending event | ACD has not seen the path. | Confirm the daemon is running for this repo and the path is not ignored or generated. |
| `blocked` | Replay stopped before it could safely publish. | Run `acd diagnose`, then `acd fix --dry-run`. |

If the daemon is alive but the file still has no decision, run:

~~~bash
acd doctor
~~~

Look for fsnotify fallback, missing harness install, safe-ignore, and sensitive
glob information. `doctor` is a health check; `explain` is the daily "why did
this path do that?" command.

## AI or manual commit handled the change externally

If another tool, AI hook, or manual `git commit` lands the same content before
ACD publishes its queued event, ACD should settle the event without creating a
duplicate commit.

~~~bash
acd events --path path/to/file
acd explain --commit HEAD
~~~

Expected decisions:

| Decision | Meaning |
|---|---|
| `handled_external` | The current `HEAD` already contains the captured after-state, so ACD marked the event published against that commit. |
| `superseded_external` | External history made the queued event obsolete, usually because `HEAD` now matches the captured before-state or otherwise proves replay would be redundant. |

No action is needed when `acd explain` says the external commit already
contains the change. If the queue remains blocked after an external commit, use:

~~~bash
acd fix --dry-run
acd fix --yes
~~~

`fix --yes` refuses unsafe mutations and backs up `state.db` first. Stop the
daemon before applying a plan if the command tells you a live daemon owns the
state database.

## Intent grouping deferred or forced a change

When `ACD_COMMIT_STRATEGY=intent` is enabled, ACD may publish several related
captures as one commit, choose exactly one capture, or defer unrelated captures
for a later planning window. Deferrals are normal. They mean the planner did not
have enough evidence to group that capture with the current selected set.

~~~bash
acd status
acd events --watch
acd explain --path path/to/file
~~~

Look for:

| Decision | Meaning |
|---|---|
| `intent_deferred` | The planner left this pending for a later window. |
| `intent_forced` | ACD forced an over-deferred capture into a one-item planning window. |
| `intent_planner_error` | The planner returned an invalid plan or failed; ACD fell back to a safe one-capture plan. |

`status` and `diagnose --json` show deferred counts, forced-aging readiness, and
the latest planner error. If deferrals keep growing, reduce
`ACD_INTENT_WINDOW`, check provider health, or temporarily return to
`ACD_COMMIT_STRATEGY=event`.

## Manual revert or superseded queued work

For planned revert, reset, or rebase work, pause first:

~~~bash
acd pause --repo . --reason "manual revert" --yes
# run git revert, git reset, or git rebase
acd resume --repo . --yes
~~~

If you are inside a harness shell with `ACD_SESSION_ID` set, run
`acd wake --repo . --session-id "$ACD_SESSION_ID"` after `resume` to nudge the
daemon immediately. Terminal users can usually run `acd status --watch` and wait
for the next tick.

After the operation:

~~~bash
acd status
acd events --watch
acd explain --path path/to/file
~~~

If the queued work was made obsolete by the revert or by a newer manual commit,
you should see `handled_external` or `superseded_external`. If the queue is
blocked by old terminal rows, including failed terminal barriers surfaced by
`status`, `diagnose`, or `doctor`, preview the conservative cleanup:

~~~bash
acd fix --dry-run
~~~

Apply only after reading the plan:

~~~bash
acd fix --yes
~~~

Use `recover` or `purge-events` only as advanced recovery tools when `diagnose`
or `fix` points you there.

## Branch reset, rebase, or other branch surgery

ACD detects same-branch rewinds and git operation markers. During a rewind
grace window, both capture and replay pause so transient files do not get
recaptured while you are still arranging the branch.

~~~bash
acd status
acd status --watch
~~~

Look for a pause source such as `rewind grace` or `manual`. When branch surgery
is deliberate, the safest workflow is:

~~~bash
acd pause --repo . --reason "branch surgery" --yes
# reset, rebase, switch, or inspect
acd resume --repo . --yes
acd status
acd events --watch
~~~

If the branch was changed while the daemon was stopped or if old-generation
events remain blocked:

~~~bash
acd diagnose
acd fix --dry-run
~~~

`diagnose` focuses on replay blockers and branch anchors. `fix` plans safe
state cleanup. Use `acd recover --repo . --auto --dry-run` only when diagnose
specifically reports stale replay state that should be retargeted. Recovery
preserves a manual pause marker unless you apply it with `--clear-pause`; use
`acd resume --yes` when the marker itself is the only problem.

## Skipped generated or sensitive files

ACD intentionally avoids committing generated dependency/cache trees and
sensitive paths. The default generated-tree safe-ignore list includes
`node_modules/`, `target/`, `.venv/`, `venv/`, `__pycache__/`,
`.pytest_cache/`, `.mypy_cache/`, `.ruff_cache/`, and `.gradle/`.

~~~bash
acd explain --path .env
acd events --path node_modules/pkg/index.js
acd doctor
~~~

Expected decisions include:

| Decision | Typical reason | Meaning |
|---|---|---|
| `protected` | `sensitive`, `safe_ignore`, or `gitignore` | ACD preserved an existing tracked/protected path without synthesizing a delete. |
| `skipped` | oversize, unreadable, invalid, unstable, or non-regular path | ACD left the path uncommitted by design. |

A new untracked file under a pruned generated tree or sensitive path can have no
decision row because ACD skipped walking that subtree. In that case, `doctor`
shows the active safe-ignore and sensitive-glob configuration.

To adjust generated-tree handling:

~~~bash
acd stop
ACD_SAFE_IGNORE=0 acd start

acd stop
ACD_SAFE_IGNORE_EXTRA=dist/,build/ acd start
~~~

An already-running daemon keeps the safe-ignore settings it started with. Stop
and restart it before expecting `ACD_SAFE_IGNORE` or `ACD_SAFE_IGNORE_EXTRA`
changes to affect capture or watcher pruning.

For sensitive paths, set `ACD_SENSITIVE_GLOBS` carefully. Empty or whitespace
values keep the default sensitive deny-list so a typo does not disable the
defaults.

## Blocked conflicts and failed barriers

`blocked_conflict` means ACD could not prove that replaying the captured event
would be safe. Later pending events on the same branch generation wait behind
that barrier.

`failed` rows are also terminal. When `acd status`, `acd diagnose`, or
`acd doctor` reports failed terminal events or `failed_blocking_pending`, treat
that as the same kind of replay barrier: inspect first, then preview cleanup
with `acd fix --dry-run`.

~~~bash
acd status
acd events
acd explain --path path/from/status
acd diagnose
acd fix --dry-run
~~~

Read the dry-run plan. It may propose safe actions such as deleting obsolete
barriers, marking externally handled events as published, clearing expired
manual pauses, or clearing drained backpressure. Apply the plan only when it
matches what happened:

~~~bash
acd fix --yes
acd status
~~~

If you are inside a harness shell with `ACD_SESSION_ID` set, run
`acd wake --repo . --session-id "$ACD_SESSION_ID"` before checking status.

If `fix --dry-run` reports unsafe conditions, keep the output and create a
support bundle.

## Support bundle creation

When asking for help, include the daily command output plus a doctor bundle:

~~~bash
acd status
acd events --limit 20
acd diagnose --json
acd doctor --bundle
~~~

`acd doctor --bundle` writes a zip to `~/Downloads` unless `--output` is set:

~~~bash
acd doctor --bundle --output /tmp
~~~

The bundle includes sanitized paths, safe-ignore patterns, sensitive-glob
configuration, fsnotify stats, state/meta JSON, and daemon log tails. Raw logs
are still available with `acd logs`, but `doctor --bundle` is the preferred
artifact for issue reports.

## Decision terms

| Decision | User meaning |
|---|---|
| `captured` | ACD noticed a change and queued it for replay. |
| `committed` | ACD published the queued change as a commit. |
| `intent_deferred` | Intent planning left the capture pending for a later commit. |
| `intent_forced` | ACD forced an over-deferred capture through a one-item planning window. |
| `intent_planner_error` | The planner failed validation; ACD used a safe fallback plan. |
| `skipped` | ACD intentionally left a path uncommitted, usually due to ignore or policy. |
| `protected` | ACD protected a sensitive or generated path and did not synthesize a delete. |
| `handled_external` | Another commit already contains the captured after-state. |
| `superseded_external` | External history made the queued work obsolete. |
| `blocked` | Replay stopped because applying the event was not provably safe. |
| `paused` / `resumed` | Capture or replay pause state changed because of a manual marker, rewind grace, or git operation marker. |

## See also

- [Capture and replay internals](capture-replay.md)
- [Running alongside another auto-committer](multi-tool.md)
- [AI provider configuration](ai-providers.md)
