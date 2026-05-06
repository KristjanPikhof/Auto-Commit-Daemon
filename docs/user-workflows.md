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
acd prompt --last
~~~

`acd status` is the current snapshot: daemon liveness, queue counts, pause
state, branch generation, active commit strategy, and recent decision counts.
`acd events` is the durable decision ledger. It tells you what ACD captured,
skipped, committed, deferred, grouped, blocked, or treated as already handled by
another committer. `acd explain` turns those decisions into a path or commit
answer.

Use `acd prompt` only when you enabled prompt tracing with
`ACD_AI_PROMPT_TRACE=1`. It reads local prompt-trace JSONL files and does not
open or migrate `state.db`.

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
useful diagnostics instead of asking someone to read raw JSONL. `acd logs`
does not contain full AI prompts; prompt traces live separately under
`<gitDir>/acd/prompt-trace/` and are visible only after you opt in with
`ACD_AI_PROMPT_TRACE=1`.

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
the latest planner error.

If commits are waiting before any planner request appears, check the batch wait
state:

~~~bash
acd status
acd diagnose
acd doctor
~~~

`ACD_INTENT_WINDOW` is the maximum offered to a normal planner pass,
`ACD_INTENT_MIN_PENDING` is the preferred count trigger, and
`ACD_INTENT_MAX_PENDING_AGE` is the age escape hatch for sparse queues. When the
visible pending count is below `min_pending` and the oldest pending capture is
younger than `max_pending_age`, ACD waits instead of asking the planner. The
status, diagnose, and doctor reports show how many more captures are needed or
how long remains until the age trigger. To publish the current visible batch
now, run `acd wake --repo . --session-id "$ACD_SESSION_ID"` from a harness shell
or otherwise request a flush; that bypasses only the batch wait, not validation
or replay safety checks.

If deferrals keep growing after planning starts, reduce the batching thresholds,
check provider health, or temporarily return to `ACD_COMMIT_STRATEGY=event`.
When a capture reaches `ACD_INTENT_DEFER_LIMIT`, ACD forces it through a
one-item planning window unless an earlier related-path capture must land first.

## Inspect an AI prompt

Prompt tracing is opt-in because it records the actual payload sent to an AI
provider or subprocess plugin. The trace is stored locally under
`<gitDir>/acd/prompt-trace/`. Records are written after ACD redacts and
truncates outbound payloads, but they may still contain source code, private
paths, request envelopes, tool schemas, provider responses, and fallback
metadata. The default deterministic provider sends no AI request, so it creates
no prompt trace. Trace files are daily JSONL logs with no automatic pruning;
ACD buffers 256 pending prompt-trace records in memory and drops the oldest
buffered record if the writer falls behind. Treat prompt traces as sensitive
local diagnostics.

Event-mode inspection:

~~~bash
export ACD_AI_PROVIDER=openai-compat
export ACD_AI_API_KEY=...
ACD_AI_PROMPT_TRACE=1 ACD_COMMIT_STRATEGY=event acd start
# make or capture a change
acd prompt --last
acd prompt --seq 42 --json
~~~

Intent-mode inspection:

~~~bash
export ACD_AI_PROVIDER=openai-compat
export ACD_AI_API_KEY=...
ACD_AI_PROMPT_TRACE=1 ACD_COMMIT_STRATEGY=intent acd start
# make enough changes for a batch, or use acd wake to flush the visible queue
acd prompt --last
acd prompt --seq 42
~~~

In event mode, `--seq` selects the commit-message prompt for that captured
event. In intent mode, `--seq` also matches planner windows where that seq was
offered, so you can see the offered seqs, selected/deferred seqs, grouping
reason, validation error, and fallback provider. If no trace is found, restart
the daemon with `ACD_AI_PROMPT_TRACE=1` and a non-deterministic provider;
`acd prompt` never creates traces by itself.

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

## Cold start commit cleanup

Use `acd commit-all` when the daemon was off for a while and your worktree
contains many uncommitted files. It performs a one-shot capture and replay cycle
without starting the persistent daemon, then exits.

Typical situations where `commit-all` helps:

- You opened a repo, made edits, and forgot to start ACD first.
- You paused ACD during a large merge and now have a dirty worktree.
- You want to bring a brand-new clone into a committed baseline before enabling
  the live daemon.

`commit-all` reads the active commit strategy from existing config — daemon meta
first, then the `ACD_COMMIT_STRATEGY` env, then the canonical default. There is
no `--strategy` override flag; the one-shot run matches exactly what the daemon
would do on its own.

**Reseed before capture.** `commit-all` always force-reseeds `shadow_paths`
from `HEAD`'s tree before it captures, and drops any stale `pending`
capture events for the active `(branch_ref, branch_generation)` pair. This
guarantees the diff that drives commit decisions is "live worktree vs
HEAD", not "live worktree vs whatever shadow happens to remain from an
earlier daemon session". Without the reseed, a daemon that captured edits
into shadow but failed to replay them would leave a poisoned shadow
mirroring live state — the next `commit-all` would see zero diff and
report `Commits: 0` while the worktree was still dirty. The JSON output
includes a `dropped_stale_pending` count and a `shadow reseeded from
HEAD` note for visibility.

**Ordering.** Because ACD has no historical modification times, files are sorted
lexicographically by path. Sibling files in the same directory cluster together
in the commit history, so directories like `pkg/a/*.go` land adjacent.
With `ACD_COMMIT_STRATEGY=intent`, the planner receives coherent windows of
path-sorted siblings, which improves grouping quality even without mtime
ordering.

**Confirmation flow.** Without `--yes`, `commit-all` prints a summary and asks
before writing any commit:

~~~
Repo: /path/to/repo (refs/heads/main @ abc123456789)
Pending events: 42
Strategy: event (provider deterministic)
Estimated passes: 42
Proceed? [y/N]:
~~~

With `intent` strategy the summary also shows intent window and defer limit:

~~~
Repo: /path/to/repo (refs/heads/main @ abc123456789)
Pending events: 12
Strategy: intent (provider openai-compat)
Intent window: 10, defer limit: 2
Estimated passes: 2
Proceed? [y/N]:
~~~

**Flags.**

~~~bash
acd commit-all --dry-run          # plan and show summary; no commits written
acd commit-all --yes              # skip the interactive confirmation prompt
acd commit-all --yes --json       # machine-readable JSON output (requires --yes)
acd commit-all --repo /path/to/repo --yes
~~~

`--dry-run` shows the pending count and estimated passes without writing
anything. With `ACD_COMMIT_STRATEGY=intent`, it also calls the planner and
prints how many captures would be selected or deferred in the first window.

**Refusal cases.** `commit-all` refuses to run when:

- `HEAD` is detached — check out a branch first.
- A git operation is in progress: rebase, merge, cherry-pick, or bisect.
- A manual pause marker is present — run `acd resume --yes` first.
- The per-repo daemon is already running — stop it first with `acd stop`.

**Intent strategy and deferred files.** When `ACD_COMMIT_STRATEGY=intent`, the
planner sees at most `ACD_INTENT_WINDOW` files per pass. Files that are deferred
`ACD_INTENT_DEFER_LIMIT` times are forced into a one-item commit so they cannot
starve. For a large dirty worktree, estimated passes equals
`ceil(pending / ACD_INTENT_WINDOW)`, and each pass makes real calls to the
configured AI provider.

**Non-interactive use.**

~~~bash
acd commit-all --yes --json | jq '.commits'
~~~

`--json` requires `--yes` because no interactive prompt is available. The JSON
payload includes `ok`, `strategy`, `provider`, `pending_before`,
`pending_after`, `commits`, `head_before`, `head_after`, and `duration_ms`.

After `commit-all` finishes, start the live daemon normally:

~~~bash
acd start
acd status
~~~

## See also

- [Capture and replay internals](capture-replay.md)
- [Running alongside another auto-committer](multi-tool.md)
- [AI provider configuration](ai-providers.md)
