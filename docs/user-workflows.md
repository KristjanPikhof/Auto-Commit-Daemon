# User workflows

Use this page when you want to know what ACD is doing, why a commit did not
appear, or how to recover a stuck queue.

## Daily command loop

| Need | Command | Reads or writes |
|---|---|---|
| Current repo state | `acd status` | Read |
| Live state refresh | `acd status --watch` | Read |
| Product decision ledger | `acd events` | Read |
| Stream new decisions | `acd events --watch` | Read |
| Explain one path | `acd explain --path path/to/file` | Read |
| Explain one commit | `acd explain --commit HEAD` | Read |
| Inspect latest AI prompt trace | `acd prompt --last` | Read |
| Tail raw daemon log | `acd logs --follow` | Read |
| Create support bundle | `acd doctor --bundle` | Read |

`acd prompt` only works after a non-deterministic provider ran with
`ACD_AI_PROMPT_TRACE=1`.

`acd events --watch` starts at the current ledger tail unless you pass
`--since <cursor>`.

## Repo registration

Most repos are automatic. A harness hook calls `acd start`, which creates
`<gitDir>/acd/state.db` and registers the canonical worktree root.

| Task | Command |
|---|---|
| Register without starting the daemon | `acd repo init` |
| Disable one registered repo, preserve state | `acd repo disable --repo <path>` |
| Enable a disabled repo, do not start daemon | `acd repo enable --repo <path>` |
| Interactive lifecycle manager | `acd repo manage` |
| Same manager from the daemon list command | `acd list --interactive` |
| List registered repos | `acd repo list` |
| Preview registry removal | `acd repo remove --dry-run` |
| Interactive removal | `acd repo remove` |
| Scriptable removal, keep repo state | `acd repo remove --yes` |
| Remove registry row and `.git/acd` state | `acd repo remove --yes --purge-state` |

Disable autodiscovery:

~~~json
{
  "repo_lifecycle": {
    "autodiscovery": false
  }
}
~~~

Override it for one command:

~~~bash
ACD_REPO_AUTODISCOVERY=disabled acd start
ACD_REPO_AUTODISCOVERY=enabled acd start
~~~

When autodiscovery is disabled, unregistered harness hooks skip without creating
state. Manual `acd start` tells you to run `acd repo init --repo <path>`.

Per-repo disable is stronger than global autodiscovery. A disabled registry row
stays disabled even when hooks rediscover the repo, and hook-driven `start`,
`wake`, `touch`, and `flush` return a clean `repo_disabled` skip. Manual
`acd start` points to `acd repo enable --repo <path>`.

Use `disable` when you may want the repo back: it stops the live daemon, clears
start caches, and keeps `.git/acd/state.db`. Use `remove` when you want to
unregister the row; `--purge-state` is the separate destructive state cleanup.

The manager supports compact and verbose views. Compact shows number, lifecycle
state, repo, daemon, pending, blocked, and status. Verbose adds state DB, last
seen, harnesses, and status details.

| Manager command | Action |
|---|---|
| `t N` | Toggle repo N between enabled and disabled. |
| `e N` | Enable repo N. |
| `d N` | Disable repo N. |
| `r` | Refresh from the registry. |
| `v` | Switch compact and verbose views. |
| `q` | Exit. |

## Recovery ladder

Run these in order. Stop when the queue is healthy again.

| Step | Command | What to decide |
|---|---|---|
| Observe | `acd status` | Is the daemon running, paused, waiting, or blocked? |
| Inspect decisions | `acd events --watch` | What is ACD doing now? |
| Inspect one path | `acd explain --path FILE` | Is the file captured, skipped, protected, or blocked? |
| Inspect blockers | `acd diagnose --json` | Which branch anchor or terminal row is holding replay? |
| Preview cleanup | `acd fix --dry-run` | Does the plan match what happened? |
| Safe apply | `acd fix --yes` | Apply only verifiable cleanup. |
| Force preview | `acd fix --force --dry-run` | Use only when terminal barriers still hold pending successors. |
| Force apply | `acd fix --force --yes` | Apply only after checking the blocked changes. |
| Post-check | `acd status` | Confirm `blk`/`blocked` cleared. |

`acd fix` backs up `state.db` before mutation and refuses a live daemon owner.
If the only problem is a manual pause marker, use:

~~~bash
acd resume --yes
~~~

## Common symptoms

| Symptom | First check | Likely answer |
|---|---|---|
| File was not committed | `acd explain --path FILE` | It may be pending, skipped, protected, or unseen. |
| Queue says `wait` | `acd status` | Intent mode may be waiting for count or age. |
| Queue says `blk` | `acd diagnose --json` | A terminal barrier needs operator action. |
| Commit message is generic | `acd status --json` | Provider may be deterministic fallback. |
| Path under generated tree is ignored | `acd doctor` | Safe-ignore pruned it. |
| Prompt trace is missing | `acd prompt --last` | Tracing was not enabled before the provider call. |
| External tool already committed the file | `acd explain --commit HEAD` | Expect `handled_external` or `superseded_external`. |

## File was not committed

Start with the path:

~~~bash
acd status
acd explain --path path/to/file
acd events --path path/to/file
~~~

| What you see | Meaning | Next step |
|---|---|---|
| `captured` | ACD queued it. | Wait, or run `acd wake --session-id "$ACD_SESSION_ID"` from a harness shell. |
| `committed` | ACD already published it. | Check `git log -- path/to/file`. |
| `protected` or `skipped` | ACD intentionally left it alone. | Check safe-ignore, sensitive globs, `.gitignore`, or path location. |
| No decision | ACD has not seen it. | Check daemon liveness and ignore settings. |
| `blocked` | Replay stopped first. | Run `acd diagnose --json`, then `acd fix --dry-run`. |

If the daemon is alive and the path still has no decision:

~~~bash
acd doctor
~~~

## External commits handled the change

When another tool or a manual `git commit` lands the same captured final state,
ACD settles its queued event instead of creating a duplicate commit.

~~~bash
acd events --path path/to/file
acd explain --commit HEAD
~~~

| Decision | Meaning |
|---|---|
| `handled_external` | Current `HEAD` already has the captured after-state. |
| `handled_external_after_block` | A blocked row self-healed after an external commit landed the captured state. |
| `superseded_external` | External history made the queued event obsolete. |

No action is needed when `explain` says `HEAD` contains the change. If the queue
stays blocked, go through the recovery ladder.

## Intent grouping waits

Intent mode may wait before asking the planner.

| Gate | Meaning |
|---|---|
| `ACD_INTENT_MIN_PENDING` | Wait for this many visible pending captures. |
| `ACD_INTENT_MAX_PENDING_AGE` | Publish when the oldest visible capture reaches this age. |
| `ACD_INTENT_WINDOW` | Offer at most this many captures to the planner. |
| `ACD_INTENT_DEFER_LIMIT` | Force a capture after this many deferrals. |

Inspect:

~~~bash
acd status
acd diagnose
acd doctor
acd events --watch
~~~

Drain the visible batch from a registered harness session:

~~~bash
acd flush --repo . --session-id "$ACD_SESSION_ID" --logical
~~~

Plain `acd wake` does not bypass intent batch gates.

## Inspect an AI prompt

Prompt tracing is opt-in because traces may contain source text and provider
responses.

Event mode:

~~~bash
export ACD_AI_PROVIDER=openai-compat
export ACD_AI_API_KEY=...
ACD_AI_PROMPT_TRACE=1 ACD_COMMIT_STRATEGY=event acd start
# make or capture a change
acd prompt --last
acd prompt --seq 42 --json
~~~

Intent mode:

~~~bash
export ACD_AI_PROVIDER=openai-compat
export ACD_AI_API_KEY=...
ACD_AI_PROMPT_TRACE=1 ACD_COMMIT_STRATEGY=intent acd start
# make enough changes for a batch, or run logical flush
acd prompt --last
acd prompt --seq 42
~~~

In intent mode, `--seq` can match any planner window where that seq was
offered. Remove `<gitDir>/acd/prompt-trace/` after debugging.

## Branch surgery

Pause before planned revert, reset, rebase, or branch inspection:

~~~bash
acd pause --repo . --reason "branch surgery" --yes
# git revert, reset, rebase, switch, or inspect
acd resume --repo . --yes
acd status
~~~

If you are inside a harness shell:

~~~bash
acd wake --repo . --session-id "$ACD_SESSION_ID"
~~~

| Operation | ACD behavior |
|---|---|
| `git revert` | Normal fast-forward. Matching pending work can settle at `HEAD`. |
| `git reset --soft` or `--mixed` | Same-branch rewind. Capture and replay pause for rewind grace. |
| `git reset --hard` | Same rewind behavior, with worktree overwritten by Git. |
| `git rebase -i` | Git operation marker pauses capture and replay. Generation bumps after rebase. |
| Deleted merged branch | Startup cleanup can remove stale unpublished rows for that dead ref. |

Set `ACD_KEEP_DEAD_BRANCH_BARRIERS=1` before daemon start if you need to inspect
deleted-branch rows before cleanup.

## Skipped generated or sensitive files

ACD prunes generated dependency/cache trees and protects sensitive paths.

~~~bash
acd explain --path .env
acd events --path node_modules/pkg/index.js
acd doctor
~~~

| Setting | Use |
|---|---|
| `ACD_SAFE_IGNORE=0` | Disable generated-tree pruning. |
| `ACD_SAFE_IGNORE_EXTRA=dist/,build/` | Add generated-tree patterns. |
| `ACD_SENSITIVE_GLOBS=...` | Configure sensitive globs. Empty keeps defaults. |

Restart the daemon after changing these settings.

## Generated cache flood recovery

Use this when a generated cache directory was tracked in Git and then deleted,
for example `.derivedData-provider-core/`.

| Step | Command | Result |
|---|---|---|
| Inspect | `acd diagnose --json` | Shows `generated_pending` roots, queued delete count, and tracked count. |
| Preview ACD cleanup | `acd fix --dry-run` | Shows `drop_generated_pending` actions. |
| Stop if needed | `acd stop` | Required when a live daemon owns the state DB. |
| Clean ACD queue | `acd fix --yes` | Removes only protected generated pending rows from ACD state. |
| Review Git cleanup | `git status -- .derivedData-provider-core` | Confirms which tracked generated files are deleted. |
| Stage Git cleanup | `git add -u -- .derivedData-provider-core` | Stages the tracked generated file removals. |
| Commit Git cleanup | `git commit -m "Remove tracked generated cache files"` | Records the repository cleanup. |

Why this happens: `.gitignore` and ACD safe-ignore prevent new generated files
from being captured, but they do not untrack files already committed to Git.
ACD keeps Git mutation separate from state recovery, so `acd fix --yes` never
stages or commits those removals for you.

## Blocked conflicts and failed barriers

`blocked_conflict` and `failed` are terminal. Later pending rows for the same
branch generation wait behind them.

~~~bash
acd status
acd events
acd explain --path path/from/status
acd diagnose
acd fix --dry-run
~~~

Apply only after reading the plan:

~~~bash
acd fix --yes
acd status
~~~

Use the force path only when the plan says barriers still have pending
successors:

~~~bash
acd fix --force --dry-run
acd fix --force --yes
acd status
~~~

## Cold start commit cleanup

Use `commit-all` when the daemon was off and the worktree is dirty:

~~~bash
acd commit-all --dry-run
acd commit-all --yes
acd commit-all --yes --json
acd commit-all --repo /path/to/repo --yes
~~~

What it does:

| Step | Behavior |
|---|---|
| Reseed | Rebuilds shadow state from `HEAD`. |
| Drop stale pending | Removes old pending rows for the active branch generation. |
| Capture | Captures live worktree vs `HEAD`. |
| Sort | Orders paths lexicographically. |
| Replay | Uses the configured commit strategy. |

`--json` requires `--yes` because there is no interactive prompt.

Refusals:

| Refusal | Fix |
|---|---|
| Detached HEAD | Check out a branch. |
| Git operation in progress | Finish the operation. |
| Manual pause marker | `acd resume --yes` |
| Per-repo daemon is running | `acd stop` first. |
| No initial commit | Create an initial commit. |

## Support bundle

When asking for help, include:

~~~bash
acd status
acd events --limit 20
acd diagnose --json
acd doctor --bundle
~~~

`acd doctor --bundle` writes a zip to `~/Downloads` unless you set `--output`:

~~~bash
acd doctor --bundle --output /tmp
~~~

The bundle includes sanitized paths, safe-ignore and sensitive-glob settings,
fsnotify information, state/meta JSON, and daemon log tails.

## Decision terms

| Decision | Meaning |
|---|---|
| `captured` | ACD queued a change. |
| `committed` | ACD published a queued change. |
| `intent_deferred` | Planner left a capture pending. |
| `intent_forced` | A deferred capture was forced into a one-item window. |
| `intent_planner_error` | Planner output failed validation or the provider failed. |
| `message_quality_rewrite` | ACD rewrote a weak planner message. |
| `message_quality_fallback` | ACD used deterministic fallback for the message. |
| `skipped` | ACD intentionally left a path uncommitted. |
| `protected` | A sensitive or generated path was protected. |
| `handled_external` | Another commit already contains the captured after-state. |
| `handled_external_after_block` | A blocked row was promoted after `HEAD` matched its after-state. |
| `superseded_external` | External history made queued work obsolete. |
| `blocked` | Replay stopped because applying the event was not safe. |
| `paused` / `resumed` | Manual pause, rewind grace, or Git operation state changed. |

## See also

| Doc | Use |
|---|---|
| [capture-replay.md](capture-replay.md) | Replay internals and blocker model. |
| [intent-commit-flow.md](intent-commit-flow.md) | Intent grouping and planner behavior. |
| [ai-providers.md](ai-providers.md) | Provider setup and diff privacy. |
| [multi-tool.md](multi-tool.md) | Running next to another auto-committer. |
