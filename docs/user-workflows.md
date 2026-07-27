# User workflows

Use this page when you want to know what ACD is doing, why a commit did not
appear, or how to recover a stuck queue.

## Daily command loop

| Need | Command | Reads or writes |
|---|---|---|
| Health summary and one next action | `acd` | Read |
| Enable and ensure the daemon is running | `acd on` | Write desired state, start if needed |
| Disable and stop while preserving state | `acd off` | Write desired state, stop if needed |
| Current repo state | `acd status` | Read |
| Live state refresh | `acd status --watch` | Read |
| Product decision ledger | `acd events` | Read |
| Stream new decisions | `acd events --watch` | Read |
| Explain one path | `acd explain --path path/to/file` | Read |
| Explain one commit | `acd explain --commit HEAD` | Read |
| Inspect latest AI prompt trace | `acd prompt --last` | Read |
| Tail raw daemon log | `acd logs --follow` | Read |
| Create support bundle | `acd doctor --bundle` | Read diagnostics and write a sanitized zip |

`acd prompt` only works after a non-deterministic provider ran with
`ACD_AI_PROMPT_TRACE=1`.

`acd events --watch` starts at the current ledger tail unless you pass
`--since <cursor>`.

Use bare `acd configure` for global provider and everyday defaults. Use
`acd configure --repo .` for a repository override or Strict Review, and
`acd settings` for advanced overrides, profiles, and experiments. Saved
non-secret values do not require shell sourcing. See the
[settings guide](settings.md) for source precedence, safe activation, and
rejected-revision recovery.

Global Everyday setup runs no project test or validation job. Repository Strict
Review returns after provider testing and durable validation queueing. Capture
stays active while commit publishing waits. Use
`acd configure --repo . --wait` to follow the strict job, or close the terminal
and inspect it later with `acd status`.

See [commands.md](commands.md) for the complete public command reference,
including repo administration, maintenance, and the hook protocol.

## Repo registration

Most repos are automatic. A harness hook calls `acd start`, which creates
`<gitDir>/acd/state.db` and registers the canonical worktree root.

For normal manual control, use `acd on` and `acd off`. Both are idempotent and
preserve `.git/acd` state. The commands below are for registry administration
or bulk lifecycle work.

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

ACD automatically reconciles a blocked or failed queue as one complete,
immutable branch-generation chain. It either proves that stable `HEAD` already
contains the final touched-path state or archives the reconstructed chain under
`refs/acd/recovery/*`, then reseeds and captures the live worktree again.

Run these in order. Stop when the queue is healthy again.

| Step | Command | What to decide |
|---|---|---|
| Observe | `acd` | Is the repo healthy, waiting, degraded, off, or in need of attention? |
| Inspect decisions | `acd events --watch` | What is ACD doing now? |
| Inspect one path | `acd explain --path FILE` | Is the file captured, skipped, protected, or blocked? |
| Inspect blockers | `acd diagnose --json` | Which branch anchor or terminal row is holding replay? |
| Preview reconciliation | `acd fix --dry-run` | Does the exact-chain proof match what happened? |
| Prevent hook restart during repair | `acd off` | Stop the daemon and keep the repo disabled while state changes. |
| Apply reconciliation | `acd fix --yes` | Prove the chain at `HEAD`; otherwise preserve it at an archive ref. |
| Preview archive-only recovery | `acd fix --force --dry-run` | Show which complete chain and recovery ref would be used. |
| Apply archive-only recovery | `acd fix --force --yes` | Save the complete chain without trying the publish proof. |
| Start normal operation again | `acd on` | Enable the repo and ensure the daemon is healthy. |
| Post-check | `acd` | Confirm the repo no longer needs attention. |

`acd fix` creates and verifies a SQLite-consistent backup before migration or
recovery mutation, and refuses a live daemon owner. It never retargets captures
to another branch generation, deletes terminal rows, or changes the live
worktree, index, or branch.
If the only problem is a manual pause marker, use:

~~~bash
acd resume --yes
~~~

## Recover an interrupted Intent repair

Balanced and Quality may rebuild a recent private ACD-only commit suffix when a
late companion capture belongs to a soft-published candidate. ACD first creates
an `refs/acd/intent-repair/.../backup` ref and stores every old-to-new commit
mapping.

If the process stops after the Git ref update, restart the same v2 binary:

~~~bash
acd on
acd status
acd doctor
~~~

The daemon completes database reconciliation when the persisted mapping and Git
state agree. Otherwise it retains the backup and reports recoverable guidance.
Do not delete the backup ref or reset the branch while that recovery is
pending.

Repair is skipped, without changing staged content, when staging overlaps the
candidate. It is also skipped if a merge, tag, another local branch, or a
remote-tracking ref contains a rewritten commit. A skipped repair becomes a
new candidate and commit.

## Inspect or restore archived work

Recovery decisions name the exact hidden ref. Find recent archive decisions and
list the refs without changing your worktree:

~~~bash
acd events --json --limit 50
git for-each-ref --format='%(refname) %(objectname:short)' refs/acd/recovery/
~~~

Use the `/archive` ref from the `recovery_archived` decision:

| Task | Command |
|---|---|
| Compare the archive with current `HEAD` | `git diff HEAD <archive-ref> --` |
| Read one archived file | `git show <archive-ref>:path/to/file` |
| Restore one path into the worktree | `git restore --source=<archive-ref> -- path/to/file` |
| Keep the whole archive as a review branch | `git branch acd-recovery-review <archive-ref>` |

The first two commands are read-only. The restore and branch commands are
explicit operator actions; ACD never applies an archive back onto the active
branch by itself. `/published` refs are proof that stable `HEAD` already held
the final captured state, so they normally need no restore.

## Common symptoms

| Symptom | First check | Likely answer |
|---|---|---|
| File was not committed | `acd explain --path FILE` | It may be pending, skipped, protected, or unseen. |
| Queue says `wait` | `acd status` | Intent mode may be waiting for count or age. |
| Queue says `blk` | `acd diagnose --json` | ACD may be reconciling a complete chain, or proof needs inspection. |
| Commit message is generic | `acd status --json` | Planner circuit may be using deterministic fallback. |
| Replay says `needs_attention` | `acd diagnose --json` | Repeated replay failed at the reported capture. Inspect its candidate IDs and bounded error before resuming. |
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
| `blocked` | Replay stopped first. | Let automatic chain reconciliation run, then use `acd diagnose --json` if it remains. |

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
| `recovery_published` | ACD proved the complete unpublished chain at stable `HEAD`. |
| `recovery_archived` | ACD preserved the complete chain under a hidden recovery ref. |

No action is needed when `explain` says `HEAD` contains the change. If the queue
stays blocked, go through the recovery ladder.

## Intent candidate waits

Intent mode may wait for a candidate boundary or a missing prerequisite.

| Gate | Meaning |
|---|---|
| `ACD_INTENT_MIN_PENDING` | Prefer this many visible captures before evaluation. |
| `ACD_INTENT_SETTLE_WINDOW` | After the count gate, wait for a quiet burst boundary before planning. |
| `ACD_INTENT_MAX_PENDING_AGE` | Evaluate when the oldest visible capture reaches this age. |
| `ACD_INTENT_WINDOW` | Offer at most this many captures to the planner. |
| Soft or logical boundary | Evaluate the current activity epoch without bypassing safety gates. |

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

Planner failure behavior is preset-specific. Fast uses the smallest valid
hard-dependency component. Balanced uses a valid or deterministic dependency
partition and runs structural verification. Quality keeps candidates pending
and reports `needs_attention`. During the 30-second, 2-minute, and 10-minute
cooldowns ACD applies that policy, then sends one automatic probe. Inspect
`intent_strategy.planner_health` in status or diagnose JSON for the circuit
state, failure class, bypass count, and next probe time.

Missing Intent v2 prerequisites always block replay, not capture. Run bare
`acd configure` for missing inherited provider, credential, or diff consent.
Run `acd configure --repo .` for repository-specific settings or Strict Review
approval. ACD does not fall back to v1 or metadata-only planning.

Configuration validation uses the same capture-only safety state. A queued or
running job needs no action. A failed or timed-out job preserves the desired
revision and last-known-good runtime. Re-run `acd configure --repo .` to retry
the exact strict check or switch experience.

To check whether work was captured, planner-visible, and committed as intended:

| Question | Command |
|---|---|
| Which captures are still waiting? | `acd status --json` |
| What did the last planner evaluation contain? | `acd status --json` and inspect `intent_strategy.last_planner_window` |
| What is waiting or ready? | Inspect `intent_v2` candidate, verification, and repair fields in status JSON |
| Was one seq offered or assigned? | `acd events --json --since <cursor>` and inspect planner and candidate data |
| Why is replay waiting? | `acd diagnose --json` and inspect migration, prerequisite, batch wait, and verification fields |

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

In intent mode, `--seq` can match any prompt-traced planner window where that
seq was offered. For privacy-safe metadata without raw prompts or diffs, use
`acd events --json` or `acd status --json`. Remove
`<gitDir>/acd/prompt-trace/` after debugging.

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
| Deleted merged branch | Startup cleanup archives complete unpublished pairs before accepting cleanup. |

Set `ACD_KEEP_DEAD_BRANCH_BARRIERS=1` before daemon start if you need to keep
deleted-branch unpublished rows in their original queue state for inspection.

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
| `ACD_SENSITIVE_GLOBS=...` | Replace the protected path globs. Unset or empty uses the defaults. |

These fields are labeled `restart required` in `acd settings`. Save the change,
then restart the daemon explicitly with `acd off` followed by `acd on`. The
new daemon resolves the saved repository, selected-profile, and global values
before capture starts. The settings command never starts a stopped daemon.

## Generated cache flood recovery

Use this when a generated cache directory was tracked in Git and then deleted,
for example `.derivedData-provider-core/`.

| Step | Command | Result |
|---|---|---|
| Inspect | `acd diagnose --json` | Shows `generated_pending` roots, queued delete count, and tracked count. |
| Preview ACD cleanup | `acd fix --dry-run` | Shows `drop_generated_pending` actions. |
| Turn ACD off | `acd off` | Stops the daemon and prevents hooks from restarting it during cleanup. |
| Clean ACD queue | `acd fix --yes` | Removes only protected generated pending rows from ACD state. |
| Turn ACD on | `acd on` | Restarts normal capture and replay after cleanup. |
| Review Git cleanup | `git status -- .derivedData-provider-core` | Confirms which tracked generated files are deleted. |
| Stage Git cleanup | `git add -u -- .derivedData-provider-core` | Stages the tracked generated file removals. |
| Commit Git cleanup | `git commit -m "Remove tracked generated cache files"` | Records the repository cleanup. |

Why this happens: `.gitignore` and ACD safe-ignore prevent new generated files
from being captured, but they do not untrack files already committed to Git.
ACD keeps Git mutation separate from state recovery, so `acd fix --yes` never
stages or commits those removals for you.

## Blocked conflicts and failed barriers

`blocked_conflict` and `failed` stop ordinary FIFO replay. Later pending rows for
the same branch generation wait while ACD tries all-or-none chain
reconciliation.

~~~bash
acd status
acd events
acd explain --path path/from/status
acd diagnose
acd fix --dry-run
~~~

Apply only after reading the plan:

~~~bash
acd off
acd fix --yes
acd on
acd
~~~

Use the force path when the chain cannot be proven at `HEAD` and you want an
explicit archive-only recovery:

~~~bash
acd fix --force --dry-run
acd off
acd fix --force --yes
acd on
acd
~~~

## Commit work made while ACD was off

Use `commit-all` when the daemon was off and the worktree is dirty:

~~~bash
acd off
acd commit-all --dry-run
acd commit-all --yes
acd commit-all --yes --json
acd commit-all --repo /path/to/repo --yes
acd on
~~~

What it does:

| Step | Behavior |
|---|---|
| Preserve | Proves or archives every pre-existing exact unpublished pair. |
| Reseed | Rebuilds shadow state from `HEAD`. |
| Capture | Captures live worktree vs `HEAD`. |
| Sort | Orders paths lexicographically. |
| Replay | Uses the configured commit strategy. |

`--json` requires `--yes` because there is no interactive prompt.
`--dry-run` and declined confirmation are read-only and do not start the AI
provider. If unpublished rows remain after replay, `commit-all` exits non-zero
with an incomplete result instead of reporting success.

Refusals:

| Refusal | Fix |
|---|---|
| Detached HEAD | Check out a branch. |
| Git operation in progress | Finish the operation. |
| Manual pause marker | `acd resume --yes` |
| The daemon is running when the command is ready to write | `acd off` first. Dry-run, a declined prompt, and a clean no-op do not acquire `daemon.lock`. |
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
| `recovery_published` | ACD proved that the branch already contains every captured change in the unpublished chain. |
| `recovery_archived` | ACD saved the unpublished chain under a hidden recovery ref before removing it from replay. |
| `blocked` | Replay stopped because applying the event was not safe. |
| `paused` / `resumed` | Manual pause, rewind grace, or Git operation state changed. |

## See also

| Doc | Use |
|---|---|
| [capture-replay.md](capture-replay.md) | Replay internals and blocker model. |
| [intent-commit-flow.md](intent-commit-flow.md) | Intent grouping and planner behavior. |
| [ai-providers.md](ai-providers.md) | Provider setup and diff privacy. |
| [multi-tool.md](multi-tool.md) | Running next to another auto-committer. |
