# User workflows

## Install and enable protection

Install the released CLI with Homebrew:

~~~bash
brew install KristjanPikhof/tap/acd
~~~

Or use the verified release installer:

~~~bash
curl -fsSL \
  https://raw.githubusercontent.com/KristjanPikhof/Auto-Commit-Daemon/main/scripts/install.sh |
  bash
export PATH="$HOME/.local/bin:$PATH"
~~~

See the [README requirements](../README.md#requirements) for platform tools.
Then enable one repository:

~~~bash
cd /path/to/repository
acd on
acd status
~~~

Fresh `acd on` creates missing protection with offline Local defaults and waits
for a complete checkpoint. It needs no provider or credential. An incompatible
installed runtime requires the explicit reviewed `acd setup` upgrade first.
Hooks cannot grant repository consent for you.

`acd setup` is also the advanced entry for integrations and configuration.
Use `acd setup --accessible` or `acd config --accessible` for linear prompts.

## Save and finish

Edit and save normally. Check protection whenever useful:

~~~bash
acd status
acd list --once
acd history
~~~

Protection and branch publication are separate. Saved changes can remain
uncommitted indefinitely until you explicitly finish them:

~~~bash
acd finish "Add offline checkout"
~~~

Local creates one complete offline batch. Intent plans completed goals through
the approved provider. Both verify exact trees and preserve live working files.
Neither pushes. If the frozen target equals HEAD, finish reports no changes
and creates no empty commit.

To supply an exact message that matches your selected style:

~~~bash
acd finish --message "Preserve staged content during finish"
# With commit.format=conventional:
acd finish --message "fix: preserve staged content"
~~~

A supplied message does not bypass the provider in Intent mode. Use reviewed
Local settings for an offline finish.

## Preview and staged work

~~~bash
acd finish --dry-run "Add offline checkout"
~~~

Dry run plans and checks an explicit target. It can write private evidence,
call Intent AI, and execute approved project checks. It leaves the branch,
index, and working files unchanged. Run a new ordinary finish afterward to
freeze current work again.

Ordinary finish refuses staged content. Keep your staging, or review it and
explicitly include that exact selection:

~~~bash
acd finish --include-staged "Complete checkout validation"
~~~

For selected paths, the target uses frozen index content or deletions. Unstaged
differences on those paths stay in your worktree for the next finish. Other
eligible worktree changes enter the batch. Excluded or sensitive staged paths
are rejected. A changed index aborts publication rather than clearing staging
added later.

During a long attempt, later edits continue to receive checkpoints and remain
outside its frozen target. First-release `acd commit-all` is a visible
deprecated delegate to finish; prefer `acd finish` in new scripts.

## A finish fails or the terminal closes

A failed provider, rejected plan, failed check, unsafe branch, or changed index
leaves work protected. Read the request ID and reason:

~~~bash
acd status --verbose
acd doctor
acd finish --retry REQUEST_ID
~~~

Closing the terminal detaches the display while the worker owns the accepted
request. Retry reconnects to active work or explicitly retries a failed target.
It does not add later edits or change purpose, message, or staged consent.
Use `--retry REQUEST_ID --dry-run` only for a failed target preview; it does not
turn a completed dry run into publication.

Intent has one planning exchange and at most one correction under a fixed
deadline. It never falls back to Local. Correct credentials or the cause before
retrying. To change the work or mode, review settings and start a new finish.
Prepared publication recovers from exact saved proof after restart; an
interrupted running attempt becomes failed rather than making new AI calls.

`none` and `structural` verification run safety checks without project commands.
`fast` and `full` use approved configured commands. Read the reported evidence;
missing or reused checks are not freshly executed tests.

## Restore a checkpoint

~~~bash
acd history
acd restore cp-...          # preview
acd restore cp-... --yes    # apply the checked preview
~~~

Bare `acd restore` opens a terminal picker. Scripts must supply an ID. Restore
saves current preimages, preserves HEAD and the index, and prints an undo
checkpoint. Resolve staged overlap before apply.

If a post-restore checkpoint was interrupted, inspect the exact repair plan:

~~~bash
acd support repair
acd support repair --yes
~~~

Forward repair requires the protected tree to match the restore target. Do not
delete locks, databases, or private refs to bypass unclear ownership.

## Rewrite existing history

History rewriting is an independent explicit operation:

~~~bash
acd history rewrite --last 5 --plan-only
acd history rewrite --show-plan PLAN_ID
acd history rewrite --apply PLAN_ID --dry-run
acd history rewrite --apply PLAN_ID --yes
~~~

Local preserves original grouping and order without AI. Intent can reconstruct
completed goals. Use `--messages-only` to retain every original commit or
`--new-branch NAME` to preserve the source branch. Read the
[history rewrite guide](rewrite-commits.md) before applying.

## Turn protection off and on

~~~bash
acd off
acd on
~~~

Off takes a final checkpoint, disables the repository, and stops its worker.
It does not start a finish. If final protection cannot be confirmed, ACD stays on;
`--force` explicitly accepts that risk. On re-enables only the selected
repository and confirms fresh protection. Existing checkpoint history remains.
Startup can recover an already authorized prepared publication from exact saved
proof; it never creates a new finish or AI attempt.

## Upgrade and diagnose

Source changes and a newer CLI do not replace an older running installation.
An old runtime can still commit automatically until you explicitly upgrade:

~~~bash
acd setup --dry-run
acd setup
~~~

Review the upgrade plan. It preserves supported settings, checkpoints, restore
history, and unfinished legacy provenance. Disabled repositories stay disabled.
Ambiguous legacy publication proof requires attention and does not authorize
restarting the retired planner.

For local evidence:

~~~bash
acd doctor
acd support diagnose --json
acd support logs
acd support bundle
~~~

Status and diagnostics do not test providers or write repository state. Worker
liveness and complete protection are separate facts. Use the specific next
action in the report rather than treating every wait as a failure.

## Uninstall

~~~bash
acd uninstall --dry-run
acd uninstall
~~~

Default uninstall preserves repository databases and private refs. Removing
protected data requires a separate purge review and explicit confirmation.
Running workers save a final checkpoint before stopping. Repositories that
are already stopped stay stopped; uninstall does not resume a prepared finish.

To start clean across registered worktrees:

~~~bash
acd uninstall --dry-run --purge-data
acd uninstall --purge-data --confirm-purge-data
~~~

This removes the runtime, ACD repository databases and directories, and private
checkpoint and archive refs. Project Git history, staging, and working files
stay unchanged. Run setup again when you want to reinstall.
