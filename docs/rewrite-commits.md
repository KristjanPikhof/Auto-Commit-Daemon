# Rewrite commit history

`acd history rewrite` selects existing commits and saves a reviewable rewrite
plan. It never runs automatically. Apply changes history, so review the exact
selection and resulting trees before confirming.

The selected current Git commits are the source of truth. Saved capture queues,
old rewrite proposals, and legacy archive errors do not supply input or block a
new rewrite.

~~~bash
acd history rewrite --last 5 --plan-only
acd history rewrite --show-plan PLAN_ID
acd history rewrite --apply PLAN_ID --dry-run
acd history rewrite --apply PLAN_ID --yes
~~~

Use `--plan-out rewrite.json` for a standalone plan file. `--show-plan` and
`--apply` accept a saved ID or file. Select a contiguous linear range with
`--last`, `--from-sha`, `--from-nr`, `--range-sha`, or `--range-nr`. Numeric
positions count from HEAD as 1. Merge commits and unsafe Git states are rejected.

## Choose the rewrite

| Mode | Result |
|---|---|
| Local | Original grouping, order, authors, and empty commits; no provider |
| Intent | Completed goals built from exact recorded versions |
| `--messages-only` | One result per original commit, including empty or mutually cancelling commits |
| `--new-branch NAME` | Reconstructed history on a new branch; source branch preserved |

`--new-branch` generates a goal plan and cannot be combined with
`--messages-only`. Show or apply that plan by its saved ID, without repeating
`--new-branch`.

Intent can split mixed commits and regroup interleaved work when exact versions,
dependencies, rename groups, and author boundaries permit it. Neutral recorded
changes remain provenance without fabricated output. Local does not claim
semantic decomposition. It keeps valid original messages and uses deterministic
style-compatible summaries when needed.

~~~bash
acd history rewrite --last 5 --messages-only --plan-only
acd history rewrite --last 5 --new-branch checkout-cleanup --plan-only
~~~

Messages-only under Intent uses bounded provider planning with fixed original
membership and order. Under Local it remains offline. Different authors never
share an emitted goal. Every resulting tree must be complete and the final
selected tree must match the original exactly.

## Saved plans and apply

Planning can create private source and goal evidence refs, even with
`--plan-only`. Intent uses the same privacy, style, one-exchange plus optional
correction, and fixed-deadline contract as finish. Local builds no provider.
Showing, editing, and applying an existing plan make no AI request.

~~~bash
acd history rewrite --edit PLAN_ID --plan-only
acd history rewrite --apply PLAN_ID --dry-run
~~~

The editor can change Local or messages-only messages. It saves a new plan
revision. Purposeful goal reconstruction needs a new plan to change its goals;
editing cannot silently rewrite immutable proof.

Apply requires an enabled canonical worker, a clean starting worktree and
index, an attached stable branch, no conflicting operation, and matching saved
configuration and source evidence. It checks every candidate and the whole
chain before one publication transaction. Later file edits remain protected
and are not overwritten. A changed index or branch aborts unsafe apply.

Before moving a branch, ACD durably saves the prepared rewrite proof with full
SQLite synchronization. Restart recovery uses that exact proof.

A private backup ref retains the original HEAD before in-place rewriting.
`--new-branch` rejects an existing target and preserves the original branch;
it does not check out the new branch. Saved plans are bound to their exact
source history and goal refs. Stale or altered proof is rejected rather than
silently regenerated. Legacy plans remain inspectable where supported but do
not restart the retired rewrite engine.

Progress goes to stderr, leaving stdout for the command result. Use
`--progress auto|plain|json|off`, or `--quiet`. See the
[rewrite flow](intent-commit-rewrite-flow.md) for the proof sequence and
[providers](ai-providers.md) for input disclosure.
