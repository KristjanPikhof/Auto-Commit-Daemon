# Protection and explicit finish contract

This is the selected implementation contract for the finish-based release.
The installed runtime may still use the earlier publication workflow until the
user explicitly upgrades it. Editing source does not upgrade the runtime.

## Commands

Commands resolve the current directory to its Git worktree. `--repo PATH`
selects another worktree explicitly. `--json` emits one result on stdout;
progress goes to stderr, and `--quiet` hides progress. None of these commands
pushes to a remote.

| Command | Help summary | Success and mutation boundary |
|---|---|---|
| `acd on` | Protect this worktree continuously | Set up a missing initial configuration, enable this worktree, start its worker, and confirm a durable checkpoint. No credentials, AI request, project checks, or branch commit. |
| `acd off` | Save a final checkpoint and stop protection | Complete a final checkpoint before disabling the worktree. Retain snapshots and state. Create no branch commit; refuse to claim success if protection is incomplete. |
| `acd finish [PURPOSE]` | Create local commits for the current protected batch | Freeze a fresh complete checkpoint and current attached HEAD, prepare and verify the whole chain, then publish once. The command itself authorizes publication; it needs no `--yes` or interactive prompt. |
| `acd finish --dry-run [PURPOSE]` | Preview and verify a finish without publishing | Freeze a checkpoint, plan, and verify as an explicit request. Private objects and request evidence may be written, and Intent may contact its configured provider. Never change the branch, index, or live files. |
| `acd finish --retry ID` | Retry the exact failed finish target | Start one new bounded attempt on the saved target. Keep its checkpoint, expected branch and HEAD, mode, style, context, message, index identity, and consent. Never absorb later edits. |
| `acd finish --include-staged` | Include the frozen staged selection explicitly | Authorize only the exact frozen index selection. Publication uses the approved-index protocol below. Neither ordinary finish nor compatibility `--yes` gives staged consent. |
| `acd finish --message TEXT` | Use this complete commit message | Create one complete batch commit with the supplied subject and optional body. Validate the selected message style and reject empty or unsafe text. Intent still uses its configured provider and never silently becomes local mode. |
| `acd history rewrite` | Preview or explicitly apply a saved history rewrite | Retain the selection, saved-plan, preview/apply, and source-history safety interfaces below. Never run it from protection or hooks. |
| `acd commit-all` | Deprecated: use acd finish | Delegate to finish for the first finish-based release only. Retain no drain, fallback, retry loop, or second publication engine. Accept legacy `--yes` as an explicit invocation alias; it does not authorize staged content. |

`PURPOSE` is one optional quoted argument describing the completed goal. It
supplies context for messages and grouping. `--message` supplies the complete
message; purpose can accompany it as explanation. Both become immutable when
the target freezes. `--retry` may combine with `--dry-run`, `--repo`, `--json`,
and `--quiet`; it rejects new purpose, message, or staged consent. Unknown IDs,
foreign-worktree IDs, and attempts to retry a successful publication are errors.
A completed preview does not publish later: the next ordinary finish freezes a
fresh checkpoint. `--retry ID --dry-run` checks a failed target without applying
it and leaves that target retryable.

`acd on` handles initial setup only for the selected worktree. Existing state
that needs an upgrade reports `setup_required` and directs the user to the
explicit advanced `acd setup` entry point. It never silently upgrades other
repositories. Hooks cannot register or re-enable worktrees. Finish requires a
compatible active protection worker; otherwise it directs the user to `acd on`
or `acd setup` without pretending that a request ran.

## Offline defaults and commit messages

New installations use `commit.strategy=local` and `commit.format=imperative`.
Migration preserves explicit Intent and message-style choices. Legacy `event`
strategy becomes local batch semantics. Supported strategies are `local` and
`intent`; both require finish.

Local mode uses no AI, provider process, credentials, or network connection. It
creates one complete batch commit by default. Supplied context explains the
goal; absent context, a deterministic summary describes observed changes and
its body gives useful facts. It does not pretend to know the developer's
motivation or promise semantic decomposition.

Intent uses the configured AI provider to identify completed goals, propose
safe boundaries, and supply messages. Missing credentials, rejection, timeout,
or unavailable providers fail the request. There is no automatic switch to
local mode. A user can change strategy explicitly and run a new finish.

Imperative subjects name an action, such as `Preserve staged content during
finish`. Conventional subjects use `type: subject`, such as
`fix: preserve staged content`. Keep the existing scope-free contract. These
are the two styles. "Authoritative"
wording means imperative, not a third style. Messages must be nonempty, respect
the selected format, and contain no terminal control characters; body newlines
remain valid.

## Frozen target and ownership

There is one unfinished batch per worktree and no begin command. A request
records repository and worktree identity, attached branch ref, expected HEAD
(or an absent ref for an unborn branch), source checkpoint/tree identity, retained finish target tree,
index identity and consent, options, and immutable input evidence before
planning. Build the complete finish target by applying frozen eligible
checkpoint changes to the base tree while preserving unchanged excluded or
sensitive tracked entries and gitlinks. Eligibility exclusions never imply
deleting those branch entries. Pin any target tree different from the source
checkpoint through its own retained private ref before planning. Private snapshots retain their normal recovery provenance.

An empty newly initialized repository can be protected. An empty finish is a
successful no-op. A nonempty finish on an attached unborn branch creates its
first commit through create-only branch CAS; it does not create an unrelated
branch. Detached HEAD, conflicts, active merge/rebase/cherry-pick/bisect state,
ambiguous branch ownership across linked worktrees, or stale branch/index
identity prevent publication. Continuous checkpoint protection remains active
where eligible files are readable. Partial checkpoints cannot be finish targets.

An identical request reconnects to the active attempt and never duplicates it.
A different request while an attempt owns the worktree returns `finish_busy`.
After an untouched failed attempt, a plain new finish may supersede its plan
using a newer complete checkpoint. Keep the old snapshots and failure evidence.
A prepared or ambiguous publication must first resolve through exact proof;
it cannot be superseded as if nothing happened.

Later file changes remain eligible for continuous protection while planning,
verification, and publication run. They never enter the frozen request. Finish
and recovery build commits using scratch indexes and private materializations;
they never restore or check out a candidate over the live worktree.

## Request state and recovery

Persist only transitions needed to prove ownership or recovery. `stage` gives
progress detail without adding a durable state machine for every activity.

| State | Meaning | Next step |
|---|---|---|
| `running` | The frozen request is preparing, planning, or verifying | Continue only within this explicit attempt's bounds. |
| `prepared` | The full chain and its one branch/index mutation proof are durable | Perform or recover the recorded transition exactly. |
| `completed` | Published once, verified preview completed, or no changes found | Report the outcome explicitly. Never infer publication from this state alone. |
| `failed` | The bounded attempt ended without publication | Report the cause and an explicit retry/new-finish action. |
| `needs_attention` | Git/index evidence cannot prove a safe automatic outcome | Preserve evidence and report the exact required action. |

Terminal states do not schedule provider calls, verification, or publication in
the background. Client disconnect detaches from the bounded attempt; it does
not cancel checkpoint protection or authorize a second attempt. On process
restart, a pre-publication `running` request becomes a retryable failure.
Restart never renews its deadline. A `prepared` request recovers only from its
saved branch/index proof. Exact pre-CAS ownership allows completing the saved
operation; an exact post-CAS chain allows recording success idempotently.
Different ref or index movement remains `needs_attention`.

An explicit retry creates a new attempt and deadline for the same frozen target,
with separate evidence. It checks current ownership before work and publication.
It never changes options or silently rebases a stale target. If the branch
moved, finish must report that a new request is needed rather than guessing.

## Planning, provider bounds, and verification

Plan completed goals before assigning changed paths. Keep implementation,
callers, imports, tests, generated output, and relevant documentation together
when they complete one goal. Put prerequisites first or in the same commit.
Each split needs a useful review/revert boundary and a valid intermediate tree.
Different files, directories, save times, age, and queue pressure do not justify
splits. Available unpublished corrections belong with the change they correct.

Each changed entry belongs to exactly one goal. Respect rename chains,
create/delete order, hard dependencies, and generated relationships. Materialize
only recorded path versions; do not fabricate code or intermediate history.
The final candidate tree must equal the entire frozen target exactly, including
unchanged entries, file modes, deletions, renames, symlinks, and binary objects.
Coverage and equality are structural evidence, not proof that project tests ran.

One Intent attempt allows one planning call plus at most one correction within
one persisted `ai.timeout` deadline (default five minutes). Messages belong to
that plan, so they have no separate retry loop. A valid AI plan whose proposed
splits cannot produce complete safe candidates may collapse to one whole-batch
goal using the provider's grounded purpose/message and all the same checks.
This is conservative handling of a valid plan, not local fallback. Provider
failure, malformed or incomplete output, unavailable evidence, and exhausted
budget fail the request instead.

Provider inputs reuse existing byte limits. These bounds are separate from
checkpoint storage; `capture.max_file_bytes` controls buffering and does not
make large regular files ineligible for protection.

| Evidence | Bound |
|---|---|
| One included textual path diff | `ai.IntentStageDiffCap`, 64 KiB |
| Aggregate textual finish evidence | `ai.IntentGoalEvidenceTotalDiffCap`, 256 KiB |
| Aggregate history rewrite diffs | `ai.HistoryRewriteTotalDiffCap`, 128 KiB |
| Serialized provider planning request | `ai.HistoryRewriteRequestByteCap`, 512 KiB |

Measure actual serialized bytes and complete required evidence before sending.
Exceeding a bound fails with `provider_input_too_large`, the measured size,
limit, and actionable guidance: use an explicit local finish for a complete
offline batch, or select a smaller explicit history range for rewriting. Keep
all work protected. Never clip evidence, silently omit eligible changes,
split by age, or send partial input while describing it as complete. Binary
entries use declared object/type/size metadata and exact Git objects for tree
validation; report that their content was not sent as text.

Diff egress remains off by default. Intent must honor explicit repository
provider, subprocess, and diff-egress approval; redact secrets before allowed
transmission. Disclose metadata, supplied purpose/message, and allowed diff
content sent to that provider. Local mode sends nothing. Status, list, doctor,
and diagnose neither build providers nor migrate/write state. Logs, JSON,
errors, and prompt traces must preserve credential redaction.

Always verify structure and exact target equality. Preserve approved
`intent.verification` policies (`none`, `structural`, `fast`, `full`) and their
configured command/timeouts: `none` skips project commands, never the safety
checks. Run approved commands in isolated candidate materializations, not the
live worktree. Record each candidate tree, policy, command, environment
identity, result, duration, and output provenance. A cached result is reusable
only for the identical tree, command, environment, and policy; label it reused
and never claim it ran afresh. Report absent commands, structural-only checks,
failed checks, and full verified checks distinctly. A candidate failure leaves
the branch unchanged and work protected.

## Publication and staged content

Build and verify the entire commit chain privately before preparing one final
branch CAS from expected HEAD to the chain tip. Persist all object IDs and the
mutation proof before CAS. A failed candidate cannot publish a partial chain.
No hook, idle period, stop event, age threshold, retry timer, or worker restart
creates a new publication request or rewrites history.

Ordinary finish refuses a staged selection, including staged-only content and
intent-to-add. Its failure tells the user to keep their staging or rerun with
`--include-staged`. It never clears it implicitly.

With explicit consent, freeze the index's content identity, including entry
modes, flags, staged deletions, and intent-to-add distinctions. The retained finish target tree uses approved index versions and deletions
only for paths changed in the frozen staged selection, with complete-checkpoint
versions elsewhere. The source checkpoint still proves the original worktree
bytes; keep its tree identity separate from the finish target tree. Unstaged differences on those paths remain live, protected
work for a later finish. Preview lists this selection so staged-only bytes are
not silently replaced with live file bytes. Staged sensitive or excluded paths
fail with `staged_scope_excluded`; explicit consent does not bypass eligibility
or silently omit those bytes. Local mode still creates one complete batch
commit from this selected target.

Before publication and index consumption, recheck branch and index identity.
Use the existing approved-index protocol: take Git's exclusive index lock,
compare content identity, prepare an index for the exact published HEAD,
preserve retained sparse-checkout/assume-unchanged flags, sync bytes, atomically
rename, and sync the containing directory. A changed index aborts consumption;
staging added later must survive. After-CAS interruption reports the proven
branch outcome separately from pending index cleanup. Recovery recognizes an
exact already-consumed index idempotently; ambiguity requires attention and
never authorizes overwriting new staging. Keep worktree bytes untouched.

## Results, exits, and observability

Retain the product JSON envelope: `ok`, `state`, `changed`, `actions`,
`next_action`, `data`, and `error`. Errors contain `code`, safe `message`,
`retryable`, and redacted `details`. `retryable` means an explicit retry can be
attempted; it never promises background retry. `ok` reports command execution,
not repository health. `changed` records command mutations, including private
checkpoints/evidence; `published` reports branch commitment separately.

Finish `data` contains `request_id`, `attempt`, `state`, `stage`, `repo`,
`worktree_id`, `branch_ref`, `expected_head`, `checkpoint_id`, `checkpoint_tree`, `target_tree`,
`mode`, `style`, `dry_run`, `protected`, `published`, `no_changes`,
`commits_created`, `new_head`, `staged_consumed`, `verification`, and the safe
failure details. Unavailable identities are null; commit count is zero until a
proven publication. Status/list show the same durable request outcome, current
coverage, worker liveness, and later protected/uncommitted work separately.
They never label saved or recovered snapshots as branch commits. Deprecated
resolved-work booleans retain their earlier meaning alongside the exact outcome.

| Result | Exit/code | Required human output and next action |
|---|---|---|
| Protected on | `0` | `Protection: on`; checkpoint ID and coverage. |
| Protected off | `0` | `Protection: off`; final checkpoint ID. |
| Published finish | `0` | Request ID, checkpoint ID, `Published: N commit(s)`, tip, verification evidence. |
| Empty finish | `0` | `No changes to finish`; `Published: 0 commit(s)`. |
| Completed preview | `0` | `Finish preview`; ordered goals/messages and exact trees; `Published: no`; run a new `acd finish` to apply current work. |
| Invalid arguments or incompatible retry options | `2 / invalid_argument` | Explain the invalid argument; make no request or mutation. |
| Incomplete coverage | `3 / protection_incomplete` | List failed eligible paths safely; protection continues; repair readability/stability and run finish again. |
| Staged selection without consent | `3 / staged_changes` | `Staged changes preserved`; use `--include-staged` only after review. |
| Excluded staged path | `3 / staged_scope_excluded` | Identify the excluded path safely; preserve staging and require an explicit eligible selection. |
| Changed frozen staging | `3 / staging_changed` | `Staging changed; current selection preserved`; new finish required. |
| Unsafe/stale Git or ambiguous worktree ownership | `3 / unsafe_git_state` | Give the exact state and safe new-request/unblock action. |
| Different active request | `3 / finish_busy` | Report active request ID; reconnect to it or wait for its bounded outcome. |
| Setup/runtime mismatch | `3 / setup_required` | State the mismatch; direct to explicit setup without upgrading other repositories. |
| Unknown or foreign retry target | `2 / invalid_argument` | Explain target identity; no request mutation. |
| Oversized evidence | `3 / provider_input_too_large` | Size/limit and guidance above; protected, unpublished. |
| Provider rejection/invalid plan/failed verification | `3 / finish_failed` | Request ID, cause, verification evidence, and `acd finish --retry ID` when ownership permits. |
| Provider timeout/transient transport failure | `4 / finish_unavailable` | Failed request ID and explicit retry; no automatic local fallback. |
| Worker/supervisor unavailable before acceptance | `4 / supervisor_unavailable` | State whether a durable request exists; reconnect only to that proven request. |
| Restarted pre-publication attempt | `3 / finish_interrupted` | Failed request ID and explicit retry; protection continues. |
| Ambiguous prepared transition | `3 / finish_needs_attention` | Exact proven branch/index facts and required user action; no guessed recovery. |
| Unexpected internal failure | `1 / internal_error` | Safe error and request ID if saved; no false publication claim. |

A disconnect cannot deliver a terminal result to that client. The durable
request remains discoverable through status/list, and reconnect returns its
actual outcome. Failure never disables independent checkpoint protection.

## Explicit history rewrite and legacy preservation

Keep `acd history rewrite --last N --plan-out FILE`, the existing explicit
`--from-sha`, `--from-nr`, `--range-sha`, and `--range-nr` selectors,
`--messages-only`, and `--new-branch NAME`. Selection remains contiguous linear
history; merges and unsupported states are rejected before mutation.
`--show-plan ID_OR_FILE` reads without an AI request. `--apply ID_OR_FILE
--dry-run` verifies without rewriting; `--apply ID_OR_FILE --yes` explicitly
applies the saved plan. Never infer apply consent from preview. Preserve saved
message-plan editing where supported; immutable goal reconstruction requires a
new plan rather than editing its proof.

Intent rewrite uses the same bounded planning, purpose, styles, privacy, and
verification rules as finish. Local rewrite uses deterministic existing order
and grouping, with no AI or claims of inferred semantic splits. Preserve
author boundaries, exact final selected tree, recorded versions, source backup
refs, live files, index, later protection, and restart proof. New-branch rewrite
preserves its source branch and rejects an existing target. Stale plans are
rejected rather than silently regenerated or applied.

Upgrade preserves captures, immutable provenance, checkpoints, restore/history,
and unfinished legacy publication evidence. Keep narrow read-only legacy
decoders only where needed to inspect, preserve, or reject old plans safely.
Resolve prepared legacy mutations through exact proof; otherwise preserve a
retryable or actionable record. Do not resume old planners, candidate waiting,
aging, automatic repair, or semantic recovery loops. Retire their configuration,
queues, diagnostics, hooks, and tests once callers migrate. Preserve compatible
provider transports, redaction, both styles, restore, and explicit rewriting.
