# Intent publication flow

Intent groups captured changes from completed checkpoints into coherent local
Git commits. It never participates in checkpoint durability.

~~~bash
acd config set commit.strategy intent
acd config set commit.preset fast
~~~

## Inputs and assignment

Every visible captured change is assigned exactly once. The planner evaluates
hard dependencies first: same-path order, rename chains, before/after objects,
create/modify/delete constraints, and known generated dependencies. Soft
evidence includes directory proximity, source/test convention, symbols,
references, roles, and activity epochs. Time is weak evidence.

Candidate metadata stores privacy-safe summaries, never raw diffs. Limits are
256 captures, 128 open candidates, 4096 edges per exact pair, bounded purpose
and atomicity fields, and 64 KiB verification output. Every hard ordering edge
gets capacity first. ACD rebuilds the remaining soft evidence from active
captures and drops the least current soft evidence when the graph is full.

Open goals retain their purpose, affected paths, membership, and missing
companions across planning windows. ACD retrieves recorded diffs for goals
connected to the current work. That context follows the same diff permission
and redaction rules as fresh captures. Later captures outside a frozen
publication target remain input for the next plan.

When a small frozen target's remaining captures fit the planning window, ACD
reviews them together. An implementation and its available test can then finish
the same goal instead of waiting in separate requests. Later captures stay
protected for the next publication target.

A documentation follow-up can also see proven published companions from its
frozen target. ACD checks their recorded versions against the current branch and
supplies them as context, without publishing them again.

## Gates

A group publishes only after cohesion, completeness, separation, dependency,
materialization, verification, and revertibility checks. No preset bypasses
hard dependencies, materialization, or required verification.

Ready groups need recorded source, test, registration, generated-file, or
version-chain evidence. A planner explanation alone cannot join unrelated
changes. Missing companions must follow from concrete evidence; a useful
correction or documentation goal does not always need new implementation or tests.
ACD rechecks message quality when it reloads a cached or saved plan.

ACD uses captured file versions to connect changed owners and callers, even
when the relevant call falls outside the edited lines. Go calls and types,
static TypeScript imports and exports, script file references, and named APIs
can supply that proof. Documented commands must match recorded registration
and flags, such as `acd support recover --force --yes`; an option name alone
is insufficient. Documentation can also connect through matching descriptions
of the same behavior. Generic prose and comments do not establish code ownership.

Published companions provide read-only context when their final recorded path,
content, and mode match the current branch. Intermediate saves remain provenance.
Lookup stays bounded, and filenames affect search order without proving a
relationship. Published goals cannot absorb new captures. Stale plans that
try to do so return to planning.

Relationship lines survive evidence clipping. Live reviews allow 64 KiB per
capture and 256 KiB in total, with current work ahead of older published context.
ACD keeps complete lines from both ends of a clipped diff and retains the exact
published call or type that admitted support. It reads recorded blobs rather
than the live worktree. TypeScript declarations are parsed once per recorded
file and reused across comparisons with published tests.

For a frozen target, later protected explanatory Go edits can clarify intent
only when their code tokens are identical and they contain no compiler
directives. These comments supply context; they do not add later work to the
publication target.

Rejected goals and valid provider waits receive another review after five
minutes, ten minutes, then hourly for unchanged evidence. Restarts preserve
deadlines. Older one-hour waits shorten from their original start time, and
an upgraded planning contract rechecks older cached waits once. ACD may review
a wait once against supplied diff and published-reference facts within the
existing planning budget. Readiness still requires every normal gate.

Valid ready groups survive correction. Omitted captures remain waiting, and
incomplete or overlapping groups return to planning together with goals whose
prerequisites they invalidate. A provisional fallback partition is not a fixed
commit boundary. Filename, raw-symbol, heading-only, and clipped messages still
need goal review. Within a frozen target, ACD can regroup an implementation and
its available tests; later captures stay protected for the next plan.

For Swift blank-line cleanup, ACD can prove one maintenance goal from complete
recorded before/after files. It groups whitespace removal from blank lines and
extra trailing blank lines, including files left in older review windows.
Multiline literals, ambiguous line endings, and other code edits use ordinary
planning. Saved history plans rebuild this proof locally without provider access.

Relationship checks cannot prove the human purpose or that a project builds.
Configure a verification command when build or test results are required.

Candidate evaluation normally waits until the newest capture has been quiet for
the configured settle window. Filling the planning window does not skip that
wait. A durable soft or hard activity boundary, an explicit logical flush, a
dependency-safe forced-aging window, or the maximum pending age can release
work sooner. Window and high-water limits bound each planning pass, not the
durable pending queue. A larger queue remains checkpoint-protected and is
offered to the planner in bounded passes.
Optional integrations may provide boundaries, but filesystem protection does
not depend on them.

## Commit purpose

The planner identifies completed goals before assigning captures. Each commit
should explain one useful step and leave a working state under the configured
checks. Implementation, callers, imports, tests, generated files, and relevant
documentation stay with the step they support. Different files, screens, or
capture times do not justify separate commits.

Each split needs a useful review or revert boundary. A preparatory refactor can
precede the feature it enables when both are complete steps. Helpers must
precede their callers or ship together. Documentation links belong with their
targets, and available test updates belong with the changed behavior.
Unpublished assertion corrections and other supporting edits stay with the
change they finish.

For example, a shortcut menu implementation, its tests, and a corrected
assertion form one change. A settings-placement document and its index link
form another. The same model-row presentation improvement across several
screens can form one commit when the diffs show a shared purpose.

A change with a broader effect, such as changing a shared default, can be a
separate step when it is independently meaningful and safe. Its message should
explain the reason supported by the available evidence. ACD must not invent
motivation or claim tests ran when it has no such evidence.

These rules guide grouping and messages; they do not impose a commit count.
ACD cannot split a captured file change into invented intermediate versions.
When a safe intermediate step is unavailable, it keeps the required changes
together. Missing dependencies must follow from evidence. Age and queue
pressure never prove completeness. Checks apply to the proposed commit tree,
not the final combined worktree. Structural checks do not prove that a project
builds or its tests pass; that requires a configured verification command.
Existing planning-window bounds, frozen targets, and history-repair limits
still apply. Planner guidance does not authorize rewriting published history.

## History quality and reconstruction

When publication is idle, optional Intent repair checks at most five recent,
private, ACD-owned commits inside the configured repair horizon. A generic
message can trigger goal planning from exact recorded path versions. The repair
keeps immutable capture provenance and records each old-to-new commit relation,
including a mixed commit that contributes to several goals. Unchanged safe or
ineligible evidence is checked once. Shared or user-owned history remains a
firm boundary.

Explicit `acd history rewrite --new-branch NAME` uses the same goal checks on a
selected linear history. It can split mixed commits and regroup interleaved
changes using recorded whole-path versions. The active worker verifies each
proposed commit tree before creating the new branch. The source branch, live
files, and staging stay intact; later edits keep entering checkpoints.

Plans are immutable and are revalidated at apply. The final tree must equal
the selected source HEAD, every recorded transition must belong to one goal,
and edited renames must stay together. Author boundaries also remain intact.
ACD never invents an intermediate file version. Reconstruction is bounded to
256 source/output commits and 256 focused path chains; larger independent
selections must be narrowed. Structural verification alone does not claim that
builds or tests passed.

ACD reconstructs generated files from their exact recorded versions. Shortened
provider evidence does not omit those changes from the resulting files.
If a planner assigns the same path chain to several goals, ACD rejects the plan
and identifies the conflicting assignments in its bounded correction retries.

## Presets

| Preset | Publication behavior |
|---|---|
| Fast | Evidence-based partition without a configured verification command. |
| Balanced | Evidence-based partition with structural verification and bounded safe repair. |
| Quality | Evidence-based partition with full verification and bounded safe repair. |

## Planner recovery

ACD can make three planning attempts for one unchanged capture window. The
`intent.retry_on_invalid` setting counts extra attempts after the first call
and is capped at two. The attempt count is tied to a durable fingerprint, so a
restart, flush, or `acd off` followed by `acd on` cannot restart the loop.

Before it reserves an attempt, ACD rebuilds a local baseline from the current
captures, candidates, dependencies, boundaries, and forced-aging state. The
baseline must assign every visible capture, preserve hard-dependency closure,
and pass the structural safety checks. Native v2 providers receive this
baseline and may refine its grouping and messages.

Retained work outside the offered window remains visible as read-only evidence.
Only offered captures carry assignment IDs. Dependencies on retained work name
the owning Intent, so the provider can understand prerequisites without trying
to assign captures that are still waiting for a later review.

Older local waiting groups labeled `Update files` can be regrouped when their
recorded provenance proves they are provisional, unpublished, and protected.
Purposeful waiting goals keep their boundaries and review deadlines.

A stale label does not override a newer exact saved plan that identifies a
protected group as an unknown goal. The newest native assignment decides this
check; a later purposeful plan closes older fallback evidence.

Rejecting a ready group does not remove other waiting groups from its retry
schedule. Older schedules missing those members use the saved plan to recover
still-pending work on the same branch, keeping the original retry deadline.

TypeScript reference checks read at most 256 KiB per file and 2 MiB per window.
Ambiguous modules, dynamic imports, and unsupported syntax provide no inferred
relationship.

History review uses the same recorded relationships from the selected source
commits. Later worktree changes cannot alter the saved plan or its validation.

Ordinary WAIT findings do not prevent a protected semantic rejection from being
reviewed again. Materialization, verification, ownership, and branch safety are
checked again before publication.

An overdue singleton becomes a normal goal review when protected companions
join it. Existing targets stopped by the old singleton request error reopen
only when their remaining members are protected and no transaction is active.

Fresh capture scans do not count as progress on a frozen publication target.
Status and list keep its progress, provider waits, and goal-review deadlines
visible while ACD protects newer edits.

If the baseline is invalid, ACD records `preflight_blocked` and does not call
the provider. A changed planning snapshot gets a new fingerprint and a fresh
preflight. Unrelated maintenance warnings remain visible, but they do not block
planning unless they make the snapshot unsafe.

After each response, ACD adds dependency declarations already proved by hard
edges. It keeps groups that are valid and have complete hard-dependency
closure, then sends only unresolved captures back for correction. Repeated
membership with the same findings stops the retry loop early. Each narrowed
correction request receives a newly validated baseline before another attempt
is reserved.

Age can trigger another evaluation, but it never makes an incomplete group
ready. ACD keeps a real missing companion, failed materialization, verification
failure, or branch-safety problem waiting. A bounded lookahead retrieves
available source/test/reference companions beyond the ordinary window;
unrelated directory or time proximity does not expand the goal.

An older run stopped by that mistaken size limit gets one automatic retry.
ACD requires the recorded forced-aging failure, the matching size-limit hold,
and a completed ready repair plan for the affected capture. Pending candidate
members must remain inside the frozen target, with no conflicting publication
or repair in progress. Recovery reopens checkpointing and repeats the normal
safety checks; it does not publish directly from the old plan.

When a completed plan still matches the same fingerprint, ACD reloads and
revalidates that plan instead of asking the provider again or rebuilding an
evidence partition. Planner windows report this as `completed_plan_reuse`.
The fingerprint covers all planning inputs and hashes captured diffs. If the
cached plan no longer validates, ACD records `local_cache_rebuild`, replaces
its grouping with the valid local baseline, and avoids a semantic replan call.

Outside an active recovery, a bounded planning failure can still use the normal
evidence-based partition. Hard path, object, and rename relationships stay
together. Source/test, migration/test, exact references, generated artifacts,
persisted membership, and corroborated symbol or hunk evidence may join new
captures. Time or directory proximity alone never joins them.

A published candidate is a firm fallback boundary. If a hard edge reaches a
recent private ACD commit, ACD first makes one semantic repair replan. A valid
result may merge or repartition the repairable suffix, but it must pass the
existing repair journal, backup-ref, materialization, verification, and
exact-ref CAS checks. Planner windows report this as `repair_replan`.

If that replan fails, ACD leaves the earlier commit OIDs unchanged. It groups
only the new captures and records the earlier candidates as dependencies. The
new group receives a local message from its captured evidence and still passes
materialization and verification. Planner windows report this as
`dependent_message_fallback`.

Each provider probe has an `ai.timeout` budget and each planning session has
at most three semantic attempts. Valid groups and messages survive partial
correction and restart. Transport failures leave the run retryable and do not
spend the semantic attempt budget.

During an outage, checkpoints continue protecting new work. ACD retries after
five minutes, ten minutes, then hourly, using a persisted schedule and one probe
at a time. Local recovery still needs a complete goal, a useful evidence-based
message, exact materialization, required verification, and the normal repair
limits. Unknown companions and generic messages remain protected for later
planning. An unchanged unresolved local plan receives a scheduled review.

If the user applies a newer verified deterministic Intent configuration with
the same message format, the existing journaled recovery path preserves the
unpublished target on a recovery ref, invalidates its old capture baseline, and
recaptures work under the new contract. This also handles an active target;
changing a saved provider string alone does not reinterpret frozen work.
Overlapping operations or ambiguous provenance still require attention.

ACD uses `needs_attention` only when it cannot prove a safe outcome.

When an AI outage leaves a local fallback group too large to publish, the
frozen run waits for the provider retry instead of stopping permanently.
Older runs stopped by this path resume after ACD matches the saved transport
failure and rechecks the branch, target, and active operations. Later captures
stay outside that target. Recovery previews show stopped runs; explicit
`acd support recover --force --yes` preserves the whole unpublished chain on
a recovery ref before recapturing current work.
Examples include unresolved dependency ambiguity, failed materialization, a
revertibility failure, or uncertain branch ownership and exact-ref state. A
verification failure first starts bounded automatic checkpoint replanning and
target widening. It needs attention only when the complete frozen recovery
target exhausts required verification. A provider or grouping failure does not
require attention when the evidence partition passes the other checks.
An older drain stopped only because soft dependency evidence filled the shared
edge limit resumes automatically with the rebuilt graph. A hard-edge overflow
or cycle remains stopped because ACD cannot safely discard ordering evidence.

Status, diagnose, and doctor report the preflight state, finding codes,
provider attempts, and why a provider call was skipped. Replay-error repeats
remain separate from provider-attempt counts, so a recovery loop cannot look
like repeated AI usage.

Each explicit publication drain has a bounded semantic path. Invalid planner
output cannot start a hot retry loop. The drain survives terminal disconnects,
worker replacement, and daemon restart, and resumes from its durable phase.

If a semantic pass repeats the same recoverable state, ACD stops rebuilding
that plan. This applies even when more events remain outside the current
planning window. The worker records a restart-safe normalization step, retires
only mixed or overlapping candidate membership, and replans the frozen pending
target from the current `HEAD`.

Recovery then alternates between two bounded modes:

1. `semantic_replan` offers only unresolved target events to the configured
   provider. Verified published events satisfy dependencies and supply read-only
   recorded context; they cannot be selected for publication again.
2. If that plan stalls, `local_unlock` selects the smallest safe hard
   dependency component. A singleton is allowed. Its local message describes
   the captured evidence and passes message-quality checks. The next pass
   returns to `semantic_replan` with a new `HEAD`, remaining target, and
   fingerprint.

A local unlock generates membership and messages without a provider request.
It still passes materialization, verification, the publication journal,
exact-ref CAS, and index reconciliation. Later captures remain outside the
frozen recovery target.

History repair remains a bounded optimization. If its time horizon has expired
or the published suffix is no longer safe to rewrite, ACD retires the blocking
candidate and moves forward from the current `HEAD`. Published commits remain
unchanged. The durable recovery marker freezes the affected pending dependency
component and records whether the next pass is `semantic_replan` or
`local_unlock`. A restarted worker continues from that stage without
reconstructing the old candidate. Existing markers without a stage begin with
semantic replanning, and the older `atomic_dependency_components` drain mode
performs one local unlock before returning to semantic planning.

Automatic recovery does not weaken publication safety. A branch or `HEAD`
transition, detached `HEAD`, manual pause, or active Git operation waits for
the repository to become stable. Exhausted required-verification recovery,
ambiguous self-publication, missing objects, materialization conflicts, and
dependency cycles that cannot be proved safe stop with `needs_attention`.

## Publication safety

The publication path prepares exact source, target, tree, and membership before
literal branch-ref CAS. Completion atomically records normal commit mappings
and checkpoint publication links. Startup recovery proves the same immutable
facts. Ambiguity remains `needs_attention` and never causes a guessed
settlement.

Intent history repair follows the same rule. It freezes the exact captures for
each rebuilt candidate before Git can move, then records the rewrite as an
ACD-owned branch transition. The running daemon and a restarted worker can
adopt that transition without treating it as an external rebase. Captures made
after a publication target was frozen stay pending for the next semantic plan.

An unpublished candidate that repeatedly fails verification cannot hold the
queue forever. ACD lets later paths reach the planner first. If that still
makes no progress, it follows the bounded overlap between failed groups and
their completed checkpoints, retires those unpublished groups together, and
replans the exact protected set. At that point the earliest failed capture wins
even if unrelated work has an older planner timestamp. Every capture must still
be pending on the same branch generation, or already be durably resolved by an
earlier recovery pass. The frozen target stays active until every member is
resolved. A healthy overlapping group or an active publication or repair
transaction stops recovery rather than weakening its boundary.

Balanced/Quality repair is restricted to a private contiguous ACD-authored
first-parent suffix at exact `HEAD`; it rejects merges, tags, other refs, Git
operations, publication pause, staged overlap, and failed gates. Normal
publication never rewrites history and never pushes.

An internal repair preserves ACD's live capture baseline and advances only its
recorded base `HEAD`. Dirty paths that are still pending therefore remain
represented once instead of being captured again after the repair.

Replay also reloads its private scratch index from the repaired tree before it
publishes another candidate. Before creating each commit, ACD verifies that the
new tree changes only paths owned by that candidate's frozen captures.

An older runtime could publish the next candidate from the tree that existed
before a repair. ACD can settle that frozen target automatically only when the
immediate child of the persisted branch head is explained exactly by one or
more adjacent, completed, frozen repairs and every remaining capture in the
active publication drain. It protects that exact child with a private proof
ref, locks the live branch at the observed descendant, and updates the frozen
capture lifecycle, live shadow, and branch token in one state transaction.
Captures made after the frozen target stay pending for the next plan. ACD does
not move `HEAD` or change the index or worktree, and it never credits newer
descendant commits to the frozen target. Missing, incomplete, or ambiguous
evidence stops recovery safely.

If an older runtime already left repeated captures in a queue, replay compares
each file update with the state produced so far. An exact after-state match is
a safe no-op, so the remaining real change can continue without user cleanup.

Inspect product-facing results with:

~~~bash
acd status
acd history
acd history explain
acd doctor
~~~
