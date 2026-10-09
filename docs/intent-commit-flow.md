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

## Gates

A group publishes only after cohesion, completeness, separation, dependency,
materialization, verification, and revertibility checks. No preset bypasses
hard dependencies, materialization, or required verification.

Ready groups need supplied source, test, registration, generated-file, or
version-chain evidence. A confident planner explanation cannot join unrelated
changes. Available implementation and test corrections stay together. ACD
checks message quality again when it reloads a cached or saved plan.

An unchanged call can still connect a changed caller to its changed owner.
ACD keeps matching references and declarations from captured file versions,
within the same privacy and evidence limits. Named APIs in inline or fenced
documentation must point to an actual supplied declaration. Generic prose and
comments cannot prove a relationship.

A documented command such as `acd support recover --force --yes` can connect
its documentation to the changed command constructors. ACD checks the exact
registered command and flags in complete recorded Go files. An option name by
itself is not enough. Available constructors and regression tests stay in the
same goal, including when ACD reconstructs history from a saved plan.

A test-table edit can call its implementation outside the changed lines. ACD
checks the enclosing function in the captured Go file and keeps missing direct
calls. Previously published companions can supply context when their captured
path, content and mode still match the current commit. They are never offered
as new work or committed again. For a companion saved several times, ACD proves
the final recorded version of each path. Earlier versions remain provenance,
not missing work. A later correction or regression test can complete a useful
goal against that published behavior.

ACD retains those relationship lines before clipping large diffs, then keeps
complete lines from both ends of the remaining evidence. It never reads the
live worktree to supply that context.

A rejected relationship remains protected for another goal review after five
minutes, ten minutes, then hourly if the evidence is unchanged. Restarts preserve
the deadline; older one-hour waits shorten automatically from their original
start time. Review can replace the rejected grouping while keeping valid groups.
The same schedule applies when a valid provider plan leaves a goal waiting;
caching that plan does not stop later reviews.
If a valid response omits a capture, ACD keeps that work waiting and checks the
other proposed goals normally. It cannot publish a goal that needs the omitted
work. An updated planning contract rechecks older cached waits once; ordinary
restarts preserve their retry deadlines.
This also covers a plan that separates an available implementation
from its supporting test. During publication, ACD can regroup those captures
only within the frozen target. Later captures remain protected for the next
plan. Dependency, verification, and branch safety blocks still require their
own proven recovery path.

For Swift blank-line cleanup, ACD can prove one maintenance goal locally from
complete recorded before/after files. It joins blank-line space removal and
extra trailing blank-line removal in one commit, including files left in older
review windows. Other code changes, multiline literals, and ambiguous line
endings stay on the normal planning path. This proof also supports explicit
history reconstruction without sending source to a provider.

A temporary fallback partition keeps captures together until their purpose is
known. It does not establish a commit boundary. ACD can reoffer the complete,
protected partition for a new plan instead of preserving an unrelated group.
If a rejected goal invalidates another goal's prerequisite, both return to
planning. Independent valid goals stay intact.

A rejected partial plan cannot assign a capture to two retained groups. ACD
reconsiders that grouping while preserving the capture and valid assignments.

A local fallback message that only names a captured symbol or heading still
needs goal review. When its wait ends, ACD retries provider planning with the
protected evidence.

Finalized Intents can supply prerequisites for new work, but cannot absorb new
captures. If an older cached plan mixes those roles, ACD rebuilds the affected
part using the remaining planning attempts and keeps valid groups.

Relationship checks and structural verification cannot prove that the planner
understood the human purpose or that a project builds. Configure a verification
command when those checks are needed.

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

Available TypeScript helpers can join their callers through exact static named
imports and exports from complete recorded files. This check reads at most
256 KiB per file and 2 MiB per window. Ambiguous modules, dynamic imports, and
unsupported syntax provide no inferred relationship.

History review uses the same recorded relationships from the selected source
commits. Later worktree changes cannot alter the saved plan or its validation.

Ordinary WAIT findings do not prevent a protected semantic rejection from being
reviewed again. Materialization, verification, ownership, and branch safety are
checked again before publication.

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
   provider. Published events satisfy dependencies and appear only as recent
   history.
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
