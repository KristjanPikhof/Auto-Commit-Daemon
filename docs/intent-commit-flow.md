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
and atomicity fields, and 64 KiB verification output.

## Gates

A group publishes only after cohesion, completeness, separation, dependency,
materialization, verification, and revertibility checks. No preset bypasses
hard dependencies, materialization, or required verification.

Candidate evaluation normally waits until the newest capture has been quiet for
the configured settle window. Filling the planning window does not skip that
wait. A durable soft or hard activity boundary, an explicit logical flush, a
dependency-safe forced-aging window, or the maximum pending age can release
work sooner. Window
and high-water limits still bound each planning pass and the pending queue.
Optional integrations may provide boundaries, but filesystem protection does
not depend on them.

## Presets

| Preset | Publication behavior |
|---|---|
| Fast | Evidence-based partition with structural checks. |
| Balanced | Evidence-based partition with fast verification and bounded safe repair. |
| Quality | Evidence-based partition with stronger verification and bounded safe repair. |

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

For forced aging, ACD may discard a missing companion invented by the model
only when an exact baseline group proves that all available hard dependencies
are complete. A real waiting dependency, missing object, materialization
failure, verification failure, or branch-safety problem still blocks the work.

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
new group still needs a locked message-only rewrite before it can publish.
Planner windows report this as `dependent_message_fallback`.

If message generation is unavailable, ACD does not publish a generic message
such as `Update <path>`. The candidate remains waiting across daemon restarts
and retries after the provider recovers. Planner windows and diagnostics report
`waiting_message_rewrite` until a meaningful message is available.

ACD uses `needs_attention` only when it cannot prove a safe outcome.
Examples include unresolved dependency ambiguity, failed materialization or
verification, a revertibility failure, or uncertain branch ownership and
exact-ref state. A provider or grouping failure does not require attention
when the evidence partition passes those checks.

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
   dependency component. A singleton is allowed. It publishes only after a
   locked semantic message is available. The next pass returns to
   `semantic_replan` with a new `HEAD`, remaining target, and fingerprint.

A local unlock uses deterministic membership and still passes materialization,
verification, the publication journal, exact-ref CAS, and index reconciliation.
It waits if the provider circuit cannot supply a semantic message. Later
captures remain outside the frozen recovery target.

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
the repository to become stable. Failed required verification, ambiguous
self-publication, missing objects, materialization conflicts, and dependency
cycles that cannot be proved safe stop with `needs_attention`.

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
makes no progress, it can supersede only the failed candidate and replan the
single completed checkpoint that contains it. This recovery requires every
checkpoint capture to remain pending on the same branch generation and refuses
to run while a publication or repair transaction is active.

Balanced/Quality repair is restricted to a private contiguous ACD-authored
first-parent suffix at exact `HEAD`; it rejects merges, tags, other refs, Git
operations, publication pause, staged overlap, and failed gates. Normal
publication never rewrites history and never pushes.

Inspect product-facing results with:

~~~bash
acd status
acd history
acd history explain
acd doctor
~~~
