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

After each response, ACD adds dependency declarations already proved by hard
edges. It keeps groups that are valid and have complete hard-dependency
closure, then sends only unresolved captures back for correction. Repeated
membership with the same findings stops the retry loop early.

Outside an active recovery, a bounded provider failure can still use the
normal evidence-based partition. Hard path, object, and rename relationships
stay together. Source/test, migration/test, exact references, generated
artifacts, persisted membership, and corroborated symbol or hunk evidence may
join components. Time or directory proximity alone never joins them. The
result passes the same materialization, verification, exact-ref CAS, and
publication journal checks as a provider plan.

ACD uses `needs_attention` only when it cannot prove a safe outcome.
Examples include unresolved dependency ambiguity, failed materialization or
verification, a revertibility failure, or uncertain branch ownership and
exact-ref state. A provider or grouping failure does not require attention
when the evidence partition passes those checks.

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
2. If that plan stalls, `local_unlock` publishes the smallest safe hard
   dependency component. A singleton is allowed. The next pass returns to
   `semantic_replan` with a new `HEAD`, remaining target, and fingerprint.

A local unlock uses a deterministic message but still passes materialization,
verification, the publication journal, exact-ref CAS, and index reconciliation.
It never calls the remote provider. If the provider circuit is open, ACD keeps
publishing one local component per pass and resumes semantic planning at the
next allowed half-open probe. Later captures remain outside the frozen recovery
target.

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
