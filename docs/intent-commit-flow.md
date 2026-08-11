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
| Fast | Smallest materializable hard component with deterministic fallback. |
| Balanced | Dependency-aware partition and fast verification; bounded safe repair may be enabled. |
| Quality | Waits for a valid high-cohesion result rather than weakening safety. |

## Planner recovery

ACD repairs a missing candidate dependency only when an existing hard capture
edge proves it. The repaired plan must pass the complete validator. It is not
used when the plan has a cycle, an unknown owner, invalid membership, invalid
ordering, or another structural defect.

An eligible metadata error can receive one remote correction. The effective
correction maximum is one even if an older saved setting contains a larger
value. Fast and Balanced move directly to their validated deterministic
fallback when local repair does not apply or the correction fails. ACD handles
this automatically and needs no additional setup.

ACD uses `needs_attention` only when it cannot prove a safe preset outcome.
Examples include unresolved dependency ambiguity, failed materialization or
verification, a revertibility failure, or uncertain branch ownership and
exact-ref state. Quality also waits rather than weakening its acceptance
rules. Provider and grouping failures that have a safe fallback remain
retrying or waiting without changing completed checkpoints.

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
