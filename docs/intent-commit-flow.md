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

Candidate evaluation begins on quiet time, soft or logical boundary, maximum
age, or capacity. Optional integrations may provide boundaries, but filesystem
protection does not depend on them.

## Presets

| Preset | Publication behavior |
|---|---|
| Fast | Smallest materializable hard component with deterministic fallback. |
| Balanced | Dependency-aware partition and fast verification; bounded safe repair may be enabled. |
| Quality | Waits for a valid high-cohesion result rather than weakening safety. |

Provider, grouping, or verification failure changes product state to `waiting`
without changing completed checkpoints.

## Publication safety

The publication path prepares exact source, target, tree, and membership before
literal branch-ref CAS. Completion atomically records normal commit mappings
and checkpoint publication links. Startup recovery proves the same immutable
facts. Ambiguity remains `needs_action` and never causes a guessed settlement.

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
