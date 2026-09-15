# AI semantic reliability: validation record

Branch: `feat/ai-semantic-reliability`  
Base: `3285f423` (refreshed `main`, 2026-09-15)

The [implementation plan](ai-semantic-reliability.md) defines the accepted
behavior. The [coverage mapping](ai-semantic-coverage.md) records retired paths
and the scenarios retained through current production commands.

## Implemented contracts

| Area | Result |
|---|---|
| Protection during slow publication | The worker services checkpoint protection while one isolated evaluation runs; it validates ownership before applying results. |
| AI failures | AI-configured repositories retain their message contract. Temporary failures use durable capped retries; semantic correction has a separate budget. |
| Restart and stale results | Immutable membership, branch/configuration identity, cancellation and durable caches prevent duplicate or stale application. |
| Recovery | Journaled preservation and recapture keep original evidence; corrected credentials and explicit local-mode changes use validated activation. |
| Status | Current checkpoint coverage, current-branch commitment and historical recovery are separate; old JSON booleans remain compatible. |
| Setup and settings | AI-first fresh setup, explicit offline choice, preserved upgrades, separate repository consent and verified runtime readiness. |
| Commit-all | Reviewed paths and staging are rechecked. Index replacement holds Git's lock and preserves flags. Fresh review can replace a refused request without rewriting its approval. |
| Restore | Terminal picker and ID commands share affected-file preview, overlap checks and undo checkpoints. |
| Maintenance | Existing checkpoint maintenance and activity filtering are retained from the base. |

## Measured production behavior

Measurements use isolated repositories and a release-style child binary. The
integration runner's race flag does not instrument that child binary. Package
race tests cover internal concurrency separately.

| Measurement | Observed |
|---|---:|
| Initial checkpoint | 1.429 s |
| Checkpoint during blocked AI call | 1.176 s |
| Idle CPU time over a 1.013 s sample | 0.040 s |
| Idle resident memory | 35.2 MiB |
| Benchmark scenario including build | 19 s |
| Ten repeated hook calls, quiet run | 1.073 s total; 101.5–116.5 ms each |

Raw benchmark: `/tmp/acd-testing-baseline/final-measurements/summary.json`.
Hook evidence: `/tmp/acd-testing-baseline/hooks-after-quiet.jsonl`.
These are local macOS observations, not hosted Linux timing guarantees.
Earlier benchmark failures required capture classification before the provider
returned; that assertion did not measure durable protection and is not used as
a before/after latency comparison.

## Test selection and timing

The production integration baseline contained 114 top-level scenarios and
613.40 seconds of summed scenario time. An unsharded run exceeded five minutes;
its remaining cases were measured separately. Four shards follow the planned
three-minute execution budget (`ceil(613.40 / 180)`). Deterministic manifests
include every discovered test, example and fuzz seed exactly once per run.
Repeated stress intentionally runs the same selected cases three times.

The full integration suite now belongs to required CI on Ubuntu and macOS.
Four core shards, the support lane and existing stress lane counts remain.
Each leaf has a five-minute hard timeout; measured execution should stay below
four minutes and thirty seconds. CI retains selection, per-test timing and wall
time artifacts. Hosted queue time must be reported separately.

### Final gate

Validation is still in progress. Replace this section with final outcomes before
handoff. Local artifacts are under `/tmp/acd-testing-baseline/local-gate`.

Focused evidence already recorded in Trekoon includes real blocked-provider and
verifier protection, semantic branch-switch/restart, ten repeated concurrency
runs (121.564 s), staging consent across restart, schema migration/read-only
compatibility, provider message/verification reuse, setup convergence, truthful
status, and restore preview. Full-suite failures must be resolved before these
focused results count as completed delivery.

## Delivery boundary

The installed user runtime was not replaced or restarted. No branch was pushed,
PR opened, tag created or release published. Hosted CI has not run for this
branch; its platform and wall-clock budgets remain an external verification
step after a separately authorized push. ACD owns all checkout commits.
