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
| Initial checkpoint | 1.670 s |
| Checkpoint during blocked AI call | 1.129 s |
| Idle CPU time over a 1.011 s sample | 0.070 s |
| Idle resident memory | 35.1 MiB |
| Benchmark scenario in required integration shard | 11.40 s |
| Ten repeated hook calls, quiet run | 1.073 s total; 101.5–116.5 ms each |

Raw benchmark: `/tmp/acd-testing-baseline/final-current/test_integration-3.jsonl`
(`ACD_MEASUREMENT` record).
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
include every discovered test, example and fuzz target exactly once per run.
Go runs each selected fuzz target against its complete seed corpus.
Repeated stress intentionally runs the same selected cases three times.

The full integration suite now belongs to required CI on Ubuntu and macOS.
Four core shards, the support lane and existing stress lane counts remain.
Each leaf has a five-minute hard timeout; measured execution should stay below
four minutes and thirty seconds. CI retains selection, per-test timing and wall
time artifacts. Hosted queue time must be reported separately.

### Final results

| Check | Result |
|---|---|
| `make lint` | Passed, including generated configuration reference. |
| `make test` | Passed in **263 s**: 2,332 top-level cases passed; one expected macOS platform skip; zero failures or duplicate selections. |
| Local core lanes | Three shards per core package; CLI 165–172 s, daemon 233–240 s. Support and timing-sensitive lanes passed. |
| Production integration | All **115** scenarios passed across four complete shards; zero skips, including the production benchmark. |
| Integration shard wall times | 115 s, 136.110 s, 140.809 s, 113 s. |
| Test-selection tooling | Five Python regression tests passed, including compiler-error propagation and complete, nonoverlapping manifests. |
| Linux build | Current source cross-compiled for Linux/amd64 with shipping build tags and CGO disabled. |
| Documentation | Generated reference, local links, whitespace and the `AGENTS.md` symlink checks passed. |

The only local skip is
`TestEnsureSessionLeavesNonDarwinServiceLifecycleUnchanged`, which applies to
non-macOS platforms. This source currently contains zero examples, fuzz targets
or fuzz seed cases; the manifest tooling supports them when present.

Local evidence: `/tmp/acd-testing-baseline/balanced-local-gate`, including
`coverage.json` and `local-all.summary.json`. Production evidence combines
passing original shards 0 and 3 under `final-current` with corrected complete
shards 1 and 2 under `clean-production-shards`. Their preserved manifest covers
all 115 current targets once; three provider-test renames are mapped explicitly.
The raw results and explicit name mapping retain the evidence for each
scenario; the renamed provider cases also passed under their current names.

### Timing changes and resolved failures

The unsharded production baseline exceeded its five-minute limit. The new
integration shards finish in 113–141 seconds locally. The initially attempted
two-shard local layout later exceeded the daemon package limit: the entire gate
finished in 292 seconds with two timeout failures. No individual test was stuck;
the sequential cases exhausted the budget before the parallel tail ran.

The default local gate now uses three core shards and limits parallel cases to
two per package process. Its passing 263-second run keeps every scenario and
retains the original timeouts. CI keeps four core shards, six daemon stress
lanes and two Git/state stress lanes. The timing reference stores measured case
durations; it does not treat summed case time as proof of hosted wall time.

Earlier failures led to fixes for worker follow-up after evaluation, restore
help expectations, canonical-lock shutdown readiness, retained same-tree
checkpoint coverage and explicit persisted staging approval in a restart
fixture. AI-outage tests now prove durable protection and unchanged branch
history while waiting for AI. External-commit fixtures assert the new recovery
outcome, exact preserved bytes and absence of duplicate commits.

Focused evidence in Trekoon also covers blocked-provider and verifier
protection, semantic branch-switch/restart, repeated concurrency runs
(121.564 s and 120.943 s), staging consent across restart, schema migration and
read-only compatibility, exact message/check reuse, setup convergence, truthful
status, and restore preview. Final review fixes cover every queued rename and
operation path in previews, and keep legacy semantic rejections out of outage
backoff. Both focused regressions and the final broad gate passed.

Independent review found no remaining source or documentation blocker. Removed
scenarios and their current replacements remain in the coverage mapping.

## Dashboard follow-up after local upgrade

The live upgrade exposed two list problems. Internal setup/readiness checkpoint
barriers renewed the same activity timestamp as user hooks. Optional publication
proofs also repeated a Git lookup and could time out before the dashboard read
activity or unfinished work.

The follow-up excludes internal barriers from activity, retains real hooks and
explicit publication, reuses the branch lookup, and collects durable visibility
facts before optional details. Targeted CLI race coverage, the production
`TestActiveDashboardWorksOutsideRepositories` scenario and lint passed after
these changes. The broad 263-second gate above precedes this focused follow-up.

Existing unlabelled activity timestamps remain intact: they cannot reliably be
separated from real hooks after the fact. Entries introduced by the earlier
upgrade age out under the unchanged one-hour rule. No live activity database
was edited to invent missing provenance.

## Delivery boundary

The user subsequently authorized a local rebuild and runtime upgrade. Build
`v2026-09-15-256-g730726c8` was installed through the reviewed compatible setup
plan. Setup checkpointed enabled repositories, restarted the managed supervisor
and completed readiness checks. The source binary, managed runtime and local
CLI have identical SHA-256 digests. Existing settings and repository opt-ins
were retained.

No branch was pushed, PR opened, tag created or release published. Hosted CI
has not run for this branch. Linux runtime behavior, hosted leaf-job budgets and complete workflow
execution time remain unverified until a separately authorized push. Hosted
queue delay must be reported separately; the local results do not establish
those hosted budgets. ACD owns all checkout commits.
