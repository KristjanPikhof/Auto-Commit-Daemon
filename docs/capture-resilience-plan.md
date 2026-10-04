# Capture and publication resilience

Branch: `fix/capture-publication-resilience`

## Incident

The AI-Assistant worker was alive but could not complete protection: four dirty
autocomplete dictionaries, between 14.8 and 21.7 MB, exceeded the 5 MiB capture
limit. Recovery checked publication repair state and reported it healthy. The
dashboard showed checkpointing without making the capture failure visible.
A temporary repository-only 32 MiB setting restored publication on the installed
runtime. Source changes do not update that runtime.

## Implementation

| Area | Change | Safety boundary |
|---|---|---|
| Capture | Stream regular files above the buffering threshold | Recheck file identity, size, mode, and modification time; preserve exact durable blobs |
| Partial coverage | Save readable files and record failed eligible paths | Keep the last complete checkpoint; reject partial full-tree restore and barriers |
| Publication | Let provably independent documentation proceed | Hold failed paths, source/configuration/assets, unknown scope, and uncertain rename/delete companions |
| AI input | Send binary filenames, operations, kind, and sizes | Never send binary contents; preserve text redaction and egress permissions |
| Provider failures | Persist one planning deadline and reuse safe local messages | Preserve valid groups, dependencies, frozen targets, materialization, verification, and repair rules |
| Diagnostics | Share capture health across all support views | Worker liveness and queue progress remain separate; empty queues can still have incomplete coverage |
| Recovery | Retry capture through the owning worker | Return failure if complete coverage cannot be proved; never bypass locks or overwrite live files |
| Retention | Keep captures referenced by publication drains | Preserve immutable membership and SQLite foreign keys |

The buffering setting retains its existing name and default. Schema v29 adds
coverage and provider-deadline data; earlier checkpoints remain complete.
Privacy exclusions stay outside the capture-failure ledger.

## Verification and delivery

- Recreate all four dictionary sizes with the default buffering setting.
- Run a real worker against an unavailable provider with binary assets, source,
  tests, documentation, and deliberate user staging.
- Prove readable capture and independent publication survive an unreadable file;
  repeated failures reuse checkpoints, and complete capture clears the failure.
- Cover complete-checkpoint barriers, restore refusal, restart-safe provider
  deadlines, bounded calls, capture-aware status/recovery, and completed-drain
  retention.
- Update README, command/settings/provider/workflow docs, generated references,
  and the changelog.
- Review the whole branch, simplify changed code, fix findings, and run focused
  race tests, process integration, lint, and the sharded local gate.
- Let ACD commit the changes. Installation, restart of the installed runtime,
  push, and release remain separate delivery actions.

## Result

The implementation is complete. The real-worker regression publishes all four
dictionary sizes during an AI outage, preserves exact file bytes and deliberate
user staging, and sends binary metadata without contents. AI-Assistant remains
protected, idle, clean, and without pending captures under its temporary setting.

The whole branch received a structured manual code review because the requested
code-review skill was unavailable. Review fixes cover schema upgrades, retention,
unknown capture scope, partial-snapshot retry reuse, preserved-group fallback,
FIFO swaps, and status overrides. The simplify pass removed the remote rewrite
path from local recovery and reused loaded capture operations. Documentation
received a humanizer pass.

Validation passed: `make lint`, the sharded `make test` gate, focused race tests,
and real-process outage, flush, planner rejection, and recovery integration tests.
Deadline, restart, partial capture, and fallback scenarios also passed ten
repetitions. `git diff --check` and the `AGENTS.md` symlink checks passed.

The installed runtime is still `v2026-09-27`. Installing or releasing these
source changes is a separate delivery step.
