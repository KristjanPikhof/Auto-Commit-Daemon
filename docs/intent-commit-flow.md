# Intent commit flow

Intent v2 turns durable captures into semantic commit candidates, validates
each candidate as an atomic change, and publishes candidates in dependency
order. Capture does not depend on the planner. If planning or verification
cannot continue safely, captures remain durable and replay reports
`needs_attention`.

Use Event mode when each capture should become a commit. Use Intent when a
reviewable implementation, its tests, and required supporting changes should
land together even when unrelated edits occur between them.

## Configure Intent mode

Start with global guided setup:

~~~bash
acd configure --strategy intent --preset balanced
~~~

Everyday work is the regular default and maps to Intent Balanced. It requires
a tested provider and redacted diff context, then uses built-in structural and
materialization verification. Global setup never detects or runs a project
test. The wizard shows the exact endpoint, diff-egress policy, and repair
limits before one approval.

The provider test runs synchronously before any write. Strict Review is
available only through `acd configure --repo .`; it queues the repository's
approved full command as a durable background activation gate. Capture
continues while replay and repair wait. Use
`acd configure --repo . --wait` to follow that gate.

| Preset | Evaluation | Verification | Planner failure | Recent repair |
|---|---|---|---|---|
| Fast | 10 captures, 10-second quiet time, 90-second max wait | None | Smallest valid hard-dependency component | Off |
| Balanced | 20 captures, 30-second quiet time, 3-minute max wait | Structural and materialization gates | Last valid or deterministic dependency partition, then structural verification | Up to 3 commits or 10 minutes |
| Quality | 30 captures, 60-second quiet time, 10-minute max wait | Approved full command, 10-minute timeout | Keep pending and report `needs_attention` | Up to 5 commits or 30 minutes |

All regular Intent presets need redacted diff context. Network providers also
need explicit diff-egress consent. Metadata-only Intent v2 is unsupported in
the regular presets.

## Build durable candidates

The planner evaluates a bounded visible capture set, but candidates persist
across evaluations. Each candidate belongs to one exact branch and generation
and records:

- a bounded purpose and readiness state;
- ordered active and superseded capture membership;
- missing companions and the latest atomicity result;
- planner protocol and immutable config metadata;
- verification and soft-publication state;
- the published commit OID, when publication succeeds.

When a hard dependency joins persisted candidates, ACD keeps the oldest
candidate as the canonical target and records the other candidate IDs, status,
and published commit OIDs in the lineage ledger. Membership moves in one
transaction. Nested lineage remains traversable for later repair.

During Balanced fallback, an exact one-to-one `test_source` or
`migration_test` edge can also reconnect new work to a persisted candidate
across planner windows. That continuation enters the same bounded repair path.
Multiple possible persisted companions stay pending for planner review instead
of publishing a deterministic singleton.

Candidate tables store paths, change classes, bounded explanations, and hashes
of symbol evidence. They do not store raw diffs. A planner request reconstructs
bounded, redacted diff context from immutable capture operations.

Each exact branch pair is capped at 128 open candidates, 4,096 dependency
edges, and 256 captures per candidate. Purpose text is capped at 512
characters. Missing-companion and atomicity summaries are capped at 2,048
characters.

## Order work with dependencies

Hard edges prevent unsafe reordering:

| Evidence | Required order |
|---|---|
| Same path | Earlier capture before later capture |
| Rename source and destination | Source state before destination state |
| Captured before/after objects | Producer before consumer |
| Create, modify, and delete | Object lifecycle order |
| Known generated source and output | Source before generated output |

Soft edges help the planner recognize likely companions. Evidence includes
package proximity, test/source naming, symbol and hunk hashes,
import/reference hints, change role, activity epoch, and temporal proximity.
Time is weak evidence and cannot override a hard edge. During Balanced
fallback, import/reference, symbol, hunk, module, activity, and time evidence
cannot merge otherwise independent components by themselves.

Candidate-level topological validation allows non-contiguous groups. For
example, independent captures `A1, B1, A2` can publish as `A=[1,3]` and
`B=[2]`. Same-path or object-dependent captures remain ordered. Intent v2 does
not split one capture into synthetic hunks. Two same-file captures separate
only when scratch materialization proves the selected sequence can apply
independently.

## Plan and correct candidates

The v2 planner request includes visible captures, persisted candidate
summaries, dependency edges, activity boundaries, recent soft commits, and
prior atomicity findings. The response assigns every visible capture exactly
once and returns a candidate ID, selected sequences, purpose, readiness,
missing companions, candidate dependencies, commit message, and grouping
reason.

ACD accepts legacy subprocess v1 responses through a compatibility adapter and
reports `planner_protocol=v1_compat`. The adapter retains structural safety,
but its result cannot claim native v2 readiness quality. Native built-in and
capable subprocess providers report protocol v2.

Invalid plans receive a correction request with bounded, exact findings.
Message-only failures use the locked rewrite path. Planner rationale stays in
`grouping_reason`; it is not copied into the commit body.

## Pass the atomicity gate

Every ready candidate must pass all applicable checks:

| Gate | Requirement |
|---|---|
| Cohesion | One reviewable purpose |
| Completeness | No required companion is still missing |
| Separation | No unrelated semantic component lacks dependency evidence |
| Dependency | No deferred or later unpublished prerequisite |
| Materialization | Candidate tree and topological publish order build in scratch indexes |
| Verification | The preset-required approved command passes on the exact tree |
| Revertibility | No unrelated dependency component would be reverted with it |

No preset bypasses hard dependency or materialization failures. A failed or
timed-out verification leaves the candidate pending. It never forces
publication.

## Seal candidates at useful boundaries

ACD evaluates candidates when any of these occurs:

- the preset quiet period ends;
- the visible evaluation reaches its capture limit;
- the oldest capture reaches the preset maximum age;
- a harness records a soft boundary;
- an active session completes `acd flush --logical`.

Codex Stop records `acd touch --soft-boundary`. Claude Code, Cursor, OpenCode,
and Pi retain their logical flush boundaries. A soft or hard boundary triggers
evaluation, but it cannot bypass atomicity, verification, pause, Git operation,
branch generation, conflict, or other replay safety checks.

Plain `acd wake` only nudges the daemon. It does not create an activity boundary.

## Repair a recent private commit

Balanced and Quality can repair an eligible recent ACD commit when a late
capture belongs to a soft-published candidate. Every condition must hold:

- HEAD is attached to the unchanged branch generation;
- HEAD matches the recorded expected OID;
- the repair range is a contiguous first-parent chain at HEAD;
- every rewritten commit is ACD-owned for the candidate;
- no merge, tag, other local branch, or remote-tracking ref contains a
  rewritten commit;
- no Git operation or manual pause is active;
- no staged path overlaps the repair set;
- the range fits the preset horizon and commit cap;
- rebuilt candidates pass dependency, materialization, atomicity, and
  verification gates.

ACD records a prepared transaction, rebuilds the chain with scratch indexes and
`commit-tree`, creates an `refs/acd/intent-repair/.../backup` ref, and
CAS-updates the literal branch ref. It then reconciles events, decisions,
candidates, and old-to-new commit mappings before reseeding the exact shadow
pair. The live index and worktree must remain unchanged.

The old repair range is still one contiguous first-parent suffix. Candidate
ownership inside that range may be non-contiguous. For example, a rebuilt
candidate can replace `A1` and `A2` while a second rebuilt candidate replaces
the independent `B1` between them. Every old commit maps to exactly one rebuilt
candidate before the ref update. Every reordered candidate must also have been
visible in the current dependency evaluation, have no hard edge or path overlap
with the merged candidate, and pass the approved check as an exact rebuilt
commit before the ref update. Otherwise ACD skips automatic repartitioning.

On restart, ACD completes database reconciliation for a Git-applied repair or
keeps the backup ref and reports recoverable guidance. Completed repair backups
are retained for seven days and capped at the newest 50 per repository.

If any eligibility check fails, ACD records the skip and creates a new
candidate. It does not rewrite shared or uncertain history.

## Understand planner fallback

The planner circuit opens immediately on transport failure and after three
consecutive validation or safety failures. Cooldowns are 30 seconds, 2 minutes,
and 10 minutes. One half-open probe runs while other evaluations follow the
preset fallback.

| Preset | Circuit or provider failure |
|---|---|
| Fast | Materialize and publish the smallest hard-dependency component |
| Balanced | Reuse the last valid partition or bounded dependency components, then run structural verification |
| Quality | Keep candidates pending and report `needs_attention` |

Balanced fallback may attach a unique test/source or migration companion.
Known generated-output relationships remain hard dependencies rather than soft
guesses. Ambiguous components stay pending. A fallback group also stays pending
when it exceeds 32 captures or 12 paths.

Circuit bypasses are observability events, not new planner errors. Caller
cancellation releases a half-open permit without changing circuit health.

## Inspect Intent decisions

~~~bash
acd status
acd diagnose
acd doctor
acd events --watch
acd explain --path path/to/file
acd status --json
~~~

Human output reports migration state, preset and version, customization,
verification mode, repair policy, candidate counts, verification attention,
and recoverable repairs. Repeated replay failures also show the bounded error,
repeat count, blocked sequence, candidate IDs, and latest fallback size. The
latest candidate includes planner protocol, atomicity, and verification status.
JSON output adds the same bounded details.

Useful traces include:

| Trace | Meaning |
|---|---|
| `intent.batch_wait` | The preset evaluation boundary has not arrived |
| `intent.planner.input` | A bounded candidate request was built |
| `intent.planner.output` | A structurally valid plan returned |
| `intent.planner.validation_failed` | The plan failed typed validation |

Verification and repair state appear in the candidate and repair projections
returned by status, diagnose, doctor, events, and explain.

Planner windows and decisions persist privacy-safe summaries, not raw diffs,
credentials, prompts, or verification source content. Rejected planner output
goes to `<gitDir>/acd/planner-rejects.jsonl`; raw rejected responses are
redacted unless the advanced sensitive override
`ACD_INTENT_REJECTS_RAW=1` is set.

## Recover from blocked v2 replay

Existing Intent repositories migrate to the current `intent.balanced@3`.
Previously materialized revisions carry their explicit overrides forward. If
the effective provider, diff consent, credential, or structural gate is
missing, capture continues and replay stops with `needs_attention`.

Run:

~~~bash
acd configure
acd configure --repo .
acd status
~~~

ACD never silently resumes v1 or metadata-only planning. Read
[Intent v2 migration](intent-v2-migration.md) for upgrade rules and
[user workflows](user-workflows.md) for repair and recovery commands.
