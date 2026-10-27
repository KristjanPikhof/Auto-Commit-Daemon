# Explicit finish removal map

This is the baseline inventory and completed removal record for Trekoon epic
`8a174f3b-5222-4740-9965-8376aaabbd1c`, measured at source commit
`84ed324a6866ec4294319986d302160a774ea9c1`. Tables marked baseline preserve the original paths and planned dispositions;
completed sections record the source implementation and scenario replacements.
This document does not describe the installed runtime. The runtime was active when execution began; this inventory
does not authorize installation, restart, live migration, or history changes.

Ordinary edits must make private checkpoints only. Branch commits, provider calls,
and project verification belong to explicit `acd finish` or history requests.
The new engine uses a frozen complete checkpoint and builds the entire commit
chain before one branch compare-and-swap (CAS). Later edits stay protected and
outside that request. Local finish creates one honest batch commit without AI;
Intent finish uses bounded evidence reviews for large batches, then one global
planning call and at most one correction. Failed requests
remain retryable on request, with no background semantic retry or mode fallback.

The source checkpoint contains eligible protected scope. The finish target is a
separate pinned tree: merge that scope onto the base while preserving excluded
tracked entries and Git submodule entries (gitlinks), then apply any approved
staged overlay. An omitted excluded entry is not a deletion. The complete chain
must end at this retained target, which can differ from the checkpoint even
without `--include-staged`.

## Reading the inventory

| Action | Meaning |
|---|---|
| Keep | Preserve the primitive and its safety tests. Remove obsolete callers where listed. |
| Replace | Move the retained behavior to the finish contract, then remove the old interface. |
| Delete | Remove the retired behavior after its consumers migrate. Do not remove retained scenarios from mixed test files. |
| Legacy | Preserve immutable records and the minimum readers/proof code required by migration, history, or restore. No scheduling, provider calls, or new old-format publication. Activation may settle an already prepared mutation only through exact proof. |

Owners below are the lane names in Trekoon. `removal-daemon-lane`,
`removal-state-lane`, and `removal-surface-lane` close obsolete callers after
replacement owners land. `test-gate-lane` removes retired assertions and stale
selectors only after the replacement scenarios exist. `migration-lane` owns
legacy proof reconciliation; `retention-lane` owns reachability and pruning.
The exhaustive inventory lists every baseline production and test file by disposition.
Deleted paths in those tables are historical references, not current interfaces.

## Baseline production paths and callers

| Baseline path / entry points | Baseline callers | Action and retained boundary | Owner |
|---|---|---|---|
| `daemon.Run` in `internal/daemon/daemon.go` | Worker startup and supervisor worker server | Replace the automatic replay portion. Keep canonical ownership, wake coalescing, checkpoint scans, backoff, heartbeat, shutdown, maintenance, and explicit request handling. No edit, idle, queue age, or shutdown publication. | `capture-lane`, `worker-lane`; final deletion `removal-daemon-lane` |
| `Capture`, `ProtectWorktree`, `BeginProtectionObservation` in `capture.go`; `classify.go`, `shadow_io.go` | Run loop; restore protection policy; legacy recovery | Use `ProtectWorktree` for normal checkpoint protection. Retain safe traversal, streamed blobs, exclusions, failed-path reporting, observation epochs, and stable reads. Retire normal capture-event/shadow classification, path-quiescence tracking and publication backpressure. Preserve recorded versions needed by restore/legacy readers. | `capture-lane`; cleanup `removal-daemon-lane` |
| `Replay` and `ReplayOpts` in `replay.go`; `replayIntentCandidateBatch` in `intent_replay_v2.go` | `Run`, commit-all plumbing, direct tests | Delete Event-per-save and continuous Intent replay. Extract bounded Git/materialization safeguards used by finish before removal. `commit-all` must delegate to finish, never keep Replay behind the alias. | `cli-lane`, `git-lane`; deletion `removal-daemon-lane` |
| `EvaluateIntentCandidates`, `BuildIntentCandidateDependencies` in `intent_candidates.go` | Intent replay, history audit/reconstruction, recorded evidence helpers | Replace durable waiting candidates with frozen whole-batch goal plans. Keep dependency closure, valid intermediate trees, completeness, exact materialization and revert boundaries in the shared plan model. Delete candidate continuation, soft publication, aging, successor IDs and local-unlock policy. | `plan-lane`, `ai-lane`, `history-lane`; deletion `removal-daemon-lane` |
| `intent_frozen_window.go`, `intent_goal_window.go`, `intent_unclassified_candidates.go`, `intent_clarifications.go` | Replay planning and cached window continuation | Delete bounded event-window growth, held-prefix waiting and repeated clarification/reoffer loops. Finish receives the frozen complete tree once; oversized evidence must be reported honestly and stay within its request deadline. | `removal-daemon-lane` |
| `publication_drain.go`: `ResumePublicationDrainCheckpointing`, `ResumePublicationDrainNormalization`, `UpdatePublicationDrainAfterReplay`, `Recover*PublicationDrain` | Run loop and old commit-all | Delete active drain escalation, normalization, local fallback and provider-wait recovery. Legacy drains become inert evidence and a finish-needed outcome. Preserve their target snapshot and staged-consent provenance. | `migration-lane`; deletion `removal-daemon-lane` |
| `publication_plan_recovery.go`, `intent_semantic_retry.go`, `intent_retry_schedule.go`, `intent_health.go` | Replay provider gates, drain retry, status circuits | Delete recovery planning, cooldown/half-open probe scheduling, retry cadence, no-progress expansion and implicit mode rescue. Provider error classification/redaction may remain in transport. A finish request has one persisted deadline and a terminal result. | `worker-lane`, `ai-lane`; deletion `removal-daemon-lane` |
| `publication_evaluation.go` and `runtime_verification.go` | Run loop slow provider/check calls and history reconstruction | Replace evaluation orchestration with explicit bounded requests that release the gate for protection, then recheck the frozen identity. Keep isolated verification and cache only identical tree, command, environment identity and policy. Do not reuse candidate ID alone as proof. | `worker-lane`, `plan-lane` |
| `publication_message_cache.go`, `message.go` | Per-event Replay message generation/recovery | Replace with plan-owned messages. No separate message provider calls or unbounded correction loops. Keep format sanitization and honest local summaries. Historical cached messages remain readable if referenced. | `ai-lane`, `local-lane`; deletion `removal-daemon-lane` |
| `intent_history_audit.go`: `MaybeRepairIntentHistory`; auto paths in `intent_repair.go` | `Run` after replay, soft-published candidates | Delete automatic audit/rewrite and repair horizon selection. Do not retain them behind a default-disabled knob. Exact recovery of a prepared legacy repair is a separate migration responsibility. | `history-lane`, `migration-lane`; deletion `removal-daemon-lane` |
| `intent_history.go`: `PlanIntentHistory`, `ProcessIntentHistoryRequest`; `intent_history_correction.go` | `acd history rewrite`, explicit worker requests | Keep explicit selection, plan/preview/apply, preserved source history and author boundaries. Replace dependencies on waiting candidates, recent soft commits and background repair. Local rewrite must use existing recorded order/grouping honestly. | `history-lane` |
| `intent_goal_evidence.go`, `intent_evidence_review.go`, `intent_source_relationships.go`, `intent_relationship_evidence.go`, `intent_recorded_*.go`, `intent_published_*.go`, `intent_cobra_*.go`, `intent_swift_maintenance.go`, `intent_file_metadata.go`, `intent_frozen_published_context.go` | Candidate evaluation and explicit rewrite | Replace event/candidate-shaped adapters, preserve grounded recorded implementation/caller/test/import/generated/documentation relationships and exact available versions. Keep purpose-first examples; no time/path proximity grouping. Remove auto-repair/published-window context once shared plan/rewrite consumers migrate. | `plan-lane`, `ai-lane`, `history-lane`; final orphan deletion `removal-daemon-lane` |
| `self_publication.go`, `self_publication_recovery.go`: `prepareSelfPublication`, `applyCAS`, `RecoverSelfPublications*` | Replay, Run startup, migration | Replace new publication with one journaled final CAS for the complete chain. Keep legacy source/parent/tree/member/ref/index proof and exact before/after-CAS settlement. Remove candidate retirement writers once cutover is proved. | `git-lane`, `migration-lane` |
| `external_repair_bridge.go`, `reconcile.go`, `branch_token.go`, `live_index_repair.go` | Branch resampling, legacy repair adoption, support recovery | Keep conservative branch identity and exact proofs. Narrow legacy bridge/index repair to owned recorded mutations. Delete guessing, automatic historical reassignment and unrelated live-index cleanup. Finish must refuse ambiguous external movement and preserve later staging. | `migration-lane`, `git-lane`; cleanup `removal-daemon-lane` |
| `capture_publication.go`, `coalesce.go`, `prune.go`, `refcount.go`, `dead_branch_sweep.go`, `rollup.go` | Event eligibility, Replay and maintenance | Delete event offer paging and publication coalescing. Replace event-queue resource/retention rules with checkpoint/request roots; retain historical statistics and legacy referenced objects. Do not prune an unresolved or retained target. | `retention-lane`; deletion `removal-daemon-lane` |
| `runtime_config.go`: bundle builder/manager, `ActivatePublicationDrainRevision`; `config_validation.go`, `runtime_migration.go` | Run startup, configuration activation | Keep immutable settings identity, credentials/privacy and operational activation. Delete provider construction/probing for ordinary protection, drain-runtime selection, planner readiness and experiment completion. Read-only commands must never build providers or migrate. | `config-lane`, `onboarding-lane`, `worker-lane`; cleanup `removal-surface-lane` |
| `root.go`, `product_commit_all.go`, `commit_all_preview.go`, `commit_all_staging_recovery.go`, `internal_protocol.go` | Root command, supervisor/worker IPC, old drain requests | Add canonical finish, optional context, preview, explicit frozen retry and staged consent. Keep `commit-all` as a first-release-only deprecated delegate. Replace drain IPC; identical active requests reconnect, disconnect detaches from the bounded attempt. | `cli-lane`, `worker-lane` |
| `control.go`, `repo_activation.go`, `setup_onboarding.go`, `setup.go`, `start*.go`, `stop.go`, `facade_mutations.go` | on/off/setup and internal lifecycle | On creates missing initial protection without credentials; off saves a final checkpoint without branch publication. Setup remains the explicit advanced upgrade entry. Remove publication waits, draining shutdown and planner setup prerequisites. Preserve incompatible runtime refusal and repository consent. | `onboarding-lane`, `capture-lane` |
| `status.go`, `product_list*.go`, `list.go`, `publication_outcome.go`, `doctor.go`, `diagnose.go`, `worker_activity_hints.go`, `intent_observability.go`, `replay_observability.go` | status/list/support, JSON projections | Replace queue/candidate/circuit/drain dashboards with saved coverage and explicit request state. Keep liveness separate from progress, partial coverage and maintenance diagnostics. Legacy publication evidence stays truthful: recovered is not branch committed. Retired JSON fields need deliberate compatibility handling, never fabricated activity. | `status-lane`; deletion `removal-surface-lane` |
| `fix.go`, `fix_runtime.go`, `global_repair.go`, `recovery_probe.go`, `recover.go`, `purge.go` | Explicit support/recovery | Keep checkpoint/restore/ownership proof and dry-run/apply boundaries. Remove recovery that starts semantic replay, rebuilds automatic plans, purges protected work or silently repairs history. | `migration-lane`, `onboarding-lane`; cleanup `removal-surface-lane` |
| `intent_planner.go`, `intent_protocol_v2.go`, `intent_wire_v2.go`, `intent_unassigned.go`, `intent_maintenance.go`, `intent_cli_reference.go` | AI transports, daemon planning and history rewrite | Replace candidate/wait/window protocol with shared goal/tree plans. Keep complete implementation/caller/companion and purpose rules. Migrate explicit rewrite callers before deleting old schemas/prompts. | `ai-lane`, `history-lane`; deletion `removal-surface-lane` |
| `intent_rejects.go`, `provider_probe.go`, `prompt.go`, `subject_fallback.go`, `rewrite_proposals.go`, `rewrite_gate.go` | AI diagnostics, setup and explicit rewrite | Keep privacy-safe rejection diagnostics, explicit connection tests, format validation and rewrite safeguards where used. Delete background retries/probes, per-event prompts and filename-message fallback. Do not turn planner failure into local mode. | `ai-lane`, `local-lane`, `history-lane`; cleanup `removal-surface-lane` |
| `settings/experiment.go`, `comparison.go`, config presets and settings UI | Configure/edit/experiment and planner-window counters | Delete candidate experiments, budget windows, automatic revert and quality-dashboard comparisons. Keep provider credentials, precedence, approved verification and simple mode/style editing. | `config-lane`; deletion `removal-surface-lane` |
| `integration_event.go`, `touch.go`, `wake.go`, templates and adapters | Tool/session/idle/stop hooks | Keep canonical worker ownership and protection hints. Remove session refcounts and logical-boundary publication; idle/stop is not task completion. Guidance permits explicit finish only for authorized completed work. | `hooks-lane`, `capture-lane`; aliases `removal-surface-lane` |
| Supervisor `protocol.go`, `worker_server.go`, `worker_status.go`; installer revalidation and migration bridge | Worker dispatch, setup/upgrade transactions | Replace drain/request and queue fields; retain peer-UID checks, allowlist, exact binary/protocol identity, canonical repository locks and protection bridge. No automatic runtime upgrade is authorized. | `worker-lane`, `status-lane`, `onboarding-lane`, `migration-lane` |

## Safety dependencies to retain

| Primitive | Exact source | Required proof / scenario | Owner |
|---|---|---|---|
| Checkpoint durability and restart | `checkpoint.Store.Create`, `Store.RecoverPrepared`; `state.PrepareCheckpoint`; `git/durability.go`, `tree.go`, `object_inventory.go` | Objects reread durably, prepared SQLite identity, create-only private-ref CAS and exact completion. Preserve complete/partial coverage and streamed large files. | `capture-lane`, `retention-lane` |
| Canonical ownership | `daemon.AcquireDaemonLock`, `AcquireControlLock`; `globalops/lock.go`; supervisor peer credentials and allowlist | PID/heartbeat are supporting evidence only. Retain mixed-version fencing and linked-worktree contention tests. Never bypass locks or delete state. | `worker-lane`, `onboarding-lane` |
| Literal-ref publication | `git.UpdateRef`, `WithLockedExpectedRef`, `CreateRefsCAS`; `ProvePublicationAtHEAD`; `state/completed_branch_transition.go` | Expected source, exact complete chain/tree and final literal ref move. Preserve detached/conflict/foreign movement refusal and ambiguous crash evidence. | `git-lane`, `migration-lane` |
| Staged consent | `git.IndexContentDigest`, `git/finish_publish.go`, `git/index_consent.go`; `state/finish_publication.go`; archived self-publication frozen index fields | Refuse nonempty staging by default. `--include-staged` freezes exact index identity and preserves staged-only bytes. Source checkpoint remains its own immutable eligible-scope snapshot; merge with preserved base exclusions/gitlinks, then apply the approved staged overlay to pin the finish target. Final chain equals that target, not a later index/worktree. Consume only consented staging; never clear staging added later. | `git-lane`, `cli-lane`, `worker-lane` |
| Restore | `restore.PreviewWithPolicy`, `Apply`, `RepairWithPolicy`; safe filesystem implementations; `state/operations.go`, checkpoint/restore relations | Preview, preimage checkpoints, safe path claims, atomic replacements, unchanged HEAD/index and forward proof. Keep restore failures and preimage roots. | `migration-lane`, `retention-lane` |
| Explicit history rewrite | CLI `finish_history_protocol.go`, `rewrite_commits.go`; `ai/finish_history.go`; `git/finish_history.go`; `state/finish_history.go` and read-only legacy rewrite plans | Explicit selection/preview/apply, author identity, recorded versions, exact final tree, source-history refs and stale-plan refusal. Old plan inspection never starts mutation. | `history-lane` |
| Privacy and transport | `ai/openai_compat.go`, `plugin_subprocess.go`, `provider.go`, `redact.go`, `error_sanitize.go`, `config.go`; `credentials/*`; `prompttrace/*`; safe ignore/sensitive matchers | Consent for diff egress, credential redaction and owner-only files; bounded cancellation/output for subprocess/HTTP. Retain opt-in raw traces only where documented. | `ai-lane`, `config-lane` |
| Exact verification | `verification.NewApprovedCommand`, `Runner.Run`, `CheckStructural`, stale workspace cleanup; daemon runtime verifier | Verify each proposed commit tree in isolation. Structural checks are not a claim that project tests passed. Cache includes tree, command, environment identity and policy; report reuse honestly. Preserve cancellation and descendant process cleanup. | `plan-lane`, `worker-lane` |
| Retention and compatibility | `checkpoint/retention.go`, `maintenance.go`; state checkpoint retention/recovery; migrations, readonly DB open, backup | Retain all unfinished targets, newest complete checkpoint, restore preimages and immutable legacy provenance; keep moved refs untouched. Disabled repositories and readonly commands do not migrate. | `migration-lane`, `retention-lane` |

## Settings and environment closure

`internal/config/fields.go` is the public catalog; presets, `ResolveAll`, settings
service/UI, setup readiness, runtime bundle, fingerprints and generated reference
consume it. `config-lane` changes the catalog and generated reference.
`removal-surface-lane` removes remaining old UI/parser/runtime branches after
replacement. Legacy parsing belongs only to the activation translation boundary.

| Setting / environment | Disposition |
|---|---|
| `commit.strategy` / `ACD_COMMIT_STRATEGY` | Replace the choices `event` and `intent` with `local` and `intent`. Translate explicit event to local; preserve explicit Intent. New default local. |
| `commit.format` / `ACD_COMMIT_FORMAT` | Keep imperative and conventional, default imperative; preserve authored choice. |
| `commit.preset` / `ACD_COMMIT_PRESET` | Delete fast/balanced/quality, preset metadata/default layers, provider-test/hard-boundary/fallback preset promises. Translate retained verification/privacy choices explicitly before dropping presets. |
| `intent.window`, `intent.min_pending`, `intent.settle_window`, `intent.max_pending_age` | Delete with `ACD_INTENT_WINDOW`, `ACD_INTENT_MIN_PENDING`, `ACD_INTENT_SETTLE_WINDOW`, `ACD_INTENT_MAX_PENDING_AGE`. Whole-batch finish has no age/count/idle publication gate. |
| `intent.recent_commits`, `intent.defer_limit`, `intent.retry_on_invalid`, `intent.path_coalescing` | Delete `ACD_INTENT_RECENT_COMMITS`, `ACD_INTENT_DEFER_LIMIT`, `ACD_INTENT_RETRY_ON_INVALID`, `ACD_INTENT_PATH_COALESCE`. Replace user retry tuning with one fixed request budget; explicit rewrite selects its own history. |
| `intent.repair.enabled`, `intent.repair.horizon`, `intent.repair.max_commits` | Delete `ACD_INTENT_REPAIR_ENABLED`, `ACD_INTENT_REPAIR_HORIZON`, `ACD_INTENT_REPAIR_MAX_COMMITS` and all auto-rewrite eligibility/progress exposure. |
| `capture.max_pending_events` / `ACD_MAX_PENDING_EVENTS` | Delete publication queue backpressure; checkpoint protection remains bounded through scans/objects/retention. |
| `ACD_PATH_QUIESCENCE_SECONDS`, `ACD_RECENT_COMMIT_AFFINITY_SECONDS` | Delete uncatalogued replay-offer knobs, trackers and repeated per-path log hints (`capture-lane`, then `removal-daemon-lane`). |
| `intent.verification` / `ACD_INTENT_VERIFICATION`; `verification.fast.command`, `.timeout`; `verification.full.command`, `.timeout` | Keep approved full/fast verification distinct from none/structural reporting. Migrate naming only with explicit documented translation; finish freezes policy/command identity. |
| `ai.provider`, `.base_url`, `.api_key`, `.model`, `.timeout`, `.ca_file`, `.diff_egress` and catalogued env counterparts | Keep transport/credentials/privacy/timeout. Protection does not construct or test a provider; Intent failure never selects deterministic mode automatically. |
| `capture.max_file_bytes`, `.sensitive_globs`, `.safe_ignore`, `.safe_ignore_extra`; `watch.fsnotify`; `trace.enabled`, `.prompt` | Keep buffering, eligible scope, watcher and opt-in tracing. Large file size is a buffer threshold, not permission to discard bytes. |
| `retention.event_days`, `recovery.rewind_grace`, `recovery.shadow_generations` | Replace event/shadow runtime policy with checkpoint/legacy retention. Keep old values only where required by historical cutover; never prune unresolved provenance (`retention-lane`). |
| `client.ttl` / `ACD_CLIENT_TTL_SECONDS` | Retire session-count lifetime and archive authored values. Enabled workers protect continuously until off or shutdown. |
| `ACD_REPO_AUTODISCOVERY` | Keep repository registration consent; no missing-repository provider setup or global migration. |
| `ACD_INTENT_REJECTS_RAW` | Preserve only as an explicit privacy-sensitive reject-log opt-in if the retained finish diagnostic uses it. Remove candidate-schema assumptions and warn/document raw retention (`ai-lane`, `docs-lane`). |
| Experiment/profile/preset source precedence and experiment window/expiry/revert controls | Delete experiment and preset sources. Preserve authored repository/global/environment precedence and credentials; retain profiles only if still used by the simplified configuration contract. |

## State writers and legacy records

Baseline schema is v30. Remove runtime writers, not captured bytes or immutable
tables needed by existing history. `state-lane` adds finish request/target state;
`migration-lane` makes old active plans inert after exact proof reconciliation;
`removal-state-lane` deletes dead writer APIs. Migration fixtures own manifests
for prepared/applied journals, partial/captured-only work and staged provenance.

| Existing tables / writers | Destination and owner |
|---|---|
| `planner_state`: `RecordPlannerOffer`, `RecordPlannerDefer`, `RecordPlannerError`, `ClearPlannerState`, `OldestOverduePlannerEvent` in `events.go` | Retire writers and automatic defer/aging queries. Legacy inspection only if needed. `removal-state-lane`. |
| `intent_planner_windows`, `intent_planner_window_events`; `intent_plan_runs`: `EnsureIntentPlanRun`, `ReserveIntentPlanAttempt`, `UpdateIntentPlanRun`, `StartIntentProviderBudget` | Retire window/cached adaptive run writers; use explicit persisted finish deadline/attempt state. Preserve old summaries as historical evidence. `state-lane`, `removal-state-lane`. |
| `intent_candidates`, `intent_candidate_events`, `intent_candidate_lineage`: `SaveIntentCandidate`, `SaveIntentRepairCandidates`, `MergeIntentCandidates`, `FinalizeExpiredIntentCandidates` | Retire new candidate writes, merges and age-based finalization. Retain immutable historical membership/lineage and exact legacy decoders. `migration-lane`, `removal-state-lane`. |
| `intent_capture_dependencies`; `intent_activity_boundaries`: `ReplaceIntentCaptureDependencies`, `AppendIntentActivityBoundary`, `ConsumeIntentActivityBoundaries` | Replace runtime dependencies with shared frozen plan evidence; remove logical/soft-boundary candidate writers. Keep old provenance if referenced. `plan-lane`, `hooks-lane`, `removal-state-lane`. |
| `intent_repairs`, `intent_repair_commits`, `intent_repair_commit_lineage`, `intent_repair_members`, `intent_repair_member_seals` | No new automatic repairs. Preserve immutable author/commit/membership seals and prepared/applied proof for lossless activation and explicit history readers. New explicit rewrite uses its owned plan/journal contract. `history-lane`, `migration-lane`, `removal-state-lane`. |
| `publication_drains`, `publication_drain_events`: create/advance/resume/fallback functions in `state/publication_drain.go` | Make existing targets inert, preserved and finish-needed; no restart drain. Keep approved index identity and referenced snapshots. `migration-lane`, `removal-state-lane`. |
| Forward-recovery metadata in `intent_forward_recovery.go`: `StartFailedIntentCheckpointRecovery`, `ForwardRecoverIntentCandidate`, `ExpandIntentForwardRecoveryTarget`, `AdvanceIntentForwardRecovery*`, `ResetIntentForwardRecovery*`, `CompleteIntentForwardRecovery` | Retire semantic recovery writers and retries. Preserve snapshot refs, target membership and typed historical reasons; no active recovery prefix used as a new plan. `migration-lane`, `removal-state-lane`. |
| `flush_requests` and `state/flush.go`; `checkpoint_intent_reconcile.go`, `intent_goal_wait.go`, `intent_published_companions.go` | Replace explicit drain requests with finish requests; retire candidate membership repair/held-goal/publication companion writers. Cutover must not label unpublished work committed. `state-lane`, `migration-lane`, `removal-state-lane`. |
| `config_experiments`; experiment activation/revert and planner-window budget writers in `runtime_config.go` | Retire experiment creation/ticking/reversion and experiment comparisons. Preserve immutable config revisions and exact current applied choice. `config-lane`, `removal-state-lane`. |
| `config_validation_runs`, `config_activation_requests`, `runtime_config_state`, `config_revisions` | Replace background provider-readiness validation with explicit configuration/finish behavior; preserve immutable config identity and approved commands/consent. Read-only inspection writes nothing. `config-lane`, `onboarding-lane`, `removal-state-lane`. |
| `capture_events`, `capture_ops`, `shadow_paths`, `publish_state` | Stop normal publication queue/shadow writers. Keep recorded-only versions and their refs for historical recovery/restore until mapped to retained snapshots. Never erase unresolved rows to pass migration. `capture-lane`, `migration-lane`, `retention-lane`, `removal-state-lane`. |
| `self_publications`, `self_publication_members`; `checkpoint_publications`; completed-branch transition proofs | Preserve and settle exact old journals; new finish publication records one complete chain/ref/index transition. Keep committed vs recovered outcome separate. `git-lane`, `migration-lane`. |
| `operations`, `operation_steps`, `checkpoints`, `checkpoint_events`, `checkpoint_exclusions`, `checkpoint_capture_issues`, `restore_operations` | Keep durability, coverage, restore and ref proof; add explicit request/target associations without discarding old membership. `state-lane`, `capture-lane`, `retention-lane`. |
| `recovery_snapshots`, `recovery_snapshot_events`, `rewrite_plans`, `rewrite_plan_commits`, `rewrite_plan_groups` | Keep immutable recovery/source-history references and explicit rewrite plan readers; migrate new explicit rewrite writers to shared plan as needed. `migration-lane`, `history-lane`. |
| `daemon_state`, `daemon_clients`, `daemon_meta`, `decision_records`, `daily_rollups` | Keep current daemon state, protection coverage, truthful history/diagnostics and historical stats. `daemon_clients` is archive-only and absent from fresh databases. Retire session writers and planner/circuit/window/replay metadata writes and dashboards. `worker-lane`, `status-lane`, `retention-lane`, `removal-state-lane`. |

The old schema's immutable membership and lineage triggers remain necessary
while their data is readable. Do not remove them just because new writers stop.
Readonly opens must remain readonly, must not run schema upgrades and must fail
clearly on unsupported versions. New mutating activation rejects a newer schema
rather than allowing an older binary to reinterpret finish records.

State removal is complete in schema v33. Fresh repositories create only current
protection, restore, configuration approval, and explicit Finish/history state.
Supported v1 through v32 upgrades use the frozen schema in `state/schema_legacy.go` and
preserve original rows, authors, membership seals, index consent, and private refs.
Read-only views inspect optional archived tables without creating them.

New candidate, drain, planner, flush, session, shadow, experiment, and background
validation writers are removed. The remaining legacy code reads old proofs or
settles an already prepared journal under exact ownership. Activation pins old
versions before checkpoint protection starts. Unprovable old publication keeps
its durable `needs_attention` result; it does not trigger shadow adoption or
label the work committed. Legacy fixture construction now uses SQL only in tests.

## CLI, integrations, diagnostics and dependencies

| Surface | Action and owner |
|---|---|
| Public `on`, `off`, `status`, `list`, `history`, `restore`, `config`, `setup`, doctor/support | Keep and replace behavior/projections through their lanes. Add public finish. On/off must not commit or test an AI connection. |
| `commit-all` | Keep a small deprecated finish delegate for the first finish release only (`cli-lane`). Remove old scoped drain and fallback engine. |
| Root compatibility `start`, `stop`, `daemon`, `wake`, `touch`, `flush`; helper extraction aliases | Remove obsolete root engine/hint aliases; keep canonical internal worker/supervisor/protection methods required by templates (`removal-surface-lane`, `hooks-lane`). `logical_boundary` no longer publishes. |
| Root compatibility `init`, `configure`, `settings`, `auth`, `version`, `events`, `prompt`, `explain`, `fix`, `logs`, `stats`, `diagnose`, `recover`, `pause`, `resume`, `purge-events`, `gc`, `rewrite-commits`, `rewrite-all` | Assign each alias to `removal-surface-lane`: retire unnecessary compatibility wrappers after canonical commands/help tests migrate. Keep the underlying explicit history, config, lifecycle and support behavior; `purge-events` cannot become data deletion. `rewrite-all` is removed; explicit reconstruction uses `history rewrite`. |
| `facade.go`, `config_facade.go`, `repo.go`, `internal_protocol.go`, supervisor method catalog | Remove config experiments/preset comparison and replay/drain dispatch; keep registry, auth, protection, restore and explicit finish/history RPC. `cli-lane`, `worker-lane`, `removal-surface-lane`. |
| Intent strategy/V2 reports, planner window totals, provider circuit projection, replay conflicts/failures, batch-wait reasons, publication drain phase/mode/progress | Replace with source checkpoint coverage, explicit frozen request/result, retryable failure and legacy outcome. No pending-queue count may substitute for saved coverage. Shared status/list/doctor/diagnose must agree (`status-lane`). |
| Claude Code, Codex, Cursor, OpenCode, Pi and shell templates; `adapter/detect.go`, integration merge and setup tests | Keep lifecycle/capture hooks, remove idle/stop automatic publication, add concise completed-task finish guidance. Synchronize template source and adapter/setup tests (`hooks-lane`). |
| Direct dependencies in `go.mod` | Keep `fsnotify` (watcher), `modernc.org/sqlite` (durable state), `cobra` (CLI), `x/sys` (filesystem/locks/restore), `huh/v2`, `x/term`, `go-isatty` (retained interactive setup/config/history). No current direct dependency is proven obsolete by deleting the planner alone. |
| Indirect dependencies and `go.sum` | `removal-surface-lane` checks remaining imports and retained tools before pruning. `govulncheck` is a declared tool, not planner baggage. Do not remove transport/UI dependencies solely because old presets disappear. |

## Tests: retained scenarios and retired assertions

File deletion is never the coverage rule. Mixed tests often combine a retired
scheduler with a retained safety scenario. Port the scenario to explicit finish
or an isolated legacy fixture before removing its old API assertion. The final
file inventory flags these as Replace or Legacy; `test-gate-lane` closes their
selectors/timings after the implementation owners record named replacements.

| Existing tests / families | Retain or retire | Replacement evidence owner |
|---|---|---|
| `capture_test.go`, `capture_resilience_test.go`, fsnotify/scheduler/lock/branch tests; checkpoint, durability, tree and retention packages | Keep privacy exclusions, symlinks/modes/deletes, large-file streaming, partial coverage, restart, polling backoff, ignored-tree cost, canonical/mixed-version locks and watcher fallback. Retire event cap/path-quiescence/lexicographic capture-offer expectations; add many-saves zero branch/AI/check work. | `capture-lane`, `retention-lane`; named `TestFinishProtection*` |
| `replay_test.go`, `capture_publication_test.go`, `publication_drain_test.go` | Port frozen-target exclusion, hard dependencies, scratch index, stale identity, timeout, rename/delete/mode and CAS safety. Retire per-save/per-event output, deferral aging, local-unlock fallback, window/candidate paging and background retry expectations. | `git-lane`, `worker-lane`; `TestFinishPublication*`, `TestFinishWorker*` |
| `TestPublicationDrainFrozenTargetOrdersHardDependenciesAndExcludesLaterEdits`; `TestCapturePublicationPagingKeepsFrozenTarget` | Keep target isolation and dependency safety under checkpoint/tree plans; discard paging mechanism. | `plan-lane`, `integration-lane` |
| `intent_candidates_test.go`, goal/complete/relationship evidence, recorded declarations/imports/calls/references, companion, published regression and language tests | Keep complete goal splits, prerequisite order, callers/tests/imports/generated/docs companionship, exact recorded middle witnesses, bounded evidence and false-relationship rejection. Retire waiting candidate continuation, soft publication, recent windows and future-companion speculation. | `plan-lane`, `ai-lane`; `TestFinishPlan*`, `TestFinishIntent*` |
| Provider/semantic retry, wait/remote/reoffer/window/health tests; `publication_plan_recovery_test.go`, message-cache recovery tests | Keep bounded timeout/invalid response/cancellation and protection during failure. Retire half-open background probes, unchanged-evidence retry cadence, semantic recovery expansion and auto-fallback. Failed finish ends and further edits remain protected until explicit retry. | `ai-lane`, `worker-lane`, `integration-lane`; `TestFinishIntent*`, `TestFinishFault*` |
| Self-publication recovery, completed-transition, external repair bridge, repair lineage/member-seal tests; `intent_history_legacy_reader_test.go` | Keep prepared/Git-applied/SQLite-completed crash boundaries, author/membership proof, readonly old-schema inspection, ambiguous ref refusal and index/worktree preservation in migration fixtures. Retire new automatic repairs and drain adoption scheduling. | `migration-lane`, `git-lane`; `TestFinishLegacy*`, `TestFinishMigration*`, `TestFinishPublication*` |
| `TestExternalRepairMappingReaderSupportsLegacyWithoutMigration`; `TestSelfPublicationEventSwitchPreservesRecoverableCandidate`; state recovery chain rollback/membership cases | Keep readonly legacy decoding and recoverable data/atomic rollback; retire mode-switch scheduling. Legacy ledger remains inspectable without mutation. | `migration-lane` |
| Runtime verifier/cache tests and `verification/service_test.go` | Keep topological parent/exact tree checks, identical-input cache after restart, isolated workspaces, process cleanup, cancellation and structural-vs-project-check reporting. No idle verification. | `plan-lane`, `worker-lane` |
| Explicit history/rewrite tests in daemon/CLI/Git/state and integration | Keep `TestIntentHistoryCorrectsCrossAuthorGoalBeforeSaving`, `TestIntentHistorySavedPrerequisitesPreserveSuccessiveGoalTrees`, authors, exact final tree, protected source and stale plan refusal. Replace candidate/auto-repair internals. | `history-lane`; `TestFinishHistoryRewrite*` |
| Config, presets, settings comparison/experiment, configure/TUI and readiness tests | Keep privacy/credentials/scopes/approved verification/binary ownership; replace defaults with local+imperative and preserved explicit Intent/style. Retire presets and experiment window/revert assertions. | `config-lane`, `onboarding-lane`; `TestFinishConfig*`, `TestFinishOnboarding*` |
| CLI status/list/projection/outcome/activity/readiness/circuit/drain tests | Keep liveness vs progress, incomplete coverage, committed vs recovered, read-only side-effect refusal and maintenance truth. Replace candidate/window/wait dashboards with saved/finish outcomes. | `status-lane`; `TestFinishStatus*` |
| Commit-all/scoped/staging tests and compatibility lifecycle/root-help tests | Keep same frozen index consent, unrelated worktree preservation, detached refusal and preview/read-only proof. Replace old engine with deprecated delegate and finish context/retry/help. | `cli-lane`, `git-lane`; `TestFinishCLI*`, `TestFinishPublication*` |
| Setup/on/off/session/adapter/global-operation/runtime-upgrade tests | Keep offline initial on, final off checkpoint, disabled/unknown repository consent, exact runtime mismatch, transactional rollback and safe owner fencing. Retire planner connection prerequisite and shutdown drain. | `onboarding-lane`, `hooks-lane` |
| `test/integration/intent_strategy_test.go`, `intent_atomicity_test.go`, `intent_flush_test.go`, `flush_logical_test.go` | Port independent completed-goal splits and exact intermediate trees to Intent finish. Retire disconnected-event queues, implicit idle/flush completion, Event fallback and background publication. | `integration-lane`; `TestFinishWorkflow*` |
| Integration self-heal/planner/semantic/publication recovery and commit-all families | Port provider/verification failures, later edits, restart, staged index, branch movement, disconnect and idempotence to requested finish and lossless legacy cutover. Do not keep semantic self-healing as product behavior. | `integration-lane`, `migration-lane`; `TestFinishFault*`, `TestFinishMigration*` |
| Integration production benchmarks, large ignored trees, lifecycle/stress, deadlock probes, support/PTY tests | Retain realistic resource/lifecycle/PTY and zero-idle-work bounds. Replace commit-per-save throughput metrics; do not claim speed improvements without measurements. | `integration-lane`, `test-gate-lane` |
| `scripts/dev/test.sh`, `test-package-shards.sh`, `test-manifest.py`, `test-timings.json`, hosted workflows | Remove retired named selectors/timings and retain every replacement exactly once. Keep three local core shards, max two parallel tests per package process; hosted four core/four integration/support on Ubuntu/macOS, six daemon-stress and two Git/state-stress lanes. Five-minute wall-clock target, leaf target 4m30s/hard 5m. | `test-gate-lane` |

The baseline core helper defaults to four parallel cases; the local gate now
passes two explicitly for its three core shards. Hosted core, integration,
support, and stress topology and timeout limits are preserved. Dynamic compiled
name discovery assigns every live test, example, and fuzz entrypoint exactly once.
The final source gate passed in 180.565 seconds; lint passed in 0.672 seconds.
These are local results, not hosted CI or installed-runtime evidence.

## Documentation ownership

`docs-lane` reconciles the canonical product and agent contracts after integrated
tests. `config-lane` owns the generated configuration reference. `hooks-lane`
owns template README/guidance; templates remain the source of truth. Preserve
the `AGENTS.md -> CLAUDE.md` symlink.

| File / current section | Required action |
|---|---|
| README, `CLAUDE.md`, changelog, workflow image | Replace automatic semantic-commit promises/instructions with continuous private saves and explicit finish; explain old active runtime ownership until explicitly upgraded. Update the image if it still implies automatic branch commits. |
| `overview.md`: diagram, publication scheduler, checkpoint store, publication evaluation, publication | Replace two competing engines with checkpoint protection and bounded request execution; retain identity, restore and global-transaction safety. |
| `capture-replay.md`: eligible scope, durable completion, publication behavior, retention | Remove event queue cap/classification/publication resumption guidance; keep streamed objects, partial coverage, exact journals and retention. Rename only with incoming link updates. |
| `intent-commit-flow.md`: inputs/assignment, gates, history reconstruction, presets, planner recovery | Rewrite around completed goals/proposed trees and explicit Intent finish. Retire wait/window/aging/preset/automatic repair/fallback sections; preserve dependency and verification constraints. |
| `settings.md`, generated `configuration-reference.md` | Remove retired knobs/presets/experiments; explain local/Intent, both styles, egress, consent and approved checks. Generated reference owned by `config-lane`. |
| `commands.md`: setup/on/off, dashboard/commit-all, JSON, aliases | Add finish/context/dry-run/retry/staged consent; explain first-release deprecated delegate. Keep canonical history rewrite and support/restore; reconcile legacy JSON fields. |
| `user-workflows.md`: setup, enable, daily check, delayed publication, off/on, upgrade | Use offline on/edit/save/finish/off examples; explain failed explicit requests, protection continuing, and advanced setup upgrades without silent global migration. |
| `ai-providers.md`: privacy, subprocess and failure behavior | Keep privacy/transport contracts; provider calls only on authorized explicit operations, bounded correction and no automatic Intent-to-local fallback. |
| `intent-commit-rewrite-flow.md`, `rewrite-commits.md` | Keep explicit selected rewrite examples, source refs, author boundaries, exact plans and local mode honesty; remove automatic history-repair assumptions. |
| `multi-tool.md`, template README/hook snippets/uninstall guidance | Keep lifecycle/protection hooks; remove idle/stop commits and document authorized completed-task finish (`hooks-lane`). |
| `intent-v2-migration.md`, `capture-resilience-plan.md` | Label historical design/migration evidence; retain exact old-schema safety/provenance. Point current operations to finish cutover guidance. |
| This map and `finish-contract.md` | Keep as version-scoped implementation references. They must not imply that pending source work has been installed or migrated. |

## Baseline and validation

Counts below are physical Go source lines, including blank lines/comments, read
from the baseline Git tree. Production means non-`_test.go`; test counts exclude
Markdown, Python/shell tooling and JSON fixtures. Top-level function counts use
`^func Test\w*\(` and `^func Benchmark\w*\(`, not subtests or runtime-discovered
cases. They describe source size and inventory, not passing tests or performance.

The required search was `rg -n
"Replay|PublicationDrain|IntentCandidate|IntentForwardRecovery|repair.enabled|logical_boundary"
internal templates docs`: 6,276 matching lines across 216 baseline files
(2,131 production-Go, 4,137 test-Go, 8 documentation/template lines). Its callers
and settings are assigned above and in the file inventory below. Future searches
also match this map by design; those are historical references, not live engines.

Documentation-only validation is `git diff --check`, the AGENTS symlink checks,
local source/link existence and exhaustive file-assignment checks. No Go build,
package/race/integration test, provider call or broad gate was run for this map.

### Baseline counts

| Scope | Production files | Production lines | Test files | Test lines |
|---|---:|---:|---:|---:|
| `cmd/acd` | 1 | 17 | 0 | 0 |
| `internal/adapter` | 2 | 444 | 1 | 887 |
| `internal/ai` | 23 | 9,072 | 23 | 8,467 |
| `internal/central` | 5 | 1,486 | 6 | 1,865 |
| `internal/checkpoint` | 3 | 708 | 3 | 664 |
| `internal/cli` | 66 | 29,879 | 71 | 29,232 |
| `internal/config` | 8 | 1,639 | 6 | 891 |
| `internal/credentials` | 2 | 438 | 1 | 209 |
| `internal/daemon` | 68 | 43,985 | 108 | 57,887 |
| `internal/git` | 16 | 5,233 | 17 | 4,847 |
| `internal/globalops` | 2 | 327 | 2 | 101 |
| `internal/identity` | 4 | 221 | 1 | 267 |
| `internal/installer` | 6 | 3,384 | 5 | 1,628 |
| `internal/integration` | 1 | 550 | 1 | 170 |
| `internal/logger` | 2 | 430 | 1 | 388 |
| `internal/migration` | 3 | 1,088 | 2 | 497 |
| `internal/paths` | 2 | 229 | 1 | 249 |
| `internal/pause` | 3 | 317 | 1 | 434 |
| `internal/prompttrace` | 2 | 627 | 1 | 146 |
| `internal/restore` | 4 | 1,718 | 1 | 1,128 |
| `internal/settings` | 5 | 1,813 | 4 | 1,394 |
| `internal/settingsui` | 3 | 908 | 2 | 426 |
| `internal/state` | 39 | 19,189 | 26 | 12,154 |
| `internal/supervisor` | 10 | 1,690 | 4 | 1,171 |
| `internal/trace` | 1 | 306 | 1 | 226 |
| `internal/verification` | 1 | 1,397 | 1 | 878 |
| `internal/version` | 1 | 89 | 1 | 22 |
| `templates/embed.go` | 1 | 10 | 0 | 0 |
| `test/integration` | 0 | 0 | 41 | 16,289 |
| Total | 284 | 127,194 | 332 | 142,517 |

The baseline contains 2,647 top-level test functions and 4 benchmarks. No
subtest count or runtime scenario count is inferred from those names.

## Exhaustive baseline file assignment

File names in each subsection are relative to that subsection. A Replace test
row preserves its user scenario while retiring old API/scheduling assertions;
a Legacy row keeps isolated proof/decoder fixtures. These are ownership
planned assignments, not final file-presence claims or permission to delete a
mixed file wholesale. The final outcome overrides below explain absent planned
Keep/Legacy files and their retained replacement proofs.

### `cmd/acd`

| Kind | Action | Owner | Files |
|---|---|---|---|
| Production | Keep | `cli-lane` | `main.go` |

### `internal/adapter`

| Kind | Action | Owner | Files |
|---|---|---|---|
| Production | Keep | `hooks-lane` | `detect.go`, `known.go` |
| Tests | Keep | `hooks-lane` | `detect_test.go` |

### `internal/ai`

| Kind | Action | Owner | Files |
|---|---|---|---|
| Production | Keep | `ai-lane` | `config.go`, `error_sanitize.go`, `message_format.go`, `openai_compat.go`, `plugin_subprocess.go`, `provider.go`, `provider_error.go`, `redact.go` |
| Production | Keep | `history-lane` | `rewrite_gate.go`, `rewrite_proposals.go` |
| Production | Replace | `ai-lane` | `intent_cli_reference.go`, `intent_maintenance.go`, `intent_planner.go`, `intent_protocol_v2.go`, `intent_rejects.go`, `intent_unassigned.go`, `intent_wire_v2.go`, `message_quality.go`, `prompt.go`, `prompt_trace.go`, `provider_probe.go` |
| Production | Replace | `local-lane` | `deterministic.go`, `subject_fallback.go` |
| Tests | Keep | `ai-lane` | `config_test.go`, `error_sanitize_test.go`, `openai_compat_test.go`, `plugin_subprocess_test.go`, `redact_test.go` |
| Tests | Keep | `history-lane` | `rewrite_gate_test.go`, `rewrite_proposals_test.go` |
| Tests | Replace | `ai-lane` | `intent_file_metadata_test.go`, `intent_goal_evidence_test.go`, `intent_maintenance_test.go`, `intent_planner_test.go`, `intent_protocol_retry_test.go`, `intent_protocol_v2_test.go`, `intent_provider_v2_test.go`, `intent_rejects_test.go`, `intent_unassigned_test.go`, `intent_wire_v2_test.go`, `message_quality_labels_test.go`, `message_quality_test.go`, `prompt_test.go`, `provider_probe_test.go` |
| Tests | Replace | `local-lane` | `deterministic_test.go`, `subject_fallback_test.go` |

### `internal/central`

| Kind | Action | Owner | Files |
|---|---|---|---|
| Production | Keep | `retention-lane` | `migrate.go`, `missing_repo.go`, `registry.go` |
| Production | Replace | `status-lane` | `aggregate.go`, `stats_db.go` |
| Tests | Keep | `retention-lane` | `aggregate_concurrency_test.go`, `missing_repo_test.go`, `registry_concurrency_test.go`, `registry_test.go` |
| Tests | Replace | `status-lane` | `aggregate_test.go`, `stats_db_test.go` |

### `internal/checkpoint`

| Kind | Action | Owner | Files |
|---|---|---|---|
| Production | Keep | `retention-lane` | `maintenance.go`, `retention.go`, `store.go` |
| Tests | Keep | `retention-lane` | `maintenance_test.go`, `retention_test.go`, `store_test.go` |

### `internal/cli`

| Kind | Action | Owner | Files |
|---|---|---|---|
| Production | Keep | `cli-lane` | `exit.go`, `helpers.go`, `version.go` |
| Production | Keep | `config-lane` | `auth.go`, `credential_runtime.go` |
| Production | Keep | `retention-lane` | `checkpoint_maintenance.go`, `gc.go` |
| Production | Keep | `status-lane` | `events.go`, `explain.go`, `logs.go`, `prompt.go` |
| Production | Keep | `worker-lane` | `session_supervisor.go` |
| Production | Replace | `cli-lane` | `commit_all_preview.go`, `commit_all_staging_recovery.go`, `internal_protocol.go`, `product_commit_all.go`, `root.go` |
| Production | Replace | `history-lane` | `intent_history.go`, `rewrite_commits.go`, `rewrite_commits_editor.go`, `rewrite_progress.go` |
| Production | Replace | `hooks-lane` | `hookhelper.go`, `integration_event.go`, `touch.go`, `wake.go` |
| Production | Replace | `migration-lane` | `fix.go`, `fix_runtime.go`, `global_repair.go`, `purge.go`, `recover.go`, `recovery_probe.go` |
| Production | Replace | `onboarding-lane` | `control.go`, `facade_mutations.go`, `pause.go`, `pause_state.go`, `repo_activation.go`, `repo_autodiscovery.go`, `restart_settings.go`, `setup.go`, `setup_onboarding.go`, `start.go`, `start_shortcircuit.go`, `stop.go` |
| Production | Replace | `removal-surface-lane` | `config_editor.go`, `config_facade.go`, `config_readiness.go`, `configure.go`, `daemon.go`, `facade.go`, `help.go`, `repo.go`, `settings.go` |
| Production | Replace | `status-lane` | `commit_display.go`, `diagnose.go`, `doctor.go`, `intent_observability.go`, `intent_planner_window.go`, `list.go`, `product_list.go`, `product_list_overview.go`, `product_result.go`, `publication_outcome.go`, `replay_observability.go`, `stats.go`, `status.go`, `worker_activity_hints.go` |
| Tests | Keep | `config-lane` | `auth_test.go`, `credential_runtime_test.go` |
| Tests | Keep | `retention-lane` | `gc_test.go` |
| Tests | Keep | `status-lane` | `events_test.go`, `explain_test.go`, `logs_test.go`, `prompt_test.go` |
| Tests | Replace | `cli-lane` | `advanced_result_test.go`, `capture_health_test.go`, `cli_testutil_test.go`, `cli_truth_test.go`, `commit_all_scope_test.go`, `commit_all_staging_recovery_test.go`, `commitall_test.go`, `compatibility_lifecycle_test.go`, `completed_transition_barrier_test.go`, `config_docs_test.go`, `fix_pending_capture_recovery_test.go`, `helpers_resolver_test.go`, `hook_failure_policy_test.go`, `internal_protocol_test.go`, `product_contract_test.go`, `product_list_activity_test.go`, `product_list_proof_test.go`, `product_recovery_changed_test.go`, `projection_fixture_test.go`, `provider_circuit_projection_test.go`, `provider_retry_status_test.go`, `publication_capture_progress_test.go`, `publication_outcome_history_test.go`, `recovery_proof_test.go`, `repo_lookup_canonical_test.go`, `root_help_test.go`, `semantic_journeys_test.go`, `staging_consent_test.go`, `worker_activity_test.go` |
| Tests | Replace | `history-lane` | `intent_history_test.go`, `rewrite_commits_test.go` |
| Tests | Replace | `hooks-lane` | `hookhelper_test.go`, `integration_event_test.go`, `wake_test.go` |
| Tests | Replace | `migration-lane` | `fix_test.go`, `global_repair_test.go`, `purge_test.go`, `recover_test.go`, `recovery_probe_test.go` |
| Tests | Replace | `onboarding-lane` | `control_test.go`, `facade_mutations_test.go`, `pause_test.go`, `repo_activation_test.go`, `repo_autodiscovery_test.go`, `restart_settings_test.go`, `setup_onboarding_test.go`, `setup_test.go`, `start_test.go`, `stop_test.go` |
| Tests | Replace | `removal-surface-lane` | `config_editor_test.go`, `config_readiness_test.go`, `configure_test.go`, `daemon_test.go`, `help_test.go`, `repo_test.go` |
| Tests | Replace | `status-lane` | `diagnose_test.go`, `doctor_test.go`, `intent_observability_test.go`, `list_test.go`, `product_list_test.go`, `replay_observability_test.go`, `stats_test.go`, `status_test.go`, `worker_activity_hints_test.go` |

### `internal/config`

| Kind | Action | Owner | Files |
|---|---|---|---|
| Production | Keep | `config-lane` | `autodiscovery.go`, `fingerprint.go`, `store.go` |
| Production | Replace | `config-lane` | `fields.go`, `presets.go`, `reference.go`, `settings.go` |
| Tests | Keep | `config-lane` | `autodiscovery_test.go`, `fingerprint_test.go` |
| Tests | Replace | `config-lane` | `presets_test.go`, `reference_test.go`, `settings_test.go` |

### `internal/config/configdoc`

| Kind | Action | Owner | Files |
|---|---|---|---|
| Production | Replace | `config-lane` | `main.go` |
| Tests | Replace | `config-lane` | `main_test.go` |

### `internal/credentials`

| Kind | Action | Owner | Files |
|---|---|---|---|
| Production | Keep | `ai-lane` | `store.go`, `transaction.go` |
| Tests | Keep | `ai-lane` | `store_test.go` |

### `internal/daemon`

| Kind | Action | Owner | Files |
|---|---|---|---|
| Production | Delete | `removal-daemon-lane` | `capture_publication.go`, `coalesce.go`, `intent_clarifications.go`, `intent_frozen_window.go`, `intent_goal_window.go`, `intent_health.go`, `intent_history_audit.go`, `intent_replay_v2.go`, `intent_retry_schedule.go`, `intent_semantic_retry.go`, `intent_unclassified_candidates.go`, `publication_drain.go`, `publication_message_cache.go`, `publication_plan_recovery.go`, `replay.go` |
| Production | Keep | `worker-lane` | `fsnotify_watcher.go`, `lock.go`, `poll_scheduler.go`, `signals_unix.go`, `trace.go` |
| Production | Legacy | `migration-lane` | `external_repair_bridge.go`, `intent_repair.go`, `live_index_repair.go`, `reconcile.go`, `self_publication_recovery.go` |
| Production | Replace | `capture-lane` | `bootstrap.go`, `branch_token.go`, `capture.go`, `classify.go`, `daemon.go`, `shadow_io.go` |
| Production | Replace | `git-lane` | `self_publication.go` |
| Production | Replace | `history-lane` | `intent_history.go`, `intent_history_correction.go` |
| Production | Replace | `local-lane` | `message.go` |
| Production | Replace | `plan-lane` | `intent_candidates.go`, `intent_cobra_commands.go`, `intent_cobra_evidence.go`, `intent_evidence_review.go`, `intent_file_metadata.go`, `intent_frozen_published_context.go`, `intent_goal_evidence.go`, `intent_published_companions.go`, `intent_published_regressions.go`, `intent_published_typescript_regressions.go`, `intent_recorded_constants.go`, `intent_recorded_declarations.go`, `intent_recorded_go_callers.go`, `intent_recorded_go_calls.go`, `intent_recorded_imports.go`, `intent_recorded_project_references.go`, `intent_recorded_references.go`, `intent_recorded_typescript.go`, `intent_relationship_evidence.go`, `intent_source_relationships.go`, `intent_swift_maintenance.go`, `intent_verification_activity.go`, `runtime_verification.go` |
| Production | Replace | `retention-lane` | `dead_branch_sweep.go`, `prune.go`, `refcount.go`, `rollup.go` |
| Production | Replace | `status-lane` | `publication_reason.go`, `replay_observability.go` |
| Production | Replace | `worker-lane` | `config_validation.go`, `publication_evaluation.go`, `runtime_config.go`, `runtime_migration.go` |
| Tests | Keep | `worker-lane` | `fsnotify_watcher_test.go`, `lock_test.go` |
| Tests | Legacy | `history-lane` | `intent_history_legacy_reader_test.go` |
| Tests | Legacy | `migration-lane` | `external_repair_bridge_atomic_test.go`, `external_repair_bridge_drain_test.go`, `external_repair_bridge_edge_test.go`, `external_repair_bridge_test.go`, `intent_repair_test.go`, `self_publication_recovery_test.go`, `semantic_message_run_recovery_test.go` |
| Tests | Replace | `capture-lane` | `bootstrap_test.go`, `branch_token_test.go`, `capture_resilience_test.go`, `capture_test.go`, `classify_test.go`, `completed_transition_adoption_test.go`, `daemon_stress_test.go`, `daemon_test.go`, `maintenance_test.go`, `parallel_test.go`, `reconcile_completed_repair_test.go`, `replay_self_heal_test.go`, `scheduler_test.go`, `test_repo_template_test.go` |
| Tests | Replace | `history-lane` | `intent_history_cli_reference_test.go`, `intent_history_correction_test.go`, `intent_history_dependencies_test.go`, `intent_history_large_diff_test.go`, `intent_history_maintenance_test.go`, `intent_history_request_capture_test.go`, `intent_history_typescript_reference_test.go`, `intent_history_validation_test.go` |
| Tests | Replace | `local-lane` | `message_test.go` |
| Tests | Replace | `plan-lane` | `intent_candidates_test.go`, `intent_checkpoint_recovery_test.go`, `intent_cobra_commands_test.go`, `intent_cobra_evidence_test.go`, `intent_companion_review_test.go`, `intent_complete_evidence_test.go`, `intent_document_companions_test.go`, `intent_documented_api_reference_test.go`, `intent_evidence_priority_test.go`, `intent_evidence_review_test.go`, `intent_expanded_window_recovery_test.go`, `intent_fallback_metadata_test.go`, `intent_forced_reoffer_test.go`, `intent_frozen_published_context_test.go`, `intent_goal_evidence_test.go`, `intent_goal_readiness_test.go`, `intent_health_integration_test.go`, `intent_late_script_companion_test.go`, `intent_legacy_fallback_subject_test.go`, `intent_local_binding_scope_test.go`, `intent_preserved_dependencies_test.go`, `intent_preserved_membership_test.go`, `intent_preserved_wait_test.go`, `intent_provider_retry_test.go`, `intent_published_callees_test.go`, `intent_published_companions_test.go`, `intent_published_regressions_test.go`, `intent_published_type_support_test.go`, `intent_published_typescript_regressions_test.go`, `intent_recorded_constants_test.go`, `intent_recorded_declarations_test.go`, `intent_recorded_go_calls_test.go`, `intent_recorded_imports_test.go`, `intent_recorded_project_references_test.go`, `intent_recorded_references_test.go`, `intent_recorded_typescript_test.go`, `intent_redundant_context_test.go`, `intent_relationship_evidence_test.go`, `intent_remote_wait_test.go`, `intent_repair_split_test.go`, `intent_semantic_rejection_test.go`, `intent_semantic_retry_cadence_test.go`, `intent_semantic_retry_replay_test.go`, `intent_source_reference_context_test.go`, `intent_stale_purpose_test.go`, `intent_swift_maintenance_test.go`, `intent_unassigned_replay_test.go`, `intent_verification_activity_test.go`, `intent_wait_membership_test.go`, `intent_wait_semantic_recovery_test.go`, `runtime_verification_test.go` |
| Tests | Replace | `removal-daemon-lane` | `capture_publication_test.go`, `coalesce_test.go`, `intent_frozen_window_test.go`, `intent_goal_window_test.go`, `intent_health_test.go`, `intent_history_audit_test.go`, `intent_retry_schedule_test.go`, `intent_semantic_retry_test.go`, `intent_unclassified_candidates_test.go`, `publication_drain_test.go`, `publication_message_cache_test.go`, `publication_plan_recovery_test.go`, `replay_test.go` |
| Tests | Replace | `retention-lane` | `dead_branch_sweep_test.go`, `refcount_test.go`, `rollup_test.go` |
| Tests | Replace | `status-lane` | `replay_observability_test.go` |
| Tests | Replace | `worker-lane` | `config_validation_test.go`, `publication_evaluation_test.go`, `publication_fallback_goal_message_test.go`, `publication_message_recovery_test.go`, `publication_transport_recovery_test.go`, `runtime_config_test.go`, `runtime_migration_test.go` |

### `internal/git`

| Kind | Action | Owner | Files |
|---|---|---|---|
| Production | Keep | `git-lane` | `diff.go`, `durability.go`, `git.go`, `history.go`, `ignore.go`, `object_inventory.go`, `pathspec.go`, `refs.go`, `tree.go`, `worktree.go` |
| Production | Legacy | `migration-lane` | `intent_repair.go` |
| Production | Replace | `git-lane` | `index_consent.go`, `publication_proof.go` |
| Production | Replace | `history-lane` | `intent_history.go`, `rewrite_apply.go`, `rewrite_selection.go` |
| Tests | Keep | `git-lane` | `diff_test.go`, `durability_test.go`, `git_test.go`, `history_test.go`, `ignore_test.go`, `object_inventory_test.go`, `pathspec_test.go`, `ref_transaction_test.go`, `refs_test.go`, `tree_test.go`, `worktree_test.go` |
| Tests | Legacy | `migration-lane` | `intent_repair_test.go` |
| Tests | Replace | `git-lane` | `index_consent_test.go`, `publication_proof_test.go` |
| Tests | Replace | `history-lane` | `intent_history_test.go`, `rewrite_apply_test.go`, `rewrite_selection_test.go` |

### `internal/globalops`

| Kind | Action | Owner | Files |
|---|---|---|---|
| Production | Keep | `onboarding-lane` | `lock.go`, `store.go` |
| Tests | Keep | `onboarding-lane` | `lock_test.go`, `store_test.go` |

### `internal/identity`

| Kind | Action | Owner | Files |
|---|---|---|---|
| Production | Keep | `worker-lane` | `fingerprint.go`, `pid.go`, `ps_darwin.go`, `ps_linux.go` |
| Tests | Keep | `worker-lane` | `identity_test.go` |

### `internal/installer`

| Kind | Action | Owner | Files |
|---|---|---|---|
| Production | Keep | `onboarding-lane` | `installer.go`, `runtime_upgrade.go`, `selftest.go`, `supervisor_shutdown.go`, `uninstall.go` |
| Production | Replace | `onboarding-lane` | `revalidate.go` |
| Tests | Keep | `onboarding-lane` | `installer_test.go`, `readiness_test.go`, `runtime_upgrade_test.go`, `setup_convergence_test.go`, `supervisor_shutdown_test.go` |

### `internal/integration`

| Kind | Action | Owner | Files |
|---|---|---|---|
| Production | Keep | `hooks-lane` | `merge.go` |
| Tests | Keep | `hooks-lane` | `merge_test.go` |

### `internal/logger`

| Kind | Action | Owner | Files |
|---|---|---|---|
| Production | Keep | `status-lane` | `logger.go`, `rotate.go` |
| Tests | Keep | `status-lane` | `logger_test.go` |

### `internal/migration`

| Kind | Action | Owner | Files |
|---|---|---|---|
| Production | Replace | `migration-lane` | `bridge.go`, `bridge_recovery.go`, `migration.go` |
| Tests | Keep | `migration-lane` | `migration_checkpoint_test.go` |
| Tests | Replace | `migration-lane` | `bridge_test.go` |

### `internal/paths`

| Kind | Action | Owner | Files |
|---|---|---|---|
| Production | Keep | `worker-lane` | `paths.go`, `repo_hash.go` |
| Tests | Keep | `worker-lane` | `paths_test.go` |

### `internal/pause`

| Kind | Action | Owner | Files |
|---|---|---|---|
| Production | Keep | `worker-lane` | `marker.go`, `marker_other.go`, `marker_unix.go` |
| Tests | Keep | `worker-lane` | `marker_test.go` |

### `internal/prompttrace`

| Kind | Action | Owner | Files |
|---|---|---|---|
| Production | Keep | `ai-lane` | `prompttrace.go`, `read.go` |
| Tests | Keep | `ai-lane` | `prompttrace_test.go` |

### `internal/restore`

| Kind | Action | Owner | Files |
|---|---|---|---|
| Production | Keep | `migration-lane` | `rename_noreplace_darwin.go`, `rename_noreplace_linux.go`, `restore.go`, `safe_fs_unix.go` |
| Tests | Keep | `migration-lane` | `restore_test.go` |

### `internal/settings`

| Kind | Action | Owner | Files |
|---|---|---|---|
| Production | Delete | `removal-surface-lane` | `comparison.go`, `experiment.go` |
| Production | Replace | `config-lane` | `apply.go`, `service.go`, `snapshot.go` |
| Tests | Replace | `config-lane` | `apply_test.go`, `service_test.go` |
| Tests | Replace | `removal-surface-lane` | `comparison_test.go`, `experiment_test.go` |

### `internal/settingsui`

| Kind | Action | Owner | Files |
|---|---|---|---|
| Production | Replace | `config-lane` | `configure.go`, `editor.go`, `fields.go` |
| Tests | Replace | `config-lane` | `configure_test.go`, `editor_test.go` |

### `internal/state`

| Kind | Action | Owner | Files |
|---|---|---|---|
| Production | Keep | `capture-lane` | `capture_health.go`, `checkpoint.go`, `checkpoint_capture.go`, `checkpoint_coverage.go`, `safe_ignore.go`, `sensitive.go` |
| Production | Keep | `retention-lane` | `checkpoint_retention.go`, `recovery.go`, `rollups.go` |
| Production | Keep | `state-lane` | `backup.go`, `db.go`, `meta.go`, `operations.go` |
| Production | Keep | `worker-lane` | `daemon_clients.go`, `daemon_state.go` |
| Production | Legacy | `removal-state-lane` | `checkpoint_intent_reconcile.go`, `flush.go`, `intent_forward_recovery.go`, `intent_goal_wait.go`, `intent_plan_run.go`, `intent_planner_window.go`, `intent_published_companions.go`, `intent_v2.go`, `legacy_preflight.go`, `publication_drain.go`, `publish.go`, `shadow.go` |
| Production | Replace | `git-lane` | `completed_branch_transition.go`, `self_publication.go`, `staging_review.go` |
| Production | Replace | `history-lane` | `intent_history_plan.go`, `rewrite_plans.go` |
| Production | Replace | `removal-state-lane` | `activity.go`, `config_validation.go`, `events.go`, `runtime_config.go` |
| Production | Replace | `state-lane` | `migrate.go`, `schema.go` |
| Production | Replace | `status-lane` | `decisions.go` |
| Tests | Keep | `capture-lane` | `checkpoint_capture_test.go`, `checkpoint_test.go`, `safe_ignore_test.go`, `sensitive_test.go` |
| Tests | Keep | `retention-lane` | `recovery_test.go` |
| Tests | Legacy | `removal-state-lane` | `checkpoint_intent_reconcile_test.go`, `flush_test.go`, `intent_forward_recovery_test.go`, `intent_plan_run_test.go`, `intent_planner_window_test.go`, `intent_v2_test.go`, `publication_drain_test.go` |
| Tests | Replace | `git-lane` | `completed_branch_transition_test.go`, `self_publication_test.go`, `staging_review_test.go` |
| Tests | Replace | `history-lane` | `intent_history_plan_test.go`, `rewrite_plans_test.go` |
| Tests | Replace | `removal-state-lane` | `config_validation_test.go`, `intent_plan_run_retry_test.go`, `intent_repair_lineage_test.go`, `readpool_test.go`, `resilience_test.go`, `runtime_config_test.go`, `staging_consent_test.go`, `state_test.go` |
| Tests | Replace | `status-lane` | `decisions_test.go` |

### `internal/supervisor`

| Kind | Action | Owner | Files |
|---|---|---|---|
| Production | Keep | `worker-lane` | `client.go`, `environment.go`, `peercred_darwin.go`, `peercred_other.go`, `server.go`, `service.go`, `session.go` |
| Production | Replace | `status-lane` | `worker_status.go` |
| Production | Replace | `worker-lane` | `protocol.go`, `worker_server.go` |
| Tests | Keep | `worker-lane` | `client_test.go`, `environment_test.go`, `session_test.go` |
| Tests | Replace | `worker-lane` | `protocol_test.go` |

### `internal/trace`

| Kind | Action | Owner | Files |
|---|---|---|---|
| Production | Keep | `status-lane` | `trace.go` |
| Tests | Keep | `status-lane` | `trace_test.go` |

### `internal/verification`

| Kind | Action | Owner | Files |
|---|---|---|---|
| Production | Keep | `plan-lane` | `service.go` |
| Tests | Keep | `plan-lane` | `service_test.go` |

### `internal/version`

| Kind | Action | Owner | Files |
|---|---|---|---|
| Production | Keep | `cli-lane` | `version.go` |
| Tests | Keep | `cli-lane` | `version_test.go` |

### `templates`

| Kind | Action | Owner | Files |
|---|---|---|---|
| Production | Keep | `hooks-lane` | `embed.go` |

### `test/integration`

| Kind | Action | Owner | Files |
|---|---|---|---|
| Tests | Keep fixtures | `integration-lane` | `helpers_test.go` |
| Tests | Keep scenarios | `integration-lane` | `active_dashboard_test.go`, `adapter_e2e_test.go`, `ai_providers_test.go`, `capture_resilience_test.go`, `explainable_ux_test.go`, `fsnotify_fallback_test.go`, `large_ignored_tree_test.go`, `lifecycle_test.go`, `pty_harness_test.go`, `session_runtime_test.go`, `session_supervisor_test.go`, `settings_live_reload_test.go`, `settings_tui_test.go`, `setup_selftest_test.go`, `start_latency_budget_test.go`, `supervisor_scale_test.go` |
| Tests | Legacy scenarios | `migration-lane` | `populated_state_test.go`, `reliability_recover_test.go`, `self_publication_transition_test.go` |
| Tests | Replace scenarios | `integration-lane` | `active_chain_self_heal_test.go`, `commitall_test.go`, `dead_branch_prune_test.go`, `deadlock_probe_test.go`, `fix_consolidated_test.go`, `flush_logical_test.go`, `intent_atomicity_test.go`, `intent_flush_test.go`, `intent_history_worker_test.go`, `intent_planner_normalization_test.go`, `intent_planner_recovery_test.go`, `intent_self_healing_test.go`, `intent_strategy_test.go`, `production_benchmark_test.go`, `publication_self_healing_test.go`, `regressions_test.go`, `rewrite_commits_test.go`, `self_heal_blocked_test.go`, `self_heal_test.go`, `settings_experiment_test.go`, `squash_backlog_recovery_test.go` |

The ledger assigns all 284 production and 332 test files in the measured Go
tree. Non-Go surfaces are assigned in the settings, CLI/integration, docs and
test-gate tables. Re-run the assignment check against the recorded baseline,
then check for new source files after integration; the baseline is not a claim
that later files are already reviewed.

## Final file outcomes override baseline assignments

The exhaustive tables above preserve the original plan. Their Keep/Replace/Legacy
labels do not assert that a file remains in the finished source tree. Static
path checks against the current checkout give these final baseline outcomes:

| Baseline files | Still present | Removed or moved |
|---|---:|---:|
| 284 production files | 186 | 98 |
| 332 test files | 168 | 164 |

These counts describe baseline paths only. New Finish modules and tests are
additional files; a surviving path can also contain a replaced implementation.
The original baseline line counts and assignments remain unchanged for audit.

All eight production files originally assigned Keep but now absent are accounted
for below, together with their five absent Keep test files:

| Absent baseline Keep paths | Final outcome and retained proof |
|---|---|
| `ai/message_format.go`, `ai/provider.go`; `ai/openai_compat_test.go` | Shared styles and message validation moved to `finish/message.go`; the old generic provider interface became the bounded Finish transport in `ai/finish_planner.go`. HTTP/subprocess implementations remain. `TestFinishLocalRejectUnsafeMessages`, `TestFinishIntentOpenAITransport`, `TestFinishIntentOpenAIRejectsFailureAndRedirect`, and `TestFinishIntentSubprocessOneExchange` retain format, transport, refusal, and one-exchange proof. |
| `ai/rewrite_gate.go`, `ai/rewrite_proposals.go` and their test files | Retired proposal/gate interfaces are replaced by `ai/finish_history.go`, `finish/plan.go`, `git/finish_history.go`, and `state/finish_history.go`. `TestFinishPlanRenameAndAuthorBoundaries` and `TestFinishWorkflowHistorySavedPlanPreservesMembershipAndAuthors` plus `TestFinishWorkflowHistoryPurposefulGoalsAndStaleRefusal` retain author/tree/source-history safety. Automatic repair and mutable candidate scaffolding are removed. |
| `cli/version.go` | The redundant command wrapper is removed. Version reporting lives in `cli/root.go` and `internal/version`; `TestRootVersionFlagAndFiniteCommandCatalog` retains the actual public flag and command catalog. |
| `cli/prompt.go`, `cli/prompt_test.go` | Public old-planner prompt inspection is retired. Privacy, redaction, and exact bounded request disclosure remain in `ai/finish_planner.go`, `ai/redact.go`, and their Finish tests, including `TestFinishIntentPrivacyAndSuppliedMessage`. No replacement public prompt command is invented. |
| `state/checkpoint_capture.go`, `state/checkpoint_capture_test.go` | New checkpoints do not append or attach old capture-event membership. Current protection uses `daemon/protection_entry.go`, checkpoint state/coverage, and exact Git trees. `TestFinishProtectionPartialCheckpointRecoversWithoutQueue` and `TestFinishSchemaFreshContainsOnlyCurrentState` prove protection without those tables. Archived membership still has read-only decoders, immutable pins, and explicit SQL fixtures. |
| `state/daemon_clients.go` | Session registration/refcount writers are retired. Canonical worker ownership and protection hints remain. `TestFinishRemovalEmptyLegacyClientsKeepProtectionRunning` proves an upgraded database with an empty old client table cannot terminate protection. |

The three absent integration files assigned Keep scenarios
(`ai_providers_test.go`, `capture_resilience_test.go`, `lifecycle_test.go`) are
scenario ports, not lost Keep obligations. Their current workflow, large-asset,
provider-failure, shutdown and restart proofs are named in the integration tables
below. No old per-save commit or session-flush expectation remains.

Baseline Legacy labels were narrowed after actual callers migrated. Ten such
production paths and thirteen Legacy test paths are absent: the removed daemon
repair/live-index adoption engines and the state reconcile/flush/forward-recovery/
waiting-plan/publication writers no longer run. Their original rows, immutable
triggers, and captured objects survive in upgraded databases through the frozen
`state/schema_legacy.go` archive. Current exact readers and settlement live in
`state/finish_legacy.go`, `state/legacy_inventory.go`,
`daemon/legacy_repair_proof.go`, `daemon/self_publication_recovery.go`, and
`migration/finish_cutover.go`. `TestFinishSchemaUpgradePreservesOriginalEvidenceAndReadonlyBoundary`,
`TestFinishLegacyImmutableCutoverAndReadOnlyProjection`, and
`TestFinishRemovalLegacyBridgeRequiresActivationWithoutMutation` retain old-row,
read-only, and fail-closed mutable-bridge evidence. Real migration integration
fixtures retain ambiguous chain, linked-worktree, index, and captured-byte proof.

Absent Replace paths are resolved by the current modules and scenario tables:
`finish/*` owns complete goals and Local messages; `ai/finish_*` owns bounded
Intent evidence; `daemon/finish.go` owns frozen requests;
`git/finish_publish.go` and `state/finish_publication.go` own the complete-chain
transaction; `finish_history` modules own explicit rewrite. Protection, restore,
checkpoint retention, exact legacy readers, and current facade commands retain
their dedicated safety tests. Removed candidate windows, automatic retry/repair,
experiments, session lifetimes, and per-save publication have no runtime delegate.

`ai/provider_error.go` remains: its small typed HTTP error is used by retained
transport failure handling. It is not an old scheduler or provider interface.

## Surface removal completed

The CLI now uses direct Local/Intent and imperative/conventional choices. Reviewed
settings apply synchronously to the exact immutable revision; saving a draft still
requires review before activation. Setup and config editing do not test a provider
unless the user chooses the connection test. Local ignores unrelated broken
credentials. Read-only configuration keeps legacy scope precedence without writing
settings, creating state, or resolving credentials.

Removed the old configure wizard, preset/experiment and repair options, candidate
and replay dashboards, prompt inspection, session commands, automatic drain RPCs,
and hidden publication aliases. `commit-all` remains only the bounded Finish
compatibility delegate. Help presents `finish` as the publication action. Retained
history/activity and support recovery read old records and preserve exact legacy
provenance; they cannot start the retired engine.

Legacy archive tests now protect later edits through checkpoints and still verify
original captured bytes, native staging, HEAD, and live files. Finish CLI/worker/Git
scenarios cover staged refusal, approved overlay, later staging, immutable targets,
and crash recovery. The old AI cooldown/queue dashboard assertions are retired.

## Integration scenario replacements

The retained integration scenarios use owned subprocesses and isolated settings,
state, repositories, and credentials. They do not upgrade the installed runtime.

| Earlier scenario | Current proof |
|---|---|
| Adapter start/stop sessions and automatic publication | Every supported hook advances protection, saves exact file bytes, and keeps the same worker owner. Hooks remain quiet when protection is disabled or the repository is unregistered. No client/session table or branch commit is created. |
| Live settings queue reconciliation | An in-flight Finish keeps its approved configuration. The next Finish uses the newly approved revision. Invalid approval leaves the last valid revision intact, including after worker restart. |
| Settings wizard and provider checks on save | The actual config editor retains terminal layouts, resizing, cancellation, confirmation retry, masked credentials, and visible narrow-terminal approval. Saving a key and declining permission send no provider requests. The removed configure route has no replacement command. |
| Sensitive tracked-file reconciliation | Finish preserves the tracked sensitive file in the branch and leaves later private worktree edits intact while publishing eligible work. |
| Activity/explain/watch ledger | Historical decisions remain readable without schema changes. The actual activity watch streams a newly appended decision. Current Finish outcomes have separate workflow and fault coverage. |
| Populated automatic queue startup | An isolated archived database retains all old rows while the actual worker starts and protects new bytes. Fixture registration upgrades the schema before startup; this is not evidence that `on` performs a runtime upgrade. Migration tests cover that boundary separately. |
| Active dashboard and process lifecycle | Repository selection uses recent protection/Finish activity without writing state. Real PTY child exit, scratch self-test, session supervisor ownership/upgrade, and multi-repository admission remain covered. |

Independent review restored observable hook effects and short-terminal final
approval coverage. Exact integration checks are recorded in the Trekoon task;
hosted Linux/macOS shards own full integration and repeated stability coverage.

## Integration fault and regression replacements

The fault fixtures run the source binary with isolated configuration, temporary
repositories and an explicitly preinstalled test runtime. They use the real
supervisor, worker, HTTP/subprocess transports and Git transactions. Fresh
bootstrap is a separate workflow test. The macOS service shim rejects every
host-manager call except the exact legacy-label lookup returning absent.

| Retired integration file | Preserved scenario and current proof |
|---|---|
| `active_chain_self_heal_test.go` | Failed target stays frozen while later edits remain saved: `TestFinishFaultProviderFailureRetryKeepsFrozenTarget`; ambiguous old chains: `TestFinishMigrationLosslessRealLegacyFixtures`. |
| `ai_providers_test.go` | Local/Intent and both styles: `TestFinishWorkflowModesStylesAndCompleteGoals`; bounded HTTP correction, timeout and subprocess failure: the named `TestFinishFault` provider cases. Private TLS and commit-inspection utilities remain shared helpers. |
| `capture_resilience_test.go` | Original four 14–22 MB binary assets plus code/tests/docs: `TestFinishFaultLargeAssetsStayProtectedDuringProviderOutage`, including exact checkpoint blobs and native staging. |
| `commitall_test.go` | Complete selected target and neutral work: workflow mode/style cases; refusal, approved overlay and later staging: `TestFinishFaultStagedSelectionAndLaterStaging`. CLI unit tests retain the thin compatibility delegate. |
| `dead_branch_prune_test.go` | Deleted-branch captured versions survive activation and GC without retargeting: `TestFinishFaultDeletedLegacyBranchKeepsCapturedVersions`. Automatic stale-generation pruning is removed. |
| `flush_logical_test.go`, `intent_flush_test.go`, `lifecycle_test.go` | Many saves and hints protect work without AI, checks or branch commits; explicit finish/off and restart are covered by workflow and fault cases. Session flush acknowledgment and lifetime assertions are removed. |
| `intent_atomicity_test.go`, `intent_strategy_test.go` | Complete implementation/test/docs goals, intermediate trees and exact messages/order: workflow mode/style cases. Branch movement and second attachment refuse the full chain: `TestFinishFaultBranchMovementAndLinkedOwnership`. |
| `intent_planner_normalization_test.go`, `intent_planner_recovery_test.go` | Strict full-batch validation and one combined correction: `TestFinishFaultOneCombinedCorrection`, plus the Finish planner/validator package cases. Deferred candidate queues are removed. |
| `intent_self_healing_test.go`, `publication_self_healing_test.go` | Failed checks, provider outage and explicit frozen retry: fault cases. Old automatic retry/fallback and repair loops are removed. |
| `intent_history_worker_test.go`, `rewrite_commits_test.go` | Explicit saved history plans preserve authors, dates, membership, per-commit trees and neutral boundaries: both `TestFinishWorkflowHistory` cases. |
| `regressions_test.go` | Actual scanner privacy/symlink, tracked excluded deletion, unsafe Git protection, branch movement, canonical locks and process fingerprint checks are retained as named fault/watchdog cases. Repeated saves are workflow cases. Session refcounts, automatic per-save commits and empty-client termination are removed. |
| `self_heal_test.go`, `self_heal_blocked_test.go`, `squash_backlog_recovery_test.go` | Immutable old objects, rows, linked worktrees, staging and partial checkpoints: real migration fixtures. Support recovery still proves exact archive contents without retargeting. Runtime backlog repair is removed. |
| `self_publication_transition_test.go` | Canonical lock after state-directory movement and across linked worktrees: two `TestFinishFaultCanonicalWriter` cases. Actual prepared/post-CAS worker restart: `TestFinishFaultPreparedRestartBeforeAndAfterCAS`; old journal settlement: migration fixtures; remaining native transaction boundaries: Git publication tests. |
| `settings_experiment_test.go` | Experiments are removed. Approved settings and frozen request scope are covered by `settings_live_reload_test.go`; saved drafts do not activate themselves. |

The retained watcher tests now prove checkpoint protection at initial/runtime
watch-budget fallback, explicit disable and ordinary fsnotify wake without a
hook. The large ignored-tree case still creates 50,000 files, checks the real
scanner walks fewer than 1,000 eligible entries and bounds the worker barrier.
Hook latency still measures ten real IPC calls and proves each edit reaches a
checkpoint without a watcher or extra barrier. The shutdown probe preserves its
30-second bound and checks all burst files survive the final checkpoint.

`reliability_recover_test.go` and the two retained `fix_consolidated_test.go`
scenarios now invoke `support recover`/`support diagnose`. Their SQL builders
explicitly create archived tables; production writers are not restored. They
retain exact pair/archive membership, branch provenance, recovery refs, backups,
index/worktree preservation and selective generated-row cleanup. The old linked
capture self-heal assertion is covered by migration plus canonical-owner cases;
removed `recover`/`purge` alias warnings are covered by current help assertions.
`session_runtime_test.go` keeps child cleanup, watchdog retry/escalation, reused
PID refusal, concurrency limits and shared ownership checks. The opt-in benchmark
measures idle resources and later checkpoint latency while an explicit Finish
provider call is blocked.

Restart tests distinguish a running exchange from a prepared transaction. A
running exchange becomes failed on owned-worker restart and makes no automatic
provider call; explicit retry retains the same target. Prepared recovery resumes
only the saved full chain and reconciles the exact approved index identity.
Completed-client reconnection reads the durable result and creates no new commit.


## Final CLI and test-gate closure

The command tree is traced from `newRootCmd` and its current facade constructors.
Old constructor strings do not define public help when a facade replaces them.
`finish` is canonical; the visible first-release `commit-all` delegate retains
`--yes` consent for the exact staged selection. Retired root session, configure,
prompt, and publication aliases are not compatibility engines.

| Earlier assertion | Retained current scenario |
|---|---|
| Diagnose anchor/queue/circuit/automatic-retry guidance | `TestDiagnoseLegacyFailuresRemainPreservedWithoutAutonomousReplay` retains blocked, failed, and recorded-error rows with exact original HEAD/blob provenance and unchanged database bytes. Unknown archival counts remain unknown. Current request/coverage truth is covered by `TestFinishStatus*`. |
| Doctor automatic repair and provider credential/PATH checks | `TestDoctor_LegacyBlockedConflictRemainsPreserved` and `TestDoctor_LegacyFailedCaptureRemainsPreserved` report saved attention without mutation. `TestDoctor_AISavedIntentDoesNotReadCredentials` and `TestDoctor_AISavedSubprocessDoesNotInspectOrRunProvider` prove inspection does not read credentials or execute providers. |
| Watcher diagnostics hidden by the checkpoint projection | `TestDoctor_JSON_FsnotifyFields` and `TestDoctor_Human_FsnotifySection` retain exact saved watch counts, dropped events, and fallback reasons in both views. |
| Root activity, explain, logs, and rewrite help | `TestEventsErrorsAndCommandHelp`, `TestExplainValidationAndHelp`, `TestLogsCommandRegisteredWithHelp`, and `TestHistoryRewriteExplainsExplicitSavedPlanWorkflow` use current namespaces while keeping flags, privacy, and explicit saved-plan boundaries. |
| Setup requires a provider and probes on save | `TestSetupNonInteractiveDefaultsToOfflineLocal`, `TestSetupNonTTYUsesAccessibleWizard`, and `TestFinishRemovalIntentSetupKeepsConnectionTestOptional` retain real unattended/accessible flows with Local defaults and no required Intent provider probe. `TestSetupDryRunIntentIncludesExactTransportConsents` retains exact endpoint/privacy approval. |
| Removed stop-session deferral in a subdirectory lookup test | `TestRepoLookupCommandsCanonicalizeSubdirBeforeRegistryLookup` now passes the actual subdirectory to `off`, proves the exact registered root/state DB, and refuses disabling without a proven final checkpoint or matching runtime. Registry/state checksums and empty success output remain asserted. Successful owned-runtime off is retained in workflow integration. |
| Missing or unregistered log remediation | `TestLogsMissingLogReturnsActionableError` and `TestLogsUnregisteredRepoReturnsActionableError` point to `acd on`; missing logs retain doctor guidance. Real tail and appended-line follow tests remain. |
| Subprocess cleanup conflated with a one-second startup assumption | `TestFinishRemovalSubprocessCloseKillsDescendants` grants a five-second startup exchange but keeps a separate seven-second Close limit, real descendant termination, process reaping, and joined response-reader proof for both EOF behaviors. The stalled-exchange deadline test remains separate. |

The broad-gate timing inventory had 1,292 compiled entrypoints across 29 packages.
Three/four/six-shard exact-once checks covered every name. Timing weights use only
successful top-level case results from the recorded gates; failed or timed-out
cases never supply durations. Historical live integration measurements remain
unchanged, and 38 names were unmeasured at that gate. New tests use the default
estimated cost until measured. No cases,
repetitions, or safety assertions were dropped to meet the budget. Ubuntu real
shell integration installs and checks `zsh`.

Earlier failed full gates and their focused corrections remain in Trekoon
rather than being presented as successful checks. The final complete local gate
passed after source corrections, measured balancing, and cleanup of identified
orphan workers belonging only to isolated test executables. No installed runtime,
live repository state, manual branch/index mutation, push, or release was part of
this source execution.

## Final review fixes

Six material findings were resolved without restoring automatic publication.
These nine added regression entrypoints are discovered by the current shard
runner along with the earlier inventory; no test selector drops them.

| Finding | Final implementation and retained regression |
|---|---|
| Directory replaced by an outside or sensitive symlink during scanning | `daemon/protection_fs_unix.go` anchors enumeration, file reads, and symlink reads beneath the opened repository, rejecting symlink ancestors. `TestFinishProtectionRejectsAncestorSymlinkSwap` proves partial coverage, readable sibling protection, no secret Git objects, and unchanged HEAD/index/live content. |
| Symbolic or unlocked private checkpoint proof | `git/durability.go` requires direct pins before reuse and under the prepared native ref lock; `checkpoint/store.go` completes SQLite under that lock. `TestFinishPrivatePinsRejectSymbolicAliases`, `TestFinishPrivatePinRejectsPostCreateSymbolicReplacement`, `TestFinishCheckpointRecoveryRejectsSymbolicPins`, and `TestFinishCheckpointCompletionHoldsNativePinLock` retain alias, creation-race, recovery, and concurrent-mutation proof. |
| Malformed archived identity aborts preservation of valid objects | `migration/finish_cutover.go` aggregates invalid identities while pinning independently available objects. `TestFinishLegacyMalformedIdentityPreservesValidObjectsThroughGC` proves durable attention, no completed cutover, immutable original rows, native GC survival, and unchanged HEAD/index/worktree. |
| Retired publication-drain RPC routes | Supervisor validation and forwarding no longer accept them. `TestSupervisorRejectsRetiredPublicationRequests` rejects the routes before worker contact; the existing supported-method test covers current Finish/history methods. The first-release CLI `commit-all` delegate remains. |
| Reachable old commands and automatic replay advice | Activity, explain, and support recovery use current commands and distinguish archived decisions from new explicit finish work. `TestExplainArchivedDecisionsUseCurrentRecoveryCommands` complements the existing read-only missing-ledger and error/help cases. Historical detector strings and excluded-cache cleanup semantics remain. |
| Interrupted settings approval blocks later Save | A newly reviewed Save may replace the exact pending desired revision through the existing compare-and-swap. `TestFinishConfigEditorSaveSupersedesInterruptedApproval` covers first approval, pending/acknowledged requests, stale review refusal, immutable revisions, no provider probe, and applied-only Finish settings. |

Independent reviewers cleared every fix. Simplify removed stale absolute scanner
paths, centralized locked checkpoint completion, and kept migration error
aggregation within the existing pinning flow. Focused race checks passed, followed
by the real fresh-offline and staged-selection process cases. The complete local
gate passed in 180.565 seconds before these fixes; only affected checks were
repeated afterward. Hosted full integration and stress remain CI-owned.

## Stale code cleanup

The unused Git repair and reconstruction engines and the old history request
queue are removed, together with their obsolete engine tests. Current history
validation returns only an error; its membership, author, dependency, and source
proof checks remain. Reader tests still cover edited renames and root commits.

Unused checkpoint barrier APIs, drain projections, tracing helpers, wrappers,
and unread runtime metadata writes are also removed. Legacy request fixtures
still prove that archived queues cannot publish. Restore fixtures retain branch
generation checks, and archive recovery readers and migration evidence remain.
