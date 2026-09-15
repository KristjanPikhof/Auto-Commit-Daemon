# Legacy CLI cleanup and scenario coverage

## Reachability baseline

Baseline: `3285f423` on 2026-09-15. `deadcode` v0.50.0 found the same
134 unreachable CLI functions for Darwin/arm64 and Linux/amd64, starting at
`./cmd/acd`. The command tree includes the supported compatibility aliases.
The production entry point has no optional build tags; `integration` selects
the separate test package, not another product command tree.

Reproduce with the host-built tool (cross-compiling `go run` cannot execute a
Linux analyzer on macOS):

```sh
go install golang.org/x/tools/cmd/deadcode@v0.50.0
deadcode -json ./cmd/acd
GOOS=linux GOARCH=amd64 deadcode -json ./cmd/acd
```

Unreachable does not mean safe to delete without review. The cleanup removes
retired implementations and migrates their useful scenarios as follows.

## Removed implementation paths

| Retired path | Current owner or reason |
|---|---|
| Direct `runStart`, detached process spawning, polling and start-cache reads/writes | `newCompatStartCmd` opens an integration session through the supervisor. `acd on` owns explicit repository enablement. |
| Direct `runCommitAll`, CLI replay loops and their error/plan types | `runProductCommitAll` freezes a checkpoint target and follows worker-owned publication. |
| Direct `runDaemon`, old worktree resolver and option wrapper | The compatibility command delegates to `runRepositoryWorker`; option tests now call `buildDaemonRunOptionsWithID`. |
| Old start/stop/repository command constructors and lifecycle wrappers | The root tree uses the current product and compatibility constructors. Repository-manager tests call the same lifecycle service used by its current UI. |
| Old status constructor and status watch loop | Status is a snapshot; live repository monitoring belongs to `acd list`. |
| Direct wake/touch/flush and autodiscovery controllers | Current aliases send supervisor hints; retained canonical identity and Intent boundary helpers are used by the current protocol. |

Preserved: migration shutdown (`runStopRegistry`, `stopOneRepo`), repository
removal and management, cached-file deletion for old runtimes, supported aliases,
all schema migrations, and shared repository test fixtures. Shutdown tests now
call the production migration service instead of the unused `runStop` wrapper.
The short SHA display helper moved out of the deleted commit-all implementation.

## Scenario mapping

| Previous scenario | Current production coverage |
|---|---|
| Session opens once, repeat opens reuse the worker, edits publish and session close leaves protection active | `TestLifecycle_StartEditWakeCommitStop`, `TestLifecycle_StartTwiceSameSession` in `test/integration/lifecycle_test.go`. |
| Manual start or stop must not silently enable/disable a repository | New `TestCompatibilityLifecycleRequiresExplicitSession` executes the current root aliases and checks that no state directory appears. |
| A hook cannot opt an unregistered repository into protection | New `TestCompatibilityStartPreservesRepositoryOptIn`; `TestSupervisorWorkerEnvironmentRequiresEnabledRepository`; `TestPrepareControlRepositoryActivatesOnlyTarget`. |
| Canonical ownership, legacy state relocation and linked worktree contention | `TestDaemonLockCanonicalWriterContendsAndReacquires`, `TestDaemonLockStateMoveRetainsCanonicalWriter`, `TestDaemonLockLinkedWorktreeOwner`, and the mixed-version matrix in `internal/daemon/lock_test.go`. |
| Missing runtime or invalid activation must not spawn an old daemon | `TestControlOnMissingRuntimeRefusesBeforeActivation`, `TestControlOnMismatchedRuntimeRefusesBeforeActivation`, and `TestControlOnReturnsErrorAfterRenderingUnhealthyResult`. The last test no longer stubs an unreachable spawn function. |
| Daemon option wiring, fsnotify setting and log lifetime | All three `TestBuildDaemonRunOptions_*` scenarios retained against `buildDaemonRunOptionsWithID`. |
| Shutdown escalation, identity checking, newer schemas, cache cleanup and failed stop reporting | Existing `TestStop_*` and `TestStopAll_*` cases retained against the migration shutdown helpers; these are still required by setup and removal. |
| Explicit registration and canonical paths | `TestPrepareControlRepositoryActivatesOnlyTarget` and existing canonical lookup tests. Old init inserted/refreshed JSON fields are no longer a product contract. |
| Repository enable/disable idempotence and state preservation | Existing `TestRepoDisable_*`, `TestRepoEnable_*`, and `TestRepoLifecycle_UnknownRepoDoesNotCreateState` now exercise `applyRepoLifecycle`, also used by the current manager. |
| Repository removal dry-run, purge consent and missing paths | Existing `TestRepoRemove_*` scenarios retained against `runRepoRemoveWithInput`, which the registered command invokes. |
| Preview does not mutate Git/state or build a provider while a writer owns the repository | New `TestProductCommitAllDryRunPreservesRepository` runs the actual root command under a canonical writer lock and verifies database checksum, HEAD, status and recovery refs. |
| Noninteractive apply requires explicit consent | New `TestProductCommitAllNonInteractiveApplyRequiresYes`, for human and JSON modes. |
| Detached HEAD, live-worker publication, event ordering and deterministic Intent grouping | `TestCommitAllRefusesOnDetachedHEAD`, `TestCommitAllWorksWhileWorkerAlive`, `TestCommitAllEventStrategyOrdersByPath`, `TestCommitAllIntentStrategyDeterministic` in the production integration suite. |
| Branch movement after selecting work | `TestCheckpointBarrierRefusesBranchChangeWhileWaiting`, `TestFreezePublicationDrainTargetRequiresCheckpointHead`, `TestFreezePublicationDrainTargetUsesCheckpointPair`. |
| Staging safety and a frozen target across restart | `TestCommitAllCheckpointBarrierRejectsStagedIndexBeforeWake`, `TestCommitAllCheckpointBarrierConsumesStagedAfterCheckpoint`, `TestPublicationSelfHealingStagedRestartAndFrozenTarget`. |
| Recovery of old capture pairs and missing objects | `TestRecoveryChainAcceptsProvenLaterBaseReset`, `TestRecoveryChainRejectsUnprovenLaterBaseReset`, `TestReplay_ReconcileMissingObjectLeavesChainUntouched`, and `TestReconcile_ProtectsRecoveryRefThroughStateTransition`. |
| Provider health reuse, AI messages and restart | `TestCommitAllIntentReplansCachedWaitAfterRestart`, `TestIntentCandidateEnginePublishesAfterMessageRecoveryAcrossRestart`, `TestResolveIntentReplayConfigDoesNotDegradeSemanticProvider`. |
| Partial progress, completed recovery and stalled target output | Retained `TestCommitAll_IncompleteTargetRendersThenExitsThree`, `TestCommitAllReconnectSelectsOnlyTheCurrentWorktreeDrain`, `TestPublicationDrainStatusDistinguishesRecoveredTarget`, and `TestCheckpointBarrierReturnsMeasuredFinalDrainProgress`. |
| Old CLI replay-pass estimates, recapture-loop counters, and an exclusive CLI writer lock | Removed as obsolete implementation behavior. The worker owns publication and can continue it after the CLI exits; `TestCommitAllDrainDetachAndReattachKeepsSameOperation` covers that contract. |
| Start cache TTL, cache parsing/round trips and post-spawn polling | Removed as obsolete implementation behavior. Current aliases use supervisor IPC and durable client registration. Migration cache deletion remains tested. |
| `status --watch` interval validation | Removed with the unregistered watch loop. Current list watch cancellation and interval tests remain. |

This is a mapping of supported scenarios, not a claim that the complete
integration suite was run during cleanup. Validation results are recorded below.

## Deleted test inventory

The following tests exercised the retired implementations. Refer to the scenario
table above for the retained production boundary or obsolete-contract reason.

### commitall_test.go

- `TestCommitAll_RefusesDetachedHEAD`
- `TestCommitAll_RefusesGitOperationInProgress`
- `TestCommitAll_RefusesManualPauseMarker`
- `TestCommitAll_RefusesWhileDaemonLockHeld`
- `TestCommitAll_DryRunAllowedWhileDaemonLockHeld`
- `TestCommitAll_CleanNoOpAllowedWhileDaemonLockHeld`
- `TestCommitAll_DeclineAllowedWhileDaemonLockHeld`
- `TestCommitAll_CleanWorktreeNoOp`
- `TestCommitAll_DryRunNeverCommits`
- `TestCommitAll_PreviewAndDeclineDoNotBuildProvider`
- `TestCommitAll_JSONRequiresYesWhenInteractive`
- `TestCommitAll_RefusesAllGitOperationMarkers`
- `TestCommitAll_YesSkipsPromptDoesNotReadStdin`
- `TestCommitAll_JSONYesEmitsValidJSON`
- `TestCommitAllEstimatePasses_Boundaries`
- `TestCommitAll_DryRunWithPendingPreservesHEAD`
- `TestCommitAll_DryRunReportsPreexistingPairWithoutReconciling`
- `TestCommitAll_PreservesBarrierThenCommitsDirtyWork`
- `TestCommitAll_PreservesNonActivePairBeforeActiveDirtyWork`
- `TestCommitAll_PreflightRecoveryLeavesGitStateUntouched`
- `TestCommitAll_MissingPreexistingObjectFailsClosed`
- `TestCommitAll_RecaptureLoopFailsWhenRecoveryDoesNotConverge`
- `TestCommitAll_RecaptureLoopReplaysFreshCapture`
- `TestCommitAll_RechecksHEADAfterConfirmation`
- `TestCommitAll_AcquiresDaemonLockAfterConfirmation`
- `TestCommitAll_StopsBeforeRecoveryWhenPauseAppears`
- `TestCommitAll_StopsBeforeCaptureWhenPauseAppears`
- `TestCommitAll_RechecksGitOperationBeforeEveryReplayPass`
- `TestCommitAll_RefusesOrphanBranch`
- `TestCommitAll_UserDeclineExitsNonZero`
- `TestFinishCommitAll_IncompleteReturnsTypedError`
- `TestFinishCommitAll_RecoveredThenDrainedSucceeds`
- `TestFinishCommitAllReplay_RendersPartialProgress`
- `TestCommitAll_RendersPartialPreexistingRecovery`
- `TestCommitAllReplayLoop_ReusesPlannerHealthAfterTransportFailure`
- `TestCommitAllReplayLoop_UsesProviderMessageFn`
- `TestCommitAllReplayLoop_PreservesPartialSummaryOnError`
- `TestCommitAllReplayLoop_ZeroProgressEscape`

### daemon_test.go

- `TestResolveDaemonWorktreeCanonicalizesSubdirectory`

### repo_autodiscovery_test.go

- `TestStart_AutodiscoveryDisabledHookUnregisteredSkipsWithoutState`
- `TestStart_DisabledRepoHookSkipsEvenWhenAutodiscoveryEnabled`
- `TestStart_DisabledRepoManualReportsEnableGuidance`
- `TestStart_RechecksDisabledAfterControlLockWait`
- `TestStart_ManualUnregisteredRequiresRepoOn`
- `TestStart_AutodiscoveryDisabledRegisteredRepoWorks`

### repo_test.go

- `TestRepoInit_JSONFromSubdir`
- `TestRepoInit_IdempotentAlreadyRegistered`
- `TestRepoInit_NonGitDirFails`

### start_shortcircuit_test.go

- `TestEvaluateShortCircuit_Matrix`
- `TestEvaluateShortCircuit_FingerprintMatchAndMismatch`
- `TestReadStartCache_TolerantOfBadInputs`
- `TestWriteStartCache_RoundTripCreatesParent`
- `TestWriteStartCache_ConcurrentWritersProduceParseableFile`
- `TestReadStartCache_LegacyV1Rejected`
- `TestTryShortCircuitStart_HappyPath`
- `TestTryShortCircuitStart_FingerprintMismatchEscalates`
- `TestTryShortCircuitStart_NoCacheEscalates`
- `TestRunStart_RepeatedActiveHooks_ShortCircuit`
- `TestRunStart_DisabledRepoRejectsStartCacheHotPath`
- `TestRunStart_DifferentSession_NoShortCircuit`
- `TestMultiSession_PerSessionCacheKeepsBothOnHotPath`
- `TestRunStart_LegacySubdirRegistryRowDoesNotRewriteConsent`
- `TestRunStart_CanonicalRegistryRowUsesEarlyShortCircuitFromSubdir`
- `TestShortCircuitNow_Overridable`

### start_test.go

- `TestStartCanonicalWriterStateMoveRefusesUnknownOwner`
- `TestStartCanonicalWriterStartupStateConverges`
- `TestStartLinkedWorktreeOwnerRefusesSecondWriter`
- `TestStartLinkedWorktreeStalePIDDoesNotClaimOwner`
- `TestStartDaemonLockMixedVersionRefusalIsActionable`
- `TestStart_FirstCall_StartsDaemon`
- `TestStart_DefaultWatchPIDDisabledPersistsNullWatchPID`
- `TestStart_AlreadyExitedWatchPIDPersistsNullWatchPID`
- `TestStart_DuplicateSession_NoRespawn`
- `TestStart_ManualEmptySessionUsesDeterministicHumanSession`
- `TestStart_EmptySessionWithHarnessRequiresExplicitSession`
- `TestStart_RegistryUpdated`
- `TestStart_CanonicalizesSubdirectoryForIdentityAndRegistry`
- `TestStart_LegacySubdirRegistryRowRequiresActivation`
- `TestStart_LinkedWorktreeGitFileCanonicalizesSubdirAndStateDB`
- `TestStart_NonWorktreeFailsBeforeRegistryOrDaemonSpawn`
- `TestStart_DetachedHEADRefused`
- `TestStart_RereadsDaemonStateAfterSpawnPollDeadline`
- `TestStart_TenConcurrentReportsSurvivingDaemonPID`

### status_test.go

- `TestStatusWatchRejectsNonPositiveInterval`

## Projection fixtures

Historical status/list serialization now lives only in
`internal/cli/projection_fixture_test.go`. Its explicitly named
`writeStatusProjectionFixture` and `writeListProjectionFixture` adapters call the
current `buildStatusReport` and `summarizeRepo` services. They preserve schema,
redaction, durable recovery and detailed projection assertions without shipping
a second command implementation. Product/control tests cover the current public
presentation. The old watch loop and its two tests were removed; current
`TestProductListWatch*` cases cover refresh and cancellation.

## Validation

- Darwin/arm64 and Linux/amd64 matched at 134 unreachable CLI functions before
  cleanup; the first removal pass reduced both to 60. A final pass follows the
  fixture/controller migration.
- Focused lifecycle, repository, commit-all and compatibility tests passed with
  the race detector: 23.115 seconds.
- Current-command preview and consent tests passed with the race detector:
  3.297 seconds.
- Residual shutdown/compatibility/signal tests passed: 9.525 seconds.
- The full CLI race suite finished in 237.572 seconds. Failures identified
  assertions against concurrently changed status text, metrics and setup help.
  The new preview fixture's missing initial commit was fixed and verified with
  its focused test. The combined branch gate must pass before completion.
- `git diff --check` passed after the structural cleanup.

## Additional removed controller tests

Direct hint-controller tests covered retired SQLite and signal mutations. The
current root compatibility opt-in test, worker hint tests, Intent boundary tests,
and lifecycle/Intent flush integration tests cover the supported IPC path.
`TestSignalProcess*` tests remain because migration shutdown still sends signals.

### repo_autodiscovery_test.go

- `TestFlushLogical_AutodiscoveryDisabledUnregisteredRefusesUnknownSessionWithoutState`
- `TestStart_AutodiscoveryDisabledHookUnregisteredSkipsWithoutState`
- `TestStart_AutodiscoveryDisabledRegisteredRepoWorks`
- `TestStart_DisabledRepoHookSkipsEvenWhenAutodiscoveryEnabled`
- `TestStart_DisabledRepoManualReportsEnableGuidance`
- `TestStart_ManualUnregisteredRequiresRepoOn`
- `TestStart_RechecksDisabledAfterControlLockWait`
- `TestWakeTouchFlush_AutodiscoveryDisabledUnregisteredSkipsWithoutState`
- `TestWakeTouchFlush_DisabledRepoSkipsWithoutState`
### flush_test.go

- `TestFlush_HeartbeatOnlyDoesNotEnqueueOrSignal`
- `TestFlush_HeartbeatOnlyStillLazyRegisters`
- `TestFlush_HelpListsLogicalFlag`
- `TestFlush_LogicalEnqueuesAndSignals`
- `TestFlush_LogicalRefusesOnDetachedHEAD`
- `TestFlush_LogicalRefusesOnGitOperation`
- `TestFlush_LogicalRefusesOnManualPause`
- `TestFlush_LogicalRequiresRegisteredClient`
- `TestFlush_PropagatesUnexpectedLockError`
- `TestFlush_SessionIDRequired`
- `TestFlush_SkippedJSONOmitsZeroLastSeenTS`
- `TestFlush_SkipsWhenControlLockHeld`
### touch_test.go

- `TestTouchCommandExposesSoftBoundaryFlag`
- `TestTouch_LazyRegistersUnknownSession`
- `TestTouch_RefreshesLastSeenOnly`
- `TestTouch_SkipsWhenControlLockHeld`
- `TestTouch_SoftBoundaryPersistsRepoWideEpochAndSignals`
### wake_test.go

- `TestSignalDaemonSettingsActivationDoesNotEnqueueWake`
- `TestSignalDaemonSettingsActivationStoppedAndFingerprintRequired`
- `TestWake_LazyRegisterIdempotent`
- `TestWake_PropagatesUnexpectedLockError`
- `TestWake_RefreshesAndSignals`
- `TestWake_SkipsWhenControlLockHeld`
### list_test.go

- `TestListWatch_AlreadyCanceledContextReturnsNil`
- `TestListWatch_RendersMultipleSnapshotsAndStopsOnCancel`
