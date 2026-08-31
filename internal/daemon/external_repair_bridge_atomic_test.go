package daemon

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/ai"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

func TestRunStartupExternalRepairBridgeAtomicallyAdoptsLiveHead(t *testing.T) {
	t.Setenv(ai.EnvCommitStrategy, string(ai.CommitStrategyEvent))
	f := newExternalRepairBridgeFixture(t, externalRepairBridgeFixtureOptions{})
	ctx := context.Background()
	registerLiveClient(t, f.capture.db)
	_ = seedExternalBridgeAtomicBaseline(t, ctx, f)

	const laterPath = "captured-before-startup.txt"
	laterBody := []byte("protected later startup edit\n")
	laterBlob, err := git.HashObjectStdin(ctx, f.capture.dir, laterBody)
	if err != nil {
		t.Fatalf("hash later startup edit: %v", err)
	}
	laterSeq := appendRecoveryEvent(
		t, ctx, f.capture, f.source, state.CaptureOp{
			Op: "create", Path: laterPath,
			AfterOID:  oidValue(laterBlob),
			AfterMode: oidValue(git.RegularFileMode),
		})
	if err := os.WriteFile(
		filepath.Join(f.capture.dir, laterPath), laterBody, 0o644); err != nil {
		t.Fatalf("write later startup edit: %v", err)
	}
	if err := state.UpsertShadowPath(ctx, f.capture.db, state.ShadowPath{
		BranchRef:        f.capture.cctx.BranchRef,
		BranchGeneration: f.capture.cctx.BranchGeneration,
		Path:             laterPath,
		Operation:        "create",
		Mode:             oidValue(git.RegularFileMode),
		OID:              oidValue(laterBlob),
		BaseHead:         f.source,
		Fidelity:         "rescan",
	}); err != nil {
		t.Fatalf("seed later startup shadow: %v", err)
	}
	statusBefore := gitRawOutput(t, ctx, f.capture.dir, "status", "--short")

	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	firstPass := make(chan struct{})
	var firstPassOnce sync.Once
	go func() {
		done <- Run(runCtx, Options{
			RepoPath:    f.capture.dir,
			GitDir:      f.capture.gitDir,
			DB:          f.capture.db,
			Scheduler:   fastScheduler(),
			BootGrace:   30 * time.Second,
			WakeCh:      make(chan struct{}, 8),
			ShutdownCh:  make(chan struct{}, 1),
			SkipSignals: true,
			MessageFn:   DeterministicMessage,
			replay: func(_ context.Context, _ string, _ *state.DB,
				cctx CaptureContext, _ ReplayOpts) (ReplaySummary, error) {
				return ReplaySummary{BaseHead: cctx.BaseHead, Skipped: true}, nil
			},
			afterRunLoopWorkDecision: func(_, _ bool) {
				firstPassOnce.Do(func() { close(firstPass) })
			},
		})
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case runErr := <-done:
			if runErr != nil {
				t.Fatalf("Run: %v", runErr)
			}
		case <-time.After(10 * time.Second):
			t.Fatal("Run did not stop after cancellation")
		}
	})
	select {
	case <-firstPass:
	case runErr := <-done:
		t.Fatalf("Run stopped before its first pass: %v", runErr)
	case <-time.After(10 * time.Second):
		t.Fatal("Run did not complete its first startup-backed pass")
	}

	assertExternalBridgePublishedPrefix(t, ctx, f)
	assertExternalBridgeEventsPending(t, ctx, f.capture.db, []int64{laterSeq})
	if got, ok, err := state.MetaGet(
		ctx, f.capture.db, MetaKeyBranchHead); err != nil || !ok || got != f.live {
		t.Fatalf("startup branch.head=(%q,%v,%v) want %s", got, ok, err, f.live)
	}
	wantToken := branchTokenRev(f.live, f.capture.cctx.BranchRef)
	if got, ok, err := state.MetaGet(
		ctx, f.capture.db, MetaKeyBranchToken); err != nil || !ok || got != wantToken {
		t.Fatalf("startup branch token=(%q,%v,%v) want %q", got, ok, err, wantToken)
	}
	marker := ShadowBootstrappedKey(
		f.capture.cctx.BranchRef, f.capture.cctx.BranchGeneration)
	if got, ok, err := state.MetaGet(
		ctx, f.capture.db, marker); err != nil || !ok || got != "1" {
		t.Fatalf("startup shadow marker=(%q,%v,%v) want (1,true,nil)", got, ok, err)
	}
	laterShadow, ok, err := state.GetShadowPath(
		ctx, f.capture.db, f.capture.cctx.BranchRef,
		f.capture.cctx.BranchGeneration, laterPath)
	if err != nil || !ok || laterShadow.Operation != "create" ||
		laterShadow.BaseHead != f.live || !laterShadow.OID.Valid ||
		laterShadow.OID.String != laterBlob {
		t.Fatalf("startup later shadow=(%+v,%v,%v) want preserved at live head",
			laterShadow, ok, err)
	}
	externalBlob, err := git.LsTreeBlobOID(ctx, f.capture.dir, f.live, "later.txt")
	if err != nil {
		t.Fatalf("read descendant-only blob: %v", err)
	}
	externalShadow, ok, err := state.GetShadowPath(
		ctx, f.capture.db, f.capture.cctx.BranchRef,
		f.capture.cctx.BranchGeneration, "later.txt")
	if err != nil || !ok || externalShadow.Operation != "bootstrap" ||
		externalShadow.BaseHead != f.live || !externalShadow.OID.Valid ||
		externalShadow.OID.String != externalBlob {
		t.Fatalf("startup descendant shadow=(%+v,%v,%v) want live bootstrap %s",
			externalShadow, ok, err, externalBlob)
	}
	if _, ok, err := state.GetShadowPath(
		ctx, f.capture.db, f.capture.cctx.BranchRef,
		f.capture.cctx.BranchGeneration,
		"old-shadow-evidence.txt"); err != nil || ok {
		t.Fatalf("stale startup shadow survived adoption: ok=%v err=%v", ok, err)
	}
	assertExternalBridgeCaptureCount(t, ctx, f.capture.db, laterPath, 1)
	assertExternalBridgeCaptureCount(t, ctx, f.capture.db, "later.txt", 0)
	var snapshotCount, snapshotEvents int
	if err := f.capture.db.ReadSQL().QueryRowContext(ctx, `
SELECT COUNT(*), COALESCE(SUM(event_count), 0)
FROM recovery_snapshots
WHERE outcome=? AND commit_oid=?`, state.EventStatePublished, f.target).Scan(
		&snapshotCount, &snapshotEvents); err != nil {
		t.Fatalf("read startup recovery snapshot: %v", err)
	}
	if snapshotCount != 1 || snapshotEvents != len(f.pendingSeqs) {
		t.Fatalf("startup recovery snapshots=(%d,%d) want (1,%d)",
			snapshotCount, snapshotEvents, len(f.pendingSeqs))
	}
	if head, err := git.RevParse(ctx, f.capture.dir, "HEAD"); err != nil || head != f.live {
		t.Fatalf("startup HEAD=%q err=%v want untouched %s", head, err, f.live)
	}
	if statusAfter := gitRawOutput(
		t, ctx, f.capture.dir, "status", "--short"); statusAfter != statusBefore {
		t.Fatalf("startup reconciliation changed worktree/index:\nbefore=%q\nafter=%q",
			statusBefore, statusAfter)
	}
}

func TestRunStartupExternalRepairBridgeResamplesHeadBeforeProtection(
	t *testing.T,
) {
	t.Setenv(ai.EnvCommitStrategy, string(ai.CommitStrategyEvent))
	f := newExternalRepairBridgeFixture(t, externalRepairBridgeFixtureOptions{})
	ctx := context.Background()
	registerLiveClient(t, f.capture.db)
	_ = seedExternalBridgeAtomicBaseline(t, ctx, f)

	const featureRef = "refs/heads/startup-bridge-feature"
	if err := git.UpdateRef(ctx, f.capture.dir, featureRef, f.live, ""); err != nil {
		t.Fatalf("seed startup feature ref: %v", err)
	}
	staleIndexOID, err := git.LsTreeBlobOID(
		ctx, f.capture.dir, f.source, "restored.txt")
	if err != nil {
		t.Fatalf("read stale restored blob: %v", err)
	}
	if _, err := git.Run(ctx, git.RunOpts{Dir: f.capture.dir},
		"update-index", "--add", "--cacheinfo", git.RegularFileMode,
		staleIndexOID, "restored.txt"); err != nil {
		t.Fatalf("seed stale live index: %v", err)
	}
	statusBefore := gitRawOutput(t, ctx, f.capture.dir, "status", "--short")
	if statusBefore == "" {
		t.Fatal("stale live-index fixture is unexpectedly clean")
	}
	var checkpointsBefore int
	if err := f.capture.db.ReadSQL().QueryRowContext(
		ctx, `SELECT COUNT(*) FROM checkpoints`,
	).Scan(&checkpointsBefore); err != nil {
		t.Fatalf("count checkpoints before startup: %v", err)
	}

	adopted := make(chan struct{})
	pausedPassDone := make(chan struct{})
	tokenErrorPassDone := make(chan struct{})
	failTokens := make(chan struct{})
	operationMarker := filepath.Join(f.capture.gitDir, "MERGE_HEAD")
	var adoptedOnce, pausedPassOnce, tokenErrorPassOnce sync.Once
	var passMu sync.Mutex
	postAdoptionPasses := 0
	replayCalls := 0
	runCtx, cancel := context.WithCancel(ctx)
	stopped := make(chan struct{})
	var runErr error
	go func() {
		defer close(stopped)
		runErr = Run(runCtx, Options{
			RepoPath:    f.capture.dir,
			GitDir:      f.capture.gitDir,
			DB:          f.capture.db,
			Scheduler:   fastScheduler(),
			BootGrace:   30 * time.Second,
			WakeCh:      make(chan struct{}, 8),
			ShutdownCh:  make(chan struct{}, 1),
			SkipSignals: true,
			MessageFn:   DeterministicMessage,
			branchGenerationToken: func(
				tokenCtx context.Context, repo string,
			) (string, error) {
				select {
				case <-failTokens:
					return "", errors.New("injected startup token failure")
				default:
					return BranchGenerationToken(tokenCtx, repo)
				}
			},
			replay: func(_ context.Context, _ string, _ *state.DB,
				cctx CaptureContext, _ ReplayOpts,
			) (ReplaySummary, error) {
				passMu.Lock()
				replayCalls++
				passMu.Unlock()
				return ReplaySummary{BaseHead: cctx.BaseHead, Skipped: true}, nil
			},
			afterExternalRepairBridgeAdoption: func(cctx CaptureContext) {
				if cctx.BranchRef != f.capture.cctx.BranchRef ||
					cctx.BaseHead != f.live {
					t.Errorf("startup adopted context=%+v want (%s,%s)",
						cctx, f.capture.cctx.BranchRef, f.live)
				}
				if _, err := git.Run(ctx, git.RunOpts{Dir: f.capture.dir},
					"symbolic-ref", "HEAD", featureRef); err != nil {
					t.Errorf("switch startup HEAD after bridge adoption: %v", err)
				}
				if err := os.WriteFile(
					operationMarker, []byte(f.live+"\n"), 0o600,
				); err != nil {
					t.Errorf("create startup operation marker: %v", err)
				}
				adoptedOnce.Do(func() { close(adopted) })
			},
			afterRunLoopWorkDecision: func(_, _ bool) {
				select {
				case <-adopted:
					passMu.Lock()
					postAdoptionPasses++
					pass := postAdoptionPasses
					passMu.Unlock()
					if pass == 1 {
						pausedPassOnce.Do(func() { close(pausedPassDone) })
					} else if pass == 2 {
						tokenErrorPassOnce.Do(func() { close(tokenErrorPassDone) })
					}
				default:
				}
			},
		})
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-stopped:
			if runErr != nil {
				t.Fatalf("Run: %v", runErr)
			}
		case <-time.After(10 * time.Second):
			t.Fatal("Run did not stop after cancellation")
		}
	})

	select {
	case <-pausedPassDone:
	case <-stopped:
		t.Fatalf("Run stopped before startup paused pass: %v", runErr)
	case <-time.After(10 * time.Second):
		t.Fatal("startup bridge paused pass did not finish")
	}
	if err := os.Remove(operationMarker); err != nil {
		t.Fatalf("remove startup operation marker: %v", err)
	}
	close(failTokens)
	select {
	case <-tokenErrorPassDone:
	case <-stopped:
		t.Fatalf("Run stopped before startup token-error pass: %v", runErr)
	case <-time.After(10 * time.Second):
		t.Fatal("startup bridge token-error pass did not finish")
	}
	cancel()
	select {
	case <-stopped:
		if runErr != nil {
			t.Fatalf("Run: %v", runErr)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Run did not stop after startup bridge pass")
	}

	passMu.Lock()
	gotReplayCalls := replayCalls
	passMu.Unlock()
	if gotReplayCalls != 0 {
		t.Fatalf("startup replay calls before successful branch resample=%d want 0",
			gotReplayCalls)
	}
	var checkpointsAfter int
	if err := f.capture.db.ReadSQL().QueryRowContext(
		ctx, `SELECT COUNT(*) FROM checkpoints`,
	).Scan(&checkpointsAfter); err != nil {
		t.Fatalf("count checkpoints after startup: %v", err)
	}
	if checkpointsAfter != checkpointsBefore {
		t.Fatalf("startup protection checkpointed stale identity: %d -> %d",
			checkpointsBefore, checkpointsAfter)
	}
	entries, err := git.LsFilesStaged(ctx, f.capture.dir, "restored.txt")
	if err != nil {
		t.Fatalf("read startup live index: %v", err)
	}
	if len(entries) != 1 || entries[0].OID != staleIndexOID {
		t.Fatalf("startup live index was repaired before resample: %+v", entries)
	}
	if statusAfter := gitRawOutput(
		t, ctx, f.capture.dir, "status", "--short",
	); statusAfter != statusBefore {
		t.Fatalf("startup bridge changed worktree/index:\nbefore=%q\nafter=%q",
			statusBefore, statusAfter)
	}
	if ref, err := git.RunBranchRef(
		ctx, f.capture.dir); err != nil || ref != featureRef {
		t.Fatalf("startup HEAD ref=(%q,%v), want %q", ref, err, featureRef)
	}
	assertExternalBridgePublishedPrefix(t, ctx, f)
}

func TestRunExternalRepairBridgeResamplesHeadBeforeProtection(t *testing.T) {
	t.Setenv(ai.EnvCommitStrategy, string(ai.CommitStrategyEvent))
	f := newExternalRepairBridgeFixture(t, externalRepairBridgeFixtureOptions{})
	ctx := context.Background()
	registerLiveClient(t, f.capture.db)
	_ = seedExternalBridgeAtomicBaseline(t, ctx, f)

	// Boot at the recorded source so the bridge is exercised by a runtime
	// branch transition rather than by startup reconciliation.
	if err := git.UpdateRef(
		ctx, f.capture.dir, f.capture.cctx.BranchRef, f.source, f.live,
	); err != nil {
		t.Fatalf("restore source branch before run: %v", err)
	}
	if _, err := git.Run(ctx, git.RunOpts{Dir: f.capture.dir},
		"reset", "--hard", f.source); err != nil {
		t.Fatalf("restore source worktree before run: %v", err)
	}
	const featureRef = "refs/heads/bridge-feature"
	if err := git.UpdateRef(ctx, f.capture.dir, featureRef, f.live, ""); err != nil {
		t.Fatalf("seed feature ref: %v", err)
	}
	var checkpointsBefore int
	if err := f.capture.db.ReadSQL().QueryRowContext(
		ctx, `SELECT COUNT(*) FROM checkpoints`,
	).Scan(&checkpointsBefore); err != nil {
		t.Fatalf("count checkpoints before run: %v", err)
	}

	checkHook, checkEntered, releaseCheck := oneShotBranchTokenCheckGate()
	defer releaseCheck()
	adopted := make(chan struct{})
	firstPassDone := make(chan struct{})
	pausedPassDone := make(chan struct{})
	tokenErrorPassDone := make(chan struct{})
	failTokens := make(chan struct{})
	var adoptedOnce, firstPassOnce, pausedPassOnce, tokenErrorPassOnce sync.Once
	var passMu sync.Mutex
	postAdoptionPasses := 0
	replayCalls := 0
	operationMarker := filepath.Join(f.capture.gitDir, "MERGE_HEAD")
	runCtx, cancel := context.WithCancel(ctx)
	stopped := make(chan struct{})
	var runErr error
	go func() {
		defer close(stopped)
		runErr = Run(runCtx, Options{
			RepoPath:               f.capture.dir,
			GitDir:                 f.capture.gitDir,
			DB:                     f.capture.db,
			Scheduler:              fastScheduler(),
			BootGrace:              30 * time.Second,
			WakeCh:                 make(chan struct{}, 8),
			ShutdownCh:             make(chan struct{}, 1),
			SkipSignals:            true,
			MessageFn:              DeterministicMessage,
			beforeBranchTokenCheck: checkHook,
			branchGenerationToken: func(tokenCtx context.Context, repo string) (string, error) {
				select {
				case <-failTokens:
					return "", errors.New("injected post-bridge token failure")
				default:
					return BranchGenerationToken(tokenCtx, repo)
				}
			},
			replay: func(_ context.Context, _ string, _ *state.DB,
				cctx CaptureContext, _ ReplayOpts,
			) (ReplaySummary, error) {
				passMu.Lock()
				replayCalls++
				passMu.Unlock()
				return ReplaySummary{BaseHead: cctx.BaseHead, Skipped: true}, nil
			},
			afterExternalRepairBridgeAdoption: func(cctx CaptureContext) {
				if cctx.BranchRef != f.capture.cctx.BranchRef ||
					cctx.BaseHead != f.live {
					t.Errorf("adopted context=%+v want (%s,%s)",
						cctx, f.capture.cctx.BranchRef, f.live)
				}
				if _, err := git.Run(ctx, git.RunOpts{Dir: f.capture.dir},
					"symbolic-ref", "HEAD", featureRef); err != nil {
					t.Errorf("switch HEAD after bridge adoption: %v", err)
				}
				// Keep the following pass from sampling the branch token. The
				// protection deferral must survive this pause rather than reset
				// merely because a new loop iteration began.
				if err := os.WriteFile(
					operationMarker, []byte(f.live+"\n"), 0o600,
				); err != nil {
					t.Errorf("create operation marker after adoption: %v", err)
				}
				adoptedOnce.Do(func() { close(adopted) })
			},
			afterRunLoopWorkDecision: func(_, _ bool) {
				select {
				case <-adopted:
					passMu.Lock()
					postAdoptionPasses++
					pass := postAdoptionPasses
					passMu.Unlock()
					if pass == 1 {
						firstPassOnce.Do(func() { close(firstPassDone) })
					} else if pass == 2 {
						pausedPassOnce.Do(func() { close(pausedPassDone) })
					} else if pass == 3 {
						tokenErrorPassOnce.Do(func() { close(tokenErrorPassDone) })
					}
				default:
				}
			},
		})
	}()
	t.Cleanup(func() {
		cancel()
		releaseCheck()
		select {
		case <-stopped:
			if runErr != nil {
				t.Fatalf("Run: %v", runErr)
			}
		case <-time.After(10 * time.Second):
			t.Fatal("Run did not stop after cancellation")
		}
	})
	waitForBranchTokenCheckGate(t, checkEntered)

	if err := git.UpdateRef(
		ctx, f.capture.dir, f.capture.cctx.BranchRef, f.live, f.source,
	); err != nil {
		t.Fatalf("advance branch to live descendant: %v", err)
	}
	if _, err := git.Run(ctx, git.RunOpts{Dir: f.capture.dir},
		"reset", "--hard", f.live); err != nil {
		t.Fatalf("advance worktree to live descendant: %v", err)
	}
	statusBefore := gitRawOutput(t, ctx, f.capture.dir, "status", "--short")
	releaseCheck()

	select {
	case <-firstPassDone:
	case <-stopped:
		t.Fatalf("Run stopped before bridge adoption pass: %v", runErr)
	case <-time.After(10 * time.Second):
		t.Fatal("runtime bridge adoption pass did not finish")
	}
	select {
	case <-pausedPassDone:
	case <-stopped:
		t.Fatalf("Run stopped before paused follow-up pass: %v", runErr)
	case <-time.After(10 * time.Second):
		t.Fatal("paused follow-up pass did not finish")
	}
	if err := os.Remove(operationMarker); err != nil {
		t.Fatalf("remove operation marker: %v", err)
	}
	close(failTokens)
	select {
	case <-tokenErrorPassDone:
	case <-stopped:
		t.Fatalf("Run stopped before token-error follow-up pass: %v", runErr)
	case <-time.After(10 * time.Second):
		t.Fatal("token-error follow-up pass did not finish")
	}
	cancel()
	select {
	case <-stopped:
		if runErr != nil {
			t.Fatalf("Run: %v", runErr)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Run did not stop after bridge adoption pass")
	}
	passMu.Lock()
	gotReplayCalls := replayCalls
	passMu.Unlock()
	if gotReplayCalls != 0 {
		t.Fatalf("replay calls before successful branch resample=%d want 0",
			gotReplayCalls)
	}

	var checkpointsAfter, stalePair int
	if err := f.capture.db.ReadSQL().QueryRowContext(
		ctx, `SELECT COUNT(*) FROM checkpoints`,
	).Scan(&checkpointsAfter); err != nil {
		t.Fatalf("count checkpoints after adoption: %v", err)
	}
	if err := f.capture.db.ReadSQL().QueryRowContext(ctx, `
SELECT COUNT(*) FROM checkpoints WHERE observed_ref=? AND observed_head=?`,
		f.capture.cctx.BranchRef, f.live).Scan(&stalePair); err != nil {
		t.Fatalf("count stale-pair checkpoints: %v", err)
	}
	if checkpointsAfter != checkpointsBefore || stalePair != 0 {
		t.Fatalf("same-pass protection checkpointed stale identity: checkpoints=%d->%d stale_pair=%d",
			checkpointsBefore, checkpointsAfter, stalePair)
	}
	if ref, err := git.RunBranchRef(ctx, f.capture.dir); err != nil || ref != featureRef {
		t.Fatalf("HEAD ref after adoption=(%q,%v), want %q", ref, err, featureRef)
	}
	if head, err := git.RevParse(ctx, f.capture.dir, "HEAD"); err != nil || head != f.live {
		t.Fatalf("HEAD after adoption=(%q,%v), want %s", head, err, f.live)
	}
	if statusAfter := gitRawOutput(
		t, ctx, f.capture.dir, "status", "--short",
	); statusAfter != statusBefore {
		t.Fatalf("bridge pass changed worktree/index:\nbefore=%q\nafter=%q",
			statusBefore, statusAfter)
	}
}

func TestExternalRepairBridgeRejectsPostProofCaptureAtomically(t *testing.T) {
	f := newExternalRepairBridgeFixture(t, externalRepairBridgeFixtureOptions{})
	ctx := context.Background()
	keys := seedExternalBridgeAtomicBaseline(t, ctx, f)
	before := readExternalBridgeAtomicState(t, ctx, f, keys)

	laterBlob := externalBridgeTestBlob(t, ctx, f.capture, "captured during reconciliation\n")
	var laterSeq int64
	opts := externalBridgeTestRecoveryOptions(f)
	opts.beforeStateTransition = func() {
		laterSeq = appendRecoveryEvent(t, ctx, f.capture, f.live, state.CaptureOp{
			Op: "create", Path: "captured-during-reconciliation.txt",
			AfterOID:  oidValue(laterBlob),
			AfterMode: oidValue(git.RegularFileMode),
		})
	}

	result, err := ReconcileUnpublishedChain(
		ctx, f.capture.dir, f.capture.db, opts)
	if !errors.Is(err, state.ErrRecoveryChainChanged) {
		t.Fatalf("ReconcileUnpublishedChain result=%+v err=%v want ErrRecoveryChainChanged",
			result, err)
	}
	if laterSeq == 0 {
		t.Fatal("post-proof capture hook did not run")
	}
	allPending := append([]int64(nil), f.pendingSeqs...)
	allPending = append(allPending, laterSeq)
	assertExternalBridgeEventsPending(t, ctx, f.capture.db,
		allPending)
	after := readExternalBridgeAtomicState(t, ctx, f, keys)
	if !reflect.DeepEqual(after, before) {
		t.Fatalf("rejected post-proof capture changed atomic state:\nbefore=%+v\nafter=%+v",
			before, after)
	}
}

func TestExternalRepairBridgeRejectsMissingLaterCaptureObjectAtomically(t *testing.T) {
	f := newExternalRepairBridgeFixture(t, externalRepairBridgeFixtureOptions{})
	ctx := context.Background()
	keys := seedExternalBridgeAtomicBaseline(t, ctx, f)
	before := readExternalBridgeAtomicState(t, ctx, f, keys)

	laterSeq := appendRecoveryEvent(t, ctx, f.capture, f.live, state.CaptureOp{
		Op: "create", Path: "missing-later-object.txt",
		AfterOID:  oidValue(strings.Repeat("f", 40)),
		AfterMode: oidValue(git.RegularFileMode),
	})
	result, err := ReconcileUnpublishedChain(
		ctx, f.capture.dir, f.capture.db, externalBridgeTestRecoveryOptions(f))
	if !errors.Is(err, state.ErrCompletedBranchTransitionProof) {
		t.Fatalf("ReconcileUnpublishedChain result=%+v err=%v want ErrCompletedBranchTransitionProof",
			result, err)
	}
	allPending := append([]int64(nil), f.pendingSeqs...)
	allPending = append(allPending, laterSeq)
	assertExternalBridgeEventsPending(t, ctx, f.capture.db, allPending)
	after := readExternalBridgeAtomicState(t, ctx, f, keys)
	if !reflect.DeepEqual(after, before) {
		t.Fatalf("rejected missing later object changed atomic state:\nbefore=%+v\nafter=%+v",
			before, after)
	}
}

func TestExternalRepairBridgeRejectsBranchMoveBeforeCombinedLock(t *testing.T) {
	f := newExternalRepairBridgeFixture(t, externalRepairBridgeFixtureOptions{})
	ctx := context.Background()
	keys := seedExternalBridgeAtomicBaseline(t, ctx, f)
	before := readExternalBridgeAtomicState(t, ctx, f, keys)

	moved := commitTreeWithIndexUpdates(
		t, ctx, f.capture, f.live, "branch moved during reconciliation")
	ref := recoveryProofRefName(
		f.capture.cctx.BranchRef, f.capture.cctx.BranchGeneration,
		f.pendingSeqs[0], f.pendingSeqs[len(f.pendingSeqs)-1], f.target)
	var callbackRan bool
	opts := externalBridgeTestRecoveryOptions(f)
	opts.beforeStateTransition = func() {
		if protected, err := git.RevParse(ctx, f.capture.dir, ref); err != nil || protected != f.target {
			t.Fatalf("recovery ref before branch race=%q err=%v want %s",
				protected, err, f.target)
		}
		if err := git.UpdateRef(
			ctx, f.capture.dir, f.capture.cctx.BranchRef, moved, f.live); err != nil {
			t.Fatalf("move branch before combined lock: %v", err)
		}
	}
	opts.afterRecoveryRefLocked = func() { callbackRan = true }

	result, err := ReconcileUnpublishedChain(
		ctx, f.capture.dir, f.capture.db, opts)
	if err == nil {
		t.Fatalf("ReconcileUnpublishedChain result=%+v err=nil after branch move", result)
	}
	if callbackRan {
		t.Fatal("state transition callback ran after the literal branch ref moved")
	}
	if head, parseErr := git.RevParse(
		ctx, f.capture.dir, f.capture.cctx.BranchRef); parseErr != nil || head != moved {
		t.Fatalf("branch ref after race=%q err=%v want moved head %s",
			head, parseErr, moved)
	}
	assertExternalBridgeEventsPending(
		t, ctx, f.capture.db, f.pendingSeqs)
	after := readExternalBridgeAtomicState(t, ctx, f, keys)
	if !reflect.DeepEqual(after, before) {
		t.Fatalf("rejected branch move changed atomic state:\nbefore=%+v\nafter=%+v",
			before, after)
	}
}

type externalBridgeAtomicMeta struct {
	Value string
	OK    bool
}

type externalBridgeAtomicState struct {
	ShadowRows      []state.ShadowPath
	Meta            map[string]externalBridgeAtomicMeta
	Snapshots       int
	SnapshotMembers int
}

func seedExternalBridgeAtomicBaseline(
	t *testing.T,
	ctx context.Context,
	f externalRepairBridgeFixture,
) []string {
	t.Helper()
	shadowBlob := externalBridgeTestBlob(t, ctx, f.capture, "old shadow evidence\n")
	if err := state.UpsertShadowPath(ctx, f.capture.db, state.ShadowPath{
		BranchRef:        f.capture.cctx.BranchRef,
		BranchGeneration: f.capture.cctx.BranchGeneration,
		Path:             "old-shadow-evidence.txt",
		Operation:        "create",
		Mode:             oidValue(git.RegularFileMode),
		OID:              oidValue(shadowBlob),
		BaseHead:         f.source,
		Fidelity:         "rescan",
		UpdatedTS:        11,
	}); err != nil {
		t.Fatalf("seed atomic bridge shadow: %v", err)
	}
	marker := ShadowBootstrappedKey(
		f.capture.cctx.BranchRef, f.capture.cctx.BranchGeneration)
	meta := map[string]string{
		marker:                                "old-marker",
		MetaKeyBranchGeneration:               "1",
		MetaKeyBranchHead:                     f.source,
		MetaKeyBranchToken:                    branchTokenRev(f.source, f.capture.cctx.BranchRef),
		MetaKeyBranchTokenChangedAt:           "7",
		MetaKeyBranchTransitionNeedsAttention: "old transition warning",
		"manual_pause.resumed_at":             "6",
	}
	keys := make([]string, 0, len(meta))
	for key, value := range meta {
		if err := state.MetaSet(ctx, f.capture.db, key, value); err != nil {
			t.Fatalf("seed atomic bridge meta %s: %v", key, err)
		}
		keys = append(keys, key)
	}
	return keys
}

func readExternalBridgeAtomicState(
	t *testing.T,
	ctx context.Context,
	f externalRepairBridgeFixture,
	metaKeys []string,
) externalBridgeAtomicState {
	t.Helper()
	result := externalBridgeAtomicState{
		Meta: make(map[string]externalBridgeAtomicMeta, len(metaKeys)),
	}
	rows, err := f.capture.db.ReadSQL().QueryContext(ctx, `
SELECT branch_ref,branch_generation,path,operation,mode,oid,old_path,
       base_head,fidelity,updated_ts
FROM shadow_paths
WHERE branch_ref=? AND branch_generation=?
ORDER BY path`, f.capture.cctx.BranchRef, f.capture.cctx.BranchGeneration)
	if err != nil {
		t.Fatalf("query atomic bridge shadow: %v", err)
	}
	defer rows.Close()
	for rows.Next() {
		var row state.ShadowPath
		if err := rows.Scan(
			&row.BranchRef, &row.BranchGeneration, &row.Path, &row.Operation,
			&row.Mode, &row.OID, &row.OldPath, &row.BaseHead, &row.Fidelity,
			&row.UpdatedTS); err != nil {
			t.Fatalf("scan atomic bridge shadow: %v", err)
		}
		result.ShadowRows = append(result.ShadowRows, row)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate atomic bridge shadow: %v", err)
	}
	for _, key := range metaKeys {
		value, ok, err := state.MetaGet(ctx, f.capture.db, key)
		if err != nil {
			t.Fatalf("read atomic bridge meta %s: %v", key, err)
		}
		result.Meta[key] = externalBridgeAtomicMeta{Value: value, OK: ok}
	}
	for query, target := range map[string]*int{
		`SELECT COUNT(*) FROM recovery_snapshots`:       &result.Snapshots,
		`SELECT COUNT(*) FROM recovery_snapshot_events`: &result.SnapshotMembers,
	} {
		if err := f.capture.db.ReadSQL().QueryRowContext(ctx, query).Scan(target); err != nil {
			t.Fatalf("count atomic bridge rows for %q: %v", query, err)
		}
	}
	return result
}

func assertExternalBridgeEventsPending(
	t *testing.T,
	ctx context.Context,
	db *state.DB,
	seqs []int64,
) {
	t.Helper()
	for _, seq := range seqs {
		eventState, commit := readEventState(t, ctx, db, seq)
		if eventState != state.EventStatePending || commit != (sql.NullString{}) {
			t.Fatalf("seq=%d lifecycle=(%q,%+v) want pending,NULL",
				seq, eventState, commit)
		}
	}
}

func assertExternalBridgePublishedPrefix(
	t *testing.T,
	ctx context.Context,
	f externalRepairBridgeFixture,
) {
	t.Helper()
	for _, seq := range f.pendingSeqs {
		eventState, commit := readEventState(t, ctx, f.capture.db, seq)
		if eventState != state.EventStatePublished || !commit.Valid ||
			commit.String != f.target {
			t.Fatalf("frozen seq=%d lifecycle=(%q,%+v) want published at %s",
				seq, eventState, commit, f.target)
		}
	}
}

func assertExternalBridgeCaptureCount(
	t *testing.T,
	ctx context.Context,
	db *state.DB,
	path string,
	want int,
) {
	t.Helper()
	var got int
	if err := db.ReadSQL().QueryRowContext(ctx,
		`SELECT COUNT(*) FROM capture_events WHERE path=?`, path).Scan(&got); err != nil {
		t.Fatalf("count captures for %s: %v", path, err)
	}
	if got != want {
		t.Fatalf("captures for %s=%d want %d", path, got, want)
	}
}
