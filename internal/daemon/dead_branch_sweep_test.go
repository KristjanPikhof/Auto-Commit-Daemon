package daemon

import (
	"context"
	"database/sql"
	"encoding/json"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	pausepkg "github.com/KristjanPikhof/Auto-Commit-Daemon/internal/pause"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

// TestIsKeepDeadBranchBarriers covers the truthy/falsy table for the env
// opt-out helper. Truthy values ("1", "true", "yes", "on", any case) disable
// the runtime + startup prune; everything else (including empty / unset /
// "garbage") keeps the default-on behaviour.
func TestIsKeepDeadBranchBarriers(t *testing.T) {
	cases := []struct {
		val  string
		want bool
	}{
		{"1", true},
		{"true", true},
		{"TRUE", true},
		{"True", true},
		{"yes", true},
		{"Yes", true},
		{"YES", true},
		{"on", true},
		{"ON", true},
		{"On", true},
		{"  on  ", true},
		{"", false},
		{"0", false},
		{"false", false},
		{"FALSE", false},
		{"no", false},
		{"NO", false},
		{"off", false},
		{"OFF", false},
		{"garbage", false},
	}
	for _, tc := range cases {
		tc := tc
		t.Run("val="+tc.val, func(t *testing.T) {
			t.Setenv(EnvKeepDeadBranchBarriers, tc.val)
			if got := isKeepDeadBranchBarriers(); got != tc.want {
				t.Fatalf("isKeepDeadBranchBarriers(%q)=%v want %v", tc.val, got, tc.want)
			}
		})
	}
	// Explicit unset case — t.Setenv only adds; verify the helper sees an
	// absent env var as falsy too.
	t.Run("unset", func(t *testing.T) {
		// Setenv to empty then unset by leaving Setenv with empty-string
		// would still set it to "". To prove "unset" we rely on the
		// empty-string case being falsy, which the table above already
		// asserts. Adding this guard documents the intent.
		t.Setenv(EnvKeepDeadBranchBarriers, "")
		if isKeepDeadBranchBarriers() {
			t.Fatalf("expected false when env is empty")
		}
	})
}

// seedTerminalEvent inserts one capture_events row in the requested terminal
// state for the given (branch_ref, branch_generation) pair. Returns the
// assigned seq.
func seedTerminalEvent(t *testing.T, db *state.DB, branchRef string, generation int64, baseHead, path, eventState string) int64 {
	t.Helper()
	ctx := context.Background()
	gitDir := filepath.Dir(filepath.Dir(db.Path()))
	repoDir := filepath.Dir(gitDir)
	afterOID, err := git.HashObjectStdin(ctx, repoDir, []byte(path+"\n"))
	if err != nil {
		t.Fatalf("hash after blob: %v", err)
	}
	seq, err := state.AppendCaptureEvent(ctx, db, state.CaptureEvent{
		BranchRef:        branchRef,
		BranchGeneration: generation,
		BaseHead:         baseHead,
		Operation:        "create",
		Path:             path,
		Fidelity:         "full",
		State:            state.EventStatePending,
	}, []state.CaptureOp{{
		Op:        "create",
		Path:      path,
		Fidelity:  "full",
		AfterMode: sql.NullString{String: git.RegularFileMode, Valid: true},
		AfterOID:  sql.NullString{String: afterOID, Valid: true},
	}})
	if err != nil {
		t.Fatalf("AppendCaptureEvent: %v", err)
	}
	if eventState == state.EventStatePending {
		return seq
	}
	if err := state.MarkEventPublished(ctx, db, seq, eventState,
		sql.NullString{}, sql.NullString{String: "seeded", Valid: true},
		sql.NullString{}, float64(time.Now().UnixNano())/1e9); err != nil {
		t.Fatalf("MarkEventPublished: %v", err)
	}
	return seq
}

func countEventsByRefState(t *testing.T, db *state.DB, branchRef, eventState string) int {
	t.Helper()
	var n int
	if err := db.SQL().QueryRowContext(context.Background(),
		`SELECT COUNT(*) FROM capture_events WHERE branch_ref = ? AND state = ?`,
		branchRef, eventState).Scan(&n); err != nil {
		t.Fatalf("count events ref=%s state=%s: %v", branchRef, eventState, err)
	}
	return n
}

// TestDeadBranchSweep_RecoversDeadRefRows seeds blocked_conflict + failed +
// pending rows for refs/heads/old (which is NOT created in the test repo) and
// terminal rows for the active refs/heads/main; runs the startup sweep;
// asserts old's pending + terminal rows all become recovered while their
// provenance rows remain, and main's rows are preserved.
func TestDeadBranchSweep_RecoversDeadRefRows(t *testing.T) {
	t.Setenv(EnvKeepDeadBranchBarriers, "")
	f := newDaemonFixture(t)
	ctx := context.Background()
	headOID, err := git.RevParse(ctx, f.dir, "HEAD")
	if err != nil {
		t.Fatalf("rev-parse HEAD: %v", err)
	}

	const deadRef = "refs/heads/old"
	const activeRef = "refs/heads/main"

	// Dead-ref rows: one of each (terminal + pending). All three must be
	// transitioned together so replay cannot restamp a blocked conflict.
	seedTerminalEvent(t, f.db, deadRef, 1, headOID, "old-blocked.txt", state.EventStateBlockedConflict)
	seedTerminalEvent(t, f.db, deadRef, 1, headOID, "old-failed.txt", state.EventStateFailed)
	seedTerminalEvent(t, f.db, deadRef, 1, headOID, "old-pending.txt", state.EventStatePending)
	// Active-ref rows: one terminal of each kind. These must not be touched
	// because cctx.BranchRef matches and the sweep skips the active pair.
	seedTerminalEvent(t, f.db, activeRef, 1, headOID, "main-blocked.txt", state.EventStateBlockedConflict)
	seedTerminalEvent(t, f.db, activeRef, 1, headOID, "main-failed.txt", state.EventStateFailed)
	// And a live-ref pending row to confirm the live-ref path leaves
	// pending alone (only the dead pair drops pending).
	seedTerminalEvent(t, f.db, activeRef, 1, headOID, "main-pending.txt", state.EventStatePending)

	cctx := CaptureContext{
		BranchRef:        activeRef,
		BranchGeneration: 1,
		BaseHead:         headOID,
	}
	runStartupDeadBranchSweep(ctx, f.dir, f.db, cctx, slog.Default(), nil)

	if got := countEventsByRefState(t, f.db, deadRef, state.EventStateBlockedConflict); got != 0 {
		t.Fatalf("dead-ref blocked_conflict rows=%d want 0", got)
	}
	if got := countEventsByRefState(t, f.db, deadRef, state.EventStateFailed); got != 0 {
		t.Fatalf("dead-ref failed rows=%d want 0", got)
	}
	// Pending lifecycle state must also be gone so PendingEvents does not
	// re-expose the chain on the next replay tick.
	if got := countEventsByRefState(t, f.db, deadRef, state.EventStatePending); got != 0 {
		t.Fatalf("dead-ref pending rows=%d want 0 after whole-chain recovery", got)
	}
	if got := countEventsByRefState(t, f.db, deadRef, state.EventStateRecovered); got != 3 {
		t.Fatalf("dead-ref recovered rows=%d want 3", got)
	}
	if got := countEventsByRefState(t, f.db, activeRef, state.EventStateBlockedConflict); got != 1 {
		t.Fatalf("active-ref blocked_conflict rows=%d want 1 (active pair must be preserved)", got)
	}
	if got := countEventsByRefState(t, f.db, activeRef, state.EventStateFailed); got != 1 {
		t.Fatalf("active-ref failed rows=%d want 1 (active pair must be preserved)", got)
	}
	if got := countEventsByRefState(t, f.db, activeRef, state.EventStatePending); got != 1 {
		t.Fatalf("active-ref pending rows=%d want 1 (live-ref pending must be preserved)", got)
	}
}

func TestDeadBranchSweep_ArchivesWhenLiveHeadMissing(t *testing.T) {
	t.Setenv(EnvKeepDeadBranchBarriers, "")
	f := newDaemonFixture(t)
	ctx := context.Background()
	baseHead, err := git.RevParse(ctx, f.dir, "HEAD")
	if err != nil {
		t.Fatalf("rev-parse HEAD: %v", err)
	}
	const deadRef = "refs/heads/dead-before-orphan"
	seedTerminalEvent(t, f.db, deadRef, 1, baseHead, "preserved.txt", state.EventStatePending)
	if _, err := git.Run(ctx, git.RunOpts{Dir: f.dir}, "symbolic-ref", "HEAD", "refs/heads/orphan"); err != nil {
		t.Fatalf("symbolic-ref orphan: %v", err)
	}

	runStartupDeadBranchSweep(ctx, f.dir, f.db, CaptureContext{
		BranchRef: "refs/heads/orphan", BranchGeneration: 2,
	}, slog.Default(), nil)

	if got := countEventsByRefState(t, f.db, deadRef, state.EventStateRecovered); got != 1 {
		t.Fatalf("recovered rows=%d want 1", got)
	}
	var recoveryRef string
	if err := f.db.SQL().QueryRowContext(ctx, `
SELECT recovery_ref FROM recovery_snapshots
WHERE branch_ref = ? AND branch_generation = ?`, deadRef, 1).Scan(&recoveryRef); err != nil {
		t.Fatalf("read recovery snapshot: %v", err)
	}
	if !strings.HasSuffix(recoveryRef, "/archive") {
		t.Fatalf("recovery_ref=%q want archive ref", recoveryRef)
	}
}

func TestDeadBranchRecoveryRefRecreationPreservesPair(t *testing.T) {
	f := newDaemonFixture(t)
	ctx := context.Background()
	baseHead, err := git.RevParse(ctx, f.dir, "HEAD")
	if err != nil {
		t.Fatalf("rev-parse HEAD: %v", err)
	}
	const deadRef = "refs/heads/recreated-during-recovery"
	seq := seedTerminalEvent(t, f.db, deadRef, 1, baseHead, "preserved.txt", state.EventStatePending)
	var recreateErr error
	_, err = ReconcileUnpublishedChain(ctx, f.dir, f.db, RecoveryReconcileOptions{
		GitDir: f.gitDir, BranchRef: deadRef, BranchGeneration: 1,
		FirstSeq: seq, Trigger: "dead_ref_race", ExpectedMissingRef: deadRef,
		beforeStateTransition: func() {
			recreateErr = git.UpdateRef(ctx, f.dir, deadRef, baseHead, "")
		},
	})
	if recreateErr != nil {
		t.Fatalf("recreate ref: %v", recreateErr)
	}
	if err == nil || !strings.Contains(err.Error(), "to remain missing") {
		t.Fatalf("Reconcile error=%v want recreated-ref refusal", err)
	}
	if got := countEventsByRefState(t, f.db, deadRef, state.EventStatePending); got != 1 {
		t.Fatalf("pending rows=%d want 1 unchanged", got)
	}
	if snapshots := countRecoverySnapshots(t, ctx, f.db); snapshots != 0 {
		t.Fatalf("recovery snapshots=%d want 0", snapshots)
	}
}

func TestDeadBranchRecoveryLocksMissingRefThroughTransition(t *testing.T) {
	f := newDaemonFixture(t)
	ctx := context.Background()
	baseHead, err := git.RevParse(ctx, f.dir, "HEAD")
	if err != nil {
		t.Fatalf("rev-parse HEAD: %v", err)
	}
	const deadRef = "refs/heads/recreated-after-recovery-lock"
	seq := seedTerminalEvent(t, f.db, deadRef, 1, baseHead, "preserved.txt", state.EventStatePending)

	recreateDone := make(chan error, 1)
	recreateStarted := make(chan struct{})
	completedWhileLocked := false
	var recreateErr error
	result, err := ReconcileUnpublishedChain(ctx, f.dir, f.db, RecoveryReconcileOptions{
		GitDir: f.gitDir, BranchRef: deadRef, BranchGeneration: 1,
		FirstSeq: seq, Trigger: "dead_ref_atomic_race", ExpectedMissingRef: deadRef,
		afterRecoveryRefLocked: func() {
			go func() {
				close(recreateStarted)
				recreateDone <- git.UpdateRef(ctx, f.dir, deadRef, baseHead, "")
			}()
			<-recreateStarted
			select {
			case recreateErr = <-recreateDone:
				completedWhileLocked = true
			case <-time.After(200 * time.Millisecond):
			}
		},
	})
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if completedWhileLocked && recreateErr == nil {
		t.Fatal("dead ref was recreated before the recovery DB transition completed")
	}
	if !result.Handled || result.Outcome != state.EventStateRecovered {
		t.Fatalf("result=%+v want recovered", result)
	}
	if !completedWhileLocked {
		select {
		case recreateErr = <-recreateDone:
		case <-time.After(5 * time.Second):
			t.Fatal("dead-ref transaction lock was not released")
		}
	}
	if recreateErr != nil {
		if err := git.UpdateRef(ctx, f.dir, deadRef, baseHead, ""); err != nil {
			t.Fatalf("recreate dead ref after transition: %v", err)
		}
	}
	if got := countEventsByRefState(t, f.db, deadRef, state.EventStateRecovered); got != 1 {
		t.Fatalf("recovered rows=%d want 1", got)
	}
}

func TestStartupDeadBranchSweep_RechecksSafetyBeforePair(t *testing.T) {
	for _, tc := range []struct {
		name  string
		setup func(*testing.T, *daemonFixture) error
	}{
		{
			name: "manual pause",
			setup: func(t *testing.T, f *daemonFixture) error {
				_, err := pausepkg.Write(pausepkg.Path(f.gitDir), pausepkg.Marker{
					Reason: "operator surgery",
					SetAt:  time.Now().UTC().Format(time.RFC3339),
					SetBy:  "test",
				}, false)
				return err
			},
		},
		{
			name: "git operation",
			setup: func(t *testing.T, f *daemonFixture) error {
				return os.MkdirAll(filepath.Join(f.gitDir, "rebase-merge"), 0o755)
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv(EnvKeepDeadBranchBarriers, "")
			f := newDaemonFixture(t)
			ctx := context.Background()
			head, err := git.RevParse(ctx, f.dir, "HEAD")
			if err != nil {
				t.Fatalf("rev-parse HEAD: %v", err)
			}
			const deadRef = "refs/heads/safety-race"
			seedTerminalEvent(t, f.db, deadRef, 1, head, "preserved.txt", state.EventStatePending)
			var setupErr error
			runStartupDeadBranchSweepWithOptions(ctx, f.dir, f.db, CaptureContext{
				BranchRef: "refs/heads/main", BranchGeneration: 1, BaseHead: head,
			}, slog.Default(), nil, startupDeadBranchSweepOptions{
				beforePairSafetyCheck: func(context.Context, deadBranchPair) {
					setupErr = tc.setup(t, f)
				},
			})
			if setupErr != nil {
				t.Fatalf("setup safety gate: %v", setupErr)
			}
			if got := countEventsByRefState(t, f.db, deadRef, state.EventStatePending); got != 1 {
				t.Fatalf("pending rows=%d want 1 preserved", got)
			}
			if snapshots := countRecoverySnapshots(t, ctx, f.db); snapshots != 0 {
				t.Fatalf("recovery snapshots=%d want 0", snapshots)
			}
		})
	}
}

// TestRun_ShutdownJoinsStartupDeadBranchSweep proves the asynchronous startup
// sweep remains owned by Run. Both shutdown mechanisms must cancel a sweep
// blocked immediately before mutation and wait for it to exit before Run
// releases daemon.lock or returns control to the DB owner.
func TestRun_ShutdownJoinsStartupDeadBranchSweep(t *testing.T) {
	for _, shutdownMode := range []string{"signal", "context"} {
		t.Run(shutdownMode, func(t *testing.T) {
			t.Setenv(EnvKeepDeadBranchBarriers, "")
			f := newDaemonFixture(t)
			registerLiveClient(t, f.db)
			ctx := context.Background()
			head, err := git.RevParse(ctx, f.dir, "HEAD")
			if err != nil {
				t.Fatalf("rev-parse HEAD: %v", err)
			}
			const deadRef = "refs/heads/shutdown-sweep"
			seedTerminalEvent(t, f.db, deadRef, 1, head, "preserved.txt", state.EventStateBlockedConflict)

			sweepStarted := make(chan struct{})
			sweepExited := make(chan struct{})
			shutdownCh := make(chan struct{}, 1)
			runCtx, cancel := context.WithCancel(ctx)
			defer cancel()
			runDone := make(chan error, 1)
			go func() {
				runDone <- Run(runCtx, Options{
					RepoPath:    f.dir,
					GitDir:      f.gitDir,
					DB:          f.db,
					Scheduler:   fastScheduler(),
					BootGrace:   30 * time.Second,
					ShutdownCh:  shutdownCh,
					SkipSignals: true,
					beforeStartupDeadBranchPairSafetyCheck: func(sweepCtx context.Context, _ deadBranchPair) {
						close(sweepStarted)
						<-sweepCtx.Done()
						close(sweepExited)
					},
				})
			}()

			select {
			case <-sweepStarted:
			case <-time.After(5 * time.Second):
				t.Fatalf("startup dead-branch sweep did not reach synchronization point")
			}
			if shutdownMode == "signal" {
				shutdownCh <- struct{}{}
			} else {
				cancel()
			}

			select {
			case err := <-runDone:
				if err != nil {
					t.Fatalf("Run shutdown: %v", err)
				}
			case <-time.After(5 * time.Second):
				t.Fatalf("Run did not join startup dead-branch sweep")
			}
			select {
			case <-sweepExited:
			default:
				t.Fatalf("Run returned before startup dead-branch sweep exited")
			}

			lock, err := AcquireDaemonLock(f.gitDir)
			if err != nil {
				t.Fatalf("reacquire daemon.lock after Run: %v", err)
			}
			if err := lock.Release(); err != nil {
				t.Fatalf("release reacquired daemon.lock: %v", err)
			}
		})
	}
}

// TestDeadBranchSweep_RegressionPendingDoesNotLeakBarrier asserts the P1
// regression: after recovering a dead branch, no later
// PendingEvents call must surface pending rows for the same dead pair (which
// would let replay re-stamp a blocked_conflict and defeat recovery). This
// is the test against the bug cr-expert flagged.
func TestDeadBranchSweep_RegressionPendingDoesNotLeakBarrier(t *testing.T) {
	t.Setenv(EnvKeepDeadBranchBarriers, "")
	f := newDaemonFixture(t)
	ctx := context.Background()
	headOID, err := git.RevParse(ctx, f.dir, "HEAD")
	if err != nil {
		t.Fatalf("rev-parse HEAD: %v", err)
	}

	const deadRef = "refs/heads/dead"
	// Seed blocked_conflict + failed + pending plus a stamped publish_state
	// singleton pointing at the blocked_conflict (the realistic shape of a
	// daemon DB after a Diverged-into-deleted-branch sequence).
	blockedSeq := seedTerminalEvent(t, f.db, deadRef, 1, headOID, "dead-blocked.txt", state.EventStateBlockedConflict)
	seedTerminalEvent(t, f.db, deadRef, 1, headOID, "dead-failed.txt", state.EventStateFailed)
	seedTerminalEvent(t, f.db, deadRef, 1, headOID, "dead-pending.txt", state.EventStatePending)
	if err := state.MarkEventBlocked(ctx, f.db, blockedSeq, "seeded blocker", float64(time.Now().UnixNano())/1e9,
		sql.NullString{String: deadRef, Valid: true},
		sql.NullInt64{Int64: 1, Valid: true},
		sql.NullString{String: headOID, Valid: true},
	); err != nil {
		t.Fatalf("MarkEventBlocked: %v", err)
	}
	for _, key := range replayErrorMetaTestKeys() {
		if err := state.MetaSet(ctx, f.db, key, "stale"); err != nil {
			t.Fatalf("seed replay metadata %s: %v", key, err)
		}
	}

	cctx := CaptureContext{
		BranchRef:        "refs/heads/main",
		BranchGeneration: 1,
		BaseHead:         headOID,
	}
	runStartupDeadBranchSweep(ctx, f.dir, f.db, cctx, slog.Default(), nil)

	// All capture_events remain as durable recovered provenance.
	var total int
	if err := f.db.SQL().QueryRowContext(ctx,
		`SELECT COUNT(*) FROM capture_events WHERE branch_ref = ?`, deadRef,
	).Scan(&total); err != nil {
		t.Fatalf("count dead-ref rows: %v", err)
	}
	if total != 3 {
		t.Fatalf("dead-ref rows=%d want 3 retained after sweep", total)
	}
	if recovered := countEventsByRefState(t, f.db, deadRef, state.EventStateRecovered); recovered != 3 {
		t.Fatalf("dead-ref recovered rows=%d want 3", recovered)
	}

	// PendingEvents must not surface anything for the dead ref — this is the
	// regression: pre-fix the helper left pending rows behind, so the next
	// replay pass would re-stamp a fresh blocked_conflict on the dead pair.
	pending, err := state.PendingEvents(ctx, f.db, 64)
	if err != nil {
		t.Fatalf("PendingEvents: %v", err)
	}
	for _, ev := range pending {
		if ev.BranchRef == deadRef {
			t.Fatalf("PendingEvents surfaced row for dead ref %q after sweep: seq=%d",
				deadRef, ev.Seq)
		}
	}

	// publish_state must read 'ok' (barrier was lifted in the same tx).
	var status string
	var errMsg sql.NullString
	if err := f.db.SQL().QueryRowContext(ctx,
		`SELECT status, error FROM publish_state WHERE id = 1`,
	).Scan(&status, &errMsg); err != nil {
		t.Fatalf("read publish_state: %v", err)
	}
	if status != "ok" {
		t.Fatalf("publish_state.status=%q want 'ok' after sweep", status)
	}
	if errMsg.Valid {
		t.Fatalf("publish_state.error=%q want NULL after sweep", errMsg.String)
	}

	// Breadcrumb meta keys must be cleared.
	for _, key := range replayErrorMetaTestKeys() {
		if _, ok, err := state.MetaGet(ctx, f.db, key); err != nil {
			t.Fatalf("MetaGet %q: %v", key, err)
		} else if ok {
			t.Fatalf("breadcrumb %q still present after sweep", key)
		}
	}
}

// TestDeadBranchSweep_OptOutPreservesRows asserts that ACD_KEEP_DEAD_BRANCH_BARRIERS=1
// short-circuits the sweep entirely; even dead-ref terminals are preserved.
func TestDeadBranchSweep_OptOutPreservesRows(t *testing.T) {
	t.Setenv(EnvKeepDeadBranchBarriers, "1")
	f := newDaemonFixture(t)
	ctx := context.Background()
	headOID, err := git.RevParse(ctx, f.dir, "HEAD")
	if err != nil {
		t.Fatalf("rev-parse HEAD: %v", err)
	}

	const deadRef = "refs/heads/gone"
	seedTerminalEvent(t, f.db, deadRef, 1, headOID, "gone-blocked.txt", state.EventStateBlockedConflict)
	seedTerminalEvent(t, f.db, deadRef, 1, headOID, "gone-failed.txt", state.EventStateFailed)

	cctx := CaptureContext{
		BranchRef:        "refs/heads/main",
		BranchGeneration: 1,
		BaseHead:         headOID,
	}
	runStartupDeadBranchSweep(ctx, f.dir, f.db, cctx, slog.Default(), nil)

	if got := countEventsByRefState(t, f.db, deadRef, state.EventStateBlockedConflict); got != 1 {
		t.Fatalf("opt-out blocked_conflict rows=%d want 1", got)
	}
	if got := countEventsByRefState(t, f.db, deadRef, state.EventStateFailed); got != 1 {
		t.Fatalf("opt-out failed rows=%d want 1", got)
	}
}

// TestDeadBranchSweep_LiveRefsErrorPreservesRows asserts that when
// git.LiveBranchSet returns an error (e.g. bogus repoDir), the sweep fails
// closed: terminal rows are preserved rather than silently dropped. The
// sweep now batches liveness probes via for-each-ref instead of per-pair
// RefExists, so a single error preserves all candidate rows in one go (the
// previous per-pair contract preserved on a per-ref basis).
func TestDeadBranchSweep_LiveRefsErrorPreservesRows(t *testing.T) {
	t.Setenv(EnvKeepDeadBranchBarriers, "")
	f := newDaemonFixture(t)
	ctx := context.Background()
	headOID, err := git.RevParse(ctx, f.dir, "HEAD")
	if err != nil {
		t.Fatalf("rev-parse HEAD: %v", err)
	}

	const deadRef = "refs/heads/probe-error"
	seedTerminalEvent(t, f.db, deadRef, 1, headOID, "probe-blocked.txt", state.EventStateBlockedConflict)
	seedTerminalEvent(t, f.db, deadRef, 1, headOID, "probe-failed.txt", state.EventStateFailed)

	// Bogus repoDir — for-each-ref will exit with 128 (not a git repo).
	// Sweep must surface the liveness-probe error and skip the prune
	// entirely (fail closed: cannot prove dead, cannot delete).
	bogusRepo := t.TempDir()
	cctx := CaptureContext{
		BranchRef:        "refs/heads/main",
		BranchGeneration: 1,
		BaseHead:         headOID,
	}
	runStartupDeadBranchSweep(ctx, bogusRepo, f.db, cctx, slog.Default(), nil)

	if got := countEventsByRefState(t, f.db, deadRef, state.EventStateBlockedConflict); got != 1 {
		t.Fatalf("probe-error blocked_conflict rows=%d want 1 (must fail closed)", got)
	}
	if got := countEventsByRefState(t, f.db, deadRef, state.EventStateFailed); got != 1 {
		t.Fatalf("probe-error failed rows=%d want 1 (must fail closed)", got)
	}
}

// TestPruneDeadBranchTerminalsRecoversOnlyDeadRef verifies that the runtime
// helper recovers a deleted ref without touching an otherwise identical live
// ref. TestRun_RuntimeDivergedRecoversDeadBranchTerminals covers the run-loop
// integration separately.
func TestPruneDeadBranchTerminalsRecoversOnlyDeadRef(t *testing.T) {
	t.Setenv(EnvKeepDeadBranchBarriers, "")
	f := newDaemonFixture(t)
	ctx := context.Background()

	headOID, err := git.RevParse(ctx, f.dir, "HEAD")
	if err != nil {
		t.Fatalf("rev-parse HEAD: %v", err)
	}

	const deadRef = "refs/heads/old"
	const liveRef = "refs/heads/keep"
	if err := git.UpdateRef(ctx, f.dir, deadRef, headOID, ""); err != nil {
		t.Fatalf("update-ref %s: %v", deadRef, err)
	}
	if err := git.UpdateRef(ctx, f.dir, liveRef, headOID, ""); err != nil {
		t.Fatalf("update-ref %s: %v", liveRef, err)
	}

	seedTerminalEvent(t, f.db, deadRef, 1, headOID, "old-blocked.txt", state.EventStateBlockedConflict)
	seedTerminalEvent(t, f.db, liveRef, 1, headOID, "keep-blocked.txt", state.EventStateBlockedConflict)
	if _, err := git.Run(ctx, git.RunOpts{Dir: f.dir}, "update-ref", "-d", deadRef); err != nil {
		t.Fatalf("delete %s: %v", deadRef, err)
	}

	pruneDeadBranchTerminals(ctx, f.dir, f.db,
		CaptureContext{BranchRef: "refs/heads/main", BranchGeneration: 2, BaseHead: headOID},
		deadRef, 1,
		slog.Default(), nil,
		"test-direct invocation")

	if n := countEventsByRefState(t, f.db, deadRef, state.EventStateBlockedConflict); n != 0 {
		t.Fatalf("dead-ref %s blocked rows=%d want 0 after recovery", deadRef, n)
	}
	if n := countEventsByRefState(t, f.db, deadRef, state.EventStateRecovered); n != 1 {
		t.Fatalf("dead-ref %s recovered rows=%d want 1", deadRef, n)
	}
	if n := countEventsByRefState(t, f.db, liveRef, state.EventStateBlockedConflict); n != 1 {
		t.Fatalf("live-ref %s terminal rows=%d want 1 (must be preserved)", liveRef, n)
	}
}

// TestDeadBranchSweep_WritesMetaKeysWhenRowsRecovered asserts the startup sweep
// stamps the three operator-facing legacy meta keys when rows are recovered.
// This is the input `acd diagnose --json` reads to surface stale-branch
// hygiene activity.
func TestDeadBranchSweep_WritesMetaKeysWhenRowsRecovered(t *testing.T) {
	t.Setenv(EnvKeepDeadBranchBarriers, "")
	f := newDaemonFixture(t)
	ctx := context.Background()
	headOID, err := git.RevParse(ctx, f.dir, "HEAD")
	if err != nil {
		t.Fatalf("rev-parse HEAD: %v", err)
	}

	const deadRef = "refs/heads/old"
	const activeRef = "refs/heads/main"
	seedTerminalEvent(t, f.db, deadRef, 1, headOID, "old-blocked.txt", state.EventStateBlockedConflict)
	seedTerminalEvent(t, f.db, deadRef, 1, headOID, "old-failed.txt", state.EventStateFailed)

	before := time.Now().Unix()
	cctx := CaptureContext{
		BranchRef:        activeRef,
		BranchGeneration: 1,
		BaseHead:         headOID,
	}
	runStartupDeadBranchSweep(ctx, f.dir, f.db, cctx, slog.Default(), nil)
	after := time.Now().Unix()

	// last_run_ts must be a parseable int within the wall-clock window.
	tsRaw, ok, err := state.MetaGet(ctx, f.db, MetaKeyDeadBranchPruneLastRunTS)
	if err != nil {
		t.Fatalf("MetaGet last_run_ts: %v", err)
	}
	if !ok || tsRaw == "" {
		t.Fatalf("expected meta %q to be set after non-empty sweep", MetaKeyDeadBranchPruneLastRunTS)
	}
	ts, err := strconv.ParseInt(tsRaw, 10, 64)
	if err != nil {
		t.Fatalf("parse last_run_ts %q: %v", tsRaw, err)
	}
	if ts < before || ts > after {
		t.Fatalf("last_run_ts=%d outside [%d, %d]", ts, before, after)
	}

	// last_count must equal the total rows pruned (2: blocked_conflict + failed).
	countRaw, ok, err := state.MetaGet(ctx, f.db, MetaKeyDeadBranchPruneLastCount)
	if err != nil {
		t.Fatalf("MetaGet last_count: %v", err)
	}
	if !ok {
		t.Fatalf("expected meta %q to be set", MetaKeyDeadBranchPruneLastCount)
	}
	if countRaw != "2" {
		t.Fatalf("last_count=%q want %q", countRaw, "2")
	}

	// last_refs must be valid JSON containing the dead ref.
	refsRaw, ok, err := state.MetaGet(ctx, f.db, MetaKeyDeadBranchPruneLastRefs)
	if err != nil {
		t.Fatalf("MetaGet last_refs: %v", err)
	}
	if !ok {
		t.Fatalf("expected meta %q to be set", MetaKeyDeadBranchPruneLastRefs)
	}
	var refs []string
	if err := json.Unmarshal([]byte(refsRaw), &refs); err != nil {
		t.Fatalf("unmarshal last_refs %q: %v", refsRaw, err)
	}
	if len(refs) != 1 || refs[0] != deadRef {
		t.Fatalf("last_refs=%v want [%q]", refs, deadRef)
	}
}

// TestDeadBranchSweep_NoMetaWhenNoOp asserts the sweep does NOT stamp the meta
// keys when no rows were recovered (only live-ref terminals exist). The previous
// "last action that did something" snapshot must survive a no-op pass.
func TestDeadBranchSweep_NoMetaWhenNoOp(t *testing.T) {
	t.Setenv(EnvKeepDeadBranchBarriers, "")
	f := newDaemonFixture(t)
	ctx := context.Background()
	headOID, err := git.RevParse(ctx, f.dir, "HEAD")
	if err != nil {
		t.Fatalf("rev-parse HEAD: %v", err)
	}

	const activeRef = "refs/heads/main"
	// Only seed terminals on the active ref — sweep must skip them.
	seedTerminalEvent(t, f.db, activeRef, 1, headOID, "main-blocked.txt", state.EventStateBlockedConflict)
	seedTerminalEvent(t, f.db, activeRef, 1, headOID, "main-failed.txt", state.EventStateFailed)

	cctx := CaptureContext{
		BranchRef:        activeRef,
		BranchGeneration: 1,
		BaseHead:         headOID,
	}
	runStartupDeadBranchSweep(ctx, f.dir, f.db, cctx, slog.Default(), nil)

	for _, key := range []string{
		MetaKeyDeadBranchPruneLastRunTS,
		MetaKeyDeadBranchPruneLastCount,
		MetaKeyDeadBranchPruneLastRefs,
	} {
		v, ok, err := state.MetaGet(ctx, f.db, key)
		if err != nil {
			t.Fatalf("MetaGet %q: %v", key, err)
		}
		if ok {
			t.Fatalf("expected meta %q absent after no-op sweep; got %q", key, v)
		}
	}
}

// TestDivergedHookWritesMetaKeys drives the runtime Diverged-hook helper
// directly with a dead previous ref and asserts the three meta keys are
// stamped. Mirrors the integration in TestDivergedHookRecoversDeadBranchTerminals
// but isolates the meta-write contract.
func TestDivergedHookWritesMetaKeys(t *testing.T) {
	t.Setenv(EnvKeepDeadBranchBarriers, "")
	f := newDaemonFixture(t)
	ctx := context.Background()
	headOID, err := git.RevParse(ctx, f.dir, "HEAD")
	if err != nil {
		t.Fatalf("rev-parse HEAD: %v", err)
	}

	const deadRef = "refs/heads/dead-after-merge"
	seedTerminalEvent(t, f.db, deadRef, 1, headOID, "dead-blocked.txt", state.EventStateBlockedConflict)

	before := time.Now().Unix()
	pruneDeadBranchTerminals(ctx, f.dir, f.db,
		CaptureContext{BranchRef: "refs/heads/main", BranchGeneration: 2, BaseHead: headOID},
		deadRef, 1,
		slog.Default(), nil,
		"diverged hook")
	after := time.Now().Unix()

	tsRaw, ok, err := state.MetaGet(ctx, f.db, MetaKeyDeadBranchPruneLastRunTS)
	if err != nil {
		t.Fatalf("MetaGet last_run_ts: %v", err)
	}
	if !ok {
		t.Fatalf("expected meta %q to be set after Diverged-hook prune", MetaKeyDeadBranchPruneLastRunTS)
	}
	ts, err := strconv.ParseInt(tsRaw, 10, 64)
	if err != nil {
		t.Fatalf("parse last_run_ts: %v", err)
	}
	if ts < before || ts > after {
		t.Fatalf("last_run_ts=%d outside [%d, %d]", ts, before, after)
	}

	countRaw, ok, _ := state.MetaGet(ctx, f.db, MetaKeyDeadBranchPruneLastCount)
	if !ok || countRaw != "1" {
		t.Fatalf("last_count=%q ok=%v want %q", countRaw, ok, "1")
	}
	refsRaw, ok, _ := state.MetaGet(ctx, f.db, MetaKeyDeadBranchPruneLastRefs)
	if !ok {
		t.Fatalf("expected meta %q to be set", MetaKeyDeadBranchPruneLastRefs)
	}
	var refs []string
	if err := json.Unmarshal([]byte(refsRaw), &refs); err != nil {
		t.Fatalf("unmarshal last_refs %q: %v", refsRaw, err)
	}
	if len(refs) != 1 || refs[0] != deadRef {
		t.Fatalf("last_refs=%v want [%q]", refs, deadRef)
	}
}
