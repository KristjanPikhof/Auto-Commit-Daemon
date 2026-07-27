//go:build integration
// +build integration

package integration_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/daemon"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

const selfPublicationMatrixSeed int64 = 0xACD18

// TestCanonicalWriterAfterStateMove pins the split-brain sequence observed in
// the live repository: a daemon holds the legacy lock inside .git/acd, that
// movable state directory is quarantined, and a second daemon attempts to
// acquire ownership through the newly-created state directory.
//
// No process timing is involved. The held flock, directory rename, contention,
// release, and reacquisition form explicit boundaries. The regression contract
// is that ownership remains fenced by the stable Git common directory even
// while the legacy state path moves.
func TestCanonicalWriterAfterStateMove(t *testing.T) {
	repo := tempRepo(t)
	gitDir := filepath.Join(repo, ".git")
	stateDir := filepath.Join(gitDir, "acd")
	movedStateDir := filepath.Join(gitDir, "acd-quarantined")

	first, err := daemon.AcquireDaemonLock(gitDir)
	if err != nil {
		t.Fatalf("acquire first daemon lock: %v", err)
	}
	firstReleased := false
	t.Cleanup(func() {
		if !firstReleased {
			_ = first.Release()
		}
	})

	if err := os.Rename(stateDir, movedStateDir); err != nil {
		t.Fatalf("quarantine movable state directory: %v", err)
	}

	second, err := daemon.AcquireDaemonLock(gitDir)
	if second != nil {
		_ = second.Release()
		t.Fatal("second writer acquired ownership after .git/acd moved")
	}
	if !errors.Is(err, daemon.ErrDaemonLockHeld) {
		t.Fatalf("second writer error=%v want ErrDaemonLockHeld", err)
	}

	if err := first.Release(); err != nil {
		t.Fatalf("release first daemon lock: %v", err)
	}
	firstReleased = true

	reacquired, err := daemon.AcquireDaemonLock(gitDir)
	if err != nil {
		t.Fatalf("reacquire ownership after first writer released: %v", err)
	}
	if err := reacquired.Release(); err != nil {
		t.Fatalf("release reacquired daemon lock: %v", err)
	}
}

func TestCanonicalWriterAcrossLinkedWorktree(t *testing.T) {
	repo := tempRepo(t)
	linked := filepath.Join(t.TempDir(), "linked")
	runGitOK(t, repo, "worktree", "add", "-q", "-b", "linked", linked)
	mainGitDir := strings.TrimSpace(runGitOK(
		t, repo, "rev-parse", "--absolute-git-dir"))
	linkedGitDir := strings.TrimSpace(runGitOK(
		t, linked, "rev-parse", "--absolute-git-dir"))

	first, err := daemon.AcquireDaemonLock(mainGitDir)
	if err != nil {
		t.Fatalf("acquire main-worktree writer: %v", err)
	}
	defer func() { _ = first.Release() }()
	second, err := daemon.AcquireDaemonLock(linkedGitDir)
	if second != nil {
		_ = second.Release()
		t.Fatal("linked worktree acquired a second canonical writer")
	}
	if !errors.Is(err, daemon.ErrDaemonLockHeld) {
		t.Fatalf("linked writer error=%v want ErrDaemonLockHeld", err)
	}
}

func TestSelfPublicationCrashRestartMatrix(t *testing.T) {
	tests := []struct {
		name        string
		phase       string
		applyRef    bool
		wantDone    int
		wantAbandon int
	}{
		{
			name: "prepared", phase: state.SelfPublicationPrepared,
			wantAbandon: 1,
		},
		{
			name: "post-CAS", phase: state.SelfPublicationPrepared,
			applyRef: true, wantDone: 1,
		},
		{
			name: "pre-completion", phase: state.SelfPublicationGitApplied,
			applyRef: true, wantDone: 1,
		},
		{
			name: "post-completion", phase: state.SelfPublicationCompleted,
			applyRef: true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			repo, db, cctx, publication := seedSelfPublicationWorkload(
				t, 1, 0, 0, "event-"+test.name)
			if test.applyRef {
				runGitOK(t, repo, "update-ref", cctx.BranchRef,
					publication.TargetCommitOID, publication.SourceHead)
			}
			if test.phase == state.SelfPublicationGitApplied ||
				test.phase == state.SelfPublicationCompleted {
				if changed, err := state.MarkSelfPublicationGitApplied(
					context.Background(), db, publication, 2); err != nil ||
					!changed {
					t.Fatalf("mark Git applied=(%v,%v)", changed, err)
				}
			}
			if test.phase == state.SelfPublicationCompleted {
				if changed, err := state.CompleteSelfPublication(
					context.Background(), db, publication,
					selfPublicationCompletion(publication)); err != nil ||
					!changed {
					t.Fatalf("complete publication=(%v,%v)", changed, err)
				}
			}

			summary, err := daemon.RecoverSelfPublications(
				context.Background(), repo, db, cctx, daemon.ReplayOpts{})
			if err != nil {
				t.Fatalf("recover %s: %v", test.name, err)
			}
			if summary.Completed != test.wantDone ||
				summary.Abandoned != test.wantAbandon {
				t.Fatalf("summary=%+v want completed=%d abandoned=%d",
					summary, test.wantDone, test.wantAbandon)
			}
			// Restart recovery must be an idempotent no-op for every terminal
			// outcome, including a pre-CAS abandon.
			restarted, err := daemon.RecoverSelfPublications(
				context.Background(), repo, db, cctx, daemon.ReplayOpts{})
			if err != nil || restarted.Inspected != 0 {
				t.Fatalf("restart recovery=(%+v,%v) want no-op", restarted, err)
			}
			if test.wantDone == 1 || test.phase == state.SelfPublicationCompleted {
				assertSelfPublicationOracle(t, repo, db,
					selfPublicationOracle{
						SourceHead: publication.SourceHead,
						TargetHead: publication.TargetCommitOID,
						EventCount: 1, BranchGeneration: 1,
						WantCleanQueue: true,
					})
			} else {
				var eventState string
				if err := db.SQL().QueryRow(`
SELECT state FROM capture_events`).Scan(&eventState); err != nil {
					t.Fatal(err)
				}
				if eventState != state.EventStatePending {
					t.Fatalf("pre-CAS event=%s want pending", eventState)
				}
			}
		})
	}
}

func TestSelfPublicationDaemonRestartRecovery(t *testing.T) {
	t.Setenv("ACD_COMMIT_STRATEGY", "event")
	t.Setenv("XDG_CONFIG_HOME", filepath.Join(t.TempDir(), "config"))
	t.Setenv("XDG_STATE_HOME", filepath.Join(t.TempDir(), "state"))
	t.Setenv("XDG_DATA_HOME", filepath.Join(t.TempDir(), "share"))
	repo, db, cctx, publication := seedSelfPublicationWorkload(
		t, 1, 0, 0, "real-daemon-restart")
	runGitOK(t, repo, "update-ref", cctx.BranchRef,
		publication.TargetCommitOID, publication.SourceHead)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := state.RegisterClient(ctx, db, state.Client{
		SessionID: "self-publication-restart",
		Harness:   "integration",
	}); err != nil {
		t.Fatalf("register restart client: %v", err)
	}

	wakeCh := make(chan struct{}, 1)
	shutdownCh := make(chan struct{}, 1)
	var wg sync.WaitGroup
	var runErr error
	wg.Add(1)
	started := time.Now()
	go func() {
		defer wg.Done()
		runErr = daemon.Run(ctx, daemon.Options{
			RepoPath: repo, GitDir: filepath.Join(repo, ".git"), DB: db,
			MessageFn: daemon.DeterministicMessage,
			Scheduler: daemon.Scheduler{
				Base: 5 * time.Millisecond, IdleCeiling: 5 * time.Millisecond,
				ErrorCeiling: 5 * time.Millisecond,
			},
			BootGrace: 30 * time.Second,
			WakeCh:    wakeCh, ShutdownCh: shutdownCh, SkipSignals: true,
		})
	}()
	t.Cleanup(func() {
		cancel()
		wg.Wait()
	})

	waitFor(t, "daemon restart completes journal", 5*time.Second, func() bool {
		var phase, eventState string
		var commitOID sql.NullString
		if err := db.SQL().QueryRowContext(ctx, `
SELECT phase FROM self_publications WHERE id=?`,
			publication.ID).Scan(&phase); err != nil {
			return false
		}
		if err := db.SQL().QueryRowContext(ctx, `
SELECT state, commit_oid FROM capture_events ORDER BY seq LIMIT 1`,
		).Scan(&eventState, &commitOID); err != nil {
			return false
		}
		daemonState, running, err := state.LoadDaemonState(ctx, db)
		if err != nil || !running || daemonState.Mode != "running" {
			return false
		}
		var token string
		if err := db.SQL().QueryRowContext(ctx, `
SELECT value FROM daemon_meta WHERE key='branch_token'`,
		).Scan(&token); err != nil {
			return false
		}
		var shadowReady string
		if err := db.SQL().QueryRowContext(ctx, `
SELECT value FROM daemon_meta WHERE key=?`,
			daemon.ShadowBootstrappedKey(
				cctx.BranchRef, cctx.BranchGeneration),
		).Scan(&shadowReady); err != nil {
			return false
		}
		return phase == state.SelfPublicationCompleted &&
			eventState == state.EventStatePublished &&
			commitOID.Valid &&
			commitOID.String == publication.TargetCommitOID &&
			token == "rev:"+publication.TargetCommitOID+" "+cctx.BranchRef &&
			shadowReady == "1"
	})
	recoveryLatency := time.Since(started)
	restartState, ok, err := state.LoadDaemonState(ctx, db)
	if err != nil || !ok {
		t.Fatalf("load restart heartbeat: ok=%v err=%v", ok, err)
	}
	waitForDaemonHeartbeatAfter(t, ctx, db, restartState.HeartbeatTS)
	cancel()
	wg.Wait()
	if runErr != nil {
		t.Fatalf("restart daemon Run: %v", runErr)
	}
	assertSelfPublicationOracle(t, repo, db, selfPublicationOracle{
		SourceHead: publication.SourceHead,
		TargetHead: publication.TargetCommitOID,
		EventCount: 1, BranchGeneration: 1,
		WantCleanQueue: true,
	})
	t.Logf("restart target=%s recovery_latency=%s",
		publication.TargetCommitOID, recoveryLatency)
}

func TestSelfPublicationIntentCapacityOracle(t *testing.T) {
	repo, db, cctx, publication := seedSelfPublicationWorkload(
		t,
		state.IntentCandidateMaxCaptures,
		state.IntentCandidateMaxOpenPerPair,
		state.IntentDependencyMaxPerPair,
		fmt.Sprintf("intent-capacity-%x", selfPublicationMatrixSeed),
	)
	runGitOK(t, repo, "update-ref", cctx.BranchRef,
		publication.TargetCommitOID, publication.SourceHead)
	if changed, err := state.MarkSelfPublicationGitApplied(
		context.Background(), db, publication, 2); err != nil || !changed {
		t.Fatalf("mark capacity publication Git applied=(%v,%v)", changed, err)
	}
	started := time.Now()
	summary, err := daemon.RecoverSelfPublications(
		context.Background(), repo, db, cctx, daemon.ReplayOpts{})
	elapsed := time.Since(started)
	if err != nil {
		t.Fatalf("recover capacity publication: %v", err)
	}
	if summary.Completed != 1 ||
		summary.FinalTargetOID != publication.TargetCommitOID {
		t.Fatalf("capacity recovery summary=%+v", summary)
	}
	var candidates, edges int
	if err := db.SQL().QueryRow(`
SELECT COUNT(*) FROM intent_candidates WHERE status='published'`,
	).Scan(&candidates); err != nil {
		t.Fatal(err)
	}
	if err := db.SQL().QueryRow(`
SELECT COUNT(*) FROM intent_capture_dependencies`,
	).Scan(&edges); err != nil {
		t.Fatal(err)
	}
	if candidates != state.IntentCandidateMaxOpenPerPair ||
		edges != state.IntentDependencyMaxPerPair {
		t.Fatalf("capacity candidates=%d edges=%d want %d/%d",
			candidates, edges, state.IntentCandidateMaxOpenPerPair,
			state.IntentDependencyMaxPerPair)
	}
	assertSelfPublicationOracle(t, repo, db, selfPublicationOracle{
		SourceHead:       publication.SourceHead,
		TargetHead:       publication.TargetCommitOID,
		EventCount:       state.IntentCandidateMaxCaptures,
		BranchGeneration: cctx.BranchGeneration,
		WantCleanQueue:   true,
	})
	t.Logf(
		"seed=%x captures=%d candidates=%d edges=%d recovery=%s target=%s",
		selfPublicationMatrixSeed, state.IntentCandidateMaxCaptures,
		state.IntentCandidateMaxOpenPerPair,
		state.IntentDependencyMaxPerPair, elapsed,
		publication.TargetCommitOID)
}

func TestSelfPublicationSixtyFourCommitChain(t *testing.T) {
	const commitCount = 64
	repo := tempRepo(t)
	ctx := context.Background()
	db, err := state.Open(ctx, filepath.Join(repo, ".git", "acd", "state.db"))
	if err != nil {
		t.Fatalf("open sequential publication state: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	initial := strings.TrimSpace(runGitOK(t, repo, "rev-parse", "HEAD"))
	source := initial
	cctx := daemon.CaptureContext{
		BranchRef: "refs/heads/main", BranchGeneration: 1,
		BaseHead: initial,
	}
	started := time.Now()
	for i := 0; i < commitCount; i++ {
		path := fmt.Sprintf("sequential/%02d.txt", i)
		writeFile(t, filepath.Join(repo, path),
			fmt.Sprintf("self-publication seed=%x commit=%d\n",
				selfPublicationMatrixSeed, i))
		afterOID := strings.TrimSpace(runGitOK(
			t, repo, "hash-object", "-w", "--", path))
		runGitOK(t, repo, "add", "--", path)
		tree := strings.TrimSpace(runGitOK(t, repo, "write-tree"))
		target := strings.TrimSpace(runGitOK(
			t, repo, "commit-tree", tree, "-p", source, "-m",
			fmt.Sprintf("sequential publication %02d", i)))
		seq, err := state.AppendCaptureEvent(ctx, db,
			state.CaptureEvent{
				BranchRef:        cctx.BranchRef,
				BranchGeneration: cctx.BranchGeneration,
				BaseHead:         source, Operation: "create",
				Path:     path,
				Fidelity: "full",
			}, []state.CaptureOp{{
				Op: "create", Path: path,
				AfterOID: sql.NullString{String: afterOID, Valid: true},
				AfterMode: sql.NullString{
					String: "100644", Valid: true,
				},
				Fidelity: "full",
			}})
		if err != nil {
			t.Fatalf("append sequential event %d: %v", i, err)
		}
		publication := state.SelfPublication{
			ID:               fmt.Sprintf("sequential-%02d", i),
			BranchRef:        cctx.BranchRef,
			BranchGeneration: cctx.BranchGeneration,
			SourceHead:       source, TargetCommitOID: target,
			TargetTreeOID: tree,
			Members:       []state.SelfPublicationMember{{EventSeq: seq}},
			Completion: state.SelfPublicationCompletion{
				PublishedTS:     float64(i + 1),
				CandidateStatus: state.IntentCandidatePublished,
			},
		}
		if created, err := state.PrepareSelfPublication(
			ctx, db, publication); err != nil || !created {
			t.Fatalf("prepare sequential %d=(%v,%v)", i, created, err)
		}
		runGitOK(t, repo, "update-ref", cctx.BranchRef, target, source)
		if changed, err := state.MarkSelfPublicationGitApplied(
			ctx, db, publication, float64(i+1)); err != nil || !changed {
			t.Fatalf("mark sequential %d=(%v,%v)", i, changed, err)
		}
		summary, err := daemon.RecoverSelfPublications(
			ctx, repo, db, cctx, daemon.ReplayOpts{})
		if err != nil || summary.Completed != 1 ||
			summary.FinalTargetOID != target {
			t.Fatalf("recover sequential %d=(%+v,%v)", i, summary, err)
		}
		source = target
		cctx.BaseHead = target
	}
	elapsed := time.Since(started)
	assertSelfPublicationOracle(t, repo, db, selfPublicationOracle{
		SourceHead: initial, TargetHead: source,
		EventCount: commitCount, BranchGeneration: 1,
		WantCleanQueue: true,
	})
	t.Logf("seed=%x sequential_commits=%d elapsed=%s final=%s",
		selfPublicationMatrixSeed, commitCount, elapsed, source)
}

func TestSelfPublicationExternalGitInterleavings(t *testing.T) {
	t.Run("external-fast-forward", func(t *testing.T) {
		repo, db, cctx, publication := seedSelfPublicationWorkload(
			t, 1, 0, 0, "external-fast-forward")
		runGitOK(t, repo, "update-ref", cctx.BranchRef,
			publication.TargetCommitOID, publication.SourceHead)
		external := strings.TrimSpace(runGitOK(
			t, repo, "commit-tree", publication.TargetTreeOID,
			"-p", publication.TargetCommitOID, "-m", "external child"))
		runGitOK(t, repo, "update-ref", cctx.BranchRef,
			external, publication.TargetCommitOID)

		_, err := daemon.RecoverSelfPublications(
			context.Background(), repo, db, cctx, daemon.ReplayOpts{})
		if !errors.Is(err, daemon.ErrSelfPublicationRecoveryAmbiguous) {
			t.Fatalf("external fast-forward recovery err=%v want ambiguous", err)
		}
		assertSelfPublicationPreserved(
			t, db, publication.ID, state.SelfPublicationPrepared)
		if head := strings.TrimSpace(runGitOK(
			t, repo, "rev-parse", "HEAD")); head != external {
			t.Fatalf("external HEAD=%s want preserved %s", head, external)
		}
	})

	t.Run("reset-rewind", func(t *testing.T) {
		repo, db, cctx, publication := seedSelfPublicationWorkload(
			t, 1, 0, 0, "reset-rewind")
		runGitOK(t, repo, "update-ref", cctx.BranchRef,
			publication.TargetCommitOID, publication.SourceHead)
		runGitOK(t, repo, "update-ref", cctx.BranchRef,
			publication.SourceHead, publication.TargetCommitOID)
		summary, err := daemon.RecoverSelfPublications(
			context.Background(), repo, db, cctx, daemon.ReplayOpts{})
		if err != nil || summary.Abandoned != 1 {
			t.Fatalf("rewind recovery=(%+v,%v) want safe abandon",
				summary, err)
		}
		assertSelfPublicationPreserved(
			t, db, publication.ID, state.SelfPublicationAbandoned)
	})

	t.Run("rebase-marker", func(t *testing.T) {
		t.Setenv("ACD_COMMIT_STRATEGY", "event")
		t.Setenv("XDG_CONFIG_HOME", filepath.Join(t.TempDir(), "config"))
		t.Setenv("XDG_STATE_HOME", filepath.Join(t.TempDir(), "state"))
		t.Setenv("XDG_DATA_HOME", filepath.Join(t.TempDir(), "share"))
		repo, db, _, publication := seedSelfPublicationWorkload(
			t, 1, 0, 0, "rebase-marker")
		marker := filepath.Join(repo, ".git", "rebase-merge")
		if err := os.Mkdir(marker, 0o755); err != nil {
			t.Fatalf("create rebase marker: %v", err)
		}
		if name, active := daemon.GitOperationInProgress(
			filepath.Join(repo, ".git")); !active || name != "rebase-merge" {
			t.Fatalf("git operation=(%q,%v) want rebase-merge", name, active)
		}
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		if err := state.RegisterClient(ctx, db, state.Client{
			SessionID: "self-publication-rebase",
			Harness:   "integration",
		}); err != nil {
			t.Fatalf("register rebase client: %v", err)
		}
		wakeCh := make(chan struct{}, 1)
		shutdownCh := make(chan struct{}, 1)
		var wg sync.WaitGroup
		var runErr error
		wg.Add(1)
		go func() {
			defer wg.Done()
			runErr = daemon.Run(ctx, daemon.Options{
				RepoPath: repo, GitDir: filepath.Join(repo, ".git"), DB: db,
				MessageFn: daemon.DeterministicMessage,
				Scheduler: daemon.Scheduler{
					Base:         5 * time.Millisecond,
					IdleCeiling:  5 * time.Millisecond,
					ErrorCeiling: 5 * time.Millisecond,
				},
				BootGrace: 30 * time.Second,
				WakeCh:    wakeCh, ShutdownCh: shutdownCh, SkipSignals: true,
			})
		}()
		t.Cleanup(func() {
			cancel()
			wg.Wait()
		})
		waitFor(t, "rebase operation recorded", 5*time.Second, func() bool {
			value, ok, err := state.MetaGet(
				ctx, db, daemon.MetaKeyOperationInProgress)
			return err == nil && ok && value == "rebase-merge"
		})
		assertSelfPublicationPreserved(
			t, db, publication.ID, state.SelfPublicationPrepared)
		if err := os.RemoveAll(marker); err != nil {
			t.Fatalf("remove rebase marker: %v", err)
		}
		select {
		case wakeCh <- struct{}{}:
		default:
		}
		waitFor(t, "post-rebase publication converges", 5*time.Second,
			func() bool {
				var phase string
				err := db.SQL().QueryRowContext(ctx, `
SELECT phase FROM self_publications WHERE id=?`,
					publication.ID).Scan(&phase)
				return err == nil &&
					phase == state.SelfPublicationAbandoned
			})
		resumedState, ok, err := state.LoadDaemonState(ctx, db)
		if err != nil || !ok {
			t.Fatalf("load post-rebase heartbeat: ok=%v err=%v", ok, err)
		}
		waitForDaemonHeartbeatAfter(t, ctx, db, resumedState.HeartbeatTS)
		cancel()
		wg.Wait()
		if runErr != nil {
			t.Fatalf("rebase daemon Run: %v", runErr)
		}
	})

	t.Run("branch-switch", func(t *testing.T) {
		repo, db, _, publication := seedSelfPublicationWorkload(
			t, 1, 0, 0, "branch-switch")
		runGitOK(t, repo, "switch", "-q", "-c", "other")
		otherHead := strings.TrimSpace(runGitOK(
			t, repo, "rev-parse", "HEAD"))
		summary, err := daemon.RecoverSelfPublications(
			context.Background(), repo, db, daemon.CaptureContext{
				BranchRef: "refs/heads/other", BranchGeneration: 2,
				BaseHead: otherHead,
			}, daemon.ReplayOpts{})
		if err != nil || summary.Inspected != 0 {
			t.Fatalf("switched-pair recovery=(%+v,%v) want no-op",
				summary, err)
		}
		assertSelfPublicationPreserved(
			t, db, publication.ID, state.SelfPublicationPrepared)
	})
}

func TestSelfPublicationFiveThousandWakeLiveness(t *testing.T) {
	const wakeCount = 5000
	t.Setenv("ACD_COMMIT_STRATEGY", "event")
	t.Setenv("XDG_CONFIG_HOME", filepath.Join(t.TempDir(), "config"))
	t.Setenv("XDG_STATE_HOME", filepath.Join(t.TempDir(), "state"))
	t.Setenv("XDG_DATA_HOME", filepath.Join(t.TempDir(), "share"))
	repo := tempRepo(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	gitDir := filepath.Join(repo, ".git")
	dbPath := filepath.Join(gitDir, "acd", "state.db")
	db, err := state.Open(ctx, dbPath)
	if err != nil {
		t.Fatalf("open wake liveness state: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if err := state.RegisterClient(ctx, db, state.Client{
		SessionID: "self-publication-wake-stress",
		Harness:   "integration",
	}); err != nil {
		t.Fatalf("register wake client: %v", err)
	}
	SeedFlushRequests(t, dbPath, wakeCount)

	wakeCh := make(chan struct{}, 1)
	shutdownCh := make(chan struct{}, 1)
	var wg sync.WaitGroup
	var runErr error
	wg.Add(1)
	started := time.Now()
	go func() {
		defer wg.Done()
		runErr = daemon.Run(ctx, daemon.Options{
			RepoPath: repo, GitDir: gitDir, DB: db,
			MessageFn: daemon.DeterministicMessage,
			Scheduler: daemon.Scheduler{
				Base: 2 * time.Millisecond, IdleCeiling: 2 * time.Millisecond,
				ErrorCeiling: 2 * time.Millisecond,
			},
			BootGrace: 30 * time.Second,
			WakeCh:    wakeCh, ShutdownCh: shutdownCh, SkipSignals: true,
		})
	}()
	t.Cleanup(func() {
		cancel()
		wg.Wait()
	})

	waitFor(t, "wake daemon owns state", 5*time.Second, func() bool {
		daemonState, ok, err := state.LoadDaemonState(ctx, db)
		return err == nil && ok && daemonState.PID > 0
	})
	second, lockErr := daemon.AcquireDaemonLock(gitDir)
	if second != nil {
		_ = second.Release()
		t.Fatal("second writer acquired lock during 5000-wake drain")
	}
	if !errors.Is(lockErr, daemon.ErrDaemonLockHeld) {
		t.Fatalf("second writer err=%v want ErrDaemonLockHeld", lockErr)
	}

	var lastHeartbeat time.Time
	heartbeatSamples := 0
	maxHeartbeatGap := time.Duration(0)
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	deadline := time.NewTimer(60 * time.Second)
	defer deadline.Stop()
	for {
		var pending int
		if err := db.SQL().QueryRowContext(ctx, `
SELECT COUNT(*) FROM flush_requests
WHERE status IN ('pending','acknowledged')`).Scan(&pending); err != nil {
			t.Fatalf("query wake queue: %v", err)
		}
		if pending == 0 {
			break
		}
		select {
		case <-ticker.C:
			daemonState, ok, err := state.LoadDaemonState(ctx, db)
			if err != nil {
				t.Fatalf("load wake heartbeat: %v", err)
			}
			if ok && daemonState.HeartbeatTS > 0 {
				observed := time.Unix(
					int64(daemonState.HeartbeatTS),
					int64((daemonState.HeartbeatTS-
						float64(int64(daemonState.HeartbeatTS)))*1e9),
				)
				if observed.After(lastHeartbeat) {
					if !lastHeartbeat.IsZero() {
						if gap := observed.Sub(lastHeartbeat); gap > maxHeartbeatGap {
							maxHeartbeatGap = gap
						}
					}
					heartbeatSamples++
					lastHeartbeat = observed
				}
			}
		case <-deadline.C:
			t.Fatal("5000 wake requests did not drain within 60s")
		}
	}
	wakeLatency := time.Since(started)
	if wakeLatency > 60*time.Second {
		t.Fatalf("wake acknowledgement=%s exceeds 60s", wakeLatency)
	}
	if maxHeartbeatGap > 3*time.Second {
		t.Fatalf("heartbeat maximum gap=%s exceeds 3s", maxHeartbeatGap)
	}
	if wakeLatency > 3*time.Second && heartbeatSamples < 2 {
		t.Fatalf(
			"heartbeat samples=%d during %s wake drain; want at least 2",
			heartbeatSamples, wakeLatency)
	}
	waitFor(t, "post-drain heartbeat", 3*time.Second, func() bool {
		daemonState, ok, err := state.LoadDaemonState(ctx, db)
		if err != nil || !ok || daemonState.HeartbeatTS <= 0 {
			return false
		}
		observed := time.Unix(
			int64(daemonState.HeartbeatTS),
			int64((daemonState.HeartbeatTS-
				float64(int64(daemonState.HeartbeatTS)))*1e9),
		)
		return observed.After(lastHeartbeat)
	})
	cancel()
	wg.Wait()
	if runErr != nil {
		t.Fatalf("wake daemon Run: %v", runErr)
	}
	var completed int
	if err := db.SQL().QueryRow(`
SELECT COUNT(*) FROM flush_requests WHERE status='completed'`,
	).Scan(&completed); err != nil {
		t.Fatal(err)
	}
	if completed != wakeCount {
		t.Fatalf("completed wakes=%d want=%d", completed, wakeCount)
	}
	t.Logf("seed=%x wakes=%d completed=%d heartbeat_max=%s wake_latency=%s",
		selfPublicationMatrixSeed, wakeCount, completed,
		maxHeartbeatGap, wakeLatency)
}

func assertSelfPublicationPreserved(
	t *testing.T,
	db *state.DB,
	id string,
	wantPhase string,
) {
	t.Helper()
	var phase, eventState string
	if err := db.SQL().QueryRow(`
SELECT phase FROM self_publications WHERE id=?`, id).Scan(&phase); err != nil {
		t.Fatalf("query publication phase: %v", err)
	}
	if err := db.SQL().QueryRow(`
SELECT state FROM capture_events ORDER BY seq LIMIT 1`,
	).Scan(&eventState); err != nil {
		t.Fatalf("query event state: %v", err)
	}
	if phase != wantPhase || eventState != state.EventStatePending {
		t.Fatalf("publication phase=%s event=%s want %s/pending",
			phase, eventState, wantPhase)
	}
	var archives int
	if err := db.SQL().QueryRow(`
SELECT COUNT(*) FROM recovery_snapshots`).Scan(&archives); err != nil {
		t.Fatal(err)
	}
	if archives != 0 {
		t.Fatalf("recovery archives=%d want 0", archives)
	}
}

func waitForDaemonHeartbeatAfter(
	t *testing.T,
	ctx context.Context,
	db *state.DB,
	after float64,
) {
	t.Helper()
	waitFor(t, "next daemon heartbeat", 3*time.Second, func() bool {
		daemonState, ok, err := state.LoadDaemonState(ctx, db)
		return err == nil && ok && daemonState.HeartbeatTS > after
	})
}

func seedSelfPublicationWorkload(
	t *testing.T,
	eventCount int,
	candidateCount int,
	edgeCount int,
	id string,
) (string, *state.DB, daemon.CaptureContext, state.SelfPublication) {
	t.Helper()
	repo := tempRepo(t)
	ctx := context.Background()
	db, err := state.Open(ctx, filepath.Join(repo, ".git", "acd", "state.db"))
	if err != nil {
		t.Fatalf("open publication state: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	source := strings.TrimSpace(runGitOK(t, repo, "rev-parse", "HEAD"))
	cctx := daemon.CaptureContext{
		BranchRef: "refs/heads/main", BranchGeneration: 1,
		BaseHead: source,
	}
	members := make([]state.SelfPublicationMember, 0, eventCount)
	seqs := make([]int64, 0, eventCount)
	for i := 0; i < eventCount; i++ {
		path := fmt.Sprintf("matrix/%03d.txt", i)
		writeFile(t, filepath.Join(repo, path),
			fmt.Sprintf("self-publication seed=%x event=%d\n",
				selfPublicationMatrixSeed, i))
		afterOID := strings.TrimSpace(runGitOK(
			t, repo, "hash-object", "-w", "--", path))
		seq, appendErr := state.AppendCaptureEvent(ctx, db,
			state.CaptureEvent{
				BranchRef:        cctx.BranchRef,
				BranchGeneration: cctx.BranchGeneration,
				BaseHead:         source, Operation: "create",
				Path:     path,
				Fidelity: "full",
			}, []state.CaptureOp{{
				Op: "create", Path: path,
				AfterOID: sql.NullString{String: afterOID, Valid: true},
				AfterMode: sql.NullString{
					String: "100644", Valid: true,
				},
				Fidelity: "full",
			}})
		if appendErr != nil {
			t.Fatalf("append matrix event %d: %v", i, appendErr)
		}
		seqs = append(seqs, seq)
		members = append(members, state.SelfPublicationMember{EventSeq: seq})
	}
	runGitOK(t, repo, "add", "--", "matrix")
	tree := strings.TrimSpace(runGitOK(t, repo, "write-tree"))
	target := strings.TrimSpace(runGitOK(
		t, repo, "commit-tree", tree, "-p", source, "-m", id))
	if candidateCount > 0 {
		for i := 0; i < candidateCount; i++ {
			candidateID := fmt.Sprintf("matrix-candidate-%03d", i)
			start := i * eventCount / candidateCount
			end := (i + 1) * eventCount / candidateCount
			events := make([]state.IntentCandidateEvent, 0, end-start)
			for ord, seq := range seqs[start:end] {
				events = append(events, state.IntentCandidateEvent{
					Ord: ord, EventSeq: seq, EventRole: "code",
				})
				members[start+ord].CandidateID = sql.NullString{
					String: candidateID, Valid: true,
				}
			}
			if err := state.SaveIntentCandidate(ctx, db,
				state.IntentCandidate{
					ID: candidateID, BranchRef: cctx.BranchRef,
					BranchGeneration: cctx.BranchGeneration,
					Status:           state.IntentCandidateReady,
					Readiness:        state.IntentReadinessReady,
					Events:           events,
				}); err != nil {
				t.Fatalf("save candidate %d: %v", i, err)
			}
		}
	}
	dependencies := make([]state.IntentCaptureDependency, 0, edgeCount)
	for prerequisite := 0; len(dependencies) < edgeCount; prerequisite++ {
		for dependent := 0; dependent < len(seqs) && len(dependencies) < edgeCount; dependent++ {
			if prerequisite%len(seqs) == dependent {
				continue
			}
			dependencies = append(dependencies,
				state.IntentCaptureDependency{
					PrerequisiteSeq: seqs[prerequisite%len(seqs)],
					DependentSeq:    seqs[dependent],
					Strength:        state.IntentDependencyHard,
					Kind:            "matrix", Evidence: strconv.Itoa(len(dependencies)),
				})
		}
	}
	if err := state.ReplaceIntentCaptureDependencies(
		ctx, db, cctx.BranchRef, cctx.BranchGeneration,
		dependencies); err != nil {
		t.Fatalf("save %d dependency edges: %v", edgeCount, err)
	}
	publication := state.SelfPublication{
		ID: id, BranchRef: cctx.BranchRef,
		BranchGeneration: cctx.BranchGeneration,
		SourceHead:       source, TargetCommitOID: target, TargetTreeOID: tree,
		Members: members,
		Completion: state.SelfPublicationCompletion{
			PublishedTS:     3,
			CandidateStatus: state.IntentCandidatePublished,
		},
	}
	if created, err := state.PrepareSelfPublication(
		ctx, db, publication); err != nil || !created {
		t.Fatalf("prepare self-publication=(%v,%v)", created, err)
	}
	return repo, db, cctx, publication
}

func selfPublicationCompletion(
	publication state.SelfPublication,
) state.SelfPublicationCompletion {
	return state.SelfPublicationCompletion{
		PublishedTS:     3,
		CandidateStatus: state.IntentCandidatePublished,
		BranchToken: "rev:" + publication.TargetCommitOID + " " +
			publication.BranchRef,
	}
}
