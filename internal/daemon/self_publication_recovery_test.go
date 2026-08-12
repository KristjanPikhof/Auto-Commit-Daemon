package daemon

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

func TestRecoverSelfPublicationPreparedBeforeCASAbandonsWithoutEventTransition(
	t *testing.T,
) {
	f := newCaptureFixture(t)
	ctx := context.Background()
	publication, seq, _ := seedRecoverableSelfPublication(
		t, ctx, f, state.SelfPublicationPrepared)

	summary, err := RecoverSelfPublications(
		ctx, f.dir, f.db, f.cctx, ReplayOpts{})
	if err != nil {
		t.Fatalf("RecoverSelfPublications: %v", err)
	}
	if summary.Inspected != 1 || summary.Abandoned != 1 ||
		summary.Completed != 0 || summary.FinalTargetOID != "" {
		t.Fatalf("summary=%+v", summary)
	}
	assertSelfPublicationRecoveryEvent(
		t, ctx, f.db, seq, state.EventStatePending, "")
	assertSelfPublicationRecoveryPhase(
		t, ctx, f.db, publication.ID, state.SelfPublicationAbandoned)
}

func TestRecoverSelfPublicationsBeforePlanningSettlesJournal(t *testing.T) {
	f := newCaptureFixture(t)
	ctx := context.Background()
	if err := state.MetaSetMany(ctx, f.db, map[string]string{
		MetaKeyBranchGeneration: "1",
		MetaKeyBranchHead:       f.cctx.BaseHead,
	}); err != nil {
		t.Fatal(err)
	}
	publication, seq, _ := seedRecoverableSelfPublication(
		t, ctx, f, state.SelfPublicationPrepared)
	if err := RecoverSelfPublicationsBeforePlanning(ctx, f.dir, f.db); err != nil {
		t.Fatal(err)
	}
	assertSelfPublicationRecoveryEvent(
		t, ctx, f.db, seq, state.EventStatePending, "")
	assertSelfPublicationRecoveryPhase(
		t, ctx, f.db, publication.ID, state.SelfPublicationAbandoned)
}

func TestRecoverSelfPublicationsLimitReportsRemainingWork(t *testing.T) {
	f := newCaptureFixture(t)
	ctx := context.Background()
	tree, err := resolveTreeOID(ctx, f.dir, f.cctx.BaseHead)
	if err != nil {
		t.Fatal(err)
	}
	var publications []state.SelfPublication
	for i := 0; i < 2; i++ {
		seq := appendSelfPublicationRecoveryEvent(
			t, ctx, f, fmt.Sprintf("bounded-%d.txt", i))
		target, err := git.CommitTree(
			ctx, f.dir, tree, fmt.Sprintf("bounded %d", i),
			f.cctx.BaseHead)
		if err != nil {
			t.Fatal(err)
		}
		publications = append(publications,
			prepareSelfPublicationRecoveryJournal(
				t, ctx, f, seq, target, tree))
	}

	first, err := RecoverSelfPublications(
		ctx, f.dir, f.db, f.cctx, ReplayOpts{Limit: 1})
	if err != nil {
		t.Fatal(err)
	}
	if first.Inspected != 1 || first.Abandoned != 1 || !first.HasMore {
		t.Fatalf("first summary=%+v", first)
	}
	assertSelfPublicationRecoveryPhase(
		t, ctx, f.db, publications[0].ID, state.SelfPublicationAbandoned)
	assertSelfPublicationRecoveryPhase(
		t, ctx, f.db, publications[1].ID, state.SelfPublicationPrepared)

	second, err := RecoverSelfPublications(
		ctx, f.dir, f.db, f.cctx, ReplayOpts{Limit: 1})
	if err != nil {
		t.Fatal(err)
	}
	if second.Inspected != 1 || second.Abandoned != 1 || second.HasMore {
		t.Fatalf("second summary=%+v", second)
	}
	assertSelfPublicationRecoveryPhase(
		t, ctx, f.db, publications[1].ID, state.SelfPublicationAbandoned)
}

func TestRunRecoveryHasMoreRequestsImmediateFollowup(t *testing.T) {
	f := newCaptureFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tree, err := resolveTreeOID(ctx, f.dir, f.cctx.BaseHead)
	if err != nil {
		t.Fatal(err)
	}
	publications := make([]state.SelfPublication, 0, 3)
	for i := 0; i < 3; i++ {
		seq := appendSelfPublicationRecoveryEvent(
			t, ctx, f, fmt.Sprintf("run-loop-bounded-%d.txt", i))
		target, err := git.CommitTree(
			ctx, f.dir, tree, fmt.Sprintf("run-loop bounded %d", i),
			f.cctx.BaseHead)
		if err != nil {
			t.Fatal(err)
		}
		publications = append(publications,
			prepareSelfPublicationRecoveryJournal(
				t, ctx, f, seq, target, tree))
	}

	decisionCh := make(chan struct{}, 1)
	shutdownCh := make(chan struct{})
	runErrCh := make(chan error, 1)
	go func() {
		runErrCh <- Run(ctx, Options{
			RepoPath:  f.dir,
			GitDir:    f.gitDir,
			DB:        f.db,
			MessageFn: DeterministicMessage,
			Scheduler: Scheduler{
				Base:         time.Second,
				IdleCeiling:  5 * time.Second,
				ErrorCeiling: 5 * time.Second,
			},
			BootGrace:     time.Hour,
			PruneInterval: time.Hour,
			ShutdownCh:    shutdownCh,
			SkipSignals:   true,
			afterRunLoopWorkDecision: func(
				hadWork, recoveryFollowup bool,
			) {
				if hadWork && recoveryFollowup {
					select {
					case decisionCh <- struct{}{}:
					default:
					}
				}
			},
		})
	}()

	select {
	case <-decisionCh:
	case <-time.After(10 * time.Second):
		t.Fatal("recovery HasMore did not request an immediate scheduler followup")
	}
	followupStarted := time.Now()
	waitFor(t, 1500*time.Millisecond,
		"next bounded recovery pass abandons the final journal", func() bool {
			for _, publication := range publications {
				got, ok, err := state.SelfPublicationByID(
					ctx, f.db, publication.ID)
				if err != nil || !ok ||
					got.Phase != state.SelfPublicationAbandoned {
					return false
				}
			}
			return true
		})
	if elapsed := time.Since(followupStarted); elapsed >= 1500*time.Millisecond {
		t.Fatalf("recovery followup took %v; idle backoff was not reset", elapsed)
	}
	cancel()
	select {
	case err := <-runErrCh:
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Run did not stop after cancellation")
	}
}

func TestRunStartupRecoveryCancelsOnShutdown(t *testing.T) {
	f := newCaptureFixture(t)
	shutdownCh := make(chan struct{}, 1)
	recoveryStarted := make(chan struct{})
	runErrCh := make(chan error, 1)
	go func() {
		runErrCh <- Run(context.Background(), Options{
			RepoPath:      f.dir,
			GitDir:        f.gitDir,
			DB:            f.db,
			MessageFn:     DeterministicMessage,
			BootGrace:     time.Hour,
			PruneInterval: time.Hour,
			ShutdownCh:    shutdownCh,
			SkipSignals:   true,
			recoverSelfPublications: func(
				ctx context.Context,
				_ string,
				_ *state.DB,
				_ CaptureContext,
				_ ReplayOpts,
			) (SelfPublicationRecoverySummary, error) {
				close(recoveryStarted)
				<-ctx.Done()
				return SelfPublicationRecoverySummary{}, ctx.Err()
			},
		})
	}()

	select {
	case <-recoveryStarted:
	case <-time.After(10 * time.Second):
		t.Fatal("startup recovery did not begin")
	}
	start := time.Now()
	shutdownCh <- struct{}{}
	select {
	case err := <-runErrCh:
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
		if elapsed := time.Since(start); elapsed > time.Second {
			t.Fatalf("shutdown took %v while recovery was in flight", elapsed)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Run did not stop after shutdown canceled recovery")
	}
	waitForDaemonModeFresh(t, f.db.Path(), "stopped", 5*time.Second)
}

func TestRecoverSelfPublicationUnbornSourceConverges(t *testing.T) {
	for _, applied := range []bool{false, true} {
		t.Run(fmt.Sprintf("applied=%v", applied), func(t *testing.T) {
			f := newCaptureFixture(t)
			ctx := context.Background()
			cctx := CaptureContext{
				BranchRef:        "refs/heads/unborn",
				BranchGeneration: 2,
			}
			seq, err := state.AppendCaptureEvent(
				ctx, f.db, state.CaptureEvent{
					BranchRef: cctx.BranchRef, BranchGeneration: 2,
					BaseHead:  f.cctx.BaseHead,
					Operation: "create", Path: "initial.txt",
					Fidelity: "full",
				}, nil)
			if err != nil {
				t.Fatal(err)
			}
			target, err := git.CommitTree(
				ctx, f.dir, git.EmptyTreeOID, "initial publication")
			if err != nil {
				t.Fatal(err)
			}
			publication := state.SelfPublication{
				ID: "sp_unborn", BranchRef: cctx.BranchRef,
				BranchGeneration: 2, TargetCommitOID: target,
				TargetTreeOID: git.EmptyTreeOID,
				Members: []state.SelfPublicationMember{{
					EventSeq: seq,
				}},
			}
			if created, err := state.PrepareSelfPublication(
				ctx, f.db, publication); err != nil || !created {
				t.Fatalf("PrepareSelfPublication=(%v,%v)", created, err)
			}
			if applied {
				if err := git.UpdateRef(
					ctx, f.dir, cctx.BranchRef, target, ""); err != nil {
					t.Fatal(err)
				}
			}

			summary, err := RecoverSelfPublications(
				ctx, f.dir, f.db, cctx, ReplayOpts{})
			if err != nil {
				t.Fatalf("RecoverSelfPublications: %v", err)
			}
			wantPhase := state.SelfPublicationAbandoned
			wantEvent := state.EventStatePending
			wantOID := ""
			if applied {
				wantPhase = state.SelfPublicationCompleted
				wantEvent = state.EventStatePublished
				wantOID = target
			}
			assertSelfPublicationRecoveryPhase(
				t, ctx, f.db, publication.ID, wantPhase)
			assertSelfPublicationRecoveryEvent(
				t, ctx, f.db, seq, wantEvent, wantOID)
			if applied && summary.FinalTargetOID != target {
				t.Fatalf("summary=%+v", summary)
			}
		})
	}
}

func TestRecoverSelfPublicationCrashAfterCASCompletesExactlyOnce(t *testing.T) {
	f := newCaptureFixture(t)
	ctx := context.Background()
	publication, seq, target := seedRecoverableSelfPublication(
		t, ctx, f, state.SelfPublicationPrepared)
	if err := git.UpdateRef(
		ctx, f.dir, f.cctx.BranchRef, target, f.cctx.BaseHead); err != nil {
		t.Fatalf("UpdateRef target: %v", err)
	}

	first, err := RecoverSelfPublications(
		ctx, f.dir, f.db, f.cctx, ReplayOpts{})
	if err != nil {
		t.Fatalf("first RecoverSelfPublications: %v", err)
	}
	if first.Completed != 1 || first.FinalTargetOID != target {
		t.Fatalf("first summary=%+v", first)
	}
	assertSelfPublicationRecoveryEvent(
		t, ctx, f.db, seq, state.EventStatePublished, target)
	assertSelfPublicationRecoveryPhase(
		t, ctx, f.db, publication.ID, state.SelfPublicationCompleted)

	second, err := RecoverSelfPublications(
		ctx, f.dir, f.db, f.cctx, ReplayOpts{})
	if err != nil {
		t.Fatalf("second RecoverSelfPublications: %v", err)
	}
	if second.Inspected != 0 || second.Completed != 0 ||
		second.Abandoned != 0 {
		t.Fatalf("second summary=%+v", second)
	}
	assertSelfPublicationRecoveryEvent(
		t, ctx, f.db, seq, state.EventStatePublished, target)
}

func TestRecoverSelfPublicationGitAppliedCompletesAndPreservesPendingSibling(
	t *testing.T,
) {
	f := newCaptureFixture(t)
	ctx := context.Background()
	publication, seq, target := seedRecoverableSelfPublication(
		t, ctx, f, state.SelfPublicationGitApplied)
	sibling := appendSelfPublicationRecoveryEvent(
		t, ctx, f, "sibling.txt")

	summary, err := RecoverSelfPublications(
		ctx, f.dir, f.db, f.cctx, ReplayOpts{})
	if err != nil {
		t.Fatalf("RecoverSelfPublications: %v", err)
	}
	if summary.Completed != 1 || summary.FinalTargetOID != target {
		t.Fatalf("summary=%+v", summary)
	}
	assertSelfPublicationRecoveryEvent(
		t, ctx, f.db, seq, state.EventStatePublished, target)
	assertSelfPublicationRecoveryEvent(
		t, ctx, f.db, sibling, state.EventStatePending, "")
	assertSelfPublicationRecoveryPhase(
		t, ctx, f.db, publication.ID, state.SelfPublicationCompleted)
}

func TestRecoverSelfPublicationPreservesCandidateCompletionIdentity(
	t *testing.T,
) {
	f := newCaptureFixture(t)
	ctx := context.Background()
	seq := appendSelfPublicationRecoveryEvent(t, ctx, f, "candidate.txt")
	sibling := appendSelfPublicationRecoveryEvent(t, ctx, f, "sibling.txt")
	const candidateID = "recovered-candidate"
	if err := state.SaveIntentCandidate(ctx, f.db, state.IntentCandidate{
		ID: candidateID, BranchRef: f.cctx.BranchRef,
		BranchGeneration: f.cctx.BranchGeneration,
		Status:           state.IntentCandidateReady, Readiness: state.IntentReadinessReady,
		Events: []state.IntentCandidateEvent{{
			EventSeq: seq, EventRole: "code",
		}},
	}); err != nil {
		t.Fatal(err)
	}
	tree, err := resolveTreeOID(ctx, f.dir, f.cctx.BaseHead)
	if err != nil {
		t.Fatal(err)
	}
	target, err := git.CommitTree(
		ctx, f.dir, tree, "candidate recovery", f.cctx.BaseHead)
	if err != nil {
		t.Fatal(err)
	}
	publication := state.SelfPublication{
		ID: "sp_candidate_recovery", BranchRef: f.cctx.BranchRef,
		BranchGeneration: f.cctx.BranchGeneration,
		SourceHead:       f.cctx.BaseHead, TargetCommitOID: target,
		TargetTreeOID: tree,
		Completion: state.SelfPublicationCompletion{
			PublishedTS:             10,
			CandidateStatus:         state.IntentCandidateSoftPublished,
			SoftPublicationDeadline: sql.NullFloat64{Float64: 20, Valid: true},
		},
		Members: []state.SelfPublicationMember{{
			EventSeq: seq,
			CandidateID: sql.NullString{
				String: candidateID, Valid: true,
			},
		}},
	}
	if created, err := state.PrepareSelfPublication(
		ctx, f.db, publication); err != nil || !created {
		t.Fatalf("PrepareSelfPublication=(%v,%v)", created, err)
	}
	if err := git.UpdateRef(
		ctx, f.dir, f.cctx.BranchRef, target, f.cctx.BaseHead); err != nil {
		t.Fatal(err)
	}
	if changed, err := state.MarkSelfPublicationGitApplied(
		ctx, f.db, publication, 11); err != nil || !changed {
		t.Fatalf("MarkSelfPublicationGitApplied=(%v,%v)", changed, err)
	}

	summary, err := RecoverSelfPublications(
		ctx, f.dir, f.db, f.cctx,
		ReplayOpts{IntentRepairEnabled: false})
	if err != nil {
		t.Fatalf("RecoverSelfPublications: %v", err)
	}
	if summary.Completed != 1 || summary.FinalTargetOID != target {
		t.Fatalf("summary=%+v", summary)
	}
	candidate, ok, err := state.IntentCandidateByID(
		ctx, f.db, candidateID)
	if err != nil || !ok ||
		candidate.Status != state.IntentCandidateSoftPublished ||
		!candidate.SoftPublicationDeadline.Valid ||
		candidate.SoftPublicationDeadline.Float64 != 20 {
		t.Fatalf("candidate=%+v ok=%v err=%v", candidate, ok, err)
	}
	assertSelfPublicationRecoveryEvent(
		t, ctx, f.db, seq, state.EventStatePublished, target)
	assertSelfPublicationRecoveryEvent(
		t, ctx, f.db, sibling, state.EventStatePending, "")
}

func TestSelfPublicationCrashRecoveryMatrix(t *testing.T) {
	tests := []struct {
		name              string
		checkpoint        SelfPublicationCheckpoint
		beforeRecovery    string
		wantRecoveryCount int
		wantEvent         string
		wantReplayError   bool
	}{
		{
			name: "before CAS", checkpoint: SelfPublicationBeforeCAS,
			beforeRecovery: state.SelfPublicationAbandoned,
			wantEvent:      state.EventStateBlockedConflict,
		},
		{
			name: "after CAS", checkpoint: SelfPublicationAfterCAS,
			beforeRecovery:    state.SelfPublicationPrepared,
			wantRecoveryCount: 1, wantEvent: state.EventStatePublished,
			wantReplayError: true,
		},
		{
			name:              "before completion",
			checkpoint:        SelfPublicationBeforeCompletion,
			beforeRecovery:    state.SelfPublicationGitApplied,
			wantRecoveryCount: 1, wantEvent: state.EventStatePublished,
			wantReplayError: true,
		},
		{
			name:            "after completion",
			checkpoint:      SelfPublicationAfterCompletion,
			beforeRecovery:  state.SelfPublicationCompleted,
			wantEvent:       state.EventStatePublished,
			wantReplayError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newCaptureFixture(t)
			ctx := context.Background()
			if _, err := BootstrapShadow(
				ctx, f.dir, f.db, f.cctx); err != nil {
				t.Fatal(err)
			}
			captureOnePendingFile(
				t, ctx, f, "crash.txt", tt.name+"\n")
			injected := errors.New("injected publication crash")
			_, replayErr := Replay(
				ctx, f.dir, f.db, f.cctx, ReplayOpts{
					GitDir: f.gitDir,
					SelfPublicationCheckpoint: func(
						event SelfPublicationCheckpointEvent,
					) error {
						if event.Checkpoint == tt.checkpoint {
							return injected
						}
						return nil
					},
				})
			if tt.wantReplayError != errors.Is(replayErr, injected) {
				t.Fatalf("Replay err=%v want injected=%v",
					replayErr, tt.wantReplayError)
			}
			journals := loadSelfPublicationRows(t, ctx, f.db)
			if len(journals) != 1 ||
				journals[0].phase != tt.beforeRecovery {
				t.Fatalf("journals=%+v want phase %s",
					journals, tt.beforeRecovery)
			}

			summary, recoverErr := RecoverSelfPublications(
				ctx, f.dir, f.db, f.cctx, ReplayOpts{})
			if recoverErr != nil {
				t.Fatalf("RecoverSelfPublications: %v", recoverErr)
			}
			if summary.Completed != tt.wantRecoveryCount {
				t.Fatalf("summary=%+v want completed=%d",
					summary, tt.wantRecoveryCount)
			}
			var eventState string
			if err := f.db.SQL().QueryRowContext(ctx, `
SELECT state FROM capture_events WHERE path='crash.txt'`,
			).Scan(&eventState); err != nil {
				t.Fatal(err)
			}
			if eventState != tt.wantEvent {
				t.Fatalf("event state=%s want %s",
					eventState, tt.wantEvent)
			}
			if tt.wantEvent == state.EventStatePublished {
				journals = loadSelfPublicationRows(t, ctx, f.db)
				if journals[0].phase != state.SelfPublicationCompleted {
					t.Fatalf("post-recovery journal=%+v", journals[0])
				}
			}
		})
	}
}

func TestRecoverSelfPublicationAmbiguityFailsClosed(t *testing.T) {
	tests := []struct {
		name             string
		wantPrimaryState string
		mutate           func(*testing.T, context.Context, *captureFixture,
			state.SelfPublication, string)
	}{
		{
			name:             "external branch movement",
			wantPrimaryState: state.EventStatePending,
			mutate: func(t *testing.T, ctx context.Context, f *captureFixture,
				_ state.SelfPublication, target string,
			) {
				tree, err := resolveTreeOID(ctx, f.dir, target)
				if err != nil {
					t.Fatal(err)
				}
				external, err := git.CommitTree(
					ctx, f.dir, tree, "external", target)
				if err != nil {
					t.Fatal(err)
				}
				if err := git.UpdateRef(
					ctx, f.dir, f.cctx.BranchRef, external, target); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name:             "target tag",
			wantPrimaryState: state.EventStatePending,
			mutate: func(t *testing.T, ctx context.Context, f *captureFixture,
				_ state.SelfPublication, target string,
			) {
				if err := git.UpdateRef(
					ctx, f.dir, "refs/tags/pinned", target, ""); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name:             "mismatched membership",
			wantPrimaryState: state.EventStateFailed,
			mutate: func(t *testing.T, ctx context.Context, f *captureFixture,
				publication state.SelfPublication, _ string,
			) {
				if _, err := f.db.SQL().ExecContext(ctx, `
UPDATE capture_events SET state='failed'
WHERE seq=?`, publication.Members[0].EventSeq); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name:             "missing target object",
			wantPrimaryState: state.EventStatePending,
			mutate: func(t *testing.T, _ context.Context, f *captureFixture,
				_ state.SelfPublication, target string,
			) {
				objectPath := filepath.Join(
					f.gitDir, "objects", target[:2], target[2:])
				if err := os.Remove(objectPath); err != nil {
					t.Fatalf("remove loose target object: %v", err)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newCaptureFixture(t)
			ctx := context.Background()
			publication, seq, target := seedRecoverableSelfPublication(
				t, ctx, f, state.SelfPublicationGitApplied)
			sibling := appendSelfPublicationRecoveryEvent(
				t, ctx, f, "sibling.txt")
			tt.mutate(t, ctx, f, publication, target)

			summary, err := RecoverSelfPublications(
				ctx, f.dir, f.db, f.cctx, ReplayOpts{})
			if !errors.Is(err, ErrSelfPublicationRecoveryAmbiguous) {
				t.Fatalf("summary=%+v err=%v, want ambiguous", summary, err)
			}
			assertSelfPublicationRecoveryPhase(
				t, ctx, f.db, publication.ID,
				state.SelfPublicationGitApplied)
			assertSelfPublicationRecoveryEvent(
				t, ctx, f.db, seq, tt.wantPrimaryState, "")
			assertSelfPublicationRecoveryEvent(
				t, ctx, f.db, sibling, state.EventStatePending, "")
		})
	}
}

func TestRecoverSelfPublicationMergeTargetFailsClosed(t *testing.T) {
	f := newCaptureFixture(t)
	ctx := context.Background()
	tree, err := resolveTreeOID(ctx, f.dir, f.cctx.BaseHead)
	if err != nil {
		t.Fatal(err)
	}
	side, err := git.CommitTree(ctx, f.dir, tree, "side root")
	if err != nil {
		t.Fatal(err)
	}
	merge, err := git.CommitTree(
		ctx, f.dir, tree, "merge target", f.cctx.BaseHead, side)
	if err != nil {
		t.Fatal(err)
	}
	seq := appendSelfPublicationRecoveryEvent(t, ctx, f, "merge.txt")
	publication := prepareSelfPublicationRecoveryJournal(
		t, ctx, f, seq, merge, tree)
	if _, err := state.MarkSelfPublicationGitApplied(
		ctx, f.db, publication, selfPublicationNow()); err != nil {
		t.Fatal(err)
	}
	if err := git.UpdateRef(
		ctx, f.dir, f.cctx.BranchRef, merge, f.cctx.BaseHead); err != nil {
		t.Fatal(err)
	}

	_, err = RecoverSelfPublications(
		ctx, f.dir, f.db, f.cctx, ReplayOpts{})
	if !errors.Is(err, ErrSelfPublicationRecoveryAmbiguous) {
		t.Fatalf("err=%v want ambiguous", err)
	}
	assertSelfPublicationRecoveryEvent(
		t, ctx, f.db, seq, state.EventStatePending, "")
}

func TestRecoverSelfPublicationCancellationIsBounded(t *testing.T) {
	f := newCaptureFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := RecoverSelfPublications(
		ctx, f.dir, f.db, f.cctx, ReplayOpts{})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err=%v want context.Canceled", err)
	}
}

func TestRecoverSelfPublicationUnknownCompletionFailsBeforeMutation(
	t *testing.T,
) {
	f := newCaptureFixture(t)
	ctx := context.Background()
	publication, seq, _ := seedRecoverableSelfPublication(
		t, ctx, f, state.SelfPublicationPrepared)
	publication.Completion.CandidateStatus =
		state.SelfPublicationCompletionUnknown

	_, err := recoverSelfPublication(ctx, f.dir, f.db, publication)
	if !errors.Is(err, ErrSelfPublicationRecoveryAmbiguous) ||
		!strings.Contains(err.Error(), "completion semantics are unknown") {
		t.Fatalf("err=%v want unknown-completion ambiguity", err)
	}
	assertSelfPublicationRecoveryPhase(
		t, ctx, f.db, publication.ID, state.SelfPublicationPrepared)
	assertSelfPublicationRecoveryEvent(
		t, ctx, f.db, seq, state.EventStatePending, "")
}

func TestRecoverSelfPublicationAttentionPersistsUntilConvergence(
	t *testing.T,
) {
	f := newCaptureFixture(t)
	ctx := context.Background()
	publication, _, target := seedRecoverableSelfPublication(
		t, ctx, f, state.SelfPublicationGitApplied)
	if err := git.UpdateRef(
		ctx, f.dir, "refs/tags/recovery-attention", target, ""); err != nil {
		t.Fatal(err)
	}

	if _, err := RecoverSelfPublications(
		ctx, f.dir, f.db, f.cctx, ReplayOpts{}); !errors.Is(
		err, ErrSelfPublicationRecoveryAmbiguous) {
		t.Fatalf("first recovery err=%v want ambiguity", err)
	}
	attention, ok, err := state.MetaGet(
		ctx, f.db, state.SelfPublicationNeedsAttentionMetaKey)
	if err != nil || !ok ||
		!strings.Contains(attention, "Automatic recovery is blocked") ||
		strings.Contains(attention, target) {
		t.Fatalf("attention=(%q,%v,%v)", attention, ok, err)
	}
	blockerID, ok, err := state.MetaGet(
		ctx, f.db, state.SelfPublicationAttentionBlockerMetaKey)
	if err != nil || !ok || blockerID != publication.ID {
		t.Fatalf("attention blocker=(%q,%v,%v)", blockerID, ok, err)
	}
	otherSeq, err := state.AppendCaptureEvent(ctx, f.db, state.CaptureEvent{
		BranchRef: "refs/heads/ordinary", BranchGeneration: 2,
		BaseHead: "ordinary-source", Operation: "update",
		Path: "ordinary.txt", Fidelity: "full",
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	ordinary := state.SelfPublication{
		ID: "ordinary-other-pair", BranchRef: "refs/heads/ordinary",
		BranchGeneration: 2, SourceHead: "ordinary-source",
		TargetCommitOID: "ordinary-target", TargetTreeOID: "ordinary-tree",
		Members: []state.SelfPublicationMember{{EventSeq: otherSeq}},
	}
	if created, err := state.PrepareSelfPublication(
		ctx, f.db, ordinary); err != nil || !created {
		t.Fatalf("prepare ordinary publication=(%v,%v)", created, err)
	}

	if _, err := git.Run(ctx, git.RunOpts{Dir: f.dir},
		"update-ref", "-d", "refs/tags/recovery-attention", target); err != nil {
		t.Fatal(err)
	}
	summary, err := RecoverSelfPublications(
		ctx, f.dir, f.db, f.cctx, ReplayOpts{})
	if err != nil || summary.Completed != 1 {
		t.Fatalf("converged recovery summary=%+v err=%v", summary, err)
	}
	if _, ok, err := state.MetaGet(
		ctx, f.db, state.SelfPublicationNeedsAttentionMetaKey); err != nil ||
		ok {
		t.Fatalf("attention after convergence ok=%v err=%v", ok, err)
	}
	if _, ok, err := state.MetaGet(
		ctx, f.db,
		state.SelfPublicationAttentionBlockerMetaKey); err != nil || ok {
		t.Fatalf("attention owner after convergence ok=%v err=%v", ok, err)
	}
	assertSelfPublicationRecoveryPhase(
		t, ctx, f.db, publication.ID, state.SelfPublicationCompleted)
	assertSelfPublicationRecoveryPhase(
		t, ctx, f.db, ordinary.ID, state.SelfPublicationPrepared)
}

func TestRecoverSelfPublicationAttentionOnOtherPairBlocksMutation(t *testing.T) {
	f := newCaptureFixture(t)
	ctx := context.Background()
	seq, err := state.AppendCaptureEvent(ctx, f.db, state.CaptureEvent{
		BranchRef: "refs/heads/other", BranchGeneration: 2,
		BaseHead: "other-source", Operation: "update",
		Path: "other.txt", Fidelity: "full",
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	other := state.SelfPublication{
		ID: "other-pair", BranchRef: "refs/heads/other",
		BranchGeneration: 2, SourceHead: "other-source",
		TargetCommitOID: "other-target", TargetTreeOID: "other-tree",
		Members: []state.SelfPublicationMember{{EventSeq: seq}},
	}
	if created, err := state.PrepareSelfPublication(
		ctx, f.db, other); err != nil || !created {
		t.Fatalf("PrepareSelfPublication=(%v,%v)", created, err)
	}
	const attention = "existing recovery blocker"
	if err := state.SetSelfPublicationRecoveryAttention(
		ctx, f.db, other.ID, attention); err != nil {
		t.Fatal(err)
	}

	summary, err := RecoverSelfPublications(
		ctx, f.dir, f.db, f.cctx, ReplayOpts{})
	if !errors.Is(err, ErrSelfPublicationRecoveryAmbiguous) ||
		summary.Inspected != 0 {
		t.Fatalf("current-pair summary=%+v err=%v want ambiguity", summary, err)
	}
	got, ok, err := state.MetaGet(
		ctx, f.db, state.SelfPublicationNeedsAttentionMetaKey)
	if err != nil || !ok || got != attention {
		t.Fatalf("attention=(%q,%v,%v)", got, ok, err)
	}
}

func TestRecoverSelfPublicationsBeforePlanningBlocksOtherPairJournal(t *testing.T) {
	f := newCaptureFixture(t)
	ctx := context.Background()
	seq, err := state.AppendCaptureEvent(ctx, f.db, state.CaptureEvent{
		BranchRef: "refs/heads/other", BranchGeneration: 2,
		BaseHead: "other-source", Operation: "update",
		Path: "other-unsettled.txt", Fidelity: "full",
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	other := state.SelfPublication{
		ID: "other-pair-before-planning", BranchRef: "refs/heads/other",
		BranchGeneration: 2, SourceHead: "other-source",
		TargetCommitOID: "other-target", TargetTreeOID: "other-tree",
		Members: []state.SelfPublicationMember{{EventSeq: seq}},
	}
	if created, err := state.PrepareSelfPublication(ctx, f.db, other); err != nil || !created {
		t.Fatalf("PrepareSelfPublication=(%t,%v)", created, err)
	}
	if err := RecoverSelfPublicationsBeforePlanning(ctx, f.dir, f.db); !errors.Is(err, ErrSelfPublicationRecoveryAmbiguous) {
		t.Fatalf("err=%v want other-pair ambiguity", err)
	}
	assertSelfPublicationRecoveryPhase(t, ctx, f.db, other.ID, state.SelfPublicationPrepared)
}

func TestRecoveryPreservesPendingSiblingsOnPreparedAmbiguity(t *testing.T) {
	f := newCaptureFixture(t)
	ctx := context.Background()
	publication, seq, target := seedRecoverableSelfPublication(
		t, ctx, f, state.SelfPublicationPrepared)
	sibling := appendSelfPublicationRecoveryEvent(
		t, ctx, f, "sibling.txt")
	tree, err := resolveTreeOID(ctx, f.dir, target)
	if err != nil {
		t.Fatal(err)
	}
	external, err := git.CommitTree(
		ctx, f.dir, tree, "external root")
	if err != nil {
		t.Fatal(err)
	}
	if err := git.UpdateRef(
		ctx, f.dir, f.cctx.BranchRef, external, f.cctx.BaseHead); err != nil {
		t.Fatal(err)
	}

	_, err = RecoverSelfPublications(
		ctx, f.dir, f.db, f.cctx, ReplayOpts{})
	if !errors.Is(err, ErrSelfPublicationRecoveryAmbiguous) {
		t.Fatalf("err=%v want ambiguous", err)
	}
	assertSelfPublicationRecoveryPhase(
		t, ctx, f.db, publication.ID, state.SelfPublicationPrepared)
	assertSelfPublicationRecoveryEvent(
		t, ctx, f.db, seq, state.EventStatePending, "")
	assertSelfPublicationRecoveryEvent(
		t, ctx, f.db, sibling, state.EventStatePending, "")
}

func TestRun_RecoversSelfPublicationAtStartupBeforeCaptureReplay(t *testing.T) {
	f := newDaemonFixture(t)
	registerLiveClient(t, f.db)
	ctx := context.Background()
	publication, seq, target := seedDaemonRestartSelfPublication(
		t, ctx, f, state.SelfPublicationPrepared, false)

	runCtx, cancel := context.WithCancel(ctx)
	var wg sync.WaitGroup
	var runErr error
	wg.Add(1)
	go func() {
		defer wg.Done()
		runErr = Run(runCtx, Options{
			RepoPath: f.dir, GitDir: f.gitDir, DB: f.db,
			Scheduler: fastScheduler(), BootGrace: 30 * time.Second,
			MessageFn: DeterministicMessage,
			WakeCh:    make(chan struct{}, 1), ShutdownCh: make(chan struct{}, 1),
			SkipSignals: true,
		})
	}()
	t.Cleanup(func() {
		cancel()
		wg.Wait()
	})

	waitForDaemonMode(t, f.db, "running", 3*time.Second)
	waitFor(t, 3*time.Second, "startup publication completes", func() bool {
		got, ok, err := state.SelfPublicationByID(
			ctx, f.db, publication.ID)
		return err == nil && ok &&
			got.Phase == state.SelfPublicationCompleted
	})
	assertSelfPublicationRecoveryEvent(
		t, ctx, f.db, seq, state.EventStatePublished, target)
	waitForMetaValue(t, f.db, MetaKeyBranchHead, target, 3*time.Second)
	waitForMetaValue(t, f.db, MetaKeyBranchToken,
		branchTokenRev(target, "refs/heads/main"), 3*time.Second)
	if generation, err := LoadBranchGeneration(ctx, f.db); err != nil ||
		generation != 1 {
		t.Fatalf("generation=%d err=%v want stable 1", generation, err)
	}
	cancel()
	wg.Wait()
	if runErr != nil {
		t.Fatalf("Run: %v", runErr)
	}
}

func TestRun_AmbiguousSelfPublicationBlocksMutationButServicesFlush(
	t *testing.T,
) {
	f := newDaemonFixture(t)
	registerLiveClient(t, f.db)
	ctx := context.Background()
	publication, seq, _ := seedDaemonRestartSelfPublication(
		t, ctx, f, state.SelfPublicationGitApplied, true)
	repairID := "repair_blocked_by_publication"
	if err := state.SaveIntentRepair(ctx, f.db, state.IntentRepair{
		ID: repairID, BranchRef: "refs/heads/main", BranchGeneration: 1,
		ExpectedHead: publication.TargetCommitOID,
		PlanDigest:   "sha256:" + strings.Repeat("0", 64),
		Commits: []state.IntentRepairCommit{{
			RepairID: repairID, OldOID: publication.SourceHead,
		}},
	}); err != nil {
		t.Fatalf("seed intent repair: %v", err)
	}
	if _, err := state.EnqueueFlushRequest(
		ctx, f.db, "wake", false,
		sql.NullString{String: "ambiguous recovery", Valid: true}); err != nil {
		t.Fatal(err)
	}

	runCtx, cancel := context.WithCancel(ctx)
	var wg sync.WaitGroup
	var runErr error
	wg.Add(1)
	go func() {
		defer wg.Done()
		runErr = Run(runCtx, Options{
			RepoPath: f.dir, GitDir: f.gitDir, DB: f.db,
			Scheduler: fastScheduler(), BootGrace: 30 * time.Second,
			MessageFn: DeterministicMessage,
			WakeCh:    make(chan struct{}, 1), ShutdownCh: make(chan struct{}, 1),
			SkipSignals: true,
		})
	}()
	t.Cleanup(func() {
		cancel()
		wg.Wait()
	})

	waitForDaemonMode(t, f.db, "running", 3*time.Second)
	// These 10s waits are load-tolerant harness bounds inside the 60s wake
	// contract. Strict sub-3s liveness is asserted separately by
	// TestRun_HeartbeatAndWakeAfterSelfPublication and integration coverage.
	waitFor(t, 10*time.Second, "flush serviced while recovery blocked", func() bool {
		return countFlushByStatus(t, f.db, "completed") == 1
	})
	waitFor(t, 10*time.Second, "durable recovery attention", func() bool {
		value, ok, err := state.MetaGet(
			ctx, f.db, state.SelfPublicationNeedsAttentionMetaKey)
		return err == nil && ok &&
			strings.Contains(value, "Automatic recovery is blocked")
	})
	assertSelfPublicationRecoveryPhase(
		t, ctx, f.db, publication.ID, state.SelfPublicationGitApplied)
	assertSelfPublicationRecoveryEvent(
		t, ctx, f.db, seq, state.EventStatePending, "")
	repair, ok, err := state.IntentRepairByID(ctx, f.db, repairID)
	if err != nil || !ok || repair.Status != state.IntentRepairPrepared {
		t.Fatalf("intent repair mutated around ambiguous publication: %+v ok=%v err=%v",
			repair, ok, err)
	}
	var snapshots int
	if err := f.db.SQL().QueryRowContext(ctx,
		`SELECT COUNT(*) FROM recovery_snapshots`).Scan(&snapshots); err != nil {
		t.Fatal(err)
	}
	if snapshots != 0 {
		t.Fatalf("recovery snapshots=%d want 0", snapshots)
	}
	cancel()
	wg.Wait()
	if runErr != nil {
		t.Fatalf("Run: %v", runErr)
	}
}

func TestRun_UnknownSelfPublicationOnOtherPairBlocksMutation(
	t *testing.T,
) {
	f := newDaemonFixture(t)
	registerLiveClient(t, f.db)
	ctx := context.Background()
	current, seq, _ := seedDaemonRestartSelfPublication(
		t, ctx, f, state.SelfPublicationPrepared, false)
	unknown, _ := seedUnknownRecoveryPublication(
		t, ctx, f.db, "refs/heads/migrated-other", 7)

	runCtx, cancel := context.WithCancel(ctx)
	var wg sync.WaitGroup
	var runErr error
	wg.Add(1)
	go func() {
		defer wg.Done()
		runErr = Run(runCtx, Options{
			RepoPath: f.dir, GitDir: f.gitDir, DB: f.db,
			Scheduler: fastScheduler(), BootGrace: 30 * time.Second,
			MessageFn:   DeterministicMessage,
			WakeCh:      make(chan struct{}, 1),
			ShutdownCh:  make(chan struct{}, 1),
			SkipSignals: true,
		})
	}()
	t.Cleanup(func() {
		cancel()
		wg.Wait()
	})

	waitForDaemonMode(t, f.db, "running", 3*time.Second)
	waitFor(t, 10*time.Second, "global unknown recovery blocker", func() bool {
		id, ok, err := state.MetaGet(
			ctx, f.db, state.SelfPublicationAttentionBlockerMetaKey)
		return err == nil && ok && id == unknown.ID
	})
	assertSelfPublicationRecoveryPhase(
		t, ctx, f.db, current.ID, state.SelfPublicationPrepared)
	assertSelfPublicationRecoveryEvent(
		t, ctx, f.db, seq, state.EventStatePending, "")
	cancel()
	wg.Wait()
	if runErr != nil {
		t.Fatalf("Run: %v", runErr)
	}
}

func seedDaemonRestartSelfPublication(
	t *testing.T,
	ctx context.Context,
	f *daemonFixture,
	phase string,
	tagTarget bool,
) (state.SelfPublication, int64, string) {
	t.Helper()
	source, err := git.RevParse(ctx, f.dir, "HEAD")
	if err != nil {
		t.Fatal(err)
	}
	if err := SaveBranchPublicationToken(
		ctx, f.db, 1, source,
		branchTokenRev(source, "refs/heads/main")); err != nil {
		t.Fatal(err)
	}
	seq, err := state.AppendCaptureEvent(ctx, f.db, state.CaptureEvent{
		BranchRef: "refs/heads/main", BranchGeneration: 1,
		BaseHead: source, Operation: "update", Path: "restart.txt",
		Fidelity: "full",
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	tree, err := resolveTreeOID(ctx, f.dir, source)
	if err != nil {
		t.Fatal(err)
	}
	target, err := git.CommitTree(
		ctx, f.dir, tree, "restart publication", source)
	if err != nil {
		t.Fatal(err)
	}
	publication := state.SelfPublication{
		ID: "sp_daemon_restart", BranchRef: "refs/heads/main",
		BranchGeneration: 1, SourceHead: source,
		TargetCommitOID: target, TargetTreeOID: tree,
		Completion: state.SelfPublicationCompletion{
			PublishedTS: selfPublicationNow(),
		},
		Members: []state.SelfPublicationMember{{EventSeq: seq}},
	}
	if created, err := state.PrepareSelfPublication(
		ctx, f.db, publication); err != nil || !created {
		t.Fatalf("PrepareSelfPublication=(%v,%v)", created, err)
	}
	if err := git.UpdateRef(
		ctx, f.dir, publication.BranchRef, target, source); err != nil {
		t.Fatal(err)
	}
	if phase == state.SelfPublicationGitApplied {
		if changed, err := state.MarkSelfPublicationGitApplied(
			ctx, f.db, publication,
			selfPublicationNow()); err != nil || !changed {
			t.Fatalf("MarkSelfPublicationGitApplied=(%v,%v)", changed, err)
		}
	}
	if tagTarget {
		if err := git.UpdateRef(
			ctx, f.dir, "refs/tags/ambiguous", target, ""); err != nil {
			t.Fatal(err)
		}
	}
	return publication, seq, target
}

func seedUnknownRecoveryPublication(
	t *testing.T,
	ctx context.Context,
	db *state.DB,
	branchRef string,
	generation int64,
) (state.SelfPublication, int64) {
	t.Helper()
	seq, err := state.AppendCaptureEvent(ctx, db, state.CaptureEvent{
		BranchRef: branchRef, BranchGeneration: generation,
		BaseHead: "migrated-source", Operation: "update",
		Path: "migrated.txt", Fidelity: "full",
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	members := []state.SelfPublicationMember{{EventSeq: seq}}
	digest, err := state.SelfPublicationMembershipDigest(members)
	if err != nil {
		t.Fatal(err)
	}
	publication := state.SelfPublication{
		ID:        "unknown-" + strings.ReplaceAll(branchRef, "/", "-"),
		BranchRef: branchRef, BranchGeneration: generation,
		SourceHead:       "migrated-source",
		TargetCommitOID:  "migrated-target",
		TargetTreeOID:    "migrated-tree",
		MembershipDigest: digest, MemberCount: 1,
		Phase: state.SelfPublicationPrepared,
		Completion: state.SelfPublicationCompletion{
			CandidateStatus: state.SelfPublicationCompletionUnknown,
		},
		Members: members,
	}
	ts := selfPublicationNow()
	if _, err := db.SQL().ExecContext(ctx, `
INSERT INTO self_publications(
    id, branch_ref, branch_generation, source_head, target_commit_oid,
    target_tree_oid, membership_digest, member_count, phase, created_ts,
    updated_ts, error, completion_published_ts,
    completion_candidate_status, completion_soft_deadline
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'prepared', ?, ?, '', 0, ?, NULL)`,
		publication.ID, publication.BranchRef,
		publication.BranchGeneration, publication.SourceHead,
		publication.TargetCommitOID, publication.TargetTreeOID,
		publication.MembershipDigest, publication.MemberCount, ts, ts,
		state.SelfPublicationCompletionUnknown); err != nil {
		t.Fatal(err)
	}
	if _, err := db.SQL().ExecContext(ctx, `
INSERT INTO self_publication_members(
    publication_id, ord, event_seq, candidate_id
) VALUES (?, 0, ?, NULL)`, publication.ID, seq); err != nil {
		t.Fatal(err)
	}
	return publication, seq
}

func seedRecoverableSelfPublication(
	t *testing.T,
	ctx context.Context,
	f *captureFixture,
	phase string,
) (state.SelfPublication, int64, string) {
	t.Helper()
	seq := appendSelfPublicationRecoveryEvent(
		t, ctx, f, strings.ReplaceAll(t.Name(), "/", "-")+".txt")
	tree, err := resolveTreeOID(ctx, f.dir, f.cctx.BaseHead)
	if err != nil {
		t.Fatalf("resolve source tree: %v", err)
	}
	target, err := git.CommitTree(
		ctx, f.dir, tree, "recovered publication", f.cctx.BaseHead)
	if err != nil {
		t.Fatalf("CommitTree: %v", err)
	}
	publication := prepareSelfPublicationRecoveryJournal(
		t, ctx, f, seq, target, tree)
	if phase == state.SelfPublicationGitApplied {
		if err := git.UpdateRef(
			ctx, f.dir, f.cctx.BranchRef,
			target, f.cctx.BaseHead); err != nil {
			t.Fatalf("UpdateRef target: %v", err)
		}
		if changed, err := state.MarkSelfPublicationGitApplied(
			ctx, f.db, publication,
			selfPublicationNow()); err != nil || !changed {
			t.Fatalf("MarkSelfPublicationGitApplied=(%v,%v)", changed, err)
		}
	}
	return publication, seq, target
}

func prepareSelfPublicationRecoveryJournal(
	t *testing.T,
	ctx context.Context,
	f *captureFixture,
	seq int64,
	target, tree string,
) state.SelfPublication {
	t.Helper()
	publication := state.SelfPublication{
		ID:        fmt.Sprintf("sp_recovery_%d", seq),
		BranchRef: f.cctx.BranchRef, BranchGeneration: f.cctx.BranchGeneration,
		SourceHead: f.cctx.BaseHead, TargetCommitOID: target,
		TargetTreeOID: tree,
		Members:       []state.SelfPublicationMember{{EventSeq: seq}},
	}
	if created, err := state.PrepareSelfPublication(
		ctx, f.db, publication); err != nil || !created {
		t.Fatalf("PrepareSelfPublication=(%v,%v)", created, err)
	}
	return publication
}

func appendSelfPublicationRecoveryEvent(
	t *testing.T,
	ctx context.Context,
	f *captureFixture,
	path string,
) int64 {
	t.Helper()
	seq, err := state.AppendCaptureEvent(ctx, f.db, state.CaptureEvent{
		BranchRef: f.cctx.BranchRef, BranchGeneration: f.cctx.BranchGeneration,
		BaseHead: f.cctx.BaseHead, Operation: "update", Path: path,
		Fidelity: "full",
	}, nil)
	if err != nil {
		t.Fatalf("AppendCaptureEvent: %v", err)
	}
	return seq
}

func assertSelfPublicationRecoveryPhase(
	t *testing.T,
	ctx context.Context,
	db *state.DB,
	id, want string,
) {
	t.Helper()
	publication, ok, err := state.SelfPublicationByID(ctx, db, id)
	if err != nil || !ok || publication.Phase != want {
		t.Fatalf("publication %s=(phase=%q ok=%v err=%v), want %q",
			id, publication.Phase, ok, err, want)
	}
}

func assertSelfPublicationRecoveryEvent(
	t *testing.T,
	ctx context.Context,
	db *state.DB,
	seq int64,
	wantState, wantOID string,
) {
	t.Helper()
	var eventState string
	var commitOID sql.NullString
	if err := db.SQL().QueryRowContext(ctx, `
SELECT state, commit_oid FROM capture_events WHERE seq=?`, seq).Scan(
		&eventState, &commitOID); err != nil {
		t.Fatalf("event %d: %v", seq, err)
	}
	if eventState != wantState ||
		(wantOID == "" && commitOID.Valid) ||
		(wantOID != "" &&
			(!commitOID.Valid || commitOID.String != wantOID)) {
		t.Fatalf("event %d=(state=%s oid=%+v), want (%s,%s)",
			seq, eventState, commitOID, wantState, wantOID)
	}
}
