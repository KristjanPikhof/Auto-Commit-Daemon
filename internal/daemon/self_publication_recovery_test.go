package daemon

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

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
