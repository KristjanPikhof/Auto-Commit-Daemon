package daemon

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/testutil"
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
		ctx, f.dir, f.db, f.cctx, SelfPublicationRecoveryOptions{})
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
		ctx, f.dir, f.db, f.cctx, SelfPublicationRecoveryOptions{Limit: 1})
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
		ctx, f.dir, f.db, f.cctx, SelfPublicationRecoveryOptions{Limit: 1})
	if err != nil {
		t.Fatal(err)
	}
	if second.Inspected != 1 || second.Abandoned != 1 || second.HasMore {
		t.Fatalf("second summary=%+v", second)
	}
	assertSelfPublicationRecoveryPhase(
		t, ctx, f.db, publications[1].ID, state.SelfPublicationAbandoned)
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
			seq, err := testutil.SeedLegacyCapture(
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
			if created, err := testutil.SeedLegacySelfPublication(
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
				ctx, f.dir, f.db, cctx, SelfPublicationRecoveryOptions{})
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
		ctx, f.dir, f.db, f.cctx, SelfPublicationRecoveryOptions{})
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
		ctx, f.dir, f.db, f.cctx, SelfPublicationRecoveryOptions{})
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
		ctx, f.dir, f.db, f.cctx, SelfPublicationRecoveryOptions{})
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
	if err := testutil.SeedLegacyCandidate(ctx, f.db, state.IntentCandidate{
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
	if created, err := testutil.SeedLegacySelfPublication(
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
		SelfPublicationRecoveryOptions{})
	if err != nil {
		t.Fatalf("RecoverSelfPublications: %v", err)
	}
	if summary.Completed != 1 || summary.FinalTargetOID != target {
		t.Fatalf("summary=%+v", summary)
	}
	var candidateStatus string
	var softDeadline sql.NullFloat64
	err = f.db.ReadSQL().QueryRowContext(ctx,
		`SELECT status,soft_publication_deadline FROM intent_candidates WHERE id=?`, candidateID).Scan(&candidateStatus, &softDeadline)
	if err != nil || candidateStatus != state.IntentCandidateSoftPublished ||
		!softDeadline.Valid || softDeadline.Float64 != 20 {
		t.Fatalf("candidate status=%q deadline=%+v err=%v", candidateStatus, softDeadline, err)
	}
	assertSelfPublicationRecoveryEvent(
		t, ctx, f.db, seq, state.EventStatePublished, target)
	assertSelfPublicationRecoveryEvent(
		t, ctx, f.db, sibling, state.EventStatePending, "")
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
				ctx, f.dir, f.db, f.cctx, SelfPublicationRecoveryOptions{})
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
		ctx, f.dir, f.db, f.cctx, SelfPublicationRecoveryOptions{})
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
		ctx, f.dir, f.db, f.cctx, SelfPublicationRecoveryOptions{})
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
		ctx, f.dir, f.db, f.cctx, SelfPublicationRecoveryOptions{}); !errors.Is(
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
	otherSeq, err := testutil.SeedLegacyCapture(ctx, f.db, state.CaptureEvent{
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
	if created, err := testutil.SeedLegacySelfPublication(
		ctx, f.db, ordinary); err != nil || !created {
		t.Fatalf("prepare ordinary publication=(%v,%v)", created, err)
	}

	if _, err := git.Run(ctx, git.RunOpts{Dir: f.dir},
		"update-ref", "-d", "refs/tags/recovery-attention", target); err != nil {
		t.Fatal(err)
	}
	summary, err := RecoverSelfPublications(
		ctx, f.dir, f.db, f.cctx, SelfPublicationRecoveryOptions{})
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
	seq, err := testutil.SeedLegacyCapture(ctx, f.db, state.CaptureEvent{
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
	if created, err := testutil.SeedLegacySelfPublication(
		ctx, f.db, other); err != nil || !created {
		t.Fatalf("PrepareSelfPublication=(%v,%v)", created, err)
	}
	const attention = "existing recovery blocker"
	if err := state.SetSelfPublicationRecoveryAttention(
		ctx, f.db, other.ID, attention); err != nil {
		t.Fatal(err)
	}

	summary, err := RecoverSelfPublications(
		ctx, f.dir, f.db, f.cctx, SelfPublicationRecoveryOptions{})
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
	seq, err := testutil.SeedLegacyCapture(ctx, f.db, state.CaptureEvent{
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
	if created, err := testutil.SeedLegacySelfPublication(ctx, f.db, other); err != nil || !created {
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
		ctx, f.dir, f.db, f.cctx, SelfPublicationRecoveryOptions{})
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

func seedUnknownRecoveryPublication(
	t *testing.T,
	ctx context.Context,
	db *state.DB,
	branchRef string,
	generation int64,
) (state.SelfPublication, int64) {
	t.Helper()
	seq, err := testutil.SeedLegacyCapture(ctx, db, state.CaptureEvent{
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
	if created, err := testutil.SeedLegacySelfPublication(
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
	seq, err := testutil.SeedLegacyCapture(ctx, f.db, state.CaptureEvent{
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
