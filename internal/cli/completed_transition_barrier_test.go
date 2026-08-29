package cli

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	checkpointpkg "github.com/KristjanPikhof/Auto-Commit-Daemon/internal/checkpoint"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/daemon"
	gitpkg "github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

func TestIntentStatusSurfacesCompletedTransitionProofAttention(t *testing.T) {
	ctx := context.Background()
	_, _, db := makeRepoStateDB(t)
	message := "Durable ACD transition proof needs attention: ambiguous repair"
	if err := state.MetaSet(ctx, db,
		daemon.MetaKeyBranchTransitionNeedsAttention, message); err != nil {
		t.Fatal(err)
	}
	report, err := loadIntentV2Report(ctx, db.SQL())
	if err != nil {
		t.Fatal(err)
	}
	if report.ReplayState != "needs_attention" ||
		!strings.Contains(report.NeedsAttention, "ambiguous repair") {
		t.Fatalf("Intent report=%+v", report)
	}
	replay, err := loadReplayObservabilityReport(ctx, db.SQL())
	if err != nil {
		t.Fatal(err)
	}
	if replay.State != "needs_attention" ||
		!strings.Contains(replay.LastError, "ambiguous repair") {
		t.Fatalf("Replay report=%+v", replay)
	}
	status := statusReport{
		Daemon:                        "running",
		Replay:                        replay,
		IntentV2:                      report,
		CheckpointProtectionAvailable: true,
		PublicationDrain: publicationDrainReport{
			Phase: state.PublicationDrainSemantic,
		},
	}
	if got := statusOperationalStateWithDaemonAlive(status, true); got != "needs_attention" {
		t.Fatalf("Operational state=%q want needs_attention", got)
	}
	result := controlResult{OK: true}
	applyControlStatusWithDaemonAlive(&result, status, true)
	if result.OK || result.Health != controlHealthNeedsAttention ||
		!result.RecoveryRequired || result.NextAction == "No action needed." {
		t.Fatalf("Control result=%+v", result)
	}
}

const completedTransitionBarrierDigest = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

func TestFreezePublicationDrainTargetAcceptsCompletedIntentRepairWithStaleCheckpointHead(
	t *testing.T,
) {
	ctx := context.Background()
	repo := materializeTestRepo(t, true)
	db, err := state.Open(ctx, filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })

	const (
		branchRef        = "refs/heads/main"
		branchGeneration = int64(7)
		repairID         = "completed-checkpoint-head-repair"
		checkpointID     = "cp-1-0123456789abcdef"
	)

	baseHead, err := gitpkg.RevParse(ctx, repo, "HEAD")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := gitpkg.Run(ctx, gitpkg.RunOpts{Dir: repo},
		"commit", "--allow-empty", "-q", "-m", "old publication head"); err != nil {
		t.Fatal(err)
	}
	oldHead, err := gitpkg.RevParse(ctx, repo, "HEAD")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := gitpkg.Run(ctx, gitpkg.RunOpts{Dir: repo},
		"reset", "--hard", "-q", baseHead); err != nil {
		t.Fatal(err)
	}
	if _, err := gitpkg.Run(ctx, gitpkg.RunOpts{Dir: repo},
		"commit", "--allow-empty", "-q", "-m", "repaired publication head"); err != nil {
		t.Fatal(err)
	}
	newHead, err := gitpkg.RevParse(ctx, repo, "HEAD")
	if err != nil {
		t.Fatal(err)
	}
	if ancestor, err := gitpkg.IsAncestor(ctx, repo, oldHead, newHead); err != nil {
		t.Fatal(err)
	} else if ancestor {
		t.Fatalf("repair fixture is a fast-forward: %s -> %s", oldHead, newHead)
	}

	repair := state.IntentRepair{
		ID: repairID, BranchRef: branchRef,
		BranchGeneration: branchGeneration,
		Status:           state.IntentRepairPrepared,
		ExpectedHead:     oldHead,
		PlanDigest:       completedTransitionBarrierDigest,
		OldHead:          sql.NullString{String: oldHead, Valid: true},
		CreatedTS:        0.1,
		UpdatedTS:        0.1,
		Commits: []state.IntentRepairCommit{{
			CandidateID: sql.NullString{
				String: "candidate-completed-checkpoint-head-repair",
				Valid:  true,
			},
			OldOID: oldHead,
		}},
	}
	if _, err := db.SQL().ExecContext(ctx, `
INSERT INTO intent_repairs(
    id,branch_ref,branch_generation,status,expected_head,plan_digest,
    old_head,created_ts,updated_ts,error
) VALUES (?,?,?,?,?,?,?,?,?,'');
INSERT INTO intent_repair_commits(
    repair_id,ord,candidate_id,old_oid
) VALUES (?,0,?,?);
INSERT INTO intent_repair_member_seals(
    repair_id,membership_mode,member_count
) VALUES (?,'legacy',0)`,
		repair.ID, repair.BranchRef, repair.BranchGeneration, repair.Status,
		repair.ExpectedHead, repair.PlanDigest, repair.OldHead,
		repair.CreatedTS, repair.UpdatedTS,
		repair.ID, repair.Commits[0].CandidateID, repair.Commits[0].OldOID,
		repair.ID); err != nil {
		t.Fatal(err)
	}
	mapping := []state.IntentRepairCommit{{
		CandidateID: repair.Commits[0].CandidateID,
		OldOID:      oldHead,
		NewOID:      sql.NullString{String: newHead, Valid: true},
	}}
	applied, err := state.TransitionIntentRepair(ctx, db, repairID,
		state.IntentRepairTransition{
			ExpectedStatus: state.IntentRepairPrepared,
			Status:         state.IntentRepairGitApplied,
			BackupRef: sql.NullString{
				String: "refs/acd/intent-repair/test/completed-checkpoint-head-repair/backup",
				Valid:  true,
			},
			OldHead:      sql.NullString{String: oldHead, Valid: true},
			NewHead:      sql.NullString{String: newHead, Valid: true},
			Commits:      mapping,
			TransitionTS: 0.2,
		})
	if err != nil || !applied {
		t.Fatalf("mark repair Git-applied=(%t,%v)", applied, err)
	}
	completed, err := state.TransitionIntentRepair(ctx, db, repairID,
		state.IntentRepairTransition{
			ExpectedStatus: state.IntentRepairGitApplied,
			Status:         state.IntentRepairCompleted,
			TransitionTS:   0.3,
		})
	if err != nil || !completed {
		t.Fatalf("complete repair=(%t,%v)", completed, err)
	}

	blobOID, err := gitpkg.HashObjectStdin(ctx, repo, []byte("protected capture\n"))
	if err != nil {
		t.Fatal(err)
	}
	pendingSeqs := make([]int64, 0, 22)
	for index := 0; index < 22; index++ {
		path := fmt.Sprintf("pending-%02d.txt", index+1)
		seq, err := state.AppendCaptureEvent(ctx, db, state.CaptureEvent{
			BranchRef: branchRef, BranchGeneration: branchGeneration,
			BaseHead: oldHead, Operation: "create", Path: path,
			Fidelity: "exact", CapturedTS: 0.4 + float64(index)/100,
		}, []state.CaptureOp{{
			Op: "create", Path: path, Fidelity: "exact",
			AfterOID:  sql.NullString{String: blobOID, Valid: true},
			AfterMode: sql.NullString{String: gitpkg.RegularFileMode, Valid: true},
		}})
		if err != nil {
			t.Fatalf("append pending capture %d: %v", index+1, err)
		}
		pendingSeqs = append(pendingSeqs, seq)
	}
	anchor, err := snapshotPublicationDrainTarget(
		ctx, db, branchRef, branchGeneration)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(anchor.EventSeqs, pendingSeqs) {
		t.Fatalf("anchor members=%v want %v", anchor.EventSeqs, pendingSeqs)
	}

	worktreeID := checkpointpkg.WorktreeID(repo)
	checkpointRef := "refs/acd/checkpoints/v1/" + worktreeID + "/" + checkpointID
	oldTree, err := gitpkg.RevParse(ctx, repo, oldHead+"^{tree}")
	if err != nil {
		t.Fatal(err)
	}
	checkpoint := state.Checkpoint{
		ID: checkpointID, OperationID: "op-" + checkpointID,
		WorktreeID: worktreeID, Reason: state.CheckpointReasonManualBarrier,
		ObservationEpoch: 1, CoverageEpoch: 1,
		ObservedHead: oldHead, ObservedRef: branchRef,
		TreeOID: oldTree, CommitOID: oldHead, Ref: checkpointRef,
		CreatedTS: 1, EventSeqs: pendingSeqs,
	}
	prepared, err := state.PrepareCheckpoint(
		ctx, db, checkpoint, completedTransitionBarrierDigest)
	if err != nil || !prepared {
		t.Fatalf("prepare checkpoint=(%t,%v)", prepared, err)
	}
	if err := gitpkg.UpdateRef(ctx, repo, checkpointRef, oldHead, ""); err != nil {
		t.Fatal(err)
	}
	if err := state.CompleteCheckpoint(
		ctx, db, checkpointID, checkpointRef, oldHead, 1.1); err != nil {
		t.Fatal(err)
	}
	if err := state.MetaSet(ctx, db, daemon.MetaKeyBranchGeneration, "7"); err != nil {
		t.Fatal(err)
	}

	target, err := freezePublicationDrainTarget(
		ctx, db, repo, checkpointID, worktreeID, 1, anchor)
	if err != nil {
		t.Fatal(err)
	}
	if target.BranchRef != branchRef || target.Generation != branchGeneration ||
		len(target.EventSeqs) != 22 ||
		!reflect.DeepEqual(target.EventSeqs, pendingSeqs) ||
		target.MaxSeq != pendingSeqs[len(pendingSeqs)-1] {
		t.Fatalf("frozen target=%+v want all 22 pending captures", target)
	}
}
