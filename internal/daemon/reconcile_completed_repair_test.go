package daemon

import (
	"context"
	"database/sql"
	"strings"
	"testing"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

func TestReconcileUnpublishedChainUsesCompletedRepairBase(t *testing.T) {
	f := newCaptureFixture(t)
	ctx := context.Background()
	beforeRepair, err := git.HashObjectStdin(ctx, f.dir, []byte("before repair\n"))
	if err != nil {
		t.Fatal(err)
	}
	afterRepair, err := git.HashObjectStdin(ctx, f.dir, []byte("after repair\n"))
	if err != nil {
		t.Fatal(err)
	}
	pending, err := git.HashObjectStdin(ctx, f.dir, []byte("pending change\n"))
	if err != nil {
		t.Fatal(err)
	}

	oldBase := commitSingleFileTree(
		t, ctx, f.dir, "doc.md", beforeRepair, "old candidate base")
	repairedBase := commitSingleFileTree(
		t, ctx, f.dir, "doc.md", afterRepair, "repaired candidate base")
	if err := git.UpdateRef(ctx, f.dir, f.cctx.BranchRef, repairedBase, ""); err != nil {
		t.Fatalf("install repaired head: %v", err)
	}

	const repairID = "completed-repair-recovery-base"
	if err := state.SaveIntentRepair(ctx, f.db, state.IntentRepair{
		ID: repairID, BranchRef: f.cctx.BranchRef,
		BranchGeneration: f.cctx.BranchGeneration,
		Status:           state.IntentRepairPrepared,
		ExpectedHead:     oldBase,
		PlanDigest:       "sha256:" + strings.Repeat("a", 64),
		OldHead:          sql.NullString{String: oldBase, Valid: true},
		Commits: []state.IntentRepairCommit{{
			CandidateID: sql.NullString{String: "repaired-candidate", Valid: true},
			OldOID:      oldBase,
		}},
	}); err != nil {
		t.Fatal(err)
	}
	applied, err := state.TransitionIntentRepair(ctx, f.db, repairID,
		state.IntentRepairTransition{
			ExpectedStatus: state.IntentRepairPrepared,
			Status:         state.IntentRepairGitApplied,
			BackupRef: sql.NullString{
				String: "refs/acd/intent-repair/test/backup", Valid: true,
			},
			OldHead: sql.NullString{String: oldBase, Valid: true},
			NewHead: sql.NullString{String: repairedBase, Valid: true},
			Commits: []state.IntentRepairCommit{{
				CandidateID: sql.NullString{String: "repaired-candidate", Valid: true},
				OldOID:      oldBase,
				NewOID:      sql.NullString{String: repairedBase, Valid: true},
			}},
		})
	if err != nil || !applied {
		t.Fatalf("mark repair Git-applied=(%t, %v)", applied, err)
	}
	completed, err := state.TransitionIntentRepair(ctx, f.db, repairID,
		state.IntentRepairTransition{
			ExpectedStatus: state.IntentRepairGitApplied,
			Status:         state.IntentRepairCompleted,
		})
	if err != nil || !completed {
		t.Fatalf("complete repair=(%t, %v)", completed, err)
	}

	seq := appendRecoveryEvent(t, ctx, f, oldBase, state.CaptureOp{
		Op:         "modify",
		Path:       "doc.md",
		BeforeOID:  sql.NullString{String: afterRepair, Valid: true},
		BeforeMode: sql.NullString{String: git.RegularFileMode, Valid: true},
		AfterOID:   sql.NullString{String: pending, Valid: true},
		AfterMode:  sql.NullString{String: git.RegularFileMode, Valid: true},
	})
	opts := RecoveryReconcileOptions{
		BranchRef: f.cctx.BranchRef, BranchGeneration: f.cctx.BranchGeneration,
		FirstSeq: seq, GitDir: f.gitDir,
	}
	proved, err := ProveUnpublishedChain(ctx, f.dir, f.db, opts)
	if err != nil {
		t.Fatalf("prove repaired-base recovery: %v", err)
	}
	if !proved.Handled || proved.Outcome != state.EventStateRecovered {
		t.Fatalf("proof=%+v want recovered", proved)
	}

	result, err := ReconcileUnpublishedChain(ctx, f.dir, f.db, opts)
	if err != nil {
		t.Fatalf("reconcile repaired-base recovery: %v", err)
	}
	if !result.Handled || result.Outcome != state.EventStateRecovered {
		t.Fatalf("result=%+v want recovered", result)
	}
	parent, err := git.RevParse(ctx, f.dir, result.RecoveryRef+"^")
	if err != nil || parent != repairedBase {
		t.Fatalf("recovery parent=%s err=%v want %s", parent, err, repairedBase)
	}
	blob, err := git.LsTreeBlobOID(ctx, f.dir, result.RecoveryRef, "doc.md")
	if err != nil || blob != pending {
		t.Fatalf("recovered blob=%s err=%v want %s", blob, err, pending)
	}
}
