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
	afterRepair, err := git.HashObjectStdin(ctx, f.dir, []byte("after repair\n"))
	if err != nil {
		t.Fatal(err)
	}
	pending, err := git.HashObjectStdin(ctx, f.dir, []byte("pending change\n"))
	if err != nil {
		t.Fatal(err)
	}

	oldBase := strings.Repeat("1", 40) // The pruned pre-repair commit is absent.
	repairedBase := commitSingleFileTree(
		t, ctx, f.dir, "doc.md", afterRepair, "repaired candidate base")
	if err := git.UpdateRef(ctx, f.dir, f.cctx.BranchRef, repairedBase, ""); err != nil {
		t.Fatalf("install repaired head: %v", err)
	}

	recordCompletedRepairMapping(
		t, ctx, f, "completed-repair-recovery-base", oldBase, repairedBase)

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

func TestReconcileUnpublishedChainProvesPublishedAcrossCompletedRepairs(t *testing.T) {
	f := newCaptureFixture(t)
	ctx := context.Background()
	anchorBefore, _ := git.HashObjectStdin(ctx, f.dir, []byte("anchor before\n"))
	anchorPublished, _ := git.HashObjectStdin(ctx, f.dir, []byte("anchor published\n"))
	anchorFinal, _ := git.HashObjectStdin(ctx, f.dir, []byte("anchor final\n"))
	a, _ := git.HashObjectStdin(ctx, f.dir, []byte("A\n"))
	b, _ := git.HashObjectStdin(ctx, f.dir, []byte("B\n"))
	c, _ := git.HashObjectStdin(ctx, f.dir, []byte("C\n"))
	d, _ := git.HashObjectStdin(ctx, f.dir, []byte("D\n"))

	canonicalBase := commitTreeWithIndexUpdates(t, ctx, f, f.cctx.BaseHead,
		"canonical base",
		git.RegularFileMode+" "+anchorBefore+"\tanchor.txt",
		git.RegularFileMode+" "+a+"\tdoc.md")
	canonicalPublished := commitTreeWithIndexUpdates(t, ctx, f, canonicalBase,
		"canonical published context",
		git.RegularFileMode+" "+anchorPublished+"\tanchor.txt",
		git.RegularFileMode+" "+c+"\tdoc.md")
	liveHead := commitTreeWithIndexUpdates(t, ctx, f, canonicalPublished,
		"final published state",
		git.RegularFileMode+" "+anchorFinal+"\tanchor.txt",
		git.RegularFileMode+" "+d+"\tdoc.md")
	if err := git.UpdateRef(
		ctx, f.dir, f.cctx.BranchRef, liveHead, f.cctx.BaseHead); err != nil {
		t.Fatalf("install final head: %v", err)
	}

	obsoleteBase := strings.Repeat("2", 40)
	obsoletePublished := strings.Repeat("3", 40)
	recordCompletedRepairMapping(
		t, ctx, f, "completed-repair-base", obsoleteBase, canonicalBase)
	recordCompletedRepairMapping(
		t, ctx, f, "completed-repair-published", obsoletePublished,
		canonicalPublished)

	first := appendRecoveryEvent(t, ctx, f, obsoleteBase, state.CaptureOp{
		Op: "modify", Path: "doc.md",
		BeforeOID:  sql.NullString{String: a, Valid: true},
		BeforeMode: sql.NullString{String: git.RegularFileMode, Valid: true},
		AfterOID:   sql.NullString{String: b, Valid: true},
		AfterMode:  sql.NullString{String: git.RegularFileMode, Valid: true},
	})
	published := appendRecoveryEvent(t, ctx, f, obsoleteBase, state.CaptureOp{
		Op: "modify", Path: "anchor.txt",
		BeforeOID:  sql.NullString{String: anchorBefore, Valid: true},
		BeforeMode: sql.NullString{String: git.RegularFileMode, Valid: true},
		AfterOID:   sql.NullString{String: anchorPublished, Valid: true},
		AfterMode:  sql.NullString{String: git.RegularFileMode, Valid: true},
	})
	if err := state.MarkEventPublished(ctx, f.db, published,
		state.EventStatePublished,
		sql.NullString{String: obsoletePublished, Valid: true},
		sql.NullString{}, sql.NullString{}, 2); err != nil {
		t.Fatalf("mark repaired context published: %v", err)
	}
	appendRecoveryEvent(t, ctx, f, obsoletePublished, state.CaptureOp{
		Op: "modify", Path: "doc.md",
		BeforeOID:  sql.NullString{String: c, Valid: true},
		BeforeMode: sql.NullString{String: git.RegularFileMode, Valid: true},
		AfterOID:   sql.NullString{String: d, Valid: true},
		AfterMode:  sql.NullString{String: git.RegularFileMode, Valid: true},
	})
	last := appendRecoveryEvent(t, ctx, f, obsoletePublished, state.CaptureOp{
		Op: "modify", Path: "anchor.txt",
		BeforeOID:  sql.NullString{String: anchorPublished, Valid: true},
		BeforeMode: sql.NullString{String: git.RegularFileMode, Valid: true},
		AfterOID:   sql.NullString{String: anchorFinal, Valid: true},
		AfterMode:  sql.NullString{String: git.RegularFileMode, Valid: true},
	})
	opts := RecoveryReconcileOptions{
		BranchRef: f.cctx.BranchRef, BranchGeneration: f.cctx.BranchGeneration,
		FirstSeq: first, GitDir: f.gitDir,
	}

	proved, err := ProveUnpublishedChain(ctx, f.dir, f.db, opts)
	if err != nil {
		t.Fatalf("prove repaired published chain: %v", err)
	}
	if !proved.Handled || proved.Outcome != state.EventStatePublished ||
		proved.EventCount != 3 {
		t.Fatalf("proof=%+v want three-event published chain", proved)
	}

	result, err := ReconcileUnpublishedChain(ctx, f.dir, f.db, opts)
	if err != nil {
		t.Fatalf("reconcile repaired published chain: %v", err)
	}
	if !result.Handled || result.Outcome != state.EventStatePublished ||
		result.CommitOID != liveHead || result.FirstSeq != first ||
		result.LastSeq != last || result.EventCount != 3 {
		t.Fatalf("result=%+v want published at %s", result, liveHead)
	}
	if gotState, gotOID := readEventState(t, ctx, f.db, published); gotState != state.EventStatePublished || !gotOID.Valid ||
		gotOID.String != obsoletePublished {
		t.Fatalf("context state=%q oid=%v changed", gotState, gotOID)
	}
}

func recordCompletedRepairMapping(
	t *testing.T,
	ctx context.Context,
	f *captureFixture,
	repairID string,
	oldOID string,
	newOID string,
) {
	t.Helper()
	candidateID := repairID + "-candidate"
	if err := state.SaveIntentRepair(ctx, f.db, state.IntentRepair{
		ID: repairID, BranchRef: f.cctx.BranchRef,
		BranchGeneration: f.cctx.BranchGeneration,
		Status:           state.IntentRepairPrepared,
		ExpectedHead:     oldOID,
		PlanDigest:       "sha256:" + strings.Repeat("a", 64),
		OldHead:          sql.NullString{String: oldOID, Valid: true},
		Commits: []state.IntentRepairCommit{{
			CandidateID: sql.NullString{String: candidateID, Valid: true},
			OldOID:      oldOID,
		}},
	}); err != nil {
		t.Fatalf("save repair %s: %v", repairID, err)
	}
	applied, err := state.TransitionIntentRepair(ctx, f.db, repairID,
		state.IntentRepairTransition{
			ExpectedStatus: state.IntentRepairPrepared,
			Status:         state.IntentRepairGitApplied,
			BackupRef: sql.NullString{
				String: "refs/acd/intent-repair/test/" + repairID + "/backup",
				Valid:  true,
			},
			OldHead: sql.NullString{String: oldOID, Valid: true},
			NewHead: sql.NullString{String: newOID, Valid: true},
			Commits: []state.IntentRepairCommit{{
				CandidateID: sql.NullString{String: candidateID, Valid: true},
				OldOID:      oldOID,
				NewOID:      sql.NullString{String: newOID, Valid: true},
			}},
		})
	if err != nil || !applied {
		t.Fatalf("mark repair %s Git-applied=(%t, %v)", repairID, applied, err)
	}
	completed, err := state.TransitionIntentRepair(ctx, f.db, repairID,
		state.IntentRepairTransition{
			ExpectedStatus: state.IntentRepairGitApplied,
			Status:         state.IntentRepairCompleted,
		})
	if err != nil || !completed {
		t.Fatalf("complete repair %s=(%t, %v)", repairID, completed, err)
	}
}
