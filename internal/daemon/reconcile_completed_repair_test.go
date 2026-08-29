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

func TestReconcileUnpublishedChainUsesRestoredPreRepairContext(t *testing.T) {
	f := newCaptureFixture(t)
	ctx := context.Background()
	a, _ := git.HashObjectStdin(ctx, f.dir, []byte("A\n"))
	b, _ := git.HashObjectStdin(ctx, f.dir, []byte("B\n"))
	c, _ := git.HashObjectStdin(ctx, f.dir, []byte("C\n"))
	marker, _ := git.HashObjectStdin(ctx, f.dir, []byte("repair\n"))

	base := commitTreeWithIndexUpdates(t, ctx, f, f.cctx.BaseHead,
		"seed recovery base", git.RegularFileMode+" "+a+"\tdoc.md")
	contextCommit := commitTreeWithIndexUpdates(t, ctx, f, base,
		"publish intermediate state", git.RegularFileMode+" "+c+"\tdoc.md")
	originalHead := commitTreeWithIndexUpdates(t, ctx, f, contextCommit,
		"publish final state", git.RegularFileMode+" "+b+"\tdoc.md")
	repairedHead := commitTreeWithIndexUpdates(t, ctx, f, contextCommit,
		"rewrite different state", git.RegularFileMode+" "+marker+"\trepair.txt")
	if err := git.UpdateRef(
		ctx, f.dir, f.cctx.BranchRef, originalHead, f.cctx.BaseHead); err != nil {
		t.Fatalf("restore original head: %v", err)
	}
	recordCompletedRepairMapping(
		t, ctx, f, "completed-repair-restored-source", originalHead, repairedHead)

	intermediateSeq := appendRecoveryEvent(t, ctx, f, base, state.CaptureOp{
		Op: "modify", Path: "doc.md",
		BeforeOID:  sql.NullString{String: a, Valid: true},
		BeforeMode: sql.NullString{String: git.RegularFileMode, Valid: true},
		AfterOID:   sql.NullString{String: c, Valid: true},
		AfterMode:  sql.NullString{String: git.RegularFileMode, Valid: true},
	})
	if err := state.MarkEventPublished(ctx, f.db, intermediateSeq,
		state.EventStatePublished,
		sql.NullString{String: contextCommit, Valid: true},
		sql.NullString{}, sql.NullString{}, 1); err != nil {
		t.Fatalf("mark intermediate context published: %v", err)
	}
	originalSeq := appendRecoveryEvent(t, ctx, f, contextCommit, state.CaptureOp{
		Op: "modify", Path: "doc.md",
		BeforeOID:  sql.NullString{String: c, Valid: true},
		BeforeMode: sql.NullString{String: git.RegularFileMode, Valid: true},
		AfterOID:   sql.NullString{String: b, Valid: true},
		AfterMode:  sql.NullString{String: git.RegularFileMode, Valid: true},
	})
	if err := state.MarkEventPublished(ctx, f.db, originalSeq,
		state.EventStatePublished,
		sql.NullString{String: originalHead, Valid: true},
		sql.NullString{}, sql.NullString{}, 2); err != nil {
		t.Fatalf("mark original context published: %v", err)
	}
	pendingSeq := appendRecoveryEvent(t, ctx, f, base, state.CaptureOp{
		Op: "modify", Path: "doc.md",
		BeforeOID:  sql.NullString{String: a, Valid: true},
		BeforeMode: sql.NullString{String: git.RegularFileMode, Valid: true},
		AfterOID:   sql.NullString{String: b, Valid: true},
		AfterMode:  sql.NullString{String: git.RegularFileMode, Valid: true},
	})
	opts := RecoveryReconcileOptions{
		BranchRef: f.cctx.BranchRef, BranchGeneration: f.cctx.BranchGeneration,
		FirstSeq: pendingSeq, GitDir: f.gitDir,
	}

	proved, err := ProveUnpublishedChain(ctx, f.dir, f.db, opts)
	if err != nil {
		t.Fatalf("prove restored-source recovery: %v", err)
	}
	if !proved.Handled || proved.Outcome != state.EventStatePublished ||
		proved.EventCount != 1 {
		t.Fatalf("proof=%+v want one-event published chain", proved)
	}

	result, err := ReconcileUnpublishedChain(ctx, f.dir, f.db, opts)
	if err != nil {
		t.Fatalf("reconcile restored-source recovery: %v", err)
	}
	if !result.Handled || result.Outcome != state.EventStatePublished ||
		result.CommitOID != originalHead || result.EventCount != 1 {
		t.Fatalf("result=%+v want published at restored head %s",
			result, originalHead)
	}
	if gotState, gotOID := readEventState(
		t, ctx, f.db, pendingSeq); gotState != state.EventStatePublished ||
		!gotOID.Valid || gotOID.String != originalHead {
		t.Fatalf("pending state=%q oid=%v want published at %s",
			gotState, gotOID, originalHead)
	}
}

func TestReconcileUnpublishedChainUsesRestoredIntermediateRepairContext(t *testing.T) {
	f := newCaptureFixture(t)
	ctx := context.Background()
	a, _ := git.HashObjectStdin(ctx, f.dir, []byte("A\n"))
	b, _ := git.HashObjectStdin(ctx, f.dir, []byte("B\n"))
	c, _ := git.HashObjectStdin(ctx, f.dir, []byte("C\n"))
	firstMarker, _ := git.HashObjectStdin(ctx, f.dir, []byte("repair one\n"))
	secondMarker, _ := git.HashObjectStdin(ctx, f.dir, []byte("repair two\n"))

	base := commitTreeWithIndexUpdates(t, ctx, f, f.cctx.BaseHead,
		"seed recovery base", git.RegularFileMode+" "+a+"\tdoc.md")
	contextCommit := commitTreeWithIndexUpdates(t, ctx, f, base,
		"publish intermediate state", git.RegularFileMode+" "+c+"\tdoc.md")
	originalHead := commitTreeWithIndexUpdates(t, ctx, f, contextCommit,
		"publish final state", git.RegularFileMode+" "+b+"\tdoc.md")
	intermediateRepair := commitTreeWithIndexUpdates(t, ctx, f, contextCommit,
		"first repair",
		git.RegularFileMode+" "+b+"\tdoc.md",
		git.RegularFileMode+" "+firstMarker+"\trepair-one.txt")
	finalRepair := commitTreeWithIndexUpdates(t, ctx, f, contextCommit,
		"second repair",
		git.RegularFileMode+" "+c+"\tdoc.md",
		git.RegularFileMode+" "+secondMarker+"\trepair-two.txt")
	if err := git.UpdateRef(
		ctx, f.dir, f.cctx.BranchRef, intermediateRepair,
		f.cctx.BaseHead); err != nil {
		t.Fatalf("restore intermediate repair head: %v", err)
	}
	recordCompletedRepairMapping(
		t, ctx, f, "completed-repair-original", originalHead,
		intermediateRepair)
	recordCompletedRepairMapping(
		t, ctx, f, "completed-repair-intermediate", intermediateRepair,
		finalRepair)

	chain, err := state.CompletedIntentRepairCommitChain(
		ctx, f.db, f.cctx.BranchRef, f.cctx.BranchGeneration, originalHead)
	if err != nil {
		t.Fatalf("load completed repair chain: %v", err)
	}
	wantChain := []string{originalHead, intermediateRepair, finalRepair}
	if len(chain) != len(wantChain) {
		t.Fatalf("repair chain=%v want %v", chain, wantChain)
	}
	for i := range wantChain {
		if chain[i] != wantChain[i] {
			t.Fatalf("repair chain=%v want %v", chain, wantChain)
		}
	}

	intermediateSeq := appendRecoveryEvent(t, ctx, f, base, state.CaptureOp{
		Op: "modify", Path: "doc.md",
		BeforeOID:  sql.NullString{String: a, Valid: true},
		BeforeMode: sql.NullString{String: git.RegularFileMode, Valid: true},
		AfterOID:   sql.NullString{String: c, Valid: true},
		AfterMode:  sql.NullString{String: git.RegularFileMode, Valid: true},
	})
	if err := state.MarkEventPublished(ctx, f.db, intermediateSeq,
		state.EventStatePublished,
		sql.NullString{String: contextCommit, Valid: true},
		sql.NullString{}, sql.NullString{}, 1); err != nil {
		t.Fatalf("mark intermediate context published: %v", err)
	}
	originalSeq := appendRecoveryEvent(t, ctx, f, contextCommit, state.CaptureOp{
		Op: "modify", Path: "doc.md",
		BeforeOID:  sql.NullString{String: c, Valid: true},
		BeforeMode: sql.NullString{String: git.RegularFileMode, Valid: true},
		AfterOID:   sql.NullString{String: b, Valid: true},
		AfterMode:  sql.NullString{String: git.RegularFileMode, Valid: true},
	})
	if err := state.MarkEventPublished(ctx, f.db, originalSeq,
		state.EventStatePublished,
		sql.NullString{String: originalHead, Valid: true},
		sql.NullString{}, sql.NullString{}, 2); err != nil {
		t.Fatalf("mark original context published: %v", err)
	}
	pendingSeq := appendRecoveryEvent(t, ctx, f, base, state.CaptureOp{
		Op: "modify", Path: "doc.md",
		BeforeOID:  sql.NullString{String: a, Valid: true},
		BeforeMode: sql.NullString{String: git.RegularFileMode, Valid: true},
		AfterOID:   sql.NullString{String: b, Valid: true},
		AfterMode:  sql.NullString{String: git.RegularFileMode, Valid: true},
	})
	opts := RecoveryReconcileOptions{
		BranchRef: f.cctx.BranchRef, BranchGeneration: f.cctx.BranchGeneration,
		FirstSeq: pendingSeq, GitDir: f.gitDir,
	}

	proved, err := ProveUnpublishedChain(ctx, f.dir, f.db, opts)
	if err != nil {
		t.Fatalf("prove restored-intermediate recovery: %v", err)
	}
	if !proved.Handled || proved.Outcome != state.EventStatePublished ||
		proved.EventCount != 1 {
		t.Fatalf("proof=%+v want one-event published chain", proved)
	}

	result, err := ReconcileUnpublishedChain(ctx, f.dir, f.db, opts)
	if err != nil {
		t.Fatalf("reconcile restored-intermediate recovery: %v", err)
	}
	if !result.Handled || result.Outcome != state.EventStatePublished ||
		result.CommitOID != intermediateRepair || result.EventCount != 1 {
		t.Fatalf("result=%+v want published at restored head %s",
			result, intermediateRepair)
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
	tx, err := f.db.SQL().BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `
INSERT INTO intent_repairs(
    id,branch_ref,branch_generation,status,expected_head,plan_digest,
    backup_ref,old_head,new_head,created_ts,updated_ts,git_applied_ts,
    completed_ts,error
) VALUES (?,?,?,'completed',?,?,?,?,?,1,3,2,3,'')`,
		repairID, f.cctx.BranchRef, f.cctx.BranchGeneration,
		oldOID, "sha256:"+strings.Repeat("a", 64),
		"refs/acd/intent-repair/test/"+repairID+"/backup",
		oldOID, newOID); err != nil {
		t.Fatalf("record completed legacy repair %s: %v", repairID, err)
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO intent_repair_commits(
    repair_id,ord,candidate_id,old_oid,new_oid
) VALUES (?,0,?,?,?)`, repairID, candidateID, oldOID, newOID); err != nil {
		t.Fatalf("record completed legacy mapping %s: %v", repairID, err)
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO intent_repair_member_seals(
    repair_id,membership_mode,member_count
) VALUES (?,'legacy',0)`, repairID); err != nil {
		t.Fatalf("seal completed legacy repair %s: %v", repairID, err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit completed legacy repair %s: %v", repairID, err)
	}
	canonical, mapped, err := state.CanonicalCompletedIntentRepairCommit(
		ctx, f.db, f.cctx.BranchRef, f.cctx.BranchGeneration, oldOID)
	if err != nil || !mapped || canonical != newOID {
		t.Fatalf("canonical repair %s=(%s,%t,%v), want %s",
			repairID, canonical, mapped, err, newOID)
	}
}
