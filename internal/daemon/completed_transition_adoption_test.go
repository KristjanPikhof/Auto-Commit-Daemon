package daemon

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

func TestCompletedTransitionProofAttentionPersistsAndClears(t *testing.T) {
	f := newDaemonFixture(t)
	ctx := context.Background()
	proofErr := errors.Join(
		state.ErrCompletedBranchTransitionProof,
		errors.New("ambiguous completed transition"))
	if err := recordCompletedTransitionProofAttention(
		ctx, f.db, proofErr); err != nil {
		t.Fatal(err)
	}
	value, ok, err := state.MetaGet(
		ctx, f.db, MetaKeyBranchTransitionNeedsAttention)
	if err != nil || !ok || !strings.Contains(value, "needs attention") ||
		!strings.Contains(value, "ambiguous completed transition") {
		t.Fatalf("transition attention=(%q,%t,%v)", value, ok, err)
	}
	if err := recordCompletedTransitionProofAttention(
		ctx, f.db, errors.New("transient database read")); err != nil {
		t.Fatal(err)
	}
	if unchanged, _, _ := state.MetaGet(
		ctx, f.db, MetaKeyBranchTransitionNeedsAttention); unchanged != value {
		t.Fatalf("transient error replaced durable attention: %q", unchanged)
	}
	head, err := git.RevParse(ctx, f.dir, "HEAD")
	if err != nil {
		t.Fatal(err)
	}
	if err := SaveBranchPublicationToken(
		ctx, f.db, 1, head,
		branchTokenRev(head, "refs/heads/main")); err != nil {
		t.Fatal(err)
	}
	if cleared, _, _ := state.MetaGet(
		ctx, f.db, MetaKeyBranchTransitionNeedsAttention); cleared != "" {
		t.Fatalf("transition attention not cleared after adoption: %q", cleared)
	}
}

func TestRunAdoptsAlreadyCompletedIntentRepairBeforeStartupTransition(t *testing.T) {
	f := newIntentRepairFixture(t, 2)
	ctx := context.Background()
	oldToken := branchTokenRev(f.plan.ExpectedHead, f.cctx.BranchRef)
	if err := SaveBranchPublicationToken(
		ctx, f.repo.db, f.cctx.BranchGeneration,
		f.plan.ExpectedHead, oldToken,
	); err != nil {
		t.Fatalf("SaveBranchPublicationToken: %v", err)
	}

	applied, err := ApplyIntentRepairTransaction(
		ctx, f.repo.dir, f.repo.gitDir, f.repo.db, f.cctx, f.plan)
	if err != nil || applied.Status != state.IntentRepairCompleted ||
		applied.NewHead == "" || applied.NewHead == f.plan.ExpectedHead {
		t.Fatalf("ApplyIntentRepairTransaction=(%+v,%v)", applied, err)
	}
	if head, err := git.RevParse(ctx, f.repo.dir, "HEAD"); err != nil || head != applied.NewHead {
		t.Fatalf("Git HEAD=%q err=%v want repaired %s",
			head, err, applied.NewHead)
	}
	if persisted, err := LoadBranchHead(ctx, f.repo.db); err != nil || persisted != f.plan.ExpectedHead {
		t.Fatalf("pre-restart branch.head=%q err=%v want stale %s",
			persisted, err, f.plan.ExpectedHead)
	}

	shutdown := make(chan struct{})
	close(shutdown)
	if err := Run(ctx, Options{
		RepoPath: f.repo.dir, GitDir: f.repo.gitDir, DB: f.repo.db,
		MessageFn: DeterministicMessage, ShutdownCh: shutdown,
		SkipSignals: true, BootGrace: time.Hour,
	}); err != nil {
		t.Fatalf("Run: %v", err)
	}

	assertIntentRepairReconciled(t, f, applied.NewHead)
	if generation, err := LoadBranchGeneration(ctx, f.repo.db); err != nil || generation != f.cctx.BranchGeneration {
		t.Fatalf("branch generation=%d err=%v want %d",
			generation, err, f.cctx.BranchGeneration)
	}
	if persisted, err := LoadBranchHead(ctx, f.repo.db); err != nil || persisted != applied.NewHead {
		t.Fatalf("branch.head=%q err=%v want %s",
			persisted, err, applied.NewHead)
	}
	wantToken := branchTokenRev(applied.NewHead, f.cctx.BranchRef)
	if token, ok, err := state.MetaGet(ctx, f.repo.db, MetaKeyBranchToken); err != nil || !ok || token != wantToken {
		t.Fatalf("branch token=(%q,%t,%v) want %q", token, ok, err, wantToken)
	}
	if _, ok, err := state.MetaGet(
		ctx, f.repo.db, MetaKeyBranchTokenChangedAt,
	); err != nil || ok {
		t.Fatalf("owned repair recorded generic transition timestamp: ok=%t err=%v",
			ok, err)
	}
	if _, ok, err := state.MetaGet(
		ctx, f.repo.db, MetaKeyReplayPausedUntil,
	); err != nil || ok {
		t.Fatalf("owned repair armed rewind grace: ok=%t err=%v", ok, err)
	}
	if snapshots := countRecoverySnapshots(t, ctx, f.repo.db); snapshots != 0 {
		t.Fatalf("owned repair created recovery snapshots=%d want 0", snapshots)
	}
}

func TestCheckEventGenerationUsesCompletedRepairMappingAncestry(t *testing.T) {
	f := newIntentRepairFixture(t, 2)
	ctx := context.Background()
	applied, err := ApplyIntentRepairTransaction(
		ctx, f.repo.dir, f.repo.gitDir, f.repo.db, f.cctx, f.plan)
	if err != nil || applied.Status != state.IntentRepairCompleted {
		t.Fatalf("ApplyIntentRepairTransaction=(%+v,%v)", applied, err)
	}
	oldBase := f.oldCommits[0]
	if mapped := applied.CommitMap[oldBase]; mapped != applied.NewHead {
		t.Fatalf("repair mapping[%s]=%q want %s",
			oldBase, mapped, applied.NewHead)
	}

	tree, err := git.RevParse(ctx, f.repo.dir, applied.NewHead+"^{tree}")
	if err != nil {
		t.Fatalf("resolve repaired tree: %v", err)
	}
	descendant, err := git.CommitTree(
		ctx, f.repo.dir, tree, "descendant of repaired base", applied.NewHead)
	if err != nil {
		t.Fatalf("create repaired descendant: %v", err)
	}
	unrelated, err := git.CommitTree(
		ctx, f.repo.dir, tree, "unrelated replay parent", f.repo.head)
	if err != nil {
		t.Fatalf("create unrelated parent: %v", err)
	}
	event := state.CaptureEvent{
		BranchRef:        f.cctx.BranchRef,
		BranchGeneration: f.cctx.BranchGeneration,
		BaseHead:         oldBase,
	}
	repairedCtx := f.cctx
	repairedCtx.BaseHead = applied.NewHead

	reason, err := checkEventGeneration(
		ctx, f.repo.dir, f.repo.db, descendant, event, repairedCtx)
	if err != nil || reason != "" {
		t.Fatalf("mapped ancestor rejected: reason=%q err=%v", reason, err)
	}

	reason, err = checkEventGeneration(
		ctx, f.repo.dir, f.repo.db, unrelated, event, repairedCtx)
	if err != nil {
		t.Fatalf("unrelated parent proof: %v", err)
	}
	if !strings.Contains(reason, "repaired event base") ||
		!strings.Contains(reason, applied.NewHead) ||
		!strings.Contains(reason, unrelated) {
		t.Fatalf("unrelated parent reason=%q", reason)
	}
}

func TestAmbiguousCompletedRepairMappingPersistsReplayAttention(t *testing.T) {
	f := newCaptureFixture(t)
	ctx := context.Background()
	oldBase := strings.Repeat("1", 40)
	recordCompletedRepairMapping(
		t, ctx, f, "ambiguous-repair-a", oldBase, f.cctx.BaseHead)

	const secondRepair = "ambiguous-repair-b"
	secondTarget := strings.Repeat("2", 40)
	if _, err := f.db.SQL().ExecContext(ctx, `
INSERT INTO intent_repairs(
    id,branch_ref,branch_generation,status,expected_head,plan_digest,
    backup_ref,old_head,new_head,created_ts,updated_ts,git_applied_ts,
    completed_ts,error
) VALUES (?,?,?,'completed',?,?,?,?,?,1,3,2,3,'')`,
		secondRepair, f.cctx.BranchRef, f.cctx.BranchGeneration,
		oldBase, "sha256:"+strings.Repeat("b", 64),
		"refs/acd/intent-repair/test/ambiguous-repair-b/backup",
		oldBase, secondTarget); err != nil {
		t.Fatal(err)
	}
	if _, err := f.db.SQL().ExecContext(ctx, `
INSERT INTO intent_repair_commits(
    repair_id,ord,candidate_id,old_oid,new_oid
) VALUES (?,0,?,?,?)`, secondRepair, "candidate-b", oldBase, secondTarget); err != nil {
		t.Fatal(err)
	}
	if _, err := f.db.SQL().ExecContext(ctx, `
INSERT INTO intent_repair_member_seals(
    repair_id,membership_mode,member_count
) VALUES (?,'legacy',0)`, secondRepair); err != nil {
		t.Fatal(err)
	}

	event := state.CaptureEvent{
		BranchRef:        f.cctx.BranchRef,
		BranchGeneration: f.cctx.BranchGeneration,
		BaseHead:         oldBase,
	}
	_, proofErr := checkEventGeneration(
		ctx, f.dir, f.db, f.cctx.BaseHead, event, f.cctx)
	if proofErr == nil ||
		!errors.Is(proofErr, state.ErrCompletedBranchTransitionProof) {
		t.Fatalf("checkEventGeneration error=%v", proofErr)
	}
	if _, _, err := recordReplayErrorObservability(
		ctx, f.db, proofErr, time.Unix(100, 0)); err != nil {
		t.Fatal(err)
	}
	attention, ok, err := state.MetaGet(
		ctx, f.db, MetaKeyBranchTransitionNeedsAttention)
	if err != nil || !ok || !strings.Contains(attention, "ambiguous") {
		t.Fatalf("transition attention=(%q,%t,%v)", attention, ok, err)
	}
}
