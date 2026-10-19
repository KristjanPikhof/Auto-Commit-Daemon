package cli

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/checkpoint"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/daemon"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

func TestFinishRemovalLegacyRecoveryPreservesLaterCheckpointAndStaging(t *testing.T) {
	repo, stateDB, db := makeRegisteredGitRepoStateDB(t)
	ctx := context.Background()
	first, second := stageRecoverableBarrierPair(t, ctx, repo, db, "refs/heads/main", 1)
	if _, err := db.SQL().ExecContext(ctx, "UPDATE capture_events SET state='pending',error=NULL WHERE seq=?", first); err != nil {
		t.Fatal(err)
	}
	const liveContents = "second captured\nlater user edit\n"
	if err := os.WriteFile(filepath.Join(repo, "second-recovery.txt"), []byte(liveContents), 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(repo, "staged-user.txt"), []byte("user staging\n"), 0644); err != nil {
		t.Fatal(err)
	}
	if _, err := git.Run(ctx, git.RunOpts{Dir: repo}, "add", "staged-user.txt"); err != nil {
		t.Fatal(err)
	}
	headBefore, err := git.RevParse(ctx, repo, "HEAD")
	if err != nil {
		t.Fatal(err)
	}
	indexBefore, err := fileSHA256(filepath.Join(repo, ".git", "index"))
	if err != nil {
		t.Fatal(err)
	}
	dbBefore, err := fileSHA256(stateDB)
	if err != nil {
		t.Fatal(err)
	}
	ordinary := runFixJSON(t, repo, true, false, false, false)
	if findFixAction(ordinary, fixActionReconcileUnpublishedChain) != nil {
		t.Fatalf("ordinary recovery selected healthy pending work: %+v", ordinary)
	}
	preview := runFixJSON(t, repo, true, false, true, false)
	action := findFixAction(preview, fixActionReconcileUnpublishedChain)
	if action == nil || !action.ArchiveOnly || !action.RequiresForce || action.PendingCount != 2 || action.Applied {
		t.Fatalf("force ignored the pending chain: %+v", preview)
	}
	if after, err := fileSHA256(stateDB); err != nil || after != dbBefore {
		t.Fatalf("preview changed state: %s/%s err=%v", dbBefore, after, err)
	}
	applied := runFixJSON(t, repo, false, true, true, false)
	action = findFixAction(applied, fixActionReconcileUnpublishedChain)
	if action == nil || !action.Applied || action.RowsChanged != 2 || action.RecoveryRef == "" || action.State != state.EventStateRecovered || applied.BackupPath == "" {
		t.Fatalf("pending chain was not preserved: %+v", applied)
	}
	for _, seq := range []int64{first, second} {
		assertFixEventState(t, ctx, db, seq, state.EventStateRecovered)
	}
	if rows := countRowsWhere(t, db, "capture_ops", "event_seq IN (?,?)", first, second); rows != 2 {
		t.Fatalf("recovery lost immutable operations: %d", rows)
	}
	archived, err := git.Run(ctx, git.RunOpts{Dir: repo}, "show", action.RecoveryRef+":second-recovery.txt")
	if err != nil || string(archived) != "second captured\n" {
		t.Fatalf("archive lost the captured version: %q err=%v", archived, err)
	}
	headAfter, err := git.RevParse(ctx, repo, "HEAD")
	if err != nil || headAfter != headBefore {
		t.Fatalf("recovery changed HEAD: %s/%s err=%v", headBefore, headAfter, err)
	}
	if after, err := fileSHA256(filepath.Join(repo, ".git", "index")); err != nil || after != indexBefore {
		t.Fatalf("recovery changed staging: %s/%s err=%v", indexBefore, after, err)
	}
	if contents, err := os.ReadFile(filepath.Join(repo, "second-recovery.txt")); err != nil || string(contents) != liveContents {
		t.Fatalf("recovery replaced later work: %q err=%v", contents, err)
	}
	assertLaterRecoveryCheckpoint(t, ctx, repo, db, headBefore, "second-recovery.txt", liveContents)
	if after, err := fileSHA256(filepath.Join(repo, ".git", "index")); err != nil || after != indexBefore {
		t.Fatalf("checkpoint changed staging: %s/%s err=%v", indexBefore, after, err)
	}
}

// A recovery archive preserves the old captured version; subsequent protection
// saves later live work without reopening the retired event queue.
func assertLaterRecoveryCheckpoint(t *testing.T, ctx context.Context, repo string, db *state.DB, head, path, contents string) {
	t.Helper()
	checker := git.NewIgnoreChecker(repo)
	defer checker.Close()
	store := checkpoint.Store{DB: db}
	_, err := daemon.ProtectWorktree(ctx, repo, db, daemon.CaptureContext{
		BranchRef: "refs/heads/main", BranchGeneration: 1, BaseHead: head,
	}, daemon.CaptureOpts{IgnoreChecker: checker, SensitiveMatcher: state.NewSensitiveMatcher(), CheckpointStore: &store})
	if err != nil {
		t.Fatal(err)
	}
	projection, err := state.ReadCheckpointProjection(ctx, db.Path(), 1)
	if err != nil || projection.Latest == nil {
		t.Fatalf("checkpoint missing: %+v err=%v", projection, err)
	}
	body, err := git.Run(ctx, git.RunOpts{Dir: repo}, "show", projection.Latest.CommitOID+":"+path)
	if err != nil || string(body) != contents {
		t.Fatalf("later work not protected: %q err=%v", body, err)
	}
	var pending int
	err = db.ReadSQL().QueryRowContext(ctx, `SELECT COUNT(*) FROM capture_events WHERE state=?`, state.EventStatePending).Scan(&pending)
	if err != nil || pending != 0 {
		t.Fatalf("checkpoint changed archived pending count: %d err=%v", pending, err)
	}
}
