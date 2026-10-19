package restore

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/checkpoint"
	gitpkg "github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/migration"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

func TestFinishRetentionRestoreFailedTargetAndUndo(t *testing.T) {
	ctx := context.Background()
	repo, gitDir, db, target := restoreFixture(t, ctx)
	now := time.Now()
	if _, err := migration.ActivateFinish(ctx, repo, db); err != nil {
		t.Fatal(err)
	}
	r := state.FinishRequest{ID: "failed-finish", DeadlineTS: float64(now.Add(time.Minute).Unix()), Target: state.FinishTarget{Version: 1, RepositoryID: "repository", WorktreeID: target.WorktreeID, BranchRef: target.ObservedRef, ExpectedHead: target.ObservedHead, CheckpointID: target.ID, CheckpointTree: target.TreeOID, TargetTree: target.TreeOID, IndexDigest: "frozen-index", Mode: "local", Style: "imperative", ConfigDigest: "config"}}
	r, _, err := state.StartFinishRequest(ctx, db, r)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = state.UpdateFinishRequest(ctx, db, r.ID, r.Attempt, state.FinishRunning, state.FinishUpdate{State: state.FinishFailed, ReasonCode: "verification_failed"}); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(repo, "file.txt"), []byte("later work\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(repo, ".env"), []byte("user staged\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err = gitpkg.Run(ctx, gitpkg.RunOpts{Dir: repo}, "add", ".env"); err != nil {
		t.Fatal(err)
	}
	index, err := digestIndex(ctx, repo)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := Preview(ctx, repo, gitDir, db.Path(), target.ID)
	if err != nil || !plan.CanApply {
		t.Fatalf("preview %+v %v", plan, err)
	}
	result, err := Apply(ctx, db, plan)
	if err != nil {
		t.Fatal(err)
	}
	if result.UndoCheckpoint == "" {
		t.Fatal("restore omitted undo checkpoint")
	}
	store := checkpoint.Store{DB: db}
	if _, err = store.ApplyRetention(ctx, repo, target.WorktreeID, now.Add(90*24*time.Hour)); err != nil {
		t.Fatal(err)
	}
	if _, err = gitpkg.Run(ctx, gitpkg.RunOpts{Dir: repo}, "gc", "--prune=now"); err != nil {
		t.Fatal(err)
	}
	undo, err := Preview(ctx, repo, gitDir, db.Path(), result.UndoCheckpoint)
	if err != nil || !undo.CanApply {
		t.Fatalf("undo %+v %v", undo, err)
	}
	if _, err = Apply(ctx, db, undo); err != nil {
		t.Fatal(err)
	}
	body, err := os.ReadFile(filepath.Join(repo, "file.txt"))
	if err != nil || string(body) != "later work\n" {
		t.Fatalf("undo bytes %q %v", body, err)
	}
	after, err := digestIndex(ctx, repo)
	if err != nil || after != index {
		t.Fatal("restore or undo changed user staging")
	}
	head, err := gitpkg.RevParse(ctx, repo, "HEAD")
	if err != nil || head != target.ObservedHead {
		t.Fatal("restore or undo published a branch commit")
	}
	saved, err := state.LoadFinishRequest(ctx, db, r.ID)
	if err != nil || saved.State != state.FinishFailed || saved.Attempt != 1 {
		t.Fatalf("restore automatically retried finish %+v %v", saved, err)
	}
}
