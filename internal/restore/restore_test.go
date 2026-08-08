package restore

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/checkpoint"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/daemon"
	gitpkg "github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

func TestRestorePreviewAndApplyPreserveHeadAndIndex(t *testing.T) {
	ctx := context.Background()
	repo, gitDir, db, target := restoreFixture(t, ctx)
	if err := os.WriteFile(filepath.Join(repo, "file.txt"), []byte("new\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	headBefore, err := gitpkg.RevParse(ctx, repo, "HEAD")
	if err != nil {
		t.Fatal(err)
	}
	indexBefore, err := digestIndex(ctx, repo)
	if err != nil {
		t.Fatal(err)
	}

	plan, err := Preview(ctx, repo, gitDir, db.Path(), target.ID[:20])
	if err != nil {
		t.Fatal(err)
	}
	if !plan.CanApply || plan.Counts.Modified != 1 || plan.PlanDigest == "" {
		t.Fatalf("plan=%+v", plan)
	}
	result, err := Apply(ctx, db, plan)
	if err != nil {
		t.Fatal(err)
	}
	if result.UndoCheckpoint == "" || result.ResultCheckpoint == "" {
		t.Fatalf("result=%+v", result)
	}
	body, err := os.ReadFile(filepath.Join(repo, "file.txt"))
	if err != nil || string(body) != "old\n" {
		t.Fatalf("file=(%q,%v)", body, err)
	}
	if headAfter, _ := gitpkg.RevParse(ctx, repo, "HEAD"); headAfter != headBefore {
		t.Fatalf("HEAD moved from %s to %s", headBefore, headAfter)
	}
	if indexAfter, _ := digestIndex(ctx, repo); indexAfter != indexBefore {
		t.Fatalf("index changed from %s to %s", indexBefore, indexAfter)
	}
	if _, err := state.ResolveCheckpoint(ctx, db.Path(), result.UndoCheckpoint); err != nil {
		t.Fatalf("undo checkpoint: %v", err)
	}
	if _, err := state.ResolveCheckpoint(ctx, db.Path(), result.ResultCheckpoint); err != nil {
		t.Fatalf("result checkpoint: %v", err)
	}
}

func TestRestorePreviewRefusesStagedOverlap(t *testing.T) {
	ctx := context.Background()
	repo, gitDir, db, target := restoreFixture(t, ctx)
	if err := os.WriteFile(filepath.Join(repo, "file.txt"), []byte("staged\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := gitpkg.Run(ctx, gitpkg.RunOpts{Dir: repo}, "add", "file.txt"); err != nil {
		t.Fatal(err)
	}
	plan, err := Preview(ctx, repo, gitDir, db.Path(), target.ID)
	if err != nil {
		t.Fatal(err)
	}
	if plan.CanApply || plan.Counts.StagedOverlap != 1 || plan.Refusal == "" {
		t.Fatalf("plan=%+v", plan)
	}
}

func TestRestorePreviewDoesNotWriteGitObjects(t *testing.T) {
	ctx := context.Background()
	repo, gitDir, db, target := restoreFixture(t, ctx)
	if err := os.WriteFile(filepath.Join(repo, "untracked.txt"), []byte("preview-only-unique\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	before, err := gitpkg.Run(ctx, gitpkg.RunOpts{Dir: repo}, "count-objects", "-v")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Preview(ctx, repo, gitDir, db.Path(), target.ID); err != nil {
		t.Fatal(err)
	}
	after, err := gitpkg.Run(ctx, gitpkg.RunOpts{Dir: repo}, "count-objects", "-v")
	if err != nil {
		t.Fatal(err)
	}
	if string(after) != string(before) {
		t.Fatalf("preview changed object store:\nbefore=%s\nafter=%s", before, after)
	}
}

func TestRepairCompletesInterruptedPostRestoreCheckpoint(t *testing.T) {
	ctx := context.Background()
	repo, _, db, target := restoreFixture(t, ctx)
	if err := os.WriteFile(filepath.Join(repo, "file.txt"), []byte("new\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	store := checkpoint.Store{DB: db}
	checker := gitpkg.NewIgnoreChecker(repo)
	preEntries, _, _, err := daemon.ScanProtectedEntries(ctx, repo,
		daemon.CaptureOpts{IgnoreChecker: checker, CheckpointStore: &store})
	_ = checker.Close()
	if err != nil {
		t.Fatal(err)
	}
	head, _ := gitpkg.RevParse(ctx, repo, "HEAD")
	pre, err := store.Create(ctx, checkpoint.Request{RepoRoot: repo,
		WorktreeID: target.WorktreeID, Reason: state.CheckpointReasonPreRestore,
		ObservationEpoch: 2, CoverageEpoch: 2, ObservedHead: head,
		ObservedRef: "refs/heads/main", Entries: preEntries})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(repo, "file.txt"), []byte("old\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	operationID := "restore-interrupted"
	digest := "sha256:" + strings.Repeat("a", 64)
	if err := state.PrepareOperation(ctx, db, state.Operation{ID: operationID,
		Kind: "restore", WorktreeID: target.WorktreeID, PlanDigest: digest}, nil); err != nil {
		t.Fatal(err)
	}
	if err := state.PrepareRestoreOperation(ctx, db, operationID, target.ID, digest); err != nil {
		t.Fatal(err)
	}
	if err := state.AdvanceRestoreOperation(ctx, db, operationID, pre.Checkpoint.ID, "", state.OperationApplying); err != nil {
		t.Fatal(err)
	}
	if err := state.AdvanceOperation(ctx, db, operationID, state.OperationNeedsAttention,
		state.OperationNeedsAttention, "post-restore checkpoint failed"); err != nil {
		t.Fatal(err)
	}
	beforeOID, _ := gitpkg.HashObjectStdin(ctx, repo, []byte("new\n"))
	afterOID, _ := gitpkg.HashObjectStdin(ctx, repo, []byte("old\n"))
	seq, err := state.AppendCaptureEvent(ctx, db, state.CaptureEvent{BranchRef: "refs/heads/main",
		BranchGeneration: 1, BaseHead: head, Operation: "modify", Path: "file.txt",
		Fidelity: "exact", CapturedTS: float64(time.Now().UnixNano()) / float64(time.Second),
		State: state.EventStatePending}, []state.CaptureOp{{Ord: 1, Op: "modify", Path: "file.txt",
		BeforeOID: sql.NullString{String: beforeOID, Valid: true}, BeforeMode: sql.NullString{String: gitpkg.RegularFileMode, Valid: true},
		AfterOID: sql.NullString{String: afterOID, Valid: true}, AfterMode: sql.NullString{String: gitpkg.RegularFileMode, Valid: true}, Fidelity: "exact"}})
	if err != nil {
		t.Fatal(err)
	}
	plan, err := PreviewRepair(ctx, db)
	if err != nil || !plan.CanRepair {
		t.Fatalf("repair plan=%+v err=%v", plan, err)
	}
	result, err := Repair(ctx, repo, db, plan)
	if err != nil {
		t.Fatal(err)
	}
	if result.UndoCheckpoint != pre.Checkpoint.ID || result.ResultCheckpoint == "" {
		t.Fatalf("result=%+v", result)
	}
	orphans, err := state.UncheckpointedEventSeqsSince(ctx, db, 0)
	if err != nil {
		t.Fatal(err)
	}
	for _, orphan := range orphans {
		if orphan == seq {
			t.Fatalf("restore event %d remained outside a completed checkpoint", seq)
		}
	}
}

func restoreFixture(t *testing.T, ctx context.Context) (string, string, *state.DB, state.Checkpoint) {
	t.Helper()
	repo := t.TempDir()
	if err := gitpkg.Init(ctx, repo); err != nil {
		t.Fatal(err)
	}
	if _, err := gitpkg.Run(ctx, gitpkg.RunOpts{Dir: repo}, "symbolic-ref", "HEAD", "refs/heads/main"); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(repo, "file.txt"), []byte("old\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := gitpkg.Run(ctx, gitpkg.RunOpts{Dir: repo}, "add", "file.txt"); err != nil {
		t.Fatal(err)
	}
	if _, err := gitpkg.Run(ctx, gitpkg.RunOpts{Dir: repo}, "-c", "user.name=Test", "-c", "user.email=test@example.com", "commit", "-m", "seed"); err != nil {
		t.Fatal(err)
	}
	wt, err := gitpkg.ResolveWorktree(ctx, repo)
	if err != nil {
		t.Fatal(err)
	}
	db, err := state.Open(ctx, state.DBPathFromGitDir(wt.GitDir))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	store := checkpoint.Store{DB: db}
	checker := gitpkg.NewIgnoreChecker(repo)
	entries, _, _, err := daemon.ScanProtectedEntries(ctx, repo, daemon.CaptureOpts{IgnoreChecker: checker, CheckpointStore: &store})
	_ = checker.Close()
	if err != nil {
		t.Fatal(err)
	}
	head, err := gitpkg.RevParse(ctx, repo, "HEAD")
	if err != nil {
		t.Fatal(err)
	}
	created, err := store.Create(ctx, checkpoint.Request{
		RepoRoot: repo, WorktreeID: checkpoint.WorktreeID(repo), Reason: state.CheckpointReasonPoll,
		ObservationEpoch: 1, CoverageEpoch: 1, ObservedHead: head, ObservedRef: "refs/heads/main", Entries: entries,
	})
	if err != nil {
		t.Fatal(err)
	}
	return repo, wt.GitDir, db, created.Checkpoint
}
