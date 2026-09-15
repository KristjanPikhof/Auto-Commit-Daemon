package cli

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/daemon"
	gitpkg "github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

func TestFreshStagingReviewPreservesOldTargetAndAllowsRecapture(t *testing.T) {
	ctx := context.Background()
	repo, _, db := makeRegisteredGitRepoStateDB(t)
	head, err := gitpkg.RevParse(ctx, repo, "HEAD")
	if err != nil {
		t.Fatal(err)
	}
	path := "approved.txt"
	blob, err := gitpkg.HashObjectStdin(ctx, repo, []byte("original protected content\n"))
	if err != nil {
		t.Fatal(err)
	}
	seq := appendFixEvent(t, ctx, db, state.CaptureEvent{
		BranchRef: "refs/heads/main", BranchGeneration: 1, BaseHead: head,
		Operation: "create", Path: path, Fidelity: "exact", State: state.EventStatePending,
	}, []state.CaptureOp{{Op: "create", Path: path, Fidelity: "exact",
		AfterOID: sql.NullString{String: blob, Valid: true}, AfterMode: sql.NullString{String: gitpkg.RegularFileMode, Valid: true}}})
	insertCompletedCheckpoint(t, db, "cp-staging-review", "0123456789abcdef", nil)
	if _, err := db.SQL().ExecContext(ctx, "INSERT INTO checkpoint_events(checkpoint_id,ord,event_seq) VALUES('cp-staging-review',0,?)", seq); err != nil {
		t.Fatal(err)
	}
	if _, err := state.PreparePublicationDrain(ctx, db, state.PublicationDrain{
		ID: "staging-review", CheckpointID: "cp-staging-review", WorktreeID: "0123456789abcdef",
		BranchRef: "refs/heads/main", BranchGeneration: 1, Phase: state.PublicationDrainNeedsAction,
		TargetEventCount: 1, StagedConsent: true, ExpectedIndexDigest: strings.Repeat("a", 64),
		ReasonCode: "staging_changed", CreatedTS: 10, UpdatedTS: 10, LastProgressTS: 10,
	}); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(repo, path), []byte("new reviewed content\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := gitpkg.Run(ctx, gitpkg.RunOpts{Dir: repo}, "add", path); err != nil {
		t.Fatal(err)
	}
	indexBefore, err := gitpkg.IndexContentDigest(ctx, repo)
	if err != nil {
		t.Fatal(err)
	}
	worktree, err := gitpkg.ResolveWorktree(ctx, repo)
	if err != nil {
		t.Fatal(err)
	}
	runtime := &workerRuntime{worktree: worktree, db: db}
	if err := recoverCommitAllStagingReview(ctx, runtime, "refs/heads/main", 1); err != nil {
		t.Fatal(err)
	}
	drain, err := state.PublicationDrainByID(ctx, db, "staging-review")
	if err != nil || drain.Phase != state.PublicationDrainCompleted || drain.StagedConsumed {
		t.Fatalf("old target: %+v %v", drain, err)
	}
	var recoveredRef string
	if err := db.SQL().QueryRowContext(ctx, "SELECT recovery_ref FROM recovery_snapshots ORDER BY id DESC LIMIT 1").Scan(&recoveredRef); err != nil {
		t.Fatal(err)
	}
	content, err := gitpkg.Run(ctx, gitpkg.RunOpts{Dir: repo}, "show", recoveredRef+":"+path)
	if err != nil || string(content) != "original protected content\n" {
		t.Fatalf("recovery content=%q error=%v", content, err)
	}
	indexAfter, err := gitpkg.IndexContentDigest(ctx, repo)
	if err != nil || indexAfter != indexBefore {
		t.Fatalf("new staging changed: %s %v", indexAfter, err)
	}
	currentHead, err := gitpkg.RevParse(ctx, repo, "HEAD")
	if err != nil || currentHead != head {
		t.Fatalf("HEAD changed: %s %v", currentHead, err)
	}
	cctx := daemon.CaptureContext{BranchRef: "refs/heads/main", BranchGeneration: 1, BaseHead: head}
	if _, err := daemon.BootstrapShadow(ctx, repo, db, cctx); err != nil {
		t.Fatal(err)
	}
	checker := gitpkg.NewIgnoreChecker(repo)
	defer checker.Close()
	if _, err := daemon.Capture(ctx, repo, db, cctx, daemon.CaptureOpts{IgnoreChecker: checker, SensitiveMatcher: state.NewSensitiveMatcher()}); err != nil {
		t.Fatal(err)
	}
	var later int
	if err := db.SQL().QueryRowContext(ctx, "SELECT COUNT(*) FROM capture_events WHERE seq>? AND state='pending' AND path=?", seq, path).Scan(&later); err != nil || later != 1 {
		t.Fatalf("new target recapture count=%d error=%v", later, err)
	}
}
