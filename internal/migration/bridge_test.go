package migration

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/central"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/checkpoint"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/daemon"
	gitpkg "github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

func TestBridgeClearsTransientScanFailureAfterCompleteRescan(t *testing.T) {
	ctx := context.Background()
	repo := t.TempDir()
	if err := gitpkg.Init(ctx, repo); err != nil {
		t.Fatal(err)
	}
	if _, err := gitpkg.Run(ctx, gitpkg.RunOpts{Dir: repo}, "symbolic-ref", "HEAD", "refs/heads/main"); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(repo, "file.txt"), []byte("protected\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	wt, err := gitpkg.ResolveWorktree(ctx, repo)
	if err != nil {
		t.Fatal(err)
	}
	record := central.RepoRecord{Path: wt.Root, CommonDir: wt.CommonDir,
		RepositoryID: central.CanonicalID(wt.CommonDir), WorktreeID: central.CanonicalID(wt.Root)}
	original := scanBridgeEntries
	var calls atomic.Int32
	scanBridgeEntries = func(ctx context.Context, repo string, opts daemon.CaptureOpts) ([]checkpoint.Entry, []state.CheckpointExclusion, daemon.CaptureSummary, error) {
		call := calls.Add(1)
		if call == 2 {
			return nil, nil, daemon.CaptureSummary{Errors: 1}, errors.New("transient unstable path")
		}
		return original(ctx, repo, opts)
	}
	t.Cleanup(func() { scanBridgeEntries = original })
	var progress []string
	bridge, err := StartBridgeWithProgress(ctx, "setup-test", []central.RepoRecord{record}, 10*time.Millisecond,
		func(completed, total int, repo string) {
			progress = append(progress, fmt.Sprintf("%d/%d:%s", completed, total, repo))
		})
	if err != nil {
		t.Fatal(err)
	}
	if len(progress) != 1 || progress[0] != "1/1:"+record.Path {
		t.Fatalf("initial scan progress=%v", progress)
	}
	if bridge.CreatedCount() != 1 {
		t.Fatalf("created bridge checkpoints=%d want 1", bridge.CreatedCount())
	}
	defer bridge.Stop()
	deadline := time.Now().Add(2 * time.Second)
	for calls.Load() < 3 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if _, err := bridge.Finalize(ctx); err != nil {
		t.Fatalf("final complete rescan did not clear transient failure: %v", err)
	}
}

func TestCleanupCreatedRefsUsesExpectedTargetCAS(t *testing.T) {
	ctx := context.Background()
	repo := t.TempDir()
	if err := gitpkg.Init(ctx, repo); err != nil {
		t.Fatal(err)
	}
	if _, err := gitpkg.Run(ctx, gitpkg.RunOpts{Dir: repo}, "symbolic-ref", "HEAD", "refs/heads/main"); err != nil {
		t.Fatal(err)
	}
	tree, err := gitpkg.WriteTreeDurable(ctx, repo, filepath.Join(t.TempDir(), "index"), nil)
	if err != nil {
		t.Fatal(err)
	}
	commit, err := gitpkg.CommitTreeDurable(ctx, repo, tree, "migration", checkpoint.IdentityName, checkpoint.IdentityEmail)
	if err != nil {
		t.Fatal(err)
	}
	worktreeID := central.CanonicalID(repo)
	ref := gitpkg.CheckpointRefPrefix + worktreeID + "/cp-1786061000000-0123456789abcdef"
	if _, err := gitpkg.EnsureCheckpointRef(ctx, repo, ref, commit); err != nil {
		t.Fatal(err)
	}
	plan := RepositoryPlan{BackupPath: filepath.Join(t.TempDir(), "state-v19.db")}
	if err := appendCreatedRef(plan, RefProof{Repo: repo, Ref: ref, CommitOID: commit}); err != nil {
		t.Fatal(err)
	}
	if err := cleanupCreatedRefs(ctx, []RepositoryPlan{plan}); err != nil {
		t.Fatal(err)
	}
	if _, err := gitpkg.RevParse(ctx, repo, ref); err == nil {
		t.Fatalf("created migration ref %s was retained", ref)
	}
}
