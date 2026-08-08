package migration

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/central"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/checkpoint"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/daemon"
	gitpkg "github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

func TestScanProtectedEntriesForMigrationRetriesTransientIncompleteScan(t *testing.T) {
	ctx := context.Background()
	repo, _ := migrationRepo(t, ctx)
	original := scanMigrationEntries
	var calls atomic.Int32
	scanMigrationEntries = func(context.Context, string, daemon.CaptureOpts) ([]checkpoint.Entry, []state.CheckpointExclusion, daemon.CaptureSummary, error) {
		if calls.Add(1) < 3 {
			return nil, nil, daemon.CaptureSummary{Oversize: 6}, errors.New("transient unstable paths")
		}
		return []checkpoint.Entry{}, nil, daemon.CaptureSummary{}, nil
	}
	t.Cleanup(func() { scanMigrationEntries = original })

	entries, _, err := scanProtectedEntriesForMigration(ctx, repo, &checkpoint.Store{})
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 || calls.Load() != 3 {
		t.Fatalf("entries=%v calls=%d", entries, calls.Load())
	}
}

func TestApplyAllProgressAndErrorIdentifyRepository(t *testing.T) {
	ctx := context.Background()
	repo, wt := migrationRepo(t, ctx)
	original := scanMigrationEntries
	scanMigrationEntries = func(context.Context, string, daemon.CaptureOpts) ([]checkpoint.Entry, []state.CheckpointExclusion, daemon.CaptureSummary, error) {
		return nil, nil, daemon.CaptureSummary{Oversize: 1}, errors.New("persistent unstable path")
	}
	t.Cleanup(func() { scanMigrationEntries = original })
	plan := RepositoryPlan{Record: central.RepoRecord{
		Path: wt.Root, StateDB: state.DBPathFromGitDir(wt.GitDir), CommonDir: wt.CommonDir,
		RepositoryID: central.CanonicalID(wt.CommonDir), WorktreeID: central.CanonicalID(wt.Root),
	}, BackupPath: filepath.Join(t.TempDir(), "state-v19.db")}
	var updates []Progress
	_, err := ApplyAllWithProgress(ctx, []RepositoryPlan{plan}, func(update Progress) { updates = append(updates, update) })
	if err == nil || !strings.Contains(err.Error(), repo) || !strings.Contains(err.Error(), "after 3 attempts") {
		t.Fatalf("migration error=%v", err)
	}
	if len(updates) != 1 || updates[0].Repo != wt.Root || updates[0].Completed != 1 || updates[0].Total != 1 {
		t.Fatalf("progress=%+v", updates)
	}
	if _, statErr := os.Stat(plan.Record.StateDB); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("failed fresh migration left state database behind: %v", statErr)
	}
}

func TestReplaceFileRemovesStaleSQLiteSidecars(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	source := filepath.Join(dir, "backup.db")
	target := filepath.Join(dir, "state.db")

	db, err := state.Open(ctx, source)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	for _, suffix := range []string{"-wal", "-shm", "-journal"} {
		if err := os.WriteFile(target+suffix, []byte("stale v20 sidecar"), 0o600); err != nil {
			t.Fatal(err)
		}
	}

	if err := replaceFile(source, target); err != nil {
		t.Fatal(err)
	}
	if err := state.QuickCheck(ctx, target); err != nil {
		t.Fatal(err)
	}
	for _, suffix := range []string{"-wal", "-shm", "-journal"} {
		body, err := os.ReadFile(target + suffix)
		if err == nil && string(body) == "stale v20 sidecar" {
			t.Fatalf("stale sidecar %s remains", suffix)
		}
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			t.Fatal(err)
		}
	}
}

func TestRetainedBridgeCheckpointImportsBeforeManifestCleanup(t *testing.T) {
	ctx := context.Background()
	repo, wt := migrationRepo(t, ctx)
	operationID := "setup-retained-test"
	treeOID, err := gitpkg.WriteTreeDurable(ctx, repo, filepath.Join(t.TempDir(), "bridge.index"), nil)
	if err != nil {
		t.Fatal(err)
	}
	commitOID, err := gitpkg.CommitTreeDurable(ctx, repo, treeOID,
		"acd migration bridge "+operationID+"\n", checkpoint.IdentityName, checkpoint.IdentityEmail)
	if err != nil {
		t.Fatal(err)
	}
	worktreeID := central.CanonicalID(wt.Root)
	ref := migrationRefPrefix + operationID + "/" + worktreeID + "/1"
	if _, err := gitpkg.EnsurePrivateRefDurable(ctx, repo, migrationRefPrefix, ref, commitOID); err != nil {
		t.Fatal(err)
	}
	snapshot := BridgeSnapshot{RepositoryID: central.CanonicalID(wt.CommonDir), WorktreeID: worktreeID,
		Repo: wt.Root, Ref: ref, CommitOID: commitOID, TreeOID: treeOID, CreatedTS: float64(time.Now().UnixNano()) / float64(time.Second)}
	setupRoot := t.TempDir()
	manifestPath := filepath.Join(setupRoot, operationID, bridgeRecoveryManifestName)
	if err := os.MkdirAll(filepath.Dir(manifestPath), 0o700); err != nil {
		t.Fatal(err)
	}
	body, err := json.Marshal(bridgeRecoveryManifest{OperationID: operationID, Retained: []BridgeSnapshot{snapshot}})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(manifestPath, body, 0o600); err != nil {
		t.Fatal(err)
	}
	plan := RepositoryPlan{Record: central.RepoRecord{
		Path: wt.Root, StateDB: state.DBPathFromGitDir(wt.GitDir), CommonDir: wt.CommonDir,
		RepositoryID: snapshot.RepositoryID, WorktreeID: worktreeID,
	}, BackupPath: filepath.Join(t.TempDir(), "state-v19.db")}
	plans, manifests, err := AttachBridgeRecoveries(ctx, setupRoot, []RepositoryPlan{plan})
	if err != nil {
		t.Fatal(err)
	}
	if len(plans[0].BridgeSnapshots) != 1 || len(manifests) != 1 {
		t.Fatalf("plans=%+v manifests=%v", plans, manifests)
	}
	results, err := ApplyAll(ctx, plans)
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 || len(results[0].BridgeCheckpoints) != 1 {
		t.Fatalf("results=%+v", results)
	}
	projection, err := state.ReadCheckpointProjection(ctx, plan.Record.StateDB, 10)
	if err != nil {
		t.Fatal(err)
	}
	if projection.Completed != 2 {
		t.Fatalf("completed checkpoints=%d want 2", projection.Completed)
	}
	if _, err := gitpkg.RevParse(ctx, repo, ref); err != nil {
		t.Fatalf("old recovery ref was removed before global commit: %v", err)
	}
	if err := CleanupBridgeRecoveryManifests(ctx, manifests); err != nil {
		t.Fatal(err)
	}
	if _, err := gitpkg.RevParse(ctx, repo, ref); !errors.Is(err, gitpkg.ErrRefNotFound) {
		t.Fatalf("old recovery ref cleanup=%v", err)
	}
	if _, err := os.Stat(manifestPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("manifest cleanup=%v", err)
	}
}

func TestAttachBridgeRecoveriesRejectsRepositoryIdentityMismatch(t *testing.T) {
	ctx := context.Background()
	repo, wt := migrationRepo(t, ctx)
	operationID := "setup-identity-mismatch"
	treeOID, err := gitpkg.WriteTreeDurable(ctx, repo, filepath.Join(t.TempDir(), "bridge.index"), nil)
	if err != nil {
		t.Fatal(err)
	}
	commitOID, err := gitpkg.CommitTreeDurable(ctx, repo, treeOID,
		"acd migration bridge "+operationID+"\n", checkpoint.IdentityName, checkpoint.IdentityEmail)
	if err != nil {
		t.Fatal(err)
	}
	worktreeID := central.CanonicalID(wt.Root)
	ref := migrationRefPrefix + operationID + "/" + worktreeID + "/1"
	if _, err := gitpkg.EnsurePrivateRefDurable(ctx, repo, migrationRefPrefix, ref, commitOID); err != nil {
		t.Fatal(err)
	}
	snapshot := BridgeSnapshot{RepositoryID: "wrong-repository", WorktreeID: worktreeID,
		Repo: wt.Root, Ref: ref, CommitOID: commitOID, TreeOID: treeOID, CreatedTS: 1}
	setupRoot := t.TempDir()
	manifestPath := filepath.Join(setupRoot, operationID, bridgeRecoveryManifestName)
	if err := os.MkdirAll(filepath.Dir(manifestPath), 0o700); err != nil {
		t.Fatal(err)
	}
	body, err := json.Marshal(bridgeRecoveryManifest{OperationID: operationID, Retained: []BridgeSnapshot{snapshot}})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(manifestPath, body, 0o600); err != nil {
		t.Fatal(err)
	}
	plan := RepositoryPlan{Record: central.RepoRecord{
		Path: wt.Root, StateDB: state.DBPathFromGitDir(wt.GitDir), CommonDir: wt.CommonDir,
		RepositoryID: central.CanonicalID(wt.CommonDir), WorktreeID: worktreeID,
	}}
	if _, _, err := AttachBridgeRecoveries(ctx, setupRoot, []RepositoryPlan{plan}); err == nil ||
		!strings.Contains(err.Error(), "unavailable or changed") {
		t.Fatalf("identity mismatch error=%v", err)
	}
}

func migrationRepo(t *testing.T, ctx context.Context) (string, gitpkg.Worktree) {
	t.Helper()
	repo := t.TempDir()
	if err := gitpkg.Init(ctx, repo); err != nil {
		t.Fatal(err)
	}
	if _, err := gitpkg.Run(ctx, gitpkg.RunOpts{Dir: repo}, "symbolic-ref", "HEAD", "refs/heads/main"); err != nil {
		t.Fatal(err)
	}
	wt, err := gitpkg.ResolveWorktree(ctx, repo)
	if err != nil {
		t.Fatal(err)
	}
	return repo, wt
}
