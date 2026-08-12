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

func TestImportRecoveryCheckpointReusesExistingEventOwnership(t *testing.T) {
	ctx := context.Background()
	repo, wt := migrationRepo(t, ctx)
	db, err := state.Open(ctx, state.DBPathFromGitDir(wt.GitDir))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	tree, err := gitpkg.WriteTreeDurable(
		ctx, repo, filepath.Join(t.TempDir(), "initial.index"), nil)
	if err != nil {
		t.Fatal(err)
	}
	head, err := gitpkg.CommitTreeDurable(ctx, repo, tree, "initial\n",
		checkpoint.IdentityName, checkpoint.IdentityEmail)
	if err != nil {
		t.Fatal(err)
	}
	if err := gitpkg.UpdateRef(ctx, repo, "refs/heads/main", head, ""); err != nil {
		t.Fatal(err)
	}
	result, err := db.SQL().ExecContext(ctx, `
INSERT INTO capture_events(
 branch_ref,branch_generation,base_head,operation,path,fidelity,captured_ts,state
) VALUES('refs/heads/main',7,?,'modify','owned.txt','exact',1,'recovered')`, head)
	if err != nil {
		t.Fatal(err)
	}
	seq, err := result.LastInsertId()
	if err != nil {
		t.Fatal(err)
	}
	const digest = "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	existing := state.Checkpoint{
		ID:           "cp-1786489000000-0123456789abcdef",
		OperationID:  "op-cp-1786489000000-0123456789abcdef",
		WorktreeID:   central.CanonicalID(wt.Root),
		Reason:       state.CheckpointReasonMigrationRecovery,
		ObservedHead: head, ObservedRef: "refs/heads/main",
		TreeOID: tree, CommitOID: head,
		Ref: "refs/acd/checkpoints/v1/" + central.CanonicalID(wt.Root) +
			"/cp-1786489000000-0123456789abcdef",
		CreatedTS: 1, EventSeqs: []int64{seq},
	}
	if created, err := state.PrepareCheckpoint(ctx, db, existing, digest); err != nil || !created {
		t.Fatalf("prepare existing checkpoint=(%t,%v)", created, err)
	}
	if err := state.CompleteCheckpoint(
		ctx, db, existing.ID, existing.Ref, existing.CommitOID, 2); err != nil {
		t.Fatal(err)
	}
	snapshotResult, err := db.SQL().ExecContext(ctx, `
INSERT INTO recovery_snapshots(
 created_ts,outcome,branch_ref,branch_generation,first_event_seq,last_event_seq,
 event_count,commit_oid,recovery_ref,reason
) VALUES(3,'recovered','refs/heads/main',7,?,?,1,?,'refs/acd/recovery/already-owned','test')`,
		seq, seq, head)
	if err != nil {
		t.Fatal(err)
	}
	snapshotID, err := snapshotResult.LastInsertId()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.SQL().ExecContext(ctx, `
INSERT INTO recovery_snapshot_events(snapshot_id,ord,event_seq) VALUES(?,0,?)`,
		snapshotID, seq); err != nil {
		t.Fatal(err)
	}
	imported, err := importRecoveryCheckpoint(ctx, wt, db,
		central.CanonicalID(wt.Root), daemon.RecoveryChainResult{
			Handled: true, Outcome: state.EventStateRecovered,
			SnapshotID: snapshotID, CommitOID: head,
			RecoveryRef: "refs/acd/recovery/already-owned",
			FirstSeq:    seq, LastSeq: seq, EventCount: 1,
		})
	if err != nil {
		t.Fatal(err)
	}
	if len(imported.EventSeqs) != 0 {
		t.Fatalf("imported duplicate memberships=%v", imported.EventSeqs)
	}
	var owner string
	if err := db.ReadSQL().QueryRowContext(ctx,
		`SELECT checkpoint_id FROM checkpoint_events WHERE event_seq=?`, seq).
		Scan(&owner); err != nil || owner != existing.ID {
		t.Fatalf("event owner=(%q,%v) want %q", owner, err, existing.ID)
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

func TestApplyAllRejectsStaleMigrationPreview(t *testing.T) {
	ctx := context.Background()
	repo, wt := migrationRepo(t, ctx)
	record := central.RepoRecord{
		Path: wt.Root, StateDB: state.DBPathFromGitDir(wt.GitDir), CommonDir: wt.CommonDir,
		RepositoryID: central.CanonicalID(wt.CommonDir), WorktreeID: central.CanonicalID(wt.Root),
	}
	backup := filepath.Join(t.TempDir(), "state-v19.db")
	plan, err := Preflight(ctx, record, backup)
	if err != nil {
		t.Fatal(err)
	}
	db, err := state.Open(ctx, record.StateDB)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := ApplyAll(ctx, []RepositoryPlan{plan}); err == nil ||
		!strings.Contains(err.Error(), "state changed after preview") {
		t.Fatalf("ApplyAll stale preview error=%v", err)
	}
	if _, err := os.Stat(backup); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("stale migration wrote backup %s: %v", repo, err)
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

func TestMigrationRevalidationRejectsChangedRetainedBridgeRef(t *testing.T) {
	ctx := context.Background()
	repo, wt := migrationRepo(t, ctx)
	operationID := "setup-stale-bridge"
	treeOID, err := gitpkg.WriteTreeDurable(ctx, repo, filepath.Join(t.TempDir(), "bridge.index"), nil)
	if err != nil {
		t.Fatal(err)
	}
	commitOID, err := gitpkg.CommitTreeDurable(ctx, repo, treeOID,
		"acd migration bridge "+operationID+"\n", checkpoint.IdentityName, checkpoint.IdentityEmail)
	if err != nil {
		t.Fatal(err)
	}
	ref := migrationRefPrefix + operationID + "/" + central.CanonicalID(wt.Root) + "/1"
	if _, err := gitpkg.EnsurePrivateRefDurable(ctx, repo, migrationRefPrefix, ref, commitOID); err != nil {
		t.Fatal(err)
	}
	snapshot := BridgeSnapshot{RepositoryID: central.CanonicalID(wt.CommonDir), WorktreeID: central.CanonicalID(wt.Root),
		Repo: wt.Root, Ref: ref, CommitOID: commitOID, TreeOID: treeOID, CreatedTS: 1}
	plan := RepositoryPlan{Record: central.RepoRecord{
		Path: wt.Root, StateDB: state.DBPathFromGitDir(wt.GitDir), CommonDir: wt.CommonDir,
		RepositoryID: snapshot.RepositoryID, WorktreeID: snapshot.WorktreeID,
	}, BridgeSnapshots: []BridgeSnapshot{snapshot}, BackupPath: filepath.Join(t.TempDir(), "state-v19.db")}
	otherTree, err := gitpkg.WriteTreeDurable(ctx, repo, filepath.Join(t.TempDir(), "other.index"), nil)
	if err != nil {
		t.Fatal(err)
	}
	otherCommit, err := gitpkg.CommitTreeDurable(ctx, repo, otherTree,
		"changed retained bridge\n", checkpoint.IdentityName, checkpoint.IdentityEmail)
	if err != nil {
		t.Fatal(err)
	}
	if err := gitpkg.UpdateRef(ctx, repo, ref, otherCommit, commitOID); err != nil {
		t.Fatal(err)
	}
	if _, err := ApplyAll(ctx, []RepositoryPlan{plan}); err == nil ||
		!strings.Contains(err.Error(), "revalidate retained bridge") {
		t.Fatalf("ApplyAll changed bridge error=%v", err)
	}
	if _, err := os.Stat(plan.Record.StateDB); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("changed bridge mutated state DB: %v", err)
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
