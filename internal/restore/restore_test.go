package restore

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

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
	headAtPreview, err := gitpkg.RevParse(ctx, repo, "HEAD")
	if err != nil {
		t.Fatal(err)
	}
	if err := daemon.SaveBranchGeneration(ctx, db, 7, headAtPreview); err != nil {
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
	if !plan.CanApply || plan.Counts.Modified != 1 || plan.PlanDigest == "" || plan.BranchGeneration != 7 {
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
	var generation int64
	if err := db.ReadSQL().QueryRowContext(ctx,
		"SELECT branch_generation FROM capture_events ORDER BY seq DESC LIMIT 1").Scan(&generation); err != nil {
		t.Fatal(err)
	}
	if generation != 7 {
		t.Fatalf("restore event generation=%d want 7", generation)
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
	repo, gitDir, db, target := restoreFixture(t, ctx)
	if err := os.WriteFile(filepath.Join(repo, "file.txt"), []byte("new\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	head, err := gitpkg.RevParse(ctx, repo, "HEAD")
	if err != nil {
		t.Fatal(err)
	}
	if err := daemon.SaveBranchGeneration(ctx, db, 7, head); err != nil {
		t.Fatal(err)
	}
	restorePlan, err := Preview(ctx, repo, gitDir, db.Path(), target.ID)
	if err != nil {
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
	pre, err := store.Create(ctx, checkpoint.Request{RepoRoot: repo,
		WorktreeID: target.WorktreeID, Reason: state.CheckpointReasonPreRestore,
		ObservationEpoch: 2, CoverageEpoch: 2, ObservedHead: head,
		ObservedRef: "refs/heads/main", Entries: preEntries})
	if err != nil {
		t.Fatal(err)
	}
	operationID := "restore-interrupted"
	steps := make([]state.OperationStep, 0, len(restorePlan.changes))
	for i, item := range restorePlan.changes {
		steps = append(steps, state.OperationStep{Sequence: i + 1, Kind: "restore_path", Target: item.Path,
			BeforeDigest: entryDigest(item.Before), AfterDigest: entryDigest(item.After)})
	}
	if err := state.PrepareOperation(ctx, db, state.Operation{ID: operationID,
		Kind: "restore", WorktreeID: target.WorktreeID, PlanDigest: restorePlan.PlanDigest}, steps); err != nil {
		t.Fatal(err)
	}
	if err := state.PrepareRestoreOperation(ctx, db, operationID, target.ID, restorePlan.PlanDigest); err != nil {
		t.Fatal(err)
	}
	backupDir := filepath.Join(gitDir, "acd", "restore", operationID)
	if _, err := createBackups(ctx, repo, backupDir, restorePlan); err != nil {
		t.Fatal(err)
	}
	if err := state.BeginRestoreApply(ctx, db, operationID, pre.Checkpoint.ID); err != nil {
		t.Fatal(err)
	}
	if err := applyChanges(ctx, repo, restorePlan.changes); err != nil {
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
	var generation int64
	if err := db.ReadSQL().QueryRowContext(ctx, `
SELECT e.branch_generation
FROM checkpoint_events ce JOIN capture_events e ON e.seq=ce.event_seq
WHERE ce.checkpoint_id=?`, result.ResultCheckpoint).Scan(&generation); err != nil {
		t.Fatal(err)
	}
	if generation != 7 {
		t.Fatalf("repair event generation=%d want 7", generation)
	}
}

func TestRestorePlanRecordsGenerationAndDetachedResult(t *testing.T) {
	ctx := context.Background()
	repo, gitDir, db, target := restoreFixture(t, ctx)
	head, err := gitpkg.RevParse(ctx, repo, "HEAD")
	if err != nil {
		t.Fatal(err)
	}
	if err := daemon.SaveBranchGeneration(ctx, db, 7, head); err != nil {
		t.Fatal(err)
	}
	if _, err := gitpkg.Run(ctx, gitpkg.RunOpts{Dir: repo}, "checkout", "--detach", "HEAD"); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(repo, "file.txt"), []byte("new\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	plan, err := Preview(ctx, repo, gitDir, db.Path(), target.ID)
	if err != nil {
		t.Fatal(err)
	}
	if plan.BranchGeneration != 7 || !plan.Detached || !plan.CanApply {
		t.Fatalf("plan=%+v", plan)
	}
	result, err := Apply(ctx, db, plan)
	if err != nil {
		t.Fatal(err)
	}
	created, err := state.ResolveCheckpoint(ctx, db.Path(), result.ResultCheckpoint)
	if err != nil {
		t.Fatal(err)
	}
	if created.ObservedRef != "" {
		t.Fatalf("detached result observed ref = %q", created.ObservedRef)
	}
	var events int
	if err := db.ReadSQL().QueryRowContext(ctx, "SELECT COUNT(*) FROM capture_events").Scan(&events); err != nil {
		t.Fatal(err)
	}
	if events != 0 {
		t.Fatalf("detached restore created %d publication events", events)
	}
}

func TestRestorePreviewRefusesCurrentExclusions(t *testing.T) {
	for _, test := range []struct {
		name string
		set  func(*testing.T, string)
	}{
		{name: "sensitive", set: func(t *testing.T, _ string) { t.Setenv(state.EnvSensitiveGlobs, "file.txt") }},
		{name: "safe-ignore", set: func(t *testing.T, _ string) { t.Setenv(state.EnvSafeIgnoreExtra, "file.txt") }},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			repo, gitDir, db, target := restoreFixture(t, ctx)
			test.set(t, gitDir)
			plan, err := Preview(ctx, repo, gitDir, db.Path(), target.ID)
			if err != nil {
				t.Fatal(err)
			}
			if plan.CanApply || !strings.Contains(plan.Refusal, "excluded") {
				t.Fatalf("plan=%+v", plan)
			}
		})
	}
}

func TestRestorePreviewUsesResolvedProtectionPolicy(t *testing.T) {
	for _, test := range []struct {
		name   string
		policy ProtectionPolicy
	}{
		{name: "sensitive", policy: ProtectionPolicy{
			Sensitive:  state.NewSensitiveMatcherFromValue("file.txt"),
			SafeIgnore: state.NewSafeIgnoreMatcherFromValues("false", ""),
		}},
		{name: "safe-ignore", policy: ProtectionPolicy{
			Sensitive:  state.NewSensitiveMatcherFromValue(""),
			SafeIgnore: state.NewSafeIgnoreMatcherFromValues("true", "file.txt"),
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			repo, gitDir, db, target := restoreFixture(t, ctx)
			plan, err := PreviewWithPolicy(ctx, repo, gitDir, db.Path(), target.ID, test.policy)
			if err != nil {
				t.Fatal(err)
			}
			if plan.CanApply || !strings.Contains(plan.Refusal, "excluded") {
				t.Fatalf("plan=%+v", plan)
			}
		})
	}
}

func TestRestorePreviewRefusesSensitiveDirectoryAncestor(t *testing.T) {
	ctx := context.Background()
	repo, gitDir, db, _ := restoreFixture(t, ctx)
	if err := os.MkdirAll(filepath.Join(repo, "secret"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(repo, "secret", "data.txt"), []byte("private\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	store := checkpoint.Store{DB: db}
	checker := gitpkg.NewIgnoreChecker(repo)
	entries, _, _, err := daemon.ScanProtectedEntries(ctx, repo,
		daemon.CaptureOpts{IgnoreChecker: checker, CheckpointStore: &store})
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
		ObservationEpoch: 2, CoverageEpoch: 2, ObservedHead: head,
		ObservedRef: "refs/heads/main", Entries: entries,
	})
	if err != nil {
		t.Fatal(err)
	}
	policy := ProtectionPolicy{
		Sensitive:  state.NewSensitiveMatcherFromValue("secret"),
		SafeIgnore: state.NewSafeIgnoreMatcherFromValues("false", ""),
	}
	plan, err := PreviewWithPolicy(ctx, repo, gitDir, db.Path(), created.Checkpoint.ID, policy)
	if err != nil {
		t.Fatal(err)
	}
	if plan.CanApply || !strings.Contains(plan.Refusal, "excluded") {
		t.Fatalf("plan=%+v", plan)
	}
}

func TestRestorePreviewRefusesNewlyGitIgnoredTarget(t *testing.T) {
	ctx := context.Background()
	repo, gitDir, db, _ := restoreFixture(t, ctx)
	if err := os.WriteFile(filepath.Join(repo, "historical.txt"), []byte("historical\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	store := checkpoint.Store{DB: db}
	checker := gitpkg.NewIgnoreChecker(repo)
	entries, _, _, err := daemon.ScanProtectedEntries(ctx, repo,
		daemon.CaptureOpts{IgnoreChecker: checker, CheckpointStore: &store})
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
		ObservationEpoch: 2, CoverageEpoch: 2, ObservedHead: head,
		ObservedRef: "refs/heads/main", Entries: entries,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(gitDir, "info", "exclude"), []byte("historical.txt\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	plan, err := Preview(ctx, repo, gitDir, db.Path(), created.Checkpoint.ID)
	if err != nil {
		t.Fatal(err)
	}
	if plan.CanApply || !strings.Contains(plan.Refusal, "excluded") {
		t.Fatalf("plan=%+v", plan)
	}
}

func TestRestoreApplyChangesHandlesPathTransitions(t *testing.T) {
	ctx := context.Background()
	for _, test := range []struct {
		name       string
		prepare    func(*testing.T, string)
		changes    func(string, string) []change
		assertPath func(*testing.T, string)
	}{
		{
			name: "directory-to-file",
			prepare: func(t *testing.T, repo string) {
				if err := os.MkdirAll(filepath.Join(repo, "node"), 0o755); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(repo, "node", "child"), []byte("child\n"), 0o644); err != nil {
					t.Fatal(err)
				}
			},
			changes: func(fileOID, childOID string) []change {
				return []change{
					{Path: "node", After: &checkpoint.Entry{Path: "node", Mode: gitpkg.RegularFileMode, OID: fileOID}},
					{Path: "node/child", Before: &checkpoint.Entry{Path: "node/child", Mode: gitpkg.RegularFileMode, OID: childOID}},
				}
			},
			assertPath: func(t *testing.T, repo string) {
				body, err := os.ReadFile(filepath.Join(repo, "node"))
				if err != nil || string(body) != "file\n" {
					t.Fatalf("node=(%q,%v)", body, err)
				}
			},
		},
		{
			name: "file-to-directory",
			prepare: func(t *testing.T, repo string) {
				if err := os.WriteFile(filepath.Join(repo, "node"), []byte("file\n"), 0o644); err != nil {
					t.Fatal(err)
				}
			},
			changes: func(fileOID, childOID string) []change {
				return []change{
					{Path: "node", Before: &checkpoint.Entry{Path: "node", Mode: gitpkg.RegularFileMode, OID: fileOID}},
					{Path: "node/child", After: &checkpoint.Entry{Path: "node/child", Mode: gitpkg.RegularFileMode, OID: childOID}},
				}
			},
			assertPath: func(t *testing.T, repo string) {
				body, err := os.ReadFile(filepath.Join(repo, "node", "child"))
				if err != nil || string(body) != "child\n" {
					t.Fatalf("child=(%q,%v)", body, err)
				}
			},
		},
		{
			name: "directory-to-symlink",
			prepare: func(t *testing.T, repo string) {
				if err := os.MkdirAll(filepath.Join(repo, "node"), 0o755); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(repo, "node", "child"), []byte("child\n"), 0o644); err != nil {
					t.Fatal(err)
				}
			},
			changes: func(linkOID, childOID string) []change {
				return []change{
					{Path: "node", After: &checkpoint.Entry{Path: "node", Mode: gitpkg.SymlinkMode, OID: linkOID}},
					{Path: "node/child", Before: &checkpoint.Entry{Path: "node/child", Mode: gitpkg.RegularFileMode, OID: childOID}},
				}
			},
			assertPath: func(t *testing.T, repo string) {
				target, err := os.Readlink(filepath.Join(repo, "node"))
				if err != nil || target != "target" {
					t.Fatalf("link=(%q,%v)", target, err)
				}
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			repo := t.TempDir()
			if err := gitpkg.Init(ctx, repo); err != nil {
				t.Fatal(err)
			}
			fileOID, err := gitpkg.HashObjectStdin(ctx, repo, []byte("file\n"))
			if err != nil {
				t.Fatal(err)
			}
			childOID, err := gitpkg.HashObjectStdin(ctx, repo, []byte("child\n"))
			if err != nil {
				t.Fatal(err)
			}
			linkOID, err := gitpkg.HashObjectStdin(ctx, repo, []byte("target"))
			if err != nil {
				t.Fatal(err)
			}
			test.prepare(t, repo)
			firstOID := fileOID
			if test.name == "directory-to-symlink" {
				firstOID = linkOID
			}
			if err := applyChanges(ctx, repo, test.changes(firstOID, childOID)); err != nil {
				t.Fatal(err)
			}
			test.assertPath(t, repo)
		})
	}
}

var directoryLeafTransitions = []struct {
	name      string
	afterMode string
	afterBody string
}{
	{name: "directory-to-file", afterMode: gitpkg.RegularFileMode, afterBody: "file\n"},
	{name: "directory-to-symlink", afterMode: gitpkg.SymlinkMode, afterBody: "target"},
}

func TestRestoreImmediateRollbackHandlesDirectoryLeafTransitions(t *testing.T) {
	ctx := context.Background()
	for _, test := range directoryLeafTransitions {
		t.Run(test.name, func(t *testing.T) {
			repo, gitDir, db, target := transitionRestoreFixture(t, ctx, test.afterMode, test.afterBody)
			plan, err := Preview(ctx, repo, gitDir, db.Path(), target.ID)
			if err != nil {
				t.Fatal(err)
			}
			crash := errors.New("simulated failure after restore apply")
			afterRestoreApplyForTest = func() error { return crash }
			t.Cleanup(func() { afterRestoreApplyForTest = nil })
			if _, err := Apply(ctx, db, plan); !errors.Is(err, crash) {
				t.Fatalf("apply error=%v, want simulated failure", err)
			}
			assertTransitionPreimage(t, repo)
		})
	}
}

func TestRestoreRepairRollbackHandlesDirectoryLeafTransitions(t *testing.T) {
	ctx := context.Background()
	for _, test := range directoryLeafTransitions {
		t.Run(test.name, func(t *testing.T) {
			item := prepareInterruptedTransitionRestore(t, ctx, test.afterMode, test.afterBody)
			plan, err := PreviewRepair(ctx, item.db)
			if err != nil || !plan.CanRepair {
				t.Fatalf("repair plan=%+v err=%v", plan, err)
			}
			result, err := Repair(ctx, item.repo, item.db, plan)
			if err != nil {
				t.Fatal(err)
			}
			if result.ResultCheckpoint != "" {
				t.Fatalf("repair result=%+v, want rollback", result)
			}
			assertTransitionPreimage(t, item.repo)
		})
	}
}

func TestRestoreApplyRefusesSymlinkAncestorSubstitution(t *testing.T) {
	ctx := context.Background()
	repo := t.TempDir()
	if err := gitpkg.Init(ctx, repo); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(repo, "ancestor"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(repo, "ancestor", "file.txt"), []byte("before\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	beforeOID, err := gitpkg.HashObjectStdin(ctx, repo, []byte("before\n"))
	if err != nil {
		t.Fatal(err)
	}
	afterOID, err := gitpkg.HashObjectStdin(ctx, repo, []byte("after\n"))
	if err != nil {
		t.Fatal(err)
	}
	changes := []change{{Path: "ancestor/file.txt",
		Before: &checkpoint.Entry{Path: "ancestor/file.txt", Mode: gitpkg.RegularFileMode, OID: beforeOID},
		After:  &checkpoint.Entry{Path: "ancestor/file.txt", Mode: gitpkg.RegularFileMode, OID: afterOID}}}
	outside := t.TempDir()
	outsidePath := filepath.Join(outside, "file.txt")
	if err := os.WriteFile(outsidePath, []byte("outside\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(repo, "ancestor", "file.txt")); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(repo, "ancestor")); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(repo, "ancestor")); err != nil {
		t.Fatal(err)
	}
	if err := applyChanges(ctx, repo, changes); err == nil {
		t.Fatal("restore followed a substituted symlink ancestor")
	}
	body, err := os.ReadFile(outsidePath)
	if err != nil || string(body) != "outside\n" {
		t.Fatalf("outside file=(%q,%v)", body, err)
	}
}

func TestRestoreBackupsRefusePreimageDriftBeforeMutation(t *testing.T) {
	ctx := context.Background()
	repo, gitDir, db, target := restoreFixture(t, ctx)
	path := filepath.Join(repo, "file.txt")
	if err := os.WriteFile(path, []byte("previewed\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	plan, err := Preview(ctx, repo, gitDir, db.Path(), target.ID)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("concurrent\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	backupDir := filepath.Join(gitDir, "acd", "restore", "preimage-drift")
	if _, err := createBackups(ctx, repo, backupDir, plan); err == nil ||
		!strings.Contains(err.Error(), "changed after preview") {
		t.Fatalf("create backups error=%v, want preimage drift refusal", err)
	}
	body, err := os.ReadFile(path)
	if err != nil || string(body) != "concurrent\n" {
		t.Fatalf("file=(%q,%v), want untouched concurrent edit", body, err)
	}
}

func TestRestoreImmediateRollbackPreservesConcurrentEdit(t *testing.T) {
	ctx := context.Background()
	repo, gitDir, db, target := restoreFixture(t, ctx)
	path := filepath.Join(repo, "file.txt")
	if err := os.WriteFile(path, []byte("new\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	plan, err := Preview(ctx, repo, gitDir, db.Path(), target.ID)
	if err != nil {
		t.Fatal(err)
	}
	targetOID := plan.changes[0].After.OID
	objectPath := filepath.Join(gitDir, "objects", targetOID[:2], targetOID[2:])
	if err := os.Remove(objectPath); err != nil {
		t.Fatal(err)
	}
	var hookErr error
	beforeRollbackClaimForTest = func(relative string) {
		if relative == "file.txt" {
			hookErr = os.WriteFile(path, []byte("third-party\n"), 0o644)
		}
	}
	t.Cleanup(func() { beforeRollbackClaimForTest = nil })
	_, err = Apply(ctx, db, plan)
	if hookErr != nil {
		t.Fatal(hookErr)
	}
	if err == nil || !strings.Contains(err.Error(), "changed during rollback") {
		t.Fatalf("restore error=%v, want concurrent rollback refusal", err)
	}
	body, readErr := os.ReadFile(path)
	if readErr != nil || string(body) != "third-party\n" {
		t.Fatalf("file=(%q,%v), want preserved concurrent edit", body, readErr)
	}
}

func TestRestoreRepairRollbackPreservesConcurrentEdit(t *testing.T) {
	ctx := context.Background()
	item := prepareInterruptedRestore(t, ctx)
	path := filepath.Join(item.repo, "file.txt")
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	plan, err := PreviewRepair(ctx, item.db)
	if err != nil || !plan.CanRepair {
		t.Fatalf("repair plan=%+v err=%v", plan, err)
	}
	var hookErr error
	beforeRollbackClaimForTest = func(relative string) {
		if relative == "file.txt" {
			hookErr = os.WriteFile(path, []byte("third-party\n"), 0o644)
		}
	}
	t.Cleanup(func() { beforeRollbackClaimForTest = nil })
	_, err = Repair(ctx, item.repo, item.db, plan)
	if hookErr != nil {
		t.Fatal(hookErr)
	}
	if err == nil || !strings.Contains(err.Error(), "changed during rollback") {
		t.Fatalf("repair error=%v, want concurrent rollback refusal", err)
	}
	body, readErr := os.ReadFile(path)
	if readErr != nil || string(body) != "third-party\n" {
		t.Fatalf("file=(%q,%v), want preserved concurrent edit", body, readErr)
	}
	pending, pendingErr := state.RestoreRepairPending(ctx, item.db)
	if pendingErr != nil || !pending {
		t.Fatalf("restore repair pending=%v err=%v", pending, pendingErr)
	}
}

func TestSafeTreeNoReplaceRenameLeavesNoAliasAtCrashBoundary(t *testing.T) {
	root := t.TempDir()
	tree, err := openSafeTree(root)
	if err != nil {
		t.Fatal(err)
	}
	defer tree.Close()
	crash := errors.New("simulated crash after no-replace rename")
	afterNoReplaceRenameForTest = func() error { return crash }
	t.Cleanup(func() { afterNoReplaceRenameForTest = nil })
	if err := tree.writeFile("file.txt", []byte("restored\n"), 0o644); !errors.Is(err, crash) {
		t.Fatalf("write error=%v, want simulated crash", err)
	}
	body, err := os.ReadFile(filepath.Join(root, "file.txt"))
	if err != nil || string(body) != "restored\n" {
		t.Fatalf("file=(%q,%v), want installed restore target", body, err)
	}
	entries, err := os.ReadDir(root)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), ".acd-restore-") {
			t.Fatalf("crash left unjournaled restore alias %s", entry.Name())
		}
	}
}

func TestRestoreRepairCrashMatrix(t *testing.T) {
	for _, test := range []struct {
		name       string
		mutate     func(*testing.T, context.Context, interruptedRestore)
		wantRepair bool
		wantBody   string
		wantResult bool
	}{
		{name: "before-mutation", wantRepair: true, wantBody: "new\n"},
		{name: "partial-delete", wantRepair: true, wantBody: "new\n", mutate: func(t *testing.T, _ context.Context, item interruptedRestore) {
			if err := os.Remove(filepath.Join(item.repo, "file.txt")); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "exact-target", wantRepair: true, wantBody: "old\n", wantResult: true, mutate: func(t *testing.T, ctx context.Context, item interruptedRestore) {
			if err := applyChanges(ctx, item.repo, item.plan.changes); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "third-party-edit", wantBody: "third-party\n", mutate: func(t *testing.T, _ context.Context, item interruptedRestore) {
			if err := os.WriteFile(filepath.Join(item.repo, "file.txt"), []byte("third-party\n"), 0o644); err != nil {
				t.Fatal(err)
			}
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			item := prepareInterruptedRestore(t, ctx)
			if test.mutate != nil {
				test.mutate(t, ctx, item)
			}
			repairPlan, err := PreviewRepair(ctx, item.db)
			if err != nil || !repairPlan.CanRepair {
				t.Fatalf("repair plan=%+v err=%v", repairPlan, err)
			}
			result, err := Repair(ctx, item.repo, item.db, repairPlan)
			if test.wantRepair && err != nil {
				t.Fatal(err)
			}
			if !test.wantRepair && err == nil {
				t.Fatal("repair unexpectedly accepted third-party edit")
			}
			if test.wantResult != (result.ResultCheckpoint != "") {
				t.Fatalf("result=%+v", result)
			}
			body, readErr := os.ReadFile(filepath.Join(item.repo, "file.txt"))
			if readErr != nil || string(body) != test.wantBody {
				t.Fatalf("file=(%q,%v)", body, readErr)
			}
			pending, pendingErr := state.RestoreRepairPending(ctx, item.db)
			if pendingErr != nil {
				t.Fatal(pendingErr)
			}
			if pending != !test.wantRepair {
				t.Fatalf("restore repair pending=%v want %v", pending, !test.wantRepair)
			}
		})
	}
}

func TestRestoreApplyRefusesSecondRestoreWhileRepairPending(t *testing.T) {
	ctx := context.Background()
	item := prepareInterruptedRestore(t, ctx)
	if _, err := Apply(ctx, item.db, item.plan); err == nil ||
		!strings.Contains(err.Error(), "repair the interrupted restore") {
		t.Fatalf("second restore error=%v, want repair-required refusal", err)
	}
	var applying int
	if err := item.db.ReadSQL().QueryRowContext(ctx, `
SELECT COUNT(*) FROM restore_operations WHERE phase IN ('applying','applied')`).Scan(&applying); err != nil {
		t.Fatal(err)
	}
	if applying != 1 {
		t.Fatalf("repairable restore count=%d want 1", applying)
	}
	body, err := os.ReadFile(filepath.Join(item.repo, "file.txt"))
	if err != nil || string(body) != "new\n" {
		t.Fatalf("file=(%q,%v), want original interrupted preimage", body, err)
	}
}

func TestRestoreRepairRefusesChangedHeadContext(t *testing.T) {
	ctx := context.Background()
	item := prepareInterruptedRestore(t, ctx)
	if err := applyChanges(ctx, item.repo, item.plan.changes); err != nil {
		t.Fatal(err)
	}
	if _, err := gitpkg.Run(ctx, gitpkg.RunOpts{Dir: item.repo}, "checkout", "-b", "other"); err != nil {
		t.Fatal(err)
	}
	plan, err := PreviewRepair(ctx, item.db)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Repair(ctx, item.repo, item.db, plan); err == nil || !strings.Contains(err.Error(), "HEAD") {
		t.Fatalf("repair error=%v", err)
	}
}

func TestRestoreRepairRefusesBranchGenerationABA(t *testing.T) {
	ctx := context.Background()
	item := prepareInterruptedRestore(t, ctx)
	if err := applyChanges(ctx, item.repo, item.plan.changes); err != nil {
		t.Fatal(err)
	}
	head, err := gitpkg.RevParse(ctx, item.repo, "HEAD")
	if err != nil {
		t.Fatal(err)
	}
	if err := daemon.SaveBranchGeneration(ctx, item.db, item.plan.BranchGeneration+1, head); err != nil {
		t.Fatal(err)
	}
	plan, err := PreviewRepair(ctx, item.db)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Repair(ctx, item.repo, item.db, plan); err == nil ||
		!strings.Contains(err.Error(), "branch generation changed") {
		t.Fatalf("repair error=%v, want generation ABA refusal", err)
	}
	var events int
	if err := item.db.ReadSQL().QueryRowContext(ctx, "SELECT COUNT(*) FROM capture_events").Scan(&events); err != nil {
		t.Fatal(err)
	}
	if events != 0 {
		t.Fatalf("generation ABA repair created %d late publication events", events)
	}
}

func TestRestoreRepairUsesResolvedProtectionPolicy(t *testing.T) {
	ctx := context.Background()
	item := prepareInterruptedRestore(t, ctx)
	if err := applyChanges(ctx, item.repo, item.plan.changes); err != nil {
		t.Fatal(err)
	}
	plan, err := PreviewRepair(ctx, item.db)
	if err != nil {
		t.Fatal(err)
	}
	policy := ProtectionPolicy{
		Sensitive:  state.NewSensitiveMatcherFromValue("file.txt"),
		SafeIgnore: state.NewSafeIgnoreMatcherFromValues("false", ""),
	}
	if _, err := RepairWithPolicy(ctx, item.repo, item.db, plan, policy); err == nil {
		t.Fatal("repair accepted a target excluded by the resolved worker policy")
	}
}

func TestRestoreRepairDetachedCreatesProtectionOnlyCheckpoint(t *testing.T) {
	ctx := context.Background()
	item := prepareInterruptedDetachedRestore(t, ctx)
	if err := applyChanges(ctx, item.repo, item.plan.changes); err != nil {
		t.Fatal(err)
	}
	plan, err := PreviewRepair(ctx, item.db)
	if err != nil {
		t.Fatal(err)
	}
	result, err := Repair(ctx, item.repo, item.db, plan)
	if err != nil {
		t.Fatal(err)
	}
	created, err := state.ResolveCheckpoint(ctx, item.db.Path(), result.ResultCheckpoint)
	if err != nil {
		t.Fatal(err)
	}
	if created.ObservedRef != "" {
		t.Fatalf("detached repair result observed ref=%q", created.ObservedRef)
	}
	var eventCount int
	if err := item.db.ReadSQL().QueryRowContext(ctx, "SELECT COUNT(*) FROM capture_events").Scan(&eventCount); err != nil {
		t.Fatal(err)
	}
	if eventCount != 0 {
		t.Fatalf("detached repair created %d publication events", eventCount)
	}
}

type interruptedRestore struct {
	repo string
	db   *state.DB
	plan Plan
}

func transitionRestoreFixture(
	t *testing.T,
	ctx context.Context,
	afterMode string,
	afterBody string,
) (string, string, *state.DB, state.Checkpoint) {
	t.Helper()
	repo, gitDir, db, _ := restoreFixture(t, ctx)
	head, err := gitpkg.RevParse(ctx, repo, "HEAD")
	if err != nil {
		t.Fatal(err)
	}
	if err := daemon.SaveBranchGeneration(ctx, db, 7, head); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(repo, "other.txt"), []byte("target\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if afterMode == gitpkg.SymlinkMode {
		if err := os.Symlink(afterBody, filepath.Join(repo, "node")); err != nil {
			t.Fatal(err)
		}
	} else if err := os.WriteFile(filepath.Join(repo, "node"), []byte(afterBody), 0o644); err != nil {
		t.Fatal(err)
	}
	store := checkpoint.Store{DB: db}
	checker := gitpkg.NewIgnoreChecker(repo)
	targetEntries, _, _, err := daemon.ScanProtectedEntries(ctx, repo,
		daemon.CaptureOpts{IgnoreChecker: checker, CheckpointStore: &store})
	_ = checker.Close()
	if err != nil {
		t.Fatal(err)
	}
	target, err := store.Create(ctx, checkpoint.Request{
		RepoRoot: repo, WorktreeID: checkpoint.WorktreeID(repo), Reason: state.CheckpointReasonPoll,
		ObservationEpoch: 2, CoverageEpoch: 2, ObservedHead: head,
		ObservedRef: "refs/heads/main", Entries: targetEntries,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(repo, "node")); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(repo, "node"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(repo, "node", "child"), []byte("child\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(repo, "other.txt"), []byte("current\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	return repo, gitDir, db, target.Checkpoint
}

func prepareInterruptedTransitionRestore(
	t *testing.T,
	ctx context.Context,
	afterMode string,
	afterBody string,
) interruptedRestore {
	t.Helper()
	repo, gitDir, db, target := transitionRestoreFixture(t, ctx, afterMode, afterBody)
	plan, err := Preview(ctx, repo, gitDir, db.Path(), target.ID)
	if err != nil {
		t.Fatal(err)
	}
	item := journalInterruptedRestore(t, ctx, repo, gitDir, db, target, plan)
	nodeChanges := make([]change, 0, 2)
	for _, change := range plan.changes {
		if change.Path == "node" || strings.HasPrefix(change.Path, "node/") {
			nodeChanges = append(nodeChanges, change)
		}
	}
	if err := applyChanges(ctx, repo, nodeChanges); err != nil {
		t.Fatal(err)
	}
	return item
}

func assertTransitionPreimage(t *testing.T, repo string) {
	t.Helper()
	body, err := os.ReadFile(filepath.Join(repo, "node", "child"))
	if err != nil || string(body) != "child\n" {
		t.Fatalf("child=(%q,%v), want restored directory preimage", body, err)
	}
	body, err = os.ReadFile(filepath.Join(repo, "other.txt"))
	if err != nil || string(body) != "current\n" {
		t.Fatalf("other=(%q,%v), want restored file preimage", body, err)
	}
}

func prepareInterruptedRestore(t *testing.T, ctx context.Context) interruptedRestore {
	return prepareInterruptedRestoreMode(t, ctx, false)
}

func prepareInterruptedDetachedRestore(t *testing.T, ctx context.Context) interruptedRestore {
	return prepareInterruptedRestoreMode(t, ctx, true)
}

func prepareInterruptedRestoreMode(t *testing.T, ctx context.Context, detached bool) interruptedRestore {
	t.Helper()
	repo, gitDir, db, target := restoreFixture(t, ctx)
	head, err := gitpkg.RevParse(ctx, repo, "HEAD")
	if err != nil {
		t.Fatal(err)
	}
	if err := daemon.SaveBranchGeneration(ctx, db, 7, head); err != nil {
		t.Fatal(err)
	}
	if detached {
		if _, err := gitpkg.Run(ctx, gitpkg.RunOpts{Dir: repo}, "checkout", "--detach", "HEAD"); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(repo, "file.txt"), []byte("new\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	plan, err := Preview(ctx, repo, gitDir, db.Path(), target.ID)
	if err != nil {
		t.Fatal(err)
	}
	return journalInterruptedRestore(t, ctx, repo, gitDir, db, target, plan)
}

func journalInterruptedRestore(
	t *testing.T,
	ctx context.Context,
	repo string,
	gitDir string,
	db *state.DB,
	target state.Checkpoint,
	plan Plan,
) interruptedRestore {
	t.Helper()
	head, err := gitpkg.RevParse(ctx, repo, "HEAD")
	if err != nil {
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
	pre, err := store.Create(ctx, checkpoint.Request{
		RepoRoot: repo, WorktreeID: target.WorktreeID, Reason: state.CheckpointReasonPreRestore,
		ObservationEpoch: 2, CoverageEpoch: 2, ObservedHead: head,
		ObservedRef: observedRef(plan.HeadToken), Entries: preEntries,
	})
	if err != nil {
		t.Fatal(err)
	}
	operationID := "restore-crash-" + strings.ReplaceAll(t.Name(), "/", "-")
	steps := make([]state.OperationStep, 0, len(plan.changes))
	for i, change := range plan.changes {
		steps = append(steps, state.OperationStep{
			Sequence: i + 1, Kind: "restore_path", Target: change.Path,
			BeforeDigest: entryDigest(change.Before), AfterDigest: entryDigest(change.After),
		})
	}
	if err := state.PrepareOperation(ctx, db, state.Operation{
		ID: operationID, Kind: "restore", WorktreeID: target.WorktreeID, PlanDigest: plan.PlanDigest,
	}, steps); err != nil {
		t.Fatal(err)
	}
	if err := state.PrepareRestoreOperation(ctx, db, operationID, target.ID, plan.PlanDigest); err != nil {
		t.Fatal(err)
	}
	backupDir := filepath.Join(gitDir, "acd", "restore", operationID)
	if _, err := createBackups(ctx, repo, backupDir, plan); err != nil {
		t.Fatal(err)
	}
	if err := state.BeginRestoreApply(ctx, db, operationID, pre.Checkpoint.ID); err != nil {
		t.Fatal(err)
	}
	operation, err := state.RepairableRestoreOperation(ctx, db)
	if err != nil {
		t.Fatal(err)
	}
	if operation.OperationID != operationID || operation.Phase != state.OperationApplying || operation.OperationStatus != state.OperationActive {
		t.Fatalf("operation not repairable before mutation: %+v", operation)
	}
	if pending, err := state.RestoreRepairPending(ctx, db); err != nil || !pending {
		t.Fatalf("restore repair pending=%v err=%v", pending, err)
	}
	return interruptedRestore{repo: repo, db: db, plan: plan}
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
