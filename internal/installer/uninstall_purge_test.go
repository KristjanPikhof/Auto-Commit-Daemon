package installer

import (
	"bytes"
	"context"
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
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/paths"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/supervisor"
)

func uninstallPurgeFixture(t *testing.T) (paths.Roots, string, gitpkg.Worktree, *state.DB, *central.Registry) {
	t.Helper()
	ctx := t.Context()
	roots, repo, _ := installerFixture(t, ctx)
	wt, err := gitpkg.ResolveWorktree(ctx, repo)
	if err != nil {
		t.Fatal(err)
	}
	db, err := state.Open(ctx, state.DBPathFromGitDir(wt.GitDir))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	registry := central.NewRegistry()
	if _, err := registry.RegisterResolvedRepo(wt, "test", 1); err != nil {
		t.Fatal(err)
	}
	registry.Repos[0].LifecycleState = central.RepoLifecycleDisabled
	if err := central.Save(roots, registry); err != nil {
		t.Fatal(err)
	}
	return roots, wt.Root, wt, db, registry
}

func uninstallCheckpoint(t *testing.T, repo string, db *state.DB) checkpoint.Result {
	t.Helper()
	result, err := (checkpoint.Store{DB: db}).Create(t.Context(), checkpoint.Request{RepoRoot: repo, WorktreeID: checkpoint.WorktreeID(repo), Reason: state.CheckpointReasonManualBarrier, ObservationEpoch: 1, CoverageEpoch: 1})
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func TestUninstallPurgeFinalCheckpointPreservesProjectGit(t *testing.T) {
	ctx := t.Context()
	roots, repo, wt, db, _ := uninstallPurgeFixture(t)
	first := uninstallCheckpoint(t, repo, db)
	if err := gitpkg.UpdateRef(ctx, repo, "refs/heads/main", first.Checkpoint.CommitOID, ""); err != nil {
		t.Fatal(err)
	}
	if err := gitpkg.UpdateRef(ctx, repo, "refs/tags/keep", first.Checkpoint.CommitOID, ""); err != nil {
		t.Fatal(err)
	}
	staged, err := gitpkg.HashObjectStdin(ctx, repo, []byte("user staging"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := gitpkg.Run(ctx, gitpkg.RunOpts{Dir: repo}, "update-index", "--add", "--cacheinfo", "100644", staged, "file.txt"); err != nil {
		t.Fatal(err)
	}
	index, err := os.ReadFile(filepath.Join(wt.GitDir, "index"))
	if err != nil {
		t.Fatal(err)
	}
	live, err := os.ReadFile(filepath.Join(repo, "file.txt"))
	if err != nil {
		t.Fatal(err)
	}
	plan, err := BuildUninstallPlan(ctx, roots, true)
	if err != nil {
		t.Fatal(err)
	}
	final := uninstallCheckpoint(t, repo, db)
	fresh, err := BuildUninstallPlan(ctx, roots, true)
	if err != nil || fresh.Digest != plan.Digest || len(fresh.PurgeTargets[0].Refs) != 2 {
		t.Fatalf("CLI fingerprint changed during continued protection: %s %s %v", plan.Digest, fresh.Digest, err)
	}
	preflight := plan
	preflight.PurgeTargets = append([]PurgeTarget(nil), plan.PurgeTargets...)
	if err := refreshUninstallPurgeRefs(ctx, roots, &preflight, true); err != nil {
		t.Fatal(err)
	}
	if len(plan.PurgeTargets[0].Refs) != 1 || plan.Digest != digestUninstallPlan(plan) {
		t.Fatal("preflight mutated reviewed plan")
	}
	if err := refreshUninstallPurgeRefs(ctx, roots, &plan, false); err != nil {
		t.Fatal(err)
	}
	if len(plan.PurgeTargets[0].Refs) != 2 {
		t.Fatalf("final checkpoint omitted: %+v", plan.PurgeTargets)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	txn, err := stagePurge(ctx, plan.OperationID, plan.PurgeTargets)
	if err != nil {
		t.Fatal(err)
	}
	if err := txn.commit(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(state.AcdDirFromGitDir(wt.GitDir)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("state not purged: %v", err)
	}
	for _, ref := range []string{first.Checkpoint.Ref, final.Checkpoint.Ref} {
		if _, err := gitpkg.RevParse(ctx, repo, ref); !errors.Is(err, gitpkg.ErrRefNotFound) {
			t.Fatalf("private ref survived: %s %v", ref, err)
		}
	}
	for _, ref := range []string{"HEAD", "refs/tags/keep"} {
		oid, err := gitpkg.RevParse(ctx, repo, ref)
		if err != nil || oid != first.Checkpoint.CommitOID {
			t.Fatalf("public history changed %s: %s %v", ref, oid, err)
		}
	}
	afterIndex, _ := os.ReadFile(filepath.Join(wt.GitDir, "index"))
	afterLive, _ := os.ReadFile(filepath.Join(repo, "file.txt"))
	if !bytes.Equal(index, afterIndex) || !bytes.Equal(live, afterLive) {
		t.Fatal("purge changed user staging or live files")
	}
}

func TestUninstallPurgeRejectsExternalChanges(t *testing.T) {
	for _, kind := range []string{"moved-ref", "new-private-ref", "unproved-checkpoint", "registry", "launcher"} {
		t.Run(kind, func(t *testing.T) {
			ctx := t.Context()
			roots, repo, _, db, registry := uninstallPurgeFixture(t)
			first := uninstallCheckpoint(t, repo, db)
			if err := os.MkdirAll(filepath.Dir(roots.ManagedBinaryPath()), 0700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(roots.ManagedBinaryPath(), []byte("owned binary"), 0755); err != nil {
				t.Fatal(err)
			}
			launcher := filepath.Join(os.Getenv("HOME"), ".local", "bin", "acd")
			if err := os.MkdirAll(filepath.Dir(launcher), 0700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(launcher, []byte("owned binary"), 0755); err != nil {
				t.Fatal(err)
			}
			plan, err := BuildUninstallPlan(ctx, roots, true)
			if err != nil {
				t.Fatal(err)
			}
			if plan.Launcher != launcher {
				t.Fatal("exact installed launcher not reviewed")
			}
			switch kind {
			case "moved-ref":
				newer := uninstallCheckpoint(t, repo, db)
				if err := gitpkg.UpdateRef(ctx, repo, first.Checkpoint.Ref, newer.Checkpoint.CommitOID, first.Checkpoint.CommitOID); err != nil {
					t.Fatal(err)
				}
			case "new-private-ref":
				if err := gitpkg.UpdateRef(ctx, repo, "refs/acd/unknown/added", first.Checkpoint.CommitOID, ""); err != nil {
					t.Fatal(err)
				}
			case "unproved-checkpoint":
				if err := gitpkg.UpdateRef(ctx, repo, gitpkg.CheckpointRefPrefix+checkpoint.WorktreeID(repo)+"/unproved", first.Checkpoint.CommitOID, ""); err != nil {
					t.Fatal(err)
				}
			case "registry":
				registry.Repos[0].Harnesses = []string{"changed"}
				if err := central.Save(roots, registry); err != nil {
					t.Fatal(err)
				}
			case "launcher":
				if err := os.WriteFile(launcher, []byte("replacement user launcher"), 0755); err != nil {
					t.Fatal(err)
				}
			}
			if err := refreshUninstallPurgeRefs(ctx, roots, &plan, false); err == nil {
				t.Fatal("external change accepted")
			}
			if _, err := os.Stat(db.Path()); err != nil {
				t.Fatal("refusal removed database")
			}
		})
	}
}

func TestUninstallPurgeMissingWorktreeAndAmbiguousRefs(t *testing.T) {
	ctx := t.Context()
	roots, repo, wt, db, registry := uninstallPurgeFixture(t)
	cp := uninstallCheckpoint(t, repo, db)
	missing := filepath.Join(t.TempDir(), "missing")
	registry.Repos = append(registry.Repos, central.RepoRecord{Path: missing, RepositoryID: registry.Repos[0].RepositoryID, WorktreeID: central.CanonicalID(missing), CommonDir: wt.CommonDir, StateDB: filepath.Join(t.TempDir(), "do-not-guess.db"), LifecycleState: central.RepoLifecycleEnabled})
	if err := central.Save(roots, registry); err != nil {
		t.Fatal(err)
	}
	plan, err := BuildUninstallPlan(ctx, roots, true)
	if err != nil {
		t.Fatal(err)
	}
	var unavailable, owned int
	for _, target := range plan.PurgeTargets {
		if target.Unavailable != "" {
			unavailable++
			if target.StateDir != "" || target.GitDir != "" {
				t.Fatal("missing path guessed")
			}
		} else if len(target.Refs) > 0 {
			owned++
		}
	}
	if unavailable != 1 || owned != 1 || len(plan.Warnings) != 1 {
		t.Fatalf("missing/common inventory: %+v", plan)
	}
	for _, kind := range []string{"symbolic", "dangling"} {
		alias := "refs/acd/ambiguous/" + kind
		target := cp.Checkpoint.Ref
		if kind == "dangling" {
			target = "refs/heads/missing"
		}
		if _, err := gitpkg.Run(ctx, gitpkg.RunOpts{Dir: repo}, "symbolic-ref", alias, target); err != nil {
			t.Fatal(err)
		}
		if _, err := BuildUninstallPlan(ctx, roots, true); err == nil || !strings.Contains(err.Error(), "ref") {
			t.Fatalf("ambiguous ref accepted: %v", err)
		}
		if _, err := gitpkg.Run(ctx, gitpkg.RunOpts{Dir: repo}, "symbolic-ref", "--delete", alias); err != nil {
			t.Fatal(err)
		}
	}
}

func TestUninstallPurgeRollbackDoesNotOverwriteExternalRef(t *testing.T) {
	ctx := context.Background()
	_, repo, wt, db, _ := uninstallPurgeFixture(t)
	first := uninstallCheckpoint(t, repo, db)
	second := uninstallCheckpoint(t, repo, db)
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	target := PurgeTarget{RepositoryID: central.CanonicalID(wt.CommonDir), WorktreeID: central.CanonicalID(repo), RepoRoot: repo, GitDir: wt.GitDir, StateDir: state.AcdDirFromGitDir(wt.GitDir), Refs: []gitpkg.RefExpectation{{Ref: first.Checkpoint.Ref, OID: first.Checkpoint.CommitOID}}}
	txn, err := stagePurge(ctx, "rollback-collision", []PurgeTarget{target})
	if err != nil {
		t.Fatal(err)
	}
	if err := gitpkg.UpdateRef(ctx, repo, first.Checkpoint.Ref, second.Checkpoint.CommitOID, ""); err != nil {
		t.Fatal(err)
	}
	if err := txn.rollback(ctx); err == nil {
		t.Fatal("rollback overwrote external ref")
	}
	actual, err := gitpkg.RevParse(ctx, repo, first.Checkpoint.Ref)
	if err != nil || actual != second.Checkpoint.CommitOID {
		t.Fatal("external replacement lost")
	}
	if _, err := os.Stat(state.DBPathFromGitDir(wt.GitDir)); err != nil {
		t.Fatal("rollback failed to restore state")
	}
}

func TestUninstallPurgeFinalCheckpointIgnoresUnrelatedOldDatabase(t *testing.T) {
	ctx := t.Context()
	roots, repo, _, db, registry := uninstallPurgeFixture(t)
	uninstallCheckpoint(t, repo, db)
	older := filepath.Join(t.TempDir(), "older")
	if err := os.MkdirAll(older, 0700); err != nil {
		t.Fatal(err)
	}
	if err := gitpkg.Init(ctx, older); err != nil {
		t.Fatal(err)
	}
	if _, err := gitpkg.Run(ctx, gitpkg.RunOpts{Dir: older}, "symbolic-ref", "HEAD", "refs/heads/main"); err != nil {
		t.Fatal(err)
	}
	olderWT, err := gitpkg.ResolveWorktree(ctx, older)
	if err != nil {
		t.Fatal(err)
	}
	olderDB, err := state.Open(ctx, state.DBPathFromGitDir(olderWT.GitDir))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := olderDB.SQL().ExecContext(ctx, "DROP TABLE checkpoints"); err != nil {
		t.Fatal(err)
	}
	if _, err := olderDB.SQL().ExecContext(ctx, "PRAGMA user_version=19"); err != nil {
		t.Fatal(err)
	}
	if err := olderDB.Close(); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(state.DBPathFromGitDir(olderWT.GitDir))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := registry.RegisterResolvedRepo(olderWT, "old", 1); err != nil {
		t.Fatal(err)
	}
	for i := range registry.Repos {
		registry.Repos[i].LifecycleState = central.RepoLifecycleDisabled
	}
	if err := central.Save(roots, registry); err != nil {
		t.Fatal(err)
	}
	plan, err := BuildUninstallPlan(ctx, roots, true)
	if err != nil {
		t.Fatal(err)
	}
	uninstallCheckpoint(t, repo, db)
	if err := refreshUninstallPurgeRefs(ctx, roots, &plan, false); err != nil {
		t.Fatal(err)
	}
	after, err := os.ReadFile(state.DBPathFromGitDir(olderWT.GitDir))
	if err != nil || !bytes.Equal(before, after) {
		t.Fatal("unrelated old database was migrated or modified")
	}
}

func TestUninstallPurgeRejectsSymlinkDataRoot(t *testing.T) {
	external := t.TempDir()
	sentinel := filepath.Join(external, "project.txt")
	if err := os.WriteFile(sentinel, []byte("project survives"), 0600); err != nil {
		t.Fatal(err)
	}
	root := filepath.Join(t.TempDir(), "acd")
	if err := os.Symlink(external, root); err != nil {
		t.Fatal(err)
	}
	if err := purgeRootExcept(root, filepath.Join(root, "operations.lock")); err == nil {
		t.Fatal("symlink data root accepted")
	}
	body, err := os.ReadFile(sentinel)
	if err != nil || string(body) != "project survives" {
		t.Fatal("purge followed data root into project")
	}
}

func TestUninstallPublicationHoldRestoresPriorOwnership(t *testing.T) {
	ctx := t.Context()
	roots, _, _, _, _ := uninstallPurgeFixture(t)
	marker := roots.SetupPublicationHoldPath()
	if err := os.MkdirAll(filepath.Dir(marker), 0700); err != nil {
		t.Fatal(err)
	}
	prior := []byte("prior operator publication hold\n")
	if err := os.WriteFile(marker, prior, 0600); err != nil {
		t.Fatal(err)
	}
	plan, err := BuildUninstallPlan(ctx, roots, true)
	if err != nil {
		t.Fatal(err)
	}
	backups, err := backupFiles(plan.BackupRoot, []string{marker}, plan.PriorService)
	if err != nil {
		t.Fatal(err)
	}
	owned := uninstallHoldBody(plan)
	if err := preparePostMutation(plan.BackupRoot, backups, plan.PriorService, marker, fileDigest(owned, 0600)); err != nil {
		t.Fatal(err)
	}
	if err := writeAtomic(marker, owned, 0600); err != nil {
		t.Fatal(err)
	}
	if err := refreshUninstallPurgeRefs(ctx, roots, &plan, false); err != nil {
		t.Fatal(err)
	}
	if err := restoreFiles(backups); err != nil {
		t.Fatal(err)
	}
	actual, err := os.ReadFile(marker)
	if err != nil || !bytes.Equal(actual, prior) {
		t.Fatal("rollback lost prior hold")
	}
	if err := os.WriteFile(marker, []byte("unrelated operation"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := refreshUninstallPurgeRefs(ctx, roots, &plan, false); err == nil {
		t.Fatal("external hold ownership accepted")
	}
}

func TestUninstallRefusesOwnedLiveFinishAndHistory(t *testing.T) {
	for _, kind := range []string{"finish", "history"} {
		t.Run(kind, func(t *testing.T) {
			ctx := t.Context()
			_, repo, wt, db, registry := uninstallPurgeFixture(t)
			cp, err := (checkpoint.Store{DB: db}).Create(ctx, checkpoint.Request{RepoRoot: repo, WorktreeID: checkpoint.WorktreeID(repo), Reason: state.CheckpointReasonManualBarrier, ObservedRef: "refs/heads/main", ObservationEpoch: 1, CoverageEpoch: 1})
			if err != nil {
				t.Fatal(err)
			}
			if kind == "finish" {
				_, created, err := state.StartFinishRequest(ctx, db, state.FinishRequest{ID: "uninstall-running", DeadlineTS: float64(time.Now().Add(time.Minute).Unix()), Target: state.FinishTarget{RepositoryID: central.CanonicalID(wt.CommonDir), WorktreeID: checkpoint.WorktreeID(repo), BranchRef: "refs/heads/main", CheckpointID: cp.Checkpoint.ID, CheckpointTree: cp.Checkpoint.TreeOID, TargetTree: cp.Checkpoint.TreeOID, IndexDigest: "approved-index", Mode: "local", Style: "imperative", ConfigDigest: "approved-config"}})
				if err != nil || !created {
					t.Fatalf("start: %t %v", created, err)
				}
			} else {
				_, created, err := state.StartFinishHistoryRequest(ctx, db, state.FinishHistoryRequest{PlanID: "uninstall-history", WorktreeID: checkpoint.WorktreeID(repo), ConfigDigest: "approved-config", IndexDigest: "approved-index", DeadlineTS: float64(time.Now().Add(time.Minute).Unix())})
				if err != nil || !created {
					t.Fatalf("start: %t %v", created, err)
				}
			}
			owner, err := daemon.AcquireDaemonLock(wt.GitDir)
			if err != nil {
				t.Fatal(err)
			}
			if err := refuseLiveUninstallPublication(ctx, registry); err == nil {
				owner.Release()
				t.Fatal("live publication accepted")
			}
			if err := owner.Release(); err != nil {
				t.Fatal(err)
			}
			if err := refuseLiveUninstallPublication(ctx, registry); err != nil {
				t.Fatalf("stopped request refused despite publication hold: %v", err)
			}
		})
	}
}

func TestUninstallPurgePreviewPreservesNativeRefLock(t *testing.T) {
	ctx := t.Context()
	roots, repo, wt, db, _ := uninstallPurgeFixture(t)
	cp := uninstallCheckpoint(t, repo, db)
	before, err := BuildUninstallPlan(ctx, roots, true)
	if err != nil {
		t.Fatal(err)
	}
	lockPath := filepath.Join(wt.CommonDir, filepath.FromSlash(cp.Checkpoint.Ref)) + ".lock"
	lockBody := []byte(cp.Checkpoint.CommitOID + "\n")
	if err := os.WriteFile(lockPath, lockBody, 0600); err != nil {
		t.Fatal(err)
	}
	preview, err := BuildUninstallPlan(ctx, roots, true)
	if err != nil {
		t.Fatal(err)
	}
	if preview.Digest != before.Digest || len(preview.PurgeTargets[0].Refs) != 1 || preview.PurgeTargets[0].Refs[0].Ref != cp.Checkpoint.Ref {
		t.Fatalf("lock metadata changed reviewed ref inventory: %+v", preview.PurgeTargets)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	txn, err := stagePurge(ctx, preview.OperationID, preview.PurgeTargets)
	if err == nil {
		t.Fatal("native ref lock collision accepted")
	}
	if err := txn.rollback(ctx); err != nil {
		t.Fatal(err)
	}
	after, err := os.ReadFile(lockPath)
	if err != nil || !bytes.Equal(after, lockBody) {
		t.Fatalf("native lock metadata changed: %q %v", after, err)
	}
	oid, err := gitpkg.RevParse(ctx, repo, cp.Checkpoint.Ref)
	if err != nil || oid != cp.Checkpoint.CommitOID {
		t.Fatalf("locked checkpoint changed: %s %v", oid, err)
	}
	if _, err := os.Stat(db.Path()); err != nil {
		t.Fatalf("collision removed protected state: %v", err)
	}
}

func TestUninstallWaitsForNewOwnedWorkerSocket(t *testing.T) {
	root, err := os.MkdirTemp("/tmp", "acd-uninstall-ready-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	roots := paths.Roots{State: filepath.Join(root, "state")}
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	repositoryID := "1111111111111111"
	worker := supervisor.WorkerStatus{RepositoryID: repositoryID, PID: os.Getpid(), State: "running"}
	var statuses, probes atomic.Int32
	missingSocketObserved := make(chan struct{})
	startReadinessServer(t, ctx, roots.SupervisorSocketPath(), func(request supervisor.Request) supervisor.Response {
		if statuses.Add(1) == 2 {
			close(missingSocketObserved)
		}
		return supervisor.Response{Version: supervisor.ProtocolVersion, ID: request.ID, OK: true,
			Data: supervisor.Status{Workers: []supervisor.WorkerStatus{worker}}}
	})
	done := make(chan error, 1)
	go func() {
		done <- waitSupervisorWorkersReady(ctx, roots, map[string]bool{repositoryID: true}, 15*time.Minute, nil)
	}()
	select {
	case <-missingSocketObserved:
	case <-ctx.Done():
		t.Fatal("worker socket absence was not retried")
	}
	startReadinessServer(t, ctx, supervisor.WorkerSocketPath(roots, repositoryID), func(request supervisor.Request) supervisor.Response {
		response := readyWorkerResponse(request)
		if probes.Add(1) == 1 {
			response.Data = supervisor.WorkerReadiness{RepositoryID: repositoryID, PID: os.Getpid() + 1, Ready: true}
		}
		return response
	})
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if probes.Load() < 2 {
		t.Fatal("accepted a worker with mismatched PID")
	}
}
