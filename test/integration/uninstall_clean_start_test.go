//go:build integration

package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/daemon"
	gitpkg "github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/supervisor"
)

func TestUninstallCleanStartPreservesProjectHistoryAndWork(t *testing.T) {
	for _, scenario := range []string{"running-capture", "stopped-capture", "stopped-prepared-finish"} {
		t.Run(scenario, func(t *testing.T) {
			testUninstallCleanStart(t, scenario != "running-capture", scenario == "stopped-prepared-finish")
		})
	}
}

func testUninstallCleanStart(t *testing.T, stopped, prepared bool) {
	t.Helper()
	f := newFinishFixture(t)
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	runGitOK(t, f.Repo, "tag", "keep-history")
	stagedPath := filepath.Join(f.Repo, "user-staged.txt")
	writeFile(t, stagedPath, "user staging\n")
	runGitOK(t, f.Repo, "add", "user-staged.txt")
	indexPath := filepath.Join(f.Repo, ".git", "index")
	index, err := os.ReadFile(indexPath)
	if err != nil {
		t.Fatal(err)
	}
	publicRefs := runGitOK(t, f.Repo, "for-each-ref", "--format=%(refname) %(objectname)", "refs/heads/", "refs/tags/")
	launcher := filepath.Join(envValue(f.Env, "HOME"), ".local", "bin", "acd")
	if err := os.MkdirAll(filepath.Dir(launcher), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.Link(f.Bin, launcher); err != nil {
		t.Fatal(err)
	}
	// Finish protecting fixture setup before reviewing the purge inventory.
	// A private ref still being prepared is not completed checkpoint proof.
	f.Barrier(t, ctx)
	if stopped {
		prepareStoppedUninstallFinish(t, ctx, f, prepared)
	}
	var preview struct {
		Data struct{ Digest string }
	}
	t.Log("Preview uninstall --dry-run --purge-data")
	finishDecodeJSON(t, runAcd(t, ctx, f.Env, "uninstall", "--dry-run", "--purge-data", "--json"), &preview)
	if preview.Data.Digest == "" {
		t.Fatal("purge preview did not provide a review identity")
	}
	// Protection continues after review. Only proved checkpoint additions may
	// extend the deletion inventory; final barrier/stop checkpoints must too.
	currentPath := filepath.Join(f.Repo, "current-work.txt")
	writeFile(t, currentPath, "latest uncommitted work\n")
	if !stopped {
		f.Barrier(t, ctx)
	}
	t.Log("Apply uninstall --purge-data with the reviewed plan")
	result := runAcd(t, ctx, f.Env, "uninstall", "--purge-data", "--confirm-purge-data", "--yes", "--non-interactive", "--expect-plan", preview.Data.Digest, "--json")
	var applied struct{ OK bool }
	finishDecodeJSON(t, result, &applied)
	if !applied.OK {
		t.Fatalf("purge failed: %s", result.Stdout)
	}
	for _, path := range []string{filepath.Join(f.Repo, ".git", "acd"), f.Roots.ManagedBinaryPath(), launcher, f.Roots.RegistryPath()} {
		if _, err := os.Lstat(path); !os.IsNotExist(err) {
			t.Fatalf("ACD artifact remains: %s error=%v", path, err)
		}
	}
	if refs := strings.TrimSpace(runGitOK(t, f.Repo, "for-each-ref", "--format=%(refname)", "refs/acd/")); refs != "" {
		t.Fatalf("private archive survived purge: %s", refs)
	}
	if head := strings.TrimSpace(runGitOK(t, f.Repo, "rev-parse", "HEAD")); head != f.BaseHead {
		t.Fatalf("purge moved project HEAD: %s", head)
	}
	if after := runGitOK(t, f.Repo, "for-each-ref", "--format=%(refname) %(objectname)", "refs/heads/", "refs/tags/"); after != publicRefs {
		t.Fatal("purge changed project branches or tags")
	}
	afterIndex, err := os.ReadFile(indexPath)
	if err != nil || !bytes.Equal(index, afterIndex) {
		t.Fatalf("purge changed the user's index: %v", err)
	}
	for path, want := range map[string]string{stagedPath: "user staging\n", currentPath: "latest uncommitted work\n"} {
		body, err := os.ReadFile(path)
		if err != nil || string(body) != want {
			t.Fatalf("purge changed live files: %s error=%v", path, err)
		}
	}
}

func prepareStoppedUninstallFinish(t *testing.T, ctx context.Context, f *finishFixture, prepared bool) {
	t.Helper()
	status, err := checkpointRuntimeStatus(ctx, f.Roots)
	if err != nil {
		t.Fatal(err)
	}
	process, err := captureCheckpointProcessIdentity(status.PID)
	if err != nil {
		t.Fatal(err)
	}
	shutdownCheckpointRuntime(t, f.Roots, supervisor.WorkerSocketPath(f.Roots, f.RepositoryID), process)
	if !prepared {
		return
	}
	db, err := state.Open(ctx, f.DBPath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	worker := daemon.FinishWorker{
		RepoPath: f.Repo, RepositoryID: f.RepositoryID, DB: db,
		Config: daemon.FinishWorkerConfig{
			Mode: "local", Style: "imperative", ConfigDigest: "approved-uninstall-fixture",
			VerificationMode: "structural", AuthorName: "Test User", AuthorEmail: "test@example.com",
		},
	}
	request, _, err := worker.Start(ctx, daemon.FinishOptions{Purpose: "Keep prepared work private during uninstall", IncludeStaged: true})
	if err != nil {
		t.Fatal(err)
	}
	proof, err := gitpkg.BuildFinishChain(ctx, f.Repo, gitpkg.FinishBuildOptions{
		RequestID: request.ID, Attempt: request.Attempt, WorktreeID: request.Target.WorktreeID,
		BranchRef: request.Target.BranchRef, ExpectedHead: request.Target.ExpectedHead,
		TargetTree: request.Target.TargetTree, IndexDigest: request.Target.IndexDigest, IncludeStaged: true,
		ObjectRef: gitpkg.FinishRefPrefix + request.ID + "/chain",
		Commits: []gitpkg.FinishCommit{{
			TreeOID: request.Target.TargetTree, Message: "Keep prepared work private during uninstall",
			AuthorName: "Test User", AuthorEmail: "test@example.com",
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := state.PrepareFinishPublication(ctx, db, request.ID, request.Attempt, proof, json.RawMessage(`{"steps":1}`)); err != nil {
		t.Fatal(err)
	}
}
