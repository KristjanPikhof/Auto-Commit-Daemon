//go:build integration

package integration_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/daemon"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/finish"
	gitpkg "github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/migration"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

func TestFinishFaultStagedSelectionAndLaterStaging(t *testing.T) {
	for _, laterStage := range []bool{false, true} {
		t.Run(map[bool]string{false: "consume only approved selection", true: "preserve changed later index"}[laterStage], func(t *testing.T) {
			t.Parallel()
			entered := make(chan struct{}, 1)
			release := make(chan struct{}, 1)
			defer close(release)
			var calls atomic.Int32
			f := finishIntentFixture(t, func(w http.ResponseWriter, r *http.Request) {
				request := decodeFinishChatRequest(t, r)
				calls.Add(1)
				entered <- struct{}{}
				select {
				case <-release:
					writeFinishChatResponse(t, w, request)
				case <-r.Context().Done():
				}
			}, "30s")
			writeFile(t, filepath.Join(f.Repo, "goal.txt"), "approved staging\n")
			runGitOK(t, f.Repo, "add", "goal.txt")
			writeFile(t, filepath.Join(f.Repo, "goal.txt"), "later unstaged bytes\n")
			before := runGitOK(t, f.Repo, "ls-files", "--stage")
			rejected := f.Run(t, t.Context(), "finish", "Complete staged goal", "--json")
			if rejected.ExitCode == 0 || !strings.Contains(rejected.Stdout+rejected.Stderr, "include-staged") || calls.Load() != 0 || runGitOK(t, f.Repo, "ls-files", "--stage") != before {
				t.Fatalf("default stage refusal: %+v calls=%d", rejected, calls.Load())
			}
			request := finishStartRPC(t, f, map[string]any{"purpose": "Complete staged goal", "message": "Complete staged goal", "include_staged": true})
			select {
			case <-entered:
			case <-time.After(15 * time.Second):
				t.Fatal("provider did not start")
			}
			if laterStage {
				writeFile(t, filepath.Join(f.Repo, "later.txt"), "later user staging\n")
				runGitOK(t, f.Repo, "add", "later.txt")
			}
			indexAtRelease := runGitOK(t, f.Repo, "ls-files", "--stage")
			release <- struct{}{}
			result := finishWaitRequest(t, f, request.ID)
			if laterStage {
				if result.State != state.FinishFailed && result.State != state.FinishNeedsAttention {
					t.Fatalf("changed index was published: %+v", result)
				}
				if strings.TrimSpace(runGitOK(t, f.Repo, "rev-parse", "HEAD")) != f.BaseHead || runGitOK(t, f.Repo, "ls-files", "--stage") != indexAtRelease {
					t.Fatal("later staging or branch changed")
				}
			} else {
				if result.State != state.FinishCompleted || runGitOK(t, f.Repo, "show", "HEAD:goal.txt") != "approved staging\n" {
					t.Fatalf("approved selection was not published: %+v", result)
				}
				if _, err := runGit(f.Repo, "diff", "--cached", "--quiet"); err != nil {
					t.Fatal("approved staged selection was not consumed")
				}
				var saved daemon.FinishResult
				if err := json.Unmarshal(result.Result, &saved); err != nil || !saved.StagedConsumed {
					t.Fatalf("staging proof %+v %v", saved, err)
				}
			}
			if body, err := os.ReadFile(filepath.Join(f.Repo, "goal.txt")); err != nil || string(body) != "later unstaged bytes\n" {
				t.Fatalf("live bytes changed: %q %v", body, err)
			}
			f.Barrier(t, t.Context())
		})
	}
}

func TestFinishFaultBranchMovementAndLinkedOwnership(t *testing.T) {
	for _, kind := range []string{"branch moved", "second attached worktree"} {
		t.Run(kind, func(t *testing.T) {
			t.Parallel()
			entered := make(chan struct{}, 1)
			release := make(chan struct{}, 1)
			defer close(release)
			f := finishIntentFixture(t, func(w http.ResponseWriter, r *http.Request) {
				request := decodeFinishChatRequest(t, r)
				entered <- struct{}{}
				select {
				case <-release:
					writeFinishChatResponse(t, w, request)
				case <-r.Context().Done():
				}
			}, "30s")
			writeFile(t, filepath.Join(f.Repo, "goal.txt"), "frozen\n")
			request := finishStartRPC(t, f, map[string]any{"purpose": "Complete frozen goal", "message": "Complete frozen goal"})
			select {
			case <-entered:
			case <-time.After(15 * time.Second):
				t.Fatal("provider did not start")
			}
			if kind == "branch moved" {
				runGitOK(t, f.Repo, "commit", "--allow-empty", "-m", "External change")
			} else {
				runGitOK(t, f.Repo, "worktree", "add", "--force", filepath.Join(t.TempDir(), "linked"), "main")
			}
			head := strings.TrimSpace(runGitOK(t, f.Repo, "rev-parse", "HEAD"))
			index := runGitOK(t, f.Repo, "ls-files", "--stage")
			release <- struct{}{}
			result := finishWaitRequest(t, f, request.ID)
			if result.State != state.FinishFailed && result.State != state.FinishNeedsAttention {
				t.Fatalf("unsafe publication accepted: %+v", result)
			}
			if strings.TrimSpace(runGitOK(t, f.Repo, "rev-parse", "HEAD")) != head || runGitOK(t, f.Repo, "ls-files", "--stage") != index {
				t.Fatal("user branch/index changed")
			}
			if body, err := os.ReadFile(filepath.Join(f.Repo, "goal.txt")); err != nil || string(body) != "frozen\n" {
				t.Fatalf("live bytes changed: %q %v", body, err)
			}
			f.Barrier(t, t.Context())
			finishAssertSavedViews(t, f, 1)
		})
	}
}

func TestFinishFaultPreparedRestartBeforeAndAfterCAS(t *testing.T) {
	for _, applied := range []bool{false, true} {
		t.Run(map[bool]string{false: "before CAS", true: "after CAS"}[applied], func(t *testing.T) {
			t.Parallel()
			f := newFinishFixture(t)
			oldPID := sqliteScalar(t, f.DBPath, "SELECT pid FROM daemon_state WHERE id=1")
			writeFile(t, filepath.Join(f.Repo, "goal.txt"), "selected\n")
			f.Barrier(t, t.Context())
			off := f.Run(t, t.Context(), "off", "--json")
			if off.ExitCode != 0 {
				t.Fatalf("owned off: %+v", off)
			}
			db, err := state.Open(t.Context(), f.DBPath)
			if err != nil {
				t.Fatal(err)
			}
			worker := daemon.FinishWorker{RepoPath: f.Repo, RepositoryID: f.RepositoryID, DB: db, Config: daemon.FinishWorkerConfig{Mode: "local", Style: "imperative", ConfigDigest: "approved-restart-fixture", AuthorName: "ACD Integration", AuthorEmail: "integration@example.com", VerificationMode: "none", VerificationTimeout: time.Minute}}
			request, created, err := worker.Start(t.Context(), daemon.FinishOptions{Purpose: "Complete restart goal", Message: "Complete restart goal"})
			if err != nil || !created {
				db.Close()
				t.Fatalf("frozen restart request: %+v %v", request, err)
			}
			proof, err := gitpkg.BuildFinishChain(t.Context(), f.Repo, gitpkg.FinishBuildOptions{RequestID: request.ID, Attempt: request.Attempt, WorktreeID: request.Target.WorktreeID, BranchRef: request.Target.BranchRef, ExpectedHead: request.Target.ExpectedHead, TargetTree: request.Target.TargetTree, IndexDigest: request.Target.IndexDigest, ObjectRef: gitpkg.FinishRefPrefix + request.ID + "/prepared", Commits: []gitpkg.FinishCommit{{TreeOID: request.Target.TargetTree, Message: "Complete restart goal", AuthorName: "ACD Integration", AuthorEmail: "integration@example.com"}}})
			if err != nil {
				db.Close()
				t.Fatal(err)
			}
			plan, err := json.Marshal(daemon.FinishSavedPlan{Version: 1, Steps: []daemon.FinishPlanStep{{Goal: finish.Goal{Purpose: request.Target.Purpose, Message: request.Target.Message, ChangeIDs: []string{"goal.txt"}}, TreeOID: request.Target.TargetTree, Author: "author-1"}}})
			if err != nil {
				db.Close()
				t.Fatal(err)
			}
			if _, err := state.PrepareFinishPublication(t.Context(), db, request.ID, request.Attempt, proof, plan); err != nil {
				db.Close()
				t.Fatalf("prepare: target=%+v proof=%+v err=%v", request.Target, proof, err)
			}
			if applied {
				if _, err := gitpkg.ApplyFinishPublication(t.Context(), f.Repo, proof, false); err != nil {
					db.Close()
					t.Fatal(err)
				}
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			writeFile(t, filepath.Join(f.Repo, "goal.txt"), "later live bytes\n")
			on := f.Run(t, t.Context(), "on", "--json")
			if on.ExitCode != 0 {
				t.Fatalf("owned startup recovery: %+v", on)
			}
			f.Barrier(t, t.Context())
			saved := finishWaitRequest(t, f, request.ID)
			if saved.State != state.FinishCompleted || saved.PlanningCalls != 0 || saved.Attempt != 1 || saved.Target != request.Target {
				t.Fatalf("prepared recovery %+v", saved)
			}
			if strings.TrimSpace(runGitOK(t, f.Repo, "rev-parse", "HEAD")) != proof.NewHead || commitCount(t, f.Repo) != 2 {
				t.Fatal("prepared chain duplicated or omitted")
			}
			digest, err := gitpkg.IndexContentDigest(t.Context(), f.Repo)
			if runGitOK(t, f.Repo, "show", "HEAD:goal.txt") != "selected\n" || err != nil || digest != proof.CleanIndexDigest {
				t.Fatalf("recovery failed exact approved index reconciliation: digest=%s want=%s err=%v", digest, proof.CleanIndexDigest, err)
			}
			if body, err := os.ReadFile(filepath.Join(f.Repo, "goal.txt")); err != nil || string(body) != "later live bytes\n" {
				t.Fatalf("restart restored old bytes: %q %v", body, err)
			}
			if pid := sqliteScalar(t, f.DBPath, "SELECT pid FROM daemon_state WHERE id=1"); pid == oldPID {
				t.Fatal("worker did not restart")
			}
			finishAssertSavedViews(t, f, 1)
		})
	}
}

func TestFinishFaultRealScannerPrivacyAndSymlink(t *testing.T) {
	t.Parallel()
	f := newFinishFixture(t, "ACD_SENSITIVE_GLOBS=")
	outside := t.TempDir()
	writeFile(t, filepath.Join(outside, "secret.txt"), "outside-private\n")
	if err := os.Symlink(outside, filepath.Join(f.Repo, "link")); err != nil {
		t.Fatal(err)
	}
	writeFile(t, filepath.Join(f.Repo, ".env"), "default-sensitive\n")
	writeFile(t, filepath.Join(f.Repo, ".gitignore"), "ignored/\n")
	writeFile(t, filepath.Join(f.Repo, "ignored", "private.txt"), "ignored\n")
	writeFile(t, filepath.Join(f.Repo, ".derivedData-client", "Index.noindex", "private.bin"), "safe-ignore\n")
	writeFile(t, filepath.Join(f.Repo, "goal.txt"), "eligible\n")
	f.Barrier(t, t.Context())
	db := f.OpenDB(t)
	projection, err := state.ReadCheckpointProjection(t.Context(), db.Path(), 1)
	if err != nil || projection.Latest == nil {
		t.Fatalf("checkpoint: %+v %v", projection, err)
	}
	tree := runGitOK(t, f.Repo, "ls-tree", "-r", projection.Latest.TreeOID)
	if !strings.Contains(tree, "120000 blob") || !strings.Contains(tree, "\tlink\n") {
		t.Fatalf("symlink mode absent: %s", tree)
	}
	for _, private := range []string{"secret.txt", ".env", "ignored/private.txt", "private.bin"} {
		if strings.Contains(tree, private) {
			t.Fatalf("excluded bytes entered checkpoint: %s", tree)
		}
	}
	result := finishRunJSON(t, f, "finish", "Complete eligible goal", "--message", "Complete eligible goal")
	if !result.OK || !result.Data.Published {
		t.Fatalf("eligible finish %+v", result)
	}
	if runGitOK(t, f.Repo, "show", "HEAD:link") != outside {
		t.Fatal("symlink target identity changed")
	}
}

func TestFinishFaultExcludedTrackedDeleteAndUnsafeGitProtection(t *testing.T) {
	t.Parallel()
	f := newFinishFixture(t)
	path := ".derivedData-client/Index.noindex/tracked.bin"
	writeFile(t, filepath.Join(f.Repo, path), "tracked private bytes\n")
	runGitOK(t, f.Repo, "add", "-f", path)
	runGitOK(t, f.Repo, "commit", "-q", "-m", "Track generated fixture")
	if err := os.RemoveAll(filepath.Join(f.Repo, ".derivedData-client")); err != nil {
		t.Fatal(err)
	}
	writeFile(t, filepath.Join(f.Repo, "goal.txt"), "eligible goal\n")
	f.Barrier(t, t.Context())
	result := finishRunJSON(t, f, "finish", "Complete eligible goal", "--message", "Complete eligible goal")
	if !result.OK || !result.Data.Published {
		t.Fatalf("finish: %+v", result)
	}
	if runGitOK(t, f.Repo, "show", "HEAD:"+path) != "tracked private bytes\n" {
		t.Fatal("excluded tracked entry became a branch deletion")
	}
	if _, err := os.Stat(filepath.Join(f.Repo, path)); !os.IsNotExist(err) {
		t.Fatalf("finish restored excluded live file: %v", err)
	}
	head := strings.TrimSpace(runGitOK(t, f.Repo, "rev-parse", "HEAD"))
	writeFile(t, filepath.Join(f.Repo, ".git", "MERGE_HEAD"), head+"\n")
	writeFile(t, filepath.Join(f.Repo, "later.txt"), "protected during merge\n")
	f.Barrier(t, t.Context())
	finishWaitProtectedFile(t, f, "later.txt", "protected during merge\n", 5*time.Second)
	rejected := f.Run(t, t.Context(), "finish", "Complete merge goal", "--json")
	if rejected.ExitCode == 0 || strings.TrimSpace(runGitOK(t, f.Repo, "rev-parse", "HEAD")) != head {
		t.Fatalf("unsafe Git finish advanced: %+v", rejected)
	}
}

func TestFinishFaultDeletedLegacyBranchKeepsCapturedVersions(t *testing.T) {
	t.Parallel()
	f := newFinishLegacyFixture(t, "protected-only")
	f.git(t, f.Repo, "update-ref", "refs/heads/removed", f.SourceHead)
	f.git(t, f.Repo, "update-ref", "-d", "refs/heads/removed", f.SourceHead)
	fixtureDB := f.openSQL(t, false)
	f.execSQL(t, fixtureDB, "UPDATE capture_events SET branch_ref='refs/heads/removed'")
	if err := fixtureDB.Close(); err != nil {
		t.Fatal(err)
	}
	before := f.manifest(t)
	db, err := state.Open(t.Context(), f.DBPath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	cutover, err := migration.ActivateFinish(t.Context(), f.Repo, db)
	if err != nil || cutover.State != "completed" || cutover.Unpublished != 2 {
		t.Fatalf("deleted branch archive: %+v %v", cutover, err)
	}
	after := f.manifest(t)
	if !reflect.DeepEqual(before.Worktrees, after.Worktrees) {
		t.Fatal("activation moved live branch, index, or bytes")
	}
	if !bytes.Equal(before.Tables["capture_events"], after.Tables["capture_events"]) {
		t.Fatal("deleted-branch provenance was retargeted or removed")
	}
	if _, err := runGit(f.Repo, "show-ref", "--verify", "refs/heads/removed"); err == nil {
		t.Fatal("activation recreated deleted branch")
	}
	f.git(t, f.Repo, "reflog", "expire", "--expire=now", "--all")
	f.git(t, f.Repo, "gc", "--prune=now")
	if got := f.git(t, f.Repo, "cat-file", "blob", f.CapturedOnlyBlob); string(got) != "earlier captured bytes\n" {
		t.Fatalf("capture-only bytes were pruned: %q", got)
	}
	f.git(t, f.Repo, "show-ref", "--verify", cutover.SnapshotRef)
}
