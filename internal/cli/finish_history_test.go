package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/config"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/daemon"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/supervisor"
)

func historyCLIPlan(t *testing.T, runtime *workerRuntime, lookup controlRepoLookup, target string) state.IntentHistoryPlan {
	t.Helper()
	for _, field := range config.Catalog() {
		t.Setenv(field.Environment, field.Default)
	}
	repo := runtime.worktree.Root
	writeRewriteTestFile(t, repo, "history.txt", "recorded history\n")
	for _, args := range [][]string{{"add", "history.txt"}, {"commit", "-qm", "Preserve recorded history"}} {
		if _, err := git.Run(t.Context(), git.RunOpts{Dir: repo}, args...); err != nil {
			t.Fatal(err)
		}
	}
	selection, err := git.ResolveRewriteSelection(t.Context(), repo, git.RewriteSelectionOptions{Last: 1})
	if err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	file := filepath.Join(t.TempDir(), "history.json")
	if err := generateFinishHistoryPlan(t.Context(), &out, repo, selection, rewriteCommitsOptions{newBranch: target, planOnly: true, planOut: file}, true); err != nil {
		t.Fatal(err)
	}
	var plan state.IntentHistoryPlan
	if err := json.Unmarshal(out.Bytes(), &plan); err != nil {
		t.Fatal(err)
	}
	loaded, ok, err := readIntentHistoryPlanRef(t.Context(), repo, file)
	if err != nil || !ok || loaded.ID != plan.ID {
		t.Fatalf("saved plan %v", err)
	}
	return plan
}

func TestFinishHistoryRewriteCLICanonicalPreviewApplyAndReadonlyShow(t *testing.T) {
	lookup, runtime, handler := finishCLIFixture(t)
	plan := historyCLIPlan(t, runtime, lookup, "semantic-history")
	lock, err := daemon.AcquireDaemonLock(runtime.worktree.GitDir)
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Release()
	serveIntentHistoryWorker(t, lookup, handler)
	indexBefore, err := os.ReadFile(filepath.Join(runtime.worktree.GitDir, "index"))
	if err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	if err := applyIntentHistoryPlan(t.Context(), &out, runtime.worktree.Root, plan, true); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "every output tree was verified") {
		t.Fatal(out.String())
	}
	if head, _ := git.RevParse(t.Context(), runtime.worktree.Root, "HEAD"); head != plan.ExpectedHead {
		t.Fatal("preview moved branch")
	}
	if _, err := git.RevParse(t.Context(), runtime.worktree.Root, plan.TargetBranchRef); err == nil {
		t.Fatal("preview created target")
	}
	preview, ok, err := state.LoadFinishHistoryRequest(t.Context(), runtime.db, plan.ID)
	if err != nil || !ok || preview.Status != "completed" || len(preview.Checks) == 0 {
		t.Fatalf("preview checks %+v %v", preview, err)
	}
	before, _ := json.Marshal(preview)
	out.Reset()
	if err := showIntentHistoryPlan(t.Context(), &out, runtime.worktree.Root, plan, true, false); err != nil {
		t.Fatal(err)
	}
	after, _, _ := state.LoadFinishHistoryRequest(t.Context(), runtime.db, plan.ID)
	raw, _ := json.Marshal(after)
	if !bytes.Equal(before, raw) {
		t.Fatal("show mutated request")
	}
	if err := applyIntentHistoryPlan(t.Context(), &out, runtime.worktree.Root, plan, false); err != nil {
		t.Fatal(err)
	}
	completed, _, err := state.LoadFinishHistoryRequest(t.Context(), runtime.db, plan.ID)
	if err != nil || completed.Attempt != 2 || completed.Status != "completed" {
		t.Fatalf("apply %+v %v", completed, err)
	}
	if head, _ := git.RevParse(t.Context(), runtime.worktree.Root, "HEAD"); head != plan.ExpectedHead {
		t.Fatal("new branch moved source")
	}
	if after, err := os.ReadFile(filepath.Join(runtime.worktree.GitDir, "index")); err != nil || !bytes.Equal(indexBefore, after) {
		t.Fatal("index changed")
	}
	// A completed reconnect is provider/config free, but still proves target and backup.
	t.Setenv("ACD_AI_API_KEY", "invalid\nsecret")
	if err := applyIntentHistoryPlan(t.Context(), &out, runtime.worktree.Root, plan, false); err != nil {
		t.Fatal(err)
	}
	if _, err := git.Run(t.Context(), git.RunOpts{Dir: runtime.worktree.Root}, "update-ref", "-d", plan.TargetBranchRef); err != nil {
		t.Fatal(err)
	}
	if err := applyIntentHistoryPlan(t.Context(), &out, runtime.worktree.Root, plan, false); err == nil {
		t.Fatal("deleted completed target silently recreated")
	}
}

func TestFinishHistoryRewriteCLIRejectsUnprovedWorkerStagingAndLegacy(t *testing.T) {
	lookup, runtime, handler := finishCLIFixture(t)
	plan := historyCLIPlan(t, runtime, lookup, "")
	var out bytes.Buffer
	if err := applyIntentHistoryPlan(t.Context(), &out, runtime.worktree.Root, plan, false); err == nil {
		t.Fatal("absent owner accepted")
	}
	if _, ok, _ := state.LoadFinishHistoryRequest(t.Context(), runtime.db, plan.ID); ok {
		t.Fatal("unproved worker wrote request")
	}
	serveIntentHistoryWorker(t, lookup, handler)
	writeRewriteTestFile(t, runtime.worktree.Root, "history.txt", "staged later\n")
	if _, err := git.Run(t.Context(), git.RunOpts{Dir: runtime.worktree.Root}, "add", "history.txt"); err != nil {
		t.Fatal(err)
	}
	if err := applyIntentHistoryPlan(t.Context(), &out, runtime.worktree.Root, plan, false); err == nil {
		t.Fatal("staging accepted without consent")
	}
	if _, ok, _ := state.LoadFinishHistoryRequest(t.Context(), runtime.db, plan.ID); ok {
		t.Fatal("staging failure froze request")
	}
	legacy := plan
	legacy.Version = 1
	file := filepath.Join(t.TempDir(), "legacy.json")
	raw, _ := json.Marshal(legacy)
	if err := os.WriteFile(file, raw, 0600); err != nil {
		t.Fatal(err)
	}
	read, ok, err := readIntentHistoryPlanRef(t.Context(), runtime.worktree.Root, file)
	if err != nil || !ok {
		t.Fatal("legacy plan not inspectable")
	}
	if err := showIntentHistoryPlan(t.Context(), &out, runtime.worktree.Root, read, true, false); err != nil {
		t.Fatal(err)
	}
	if err := applyIntentHistoryPlan(t.Context(), &out, runtime.worktree.Root, read, false); err == nil || !strings.Contains(err.Error(), "legacy") {
		t.Fatalf("legacy apply %v", err)
	}
}

func TestFinishHistoryRewriteCLIStandalonePlanLeavesOlderSchemaUntouched(t *testing.T) {
	lookup, runtime, _ := finishCLIFixture(t)
	if _, err := runtime.db.SQL().ExecContext(context.Background(), "PRAGMA user_version=29"); err != nil {
		t.Fatal(err)
	}
	plan := historyCLIPlan(t, runtime, lookup, "")
	var out bytes.Buffer
	if err := showIntentHistoryPlan(t.Context(), &out, runtime.worktree.Root, plan, true, false); err != nil {
		t.Fatal(err)
	}
	version, err := runtime.db.UserVersion(t.Context())
	if err != nil || version != 29 {
		t.Fatalf("preview migrated %d %v", version, err)
	}
	if _, ok, _ := state.LoadFinishHistoryRequest(t.Context(), runtime.db, plan.ID); ok {
		t.Fatal("standalone plan queued request")
	}
}

func TestFinishHistoryRewriteCLISetupHoldBlocksMutationAndKeepsReads(t *testing.T) {
	lookup, runtime, handler := finishCLIFixture(t)
	plan := historyCLIPlan(t, runtime, lookup, "")
	if err := os.MkdirAll(filepath.Dir(lookup.Roots.SetupPublicationHoldPath()), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(lookup.Roots.SetupPublicationHoldPath(), []byte("owned setup hold"), 0600); err != nil {
		t.Fatal(err)
	}
	params, _ := json.Marshal(finishHistoryParams{Plan: plan})
	for _, method := range []string{"history_save", "history_start"} {
		_, failure := handler.HandleWorkerRequest(t.Context(), supervisor.Request{RepositoryID: lookup.Record.RepositoryID, WorktreeID: lookup.Record.WorktreeID, Method: method, Params: params})
		if failure == nil || failure.Code != "finish_busy" {
			t.Fatalf("hold accepted %s: %+v", method, failure)
		}
	}
	var out bytes.Buffer
	if err := showIntentHistoryPlan(t.Context(), &out, runtime.worktree.Root, plan, true, false); err != nil {
		t.Fatal(err)
	}
	if _, ok, _ := state.LoadFinishHistoryRequest(t.Context(), runtime.db, plan.ID); ok {
		t.Fatal("held request wrote state")
	}
	finishParams, _ := json.Marshal(finishStartParams{Purpose: "complete explicit goal"})
	_, failure := handler.HandleWorkerRequest(t.Context(), supervisor.Request{RepositoryID: lookup.Record.RepositoryID, WorktreeID: lookup.Record.WorktreeID, Method: "finish_start", Params: finishParams})
	if failure == nil || failure.Code != "finish_busy" {
		t.Fatalf("held finish %+v", failure)
	}
	if head, _ := git.RevParse(t.Context(), runtime.worktree.Root, "HEAD"); head != plan.ExpectedHead {
		t.Fatal("hold moved branch")
	}
	if err := os.Remove(lookup.Roots.SetupPublicationHoldPath()); err != nil {
		t.Fatal(err)
	}
	_, failure = handler.HandleWorkerRequest(t.Context(), supervisor.Request{RepositoryID: lookup.Record.RepositoryID, WorktreeID: lookup.Record.WorktreeID, Method: "history_start", Params: params})
	if failure != nil {
		t.Fatalf("released hold %+v", failure)
	}
	runtime.finishJobs.Wait()
	completed, _, err := state.LoadFinishHistoryRequest(t.Context(), runtime.db, plan.ID)
	if err != nil || completed.Status != "completed" {
		t.Fatalf("released history %+v %v", completed, err)
	}
}

func TestFinishHistoryRewriteCLIMessageEditorKeepsMembershipWithoutAI(t *testing.T) {
	lookup, runtime, _ := finishCLIFixture(t)
	plan := historyCLIPlan(t, runtime, lookup, "")
	file := filepath.Join(t.TempDir(), "review.json")
	raw, _ := json.Marshal(plan)
	if err := os.WriteFile(file, raw, 0600); err != nil {
		t.Fatal(err)
	}
	editor := filepath.Join(t.TempDir(), "edit.sh")
	if err := os.WriteFile(editor, []byte("#!/bin/sh\nsed 's/Preserve recorded history/Clarify recorded history/g' \"$1\" > \"$1.new\"\nmv \"$1.new\" \"$1\"\n"), 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("EDITOR", editor)
	var out bytes.Buffer
	if err := editFinishHistoryMessages(t.Context(), &out, runtime.worktree.Root, plan, rewriteCommitsOptions{editPlan: file, editFormat: "text", planOnly: true}, true); err != nil {
		t.Fatal(err)
	}
	var edited state.IntentHistoryPlan
	if err := json.Unmarshal(out.Bytes(), &edited); err != nil {
		t.Fatal(err)
	}
	if edited.ID == plan.ID || edited.Goals[0].Message != "Clarify recorded history" || edited.Goals[0].TreeOID != plan.Goals[0].TreeOID || edited.Goals[0].AuthorOID != plan.Goals[0].AuthorOID {
		t.Fatalf("edited %+v", edited)
	}
	if err := daemon.ValidateIntentHistoryPlan(t.Context(), runtime.worktree.Root, edited); err != nil {
		t.Fatal(err)
	}
}
