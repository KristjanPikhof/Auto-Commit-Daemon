package cli

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/daemon"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

func TestFinishCLIExplainsLaterEditsAndNextFinish(t *testing.T) {
	rec, _, worker := finishStatusFixture(t)
	lookup, err := loadControlRepo(t.Context(), rec.Path)
	if err != nil {
		t.Fatal(err)
	}
	finishStatusWrite(t, rec.Path, "goal.txt", "completed goal\n")
	request, _, err := worker.Start(t.Context(), daemon.FinishOptions{Purpose: "Complete the first goal"})
	if err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{"goal.txt", "version.txt", "docs/handoff.md", "docs/typing.md"} {
		finishStatusWrite(t, rec.Path, path, "later work\n")
	}
	completed, err := worker.Execute(t.Context(), request.ID)
	if err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	if err := renderFinish(&out, lookup, completed, false, ""); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"Created 1 local Git commit.", "Uncommitted files remaining: 4.", "snapshot saved at", "Later edits", "git status --short", "when the remaining work is ready"} {
		if !strings.Contains(out.String(), want) {
			t.Fatalf("finish omitted %q:\n%s", want, out.String())
		}
	}
	if tree := finishStatusGit(t, rec.Path, "rev-parse", "HEAD^{tree}"); tree != request.Target.TargetTree {
		t.Fatal("publication did not match the complete frozen target")
	}
	if body := finishStatusGit(t, rec.Path, "show", "HEAD:goal.txt"); body != "completed goal" {
		t.Fatal("first finish absorbed later edits")
	}
	out.Reset()
	if err := renderFinish(&out, lookup, completed, true, ""); err != nil {
		t.Fatal(err)
	}
	var response struct{ Data finishCLIResult }
	if err := json.Unmarshal(out.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	if response.Data.WorktreeChanges != 4 || response.Data.SnapshotSavedAt == "" {
		t.Fatalf("JSON omitted remaining files or snapshot time: %s", out.String())
	}
	next, _, err := worker.Start(t.Context(), daemon.FinishOptions{Purpose: "Complete the later work"})
	if err != nil {
		t.Fatal(err)
	}
	completed, err = worker.Execute(t.Context(), next.ID)
	if err != nil {
		t.Fatal(err)
	}
	if count, err := productWorktreeChangeCount(t.Context(), rec.Path); err != nil || count != 0 {
		t.Fatalf("next finish left work behind: count=%d err=%v", count, err)
	}
	out.Reset()
	if err := renderFinish(&out, lookup, completed, false, ""); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "Working copy: clean.") || strings.Contains(out.String(), "Uncommitted files remaining") || strings.Contains(out.String(), "Later edits") {
		t.Fatalf("clean worktree reported remaining work:\n%s", out.String())
	}
}

func TestFinishCLICollapsesRepeatedCheckSummaries(t *testing.T) {
	lookup, _, _ := finishCLIFixture(t)
	steps := make([]daemon.FinishPlanStep, 4)
	for i := range steps {
		steps[i].Check.Policy = "none"
	}
	plan, err := json.Marshal(daemon.FinishSavedPlan{Steps: steps})
	if err != nil {
		t.Fatal(err)
	}
	request := state.FinishRequest{State: state.FinishCompleted, Plan: plan, Result: json.RawMessage(`{"commits_created":4}`)}
	var out bytes.Buffer
	if err := renderFinish(&out, lookup, request, false, ""); err != nil {
		t.Fatal(err)
	}
	if strings.Count(out.String(), "Project checks: skipped (none configured).") != 1 || !strings.Contains(out.String(), "Created 4 local Git commits.") {
		t.Fatalf("repeated checks obscured the result:\n%s", out.String())
	}
	out.Reset()
	if err := renderFinish(&out, lookup, request, true, ""); err != nil {
		t.Fatal(err)
	}
	var response struct{ Data finishCLIResult }
	if err := json.Unmarshal(out.Bytes(), &response); err != nil || len(response.Data.Verification) != 4 {
		t.Fatalf("JSON lost individual check results: %s %v", out.String(), err)
	}
}
