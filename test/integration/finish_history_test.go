//go:build integration

package integration_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/ai"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/config"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/finish"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

func TestFinishWorkflowHistorySavedPlanPreservesMembershipAndAuthors(t *testing.T) {
	for _, mode := range []string{"local", "intent"} {
		for _, style := range []string{finish.StyleImperative, finish.StyleConventional} {
			t.Run(mode+"/"+style, func(t *testing.T) {
				ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
				defer cancel()
				pluginDir := t.TempDir()
				calls := filepath.Join(pluginDir, "calls")
				f := newFinishFixture(t, "PATH="+pluginDir+string(os.PathListSeparator)+os.Getenv("PATH"))
				var originals []string
				for index, spec := range []struct{ body, message, author string }{
					{"package example\n\nfunc Stored() int { return 1 }\n", "Record stored settings", "Author One"},
					{"package example\n\nfunc Stored() int { return 2 }\n", "Explore another setting", "Author Two"},
					{"package example\n\nfunc Stored() int { return 1 }\n", "Keep the recorded setting", "Author Two"},
					{"", "Preserve an empty checkpoint", "Author Two"},
				} {
					if index < 3 {
						writeFile(t, filepath.Join(f.Repo, "stored.go"), spec.body)
						runGitOK(t, f.Repo, "add", "stored.go")
					}
					runGitOK(t, f.Repo, "-c", "user.name="+spec.author, "-c", "user.email="+strings.ReplaceAll(spec.author, " ", "")+"@example.test", "commit", "--allow-empty", "-qm", spec.message)
					originals = append(originals, strings.TrimSpace(runGitOK(t, f.Repo, "rev-parse", "HEAD")))
				}
				f.Barrier(t, ctx)
				head := originals[len(originals)-1]
				tree := runGitOK(t, f.Repo, "rev-parse", "HEAD^{tree}")
				index := runGitOK(t, f.Repo, "ls-files", "--stage")
				var goals []finish.Goal
				for position, oid := range originals {
					message := []string{"Preserve stored settings", "Record another setting", "Restore the recorded setting", "Preserve recorded empty history"}[position]
					if style == finish.StyleConventional {
						message = "chore: " + message
					}
					goals = append(goals, finish.Goal{Purpose: "Preserve each recorded history boundary", Boundary: "Keep the original recorded membership", Message: message, ChangeIDs: []string{oid}})
				}
				response, _ := json.Marshal(map[string]any{"version": 1, "finish_plan": map[string]any{"goals": goals}})
				script := fmt.Sprintf("#!/bin/sh\nwhile IFS= read -r line; do printf 'call\\n' >> '%s'; printf '%%s\\n' '%s'; done\n", calls, response)
				if err := os.WriteFile(filepath.Join(pluginDir, "acd-provider-history"), []byte(script), 0o755); err != nil {
					t.Fatal(err)
				}
				f.Apply(t, ctx, map[string]string{config.FieldCommitStrategy: mode, config.FieldCommitFormat: style, config.FieldProvider: "subprocess:history", config.FieldDiffEgress: "true"}, ai.ConfirmationSubprocessExecution, ai.ConfirmationDiffEgress)
				planPath := filepath.Join(t.TempDir(), "history.json")
				var plan state.IntentHistoryPlan
				finishDecodeJSON(t, f.Run(t, ctx, "history", "rewrite", "--last", "4", "--messages-only", "--plan-out", planPath, "--plan-only", "--json"), &plan)
				if len(plan.Goals) != 4 || len(plan.SourceChain) != 4 || !plan.MessagesOnly {
					t.Fatalf("history membership lost: %+v", plan)
				}
				for position, goal := range plan.Goals {
					if goal.AuthorOID != originals[position] || len(goal.OldOIDs) != 1 || goal.OldOIDs[0] != originals[position] {
						t.Fatalf("author/membership changed: %+v", goal)
					}
				}
				// Make the provider unavailable after planning. Saved show,
				// preview, apply and completed reconnect must never invoke it.
				if err := os.Remove(filepath.Join(pluginDir, "acd-provider-history")); err != nil {
					t.Fatal(err)
				}
				var shown state.IntentHistoryPlan
				finishDecodeJSON(t, f.Run(t, ctx, "history", "rewrite", "--show-plan", planPath, "--json"), &shown)
				if shown.ID != plan.ID {
					t.Fatal("show changed plan identity")
				}
				if result := f.Run(t, ctx, "history", "rewrite", "--apply", planPath, "--dry-run"); result.ExitCode != 0 {
					t.Fatalf("saved preview: %+v", result)
				}
				if strings.TrimSpace(runGitOK(t, f.Repo, "rev-parse", "HEAD")) != head || runGitOK(t, f.Repo, "ls-files", "--stage") != index {
					t.Fatal("history preview changed source/index")
				}
				for attempt := 0; attempt < 2; attempt++ {
					if result := f.Run(t, ctx, "history", "rewrite", "--apply", planPath, "--yes"); result.ExitCode != 0 {
						t.Fatalf("saved apply/reconnect: %+v", result)
					}
				}
				if runGitOK(t, f.Repo, "rev-parse", "HEAD^{tree}") != tree || runGitOK(t, f.Repo, "ls-files", "--stage") != index || strings.TrimSpace(runGitOK(t, f.Repo, "rev-parse", plan.SourceRef)) != head {
					t.Fatal("history apply changed original proof/index or final tree")
				}
				rewritten := strings.Fields(runGitOK(t, f.Repo, "rev-list", "--reverse", f.BaseHead+"..HEAD"))
				if len(rewritten) != 4 {
					t.Fatalf("empty/net-neutral commits were removed: %v", rewritten)
				}
				for position, oid := range rewritten {
					if runGitOK(t, f.Repo, "show", "-s", "--format=%an <%ae>%n%aI%n%T", oid) != runGitOK(t, f.Repo, "show", "-s", "--format=%an <%ae>%n%aI%n%T", originals[position]) {
						t.Fatalf("recorded author/date/tree changed at %d", position)
					}
					if err := finish.ValidateMessage(style, strings.TrimSpace(runGitOK(t, f.Repo, "log", "-1", "--format=%B", oid))); err != nil {
						t.Fatal(err)
					}
				}
				if backup := strings.TrimSpace(runGitOK(t, f.Repo, "rev-parse", "refs/acd/rewrite-backups/"+plan.ID)); backup != head {
					t.Fatal("original history backup missing")
				}
				raw, err := os.ReadFile(calls)
				if mode == "intent" {
					if err != nil || strings.Count(string(raw), "call\n") != 1 {
						t.Fatalf("history actual exchanges=%s err=%v", raw, err)
					}
				} else if !os.IsNotExist(err) {
					t.Fatal("Local history called provider")
				}
			})
		}
	}
}

func TestFinishWorkflowHistoryPurposefulGoalsAndStaleRefusal(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	pluginDir := t.TempDir()
	f := newFinishFixture(t, "PATH="+pluginDir+string(os.PathListSeparator)+os.Getenv("PATH"))
	for _, spec := range []struct{ path, body string }{
		{"protect.go", "package protect\n\nfunc Saved() bool { return true }\n"},
		{"protect_test.go", "package protect\n\n// Saved requires retained protection.\n"},
	} {
		writeFile(t, filepath.Join(f.Repo, spec.path), spec.body)
		runGitOK(t, f.Repo, "add", spec.path)
		runGitOK(t, f.Repo, "commit", "-qm", "Record protection changes")
	}
	f.Barrier(t, ctx)
	response := `{"version":1,"finish_plan":{"goals":[{"purpose":"Require retained protection","message":"Require retained protection\n\n- Keep protection behavior and its direct test together","change_ids":["1","2"]}]}}`
	if err := os.WriteFile(filepath.Join(pluginDir, "acd-provider-purpose"), []byte("#!/bin/sh\nwhile IFS= read -r line; do printf '%s\\n' '"+response+"'; done\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	f.Apply(t, ctx, map[string]string{config.FieldCommitStrategy: "intent", config.FieldProvider: "subprocess:purpose", config.FieldDiffEgress: "true"}, ai.ConfirmationSubprocessExecution, ai.ConfirmationDiffEgress)
	planPath := filepath.Join(t.TempDir(), "purpose.json")
	var plan state.IntentHistoryPlan
	finishDecodeJSON(t, f.Run(t, ctx, "history", "rewrite", "--last", "2", "--new-branch", "purposeful", "--plan-only", "--plan-out", planPath, "--json"), &plan)
	if len(plan.Goals) != 1 || plan.SharedPlan == nil || len(plan.Goals[0].Units) != 2 {
		t.Fatalf("related implementation/test saves did not form a goal: %+v", plan)
	}
	if result := f.Run(t, ctx, "history", "rewrite", "--apply", planPath, "--yes"); result.ExitCode != 0 {
		t.Fatalf("purposeful saved apply: %+v", result)
	}
	if count := strings.TrimSpace(runGitOK(t, f.Repo, "rev-list", "--count", f.BaseHead+"..purposeful")); count != "1" {
		t.Fatalf("purposeful history did not create one complete goal: %s", count)
	}
	if runGitOK(t, f.Repo, "rev-parse", "HEAD^{tree}") != runGitOK(t, f.Repo, "rev-parse", "purposeful^{tree}") {
		t.Fatal("purposeful rewrite changed final contents")
	}
	finishDecodeJSON(t, f.Run(t, ctx, "history", "rewrite", "--last", "2", "--new-branch", "stale-purpose", "--plan-only", "--plan-out", planPath, "--json"), &plan)
	// A real later manual commit makes this frozen plan stale. The reviewed
	// plan must stay saved and original Git objects must remain reachable.
	writeFile(t, filepath.Join(f.Repo, "later.txt"), "later independent work\n")
	runGitOK(t, f.Repo, "add", "later.txt")
	runGitOK(t, f.Repo, "commit", "-qm", "Preserve later work")
	head := runGitOK(t, f.Repo, "rev-parse", "HEAD")
	result := f.Run(t, ctx, "history", "rewrite", "--apply", planPath, "--yes")
	if result.ExitCode == 0 || runGitOK(t, f.Repo, "rev-parse", "HEAD") != head {
		t.Fatalf("stale apply accepted or changed HEAD: %+v", result)
	}
	if pin := strings.TrimSpace(runGitOK(t, f.Repo, "rev-parse", plan.SourceRef)); pin != plan.ExpectedHead {
		t.Fatal("stale plan lost its original source proof")
	}
	var saved state.IntentHistoryPlan
	finishDecodeJSON(t, f.Run(t, ctx, "history", "rewrite", "--show-plan", planPath, "--json"), &saved)
	if saved.ID != plan.ID {
		t.Fatal("refused apply mutated saved plan")
	}
}
