//go:build integration

package integration_test

import (
	"encoding/json"
	"net/http"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/ai"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/config"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/finish"
)

func TestFinishWorkflowSameFileDecisions(t *testing.T) {
	purpose := "Problem: password dispatch needs a Google provider.\n\nDecision: extract dispatch without changing password behavior, then add Google.\n\n" + strings.Repeat("Constraint: keep password callers working at each step.\n", 20)
	var calls atomic.Int32
	var preparedBlob atomic.Value
	preparedBlob.Store("")
	f := finishIntentFixtureWithSettings(t, func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		request := decodeFinishChatRequest(t, r).Plan
		if request.Purpose != purpose || request.Message != "" {
			t.Error("long context changed or disabled splitting")
		}
		goals := []finish.Goal{
			{Purpose: "Prepare password authentication for another provider", Boundary: "Review the behavior-preserving dispatch preparation", Reason: "Prepare dispatch while preserving existing password behavior.", Message: "Extract password authentication\n\n- Prepare dispatch while preserving existing password behavior."},
			{Purpose: "Add Google authentication and its regression coverage", Boundary: "Review Google behavior separately from the refactor", Reason: "Route Google requests without changing password authentication.", Message: "Add Google authentication\n\n- Route Google requests without changing password authentication."},
		}
		for _, change := range request.Changes {
			group := 1
			if change.Path == "auth.sh" && change.After.OID == preparedBlob.Load().(string) {
				group = 0
			}
			goals[group].ChangeIDs = append(goals[group].ChangeIDs, change.ID)
		}
		if len(goals[0].ChangeIDs) == 0 || len(goals[1].ChangeIDs) == 0 {
			t.Error("provider did not receive the recorded same-file decisions")
		}
		response, _ := json.Marshal(map[string]any{"goals": goals})
		writeFinishChatArguments(t, w, string(response))
	}, "30s", map[string]string{config.FieldDiffEgress: "true", config.FieldIntentVerification: "fast", config.FieldVerificationFastCommand: "sh verify.sh", config.FieldVerificationFastTimeout: "5s"}, ai.ConfirmationDiffEgress, ai.ConfirmationVerificationCommand)
	writeFile(t, filepath.Join(f.Repo, "auth.sh"), "authenticate() { printf password; }\n")
	writeFile(t, filepath.Join(f.Repo, "verify.sh"), ". ./auth.sh\ntest \"$(authenticate local)\" = password\n")
	runGitOK(t, f.Repo, "add", "auth.sh", "verify.sh")
	runGitOK(t, f.Repo, "commit", "-m", "Seed password authentication")
	f.BaseHead = strings.TrimSpace(runGitOK(t, f.Repo, "rev-parse", "HEAD"))
	f.Barrier(t, t.Context())
	writeFile(t, filepath.Join(f.Repo, "auth.sh"), "local_auth() { printf password; }\nauthenticate() { local_auth; }\n")
	preparedBlob.Store(strings.TrimSpace(runGitOK(t, f.Repo, "hash-object", "auth.sh")))
	f.Barrier(t, t.Context())
	writeFile(t, filepath.Join(f.Repo, "auth.sh"), "local_auth() { printf password; }\nauthenticate() { case \"$1\" in google) printf google;; *) local_auth;; esac; }\n")
	writeFile(t, filepath.Join(f.Repo, "verify.sh"), ". ./auth.sh\ntest \"$(authenticate local)\" = password\ntest \"$(authenticate google)\" = google\n")
	f.Barrier(t, t.Context())
	if calls.Load() != 0 {
		t.Fatal("protection planned without finish")
	}
	result := decodeFinishWorkflow(t, f.Run(t, t.Context(), "finish", purpose, "--json"))
	if !result.Published || result.CommitsCreated != 2 || calls.Load() != 1 || len(result.Target.Checkpoints) == 0 {
		t.Fatalf("same-file decisions were not published: %+v calls=%d", result, calls.Load())
	}
	commits := strings.Fields(runGitOK(t, f.Repo, "rev-list", "--reverse", f.BaseHead+"..HEAD"))
	if first := runGitOK(t, f.Repo, "show", commits[0]+":auth.sh"); strings.Contains(first, "google") || !strings.Contains(first, "local_auth") {
		t.Fatalf("first commit is not the refactor: %s", first)
	}
	for _, check := range result.Verification {
		if !check.Executed || check.Result.ExitCode != 0 {
			t.Fatalf("intermediate project check missing: %+v", check)
		}
	}
	for _, oid := range commits {
		if message := runGitOK(t, f.Repo, "show", "-s", "--format=%B", oid); !strings.Contains(message, "\n\n- ") {
			t.Fatalf("published message lost its reason: %s", message)
		}
	}
	f.Barrier(t, t.Context())
	assertFinishWorkflowStatus(t, t.Context(), f, true)
}

func TestFinishWorkflowOptionalContextAndExactOverride(t *testing.T) {
	f := newFinishFixture(t)
	purpose := "Problem: reloads reset saved choices.\n\nDecision: keep authored values ahead of defaults.\n\n" + strings.Repeat("Constraint: preserve offline settings and current callers.\n", 20)
	for i, args := range [][]string{{"finish"}, {"finish", purpose}, {"finish", "--message", "Preserve approved work"}} {
		writeFile(t, filepath.Join(f.Repo, "goal.txt"), strings.Repeat("approved work\n", i+1))
		completed := finishRunJSON(t, f, args...)
		if !completed.OK || !completed.Data.Published || completed.Data.CommitsCreated != 1 {
			t.Fatalf("optional context blocked Local finish: %+v", completed)
		}
		message := strings.TrimSpace(runGitOK(t, f.Repo, "log", "-1", "--format=%B"))
		if i == 0 && !strings.Contains(message, "offline commit") {
			t.Fatalf("bare Local finish invented purpose: %s", message)
		}
		if i == 1 && completed.Data.Target.Purpose != purpose {
			t.Fatal("long multiline context was clipped")
		}
		if i == 2 && message != "Preserve approved work" {
			t.Fatalf("exact override changed: %s", message)
		}
	}
	f.Barrier(t, t.Context())
	assertFinishWorkflowStatus(t, t.Context(), f, true)
	listed := runAcd(t, t.Context(), f.Env, "list", "--once", "--json")
	if listed.ExitCode != 0 || strings.Contains(listed.Stdout, "what changed and why") {
		t.Fatalf("list requires context after successful finish: %+v", listed)
	}
}

func TestFinishWorkflowIntentInfersWithoutContext(t *testing.T) {
	var calls atomic.Int32
	f := finishIntentFixtureWithSettings(t, func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		request := decodeFinishChatRequest(t, r).Plan
		if request.Purpose != "" || request.Message != "" {
			t.Error("bare Intent finish added mandatory input")
		}
		var ids []string
		for _, change := range request.Changes {
			ids = append(ids, change.ID)
		}
		goal := finish.Goal{Purpose: "Preserve saved settings", Reason: "Keep authored values ahead of defaults so reloads preserve saved choices.", Message: "Preserve saved settings\n\n- Keep authored values ahead of defaults so reloads preserve saved choices.", ChangeIDs: ids}
		raw, _ := json.Marshal(map[string]any{"goals": []finish.Goal{goal}})
		writeFinishChatArguments(t, w, string(raw))
	}, "30s", map[string]string{config.FieldDiffEgress: "true"}, ai.ConfirmationDiffEgress)
	writeFile(t, filepath.Join(f.Repo, "settings.txt"), "prefer authored values over defaults\n")
	completed := finishRunJSON(t, f, "finish")
	message := runGitOK(t, f.Repo, "log", "-1", "--format=%B")
	if !completed.OK || !completed.Data.Published || calls.Load() != 1 || !strings.Contains(strings.Join(strings.Fields(message), " "), "so reloads preserve saved choices") {
		t.Fatalf("bare Intent finish lost inferred reason: %+v %s", completed, message)
	}
}

func TestFinishWorkflowRepairsCheckedSourceSelections(t *testing.T) {
	var calls atomic.Int32
	f := finishIntentFixtureWithSettings(t, func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		request := decodeFinishChatRequest(t, r).Plan
		goals := []finish.Goal{
			{Purpose: "Expose the active camera identity", Boundary: "Provide identity before its consumer", Reason: "Expose the active identity so controls can follow the camera.", Message: "Expose the active camera identity\n\n- Expose the active identity so controls can follow the camera."},
			{Purpose: "Synchronize camera selection", Boundary: "Consume the available camera identity", Reason: "Read the running camera identity instead of retaining an empty selection.", Message: "Synchronize camera selection\n\n- Read the running camera identity instead of retaining an empty selection."},
		}
		for _, diff := range request.Diffs {
			for _, line := range strings.Split(diff.Text, "\n") {
				if !strings.HasPrefix(line, "[") {
					continue
				}
				id, _, ok := strings.Cut(strings.TrimPrefix(line, "["), "] ")
				if !ok {
					t.Error("source evidence lost its edit identity")
					continue
				}
				group := 0
				if strings.Contains(line, "selected()") {
					group = 1
				}
				goals[group].ChangeIDs = append(goals[group].ChangeIDs, id)
			}
		}
		if len(request.SelectionOrder) != 3 || len(request.SourceEdits) != 0 {
			t.Errorf("expected three exact selections without duplicate coordinates, got %+v", request.SelectionOrder)
		}
		if request.Correction == nil {
			goals[0], goals[1] = goals[1], goals[0]
		} else if !strings.Contains(request.Correction.ValidationError, "step 1") {
			t.Errorf("failed project check was not used in correction: %+v", request.Correction)
		}
		response, _ := json.Marshal(map[string]any{"goals": goals})
		writeFinishChatArguments(t, w, string(response))
	}, "1m", map[string]string{config.FieldIntentVerification: "fast", config.FieldVerificationFastCommand: "sh verify.sh", config.FieldVerificationFastTimeout: "5s"}, ai.ConfirmationVerificationCommand)
	writeFile(t, filepath.Join(f.Repo, "camera.sh"), "selected() { printf none; }\n")
	writeFile(t, filepath.Join(f.Repo, "verify.sh"), ". ./camera.sh\nvalue=$(selected) || exit 1\ntest \"$value\" = none || test \"$value\" = camera\n")
	runGitOK(t, f.Repo, "add", "camera.sh", "verify.sh")
	runGitOK(t, f.Repo, "commit", "-m", "Seed camera controls")
	f.BaseHead = strings.TrimSpace(runGitOK(t, f.Repo, "rev-parse", "HEAD"))
	f.Barrier(t, t.Context())
	final := "active_identity() { printf camera; }\nselected() { active_identity; }\n"
	writeFile(t, filepath.Join(f.Repo, "camera.sh"), final)
	result := decodeFinishWorkflow(t, f.Run(t, t.Context(), "finish", "Expose camera identity, then synchronize controls", "--json"))
	if !result.Published || result.CommitsCreated != 2 || !result.Target.SourceSelection || calls.Load() != 2 || result.PlanningCalls != 2 {
		t.Fatalf("checked source correction was not published: %+v calls=%d", result, calls.Load())
	}
	if got := runGitOK(t, f.Repo, "show", "HEAD:camera.sh"); got != final {
		t.Fatalf("source correction changed final bytes: %q", got)
	}
	for _, check := range result.Verification {
		if !check.Executed || check.Result.ExitCode != 0 {
			t.Fatalf("corrected candidate was not checked: %+v", check)
		}
	}
	f.Barrier(t, t.Context())
	assertFinishWorkflowStatus(t, t.Context(), f, true)
}

func TestFinishWorkflowReviewedSourceMappings(t *testing.T) {
	var reviews, corrections, plans atomic.Int32
	var rejected atomic.Bool
	f := finishIntentFixtureWithSettings(t, func(w http.ResponseWriter, r *http.Request) {
		request := decodeFinishChatRequest(t, r).Plan
		if request.Review != nil {
			reviews.Add(1)
			var observations []map[string]string
			for _, part := range request.Review.Parts {
				summary := "Record the supplied operational conventions."
				if part.ChangeID != "notes.txt" {
					t.Error("selectable code was redundantly reviewed")
				}
				observations = append(observations, map[string]string{"id": part.ID, "summary": summary})
			}
			if !rejected.Swap(true) {
				observations[0]["summary"] = "All tests passed"
			}
			if request.Correction != nil {
				corrections.Add(1)
			}
			raw, _ := json.Marshal(map[string]any{"observations": observations})
			_ = json.NewEncoder(w).Encode(map[string]any{"choices": []any{map[string]any{"message": map[string]any{"tool_calls": []any{map[string]any{"function": map[string]any{"name": "finish_evidence", "arguments": string(raw)}}}}}}})
			return
		}
		plans.Add(1)
		goals := []finish.Goal{
			{Purpose: "Expose the active camera identity", Boundary: "Supply identity before its consumer", Reason: "Expose the active identity so controls can follow the camera.", Message: "Expose the active camera identity\n\n- Expose the active identity so controls can follow the camera."},
			{Purpose: "Synchronize camera selection", Boundary: "Consume the available camera identity", Reason: "Read the running camera identity instead of retaining an empty selection.", Message: "Synchronize camera selection\n\n- Read the running camera identity instead of retaining an empty selection."},
			{Purpose: "Document operational conventions", Boundary: "Keep reference material independently reviewable", Reason: "Make operational conventions available to project contributors.", Message: "Document operational conventions\n\n- Make operational conventions available to project contributors.", ChangeIDs: []string{"notes.txt"}},
		}
		for _, diff := range request.Diffs {
			if diff.ChangeID == "camera.sh" {
				if !strings.Contains(diff.Text, "] +active_identity()") || !strings.Contains(diff.Text, "] +selected()") {
					t.Error("global plan lost exact selectable code")
				}
			} else if diff.Text != "" {
				t.Error("global plan resent the whole recorded reference file")
			}
		}
		for _, diff := range request.Diffs {
			for _, line := range strings.Split(diff.Text, "\n") {
				if !strings.HasPrefix(line, "[") {
					continue
				}
				id, _, _ := strings.Cut(line[1:], "] ")
				group := 1
				if strings.Contains(line, "] +active_identity()") {
					group = 0
				}
				goals[group].ChangeIDs = append(goals[group].ChangeIDs, id)
			}
		}
		owners := make(map[string]int)
		for i := range goals {
			for _, id := range goals[i].ChangeIDs {
				owners[id] = i + 1
			}
			goals[i].ChangeIDs = nil
		}
		var runs []map[string]any
		for i, id := range request.SelectionOrder {
			if i+1 == len(request.SelectionOrder) || owners[request.SelectionOrder[i+1]] != owners[id] {
				runs = append(runs, map[string]any{"through_id": id, "goal": owners[id]})
			}
		}
		raw, _ := json.Marshal(map[string]any{"goals": goals, "ownership": runs})
		writeFinishChatArguments(t, w, string(raw))
	}, "1m", map[string]string{config.FieldIntentVerification: "fast", config.FieldVerificationFastCommand: "sh verify.sh", config.FieldVerificationFastTimeout: "30s"}, ai.ConfirmationVerificationCommand)
	writeFile(t, filepath.Join(f.Repo, "camera.sh"), "selected() { printf none; }\n")
	writeFile(t, filepath.Join(f.Repo, "verify.sh"), ". ./camera.sh\nvalue=$(selected) || exit 1\ntest \"$value\" = none || test \"$value\" = camera\n")
	runGitOK(t, f.Repo, "add", "camera.sh", "verify.sh")
	runGitOK(t, f.Repo, "commit", "-m", "Seed camera controls")
	f.BaseHead = strings.TrimSpace(runGitOK(t, f.Repo, "rev-parse", "HEAD"))
	f.Barrier(t, t.Context())
	writeFile(t, filepath.Join(f.Repo, "camera.sh"), "active_identity() { printf camera; }\nselected() { active_identity; }\n")
	writeFile(t, filepath.Join(f.Repo, "notes.txt"), strings.Repeat("Rule: retain the operational conventions for contributors.\n", 40000))
	completed := decodeFinishWorkflow(t, f.Run(t, t.Context(), "finish", "--json"))
	if !completed.Published || completed.CommitsCreated != 3 || !completed.Target.SourceSelection || completed.EvidenceCalls != int(reviews.Load()) || corrections.Load() != 1 || plans.Load() != 1 {
		t.Fatalf("reviewed source mappings were not published: %+v reviews=%d corrections=%d plans=%d", completed, reviews.Load(), corrections.Load(), plans.Load())
	}
	if first := runGitOK(t, f.Repo, "show", "HEAD~2:camera.sh"); !strings.Contains(first, "active_identity()") || !strings.Contains(first, "printf none") {
		t.Fatalf("first source step lost its independent prerequisite: %s", first)
	}
	for _, check := range completed.Verification {
		if !check.Executed || check.Result.ExitCode != 0 {
			t.Fatalf("reviewed intermediate source was not checked: %+v", check)
		}
	}
	f.Barrier(t, t.Context())
	assertFinishWorkflowStatus(t, t.Context(), f, true)
}

func TestFinishWorkflowRejectsUnexplainedDecision(t *testing.T) {
	var calls atomic.Int32
	f := finishIntentFixtureWithSettings(t, func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		request := decodeFinishChatRequest(t, r).Plan
		var ids []string
		for _, change := range request.Changes {
			ids = append(ids, change.ID)
		}
		// A plausible title and edit list used to pass without a reason.
		arguments, err := json.Marshal(map[string]any{"goals": []finish.Goal{{Purpose: "Preserve verified credentials", Boundary: "The observer and its regression complete one fix", Message: "Preserve verified credentials\n\n- Update the key observer and its regression.", ChangeIDs: ids}}})
		if err != nil {
			t.Error(err)
		}
		writeFinishChatArguments(t, w, string(arguments))
	}, "30s", map[string]string{config.FieldDiffEgress: "true"}, ai.ConfirmationDiffEgress)
	writeFile(t, filepath.Join(f.Repo, "settings.swift"), "if key != oldValue { invalidateConnection() }\n")
	result := f.Run(t, t.Context(), "finish", "Keep the connection when focus writes the unchanged key")
	if result.ExitCode == 0 || !strings.Contains(result.Stdout+result.Stderr, "reason") || calls.Load() != 2 {
		t.Fatalf("unexplained decision was not corrected then refused: %+v calls=%d", result, calls.Load())
	}
	if head := strings.TrimSpace(runGitOK(t, f.Repo, "rev-parse", "HEAD")); head != f.BaseHead {
		t.Fatal("unexplained decision moved the branch")
	}
	f.Barrier(t, t.Context())
	if status := runGitOK(t, f.Repo, "status", "--short"); !strings.Contains(status, "settings.swift") {
		t.Fatalf("rejected decision lost live work: %s", status)
	}
}

func TestFinishWorkflowSharesCodeInEveryPlanningExchange(t *testing.T) {
	var calls atomic.Int32
	f := finishIntentFixture(t, func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		request := decodeFinishChatRequest(t, r)
		if !request.Plan.DiffIncluded || len(request.Plan.Diffs) != len(request.Plan.Changes) {
			t.Error("planning exchange omitted required code evidence")
		}
		text := ""
		for _, diff := range request.Plan.Diffs {
			text += diff.Text
		}
		if !strings.Contains(text, "+const allow = true") || strings.Contains(text, "abcdefgh12345678") || !strings.Contains(text, "[REDACTED_SECRET]") {
			t.Error("planning exchange lost changed code or sent an unredacted credential")
		}
		if request.Plan.Correction == nil {
			writeFinishChatArguments(t, w, `{"goals":[]}`)
			return
		}
		if request.Plan.Purpose == "" {
			request.Plan.Purpose = request.Plan.Message
		}
		writeFinishChatResponse(t, w, request)
	}, "30s")
	writeFile(t, filepath.Join(f.Repo, "policy.go"), "package policy\nconst allow = true\n// api_key=abcdefgh12345678\n")
	f.Barrier(t, t.Context())
	for _, args := range [][]string{
		{"finish", "Allow the completed policy", "--dry-run", "--json"},
		{"finish", "--message", "Allow the completed policy\n\n- Keep the approved work separate from later edits.", "--dry-run", "--json"},
		{"finish", "Allow the completed policy", "--json"},
	} {
		result := decodeFinishWorkflow(t, f.Run(t, t.Context(), args...))
		if result.PlanningCalls != 2 {
			t.Fatalf("correction was not checked: %+v", result)
		}
	}
	if calls.Load() != 6 {
		t.Fatalf("exchanges=%d want=6", calls.Load())
	}
	if status := strings.TrimSpace(runGitOK(t, f.Repo, "status", "--porcelain")); status != "" {
		t.Fatalf("publication changed live work: %s", status)
	}
}
