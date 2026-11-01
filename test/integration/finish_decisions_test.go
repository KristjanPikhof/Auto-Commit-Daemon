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
	var calls atomic.Int32
	var preparedBlob atomic.Value
	preparedBlob.Store("")
	f := finishIntentFixtureWithSettings(t, func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		request := decodeFinishChatRequest(t, r).Plan
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
	result := decodeFinishWorkflow(t, f.Run(t, t.Context(), "finish", "Prepare dispatch, then add Google while preserving password behavior", "--json"))
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

func TestFinishWorkflowRequiresExplanation(t *testing.T) {
	f := newFinishFixture(t)
	writeFile(t, filepath.Join(f.Repo, "goal.txt"), "approved work\n")
	failed := finishRunJSON(t, f, "finish")
	if failed.OK || failed.Data.ReasonCode != "finish_context_required" || failed.Data.Published {
		t.Fatalf("Local published without an explanation: %+v", failed)
	}
	exact := f.Run(t, t.Context(), "finish", "--message", "Preserve approved work")
	if exact.ExitCode == 0 || !strings.Contains(exact.Stdout+exact.Stderr, "body") || strings.TrimSpace(runGitOK(t, f.Repo, "rev-parse", "HEAD")) != f.BaseHead {
		t.Fatalf("exact message bypassed the reason requirement: %+v", exact)
	}
	f.Barrier(t, t.Context())
	for _, command := range []string{"status", "list"} {
		var result ExecResult
		if command == "list" {
			result = runAcd(t, t.Context(), f.Env, "list", "--once", "--json")
		} else {
			result = f.Run(t, t.Context(), command, "--json")
		}
		if result.ExitCode != 0 || !strings.Contains(result.Stdout, "what changed and why") || strings.Contains(result.Stdout, "--retry") {
			t.Fatalf("%s lost saved work or requested an unchanged retry: %+v", command, result)
		}
	}
	purpose := "Keep approved work separate from later edits"
	completed := finishRunJSON(t, f, "finish", purpose)
	if !completed.OK || !completed.Data.Published || completed.Data.CommitsCreated != 1 || !strings.Contains(runGitOK(t, f.Repo, "log", "-1", "--format=%B"), purpose) {
		t.Fatalf("explained Local finish did not publish its reason: %+v", completed)
	}
	f.Barrier(t, t.Context())
	assertFinishWorkflowStatus(t, t.Context(), f, true)
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
		if len(request.SourceEdits) != 3 {
			t.Errorf("expected the complete three-edit catalogue, got %+v", request.SourceEdits)
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
