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
			{Purpose: "Prepare password authentication for another provider", Boundary: "Review the behavior-preserving dispatch preparation", Message: "Extract password authentication\n\n- Prepare dispatch while preserving existing password behavior."},
			{Purpose: "Add Google authentication and its regression coverage", Boundary: "Review Google behavior separately from the refactor", Message: "Add Google authentication\n\n- Route Google requests without changing password authentication."},
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
