package daemon

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/ai"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/finish"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/verification"
)

func TestFinishWorkerProtectsWorkWhenDecisionsCannotBeExplained(t *testing.T) {
	for _, scenario := range []string{"unexplained edits", "inseparable purposes"} {
		t.Run(scenario, func(t *testing.T) {
			f, w := finishWorkerFixture(t)
			finishWrite(t, f.dir, "settings.swift", "if key != oldValue { invalidateConnection() }\n")
			w.Config.Mode = "intent"
			w.Config.Provider.DiffEgress = true
			w.Transport = &finishTestTransport{exchange: func(_ context.Context, request ai.FinishPlanRequest) ([]byte, error) {
				if scenario == "inseparable purposes" {
					return json.Marshal(map[string]any{
						"blocked_reason":     "The settings.swift version combines unrelated layout and credential behavior; separate versions are unavailable.",
						"blocked_change_ids": []string{request.Changes[0].ID},
					})
				}
				var ids []string
				for _, change := range request.Changes {
					ids = append(ids, change.ID)
				}
				return json.Marshal(map[string]any{"goals": []finish.Goal{{Purpose: "Preserve verified credentials", Message: "Preserve verified credentials\n\n- Update the key observer and its tests.", ChangeIDs: ids}}})
			}}
			r := startFinishWorker(t, w, FinishOptions{Purpose: "Keep the connection verified when focus writes the same key"})
			completed, err := w.Execute(t.Context(), r.ID)
			wantReason := "invalid_provider_plan"
			if scenario == "inseparable purposes" {
				wantReason = "provider_boundaries_unavailable"
			}
			if err == nil || completed.State != state.FinishFailed || completed.ReasonCode != wantReason {
				t.Fatalf("unexplained commit accepted: %+v %v", completed, err)
			}
			if finishWorkerGit(t, f.dir, "rev-parse", "HEAD") != r.Target.ExpectedHead {
				t.Fatal("failed decision planning moved the branch")
			}
			if body := finishWorkerGit(t, f.dir, "show", r.Target.CheckpointTree+":settings.swift"); !strings.Contains(body, "key != oldValue") {
				t.Fatalf("failed planning lost protected work: %s", body)
			}
			if status := finishWorkerGit(t, f.dir, "status", "--short"); !strings.Contains(status, "settings.swift") {
				t.Fatalf("failed planning replaced the working copy: %s", status)
			}
		})
	}
}

func TestFinishWorkerKeepsIndivisibleVersionTogether(t *testing.T) {
	f, w := finishWorkerFixture(t)
	finishWrite(t, f.dir, "tasks.sh", "group() { printf flat; }\ncapture() { printf task; }\n")
	finishWorkerGit(t, f.dir, "add", "tasks.sh")
	finishWorkerGit(t, f.dir, "commit", "-m", "Seed task controls")
	w.Config.Mode, w.Config.VerificationMode = "intent", "none"
	w.Config.Provider.DiffEgress = true

	finishWrite(t, f.dir, "tasks.sh", "group() { printf active; }\ncapture() { printf note; }\n")
	finishCaptureVersion(t, w)
	final := "group() { printf 'active completed'; }\ncapture() { printf note; }\n"
	finishWrite(t, f.dir, "tasks.sh", final)
	finishWrite(t, f.dir, "docs/operations.md", "Restart the relay before reconnecting.\n")

	w.Transport = &finishTestTransport{exchange: func(_ context.Context, request ai.FinishPlanRequest) ([]byte, error) {
		if !request.GroupIndivisibleVersions || !request.AllowPathOwnership || request.RefineRecorded || len(request.SourceEdits) != 0 {
			t.Fatal("unchecked request did not restrict planning to whole versions")
		}
		var mixedID string
		goals := []finish.Goal{
			{Purpose: "Group tasks and default captures to notes", Reason: "Separate active and completed tasks and open dictated captures as notes by default.", Message: "Group tasks and default captures to notes\n\n- Separate active and completed tasks and open dictated captures as\n  notes by default."},
			{Purpose: "Document relay restart order", Boundary: "Keep relay operations independent of task controls", Reason: "Tell operators when to restart the relay so they reconnect in order.", Message: "Document relay restart order\n\n- Tell operators when to restart the relay so they reconnect in order."},
		}
		for _, change := range request.Changes {
			if change.Path == "tasks.sh" && strings.HasPrefix(change.ID, "checkpoint-") {
				mixedID = change.ID
			}
		}
		if mixedID == "" {
			t.Fatal("missing mixed checkpoint")
		}
		if request.Correction == nil {
			return json.Marshal(map[string]any{"blocked_reason": "Cannot guarantee a complete plan or dependency order."})
		}
		if !strings.Contains(request.Correction.ValidationError, "ACD checks coverage and orders recorded prerequisites") {
			t.Fatalf("correction repeated an unsupported boundary refusal: %+v", request.Correction)
		}
		goals[0].Boundary = "The first saved version " + mixedID + " contains both task behaviors"
		return json.Marshal(map[string]any{
			"goals": goals,
			"path_ownership": []map[string]any{
				{"goal": 1, "paths": []string{"tasks.sh"}},
				{"goal": 2, "paths": []string{"docs/"}},
			},
		})
	}}

	request := startFinishWorker(t, w, FinishOptions{})
	completed, err := w.Execute(t.Context(), request.ID)
	if err != nil || completed.State != state.FinishCompleted || completed.PlanningCalls != 2 {
		t.Fatalf("finish did not publish the complete plan: %s %v", completed.State, err)
	}
	var plan FinishSavedPlan
	if err := json.Unmarshal(completed.Plan, &plan); err != nil || len(plan.Steps) != 2 || plan.Source != nil {
		t.Fatalf("finish did not preserve two whole-version steps: %+v: %v", plan.Steps, err)
	}
	for _, step := range plan.Steps {
		if step.Check.Executed || step.Check.Result.Status != verification.StatusPassed {
			t.Fatalf("unexpected project check or failed structural check: %+v", step.Check)
		}
	}
	if head := finishWorkerGit(t, f.dir, "rev-parse", "HEAD^{tree}"); head != request.Target.TargetTree {
		t.Fatal("published tree differs from the saved target")
	}
}

func TestFinishWorkerChecksFieldDefinitionsBeforeConsumers(t *testing.T) {
	for _, consumerFirst := range []bool{false, true} {
		t.Run(map[bool]string{false: "valid prerequisite", true: "broken intermediate"}[consumerFirst], func(t *testing.T) {
			f, w := finishWorkerFixture(t)
			finishWrite(t, f.dir, "go.mod", "module camera.example\n\ngo 1.27\n")
			finishWrite(t, f.dir, "status.go", "package camera\ntype Status struct { Running bool }\n")
			finishWrite(t, f.dir, "controls.go", "package camera\nfunc Selected(s Status) string { return \"\" }\n")
			finishWorkerGit(t, f.dir, "add", "go.mod", "status.go", "controls.go")
			finishWorkerGit(t, f.dir, "commit", "-m", "Seed camera status")
			finishWrite(t, f.dir, "status.go", "package camera\ntype Status struct { Running bool; CameraID string }\n")
			finishWrite(t, f.dir, "controls.go", "package camera\nfunc Selected(s Status) string { return s.CameraID }\n")
			w.Config.Mode = "intent"
			w.Config.Provider.DiffEgress = true
			w.Config.VerificationMode = "fast"
			w.Config.VerificationTimeout = time.Minute
			command, err := verification.NewApprovedCommand(f.dir, w.Config.ConfigDigest, verification.ModeFast, "GOWORK=off go test ./...", 30*time.Second)
			if err != nil {
				t.Fatal(err)
			}
			w.Config.Command = command
			w.Transport = &finishTestTransport{exchange: func(_ context.Context, request ai.FinishPlanRequest) ([]byte, error) {
				goals := []finish.Goal{
					{Purpose: "Expose camera identity in status", Boundary: "Provide status data before its consumer", Reason: "Controls need the active camera identity to synchronize their selection.", Message: "Expose camera identity in status\n\n- Controls need the active camera identity to synchronize\n  their selection.", ChangeIDs: []string{"status.go"}},
					{Purpose: "Synchronize the selected camera", Boundary: "Consume the status field after its definition", Reason: "Read the active camera identity so controls follow the running source.", Message: "Synchronize the selected camera\n\n- Read the active camera identity so controls follow the\n  running source.", ChangeIDs: []string{"controls.go"}},
				}
				if len(request.SourceEdits) > 0 {
					goals[0].ChangeIDs, goals[1].ChangeIDs = nil, nil
					for _, edit := range request.SourceEdits {
						group := 0
						if edit.ChangeID == "controls.go" {
							group = 1
						}
						goals[group].ChangeIDs = append(goals[group].ChangeIDs, edit.ID)
					}
				}
				if consumerFirst {
					goals[0], goals[1] = goals[1], goals[0]
				}
				return json.Marshal(map[string]any{"goals": goals})
			}}
			r := startFinishWorker(t, w, FinishOptions{Purpose: "Expose active camera identity and keep controls synchronized"})
			completed, err := w.Execute(t.Context(), r.ID)
			if !consumerFirst {
				if err != nil || completed.State != state.FinishCompleted {
					t.Fatalf("valid prerequisite order rejected: %+v %v", completed, err)
				}
				return
			}
			if err == nil || completed.State != state.FinishFailed || finishWorkerGit(t, f.dir, "rev-parse", "HEAD") != r.Target.ExpectedHead {
				t.Fatalf("undefined field published: %+v %v", completed, err)
			}
			var plan FinishSavedPlan
			if err := json.Unmarshal(completed.Plan, &plan); err != nil {
				t.Fatal(err)
			}
			if len(plan.Steps) == 0 || !strings.Contains(plan.Steps[0].Check.Result.Output, "CameraID undefined") {
				t.Fatalf("missing real compiler failure: %+v", plan)
			}
		})
	}
}
