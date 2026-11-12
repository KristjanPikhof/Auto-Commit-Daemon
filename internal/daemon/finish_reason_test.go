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
					return []byte(`{"goals":[],"blocked_reason":"The settings.swift version combines unrelated layout and credential behavior; separate versions are unavailable."}`), nil
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
