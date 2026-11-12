package daemon

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/ai"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/finish"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/verification"
)

func sourceWorkerFixture(t *testing.T, consumerFirst bool) (*captureFixture, *FinishWorker, string) {
	t.Helper()
	f, w := finishWorkerFixture(t)
	finishWrite(t, f.dir, "go.mod", "module camera.example\n\ngo 1.27\n")
	finishWrite(t, f.dir, "camera.go", "package camera\ntype Status struct { Running bool }\nfunc Selected(s Status) string { return \"\" }\n")
	finishWrite(t, f.dir, "camera_test.go", "package camera\nimport \"testing\"\nfunc TestSelected(t *testing.T) { if Selected(Status{}) != \"\" { t.Fatal(\"unexpected\") } }\n")
	finishWorkerGit(t, f.dir, "add", "go.mod", "camera.go", "camera_test.go")
	finishWorkerGit(t, f.dir, "commit", "-m", "Seed camera selection")
	final := "package camera\ntype Status struct { Running bool; CameraID string }\nfunc Selected(s Status) string { return s.CameraID }\n"
	finishWrite(t, f.dir, "camera.go", final)
	finishWrite(t, f.dir, "camera_test.go", "package camera\nimport \"testing\"\nfunc TestCameraIdentity(t *testing.T) { if (Status{CameraID: \"camera-A\"}).CameraID != \"camera-A\" { t.Fatal(\"identity\") } }\nfunc TestSelected(t *testing.T) { if Selected(Status{CameraID: \"camera-A\"}) != \"camera-A\" { t.Fatal(\"wrong selection\") } }\n")
	w.Config.Mode, w.Config.VerificationMode = "intent", "fast"
	w.Config.Provider.DiffEgress = true
	w.Config.VerificationTimeout = time.Minute
	command, err := verification.NewApprovedCommand(f.dir, w.Config.ConfigDigest, verification.ModeFast, "GOWORK=off go test ./...", 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	w.Config.Command = command
	w.Transport = &finishTestTransport{exchange: func(_ context.Context, request ai.FinishPlanRequest) ([]byte, error) {
		if len(request.SourceEdits) == 0 {
			t.Fatal("coarse snapshot did not offer source selections")
		}
		goals := []finish.Goal{
			{Purpose: "Expose camera identity in status", Boundary: "Provide identity before its consumer", Reason: "Controls need the active camera identity to synchronize their selection.", Message: "Expose camera identity in status\n\n- Controls need the active camera identity to synchronize their\n  selection."},
			{Purpose: "Synchronize selected camera", Boundary: "Consume the available status field", Reason: "Read the active camera identity so controls follow the running source.", Message: "Synchronize selected camera\n\n- Read the active camera identity so controls follow the running source."},
		}
		for _, edit := range request.SourceEdits {
			text := ""
			for _, diff := range request.Diffs {
				for _, line := range strings.Split(diff.Text, "\n") {
					if strings.HasPrefix(line, "["+edit.ID+"] ") {
						text = line
					}
				}
			}
			if text == "" {
				t.Fatal("source selection has no approved text evidence")
			}
			group := 0
			if strings.Contains(text, "Selected") {
				group = 1
			}
			goals[group].ChangeIDs = append(goals[group].ChangeIDs, edit.ID)
		}
		if consumerFirst {
			goals[0], goals[1] = goals[1], goals[0]
		}
		return json.Marshal(map[string]any{"goals": goals})
	}}
	return f, w, final
}

func TestFinishWorkerSourceSelectionsPublishCheckedSteps(t *testing.T) {
	t.Parallel()
	for _, staged := range []bool{false, true} {
		t.Run(map[bool]string{false: "later work", true: "staged frozen source"}[staged], func(t *testing.T) {
			t.Parallel()
			f, w, final := sourceWorkerFixture(t, false)
			if staged {
				finishWorkerGit(t, f.dir, "add", "camera.go", "camera_test.go")
			}
			r := startFinishWorker(t, w, FinishOptions{Purpose: "Expose camera identity, then synchronize controls", IncludeStaged: staged})
			if !r.Target.SourceSelection || len(r.Target.Checkpoints) != 0 {
				t.Fatalf("new source request was not frozen: %+v", r.Target)
			}
			finishWrite(t, f.dir, "camera.go", final+"// later work\n")
			completed, err := w.Execute(t.Context(), r.ID)
			if err != nil || completed.State != state.FinishCompleted {
				t.Fatalf("source publication: %+v %v", completed, err)
			}
			if head := finishWorkerGit(t, f.dir, "rev-parse", "HEAD^{tree}"); head != r.Target.TargetTree {
				t.Fatal("publication changed the frozen target")
			}
			if live, err := os.ReadFile(filepath.Join(f.dir, "camera.go")); err != nil || string(live) != final+"// later work\n" {
				t.Fatalf("source publication replaced later work: %s %v", live, err)
			}
			index := finishWorkerGit(t, f.dir, "diff", "--cached", "--name-only")
			if index != "" {
				t.Fatalf("staged consent changed: %s", index)
			}
			var saved FinishSavedPlan
			if err := json.Unmarshal(completed.Plan, &saved); err != nil || saved.Source == nil || len(saved.Steps) != 2 || len(saved.Source.Files) != 2 {
				t.Fatalf("source proof missing: %+v %v", saved, err)
			}
			for _, step := range saved.Steps {
				if !step.Check.Executed || step.Check.Result.Status != verification.StatusPassed || step.Reason == "" {
					t.Fatalf("source candidate was not explained and checked: %+v", step)
				}
			}
		})
	}
}

func TestFinishWorkerSourceSelectionDoesNotWaiveStagedConsent(t *testing.T) {
	t.Parallel()
	f, w, _ := sourceWorkerFixture(t, false)
	finishWrite(t, f.dir, "user-staged.txt", "keep staged\n")
	finishWorkerGit(t, f.dir, "add", "user-staged.txt")
	before, err := git.IndexContentDigest(t.Context(), f.dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := w.Start(t.Context(), FinishOptions{Purpose: "Expose camera identity"}); err == nil || !strings.Contains(err.Error(), "staged") {
		t.Fatalf("source selection waived staging consent: %v", err)
	}
	after, err := git.IndexContentDigest(t.Context(), f.dir)
	if err != nil || before != after {
		t.Fatalf("refused source finish changed the index: %s != %s, %v", before, after, err)
	}
}

func TestFinishWorkerRefinesBlockedRecordedChange(t *testing.T) {
	f, w := finishWorkerFixture(t)
	finishWrite(t, f.dir, "tasks.sh", "group() { printf flat; }\n\ncapture() { printf task; }\n")
	finishWrite(t, f.dir, "notes.txt", "initial notes\n")
	finishWorkerGit(t, f.dir, "add", "tasks.sh", "notes.txt")
	finishWorkerGit(t, f.dir, "commit", "-m", "Seed task controls")
	w.Config.Mode, w.Config.VerificationMode = "intent", "fast"
	w.Config.Provider.DiffEgress = true
	w.Config.Provider.Timeout = time.Minute
	check, err := verification.NewApprovedCommand(f.dir, w.Config.ConfigDigest, verification.ModeFast, "sh -n tasks.sh", 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	w.Config.Command = check

	finishWrite(t, f.dir, "notes.txt", "draft notes\n")
	finishCaptureVersion(t, w)
	finishWrite(t, f.dir, "tasks.sh", "group() { printf active; }\n\ncapture() { printf note; }\n")
	finishWrite(t, f.dir, "notes.txt", strings.Repeat("reviewed notes ", 36_000)+"\n")
	finishCaptureVersion(t, w)
	final := "group() { printf 'active completed'; }\n\ncapture() { printf note; }\n"
	finishWrite(t, f.dir, "tasks.sh", final)
	var reviewCalls atomic.Int32
	var reviewed int
	w.Transport = &finishTestTransport{exchange: func(_ context.Context, request ai.FinishPlanRequest) ([]byte, error) {
		if request.Review != nil {
			reviewCalls.Add(1)
			var observations []map[string]string
			for _, part := range request.Review.Parts {
				observations = append(observations, map[string]string{"id": part.ID, "summary": "Review the supplied task control or operator note evidence."})
			}
			return json.Marshal(map[string]any{"observations": observations})
		}
		if !request.RefineRecorded {
			t.Fatal("recorded source construction was unavailable")
		}
		if request.Correction == nil {
			reviewed = len(request.Reviews)
			for _, change := range request.Changes {
				if change.Path == "tasks.sh" && strings.HasPrefix(change.ID, "checkpoint-") {
					return json.Marshal(map[string]any{"goals": []finish.Goal{}, "blocked_reason": change.ID + " combines task grouping and capture defaults."})
				}
			}
			t.Fatal("initial request lost the mixed checkpoint")
		}
		if reviewed == 0 || len(request.Reviews) != reviewed {
			t.Fatal("correction did not retain the completed reviews")
		}
		goals := []finish.Goal{
			{Purpose: "Group tasks by status", Boundary: "Review the list grouping separately", Reason: "Show active and completed tasks in separate groups.", Message: "Group tasks by status\n\n- Show active and completed tasks in separate groups."},
			{Purpose: "Default capture to note", Boundary: "Review capture defaults independently", Reason: "Open new captures as notes by default.", Message: "Default capture to note\n\n- Open new captures as notes by default."},
			{Purpose: "Update operator notes", Boundary: "Keep the notes separately reviewable", Reason: "Keep the operator notes current for reviewers.", Message: "Update operator notes\n\n- Keep the operator notes current for reviewers."},
		}
		for _, change := range request.Changes {
			if change.Path == "notes.txt" {
				goals[2].ChangeIDs = append(goals[2].ChangeIDs, change.ID)
			}
		}
		for _, diff := range request.Diffs {
			for _, line := range strings.Split(diff.Text, "\n") {
				if !strings.HasPrefix(line, "[") {
					continue
				}
				id, code, _ := strings.Cut(line[1:], "] ")
				group := 0
				if strings.Contains(code, "capture()") {
					group = 1
				}
				goals[group].ChangeIDs = append(goals[group].ChangeIDs, id)
			}
		}
		if len(goals[0].ChangeIDs) == 0 || len(goals[1].ChangeIDs) == 0 || len(goals[2].ChangeIDs) != 2 {
			t.Fatal("correction lost exact selections or unrelated recorded versions")
		}
		return json.Marshal(map[string]any{"goals": goals})
	}}
	r := startFinishWorker(t, w, FinishOptions{})
	if !r.Target.RecordedSourceSelection || r.Target.SourceSelection {
		t.Fatal("request did not freeze recorded source consent")
	}
	completed, err := w.Execute(t.Context(), r.ID)
	if err != nil || completed.State != state.FinishCompleted || completed.PlanningCalls != 2 || completed.EvidenceCalls != int(reviewCalls.Load()) {
		t.Fatalf("refined finish did not publish: %s %v", completed.State, err)
	}
	first := finishWorkerGit(t, f.dir, "show", "HEAD~2:tasks.sh")
	if !strings.Contains(first, "active completed") || !strings.Contains(first, "printf task") {
		t.Fatalf("first commit lost the grouping correction or included capture: %s", first)
	}
	if head := finishWorkerGit(t, f.dir, "rev-parse", "HEAD^{tree}"); head != r.Target.TargetTree {
		t.Fatal("refinement changed the frozen final tree")
	}
	var saved FinishSavedPlan
	if err := json.Unmarshal(completed.Plan, &saved); err != nil || saved.Source == nil || len(saved.Steps) != 3 {
		t.Fatalf("refinement lost its verification proof: %v", err)
	}
	for _, step := range saved.Steps {
		if !step.Check.Executed || step.Check.Result.Status != verification.StatusPassed {
			t.Fatalf("refined candidate did not pass its approved check: %+v", step.Check)
		}
	}
}

func TestFinishWorkerSourceSelectionsRejectBrokenIntermediate(t *testing.T) {
	t.Parallel()
	f, w, final := sourceWorkerFixture(t, true)
	r := startFinishWorker(t, w, FinishOptions{Purpose: "Expose camera identity, then synchronize controls"})
	completed, err := w.Execute(t.Context(), r.ID)
	if err == nil || completed.State != state.FinishFailed || completed.PlanningCalls != 2 || finishWorkerGit(t, f.dir, "rev-parse", "HEAD") != r.Target.ExpectedHead {
		t.Fatalf("broken source candidate was published: %+v %v", completed, err)
	}
	var saved FinishSavedPlan
	if err := json.Unmarshal(completed.Plan, &saved); err != nil || saved.Source == nil || !strings.Contains(saved.Steps[0].Check.Result.Output, "CameraID") {
		t.Fatalf("compiler failure or source proof lost: %+v %v", saved, err)
	}
	for _, step := range saved.Steps {
		if !step.Check.Executed {
			t.Fatal("source correction did not collect the complete candidate feedback")
		}
	}
	ref := git.FinishRefPrefix + r.Target.WorktreeID + "/" + r.ID + "/1/chain"
	if _, err := git.RevParse(t.Context(), f.dir, ref); !errors.Is(err, git.ErrRefNotFound) {
		t.Fatalf("rejected source chain was pinned for publication: %v", err)
	}
	if live, err := os.ReadFile(filepath.Join(f.dir, "camera.go")); err != nil || string(live) != final {
		t.Fatal("failed source planning replaced the working file")
	}
	if got := finishWorkerGit(t, f.dir, "show", r.Target.CheckpointTree+":camera.go"); !strings.Contains(got, "CameraID") {
		t.Fatal("failed intermediate lost the protected target")
	}
}

func TestFinishWorkerSourceSelectionsRepairFailedCheck(t *testing.T) {
	t.Parallel()
	f, w, _ := sourceWorkerFixture(t, true)
	provider := w.Transport.(*finishTestTransport)
	first := provider.exchange
	provider.exchange = func(ctx context.Context, request ai.FinishPlanRequest) ([]byte, error) {
		raw, err := first(ctx, request)
		if request.Correction == nil || err != nil {
			return raw, err
		}
		if !strings.Contains(request.Correction.ValidationError, "CameraID") || finishWorkerGit(t, f.dir, "log", "-1", "--format=%s") != "Seed camera selection" {
			t.Fatal("correction lost the diagnostic or published before all checks passed")
		}
		var reply struct {
			Goals []finish.Goal `json:"goals"`
		}
		if err := json.Unmarshal(raw, &reply); err != nil {
			return nil, err
		}
		reply.Goals[0], reply.Goals[1] = reply.Goals[1], reply.Goals[0]
		return json.Marshal(reply)
	}
	r := startFinishWorker(t, w, FinishOptions{Purpose: "Expose camera identity, then synchronize controls"})
	completed, err := w.Execute(t.Context(), r.ID)
	if err != nil || completed.State != state.FinishCompleted || completed.PlanningCalls != 2 || finishWorkerGit(t, f.dir, "rev-parse", "HEAD^{tree}") != r.Target.TargetTree {
		t.Fatalf("source correction: %+v %v", completed, err)
	}
	var saved FinishSavedPlan
	if err := json.Unmarshal(completed.Plan, &saved); err != nil || len(saved.Steps) != 2 {
		t.Fatalf("corrected source plan: %+v %v", saved, err)
	}
	for _, step := range saved.Steps {
		if !step.Check.Executed || step.Check.Result.Status != verification.StatusPassed {
			t.Fatalf("corrected chain was not checked: %+v", step)
		}
	}
}

func TestFinishWorkerInfersAndSeparatesNewSettingsWithoutContext(t *testing.T) {
	f, w, _ := sourceWorkerFixture(t, false)
	finishWrite(t, f.dir, "settings.go", "package camera\nfunc Settings() string { return \"camera\" }\nfunc RoomControls() string { return GenerateRoom() }\n")
	finishWrite(t, f.dir, "room.go", "package camera\nfunc GenerateRoom() string { return \"generated room\" }\n")
	w.Transport = &finishTestTransport{exchange: func(_ context.Context, request ai.FinishPlanRequest) ([]byte, error) {
		if request.Purpose != "" || len(request.SelectionOrder) == 0 {
			return nil, errors.New("bare finish lost its exclusive source ownership catalogue")
		}
		goals := []finish.Goal{
			{Purpose: "Expose camera identity", Boundary: "Supply identity before its consumer", Reason: "Controls need the active camera identity.", Message: "Expose camera identity\n\n- Controls need the active camera identity."},
			{Purpose: "Synchronize camera selection", Boundary: "Consume the available status field", Reason: "Read status so controls follow the running camera.", Message: "Synchronize camera selection\n\n- Read status so controls follow the running camera."},
			{Purpose: "Organize camera settings", Boundary: "Introduce working Settings without the independent room feature", Reason: "Give camera controls a dedicated settings entry point.", Message: "Organize camera settings\n\n- Give camera controls a dedicated settings entry point."},
			{Purpose: "Add room generation", Boundary: "Keep the optional room feature independently revertible", Reason: "Generate a room when users cannot capture an empty room.", Message: "Add room generation\n\n- Generate a room when users cannot capture an empty room."},
		}
		owners := make(map[string]int)
		for _, diff := range request.Diffs {
			for _, line := range strings.Split(diff.Text, "\n") {
				if !strings.HasPrefix(line, "[") {
					continue
				}
				id, _, _ := strings.Cut(line[1:], "] ")
				owner := 1
				switch diff.ChangeID {
				case "room.go":
					owner = 4
				case "settings.go":
					owner = 3
					if strings.Contains(line, "RoomControls") && request.Correction != nil {
						owner = 4
					}
				default:
					if strings.Contains(line, "Selected") {
						owner = 2
					}
				}
				if request.Correction == nil && owner <= 2 {
					owner = 3 - owner // Recreate an undefined-field intermediate.
				}
				owners[id] = owner
			}
		}
		if request.Correction != nil {
			for _, needed := range []string{"step 1", "step 3", "CameraID", "GenerateRoom"} {
				if !strings.Contains(request.Correction.ValidationError, needed) {
					return nil, errors.New("correction lost a failed intermediate diagnostic: " + needed)
				}
			}
		}
		var runs []map[string]any
		for index, id := range request.SelectionOrder {
			if index+1 == len(request.SelectionOrder) || owners[request.SelectionOrder[index+1]] != owners[id] {
				runs = append(runs, map[string]any{"through_id": id, "goal": owners[id]})
			}
		}
		return json.Marshal(map[string]any{"goals": goals, "ownership": runs})
	}}
	r := startFinishWorker(t, w, FinishOptions{})
	completed, err := w.Execute(t.Context(), r.ID)
	if err != nil || completed.State != state.FinishCompleted || completed.PlanningCalls != 2 || r.Target.Version != state.FinishVersion || r.Target.Purpose != "" {
		t.Fatalf("bare source finish: %+v %v", completed, err)
	}
	var saved FinishSavedPlan
	if err := json.Unmarshal(completed.Plan, &saved); err != nil || len(saved.Steps) != 4 {
		t.Fatalf("source plan lost independent settings and feature goals: %+v %v", saved, err)
	}
	settings := finishWorkerGit(t, f.dir, "show", saved.Steps[2].TreeOID+":settings.go")
	if !strings.Contains(settings, "func Settings()") || strings.Contains(settings, "RoomControls") {
		t.Fatalf("settings were combined with room generation: %s", settings)
	}
	if _, err := git.Run(t.Context(), git.RunOpts{Dir: f.dir}, "cat-file", "-e", saved.Steps[2].TreeOID+":room.go"); err == nil {
		t.Fatal("room implementation entered the Settings commit")
	}
	for _, step := range saved.Steps {
		if !step.Check.Executed || step.Check.Result.Status != verification.StatusPassed {
			t.Fatalf("source candidate was not checked: %+v", step)
		}
	}
}

func TestFinishWorkerPreparedSourceRecoveryNeedsNoPlanner(t *testing.T) {
	t.Parallel()
	f, w, _ := sourceWorkerFixture(t, false)
	r := startFinishWorker(t, w, FinishOptions{Purpose: "Expose camera identity, then synchronize controls"})
	wt, cfg, _, err := w.resolved(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	evidence, base, err := w.evidence(t.Context(), wt, r.Target)
	if err != nil {
		t.Fatal(err)
	}
	planned, err := w.plan(t.Context(), wt, cfg, r, evidence, base)
	if err != nil {
		t.Fatal(err)
	}
	if planned.Checked == nil {
		t.Fatal("source plan was not checked before preparation")
	}
	saved, proof := planned.Checked.saved, planned.Checked.proof
	raw, _ := json.Marshal(saved)
	if _, err := state.PrepareFinishPublication(t.Context(), f.db, r.ID, r.Attempt, proof, raw); err != nil {
		t.Fatal(err)
	}
	if _, err := git.ApplyFinishPublication(t.Context(), f.dir, proof, false, nil); err != nil {
		t.Fatal(err)
	}
	restarted := &FinishWorker{RepoPath: f.dir, DB: f.db, Transport: &finishTestTransport{exchange: func(context.Context, ai.FinishPlanRequest) ([]byte, error) {
		t.Fatal("prepared source recovery called a provider")
		return nil, nil
	}}}
	completed, err := restarted.RecoverPrepared(t.Context())
	if err != nil || completed.State != state.FinishCompleted || completed.Target.SourceSelection != r.Target.SourceSelection {
		t.Fatalf("prepared source recovery: %+v %v", completed, err)
	}
}
