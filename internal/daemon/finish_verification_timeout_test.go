package daemon

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/ai"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/verification"
)

func TestFinishWorkerReviewSizeKeepsPerCommandTimeout(t *testing.T) {
	t.Parallel()
	f, w := finishSizeFixture(t)
	const commandTimeout = 3 * time.Second
	// Each check fits its approved timeout, but both checks together exceed it.
	// Whole seconds leave room for command startup under the parallel test gate.
	approved, err := verification.NewApprovedCommand(f.dir, w.Config.ConfigDigest, verification.ModeFast, "sleep 2; test -f routes.txt", commandTimeout)
	if err != nil {
		t.Fatal(err)
	}
	w.Config.VerificationMode, w.Config.Command = "fast", approved
	w.Config.VerificationTimeout = commandTimeout
	w.Transport = &finishTestTransport{exchange: func(_ context.Context, request ai.FinishPlanRequest) ([]byte, error) {
		return json.Marshal(map[string]any{"goals": finishSizeGoals(request, true)})
	}}
	r := startFinishWorker(t, w, FinishOptions{})
	completed, err := w.Execute(t.Context(), r.ID)
	if err != nil || completed.State != state.FinishCompleted || completed.PlanningCalls != 1 {
		t.Fatalf("candidate checks shared one command timeout: %+v %v", completed, err)
	}
	var saved FinishSavedPlan
	if err := json.Unmarshal(completed.Plan, &saved); err != nil || len(saved.Steps) != 2 {
		t.Fatalf("checked goals missing: %+v %v", saved, err)
	}
	var elapsed time.Duration
	for _, step := range saved.Steps {
		check := step.Check.Result
		if !step.Check.Executed || check.Status != verification.StatusPassed || check.TimedOut || check.Duration >= commandTimeout {
			t.Fatalf("candidate exceeded its command timeout: %+v", step.Check)
		}
		elapsed += check.Duration
	}
	if elapsed <= commandTimeout {
		t.Fatalf("checks did not exceed one command budget: %v", elapsed)
	}
	if finishWorkerGit(t, f.dir, "rev-list", "--count", r.Target.ExpectedHead+"..HEAD") != "2" || finishWorkerGit(t, f.dir, "rev-parse", "HEAD^{tree}") != r.Target.TargetTree {
		t.Fatal("checked goals did not publish the complete frozen target")
	}
}
