package daemon

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/ai"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/finish"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/verification"
)

func finishSizeFixture(t *testing.T) (*captureFixture, *FinishWorker) {
	t.Helper()
	f, w := finishWorkerFixture(t)
	w.Config.Mode = "intent"
	w.Config.Provider.CommitStrategy = ai.CommitStrategyIntent
	w.Config.Provider.DiffEgress = true
	w.Config.Provider.Timeout = time.Minute
	w.Config.VerificationTimeout = time.Minute
	for _, name := range []string{"routes.txt", "errors.txt"} {
		var body strings.Builder
		for index := 0; index < 120; index++ {
			fmt.Fprintf(&body, "%s entry %d\n", name, index)
		}
		finishWrite(t, f.dir, name, body.String())
	}
	return f, w
}

func finishSizeGoals(request ai.FinishPlanRequest, split bool) []finish.Goal {
	goals := []finish.Goal{{Purpose: "Add routing and error reference tables", Boundary: "Keep the supplied reference tables available to readers", Reason: "Supply routing and error entries so clients can resolve protocol values.", Message: "Add protocol reference tables\n\n- Supply routing and error entries so clients can resolve protocol values."}}
	if split {
		goals = []finish.Goal{
			{Purpose: "Add the route lookup table", Boundary: "Review route lookup independently from error descriptions", Reason: "Supply route entries so clients can resolve protocol route values.", Message: "Add route lookup entries\n\n- Supply route entries so clients can resolve protocol route values."},
			{Purpose: "Add the error description table", Boundary: "Review error descriptions independently from route lookup", Reason: "Supply error descriptions so clients can explain rejected protocol requests.", Message: "Add protocol error descriptions\n\n- Supply error descriptions so clients can explain rejected protocol\n  requests."},
		}
	}
	for _, change := range request.Changes {
		index := 0
		if split && change.Path == "errors.txt" {
			index = 1
		}
		goals[index].ChangeIDs = append(goals[index].ChangeIDs, change.ID)
	}
	return goals
}

func TestFinishWorkerReviewSizeCorrectsOversizedRecordedPlan(t *testing.T) {
	t.Parallel()
	f, w := finishSizeFixture(t)
	checks := filepath.Join(t.TempDir(), "checks")
	command := fmt.Sprintf("for file in routes.txt errors.txt; do if [ -f \"$file\" ]; then test \"$(wc -l < \"$file\" | tr -d ' ')\" = 120 || exit 1; fi; done; printf 'checked\\n' >> %q", checks)
	approved, err := verification.NewApprovedCommand(f.dir, w.Config.ConfigDigest, verification.ModeFast, command, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	w.Config.VerificationMode, w.Config.Command = "fast", approved
	w.Transport = &finishTestTransport{exchange: func(_ context.Context, request ai.FinishPlanRequest) ([]byte, error) {
		if request.ReviewLineTarget != ai.FinishReviewLineTarget {
			t.Fatal("new request omitted its frozen review target")
		}
		if request.Correction != nil {
			if !strings.Contains(request.Correction.ValidationError, "240") || !strings.Contains(request.Correction.ValidationError, "200") {
				t.Fatalf("correction lost actual candidate size: %+v", request.Correction)
			}
			if finishWorkerGit(t, f.dir, "log", "-1", "--format=%s") == "Add protocol reference tables" {
				t.Fatal("oversized proposal was published before correction")
			}
		}
		return json.Marshal(map[string]any{"goals": finishSizeGoals(request, request.Correction != nil)})
	}}
	r := startFinishWorker(t, w, FinishOptions{})
	finishWrite(t, f.dir, "later.txt", "next request\n")
	completed, err := w.Execute(t.Context(), r.ID)
	if err != nil || completed.State != state.FinishCompleted || completed.PlanningCalls != 2 {
		t.Fatalf("oversized plan was not corrected: %+v %v", completed, err)
	}
	var saved FinishSavedPlan
	if err := json.Unmarshal(completed.Plan, &saved); err != nil || len(saved.Steps) != 2 {
		t.Fatalf("two useful steps missing: %+v %v", saved, err)
	}
	for _, step := range saved.Steps {
		if step.ReviewSize == nil || *step.ReviewSize != (FinishReviewSize{ChangedLines: 120, ChangedFiles: 1}) || !step.Check.Executed || step.Check.Result.Status != verification.StatusPassed {
			t.Fatalf("candidate size/check missing: %+v", step)
		}
	}
	if finishWorkerGit(t, f.dir, "rev-parse", "HEAD^{tree}") != r.Target.TargetTree || !strings.Contains(finishWorkerGit(t, f.dir, "status", "--short"), "later.txt") {
		t.Fatal("corrected publication changed the frozen target or later work")
	}
	runs, err := os.ReadFile(checks)
	if err != nil || strings.Count(string(runs), "checked") != 3 {
		t.Fatalf("candidate checks repeated after accepted proof: %q %v", runs, err)
	}
}

func TestFinishWorkerReviewSizeRejectsUnexplainedLargePlan(t *testing.T) {
	t.Parallel()
	f, w := finishSizeFixture(t)
	w.Transport = &finishTestTransport{exchange: func(_ context.Context, request ai.FinishPlanRequest) ([]byte, error) {
		return json.Marshal(map[string]any{"goals": finishSizeGoals(request, false)})
	}}
	r := startFinishWorker(t, w, FinishOptions{})
	failed, err := w.Execute(t.Context(), r.ID)
	if err == nil || failed.State != state.FinishFailed || failed.ReasonCode != "invalid_provider_plan" || !strings.Contains(err.Error(), "split review") || finishWorkerGit(t, f.dir, "rev-parse", "HEAD") != r.Target.ExpectedHead {
		t.Fatalf("unexplained oversized plan was accepted: %+v %v", failed, err)
	}
	if failed.PlanningCalls != 1 || failed.PartitionCalls != 2 {
		t.Fatalf("rejection did not retain one global plan and one corrected focused review: %+v", failed)
	}
	ref := git.FinishRefPrefix + r.Target.WorktreeID + "/" + r.ID + "/1/chain"
	if _, err := git.RevParse(t.Context(), f.dir, ref); !errors.Is(err, git.ErrRefNotFound) {
		t.Fatalf("rejected chain was pinned: %v", err)
	}
	if finishWorkerGit(t, f.dir, "rev-parse", r.Target.CheckpointTree) != r.Target.TargetTree {
		t.Fatal("rejected plan lost its protected target")
	}
}

func TestFinishWorkerReviewSizeAllowsExplainedIndivisibleChange(t *testing.T) {
	t.Parallel()
	f, w := finishWorkerFixture(t)
	w.Config.Mode, w.Config.Provider.DiffEgress = "intent", true
	w.Config.Provider.Timeout = time.Minute
	finishWrite(t, f.dir, "compatibility.json", "[\n"+strings.Repeat("  {\"code\": 1},\n", 238)+"  {\"code\": 2}\n]\n")
	const largeReason = "The imported compatibility.json table is one recorded version; partial tables would omit required protocol entries."
	w.Transport = &finishTestTransport{exchange: func(_ context.Context, request ai.FinishPlanRequest) ([]byte, error) {
		goal := finish.Goal{Purpose: "Import the supported protocol compatibility table", Boundary: "Keep the complete versioned compatibility table together", Reason: "Supply the supported protocol entries so clients can recognize compatible values.", LargeChangeReason: largeReason, Message: "Import protocol compatibility entries\n\n- Supply the supported protocol entries so clients can recognize\n  compatible values.\n- " + largeReason}
		for _, change := range request.Changes {
			goal.ChangeIDs = append(goal.ChangeIDs, change.ID)
		}
		return json.Marshal(map[string]any{"goals": []finish.Goal{goal}})
	}}
	r := startFinishWorker(t, w, FinishOptions{})
	completed, err := w.Execute(t.Context(), r.ID)
	if err != nil || completed.State != state.FinishCompleted || completed.PlanningCalls != 1 {
		t.Fatalf("explained indivisible change failed: %+v %v", completed, err)
	}
	var saved FinishSavedPlan
	if err := json.Unmarshal(completed.Plan, &saved); err != nil || len(saved.Steps) != 1 || saved.Steps[0].ReviewSize == nil || saved.Steps[0].ReviewSize.ChangedLines != 241 {
		t.Fatalf("large candidate size missing: %+v %v", saved, err)
	}
	message := finishWorkerGit(t, f.dir, "log", "-1", "--format=%B")
	if !strings.Contains(strings.Join(strings.Fields(message), " "), "partial tables would omit required protocol entries.") {
		t.Fatal("published message lost the large-change explanation")
	}
}

func TestFinishWorkerRetryExplainsAlreadyCheckedLargeSteps(t *testing.T) {
	f, w := finishWorkerFixture(t)
	w.Config.Mode, w.Config.Provider.DiffEgress = "intent", true
	w.Config.Provider.Timeout = time.Minute
	finishWrite(t, f.dir, "routes.txt", strings.Repeat("recorded route\n", 240))
	finishWrite(t, f.dir, "errors.txt", "Describe failed lookups.\n")
	w.Transport = &finishTestTransport{exchange: func(_ context.Context, request ai.FinishPlanRequest) ([]byte, error) {
		return json.Marshal(map[string]any{"goals": finishSizeGoals(request, true)})
	}}
	r := startFinishWorker(t, w, FinishOptions{})
	if _, err := state.InterruptFinishRequests(t.Context(), f.db, r.Target.WorktreeID); err != nil {
		t.Fatal(err)
	}
	target := r.Target
	target.Version = state.FinishPathOwnershipVersion
	r, _, err := state.StartFinishRequest(t.Context(), f.db, state.FinishRequest{ID: "legacy-size-retry", Target: target, DeadlineTS: r.DeadlineTS})
	if err != nil {
		t.Fatal(err)
	}
	failed, err := w.Execute(t.Context(), r.ID)
	if err == nil || failed.ReasonCode != "invalid_provider_plan" ||
		finishWorkerGit(t, f.dir, "rev-parse", "HEAD") != r.Target.ExpectedHead {
		t.Fatalf("unexplained plan was published: %s %v", failed.State, err)
	}

	const reason = "The complete route table is one recorded file version and clients need all supported routes together."
	w.Transport = &finishTestTransport{exchange: func(_ context.Context, request ai.FinishPlanRequest) ([]byte, error) {
		if len(request.ReviewSizes) != 1 || len(request.Changes) != 1 || request.Changes[0].Path != "routes.txt" {
			t.Fatalf("retry did not scope the question to the oversized step: %+v", request.ReviewSizes)
		}
		size := request.ReviewSizes[0]
		if size.Step != 1 || size.ChangedLines != 240 || size.ChangedFiles != 1 {
			t.Fatalf("retry lost measured size: %+v", size)
		}
		return json.Marshal(map[string]any{"reasons": []map[string]any{{"step": size.Step, "reason": reason}}})
	}}
	retried := startFinishWorker(t, w, FinishOptions{RetryID: r.ID})
	completed, err := w.Execute(t.Context(), retried.ID)
	if err != nil || completed.State != state.FinishCompleted || completed.PlanningCalls != 1 ||
		finishWorkerGit(t, f.dir, "rev-parse", "HEAD^{tree}") != r.Target.TargetTree {
		t.Fatalf("retry did not publish the exact checked plan: %s %v", completed.State, err)
	}
	var old, updated FinishSavedPlan
	if json.Unmarshal(failed.Plan, &old) != nil || json.Unmarshal(completed.Plan, &updated) != nil ||
		len(old.Steps) != 2 || len(updated.Steps) != 2 {
		t.Fatal("retry lost the original commit boundaries")
	}
	for i, step := range updated.Steps {
		if step.TreeOID != old.Steps[i].TreeOID || step.Check.Result.Status != verification.StatusPassed {
			t.Fatalf("retry changed the tree or skipped checking step %d", i+1)
		}
	}
	message := finishWorkerGit(t, f.dir, "log", "-1", "--format=%B", "HEAD^")
	if !strings.Contains(strings.Join(strings.Fields(message), " "), reason) {
		t.Fatal("published commit body is missing the measured size explanation")
	}
}

func TestFinishWorkerSplitsLargeGroupDespiteExplanation(t *testing.T) {
	f, w := finishSizeFixture(t)
	w.Transport = &finishTestTransport{exchange: func(_ context.Context, request ai.FinishPlanRequest) ([]byte, error) {
		goals := finishSizeGoals(request, request.Partition != nil)
		if request.Partition == nil {
			goals[0].LargeChangeReason = "The protocol implementation and its reference entries need to change together for callers."
			goals[0].Message += "\n- " + goals[0].LargeChangeReason
		}
		return json.Marshal(map[string]any{"goals": goals})
	}}
	r := startFinishWorker(t, w, FinishOptions{})
	completed, err := w.Execute(t.Context(), r.ID)
	if err != nil || completed.State != state.FinishCompleted || completed.PartitionCalls != 1 {
		t.Fatalf("focused split did not finish: %s %s %v", completed.State, completed.Reason, err)
	}
	var saved FinishSavedPlan
	if json.Unmarshal(completed.Plan, &saved) != nil || len(saved.Steps) != 2 {
		t.Fatal("large explanation bypassed the useful split")
	}
	if got := finishWorkerGit(t, f.dir, "rev-parse", "HEAD^{tree}"); got != r.Target.TargetTree {
		t.Fatal("focused splitting changed the saved target")
	}
}

func TestFinishWorkerReviewSizePreservesLegacyAndExactOverrides(t *testing.T) {
	t.Parallel()
	for _, kind := range []string{"legacy", "exact"} {
		t.Run(kind, func(t *testing.T) {
			f, w := finishSizeFixture(t)
			w.Transport = &finishTestTransport{exchange: func(_ context.Context, request ai.FinishPlanRequest) ([]byte, error) {
				if request.ReviewLineTarget != 0 {
					t.Fatal("old request or exact override acquired a new size policy")
				}
				goals := finishSizeGoals(request, false)
				if request.Message != "" {
					goals[0].Message = request.Message
				}
				return json.Marshal(map[string]any{"goals": goals})
			}}
			options := FinishOptions{}
			if kind == "exact" {
				options.Message = "Import approved protocol tables"
			}
			r := startFinishWorker(t, w, options)
			if kind == "legacy" {
				if _, err := state.InterruptFinishRequests(t.Context(), f.db, r.Target.WorktreeID); err != nil {
					t.Fatal(err)
				}
				target := r.Target
				target.Version = 4
				var created bool
				var err error
				r, created, err = state.StartFinishRequest(t.Context(), f.db, state.FinishRequest{ID: "legacy-large-plan", Target: target, DeadlineTS: r.DeadlineTS})
				if err != nil || !created {
					t.Fatalf("legacy request: %+v %v", r, err)
				}
			}
			completed, err := w.Execute(t.Context(), r.ID)
			if err != nil || completed.State != state.FinishCompleted || completed.PlanningCalls != 1 {
				t.Fatalf("existing contract changed: %+v %v", completed, err)
			}
			var saved FinishSavedPlan
			if err := json.Unmarshal(completed.Plan, &saved); err != nil || len(saved.Steps) != 1 || saved.Steps[0].ReviewSize != nil {
				t.Fatalf("existing contract gained size evidence: %+v %v", saved, err)
			}
		})
	}
}

func TestFinishWorkerReviewSizeUsesNetRecordedTree(t *testing.T) {
	t.Parallel()
	f, w := finishWorkerFixture(t)
	finishWrite(t, f.dir, "lookup.txt", "original entry\n")
	finishWorkerGit(t, f.dir, "add", "lookup.txt")
	finishWorkerGit(t, f.dir, "commit", "-m", "Seed lookup table")
	w.Config.Mode, w.Config.Provider.DiffEgress = "intent", true
	w.Config.Provider.Timeout = time.Minute
	finishWrite(t, f.dir, "lookup.txt", strings.Repeat("unfinished entry\n", 300))
	finishCaptureVersion(t, w)
	finishWrite(t, f.dir, "lookup.txt", "final entry\n")
	w.Transport = &finishTestTransport{exchange: func(_ context.Context, request ai.FinishPlanRequest) ([]byte, error) {
		goal := finish.Goal{Purpose: "Replace the lookup table entry", Boundary: "Keep the unfinished table and its correction in one complete change", Reason: "Use the final lookup entry so readers no longer depend on the original value.", Message: "Replace the lookup entry\n\n- Use the final lookup entry so readers no longer depend on the\n  original value."}
		for _, change := range request.Changes {
			goal.ChangeIDs = append(goal.ChangeIDs, change.ID)
		}
		return json.Marshal(map[string]any{"goals": []finish.Goal{goal}})
	}}
	r := startFinishWorker(t, w, FinishOptions{})
	if len(r.Target.Checkpoints) != 1 || r.Target.SourceSelection {
		t.Fatal("fixture did not retain its intermediate version")
	}
	completed, err := w.Execute(t.Context(), r.ID)
	if err != nil || completed.State != state.FinishCompleted || completed.PlanningCalls != 1 {
		t.Fatalf("cancelled checkpoint churn inflated candidate size: %+v %v", completed, err)
	}
	var saved FinishSavedPlan
	if err := json.Unmarshal(completed.Plan, &saved); err != nil || len(saved.Steps) != 1 || saved.Steps[0].ReviewSize == nil || *saved.Steps[0].ReviewSize != (FinishReviewSize{ChangedLines: 2, ChangedFiles: 1}) {
		t.Fatalf("actual net size differs: %+v %v", saved, err)
	}
}

func TestFinishWorkerReviewSizeCollectsProjectFailure(t *testing.T) {
	t.Parallel()
	f, w := finishSizeFixture(t)
	approved, err := verification.NewApprovedCommand(f.dir, w.Config.ConfigDigest, verification.ModeFast, "exit 23", 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	w.Config.VerificationMode, w.Config.Command = "fast", approved
	w.Transport = &finishTestTransport{exchange: func(_ context.Context, request ai.FinishPlanRequest) ([]byte, error) {
		if request.Correction != nil && (!strings.Contains(request.Correction.ValidationError, "240") || !strings.Contains(request.Correction.ValidationError, "checks rejected")) {
			t.Fatalf("single correction lost size or project failure: %+v", request.Correction)
		}
		return json.Marshal(map[string]any{"goals": finishSizeGoals(request, false)})
	}}
	r := startFinishWorker(t, w, FinishOptions{})
	failed, err := w.Execute(t.Context(), r.ID)
	if err == nil || failed.State != state.FinishFailed || failed.PlanningCalls != 2 || finishWorkerGit(t, f.dir, "rev-parse", "HEAD") != r.Target.ExpectedHead {
		t.Fatalf("failed oversized candidate did not stay protected: %+v %v", failed, err)
	}
}

func TestFinishReviewSizeCountsTextAndBinaryPaths(t *testing.T) {
	t.Parallel()
	f, _ := finishWorkerFixture(t)
	finishWrite(t, f.dir, "lookup.txt", "unchanged\nold value\n")
	finishWorkerGit(t, f.dir, "add", "lookup.txt")
	finishWorkerGit(t, f.dir, "commit", "-m", "Seed lookup data")
	base := finishWorkerGit(t, f.dir, "rev-parse", "HEAD")
	finishWrite(t, f.dir, "lookup.txt", "unchanged\nnew value\n")
	finishWrite(t, f.dir, "tab\tname.txt", "first line\nsecond line\n")
	finishWrite(t, f.dir, "asset.bin", "binary\x00bytes")
	finishWorkerGit(t, f.dir, "add", "lookup.txt", "tab\tname.txt", "asset.bin")
	finishWorkerGit(t, f.dir, "commit", "-m", "Add text and binary entries")
	size, err := measureFinishReviewSize(t.Context(), f.dir, base, "HEAD")
	if err != nil || size != (FinishReviewSize{ChangedLines: 4, ChangedFiles: 3}) {
		t.Fatalf("candidate text/binary size: %+v %v", size, err)
	}
}
