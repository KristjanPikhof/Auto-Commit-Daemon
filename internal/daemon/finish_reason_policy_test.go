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
)

func TestFinishWorkerLocalContextIsOptional(t *testing.T) {
	t.Parallel()
	f, w := finishWorkerFixture(t)
	finishWrite(t, f.dir, "goal.txt", "approved work\n")
	r := startFinishWorker(t, w, FinishOptions{})
	finishWrite(t, f.dir, "goal.txt", "later edit\n")
	completed, err := w.Execute(t.Context(), r.ID)
	if err != nil || completed.State != state.FinishCompleted || completed.PlanningCalls != 0 {
		t.Fatalf("bare Local finish: %+v %v", completed, err)
	}
	if finishWorkerGit(t, f.dir, "show", "HEAD:goal.txt") != "approved work" || finishWorkerGit(t, f.dir, "show", r.Target.CheckpointTree+":goal.txt") != "approved work" {
		t.Fatal("Local lost the frozen work")
	}
	if !strings.Contains(finishWorkerGit(t, f.dir, "status", "--short"), "goal.txt") {
		t.Fatal("Local removed later live work")
	}
}

func TestFinishWorkerExactMessageKeepsNewAndLegacyRequests(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"local", "intent"} {
		for _, legacy := range []bool{false, true} {
			t.Run(mode+map[bool]string{false: "/new", true: "/legacy"}[legacy], func(t *testing.T) {
				f, w := finishWorkerFixture(t)
				w.Config.Mode = mode
				w.Config.Provider.DiffEgress = true
				w.Config.Provider.CommitStrategy = ai.CommitStrategyIntent
				w.Config.Provider.Timeout = time.Minute
				provider := &finishTestTransport{exchange: func(_ context.Context, request ai.FinishPlanRequest) ([]byte, error) {
					if request.Message != "Preserve approved work" || request.Purpose != "" {
						t.Fatalf("exact override changed: %+v", request)
					}
					var plan struct {
						Goals []finish.Goal `json:"goals"`
					}
					if err := json.Unmarshal(completeFinishTestPlan(request), &plan); err != nil {
						t.Fatal(err)
					}
					plan.Goals[0].Message = request.Message
					return json.Marshal(plan)
				}}
				w.Transport = provider
				finishWrite(t, f.dir, "goal.txt", "approved work\n")
				r := startFinishWorker(t, w, FinishOptions{Message: "Preserve approved work"})
				if legacy {
					if _, err := state.InterruptFinishRequests(t.Context(), f.db, r.Target.WorktreeID); err != nil {
						t.Fatal(err)
					}
					target := r.Target
					target.Version = 2
					var created bool
					var err error
					r, created, err = state.StartFinishRequest(t.Context(), f.db, state.FinishRequest{ID: "legacy-exact-without-body", Target: target, DeadlineTS: r.DeadlineTS})
					if err != nil || !created {
						t.Fatalf("legacy request: %+v %v", r, err)
					}
				}
				completed, err := w.Execute(t.Context(), r.ID)
				calls := int32(0)
				if mode == "intent" {
					calls = 1
				}
				if err != nil || completed.State != state.FinishCompleted || provider.calls.Load() != calls || finishWorkerGit(t, f.dir, "log", "-1", "--format=%B") != r.Target.Message {
					t.Fatalf("exact message failed or changed: %+v calls=%d %v", completed, provider.calls.Load(), err)
				}
			})
		}
	}
}

func TestFinishWorkerLocalRetryKeepsOptionalContext(t *testing.T) {
	t.Parallel()
	f, w := finishWorkerFixture(t)
	finishWrite(t, f.dir, "goal.txt", "approved work\n")
	r := startFinishWorker(t, w, FinishOptions{})
	_, err := state.UpdateFinishRequest(t.Context(), f.db, r.ID, r.Attempt, state.FinishRunning, state.FinishUpdate{State: state.FinishFailed, ReasonCode: "finish_context_required", Reason: "Old runtime required context"})
	if err != nil {
		t.Fatal(err)
	}
	finishWrite(t, f.dir, "goal.txt", "later edit\n")
	retried := startFinishWorker(t, w, FinishOptions{RetryID: r.ID})
	completed, err := w.Execute(t.Context(), retried.ID)
	if err != nil || completed.State != state.FinishCompleted || completed.Target.Purpose != "" || completed.Target.TargetTree != r.Target.TargetTree || finishWorkerGit(t, f.dir, "show", "HEAD:goal.txt") != "approved work" {
		t.Fatalf("old missing-context request cannot retry its frozen target: %+v %v", completed, err)
	}
}

func TestFinishHistoryLocalPreservesTitleOnlyMessages(t *testing.T) {
	t.Parallel()
	f, w := finishWorkerFixture(t)
	oid := mustCommitPath(t, f.dir, "goal.txt", "approved work\n", "Preserve approved work")
	plan, err := PlanFinishHistory(t.Context(), f.dir, f.cctx.BranchRef, []string{oid}, nil, nil, FinishHistoryOptions{Mode: "local", Style: "imperative", VerificationConfigDigest: w.Config.ConfigDigest})
	if err != nil || plan.PlanningCalls != 0 || plan.Goals[0].Message != "Preserve approved work" {
		t.Fatalf("Local must preserve the original message: %+v %v", plan, err)
	}
	if err := ValidateIntentHistoryPlan(t.Context(), f.dir, plan); err != nil {
		t.Fatal(err)
	}
	r := historyStartTest(t, w, plan, false)
	completed, err := w.ExecuteFinishHistoryRequest(t.Context(), plan, r)
	if err != nil || completed.Status != "completed" || finishWorkerGit(t, f.dir, "log", "-1", "--format=%B") != "Preserve approved work" {
		t.Fatalf("Local imposed generated-message policy on original history: %+v %v", completed, err)
	}
}
