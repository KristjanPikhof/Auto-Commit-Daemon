package daemon

import (
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
	"strings"
	"testing"
)

func TestFinishWorkerLocalMissingReasonPreservesProtectedWork(t *testing.T) {
	t.Parallel()
	f, w := finishWorkerFixture(t)
	finishWrite(t, f.dir, "goal.txt", "approved work\n")
	r := startFinishWorker(t, w, FinishOptions{})
	completed, err := w.Execute(t.Context(), r.ID)
	if err == nil || completed.State != state.FinishFailed || completed.ReasonCode != "finish_context_required" || finishWorkerGit(t, f.dir, "rev-parse", "HEAD") != r.Target.ExpectedHead {
		t.Fatalf("reasonless local publication: %+v %v", completed, err)
	}
	if got := finishWorkerGit(t, f.dir, "show", r.Target.CheckpointTree+":goal.txt"); got != "approved work" {
		t.Fatal("missing reason lost the checkpoint")
	}
	if got := finishWorkerGit(t, f.dir, "status", "--short"); !strings.Contains(got, "goal.txt") {
		t.Fatal("missing reason removed live work")
	}
}
func TestFinishWorkerExactMissingReasonStartsNoRequest(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"local", "intent"} {
		t.Run(mode, func(t *testing.T) {
			f, w := finishWorkerFixture(t)
			w.Config.Mode = mode
			finishWrite(t, f.dir, "goal.txt", "approved work\n")
			head := finishWorkerGit(t, f.dir, "rev-parse", "HEAD")
			if _, created, err := w.Start(t.Context(), FinishOptions{Message: "Preserve approved work"}); err == nil || created || !strings.Contains(err.Error(), "body") {
				t.Fatalf("reasonless exact request accepted: created=%t %v", created, err)
			}
			if finishWorkerGit(t, f.dir, "rev-parse", "HEAD") != head {
				t.Fatal("invalid exact message moved the branch")
			}
		})
	}
}
func TestFinishHistoryLocalRequiresReasonBeforeApply(t *testing.T) {
	t.Parallel()
	f, w := finishWorkerFixture(t)
	oid := mustCommitPath(t, f.dir, "goal.txt", "approved work\n", "Preserve approved work")
	plan, err := PlanFinishHistory(t.Context(), f.dir, f.cctx.BranchRef, []string{oid}, nil, nil, FinishHistoryOptions{Mode: "local", Style: "imperative", VerificationConfigDigest: w.Config.ConfigDigest})
	if err != nil || plan.PlanningCalls != 0 || plan.Goals[0].Message != "Preserve approved work" {
		t.Fatalf("Local must preserve the original message for review: %+v %v", plan, err)
	}
	r := historyStartTest(t, w, plan, false)
	if failed, err := w.ExecuteFinishHistoryRequest(t.Context(), plan, r); err == nil || failed.Status != "failed" || !strings.Contains(err.Error(), "body") {
		t.Fatalf("Local applied a message without a reason: %+v %v", failed, err)
	}
	if finishWorkerGit(t, f.dir, "rev-parse", "HEAD") != oid {
		t.Fatal("missing historical reason changed the branch")
	}
}

func TestFinishWorkerLegacyExactMissingReasonNeedsANewRequest(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"local", "intent"} {
		t.Run(mode, func(t *testing.T) {
			f, w := finishWorkerFixture(t)
			w.Config.Mode = mode
			finishWrite(t, f.dir, "goal.txt", "approved work\n")
			r := startFinishWorker(t, w, FinishOptions{Purpose: "Keep approved work separate from later edits"})
			if _, err := state.InterruptFinishRequests(t.Context(), f.db, r.Target.WorktreeID); err != nil {
				t.Fatal(err)
			}
			target := r.Target
			target.Version, target.Purpose, target.Message = 2, "", "Preserve approved work"
			legacy, created, err := state.StartFinishRequest(t.Context(), f.db, state.FinishRequest{ID: "legacy-exact-without-reason", Target: target, DeadlineTS: r.DeadlineTS})
			if err != nil || !created {
				t.Fatalf("legacy request: %+v %v", legacy, err)
			}
			failed, err := w.Execute(t.Context(), legacy.ID)
			if err == nil || failed.State != state.FinishFailed || failed.ReasonCode != "finish_message_reason_required" || failed.PlanningCalls != 0 {
				t.Fatalf("legacy exact message bypassed the reason gate: %+v %v", failed, err)
			}
			if finishWorkerGit(t, f.dir, "rev-parse", "HEAD") != target.ExpectedHead || finishWorkerGit(t, f.dir, "show", target.CheckpointTree+":goal.txt") != "approved work" {
				t.Fatal("missing legacy explanation moved the branch or lost protection")
			}

		})
	}
}

func TestFinishHistoryLegacyMessageRemainsReadableBeforeRefusal(t *testing.T) {
	t.Parallel()
	f, w := finishWorkerFixture(t)
	oid := mustCommitPath(t, f.dir, "goal.txt", "approved work\n", "Preserve approved work\n\n- Keep the approved value available for rollback.")
	plan, err := PlanFinishHistory(t.Context(), f.dir, f.cctx.BranchRef, []string{oid}, nil, nil, FinishHistoryOptions{Mode: "local", Style: "imperative", VerificationConfigDigest: w.Config.ConfigDigest})
	if err != nil {
		t.Fatal(err)
	}
	plan.Goals[0].Message = "Preserve approved work"
	if err := ValidateIntentHistoryPlan(t.Context(), f.dir, plan); err != nil {
		t.Fatalf("legacy message cannot be inspected: %v", err)
	}
	r := historyStartTest(t, w, plan, false)
	failed, err := w.ExecuteFinishHistoryRequest(t.Context(), plan, r)
	if err == nil || failed.Status != "failed" || !strings.Contains(err.Error(), "body") || finishWorkerGit(t, f.dir, "rev-parse", "HEAD") != oid {
		t.Fatalf("legacy history was published without an explanation: %+v %v", failed, err)
	}
}
