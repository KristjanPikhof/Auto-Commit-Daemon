package daemon

import (
	"context"
	"reflect"
	"strings"
	"testing"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/ai"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

type successiveHistoryGoalsPlanner struct{}

func (successiveHistoryGoalsPlanner) Name() string { return "successive-history-goals" }
func (successiveHistoryGoalsPlanner) PlanIntentV2(_ context.Context, req ai.IntentPlanRequestV2) (ai.IntentPlanV2, error) {
	return ai.IntentPlanV2{ProtocolVersion: ai.IntentPlannerProtocolV2, Candidates: []ai.IntentCandidateAssignment{
		{CandidateID: "bounded-window", SelectedSeqs: []int64{req.OfferedCaptures[0].Seq}, Purpose: "Bound the number of captures offered for planning", Readiness: ai.IntentCandidateReady,
			Subject: "Bound capture planning windows", Body: "- Keep large capture queues within a focused planning request", GroupingReason: "The capture window limit is independently reviewable"},
		{CandidateID: "provider-retry", SelectedSeqs: []int64{req.OfferedCaptures[1].Seq}, Purpose: "Schedule provider outage retries", Readiness: ai.IntentCandidateReady,
			Subject: "Schedule provider outage retries", Body: "- Retry temporary outages without losing protected capture work", GroupingReason: "Retry timing is a separate policy added after the window limit", DependsOnCandidates: []string{"bounded-window"}},
	}}, nil
}

func TestIntentHistorySavedPrerequisitesPreserveSuccessiveGoalTrees(t *testing.T) {
	t.Parallel()
	f := newCaptureFixture(t)
	ctx := context.Background()
	firstBody := "package policy\n\nfunc ClampPendingWindow(pending int) int {\n if pending > 64 { return 64 }\n return pending\n}\n"
	first := mustCommitPath(t, f.dir, "policy.go", firstBody, "Bound capture planning windows")
	second := mustCommitPath(t, f.dir, "policy.go", firstBody+"\nfunc ProviderRetryInterval(attempt int) int {\n if attempt > 1 { return 600 }\n return 300\n}\n", "Schedule provider outage retries")
	plan, err := PlanIntentHistory(ctx, f.dir, f.cctx.BranchRef, []string{first, second}, successiveHistoryGoalsPlanner{}, ai.CommitFormatImperative, true)
	if err != nil {
		t.Fatal(err)
	}
	plan.TargetBranchRef = "refs/heads/successive-goals"
	plan, err = state.SaveIntentHistoryPlan(ctx, f.db, plan)
	if err != nil {
		t.Fatal(err)
	}
	saved, ok, err := state.LoadIntentHistoryPlan(ctx, f.db, plan.ID)
	if err != nil || !ok || !reflect.DeepEqual(saved, plan) {
		t.Fatalf("saved plan lost its prerequisites: %+v ok=%v err=%v", saved, ok, err)
	}
	if len(saved.Goals) != 2 || !reflect.DeepEqual(saved.Goals[1].DependsOnCandidates, []string{"bounded-window"}) {
		t.Fatalf("successive goals lost their declared prerequisite: %+v", saved.Goals)
	}
	replacements, err := ValidateIntentHistoryPlan(ctx, f.dir, saved)
	if err != nil {
		t.Fatal(err)
	}
	var expectedTrees []string
	for _, oid := range []string{first, second} {
		tree, err := git.RevParse(ctx, f.dir, oid+"^{tree}")
		if err != nil {
			t.Fatal(err)
		}
		expectedTrees = append(expectedTrees, tree)
	}
	var verified []string
	result, err := git.ApplyIntentHistoryReconstruction(ctx, f.dir, git.IntentHistoryReconstructionOptions{
		SourceBranchRef: saved.SourceBranchRef, TargetBranchRef: saved.TargetBranchRef, ExpectedHead: saved.ExpectedHead,
		OldChain: saved.SourceChain, Replacements: replacements, PlanID: saved.ID,
		VerifyCommit: func(callCtx context.Context, oid string, index int) error {
			tree, err := git.RevParse(callCtx, f.dir, oid+"^{tree}")
			if err != nil {
				return err
			}
			if tree != expectedTrees[index] {
				t.Fatalf("goal %d tree=%s want=%s", index, tree, expectedTrees[index])
			}
			verified = append(verified, oid)
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(verified) != 2 || result.NewHead != verified[1] {
		t.Fatalf("successive verification order=%v result=%+v", verified, result)
	}
	parent, err := git.RevParse(ctx, f.dir, result.NewHead+"^")
	if err != nil || parent != verified[0] {
		t.Fatalf("dependent goal parent=%s want=%s err=%v", parent, verified[0], err)
	}
	if source, err := git.RevParse(ctx, f.dir, saved.SourceBranchRef); err != nil || source != second {
		t.Fatalf("reconstruction changed source=%s want=%s err=%v", source, second, err)
	}
	tampered := saved
	tampered.Goals = append([]state.IntentHistoryGoal(nil), saved.Goals...)
	tampered.Goals[1].DependsOnCandidates = nil
	if _, err := ValidateIntentHistoryPlan(ctx, f.dir, tampered); err == nil || !strings.Contains(err.Error(), "hard_dependency_undeclared") {
		t.Fatalf("omitted prerequisite accepted: %v", err)
	}
}
