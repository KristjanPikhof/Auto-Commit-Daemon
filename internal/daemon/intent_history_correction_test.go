package daemon

import (
	"context"
	"errors"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/ai"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
)

type correctingHistoryOwnershipPlanner struct {
	t      *testing.T
	calls  int
	always bool
}

func (*correctingHistoryOwnershipPlanner) Name() string { return "history-ownership-correction" }

func (p *correctingHistoryOwnershipPlanner) PlanIntentV2(_ context.Context, req ai.IntentPlanRequestV2) (ai.IntentPlanV2, error) {
	p.calls++
	if p.calls > 1 {
		for _, want := range []string{`seq=1 owners=["capture-goal","retry-goal"]`, `seq=2 owners=["capture-goal","retry-goal"]`, "indivisible"} {
			if !strings.Contains(req.RetryCorrection, want) {
				p.t.Fatalf("correction omitted rejected ownership %q: %s", want, req.RetryCorrection)
			}
		}
		if strings.Contains(req.RetryCorrection, "UNTRUSTED_RESPONSE_TEXT") {
			p.t.Fatal("history correction replayed rejected response prose")
		}
	}
	groups := []ai.IntentCandidateAssignment{
		{CandidateID: "capture-goal", SelectedSeqs: []int64{1}, Purpose: "Explain how completed checkpoints protect captured changes", Readiness: ai.IntentCandidateReady,
			Subject: "Document protected file capture", Body: "- Explain checkpoint protection before branch publication", GroupingReason: "The capture guide describes one independently useful workflow"},
		{CandidateID: "retry-goal", SelectedSeqs: []int64{2}, Purpose: "Explain scheduled reconnection after temporary provider failures", Readiness: ai.IntentCandidateReady,
			Subject: "Document provider retry timing", Body: "- Explain when automatic provider reconnection resumes", GroupingReason: "The retry guide describes one independently useful workflow"},
	}
	if p.calls == 1 || p.always {
		for i := range groups {
			groups[i].SelectedSeqs = []int64{1, 2}
			groups[i].Body += "\n- UNTRUSTED_RESPONSE_TEXT must not be replayed"
		}
	}
	return ai.IntentPlanV2{ProtocolVersion: ai.IntentPlannerProtocolV2, Candidates: groups}, nil
}

func TestIntentHistoryCorrectionReportsAllDuplicateOwners(t *testing.T) {
	t.Parallel()
	for _, always := range []bool{false, true} {
		t.Run(map[bool]string{false: "corrected", true: "bounded_rejection"}[always], func(t *testing.T) {
			f := newCaptureFixture(t)
			ctx := context.Background()
			first := mustCommitPath(t, f.dir, "capture-guide.md", "# Protected file capture\n\nCompleted checkpoints protect captured changes before publication.\n", "Add capture guide")
			head := mustCommitPath(t, f.dir, "retry-guide.md", "# Provider retry timing\n\nRetry provider connections after five minutes, ten minutes, then hourly.\n", "Add retry guide")
			planner := &correctingHistoryOwnershipPlanner{t: t, always: always}
			plan, err := PlanIntentHistory(ctx, f.dir, f.cctx.BranchRef, []string{first, head}, planner, ai.CommitFormatImperative, true)
			if always {
				var validation *ai.IntentPlanV2ValidationError
				if !errors.As(err, &validation) || planner.calls != 3 {
					t.Fatalf("persistent duplicate escaped bounded retries: calls=%d err=%v", planner.calls, err)
				}
			} else {
				if err != nil || planner.calls != 2 {
					t.Fatalf("complete ownership correction failed: calls=%d err=%v", planner.calls, err)
				}
				replacements, err := ValidateIntentHistoryPlan(ctx, f.dir, plan)
				if err != nil {
					t.Fatal(err)
				}
				tree, err := git.RevParse(ctx, f.dir, head+"^{tree}")
				if err != nil || len(replacements) != 2 || replacements[1].TreeOID != tree {
					t.Fatalf("correction changed recorded final tree: replacements=%+v tree=%s err=%v", replacements, tree, err)
				}
			}
			if current, err := git.RevParse(ctx, f.dir, "HEAD"); err != nil || current != head {
				t.Fatalf("planning changed source HEAD=%s want=%s err=%v", current, head, err)
			}
		})
	}
}

func TestIntentHistoryOwnershipCorrectionIsBoundedMetadataOnly(t *testing.T) {
	t.Parallel()
	req := ai.IntentPlanRequestV2{}
	plan := ai.IntentPlanV2{}
	for seq := int64(1); seq <= ai.IntentCandidateCaptureCap; seq++ {
		req.OfferedCaptures = append(req.OfferedCaptures, ai.OfferedCapture{Seq: seq, Path: "PRIVATE_SOURCE_TEXT"})
	}
	for i := 0; i < ai.IntentOpenCandidateCap+1; i++ {
		candidate := ai.IntentCandidateAssignment{CandidateID: "api_key=sk-privatecredentialvalue", Purpose: "PRIVATE_PURPOSE_TEXT", Body: "PRIVATE_BODY_TEXT"}
		for seq := int64(1); seq <= ai.IntentCandidateCaptureCap+1; seq++ {
			candidate.SelectedSeqs = append(candidate.SelectedSeqs, seq)
		}
		plan.Candidates = append(plan.Candidates, candidate)
	}
	failure := &ai.IntentPlanV2ValidationError{Message: "duplicate captured ownership", Findings: []ai.IntentAtomicityFinding{{Code: "capture_assigned_twice"}}}
	first := intentHistoryPlanCorrection(req, plan, failure)
	if first != intentHistoryPlanCorrection(req, plan, failure) || utf8.RuneCountInString(first) > ai.IntentAtomicityCorrectionCap ||
		!strings.Contains(first, "overlap rows omitted") {
		t.Fatalf("ownership diagnostics were nondeterministic or unbounded: chars=%d", utf8.RuneCountInString(first))
	}
	for _, secret := range []string{"sk-privatecredentialvalue", "PRIVATE_SOURCE_TEXT", "PRIVATE_PURPOSE_TEXT", "PRIVATE_BODY_TEXT", "seq=257"} {
		if strings.Contains(first, secret) {
			t.Fatalf("correction replayed unavailable/private response field %q", secret)
		}
	}
}

type mergingHistoryOwnershipPlanner struct {
	t     *testing.T
	calls int
}

func (*mergingHistoryOwnershipPlanner) Name() string { return "history-merged-goal-correction" }

func (p *mergingHistoryOwnershipPlanner) PlanIntentV2(_ context.Context, req ai.IntentPlanRequestV2) (ai.IntentPlanV2, error) {
	p.calls++
	if p.calls == 1 {
		return ai.IntentPlanV2{ProtocolVersion: ai.IntentPlannerProtocolV2, Candidates: []ai.IntentCandidateAssignment{
			{CandidateID: "provider-retry-health", SelectedSeqs: []int64{1, 2}, Purpose: "restore provider readiness after a wait", Readiness: ai.IntentCandidateReady,
				Subject: "Restore provider readiness", Body: "- Resume readiness through the provider wait helper", GroupingReason: "the readiness consumer requires its provider wait implementation"},
			{CandidateID: "intent-history-observability", SelectedSeqs: []int64{2, 3}, Purpose: "verify provider readiness after a wait", Readiness: ai.IntentCandidateReady,
				Subject: "Verify provider readiness", Body: "- Cover the public readiness consumer and its recovery result", GroupingReason: "the focused regression calls the public readiness consumer"},
		}}, nil
	}
	for _, expected := range []string{"merge those inseparable goals", "all their required implementation, callers, tests, and documentation", "combined net behavior", "connected overlap set", "correct the assignment instead", `seq=2 owners=["intent-history-observability","provider-retry-health"]`} {
		if !strings.Contains(req.RetryCorrection, expected) {
			p.t.Fatalf("correction omitted complete-goal guidance %q: %s", expected, req.RetryCorrection)
		}
	}
	return ai.IntentPlanV2{ProtocolVersion: ai.IntentPlannerProtocolV2, Candidates: []ai.IntentCandidateAssignment{{
		CandidateID: "restore-provider-readiness", SelectedSeqs: []int64{1, 2, 3}, Purpose: "restore and verify provider readiness after a wait", Readiness: ai.IntentCandidateReady,
		Subject: "Restore provider readiness after waits", Body: "- Keep the wait helper, readiness consumer, and regression complete",
		GroupingReason: "the wait helper, its public consumer, and the direct regression form one complete readiness behavior",
	}}}, nil
}

func TestIntentHistoryCorrectionMergesInseparableGoals(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	f := newCaptureFixture(t)
	first := mustCommitPath(t, f.dir, "provider_wait.go", "package provider\nfunc RetryProviderWait() bool { return true }\n", "Add provider wait readiness")
	second := mustCommitPath(t, f.dir, "provider.go", "package provider\nfunc ProviderReady() bool { return RetryProviderWait() }\n", "Expose provider readiness")
	head := mustCommitPath(t, f.dir, "provider_test.go", "package provider\nimport \"testing\"\nfunc TestProviderReady(t *testing.T) { if !ProviderReady() { t.Fatal(\"provider did not resume\") } }\n", "Verify provider readiness")
	planner := &mergingHistoryOwnershipPlanner{t: t}
	plan, err := PlanIntentHistory(ctx, f.dir, f.cctx.BranchRef, []string{first, second, head}, planner, ai.CommitFormatImperative, true)
	if err != nil || planner.calls != 2 || len(plan.Goals) != 1 || len(plan.Goals[0].Units) != 3 {
		t.Fatalf("inseparable goals did not merge through bounded correction: goals=%+v calls=%d err=%v", plan.Goals, planner.calls, err)
	}
	if plan.Goals[0].Message != "Restore provider readiness after waits\n\n- Keep the wait helper, readiness consumer, and regression complete" {
		t.Fatalf("merged goal lost its semantic message: %q", plan.Goals[0].Message)
	}
	replacements, err := ValidateIntentHistoryPlan(ctx, f.dir, plan)
	if err != nil {
		t.Fatal(err)
	}
	tree, err := git.RevParse(ctx, f.dir, head+"^{tree}")
	if err != nil || len(replacements) != 1 || replacements[0].TreeOID != tree {
		t.Fatalf("merged goal changed the recorded final tree: replacements=%+v tree=%s err=%v", replacements, tree, err)
	}
	if current, err := git.RevParse(ctx, f.dir, "HEAD"); err != nil || current != head {
		t.Fatalf("correction changed source HEAD: %s want=%s err=%v", current, head, err)
	}
}
