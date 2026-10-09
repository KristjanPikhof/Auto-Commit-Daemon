package daemon

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/ai"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/config"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

func TestIntentPreservationRejectsReadyDependentOfWaitingGoal(t *testing.T) {
	t.Parallel()
	req := ai.IntentPlanRequestV2{ProtocolVersion: ai.IntentPlannerProtocolV2,
		OfferedCaptures: []ai.OfferedCapture{{Seq: 1}, {Seq: 2}, {Seq: 3}}}
	plan := ai.IntentPlanV2{ProtocolVersion: ai.IntentPlannerProtocolV2,
		Candidates: []ai.IntentCandidateAssignment{
			{CandidateID: "waiting-prerequisite", SelectedSeqs: []int64{1}, Readiness: ai.IntentCandidateWait},
			{CandidateID: "ready-dependent", SelectedSeqs: []int64{2}, Readiness: ai.IntentCandidateReady,
				DependsOnCandidates: []string{"waiting-prerequisite"}},
			{CandidateID: "independent-guide", SelectedSeqs: []int64{3}, Readiness: ai.IntentCandidateReady},
		}}
	preserved, partial, ok := preserveIntentPlanGroups(req, plan, nil)
	if !ok || !reflect.DeepEqual(intentAssignmentMembership(preserved), [][]int64{{3}}) ||
		!reflect.DeepEqual(offeredIntentSeqs(partial), []int64{1, 2}) {
		t.Fatalf("incomplete prerequisite was locked: preserved=%+v partial=%+v", preserved, partial)
	}
	if len(partial.Candidates) != 1 || partial.Candidates[0].CandidateID != "independent-guide" ||
		partial.Candidates[0].Status != "locked_ready" || !partial.Candidates[0].Ready {
		t.Fatalf("correction retained an unresolved descriptor: %+v", partial.Candidates)
	}
}

type preservedWaitCorrectionPlanner struct {
	requests []ai.IntentPlanRequestV2
}

func (*preservedWaitCorrectionPlanner) Name() string { return "preserved-wait-correction-test" }

func (*preservedWaitCorrectionPlanner) PlanIntent(context.Context, ai.IntentPlanRequest) (ai.IntentPlan, error) {
	return ai.IntentPlan{}, errors.New("native v2 required")
}

func (p *preservedWaitCorrectionPlanner) PlanIntentV2(_ context.Context, req ai.IntentPlanRequestV2) (ai.IntentPlanV2, error) {
	p.requests = append(p.requests, req)
	seqs := make(map[string]int64)
	for _, capture := range req.OfferedCaptures {
		seqs[capture.Path] = capture.Seq
	}
	if len(p.requests) == 1 {
		return ai.IntentPlanV2{ProtocolVersion: ai.IntentPlannerProtocolV2,
			Candidates: []ai.IntentCandidateAssignment{
				{CandidateID: "keyboard-guide", SelectedSeqs: []int64{seqs["usage.md"]},
					Purpose: "explain how to discover keyboard shortcuts", Readiness: ai.IntentCandidateReady,
					Subject: "Document keyboard shortcuts", Body: "- Explain how to discover available keyboard commands",
					GroupingReason: "an independent keyboard shortcut guide"},
				{CandidateID: "waiting-value", SelectedSeqs: []int64{seqs["source.go"]},
					Purpose: "return the updated source value", Readiness: ai.IntentCandidateWait,
					MissingCompanions: []string{"keep the matching regression with the implementation"},
					GroupingReason:    "the source change still needs its complete regression goal"},
				{CandidateID: "waiting-regression", SelectedSeqs: []int64{seqs["source_test.go"]},
					Purpose: "verify the updated source value", Readiness: ai.IntentCandidateWait,
					MissingCompanions: []string{"keep the source implementation with the regression"},
					GroupingReason:    "the matching assertion is unresolved until grouped with its source"},
				{CandidateID: "invalid-empty-goal", Purpose: "retain the unresolved source goal", Readiness: ai.IntentCandidateReady,
					Subject: "Return the updated source value", GroupingReason: "this invalid empty assignment must be corrected"},
			}}, nil
	}
	if len(p.requests) != 2 {
		return ai.IntentPlanV2{}, errors.New("correction unexpectedly required another provider call")
	}
	return ai.IntentPlanV2{ProtocolVersion: ai.IntentPlannerProtocolV2,
		Candidates: []ai.IntentCandidateAssignment{{
			CandidateID: "complete-value-goal", SelectedSeqs: []int64{seqs["source.go"], seqs["source_test.go"]},
			Purpose: "return and verify the updated source value", Readiness: ai.IntentCandidateReady,
			Subject: "Return the updated source value", Body: "- Keep the implementation and its matching regression together",
			GroupingReason: "the value implementation and its matching assertion complete one goal",
		}}}, nil
}

func TestReplayIntentCorrectionReoffersWaitingGoals(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	f := newCaptureFixture(t)
	seedTrackedFileCommit(t, ctx, f, "source.go", "package source\nfunc Value() int { return 1 }\n")
	seedTrackedFileCommit(t, ctx, f, "source_test.go", "package source\nimport \"testing\"\nfunc TestValue(t *testing.T) { if Value() != 1 { t.Fatal(Value()) } }\n")
	if _, err := BootstrapShadow(ctx, f.dir, f.db, f.cctx); err != nil {
		t.Fatal(err)
	}
	for path, body := range map[string]string{
		"source.go":      "package source\nfunc Value() int { return 2 }\n",
		"source_test.go": "package source\nimport \"testing\"\nfunc TestValue(t *testing.T) { if Value() != 2 { t.Fatal(Value()) } }\n",
		"usage.md":       "# Keyboard shortcuts\nUse the command menu to discover available shortcuts.\n",
	} {
		writePublicationFile(t, f, path, body)
	}
	if captured := capturePublicationFiles(t, f); !captured.Protected {
		t.Fatalf("correction fixture was not protected: %+v", captured)
	}
	pending, err := state.PendingEvents(ctx, f.db, 0)
	if err != nil || len(pending) != 3 {
		t.Fatalf("captures=%+v err=%v", pending, err)
	}
	planner := &preservedWaitCorrectionPlanner{}
	retryLimit := 2
	before := revListCount(t, ctx, f.dir, "HEAD")
	result, err := Replay(ctx, f.dir, f.db, f.cctx, ReplayOpts{
		GitDir: f.gitDir, CommitStrategy: ai.CommitStrategyIntent, IntentPlanner: planner,
		IntentPreset: config.PresetBalanced, IntentBypassBatchWait: true, IntentWindow: 10,
		IntentRetryLimit: &retryLimit, IntentVerificationMode: "structural", IntentIncludeDiffs: true,
	})
	if err != nil || result.Published != 3 || result.Failed != 0 || len(planner.requests) != 2 ||
		revListCount(t, ctx, f.dir, "HEAD") != before+2 {
		t.Fatalf("waiting goals blocked correction: result=%+v calls=%d err=%v", result, len(planner.requests), err)
	}
	correction := planner.requests[1]
	var offeredPaths []string
	for _, capture := range correction.OfferedCaptures {
		offeredPaths = append(offeredPaths, capture.Path)
	}
	if !reflect.DeepEqual(offeredPaths, []string{"source.go", "source_test.go"}) || correction.RetryCorrection == "" {
		t.Fatalf("correction lost unresolved source/test membership: paths=%v correction=%q", offeredPaths, correction.RetryCorrection)
	}
	if len(correction.Candidates) != 1 || correction.Candidates[0].CandidateID != "keyboard-guide" ||
		correction.Candidates[0].Status != "locked_ready" || !correction.Candidates[0].Ready {
		t.Fatalf("WAIT descriptors became locked ready: %+v", correction.Candidates)
	}
	if err := ai.ValidateIntentPlanRequestV2(correction); err != nil {
		t.Fatalf("correction request violated unique ownership: %v", err)
	}
	commits := make(map[string]string)
	for _, event := range pending {
		var commit string
		if err := f.db.ReadSQL().QueryRowContext(ctx,
			"SELECT commit_oid FROM capture_events WHERE seq=? AND state='published'", event.Seq).Scan(&commit); err != nil {
			t.Fatal(err)
		}
		commits[event.Path] = commit
		var owners int
		if err := f.db.ReadSQL().QueryRowContext(ctx,
			"SELECT COUNT(*) FROM intent_candidate_events WHERE event_seq=? AND membership_state='active'", event.Seq).Scan(&owners); err != nil || owners != 1 {
			t.Fatalf("capture %d acquired duplicate/missing ownership: owners=%d err=%v", event.Seq, owners, err)
		}
	}
	if commits["source.go"] == "" || commits["source.go"] != commits["source_test.go"] || commits["source.go"] == commits["usage.md"] {
		t.Fatalf("commits lost their complete goals: %+v", commits)
	}
	for path, want := range map[string][]string{
		"source.go": {"source.go", "source_test.go"},
		"usage.md":  {"usage.md"},
	} {
		changed := strings.Fields(mustGitOutput(t, f.dir, "show", "--format=", "--name-only", commits[path]))
		if !reflect.DeepEqual(changed, want) {
			t.Fatalf("commit for %s changed paths outside its goal: got=%v want=%v", path, changed, want)
		}
	}
	remaining, err := state.PendingEvents(ctx, f.db, 0)
	if err != nil || len(remaining) != 0 {
		t.Fatalf("corrected goals remained pending: %+v err=%v", remaining, err)
	}
	var invalid int
	if err := f.db.ReadSQL().QueryRowContext(ctx,
		"SELECT COUNT(*) FROM intent_candidates WHERE id IN ('waiting-value','waiting-regression','invalid-empty-goal')").Scan(&invalid); err != nil || invalid != 0 {
		t.Fatalf("rejected WAIT partition became durable: candidates=%d err=%v", invalid, err)
	}
}
