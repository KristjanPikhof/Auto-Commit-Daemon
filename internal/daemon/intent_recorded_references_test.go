package daemon

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/ai"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
)

type recordedScriptGoalPlanner struct{}

func TestIntentRecordedReferenceContextStaysBoundedAndFresh(t *testing.T) {
	t.Parallel()
	const changedLine = "+changed_code = 2\n"
	diff := "+api_key = abcdef1234567890\n" + strings.Repeat(changedLine, ai.IntentStageDiffCap/len(changedLine)+1)
	references := " python3 scripts/manifest.py\n"
	first := includeIntentRecordedReferenceContext(diff, references)
	if len(first) > ai.IntentStageDiffCap || strings.Contains(first, "abcdef1234567890") {
		t.Fatal("recorded reference context exceeded its budget or leaked a secret")
	}
	if second := includeIntentRecordedReferenceContext(first, references); second != first {
		t.Fatal("reloaded recorded evidence accumulated context")
	}
	if refreshed := includeIntentRecordedReferenceContext(first, ""); strings.Contains(refreshed, "scripts/manifest.py") {
		t.Fatal("obsolete offered-path context remained after regrounding")
	}
}

func TestIntentRecordedReferencesOmitBinaryAndSymlinkContents(t *testing.T) {
	t.Parallel()
	f := newCaptureFixture(t)
	ctx := context.Background()
	for _, tc := range []struct{ contents, mode string }{
		{"python3 scripts/manifest.py\n\x00binary", "100644"},
		{"python3 scripts/manifest.py\n\xff", "100644"},
		{"python3 scripts/manifest.py\n", "120000"},
	} {
		oid, err := git.HashObjectStdin(ctx, f.dir, []byte(tc.contents))
		if err != nil {
			t.Fatal(err)
		}
		references, err := loadIntentRecordedReferenceContext(ctx, f.dir, "scripts/runner.sh", oid, tc.mode, []string{"scripts/manifest.py"})
		if err != nil || references != "" {
			t.Fatalf("binary or symlink supplied source references: %q err=%v", references, err)
		}
	}
}

func (recordedScriptGoalPlanner) Name() string { return "recorded-script-goal" }
func (recordedScriptGoalPlanner) PlanIntentV2(_ context.Context, req ai.IntentPlanRequestV2) (ai.IntentPlanV2, error) {
	var seqs []int64
	for _, capture := range req.OfferedCaptures {
		seqs = append(seqs, capture.Seq)
	}
	return ai.IntentPlanV2{ProtocolVersion: ai.IntentPlannerProtocolV2, Candidates: []ai.IntentCandidateAssignment{{
		CandidateID: "feedback", SelectedSeqs: seqs, Purpose: "Balance complete regression feedback across shards",
		Readiness: ai.IntentCandidateReady, Subject: "Balance regression feedback across shards",
		Body:           "- Keep the runner, shard selection and measured costs together",
		GroupingReason: "Recorded script calls and file reads connect the complete feedback goal",
	}}}, nil
}

func recordedScriptFixture() map[string][2]string {
	padding := strings.Repeat("# unchanged runner context\n", 20)
	return map[string][2]string{
		"scripts/runner.sh":    {"python3 scripts/manifest.py\n" + padding + "workers=1\n", "python3 scripts/manifest.py\n" + padding + "workers=2\n"},
		"scripts/manifest.py":  {"costs = Path(__file__).with_name(\"timings.json\")\n" + padding + "shards = 1\n", "costs = Path(__file__).with_name(\"timings.json\")\n" + padding + "shards = 2\n"},
		"scripts/timings.json": {"{\"runner\": 10}\n", "{\"runner\": 5}\n"},
	}
}

func TestIntentHistoryRetainsUnchangedRecordedScriptCalls(t *testing.T) {
	t.Parallel()
	f := newCaptureFixture(t)
	ctx := context.Background()
	fixture := recordedScriptFixture()
	paths := []string{"scripts/runner.sh", "scripts/manifest.py", "scripts/timings.json"}
	if err := os.MkdirAll(filepath.Join(f.dir, "scripts"), 0700); err != nil {
		t.Fatal(err)
	}
	for _, path := range paths {
		mustCommitPath(t, f.dir, path, fixture[path][0], "Add regression runner baseline")
	}
	var chain []string
	for _, path := range paths {
		chain = append(chain, mustCommitPath(t, f.dir, path, fixture[path][1], "Balance regression feedback"))
	}
	if err := os.WriteFile(filepath.Join(f.dir, paths[0]), []byte("python3 unrelated.py\n"), 0600); err != nil {
		t.Fatal(err)
	}
	plan, err := PlanIntentHistory(ctx, f.dir, f.cctx.BranchRef, chain, recordedScriptGoalPlanner{}, ai.CommitFormatImperative, true)
	if err != nil {
		t.Fatal(err)
	}
	plan.TargetBranchRef = "refs/heads/feedback-goals"
	if _, err := ValidateIntentHistoryPlan(ctx, f.dir, plan); err != nil {
		t.Fatalf("saved plan lost its immutable reference context: %v", err)
	}
	if len(plan.Goals) != 1 || len(plan.Goals[0].Units) != 3 {
		t.Fatalf("complete feedback goal was split: %+v", plan.Goals)
	}
	if got, _ := os.ReadFile(filepath.Join(f.dir, paths[0])); string(got) != "python3 unrelated.py\n" {
		t.Fatal("history planning changed live work")
	}
}

func TestIntentLiveGoalsUseRecordedCallsWithoutDiffEgressBypass(t *testing.T) {
	t.Parallel()
	f := newCaptureFixture(t)
	ctx := context.Background()
	fixture := recordedScriptFixture()
	var captures []IntentCandidateCapture
	for _, path := range []string{"scripts/runner.sh", "scripts/manifest.py", "scripts/timings.json"} {
		versions := fixture[path]
		before, err := git.HashObjectStdin(ctx, f.dir, []byte(versions[0]))
		if err != nil {
			t.Fatal(err)
		}
		after, err := git.HashObjectStdin(ctx, f.dir, []byte(versions[1]))
		if err != nil {
			t.Fatal(err)
		}
		captures = append(captures, appendIntentCandidateCapture(t, f.db, path, "modify", before, after))
	}
	input := IntentCandidateEvaluation{RepoPath: f.dir, BranchRef: captures[0].Event.BranchRef, BranchGeneration: captures[0].Event.BranchGeneration, Captures: captures, IncludeDiffs: true, Now: time.Now()}
	evidence, err := loadFocusedIntentGoalEvidence(ctx, input, nil, append([]IntentCandidateCapture(nil), captures...))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(evidence[0].CapturedDiff, " python3 scripts/manifest.py") || !strings.Contains(evidence[0].CapturedDiff, "+workers=2") {
		t.Fatal("immutable calls or actual changed code were omitted")
	}
	input.Captures = evidence
	req, err := buildIntentCandidateRequest(input, nil, nil, nil, evidence)
	if err != nil {
		t.Fatal(err)
	}
	plan, _ := (recordedScriptGoalPlanner{}).PlanIntentV2(ctx, req)
	if err := ValidateIntentGoalPlan(req, plan); err != nil {
		t.Fatalf("live goal lost the same relationship as history: %v", err)
	}
	input.IncludeDiffs = false
	private, err := buildIntentCandidateRequest(input, nil, nil, nil, evidence)
	if err != nil {
		t.Fatal(err)
	}
	for _, offered := range private.OfferedCaptures {
		if offered.CapturedDiff != "" {
			t.Fatal("recorded context bypassed diff privacy permission")
		}
	}
}
