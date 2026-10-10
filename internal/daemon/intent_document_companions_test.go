package daemon

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/ai"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/config"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

func TestIntentDocumentCompanionsRequireExactPublicReferences(t *testing.T) {
	t.Parallel()
	for _, testCase := range []struct {
		name, source, documentation string
		connected                   bool
	}{
		{"inline_flag", `+cmd.Flags().StringVar(&branch, "new-branch", "", "Create a branch")`, "+Use `acd history rewrite --new-branch NAME`.\n", true},
		{"fenced_flag", `+cmd.Flags().StringVar(&branch, "new-branch", "", "Create a branch")`, " ~~~bash\n+acd history rewrite --new-branch goals\n ~~~\n", true},
		{"backtick_fenced_flag", `+cmd.Flags().StringVar(&branch, "new-branch", "", "Create a branch")`, " ```bash\n+acd history rewrite --new-branch goals\n ```\n", true},
		{"status_label", `+return "provider-retry-due"`, "+Wait until `provider-retry-due` appears.\n", true},
		{"bare_prose", `+return "provider-retry-due"`, "+Wait until provider-retry-due appears.\n", false},
		{"unregistered_quote", `+const privateKey = "new-branch"`, "+Use `--new-branch`.\n", false},
		{"partial_label", `+return "provider-retry-due"`, "+Inspect `other-provider-retry-due`.\n", false},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			req := ai.IntentPlanRequestV2{ProtocolVersion: ai.IntentPlannerProtocolV2,
				OfferedCaptures: []ai.OfferedCapture{
					{Seq: 1, Path: "internal/cli/command.go", CapturedDiff: testCase.source + "\n"},
					{Seq: 2, Path: "docs/commands.md", CapturedDiff: testCase.documentation},
				}}
			goal := ai.IntentCandidateAssignment{CandidateID: "public-behavior", SelectedSeqs: []int64{1, 2},
				Purpose: "expose and document the public behavior", Readiness: ai.IntentCandidateReady,
				Subject: "Expose and document public behavior", Body: "- Keep the command contract with its implementation",
				GroupingReason: "exact registered flag or published status reference"}
			plan := ai.IntentPlanV2{ProtocolVersion: ai.IntentPlannerProtocolV2, Candidates: []ai.IntentCandidateAssignment{goal}}
			if err := ValidateIntentGoalPlan(req, plan); (err == nil) != testCase.connected {
				t.Fatalf("connected=%t error=%v", testCase.connected, err)
			}
			if !testCase.connected {
				req.Dependencies = []ai.IntentCaptureDependency{{FromSeq: 1, ToSeq: 2,
					Strength: ai.IntentDependencySoft, Kind: "documented_public_reference", EvidenceHash: "unproved-public-token"}}
				if err := ValidateIntentGoalPlan(req, plan); err == nil {
					t.Fatal("retained hint bypassed exact public reference checks")
				}
				return
			}
			source, doc := goal, goal
			source.SelectedSeqs, doc.SelectedSeqs = []int64{1}, []int64{2}
			doc.CandidateID = "separate-docs"
			plan.Candidates = []ai.IntentCandidateAssignment{source, doc}
			if err := ValidateIntentGoalPlan(req, plan); err == nil {
				t.Fatal("available documentation was split from its new behavior")
			}
			fallback, attention := balancedIntentCandidatePlan(req)
			if attention || len(fallback.Candidates) != 1 || !reflect.DeepEqual(fallback.Candidates[0].SelectedSeqs, []int64{1, 2}) {
				t.Fatalf("local recovery split the documented goal: %+v attention=%t", fallback, attention)
			}
			// Previously published behavior can explain a later documentation
			// goal without forcing the source to be published again.
			req.OfferedCaptures = req.OfferedCaptures[1:]
			req.Candidates = []ai.IntentCandidateSummary{{CandidateID: "published-source", Status: state.IntentCandidatePublished,
				SelectedSeqs: []int64{1}, CapturedEvidence: []ai.OfferedCapture{{Seq: 1, Path: "internal/cli/command.go", CapturedDiff: testCase.source + "\n"}}}}
			plan.Candidates = []ai.IntentCandidateAssignment{doc}
			if err := ValidateIntentGoalPlan(req, plan); err != nil {
				t.Fatalf("published context forced republishing: %v", err)
			}
		})
	}
}

func TestIntentDocumentReferenceKeepsQuotedCodeIndependent(t *testing.T) {
	t.Parallel()
	first := intentCandidateCaptureFixture(1, "phase.go", "create", "", "phase")
	first.CapturedDiff = "+return \"provider-retry-due\"\n"
	second := intentCandidateCaptureFixture(2, "label.go", "create", "", "label")
	second.CapturedDiff = "+label := \"provider-retry-due\"\n"
	if hints := runtimeIntentDependencyHints([]IntentCandidateCapture{first, second}); len(hints) != 0 {
		t.Fatalf("quoted code label supplied documentation evidence: %+v", hints)
	}
}

func TestIntentDocumentCompanionWindowFindsLateUsageGuide(t *testing.T) {
	t.Parallel()
	f := newCaptureFixture(t)
	ctx := context.Background()
	if _, err := BootstrapShadow(ctx, f.dir, f.db, f.cctx); err != nil {
		t.Fatal(err)
	}
	source := captureSamePathEdit(t, ctx, f, "phase.go", "package phase\n\nfunc ProviderPhase() string {\n    return \"provider-retry-due\"\n}\n")
	unrelated := captureSamePathEdit(t, ctx, f, "unrelated.md", "# Independent release checklist\n")
	if err := os.Mkdir(filepath.Join(f.dir, "docs"), 0o755); err != nil {
		t.Fatal(err)
	}
	doc := captureSamePathEdit(t, ctx, f, "docs/provider.md", "Wait until `provider-retry-due` appears.\n")
	pending, err := state.PendingEvents(ctx, f.db, 0)
	if err != nil {
		t.Fatal(err)
	}
	window, hints, reason, err := expandIntentGoalWindow(ctx, f.dir, f.db, f.cctx, pending, pending[:1],
		intentReplayConfig{window: 1}, time.Now())
	if err != nil || reason != "" || len(window) != 2 || window[0].Seq != source || window[1].Seq != doc {
		t.Fatalf("documented goal window=%+v hints=%+v reason=%s err=%v", window, hints, reason, err)
	}
	for _, event := range window {
		if event.Seq == unrelated {
			t.Fatal("unrelated prose entered the documented goal")
		}
	}
	before, err := git.RevParse(ctx, f.dir, "HEAD")
	if err != nil || before != f.cctx.BaseHead {
		t.Fatalf("lookahead moved source: head=%s err=%v", before, err)
	}
	planner := &intentGoalWindowPlanner{intentCandidatePlannerStub{plan: ai.IntentPlanV2{
		ProtocolVersion: ai.IntentPlannerProtocolV2, Candidates: []ai.IntentCandidateAssignment{{
			CandidateID: "provider-status", SelectedSeqs: []int64{source, doc},
			Purpose: "expose and document when a provider retry is due", Readiness: ai.IntentCandidateReady,
			Subject:        "Add documented provider retry status",
			Body:           "- Explain when provider backoff makes the next probe due",
			GroupingReason: "the usage guide names the exact published status label",
		}},
	}}}
	commitsBefore := revListCount(t, ctx, f.dir, "HEAD")
	summary, err := Replay(ctx, f.dir, f.db, f.cctx, ReplayOpts{
		GitDir: f.gitDir, CommitStrategy: ai.CommitStrategyIntent,
		IntentPlanner: planner, IntentPreset: config.PresetBalanced,
		IntentWindow: 1, IntentBypassBatchWait: true, IntentIncludeDiffs: true,
		IntentVerificationMode: "structural",
	})
	if err != nil || summary.Published != 2 || revListCount(t, ctx, f.dir, "HEAD") != commitsBefore+1 {
		t.Fatalf("implementation published without its guide: summary=%+v err=%v", summary, err)
	}
	pending, err = state.PendingEvents(ctx, f.db, 0)
	if err != nil || len(pending) != 1 || pending[0].Seq != unrelated {
		t.Fatalf("unrelated guide was not retained: pending=%+v err=%v", pending, err)
	}
}
