package daemon

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/ai"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/config"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

func TestIntentCobraRecoveryDocumentationPublishesCompleteGoal(t *testing.T) {
	t.Parallel()
	f := newCaptureFixture(t)
	ctx := context.Background()
	for _, dir := range []string{"cli", "docs"} {
		if err := os.MkdirAll(filepath.Join(f.dir, dir), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	seedTrackedFileCommit(t, ctx, f, "cli/root.go", intentCobraFixtureRoot)
	seedTrackedFileCommit(t, ctx, f, "cli/fix.go", strings.Replace(intentCobraFixtureFix, "return force }", "return false }", 1))
	seedTrackedFileCommit(t, ctx, f, "cli/facade.go", strings.Replace(intentCobraFixtureFacade, "return buildFixPlan(force) }", "return false }", 1))
	if _, err := BootstrapShadow(ctx, f.dir, f.db, f.cctx); err != nil {
		t.Fatal(err)
	}
	doc := captureSamePathEdit(t, ctx, f, "docs/commands.md", "# Recovery\nUse `acd support recover --force --yes` to preserve pending work.\n")
	fix := captureSamePathEdit(t, ctx, f, "cli/fix.go", intentCobraFixtureFix)
	fixTest := captureSamePathEdit(t, ctx, f, "cli/fix_pending_capture_recovery_test.go", "package cli\nfunc TestPendingRecovery() { if !buildFixPlan(true) { panic(fixActionReconcileUnpublishedChain) } }\n")
	facade := captureSamePathEdit(t, ctx, f, "cli/facade.go", intentCobraFixtureFacade)
	productTest := captureSamePathEdit(t, ctx, f, "cli/product_recovery_changed_test.go", "package cli\nfunc TestRecoveryChanged() { if !runProductFix(true) { panic(fixActionReconcileUnpublishedChain) } }\n")
	seqs := []int64{doc, fix, fixTest, facade, productTest}
	goal := ai.IntentCandidateAssignment{CandidateID: "pending-recovery", SelectedSeqs: seqs,
		Readiness: ai.IntentCandidateReady, Purpose: "preserve unresolved pending capture chains during forced recovery",
		Subject: "Preserve pending capture chains", Body: "- Keep forced recovery and its documented result complete",
		GroupingReason: "The qualified recovery command owns both implementations and their actual regression callers"}
	planner := &intentGoalWindowPlanner{intentCandidatePlannerStub{plan: ai.IntentPlanV2{ProtocolVersion: ai.IntentPlannerProtocolV2, Candidates: []ai.IntentCandidateAssignment{goal}}}}
	head, err := git.RevParse(ctx, f.dir, "HEAD")
	if err != nil {
		t.Fatal(err)
	}
	before := revListCount(t, ctx, f.dir, "HEAD")
	summary, err := Replay(ctx, f.dir, f.db, f.cctx, ReplayOpts{GitDir: f.gitDir, CommitStrategy: ai.CommitStrategyIntent,
		IntentPlanner: planner, IntentPreset: config.PresetBalanced, IntentWindow: 1, IntentBypassBatchWait: true,
		IntentIncludeDiffs: true, IntentVerificationMode: "structural"})
	if err != nil || summary.Published != 5 || planner.calls != 1 || revListCount(t, ctx, f.dir, "HEAD") != before+1 {
		t.Fatalf("complete recovery goal did not publish: %+v calls=%d err=%v", summary, planner.calls, err)
	}
	var offered []int64
	for _, capture := range planner.req.OfferedCaptures {
		offered = append(offered, capture.Seq)
	}
	if !reflect.DeepEqual(offered, seqs) {
		t.Fatalf("qualified CLI closure omitted an available constructor/test: %v", offered)
	}
	if err := ValidateIntentGoalPlan(planner.req, planner.plan); err != nil {
		t.Fatalf("full goal proof rejected complete recovery: %v", err)
	}
	candidate, found, err := state.IntentCandidateByPublishedCommit(ctx, f.db, f.cctx.BranchRef, f.cctx.BranchGeneration, summary.SelfPublicationTargetOID)
	if err != nil || !found || (candidate.Status != state.IntentCandidatePublished && candidate.Status != state.IntentCandidateSoftPublished) ||
		!candidate.AtomicityStatus.Valid || candidate.AtomicityStatus.String != "passed" {
		t.Fatalf("complete published goal/atomicity=%+v found=%t err=%v", candidate, found, err)
	}
	// Both constructors are required; a four-capture approximation must fail.
	partial := goal
	partial.SelectedSeqs = []int64{doc, fix, fixTest, productTest}
	other := goal
	other.CandidateID = "facade-separate"
	other.SelectedSeqs = []int64{facade}
	if err := ValidateIntentGoalPlan(planner.req, ai.IntentPlanV2{ProtocolVersion: ai.IntentPlannerProtocolV2, Candidates: []ai.IntentCandidateAssignment{partial, other}}); err == nil {
		t.Fatal("published recovery omitted its available facade constructor")
	}
	// JSON cannot assert the private proof or preserve it accidentally.
	raw, err := json.Marshal(planner.req)
	if err != nil {
		t.Fatal(err)
	}
	var untrusted ai.IntentPlanRequestV2
	if err := json.Unmarshal(raw, &untrusted); err != nil {
		t.Fatal(err)
	}
	if err := ValidateIntentGoalPlan(untrusted, planner.plan); err == nil {
		t.Fatal("provider JSON forged qualified constructor evidence")
	}
	pending, err := state.PendingEvents(ctx, f.db, 0)
	if err != nil || len(pending) != 0 {
		t.Fatalf("pending=%+v err=%v", pending, err)
	}
	// A known current baseline is immutable evidence, not offered work.
	for _, capture := range planner.req.OfferedCaptures {
		if capture.Path == "cli/root.go" {
			t.Fatal("unchanged root was offered for publication")
		}
	}
	if head == "" {
		t.Fatal("missing original baseline")
	}
}

func TestIntentCobraReferencesPreserveFrozenTargetAndNetVersions(t *testing.T) {
	t.Parallel()
	f := newCaptureFixture(t)
	ctx := context.Background()
	for _, dir := range []string{"cli", "docs"} {
		if err := os.MkdirAll(filepath.Join(f.dir, dir), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	seedTrackedFileCommit(t, ctx, f, "cli/root.go", intentCobraFixtureRoot)
	if _, err := BootstrapShadow(ctx, f.dir, f.db, f.cctx); err != nil {
		t.Fatal(err)
	}
	doc := captureSamePathEdit(t, ctx, f, "docs/commands.md", "# Recovery\nUse `acd support recover --force`.\n")
	fix := captureSamePathEdit(t, ctx, f, "cli/fix.go", intentCobraFixtureFix)
	oldFacade := captureSamePathEdit(t, ctx, f, "cli/facade.go", intentCobraFixtureFacade)
	facade := captureSamePathEdit(t, ctx, f, "cli/facade.go", strings.Replace(intentCobraFixtureFacade, `command.Use = "recover"`, `command.Use = "restore"`, 1))
	pending, err := state.PendingEvents(ctx, f.db, 0)
	if err != nil {
		t.Fatal(err)
	}
	window, _, reason, err := expandIntentGoalWindow(ctx, f.dir, f.db, f.cctx, pending, pending[:1], intentReplayConfig{targetEventSeqs: []int64{doc, fix}}, time.Now())
	if err != nil || reason != "" || len(window) == 0 || window[0].Seq != doc {
		t.Fatalf("later constructor escaped frozen target: %+v %q %v", window, reason, err)
	}
	for _, event := range window {
		if event.Seq != doc && event.Seq != fix {
			t.Fatalf("later constructor escaped frozen target: %+v", window)
		}
	}
	var captures []IntentCandidateCapture
	for _, event := range pending {
		ops, err := state.LoadCaptureOps(ctx, f.db, event.Seq)
		if err != nil {
			t.Fatal(err)
		}
		diff, err := BuildOpsDiffWithCap(ctx, f.dir, ops, intentSourceReferenceScanCap)
		if err != nil {
			t.Fatal(err)
		}
		captures = append(captures, IntentCandidateCapture{Event: event, Ops: ops, CapturedDiff: diff})
	}
	proved, err := proveIntentCobraCLIReferences(ctx, f.dir, "HEAD", captures)
	if err != nil {
		t.Fatal(err)
	}
	for _, capture := range proved {
		if len(capture.FileMetadata.IntentCLIReferences(capture.Event.Seq, capture.Event.Path)) > 0 {
			t.Fatalf("intermediate recover spelling lent authority to final restore: seq=%d old=%d final=%d", capture.Event.Seq, oldFacade, facade)
		}
	}
}
