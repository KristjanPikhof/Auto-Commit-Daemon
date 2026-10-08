package daemon

import (
	"context"
	"errors"
	"os/exec"
	"reflect"
	"testing"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/ai"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/config"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

type intentGoalWindowPlanner struct{ intentCandidatePlannerStub }

func (*intentGoalWindowPlanner) PlanIntent(context.Context, ai.IntentPlanRequest) (ai.IntentPlan, error) {
	return ai.IntentPlan{}, errors.New("legacy planner must not bypass native goal readiness")
}

func TestIntentGoalWindowFindsLateHelperAndTestBeforePublication(t *testing.T) {
	f := newCaptureFixture(t)
	ctx := context.Background()
	seedTrackedFileCommit(t, ctx, f, "exporter.go", "package archive\n\nfunc ExportRecording(text string) string { return text }\n")
	if _, err := BootstrapShadow(ctx, f.dir, f.db, f.cctx); err != nil {
		t.Fatal(err)
	}
	bodies := map[string]string{
		"exporter.go":      "package archive\n\nfunc ExportRecording(text string) string { return BuildRecordingArchive(text) }\n",
		"builder.go":       "package archive\n\nfunc BuildRecordingArchive(text string) string { return \"archive:\" + text }\n",
		"exporter_test.go": "package archive\n\nimport \"testing\"\n\nfunc TestRecordingExport(t *testing.T) { if ExportRecording(\"hello\") != \"archive:hello\" { t.Fatal(\"invalid archive\") } }\n",
	}
	implementation := captureSamePathEdit(t, ctx, f, "exporter.go", bodies["exporter.go"])
	unrelated := captureSamePathEdit(t, ctx, f, "walkthrough.md", "# Recovery walkthrough\n")
	helper := captureSamePathEdit(t, ctx, f, "builder.go", bodies["builder.go"])
	test := captureSamePathEdit(t, ctx, f, "exporter_test.go", bodies["exporter_test.go"])
	planner := &intentGoalWindowPlanner{intentCandidatePlannerStub{plan: ai.IntentPlanV2{
		ProtocolVersion: ai.IntentPlannerProtocolV2,
		Candidates: []ai.IntentCandidateAssignment{{
			CandidateID: "archive-export", SelectedSeqs: []int64{implementation, helper, test},
			Purpose: "export recordings as archives", Readiness: ai.IntentCandidateReady,
			Subject:        "Add recording archive exports",
			Body:           "- Keep the builder and coverage with archive export behavior",
			GroupingReason: "the exporter requires the builder and its matching test",
		}},
	}}}
	before := revListCount(t, ctx, f.dir, "HEAD")
	summary, err := Replay(ctx, f.dir, f.db, f.cctx, ReplayOpts{
		GitDir: f.gitDir, CommitStrategy: ai.CommitStrategyIntent,
		IntentPlanner: planner, IntentPreset: config.PresetBalanced,
		IntentWindow: 1, IntentBypassBatchWait: true, IntentIncludeDiffs: true,
		IntentVerificationMode: "structural",
	})
	if err != nil {
		t.Fatal(err)
	}
	if summary.Published != 3 || planner.calls != 1 || revListCount(t, ctx, f.dir, "HEAD") != before+1 {
		t.Fatalf("implementation published before its companions: summary=%+v calls=%d", summary, planner.calls)
	}
	var offered []int64
	for _, capture := range planner.req.OfferedCaptures {
		offered = append(offered, capture.Seq)
	}
	if !reflect.DeepEqual(offered, []int64{implementation, helper, test}) {
		t.Fatalf("bounded goal window=%v", offered)
	}
	pending, err := state.PendingEvents(ctx, f.db, 0)
	if err != nil || len(pending) != 1 || pending[0].Seq != unrelated {
		t.Fatalf("unrelated work was pulled into archive goal: pending=%+v err=%v", pending, err)
	}
	for name, body := range bodies {
		actual, err := git.LsTreeBlobOID(ctx, f.dir, "HEAD", name)
		if err != nil {
			t.Fatal(err)
		}
		wanted, err := git.HashObjectStdin(ctx, f.dir, []byte(body))
		if err != nil || actual != wanted {
			t.Fatalf("published %s blob=%s want=%s err=%v", name, actual, wanted, err)
		}
	}
	// The checked-out code blobs were proven equal to the one published tree.
	command := exec.CommandContext(ctx, "go", "test", ".")
	command.Dir = f.dir
	command.Env = append(command.Environ(), "GO111MODULE=off")
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("published archive tree failed: %v\n%s", err, output)
	}
}

func TestIntentGoalWindowPreservesFrozenPublicationTarget(t *testing.T) {
	f := newCaptureFixture(t)
	ctx := context.Background()
	if _, err := BootstrapShadow(ctx, f.dir, f.db, f.cctx); err != nil {
		t.Fatal(err)
	}
	caller := captureSamePathEdit(t, ctx, f, "caller.go", "package archive\n\nfunc ExportRecording() string { return BuildRecordingArchive() }\n")
	_ = captureSamePathEdit(t, ctx, f, "helper.go", "package archive\n\nfunc BuildRecordingArchive() string { return \"archive\" }\n")
	pending, err := state.PendingEvents(ctx, f.db, 0)
	if err != nil {
		t.Fatal(err)
	}
	window, _, reason, err := expandIntentGoalWindow(ctx, f.dir, f.db, f.cctx, pending, pending[:1],
		intentReplayConfig{targetEventSeqs: []int64{caller}}, time.Now())
	if err != nil || reason != "" || len(window) != 1 || window[0].Seq != caller {
		t.Fatalf("later capture entered frozen publication target: window=%+v reason=%q err=%v", window, reason, err)
	}
}

func TestIntentGoalWindowWaitsForHotCompanion(t *testing.T) {
	f := newCaptureFixture(t)
	ctx := context.Background()
	if _, err := BootstrapShadow(ctx, f.dir, f.db, f.cctx); err != nil {
		t.Fatal(err)
	}
	callerPath, helperPath := "goal_quiet_caller.go", "goal_quiet_helper.go"
	_ = captureSamePathEdit(t, ctx, f, callerPath, "package archive\n\nfunc ExportRecording() string { return BuildRecordingArchive() }\n")
	_ = captureSamePathEdit(t, ctx, f, helperPath, "package archive\n\nfunc BuildRecordingArchive() string { return \"archive\" }\n")
	pending, err := state.PendingEvents(ctx, f.db, 0)
	if err != nil {
		t.Fatal(err)
	}
	wasEnabled := pathQuiescenceEnabled.Load()
	SetPathQuiescenceEnabled(true)
	t.Cleanup(func() {
		SetPathQuiescenceEnabled(wasEnabled)
		pathQuiescenceMu.Lock()
		delete(pathQuiescenceWrites, callerPath)
		delete(pathQuiescenceWrites, helperPath)
		pathQuiescenceMu.Unlock()
	})
	now := time.Now()
	RecordPathWrite(callerPath, now.Add(-time.Minute))
	RecordPathWrite(helperPath, now)
	window, _, reason, err := expandIntentGoalWindow(ctx, f.dir, f.db, f.cctx,
		pending, pending[:1], intentReplayConfig{pathQuiescence: 5 * time.Second}, now)
	if err != nil || len(window) != 0 || reason != "skipped_due_path_quiescence" {
		t.Fatalf("hot companion was bypassed: window=%+v reason=%q err=%v", window, reason, err)
	}
}

func TestIntentGoalWindowKeepsFreshGoalMovingPastIncompleteGoal(t *testing.T) {
	f := newCaptureFixture(t)
	ctx := context.Background()
	if _, err := BootstrapShadow(ctx, f.dir, f.db, f.cctx); err != nil {
		t.Fatal(err)
	}
	incomplete := captureSamePathEdit(t, ctx, f, "unfinished_export.go", "package archive\n\nfunc ExportRecording() string { return MissingArchiveBuilder() }\n")
	planner := &intentGoalWindowPlanner{intentCandidatePlannerStub{
		err: &ai.IntentPlanV2ValidationError{Message: "the goal cannot be completed from the supplied captures"},
	}}
	opts := ReplayOpts{GitDir: f.gitDir, CommitStrategy: ai.CommitStrategyIntent,
		IntentPlanner: planner, IntentPreset: config.PresetBalanced, IntentWindow: 1,
		IntentBypassBatchWait: true, IntentIncludeDiffs: true, IntentVerificationMode: "structural"}
	first, err := Replay(ctx, f.dir, f.db, f.cctx, opts)
	if err != nil || first.Published != 0 {
		t.Fatalf("incomplete goal published: %+v err=%v", first, err)
	}
	var timedWaits int
	if err := f.db.ReadSQL().QueryRowContext(ctx, `SELECT COUNT(*) FROM intent_plan_runs WHERE progress_state='waiting_semantic_retry'`).Scan(&timedWaits); err != nil || timedWaits != 1 {
		t.Fatalf("semantic retry did not retain incomplete goal: waits=%d err=%v", timedWaits, err)
	}
	firstCalls := planner.calls
	independent := captureSamePathEdit(t, ctx, f, "release_checklist.md", "# Release checklist\n")
	planner.err = nil
	planner.plan = ai.IntentPlanV2{ProtocolVersion: ai.IntentPlannerProtocolV2, Candidates: []ai.IntentCandidateAssignment{{
		CandidateID: "release-checklist", SelectedSeqs: []int64{independent},
		Purpose: "document release readiness checks", Readiness: ai.IntentCandidateReady,
		Subject:        "Document release readiness checks",
		GroupingReason: "the release guide is complete and independent of archive export",
	}}}
	second, err := Replay(ctx, f.dir, f.db, f.cctx, opts)
	if err != nil || second.Published != 1 || planner.calls != firstCalls+1 {
		t.Fatalf("fresh goal was starved by waiting goal: %+v calls=%d err=%v", second, planner.calls, err)
	}
	pending, err := state.PendingEvents(ctx, f.db, 0)
	if err != nil || len(pending) != 1 || pending[0].Seq != incomplete {
		t.Fatalf("incomplete goal lost protection: %+v err=%v", pending, err)
	}
	if len(planner.req.OfferedCaptures) != 1 || planner.req.OfferedCaptures[0].Seq != independent {
		t.Fatalf("fresh selection mixed unrelated waiting work: %+v", planner.req.OfferedCaptures)
	}
	if oid, err := git.LsTreeBlobOID(ctx, f.dir, "HEAD", "unfinished_export.go"); err != nil || oid != "" {
		t.Fatalf("unfinished source entered the independent documentation commit: oid=%s err=%v", oid, err)
	}
}
