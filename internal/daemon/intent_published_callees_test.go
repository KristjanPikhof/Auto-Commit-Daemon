package daemon

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/ai"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/checkpoint"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/config"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

type publishedCalleePlanner struct{ intentCandidatePlannerStub }

func (*publishedCalleePlanner) PlanIntent(context.Context, ai.IntentPlanRequest) (ai.IntentPlan, error) {
	panic("native v2 planner required")
}

func (p *publishedCalleePlanner) PlanIntentV2(_ context.Context, req ai.IntentPlanRequestV2) (ai.IntentPlanV2, error) {
	p.calls++
	p.req = req
	foundLoad, foundAttach, foundTest := false, false, false
	for _, candidate := range req.Candidates {
		if candidate.Status != state.IntentCandidatePublished {
			continue
		}
		for _, evidence := range candidate.CapturedEvidence {
			foundLoad = foundLoad || strings.Contains(evidence.CapturedDiff, " func loadIntentRecordedTypeScriptReferences(")
			foundAttach = foundAttach || strings.Contains(evidence.CapturedDiff, " func attachIntentTypeScriptReferences(")
			foundTest = foundTest || strings.Contains(evidence.CapturedDiff, "PlanIntentHistory()")
		}
	}
	assignment := ai.IntentCandidateAssignment{CandidateID: "recorded-history-callees", Purpose: "compose recorded history from existing reference helpers", Readiness: ai.IntentCandidateWait, MissingCompanions: []string{"the existing reference helper implementations and focused test are unavailable"}, GroupingReason: "the recorded history change calls named reference helpers and has an existing public API regression"}
	for _, capture := range req.OfferedCaptures {
		assignment.SelectedSeqs = append(assignment.SelectedSeqs, capture.Seq)
	}
	if foundLoad && foundAttach && foundTest {
		assignment.Readiness = ai.IntentCandidateReady
		assignment.MissingCompanions = nil
		assignment.Subject = "Compose recorded history references"
		assignment.Body = "- Reuse published reference helpers and the existing API regression"
	}
	return ai.IntentPlanV2{ProtocolVersion: ai.IntentPlannerProtocolV2, Candidates: []ai.IntentCandidateAssignment{assignment}}, nil
}

func TestIntentPublishedDirectCalleesCompleteHistoryImplementation(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	f := newCaptureFixture(t)
	before := "package app\nfunc PlanIntentHistory() int { return historyEvidence() }\nfunc historyEvidence() int { return 1 }\n"
	f.cctx.BaseHead = mustCommitPath(t, f.dir, "history.go", before, "Add recorded history planning")
	if _, err := BootstrapShadow(ctx, f.dir, f.db, f.cctx); err != nil {
		t.Fatal(err)
	}
	writePublicationFile(t, f, "intent_recorded_typescript.go", "package app\nfunc loadIntentRecordedTypeScriptReferences() int { return 2 }\nfunc attachIntentTypeScriptReferences(value int) int { return value }\n")
	writePublicationFile(t, f, "history_typescript_reference_test.go", "package app\nimport \"testing\"\nfunc TestRecordedHistory(t *testing.T) { if PlanIntentHistory() != 2 { t.Fatal(\"wrong recorded references\") } }\n")
	if captured := capturePublicationFiles(t, f); !captured.Protected {
		t.Fatal("published support was not protected")
	}
	events, err := state.PendingEvents(ctx, f.db, 0)
	if err != nil || len(events) != 2 {
		t.Fatalf("support=%+v err=%v", events, err)
	}
	published, err := Replay(ctx, f.dir, f.db, f.cctx, ReplayOpts{GitDir: f.gitDir, CommitStrategy: ai.CommitStrategyEvent})
	if err != nil || published.Published != 2 {
		t.Fatalf("support publication=%+v err=%v", published, err)
	}
	f.cctx.BaseHead = published.BaseHead
	var originals []state.IntentCandidate
	for _, event := range events {
		current, err := loadIntentCaptureEvent(ctx, f.db, event.Seq)
		if err != nil {
			t.Fatal(err)
		}
		candidate := state.IntentCandidate{ID: event.Path, BranchRef: f.cctx.BranchRef, BranchGeneration: f.cctx.BranchGeneration, Status: state.IntentCandidatePublished, Readiness: state.IntentReadinessReady, Purpose: "supply existing recorded history references", PublishedCommitOID: current.CommitOID, Events: []state.IntentCandidateEvent{{EventSeq: event.Seq, EventRole: "code"}}}
		if err := state.SaveIntentCandidate(ctx, f.db, candidate); err != nil {
			t.Fatal(err)
		}
		saved, _, err := state.IntentCandidateByID(ctx, f.db, candidate.ID)
		if err != nil {
			t.Fatal(err)
		}
		originals = append(originals, saved)
	}
	after := strings.Replace(before, "return 1", "return attachIntentTypeScriptReferences(loadIntentRecordedTypeScriptReferences())", 1)
	writePublicationFile(t, f, "history.go", after)
	if captured := capturePublicationFiles(t, f); !captured.Protected {
		t.Fatal("history implementation was not protected")
	}
	pending, err := state.PendingEvents(ctx, f.db, 0)
	if err != nil || len(pending) != 1 {
		t.Fatalf("pending=%+v err=%v", pending, err)
	}
	seq := pending[0].Seq
	var checkpointID string
	if err := f.db.ReadSQL().QueryRowContext(ctx, "SELECT checkpoint_id FROM checkpoint_events WHERE event_seq=?", seq).Scan(&checkpointID); err != nil {
		t.Fatal(err)
	}
	ts := intentPlannerHealthTimestamp(time.Now())
	drain := state.PublicationDrain{ID: "published-history-callees", CheckpointID: checkpointID, WorktreeID: checkpoint.WorktreeID(f.dir), BranchRef: f.cctx.BranchRef, BranchGeneration: f.cctx.BranchGeneration, CommitStrategy: "intent", CommitFormat: "imperative", Provider: "intent-v2-test", ProviderFingerprint: "sha256:" + strings.Repeat("0", 64), Phase: state.PublicationDrainSemantic, TargetEventCount: 1, EventSeqs: []int64{seq}, CreatedTS: ts, UpdatedTS: ts, LastProgressTS: ts}
	if _, err := state.PreparePublicationDrain(ctx, f.db, drain); err != nil {
		t.Fatal(err)
	}
	writePublicationFile(t, f, "later.md", "# Later unrelated work\n")
	if captured := capturePublicationFiles(t, f); !captured.Protected {
		t.Fatal("later work was not protected")
	}
	planner := &publishedCalleePlanner{}
	result, err := Replay(ctx, f.dir, f.db, f.cctx, ReplayOpts{GitDir: f.gitDir, CommitStrategy: ai.CommitStrategyIntent, IntentPlanner: planner, IntentPreset: config.PresetBalanced, IntentIncludeDiffs: true, IntentWindow: 1, IntentBypassBatchWait: true, IntentVerificationMode: "structural", PublicationDrain: &drain})
	if err != nil || result.Published != 1 || result.Failed != 0 || result.Disposition == ReplayDispositionNeedsAttention || planner.calls != 1 {
		t.Fatalf("history did not publish from baseline callees: %+v calls=%d err=%v", result, planner.calls, err)
	}
	if len(planner.req.OfferedCaptures) != 1 || planner.req.OfferedCaptures[0].Seq != seq {
		t.Fatal("published helpers or later work gained assignment authority")
	}
	for _, original := range originals {
		current, found, err := state.IntentCandidateByID(ctx, f.db, original.ID)
		if err != nil || !found || !reflect.DeepEqual(current, original) {
			t.Fatalf("published provenance changed: %+v err=%v", current, err)
		}
	}
	pending, err = state.PendingEvents(ctx, f.db, 0)
	if err != nil || len(pending) != 1 || pending[0].Path != "later.md" {
		t.Fatalf("later capture escaped frozen target: %+v err=%v", pending, err)
	}
	if got := strings.TrimSpace(mustGitOutput(t, f.dir, "log", "-1", "--format=%s")); got != "Compose recorded history references" {
		t.Fatalf("commit lost its goal: %q", got)
	}
	progress, err := UpdatePublicationDrainAfterReplay(ctx, f.db, drain, result, nil, time.Now())
	if err != nil || progress.Phase != state.PublicationDrainCompleted || progress.TargetEventCount != 1 {
		t.Fatalf("frozen goal progress=%+v err=%v", progress, err)
	}
	if attention, err := hasUnresolvedIntentV2CandidateAttention(ctx, f.db); err != nil || attention {
		t.Fatalf("status/list require action after safe publication: %t err=%v", attention, err)
	}
}

func TestIntentRecordedDirectCalleesRejectShadowedAndQuotedCalls(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, definition string
		want             bool
	}{
		{"direct", "func Changed() int { return loadReferences() }", true},
		{"parameter", "func Changed(loadReferences func() int) int { return loadReferences() }", false},
		{"local", "func Changed() int { loadReferences := func() int { return 2 }; return loadReferences() }", false},
		{"selector", "func Changed() int { return foreign.loadReferences() }", false},
		{"quoted", "func Changed() string { return \"loadReferences()\" }", false},
		{"comment", "func Changed() int { /* loadReferences() */ return 2 }", false},
		{"same_file", "func Changed() int { return loadReferences() }\nfunc loadReferences() int { return 2 }", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			contents := "package app\n" + tc.definition + "\n"
			diff := "+" + strings.Split(tc.definition, "\n")[0] + "\n"
			calls := intentGoRecordedCallContext("history.go", contents, diff, intentSourceReferenceContextCap)
			found := false
			for _, name := range calls.calledFunctions {
				found = found || name == "loadReferences"
			}
			if found != tc.want {
				t.Fatalf("direct callee=%t want=%t inventory=%+v", found, tc.want, calls)
			}
		})
	}
}

func TestIntentPublishedDirectCalleeDeclarationsRequirePackageAndOwner(t *testing.T) {
	t.Parallel()
	source := intentGoRegressionSource{packageName: "app", calls: map[string]bool{"loadReferences": true}}
	for _, tc := range []struct {
		code string
		want bool
	}{
		{"package app\nfunc loadReferences() int { return 2 }\n", true},
		{"package other\nfunc loadReferences() int { return 2 }\n", false},
		{"package app\n// func loadReferences() int { return 2 }\n", false},
		{"package app\nfunc Other() string { return \"func loadReferences() int\" }\n", false},
		{"package app\ntype Receiver struct{}\nfunc (Receiver) loadReferences() int { return 2 }\n", false},
		{"package app\nvar loadReferences = func() int { return 2 }\n", false},
	} {
		got := intentGoPublishedReferenceContext("helpers.go", []byte(tc.code), source)
		if (got != "") != tc.want {
			t.Fatalf("published declaration=%q want=%t source=%s", got, tc.want, tc.code)
		}
	}
	if got := intentGoPublishedReferenceContext("helpers.go", []byte(fmt.Sprintf("package app\nfunc loadReferences(%s) {}\n", strings.Repeat("x", intentSourceReferenceContextCap+1))), source); got != "" {
		t.Fatal("partial declaration exceeded context bound")
	}
}
