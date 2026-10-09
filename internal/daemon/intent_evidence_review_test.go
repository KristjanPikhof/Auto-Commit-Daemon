package daemon

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/ai"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/checkpoint"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/config"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

type factualReviewPlanner struct {
	requests  []ai.IntentPlanRequestV2
	deadlines []time.Time
	wait      bool
	err       error
}

func (*factualReviewPlanner) Name() string { return "intent-factual-review-test" }
func (*factualReviewPlanner) PlanIntent(context.Context, ai.IntentPlanRequest) (ai.IntentPlan, error) {
	panic("native v2 planner required")
}
func (p *factualReviewPlanner) PlanIntentV2(ctx context.Context, req ai.IntentPlanRequestV2) (ai.IntentPlanV2, error) {
	p.requests = append(p.requests, req)
	deadline, _ := ctx.Deadline()
	p.deadlines = append(p.deadlines, deadline)
	if p.err != nil && len(p.requests) > 1 {
		return ai.IntentPlanV2{}, p.err
	}
	candidate := ai.IntentCandidateAssignment{CandidateID: "recognition-pauses", Purpose: "restore recognition after pauses with its focused regression", Readiness: ai.IntentCandidateWait,
		MissingCompanions: []string{"the recognition implementation and focused regression were not supplied"}, GroupingReason: "the recognition function and its direct test describe one restart goal"}
	for _, capture := range req.OfferedCaptures {
		candidate.SelectedSeqs = append(candidate.SelectedSeqs, capture.Seq)
	}
	if len(p.requests) > 1 && !p.wait {
		candidate.Readiness, candidate.MissingCompanions = ai.IntentCandidateReady, nil
		candidate.Subject = "Restore recognition after pauses"
		candidate.Body = "- Resume recognition after a pause and cover its restart behavior"
	}
	return ai.IntentPlanV2{ProtocolVersion: ai.IntentPlannerProtocolV2, Candidates: []ai.IntentCandidateAssignment{candidate}}, nil
}

func TestIntentFactualWaitReviewPublishesGoalAndRetainsCooldown(t *testing.T) {
	t.Parallel()
	for _, waiting := range []bool{false, true} {
		name := "publishes"
		if waiting {
			name = "still_waits_after_restart"
		}
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			f := newCaptureFixture(t)
			if _, err := BootstrapShadow(ctx, f.dir, f.db, f.cctx); err != nil {
				t.Fatal(err)
			}
			writePublicationFile(t, f, "recognition.go", "package app\nfunc ResumeRecognition() bool { return true }\n")
			writePublicationFile(t, f, "recognition_test.go", "package app\nimport \"testing\"\nfunc TestRecognitionRestarts(t *testing.T) { if !ResumeRecognition() { t.Fatal(\"recognition did not resume\") } }\n")
			captured := capturePublicationFiles(t, f)
			if !captured.Protected {
				t.Fatal("recognition goal was not checkpoint protected")
			}
			events, err := state.PendingEvents(ctx, f.db, 0)
			if err != nil || len(events) != 2 {
				t.Fatalf("goal captures=%+v err=%v", events, err)
			}
			seqs := []int64{events[0].Seq, events[1].Seq}
			planner := &factualReviewPlanner{wait: waiting}
			ts := intentPlannerHealthTimestamp(time.Now())
			drain := state.PublicationDrain{ID: "factual-wait-review", CheckpointID: captured.CheckpointID, WorktreeID: checkpoint.WorktreeID(f.dir), BranchRef: f.cctx.BranchRef, BranchGeneration: f.cctx.BranchGeneration,
				CommitStrategy: "intent", CommitFormat: "imperative", Provider: planner.Name(), ProviderFingerprint: "sha256:" + strings.Repeat("0", 64), Phase: state.PublicationDrainSemantic,
				TargetEventCount: 2, EventSeqs: seqs, CreatedTS: ts, UpdatedTS: ts, LastProgressTS: ts}
			if _, err := state.PreparePublicationDrain(ctx, f.db, drain); err != nil {
				t.Fatal(err)
			}
			writePublicationFile(t, f, "later.md", "# Later independent release checks\n")
			if later := capturePublicationFiles(t, f); !later.Protected {
				t.Fatal("later work was not protected")
			}
			retries := 2
			opts := ReplayOpts{GitDir: f.gitDir, CommitStrategy: ai.CommitStrategyIntent, IntentPlanner: planner, IntentPlannerProvider: planner.Name(), IntentPreset: config.PresetBalanced,
				IntentIncludeDiffs: true, IntentRetryLimit: &retries, IntentWindow: 2, IntentBypassBatchWait: true, IntentVerificationMode: "structural", PublicationDrain: &drain}
			result, err := Replay(ctx, f.dir, f.db, f.cctx, opts)
			if err != nil || len(planner.requests) != 2 || result.Failed != 0 || result.Disposition == ReplayDispositionNeedsAttention {
				t.Fatalf("bounded factual review=%+v calls=%d err=%v", result, len(planner.requests), err)
			}
			correction := planner.requests[1].RetryCorrection
			if planner.deadlines[0].IsZero() || !planner.deadlines[0].Equal(planner.deadlines[1]) {
				t.Fatalf("factual review extended the provider deadline: %v", planner.deadlines)
			}
			if !strings.Contains(correction, "recognition.go") || !strings.Contains(correction, "recognition_test.go") || !strings.Contains(correction, "no renderer clipping or omission") || strings.Contains(correction, "failed atomicity") {
				t.Fatalf("review did not describe actual supplied facts: %q", correction)
			}
			for _, request := range planner.requests {
				if !reflect.DeepEqual(offeredIntentSeqs(request), seqs) {
					t.Fatalf("review consumed later work or changed target: %+v", request.OfferedCaptures)
				}
			}
			if waiting {
				if result.Published != 0 || result.Disposition != ReplayDispositionTransientWait {
					t.Fatalf("facts manufactured readiness: %+v", result)
				}
				retry, found, err := loadIntentSemanticRetry(ctx, f.db)
				if err != nil || !found || retry.ReviewCount != 1 || retry.RetryAtTS-retry.ScheduledAtTS != (5*time.Minute).Seconds() {
					t.Fatalf("genuine WAIT lost its normal deadline: %+v found=%t err=%v", retry, found, err)
				}
				path := f.db.Path()
				if err := f.db.Close(); err != nil {
					t.Fatal(err)
				}
				f.db, err = state.Open(ctx, path)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = f.db.Close() })
				for i := 0; i < 3; i++ {
					result, err = Replay(ctx, f.dir, f.db, f.cctx, opts)
					if err != nil || result.Published != 0 || result.Disposition != ReplayDispositionTransientWait || len(planner.requests) != 2 {
						t.Fatalf("unchanged restart repeated review: %+v calls=%d err=%v", result, len(planner.requests), err)
					}
					saved, found, err := loadIntentSemanticRetry(ctx, f.db)
					if err != nil || !found || saved.RetryAtTS != retry.RetryAtTS || saved.ScheduledAtTS != retry.ScheduledAtTS || saved.ReviewCount != retry.ReviewCount {
						t.Fatalf("restart moved the deadline: %+v err=%v", saved, err)
					}
				}
			} else {
				if result.Published != 2 || strings.TrimSpace(mustGitOutput(t, f.dir, "log", "-1", "--format=%s")) != "Restore recognition after pauses" {
					t.Fatalf("factual review did not publish its purposeful goal: %+v", result)
				}
				final, err := UpdatePublicationDrainAfterReplay(ctx, f.db, drain, result, nil, time.Now())
				if err != nil || final.Phase != state.PublicationDrainCompleted || !reflect.DeepEqual(final.EventSeqs, seqs) {
					t.Fatalf("publication changed its target: %+v err=%v", final, err)
				}
			}
			pending, err := state.PendingEvents(ctx, f.db, 0)
			wantPending := 1
			if waiting {
				wantPending = 3
			}
			if err != nil || len(pending) != wantPending || pending[len(pending)-1].Path != "later.md" {
				t.Fatalf("frozen publication lost protected work: %+v err=%v", pending, err)
			}
			if attention, err := hasUnresolvedIntentV2CandidateAttention(ctx, f.db); err != nil || attention {
				t.Fatalf("shared status/list reports required action: %t err=%v", attention, err)
			}
		})
	}
}

func TestIntentFactualWaitReviewKeepsReadyAcrossInterruptedCorrection(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	db := openIntentCandidateTestDB(t)
	req, input, _ := semanticRetryRequest(t)
	req.OfferedCaptures = []ai.OfferedCapture{{Seq: 1, Path: "recognition.md", Op: "modify", CapturedDiff: "+# Resume recognition after pauses\n", FileMetadata: &ai.IntentFileMetadata{Kind: "text"}},
		{Seq: 2, Path: "retry.md", Op: "modify", CapturedDiff: "+# Recognition retry guidance\n", FileMetadata: &ai.IntentFileMetadata{Kind: "text"}}}
	ready := restoredSemanticPlan().Candidates[0]
	ready.SelectedSeqs = []int64{1}
	ready.Subject, ready.Purpose = "Document recognition pause recovery", "document recognition pause recovery"
	waiting := remoteWaitingGoal()
	waiting.CandidateID, waiting.SelectedSeqs = "recognition-pauses", []int64{2}
	planner := &factualMixedReviewPlanner{ready: ready, waiting: waiting}
	_, _, _, _, _, _, run, err := chooseIntentCandidatePlan(ctx, req, planner, nil, 2, input.Preset, nil, db, input)
	var circuit *IntentPlannerCircuitOpenError
	if !errors.As(err, &circuit) || planner.calls != 2 || run.AttemptCount != 1 || !intentPlanRunEvidenceReviewed(run) || !reflect.DeepEqual(run.PreservedGroups, [][]int64{{1}}) {
		t.Fatalf("interrupted factual review lost ready progress: %+v calls=%d err=%v", run, planner.calls, err)
	}
	if len(planner.req.OfferedCaptures) != 1 || planner.req.OfferedCaptures[0].Seq != 2 {
		t.Fatalf("validated READY was offered again: %+v", planner.req)
	}
	path := db.Path()
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = state.Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	planner.reconnected = true
	plan, _, _, _, _, _, run, err := chooseIntentCandidatePlan(ctx, req, planner, nil, 2, input.Preset, nil, db, input)
	if err != nil || planner.calls != 3 || len(plan.Candidates) != 2 || !reflect.DeepEqual(plan.Candidates[0], ready) || run.AttemptCount != 2 || !intentPlanRunEvidenceReviewed(run) {
		t.Fatalf("restart repeated factual review or lost READY: %+v run=%+v calls=%d err=%v", plan, run, planner.calls, err)
	}
}

type factualMixedReviewPlanner struct {
	ready, waiting ai.IntentCandidateAssignment
	calls          int
	req            ai.IntentPlanRequestV2
	reconnected    bool
}

func (*factualMixedReviewPlanner) Name() string { return "factual-mixed-review" }
func (p *factualMixedReviewPlanner) PlanIntentV2(_ context.Context, req ai.IntentPlanRequestV2) (ai.IntentPlanV2, error) {
	p.calls++
	p.req = req
	if p.calls == 1 {
		return ai.IntentPlanV2{ProtocolVersion: ai.IntentPlannerProtocolV2, Candidates: []ai.IntentCandidateAssignment{p.ready, p.waiting}}, nil
	}
	if !p.reconnected {
		return ai.IntentPlanV2{}, errors.New("connection reset by peer")
	}
	return ai.IntentPlanV2{ProtocolVersion: ai.IntentPlannerProtocolV2, Candidates: []ai.IntentCandidateAssignment{p.waiting}}, nil
}

func TestIntentFactualReviewInventoryRejectsClippedAndUnknownEvidence(t *testing.T) {
	t.Parallel()
	complete := ai.OfferedCapture{Seq: 1, Path: "recognition.go", CapturedDiff: "+func ResumeRecognition() {}\n", FileMetadata: &ai.IntentFileMetadata{Kind: "text"}}
	for _, variant := range []string{"complete", "clipped", "marker", "omitted", "binary", "no_metadata", "no_diff"} {
		capture := complete
		metadata := *complete.FileMetadata
		capture.FileMetadata = &metadata
		switch variant {
		case "clipped":
			capture.CapturedDiffTruncated = true
		case "marker":
			capture.CapturedDiff += "\n... <truncated> ...\n"
		case "omitted":
			capture.FileMetadata.DiffOmittedReason = "truncated"
		case "binary":
			capture.FileMetadata.Kind = "binary"
		case "no_metadata":
			capture.FileMetadata = nil
		case "no_diff":
			capture.CapturedDiff = ""
		}
		got := intentRecordedEvidenceCorrection(ai.IntentPlanRequestV2{OfferedCaptures: []ai.OfferedCapture{capture}}, []int64{1})
		if (got != "") != (variant == "complete") {
			t.Fatalf("%s falsely claimed supplied complete text: %q", variant, got)
		}
	}
	req := ai.IntentPlanRequestV2{OfferedCaptures: []ai.OfferedCapture{complete}, Candidates: []ai.IntentCandidateSummary{
		{CandidateID: "published-recognition-test", Status: state.IntentCandidatePublished, SelectedSeqs: []int64{2}, CapturedEvidence: []ai.OfferedCapture{{Seq: 2, Path: "recognition_test.go", CapturedDiff: "+func TestResume() { ResumeRecognition() }\n"}}},
		{CandidateID: "unrelated-published", Status: state.IntentCandidatePublished, SelectedSeqs: []int64{3}, CapturedEvidence: []ai.OfferedCapture{{Seq: 3, Path: "camera.go", CapturedDiff: "+func OpenCamera() {}\n"}}},
	}}
	got := intentRecordedEvidenceCorrection(req, []int64{1})
	if !strings.Contains(got, "published-recognition-test") || strings.Contains(got, "unrelated-published") || !strings.Contains(got, "read-only context") {
		t.Fatalf("published facts were not precise/read-only: %q", got)
	}
	binary := complete
	binary.CapturedDiff, binary.FileMetadata = "", &ai.IntentFileMetadata{Kind: "binary"}
	req.OfferedCaptures = []ai.OfferedCapture{binary}
	req.Dependencies = []ai.IntentCaptureDependency{{FromSeq: 1, ToSeq: 2, Strength: ai.IntentDependencyHard, Kind: "source_test"}}
	if got := intentRecordedEvidenceCorrection(req, []int64{1}); got != "" {
		t.Fatalf("binary-only WAIT used published context to spend a review: %q", got)
	}
	req.OfferedCaptures = nil
	for i := int64(1); i <= 256; i++ {
		capture := complete
		capture.Seq, capture.Path = i, strings.Repeat("recognition", 20)+".go"
		req.OfferedCaptures = append(req.OfferedCaptures, capture)
	}
	got = intentRecordedEvidenceCorrection(req, offeredIntentSeqs(req))
	if len([]rune(got)) > ai.IntentAtomicityCorrectionCap || !strings.Contains(got, "Keep WAIT") {
		t.Fatalf("facts exceeded their bound or lost safety guidance: %d", len([]rune(got)))
	}
}
