package daemon

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/ai"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/checkpoint"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/config"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

type waitMembershipReviewPlanner struct{ calls int }

func (p *waitMembershipReviewPlanner) Name() string { return "wait-membership-review" }
func (p *waitMembershipReviewPlanner) PlanIntent(context.Context, ai.IntentPlanRequest) (ai.IntentPlan, error) {
	return ai.IntentPlan{}, errors.New("native v2 required")
}
func (p *waitMembershipReviewPlanner) PlanIntentV2(_ context.Context, req ai.IntentPlanRequestV2) (ai.IntentPlanV2, error) {
	p.calls++
	plan := ai.IntentPlanV2{ProtocolVersion: ai.IntentPlannerProtocolV2}
	for _, capture := range req.OfferedCaptures {
		subject := map[string]string{
			"SpeechEngine.swift":      "Restore speech recognition after pauses",
			"TranslationEngine.swift": "Preserve the selected translation locale",
			"review.md":               "Document goal review intervals",
			"fixtures.md":             "Document isolated fixture ownership",
			"release.md":              "Document release verification checks",
		}[capture.Path]
		id := capture.Path
		for _, candidate := range req.Candidates {
			if containsIntentSeq(candidate.SelectedSeqs, capture.Seq) && candidate.Status != "published" {
				id = candidate.CandidateID
			}
		}
		plan.Candidates = append(plan.Candidates, ai.IntentCandidateAssignment{
			CandidateID: id, SelectedSeqs: []int64{capture.Seq}, Purpose: subject,
			Readiness: ai.IntentCandidateReady, Subject: subject,
			Body:           "- Keep this complete behavior independently reviewable",
			GroupingReason: "the recorded behavior is independently complete",
		})
	}
	return plan, nil
}

func TestIntentRejectedReadyReviewRetainsWaitMembershipAndPublishesAfterRestart(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	f, input, _, plan, rejected := semanticRejectedFixture(t)
	for path, body := range map[string]string{
		"review.md":   "# Review intervals\nReview uncertain goals after five and ten minutes.\n",
		"fixtures.md": "# Fixture ownership\nGive each test its own state database.\n",
		"release.md":  "# Release verification\nCheck the release build before distribution.\n",
	} {
		writePublicationFile(t, f, path, body)
	}
	protected := capturePublicationFiles(t, f)
	if !protected.Protected {
		t.Fatalf("mixed goals unprotected: %+v", protected)
	}
	pending, err := state.PendingEvents(ctx, f.db, 0)
	if err != nil || len(pending) != 5 {
		t.Fatalf("pending=%+v err=%v", pending, err)
	}
	input.Captures, input.TargetEventSeqs = nil, nil
	bySeq := map[int64]IntentCandidateCapture{}
	var readySeq int64
	for _, event := range pending {
		ops, err := state.LoadCaptureOps(ctx, f.db, event.Seq)
		if err != nil {
			t.Fatal(err)
		}
		diff, err := BuildOpsDiffWithCap(ctx, f.dir, ops, ai.IntentStageDiffCap)
		if err != nil {
			t.Fatal(err)
		}
		capture := IntentCandidateCapture{Event: event, Ops: ops, CapturedDiff: diff}
		input.Captures = append(input.Captures, capture)
		input.TargetEventSeqs = append(input.TargetEventSeqs, event.Seq)
		bySeq[event.Seq] = capture
		if strings.HasSuffix(event.Path, ".md") {
			assignment := ai.IntentCandidateAssignment{CandidateID: event.Path, SelectedSeqs: []int64{event.Seq},
				Purpose: "complete the recorded documentation goal", Readiness: ai.IntentCandidateWait,
				MissingCompanions: []string{"the documentation goal needs review"}, GroupingReason: "retain the protected documentation goal"}
			if event.Path == "release.md" {
				readySeq = event.Seq
				assignment.Readiness, assignment.MissingCompanions = ai.IntentCandidateReady, nil
				assignment.Purpose, assignment.Subject = "document release verification checks", "Document release verification checks"
				assignment.Body = "- Explain the checks required before distribution"
			}
			plan.Candidates = append(plan.Candidates, assignment)
		}
	}
	dependencies, err := BuildIntentCandidateDependencies(input.BranchRef, input.BranchGeneration, input.Captures, runtimeIntentDependencyHints(input.Captures), input.Now)
	if err != nil {
		t.Fatal(err)
	}
	req, err := buildIntentCandidateRequest(input, nil, dependencies, nil, input.Captures)
	if err != nil {
		t.Fatal(err)
	}
	result := IntentCandidateEvaluationResult{Decisions: []IntentCandidateDecision{rejected}}
	for _, assignment := range plan.Candidates[1:] {
		decision, err := evaluateIntentCandidateAssignment(ctx, f.db, input, plan, assignment, dependencies, nil, bySeq)
		if err != nil || decision.Publishable != (assignment.Readiness == ai.IntentCandidateReady) {
			t.Fatalf("mixed goal gates=%+v err=%v", decision, err)
		}
		result.Decisions = append(result.Decisions, decision)
	}
	for _, decision := range result.Decisions {
		if err := state.SaveIntentCandidate(ctx, f.db, decision.Candidate); err != nil {
			t.Fatal(err)
		}
	}
	run, err := newIntentPlanRun(req, input, 3)
	if err != nil {
		t.Fatal(err)
	}
	run, err = state.EnsureIntentPlanRun(ctx, f.db, run)
	if err != nil {
		t.Fatal(err)
	}
	if err := scheduleRejectedIntentGoalReview(ctx, f.db, input, req, plan, nil, run, &result); err != nil {
		t.Fatal(err)
	}
	run, found, err := state.IntentPlanRunByFingerprint(ctx, f.db, run.Fingerprint)
	wanted := append([]int64(nil), input.TargetEventSeqs...)
	for i, seq := range wanted {
		if seq == readySeq {
			wanted = append(wanted[:i], wanted[i+1:]...)
			break
		}
	}
	if err != nil || !found || !reflect.DeepEqual(run.UnresolvedSeqs, wanted) || !reflect.DeepEqual(run.PreservedGroups, [][]int64{{readySeq}}) {
		t.Fatalf("rejected READY lost WAIT goals: run=%+v want=%v err=%v", run, wanted, err)
	}
	retry, found, err := loadIntentSemanticRetry(ctx, f.db)
	if err != nil || !found || retry.ReviewCount != 1 || retry.RetryAtTS != intentPlannerHealthTimestamp(input.Now.Add(5*time.Minute)) {
		t.Fatalf("review deadline=%+v found=%t err=%v", retry, found, err)
	}
	// Reproduce the old worker's narrowed record. Due selection must repair
	// its view without changing the saved record or its existing deadline.
	run.UnresolvedSeqs = append([]int64(nil), rejected.Assignment.SelectedSeqs...)
	if err := state.UpdateIntentPlanRun(ctx, f.db, run); err != nil {
		t.Fatal(err)
	}
	window, err := dueIntentSemanticReviewWindow(ctx, f.db, pending, 20, secondsTime(retry.RetryAtTS))
	var offered []int64
	for _, event := range window {
		offered = append(offered, event.Seq)
	}
	if err != nil || !reflect.DeepEqual(offered, wanted) {
		t.Fatalf("legacy WAIT goals unreachable: offered=%v want=%v err=%v", offered, wanted, err)
	}
	beforeRepair, _, _ := loadIntentSemanticRetry(ctx, f.db)
	if beforeRepair != retry {
		t.Fatalf("read-only membership repair moved deadline: before=%+v after=%+v", retry, beforeRepair)
	}
	input.Now = input.Now.Add(time.Minute)
	if err := scheduleRejectedIntentGoalReview(ctx, f.db, input, req, plan, nil, run, &result); err != nil {
		t.Fatal(err)
	}
	if saved, _, err := loadIntentSemanticRetry(ctx, f.db); err != nil || saved != retry {
		t.Fatalf("membership repair reset cooldown: before=%+v after=%+v err=%v", retry, saved, err)
	}
	now := intentPlannerHealthTimestamp(time.Now())
	planner := &waitMembershipReviewPlanner{}
	drain := state.PublicationDrain{ID: "mixed-goal-review", CheckpointID: protected.CheckpointID, WorktreeID: checkpoint.WorktreeID(f.dir),
		BranchRef: input.BranchRef, BranchGeneration: input.BranchGeneration, CommitStrategy: "intent", CommitFormat: "imperative",
		Provider: planner.Name(), ProviderFingerprint: "sha256:" + strings.Repeat("0", 64), Phase: state.PublicationDrainSemantic,
		TargetEventCount: 5, EventSeqs: input.TargetEventSeqs, CreatedTS: now, UpdatedTS: now, LastProgressTS: now}
	if _, err := state.PreparePublicationDrain(ctx, f.db, drain); err != nil {
		t.Fatal(err)
	}
	writePublicationFile(t, f, "later.md", "# Later independent work\n")
	if later := capturePublicationFiles(t, f); !later.Protected {
		t.Fatalf("later work unprotected: %+v", later)
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
	if attention, err := hasUnresolvedIntentV2CandidateAttention(ctx, f.db); err != nil || attention {
		t.Fatalf("protected review asks for user action: %t err=%v", attention, err)
	}
	retry.RetryAtTS = intentPlannerHealthTimestamp(time.Now().Add(-time.Second))
	retry.ScheduledAtTS = retry.RetryAtTS - intentSemanticReviewDelay(retry.ReviewCount).Seconds()
	if err := saveIntentSemanticRetry(ctx, f.db, retry); err != nil {
		t.Fatal(err)
	}
	published, err := Replay(ctx, f.dir, f.db, f.cctx, ReplayOpts{GitDir: f.gitDir, CommitStrategy: ai.CommitStrategyIntent,
		IntentPlanner: planner, IntentPlannerProvider: planner.Name(), IntentPreset: config.PresetBalanced, IntentWindow: 20,
		IntentIncludeDiffs: true, IntentBypassBatchWait: true, IntentVerificationMode: "structural", PublicationDrain: &drain})
	if err != nil || published.Published != 5 || published.Failed != 0 || published.Disposition == ReplayDispositionNeedsAttention {
		t.Fatalf("due mixed review did not commit complete goals: %+v calls=%d err=%v", published, planner.calls, err)
	}
	remaining, err := state.PendingEvents(ctx, f.db, 0)
	if err != nil || len(remaining) != 1 || remaining[0].Path != "later.md" {
		t.Fatalf("review consumed later work or left old WAITs: %+v err=%v", remaining, err)
	}
	if _, err := git.Run(ctx, git.RunOpts{Dir: f.dir}, "cat-file", "-e", "HEAD:later.md"); err == nil {
		t.Fatal("later work escaped the frozen target")
	}
	completed, err := UpdatePublicationDrainAfterReplay(ctx, f.db, drain, published, nil, time.Now())
	if err != nil || completed.Phase != state.PublicationDrainCompleted || !reflect.DeepEqual(completed.EventSeqs, input.TargetEventSeqs) {
		t.Fatalf("visible publication target changed: %+v err=%v", completed, err)
	}
	if err := updateIntentV2EvaluationMeta(ctx, f.db, &RuntimeBundle{PresetID: "intent.balanced", PresetVersion: config.PresetCatalogVersion}, published, nil); err != nil {
		t.Fatal(err)
	}
	if attention, err := hasUnresolvedIntentV2CandidateAttention(ctx, f.db); err != nil || attention {
		t.Fatalf("committed goals still require action: %t err=%v", attention, err)
	}
}

func TestIntentLegacyWaitReviewRequiresCurrentPendingOwnership(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	db := openIntentCandidateTestDB(t)
	var captures []IntentCandidateCapture
	plan := ai.IntentPlanV2{ProtocolVersion: ai.IntentPlannerProtocolV2}
	for i, name := range []string{"waiting", "published", "ready", "foreign", "other-owner", "later"} {
		capture := appendIntentCandidateCapture(t, db, name+".md", "create", "", "recorded-"+name)
		captures = append(captures, capture)
		candidate := state.IntentCandidate{ID: name, BranchRef: capture.Event.BranchRef, BranchGeneration: capture.Event.BranchGeneration,
			Status: state.IntentCandidateWaiting, Readiness: state.IntentReadinessWait, Purpose: "review the recorded documentation goal",
			CreatedTS: 100, UpdatedTS: 100, Events: []state.IntentCandidateEvent{{EventSeq: capture.Event.Seq, EventRole: "documentation"}}}
		assignment := ai.IntentCandidateAssignment{CandidateID: name, SelectedSeqs: []int64{capture.Event.Seq}, Readiness: ai.IntentCandidateWait}
		switch name {
		case "published":
			candidate.Status = state.IntentCandidatePublished
			candidate.PublishedCommitOID = sql.NullString{String: "published-head", Valid: true}
		case "ready":
			candidate.Status, candidate.Readiness = state.IntentCandidateOpen, state.IntentReadinessReady
			assignment.Readiness = ai.IntentCandidateReady
		case "foreign":
			candidate.BranchRef = "refs/heads/other"
			if _, err := db.SQL().ExecContext(ctx, "UPDATE capture_events SET branch_ref=? WHERE seq=?", candidate.BranchRef, capture.Event.Seq); err != nil {
				t.Fatal(err)
			}
		case "other-owner":
			assignment.CandidateID = "obsolete-owner"
		}
		if err := state.SaveIntentCandidate(ctx, db, candidate); err != nil {
			t.Fatal(err)
		}
		if i < 5 {
			plan.Candidates = append(plan.Candidates, assignment)
		}
	}
	run, err := state.EnsureIntentPlanRun(ctx, db, state.IntentPlanRun{Fingerprint: "sha256:" + strings.Repeat("a", 64),
		BranchRef: captures[0].Event.BranchRef, BranchGeneration: captures[0].Event.BranchGeneration, AttemptLimit: 3})
	if err != nil {
		t.Fatal(err)
	}
	run.Completed = true
	run.ProgressState, run.ResolutionMode = sql.NullString{String: "waiting_semantic_retry", Valid: true}, sql.NullString{String: "waiting_semantic_retry", Valid: true}
	if err := storeResolvedIntentPlanRun(&run, plan, nil); err != nil {
		t.Fatal(err)
	}
	if err := state.UpdateIntentPlanRun(ctx, db, run); err != nil {
		t.Fatal(err)
	}
	retry := IntentSemanticRetrySnapshot{Version: 1, BranchRef: run.BranchRef, BranchGeneration: run.BranchGeneration,
		EvidenceFingerprint: "sha256:" + strings.Repeat("b", 64), PlanFingerprint: run.Fingerprint, ReviewCount: 1, ScheduledAtTS: 100, RetryAtTS: 400}
	if err := saveIntentSemanticRetry(ctx, db, retry); err != nil {
		t.Fatal(err)
	}
	pending, err := state.PendingEvents(ctx, db, 0)
	if err != nil {
		t.Fatal(err)
	}
	for _, now := range []time.Time{secondsTime(399), secondsTime(400), secondsTime(450)} {
		window, err := dueIntentSemanticReviewWindow(ctx, db, pending, 20, now)
		var seqs []int64
		for _, event := range window {
			seqs = append(seqs, event.Seq)
		}
		var want []int64
		if now.Unix() >= 400 {
			want = []int64{captures[0].Event.Seq}
		}
		sort.Slice(seqs, func(i, j int) bool { return seqs[i] < seqs[j] })
		if err != nil || !reflect.DeepEqual(seqs, want) {
			t.Fatalf("derived legacy review chose unsafe membership: now=%v selected=%v want=%v err=%v", now, seqs, want, err)
		}
	}
	if saved, found, err := loadIntentSemanticRetry(ctx, db); err != nil || !found || saved != retry {
		t.Fatalf("read-only legacy membership changed the deadline: %+v found=%t err=%v", saved, found, err)
	}
}
