package daemon

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/ai"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/config"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

type intentCandidatePlannerStub struct {
	plan ai.IntentPlanV2
	err  error
	req  ai.IntentPlanRequestV2
}

func (p *intentCandidatePlannerStub) Name() string { return "intent-v2-test" }

func (p *intentCandidatePlannerStub) PlanIntentV2(
	_ context.Context,
	req ai.IntentPlanRequestV2,
) (ai.IntentPlanV2, error) {
	p.req = req
	return p.plan, p.err
}

type correctingIntentCandidatePlannerStub struct {
	calls int
	reqs  []ai.IntentPlanRequestV2
}

func (p *correctingIntentCandidatePlannerStub) Name() string { return "intent-v2-correcting-test" }

func (p *correctingIntentCandidatePlannerStub) PlanIntentV2(
	_ context.Context,
	req ai.IntentPlanRequestV2,
) (ai.IntentPlanV2, error) {
	p.calls++
	p.reqs = append(p.reqs, req)
	if p.calls == 1 {
		return ai.IntentPlanV2{
			ProtocolVersion: ai.IntentPlannerProtocolV2,
			Candidates: []ai.IntentCandidateAssignment{{
				CandidateID: "mega",
				SelectedSeqs: []int64{
					req.OfferedCaptures[0].Seq,
					req.OfferedCaptures[1].Seq,
				},
				Purpose:        "mix independent changes",
				Readiness:      ai.IntentCandidateReady,
				Subject:        "Mix independent changes",
				GroupingReason: "same time window",
			}},
		}, nil
	}
	var candidates []ai.IntentCandidateAssignment
	for _, capture := range req.OfferedCaptures {
		candidates = append(candidates, ai.IntentCandidateAssignment{
			CandidateID:    fmt.Sprintf("candidate-%d", capture.Seq),
			SelectedSeqs:   []int64{capture.Seq},
			Purpose:        "separate independent change",
			Readiness:      ai.IntentCandidateReady,
			Subject:        "Separate independent change",
			GroupingReason: "atomicity correction split disconnected components",
		})
	}
	return ai.IntentPlanV2{
		ProtocolVersion: ai.IntentPlannerProtocolV2,
		Candidates:      candidates,
	}, nil
}

type intentCandidateV1PlannerStub struct{}

func (intentCandidateV1PlannerStub) Name() string { return "intent-v1-test" }

func (intentCandidateV1PlannerStub) PlanIntent(
	_ context.Context,
	req ai.IntentPlanRequest,
) (ai.IntentPlan, error) {
	var selected []int64
	for _, capture := range req.OfferedCaptures {
		selected = append(selected, capture.Seq)
	}
	return ai.IntentPlan{
		SelectedSeqs: selected, Subject: "Update compatible candidate",
		GroupingReason: "legacy compatibility selection",
	}, nil
}

type invalidIntentCandidatePlannerStub struct {
	calls int
}

func (p *invalidIntentCandidatePlannerStub) Name() string {
	return "intent-v2-invalid-test"
}

func (p *invalidIntentCandidatePlannerStub) PlanIntentV2(
	_ context.Context,
	req ai.IntentPlanRequestV2,
) (ai.IntentPlanV2, error) {
	p.calls++
	return ai.IntentPlanV2{
		ProtocolVersion: ai.IntentPlannerProtocolV2,
		Candidates: []ai.IntentCandidateAssignment{{
			CandidateID: "invalid-merged",
			SelectedSeqs: []int64{
				req.OfferedCaptures[0].Seq,
				req.OfferedCaptures[1].Seq,
			},
			Purpose: "merge disconnected captures", Readiness: ai.IntentCandidateReady,
			Subject:        "Merge disconnected captures",
			GroupingReason: "invalid grouping for retry budget test",
		}},
	}, nil
}

type failingIntentCandidatePlannerStub struct {
	calls int
}

func (p *failingIntentCandidatePlannerStub) Name() string {
	return "intent-v2-failing-test"
}

func (p *failingIntentCandidatePlannerStub) PlanIntentV2(
	context.Context,
	ai.IntentPlanRequestV2,
) (ai.IntentPlanV2, error) {
	p.calls++
	return ai.IntentPlanV2{}, errors.New("provider transport unavailable")
}

type halfOpenCancelIntentCandidatePlannerStub struct {
	calls   int
	started chan struct{}
}

func (p *halfOpenCancelIntentCandidatePlannerStub) Name() string {
	return "intent-v2-half-open-cancel-test"
}

func (p *halfOpenCancelIntentCandidatePlannerStub) PlanIntentV2(
	ctx context.Context,
	_ ai.IntentPlanRequestV2,
) (ai.IntentPlanV2, error) {
	p.calls++
	if p.calls == 1 {
		return ai.IntentPlanV2{}, errors.New("provider transport unavailable")
	}
	close(p.started)
	<-ctx.Done()
	return ai.IntentPlanV2{}, ctx.Err()
}

func TestIntentCandidateEnginePersistsNonContiguousCandidateAcrossWindows(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	db := openIntentCandidateTestDB(t)
	firstA := appendIntentCandidateCapture(t, db, "internal/a/a.go", "create", "", "a1")
	b := appendIntentCandidateCapture(t, db, "internal/b/b.go", "create", "", "b1")
	secondA := appendIntentCandidateCapture(t, db, "internal/a/a.go", "modify", "a1", "a2")

	planner := &intentCandidatePlannerStub{plan: ai.IntentPlanV2{
		ProtocolVersion: ai.IntentPlannerProtocolV2,
		Candidates: []ai.IntentCandidateAssignment{
			{
				CandidateID: "candidate-a", SelectedSeqs: []int64{firstA.Event.Seq, secondA.Event.Seq},
				Purpose: "implement a", Readiness: ai.IntentCandidateReady,
				Subject: "Implement a", GroupingReason: "same-path object chain",
			},
			{
				CandidateID: "candidate-b", SelectedSeqs: []int64{b.Event.Seq},
				Purpose: "implement b", Readiness: ai.IntentCandidateReady,
				Subject: "Implement b", GroupingReason: "independent component",
			},
		},
	}}
	passMaterialization := func(context.Context, []IntentCandidateCapture) error { return nil }
	passVerification := func(
		context.Context,
		ai.IntentCandidateAssignment,
		[]IntentCandidateCapture,
	) (IntentCandidateVerification, error) {
		return IntentCandidateVerification{Status: "passed"}, nil
	}
	first, err := EvaluateIntentCandidates(ctx, db, IntentCandidateEvaluation{
		BranchRef: "refs/heads/main", BranchGeneration: 1,
		Captures: []IntentCandidateCapture{firstA, b, secondA},
		Planner:  planner, Preset: config.PresetBalanced,
		Materialize: passMaterialization, Verify: passVerification,
		Now: time.Unix(100, 0),
	})
	if err != nil {
		t.Fatalf("EvaluateIntentCandidates first window: %v", err)
	}
	if len(first.Decisions) != 2 || !first.Decisions[0].Publishable ||
		!first.Decisions[1].Publishable {
		t.Fatalf("first decisions=%+v", first.Decisions)
	}
	var hardAChain bool
	for _, edge := range first.Dependencies {
		if edge.PrerequisiteSeq == firstA.Event.Seq &&
			edge.DependentSeq == secondA.Event.Seq &&
			edge.Strength == state.IntentDependencyHard {
			hardAChain = true
		}
		if edge.Strength == state.IntentDependencyHard &&
			(edge.PrerequisiteSeq == b.Event.Seq || edge.DependentSeq == b.Event.Seq) {
			t.Fatalf("independent B received a hard edge: %+v", edge)
		}
	}
	if !hardAChain {
		t.Fatalf("hard A1->A2 edge missing: %+v", first.Dependencies)
	}
	if got := first.Decisions[0].Assignment.SelectedSeqs; fmt.Sprint(got) != fmt.Sprint([]int64{firstA.Event.Seq, secondA.Event.Seq}) {
		t.Fatalf("non-contiguous A selection=%v", got)
	}

	testA := appendIntentCandidateCapture(t, db, "internal/a/a_test.go", "create", "", "at1")
	candidateAID := first.Decisions[0].Candidate.ID
	planner.plan = ai.IntentPlanV2{
		ProtocolVersion: ai.IntentPlannerProtocolV2,
		Candidates: []ai.IntentCandidateAssignment{{
			CandidateID: candidateAID, SelectedSeqs: []int64{testA.Event.Seq},
			Purpose: "implement and test a", Readiness: ai.IntentCandidateReady,
			Subject: "Implement and test a", GroupingReason: "matching source and test",
		}},
	}
	second, err := EvaluateIntentCandidates(ctx, db, IntentCandidateEvaluation{
		BranchRef: "refs/heads/main", BranchGeneration: 1,
		Captures: []IntentCandidateCapture{testA}, Planner: planner,
		Preset: config.PresetBalanced, Materialize: passMaterialization,
		Verify: passVerification, Now: time.Unix(110, 0),
	})
	if err != nil {
		t.Fatalf("EvaluateIntentCandidates second window: %v", err)
	}
	if len(planner.req.Candidates) != 2 {
		t.Fatalf("persisted candidate summaries=%d want=2", len(planner.req.Candidates))
	}
	if len(second.Decisions) != 1 ||
		second.Decisions[0].Candidate.ID != candidateAID {
		t.Fatalf("second decisions=%+v", second.Decisions)
	}
	got, ok, err := state.IntentCandidateByID(ctx, db, candidateAID)
	if err != nil || !ok {
		t.Fatalf("IntentCandidateByID: ok=%v err=%v", ok, err)
	}
	if len(got.Events) != 3 {
		t.Fatalf("candidate events=%+v want A1,A2,test", got.Events)
	}
	if got.Events[0].EventSeq != firstA.Event.Seq ||
		got.Events[1].EventSeq != secondA.Event.Seq ||
		got.Events[2].EventSeq != testA.Event.Seq {
		t.Fatalf("candidate event order=%+v", got.Events)
	}
}

func TestIntentCandidateEngineBoundsFiftyThousandPendingEvents(t *testing.T) {
	ctx := context.Background()
	db := openIntentCandidateTestDB(t)
	const totalPending = 50_000
	tx, err := db.SQL().BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	stmt, err := tx.PrepareContext(ctx, `
INSERT INTO capture_events(
  branch_ref, branch_generation, base_head, operation, path,
  fidelity, captured_ts, state
) VALUES('refs/heads/main', 1, 'base', 'create', ?, 'full', ?, 'pending')`)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < totalPending; i++ {
		if _, err := stmt.ExecContext(ctx,
			fmt.Sprintf("bulk/%05d.go", i), float64(i+1)); err != nil {
			_ = stmt.Close()
			_ = tx.Rollback()
			t.Fatalf("seed pending event %d: %v", i, err)
		}
	}
	if err := stmt.Close(); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	summary, err := state.CountAllPendingCaptureEvents(ctx, db)
	if err != nil {
		t.Fatal(err)
	}
	if summary.Count != totalPending {
		t.Fatalf("pending count=%d want=%d", summary.Count, totalPending)
	}
	const configuredWindow = 30
	visible, err := state.PendingEvents(ctx, db, configuredWindow)
	if err != nil {
		t.Fatal(err)
	}
	if len(visible) != configuredWindow {
		t.Fatalf("visible events=%d want configured window=%d",
			len(visible), configuredWindow)
	}
	captures := make([]IntentCandidateCapture, 0, len(visible))
	for _, event := range visible {
		captures = append(captures, IntentCandidateCapture{Event: event})
	}
	liveRoot := t.TempDir()
	livePath := filepath.Join(liveRoot, "live.txt")
	if err := os.WriteFile(livePath, []byte("unchanged\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	result, err := EvaluateIntentCandidates(ctx, db,
		IntentCandidateEvaluation{
			BranchRef: "refs/heads/main", BranchGeneration: 1,
			Captures: captures, RetryLimit: 0, RetryLimitSet: true,
			Preset: config.PresetFast,
			Materialize: func(
				context.Context,
				[]IntentCandidateCapture,
			) error {
				return nil
			},
		})
	if err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(started); elapsed > 10*time.Second {
		t.Fatalf("bounded candidate evaluation took %s", elapsed)
	}
	if len(result.Decisions) != configuredWindow ||
		len(result.Dependencies) > state.IntentDependencyMaxPerPair {
		t.Fatalf("bounded evaluation decisions=%d dependencies=%d",
			len(result.Decisions), len(result.Dependencies))
	}
	ready := 0
	for _, decision := range result.Decisions {
		if decision.Publishable {
			ready++
		}
	}
	if ready != 1 {
		t.Fatalf("Fast fallback publishable candidates=%d want=1", ready)
	}
	after, err := state.CountAllPendingCaptureEvents(ctx, db)
	if err != nil {
		t.Fatal(err)
	}
	if after.Count != totalPending {
		t.Fatalf("candidate evaluation changed capture queue: before=%d after=%d",
			totalPending, after.Count)
	}
	if body, err := os.ReadFile(livePath); err != nil ||
		string(body) != "unchanged\n" {
		t.Fatalf("candidate evaluation changed live file: body=%q err=%v",
			body, err)
	}
}

func TestIntentCandidateEngineHonorsCorrectionRetryBudget(t *testing.T) {
	for _, retryLimit := range []int{0, 2} {
		t.Run(fmt.Sprintf("retry_limit_%d", retryLimit), func(t *testing.T) {
			ctx := context.Background()
			db := openIntentCandidateTestDB(t)
			a := appendIntentCandidateCapture(t, db,
				"internal/a/a.go", "create", "", "a1")
			b := appendIntentCandidateCapture(t, db,
				"internal/b/b.go", "create", "", "b1")
			planner := &invalidIntentCandidatePlannerStub{}
			result, err := EvaluateIntentCandidates(ctx, db,
				IntentCandidateEvaluation{
					BranchRef: "refs/heads/main", BranchGeneration: 1,
					Captures: []IntentCandidateCapture{a, b}, Planner: planner,
					RetryLimit: retryLimit, RetryLimitSet: true,
					Preset: config.PresetFast,
					Materialize: func(
						context.Context,
						[]IntentCandidateCapture,
					) error {
						return nil
					},
				})
			if err != nil {
				t.Fatal(err)
			}
			if planner.calls != 1+retryLimit {
				t.Fatalf("planner calls=%d want=%d",
					planner.calls, 1+retryLimit)
			}
			if result.Fallback != "hard_dependency_component" ||
				result.PlannerFailure == "" {
				t.Fatalf("fallback result=%+v", result)
			}
		})
	}
}

func TestIntentCandidateEnginePersistsCoalescedMembership(t *testing.T) {
	ctx := context.Background()
	db := openIntentCandidateTestDB(t)
	first := appendIntentCandidateCapture(t, db,
		"same.go", "create", "", "first")
	second := appendIntentCandidateCapture(t, db,
		"same.go", "modify", "first", "second")
	planner := intentCandidatePlannerStub{plan: ai.IntentPlanV2{
		ProtocolVersion: ai.IntentPlannerProtocolV2,
		Candidates: []ai.IntentCandidateAssignment{{
			CandidateID: "candidate-1", SelectedSeqs: []int64{first.Event.Seq},
			Purpose: "complete one same-path edit", Readiness: ai.IntentCandidateReady,
			Subject:        "Update same-path edit",
			GroupingReason: "coalesced captures have one final tree",
		}},
	}}
	first.CoveredEvents = []state.CaptureEvent{second.Event}
	result, err := EvaluateIntentCandidates(ctx, db, IntentCandidateEvaluation{
		BranchRef: "refs/heads/main", BranchGeneration: 1,
		Captures: []IntentCandidateCapture{first}, Planner: &planner,
		RetryLimit: 0, RetryLimitSet: true, Preset: config.PresetFast,
		Materialize: func(context.Context, []IntentCandidateCapture) error {
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Decisions) != 1 {
		t.Fatalf("decisions=%d want=1", len(result.Decisions))
	}
	events := result.Decisions[0].Candidate.Events
	if len(events) != 2 ||
		events[0].EventSeq != first.Event.Seq ||
		events[1].EventSeq != second.Event.Seq ||
		events[1].EventRole != "coalesced" {
		t.Fatalf("candidate events=%+v", events)
	}
	existing, err := state.IntentCandidatesForPair(
		ctx, db, "refs/heads/main", 1, state.IntentCandidateMaxOpenPerPair)
	if err != nil {
		t.Fatal(err)
	}
	reloaded, err := loadCandidateCaptureContext(ctx, db,
		IntentCandidateEvaluation{
			BranchRef: "refs/heads/main", BranchGeneration: 1,
		}, existing)
	if err != nil {
		t.Fatal(err)
	}
	if len(reloaded) != 1 || len(reloaded[0].CoveredEvents) != 1 ||
		len(reloaded[0].Ops) != 1 ||
		reloaded[0].Ops[0].AfterOID != second.Ops[0].AfterOID {
		t.Fatalf("reloaded coalesced capture=%+v", reloaded)
	}
}

func TestStabilizeIntentCandidatePlanNamespacesRepeatedProviderIDs(t *testing.T) {
	plan := func(seq int64) ai.IntentPlanV2 {
		return ai.IntentPlanV2{
			ProtocolVersion: ai.IntentPlannerProtocolV2,
			Candidates: []ai.IntentCandidateAssignment{{
				CandidateID: "candidate-1", SelectedSeqs: []int64{seq},
				Purpose: "one change", Readiness: ai.IntentCandidateReady,
				Subject: "Update one change", GroupingReason: "one capture",
			}},
		}
	}
	first, err := stabilizeIntentCandidatePlan(
		plan(1), nil, "refs/heads/main", 7)
	if err != nil {
		t.Fatal(err)
	}
	second, err := stabilizeIntentCandidatePlan(
		plan(2), nil, "refs/heads/main", 7)
	if err != nil {
		t.Fatal(err)
	}
	firstID := first.Candidates[0].CandidateID
	secondID := second.Candidates[0].CandidateID
	if firstID == "candidate-1" || secondID == "candidate-1" ||
		firstID == secondID {
		t.Fatalf("stabilized ids first=%q second=%q", firstID, secondID)
	}
	continued, err := stabilizeIntentCandidatePlan(plan(1), []state.IntentCandidate{{
		ID: firstID, BranchRef: "refs/heads/main", BranchGeneration: 7,
		Events: []state.IntentCandidateEvent{{EventSeq: 1, EventRole: "code"}},
	}}, "refs/heads/main", 7)
	if err != nil {
		t.Fatal(err)
	}
	if continued.Candidates[0].CandidateID != firstID {
		t.Fatalf("continued id=%q want=%q",
			continued.Candidates[0].CandidateID, firstID)
	}
	if got, want := stableIntentCandidateID(
		"refs/heads/main", 7, []int64{3, 1, 2}),
		stableIntentCandidateID(
			"refs/heads/main", 7, []int64{1, 2, 3}); got != want {
		t.Fatalf("order-sensitive candidate ids got=%q want=%q", got, want)
	}
}

func TestIntentCandidateEngineReportsCircuitBypassWithoutReopening(t *testing.T) {
	ctx := context.Background()
	db := openIntentCandidateTestDB(t)
	capture := appendIntentCandidateCapture(t, db,
		"internal/a/a.go", "create", "", "a1")
	planner := &failingIntentCandidatePlannerStub{}
	health := NewIntentPlannerHealth(ctx, db, IntentPlannerHealthOptions{
		Provider: IntentPlannerProviderIdentity{Provider: planner.Name()},
	})
	input := IntentCandidateEvaluation{
		BranchRef: "refs/heads/main", BranchGeneration: 1,
		Captures: []IntentCandidateCapture{capture}, Planner: planner,
		Health: health, RetryLimit: 0, RetryLimitSet: true,
		Preset: config.PresetFast,
		Materialize: func(
			context.Context,
			[]IntentCandidateCapture,
		) error {
			return nil
		},
	}
	first, err := EvaluateIntentCandidates(ctx, db, input)
	if err != nil {
		t.Fatal(err)
	}
	if first.PlannerFailure == "" || planner.calls != 1 {
		t.Fatalf("first evaluation=%+v calls=%d", first, planner.calls)
	}
	second, err := EvaluateIntentCandidates(ctx, db, input)
	if err != nil {
		t.Fatal(err)
	}
	if second.PlannerFailure != "" ||
		second.Fallback != "hard_dependency_component" ||
		planner.calls != 1 {
		t.Fatalf("circuit bypass=%+v calls=%d", second, planner.calls)
	}
	snapshot := health.Snapshot()
	if snapshot.State != IntentPlannerCircuitOpen ||
		snapshot.BypassCount != 1 {
		t.Fatalf("health after bypass=%+v", snapshot)
	}
}

func TestIntentCandidateEngineCancellationReleasesHalfOpenProbe(t *testing.T) {
	ctx := context.Background()
	db := openIntentCandidateTestDB(t)
	capture := appendIntentCandidateCapture(t, db,
		"internal/a/a.go", "create", "", "a1")
	planner := &halfOpenCancelIntentCandidatePlannerStub{
		started: make(chan struct{}),
	}
	now := time.Date(2026, 7, 26, 12, 0, 0, 0, time.UTC)
	health := NewIntentPlannerHealth(ctx, db, IntentPlannerHealthOptions{
		Provider: IntentPlannerProviderIdentity{Provider: planner.Name()},
		Now:      func() time.Time { return now },
	})
	input := IntentCandidateEvaluation{
		BranchRef: "refs/heads/main", BranchGeneration: 1,
		Captures: []IntentCandidateCapture{capture}, Planner: planner,
		Health: health, RetryLimit: 0, RetryLimitSet: true,
		Preset: config.PresetFast,
		Materialize: func(
			context.Context,
			[]IntentCandidateCapture,
		) error {
			return nil
		},
	}
	if _, err := EvaluateIntentCandidates(ctx, db, input); err != nil {
		t.Fatal(err)
	}
	opened := health.Snapshot()
	if opened.State != IntentPlannerCircuitOpen {
		t.Fatalf("health after transport failure=%+v", opened)
	}
	now = now.Add(31 * time.Second)
	probeCtx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := EvaluateIntentCandidates(probeCtx, db, input)
		done <- err
	}()
	<-planner.started
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("half-open evaluation error=%v", err)
	}
	after := health.Snapshot()
	if after.State != IntentPlannerCircuitOpen ||
		after.ConsecutiveFailures != opened.ConsecutiveFailures {
		t.Fatalf("health after caller cancellation=%+v opened=%+v",
			after, opened)
	}
}

func TestIntentCandidateEngineRetriesDisconnectedGroupingWithCorrection(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	db := openIntentCandidateTestDB(t)
	a := appendIntentCandidateCapture(t, db, "internal/a/a.go", "create", "", "a1")
	b := appendIntentCandidateCapture(t, db, "internal/b/b.go", "create", "", "b1")
	planner := &correctingIntentCandidatePlannerStub{}
	result, err := EvaluateIntentCandidates(ctx, db, IntentCandidateEvaluation{
		BranchRef: "refs/heads/main", BranchGeneration: 1,
		Captures: []IntentCandidateCapture{a, b}, Planner: planner,
		Preset:      config.PresetBalanced,
		Materialize: func(context.Context, []IntentCandidateCapture) error { return nil },
		Verify: func(
			context.Context,
			ai.IntentCandidateAssignment,
			[]IntentCandidateCapture,
		) (IntentCandidateVerification, error) {
			return IntentCandidateVerification{Status: "passed"}, nil
		},
	})
	if err != nil {
		t.Fatalf("EvaluateIntentCandidates: %v", err)
	}
	if planner.calls != 2 || len(planner.reqs) != 2 ||
		planner.reqs[1].RetryCorrection == "" {
		t.Fatalf("correction calls=%d requests=%+v", planner.calls, planner.reqs)
	}
	if result.Fallback != "" || len(result.Decisions) != 2 {
		t.Fatalf("corrected result=%+v", result)
	}
}

func TestIntentCandidateEngineCorrectsDisconnectedMegaGroupWithBalancedFallback(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	db := openIntentCandidateTestDB(t)
	a := appendIntentCandidateCapture(t, db, "internal/a/a.go", "create", "", "a1")
	b := appendIntentCandidateCapture(t, db, "internal/b/b.go", "create", "", "b1")
	planner := &intentCandidatePlannerStub{plan: ai.IntentPlanV2{
		ProtocolVersion: ai.IntentPlannerProtocolV2,
		Candidates: []ai.IntentCandidateAssignment{{
			CandidateID: "mega", SelectedSeqs: []int64{a.Event.Seq, b.Event.Seq},
			Purpose: "mix unrelated work", Readiness: ai.IntentCandidateReady,
			Subject: "Update unrelated work", GroupingReason: "same evaluation window",
		}},
	}}
	verifyCalls := 0
	result, err := EvaluateIntentCandidates(ctx, db, IntentCandidateEvaluation{
		BranchRef: "refs/heads/main", BranchGeneration: 1,
		Captures: []IntentCandidateCapture{a, b}, Planner: planner,
		Preset:      config.PresetBalanced,
		Materialize: func(context.Context, []IntentCandidateCapture) error { return nil },
		Verify: func(
			context.Context,
			ai.IntentCandidateAssignment,
			[]IntentCandidateCapture,
		) (IntentCandidateVerification, error) {
			verifyCalls++
			return IntentCandidateVerification{Status: "passed"}, nil
		},
	})
	if err != nil {
		t.Fatalf("EvaluateIntentCandidates: %v", err)
	}
	if result.Fallback != "verified_dependency_partition" {
		t.Fatalf("fallback=%q", result.Fallback)
	}
	if len(result.Decisions) != 2 || verifyCalls != 2 {
		t.Fatalf("decisions=%d verifyCalls=%d", len(result.Decisions), verifyCalls)
	}
	for _, decision := range result.Decisions {
		if len(decision.Assignment.SelectedSeqs) != 1 || !decision.Publishable {
			t.Fatalf("fallback decision=%+v", decision)
		}
	}
}

func TestIntentCandidateEngineRejectsSameDirectoryMegaGroup(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	db := openIntentCandidateTestDB(t)
	first := appendIntentCandidateCapture(t, db, "internal/api/alpha.go", "create", "", "a1")
	second := appendIntentCandidateCapture(t, db, "internal/api/beta.go", "create", "", "b1")
	planner := &intentCandidatePlannerStub{plan: ai.IntentPlanV2{
		ProtocolVersion: ai.IntentPlannerProtocolV2,
		Candidates: []ai.IntentCandidateAssignment{{
			CandidateID:  "same-directory-mega",
			SelectedSeqs: []int64{first.Event.Seq, second.Event.Seq},
			Purpose:      "mix unrelated same-directory work",
			Readiness:    ai.IntentCandidateReady, Subject: "Update API work",
			GroupingReason: "files share a module directory",
		}},
	}}
	result, err := EvaluateIntentCandidates(ctx, db, IntentCandidateEvaluation{
		BranchRef: "refs/heads/main", BranchGeneration: 1,
		Captures: []IntentCandidateCapture{first, second},
		Planner:  planner, Preset: config.PresetFast,
		Materialize: func(context.Context, []IntentCandidateCapture) error {
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Fallback != "hard_dependency_component" ||
		len(result.Decisions) != 2 ||
		!result.Decisions[0].Publishable ||
		result.Decisions[1].Publishable {
		t.Fatalf("same-directory mega-group was not split safely: %+v", result)
	}
	for _, decision := range result.Decisions {
		if len(decision.Assignment.SelectedSeqs) != 1 {
			t.Fatalf("fallback component=%+v", decision)
		}
	}
}

func TestIntentCandidateEngineAdvancesFastFallbackComponents(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	db := openIntentCandidateTestDB(t)
	first := appendIntentCandidateCapture(t, db, "first.txt", "create", "", "a1")
	second := appendIntentCandidateCapture(t, db, "second.txt", "create", "", "b1")
	planner := &intentCandidatePlannerStub{plan: ai.IntentPlanV2{
		ProtocolVersion: ai.IntentPlannerProtocolV2,
		Candidates: []ai.IntentCandidateAssignment{{
			CandidateID:  "disconnected",
			SelectedSeqs: []int64{first.Event.Seq, second.Event.Seq},
			Purpose:      "mix independent captures", Readiness: ai.IntentCandidateReady,
			Subject: "Mix captures", GroupingReason: "same window",
		}},
	}}
	evaluate := func(captures []IntentCandidateCapture) IntentCandidateEvaluationResult {
		result, err := EvaluateIntentCandidates(ctx, db, IntentCandidateEvaluation{
			BranchRef: "refs/heads/main", BranchGeneration: 1,
			Captures: captures, Planner: planner, Preset: config.PresetFast,
			Materialize: func(context.Context, []IntentCandidateCapture) error {
				return nil
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		return result
	}
	initial := evaluate([]IntentCandidateCapture{first, second})
	if len(initial.Decisions) != 2 ||
		!initial.Decisions[0].Publishable ||
		initial.Decisions[1].Publishable {
		t.Fatalf("initial fallback=%+v", initial)
	}
	firstID := initial.Decisions[0].Candidate.ID
	if _, err := db.SQL().ExecContext(ctx, `
UPDATE intent_candidates
SET status='soft_published', published_commit_oid='first-commit',
    soft_publication_deadline=?
WHERE id=?`,
		float64(time.Now().Add(time.Minute).UnixNano())/1e9,
		firstID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.SQL().ExecContext(ctx, `
UPDATE capture_events
SET state='published', commit_oid='first-commit'
WHERE seq=?`, first.Event.Seq); err != nil {
		t.Fatal(err)
	}
	next := evaluate([]IntentCandidateCapture{second})
	if len(next.Decisions) != 1 || !next.Decisions[0].Publishable ||
		len(next.Decisions[0].Assignment.SelectedSeqs) != 1 ||
		next.Decisions[0].Assignment.SelectedSeqs[0] != second.Event.Seq {
		t.Fatalf("next fallback=%+v", next)
	}
}

func TestIntentCandidateEnginePresetProviderFailurePolicies(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name           string
		preset         config.PresetName
		wantFallback   string
		wantReady      int
		wantAttention  bool
		wantVerifyCall int
	}{
		{
			name: "fast", preset: config.PresetFast,
			wantFallback: "hard_dependency_component", wantReady: 1,
		},
		{
			name: "balanced", preset: config.PresetBalanced,
			wantFallback: "verified_dependency_partition", wantReady: 2,
			wantVerifyCall: 2,
		},
		{
			name: "quality", preset: config.PresetQuality,
			wantFallback: "needs_attention", wantAttention: true,
		},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			db := openIntentCandidateTestDB(t)
			a := appendIntentCandidateCapture(t, db, "internal/a/a.go", "create", "", "a1")
			b := appendIntentCandidateCapture(t, db, "internal/b/b.go", "create", "", "b1")
			verifyCalls := 0
			result, err := EvaluateIntentCandidates(ctx, db, IntentCandidateEvaluation{
				BranchRef: "refs/heads/main", BranchGeneration: 1,
				Captures:    []IntentCandidateCapture{a, b},
				Planner:     &intentCandidatePlannerStub{err: errors.New("provider unavailable")},
				Preset:      tc.preset,
				Materialize: func(context.Context, []IntentCandidateCapture) error { return nil },
				Verify: func(
					context.Context,
					ai.IntentCandidateAssignment,
					[]IntentCandidateCapture,
				) (IntentCandidateVerification, error) {
					verifyCalls++
					return IntentCandidateVerification{Status: "passed"}, nil
				},
			})
			if err != nil {
				t.Fatalf("EvaluateIntentCandidates: %v", err)
			}
			ready := 0
			for _, decision := range result.Decisions {
				if decision.Publishable {
					ready++
				}
			}
			if result.Fallback != tc.wantFallback || ready != tc.wantReady ||
				result.NeedsAttention != tc.wantAttention || verifyCalls != tc.wantVerifyCall {
				t.Fatalf("result fallback=%q ready=%d attention=%v verify=%d",
					result.Fallback, ready, result.NeedsAttention, verifyCalls)
			}
		})
	}
}

func TestIntentCandidateEngineVerificationFailureStaysPending(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	db := openIntentCandidateTestDB(t)
	capture := appendIntentCandidateCapture(t, db, "internal/a/a.go", "create", "", "a1")
	planner := &intentCandidatePlannerStub{plan: ai.IntentPlanV2{
		ProtocolVersion: ai.IntentPlannerProtocolV2,
		Candidates: []ai.IntentCandidateAssignment{{
			CandidateID: "candidate-a", SelectedSeqs: []int64{capture.Event.Seq},
			Purpose: "implement a", Readiness: ai.IntentCandidateReady,
			Subject: "Implement a", GroupingReason: "single capture",
		}},
	}}
	result, err := EvaluateIntentCandidates(ctx, db, IntentCandidateEvaluation{
		BranchRef: "refs/heads/main", BranchGeneration: 1,
		Captures: []IntentCandidateCapture{capture}, Planner: planner,
		Preset:      config.PresetBalanced,
		Materialize: func(context.Context, []IntentCandidateCapture) error { return nil },
		Verify: func(
			context.Context,
			ai.IntentCandidateAssignment,
			[]IntentCandidateCapture,
		) (IntentCandidateVerification, error) {
			return IntentCandidateVerification{
				Status: "failed", Output: "bounded output", CheckedTS: 123,
			}, errors.New("fast check failed")
		},
	})
	if err != nil {
		t.Fatalf("EvaluateIntentCandidates: %v", err)
	}
	if len(result.Decisions) != 1 || result.Decisions[0].Publishable ||
		result.Decisions[0].Candidate.Status != state.IntentCandidateWaiting ||
		!result.NeedsAttention {
		t.Fatalf("verification failure result=%+v", result)
	}
	candidate := result.Decisions[0].Candidate
	if candidate.VerificationStatus.String != "failed" ||
		candidate.VerificationOutput != "bounded output" ||
		!candidate.VerificationTS.Valid ||
		candidate.VerificationTS.Float64 != 123 {
		t.Fatalf("verification evidence=%+v", candidate)
	}
}

func TestIntentCandidateEngineDoesNotVerifyRejectedPredecessor(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	db := openIntentCandidateTestDB(t)
	bad := appendIntentCandidateCapture(
		t, db, "bad.go", "create", "", "bad")
	good := appendIntentCandidateCapture(
		t, db, "good.go", "create", "", "good")
	planner := &intentCandidatePlannerStub{plan: ai.IntentPlanV2{
		ProtocolVersion: ai.IntentPlannerProtocolV2,
		Candidates: []ai.IntentCandidateAssignment{
			{
				CandidateID:  "candidate-bad",
				SelectedSeqs: []int64{bad.Event.Seq},
				Purpose:      "bad candidate", Readiness: ai.IntentCandidateReady,
				Subject: "Add bad candidate", GroupingReason: "one component",
			},
			{
				CandidateID:  "candidate-good",
				SelectedSeqs: []int64{good.Event.Seq},
				Purpose:      "good candidate", Readiness: ai.IntentCandidateReady,
				Subject: "Add good candidate", GroupingReason: "one component",
			},
		},
	}}
	var verified []string
	result, err := EvaluateIntentCandidates(ctx, db, IntentCandidateEvaluation{
		BranchRef: "refs/heads/main", BranchGeneration: 1,
		Captures: []IntentCandidateCapture{bad, good}, Planner: planner,
		Preset: config.PresetBalanced,
		Materialize: func(
			_ context.Context,
			captures []IntentCandidateCapture,
		) error {
			if captures[0].Event.Path == "bad.go" {
				return errors.New("scratch materialization failed")
			}
			return nil
		},
		Verify: func(
			_ context.Context,
			assignment ai.IntentCandidateAssignment,
			_ []IntentCandidateCapture,
		) (IntentCandidateVerification, error) {
			verified = append(verified, assignment.CandidateID)
			return IntentCandidateVerification{Status: "passed"}, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Decisions) != 2 ||
		result.Decisions[0].Publishable ||
		!result.Decisions[1].Publishable {
		t.Fatalf("decisions=%+v", result.Decisions)
	}
	if !reflect.DeepEqual(verified,
		[]string{result.Decisions[1].Assignment.CandidateID}) {
		t.Fatalf("verified rejected predecessor: %v", verified)
	}
}

func TestIntentCandidateEngineSameFileSplitRequiresTopologicalMaterialization(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	db := openIntentCandidateTestDB(t)
	first := appendIntentCandidateCapture(t, db, "same.go", "create", "", "a1")
	second := appendIntentCandidateCapture(t, db, "same.go", "modify", "a1", "a2")
	planner := &intentCandidatePlannerStub{plan: ai.IntentPlanV2{
		ProtocolVersion: ai.IntentPlannerProtocolV2,
		Candidates: []ai.IntentCandidateAssignment{
			{
				CandidateID: "first", SelectedSeqs: []int64{first.Event.Seq},
				Purpose: "first same-file unit", Readiness: ai.IntentCandidateReady,
				Subject: "Add first unit", GroupingReason: "independent scratch selection",
			},
			{
				CandidateID: "second", SelectedSeqs: []int64{second.Event.Seq},
				Purpose: "second same-file unit", Readiness: ai.IntentCandidateReady,
				DependsOnCandidates: []string{"first"},
				Subject:             "Add second unit", GroupingReason: "topological same-file selection",
			},
		},
	}}
	var materialized [][]int64
	result, err := EvaluateIntentCandidates(ctx, db, IntentCandidateEvaluation{
		BranchRef: "refs/heads/main", BranchGeneration: 1,
		Captures: []IntentCandidateCapture{first, second}, Planner: planner,
		Preset: config.PresetFast,
		Materialize: func(_ context.Context, captures []IntentCandidateCapture) error {
			var seqs []int64
			for _, capture := range captures {
				seqs = append(seqs, capture.Event.Seq)
			}
			materialized = append(materialized, seqs)
			return nil
		},
	})
	if err != nil {
		t.Fatalf("EvaluateIntentCandidates: %v", err)
	}
	if len(result.Decisions) != 2 || len(materialized) != 2 ||
		!result.Decisions[0].Publishable || !result.Decisions[1].Publishable {
		t.Fatalf("result=%+v materialized=%v", result, materialized)
	}
}

func TestIntentCandidateEngineReportsV1CompatibilityAndConsumesBoundary(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	db := openIntentCandidateTestDB(t)
	capture := appendIntentCandidateCapture(t, db, "a.go", "create", "", "a1")
	boundary, err := state.AppendIntentActivityBoundary(ctx, db, state.IntentActivityBoundary{
		Kind: state.IntentBoundarySoft, Source: "test",
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := EvaluateIntentCandidates(ctx, db, IntentCandidateEvaluation{
		BranchRef: "refs/heads/main", BranchGeneration: 1,
		Captures: []IntentCandidateCapture{capture},
		Planner:  intentCandidateV1PlannerStub{}, Preset: config.PresetFast,
		Materialize: func(context.Context, []IntentCandidateCapture) error { return nil },
	})
	if err != nil {
		t.Fatalf("EvaluateIntentCandidates: %v", err)
	}
	if result.ProtocolVersion != ai.IntentPlannerProtocolV1Compat ||
		len(result.Boundaries) != 1 || result.Boundaries[0].Epoch != boundary.Epoch {
		t.Fatalf("result=%+v", result)
	}
	got, ok, err := state.IntentCandidateByID(ctx, db, result.Decisions[0].Candidate.ID)
	if err != nil || !ok || got.PlannerProtocol.String != ai.IntentPlannerProtocolV1Compat {
		t.Fatalf("persisted protocol candidate=%+v ok=%v err=%v", got, ok, err)
	}
	pending, err := state.PendingIntentActivityBoundaries(ctx, db, 0, 10)
	if err != nil || len(pending) != 0 {
		t.Fatalf("pending boundaries=%+v err=%v", pending, err)
	}
}

func TestIntentCandidateBoundariesSkipStaleBranchEpochs(t *testing.T) {
	t.Parallel()
	boundaries := []state.IntentActivityBoundary{
		{
			Epoch: 1,
			BranchRef: sql.NullString{
				String: "refs/heads/old", Valid: true,
			},
			BranchGeneration: sql.NullInt64{Int64: 1, Valid: true},
		},
		{Epoch: 2, Kind: state.IntentBoundarySoft, Source: "global"},
		{
			Epoch: 3, Kind: state.IntentBoundarySoft, Source: "active",
			BranchRef: sql.NullString{
				String: "refs/heads/main", Valid: true,
			},
			BranchGeneration: sql.NullInt64{Int64: 2, Valid: true},
		},
	}
	got, through := consumableBoundariesForPair(
		boundaries, "refs/heads/main", 2)
	if through != 3 || len(got) != 2 ||
		got[0].Epoch != 2 || got[1].Epoch != 3 {
		t.Fatalf("boundaries=%+v through=%d", got, through)
	}
}

func TestBuildIntentCandidateDependenciesHashesEvidenceAndEnforcesCap(t *testing.T) {
	t.Parallel()
	first := intentCandidateCaptureFixture(1, "a.go", "create", "", "a1")
	second := intentCandidateCaptureFixture(2, "b.go", "create", "", "b1")
	edges, err := BuildIntentCandidateDependencies(
		"refs/heads/main", 1, []IntentCandidateCapture{first, second},
		[]IntentDependencyHint{{
			PrerequisiteSeq: 1, DependentSeq: 2,
			Strength: ai.IntentDependencySoft, Kind: "import_reference",
			Evidence: "private symbol name",
		}}, time.Unix(100, 0),
	)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, edge := range edges {
		if edge.Kind != "import_reference" {
			continue
		}
		found = true
		if edge.Evidence == "private symbol name" ||
			!strings.HasPrefix(edge.Evidence, "sha256:") {
			t.Fatalf("evidence was not privacy hashed: %q", edge.Evidence)
		}
	}
	if !found {
		t.Fatal("import_reference edge missing")
	}

	hints := make([]IntentDependencyHint, state.IntentDependencyMaxPerPair+1)
	for i := range hints {
		hints[i] = IntentDependencyHint{
			PrerequisiteSeq: 1, DependentSeq: 2,
			Strength: ai.IntentDependencySoft,
			Kind:     fmt.Sprintf("symbol_%04d", i), Evidence: fmt.Sprint(i),
		}
	}
	_, err = BuildIntentCandidateDependencies(
		"refs/heads/main", 1, []IntentCandidateCapture{first, second},
		hints, time.Unix(100, 0),
	)
	if err == nil || !strings.Contains(err.Error(), "edge cap 4096") {
		t.Fatalf("cap error=%v", err)
	}
}

func TestBuildIntentCandidateDependenciesKeepsDocumentationIndependent(t *testing.T) {
	t.Parallel()
	documentation := intentCandidateCaptureFixture(
		1, "internal/a/guide.md", "create", "", "doc1")
	code := intentCandidateCaptureFixture(
		2, "internal/a/a.go", "create", "", "code1")
	edges, err := BuildIntentCandidateDependencies(
		"refs/heads/main", 1,
		[]IntentCandidateCapture{documentation, code}, nil, time.Unix(100, 0),
	)
	if err != nil {
		t.Fatal(err)
	}
	for _, edge := range edges {
		if edge.Kind != "temporal_proximity" {
			t.Fatalf("documentation and code gained cohesion evidence: %+v", edge)
		}
	}
}

func TestRuntimeIntentDependencyHintsUseSourceEvidence(t *testing.T) {
	t.Parallel()
	source := intentCandidateCaptureFixture(
		1, "internal/api/client.go", "create", "", "source")
	source.CapturedDiff = "+func ResolveTicketClient() {}\n"
	generated := intentCandidateCaptureFixture(
		2, "internal/api/client_generated.go", "create", "", "generated")
	generated.CapturedDiff = "// Code generated from client.go\n+ResolveTicketClient()\n"
	hints := runtimeIntentDependencyHints(
		[]IntentCandidateCapture{source, generated})
	kinds := make(map[string]ai.IntentDependencyStrength)
	for _, hint := range hints {
		kinds[hint.Kind] = hint.Strength
	}
	if kinds["symbol_hash"] != ai.IntentDependencySoft ||
		kinds["import_reference"] != ai.IntentDependencySoft ||
		kinds["generated_source"] != ai.IntentDependencyHard {
		t.Fatalf("runtime hints=%+v", hints)
	}
}

func openIntentCandidateTestDB(t *testing.T) *state.DB {
	t.Helper()
	db, err := state.Open(context.Background(), filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatalf("state.Open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func appendIntentCandidateCapture(
	t *testing.T,
	db *state.DB,
	capturePath string,
	op string,
	before string,
	after string,
) IntentCandidateCapture {
	t.Helper()
	fixture := intentCandidateCaptureFixture(0, capturePath, op, before, after)
	seq, err := state.AppendCaptureEvent(context.Background(), db, fixture.Event, fixture.Ops)
	if err != nil {
		t.Fatalf("AppendCaptureEvent: %v", err)
	}
	fixture.Event.Seq = seq
	for i := range fixture.Ops {
		fixture.Ops[i].EventSeq = seq
	}
	return fixture
}

func intentCandidateCaptureFixture(
	seq int64,
	capturePath string,
	op string,
	before string,
	after string,
) IntentCandidateCapture {
	event := state.CaptureEvent{
		Seq: seq, BranchRef: "refs/heads/main", BranchGeneration: 1,
		BaseHead: "base", Operation: op, Path: capturePath,
		Fidelity: "full", CapturedTS: float64(10 + seq),
		State: state.EventStatePending,
	}
	captureOp := state.CaptureOp{
		EventSeq: seq, Op: op, Path: capturePath, Fidelity: "full",
	}
	if before != "" {
		captureOp.BeforeOID = sql.NullString{String: before, Valid: true}
		captureOp.BeforeMode = sql.NullString{String: "100644", Valid: true}
	}
	if after != "" {
		captureOp.AfterOID = sql.NullString{String: after, Valid: true}
		captureOp.AfterMode = sql.NullString{String: "100644", Valid: true}
	}
	return IntentCandidateCapture{Event: event, Ops: []state.CaptureOp{captureOp}}
}
