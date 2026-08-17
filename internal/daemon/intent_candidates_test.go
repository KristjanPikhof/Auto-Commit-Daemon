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
	plan  ai.IntentPlanV2
	err   error
	req   ai.IntentPlanRequestV2
	calls int
}

func (p *intentCandidatePlannerStub) Name() string { return "intent-v2-test" }

func (p *intentCandidatePlannerStub) PlanIntentV2(
	_ context.Context,
	req ai.IntentPlanRequestV2,
) (ai.IntentPlanV2, error) {
	p.calls++
	p.req = req
	return p.plan, p.err
}

type correctingIntentCandidatePlannerStub struct {
	calls int
	reqs  []ai.IntentPlanRequestV2
}

type partialReplanIntentCandidatePlannerStub struct {
	calls int
	reqs  []ai.IntentPlanRequestV2
}

func (p *partialReplanIntentCandidatePlannerStub) Name() string { return "intent-v2-partial-test" }

func (p *partialReplanIntentCandidatePlannerStub) PlanIntentV2(
	_ context.Context,
	req ai.IntentPlanRequestV2,
) (ai.IntentPlanV2, error) {
	p.calls++
	p.reqs = append(p.reqs, req)
	if p.calls == 1 {
		return ai.IntentPlanV2{ProtocolVersion: ai.IntentPlannerProtocolV2,
			Candidates: []ai.IntentCandidateAssignment{
				{CandidateID: "locked-a", SelectedSeqs: []int64{req.OfferedCaptures[0].Seq},
					Purpose: "keep valid source change", Readiness: ai.IntentCandidateReady,
					Subject: "Update source change", GroupingReason: "complete source intent"},
				{CandidateID: "invalid-b", SelectedSeqs: []int64{req.OfferedCaptures[1].Seq},
					Purpose: "repair test change", Readiness: ai.IntentCandidateReady,
					Subject: "Update test change", GroupingReason: "invalid dependency metadata",
					DependsOnCandidates: []string{"invalid-b"}},
			}}, nil
	}
	capture := req.OfferedCaptures[0]
	return ai.IntentPlanV2{ProtocolVersion: ai.IntentPlannerProtocolV2,
		Candidates: []ai.IntentCandidateAssignment{{
			CandidateID: "replanned-b", SelectedSeqs: []int64{capture.Seq},
			Purpose: "complete test change", Readiness: ai.IntentCandidateReady,
			Subject: "Update test change", GroupingReason: "corrected unresolved capture",
		}}}, nil
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
			Candidates: []ai.IntentCandidateAssignment{
				{
					CandidateID: "candidate-a", SelectedSeqs: []int64{req.OfferedCaptures[0].Seq},
					Purpose: "separate independent change", Readiness: ai.IntentCandidateReady,
					GroupingReason: "capture has independent evidence",
				},
				{
					CandidateID: "candidate-b", SelectedSeqs: []int64{req.OfferedCaptures[1].Seq},
					Purpose: "separate independent change", Readiness: ai.IntentCandidateReady,
					Subject: "Separate independent change", GroupingReason: "capture has independent evidence",
				},
			},
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

type semanticIntentCandidatePlannerStub struct {
	calls int
}

func (p *semanticIntentCandidatePlannerStub) Name() string {
	return "intent-v2-semantic-test"
}

type selfDependentIntentCandidatePlannerStub struct {
	calls int
}

func (p *selfDependentIntentCandidatePlannerStub) Name() string {
	return "intent-v2-self-dependent-test"
}

func (p *selfDependentIntentCandidatePlannerStub) PlanIntentV2(
	_ context.Context,
	req ai.IntentPlanRequestV2,
) (ai.IntentPlanV2, error) {
	p.calls++
	return ai.IntentPlanV2{
		ProtocolVersion: ai.IntentPlannerProtocolV2,
		Candidates: []ai.IntentCandidateAssignment{{
			CandidateID:  "candidate-self",
			SelectedSeqs: []int64{req.OfferedCaptures[0].Seq},
			Purpose:      "invalid self-dependent candidate",
			Readiness:    ai.IntentCandidateReady,
			Subject:      "Update self-dependent candidate",
			GroupingReason: "exercise bounded structural correction " +
				"and deterministic fallback",
			DependsOnCandidates: []string{"candidate-self"},
		}},
	}, nil
}

func (p *semanticIntentCandidatePlannerStub) PlanIntentV2(
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
			Purpose: "implement shared request validation", Readiness: ai.IntentCandidateReady,
			Subject:        "Implement shared request validation",
			GroupingReason: "both captures implement the same validation behavior",
		}},
	}, nil
}

type failingIntentCandidatePlannerStub struct {
	calls int
}

func (p *failingIntentCandidatePlannerStub) Name() string {
	return "intent-v2-failing-test"
}

type modelWideFailingIntentCandidatePlannerStub struct {
	plannerCalls int
	rewriteCalls int
}

func (p *modelWideFailingIntentCandidatePlannerStub) Name() string {
	return "intent-v2-model-wide-failing-test"
}

func (p *modelWideFailingIntentCandidatePlannerStub) PlanIntentV2(
	context.Context,
	ai.IntentPlanRequestV2,
) (ai.IntentPlanV2, error) {
	p.plannerCalls++
	return ai.IntentPlanV2{}, errors.New("provider planning unavailable")
}

func (p *modelWideFailingIntentCandidatePlannerStub) RewriteIntentMessage(
	context.Context,
	ai.IntentMessageRewriteRequest,
) (ai.Result, error) {
	p.rewriteCalls++
	return ai.Result{}, errors.New("provider message rewrite unavailable")
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
	if ready != configuredWindow {
		t.Fatalf("Fast evidence fallback publishable candidates=%d want=%d",
			ready, configuredWindow)
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

func TestIntentCandidateEngineAcceptsSemanticGroupingWithoutGraphPath(t *testing.T) {
	for _, retryLimit := range []int{0, 2} {
		t.Run(fmt.Sprintf("retry_limit_%d", retryLimit), func(t *testing.T) {
			ctx := context.Background()
			db := openIntentCandidateTestDB(t)
			a := appendIntentCandidateCapture(t, db,
				"internal/a/a.go", "create", "", "a1")
			b := appendIntentCandidateCapture(t, db,
				"internal/b/b.go", "create", "", "b1")
			planner := &semanticIntentCandidatePlannerStub{}
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
			if planner.calls != 1 {
				t.Fatalf("structural planner calls=%d want=1",
					planner.calls)
			}
			if result.Fallback != "" || result.PlannerFailure != "" ||
				result.ResolutionMode != "provider" {
				t.Fatalf("semantic result=%+v", result)
			}
		})
	}
}

func TestIntentCandidateEngineBoundedFallbackAfterOneCorrection(t *testing.T) {
	ctx := context.Background()
	db := openIntentCandidateTestDB(t)
	capture := appendIntentCandidateCapture(
		t, db, "internal/a.go", "create", "", "a")
	planner := &selfDependentIntentCandidatePlannerStub{}
	result, err := EvaluateIntentCandidates(ctx, db, IntentCandidateEvaluation{
		BranchRef: "refs/heads/main", BranchGeneration: 1,
		Captures: []IntentCandidateCapture{capture}, Planner: planner,
		RetryLimit: 99, RetryLimitSet: true, Preset: config.PresetBalanced,
		VerificationMode: "structural",
		Materialize: func(context.Context, []IntentCandidateCapture) error {
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if planner.calls != 2 || result.RetryCount != 1 ||
		result.Fallback != "evidence_partition" ||
		result.PlannerFailure == "" || len(result.Decisions) != 1 ||
		!result.Decisions[0].Publishable {
		t.Fatalf("bounded fallback calls=%d result=%+v", planner.calls, result)
	}
	candidates, err := state.IntentCandidatesForPair(
		ctx, db, "refs/heads/main", 1, state.IntentCandidateMaxOpenPerPair)
	if err != nil {
		t.Fatal(err)
	}
	if len(candidates) != 1 || candidates[0].ID == "candidate-self" {
		t.Fatalf("durable candidates=%+v", candidates)
	}
}

func TestIntentCandidateSemanticReplanDoesNotPersistLocalFallback(t *testing.T) {
	ctx := context.Background()
	db := openIntentCandidateTestDB(t)
	capture := appendIntentCandidateCapture(
		t, db, "internal/a.go", "create", "", "a")
	planner := &selfDependentIntentCandidatePlannerStub{}
	result, err := EvaluateIntentCandidates(ctx, db, IntentCandidateEvaluation{
		BranchRef: "refs/heads/main", BranchGeneration: 1,
		Captures: []IntentCandidateCapture{capture}, Planner: planner,
		RetryLimit: 2, RetryLimitSet: true, Preset: config.PresetBalanced,
		VerificationMode: "structural", RejectLocalFallback: true,
		Materialize: func(context.Context, []IntentCandidateCapture) error {
			return nil
		},
	})
	var fallbackErr *IntentSemanticFallbackRequiredError
	if !errors.As(err, &fallbackErr) || planner.calls != 2 ||
		result.Fallback != "evidence_partition" || len(result.Decisions) != 0 {
		t.Fatalf("semantic fallback calls=%d result=%+v err=%v",
			planner.calls, result, err)
	}
	candidates, loadErr := state.IntentCandidatesForPair(
		ctx, db, "refs/heads/main", 1, state.IntentCandidateMaxOpenPerPair)
	if loadErr != nil || len(candidates) != 0 {
		t.Fatalf("persisted fallback candidates=%+v err=%v", candidates, loadErr)
	}
}

func TestIntentCandidateEngineStopsRepeatedInvalidPlanEarly(t *testing.T) {
	ctx := context.Background()
	db := openIntentCandidateTestDB(t)
	capture := appendIntentCandidateCapture(t, db, "a.go", "create", "", "a")
	planner := &selfDependentIntentCandidatePlannerStub{}
	result, err := EvaluateIntentCandidates(ctx, db, IntentCandidateEvaluation{
		BranchRef: "refs/heads/main", BranchGeneration: 1,
		Captures: []IntentCandidateCapture{capture}, Planner: planner,
		RetryLimit: 2, RetryLimitSet: true, Preset: config.PresetFast,
		Materialize: func(context.Context, []IntentCandidateCapture) error { return nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	if planner.calls != 2 || result.PlanAttempt != 2 ||
		result.PlanAttemptLimit != 3 || result.ResolutionMode != "evidence_partition" {
		t.Fatalf("no-progress fallback calls=%d result=%+v", planner.calls, result)
	}
}

func TestIntentCandidateValidationFallbackKeepsTransportCircuitClosed(t *testing.T) {
	ctx := context.Background()
	db := openIntentCandidateTestDB(t)
	capture := appendIntentCandidateCapture(t, db, "a.go", "create", "", "a")
	planner := &selfDependentIntentCandidatePlannerStub{}
	health := NewIntentPlannerHealth(ctx, db, IntentPlannerHealthOptions{
		Provider: IntentPlannerProviderIdentity{Provider: planner.Name()},
	})
	result, err := EvaluateIntentCandidates(ctx, db, IntentCandidateEvaluation{
		BranchRef: "refs/heads/main", BranchGeneration: 1,
		Captures: []IntentCandidateCapture{capture}, Planner: planner, Health: health,
		RetryLimit: 2, RetryLimitSet: true, Preset: config.PresetQuality,
		VerificationMode: "structural",
		Materialize:      func(context.Context, []IntentCandidateCapture) error { return nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.ResolutionMode != "evidence_partition" || planner.calls != 2 {
		t.Fatalf("validation recovery calls=%d result=%+v", planner.calls, result)
	}
	if snapshot := health.Snapshot(); snapshot.State != IntentPlannerCircuitClosed ||
		snapshot.ConsecutiveFailures != 0 {
		t.Fatalf("semantic validation changed transport health: %+v", snapshot)
	}
}

func TestIntentCandidateEnginePreservesValidGroupsDuringPartialReplan(t *testing.T) {
	ctx := context.Background()
	db := openIntentCandidateTestDB(t)
	first := appendIntentCandidateCapture(t, db, "source.go", "create", "", "a")
	second := appendIntentCandidateCapture(t, db, "source_test.go", "create", "", "b")
	planner := &partialReplanIntentCandidatePlannerStub{}
	result, err := EvaluateIntentCandidates(ctx, db, IntentCandidateEvaluation{
		BranchRef: "refs/heads/main", BranchGeneration: 1,
		Captures: []IntentCandidateCapture{first, second}, Planner: planner,
		RetryLimit: 2, RetryLimitSet: true, Preset: config.PresetBalanced,
		VerificationMode: "structural",
		Materialize:      func(context.Context, []IntentCandidateCapture) error { return nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	if planner.calls != 2 || len(planner.reqs[1].OfferedCaptures) != 1 ||
		planner.reqs[1].OfferedCaptures[0].Seq != second.Event.Seq ||
		result.ResolutionMode != "partial_replan" || result.PreservedGroupCount != 1 ||
		len(result.Decisions) != 2 {
		t.Fatalf("partial replan requests=%+v result=%+v", planner.reqs, result)
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

func TestIntentNormalizeStabilizedPlanCollapsesSatisfiedPersistedDependency(t *testing.T) {
	const persistedID = "intent-346340d0cb795eedc5e8b7c8"
	plan := ai.IntentPlanV2{
		ProtocolVersion: ai.IntentPlannerProtocolV2,
		Candidates: []ai.IntentCandidateAssignment{
			{
				CandidateID: "provider-dependent", SelectedSeqs: []int64{86651},
				Purpose:   "continue the persisted documentation change",
				Readiness: ai.IntentCandidateReady,
				Subject:   "Continue documentation history",
				GroupingReason: "same-path hard dependency continues the " +
					"persisted candidate",
				DependsOnCandidates: []string{persistedID, persistedID},
			},
		},
	}
	stabilized, err := stabilizeIntentCandidatePlan(plan, []state.IntentCandidate{{
		ID: persistedID, BranchRef: "refs/heads/main", BranchGeneration: 412,
		Events: []state.IntentCandidateEvent{
			{EventSeq: 86622, EventRole: "code"},
			{EventSeq: 86651, EventRole: "code"},
		},
	}}, "refs/heads/main", 412)
	if err != nil {
		t.Fatal(err)
	}
	if len(stabilized.Candidates) != 1 ||
		stabilized.Candidates[0].CandidateID != persistedID ||
		len(stabilized.Candidates[0].DependsOnCandidates) != 0 {
		t.Fatalf("stabilized topology=%+v", stabilized.Candidates)
	}
	req, err := ai.NewIntentPlanRequestV2(ai.IntentPlanRequestV2Options{
		OfferedCaptures: []ai.OfferedCapture{{
			Seq: 86651, Path: "docs/history/README.md", Op: "modify",
		}},
		Candidates: []ai.IntentCandidateSummary{{
			CandidateID: persistedID, Status: "active", Ready: true,
			SelectedSeqs: []int64{86622},
		}},
		Dependencies: []ai.IntentCaptureDependency{{
			FromSeq: 86622, ToSeq: 86651,
			Strength: ai.IntentDependencyHard, Kind: "same_path_order",
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := ai.ValidateIntentPlanV2(req, stabilized); err != nil {
		t.Fatalf("normalized 86622 -> 86651 plan: %v", err)
	}
}

func TestIntentNormalizeStabilizedPlanDeduplicatesAndTopologicallyOrders(t *testing.T) {
	plan := ai.IntentPlanV2{
		ProtocolVersion: ai.IntentPlannerProtocolV2,
		Candidates: []ai.IntentCandidateAssignment{
			{
				CandidateID: "dependent", SelectedSeqs: []int64{20},
				DependsOnCandidates: []string{
					"prerequisite", "prerequisite", "persisted-external",
				},
			},
			{CandidateID: "independent", SelectedSeqs: []int64{30}},
			{CandidateID: "prerequisite", SelectedSeqs: []int64{10}},
		},
	}
	stabilized, err := stabilizeIntentCandidatePlan(
		plan, nil, "refs/heads/main", 9)
	if err != nil {
		t.Fatal(err)
	}
	if got := []int64{
		stabilized.Candidates[0].SelectedSeqs[0],
		stabilized.Candidates[1].SelectedSeqs[0],
		stabilized.Candidates[2].SelectedSeqs[0],
	}; !reflect.DeepEqual(got, []int64{10, 20, 30}) {
		t.Fatalf("stable topological order=%v", got)
	}
	dependencies := stabilized.Candidates[1].DependsOnCandidates
	if len(dependencies) != 2 || dependencies[1] != "persisted-external" {
		t.Fatalf("normalized dependencies=%v", dependencies)
	}
	if dependencies[0] != stabilized.Candidates[0].CandidateID {
		t.Fatalf("internal dependency=%q prerequisite=%q",
			dependencies[0], stabilized.Candidates[0].CandidateID)
	}
}

func TestIntentNormalizePreservesExplicitSelfDependencyForValidation(t *testing.T) {
	plan := ai.IntentPlanV2{
		ProtocolVersion: ai.IntentPlannerProtocolV2,
		Candidates: []ai.IntentCandidateAssignment{{
			CandidateID: "self", SelectedSeqs: []int64{1},
			DependsOnCandidates: []string{"self"},
		}},
	}
	stabilized, err := stabilizeIntentCandidatePlan(
		plan, nil, "refs/heads/main", 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(stabilized.Candidates[0].DependsOnCandidates) != 1 ||
		stabilized.Candidates[0].DependsOnCandidates[0] !=
			stabilized.Candidates[0].CandidateID {
		t.Fatalf("explicit self dependency was hidden: %+v",
			stabilized.Candidates[0])
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
		second.Fallback != "evidence_partition" ||
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

func TestIntentCandidateEngineRetriesEligibleMetadataWithCorrection(t *testing.T) {
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

func TestIntentCandidatePlanRepairsProvenHardDependencyDeclaration(t *testing.T) {
	req, err := ai.NewIntentPlanRequestV2(ai.IntentPlanRequestV2Options{
		OfferedCaptures: []ai.OfferedCapture{
			{Seq: 86512, Path: "a.go", Op: "modify"},
			{Seq: 86519, Path: "b.go", Op: "modify"},
		},
		Dependencies: []ai.IntentCaptureDependency{{
			FromSeq: 86512, ToSeq: 86519, Strength: ai.IntentDependencyHard,
			Kind: "object_reference", EvidenceHash: "sha256:proven",
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	plan := ai.IntentPlanV2{
		ProtocolVersion: ai.IntentPlannerProtocolV2,
		Candidates: []ai.IntentCandidateAssignment{
			{
				CandidateID: "prerequisite", SelectedSeqs: []int64{86512},
				Purpose: "update prerequisite", Readiness: ai.IntentCandidateReady,
				Subject: "Update prerequisite", Body: "- preserve exact body",
				GroupingReason: "independent candidate",
			},
			{
				CandidateID: "dependent", SelectedSeqs: []int64{86519},
				Purpose: "update dependent", Readiness: ai.IntentCandidateReady,
				Subject: "Update dependent", Body: "- preserve exact body",
				GroupingReason: "independent candidate",
			},
		},
	}
	original := cloneIntentPlanV2(plan)
	repaired, ok := repairIntentCandidateDependencies(req, plan)
	if !ok {
		t.Fatal("proven dependency was not repaired")
	}
	if !reflect.DeepEqual(plan, original) {
		t.Fatalf("original plan mutated: got=%+v want=%+v", plan, original)
	}
	wantDependencies := []string{"prerequisite"}
	if !reflect.DeepEqual(repaired.Candidates[1].DependsOnCandidates, wantDependencies) {
		t.Fatalf("dependencies=%v want=%v",
			repaired.Candidates[1].DependsOnCandidates, wantDependencies)
	}
	repaired.Candidates[1].DependsOnCandidates = nil
	original.Candidates[1].DependsOnCandidates = nil
	if !reflect.DeepEqual(repaired, original) {
		t.Fatalf("repair changed non-dependency fields: got=%+v want=%+v",
			repaired, original)
	}
}

func TestIntentCandidateEngineRepairsDependencyWithoutPlannerRetry(t *testing.T) {
	ctx := context.Background()
	db := openIntentCandidateTestDB(t)
	prerequisite := appendIntentCandidateCapture(
		t, db, "a.go", "create", "", "a")
	dependent := appendIntentCandidateCapture(
		t, db, "b.go", "create", "", "b")
	planner := &intentCandidatePlannerStub{plan: ai.IntentPlanV2{
		ProtocolVersion: ai.IntentPlannerProtocolV2,
		Candidates: []ai.IntentCandidateAssignment{
			{CandidateID: "prerequisite", SelectedSeqs: []int64{prerequisite.Event.Seq}, Purpose: "update prerequisite", Readiness: ai.IntentCandidateReady, Subject: "Update prerequisite", GroupingReason: "separate hard-linked change"},
			{CandidateID: "dependent", SelectedSeqs: []int64{dependent.Event.Seq}, Purpose: "update dependent", Readiness: ai.IntentCandidateReady, Subject: "Update dependent", GroupingReason: "separate hard-linked change"},
		},
	}}
	result, err := EvaluateIntentCandidates(ctx, db, IntentCandidateEvaluation{
		BranchRef: "refs/heads/main", BranchGeneration: 1,
		Captures: []IntentCandidateCapture{prerequisite, dependent},
		Hints: []IntentDependencyHint{{
			PrerequisiteSeq: prerequisite.Event.Seq,
			DependentSeq:    dependent.Event.Seq,
			Strength:        ai.IntentDependencyHard,
			Kind:            "object_reference",
			Evidence:        "mechanically proven",
		}},
		Planner: planner, RetryLimit: 9, RetryLimitSet: true,
		Preset: config.PresetBalanced, VerificationMode: "structural",
		Materialize: func(context.Context, []IntentCandidateCapture) error { return nil },
	})
	if err != nil {
		t.Fatalf("EvaluateIntentCandidates: %v", err)
	}
	if planner.calls != 1 || result.RetryCount != 0 ||
		result.Fallback != "repaired_dependency_declarations" ||
		result.PlannerFailure == "" || result.NeedsAttention {
		t.Fatalf("repair result calls=%d result=%+v", planner.calls, result)
	}
	if len(result.Decisions) != 2 ||
		!reflect.DeepEqual(result.Decisions[1].Assignment.DependsOnCandidates,
			[]string{result.Decisions[0].Assignment.CandidateID}) {
		t.Fatalf("repair decisions=%+v", result.Decisions)
	}
	if len(planner.plan.Candidates[1].DependsOnCandidates) != 0 {
		t.Fatalf("planner plan mutated=%+v", planner.plan)
	}
}

func TestIntentCandidatePlanReusesCompletedResolution(t *testing.T) {
	ctx := context.Background()
	db := openIntentCandidateTestDB(t)
	req, err := ai.NewIntentPlanRequestV2(ai.IntentPlanRequestV2Options{
		OfferedCaptures: []ai.OfferedCapture{{
			Seq: 1, Path: "service.go", Op: "modify",
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	planner := &intentCandidatePlannerStub{plan: ai.IntentPlanV2{
		ProtocolVersion: ai.IntentPlannerProtocolV2,
		Candidates: []ai.IntentCandidateAssignment{{
			CandidateID: "service-change", SelectedSeqs: []int64{1},
			Purpose: "fix service behavior", Readiness: ai.IntentCandidateReady,
			Subject:        "Fix service behavior",
			GroupingReason: "the capture contains the complete service fix",
		}},
	}}
	input := IntentCandidateEvaluation{
		BranchRef: "refs/heads/main", BranchGeneration: 1,
		Preset: config.PresetBalanced, Provider: planner.Name(),
		CommitFormat: ai.CommitFormatImperative,
	}

	first, _, _, _, _, _, firstRun, err := chooseIntentCandidatePlan(
		ctx, req, planner, nil, 2, config.PresetBalanced, nil, db, input)
	if err != nil {
		t.Fatal(err)
	}
	second, fallback, failure, _, _, _, secondRun, err :=
		chooseIntentCandidatePlan(
			ctx, req, planner, nil, 2, config.PresetBalanced, nil, db, input)
	if err != nil {
		t.Fatal(err)
	}
	if planner.calls != 1 {
		t.Fatalf("planner calls=%d want 1", planner.calls)
	}
	if fallback != "" || failure != "" ||
		secondRun.ResolutionMode.String != "completed_plan_reuse" {
		t.Fatalf("reused resolution fallback=%q failure=%q run=%+v",
			fallback, failure, secondRun)
	}
	if !reflect.DeepEqual(first, second) || !firstRun.ResolvedPlanJSON.Valid {
		t.Fatalf("reused plan differs: first=%+v second=%+v run=%+v",
			first, second, firstRun)
	}
}

func TestIntentCandidatePlanRefusesNonTopologicalDependencyRepair(t *testing.T) {
	req, err := ai.NewIntentPlanRequestV2(ai.IntentPlanRequestV2Options{
		OfferedCaptures: []ai.OfferedCapture{
			{Seq: 1, Path: "a.go", Op: "modify"},
			{Seq: 2, Path: "b.go", Op: "modify"},
		},
		Dependencies: []ai.IntentCaptureDependency{{
			FromSeq: 2, ToSeq: 1, Strength: ai.IntentDependencyHard,
			Kind: "object_reference", EvidenceHash: "sha256:proven",
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	plan := ai.IntentPlanV2{ProtocolVersion: ai.IntentPlannerProtocolV2,
		Candidates: []ai.IntentCandidateAssignment{
			{CandidateID: "dependent", SelectedSeqs: []int64{1}, Purpose: "dependent", Readiness: ai.IntentCandidateReady, Subject: "Update dependent", GroupingReason: "independent candidate"},
			{CandidateID: "later-prerequisite", SelectedSeqs: []int64{2}, Purpose: "prerequisite", Readiness: ai.IntentCandidateReady, Subject: "Update prerequisite", GroupingReason: "independent candidate"},
		}}
	if repaired, ok := repairIntentCandidateDependencies(req, plan); ok || !reflect.DeepEqual(repaired, plan) {
		t.Fatalf("unsafe repair accepted: ok=%v plan=%+v", ok, repaired)
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
	if result.Fallback != "evidence_partition" {
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
	if result.Fallback != "evidence_partition" ||
		len(result.Decisions) != 2 ||
		!result.Decisions[0].Publishable ||
		!result.Decisions[1].Publishable {
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
		!initial.Decisions[1].Publishable {
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
			wantFallback: "evidence_partition", wantReady: 2,
		},
		{
			name: "balanced", preset: config.PresetBalanced,
			wantFallback: "evidence_partition", wantReady: 2,
			wantVerifyCall: 2,
		},
		{
			name: "quality", preset: config.PresetQuality,
			wantFallback: "evidence_partition", wantReady: 2,
			wantVerifyCall: 2,
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

func TestIntentCandidateEngineModelWideFailureKeepsValidatedFallback(t *testing.T) {
	for _, preset := range []config.PresetName{
		config.PresetFast,
		config.PresetBalanced,
	} {
		t.Run(string(preset), func(t *testing.T) {
			ctx := context.Background()
			db := openIntentCandidateTestDB(t)
			first := appendIntentCandidateCapture(
				t, db, "internal/a.go", "create", "", "a")
			second := appendIntentCandidateCapture(
				t, db, "internal/b.go", "create", "", "b")
			planner := &modelWideFailingIntentCandidatePlannerStub{}
			result, err := EvaluateIntentCandidates(ctx, db,
				IntentCandidateEvaluation{
					BranchRef: "refs/heads/main", BranchGeneration: 1,
					Captures: []IntentCandidateCapture{first, second},
					Planner:  planner, RetryLimit: 9, RetryLimitSet: true,
					Preset: preset, VerificationMode: "structural",
					Materialize: func(
						context.Context,
						[]IntentCandidateCapture,
					) error {
						return nil
					},
				})
			if err != nil {
				t.Fatalf("EvaluateIntentCandidates: %v", err)
			}
			if planner.plannerCalls != 1 || planner.rewriteCalls != 1 ||
				result.RetryCount != 0 || result.Fallback == "" ||
				result.NeedsAttention || len(result.Decisions) == 0 {
				t.Fatalf("bounded fallback planner=%d rewrite=%d result=%+v",
					planner.plannerCalls, planner.rewriteCalls, result)
			}
			if !strings.Contains(result.PlannerFailure,
				"provider planning unavailable") ||
				!strings.Contains(result.PlannerFailure,
					"message quality fallback") {
				t.Fatalf("planner failure=%q", result.PlannerFailure)
			}
			publishable := 0
			for _, decision := range result.Decisions {
				if !decision.Publishable {
					continue
				}
				publishable++
				if decision.Assignment.Subject == "" || !decision.Atomicity.Valid {
					t.Fatalf("fallback decision=%+v", decision)
				}
			}
			if publishable == 0 {
				t.Fatalf("fallback published no safe decision: %+v", result)
			}
		})
	}
}

func TestIntentCandidateEngineFallbackContinuesPersistedDependent(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	db := openIntentCandidateTestDB(t)
	setNextIntentCandidateSeq(t, db, 86512)
	prerequisite := appendIntentCandidateCapture(
		t, db, "same.go", "create", "", "first")
	setNextIntentCandidateSeq(t, db, 86519)
	dependent := appendIntentCandidateCapture(
		t, db, "same.go", "modify", "first", "second")
	if prerequisite.Event.Seq != 86512 || dependent.Event.Seq != 86519 {
		t.Fatalf("observed dependent shape=%d->%d",
			prerequisite.Event.Seq, dependent.Event.Seq)
	}
	const candidateID = "persisted-dependent"
	if err := state.SaveIntentCandidate(ctx, db, state.IntentCandidate{
		ID: candidateID, BranchRef: "refs/heads/main", BranchGeneration: 1,
		Status: state.IntentCandidateWaiting, Purpose: "finish same-path change",
		Readiness: state.IntentReadinessWait,
		Events: []state.IntentCandidateEvent{
			{EventSeq: prerequisite.Event.Seq, EventRole: "code"},
			{EventSeq: dependent.Event.Seq, EventRole: "code"},
		},
	}); err != nil {
		t.Fatal(err)
	}

	result, err := EvaluateIntentCandidates(ctx, db, IntentCandidateEvaluation{
		BranchRef: "refs/heads/main", BranchGeneration: 1,
		Captures: []IntentCandidateCapture{prerequisite},
		Planner: &intentCandidatePlannerStub{
			err: errors.New("provider unavailable"),
		},
		Preset: config.PresetBalanced, VerificationMode: "structural",
		Materialize: func(
			context.Context,
			[]IntentCandidateCapture,
		) error {
			return nil
		},
	})
	if err != nil {
		t.Fatalf("EvaluateIntentCandidates: %v", err)
	}
	if result.Fallback != "evidence_partition" ||
		len(result.Decisions) != 1 {
		t.Fatalf("fallback result=%+v", result)
	}
	decision := result.Decisions[0]
	if !decision.Publishable || decision.Candidate.ID != candidateID ||
		len(decision.Candidate.Events) != 2 {
		t.Fatalf("continued candidate=%+v", decision.Candidate)
	}
	if decision.Candidate.Events[0].EventSeq != prerequisite.Event.Seq ||
		decision.Candidate.Events[1].EventSeq != dependent.Event.Seq {
		t.Fatalf("continued events=%+v", decision.Candidate.Events)
	}
}

func TestIntentCandidateEngineFallbackContinuesPersistedPrerequisite(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	db := openIntentCandidateTestDB(t)
	setNextIntentCandidateSeq(t, db, 86508)
	prerequisite := appendIntentCandidateCapture(
		t, db, "same.go", "create", "", "first")
	setNextIntentCandidateSeq(t, db, 86525)
	dependent := appendIntentCandidateCapture(
		t, db, "same.go", "modify", "first", "second")
	if prerequisite.Event.Seq != 86508 || dependent.Event.Seq != 86525 {
		t.Fatalf("observed prerequisite shape=%d->%d",
			prerequisite.Event.Seq, dependent.Event.Seq)
	}
	const candidateID = "persisted-prerequisite"
	if err := state.SaveIntentCandidate(ctx, db, state.IntentCandidate{
		ID: candidateID, BranchRef: "refs/heads/main", BranchGeneration: 1,
		Status:    state.IntentCandidateWaiting,
		Purpose:   "wait for prerequisite completion",
		Readiness: state.IntentReadinessWait,
		Events: []state.IntentCandidateEvent{{
			EventSeq: prerequisite.Event.Seq, EventRole: "code",
		}},
	}); err != nil {
		t.Fatal(err)
	}

	result, err := EvaluateIntentCandidates(ctx, db, IntentCandidateEvaluation{
		BranchRef: "refs/heads/main", BranchGeneration: 1,
		Captures: []IntentCandidateCapture{dependent},
		Planner: &intentCandidatePlannerStub{
			err: errors.New("provider unavailable"),
		},
		Preset: config.PresetBalanced, VerificationMode: "structural",
		Materialize: func(
			context.Context,
			[]IntentCandidateCapture,
		) error {
			return nil
		},
	})
	if err != nil {
		t.Fatalf("EvaluateIntentCandidates: %v", err)
	}
	if result.Fallback != "evidence_partition" ||
		len(result.Decisions) != 1 {
		t.Fatalf("fallback result=%+v", result)
	}
	decision := result.Decisions[0]
	if !decision.Publishable || decision.Candidate.ID != candidateID ||
		len(decision.Candidate.Events) != 2 {
		t.Fatalf("continued fallback=%+v", decision)
	}
}

func TestIntentCandidateEngineBalancedFallbackMergesThroughPersistedCandidate(t *testing.T) {
	t.Parallel()
	testIntentCandidateFallbackMergesThroughPersistedCandidate(
		t, config.PresetBalanced)
}

func TestIntentCandidateEngineFastFallbackMergesThroughPersistedCandidate(t *testing.T) {
	t.Parallel()
	testIntentCandidateFallbackMergesThroughPersistedCandidate(
		t, config.PresetFast)
}

func TestIntentCandidateEngineFallbackMergesHardBridgeBetweenSoftCandidates(
	t *testing.T,
) {
	t.Parallel()
	ctx := context.Background()
	db := openIntentCandidateTestDB(t)
	setNextIntentCandidateSeq(t, db, 6097)
	left := appendIntentCandidateCapture(
		t, db, "internal/left.go", "create", "", "left")
	bridge := appendIntentCandidateCapture(
		t, db, "internal/bridge.go", "create", "", "bridge")
	setNextIntentCandidateSeq(t, db, 6105)
	right := appendIntentCandidateCapture(
		t, db, "internal/right.go", "create", "", "right")
	if left.Event.Seq != 6097 || bridge.Event.Seq != 6098 ||
		right.Event.Seq != 6105 {
		t.Fatalf("production sequence fixture=%d,%d,%d",
			left.Event.Seq, bridge.Event.Seq, right.Event.Seq)
	}
	saveSoftPublishedIntentCandidate(
		t, db, "soft-left", left, "left-commit", 100)
	saveSoftPublishedIntentCandidate(
		t, db, "soft-right", right, "right-commit", 110)

	result, err := EvaluateIntentCandidates(ctx, db, IntentCandidateEvaluation{
		BranchRef: "refs/heads/main", BranchGeneration: 1,
		Captures: []IntentCandidateCapture{bridge},
		Hints: []IntentDependencyHint{
			{
				PrerequisiteSeq: left.Event.Seq,
				DependentSeq:    bridge.Event.Seq,
				Strength:        ai.IntentDependencyHard,
				Kind:            "object_reference",
				Evidence:        "left to bridge",
			},
			{
				PrerequisiteSeq: bridge.Event.Seq,
				DependentSeq:    right.Event.Seq,
				Strength:        ai.IntentDependencyHard,
				Kind:            "object_reference",
				Evidence:        "bridge to right",
			},
		},
		Now: time.Unix(120, 0),
		Planner: &intentCandidatePlannerStub{
			err: errors.New("provider unavailable"),
		},
		Preset: config.PresetBalanced, VerificationMode: "structural",
		Materialize: func(
			_ context.Context,
			captures []IntentCandidateCapture,
		) error {
			if got := intentCandidateCaptureSeqs(captures); !reflect.DeepEqual(
				got, []int64{6097, 6098, 6105},
			) {
				return fmt.Errorf("materialized hard bridge=%v", got)
			}
			return nil
		},
	})
	if err != nil {
		t.Fatalf("EvaluateIntentCandidates hard bridge: %v", err)
	}
	if result.Fallback != "evidence_partition" ||
		result.NeedsAttention || len(result.Decisions) != 1 ||
		!result.Decisions[0].Publishable {
		t.Fatalf("hard bridge result=%+v", result)
	}
	decision := result.Decisions[0]
	if decision.Candidate.ID != "soft-left" ||
		!reflect.DeepEqual(intentCandidateEventSeqs(decision.Candidate.Events),
			[]int64{6097, 6098, 6105}) {
		t.Fatalf("canonical hard bridge=%+v", decision.Candidate)
	}
	leftCandidate, ok, err := state.IntentCandidateByID(
		ctx, db, "soft-left")
	if err != nil || !ok ||
		!reflect.DeepEqual(intentCandidateEventSeqs(leftCandidate.Events),
			[]int64{6097, 6098, 6105}) {
		t.Fatalf("persisted target=%+v ok=%v err=%v",
			leftCandidate, ok, err)
	}
	rightCandidate, ok, err := state.IntentCandidateByID(
		ctx, db, "soft-right")
	if err != nil || !ok ||
		rightCandidate.Status != state.IntentCandidateSuperseded ||
		len(rightCandidate.Events) != 0 {
		t.Fatalf("persisted source=%+v ok=%v err=%v",
			rightCandidate, ok, err)
	}
	lineage, err := state.IntentCandidateLineageForTarget(
		ctx, db, "refs/heads/main", 1, "soft-left", 10)
	if err != nil || len(lineage) != 1 ||
		lineage[0].SourceCandidateID != "soft-right" ||
		lineage[0].SourceStatus != state.IntentCandidateSoftPublished ||
		!lineage[0].SourcePublishedCommitOID.Valid ||
		lineage[0].SourcePublishedCommitOID.String != "right-commit" {
		t.Fatalf("hard bridge lineage=%+v err=%v", lineage, err)
	}
}

func TestIntentCandidateEngineNativePlannerMergesHardBridgeBetweenCandidates(
	t *testing.T,
) {
	t.Parallel()
	ctx := context.Background()
	db := openIntentCandidateTestDB(t)
	left := appendIntentCandidateCapture(
		t, db, "left.go", "create", "", "left")
	bridge := appendIntentCandidateCapture(
		t, db, "bridge.go", "create", "", "bridge")
	right := appendIntentCandidateCapture(
		t, db, "right.go", "create", "", "right")
	saveSoftPublishedIntentCandidate(
		t, db, "native-left", left, "left-commit", 100)
	saveSoftPublishedIntentCandidate(
		t, db, "native-right", right, "right-commit", 110)
	planner := &intentCandidatePlannerStub{plan: ai.IntentPlanV2{
		ProtocolVersion: ai.IntentPlannerProtocolV2,
		Candidates: []ai.IntentCandidateAssignment{{
			CandidateID: "native-left", SelectedSeqs: []int64{bridge.Event.Seq},
			Purpose: "complete the hard-linked change", Readiness: ai.IntentCandidateReady,
			Subject:        "Complete hard-linked change",
			GroupingReason: "hard bridge completes the persisted candidate",
		}},
	}}

	result, err := EvaluateIntentCandidates(ctx, db, IntentCandidateEvaluation{
		BranchRef: "refs/heads/main", BranchGeneration: 1,
		Captures: []IntentCandidateCapture{bridge},
		Hints: []IntentDependencyHint{
			{
				PrerequisiteSeq: left.Event.Seq,
				DependentSeq:    bridge.Event.Seq,
				Strength:        ai.IntentDependencyHard,
				Kind:            "object_reference",
				Evidence:        "left to bridge",
			},
			{
				PrerequisiteSeq: bridge.Event.Seq,
				DependentSeq:    right.Event.Seq,
				Strength:        ai.IntentDependencyHard,
				Kind:            "object_reference",
				Evidence:        "bridge to right",
			},
		},
		Now: time.Unix(120, 0), Planner: planner,
		Preset: config.PresetBalanced, VerificationMode: "structural",
		Materialize: func(
			_ context.Context,
			captures []IntentCandidateCapture,
		) error {
			if len(captures) != 3 {
				return fmt.Errorf("native bridge materialized %d captures",
					len(captures))
			}
			return nil
		},
	})
	if err != nil {
		t.Fatalf("EvaluateIntentCandidates native hard bridge: %v", err)
	}
	if result.Fallback != "" || result.NeedsAttention ||
		len(result.Decisions) != 1 || !result.Decisions[0].Publishable ||
		result.Decisions[0].Candidate.ID != "native-left" ||
		len(result.Decisions[0].Candidate.Events) != 3 {
		t.Fatalf("native hard bridge result=%+v", result)
	}
	if len(planner.req.Candidates) != 1 ||
		planner.req.Candidates[0].CandidateID != "native-left" ||
		len(planner.req.Candidates[0].SelectedSeqs) != 2 {
		t.Fatalf("canonical planner request candidates=%+v",
			planner.req.Candidates)
	}
	rightCandidate, ok, err := state.IntentCandidateByID(
		ctx, db, "native-right")
	if err != nil || !ok ||
		rightCandidate.Status != state.IntentCandidateSuperseded {
		t.Fatalf("native source=%+v ok=%v err=%v",
			rightCandidate, ok, err)
	}
}

func TestIntentCandidateEngineBalancedFallbackKeepsA1B1A2Atomic(
	t *testing.T,
) {
	t.Parallel()
	ctx := context.Background()
	db := openIntentCandidateTestDB(t)
	firstA := appendIntentCandidateCapture(
		t, db, "internal/a.go", "create", "", "a1")
	firstB := appendIntentCandidateCapture(
		t, db, "internal/b.go", "create", "", "b1")
	secondA := appendIntentCandidateCapture(
		t, db, "internal/a.go", "modify", "a1", "a2")
	result, err := EvaluateIntentCandidates(ctx, db, IntentCandidateEvaluation{
		BranchRef: "refs/heads/main", BranchGeneration: 1,
		Captures: []IntentCandidateCapture{firstA, firstB, secondA},
		Planner: &intentCandidatePlannerStub{
			err: errors.New("provider unavailable"),
		},
		Preset: config.PresetBalanced, VerificationMode: "structural",
		Materialize: func(context.Context, []IntentCandidateCapture) error {
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Decisions) != 2 {
		t.Fatalf("fallback decisions=%+v", result.Decisions)
	}
	got := make([][]int64, 0, 2)
	for _, decision := range result.Decisions {
		if !decision.Publishable {
			t.Fatalf("fallback decision not publishable=%+v", decision)
		}
		got = append(got, decision.Assignment.SelectedSeqs)
	}
	want := [][]int64{
		{firstA.Event.Seq, secondA.Event.Seq},
		{firstB.Event.Seq},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("A1,B1,A2 fallback=%v want=%v", got, want)
	}
}

func TestIntentCandidateEngineBalancedFallbackDoesNotMegaGroupWeakEvidence(
	t *testing.T,
) {
	t.Parallel()
	ctx := context.Background()
	db := openIntentCandidateTestDB(t)
	var captures []IntentCandidateCapture
	var hints []IntentDependencyHint
	for i := 0; i < 6; i++ {
		capture := appendIntentCandidateCapture(
			t, db, fmt.Sprintf("component-%d.go", i),
			"create", "", fmt.Sprintf("value-%d", i))
		captures = append(captures, capture)
		if i > 0 {
			hints = append(hints, IntentDependencyHint{
				PrerequisiteSeq: captures[i-1].Event.Seq,
				DependentSeq:    capture.Event.Seq,
				Strength:        ai.IntentDependencySoft,
				Kind:            "symbol_hash",
				Evidence:        "shared weak symbol",
			})
		}
	}
	materializeCalls := 0
	result, err := EvaluateIntentCandidates(ctx, db, IntentCandidateEvaluation{
		BranchRef: "refs/heads/main", BranchGeneration: 1,
		Captures: captures, Hints: hints,
		Planner: &intentCandidatePlannerStub{
			err: errors.New("provider unavailable"),
		},
		Preset: config.PresetBalanced, VerificationMode: "structural",
		Materialize: func(context.Context, []IntentCandidateCapture) error {
			materializeCalls++
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Decisions) != len(captures) ||
		materializeCalls != len(captures) ||
		result.NeedsAttention {
		t.Fatalf("weak evidence fallback=%+v materialize=%d",
			result, materializeCalls)
	}
	for _, decision := range result.Decisions {
		if len(decision.Assignment.SelectedSeqs) != 1 ||
			!decision.Publishable {
			t.Fatalf("weak evidence created mega-group=%+v", decision)
		}
	}
}

func TestIntentCandidateEngineBalancedFallbackKeepsImportEvidenceSeparate(
	t *testing.T,
) {
	t.Parallel()
	ctx := context.Background()
	db := openIntentCandidateTestDB(t)
	left := appendIntentCandidateCapture(
		t, db, "left.go", "create", "", "left")
	right := appendIntentCandidateCapture(
		t, db, "right.go", "create", "", "right")
	result, err := EvaluateIntentCandidates(ctx, db, IntentCandidateEvaluation{
		BranchRef: "refs/heads/main", BranchGeneration: 1,
		Captures: []IntentCandidateCapture{left, right},
		Hints: []IntentDependencyHint{{
			PrerequisiteSeq: left.Event.Seq,
			DependentSeq:    right.Event.Seq,
			Strength:        ai.IntentDependencySoft,
			Kind:            "import_reference",
			Evidence:        "reference similarity alone",
		}},
		Planner: &intentCandidatePlannerStub{
			err: errors.New("provider unavailable"),
		},
		Preset: config.PresetBalanced, VerificationMode: "structural",
		Materialize: func(context.Context, []IntentCandidateCapture) error {
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.NeedsAttention || len(result.Decisions) != 2 {
		t.Fatalf("import-only fallback=%+v", result)
	}
	for _, decision := range result.Decisions {
		if !decision.Publishable ||
			len(decision.Assignment.SelectedSeqs) != 1 {
			t.Fatalf("import evidence merged fallback=%+v", decision)
		}
	}
}

func TestIntentCandidateEngineBalancedFallbackUsesUnambiguousTestCompanion(
	t *testing.T,
) {
	t.Parallel()
	ctx := context.Background()
	db := openIntentCandidateTestDB(t)
	source := appendIntentCandidateCapture(
		t, db, "feature.go", "create", "", "source")
	test := appendIntentCandidateCapture(
		t, db, "feature_test.go", "create", "", "test")
	result, err := EvaluateIntentCandidates(ctx, db, IntentCandidateEvaluation{
		BranchRef: "refs/heads/main", BranchGeneration: 1,
		Captures: []IntentCandidateCapture{source, test},
		Hints: []IntentDependencyHint{{
			PrerequisiteSeq: source.Event.Seq,
			DependentSeq:    test.Event.Seq,
			Strength:        ai.IntentDependencySoft,
			Kind:            "test_source",
			Evidence:        "exact test companion",
		}},
		Planner: &intentCandidatePlannerStub{
			err: errors.New("provider unavailable"),
		},
		Preset: config.PresetBalanced, VerificationMode: "structural",
		Materialize: func(context.Context, []IntentCandidateCapture) error {
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.NeedsAttention || len(result.Decisions) != 1 ||
		!result.Decisions[0].Publishable ||
		!reflect.DeepEqual(result.Decisions[0].Assignment.SelectedSeqs,
			[]int64{source.Event.Seq, test.Event.Seq}) {
		t.Fatalf("unambiguous test companion=%+v", result)
	}
}

func TestIntentCandidateEngineBalancedFallbackContinuesPersistedTestCompanion(
	t *testing.T,
) {
	t.Parallel()
	ctx := context.Background()
	db := openIntentCandidateTestDB(t)
	source := appendIntentCandidateCapture(
		t, db, "feature.go", "create", "", "source")
	test := appendIntentCandidateCapture(
		t, db, "feature_test.go", "create", "", "test")
	saveSoftPublishedIntentCandidate(
		t, db, "persisted-source", source, "source-commit", 100)

	result, err := EvaluateIntentCandidates(ctx, db, IntentCandidateEvaluation{
		BranchRef: "refs/heads/main", BranchGeneration: 1,
		Captures: []IntentCandidateCapture{test},
		Hints: []IntentDependencyHint{{
			PrerequisiteSeq: source.Event.Seq,
			DependentSeq:    test.Event.Seq,
			Strength:        ai.IntentDependencySoft,
			Kind:            "test_source",
			Evidence:        "exact persisted test companion",
		}},
		Now: time.Unix(120, 0),
		Planner: &intentCandidatePlannerStub{
			err: errors.New("provider unavailable"),
		},
		Preset: config.PresetBalanced, VerificationMode: "structural",
		Materialize: func(
			_ context.Context,
			captures []IntentCandidateCapture,
		) error {
			if got := intentCandidateCaptureSeqs(captures); !reflect.DeepEqual(
				got, []int64{source.Event.Seq, test.Event.Seq},
			) {
				return fmt.Errorf("materialized persisted companion=%v", got)
			}
			return nil
		},
	})
	if err != nil {
		t.Fatalf("EvaluateIntentCandidates: %v", err)
	}
	if result.Fallback != "evidence_partition" ||
		result.NeedsAttention || len(result.Decisions) != 1 {
		t.Fatalf("fallback result=%+v", result)
	}
	decision := result.Decisions[0]
	if !decision.Publishable ||
		decision.Candidate.ID != "persisted-source" ||
		!reflect.DeepEqual(
			intentCandidateEventSeqs(decision.Candidate.Events),
			[]int64{source.Event.Seq, test.Event.Seq},
		) {
		t.Fatalf("continued persisted companion=%+v", decision)
	}
	if decision.Assignment.GroupingReason !=
		"bounded deterministic persisted companion continuation" {
		t.Fatalf("grouping reason=%q",
			decision.Assignment.GroupingReason)
	}
}

func TestIntentCandidateEngineBalancedFallbackHoldsAmbiguousPersistedCompanion(
	t *testing.T,
) {
	t.Parallel()
	ctx := context.Background()
	db := openIntentCandidateTestDB(t)
	firstSource := appendIntentCandidateCapture(
		t, db, "first.go", "create", "", "first")
	secondSource := appendIntentCandidateCapture(
		t, db, "second.go", "create", "", "second")
	test := appendIntentCandidateCapture(
		t, db, "feature_test.go", "create", "", "test")
	saveSoftPublishedIntentCandidate(
		t, db, "persisted-first", firstSource, "first-commit", 100)
	saveSoftPublishedIntentCandidate(
		t, db, "persisted-second", secondSource, "second-commit", 101)

	result, err := EvaluateIntentCandidates(ctx, db, IntentCandidateEvaluation{
		BranchRef: "refs/heads/main", BranchGeneration: 1,
		Captures: []IntentCandidateCapture{test},
		Hints: []IntentDependencyHint{
			{
				PrerequisiteSeq: firstSource.Event.Seq,
				DependentSeq:    test.Event.Seq,
				Strength:        ai.IntentDependencySoft,
				Kind:            "test_source",
				Evidence:        "first possible persisted companion",
			},
			{
				PrerequisiteSeq: secondSource.Event.Seq,
				DependentSeq:    test.Event.Seq,
				Strength:        ai.IntentDependencySoft,
				Kind:            "test_source",
				Evidence:        "second possible persisted companion",
			},
		},
		Planner: &intentCandidatePlannerStub{
			err: errors.New("provider unavailable"),
		},
		Preset: config.PresetBalanced, VerificationMode: "structural",
		Now: time.Unix(120, 0),
		Materialize: func(
			context.Context,
			[]IntentCandidateCapture,
		) error {
			return errors.New("ambiguous companion must not materialize")
		},
	})
	if err != nil {
		t.Fatalf("EvaluateIntentCandidates: %v", err)
	}
	if !result.NeedsAttention || len(result.Decisions) != 1 ||
		result.Decisions[0].Publishable ||
		!strings.Contains(strings.Join(
			result.Decisions[0].Assignment.MissingCompanions, " "),
			"ambiguous persisted companion") {
		t.Fatalf("ambiguous persisted companion=%+v", result)
	}
}

func TestIntentCandidateEngineBalancedFallbackHoldsAmbiguousCompanions(
	t *testing.T,
) {
	t.Parallel()
	ctx := context.Background()
	db := openIntentCandidateTestDB(t)
	source := appendIntentCandidateCapture(
		t, db, "source.go", "create", "", "source")
	first := appendIntentCandidateCapture(
		t, db, "first_test.go", "create", "", "first")
	second := appendIntentCandidateCapture(
		t, db, "second_test.go", "create", "", "second")
	materializeCalls := 0
	result, err := EvaluateIntentCandidates(ctx, db, IntentCandidateEvaluation{
		BranchRef: "refs/heads/main", BranchGeneration: 1,
		Captures: []IntentCandidateCapture{source, first, second},
		Hints: []IntentDependencyHint{
			{
				PrerequisiteSeq: source.Event.Seq,
				DependentSeq:    first.Event.Seq,
				Strength:        ai.IntentDependencySoft,
				Kind:            "test_source",
				Evidence:        "first possible companion",
			},
			{
				PrerequisiteSeq: source.Event.Seq,
				DependentSeq:    second.Event.Seq,
				Strength:        ai.IntentDependencySoft,
				Kind:            "test_source",
				Evidence:        "second possible companion",
			},
		},
		Planner: &intentCandidatePlannerStub{
			err: errors.New("provider unavailable"),
		},
		Preset: config.PresetBalanced, VerificationMode: "structural",
		Materialize: func(context.Context, []IntentCandidateCapture) error {
			materializeCalls++
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !result.NeedsAttention || len(result.Decisions) != 3 ||
		materializeCalls != 0 {
		t.Fatalf("ambiguous fallback=%+v materialize=%d",
			result, materializeCalls)
	}
	for _, decision := range result.Decisions {
		if decision.Publishable ||
			!strings.Contains(strings.Join(
				decision.Assignment.MissingCompanions, " "),
				"ambiguous") {
			t.Fatalf("ambiguous companion published=%+v", decision)
		}
	}
}

func TestIntentCandidateEngineBalancedFallbackHoldsOversizedHardComponent(
	t *testing.T,
) {
	t.Parallel()
	ctx := context.Background()
	db := openIntentCandidateTestDB(t)
	captures := make([]IntentCandidateCapture, 0, 33)
	before := ""
	for i := 0; i < 33; i++ {
		operation := "modify"
		if i == 0 {
			operation = "create"
		}
		after := fmt.Sprintf("version-%d", i)
		captures = append(captures, appendIntentCandidateCapture(
			t, db, "oversized.go", operation, before, after))
		before = after
	}
	materializeCalls := 0
	result, err := EvaluateIntentCandidates(ctx, db, IntentCandidateEvaluation{
		BranchRef: "refs/heads/main", BranchGeneration: 1,
		Captures: captures,
		Planner: &intentCandidatePlannerStub{
			err: errors.New("provider unavailable"),
		},
		Preset: config.PresetBalanced, VerificationMode: "structural",
		Materialize: func(context.Context, []IntentCandidateCapture) error {
			materializeCalls++
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !result.NeedsAttention || len(result.Decisions) != 1 ||
		result.Decisions[0].Publishable || materializeCalls != 0 ||
		!strings.Contains(strings.Join(
			result.Decisions[0].Assignment.MissingCompanions, " "),
			"32 captures") {
		t.Fatalf("oversized hard fallback=%+v materialize=%d",
			result, materializeCalls)
	}
}

func TestIntentCandidateEngineBalancedFallbackHoldsTooManyPaths(
	t *testing.T,
) {
	t.Parallel()
	ctx := context.Background()
	db := openIntentCandidateTestDB(t)
	captures := make([]IntentCandidateCapture, 0, 13)
	hints := make([]IntentDependencyHint, 0, 12)
	for i := 0; i < 13; i++ {
		capture := appendIntentCandidateCapture(
			t, db, fmt.Sprintf("path-%02d.go", i),
			"create", "", fmt.Sprintf("value-%d", i))
		captures = append(captures, capture)
		if i > 0 {
			hints = append(hints, IntentDependencyHint{
				PrerequisiteSeq: captures[i-1].Event.Seq,
				DependentSeq:    capture.Event.Seq,
				Strength:        ai.IntentDependencyHard,
				Kind:            "object_reference",
				Evidence:        "hard path chain",
			})
		}
	}
	result, err := EvaluateIntentCandidates(ctx, db, IntentCandidateEvaluation{
		BranchRef: "refs/heads/main", BranchGeneration: 1,
		Captures: captures, Hints: hints,
		Planner: &intentCandidatePlannerStub{
			err: errors.New("provider unavailable"),
		},
		Preset: config.PresetBalanced, VerificationMode: "structural",
		Materialize: func(
			context.Context,
			[]IntentCandidateCapture,
		) error {
			return errors.New("oversized fallback must not materialize")
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !result.NeedsAttention || len(result.Decisions) != 1 ||
		result.Decisions[0].Publishable ||
		!strings.Contains(strings.Join(
			result.Decisions[0].Assignment.MissingCompanions, " "),
			"12 paths") {
		t.Fatalf("path-capped hard fallback=%+v", result)
	}
}

func TestIntentCandidateEngineFallbackMergesCrossCandidateHardClosure(
	t *testing.T,
) {
	t.Parallel()
	ctx := context.Background()
	db := openIntentCandidateTestDB(t)
	leftFirst := appendIntentCandidateCapture(
		t, db, "left-a.go", "create", "", "left-a")
	leftLinked := appendIntentCandidateCapture(
		t, db, "left-b.go", "create", "", "left-b")
	bridge := appendIntentCandidateCapture(
		t, db, "bridge.go", "create", "", "bridge")
	rightLinked := appendIntentCandidateCapture(
		t, db, "right-a.go", "create", "", "right-a")
	rightLast := appendIntentCandidateCapture(
		t, db, "right-b.go", "create", "", "right-b")
	saveWaitingIntentCandidate(
		t, db, "closure-left", 100, leftFirst, leftLinked)
	saveWaitingIntentCandidate(
		t, db, "closure-right", 110, rightLinked, rightLast)

	result, err := EvaluateIntentCandidates(ctx, db, IntentCandidateEvaluation{
		BranchRef: "refs/heads/main", BranchGeneration: 1,
		Captures: []IntentCandidateCapture{bridge},
		Hints: []IntentDependencyHint{
			{
				PrerequisiteSeq: leftLinked.Event.Seq,
				DependentSeq:    bridge.Event.Seq,
				Strength:        ai.IntentDependencyHard,
				Kind:            "object_reference",
				Evidence:        "left closure",
			},
			{
				PrerequisiteSeq: bridge.Event.Seq,
				DependentSeq:    rightLinked.Event.Seq,
				Strength:        ai.IntentDependencyHard,
				Kind:            "object_reference",
				Evidence:        "right closure",
			},
		},
		Planner: &intentCandidatePlannerStub{
			err: errors.New("provider unavailable"),
		},
		Preset: config.PresetBalanced, VerificationMode: "structural",
		Materialize: func(
			_ context.Context,
			captures []IntentCandidateCapture,
		) error {
			if len(captures) != 5 {
				return fmt.Errorf(
					"materialized cross-candidate closure=%v",
					intentCandidateCaptureSeqs(captures))
			}
			return nil
		},
	})
	if err != nil {
		t.Fatalf("EvaluateIntentCandidates hard closure: %v", err)
	}
	if len(result.Decisions) != 1 ||
		result.Decisions[0].Candidate.ID != "closure-left" ||
		!result.Decisions[0].Publishable ||
		len(result.Decisions[0].Candidate.Events) != 5 {
		t.Fatalf("cross-candidate hard closure=%+v", result)
	}
}

func TestIntentCandidateEngineHoldsOverCapHardContinuationWithoutErrors(
	t *testing.T,
) {
	for _, testCase := range []struct {
		name   string
		native bool
	}{
		{name: "fallback"},
		{name: "native", native: true},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			ctx := context.Background()
			db := openIntentCandidateTestDB(t)
			left := make([]IntentCandidateCapture, 0, 128)
			for i := 0; i < 128; i++ {
				left = append(left, appendIntentCandidateCapture(
					t, db, fmt.Sprintf("left/%03d.go", i),
					"create", "", fmt.Sprintf("left-%d", i)))
			}
			bridge := appendIntentCandidateCapture(
				t, db, "bridge.go", "create", "", "bridge")
			right := make([]IntentCandidateCapture, 0, 128)
			for i := 0; i < 128; i++ {
				right = append(right, appendIntentCandidateCapture(
					t, db, fmt.Sprintf("right/%03d.go", i),
					"create", "", fmt.Sprintf("right-%d", i)))
			}
			saveWaitingIntentCandidate(
				t, db, "cap-left", 100, left...)
			saveWaitingIntentCandidate(
				t, db, "cap-right", 110, right...)

			planner := &intentCandidatePlannerStub{
				err: errors.New("provider unavailable"),
			}
			if testCase.native {
				planner.err = nil
				planner.plan = ai.IntentPlanV2{
					ProtocolVersion: ai.IntentPlannerProtocolV2,
					Candidates: []ai.IntentCandidateAssignment{{
						CandidateID:    "cap-left",
						SelectedSeqs:   []int64{bridge.Event.Seq},
						Purpose:        "complete the hard dependency closure",
						Readiness:      ai.IntentCandidateReady,
						Subject:        "Complete hard dependency closure",
						GroupingReason: "bridge joins durable candidates",
					}},
				}
			}
			materializeCalls := 0
			input := IntentCandidateEvaluation{
				BranchRef: "refs/heads/main", BranchGeneration: 1,
				Captures: []IntentCandidateCapture{bridge},
				Hints: []IntentDependencyHint{
					{
						PrerequisiteSeq: left[len(left)-1].Event.Seq,
						DependentSeq:    bridge.Event.Seq,
						Strength:        ai.IntentDependencyHard,
						Kind:            "object_reference",
						Evidence:        "left cap bridge",
					},
					{
						PrerequisiteSeq: bridge.Event.Seq,
						DependentSeq:    right[0].Event.Seq,
						Strength:        ai.IntentDependencyHard,
						Kind:            "object_reference",
						Evidence:        "right cap bridge",
					},
				},
				Planner: planner, Preset: config.PresetBalanced,
				VerificationMode: "structural",
				Materialize: func(
					context.Context,
					[]IntentCandidateCapture,
				) error {
					materializeCalls++
					return errors.New("over-cap closure must not materialize")
				},
			}
			for attempt := 0; attempt < 2; attempt++ {
				result, err := EvaluateIntentCandidates(ctx, db, input)
				if err != nil {
					t.Fatalf("attempt %d returned replay-loop error: %v",
						attempt+1, err)
				}
				if !result.NeedsAttention ||
					len(result.Decisions) != 1 ||
					result.Decisions[0].Publishable ||
					result.Decisions[0].Assignment.Readiness !=
						ai.IntentCandidateWait ||
					!strings.Contains(strings.Join(
						result.Decisions[0].Assignment.MissingCompanions,
						" "), "256-capture") {
					t.Fatalf("attempt %d over-cap hold=%+v",
						attempt+1, result)
				}
			}
			if materializeCalls != 0 {
				t.Fatalf("over-cap materialization calls=%d",
					materializeCalls)
			}
			for candidateID, wantEvents := range map[string]int{
				"cap-left": 128, "cap-right": 128,
			} {
				candidate, ok, err := state.IntentCandidateByID(
					ctx, db, candidateID)
				if err != nil || !ok ||
					candidate.Status != state.IntentCandidateWaiting ||
					len(candidate.Events) != wantEvents {
					t.Fatalf("candidate %s=%+v ok=%v err=%v",
						candidateID, candidate, ok, err)
				}
			}
			lineage, err := state.IntentCandidateLineageForTarget(
				ctx, db, "refs/heads/main", 1, "cap-left", 10)
			if err != nil || len(lineage) != 0 {
				t.Fatalf("over-cap lineage=%+v err=%v", lineage, err)
			}
		})
	}
}

func testIntentCandidateFallbackMergesThroughPersistedCandidate(
	t *testing.T,
	preset config.PresetName,
) {
	t.Helper()
	ctx := context.Background()
	db := openIntentCandidateTestDB(t)
	firstA := appendIntentCandidateCapture(
		t, db, "a.go", "create", "", "a1")
	firstB := appendIntentCandidateCapture(
		t, db, "b.go", "create", "", "b1")
	secondA := appendIntentCandidateCapture(
		t, db, "a.go", "modify", "a1", "a2")
	secondB := appendIntentCandidateCapture(
		t, db, "b.go", "modify", "b1", "b2")
	const candidateID = "persisted-bridge"
	if err := state.SaveIntentCandidate(ctx, db, state.IntentCandidate{
		ID: candidateID, BranchRef: "refs/heads/main", BranchGeneration: 1,
		Status: state.IntentCandidateWaiting, Purpose: "finish related changes",
		Readiness: state.IntentReadinessWait,
		Events: []state.IntentCandidateEvent{
			{EventSeq: secondA.Event.Seq, EventRole: "code"},
			{EventSeq: secondB.Event.Seq, EventRole: "code"},
		},
	}); err != nil {
		t.Fatal(err)
	}

	result, err := EvaluateIntentCandidates(ctx, db, IntentCandidateEvaluation{
		BranchRef: "refs/heads/main", BranchGeneration: 1,
		Captures: []IntentCandidateCapture{firstA, firstB},
		Hints: []IntentDependencyHint{{
			PrerequisiteSeq: secondA.Event.Seq,
			DependentSeq:    secondB.Event.Seq,
			Strength:        ai.IntentDependencySoft,
			Kind:            "import_reference",
			Evidence:        "persisted bridge",
		}},
		Planner: &intentCandidatePlannerStub{
			err: errors.New("provider unavailable"),
		},
		Preset: preset,
		Materialize: func(
			context.Context,
			[]IntentCandidateCapture,
		) error {
			return nil
		},
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
	if len(result.Decisions) != 1 ||
		result.Decisions[0].Candidate.ID != candidateID ||
		len(result.Decisions[0].Candidate.Events) != 4 ||
		!result.Decisions[0].Publishable {
		t.Fatalf("merged fallback=%+v", result)
	}
}

func TestIntentCandidateEngineStructuralVerificationUsesAtomicityGates(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	db := openIntentCandidateTestDB(t)
	capture := appendIntentCandidateCapture(
		t, db, "same.go", "create", "", "first")
	planner := &intentCandidatePlannerStub{plan: ai.IntentPlanV2{
		ProtocolVersion: ai.IntentPlannerProtocolV2,
		Candidates: []ai.IntentCandidateAssignment{{
			CandidateID: "structural", SelectedSeqs: []int64{capture.Event.Seq},
			Purpose:        "apply one structurally valid change",
			Readiness:      ai.IntentCandidateReady,
			Subject:        "Apply structurally valid change",
			GroupingReason: "single capture is independently materializable",
		}},
	}}

	result, err := EvaluateIntentCandidates(ctx, db, IntentCandidateEvaluation{
		BranchRef: "refs/heads/main", BranchGeneration: 1,
		Captures: []IntentCandidateCapture{capture},
		Planner:  planner, Preset: config.PresetBalanced,
		VerificationMode: "structural",
		Materialize: func(
			context.Context,
			[]IntentCandidateCapture,
		) error {
			return nil
		},
	})
	if err != nil {
		t.Fatalf("EvaluateIntentCandidates: %v", err)
	}
	if len(result.Decisions) != 1 || !result.Decisions[0].Publishable {
		t.Fatalf("structural decision=%+v", result)
	}
	for _, gate := range result.Decisions[0].Atomicity.Gates {
		if gate.Gate == ai.IntentAtomicityVerification &&
			gate.Status != ai.IntentAtomicityNotRequired {
			t.Fatalf("verification gate=%+v", gate)
		}
	}
}

func TestIntentCandidateEngineRejectsUnknownVerificationMode(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	db := openIntentCandidateTestDB(t)
	capture := appendIntentCandidateCapture(
		t, db, "same.go", "create", "", "first")
	_, err := EvaluateIntentCandidates(ctx, db, IntentCandidateEvaluation{
		BranchRef: "refs/heads/main", BranchGeneration: 1,
		Captures: []IntentCandidateCapture{capture},
		Planner: &intentCandidatePlannerStub{
			err: errors.New("provider unavailable"),
		},
		Preset:           config.PresetBalanced,
		VerificationMode: "unexpected",
	})
	if err == nil || !strings.Contains(err.Error(),
		`unsupported verification mode "unexpected"`) {
		t.Fatalf("unknown verification mode error=%v", err)
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

func TestRuntimeIntentDependencyHintsFindOutputArchiveReferencesInEitherOrder(t *testing.T) {
	t.Parallel()
	archive := intentCandidateCaptureFixture(
		1, "output/gitlab-to-teams.zip", "create", "", "archive")
	archive.CapturedDiff = "Binary files differ\n"
	verifier := intentCandidateCaptureFixture(
		2, "scripts/verify-release.sh", "create", "", "verifier")
	verifier.CapturedDiff = "+unzip -t output/gitlab-to-teams.zip\n"
	for _, captures := range [][]IntentCandidateCapture{
		{archive, verifier}, {verifier, archive},
	} {
		hints := runtimeIntentDependencyHints(captures)
		found := false
		for _, hint := range hints {
			if hint.Kind == "generated_artifact_reference" &&
				hint.PrerequisiteSeq == verifier.Event.Seq &&
				hint.DependentSeq == archive.Event.Seq {
				found = true
			}
		}
		if !found {
			t.Fatalf("generated artifact reference missing: %+v", hints)
		}
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

func TestAdvanceTerminalIntentCandidateIDsUsesStableSuccessor(t *testing.T) {
	ctx := context.Background()
	db := openIntentCandidateTestDB(t)
	capture := appendIntentCandidateCapture(
		t, db, "service.go", "modify", "before", "after")
	terminal := state.IntentCandidate{
		ID: "reused-planner-id", BranchRef: "refs/heads/main",
		BranchGeneration: 1, Status: state.IntentCandidateReady,
		Readiness: state.IntentReadinessReady,
		Events: []state.IntentCandidateEvent{{
			EventSeq: capture.Event.Seq, EventRole: "change",
		}},
	}
	if err := state.SaveIntentCandidate(ctx, db, terminal); err != nil {
		t.Fatal(err)
	}
	if _, err := db.SQL().ExecContext(ctx, `
UPDATE intent_candidate_events SET membership_state='superseded'
WHERE candidate_id=?`, terminal.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.SQL().ExecContext(ctx,
		`UPDATE intent_candidates SET status='superseded' WHERE id=?`,
		terminal.ID); err != nil {
		t.Fatal(err)
	}
	plan := ai.IntentPlanV2{
		ProtocolVersion: ai.IntentPlannerProtocolV2,
		Candidates: []ai.IntentCandidateAssignment{{
			CandidateID: terminal.ID, SelectedSeqs: []int64{capture.Event.Seq},
			DependsOnCandidates: []string{terminal.ID},
		}},
	}
	first, _, err := advanceTerminalIntentCandidateIDs(
		ctx, db, "refs/heads/main", 1, plan, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got := first.Candidates[0].CandidateID; got == terminal.ID || !strings.HasPrefix(got, "intent-successor-") {
		t.Fatalf("successor candidate id=%q", got)
	}
	if got, want := first.Candidates[0].DependsOnCandidates[0],
		first.Candidates[0].CandidateID; got != want {
		t.Fatalf("remapped dependency=%q want=%q", got, want)
	}
	second, _, err := advanceTerminalIntentCandidateIDs(
		ctx, db, "refs/heads/main", 1, plan, nil)
	if err != nil || second.Candidates[0].CandidateID != first.Candidates[0].CandidateID {
		t.Fatalf("stable successor=%q first=%q err=%v",
			second.Candidates[0].CandidateID, first.Candidates[0].CandidateID, err)
	}
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

func setNextIntentCandidateSeq(t *testing.T, db *state.DB, next int64) {
	t.Helper()
	if next <= 0 {
		t.Fatalf("invalid next capture seq %d", next)
	}
	ctx := context.Background()
	if _, err := db.SQL().ExecContext(
		ctx, `DELETE FROM sqlite_sequence WHERE name='capture_events'`,
	); err != nil {
		t.Fatalf("clear capture sequence: %v", err)
	}
	if _, err := db.SQL().ExecContext(ctx, `
INSERT INTO sqlite_sequence(name, seq) VALUES('capture_events', ?)`,
		next-1,
	); err != nil {
		t.Fatalf("set capture sequence: %v", err)
	}
}

func saveSoftPublishedIntentCandidate(
	t *testing.T,
	db *state.DB,
	id string,
	capture IntentCandidateCapture,
	commitOID string,
	updated float64,
) {
	t.Helper()
	deadline := updated + 600
	if err := state.SaveIntentCandidate(
		context.Background(), db, state.IntentCandidate{
			ID: id, BranchRef: "refs/heads/main", BranchGeneration: 1,
			Status:    state.IntentCandidateSoftPublished,
			Purpose:   "soft-published candidate",
			CreatedTS: updated, UpdatedTS: updated,
			Readiness: state.IntentReadinessReady,
			AtomicityStatus: sql.NullString{
				String: string(ai.IntentAtomicityPassed), Valid: true,
			},
			SoftPublicationDeadline: sql.NullFloat64{
				Float64: deadline, Valid: true,
			},
			PublishedCommitOID: sql.NullString{
				String: commitOID, Valid: true,
			},
			Events: []state.IntentCandidateEvent{{
				EventSeq:  capture.Event.Seq,
				EventRole: intentCaptureRole(capture),
			}},
		},
	); err != nil {
		t.Fatalf("save soft candidate %s: %v", id, err)
	}
}

func saveWaitingIntentCandidate(
	t *testing.T,
	db *state.DB,
	id string,
	updated float64,
	captures ...IntentCandidateCapture,
) {
	t.Helper()
	events := make([]state.IntentCandidateEvent, 0, len(captures))
	for _, capture := range captures {
		events = append(events, state.IntentCandidateEvent{
			EventSeq:  capture.Event.Seq,
			EventRole: intentCaptureRole(capture),
		})
	}
	if err := state.SaveIntentCandidate(
		context.Background(), db, state.IntentCandidate{
			ID: id, BranchRef: "refs/heads/main", BranchGeneration: 1,
			Status:    state.IntentCandidateWaiting,
			Purpose:   "persist hard closure",
			CreatedTS: updated, UpdatedTS: updated,
			Readiness: state.IntentReadinessWait, Events: events,
		},
	); err != nil {
		t.Fatalf("save waiting candidate %s: %v", id, err)
	}
}

func intentCandidateCaptureSeqs(
	captures []IntentCandidateCapture,
) []int64 {
	out := make([]int64, 0, len(captures))
	for _, capture := range captures {
		out = append(out, capture.Event.Seq)
	}
	return out
}

func intentCandidateEventSeqs(
	events []state.IntentCandidateEvent,
) []int64 {
	out := make([]int64, 0, len(events))
	for _, event := range events {
		out = append(out, event.EventSeq)
	}
	return out
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
