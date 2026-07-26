package daemon

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
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
	passVerification := func(context.Context, []IntentCandidateCapture) error { return nil }
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
	planner.plan = ai.IntentPlanV2{
		ProtocolVersion: ai.IntentPlannerProtocolV2,
		Candidates: []ai.IntentCandidateAssignment{{
			CandidateID: "candidate-a", SelectedSeqs: []int64{testA.Event.Seq},
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
	if len(second.Decisions) != 1 || second.Decisions[0].Candidate.ID != "candidate-a" {
		t.Fatalf("second decisions=%+v", second.Decisions)
	}
	got, ok, err := state.IntentCandidateByID(ctx, db, "candidate-a")
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
		Verify:      func(context.Context, []IntentCandidateCapture) error { return nil },
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
		Verify: func(context.Context, []IntentCandidateCapture) error {
			verifyCalls++
			return nil
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
				Verify: func(context.Context, []IntentCandidateCapture) error {
					verifyCalls++
					return nil
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
		Verify: func(context.Context, []IntentCandidateCapture) error {
			return errors.New("fast check failed")
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
