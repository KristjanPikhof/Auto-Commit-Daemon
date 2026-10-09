package daemon

import (
	"context"
	"reflect"
	"testing"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/ai"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/config"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

func TestIntentRedundantMutableContextSelectionsNormalizeWithoutProviderRetry(t *testing.T) {
	t.Parallel()
	f := newCaptureFixture(t)
	ctx := context.Background()
	code := func(limit string) string {
		return "package app\nfunc RecordedLimit() int { return " + limit + " }\n"
	}
	blob := func(contents string) string {
		oid, err := git.HashObjectStdin(ctx, f.dir, []byte(contents))
		if err != nil {
			t.Fatal(err)
		}
		return oid
	}
	head := mustCommitPath(t, f.dir, "limit.go", code("256"), "Seed recorded limit")
	retained := appendIntentCandidateCapture(t, f.db, "limit.go", "modify", blob(code("256")), blob(code("128")))
	saveWaitingIntentCandidate(t, f.db, "limit-owner", 1, retained)
	fresh := appendIntentCandidateCapture(t, f.db, "limit.go", "modify", retained.Ops[0].AfterOID.String, blob(code("64")))
	planner := &intentCandidatePlannerStub{plan: ai.IntentPlanV2{ProtocolVersion: ai.IntentPlannerProtocolV2,
		Candidates: []ai.IntentCandidateAssignment{{CandidateID: "limit-owner", SelectedSeqs: []int64{retained.Event.Seq, fresh.Event.Seq},
			Purpose: "Tighten recorded limits", Readiness: ai.IntentCandidateReady, Subject: "Tighten recorded limits",
			Body: "- Bound the recorded limit to64", GroupingReason: "The recorded edits complete one limit change"}}}}
	original := cloneIntentPlanV2(planner.plan)
	result, err := EvaluateIntentCandidates(ctx, f.db, IntentCandidateEvaluation{
		RepoPath: f.dir, BranchRef: f.cctx.BranchRef, BranchGeneration: 1, IncludeDiffs: true,
		Captures: []IntentCandidateCapture{fresh}, LatestCommit: &ai.CommitSummary{OID: head},
		Planner: planner, Preset: config.PresetBalanced, VerificationMode: "structural",
		Materialize: func(context.Context, []IntentCandidateCapture) error { return nil },
	})
	if err != nil || planner.calls != 1 || result.Fallback != "repaired_redundant_context_selections" ||
		len(result.Decisions) != 1 || !result.Decisions[0].Publishable {
		t.Fatalf("same-owner redundancy stranded a complete goal: %+v calls=%d err=%v", result, planner.calls, err)
	}
	decision := result.Decisions[0]
	if !reflect.DeepEqual(decision.Assignment.SelectedSeqs, []int64{fresh.Event.Seq}) || len(decision.Candidate.Events) != 2 ||
		decision.Candidate.Events[0].EventSeq != retained.Event.Seq || !reflect.DeepEqual(planner.plan, original) {
		t.Fatalf("normalization dropped retained work or mutated the response: decision=%+v plan=%+v", decision, planner.plan)
	}
	for _, test := range []struct {
		name   string
		change func(*ai.IntentPlanRequestV2, *ai.IntentPlanV2)
	}{
		{"published context", func(req *ai.IntentPlanRequestV2, _ *ai.IntentPlanV2) {
			req.Candidates[0].Status = state.IntentCandidatePublished
		}},
		{"soft-published context", func(req *ai.IntentPlanRequestV2, _ *ai.IntentPlanV2) {
			req.Candidates[0].Status = state.IntentCandidateSoftPublished
		}},
		{"blocked context", func(req *ai.IntentPlanRequestV2, _ *ai.IntentPlanV2) {
			req.Candidates[0].Status = state.IntentCandidateBlocked
		}},
		{"ambiguous ownership", func(req *ai.IntentPlanRequestV2, _ *ai.IntentPlanV2) {
			other := req.Candidates[0]
			other.CandidateID = "another-owner"
			req.Candidates = append(req.Candidates, other)
		}},
		{"unknown capture", func(_ *ai.IntentPlanRequestV2, plan *ai.IntentPlanV2) { plan.Candidates[0].SelectedSeqs[0] = 999999 }},
		{"another owner", func(_ *ai.IntentPlanRequestV2, plan *ai.IntentPlanV2) {
			plan.Candidates[0].CandidateID = "another-owner"
		}},
		{"repartitioned context", func(_ *ai.IntentPlanRequestV2, plan *ai.IntentPlanV2) {
			plan.Candidates = append(plan.Candidates, ai.IntentCandidateAssignment{CandidateID: "another-owner", SelectedSeqs: []int64{retained.Event.Seq}})
		}},
		{"missing offered capture", func(req *ai.IntentPlanRequestV2, _ *ai.IntentPlanV2) {
			req.OfferedCaptures = append(req.OfferedCaptures, ai.OfferedCapture{Seq: 999999, Path: "other.go"})
		}},
		{"duplicate offered request", func(req *ai.IntentPlanRequestV2, _ *ai.IntentPlanV2) {
			req.OfferedCaptures = append(req.OfferedCaptures, req.OfferedCaptures[0])
		}},
		{"duplicate offered capture", func(_ *ai.IntentPlanRequestV2, plan *ai.IntentPlanV2) {
			plan.Candidates[0].SelectedSeqs = append(plan.Candidates[0].SelectedSeqs, fresh.Event.Seq)
		}},
		{"context-only update", func(_ *ai.IntentPlanRequestV2, plan *ai.IntentPlanV2) {
			plan.Candidates[0].SelectedSeqs = []int64{retained.Event.Seq}
			plan.Candidates = append(plan.Candidates, ai.IntentCandidateAssignment{CandidateID: "new-owner", SelectedSeqs: []int64{fresh.Event.Seq}})
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			req := planner.req
			req.Candidates = append([]ai.IntentCandidateSummary(nil), req.Candidates...)
			req.OfferedCaptures = append([]ai.OfferedCapture(nil), req.OfferedCaptures...)
			plan := cloneIntentPlanV2(original)
			test.change(&req, &plan)
			before := cloneIntentPlanV2(plan)
			repaired, changed := repairRedundantIntentCandidateContext(req, plan)
			if changed || !reflect.DeepEqual(repaired, before) || !reflect.DeepEqual(plan, before) {
				t.Fatalf("unsafe membership was silently normalized: changed=%t before=%+v after=%+v", changed, before, repaired)
			}
		})
	}
}
