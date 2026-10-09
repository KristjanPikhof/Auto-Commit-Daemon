package daemon

import (
	"context"
	"database/sql"
	"encoding/json"
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

func TestIntentPublishedContinuationCacheReplansOnlyUnresolvedGoal(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	db, err := state.Open(ctx, filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	req, err := ai.NewIntentPlanRequestV2(ai.IntentPlanRequestV2Options{
		IncludeCapturedDiffs: true,
		OfferedCaptures: []ai.OfferedCapture{
			{Seq: 3, Path: "limit.go", CapturedDiff: "-func RecordedLimit() int { return 64 }\n+func RecordedLimit() int { return 32 }\n"},
			{Seq: 4, Path: "export-guide.md", CapturedDiff: "+# Export task recordings\n+Use the task export command to save recordings for later review.\n"},
		},
		Candidates: []ai.IntentCandidateSummary{
			{CandidateID: "published-limit", Status: state.IntentCandidatePublished, Ready: true, SelectedSeqs: []int64{1}, Purpose: "Bound recorded limits"},
			{CandidateID: "mutable-limit", Status: state.IntentCandidateWaiting, SelectedSeqs: []int64{2}, Purpose: "Tighten recorded limits",
				CapturedEvidence: []ai.OfferedCapture{{Seq: 2, Path: "limit.go", CapturedDiff: "-func RecordedLimit() int { return 128 }\n+func RecordedLimit() int { return 64 }\n"}}},
		},
		Dependencies: []ai.IntentCaptureDependency{
			{FromSeq: 1, ToSeq: 2, Strength: ai.IntentDependencyHard, Kind: "same_path_order"},
			{FromSeq: 2, ToSeq: 3, Strength: ai.IntentDependencyHard, Kind: "same_path_order"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	input := IntentCandidateEvaluation{BranchRef: "refs/heads/main", BranchGeneration: 1,
		Preset: config.PresetBalanced, Now: time.Now(), ProviderBudget: time.Minute}
	preflight, _, err := preflightIntentCandidatePlan(ctx, req, input.Preset, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	fingerprintRequest := req
	fingerprintRequest.BaselineCandidates = preflight.BaselineCandidates
	run, err := newIntentPlanRun(fingerprintRequest, input, 3)
	if err != nil {
		t.Fatal(err)
	}
	run, err = state.EnsureIntentPlanRun(ctx, db, run)
	if err != nil {
		t.Fatal(err)
	}
	valid := ai.IntentCandidateAssignment{CandidateID: "export-guide", SelectedSeqs: []int64{4},
		Purpose: "Document task recording exports", Readiness: ai.IntentCandidateReady,
		Subject: "Document task recording exports", Body: "- Explain how task recordings can be saved for later review", GroupingReason: "The guide completes the export instructions"}
	run.Completed = true
	run.AttemptCount = 1
	run.ProviderDeadlineTS = intentPlannerHealthTimestamp(input.Now.Add(-time.Minute))
	if err := storeResolvedIntentPlanRun(&run, ai.IntentPlanV2{ProtocolVersion: ai.IntentPlannerProtocolV2,
		Candidates: []ai.IntentCandidateAssignment{valid, {CandidateID: "published-limit", SelectedSeqs: []int64{3},
			Purpose: "Tighten recorded limits", Readiness: ai.IntentCandidateReady, Subject: "Tighten recorded limits",
			GroupingReason: "The capture narrows the recorded limit"}}},
		[]intentCandidateContinuation{{TargetID: "published-limit", SourceIDs: []string{"mutable-limit"}}}); err != nil {
		t.Fatal(err)
	}
	if err := state.UpdateIntentPlanRun(ctx, db, run); err != nil {
		t.Fatal(err)
	}
	planner := &intentCandidatePlannerStub{plan: ai.IntentPlanV2{ProtocolVersion: ai.IntentPlannerProtocolV2,
		Candidates: []ai.IntentCandidateAssignment{{CandidateID: "mutable-limit", SelectedSeqs: []int64{3},
			Purpose: "Tighten recorded limits", Readiness: ai.IntentCandidateReady, Subject: "Tighten recorded limits",
			Body: "- Bound recorded limits to32", DependsOnCandidates: []string{"published-limit"}, GroupingReason: "The capture narrows the recorded limit"}}}}
	plan, fallback, _, _, attention, _, corrected, err := chooseIntentCandidatePlan(
		ctx, req, planner, nil, 2, input.Preset, nil, db, input)
	if err != nil || fallback != "" || attention || planner.calls != 1 || corrected.AttemptCount != 2 || corrected.AttemptLimit != 3 ||
		corrected.ResolutionMode.String != "partial_replan" || corrected.ProviderDeadlineTS <= intentPlannerHealthTimestamp(input.Now) {
		t.Fatalf("obsolete cache did not resume its remaining semantic budget: plan=%+v run=%+v fallback=%s calls=%d attention=%t err=%v", plan, corrected, fallback, planner.calls, attention, err)
	}
	if len(planner.req.OfferedCaptures) != 1 || planner.req.OfferedCaptures[0].Seq != 3 ||
		len(plan.Candidates) != 2 || !reflect.DeepEqual(plan.Candidates[0], valid) {
		t.Fatalf("valid goal was replanned or lost: request=%+v plan=%+v", planner.req.OfferedCaptures, plan)
	}
	if err := ValidateIntentGoalPlan(req, plan); err != nil {
		t.Fatalf("corrected ready goals cannot pass publication planning: %v", err)
	}
	_, _, _, _, _, _, reused, err := chooseIntentCandidatePlan(ctx, req, planner, nil, 2, input.Preset, nil, db, input)
	if err != nil || planner.calls != 1 || reused.AttemptCount != corrected.AttemptCount || reused.ProviderDeadlineTS != corrected.ProviderDeadlineTS {
		t.Fatalf("same corrected evidence reset its budget or recalled provider: calls=%d run=%+v err=%v", planner.calls, reused, err)
	}
	if _, found, err := state.MetaGet(ctx, db, MetaKeyIntentSemanticRetry); err != nil || found {
		t.Fatalf("local cache correction invented an hourly wait: found=%t err=%v", found, err)
	}
}

func TestIntentPublishedBaselineDoesNotJoinMutableContinuation(t *testing.T) {
	t.Parallel()
	f := newCaptureFixture(t)
	ctx := context.Background()
	blob := func(contents string) string {
		oid, err := git.HashObjectStdin(ctx, f.dir, []byte(contents))
		if err != nil {
			t.Fatal(err)
		}
		return oid
	}
	code := func(limit string) string {
		return "package app\nfunc RecordedLimit() int { return " + limit + " }\n"
	}
	testCode := func(limit string) string {
		return "package app\nimport \"testing\"\nfunc TestRecordedLimit(t *testing.T) { if RecordedLimit() != " + limit + " { t.Fatal(\"wrong limit\") } }\n"
	}
	baseline := appendIntentCandidateCapture(t, f.db, "limit.go", "modify", blob(code("256")), blob(code("128")))
	mutableCode := appendIntentCandidateCapture(t, f.db, "limit.go", "modify", baseline.Ops[0].AfterOID.String, blob(code("64")))
	mutableTest := appendIntentCandidateCapture(t, f.db, "limit_test.go", "create", "", blob(testCode("64")))
	saveWaitingIntentCandidate(t, f.db, "mutable-limit", 1, baseline, mutableCode, mutableTest)
	head := mustCommitPath(t, f.dir, "limit.go", code("128"), "Bound recorded limits")
	if err := state.MarkEventPublished(ctx, f.db, baseline.Event.Seq, state.EventStatePublished,
		sql.NullString{String: head, Valid: true}, sql.NullString{}, sql.NullString{}, 2); err != nil {
		t.Fatal(err)
	}
	if err := state.SaveIntentCandidate(ctx, f.db, state.IntentCandidate{
		ID: "published-limit", BranchRef: f.cctx.BranchRef, BranchGeneration: 1,
		Status: state.IntentCandidatePublished, Readiness: state.IntentReadinessReady,
		Purpose: "Bound recorded limits", PublishedCommitOID: sql.NullString{String: head, Valid: true},
		Events: []state.IntentCandidateEvent{{EventSeq: baseline.Event.Seq, EventRole: "code"}},
	}); err != nil {
		t.Fatal(err)
	}
	before, ok, err := state.IntentCandidateByID(ctx, f.db, "published-limit")
	if err != nil || !ok {
		t.Fatal(err)
	}
	freshCode := appendIntentCandidateCapture(t, f.db, "limit.go", "modify", mutableCode.Ops[0].AfterOID.String, blob(code("32")))
	freshTest := appendIntentCandidateCapture(t, f.db, "limit_test.go", "modify", mutableTest.Ops[0].AfterOID.String, blob(testCode("32")))
	planner := &intentCandidatePlannerStub{plan: ai.IntentPlanV2{ProtocolVersion: ai.IntentPlannerProtocolV2,
		Candidates: []ai.IntentCandidateAssignment{{CandidateID: "mutable-limit", SelectedSeqs: []int64{freshCode.Event.Seq, freshTest.Event.Seq},
			Purpose: "Tighten recorded limits with matching regression coverage", Readiness: ai.IntentCandidateReady,
			Subject: "Tighten recorded limits", Body: "- Keep the recorded limit and its regression expectation consistent",
			DependsOnCandidates: []string{"published-limit"}, GroupingReason: "The implementation and regression complete one limit change"}}}}
	result, err := EvaluateIntentCandidates(ctx, f.db, IntentCandidateEvaluation{
		RepoPath: f.dir, BranchRef: f.cctx.BranchRef, BranchGeneration: 1, IncludeDiffs: true,
		Captures: []IntentCandidateCapture{freshCode, freshTest}, LatestCommit: &ai.CommitSummary{OID: head},
		Planner: planner, Preset: config.PresetBalanced, VerificationMode: "structural",
		Materialize: func(context.Context, []IntentCandidateCapture) error { return nil },
	})
	if err != nil || len(result.Decisions) != 1 || !result.Decisions[0].Publishable || planner.calls != 1 {
		t.Fatalf("published baseline trapped a mutable continuation: %+v calls=%d err=%v", result, planner.calls, err)
	}
	decision := result.Decisions[0]
	if decision.Candidate.ID != "mutable-limit" || !reflect.DeepEqual(decision.Assignment.DependsOnCandidates, []string{"published-limit"}) {
		t.Fatalf("baseline was absorbed instead of remaining a satisfied prerequisite: %+v", decision)
	}
	if len(decision.Candidate.Events) != 4 || len(planner.req.OfferedCaptures) != 2 {
		t.Fatalf("mutable lineage or offered window changed: candidate=%+v request=%+v", decision.Candidate, planner.req.OfferedCaptures)
	}
	for _, capture := range planner.req.OfferedCaptures {
		if capture.Seq == baseline.Event.Seq {
			t.Fatal("published baseline was reoffered")
		}
	}
	after, ok, err := state.IntentCandidateByID(ctx, f.db, "published-limit")
	if err != nil || !ok || !reflect.DeepEqual(before, after) {
		t.Fatalf("immutable published provenance changed: before=%+v after=%+v err=%v", before, after, err)
	}
	currentHead, err := git.RevParse(ctx, f.dir, "HEAD")
	if err != nil || currentHead != head {
		t.Fatal("candidate evaluation rewrote the published baseline")
	}
	// An older worker stored the contaminated continuation as completed.
	// It must lose cache authority so automatic planning can rebuild it.
	contaminated := resolvedIntentPlanRun{
		Plan:          planner.plan,
		Continuations: []intentCandidateContinuation{{TargetID: "published-limit", SourceIDs: []string{"mutable-limit"}}},
	}
	raw, err := json.Marshal(contaminated)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := loadResolvedIntentPlanRun(planner.req, string(raw)); err == nil || !strings.Contains(err.Error(), "published baseline") {
		t.Fatalf("obsolete published merge remained authoritative: %v", err)
	}
	contaminated.Continuations = []intentCandidateContinuation{{TargetID: "mutable-limit", SourceIDs: []string{"published-limit"}}}
	raw, err = json.Marshal(contaminated)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := loadResolvedIntentPlanRun(planner.req, string(raw)); err == nil || !strings.Contains(err.Error(), "published baseline") {
		t.Fatalf("obsolete published source remained authoritative: %v", err)
	}
}

func TestIntentPublishedFormerCompanionsUseLatestProvenHead(t *testing.T) {
	t.Parallel()
	f := newCaptureFixture(t)
	ctx := context.Background()
	parent := f.cctx.BaseHead
	commit := func(contents, mode string) (string, string) {
		t.Helper()
		oid, err := git.HashObjectStdin(ctx, f.dir, []byte(contents))
		if err != nil {
			t.Fatal(err)
		}
		tree, err := git.Mktree(ctx, f.dir, []git.MktreeEntry{{Mode: mode, Type: "blob", OID: oid, Path: "helper.go"}})
		if err != nil {
			t.Fatal(err)
		}
		head, err := git.CommitTree(ctx, f.dir, tree, "Preserve helper behavior", parent)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := git.Run(ctx, git.RunOpts{Dir: f.dir}, "update-ref", "refs/heads/main", head, parent); err != nil {
			t.Fatal(err)
		}
		parent = head
		return oid, head
	}
	publish := func(id string, capture IntentCandidateCapture, head string) {
		t.Helper()
		if err := state.MarkEventPublished(ctx, f.db, capture.Event.Seq, state.EventStatePublished,
			sql.NullString{String: head, Valid: true}, sql.NullString{}, sql.NullString{}, 2); err != nil {
			t.Fatal(err)
		}
		if err := state.SaveIntentCandidate(ctx, f.db, state.IntentCandidate{
			ID: id, BranchRef: f.cctx.BranchRef, BranchGeneration: 1,
			Status: state.IntentCandidatePublished, Purpose: "Preserve helper behavior", Readiness: state.IntentReadinessReady,
			PublishedCommitOID: sql.NullString{String: head, Valid: true},
			Events:             []state.IntentCandidateEvent{{EventSeq: capture.Event.Seq, EventRole: "code"}},
		}); err != nil {
			t.Fatal(err)
		}
	}
	oldOID, oldHead := commit("package example\nfunc CompleteHelper() { _ = 1 }\n", "100644")
	old := appendIntentCandidateCapture(t, f.db, "helper.go", "create", "", oldOID)
	test := appendIntentCandidateCapture(t, f.db, "helper_test.go", "create", "", "test-fixture")
	saveWaitingIntentCandidate(t, f.db, "waiting-test", 1, old, test)
	publish("old-helper", old, oldHead)
	latestOID, latestHead := commit("package example\nfunc CompleteHelper() { _ = 2 }\n", "100644")
	latest := appendIntentCandidateCapture(t, f.db, "helper.go", "modify", oldOID, latestOID)
	publish("latest-helper", latest, latestHead)
	existing, err := state.IntentCandidatesForPair(ctx, f.db, f.cctx.BranchRef, 1, 128)
	if err != nil || len(existing) != 1 {
		t.Fatalf("nonterminal context=%+v err=%v", existing, err)
	}
	input := IntentCandidateEvaluation{RepoPath: f.dir, BranchRef: f.cctx.BranchRef, BranchGeneration: 1,
		IncludeDiffs: true, Captures: []IntentCandidateCapture{test}, LatestCommit: &ai.CommitSummary{OID: latestHead}}
	for _, oid := range []string{latestHead, aiCommitSummary(git.CommitSummary{ShortOID: latestHead[:12]}).OID} {
		copy := input
		copy.LatestCommit = &ai.CommitSummary{OID: oid}
		loaded, err := loadPublishedIntentFormerCompanions(ctx, f.db, &copy, existing)
		if err != nil || len(loaded) != 2 || loaded[1].ID != "latest-helper" || loaded[1].Status != state.IntentCandidatePublished {
			t.Fatalf("latest proven helper missing for %s or stale helper reused: %+v err=%v", oid, loaded, err)
		}
		if copy.LatestCommit.OID != oid {
			t.Fatal("availability proof rewrote the supplied prompt summary")
		}
	}
	if len(input.Captures) != 1 || input.Captures[0].Event.Seq != test.Event.Seq {
		t.Fatal("published evidence became newly offered work")
	}
	// Captured blobs never gain authority from current worktree text or stale
	// HEAD context. The exact captured mode must still match the planning tree.
	_, modeHead := commit("package example\nfunc CompleteHelper() { _ = 2 }\n", "100755")
	for _, tc := range []struct {
		name  string
		input IntentCandidateEvaluation
	}{
		{"stale planning HEAD", input},
		{"stale abbreviated HEAD", func() IntentCandidateEvaluation {
			copy := input
			copy.LatestCommit = aiCommitSummary(git.CommitSummary{ShortOID: latestHead[:12]})
			return copy
		}()},
		{"changed captured mode", func() IntentCandidateEvaluation {
			copy := input
			copy.LatestCommit = &ai.CommitSummary{OID: modeHead}
			return copy
		}()},
		{"diffs disabled", func() IntentCandidateEvaluation { copy := input; copy.IncludeDiffs = false; return copy }()},
		{"another branch generation", func() IntentCandidateEvaluation { copy := input; copy.BranchGeneration = 2; return copy }()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			loaded, err := loadPublishedIntentFormerCompanions(ctx, f.db, &tc.input, existing)
			if err != nil || len(loaded) != 1 {
				t.Fatalf("unproven helper entered planner context: %+v err=%v", loaded, err)
			}
		})
	}
}

func TestIntentPublishedFormerCompanionsNormalizeInterleavedSavesForEvaluation(t *testing.T) {
	t.Parallel()
	f := newCaptureFixture(t)
	ctx := context.Background()
	blob := func(contents string) string {
		oid, err := git.HashObjectStdin(ctx, f.dir, []byte(contents))
		if err != nil {
			t.Fatal(err)
		}
		return oid
	}
	appendCapture := func(capturePath, before, after, base string) IntentCandidateCapture {
		capture := intentCandidateCaptureFixture(0, capturePath, "modify", blob(before), blob(after))
		capture.Event.BaseHead = base
		seq, err := state.AppendCaptureEvent(ctx, f.db, capture.Event, capture.Ops)
		if err != nil {
			t.Fatal(err)
		}
		capture.Event.Seq = seq
		return capture
	}
	old := "package app\nfunc CapturedReference(value string) bool { return len(value) < 256 }\n"
	middle := strings.ReplaceAll(old, "256", "128")
	final := strings.ReplaceAll(old, "256", "64")
	callerOld := "package app\nfunc VerifyReference(value string) bool { return false }\n"
	callerMiddle := "package app\nfunc VerifyReference(value string) bool { return CapturedReference(value) }\n"
	callerFinal := "package app\nfunc VerifyReference(value string) bool { return CapturedReference(value) && value != \"\" }\n"
	first := appendCapture("references.go", old, middle, f.cctx.BaseHead)
	second := appendCapture("caller.go", callerOld, callerMiddle, f.cctx.BaseHead)
	intermediate := mustCommitPath(t, f.dir, "references.go", middle, "Bound reference examples")
	third := appendCapture("references.go", middle, final, intermediate)
	fourth := appendCapture("caller.go", callerMiddle, callerFinal, intermediate)
	rows := strings.Repeat("\t\t\"ordinary example\",\n", 20)
	testBefore := "package app\nimport \"testing\"\nfunc TestReferences(t *testing.T) {\n\tcases := []string{\n" + rows + "\t}\n\tfor _, value := range cases {\n\t\tif CapturedReference(value) {\n\t\t\tt.Fatal(\"unexpected reference\")\n\t\t}\n\t}\n}\n"
	correction := appendCapture("table_test.go", testBefore, strings.Replace(testBefore, rows, "\t\t\"quoted span cap\",\n"+rows, 1), intermediate)
	all := []IntentCandidateCapture{first, second, third, fourth, correction}
	saveWaitingIntentCandidate(t, f.db, "late-regression", 1, all...)
	mustCommitPath(t, f.dir, "references.go", final, "Tighten reference limits")
	head := mustCommitPath(t, f.dir, "caller.go", callerFinal, "Complete reference validation")
	var members []state.IntentCandidateEvent
	for _, capture := range all[:4] {
		if err := state.MarkEventPublished(ctx, f.db, capture.Event.Seq, state.EventStatePublished,
			sql.NullString{String: head, Valid: true}, sql.NullString{}, sql.NullString{}, float64(time.Now().Unix())); err != nil {
			t.Fatal(err)
		}
		members = append(members, state.IntentCandidateEvent{EventSeq: capture.Event.Seq, EventRole: "code"})
	}
	if err := state.SaveIntentCandidate(ctx, f.db, state.IntentCandidate{
		ID: "published-reference-goal", BranchRef: f.cctx.BranchRef, BranchGeneration: 1,
		Status: state.IntentCandidatePublished, Purpose: "bound reference examples", Readiness: state.IntentReadinessReady,
		PublishedCommitOID: sql.NullString{String: head, Valid: true}, Events: members,
	}); err != nil {
		t.Fatal(err)
	}
	planner := &intentCandidatePlannerStub{plan: ai.IntentPlanV2{ProtocolVersion: ai.IntentPlannerProtocolV2,
		Candidates: []ai.IntentCandidateAssignment{{CandidateID: "late-regression", SelectedSeqs: []int64{correction.Event.Seq},
			Purpose: "cover bounded quoted reference examples", Readiness: ai.IntentCandidateReady,
			Subject: "Cover bounded quoted reference examples", Body: "- Exercise the published reference cap with a late table case",
			GroupingReason: "The recorded enclosing test calls the already-published reference helper"}}}}
	result, err := EvaluateIntentCandidates(ctx, f.db, IntentCandidateEvaluation{
		RepoPath: f.dir, BranchRef: f.cctx.BranchRef, BranchGeneration: 1, IncludeDiffs: true,
		Captures: []IntentCandidateCapture{correction}, LatestCommit: aiCommitSummary(git.CommitSummary{ShortOID: head[:12]}),
		Planner: planner, Preset: config.PresetBalanced, VerificationMode: "structural",
		Materialize: func(context.Context, []IntentCandidateCapture) error { return nil },
	})
	if err != nil || len(result.Decisions) != 1 || !result.Decisions[0].Publishable || planner.calls != 1 {
		t.Fatalf("multi-save published context did not resolve late test: %+v calls=%d err=%v", result, planner.calls, err)
	}
	if len(planner.req.OfferedCaptures) != 1 || planner.req.OfferedCaptures[0].Seq != correction.Event.Seq {
		t.Fatal("published saves were reoffered")
	}
	if planner.req.LatestCommit.OID != head[:12] {
		t.Fatal("proof changed the abbreviated planner summary")
	}
	found := false
	for _, candidate := range planner.req.Candidates {
		if candidate.CandidateID != "published-reference-goal" {
			continue
		}
		found = true
		if candidate.Status != state.IntentCandidatePublished || len(candidate.SelectedSeqs) != 2 || len(candidate.CapturedEvidence) != 2 {
			t.Fatalf("published context is not the complete final path evidence: %+v", candidate)
		}
		for _, capture := range candidate.CapturedEvidence {
			if capture.Path == "references.go" && (!strings.Contains(capture.CapturedDiff, "< 64") || strings.Contains(capture.CapturedDiff, "+func CapturedReference(value string) bool { return len(value) < 128 }")) {
				t.Fatalf("stale intermediate version reached provider: %q", capture.CapturedDiff)
			}
		}
	}
	if !found {
		t.Fatal("published companion is absent from actual provider request")
	}
	stored, ok, err := state.IntentCandidateByID(ctx, f.db, "published-reference-goal")
	if err != nil || !ok || len(stored.Events) != 4 {
		t.Fatalf("immutable published provenance changed: %+v err=%v", stored, err)
	}
	for _, member := range stored.Events {
		if member.EventRole != "code" {
			t.Fatal("derived context rewrote durable membership roles")
		}
	}
	for _, tc := range []struct{ column, wrong string }{
		{"before_oid", blob(old)}, {"before_mode", "100755"},
	} {
		t.Run("broken "+tc.column, func(t *testing.T) {
			if _, err := f.db.SQL().ExecContext(ctx, "UPDATE capture_ops SET "+tc.column+"=? WHERE event_seq=?",
				tc.wrong, third.Event.Seq); err != nil {
				t.Fatal(err)
			}
			_, captures, valid, err := normalizedPublishedIntentContext(ctx, f.db, stored)
			if err != nil || valid || len(captures) != 0 {
				t.Fatalf("matching final HEAD blessed a broken intermediate chain: valid=%t captures=%+v err=%v", valid, captures, err)
			}
			if _, err := f.db.SQL().ExecContext(ctx, "UPDATE capture_ops SET before_oid=?,before_mode=? WHERE event_seq=?",
				third.Ops[0].BeforeOID.String, third.Ops[0].BeforeMode.String, third.Event.Seq); err != nil {
				t.Fatal(err)
			}
			current, err := git.RevParse(ctx, f.dir, "HEAD")
			if err != nil || current != head {
				t.Fatal("chain proof changed the live publication HEAD")
			}
		})
	}
}
