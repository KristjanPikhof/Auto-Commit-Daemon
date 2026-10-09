package daemon

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/ai"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/checkpoint"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/config"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

func TestIntentReofferCandidateMembersPreservesBoundsAndAliases(t *testing.T) {
	t.Parallel()
	first := IntentCandidateCapture{Event: state.CaptureEvent{Seq: 1, BranchRef: "refs/heads/main", BranchGeneration: 1, State: state.EventStatePending}}
	last := first
	last.Event.Seq = 3
	covered := first.Event
	covered.Seq = 2
	last.CoveredEvents = []state.CaptureEvent{covered}
	candidate := state.IntentCandidate{Events: []state.IntentCandidateEvent{{EventSeq: 1}, {EventSeq: 2}, {EventSeq: 3}}}
	input := IntentCandidateEvaluation{BranchRef: first.Event.BranchRef, BranchGeneration: 1, Captures: []IntentCandidateCapture{first}, TargetEventSeqs: []int64{1, 2, 3}}
	if !reofferIntentCandidateMembers(&input, candidate, []IntentCandidateCapture{first, last}) || len(input.Captures) != 2 || input.Captures[1].Event.Seq != 3 || len(input.Captures[1].CoveredEvents) != 1 {
		t.Fatalf("recorded aliases were not preserved: %+v", input.Captures)
	}
	input.Captures = []IntentCandidateCapture{first}
	outside := covered
	outside.Seq = 4
	last.CoveredEvents = append(last.CoveredEvents, outside)
	if reofferIntentCandidateMembers(&input, candidate, []IntentCandidateCapture{first, last}) || len(input.Captures) != 1 {
		t.Fatal("later covered capture widened the frozen target")
	}
	input.Captures = nil
	input.TargetEventSeqs = nil
	for seq := int64(1); seq <= ai.IntentCandidateCaptureCap; seq++ {
		capture := first
		capture.Event.Seq = seq
		input.Captures = append(input.Captures, capture)
		input.TargetEventSeqs = append(input.TargetEventSeqs, seq)
	}
	last = first
	last.Event.Seq = ai.IntentCandidateCaptureCap + 1
	input.TargetEventSeqs = append(input.TargetEventSeqs, last.Event.Seq)
	candidate.Events = []state.IntentCandidateEvent{{EventSeq: last.Event.Seq}}
	if reofferIntentCandidateMembers(&input, candidate, []IntentCandidateCapture{last}) || len(input.Captures) != ai.IntentCandidateCaptureCap {
		t.Fatal("reoffering exceeded the bounded planner window")
	}
}

func TestIntentUnclassifiedFallbackRepartitionsFrozenFiftyFourCaptures(t *testing.T) {
	t.Parallel()
	testIntentUnclassifiedFallbackRepartition(t, 52, false)
}

func TestIntentUnclassifiedFallbackRetryDoesNotRotateCooldown(t *testing.T) {
	t.Parallel()
	testIntentUnclassifiedFallbackRepartition(t, 2, true)
}

func testIntentUnclassifiedFallbackRepartition(t *testing.T, versions int, rejectFirst bool, stalePurpose ...bool) {
	t.Helper()
	ctx := context.Background()
	f := newCaptureFixture(t)
	if _, err := BootstrapShadow(ctx, f.dir, f.db, f.cctx); err != nil {
		t.Fatal(err)
	}
	blob := func(body string) string {
		oid, err := git.HashObjectStdinDurable(ctx, f.dir, []byte(body))
		if err != nil {
			t.Fatal(err)
		}
		return oid
	}
	yes := "package app\nfunc RetrySpeech() bool { return true }\n"
	no := "package app\nfunc RetrySpeech() bool { return false }\n"
	yesOID, noOID := blob(yes), blob(no)
	testBody := "package app\nimport \"testing\"\nfunc TestRetrySpeech(t *testing.T) { if RetrySpeech() { t.Fatal(\"unsupported retry\") } }\n"
	timingBody := "{\"test_seconds\": 1.25}\n"
	testOID, timingOID := blob(testBody), blob(timingBody)
	var captures []IntentCandidateCapture
	before := ""
	for i := 0; i < versions; i++ {
		after := yesOID
		if i%2 == 1 {
			after = noOID
		}
		op := "modify"
		if i == 0 {
			op = "create"
		}
		capture := intentCandidateCaptureFixture(0, "retry.go", op, before, after)
		capture.Event.BaseHead = f.cctx.BaseHead
		capture.Event.CapturedTS = 100 + float64(i)
		captures = append(captures, capture)
		before = after
	}
	for _, file := range []struct{ path, oid string }{{"retry_test.go", testOID}, {"scripts/dev/test-timings.json", timingOID}} {
		capture := intentCandidateCaptureFixture(0, file.path, "create", "", file.oid)
		capture.Event.BaseHead = f.cctx.BaseHead
		capture.Event.CapturedTS = 153
		captures = append(captures, capture)
	}
	seedIntentCandidateCaptureBatch(t, f.db, captures)
	writePublicationFile(t, f, "retry.go", no)
	writePublicationFile(t, f, "retry_test.go", testBody)
	if err := os.MkdirAll(filepath.Join(f.dir, "scripts", "dev"), 0o755); err != nil {
		t.Fatal(err)
	}
	writePublicationFile(t, f, "scripts/dev/test-timings.json", timingBody)
	now := time.Now().UTC().Truncate(time.Second)
	input := IntentCandidateEvaluation{RepoPath: f.dir, BranchRef: f.cctx.BranchRef, BranchGeneration: f.cctx.BranchGeneration,
		Captures: captures, Preset: config.PresetBalanced, PresetID: "intent.balanced", PresetVersion: config.PresetCatalogVersion,
		CommitFormat: ai.CommitFormatImperative, IncludeDiffs: true, VerificationMode: "structural", Provider: "intent-v2-test", Now: now,
		Materialize: intentCandidateScratchMaterializer(f.dir, f.gitDir, f.cctx.BaseHead)}
	for _, capture := range captures {
		input.TargetEventSeqs = append(input.TargetEventSeqs, capture.Event.Seq)
	}
	entries, err := git.LsTree(ctx, f.dir, "HEAD", true)
	if err != nil {
		t.Fatal(err)
	}
	var checkpointEntries []checkpoint.Entry
	for _, entry := range entries {
		checkpointEntries = append(checkpointEntries, checkpoint.Entry{Path: entry.Path, Mode: entry.Mode, OID: entry.OID})
	}
	checkpointEntries = append(checkpointEntries, checkpoint.Entry{Path: "retry.go", Mode: git.RegularFileMode, OID: noOID}, checkpoint.Entry{Path: "retry_test.go", Mode: git.RegularFileMode, OID: testOID}, checkpoint.Entry{Path: "scripts/dev/test-timings.json", Mode: git.RegularFileMode, OID: timingOID})
	protected, err := (checkpoint.Store{DB: f.db}).Create(ctx, checkpoint.Request{RepoRoot: f.dir, WorktreeID: checkpoint.WorktreeID(f.dir), Reason: state.CheckpointReasonManualBarrier,
		ObservationEpoch: 1, CoverageEpoch: 1, ObservedRef: f.cctx.BranchRef, ObservedHead: f.cctx.BaseHead, Entries: checkpointEntries, EventSeqs: input.TargetEventSeqs, Now: now})
	if err != nil {
		t.Fatal(err)
	}
	drain := state.PublicationDrain{ID: "frozen-provisional-partition", CheckpointID: protected.Checkpoint.ID, WorktreeID: checkpoint.WorktreeID(f.dir), BranchRef: input.BranchRef, BranchGeneration: input.BranchGeneration,
		CommitStrategy: "intent", CommitFormat: "imperative", Provider: input.Provider, ProviderFingerprint: "sha256:" + strings.Repeat("0", 64), Phase: state.PublicationDrainSemantic,
		TargetEventCount: int64(len(captures)), EventSeqs: input.TargetEventSeqs, CreatedTS: float64(now.Unix()), UpdatedTS: float64(now.Unix()), LastProgressTS: float64(now.Unix())}
	if _, err := state.PreparePublicationDrain(ctx, f.db, drain); err != nil {
		t.Fatal(err)
	}
	input.Captures, err = loadFocusedIntentGoalEvidence(ctx, input, nil, input.Captures)
	if err != nil {
		t.Fatal(err)
	}
	if err := attachIntentFileMetadata(ctx, f.dir, input.Captures); err != nil {
		t.Fatal(err)
	}
	deps, err := BuildIntentCandidateDependencies(input.BranchRef, input.BranchGeneration, input.Captures, runtimeIntentDependencyHints(input.Captures), now)
	if err != nil {
		t.Fatal(err)
	}
	if err := state.ReplaceIntentCaptureDependencies(ctx, f.db, input.BranchRef, input.BranchGeneration, deps); err != nil {
		t.Fatal(err)
	}
	req, err := buildIntentCandidateRequest(input, nil, deps, nil, input.Captures)
	if err != nil {
		t.Fatal(err)
	}
	unknown := ai.IntentCandidateAssignment{CandidateID: "provisional-fifty-four", SelectedSeqs: input.TargetEventSeqs, Purpose: "retain dependency component until its goal is known", Readiness: ai.IntentCandidateWait,
		MissingCompanions: []string{unclassifiedIntentCompanion}, GroupingReason: "protected dependency component needs a meaningful goal message"}
	if len(captures) > intentBalancedFallbackCaptureCap {
		unknown.MissingCompanions = append(unknown.MissingCompanions, "balanced fallback exceeds 32 captures")
		unknown.GroupingReason = "bounded fallback requires planner review"
	}
	plan := ai.IntentPlanV2{ProtocolVersion: ai.IntentPlannerProtocolV2, Candidates: []ai.IntentCandidateAssignment{unknown}}
	bySeq := map[int64]IntentCandidateCapture{}
	for _, capture := range input.Captures {
		bySeq[capture.Event.Seq] = capture
	}
	// Earlier workers used an existing local partition as membership
	// evidence when extending it. This creates the actual pending boundary
	// that later native plans must be able to repartition.
	prior := state.IntentCandidate{ID: unknown.CandidateID, BranchRef: input.BranchRef, BranchGeneration: input.BranchGeneration,
		Status: state.IntentCandidateWaiting, Readiness: state.IntentReadinessWait, Purpose: unknown.Purpose,
		CreatedTS: float64(now.Unix()), UpdatedTS: float64(now.Unix())}
	for _, capture := range input.Captures {
		prior.Events = append(prior.Events, state.IntentCandidateEvent{EventSeq: capture.Event.Seq, EventRole: intentCaptureRole(capture)})
	}
	if err := state.SaveIntentCandidate(ctx, f.db, prior); err != nil {
		t.Fatal(err)
	}
	decision, err := evaluateIntentCandidateAssignment(ctx, f.db, input, plan, unknown,
		appendIntentCandidateMembershipDependencies(deps, []state.IntentCandidate{prior}), map[string]state.IntentCandidate{prior.ID: prior}, bySeq)
	if err != nil {
		t.Fatal(err)
	}
	if decision.Candidate.Status != state.IntentCandidateWaiting || decision.Candidate.AtomicityStatus.String != string(ai.IntentAtomicityPending) {
		t.Fatalf("legacy local partition was not recreated: status=%s atomicity=%s", decision.Candidate.Status, decision.Candidate.AtomicityStatus.String)
	}
	if err := state.SaveIntentCandidate(ctx, f.db, decision.Candidate); err != nil {
		t.Fatal(err)
	}
	if len(stalePurpose) > 0 && stalePurpose[0] {
		// Reproduce Source's older meaningful label, never approved as a
		// complete goal, followed by a newer exact unknown-goal plan.
		decision.Candidate.Purpose = "prove command ownership and preserve replay feedback"
		decision.Candidate.MissingCompanions = "balanced fallback exceeds 12 paths"
		decision.Candidate.UpdatedTS = float64(now.Add(-time.Minute).Unix())
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
	run.Completed = true
	run.ResolutionMode = sql.NullString{String: "waiting_semantic_retry", Valid: true}
	run.ProgressState = run.ResolutionMode
	run.UnresolvedSeqs = input.TargetEventSeqs
	if err := storeResolvedIntentPlanRun(&run, plan, nil); err != nil {
		t.Fatal(err)
	}
	if err := state.UpdateIntentPlanRun(ctx, f.db, run); err != nil {
		t.Fatal(err)
	}
	evidence, err := intentSemanticRetryEvidence(req, input, 3)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := scheduleIntentSemanticRetry(ctx, f.db, input, run, evidence, now); err != nil {
		t.Fatal(err)
	}
	// A partial normal window must keep the boundary. The frozen target can
	// reoffer its recorded members without including later work.
	partial := input
	partial.Captures = append([]IntentCandidateCapture(nil), input.Captures[:1]...)
	partial.TargetEventSeqs = nil
	if ids, err := unclassifiedIntentCandidatesForRepartition(ctx, f.db, &partial, []state.IntentCandidate{decision.Candidate}, input.Captures); err != nil || len(ids) != 0 {
		t.Fatalf("unfrozen partial boundary released: %v err=%v", ids, err)
	}
	for _, tc := range []struct {
		name   string
		status string
		mode   string
	}{{"approved_goal", string(ai.IntentAtomicityPassed), "waiting_semantic_retry"}, {"provider_goal", string(ai.IntentAtomicityPending), "provider"}} {
		t.Run(tc.name, func(t *testing.T) {
			candidate := decision.Candidate
			candidate.AtomicityStatus = sql.NullString{String: tc.status, Valid: true}
			if _, err := f.db.SQL().ExecContext(ctx, "UPDATE intent_plan_runs SET resolution_mode=? WHERE fingerprint=?", tc.mode, run.Fingerprint); err != nil {
				t.Fatal(err)
			}
			copy := input
			if ids, err := unclassifiedIntentCandidatesForRepartition(ctx, f.db, &copy, []state.IntentCandidate{candidate}, input.Captures); err != nil || len(ids) != 0 {
				t.Fatalf("valid semantic boundary released: %v err=%v", ids, err)
			}
		})
	}
	if _, err := f.db.SQL().ExecContext(ctx, "UPDATE intent_plan_runs SET resolution_mode='waiting_semantic_retry' WHERE fingerprint=?", run.Fingerprint); err != nil {
		t.Fatal(err)
	}
	if rejectFirst {
		for _, status := range []string{"pending", "not_required", "failed", "timed_out", "needs_attention"} {
			t.Run("verification_"+status, func(t *testing.T) {
				candidate := decision.Candidate
				candidate.VerificationStatus = sql.NullString{String: status, Valid: true}
				copy := input
				ids, err := unclassifiedIntentCandidatesForRepartition(ctx, f.db, &copy, []state.IntentCandidate{candidate}, input.Captures)
				want := status == "pending" || status == "not_required"
				if err != nil || ids[candidate.ID] != want {
					t.Fatalf("verification mode=%s released=%v want=%t err=%v", status, ids, want, err)
				}
			})
		}
	}
	// Rejection creates another local partition. Its ID must not invalidate
	// the same evidence again or spend additional provider calls.
	planner := &semanticRetryReplayPlanner{}
	input.Planner = planner
	if rejectFirst {
		planner.err = &ai.IntentPlanV2ValidationError{Message: "invalid response"}
		first, err := EvaluateIntentCandidates(ctx, f.db, input)
		if err != nil || first.ResolutionMode != "waiting_semantic_retry" {
			t.Fatalf("fallback review=%+v err=%v", first, err)
		}
		calls := planner.calls
		retry, found, err := loadIntentSemanticRetry(ctx, f.db)
		if err != nil || !found {
			t.Fatalf("retry=%+v found=%t err=%v", retry, found, err)
		}
		again, err := EvaluateIntentCandidates(ctx, f.db, input)
		after, _, _ := loadIntentSemanticRetry(ctx, f.db)
		if err != nil || again.ResolutionMode != "waiting_semantic_retry" || planner.calls != calls || after.RetryAtTS != retry.RetryAtTS {
			t.Fatalf("provisional ID bypassed cooldown: %+v calls=%d/%d retry=%+v/%+v err=%v", again, planner.calls, calls, retry, after, err)
		}
		input.Now = secondsTime(retry.RetryAtTS).Add(time.Second)
	}
	planner.err = nil
	planner.plan = ai.IntentPlanV2{ProtocolVersion: ai.IntentPlannerProtocolV2, Candidates: []ai.IntentCandidateAssignment{
		{CandidateID: "speech-retry", SelectedSeqs: input.TargetEventSeqs[:versions+1], Purpose: "disable unsupported speech retry attempts", Readiness: ai.IntentCandidateReady, Subject: "Add speech retry availability checks", Body: "- Keep unsupported attempts disabled and covered by their test", GroupingReason: "the recorded retry implementation and test complete one goal"},
		{CandidateID: "timing-metadata", SelectedSeqs: input.TargetEventSeqs[versions+1:], Purpose: "record measured regression runtimes", Readiness: ai.IntentCandidateReady, Subject: "Record measured regression runtimes", Body: "- Keep the local test budget based on observed durations", GroupingReason: "timing observations are independently reviewable"}}}
	reviewed, err := EvaluateIntentCandidates(ctx, f.db, input)
	if err != nil || reviewed.NeedsAttention || len(reviewed.Decisions) != 2 {
		t.Fatalf("safe goals stayed glued: mode=%s failure=%s decisions=%d err=%v", reviewed.ResolutionMode, reviewed.PlannerFailure, len(reviewed.Decisions), err)
	}
	var owned []int64
	for _, goal := range reviewed.Decisions {
		if !goal.Publishable {
			t.Fatalf("corrected goal blocked: %+v", goal)
		}
		for _, member := range goal.Candidate.Events {
			owned = append(owned, member.EventSeq)
		}
	}
	if !reflect.DeepEqual(owned, input.TargetEventSeqs) {
		t.Fatalf("immutable members changed: got=%v want=%v", owned, input.TargetEventSeqs)
	}
	old, found, err := state.IntentCandidateByID(ctx, f.db, decision.Candidate.ID)
	if err != nil || !found || old.Status != state.IntentCandidateSuperseded {
		t.Fatalf("provisional owner survived: %+v err=%v", old, err)
	}
	if history, err := state.IntentCandidateEventHistory(ctx, f.db, old.ID); err != nil || len(history) != len(captures) {
		t.Fatalf("provenance lost: %d err=%v", len(history), err)
	}
	if _, err := git.Run(ctx, git.RunOpts{Dir: f.dir}, "diff", "--cached", "--exit-code"); err != nil {
		t.Fatal(err)
	}
	if head, err := git.RevParse(ctx, f.dir, "HEAD"); err != nil || head != f.cctx.BaseHead {
		t.Fatalf("review moved branch: %s err=%v", head, err)
	}
	published, err := Replay(ctx, f.dir, f.db, f.cctx, ReplayOpts{
		GitDir: f.gitDir, CommitStrategy: ai.CommitStrategyIntent, IntentPlanner: planner,
		IntentPlannerProvider: planner.Name(), IntentPreset: config.PresetBalanced,
		IntentWindow: 64, IntentBypassBatchWait: true, IntentVerificationMode: "structural", PublicationDrain: &drain,
	})
	if err != nil || published.Published != len(captures) || published.Failed != 0 || published.Disposition == ReplayDispositionNeedsAttention {
		t.Fatalf("corrected goals did not commit: %+v err=%v", published, err)
	}
	completed, err := UpdatePublicationDrainAfterReplay(ctx, f.db, drain, published, nil, time.Now())
	if err != nil || completed.Phase != state.PublicationDrainCompleted || !reflect.DeepEqual(completed.EventSeqs, input.TargetEventSeqs) {
		t.Fatalf("frozen publication outcome incomplete: %+v err=%v", completed, err)
	}
	if pending, err := state.PendingEvents(ctx, f.db, 0); err != nil || len(pending) != 0 {
		t.Fatalf("publication left queued work: %+v err=%v", pending, err)
	}
	if attention, err := hasUnresolvedIntentV2CandidateAttention(ctx, f.db); err != nil || attention {
		t.Fatalf("shared status/list projection still requires action: %t err=%v", attention, err)
	}
	if err := updateIntentV2EvaluationMeta(ctx, f.db, &RuntimeBundle{PresetID: "intent.balanced", PresetVersion: config.PresetCatalogVersion}, published, nil); err != nil {
		t.Fatal(err)
	}
	if raw, _, err := state.MetaGet(ctx, f.db, "intent.v2.needs_attention"); err != nil || raw != "" {
		t.Fatalf("publication left a visible safety block: %q err=%v", raw, err)
	}
	commits := strings.TrimSpace(mustGitOutput(t, f.dir, "log", "-2", "--format=%s"))
	if commits != "Record measured regression runtimes\nAdd speech retry availability checks" {
		t.Fatalf("goals did not form two purposeful commits: %q", commits)
	}
}
