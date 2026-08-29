package daemon

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/ai"
	checkpointpkg "github.com/KristjanPikhof/Auto-Commit-Daemon/internal/checkpoint"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/config"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/verification"
)

type failedCheckpointRecoveryPlanner struct {
	stalledCandidateID string
	offeredCounts      []int
	offeredSeqs        [][]int64
}

func (*failedCheckpointRecoveryPlanner) Name() string {
	return "failed-checkpoint-recovery-test"
}

func (*failedCheckpointRecoveryPlanner) PlanIntent(
	context.Context,
	ai.IntentPlanRequest,
) (ai.IntentPlan, error) {
	return ai.IntentPlan{}, errors.New("legacy intent planning is not expected")
}

func (p *failedCheckpointRecoveryPlanner) PlanIntentV2(
	_ context.Context,
	req ai.IntentPlanRequestV2,
) (ai.IntentPlanV2, error) {
	seqs := make([]int64, 0, len(req.OfferedCaptures))
	for _, capture := range req.OfferedCaptures {
		seqs = append(seqs, capture.Seq)
	}
	p.offeredCounts = append(p.offeredCounts, len(seqs))
	p.offeredSeqs = append(p.offeredSeqs, append([]int64(nil), seqs...))
	candidateID := p.stalledCandidateID
	if len(seqs) == 3 {
		candidateID = "checkpoint-recovered-candidate"
	}
	return ai.IntentPlanV2{
		ProtocolVersion: ai.IntentPlannerProtocolV2,
		Candidates: []ai.IntentCandidateAssignment{{
			CandidateID:  candidateID,
			SelectedSeqs: seqs,
			Purpose:      "complete the source change and its test companion",
			Readiness:    ai.IntentCandidateReady,
			Subject:      "Complete source change",
			GroupingReason: "the implementation, fixture, and test form one " +
				"verified source change",
		}},
	}, nil
}

const stalledCheckpointCandidateID = "checkpoint-stalled-candidate"

type failedCheckpointReplayFixture struct {
	capture *captureFixture
	seqs    []int64
}

type semanticPrefixMessagePlanner struct {
	rewriteSeqs [][]int64
}

type exactTargetReplanPlanner struct {
	offeredSeqs [][]int64
}

type expandedTargetRecoveryPlanner struct {
	offeredSeqs [][]int64
}

func (*semanticPrefixMessagePlanner) Name() string {
	return "semantic-prefix-message-test"
}

func (*semanticPrefixMessagePlanner) PlanIntent(
	context.Context,
	ai.IntentPlanRequest,
) (ai.IntentPlan, error) {
	return ai.IntentPlan{}, errors.New(
		"semantic-prefix membership must be locked locally")
}

func (p *semanticPrefixMessagePlanner) RewriteIntentMessage(
	_ context.Context,
	req ai.IntentMessageRewriteRequest,
) (ai.Result, error) {
	p.rewriteSeqs = append(p.rewriteSeqs,
		append([]int64(nil), req.LockedPlan.SelectedSeqs...))
	return ai.Result{
		Subject: "Restore checkpoint compilation",
		Body:    "- Keep the semantic dependency prefix together",
		Source:  p.Name(),
	}, nil
}

func (*exactTargetReplanPlanner) Name() string {
	return "exact-target-replan-test"
}

func (*exactTargetReplanPlanner) PlanIntent(
	context.Context,
	ai.IntentPlanRequest,
) (ai.IntentPlan, error) {
	return ai.IntentPlan{}, errors.New("Intent v2 planning is required")
}

func (p *exactTargetReplanPlanner) PlanIntentV2(
	_ context.Context,
	req ai.IntentPlanRequestV2,
) (ai.IntentPlanV2, error) {
	seqs := make([]int64, 0, len(req.OfferedCaptures))
	for _, capture := range req.OfferedCaptures {
		seqs = append(seqs, capture.Seq)
	}
	p.offeredSeqs = append(p.offeredSeqs, append([]int64(nil), seqs...))
	return ai.IntentPlanV2{
		ProtocolVersion: ai.IntentPlannerProtocolV2,
		Candidates: []ai.IntentCandidateAssignment{{
			CandidateID:  "exact-target-replanned-candidate",
			SelectedSeqs: seqs,
			Purpose:      "replan the remaining frozen recovery target",
			Readiness:    ai.IntentCandidateReady,
			Subject:      "Replan remaining recovery",
			GroupingReason: "the remaining captures form the exact unresolved " +
				"semantic target",
		}},
	}, nil
}

func (*expandedTargetRecoveryPlanner) Name() string {
	return "expanded-target-recovery-test"
}

func (*expandedTargetRecoveryPlanner) PlanIntent(
	context.Context,
	ai.IntentPlanRequest,
) (ai.IntentPlan, error) {
	return ai.IntentPlan{}, errors.New("Intent v2 planning is required")
}

func (p *expandedTargetRecoveryPlanner) PlanIntentV2(
	_ context.Context,
	req ai.IntentPlanRequestV2,
) (ai.IntentPlanV2, error) {
	seqs := make([]int64, 0, len(req.OfferedCaptures))
	for _, capture := range req.OfferedCaptures {
		seqs = append(seqs, capture.Seq)
	}
	p.offeredSeqs = append(p.offeredSeqs, append([]int64(nil), seqs...))
	return ai.IntentPlanV2{
		ProtocolVersion: ai.IntentPlannerProtocolV2,
		Candidates: []ai.IntentCandidateAssignment{{
			CandidateID:  "expanded-recovery-candidate",
			SelectedSeqs: seqs,
			Purpose:      "complete the API transition at its latest snapshot",
			Readiness:    ai.IntentCandidateReady,
			Subject:      "Complete API transition",
			GroupingReason: "the newer same-path snapshots complete the frozen " +
				"intermediate implementation",
		}},
	}, nil
}

func TestIntentEvaluationAwaitingCheckpointRecovery(t *testing.T) {
	recoverable := IntentCandidateEvaluationResult{
		NeedsAttention: true,
		Decisions: []IntentCandidateDecision{{
			Assignment: ai.IntentCandidateAssignment{
				Readiness: ai.IntentCandidateReady,
			},
			Candidate: state.IntentCandidate{
				Status: state.IntentCandidateWaiting,
				VerificationStatus: sql.NullString{
					String: "failed", Valid: true,
				},
			},
		}},
	}
	if !intentEvaluationAwaitingCheckpointRecovery(recoverable) {
		t.Fatal("failed verification was not classified as automatic recovery")
	}

	for _, test := range []struct {
		name   string
		mutate func(*IntentCandidateEvaluationResult)
	}{
		{name: "planner failure", mutate: func(result *IntentCandidateEvaluationResult) {
			result.PlannerFailure = "provider rejected the plan"
		}},
		{name: "blocked candidate", mutate: func(result *IntentCandidateEvaluationResult) {
			result.Decisions[0].Candidate.Status = state.IntentCandidateBlocked
		}},
		{name: "non-verification failure", mutate: func(result *IntentCandidateEvaluationResult) {
			result.Decisions[0].Candidate.VerificationStatus.String = "needs_attention"
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			result := recoverable
			result.Decisions = append([]IntentCandidateDecision(nil),
				recoverable.Decisions...)
			test.mutate(&result)
			if intentEvaluationAwaitingCheckpointRecovery(result) {
				t.Fatalf("%s was classified as automatic recovery", test.name)
			}
		})
	}
}

func TestReplayVerificationResourceWaitRetainsSemanticTarget(t *testing.T) {
	ctx := context.Background()
	f := newCaptureFixture(t)
	seedTrackedFileCommit(t, ctx, f, "resource_wait.go",
		"package source\n\nconst Value = 1\n")
	if _, err := BootstrapShadow(ctx, f.dir, f.db, f.cctx); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(f.dir, "resource_wait.go"),
		[]byte("package source\n\nconst Value = 2\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	checkpointStore := checkpointpkg.Store{DB: f.db}
	if captured, err := Capture(ctx, f.dir, f.db, f.cctx, CaptureOpts{
		IgnoreChecker: f.ig, SensitiveMatcher: f.matcher,
		CheckpointStore:  &checkpointStore,
		WorktreeID:       checkpointpkg.WorktreeID(f.dir),
		ObservationEpoch: 1, CheckpointReason: state.CheckpointReasonPoll,
		DisablePendingCap: true,
	}); err != nil || captured.EventsAppended != 1 ||
		captured.CheckpointID == "" {
		t.Fatalf("resource wait capture=%+v err=%v", captured, err)
	}
	pendingBefore, err := state.PublishableEvents(ctx, f.db, 0)
	if err != nil || len(pendingBefore) != 1 {
		t.Fatalf("resource wait pending=%+v err=%v", pendingBefore, err)
	}
	planner := &exactTargetReplanPlanner{}
	opts := semanticPrefixReplayOpts(planner, func(
		context.Context,
		ai.IntentCandidateAssignment,
		[]IntentCandidateCapture,
	) (IntentCandidateVerification, error) {
		return IntentCandidateVerification{Status: "needs_attention"},
			fmt.Errorf("prepare verification workspace: %w",
				verification.ErrResourceUnavailable)
	})
	opts.GitDir = f.gitDir

	result, err := Replay(ctx, f.dir, f.db, f.cctx, opts)
	if err != nil {
		t.Fatal(err)
	}
	if !result.Skipped ||
		result.SkippedReason != intentVerificationResourceWaitSkipReason ||
		result.Disposition != ReplayDispositionTransientWait ||
		result.DispositionReason != intentVerificationResourceWaitReason ||
		result.HasMore || result.Published != 0 || result.Failed != 0 ||
		result.Conflicts != 0 {
		t.Fatalf("resource wait replay=%+v", result)
	}
	wantSeqs := []int64{pendingBefore[0].Seq}
	if !reflect.DeepEqual(planner.offeredSeqs, [][]int64{wantSeqs}) {
		t.Fatalf("resource wait offers=%v want %v",
			planner.offeredSeqs, wantSeqs)
	}
	if _, active, err := state.IntentForwardRecoveryForPair(
		ctx, f.db, f.cctx.BranchRef, f.cctx.BranchGeneration,
	); err != nil || active {
		t.Fatalf("resource wait recovery active=%t err=%v", active, err)
	}
	for _, seq := range wantSeqs {
		var eventState string
		if err := f.db.ReadSQL().QueryRowContext(ctx,
			`SELECT state FROM capture_events WHERE seq=?`, seq).
			Scan(&eventState); err != nil {
			t.Fatal(err)
		}
		if eventState != state.EventStatePending {
			t.Fatalf("resource wait event %d state=%q", seq, eventState)
		}
	}
}

func seedFailedCheckpointReplayFixture(t *testing.T) failedCheckpointReplayFixture {
	t.Helper()
	f := newCaptureFixture(t)
	ctx := context.Background()
	for path, contents := range map[string]string{
		"companion_test.go": "package source\n\nconst Companion = 1\n",
		"fresh.go":          "package source\n\nconst Fresh = 1\n",
		"source.go": "package source\n\n" +
			"func Value() int { return 1 }\n",
		"source_fixture.go": "package source\n\nconst Fixture = 1\n",
		"source_test.go":    "package source\n\nvar Want = Value()\n",
	} {
		seedTrackedFileCommit(t, ctx, f, path, contents)
	}
	if _, err := BootstrapShadow(ctx, f.dir, f.db, f.cctx); err != nil {
		t.Fatal(err)
	}
	for path, contents := range map[string]string{
		"source.go": "package source\n\n" +
			"func Value(label string) int { return len(label) }\n",
		"source_fixture.go": "package source\n\nconst Fixture = 2\n",
		// This checkpoint is an intentionally incomplete API transition:
		// Value now requires a label, while the test still calls Value().
		"source_test.go": "package source\n\nvar Want = Value() + 1\n",
	} {
		if err := os.WriteFile(filepath.Join(f.dir, path), []byte(contents), 0o644); err != nil {
			t.Fatal(err)
		}
	}

	checkpointStore := checkpointpkg.Store{DB: f.db}
	capture, err := Capture(ctx, f.dir, f.db, f.cctx, CaptureOpts{
		IgnoreChecker:     f.ig,
		SensitiveMatcher:  f.matcher,
		CheckpointStore:   &checkpointStore,
		WorktreeID:        checkpointpkg.WorktreeID(f.dir),
		ObservationEpoch:  1,
		CheckpointReason:  state.CheckpointReasonPoll,
		SortByPath:        true,
		DisablePendingCap: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !capture.Protected || capture.CheckpointID == "" || capture.EventsAppended != 3 {
		t.Fatalf("checkpoint capture=%+v", capture)
	}
	pending, err := state.PublishableEvents(ctx, f.db, 0)
	if err != nil || len(pending) != 3 {
		t.Fatalf("publishable events=%+v err=%v", pending, err)
	}
	seqs := []int64{pending[0].Seq, pending[1].Seq, pending[2].Seq}

	if err := state.SaveIntentCandidate(ctx, f.db, state.IntentCandidate{
		ID: stalledCheckpointCandidateID, BranchRef: f.cctx.BranchRef,
		BranchGeneration:  f.cctx.BranchGeneration,
		Status:            state.IntentCandidateWaiting,
		Readiness:         state.IntentReadinessWait,
		Purpose:           "complete the source implementation",
		MissingCompanions: "source_test.go",
		AtomicityStatus: sql.NullString{
			String: string(ai.IntentAtomicityFailed), Valid: true,
		},
		AtomicitySummary: "verification failed without the test companion",
		VerificationStatus: sql.NullString{
			String: "failed", Valid: true,
		},
		Events: []state.IntentCandidateEvent{
			{EventSeq: seqs[0], EventRole: "implementation"},
			{EventSeq: seqs[1], EventRole: "fixture"},
		},
	}); err != nil {
		t.Fatal(err)
	}
	for _, seq := range seqs {
		if err := state.RecordPlannerDefer(ctx, f.db, seq, 1,
			"waiting for the complete protected change"); err != nil {
			t.Fatal(err)
		}
	}
	return failedCheckpointReplayFixture{capture: f, seqs: seqs}
}

func seedSemanticPrefixRecovery(
	t *testing.T,
	fixture failedCheckpointReplayFixture,
) (state.IntentForwardRecovery, ai.IntentPlanV2) {
	t.Helper()
	f := fixture.capture
	ctx := context.Background()
	recovery, changed, err := state.StartFailedIntentCheckpointRecovery(
		ctx, f.db, f.cctx.BranchRef, f.cctx.BranchGeneration,
		fixture.seqs[0])
	if err != nil || !changed {
		t.Fatalf("start recovery=(%+v changed=%t err=%v)",
			recovery, changed, err)
	}
	plan := ai.IntentPlanV2{
		ProtocolVersion: ai.IntentPlannerProtocolV2,
		Candidates: []ai.IntentCandidateAssignment{
			{
				CandidateID:  "semantic-prerequisite",
				SelectedSeqs: []int64{fixture.seqs[2]},
				Purpose:      "establish the semantic prerequisite",
				Readiness:    ai.IntentCandidateReady,
				Subject:      "Add semantic prerequisite",
				GroupingReason: "the provider identified this capture as the " +
					"first independently reviewable intent",
			},
			{
				CandidateID:         "semantic-dependent",
				SelectedSeqs:        []int64{fixture.seqs[0]},
				Purpose:             "apply the first dependent change",
				Readiness:           ai.IntentCandidateReady,
				DependsOnCandidates: []string{"semantic-prerequisite"},
				Subject:             "Apply semantic dependent",
				GroupingReason: "the provider declared an explicit dependency " +
					"on the prerequisite intent",
			},
			{
				CandidateID:         "semantic-final",
				SelectedSeqs:        []int64{fixture.seqs[1]},
				Purpose:             "complete the semantic change",
				Readiness:           ai.IntentCandidateReady,
				DependsOnCandidates: []string{"semantic-dependent"},
				Subject:             "Complete semantic change",
				GroupingReason: "the provider declared an explicit dependency " +
					"on the preceding intent",
			},
		},
	}
	run, err := state.EnsureIntentPlanRun(ctx, f.db, state.IntentPlanRun{
		Fingerprint:      "semantic-prefix-plan-test",
		BranchRef:        f.cctx.BranchRef,
		BranchGeneration: f.cctx.BranchGeneration,
		AttemptLimit:     1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := storeResolvedIntentPlanRun(&run, plan, nil); err != nil {
		t.Fatal(err)
	}
	run.Completed = true
	run.ResolutionMode = sql.NullString{String: "provider", Valid: true}
	run.ProgressState = sql.NullString{String: "completed", Valid: true}
	if err := state.UpdateIntentPlanRun(ctx, f.db, run); err != nil {
		t.Fatal(err)
	}
	recovery, err = state.AdvanceIntentForwardRecoveryPrefix(
		ctx, f.db, recovery, run.Fingerprint, f.cctx.BaseHead, 1)
	if err != nil {
		t.Fatal(err)
	}
	return recovery, plan
}

func appendLaterRecoverySnapshots(
	t *testing.T,
	fixture failedCheckpointReplayFixture,
) (matching []int64, checkpointSibling []int64, unrelated int64) {
	t.Helper()
	f := fixture.capture
	ctx := context.Background()
	checkpointStore := checkpointpkg.Store{DB: f.db}
	capture := func(epoch int64, edits map[string]string) []state.CaptureEvent {
		t.Helper()
		for path, contents := range edits {
			if err := os.WriteFile(
				filepath.Join(f.dir, path), []byte(contents), 0o644,
			); err != nil {
				t.Fatal(err)
			}
		}
		before, err := state.PublishableEvents(ctx, f.db, 0)
		if err != nil {
			t.Fatal(err)
		}
		result, err := Capture(ctx, f.dir, f.db, f.cctx, CaptureOpts{
			IgnoreChecker:     f.ig,
			SensitiveMatcher:  f.matcher,
			CheckpointStore:   &checkpointStore,
			WorktreeID:        checkpointpkg.WorktreeID(f.dir),
			ObservationEpoch:  epoch,
			CheckpointReason:  state.CheckpointReasonPoll,
			SortByPath:        true,
			DisablePendingCap: true,
		})
		if err != nil || result.CheckpointID == "" ||
			result.EventsAppended != len(edits) {
			t.Fatalf("later checkpoint capture=%+v err=%v", result, err)
		}
		after, err := state.PublishableEvents(ctx, f.db, 0)
		if err != nil {
			t.Fatal(err)
		}
		return after[len(before):]
	}
	unrelatedEvents := capture(2, map[string]string{
		"fresh.go": "package source\n\nconst Fresh = 2\n",
	})
	if len(unrelatedEvents) != 1 {
		t.Fatalf("unrelated checkpoint events=%+v", unrelatedEvents)
	}
	unrelated = unrelatedEvents[0].Seq
	bridge := capture(3, map[string]string{
		"companion_test.go": "package source\n\nconst Companion = 2\n",
		"source_test.go": "package source\n\n" +
			"var Want = Value(\"complete\") + 1\n",
	})
	for _, event := range bridge {
		if event.Path == "source_test.go" {
			matching = append(matching, event.Seq)
		} else {
			checkpointSibling = append(checkpointSibling, event.Seq)
		}
	}
	continued := capture(4, map[string]string{
		"companion_test.go": "package source\n\nconst Companion = 3\n",
	})
	checkpointSibling = append(checkpointSibling, continued[0].Seq)
	if len(matching) != 1 || len(checkpointSibling) != 2 || unrelated == 0 {
		t.Fatalf("later matching=%v sibling=%v unrelated=%d",
			matching, checkpointSibling, unrelated)
	}
	return matching, checkpointSibling, unrelated
}

func semanticPrefixReplayOpts(
	planner ai.IntentPlanner,
	verify IntentCandidateVerifier,
) ReplayOpts {
	retryLimit := 0
	pathCoalescing := false
	return ReplayOpts{
		CommitStrategy:             ai.CommitStrategyIntent,
		IntentPlanner:              planner,
		IntentPreset:               config.PresetBalanced,
		IntentBypassBatchWait:      true,
		IntentWindow:               3,
		IntentMinPending:           1,
		IntentDeferLimit:           1,
		IntentRetryLimit:           &retryLimit,
		IntentPathCoalescing:       &pathCoalescing,
		IntentVerificationMode:     "fast",
		IntentCandidateVerify:      verify,
		RequireCompletedCheckpoint: true,
	}
}

func replaceIntentForwardRecoveryMarkerForTest(
	t *testing.T,
	f *captureFixture,
	recovery state.IntentForwardRecovery,
) {
	t.Helper()
	raw, err := json.Marshal(recovery)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.db.SQL().ExecContext(context.Background(), `
UPDATE daemon_meta SET value=? WHERE key='intent.v2.forward_recovery'`,
		string(raw)); err != nil {
		t.Fatal(err)
	}
}

func TestReplayLocalUnlockWidensResolvedSemanticPrefix(t *testing.T) {
	fixture := seedFailedCheckpointReplayFixture(t)
	f := fixture.capture
	ctx := context.Background()
	_, _ = seedSemanticPrefixRecovery(t, fixture)
	planner := &semanticPrefixMessagePlanner{}
	var verifiedSeqs [][]int64
	verify := func(
		_ context.Context,
		_ ai.IntentCandidateAssignment,
		captures []IntentCandidateCapture,
	) (IntentCandidateVerification, error) {
		seqs := make([]int64, 0, len(captures))
		for _, capture := range captures {
			seqs = append(seqs, capture.Event.Seq)
		}
		verifiedSeqs = append(verifiedSeqs, seqs)
		if len(captures) != len(fixture.seqs) {
			return IntentCandidateVerification{}, errors.New(
				"later semantic candidates are required for compilation")
		}
		return IntentCandidateVerification{Status: "passed", CheckedTS: 1}, nil
	}
	opts := semanticPrefixReplayOpts(planner, verify)
	opts.GitDir = f.gitDir

	first, err := Replay(ctx, f.dir, f.db, f.cctx, opts)
	if err != nil {
		t.Fatalf("first Replay: %v", err)
	}
	marker, active, err := state.IntentForwardRecoveryForPair(
		ctx, f.db, f.cctx.BranchRef, f.cctx.BranchGeneration)
	if err != nil || !active || marker.PrefixCursor != 2 ||
		marker.PlanFingerprint != "semantic-prefix-plan-test" {
		t.Fatalf("first marker=(%+v active=%t err=%v)", marker, active, err)
	}
	if first.RecoveryPrefixCandidateCount != 1 ||
		first.RecoveryPrefixTotalCandidates != 3 {
		t.Fatalf("first summary=%+v", first)
	}

	second, err := Replay(ctx, f.dir, f.db, f.cctx, opts)
	if err != nil {
		t.Fatalf("second Replay: %v", err)
	}
	marker, active, err = state.IntentForwardRecoveryForPair(
		ctx, f.db, f.cctx.BranchRef, f.cctx.BranchGeneration)
	if err != nil || !active || marker.PrefixCursor != 4 {
		t.Fatalf("second marker=(%+v active=%t err=%v)", marker, active, err)
	}
	if second.RecoveryPrefixCandidateCount != 2 ||
		second.RecoveryPrefixTotalCandidates != 3 {
		t.Fatalf("second summary=%+v", second)
	}

	third, err := Replay(ctx, f.dir, f.db, f.cctx, opts)
	if err != nil {
		t.Fatalf("third Replay: %v", err)
	}
	wantAttempts := [][]int64{
		{fixture.seqs[2]},
		{fixture.seqs[0], fixture.seqs[2]},
		fixture.seqs,
	}
	if !reflect.DeepEqual(verifiedSeqs, wantAttempts) {
		t.Fatalf("verified semantic prefixes=%v want %v",
			verifiedSeqs, wantAttempts)
	}
	if !reflect.DeepEqual(planner.rewriteSeqs, wantAttempts) {
		t.Fatalf("provider message prefixes=%v want %v",
			planner.rewriteSeqs, wantAttempts)
	}
	if third.Published != len(fixture.seqs) || third.Failed != 0 ||
		third.Conflicts != 0 {
		t.Fatalf("third summary=%+v", third)
	}
	if _, active, err := state.IntentForwardRecoveryForPair(
		ctx, f.db, f.cctx.BranchRef, f.cctx.BranchGeneration,
	); err != nil || active {
		t.Fatalf("completed marker active=%t err=%v", active, err)
	}
	if subject := strings.TrimSpace(mustGitOutput(
		t, f.dir, "log", "-1", "--format=%s")); subject != "Restore checkpoint compilation" {
		t.Fatalf("commit subject=%q want provider rewrite", subject)
	}
}

func TestReplayLocalUnlockStopsAfterFullSemanticPrefixFailure(t *testing.T) {
	fixture := seedFailedCheckpointReplayFixture(t)
	f := fixture.capture
	ctx := context.Background()
	_, _ = seedSemanticPrefixRecovery(t, fixture)
	planner := &semanticPrefixMessagePlanner{}
	verificationCalls := 0
	verify := func(
		context.Context,
		ai.IntentCandidateAssignment,
		[]IntentCandidateCapture,
	) (IntentCandidateVerification, error) {
		verificationCalls++
		return IntentCandidateVerification{}, errors.New(
			"semantic prefix still does not compile")
	}
	opts := semanticPrefixReplayOpts(planner, verify)
	opts.GitDir = f.gitDir
	var result ReplaySummary
	var err error
	for attempt := 0; attempt < 3; attempt++ {
		result, err = Replay(ctx, f.dir, f.db, f.cctx, opts)
		if err != nil {
			t.Fatalf("Replay attempt %d: %v", attempt+1, err)
		}
	}
	if result.Disposition != ReplayDispositionNeedsAttention ||
		result.SkippedReason !=
			"intent_forward_recovery_verification_needs_attention" ||
		result.HasMore {
		t.Fatalf("exhausted result=%+v", result)
	}
	marker, active, err := state.IntentForwardRecoveryForPair(
		ctx, f.db, f.cctx.BranchRef, f.cctx.BranchGeneration)
	if err != nil || !active || !marker.NeedsAttention ||
		!marker.PrefixExhausted || marker.PrefixCursor != 4 ||
		marker.AttentionReason != intentForwardRecoveryVerificationExhausted ||
		marker.ExpansionScannedThroughSeq != fixture.seqs[len(fixture.seqs)-1] {
		t.Fatalf("exhausted marker=(%+v active=%t err=%v)",
			marker, active, err)
	}
	if _, err := f.db.SQL().ExecContext(ctx, `DROP TABLE capture_ops`); err != nil {
		t.Fatal(err)
	}
	if _, changed, scannedThrough, err :=
		supersedingIntentForwardRecoveryTarget(ctx, f.db, marker); err != nil ||
		changed || scannedThrough != marker.ExpansionScannedThroughSeq {
		t.Fatalf("cached terminal scan=(changed=%t horizon=%d err=%v)",
			changed, scannedThrough, err)
	}
	stopped, err := Replay(ctx, f.dir, f.db, f.cctx, opts)
	if err != nil {
		t.Fatalf("stopped Replay: %v", err)
	}
	if verificationCalls != 3 ||
		stopped.Disposition != ReplayDispositionNeedsAttention ||
		stopped.HasMore {
		t.Fatalf("stopped result=%+v verification_calls=%d",
			stopped, verificationCalls)
	}
}

func TestSupersedingIntentForwardRecoveryTargetIncludesCompletePathChain(
	t *testing.T,
) {
	fixture := seedFailedCheckpointReplayFixture(t)
	recovery, _ := seedSemanticPrefixRecovery(t, fixture)
	matching, siblings, unrelated := appendLaterRecoverySnapshots(t, fixture)

	expanded, changed, _, err := supersedingIntentForwardRecoveryTarget(
		context.Background(), fixture.capture.db, recovery)
	if err != nil || !changed {
		t.Fatalf("expanded target=(%v changed=%t err=%v)",
			expanded, changed, err)
	}
	want := append(append([]int64(nil), fixture.seqs...), matching...)
	want = append(want, siblings...)
	sort.Slice(want, func(i, j int) bool { return want[i] < want[j] })
	if !reflect.DeepEqual(expanded, want) {
		t.Fatalf("expanded target=%v want %v", expanded, want)
	}
	if slices.Contains(expanded, unrelated) {
		t.Fatalf("expanded target included unrelated capture %d: %v",
			unrelated, expanded)
	}
}

func TestReplayFullSemanticPrefixExpandsToLaterCheckpointChain(t *testing.T) {
	fixture := seedFailedCheckpointReplayFixture(t)
	f := fixture.capture
	ctx := context.Background()
	_, _ = seedSemanticPrefixRecovery(t, fixture)
	matching, siblings, unrelated := appendLaterRecoverySnapshots(t, fixture)
	wantTarget := append(append([]int64(nil), fixture.seqs...), matching...)
	wantTarget = append(wantTarget, siblings...)
	sort.Slice(wantTarget, func(i, j int) bool { return wantTarget[i] < wantTarget[j] })

	prefixPlanner := &semanticPrefixMessagePlanner{}
	failVerification := func(
		context.Context,
		ai.IntentCandidateAssignment,
		[]IntentCandidateCapture,
	) (IntentCandidateVerification, error) {
		return IntentCandidateVerification{}, errors.New(
			"the frozen intermediate API snapshot does not compile")
	}
	prefixOpts := semanticPrefixReplayOpts(prefixPlanner, failVerification)
	prefixOpts.GitDir = f.gitDir
	var expandedSummary ReplaySummary
	for attempt := 0; attempt < 3; attempt++ {
		var err error
		expandedSummary, err = Replay(ctx, f.dir, f.db, f.cctx, prefixOpts)
		if err != nil {
			t.Fatalf("Replay prefix attempt %d: %v", attempt+1, err)
		}
	}
	if expandedSummary.SkippedReason !=
		"intent_forward_recovery_target_expanded" ||
		expandedSummary.Disposition != ReplayDispositionRecoverableStall ||
		!expandedSummary.HasMore {
		t.Fatalf("expanded prefix summary=%+v", expandedSummary)
	}
	marker, active, err := state.IntentForwardRecoveryForPair(
		ctx, f.db, f.cctx.BranchRef, f.cctx.BranchGeneration)
	if err != nil || !active || marker.Stage != publicationFallbackSemanticReplan ||
		marker.NeedsAttention || marker.PrefixExhausted ||
		!reflect.DeepEqual(marker.TargetEventSeqs, wantTarget) {
		t.Fatalf("expanded marker=(%+v active=%t err=%v), want target %v",
			marker, active, err, wantTarget)
	}

	planner := &expandedTargetRecoveryPlanner{}
	verifyComplete := func(
		_ context.Context,
		_ ai.IntentCandidateAssignment,
		captures []IntentCandidateCapture,
	) (IntentCandidateVerification, error) {
		seqs := make([]int64, 0, len(captures))
		for _, capture := range captures {
			seqs = append(seqs, capture.Event.Seq)
		}
		sort.Slice(seqs, func(i, j int) bool { return seqs[i] < seqs[j] })
		if !reflect.DeepEqual(seqs, wantTarget) {
			return IntentCandidateVerification{}, fmt.Errorf(
				"verification saw %v, want complete target %v", seqs, wantTarget)
		}
		return IntentCandidateVerification{Status: "passed", CheckedTS: 1}, nil
	}
	replanOpts := semanticPrefixReplayOpts(planner, verifyComplete)
	replanOpts.GitDir = f.gitDir
	result, err := Replay(ctx, f.dir, f.db, f.cctx, replanOpts)
	if err != nil || result.Published != len(wantTarget) || result.Failed != 0 ||
		result.Conflicts != 0 {
		t.Fatalf("expanded target replay=%+v err=%v", result, err)
	}
	if !reflect.DeepEqual(planner.offeredSeqs, [][]int64{wantTarget}) {
		t.Fatalf("semantic replan offers=%v want %v",
			planner.offeredSeqs, wantTarget)
	}
	if subject := strings.TrimSpace(mustGitOutput(
		t, f.dir, "log", "-1", "--format=%s")); subject != "Complete API transition" {
		t.Fatalf("expanded commit subject=%q", subject)
	}
	var unrelatedState string
	if err := f.db.ReadSQL().QueryRowContext(ctx,
		`SELECT state FROM capture_events WHERE seq=?`, unrelated).
		Scan(&unrelatedState); err != nil {
		t.Fatal(err)
	}
	if unrelatedState != state.EventStatePending {
		t.Fatalf("unrelated capture state=%q want pending", unrelatedState)
	}
}

func TestReplayReopensExhaustedRecoveryForLaterCheckpointChain(t *testing.T) {
	fixture := seedFailedCheckpointReplayFixture(t)
	f := fixture.capture
	ctx := context.Background()
	_, _ = seedSemanticPrefixRecovery(t, fixture)
	prefixOpts := semanticPrefixReplayOpts(
		&semanticPrefixMessagePlanner{},
		func(
			context.Context,
			ai.IntentCandidateAssignment,
			[]IntentCandidateCapture,
		) (IntentCandidateVerification, error) {
			return IntentCandidateVerification{}, errors.New(
				"the frozen intermediate API snapshot does not compile")
		},
	)
	prefixOpts.GitDir = f.gitDir
	for attempt := 0; attempt < 3; attempt++ {
		if _, err := Replay(ctx, f.dir, f.db, f.cctx, prefixOpts); err != nil {
			t.Fatalf("Replay prefix attempt %d: %v", attempt+1, err)
		}
	}
	exhausted, active, err := state.IntentForwardRecoveryForPair(
		ctx, f.db, f.cctx.BranchRef, f.cctx.BranchGeneration)
	if err != nil || !active || !exhausted.NeedsAttention ||
		!exhausted.PrefixExhausted {
		t.Fatalf("exhausted marker=(%+v active=%t err=%v)",
			exhausted, active, err)
	}

	matching, siblings, _ := appendLaterRecoverySnapshots(t, fixture)
	wantTarget := append(append([]int64(nil), fixture.seqs...), matching...)
	wantTarget = append(wantTarget, siblings...)
	sort.Slice(wantTarget, func(i, j int) bool { return wantTarget[i] < wantTarget[j] })
	planner := &expandedTargetRecoveryPlanner{}
	replanOpts := semanticPrefixReplayOpts(planner, func(
		_ context.Context,
		_ ai.IntentCandidateAssignment,
		captures []IntentCandidateCapture,
	) (IntentCandidateVerification, error) {
		if len(captures) != len(wantTarget) {
			return IntentCandidateVerification{}, fmt.Errorf(
				"verification capture count=%d want %d",
				len(captures), len(wantTarget))
		}
		return IntentCandidateVerification{Status: "passed", CheckedTS: 1}, nil
	})
	replanOpts.GitDir = f.gitDir
	result, err := Replay(ctx, f.dir, f.db, f.cctx, replanOpts)
	if err != nil || result.Published != len(wantTarget) ||
		result.Disposition != ReplayDispositionProgress {
		t.Fatalf("restart expansion replay=%+v err=%v", result, err)
	}
	if !reflect.DeepEqual(planner.offeredSeqs, [][]int64{wantTarget}) {
		t.Fatalf("restart semantic offers=%v want %v",
			planner.offeredSeqs, wantTarget)
	}
	if _, active, err := state.IntentForwardRecoveryForPair(
		ctx, f.db, f.cctx.BranchRef, f.cctx.BranchGeneration,
	); err != nil || active {
		t.Fatalf("restart recovery marker active=%t err=%v", active, err)
	}
}

func TestReplayResetsStalePrefixAfterCrashPublication(t *testing.T) {
	fixture := seedFailedCheckpointReplayFixture(t)
	f := fixture.capture
	ctx := context.Background()
	recovery, _ := seedSemanticPrefixRecovery(t, fixture)
	if recovery.PrefixUnresolvedCount != len(fixture.seqs) {
		t.Fatalf("prefix baseline=%d want %d",
			recovery.PrefixUnresolvedCount, len(fixture.seqs))
	}

	mustGitOutput(t, f.dir, "add", "source_test.go")
	mustGitOutput(t, f.dir, "commit", "-m", "Publish semantic prerequisite")
	newHead := strings.TrimSpace(mustGitOutput(t, f.dir, "rev-parse", "HEAD"))
	if _, err := f.db.SQL().ExecContext(ctx, `
UPDATE capture_events
SET state='published',published_ts=2,commit_oid=?
WHERE seq=?`, newHead, fixture.seqs[2]); err != nil {
		t.Fatal(err)
	}

	planner := &exactTargetReplanPlanner{}
	verify := func(
		context.Context,
		ai.IntentCandidateAssignment,
		[]IntentCandidateCapture,
	) (IntentCandidateVerification, error) {
		return IntentCandidateVerification{}, errors.New(
			"hold the replanned target for prefix recovery")
	}
	opts := semanticPrefixReplayOpts(planner, verify)
	opts.GitDir = f.gitDir
	restartCtx := f.cctx
	restartCtx.BaseHead = newHead
	result, err := Replay(ctx, f.dir, f.db, restartCtx, opts)
	if err != nil {
		t.Fatalf("Replay after crash boundary: %v", err)
	}
	marker, active, err := state.IntentForwardRecoveryForPair(
		ctx, f.db, f.cctx.BranchRef, f.cctx.BranchGeneration)
	if err != nil || !active || marker.Stage != publicationFallbackLocalUnlock ||
		marker.PrefixBaseHead != newHead || marker.PrefixUnresolvedCount != 2 ||
		marker.PrefixCursor != 1 || marker.PlanFingerprint == recovery.PlanFingerprint {
		t.Fatalf("restarted marker=(%+v active=%t err=%v)", marker, active, err)
	}
	wantOffered := [][]int64{{fixture.seqs[0], fixture.seqs[1]}}
	if !reflect.DeepEqual(planner.offeredSeqs, wantOffered) ||
		result.PlanFingerprint != marker.PlanFingerprint {
		t.Fatalf("replan offers=%v fingerprint=(%q,%q)",
			planner.offeredSeqs, result.PlanFingerprint, marker.PlanFingerprint)
	}
}

func TestReplayReplansPartiallyResolvedStoredCandidate(t *testing.T) {
	fixture := seedFailedCheckpointReplayFixture(t)
	f := fixture.capture
	ctx := context.Background()
	recovery, changed, err := state.StartFailedIntentCheckpointRecovery(
		ctx, f.db, f.cctx.BranchRef, f.cctx.BranchGeneration,
		fixture.seqs[0])
	if err != nil || !changed {
		t.Fatalf("start recovery=(%+v changed=%t err=%v)",
			recovery, changed, err)
	}
	if _, err := f.db.SQL().ExecContext(ctx, `
UPDATE capture_events
SET state='published',published_ts=1,commit_oid=?
WHERE seq=?`, f.cctx.BaseHead, fixture.seqs[2]); err != nil {
		t.Fatal(err)
	}
	plan := ai.IntentPlanV2{
		ProtocolVersion: ai.IntentPlannerProtocolV2,
		Candidates: []ai.IntentCandidateAssignment{
			semanticPlanTestCandidate(
				"partially-resolved", fixture.seqs[2], fixture.seqs[0]),
			semanticPlanTestCandidate("remaining", fixture.seqs[1]),
		},
	}
	run, err := state.EnsureIntentPlanRun(ctx, f.db, state.IntentPlanRun{
		Fingerprint: "partial-stored-plan", BranchRef: f.cctx.BranchRef,
		BranchGeneration: f.cctx.BranchGeneration, AttemptLimit: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := storeResolvedIntentPlanRun(&run, plan, nil); err != nil {
		t.Fatal(err)
	}
	run.Completed = true
	if err := state.UpdateIntentPlanRun(ctx, f.db, run); err != nil {
		t.Fatal(err)
	}
	recovery, err = state.AdvanceIntentForwardRecoveryPrefix(
		ctx, f.db, recovery, run.Fingerprint, f.cctx.BaseHead, 1)
	if err != nil || recovery.PrefixUnresolvedCount != 2 {
		t.Fatalf("advance partial recovery=%+v err=%v", recovery, err)
	}

	planner := &exactTargetReplanPlanner{}
	verify := func(
		context.Context,
		ai.IntentCandidateAssignment,
		[]IntentCandidateCapture,
	) (IntentCandidateVerification, error) {
		return IntentCandidateVerification{}, errors.New(
			"hold the replacement semantic plan")
	}
	opts := semanticPrefixReplayOpts(planner, verify)
	opts.GitDir = f.gitDir
	result, err := Replay(ctx, f.dir, f.db, f.cctx, opts)
	if err != nil {
		t.Fatalf("Replay partial plan: %v", err)
	}
	marker, active, err := state.IntentForwardRecoveryForPair(
		ctx, f.db, f.cctx.BranchRef, f.cctx.BranchGeneration)
	if err != nil || !active || marker.PlanFingerprint == run.Fingerprint ||
		marker.PlanFingerprint == "" || marker.PrefixCursor != 1 ||
		marker.PrefixUnresolvedCount != 2 {
		t.Fatalf("replacement marker=(%+v active=%t err=%v)",
			marker, active, err)
	}
	wantOffered := [][]int64{{fixture.seqs[0], fixture.seqs[1]}}
	if !reflect.DeepEqual(planner.offeredSeqs, wantOffered) ||
		result.PlanFingerprint != marker.PlanFingerprint {
		t.Fatalf("replacement offers=%v fingerprint=(%q,%q)",
			planner.offeredSeqs, result.PlanFingerprint, marker.PlanFingerprint)
	}
}

func TestReplayReplansInvalidCachedPrefixMarker(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*testing.T, *captureFixture, state.IntentForwardRecovery)
	}{
		{
			name: "missing resolved plan",
			mutate: func(
				t *testing.T,
				f *captureFixture,
				recovery state.IntentForwardRecovery,
			) {
				t.Helper()
				if _, err := f.db.SQL().ExecContext(context.Background(),
					`DELETE FROM intent_plan_runs WHERE fingerprint=?`,
					recovery.PlanFingerprint); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "missing progress baseline",
			mutate: func(
				t *testing.T,
				f *captureFixture,
				recovery state.IntentForwardRecovery,
			) {
				t.Helper()
				recovery.PrefixUnresolvedCount = 0
				replaceIntentForwardRecoveryMarkerForTest(t, f, recovery)
			},
		},
		{
			name: "missing prefix identity",
			mutate: func(
				t *testing.T,
				f *captureFixture,
				recovery state.IntentForwardRecovery,
			) {
				t.Helper()
				recovery.PrefixCursor = 0
				recovery.PrefixBaseHead = ""
				replaceIntentForwardRecoveryMarkerForTest(t, f, recovery)
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture := seedFailedCheckpointReplayFixture(t)
			f := fixture.capture
			ctx := context.Background()
			recovery, _ := seedSemanticPrefixRecovery(t, fixture)
			test.mutate(t, f, recovery)

			planner := &exactTargetReplanPlanner{}
			verify := func(
				context.Context,
				ai.IntentCandidateAssignment,
				[]IntentCandidateCapture,
			) (IntentCandidateVerification, error) {
				return IntentCandidateVerification{}, errors.New(
					"hold the replacement semantic plan")
			}
			opts := semanticPrefixReplayOpts(planner, verify)
			opts.GitDir = f.gitDir
			result, err := Replay(ctx, f.dir, f.db, f.cctx, opts)
			if err != nil {
				t.Fatalf("Replay invalid prefix: %v", err)
			}
			marker, active, err := state.IntentForwardRecoveryForPair(
				ctx, f.db, f.cctx.BranchRef, f.cctx.BranchGeneration)
			if err != nil || !active ||
				marker.PlanFingerprint == recovery.PlanFingerprint ||
				marker.PlanFingerprint == "" || marker.PrefixCursor != 1 ||
				marker.PrefixUnresolvedCount != len(fixture.seqs) {
				t.Fatalf("replacement marker=(%+v active=%t err=%v)",
					marker, active, err)
			}
			if !reflect.DeepEqual(planner.offeredSeqs, [][]int64{fixture.seqs}) ||
				result.PlanFingerprint != marker.PlanFingerprint {
				t.Fatalf("replacement offers=%v fingerprint=(%q,%q)",
					planner.offeredSeqs,
					result.PlanFingerprint, marker.PlanFingerprint)
			}
		})
	}
}

func TestReplayLegacyLocalUnlockReplansBeforeSelectingPrefix(t *testing.T) {
	fixture := seedFailedCheckpointReplayFixture(t)
	f := fixture.capture
	ctx := context.Background()
	recovery, changed, err := state.StartFailedIntentCheckpointRecovery(
		ctx, f.db, f.cctx.BranchRef, f.cctx.BranchGeneration,
		fixture.seqs[0])
	if err != nil || !changed {
		t.Fatalf("start recovery=(%+v changed=%t err=%v)",
			recovery, changed, err)
	}
	recovery, err = state.AdvanceIntentForwardRecovery(
		ctx, f.db, recovery, publicationFallbackLocalUnlock, 0)
	if err != nil {
		t.Fatal(err)
	}
	planner := &failedCheckpointRecoveryPlanner{
		stalledCandidateID: stalledCheckpointCandidateID,
	}
	verify := func(
		context.Context,
		ai.IntentCandidateAssignment,
		[]IntentCandidateCapture,
	) (IntentCandidateVerification, error) {
		return IntentCandidateVerification{}, errors.New(
			"force one semantic no-progress transition")
	}
	opts := semanticPrefixReplayOpts(planner, verify)
	opts.GitDir = f.gitDir
	result, err := Replay(ctx, f.dir, f.db, f.cctx, opts)
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	marker, active, err := state.IntentForwardRecoveryForPair(
		ctx, f.db, f.cctx.BranchRef, f.cctx.BranchGeneration)
	if err != nil || !active || marker.Stage != publicationFallbackLocalUnlock ||
		marker.PlanFingerprint == "" || marker.PrefixCursor != 1 ||
		marker.PrefixBaseHead != f.cctx.BaseHead {
		t.Fatalf("replanned marker=(%+v active=%t err=%v)", marker, active, err)
	}
	if len(planner.offeredSeqs) != 1 ||
		!reflect.DeepEqual(planner.offeredSeqs[0], fixture.seqs) ||
		result.PlanFingerprint != marker.PlanFingerprint {
		t.Fatalf("provider offers=%v result fingerprint=%q marker=%q",
			planner.offeredSeqs, result.PlanFingerprint, marker.PlanFingerprint)
	}
}

func TestReplayIntentCandidateRecoversBeforeForcedPreflightFailure(t *testing.T) {
	fixture := seedFailedCheckpointReplayFixture(t)
	f := fixture.capture
	seqs := fixture.seqs
	ctx := context.Background()
	oldSeq, err := state.AppendCaptureEvent(ctx, f.db, state.CaptureEvent{
		BranchRef: f.cctx.BranchRef, BranchGeneration: f.cctx.BranchGeneration - 1,
		BaseHead: f.cctx.BaseHead, Operation: "modify",
		Path: "resolved-old-generation.go", Fidelity: "exact",
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.db.SQL().ExecContext(ctx, `
UPDATE capture_events SET state='recovered',published_ts=1 WHERE seq=?`,
		oldSeq); err != nil {
		t.Fatal(err)
	}
	oldRecovery := state.IntentForwardRecovery{
		BranchRef:        f.cctx.BranchRef,
		BranchGeneration: f.cctx.BranchGeneration - 1,
		CandidateID:      "resolved-old-generation-candidate",
		Reason:           "repair_commit_outside_suffix", Stage: "local_unlock",
		TargetEventSeqs: []int64{oldSeq},
	}
	oldRecoveryRaw, err := json.Marshal(oldRecovery)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.db.SQL().ExecContext(ctx, `
INSERT INTO daemon_meta(key,value,updated_ts) VALUES(?,?,1)`,
		"intent.v2.forward_recovery", string(oldRecoveryRaw)); err != nil {
		t.Fatal(err)
	}

	planner := &failedCheckpointRecoveryPlanner{
		stalledCandidateID: stalledCheckpointCandidateID,
	}
	var verifiedCounts []int
	verify := func(
		_ context.Context,
		_ ai.IntentCandidateAssignment,
		captures []IntentCandidateCapture,
	) (IntentCandidateVerification, error) {
		verifiedCounts = append(verifiedCounts, len(captures))
		if len(captures) != 3 {
			return IntentCandidateVerification{},
				errors.New("missing source_test.go companion")
		}
		return IntentCandidateVerification{
			Status: "passed", CheckedTS: 1,
		}, nil
	}
	retryLimit := 0
	pathCoalescing := false
	headBefore := f.cctx.BaseHead
	result, err := Replay(ctx, f.dir, f.db, f.cctx, ReplayOpts{
		GitDir: f.gitDir, CommitStrategy: ai.CommitStrategyIntent,
		IntentPlanner: planner, IntentPreset: config.PresetBalanced,
		IntentBypassBatchWait: true, IntentWindow: 2, IntentMinPending: 1,
		IntentDeferLimit: 1, IntentRetryLimit: &retryLimit,
		IntentPathCoalescing:   &pathCoalescing,
		IntentVerificationMode: "fast", IntentCandidateVerify: verify,
		RequireCompletedCheckpoint: true,
	})
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if result.Published != 3 || result.Conflicts != 0 || result.Failed != 0 {
		t.Fatalf("replay result=%+v", result)
	}
	if !reflect.DeepEqual(planner.offeredCounts, []int{3}) {
		t.Fatalf("planner offered counts=%v want [3]", planner.offeredCounts)
	}
	if !reflect.DeepEqual(verifiedCounts, []int{3}) {
		t.Fatalf("verified candidate sizes=%v want [3]", verifiedCounts)
	}

	headAfter, err := git.RevParse(ctx, f.dir, "HEAD")
	if err != nil {
		t.Fatal(err)
	}
	countRaw, err := git.Run(ctx, git.RunOpts{Dir: f.dir},
		"rev-list", "--count", headBefore+".."+headAfter)
	if err != nil {
		t.Fatal(err)
	}
	commitCount, err := strconv.Atoi(strings.TrimSpace(string(countRaw)))
	if err != nil || commitCount != 1 {
		t.Fatalf("recovery commits=%q parsed=%d err=%v want 1",
			strings.TrimSpace(string(countRaw)), commitCount, err)
	}

	stalled, ok, err := state.IntentCandidateByID(
		ctx, f.db, stalledCheckpointCandidateID)
	if err != nil || !ok || stalled.Status != state.IntentCandidateSuperseded ||
		len(stalled.Events) != 0 {
		t.Fatalf("stalled candidate=(%+v ok=%t err=%v)", stalled, ok, err)
	}
	if _, active, err := state.IntentForwardRecoveryForPair(
		ctx, f.db, f.cctx.BranchRef, f.cctx.BranchGeneration,
	); err != nil || active {
		t.Fatalf("forward recovery marker active=%t err=%v", active, err)
	}
	if _, active, err := state.IntentForwardRecoveryForPair(
		ctx, f.db, oldRecovery.BranchRef, oldRecovery.BranchGeneration,
	); err != nil || active {
		t.Fatalf("old generation recovery marker active=%t err=%v", active, err)
	}
	var publishedCommit string
	for _, seq := range seqs {
		var eventState string
		var commitOID sql.NullString
		if err := f.db.ReadSQL().QueryRowContext(ctx, `
SELECT state,commit_oid FROM capture_events WHERE seq=?`, seq).
			Scan(&eventState, &commitOID); err != nil {
			t.Fatal(err)
		}
		if eventState != state.EventStatePublished || !commitOID.Valid ||
			commitOID.String == "" {
			t.Fatalf("event %d state=%q commit=%+v", seq, eventState, commitOID)
		}
		if publishedCommit == "" {
			publishedCommit = commitOID.String
		} else if commitOID.String != publishedCommit {
			t.Fatalf("event %d commit=%s want %s", seq, commitOID.String, publishedCommit)
		}
	}
	if publishedCommit != headAfter || result.BaseHead != headAfter {
		t.Fatalf("published commit=%s result head=%s git head=%s",
			publishedCommit, result.BaseHead, headAfter)
	}
	if status := strings.TrimSpace(mustGitOutput(
		t, f.dir, "status", "--short")); status != "" {
		t.Fatalf("recovery left worktree dirty: %s", status)
	}
}

func TestReplayFailedCheckpointRecoveryPrecedesFreshCapture(t *testing.T) {
	fixture := seedFailedCheckpointReplayFixture(t)
	f := fixture.capture
	ctx := context.Background()
	if err := os.WriteFile(filepath.Join(f.dir, "fresh.go"),
		[]byte("package source\n\nconst Fresh = 2\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	checkpointStore := checkpointpkg.Store{DB: f.db}
	capture, err := Capture(ctx, f.dir, f.db, f.cctx, CaptureOpts{
		IgnoreChecker: f.ig, SensitiveMatcher: f.matcher,
		CheckpointStore: &checkpointStore, WorktreeID: checkpointpkg.WorktreeID(f.dir),
		ObservationEpoch: 2, CheckpointReason: state.CheckpointReasonPoll,
		SortByPath: true, DisablePendingCap: true,
	})
	if err != nil || capture.CheckpointID == "" || capture.EventsAppended != 1 {
		t.Fatalf("fresh checkpoint capture=%+v err=%v", capture, err)
	}
	pending, err := state.PublishableEvents(ctx, f.db, 0)
	if err != nil || len(pending) != 4 {
		t.Fatalf("publishable events=%+v err=%v", pending, err)
	}
	freshSeq := pending[3].Seq
	if freshSeq <= fixture.seqs[2] || pending[3].Path != "fresh.go" {
		t.Fatalf("fresh event=%+v old target=%v", pending[3], fixture.seqs)
	}
	// Give the unrelated capture an earlier planner timestamp than the failed
	// checkpoint. A generic forced-aging lookup will select this row first;
	// checkpoint recovery must still prioritize the older failed candidate.
	if err := state.RecordPlannerDefer(ctx, f.db, freshSeq, 0.5,
		"unrelated capture was considered earlier"); err != nil {
		t.Fatal(err)
	}

	planner := &failedCheckpointRecoveryPlanner{
		stalledCandidateID: stalledCheckpointCandidateID,
	}
	var verifiedCounts []int
	verify := func(
		_ context.Context,
		_ ai.IntentCandidateAssignment,
		captures []IntentCandidateCapture,
	) (IntentCandidateVerification, error) {
		verifiedCounts = append(verifiedCounts, len(captures))
		if len(captures) != 3 {
			return IntentCandidateVerification{},
				errors.New("fresh capture planned before failed checkpoint recovery")
		}
		return IntentCandidateVerification{Status: "passed", CheckedTS: 1}, nil
	}
	retryLimit := 0
	pathCoalescing := false
	result, err := Replay(ctx, f.dir, f.db, f.cctx, ReplayOpts{
		GitDir: f.gitDir, CommitStrategy: ai.CommitStrategyIntent,
		IntentPlanner: planner, IntentPreset: config.PresetBalanced,
		IntentBypassBatchWait: true, IntentWindow: 2, IntentMinPending: 1,
		IntentDeferLimit: 1, IntentRetryLimit: &retryLimit,
		IntentPathCoalescing:   &pathCoalescing,
		IntentVerificationMode: "fast", IntentCandidateVerify: verify,
		RequireCompletedCheckpoint: true,
	})
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if result.Published != 3 || result.Conflicts != 0 || result.Failed != 0 {
		t.Fatalf("replay result=%+v", result)
	}
	if !reflect.DeepEqual(planner.offeredSeqs, [][]int64{fixture.seqs}) {
		t.Fatalf("planner offered seqs=%v want old target %v",
			planner.offeredSeqs, fixture.seqs)
	}
	if !reflect.DeepEqual(verifiedCounts, []int{3}) {
		t.Fatalf("verified candidate sizes=%v want [3]", verifiedCounts)
	}
	for _, seq := range fixture.seqs {
		var eventState string
		if err := f.db.ReadSQL().QueryRowContext(ctx,
			`SELECT state FROM capture_events WHERE seq=?`, seq).
			Scan(&eventState); err != nil {
			t.Fatal(err)
		}
		if eventState != state.EventStatePublished {
			t.Fatalf("old target event %d state=%q want published", seq, eventState)
		}
	}
	var freshState string
	if err := f.db.ReadSQL().QueryRowContext(ctx,
		`SELECT state FROM capture_events WHERE seq=?`, freshSeq).
		Scan(&freshState); err != nil {
		t.Fatal(err)
	}
	if freshState != state.EventStatePending {
		t.Fatalf("fresh event %d state=%q want pending", freshSeq, freshState)
	}
	if _, active, err := state.IntentForwardRecoveryForPair(
		ctx, f.db, f.cctx.BranchRef, f.cctx.BranchGeneration,
	); err != nil || active {
		t.Fatalf("forward recovery marker active=%t err=%v", active, err)
	}
	if changed := strings.TrimSpace(mustGitOutput(
		t, f.dir, "diff", "--name-only")); changed != "fresh.go" {
		t.Fatalf("remaining worktree paths=%q want fresh.go", changed)
	}
}

func TestReplayFailedCheckpointRecoveryWaitsForPathQuiescence(t *testing.T) {
	ResetPathQuiescenceForTest(t)
	t.Setenv(EnvPathQuiescenceSeconds, "30")
	_ = resolvePathQuiescenceSeconds()
	now := time.Date(2026, 8, 29, 1, 0, 0, 0, time.UTC)
	SetPathQuiescenceClockForTest(t, func() time.Time { return now })
	t.Cleanup(func() { SetPathQuiescenceClockForTest(t, nil) })

	fixture := seedFailedCheckpointReplayFixture(t)
	f := fixture.capture
	ctx := context.Background()
	planner := &failedCheckpointRecoveryPlanner{
		stalledCandidateID: stalledCheckpointCandidateID,
	}
	retryLimit := 0
	pathCoalescing := false
	result, err := Replay(ctx, f.dir, f.db, f.cctx, ReplayOpts{
		GitDir: f.gitDir, CommitStrategy: ai.CommitStrategyIntent,
		IntentPlanner: planner, IntentPreset: config.PresetBalanced,
		IntentBypassBatchWait: true, IntentWindow: 2, IntentMinPending: 1,
		IntentDeferLimit: 1, IntentRetryLimit: &retryLimit,
		IntentPathCoalescing:       &pathCoalescing,
		IntentVerificationMode:     "fast",
		RequireCompletedCheckpoint: true,
	})
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if !result.Skipped ||
		result.SkippedReason != "skipped_due_path_quiescence" ||
		result.Published != 0 {
		t.Fatalf("quiescence recovery result=%+v", result)
	}
	if len(planner.offeredSeqs) != 0 {
		t.Fatalf("planner offered hot checkpoint=%v", planner.offeredSeqs)
	}
	candidate, ok, err := state.IntentCandidateByID(
		ctx, f.db, stalledCheckpointCandidateID)
	if err != nil || !ok || candidate.Status != state.IntentCandidateWaiting ||
		len(candidate.Events) != 2 {
		t.Fatalf("hot candidate changed=(%+v ok=%t err=%v)", candidate, ok, err)
	}
	if _, active, err := state.IntentForwardRecoveryForPair(
		ctx, f.db, f.cctx.BranchRef, f.cctx.BranchGeneration,
	); err != nil || active {
		t.Fatalf("hot recovery marker active=%t err=%v", active, err)
	}
	for _, seq := range fixture.seqs {
		var eventState string
		if err := f.db.ReadSQL().QueryRowContext(ctx,
			`SELECT state FROM capture_events WHERE seq=?`, seq).
			Scan(&eventState); err != nil {
			t.Fatal(err)
		}
		if eventState != state.EventStatePending {
			t.Fatalf("hot event %d state=%q want pending", seq, eventState)
		}
	}
}

func TestReplayFailedCheckpointRecoveryWaitsForEveryTargetPath(t *testing.T) {
	ResetPathQuiescenceForTest(t)
	t.Setenv(EnvPathQuiescenceSeconds, "30")
	_ = resolvePathQuiescenceSeconds()
	now := time.Date(2026, 8, 29, 2, 0, 0, 0, time.UTC)
	SetPathQuiescenceClockForTest(t, func() time.Time { return now })
	t.Cleanup(func() { SetPathQuiescenceClockForTest(t, nil) })

	fixture := seedFailedCheckpointReplayFixture(t)
	f := fixture.capture
	ctx := context.Background()
	now = now.Add(31 * time.Second)
	RecordPathWrite("source_test.go", now)

	planner := &failedCheckpointRecoveryPlanner{
		stalledCandidateID: stalledCheckpointCandidateID,
	}
	var verifiedCounts []int
	verify := func(
		_ context.Context,
		_ ai.IntentCandidateAssignment,
		captures []IntentCandidateCapture,
	) (IntentCandidateVerification, error) {
		verifiedCounts = append(verifiedCounts, len(captures))
		if len(captures) != len(fixture.seqs) {
			return IntentCandidateVerification{},
				errors.New("incomplete checkpoint recovery target")
		}
		return IntentCandidateVerification{Status: "passed", CheckedTS: 1}, nil
	}
	retryLimit := 0
	pathCoalescing := false
	opts := ReplayOpts{
		GitDir: f.gitDir, CommitStrategy: ai.CommitStrategyIntent,
		IntentPlanner: planner, IntentPreset: config.PresetBalanced,
		IntentBypassBatchWait: true, IntentWindow: 2, IntentMinPending: 1,
		IntentDeferLimit: 1, IntentRetryLimit: &retryLimit,
		IntentPathCoalescing:   &pathCoalescing,
		IntentVerificationMode: "fast", IntentCandidateVerify: verify,
		RequireCompletedCheckpoint: true,
	}

	hot, err := Replay(ctx, f.dir, f.db, f.cctx, opts)
	if err != nil {
		t.Fatalf("Replay hot target: %v", err)
	}
	if !hot.Skipped ||
		hot.SkippedReason != "skipped_due_path_quiescence" ||
		hot.Published != 0 || !hot.HasMore {
		t.Fatalf("hot target result=%+v", hot)
	}
	if len(planner.offeredSeqs) != 0 || len(verifiedCounts) != 0 {
		t.Fatalf("hot target planned=%v verified=%v",
			planner.offeredSeqs, verifiedCounts)
	}
	recovery, active, err := state.IntentForwardRecoveryForPair(
		ctx, f.db, f.cctx.BranchRef, f.cctx.BranchGeneration)
	if err != nil || !active ||
		recovery.Stage != publicationFallbackSemanticReplan ||
		recovery.UnlockCount != 0 ||
		!reflect.DeepEqual(recovery.TargetEventSeqs, fixture.seqs) {
		t.Fatalf("hot recovery=(%+v active=%t err=%v)", recovery, active, err)
	}
	for _, seq := range fixture.seqs {
		var eventState string
		if err := f.db.ReadSQL().QueryRowContext(ctx,
			`SELECT state FROM capture_events WHERE seq=?`, seq).
			Scan(&eventState); err != nil {
			t.Fatal(err)
		}
		if eventState != state.EventStatePending {
			t.Fatalf("hot event %d state=%q want pending", seq, eventState)
		}
	}

	now = now.Add(31 * time.Second)
	settled, err := Replay(ctx, f.dir, f.db, f.cctx, opts)
	if err != nil {
		t.Fatalf("Replay settled target: %v", err)
	}
	if settled.Published != len(fixture.seqs) || settled.Failed != 0 ||
		settled.Conflicts != 0 {
		t.Fatalf("settled target result=%+v", settled)
	}
	if !reflect.DeepEqual(planner.offeredSeqs, [][]int64{fixture.seqs}) ||
		!reflect.DeepEqual(verifiedCounts, []int{len(fixture.seqs)}) {
		t.Fatalf("settled target planned=%v verified=%v",
			planner.offeredSeqs, verifiedCounts)
	}
	if _, active, err := state.IntentForwardRecoveryForPair(
		ctx, f.db, f.cctx.BranchRef, f.cctx.BranchGeneration,
	); err != nil || active {
		t.Fatalf("settled recovery marker active=%t err=%v", active, err)
	}
}

func TestUpdateIntentForwardRecoveryRetainsPartiallyResolvedTarget(t *testing.T) {
	fixture := seedFailedCheckpointReplayFixture(t)
	f := fixture.capture
	ctx := context.Background()
	recovery, changed, err := state.StartFailedIntentCheckpointRecovery(
		ctx, f.db, f.cctx.BranchRef, f.cctx.BranchGeneration, fixture.seqs[0])
	if err != nil || !changed {
		t.Fatalf("start recovery=(%+v changed=%t err=%v)",
			recovery, changed, err)
	}
	if _, err := f.db.SQL().ExecContext(ctx, `
UPDATE capture_events
SET state='published',commit_oid='partial-publication',published_ts=2
WHERE seq IN (?,?)`, fixture.seqs[0], fixture.seqs[1]); err != nil {
		t.Fatal(err)
	}

	partial, err := updateIntentForwardRecoveryAfterReplay(
		ctx, f.db, recovery, ReplaySummary{Published: 2}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !partial.HasMore || partial.Published != 2 {
		t.Fatalf("partial recovery summary=%+v want published=2 has_more", partial)
	}
	marker, active, err := state.IntentForwardRecoveryForPair(
		ctx, f.db, f.cctx.BranchRef, f.cctx.BranchGeneration)
	if err != nil || !active ||
		!reflect.DeepEqual(marker.TargetEventSeqs, recovery.TargetEventSeqs) {
		t.Fatalf("partial marker=(%+v active=%t err=%v), want target %v",
			marker, active, err, recovery.TargetEventSeqs)
	}
	var remainingState string
	if err := f.db.ReadSQL().QueryRowContext(ctx,
		`SELECT state FROM capture_events WHERE seq=?`, fixture.seqs[2]).
		Scan(&remainingState); err != nil {
		t.Fatal(err)
	}
	if remainingState != state.EventStatePending {
		t.Fatalf("remaining target state=%q want pending", remainingState)
	}

	if _, err := f.db.SQL().ExecContext(ctx, `
UPDATE capture_events
SET state='recovered',commit_oid=NULL,published_ts=3
WHERE seq=?`, fixture.seqs[2]); err != nil {
		t.Fatal(err)
	}
	completed, err := updateIntentForwardRecoveryAfterReplay(
		ctx, f.db, marker, ReplaySummary{Published: 1}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if completed.Published != 1 {
		t.Fatalf("completed recovery summary=%+v", completed)
	}
	if _, active, err := state.IntentForwardRecoveryForPair(
		ctx, f.db, f.cctx.BranchRef, f.cctx.BranchGeneration,
	); err != nil || active {
		t.Fatalf("completed marker active=%t err=%v", active, err)
	}
}

func TestUpdateIntentForwardRecoveryPreservesTransientWaitStage(t *testing.T) {
	fixture := seedFailedCheckpointReplayFixture(t)
	f := fixture.capture
	ctx := context.Background()
	recovery, changed, err := state.StartFailedIntentCheckpointRecovery(
		ctx, f.db, f.cctx.BranchRef, f.cctx.BranchGeneration, fixture.seqs[0])
	if err != nil || !changed {
		t.Fatalf("start recovery=(%+v changed=%t err=%v)",
			recovery, changed, err)
	}

	waiting, err := updateIntentForwardRecoveryAfterReplay(
		ctx, f.db, recovery, ReplaySummary{
			Skipped:           true,
			SkippedReason:     "intent_v2_repair_skipped_head_changed",
			Disposition:       ReplayDispositionTransientWait,
			DispositionReason: "HEAD changed",
		}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !waiting.HasMore || waiting.Published != 0 {
		t.Fatalf("transient recovery summary=%+v", waiting)
	}
	marker, active, err := state.IntentForwardRecoveryForPair(
		ctx, f.db, f.cctx.BranchRef, f.cctx.BranchGeneration)
	if err != nil || !active ||
		marker.Stage != publicationFallbackSemanticReplan ||
		marker.UnlockCount != 0 {
		t.Fatalf("transient marker=(%+v active=%t err=%v)", marker, active, err)
	}
}

func TestUpdateIntentForwardRecoveryPreservesProviderWaitStage(t *testing.T) {
	fixture := seedFailedCheckpointReplayFixture(t)
	f := fixture.capture
	ctx := context.Background()
	recovery, changed, err := state.StartFailedIntentCheckpointRecovery(
		ctx, f.db, f.cctx.BranchRef, f.cctx.BranchGeneration, fixture.seqs[0])
	if err != nil || !changed {
		t.Fatalf("start recovery=(%+v changed=%t err=%v)",
			recovery, changed, err)
	}

	waitErr := &IntentPlannerCircuitOpenError{
		RetryAt: time.Unix(30, 0).UTC(),
	}
	waiting, err := updateIntentForwardRecoveryAfterReplay(
		ctx, f.db, recovery, ReplaySummary{},
		&IntentSemanticFallbackRequiredError{
			Failure: waitErr.Error(), plannerWait: waitErr,
		})
	if err != nil {
		t.Fatal(err)
	}
	if !waiting.HasMore || !waiting.Skipped ||
		waiting.Disposition != ReplayDispositionTransientWait ||
		waiting.SkippedReason != "intent_v2_provider_wait" {
		t.Fatalf("provider wait summary=%+v", waiting)
	}
	marker, active, err := state.IntentForwardRecoveryForPair(
		ctx, f.db, f.cctx.BranchRef, f.cctx.BranchGeneration)
	if err != nil || !active ||
		marker.Stage != publicationFallbackSemanticReplan ||
		marker.UnlockCount != 0 || marker.NeedsAttention {
		t.Fatalf("provider wait marker=(%+v active=%t err=%v)",
			marker, active, err)
	}
}

func TestReplayClearsResolvedOlderIntentCheckpointRecoveryAfterRestart(t *testing.T) {
	f := newCaptureFixture(t)
	ctx := context.Background()
	for path, contents := range map[string]string{
		"restart.go":      "package restart\n\nconst Value = 1\n",
		"restart_test.go": "package restart\n\nconst Want = 1\n",
	} {
		seedTrackedFileCommit(t, ctx, f, path, contents)
	}
	if _, err := BootstrapShadow(ctx, f.dir, f.db, f.cctx); err != nil {
		t.Fatal(err)
	}
	for path, contents := range map[string]string{
		"restart.go":      "package restart\n\nconst Value = 2\n",
		"restart_test.go": "package restart\n\nconst Want = 2\n",
	} {
		if err := os.WriteFile(filepath.Join(f.dir, path), []byte(contents), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	checkpointStore := checkpointpkg.Store{DB: f.db}
	capture, err := Capture(ctx, f.dir, f.db, f.cctx, CaptureOpts{
		IgnoreChecker: f.ig, SensitiveMatcher: f.matcher,
		CheckpointStore: &checkpointStore, WorktreeID: checkpointpkg.WorktreeID(f.dir),
		ObservationEpoch: 1, CheckpointReason: state.CheckpointReasonPoll,
		SortByPath: true, DisablePendingCap: true,
	})
	if err != nil || capture.CheckpointID == "" || capture.EventsAppended != 2 {
		t.Fatalf("checkpoint capture=%+v err=%v", capture, err)
	}
	pending, err := state.PublishableEvents(ctx, f.db, 0)
	if err != nil || len(pending) != 2 {
		t.Fatalf("publishable events=%+v err=%v", pending, err)
	}
	seqs := []int64{pending[0].Seq, pending[1].Seq}
	const candidateID = "checkpoint-restart-candidate"
	if err := state.SaveIntentCandidate(ctx, f.db, state.IntentCandidate{
		ID: candidateID, BranchRef: f.cctx.BranchRef,
		BranchGeneration: f.cctx.BranchGeneration,
		Status:           state.IntentCandidateWaiting, Readiness: state.IntentReadinessWait,
		Purpose:            "complete the restart source change",
		VerificationStatus: sql.NullString{String: "failed", Valid: true},
		Events: []state.IntentCandidateEvent{{
			EventSeq: seqs[0], EventRole: "implementation",
		}},
	}); err != nil {
		t.Fatal(err)
	}
	recovery, changed, err := state.StartFailedIntentCheckpointRecovery(
		ctx, f.db, f.cctx.BranchRef, f.cctx.BranchGeneration, seqs[0])
	if err != nil || !changed || !reflect.DeepEqual(recovery.TargetEventSeqs, seqs) {
		t.Fatalf("start recovery=(%+v changed=%t err=%v), want target %v",
			recovery, changed, err, seqs)
	}
	if _, err := f.db.SQL().ExecContext(ctx, `
UPDATE capture_events
SET state=CASE WHEN seq=? THEN 'published' ELSE 'recovered' END,
    commit_oid=CASE WHEN seq=? THEN ? ELSE NULL END,
    published_ts=1
WHERE seq IN (?,?)`, seqs[0], seqs[0], f.cctx.BaseHead,
		seqs[0], seqs[1]); err != nil {
		t.Fatal(err)
	}
	newGenerationCtx := f.cctx
	newGenerationCtx.BranchGeneration++

	headBefore := strings.TrimSpace(mustGitOutput(t, f.dir, "rev-parse", "HEAD"))
	statusBefore := mustGitOutput(t, f.dir, "status", "--porcelain")
	diffBefore := mustGitOutput(t, f.dir, "diff", "--binary")
	result, err := Replay(ctx, f.dir, f.db, newGenerationCtx, ReplayOpts{
		GitDir: f.gitDir, CommitStrategy: ai.CommitStrategyIntent,
		IntentPlanner: ai.DeterministicProvider{}, IntentPreset: config.PresetBalanced,
		RequireCompletedCheckpoint: true,
	})
	if err != nil || result.Published != 0 || result.Conflicts != 0 ||
		result.Failed != 0 || result.Disposition != ReplayDispositionIdle {
		t.Fatalf("restart replay=%+v err=%v", result, err)
	}
	if _, active, err := state.IntentForwardRecoveryForPair(
		ctx, f.db, f.cctx.BranchRef, f.cctx.BranchGeneration,
	); err != nil || active {
		t.Fatalf("forward recovery marker active=%t err=%v", active, err)
	}
	if headAfter := strings.TrimSpace(mustGitOutput(t, f.dir, "rev-parse", "HEAD")); headAfter != headBefore {
		t.Fatalf("HEAD changed from %s to %s", headBefore, headAfter)
	}
	if statusAfter := mustGitOutput(t, f.dir, "status", "--porcelain"); statusAfter != statusBefore {
		t.Fatalf("worktree status changed from %q to %q", statusBefore, statusAfter)
	}
	if diffAfter := mustGitOutput(t, f.dir, "diff", "--binary"); diffAfter != diffBefore {
		t.Fatal("worktree diff changed while clearing resolved recovery marker")
	}
}
