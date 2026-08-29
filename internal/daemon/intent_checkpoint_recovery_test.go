package daemon

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/ai"
	checkpointpkg "github.com/KristjanPikhof/Auto-Commit-Daemon/internal/checkpoint"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/config"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
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

func seedFailedCheckpointReplayFixture(t *testing.T) failedCheckpointReplayFixture {
	t.Helper()
	f := newCaptureFixture(t)
	ctx := context.Background()
	for path, contents := range map[string]string{
		"fresh.go":          "package source\n\nconst Fresh = 1\n",
		"source.go":         "package source\n\nconst Value = 1\n",
		"source_fixture.go": "package source\n\nconst Fixture = 1\n",
		"source_test.go":    "package source\n\nconst Want = 1\n",
	} {
		seedTrackedFileCommit(t, ctx, f, path, contents)
	}
	if _, err := BootstrapShadow(ctx, f.dir, f.db, f.cctx); err != nil {
		t.Fatal(err)
	}
	for path, contents := range map[string]string{
		"source.go":         "package source\n\nconst Value = 2\n",
		"source_fixture.go": "package source\n\nconst Fixture = 2\n",
		"source_test.go":    "package source\n\nconst Want = 2\n",
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

func TestReplayIntentCandidateRecoversBeforeForcedPreflightFailure(t *testing.T) {
	fixture := seedFailedCheckpointReplayFixture(t)
	f := fixture.capture
	seqs := fixture.seqs
	ctx := context.Background()

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

func TestReplayClearsResolvedIntentCheckpointRecoveryAfterRestart(t *testing.T) {
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

	headBefore := strings.TrimSpace(mustGitOutput(t, f.dir, "rev-parse", "HEAD"))
	statusBefore := mustGitOutput(t, f.dir, "status", "--porcelain")
	diffBefore := mustGitOutput(t, f.dir, "diff", "--binary")
	result, err := Replay(ctx, f.dir, f.db, f.cctx, ReplayOpts{
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
