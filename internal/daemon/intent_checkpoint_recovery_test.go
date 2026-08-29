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

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/ai"
	checkpointpkg "github.com/KristjanPikhof/Auto-Commit-Daemon/internal/checkpoint"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/config"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

type failedCheckpointRecoveryPlanner struct {
	stalledCandidateID string
	offeredCounts      []int
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

func TestReplayIntentCandidateRecoversFailedCheckpointCompanion(t *testing.T) {
	f := newCaptureFixture(t)
	ctx := context.Background()
	for path, contents := range map[string]string{
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

	const stalledCandidateID = "checkpoint-stalled-candidate"
	if err := state.SaveIntentCandidate(ctx, f.db, state.IntentCandidate{
		ID: stalledCandidateID, BranchRef: f.cctx.BranchRef,
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

	planner := &failedCheckpointRecoveryPlanner{
		stalledCandidateID: stalledCandidateID,
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
	if !reflect.DeepEqual(planner.offeredCounts, []int{1, 3}) {
		t.Fatalf("planner offered counts=%v want [1 3]", planner.offeredCounts)
	}
	if !reflect.DeepEqual(verifiedCounts, []int{2, 3}) {
		t.Fatalf("verified candidate sizes=%v want [2 3]", verifiedCounts)
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

	stalled, ok, err := state.IntentCandidateByID(ctx, f.db, stalledCandidateID)
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
