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
	offeredCounts       []int
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
			CandidateID: candidateID,
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
		SortByPath:         true,
		DisablePendingCap:  true,
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
		BranchGeneration: f.cctx.BranchGeneration,
		Status:           state.IntentCandidateWaiting,
		Readiness:        state.IntentReadinessWait,
		Purpose:          "complete the source implementation",
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
	verify := func(
		_ context.Context,
		_ ai.IntentCandidateAssignment,
		captures []IntentCandidateCapture,
	) (IntentCandidateVerification, error) {
		if len(captures) != 3 {
			return IntentCandidateVerification{},
				errors.New("missing source_test.go companion")
		}
		return IntentCandidateVerification{
			Status: "passed", CheckedTS: float64(time.Now().UnixNano()) / 1e9,
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
		IntentPathCoalescing: &pathCoalescing,
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
