package daemon

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/ai"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/config"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/prompttrace"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

func TestReplayIntentV2PublishesCandidatesInPlannerOrder(t *testing.T) {
	f := newCaptureFixture(t)
	ctx := context.Background()
	if _, err := BootstrapShadow(ctx, f.dir, f.db, f.cctx); err != nil {
		t.Fatal(err)
	}
	for path, contents := range map[string]string{
		"a.txt": "a\n",
		"b.txt": "b\n",
	} {
		if err := os.WriteFile(filepath.Join(f.dir, path),
			[]byte(contents), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := Capture(ctx, f.dir, f.db, f.cctx, CaptureOpts{
		IgnoreChecker: f.ig, SensitiveMatcher: f.matcher,
	}); err != nil {
		t.Fatal(err)
	}
	pending, err := state.PendingEvents(ctx, f.db, 0)
	if err != nil || len(pending) != 2 {
		t.Fatalf("pending=%+v err=%v", pending, err)
	}
	planner := orderedIntentV2Planner{}
	result, err := Replay(ctx, f.dir, f.db, f.cctx, ReplayOpts{
		GitDir: f.gitDir, CommitStrategy: ai.CommitStrategyIntent,
		IntentPlanner: planner, IntentPreset: config.PresetFast,
		IntentBypassBatchWait: true, IntentWindow: 10,
	})
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if result.Published != 2 || result.Conflicts != 0 || result.Failed != 0 {
		t.Fatalf("result=%+v", result)
	}
	subjects := strings.Split(strings.TrimSpace(mustGitOutput(t, f.dir,
		"log", "-2", "--format=%s")), "\n")
	if strings.Join(subjects, "|") != "Add beta unit|Add alpha unit" {
		t.Fatalf("subjects=%v", subjects)
	}
	var published int
	if err := f.db.ReadSQL().QueryRowContext(ctx,
		`SELECT COUNT(*) FROM intent_candidates WHERE status='published'`,
	).Scan(&published); err != nil || published != 2 {
		t.Fatalf("published candidates=%d err=%v", published, err)
	}
}

func TestReplayIntentV2AdvancesFastFallbackComponents(t *testing.T) {
	f := newCaptureFixture(t)
	ctx := context.Background()
	if _, err := BootstrapShadow(ctx, f.dir, f.db, f.cctx); err != nil {
		t.Fatal(err)
	}
	for path, contents := range map[string]string{
		"first.txt": "one\n", "second.txt": "two\n",
	} {
		if err := os.WriteFile(filepath.Join(f.dir, path),
			[]byte(contents), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := Capture(ctx, f.dir, f.db, f.cctx, CaptureOpts{
		IgnoreChecker: f.ig, SensitiveMatcher: f.matcher,
	}); err != nil {
		t.Fatal(err)
	}
	opts := ReplayOpts{
		GitDir: f.gitDir, CommitStrategy: ai.CommitStrategyIntent,
		IntentPlanner: &disconnectedIntentV2Planner{},
		IntentPreset:  config.PresetFast, IntentBypassBatchWait: true,
		IntentWindow: 10, IntentDeferLimit: 5,
	}
	first, err := Replay(ctx, f.dir, f.db, f.cctx, opts)
	if err != nil || first.Published != 1 {
		t.Fatalf("first replay=%+v err=%v", first, err)
	}
	f.cctx.BaseHead = first.BaseHead
	second, err := Replay(ctx, f.dir, f.db, f.cctx, opts)
	if err != nil || second.Published != 1 {
		t.Fatalf("second replay=%+v err=%v", second, err)
	}
	pending, err := state.PendingEvents(ctx, f.db, 0)
	if err != nil || len(pending) != 0 {
		t.Fatalf("pending=%+v err=%v", pending, err)
	}
}

func TestReplayIntentV2LateCompanionRepairsSoftCommit(t *testing.T) {
	f := newCaptureFixture(t)
	ctx := context.Background()
	if _, err := BootstrapShadow(ctx, f.dir, f.db, f.cctx); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(f.dir, "feature.go"),
		[]byte("package feature\n\nfunc Value() int { return 1 }\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := Capture(ctx, f.dir, f.db, f.cctx, CaptureOpts{
		IgnoreChecker: f.ig, SensitiveMatcher: f.matcher,
	}); err != nil {
		t.Fatal(err)
	}
	planner := &revisingIntentV2Planner{}
	opts := ReplayOpts{
		GitDir: f.gitDir, CommitStrategy: ai.CommitStrategyIntent,
		IntentPlanner: planner, IntentPreset: config.PresetFast,
		IntentBypassBatchWait: true, IntentWindow: 10,
		IntentRepairEnabled: true, IntentRepairHorizon: 10 * time.Minute,
		IntentRepairMaxCommits: 3,
	}
	first, err := Replay(ctx, f.dir, f.db, f.cctx, opts)
	if err != nil || first.Published != 1 {
		t.Fatalf("first replay=%+v err=%v", first, err)
	}
	softHead := first.BaseHead
	f.cctx.BaseHead = softHead
	if err := os.WriteFile(filepath.Join(f.dir, "feature_test.go"),
		[]byte("package feature\n\nfunc ExampleValue() { _ = Value() }\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := Capture(ctx, f.dir, f.db, f.cctx, CaptureOpts{
		IgnoreChecker: f.ig, SensitiveMatcher: f.matcher,
	}); err != nil {
		t.Fatal(err)
	}
	second, err := Replay(ctx, f.dir, f.db, f.cctx, opts)
	if err != nil {
		t.Fatalf("second Replay: %v", err)
	}
	if second.Published != 1 || second.BaseHead == softHead {
		t.Fatalf("second replay=%+v softHead=%s", second, softHead)
	}
	if commits := strings.Fields(mustGitOutput(t, f.dir,
		"rev-list", "--first-parent", "HEAD")); len(commits) != 2 {
		t.Fatalf("repair should replace, not append, soft commit: commits=%v", commits)
	}
	if status := strings.TrimSpace(mustGitOutput(
		t, f.dir, "status", "--short")); status != "" {
		t.Fatalf("repair left live index or worktree dirty: %s", status)
	}
	var candidateID string
	if err := f.db.ReadSQL().QueryRowContext(ctx, `
SELECT member.candidate_id
FROM intent_candidate_events member
JOIN capture_events capture ON capture.seq=member.event_seq
WHERE capture.path='feature.go' AND member.membership_state='active'
LIMIT 1`).Scan(&candidateID); err != nil {
		t.Fatalf("candidate id: %v", err)
	}
	candidate, ok, err := state.IntentCandidateByID(ctx, f.db, candidateID)
	if err != nil || !ok || candidate.Status != state.IntentCandidatePublished ||
		len(candidate.Events) != 2 ||
		candidate.PublishedCommitOID.String != second.BaseHead {
		t.Fatalf("candidate=%+v ok=%v err=%v", candidate, ok, err)
	}
	repairs, err := state.RecoverableIntentRepairs(ctx, f.db, 10)
	if err != nil || len(repairs) != 0 {
		t.Fatalf("recoverable repairs=%+v err=%v", repairs, err)
	}
}

func TestReplayIntentV2RepairVerificationFailureIsDurable(t *testing.T) {
	f := newCaptureFixture(t)
	ctx := context.Background()
	if _, err := BootstrapShadow(ctx, f.dir, f.db, f.cctx); err != nil {
		t.Fatal(err)
	}
	planner := &revisingIntentV2Planner{}
	repairChecks := 0
	opts := ReplayOpts{
		GitDir: f.gitDir, CommitStrategy: ai.CommitStrategyIntent,
		IntentPlanner: planner, IntentPreset: config.PresetFast,
		IntentBypassBatchWait: true, IntentWindow: 10,
		IntentRepairEnabled: true, IntentRepairHorizon: 10 * time.Minute,
		IntentRepairMaxCommits: 3, IntentVerificationMode: "full",
		IntentCandidateVerify: func(
			context.Context,
			ai.IntentCandidateAssignment,
			[]IntentCandidateCapture,
		) (IntentCandidateVerification, error) {
			return IntentCandidateVerification{
				Status: "passed", CheckedTS: 1,
			}, nil
		},
		IntentRepairCommitVerify: func(
			context.Context,
			string,
			int,
		) error {
			repairChecks++
			failure := &intentRepairVerificationFailure{
				Status:    "failed",
				Output:    "FAIL: package regression",
				CheckedTS: 2,
			}
			return fmt.Errorf("%w: %w",
				git.ErrIntentRepairVerification, failure)
		},
	}
	if err := os.WriteFile(filepath.Join(f.dir, "feature.go"),
		[]byte("package feature\n\nfunc Value() int { return 1 }\n"),
		0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := Capture(ctx, f.dir, f.db, f.cctx, CaptureOpts{
		IgnoreChecker: f.ig, SensitiveMatcher: f.matcher,
	}); err != nil {
		t.Fatal(err)
	}
	first, err := Replay(ctx, f.dir, f.db, f.cctx, opts)
	if err != nil || first.Published != 1 {
		t.Fatalf("first replay=%+v err=%v", first, err)
	}
	f.cctx.BaseHead = first.BaseHead
	if err := os.WriteFile(filepath.Join(f.dir, "feature_test.go"),
		[]byte("package feature\n\nfunc ExampleValue() { _ = Value() }\n"),
		0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := Capture(ctx, f.dir, f.db, f.cctx, CaptureOpts{
		IgnoreChecker: f.ig, SensitiveMatcher: f.matcher,
	}); err != nil {
		t.Fatal(err)
	}
	second, err := Replay(ctx, f.dir, f.db, f.cctx, opts)
	if err != nil || !second.Skipped ||
		second.SkippedReason !=
			"intent_v2_repair_failed_repair_verification_needs_attention" {
		t.Fatalf("second replay=%+v err=%v", second, err)
	}
	third, err := Replay(ctx, f.dir, f.db, f.cctx, opts)
	if err != nil || !third.Skipped ||
		third.SkippedReason !=
			"intent_v2_repair_skipped_repair_verification_needs_attention" {
		t.Fatalf("third replay=%+v err=%v", third, err)
	}
	if repairChecks != 1 {
		t.Fatalf("repair checks=%d want 1", repairChecks)
	}
	var failedRepairs int
	if err := f.db.ReadSQL().QueryRowContext(ctx, `
SELECT COUNT(*) FROM intent_repairs WHERE status='failed'`,
	).Scan(&failedRepairs); err != nil || failedRepairs != 1 {
		t.Fatalf("failed repairs=%d err=%v", failedRepairs, err)
	}
	var candidateStatus, verificationStatus, output string
	if err := f.db.ReadSQL().QueryRowContext(ctx, `
SELECT status, verification_status, verification_output
FROM intent_candidates
WHERE verification_output<>''
ORDER BY updated_ts DESC LIMIT 1`,
	).Scan(&candidateStatus, &verificationStatus, &output); err != nil {
		t.Fatal(err)
	}
	if candidateStatus != state.IntentCandidateBlocked ||
		verificationStatus != "failed" ||
		!strings.Contains(output, "package regression") {
		t.Fatalf("candidate status=%s verification=%s output=%q",
			candidateStatus, verificationStatus, output)
	}
}

func TestIntentRepairRequiredVerificationUnavailableFailsClosed(t *testing.T) {
	result, _, err := repairIntentCandidateDecision(
		context.Background(),
		"",
		"",
		nil,
		CaptureContext{},
		ReplayOpts{IntentVerificationMode: "full"},
		IntentCandidateDecision{},
		nil,
		"",
		nil,
	)
	if err != nil ||
		result.Status != state.IntentRepairSkipped ||
		result.Reason != "repair_verification_unavailable" {
		t.Fatalf("result=%+v err=%v", result, err)
	}
}

func TestReplayIntentV2RepairsOwnedCommitSuffix(t *testing.T) {
	f := newCaptureFixture(t)
	ctx := context.Background()
	if _, err := BootstrapShadow(ctx, f.dir, f.db, f.cctx); err != nil {
		t.Fatal(err)
	}
	planner := &suffixRepairIntentV2Planner{}
	opts := ReplayOpts{
		GitDir: f.gitDir, CommitStrategy: ai.CommitStrategyIntent,
		IntentPlanner: planner, IntentPreset: config.PresetFast,
		IntentBypassBatchWait: true, IntentWindow: 10,
		IntentRepairEnabled: true, IntentRepairHorizon: 10 * time.Minute,
		IntentRepairMaxCommits: 3,
	}
	writeCapture := func(path, contents string) ReplaySummary {
		t.Helper()
		if err := os.WriteFile(filepath.Join(f.dir, path),
			[]byte(contents), 0o644); err != nil {
			t.Fatal(err)
		}
		if _, err := Capture(ctx, f.dir, f.db, f.cctx, CaptureOpts{
			IgnoreChecker: f.ig, SensitiveMatcher: f.matcher,
		}); err != nil {
			t.Fatal(err)
		}
		sum, err := Replay(ctx, f.dir, f.db, f.cctx, opts)
		if err != nil {
			t.Fatal(err)
		}
		f.cctx.BaseHead = sum.BaseHead
		return sum
	}
	first := writeCapture("feature.go", "package feature\n\nfunc Value() int { return 1 }\n")
	second := writeCapture("guide.md", "# Guide\n")
	third := writeCapture("feature_test.go",
		"package feature\n\nfunc ExampleValue() { _ = Value() }\n")
	if first.Published != 1 || second.Published != 1 ||
		third.Published != 1 {
		t.Fatalf("replays first=%+v second=%+v third=%+v",
			first, second, third)
	}
	subjects := strings.Split(strings.TrimSpace(mustGitOutput(
		t, f.dir, "log", "-2", "--format=%s")), "\n")
	if strings.Join(subjects, "|") != "Document feature|Add tested feature" {
		t.Fatalf("subjects=%v", subjects)
	}
	var completed int
	if err := f.db.ReadSQL().QueryRowContext(ctx, `
SELECT COUNT(*) FROM intent_repairs
WHERE status='completed'`).Scan(&completed); err != nil || completed != 1 {
		t.Fatalf("completed repairs=%d err=%v", completed, err)
	}
}

func TestReplayIntentV2SkipsRepartitionOfUnseenCandidate(t *testing.T) {
	f := newCaptureFixture(t)
	ctx := context.Background()
	if _, err := BootstrapShadow(ctx, f.dir, f.db, f.cctx); err != nil {
		t.Fatal(err)
	}
	planner := &suffixRepairIntentV2Planner{}
	opts := ReplayOpts{
		GitDir: f.gitDir, CommitStrategy: ai.CommitStrategyIntent,
		IntentPlanner: planner, IntentPreset: config.PresetFast,
		IntentBypassBatchWait: true, IntentWindow: 10,
		IntentRepairEnabled: true, IntentRepairHorizon: 10 * time.Minute,
		IntentRepairMaxCommits: 3,
	}
	publish := func(path, contents string) ReplaySummary {
		t.Helper()
		if err := os.WriteFile(filepath.Join(f.dir, path),
			[]byte(contents), 0o644); err != nil {
			t.Fatal(err)
		}
		if _, err := Capture(ctx, f.dir, f.db, f.cctx, CaptureOpts{
			IgnoreChecker: f.ig, SensitiveMatcher: f.matcher,
		}); err != nil {
			t.Fatal(err)
		}
		sum, err := Replay(ctx, f.dir, f.db, f.cctx, opts)
		if err != nil {
			t.Fatal(err)
		}
		f.cctx.BaseHead = sum.BaseHead
		return sum
	}
	if sum := publish(
		"feature.go",
		"package feature\n\nfunc Value() int { return 1 }\n",
	); sum.Published != 1 {
		t.Fatalf("feature replay=%+v", sum)
	}
	if sum := publish("guide.md", "# Guide\n"); sum.Published != 1 {
		t.Fatalf("guide replay=%+v", sum)
	}
	headBefore := f.cctx.BaseHead
	if _, err := f.db.SQL().ExecContext(ctx, `
UPDATE intent_candidates
SET status='published', soft_publication_deadline=NULL
WHERE purpose LIKE '%documentation%'`); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(
		filepath.Join(f.dir, "feature_test.go"),
		[]byte("package feature\n\nfunc ExampleValue() { _ = Value() }\n"),
		0o644,
	); err != nil {
		t.Fatal(err)
	}
	if _, err := Capture(ctx, f.dir, f.db, f.cctx, CaptureOpts{
		IgnoreChecker: f.ig, SensitiveMatcher: f.matcher,
	}); err != nil {
		t.Fatal(err)
	}
	result, err := Replay(ctx, f.dir, f.db, f.cctx, opts)
	if err != nil {
		t.Fatal(err)
	}
	if !result.Skipped ||
		result.SkippedReason !=
			"intent_v2_repair_skipped_repair_repartition_not_proven" ||
		result.BaseHead != headBefore {
		t.Fatalf("result=%+v headBefore=%s", result, headBefore)
	}
	headAfter, err := git.RevParse(ctx, f.dir, "HEAD")
	if err != nil || headAfter != headBefore {
		t.Fatalf("HEAD=%s err=%v want %s", headAfter, err, headBefore)
	}
}

func TestIntentRepairMergesTwoSoftPublishedCandidates(t *testing.T) {
	ctx := context.Background()
	repo := cloneDaemonTestRepo(t, daemonRepoTemplate)
	base := repo.head

	oldA := mustCommitPath(t, repo.dir, "a.txt", "a1\n", "add alpha")
	seqA, err := state.AppendCaptureEvent(ctx, repo.db, state.CaptureEvent{
		BranchRef: "refs/heads/main", BranchGeneration: 1,
		BaseHead: base, Operation: "create", Path: "a.txt",
		Fidelity: "full", State: state.EventStatePublished,
		CommitOID:   sql.NullString{String: oldA, Valid: true},
		PublishedTS: sql.NullFloat64{Float64: float64(time.Now().UnixNano()) / 1e9, Valid: true},
	}, []state.CaptureOp{{
		Op: "create", Path: "a.txt", Fidelity: "full",
		AfterOID:  sql.NullString{String: mustBlobOID(t, repo.dir, oldA, "a.txt"), Valid: true},
		AfterMode: sql.NullString{String: "100644", Valid: true},
	}})
	if err != nil {
		t.Fatal(err)
	}
	oldB := mustCommitPath(t, repo.dir, "b.txt", "b1\n", "add beta")
	seqB, err := state.AppendCaptureEvent(ctx, repo.db, state.CaptureEvent{
		BranchRef: "refs/heads/main", BranchGeneration: 1,
		BaseHead: oldA, Operation: "create", Path: "b.txt",
		Fidelity: "full", State: state.EventStatePublished,
		CommitOID:   sql.NullString{String: oldB, Valid: true},
		PublishedTS: sql.NullFloat64{Float64: float64(time.Now().UnixNano()) / 1e9, Valid: true},
	}, []state.CaptureOp{{
		Op: "create", Path: "b.txt", Fidelity: "full",
		AfterOID:  sql.NullString{String: mustBlobOID(t, repo.dir, oldB, "b.txt"), Valid: true},
		AfterMode: sql.NullString{String: "100644", Valid: true},
	}})
	if err != nil {
		t.Fatal(err)
	}
	afterA, err := git.HashObjectStdin(ctx, repo.dir, []byte("a2\n"))
	if err != nil {
		t.Fatal(err)
	}
	beforeA := mustBlobOID(t, repo.dir, oldA, "a.txt")
	seqBridge, err := state.AppendCaptureEvent(ctx, repo.db, state.CaptureEvent{
		BranchRef: "refs/heads/main", BranchGeneration: 1,
		BaseHead: oldB, Operation: "modify", Path: "a.txt",
		Fidelity: "full", State: state.EventStatePending,
	}, []state.CaptureOp{{
		Op: "modify", Path: "a.txt", Fidelity: "full",
		BeforeOID:  sql.NullString{String: beforeA, Valid: true},
		BeforeMode: sql.NullString{String: "100644", Valid: true},
		AfterOID:   sql.NullString{String: afterA, Valid: true},
		AfterMode:  sql.NullString{String: "100644", Valid: true},
	}})
	if err != nil {
		t.Fatal(err)
	}
	deadline := float64(time.Now().Add(10*time.Minute).UnixNano()) / 1e9
	for _, candidate := range []state.IntentCandidate{
		{
			ID: "candidate-a", BranchRef: "refs/heads/main",
			BranchGeneration: 1, Status: state.IntentCandidateSoftPublished,
			Purpose: "complete alpha", Readiness: state.IntentReadinessReady,
			SoftPublicationDeadline: sql.NullFloat64{Float64: deadline, Valid: true},
			PublishedCommitOID:      sql.NullString{String: oldA, Valid: true},
			Events: []state.IntentCandidateEvent{{
				EventSeq: seqA, EventRole: "code",
			}},
		},
		{
			ID: "candidate-b", BranchRef: "refs/heads/main",
			BranchGeneration: 1, Status: state.IntentCandidateSoftPublished,
			Purpose: "add beta", Readiness: state.IntentReadinessReady,
			SoftPublicationDeadline: sql.NullFloat64{Float64: deadline, Valid: true},
			PublishedCommitOID:      sql.NullString{String: oldB, Valid: true},
			Events: []state.IntentCandidateEvent{{
				EventSeq: seqB, EventRole: "code",
			}},
		},
	} {
		if err := state.SaveIntentCandidate(ctx, repo.db, candidate); err != nil {
			t.Fatal(err)
		}
	}
	target, ok, err := state.IntentCandidateByID(ctx, repo.db, "candidate-a")
	if err != nil || !ok {
		t.Fatalf("target=%+v ok=%v err=%v", target, ok, err)
	}
	target.Status = state.IntentCandidateReady
	target.Events = append(target.Events, state.IntentCandidateEvent{
		EventSeq: seqBridge, EventRole: "code",
	})
	merged, err := state.MergeIntentCandidates(
		ctx,
		repo.db,
		state.IntentCandidateMergeRequest{
			Target:             target,
			SourceCandidateIDs: []string{"candidate-b"},
			Reason:             "hard dependency closure",
		},
	)
	if err != nil {
		t.Fatalf("MergeIntentCandidates: %v", err)
	}
	cctx := CaptureContext{
		BranchRef: "refs/heads/main", BranchGeneration: 1, BaseHead: oldB,
	}
	if _, err := BootstrapShadow(ctx, repo.dir, repo.db, cctx); err != nil {
		t.Fatal(err)
	}
	var selected []intentReplayItem
	for _, seq := range []int64{seqA, seqB, seqBridge} {
		event, err := loadIntentCaptureEvent(ctx, repo.db, seq)
		if err != nil {
			t.Fatal(err)
		}
		ops, err := state.LoadCaptureOps(ctx, repo.db, seq)
		if err != nil {
			t.Fatal(err)
		}
		selected = append(selected, intentReplayItem{event: event, ops: ops})
	}
	result, pending, err := repairIntentCandidateDecision(
		ctx,
		repo.dir,
		repo.gitDir,
		repo.db,
		cctx,
		ReplayOpts{
			IntentRepairEnabled:    true,
			IntentRepairHorizon:    10 * time.Minute,
			IntentRepairMaxCommits: 3,
		},
		IntentCandidateDecision{
			Candidate: merged.Candidate,
			Assignment: ai.IntentCandidateAssignment{
				CandidateID:  "candidate-a",
				SelectedSeqs: []int64{seqA, seqB, seqBridge},
				Subject:      "Complete alpha with beta",
			},
		},
		selected,
		oldB,
		nil,
	)
	if err != nil {
		t.Fatalf("repairIntentCandidateDecision: %v", err)
	}
	if result.Status != state.IntentRepairCompleted || pending != 1 {
		t.Fatalf("result=%+v pending=%d", result, pending)
	}
	if result.CommitMap[oldA] == "" ||
		result.CommitMap[oldA] != result.CommitMap[oldB] {
		t.Fatalf("commit map=%+v", result.CommitMap)
	}
	lineage, err := state.IntentCandidateLineageForTarget(
		ctx,
		repo.db,
		"refs/heads/main",
		1,
		"candidate-a",
		10,
	)
	if err != nil || len(lineage) != 1 ||
		lineage[0].SourceCandidateID != "candidate-b" {
		t.Fatalf("lineage=%+v err=%v", lineage, err)
	}
}

func TestIntentRepairSourceCommitsTraverseMergedLineage(t *testing.T) {
	ctx := context.Background()
	repo := cloneDaemonTestRepo(t, daemonRepoTemplate)
	const (
		branch     = "refs/heads/main"
		generation = int64(1)
	)
	candidates := []struct {
		id, oid, path string
	}{
		{id: "candidate-a", oid: "commit-a", path: "a.txt"},
		{id: "candidate-b", oid: "commit-b", path: "b.txt"},
		{id: "candidate-c", oid: "commit-c", path: "c.txt"},
	}
	for _, item := range candidates {
		seq, err := state.AppendCaptureEvent(ctx, repo.db, state.CaptureEvent{
			BranchRef: branch, BranchGeneration: generation,
			BaseHead: repo.head, Operation: "create", Path: item.path,
			Fidelity: "full", State: state.EventStatePublished,
			CommitOID: sql.NullString{String: item.oid, Valid: true},
		}, nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := state.SaveIntentCandidate(ctx, repo.db, state.IntentCandidate{
			ID: item.id, BranchRef: branch, BranchGeneration: generation,
			Status:    state.IntentCandidateSoftPublished,
			Readiness: state.IntentReadinessReady,
			PublishedCommitOID: sql.NullString{
				String: item.oid, Valid: true,
			},
			Events: []state.IntentCandidateEvent{{
				EventSeq: seq, EventRole: "code",
			}},
		}); err != nil {
			t.Fatal(err)
		}
	}
	load := func(id string) state.IntentCandidate {
		t.Helper()
		candidate, ok, err := state.IntentCandidateByID(ctx, repo.db, id)
		if err != nil || !ok {
			t.Fatalf("candidate %s=%+v ok=%v err=%v",
				id, candidate, ok, err)
		}
		return candidate
	}
	mergedB, err := state.MergeIntentCandidates(
		ctx,
		repo.db,
		state.IntentCandidateMergeRequest{
			Target:             load("candidate-b"),
			SourceCandidateIDs: []string{"candidate-c"},
			Reason:             "hard dependency closure",
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := state.MergeIntentCandidates(
		ctx,
		repo.db,
		state.IntentCandidateMergeRequest{
			Target:             load("candidate-a"),
			SourceCandidateIDs: []string{mergedB.Candidate.ID},
			Reason:             "hard dependency closure",
		},
	); err != nil {
		t.Fatal(err)
	}
	commits, err := intentRepairCandidateSourceCommits(
		ctx,
		repo.db,
		load("candidate-a"),
	)
	if err != nil {
		t.Fatal(err)
	}
	sort.Strings(commits)
	if strings.Join(commits, ",") != "commit-a,commit-b,commit-c" {
		t.Fatalf("source commits=%v", commits)
	}
}

func TestReplayIntentV2AdvancesDeferredCaptureState(t *testing.T) {
	f := newCaptureFixture(t)
	ctx := context.Background()
	if _, err := BootstrapShadow(ctx, f.dir, f.db, f.cctx); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(f.dir, "pending.go"),
		[]byte("package pending\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := Capture(ctx, f.dir, f.db, f.cctx, CaptureOpts{
		IgnoreChecker: f.ig, SensitiveMatcher: f.matcher,
	}); err != nil {
		t.Fatal(err)
	}
	pending, err := state.PendingEvents(ctx, f.db, 0)
	if err != nil || len(pending) != 1 {
		t.Fatalf("pending=%+v err=%v", pending, err)
	}
	result, err := Replay(ctx, f.dir, f.db, f.cctx, ReplayOpts{
		GitDir: f.gitDir, CommitStrategy: ai.CommitStrategyIntent,
		IntentPlanner: &waitingIntentV2Planner{},
		IntentPreset:  config.PresetFast, IntentBypassBatchWait: true,
		IntentWindow: 10, IntentDeferLimit: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !result.Skipped || result.Published != 0 {
		t.Fatalf("result=%+v", result)
	}
	plannerState, ok, err := state.PlannerStateForEvent(
		ctx, f.db, pending[0].Seq)
	if err != nil || !ok || plannerState.DeferCount != 1 {
		t.Fatalf("planner state=%+v ok=%v err=%v", plannerState, ok, err)
	}
}

func TestReplayIntentV2WiresPromptAndOperationalTrace(t *testing.T) {
	f := newCaptureFixture(t)
	ctx := withRuntimeTelemetry(context.Background(), &RuntimeBundle{
		RevisionID: 42, Profile: "quality-check",
	})
	if _, err := BootstrapShadow(ctx, f.dir, f.db, f.cctx); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(f.dir, "trace.go"),
		[]byte("package trace\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := Capture(ctx, f.dir, f.db, f.cctx, CaptureOpts{
		IgnoreChecker: f.ig, SensitiveMatcher: f.matcher,
	}); err != nil {
		t.Fatal(err)
	}
	prompts := &intentHealthPromptRecorder{}
	traces := &memoryTraceLogger{}
	retryLimit := 0
	result, err := Replay(ctx, f.dir, f.db, f.cctx, ReplayOpts{
		GitDir: f.gitDir, CommitStrategy: ai.CommitStrategyIntent,
		IntentPlanner: &tracedInvalidIntentV2Planner{},
		IntentPreset:  config.PresetFast, IntentRetryLimit: &retryLimit,
		IntentBypassBatchWait: true, IntentWindow: 10,
		IntentIncludeDiffs: true, IntentPlannerProvider: "openai-compat",
		IntentPlannerModel: "trace-model",
		PromptTrace:        prompts, Trace: traces,
	})
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if result.Published != 1 {
		t.Fatalf("result=%+v", result)
	}
	records := prompts.Records()
	if len(records) != 2 {
		t.Fatalf("prompt records=%+v", records)
	}
	for _, record := range records {
		if record.Strategy != "intent_v2" ||
			record.Protocol != ai.IntentPlannerProtocolV2 ||
			len(record.OfferedSeqs) != 1 ||
			record.BranchRef != f.cctx.BranchRef ||
			record.Generation != f.cctx.BranchGeneration ||
			!record.DiffIncluded || record.DiffCap != ai.IntentStageDiffCap ||
			record.ConfigRevisionID != 42 ||
			record.ConfigProfile != "quality-check" ||
			record.RetryCount != 0 {
			t.Fatalf("prompt metadata=%+v", record)
		}
	}
	events := traces.Events()
	for _, class := range []string{
		"intent.planner.input",
		"intent.planner.validation_failed",
		"intent.planner.output",
	} {
		if !traceHasClass(events, class) {
			t.Fatalf("trace class %q missing from %+v", class, events)
		}
	}
}

type orderedIntentV2Planner struct{}

type tracedInvalidIntentV2Planner struct{}

type disconnectedIntentV2Planner struct{}

func (*disconnectedIntentV2Planner) Name() string { return "disconnected-v2-test" }

func (*disconnectedIntentV2Planner) PlanIntent(
	context.Context,
	ai.IntentPlanRequest,
) (ai.IntentPlan, error) {
	return ai.IntentPlan{}, errors.New("legacy planner path must not run")
}

func (*disconnectedIntentV2Planner) PlanIntentV2(
	_ context.Context,
	req ai.IntentPlanRequestV2,
) (ai.IntentPlanV2, error) {
	seqs := make([]int64, 0, len(req.OfferedCaptures))
	for _, capture := range req.OfferedCaptures {
		seqs = append(seqs, capture.Seq)
	}
	return ai.IntentPlanV2{
		ProtocolVersion: ai.IntentPlannerProtocolV2,
		Candidates: []ai.IntentCandidateAssignment{{
			CandidateID: "disconnected", SelectedSeqs: seqs,
			Purpose: "mix independent captures", Readiness: ai.IntentCandidateReady,
			Subject: "Mix captures", GroupingReason: "same window",
		}},
	}, nil
}

func (*tracedInvalidIntentV2Planner) Name() string { return "openai-compat" }

func (*tracedInvalidIntentV2Planner) PlanIntent(
	context.Context,
	ai.IntentPlanRequest,
) (ai.IntentPlan, error) {
	return ai.IntentPlan{}, errors.New("legacy planner path must not run")
}

func (*tracedInvalidIntentV2Planner) PlanIntentV2(
	ctx context.Context,
	_ ai.IntentPlanRequestV2,
) (ai.IntentPlanV2, error) {
	logger, meta, ok := prompttrace.From(ctx)
	if !ok {
		return ai.IntentPlanV2{}, errors.New("native v2 prompt trace context missing")
	}
	record := func(stage string) {
		logger.Record(prompttrace.Record{
			Stage: stage, Strategy: meta.Strategy, Protocol: meta.Protocol,
			Provider: meta.Provider, Model: meta.Model,
			OfferedSeqs: append([]int64(nil), meta.OfferedSeqs...),
			BranchRef:   meta.BranchRef, Generation: meta.Generation,
			DiffIncluded: meta.DiffIncluded, DiffCap: meta.DiffCap,
			ConfigRevisionID: meta.ConfigRevisionID,
			ConfigProfile:    meta.ConfigProfile, RetryCount: meta.RetryCount,
		})
	}
	record("request")
	record("response")
	return ai.IntentPlanV2{ProtocolVersion: ai.IntentPlannerProtocolV2}, nil
}

func (orderedIntentV2Planner) Name() string { return "ordered-v2-test" }

func (orderedIntentV2Planner) PlanIntent(
	context.Context,
	ai.IntentPlanRequest,
) (ai.IntentPlan, error) {
	return ai.IntentPlan{}, errors.New("legacy planner path must not run")
}

type revisingIntentV2Planner struct{}

type waitingIntentV2Planner struct{}

type suffixRepairIntentV2Planner struct{}

func (*suffixRepairIntentV2Planner) Name() string { return "suffix-repair-v2-test" }

func (*suffixRepairIntentV2Planner) PlanIntent(
	context.Context,
	ai.IntentPlanRequest,
) (ai.IntentPlan, error) {
	return ai.IntentPlan{}, errors.New("legacy planner path must not run")
}

func (*suffixRepairIntentV2Planner) PlanIntentV2(
	_ context.Context,
	req ai.IntentPlanRequestV2,
) (ai.IntentPlanV2, error) {
	byPath := make(map[string]int64)
	for _, capture := range req.OfferedCaptures {
		byPath[capture.Path] = capture.Seq
	}
	featureID := "candidate-feature"
	docID := "candidate-doc"
	for _, candidate := range req.Candidates {
		switch {
		case strings.Contains(candidate.Purpose, "documentation"):
			docID = candidate.CandidateID
		case strings.Contains(candidate.Purpose, "feature"):
			featureID = candidate.CandidateID
		}
	}
	var assignments []ai.IntentCandidateAssignment
	switch {
	case byPath["feature.go"] != 0:
		assignments = append(assignments, ai.IntentCandidateAssignment{
			CandidateID:    featureID,
			SelectedSeqs:   []int64{byPath["feature.go"]},
			Purpose:        "add feature with its test",
			Readiness:      ai.IntentCandidateReady,
			Subject:        "Add tested feature",
			GroupingReason: "feature candidate",
		})
	case byPath["feature_test.go"] != 0:
		assignments = append(assignments, ai.IntentCandidateAssignment{
			CandidateID:    featureID,
			SelectedSeqs:   []int64{byPath["feature_test.go"]},
			Purpose:        "add feature with its test",
			Readiness:      ai.IntentCandidateReady,
			Subject:        "Add tested feature",
			GroupingReason: "late companion completes feature candidate",
		})
	case byPath["guide.md"] != 0:
		assignments = append(assignments, ai.IntentCandidateAssignment{
			CandidateID:    docID,
			SelectedSeqs:   []int64{byPath["guide.md"]},
			Purpose:        "feature documentation",
			Readiness:      ai.IntentCandidateReady,
			Subject:        "Document feature",
			GroupingReason: "independent documentation candidate",
		})
	}
	return ai.IntentPlanV2{
		ProtocolVersion: ai.IntentPlannerProtocolV2,
		Candidates:      assignments,
	}, nil
}

func (*waitingIntentV2Planner) Name() string { return "waiting-v2-test" }

func (*waitingIntentV2Planner) PlanIntent(
	context.Context,
	ai.IntentPlanRequest,
) (ai.IntentPlan, error) {
	return ai.IntentPlan{}, errors.New("legacy planner path must not run")
}

func (*waitingIntentV2Planner) PlanIntentV2(
	_ context.Context,
	req ai.IntentPlanRequestV2,
) (ai.IntentPlanV2, error) {
	seqs := make([]int64, 0, len(req.OfferedCaptures))
	for _, capture := range req.OfferedCaptures {
		seqs = append(seqs, capture.Seq)
	}
	return ai.IntentPlanV2{
		ProtocolVersion: ai.IntentPlannerProtocolV2,
		Candidates: []ai.IntentCandidateAssignment{{
			CandidateID: "waiting", SelectedSeqs: seqs,
			Purpose: "wait for a companion", Readiness: ai.IntentCandidateWait,
			MissingCompanions: []string{"companion test"},
			Subject:           "Add pending unit", GroupingReason: "companion expected",
		}},
	}, nil
}

func (*revisingIntentV2Planner) Name() string { return "revising-v2-test" }

func (*revisingIntentV2Planner) PlanIntent(
	context.Context,
	ai.IntentPlanRequest,
) (ai.IntentPlan, error) {
	return ai.IntentPlan{}, errors.New("legacy planner path must not run")
}

func (*revisingIntentV2Planner) PlanIntentV2(
	_ context.Context,
	req ai.IntentPlanRequestV2,
) (ai.IntentPlanV2, error) {
	seqs := make([]int64, 0, len(req.OfferedCaptures))
	for _, capture := range req.OfferedCaptures {
		seqs = append(seqs, capture.Seq)
	}
	candidateID := "candidate-feature"
	if len(req.Candidates) > 0 {
		candidateID = req.Candidates[0].CandidateID
	}
	return ai.IntentPlanV2{
		ProtocolVersion: ai.IntentPlannerProtocolV2,
		Candidates: []ai.IntentCandidateAssignment{{
			CandidateID: candidateID, SelectedSeqs: seqs,
			Purpose: "add feature with its test", Readiness: ai.IntentCandidateReady,
			Subject:        "Add tested feature",
			Body:           "- Keep implementation and companion coverage atomic",
			GroupingReason: "source and companion test form one purpose",
		}},
	}, nil
}

func (orderedIntentV2Planner) PlanIntentV2(
	_ context.Context,
	req ai.IntentPlanRequestV2,
) (ai.IntentPlanV2, error) {
	byPath := make(map[string]int64)
	for _, capture := range req.OfferedCaptures {
		byPath[capture.Path] = capture.Seq
	}
	return ai.IntentPlanV2{
		ProtocolVersion: ai.IntentPlannerProtocolV2,
		Candidates: []ai.IntentCandidateAssignment{
			{
				CandidateID: "candidate-alpha", SelectedSeqs: []int64{byPath["a.txt"]},
				Purpose: "add alpha unit", Readiness: ai.IntentCandidateReady,
				Subject: "Add alpha unit", GroupingReason: "independent alpha purpose",
			},
			{
				CandidateID: "candidate-beta", SelectedSeqs: []int64{byPath["b.txt"]},
				Purpose: "add beta unit", Readiness: ai.IntentCandidateReady,
				Subject: "Add beta unit", GroupingReason: "independent beta purpose",
			},
		},
	}, nil
}

func TestIntentRepairTransactionCompletesAndPreservesDirtyState(t *testing.T) {
	f := newIntentRepairFixture(t, 2)
	ctx := context.Background()
	if err := os.WriteFile(filepath.Join(f.repo.dir, "unrelated.txt"),
		[]byte("staged but unrelated\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := git.Run(ctx, git.RunOpts{Dir: f.repo.dir},
		"add", "unrelated.txt"); err != nil {
		t.Fatal(err)
	}
	beforeIndex := mustGitOutput(t, f.repo.dir, "diff", "--cached", "--binary")
	beforeWorktree := mustGitOutput(t, f.repo.dir, "diff", "--binary")

	result, err := ApplyIntentRepairTransaction(ctx, f.repo.dir, f.repo.gitDir,
		f.repo.db, f.cctx, f.plan)
	if err != nil {
		t.Fatalf("ApplyIntentRepairTransaction: %v", err)
	}
	if result.Status != state.IntentRepairCompleted || result.NewHead == "" ||
		result.NewHead == result.OldHead {
		t.Fatalf("result=%+v", result)
	}
	if got := mustGitOutput(t, f.repo.dir, "diff", "--cached", "--binary"); got != beforeIndex {
		t.Fatalf("staged state changed:\n--- before\n%s\n--- after\n%s", beforeIndex, got)
	}
	if got := mustGitOutput(t, f.repo.dir, "diff", "--binary"); got != beforeWorktree {
		t.Fatalf("worktree state changed:\n--- before\n%s\n--- after\n%s", beforeWorktree, got)
	}
	if backup, err := git.RevParse(ctx, f.repo.dir, result.BackupRef); err != nil || backup != f.plan.ExpectedHead {
		t.Fatalf("backup=%q err=%v want %s", backup, err, f.plan.ExpectedHead)
	}
	assertIntentRepairReconciled(t, f, result.NewHead)
}

func TestIntentRepairVerificationFailureLeavesPreparedRepairFailed(
	t *testing.T,
) {
	f := newIntentRepairFixture(t, 1)
	ctx := context.Background()
	f.plan.VerifyCommit = func(
		context.Context,
		string,
		int,
	) error {
		return errors.New("approved check failed")
	}
	_, err := ApplyIntentRepairTransaction(
		ctx, f.repo.dir, f.repo.gitDir, f.repo.db, f.cctx, f.plan)
	if err == nil || !strings.Contains(err.Error(), "approved check failed") {
		t.Fatalf("error=%v", err)
	}
	head, resolveErr := git.RevParse(ctx, f.repo.dir, "HEAD")
	if resolveErr != nil || head != f.plan.ExpectedHead {
		t.Fatalf("HEAD=%s err=%v want %s",
			head, resolveErr, f.plan.ExpectedHead)
	}
	repair, ok, loadErr := state.IntentRepairByID(
		ctx, f.repo.db, f.plan.ID)
	if loadErr != nil || !ok ||
		repair.Status != state.IntentRepairFailed ||
		!strings.Contains(repair.Error, "approved check failed") {
		t.Fatalf("repair=%+v ok=%v err=%v", repair, ok, loadErr)
	}
	backupRef, refErr := git.IntentRepairBackupRef(
		f.plan.BranchRef, f.plan.ID)
	if refErr != nil {
		t.Fatal(refErr)
	}
	if _, resolveErr = git.RevParse(ctx, f.repo.dir, backupRef); !errors.Is(resolveErr, git.ErrRefNotFound) {
		t.Fatalf("backup exists after failed verification: %v", resolveErr)
	}
}

func TestValidateIntentRepairPlanAllowsNonContiguousPartition(t *testing.T) {
	plan := IntentRepairPlan{
		BranchRef:        "refs/heads/main",
		BranchGeneration: 1,
		ExpectedHead:     "old-a2",
		OldChain:         []string{"old-a1", "old-b1", "old-a2"},
		MaxCommits:       3,
		Candidates: []IntentRepairCandidatePlan{
			{
				CandidateID: "candidate-a",
				Replaces:    []string{"old-a1", "old-a2"},
				TreeOID:     "tree-a",
				Message:     "Complete alpha",
			},
			{
				CandidateID: "candidate-b",
				Replaces:    []string{"old-b1"},
				TreeOID:     "tree-b",
				Message:     "Add beta",
			},
		},
	}
	cctx := CaptureContext{
		BranchRef:        plan.BranchRef,
		BranchGeneration: plan.BranchGeneration,
	}
	if err := validateIntentRepairPlan(plan, cctx); err != nil {
		t.Fatalf("validateIntentRepairPlan: %v", err)
	}
	eligibility := intentRepairEligibility(plan)
	got := make([]string, 0, len(eligibility.Commits))
	for _, commit := range eligibility.Commits {
		got = append(got, commit.OID+":"+commit.CandidateID)
	}
	if strings.Join(got, ",") !=
		"old-a1:candidate-a,old-b1:candidate-b,old-a2:candidate-a" {
		t.Fatalf("eligibility chain=%v", got)
	}
}

func TestValidateIntentRepairPlanRejectsIncompleteRepartition(t *testing.T) {
	plan := IntentRepairPlan{
		BranchRef:        "refs/heads/main",
		BranchGeneration: 1,
		ExpectedHead:     "old-a2",
		OldChain:         []string{"old-a1", "old-b1", "old-a2"},
		MaxCommits:       3,
		Candidates: []IntentRepairCandidatePlan{{
			CandidateID: "candidate-a",
			Replaces:    []string{"old-a1", "old-a2"},
			TreeOID:     "tree-a",
			Message:     "Complete alpha",
		}},
	}
	err := validateIntentRepairPlan(plan, CaptureContext{
		BranchRef: plan.BranchRef, BranchGeneration: plan.BranchGeneration,
	})
	if err == nil || !strings.Contains(err.Error(), "partition the complete old chain") {
		t.Fatalf("error=%v", err)
	}
}

func TestIntentRepairCrashAfterCASRecoversOnRestart(t *testing.T) {
	f := newIntentRepairFixture(t, 1)
	ctx := context.Background()
	crash := errors.New("simulated crash after CAS")
	intentRepairAfterGitApply = func(IntentRepairResult) error { return crash }
	t.Cleanup(func() { intentRepairAfterGitApply = nil })

	applied, err := ApplyIntentRepairTransaction(ctx, f.repo.dir, f.repo.gitDir,
		f.repo.db, f.cctx, f.plan)
	if !errors.Is(err, crash) {
		t.Fatalf("ApplyIntentRepairTransaction err=%v want crash", err)
	}
	if applied.NewHead == "" || applied.NewHead == f.plan.ExpectedHead {
		t.Fatalf("Git CAS did not land before crash: %+v", applied)
	}
	repair, ok, err := state.IntentRepairByID(ctx, f.repo.db, f.plan.ID)
	if err != nil || !ok || repair.Status != state.IntentRepairPrepared {
		t.Fatalf("prepared repair=%+v ok=%v err=%v", repair, ok, err)
	}

	intentRepairAfterGitApply = nil
	recovered, err := RecoverIntentRepairs(ctx, f.repo.dir, f.repo.gitDir,
		f.repo.db, f.cctx)
	if err != nil {
		t.Fatalf("RecoverIntentRepairs: %v", err)
	}
	if len(recovered) != 1 || !recovered[0].Recovered ||
		recovered[0].Status != state.IntentRepairCompleted ||
		recovered[0].NewHead != applied.NewHead {
		t.Fatalf("recovered=%+v", recovered)
	}
	assertIntentRepairReconciled(t, f, applied.NewHead)
}

func TestRunRecoversIntentRepairBeforeStartupTransition(t *testing.T) {
	f := newIntentRepairFixture(t, 1)
	ctx := context.Background()
	crash := errors.New("simulated crash after CAS")
	intentRepairAfterGitApply = func(IntentRepairResult) error { return crash }
	t.Cleanup(func() { intentRepairAfterGitApply = nil })

	applied, err := ApplyIntentRepairTransaction(ctx, f.repo.dir, f.repo.gitDir,
		f.repo.db, f.cctx, f.plan)
	if !errors.Is(err, crash) {
		t.Fatalf("ApplyIntentRepairTransaction err=%v want crash", err)
	}
	intentRepairAfterGitApply = nil
	shutdown := make(chan struct{})
	close(shutdown)
	if err := Run(ctx, Options{
		RepoPath: f.repo.dir, GitDir: f.repo.gitDir, DB: f.repo.db,
		MessageFn: DeterministicMessage, ShutdownCh: shutdown,
		SkipSignals: true, BootGrace: time.Hour,
	}); err != nil {
		t.Fatalf("Run: %v", err)
	}
	assertIntentRepairReconciled(t, f, applied.NewHead)
	generation, err := LoadBranchGeneration(ctx, f.repo.db)
	if err != nil || generation != f.cctx.BranchGeneration {
		t.Fatalf("generation=%d err=%v", generation, err)
	}
}

func TestIntentRepairRecoveryRejectsDifferentReplacementChain(t *testing.T) {
	f := newIntentRepairFixture(t, 1)
	ctx := context.Background()
	crash := errors.New("simulated crash after CAS")
	intentRepairAfterGitApply = func(IntentRepairResult) error { return crash }
	t.Cleanup(func() { intentRepairAfterGitApply = nil })
	applied, err := ApplyIntentRepairTransaction(ctx, f.repo.dir, f.repo.gitDir,
		f.repo.db, f.cctx, f.plan)
	if !errors.Is(err, crash) {
		t.Fatalf("ApplyIntentRepairTransaction err=%v want crash", err)
	}
	intentRepairAfterGitApply = nil
	base := strings.TrimSpace(mustGitOutput(
		t, f.repo.dir, "rev-parse", f.oldCommits[0]+"^"))
	alternate, err := git.CommitTree(
		ctx, f.repo.dir, f.plan.Candidates[0].TreeOID,
		"External replacement", base)
	if err != nil {
		t.Fatal(err)
	}
	if err := git.UpdateRef(ctx, f.repo.dir, f.cctx.BranchRef,
		alternate, applied.NewHead); err != nil {
		t.Fatal(err)
	}
	_, err = RecoverIntentRepairs(
		ctx, f.repo.dir, f.repo.gitDir, f.repo.db, f.cctx)
	if err == nil || !strings.Contains(err.Error(),
		"does not match prepared plan") {
		t.Fatalf("RecoverIntentRepairs err=%v", err)
	}
	if backup, backupErr := git.RevParse(
		ctx, f.repo.dir, applied.BackupRef); backupErr != nil ||
		backup != f.plan.ExpectedHead {
		t.Fatalf("backup=%q err=%v", backup, backupErr)
	}
}

func TestIntentRepairDirtyOverlapSkipsWithoutMutation(t *testing.T) {
	f := newIntentRepairFixture(t, 1)
	ctx := context.Background()
	if err := os.WriteFile(filepath.Join(f.repo.dir, "file-1.txt"),
		[]byte("overlap\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := git.Run(ctx, git.RunOpts{Dir: f.repo.dir},
		"add", "file-1.txt"); err != nil {
		t.Fatal(err)
	}
	before := mustGitOutput(t, f.repo.dir, "diff", "--cached", "--binary")
	result, err := ApplyIntentRepairTransaction(ctx, f.repo.dir, f.repo.gitDir,
		f.repo.db, f.cctx, f.plan)
	if err != nil {
		t.Fatalf("ApplyIntentRepairTransaction: %v", err)
	}
	if result.Status != state.IntentRepairSkipped ||
		result.Reason != git.IntentRepairReasonStagedOverlap {
		t.Fatalf("result=%+v", result)
	}
	if head, _ := git.RevParse(ctx, f.repo.dir, "HEAD"); head != f.plan.ExpectedHead {
		t.Fatalf("HEAD=%s want unchanged %s", head, f.plan.ExpectedHead)
	}
	if got := mustGitOutput(t, f.repo.dir, "diff", "--cached", "--binary"); got != before {
		t.Fatalf("staged overlap changed:\n%s", got)
	}
}

func TestIntentRepairBackupRetentionPrunesOnlyAdvancedBranch(t *testing.T) {
	f := newIntentRepairFixture(t, 1)
	ctx := context.Background()
	result, err := ApplyIntentRepairTransaction(ctx, f.repo.dir, f.repo.gitDir,
		f.repo.db, f.cctx, f.plan)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := PruneIntentRepairBackups(ctx, f.repo.dir, f.repo.db,
		time.Now().Add(8*24*time.Hour)); err != nil {
		t.Fatal(err)
	}
	if _, err := git.RevParse(ctx, f.repo.dir, result.BackupRef); err != nil {
		t.Fatalf("backup pruned while branch still required it: %v", err)
	}

	mustCommitPath(t, f.repo.dir, "after.txt", "after\n", "advance")
	if _, err := f.repo.db.SQL().ExecContext(ctx, `
UPDATE intent_repairs SET completed_ts=? WHERE id=?`,
		float64(time.Now().Add(-8*24*time.Hour).UnixNano())/1e9,
		result.ID); err != nil {
		t.Fatal(err)
	}
	pruned, err := PruneIntentRepairBackups(ctx, f.repo.dir, f.repo.db, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if pruned != 1 {
		t.Fatalf("pruned=%d want 1", pruned)
	}
	if _, err := git.RevParse(ctx, f.repo.dir, result.BackupRef); !errors.Is(err, git.ErrRefNotFound) {
		t.Fatalf("backup ref still exists or unexpected error: %v", err)
	}
}

type intentRepairFixture struct {
	repo       *daemonTestRepo
	cctx       CaptureContext
	plan       IntentRepairPlan
	eventSeqs  []int64
	oldCommits []string
}

func newIntentRepairFixture(t *testing.T, commitCount int) intentRepairFixture {
	t.Helper()
	ctx := context.Background()
	repo := cloneDaemonTestRepo(t, daemonRepoTemplate)
	f := intentRepairFixture{repo: repo}
	base := repo.head
	for i := 1; i <= commitCount; i++ {
		path := "file-" + string(rune('0'+i)) + ".txt"
		old := mustCommitPath(t, repo.dir, path,
			"value "+string(rune('0'+i))+"\n", "old semantic part")
		seq, err := state.AppendCaptureEvent(ctx, repo.db, state.CaptureEvent{
			BranchRef: "refs/heads/main", BranchGeneration: 1,
			BaseHead: base, Operation: "create", Path: path,
			Fidelity: "full", State: state.EventStatePublished,
			CommitOID:   sql.NullString{String: old, Valid: true},
			PublishedTS: sql.NullFloat64{Float64: float64(time.Now().UnixNano()) / 1e9, Valid: true},
		}, []state.CaptureOp{{
			Op: "create", Path: path, Fidelity: "full",
			AfterOID:  sql.NullString{String: mustBlobOID(t, repo.dir, old, path), Valid: true},
			AfterMode: sql.NullString{String: "100644", Valid: true},
		}})
		if err != nil {
			t.Fatal(err)
		}
		f.eventSeqs = append(f.eventSeqs, seq)
		f.oldCommits = append(f.oldCommits, old)
		base = old
	}
	head := f.oldCommits[len(f.oldCommits)-1]
	events := make([]state.IntentCandidateEvent, 0, len(f.eventSeqs))
	for _, seq := range f.eventSeqs {
		events = append(events, state.IntentCandidateEvent{
			EventSeq: seq, EventRole: "code",
		})
	}
	if err := state.SaveIntentCandidate(ctx, repo.db, state.IntentCandidate{
		ID: "candidate-repair", BranchRef: "refs/heads/main",
		BranchGeneration: 1, Status: state.IntentCandidateSoftPublished,
		Purpose: "combine one semantic change", Readiness: state.IntentReadinessReady,
		PublishedCommitOID: sql.NullString{String: head, Valid: true},
		Events:             events,
	}); err != nil {
		t.Fatal(err)
	}
	tree, err := git.RevParse(ctx, repo.dir, head+"^{tree}")
	if err != nil {
		t.Fatal(err)
	}
	f.cctx = CaptureContext{
		BranchRef: "refs/heads/main", BranchGeneration: 1, BaseHead: head,
	}
	if _, err := BootstrapShadow(ctx, repo.dir, repo.db, f.cctx); err != nil {
		t.Fatal(err)
	}
	paths := make([]string, 0, commitCount)
	for i := 1; i <= commitCount; i++ {
		paths = append(paths, "file-"+string(rune('0'+i))+".txt")
	}
	f.plan = IntentRepairPlan{
		ID:        "repair-test-" + string(rune('0'+commitCount)),
		BranchRef: "refs/heads/main", BranchGeneration: 1,
		ExpectedHead: head, Paths: paths, MaxCommits: commitCount,
		Candidates: []IntentRepairCandidatePlan{{
			CandidateID: "candidate-repair",
			Replaces:    append([]string(nil), f.oldCommits...),
			TreeOID:     tree,
			Message:     "Combine semantic change\n\n- Keep related files atomic",
		}},
	}
	return f
}

func mustCommitPath(t *testing.T, repo, path, contents, message string) string {
	t.Helper()
	if err := os.WriteFile(filepath.Join(repo, path), []byte(contents), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := git.Run(context.Background(), git.RunOpts{Dir: repo},
		"add", "--", path); err != nil {
		t.Fatal(err)
	}
	if _, err := git.Run(context.Background(), git.RunOpts{Dir: repo},
		"commit", "-q", "-m", message); err != nil {
		t.Fatal(err)
	}
	head, err := git.RevParse(context.Background(), repo, "HEAD")
	if err != nil {
		t.Fatal(err)
	}
	return head
}

func mustBlobOID(t *testing.T, repo, commit, path string) string {
	t.Helper()
	oid, err := git.LsTreeBlobOID(context.Background(), repo, commit, path)
	if err != nil {
		t.Fatal(err)
	}
	return oid
}

func mustGitOutput(t *testing.T, repo string, args ...string) string {
	t.Helper()
	out, err := git.Run(context.Background(), git.RunOpts{Dir: repo}, args...)
	if err != nil {
		t.Fatal(err)
	}
	return string(out)
}

func assertIntentRepairReconciled(t *testing.T, f intentRepairFixture, newHead string) {
	t.Helper()
	ctx := context.Background()
	repair, ok, err := state.IntentRepairByID(ctx, f.repo.db, f.plan.ID)
	if err != nil || !ok || repair.Status != state.IntentRepairCompleted {
		t.Fatalf("repair=%+v ok=%v err=%v", repair, ok, err)
	}
	candidate, ok, err := state.IntentCandidateByID(ctx, f.repo.db, "candidate-repair")
	if err != nil || !ok || candidate.Status != state.IntentCandidatePublished ||
		!candidate.PublishedCommitOID.Valid ||
		candidate.PublishedCommitOID.String != newHead {
		t.Fatalf("candidate=%+v ok=%v err=%v", candidate, ok, err)
	}
	for _, seq := range f.eventSeqs {
		var eventState string
		var oid sql.NullString
		if err := f.repo.db.SQL().QueryRowContext(ctx,
			`SELECT state, commit_oid FROM capture_events WHERE seq=?`, seq).
			Scan(&eventState, &oid); err != nil {
			t.Fatal(err)
		}
		if eventState != state.EventStatePublished || !oid.Valid || oid.String != newHead {
			t.Fatalf("event %d state=%s oid=%+v want %s", seq, eventState, oid, newHead)
		}
	}
	var shadowHead string
	if err := f.repo.db.SQL().QueryRowContext(ctx, `
SELECT value FROM daemon_meta WHERE key=?`,
		ShadowBootstrappedKey(f.cctx.BranchRef, f.cctx.BranchGeneration)).
		Scan(&shadowHead); err != nil && !strings.Contains(err.Error(), "no rows") {
		t.Fatal(err)
	}
}
