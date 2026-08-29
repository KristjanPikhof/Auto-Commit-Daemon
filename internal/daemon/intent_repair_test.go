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

func TestReplayIntentV2DrainsDuplicateRecaptureChain(t *testing.T) {
	f := newCaptureFixture(t)
	ctx := context.Background()
	path := "duplicate.txt"
	if err := os.WriteFile(filepath.Join(f.dir, path), []byte("A\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := git.Run(ctx, git.RunOpts{Dir: f.dir}, "add", "--", path); err != nil {
		t.Fatal(err)
	}
	if _, err := git.Run(ctx, git.RunOpts{Dir: f.dir},
		"commit", "-q", "-m", "seed duplicate chain"); err != nil {
		t.Fatal(err)
	}
	base, err := git.RevParse(ctx, f.dir, "HEAD")
	if err != nil {
		t.Fatal(err)
	}
	f.cctx.BaseHead = base
	if _, err := BootstrapShadow(ctx, f.dir, f.db, f.cctx); err != nil {
		t.Fatal(err)
	}
	a, err := git.HashObjectStdin(ctx, f.dir, []byte("A\n"))
	if err != nil {
		t.Fatal(err)
	}
	b, err := git.HashObjectStdin(ctx, f.dir, []byte("B\n"))
	if err != nil {
		t.Fatal(err)
	}
	c, err := git.HashObjectStdin(ctx, f.dir, []byte("C\n"))
	if err != nil {
		t.Fatal(err)
	}
	for _, transition := range [][2]string{{a, b}, {a, b}, {b, c}} {
		_, err := state.AppendCaptureEvent(ctx, f.db, state.CaptureEvent{
			BranchRef: f.cctx.BranchRef, BranchGeneration: f.cctx.BranchGeneration,
			BaseHead: base, Operation: "modify", Path: path, Fidelity: "full",
		}, []state.CaptureOp{{
			Op: "modify", Path: path, Fidelity: "full",
			BeforeOID:  sql.NullString{String: transition[0], Valid: true},
			BeforeMode: sql.NullString{String: git.RegularFileMode, Valid: true},
			AfterOID:   sql.NullString{String: transition[1], Valid: true},
			AfterMode:  sql.NullString{String: git.RegularFileMode, Valid: true},
		}})
		if err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(f.dir, path), []byte("C\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	opts := ReplayOpts{
		GitDir: f.gitDir, CommitStrategy: ai.CommitStrategyIntent,
		IntentPlanner: ai.DeterministicProvider{}, IntentPreset: config.PresetFast,
		IntentBypassBatchWait: true, IntentWindow: 10,
	}
	for attempt := 0; attempt < 6; attempt++ {
		result, replayErr := Replay(ctx, f.dir, f.db, f.cctx, opts)
		if replayErr != nil {
			t.Fatalf("replay attempt %d=%+v err=%v", attempt, result, replayErr)
		}
		if result.BaseHead != "" {
			f.cctx.BaseHead = result.BaseHead
		}
		pending, loadErr := state.PendingEvents(ctx, f.db, 0)
		if loadErr != nil {
			t.Fatal(loadErr)
		}
		if len(pending) == 0 {
			break
		}
		if result.Published == 0 {
			t.Fatalf("replay attempt %d made no progress: %+v pending=%+v",
				attempt, result, pending)
		}
	}
	pending, err := state.PendingEvents(ctx, f.db, 0)
	if err != nil || len(pending) != 0 {
		t.Fatalf("pending after replay=%+v err=%v", pending, err)
	}
	if f.cctx.BaseHead == base {
		t.Fatal("duplicate chain did not advance HEAD")
	}
	if got := mustGitOutput(t, f.dir, "show", "HEAD:"+path); got != "C\n" {
		t.Fatalf("published contents=%q want C", got)
	}
}

func TestReplayIntentV2DrainsDuplicateDeleteAndRenameRecaptures(t *testing.T) {
	t.Run("delete", func(t *testing.T) {
		f := newCaptureFixture(t)
		ctx := context.Background()
		path := "removed.txt"
		contents := []byte("remove once\n")
		if err := os.WriteFile(filepath.Join(f.dir, path), contents, 0o644); err != nil {
			t.Fatal(err)
		}
		if _, err := git.Run(ctx, git.RunOpts{Dir: f.dir}, "add", "--", path); err != nil {
			t.Fatal(err)
		}
		if _, err := git.Run(ctx, git.RunOpts{Dir: f.dir},
			"commit", "-q", "-m", "seed duplicate delete"); err != nil {
			t.Fatal(err)
		}
		base, err := git.RevParse(ctx, f.dir, "HEAD")
		if err != nil {
			t.Fatal(err)
		}
		f.cctx.BaseHead = base
		if _, err := BootstrapShadow(ctx, f.dir, f.db, f.cctx); err != nil {
			t.Fatal(err)
		}
		before, err := git.HashObjectStdin(ctx, f.dir, contents)
		if err != nil {
			t.Fatal(err)
		}
		for range 2 {
			_, err := state.AppendCaptureEvent(ctx, f.db, state.CaptureEvent{
				BranchRef: f.cctx.BranchRef, BranchGeneration: f.cctx.BranchGeneration,
				BaseHead: base, Operation: "delete", Path: path, Fidelity: "full",
			}, []state.CaptureOp{{
				Op: "delete", Path: path, Fidelity: "full",
				BeforeOID:  sql.NullString{String: before, Valid: true},
				BeforeMode: sql.NullString{String: git.RegularFileMode, Valid: true},
			}})
			if err != nil {
				t.Fatal(err)
			}
		}
		if err := os.Remove(filepath.Join(f.dir, path)); err != nil {
			t.Fatal(err)
		}
		replayAllIntentPendingForTest(t, f)
		if _, err := git.Run(ctx, git.RunOpts{Dir: f.dir},
			"cat-file", "-e", "HEAD:"+path); err == nil {
			t.Fatalf("%s still exists at HEAD", path)
		}
	})

	t.Run("rename", func(t *testing.T) {
		f := newCaptureFixture(t)
		ctx := context.Background()
		oldPath, newPath := "before.txt", "after.txt"
		contents := []byte("rename once\n")
		if err := os.WriteFile(filepath.Join(f.dir, oldPath), contents, 0o644); err != nil {
			t.Fatal(err)
		}
		if _, err := git.Run(ctx, git.RunOpts{Dir: f.dir}, "add", "--", oldPath); err != nil {
			t.Fatal(err)
		}
		if _, err := git.Run(ctx, git.RunOpts{Dir: f.dir},
			"commit", "-q", "-m", "seed duplicate rename"); err != nil {
			t.Fatal(err)
		}
		base, err := git.RevParse(ctx, f.dir, "HEAD")
		if err != nil {
			t.Fatal(err)
		}
		f.cctx.BaseHead = base
		if _, err := BootstrapShadow(ctx, f.dir, f.db, f.cctx); err != nil {
			t.Fatal(err)
		}
		blob, err := git.HashObjectStdin(ctx, f.dir, contents)
		if err != nil {
			t.Fatal(err)
		}
		for range 2 {
			_, err := state.AppendCaptureEvent(ctx, f.db, state.CaptureEvent{
				BranchRef: f.cctx.BranchRef, BranchGeneration: f.cctx.BranchGeneration,
				BaseHead: base, Operation: "rename", Path: newPath,
				OldPath: sql.NullString{String: oldPath, Valid: true}, Fidelity: "full",
			}, []state.CaptureOp{{
				Op: "rename", Path: newPath,
				OldPath:    sql.NullString{String: oldPath, Valid: true},
				BeforeOID:  sql.NullString{String: blob, Valid: true},
				BeforeMode: sql.NullString{String: git.RegularFileMode, Valid: true},
				AfterOID:   sql.NullString{String: blob, Valid: true},
				AfterMode:  sql.NullString{String: git.RegularFileMode, Valid: true},
				Fidelity:   "full",
			}})
			if err != nil {
				t.Fatal(err)
			}
		}
		if err := os.Rename(filepath.Join(f.dir, oldPath),
			filepath.Join(f.dir, newPath)); err != nil {
			t.Fatal(err)
		}
		replayAllIntentPendingForTest(t, f)
		if got := mustGitOutput(t, f.dir, "show", "HEAD:"+newPath); got != string(contents) {
			t.Fatalf("renamed contents=%q want %q", got, contents)
		}
		if _, err := git.Run(ctx, git.RunOpts{Dir: f.dir},
			"cat-file", "-e", "HEAD:"+oldPath); err == nil {
			t.Fatalf("%s still exists at HEAD", oldPath)
		}
	})
}

func replayAllIntentPendingForTest(t *testing.T, f *captureFixture) {
	t.Helper()
	ctx := context.Background()
	opts := ReplayOpts{
		GitDir: f.gitDir, CommitStrategy: ai.CommitStrategyIntent,
		IntentPlanner: ai.DeterministicProvider{}, IntentPreset: config.PresetFast,
		IntentBypassBatchWait: true, IntentWindow: 10,
	}
	for attempt := 0; attempt < 6; attempt++ {
		result, err := Replay(ctx, f.dir, f.db, f.cctx, opts)
		if err != nil {
			t.Fatalf("replay attempt %d=%+v err=%v", attempt, result, err)
		}
		if result.BaseHead != "" {
			f.cctx.BaseHead = result.BaseHead
		}
		pending, err := state.PendingEvents(ctx, f.db, 0)
		if err != nil {
			t.Fatal(err)
		}
		if len(pending) == 0 {
			return
		}
		if result.Published == 0 {
			t.Fatalf("replay attempt %d made no progress: %+v pending=%+v",
				attempt, result, pending)
		}
	}
	t.Fatalf("Intent replay did not drain pending captures")
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
	if err != nil || first.Published != 2 {
		t.Fatalf("first replay=%+v err=%v", first, err)
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

func TestIntentRepairForwardRecoveryClassification(t *testing.T) {
	t.Parallel()
	for _, reason := range []string{
		"repair_horizon_expired",
		"repair_commit_outside_suffix",
		"repair_final_tree_mismatch",
		"repair_suffix_not_acd_owned",
		"repair_repartition_not_proven",
		"repair_repartition_dependency",
		"repair_repartition_path_overlap",
		git.IntentRepairReasonNonLinearChain,
		git.IntentRepairReasonMergeCommit,
		git.IntentRepairReasonAlternateRef,
		git.IntentRepairReasonStagedOverlap,
		git.IntentRepairReasonOwnershipMissing,
	} {
		if !intentRepairSupportsForwardRecovery(reason) {
			t.Fatalf("reason %q did not enable forward recovery", reason)
		}
	}
	for _, reason := range []string{
		git.IntentRepairReasonDetached,
		git.IntentRepairReasonBranchChanged,
		git.IntentRepairReasonHeadChanged,
		"repair_verification_unavailable",
		"repair_verification_needs_attention",
	} {
		if intentRepairSupportsForwardRecovery(reason) {
			t.Fatalf("unsafe reason %q enabled forward recovery", reason)
		}
	}
}

func TestIntentRepairRejectsFinalTreeThatDropsHeadContent(t *testing.T) {
	ctx := context.Background()
	repo := cloneDaemonTestRepo(t, daemonRepoTemplate)
	beforeHead := mustCommitPath(
		t, repo.dir, "owned.txt", "before\n", "seed owned state")
	afterHead := mustCommitPath(
		t, repo.dir, "owned.txt", "after\n", "publish owned state")
	unrelatedHead := mustCommitPath(
		t, repo.dir, "keep.txt", "keep\n", "preserve unrelated change")
	beforeOID := mustBlobOID(t, repo.dir, beforeHead, "owned.txt")
	afterOID := mustBlobOID(t, repo.dir, afterHead, "owned.txt")
	op := state.CaptureOp{
		Op: "modify", Path: "owned.txt", Fidelity: "full",
		BeforeOID: sql.NullString{String: beforeOID, Valid: true},
		BeforeMode: sql.NullString{
			String: git.RegularFileMode, Valid: true,
		},
		AfterOID: sql.NullString{String: afterOID, Valid: true},
		AfterMode: sql.NullString{
			String: git.RegularFileMode, Valid: true,
		},
	}
	publishedSeq, err := state.AppendCaptureEvent(ctx, repo.db,
		state.CaptureEvent{
			BranchRef: "refs/heads/main", BranchGeneration: 1,
			BaseHead: beforeHead, Operation: "modify", Path: "owned.txt",
			Fidelity: "full", State: state.EventStatePublished,
			CommitOID: sql.NullString{
				String: unrelatedHead, Valid: true,
			},
			PublishedTS: sql.NullFloat64{
				Float64: float64(time.Now().UnixNano()) / 1e9, Valid: true,
			},
		}, []state.CaptureOp{op})
	if err != nil {
		t.Fatal(err)
	}
	pendingSeq, err := state.AppendCaptureEvent(ctx, repo.db,
		state.CaptureEvent{
			BranchRef: "refs/heads/main", BranchGeneration: 1,
			BaseHead: unrelatedHead, Operation: "modify", Path: "owned.txt",
			Fidelity: "full", State: state.EventStatePending,
		}, []state.CaptureOp{op})
	if err != nil {
		t.Fatal(err)
	}
	const candidateID = "candidate-misowned-no-op"
	candidate := state.IntentCandidate{
		ID: candidateID, BranchRef: "refs/heads/main",
		BranchGeneration: 1, Status: state.IntentCandidateReady,
		Purpose:   "reuse already published owned state",
		Readiness: state.IntentReadinessReady,
		PublishedCommitOID: sql.NullString{
			String: unrelatedHead, Valid: true,
		},
		SoftPublicationDeadline: sql.NullFloat64{
			Float64: float64(time.Now().Add(time.Minute).UnixNano()) / 1e9,
			Valid:   true,
		},
		Events: []state.IntentCandidateEvent{
			{EventSeq: publishedSeq, EventRole: "code"},
			{EventSeq: pendingSeq, EventRole: "code"},
		},
	}
	if err := state.SaveIntentCandidate(ctx, repo.db, candidate); err != nil {
		t.Fatal(err)
	}
	selected := make([]intentReplayItem, 0, 2)
	for _, seq := range []int64{publishedSeq, pendingSeq} {
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
		ctx, repo.dir, repo.gitDir, repo.db,
		CaptureContext{
			BranchRef: "refs/heads/main", BranchGeneration: 1,
			BaseHead: unrelatedHead,
		},
		ReplayOpts{
			IntentRepairEnabled: true, IntentRepairHorizon: time.Minute,
			IntentRepairMaxCommits: 1,
		},
		IntentCandidateDecision{
			Candidate: candidate,
			Assignment: ai.IntentCandidateAssignment{
				CandidateID:  candidateID,
				SelectedSeqs: []int64{publishedSeq, pendingSeq},
				Subject:      "Reuse owned state",
			},
		},
		selected, unrelatedHead, map[string]struct{}{candidateID: {}},
	)
	if err != nil || pending != 0 ||
		result.Status != state.IntentRepairSkipped ||
		result.Reason != "repair_final_tree_mismatch" {
		t.Fatalf("result=%+v pending=%d err=%v", result, pending, err)
	}
	head, err := git.RevParse(ctx, repo.dir, "HEAD")
	if err != nil || head != unrelatedHead {
		t.Fatalf("HEAD=%s err=%v want %s", head, err, unrelatedHead)
	}
	if got := strings.TrimSpace(mustGitOutput(
		t, repo.dir, "show", "HEAD:keep.txt")); got != "keep" {
		t.Fatalf("unrelated HEAD content=%q", got)
	}
	var repairs int
	if err := repo.db.ReadSQL().QueryRowContext(
		ctx, `SELECT COUNT(*) FROM intent_repairs`).Scan(&repairs); err != nil ||
		repairs != 0 {
		t.Fatalf("repair rows=%d err=%v", repairs, err)
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

func TestReplayIntentV2SemanticRepairReplan(t *testing.T) {
	t.Run("repairs private suffix", func(t *testing.T) {
		testReplayIntentV2SemanticRepairReplan(t, true)
	})
	t.Run("preserves suffix after failed replan", func(t *testing.T) {
		testReplayIntentV2SemanticRepairReplan(t, false)
	})
}

func testReplayIntentV2SemanticRepairReplan(t *testing.T, repairSucceeds bool) {
	t.Helper()
	f := newCaptureFixture(t)
	ctx := context.Background()
	if _, err := BootstrapShadow(ctx, f.dir, f.db, f.cctx); err != nil {
		t.Fatal(err)
	}
	retryLimit := 1
	planner := &fallbackRepairReplanIntentV2Planner{
		repairSucceeds: repairSucceeds,
	}
	opts := ReplayOpts{
		GitDir: f.gitDir, CommitStrategy: ai.CommitStrategyIntent,
		IntentPlanner: planner, IntentPreset: config.PresetFast,
		IntentBypassBatchWait: true, IntentWindow: 10,
		IntentRetryLimit:    &retryLimit,
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
	first := publish(
		"feature.go", "package feature\n\nfunc Value() int { return 1 }\n")
	second := publish("guide.md", "# Feature guide\n")
	headBefore := second.BaseHead
	third := publish(
		"feature.go", "package feature\n\nfunc Value() int { return 2 }\n")
	if first.Published != 1 || second.Published != 1 || third.Published != 1 {
		t.Fatalf("replays first=%+v second=%+v third=%+v",
			first, second, third)
	}
	if got := strings.TrimSpace(mustGitOutput(
		t, f.dir, "show", "HEAD:feature.go")); !strings.Contains(got, "return 2") {
		t.Fatalf("final feature tree=%q", got)
	}
	if got := strings.TrimSpace(mustGitOutput(
		t, f.dir, "show", "HEAD:guide.md")); got != "# Feature guide" {
		t.Fatalf("final guide tree=%q", got)
	}
	if status := strings.TrimSpace(mustGitOutput(
		t, f.dir, "status", "--short")); status != "" {
		t.Fatalf("semantic replan left worktree dirty: %s", status)
	}
	var resolution string
	if err := f.db.ReadSQL().QueryRowContext(ctx, `
SELECT resolution_mode
FROM intent_planner_windows
ORDER BY id DESC LIMIT 1`).Scan(&resolution); err != nil {
		t.Fatal(err)
	}
	if repairSucceeds {
		if commits := strings.Fields(mustGitOutput(
			t, f.dir, "rev-list", "--first-parent", "HEAD")); len(commits) != 3 {
			t.Fatalf("successful repair commits=%v", commits)
		}
		if resolution != "repair_replan" {
			t.Fatalf("successful repair resolution=%q", resolution)
		}
		var completed int
		if err := f.db.ReadSQL().QueryRowContext(ctx, `
SELECT COUNT(*) FROM intent_repairs WHERE status='completed'`).
			Scan(&completed); err != nil || completed != 1 {
			t.Fatalf("completed repairs=%d err=%v", completed, err)
		}
		return
	}
	if _, err := git.Run(ctx, git.RunOpts{Dir: f.dir},
		"merge-base", "--is-ancestor", headBefore, third.BaseHead); err != nil {
		t.Fatalf("failed replan rewrote prior commits: %v", err)
	}
	if subject := strings.TrimSpace(mustGitOutput(
		t, f.dir, "show", "-s", "--format=%s", "HEAD")); subject != "Complete dependent feature update" {
		t.Fatalf("fallback subject=%q", subject)
	}
	if resolution != "dependent_message_fallback" {
		t.Fatalf("failed repair resolution=%q", resolution)
	}
}

func TestReplayIntentV2RecoversForwardWhenRepartitionIsUnproven(t *testing.T) {
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
	if result.Published != 1 || result.Skipped || result.BaseHead == headBefore ||
		result.Disposition != ReplayDispositionProgress {
		t.Fatalf("result=%+v headBefore=%s", result, headBefore)
	}
	headAfter, err := git.RevParse(ctx, f.dir, "HEAD")
	if err != nil || headAfter != result.BaseHead {
		t.Fatalf("HEAD=%s err=%v want %s", headAfter, err, result.BaseHead)
	}
	if _, err := git.Run(ctx, git.RunOpts{Dir: f.dir},
		"merge-base", "--is-ancestor", headBefore, headAfter); err != nil {
		t.Fatalf("forward recovery rewrote published history: %v", err)
	}
	var decisions int
	if err := f.db.ReadSQL().QueryRowContext(ctx, `
SELECT COUNT(*) FROM decision_records
WHERE kind=? AND reason='repair_repartition_not_proven'`,
		state.DecisionKindIntentForwardRecovery).Scan(&decisions); err != nil || decisions != 1 {
		t.Fatalf("forward recovery decisions=%d err=%v", decisions, err)
	}
	if _, active, err := state.IntentForwardRecoveryForPair(
		ctx, f.db, f.cctx.BranchRef, f.cctx.BranchGeneration,
	); err != nil || active {
		t.Fatalf("forward recovery marker active=%t err=%v", active, err)
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
	repair, ok, err := state.IntentRepairByID(ctx, repo.db, result.ID)
	if err != nil || !ok || len(repair.Members) != 3 {
		t.Fatalf("repair=%+v ok=%v err=%v", repair, ok, err)
	}
	priorStates := make(map[string]int)
	for _, member := range repair.Members {
		priorStates[member.PriorState]++
	}
	if priorStates[state.EventStatePublished] != 2 ||
		priorStates[state.EventStatePending] != 1 {
		t.Fatalf("repair prior states=%v", priorStates)
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

func (*disconnectedIntentV2Planner) RewriteIntentMessage(
	context.Context,
	ai.IntentMessageRewriteRequest,
) (ai.Result, error) {
	return ai.Result{
		Subject: "Publish independent fallback",
		Body:    "- Keep deterministic components semantically named",
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

func (*tracedInvalidIntentV2Planner) RewriteIntentMessage(
	context.Context,
	ai.IntentMessageRewriteRequest,
) (ai.Result, error) {
	return ai.Result{
		Subject: "Publish traced fallback",
		Body:    "- Preserve trace coverage during semantic fallback",
	}, nil
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

type fallbackRepairReplanIntentV2Planner struct {
	repairSucceeds bool
}

func (*suffixRepairIntentV2Planner) Name() string { return "suffix-repair-v2-test" }

func (*fallbackRepairReplanIntentV2Planner) Name() string {
	return "fallback-repair-replan-v2-test"
}

func (*fallbackRepairReplanIntentV2Planner) PlanIntent(
	context.Context,
	ai.IntentPlanRequest,
) (ai.IntentPlan, error) {
	return ai.IntentPlan{}, errors.New("legacy planner path must not run")
}

func (p *fallbackRepairReplanIntentV2Planner) PlanIntentV2(
	ctx context.Context,
	req ai.IntentPlanRequestV2,
) (ai.IntentPlanV2, error) {
	byPath := make(map[string]int64)
	for _, capture := range req.OfferedCaptures {
		byPath[capture.Path] = capture.Seq
	}
	if byPath["feature.go"] == 0 || len(req.RecentSoftCommits) < 2 {
		return (&suffixRepairIntentV2Planner{}).PlanIntentV2(ctx, req)
	}
	if p.repairSucceeds && strings.Contains(
		req.RetryCorrection, "repairable private ACD suffix") {
		return (&suffixRepairIntentV2Planner{}).PlanIntentV2(ctx, req)
	}
	return ai.IntentPlanV2{
		ProtocolVersion: ai.IntentPlannerProtocolV2,
		Candidates: []ai.IntentCandidateAssignment{{
			CandidateID:  "invalid-private-replan",
			SelectedSeqs: []int64{byPath["feature.go"]},
			Purpose:      "invalid private suffix plan",
			Readiness:    ai.IntentCandidateReady,
			Subject:      "Attempt private suffix repair",
			GroupingReason: "exercise semantic repair replanning before " +
				"the protected fallback",
			DependsOnCandidates: []string{"invalid-private-replan"},
		}},
	}, nil
}

func (*fallbackRepairReplanIntentV2Planner) RewriteIntentMessage(
	context.Context,
	ai.IntentMessageRewriteRequest,
) (ai.Result, error) {
	return ai.Result{
		Subject: "Complete dependent feature update",
		Body:    "- Preserve the prior semantic commits during fallback",
	}, nil
}

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
	unrelatedSeq, err := state.AppendCaptureEvent(ctx, f.repo.db,
		state.CaptureEvent{
			BranchRef: f.cctx.BranchRef, BranchGeneration: f.cctx.BranchGeneration,
			BaseHead: f.plan.ExpectedHead, Operation: "create",
			Path: "unrelated-ledger.txt", Fidelity: "full",
			State: state.EventStatePublished,
			CommitOID: sql.NullString{
				String: f.oldCommits[0], Valid: true,
			},
		}, nil)
	if err != nil {
		t.Fatal(err)
	}
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
	var unrelatedOID string
	if err := f.repo.db.SQL().QueryRowContext(ctx,
		`SELECT commit_oid FROM capture_events WHERE seq=?`, unrelatedSeq).
		Scan(&unrelatedOID); err != nil {
		t.Fatal(err)
	}
	if unrelatedOID != f.oldCommits[0] {
		t.Fatalf("unrelated event oid=%s want unchanged %s",
			unrelatedOID, f.oldCommits[0])
	}
}

func TestIntentRepairPreservesCapturedDirtyShadow(t *testing.T) {
	f := newIntentRepairFixture(t, 2)
	ctx := context.Background()
	ignore := git.NewIgnoreChecker(f.repo.dir)
	t.Cleanup(func() { _ = ignore.Close() })
	path := "unrelated.txt"
	if err := os.WriteFile(filepath.Join(f.repo.dir, path),
		[]byte("captured once\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	first, err := Capture(ctx, f.repo.dir, f.repo.db, f.cctx, CaptureOpts{
		IgnoreChecker: ignore, SensitiveMatcher: state.NewSensitiveMatcher(),
	})
	if err != nil || first.EventsAppended != 1 {
		t.Fatalf("first capture=%+v err=%v", first, err)
	}
	shadowBefore, ok, err := state.GetShadowPath(ctx, f.repo.db,
		f.cctx.BranchRef, f.cctx.BranchGeneration, path)
	if err != nil || !ok {
		t.Fatalf("shadow before repair=%+v ok=%v err=%v",
			shadowBefore, ok, err)
	}
	pendingBefore, err := state.CountPendingEventsForGeneration(ctx, f.repo.db,
		f.cctx.BranchRef, f.cctx.BranchGeneration)
	if err != nil || pendingBefore != 1 {
		t.Fatalf("pending before repair=%d err=%v", pendingBefore, err)
	}

	result, err := ApplyIntentRepairTransaction(ctx, f.repo.dir, f.repo.gitDir,
		f.repo.db, f.cctx, f.plan)
	if err != nil || result.Status != state.IntentRepairCompleted {
		t.Fatalf("repair=%+v err=%v", result, err)
	}
	shadowAfter, ok, err := state.GetShadowPath(ctx, f.repo.db,
		f.cctx.BranchRef, f.cctx.BranchGeneration, path)
	if err != nil || !ok {
		t.Fatalf("shadow after repair=%+v ok=%v err=%v", shadowAfter, ok, err)
	}
	if shadowAfter.OID != shadowBefore.OID ||
		shadowAfter.Mode != shadowBefore.Mode ||
		shadowAfter.BaseHead != result.NewHead {
		t.Fatalf("shadow after repair=%+v before=%+v new_head=%s",
			shadowAfter, shadowBefore, result.NewHead)
	}

	repairedCtx := f.cctx
	repairedCtx.BaseHead = result.NewHead
	second, err := Capture(ctx, f.repo.dir, f.repo.db, repairedCtx, CaptureOpts{
		IgnoreChecker: ignore, SensitiveMatcher: state.NewSensitiveMatcher(),
	})
	if err != nil || second.EventsAppended != 0 {
		t.Fatalf("capture after unchanged repair=%+v err=%v", second, err)
	}
	pendingAfter, err := state.CountPendingEventsForGeneration(ctx, f.repo.db,
		f.cctx.BranchRef, f.cctx.BranchGeneration)
	if err != nil || pendingAfter != pendingBefore {
		t.Fatalf("pending after repair=%d want=%d err=%v",
			pendingAfter, pendingBefore, err)
	}
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
				EventSeqs:   []int64{1, 3},
				TreeOID:     "tree-a",
				Message:     "Complete alpha",
			},
			{
				CandidateID: "candidate-b",
				Replaces:    []string{"old-b1"},
				EventSeqs:   []int64{2},
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
			EventSeqs:   []int64{1, 3},
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

func TestIntentRepairNoncontiguousCrashRecoversFrozenMembers(t *testing.T) {
	f := newNoncontiguousIntentRepairFixture(t)
	ctx := context.Background()
	crash := errors.New("simulated crash after non-contiguous Git CAS")
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
	if applied.CommitMap[f.oldA1] == "" ||
		applied.CommitMap[f.oldA1] != applied.CommitMap[f.oldA2] ||
		applied.CommitMap[f.oldB1] == "" ||
		applied.CommitMap[f.oldB1] == applied.CommitMap[f.oldA1] {
		t.Fatalf("non-contiguous commit map=%+v", applied.CommitMap)
	}
	repair, ok, err := state.IntentRepairByID(ctx, f.repo.db, f.plan.ID)
	if err != nil || !ok || repair.Status != state.IntentRepairPrepared ||
		repair.MembershipMode != state.IntentRepairMembershipFrozen {
		t.Fatalf("prepared repair=%+v ok=%v err=%v", repair, ok, err)
	}
	assertNoncontiguousIntentRepairMembers(t, repair.Members, f)

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

	repair, ok, err = state.IntentRepairByID(ctx, f.repo.db, f.plan.ID)
	if err != nil || !ok || repair.Status != state.IntentRepairCompleted ||
		repair.MembershipMode != state.IntentRepairMembershipFrozen {
		t.Fatalf("completed repair=%+v ok=%v err=%v", repair, ok, err)
	}
	assertNoncontiguousIntentRepairMembers(t, repair.Members, f)

	wantCommit := map[int64]string{
		f.seqA1: applied.CommitMap[f.oldA1],
		f.seqA2: applied.CommitMap[f.oldA2],
		f.seqB1: applied.CommitMap[f.oldB1],
	}
	for seq, wantOID := range wantCommit {
		var eventState string
		var commitOID sql.NullString
		if err := f.repo.db.SQL().QueryRowContext(ctx,
			`SELECT state, commit_oid FROM capture_events WHERE seq=?`, seq).
			Scan(&eventState, &commitOID); err != nil {
			t.Fatal(err)
		}
		if eventState != state.EventStatePublished || !commitOID.Valid ||
			commitOID.String != wantOID {
			t.Fatalf("event %d state=%s oid=%+v want %s",
				seq, eventState, commitOID, wantOID)
		}
	}
	for candidateID, wantOID := range map[string]string{
		"candidate-a": applied.CommitMap[f.oldA1],
		"candidate-b": applied.CommitMap[f.oldB1],
	} {
		candidate, ok, loadErr := state.IntentCandidateByID(
			ctx, f.repo.db, candidateID)
		if loadErr != nil || !ok ||
			candidate.Status != state.IntentCandidatePublished ||
			!candidate.PublishedCommitOID.Valid ||
			candidate.PublishedCommitOID.String != wantOID {
			t.Fatalf("candidate %s=%+v ok=%v err=%v want %s",
				candidateID, candidate, ok, loadErr, wantOID)
		}
	}
}

func TestValidateIntentRepairPlanRejectsDuplicateCandidateID(t *testing.T) {
	plan := IntentRepairPlan{
		BranchRef:        "refs/heads/main",
		BranchGeneration: 1,
		ExpectedHead:     "old-b",
		OldChain:         []string{"old-a", "old-b"},
		MaxCommits:       2,
		Candidates: []IntentRepairCandidatePlan{
			{
				CandidateID: "candidate-shared",
				Replaces:    []string{"old-a"},
				EventSeqs:   []int64{1},
				TreeOID:     "tree-a",
				Message:     "Add alpha",
			},
			{
				CandidateID: "candidate-shared",
				Replaces:    []string{"old-b"},
				EventSeqs:   []int64{2},
				TreeOID:     "tree-b",
				Message:     "Add beta",
			},
		},
	}
	err := validateIntentRepairPlan(plan, CaptureContext{
		BranchRef: plan.BranchRef, BranchGeneration: plan.BranchGeneration,
	})
	if err == nil || !strings.Contains(err.Error(), "duplicate candidate id") {
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

func TestIntentRepairRejectsMembershipDriftAfterGitCAS(t *testing.T) {
	f := newIntentRepairFixture(t, 1)
	ctx := context.Background()
	var lateSeq int64
	intentRepairAfterGitApply = func(IntentRepairResult) error {
		var err error
		lateSeq, err = state.AppendCaptureEvent(ctx, f.repo.db,
			state.CaptureEvent{
				BranchRef:        f.cctx.BranchRef,
				BranchGeneration: f.cctx.BranchGeneration,
				BaseHead:         f.plan.ExpectedHead, Operation: "create",
				Path: "late.go", Fidelity: "full",
				State: state.EventStatePending,
			}, nil)
		if err != nil {
			return err
		}
		_, err = f.repo.db.SQL().ExecContext(ctx, `
INSERT INTO intent_candidate_events(
    candidate_id,ord,event_seq,event_role,membership_state
) VALUES ('candidate-repair',99,?,'test','active')`, lateSeq)
		return err
	}
	t.Cleanup(func() { intentRepairAfterGitApply = nil })

	applied, err := ApplyIntentRepairTransaction(ctx, f.repo.dir, f.repo.gitDir,
		f.repo.db, f.cctx, f.plan)
	if err == nil || !strings.Contains(err.Error(),
		"intent candidate membership is locked by repair") {
		t.Fatalf("ApplyIntentRepairTransaction result=%+v err=%v", applied, err)
	}
	repair, ok, loadErr := state.IntentRepairByID(ctx, f.repo.db, f.plan.ID)
	if loadErr != nil || !ok || repair.Status != state.IntentRepairPrepared ||
		len(repair.Members) != len(f.eventSeqs) {
		t.Fatalf("repair=%+v ok=%v err=%v", repair, ok, loadErr)
	}
	var lateState string
	if err := f.repo.db.SQL().QueryRowContext(ctx,
		`SELECT state FROM capture_events WHERE seq=?`, lateSeq).
		Scan(&lateState); err != nil {
		t.Fatal(err)
	}
	if lateState != state.EventStatePending {
		t.Fatalf("late event state=%s want pending", lateState)
	}

	intentRepairAfterGitApply = nil
	recovered, recoverErr := RecoverIntentRepairs(ctx, f.repo.dir,
		f.repo.gitDir, f.repo.db, f.cctx)
	if recoverErr != nil {
		t.Fatalf("RecoverIntentRepairs: %v", recoverErr)
	}
	if len(recovered) != 1 || !recovered[0].Recovered ||
		recovered[0].Status != state.IntentRepairCompleted ||
		recovered[0].NewHead != applied.NewHead {
		t.Fatalf("recovered=%+v", recovered)
	}
	assertIntentRepairReconciled(t, f, applied.NewHead)
	if err := f.repo.db.SQL().QueryRowContext(ctx,
		`SELECT state FROM capture_events WHERE seq=?`, lateSeq).
		Scan(&lateState); err != nil {
		t.Fatal(err)
	}
	if lateState != state.EventStatePending {
		t.Fatalf("late event state after recovery=%s want pending", lateState)
	}
}

func TestIntentRepairRejectsMembershipDriftBeforeGitCAS(t *testing.T) {
	f := newIntentRepairFixture(t, 1)
	ctx := context.Background()
	lateSeq, err := state.AppendCaptureEvent(ctx, f.repo.db,
		state.CaptureEvent{
			BranchRef:        f.cctx.BranchRef,
			BranchGeneration: f.cctx.BranchGeneration,
			BaseHead:         f.plan.ExpectedHead,
			Operation:        "create",
			Path:             "late.go",
			Fidelity:         "full",
			State:            state.EventStatePending,
		}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.repo.db.SQL().ExecContext(ctx, `
INSERT INTO intent_candidate_events(
    candidate_id,ord,event_seq,event_role,membership_state
) VALUES ('candidate-repair',99,?,'test','active')`, lateSeq); err != nil {
		t.Fatal(err)
	}

	_, err = ApplyIntentRepairTransaction(ctx, f.repo.dir, f.repo.gitDir,
		f.repo.db, f.cctx, f.plan)
	if err == nil || !strings.Contains(err.Error(),
		"membership changed since materialization") {
		t.Fatalf("ApplyIntentRepairTransaction error=%v", err)
	}
	head, resolveErr := git.RevParse(ctx, f.repo.dir, "HEAD")
	if resolveErr != nil || head != f.plan.ExpectedHead {
		t.Fatalf("HEAD=%s err=%v want %s", head, resolveErr,
			f.plan.ExpectedHead)
	}
	if _, ok, loadErr := state.IntentRepairByID(
		ctx, f.repo.db, f.plan.ID); loadErr != nil || ok {
		t.Fatalf("repair persisted before membership validation: ok=%v err=%v",
			ok, loadErr)
	}
}

func TestIntentRepairRecoveryIsIdempotentAfterLedgerSettlement(t *testing.T) {
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
	mappings := intentRepairStateCommits(f.plan, applied.CommitMap)
	ok, err := state.TransitionIntentRepair(ctx, f.repo.db, f.plan.ID,
		state.IntentRepairTransition{
			ExpectedStatus: state.IntentRepairPrepared,
			Status:         state.IntentRepairGitApplied,
			BackupRef: sql.NullString{
				String: applied.BackupRef, Valid: true,
			},
			OldHead: sql.NullString{String: applied.OldHead, Valid: true},
			NewHead: sql.NullString{String: applied.NewHead, Valid: true},
			Commits: mappings,
		})
	if err != nil || !ok {
		t.Fatalf("git-applied transition=(%v,%v)", ok, err)
	}
	repair, ok, err := state.IntentRepairByID(ctx, f.repo.db, f.plan.ID)
	if err != nil || !ok {
		t.Fatalf("repair=%+v ok=%v err=%v", repair, ok, err)
	}
	candidateMap := map[string]string{
		"candidate-repair": mappings[0].NewOID.String,
	}
	if err := reconcileIntentRepairLedger(ctx, f.repo.db, f.plan.ID,
		applied.CommitMap, candidateMap, repair.MembershipMode,
		repair.Members, f.cctx,
		applied.NewHead); err != nil {
		t.Fatal(err)
	}
	recovered, err := RecoverIntentRepairs(ctx, f.repo.dir, f.repo.gitDir,
		f.repo.db, f.cctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(recovered) != 1 ||
		recovered[0].Status != state.IntentRepairCompleted {
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

func TestRunStartupSurfacesDurableIntentRepairRecoveryFailure(t *testing.T) {
	f := newIntentRepairFixture(t, 1)
	ctx := context.Background()
	crash := errors.New("simulated crash before daemon restart")
	intentRepairAfterGitApply = func(IntentRepairResult) error { return crash }
	t.Cleanup(func() { intentRepairAfterGitApply = nil })

	applied, err := ApplyIntentRepairTransaction(ctx, f.repo.dir, f.repo.gitDir,
		f.repo.db, f.cctx, f.plan)
	if !errors.Is(err, crash) {
		t.Fatalf("ApplyIntentRepairTransaction err=%v want crash", err)
	}
	intentRepairAfterGitApply = nil
	if err := git.UpdateRef(ctx, f.repo.dir, applied.BackupRef,
		f.repo.head, f.plan.ExpectedHead); err != nil {
		t.Fatal(err)
	}
	shutdown := make(chan struct{})
	close(shutdown)
	if err := Run(ctx, Options{
		RepoPath: f.repo.dir, GitDir: f.repo.gitDir, DB: f.repo.db,
		MessageFn: DeterministicMessage, ShutdownCh: shutdown,
		SkipSignals: true, BootGrace: time.Hour,
	}); err != nil {
		t.Fatalf("Run: %v", err)
	}
	attention, ok, err := state.MetaGet(
		ctx, f.repo.db, MetaKeyBranchTransitionNeedsAttention)
	if err != nil || !ok ||
		!strings.HasPrefix(attention, intentRepairRecoveryAttentionPrefix) ||
		!strings.Contains(attention, "unexpected commit") {
		t.Fatalf("startup repair attention=(%q,%t,%v)", attention, ok, err)
	}
	repair, ok, err := state.IntentRepairByID(ctx, f.repo.db, f.plan.ID)
	if err != nil || !ok || repair.Status != state.IntentRepairPrepared {
		t.Fatalf("repair=%+v ok=%v err=%v", repair, ok, err)
	}
}

func TestRunRecoversIntentRepairAtActiveLoopBoundary(t *testing.T) {
	f := newIntentRepairFixture(t, 1)
	registerLiveClient(t, f.repo.db)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	wake := make(chan struct{}, 2)
	shutdown := make(chan struct{})
	faulted := make(chan IntentRepairResult, 1)
	adopted := make(chan CaptureContext, 1)
	crash := errors.New("simulated active-loop crash after Git CAS")
	intentRepairAfterGitApply = func(result IntentRepairResult) error {
		faulted <- result
		wake <- struct{}{}
		return crash
	}
	t.Cleanup(func() { intentRepairAfterGitApply = nil })

	replayCalls := 0
	runErr := make(chan error, 1)
	go func() {
		runErr <- Run(ctx, Options{
			RepoPath: f.repo.dir, GitDir: f.repo.gitDir, DB: f.repo.db,
			MessageFn: DeterministicMessage, Scheduler: fastScheduler(),
			BootGrace: time.Hour, WakeCh: wake, ShutdownCh: shutdown,
			SkipSignals: true,
			replay: func(
				replayCtx context.Context,
				_ string,
				_ *state.DB,
				cctx CaptureContext,
				_ ReplayOpts,
			) (ReplaySummary, error) {
				replayCalls++
				if replayCalls != 1 {
					return ReplaySummary{BaseHead: cctx.BaseHead}, nil
				}
				_, err := ApplyIntentRepairTransaction(
					replayCtx, f.repo.dir, f.repo.gitDir,
					f.repo.db, cctx, f.plan)
				return ReplaySummary{BaseHead: cctx.BaseHead}, err
			},
			afterSelfPublicationAdoption: func(
				cctx CaptureContext, _, _, _ string,
			) {
				select {
				case adopted <- cctx:
				default:
				}
			},
		})
	}()

	var applied IntentRepairResult
	select {
	case applied = <-faulted:
	case <-time.After(5 * time.Second):
		cancel()
		t.Fatal("active replay did not reach the post-CAS fault")
	}
	if applied.NewHead == "" || applied.NewHead == f.plan.ExpectedHead {
		cancel()
		t.Fatalf("Git CAS did not land before fault: %+v", applied)
	}
	intentRepairAfterGitApply = nil

	var adoptedContext CaptureContext
	select {
	case adoptedContext = <-adopted:
	case <-time.After(5 * time.Second):
		cancel()
		t.Fatal("active run loop did not recover and adopt the repair")
	}
	close(shutdown)
	select {
	case err := <-runErr:
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
	case <-time.After(5 * time.Second):
		cancel()
		t.Fatal("Run did not stop")
	}

	assertIntentRepairReconciled(t, f, applied.NewHead)
	if adoptedContext.BaseHead != applied.NewHead ||
		adoptedContext.BranchGeneration != f.cctx.BranchGeneration {
		t.Fatalf("adopted context=%+v want head=%s generation=%d",
			adoptedContext, applied.NewHead, f.cctx.BranchGeneration)
	}
	if generation, err := LoadBranchGeneration(
		ctx, f.repo.db); err != nil || generation != f.cctx.BranchGeneration {
		t.Fatalf("generation=%d err=%v want %d",
			generation, err, f.cctx.BranchGeneration)
	}
}

func TestRunSurfacesDurableActiveIntentRepairRecoveryFailure(t *testing.T) {
	f := newIntentRepairFixture(t, 1)
	registerLiveClient(t, f.repo.db)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	wake := make(chan struct{}, 2)
	shutdown := make(chan struct{})
	faulted := make(chan IntentRepairResult, 1)
	adopted := make(chan CaptureContext, 1)
	crash := errors.New("simulated active repair recovery fault")
	wrongBackup := f.repo.head
	intentRepairAfterGitApply = func(result IntentRepairResult) error {
		if err := git.UpdateRef(ctx, f.repo.dir, result.BackupRef,
			wrongBackup, f.plan.ExpectedHead); err != nil {
			return err
		}
		faulted <- result
		wake <- struct{}{}
		return crash
	}
	t.Cleanup(func() { intentRepairAfterGitApply = nil })

	replayCalls := 0
	runErr := make(chan error, 1)
	go func() {
		runErr <- Run(ctx, Options{
			RepoPath: f.repo.dir, GitDir: f.repo.gitDir, DB: f.repo.db,
			MessageFn: DeterministicMessage, Scheduler: fastScheduler(),
			BootGrace: time.Hour, WakeCh: wake, ShutdownCh: shutdown,
			SkipSignals: true,
			replay: func(
				replayCtx context.Context,
				_ string,
				_ *state.DB,
				cctx CaptureContext,
				_ ReplayOpts,
			) (ReplaySummary, error) {
				replayCalls++
				if replayCalls != 1 {
					return ReplaySummary{BaseHead: cctx.BaseHead}, nil
				}
				_, err := ApplyIntentRepairTransaction(
					replayCtx, f.repo.dir, f.repo.gitDir,
					f.repo.db, cctx, f.plan)
				return ReplaySummary{BaseHead: cctx.BaseHead}, err
			},
			afterSelfPublicationAdoption: func(
				cctx CaptureContext, _, _, _ string,
			) {
				select {
				case adopted <- cctx:
				default:
				}
			},
		})
	}()

	var applied IntentRepairResult
	select {
	case applied = <-faulted:
	case <-time.After(5 * time.Second):
		cancel()
		t.Fatal("active replay did not create the contradictory repair")
	}
	intentRepairAfterGitApply = nil
	waitFor(t, 5*time.Second, "durable repair recovery attention", func() bool {
		value, ok, err := state.MetaGet(
			ctx, f.repo.db, MetaKeyBranchTransitionNeedsAttention)
		return err == nil && ok &&
			strings.HasPrefix(value, intentRepairRecoveryAttentionPrefix) &&
			strings.Contains(value, "unexpected commit")
	})
	attention, _, err := state.MetaGet(
		ctx, f.repo.db, MetaKeyBranchTransitionNeedsAttention)
	if err != nil {
		t.Fatal(err)
	}
	if err := recordIntentRepairRecoveryAttention(
		ctx, f.repo.db, errors.New("transient database read")); err != nil {
		t.Fatal(err)
	}
	if unchanged, _, _ := state.MetaGet(
		ctx, f.repo.db, MetaKeyBranchTransitionNeedsAttention); unchanged != attention {
		t.Fatalf("transient error replaced durable attention: %q", unchanged)
	}

	if err := git.UpdateRef(ctx, f.repo.dir, applied.BackupRef,
		f.plan.ExpectedHead, wrongBackup); err != nil {
		t.Fatal(err)
	}
	wake <- struct{}{}
	select {
	case recoveredContext := <-adopted:
		if recoveredContext.BaseHead != applied.NewHead ||
			recoveredContext.BranchGeneration != f.cctx.BranchGeneration {
			t.Fatalf("recovered context=%+v", recoveredContext)
		}
	case <-time.After(5 * time.Second):
		cancel()
		t.Fatal("repair did not recover after durable proof was restored")
	}
	waitFor(t, 5*time.Second, "settled repair attention clears", func() bool {
		value, _, err := state.MetaGet(
			ctx, f.repo.db, MetaKeyBranchTransitionNeedsAttention)
		return err == nil && value == ""
	})
	close(shutdown)
	select {
	case err := <-runErr:
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
	case <-time.After(5 * time.Second):
		cancel()
		t.Fatal("Run did not stop")
	}
	assertIntentRepairReconciled(t, f, applied.NewHead)
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

type noncontiguousIntentRepairFixture struct {
	repo                *daemonTestRepo
	cctx                CaptureContext
	plan                IntentRepairPlan
	oldA1, oldB1, oldA2 string
	seqA1, seqB1, seqA2 int64
}

func newNoncontiguousIntentRepairFixture(
	t *testing.T,
) noncontiguousIntentRepairFixture {
	t.Helper()
	ctx := context.Background()
	repo := cloneDaemonTestRepo(t, daemonRepoTemplate)
	f := noncontiguousIntentRepairFixture{repo: repo}

	f.oldA1 = mustCommitPath(t, repo.dir, "a.txt", "a1\n", "old alpha part")
	f.seqA1 = appendPublishedIntentRepairCapture(t, repo, repo.head, f.oldA1,
		"create", "a.txt", "")
	f.oldB1 = mustCommitPath(t, repo.dir, "b.txt", "b1\n", "old beta part")
	f.seqB1 = appendPublishedIntentRepairCapture(t, repo, f.oldA1, f.oldB1,
		"create", "b.txt", "")
	beforeA := mustBlobOID(t, repo.dir, f.oldA1, "a.txt")
	f.oldA2 = mustCommitPath(t, repo.dir, "a.txt", "a2\n", "old alpha finish")
	f.seqA2 = appendPublishedIntentRepairCapture(t, repo, f.oldB1, f.oldA2,
		"modify", "a.txt", beforeA)

	for _, candidate := range []state.IntentCandidate{
		{
			ID: "candidate-a", BranchRef: "refs/heads/main",
			BranchGeneration: 1, Status: state.IntentCandidateSoftPublished,
			Purpose: "complete alpha", Readiness: state.IntentReadinessReady,
			PublishedCommitOID: sql.NullString{String: f.oldA2, Valid: true},
			Events: []state.IntentCandidateEvent{
				{EventSeq: f.seqA1, EventRole: "code"},
				{EventSeq: f.seqA2, EventRole: "code"},
			},
		},
		{
			ID: "candidate-b", BranchRef: "refs/heads/main",
			BranchGeneration: 1, Status: state.IntentCandidateSoftPublished,
			Purpose: "add beta", Readiness: state.IntentReadinessReady,
			PublishedCommitOID: sql.NullString{String: f.oldB1, Valid: true},
			Events: []state.IntentCandidateEvent{
				{EventSeq: f.seqB1, EventRole: "code"},
			},
		},
	} {
		if err := state.SaveIntentCandidate(ctx, repo.db, candidate); err != nil {
			t.Fatal(err)
		}
	}

	baseEntries, err := git.LsTree(ctx, repo.dir, repo.head, false)
	if err != nil {
		t.Fatal(err)
	}
	aTreeEntries := make([]git.MktreeEntry, 0, len(baseEntries)+1)
	for _, entry := range baseEntries {
		aTreeEntries = append(aTreeEntries, git.MktreeEntry{
			Mode: entry.Mode, Type: entry.Type, OID: entry.OID, Path: entry.Path,
		})
	}
	aTreeEntries = append(aTreeEntries, git.MktreeEntry{
		Mode: git.RegularFileMode, Type: "blob",
		OID: mustBlobOID(t, repo.dir, f.oldA2, "a.txt"), Path: "a.txt",
	})
	aTree, err := git.Mktree(ctx, repo.dir, aTreeEntries)
	if err != nil {
		t.Fatal(err)
	}
	finalTree, err := git.RevParse(ctx, repo.dir, f.oldA2+"^{tree}")
	if err != nil {
		t.Fatal(err)
	}

	f.cctx = CaptureContext{
		BranchRef: "refs/heads/main", BranchGeneration: 1,
		BaseHead: f.oldA2,
	}
	if _, err := BootstrapShadow(ctx, repo.dir, repo.db, f.cctx); err != nil {
		t.Fatal(err)
	}
	f.plan = IntentRepairPlan{
		ID: "repair-noncontiguous", BranchRef: f.cctx.BranchRef,
		BranchGeneration: f.cctx.BranchGeneration,
		ExpectedHead:     f.oldA2,
		OldChain:         []string{f.oldA1, f.oldB1, f.oldA2},
		Paths:            []string{"a.txt", "b.txt"}, MaxCommits: 3,
		Candidates: []IntentRepairCandidatePlan{
			{
				CandidateID: "candidate-a",
				Replaces:    []string{f.oldA1, f.oldA2},
				EventSeqs:   []int64{f.seqA1, f.seqA2},
				TreeOID:     aTree, Message: "Complete alpha",
			},
			{
				CandidateID: "candidate-b", Replaces: []string{f.oldB1},
				EventSeqs: []int64{f.seqB1}, TreeOID: finalTree,
				Message: "Add beta",
			},
		},
	}
	return f
}

func appendPublishedIntentRepairCapture(
	t *testing.T,
	repo *daemonTestRepo,
	baseHead, commitOID, operation, path, beforeOID string,
) int64 {
	t.Helper()
	afterOID := mustBlobOID(t, repo.dir, commitOID, path)
	op := state.CaptureOp{
		Op: operation, Path: path, Fidelity: "full",
		AfterOID:  sql.NullString{String: afterOID, Valid: true},
		AfterMode: sql.NullString{String: git.RegularFileMode, Valid: true},
	}
	if beforeOID != "" {
		op.BeforeOID = sql.NullString{String: beforeOID, Valid: true}
		op.BeforeMode = sql.NullString{String: git.RegularFileMode, Valid: true}
	}
	seq, err := state.AppendCaptureEvent(context.Background(), repo.db,
		state.CaptureEvent{
			BranchRef: "refs/heads/main", BranchGeneration: 1,
			BaseHead: baseHead, Operation: operation, Path: path,
			Fidelity: "full", State: state.EventStatePublished,
			CommitOID:   sql.NullString{String: commitOID, Valid: true},
			PublishedTS: sql.NullFloat64{Float64: float64(time.Now().UnixNano()) / 1e9, Valid: true},
		}, []state.CaptureOp{op})
	if err != nil {
		t.Fatal(err)
	}
	return seq
}

func assertNoncontiguousIntentRepairMembers(
	t *testing.T,
	members []state.IntentRepairMember,
	f noncontiguousIntentRepairFixture,
) {
	t.Helper()
	want := []struct {
		candidateID string
		eventSeq    int64
	}{
		{candidateID: "candidate-a", eventSeq: f.seqA1},
		{candidateID: "candidate-a", eventSeq: f.seqA2},
		{candidateID: "candidate-b", eventSeq: f.seqB1},
	}
	if len(members) != len(want) {
		t.Fatalf("repair members=%+v want %d", members, len(want))
	}
	for i, member := range members {
		if member.Ord != i || member.CandidateID != want[i].candidateID ||
			member.EventSeq != want[i].eventSeq ||
			member.PriorState != state.EventStatePublished {
			t.Fatalf("repair member %d=%+v want candidate=%s event=%d",
				i, member, want[i].candidateID, want[i].eventSeq)
		}
	}
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
			EventSeqs:   append([]int64(nil), f.eventSeqs...),
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
	if len(repair.Members) != len(f.eventSeqs) {
		t.Fatalf("repair members=%+v want %d", repair.Members, len(f.eventSeqs))
	}
	for i, member := range repair.Members {
		if member.EventSeq != f.eventSeqs[i] ||
			member.PriorState != state.EventStatePublished {
			t.Fatalf("repair member %d=%+v", i, member)
		}
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
