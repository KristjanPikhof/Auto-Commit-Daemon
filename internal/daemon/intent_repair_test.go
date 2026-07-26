package daemon

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/ai"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/config"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
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
	for _, id := range []string{"candidate-alpha", "candidate-beta"} {
		candidate, ok, err := state.IntentCandidateByID(ctx, f.db, id)
		if err != nil || !ok || candidate.Status != state.IntentCandidatePublished {
			t.Fatalf("candidate %s=%+v ok=%v err=%v", id, candidate, ok, err)
		}
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
	candidate, ok, err := state.IntentCandidateByID(ctx, f.db, "candidate-feature")
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

type orderedIntentV2Planner struct{}

func (orderedIntentV2Planner) Name() string { return "ordered-v2-test" }

func (orderedIntentV2Planner) PlanIntent(
	context.Context,
	ai.IntentPlanRequest,
) (ai.IntentPlan, error) {
	return ai.IntentPlan{}, errors.New("legacy planner path must not run")
}

type revisingIntentV2Planner struct{}

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
	return ai.IntentPlanV2{
		ProtocolVersion: ai.IntentPlannerProtocolV2,
		Candidates: []ai.IntentCandidateAssignment{{
			CandidateID: "candidate-feature", SelectedSeqs: seqs,
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
