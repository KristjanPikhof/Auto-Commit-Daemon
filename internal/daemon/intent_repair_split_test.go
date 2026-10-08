package daemon

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

func TestIntentRepairMixedCommitSplitCrashPreservesCaptureLineage(t *testing.T) {
	ctx := context.Background()
	repo := cloneDaemonTestRepo(t, daemonRepoTemplate)
	for path, body := range map[string]string{"a.txt": "alpha\n", "b.txt": "beta\n"} {
		if err := os.WriteFile(filepath.Join(repo.dir, path), []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := git.Run(ctx, git.RunOpts{Dir: repo.dir}, "add", "a.txt", "b.txt"); err != nil {
		t.Fatal(err)
	}
	if _, err := git.Run(ctx, git.RunOpts{Dir: repo.dir}, "commit", "-q", "-m", "Mix alpha and beta"); err != nil {
		t.Fatal(err)
	}
	mixed, err := git.RevParse(ctx, repo.dir, "HEAD")
	if err != nil {
		t.Fatal(err)
	}
	seqs := map[string]int64{}
	for _, path := range []string{"a.txt", "b.txt"} {
		seqs[path] = appendPublishedIntentRepairCapture(t, repo, repo.head, mixed, "create", path, "")
	}
	units, err := git.ReadIntentHistoryUnits(ctx, repo.dir, []string{mixed})
	if err != nil {
		t.Fatal(err)
	}
	baseTree, err := git.RevParse(ctx, repo.dir, repo.head+"^{tree}")
	if err != nil {
		t.Fatal(err)
	}
	trees, err := git.MaterializeIntentHistoryUnits(ctx, repo.dir, baseTree, units, [][]git.IntentHistoryUnit{{units[0]}, {units[1]}})
	if err != nil {
		t.Fatal(err)
	}
	cctx := CaptureContext{BranchRef: "refs/heads/main", BranchGeneration: 1, BaseHead: mixed}
	if _, err := BootstrapShadow(ctx, repo.dir, repo.db, cctx); err != nil {
		t.Fatal(err)
	}
	plan := IntentRepairPlan{ID: "split-mixed", BranchRef: cctx.BranchRef, BranchGeneration: 1, ExpectedHead: mixed, OldChain: []string{mixed}, Paths: []string{"a.txt", "b.txt"}, AllowCaptureRepartition: true, ExpectedFinalTree: trees[1]}
	for i, unit := range units {
		id := "candidate-" + unit.Path
		if err := state.SaveIntentCandidate(ctx, repo.db, state.IntentCandidate{ID: id, BranchRef: cctx.BranchRef, BranchGeneration: 1, Status: state.IntentCandidateSoftPublished, Purpose: "complete " + unit.Path, Readiness: state.IntentReadinessReady, PublishedCommitOID: sql.NullString{String: mixed, Valid: true}, Events: []state.IntentCandidateEvent{{EventSeq: seqs[unit.Path], EventRole: "code"}}}); err != nil {
			t.Fatal(err)
		}
		plan.Candidates = append(plan.Candidates, IntentRepairCandidatePlan{CandidateID: id, Replaces: []string{mixed}, EventSeqs: []int64{seqs[unit.Path]}, TreeOID: trees[i], Message: "Complete " + unit.Path})
	}
	crash := errors.New("process died after split Git CAS")
	intentRepairAfterGitApply = func(IntentRepairResult) error { return crash }
	t.Cleanup(func() { intentRepairAfterGitApply = nil })
	applied, err := ApplyIntentRepairTransaction(ctx, repo.dir, repo.gitDir, repo.db, cctx, plan)
	if !errors.Is(err, crash) {
		t.Fatalf("apply=%+v err=%v", applied, err)
	}
	if len(applied.CommitLineage[mixed]) != 2 || applied.CommitMap[mixed] != "" {
		t.Fatalf("ambiguous old commit acquired one representative: %+v", applied)
	}
	if applied.CandidateMap[plan.Candidates[0].CandidateID] == applied.CandidateMap[plan.Candidates[1].CandidateID] {
		t.Fatalf("goals were not split: %+v", applied)
	}
	intentRepairAfterGitApply = nil
	recovered, err := RecoverIntentRepairs(ctx, repo.dir, repo.gitDir, repo.db, cctx)
	if err != nil || len(recovered) != 1 || recovered[0].Status != state.IntentRepairCompleted {
		t.Fatalf("recovery=%+v err=%v", recovered, err)
	}
	for _, candidate := range plan.Candidates {
		event, err := state.CaptureEventBySeq(ctx, repo.db, candidate.EventSeqs[0])
		if err != nil {
			t.Fatal(err)
		}
		if event.CommitOID.String != applied.CandidateMap[candidate.CandidateID] {
			t.Fatalf("capture lineage=%+v", event)
		}
	}
	stored, ok, err := state.IntentRepairByID(ctx, repo.db, plan.ID)
	if err != nil || !ok || len(stored.Commits) != 2 || stored.Commits[0].OldOID != mixed || stored.Commits[1].OldOID != mixed {
		t.Fatalf("durable split lineage=%+v err=%v", stored, err)
	}
}

func TestIntentRepairQualityOnlyRejectsFinalTreeChange(t *testing.T) {
	f := newIntentRepairFixture(t, 1)
	// A history quality operation cannot silently undo source content, even
	// when the supplied tree is a valid Git object and the plan is otherwise safe.
	tree, err := git.RevParse(context.Background(), f.repo.dir, f.repo.head+"^{tree}")
	if err != nil {
		t.Fatal(err)
	}
	f.plan.Candidates[0].TreeOID = tree
	if _, err := ApplyIntentRepairTransaction(context.Background(), f.repo.dir, f.repo.gitDir, f.repo.db, f.cctx, f.plan); err == nil {
		t.Fatal("quality-only repair changed the final tree")
	}
	head, _ := git.RevParse(context.Background(), f.repo.dir, "HEAD")
	if head != f.plan.ExpectedHead {
		t.Fatal("quality-only rejection moved HEAD")
	}
}
