package git

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func intentHistoryMixedFixture(t *testing.T) (string, IntentHistoryReconstructionOptions, []IntentHistoryUnit, string) {
	t.Helper()
	ctx := context.Background()
	repo := initRepo(t)
	base := commitWorktreePath(t, ctx, repo, "base.txt", "base\n", "Base")
	for path, body := range map[string]string{"a.txt": "a1\n", "b.txt": "b1\n"} {
		if err := os.WriteFile(filepath.Join(repo, path), []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := Run(ctx, RunOpts{Dir: repo}, "add", "a.txt", "b.txt"); err != nil {
		t.Fatal(err)
	}
	if _, err := Run(ctx, RunOpts{Dir: repo}, "commit", "-q", "-m", "Mix unrelated goals"); err != nil {
		t.Fatal(err)
	}
	mixed, err := RevParse(ctx, repo, "HEAD")
	if err != nil {
		t.Fatal(err)
	}
	correction := commitWorktreePath(t, ctx, repo, "a.txt", "a2\n", "Correct alpha")
	units, err := ReadIntentHistoryUnits(ctx, repo, []string{mixed, correction})
	if err != nil {
		t.Fatal(err)
	}
	var alpha, beta []IntentHistoryUnit
	for _, unit := range units {
		if unit.Path == "a.txt" {
			alpha = append(alpha, unit)
		} else {
			beta = append(beta, unit)
		}
	}
	baseTree, err := RevParse(ctx, repo, base+"^{tree}")
	if err != nil {
		t.Fatal(err)
	}
	trees, err := MaterializeIntentHistoryUnits(ctx, repo, baseTree, units, [][]IntentHistoryUnit{alpha, beta})
	if err != nil {
		t.Fatal(err)
	}
	return repo, IntentHistoryReconstructionOptions{
		SourceBranchRef: "refs/heads/main", TargetBranchRef: "refs/heads/semantic-history",
		ExpectedHead: correction, OldChain: []string{mixed, correction}, PlanID: "mixed-capture-goals",
		Replacements: []IntentRepairReplacement{
			{Replaces: []string{mixed, correction}, TreeOID: trees[0], Message: "Complete alpha\n\n- Include its available correction"},
			{Replaces: []string{mixed}, TreeOID: trees[1], Message: "Add beta\n\n- Keep the independent outcome separately revertible"},
		},
	}, units, baseTree
}

func TestIntentHistoryReconstructionSplitsMixedCommitAndPreservesLiveState(t *testing.T) {
	repo, opts, _, _ := intentHistoryMixedFixture(t)
	ctx := context.Background()
	if err := os.WriteFile(filepath.Join(repo, "b.txt"), []byte("user's later edit\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := Run(ctx, RunOpts{Dir: repo}, "add", "b.txt"); err != nil {
		t.Fatal(err)
	}
	index := mustReadFile(t, filepath.Join(repo, ".git", "index"))
	var verified []string
	opts.VerifyCommit = func(_ context.Context, oid string, _ int) error { verified = append(verified, oid); return nil }
	result, err := ApplyIntentHistoryReconstruction(ctx, repo, opts)
	if err != nil {
		t.Fatal(err)
	}
	if !result.Eligible || len(verified) != 2 || len(result.CommitMappings) != 3 {
		t.Fatalf("result=%+v verified=%v", result, verified)
	}
	if result.CommitMappings[0].OldOID != result.CommitMappings[2].OldOID || result.CommitMappings[0].NewOID == result.CommitMappings[2].NewOID {
		t.Fatalf("lost one-to-many lineage: %+v", result.CommitMappings)
	}
	if head, _ := RevParse(ctx, repo, "HEAD"); head != opts.ExpectedHead {
		t.Fatalf("source HEAD changed: %s", head)
	}
	if string(mustReadFile(t, filepath.Join(repo, ".git", "index"))) != string(index) {
		t.Fatal("user index changed")
	}
	if string(mustReadFile(t, filepath.Join(repo, "b.txt"))) != "user's later edit\n" {
		t.Fatal("user worktree changed")
	}
	if tree, _ := RevParse(ctx, repo, result.NewHead+"^{tree}"); tree != opts.Replacements[1].TreeOID {
		t.Fatalf("final tree=%s", tree)
	}
	// A worker may die after the atomic refs and before acknowledging its
	// request. Retrying proves the existing history, rather than rewriting it.
	recovered, err := ApplyIntentHistoryReconstruction(ctx, repo, opts)
	if err != nil || recovered.NewHead != result.NewHead {
		t.Fatalf("recovered=%+v err=%v", recovered, err)
	}
}

func TestIntentHistoryUnitsRejectFabricationAndMissingPrerequisites(t *testing.T) {
	repo, _, units, base := intentHistoryMixedFixture(t)
	var alpha, beta []IntentHistoryUnit
	for _, unit := range units {
		if unit.Path == "a.txt" {
			alpha = append(alpha, unit)
		} else {
			beta = append(beta, unit)
		}
	}
	for _, tc := range []struct {
		name   string
		groups [][]IntentHistoryUnit
		want   string
	}{
		{"reverse dependent versions", [][]IntentHistoryUnit{{alpha[1], alpha[0]}, beta}, "prerequisite version"},
		{"missing unit", [][]IntentHistoryUnit{alpha}, "unowned"},
		{"duplicate unit", [][]IntentHistoryUnit{alpha, beta, beta}, "multiply owned"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := MaterializeIntentHistoryUnits(context.Background(), repo, base, units, tc.groups)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err=%v want=%q", err, tc.want)
			}
		})
	}
	fake := alpha[0]
	fake.After.OID = alpha[1].After.OID
	if _, err := MaterializeIntentHistoryUnits(context.Background(), repo, base, units, [][]IntentHistoryUnit{{fake, alpha[1]}, beta}); err == nil || !strings.Contains(err.Error(), "fabricated") {
		t.Fatalf("fabrication err=%v", err)
	}
}

func TestIntentHistoryReconstructionRejectsFinalTreeLossAndSourceMovement(t *testing.T) {
	for _, name := range []string{"final tree loss", "source movement", "verification failure"} {
		t.Run(name, func(t *testing.T) {
			repo, opts, _, _ := intentHistoryMixedFixture(t)
			sentinel := errors.New("configured verification failed")
			switch name {
			case "final tree loss":
				opts.Replacements[1].TreeOID = opts.Replacements[0].TreeOID
			case "source movement":
				opts.VerifyCommit = func(_ context.Context, _ string, i int) error {
					if i == 0 {
						mustUpdateRef(t, context.Background(), repo, opts.SourceBranchRef, opts.OldChain[0])
					}
					return nil
				}
			case "verification failure":
				opts.VerifyCommit = func(context.Context, string, int) error { return sentinel }
			}
			_, err := ApplyIntentHistoryReconstruction(context.Background(), repo, opts)
			if err == nil {
				t.Fatal("unsafe reconstruction succeeded")
			}
			if _, err := RevParse(context.Background(), repo, opts.TargetBranchRef); !errors.Is(err, ErrRefNotFound) {
				t.Fatalf("target created: %v", err)
			}
			backup, _ := IntentRepairBackupRef(opts.SourceBranchRef, opts.PlanID)
			if _, err := RevParse(context.Background(), repo, backup); !errors.Is(err, ErrRefNotFound) {
				t.Fatalf("backup created before failed validation: %v", err)
			}
		})
	}
}

func TestIntentHistoryReconstructionRecoveryRejectsTargetDrift(t *testing.T) {
	repo, opts, _, _ := intentHistoryMixedFixture(t)
	result, err := ApplyIntentHistoryReconstruction(context.Background(), repo, opts)
	if err != nil {
		t.Fatal(err)
	}
	mustUpdateRef(t, context.Background(), repo, opts.TargetBranchRef, opts.OldChain[0])
	if _, err := RecoverIntentHistoryReconstruction(context.Background(), repo, opts); err == nil {
		t.Fatalf("accepted moved target originally at %s", result.NewHead)
	}
}

func TestIntentHistoryReconstructionRecoveryRunsCurrentVerification(t *testing.T) {
	repo, opts, _, _ := intentHistoryMixedFixture(t)
	ctx := context.Background()
	result, err := ApplyIntentHistoryReconstruction(ctx, repo, opts)
	if err != nil {
		t.Fatal(err)
	}
	failure := errors.New("new approved project check rejected the reconstructed goal")
	calls := 0
	opts.VerifyCommit = func(context.Context, string, int) error { calls++; return failure }
	if _, err := ApplyIntentHistoryReconstruction(ctx, repo, opts); !errors.Is(err, failure) || calls != 1 {
		t.Fatalf("resumed verification calls=%d err=%v", calls, err)
	}
	if source, _ := RevParse(ctx, repo, opts.SourceBranchRef); source != opts.ExpectedHead {
		t.Fatal("failed resumed check moved source")
	}
	if target, _ := RevParse(ctx, repo, opts.TargetBranchRef); target != result.NewHead {
		t.Fatal("failed resumed check removed the protected target")
	}
	if backup, _ := RevParse(ctx, repo, result.BackupRef); backup != opts.ExpectedHead {
		t.Fatal("failed resumed check removed original history backup")
	}
}

func TestIntentHistoryReconstructionRejectsCrossAuthorGoal(t *testing.T) {
	repo, opts, _, _ := intentHistoryMixedFixture(t)
	ctx := context.Background()
	other, err := CommitTreeWithIdentity(ctx, repo, opts.Replacements[1].TreeOID, "Another author's correction", "Other Author", "other@example.com", opts.OldChain[0])
	if err != nil {
		t.Fatal(err)
	}
	if err := UpdateRef(ctx, repo, opts.SourceBranchRef, other, opts.ExpectedHead); err != nil {
		t.Fatal(err)
	}
	opts.ExpectedHead = other
	opts.OldChain[1] = other
	opts.Replacements[0].Replaces[1] = other
	if _, err := ApplyIntentHistoryReconstruction(ctx, repo, opts); err == nil || !strings.Contains(err.Error(), "author boundary") {
		t.Fatalf("cross-author goal err=%v", err)
	}
	if _, err := RevParse(ctx, repo, opts.TargetBranchRef); !errors.Is(err, ErrRefNotFound) {
		t.Fatal("cross-author goal created a branch")
	}
}

func TestIntentHistoryUnitsKeepEditedRenameTogether(t *testing.T) {
	repo := initRepo(t)
	ctx := context.Background()
	before := commitWorktreePath(t, ctx, repo, "before.txt", "first stable line\nsecond stable line\nthird stable line\nfourth old line\n", "Add source")
	if err := os.Rename(filepath.Join(repo, "before.txt"), filepath.Join(repo, "after.txt")); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(repo, "after.txt"), []byte("first stable line\nsecond stable line\nthird stable line\nfourth corrected line\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := Run(ctx, RunOpts{Dir: repo}, "add", "-A"); err != nil {
		t.Fatal(err)
	}
	if _, err := Run(ctx, RunOpts{Dir: repo}, "commit", "-q", "-m", "Move and correct source"); err != nil {
		t.Fatal(err)
	}
	head, err := RevParse(ctx, repo, "HEAD")
	if err != nil {
		t.Fatal(err)
	}
	units, err := ReadIntentHistoryUnits(ctx, repo, []string{head})
	if err != nil {
		t.Fatal(err)
	}
	pairs, err := ReadIntentHistoryRenamePairs(ctx, repo, []string{head})
	if err != nil || len(pairs) != 1 || pairs[0].BeforePath != "before.txt" || pairs[0].AfterPath != "after.txt" {
		t.Fatalf("rename evidence=%+v err=%v", pairs, err)
	}
	base, err := IntentHistoryBaseTree(ctx, repo, head)
	if err != nil {
		t.Fatal(err)
	}
	if want, _ := RevParse(ctx, repo, before+"^{tree}"); base != want {
		t.Fatalf("base=%s want=%s", base, want)
	}
	if _, err := MaterializeIntentHistoryUnits(ctx, repo, base, units, [][]IntentHistoryUnit{{units[0]}, {units[1]}}); err == nil || !strings.Contains(err.Error(), "must remain in one goal") {
		t.Fatalf("split edited rename err=%v", err)
	}
	if _, err := MaterializeIntentHistoryUnits(ctx, repo, base, units, [][]IntentHistoryUnit{units}); err != nil {
		t.Fatalf("complete edited rename refused: %v", err)
	}
}

func TestIntentHistoryReconstructionIncludesRootCommit(t *testing.T) {
	repo := initRepo(t)
	ctx := context.Background()
	head := commitWorktreePath(t, ctx, repo, "root.txt", "root content\n", "Initialize behavior")
	base, err := IntentHistoryBaseTree(ctx, repo, head)
	if err != nil {
		t.Fatal(err)
	}
	units, err := ReadIntentHistoryUnits(ctx, repo, []string{head})
	if err != nil {
		t.Fatal(err)
	}
	trees, err := MaterializeIntentHistoryUnits(ctx, repo, base, units, [][]IntentHistoryUnit{units})
	if err != nil {
		t.Fatal(err)
	}
	options := IntentHistoryReconstructionOptions{SourceBranchRef: "refs/heads/main", TargetBranchRef: "refs/heads/reconstructed-root", ExpectedHead: head, OldChain: []string{head}, PlanID: "root-plan", Replacements: []IntentRepairReplacement{{Replaces: []string{head}, TreeOID: trees[0], Message: "Initialize complete behavior\n\n- Preserve the original initial source"}}}
	result, err := ApplyIntentHistoryReconstruction(ctx, repo, options)
	if err != nil {
		t.Fatal(err)
	}
	parents, err := parentsOf(ctx, repo, result.NewHead)
	if err != nil || len(parents) != 0 {
		t.Fatalf("root parents=%v err=%v", parents, err)
	}
	if recovered, err := ApplyIntentHistoryReconstruction(ctx, repo, options); err != nil || recovered.NewHead != result.NewHead {
		t.Fatalf("root recovery=%+v err=%v", recovered, err)
	}
}
