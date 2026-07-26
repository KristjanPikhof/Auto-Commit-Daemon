package git

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestCheckIntentRepairEligibilityAllowsUnrelatedDirtyState(t *testing.T) {
	repo := initRepo(t)
	ctx := context.Background()
	old1 := commitWorktreePath(t, ctx, repo, "one.txt", "one\n", "one")
	old2 := commitWorktreePath(t, ctx, repo, "two.txt", "two\n", "two")
	if err := os.WriteFile(filepath.Join(repo, "unrelated.txt"), []byte("dirty\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	result, err := CheckIntentRepairEligibility(ctx, repo, intentRepairEligibility(
		old2,
		[]string{old1, old2},
		[]string{"one.txt", "two.txt"},
	))
	if err != nil {
		t.Fatalf("CheckIntentRepairEligibility: %v", err)
	}
	if !result.Eligible || result.Reason != "" {
		t.Fatalf("eligibility=%+v", result)
	}
}

func TestCheckIntentRepairEligibilityRejectsUnsafeVisibility(t *testing.T) {
	tests := []struct {
		name      string
		createRef func(t *testing.T, ctx context.Context, repo, oid string)
		wantRef   string
	}{
		{
			name: "other local branch",
			createRef: func(t *testing.T, ctx context.Context, repo, oid string) {
				t.Helper()
				mustUpdateRef(t, ctx, repo, "refs/heads/shared", oid)
			},
			wantRef: "refs/heads/shared",
		},
		{
			name: "remote tracking ref",
			createRef: func(t *testing.T, ctx context.Context, repo, oid string) {
				t.Helper()
				mustUpdateRef(t, ctx, repo, "refs/remotes/origin/main", oid)
			},
			wantRef: "refs/remotes/origin/main",
		},
		{
			name: "tag",
			createRef: func(t *testing.T, ctx context.Context, repo, oid string) {
				t.Helper()
				mustUpdateRef(t, ctx, repo, "refs/tags/published", oid)
			},
			wantRef: "refs/tags/published",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			repo := initRepo(t)
			ctx := context.Background()
			head := commitWorktreePath(t, ctx, repo, "one.txt", "one\n", "one")
			tc.createRef(t, ctx, repo, head)

			result, err := CheckIntentRepairEligibility(ctx, repo, intentRepairEligibility(
				head,
				[]string{head},
				[]string{"one.txt"},
			))
			if err != nil {
				t.Fatalf("CheckIntentRepairEligibility: %v", err)
			}
			if result.Eligible || result.Reason != IntentRepairReasonAlternateRef {
				t.Fatalf("eligibility=%+v", result)
			}
			if strings.Join(result.ContainingRefs, ",") != tc.wantRef {
				t.Fatalf("containing refs=%v want %s", result.ContainingRefs, tc.wantRef)
			}
		})
	}
}

func TestCheckIntentRepairEligibilityRejectsStagedOverlap(t *testing.T) {
	repo := initRepo(t)
	ctx := context.Background()
	head := commitWorktreePath(t, ctx, repo, "one.txt", "one\n", "one")
	if err := os.WriteFile(filepath.Join(repo, "one.txt"), []byte("staged\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := Run(ctx, RunOpts{Dir: repo}, "add", "one.txt"); err != nil {
		t.Fatalf("git add: %v", err)
	}

	result, err := CheckIntentRepairEligibility(ctx, repo, intentRepairEligibility(
		head,
		[]string{head},
		[]string{"one.txt"},
	))
	if err != nil {
		t.Fatalf("CheckIntentRepairEligibility: %v", err)
	}
	if result.Eligible || result.Reason != IntentRepairReasonStagedOverlap ||
		strings.Join(result.StagedPaths, ",") != "one.txt" {
		t.Fatalf("eligibility=%+v", result)
	}
}

func TestCheckIntentRepairEligibilityRejectsBranchAndChainChanges(t *testing.T) {
	t.Run("branch changed", func(t *testing.T) {
		repo := initRepo(t)
		ctx := context.Background()
		head := commitWorktreePath(t, ctx, repo, "one.txt", "one\n", "one")
		opts := intentRepairEligibility(head, []string{head}, []string{"one.txt"})
		opts.BranchRef = "refs/heads/expected"
		result, err := CheckIntentRepairEligibility(ctx, repo, opts)
		if err != nil {
			t.Fatalf("CheckIntentRepairEligibility: %v", err)
		}
		if result.Eligible || result.Reason != IntentRepairReasonBranchChanged {
			t.Fatalf("eligibility=%+v", result)
		}
	})

	t.Run("head changed", func(t *testing.T) {
		repo := initRepo(t)
		ctx := context.Background()
		old := commitWorktreePath(t, ctx, repo, "one.txt", "one\n", "one")
		_ = commitWorktreePath(t, ctx, repo, "two.txt", "two\n", "two")
		result, err := CheckIntentRepairEligibility(ctx, repo, intentRepairEligibility(
			old,
			[]string{old},
			[]string{"one.txt"},
		))
		if err != nil {
			t.Fatalf("CheckIntentRepairEligibility: %v", err)
		}
		if result.Eligible || result.Reason != IntentRepairReasonHeadChanged {
			t.Fatalf("eligibility=%+v", result)
		}
	})

	t.Run("non linear chain", func(t *testing.T) {
		repo := initRepo(t)
		ctx := context.Background()
		old1 := commitWorktreePath(t, ctx, repo, "one.txt", "one\n", "one")
		_ = commitWorktreePath(t, ctx, repo, "two.txt", "two\n", "two")
		old3 := commitWorktreePath(t, ctx, repo, "three.txt", "three\n", "three")
		result, err := CheckIntentRepairEligibility(ctx, repo, intentRepairEligibility(
			old3,
			[]string{old1, old3},
			[]string{"one.txt", "three.txt"},
		))
		if err != nil {
			t.Fatalf("CheckIntentRepairEligibility: %v", err)
		}
		if result.Eligible || result.Reason != IntentRepairReasonNonLinearChain {
			t.Fatalf("eligibility=%+v", result)
		}
	})

	t.Run("detached head", func(t *testing.T) {
		repo := initRepo(t)
		ctx := context.Background()
		head := commitWorktreePath(t, ctx, repo, "one.txt", "one\n", "one")
		if _, err := Run(ctx, RunOpts{Dir: repo}, "switch", "--detach", "-q", head); err != nil {
			t.Fatalf("detach: %v", err)
		}
		result, err := CheckIntentRepairEligibility(ctx, repo, intentRepairEligibility(
			head,
			[]string{head},
			[]string{"one.txt"},
		))
		if err != nil {
			t.Fatalf("CheckIntentRepairEligibility: %v", err)
		}
		if result.Eligible || result.Reason != IntentRepairReasonDetached {
			t.Fatalf("eligibility=%+v", result)
		}
	})
}

func TestCheckIntentRepairEligibilityRejectsMergeAndMissingOwnership(t *testing.T) {
	t.Run("merge", func(t *testing.T) {
		repo := initRepo(t)
		ctx := context.Background()
		base := commitWorktreePath(t, ctx, repo, "base.txt", "base\n", "base")
		mustUpdateRef(t, ctx, repo, "refs/heads/side", base)
		if _, err := Run(ctx, RunOpts{Dir: repo}, "switch", "-q", "side"); err != nil {
			t.Fatalf("switch side: %v", err)
		}
		_ = commitWorktreePath(t, ctx, repo, "side.txt", "side\n", "side")
		if _, err := Run(ctx, RunOpts{Dir: repo}, "switch", "-q", "main"); err != nil {
			t.Fatalf("switch main: %v", err)
		}
		_ = commitWorktreePath(t, ctx, repo, "main.txt", "main\n", "main")
		if _, err := Run(ctx, RunOpts{Dir: repo}, "merge", "--no-ff", "-q", "-m", "merge", "side"); err != nil {
			t.Fatalf("merge: %v", err)
		}
		head, err := RevParse(ctx, repo, "HEAD")
		if err != nil {
			t.Fatal(err)
		}
		result, err := CheckIntentRepairEligibility(ctx, repo, intentRepairEligibility(
			head,
			[]string{head},
			[]string{"side.txt", "main.txt"},
		))
		if err != nil {
			t.Fatalf("CheckIntentRepairEligibility: %v", err)
		}
		if result.Eligible || result.Reason != IntentRepairReasonMergeCommit {
			t.Fatalf("eligibility=%+v", result)
		}
	})

	t.Run("missing ownership", func(t *testing.T) {
		repo := initRepo(t)
		ctx := context.Background()
		head := commitWorktreePath(t, ctx, repo, "one.txt", "one\n", "one")
		opts := intentRepairEligibility(head, []string{head}, []string{"one.txt"})
		opts.Commits[0].CandidateID = ""
		result, err := CheckIntentRepairEligibility(ctx, repo, opts)
		if err != nil {
			t.Fatalf("CheckIntentRepairEligibility: %v", err)
		}
		if result.Eligible || result.Reason != IntentRepairReasonOwnershipMissing {
			t.Fatalf("eligibility=%+v", result)
		}
	})
}

func TestApplyIntentRepairAtomicallyBacksUpAndPreservesLiveState(t *testing.T) {
	repo := initRepo(t)
	ctx := context.Background()
	old1 := commitWorktreePath(t, ctx, repo, "one.txt", "one\n", "one")
	old2 := commitWorktreePath(t, ctx, repo, "two.txt", "two\n", "two")
	if err := os.WriteFile(filepath.Join(repo, "unrelated.txt"), []byte("staged\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := Run(ctx, RunOpts{Dir: repo}, "add", "unrelated.txt"); err != nil {
		t.Fatalf("git add unrelated: %v", err)
	}
	tree, err := commitTreeOID(ctx, repo, old2)
	if err != nil {
		t.Fatal(err)
	}
	beforeIndex := mustReadFile(t, filepath.Join(repo, ".git", "index"))
	beforeOne := mustReadFile(t, filepath.Join(repo, "one.txt"))
	beforeTwo := mustReadFile(t, filepath.Join(repo, "two.txt"))
	beforeUnrelated := mustReadFile(t, filepath.Join(repo, "unrelated.txt"))

	result, err := ApplyIntentRepair(ctx, repo, IntentRepairApplyOptions{
		Eligibility: intentRepairEligibility(
			old2,
			[]string{old1, old2},
			[]string{"one.txt", "two.txt"},
		),
		RepairID: "repair-1",
		Replacements: []IntentRepairReplacement{{
			Replaces: []string{old1, old2},
			TreeOID:  tree,
			Message:  "Combine one and two",
		}},
	})
	if err != nil {
		t.Fatalf("ApplyIntentRepair: %v", err)
	}
	if !result.Eligible || result.NewHead == "" || result.NewHead == old2 {
		t.Fatalf("result=%+v", result)
	}
	if len(result.CommitMappings) != 2 ||
		result.CommitMappings[0].NewOID != result.NewHead ||
		result.CommitMappings[1].NewOID != result.NewHead {
		t.Fatalf("mappings=%+v", result.CommitMappings)
	}
	head, err := RevParse(ctx, repo, "HEAD")
	if err != nil || head != result.NewHead {
		t.Fatalf("HEAD=%s err=%v result=%+v", head, err, result)
	}
	backup, err := RevParse(ctx, repo, result.BackupRef)
	if err != nil || backup != old2 {
		t.Fatalf("backup=%s err=%v want %s", backup, err, old2)
	}
	for path, before := range map[string][]byte{
		filepath.Join(repo, ".git", "index"): beforeIndex,
		filepath.Join(repo, "one.txt"):       beforeOne,
		filepath.Join(repo, "two.txt"):       beforeTwo,
		filepath.Join(repo, "unrelated.txt"): beforeUnrelated,
	} {
		if after := mustReadFile(t, path); string(after) != string(before) {
			t.Fatalf("%s changed during ref-only repair", path)
		}
	}
}

func TestApplyIntentRepairDryRunIsInert(t *testing.T) {
	repo := initRepo(t)
	ctx := context.Background()
	head := commitWorktreePath(t, ctx, repo, "one.txt", "one\n", "one")
	tree, err := commitTreeOID(ctx, repo, head)
	if err != nil {
		t.Fatal(err)
	}

	result, err := ApplyIntentRepair(ctx, repo, IntentRepairApplyOptions{
		Eligibility: intentRepairEligibility(head, []string{head}, []string{"one.txt"}),
		RepairID:    "dry-run",
		Replacements: []IntentRepairReplacement{{
			Replaces: []string{head},
			TreeOID:  tree,
			Message:  "Rewrite one",
		}},
		DryRun: true,
	})
	if err != nil {
		t.Fatalf("ApplyIntentRepair dry-run: %v", err)
	}
	if !result.Eligible || !result.DryRun || result.NewHead != "" || len(result.CommitMappings) != 0 {
		t.Fatalf("result=%+v", result)
	}
	gotHead, err := RevParse(ctx, repo, "HEAD")
	if err != nil || gotHead != head {
		t.Fatalf("HEAD=%s err=%v want %s", gotHead, err, head)
	}
	exists, err := RefExists(ctx, repo, result.BackupRef)
	if err != nil || exists {
		t.Fatalf("backup exists=%v err=%v", exists, err)
	}
}

func TestApplyIntentRepairBackupCollisionLeavesBranchUnchanged(t *testing.T) {
	repo := initRepo(t)
	ctx := context.Background()
	head := commitWorktreePath(t, ctx, repo, "one.txt", "one\n", "one")
	tree, err := commitTreeOID(ctx, repo, head)
	if err != nil {
		t.Fatal(err)
	}
	backupRef, err := IntentRepairBackupRef("refs/heads/main", "collision")
	if err != nil {
		t.Fatal(err)
	}
	mustUpdateRef(t, ctx, repo, backupRef, head)

	_, err = ApplyIntentRepair(ctx, repo, IntentRepairApplyOptions{
		Eligibility: intentRepairEligibility(head, []string{head}, []string{"one.txt"}),
		RepairID:    "collision",
		Replacements: []IntentRepairReplacement{{
			Replaces: []string{head},
			TreeOID:  tree,
			Message:  "Rewrite one",
		}},
	})
	if err == nil || !strings.Contains(err.Error(), "atomic backup and branch CAS") {
		t.Fatalf("error=%v", err)
	}
	gotHead, resolveErr := RevParse(ctx, repo, "HEAD")
	if resolveErr != nil || gotHead != head {
		t.Fatalf("HEAD=%s err=%v want %s", gotHead, resolveErr, head)
	}
}

func intentRepairEligibility(head string, commits, paths []string) IntentRepairEligibilityOptions {
	owned := make([]IntentRepairOwnedCommit, 0, len(commits))
	for _, oid := range commits {
		owned = append(owned, IntentRepairOwnedCommit{OID: oid, CandidateID: "candidate-1"})
	}
	return IntentRepairEligibilityOptions{
		BranchRef:    "refs/heads/main",
		ExpectedHead: head,
		Commits:      owned,
		Paths:        paths,
		MaxCommits:   MaxIntentRepairCommits,
	}
}

func mustUpdateRef(t *testing.T, ctx context.Context, repo, ref, oid string) {
	t.Helper()
	if err := UpdateRef(ctx, repo, ref, oid, ""); err != nil {
		t.Fatalf("UpdateRef %s: %v", ref, err)
	}
}

func mustReadFile(t *testing.T, path string) []byte {
	t.Helper()
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return body
}
