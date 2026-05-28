package git

import (
	"context"
	"strconv"
	"strings"
	"testing"
)

func TestResolveRewriteSelectionPositions(t *testing.T) {
	dir := initRepoWithMain(t)
	ctx := context.Background()
	commits := []string{
		commitWorktreeFile(t, ctx, dir, "1.txt", "1\n", "one"),
		commitWorktreeFile(t, ctx, dir, "2.txt", "2\n", "two"),
		commitWorktreeFile(t, ctx, dir, "3.txt", "3\n", "three"),
		commitWorktreeFile(t, ctx, dir, "4.txt", "4\n", "four"),
	}

	for _, tc := range []struct {
		name      string
		opts      RewriteSelectionOptions
		selected  []string
		recreated []string
		positions [2]int
	}{
		{name: "last", opts: RewriteSelectionOptions{Last: 2}, selected: []string{commits[2], commits[3]}, positions: [2]int{1, 2}},
		{name: "from_position", opts: RewriteSelectionOptions{From: "3"}, selected: []string{commits[1], commits[2], commits[3]}, positions: [2]int{1, 3}},
		{name: "range", opts: RewriteSelectionOptions{Range: "2-3"}, selected: []string{commits[1], commits[2]}, recreated: []string{commits[3]}, positions: [2]int{2, 3}},
		{name: "from_sha", opts: RewriteSelectionOptions{From: commits[1][:12]}, selected: []string{commits[1], commits[2], commits[3]}, positions: [2]int{1, 3}},
		{name: "git_range", opts: RewriteSelectionOptions{GitRange: commits[0] + ".." + commits[2]}, selected: []string{commits[1], commits[2]}, recreated: []string{commits[3]}, positions: [2]int{2, 3}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ResolveRewriteSelection(ctx, dir, tc.opts)
			if err != nil {
				t.Fatalf("ResolveRewriteSelection: %v", err)
			}
			assertRecordOIDs(t, got.Selected, tc.selected)
			assertRecordOIDs(t, got.RecreateUnchanged, tc.recreated)
			if got.SelectedNewestIndex != tc.positions[0] || got.SelectedOldestIndex != tc.positions[1] {
				t.Fatalf("positions=%d-%d want %d-%d", got.SelectedNewestIndex, got.SelectedOldestIndex, tc.positions[0], tc.positions[1])
			}
		})
	}
}

func TestResolveFromPositionOrCommit_AllDigitShortSHA(t *testing.T) {
	dir := initRepoWithMain(t)
	ctx := context.Background()

	for i := 0; i < 128; i++ {
		oid := commitWorktreeFile(t, ctx, dir, "numeric.txt", strconv.Itoa(i)+"\n", "numeric")
		short := oid[:4]
		n, err := strconv.Atoi(short)
		if err != nil {
			continue
		}
		chain, err := firstParentChain(ctx, dir)
		if err != nil {
			t.Fatalf("firstParentChain: %v", err)
		}
		if n <= len(chain) {
			continue
		}
		if resolved, err := RevParse(ctx, dir, short+"^{commit}"); err != nil || resolved != oid {
			continue
		}
		got, err := resolveFromPositionOrCommit(ctx, dir, short, chain)
		if err != nil {
			t.Fatalf("resolveFromPositionOrCommit(%q): %v", short, err)
		}
		for want, candidate := range chain {
			if candidate == oid {
				if got != want {
					t.Fatalf("position=%d want %d for %s", got, want, short)
				}
				return
			}
		}
		t.Fatalf("commit %s missing from chain", oid)
	}
	t.Fatal("did not generate an unambiguous all-digit short SHA")
}

func TestResolveRewriteSelectionRejectsUnsupportedShapes(t *testing.T) {
	t.Run("dirty worktree", func(t *testing.T) {
		dir := initRepoWithMain(t)
		ctx := context.Background()
		commitWorktreeFile(t, ctx, dir, "a.txt", "a\n", "a")
		writeFile(t, dir+"/dirty.txt", "dirty\n")
		_, err := ResolveRewriteSelection(ctx, dir, RewriteSelectionOptions{Last: 1})
		if err == nil || !strings.Contains(err.Error(), "dirty index or worktree") {
			t.Fatalf("expected dirty refusal, got %v", err)
		}
	})

	t.Run("merge in recreated chain", func(t *testing.T) {
		dir := initRepoWithMain(t)
		ctx := context.Background()
		commitWorktreeFile(t, ctx, dir, "base.txt", "base\n", "base")
		if _, err := Run(ctx, RunOpts{Dir: dir, Timeout: DefaultWriteTimeout}, "checkout", "-q", "-b", "side"); err != nil {
			t.Fatalf("checkout side: %v", err)
		}
		commitWorktreeFile(t, ctx, dir, "side.txt", "side\n", "side")
		if _, err := Run(ctx, RunOpts{Dir: dir, Timeout: DefaultWriteTimeout}, "checkout", "-q", "main"); err != nil {
			t.Fatalf("checkout main: %v", err)
		}
		commitWorktreeFile(t, ctx, dir, "main.txt", "main\n", "main")
		if _, err := Run(ctx, RunOpts{Dir: dir, Timeout: DefaultWriteTimeout}, "merge", "--no-ff", "-m", "merge side", "side"); err != nil {
			t.Fatalf("merge side: %v", err)
		}
		commitWorktreeFile(t, ctx, dir, "after.txt", "after\n", "after")
		_, err := ResolveRewriteSelection(ctx, dir, RewriteSelectionOptions{Range: "2-2"})
		if err == nil || !strings.Contains(err.Error(), "merge commit") {
			t.Fatalf("expected merge refusal, got %v", err)
		}
	})

	t.Run("detached head", func(t *testing.T) {
		dir := initRepoWithMain(t)
		ctx := context.Background()
		commit := commitWorktreeFile(t, ctx, dir, "a.txt", "a\n", "a")
		if _, err := Run(ctx, RunOpts{Dir: dir, Timeout: DefaultWriteTimeout}, "checkout", "-q", "--detach", commit); err != nil {
			t.Fatalf("detach: %v", err)
		}
		_, err := ResolveRewriteSelection(ctx, dir, RewriteSelectionOptions{Last: 1})
		if err == nil || !strings.Contains(err.Error(), "detached HEAD") {
			t.Fatalf("expected detached refusal, got %v", err)
		}
	})

	t.Run("non contiguous git range", func(t *testing.T) {
		dir := initRepoWithMain(t)
		ctx := context.Background()
		c1 := commitWorktreeFile(t, ctx, dir, "1.txt", "1\n", "one")
		commitWorktreeFile(t, ctx, dir, "2.txt", "2\n", "two")
		c3 := commitWorktreeFile(t, ctx, dir, "3.txt", "3\n", "three")
		_, err := ResolveRewriteSelection(ctx, dir, RewriteSelectionOptions{GitRange: "--no-walk " + c3 + " " + c1})
		if err == nil || !strings.Contains(err.Error(), "contiguous") {
			t.Fatalf("expected contiguous refusal, got %v", err)
		}
	})
}

func assertRecordOIDs(t *testing.T, got []RewriteCommitRecord, want []string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("records=%d want %d: %#v", len(got), len(want), got)
	}
	for i := range want {
		if got[i].OID != want[i] {
			t.Fatalf("record %d oid=%s want %s", i, got[i].OID, want[i])
		}
	}
}
