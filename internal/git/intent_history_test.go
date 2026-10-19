package git

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestIntentHistoryReadersPreserveEditedRenameEvidence(t *testing.T) {
	t.Parallel()
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
	if len(units) != 2 || units[0].Path != "after.txt" || units[1].Path != "before.txt" || units[0].Before != (TreeEntry{}) || units[1].After != (TreeEntry{}) {
		t.Fatalf("rename versions=%+v", units)
	}
}

func TestIntentHistoryReadersIncludeRootCommit(t *testing.T) {
	t.Parallel()
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
	if entries, err := LsTree(ctx, repo, base, true); err != nil || len(entries) != 0 {
		t.Fatalf("root base entries=%+v err=%v", entries, err)
	}
	if len(units) != 1 || units[0].OldOID != head || units[0].Path != "root.txt" || units[0].Before != (TreeEntry{}) || units[0].After.OID == "" {
		t.Fatalf("root versions=%+v", units)
	}
}
