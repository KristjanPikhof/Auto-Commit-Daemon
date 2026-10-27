package git

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func TestPrivateRefDeleteRejectsAliasUnderPreparedLock(t *testing.T) {
	repo := initRepo(t)
	ctx := t.Context()
	tree, err := WriteTreeDurable(ctx, repo, filepath.Join(t.TempDir(), "empty.index"), nil)
	if err != nil {
		t.Fatal(err)
	}
	oid, err := CommitTreeDurable(ctx, repo, tree, "original", "Original", "original@example.com")
	if err != nil {
		t.Fatal(err)
	}
	if err := UpdateRef(ctx, repo, "refs/heads/main", oid, ""); err != nil {
		t.Fatal(err)
	}
	refs := []RefExpectation{{Ref: "refs/acd/checkpoints/one", OID: oid}, {Ref: "refs/acd/checkpoints/two", OID: oid}}
	if err := CreateRefsCAS(ctx, repo, refs); err != nil {
		t.Fatal(err)
	}
	hook := filepath.Join(repo, ".git", "hooks", "reference-transaction")
	body := "#!/bin/sh\nif [ \"$1\" = prepared ]; then printf 'ref: refs/heads/main\\n' > \"$(git rev-parse --git-dir)/refs/acd/checkpoints/two\"; fi\n"
	if err := os.WriteFile(hook, []byte(body), 0755); err != nil {
		t.Fatal(err)
	}
	removed, err := DeletePrivateRefsCAS(ctx, repo, refs)
	if err == nil || len(removed) != 0 {
		t.Fatalf("alias removed: %v %v", removed, err)
	}
	if err := os.Remove(hook); err != nil {
		t.Fatal(err)
	}
	actual, err := RevParse(ctx, repo, "refs/heads/main")
	if err != nil || actual != oid {
		t.Fatal("public branch changed")
	}
	actual, err = RevParse(ctx, repo, refs[0].Ref)
	if err != nil || actual != oid {
		t.Fatal("independent direct ref deleted on abort")
	}
	alias, err := Run(ctx, RunOpts{Dir: repo}, "symbolic-ref", refs[1].Ref)
	if err != nil || string(alias) != "refs/heads/main\n" {
		t.Fatal("alias was replaced")
	}
}

func TestPrivateRefDeleteReportsBoundedPartialGroups(t *testing.T) {
	repo := initRepo(t)
	ctx := t.Context()
	tree, err := WriteTreeDurable(ctx, repo, filepath.Join(t.TempDir(), "empty.index"), nil)
	if err != nil {
		t.Fatal(err)
	}
	oid, err := CommitTreeDurable(ctx, repo, tree, "original", "Original", "original@example.com")
	if err != nil {
		t.Fatal(err)
	}
	if err := UpdateRef(ctx, repo, "refs/heads/main", oid, ""); err != nil {
		t.Fatal(err)
	}
	var refs []RefExpectation
	for i := 0; i < 129; i++ {
		refs = append(refs, RefExpectation{Ref: fmt.Sprintf("refs/acd/checkpoints/bounded/%03d", i), OID: oid})
	}
	for start := 0; start < len(refs); start += 64 {
		if err := CreateRefsCAS(ctx, repo, refs[start:min(start+64, len(refs))]); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := Run(ctx, RunOpts{Dir: repo}, "symbolic-ref", refs[128].Ref, "refs/heads/main"); err != nil {
		t.Fatal(err)
	}
	removed, err := DeletePrivateRefsCAS(ctx, repo, refs)
	if err == nil || len(removed) != 128 {
		t.Fatalf("wrong partial proof %d: %v", len(removed), err)
	}
	for start := 0; start < len(removed); start += 64 {
		if err := CreateRefsCAS(ctx, repo, removed[start:min(start+64, len(removed))]); err != nil {
			t.Fatal(err)
		}
	}
	for _, ref := range refs {
		actual, err := RevParse(ctx, repo, ref.Ref)
		if err != nil || actual != oid {
			t.Fatalf("partial rollback lost %s: %s %v", ref.Ref, actual, err)
		}
	}
	actual, err := RevParse(ctx, repo, "refs/heads/main")
	if err != nil || actual != oid {
		t.Fatal("public branch changed")
	}
}

func TestPrivateRefDeleteLostCommitAcknowledgementReportsRemoved(t *testing.T) {
	repo := initRepo(t)
	ctx := t.Context()
	tree, err := WriteTreeDurable(ctx, repo, filepath.Join(t.TempDir(), "empty.index"), nil)
	if err != nil {
		t.Fatal(err)
	}
	oid, err := CommitTreeDurable(ctx, repo, tree, "original", "Original", "original@example.com")
	if err != nil {
		t.Fatal(err)
	}
	refs := []RefExpectation{{Ref: "refs/acd/checkpoints/ack/one", OID: oid}, {Ref: "refs/acd/checkpoints/ack/two", OID: oid}}
	if err := CreateRefsCAS(ctx, repo, refs); err != nil {
		t.Fatal(err)
	}
	hook := filepath.Join(repo, ".git", "hooks", "reference-transaction")
	// Lose the acknowledgement only after Git commits, regardless of startup time.
	if err := os.WriteFile(hook, []byte("#!/bin/sh\nif [ \"$1\" = committed ]; then kill -KILL \"$PPID\"; fi\n"), 0755); err != nil {
		t.Fatal(err)
	}
	removed, err := DeletePrivateRefsCAS(ctx, repo, refs)
	if err == nil || len(removed) != 2 {
		t.Fatalf("committed removals not reported: %d %v", len(removed), err)
	}
	if err := os.Remove(hook); err != nil {
		t.Fatal(err)
	}
	if err := CreateRefsCAS(ctx, repo, removed); err != nil {
		t.Fatal(err)
	}
	for _, ref := range refs {
		actual, err := RevParse(ctx, repo, ref.Ref)
		if err != nil || actual != oid {
			t.Fatal("lost acknowledgement prevented rollback")
		}
	}
}
