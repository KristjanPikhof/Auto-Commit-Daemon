package git

import (
	"context"
	"testing"
)

func TestReachableObjectSizesForRefsReturnsUnion(t *testing.T) {
	ctx := context.Background()
	repo := initRepo(t)
	shared, err := HashObjectStdin(ctx, repo, []byte("shared\n"))
	if err != nil {
		t.Fatal(err)
	}
	makeRef := func(ref, path, body string) string {
		t.Helper()
		unique, hashErr := HashObjectStdin(ctx, repo, []byte(body))
		if hashErr != nil {
			t.Fatal(hashErr)
		}
		tree, treeErr := Mktree(ctx, repo, []MktreeEntry{
			{Mode: RegularFileMode, Type: "blob", OID: shared, Path: "shared.txt"},
			{Mode: RegularFileMode, Type: "blob", OID: unique, Path: path},
		})
		if treeErr != nil {
			t.Fatal(treeErr)
		}
		commit, commitErr := CommitTree(ctx, repo, tree, body)
		if commitErr != nil {
			t.Fatal(commitErr)
		}
		if updateErr := UpdateRef(ctx, repo, ref, commit, ""); updateErr != nil {
			t.Fatal(updateErr)
		}
		return unique
	}
	firstUnique := makeRef("refs/acd/inventory/first", "first.txt", "first\n")
	secondUnique := makeRef("refs/acd/inventory/second", "second.txt", "second\n")

	sizes, err := ReachableObjectSizesForRefs(ctx, repo, []string{
		"refs/acd/inventory/first", "refs/acd/inventory/second",
		"refs/acd/inventory/first",
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, oid := range []string{shared, firstUnique, secondUnique} {
		if sizes[oid] <= 0 {
			t.Fatalf("union is missing object %s: %v", oid, sizes)
		}
	}
}

func TestReachableObjectSizesForRefsRejectsInvalidRef(t *testing.T) {
	repo := initRepo(t)
	if _, err := ReachableObjectSizesForRefs(context.Background(), repo,
		[]string{"refs/acd/good\n^refs/heads/main"}); err == nil {
		t.Fatal("invalid revision input was accepted")
	}
}
