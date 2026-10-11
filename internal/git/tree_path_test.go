package git

import (
	"context"
	"testing"
)

func TestLsTreePreservesPathspecCharacters(t *testing.T) {
	ctx := context.Background()
	repo := initRepo(t)
	blob, err := HashObjectStdin(ctx, repo, []byte("content"))
	if err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{"[a].txt", "a*.txt", ":(literal)a.txt"} {
		t.Run(path, func(t *testing.T) {
			tree, err := Mktree(ctx, repo, []MktreeEntry{{Mode: "100644", Type: "blob", OID: blob, Path: path}})
			if err != nil {
				t.Fatal(err)
			}
			got, err := LsTreeBlobOID(ctx, repo, tree, path)
			if err != nil || got != blob {
				t.Fatalf("blob=%q want %q err=%v", got, blob, err)
			}
		})
	}
}

func TestLsFilesIndexTreatsCapturedPathsLiterally(t *testing.T) {
	ctx := context.Background()
	repo := initRepo(t)
	blob, err := HashObjectStdin(ctx, repo, []byte("content"))
	if err != nil {
		t.Fatal(err)
	}
	paths := []string{"[a].txt", "a.txt", "a*.txt", "abc.txt", ":(literal)a.txt"}
	var lines []string
	for _, path := range paths {
		lines = append(lines, "100644 "+blob+"\t"+path)
	}
	if err := UpdateIndexInfo(ctx, repo, "", lines); err != nil {
		t.Fatal(err)
	}
	for _, path := range paths {
		t.Run(path, func(t *testing.T) {
			entries, err := LsFilesStaged(ctx, repo, path)
			if err != nil || len(entries) != 1 || entries[0].Path != path {
				t.Fatalf("entries=%+v err=%v", entries, err)
			}
		})
	}
}
