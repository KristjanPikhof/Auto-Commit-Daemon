package daemon

import (
	"testing"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
)

func TestExternalBridgeTreeProofPreservesLiteralFilenames(t *testing.T) {
	f := newCaptureFixture(t)
	ctx := t.Context()
	blob, err := git.HashObjectStdin(ctx, f.dir, []byte("captured content"))
	if err != nil {
		t.Fatal(err)
	}
	paths := []string{"ordinary.txt", ":(literal)a.txt", "a*.txt", "[a].txt"}
	var entries []git.MktreeEntry
	for _, path := range paths {
		entries = append(entries, git.MktreeEntry{Mode: "100644", Type: "blob", OID: blob, Path: path})
	}
	tree, err := git.Mktree(ctx, f.dir, entries)
	if err != nil {
		t.Fatal(err)
	}
	proved, err := externalBridgeIndexState(ctx, f.dir, tree, paths)
	if err != nil {
		t.Fatal(err)
	}
	if len(proved) != len(paths) {
		t.Fatalf("proof entries=%+v", proved)
	}
	for _, path := range paths {
		if entry := proved[path]; entry.Path != path || entry.OID != blob {
			t.Fatalf("proof for %q=%+v", path, entry)
		}
	}
}
