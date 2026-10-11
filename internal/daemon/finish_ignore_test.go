package daemon

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/finish"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

func TestFinishWorkerDefaultCaptureHonorsGitignore(t *testing.T) {
	f, w := finishWorkerFixture(t)
	// Runtime finish callers supply capture policy, not an injected checker.
	w.Config.Capture.IgnoreChecker = nil
	finishWrite(t, f.dir, ".gitignore", ".tmp/\n.trekoon/\ndist/\nbin/\n")
	finishWrite(t, f.dir, "nested/.gitignore", "*.tmp\n!keep.tmp\n")
	ignored := []string{".tmp/work.txt", ".trekoon/state.txt", "dist/app.js", "bin/acd", "nested/drop.tmp"}
	for _, path := range ignored {
		finishWrite(t, f.dir, path, "ignored\n")
	}
	finishWrite(t, f.dir, "nested/keep.tmp", "eligible negation\n")
	finishWrite(t, f.dir, "goal.txt", "eligible goal\n")
	r := startFinishWorker(t, w, FinishOptions{Purpose: "Save eligible changes"})
	for _, treeOID := range []string{r.Target.CheckpointTree, r.Target.TargetTree} {
		tree, err := readFinishTree(context.Background(), f.dir, treeOID)
		if err != nil {
			t.Fatal(err)
		}
		for _, path := range ignored {
			if _, present := tree[path]; present {
				t.Errorf("ignored path %q leaked into frozen tree %s", path, treeOID)
			}
		}
		for _, path := range []string{".gitignore", "nested/.gitignore", "nested/keep.tmp", "goal.txt"} {
			if _, present := tree[path]; !present {
				t.Errorf("eligible path %q missing from frozen tree", path)
			}
		}
	}
	completed, err := w.Execute(context.Background(), r.ID)
	if err != nil || completed.State != state.FinishCompleted {
		t.Fatalf("finish: %+v %v", completed, err)
	}
	published, err := readFinishTree(context.Background(), f.dir, "HEAD")
	if err != nil {
		t.Fatal(err)
	}
	for _, path := range ignored {
		if _, present := published[path]; present {
			t.Errorf("ignored path %q was committed", path)
		}
		if body, err := os.ReadFile(filepath.Join(f.dir, path)); err != nil || string(body) != "ignored\n" {
			t.Errorf("finish changed ignored working file %q: %q %v", path, body, err)
		}
	}
}

func TestFinishTargetRejectsExcludedCheckpointEntries(t *testing.T) {
	f, w := finishWorkerFixture(t)
	finishWrite(t, f.dir, ".gitignore", "dist/\nnested/*.tmp\n")
	// An old or incorrectly constructed checkpoint must not expand eligible scope.
	entry := finish.Entry{Mode: "100644", OID: finishWorkerGit(t, f.dir, "hash-object", "-w", "--stdin")}
	finishWrite(t, f.dir, "changed.txt", "changed\n")
	changed := finish.Entry{Mode: "100644", OID: finishWorkerGit(t, f.dir, "hash-object", "-w", "changed.txt")}
	base := finish.Tree{".env": entry}
	source := finish.Tree{"goal.txt": entry, "dist/app.js": entry, "nested/drop.tmp": entry, ".env": changed, ".env.local": changed}
	target, err := finishEligibleTarget(context.Background(), f.dir, base, source, w.Config.Capture)
	if err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{"dist/app.js", "nested/drop.tmp", ".env.local"} {
		if _, present := target[path]; present {
			t.Errorf("excluded checkpoint entry %q leaked into target", path)
		}
	}
	if target[".env"] != entry {
		t.Fatal("excluded base entry was removed")
	}
	if target["goal.txt"] != entry {
		t.Fatal("eligible checkpoint entry was removed")
	}
}

func TestProtectionScanDefaultOptionsHonorGitignore(t *testing.T) {
	f := newCaptureFixture(t)
	finishWrite(t, f.dir, ".gitignore", "scratch/\n")
	finishWrite(t, f.dir, "scratch/private.txt", "ignored\n")
	finishWrite(t, f.dir, "nested/.gitignore", "*.tmp\n!keep.tmp\n")
	finishWrite(t, f.dir, "nested/drop.tmp", "ignored\n")
	finishWrite(t, f.dir, "nested/keep.tmp", "eligible\n")
	entries, _, _, err := ScanProtectedEntries(context.Background(), f.dir, CaptureOpts{})
	if err != nil {
		t.Fatal(err)
	}
	found := map[string]bool{}
	for _, entry := range entries {
		found[entry.Path] = true
	}
	for _, path := range []string{"scratch/private.txt", "nested/drop.tmp"} {
		if found[path] {
			t.Errorf("default scan captured ignored path %q", path)
		}
	}
	for _, path := range []string{".gitignore", "nested/.gitignore", "nested/keep.tmp"} {
		if !found[path] {
			t.Errorf("default scan omitted eligible path %q", path)
		}
	}
}
