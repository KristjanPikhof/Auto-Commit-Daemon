package daemon

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

func TestFinishPreservesSparseCheckoutEntries(t *testing.T) {
	for _, sparseIndex := range []bool{false, true} {
		name := "full-index"
		if sparseIndex {
			name = "sparse-index"
		}
		t.Run(name, func(t *testing.T) {
			f, w := finishWorkerFixture(t)
			finishWrite(t, f.dir, "visible/file.txt", "base\n")
			finishWrite(t, f.dir, "visible/delete.txt", "delete\n")
			finishWrite(t, f.dir, "hidden/file.txt", "keep\n")
			finishWorkerGit(t, f.dir, "add", "visible", "hidden")
			finishWorkerGit(t, f.dir, "commit", "-m", "Seed sparse checkout")
			args := []string{"sparse-checkout", "init", "--cone"}
			if sparseIndex {
				args = append(args, "--sparse-index")
			}
			finishWorkerGit(t, f.dir, args...)
			finishWorkerGit(t, f.dir, "sparse-checkout", "set", "visible")
			if _, err := os.Stat(filepath.Join(f.dir, "hidden/file.txt")); !os.IsNotExist(err) {
				t.Fatalf("hidden entry is present: %v", err)
			}
			finishWrite(t, f.dir, "visible/file.txt", "changed\n")
			if err := os.Remove(filepath.Join(f.dir, "visible/delete.txt")); err != nil {
				t.Fatal(err)
			}
			request := startFinishWorker(t, w, FinishOptions{Purpose: "Update visible files"})
			completed, err := w.Execute(context.Background(), request.ID)
			if err != nil || completed.State != state.FinishCompleted {
				t.Fatalf("finish: %+v %v", completed, err)
			}
			entries, err := readFinishTree(context.Background(), f.dir, "HEAD")
			if err != nil {
				t.Fatal(err)
			}
			if _, ok := entries["hidden/file.txt"]; !ok {
				t.Fatal("finish deleted unchanged sparse-checkout entry hidden/file.txt")
			}
			if body := finishWorkerGit(t, f.dir, "show", "HEAD:hidden/file.txt"); body != "keep" {
				t.Fatalf("hidden entry changed: %q", body)
			}
			if body := finishWorkerGit(t, f.dir, "show", "HEAD:visible/file.txt"); body != "changed" {
				t.Fatalf("visible change missing: %q", body)
			}
			if _, ok := entries["visible/delete.txt"]; ok {
				t.Fatal("visible deletion was not published")
			}
			if flags := finishWorkerGit(t, f.dir, "ls-files", "-v", "--", "hidden/file.txt"); flags != "S hidden/file.txt" {
				t.Fatalf("sparse flag changed: %q", flags)
			}
			if _, err := os.Stat(filepath.Join(f.dir, "hidden/file.txt")); !os.IsNotExist(err) {
				t.Fatalf("finish materialized sparse entry: %v", err)
			}
		})
	}
}
