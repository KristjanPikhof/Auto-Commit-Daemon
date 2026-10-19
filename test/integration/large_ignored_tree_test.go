//go:build integration

package integration_test

import (
	"fmt"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/daemon"
	gitpkg "github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestFinishFaultLargeIgnoredTree(t *testing.T) {
	if testing.Short() {
		t.Skip("50k ignored-file fixture")
	}
	f := newFinishFixture(t, "ACD_FSNOTIFY_ENABLED=0")
	writeFile(t, filepath.Join(f.Repo, ".gitignore"), "build/\n")
	if err := writeIgnoredFanout(filepath.Join(f.Repo, "build"), 50_000, 500); err != nil {
		t.Fatal(err)
	}
	writeFile(t, filepath.Join(f.Repo, "kept.txt"), "eligible work\n")
	start := time.Now()
	f.Barrier(t, t.Context())
	finishWaitProtectedFile(t, f, "kept.txt", "eligible work\n", 5*time.Second)
	if elapsed := time.Since(start); elapsed > 10*time.Second {
		t.Fatalf("protection took %s", elapsed)
	}
	checker := gitpkg.NewIgnoreChecker(f.Repo)
	defer checker.Close()
	entries, _, summary, err := daemon.ScanProtectedEntries(t.Context(), f.Repo, daemon.CaptureOpts{IgnoreChecker: checker})
	if err != nil {
		t.Fatal(err)
	}
	if summary.WalkedFiles >= 1000 {
		t.Fatalf("walked ignored descendants: %d", summary.WalkedFiles)
	}
	for _, entry := range entries {
		if strings.HasPrefix(entry.Path, "build/") {
			t.Fatalf("ignored entry: %+v", entry)
		}
	}
}

// writeIgnoredFanout writes `total` empty files spread across multiple
// subdirectories (≤ perDir files each). Returns the first I/O error.
func writeIgnoredFanout(root string, total, perDir int) error {
	if perDir <= 0 {
		perDir = 500
	}
	if err := os.MkdirAll(root, 0o755); err != nil {
		return fmt.Errorf("mkdir %s: %w", root, err)
	}
	for i := 0; i < total; i++ {
		dirIdx := i / perDir
		sub := filepath.Join(root, fmt.Sprintf("d%05d", dirIdx))
		if i%perDir == 0 {
			if err := os.MkdirAll(sub, 0o755); err != nil {
				return fmt.Errorf("mkdir %s: %w", sub, err)
			}
		}
		leaf := filepath.Join(sub, fmt.Sprintf("f%06d.bin", i))
		f, err := os.Create(leaf)
		if err != nil {
			return fmt.Errorf("create %s: %w", leaf, err)
		}
		if err := f.Close(); err != nil {
			return fmt.Errorf("close %s: %w", leaf, err)
		}
	}
	return nil
}
