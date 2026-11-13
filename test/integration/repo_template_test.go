//go:build integration

package integration_test

import (
	"os"
	"path/filepath"
	"testing"
)

func TestRepoTemplateCopyPreservesGitStateWithoutMaintenanceLocks(t *testing.T) {
	source, target := tempRepo(t), t.TempDir()
	for _, path := range []string{".git/objects/maintenance.lock", "project.lock"} {
		writeFile(t, filepath.Join(source, path), "keep only project data\n")
	}
	if err := copyRepoTemplate(source, target); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(target, ".git/objects/maintenance.lock")); !os.IsNotExist(err) {
		t.Fatalf("copied a source-process lock: %v", err)
	}
	if body, err := os.ReadFile(filepath.Join(target, "project.lock")); err != nil || string(body) != "keep only project data\n" {
		t.Fatalf("lost a project lockfile: %q %v", body, err)
	}
	if runGitOK(t, source, "rev-parse", "HEAD") != runGitOK(t, target, "rev-parse", "HEAD") {
		t.Fatal("copy changed the repository history")
	}
}
