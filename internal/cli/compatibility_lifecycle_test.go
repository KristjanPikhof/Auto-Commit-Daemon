package cli

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestCompatibilityLifecycleRequiresExplicitSession(t *testing.T) {
	withIsolatedHome(t)
	repo := initRepoForRepoLifecycle(t)
	for command, replacement := range map[string]string{"start": "on", "stop": "off"} {
		t.Run(command, func(t *testing.T) {
			root := newRootCmd()
			var out bytes.Buffer
			root.SetOut(&out)
			root.SetErr(&out)
			root.SetArgs([]string{command, "--repo", repo, "--json"})
			err := root.ExecuteContext(context.Background())
			if err == nil || !strings.Contains(err.Error(), "acd "+replacement) {
				t.Fatalf("missing product guidance: %v", err)
			}
			if _, err := os.Stat(filepath.Join(repo, ".git", "acd")); !os.IsNotExist(err) {
				t.Fatalf("compatibility alias created repository state: %v", err)
			}
		})
	}
}

func TestCompatibilityStartPreservesRepositoryOptIn(t *testing.T) {
	withIsolatedHome(t)
	repo := initRepoForRepoLifecycle(t)
	root := newRootCmd()
	var out bytes.Buffer
	root.SetOut(&out)
	root.SetErr(&out)
	root.SetArgs([]string{"start", "--repo", repo, "--session-id", "editor-session", "--harness", "codex", "--json"})
	if err := root.ExecuteContext(context.Background()); err == nil {
		t.Fatal("session alias enabled an unregistered repository")
	}
	if _, err := os.Stat(filepath.Join(repo, ".git", "acd")); !os.IsNotExist(err) {
		t.Fatalf("session alias created repository state: %v", err)
	}
}
