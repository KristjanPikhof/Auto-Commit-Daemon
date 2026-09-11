//go:build integration

package integration_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/installer"
)

func TestSetupScratchSelfTest(t *testing.T) {
	// The worker runs with an isolated HOME. Inherited identity variables or
	// global Git configuration must not mask a missing scratch-repo identity.
	t.Setenv("GIT_CONFIG_NOSYSTEM", "1")
	t.Setenv("GIT_CONFIG_GLOBAL", os.DevNull)
	for _, key := range []string{"GIT_AUTHOR_NAME", "GIT_AUTHOR_EMAIL", "GIT_COMMITTER_NAME", "GIT_COMMITTER_EMAIL", "EMAIL", "GIT_CONFIG_COUNT", "GIT_CONFIG_PARAMETERS"} {
		t.Setenv(key, "")
		if err := os.Unsetenv(key); err != nil {
			t.Fatal(err)
		}
	}
	backupRoot, err := os.MkdirTemp("/tmp", "acd-setup-selftest-")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	err = installer.ScratchSelfTest(ctx, installer.Plan{
		BackupRoot:    backupRoot,
		ManagedBinary: buildAcdBinary(t),
	})
	if err != nil {
		t.Fatalf("scratch self-test failed in %s: %v", filepath.Clean(backupRoot), err)
	}
	if err := os.RemoveAll(backupRoot); err != nil {
		t.Fatal(err)
	}
}
