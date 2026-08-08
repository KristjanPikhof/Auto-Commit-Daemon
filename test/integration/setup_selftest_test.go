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
