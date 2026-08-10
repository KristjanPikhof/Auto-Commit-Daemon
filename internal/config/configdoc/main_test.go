package main

import (
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/config"
)

func TestCheckAcceptsExactGeneratedReference(t *testing.T) {
	path := filepath.Join(t.TempDir(), "reference.md")
	if err := os.WriteFile(path, config.RenderReference(), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := run(path, true, false, io.Discard); err != nil {
		t.Fatalf("check exact reference: %v", err)
	}
}

func TestCheckRejectsDrift(t *testing.T) {
	path := filepath.Join(t.TempDir(), "reference.md")
	if err := os.WriteFile(path, []byte("stale\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	err := run(path, true, false, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "is stale") {
		t.Fatalf("check drift error=%v, want stale error", err)
	}
}
