package adapter

import (
	"os"
	"path/filepath"
	"testing"
)

func TestDetectInstalled_ClaudeCodeMarker(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)

	settings := filepath.Join(home, ".claude", "settings.json")
	if err := os.MkdirAll(filepath.Dir(settings), 0o700); err != nil {
		t.Fatalf("mkdir settings dir: %v", err)
	}
	if err := os.WriteFile(settings, []byte(`{"_acd_managed": true}`), 0o600); err != nil {
		t.Fatalf("write settings: %v", err)
	}

	got := DetectInstalled()
	if len(got) != 1 {
		t.Fatalf("DetectInstalled len=%d, want 1: %#v", len(got), got)
	}
	if got[0].Name() != "claude-code" {
		t.Fatalf("DetectInstalled[0]=%q, want claude-code", got[0].Name())
	}
}

func TestDetectInstalled_IgnoresUnmanagedConfig(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)

	settings := filepath.Join(home, ".claude", "settings.json")
	if err := os.MkdirAll(filepath.Dir(settings), 0o700); err != nil {
		t.Fatalf("mkdir settings dir: %v", err)
	}
	if err := os.WriteFile(settings, []byte(`{"hooks": {}}`), 0o600); err != nil {
		t.Fatalf("write settings: %v", err)
	}

	if got := DetectInstalled(); len(got) != 0 {
		t.Fatalf("DetectInstalled len=%d, want 0: %#v", len(got), got)
	}
}

func TestNamesIncludesSupportedHarnessesInOrder(t *testing.T) {
	want := []string{"claude-code", "codex", "opencode", "pi", "shell"}
	got := Names()
	if len(got) != len(want) {
		t.Fatalf("Names len=%d, want %d: %#v", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("Names[%d]=%q, want %q", i, got[i], want[i])
		}
	}
}
