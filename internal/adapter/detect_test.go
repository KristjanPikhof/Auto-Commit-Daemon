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

func TestDetectInstalled_CodexHooksJSONMarker(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)

	hooks := filepath.Join(home, ".codex", "hooks.json")
	if err := os.MkdirAll(filepath.Dir(hooks), 0o700); err != nil {
		t.Fatalf("mkdir codex dir: %v", err)
	}
	if err := os.WriteFile(hooks, []byte(`{"_acd_managed": true,"hooks":{}}`), 0o600); err != nil {
		t.Fatalf("write hooks.json: %v", err)
	}

	got := DetectInstalled()
	if len(got) != 1 || got[0].Name() != "codex" {
		t.Fatalf("DetectInstalled=%#v, want codex only", got)
	}
	h, _ := Lookup("codex")
	if path := h.ConfigPath(); path != hooks {
		t.Fatalf("ConfigPath=%q, want %q", path, hooks)
	}
}

func TestDetectInstalled_CodexLegacyTOMLMarker(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)

	cfg := filepath.Join(home, ".codex", "config.toml")
	if err := os.MkdirAll(filepath.Dir(cfg), 0o700); err != nil {
		t.Fatalf("mkdir codex dir: %v", err)
	}
	if err := os.WriteFile(cfg, []byte("# acd-managed: true\n[features]\n"), 0o600); err != nil {
		t.Fatalf("write config.toml: %v", err)
	}

	got := DetectInstalled()
	if len(got) != 1 || got[0].Name() != "codex" {
		t.Fatalf("DetectInstalled=%#v, want codex only", got)
	}
}

func TestDetectInstalled_CodexHooksJSONIgnoresTOMLMarker(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)

	hooks := filepath.Join(home, ".codex", "hooks.json")
	if err := os.MkdirAll(filepath.Dir(hooks), 0o700); err != nil {
		t.Fatalf("mkdir codex dir: %v", err)
	}
	if err := os.WriteFile(hooks, []byte(`{"comment":"acd-managed: true"}`), 0o600); err != nil {
		t.Fatalf("write hooks.json: %v", err)
	}

	if got := DetectInstalled(); len(got) != 0 {
		t.Fatalf("DetectInstalled=%#v, want none (TOML marker must not match JSON path)", got)
	}
}

func TestDetectInstalled_CodexConfigTOMLIgnoresJSONMarker(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)

	cfg := filepath.Join(home, ".codex", "config.toml")
	if err := os.MkdirAll(filepath.Dir(cfg), 0o700); err != nil {
		t.Fatalf("mkdir codex dir: %v", err)
	}
	if err := os.WriteFile(cfg, []byte(`note = "\"_acd_managed\": true"`), 0o600); err != nil {
		t.Fatalf("write config.toml: %v", err)
	}

	if got := DetectInstalled(); len(got) != 0 {
		t.Fatalf("DetectInstalled=%#v, want none (JSON marker must not match TOML path)", got)
	}
}

func TestCodexInstalls_BothShadow(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)

	if err := os.MkdirAll(filepath.Join(home, ".codex"), 0o700); err != nil {
		t.Fatalf("mkdir codex dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(home, ".codex", "hooks.json"), []byte(`{"_acd_managed": true}`), 0o600); err != nil {
		t.Fatalf("write hooks.json: %v", err)
	}
	if err := os.WriteFile(filepath.Join(home, ".codex", "config.toml"), []byte("# acd-managed: true\n"), 0o600); err != nil {
		t.Fatalf("write config.toml: %v", err)
	}

	jsonOK, tomlOK := CodexInstalls()
	if !jsonOK || !tomlOK {
		t.Fatalf("CodexInstalls jsonOK=%v tomlOK=%v, want both true", jsonOK, tomlOK)
	}
}

func TestCodexInstalls_BothShadowWithConfigHomeLegacyTOML(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)

	if err := os.MkdirAll(filepath.Join(home, ".codex"), 0o700); err != nil {
		t.Fatalf("mkdir codex dir: %v", err)
	}
	if err := os.MkdirAll(filepath.Join(home, ".config", "codex"), 0o700); err != nil {
		t.Fatalf("mkdir config codex dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(home, ".codex", "hooks.json"), []byte(`{"_acd_managed": true}`), 0o600); err != nil {
		t.Fatalf("write hooks.json: %v", err)
	}
	if err := os.WriteFile(filepath.Join(home, ".config", "codex", "config.toml"), []byte("# acd-managed: true\n"), 0o600); err != nil {
		t.Fatalf("write legacy config.toml: %v", err)
	}

	jsonOK, tomlOK := CodexInstalls()
	if !jsonOK || !tomlOK {
		t.Fatalf("CodexInstalls jsonOK=%v tomlOK=%v, want both true", jsonOK, tomlOK)
	}
}

func TestCodexInstalls_NeitherFile(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	jsonOK, tomlOK := CodexInstalls()
	if jsonOK || tomlOK {
		t.Fatalf("CodexInstalls jsonOK=%v tomlOK=%v, want both false", jsonOK, tomlOK)
	}
}

func TestCodexInstalls_OnlyJSON(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	if err := os.MkdirAll(filepath.Join(home, ".codex"), 0o700); err != nil {
		t.Fatalf("mkdir codex dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(home, ".codex", "hooks.json"), []byte(`{"_acd_managed":true}`), 0o600); err != nil {
		t.Fatalf("write hooks.json: %v", err)
	}
	jsonOK, tomlOK := CodexInstalls()
	if !jsonOK || tomlOK {
		t.Fatalf("CodexInstalls jsonOK=%v tomlOK=%v, want jsonOK=true tomlOK=false", jsonOK, tomlOK)
	}
}
