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

// TestDetectInstalled_OpenCodeIgnoresBareYAMLMarker guards against the
// substring-collision regression: a hand-edited or third-party YAML that
// contains a bare `acd-managed: true` line (without the canonical `#`
// comment prefix) must NOT be classified as an acd-managed install. The
// canonical acd templates always write `# acd-managed: true`, so requiring
// the comment prefix removes the false-positive without breaking any real
// install. Asserts the canonical (`hook/hooks.yaml`) layout.
func TestDetectInstalled_OpenCodeIgnoresBareYAMLMarker(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)

	hooks := filepath.Join(home, ".config", "opencode", "hook", "hooks.yaml")
	if err := os.MkdirAll(filepath.Dir(hooks), 0o700); err != nil {
		t.Fatalf("mkdir opencode dir: %v", err)
	}
	// Bare key form (no `#` prefix). Could appear in a user-authored
	// hooks.yaml as a config key whose name collides with the acd marker.
	if err := os.WriteFile(hooks, []byte("hooks:\n  - id: foo\n    acd-managed: true\n"), 0o600); err != nil {
		t.Fatalf("write hooks.yaml: %v", err)
	}

	if got := DetectInstalled(); len(got) != 0 {
		t.Fatalf("DetectInstalled=%#v, want none (bare YAML acd-managed line must not match)", got)
	}
}

// TestDetectInstalled_PiIgnoresBareYAMLMarker mirrors the opencode case for
// the Pi harness path. Asserts the canonical (`agent/hook/hooks.yaml`) layout.
func TestDetectInstalled_PiIgnoresBareYAMLMarker(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)

	hooks := filepath.Join(home, ".pi", "agent", "hook", "hooks.yaml")
	if err := os.MkdirAll(filepath.Dir(hooks), 0o700); err != nil {
		t.Fatalf("mkdir pi dir: %v", err)
	}
	if err := os.WriteFile(hooks, []byte("hooks:\n  - id: foo\n    acd-managed: true\n"), 0o600); err != nil {
		t.Fatalf("write hooks.yaml: %v", err)
	}

	if got := DetectInstalled(); len(got) != 0 {
		t.Fatalf("DetectInstalled=%#v, want none (bare YAML acd-managed line must not match)", got)
	}
}

// TestDetectInstalled_OpenCodeMatchesCommentMarker confirms the canonical
// comment-form template still classifies as installed at the canonical
// `~/.config/opencode/hook/hooks.yaml` location.
func TestDetectInstalled_OpenCodeMatchesCommentMarker(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)

	hooks := filepath.Join(home, ".config", "opencode", "hook", "hooks.yaml")
	if err := os.MkdirAll(filepath.Dir(hooks), 0o700); err != nil {
		t.Fatalf("mkdir opencode dir: %v", err)
	}
	if err := os.WriteFile(hooks, []byte("# acd-managed: true\nhooks: []\n"), 0o600); err != nil {
		t.Fatalf("write hooks.yaml: %v", err)
	}

	got := DetectInstalled()
	if len(got) != 1 || got[0].Name() != "opencode" {
		t.Fatalf("DetectInstalled=%#v, want opencode only", got)
	}
	// ConfigPath() must report the canonical primary, regardless of where
	// the marker lives — doctor uses this to surface remediation hints.
	if path := got[0].ConfigPath(); path != hooks {
		t.Fatalf("ConfigPath=%q, want canonical %q", path, hooks)
	}
}

// TestDetectInstalled_OpenCodeLegacyPath confirms a marker at the
// pre-canonical legacy path (~/.config/opencode/hooks.yaml) still detects as
// installed so users mid-migration are not silently dropped from doctor's
// view. ConfigPath() still reports the canonical primary so remediation
// nudges them forward.
func TestDetectInstalled_OpenCodeLegacyPath(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)

	legacy := filepath.Join(home, ".config", "opencode", "hooks.yaml")
	if err := os.MkdirAll(filepath.Dir(legacy), 0o700); err != nil {
		t.Fatalf("mkdir opencode dir: %v", err)
	}
	if err := os.WriteFile(legacy, []byte("# acd-managed: true\nhooks: []\n"), 0o600); err != nil {
		t.Fatalf("write legacy hooks.yaml: %v", err)
	}

	got := DetectInstalled()
	if len(got) != 1 || got[0].Name() != "opencode" {
		t.Fatalf("DetectInstalled=%#v, want opencode only via legacy path", got)
	}
	canonical := filepath.Join(home, ".config", "opencode", "hook", "hooks.yaml")
	if path := got[0].ConfigPath(); path != canonical {
		t.Fatalf("ConfigPath=%q, want canonical %q (legacy must not steer remediation)", path, canonical)
	}
}

// TestDetectInstalled_OpenCodeCanonicalWinsOverLegacy asserts the pathSpec
// slice ordering for OpenCode: when both files carry the marker, the
// canonical primary wins for both ConfigPath (paths[0]) and MatchedPath
// (first marker-bearing path in slice order). Repurposed from a pure
// ConfigPath check to also exercise MatchedPath so this test genuinely
// fails when slice order in known.go is reversed.
func TestDetectInstalled_OpenCodeCanonicalWinsOverLegacy(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)

	canonical := filepath.Join(home, ".config", "opencode", "hook", "hooks.yaml")
	legacy := filepath.Join(home, ".config", "opencode", "hooks.yaml")
	if err := os.MkdirAll(filepath.Dir(canonical), 0o700); err != nil {
		t.Fatalf("mkdir opencode dir: %v", err)
	}
	if err := os.WriteFile(canonical, []byte("# acd-managed: true\nhooks: []\n"), 0o600); err != nil {
		t.Fatalf("write canonical hooks.yaml: %v", err)
	}
	if err := os.WriteFile(legacy, []byte("# acd-managed: true\nhooks: []\n"), 0o600); err != nil {
		t.Fatalf("write legacy hooks.yaml: %v", err)
	}

	got := DetectInstalled()
	if len(got) != 1 || got[0].Name() != "opencode" {
		t.Fatalf("DetectInstalled=%#v, want opencode only", got)
	}
	if path := got[0].ConfigPath(); path != canonical {
		t.Fatalf("ConfigPath=%q, want canonical primary %q", path, canonical)
	}
	// MatchedPath must also pick the canonical because slice iteration
	// hits the canonical pathSpec before the legacy one. Reversing the
	// slice order in known.go must break THIS assertion.
	matched, ok := got[0].MatchedPath()
	if !ok {
		t.Fatalf("MatchedPath ok=false, want true")
	}
	if matched != canonical {
		t.Fatalf("MatchedPath=%q, want canonical %q (slice order broken?)", matched, canonical)
	}

	// Sibling sub-case: when only the legacy file carries the marker,
	// MatchedPath must return legacy (proves the iteration falls through
	// past the canonical pathSpec when canonical has no marker).
	if err := os.Remove(canonical); err != nil {
		t.Fatalf("remove canonical: %v", err)
	}
	h, ok := Lookup("opencode")
	if !ok {
		t.Fatalf("Lookup(opencode) missing")
	}
	matched, ok = h.MatchedPath()
	if !ok {
		t.Fatalf("MatchedPath ok=false after canonical removed, want true")
	}
	if matched != legacy {
		t.Fatalf("MatchedPath=%q, want legacy %q (legacy fall-through broken?)", matched, legacy)
	}
}

// TestDetectInstalled_PiMatchesCommentMarker confirms the canonical Pi
// install at ~/.pi/agent/hook/hooks.yaml is detected.
func TestDetectInstalled_PiMatchesCommentMarker(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)

	hooks := filepath.Join(home, ".pi", "agent", "hook", "hooks.yaml")
	if err := os.MkdirAll(filepath.Dir(hooks), 0o700); err != nil {
		t.Fatalf("mkdir pi dir: %v", err)
	}
	if err := os.WriteFile(hooks, []byte("# acd-managed: true\nhooks: []\n"), 0o600); err != nil {
		t.Fatalf("write hooks.yaml: %v", err)
	}

	got := DetectInstalled()
	if len(got) != 1 || got[0].Name() != "pi" {
		t.Fatalf("DetectInstalled=%#v, want pi only", got)
	}
	if path := got[0].ConfigPath(); path != hooks {
		t.Fatalf("ConfigPath=%q, want canonical %q", path, hooks)
	}
}

// TestDetectInstalled_PiLegacyPath confirms a marker at the pre-canonical
// legacy Pi path (~/.pi/hook/hooks.yaml) still detects so users mid-
// migration are not silently dropped. ConfigPath() returns the canonical
// primary regardless.
func TestDetectInstalled_PiLegacyPath(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)

	legacy := filepath.Join(home, ".pi", "hook", "hooks.yaml")
	if err := os.MkdirAll(filepath.Dir(legacy), 0o700); err != nil {
		t.Fatalf("mkdir pi dir: %v", err)
	}
	if err := os.WriteFile(legacy, []byte("# acd-managed: true\nhooks: []\n"), 0o600); err != nil {
		t.Fatalf("write legacy hooks.yaml: %v", err)
	}

	got := DetectInstalled()
	if len(got) != 1 || got[0].Name() != "pi" {
		t.Fatalf("DetectInstalled=%#v, want pi only via legacy path", got)
	}
	canonical := filepath.Join(home, ".pi", "agent", "hook", "hooks.yaml")
	if path := got[0].ConfigPath(); path != canonical {
		t.Fatalf("ConfigPath=%q, want canonical %q (legacy must not steer remediation)", path, canonical)
	}
}

// TestDetectInstalled_PiCanonicalWinsOverLegacy mirrors the OpenCode
// ordering test for Pi: when both files carry the marker, canonical wins
// for both ConfigPath and MatchedPath. Repurposed from a pure ConfigPath
// check so reversing slice order in known.go genuinely breaks this test.
func TestDetectInstalled_PiCanonicalWinsOverLegacy(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)

	canonical := filepath.Join(home, ".pi", "agent", "hook", "hooks.yaml")
	legacy := filepath.Join(home, ".pi", "hook", "hooks.yaml")
	if err := os.MkdirAll(filepath.Dir(canonical), 0o700); err != nil {
		t.Fatalf("mkdir canonical pi dir: %v", err)
	}
	if err := os.MkdirAll(filepath.Dir(legacy), 0o700); err != nil {
		t.Fatalf("mkdir legacy pi dir: %v", err)
	}
	if err := os.WriteFile(canonical, []byte("# acd-managed: true\nhooks: []\n"), 0o600); err != nil {
		t.Fatalf("write canonical hooks.yaml: %v", err)
	}
	if err := os.WriteFile(legacy, []byte("# acd-managed: true\nhooks: []\n"), 0o600); err != nil {
		t.Fatalf("write legacy hooks.yaml: %v", err)
	}

	got := DetectInstalled()
	if len(got) != 1 || got[0].Name() != "pi" {
		t.Fatalf("DetectInstalled=%#v, want pi only", got)
	}
	if path := got[0].ConfigPath(); path != canonical {
		t.Fatalf("ConfigPath=%q, want canonical primary %q", path, canonical)
	}
	// MatchedPath must also pick canonical because slice iteration hits
	// the canonical pathSpec before the legacy one. Reversing slice order
	// in known.go must break THIS assertion.
	matched, ok := got[0].MatchedPath()
	if !ok {
		t.Fatalf("MatchedPath ok=false, want true")
	}
	if matched != canonical {
		t.Fatalf("MatchedPath=%q, want canonical %q (slice order broken?)", matched, canonical)
	}

	// Sibling sub-case: with only legacy carrying the marker, MatchedPath
	// returns legacy — proves the iteration falls past the canonical
	// pathSpec when canonical lacks a marker.
	if err := os.Remove(canonical); err != nil {
		t.Fatalf("remove canonical: %v", err)
	}
	h, ok := Lookup("pi")
	if !ok {
		t.Fatalf("Lookup(pi) missing")
	}
	matched, ok = h.MatchedPath()
	if !ok {
		t.Fatalf("MatchedPath ok=false after canonical removed, want true")
	}
	if matched != legacy {
		t.Fatalf("MatchedPath=%q, want legacy %q (legacy fall-through broken?)", matched, legacy)
	}
}

// TestMatchedPath_OpenCodeCanonicalOnly verifies MatchedPath returns the
// canonical path when only the canonical primary carries the marker.
func TestMatchedPath_OpenCodeCanonicalOnly(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)

	canonical := filepath.Join(home, ".config", "opencode", "hook", "hooks.yaml")
	if err := os.MkdirAll(filepath.Dir(canonical), 0o700); err != nil {
		t.Fatalf("mkdir opencode dir: %v", err)
	}
	if err := os.WriteFile(canonical, []byte("# acd-managed: true\nhooks: []\n"), 0o600); err != nil {
		t.Fatalf("write canonical hooks.yaml: %v", err)
	}

	h, ok := Lookup("opencode")
	if !ok {
		t.Fatalf("Lookup(opencode) missing")
	}
	got, found := h.MatchedPath()
	if !found {
		t.Fatalf("MatchedPath found=false, want true")
	}
	if got != canonical {
		t.Fatalf("MatchedPath=%q, want canonical %q", got, canonical)
	}
}

// TestMatchedPath_OpenCodeLegacyOnly verifies MatchedPath returns the legacy
// path when only the legacy fallback carries the marker.
func TestMatchedPath_OpenCodeLegacyOnly(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)

	legacy := filepath.Join(home, ".config", "opencode", "hooks.yaml")
	if err := os.MkdirAll(filepath.Dir(legacy), 0o700); err != nil {
		t.Fatalf("mkdir opencode dir: %v", err)
	}
	if err := os.WriteFile(legacy, []byte("# acd-managed: true\nhooks: []\n"), 0o600); err != nil {
		t.Fatalf("write legacy hooks.yaml: %v", err)
	}

	h, ok := Lookup("opencode")
	if !ok {
		t.Fatalf("Lookup(opencode) missing")
	}
	got, found := h.MatchedPath()
	if !found {
		t.Fatalf("MatchedPath found=false, want true")
	}
	if got != legacy {
		t.Fatalf("MatchedPath=%q, want legacy %q", got, legacy)
	}
}

// TestMatchedPath_OpenCodeBothCanonicalWins verifies pathSpec slice order:
// when both canonical and legacy carry markers, MatchedPath returns canonical.
func TestMatchedPath_OpenCodeBothCanonicalWins(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)

	canonical := filepath.Join(home, ".config", "opencode", "hook", "hooks.yaml")
	legacy := filepath.Join(home, ".config", "opencode", "hooks.yaml")
	if err := os.MkdirAll(filepath.Dir(canonical), 0o700); err != nil {
		t.Fatalf("mkdir opencode dir: %v", err)
	}
	if err := os.WriteFile(canonical, []byte("# acd-managed: true\nhooks: []\n"), 0o600); err != nil {
		t.Fatalf("write canonical hooks.yaml: %v", err)
	}
	if err := os.WriteFile(legacy, []byte("# acd-managed: true\nhooks: []\n"), 0o600); err != nil {
		t.Fatalf("write legacy hooks.yaml: %v", err)
	}

	h, ok := Lookup("opencode")
	if !ok {
		t.Fatalf("Lookup(opencode) missing")
	}
	got, found := h.MatchedPath()
	if !found {
		t.Fatalf("MatchedPath found=false, want true")
	}
	if got != canonical {
		t.Fatalf("MatchedPath=%q, want canonical %q (canonical must win over legacy)", got, canonical)
	}
}

// TestMatchedPath_OpenCodeNeither verifies MatchedPath returns "", false when
// no candidate carries a marker.
func TestMatchedPath_OpenCodeNeither(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)

	h, ok := Lookup("opencode")
	if !ok {
		t.Fatalf("Lookup(opencode) missing")
	}
	got, found := h.MatchedPath()
	if found {
		t.Fatalf("MatchedPath found=true (path=%q), want false", got)
	}
	if got != "" {
		t.Fatalf("MatchedPath=%q, want empty", got)
	}
}

// TestMatchedPath_PiCanonicalWinsOverLegacy mirrors the OpenCode ordering
// check for the Pi harness.
func TestMatchedPath_PiCanonicalWinsOverLegacy(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)

	canonical := filepath.Join(home, ".pi", "agent", "hook", "hooks.yaml")
	legacy := filepath.Join(home, ".pi", "hook", "hooks.yaml")
	if err := os.MkdirAll(filepath.Dir(canonical), 0o700); err != nil {
		t.Fatalf("mkdir canonical pi dir: %v", err)
	}
	if err := os.MkdirAll(filepath.Dir(legacy), 0o700); err != nil {
		t.Fatalf("mkdir legacy pi dir: %v", err)
	}
	if err := os.WriteFile(canonical, []byte("# acd-managed: true\nhooks: []\n"), 0o600); err != nil {
		t.Fatalf("write canonical hooks.yaml: %v", err)
	}
	if err := os.WriteFile(legacy, []byte("# acd-managed: true\nhooks: []\n"), 0o600); err != nil {
		t.Fatalf("write legacy hooks.yaml: %v", err)
	}

	h, ok := Lookup("pi")
	if !ok {
		t.Fatalf("Lookup(pi) missing")
	}
	got, found := h.MatchedPath()
	if !found {
		t.Fatalf("MatchedPath found=false, want true")
	}
	if got != canonical {
		t.Fatalf("MatchedPath=%q, want canonical %q (canonical must win over legacy)", got, canonical)
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
