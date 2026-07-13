package settingsui

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func viewModel(width, height int) Model {
	m := New(nil)
	m.noColor = true
	m.Width = width
	m.Height = height
	m.Snapshot = baseSnapshot()
	m.PendingRevision = 8
	m.AppliedRevision = 7
	m.Draft = map[string]string{"ai.provider": "openai-compat", "ai.model": "new-model", "ai.base_url": "https://example.invalid/v1", "commit.strategy": "intent", "capture.sensitive_globs": "defaults"}
	m.Dirty = map[string]bool{"ai.model": true}
	m.Test = TestResult{OK: true, Summary: "synthetic request passed"}
	m.TestFingerprint = m.Fingerprint()
	m.Status = "QUEUED: applies at next safe boundary"
	m.Experiment = Experiment{Active: true, CompletedWindows: 3, TotalWindows: 10}
	return m
}

func TestResponsiveGoldenViews(t *testing.T) {
	for _, tc := range []struct {
		name string
		w, h int
	}{{"wide", 120, 36}, {"medium", 88, 28}, {"narrow", 60, 20}} {
		t.Run(tc.name, func(t *testing.T) {
			m := viewModel(tc.w, tc.h)
			got := m.Render()
			path := filepath.Join("testdata", tc.name+".golden")
			want, err := os.ReadFile(path)
			if err != nil {
				t.Fatalf("%v\n--- got ---\n%s", err, got)
			}
			if got != strings.TrimSuffix(string(want), "\n") {
				t.Fatalf("golden mismatch %s\n--- got ---\n%s\n--- want ---\n%s", tc.name, got, want)
			}
			for _, text := range []string{"Source:", "Changed:", "Apply:", "DRAFT", "TESTED", "QUEUED", "ACTIVE", "Experiment:", "descriptive", "[q] quit"} {
				if !strings.Contains(got, text) {
					t.Errorf("missing %q", text)
				}
			}
		})
	}
}

func TestViewQueuedFailureAndDirtyExit(t *testing.T) {
	m := viewModel(88, 28)
	m.Snapshot.PendingError = "provider rejected"
	m.Status = "FAILED: candidate rejected"
	got := m.Render()
	for _, s := range []string{"FAILED", "provider rejected", "candidate rejected"} {
		if !strings.Contains(got, s) {
			t.Errorf("missing %q", s)
		}
	}
	m.Mode = ModeConfirmQuit
	got = m.Render()
	if !strings.Contains(got, "discard and quit") {
		t.Fatal("dirty exit not rendered")
	}
}

func TestThemeNoColorAndDumb(t *testing.T) {
	for _, env := range []struct{ k, v string }{{"NO_COLOR", "1"}, {"TERM", "dumb"}} {
		t.Run(env.k, func(t *testing.T) {
			t.Setenv(env.k, env.v)
			m := viewModel(120, 36)
			if strings.Contains(m.Render(), "\x1b[") {
				t.Fatal("color escape in textual mode")
			}
			if !strings.Contains(m.Render(), "TESTED [yes]") {
				t.Fatal("status relied on color")
			}
		})
	}
}

func TestThemeColorEnabled(t *testing.T) {
	t.Setenv("NO_COLOR", "")
	t.Setenv("TERM", "xterm-256color")
	if !NewTheme(false).Enabled {
		t.Fatal("color-capable terminal unexpectedly disabled")
	}
	if !NewThemeForBackground(false, false).Enabled || !NewThemeForBackground(false, true).Enabled {
		t.Fatal("light/dark themes must both retain styled rendering")
	}
}

func TestThemeDumbRenderingIsASCII(t *testing.T) {
	t.Setenv("TERM", "dumb")
	m := viewModel(40, 20)
	m.noColor = false
	for _, r := range m.Render() {
		if r > 127 {
			t.Fatalf("non-ASCII rune %q in dumb terminal", r)
		}
	}
}

func TestViewControlSanitizeAndSecretRedaction(t *testing.T) {
	m := viewModel(120, 36)
	m.Snapshot.Profile = "safe\x1b[2J\x00"
	m.Snapshot.Fields = append(m.Snapshot.Fields, FieldValue{Key: "ai.api_key", Value: "sk-secret\x1b[31m", SensitiveSet: true})
	m.Draft["ai.api_key"] = "sk-secret"
	got := m.Render()
	for _, bad := range []string{"sk-secret", "\x00", "\x1b[2J"} {
		if strings.Contains(got, bad) {
			t.Fatalf("render leaked %q", bad)
		}
	}
	if !strings.Contains(got, "[set]") {
		t.Fatal("missing safe secret state")
	}
}

func TestViewStableFocusDuringPolling(t *testing.T) {
	m := viewModel(88, 28)
	m.Focus = 2
	before := m.ActiveField().Key
	m, _ = updated(t, m, PollMsg{})
	if m.Focus != 2 || m.ActiveField().Key != before {
		t.Fatal("poll moved focus")
	}
}
