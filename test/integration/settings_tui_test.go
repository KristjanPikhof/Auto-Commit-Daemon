//go:build integration
// +build integration

package integration_test

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

func TestSettingsTUIRealPTYLayoutsResizeAndRestore(t *testing.T) {
	repo := tempRepo(t)
	env := envWith(withIsolatedHome(t), "TERM=xterm-256color")
	bin := buildAcdBinary(t)

	for _, tc := range []struct {
		name       string
		cols, rows int
		want       string
	}{
		{name: "wide", cols: 120, rows: 40, want: "FIELDS"},
		{name: "medium", cols: 84, rows: 30, want: "DETAILS"},
		{name: "narrow", cols: 58, rows: 22, want: "FIELD 1/"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			result := runPTYCommand(t, ctx, env, tc.cols, tc.rows, 0, 0, "q", bin, "settings", "--repo", repo)
			if result.ExitCode != 0 {
				t.Fatalf("settings PTY exit=%d\n%s", result.ExitCode, result.Stdout)
			}
			if !strings.Contains(result.Stdout, "ACD SETTINGS") || !strings.Contains(result.Stdout, tc.want) {
				t.Fatalf("missing %q layout at %dx%d\n%s", tc.want, tc.cols, tc.rows, result.Stdout)
			}
			assertAltScreenRestored(t, result.Stdout)
		})
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	resized := runPTYCommand(t, ctx, env, 120, 36, 58, 20, "q", bin, "settings", "--repo", repo)
	if resized.ExitCode != 0 {
		t.Fatalf("resize PTY exit=%d\n%s", resized.ExitCode, resized.Stdout)
	}
	if !strings.Contains(resized.Stdout, "FIELDS") || !strings.Contains(resized.Stdout, "FIELD 1/") {
		t.Fatalf("SIGWINCH did not render both wide and narrow layouts\n%s", resized.Stdout)
	}
	assertAltScreenRestored(t, resized.Stdout)
}

func TestSettingsTUIKeyboardNoColorAccessibleAndDirtyDiscard(t *testing.T) {
	repo := tempRepo(t)
	baseEnv := envWith(withIsolatedHome(t), "TERM=xterm-256color")
	bin := buildAcdBinary(t)
	dbPath := filepath.Join(repo, ".git", "acd", "state.db")

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	noColor := runPTYCommand(t, ctx, envWith(baseEnv, "NO_COLOR=1"), 84, 28, 0, 0, "jkq", bin, "settings", "--repo", repo)
	cancel()
	if noColor.ExitCode != 0 {
		t.Fatalf("NO_COLOR PTY exit=%d\n%s", noColor.ExitCode, noColor.Stdout)
	}
	if strings.Contains(noColor.Stdout, "\x1b[38;") || strings.Contains(noColor.Stdout, "\x1b[48;") {
		t.Fatalf("NO_COLOR emitted color SGR sequences\n%q", noColor.Stdout)
	}
	assertAltScreenRestored(t, noColor.Stdout)

	beforeConfig := readOptionalFile(t, settingsConfigPath(baseEnv))
	beforeRevisions := "0"
	if _, err := os.Stat(dbPath); err == nil {
		beforeRevisions = sqliteScalar(t, dbPath, "SELECT COUNT(*) FROM config_revisions")
	}
	ctx, cancel = context.WithTimeout(context.Background(), 15*time.Second)
	// Enter edit mode, replace the first field, accept, then use the explicit
	// dirty-quit discard confirmation. No provider test or apply key is sent.
	dirty := runPTYCommand(t, ctx, baseEnv, 84, 28, 0, 0, "\r\x15openai-compat\rqd", bin, "settings", "--repo", repo)
	cancel()
	if dirty.ExitCode != 0 {
		t.Fatalf("dirty-discard PTY exit=%d\n%s", dirty.ExitCode, dirty.Stdout)
	}
	if !strings.Contains(dirty.Stdout, "Unsaved DRAFT") {
		t.Fatalf("dirty quit confirmation not rendered\n%s", dirty.Stdout)
	}
	assertAltScreenRestored(t, dirty.Stdout)
	if got := readOptionalFile(t, settingsConfigPath(baseEnv)); got != beforeConfig {
		t.Fatalf("discarded draft mutated XDG config: before=%q after=%q", beforeConfig, got)
	}
	if got := sqliteScalar(t, dbPath, "SELECT COUNT(*) FROM config_revisions"); got != beforeRevisions {
		t.Fatalf("discarded draft created runtime revisions: before=%s after=%s", beforeRevisions, got)
	}

	ctx, cancel = context.WithTimeout(context.Background(), 15*time.Second)
	accessible := runPTYCommand(t, ctx, baseEnv, 72, 28, 0, 0, "\x03", bin, "settings", "--repo", repo, "--accessible")
	cancel()
	if accessible.ExitCode == 0 {
		t.Fatalf("cancelled accessible action unexpectedly succeeded\n%s", accessible.Stdout)
	}
	if !strings.Contains(accessible.Stdout, "ACD SETTINGS - accessible mode") || !strings.Contains(accessible.Stdout, "set/unset only; value never displayed") {
		t.Fatalf("accessible transcript missing\n%s", accessible.Stdout)
	}
	if !strings.Contains(accessible.Stdout, "Provider (next safe boundary) [current: deterministic; Enter keeps current]") {
		t.Fatalf("accessible prompt omitted retained value\n%s", accessible.Stdout)
	}
	if strings.Contains(accessible.Stdout, "\x1b[?1049h") {
		t.Fatalf("accessible mode entered alternate screen\n%q", accessible.Stdout)
	}
}

func TestSettingsTUIRealPTYActionsAndErrorRestoration(t *testing.T) {
	repo := tempRepo(t)
	env := envWith(withIsolatedHome(t), "TERM=xterm-256color")
	bin := buildAcdBinary(t)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	// Edit provider, save, wait for the real config write, then quit.
	saved := runPTYCommand(t, ctx, env, 100, 32, 0, 0,
		"\r\x15openai-compat\rs\x00q", bin, "settings", "--repo", repo)
	cancel()
	if saved.ExitCode != 0 || !strings.Contains(saved.Stdout, "draft saved") {
		t.Fatalf("save action exit=%d\n%s", saved.ExitCode, saved.Stdout)
	}
	assertAltScreenRestored(t, saved.Stdout)
	if body := readOptionalFile(t, settingsConfigPath(env)); !strings.Contains(body, `"ai.provider": "openai-compat"`) {
		t.Fatalf("save action did not persist scoped provider: %s", body)
	}

	ctx, cancel = context.WithTimeout(context.Background(), 20*time.Second)
	// Make the provider invalid, exercise the asynchronous strict-test error,
	// wait for it to render, then use the dirty-discard path to exit.
	failed := runPTYCommand(t, ctx, env, 100, 32, 0, 0,
		"\r\x15invalid-provider\rt\x00qd", bin, "settings", "--repo", repo)
	cancel()
	if failed.ExitCode != 0 || !strings.Contains(failed.Stdout, "FAILED:") {
		t.Fatalf("failed action exit=%d\n%s", failed.ExitCode, failed.Stdout)
	}
	assertAltScreenRestored(t, failed.Stdout)
}

func TestSettingsTUIProductionBinaryIsCGODisabled(t *testing.T) {
	bin := buildAcdBinary(t)
	out, err := exec.Command("file", bin).CombinedOutput()
	if err != nil {
		t.Fatalf("file binary: %v\n%s", err, out)
	}
	metadata := string(out)
	if !strings.Contains(metadata, "executable") {
		t.Fatalf("unexpected binary metadata: %s", metadata)
	}
	if runtime.GOOS == "linux" && !strings.Contains(strings.ToLower(metadata), "statically linked") {
		t.Fatalf("Linux integration binary is not static: %s", metadata)
	}
	// buildAcdBinary itself sets CGO_ENABLED=0 and the release build tags.
	// Darwin's pure-Go linker still records system framework dependencies, so
	// file(1) metadata is the portable host assertion there.
}

func assertAltScreenRestored(t *testing.T, output string) {
	t.Helper()
	enter := strings.LastIndex(output, "\x1b[?1049h")
	exit := strings.LastIndex(output, "\x1b[?1049l")
	if enter < 0 || exit <= enter {
		t.Fatalf("alternate screen was not entered and restored in order\n%q", output)
	}
}

func settingsConfigPath(env []string) string {
	home := ""
	config := ""
	for _, item := range env {
		if strings.HasPrefix(item, "HOME=") {
			home = strings.TrimPrefix(item, "HOME=")
		}
		if strings.HasPrefix(item, "XDG_CONFIG_HOME=") {
			config = strings.TrimPrefix(item, "XDG_CONFIG_HOME=")
		}
	}
	if config == "" {
		config = filepath.Join(home, ".config")
	}
	return filepath.Join(config, "acd", "config.json")
}

func readOptionalFile(t *testing.T, path string) string {
	t.Helper()
	body, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return ""
	}
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return string(body)
}
