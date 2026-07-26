//go:build integration
// +build integration

package integration_test

import (
	"context"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
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

func TestConfigureRealPTYNarrowResizeAccessibleAndNoColor(t *testing.T) {
	repo := tempRepo(t)
	baseEnv := envWith(withIsolatedHome(t), "TERM=xterm-256color")
	bin := buildAcdBinary(t)

	for _, tc := range []struct {
		name       string
		env        []string
		cols, rows int
		args       []string
		input      string
	}{
		{name: "narrow", env: baseEnv, cols: 52, rows: 18, input: "\x03"},
		{name: "accessible", env: baseEnv, cols: 58, rows: 20,
			args: []string{"--accessible"}, input: "1\n\x00\x03"},
		{name: "no_color", env: envWith(baseEnv, "NO_COLOR=1"),
			cols: 72, rows: 24, input: "\x03"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			args := append([]string{"configure", "--repo", repo}, tc.args...)
			command := append([]string{bin}, args...)
			result := runPTYCommand(t, ctx, tc.env, tc.cols, tc.rows, 0, 0,
				tc.input, command...)
			if result.ExitCode == 0 {
				t.Fatalf("cancelled configure unexpectedly succeeded\n%s",
					result.Stdout)
			}
			if !strings.Contains(result.Stdout, "How should ACD work?") ||
				!strings.Contains(result.Stdout, "Everyday work") {
				t.Fatalf("configure choices unreadable at %dx%d\n%s",
					tc.cols, tc.rows, result.Stdout)
			}
			if tc.name == "no_color" &&
				!strings.Contains(result.Stdout, "Strict review") {
				t.Fatalf("rich configure omitted experiences at %dx%d\n%s",
					tc.cols, tc.rows, result.Stdout)
			}
			if (tc.name == "narrow" || tc.name == "accessible") &&
				strings.Contains(result.Stdout, "\x1b[?1049h") {
				t.Fatalf("linear configure entered alternate screen\n%q",
					result.Stdout)
			}
			if (tc.name == "narrow" || tc.name == "accessible") &&
				(strings.Contains(result.Stdout, "\x1b[?2026") ||
					strings.Contains(result.Stdout, "\x1b[?2027")) {
				t.Fatalf("linear configure queried terminal capabilities\n%q",
					result.Stdout)
			}
			if tc.name == "no_color" &&
				(strings.Contains(result.Stdout, "\x1b[38;") ||
					strings.Contains(result.Stdout, "\x1b[48;")) {
				t.Fatalf("NO_COLOR configure emitted color SGR\n%q",
					result.Stdout)
			}
		})
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	resized := runPTYCommand(t, ctx, baseEnv, 100, 32, 52, 18,
		"\x03", bin, "configure", "--repo", repo)
	if resized.ExitCode == 0 ||
		!strings.Contains(resized.Stdout, "How should ACD work?") ||
		!strings.Contains(resized.Stdout, "Everyday work") {
		t.Fatalf("resized configure transcript incomplete\n%s", resized.Stdout)
	}
}

func TestConfigureFinalApprovalVisibleInNarrowPTY(t *testing.T) {
	repo := tempRepo(t)
	if err := os.WriteFile(filepath.Join(repo, "Makefile"),
		[]byte("test:\n\t@true\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	env := envWith(withIsolatedHome(t),
		"TERM=xterm-256color",
		"ACD_AI_API_KEY=configure-test-key",
		"ACD_AI_BASE_URL=https://provider.example.invalid/v1",
	)
	bin := buildAcdBinary(t)

	// A short terminal selects the linear renderer for the whole wizard.
	// Approve only the preview prerequisites, then cancel when the final
	// approval is visible.
	input := strings.Join([]string{
		"1\n",      // OpenAI-compatible provider
		"\n", "\n", // keep environment endpoint and default model
	}, "") + "\x00\x03" // cancel the one approval before calls or writes
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	result := runPTYCommand(t, ctx, env, 120, 18, 0, 0, input,
		bin, "configure", "--repo", repo,
		"--strategy", "intent", "--preset", "quality")
	if result.ExitCode == 0 {
		t.Fatalf("cancelled configure unexpectedly succeeded\n%s", result.Stdout)
	}
	previewAt := strings.Index(result.Stdout, "ACD CONFIGURE PREVIEW")
	if previewAt < 0 {
		t.Fatalf("configure never reached final preview\n%s", result.Stdout)
	}
	final := result.Stdout[previewAt:]
	for _, want := range []string{
		"Verification: full",
		"repository command will run in an ephemeral worktree: make test",
		"eligible recent ACD-owned commits may be repaired automatically",
		"Approve every permission shown above",
	} {
		if !strings.Contains(final, want) {
			t.Errorf("final approval missing %q\n%s", want, final)
		}
	}
	for _, rawMode := range []string{"\x1b[?25l", "\x1b[?2004h", "\x1b[?1004h"} {
		if strings.Contains(final, rawMode) {
			t.Errorf("final approval entered rich raw mode %q\n%q", rawMode, final)
		}
	}
	for _, query := range []string{"\x1b[?2026", "\x1b[?2027"} {
		if strings.Contains(result.Stdout, query) {
			t.Errorf("linear configure queried terminal capability %q\n%q", query, result.Stdout)
		}
	}
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
	if !strings.Contains(accessible.Stdout, "What do you want to do?") ||
		!strings.Contains(accessible.Stdout, "Test current settings (recommended)") {
		t.Fatalf("accessible prompt omitted action-first onboarding\n%s", accessible.Stdout)
	}
	if strings.Contains(accessible.Stdout, "Minimum pending (next safe boundary) [current:") {
		t.Fatalf("accessible start unexpectedly opened the advanced catalog\n%s", accessible.Stdout)
	}
	if strings.Contains(accessible.Stdout, "\x1b[?1049h") {
		t.Fatalf("accessible mode entered alternate screen\n%q", accessible.Stdout)
	}
}

func TestSettingsTUIAccessibleActionFirstTestAndRiskDecline(t *testing.T) {
	repo := tempRepo(t)
	baseEnv := envWith(withIsolatedHome(t), "TERM=xterm-256color")
	bin := buildAcdBinary(t)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	tested := runPTYCommand(t, ctx, baseEnv, 72, 28, 0, 0,
		"\n\x00y\n\x00", bin, "settings", "--repo", repo, "--accessible")
	cancel()
	if tested.ExitCode != 0 || !strings.Contains(tested.Stdout, "TESTED:") {
		t.Fatalf("action-first deterministic test exit=%d\n%s", tested.ExitCode, tested.Stdout)
	}
	if !strings.Contains(tested.Stdout, "Test current settings (recommended)") {
		t.Fatalf("action-first default was not rendered\n%s", tested.Stdout)
	}
	if strings.Contains(tested.Stdout, "Minimum pending (next safe boundary) [current:") {
		t.Fatalf("current-settings test visited the advanced catalog\n%s", tested.Stdout)
	}
	if strings.Contains(tested.Stdout, "\x1b[?1049h") {
		t.Fatalf("accessible test entered alternate screen\n%q", tested.Stdout)
	}

	riskEnv := envWith(baseEnv,
		"ACD_AI_PROVIDER=openai-compat",
		"ACD_AI_MODEL=synthetic-test-model",
		"ACD_AI_BASE_URL=https://example.invalid/v1",
		"ACD_AI_API_KEY=integration-placeholder",
	)
	ctx, cancel = context.WithTimeout(context.Background(), 20*time.Second)
	declined := runPTYCommand(t, ctx, riskEnv, 72, 28, 0, 0,
		"\n\x00y\n\x00n\n\x00", bin, "settings", "--repo", repo, "--accessible")
	cancel()
	if declined.ExitCode == 0 {
		t.Fatalf("declined endpoint risk unexpectedly succeeded\n%s", declined.Stdout)
	}
	if !strings.Contains(declined.Stdout, "send credentials to a non-default endpoint") ||
		!strings.Contains(declined.Stdout, "no request or change was made") {
		t.Fatalf("endpoint risk decline was not explicit\n%s", declined.Stdout)
	}
	if strings.Contains(declined.Stdout, "integration-placeholder") {
		t.Fatalf("accessible transcript exposed the test credential\n%s", declined.Stdout)
	}
	if strings.Contains(declined.Stdout, "\x1b[?1049h") {
		t.Fatalf("accessible risk prompt entered alternate screen\n%q", declined.Stdout)
	}
}

func TestSettingsTUIRealPTYConfirmationRetryAndApplyDecline(t *testing.T) {
	repo := tempRepo(t)
	baseEnv := envWith(withIsolatedHome(t), "TERM=xterm-256color")
	bin := buildAcdBinary(t)
	const response = `{
  "id": "chatcmpl-settings-probe",
  "object": "chat.completion",
  "model": "synthetic-test-model",
  "choices": [{
    "index": 0,
    "message": {
      "role": "assistant",
      "content": "",
      "tool_calls": [{
        "id": "call_settings_probe",
        "type": "function",
        "function": {
          "name": "commit_message",
          "arguments": "{\"subject\":\"Probe provider\",\"body\":\"\"}"
        }
      }]
    },
    "finish_reason": "tool_calls"
  }]
}`
	var hits atomic.Int32
	server, trustEnv := newOpenAITestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		if !strings.HasSuffix(r.URL.Path, "/chat/completions") {
			http.Error(w, "wrong path", http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(response))
	}))
	defer server.Close()
	providerEnv := envWith(baseEnv,
		"ACD_AI_PROVIDER=openai-compat",
		"ACD_AI_MODEL=synthetic-test-model",
		"ACD_AI_BASE_URL="+server.URL,
		"ACD_AI_API_KEY=integration-placeholder",
		trustEnv,
	)

	for _, key := range []string{"t", "T"} {
		t.Run("rich_"+key, func(t *testing.T) {
			before := hits.Load()
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			result := runPTYCommand(t, ctx, providerEnv, 100, 32, 0, 0,
				key+"\x00y\x00\x00q\x00", bin, "settings", "--repo", repo)
			cancel()
			if result.ExitCode != 0 || !strings.Contains(result.Stdout, "Confirm send credentials to a non-default endpoint?") ||
				!strings.Contains(result.Stdout, "TESTED") || !strings.Contains(result.Stdout, "[t/T] test") {
				t.Fatalf("rich %q confirmation/retry exit=%d\n%s", key, result.ExitCode, result.Stdout)
			}
			if got := hits.Load() - before; got != 1 {
				t.Fatalf("rich %q provider requests=%d want 1", key, got)
			}
			assertAltScreenRestored(t, result.Stdout)
		})
	}

	applyEnv := envWith(providerEnv, "ACD_AI_DIFF_EGRESS=true")
	beforeConfig := readOptionalFile(t, settingsConfigPath(applyEnv))
	beforeHits := hits.Load()
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	declined := runPTYCommand(t, ctx, applyEnv, 72, 28, 0, 0,
		"2\n\x00y\n\x00y\n\x00n\n\x00", bin, "settings", "--repo", repo, "--accessible")
	cancel()
	if declined.ExitCode == 0 {
		t.Fatalf("declined diff egress unexpectedly applied\n%s", declined.Stdout)
	}
	if !strings.Contains(declined.Stdout, "allow redacted repository diff egress") ||
		!strings.Contains(declined.Stdout, "synthetic test completed, but no apply or activation change was made") {
		t.Fatalf("accessible apply decline was inaccurate\n%s", declined.Stdout)
	}
	if got := hits.Load() - beforeHits; got != 1 {
		t.Fatalf("accessible apply provider requests=%d want 1", got)
	}
	if strings.Contains(declined.Stdout, "integration-placeholder") {
		t.Fatalf("accessible apply exposed the test credential\n%s", declined.Stdout)
	}
	if got := readOptionalFile(t, settingsConfigPath(applyEnv)); got != beforeConfig {
		t.Fatalf("declined apply changed config: before=%q after=%q", beforeConfig, got)
	}
	dbPath := filepath.Join(repo, ".git", "acd", "state.db")
	if got := sqliteScalar(t, dbPath, "SELECT COUNT(*) FROM config_revisions"); got != "0" {
		t.Fatalf("declined apply created runtime revisions: %s", got)
	}
	if strings.Contains(declined.Stdout, "\x1b[?1049h") {
		t.Fatalf("accessible apply entered alternate screen\n%q", declined.Stdout)
	}
}

func TestSettingsTUIRealPTYActionsAndErrorRestoration(t *testing.T) {
	repo := tempRepo(t)
	env := envWith(withIsolatedHome(t), "TERM=xterm-256color")
	bin := buildAcdBinary(t)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	// Edit provider, save, wait for the real config write, then quit.
	saved := runPTYCommand(t, ctx, env, 100, 32, 0, 0,
		"\r\x00\x15\x00openai-compat\x00\r\x00s\x00q", bin, "settings", "--repo", repo)
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
		"\r\x00\x15\x00invalid-provider\x00\r\x00t\x00qd", bin, "settings", "--repo", repo)
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
