//go:build integration

package integration_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestConfigureRealPTYNarrowResizeAccessibleAndNoColor(t *testing.T) {
	t.Parallel()

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
	t.Parallel()

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
		"Approve these permissions, save, and enable ACD?",
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

func TestSettingsTUIRealPTYLayoutsResizeAndRestore(t *testing.T) {
	t.Parallel()
	repo := tempRepo(t)
	env := envWith(withIsolatedHome(t), "TERM=xterm-256color")
	bin := buildAcdBinary(t)
	for _, size := range [][2]int{{120, 40}, {84, 30}, {58, 22}} {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		input := "\x03\x00"
		if size[1] < 24 {
			input = "10\n\x00"
		}
		result := runPTYCommand(t, ctx, env, size[0], size[1], 0, 0, input, bin, "config", "--repo", repo)
		cancel()
		if result.ExitCode != 0 || !strings.Contains(result.Stdout, "ACD Settings") || !strings.Contains(result.Stdout, "Model") {
			t.Fatalf("layout %v exit=%d\n%s", size, result.ExitCode, result.Stdout)
		}
		if strings.Contains(result.Stdout, "\x1b[?1049h") {
			assertAltScreenRestored(t, result.Stdout)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	resized := runPTYCommand(t, ctx, env, 120, 36, 58, 20, "\x03", bin, "config", "edit", "--repo", repo)
	if resized.ExitCode != 0 || !strings.Contains(resized.Stdout, "ACD Settings") {
		t.Fatalf("resize exit=%d\n%s", resized.ExitCode, resized.Stdout)
	}
	if strings.Contains(resized.Stdout, "\x1b[?1049h") {
		assertAltScreenRestored(t, resized.Stdout)
	}
	if _, err := os.Stat(filepath.Join(repo, ".git", "acd", "state.db")); !os.IsNotExist(err) {
		t.Fatal("opening/cancelling editor created repository state")
	}
}

func TestSettingsTUIKeyboardNoColorAccessibleAndDirtyDiscard(t *testing.T) {
	t.Parallel()
	repo := tempRepo(t)
	env := envWith(withIsolatedHome(t), "TERM=xterm-256color", "NO_COLOR=1")
	bin := buildAcdBinary(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	rich := runPTYCommand(t, ctx, env, 84, 28, 0, 0, "\x1b[B\x1b[B\r\x00\x15unsaved-model\r\x00\x03", bin, "config", "--repo", repo)
	cancel()
	if rich.ExitCode != 0 || strings.Contains(rich.Stdout, "\x1b[38;") || strings.Contains(rich.Stdout, "\x1b[48;") {
		t.Fatalf("rich exit=%d\n%s", rich.ExitCode, rich.Stdout)
	}
	ctx, cancel = context.WithTimeout(context.Background(), 20*time.Second)
	dirty := runPTYCommand(t, ctx, env, 72, 28, 0, 0, "3\n\x00unsaved-model\n\x0010\n\x00y\n\x00", bin, "config", "--repo", repo, "--accessible")
	cancel()
	if dirty.ExitCode != 0 || !strings.Contains(dirty.Stdout, "Discard unsaved changes") {
		t.Fatalf("discard exit=%d\n%s", dirty.ExitCode, dirty.Stdout)
	}
	if readOptionalFile(t, settingsConfigPath(env)) != "" {
		t.Fatal("discard wrote settings")
	}
	if strings.Contains(dirty.Stdout, "\x1b[?1049h") {
		t.Fatal("accessible editor entered alternate screen")
	}
}

func TestSettingsTUIAccessibleActionFirstTestAndRiskDecline(t *testing.T) {
	t.Parallel()
	repo := tempRepo(t)
	env := envWith(withIsolatedHome(t), "TERM=xterm-256color", "ACD_AI_PROVIDER=openai-compat", "ACD_AI_MODEL=synthetic-test-model", "ACD_AI_BASE_URL=https://example.invalid/v1", "ACD_AI_API_KEY=integration-placeholder")
	bin := buildAcdBinary(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	declined := runPTYCommand(t, ctx, env, 72, 28, 0, 0, "9\n\x00n\n\x0010\n\x00", bin, "config", "--repo", repo, "--accessible")
	if declined.ExitCode != 0 || !strings.Contains(declined.Stdout, "Permission: send credentials to https://example.invalid/v1") {
		t.Fatalf("decline exit=%d\n%s", declined.ExitCode, declined.Stdout)
	}
	if strings.Contains(declined.Stdout, "integration-placeholder") || strings.Contains(declined.Stdout, "Testing and saving") {
		t.Fatal("declined review probed or leaked key")
	}
	if readOptionalFile(t, settingsConfigPath(env)) != "" {
		t.Fatal("declined review wrote config")
	}
}

func TestSettingsTUIRealPTYConfirmationRetryAndApplyDecline(t *testing.T) {
	t.Parallel()
	repo := tempRepo(t)
	env := envWith(withIsolatedHome(t), "TERM=xterm-256color")
	bin := buildAcdBinary(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	// Review a model edit, decline it, then review and approve the retained draft.
	result := runPTYCommand(t, ctx, env, 84, 30, 0, 0, "3\n\x00retained-model\n\x009\n\x00n\n\x009\n\x00y\n\x00", bin, "config", "--repo", repo, "--accessible")
	if result.ExitCode != 0 || !strings.Contains(result.Stdout, "Waiting to apply") {
		t.Fatalf("retry exit=%d\n%s", result.ExitCode, result.Stdout)
	}
	if strings.Count(result.Stdout, "Review changes") != 2 {
		t.Fatal("declined review did not retain editable draft")
	}
	dbPath := filepath.Join(repo, ".git", "acd", "state.db")
	if got := sqliteScalar(t, dbPath, "SELECT COUNT(*) FROM config_revisions"); got != "1" {
		t.Fatalf("expected one approved activation, got %s", got)
	}
	if !strings.Contains(readOptionalFile(t, settingsConfigPath(env)), `"ai.model": "retained-model"`) {
		t.Fatal("reviewed model was not saved")
	}
}

func TestSettingsTUIRealPTYActionsAndErrorRestoration(t *testing.T) {
	t.Parallel()
	repo := tempRepo(t)
	env := envWith(withIsolatedHome(t), "TERM=xterm-256color")
	bin := buildAcdBinary(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	// Global is the initial scope even inside a worktree. Model is always editable.
	result := runPTYCommand(t, ctx, env, 84, 30, 0, 0, "3\n\x00global-model\n\x009\n\x00y\n\x00", bin, "config", "--accessible", "--scope", "global")
	if result.ExitCode != 0 || !strings.Contains(result.Stdout, "Global defaults") || !strings.Contains(result.Stdout, "Settings saved") {
		t.Fatalf("global save exit=%d\n%s", result.ExitCode, result.Stdout)
	}
	body := readOptionalFile(t, settingsConfigPath(env))
	if !strings.Contains(body, `"ai.model": "global-model"`) {
		t.Fatalf("model not saved: %s", body)
	}
	// An invalid value is rejected at the field and can be corrected before saving.
	ctx2, cancel2 := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel2()
	failed := runPTYCommand(t, ctx2, env, 84, 30, 0, 0, "4\n\x00not-a-url\n\x00https://api.openai.com/v1\n\x0010\n\x00y\n\x00", bin, "config", "--repo", repo, "--accessible")
	if failed.ExitCode != 0 || !strings.Contains(failed.Stdout, "Endpoint") {
		t.Fatalf("validation recovery exit=%d\n%s", failed.ExitCode, failed.Stdout)
	}
	if after := readOptionalFile(t, settingsConfigPath(env)); after != body {
		t.Fatal("discard after invalid input changed saved config")
	}
}

func TestConfigEditorAPIKeyMaskedAndTestedInRealPTY(t *testing.T) {
	t.Parallel()
	var calls atomic.Int32
	var wrongKey atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.Header.Get("Authorization") != "Bearer editor-secret-test-token" {
			wrongKey.Store(true)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"choices":[{"message":{"role":"assistant","tool_calls":[{"id":"test","type":"function","function":{"name":"commit_message","arguments":"{\"subject\":\"Test connection\",\"body\":\"\"}"}}]},"finish_reason":"tool_calls"}]}`))
	}))
	defer server.Close()
	repo := tempRepo(t)
	env := envWith(withIsolatedHome(t), "TERM=xterm-256color", "ACD_AI_PROVIDER=openai-compat", "ACD_AI_MODEL=synthetic-test-model", "ACD_AI_BASE_URL="+server.URL+"/v1", "ACD_AI_API_KEY=")
	bin := buildAcdBinary(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	result := runPTYCommand(t, ctx, env, 84, 30, 0, 0, "5\n\x00editor-secret-test-token\n\x009\n\x00y\n\x00", bin, "config", "--repo", repo, "--accessible")
	if result.ExitCode != 0 || !strings.Contains(result.Stdout, "Waiting to apply") {
		t.Fatalf("API key editor exit=%d\n%s", result.ExitCode, result.Stdout)
	}
	if strings.Contains(result.Stdout, "editor-secret-test-token") {
		t.Fatal("terminal exposed API key")
	}
	if calls.Load() != 1 || wrongKey.Load() {
		t.Fatalf("probe count=%d wrongKey=%t", calls.Load(), wrongKey.Load())
	}
	configPath := settingsConfigPath(env)
	if strings.Contains(readOptionalFile(t, configPath), "editor-secret-test-token") {
		t.Fatal("key entered ordinary config")
	}
	credentialPath := filepath.Join(filepath.Dir(configPath), "credentials.json")
	info, err := os.Stat(credentialPath)
	if err != nil || info.Mode().Perm() != 0600 {
		t.Fatalf("credential storage permissions: %v", err)
	}
	body := readOptionalFile(t, credentialPath)
	if !strings.Contains(body, "editor-secret-test-token") {
		t.Fatal("tested key was not stored")
	}
	dbPath := filepath.Join(repo, ".git", "acd", "state.db")
	snapshot := sqliteScalar(t, dbPath, "SELECT snapshot_json FROM config_revisions ORDER BY id DESC LIMIT 1")
	if strings.Contains(snapshot, "editor-secret-test-token") {
		t.Fatal("key entered runtime revision")
	}
}
