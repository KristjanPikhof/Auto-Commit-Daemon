//go:build integration
// +build integration

package integration_test

// adapter_e2e_test.go — §7.9 / §9 end-to-end coverage. Each subtest renders a
// harness's snippet via `acd setup <harness>`, executes the start-equivalent
// command(s) under a fake harness env (mock CLAUDE_PROJECT_DIR /
// OPENCODE_SESSION_ID / PI_SESSION_ID / etc.), and asserts the daemon's
// per-repo state.db has the expected daemon_clients row (session_id + harness).
// Then runs the stop-equivalent command (or `acd stop --force` fallback) so
// the daemon shuts down cleanly between subtests.
//
// Skip rules: bash missing → skip the file; Windows → skip the file.

import (
	"context"
	"encoding/json"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"testing"
	"time"

	pausepkg "github.com/KristjanPikhof/Auto-Commit-Daemon/internal/pause"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/templates"
)

// TestAdapterE2E orchestrates the five harness subtests.
func TestAdapterE2E(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("adapter e2e: Windows snippets not in scope for v1")
	}
	if _, err := exec.LookPath("bash"); err != nil {
		t.Skip("adapter e2e: bash not on PATH")
	}
	if _, err := exec.LookPath("sqlite3"); err != nil {
		t.Skip("adapter e2e: sqlite3 binary required for daemon_clients probes")
	}

	// Ensure init renders for every harness up-front so a missing snippet
	// surfaces as one obvious failure rather than five copies.
	bin := buildAcdBinary(t)
	for _, h := range []string{"claude-code", "codex", "cursor", "opencode", "pi", "shell"} {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		out := runAcd(t, ctx, os.Environ(), "setup", h)
		cancel()
		if out.ExitCode != 0 {
			t.Fatalf("acd setup %s exit=%d\nstdout=%s\nstderr=%s",
				h, out.ExitCode, out.Stdout, out.Stderr)
		}
		if len(strings.TrimSpace(out.Stdout)) == 0 {
			t.Fatalf("acd setup %s emitted empty stdout", h)
		}
	}

	t.Run("claude-code", func(t *testing.T) {
		runClaudeCodeE2E(t, bin)
	})
	t.Run("codex", func(t *testing.T) {
		runCodexE2E(t, bin)
		runCodexMissingAcdWritesHookLog(t)
		runCodexLegacyTOMLAutoDetect(t, bin)
	})
	t.Run("cursor", func(t *testing.T) {
		runCursorE2E(t, bin)
	})
	t.Run("opencode", func(t *testing.T) {
		runOpencodeE2E(t, bin)
	})
	t.Run("pi", func(t *testing.T) {
		runPiE2E(t, bin)
	})
	t.Run("shell", func(t *testing.T) {
		runShellE2E(t, bin)
	})
	t.Run("pause-resume", func(t *testing.T) {
		runPauseResumeE2E(t, bin)
	})
}

// runPauseResumeE2E covers P2 finding 15: drives `acd pause` / `acd resume`
// against a daemon spawned through the existing claude-code harness lifecycle
// and asserts the on-disk marker, marker mode, and `acd status --json`
// projection of the pause state in both directions. We reuse the claude-code
// path (vs reimplementing daemon spawn) so this stays one of the AdapterE2E
// subtests instead of a parallel scaffold.
func runPauseResumeE2E(t *testing.T, bin string) {
	body := readSnippet(t, "claude-code/settings.snippet.json")
	hooks := parseClaudeCodeSnippet(t, body)

	repo := tempRepo(t)
	binDir := filepath.Dir(bin)
	sessionID := "e2e-pause-resume"
	stdin := fmt.Sprintf(`{"session_id":"%s"}`, sessionID)

	env := adapterEnv(t, binDir, "CLAUDE_PROJECT_DIR="+repo)
	env = addFailingJQ(t, env)

	// Drive the claude-code SessionStart hook so the daemon comes up under
	// the same code path the harness uses in production.
	startHook := pickHookByEvent(t, hooks, "SessionStart")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	res := runBash(t, ctx, env, stdin, startHook.Command)
	if res.ExitCode != 0 {
		t.Fatalf("pause-resume SessionStart exit=%d\nstdout=%s\nstderr=%s",
			res.ExitCode, res.Stdout, res.Stderr)
	}
	waitFor(t, "pause-resume daemon mode==running", 10*time.Second, func() bool {
		return readDaemonStateMode(repo) == "running"
	})
	assertClientRow(t, repo, sessionID, "claude-code", 5*time.Second)

	// Always tear down the daemon, even on assertion failure, so subsequent
	// subtests start from a clean slate.
	t.Cleanup(func() {
		stopHook := pickHookByEvent(t, hooks, "SessionEnd")
		stopRes := runBash(t, ctx, env, stdin, stopHook.Command)
		if stopRes.ExitCode != 0 {
			// Fall back to forced stop so we never leave a stray daemon
			// behind.
			t.Logf("pause-resume SessionEnd exit=%d (stdout=%s stderr=%s); forcing stop",
				stopRes.ExitCode, stopRes.Stdout, stopRes.Stderr)
			shutdownDaemon(t, env, repo, sessionID)
			return
		}
		waitDaemonStoppedOrKill(t, "pause-resume daemon stopped", repo)
	})

	// Use pausepkg.Path so a future marker-path refactor breaks the test
	// instead of rubber-stamping a stale hardcoded location.
	markerPath := pausepkg.Path(filepath.Join(repo, ".git"))

	// Step 1: `acd pause --reason e2e --yes` writes a 0o600 marker.
	pauseRes := runAcd(t, ctx, env,
		"pause", "--repo", repo, "--reason", "e2e", "--yes", "--json")
	if pauseRes.ExitCode != 0 {
		t.Fatalf("acd pause exit=%d\nstdout=%s\nstderr=%s",
			pauseRes.ExitCode, pauseRes.Stdout, pauseRes.Stderr)
	}
	info, err := os.Lstat(markerPath)
	if err != nil {
		t.Fatalf("stat pause marker: %v", err)
	}
	if !info.Mode().IsRegular() {
		t.Fatalf("pause marker is not a regular file: mode=%v", info.Mode())
	}
	// Permissions: 0o600. We test only the lower 9 bits because the temp file
	// publish path may set additional bits (e.g. setgid via umask) on some
	// platforms; the security-relevant part is "no world/group access".
	if perm := info.Mode().Perm(); perm != 0o600 {
		t.Fatalf("pause marker mode=%o want 0o600", perm)
	}

	// Step 2: `acd status --json` reflects paused=true with source=manual.
	assertStatusPause(t, ctx, env, repo, true, "manual")

	// Step 3: `acd resume --yes` removes the marker.
	resumeRes := runAcd(t, ctx, env, "resume", "--repo", repo, "--yes", "--json")
	if resumeRes.ExitCode != 0 {
		t.Fatalf("acd resume exit=%d\nstdout=%s\nstderr=%s",
			resumeRes.ExitCode, resumeRes.Stdout, resumeRes.Stderr)
	}
	if _, err := os.Lstat(markerPath); !os.IsNotExist(err) {
		t.Fatalf("pause marker still present after acd resume: err=%v", err)
	}

	// Step 4: `acd status --json` reflects paused=false (Pause field absent).
	assertStatusPause(t, ctx, env, repo, false, "")
}

// assertStatusPause runs `acd status --json` and verifies the paused/source
// fields. wantSource is ignored when wantPaused==false.
func assertStatusPause(t *testing.T, ctx context.Context, env []string, repo string, wantPaused bool, wantSource string) {
	t.Helper()
	res := runAcd(t, ctx, env, "status", "--repo", repo, "--json")
	if res.ExitCode != 0 {
		t.Fatalf("acd status exit=%d\nstdout=%s\nstderr=%s",
			res.ExitCode, res.Stdout, res.Stderr)
	}
	var rep struct {
		Paused bool `json:"paused"`
		Pause  *struct {
			Source string `json:"source"`
		} `json:"pause"`
	}
	if err := json.Unmarshal([]byte(res.Stdout), &rep); err != nil {
		t.Fatalf("decode status json: %v\nstdout=%s", err, res.Stdout)
	}
	if rep.Paused != wantPaused {
		t.Fatalf("acd status paused=%v want %v\nstdout=%s",
			rep.Paused, wantPaused, res.Stdout)
	}
	if !wantPaused {
		if rep.Pause != nil {
			t.Fatalf("acd status pause object present after resume: %+v\nstdout=%s",
				rep.Pause, res.Stdout)
		}
		return
	}
	if rep.Pause == nil {
		t.Fatalf("acd status pause object nil, want source=%q\nstdout=%s",
			wantSource, res.Stdout)
	}
	if rep.Pause.Source != wantSource {
		t.Fatalf("acd status pause.source=%q want %q\nstdout=%s",
			rep.Pause.Source, wantSource, res.Stdout)
	}
}

// -----------------------------------------------------------------------------
// helpers
// -----------------------------------------------------------------------------

// adapterEnv stitches isolated $HOME, the binary's directory on PATH, and any
// per-harness extras into one env slice.
func adapterEnv(t *testing.T, binDir string, extras ...string) []string {
	t.Helper()
	base := withIsolatedHome(t)
	// Prepend binDir to PATH so `acd` resolves inside `bash -c '...'` even
	// without an absolute path.
	pathPrepended := false
	for i, kv := range base {
		if strings.HasPrefix(kv, "PATH=") {
			base[i] = "PATH=" + binDir + string(os.PathListSeparator) + strings.TrimPrefix(kv, "PATH=")
			pathPrepended = true
			break
		}
	}
	if !pathPrepended {
		base = append(base, "PATH="+binDir+string(os.PathListSeparator)+os.Getenv("PATH"))
	}
	return envWith(base, extras...)
}

func prependPath(env []string, dir string) []string {
	out := append([]string{}, env...)
	for i, kv := range out {
		if strings.HasPrefix(kv, "PATH=") {
			out[i] = "PATH=" + dir + string(os.PathListSeparator) + strings.TrimPrefix(kv, "PATH=")
			return out
		}
	}
	return append(out, "PATH="+dir)
}

func addFailingJQ(t *testing.T, env []string) []string {
	t.Helper()
	fakeBin := t.TempDir()
	jq := filepath.Join(fakeBin, "jq")
	writeFile(t, jq, "#!/usr/bin/env bash\necho jq should not be used >&2\nexit 127\n")
	if err := os.Chmod(jq, 0o755); err != nil {
		t.Fatalf("chmod fake jq: %v", err)
	}
	return prependPath(env, fakeBin)
}

func daemonStopped(repo string) bool {
	if readDaemonStateMode(repo) == "stopped" {
		return true
	}
	pid := readDaemonStatePID(repo)
	return pid > 0 && syscall.Kill(pid, 0) != nil
}

func waitDaemonStoppedOrKill(t *testing.T, label, repo string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if daemonStopped(repo) {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	pid := readDaemonStatePID(repo)
	if pid <= 0 {
		return
	}
	t.Logf("%s: daemon pid %d still alive after stop command; killing test daemon", label, pid)
	_ = syscall.Kill(pid, syscall.SIGTERM)
	deadline = time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if daemonStopped(repo) {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	_ = syscall.Kill(pid, syscall.SIGKILL)
	waitFor(t, label, 5*time.Second, func() bool {
		return daemonStopped(repo)
	})
}

// runBash runs `bash -c command` with the given env and stdin. Returns
// stdout, stderr, exit code.
func runBash(t *testing.T, ctx context.Context, env []string, stdin, command string) ExecResult {
	t.Helper()
	cmd := exec.CommandContext(ctx, "bash", "-c", command)
	cmd.Env = env
	if stdin != "" {
		cmd.Stdin = strings.NewReader(stdin)
	}
	var stdout, stderr strings.Builder
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()
	exit := 0
	if err != nil {
		if ee, ok := err.(*exec.ExitError); ok {
			exit = ee.ExitCode()
		} else {
			stderr.WriteString("\n[runBash]: " + err.Error())
			exit = -1
		}
	}
	return ExecResult{
		Stdout:   stdout.String(),
		Stderr:   stderr.String(),
		ExitCode: exit,
	}
}

// readSnippet reads a verbatim snippet file from the embedded templates FS.
func readSnippet(t *testing.T, path string) string {
	t.Helper()
	b, err := fs.ReadFile(templates.FS, path)
	if err != nil {
		t.Fatalf("read embedded snippet %s: %v", path, err)
	}
	return string(b)
}

// hookSpec captures one extracted shell command keyed by its hook event so
// tests can pick the start- and stop-equivalent entries.
type hookSpec struct {
	Event   string
	Command string
}

// parseClaudeCodeSnippet parses templates/claude-code/settings.snippet.json
// and returns one hookSpec per event/command pair. Claude Code's schema is
// nested: each event holds matcher groups, and each matcher group holds a
// `hooks` array of {type:"command", command:"…"} handlers. The matcher field
// is not used here — every command is exercised through the same fake stdin
// payload.
func parseClaudeCodeSnippet(t *testing.T, body string) []hookSpec {
	t.Helper()
	var doc struct {
		Hooks map[string][]struct {
			Matcher string `json:"matcher"`
			Hooks   []struct {
				Type    string `json:"type"`
				Command string `json:"command"`
			} `json:"hooks"`
		} `json:"hooks"`
	}
	if err := json.Unmarshal([]byte(body), &doc); err != nil {
		t.Fatalf("claude-code snippet not valid JSON: %v\n%s", err, body)
	}
	var out []hookSpec
	for _, event := range []string{"SessionStart", "PreToolUse", "PostToolUse", "Stop", "SessionEnd"} {
		for _, e := range doc.Hooks[event] {
			for _, h := range e.Hooks {
				out = append(out, hookSpec{Event: event, Command: h.Command})
			}
		}
	}
	if len(out) == 0 {
		t.Fatalf("claude-code snippet has no hooks: %s", body)
	}
	return out
}

// parseCodexHooksJSON walks the codex hooks.json snippet and returns one
// hookSpec per registered command. The schema mirrors claude-code:
// hooks.<EventName> is an array of entries, each with optional matcher and a
// hooks array of {type, timeout, command} handlers.
func parseCodexHooksJSON(t *testing.T, body string) []hookSpec {
	t.Helper()
	var doc struct {
		Hooks map[string][]struct {
			Matcher *string `json:"matcher,omitempty"`
			Hooks   []struct {
				Type    string `json:"type"`
				Timeout int    `json:"timeout"`
				Command string `json:"command"`
			} `json:"hooks"`
		} `json:"hooks"`
	}
	if err := json.Unmarshal([]byte(body), &doc); err != nil {
		t.Fatalf("parse codex hooks.json: %v\nbody:\n%s", err, body)
	}
	var hooks []hookSpec
	for ev, entries := range doc.Hooks {
		for _, entry := range entries {
			for _, h := range entry.Hooks {
				if h.Command == "" {
					continue
				}
				hooks = append(hooks, hookSpec{
					Event:   ev,
					Command: h.Command,
				})
			}
		}
	}
	if len(hooks) == 0 {
		t.Fatalf("codex hooks.json snippet contained no handlers:\n%s", body)
	}
	return hooks
}

// parseCursorHooksJSON walks templates/cursor/hooks.json. Cursor uses a flat
// schema: hooks.<eventName>[] entries each carry command and timeout directly
// (no nested type/hooks arrays like codex).
func parseCursorHooksJSON(t *testing.T, body string) []hookSpec {
	t.Helper()
	var doc struct {
		Hooks map[string][]struct {
			Command string `json:"command"`
			Timeout int    `json:"timeout"`
		} `json:"hooks"`
	}
	if err := json.Unmarshal([]byte(body), &doc); err != nil {
		t.Fatalf("parse cursor hooks.json: %v\nbody:\n%s", err, body)
	}
	var hooks []hookSpec
	for ev, entries := range doc.Hooks {
		for _, e := range entries {
			if e.Command == "" {
				continue
			}
			hooks = append(hooks, hookSpec{
				Event:   ev,
				Command: e.Command,
			})
		}
	}
	if len(hooks) == 0 {
		t.Fatalf("cursor hooks.json snippet contained no commands:\n%s", body)
	}
	return hooks
}

// installCursorHooks copies the embedded hooks.json and acd-lifecycle.sh into
// isolated $HOME/.cursor so hook commands resolve relative to ~/.cursor.
func installCursorHooks(t *testing.T, home string) {
	t.Helper()
	cursorDir := filepath.Join(home, ".cursor")
	hooksDir := filepath.Join(cursorDir, "hooks")
	if err := os.MkdirAll(hooksDir, 0o700); err != nil {
		t.Fatalf("install cursor hooks: mkdir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(cursorDir, "hooks.json"),
		[]byte(readSnippet(t, "cursor/hooks.json")), 0o600); err != nil {
		t.Fatalf("install cursor hooks: write hooks.json: %v", err)
	}
	scriptPath := filepath.Join(hooksDir, "acd-lifecycle.sh")
	writeFile(t, scriptPath, readSnippet(t, "cursor/hooks/acd-lifecycle.sh"))
	if err := os.Chmod(scriptPath, 0o755); err != nil {
		t.Fatalf("install cursor hooks: chmod lifecycle script: %v", err)
	}
}

func homeFromEnv(env []string) string {
	for _, kv := range env {
		if strings.HasPrefix(kv, "HOME=") {
			return strings.TrimPrefix(kv, "HOME=")
		}
	}
	return ""
}

// runCursorHook executes a hooks.json command with cwd=$HOME/.cursor, matching
// how Cursor resolves ./hooks/acd-lifecycle.sh paths.
func runCursorHook(t *testing.T, ctx context.Context, env []string, home, stdin, command string) ExecResult {
	t.Helper()
	cursorDir := filepath.Join(home, ".cursor")
	return runBash(t, ctx, env, stdin, "cd "+shellQuote(cursorDir)+" && "+command)
}

// parseYAMLBashBlocks extracts every `bash: |` heredoc block from an
// opencode/pi-style YAML hook snippet, plus the surrounding `id:` so we can
// pick the start/stop one. Returns specs in document order.
//
// We avoid pulling in a YAML dependency by walking lines and tracking the
// current `id:` and `event:` fields, then collecting the multi-line bash
// body that follows `bash: |` until indentation drops back.
func parseYAMLBashBlocks(t *testing.T, body string) []hookSpec {
	t.Helper()
	lines := strings.Split(body, "\n")
	var specs []hookSpec
	var curID, curEvent string
	for i := 0; i < len(lines); i++ {
		line := lines[i]
		trim := strings.TrimSpace(line)
		switch {
		case strings.HasPrefix(trim, "- id:"):
			curID = strings.TrimSpace(strings.TrimPrefix(trim, "- id:"))
		case strings.HasPrefix(trim, "id:"):
			curID = strings.TrimSpace(strings.TrimPrefix(trim, "id:"))
		case strings.HasPrefix(trim, "event:"):
			curEvent = strings.TrimSpace(strings.TrimPrefix(trim, "event:"))
		case strings.HasPrefix(trim, "- bash: |") || trim == "bash: |":
			// Determine the indent of the literal block scalar — every
			// content line must have at least this many leading spaces.
			j := i + 1
			blockIndent := -1
			var collected []string
			for ; j < len(lines); j++ {
				bl := lines[j]
				if strings.TrimSpace(bl) == "" {
					collected = append(collected, "")
					continue
				}
				ind := len(bl) - len(strings.TrimLeft(bl, " "))
				if blockIndent < 0 {
					blockIndent = ind
				}
				if ind < blockIndent {
					break
				}
				collected = append(collected, bl[blockIndent:])
			}
			cmd := strings.TrimSpace(strings.Join(collected, "\n"))
			specs = append(specs, hookSpec{
				Event:   curEvent + " (" + curID + ")",
				Command: cmd,
			})
			i = j - 1
		}
	}
	if len(specs) == 0 {
		t.Fatalf("yaml snippet contained no `bash: |` blocks:\n%s", body)
	}
	return specs
}

// pickHookByEvent returns the first hook whose Event field contains the
// given substring (case-sensitive). Fails the test if none matches.
func pickHookByEvent(t *testing.T, hooks []hookSpec, want string) hookSpec {
	t.Helper()
	for _, h := range hooks {
		if strings.Contains(h.Event, want) {
			return h
		}
	}
	t.Fatalf("no hook with event containing %q in %+v", want, hooks)
	return hookSpec{}
}

// clientRow models one daemon_clients row from the per-repo state.db.
type clientRow struct {
	SessionID string
	Harness   string
}

// readClients returns every daemon_clients row in the repo's state.db.
func readClients(t *testing.T, repo string) []clientRow {
	t.Helper()
	dbPath := filepath.Join(repo, ".git", "acd", "state.db")
	if _, err := os.Stat(dbPath); err != nil {
		return nil
	}
	out, err := exec.Command("sqlite3", "-separator", "|", dbPath,
		"SELECT session_id, harness FROM daemon_clients").CombinedOutput()
	if err != nil {
		t.Fatalf("sqlite3 daemon_clients: %v\n%s", err, out)
	}
	var rows []clientRow
	for _, line := range strings.Split(strings.TrimSpace(string(out)), "\n") {
		if line == "" {
			continue
		}
		parts := strings.SplitN(line, "|", 2)
		if len(parts) != 2 {
			continue
		}
		rows = append(rows, clientRow{SessionID: parts[0], Harness: parts[1]})
	}
	return rows
}

// assertClientRow polls daemon_clients for up to timeout, asserting a row
// with the given session_id + harness. Fails the test on timeout.
func assertClientRow(t *testing.T, repo, sessionID, harness string, timeout time.Duration) {
	t.Helper()
	waitFor(t, fmt.Sprintf("daemon_clients row session=%s harness=%s", sessionID, harness),
		timeout, func() bool {
			for _, c := range readClients(t, repo) {
				if c.SessionID == sessionID && c.Harness == harness {
					return true
				}
			}
			return false
		})
}

// shutdownDaemon force-stops any running daemon for `repo` so subsequent
// subtests start from a clean slate. Errors are surfaced via t.Logf so a
// stuck cleanup does not mask the real assertion failure.
func shutdownDaemon(t *testing.T, env []string, repo, sessionID string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	res := runAcd(t, ctx, env,
		"stop", "--repo", repo, "--session-id", sessionID, "--force", "--json")
	if res.ExitCode != 0 {
		t.Logf("cleanup acd stop --force exit=%d\nstdout=%s\nstderr=%s",
			res.ExitCode, res.Stdout, res.Stderr)
	}
	waitDaemonStoppedOrKill(t, "post-cleanup daemon stopped", repo)
}

func assertActiveHookSelfHeals(t *testing.T, label string, ctx context.Context, env []string, repo, sessionID, harness string, hook hookSpec, stdin string) {
	t.Helper()
	selfHealStop := runBash(t, ctx, env, "",
		"acd stop --session-id "+shellQuote(sessionID)+
			" --repo "+shellQuote(repo)+" --force >/dev/null 2>&1")
	if selfHealStop.ExitCode != 0 {
		t.Fatalf("%s self-heal pre-stop exit=%d\nstdout=%s\nstderr=%s",
			label, selfHealStop.ExitCode, selfHealStop.Stdout, selfHealStop.Stderr)
	}
	waitDaemonStoppedOrKill(t, label+" daemon stopped before self-heal", repo)
	if healRes := runBash(t, ctx, env, stdin, hook.Command); healRes.ExitCode != 0 {
		t.Fatalf("%s active-hook self-heal exit=%d\nstdout=%s\nstderr=%s",
			label, healRes.ExitCode, healRes.Stdout, healRes.Stderr)
	}
	waitFor(t, label+" daemon mode==running after self-heal", 10*time.Second, func() bool {
		return readDaemonStateMode(repo) == "running"
	})
	assertClientRow(t, repo, sessionID, harness, 5*time.Second)
}

// assertActiveHookSelfHealsAfterStopAll tears the daemon down via
// `acd stop --all --force` (registry-wide teardown rather than per-session
// deregistration), then re-fires the active hook and asserts the daemon
// comes back up with the same client registered. Covers P2-7 — the
// stop --all path was previously not exercised in the harness E2E flows.
func assertActiveHookSelfHealsAfterStopAll(t *testing.T, label string, ctx context.Context, env []string, repo, sessionID, harness string, hook hookSpec, stdin string) {
	t.Helper()
	selfHealStop := runBash(t, ctx, env, "",
		"acd stop --all --force >/dev/null 2>&1")
	if selfHealStop.ExitCode != 0 {
		t.Fatalf("%s stop-all self-heal pre-stop exit=%d\nstdout=%s\nstderr=%s",
			label, selfHealStop.ExitCode, selfHealStop.Stdout, selfHealStop.Stderr)
	}
	waitDaemonStoppedOrKill(t, label+" daemon stopped before stop-all self-heal", repo)
	if healRes := runBash(t, ctx, env, stdin, hook.Command); healRes.ExitCode != 0 {
		t.Fatalf("%s active-hook stop-all self-heal exit=%d\nstdout=%s\nstderr=%s",
			label, healRes.ExitCode, healRes.Stdout, healRes.Stderr)
	}
	waitFor(t, label+" daemon mode==running after stop-all self-heal", 10*time.Second, func() bool {
		return readDaemonStateMode(repo) == "running"
	})
	assertClientRow(t, repo, sessionID, harness, 5*time.Second)
}

// assertActiveHookFailsOnCorruptDB verifies the new chain semantics from the
// templates lane: when `acd start` fails (here because state.db is garbage),
// the active hook exits nonzero AND writes an "active hook failed exit=" line
// to the harness log under XDG_STATE_HOME/acd/<harness>-hook.log. Caller
// must have already torn the daemon down.
func assertActiveHookFailsOnCorruptDB(t *testing.T, label string, ctx context.Context, env []string, repo, harness string, hook hookSpec, stdin string) {
	t.Helper()
	// Resolve the harness log file from the env we are about to invoke the
	// hook under so the test reads the same file the hook writes.
	home := ""
	xdgState := ""
	for _, kv := range env {
		switch {
		case strings.HasPrefix(kv, "HOME="):
			home = strings.TrimPrefix(kv, "HOME=")
		case strings.HasPrefix(kv, "XDG_STATE_HOME="):
			xdgState = strings.TrimPrefix(kv, "XDG_STATE_HOME=")
		}
	}
	if home == "" {
		t.Fatalf("%s corrupt-db: HOME missing from env", label)
	}
	stateRoot := xdgState
	if stateRoot == "" {
		stateRoot = filepath.Join(home, ".local", "state")
	}
	logPath := filepath.Join(stateRoot, "acd", harness+"-hook.log")
	// Truncate any pre-existing log content so we can assert the failure
	// line was written by THIS hook invocation.
	_ = os.MkdirAll(filepath.Dir(logPath), 0o755)
	_ = os.WriteFile(logPath, nil, 0o644)

	// Corrupt state.db. The daemon's sqlite open should fail on garbage,
	// which propagates as a nonzero exit from `acd start`. Per the new
	// chain semantics, that nonzero must surface as a nonzero hook exit
	// (the previous `;` chain swallowed it).
	dbPath := filepath.Join(repo, ".git", "acd", "state.db")
	if err := os.MkdirAll(filepath.Dir(dbPath), 0o755); err != nil {
		t.Fatalf("%s corrupt-db: mkdir: %v", label, err)
	}
	if err := os.WriteFile(dbPath, []byte("not a sqlite database -- garbage bytes\n"), 0o644); err != nil {
		t.Fatalf("%s corrupt-db: write garbage: %v", label, err)
	}

	res := runBash(t, ctx, env, stdin, hook.Command)
	if res.ExitCode == 0 {
		t.Fatalf("%s active hook with corrupt state.db: want nonzero exit, got 0\nstdout=%s\nstderr=%s",
			label, res.Stdout, res.Stderr)
	}

	// Tail the harness log; it must include the "active hook failed exit="
	// line emitted by the snippet's failure branch.
	waitFor(t, label+" harness log records active hook failure", 5*time.Second, func() bool {
		body, err := os.ReadFile(logPath)
		if err != nil {
			return false
		}
		return strings.Contains(string(body), "active hook failed exit=")
	})

	// Clean up: remove the corrupt db so subsequent steps (or a fresh
	// daemon spawn from the caller) can rebuild a clean schema.
	if err := os.Remove(dbPath); err != nil && !os.IsNotExist(err) {
		t.Fatalf("%s corrupt-db: remove garbage db: %v", label, err)
	}
}

// -----------------------------------------------------------------------------
// per-harness flows
// -----------------------------------------------------------------------------

func runClaudeCodeE2E(t *testing.T, bin string) {
	body := readSnippet(t, "claude-code/settings.snippet.json")
	hooks := parseClaudeCodeSnippet(t, body)

	repo := tempRepo(t)
	binDir := filepath.Dir(bin)
	sessionID := "e2e-claude-code"
	stdin := fmt.Sprintf(`{"session_id":"%s"}`, sessionID)

	// Fake claude-code env: CLAUDE_PROJECT_DIR points at the repo so the
	// snippet's ${CLAUDE_PROJECT_DIR:-$PWD} expansion picks it up.
	env := adapterEnv(t, binDir, "CLAUDE_PROJECT_DIR="+repo)
	env = addFailingJQ(t, env)

	startHook := pickHookByEvent(t, hooks, "SessionStart")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	res := runBash(t, ctx, env, stdin, startHook.Command)
	if res.ExitCode != 0 {
		t.Fatalf("claude-code SessionStart exit=%d\nstdout=%s\nstderr=%s",
			res.ExitCode, res.Stdout, res.Stderr)
	}

	waitFor(t, "claude-code daemon mode==running", 10*time.Second, func() bool {
		return readDaemonStateMode(repo) == "running"
	})
	assertClientRow(t, repo, sessionID, "claude-code", 5*time.Second)

	// Exercise PreToolUse so we know `acd wake` works through the same
	// JSON-piped path the snippet expects in production.
	wakeHook := pickHookByEvent(t, hooks, "PreToolUse")
	wakeRes := runBash(t, ctx, env, stdin, wakeHook.Command)
	if wakeRes.ExitCode != 0 {
		t.Fatalf("claude-code PreToolUse exit=%d\nstdout=%s\nstderr=%s",
			wakeRes.ExitCode, wakeRes.Stdout, wakeRes.Stderr)
	}
	assertActiveHookSelfHeals(t, "claude-code", ctx, env, repo, sessionID, "claude-code", wakeHook, stdin)

	// Cover P2-7: tear the daemon down via `acd stop --all --force` (not
	// per-session deregistration) and prove the active hook still self-heals.
	assertActiveHookSelfHealsAfterStopAll(t, "claude-code", ctx, env, repo, sessionID, "claude-code", wakeHook, stdin)

	// Negative-path: corrupt state.db so `acd start` fails. The new
	// templates chain semantics must surface the failure as a nonzero hook
	// exit AND write the "active hook failed" line to the harness log.
	negStop := runBash(t, ctx, env, "",
		"acd stop --session-id "+shellQuote(sessionID)+
			" --repo "+shellQuote(repo)+" --force >/dev/null 2>&1")
	if negStop.ExitCode != 0 {
		t.Fatalf("claude-code negative-path pre-stop exit=%d\nstdout=%s\nstderr=%s",
			negStop.ExitCode, negStop.Stdout, negStop.Stderr)
	}
	waitDaemonStoppedOrKill(t, "claude-code daemon stopped before negative-path", repo)
	assertActiveHookFailsOnCorruptDB(t, "claude-code", ctx, env, repo, "claude-code", wakeHook, stdin)
	// Re-arm the daemon so the SessionEnd path below operates on a clean,
	// running daemon (avoids a flaky stop on a never-started daemon).
	if rearm := runBash(t, ctx, env, stdin, startHook.Command); rearm.ExitCode != 0 {
		t.Fatalf("claude-code re-arm after negative-path exit=%d\nstdout=%s\nstderr=%s",
			rearm.ExitCode, rearm.Stdout, rearm.Stderr)
	}
	waitFor(t, "claude-code daemon mode==running after re-arm", 10*time.Second, func() bool {
		return readDaemonStateMode(repo) == "running"
	})
	assertClientRow(t, repo, sessionID, "claude-code", 5*time.Second)

	// Stop hook (d1 rewire): now calls `acd flush --logical` rather than the
	// legacy `acd touch`. Exit must be 0 and the daemon must remain alive
	// (Stop fires when Claude finishes a turn — the agent is not exiting,
	// only pausing). The flush request itself drives the bypass-min-pending
	// commit; we cover the commit-within-2s timing in the dedicated
	// flush_logical integration test.
	turnStopHook := pickHookByEvent(t, hooks, "Stop")
	if turnStopHook.Command == "" {
		t.Fatalf("claude-code snippet missing Stop hook entry")
	}
	if !strings.Contains(turnStopHook.Command, "acd flush --logical") {
		t.Fatalf("claude-code Stop hook must call `acd flush --logical`, got: %s", turnStopHook.Command)
	}
	if strings.Contains(turnStopHook.Command, "acd touch") {
		t.Fatalf("claude-code Stop hook still calls legacy `acd touch`; rewire incomplete: %s", turnStopHook.Command)
	}
	if turnRes := runBash(t, ctx, env, stdin, turnStopHook.Command); turnRes.ExitCode != 0 {
		t.Fatalf("claude-code Stop (turn end) exit=%d\nstdout=%s\nstderr=%s",
			turnRes.ExitCode, turnRes.Stdout, turnRes.Stderr)
	}
	if mode := readDaemonStateMode(repo); mode != "running" {
		t.Fatalf("claude-code daemon mode after Stop=%q; want running (Stop must flush, not stop)", mode)
	}

	// SessionEnd → acd stop. The daemon should shut down because this is
	// the only registered session.
	stopHook := pickHookByEvent(t, hooks, "SessionEnd")
	stopRes := runBash(t, ctx, env, stdin, stopHook.Command)
	if stopRes.ExitCode != 0 {
		t.Fatalf("claude-code SessionEnd exit=%d\nstdout=%s\nstderr=%s",
			stopRes.ExitCode, stopRes.Stdout, stopRes.Stderr)
	}
	waitDaemonStoppedOrKill(t, "claude-code daemon stopped", repo)
}

func runCodexE2E(t *testing.T, bin string) {
	body := readSnippet(t, "codex/hooks.json")
	hooks := parseCodexHooksJSON(t, body)

	repo := tempRepo(t)
	binDir := filepath.Dir(bin)
	sessionID := "e2e-codex"
	// Codex hooks v2: cwd comes from stdin, not CODEX_PROJECT_DIR.
	stdin := fmt.Sprintf(`{"session_id":"%s","cwd":"%s"}`, sessionID, repo)

	// Run the bash subprocess outside the repo so the snippet must source the
	// repo path from the stdin cwd field rather than $PWD.
	env := adapterEnv(t, binDir)

	startHook := pickHookByEvent(t, hooks, "SessionStart")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	res := runBash(t, ctx, env, stdin, startHook.Command)
	if res.ExitCode != 0 {
		t.Fatalf("codex SessionStart exit=%d\nstdout=%s\nstderr=%s",
			res.ExitCode, res.Stdout, res.Stderr)
	}
	waitFor(t, "codex daemon mode==running", 10*time.Second, func() bool {
		return readDaemonStateMode(repo) == "running"
	})
	assertClientRow(t, repo, sessionID, "codex", 5*time.Second)

	// Missing cwd falls back to the hook process working directory rather than
	// silently skipping the hook.
	fallbackRepo := tempRepo(t)
	fallbackSessionID := "e2e-codex-pwd-fallback"
	fallbackStdin := fmt.Sprintf(`{"session_id":"%s"}`, fallbackSessionID)
	fallbackCommand := "cd " + shellQuote(fallbackRepo) + " && " + startHook.Command
	fallbackRes := runBash(t, ctx, env, fallbackStdin, fallbackCommand)
	if fallbackRes.ExitCode != 0 {
		t.Fatalf("codex SessionStart without cwd exit=%d\nstdout=%s\nstderr=%s",
			fallbackRes.ExitCode, fallbackRes.Stdout, fallbackRes.Stderr)
	}
	waitFor(t, "codex daemon mode==running with pwd fallback", 10*time.Second, func() bool {
		return readDaemonStateMode(fallbackRepo) == "running"
	})
	assertClientRow(t, fallbackRepo, fallbackSessionID, "codex", 5*time.Second)
	fallbackStop := runBash(t, ctx, env, "",
		"acd stop --session-id "+shellQuote(fallbackSessionID)+
			" --repo "+shellQuote(fallbackRepo)+" --force >/dev/null 2>&1")
	if fallbackStop.ExitCode != 0 {
		t.Fatalf("codex fallback stop exit=%d\nstdout=%s\nstderr=%s",
			fallbackStop.ExitCode, fallbackStop.Stdout, fallbackStop.Stderr)
	}
	waitDaemonStoppedOrKill(t, "codex pwd-fallback daemon stopped", fallbackRepo)

	// UserPromptSubmit -> acd wake.
	upHook := pickHookByEvent(t, hooks, "UserPromptSubmit")
	if upRes := runBash(t, ctx, env, stdin, upHook.Command); upRes.ExitCode != 0 {
		t.Fatalf("codex UserPromptSubmit exit=%d\nstdout=%s\nstderr=%s",
			upRes.ExitCode, upRes.Stdout, upRes.Stderr)
	}

	assertActiveHookSelfHeals(t, "codex", ctx, env, repo, sessionID, "codex", upHook, stdin)

	// Cover P2-7: stop --all teardown then re-fire the active hook.
	assertActiveHookSelfHealsAfterStopAll(t, "codex", ctx, env, repo, sessionID, "codex", upHook, stdin)

	// Negative-path: corrupt state.db so `acd start` fails. Active hook
	// must exit nonzero AND log "active hook failed" to the codex hook log.
	negStop := runBash(t, ctx, env, "",
		"acd stop --session-id "+shellQuote(sessionID)+
			" --repo "+shellQuote(repo)+" --force >/dev/null 2>&1")
	if negStop.ExitCode != 0 {
		t.Fatalf("codex negative-path pre-stop exit=%d\nstdout=%s\nstderr=%s",
			negStop.ExitCode, negStop.Stdout, negStop.Stderr)
	}
	waitDaemonStoppedOrKill(t, "codex daemon stopped before negative-path", repo)
	assertActiveHookFailsOnCorruptDB(t, "codex", ctx, env, repo, "codex", upHook, stdin)
	// Re-arm the daemon so the Stop/teardown logic below works on a clean
	// running daemon.
	if rearm := runBash(t, ctx, env, stdin, startHook.Command); rearm.ExitCode != 0 {
		t.Fatalf("codex re-arm after negative-path exit=%d\nstdout=%s\nstderr=%s",
			rearm.ExitCode, rearm.Stdout, rearm.Stderr)
	}
	waitFor(t, "codex daemon mode==running after re-arm", 10*time.Second, func() bool {
		return readDaemonStateMode(repo) == "running"
	})
	assertClientRow(t, repo, sessionID, "codex", 5*time.Second)

	// PreToolUse -> acd wake (matcher path).
	preHook := pickHookByEvent(t, hooks, "PreToolUse")
	if preRes := runBash(t, ctx, env, stdin, preHook.Command); preRes.ExitCode != 0 {
		t.Fatalf("codex PreToolUse exit=%d\nstdout=%s\nstderr=%s",
			preRes.ExitCode, preRes.Stdout, preRes.Stderr)
	}

	// Stop -> acd touch. Daemon must remain alive (mirrors claude-code Stop)
	// because PostToolUse replay can still be draining when Stop fires.
	stopHook := pickHookByEvent(t, hooks, "Stop")
	if stopRes := runBash(t, ctx, env, stdin, stopHook.Command); stopRes.ExitCode != 0 {
		t.Fatalf("codex Stop exit=%d\nstdout=%s\nstderr=%s",
			stopRes.ExitCode, stopRes.Stdout, stopRes.Stderr)
	}
	if mode := readDaemonStateMode(repo); mode != "running" {
		t.Fatalf("codex daemon mode after Stop=%q, want running (Stop must touch, not stop)", mode)
	}

	// Production cleanup relies on watch_pid death + refcount sweep; in the
	// test we drive shutdown explicitly with `acd stop --force`.
	tearDown := runBash(t, ctx, env, "",
		"acd stop --session-id "+shellQuote(sessionID)+
			" --repo "+shellQuote(repo)+" --force >/dev/null 2>&1")
	if tearDown.ExitCode != 0 {
		t.Fatalf("codex stop exit=%d\nstdout=%s\nstderr=%s",
			tearDown.ExitCode, tearDown.Stdout, tearDown.Stderr)
	}
	waitDaemonStoppedOrKill(t, "codex daemon stopped", repo)
}

func runCursorE2E(t *testing.T, bin string) {
	body := readSnippet(t, "cursor/hooks.json")
	hooks := parseCursorHooksJSON(t, body)

	repo := tempRepo(t)
	binDir := filepath.Dir(bin)
	sessionID := "e2e-cursor"
	stdin := fmt.Sprintf(`{"conversation_id":%q,"workspace_roots":[%q]}`,
		sessionID, repo)

	env := adapterEnv(t, binDir)
	home := homeFromEnv(env)
	if home == "" {
		t.Fatal("cursor e2e: HOME missing from env")
	}
	installCursorHooks(t, home)

	startHook := pickHookByEvent(t, hooks, "sessionStart")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	res := runCursorHook(t, ctx, env, home, stdin, startHook.Command)
	if res.ExitCode != 0 {
		t.Fatalf("cursor sessionStart exit=%d\nstdout=%s\nstderr=%s",
			res.ExitCode, res.Stdout, res.Stderr)
	}
	waitFor(t, "cursor daemon mode==running", 10*time.Second, func() bool {
		return readDaemonStateMode(repo) == "running"
	})
	assertClientRow(t, repo, sessionID, "cursor", 5*time.Second)

	wakeHook := pickHookByEvent(t, hooks, "postToolUse")
	if wakeRes := runCursorHook(t, ctx, env, home, stdin, wakeHook.Command); wakeRes.ExitCode != 0 {
		t.Fatalf("cursor postToolUse exit=%d\nstdout=%s\nstderr=%s",
			wakeRes.ExitCode, wakeRes.Stdout, wakeRes.Stderr)
	}
	assertActiveHookSelfHealsCursor(t, "cursor", ctx, env, home, repo, sessionID, wakeHook, stdin)

	flushHook := pickHookByEvent(t, hooks, "stop")
	if !strings.Contains(flushHook.Command, "acd-lifecycle.sh flush") {
		t.Fatalf("cursor stop hook must call acd-lifecycle.sh flush, got: %s", flushHook.Command)
	}
	if flushRes := runCursorHook(t, ctx, env, home, stdin, flushHook.Command); flushRes.ExitCode != 0 {
		t.Fatalf("cursor stop (flush) exit=%d\nstdout=%s\nstderr=%s",
			flushRes.ExitCode, flushRes.Stdout, flushRes.Stderr)
	}
	if mode := readDaemonStateMode(repo); mode != "running" {
		t.Fatalf("cursor daemon mode after stop=%q; want running (stop must flush, not stop daemon)", mode)
	}

	endHook := pickHookByEvent(t, hooks, "sessionEnd")
	if endRes := runCursorHook(t, ctx, env, home, stdin, endHook.Command); endRes.ExitCode != 0 {
		t.Fatalf("cursor sessionEnd exit=%d\nstdout=%s\nstderr=%s",
			endRes.ExitCode, endRes.Stdout, endRes.Stderr)
	}
	waitDaemonStoppedOrKill(t, "cursor daemon stopped", repo)
}

// assertActiveHookSelfHealsCursor mirrors assertActiveHookSelfHeals but runs
// hook bodies from $HOME/.cursor like Cursor does in production.
func assertActiveHookSelfHealsCursor(t *testing.T, label string, ctx context.Context, env []string, home, repo, sessionID string, hook hookSpec, stdin string) {
	t.Helper()
	selfHealStop := runBash(t, ctx, env, "",
		"acd stop --session-id "+shellQuote(sessionID)+
			" --repo "+shellQuote(repo)+" --force >/dev/null 2>&1")
	if selfHealStop.ExitCode != 0 {
		t.Fatalf("%s self-heal pre-stop exit=%d\nstdout=%s\nstderr=%s",
			label, selfHealStop.ExitCode, selfHealStop.Stdout, selfHealStop.Stderr)
	}
	waitDaemonStoppedOrKill(t, label+" daemon stopped before self-heal", repo)
	if healRes := runCursorHook(t, ctx, env, home, stdin, hook.Command); healRes.ExitCode != 0 {
		t.Fatalf("%s active-hook self-heal exit=%d\nstdout=%s\nstderr=%s",
			label, healRes.ExitCode, healRes.Stdout, healRes.Stderr)
	}
	waitFor(t, label+" daemon mode==running after self-heal", 10*time.Second, func() bool {
		return readDaemonStateMode(repo) == "running"
	})
	assertClientRow(t, repo, sessionID, "cursor", 5*time.Second)
}

// runCodexLegacyTOMLAutoDetect ensures `acd setup` with no harness arg still
// resolves to codex when only the legacy `~/.codex/config.toml` carries the
// acd marker (hooks.json absent). Codex hooks v2 added hooks.json discovery
// but must keep the legacy TOML install detectable.
func runCodexLegacyTOMLAutoDetect(t *testing.T, bin string) {
	binDir := filepath.Dir(bin)
	base := withIsolatedHome(t)
	home := ""
	for _, kv := range base {
		if strings.HasPrefix(kv, "HOME=") {
			home = strings.TrimPrefix(kv, "HOME=")
			break
		}
	}
	if home == "" {
		t.Fatal("isolated HOME missing from env")
	}
	if err := os.MkdirAll(filepath.Join(home, ".codex"), 0o700); err != nil {
		t.Fatalf("mkdir codex: %v", err)
	}
	if err := os.WriteFile(filepath.Join(home, ".codex", "config.toml"),
		[]byte("# acd-managed: true\n[features]\n"), 0o600); err != nil {
		t.Fatalf("write legacy config.toml: %v", err)
	}

	env := envWith(base,
		"PATH="+binDir+string(os.PathListSeparator)+"/bin"+string(os.PathListSeparator)+"/usr/bin",
	)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	res := runAcd(t, ctx, env, "setup")
	if res.ExitCode != 0 {
		t.Fatalf("acd setup auto-detect with legacy codex TOML exit=%d\nstdout=%s\nstderr=%s",
			res.ExitCode, res.Stdout, res.Stderr)
	}
	if !strings.Contains(res.Stdout, "acd setup codex") {
		t.Fatalf("auto-detect should pick codex; output:\n%s", res.Stdout)
	}
}

func runCodexMissingAcdWritesHookLog(t *testing.T) {
	body := readSnippet(t, "codex/hooks.json")
	hooks := parseCodexHooksJSON(t, body)
	startHook := pickHookByEvent(t, hooks, "SessionStart")

	fakeBin := t.TempDir()

	base := withIsolatedHome(t)
	home := ""
	for _, kv := range base {
		if strings.HasPrefix(kv, "HOME=") {
			home = strings.TrimPrefix(kv, "HOME=")
			break
		}
	}
	if home == "" {
		t.Fatal("isolated HOME missing from env")
	}

	env := envWith(base,
		"PATH="+fakeBin+string(os.PathListSeparator)+"/bin"+string(os.PathListSeparator)+"/usr/bin",
	)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	stdin := fmt.Sprintf(`{"session_id":"e2e-codex-missing-acd","cwd":"%s"}`, t.TempDir())
	// Codex hooks v2: hook bodies use `|| exit 0` after acd hook-stdin-extract
	// fails so missing/broken acd never blocks the user. The hook log still
	// captures the stderr from the failed extract attempt.
	res := runBash(t, ctx, env, stdin, startHook.Command)
	if res.ExitCode != 0 {
		t.Fatalf("codex SessionStart without acd should still exit 0, got=%d\nstdout=%s\nstderr=%s",
			res.ExitCode, res.Stdout, res.Stderr)
	}

	logPath := filepath.Join(home, ".local", "state", "acd", "codex-hook.log")
	logBody, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatalf("read codex hook log %s: %v", logPath, err)
	}
	// Per the new codex bash body (P1-8 regression), hook-stdin-extract
	// failure must surface as a printf line tagged
	// `active hook failed exit=<rc> cmd=acd-hook-stdin-extract`. The
	// previous shape silently swallowed the helper exit code and only the
	// "acd: command not found" stderr ended up in the log.
	for _, want := range []string{"active hook failed exit=", "cmd=acd-hook-stdin-extract"} {
		if !strings.Contains(string(logBody), want) {
			t.Fatalf("codex hook log missing %q, got:\n%s", want, logBody)
		}
	}
}

// TestAdapterE2E_Codex_HelperMissing covers the P1-8 regression target: when
// `acd` is on PATH but exits non-zero (here `/usr/bin/false` masquerades as
// the binary), the codex hook bodies must
//   - return exit 0 to the harness so the user is not blocked,
//   - log an explicit printf line tagged `cmd=acd-hook-stdin-extract` so the
//     failure cause is visible in `acd doctor` output and the harness log.
//
// Distinct from `runCodexMissingAcdWritesHookLog` which removes acd from
// PATH entirely (testing exit=127 / "command not found"). This case tests
// the more subtle real-world failure where acd exists but is broken.
func TestAdapterE2E_Codex_HelperMissing(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("adapter e2e: Windows snippets not in scope for v1")
	}
	if _, err := exec.LookPath("bash"); err != nil {
		t.Skip("adapter e2e: bash not on PATH")
	}
	falseBin := "/usr/bin/false"
	if _, err := os.Stat(falseBin); err != nil {
		t.Skipf("skip: %s missing", falseBin)
	}

	body := readSnippet(t, "codex/hooks.json")
	hooks := parseCodexHooksJSON(t, body)

	// Build a directory where `acd` is a symlink to /usr/bin/false. This
	// shadows any installed acd on PATH so every invocation exits 1.
	fakeBin := t.TempDir()
	if err := os.Symlink(falseBin, filepath.Join(fakeBin, "acd")); err != nil {
		t.Fatalf("symlink acd -> %s: %v", falseBin, err)
	}

	base := withIsolatedHome(t)
	home := ""
	for _, kv := range base {
		if strings.HasPrefix(kv, "HOME=") {
			home = strings.TrimPrefix(kv, "HOME=")
			break
		}
	}
	if home == "" {
		t.Fatal("isolated HOME missing from env")
	}

	env := envWith(base,
		"PATH="+fakeBin+string(os.PathListSeparator)+"/bin"+string(os.PathListSeparator)+"/usr/bin",
	)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	logPath := filepath.Join(home, ".local", "state", "acd", "codex-hook.log")

	// Run every event the codex template registers; each must exit 0
	// (helper failure is soft-failed) and append a tagged printf line.
	for _, event := range []string{"SessionStart", "UserPromptSubmit", "PreToolUse", "PostToolUse", "Stop"} {
		// Truncate the log between events so each assertion observes only
		// the printf line written by this event.
		_ = os.MkdirAll(filepath.Dir(logPath), 0o755)
		_ = os.WriteFile(logPath, nil, 0o644)

		hook := pickHookByEvent(t, hooks, event)
		stdin := fmt.Sprintf(`{"session_id":"e2e-codex-helper-missing-%s","cwd":"%s"}`, event, t.TempDir())
		res := runBash(t, ctx, env, stdin, hook.Command)
		if res.ExitCode != 0 {
			t.Fatalf("codex %s with broken acd should exit 0, got=%d\nstdout=%s\nstderr=%s",
				event, res.ExitCode, res.Stdout, res.Stderr)
		}
		logBody, err := os.ReadFile(logPath)
		if err != nil {
			t.Fatalf("%s: read codex hook log %s: %v", event, logPath, err)
		}
		// `/usr/bin/false` exits 1, so the hook-stdin-extract printf branch
		// fires. The exit code printed must be the real exit (1), not 0.
		for _, want := range []string{
			"active hook failed exit=",
			"cmd=acd-hook-stdin-extract",
		} {
			if !strings.Contains(string(logBody), want) {
				t.Fatalf("%s: codex hook log missing %q, got:\n%s", event, want, logBody)
			}
		}
		// Guard against the rc=$? regression: the printed exit must NOT be
		// 0 because /usr/bin/false exits 1. If a future template edit drops
		// the rc=$? capture, $(date +...) would clobber $? to 0.
		if strings.Contains(string(logBody), "active hook failed exit=0 cmd=acd-hook-stdin-extract") {
			t.Fatalf("%s: codex hook log printed exit=0 — rc=$? capture lost (P1-8 regression):\n%s", event, logBody)
		}
	}
}

func runOpencodeE2E(t *testing.T, bin string) {
	body := readSnippet(t, "opencode/hooks.snippet.yaml")
	hooks := parseYAMLBashBlocks(t, body)

	repo := tempRepo(t)
	binDir := filepath.Dir(bin)
	sessionID := "e2e-opencode"
	env := adapterEnv(t, binDir,
		"OPENCODE_SESSION_ID="+sessionID,
		"OPENCODE_PROJECT_DIR="+repo,
	)

	startHook := pickHookByEvent(t, hooks, "acd-start")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	res := runBash(t, ctx, env, "", startHook.Command)
	if res.ExitCode != 0 {
		t.Fatalf("opencode acd-start exit=%d\nstdout=%s\nstderr=%s",
			res.ExitCode, res.Stdout, res.Stderr)
	}
	waitFor(t, "opencode daemon mode==running", 10*time.Second, func() bool {
		return readDaemonStateMode(repo) == "running"
	})
	assertClientRow(t, repo, sessionID, "opencode", 5*time.Second)

	wakeHook := pickHookByEvent(t, hooks, "acd-wake-tool-before")
	wakeRes := runBash(t, ctx, env, "", wakeHook.Command)
	if wakeRes.ExitCode != 0 {
		t.Fatalf("opencode acd-wake-tool-before exit=%d\nstdout=%s\nstderr=%s",
			wakeRes.ExitCode, wakeRes.Stdout, wakeRes.Stderr)
	}
	assertActiveHookSelfHeals(t, "opencode", ctx, env, repo, sessionID, "opencode", wakeHook, "")

	// Idle hook (d1 rewire): now calls `acd flush --logical` rather than
	// the legacy `acd touch`. Daemon must remain alive — session.idle does
	// not end the session.
	idleHook := pickHookByEvent(t, hooks, "acd-flush-idle")
	if idleHook.Command == "" {
		t.Fatalf("opencode snippet missing acd-flush-idle entry (legacy acd-touch-idle id no longer recognised)")
	}
	if !strings.Contains(idleHook.Command, "acd flush --logical") {
		t.Fatalf("opencode acd-flush-idle must call `acd flush --logical`, got: %s", idleHook.Command)
	}
	if idleRes := runBash(t, ctx, env, "", idleHook.Command); idleRes.ExitCode != 0 {
		t.Fatalf("opencode acd-flush-idle exit=%d\nstdout=%s\nstderr=%s",
			idleRes.ExitCode, idleRes.Stdout, idleRes.Stderr)
	}
	if mode := readDaemonStateMode(repo); mode != "running" {
		t.Fatalf("opencode daemon mode after idle=%q; want running", mode)
	}

	stopHook := pickHookByEvent(t, hooks, "acd-stop")
	stopRes := runBash(t, ctx, env, "", stopHook.Command)
	if stopRes.ExitCode != 0 {
		t.Fatalf("opencode acd-stop exit=%d\nstdout=%s\nstderr=%s",
			stopRes.ExitCode, stopRes.Stdout, stopRes.Stderr)
	}
	waitDaemonStoppedOrKill(t, "opencode daemon stopped", repo)
}

func runPiE2E(t *testing.T, bin string) {
	body := readSnippet(t, "pi/hooks.snippet.yaml")
	hooks := parseYAMLBashBlocks(t, body)

	repo := tempRepo(t)
	binDir := filepath.Dir(bin)
	sessionID := "e2e-pi"
	env := adapterEnv(t, binDir,
		"PI_SESSION_ID="+sessionID,
		"PI_PROJECT_DIR="+repo,
	)

	startHook := pickHookByEvent(t, hooks, "acd-start")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	res := runBash(t, ctx, env, "", startHook.Command)
	if res.ExitCode != 0 {
		t.Fatalf("pi acd-start exit=%d\nstdout=%s\nstderr=%s",
			res.ExitCode, res.Stdout, res.Stderr)
	}
	waitFor(t, "pi daemon mode==running", 10*time.Second, func() bool {
		return readDaemonStateMode(repo) == "running"
	})
	assertClientRow(t, repo, sessionID, "pi", 5*time.Second)

	wakeHook := pickHookByEvent(t, hooks, "acd-wake-tool-before")
	wakeRes := runBash(t, ctx, env, "", wakeHook.Command)
	if wakeRes.ExitCode != 0 {
		t.Fatalf("pi acd-wake-tool-before exit=%d\nstdout=%s\nstderr=%s",
			wakeRes.ExitCode, wakeRes.Stdout, wakeRes.Stderr)
	}
	assertActiveHookSelfHeals(t, "pi", ctx, env, repo, sessionID, "pi", wakeHook, "")

	// Idle hook (d1 rewire): now calls `acd flush --logical` rather than
	// the legacy `acd touch`. Daemon must remain alive after idle.
	idleHook := pickHookByEvent(t, hooks, "acd-flush-idle")
	if idleHook.Command == "" {
		t.Fatalf("pi snippet missing acd-flush-idle entry (legacy acd-touch-idle id no longer recognised)")
	}
	if !strings.Contains(idleHook.Command, "acd flush --logical") {
		t.Fatalf("pi acd-flush-idle must call `acd flush --logical`, got: %s", idleHook.Command)
	}
	if idleRes := runBash(t, ctx, env, "", idleHook.Command); idleRes.ExitCode != 0 {
		t.Fatalf("pi acd-flush-idle exit=%d\nstdout=%s\nstderr=%s",
			idleRes.ExitCode, idleRes.Stdout, idleRes.Stderr)
	}
	if mode := readDaemonStateMode(repo); mode != "running" {
		t.Fatalf("pi daemon mode after idle=%q; want running", mode)
	}

	stopHook := pickHookByEvent(t, hooks, "acd-stop")
	stopRes := runBash(t, ctx, env, "", stopHook.Command)
	if stopRes.ExitCode != 0 {
		t.Fatalf("pi acd-stop exit=%d\nstdout=%s\nstderr=%s",
			stopRes.ExitCode, stopRes.Stdout, stopRes.Stderr)
	}
	waitDaemonStoppedOrKill(t, "pi daemon stopped", repo)
}

func runShellE2E(t *testing.T, bin string) {
	// The direnv envrc snippet generates `SID=$(uuidgen)` and exports
	// ACD_SESSION_ID. To make the assertion deterministic without rewriting
	// the snippet, we shadow `uuidgen` with a stub on PATH that prints a
	// known UUID, then read $ACD_SESSION_ID back out via a marker file.
	body := readSnippet(t, "shell/direnv.envrc.snippet")
	if !strings.Contains(body, "acd start") {
		t.Skip("shell direnv snippet has no `acd start` invocation")
	}

	repo := tempRepo(t)
	binDir := filepath.Dir(bin)
	sessionID := "e2e-shell"

	// Stage a fake `uuidgen` on a private bin dir so the snippet's
	// `$(uuidgen)` returns our predictable session id.
	fakeBin := t.TempDir()
	stub := filepath.Join(fakeBin, "uuidgen")
	writeFile(t, stub, "#!/usr/bin/env bash\necho '"+sessionID+"'\n")
	if err := os.Chmod(stub, 0o755); err != nil {
		t.Fatalf("chmod uuidgen stub: %v", err)
	}

	// Prepend fakeBin and binDir to PATH (fakeBin first so its uuidgen
	// wins over any system uuidgen).
	env := withIsolatedHome(t)
	for i, kv := range env {
		if strings.HasPrefix(kv, "PATH=") {
			env[i] = "PATH=" + fakeBin + string(os.PathListSeparator) +
				binDir + string(os.PathListSeparator) +
				strings.TrimPrefix(kv, "PATH=")
			break
		}
	}

	// Run the snippet body inside the repo. The snippet defines a function
	// + sets a trap; we need to keep the trap from killing our daemon when
	// the bash subshell exits, so we replace the EXIT trap with a no-op
	// after the snippet body executes. The simplest path: copy the snippet
	// and strip the `trap` line.
	stripped := stripTrapLines(body)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cmd := "set -e; cd " + shellQuote(repo) + "; " + stripped
	res := runBash(t, ctx, env, "", cmd)
	if res.ExitCode != 0 {
		t.Fatalf("shell direnv snippet exit=%d\nstdout=%s\nstderr=%s",
			res.ExitCode, res.Stdout, res.Stderr)
	}

	waitFor(t, "shell daemon mode==running", 10*time.Second, func() bool {
		return readDaemonStateMode(repo) == "running"
	})
	assertClientRow(t, repo, sessionID, "shell", 5*time.Second)

	// The shell snippet's stop path runs from a direnv unload trap which
	// we cannot reliably simulate here. Force-stop via `acd stop` so the
	// daemon shuts down before the test exits.
	shutdownDaemon(t, env, repo, sessionID)
}

// stripTrapLines removes any `trap ... EXIT` line from a bash snippet so the
// test subshell's exit does not propagate `acd stop` and pre-empt our
// assertions. The direnv snippet's stop path is verified separately in the
// shutdownDaemon cleanup at the end of the subtest.
func stripTrapLines(body string) string {
	var keep []string
	for _, line := range strings.Split(body, "\n") {
		t := strings.TrimSpace(line)
		if strings.HasPrefix(t, "trap ") {
			continue
		}
		keep = append(keep, line)
	}
	return strings.Join(keep, "\n")
}

// shellQuote wraps a string in single quotes, escaping any embedded single
// quotes for bash -c consumption.
func shellQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
}
