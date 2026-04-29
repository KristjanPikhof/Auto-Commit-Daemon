//go:build integration
// +build integration

package integration_test

// adapter_e2e_test.go — §7.9 / §9 end-to-end coverage. Each subtest renders a
// harness's snippet via `acd init <harness>`, executes the start-equivalent
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
	for _, h := range []string{"claude-code", "codex", "opencode", "pi", "shell"} {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		out := runAcd(t, ctx, os.Environ(), "init", h)
		cancel()
		if out.ExitCode != 0 {
			t.Fatalf("acd init %s exit=%d\nstdout=%s\nstderr=%s",
				h, out.ExitCode, out.Stdout, out.Stderr)
		}
		if len(strings.TrimSpace(out.Stdout)) == 0 {
			t.Fatalf("acd init %s emitted empty stdout", h)
		}
	}

	t.Run("claude-code", func(t *testing.T) {
		runClaudeCodeE2E(t, bin)
	})
	t.Run("codex", func(t *testing.T) {
		runCodexE2E(t, bin)
		runCodexMissingAcdWritesHookLog(t)
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
// and returns one hookSpec per event/command pair. The matcher field is not
// used here — every command is exercised through the same fake stdin payload.
func parseClaudeCodeSnippet(t *testing.T, body string) []hookSpec {
	t.Helper()
	var doc struct {
		Hooks map[string][]struct {
			Matcher string `json:"matcher"`
			Command string `json:"command"`
		} `json:"hooks"`
	}
	if err := json.Unmarshal([]byte(body), &doc); err != nil {
		t.Fatalf("claude-code snippet not valid JSON: %v\n%s", err, body)
	}
	var out []hookSpec
	for _, event := range []string{"SessionStart", "PreToolUse", "PostToolUse", "Stop", "SessionEnd"} {
		for _, e := range doc.Hooks[event] {
			out = append(out, hookSpec{Event: event, Command: e.Command})
		}
	}
	if len(out) == 0 {
		t.Fatalf("claude-code snippet has no hooks: %s", body)
	}
	return out
}

// parseCodexSnippet walks the codex TOML snippet (avoids a TOML dependency).
// Codex schema: [[hooks.<EventName>]] is a matcher group, and the runnable
// handler lives in [[hooks.<EventName>.hooks]] with `type` and `command`. The
// outer [[hooks.X]] establishes Event; the inner [[hooks.X.hooks]] block
// supplies the `command` we exec.
func parseCodexSnippet(t *testing.T, body string) []hookSpec {
	t.Helper()
	lines := strings.Split(body, "\n")
	var hooks []hookSpec
	var curEvent string
	var cur hookSpec
	flush := func() {
		if cur.Command != "" {
			hooks = append(hooks, cur)
		}
		cur = hookSpec{}
	}
	for _, raw := range lines {
		line := strings.TrimSpace(raw)
		switch {
		case strings.HasPrefix(line, "[[hooks.") && strings.HasSuffix(line, ".hooks]]"):
			// Inner handler block; closes any in-flight handler under the same event.
			flush()
			cur.Event = curEvent
		case strings.HasPrefix(line, "[[hooks.") && strings.HasSuffix(line, "]]"):
			// Outer matcher block: [[hooks.SessionStart]] etc. Capture event name.
			flush()
			inner := strings.TrimSuffix(strings.TrimPrefix(line, "[[hooks."), "]]")
			curEvent = inner
		case strings.HasPrefix(line, "command"):
			cur.Command = stripTOMLValue(line)
		}
	}
	flush()
	if len(hooks) == 0 {
		t.Fatalf("codex snippet contained no hook handlers:\n%s", body)
	}
	return hooks
}

// stripTOMLValue extracts the quoted value of `key = "value"`.
func stripTOMLValue(line string) string {
	if i := strings.Index(line, "="); i >= 0 {
		v := strings.TrimSpace(line[i+1:])
		if len(v) >= 2 && v[0] == '"' && v[len(v)-1] == '"' {
			// Re-interpret the TOML escape vocabulary we actually use:
			// the snippets only escape \" and \\ — Go's strconv.Unquote
			// handles both correctly when the body is a valid Go literal.
			out := v[1 : len(v)-1]
			out = strings.ReplaceAll(out, `\"`, `"`)
			out = strings.ReplaceAll(out, `\\`, `\`)
			return out
		}
		return v
	}
	return ""
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
	waitFor(t, "post-cleanup mode==stopped", 10*time.Second, func() bool {
		return daemonStopped(repo)
	})
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

	// SessionEnd → acd stop. The daemon should shut down because this is
	// the only registered session.
	stopHook := pickHookByEvent(t, hooks, "SessionEnd")
	stopRes := runBash(t, ctx, env, stdin, stopHook.Command)
	if stopRes.ExitCode != 0 {
		t.Fatalf("claude-code SessionEnd exit=%d\nstdout=%s\nstderr=%s",
			stopRes.ExitCode, stopRes.Stdout, stopRes.Stderr)
	}
	waitFor(t, "claude-code daemon mode==stopped", 10*time.Second, func() bool {
		return daemonStopped(repo)
	})
}

func runCodexE2E(t *testing.T, bin string) {
	body := readSnippet(t, "codex/config.snippet.toml")
	hooks := parseCodexSnippet(t, body)

	repo := tempRepo(t)
	binDir := filepath.Dir(bin)
	sessionID := "e2e-codex"
	stdin := fmt.Sprintf(`{"session_id":"%s"}`, sessionID)

	// Codex provides the project directory separately from the hook process
	// cwd; keep the bash subprocess outside repo to prove the snippet honors it.
	env := adapterEnv(t, binDir, "CODEX_PROJECT_DIR="+repo)
	env = addFailingJQ(t, env)

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

	// Codex template intentionally omits the Stop hook (race vs PostToolUse).
	// Production cleanup relies on watch_pid death + refcount sweep; in the
	// test we drive shutdown explicitly with `acd stop --force`.
	stopRes := runBash(t, ctx, env, "",
		"acd stop --session-id "+shellQuote(sessionID)+
			" --repo "+shellQuote(repo)+" --force >/dev/null 2>&1")
	if stopRes.ExitCode != 0 {
		t.Fatalf("codex stop exit=%d\nstdout=%s\nstderr=%s",
			stopRes.ExitCode, stopRes.Stdout, stopRes.Stderr)
	}
	waitFor(t, "codex daemon mode==stopped", 10*time.Second, func() bool {
		return daemonStopped(repo)
	})
}

func runCodexMissingAcdWritesHookLog(t *testing.T) {
	body := readSnippet(t, "codex/config.snippet.toml")
	hooks := parseCodexSnippet(t, body)
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
		"CODEX_PROJECT_DIR="+t.TempDir(),
	)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	stdin := `{"session_id":"e2e-codex-missing-acd"}`
	res := runBash(t, ctx, env, stdin, startHook.Command)
	if res.ExitCode == 0 {
		t.Fatalf("codex SessionStart without acd should fail\nstdout=%s\nstderr=%s", res.Stdout, res.Stderr)
	}
	if strings.TrimSpace(res.Stdout) != "{}" {
		t.Fatalf("codex failure path should still emit JSON stdout, got %q", res.Stdout)
	}

	logPath := filepath.Join(home, ".local", "state", "acd", "codex-hook.log")
	logBody, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatalf("read codex hook log %s: %v", logPath, err)
	}
	if !strings.Contains(string(logBody), "acd") {
		t.Fatalf("codex hook log missing acd failure, got:\n%s", logBody)
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

	stopHook := pickHookByEvent(t, hooks, "acd-stop")
	stopRes := runBash(t, ctx, env, "", stopHook.Command)
	if stopRes.ExitCode != 0 {
		t.Fatalf("opencode acd-stop exit=%d\nstdout=%s\nstderr=%s",
			stopRes.ExitCode, stopRes.Stdout, stopRes.Stderr)
	}
	waitFor(t, "opencode daemon mode==stopped", 10*time.Second, func() bool {
		return daemonStopped(repo)
	})
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

	stopHook := pickHookByEvent(t, hooks, "acd-stop")
	stopRes := runBash(t, ctx, env, "", stopHook.Command)
	if stopRes.ExitCode != 0 {
		t.Fatalf("pi acd-stop exit=%d\nstdout=%s\nstderr=%s",
			stopRes.ExitCode, stopRes.Stdout, stopRes.Stderr)
	}
	waitFor(t, "pi daemon mode==stopped", 10*time.Second, func() bool {
		return daemonStopped(repo)
	})
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
