//go:build integration

package integration_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/central"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/templates"
)

func adapterHooks(t *testing.T, harness string) []hookSpec {
	t.Helper()
	switch harness {
	case "claude-code":
		return parseClaudeCodeSnippet(t, readSnippet(t, "claude-code/settings.snippet.json"))
	case "codex":
		return parseCodexHooksJSON(t, readSnippet(t, "codex/hooks.json"))
	case "cursor":
		return parseCursorHooksJSON(t, readSnippet(t, "cursor/hooks.json"))
	case "opencode", "pi":
		return parseYAMLBashBlocks(t, readSnippet(t, harness+"/hooks.snippet.yaml"))
	case "shell":
		return []hookSpec{{Event: "direnv", Command: readSnippet(t, "shell/direnv.envrc.snippet")}, {Event: "zsh", Command: "zsh -f -c " + shellQuote(readSnippet(t, "shell/zshrc.snippet.sh")+"\nacd_wake_protection")}}
	default:
		t.Fatal("unknown harness", harness)
		return nil
	}
}

func adapterInvocation(env []string, bin, repo string) ([]string, string) {
	env = prependPath(env, filepath.Dir(bin))
	env = envWith(env, "CLAUDE_PROJECT_DIR="+repo, "OPENCODE_PROJECT_DIR="+repo, "PI_PROJECT_DIR="+repo)
	payload := fmt.Sprintf(`{"session_id":"adapter","conversation_id":"adapter","cwd":%q,"workspace_roots":[%q]}`, repo, repo)
	return env, payload
}

func TestAdapterE2E(t *testing.T) {
	for _, harness := range []string{"claude-code", "codex", "cursor", "opencode", "pi", "shell"} {
		t.Run(harness, func(t *testing.T) {
			f := newFinishFixture(t, "ACD_DISABLE_FSNOTIFY=1")
			ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
			defer cancel()
			env, payload := adapterInvocation(f.Env, f.Bin, f.Repo)
			env = addFailingJQ(t, env)
			resolved := runBash(t, ctx, env, "", "bash -c 'command -v acd'")
			if strings.TrimSpace(resolved.Stdout) != f.Bin {
				t.Fatalf("hook resolves unexpected binary: %+v want %s", resolved, f.Bin)
			}
			hookDir := f.Repo
			if harness == "cursor" {
				installCursorHooks(t, envValue(env, "HOME"))
				hookDir = filepath.Join(envValue(env, "HOME"), ".cursor")
			}
			for i, hook := range adapterHooks(t, harness) {
				path := fmt.Sprintf("hook-%d.txt", i)
				waitFor(t, "completed protection before hook", 5*time.Second, func() bool {
					return sqliteScalar(t, f.DBPath, "SELECT value FROM daemon_meta WHERE key='protection.complete'") == "true"
				})
				before, err := strconv.ParseInt(sqliteScalar(t, f.DBPath, "SELECT value FROM daemon_meta WHERE key='protection.observation_epoch'"), 10, 64)
				if err != nil {
					t.Fatal(err)
				}
				owner := readDaemonStatePID(f.Repo)
				if owner <= 0 {
					t.Fatal("enabled worker has no owner")
				}
				writeFile(t, filepath.Join(f.Repo, path), hook.Event+"\n")
				result := runBash(t, ctx, env, payload, "cd "+shellQuote(hookDir)+" && "+hook.Command)
				if result.ExitCode != 0 || result.Stdout != "" {
					t.Fatalf("%s hook: %+v", hook.Event, result)
				}
				after, err := strconv.ParseInt(sqliteScalar(t, f.DBPath, "SELECT value FROM daemon_meta WHERE key='protection.observation_epoch'"), 10, 64)
				if err != nil || after <= before {
					log, _ := os.ReadFile(filepath.Join(f.Roots.State, harness+"-hook.log"))
					t.Fatalf("hook did not report observation: %d -> %d %v; complete=%s stderr=%s log=%s command=%s", before, after, err, sqliteScalar(t, f.DBPath, "SELECT value FROM daemon_meta WHERE key='protection.complete'"), result.Stderr, log, hook.Command)
				}
				waitFor(t, "hook protects "+hook.Event, 5*time.Second, func() bool {
					ref := sqliteScalar(t, f.DBPath, "SELECT checkpoint_ref FROM checkpoints WHERE phase='completed' AND coverage_complete=1 ORDER BY seq DESC LIMIT 1")
					out, err := runGit(f.Repo, "show", ref+":"+path)
					return err == nil && out == hook.Event+"\n"
				})
				if readDaemonStatePID(f.Repo) != owner || readDaemonStateMode(f.Repo) != "running" {
					t.Fatal("hook changed worker ownership or stopped protection")
				}
			}
			if got := strings.TrimSpace(runGitOK(t, f.Repo, "rev-parse", "HEAD")); got != f.BaseHead {
				t.Fatal("hook created branch commits")
			}
			db := f.OpenDB(t)
			exists, err := state.LegacyTableExists(ctx, db.ReadSQL(), "daemon_clients")
			if err != nil || exists {
				t.Fatalf("hook created client/session state: %v %v", exists, err)
			}
			off := f.Run(t, ctx, "off", "--json")
			if off.ExitCode != 0 {
				t.Fatalf("off %+v", off)
			}
			env, payload = adapterInvocation(f.Env, f.Bin, f.Repo)
			for _, hook := range adapterHooks(t, harness) {
				result := runBash(t, ctx, env, payload, "cd "+shellQuote(hookDir)+" && "+hook.Command)
				if result.ExitCode != 0 || result.Stdout != "" {
					t.Fatalf("disabled hook %+v", result)
				}
			}
			registry, err := central.Load(f.Roots)
			if err != nil {
				t.Fatal(err)
			}
			for _, record := range registry.Repos {
				if !record.LifecycleDisabled() {
					t.Fatal("hook re-enabled disabled protection")
				}
			}
		})
	}
	runCodexMissingAcdIsConfigurationError(t)
	runCodexLegacyTOMLAutoDetect(t, buildAcdBinary(t))
}

func TestAdapterE2E_InactiveRepositoriesAreSilent(t *testing.T) {
	for _, harness := range []string{"claude-code", "codex", "cursor", "opencode", "pi", "shell"} {
		for _, gitRepo := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/git=%t", harness, gitRepo), func(t *testing.T) {
				repo := t.TempDir()
				if gitRepo {
					repo = tempRepo(t)
				}
				env, payload := adapterInvocation(withIsolatedHome(t), buildAcdBinary(t), repo)
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				for _, hook := range adapterHooks(t, harness) {
					result := runBash(t, ctx, env, payload, "cd "+shellQuote(repo)+" && "+hook.Command)
					if result.ExitCode != 0 || result.Stdout != "" {
						t.Fatalf("inactive hook %+v", result)
					}
				}
				for _, path := range []string{filepath.Join(repo, ".git", "acd"), filepath.Join(envValue(env, "XDG_STATE_HOME"), "acd", "run", "supervisor.sock")} {
					if _, err := os.Stat(path); !os.IsNotExist(err) {
						t.Fatalf("inactive hook wrote %s: %v", path, err)
					}
				}
			})
		}
	}
}

func prependPath(env []string, dir string) []string {
	return envWith(env, "PATH="+dir+string(os.PathListSeparator)+envValue(env, "PATH"))
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

// installCursorHooks copies the embedded hooks.json into isolated $HOME/.cursor.

func installCursorHooks(t *testing.T, home string) {
	t.Helper()
	cursorDir := filepath.Join(home, ".cursor")
	if err := os.MkdirAll(cursorDir, 0o700); err != nil {
		t.Fatalf("install cursor hooks: mkdir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(cursorDir, "hooks.json"),
		[]byte(readSnippet(t, "cursor/hooks.json")), 0o600); err != nil {
		t.Fatalf("install cursor hooks: write hooks.json: %v", err)
	}
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
	res := runAcdFromDir(t, ctx, env, tempRepo(t), "setup", "--dry-run", "--provider", "deterministic")
	if res.ExitCode != 0 {
		t.Fatalf("acd setup auto-detect with legacy codex TOML exit=%d\nstdout=%s\nstderr=%s",
			res.ExitCode, res.Stdout, res.Stderr)
	}
	if !strings.Contains(res.Stdout, "Setup plan: sha256:") {
		t.Fatalf("setup should render the transactional plan; output:\n%s", res.Stdout)
	}
	if strings.Contains(res.Stdout, "acd setup codex") {
		t.Fatalf("public setup must not fall back to the legacy snippet route; output:\n%s", res.Stdout)
	}
}

func runCodexMissingAcdIsConfigurationError(t *testing.T) {
	body := readSnippet(t, "codex/hooks.json")
	hooks := parseCodexHooksJSON(t, body)
	startHook := pickHookByEvent(t, hooks, "SessionStart")

	fakeBin := t.TempDir()

	base := withIsolatedHome(t)
	env := envWith(base,
		"PATH="+fakeBin+string(os.PathListSeparator)+"/bin"+string(os.PathListSeparator)+"/usr/bin",
	)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	stdin := fmt.Sprintf(`{"session_id":"e2e-codex-missing-acd","cwd":"%s"}`, t.TempDir())
	res := runBash(t, ctx, env, stdin, startHook.Command)
	if res.ExitCode == 0 {
		t.Fatalf("codex SessionStart without acd should report invalid hook configuration\nstdout=%s\nstderr=%s",
			res.Stdout, res.Stderr)
	}

	logPath := filepath.Join(envValue(base, "XDG_STATE_HOME"), "acd", "codex-hook.log")
	if _, err := os.Stat(logPath); !os.IsNotExist(err) {
		t.Fatalf("missing acd binary must not create %s: %v", logPath, err)
	}
}

// TestAdapterE2E_Codex_BrokenBinary proves command configuration errors stay
// visible. ACD runtime failures fail open inside the command, but a binary
// that cannot execute the integration protocol is not an ACD runtime result.

func TestAdapterE2E_Codex_BrokenBinary(t *testing.T) {
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

	fakeBin := t.TempDir()
	if err := os.Symlink(falseBin, filepath.Join(fakeBin, "acd")); err != nil {
		t.Fatalf("symlink acd -> %s: %v", falseBin, err)
	}

	base := withIsolatedHome(t)
	env := envWith(base,
		"PATH="+fakeBin+string(os.PathListSeparator)+"/bin"+string(os.PathListSeparator)+"/usr/bin",
	)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	logPath := filepath.Join(envValue(base, "XDG_STATE_HOME"), "acd", "codex-hook.log")
	for _, event := range []string{"SessionStart", "UserPromptSubmit", "PreToolUse", "PostToolUse", "Stop"} {
		hook := pickHookByEvent(t, hooks, event)
		stdin := fmt.Sprintf(`{"session_id":"e2e-codex-broken-%s","cwd":"%s"}`, event, t.TempDir())
		res := runBash(t, ctx, env, stdin, hook.Command)
		if res.ExitCode == 0 {
			t.Fatalf("codex %s with broken acd should report invalid hook configuration\nstdout=%s\nstderr=%s",
				event, res.Stdout, res.Stderr)
		}
	}
	if _, err := os.Stat(logPath); !os.IsNotExist(err) {
		t.Fatalf("broken acd binary must not create %s: %v", logPath, err)
	}
}

func shellQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
}
