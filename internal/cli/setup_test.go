package cli

// Tests for §7.9 — acd setup <harness>.

import (
	"bytes"
	"encoding/json"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/templates"
)

// runSetupCmd is a test helper that drives newSetupCmd() through its cobra
// RunE and captures stdout + stderr. It returns the captured output and
// any error the command returned.
func runSetupCmd(t *testing.T, args ...string) (stdout, stderr string, err error) {
	t.Helper()
	var outBuf, errBuf bytes.Buffer
	cmd := newSetupCmd()
	cmd.SetOut(&outBuf)
	cmd.SetErr(&errBuf)
	cmd.SetArgs(args)
	// cobra.Command.Execute wires the full flag/arg machinery; use RunE
	// directly via Execute so cobra can validate ExactArgs too.
	err = cmd.Execute()
	return outBuf.String(), errBuf.String(), err
}

// snippetBody reads the canonical snippet file from the embedded FS so tests
// can assert verbatim content without hard-coding the snippet here.
func snippetBody(t *testing.T, path string) string {
	t.Helper()
	b, err := fs.ReadFile(templates.FS, path)
	if err != nil {
		t.Fatalf("read embedded template %s: %v", path, err)
	}
	return string(b)
}

// --- per-harness happy-path tests ------------------------------------------

func TestSetup_ClaudeCode_ExitsZero(t *testing.T) {
	out, _, err := runSetupCmd(t, "claude-code")
	if err != nil {
		t.Fatalf("expected exit 0, got: %v\nstdout:\n%s", err, out)
	}
}

func TestSetup_ClaudeCode_ContainsSnippet(t *testing.T) {
	out, _, _ := runSetupCmd(t, "claude-code")
	want := snippetBody(t, "claude-code/settings.snippet.json")
	if !strings.Contains(out, strings.TrimSpace(want)) {
		t.Errorf("snippet body not found in output.\nwant substring:\n%s\ngot:\n%s", want, out)
	}
}

func TestSetup_ClaudeCode_ValidJSON(t *testing.T) {
	out, _, _ := runSetupCmd(t, "claude-code")
	// Extract the JSON block: everything between the first '{' and the last '}'.
	start := strings.Index(out, "{")
	end := strings.LastIndex(out, "}")
	if start == -1 || end == -1 || end <= start {
		t.Fatalf("no JSON block found in output:\n%s", out)
	}
	jsonBlock := out[start : end+1]
	var v interface{}
	if err := json.Unmarshal([]byte(jsonBlock), &v); err != nil {
		t.Fatalf("JSON parse error: %v\nblock:\n%s", err, jsonBlock)
	}
}

func TestSetup_ClaudeCode_AcdManagedMarker(t *testing.T) {
	out, _, _ := runSetupCmd(t, "claude-code")
	if !strings.Contains(out, `"_acd_managed": true`) {
		t.Errorf("acd-managed marker not found in output:\n%s", out)
	}
}

func TestSetup_ClaudeCode_FooterInstructions(t *testing.T) {
	out, _, _ := runSetupCmd(t, "claude-code")
	// README says "Merge the printed JSON into ~/.claude/settings.json"
	if !strings.Contains(out, "settings.json") {
		t.Errorf("footer instructions missing 'settings.json' in output:\n%s", out)
	}
}

// TestSetup_ClaudeCode_HasCanonicalHookSchema guards against schema drift in
// templates/claude-code/settings.snippet.json. Claude Code rejects entries
// that lack a nested "hooks" array of {type:"command", command:"…"} handlers
// — the surface symptom is "hooks: Expected array, but received undefined"
// at startup. Earlier snippets used a flat {matcher, command} shape; this
// test exists so that regression cannot ship again.
func TestSetup_ClaudeCode_HasCanonicalHookSchema(t *testing.T) {
	out, _, _ := runSetupCmd(t, "claude-code")
	start := strings.Index(out, "{")
	end := strings.LastIndex(out, "}")
	if start == -1 || end == -1 || end <= start {
		t.Fatalf("no JSON block found in output:\n%s", out)
	}
	var settings struct {
		Hooks map[string][]struct {
			Matcher *string `json:"matcher,omitempty"`
			Hooks   []struct {
				Type    string `json:"type"`
				Command string `json:"command"`
			} `json:"hooks"`
		} `json:"hooks"`
	}
	if err := json.Unmarshal([]byte(out[start:end+1]), &settings); err != nil {
		t.Fatalf("parse JSON: %v\nblock:\n%s", err, out[start:end+1])
	}
	required := []string{"SessionStart", "PreToolUse", "PostToolUse", "Stop", "SessionEnd"}
	for _, ev := range required {
		entries, ok := settings.Hooks[ev]
		if !ok || len(entries) == 0 {
			t.Errorf("event %q missing or has no entries", ev)
			continue
		}
		for i, entry := range entries {
			if len(entry.Hooks) == 0 {
				t.Errorf("event %q entry %d: nested 'hooks' array missing or empty (Claude Code requires {matcher, hooks:[{type, command}]})", ev, i)
				continue
			}
			for j, h := range entry.Hooks {
				if h.Type != "command" {
					t.Errorf("event %q entry %d hook %d: type=%q, want %q", ev, i, j, h.Type, "command")
				}
				if h.Command == "" {
					t.Errorf("event %q entry %d hook %d: command is empty", ev, i, j)
				}
				if (ev == "PreToolUse" || ev == "PostToolUse") &&
					(!strings.Contains(h.Command, "acd start") || !strings.Contains(h.Command, "acd wake")) {
					t.Errorf("event %q entry %d hook %d: active hook must start before wake: %s", ev, i, j, h.Command)
				}
			}
		}
	}
}

// TestSetup_ClaudeCode_ActiveHooksAndChainPlusLogFallback guards that
// PreToolUse and PostToolUse:
//   - chain `acd start` and `acd wake` with logical-and (`&&`) so a failed
//     start cannot be silently masked by a successful wake;
//   - end with an or-clause that writes the failure cause into the harness
//     LOG file and exits nonzero so Claude Code can surface it.
//
// Regression target: P1-3 (wake masks start failure).
func TestSetup_ClaudeCode_ActiveHooksAndChainPlusLogFallback(t *testing.T) {
	out, _, _ := runSetupCmd(t, "claude-code")
	start := strings.Index(out, "{")
	end := strings.LastIndex(out, "}")
	if start == -1 || end == -1 || end <= start {
		t.Fatalf("no JSON block in output:\n%s", out)
	}
	var settings struct {
		Hooks map[string][]struct {
			Hooks []struct {
				Command string `json:"command"`
			} `json:"hooks"`
		} `json:"hooks"`
	}
	if err := json.Unmarshal([]byte(out[start:end+1]), &settings); err != nil {
		t.Fatalf("parse JSON: %v", err)
	}
	for _, ev := range []string{"PreToolUse", "PostToolUse"} {
		for i, entry := range settings.Hooks[ev] {
			for j, h := range entry.Hooks {
				assertActiveHookAndChainAndLogFallback(t, ev, i, j, h.Command, "claude-code-hook.log")
			}
		}
	}
}

// TestSetup_ClaudeCode_SessionStartFailSoft guards that SessionStart adopts
// the codex fail-soft pattern: defines LOG, makes its directory, redirects
// stderr into LOG, and guards the extract pipeline so a missing acd binary
// or schema drift never blocks SessionStart.
//
// Regression target: P2-15 (claude-code SessionStart no fail-soft).
func TestSetup_ClaudeCode_SessionStartFailSoft(t *testing.T) {
	out, _, _ := runSetupCmd(t, "claude-code")
	start := strings.Index(out, "{")
	end := strings.LastIndex(out, "}")
	if start == -1 || end == -1 {
		t.Fatalf("no JSON block")
	}
	var settings struct {
		Hooks map[string][]struct {
			Hooks []struct {
				Command string `json:"command"`
			} `json:"hooks"`
		} `json:"hooks"`
	}
	if err := json.Unmarshal([]byte(out[start:end+1]), &settings); err != nil {
		t.Fatalf("parse JSON: %v", err)
	}
	ss := settings.Hooks["SessionStart"]
	if len(ss) == 0 || len(ss[0].Hooks) == 0 {
		t.Fatalf("SessionStart hook missing")
	}
	cmd := ss[0].Hooks[0].Command
	for _, want := range []string{
		`LOG="${XDG_STATE_HOME:-$HOME/.local/state}/acd/claude-code-hook.log"`,
		`mkdir -p`,
		`acd hook-stdin-extract session_id`,
		`|| exit 0`,
		`2>>"$LOG"`,
	} {
		if !strings.Contains(cmd, want) {
			t.Errorf("SessionStart missing fail-soft fragment %q in:\n%s", want, cmd)
		}
	}
}

// --- codex ------------------------------------------------------------------

func TestSetup_Codex_ExitsZero(t *testing.T) {
	out, _, err := runSetupCmd(t, "codex")
	if err != nil {
		t.Fatalf("expected exit 0, got: %v\nstdout:\n%s", err, out)
	}
}

func TestSetup_Codex_ContainsSnippet(t *testing.T) {
	out, _, _ := runSetupCmd(t, "codex")
	want := snippetBody(t, "codex/hooks.json")
	if !strings.Contains(out, strings.TrimSpace(want)) {
		t.Errorf("codex snippet body not found.\nwant:\n%s\ngot:\n%s", want, out)
	}
}

func TestSetup_Codex_AcdManagedMarker(t *testing.T) {
	out, _, _ := runSetupCmd(t, "codex")
	if !strings.Contains(out, `"_acd_managed": true`) {
		t.Errorf("acd-managed marker not found in codex output:\n%s", out)
	}
}

func TestSetup_Codex_FooterInstructions(t *testing.T) {
	out, _, _ := runSetupCmd(t, "codex")
	if !strings.Contains(out, "hooks.json") {
		t.Errorf("footer missing 'hooks.json' in output:\n%s", out)
	}
}

func TestSetup_Codex_RawEmitsValidJSONOnly(t *testing.T) {
	out, _, err := runSetupCmd(t, "codex", "--raw")
	if err != nil {
		t.Fatalf("acd setup codex --raw exit=%v\nstdout=%s", err, out)
	}
	// Raw output must parse as JSON without any pre/post comment wrapping;
	// users will redirect this directly into ~/.codex/hooks.json.
	var v interface{}
	if err := json.Unmarshal([]byte(out), &v); err != nil {
		t.Fatalf("--raw output must be valid JSON, got error %v\noutput:\n%s", err, out)
	}
	if strings.HasPrefix(strings.TrimSpace(out), "//") {
		t.Errorf("--raw output must not start with comment wrapper:\n%s", out)
	}
}

func TestSetup_Codex_HasCanonicalHookSchema(t *testing.T) {
	out, _, _ := runSetupCmd(t, "codex")
	// Strip the leading "// " comment prefix from each line so the embedded
	// JSON snippet parses as a single block. The output starts and ends
	// with header/footer comment lines and the snippet body sits in
	// between as raw JSON, so locate the JSON block by braces.
	start := strings.Index(out, "{")
	end := strings.LastIndex(out, "}")
	if start == -1 || end == -1 || end <= start {
		t.Fatalf("no JSON block found in codex output:\n%s", out)
	}
	var settings struct {
		ACDManaged bool `json:"_acd_managed"`
		Hooks      map[string][]struct {
			Matcher *string `json:"matcher,omitempty"`
			Hooks   []struct {
				Type    string `json:"type"`
				Timeout int    `json:"timeout"`
				Command string `json:"command"`
			} `json:"hooks"`
		} `json:"hooks"`
	}
	if err := json.Unmarshal([]byte(out[start:end+1]), &settings); err != nil {
		t.Fatalf("parse codex JSON: %v\nblock:\n%s", err, out[start:end+1])
	}
	if !settings.ACDManaged {
		t.Errorf("_acd_managed not true at top level")
	}
	required := []string{"SessionStart", "UserPromptSubmit", "PreToolUse", "PostToolUse", "Stop"}
	for _, ev := range required {
		entries, ok := settings.Hooks[ev]
		if !ok || len(entries) == 0 {
			t.Errorf("event %q missing or has no entries", ev)
			continue
		}
		for i, entry := range entries {
			if (ev == "PreToolUse" || ev == "PostToolUse") && (entry.Matcher == nil || *entry.Matcher == "") {
				t.Errorf("event %q entry %d: matcher must be set on tool-use hooks", ev, i)
			}
			if len(entry.Hooks) == 0 {
				t.Errorf("event %q entry %d: nested hooks array empty", ev, i)
				continue
			}
			for j, h := range entry.Hooks {
				if h.Type != "command" {
					t.Errorf("event %q entry %d hook %d: type=%q want command", ev, i, j, h.Type)
				}
				if h.Command == "" {
					t.Errorf("event %q entry %d hook %d: command empty", ev, i, j)
				}
				if h.Timeout <= 0 {
					t.Errorf("event %q entry %d hook %d: timeout must be positive, got %d", ev, i, j, h.Timeout)
				}
				if !strings.Contains(h.Command, "acd hook-stdin-extract session_id cwd") {
					t.Errorf("event %q entry %d hook %d: command missing multi-arg hook-stdin-extract: %s", ev, i, j, h.Command)
				}
			}
		}
	}
	// Stop must call acd touch (mirrors claude-code).
	if stop := settings.Hooks["Stop"]; len(stop) > 0 && len(stop[0].Hooks) > 0 {
		if !strings.Contains(stop[0].Hooks[0].Command, "acd touch") {
			t.Errorf("Stop hook must call acd touch: %s", stop[0].Hooks[0].Command)
		}
	}
}

// TestSetup_Codex_ActiveHooksSelfHeal guards that codex active hooks
// (UserPromptSubmit, PreToolUse, PostToolUse) are resilient: they must
//   - call `acd start` before `acd wake` so a fresh session can self-register;
//   - chain start and wake with `&&` so a failed start cannot be masked by
//     a successful wake;
//   - tail with an or-clause that logs the failure cause into the harness
//     LOG file and exits nonzero so codex can surface it;
//   - gate `mkdir -p` behind a directory-exists check so the hot path does
//     not fork+exec mkdir on every tool turn.
//
// Regression targets: P1-3 (wake masks start failure), P3-17 (mkdir hot-path).
func TestSetup_Codex_ActiveHooksSelfHeal(t *testing.T) {
	out, _, _ := runSetupCmd(t, "codex")
	start := strings.Index(out, "{")
	end := strings.LastIndex(out, "}")
	if start == -1 || end == -1 {
		t.Fatalf("no JSON block")
	}
	var settings struct {
		Hooks map[string][]struct {
			Hooks []struct {
				Command string `json:"command"`
			} `json:"hooks"`
		} `json:"hooks"`
	}
	if err := json.Unmarshal([]byte(out[start:end+1]), &settings); err != nil {
		t.Fatalf("parse JSON: %v", err)
	}
	for _, ev := range []string{"UserPromptSubmit", "PreToolUse", "PostToolUse"} {
		entries := settings.Hooks[ev]
		if len(entries) == 0 {
			t.Errorf("event %q missing", ev)
			continue
		}
		for i, entry := range entries {
			for j, h := range entry.Hooks {
				cmd := h.Command
				if !strings.Contains(cmd, "acd start") || !strings.Contains(cmd, "acd wake") {
					t.Errorf("%s entry %d hook %d: must call both acd start and acd wake: %s", ev, i, j, cmd)
				}
				if strings.Index(cmd, "acd start") > strings.Index(cmd, "acd wake") {
					t.Errorf("%s entry %d hook %d: acd start must precede acd wake: %s", ev, i, j, cmd)
				}
				assertActiveHookAndChainAndLogFallback(t, ev, i, j, cmd, "codex-hook.log")
			}
		}
	}
	// All codex hook bodies must gate mkdir behind a directory-exists check
	// to avoid fork+exec on every PreToolUse / PostToolUse.
	for ev, entries := range settings.Hooks {
		for i, entry := range entries {
			for j, h := range entry.Hooks {
				if strings.Contains(h.Command, "mkdir -p") &&
					!strings.Contains(h.Command, `[ -d "$LOG_DIR" ] || mkdir -p`) {
					t.Errorf("%s entry %d hook %d: mkdir -p must be gated by [ -d \"$LOG_DIR\" ] || mkdir -p: %s", ev, i, j, h.Command)
				}
			}
		}
	}
}

// --- opencode ---------------------------------------------------------------

func TestSetup_OpenCode_ExitsZero(t *testing.T) {
	out, _, err := runSetupCmd(t, "opencode")
	if err != nil {
		t.Fatalf("expected exit 0, got: %v\nstdout:\n%s", err, out)
	}
}

func TestSetup_OpenCode_ContainsSnippet(t *testing.T) {
	out, _, _ := runSetupCmd(t, "opencode")
	want := snippetBody(t, "opencode/hooks.snippet.yaml")
	if !strings.Contains(out, strings.TrimSpace(want)) {
		t.Errorf("opencode snippet body not found.\nwant:\n%s\ngot:\n%s", want, out)
	}
}

func TestSetup_OpenCode_AcdManagedMarker(t *testing.T) {
	out, _, _ := runSetupCmd(t, "opencode")
	if !strings.Contains(out, "acd-managed: true") {
		t.Errorf("acd-managed marker not found in opencode output:\n%s", out)
	}
}

func TestSetup_OpenCode_FooterInstructions(t *testing.T) {
	out, _, _ := runSetupCmd(t, "opencode")
	// README says "hooks.yaml"
	if !strings.Contains(out, "hooks.yaml") {
		t.Errorf("footer missing 'hooks.yaml' in output:\n%s", out)
	}
}

func TestSetup_OpenCode_ActiveHooksStartBeforeWake(t *testing.T) {
	body := snippetBody(t, "opencode/hooks.snippet.yaml")
	for _, id := range []string{"acd-wake-tool-before", "acd-wake-tool-after"} {
		block := yamlHookBlock(t, body, id)
		if !strings.Contains(block, "acd start") || !strings.Contains(block, "acd wake") {
			t.Fatalf("%s must run acd start before acd wake:\n%s", id, block)
		}
		if strings.Index(block, "acd start") > strings.Index(block, "acd wake") {
			t.Fatalf("%s runs acd wake before acd start:\n%s", id, block)
		}
	}
}

// TestSetup_OpenCode_ActiveHooksAndChainPlusLogFallback guards that
// tool.before.* and tool.after.* hooks chain start AND wake with `&&` and
// route a failure through the harness LOG file, exiting nonzero so opencode
// surfaces it. Regression target: P1-3 (wake masks start failure).
func TestSetup_OpenCode_ActiveHooksAndChainPlusLogFallback(t *testing.T) {
	body := snippetBody(t, "opencode/hooks.snippet.yaml")
	for _, id := range []string{"acd-wake-tool-before", "acd-wake-tool-after"} {
		block := yamlHookBlock(t, body, id)
		assertYAMLActiveHookAndChainAndLogFallback(t, id, block, "opencode-hook.log")
	}
}

// --- pi ---------------------------------------------------------------------

func TestSetup_Pi_ExitsZero(t *testing.T) {
	out, _, err := runSetupCmd(t, "pi")
	if err != nil {
		t.Fatalf("expected exit 0, got: %v\nstdout:\n%s", err, out)
	}
}

func TestSetup_Pi_ContainsSnippet(t *testing.T) {
	out, _, _ := runSetupCmd(t, "pi")
	want := snippetBody(t, "pi/hooks.snippet.yaml")
	if !strings.Contains(out, strings.TrimSpace(want)) {
		t.Errorf("pi snippet body not found.\nwant:\n%s\ngot:\n%s", want, out)
	}
}

func TestSetup_Pi_AcdManagedMarker(t *testing.T) {
	out, _, _ := runSetupCmd(t, "pi")
	if !strings.Contains(out, "acd-managed: true") {
		t.Errorf("acd-managed marker not found in pi output:\n%s", out)
	}
}

func TestSetup_Pi_FooterInstructions(t *testing.T) {
	out, _, _ := runSetupCmd(t, "pi")
	// README says ".pi/hook/hooks.yaml"
	if !strings.Contains(out, ".pi/hook/hooks.yaml") {
		t.Errorf("footer missing '.pi/hook/hooks.yaml' in output:\n%s", out)
	}
}

func TestSetup_Pi_ActiveHooksStartBeforeWakeAndSessionFallbackIsStable(t *testing.T) {
	body := snippetBody(t, "pi/hooks.snippet.yaml")
	if strings.Contains(body, "uuidgen") {
		t.Fatalf("pi snippet must not create one-off session ids with uuidgen:\n%s", body)
	}
	for _, id := range []string{"acd-wake-tool-before", "acd-wake-tool-after"} {
		block := yamlHookBlock(t, body, id)
		if !strings.Contains(block, "acd start") || !strings.Contains(block, "acd wake") {
			t.Fatalf("%s must run acd start before acd wake:\n%s", id, block)
		}
		if strings.Index(block, "acd start") > strings.Index(block, "acd wake") {
			t.Fatalf("%s runs acd wake before acd start:\n%s", id, block)
		}
		if !strings.Contains(block, `SID="${PI_SESSION_ID:-unknown}"`) || !strings.Contains(block, `--session-id "$SID"`) {
			t.Fatalf("%s must use the stable SID fallback:\n%s", id, block)
		}
	}
}

// TestSetup_Pi_ActiveHooksAndChainPlusLogFallback guards that tool.before.*
// and tool.after.* hooks chain start AND wake with `&&` and route failure
// through the harness LOG file, exiting nonzero so pi surfaces it.
// Regression target: P1-3 (wake masks start failure).
func TestSetup_Pi_ActiveHooksAndChainPlusLogFallback(t *testing.T) {
	body := snippetBody(t, "pi/hooks.snippet.yaml")
	for _, id := range []string{"acd-wake-tool-before", "acd-wake-tool-after"} {
		block := yamlHookBlock(t, body, id)
		assertYAMLActiveHookAndChainAndLogFallback(t, id, block, "pi-hook.log")
	}
}

// --- shell ------------------------------------------------------------------

func TestSetup_Shell_ExitsZero(t *testing.T) {
	out, _, err := runSetupCmd(t, "shell")
	if err != nil {
		t.Fatalf("expected exit 0, got: %v\nstdout:\n%s", err, out)
	}
}

func TestSetup_Shell_ContainsBothSnippets(t *testing.T) {
	out, _, _ := runSetupCmd(t, "shell")

	wantDirenv := snippetBody(t, "shell/direnv.envrc.snippet")
	if !strings.Contains(out, strings.TrimSpace(wantDirenv)) {
		t.Errorf("shell direnv snippet not found in output:\n%s", out)
	}

	wantZshrc := snippetBody(t, "shell/zshrc.snippet.sh")
	if !strings.Contains(out, strings.TrimSpace(wantZshrc)) {
		t.Errorf("shell zshrc snippet not found in output:\n%s", out)
	}
}

func TestSetup_Shell_AcdManagedMarker(t *testing.T) {
	out, _, _ := runSetupCmd(t, "shell")
	if !strings.Contains(out, "acd-managed: true") {
		t.Errorf("acd-managed marker not found in shell output:\n%s", out)
	}
}

func TestSetup_Shell_FooterInstructions(t *testing.T) {
	out, _, _ := runSetupCmd(t, "shell")
	// README mentions "direnv" and "zsh"
	if !strings.Contains(out, "direnv") {
		t.Errorf("footer missing 'direnv' in shell output:\n%s", out)
	}
}

func TestSetup_Shell_BashSyntaxCheck(t *testing.T) {
	bash, err := exec.LookPath("bash")
	if err != nil {
		t.Skip("bash not on PATH; skipping syntax check")
	}

	// Check direnv snippet.
	direnvBody := snippetBody(t, "shell/direnv.envrc.snippet")
	cmd := exec.Command(bash, "-n", "-c", direnvBody)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Errorf("bash -n on direnv snippet failed: %v\n%s", err, out)
	}

	// Check zshrc snippet.
	zshrcBody := snippetBody(t, "shell/zshrc.snippet.sh")
	cmd2 := exec.Command(bash, "-n", "-c", zshrcBody)
	if out, err := cmd2.CombinedOutput(); err != nil {
		t.Errorf("bash -n on zshrc snippet failed: %v\n%s", err, out)
	}
}

func yamlHookBlock(t *testing.T, body, id string) string {
	t.Helper()
	start := strings.Index(body, "- id: "+id)
	if start < 0 {
		t.Fatalf("hook id %q not found in:\n%s", id, body)
	}
	rest := body[start+len("- id: "+id):]
	next := strings.Index(rest, "\n  - id:")
	if next >= 0 {
		return rest[:next]
	}
	return rest
}

// --- error cases ------------------------------------------------------------

func TestSetup_UnknownHarness_NonZeroExit(t *testing.T) {
	_, stderr, err := runSetupCmd(t, "unknown")
	if err == nil {
		t.Fatal("expected non-zero exit for unknown harness, got nil")
	}
	if !strings.Contains(stderr, "supported") && !strings.Contains(stderr, "Supported") {
		t.Errorf("stderr should list supported harnesses, got: %q", stderr)
	}
	// stderr should list each supported harness name.
	for _, h := range supportedHarnesses {
		if !strings.Contains(stderr, h) {
			t.Errorf("stderr missing supported harness %q: %q", h, stderr)
		}
	}
}

func TestSetup_ApplyFlag_NonZero(t *testing.T) {
	out, stderr, err := runSetupCmd(t, "claude-code", "--apply")
	if err == nil {
		t.Fatalf("--apply should fail, got nil\nstdout:\n%s", out)
	}
	if out != "" {
		t.Errorf("--apply should not render snippet stdout, got:\n%s", out)
	}
	if !strings.Contains(stderr, "--apply is not implemented") {
		t.Errorf("--apply stderr should explain rejection, got: %q", stderr)
	}
}

func TestSetup_NoArg_AutoDetectsSingleHarness(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)

	settings := filepath.Join(home, ".claude", "settings.json")
	if err := os.MkdirAll(filepath.Dir(settings), 0o700); err != nil {
		t.Fatalf("mkdir settings dir: %v", err)
	}
	if err := os.WriteFile(settings, []byte(`{"_acd_managed": true}`), 0o600); err != nil {
		t.Fatalf("write settings: %v", err)
	}

	out, stderr, err := runSetupCmd(t)
	if err != nil {
		t.Fatalf("expected auto-detected setup to exit 0, got: %v\nstderr:\n%s", err, stderr)
	}
	want := snippetBody(t, "claude-code/settings.snippet.json")
	if !strings.Contains(out, strings.TrimSpace(want)) {
		t.Errorf("auto-detected setup did not render claude-code snippet.\nwant:\n%s\ngot:\n%s", want, out)
	}
}

func TestSetup_NoArg_MultipleDetectedListsHarnesses(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)

	files := map[string]string{
		filepath.Join(home, ".claude", "settings.json"): `{"_acd_managed": true}`,
		filepath.Join(home, ".codex", "config.toml"):    "# acd-managed: true\n",
	}
	for path, body := range files {
		if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
			t.Fatalf("mkdir %s: %v", filepath.Dir(path), err)
		}
		if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
			t.Fatalf("write %s: %v", path, err)
		}
	}

	out, stderr, err := runSetupCmd(t)
	if err == nil {
		t.Fatalf("expected multiple detected harnesses to fail, got nil\nstdout:\n%s", out)
	}
	if out != "" {
		t.Errorf("multi-detect should not render snippet stdout, got:\n%s", out)
	}
	for _, want := range []string{"claude-code", "codex"} {
		if !strings.Contains(stderr, want) {
			t.Errorf("multi-detect stderr missing %q: %q", want, stderr)
		}
	}
}

// --- alias tests ------------------------------------------------------------

// TestSetup_InitAliasStillWorks verifies that invoking the command via the
// "init" alias still produces the snippet on stdout and emits a deprecation
// warning on stderr.
func TestSetup_InitAliasStillWorks(t *testing.T) {
	var outBuf, errBuf bytes.Buffer
	cmd := newSetupCmd()
	cmd.SetOut(&outBuf)
	cmd.SetErr(&errBuf)
	// Simulate the user running: acd init claude-code
	// CalledAs() only returns the alias name when cobra routes via the alias,
	// which requires the command to be added to a parent. Build a minimal
	// parent so Cobra resolves "init" as an alias.
	root := newRootCmd()
	root.SetOut(&outBuf)
	root.SetErr(&errBuf)
	root.SetArgs([]string{"init", "claude-code"})
	if err := root.Execute(); err != nil {
		t.Fatalf("expected exit 0 via init alias, got: %v\nstdout:\n%s\nstderr:\n%s",
			err, outBuf.String(), errBuf.String())
	}

	stdout := outBuf.String()
	stderr := errBuf.String()

	// The snippet must appear in stdout.
	want := snippetBody(t, "claude-code/settings.snippet.json")
	if !strings.Contains(stdout, strings.TrimSpace(want)) {
		t.Errorf("init alias did not render claude-code snippet.\nwant substring:\n%s\ngot stdout:\n%s", want, stdout)
	}

	// A deprecation warning must appear in stderr.
	if !strings.Contains(stderr, "deprecated") {
		t.Errorf("init alias did not emit deprecation warning on stderr.\ngot stderr: %q", stderr)
	}
}

// TestSetup_HelpHidesInitAlias verifies that the root --help output contains
// "acd setup" in the Setup section and does not list "acd init" as a separate
// visible command row.
//
// Design note: Cobra includes aliases in per-command help (e.g. "acd setup
// --help" will show "Aliases: init"), but the root help is rendered via our
// custom rootHelpTemplate which hard-codes the Setup table — so the alias
// cannot appear there as a separate row. This test guards that invariant.
func TestSetup_HelpHidesInitAlias(t *testing.T) {
	root := newRootCmd()
	var outBuf, errBuf bytes.Buffer
	root.SetOut(&outBuf)
	root.SetErr(&errBuf)
	root.SetArgs([]string{"--help"})
	// Execute returns ErrNoCommand; that's expected for root --help.
	_ = root.Execute()

	got := outBuf.String()

	// The Setup section must mention "acd setup".
	if !strings.Contains(got, "acd setup") {
		t.Errorf("root help missing 'acd setup' in Setup section:\n%s", got)
	}

	// There must be no standalone "acd init   Print harness install snippets"
	// row — the alias must not appear as a separate entry in the Setup table.
	if strings.Contains(got, "acd init   ") {
		t.Errorf("root help contains a standalone 'acd init' row; alias should be hidden from root help listing:\n%s", got)
	}
}
