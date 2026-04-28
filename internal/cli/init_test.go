package cli

// Tests for §7.9 — acd init <harness>.

import (
	"bytes"
	"encoding/json"
	"io/fs"
	"os/exec"
	"strings"
	"testing"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/templates"
)

// runInitCmd is a test helper that drives newInitCmd() through its cobra
// RunE and captures stdout + stderr. It returns the captured output and
// any error the command returned.
func runInitCmd(t *testing.T, args ...string) (stdout, stderr string, err error) {
	t.Helper()
	var outBuf, errBuf bytes.Buffer
	cmd := newInitCmd()
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

func TestInit_ClaudeCode_ExitsZero(t *testing.T) {
	out, _, err := runInitCmd(t, "claude-code")
	if err != nil {
		t.Fatalf("expected exit 0, got: %v\nstdout:\n%s", err, out)
	}
}

func TestInit_ClaudeCode_ContainsSnippet(t *testing.T) {
	out, _, _ := runInitCmd(t, "claude-code")
	want := snippetBody(t, "claude-code/settings.snippet.json")
	if !strings.Contains(out, strings.TrimSpace(want)) {
		t.Errorf("snippet body not found in output.\nwant substring:\n%s\ngot:\n%s", want, out)
	}
}

func TestInit_ClaudeCode_ValidJSON(t *testing.T) {
	out, _, _ := runInitCmd(t, "claude-code")
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

func TestInit_ClaudeCode_AcdManagedMarker(t *testing.T) {
	out, _, _ := runInitCmd(t, "claude-code")
	if !strings.Contains(out, `"_acd_managed": true`) {
		t.Errorf("acd-managed marker not found in output:\n%s", out)
	}
}

func TestInit_ClaudeCode_FooterInstructions(t *testing.T) {
	out, _, _ := runInitCmd(t, "claude-code")
	// README says "Merge the printed JSON into ~/.claude/settings.json"
	if !strings.Contains(out, "settings.json") {
		t.Errorf("footer instructions missing 'settings.json' in output:\n%s", out)
	}
}

// --- codex ------------------------------------------------------------------

func TestInit_Codex_ExitsZero(t *testing.T) {
	out, _, err := runInitCmd(t, "codex")
	if err != nil {
		t.Fatalf("expected exit 0, got: %v\nstdout:\n%s", err, out)
	}
}

func TestInit_Codex_ContainsSnippet(t *testing.T) {
	out, _, _ := runInitCmd(t, "codex")
	want := snippetBody(t, "codex/config.snippet.toml")
	if !strings.Contains(out, strings.TrimSpace(want)) {
		t.Errorf("codex snippet body not found.\nwant:\n%s\ngot:\n%s", want, out)
	}
}

func TestInit_Codex_AcdManagedMarker(t *testing.T) {
	out, _, _ := runInitCmd(t, "codex")
	// TOML uses "# acd-managed: true" comment line.
	if !strings.Contains(out, "acd-managed: true") {
		t.Errorf("acd-managed marker not found in codex output:\n%s", out)
	}
}

func TestInit_Codex_FooterInstructions(t *testing.T) {
	out, _, _ := runInitCmd(t, "codex")
	// README says "config.toml"
	if !strings.Contains(out, "config.toml") {
		t.Errorf("footer missing 'config.toml' in output:\n%s", out)
	}
}

// --- opencode ---------------------------------------------------------------

func TestInit_OpenCode_ExitsZero(t *testing.T) {
	out, _, err := runInitCmd(t, "opencode")
	if err != nil {
		t.Fatalf("expected exit 0, got: %v\nstdout:\n%s", err, out)
	}
}

func TestInit_OpenCode_ContainsSnippet(t *testing.T) {
	out, _, _ := runInitCmd(t, "opencode")
	want := snippetBody(t, "opencode/hooks.snippet.yaml")
	if !strings.Contains(out, strings.TrimSpace(want)) {
		t.Errorf("opencode snippet body not found.\nwant:\n%s\ngot:\n%s", want, out)
	}
}

func TestInit_OpenCode_AcdManagedMarker(t *testing.T) {
	out, _, _ := runInitCmd(t, "opencode")
	if !strings.Contains(out, "acd-managed: true") {
		t.Errorf("acd-managed marker not found in opencode output:\n%s", out)
	}
}

func TestInit_OpenCode_FooterInstructions(t *testing.T) {
	out, _, _ := runInitCmd(t, "opencode")
	// README says "hooks.yaml"
	if !strings.Contains(out, "hooks.yaml") {
		t.Errorf("footer missing 'hooks.yaml' in output:\n%s", out)
	}
}

// --- pi ---------------------------------------------------------------------

func TestInit_Pi_ExitsZero(t *testing.T) {
	out, _, err := runInitCmd(t, "pi")
	if err != nil {
		t.Fatalf("expected exit 0, got: %v\nstdout:\n%s", err, out)
	}
}

func TestInit_Pi_ContainsSnippet(t *testing.T) {
	out, _, _ := runInitCmd(t, "pi")
	want := snippetBody(t, "pi/hooks.snippet.yaml")
	if !strings.Contains(out, strings.TrimSpace(want)) {
		t.Errorf("pi snippet body not found.\nwant:\n%s\ngot:\n%s", want, out)
	}
}

func TestInit_Pi_AcdManagedMarker(t *testing.T) {
	out, _, _ := runInitCmd(t, "pi")
	if !strings.Contains(out, "acd-managed: true") {
		t.Errorf("acd-managed marker not found in pi output:\n%s", out)
	}
}

func TestInit_Pi_FooterInstructions(t *testing.T) {
	out, _, _ := runInitCmd(t, "pi")
	// README says ".pi/hook/hooks.yaml"
	if !strings.Contains(out, ".pi/hook/hooks.yaml") {
		t.Errorf("footer missing '.pi/hook/hooks.yaml' in output:\n%s", out)
	}
}

// --- shell ------------------------------------------------------------------

func TestInit_Shell_ExitsZero(t *testing.T) {
	out, _, err := runInitCmd(t, "shell")
	if err != nil {
		t.Fatalf("expected exit 0, got: %v\nstdout:\n%s", err, out)
	}
}

func TestInit_Shell_ContainsBothSnippets(t *testing.T) {
	out, _, _ := runInitCmd(t, "shell")

	wantDirenv := snippetBody(t, "shell/direnv.envrc.snippet")
	if !strings.Contains(out, strings.TrimSpace(wantDirenv)) {
		t.Errorf("shell direnv snippet not found in output:\n%s", out)
	}

	wantZshrc := snippetBody(t, "shell/zshrc.snippet.sh")
	if !strings.Contains(out, strings.TrimSpace(wantZshrc)) {
		t.Errorf("shell zshrc snippet not found in output:\n%s", out)
	}
}

func TestInit_Shell_AcdManagedMarker(t *testing.T) {
	out, _, _ := runInitCmd(t, "shell")
	if !strings.Contains(out, "acd-managed: true") {
		t.Errorf("acd-managed marker not found in shell output:\n%s", out)
	}
}

func TestInit_Shell_FooterInstructions(t *testing.T) {
	out, _, _ := runInitCmd(t, "shell")
	// README mentions "direnv" and "zsh"
	if !strings.Contains(out, "direnv") {
		t.Errorf("footer missing 'direnv' in shell output:\n%s", out)
	}
}

func TestInit_Shell_BashSyntaxCheck(t *testing.T) {
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

// --- error cases ------------------------------------------------------------

func TestInit_UnknownHarness_NonZeroExit(t *testing.T) {
	_, stderr, err := runInitCmd(t, "unknown")
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

func TestInit_ApplyFlag_ExitsZeroAndMentionsDeferred(t *testing.T) {
	out, _, err := runInitCmd(t, "claude-code", "--apply")
	if err != nil {
		t.Fatalf("--apply should exit 0, got: %v\nstdout:\n%s", err, out)
	}
	// Must mention that --apply is deferred / not implemented in v0.1.
	lower := strings.ToLower(out)
	if !strings.Contains(lower, "not implemented") && !strings.Contains(lower, "deferred") {
		t.Errorf("--apply output should mention not-implemented/deferred, got:\n%s", out)
	}
}
