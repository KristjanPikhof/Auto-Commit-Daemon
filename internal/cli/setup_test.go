package cli

// Tests for §7.9 — acd setup <harness>.

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"testing/fstest"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/installer"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/templates"
)

// withTemplatesFSOverride swaps the package-level templatesFS for an FS
// that returns the supplied bytes at each given path and falls back to the
// real templates FS otherwise. The original FS is restored at test end.
// Used by raw-mode JSON validation tests to inject malformed templates
// without touching the embedded production data.
func withTemplatesFSOverride(t *testing.T, files map[string][]byte) {
	t.Helper()
	overlay := fstest.MapFS{}
	now := time.Now()
	for path, body := range files {
		overlay[path] = &fstest.MapFile{Data: body, ModTime: now}
	}
	prev := templatesFS
	templatesFS = overlayFS{primary: overlay, fallback: prev}
	t.Cleanup(func() { templatesFS = prev })
}

// overlayFS reads from primary first; on os.ErrNotExist it falls back to
// fallback. Used to splice in a malformed template path while leaving the
// rest of the production templates FS intact.
type overlayFS struct {
	primary  fs.FS
	fallback fs.FS
}

func (o overlayFS) Open(name string) (fs.File, error) {
	f, err := o.primary.Open(name)
	if err == nil {
		return f, nil
	}
	if os.IsNotExist(err) {
		return o.fallback.Open(name)
	}
	return nil, err
}

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

func assertTopLevelJSONKeys(t *testing.T, block []byte, want ...string) map[string]json.RawMessage {
	t.Helper()
	var top map[string]json.RawMessage
	if err := json.Unmarshal(block, &top); err != nil {
		t.Fatalf("parse top-level JSON object: %v\nblock:\n%s", err, block)
	}
	if len(top) != len(want) {
		t.Errorf("top-level JSON key count=%d, want %d; keys=%v", len(top), len(want), top)
	}
	for _, key := range want {
		if _, ok := top[key]; !ok {
			t.Errorf("top-level JSON key %q missing; keys=%v", key, top)
		}
	}
	return top
}

func TestSetup_JSONHarnessUninstallDocsCoverLifecycleCommands(t *testing.T) {
	cases := []struct {
		path string
		want []string
	}{
		{
			path: "claude-code/uninstall.md",
			want: []string{"acd internal integration stdin-extract", "acd internal session open", "acd internal hint --kind wake", "acd internal hint --kind logical_boundary", "acd internal session close"},
		},
		{
			path: "codex/uninstall.md",
			want: []string{"acd internal integration stdin-extract", "acd internal session open", "acd internal hint --kind wake", "acd internal hint --kind soft_boundary"},
		},
		{
			path: "cursor/uninstall.md",
			want: []string{"acd internal integration cursor-extract", "acd internal session open", "acd internal hint --kind wake", "acd internal hint --kind logical_boundary", "acd internal session close"},
		},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.path, func(t *testing.T) {
			body := snippetBody(t, tc.path)
			for _, want := range tc.want {
				if !strings.Contains(body, want) {
					t.Fatalf("%s uninstall docs missing %q:\n%s", tc.path, want, body)
				}
			}
		})
	}
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

func TestSetup_ClaudeCode_RawEmitsValidJSONOnly(t *testing.T) {
	out, _, err := runSetupCmd(t, "claude-code", "--raw")
	if err != nil {
		t.Fatalf("acd setup claude-code --raw exit=%v\nstdout=%s", err, out)
	}
	var v interface{}
	if err := json.Unmarshal([]byte(out), &v); err != nil {
		t.Fatalf("--raw output must be valid JSON, got error %v\noutput:\n%s", err, out)
	}
	assertTopLevelJSONKeys(t, []byte(out), "hooks")
	if strings.HasPrefix(strings.TrimSpace(out), "//") {
		t.Errorf("--raw output must not start with comment wrapper:\n%s", out)
	}
}

func TestSetup_ClaudeCode_NoLegacyJSONMarker(t *testing.T) {
	out, _, _ := runSetupCmd(t, "claude-code")
	start := strings.Index(out, "{")
	end := strings.LastIndex(out, "}")
	if start == -1 || end == -1 || end <= start {
		t.Fatalf("no JSON block found in output:\n%s", out)
	}
	if strings.Contains(out[start:end+1], `_acd_managed`) {
		t.Errorf("claude-code JSON output must not emit legacy _acd_managed marker:\n%s", out[start:end+1])
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
	block := []byte(out[start : end+1])
	assertTopLevelJSONKeys(t, block, "hooks")
	var settings struct {
		Hooks map[string][]struct {
			Matcher *string `json:"matcher,omitempty"`
			Hooks   []struct {
				Type    string `json:"type"`
				Command string `json:"command"`
			} `json:"hooks"`
		} `json:"hooks"`
	}
	if err := json.Unmarshal(block, &settings); err != nil {
		t.Fatalf("parse JSON: %v\nblock:\n%s", err, block)
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
					(!strings.Contains(h.Command, "acd internal session open") || !strings.Contains(h.Command, "acd internal hint --kind wake")) {
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
		`acd internal integration stdin-extract session_id`,
		`|| exit 0`,
		`2>>"$LOG"`,
	} {
		if !strings.Contains(cmd, want) {
			t.Errorf("SessionStart missing fail-soft fragment %q in:\n%s", want, cmd)
		}
	}
}

// --- cursor -----------------------------------------------------------------

func TestSetup_Cursor_ExitsZero(t *testing.T) {
	out, _, err := runSetupCmd(t, "cursor")
	if err != nil {
		t.Fatalf("expected exit 0, got: %v\nstdout:\n%s", err, out)
	}
}

func TestSetup_Cursor_ContainsSnippet(t *testing.T) {
	out, _, _ := runSetupCmd(t, "cursor")
	want := snippetBody(t, "cursor/hooks.json")
	if !strings.Contains(out, strings.TrimSpace(want)) {
		t.Errorf("cursor snippet body not found.\nwant:\n%s\ngot:\n%s", want, out)
	}
}

func TestSetup_Cursor_NoLegacyJSONMarker(t *testing.T) {
	out, _, _ := runSetupCmd(t, "cursor")
	start := strings.Index(out, "{")
	end := strings.LastIndex(out, "}")
	if start == -1 || end == -1 || end <= start {
		t.Fatalf("no JSON block found in output:\n%s", out)
	}
	if strings.Contains(out[start:end+1], `_acd_managed`) {
		t.Errorf("cursor JSON output must not emit legacy _acd_managed marker:\n%s", out[start:end+1])
	}
}

func TestSetup_Cursor_FooterInstructions(t *testing.T) {
	out, _, _ := runSetupCmd(t, "cursor")
	if !strings.Contains(out, "hooks.json") {
		t.Errorf("footer missing 'hooks.json' in output:\n%s", out)
	}
}

func TestSetup_Cursor_RawEmitsValidJSONOnly(t *testing.T) {
	out, _, err := runSetupCmd(t, "cursor", "--raw")
	if err != nil {
		t.Fatalf("acd setup cursor --raw exit=%v\nstdout=%s", err, out)
	}
	var v interface{}
	if err := json.Unmarshal([]byte(out), &v); err != nil {
		t.Fatalf("--raw output must be valid JSON, got error %v\noutput:\n%s", err, out)
	}
	assertTopLevelJSONKeys(t, []byte(out), "version", "hooks")
	if strings.HasPrefix(strings.TrimSpace(out), "//") {
		t.Errorf("--raw output must not start with comment wrapper:\n%s", out)
	}
}

func TestSetup_Cursor_RawRejectsInvalidJSON(t *testing.T) {
	bad := []byte(`{"version": 1, "hooks": {},}`)
	withTemplatesFSOverride(t, map[string][]byte{"cursor/hooks.json": bad})

	out, stderr, err := runSetupCmd(t, "cursor", "--raw")
	if err == nil {
		t.Fatalf("acd setup cursor --raw with invalid template must return non-zero, got nil\nstdout:%s\nstderr:%s", out, stderr)
	}
	if out != "" {
		t.Errorf("invalid JSON template must not emit body to stdout, got:\n%s", out)
	}
	for _, want := range []string{"cursor/hooks.json", "not valid JSON", "byte offset"} {
		if !strings.Contains(stderr, want) {
			t.Errorf("stderr missing %q in:\n%s", want, stderr)
		}
	}
}

func TestSetup_Cursor_HasCanonicalHookSchema(t *testing.T) {
	out, _, _ := runSetupCmd(t, "cursor")
	start := strings.Index(out, "{")
	if start == -1 {
		t.Fatalf("no JSON block found in cursor output:\n%s", out)
	}
	tail := out[start:]
	if footer := strings.Index(tail, "\n// ── install instructions"); footer >= 0 {
		tail = tail[:footer]
	}
	end := strings.LastIndex(tail, "}")
	if end == -1 {
		t.Fatalf("no JSON block found in cursor output:\n%s", out)
	}
	block := tail[:end+1]
	assertTopLevelJSONKeys(t, []byte(block), "version", "hooks")
	var settings struct {
		Version int `json:"version"`
		Hooks   map[string][]struct {
			Command string `json:"command"`
			Timeout int    `json:"timeout"`
		} `json:"hooks"`
	}
	if err := json.Unmarshal([]byte(block), &settings); err != nil {
		t.Fatalf("parse cursor JSON: %v\nblock:\n%s", err, block)
	}
	if settings.Version != 1 {
		t.Errorf("version=%d want 1", settings.Version)
	}
	required := []string{"sessionStart", "postToolUse", "afterFileEdit", "stop", "sessionEnd"}
	for _, ev := range required {
		entries, ok := settings.Hooks[ev]
		if !ok || len(entries) == 0 {
			t.Errorf("event %q missing or has no entries", ev)
			continue
		}
		for i, h := range entries {
			if h.Command == "" {
				t.Errorf("event %q entry %d: command empty", ev, i)
			}
			if h.Timeout <= 0 {
				t.Errorf("event %q entry %d: timeout must be positive, got %d", ev, i, h.Timeout)
			}
			if !strings.Contains(h.Command, "hook-cursor-extract") {
				t.Errorf("event %q entry %d: command must extract Cursor stdin: %s", ev, i, h.Command)
			}
		}
	}
	if start := settings.Hooks["sessionStart"]; len(start) > 0 {
		if !strings.Contains(start[0].Command, "acd internal session open") {
			t.Errorf("sessionStart hook must call acd start: %s", start[0].Command)
		}
	}
	for _, ev := range []string{"postToolUse", "afterFileEdit"} {
		entries := settings.Hooks[ev]
		if len(entries) == 0 {
			continue
		}
		if !strings.Contains(entries[0].Command, "acd internal session open") || !strings.Contains(entries[0].Command, "acd internal hint --kind wake") {
			t.Errorf("%s hook must call acd start+wake: %s", ev, entries[0].Command)
		}
	}
	if stop := settings.Hooks["stop"]; len(stop) > 0 {
		if !strings.Contains(stop[0].Command, "acd internal hint --kind logical_boundary") {
			t.Errorf("stop hook must call acd flush --logical: %s", stop[0].Command)
		}
	}
	if sessionEnd := settings.Hooks["sessionEnd"]; len(sessionEnd) > 0 {
		if !strings.Contains(sessionEnd[0].Command, "acd internal session close") {
			t.Errorf("sessionEnd hook must call acd stop: %s", sessionEnd[0].Command)
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

func TestSetup_Codex_NoLegacyJSONMarker(t *testing.T) {
	out, _, _ := runSetupCmd(t, "codex")
	start := strings.Index(out, "{")
	end := strings.LastIndex(out, "}")
	if start == -1 || end == -1 || end <= start {
		t.Fatalf("no JSON block found in output:\n%s", out)
	}
	if strings.Contains(out[start:end+1], `_acd_managed`) {
		t.Errorf("codex JSON output must not emit legacy _acd_managed marker:\n%s", out[start:end+1])
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
	assertTopLevelJSONKeys(t, []byte(out), "hooks")
	if strings.HasPrefix(strings.TrimSpace(out), "//") {
		t.Errorf("--raw output must not start with comment wrapper:\n%s", out)
	}
}

// TestSetup_Codex_RawRejectsInvalidJSON guards that `acd setup codex --raw`
// refuses to emit a body that would silently corrupt ~/.codex/hooks.json.
// We swap templatesFS for an overlay FS that returns malformed JSON for
// the codex template path; runSetup must detect the parse error, write an
// actionable message to stderr, and return non-zero. Regression target:
// P1-11 (invalid template JSON would cause Codex to silently disable hooks).
func TestSetup_Codex_RawRejectsInvalidJSON(t *testing.T) {
	// Trailing comma after first key — clearly invalid per RFC 8259.
	bad := []byte(`{"hooks": {},}`)
	withTemplatesFSOverride(t, map[string][]byte{"codex/hooks.json": bad})

	out, stderr, err := runSetupCmd(t, "codex", "--raw")
	if err == nil {
		t.Fatalf("acd setup codex --raw with invalid template must return non-zero, got nil\nstdout:%s\nstderr:%s", out, stderr)
	}
	if out != "" {
		t.Errorf("invalid JSON template must not emit body to stdout, got:\n%s", out)
	}
	for _, want := range []string{"codex/hooks.json", "not valid JSON", "byte offset"} {
		if !strings.Contains(stderr, want) {
			t.Errorf("stderr missing %q in:\n%s", want, stderr)
		}
	}
}

// TestSetup_ClaudeCode_RawRejectsInvalidJSON mirrors the codex case for the
// claude-code raw path, since it also targets a strict-JSON config file.
func TestSetup_ClaudeCode_RawRejectsInvalidJSON(t *testing.T) {
	bad := []byte(`{"hooks": {`) // truncated
	withTemplatesFSOverride(t, map[string][]byte{"claude-code/settings.snippet.json": bad})

	out, stderr, err := runSetupCmd(t, "claude-code", "--raw")
	if err == nil {
		t.Fatalf("acd setup claude-code --raw with invalid template must return non-zero, got nil\nstdout:%s\nstderr:%s", out, stderr)
	}
	if out != "" {
		t.Errorf("invalid JSON template must not emit body to stdout, got:\n%s", out)
	}
	if !strings.Contains(stderr, "not valid JSON") {
		t.Errorf("stderr missing 'not valid JSON' marker:\n%s", stderr)
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
	block := []byte(out[start : end+1])
	assertTopLevelJSONKeys(t, block, "hooks")
	var settings struct {
		Hooks map[string][]struct {
			Matcher *string `json:"matcher,omitempty"`
			Hooks   []struct {
				Type    string `json:"type"`
				Timeout int    `json:"timeout"`
				Command string `json:"command"`
			} `json:"hooks"`
		} `json:"hooks"`
	}
	if err := json.Unmarshal(block, &settings); err != nil {
		t.Fatalf("parse codex JSON: %v\nblock:\n%s", err, block)
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
				if !strings.Contains(h.Command, "acd internal integration stdin-extract session_id cwd") {
					t.Errorf("event %q entry %d hook %d: command missing multi-arg hook-stdin-extract: %s", ev, i, j, h.Command)
				}
			}
		}
	}
	// Stop must record a soft boundary; unlike Claude's logical flush this
	// requests evaluation without bypassing Intent v2 safety gates.
	if stop := settings.Hooks["Stop"]; len(stop) > 0 && len(stop[0].Hooks) > 0 {
		command := stop[0].Hooks[0].Command
		if !strings.Contains(command, "acd internal hint --kind soft_boundary") {
			t.Errorf("Stop hook must call acd touch --soft-boundary: %s", command)
		}
		if strings.Contains(command, "acd flush --logical") {
			t.Errorf("Codex Stop must remain a soft boundary, got hard flush: %s", command)
		}
	}
}

// TestSetup_Codex_HelperFailureExplicitlyLogged guards that every codex hook
// command captures `acd hook-stdin-extract` exit explicitly: when the helper
// fails (binary missing, oversized stdin, bad JSON), the snippet must
//   - log a `cmd=acd-hook-stdin-extract` line to LOG before any subsequent
//     command runs (so the failure cause is visible);
//   - capture rc=$? immediately after each guarded command so the printed
//     `exit=%d` is the real failure code rather than an exit code clobbered
//     by an intervening `$(date +...)` substitution; the regression target
//     is rendered exit=0 hiding a real exit=7 from `acd start`.
//
// Regression target: P1-8 (codex SessionStart silently swallowed helper
// failure when previously fed via process substitution + read).
func TestSetup_Codex_HelperFailureExplicitlyLogged(t *testing.T) {
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
	for ev, entries := range settings.Hooks {
		for i, entry := range entries {
			for j, h := range entry.Hooks {
				cmd := h.Command
				// New shape: capture helper output via OUT=$(...) so we can
				// distinguish helper failure from start failure. The old
				// shape used `{ read -r SID; read -r CWD; } < <(...) || exit 0`
				// which dropped helper exit on the floor.
				if strings.Contains(cmd, "< <(acd internal integration stdin-extract") {
					t.Errorf("%s entry %d hook %d: must not use process substitution + read for helper (drops helper exit): %s", ev, i, j, cmd)
				}
				if !strings.Contains(cmd, "OUT=$(acd internal integration stdin-extract") {
					t.Errorf("%s entry %d hook %d: must capture helper output via OUT=$(acd hook-stdin-extract ...): %s", ev, i, j, cmd)
				}
				// Helper failure path must log with cmd=acd-hook-stdin-extract
				// (so corrupt-DB vs missing-binary cases are distinguishable
				// in the harness log).
				if !strings.Contains(cmd, "cmd=acd-hook-stdin-extract") {
					t.Errorf("%s entry %d hook %d: helper failure branch must record cmd=acd-hook-stdin-extract: %s", ev, i, j, cmd)
				}
				// Every failure branch must capture rc immediately so an
				// intervening $(date) substitution does not clobber $?
				// before printf reads it. SessionStart, UserPromptSubmit,
				// PreToolUse, PostToolUse have two failure branches (helper
				// + start/wake); Stop has one (helper only) because the
				// trailing `acd touch --soft-boundary` is best-effort with
				// no log line.
				wantRC := 2
				if ev == "Stop" {
					wantRC = 1
				}
				if got := strings.Count(cmd, "rc=$?"); got < wantRC {
					t.Errorf("%s entry %d hook %d: failure branches must capture rc=$? before printing (got %d, want >= %d): %s", ev, i, j, got, wantRC, cmd)
				}
			}
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
				if !strings.Contains(cmd, "acd internal session open") || !strings.Contains(cmd, "acd internal hint --kind wake") {
					t.Errorf("%s entry %d hook %d: must call both acd start and acd wake: %s", ev, i, j, cmd)
				}
				if strings.Index(cmd, "acd internal session open") > strings.Index(cmd, "acd internal hint --kind wake") {
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
	// README says "~/.config/opencode/hook/hooks.yaml"
	if !strings.Contains(out, ".config/opencode/hook/hooks.yaml") {
		t.Errorf("footer missing '.config/opencode/hook/hooks.yaml' in output:\n%s", out)
	}
}

func TestSetup_OpenCode_ActiveHooksStartBeforeWake(t *testing.T) {
	body := snippetBody(t, "opencode/hooks.snippet.yaml")
	for _, id := range []string{"acd-wake-tool-before", "acd-wake-tool-after"} {
		block := yamlHookBlock(t, body, id)
		if !strings.Contains(block, "acd internal session open") || !strings.Contains(block, "acd internal hint --kind wake") {
			t.Fatalf("%s must run acd start before acd wake:\n%s", id, block)
		}
		if strings.Index(block, "acd internal session open") > strings.Index(block, "acd internal hint --kind wake") {
			t.Fatalf("%s runs acd wake before acd start:\n%s", id, block)
		}
	}
}

// TestSetup_ClaudeCode_StopHookCallsFlushLogical guards the d1 rewire: the
// Claude Code Stop hook must call `acd flush --logical` rather than the
// legacy `acd touch`. The rewire is the whole point of the d1 task — Stop
// fires when Claude finishes a turn, and we want pending captures to
// commit immediately rather than wait the full IntentMaxPendingAge timer.
// Regression target: a future template edit reverting to acd touch would
// silently re-introduce the 5-minute commit lag.
func TestSetup_ClaudeCode_StopHookCallsFlushLogical(t *testing.T) {
	out, _, _ := runSetupCmd(t, "claude-code")
	start := strings.Index(out, "{")
	end := strings.LastIndex(out, "}")
	if start == -1 || end == -1 {
		t.Fatalf("no JSON block")
	}
	jsonBlock := out[start : end+1]

	var settings struct {
		Hooks map[string][]struct {
			Hooks []struct {
				Command string `json:"command"`
			} `json:"hooks"`
		} `json:"hooks"`
	}
	if err := json.Unmarshal([]byte(jsonBlock), &settings); err != nil {
		t.Fatalf("parse JSON: %v", err)
	}
	stop := settings.Hooks["Stop"]
	if len(stop) == 0 || len(stop[0].Hooks) == 0 {
		t.Fatalf("Stop hook missing")
	}
	cmd := stop[0].Hooks[0].Command
	if !strings.Contains(cmd, "acd internal hint --kind logical_boundary") {
		t.Errorf("Stop hook must call `acd flush --logical`, got: %s", cmd)
	}
	if strings.Contains(cmd, "acd touch") {
		t.Errorf("Stop hook still calls legacy `acd touch` — rewire incomplete: %s", cmd)
	}
	// Same fail-soft pattern as the legacy touch body: stderr lands in $LOG.
	if !strings.Contains(cmd, `2>>"$LOG"`) {
		t.Errorf("Stop hook missing stderr->LOG redirect: %s", cmd)
	}
}

// TestSetup_OpenCode_IdleHookCallsFlushLogical mirrors the claude-code rewire
// guard for the OpenCode session.idle event: it must call `acd flush
// --logical` (under the new acd-flush-idle id) instead of the legacy
// acd-touch-idle / `acd touch` body.
func TestSetup_OpenCode_IdleHookCallsFlushLogical(t *testing.T) {
	body := snippetBody(t, "opencode/hooks.snippet.yaml")
	// YAML harnesses use a comment-prefixed marker `# acd-managed: true`
	// at the top of the snippet. The `# ` prefix is load-bearing — a
	// hand-edited bare `acd-managed: true` is intentionally NOT
	// detected (see internal/cli/doctor.go). Catch a regression that
	// drops or alters the marker on the OpenCode YAML template.
	const yamlMarker = "# acd-managed: true"
	if !strings.Contains(body, yamlMarker) {
		t.Errorf("opencode snippet missing %q marker:\n%s", yamlMarker, body)
	}
	if strings.Contains(body, "- id: acd-touch-idle") {
		t.Errorf("opencode snippet still has legacy `acd-touch-idle` id; expected `acd-flush-idle`:\n%s", body)
	}
	block := yamlHookBlock(t, body, "acd-flush-idle")
	if !strings.Contains(block, "acd internal hint --kind logical_boundary") {
		t.Errorf("opencode acd-flush-idle must call `acd flush --logical`:\n%s", block)
	}
	if !strings.Contains(block, "session.idle") {
		t.Errorf("opencode acd-flush-idle must bind to session.idle event:\n%s", block)
	}
}

// TestSetup_Pi_IdleHookCallsFlushLogical mirrors the OpenCode case for Pi.
func TestSetup_Pi_IdleHookCallsFlushLogical(t *testing.T) {
	body := snippetBody(t, "pi/hooks.snippet.yaml")
	// Same YAML marker contract as opencode (P2 #18).
	const yamlMarker = "# acd-managed: true"
	if !strings.Contains(body, yamlMarker) {
		t.Errorf("pi snippet missing %q marker:\n%s", yamlMarker, body)
	}
	if strings.Contains(body, "- id: acd-touch-idle") {
		t.Errorf("pi snippet still has legacy `acd-touch-idle` id; expected `acd-flush-idle`:\n%s", body)
	}
	block := yamlHookBlock(t, body, "acd-flush-idle")
	if !strings.Contains(block, "acd internal hint --kind logical_boundary") {
		t.Errorf("pi acd-flush-idle must call `acd flush --logical`:\n%s", block)
	}
	if !strings.Contains(block, "session.idle") {
		t.Errorf("pi acd-flush-idle must bind to session.idle event:\n%s", block)
	}
}

// TestSetup_OpenCode_AllHooksGateMkdir guards that every opencode YAML hook
// body gates `mkdir -p` behind a `[ -d "$LOG_DIR" ]` check, mirroring the
// codex template invariant. Unconditional `mkdir -p` on every hook event
// fork+execs an extra subprocess on the hot path; the gate elides that
// when the log dir already exists. Regression target: P2-20 (parity gap
// between codex and opencode/pi snippets).
func TestSetup_OpenCode_AllHooksGateMkdir(t *testing.T) {
	body := snippetBody(t, "opencode/hooks.snippet.yaml")
	for _, id := range []string{"acd-start", "acd-wake-tool-before", "acd-wake-tool-after", "acd-flush-idle", "acd-stop"} {
		block := yamlHookBlock(t, body, id)
		if !strings.Contains(block, "mkdir -p") {
			t.Errorf("%s: snippet must mkdir LOG_DIR before logging:\n%s", id, block)
		}
		if !strings.Contains(block, `[ -d "$LOG_DIR" ] || mkdir -p`) {
			t.Errorf("%s: mkdir -p must be gated by [ -d \"$LOG_DIR\" ] || mkdir -p:\n%s", id, block)
		}
	}
}

// TestSetup_Pi_AllHooksGateMkdir mirrors the opencode case for the Pi
// template — every Pi YAML hook body must gate mkdir -p behind a
// directory-exists check.
func TestSetup_Pi_AllHooksGateMkdir(t *testing.T) {
	body := snippetBody(t, "pi/hooks.snippet.yaml")
	for _, id := range []string{"acd-start", "acd-wake-tool-before", "acd-wake-tool-after", "acd-flush-idle", "acd-stop"} {
		block := yamlHookBlock(t, body, id)
		if !strings.Contains(block, "mkdir -p") {
			t.Errorf("%s: snippet must mkdir LOG_DIR before logging:\n%s", id, block)
		}
		if !strings.Contains(block, `[ -d "$LOG_DIR" ] || mkdir -p`) {
			t.Errorf("%s: mkdir -p must be gated by [ -d \"$LOG_DIR\" ] || mkdir -p:\n%s", id, block)
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
	// README says ".pi/agent/hook/hooks.yaml"
	if !strings.Contains(out, ".pi/agent/hook/hooks.yaml") {
		t.Errorf("footer missing '.pi/agent/hook/hooks.yaml' in output:\n%s", out)
	}
}

func TestSetup_Pi_ActiveHooksStartBeforeWakeAndSessionFallbackIsStable(t *testing.T) {
	body := snippetBody(t, "pi/hooks.snippet.yaml")
	if strings.Contains(body, "uuidgen") {
		t.Fatalf("pi snippet must not create one-off session ids with uuidgen:\n%s", body)
	}
	// CLAUDE.md / P2-14 requires the SID fallback to be unique per process
	// when PI_SESSION_ID is unset, so concurrent Pi sessions do not collapse
	// onto a single shared "unknown" client (which would let the first
	// session.deleted tear down the daemon while sibling sessions stay
	// active). The fallback is `pi-$$-$(date +%s)`: $$ is the shell pid
	// and date is in seconds, so every hook process gets a unique id
	// without pulling in uuidgen.
	for _, id := range []string{"acd-wake-tool-before", "acd-wake-tool-after"} {
		block := yamlHookBlock(t, body, id)
		if !strings.Contains(block, "acd internal session open") || !strings.Contains(block, "acd internal hint --kind wake") {
			t.Fatalf("%s must run acd start before acd wake:\n%s", id, block)
		}
		if strings.Index(block, "acd internal session open") > strings.Index(block, "acd internal hint --kind wake") {
			t.Fatalf("%s runs acd wake before acd start:\n%s", id, block)
		}
		if !strings.Contains(block, `SID="${PI_SESSION_ID:-pi-$$-$(date +%s)}"`) || !strings.Contains(block, `--session-id "$SID"`) {
			t.Fatalf("%s must use the per-process unique SID fallback:\n%s", id, block)
		}
		// Must not regress to the shared "unknown" placeholder.
		if strings.Contains(block, `PI_SESSION_ID:-unknown`) {
			t.Fatalf("%s reverted to shared 'unknown' SID — concurrent Pi sessions would collapse:\n%s", id, block)
		}
	}
}

// TestSetup_Pi_AllHooksUsePerProcessSIDFallback guards every Pi hook id
// (start, wake-before, wake-after, idle-touch, stop) carries the same
// per-process unique fallback. Regression target: P2-14 — if any single
// hook keeps the legacy `unknown` literal, two concurrent Pi sessions with
// neither setting PI_SESSION_ID would still collapse on that event.
func TestSetup_Pi_AllHooksUsePerProcessSIDFallback(t *testing.T) {
	body := snippetBody(t, "pi/hooks.snippet.yaml")
	for _, id := range []string{"acd-start", "acd-wake-tool-before", "acd-wake-tool-after", "acd-flush-idle", "acd-stop"} {
		block := yamlHookBlock(t, body, id)
		if !strings.Contains(block, `SID="${PI_SESSION_ID:-pi-$$-$(date +%s)}"`) {
			t.Errorf("%s: must use per-process unique SID fallback (pi-$$-$(date +%%s)):\n%s", id, block)
		}
	}
}

// TestPiSIDFallbackProducesUniqueIDsAcrossProcesses runs the bash fallback
// expression in two distinct shells and asserts they produce different IDs
// when PI_SESSION_ID is unset. This catches a future regression where the
// fallback drifts to a shared literal and concurrent sessions would
// collapse onto the same client row.
func TestPiSIDFallbackProducesUniqueIDsAcrossProcesses(t *testing.T) {
	bash, err := exec.LookPath("bash")
	if err != nil {
		t.Skip("bash not on PATH; skipping process-uniqueness check")
	}
	// Use the exact fallback expression from the snippet so any drift in
	// templates/pi/hooks.snippet.yaml surfaces here too.
	expr := `unset PI_SESSION_ID; SID="${PI_SESSION_ID:-pi-$$-$(date +%s)}"; printf '%s' "$SID"`
	out1, err := exec.Command(bash, "-c", expr).Output()
	if err != nil {
		t.Fatalf("first bash invocation: %v", err)
	}
	// Sleep just long enough that even if both shells happened to share a
	// second boundary, we see distinct seconds in the second pid run too.
	// $$ alone (different pid per process) is enough; the date guard is
	// belt-and-suspenders against a degenerate scheduling case.
	out2, err := exec.Command(bash, "-c", expr).Output()
	if err != nil {
		t.Fatalf("second bash invocation: %v", err)
	}
	id1, id2 := string(out1), string(out2)
	if id1 == id2 {
		t.Fatalf("two bash processes produced identical SIDs: %q == %q", id1, id2)
	}
	for _, id := range []string{id1, id2} {
		if !strings.HasPrefix(id, "pi-") {
			t.Errorf("SID %q missing pi- prefix", id)
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

// TestSetup_Shell_RawHasSeparatorAndParses verifies that `acd setup shell
// --raw` emits a blank-line separator between the direnv and zshrc
// snippets and that the concatenated body parses as bash. Regression
// target: P3-21 (shell --raw output must be safe to redirect into a
// startup file even if either snippet's last line lacks a trailing
// newline).
func TestSetup_Shell_RawHasSeparatorAndParses(t *testing.T) {
	out, _, err := runSetupCmd(t, "shell", "--raw")
	if err != nil {
		t.Fatalf("acd setup shell --raw exit=%v\nout:\n%s", err, out)
	}
	// Both snippets must be present.
	direnv := snippetBody(t, "shell/direnv.envrc.snippet")
	zshrc := snippetBody(t, "shell/zshrc.snippet.sh")
	if !strings.Contains(out, strings.TrimSpace(direnv)) {
		t.Errorf("--raw output missing direnv snippet:\n%s", out)
	}
	if !strings.Contains(out, strings.TrimSpace(zshrc)) {
		t.Errorf("--raw output missing zshrc snippet:\n%s", out)
	}
	// There must be at least one blank line between the two snippets so
	// the boundary is unambiguous regardless of trailing-newline policy.
	// We anchor on the zshrc snippet's first line and look for "\n\n"
	// preceding it.
	zshrcStart := strings.Index(out, "# acd-managed: true\n# Add to ~/.zshrc")
	if zshrcStart < 0 {
		t.Fatalf("--raw output: zshrc anchor not found:\n%s", out)
	}
	prefix := out[:zshrcStart]
	if !strings.HasSuffix(prefix, "\n\n") {
		t.Errorf("--raw output must have a blank-line separator before zshrc snippet, got prefix tail %q", tailString(prefix, 20))
	}
	// The concatenated body must parse as bash.
	bash, err := exec.LookPath("bash")
	if err != nil {
		t.Skip("bash not on PATH; skipping syntax check")
	}
	cmd := exec.Command(bash, "-n", "-c", out)
	if combined, err := cmd.CombinedOutput(); err != nil {
		t.Errorf("bash -n on concatenated --raw shell snippet failed: %v\n%s", err, combined)
	}
}

func tailString(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[len(s)-n:]
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

// assertActiveHookAndChainAndLogFallback verifies that an active hook
// command chains `acd start` and `acd wake` with logical-and so a failed
// start is not masked by a successful wake, and that the trailing
// or-clause writes the failure cause into logFile and exits nonzero so the
// harness can surface it. Used by JSON-bodied harnesses (claude-code,
// codex) where the bash body lives inside a JSON string.
func assertActiveHookAndChainAndLogFallback(t *testing.T, ev string, i, j int, cmd, logFile string) {
	t.Helper()
	// Order: acd start before acd wake.
	startIdx := strings.Index(cmd, "acd internal session open")
	wakeIdx := strings.Index(cmd, "acd internal hint --kind wake")
	if startIdx < 0 || wakeIdx < 0 {
		t.Errorf("%s entry %d hook %d: must call both acd start and acd wake: %s", ev, i, j, cmd)
		return
	}
	if startIdx > wakeIdx {
		t.Errorf("%s entry %d hook %d: acd start must precede acd wake: %s", ev, i, j, cmd)
	}
	// Logical-and chain between start and wake (no plain `;` masking exit).
	chain := cmd[startIdx:wakeIdx]
	if !strings.Contains(chain, "&&") {
		t.Errorf("%s entry %d hook %d: acd start and acd wake must be chained with &&, got: %s", ev, i, j, chain)
	}
	// LOG file path appears.
	if !strings.Contains(cmd, logFile) {
		t.Errorf("%s entry %d hook %d: missing LOG path %q in: %s", ev, i, j, logFile, cmd)
	}
	// Trailing or-clause that writes failure cause and exits nonzero.
	// Either pattern: `|| { printf ... ; exit 1; }` (JSON-escaped) or `|| {`.
	if !strings.Contains(cmd, "|| {") {
		t.Errorf("%s entry %d hook %d: missing tail or-clause `|| { ... ; exit 1; }`: %s", ev, i, j, cmd)
	}
	if !strings.Contains(cmd, "exit 1") {
		t.Errorf("%s entry %d hook %d: failure branch must exit nonzero: %s", ev, i, j, cmd)
	}
	// Failure cause goes to LOG.
	if !strings.Contains(cmd, ">>\\\"$LOG\\\"") && !strings.Contains(cmd, `>>"$LOG"`) {
		t.Errorf("%s entry %d hook %d: failure must be appended to $LOG: %s", ev, i, j, cmd)
	}
}

// assertYAMLActiveHookAndChainAndLogFallback is the YAML/block-scalar
// counterpart for opencode and pi snippets.
func assertYAMLActiveHookAndChainAndLogFallback(t *testing.T, id, block, logFile string) {
	t.Helper()
	startIdx := strings.Index(block, "acd internal session open")
	wakeIdx := strings.Index(block, "acd internal hint --kind wake")
	if startIdx < 0 || wakeIdx < 0 {
		t.Errorf("%s: must call both acd start and acd wake:\n%s", id, block)
		return
	}
	if startIdx > wakeIdx {
		t.Errorf("%s: acd start must precede acd wake:\n%s", id, block)
	}
	chain := block[startIdx:wakeIdx]
	if !strings.Contains(chain, "&&") {
		t.Errorf("%s: acd start and acd wake must be chained with &&:\n%s", id, chain)
	}
	if !strings.Contains(block, logFile) {
		t.Errorf("%s: missing LOG path %q:\n%s", id, logFile, block)
	}
	if !strings.Contains(block, "|| {") {
		t.Errorf("%s: missing tail or-clause `|| { ... ; exit 1 }`:\n%s", id, block)
	}
	if !strings.Contains(block, "exit 1") {
		t.Errorf("%s: failure branch must exit nonzero:\n%s", id, block)
	}
	if !strings.Contains(block, `>>"$LOG"`) {
		t.Errorf("%s: failure must be appended to $LOG:\n%s", id, block)
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
	if !strings.Contains(stderr, "unknown flag: --apply") {
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
	if !strings.Contains(out, "merge_integration: claude-code") {
		t.Errorf("transactional plan did not include detected claude-code integration:\n%s", out)
	}
}

func TestSetup_NoArg_AutoDetectsRepoLocalCodex(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)

	repo := t.TempDir()
	if out, err := exec.Command("git", "-C", repo, "init").CombinedOutput(); err != nil {
		t.Fatalf("git init: %v: %s", err, out)
	}
	if out, err := exec.Command("git", "-C", repo, "symbolic-ref", "HEAD", "refs/heads/main").CombinedOutput(); err != nil {
		t.Fatalf("set main: %v: %s", err, out)
	}
	hooks := filepath.Join(repo, ".codex", "hooks.json")
	if err := os.MkdirAll(filepath.Dir(hooks), 0o700); err != nil {
		t.Fatalf("mkdir .codex: %v", err)
	}
	if err := os.WriteFile(hooks, []byte(`{"hooks":{"PreToolUse":[{"hooks":[{"type":"command","command":"acd hook-stdin-extract session_id cwd? && acd start --harness codex && acd wake"}]}]}}`), 0o600); err != nil {
		t.Fatalf("write hooks.json: %v", err)
	}
	chdirForTest(t, repo)

	out, stderr, err := runSetupCmd(t)
	if err != nil {
		t.Fatalf("expected repo-local codex auto-detect to exit 0, got: %v\nstderr:\n%s", err, stderr)
	}
	if strings.Contains(out, "merge_integration: codex") {
		t.Errorf("transactional setup must not treat repo-local hooks as a user-level owned integration:\n%s", out)
	}
}

func TestSetup_NoArg_MultipleDetectedListsHarnesses(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)

	files := map[string]string{
		filepath.Join(home, ".claude", "settings.json"): `{"_acd_managed": true}`,
		filepath.Join(home, ".codex", "hooks.json"):     `{"hooks":{}}`,
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
	if err != nil {
		t.Fatalf("transactional setup should merge all detected integrations: %v\nstderr:\n%s", err, stderr)
	}
	for _, want := range []string{"claude-code", "codex"} {
		if !strings.Contains(out, "merge_integration: "+want) {
			t.Errorf("multi-detect plan missing %q: %q", want, out)
		}
	}
}

// --- alias tests ------------------------------------------------------------

// TestSetup_InitAliasStillWorks verifies that invoking the command via the
// hidden "init" compatibility command still produces the snippet on stdout and emits a deprecation
// warning on stderr.
func TestSetup_InitAliasStillWorks(t *testing.T) {
	var outBuf, errBuf bytes.Buffer
	// Simulate the user running: acd init claude-code
	// Build the real root so Cobra resolves the hidden compatibility command.
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
	if !strings.Contains(stderr, "compatibility alias") {
		t.Errorf("init alias did not emit deprecation warning on stderr.\ngot stderr: %q", stderr)
	}
}

// TestSetup_HelpHidesInitAlias verifies that the root --help output contains
// "acd setup" in the Setup section and does not list "acd init" as a separate
// visible command row.
//
// The compatibility command remains callable but is hidden from both root and
// setup help. This test guards the root-help half of that contract.
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
	if !strings.Contains(got, "setup       Install or upgrade ACD transactionally") {
		t.Errorf("root help missing 'acd setup' in Setup section:\n%s", got)
	}

	// There must be no standalone "acd init   Print harness install snippets"
	// row — the alias must not appear as a separate entry in the Setup table.
	if strings.Contains(got, "acd init   ") {
		t.Errorf("root help contains a standalone 'acd init' row; alias should be hidden from root help listing:\n%s", got)
	}
}

func TestSetup_HelpHidesCompatibilitySyntax(t *testing.T) {
	cmd := newSetupCmd()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetArgs([]string{"--help"})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("setup help: %v", err)
	}
	if got := out.String(); strings.Contains(got, "Aliases:") || strings.Contains(got, "[harness]") {
		t.Fatalf("setup help exposed compatibility syntax:\n%s", got)
	}
}

func TestSetupProgressShowsPhaseAndHeartbeat(t *testing.T) {
	var out bytes.Buffer
	progress := newSetupProgress(&out, false, 5*time.Millisecond)
	progress.Update(installer.Progress{Phase: "bridge", Detail: "Scanning registered repositories"})
	time.Sleep(25 * time.Millisecond)
	progress.Close()

	got := out.String()
	if !strings.Contains(got, "Setup: Scanning registered repositories\n") {
		t.Fatalf("missing phase progress: %q", got)
	}
	if !strings.Contains(got, "Setup: still working on scanning registered repositories") {
		t.Fatalf("missing heartbeat: %q", got)
	}
}

func TestSetupProgressIsSilentForQuietOrJSON(t *testing.T) {
	var out bytes.Buffer
	progress := newSetupProgress(&out, true, time.Millisecond)
	progress.Update(installer.Progress{Phase: "bridge", Detail: "Scanning registered repositories"})
	progress.Close()
	if out.Len() != 0 {
		t.Fatalf("silent progress wrote %q", out.String())
	}
}

func TestSetupAccessDenialRequiresUserAction(t *testing.T) {
	err := classifySetupApplyError(&installer.ServiceAccessError{
		Target: "/Users/test/Desktop/project", ManagedBinary: "/Users/test/.local/share/acd/bin/acd",
		Cause: errors.New("background read timed out"),
	})
	var commandErr *CommandError
	if !errors.As(err, &commandErr) {
		t.Fatalf("error=%v, want CommandError", err)
	}
	if commandErr.Exit != ExitActionRequired || commandErr.Code != "service_access_required" {
		t.Fatalf("command error=%+v", commandErr)
	}
	for _, required := range []string{"Full Disk Access", "/Users/test/.local/share/acd/bin/acd", "rerun `acd setup`"} {
		if !strings.Contains(commandErr.Message, required) {
			t.Fatalf("message missing %q: %s", required, commandErr.Message)
		}
	}
}

func TestSetupServiceAccessPromptRetriesOrCancels(t *testing.T) {
	accessErr := &installer.ServiceAccessError{
		Target: "/Users/test/Desktop/project", ManagedBinary: "/Users/test/.local/share/acd/bin/acd",
	}
	var out bytes.Buffer
	if err := promptSetupServiceAccess(context.Background(), &out,
		bufio.NewReader(strings.NewReader("\n")), accessErr); err != nil {
		t.Fatalf("retry prompt: %v", err)
	}
	for _, required := range []string{"Full Disk Access", "Command-Shift-G", accessErr.ManagedBinary} {
		if !strings.Contains(out.String(), required) {
			t.Fatalf("prompt missing %q: %s", required, out.String())
		}
	}
	if err := promptSetupServiceAccess(context.Background(), io.Discard,
		bufio.NewReader(strings.NewReader("cancel\n")), accessErr); err == nil {
		t.Fatal("cancel unexpectedly retried")
	}
}
