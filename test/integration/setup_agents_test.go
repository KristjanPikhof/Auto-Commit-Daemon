//go:build integration

package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

func TestSetupAgentsGuidedDeclineAndApply(t *testing.T) {
	f := newAgentSetupFixture(t)
	home := envValue(f.Env, "HOME")
	hooks := filepath.Join(home, ".codex", "hooks.json")
	instructions := filepath.Join(f.Repo, "AGENTS.md")
	before := "# Project instructions\nKeep these rules.\n"
	writeFile(t, instructions, before)
	// Declining in a real terminal must preserve hooks and instruction files.
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	declined := runPTYCommand(t, ctx, f.Env, 100, 30, 0, 0, "codex\n\x00copy\n\x00n\n\x00n\n\x00", f.Bin, "setup", "agents", "--repo", f.Repo)
	if declined.ExitCode != 0 || !strings.Contains(declined.Stdout, "Copy-ready AGENTS.md section") || !strings.Contains(declined.Stdout, "Preview only. No changes applied.") {
		t.Fatalf("guided decline: %+v", declined)
	}
	if _, err := os.Stat(hooks); !os.IsNotExist(err) {
		t.Fatalf("declined guide wrote hooks: %v", err)
	}
	// Regular setup keeps only a pointer to agent setup, including JSON mode.
	regular := f.Run(t, t.Context(), "setup", "--integrations=none", "--yes", "--json")
	if regular.ExitCode != 0 || strings.Contains(regular.Stdout, "recommended_agent_instructions") || strings.Contains(regular.Stdout, "## Commits: ACD") {
		t.Fatalf("regular setup still carries the instruction copy: %+v", regular)
	}
	preview := f.Run(t, t.Context(), "setup", "agents", "--integrations=codex", "--instructions-file", instructions, "--dry-run", "--json")
	if preview.ExitCode != 0 || !strings.Contains(preview.Stdout, "recommended_agent_instructions") || strings.Contains(preview.Stdout, "Keep these rules.") {
		t.Fatalf("agent preview missing guidance or leaking existing instructions: %+v", preview)
	}
	if body, err := os.ReadFile(instructions); err != nil || string(body) != before {
		t.Fatalf("preview changed instructions: %q %v", body, err)
	}
	if err := os.MkdirAll(filepath.Dir(hooks), 0o700); err != nil {
		t.Fatal(err)
	}
	writeFile(t, hooks, `{"custom":"keep me","hooks":{}}`)
	applied := f.Run(t, t.Context(), "setup", "agents", "--integrations=codex", "--instructions-file", instructions, "--yes", "--json")
	if applied.ExitCode != 0 {
		t.Fatalf("agent apply: %+v", applied)
	}
	var envelope struct {
		OK bool `json:"ok"`
	}
	if err := json.Unmarshal([]byte(applied.Stdout), &envelope); err != nil || !envelope.OK {
		t.Fatalf("agent apply JSON: %s: %v", applied.Stdout, err)
	}
	body, err := os.ReadFile(instructions)
	if err != nil || !bytes.HasPrefix(body, []byte(before)) || !bytes.Contains(body, []byte("acd finish")) {
		t.Fatalf("user instructions were not preserved: %q: %v", body, err)
	}
	hookBody, err := os.ReadFile(hooks)
	if err != nil || !bytes.Contains(hookBody, []byte("keep me")) || !bytes.Contains(hookBody, []byte("acd internal integration event")) {
		t.Fatalf("hooks missing or unrelated config lost: %q: %v", hookBody, err)
	}
	repeated := f.Run(t, t.Context(), "setup", "agents", "--integrations=codex", "--instructions-file", instructions, "--yes", "--json")
	if repeated.ExitCode != 0 {
		t.Fatalf("repeated agent setup: %+v", repeated)
	}
	after, err := os.ReadFile(instructions)
	if err != nil || !bytes.Equal(body, after) || strings.Count(string(after), "<!-- acd:agents:start -->") != 1 {
		t.Fatalf("repeated setup duplicated instructions: %q: %v", after, err)
	}
	f.Barrier(t, t.Context())
}

func TestSetupAgentsProtectionRequiresExplicitSelection(t *testing.T) {
	f := newAgentSetupFixture(t)
	repo := tempRepo(t)
	writeFile(t, filepath.Join(repo, "goal.txt"), "new protected work\n")
	preview := runAcd(t, t.Context(), f.Env, "--repo", repo, "setup", "agents", "--integrations=none", "--protect", "--dry-run", "--json")
	if preview.ExitCode != 0 || !strings.Contains(preview.Stdout, "protect_repository") {
		t.Fatalf("protection preview: %+v", preview)
	}
	if _, err := os.Stat(filepath.Join(repo, ".git", "acd", "state.db")); !os.IsNotExist(err) {
		t.Fatalf("protection preview enabled repository: %v", err)
	}
	applied := runAcd(t, t.Context(), f.Env, "--repo", repo, "setup", "agents", "--integrations=none", "--protect", "--yes", "--json")
	if applied.ExitCode != 0 || !strings.Contains(applied.Stdout, `"protection_enabled": true`) {
		t.Fatalf("explicit protection: %+v", applied)
	}
	status := runAcd(t, t.Context(), f.Env, "--repo", repo, "status", "--json")
	var report struct {
		Data struct {
			Enabled        bool   `json:"enabled"`
			ActionRequired bool   `json:"action_required"`
			CheckpointID   string `json:"checkpoint_id"`
		} `json:"data"`
	}
	if err := json.Unmarshal([]byte(status.Stdout), &report); err != nil || status.ExitCode != 0 || !report.Data.Enabled || report.Data.ActionRequired || report.Data.CheckpointID == "" {
		t.Fatalf("protection not confirmed: %+v: %v", status, err)
	}
	// Status may observe the next automatic scan. Prove the enabled repository
	// already has the promised durable checkpoint rather than requiring idle time.
	checkpoint, err := state.ResolveCheckpoint(t.Context(), filepath.Join(repo, ".git", "acd", "state.db"), report.Data.CheckpointID)
	if err != nil {
		t.Fatal(err)
	}
	if body := strings.TrimSpace(runGitOK(t, repo, "show", checkpoint.TreeOID+":goal.txt")); body != "new protected work" {
		t.Fatalf("enabled repository has no saved goal: %q", body)
	}
}

// The fixture owns a real supervisor directly. On Linux, setup must not use
// the host user's systemd instance, which does not share the temporary HOME.
// Only the service-install boundary is simulated; setup still applies hooks,
// instructions, registration, and checkpoint barriers through the real CLI.
func newAgentSetupFixture(t *testing.T) *finishFixture {
	t.Helper()
	f := newFinishFixture(t)
	if runtime.GOOS != "linux" {
		return f
	}
	dir := t.TempDir()
	violations := filepath.Join(dir, "unexpected-systemctl")
	body := `#!/bin/sh
case "$*" in
  '--user is-active --quiet acd-supervisor.service'|'--user is-enabled --quiet acd-supervisor.service')
    exit 1 ;;
  '--user daemon-reload'|'--user enable --now acd-supervisor.service')
    exit 0 ;;
  *)
    printf 'Unexpected fixture systemctl command: %s\n' "$*" >> "$ACD_INTEGRATION_SYSTEMCTL_VIOLATIONS"
    exit 97 ;;
esac
`
	if err := os.WriteFile(filepath.Join(dir, "systemctl"), []byte(body), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if body, err := os.ReadFile(violations); err == nil {
			t.Errorf("unexpected service manager call: %s", body)
		} else if !os.IsNotExist(err) {
			t.Error(err)
		}
	})
	f.Env = envWith(f.Env, "PATH="+dir+string(os.PathListSeparator)+envValue(f.Env, "PATH"), "ACD_INTEGRATION_SYSTEMCTL_VIOLATIONS="+violations)
	return f
}
