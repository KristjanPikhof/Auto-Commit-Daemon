//go:build integration

package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestSetupAgentsGuidedDeclineAndApply(t *testing.T) {
	f := newFinishFixture(t)
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
	f := newFinishFixture(t)
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
	if status.ExitCode != 0 || !strings.Contains(status.Stdout, `"protected": true`) {
		t.Fatalf("protection not confirmed: %+v", status)
	}
}
