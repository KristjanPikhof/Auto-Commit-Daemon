package cli

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/installer"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/paths"
)

func TestSetupAgentsPreviewAndManualLeaveFilesUnchanged(t *testing.T) {
	for _, flags := range [][]string{{}, {"--dry-run"}, {"--manual"}, {"--manual", "--yes"}, {"--dry-run", "--yes"}, {"--json"}} {
		t.Run(strings.Join(flags, " "), func(t *testing.T) {
			roots := withIsolatedHome(t)
			path := filepath.Join(t.TempDir(), "AGENTS.md")
			before := []byte("# My instructions\nKeep this text.\n")
			if err := os.WriteFile(path, before, 0o640); err != nil {
				t.Fatal(err)
			}
			var out bytes.Buffer
			cmd := newRootCmd()
			cmd.SetOut(&out)
			cmd.SetErr(&out)
			cmd.SetArgs(append([]string{"setup", "agents", "--integrations=codex", "--instructions-file", path}, flags...))
			if err := cmd.Execute(); err != nil {
				t.Fatal(err)
			}
			for _, want := range []string{"acd status", "acd finish", "--dry-run", "--yes", "acd setup codex --raw"} {
				if !strings.Contains(out.String(), want) {
					t.Errorf("missing manual guidance %q: %s", want, out.String())
				}
			}
			if strings.Contains(strings.Join(flags, " "), "--json") {
				var envelope productEnvelope
				if err := json.Unmarshal(out.Bytes(), &envelope); err != nil || envelope.Changed {
					t.Fatalf("preview JSON: %s: %v", out.String(), err)
				}
			}
			after, err := os.ReadFile(path)
			if err != nil || !bytes.Equal(before, after) {
				t.Fatalf("preview modified instructions: %v", err)
			}
			for _, unexpected := range []string{roots.RegistryPath(), roots.ManagedBinaryPath(), roots.IntegrationsOwnershipPath()} {
				if _, err := os.Stat(unexpected); !os.IsNotExist(err) {
					t.Fatalf("preview wrote %s: %v", unexpected, err)
				}
			}
		})
	}
}

func TestSetupAgentsInstructionFilesPreserveContentAndSymlinks(t *testing.T) {
	for _, scenario := range []string{"new", "append", "symlink", "managed", "unmanaged", "changed", "moved_symlink", "foreign_lock", "broken_markers"} {
		t.Run(scenario, func(t *testing.T) {
			roots := withIsolatedHome(t)
			dir := t.TempDir()
			path, target := filepath.Join(dir, "AGENTS.md"), filepath.Join(dir, "AGENTS.md")
			before := "# Project rules\nKeep this byte-for-byte.\n"
			guidance := snippetBody(t, "agent-guidance.md")
			if scenario == "managed" {
				before += agentInstructionsStart + "\nOld ACD guidance\n" + agentInstructionsEnd + "\nKeep the footer.\n"
			}
			if scenario == "unmanaged" {
				before += "## Commits: ACD\nMy existing ACD rules.\n"
			}
			if scenario == "broken_markers" {
				before += agentInstructionsStart
			}
			if scenario == "symlink" || scenario == "moved_symlink" {
				target = filepath.Join(dir, "CLAUDE.md")
				if err := os.Symlink("CLAUDE.md", path); err != nil {
					t.Fatal(err)
				}
			}
			if scenario != "new" {
				if err := os.WriteFile(target, []byte(before), 0o640); err != nil {
					t.Fatal(err)
				}
			}
			plan, err := planAgentInstructions(path, guidance)
			if scenario == "broken_markers" {
				if err == nil {
					t.Fatal("ambiguous instructions accepted")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if scenario == "changed" {
				if err := os.WriteFile(path, []byte("New user edits\n"), 0o640); err != nil {
					t.Fatal(err)
				}
			}
			if scenario == "moved_symlink" {
				if err := os.Remove(path); err != nil {
					t.Fatal(err)
				}
				other := filepath.Join(dir, "other.md")
				if err := os.WriteFile(other, []byte(before), 0o640); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink("other.md", path); err != nil {
					t.Fatal(err)
				}
			}
			if scenario == "foreign_lock" {
				if err := os.WriteFile(target+".acd.lock", []byte("another owner"), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			backup, err := plan.apply(roots)
			if scenario == "changed" || scenario == "moved_symlink" || scenario == "foreign_lock" {
				if err == nil {
					t.Fatal("stale preview overwritten")
				}
				if scenario == "foreign_lock" {
					body, err := os.ReadFile(target + ".acd.lock")
					if err != nil || string(body) != "another owner" {
						t.Fatalf("foreign lock changed: %q: %v", body, err)
					}
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			after, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			if scenario == "unmanaged" {
				if string(after) != before || backup != "" || plan.Changed {
					t.Fatal("unowned guidance was changed or duplicated")
				}
				return
			}
			if !strings.Contains(string(after), guidance) || strings.Count(string(after), agentInstructionsStart) != 1 {
				t.Fatalf("missing or duplicate guidance: %s", after)
			}
			if scenario != "new" {
				saved, err := os.ReadFile(backup)
				if err != nil || string(saved) != before || !bytes.HasPrefix(after, []byte("# Project rules\nKeep this byte-for-byte.\n")) {
					t.Fatalf("user instructions or backup lost: %v", err)
				}
				info, _ := os.Stat(target)
				if info.Mode().Perm() != 0o640 {
					t.Fatal("instruction permissions changed")
				}
			}
			if scenario == "symlink" {
				if link, err := os.Readlink(path); err != nil || link != "CLAUDE.md" {
					t.Fatalf("symlink replaced: %s: %v", link, err)
				}
			}
			if scenario == "managed" && (!strings.Contains(string(after), "Keep the footer.") || strings.Contains(string(after), "Old ACD guidance")) {
				t.Fatal("managed section update lost the footer")
			}
			again, err := planAgentInstructions(path, guidance)
			if err != nil || again.Changed {
				t.Fatalf("repeated setup is not idempotent: %+v: %v", again, err)
			}
		})
	}
}

func TestSetupAgentsPreservesInstructionEditsDuringHookSetup(t *testing.T) {
	withIsolatedHome(t)
	path := filepath.Join(t.TempDir(), "AGENTS.md")
	if err := os.WriteFile(path, []byte("Original user instructions\n"), 0o640); err != nil {
		t.Fatal(err)
	}
	original := applySetupAgentsHooks
	t.Cleanup(func() { applySetupAgentsHooks = original })
	applySetupAgentsHooks = func(context.Context, paths.Roots, installer.Plan) (installer.Result, error) {
		return installer.Result{Changed: true}, os.WriteFile(path, []byte("New user edits\n"), 0o640)
	}
	cmd := newRootCmd()
	cmd.SetOut(&bytes.Buffer{})
	cmd.SetErr(&bytes.Buffer{})
	cmd.SetArgs([]string{"setup", "agents", "--integrations=none", "--instructions-file", path, "--yes", "--json"})
	if err := cmd.Execute(); err == nil || !strings.Contains(err.Error(), "hooks were configured, but instructions were not updated") {
		t.Fatalf("partial completion was not reported: %v", err)
	}
	if body, err := os.ReadFile(path); err != nil || string(body) != "New user edits\n" {
		t.Fatalf("later instructions overwritten: %q: %v", body, err)
	}
}

func TestSetupAgentsApplyAndManualProtectionConsent(t *testing.T) {
	roots := withIsolatedHome(t)
	working := t.TempDir()
	t.Chdir(working)
	repo, _, _ := makeSeededRepoStateDB(t)
	path := filepath.Join(repo, "AGENTS.md")
	original := applySetupAgentsHooks
	t.Cleanup(func() { applySetupAgentsHooks = original })
	calls := 0
	applySetupAgentsHooks = func(_ context.Context, _ paths.Roots, plan installer.Plan) (installer.Result, error) {
		calls++
		if len(plan.Integrations) != 1 || plan.Integrations[0] != "codex" {
			t.Fatalf("wrong hook plan: %+v", plan.Integrations)
		}
		return installer.Result{Changed: true}, nil
	}
	var out bytes.Buffer
	cmd := newRootCmd()
	cmd.SetOut(&out)
	cmd.SetArgs([]string{"setup", "agents", "--repo", repo, "--integrations=codex", "--instructions-file", "AGENTS.md", "--yes", "--json"})
	if err := cmd.Execute(); err != nil || calls != 1 {
		t.Fatalf("apply: %s: %v", out.String(), err)
	}
	var envelope productEnvelope
	if err := json.Unmarshal(out.Bytes(), &envelope); err != nil || !envelope.Changed {
		t.Fatalf("apply JSON: %s: %v", out.String(), err)
	}
	if body, err := os.ReadFile(path); err != nil || !strings.Contains(string(body), "acd finish") {
		t.Fatalf("instruction apply failed: %v", err)
	}
	if _, err := os.Stat(roots.RegistryPath()); !os.IsNotExist(err) {
		t.Fatal("guide implicitly enabled repository protection")
	}
	if _, err := os.Stat(filepath.Join(working, "AGENTS.md")); !os.IsNotExist(err) {
		t.Fatal("guide wrote instructions outside the selected repository")
	}
}

func TestSetupAgentsChoicesDefaultToCopyAndNoProtection(t *testing.T) {
	withIsolatedHome(t)
	cmd := newSetupAgentsCmd()
	cmd.SetErr(&bytes.Buffer{})
	options := setupAgentsOptions{}
	if err := chooseAgentSetup(cmd, bufio.NewReader(strings.NewReader("codex,cursor\ncopy\nn\n")), &options, "."); err != nil {
		t.Fatal(err)
	}
	if options.Integrations != "codex,cursor" || options.InstructionsFile != "" || options.Protect {
		t.Fatalf("unexpected consent: %+v", options)
	}
}

func TestSetupAgentsEOFAndDetectionBeforeHooksExist(t *testing.T) {
	withIsolatedHome(t)
	home, err := os.UserHomeDir()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(home, ".codex"), 0o700); err != nil {
		t.Fatal(err)
	}
	if names := detectedAgentTools(); !strings.Contains(strings.Join(names, ","), "codex") {
		t.Fatalf("new Codex install was not detected: %v", names)
	}
	var out bytes.Buffer
	if _, err := agentSetupAnswer(bufio.NewReader(strings.NewReader("")), &out, "Apply?", "N"); err == nil {
		t.Fatal("EOF silently accepted a default choice")
	}
}
