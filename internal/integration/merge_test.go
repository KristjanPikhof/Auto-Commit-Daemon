package integration

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/paths"
)

func TestMergeJSONPreservesCustomHooksAndIsIdempotent(t *testing.T) {
	existing := []byte(`{"custom":{"keep":true},"hooks":{"SessionStart":[{"hooks":[{"type":"command","command":"echo custom"}]}]}}`)
	template := []byte(`{"hooks":{"SessionStart":[{"hooks":[{"type":"command","command":"acd internal hint --kind wake"}]}]}}`)
	first, err := mergeJSON(existing, template)
	if err != nil {
		t.Fatal(err)
	}
	second, err := mergeJSON(first, template)
	if err != nil {
		t.Fatal(err)
	}
	if string(first) != string(second) {
		t.Fatalf("merge is not idempotent:\n%s\n%s", first, second)
	}
	var decoded map[string]any
	if err := json.Unmarshal(first, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded["custom"] == nil || strings.Count(string(first), "echo custom") != 1 || strings.Count(string(first), "acd internal hint") != 1 {
		t.Fatalf("merged=%s", first)
	}
}

func TestRemovalPreservesUnrelatedJSONAddedAfterInstall(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(dir, "hooks.json")
	ownership := filepath.Join(dir, "integrations.json")
	template := []byte(`{"hooks":{"SessionStart":[{"command":"acd internal session open --harness codex"}]}}`)
	installed, err := mergeJSON([]byte(`{"custom":{"keep":true}}`), template)
	if err != nil {
		t.Fatal(err)
	}
	elements, err := ownedElements("json", template, "codex")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(target, installed, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := WriteOwnership(ownership, []Plan{{Name: "codex", Target: target, Format: "json", AfterDigest: digest(installed), Owned: elements}}); err != nil {
		t.Fatal(err)
	}
	var document map[string]any
	if err := json.Unmarshal(installed, &document); err != nil {
		t.Fatal(err)
	}
	document["added_later"] = map[string]any{"preserve": true}
	modified, _ := json.MarshalIndent(document, "", "  ")
	if err := os.WriteFile(target, append(modified, '\n'), 0o600); err != nil {
		t.Fatal(err)
	}
	plans, err := BuildRemovalPlans(ownership)
	if err != nil {
		t.Fatal(err)
	}
	if len(plans) != 1 || strings.Contains(string(plans[0].Content), "acd internal") ||
		!strings.Contains(string(plans[0].Content), "added_later") || !strings.Contains(string(plans[0].Content), "custom") {
		t.Fatalf("removal plan=%+v content=%s", plans, plans[0].Content)
	}
}

func TestRemovalRejectsModifiedOwnedJSONElement(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(dir, "hooks.json")
	ownership := filepath.Join(dir, "integrations.json")
	template := []byte(`{"hooks":{"SessionStart":[{"command":"acd internal session open --harness codex"}]}}`)
	elements, _ := ownedElements("json", template, "codex")
	if err := os.WriteFile(target, template, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := WriteOwnership(ownership, []Plan{{Name: "codex", Target: target, Format: "json", AfterDigest: digest(template), Owned: elements}}); err != nil {
		t.Fatal(err)
	}
	changed := strings.Replace(string(template), "session open", "session close", 1)
	if err := os.WriteFile(target, []byte(changed), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := BuildRemovalPlans(ownership); err == nil || !strings.Contains(err.Error(), "modified or removed") {
		t.Fatalf("modified owned entry error=%v", err)
	}
}

func TestMergeJSONRejectsAmbiguousACDCommand(t *testing.T) {
	existing := []byte(`{"hooks":{"SessionStart":[{"command":"my-acd wrapper"}]}}`)
	template := []byte(`{"hooks":{"SessionStart":[{"command":"acd internal hint"}]}}`)
	if _, err := mergeJSON(existing, template); err == nil {
		t.Fatal("ambiguous command accepted")
	}
}

func TestMergeTextUsesBoundedOwnedBlock(t *testing.T) {
	first, err := mergeText([]byte("custom: true\n"), []byte("hooks: []\n"), "pi")
	if err != nil {
		t.Fatal(err)
	}
	second, err := mergeText(first, []byte("hooks: []\n"), "pi")
	if err != nil {
		t.Fatal(err)
	}
	if string(first) != string(second) || !strings.Contains(string(first), "custom: true") {
		t.Fatalf("merged=%q", first)
	}
}

func TestMergeTextUpgradesExactLegacyTemplate(t *testing.T) {
	current := []byte("# acd-managed: true\nacd internal session open\nacd internal hint --kind wake\nacd internal hint --kind logical_boundary\nacd internal session close\n")
	legacy := []byte("# acd-managed: true\nacd start\nacd wake\nacd flush --logical\nacd stop\n")
	merged, err := mergeText(legacy, current, "opencode")
	if err != nil {
		t.Fatal(err)
	}
	text := string(merged)
	if !strings.Contains(text, "# >>> acd managed v1 opencode") ||
		!strings.Contains(text, "acd internal session open") || strings.Contains(text, "\nacd start\n") {
		t.Fatalf("merged legacy template=%q", merged)
	}
}

func TestMergeTextRejectsModifiedLegacyTemplate(t *testing.T) {
	current := []byte("# acd-managed: true\nacd internal session open\n")
	modifiedLegacy := []byte("# acd-managed: true\nacd start --custom\n")
	if _, err := mergeText(modifiedLegacy, current, "opencode"); err == nil ||
		!strings.Contains(err.Error(), "ambiguous legacy") {
		t.Fatalf("modified legacy error=%v", err)
	}
}

func TestMergeTextUpgradesEmbeddedLegacyShellBlock(t *testing.T) {
	current := []byte("# acd-managed: true\nacd internal session open\n")
	existing := []byte("export KEEP=1\n\n" + legacyShellZshBlock + "\n\nexport ALSO_KEEP=1\n")
	merged, err := mergeText(existing, current, "shell")
	if err != nil {
		t.Fatal(err)
	}
	text := string(merged)
	if !strings.Contains(text, "export KEEP=1") || !strings.Contains(text, "export ALSO_KEEP=1") ||
		!strings.Contains(text, "# >>> acd managed v1 shell") || strings.Contains(text, "acd start --session-id") {
		t.Fatalf("merged embedded shell block=%q", merged)
	}
}

func TestShellIntegrationUsesOwnedZshBlock(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	zshrc := filepath.Join(home, ".zshrc")
	if err := os.WriteFile(zshrc, []byte("# custom\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	plans, err := BuildPlans(paths.Roots{}, "shell")
	if err != nil {
		t.Fatal(err)
	}
	if len(plans) != 1 || plans[0].Target != zshrc ||
		!strings.Contains(string(plans[0].Content), "# custom") ||
		!strings.Contains(string(plans[0].Content), "# >>> acd managed v1 shell") {
		t.Fatalf("shell plan=%+v content=%s", plans, plans[0].Content)
	}
}
