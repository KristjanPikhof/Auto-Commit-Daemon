// Package integration structurally merges optional semantic hint commands.
// Filesystem protection never depends on these integrations.
package integration

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/adapter"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/paths"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/templates"
)

const TemplateVersion = 1

type Plan struct {
	Name         string         `json:"name"`
	Target       string         `json:"target"`
	Format       string         `json:"format"`
	BeforeDigest string         `json:"before_digest"`
	AfterDigest  string         `json:"after_digest"`
	Changed      bool           `json:"changed"`
	Content      []byte         `json:"-"`
	Owned        []OwnedElement `json:"-"`
}

type Ownership struct {
	Version int                   `json:"version"`
	Entries map[string]OwnedEntry `json:"entries"`
}
type OwnedEntry struct {
	Name            string         `json:"name"`
	Digest          string         `json:"digest"`
	TemplateVersion int            `json:"template_version"`
	Signatures      []string       `json:"signatures"`
	Format          string         `json:"format"`
	Elements        []OwnedElement `json:"elements"`
}

type OwnedElement struct {
	Event  string `json:"event,omitempty"`
	Digest string `json:"digest"`
}

type RemovalPlan struct {
	Name         string `json:"name"`
	Target       string `json:"target"`
	BeforeDigest string `json:"before_digest"`
	AfterDigest  string `json:"after_digest"`
	Changed      bool   `json:"changed"`
	Content      []byte `json:"-"`
}

var templateFiles = map[string]string{
	"claude-code": "claude-code/settings.snippet.json", "codex": "codex/hooks.json", "cursor": "cursor/hooks.json",
	"opencode": "opencode/hooks.snippet.yaml", "pi": "pi/hooks.snippet.yaml",
	"shell": "shell/zshrc.snippet.sh",
}

func BuildPlans(roots paths.Roots, selection string) ([]Plan, error) {
	names, err := selectNames(selection)
	if err != nil {
		return nil, err
	}
	var plans []Plan
	for _, name := range names {
		harness, ok := adapter.Lookup(name)
		if !ok {
			return nil, fmt.Errorf("integration: unknown integration %q", name)
		}
		target := harness.ConfigPath()
		if target == "" {
			return nil, fmt.Errorf("integration: %s has no install path", name)
		}
		templatePath, ok := templateFiles[name]
		if !ok {
			return nil, fmt.Errorf("integration: %s requires manual compatibility setup", name)
		}
		templateBody, err := templates.FS.ReadFile(templatePath)
		if err != nil {
			return nil, err
		}
		before, err := os.ReadFile(target)
		if errors.Is(err, os.ErrNotExist) {
			before = nil
		} else if err != nil {
			return nil, err
		}
		format := "text"
		var after []byte
		if filepath.Ext(target) == ".json" {
			format = "json"
			after, err = mergeJSON(before, templateBody)
		} else {
			after, err = mergeText(before, templateBody, name)
		}
		if err != nil {
			return nil, fmt.Errorf("integration: merge %s: %w", name, err)
		}
		owned, err := ownedElements(format, templateBody, name)
		if err != nil {
			return nil, err
		}
		plans = append(plans, Plan{Name: name, Target: target, Format: format, BeforeDigest: digest(before), AfterDigest: digest(after), Changed: string(before) != string(after), Content: after, Owned: owned})
	}
	_ = roots
	return plans, nil
}

func selectNames(selection string) ([]string, error) {
	selection = strings.TrimSpace(selection)
	if selection == "" || selection == "auto" {
		var names []string
		for _, name := range adapter.Names() {
			h, _ := adapter.Lookup(name)
			if _, err := os.Stat(h.ConfigPath()); err == nil {
				names = append(names, name)
			}
		}
		return names, nil
	}
	if selection == "none" {
		return []string{}, nil
	}
	seen := map[string]bool{}
	var names []string
	for _, name := range strings.Split(selection, ",") {
		name = strings.TrimSpace(name)
		if name == "" || seen[name] {
			continue
		}
		if _, ok := adapter.Lookup(name); !ok {
			return nil, fmt.Errorf("integration: unknown integration %q", name)
		}
		seen[name] = true
		names = append(names, name)
	}
	sort.Strings(names)
	return names, nil
}

func mergeJSON(existing, templateBody []byte) ([]byte, error) {
	var source map[string]any
	if len(existing) == 0 {
		source = map[string]any{}
	} else if err := json.Unmarshal(existing, &source); err != nil {
		return nil, fmt.Errorf("existing file is invalid JSON: %w", err)
	}
	var desired map[string]any
	if err := json.Unmarshal(templateBody, &desired); err != nil {
		return nil, err
	}
	desiredHooks, _ := desired["hooks"].(map[string]any)
	existingHooks, _ := source["hooks"].(map[string]any)
	if existingHooks == nil {
		existingHooks = map[string]any{}
	}
	for event, raw := range desiredHooks {
		desiredItems, _ := raw.([]any)
		existingItems, _ := existingHooks[event].([]any)
		kept := make([]any, 0, len(existingItems)+len(desiredItems))
		for _, item := range existingItems {
			commands := commandStrings(item)
			acdLike := false
			recognized := true
			for _, command := range commands {
				if strings.Contains(command, "acd ") {
					acdLike = true
					if !knownACDCommand(command) {
						recognized = false
					}
				}
			}
			if acdLike && !recognized {
				return nil, fmt.Errorf("ambiguous ACD-like command under hooks.%s", event)
			}
			if !acdLike {
				kept = append(kept, item)
			}
		}
		kept = append(kept, desiredItems...)
		existingHooks[event] = kept
	}
	source["hooks"] = existingHooks
	for key, value := range desired {
		if key != "hooks" {
			if _, exists := source[key]; !exists {
				source[key] = value
			}
		}
	}
	body, err := json.MarshalIndent(source, "", "  ")
	if err != nil {
		return nil, err
	}
	return append(body, '\n'), nil
}

func commandStrings(value any) []string {
	var result []string
	var walk func(any)
	walk = func(current any) {
		switch typed := current.(type) {
		case map[string]any:
			for key, value := range typed {
				if key == "command" {
					if command, ok := value.(string); ok {
						result = append(result, command)
					}
				}
				walk(value)
			}
		case []any:
			for _, item := range typed {
				walk(item)
			}
		}
	}
	walk(value)
	return result
}
func knownACDCommand(command string) bool {
	return strings.Contains(command, "acd internal ") || strings.Contains(command, "acd start") || strings.Contains(command, "acd wake") || strings.Contains(command, "acd stop") || strings.Contains(command, "acd flush") || strings.Contains(command, "acd touch") || strings.Contains(command, "acd hook-")
}

func mergeText(existing, templateBody []byte, name string) ([]byte, error) {
	start := "# >>> acd managed v1 " + name
	end := "# <<< acd managed v1 " + name
	text := string(existing)
	startIndex := strings.Index(text, start)
	endIndex := strings.Index(text, end)
	block := start + "\n" + strings.TrimRight(string(templateBody), "\n") + "\n" + end + "\n"
	if startIndex >= 0 || endIndex >= 0 {
		if startIndex < 0 || endIndex < startIndex {
			return nil, errors.New("modified or incomplete owned marker block")
		}
		endIndex += len(end)
		for endIndex < len(text) && (text[endIndex] == '\n' || text[endIndex] == '\r') {
			endIndex++
		}
		return []byte(text[:startIndex] + block + text[endIndex:]), nil
	}
	if matchesLegacyTemplate(text, string(templateBody)) {
		return []byte(block), nil
	}
	if name == "shell" && strings.Count(text, legacyShellZshBlock) == 1 {
		return []byte(strings.Replace(text, legacyShellZshBlock, strings.TrimRight(block, "\n"), 1)), nil
	}
	if strings.Contains(text, "acd-managed: true") || strings.Contains(text, "acd internal ") || strings.Contains(text, "acd start") {
		return nil, errors.New("ambiguous legacy ACD block requires an explicit cleanup plan")
	}
	if len(text) > 0 && !strings.HasSuffix(text, "\n") {
		text += "\n"
	}
	if len(text) > 0 {
		text += "\n"
	}
	return []byte(text + block), nil
}

const legacyShellZshBlock = `# acd-managed: true
# Add to ~/.zshrc to auto-register every shell session inside a git repo.
acd_auto_start() {
    command -v acd >/dev/null 2>&1 || return 0

    local repo
    repo=$(git rev-parse --show-toplevel 2>/dev/null) || return 0

    export ACD_SESSION_ID=${ACD_SESSION_ID:-$(uuidgen)}
    acd start --session-id "$ACD_SESSION_ID" --harness shell \
              --watch-pid "$$" --repo "$repo" >/dev/null 2>&1 &!
}
# Hook into chpwd so every cd into a repo triggers registration.
autoload -U add-zsh-hook
add-zsh-hook chpwd acd_auto_start
acd_auto_start`

func matchesLegacyTemplate(existing, current string) bool {
	legacy := strings.NewReplacer(
		"acd internal session open", "acd start",
		"acd internal hint --kind wake", "acd wake",
		"acd internal hint --kind logical_boundary", "acd flush --logical",
		"acd internal session close", "acd stop",
	).Replace(current)
	return strings.TrimSpace(existing) == strings.TrimSpace(legacy)
}

func WriteOwnership(path string, plans []Plan) error {
	document := Ownership{Version: 1, Entries: map[string]OwnedEntry{}}
	if body, err := os.ReadFile(path); err == nil {
		if err := json.Unmarshal(body, &document); err != nil {
			return err
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if document.Entries == nil {
		document.Entries = map[string]OwnedEntry{}
	}
	for _, plan := range plans {
		document.Entries[plan.Target] = OwnedEntry{Name: plan.Name, Digest: plan.AfterDigest,
			TemplateVersion: TemplateVersion, Signatures: []string{"acd internal "},
			Format: plan.Format, Elements: append([]OwnedElement(nil), plan.Owned...)}
	}
	body, err := json.MarshalIndent(document, "", "  ")
	if err != nil {
		return err
	}
	body = append(body, '\n')
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), ".integrations-")
	if err != nil {
		return err
	}
	name := tmp.Name()
	defer os.Remove(name)
	if err := tmp.Chmod(0o600); err != nil {
		tmp.Close()
		return err
	}
	if _, err := tmp.Write(body); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(name, path); err != nil {
		return err
	}
	dir, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}

func digest(body []byte) string {
	sum := sha256.Sum256(body)
	return "sha256:" + hex.EncodeToString(sum[:])
}

// BuildRemovalPlans removes only entries whose full post-install file digest
// still matches ownership metadata. A user-modified owned file blocks the
// transaction instead of being overwritten.
func BuildRemovalPlans(ownershipPath string) ([]RemovalPlan, error) {
	body, err := os.ReadFile(ownershipPath)
	if errors.Is(err, os.ErrNotExist) {
		return []RemovalPlan{}, nil
	}
	if err != nil {
		return nil, err
	}
	var ownership Ownership
	if err := json.Unmarshal(body, &ownership); err != nil {
		return nil, err
	}
	var targets []string
	for target := range ownership.Entries {
		targets = append(targets, target)
	}
	sort.Strings(targets)
	plans := make([]RemovalPlan, 0, len(targets))
	for _, target := range targets {
		owned := ownership.Entries[target]
		before, err := os.ReadFile(target)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return nil, err
		}
		var after []byte
		if len(owned.Elements) == 0 {
			if digest(before) != owned.Digest {
				return nil, fmt.Errorf("integration: legacy owned file was modified: %s", target)
			}
			if filepath.Ext(target) == ".json" {
				after, err = removeLegacyJSON(before)
			} else {
				after, err = removeLegacyText(before, owned.Name)
			}
		} else if filepath.Ext(target) == ".json" {
			after, err = removeJSON(before, owned.Elements)
		} else {
			after, err = removeText(before, owned.Name, owned.Elements)
		}
		if err != nil {
			return nil, err
		}
		plans = append(plans, RemovalPlan{Name: owned.Name, Target: target,
			BeforeDigest: digest(before), AfterDigest: digest(after),
			Changed: string(before) != string(after), Content: after})
	}
	return plans, nil
}

func removeLegacyJSON(body []byte) ([]byte, error) {
	var source map[string]any
	if err := json.Unmarshal(body, &source); err != nil {
		return nil, err
	}
	hooks, _ := source["hooks"].(map[string]any)
	for event, raw := range hooks {
		items, _ := raw.([]any)
		kept := make([]any, 0, len(items))
		for _, item := range items {
			owned := false
			for _, command := range commandStrings(item) {
				owned = owned || strings.Contains(command, "acd internal ")
			}
			if !owned {
				kept = append(kept, item)
			}
		}
		if len(kept) == 0 {
			delete(hooks, event)
		} else {
			hooks[event] = kept
		}
	}
	body, err := json.MarshalIndent(source, "", "  ")
	return append(body, '\n'), err
}

func removeLegacyText(body []byte, name string) ([]byte, error) {
	start := "# >>> acd managed v1 " + name
	end := "# <<< acd managed v1 " + name
	text := string(body)
	left, right := strings.Index(text, start), strings.Index(text, end)
	if left < 0 || right < left {
		return nil, errors.New("integration: owned marker block is missing")
	}
	right += len(end)
	for right < len(text) && (text[right] == '\r' || text[right] == '\n') {
		right++
	}
	return []byte(strings.TrimRight(text[:left], "\r\n") + "\n" + text[right:]), nil
}

func removeJSON(body []byte, owned []OwnedElement) ([]byte, error) {
	var source map[string]any
	if err := json.Unmarshal(body, &source); err != nil {
		return nil, err
	}
	hooks, _ := source["hooks"].(map[string]any)
	wanted := make(map[string]map[string]int)
	for _, element := range owned {
		if wanted[element.Event] == nil {
			wanted[element.Event] = make(map[string]int)
		}
		wanted[element.Event][element.Digest]++
	}
	for event, raw := range hooks {
		items, _ := raw.([]any)
		kept := make([]any, 0, len(items))
		for _, item := range items {
			itemDigest := digestJSONValue(item)
			if wanted[event][itemDigest] > 0 {
				wanted[event][itemDigest]--
				continue
			}
			kept = append(kept, item)
		}
		if len(kept) == 0 {
			delete(hooks, event)
		} else {
			hooks[event] = kept
		}
	}
	for event, digests := range wanted {
		for _, count := range digests {
			if count > 0 {
				return nil, fmt.Errorf("integration: owned JSON entry under hooks.%s was modified or removed", event)
			}
		}
	}
	source["hooks"] = hooks
	out, err := json.MarshalIndent(source, "", "  ")
	if err != nil {
		return nil, err
	}
	return append(out, '\n'), nil
}

func removeText(body []byte, name string, owned []OwnedElement) ([]byte, error) {
	start := "# >>> acd managed v1 " + name
	end := "# <<< acd managed v1 " + name
	text := string(body)
	left := strings.Index(text, start)
	right := strings.Index(text, end)
	if left < 0 || right < left {
		return nil, errors.New("integration: owned marker block is missing")
	}
	blockEnd := right + len(end)
	if len(owned) != 1 || digest([]byte(text[left:blockEnd])) != owned[0].Digest {
		return nil, errors.New("integration: owned marker block was modified")
	}
	right = blockEnd
	for right < len(text) && (text[right] == '\r' || text[right] == '\n') {
		right++
	}
	return []byte(strings.TrimRight(text[:left], "\r\n") + "\n" + text[right:]), nil
}

func ownedElements(format string, templateBody []byte, name string) ([]OwnedElement, error) {
	if format == "json" {
		var document map[string]any
		if err := json.Unmarshal(templateBody, &document); err != nil {
			return nil, err
		}
		hooks, _ := document["hooks"].(map[string]any)
		var elements []OwnedElement
		var events []string
		for event := range hooks {
			events = append(events, event)
		}
		sort.Strings(events)
		for _, event := range events {
			items, _ := hooks[event].([]any)
			for _, item := range items {
				elements = append(elements, OwnedElement{Event: event, Digest: digestJSONValue(item)})
			}
		}
		return elements, nil
	}
	start := "# >>> acd managed v1 " + name
	end := "# <<< acd managed v1 " + name
	block := start + "\n" + strings.TrimRight(string(templateBody), "\n") + "\n" + end
	return []OwnedElement{{Digest: digest([]byte(block))}}, nil
}

func digestJSONValue(value any) string {
	body, _ := json.Marshal(value)
	return digest(body)
}
