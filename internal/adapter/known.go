package adapter

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
)

// pathSpec binds a config path candidate to the detector that proves acd
// installed it. Different paths for the same harness can use different
// detection strategies (e.g., JSON command signatures vs TOML comments).
type pathSpec struct {
	path      string
	detector  installDetector
	repoLocal bool
}

type installDetector func([]byte) bool

func textInstallDetector(markers ...string) installDetector {
	return func(body []byte) bool {
		text := string(body)
		for _, m := range markers {
			if strings.Contains(text, m) {
				return true
			}
		}
		return false
	}
}

func jsonCommandSignatureDetector(required ...string) installDetector {
	return func(body []byte) bool {
		var decoded any
		if err := json.Unmarshal(body, &decoded); err != nil {
			return false
		}
		for _, command := range jsonCommandStrings(decoded) {
			if containsAll(command, required) {
				return true
			}
		}
		return false
	}
}

func containsAll(text string, required []string) bool {
	for _, needle := range required {
		if !strings.Contains(text, needle) {
			return false
		}
	}
	return true
}

func jsonCommandStrings(v any) []string {
	var out []string
	var walk func(any)
	walk = func(cur any) {
		switch x := cur.(type) {
		case map[string]any:
			for key, value := range x {
				if key == "command" {
					if s, ok := value.(string); ok {
						out = append(out, s)
					}
				}
				walk(value)
			}
		case []any:
			for _, value := range x {
				walk(value)
			}
		}
	}
	walk(v)
	return out
}

func hasLegacyJSONManagedKey(body []byte) bool {
	var top map[string]json.RawMessage
	if err := json.Unmarshal(body, &top); err != nil {
		return false
	}
	raw, ok := top["_acd_managed"]
	if !ok {
		return false
	}
	var managed bool
	return json.Unmarshal(raw, &managed) == nil && managed
}

// HasLegacyJSONManagedKey reports whether a JSON config still carries the
// pre-schema-clean ACD marker. Current ACD JSON templates do not emit this key,
// but doctor uses it to guide users through legacy migrations.
func HasLegacyJSONManagedKey(body []byte) bool {
	return hasLegacyJSONManagedKey(body)
}

func legacyJSONInstallDetector() installDetector {
	return func(body []byte) bool {
		if hasLegacyJSONManagedKey(body) {
			return true
		}
		// Older tests and hand-written files may preserve compact/minified marker
		// text that is still valid JSON but avoid relying on spacing.
		text := string(body)
		return strings.Contains(text, `"_acd_managed": true`) ||
			strings.Contains(text, `"_acd_managed":true`)
	}
}

func orInstallDetector(detectors ...installDetector) installDetector {
	return func(body []byte) bool {
		for _, detector := range detectors {
			if detector != nil && detector(body) {
				return true
			}
		}
		return false
	}
}

func jsonSignatureOrLegacyDetector(required ...string) installDetector {
	return orInstallDetector(jsonCommandSignatureDetector(required...), legacyJSONInstallDetector())
}

func textFileContains(path string, detector installDetector) bool {
	body, err := os.ReadFile(path)
	if err != nil {
		return false
	}
	return detector != nil && detector(body)
}

var (
	claudeCodeJSONDetector = jsonSignatureOrLegacyDetector("acd hook-stdin-extract", "--harness claude-code")
	codexJSONDetector      = jsonSignatureOrLegacyDetector("acd hook-stdin-extract", "--harness codex")
	cursorJSONDetector     = jsonSignatureOrLegacyDetector("acd hook-cursor-extract", "--harness cursor")
	// tomlAcdManagedMarkers requires the leading `#` comment prefix because
	// TOML has no other way to embed a free-form line, and the canonical acd
	// install writes a `# acd-managed: true` comment block.
	tomlAcdManagedMarkers = []string{"# acd-managed: true"}
	tomlAcdManagedDetector = textInstallDetector(tomlAcdManagedMarkers...)
	// yamlAcdManagedMarkers matches only the canonical comment form
	// `# acd-managed: true`. acd-managed YAML templates always write the
	// comment-prefixed form (never a bare key), so this is sufficient to
	// detect canonical installs. Requiring the `#` prefix prevents a
	// hand-edited YAML containing a bare `acd-managed: true` line — e.g.,
	// a user-config key that happens to use the same identifier — from
	// being misclassified as an acd install (which would cause `acd doctor`
	// to recommend overwrite remediation against a file acd never wrote).
	yamlAcdManagedMarkers = []string{"# acd-managed: true"}
	yamlAcdManagedDetector = textInstallDetector(yamlAcdManagedMarkers...)
)

var knownHarnesses = []knownHarness{
	{
		name: "claude-code",
		paths: []pathSpec{
			{path: "~/.claude/settings.json", detector: claudeCodeJSONDetector},
		},
	},
	{
		name: "codex",
		// hooks.json wins Codex discovery order over config.toml; primary
		// path is the user-scoped JSON file. config.toml stays in the
		// candidate set for legacy installs and triggers the doctor shadow
		// warning when both files carry acd markers. Repo-local candidates
		// are resolved from the current git root so a trusted project-local
		// install is detected without treating arbitrary cwd-relative
		// `.codex/*` files as global installs.
		paths: []pathSpec{
			{path: "~/.codex/hooks.json", detector: codexJSONDetector},
			{path: "~/.codex/config.toml", detector: tomlAcdManagedDetector},
			{path: "~/.config/codex/config.toml", detector: tomlAcdManagedDetector},
			{path: ".codex/hooks.json", detector: codexJSONDetector, repoLocal: true},
			{path: ".codex/config.toml", detector: tomlAcdManagedDetector, repoLocal: true},
		},
	},
	{
		name: "cursor",
		// User-scoped hooks.json only; Cursor does not use a repo-local
		// `.cursor/hooks.json` install path for acd (unlike Codex).
		paths: []pathSpec{
			{path: "~/.cursor/hooks.json", detector: cursorJSONDetector},
		},
	},
	{
		name: "opencode",
		// Canonical OpenCode default is ~/.config/opencode/hook/hooks.yaml
		// (note the `hook/` segment). The pre-canonical layout
		// ~/.config/opencode/hooks.yaml ships as a SECONDARY legacy
		// fallback so a user mid-migration still detects as installed and
		// `acd doctor` can steer them to the canonical path. ConfigPath()
		// always returns the primary candidate so remediation hints point
		// at the canonical location regardless of which file holds the
		// existing marker.
		paths: []pathSpec{
			{path: "~/.config/opencode/hook/hooks.yaml", detector: yamlAcdManagedDetector},
			// legacy fallback (pre-canonical default).
			{path: "~/.config/opencode/hooks.yaml", detector: yamlAcdManagedDetector},
		},
	},
	{
		name: "pi",
		// Canonical Pi default is ~/.pi/agent/hook/hooks.yaml. The
		// pre-canonical ~/.pi/hook/hooks.yaml stays as a SECONDARY legacy
		// fallback so existing installs still register; doctor remediation
		// uses the canonical path so users are nudged forward.
		paths: []pathSpec{
			{path: "~/.pi/agent/hook/hooks.yaml", detector: yamlAcdManagedDetector},
			// legacy fallback (pre-canonical default).
			{path: "~/.pi/hook/hooks.yaml", detector: yamlAcdManagedDetector},
		},
	},
	{
		name: "shell",
		paths: []pathSpec{
			{path: "~/.zshrc", detector: yamlAcdManagedDetector},
		},
	},
}

// Names returns the canonical ordered list of supported harness identifiers.
func Names() []string {
	names := make([]string, 0, len(knownHarnesses))
	for _, h := range knownHarnesses {
		names = append(names, h.Name())
	}
	return names
}

// Lookup returns the registered harness with name.
func Lookup(name string) (Harness, bool) {
	for _, h := range knownHarnesses {
		if h.Name() == name {
			return h, true
		}
	}
	return nil, false
}

// PrimaryPathMatchesMarker reports whether `body` matches the detector
// registered for the harness's primary candidate path. Doctor uses this to
// avoid cross-format false positives (e.g., a TOML marker string inside a
// JSON config). The legacy name is kept for doctor JSON compatibility.
func PrimaryPathMatchesMarker(harnessName string, body []byte) bool {
	for _, h := range knownHarnesses {
		if h.name != harnessName || len(h.paths) == 0 {
			continue
		}
		return h.paths[0].detector != nil && h.paths[0].detector(body)
	}
	return false
}

// CodexInstalls reports whether the user-scoped Codex hooks.json contains ACD
// hook commands (or a legacy JSON marker) and whether the legacy
// ~/.codex/config.toml still carries the TOML acd marker. Used by `acd doctor`
// to warn about shadowed installs.
func CodexInstalls() (jsonInstalled, legacyTOMLInstalled bool) {
	home, err := os.UserHomeDir()
	if err != nil || home == "" {
		return false, false
	}
	jsonInstalled = textFileContains(filepath.Join(home, ".codex", "hooks.json"), codexJSONDetector)
	legacyTOMLInstalled =
		textFileContains(filepath.Join(home, ".codex", "config.toml"), tomlAcdManagedDetector) ||
			textFileContains(filepath.Join(home, ".config", "codex", "config.toml"), tomlAcdManagedDetector)
	return
}
