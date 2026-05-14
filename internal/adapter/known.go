package adapter

import (
	"os"
	"path/filepath"
	"strings"
)

// pathSpec binds a config path candidate to the marker strings that prove
// acd installed it. Different paths for the same harness can use different
// marker syntaxes (e.g., codex JSON vs TOML).
type pathSpec struct {
	path      string
	markers   []string
	repoLocal bool
}

func fileContainsAny(path string, markers []string) bool {
	body, err := os.ReadFile(path)
	if err != nil {
		return false
	}
	text := string(body)
	for _, m := range markers {
		if strings.Contains(text, m) {
			return true
		}
	}
	return false
}

var (
	jsonAcdManagedMarkers = []string{`"_acd_managed": true`, `"_acd_managed":true`}
	// tomlAcdManagedMarkers requires the leading `#` comment prefix because
	// TOML has no other way to embed a free-form line, and the canonical acd
	// install writes a `# acd-managed: true` comment block.
	tomlAcdManagedMarkers = []string{"# acd-managed: true"}
	// yamlAcdManagedMarkers matches only the canonical comment form
	// `# acd-managed: true`. acd-managed YAML templates always write the
	// comment-prefixed form (never a bare key), so this is sufficient to
	// detect canonical installs. Requiring the `#` prefix prevents a
	// hand-edited YAML containing a bare `acd-managed: true` line — e.g.,
	// a user-config key that happens to use the same identifier — from
	// being misclassified as an acd install (which would cause `acd doctor`
	// to recommend overwrite remediation against a file acd never wrote).
	// All three marker slices now require the canonical prefix:
	// JSON uses the `_acd_managed` key; TOML and YAML use `# acd-managed: true`.
	yamlAcdManagedMarkers = []string{"# acd-managed: true"}
)

var knownHarnesses = []knownHarness{
	{
		name: "claude-code",
		paths: []pathSpec{
			{path: "~/.claude/settings.json", markers: jsonAcdManagedMarkers},
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
			{path: "~/.codex/hooks.json", markers: jsonAcdManagedMarkers},
			{path: "~/.codex/config.toml", markers: tomlAcdManagedMarkers},
			{path: "~/.config/codex/config.toml", markers: tomlAcdManagedMarkers},
			{path: ".codex/hooks.json", markers: jsonAcdManagedMarkers, repoLocal: true},
			{path: ".codex/config.toml", markers: tomlAcdManagedMarkers, repoLocal: true},
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
			{path: "~/.config/opencode/hook/hooks.yaml", markers: yamlAcdManagedMarkers},
			// legacy fallback (pre-canonical default).
			{path: "~/.config/opencode/hooks.yaml", markers: yamlAcdManagedMarkers},
		},
	},
	{
		name: "pi",
		// Canonical Pi default is ~/.pi/agent/hook/hooks.yaml. The
		// pre-canonical ~/.pi/hook/hooks.yaml stays as a SECONDARY legacy
		// fallback so existing installs still register; doctor remediation
		// uses the canonical path so users are nudged forward.
		paths: []pathSpec{
			{path: "~/.pi/agent/hook/hooks.yaml", markers: yamlAcdManagedMarkers},
			// legacy fallback (pre-canonical default).
			{path: "~/.pi/hook/hooks.yaml", markers: yamlAcdManagedMarkers},
		},
	},
	{
		name: "shell",
		paths: []pathSpec{
			{path: "~/.zshrc", markers: yamlAcdManagedMarkers},
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

// PrimaryPathMatchesMarker reports whether `body` contains a marker
// registered for the harness's primary candidate path. Doctor uses this to
// avoid cross-format false positives (e.g., a TOML marker string inside a
// JSON config).
func PrimaryPathMatchesMarker(harnessName string, body []byte) bool {
	for _, h := range knownHarnesses {
		if h.name != harnessName || len(h.paths) == 0 {
			continue
		}
		text := string(body)
		for _, m := range h.paths[0].markers {
			if strings.Contains(text, m) {
				return true
			}
		}
	}
	return false
}

// CodexInstalls reports whether the user-scoped Codex hooks.json carries the
// JSON acd marker and whether the legacy ~/.codex/config.toml still carries
// the TOML acd marker. Used by `acd doctor` to warn about shadowed installs.
func CodexInstalls() (jsonInstalled, legacyTOMLInstalled bool) {
	home, err := os.UserHomeDir()
	if err != nil || home == "" {
		return false, false
	}
	jsonInstalled = fileContainsAny(filepath.Join(home, ".codex", "hooks.json"), jsonAcdManagedMarkers)
	legacyTOMLInstalled =
		fileContainsAny(filepath.Join(home, ".codex", "config.toml"), tomlAcdManagedMarkers) ||
			fileContainsAny(filepath.Join(home, ".config", "codex", "config.toml"), tomlAcdManagedMarkers)
	return
}
