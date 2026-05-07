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
	path    string
	markers []string
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
	tomlAcdManagedMarkers = []string{"acd-managed: true"}
	yamlAcdManagedMarkers = []string{"acd-managed: true"}
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
		// path is the JSON file. config.toml stays in the candidate set
		// for legacy installs and triggers the doctor shadow warning when
		// both files carry acd markers. Repo-local detection is intentionally
		// scoped out of DetectInstalled because it has no repo-root context;
		// adding `.codex/*` entries here would resolve against process cwd
		// and produce false positives in shared dirs.
		paths: []pathSpec{
			{path: "~/.codex/hooks.json", markers: jsonAcdManagedMarkers},
			{path: "~/.codex/config.toml", markers: tomlAcdManagedMarkers},
			{path: "~/.config/codex/config.toml", markers: tomlAcdManagedMarkers},
		},
	},
	{
		name: "opencode",
		paths: []pathSpec{
			{path: "~/.config/opencode/hooks.yaml", markers: yamlAcdManagedMarkers},
		},
	},
	{
		name: "pi",
		paths: []pathSpec{
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
	legacyTOMLInstalled = fileContainsAny(filepath.Join(home, ".codex", "config.toml"), tomlAcdManagedMarkers)
	return
}
