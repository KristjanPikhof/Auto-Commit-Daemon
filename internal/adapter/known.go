package adapter

// pathSpec binds a config path candidate to the marker strings that prove
// acd installed it. Different paths for the same harness can use different
// marker syntaxes (e.g., codex JSON vs TOML).
type pathSpec struct {
	path    string
	markers []string
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
		// both files carry acd markers.
		paths: []pathSpec{
			{path: "~/.codex/hooks.json", markers: jsonAcdManagedMarkers},
			{path: "~/.codex/config.toml", markers: tomlAcdManagedMarkers},
			{path: "~/.config/codex/config.toml", markers: tomlAcdManagedMarkers},
			{path: ".codex/hooks.json", markers: jsonAcdManagedMarkers},
			{path: ".codex/config.toml", markers: tomlAcdManagedMarkers},
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

// CodexInstalls reports whether the user-scoped Codex hooks.json carries the
// JSON acd marker and whether the legacy ~/.codex/config.toml still carries
// the TOML acd marker. Used by `acd doctor` to warn about shadowed installs.
func CodexInstalls() (jsonInstalled, legacyTOMLInstalled bool) {
	home, err := osUserHomeDir()
	if err != nil || home == "" {
		return false, false
	}
	jsonInstalled = fileContainsAny(filepathJoin(home, ".codex", "hooks.json"), jsonAcdManagedMarkers)
	legacyTOMLInstalled = fileContainsAny(filepathJoin(home, ".codex", "config.toml"), tomlAcdManagedMarkers)
	return
}
