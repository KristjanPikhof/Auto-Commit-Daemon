package adapter

var knownHarnesses = []knownHarness{
	{
		name:        "claude-code",
		configPath:  "~/.claude/settings.json",
		markerTexts: []string{`"_acd_managed": true`, `"_acd_managed":true`},
	},
	{
		name:        "codex",
		configPath:  "~/.codex/config.toml",
		extraPaths:  []string{"~/.config/codex/config.toml"},
		markerTexts: []string{"acd-managed: true"},
	},
	{
		name:        "opencode",
		configPath:  "~/.config/opencode/hooks.yaml",
		markerTexts: []string{"acd-managed: true"},
	},
	{
		name:        "pi",
		configPath:  "~/.pi/hook/hooks.yaml",
		markerTexts: []string{"acd-managed: true"},
	},
	{
		name:        "shell",
		configPath:  "~/.zshrc",
		markerTexts: []string{"acd-managed: true"},
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
