// Package adapter holds harness identification helpers.
package adapter

import (
	"os"
	"path/filepath"
	"strings"
)

// Harness describes one supported harness adapter and where acd can detect
// an existing managed install.
type Harness interface {
	Name() string
	ConfigPath() string
	IsInstalled() bool
	HasMarker() bool
}

type knownHarness struct {
	name        string
	configPath  string
	extraPaths  []string
	markerTexts []string
}

func (h knownHarness) Name() string {
	return h.name
}

func (h knownHarness) ConfigPath() string {
	return expandHome(h.configPath)
}

func (h knownHarness) IsInstalled() bool {
	return h.HasMarker()
}

func (h knownHarness) HasMarker() bool {
	for _, path := range h.allPaths() {
		body, err := os.ReadFile(path)
		if err != nil {
			continue
		}
		text := string(body)
		for _, marker := range h.markerTexts {
			if strings.Contains(text, marker) {
				return true
			}
		}
	}
	return false
}

func (h knownHarness) allPaths() []string {
	paths := make([]string, 0, 1+len(h.extraPaths))
	paths = append(paths, h.ConfigPath())
	for _, path := range h.extraPaths {
		paths = append(paths, expandHome(path))
	}
	return paths
}

// DetectInstalled returns the supported harnesses that already have an
// acd-managed marker in their known config path.
func DetectInstalled() []Harness {
	var out []Harness
	for _, h := range knownHarnesses {
		if h.IsInstalled() {
			out = append(out, h)
		}
	}
	return out
}

func expandHome(path string) string {
	if path == "~" {
		if home, err := os.UserHomeDir(); err == nil && home != "" {
			return home
		}
		return path
	}
	if strings.HasPrefix(path, "~/") {
		if home, err := os.UserHomeDir(); err == nil && home != "" {
			return filepath.Join(home, path[2:])
		}
	}
	return path
}
