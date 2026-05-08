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
	name  string
	paths []pathSpec
}

func (h knownHarness) Name() string {
	return h.name
}

// ConfigPath returns the primary candidate path for this harness with `~`
// expanded. Repo-local relative paths are returned verbatim.
func (h knownHarness) ConfigPath() string {
	if len(h.paths) == 0 {
		return ""
	}
	return expandHome(h.paths[0].path)
}

func (h knownHarness) IsInstalled() bool {
	return h.HasMarker()
}

// HasMarker returns true when any candidate path contains a marker
// registered for that path. Markers are checked per-path so JSON files do
// not match TOML markers and vice versa.
func (h knownHarness) HasMarker() bool {
	for _, p := range h.paths {
		body, err := os.ReadFile(expandHome(p.path))
		if err != nil {
			continue
		}
		text := string(body)
		for _, marker := range p.markers {
			if strings.Contains(text, marker) {
				return true
			}
		}
	}
	return false
}

// allPaths returns every candidate path for this harness with `~` expanded.
func (h knownHarness) allPaths() []string {
	paths := make([]string, 0, len(h.paths))
	for _, p := range h.paths {
		paths = append(paths, expandHome(p.path))
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
