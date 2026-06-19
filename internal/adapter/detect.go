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
	// MatchedPath returns the candidate path (with `~` expanded) that
	// actually carries the acd marker on disk. Iterates candidates in
	// pathSpec slice order, so canonical wins over legacy when both are
	// marked. Returns "", false when no candidate carries a marker.
	MatchedPath() (string, bool)
}

type knownHarness struct {
	name     string
	paths    []pathSpec
	repoRoot string
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
	return h.expandPath(h.paths[0])
}

func (h knownHarness) IsInstalled() bool {
	return h.HasMarker()
}

// HasMarker returns true when any candidate path matches the install
// detector registered for that path. Detectors are checked per-path so JSON
// files do not match TOML markers and vice versa.
func (h knownHarness) HasMarker() bool {
	for _, p := range h.candidatePaths() {
		if textFileContains(p.path, p.detector) {
			return true
		}
	}
	return false
}

// allPaths returns every candidate path for this harness with `~` expanded.
func (h knownHarness) allPaths() []string {
	candidates := h.candidatePaths()
	paths := make([]string, 0, len(candidates))
	for _, p := range candidates {
		paths = append(paths, p.path)
	}
	return paths
}

// MatchedPath returns the expanded candidate path that currently carries an
// acd marker on disk. Iteration follows pathSpec slice order, so the
// canonical primary is preferred over legacy fallbacks when both carry the
// marker. Returns "", false when no candidate carries a marker.
func (h knownHarness) MatchedPath() (string, bool) {
	for _, p := range h.candidatePaths() {
		if textFileContains(p.path, p.detector) {
			return p.path, true
		}
	}
	return "", false
}

// DetectInstalled returns the supported harnesses that already have an
// acd-managed marker in their known config path.
func DetectInstalled() []Harness {
	return DetectInstalledFromDir("")
}

// DetectInstalledFromDir returns supported harnesses with an acd-managed marker
// in a user-scoped config path or in a repo-local config path resolved from dir's
// containing git worktree. An empty dir means the current working directory.
func DetectInstalledFromDir(dir string) []Harness {
	repoRoot := discoverGitRoot(dir)
	var out []Harness
	for _, h := range knownHarnesses {
		h.repoRoot = repoRoot
		if h.IsInstalled() {
			out = append(out, h)
		}
	}
	return out
}

type expandedPathSpec struct {
	path     string
	detector installDetector
}

func (h knownHarness) candidatePaths() []expandedPathSpec {
	out := make([]expandedPathSpec, 0, len(h.paths))
	repoRoot := h.repoRoot
	if repoRoot == "" {
		repoRoot = discoverGitRoot("")
	}
	for _, p := range h.paths {
		expanded := h.expandPathWithRoot(p, repoRoot)
		if expanded == "" {
			continue
		}
		out = append(out, expandedPathSpec{path: expanded, detector: p.detector})
	}
	return out
}

func (h knownHarness) expandPath(p pathSpec) string {
	return h.expandPathWithRoot(p, h.repoRoot)
}

func (h knownHarness) expandPathWithRoot(p pathSpec, repoRoot string) string {
	if p.repoLocal {
		if repoRoot == "" {
			return ""
		}
		return filepath.Join(repoRoot, p.path)
	}
	return expandHome(p.path)
}

func discoverGitRoot(dir string) string {
	if dir == "" {
		var err error
		dir, err = os.Getwd()
		if err != nil {
			return ""
		}
	}
	abs, err := filepath.Abs(dir)
	if err != nil {
		return ""
	}
	cur := filepath.Clean(abs)
	for {
		if _, err := os.Stat(filepath.Join(cur, ".git")); err == nil {
			return cur
		}
		parent := filepath.Dir(cur)
		if parent == cur {
			return ""
		}
		cur = parent
	}
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
