package state

import (
	"os"
	"path"
	"path/filepath"
	"strings"
)

const (
	// EnvSafeIgnore disables the generated-tree safe-ignore guard when set
	// to a falsey value: 0, false, no, or off.
	EnvSafeIgnore = "ACD_SAFE_IGNORE"
	// EnvSafeIgnoreExtra appends comma-separated safe-ignore patterns to
	// the defaults. Invalid entries are ignored so the default guard stays
	// active even when this env var is misconfigured.
	EnvSafeIgnoreExtra = "ACD_SAFE_IGNORE_EXTRA"
)

// DefaultSafeIgnorePatterns is the conservative generated-tree list ACD
// skips internally even when a repository forgot to gitignore these paths.
// Keep this list to dependency/cache trees; do not add ambiguous build
// outputs such as dist/, build/, vendor/, or bin/ by default.
var DefaultSafeIgnorePatterns = []string{
	"node_modules/",
	"target/",
	".venv/",
	"venv/",
	"__pycache__/",
	".pytest_cache/",
	".mypy_cache/",
	".ruff_cache/",
	".gradle/",
}

// SafeIgnorePatterns returns the active generated-tree safe-ignore list.
// The guard is default-on; EnvSafeIgnoreExtra appends valid entries and
// EnvSafeIgnore=falsey disables the guard entirely.
func SafeIgnorePatterns() []string {
	if safeIgnoreDisabled(os.Getenv(EnvSafeIgnore)) {
		return nil
	}
	out := make([]string, 0, len(DefaultSafeIgnorePatterns))
	for _, p := range DefaultSafeIgnorePatterns {
		if normalized, ok := normalizeSafeIgnorePattern(p); ok {
			out = append(out, normalized)
		}
	}
	for _, p := range splitAndTrim(os.Getenv(EnvSafeIgnoreExtra)) {
		if normalized, ok := normalizeSafeIgnorePattern(p); ok {
			out = append(out, normalized)
		}
	}
	return dedupeStrings(out)
}

func safeIgnoreDisabled(raw string) bool {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "0", "false", "no", "off":
		return true
	default:
		return false
	}
}

func normalizeSafeIgnorePattern(raw string) (string, bool) {
	p := filepath.ToSlash(strings.TrimSpace(raw))
	p = strings.TrimPrefix(p, "./")
	if p == "" || strings.HasPrefix(p, "/") || p == "." || p == ".." ||
		strings.HasPrefix(p, "../") || strings.Contains(p, "/../") {
		return "", false
	}

	dirPattern := strings.HasSuffix(p, "/")
	if dirPattern {
		p = strings.TrimRight(p, "/")
		if p == "" {
			return "", false
		}
	}
	if _, err := path.Match(p, p); err != nil {
		return "", false
	}
	if dirPattern {
		p += "/"
	}
	return p, true
}

func dedupeStrings(in []string) []string {
	out := in[:0]
	seen := make(map[string]struct{}, len(in))
	for _, p := range in {
		if _, ok := seen[p]; ok {
			continue
		}
		seen[p] = struct{}{}
		out = append(out, p)
	}
	return out
}

// IsSafeIgnoredPath reports whether rel matches the active generated-tree
// safe-ignore guard.
func IsSafeIgnoredPath(rel string) bool {
	return NewSafeIgnoreMatcher().Match(rel)
}

// SafeIgnoreMatcher is a precomputed generated-tree safe-ignore check. The
// daemon builds one at startup and reuses it on capture/watch hot paths.
type SafeIgnoreMatcher struct {
	patterns []string
}

// NewSafeIgnoreMatcher snapshots SafeIgnorePatterns once.
func NewSafeIgnoreMatcher() *SafeIgnoreMatcher {
	return &SafeIgnoreMatcher{patterns: SafeIgnorePatterns()}
}

// Match reports whether rel matches any safe-ignore pattern.
func (m *SafeIgnoreMatcher) Match(rel string) bool {
	if m == nil || len(m.patterns) == 0 {
		return false
	}
	rel = cleanSafeIgnoreRel(rel)
	if rel == "" {
		return false
	}
	for _, pattern := range m.patterns {
		if strings.HasSuffix(pattern, "/") {
			if matchSafeIgnoreDirPattern(strings.TrimSuffix(pattern, "/"), rel) {
				return true
			}
			continue
		}
		if matchGlob(pattern, rel) {
			return true
		}
	}
	return false
}

// MatchDirectory reports whether rel is a directory that should be pruned
// before walking descendants.
func (m *SafeIgnoreMatcher) MatchDirectory(rel string) bool {
	return m.Match(rel)
}

// Patterns returns a copy of the precomputed pattern list.
func (m *SafeIgnoreMatcher) Patterns() []string {
	if m == nil {
		return nil
	}
	out := make([]string, len(m.patterns))
	copy(out, m.patterns)
	return out
}

func cleanSafeIgnoreRel(rel string) string {
	rel = filepath.ToSlash(strings.TrimSpace(rel))
	rel = strings.TrimPrefix(rel, "./")
	rel = strings.Trim(rel, "/")
	return rel
}

func matchSafeIgnoreDirPattern(pattern, rel string) bool {
	if pattern == "" || rel == "" {
		return false
	}
	if strings.Contains(pattern, "/") {
		return rel == pattern || strings.HasPrefix(rel, pattern+"/")
	}
	segments := strings.Split(rel, "/")
	for _, segment := range segments {
		if ok, _ := path.Match(pattern, segment); ok {
			return true
		}
	}
	return false
}
