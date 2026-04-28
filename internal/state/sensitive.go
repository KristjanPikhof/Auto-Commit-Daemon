package state

import (
	"os"
	"path"
	"path/filepath"
	"strings"
)

// EnvSensitiveGlobs is the env var that overrides the default-deny sensitive
// glob list. Renamed from the legacy SNAPSHOTD_SENSITIVE_GLOBS but follows the
// exact same semantics (§8.7):
//
//   - unset, empty, or whitespace-only -> use defaults.
//   - non-empty            -> parse comma-separated patterns; whitespace is
//     trimmed and empty entries are dropped. If the parsed list is empty
//     (e.g. ",,, ,") the safe baseline still applies.
//
// Treating an empty override as "disable filtering" is a foot-gun: shell
// exports like ACD_SENSITIVE_GLOBS="" are easy to write by accident and would
// silently let secrets enter the object store. The defaults always apply
// unless the operator provides an explicit non-empty override.
const EnvSensitiveGlobs = "ACD_SENSITIVE_GLOBS"

// DefaultSensitiveGlobs is the canonical default-deny list ported verbatim
// from snapshot_state.DEFAULT_SENSITIVE_GLOBS (legacy daemon). Order is
// preserved so any future diff against the legacy list reads cleanly.
//
// See §8.7 for the rationale on why each entry is here.
var DefaultSensitiveGlobs = []string{
	".env",
	".env.*",
	"**/.env",
	"**/.env.*",
	".npmrc",
	"**/.npmrc",
	".netrc",
	"**/.netrc",
	".pgpass",
	"**/.pgpass",
	".git-credentials",
	"**/.git-credentials",
	"kubeconfig",
	"**/kubeconfig",
	"**/.aws/credentials",
	"**/.docker/config.json",
	"**/.kube/config",
	"**/id_rsa*",
	"**/id_ed25519*",
	"**/id_ecdsa*",
	"**/*.pem",
	"**/*.key",
	"**/*.p12",
	"**/*.pfx",
	"**/*.crt",
	"**/*.pkcs8",
	"**/*.kdbx",
	"**/service-account*.json",
	"**/*.gpg",
	"**/*.asc",
	"**/secrets/*",
	"**/credentials*",
}

// expandGlobs pairs every "**/X" pattern with its bare "X" form so root-level
// matches are caught. Mirrors snapshot_state._expand_globs verbatim. Go's
// path.Match (and filepath.Match) does not understand "**" the way gitignore
// does, so this is the same workaround the legacy daemon used.
func expandGlobs(patterns []string) []string {
	out := make([]string, 0, len(patterns)*2)
	seen := make(map[string]struct{}, len(patterns)*2)
	add := func(p string) {
		if _, ok := seen[p]; ok {
			return
		}
		seen[p] = struct{}{}
		out = append(out, p)
	}
	for _, p := range patterns {
		add(p)
		if strings.HasPrefix(p, "**/") {
			tail := p[len("**/"):]
			if tail != "" {
				add(tail)
			}
		}
	}
	return out
}

// SensitivePatterns returns the active sensitive-path glob list, applying the
// EnvSensitiveGlobs override semantics described above.
//
// The result is freshly expanded on every call so callers can change the env
// var at runtime in tests without having to reset a package-level cache. The
// daemon itself reads the env var once at startup (via the matcher returned
// from NewSensitiveMatcher) so the hot capture path does not pay the env-read
// or expansion cost on every file.
func SensitivePatterns() []string {
	override := os.Getenv(EnvSensitiveGlobs)
	if strings.TrimSpace(override) == "" {
		return expandGlobs(DefaultSensitiveGlobs)
	}
	parsed := splitAndTrim(override)
	if len(parsed) == 0 {
		return expandGlobs(DefaultSensitiveGlobs)
	}
	return expandGlobs(parsed)
}

func splitAndTrim(s string) []string {
	parts := strings.Split(s, ",")
	out := parts[:0]
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

// IsSensitivePath reports whether rel matches any active sensitive glob. rel
// must be a forward-slash-relative path inside the worktree (the canonical
// form the capture pipeline emits).
//
// Matching uses path.Match — the same semantics as fnmatch in the legacy
// Python. We deliberately do not use filepath.Match here because it splits on
// the OS separator and would mismatch Windows-style "\" on darwin/linux.
func IsSensitivePath(rel string) bool {
	rel = filepath.ToSlash(rel)
	for _, pattern := range SensitivePatterns() {
		if matchGlob(pattern, rel) {
			return true
		}
	}
	return false
}

// SensitiveMatcher is a precomputed sensitivity check. The daemon's hot path
// builds one at startup and reuses it; tests build short-lived ones around
// env-var manipulation.
type SensitiveMatcher struct {
	patterns []string
}

// NewSensitiveMatcher snapshots SensitivePatterns() once.
func NewSensitiveMatcher() *SensitiveMatcher {
	return &SensitiveMatcher{patterns: SensitivePatterns()}
}

// Match reports whether rel matches any pattern in the snapshot.
func (m *SensitiveMatcher) Match(rel string) bool {
	rel = filepath.ToSlash(rel)
	for _, pattern := range m.patterns {
		if matchGlob(pattern, rel) {
			return true
		}
	}
	return false
}

// Patterns returns a copy of the precomputed pattern list (for diagnostics).
func (m *SensitiveMatcher) Patterns() []string {
	out := make([]string, len(m.patterns))
	copy(out, m.patterns)
	return out
}

// matchGlob is path.Match plus a "**/" recursive prefix expansion already
// performed by expandGlobs. We additionally check that "**/X" patterns also
// match any subpath ending in "X" — Go's path.Match has no recursive form, so
// "**/secrets/*" against "a/b/secrets/x" would otherwise miss.
func matchGlob(pattern, rel string) bool {
	// Direct match against the pattern as-is.
	if ok, _ := path.Match(pattern, rel); ok {
		return true
	}
	// Recursive "**/" prefix: try the tail against every suffix of rel. This
	// is bounded by the segment count of rel and stays cheap in practice.
	if strings.HasPrefix(pattern, "**/") {
		tail := pattern[len("**/"):]
		if tail == "" {
			return rel != ""
		}
		// Match tail against rel itself and every "<segment>/.../tail" suffix.
		segments := strings.Split(rel, "/")
		for i := 0; i < len(segments); i++ {
			candidate := strings.Join(segments[i:], "/")
			if ok, _ := path.Match(tail, candidate); ok {
				return true
			}
		}
	}
	return false
}
