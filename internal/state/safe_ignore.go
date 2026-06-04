package state

import (
	"context"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"sort"
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
	"DerivedData/",
	".derivedData*/",
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

// SafeIgnoreMatch describes the generated-tree root responsible for a
// safe-ignore match.
type SafeIgnoreMatch struct {
	Root    string
	Pattern string
}

// GeneratedPendingGroup summarizes pending delete capture rows under one
// active safe-ignore root for a branch generation.
type GeneratedPendingGroup struct {
	Root             string  `json:"root"`
	Pattern          string  `json:"pattern"`
	BranchRef        string  `json:"branch_ref"`
	BranchGeneration int64   `json:"branch_generation"`
	BaseHead         string  `json:"base_head,omitempty"`
	PendingCount     int     `json:"pending_count"`
	OldestSeq        int64   `json:"oldest_seq"`
	NewestSeq        int64   `json:"newest_seq"`
	EventSeqs        []int64 `json:"event_seqs,omitempty"`
}

// NewSafeIgnoreMatcher snapshots SafeIgnorePatterns once.
func NewSafeIgnoreMatcher() *SafeIgnoreMatcher {
	return &SafeIgnoreMatcher{patterns: SafeIgnorePatterns()}
}

// Match reports whether rel matches any safe-ignore pattern.
func (m *SafeIgnoreMatcher) Match(rel string) bool {
	return m.match(rel, true)
}

// MatchRoot returns the concrete generated root that matched rel. For a
// nested directory pattern such as node_modules/, a descendant like
// frontend/node_modules/react/index.js maps to frontend/node_modules.
func (m *SafeIgnoreMatcher) MatchRoot(rel string) (SafeIgnoreMatch, bool) {
	if m == nil || len(m.patterns) == 0 {
		return SafeIgnoreMatch{}, false
	}
	rel = cleanSafeIgnoreRel(rel)
	if rel == "" {
		return SafeIgnoreMatch{}, false
	}
	for _, pattern := range m.patterns {
		if strings.HasSuffix(pattern, "/") {
			if root, ok := matchSafeIgnoreDirRoot(strings.TrimSuffix(pattern, "/"), rel); ok {
				return SafeIgnoreMatch{Root: root, Pattern: pattern}, true
			}
			continue
		}
		if matchGlob(pattern, rel) {
			return SafeIgnoreMatch{Root: rel, Pattern: pattern}, true
		}
	}
	return SafeIgnoreMatch{}, false
}

// MatchFile reports whether rel is a file-like path that should be skipped.
// Directory patterns match descendants, but not a same-named file/symlink.
func (m *SafeIgnoreMatcher) MatchFile(rel string) bool {
	return m.match(rel, false)
}

func (m *SafeIgnoreMatcher) match(rel string, includeDirSelf bool) bool {
	if m == nil || len(m.patterns) == 0 {
		return false
	}
	rel = cleanSafeIgnoreRel(rel)
	if rel == "" {
		return false
	}
	for _, pattern := range m.patterns {
		if strings.HasSuffix(pattern, "/") {
			if matchSafeIgnoreDirPattern(strings.TrimSuffix(pattern, "/"), rel, includeDirSelf) {
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

func matchSafeIgnoreDirPattern(pattern, rel string, includeSelf bool) bool {
	_, ok := matchSafeIgnoreDirRootWithSelf(pattern, rel, includeSelf)
	return ok
}

func matchSafeIgnoreDirRoot(pattern, rel string) (string, bool) {
	return matchSafeIgnoreDirRootWithSelf(pattern, rel, true)
}

func matchSafeIgnoreDirRootWithSelf(pattern, rel string, includeSelf bool) (string, bool) {
	if pattern == "" || rel == "" {
		return "", false
	}
	if strings.Contains(pattern, "/") {
		if includeSelf && rel == pattern {
			return pattern, true
		}
		if strings.HasPrefix(rel, pattern+"/") {
			return pattern, true
		}
		return "", false
	}
	segments := strings.Split(rel, "/")
	limit := len(segments)
	if !includeSelf {
		limit--
	}
	for i := 0; i < limit; i++ {
		segment := segments[i]
		if ok, _ := path.Match(pattern, segment); ok {
			return strings.Join(segments[:i+1], "/"), true
		}
	}
	return "", false
}

// ScanGeneratedPendingDeletes groups pending delete events whose path matches
// the active safe-ignore generated-tree guard. It is read-only and accepts any
// query-capable SQLite handle so recovery CLIs can use read-only connections.
func ScanGeneratedPendingDeletes(ctx context.Context, q queryer, matcher *SafeIgnoreMatcher, limit int) ([]GeneratedPendingGroup, error) {
	if q == nil {
		return nil, fmt.Errorf("state: ScanGeneratedPendingDeletes: nil queryer")
	}
	if matcher == nil {
		matcher = NewSafeIgnoreMatcher()
	}
	sql := `
SELECT seq, branch_ref, branch_generation, base_head, path
FROM capture_events
WHERE state = ? AND operation = ?
ORDER BY seq ASC`
	args := []any{EventStatePending, "delete"}
	if limit > 0 {
		sql += " LIMIT ?"
		args = append(args, limit)
	}
	rows, err := q.QueryContext(ctx, sql, args...)
	if err != nil {
		return nil, fmt.Errorf("state: query generated pending deletes: %w", err)
	}
	defer rows.Close()

	type key struct {
		root, pattern, branchRef, baseHead string
		generation                         int64
	}
	groups := map[key]*GeneratedPendingGroup{}
	for rows.Next() {
		var seq int64
		var branchRef, baseHead, rel string
		var generation int64
		if err := rows.Scan(&seq, &branchRef, &generation, &baseHead, &rel); err != nil {
			return nil, fmt.Errorf("state: scan generated pending delete: %w", err)
		}
		if !matcher.MatchFile(rel) {
			continue
		}
		match, ok := matcher.MatchRoot(rel)
		if !ok {
			continue
		}
		k := key{
			root:       match.Root,
			pattern:    match.Pattern,
			branchRef:  branchRef,
			generation: generation,
			baseHead:   baseHead,
		}
		g := groups[k]
		if g == nil {
			g = &GeneratedPendingGroup{
				Root:             match.Root,
				Pattern:          match.Pattern,
				BranchRef:        branchRef,
				BranchGeneration: generation,
				BaseHead:         baseHead,
				OldestSeq:        seq,
			}
			groups[k] = g
		}
		g.PendingCount++
		g.NewestSeq = seq
		g.EventSeqs = append(g.EventSeqs, seq)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("state: iterate generated pending deletes: %w", err)
	}

	out := make([]GeneratedPendingGroup, 0, len(groups))
	for _, g := range groups {
		out = append(out, *g)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].OldestSeq != out[j].OldestSeq {
			return out[i].OldestSeq < out[j].OldestSeq
		}
		if out[i].Root != out[j].Root {
			return out[i].Root < out[j].Root
		}
		if out[i].BranchRef != out[j].BranchRef {
			return out[i].BranchRef < out[j].BranchRef
		}
		return out[i].BranchGeneration < out[j].BranchGeneration
	})
	return out, nil
}
