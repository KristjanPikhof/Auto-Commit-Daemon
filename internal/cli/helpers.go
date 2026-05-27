package cli

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/central"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/paths"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

// defaultClientTTL is the heartbeat freshness window per D21 (§7.6 stale
// overlay). Override via env ACD_CLIENT_TTL_SECONDS.
const defaultClientTTLSeconds = 1800

// clientTTL returns the configured heartbeat freshness window.
func clientTTL() time.Duration {
	if v := os.Getenv("ACD_CLIENT_TTL_SECONDS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return time.Duration(n) * time.Second
		}
	}
	return defaultClientTTLSeconds * time.Second
}

// parseSince parses a Go duration extended with the `d` suffix (Nd → N*24h).
// The suffix `y` is also accepted (Ny → N*365*24h) so `--since 1y` works as
// shown in the §7.8 examples.
//
// Plain Go durations (e.g. "24h", "90m") are passed through unchanged.
func parseSince(s string) (time.Duration, error) {
	if s == "" {
		return 0, errors.New("empty duration")
	}
	// Try direct ParseDuration first.
	if d, err := time.ParseDuration(s); err == nil {
		return d, nil
	}
	// Handle the d / y suffix: split off the unit, parse the prefix as int.
	last := s[len(s)-1]
	if last == 'd' || last == 'y' {
		prefix := s[:len(s)-1]
		n, err := strconv.Atoi(prefix)
		if err != nil {
			return 0, fmt.Errorf("invalid duration %q: %w", s, err)
		}
		if n < 0 {
			return 0, fmt.Errorf("duration must be non-negative: %q", s)
		}
		switch last {
		case 'd':
			return time.Duration(n) * 24 * time.Hour, nil
		case 'y':
			return time.Duration(n) * 365 * 24 * time.Hour, nil
		}
	}
	return 0, fmt.Errorf("invalid duration %q", s)
}

// resolveRepo returns the canonical Git worktree root for the supplied repo.
// If repo is empty, the current working directory is used.
func resolveRepo(repo string) (string, error) {
	wt, err := git.ResolveWorktree(context.Background(), repo)
	if err != nil {
		if errors.Is(err, git.ErrNotWorktree) {
			return "", fmt.Errorf("cli: repo %q is not inside a Git worktree: %w", repo, err)
		}
		return "", err
	}
	return wt.Root, nil
}

// lookupRegisteredRepo canonicalizes repo to the Git worktree root before
// loading the central registry and doing an exact registered-repo lookup. It
// only reads registry state; callers that must stay read-only can use it
// without creating or migrating per-repo state.
func lookupRegisteredRepo(command, repo string) (central.RepoRecord, paths.Roots, string, error) {
	wt, err := git.ResolveWorktree(context.Background(), repo)
	if err != nil {
		if errors.Is(err, git.ErrNotWorktree) {
			return central.RepoRecord{}, paths.Roots{}, "", fmt.Errorf("cli: repo %q is not inside a Git worktree: %w", repo, err)
		}
		return central.RepoRecord{}, paths.Roots{}, "", err
	}
	abs := wt.Root
	stateDB := state.DBPathFromGitDir(wt.GitDir)
	roots, err := paths.Resolve()
	if err != nil {
		return central.RepoRecord{}, paths.Roots{}, "", fmt.Errorf("acd %s: resolve paths: %w", command, err)
	}
	reg, err := central.Load(roots)
	if err != nil {
		return central.RepoRecord{}, paths.Roots{}, "", fmt.Errorf("acd %s: load registry: %w", command, err)
	}
	if rec, ok := findRepo(reg, abs, stateDB); ok {
		return rec, roots, abs, nil
	}
	return central.RepoRecord{}, paths.Roots{}, abs, fmt.Errorf("acd %s: repo %s is not registered (try `acd start --repo %s`)", command, abs, abs)
}

// formatDurationCompact renders a duration as "2s", "47s", "3m 14s",
// "3h 14m", "5d 7h", matching the §7.6 example output style. Negative
// durations clamp to zero.
func formatDurationCompact(d time.Duration) string {
	if d < 0 {
		d = 0
	}
	if d < time.Minute {
		return fmt.Sprintf("%ds", int(d.Seconds()))
	}
	if d < time.Hour {
		m := int(d / time.Minute)
		s := int((d % time.Minute) / time.Second)
		if s == 0 {
			return fmt.Sprintf("%dm", m)
		}
		return fmt.Sprintf("%dm %ds", m, s)
	}
	if d < 24*time.Hour {
		h := int(d / time.Hour)
		m := int((d % time.Hour) / time.Minute)
		if m == 0 {
			return fmt.Sprintf("%dh", h)
		}
		return fmt.Sprintf("%dh %dm", h, m)
	}
	days := int(d / (24 * time.Hour))
	hours := int((d % (24 * time.Hour)) / time.Hour)
	if hours == 0 {
		return fmt.Sprintf("%dd", days)
	}
	return fmt.Sprintf("%dd %dh", days, hours)
}

// formatBytesSigned renders a signed byte count as "+12.4 MB" / "-3.1 MB"
// for the §7.8 stats summary. Uses 1024-based units.
func formatBytesSigned(n int64) string {
	sign := "+"
	v := n
	if n < 0 {
		sign = "-"
		v = -n
	}
	const unit = 1024
	if v < unit {
		return fmt.Sprintf("%s%d B", sign, v)
	}
	div, exp := int64(unit), 0
	for x := v / unit; x >= unit; x /= unit {
		div *= unit
		exp++
	}
	suffixes := []string{"KB", "MB", "GB", "TB", "PB"}
	if exp >= len(suffixes) {
		exp = len(suffixes) - 1
	}
	return fmt.Sprintf("%s%.1f %s", sign, float64(v)/float64(div), suffixes[exp])
}

// formatThousands prints n with thousands separators ("1,847").
func formatThousands(n int64) string {
	if n < 0 {
		return "-" + formatThousands(-n)
	}
	s := strconv.FormatInt(n, 10)
	if len(s) <= 3 {
		return s
	}
	var b strings.Builder
	first := len(s) % 3
	if first > 0 {
		b.WriteString(s[:first])
		if len(s) > first {
			b.WriteByte(',')
		}
	}
	for i := first; i < len(s); i += 3 {
		b.WriteString(s[i : i+3])
		if i+3 < len(s) {
			b.WriteByte(',')
		}
	}
	return b.String()
}

// homeShort replaces a leading $HOME with "~" so paths render compactly
// ("~/repo-A") without losing absoluteness elsewhere.
func homeShort(p string) string {
	home, err := os.UserHomeDir()
	if err != nil || home == "" {
		return p
	}
	if p == home {
		return "~"
	}
	if strings.HasPrefix(p, home+string(os.PathSeparator)) {
		return "~" + p[len(home):]
	}
	return p
}

// fileExists returns true if the path exists (any kind of entry).
func fileExists(p string) bool {
	_, err := os.Stat(p)
	return err == nil
}

// pathTwoSegmentLabel returns the last two path segments joined with "/"
// (e.g. Development/Auto-Commit-Daemon). A single-segment path uses that name.
func pathTwoSegmentLabel(path string) string {
	clean := filepath.Clean(path)
	parts := strings.Split(clean, string(filepath.Separator))
	var segs []string
	for _, p := range parts {
		if p != "" {
			segs = append(segs, p)
		}
	}
	switch len(segs) {
	case 0:
		return "?"
	case 1:
		return segs[0]
	default:
		return segs[len(segs)-2] + "/" + segs[len(segs)-1]
	}
}

// buildListRepoLabelsCompact assigns a compact REPO column label per path. When
// two entries share the same two-segment label, each colliding row gets a "#"
// suffix with the last four characters of repo_hash.
func buildListRepoLabelsCompact(entries []listEntry) map[string]string {
	baseCounts := make(map[string]int)
	bases := make(map[string]string, len(entries))
	for _, e := range entries {
		base := pathTwoSegmentLabel(e.Path)
		bases[e.Path] = base
		baseCounts[base]++
	}
	labels := make(map[string]string, len(entries))
	for _, e := range entries {
		base := bases[e.Path]
		if baseCounts[base] > 1 {
			tail := e.RepoHash
			if len(tail) > 4 {
				tail = tail[len(tail)-4:]
			}
			labels[e.Path] = base + "#" + tail
			continue
		}
		labels[e.Path] = base
	}
	return labels
}

// listRepoLabelCompact returns the REPO column label for compact list output.
func listRepoLabelCompact(path, repoHash string, labels map[string]string) string {
	if label, ok := labels[path]; ok {
		return label
	}
	return pathTwoSegmentLabel(path)
}

// listStatusCompact maps list status strings to short dashboard tokens.
func listStatusCompact(status string) string {
	switch status {
	case "OK":
		return "OK"
	case "waiting":
		return "wait"
	case "blocked":
		return "blk"
	case "paused":
		return "pause"
	case "missing", "unreadable":
		return "miss"
	case "stale":
		return "stale"
	default:
		return status
	}
}

// listLastCommitShort renders the HEAD/LAST_COMMIT column (7-char oid prefix).
func listLastCommitShort(oid string) string {
	if oid == "" {
		return "-"
	}
	if len(oid) > 7 {
		return oid[:7]
	}
	return oid
}

// listRowMissing reports rows without readable state.db summary data.
func listRowMissing(status string) bool {
	return status == "missing" || status == "unreadable"
}
