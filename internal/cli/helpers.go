package cli

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
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

// resolveRepo returns the absolute, cleaned path of the supplied repo.
// If repo is empty, the current working directory is used.
func resolveRepo(repo string) (string, error) {
	if repo == "" {
		cwd, err := os.Getwd()
		if err != nil {
			return "", fmt.Errorf("cli: getwd: %w", err)
		}
		repo = cwd
	}
	abs, err := filepath.Abs(repo)
	if err != nil {
		return "", fmt.Errorf("cli: abs %q: %w", repo, err)
	}
	return filepath.Clean(abs), nil
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
