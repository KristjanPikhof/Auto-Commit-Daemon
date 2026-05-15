package git

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"unicode/utf8"
)

// History defaults keep recent commit context small enough to include in AI
// prompt payloads without letting a large repository or long subject line
// dominate the request.
const (
	DefaultHistoryLimit     = 8
	MaxHistoryLimit         = 32
	MaxHistoryPathspecs     = 64
	HistorySubjectByteCap   = 240
	HistoryPathByteCap      = 160
	MaxHistoryTouchedPaths  = 16
	historySubjectReadBytes = HistorySubjectByteCap + 1
)

// CommitSummary is a compact, prompt-safe description of a recent commit.
// TouchedPaths is populated by LatestPathCommitSummaries when git can cheaply
// report which requested paths or directories were relevant to the commit.
type CommitSummary struct {
	ShortOID     string
	Subject      string
	TouchedPaths []string
}

// CompactString renders the summary into a single line suitable for
// ai.CommitContext.Commits.
func (c CommitSummary) CompactString() string {
	subject := strings.TrimSpace(c.Subject)
	if subject == "" {
		subject = "(no subject)"
	}
	prefix := strings.TrimSpace(c.ShortOID)
	if prefix != "" {
		prefix += " "
	}
	if len(c.TouchedPaths) == 0 {
		return prefix + subject
	}
	return fmt.Sprintf("%s%s (touches: %s)", prefix, subject, strings.Join(c.TouchedPaths, ", "))
}

// FormatCommitSummaries renders compact commit summaries for callers that
// need string-only context payloads.
func FormatCommitSummaries(commits []CommitSummary) []string {
	if len(commits) == 0 {
		return nil
	}
	out := make([]string, 0, len(commits))
	for _, commit := range commits {
		out = append(out, commit.CompactString())
	}
	return out
}

// LatestBranchCommitSummaries returns the latest commits reachable from ref,
// newest first. Missing or ambiguous refs return an empty slice plus an error
// that wraps ErrRefNotFound or ErrRefAmbiguous so callers can log a warning and
// continue without commit context.
func LatestBranchCommitSummaries(ctx context.Context, repoDir, ref string, limit int) ([]CommitSummary, error) {
	limit = clampHistoryLimit(limit)
	if limit == 0 {
		return nil, nil
	}
	commit, err := resolveHistoryCommit(ctx, repoDir, ref)
	if err != nil {
		return nil, err
	}
	oids, err := revList(ctx, repoDir, commit, limit, nil)
	if err != nil {
		return nil, err
	}
	return summariesForOIDs(ctx, repoDir, oids, nil)
}

// PathHeadCommit describes the latest commit reachable from a ref that
// touched a single repo-relative path. It is intentionally narrower than
// CommitSummary — the recent-commit-affinity hint only needs the OID and
// commit timestamp to compute the age window.
type PathHeadCommit struct {
	OID         string // full 40-char commit OID; empty when not found.
	CommitTSSec int64  // committer timestamp, Unix seconds; 0 when not found.
}

// LatestPathHeadCommit returns the most recent commit reachable from ref
// that touched the supplied repo-relative path, plus its committer
// timestamp. Returns (PathHeadCommit{}, false, nil) when no commit reachable
// from ref touches the path; ErrRefNotFound / ErrRefAmbiguous propagate so
// callers can degrade gracefully when ref is missing.
//
// Why a dedicated helper rather than reusing LatestPathCommitSummaries:
// callers building the planner's prior-commit affinity hint only need OID +
// committer timestamp per offered path, never the touched-path list or
// subject. Reusing LatestPathCommitSummaries would shell out an extra
// `git diff-tree` per OID per path, which the planner request does not
// consume; this helper bounds the work to one `git log -1 --format=%H %ct`
// per path.
func LatestPathHeadCommit(ctx context.Context, repoDir, ref, path string) (PathHeadCommit, bool, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return PathHeadCommit{}, false, nil
	}
	commit, err := resolveHistoryCommit(ctx, repoDir, ref)
	if err != nil {
		return PathHeadCommit{}, false, err
	}
	args := []string{
		"log",
		"-1",
		"--no-merges",
		"--format=%H %ct",
		commit,
		"--",
		LiteralPathspec(path),
	}
	out, err := RunWithLimit(ctx, RunOpts{Dir: repoDir, Timeout: DefaultReadTimeout},
		// Full SHA + space + epoch + LF easily fits in 128 bytes; leave headroom.
		256, args...)
	if err != nil {
		return PathHeadCommit{}, false, fmt.Errorf("git history path-head: %w", err)
	}
	line := strings.TrimSpace(string(out))
	if line == "" {
		return PathHeadCommit{}, false, nil
	}
	fields := strings.Fields(line)
	if len(fields) < 2 {
		return PathHeadCommit{}, false, nil
	}
	oid := strings.TrimSpace(fields[0])
	if oid == "" {
		return PathHeadCommit{}, false, nil
	}
	ts, perr := parseInt64(fields[1])
	if perr != nil {
		// Treat malformed timestamp as "not found" — never poison the hint.
		return PathHeadCommit{}, false, nil
	}
	return PathHeadCommit{OID: oid, CommitTSSec: ts}, true, nil
}

func parseInt64(s string) (int64, error) {
	var n int64
	for _, r := range s {
		if r < '0' || r > '9' {
			return 0, fmt.Errorf("non-digit %q", r)
		}
		n = n*10 + int64(r-'0')
	}
	return n, nil
}

// LatestPathCommitSummaries returns recent commits reachable from ref that
// touched at least one of the supplied repo-relative paths or directories.
// Returned summaries include the touched paths when diff-tree can report them
// within the compact output budget.
func LatestPathCommitSummaries(ctx context.Context, repoDir, ref string, paths []string, limit int) ([]CommitSummary, error) {
	limit = clampHistoryLimit(limit)
	pathspecs := normalizeHistoryPathspecs(paths)
	if limit == 0 || len(pathspecs) == 0 {
		return nil, nil
	}
	commit, err := resolveHistoryCommit(ctx, repoDir, ref)
	if err != nil {
		return nil, err
	}
	oids, err := revList(ctx, repoDir, commit, limit, pathspecs)
	if err != nil {
		return nil, err
	}
	return summariesForOIDs(ctx, repoDir, oids, pathspecs)
}

func clampHistoryLimit(limit int) int {
	if limit <= 0 {
		return 0
	}
	if limit > MaxHistoryLimit {
		return MaxHistoryLimit
	}
	return limit
}

func resolveHistoryCommit(ctx context.Context, repoDir, ref string) (string, error) {
	ref = strings.TrimSpace(ref)
	if ref == "" {
		ref = "HEAD"
	}
	commit, err := RevParse(ctx, repoDir, ref+"^{commit}")
	if err != nil {
		return "", fmt.Errorf("git history context unavailable for ref %q: %w", ref, err)
	}
	return commit, nil
}

func revList(ctx context.Context, repoDir, commit string, limit int, pathspecs []string) ([]string, error) {
	args := []string{"rev-list", fmt.Sprintf("--max-count=%d", limit), commit}
	if len(pathspecs) > 0 {
		args = append(args, "--")
		args = append(args, pathspecs...)
	}
	out, err := RunWithLimit(ctx, RunOpts{Dir: repoDir, Timeout: DefaultReadTimeout}, revListOutputCap(limit), args...)
	if err != nil {
		return nil, fmt.Errorf("git history rev-list: %w", err)
	}
	return parseRevList(out), nil
}

func revListOutputCap(limit int) int64 {
	// Full SHA + LF for each requested commit, plus headroom for future git
	// decorations or slightly different hash sizes.
	return int64(limit*64 + 256)
}

func parseRevList(out []byte) []string {
	fields := strings.Fields(string(out))
	if len(fields) == 0 {
		return nil
	}
	oids := make([]string, 0, len(fields))
	for _, field := range fields {
		if field != "" {
			oids = append(oids, field)
		}
	}
	return oids
}

func summariesForOIDs(ctx context.Context, repoDir string, oids []string, pathspecs []string) ([]CommitSummary, error) {
	if len(oids) == 0 {
		return nil, nil
	}
	summaries := make([]CommitSummary, 0, len(oids))
	for _, oid := range oids {
		subject, err := commitSubject(ctx, repoDir, oid)
		if err != nil {
			return summaries, err
		}
		summary := CommitSummary{
			ShortOID: shortCommitOID(oid),
			Subject:  subject,
		}
		if len(pathspecs) > 0 {
			touched, err := touchedPathsForCommit(ctx, repoDir, oid, pathspecs)
			if err != nil {
				return summaries, err
			}
			summary.TouchedPaths = touched
		}
		summaries = append(summaries, summary)
	}
	return summaries, nil
}

func commitSubject(ctx context.Context, repoDir, oid string) (string, error) {
	out, err := RunWithLimit(ctx, RunOpts{Dir: repoDir, Timeout: DefaultReadTimeout}, historySubjectReadBytes,
		"show", "-s", "--format=%s", oid,
	)
	if err != nil && !errors.Is(err, ErrStdoutOverflow) {
		return "", fmt.Errorf("git history subject %s: %w", shortCommitOID(oid), err)
	}
	return compactHistoryText(string(out), HistorySubjectByteCap, "(no subject)"), nil
}

func touchedPathsForCommit(ctx context.Context, repoDir, oid string, pathspecs []string) ([]string, error) {
	args := []string{"diff-tree", "--root", "--no-commit-id", "--name-only", "-r", "-z", oid, "--"}
	args = append(args, pathspecs...)
	out, err := RunWithLimit(ctx, RunOpts{Dir: repoDir, Timeout: DefaultReadTimeout}, touchedPathsOutputCap(), args...)
	overflow := errors.Is(err, ErrStdoutOverflow)
	if err != nil && !overflow {
		return nil, fmt.Errorf("git history touched paths %s: %w", shortCommitOID(oid), err)
	}
	return parseTouchedPaths(out, overflow), nil
}

func touchedPathsOutputCap() int64 {
	return int64(MaxHistoryTouchedPaths*(HistoryPathByteCap+1) + 512)
}

func parseTouchedPaths(out []byte, overflow bool) []string {
	if len(out) == 0 {
		return nil
	}
	records := bytes.Split(out, []byte{0})
	if overflow && out[len(out)-1] != 0 && len(records) > 0 {
		records = records[:len(records)-1]
	}
	seen := make(map[string]struct{}, len(records))
	paths := make([]string, 0, min(len(records), MaxHistoryTouchedPaths))
	for _, rec := range records {
		if len(rec) == 0 {
			continue
		}
		path := compactHistoryPath(string(rec))
		if path == "" {
			continue
		}
		if _, ok := seen[path]; ok {
			continue
		}
		seen[path] = struct{}{}
		paths = append(paths, path)
		if len(paths) >= MaxHistoryTouchedPaths {
			break
		}
	}
	return paths
}

func normalizeHistoryPathspecs(paths []string) []string {
	if len(paths) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, min(len(paths), MaxHistoryPathspecs))
	out := make([]string, 0, min(len(paths), MaxHistoryPathspecs))
	for _, path := range paths {
		path = strings.TrimSpace(filepath.ToSlash(path))
		path = strings.TrimPrefix(path, "./")
		if path == "" {
			continue
		}
		if _, ok := seen[path]; ok {
			continue
		}
		seen[path] = struct{}{}
		out = append(out, LiteralPathspec(path))
		if len(out) >= MaxHistoryPathspecs {
			break
		}
	}
	return out
}

func compactHistoryPath(path string) string {
	path = filepath.ToSlash(path)
	return compactHistoryText(path, HistoryPathByteCap, "")
}

func compactHistoryText(s string, maxBytes int, empty string) string {
	var b strings.Builder
	for _, r := range s {
		switch {
		case r == '\n' || r == '\r' || r == '\t':
			b.WriteByte(' ')
		case r < 0x20 || r == 0x7f:
			continue
		default:
			b.WriteRune(r)
		}
	}
	clean := strings.Join(strings.Fields(b.String()), " ")
	clean = truncateUTF8Bytes(clean, maxBytes)
	if clean == "" {
		return empty
	}
	return clean
}

func truncateUTF8Bytes(s string, maxBytes int) string {
	if maxBytes <= 0 || len(s) <= maxBytes {
		return s
	}
	cut := 0
	for cut < len(s) {
		r, size := utf8.DecodeRuneInString(s[cut:])
		if r == utf8.RuneError && size == 0 {
			break
		}
		if cut+size > maxBytes {
			break
		}
		cut += size
	}
	if cut == 0 {
		return ""
	}
	return strings.TrimSpace(s[:cut])
}

func shortCommitOID(oid string) string {
	oid = strings.TrimSpace(oid)
	if len(oid) <= 12 {
		return oid
	}
	return oid[:12]
}
