package git

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// RewriteSelectionOptions describes one user-facing way to select commits
// whose messages will be rewritten. Exactly one selector must be set.
type RewriteSelectionOptions struct {
	// From accepts either a commit-ish (full/short SHA, ref) or a 1-based
	// first-parent position where 1 is HEAD. It selects that commit through HEAD.
	From string
	// FromSHA accepts only a commit-ish and selects that commit through HEAD.
	// It exists so all-digit short SHAs are never interpreted as positions.
	FromSHA string
	// FromPosition selects the commit at a 1-based first-parent position through
	// HEAD, where 1 is HEAD.
	FromPosition int
	// Range is a 1-based first-parent position range "start-end" where 1 is
	// HEAD. The start position must be newer than or equal to the end position;
	// selecting 5-12 records positions 5..12 for rewrite and 1..4 for unchanged
	// recreation.
	Range string
	// Last selects the newest N first-parent commits.
	Last int
	// GitRange is an advanced git rev-list revset. The resulting commits must be
	// a single contiguous range on the current branch's first-parent chain.
	GitRange string
}

// RewriteCommitRecord is a stable snapshot of a commit participating in a
// rewrite plan. Message is the full original commit message body for selected
// commits and for newer commits that must be recreated unchanged.
type RewriteCommitRecord struct {
	OID     string `json:"oid"`
	Subject string `json:"subject"`
	Message string `json:"message"`
}

// RewriteSelection is the resolved, stable commit set for a message rewrite.
// Commits are ordered oldest-to-newest so callers can replay them directly.
type RewriteSelection struct {
	BranchRef           string                `json:"branch_ref"`
	Head                string                `json:"head"`
	Selected            []RewriteCommitRecord `json:"selected"`
	RecreateUnchanged   []RewriteCommitRecord `json:"recreate_unchanged"`
	SelectedNewestIndex int                   `json:"selected_newest_position"`
	SelectedOldestIndex int                   `json:"selected_oldest_position"`
}

// ResolveRewriteSelection validates repository safety and resolves a user
// selection to stable commit OIDs/messages. It is read-only and intentionally
// does not perform any rewrite/apply work.
func ResolveRewriteSelection(ctx context.Context, repoDir string, opts RewriteSelectionOptions) (RewriteSelection, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := validateRewriteSelector(opts); err != nil {
		return RewriteSelection{}, err
	}
	if err := ensureRewriteSafeRepo(ctx, repoDir); err != nil {
		return RewriteSelection{}, err
	}
	branchRef, err := RunBranchRef(ctx, repoDir)
	if err != nil {
		return RewriteSelection{}, fmt.Errorf("git rewrite selection: resolve HEAD branch: %w", err)
	}
	if branchRef == "" {
		return RewriteSelection{}, errors.New("git rewrite selection: detached HEAD; checkout a branch before rewriting commits")
	}
	head, err := RevParse(ctx, repoDir, "HEAD")
	if err != nil {
		if errors.Is(err, ErrRefNotFound) {
			return RewriteSelection{}, errors.New("git rewrite selection: no commits on branch")
		}
		return RewriteSelection{}, fmt.Errorf("git rewrite selection: resolve HEAD: %w", err)
	}
	chain, err := firstParentChain(ctx, repoDir)
	if err != nil {
		return RewriteSelection{}, err
	}
	if len(chain) == 0 {
		return RewriteSelection{}, errors.New("git rewrite selection: no commits on branch")
	}

	newest, oldest, err := resolveRewriteBounds(ctx, repoDir, opts, chain)
	if err != nil {
		return RewriteSelection{}, err
	}
	if newest < 0 || oldest < newest || oldest >= len(chain) {
		return RewriteSelection{}, fmt.Errorf("git rewrite selection: invalid selection bounds %d-%d", newest+1, oldest+1)
	}
	involvedNewestFirst := append([]string{}, chain[:oldest+1]...)
	if err := rejectMergeCommits(ctx, repoDir, involvedNewestFirst); err != nil {
		return RewriteSelection{}, err
	}
	selected, err := commitRecords(ctx, repoDir, reverseStrings(chain[newest:oldest+1]))
	if err != nil {
		return RewriteSelection{}, err
	}
	recreate, err := commitRecords(ctx, repoDir, reverseStrings(chain[:newest]))
	if err != nil {
		return RewriteSelection{}, err
	}
	return RewriteSelection{
		BranchRef:           branchRef,
		Head:                head,
		Selected:            selected,
		RecreateUnchanged:   recreate,
		SelectedNewestIndex: newest + 1,
		SelectedOldestIndex: oldest + 1,
	}, nil
}

func validateRewriteSelector(opts RewriteSelectionOptions) error {
	count := 0
	if strings.TrimSpace(opts.From) != "" {
		count++
	}
	if strings.TrimSpace(opts.FromSHA) != "" {
		count++
	}
	if opts.FromPosition > 0 {
		count++
	}
	if strings.TrimSpace(opts.Range) != "" {
		count++
	}
	if opts.Last > 0 {
		count++
	}
	if strings.TrimSpace(opts.GitRange) != "" {
		count++
	}
	if opts.Last < 0 {
		return errors.New("git rewrite selection: --last must be positive")
	}
	if opts.FromPosition < 0 {
		return errors.New("git rewrite selection: --from-nr must be positive")
	}
	if count != 1 {
		return errors.New("git rewrite selection: specify exactly one of --from-sha, --from-nr, --range-nr, --range-sha, --last, --from, --range, or --git-range")
	}
	return nil
}

func ensureRewriteSafeRepo(ctx context.Context, repoDir string) error {
	out, err := Run(ctx, RunOpts{Dir: repoDir, Timeout: DefaultReadTimeout}, "status", "--porcelain=v1", "--untracked-files=all")
	if err != nil {
		return fmt.Errorf("git rewrite selection: inspect worktree status: %w", err)
	}
	if strings.TrimSpace(string(out)) != "" {
		return errors.New("git rewrite selection: dirty index or worktree; commit or stash changes before rewriting commits")
	}
	gitDir, err := AbsoluteGitDir(ctx, repoDir)
	if err != nil {
		return fmt.Errorf("git rewrite selection: resolve git dir: %w", err)
	}
	if name, active := gitOperationMarker(gitDir); active {
		return fmt.Errorf("git rewrite selection: refusing while git operation %q is in progress", name)
	}
	return nil
}

func gitOperationMarker(gitDir string) (string, bool) {
	markers := []struct{ name, rel string }{
		{"merge", "MERGE_HEAD"}, {"cherry-pick", "CHERRY_PICK_HEAD"}, {"revert", "REVERT_HEAD"},
		{"bisect", "BISECT_LOG"}, {"rebase", "rebase-merge"}, {"rebase", "rebase-apply"},
	}
	for _, m := range markers {
		if _, err := os.Stat(filepath.Join(gitDir, m.rel)); err == nil {
			return m.name, true
		}
	}
	return "", false
}

func firstParentChain(ctx context.Context, repoDir string) ([]string, error) {
	out, err := Run(ctx, RunOpts{Dir: repoDir, Timeout: DefaultReadTimeout}, "rev-list", "--first-parent", "HEAD")
	if err != nil {
		return nil, fmt.Errorf("git rewrite selection: list first-parent history: %w", err)
	}
	return parseRevList(out), nil
}

func resolveRewriteBounds(ctx context.Context, repoDir string, opts RewriteSelectionOptions, chain []string) (newest int, oldest int, err error) {
	if opts.Last > 0 {
		if opts.Last > len(chain) {
			return 0, 0, fmt.Errorf("git rewrite selection: --last %d exceeds branch history length %d", opts.Last, len(chain))
		}
		return 0, opts.Last - 1, nil
	}
	if opts.FromPosition > 0 {
		if opts.FromPosition > len(chain) {
			return 0, 0, fmt.Errorf("git rewrite selection: --from-nr %d exceeds branch history length %d", opts.FromPosition, len(chain))
		}
		return 0, opts.FromPosition - 1, nil
	}
	if r := strings.TrimSpace(opts.Range); r != "" {
		start, end, err := parsePositionRange(r)
		if err != nil {
			return 0, 0, err
		}
		if start > len(chain) || end > len(chain) {
			return 0, 0, fmt.Errorf("git rewrite selection: range %d-%d exceeds branch history length %d", start, end, len(chain))
		}
		return start - 1, end - 1, nil
	}
	if from := strings.TrimSpace(opts.FromSHA); from != "" {
		pos, err := resolveFromCommit(ctx, repoDir, from, chain)
		if err != nil {
			return 0, 0, err
		}
		return 0, pos, nil
	}
	if from := strings.TrimSpace(opts.From); from != "" {
		pos, err := resolveFromPositionOrCommit(ctx, repoDir, from, chain)
		if err != nil {
			return 0, 0, err
		}
		return 0, pos, nil
	}
	return resolveGitRangeBounds(ctx, repoDir, strings.TrimSpace(opts.GitRange), chain)
}

func parsePositionRange(s string) (int, int, error) {
	parts := strings.Split(s, "-")
	if len(parts) != 2 || strings.TrimSpace(parts[0]) == "" || strings.TrimSpace(parts[1]) == "" {
		return 0, 0, fmt.Errorf("git rewrite selection: ambiguous --range %q; use start-end positions", s)
	}
	start, err := strconv.Atoi(strings.TrimSpace(parts[0]))
	if err != nil || start <= 0 {
		return 0, 0, fmt.Errorf("git rewrite selection: invalid range start %q", parts[0])
	}
	end, err := strconv.Atoi(strings.TrimSpace(parts[1]))
	if err != nil || end <= 0 {
		return 0, 0, fmt.Errorf("git rewrite selection: invalid range end %q", parts[1])
	}
	if start > end {
		return 0, 0, fmt.Errorf("git rewrite selection: ambiguous --range %q; positions must be newest-to-oldest (for example 5-12)", s)
	}
	return start, end, nil
}

func resolveFromPositionOrCommit(ctx context.Context, repoDir, from string, chain []string) (int, error) {
	var positionErr error
	if n, err := strconv.Atoi(from); err == nil {
		if n <= 0 {
			return 0, fmt.Errorf("git rewrite selection: --from position must be positive")
		}
		if n <= len(chain) {
			return n - 1, nil
		}
		positionErr = fmt.Errorf("git rewrite selection: --from position %d exceeds branch history length %d", n, len(chain))
	}
	pos, err := resolveFromCommit(ctx, repoDir, from, chain)
	if err != nil && positionErr != nil {
		return 0, positionErr
	}
	return pos, err
}

func resolveFromCommit(ctx context.Context, repoDir, from string, chain []string) (int, error) {
	oid, err := RevParse(ctx, repoDir, from+"^{commit}")
	if err != nil {
		return 0, fmt.Errorf("git rewrite selection: resolve --from %q: %w", from, err)
	}
	for i, c := range chain {
		if c == oid {
			return i, nil
		}
	}
	return 0, fmt.Errorf("git rewrite selection: --from commit %s is not on the current branch first-parent history", shortCommitOID(oid))
}

func resolveGitRangeBounds(ctx context.Context, repoDir, revset string, chain []string) (int, int, error) {
	args := append([]string{"rev-list"}, strings.Fields(revset)...)
	out, err := Run(ctx, RunOpts{Dir: repoDir, Timeout: DefaultReadTimeout}, args...)
	if err != nil {
		return 0, 0, fmt.Errorf("git rewrite selection: resolve --git-range %q: %w", revset, err)
	}
	selected := parseRevList(out)
	if len(selected) == 0 {
		return 0, 0, fmt.Errorf("git rewrite selection: --git-range %q selected no commits", revset)
	}
	posByOID := make(map[string]int, len(chain))
	for i, oid := range chain {
		posByOID[oid] = i
	}
	minPos, maxPos := len(chain), -1
	seen := make(map[int]struct{}, len(selected))
	for _, oid := range selected {
		pos, ok := posByOID[oid]
		if !ok {
			return 0, 0, fmt.Errorf("git rewrite selection: --git-range selected commit %s outside current branch first-parent history", shortCommitOID(oid))
		}
		seen[pos] = struct{}{}
		if pos < minPos {
			minPos = pos
		}
		if pos > maxPos {
			maxPos = pos
		}
	}
	if len(seen) != maxPos-minPos+1 {
		return 0, 0, fmt.Errorf("git rewrite selection: ambiguous --git-range %q; selected commits must be contiguous on current branch", revset)
	}
	return minPos, maxPos, nil
}

func rejectMergeCommits(ctx context.Context, repoDir string, newestFirst []string) error {
	for _, oid := range newestFirst {
		out, err := Run(ctx, RunOpts{Dir: repoDir, Timeout: DefaultReadTimeout}, "rev-list", "--parents", "-n", "1", oid)
		if err != nil {
			return fmt.Errorf("git rewrite selection: inspect parents for %s: %w", shortCommitOID(oid), err)
		}
		fields := strings.Fields(string(out))
		if len(fields) > 2 {
			return fmt.Errorf("git rewrite selection: merge commit %s is in the selected or recreated chain; merges are not supported", shortCommitOID(oid))
		}
	}
	return nil
}

func commitRecords(ctx context.Context, repoDir string, oids []string) ([]RewriteCommitRecord, error) {
	if len(oids) == 0 {
		return nil, nil
	}
	records := make([]RewriteCommitRecord, 0, len(oids))
	for _, oid := range oids {
		msg, err := Run(ctx, RunOpts{Dir: repoDir, Timeout: DefaultReadTimeout}, "show", "-s", "--format=%B", oid)
		if err != nil {
			return nil, fmt.Errorf("git rewrite selection: read message for %s: %w", shortCommitOID(oid), err)
		}
		subj, err := commitSubject(ctx, repoDir, oid)
		if err != nil {
			return nil, err
		}
		records = append(records, RewriteCommitRecord{OID: oid, Subject: subj, Message: strings.TrimRight(string(msg), "\n")})
	}
	return records, nil
}

func reverseStrings(in []string) []string {
	out := make([]string, len(in))
	for i := range in {
		out[len(in)-1-i] = in[i]
	}
	return out
}
