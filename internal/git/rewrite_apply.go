package git

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"
)

// RewriteApplyCommit is one saved message rewrite operation, ordered
// oldest-to-newest.
type RewriteApplyCommit struct {
	OldOID          string
	ProposedMessage string
}

// RewriteApplyOptions describes a saved rewrite plan application.
type RewriteApplyOptions struct {
	BranchRef    string
	ExpectedHead string
	PlanID       string
	Commits      []RewriteApplyCommit
	DryRun       bool
	Now          time.Time
}

// RewriteApplyResult reports refs and old->new commit mapping produced by an
// apply. Mapping includes selected commits and newer unchanged commits that had
// to be recreated with remapped parents.
type RewriteApplyResult struct {
	OldHead           string
	NewHead           string
	BackupBranchRef   string
	InternalBackupRef string
	CommitMap         map[string]string
	RecreatedCount    int
}

// ApplyRewritePlan safely applies a saved linear commit-message rewrite plan to
// the current branch. It rechecks clean worktree/operation markers, verifies the
// current branch and expected HEAD, creates backup refs before changing the
// branch, recreates selected commits and newer unchanged descendants with
// remapped parents, then CAS-updates the branch ref from ExpectedHead to NewHead.
func ApplyRewritePlan(ctx context.Context, repoDir string, opts RewriteApplyOptions) (RewriteApplyResult, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if opts.BranchRef == "" || opts.ExpectedHead == "" || len(opts.Commits) == 0 {
		return RewriteApplyResult{}, fmt.Errorf("git rewrite apply: missing required plan fields")
	}
	if err := ensureRewriteSafeRepo(ctx, repoDir); err != nil {
		return RewriteApplyResult{}, err
	}
	branchRef, err := RunBranchRef(ctx, repoDir)
	if err != nil {
		return RewriteApplyResult{}, fmt.Errorf("git rewrite apply: resolve HEAD branch: %w", err)
	}
	if branchRef == "" {
		return RewriteApplyResult{}, errors.New("git rewrite apply: detached HEAD; checkout the planned branch before applying")
	}
	if branchRef != opts.BranchRef {
		return RewriteApplyResult{}, fmt.Errorf("git rewrite apply: current branch %s does not match plan branch %s", branchRef, opts.BranchRef)
	}
	head, err := RevParse(ctx, repoDir, "HEAD")
	if err != nil {
		return RewriteApplyResult{}, fmt.Errorf("git rewrite apply: resolve HEAD: %w", err)
	}
	if head != opts.ExpectedHead {
		return RewriteApplyResult{}, fmt.Errorf("git rewrite apply: HEAD moved since plan creation (expected %s, got %s); regenerate the rewrite plan", shortApplyOID(opts.ExpectedHead), shortApplyOID(head))
	}
	if err := validateRewriteApplyChain(ctx, repoDir, opts); err != nil {
		return RewriteApplyResult{}, err
	}

	res := RewriteApplyResult{OldHead: head, CommitMap: make(map[string]string)}
	if opts.DryRun {
		return res, nil
	}

	backupBranch, err := createRewriteBackupRefs(ctx, repoDir, opts, head)
	if err != nil {
		return RewriteApplyResult{}, err
	}
	res.BackupBranchRef = backupBranch
	if opts.PlanID != "" {
		res.InternalBackupRef = "refs/acd/rewrite-backups/" + sanitizeRewriteRefPart(opts.PlanID)
	}

	parent, err := firstParent(ctx, repoDir, opts.Commits[0].OldOID)
	if err != nil {
		return RewriteApplyResult{}, err
	}
	newParent := parent
	for _, c := range opts.Commits {
		newOID, err := recreateCommitWithMessage(ctx, repoDir, c.OldOID, c.ProposedMessage, newParent)
		if err != nil {
			return res, err
		}
		res.CommitMap[c.OldOID] = newOID
		newParent = newOID
		res.RecreatedCount++
	}

	newer, err := firstParentDescendantsReverse(ctx, repoDir, opts.Commits[len(opts.Commits)-1].OldOID, head)
	if err != nil {
		return res, err
	}
	for _, oldOID := range newer {
		msg, err := commitMessage(ctx, repoDir, oldOID)
		if err != nil {
			return res, err
		}
		newOID, err := recreateCommitWithMessage(ctx, repoDir, oldOID, msg, newParent)
		if err != nil {
			return res, err
		}
		res.CommitMap[oldOID] = newOID
		newParent = newOID
		res.RecreatedCount++
	}
	res.NewHead = newParent
	if err := UpdateRef(ctx, repoDir, opts.BranchRef, res.NewHead, head); err != nil {
		return res, fmt.Errorf("git rewrite apply: CAS update %s: %w", opts.BranchRef, err)
	}
	return res, nil
}

func validateRewriteApplyChain(ctx context.Context, repoDir string, opts RewriteApplyOptions) error {
	seen := map[string]struct{}{}
	for i, c := range opts.Commits {
		if c.OldOID == "" || strings.TrimSpace(c.ProposedMessage) == "" {
			return fmt.Errorf("git rewrite apply: commit %d missing oid or proposed message", i)
		}
		if _, ok := seen[c.OldOID]; ok {
			return fmt.Errorf("git rewrite apply: duplicate commit %s in plan", shortApplyOID(c.OldOID))
		}
		seen[c.OldOID] = struct{}{}
		if err := rejectMergeCommit(ctx, repoDir, c.OldOID); err != nil {
			return err
		}
		if i > 0 {
			p, err := firstParent(ctx, repoDir, c.OldOID)
			if err != nil {
				return err
			}
			if p != opts.Commits[i-1].OldOID {
				return fmt.Errorf("git rewrite apply: plan commits are not contiguous at %s", shortApplyOID(c.OldOID))
			}
		}
	}
	isAnc, err := IsAncestor(ctx, repoDir, opts.Commits[len(opts.Commits)-1].OldOID, opts.ExpectedHead)
	if err != nil {
		return fmt.Errorf("git rewrite apply: verify selected commit ancestry: %w", err)
	}
	if !isAnc {
		return fmt.Errorf("git rewrite apply: selected commits are not ancestors of expected HEAD")
	}
	newer, err := firstParentDescendantsReverse(ctx, repoDir, opts.Commits[len(opts.Commits)-1].OldOID, opts.ExpectedHead)
	if err != nil {
		return err
	}
	for _, oid := range newer {
		if err := rejectMergeCommit(ctx, repoDir, oid); err != nil {
			return err
		}
	}
	return nil
}

func createRewriteBackupRefs(ctx context.Context, repoDir string, opts RewriteApplyOptions, head string) (string, error) {
	now := opts.Now
	if now.IsZero() {
		now = time.Now()
	}
	base := "refs/heads/acd-backup/rewrite-" + now.Format("20060102-150405")
	backup := base
	for i := 0; ; i++ {
		if i > 0 {
			backup = fmt.Sprintf("%s-%02d", base, i)
		}
		exists, err := RefExists(ctx, repoDir, backup)
		if err != nil {
			return "", fmt.Errorf("git rewrite apply: check backup ref: %w", err)
		}
		if !exists {
			break
		}
	}
	if err := UpdateRef(ctx, repoDir, backup, head, ""); err != nil {
		return "", fmt.Errorf("git rewrite apply: create backup branch %s: %w", backup, err)
	}
	if opts.PlanID != "" {
		internal := "refs/acd/rewrite-backups/" + sanitizeRewriteRefPart(opts.PlanID)
		if err := UpdateRef(ctx, repoDir, internal, head, ""); err != nil {
			return "", fmt.Errorf("git rewrite apply: create internal backup ref %s: %w", internal, err)
		}
	}
	return backup, nil
}

func firstParentDescendantsReverse(ctx context.Context, repoDir, ancestor, head string) ([]string, error) {
	out, err := Run(ctx, RunOpts{Dir: repoDir, Timeout: DefaultReadTimeout}, "rev-list", "--first-parent", "--reverse", ancestor+".."+head)
	if err != nil {
		return nil, fmt.Errorf("git rewrite apply: list newer commits: %w", err)
	}
	return parseRevList(out), nil
}

func recreateCommitWithMessage(ctx context.Context, repoDir, oldOID, message, parent string) (string, error) {
	tree, err := commitTreeOID(ctx, repoDir, oldOID)
	if err != nil {
		return "", err
	}
	author, err := commitAuthorEnv(ctx, repoDir, oldOID)
	if err != nil {
		return "", err
	}
	parents := []string{}
	if parent != "" {
		parents = append(parents, parent)
	}
	newOID, err := commitTreeWithEnv(ctx, repoDir, tree, strings.TrimRight(message, "\n"), author, parents...)
	if err != nil {
		return "", fmt.Errorf("git rewrite apply: recreate commit %s: %w", shortApplyOID(oldOID), err)
	}
	return newOID, nil
}

func commitTreeWithEnv(ctx context.Context, repoDir, treeOID, message string, env map[string]string, parents ...string) (string, error) {
	args := []string{"commit-tree", treeOID}
	for _, p := range parents {
		args = append(args, "-p", p)
	}
	args = append(args, "-F", "-")
	out, err := Run(ctx, RunOpts{Dir: repoDir, Stdin: strings.NewReader(message), ExtraEnv: env}, args...)
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(out)), nil
}

func commitAuthorEnv(ctx context.Context, repoDir, oid string) (map[string]string, error) {
	out, err := Run(ctx, RunOpts{Dir: repoDir, Timeout: DefaultReadTimeout}, "show", "-s", "--format=%an%x00%ae%x00%aI", oid)
	if err != nil {
		return nil, fmt.Errorf("git rewrite apply: read author for %s: %w", shortApplyOID(oid), err)
	}
	parts := strings.Split(strings.TrimRight(string(out), "\n"), "\x00")
	if len(parts) != 3 || parts[0] == "" || parts[1] == "" || parts[2] == "" {
		return nil, fmt.Errorf("git rewrite apply: malformed author metadata for %s", shortApplyOID(oid))
	}
	return map[string]string{
		"GIT_AUTHOR_NAME":  parts[0],
		"GIT_AUTHOR_EMAIL": parts[1],
		"GIT_AUTHOR_DATE":  parts[2],
	}, nil
}

func commitTreeOID(ctx context.Context, repoDir, oid string) (string, error) {
	out, err := Run(ctx, RunOpts{Dir: repoDir, Timeout: DefaultReadTimeout}, "show", "-s", "--format=%T", oid)
	if err != nil {
		return "", fmt.Errorf("git rewrite apply: read tree for %s: %w", shortApplyOID(oid), err)
	}
	return strings.TrimSpace(string(out)), nil
}

func commitMessage(ctx context.Context, repoDir, oid string) (string, error) {
	out, err := Run(ctx, RunOpts{Dir: repoDir, Timeout: DefaultReadTimeout}, "show", "-s", "--format=%B", oid)
	if err != nil {
		return "", fmt.Errorf("git rewrite apply: read message for %s: %w", shortApplyOID(oid), err)
	}
	return strings.TrimRight(string(out), "\n"), nil
}

func firstParent(ctx context.Context, repoDir, oid string) (string, error) {
	parents, err := parentsOf(ctx, repoDir, oid)
	if err != nil {
		return "", err
	}
	if len(parents) == 0 {
		return "", nil
	}
	return parents[0], nil
}

func rejectMergeCommit(ctx context.Context, repoDir, oid string) error {
	parents, err := parentsOf(ctx, repoDir, oid)
	if err != nil {
		return err
	}
	if len(parents) > 1 {
		return fmt.Errorf("git rewrite apply: merge commit %s is in the recreated chain; merges are not supported", shortApplyOID(oid))
	}
	return nil
}

func parentsOf(ctx context.Context, repoDir, oid string) ([]string, error) {
	out, err := Run(ctx, RunOpts{Dir: repoDir, Timeout: DefaultReadTimeout}, "show", "-s", "--format=%P", oid)
	if err != nil {
		return nil, fmt.Errorf("git rewrite apply: inspect parents for %s: %w", shortApplyOID(oid), err)
	}
	return strings.Fields(string(out)), nil
}

func sanitizeRewriteRefPart(s string) string {
	var b strings.Builder
	for _, r := range s {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '-' || r == '_' || r == '.' {
			b.WriteRune(r)
		} else {
			b.WriteByte('-')
		}
	}
	if b.Len() == 0 {
		return "unknown"
	}
	return b.String()
}

func shortApplyOID(oid string) string {
	if len(oid) > 12 {
		return oid[:12]
	}
	return oid
}
