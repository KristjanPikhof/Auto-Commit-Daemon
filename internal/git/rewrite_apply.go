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

// RewriteApplyGroup replaces one or more adjacent commits with one commit.
type RewriteApplyGroup struct {
	OldOIDs         []string
	ProposedMessage string
}

// RewriteApplyOptions describes a saved rewrite plan application.
type RewriteApplyOptions struct {
	BranchRef    string
	ExpectedHead string
	PlanID       string
	Groups       []RewriteApplyGroup
	// Commits is retained for callers applying legacy message-only plans.
	Commits  []RewriteApplyCommit
	DryRun   bool
	Now      time.Time
	Progress func(RewriteApplyProgress) error
}

// RewriteApplyProgress reports durable milestones during rewrite apply.
type RewriteApplyProgress struct {
	Phase     string
	Message   string
	Current   int
	Total     int
	OldOID    string
	NewOID    string
	BackupRef string
}

// RewriteApplyResult reports refs and old->new commit mapping produced by an
// apply. Mapping includes selected commits and newer unchanged commits that had
// to be recreated with remapped parents.
type RewriteApplyResult struct {
	OldHead                  string
	NewHead                  string
	BackupBranchRef          string
	InternalBackupRef        string
	CommitMap                map[string]string
	RecreatedCount           int
	SelectedInputCount       int
	SelectedOutputCount      int
	UnchangedDescendantCount int
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
	groups, err := rewriteApplyGroups(opts)
	if err != nil {
		return RewriteApplyResult{}, err
	}
	opts.Groups = groups
	opts.Commits = nil
	if opts.BranchRef == "" || opts.ExpectedHead == "" || len(opts.Groups) == 0 {
		return RewriteApplyResult{}, fmt.Errorf("git rewrite apply: missing required plan fields")
	}
	if err := emitRewriteApplyProgress(opts, RewriteApplyProgress{Phase: "validate", Message: "checking repository"}); err != nil {
		return RewriteApplyResult{}, err
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
	if err := emitRewriteApplyProgress(opts, RewriteApplyProgress{Phase: "validate", Message: "validated plan"}); err != nil {
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
	if err := emitRewriteApplyProgress(opts, RewriteApplyProgress{Phase: "backup", Message: "created backup refs", BackupRef: backupBranch}); err != nil {
		return RewriteApplyResult{}, err
	}
	if opts.PlanID != "" {
		res.InternalBackupRef = "refs/acd/rewrite-backups/" + sanitizeRewriteRefPart(opts.PlanID)
	}

	parent, err := firstParent(ctx, repoDir, opts.Groups[0].OldOIDs[0])
	if err != nil {
		return RewriteApplyResult{}, err
	}
	newParent := parent
	for i, group := range opts.Groups {
		lastOID := group.OldOIDs[len(group.OldOIDs)-1]
		newOID, err := recreateCommitWithMessage(ctx, repoDir, lastOID, group.ProposedMessage, newParent)
		if err != nil {
			return res, err
		}
		for _, oldOID := range group.OldOIDs {
			res.CommitMap[oldOID] = newOID
			res.SelectedInputCount++
		}
		newParent = newOID
		res.RecreatedCount++
		res.SelectedOutputCount++
		commitLabel := "commits"
		if len(group.OldOIDs) == 1 {
			commitLabel = "commit"
		}
		if err := emitRewriteApplyProgress(opts, RewriteApplyProgress{
			Phase:   "recreate_selected",
			Message: fmt.Sprintf("grouped %d selected %s", len(group.OldOIDs), commitLabel),
			Current: i + 1,
			Total:   len(opts.Groups),
			OldOID:  group.OldOIDs[0],
			NewOID:  newOID,
		}); err != nil {
			return res, err
		}
	}

	lastSelectedOID := opts.Groups[len(opts.Groups)-1].OldOIDs[len(opts.Groups[len(opts.Groups)-1].OldOIDs)-1]
	newer, err := firstParentDescendantsReverse(ctx, repoDir, lastSelectedOID, head)
	if err != nil {
		return res, err
	}
	for i, oldOID := range newer {
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
		res.UnchangedDescendantCount++
		if err := emitRewriteApplyProgress(opts, RewriteApplyProgress{
			Phase:   "recreate_unchanged",
			Message: "recreated unchanged descendant",
			Current: i + 1,
			Total:   len(newer),
			OldOID:  oldOID,
			NewOID:  newOID,
		}); err != nil {
			return res, err
		}
	}
	res.NewHead = newParent
	originalTree, err := commitTreeOID(ctx, repoDir, head)
	if err != nil {
		return res, err
	}
	rewrittenTree, err := commitTreeOID(ctx, repoDir, res.NewHead)
	if err != nil {
		return res, err
	}
	if rewrittenTree != originalTree {
		return res, errors.New("git rewrite apply: rewritten HEAD tree differs from the original; branch ref was not changed")
	}
	if err := emitRewriteApplyProgress(opts, RewriteApplyProgress{Phase: "update_ref", Message: "updating branch ref", OldOID: head, NewOID: res.NewHead}); err != nil {
		return res, err
	}
	if err := UpdateRef(ctx, repoDir, opts.BranchRef, res.NewHead, head); err != nil {
		return res, fmt.Errorf("git rewrite apply: CAS update %s: %w", opts.BranchRef, err)
	}
	return res, nil
}

func emitRewriteApplyProgress(opts RewriteApplyOptions, event RewriteApplyProgress) error {
	if opts.Progress == nil {
		return nil
	}
	return opts.Progress(event)
}

func validateRewriteApplyChain(ctx context.Context, repoDir string, opts RewriteApplyOptions) error {
	if err := ValidateRewriteGroupSemantics(ctx, repoDir, opts.Groups); err != nil {
		return err
	}
	seen := map[string]struct{}{}
	var flattened []string
	for groupIndex, group := range opts.Groups {
		if strings.TrimSpace(group.ProposedMessage) == "" {
			return fmt.Errorf("git rewrite apply: group %d missing proposed message", groupIndex)
		}
		for _, oldOID := range group.OldOIDs {
			if _, ok := seen[oldOID]; ok {
				return fmt.Errorf("git rewrite apply: duplicate commit %s in plan", shortApplyOID(oldOID))
			}
			seen[oldOID] = struct{}{}
			if err := rejectMergeCommit(ctx, repoDir, oldOID); err != nil {
				return err
			}
			if len(flattened) > 0 {
				p, err := firstParent(ctx, repoDir, oldOID)
				if err != nil {
					return err
				}
				if p != flattened[len(flattened)-1] {
					return fmt.Errorf("git rewrite apply: plan commits are not contiguous at %s", shortApplyOID(oldOID))
				}
			}
			flattened = append(flattened, oldOID)
		}
	}
	lastSelected := flattened[len(flattened)-1]
	isAnc, err := IsAncestor(ctx, repoDir, lastSelected, opts.ExpectedHead)
	if err != nil {
		return fmt.Errorf("git rewrite apply: verify selected commit ancestry: %w", err)
	}
	if !isAnc {
		return fmt.Errorf("git rewrite apply: selected commits are not ancestors of expected HEAD")
	}
	newer, err := firstParentDescendantsReverse(ctx, repoDir, lastSelected, opts.ExpectedHead)
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

// ValidateRewriteGroupSemantics checks group-local rules that require Git
// metadata but do not mutate repository state.
func ValidateRewriteGroupSemantics(ctx context.Context, repoDir string, groups []RewriteApplyGroup) error {
	if len(groups) == 0 {
		return errors.New("git rewrite apply: plan has no groups")
	}
	for groupIndex, group := range groups {
		if len(group.OldOIDs) == 0 {
			return fmt.Errorf("git rewrite apply: group %d has no commits", groupIndex+1)
		}
		firstIdentity, err := commitAuthorIdentity(ctx, repoDir, group.OldOIDs[0])
		if err != nil {
			return err
		}
		for _, oid := range group.OldOIDs[1:] {
			identity, err := commitAuthorIdentity(ctx, repoDir, oid)
			if err != nil {
				return err
			}
			if identity != firstIdentity {
				return fmt.Errorf("git rewrite apply: group %d crosses an author boundary at %s", groupIndex+1, shortApplyOID(oid))
			}
		}
		if len(group.OldOIDs) > 1 {
			beforeOID, err := firstParent(ctx, repoDir, group.OldOIDs[0])
			if err != nil {
				return err
			}
			beforeTree, err := treeBeforeCommit(ctx, repoDir, beforeOID)
			if err != nil {
				return err
			}
			afterTree, err := commitTreeOID(ctx, repoDir, group.OldOIDs[len(group.OldOIDs)-1])
			if err != nil {
				return err
			}
			if beforeTree == afterTree {
				return fmt.Errorf("git rewrite apply: group %d has no net tree change; keep its change and revert as separate commits", groupIndex+1)
			}
		}
	}
	return nil
}

func rewriteApplyGroups(opts RewriteApplyOptions) ([]RewriteApplyGroup, error) {
	if len(opts.Groups) > 0 && len(opts.Commits) > 0 {
		return nil, errors.New("git rewrite apply: plan cannot include both groups and legacy commits")
	}
	if len(opts.Groups) > 0 {
		groups := make([]RewriteApplyGroup, len(opts.Groups))
		for i, group := range opts.Groups {
			groups[i] = group
			groups[i].OldOIDs = append([]string(nil), group.OldOIDs...)
		}
		return groups, nil
	}
	groups := make([]RewriteApplyGroup, 0, len(opts.Commits))
	for _, commit := range opts.Commits {
		groups = append(groups, RewriteApplyGroup{OldOIDs: []string{commit.OldOID}, ProposedMessage: commit.ProposedMessage})
	}
	return groups, nil
}

func commitAuthorIdentity(ctx context.Context, repoDir, oid string) (string, error) {
	out, err := Run(ctx, RunOpts{Dir: repoDir, Timeout: DefaultReadTimeout}, "show", "-s", "--format=%an%x00%ae", oid)
	if err != nil {
		return "", fmt.Errorf("git rewrite apply: read author identity for %s: %w", shortApplyOID(oid), err)
	}
	identity := strings.TrimRight(string(out), "\n")
	if !strings.Contains(identity, "\x00") {
		return "", fmt.Errorf("git rewrite apply: malformed author identity for %s", shortApplyOID(oid))
	}
	return identity, nil
}

func treeBeforeCommit(ctx context.Context, repoDir, parentOID string) (string, error) {
	if parentOID != "" {
		return commitTreeOID(ctx, repoDir, parentOID)
	}
	out, err := Run(ctx, RunOpts{Dir: repoDir, Timeout: DefaultReadTimeout}, "hash-object", "-t", "tree", "--stdin")
	if err != nil {
		return "", fmt.Errorf("git rewrite apply: resolve empty tree: %w", err)
	}
	return strings.TrimSpace(string(out)), nil
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
