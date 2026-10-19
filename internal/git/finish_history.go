package git

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

type FinishHistoryGoal struct {
	TreeOID, Message, AuthorOID string
	OldOIDs                     []string
}

type FinishHistoryBuildOptions struct {
	PlanID, WorktreeID               string
	SourceBranchRef, TargetBranchRef string
	ExpectedHead, IndexDigest        string
	SourceRef, LockToken             string
	SourceChain, RecreateUnchanged   []string
	Goals                            []FinishHistoryGoal
	Verify                           func(context.Context, string, string, int) error
}

// BuildFinishHistoryChain writes only a retained private chain. Every output
// copies an original author and uses a previously proved complete tree.
func BuildFinishHistoryChain(ctx context.Context, repo string, options FinishHistoryBuildOptions) (state.FinishHistoryProof, error) {
	var p state.FinishHistoryProof
	if options.PlanID == "" || options.WorktreeID == "" || len(options.SourceChain) == 0 || len(options.SourceChain)+len(options.RecreateUnchanged) > MaxIntentHistoryCommits || options.LockToken == "" {
		return p, ErrFinishUnsafe
	}
	if err := CheckFinishPublicationSafety(ctx, repo, options.SourceBranchRef, options.ExpectedHead, options.IndexDigest, false); err != nil {
		return p, err
	}
	if !isFullBranchRef(options.TargetBranchRef) {
		return p, ErrFinishUnsafe
	}
	if _, err := Run(ctx, RunOpts{Dir: repo}, "check-ref-format", options.TargetBranchRef); err != nil {
		return p, err
	}
	chain := append(append([]string{}, options.SourceChain...), options.RecreateUnchanged...)
	if chain[len(chain)-1] != options.ExpectedHead {
		return p, ErrFinishUnsafe
	}
	if _, err := ReadIntentHistoryUnits(ctx, repo, chain); err != nil {
		return p, err
	}
	base, err := firstParent(ctx, repo, chain[0])
	if err != nil {
		return p, err
	}
	p = state.FinishHistoryProof{Version: 1, PlanID: options.PlanID, WorktreeID: options.WorktreeID, SourceBranchRef: options.SourceBranchRef, TargetBranchRef: options.TargetBranchRef, ExpectedHead: options.ExpectedHead, BaseParent: base, IndexDigest: options.IndexDigest, SourceRef: options.SourceRef, ObjectRef: FinishRefPrefix + options.WorktreeID + "/history/" + options.PlanID + "/" + options.LockToken + "/chain", BackupRef: "refs/acd/rewrite-backups/" + options.PlanID, LockToken: options.LockToken}
	if _, err := EnsurePrivateRefDurable(ctx, repo, FinishRefPrefix, p.SourceRef, p.ExpectedHead); err != nil {
		return p, err
	}
	goals := append([]FinishHistoryGoal{}, options.Goals...)
	for _, oid := range options.RecreateUnchanged {
		tree, err := RevParse(ctx, repo, oid+"^{tree}")
		if err != nil {
			return p, err
		}
		message, err := commitMessage(ctx, repo, oid)
		if err != nil {
			return p, err
		}
		goals = append(goals, FinishHistoryGoal{TreeOID: tree, Message: message, AuthorOID: oid, OldOIDs: []string{oid}})
	}
	parent := base
	for index, goal := range goals {
		if goal.AuthorOID == "" || strings.TrimSpace(goal.Message) == "" {
			return p, ErrFinishUnsafe
		}
		if err := verifyObjectType(ctx, repo, goal.TreeOID, "tree"); err != nil {
			return p, err
		}
		author, err := commitAuthorEnv(ctx, repo, goal.AuthorOID)
		if err != nil {
			return p, err
		}
		args := []string{"commit-tree", goal.TreeOID, "-F", "-"}
		if parent != "" {
			args = append(args, "-p", parent)
		}
		out, err := runDurableWithEnv(ctx, repo, strings.NewReader(goal.Message), author, "loose-object", args...)
		if err != nil {
			return p, err
		}
		oid := strings.TrimSpace(string(out))
		p.Commits = append(p.Commits, state.FinishHistoryCommit{OID: oid, ParentOID: parent, TreeOID: goal.TreeOID, Message: goal.Message, AuthorOID: goal.AuthorOID})
		if options.Verify != nil {
			if err := options.Verify(ctx, oid, goal.TreeOID, index); err != nil {
				return p, err
			}
		}
		for _, old := range goal.OldOIDs {
			p.Mappings = append(p.Mappings, state.FinishHistoryMapping{OldOID: old, NewOID: oid})
		}
		parent = oid
	}
	if parent == "" {
		return p, errors.New("history rewrite: a neutral root selection has no commit target; preserve its membership with --messages-only")
	}
	for _, old := range chain {
		found := false
		for _, mapping := range p.Mappings {
			if mapping.OldOID == old {
				found = true
				break
			}
		}
		if !found {
			p.Mappings = append(p.Mappings, state.FinishHistoryMapping{OldOID: old})
		}
	}
	p.NewHead = parent
	if _, err := EnsurePrivateRefDurable(ctx, repo, FinishRefPrefix, p.ObjectRef, p.NewHead); err != nil {
		return p, err
	}
	if err := ValidateFinishHistoryProof(ctx, repo, p); err != nil {
		return p, err
	}
	return p, nil
}

func ValidateFinishHistoryProof(ctx context.Context, repo string, p state.FinishHistoryProof) error {
	if p.Version != 1 || p.PlanID == "" || p.NewHead == "" || p.LockToken == "" || !isFullBranchRef(p.SourceBranchRef) || !isFullBranchRef(p.TargetBranchRef) || !strings.HasPrefix(p.SourceRef, FinishRefPrefix) || !strings.HasPrefix(p.ObjectRef, FinishRefPrefix) || p.BackupRef != "refs/acd/rewrite-backups/"+p.PlanID {
		return ErrFinishUnsafe
	}
	for ref, expected := range map[string]string{p.SourceRef: p.ExpectedHead, p.ObjectRef: p.NewHead} {
		oid, err := RevParse(ctx, repo, ref)
		if err != nil || oid != expected {
			return errors.Join(err, ErrFinishUnsafe)
		}
		if err := ensureDirectFinishRef(ctx, repo, ref); err != nil {
			return err
		}
	}
	parent := p.BaseParent
	for _, commit := range p.Commits {
		if commit.ParentOID != parent {
			return ErrFinishUnsafe
		}
		if err := verifyFinishCommit(ctx, repo, state.FinishPublicationCommit{OID: commit.OID, ParentOID: parent, TreeOID: commit.TreeOID}); err != nil {
			return err
		}
		message, err := commitMessage(ctx, repo, commit.OID)
		if err != nil || strings.TrimRight(message, "\n") != strings.TrimRight(commit.Message, "\n") {
			return errors.Join(err, ErrFinishUnsafe)
		}
		expected, err := commitAuthorEnv(ctx, repo, commit.AuthorOID)
		if err != nil {
			return err
		}
		actual, err := commitAuthorEnv(ctx, repo, commit.OID)
		if err != nil {
			return err
		}
		for key, value := range expected {
			if actual[key] != value {
				return errors.New("history rewrite: copied author differs from source")
			}
		}
		parent = commit.OID
	}
	if parent != p.NewHead {
		return ErrFinishUnsafe
	}
	before, err := RevParse(ctx, repo, p.ExpectedHead+"^{tree}")
	if err != nil {
		return err
	}
	after, err := RevParse(ctx, repo, p.NewHead+"^{tree}")
	if err != nil || before != after {
		return errors.Join(err, ErrFinishUnsafe)
	}
	return nil
}

// ApplyFinishHistoryProof never rewrites files or index bytes. The durable
// proof precedes one source/backup/target transaction; completion is recorded
// while the proven output ref remains locked. Re-entry checks exact objects.
func ApplyFinishHistoryProof(ctx context.Context, repo string, p state.FinishHistoryProof, resume bool, complete func() error) error {
	if err := ValidateFinishHistoryProof(ctx, repo, p); err != nil {
		return err
	}
	wt, err := ResolveWorktree(ctx, repo)
	if err != nil {
		return err
	}
	identity := sha256.Sum256([]byte(wt.Root))
	if hex.EncodeToString(identity[:8]) != p.WorktreeID {
		return ErrFinishUnsafe
	}
	release, err := acquireFinishLock(filepath.Join(wt.GitDir, "index.lock"), p.LockToken, resume)
	if err != nil {
		return err
	}
	defer release()
	headRelease, err := acquireFinishLock(filepath.Join(wt.GitDir, "HEAD.lock"), p.LockToken, resume)
	if err != nil {
		return err
	}
	defer func() {
		if headRelease != nil {
			headRelease()
		}
	}()
	tip, tipErr := RevParse(ctx, repo, p.TargetBranchRef)
	backup, backupErr := RevParse(ctx, repo, p.BackupRef)
	post := tipErr == nil && tip == p.NewHead && backupErr == nil && backup == p.ExpectedHead
	if !post {
		if p.TargetBranchRef != p.SourceBranchRef && !errors.Is(tipErr, ErrRefNotFound) {
			return ErrFinishUnsafe
		}
		if err := CheckFinishPublicationSafety(ctx, repo, p.SourceBranchRef, p.ExpectedHead, p.IndexDigest, false); err != nil {
			return err
		}
		headIdentity, err := finishHeadIdentity(ctx, repo, wt.GitDir, p.SourceBranchRef)
		if err != nil {
			return err
		}
		creates := map[string]string{p.BackupRef: p.ExpectedHead}
		var replacement []string
		if p.TargetBranchRef == p.SourceBranchRef {
			replacement = []string{p.NewHead}
		} else {
			creates[p.TargetBranchRef] = p.NewHead
		}
		headRelease()
		headRelease = nil
		err = withLockedExpectedRefsTransaction(ctx, repo, p.SourceBranchRef, p.ExpectedHead, p.SourceRef, p.ExpectedHead, false, DefaultWriteTimeout, func(lockCtx context.Context) error {
			if err := verifyFinishHeadIdentity(wt.GitDir, headIdentity); err != nil {
				return err
			}
			if err := CheckFinishPublicationSafety(lockCtx, repo, p.SourceBranchRef, p.ExpectedHead, p.IndexDigest, false); err != nil {
				return err
			}
			return ensureDirectFinishRef(lockCtx, repo, p.SourceRef)
		}, creates, nil, replacement...)
		if err != nil {
			return fmt.Errorf("history rewrite: atomic refs: %w", err)
		}
		if headRelease == nil {
			headRelease, err = acquireFinishLock(filepath.Join(wt.GitDir, "HEAD.lock"), p.LockToken, false)
			if err != nil {
				return err
			}
		}
	}
	expectedSource := p.ExpectedHead
	if p.SourceBranchRef == p.TargetBranchRef {
		expectedSource = p.NewHead
	}
	if err := CheckFinishPublicationSafety(ctx, repo, p.SourceBranchRef, expectedSource, p.IndexDigest, false); err != nil {
		return err
	}
	completionHead, err := finishHeadIdentity(ctx, repo, wt.GitDir, p.SourceBranchRef)
	if err != nil {
		return err
	}
	if p.TargetBranchRef == p.SourceBranchRef {
		headRelease()
		headRelease = nil
	}
	return withLockedExpectedRef(ctx, repo, p.TargetBranchRef, p.NewHead, p.ObjectRef, p.NewHead, false, func(lockCtx context.Context) error {
		if err := verifyFinishHeadIdentity(wt.GitDir, completionHead); err != nil {
			return err
		}
		if err := CheckFinishPublicationSafety(lockCtx, repo, p.SourceBranchRef, expectedSource, p.IndexDigest, false); err != nil {
			return err
		}
		backup, err := RevParse(lockCtx, repo, p.BackupRef)
		if err != nil || backup != p.ExpectedHead {
			return errors.Join(err, ErrFinishUnsafe)
		}
		if complete != nil {
			return complete()
		}
		return nil
	})
}

// CheckFinishHistoryStart requires a clean starting worktree and index. New edits
// after the request is frozen remain protected and are never overwritten.
func CheckFinishHistoryStart(ctx context.Context, repo, branch, head, index string) error {
	if err := ensureRewriteSafeRepo(ctx, repo); err != nil {
		return err
	}
	return CheckFinishPublicationSafety(ctx, repo, branch, head, index, false)
}

func VerifyPublishedFinishHistoryProof(ctx context.Context, repo string, p state.FinishHistoryProof) error {
	if err := ValidateFinishHistoryProof(ctx, repo, p); err != nil {
		return err
	}
	for ref, expected := range map[string]string{p.TargetBranchRef: p.NewHead, p.BackupRef: p.ExpectedHead} {
		oid, err := RevParse(ctx, repo, ref)
		if err != nil || oid != expected {
			return errors.Join(err, ErrFinishUnsafe)
		}
		if err := ensureDirectFinishRef(ctx, repo, ref); err != nil {
			return err
		}
	}
	return nil
}

// PinFinishHistoryTree retains a generated complete tree through a private
// rootless evidence commit; the commit is never part of rewritten branch history.
func PinFinishHistoryTree(ctx context.Context, repo, ref, tree string) error {
	if oid, err := RevParse(ctx, repo, ref); err == nil {
		actual, err := RevParse(ctx, repo, oid+"^{tree}")
		if err != nil || actual != tree {
			return errors.Join(err, ErrFinishUnsafe)
		}
		return ensureDirectFinishRef(ctx, repo, ref)
	} else if !errors.Is(err, ErrRefNotFound) {
		return err
	}
	oid, err := CommitTreeDurable(ctx, repo, tree, "ACD history goal evidence", "ACD History", "acd-history@localhost")
	if err != nil {
		return err
	}
	_, err = EnsurePrivateRefDurable(ctx, repo, FinishRefPrefix, ref, oid)
	return err
}
