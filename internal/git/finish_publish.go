package git

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

const FinishRefPrefix = "refs/acd/finish/"
const finishCommitBatchSize = 256

var (
	ErrFinishUnsafe = errors.New("git: finish publication ownership or Git state changed")
	ErrFinishStaged = errors.New("git: staged changes require explicit --include-staged consent")
	ErrFinishLock   = errors.New("git: finish cannot prove ownership of the existing Git lock")
)

// FinishCommit is an already-materialized candidate. The caller supplies full
// tree identities and its approved verifier; this package performs no planning.
type FinishCommit struct {
	TreeOID, Message                    string
	AuthorName, AuthorEmail, AuthorDate string
}

type FinishBuildOptions struct {
	RequestID                                        string
	Attempt                                          int
	WorktreeID                                       string
	BranchRef, ExpectedHead, TargetTree, IndexDigest string
	IncludeStaged                                    bool
	ObjectRef                                        string
	Commits                                          []FinishCommit
	Verify                                           func(context.Context, int, string, string) error
}

type FinishPublicationProof = state.FinishPublicationProof
type FinishPublicationResult = state.FinishPublicationResult

func finishHead(ctx context.Context, repo, branch string) (string, error) {
	head, err := RevParse(ctx, repo, branch)
	if errors.Is(err, ErrRefNotFound) {
		return "", nil
	}
	return head, err
}

func finishAttachedBranch(ctx context.Context, repo string) (string, error) {
	out, err := Run(ctx, RunOpts{Dir: repo}, "symbolic-ref", "--quiet", "--no-recurse", "HEAD")
	if err != nil {
		var gitErr *Error
		if errors.As(err, &gitErr) && gitErr.ExitCode == 1 {
			return "", nil
		}
		return "", err
	}
	return strings.TrimSpace(string(out)), nil
}

func ensureDirectFinishRef(ctx context.Context, repo, ref string) error {
	_, err := Run(ctx, RunOpts{Dir: repo}, "symbolic-ref", "--quiet", "--no-recurse", ref)
	if err == nil {
		return fmt.Errorf("%w: symbolic publication ref", ErrFinishUnsafe)
	}
	var gitErr *Error
	if errors.As(err, &gitErr) && gitErr.ExitCode == 1 {
		return nil
	}
	return err
}

// Git conditionally locks HEAD for an attached branch transaction. Freeze the
// actual HEAD file identity across prepare so an away-and-back attachment
// cannot satisfy the branch check without that native lock ownership.
func finishHeadIdentity(ctx context.Context, repo, gitDir, branch string) (os.FileInfo, error) {
	path := filepath.Join(gitDir, "HEAD")
	before, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	attached, err := finishAttachedBranch(ctx, repo)
	if err != nil {
		return nil, err
	}
	if attached != branch || !before.Mode().IsRegular() {
		return nil, ErrFinishUnsafe
	}
	if err := verifyFinishHeadIdentity(gitDir, before); err != nil {
		return nil, err
	}
	return before, nil
}

func verifyFinishHeadIdentity(gitDir string, expected os.FileInfo) error {
	current, err := os.Lstat(filepath.Join(gitDir, "HEAD"))
	if err != nil {
		return err
	}
	if !os.SameFile(expected, current) || expected.Size() != current.Size() || !expected.ModTime().Equal(current.ModTime()) {
		return ErrFinishUnsafe
	}
	return nil
}

// CheckFinishPublicationSafety is read-only. Worktree edits do not invalidate
// a frozen request; branch, staging, conflicts and linked ownership do.
func CheckFinishPublicationSafety(ctx context.Context, repo, branch, head, indexDigest string, includeStaged bool) error {
	worktree, err := ResolveWorktree(ctx, repo)
	if err != nil {
		return err
	}
	if !isFullBranchRef(branch) || !isValidFullRef(branch) {
		return ErrFinishUnsafe
	}
	if err := ensureDirectFinishRef(ctx, repo, branch); err != nil {
		return err
	}
	if _, err := finishHeadIdentity(ctx, repo, worktree.GitDir, branch); err != nil {
		return err
	}
	currentHead, err := finishHead(ctx, repo, branch)
	if err != nil {
		return err
	}
	if currentHead != head {
		return ErrFinishUnsafe
	}
	if operation, active := gitOperationMarker(worktree.GitDir); active {
		return fmt.Errorf("%w: %s in progress", ErrFinishUnsafe, operation)
	}
	conflicts, err := Run(ctx, RunOpts{Dir: repo}, "ls-files", "--unmerged", "-z")
	if err != nil {
		return err
	}
	if len(conflicts) > 0 {
		return ErrFinishUnsafe
	}
	worktrees, err := Run(ctx, RunOpts{Dir: repo}, "worktree", "list", "--porcelain", "-z")
	if err != nil {
		return err
	}
	owners := 0
	for _, field := range strings.Split(string(worktrees), "\x00") {
		if field == "branch "+branch {
			owners++
		}
	}
	if owners != 1 {
		return fmt.Errorf("%w: branch is attached to %d worktrees", ErrFinishUnsafe, owners)
	}
	digest, err := IndexContentDigest(ctx, repo)
	if err != nil {
		return err
	}
	if digest != indexDigest {
		return ErrStagingChanged
	}
	if !includeStaged {
		staged, err := Run(ctx, RunOpts{Dir: repo}, "diff", "--cached", "--name-only", "--ita-visible-in-index", "-z")
		if err != nil {
			return err
		}
		if len(staged) > 0 {
			return ErrFinishStaged
		}
	}
	return nil
}

// BuildFinishChain prepares the entire verified chain privately. Failure
// creates no branch commit, index replacement, or partial private publication.
func BuildFinishChain(ctx context.Context, repo string, opts FinishBuildOptions) (FinishPublicationProof, error) {
	var proof FinishPublicationProof
	if opts.RequestID == "" || opts.Attempt < 1 || opts.WorktreeID == "" || len(opts.Commits) == 0 ||
		!isValidFullRef(opts.ObjectRef) || !strings.HasPrefix(opts.ObjectRef, FinishRefPrefix) {
		return proof, ErrFinishUnsafe
	}
	if err := CheckFinishPublicationSafety(ctx, repo, opts.BranchRef, opts.ExpectedHead, opts.IndexDigest, opts.IncludeStaged); err != nil {
		return proof, err
	}
	worktree, err := ResolveWorktree(ctx, repo)
	if err != nil {
		return proof, err
	}
	proof = FinishPublicationProof{Version: state.FinishPublicationVersion, RequestID: opts.RequestID, Attempt: opts.Attempt, WorktreeID: opts.WorktreeID, GitDir: worktree.GitDir, BranchRef: opts.BranchRef, ExpectedHead: opts.ExpectedHead, TargetTree: opts.TargetTree, IndexDigest: opts.IndexDigest, IncludeStaged: opts.IncludeStaged, ObjectRef: opts.ObjectRef}
	trees := make([]string, len(opts.Commits))
	for i, candidate := range opts.Commits {
		trees[i] = candidate.TreeOID
	}
	types, err := ObjectTypes(ctx, repo, trees)
	if err != nil {
		return proof, err
	}
	for _, tree := range trees {
		if types[tree] != "tree" {
			return proof, ErrFinishUnsafe
		}
	}
	parent := opts.ExpectedHead
	verified := 0
	for index, candidate := range opts.Commits {
		if strings.TrimSpace(candidate.Message) == "" || strings.ContainsRune(candidate.Message, 0) {
			return proof, errors.New("git: invalid finish commit message")
		}
		env := map[string]string{}
		if candidate.AuthorName != "" || candidate.AuthorEmail != "" {
			if candidate.AuthorName == "" || candidate.AuthorEmail == "" {
				return proof, errors.New("git: incomplete finish author identity")
			}
			env["GIT_AUTHOR_NAME"] = candidate.AuthorName
			env["GIT_AUTHOR_EMAIL"] = candidate.AuthorEmail
		}
		if candidate.AuthorDate != "" {
			env["GIT_AUTHOR_DATE"] = candidate.AuthorDate
		}
		args := []string{"commit-tree", candidate.TreeOID, "-F", "-"}
		if parent != "" {
			args = append(args, "-p", parent)
		}
		out, err := runDurableWithEnv(ctx, repo, strings.NewReader(candidate.Message), env, "loose-object", args...)
		if err != nil {
			return proof, err
		}
		oid := strings.TrimSpace(string(out))
		proof.Commits = append(proof.Commits, state.FinishPublicationCommit{OID: oid, ParentOID: parent, TreeOID: candidate.TreeOID})
		parent = oid
		if len(proof.Commits)-verified == finishCommitBatchSize || index == len(opts.Commits)-1 {
			if err := verifyFinishCommits(ctx, repo, proof.Commits[verified:]); err != nil {
				return proof, err
			}
			for i := verified; i < len(proof.Commits); i++ {
				commit := proof.Commits[i]
				if opts.Verify != nil {
					if err := opts.Verify(ctx, i, commit.OID, commit.TreeOID); err != nil {
						return proof, err
					}
				}
			}
			verified = len(proof.Commits)
		}
	}
	proof.NewHead = parent
	proof.ObjectOID = parent
	if proof.Commits[len(proof.Commits)-1].TreeOID != opts.TargetTree {
		return proof, ErrFinishUnsafe
	}
	// Freeze the replacement identity from the exact current index flags before
	// the journal is prepared. Recheck index/branch after private verification.
	replacement, cleanup, err := prepareFinishIndex(ctx, repo, parent)
	if err != nil {
		return proof, err
	}
	defer cleanup()
	proof.CleanIndexDigest, err = indexContentDigest(ctx, repo, replacement)
	if err != nil {
		return proof, err
	}
	if err := CheckFinishPublicationSafety(ctx, repo, opts.BranchRef, opts.ExpectedHead, opts.IndexDigest, opts.IncludeStaged); err != nil {
		return proof, err
	}
	token := make([]byte, 32)
	if _, err := rand.Read(token); err != nil {
		return proof, err
	}
	proof.LockToken = hex.EncodeToString(token)
	if err := state.ValidateFinishPublicationProof(proof); err != nil {
		return proof, err
	}
	if err := pinFinishChain(ctx, repo, proof.ObjectRef, proof.ObjectOID); err != nil {
		return proof, err
	}
	return proof, nil
}

func verifyFinishCommit(ctx context.Context, repo string, commit state.FinishPublicationCommit) error {
	tree, err := RevParse(ctx, repo, commit.OID+"^{tree}")
	if err != nil {
		return err
	}
	if tree != commit.TreeOID {
		return ErrFinishUnsafe
	}
	parents, err := parentsOf(ctx, repo, commit.OID)
	if err != nil {
		return err
	}
	if commit.ParentOID == "" {
		if len(parents) != 0 {
			return ErrFinishUnsafe
		}
	} else if len(parents) != 1 || parents[0] != commit.ParentOID {
		return ErrFinishUnsafe
	}
	return nil
}

// Recheck immutable commit trees and parents in bounded reads rather than
// starting two Git processes per commit each time a large proof is inspected.
func verifyFinishCommits(ctx context.Context, repo string, commits []state.FinishPublicationCommit) error {
	for start := 0; start < len(commits); start += finishCommitBatchSize {
		batch := commits[start:min(start+finishCommitBatchSize, len(commits))]
		oids := make([]string, len(batch))
		for i, commit := range batch {
			if !exactObjectID(commit.OID) {
				return ErrFinishUnsafe
			}
			oids[i] = commit.OID
		}
		out, err := RunWithLimit(ctx, RunOpts{Dir: repo, Stdin: strings.NewReader(strings.Join(oids, "\n") + "\n")},
			int64(len(batch)*256), "log", "--no-walk=unsorted", "--no-show-signature", "--no-notes", "--format=%H%x00%T%x00%P", "--stdin")
		if err != nil {
			return err
		}
		lines := strings.Split(strings.TrimSuffix(string(out), "\n"), "\n")
		if len(lines) != len(batch) {
			return ErrFinishUnsafe
		}
		for i, line := range lines {
			fields := strings.Split(line, "\x00")
			commit := batch[i]
			if len(fields) != 3 || fields[0] != commit.OID || fields[1] != commit.TreeOID || fields[2] != commit.ParentOID {
				return ErrFinishUnsafe
			}
		}
	}
	return nil
}

func pinFinishChain(ctx context.Context, repo, ref, oid string) error {
	if err := ensureDirectFinishRef(ctx, repo, ref); err != nil {
		return err
	}
	current, err := RevParse(ctx, repo, ref)
	if err == nil {
		if current == oid {
			return nil
		}
		return ErrFinishUnsafe
	}
	if !errors.Is(err, ErrRefNotFound) {
		return err
	}
	zero, err := repositoryZeroOID(ctx, repo)
	if err != nil {
		return err
	}
	_, err = runDurable(ctx, repo, nil, "reference", "update-ref", "--no-deref", ref, oid, zero)
	if err != nil {
		return err
	}
	current, err = RevParse(ctx, repo, ref)
	if err != nil {
		return err
	}
	if current != oid {
		return ErrFinishUnsafe
	}
	return nil
}

func prepareFinishIndex(ctx context.Context, repo, newHead string) (string, func(), error) {
	worktree, err := ResolveWorktree(ctx, repo)
	if err != nil {
		return "", nil, err
	}
	temporary, err := os.MkdirTemp(worktree.GitDir, "acd-finish-index-")
	if err != nil {
		return "", nil, err
	}
	cleanup := func() { _ = os.RemoveAll(temporary) }
	replacement := filepath.Join(temporary, "index")
	if err := ReadTree(ctx, repo, replacement, newHead); err != nil {
		cleanup()
		return "", nil, err
	}
	if err := preserveIndexFlags(ctx, repo, replacement); err != nil {
		cleanup()
		return "", nil, err
	}
	return replacement, cleanup, nil
}

// Finish lock markers stay separate from replacement index bytes. The random
// token is journaled before lock creation, so a restarted canonical worker can
// reclaim only its exact marker; unknown Git locks always remain untouched.
func acquireFinishLock(path, token string, resume bool) (func(), error) {
	marker := []byte("acd finish " + token + "\n")
	if resume {
		info, err := os.Lstat(path)
		if err == nil {
			if !info.Mode().IsRegular() || info.Size() != int64(len(marker)) {
				return nil, ErrFinishLock
			}
			body, err := os.ReadFile(path)
			if err != nil {
				return nil, err
			}
			if string(body) != string(marker) {
				return nil, ErrFinishLock
			}
			if err := os.Remove(path); err != nil {
				return nil, err
			}
		} else if !errors.Is(err, os.ErrNotExist) {
			return nil, err
		}
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return nil, fmt.Errorf("%w: %s: %w", ErrFinishLock, filepath.Base(path), err)
	}
	release := func() {
		_ = file.Close()
		body, err := os.ReadFile(path)
		if err == nil && string(body) == string(marker) {
			_ = os.Remove(path)
		}
	}
	if _, err := file.Write(marker); err != nil {
		release()
		return nil, err
	}
	if err := file.Sync(); err != nil {
		release()
		return nil, err
	}
	if err := file.Close(); err != nil {
		release()
		return nil, err
	}
	if err := syncFinishDirectory(filepath.Dir(path)); err != nil {
		release()
		return nil, err
	}
	return release, nil
}

func syncFinishDirectory(path string) error {
	dir, err := os.Open(path)
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}

// ApplyFinishPublication accepts only an already-journaled proof. Resume must
// be used exclusively by the canonical worker after it proves the previous
// owner has stopped. It can reclaim this proof's exact lock markers only.
// The completion callback records the journal result while the exact refs and
// index remain locked. Nil applies the Git transition without completing a journal.
func ApplyFinishPublication(ctx context.Context, repo string, proof FinishPublicationProof, resume bool, complete func(FinishPublicationResult) error) (FinishPublicationResult, error) {
	return applyFinishPublication(ctx, repo, proof, resume, complete, nil)
}

func applyFinishPublication(ctx context.Context, repo string, p FinishPublicationProof, resume bool, complete func(FinishPublicationResult) error, fault func(string) error) (FinishPublicationResult, error) {
	var result FinishPublicationResult
	if err := state.ValidateFinishPublicationProof(p); err != nil {
		return result, err
	}
	worktree, err := ResolveWorktree(ctx, repo)
	if err != nil {
		return result, err
	}
	if worktree.GitDir != p.GitDir {
		return result, ErrFinishUnsafe
	}
	// Preflight all evidence before even reclaiming a known lock.
	if err := verifyFinishProof(ctx, repo, p); err != nil {
		return result, err
	}
	head, err := finishHead(ctx, repo, p.BranchRef)
	if err != nil {
		return result, err
	}
	if head != p.ExpectedHead && head != p.NewHead {
		return result, ErrFinishUnsafe
	}
	if head == p.NewHead {
		result = publishedFinishResult(p)
	}
	preserveLocks := false
	indexRelease, err := acquireFinishLock(filepath.Join(worktree.GitDir, "index.lock"), p.LockToken, resume)
	if err != nil {
		return result, err
	}
	defer func() {
		if !preserveLocks {
			indexRelease()
		}
	}()
	headRelease, err := acquireFinishLock(filepath.Join(worktree.GitDir, "HEAD.lock"), p.LockToken, resume)
	if err != nil {
		return result, err
	}
	defer func() {
		if !preserveLocks && headRelease != nil {
			headRelease()
		}
	}()
	boundary := func(name string) error {
		if fault != nil {
			if err := fault(name); err != nil {
				preserveLocks = true
				return err
			}
		}
		return nil
	}
	if err := boundary("locked"); err != nil {
		return result, err
	}
	digest, err := IndexContentDigest(ctx, repo)
	if err != nil {
		return result, err
	}
	if !result.Published && digest != p.IndexDigest {
		return result, ErrStagingChanged
	}
	if result.Published && digest != p.IndexDigest && digest != p.CleanIndexDigest {
		return result, ErrStagingChanged
	}
	expectedHead := p.ExpectedHead
	if result.Published {
		expectedHead = p.NewHead
	}
	// Once published, the original staged selection naturally differs from HEAD;
	// it is still allowed only under its exact saved index identity.
	if err := CheckFinishPublicationSafety(ctx, repo, p.BranchRef, expectedHead, digest, p.IncludeStaged || result.Published); err != nil {
		return result, err
	}
	if err := boundary("before_cas"); err != nil {
		return result, err
	}
	if !result.Published {
		old := p.ExpectedHead
		if old == "" {
			old, err = repositoryZeroOID(ctx, repo)
			if err != nil {
				return result, err
			}
		}
		// Git takes HEAD.lock itself when updating its attached branch. Hand off
		// our marker to that prepared transaction, then reacquire it before index
		// reconciliation. Both sides recheck the attached branch under ownership.
		headIdentity, err := finishHeadIdentity(ctx, repo, worktree.GitDir, p.BranchRef)
		if err != nil {
			return result, err
		}
		headRelease()
		headRelease = nil
		err = withLockedExpectedRefTimeout(ctx, repo, p.BranchRef, old, p.ObjectRef, p.ObjectOID, false, DefaultWriteTimeout, func(lockCtx context.Context) error {
			if err := verifyFinishHeadIdentity(worktree.GitDir, headIdentity); err != nil {
				return err
			}
			if err := CheckFinishPublicationSafety(lockCtx, repo, p.BranchRef, p.ExpectedHead, p.IndexDigest, p.IncludeStaged); err != nil {
				return err
			}
			return ensureDirectFinishRef(lockCtx, repo, p.ObjectRef)
		}, p.NewHead)
		if err != nil {
			probeCtx, cancel := context.WithTimeout(context.Background(), DefaultReadTimeout)
			defer cancel()
			observed, probeErr := finishHead(probeCtx, repo, p.BranchRef)
			if probeErr == nil && observed == p.NewHead && ensureDirectFinishRef(probeCtx, repo, p.BranchRef) == nil {
				result = publishedFinishResult(p)
			}
			return result, err
		}
		result = publishedFinishResult(p)
		headRelease, err = acquireFinishLock(filepath.Join(worktree.GitDir, "HEAD.lock"), p.LockToken, false)
		if err != nil {
			return result, err
		}
	}
	if err := boundary("after_cas"); err != nil {
		return result, err
	}
	// Hold both exact refs while replacing the index. Another Git writer cannot
	// change HEAD or staging under these locks. Git's prepared transaction also
	// takes HEAD.lock, so hand the marker back before entering it.
	headIdentity, err := finishHeadIdentity(ctx, repo, worktree.GitDir, p.BranchRef)
	if err != nil {
		return result, err
	}
	headRelease()
	headRelease = nil
	err = withLockedExpectedRef(ctx, repo, p.BranchRef, p.NewHead, p.ObjectRef, p.ObjectOID, false, func(lockCtx context.Context) error {
		if err := verifyFinishHeadIdentity(worktree.GitDir, headIdentity); err != nil {
			return err
		}
		if err := ensureDirectFinishRef(lockCtx, repo, p.ObjectRef); err != nil {
			return err
		}
		current, err := IndexContentDigest(lockCtx, repo)
		if err != nil {
			return err
		}
		if err := CheckFinishPublicationSafety(lockCtx, repo, p.BranchRef, p.NewHead, current, true); err != nil {
			return err
		}
		if current == p.CleanIndexDigest {
			result.IndexReconciled = true
			result.StagedConsumed = p.IncludeStaged
			if complete != nil {
				return complete(result)
			}
			return nil
		}
		if current != p.IndexDigest {
			return ErrStagingChanged
		}
		replacement, cleanup, err := prepareFinishIndex(lockCtx, repo, p.NewHead)
		if err != nil {
			return err
		}
		defer cleanup()
		replacementDigest, err := indexContentDigest(lockCtx, repo, replacement)
		if err != nil {
			return err
		}
		if replacementDigest != p.CleanIndexDigest {
			return ErrStagingChanged
		}
		file, err := os.OpenFile(replacement, os.O_RDWR, 0600)
		if err != nil {
			return err
		}
		err = file.Sync()
		closeErr := file.Close()
		if err != nil {
			return err
		}
		if closeErr != nil {
			return closeErr
		}
		if err := boundary("before_index"); err != nil {
			return err
		}
		if err := os.Rename(replacement, filepath.Join(worktree.GitDir, "index")); err != nil {
			return err
		}
		if err := syncFinishDirectory(worktree.GitDir); err != nil {
			return err
		}
		result.IndexReconciled = true
		result.StagedConsumed = p.IncludeStaged
		if err := boundary("after_index"); err != nil {
			return err
		}
		if complete != nil {
			return complete(result)
		}
		return nil
	})
	return result, err
}

func publishedFinishResult(p FinishPublicationProof) FinishPublicationResult {
	return FinishPublicationResult{Published: true, NewHead: p.NewHead, CommitsCreated: len(p.Commits)}
}

func verifyFinishProof(ctx context.Context, repo string, p FinishPublicationProof) error {
	if err := ensureDirectFinishRef(ctx, repo, p.BranchRef); err != nil {
		return err
	}
	if err := ensureDirectFinishRef(ctx, repo, p.ObjectRef); err != nil {
		return err
	}
	pin, err := RevParse(ctx, repo, p.ObjectRef)
	if err != nil {
		return err
	}
	if pin != p.ObjectOID {
		return ErrFinishUnsafe
	}
	return verifyFinishCommits(ctx, repo, p.Commits)
}

// WithPreservedFinishPublication proves the saved chain remains pinned while
// explicit recovery closes its request. It makes no publication claim and
// neither locks nor changes the current branch, HEAD, or index.
func WithPreservedFinishPublication(ctx context.Context, repo string, p FinishPublicationProof, fn func(context.Context) error) error {
	if err := state.ValidateFinishPublicationProof(p); err != nil {
		return err
	}
	// The original branch may have moved or disappeared. Only private chain
	// evidence is needed to preserve the request, never to publish it.
	if err := verifyFinishCommits(ctx, repo, p.Commits); err != nil {
		return err
	}
	return WithLockedPrivateRef(ctx, repo, p.ObjectRef, p.ObjectOID, fn)
}
