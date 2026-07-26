package git

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"sort"
	"strings"
)

const (
	// MaxIntentRepairCommits is the hard safety cap for automatic repair.
	// Presets may select a smaller limit but may never exceed this value.
	MaxIntentRepairCommits = 5

	IntentRepairReasonDetached         = "detached_head"
	IntentRepairReasonBranchChanged    = "branch_changed"
	IntentRepairReasonHeadChanged      = "head_changed"
	IntentRepairReasonNonLinearChain   = "non_linear_chain"
	IntentRepairReasonMergeCommit      = "merge_commit"
	IntentRepairReasonAlternateRef     = "alternate_ref_contains_commit"
	IntentRepairReasonStagedOverlap    = "staged_path_overlap"
	IntentRepairReasonOwnershipMissing = "ownership_missing"
)

// IntentRepairOwnedCommit is one ACD-owned commit in the contiguous repair
// range. Commits are supplied oldest-to-newest and the final commit must be
// ExpectedHead. CandidateID is required so callers cannot accidentally repair
// commits that were not associated with durable intent state.
type IntentRepairOwnedCommit struct {
	OID         string
	CandidateID string
}

// IntentRepairEligibilityOptions describes the live Git conditions required
// before an automatic intent repair may be prepared or applied.
type IntentRepairEligibilityOptions struct {
	BranchRef    string
	ExpectedHead string
	Commits      []IntentRepairOwnedCommit
	Paths        []string
	MaxCommits   int
}

// IntentRepairEligibility is a read-only, evidence-bearing eligibility result.
// Repository-state refusals return Eligible=false with a stable Reason. Invalid
// caller input and Git inspection failures return an error instead.
type IntentRepairEligibility struct {
	Eligible       bool
	Reason         string
	CurrentBranch  string
	CurrentHead    string
	CommitOIDs     []string
	ContainingRefs []string
	StagedPaths    []string
}

// CheckIntentRepairEligibility proves that the requested ACD-owned range is a
// contiguous, non-merge first-parent suffix at HEAD, is not visible from any
// tag, remote-tracking ref, or other local branch, and has no staged overlap.
// It is read-only and intentionally permits unrelated worktree or index edits.
func CheckIntentRepairEligibility(
	ctx context.Context,
	repoDir string,
	opts IntentRepairEligibilityOptions,
) (IntentRepairEligibility, error) {
	var result IntentRepairEligibility
	if ctx == nil {
		ctx = context.Background()
	}
	if repoDir == "" || opts.BranchRef == "" || opts.ExpectedHead == "" || len(opts.Commits) == 0 {
		return result, errors.New("git intent repair eligibility: missing required fields")
	}
	if !isFullBranchRef(opts.BranchRef) {
		return result, fmt.Errorf("git intent repair eligibility: invalid branch ref %q", opts.BranchRef)
	}
	if _, err := Run(ctx, RunOpts{Dir: repoDir, Timeout: DefaultReadTimeout}, "check-ref-format", opts.BranchRef); err != nil {
		return result, fmt.Errorf("git intent repair eligibility: invalid branch ref %q: %w", opts.BranchRef, err)
	}
	limit := opts.MaxCommits
	if limit == 0 {
		limit = MaxIntentRepairCommits
	}
	if limit < 1 || limit > MaxIntentRepairCommits {
		return result, fmt.Errorf("git intent repair eligibility: max commits must be between 1 and %d", MaxIntentRepairCommits)
	}
	if len(opts.Commits) > limit {
		return result, fmt.Errorf("git intent repair eligibility: %d commits exceed limit %d", len(opts.Commits), limit)
	}

	result.CommitOIDs = make([]string, 0, len(opts.Commits))
	seen := make(map[string]struct{}, len(opts.Commits))
	for i, commit := range opts.Commits {
		if commit.OID == "" {
			return result, fmt.Errorf("git intent repair eligibility: commit %d has no oid", i)
		}
		if strings.TrimSpace(commit.CandidateID) == "" {
			result.Reason = IntentRepairReasonOwnershipMissing
			return result, nil
		}
		if _, ok := seen[commit.OID]; ok {
			return result, fmt.Errorf("git intent repair eligibility: duplicate commit %s", shortApplyOID(commit.OID))
		}
		seen[commit.OID] = struct{}{}
		resolved, err := RevParse(ctx, repoDir, commit.OID+"^{commit}")
		if err != nil {
			return result, fmt.Errorf("git intent repair eligibility: resolve commit %s: %w", shortApplyOID(commit.OID), err)
		}
		if resolved != commit.OID {
			return result, fmt.Errorf("git intent repair eligibility: commit %s resolved to %s", shortApplyOID(commit.OID), shortApplyOID(resolved))
		}
		result.CommitOIDs = append(result.CommitOIDs, commit.OID)
	}

	branchRef, err := RunBranchRef(ctx, repoDir)
	if err != nil {
		return result, fmt.Errorf("git intent repair eligibility: resolve HEAD branch: %w", err)
	}
	result.CurrentBranch = branchRef
	if branchRef == "" {
		result.Reason = IntentRepairReasonDetached
		return result, nil
	}
	if branchRef != opts.BranchRef {
		result.Reason = IntentRepairReasonBranchChanged
		return result, nil
	}
	head, err := RevParse(ctx, repoDir, "HEAD")
	if err != nil {
		return result, fmt.Errorf("git intent repair eligibility: resolve HEAD: %w", err)
	}
	result.CurrentHead = head
	if head != opts.ExpectedHead || opts.Commits[len(opts.Commits)-1].OID != opts.ExpectedHead {
		result.Reason = IntentRepairReasonHeadChanged
		return result, nil
	}

	for i, commit := range opts.Commits {
		parents, err := parentsOf(ctx, repoDir, commit.OID)
		if err != nil {
			return result, fmt.Errorf("git intent repair eligibility: inspect commit %s: %w", shortApplyOID(commit.OID), err)
		}
		if len(parents) > 1 {
			result.Reason = IntentRepairReasonMergeCommit
			return result, nil
		}
		if i > 0 && (len(parents) == 0 || parents[0] != opts.Commits[i-1].OID) {
			result.Reason = IntentRepairReasonNonLinearChain
			return result, nil
		}
	}

	containing, err := alternateRefsContaining(ctx, repoDir, opts.BranchRef, result.CommitOIDs)
	if err != nil {
		return result, err
	}
	if len(containing) > 0 {
		result.Reason = IntentRepairReasonAlternateRef
		result.ContainingRefs = containing
		return result, nil
	}

	staged, err := stagedRepairPathOverlap(ctx, repoDir, opts.Paths)
	if err != nil {
		return result, err
	}
	if len(staged) > 0 {
		result.Reason = IntentRepairReasonStagedOverlap
		result.StagedPaths = staged
		return result, nil
	}

	result.Eligible = true
	return result, nil
}

// IntentRepairReplacement describes one rebuilt commit. Replaces must be a
// non-empty, ordered partition of the eligible old chain. Combining adjacent
// commits is supported; splitting a single old commit into synthetic commits
// is intentionally unsupported by this Git primitive.
type IntentRepairReplacement struct {
	Replaces  []string
	TreeOID   string
	Message   string
	AuthorOID string
}

// IntentRepairApplyOptions carries an already-approved repair plan. The
// repository is revalidated before objects are created and again before the
// atomic backup/ref transaction.
type IntentRepairApplyOptions struct {
	Eligibility  IntentRepairEligibilityOptions
	RepairID     string
	Replacements []IntentRepairReplacement
	DryRun       bool
}

// IntentRepairCommitMapping records one old-to-new relation. Adjacent old
// commits may map to one rebuilt candidate commit.
type IntentRepairCommitMapping struct {
	OldOID string
	NewOID string
}

// IntentRepairApplyResult reports the inert dry-run plan or the refs and
// commit mapping created by a successful apply.
type IntentRepairApplyResult struct {
	Eligible       bool
	Reason         string
	OldHead        string
	NewHead        string
	BackupRef      string
	PlannedCommits int
	CommitMappings []IntentRepairCommitMapping
	DryRun         bool
}

// ApplyIntentRepair rebuilds approved candidate commits with commit-tree, then
// creates the private backup and CAS-updates the literal branch ref in one
// update-ref transaction. It never checks out files or touches the live index.
func ApplyIntentRepair(
	ctx context.Context,
	repoDir string,
	opts IntentRepairApplyOptions,
) (IntentRepairApplyResult, error) {
	var result IntentRepairApplyResult
	if ctx == nil {
		ctx = context.Background()
	}
	if strings.TrimSpace(opts.RepairID) == "" || len(opts.Replacements) == 0 {
		return result, errors.New("git intent repair apply: repair id and replacements are required")
	}
	backupRef, err := IntentRepairBackupRef(opts.Eligibility.BranchRef, opts.RepairID)
	if err != nil {
		return result, err
	}
	result.BackupRef = backupRef
	result.PlannedCommits = len(opts.Replacements)
	result.DryRun = opts.DryRun

	eligibility, err := CheckIntentRepairEligibility(ctx, repoDir, opts.Eligibility)
	if err != nil {
		return result, err
	}
	result.Eligible = eligibility.Eligible
	result.Reason = eligibility.Reason
	result.OldHead = eligibility.CurrentHead
	if !eligibility.Eligible {
		return result, nil
	}
	if err := validateIntentRepairReplacements(ctx, repoDir, opts.Eligibility.Commits, opts.Replacements); err != nil {
		return result, err
	}
	if opts.DryRun {
		return result, nil
	}

	parent, err := firstParent(ctx, repoDir, opts.Eligibility.Commits[0].OID)
	if err != nil {
		return result, fmt.Errorf("git intent repair apply: resolve base parent: %w", err)
	}
	newParent := parent
	for _, replacement := range opts.Replacements {
		authorOID := replacement.AuthorOID
		if authorOID == "" {
			authorOID = replacement.Replaces[0]
		}
		author, err := commitAuthorEnv(ctx, repoDir, authorOID)
		if err != nil {
			return result, fmt.Errorf("git intent repair apply: read author: %w", err)
		}
		parents := []string{}
		if newParent != "" {
			parents = append(parents, newParent)
		}
		newOID, err := commitTreeWithEnv(
			ctx,
			repoDir,
			replacement.TreeOID,
			strings.TrimRight(replacement.Message, "\n"),
			author,
			parents...,
		)
		if err != nil {
			return result, fmt.Errorf("git intent repair apply: rebuild candidate commit: %w", err)
		}
		for _, oldOID := range replacement.Replaces {
			result.CommitMappings = append(result.CommitMappings, IntentRepairCommitMapping{
				OldOID: oldOID,
				NewOID: newOID,
			})
		}
		newParent = newOID
	}
	result.NewHead = newParent

	eligibility, err = CheckIntentRepairEligibility(ctx, repoDir, opts.Eligibility)
	if err != nil {
		return IntentRepairApplyResult{}, err
	}
	if !eligibility.Eligible {
		result.Eligible = false
		result.Reason = eligibility.Reason
		result.NewHead = ""
		result.CommitMappings = nil
		return result, nil
	}
	if err := atomicIntentRepairRefSwap(
		ctx,
		repoDir,
		opts.Eligibility.BranchRef,
		backupRef,
		opts.Eligibility.ExpectedHead,
		newParent,
	); err != nil {
		return result, err
	}
	return result, nil
}

// IntentRepairBackupRef returns the private, deterministic namespace for a
// repair backup without exposing branch names in refs.
func IntentRepairBackupRef(branchRef, repairID string) (string, error) {
	if !isFullBranchRef(branchRef) {
		return "", fmt.Errorf("git intent repair backup: invalid branch ref %q", branchRef)
	}
	repairID = sanitizeRewriteRefPart(strings.TrimSpace(repairID))
	if repairID == "" || repairID == "unknown" {
		return "", errors.New("git intent repair backup: invalid repair id")
	}
	sum := sha256.Sum256([]byte(branchRef))
	ref := fmt.Sprintf("refs/acd/intent-repair/%x/%s/backup", sum[:8], repairID)
	if _, err := Run(context.Background(), RunOpts{Timeout: DefaultReadTimeout}, "check-ref-format", ref); err != nil {
		return "", fmt.Errorf("git intent repair backup: invalid generated ref: %w", err)
	}
	return ref, nil
}

func validateIntentRepairReplacements(
	ctx context.Context,
	repoDir string,
	commits []IntentRepairOwnedCommit,
	replacements []IntentRepairReplacement,
) error {
	var flattened []string
	for i, replacement := range replacements {
		if len(replacement.Replaces) == 0 || replacement.TreeOID == "" || strings.TrimSpace(replacement.Message) == "" {
			return fmt.Errorf("git intent repair apply: replacement %d is incomplete", i)
		}
		tree, err := RevParse(ctx, repoDir, replacement.TreeOID+"^{tree}")
		if err != nil {
			return fmt.Errorf("git intent repair apply: resolve replacement %d tree: %w", i, err)
		}
		if tree != replacement.TreeOID {
			return fmt.Errorf("git intent repair apply: replacement %d tree resolved unexpectedly", i)
		}
		if replacement.AuthorOID != "" {
			if _, err := RevParse(ctx, repoDir, replacement.AuthorOID+"^{commit}"); err != nil {
				return fmt.Errorf("git intent repair apply: resolve replacement %d author: %w", i, err)
			}
		}
		flattened = append(flattened, replacement.Replaces...)
	}
	if len(flattened) != len(commits) {
		return errors.New("git intent repair apply: replacements must partition the complete old chain")
	}
	for i, commit := range commits {
		if flattened[i] != commit.OID {
			return fmt.Errorf("git intent repair apply: replacements do not preserve old-chain order at position %d", i)
		}
	}
	return nil
}

func alternateRefsContaining(ctx context.Context, repoDir, branchRef string, commits []string) ([]string, error) {
	refs := make(map[string]struct{})
	for _, oid := range commits {
		out, err := RunWithLimit(
			ctx,
			RunOpts{Dir: repoDir, Timeout: DefaultReadTimeout},
			DefaultDiffCap,
			"for-each-ref",
			"--contains="+oid,
			"--format=%(refname)",
			"refs/heads/",
			"refs/remotes/",
			"refs/tags/",
		)
		if err != nil {
			return nil, fmt.Errorf("git intent repair eligibility: inspect containing refs for %s: %w", shortApplyOID(oid), err)
		}
		for _, line := range strings.Split(string(out), "\n") {
			ref := strings.TrimSpace(line)
			if ref != "" && ref != branchRef {
				refs[ref] = struct{}{}
			}
		}
	}
	result := make([]string, 0, len(refs))
	for ref := range refs {
		result = append(result, ref)
	}
	sort.Strings(result)
	return result, nil
}

func stagedRepairPathOverlap(ctx context.Context, repoDir string, paths []string) ([]string, error) {
	cleaned := make([]string, 0, len(paths))
	seen := make(map[string]struct{}, len(paths))
	for _, path := range paths {
		path = strings.TrimPrefix(strings.ReplaceAll(path, "\\", "/"), "./")
		if path == "" || strings.HasPrefix(path, "/") || path == ".." ||
			strings.HasPrefix(path, "../") || strings.Contains(path, "/../") ||
			strings.ContainsRune(path, '\x00') {
			return nil, fmt.Errorf("git intent repair eligibility: invalid repair path %q", path)
		}
		if _, ok := seen[path]; ok {
			continue
		}
		seen[path] = struct{}{}
		cleaned = append(cleaned, path)
	}
	if len(cleaned) == 0 {
		return nil, nil
	}
	args := []string{"--literal-pathspecs", "diff", "--cached", "--name-only", "-z", "--no-renames", "--"}
	args = append(args, cleaned...)
	out, err := RunWithLimit(ctx, RunOpts{Dir: repoDir, Timeout: DefaultReadTimeout}, DefaultDiffCap, args...)
	if err != nil {
		return nil, fmt.Errorf("git intent repair eligibility: inspect staged overlap: %w", err)
	}
	var overlaps []string
	for _, record := range bytes.Split(out, []byte{0}) {
		if len(record) > 0 {
			overlaps = append(overlaps, string(record))
		}
	}
	sort.Strings(overlaps)
	return overlaps, nil
}

func atomicIntentRepairRefSwap(
	ctx context.Context,
	repoDir string,
	branchRef string,
	backupRef string,
	oldHead string,
	newHead string,
) error {
	if !isFullBranchRef(branchRef) || !strings.HasPrefix(backupRef, "refs/acd/intent-repair/") {
		return errors.New("git intent repair apply: invalid ref transaction target")
	}
	var input strings.Builder
	input.WriteString("start\n")
	input.WriteString("create " + backupRef + " " + oldHead + "\n")
	input.WriteString("update " + branchRef + " " + newHead + " " + oldHead + "\n")
	input.WriteString("prepare\n")
	input.WriteString("commit\n")
	if _, err := Run(ctx, RunOpts{
		Dir:     repoDir,
		Stdin:   strings.NewReader(input.String()),
		Timeout: DefaultWriteTimeout,
	}, "update-ref", "--no-deref", "--stdin"); err != nil {
		return fmt.Errorf("git intent repair apply: atomic backup and branch CAS: %w", err)
	}
	return nil
}

func isFullBranchRef(ref string) bool {
	return strings.HasPrefix(ref, "refs/heads/") &&
		len(ref) > len("refs/heads/") &&
		!strings.ContainsAny(ref, " \t\r\n\x00")
}
