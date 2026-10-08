package git

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// Explicit reconstruction may examine a whole branch. Automatic repair keeps
// the much smaller MaxIntentRepairCommits private-suffix limit.
const MaxIntentHistoryCommits = 256
const MaxIntentHistoryUnits = 16384

// IntentHistoryUnit is one recorded path transition, rather than one old
// commit. A mixed commit can therefore contribute units to several goals.
// Empty Before/After entries mean absence. No intermediate text is invented.
type IntentHistoryUnit struct {
	OldOID string
	Path   string
	Before TreeEntry
	After  TreeEntry
}

type IntentHistoryRenamePair struct {
	OldOID     string
	BeforePath string
	AfterPath  string
}

// ReadIntentHistoryRenamePairs retains Git's recorded similarity evidence for
// delete/create units, including renames with edits. Both sides must remain in
// one goal; treating them as independent changes can create invalid history.
func ReadIntentHistoryRenamePairs(ctx context.Context, repoDir string, chain []string) ([]IntentHistoryRenamePair, error) {
	if len(chain) == 0 || len(chain) > MaxIntentHistoryCommits {
		return nil, errors.New("git intent history: invalid rename evidence range")
	}
	var pairs []IntentHistoryRenamePair
	for _, oid := range chain {
		out, err := RunWithLimit(ctx, RunOpts{Dir: repoDir, Timeout: DefaultReadTimeout}, DefaultDiffCap, "diff-tree", "--root", "--no-commit-id", "--raw", "--no-abbrev", "--find-renames=50%", "-r", "-z", oid, "--")
		if err != nil {
			return nil, err
		}
		records := bytes.Split(out, []byte{0})
		for i := 0; i < len(records) && len(records[i]) != 0; {
			fields := strings.Fields(strings.TrimPrefix(string(records[i]), ":"))
			if len(fields) != 5 || i+1 >= len(records) {
				return nil, errors.New("git intent history: malformed rename evidence")
			}
			if strings.HasPrefix(fields[4], "R") {
				if i+2 >= len(records) {
					return nil, errors.New("git intent history: missing rename target")
				}
				pairs = append(pairs, IntentHistoryRenamePair{OldOID: oid, BeforePath: string(records[i+1]), AfterPath: string(records[i+2])})
				i += 3
			} else {
				i += 2
			}
			if len(pairs) > MaxIntentHistoryUnits {
				return nil, errors.New("git intent history: rename evidence limit exceeded")
			}
		}
	}
	return pairs, nil
}

// IntentHistoryBaseTree supports an initial commit without assuming SHA-1.
// Materialization already creates inert Git objects; this writes only the
// canonical empty tree when the selected range begins at the root commit.
func IntentHistoryBaseTree(ctx context.Context, repoDir, oldestOID string) (string, error) {
	parents, err := parentsOf(ctx, repoDir, oldestOID)
	if err != nil {
		return "", err
	}
	if len(parents) > 1 {
		return "", errors.New("git intent history: merge has no single reconstruction base")
	}
	if len(parents) == 1 {
		return RevParse(ctx, repoDir, parents[0]+"^{tree}")
	}
	out, err := Run(ctx, RunOpts{Dir: repoDir, Timeout: DefaultWriteTimeout}, "hash-object", "-w", "-t", "tree", "--stdin")
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(out)), nil
}

// ReadIntentHistoryUnits extracts exact before/after versions from a linear
// oldest-to-newest chain. Renames are represented by deletion and creation;
// planners must keep their dependencies together. Reads and output are bounded.
func ReadIntentHistoryUnits(ctx context.Context, repoDir string, chain []string) ([]IntentHistoryUnit, error) {
	if len(chain) == 0 || len(chain) > MaxIntentHistoryCommits {
		return nil, fmt.Errorf("git intent history: source chain requires 1..%d commits", MaxIntentHistoryCommits)
	}
	var units []IntentHistoryUnit
	for i, oid := range chain {
		parents, err := parentsOf(ctx, repoDir, oid)
		if err != nil {
			return nil, err
		}
		if len(parents) > 1 || (i > 0 && (len(parents) == 0 || parents[0] != chain[i-1])) {
			return nil, errors.New("git intent history: source chain must be linear and contiguous")
		}
		out, err := RunWithLimit(ctx, RunOpts{Dir: repoDir, Timeout: DefaultReadTimeout}, DefaultDiffCap,
			"diff-tree", "--root", "--no-commit-id", "--raw", "--no-abbrev", "--no-renames", "-r", "-z", oid, "--")
		if err != nil {
			return nil, err
		}
		records := bytes.Split(out, []byte{0})
		for j := 0; j < len(records) && len(records[j]) != 0; j += 2 {
			if j+1 >= len(records) || len(records[j+1]) == 0 {
				return nil, errors.New("git intent history: malformed path transition")
			}
			fields := strings.Fields(strings.TrimPrefix(string(records[j]), ":"))
			if len(fields) != 5 {
				return nil, errors.New("git intent history: malformed transition metadata")
			}
			path := string(records[j+1])
			if err := validateIntentHistoryPath(path); err != nil {
				return nil, err
			}
			unit := IntentHistoryUnit{OldOID: oid, Path: path}
			if fields[0] != "000000" {
				unit.Before = intentHistoryEntry(path, fields[0], fields[2])
			}
			if fields[1] != "000000" {
				unit.After = intentHistoryEntry(path, fields[1], fields[3])
			}
			units = append(units, unit)
			if len(units) > MaxIntentHistoryUnits {
				return nil, errors.New("git intent history: path transition limit exceeded")
			}
		}
	}
	return units, nil
}

func intentHistoryEntry(path, mode, oid string) TreeEntry {
	typeName := "blob"
	if mode == "160000" {
		typeName = "commit"
	}
	return TreeEntry{Path: path, Mode: mode, OID: oid, Type: typeName}
}

func validateIntentHistoryPath(path string) error {
	if path == "" || path == "." || path == ".." || strings.HasPrefix(path, "/") ||
		strings.HasPrefix(path, "../") || strings.Contains(path, "/../") ||
		strings.ContainsRune(path, '\x00') || path == ".git" || strings.HasPrefix(path, ".git/") {
		return fmt.Errorf("git intent history: invalid path %q", path)
	}
	return nil
}

// MaterializeIntentHistoryUnits applies recorded transitions to an isolated
// index, proving the before-version at every step. Every original unit must
// appear exactly once across groups. A planner can move independent paths,
// but cannot reorder dependent versions or inject a fabricated file version.
func MaterializeIntentHistoryUnits(ctx context.Context, repoDir, baseTree string, original []IntentHistoryUnit, groups [][]IntentHistoryUnit) ([]string, error) {
	if baseTree == "" || len(original) == 0 || len(original) > MaxIntentHistoryUnits || len(groups) == 0 || len(groups) > MaxIntentHistoryCommits {
		return nil, errors.New("git intent history: invalid materialization input")
	}
	available := make(map[IntentHistoryUnit]bool, len(original))
	var oldChain []string
	oldSeen := make(map[string]struct{})
	for _, unit := range original {
		if _, exists := available[unit]; exists {
			return nil, errors.New("git intent history: duplicate original transition")
		}
		if err := validateIntentHistoryPath(unit.Path); err != nil {
			return nil, err
		}
		available[unit] = false
		if _, seen := oldSeen[unit.OldOID]; !seen {
			oldSeen[unit.OldOID] = struct{}{}
			oldChain = append(oldChain, unit.OldOID)
		}
	}
	owners := make(map[string]int, len(original))
	for groupIndex, group := range groups {
		if len(group) == 0 {
			return nil, errors.New("git intent history: empty goal")
		}
		for _, unit := range group {
			used, exists := available[unit]
			if !exists || used {
				return nil, errors.New("git intent history: fabricated or multiply owned transition")
			}
			available[unit] = true
			owners[unit.OldOID+"\x00"+unit.Path] = groupIndex
		}
	}
	for _, used := range available {
		if !used {
			return nil, errors.New("git intent history: unowned original transition")
		}
	}
	pairs, err := ReadIntentHistoryRenamePairs(ctx, repoDir, oldChain)
	if err != nil {
		return nil, err
	}
	for _, pair := range pairs {
		before, hasBefore := owners[pair.OldOID+"\x00"+pair.BeforePath]
		after, hasAfter := owners[pair.OldOID+"\x00"+pair.AfterPath]
		if hasBefore != hasAfter || hasBefore && before != after {
			return nil, fmt.Errorf("git intent history: rename %q to %q must remain in one goal", pair.BeforePath, pair.AfterPath)
		}
	}
	dir, err := os.MkdirTemp("", "acd-intent-history-index-")
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(dir)
	index := filepath.Join(dir, "index")
	if err := ReadTree(ctx, repoDir, index, baseTree); err != nil {
		return nil, err
	}
	var trees []string
	previousTree := baseTree
	for _, group := range groups {
		for _, unit := range group {
			entries, err := LsFilesIndex(ctx, repoDir, index, ":(literal)"+unit.Path)
			if err != nil {
				return nil, err
			}
			beforeMatches := len(entries) == 0 && unit.Before.OID == ""
			if len(entries) == 1 {
				beforeMatches = entries[0].Path == unit.Path && entries[0].Stage == 0 && entries[0].Mode == unit.Before.Mode && entries[0].OID == unit.Before.OID
			}
			if !beforeMatches {
				return nil, fmt.Errorf("git intent history: prerequisite version is missing for %q from %s", unit.Path, shortApplyOID(unit.OldOID))
			}
			line := "0 " + strings.Repeat("0", len(unit.OldOID)) + "\t" + unit.Path
			if unit.After.OID != "" {
				line = unit.After.Mode + " " + unit.After.OID + "\t" + unit.Path
			}
			if err := UpdateIndexInfo(ctx, repoDir, index, []string{line}); err != nil {
				return nil, err
			}
		}
		tree, err := WriteTree(ctx, repoDir, index)
		if err != nil {
			return nil, err
		}
		if tree == previousTree {
			return nil, errors.New("git intent history: proposed goal has no net change")
		}
		previousTree = tree
		trees = append(trees, tree)
	}
	return trees, nil
}

// IntentHistoryReconstructionOptions is explicit, quality-only reconstruction
// onto a new branch. The original branch is preserved even when it is shared.
// Callers provide already-validated semantic goals and exact recorded trees.
type IntentHistoryReconstructionOptions struct {
	SourceBranchRef string
	TargetBranchRef string
	ExpectedHead    string
	OldChain        []string
	Replacements    []IntentRepairReplacement
	PlanID          string
	VerifyCommit    IntentRepairCommitVerifier
	DryRun          bool
}

// ApplyIntentHistoryReconstruction creates an approved history on a new branch.
// It never checks out files or changes the original branch or index. Each exact
// commit is verified before an atomic source-CAS/backup/new-branch transaction.
func ApplyIntentHistoryReconstruction(ctx context.Context, repoDir string, opts IntentHistoryReconstructionOptions) (IntentRepairApplyResult, error) {
	var result IntentRepairApplyResult
	if !isFullBranchRef(opts.SourceBranchRef) || !isFullBranchRef(opts.TargetBranchRef) || opts.SourceBranchRef == opts.TargetBranchRef || opts.ExpectedHead == "" || opts.PlanID == "" || len(opts.OldChain) == 0 || len(opts.OldChain) > MaxIntentHistoryCommits || len(opts.Replacements) == 0 || len(opts.Replacements) > MaxIntentHistoryCommits {
		return result, errors.New("git intent history: invalid explicit reconstruction input")
	}
	for _, ref := range []string{opts.SourceBranchRef, opts.TargetBranchRef} {
		if _, err := Run(ctx, RunOpts{Dir: repoDir, Timeout: DefaultReadTimeout}, "check-ref-format", ref); err != nil {
			return result, err
		}
	}
	if _, err := RevParse(ctx, repoDir, opts.TargetBranchRef); err == nil {
		return RecoverIntentHistoryReconstruction(ctx, repoDir, opts)
	} else if !errors.Is(err, ErrRefNotFound) {
		return result, err
	}
	if err := checkIntentHistorySource(ctx, repoDir, opts); err != nil {
		return result, err
	}
	commits := make([]IntentRepairOwnedCommit, 0, len(opts.OldChain))
	for _, oid := range opts.OldChain {
		commits = append(commits, IntentRepairOwnedCommit{OID: oid})
	}
	if err := validateIntentRepairReplacements(ctx, repoDir, commits, opts.Replacements, true, true); err != nil {
		return result, err
	}
	finalTree, err := RevParse(ctx, repoDir, opts.ExpectedHead+"^{tree}")
	if err != nil {
		return result, err
	}
	if err := validateIntentRepairFinalTree(ctx, repoDir, opts.Replacements, finalTree); err != nil {
		return result, err
	}
	backup, err := IntentRepairBackupRef(opts.SourceBranchRef, opts.PlanID)
	if err != nil {
		return result, err
	}
	result = IntentRepairApplyResult{Eligible: true, OldHead: opts.ExpectedHead, BackupRef: backup, PlannedCommits: len(opts.Replacements), DryRun: opts.DryRun}
	if opts.DryRun {
		return result, nil
	}
	parent, err := firstParent(ctx, repoDir, opts.OldChain[0])
	if err != nil {
		return result, err
	}
	for i, replacement := range opts.Replacements {
		authorOID := replacement.AuthorOID
		if authorOID == "" {
			authorOID = replacement.Replaces[0]
		}
		author, err := commitAuthorEnv(ctx, repoDir, authorOID)
		if err != nil {
			return result, err
		}
		var parents []string
		if parent != "" {
			parents = append(parents, parent)
		}
		oid, err := commitTreeWithEnv(ctx, repoDir, replacement.TreeOID, strings.TrimRight(replacement.Message, "\n"), author, parents...)
		if err != nil {
			return result, err
		}
		if opts.VerifyCommit != nil {
			if err := opts.VerifyCommit(ctx, oid, i); err != nil {
				return result, fmt.Errorf("git intent history: verify rebuilt commit: %w", err)
			}
		}
		for _, old := range replacement.Replaces {
			result.CommitMappings = append(result.CommitMappings, IntentRepairCommitMapping{OldOID: old, NewOID: oid})
		}
		parent = oid
	}
	if err := checkIntentHistorySource(ctx, repoDir, opts); err != nil {
		return result, err
	}
	input := "start\nverify " + opts.SourceBranchRef + " " + opts.ExpectedHead + "\ncreate " + backup + " " + opts.ExpectedHead + "\ncreate " + opts.TargetBranchRef + " " + parent + "\nprepare\ncommit\n"
	if _, err := Run(ctx, RunOpts{Dir: repoDir, Stdin: strings.NewReader(input), Timeout: DefaultWriteTimeout}, "update-ref", "--no-deref", "--stdin"); err != nil {
		return result, fmt.Errorf("git intent history: atomic reconstruction refs: %w", err)
	}
	result.NewHead = parent
	return result, nil
}

// RecoverIntentHistoryReconstruction proves a target created before the worker
// could record completion. Matching messages alone are insufficient: every
// saved tree, parent boundary, author identity and backup must still agree.
func RecoverIntentHistoryReconstruction(ctx context.Context, repoDir string, opts IntentHistoryReconstructionOptions) (IntentRepairApplyResult, error) {
	var result IntentRepairApplyResult
	backup, err := IntentRepairBackupRef(opts.SourceBranchRef, opts.PlanID)
	if err != nil {
		return result, err
	}
	backupOID, err := RevParse(ctx, repoDir, backup)
	if err != nil || backupOID != opts.ExpectedHead {
		return result, errors.New("git intent history: existing target has no matching reconstruction backup")
	}
	head, err := RevParse(ctx, repoDir, opts.TargetBranchRef)
	if err != nil {
		return result, err
	}
	base, err := firstParent(ctx, repoDir, opts.OldChain[0])
	if err != nil {
		return result, err
	}
	var args = []string{"rev-list", "--first-parent", "--reverse", "--max-count=" + fmt.Sprint(MaxIntentHistoryCommits+1)}
	if base != "" {
		args = append(args, base+".."+head)
	} else {
		args = append(args, head)
	}
	out, err := RunWithLimit(ctx, RunOpts{Dir: repoDir, Timeout: DefaultReadTimeout}, DefaultDiffCap, args...)
	if err != nil {
		return result, err
	}
	chain := strings.Fields(string(out))
	if len(chain) != len(opts.Replacements) {
		return result, errors.New("git intent history: existing target does not match the saved goal count")
	}
	result = IntentRepairApplyResult{Eligible: true, OldHead: opts.ExpectedHead, NewHead: head, BackupRef: backup, PlannedCommits: len(chain)}
	parent := base
	for i, oid := range chain {
		parents, err := parentsOf(ctx, repoDir, oid)
		if err != nil {
			return result, err
		}
		if (parent == "" && len(parents) != 0) || (parent != "" && (len(parents) != 1 || parents[0] != parent)) {
			return result, errors.New("git intent history: existing target parent chain changed")
		}
		tree, err := RevParse(ctx, repoDir, oid+"^{tree}")
		if err != nil {
			return result, err
		}
		message, err := commitMessage(ctx, repoDir, oid)
		if err != nil {
			return result, err
		}
		replacement := opts.Replacements[i]
		if tree != replacement.TreeOID || strings.TrimRight(message, "\n") != strings.TrimRight(replacement.Message, "\n") {
			return result, errors.New("git intent history: existing target goal changed")
		}
		authorOID := replacement.AuthorOID
		if authorOID == "" {
			authorOID = replacement.Replaces[0]
		}
		expectedAuthor, err := commitAuthorEnv(ctx, repoDir, authorOID)
		if err != nil {
			return result, err
		}
		actualAuthor, err := commitAuthorEnv(ctx, repoDir, oid)
		if err != nil {
			return result, err
		}
		for key, value := range expectedAuthor {
			if actualAuthor[key] != value {
				return result, errors.New("git intent history: existing target author changed")
			}
		}
		for _, old := range replacement.Replaces {
			result.CommitMappings = append(result.CommitMappings, IntentRepairCommitMapping{OldOID: old, NewOID: oid})
		}
		parent = oid
	}
	finalTree, err := RevParse(ctx, repoDir, opts.ExpectedHead+"^{tree}")
	if err != nil {
		return result, err
	}
	if err := validateIntentRepairFinalTree(ctx, repoDir, opts.Replacements, finalTree); err != nil {
		return result, err
	}
	for i, oid := range chain {
		if opts.VerifyCommit != nil {
			if err := opts.VerifyCommit(ctx, oid, i); err != nil {
				return result, fmt.Errorf("git intent history: verify recovered goal: %w", err)
			}
		}
	}
	currentTarget, err := RevParse(ctx, repoDir, opts.TargetBranchRef)
	if err != nil {
		return result, err
	}
	currentBackup, err := RevParse(ctx, repoDir, backup)
	if err != nil {
		return result, err
	}
	if currentTarget != head || currentBackup != opts.ExpectedHead {
		return result, errors.New("git intent history: reconstruction refs changed during recovery verification")
	}
	return result, nil
}

func checkIntentHistorySource(ctx context.Context, repoDir string, opts IntentHistoryReconstructionOptions) error {
	branch, err := RunBranchRef(ctx, repoDir)
	if err != nil {
		return err
	}
	head, err := RevParse(ctx, repoDir, opts.SourceBranchRef)
	if err != nil {
		return err
	}
	if branch != opts.SourceBranchRef || head != opts.ExpectedHead || opts.OldChain[len(opts.OldChain)-1] != head {
		return errors.New("git intent history: source branch changed; regenerate the plan")
	}
	if _, err := RevParse(ctx, repoDir, opts.TargetBranchRef); !errors.Is(err, ErrRefNotFound) {
		if err != nil {
			return err
		}
		return errors.New("git intent history: target branch already exists")
	}
	for i, oid := range opts.OldChain {
		resolved, err := RevParse(ctx, repoDir, oid+"^{commit}")
		if err != nil || resolved != oid {
			return fmt.Errorf("git intent history: source commit %q is not an exact oid", oid)
		}
		parents, err := parentsOf(ctx, repoDir, oid)
		if err != nil {
			return err
		}
		if len(parents) > 1 || (i > 0 && (len(parents) == 0 || parents[0] != opts.OldChain[i-1])) {
			return errors.New("git intent history: source chain is not a linear suffix")
		}
	}
	return nil
}
