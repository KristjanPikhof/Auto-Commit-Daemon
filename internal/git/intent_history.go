package git

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
)

// Explicit history rewriting may examine a whole branch.
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

// IntentHistoryAuthors assigns stable boundary IDs in recorded order. Names
// and email addresses stay local; author dates do not create new boundaries.
func IntentHistoryAuthors(ctx context.Context, repoDir string, units []IntentHistoryUnit) (map[string]int, error) {
	authors := make(map[string]int)
	identities := make(map[string]int)
	for _, unit := range units {
		if authors[unit.OldOID] != 0 {
			continue
		}
		author, err := commitAuthorEnv(ctx, repoDir, unit.OldOID)
		if err != nil {
			return nil, err
		}
		identity := author["GIT_AUTHOR_NAME"] + "\x00" + author["GIT_AUTHOR_EMAIL"]
		if identities[identity] == 0 {
			identities[identity] = len(identities) + 1
		}
		authors[unit.OldOID] = identities[identity]
	}
	return authors, nil
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
