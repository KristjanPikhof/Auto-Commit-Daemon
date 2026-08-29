package git

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"strconv"
	"strings"
)

// ReachableObjectSizes returns the logical size of every object reachable
// from one private ref.
func ReachableObjectSizes(ctx context.Context, repoDir, ref string) (map[string]int64, error) {
	return ReachableObjectSizesForRefs(ctx, repoDir, []string{ref})
}

// ReachableObjectSizesForRefs returns the logical size of the union of all
// objects reachable from refs. Feeding revisions over stdin keeps the command
// bounded when a repository retains thousands of checkpoint refs, while one
// rev-list traversal ensures shared history is visited only once.
func ReachableObjectSizesForRefs(ctx context.Context, repoDir string, refs []string) (map[string]int64, error) {
	if len(refs) == 0 {
		return map[string]int64{}, nil
	}
	var revisions bytes.Buffer
	seenRefs := make(map[string]struct{}, len(refs))
	for _, ref := range refs {
		if !isValidFullRef(ref) {
			return nil, fmt.Errorf("git: invalid inventory ref %q", ref)
		}
		if _, ok := seenRefs[ref]; ok {
			continue
		}
		seenRefs[ref] = struct{}{}
		revisions.WriteString(ref)
		revisions.WriteByte('\n')
	}
	out, err := RunWithLimit(ctx, RunOpts{
		Dir: repoDir, Timeout: DefaultReadTimeout, Stdin: &revisions,
	}, 256<<20, "rev-list", "--objects", "--no-object-names", "--stdin")
	if err != nil {
		return nil, fmt.Errorf("git: inventory refs: %w", err)
	}
	oids := strings.Fields(string(out))
	if len(oids) == 0 {
		return map[string]int64{}, nil
	}
	var input bytes.Buffer
	for _, oid := range oids {
		input.WriteString(oid)
		input.WriteByte('\n')
	}
	out, err = RunWithLimit(ctx, RunOpts{Dir: repoDir, Timeout: DefaultReadTimeout, Stdin: &input},
		256<<20, "cat-file", "--batch-check=%(objectname) %(objectsize)")
	if err != nil {
		return nil, fmt.Errorf("git: size checkpoint objects: %w", err)
	}
	sizes := make(map[string]int64, len(oids))
	scanner := bufio.NewScanner(bytes.NewReader(out))
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) != 2 {
			return nil, fmt.Errorf("git: malformed object inventory line")
		}
		size, parseErr := strconv.ParseInt(fields[1], 10, 64)
		if parseErr != nil || size < 0 {
			return nil, fmt.Errorf("git: malformed object size")
		}
		sizes[fields[0]] = size
	}
	return sizes, scanner.Err()
}
