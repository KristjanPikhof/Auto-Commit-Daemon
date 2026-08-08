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
// from one private ref. The caller combines sets across refs so shared blobs
// count once toward a worktree checkpoint-content budget.
func ReachableObjectSizes(ctx context.Context, repoDir, ref string) (map[string]int64, error) {
	out, err := RunWithLimit(ctx, RunOpts{Dir: repoDir, Timeout: DefaultReadTimeout},
		256<<20, "rev-list", "--objects", "--no-object-names", ref)
	if err != nil {
		return nil, fmt.Errorf("git: inventory %s: %w", ref, err)
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
