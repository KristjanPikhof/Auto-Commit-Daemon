package git

import (
	"bytes"
	"context"
	"fmt"
	"strings"
)

// TreeEntry is one row of `git ls-tree -z` output.
type TreeEntry struct {
	Mode string // e.g. "100644", "100755", "120000", "040000", "160000"
	Type string // "blob", "tree", "commit"
	OID  string // hex-encoded object id
	Path string // path relative to the tree root
}

// LsTree runs `git ls-tree -z [-r] <rev> [-- <paths...>]` and parses the
// NUL-delimited output. Recursive=true expands tree entries.
func LsTree(ctx context.Context, repoDir, rev string, recursive bool, paths ...string) ([]TreeEntry, error) {
	args := []string{"ls-tree", "-z"}
	if recursive {
		args = append(args, "-r")
	}
	args = append(args, rev)
	if len(paths) > 0 {
		args = append(args, "--")
		args = append(args, paths...)
	}
	out, err := Run(ctx, RunOpts{Dir: repoDir}, args...)
	if err != nil {
		return nil, err
	}
	return parseLsTree(out)
}

// LsTreeBlobOID returns the blob OID for path at ref, or an empty string
// when path is absent or resolves to a non-blob tree entry.
func LsTreeBlobOID(ctx context.Context, repoDir, ref, path string) (string, error) {
	entries, err := LsTree(ctx, repoDir, ref, false, path)
	if err != nil {
		return "", err
	}
	for _, entry := range entries {
		if entry.Path == path && entry.Type == "blob" {
			return entry.OID, nil
		}
	}
	return "", nil
}

func parseLsTree(out []byte) ([]TreeEntry, error) {
	var entries []TreeEntry
	for _, rec := range bytes.Split(out, []byte{0}) {
		if len(rec) == 0 {
			continue
		}
		// "<mode> SP <type> SP <oid> TAB <path>"
		tab := bytes.IndexByte(rec, '\t')
		if tab < 0 {
			return nil, fmt.Errorf("ls-tree: malformed record %q", string(rec))
		}
		head := string(rec[:tab])
		path := string(rec[tab+1:])
		fields := strings.Fields(head)
		if len(fields) != 3 {
			return nil, fmt.Errorf("ls-tree: malformed header %q", head)
		}
		entries = append(entries, TreeEntry{
			Mode: fields[0],
			Type: fields[1],
			OID:  fields[2],
			Path: path,
		})
	}
	return entries, nil
}

// MktreeEntry is one input line for `git mktree`. For nested trees, the
// caller must build child trees first and pass their OIDs here.
type MktreeEntry struct {
	Mode string
	Type string // "blob" | "tree" | "commit"
	OID  string
	Path string // basename, no slashes
}

// Mktree runs `git mktree -z` over the supplied entries and returns the
// resulting tree OID. Entries do not need to be pre-sorted; git sorts them.
func Mktree(ctx context.Context, repoDir string, entries []MktreeEntry) (string, error) {
	var buf bytes.Buffer
	for _, e := range entries {
		fmt.Fprintf(&buf, "%s %s %s\t%s\x00", e.Mode, e.Type, e.OID, e.Path)
	}
	out, err := Run(ctx, RunOpts{Dir: repoDir, Stdin: &buf}, "mktree", "-z")
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(out)), nil
}

// HashObjectStdin pipes content into `git hash-object -w --stdin` and
// returns the resulting blob OID. The blob is written to the object store.
//
// For symlinks, pass the link target string as content; the caller is
// responsible for using SymlinkMode (120000) when wiring the resulting OID
// into a tree.
func HashObjectStdin(ctx context.Context, repoDir string, content []byte) (string, error) {
	out, err := Run(ctx, RunOpts{
		Dir:   repoDir,
		Stdin: bytes.NewReader(content),
	}, "hash-object", "-w", "--stdin")
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(out)), nil
}

// HashSymlinkBlob writes a blob whose content is the symlink target string.
// Returns the OID and the canonical SymlinkMode so callers can plug it
// straight into a tree builder. Per CLAUDE.md, every symlink — to file or
// directory — must be encoded as mode 120000; the legacy daemon shipped a
// regression here and the Go port repeats the fix.
func HashSymlinkBlob(ctx context.Context, repoDir, target string) (oid, mode string, err error) {
	oid, err = HashObjectStdin(ctx, repoDir, []byte(target))
	if err != nil {
		return "", "", err
	}
	return oid, SymlinkMode, nil
}

// CommitTree runs `git commit-tree <tree> -p <parent> ... -F -` (message
// supplied on stdin) and returns the new commit OID. Pass an empty parent
// slice for an initial commit.
func CommitTree(ctx context.Context, repoDir, treeOID, message string, parents ...string) (string, error) {
	args := []string{"commit-tree", treeOID}
	for _, p := range parents {
		args = append(args, "-p", p)
	}
	args = append(args, "-F", "-")
	out, err := Run(ctx, RunOpts{
		Dir:   repoDir,
		Stdin: strings.NewReader(message),
	}, args...)
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(out)), nil
}

// IndexEntry is one row of `git ls-files -s -z` output.
type IndexEntry struct {
	Mode  string
	OID   string
	Stage int
	Path  string
}

// LiveIndexOp describes one path-scoped reconciliation against the repo's
// default live index. Empty BeforeMode/BeforeOID means the path is expected
// to be absent. Delete removes the entry when the before-state matches;
// otherwise AfterMode/AfterOID is written.
type LiveIndexOp struct {
	Path       string
	BeforeMode string
	BeforeOID  string
	AfterMode  string
	AfterOID   string
	Delete     bool
}

// LiveIndexSkip records a path that was intentionally not reconciled because
// the live index contained protected user state or no longer matched the
// expected before-state.
type LiveIndexSkip struct {
	Path   string
	Reason string
}

// LiveIndexReconcileResult reports which path-scoped operations were applied
// and which were skipped. Applied paths are populated only after update-index
// succeeds.
type LiveIndexReconcileResult struct {
	Applied []string
	Skipped []LiveIndexSkip
}

// LsFilesStaged returns the staged index entries for the given repo's
// default index, optionally scoped to the supplied paths. NUL-delimited
// output so paths with spaces or newlines are handled correctly.
//
// For inspecting an isolated scratch index (e.g. the per-replay index seeded
// from BaseHead), use LsFilesIndex instead.
func LsFilesStaged(ctx context.Context, repoDir string, paths ...string) ([]IndexEntry, error) {
	return LsFilesIndex(ctx, repoDir, "", paths...)
}

// LsFilesIndex returns the staged index entries for the given repo,
// optionally redirected to indexFile via GIT_INDEX_FILE. When indexFile is
// empty the call falls through to the repo's default index (the legacy
// LsFilesStaged behavior). Paths are NUL-delimited so spaces and newlines
// in filenames round-trip correctly.
//
// Mirrors snapshot-replay._live_index_entries — the replay loop seeds a
// scratch index from BaseHead via read-tree, advances it with each event's
// ops, and reads back through this helper to verify the next op's
// before-state. The repo's main index is never inspected for normal queued
// history.
func LsFilesIndex(ctx context.Context, repoDir, indexFile string, paths ...string) ([]IndexEntry, error) {
	args := []string{"ls-files", "-s", "-z"}
	if len(paths) > 0 {
		args = append(args, "--")
		args = append(args, paths...)
	}
	extra := map[string]string{}
	if indexFile != "" {
		extra["GIT_INDEX_FILE"] = indexFile
	}
	out, err := Run(ctx, RunOpts{Dir: repoDir, ExtraEnv: extra}, args...)
	if err != nil {
		return nil, err
	}
	var entries []IndexEntry
	for _, rec := range bytes.Split(out, []byte{0}) {
		if len(rec) == 0 {
			continue
		}
		// "<mode> SP <oid> SP <stage> TAB <path>"
		tab := bytes.IndexByte(rec, '\t')
		if tab < 0 {
			return nil, fmt.Errorf("ls-files -s: malformed record %q", string(rec))
		}
		head := string(rec[:tab])
		path := string(rec[tab+1:])
		fields := strings.Fields(head)
		if len(fields) != 3 {
			return nil, fmt.Errorf("ls-files -s: malformed header %q", head)
		}
		var stage int
		if _, err := fmt.Sscanf(fields[2], "%d", &stage); err != nil {
			return nil, fmt.Errorf("ls-files -s: bad stage %q: %w", fields[2], err)
		}
		entries = append(entries, IndexEntry{
			Mode:  fields[0],
			OID:   fields[1],
			Stage: stage,
			Path:  path,
		})
	}
	return entries, nil
}

// UpdateIndexInfo pipes index entries to `git update-index -z --index-info`.
// Each entry of the form "<mode> <oid>\t<path>" is appended; passing
// "0 0000...0\t<path>" deletes that path. Mirrors apply_ops_to_index in the
// legacy snapshot_state.py.
//
// indexFile is optional: when non-empty, the call uses GIT_INDEX_FILE so the
// repo's main index is not touched (the legacy daemon uses a per-worker
// scratch index for the same reason — see plan §8.3).
func UpdateIndexInfo(ctx context.Context, repoDir, indexFile string, lines []string) error {
	var buf bytes.Buffer
	for _, l := range lines {
		buf.WriteString(l)
		buf.WriteByte(0)
	}
	extra := map[string]string{}
	if indexFile != "" {
		extra["GIT_INDEX_FILE"] = indexFile
	}
	_, err := Run(ctx, RunOpts{
		Dir:      repoDir,
		Stdin:    &buf,
		ExtraEnv: extra,
	}, "update-index", "-z", "--index-info")
	return err
}

// ReconcileLiveIndex applies guarded, path-scoped compare-and-swap updates to
// the repo's default live index. It never reads from or writes to a scratch
// GIT_INDEX_FILE, and it never mutates the worktree.
func ReconcileLiveIndex(ctx context.Context, repoDir string, ops []LiveIndexOp) (LiveIndexReconcileResult, error) {
	var res LiveIndexReconcileResult
	if len(ops) == 0 {
		return res, nil
	}

	paths := make([]string, 0, len(ops))
	for _, op := range ops {
		paths = append(paths, op.Path)
	}
	entries, err := LsFilesStaged(ctx, repoDir, paths...)
	if err != nil {
		return res, err
	}
	byPath := make(map[string][]IndexEntry)
	for _, entry := range entries {
		byPath[entry.Path] = append(byPath[entry.Path], entry)
	}

	const zeroOID = "0000000000000000000000000000000000000000"
	var lines []string
	var applied []string
	for _, op := range ops {
		if op.Path == "" {
			res.Skipped = append(res.Skipped, LiveIndexSkip{Path: op.Path, Reason: "empty_path"})
			continue
		}
		current := byPath[op.Path]
		if reason, ok, err := liveIndexBeforeMatches(ctx, repoDir, op, current); err != nil {
			return res, err
		} else if !ok {
			res.Skipped = append(res.Skipped, LiveIndexSkip{Path: op.Path, Reason: reason})
			continue
		}
		if op.Delete {
			lines = append(lines, fmt.Sprintf("0 %s\t%s", zeroOID, op.Path))
		} else if op.AfterMode == "" || op.AfterOID == "" {
			res.Skipped = append(res.Skipped, LiveIndexSkip{Path: op.Path, Reason: "missing_after_state"})
			continue
		} else {
			lines = append(lines, fmt.Sprintf("%s %s\t%s", op.AfterMode, op.AfterOID, op.Path))
		}
		applied = append(applied, op.Path)
	}
	if len(lines) == 0 {
		return res, nil
	}
	if err := UpdateIndexInfo(ctx, repoDir, "", lines); err != nil {
		return res, err
	}
	res.Applied = applied
	return res, nil
}

func liveIndexBeforeMatches(ctx context.Context, repoDir string, op LiveIndexOp, entries []IndexEntry) (string, bool, error) {
	if len(entries) == 0 {
		if op.BeforeMode == "" && op.BeforeOID == "" {
			return "", true, nil
		}
		return "missing_expected_entry", false, nil
	}
	if len(entries) != 1 || entries[0].Stage != 0 {
		return "unmerged_entry", false, nil
	}
	entry := entries[0]
	if flags, err := liveIndexDebugFlags(ctx, repoDir, op.Path); err != nil {
		return "", false, err
	} else if flags != "" && flags != "0" {
		return "protected_index_flags", false, nil
	}
	if op.BeforeMode == "" && op.BeforeOID == "" {
		return "unexpected_existing_entry", false, nil
	}
	if entry.Mode != op.BeforeMode || entry.OID != op.BeforeOID {
		return "mismatched_entry", false, nil
	}
	return "", true, nil
}

func liveIndexDebugFlags(ctx context.Context, repoDir, path string) (string, error) {
	out, err := Run(ctx, RunOpts{Dir: repoDir}, "ls-files", "--debug", "-z", "--", path)
	if err != nil {
		return "", err
	}
	_, debug, ok := bytes.Cut(out, []byte{0})
	if !ok {
		return "", nil
	}
	for _, line := range bytes.Split(debug, []byte{'\n'}) {
		line = bytes.TrimSpace(line)
		if bytes.HasPrefix(line, []byte("size: ")) {
			if i := bytes.Index(line, []byte("flags: ")); i >= 0 {
				return strings.TrimSpace(string(line[i+len("flags: "):])), nil
			}
		}
	}
	return "", nil
}

// WriteTree runs `git write-tree` and returns the OID of the tree built
// from the current index. Pair with UpdateIndexInfo + GIT_INDEX_FILE for an
// isolated index.
func WriteTree(ctx context.Context, repoDir, indexFile string) (string, error) {
	extra := map[string]string{}
	if indexFile != "" {
		extra["GIT_INDEX_FILE"] = indexFile
	}
	out, err := Run(ctx, RunOpts{Dir: repoDir, ExtraEnv: extra}, "write-tree")
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(out)), nil
}

// ReadTree resets the index pointed to by indexFile (or the repo index when
// empty) to match rev. Used by the replay's recovery path.
func ReadTree(ctx context.Context, repoDir, indexFile, rev string) error {
	extra := map[string]string{}
	if indexFile != "" {
		extra["GIT_INDEX_FILE"] = indexFile
	}
	_, err := Run(ctx, RunOpts{Dir: repoDir, ExtraEnv: extra}, "read-tree", rev)
	return err
}
