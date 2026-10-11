// capture.go stores complete eligible worktree snapshots in durable checkpoints.
//
// Walk semantics carried over from the legacy daemon (snapshot-capture.py):
//   - Root-anchored directory and file reads reject every symlink ancestor.
//   - Symlinks always emit mode 120000 regardless of target type. NEVER
//     descend into a symlinked directory.
//   - Skip nested .git (file or dir) and submodule (gitlink mode 160000).
//   - Skip ACD's own .git/acd state subdir.
//   - Sensitive default-deny via state.SensitiveMatcher.
//   - Generated dependency/cache tree pruning via state.SafeIgnoreMatcher.
//   - Gitignored paths via batch git.IgnoreChecker.
//   - Regular files above ACD_MAX_FILE_BYTES stream into Git objects.
//   - Regular files opened with O_NOFOLLOW + post-open lstat/fstat
//     ino+dev+mode verification (TOCTOU defense against symlink swap).
package daemon

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/checkpoint"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
	acdtrace "github.com/KristjanPikhof/Auto-Commit-Daemon/internal/trace"
)

// EnvMaxFileBytes is the regular-file buffering threshold. Mirrors the legacy
// SNAPSHOTD_MAX_FILE_BYTES knob with the new ACD_ prefix.
const EnvMaxFileBytes = "ACD_MAX_FILE_BYTES"

// DefaultMaxFileBytes is the default buffering threshold (5 MiB).
const DefaultMaxFileBytes int64 = 5 << 20

const (
	MetaKeyProtectionObservationEpoch    = "protection.observation_epoch"
	MetaKeyProtectionCoveredEpoch        = "protection.covered_epoch"
	MetaKeyProtectionCheckpointID        = "protection.checkpoint_id"
	MetaKeyProtectionComplete            = "protection.complete"
	MetaKeyProtectionRetentionOverBudget = "protection.retention_over_budget"
	MetaKeyProtectionFullPollTS          = "protection.full_poll_ts"
	MetaKeyProtectionWatcherQueueDepth   = "protection.watcher_queue_depth"
	MetaKeyProtectionScope               = "protection.scope"
	MetaKeyProtectionTreeDigest          = "protection.tree_digest"
)

func requiredProtectionCheckpointEpochKey(worktreeID string) string {
	return "protection.required_checkpoint_epoch:" + worktreeID
}

// stateSubdir is the per-repo state directory name inside .git/. Keeping it
// here as a local constant avoids importing internal/state just for the
// string; the package-level helper in state/db.go is "acd" via
// AcdDirFromGitDir.
const stateSubdir = "acd"

// CaptureSummary describes one checkpoint protection pass.
type CaptureSummary struct {
	Oversize, Errors   int
	WalkedFiles        int64
	CheckpointID       string
	Protected, Partial bool
}

// CaptureContext carries the repository identity frozen at the start of a pass.
type CaptureContext struct {
	BranchRef        string
	BranchGeneration int64
	BaseHead         string // HEAD OID at start of pass (or "" if no HEAD)
}

// CaptureOpts configures one capture pass. Zero-valued fields fall back to
// production defaults; tests inject lighter substitutes.
type CaptureOpts struct {
	MaxFileBytes      int64
	IgnoreChecker     *git.IgnoreChecker
	SensitiveMatcher  *state.SensitiveMatcher
	SafeIgnoreMatcher *state.SafeIgnoreMatcher
	CheckpointStore   *checkpoint.Store
	WorktreeID        string
	ObservationEpoch  int64
	CheckpointReason  string
	// Both observed HEAD and every stage of the user's index supply exclusions.
	SubmodulePaths map[string]bool
	Trace          acdtrace.Logger
}

// resolveMaxFileBytes consults EnvMaxFileBytes, falls back to default.
func resolveMaxFileBytes(opt int64) int64 {
	if opt > 0 {
		return opt
	}
	if env := os.Getenv(EnvMaxFileBytes); env != "" {
		if n, err := strconv.ParseInt(env, 10, 64); err == nil && n > 0 {
			return n
		}
	}
	return DefaultMaxFileBytes
}

// seconds

// protectionIndexCache avoids reparsing an unchanged native index on idle scans.
// It stores only Gitlink paths; checkpoint protection never writes the index.
type protectionIndexCache struct {
	digest string
	paths  map[string]bool
}

func protectionSubmodulePaths(ctx context.Context, repo string, base map[string]bool, cache *protectionIndexCache) (map[string]bool, error) {
	if cache == nil {
		cache = &protectionIndexCache{}
	}
	digest, err := git.IndexContentDigest(ctx, repo)
	if err != nil {
		return nil, err
	}
	if cache.paths == nil || cache.digest != digest {
		entries, err := git.LsFilesIndex(ctx, repo, "")
		if err != nil {
			return nil, err
		}
		paths := map[string]bool{}
		for _, entry := range entries {
			if entry.Mode == "160000" {
				paths[entry.Path] = true
			}
		}
		// A changed index may be used conservatively for this scan, but it
		// cannot populate the cache under the earlier digest.
		after, err := git.IndexContentDigest(ctx, repo)
		if err != nil {
			return nil, err
		}
		cache.paths = paths
		cache.digest = ""
		if after == digest {
			cache.digest = digest
		}
	}
	paths := make(map[string]bool, len(base)+len(cache.paths))
	for path, excluded := range base {
		paths[path] = excluded
	}
	for path := range cache.paths {
		paths[path] = true
	}
	return paths, nil
}

// ProtectWorktree runs the universal safety path without classifying or
// publishing capture events. The normal worker uses it for every observed
// tree, including detached HEAD, conflicts and active Git operations.
func ProtectWorktree(ctx context.Context, repoRoot string, db *state.DB, cctx CaptureContext, opts CaptureOpts) (CaptureSummary, error) {
	if repoRoot == "" || db == nil || opts.CheckpointStore == nil {
		return CaptureSummary{}, errors.New("daemon: ProtectWorktree requires repo, db, and checkpoint store")
	}
	matcher := opts.SensitiveMatcher
	if matcher == nil {
		matcher = state.NewSensitiveMatcher()
	}
	safeIgnore := opts.SafeIgnoreMatcher
	if safeIgnore == nil {
		safeIgnore = state.NewSafeIgnoreMatcher()
	}
	live, protected, summary, err := walkLive(ctx, repoRoot, walkOpts{
		matcher:       matcher,
		safeIgnore:    safeIgnore,
		ignoreChecker: opts.IgnoreChecker,
		submodules:    opts.SubmodulePaths,
		maxBytes:      resolveMaxFileBytes(opts.MaxFileBytes),
		db:            db,
		durable:       true,
	})
	if err != nil {
		_ = state.MetaSet(context.Background(), db, MetaKeyProtectionComplete, "false")
		return summary, err
	}
	if err := completeProtectionCheckpoint(ctx, repoRoot, db, cctx, opts,
		live, protected, false, &summary); err != nil {
		_ = state.MetaSet(context.Background(), db, MetaKeyProtectionComplete, "false")
		return summary, err
	}
	if summary.Partial {
		return summary, partialCaptureError(summary)
	}
	return summary, nil
}

// ScanProtectedEntries performs the same privacy and safety filtered scan used
// by checkpoint capture without mutating SQLite, refs, the index, or shadow
// state. Restore preview uses it to compare the live protected scope with a
// retained checkpoint.
func ScanProtectedEntries(ctx context.Context, repoRoot string, opts CaptureOpts) ([]checkpoint.Entry, []state.CheckpointExclusion, CaptureSummary, error) {
	matcher := opts.SensitiveMatcher
	if matcher == nil {
		matcher = state.NewSensitiveMatcher()
	}
	safeIgnore := opts.SafeIgnoreMatcher
	if safeIgnore == nil {
		safeIgnore = state.NewSafeIgnoreMatcher()
	}
	live, protected, summary, err := walkLive(ctx, repoRoot, walkOpts{
		matcher:       matcher,
		safeIgnore:    safeIgnore,
		ignoreChecker: opts.IgnoreChecker,
		submodules:    opts.SubmodulePaths,
		maxBytes:      resolveMaxFileBytes(opts.MaxFileBytes),
		durable:       opts.CheckpointStore != nil,
		hashOnly:      opts.CheckpointStore == nil,
	})
	if err != nil {
		return nil, nil, summary, err
	}
	if summary.Errors > 0 || summary.Oversize > 0 {
		return nil, checkpointExclusions(protected), summary,
			fmt.Errorf("daemon: protection scan incomplete (unreadable=%d unstable=%d)", summary.Errors, summary.Oversize)
	}
	entries, _ := checkpointEntries(live)
	return entries, checkpointExclusions(protected), summary, nil
}

func completeProtectionCheckpoint(
	ctx context.Context,
	repoRoot string,
	db *state.DB,
	cctx CaptureContext,
	opts CaptureOpts,
	live map[string]LiveEntry,
	protectedSkips map[string]skippedPresent,
	forceNew bool,
	summary *CaptureSummary,
) error {
	epoch := opts.ObservationEpoch
	if epoch <= 0 {
		var err error
		epoch, err = state.BeginOrJoinMetaEpoch(ctx, db,
			MetaKeyProtectionObservationEpoch, MetaKeyProtectionComplete)
		if err != nil {
			return fmt.Errorf("daemon: begin implicit protection observation: %w", err)
		}
	}
	if err := state.MetaSet(ctx, db, MetaKeyProtectionObservationEpoch,
		strconv.FormatInt(epoch, 10)); err != nil {
		return fmt.Errorf("daemon: persist protection observation epoch: %w", err)
	}
	issues := captureIssues(protectedSkips)
	summary.Partial = len(issues) > 0

	entries, liveDigest := checkpointEntries(live)
	worktreeID := opts.WorktreeID
	if worktreeID == "" {
		worktreeID = checkpoint.WorktreeID(repoRoot)
	}
	projection, err := state.ReadCheckpointProjection(ctx, db.Path(), 1)
	if err != nil {
		return fmt.Errorf("daemon: read checkpoint projection: %w", err)
	}
	priorDigest, digestOK, digestErr := state.MetaGet(ctx, db, MetaKeyProtectionTreeDigest)
	if digestErr != nil {
		return fmt.Errorf("daemon: read protection tree digest: %w", digestErr)
	}
	requiredEpoch, err := requiredProtectionCheckpointEpoch(ctx, db, worktreeID)
	if err != nil {
		return err
	}
	barrierNeedsCheckpoint := requiredEpoch > 0 && epoch >= requiredEpoch
	if !forceNew && !barrierNeedsCheckpoint && projection.Latest != nil && projection.Latest.WorktreeID == worktreeID &&
		projection.Latest.ObservedHead == cctx.BaseHead && projection.Latest.ObservedRef == cctx.BranchRef &&
		!summary.Partial && !projection.Latest.Partial && projection.Latest.Phase == state.CheckpointCompleted &&
		digestOK && priorDigest == liveDigest {
		if err := persistProtectionCoverage(ctx, db, epoch, projection.Latest.ID, liveDigest, protectionScope(opts, projection.Latest.ID)); err != nil {
			return err
		}
		summary.CheckpointID = projection.Latest.ID
		summary.Protected = true
		return nil
	}

	partialFingerprint := fmt.Sprintf("%s/%v", liveDigest, issues)
	if summary.Partial && !forceNew && projection.Latest != nil && projection.Latest.WorktreeID == worktreeID &&
		projection.Latest.ObservedHead == cctx.BaseHead && projection.Latest.ObservedRef == cctx.BranchRef && projection.Latest.Phase == state.CheckpointCompleted && projection.Latest.Partial {
		previous, _, err := state.MetaGet(ctx, db, "protection.partial_fingerprint")
		if err != nil {
			return err
		}
		if previous == partialFingerprint {
			summary.CheckpointID = projection.Latest.ID
			return nil
		}
	}
	exclusions := checkpointExclusions(protectedSkips)
	reason := opts.CheckpointReason
	if reason == "" {
		reason = state.CheckpointReasonPoll
	}
	result, err := opts.CheckpointStore.Create(ctx, checkpoint.Request{
		RepoRoot:         repoRoot,
		WorktreeID:       worktreeID,
		Reason:           reason,
		ObservationEpoch: epoch,
		CoverageEpoch:    epoch,
		ObservedHead:     cctx.BaseHead,
		ObservedRef:      cctx.BranchRef,
		Entries:          entries,
		Exclusions:       exclusions,
		CaptureIssues:    issues,
	})
	if err != nil {
		return fmt.Errorf("daemon: complete protection checkpoint: %w", err)
	}
	if summary.Partial {
		summary.CheckpointID = result.Checkpoint.ID
		return state.MetaSetMany(ctx, db, map[string]string{
			MetaKeyProtectionCheckpointID:    summary.CheckpointID,
			MetaKeyProtectionComplete:        "false",
			"protection.partial_fingerprint": partialFingerprint,
		})
	}
	if err := persistProtectionCoverage(ctx, db, epoch, result.Checkpoint.ID, liveDigest, protectionScope(opts, result.Checkpoint.ID)); err != nil {
		return err
	}
	if requiredEpoch > 0 && epoch >= requiredEpoch {
		if _, err := db.SQL().ExecContext(ctx, `
DELETE FROM daemon_meta WHERE key=? AND CAST(value AS INTEGER)<=?`,
			requiredProtectionCheckpointEpochKey(worktreeID), epoch); err != nil {
			return fmt.Errorf("daemon: clear required protection checkpoint: %w", err)
		}
	}
	summary.CheckpointID = result.Checkpoint.ID
	summary.Protected = true
	return nil
}

// BeginProtectionObservation marks protection incomplete before a scan or an
// accepted semantic hint can return. Concurrent hints and the scan they wake
// join the same incomplete epoch instead of creating artificial uncovered
// gaps. A completed checkpoint is the only path that restores coverage.
func BeginProtectionObservation(ctx context.Context, db *state.DB) (int64, error) {
	if db == nil {
		return 0, errors.New("daemon: protection observation requires state")
	}
	epoch, err := state.BeginOrJoinMetaEpoch(ctx, db,
		MetaKeyProtectionObservationEpoch, MetaKeyProtectionComplete)
	if err != nil {
		return 0, fmt.Errorf("daemon: begin protection observation: %w", err)
	}
	return epoch, nil
}

// RequireProtectionCheckpoint prevents tree-digest reuse for one accepted
// publication barrier. The requirement is scoped to the canonical worktree
// checkpoint identity because linked worktrees share daemon_meta.
func RequireProtectionCheckpoint(
	ctx context.Context,
	db *state.DB,
	worktreeID string,
	epoch int64,
) error {
	if db == nil || len(worktreeID) != 16 || epoch <= 0 {
		return errors.New("daemon: invalid required protection checkpoint identity")
	}
	key := requiredProtectionCheckpointEpochKey(worktreeID)
	current, ok, err := state.MetaGet(ctx, db, key)
	if err != nil {
		return fmt.Errorf("daemon: read required protection checkpoint: %w", err)
	}
	if ok {
		currentEpoch, parseErr := strconv.ParseInt(current, 10, 64)
		if parseErr != nil {
			return fmt.Errorf("daemon: parse required protection checkpoint: %w", parseErr)
		}
		if currentEpoch >= epoch {
			return nil
		}
	}
	if err := state.MetaSet(ctx, db, key, strconv.FormatInt(epoch, 10)); err != nil {
		return fmt.Errorf("daemon: persist required protection checkpoint: %w", err)
	}
	return nil
}

func requiredProtectionCheckpointEpoch(
	ctx context.Context,
	db *state.DB,
	worktreeID string,
) (int64, error) {
	if len(worktreeID) != 16 {
		return 0, nil
	}
	raw, ok, err := state.MetaGet(
		ctx, db, requiredProtectionCheckpointEpochKey(worktreeID))
	if err != nil || !ok {
		return 0, err
	}
	epoch, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || epoch <= 0 {
		return 0, fmt.Errorf(
			"daemon: invalid required protection checkpoint epoch %q", raw)
	}
	return epoch, nil
}

type ProtectionScope struct {
	Version        int      `json:"version"`
	CheckpointID   string   `json:"checkpoint_id"`
	Sensitive      []string `json:"sensitive"`
	SensitiveAllow []string `json:"sensitive_allow"`
	SafeIgnore     []string `json:"safe_ignore"`
	Submodules     []string `json:"submodules"`
}

func protectionSubmoduleList(paths map[string]bool) []string {
	var result []string
	for path, excluded := range paths {
		if excluded {
			result = append(result, path)
		}
	}
	sort.Strings(result)
	return result
}

func protectionScope(opts CaptureOpts, id string) *ProtectionScope {
	sensitive := opts.SensitiveMatcher
	if sensitive == nil {
		sensitive = state.NewSensitiveMatcher()
	}
	safe := opts.SafeIgnoreMatcher
	if safe == nil {
		safe = state.NewSafeIgnoreMatcher()
	}
	return &ProtectionScope{
		Version: 1, CheckpointID: id,
		Sensitive: sensitive.Patterns(), SensitiveAllow: sensitive.AllowPatterns(),
		SafeIgnore: safe.Patterns(), Submodules: protectionSubmoduleList(opts.SubmodulePaths),
	}
}

func persistProtectionCoverage(ctx context.Context, db *state.DB, epoch int64, checkpointID, liveDigest string, scope *ProtectionScope) error {
	encoded := ""
	if scope != nil {
		body, err := json.Marshal(scope)
		if err != nil {
			return err
		}
		encoded = string(body)
	}
	return state.MetaSetMany(ctx, db, map[string]string{
		MetaKeyProtectionScope:             encoded,
		MetaKeyProtectionCoveredEpoch:      strconv.FormatInt(epoch, 10),
		MetaKeyProtectionCheckpointID:      checkpointID,
		MetaKeyProtectionTreeDigest:        liveDigest,
		MetaKeyProtectionComplete:          "true",
		MetaKeyProtectionFullPollTS:        strconv.FormatFloat(float64(time.Now().UnixNano())/float64(time.Second), 'f', -1, 64),
		MetaKeyProtectionWatcherQueueDepth: "0",
	})
}

// CompleteProtectionCoverage records that checkpointID durably covers the
// supplied observation. Recovery paths use it only after independently
// proving the complete protected entry set.
func CompleteProtectionCoverage(ctx context.Context, db *state.DB, epoch int64, checkpointID string, entries []checkpoint.Entry) error {
	if _, err := state.ResolveCheckpoint(ctx, db.Path(), checkpointID); err != nil {
		return err
	}
	return persistProtectionCoverage(ctx, db, epoch, checkpointID, ProtectionEntriesDigest(entries), nil)
}

// ProtectionEntriesDigest returns the canonical digest used by checkpoint
// reuse and protection status. Callers may supply entries in any order.
func ProtectionEntriesDigest(entries []checkpoint.Entry) string {
	entries = append([]checkpoint.Entry(nil), entries...)
	sort.Slice(entries, func(i, j int) bool { return entries[i].Path < entries[j].Path })
	hash := sha256.New()
	for _, entry := range entries {
		hash.Write([]byte(entry.Path))
		hash.Write([]byte{0})
		hash.Write([]byte(entry.Mode))
		hash.Write([]byte{0})
		hash.Write([]byte(entry.OID))
		hash.Write([]byte{0})
	}
	return "sha256:" + hex.EncodeToString(hash.Sum(nil))
}

func checkpointEntries(live map[string]LiveEntry) ([]checkpoint.Entry, string) {
	entries := make([]checkpoint.Entry, 0, len(live))
	for _, entry := range live {
		entries = append(entries, checkpoint.Entry{Path: entry.Path, Mode: entry.Mode, OID: entry.OID})
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Path < entries[j].Path })
	return entries, ProtectionEntriesDigest(entries)
}

func checkpointExclusions(protectedSkips map[string]skippedPresent) []state.CheckpointExclusion {
	counts := make(map[string]int64)
	for _, skipped := range protectedSkips {
		if skipped.Reason != "" {
			counts[skipped.Reason]++
		}
	}
	categories := make([]string, 0, len(counts))
	for category := range counts {
		categories = append(categories, category)
	}
	sort.Strings(categories)
	exclusions := make([]state.CheckpointExclusion, 0, len(categories))
	for _, category := range categories {
		exclusions = append(exclusions, state.CheckpointExclusion{Category: category, Count: counts[category]})
	}
	return exclusions
}

// walkOpts bundles inputs to walkLive so the function signature stays
// readable.
type walkOpts struct {
	tree          *protectionTree
	matcher       *state.SensitiveMatcher
	safeIgnore    *state.SafeIgnoreMatcher
	ignoreChecker *git.IgnoreChecker
	submodules    map[string]bool
	maxBytes      int64
	db            *state.DB
	durable       bool
	hashOnly      bool
	blobHasher    *git.BlobHasher
	blobBatch     *durableBlobBatch
}

type durableBlobBatch struct {
	items []git.DurableBlob
	bytes int
}

type skippedPresent struct {
	Reason string
	Dir    bool
}

// ignoreCheckBatchSize caps how many paths walkLive sends per
// IgnoreChecker.Check call. The cap is by file count (not byte size) so
// pathological worktrees with very long path names cannot wedge the
// long-lived `git check-ignore --stdin` subprocess on a single huge write.
// Layers larger than the cap are sliced into multiple round-trips; the
// results are concatenated in order before survivors descend.
const ignoreCheckBatchSize = 1000

// classifyIgnoredBatched calls ig.Check in slices of at most batchSize
// paths and concatenates the boolean results so the returned slice is
// 1:1 with the input. Empty input returns a non-nil empty slice for
// caller convenience. Any error from the underlying Check call aborts
// the loop and surfaces immediately.
func classifyIgnoredBatched(ctx context.Context, ig *git.IgnoreChecker, paths []string, batchSize int) ([]bool, error) {
	out := make([]bool, 0, len(paths))
	if len(paths) == 0 {
		return out, nil
	}
	if batchSize <= 0 {
		batchSize = ignoreCheckBatchSize
	}
	for start := 0; start < len(paths); start += batchSize {
		end := start + batchSize
		if end > len(paths) {
			end = len(paths)
		}
		results, err := ig.Check(ctx, paths[start:end])
		if err != nil {
			return nil, err
		}
		out = append(out, results...)
	}
	return out, nil
}

// walkLive walks the worktree and returns the live map.
//
// Implementation notes:
//   - We BFS the worktree by directory layer instead of letting
//     filepath.WalkDir DFS the whole tree. Each layer batches the cheap
//     filters (symlink, .git, ACD subdir, nested repo, submodule, sensitive)
//     locally; survivors are then run through IgnoreChecker.Check in
//     batches of ignoreCheckBatchSize so a top-level gitignored directory
//     like build/, node_modules/, or DerivedData is pruned before we
//     readdir its 100k+ children. This mirrors fsnotify_watcher.preWalk
//     (commit f647b92): per-layer batched ignore-classify + parent-prune.
//   - Symlinks-to-directory are NEVER descended into (followlinks=false
//     equivalent), preserving the legacy CLAUDE.md regression. They are
//     captured as mode-120000 candidates instead.
//   - Sensitive + ignore checks short-circuit before O_NOFOLLOW + read.
//   - All errors except context cancellation are soft: the daemon must keep
//     running across permission errors or file races.
func walkLive(ctx context.Context, repoRoot string, opts walkOpts) (map[string]LiveEntry, map[string]skippedPresent, CaptureSummary, error) {
	tree, err := openProtectionTree(repoRoot)
	if err != nil {
		return nil, nil, CaptureSummary{}, err
	}
	defer tree.Close()
	opts.tree = tree
	if opts.hashOnly || opts.durable {
		hasher, err := git.NewBlobHasher(ctx, repoRoot)
		if err != nil {
			return nil, nil, CaptureSummary{}, err
		}
		opts.blobHasher = &hasher
	}
	if opts.durable {
		opts.blobBatch = &durableBlobBatch{}
	}
	live := map[string]LiveEntry{}
	protected := map[string]skippedPresent{}
	var summary CaptureSummary
	markProtected := func(rel, reason string, dir bool) {
		if reason == "" {
			return
		}
		protected[rel] = skippedPresent{Reason: reason, Dir: dir}
	}

	// First pass: BFS the worktree, collecting (a) regular-file + symlink
	// candidates that survived the cheap filters and (b) the directory
	// frontier we still need to descend. Both are then batch-classified
	// against IgnoreChecker; ignored directories are pruned before their
	// children are read.
	type candidate = candidateLike
	var pending []candidate

	type dirEntry struct {
		rel string // worktree-relative slashed path; "" for the root
	}
	frontier := []dirEntry{{rel: ""}}

	for len(frontier) > 0 {
		if err := ctx.Err(); err != nil {
			return nil, protected, summary, err
		}

		var nextDirs []dirEntry
		var fileCands []candidate

		// layerHadError coalesces per-entry soft errors into a single
		// summary.Errors bump for the whole BFS layer. Previously the DFS
		// path bumped Errors at most once per pass; the BFS rewrite would
		// otherwise inflate the counter linearly with the number of
		// unreadable entries (a corrupt subtree with 10k bad children
		// reported Errors=10k instead of Errors=1), breaking comparison
		// across versions for triage. Checkpoint issues retain per-path
		// detail; only the aggregate counter is coalesced here.
		layerHadError := false
		bumpLayerError := func() {
			if !layerHadError {
				summary.Errors++
				layerHadError = true
			}
		}

		for _, parent := range frontier {
			children, err := tree.readDir(parent.rel)
			if err != nil {
				// Soft error: the directory vanished or is unreadable.
				markProtected(parent.rel, "unreadable", true)
				bumpLayerError()
			}
			sort.Slice(children, func(i, j int) bool { return children[i].Name() < children[j].Name() })
			for _, d := range children {
				name := d.Name()
				var childRel string
				if parent.rel == "" {
					childRel = name
				} else {
					childRel = parent.rel + "/" + name
				}
				if hasControlPathChar(childRel) {
					recordInvalidPath(ctx, opts.db, childRel, "control_chars")
					markProtected(childRel, "invalid_path", false)
					bumpLayerError()
					continue
				}

				// Always step around .git, regardless of depth. The token
				// only ever exists as a top-level component, but the cheap
				// topComponent slice keeps the check identical to the
				// previous DFS implementation. Our own state lives at
				// <gitDir>/acd, which is inside .git and never reachable as
				// a worktree-rooted top component, so we deliberately do
				// NOT prune top-level "acd/" — that would silently delete
				// user repos containing a literal acd/ directory.
				// TODO: if state ever lives outside .git, prune by
				// comparing the absolute path against gitDir/stateSubdir.
				topComponent := childRel
				if i := strings.IndexByte(childRel, '/'); i >= 0 {
					topComponent = childRel[:i]
				}
				if topComponent == ".git" {
					continue
				}

				fi := d
				mode := fi.Mode()

				// Symlinks: capture as 120000 candidate, never descend.
				if mode&os.ModeSymlink != 0 {
					if reason := protectedFileReason(childRel, opts); reason != "" {
						markProtected(childRel, reason, false)
						continue
					}
					fileCands = append(fileCands, candidate{rel: childRel, fi: fi})
					continue
				}

				if fi.IsDir() {
					if opts.submodules != nil && opts.submodules[childRel] {
						continue
					}
					if opts.matcher != nil && opts.matcher.MatchDirectory(childRel) {
						markProtected(childRel, "sensitive", true)
						continue
					}
					if opts.safeIgnore != nil && opts.safeIgnore.MatchDirectory(childRel) {
						markProtected(childRel, "safe_ignore", true)
						continue
					}
					nextDirs = append(nextDirs, dirEntry{rel: childRel})
					continue
				}

				// Regular files only — sockets/FIFOs/devices skipped quietly.
				if !mode.IsRegular() {
					markProtected(childRel, "non_regular", false)
					continue
				}
				if reason := protectedFileReason(childRel, opts); reason != "" {
					markProtected(childRel, reason, false)
					continue
				}
				fileCands = append(fileCands, candidate{rel: childRel, fi: fi})
			}
		}

		// Per-layer batched ignore check: classify the directory frontier
		// AND the file candidates collected at this layer in one pass so
		// ignored top-level subtrees never get read on the next iteration.
		// Batches are capped at ignoreCheckBatchSize paths to keep any
		// single check-ignore round-trip bounded; larger layers are sliced
		// into successive Check calls.
		if opts.ignoreChecker != nil && (len(nextDirs) > 0 || len(fileCands) > 0) {
			origDirCount := len(nextDirs)
			paths := make([]string, 0, origDirCount+len(fileCands))
			for _, e := range nextDirs {
				paths = append(paths, e.rel)
			}
			for _, c := range fileCands {
				paths = append(paths, c.rel)
			}
			results, ierr := classifyIgnoredBatched(ctx, opts.ignoreChecker, paths, ignoreCheckBatchSize)
			if ierr != nil {
				// Fail-closed: if check-ignore is busted, abort the pass
				// rather than silently committing files git considers
				// ignored.
				return nil, protected, summary, fmt.Errorf("daemon: check-ignore: %w", ierr)
			}

			survivorDirs := make([]dirEntry, 0, len(nextDirs))
			for i, e := range nextDirs {
				if results[i] {
					markProtected(e.rel, "gitignore", true)
					continue
				}
				survivorDirs = append(survivorDirs, e)
			}
			nextDirs = survivorDirs

			survivorFiles := make([]candidate, 0, len(fileCands))
			for j, c := range fileCands {
				if results[origDirCount+j] {
					markProtected(c.rel, "gitignore", false)
					continue
				}
				survivorFiles = append(survivorFiles, c)
			}
			fileCands = survivorFiles
		}

		pending = append(pending, fileCands...)
		frontier = nextDirs
	}

	if err := ctx.Err(); err != nil {
		return nil, protected, summary, err
	}

	for _, c := range pending {
		if err := ctx.Err(); err != nil {
			return nil, protected, summary, err
		}
		summary.WalkedFiles++
		entry, ok, reason, err := hashCandidate(ctx, repoRoot, c, opts)
		if err != nil {
			markProtected(c.rel, "unreadable", false)
			summary.Errors++
			continue
		}
		if !ok {
			markProtected(c.rel, reason, false)
			summary.Oversize++
			continue
		}
		live[c.rel] = entry
	}
	if opts.blobBatch != nil {
		if err := opts.blobBatch.flush(ctx, repoRoot); err != nil {
			return nil, protected, summary, err
		}
	}

	return live, protected, summary, nil
}

func protectedFileReason(path string, opts walkOpts) string {
	if opts.matcher != nil && opts.matcher.Match(path) {
		return "sensitive"
	}
	if opts.safeIgnore != nil && opts.safeIgnore.MatchFile(path) {
		return "safe_ignore"
	}
	return ""
}

// hashCandidate reads beneath the anchored root. Symlinks hash their target
// bytes with mode 120000. Regular files verify identity before and after read,
// and stream into Git when they exceed the buffering threshold.
//
// Returns:
//   - (entry, true,  "",     nil) — captured ok.
//   - (zero,  false, reason, nil) — skipped (unstable or type changed).
//   - (zero,  _,     "",     err) — hard error worth recording in summary.
func hashCandidate(ctx context.Context, repoRoot string, c candidateLike, opts walkOpts) (LiveEntry, bool, string, error) {
	if opts.tree == nil {
		tree, err := openProtectionTree(repoRoot)
		if err != nil {
			return LiveEntry{}, false, "", err
		}
		defer tree.Close()
		opts.tree = tree
	}
	mode := c.fi.Mode()
	if mode&os.ModeSymlink != 0 {
		target, rerr := opts.tree.readlink(c.rel)
		if rerr != nil {
			return LiveEntry{}, false, "", rerr
		}
		if !opts.tree.matches(c.rel, c.fi) {
			return LiveEntry{}, false, "unstable", nil
		}
		var oid string
		var herr error
		content := []byte(target)
		if opts.blobHasher != nil {
			oid, herr = opts.blobHasher.BlobOID(content)
			if herr == nil && opts.blobBatch != nil {
				herr = opts.blobBatch.add(ctx, repoRoot, oid, content)
			}
		} else {
			oid, _, herr = git.HashSymlinkBlob(ctx, repoRoot, target)
		}
		if herr != nil {
			return LiveEntry{}, false, "", herr
		}
		return LiveEntry{Path: c.rel, Mode: git.SymlinkMode, OID: oid}, true, "", nil
	}

	// Regular file: O_NOFOLLOW + verify ino/dev/mode (TOCTOU defense).
	// O_NONBLOCK prevents a regular-file-to-FIFO swap from blocking the open.
	f, err := opts.tree.openFile(c.rel)
	if err != nil {
		return LiveEntry{}, false, "", err
	}
	defer f.Close()

	post, err := f.Stat()
	if err != nil {
		return LiveEntry{}, false, "", err
	}
	if !sameFile(c.fi, post) {
		// Swapped between lstat and open — discard.
		return LiveEntry{}, false, "unstable", nil
	}
	if !post.Mode().IsRegular() {
		return LiveEntry{}, false, "non_regular", nil
	}
	var oid string
	bufferLimit := min(opts.maxBytes, int64(32<<20))
	if post.Size() > bufferLimit {
		// The buffer threshold is not a protection limit. Git consumes the
		// stream under its operation deadline without copying the asset here.
		reader := &io.LimitedReader{R: f, N: post.Size() + 1}
		if opts.hashOnly {
			oid, err = git.HashObjectReaderReadOnly(ctx, repoRoot, reader)
		} else {
			oid, err = git.HashObjectReaderDurable(ctx, repoRoot, reader)
		}
		if err == nil && reader.N != 1 {
			return LiveEntry{}, false, "unstable", nil
		}
	} else {
		var buf []byte
		buf, err = io.ReadAll(io.LimitReader(f, bufferLimit+1))
		if err == nil && int64(len(buf)) != post.Size() {
			return LiveEntry{}, false, "unstable", nil
		}
		if err == nil {
			if opts.blobHasher != nil {
				oid, err = opts.blobHasher.BlobOID(buf)
				if err == nil && opts.blobBatch != nil {
					err = opts.blobBatch.add(ctx, repoRoot, oid, buf)
				}
			} else {
				oid, err = git.HashObjectStdin(ctx, repoRoot, buf)
			}
		}
	}
	if err != nil {
		return LiveEntry{}, false, "", err
	}
	after, err := f.Stat()
	if err != nil {
		return LiveEntry{}, false, "", err
	}
	if !sameFileSnapshot(post, after) || !opts.tree.matches(c.rel, after) {
		return LiveEntry{}, false, "unstable", nil
	}
	return LiveEntry{Path: c.rel, Mode: gitModeFor(post.Mode()), OID: oid}, true, "", nil
}

// candidateLike is the minimal shape hashCandidate needs. Aliasing the
// closure-captured candidate type keeps walkLive's pending slice unboxed.
type candidateLike = struct {
	rel string
	fi  os.FileInfo
}

// sameFile compares ino+dev+mode-type to defend against symlink swaps and
// inode swaps between lstat and open. Mirrors the legacy
// _open_regular_file_safely check.
func sameFile(pre, post os.FileInfo) bool {
	preStat, ok1 := pre.Sys().(*syscall.Stat_t)
	postStat, ok2 := post.Sys().(*syscall.Stat_t)
	if !ok1 || !ok2 {
		// Cannot verify on this OS — best-effort: require type bits to match.
		return pre.Mode().Type() == post.Mode().Type()
	}
	if preStat.Ino != postStat.Ino || preStat.Dev != postStat.Dev {
		return false
	}
	if pre.Mode().Type() != post.Mode().Type() {
		return false
	}
	return true
}

func sameFileSnapshot(pre, post os.FileInfo) bool {
	return sameFile(pre, post) &&
		pre.Size() == post.Size() &&
		pre.Mode() == post.Mode() &&
		pre.ModTime().Equal(post.ModTime())
}

// gitModeFor maps a Go fs.Mode onto a git tree mode for regular files.
// Symlinks are handled separately via SymlinkMode.
func gitModeFor(m os.FileMode) string {
	if m&0o111 != 0 {
		return git.ExecutableFileMode
	}
	return git.RegularFileMode
}

func hasControlPathChar(rel string) bool {
	return strings.ContainsAny(rel, "\x00\t\n\r")
}

func recordInvalidPath(ctx context.Context, db *state.DB, rel, reason string) {
	if db == nil {
		return
	}
	key := "capture-skip-invalid-path:" + metaPathKey(rel)
	_ = state.MetaSet(ctx, db, key, "reason="+reason)
}

func metaPathKey(rel string) string {
	replacer := strings.NewReplacer(
		"\x00", "\\0",
		"\t", "\\t",
		"\n", "\\n",
		"\r", "\\r",
	)
	return replacer.Replace(rel)
}

func captureIssues(skips map[string]skippedPresent) []state.CheckpointCaptureIssue {
	var issues []state.CheckpointCaptureIssue
	for path, skip := range skips {
		switch skip.Reason {
		case "sensitive", "safe_ignore", "gitignore", "non_regular":
			continue
		case "invalid_path":
			path = "" // no control characters in the ledger
		}
		issues = append(issues, state.CheckpointCaptureIssue{Path: path, Subtree: skip.Dir || path == "", Reason: skip.Reason})
	}
	sort.Slice(issues, func(i, j int) bool { return issues[i].Path < issues[j].Path })
	// Several invalid names all represent the same unknown scope.
	unique := issues[:0]
	for _, issue := range issues {
		if len(unique) == 0 || unique[len(unique)-1].Path != issue.Path {
			unique = append(unique, issue)
		}
	}
	return unique
}

func partialCaptureError(summary CaptureSummary) error {
	return fmt.Errorf("daemon: protection scan incomplete (unreadable=%d unstable=%d); readable files saved in a partial checkpoint", summary.Errors, summary.Oversize)
}

func (b *durableBlobBatch) add(ctx context.Context, repoRoot, oid string, content []byte) error {
	b.items = append(b.items, git.DurableBlob{OID: oid, Content: content})
	b.bytes += len(content)
	if len(b.items) < durableBlobBatchCount && b.bytes < durableBlobBatchBytes {
		return nil
	}
	return b.flush(ctx, repoRoot)
}

func (b *durableBlobBatch) flush(ctx context.Context, repoRoot string) error {
	if len(b.items) == 0 {
		return nil
	}
	items := b.items
	b.items = nil
	b.bytes = 0
	return git.EnsureBlobObjectsDurable(ctx, repoRoot, items)
}

const (
	durableBlobBatchCount = 128
	durableBlobBatchBytes = 32 << 20
)
