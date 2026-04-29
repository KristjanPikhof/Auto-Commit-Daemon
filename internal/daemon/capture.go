// capture.go walks the worktree, hashes every captured file into the git
// object store, and emits classify ops persisted into capture_events +
// capture_ops per §8.2.
//
// Walk semantics carried over from the legacy daemon (snapshot-capture.py):
//   - filepath.WalkDir + manual symlink handling (do NOT call WalkDir on
//     followlinks=true — this is the regression CLAUDE.md calls out).
//   - Symlinks always emit mode 120000 regardless of target type. NEVER
//     descend into a symlinked directory.
//   - Skip nested .git (file or dir) and submodule (gitlink mode 160000).
//   - Skip ACD's own .git/acd state subdir.
//   - Sensitive default-deny via state.SensitiveMatcher.
//   - Gitignored paths via batch git.IgnoreChecker.
//   - Oversize regulars (> ACD_MAX_FILE_BYTES, default 5 MiB) -> meta-only.
//   - Regular files opened with O_NOFOLLOW + post-open lstat/fstat
//     ino+dev+mode verification (TOCTOU defense against symlink swap).
package daemon

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

// EnvMaxFileBytes is the per-file size cap. Mirrors the legacy
// SNAPSHOTD_MAX_FILE_BYTES knob with the new ACD_ prefix.
const EnvMaxFileBytes = "ACD_MAX_FILE_BYTES"

// DefaultMaxFileBytes is the default per-file size cap (5 MiB).
const DefaultMaxFileBytes int64 = 5 << 20

// stateSubdir is the per-repo state directory name inside .git/. Keeping it
// here as a local constant avoids importing internal/state just for the
// string; the package-level helper in state/db.go is "acd" via
// AcdDirFromGitDir.
const stateSubdir = "acd"

// CaptureSummary describes one capture pass.
type CaptureSummary struct {
	EventsAppended int   // number of capture_events rows inserted
	Oversize       int   // files skipped due to size cap
	Errors         int   // soft errors (per-file lstat/open failures)
	WalkedFiles    int64 // for diagnostics
}

// CaptureContext carries the per-pass repository identity that the legacy
// daemon calls "ctx" (branch_ref, branch_generation, base_head). Phase 1
// keeps this struct small and lets the run loop populate it; the
// branch-generation token implementation lives elsewhere (§8.9).
type CaptureContext struct {
	BranchRef        string
	BranchGeneration int64
	BaseHead         string // HEAD OID at start of pass (or "" if no HEAD)
}

// CaptureOpts configures one capture pass. Zero-valued fields fall back to
// production defaults; tests inject lighter substitutes.
type CaptureOpts struct {
	// MaxFileBytes overrides EnvMaxFileBytes / DefaultMaxFileBytes.
	MaxFileBytes int64
	// IgnoreChecker batches gitignore checks. Caller owns the lifetime —
	// typically built once at daemon start and reused for the run.
	IgnoreChecker *git.IgnoreChecker
	// SensitiveMatcher precomputes the active sensitive glob set. Caller
	// owns the lifetime; nil falls back to a fresh matcher per pass (slow
	// but correct).
	SensitiveMatcher *state.SensitiveMatcher
	// SubmodulePaths is the set of repo-relative paths that are submodules
	// (mode 160000 in HEAD's tree). Capture must not descend into them.
	SubmodulePaths map[string]bool
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

// Capture walks the repo, builds the live map, classifies vs the persisted
// shadow_paths for this (branch, generation), persists capture events +
// updates shadow rows, and returns a summary. The caller is expected to
// have bootstrapped the shadow against HEAD before the first capture; this
// helper does not own the bootstrap path.
//
// Callers must pass a stable cctx — the (branch, generation) tuple keys both
// the shadow_paths read AND the capture_events insert, so a concurrent
// branch swap mid-walk would emit events keyed to the new generation while
// the live map was sampled under the old one.
func Capture(ctx context.Context, repoRoot string, db *state.DB, cctx CaptureContext, opts CaptureOpts) (CaptureSummary, error) {
	if repoRoot == "" {
		return CaptureSummary{}, fmt.Errorf("daemon: Capture: empty repoRoot")
	}
	if db == nil {
		return CaptureSummary{}, fmt.Errorf("daemon: Capture: nil db")
	}
	if cctx.BranchRef == "" {
		return CaptureSummary{}, fmt.Errorf("daemon: Capture: empty branch_ref")
	}
	if cctx.BaseHead == "" {
		return CaptureSummary{}, fmt.Errorf("daemon: Capture: empty base_head")
	}

	matcher := opts.SensitiveMatcher
	if matcher == nil {
		matcher = state.NewSensitiveMatcher()
	}
	maxBytes := resolveMaxFileBytes(opts.MaxFileBytes)

	live, summary, err := walkLive(ctx, repoRoot, walkOpts{
		matcher:       matcher,
		ignoreChecker: opts.IgnoreChecker,
		submodules:    opts.SubmodulePaths,
		maxBytes:      maxBytes,
		db:            db,
	})
	if err != nil {
		return summary, err
	}

	shadow, err := loadShadow(ctx, db, cctx)
	if err != nil {
		return summary, fmt.Errorf("daemon: load shadow: %w", err)
	}

	ops := Classify(shadow, live)

	// Persist each classified op as its own capture_events row + capture_ops
	// child. Atomic-per-file commits (§8.3) means one event = one op. We do
	// NOT batch multiple ops into a single event in v1 — keeping the schema
	// flexible is fine, but the replay invariant is "1 commit per event".
	for _, op := range ops {
		if err := ctx.Err(); err != nil {
			return summary, err
		}
		ev := state.CaptureEvent{
			BranchRef:        cctx.BranchRef,
			BranchGeneration: cctx.BranchGeneration,
			BaseHead:         cctx.BaseHead,
			Operation:        op.Op,
			Path:             op.Path,
			Fidelity:         op.Fidelity,
			OldPath:          nullString(op.OldPath),
		}
		stateOps := []state.CaptureOp{toStateOp(op)}
		if _, err := state.AppendCaptureEvent(ctx, db, ev, stateOps); err != nil {
			return summary, fmt.Errorf("daemon: append capture event %s %s: %w", op.Op, op.Path, err)
		}
		summary.EventsAppended++

		// Update shadow_paths to reflect the new live state. Renames erase
		// the old path; deletes erase the path; everything else upserts.
		if err := updateShadow(ctx, db, cctx, op); err != nil {
			return summary, fmt.Errorf("daemon: update shadow %s: %w", op.Path, err)
		}
	}

	return summary, nil
}

func toStateOp(op ClassifiedOp) state.CaptureOp {
	return state.CaptureOp{
		Ord:        0,
		Op:         op.Op,
		Path:       op.Path,
		OldPath:    nullString(op.OldPath),
		BeforeOID:  nullString(op.BeforeOID),
		BeforeMode: nullString(op.BeforeMode),
		AfterOID:   nullString(op.AfterOID),
		AfterMode:  nullString(op.AfterMode),
		Fidelity:   op.Fidelity,
	}
}

// nullString wraps an empty/non-empty string as sql.NullString.
func nullString(s string) sql.NullString {
	if s == "" {
		return sql.NullString{}
	}
	return sql.NullString{String: s, Valid: true}
}

// walkOpts bundles inputs to walkLive so the function signature stays
// readable.
type walkOpts struct {
	matcher       *state.SensitiveMatcher
	ignoreChecker *git.IgnoreChecker
	submodules    map[string]bool
	maxBytes      int64
	db            *state.DB
}

// walkLive walks the worktree and returns the live map.
//
// Implementation notes:
//   - We use filepath.WalkDir but consult lstat ourselves for every entry so
//     symlinks-to-directory are NEVER descended into (followlinks=false
//     equivalent). The `fs.SkipDir` return path lets us prune ignored,
//     submoduled, or nested-repo directories cleanly.
//   - Sensitive + ignore checks short-circuit before O_NOFOLLOW + read.
//   - All errors except context cancellation are soft: the daemon must keep
//     running across permission errors or file races.
func walkLive(ctx context.Context, repoRoot string, opts walkOpts) (map[string]LiveEntry, CaptureSummary, error) {
	live := map[string]LiveEntry{}
	var summary CaptureSummary

	// First pass: collect candidate (rel, fullPath, FileInfo) entries while
	// walking. Defer hashing until after the batched ignore check so we
	// don't hash files git considers ignored.
	type candidate struct {
		rel  string
		full string
		fi   os.FileInfo
	}
	var pending []candidate

	walkErr := filepath.WalkDir(repoRoot, func(path string, d fs.DirEntry, walkErr error) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if walkErr != nil {
			// A descent error (permission, vanished dir). Record and move on.
			summary.Errors++
			if d != nil && d.IsDir() {
				return fs.SkipDir
			}
			return nil
		}

		// Top-level: same dir, skip the root entry itself.
		if path == repoRoot {
			return nil
		}

		rel, err := filepath.Rel(repoRoot, path)
		if err != nil {
			summary.Errors++
			return nil
		}
		rel = filepath.ToSlash(rel)
		if hasControlPathChar(rel) {
			recordInvalidPath(ctx, opts.db, rel, "control_chars")
			summary.Errors++
			if d.IsDir() {
				return fs.SkipDir
			}
			return nil
		}

		// Always step around our own state subdir + .git.
		topComponent := rel
		if i := strings.IndexByte(rel, '/'); i >= 0 {
			topComponent = rel[:i]
		}
		if topComponent == ".git" || topComponent == stateSubdir {
			if d.IsDir() {
				return fs.SkipDir
			}
			return nil
		}

		// lstat the entry (do NOT follow symlinks).
		fi, lstatErr := os.Lstat(path)
		if lstatErr != nil {
			summary.Errors++
			if d.IsDir() {
				return fs.SkipDir
			}
			return nil
		}
		mode := fi.Mode()

		// Symlink handling: ALWAYS treat as symlink entry, regardless of
		// whether the target is a file or a directory. Capture the link
		// target as content, mode 120000. Do not descend.
		//
		// Note: filepath.WalkDir, when it encounters a symlink-to-dir on
		// disk with `d` reflecting the *link*, calls us with
		// d.IsDir()==false on most platforms (the entry's type bits are
		// the LINK bits, not the target). We still defensively check
		// fi.Mode() for ModeSymlink and route via the symlink path.
		if mode&os.ModeSymlink != 0 {
			if opts.matcher.Match(rel) {
				return nil
			}
			pending = append(pending, candidate{rel: rel, full: path, fi: fi})
			return nil
		}

		// Directory pruning: nested .git, submodules, and ACD state subdir.
		if d.IsDir() {
			// nested-repo/submodule detection: skip when <dir>/.git exists.
			if _, err := os.Stat(filepath.Join(path, ".git")); err == nil {
				return fs.SkipDir
			}
			if opts.submodules != nil && opts.submodules[rel] {
				return fs.SkipDir
			}
			return nil
		}

		// Regular files only — sockets/FIFOs/devices skipped quietly.
		if !mode.IsRegular() {
			return nil
		}

		if opts.matcher.Match(rel) {
			return nil
		}
		pending = append(pending, candidate{rel: rel, full: path, fi: fi})
		return nil
	})
	if walkErr != nil {
		// ctx cancellation is the only walkErr we surface as fatal.
		if errors.Is(walkErr, context.Canceled) || errors.Is(walkErr, context.DeadlineExceeded) {
			return nil, summary, walkErr
		}
		// Non-fatal: log via summary.Errors and proceed with whatever we
		// collected so far.
		summary.Errors++
	}

	// Batched ignore check (one git subprocess per pass, not per file).
	ignored := map[string]bool{}
	if opts.ignoreChecker != nil && len(pending) > 0 {
		paths := make([]string, len(pending))
		for i, c := range pending {
			paths[i] = c.rel
		}
		results, ierr := opts.ignoreChecker.Check(ctx, paths)
		if ierr != nil {
			// Fail-closed: if check-ignore is busted, abort the pass rather
			// than silently committing files git considers ignored.
			return nil, summary, fmt.Errorf("daemon: check-ignore: %w", ierr)
		}
		for i, isIgn := range results {
			if isIgn {
				ignored[pending[i].rel] = true
			}
		}
	}

	for _, c := range pending {
		if err := ctx.Err(); err != nil {
			return nil, summary, err
		}
		summary.WalkedFiles++
		if ignored[c.rel] {
			continue
		}
		entry, ok, err := hashCandidate(ctx, repoRoot, c, opts)
		if err != nil {
			summary.Errors++
			continue
		}
		if !ok {
			summary.Oversize++
			continue
		}
		live[c.rel] = entry
	}

	return live, summary, nil
}

// hashCandidate hashes one candidate path into the git object store. For
// symlinks: read target bytes, hash with mode 120000. For regulars: open
// O_NOFOLLOW, verify ino+dev+mode unchanged across the open, enforce the
// size cap (recording oversize via daemon_meta), then hash via stdin.
//
// Returns:
//   - (entry, true,  nil) — captured ok.
//   - (zero,  false, nil) — skipped (oversize, vanished, type changed).
//   - (zero,  _,     err) — hard error worth recording in summary.
func hashCandidate(ctx context.Context, repoRoot string, c candidateLike, opts walkOpts) (LiveEntry, bool, error) {
	mode := c.fi.Mode()
	if mode&os.ModeSymlink != 0 {
		target, rerr := os.Readlink(c.full)
		if rerr != nil {
			return LiveEntry{}, false, rerr
		}
		oid, _, herr := git.HashSymlinkBlob(ctx, repoRoot, target)
		if herr != nil {
			return LiveEntry{}, false, herr
		}
		return LiveEntry{Path: c.rel, Mode: git.SymlinkMode, OID: oid}, true, nil
	}

	// Regular file: O_NOFOLLOW + verify ino/dev/mode (TOCTOU defense).
	flags := os.O_RDONLY | syscall.O_NOFOLLOW
	f, err := os.OpenFile(c.full, flags, 0)
	if err != nil {
		return LiveEntry{}, false, err
	}
	defer f.Close()

	post, err := f.Stat()
	if err != nil {
		return LiveEntry{}, false, err
	}
	if !sameFile(c.fi, post) {
		// Swapped between lstat and open — discard.
		return LiveEntry{}, false, nil
	}
	if !post.Mode().IsRegular() {
		return LiveEntry{}, false, nil
	}
	if post.Size() > opts.maxBytes {
		recordOversize(ctx, opts.db, c.rel, post.Size(), opts.maxBytes)
		return LiveEntry{}, false, nil
	}
	// Read up to maxBytes+1 to detect truncation/grow during read; if we
	// exceed, record oversize and discard.
	buf, err := io.ReadAll(f)
	if err != nil {
		return LiveEntry{}, false, err
	}
	if int64(len(buf)) > opts.maxBytes {
		recordOversize(ctx, opts.db, c.rel, int64(len(buf)), opts.maxBytes)
		return LiveEntry{}, false, nil
	}
	oid, herr := git.HashObjectStdin(ctx, repoRoot, buf)
	if herr != nil {
		return LiveEntry{}, false, herr
	}
	return LiveEntry{
		Path: c.rel,
		Mode: gitModeFor(post.Mode()),
		OID:  oid,
	}, true, nil
}

// candidateLike is the minimal shape hashCandidate needs. Aliasing the
// closure-captured candidate type keeps walkLive's pending slice unboxed.
type candidateLike = struct {
	rel  string
	full string
	fi   os.FileInfo
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

// gitModeFor maps a Go fs.Mode onto a git tree mode for regular files.
// Symlinks are handled separately via SymlinkMode.
func gitModeFor(m os.FileMode) string {
	if m&0o111 != 0 {
		return git.ExecutableFileMode
	}
	return git.RegularFileMode
}

// recordOversize stores a daemon_meta breadcrumb so operators can see why a
// path was skipped without having to grep the daemon log. Best-effort:
// errors are dropped because the capture pipeline must keep running.
func recordOversize(ctx context.Context, db *state.DB, rel string, size, cap int64) {
	if db == nil {
		return
	}
	key := "capture-skip-large:" + rel
	val := fmt.Sprintf("size=%d>cap=%d", size, cap)
	_ = state.MetaSet(ctx, db, key, val)
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
