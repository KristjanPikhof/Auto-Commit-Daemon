package daemon

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	checkpointpkg "github.com/KristjanPikhof/Auto-Commit-Daemon/internal/checkpoint"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	pausepkg "github.com/KristjanPikhof/Auto-Commit-Daemon/internal/pause"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

// captureFixture is a tiny helper: makes a temp git repo with an initial
// commit so HEAD resolves, opens the per-repo state DB, and returns
// everything the test needs.
type captureFixture struct {
	dir     string
	gitDir  string
	db      *state.DB
	cctx    CaptureContext
	ig      *git.IgnoreChecker
	matcher *state.SensitiveMatcher
}

func newCaptureFixture(t *testing.T) *captureFixture {
	t.Helper()
	repo := cloneDaemonTestRepo(t, captureRepoTemplate)

	ig := git.NewIgnoreChecker(repo.dir)
	t.Cleanup(func() { _ = ig.Close() })

	return &captureFixture{
		dir:    repo.dir,
		gitDir: repo.gitDir,
		db:     repo.db,
		cctx: CaptureContext{
			BranchRef:        "refs/heads/main",
			BranchGeneration: 1,
			BaseHead:         repo.head,
		},
		ig:      ig,
		matcher: state.NewSensitiveMatcher(),
	}
}

// firstCapture seeds shadow_paths from a fresh capture, treating everything
// already on disk as "the baseline". This mirrors what bootstrap_shadow does
// in the legacy daemon for the first poll on a clean worktree.
func (f *captureFixture) firstCapture(t *testing.T) CaptureSummary {
	t.Helper()
	ctx := context.Background()
	sum, err := Capture(ctx, f.dir, f.db, f.cctx, CaptureOpts{
		IgnoreChecker:    f.ig,
		SensitiveMatcher: f.matcher,
	})
	if err != nil {
		t.Fatalf("first Capture: %v", err)
	}
	return sum
}

// pendingOps returns the (op, path) pairs from capture_events for assertions.
func pendingOps(t *testing.T, db *state.DB) []struct{ Op, Path string } {
	t.Helper()
	rows, err := db.SQL().QueryContext(context.Background(),
		`SELECT operation, path FROM capture_events ORDER BY seq ASC`)
	if err != nil {
		t.Fatalf("query events: %v", err)
	}
	defer rows.Close()
	var out []struct{ Op, Path string }
	for rows.Next() {
		var op, p string
		if err := rows.Scan(&op, &p); err != nil {
			t.Fatalf("scan: %v", err)
		}
		out = append(out, struct{ Op, Path string }{op, p})
	}
	return out
}

func assertProtectedDecision(t *testing.T, db *state.DB, path, reason string) {
	t.Helper()
	decisions, err := state.DecisionsForPath(context.Background(), db, path, 10)
	if err != nil {
		t.Fatalf("DecisionsForPath: %v", err)
	}
	for _, decision := range decisions {
		if decision.Kind == state.DecisionKindProtected &&
			decision.Reason.Valid &&
			decision.Reason.String == reason &&
			decision.ActionTaken.Valid &&
			decision.ActionTaken.String == "no_delete_generated" {
			return
		}
	}
	t.Fatalf("missing protected decision path=%q reason=%q: %+v", path, reason, decisions)
}

func assertNoProtectedDecision(t *testing.T, db *state.DB, path, reason string) {
	t.Helper()
	decisions, err := state.DecisionsForPath(context.Background(), db, path, 10)
	if err != nil {
		t.Fatalf("DecisionsForPath: %v", err)
	}
	for _, decision := range decisions {
		if decision.Kind == state.DecisionKindProtected &&
			decision.Reason.Valid &&
			decision.Reason.String == reason &&
			decision.ActionTaken.Valid &&
			decision.ActionTaken.String == "no_delete_generated" {
			t.Fatalf("unexpected protected decision path=%q reason=%q: %+v", path, reason, decisions)
		}
	}
}

// TestCapture_SymlinkDirNotRecursed: the legacy regression. A symlink to a
// directory must capture as mode 120000 with no descent into the link
// target. The contained file MUST NOT appear in capture_events.
func TestCapture_SymlinkDirNotRecursed(t *testing.T) {
	f := newCaptureFixture(t)

	// Outside-repo target with a file inside.
	target := t.TempDir()
	if err := os.WriteFile(filepath.Join(target, "buried.txt"), []byte("nope"), 0o644); err != nil {
		t.Fatalf("write target: %v", err)
	}
	link := filepath.Join(f.dir, "linkdir")
	if err := os.Symlink(target, link); err != nil {
		t.Fatalf("symlink: %v", err)
	}

	// First capture seeds shadow including the new symlink as a 120000 entry.
	f.firstCapture(t)

	ops := pendingOps(t, f.db)
	var sawLink bool
	for _, op := range ops {
		if strings.Contains(op.Path, "buried.txt") {
			t.Fatalf("captured file inside symlinked dir: %+v (full=%+v)", op, ops)
		}
		if op.Path == "linkdir" {
			sawLink = true
			if op.Op != "create" {
				t.Fatalf("expected create on linkdir, got op=%s", op.Op)
			}
		}
	}
	if !sawLink {
		t.Fatalf("expected create event for symlink 'linkdir', got %+v", ops)
	}

	// Verify the persisted shadow row carries mode 120000.
	rows, err := f.db.SQL().QueryContext(context.Background(),
		`SELECT mode FROM shadow_paths WHERE path = ?`, "linkdir")
	if err != nil {
		t.Fatalf("query shadow: %v", err)
	}
	defer rows.Close()
	if !rows.Next() {
		t.Fatalf("no shadow row for linkdir")
	}
	var mode string
	_ = rows.Scan(&mode)
	if mode != git.SymlinkMode {
		t.Fatalf("symlink shadow mode = %q, want %q", mode, git.SymlinkMode)
	}
}

// TestCapture_SensitiveDefaultDeny: a .env file is skipped without env
// override. The default sensitive globs include `.env` at the root.
func TestCapture_SensitiveDefaultDeny(t *testing.T) {
	t.Setenv(state.EnvSensitiveGlobs, "") // explicit empty -> defaults
	f := newCaptureFixture(t)
	f.matcher = state.NewSensitiveMatcher()

	if err := os.WriteFile(filepath.Join(f.dir, ".env"), []byte("SECRET=1\n"), 0o600); err != nil {
		t.Fatalf("write env: %v", err)
	}
	if err := os.WriteFile(filepath.Join(f.dir, "fine.txt"), []byte("ok\n"), 0o644); err != nil {
		t.Fatalf("write fine: %v", err)
	}

	f.firstCapture(t)
	ops := pendingOps(t, f.db)
	for _, op := range ops {
		if op.Path == ".env" {
			t.Fatalf("sensitive .env captured: %+v", ops)
		}
	}
	var sawFine bool
	for _, op := range ops {
		if op.Path == "fine.txt" {
			sawFine = true
		}
	}
	if !sawFine {
		t.Fatalf("expected fine.txt to be captured, got %+v", ops)
	}
}

func TestCapture_TrackedEnvExampleIsNotPhantomDelete(t *testing.T) {
	t.Setenv(state.EnvSensitiveGlobs, "") // explicit empty -> defaults
	f := newCaptureFixture(t)
	f.matcher = state.NewSensitiveMatcher()
	ctx := context.Background()

	envPath := filepath.Join(f.dir, "apps", "server", ".env.example")
	if err := os.MkdirAll(filepath.Dir(envPath), 0o755); err != nil {
		t.Fatalf("mkdir env dir: %v", err)
	}
	if err := os.WriteFile(envPath, []byte("API_URL=http://localhost:3000\n"), 0o644); err != nil {
		t.Fatalf("write env example: %v", err)
	}
	if _, err := git.Run(ctx, git.RunOpts{Dir: f.dir}, "add", "apps/server/.env.example"); err != nil {
		t.Fatalf("git add env example: %v", err)
	}
	if _, err := git.Run(ctx, git.RunOpts{Dir: f.dir}, "commit", "-q", "-m", "add env example"); err != nil {
		t.Fatalf("git commit env example: %v", err)
	}
	head, err := git.RevParse(ctx, f.dir, "HEAD")
	if err != nil {
		t.Fatalf("rev-parse HEAD: %v", err)
	}
	f.cctx.BaseHead = head
	if _, err := BootstrapShadow(ctx, f.dir, f.db, f.cctx); err != nil {
		t.Fatalf("bootstrap shadow: %v", err)
	}

	if _, err := Capture(ctx, f.dir, f.db, f.cctx, CaptureOpts{
		IgnoreChecker:    f.ig,
		SensitiveMatcher: f.matcher,
	}); err != nil {
		t.Fatalf("capture: %v", err)
	}

	if ops := pendingOps(t, f.db); len(ops) != 0 {
		t.Fatalf("tracked .env.example should not be captured as a change, got %+v", ops)
	}
}

func TestCapture_TrackedSensitivePresentIsProtectedFromDelete(t *testing.T) {
	t.Setenv(state.EnvSensitiveGlobs, "") // explicit empty -> defaults
	f := newCaptureFixture(t)
	f.matcher = state.NewSensitiveMatcher()
	ctx := context.Background()

	envPath := filepath.Join(f.dir, ".env")
	if err := os.WriteFile(envPath, []byte("SECRET=1\n"), 0o600); err != nil {
		t.Fatalf("write env: %v", err)
	}
	if _, err := git.Run(ctx, git.RunOpts{Dir: f.dir}, "add", ".env"); err != nil {
		t.Fatalf("git add env: %v", err)
	}
	if _, err := git.Run(ctx, git.RunOpts{Dir: f.dir}, "commit", "-q", "-m", "track env"); err != nil {
		t.Fatalf("git commit env: %v", err)
	}
	head, err := git.RevParse(ctx, f.dir, "HEAD")
	if err != nil {
		t.Fatalf("rev-parse HEAD: %v", err)
	}
	f.cctx.BaseHead = head
	if _, err := BootstrapShadow(ctx, f.dir, f.db, f.cctx); err != nil {
		t.Fatalf("bootstrap shadow: %v", err)
	}

	if _, err := Capture(ctx, f.dir, f.db, f.cctx, CaptureOpts{
		IgnoreChecker:    f.ig,
		SensitiveMatcher: f.matcher,
	}); err != nil {
		t.Fatalf("capture: %v", err)
	}
	if ops := pendingOps(t, f.db); len(ops) != 0 {
		t.Fatalf("tracked present .env should be protected, got ops %+v", ops)
	}
	assertProtectedDecision(t, f.db, ".env", "sensitive")
}

func TestCapture_TrackedSafeIgnorePresentIsProtectedFromDelete(t *testing.T) {
	f := newCaptureFixture(t)
	ctx := context.Background()

	tracked := filepath.Join(f.dir, "node_modules", "pkg", "index.js")
	if err := os.MkdirAll(filepath.Dir(tracked), 0o755); err != nil {
		t.Fatalf("mkdir tracked: %v", err)
	}
	if err := os.WriteFile(tracked, []byte("module.exports = 1\n"), 0o644); err != nil {
		t.Fatalf("write tracked: %v", err)
	}
	if _, err := git.Run(ctx, git.RunOpts{Dir: f.dir}, "add", "-f", "node_modules/pkg/index.js"); err != nil {
		t.Fatalf("git add tracked safe-ignore: %v", err)
	}
	if _, err := git.Run(ctx, git.RunOpts{Dir: f.dir}, "commit", "-q", "-m", "track generated file"); err != nil {
		t.Fatalf("git commit tracked safe-ignore: %v", err)
	}
	head, err := git.RevParse(ctx, f.dir, "HEAD")
	if err != nil {
		t.Fatalf("rev-parse HEAD: %v", err)
	}
	f.cctx.BaseHead = head
	if _, err := BootstrapShadow(ctx, f.dir, f.db, f.cctx); err != nil {
		t.Fatalf("bootstrap shadow: %v", err)
	}

	if _, err := Capture(ctx, f.dir, f.db, f.cctx, CaptureOpts{
		IgnoreChecker:    f.ig,
		SensitiveMatcher: f.matcher,
	}); err != nil {
		t.Fatalf("capture: %v", err)
	}
	if ops := pendingOps(t, f.db); len(ops) != 0 {
		t.Fatalf("tracked present safe-ignore file should be protected, got ops %+v", ops)
	}
	assertProtectedDecision(t, f.db, "node_modules/pkg/index.js", "safe_ignore")
}

func TestCapture_TrackedSafeIgnoreDeletedChildIsProtectedFromDelete(t *testing.T) {
	f := newCaptureFixture(t)
	ctx := context.Background()

	trackedRel := "node_modules/pkg/index.js"
	tracked := filepath.Join(f.dir, filepath.FromSlash(trackedRel))
	if err := os.MkdirAll(filepath.Dir(tracked), 0o755); err != nil {
		t.Fatalf("mkdir tracked: %v", err)
	}
	if err := os.WriteFile(tracked, []byte("module.exports = 1\n"), 0o644); err != nil {
		t.Fatalf("write tracked: %v", err)
	}
	if _, err := git.Run(ctx, git.RunOpts{Dir: f.dir}, "add", "-f", trackedRel); err != nil {
		t.Fatalf("git add tracked safe-ignore: %v", err)
	}
	if _, err := git.Run(ctx, git.RunOpts{Dir: f.dir}, "commit", "-q", "-m", "track generated file"); err != nil {
		t.Fatalf("git commit tracked safe-ignore: %v", err)
	}
	head, err := git.RevParse(ctx, f.dir, "HEAD")
	if err != nil {
		t.Fatalf("rev-parse HEAD: %v", err)
	}
	f.cctx.BaseHead = head
	if _, err := BootstrapShadow(ctx, f.dir, f.db, f.cctx); err != nil {
		t.Fatalf("bootstrap shadow: %v", err)
	}
	if err := os.Remove(tracked); err != nil {
		t.Fatalf("remove tracked: %v", err)
	}

	if _, err := Capture(ctx, f.dir, f.db, f.cctx, CaptureOpts{
		IgnoreChecker:    f.ig,
		SensitiveMatcher: f.matcher,
	}); err != nil {
		t.Fatalf("capture: %v", err)
	}

	ops := pendingOps(t, f.db)
	if len(ops) != 0 {
		t.Fatalf("deleted child under safe-ignore dir should be protected, got ops=%+v", ops)
	}
	assertProtectedDecision(t, f.db, trackedRel, "safe_ignore")

	if _, err := Capture(ctx, f.dir, f.db, f.cctx, CaptureOpts{
		IgnoreChecker:    f.ig,
		SensitiveMatcher: f.matcher,
	}); err != nil {
		t.Fatalf("second capture: %v", err)
	}
	if ops := pendingOps(t, f.db); len(ops) != 0 {
		t.Fatalf("safe-ignore delete recaptured on second pass: %+v", ops)
	}
	var shadowRows int
	if err := f.db.SQL().QueryRowContext(ctx,
		`SELECT COUNT(*) FROM shadow_paths WHERE branch_ref = ? AND branch_generation = ? AND path = ?`,
		f.cctx.BranchRef, f.cctx.BranchGeneration, trackedRel,
	).Scan(&shadowRows); err != nil {
		t.Fatalf("count shadow row: %v", err)
	}
	if shadowRows != 0 {
		t.Fatalf("shadow row for protected deleted safe-ignore path remains: %d", shadowRows)
	}
}

func TestCapture_TrackedGitignoredDeletedChildEmitsDelete(t *testing.T) {
	f := newCaptureFixture(t)
	ctx := context.Background()

	gitignore := filepath.Join(f.dir, ".gitignore")
	if err := os.WriteFile(gitignore, []byte("ignored.txt\nbuild/\n"), 0o644); err != nil {
		t.Fatalf("rewrite .gitignore: %v", err)
	}
	trackedRel := "build/keep.txt"
	tracked := filepath.Join(f.dir, filepath.FromSlash(trackedRel))
	if err := os.MkdirAll(filepath.Dir(tracked), 0o755); err != nil {
		t.Fatalf("mkdir tracked: %v", err)
	}
	if err := os.WriteFile(tracked, []byte("tracked despite ignore\n"), 0o644); err != nil {
		t.Fatalf("write tracked: %v", err)
	}
	if _, err := git.Run(ctx, git.RunOpts{Dir: f.dir}, "add", ".gitignore"); err != nil {
		t.Fatalf("git add .gitignore: %v", err)
	}
	if _, err := git.Run(ctx, git.RunOpts{Dir: f.dir}, "add", "-f", trackedRel); err != nil {
		t.Fatalf("git add tracked ignored child: %v", err)
	}
	if _, err := git.Run(ctx, git.RunOpts{Dir: f.dir}, "commit", "-q", "-m", "track ignored child"); err != nil {
		t.Fatalf("git commit tracked ignored child: %v", err)
	}
	f.ig.Invalidate()
	head, err := git.RevParse(ctx, f.dir, "HEAD")
	if err != nil {
		t.Fatalf("rev-parse HEAD: %v", err)
	}
	f.cctx.BaseHead = head
	if _, err := BootstrapShadow(ctx, f.dir, f.db, f.cctx); err != nil {
		t.Fatalf("bootstrap shadow: %v", err)
	}
	if err := os.Remove(tracked); err != nil {
		t.Fatalf("remove tracked: %v", err)
	}

	if _, err := Capture(ctx, f.dir, f.db, f.cctx, CaptureOpts{
		IgnoreChecker:    f.ig,
		SensitiveMatcher: f.matcher,
	}); err != nil {
		t.Fatalf("capture: %v", err)
	}

	ops := pendingOps(t, f.db)
	if len(ops) != 1 || ops[0].Op != "delete" || ops[0].Path != trackedRel {
		t.Fatalf("deleted child under gitignored dir ops=%+v, want one delete", ops)
	}
	assertNoProtectedDecision(t, f.db, trackedRel, "gitignore")
}

func TestCapture_TrackedAbsentFileStillDeletes(t *testing.T) {
	f := newCaptureFixture(t)
	ctx := context.Background()

	tracked := filepath.Join(f.dir, "tracked.txt")
	if err := os.WriteFile(tracked, []byte("tracked\n"), 0o644); err != nil {
		t.Fatalf("write tracked: %v", err)
	}
	if _, err := git.Run(ctx, git.RunOpts{Dir: f.dir}, "add", "tracked.txt"); err != nil {
		t.Fatalf("git add tracked: %v", err)
	}
	if _, err := git.Run(ctx, git.RunOpts{Dir: f.dir}, "commit", "-q", "-m", "track file"); err != nil {
		t.Fatalf("git commit tracked: %v", err)
	}
	head, err := git.RevParse(ctx, f.dir, "HEAD")
	if err != nil {
		t.Fatalf("rev-parse HEAD: %v", err)
	}
	f.cctx.BaseHead = head
	if _, err := BootstrapShadow(ctx, f.dir, f.db, f.cctx); err != nil {
		t.Fatalf("bootstrap shadow: %v", err)
	}
	if err := os.Remove(tracked); err != nil {
		t.Fatalf("remove tracked: %v", err)
	}

	if _, err := Capture(ctx, f.dir, f.db, f.cctx, CaptureOpts{
		IgnoreChecker:    f.ig,
		SensitiveMatcher: f.matcher,
	}); err != nil {
		t.Fatalf("capture: %v", err)
	}
	ops := pendingOps(t, f.db)
	if len(ops) != 1 || ops[0].Op != "delete" || ops[0].Path != "tracked.txt" {
		t.Fatalf("absent tracked file ops=%+v, want one delete", ops)
	}
}

func TestCapture_SafeIgnoreDefaultPrunesGeneratedTrees(t *testing.T) {
	t.Setenv(state.EnvSafeIgnore, "")
	t.Setenv(state.EnvSafeIgnoreExtra, "")
	f := newCaptureFixture(t)

	if err := os.WriteFile(filepath.Join(f.dir, "fine.txt"), []byte("ok\n"), 0o644); err != nil {
		t.Fatalf("write fine: %v", err)
	}
	for _, root := range []string{"node_modules", "target", "DerivedData", ".derivedData-provider-core"} {
		for i := 0; i < 16; i++ {
			dir := filepath.Join(f.dir, root, fmt.Sprintf("pkg-%02d", i))
			if err := os.MkdirAll(dir, 0o755); err != nil {
				t.Fatalf("mkdir %s: %v", dir, err)
			}
			for j := 0; j < 16; j++ {
				leaf := filepath.Join(dir, fmt.Sprintf("leaf-%02d.txt", j))
				if err := os.WriteFile(leaf, []byte("generated"), 0o644); err != nil {
					t.Fatalf("write %s: %v", leaf, err)
				}
			}
		}
	}

	sum, err := Capture(context.Background(), f.dir, f.db, f.cctx, CaptureOpts{
		IgnoreChecker:     f.ig,
		SensitiveMatcher:  f.matcher,
		SafeIgnoreMatcher: state.NewSafeIgnoreMatcher(),
	})
	if err != nil {
		t.Fatalf("Capture: %v", err)
	}
	if sum.WalkedFiles >= 100 {
		t.Fatalf("WalkedFiles=%d; generated trees should be pruned before descendants are read", sum.WalkedFiles)
	}

	var sawFine bool
	for _, op := range pendingOps(t, f.db) {
		if strings.HasPrefix(op.Path, "node_modules/") ||
			strings.HasPrefix(op.Path, "target/") ||
			strings.HasPrefix(op.Path, "DerivedData/") ||
			strings.HasPrefix(op.Path, ".derivedData-provider-core/") {
			t.Fatalf("safe-ignore generated path captured: %+v", op)
		}
		if op.Path == "fine.txt" {
			sawFine = true
		}
	}
	if !sawFine {
		t.Fatalf("expected fine.txt to be captured, got %+v", pendingOps(t, f.db))
	}
}

func TestCapture_SafeIgnoreDisableRestoresGeneratedTreeCapture(t *testing.T) {
	t.Setenv(state.EnvSafeIgnore, "0")
	t.Setenv(state.EnvSafeIgnoreExtra, "")
	f := newCaptureFixture(t)

	generated := filepath.Join(f.dir, "node_modules", "pkg", "index.js")
	if err := os.MkdirAll(filepath.Dir(generated), 0o755); err != nil {
		t.Fatalf("mkdir generated: %v", err)
	}
	if err := os.WriteFile(generated, []byte("module.exports = 1\n"), 0o644); err != nil {
		t.Fatalf("write generated: %v", err)
	}

	_, err := Capture(context.Background(), f.dir, f.db, f.cctx, CaptureOpts{
		IgnoreChecker:     f.ig,
		SensitiveMatcher:  f.matcher,
		SafeIgnoreMatcher: state.NewSafeIgnoreMatcher(),
	})
	if err != nil {
		t.Fatalf("Capture: %v", err)
	}

	for _, op := range pendingOps(t, f.db) {
		if op.Path == "node_modules/pkg/index.js" {
			return
		}
	}
	t.Fatalf("expected generated file to be captured when %s=0; got %+v", state.EnvSafeIgnore, pendingOps(t, f.db))
}

func TestCapture_SafeIgnoreDirectoryPatternKeepsSameNamedFile(t *testing.T) {
	t.Setenv(state.EnvSafeIgnore, "")
	t.Setenv(state.EnvSafeIgnoreExtra, "")
	f := newCaptureFixture(t)

	if err := os.WriteFile(filepath.Join(f.dir, "target"), []byte("real file\n"), 0o644); err != nil {
		t.Fatalf("write target file: %v", err)
	}

	_, err := Capture(context.Background(), f.dir, f.db, f.cctx, CaptureOpts{
		IgnoreChecker:     f.ig,
		SensitiveMatcher:  f.matcher,
		SafeIgnoreMatcher: state.NewSafeIgnoreMatcher(),
	})
	if err != nil {
		t.Fatalf("Capture: %v", err)
	}

	for _, op := range pendingOps(t, f.db) {
		if op.Path == "target" {
			return
		}
	}
	t.Fatalf("expected same-named target file to be captured, got %+v", pendingOps(t, f.db))
}

// TestCapture_OversizeMetaOnly: a file > MaxFileBytes records a daemon_meta
// row and produces NO commit-event.
func TestCapture_OversizeMetaOnly(t *testing.T) {
	f := newCaptureFixture(t)

	// 4kB file with a 2kB cap.
	big := make([]byte, 4096)
	for i := range big {
		big[i] = 'a'
	}
	if err := os.WriteFile(filepath.Join(f.dir, "big.bin"), big, 0o644); err != nil {
		t.Fatalf("write big: %v", err)
	}

	sum, err := Capture(context.Background(), f.dir, f.db, f.cctx, CaptureOpts{
		IgnoreChecker:    f.ig,
		SensitiveMatcher: f.matcher,
		MaxFileBytes:     2048,
	})
	if err != nil {
		t.Fatalf("Capture: %v", err)
	}
	if sum.Oversize != 1 {
		t.Fatalf("oversize=%d want 1", sum.Oversize)
	}

	for _, op := range pendingOps(t, f.db) {
		if op.Path == "big.bin" {
			t.Fatalf("big.bin should not have produced a capture event: %+v", op)
		}
	}

	val, ok, err := state.MetaGet(context.Background(), f.db, "capture-skip-large:big.bin")
	if err != nil {
		t.Fatalf("MetaGet: %v", err)
	}
	if !ok {
		t.Fatalf("expected capture-skip-large daemon_meta row")
	}
	if !strings.Contains(val, "size=4096") || !strings.Contains(val, "cap=2048") {
		t.Fatalf("oversize meta value=%q, want size=4096>cap=2048", val)
	}
}

func TestScanProtectedEntries_ReusesExactIndexedOversizeBlob(t *testing.T) {
	f := newCaptureFixture(t)
	path := "tracked-large.bin"
	body := bytes.Repeat([]byte("a"), 4096)
	if err := os.WriteFile(filepath.Join(f.dir, path), body, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := git.Run(context.Background(), git.RunOpts{Dir: f.dir}, "add", "--", path); err != nil {
		t.Fatal(err)
	}
	indexed, err := git.LsFilesStaged(context.Background(), f.dir, path)
	if err != nil || len(indexed) != 1 {
		t.Fatalf("index entries=(%+v,%v)", indexed, err)
	}

	entries, _, summary, err := ScanProtectedEntries(context.Background(), f.dir, CaptureOpts{
		IgnoreChecker: f.ig,
		MaxFileBytes:  2048,
	})
	if err != nil {
		t.Fatalf("ScanProtectedEntries: %v", err)
	}
	if summary.Oversize != 0 {
		t.Fatalf("oversize=%d want 0", summary.Oversize)
	}
	for _, entry := range entries {
		if entry.Path == path {
			if entry.OID != indexed[0].OID || entry.Mode != indexed[0].Mode {
				t.Fatalf("entry=%+v index=%+v", entry, indexed[0])
			}
			return
		}
	}
	t.Fatalf("missing %s in entries: %+v", path, entries)
}

func TestScanProtectedEntries_RejectsDirtyIndexedOversizeBlob(t *testing.T) {
	f := newCaptureFixture(t)
	path := "tracked-large.bin"
	if err := os.WriteFile(filepath.Join(f.dir, path), bytes.Repeat([]byte("a"), 4096), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := git.Run(context.Background(), git.RunOpts{Dir: f.dir}, "add", "--", path); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(f.dir, path), bytes.Repeat([]byte("b"), 4096), 0o644); err != nil {
		t.Fatal(err)
	}

	_, _, summary, err := ScanProtectedEntries(context.Background(), f.dir, CaptureOpts{
		IgnoreChecker: f.ig,
		MaxFileBytes:  2048,
	})
	if err == nil || summary.Oversize != 1 {
		t.Fatalf("err=%v oversize=%d, want incomplete scan with one oversize", err, summary.Oversize)
	}
}

func TestCapture_SkipsPathWithControlCharacters(t *testing.T) {
	f := newCaptureFixture(t)
	name := "bad\tname.txt"
	if err := os.WriteFile(filepath.Join(f.dir, name), []byte("secret-ish"), 0o644); err != nil {
		t.Fatalf("write control-char path: %v", err)
	}
	if err := os.WriteFile(filepath.Join(f.dir, "fine.txt"), []byte("ok"), 0o644); err != nil {
		t.Fatalf("write fine: %v", err)
	}

	sum, err := Capture(context.Background(), f.dir, f.db, f.cctx, CaptureOpts{
		IgnoreChecker:    f.ig,
		SensitiveMatcher: f.matcher,
	})
	if err != nil {
		t.Fatalf("Capture: %v", err)
	}
	if sum.Errors == 0 {
		t.Fatalf("expected control-char path to be counted as a soft error")
	}
	for _, op := range pendingOps(t, f.db) {
		if op.Path == name {
			t.Fatalf("control-char path should not have produced a capture event: %+v", op)
		}
	}
	if _, ok, err := state.MetaGet(context.Background(), f.db, "capture-skip-invalid-path:bad\\tname.txt"); err != nil {
		t.Fatalf("MetaGet invalid path: %v", err)
	} else if !ok {
		t.Fatalf("expected capture-skip-invalid-path daemon_meta row")
	}
}

// TestCapture_RoundTrip: walk twice, the second walk emits the right diff.
//
//	pass 1 (first walk):  create foo.txt, modify .gitignore? no — fresh capture
//	pass 2: modify foo.txt, create bar.txt, delete .gitignore
func TestCapture_RoundTrip(t *testing.T) {
	f := newCaptureFixture(t)

	// First state: foo.txt exists (will be created in shadow on first capture).
	if err := os.WriteFile(filepath.Join(f.dir, "foo.txt"), []byte("v1"), 0o644); err != nil {
		t.Fatalf("write foo: %v", err)
	}

	// First capture seeds shadow with foo.txt + .gitignore as creates.
	f.firstCapture(t)

	first := pendingOps(t, f.db)
	wantCreate := map[string]bool{".gitignore": true, "foo.txt": true}
	for _, op := range first {
		if op.Op != "create" {
			t.Fatalf("first pass non-create: %+v", op)
		}
		delete(wantCreate, op.Path)
	}
	if len(wantCreate) != 0 {
		t.Fatalf("first pass missing creates: %v (got %+v)", wantCreate, first)
	}

	// Mutate: modify foo.txt, create bar.txt, delete .gitignore.
	if err := os.WriteFile(filepath.Join(f.dir, "foo.txt"), []byte("v2 changed"), 0o644); err != nil {
		t.Fatalf("write foo v2: %v", err)
	}
	if err := os.WriteFile(filepath.Join(f.dir, "bar.txt"), []byte("hi"), 0o644); err != nil {
		t.Fatalf("write bar: %v", err)
	}
	if err := os.Remove(filepath.Join(f.dir, ".gitignore")); err != nil {
		t.Fatalf("remove .gitignore: %v", err)
	}

	// Second capture; rebuild matcher so env defaults stay stable.
	if _, err := Capture(context.Background(), f.dir, f.db, f.cctx, CaptureOpts{
		IgnoreChecker:    f.ig,
		SensitiveMatcher: f.matcher,
	}); err != nil {
		t.Fatalf("second Capture: %v", err)
	}

	all := pendingOps(t, f.db)
	// Subtract first-pass events.
	second := all[len(first):]
	gotMap := make(map[string]string)
	for _, op := range second {
		gotMap[op.Path] = op.Op
	}
	want := map[string]string{
		"foo.txt":    "modify",
		"bar.txt":    "create",
		".gitignore": "delete",
	}
	for k, v := range want {
		if gotMap[k] != v {
			t.Fatalf("path %q: got op %q, want %q (all=%+v)", k, gotMap[k], v, second)
		}
	}
}

// TestCapture_ModeChange: a chmod from 644 to 755 produces a `mode` event.
// Skipped on Windows-style filesystems that don't honor exec bit, but the
// project targets unix-only platforms.
func TestCapture_ModeChange(t *testing.T) {
	f := newCaptureFixture(t)
	p := filepath.Join(f.dir, "script.sh")
	if err := os.WriteFile(p, []byte("#!/bin/sh\necho hi\n"), 0o644); err != nil {
		t.Fatalf("write script: %v", err)
	}
	f.firstCapture(t)
	if err := os.Chmod(p, 0o755); err != nil {
		t.Fatalf("chmod: %v", err)
	}
	if _, err := Capture(context.Background(), f.dir, f.db, f.cctx, CaptureOpts{
		IgnoreChecker:    f.ig,
		SensitiveMatcher: f.matcher,
	}); err != nil {
		t.Fatalf("second Capture: %v", err)
	}
	// Expect at least one `mode` event for script.sh.
	var sawMode bool
	for _, op := range pendingOps(t, f.db) {
		if op.Path == "script.sh" && op.Op == "mode" {
			sawMode = true
		}
	}
	if !sawMode {
		t.Fatalf("expected mode event for script.sh, got %+v", pendingOps(t, f.db))
	}
}

// TestCapture_PendingDepthCap_DropsNewEvents verifies the new durable
// backpressure contract. Pass A: drive pending up to the cap, observe
// mid-pass entry into backpressure (the in-loop saturation guard that
// stamps MetaKeyCaptureBackpressurePausedAt and stops the pass). Pass B:
// while saturated, an additional capture pass MUST early-return BEFORE
// walkLive runs — `WalkedFiles` stays 0 and `BackpressurePaused` is true.
// Pass C: after replay drains the queue below the high-water mark, the
// next capture pass clears the gate and emits a `capture.pause cleared`
// trace event.
func TestCapture_PendingDepthCap_DropsNewEvents(t *testing.T) {
	t.Setenv(EnvMaxPendingEvents, "10")
	resetPendingCapWarnForTest(t, 1) // 1-second interval; we only care that *one* warn lands

	// Capture warn output via a buffer-backed slog handler. Restore the
	// process default on cleanup so we don't bleed state into other tests.
	prevDefault := slog.Default()
	t.Cleanup(func() { slog.SetDefault(prevDefault) })
	var logBuf bytes.Buffer
	slog.SetDefault(slog.New(slog.NewJSONHandler(&logBuf, &slog.HandlerOptions{Level: slog.LevelWarn})))

	f := newCaptureFixture(t)
	for i := 0; i < 15; i++ {
		name := fmt.Sprintf("file-%02d.txt", i)
		if err := os.WriteFile(filepath.Join(f.dir, name), []byte("hello"), 0o644); err != nil {
			t.Fatalf("write %s: %v", name, err)
		}
	}

	// Pass A: walk runs, fills the FIFO to cap, the in-loop saturation
	// guard stamps the durable backpressure key, and the pass returns
	// without processing the remaining ops.
	sum, err := Capture(context.Background(), f.dir, f.db, f.cctx, CaptureOpts{
		IgnoreChecker:    f.ig,
		SensitiveMatcher: f.matcher,
	})
	if err != nil {
		t.Fatalf("Capture: %v", err)
	}
	if sum.EventsAppended != 10 {
		t.Fatalf("EventsAppended=%d, want 10; summary=%+v", sum.EventsAppended, sum)
	}
	if !sum.BackpressurePaused {
		t.Fatalf("BackpressurePaused=false; want true after mid-pass saturation; summary=%+v", sum)
	}
	if sum.EventsDropped < 1 {
		t.Fatalf("EventsDropped=%d, want >=1; summary=%+v", sum.EventsDropped, sum)
	}
	if sum.EventsDroppedTotal < 1 {
		t.Fatalf("EventsDroppedTotal=%d, want >=1; summary=%+v", sum.EventsDroppedTotal, sum)
	}
	if sum.PendingDepth != 10 {
		t.Fatalf("PendingDepth=%d, want 10; summary=%+v", sum.PendingDepth, sum)
	}
	if sum.PendingHighWater != 10 {
		t.Fatalf("PendingHighWater=%d, want 10; summary=%+v", sum.PendingHighWater, sum)
	}

	var rowCount int
	if err := f.db.SQL().QueryRowContext(context.Background(),
		`SELECT COUNT(*) FROM capture_events WHERE state = 'pending'`).Scan(&rowCount); err != nil {
		t.Fatalf("count rows: %v", err)
	}
	if rowCount != 10 {
		t.Fatalf("rows=%d, want 10 (cap should hold the FIFO at the limit)", rowCount)
	}

	hw, ok, err := state.MetaGet(context.Background(), f.db, MetaKeyPendingHighWater)
	if err != nil {
		t.Fatalf("MetaGet: %v", err)
	}
	if !ok {
		t.Fatalf("expected daemon_meta.%s to be set", MetaKeyPendingHighWater)
	}
	if hwInt, perr := strconv.ParseInt(hw, 10, 64); perr != nil || hwInt != 10 {
		t.Fatalf("daemon_meta.%s=%q, want 10", MetaKeyPendingHighWater, hw)
	}

	// MetaKeyCaptureBackpressurePausedAt must now be set.
	bp, bpOK, err := state.MetaGet(context.Background(), f.db, MetaKeyCaptureBackpressurePausedAt)
	if err != nil {
		t.Fatalf("MetaGet backpressure: %v", err)
	}
	if !bpOK || bp == "" {
		t.Fatalf("expected daemon_meta.%s set after saturation", MetaKeyCaptureBackpressurePausedAt)
	}
	if _, perr := time.Parse(time.RFC3339, bp); perr != nil {
		t.Fatalf("backpressure_paused_at=%q is not RFC3339: %v", bp, perr)
	}

	// MetaKeyCaptureEventsDroppedTotal must be advanced.
	dt, dtOK, err := state.MetaGet(context.Background(), f.db, MetaKeyCaptureEventsDroppedTotal)
	if err != nil {
		t.Fatalf("MetaGet dropped total: %v", err)
	}
	if !dtOK || dt == "" {
		t.Fatalf("expected daemon_meta.%s set after saturation", MetaKeyCaptureEventsDroppedTotal)
	}
	if total, perr := strconv.ParseInt(dt, 10, 64); perr != nil || total < 1 {
		t.Fatalf("events_dropped_total=%q, want >=1", dt)
	}

	if !strings.Contains(logBuf.String(), "capture pending depth at cap") {
		t.Fatalf("expected slog.Warn about capture pending depth at cap, got: %s", logBuf.String())
	}

	// Pass B: a second pass while saturated MUST early-return ahead of
	// walkLive. Drop more files into the worktree to make sure the walk
	// would otherwise produce work.
	for i := 0; i < 5; i++ {
		name := fmt.Sprintf("extra-%02d.txt", i)
		if err := os.WriteFile(filepath.Join(f.dir, name), []byte("y"), 0o644); err != nil {
			t.Fatalf("write %s: %v", name, err)
		}
	}
	prevTotal, _, _ := state.MetaGet(context.Background(), f.db, MetaKeyCaptureEventsDroppedTotal)
	resetPendingCapWarnForTest(t, 1)
	logBuf.Reset()
	sumB, err := Capture(context.Background(), f.dir, f.db, f.cctx, CaptureOpts{
		IgnoreChecker:    f.ig,
		SensitiveMatcher: f.matcher,
	})
	if err != nil {
		t.Fatalf("Capture pass B: %v", err)
	}
	if sumB.WalkedFiles != 0 {
		t.Fatalf("WalkedFiles=%d, want 0 (walk must be skipped while saturated); summary=%+v",
			sumB.WalkedFiles, sumB)
	}
	if !sumB.BackpressurePaused {
		t.Fatalf("BackpressurePaused=false on saturated pass; summary=%+v", sumB)
	}
	if sumB.EventsAppended != 0 {
		t.Fatalf("EventsAppended=%d, want 0 on saturated pass; summary=%+v", sumB.EventsAppended, sumB)
	}
	// Cumulative counter must advance monotonically.
	prevN, _ := strconv.ParseInt(prevTotal, 10, 64)
	if sumB.EventsDroppedTotal <= prevN {
		t.Fatalf("EventsDroppedTotal did not advance: prev=%d cur=%d", prevN, sumB.EventsDroppedTotal)
	}

	// Pass C: simulate replay drain. Mark 1 of the 10 pending rows as
	// published (depth 9), still above the high-water mark of 8 (10*0.8),
	// so the gate stays active.
	if _, err := f.db.SQL().ExecContext(context.Background(),
		`UPDATE capture_events SET state = 'published' WHERE seq IN (
			SELECT seq FROM capture_events WHERE state = 'pending' ORDER BY seq ASC LIMIT 1
		)`); err != nil {
		t.Fatalf("simulate drain (1 row): %v", err)
	}
	sumC, err := Capture(context.Background(), f.dir, f.db, f.cctx, CaptureOpts{
		IgnoreChecker:    f.ig,
		SensitiveMatcher: f.matcher,
	})
	if err != nil {
		t.Fatalf("Capture pass C: %v", err)
	}
	if !sumC.BackpressurePaused {
		t.Fatalf("expected backpressure to still be active above high-water (depth=9, high_water=8); summary=%+v", sumC)
	}
	if sumC.WalkedFiles != 0 {
		t.Fatalf("WalkedFiles=%d, want 0 above high-water; summary=%+v", sumC.WalkedFiles, sumC)
	}

	// Pass D: drain further so depth drops below the high-water mark.
	// Mark two more rows published (depth 7 < 8), backpressure must clear.
	if _, err := f.db.SQL().ExecContext(context.Background(),
		`UPDATE capture_events SET state = 'published' WHERE seq IN (
			SELECT seq FROM capture_events WHERE state = 'pending' ORDER BY seq ASC LIMIT 2
		)`); err != nil {
		t.Fatalf("simulate drain (2 rows): %v", err)
	}
	sumD, err := Capture(context.Background(), f.dir, f.db, f.cctx, CaptureOpts{
		IgnoreChecker:    f.ig,
		SensitiveMatcher: f.matcher,
	})
	if err != nil {
		t.Fatalf("Capture pass D: %v", err)
	}
	if !sumD.BackpressureCleared {
		t.Fatalf("BackpressureCleared=false after drain below high-water; summary=%+v", sumD)
	}
	// Pass D's clear ran at entry, so the meta key was deleted and the
	// walk proceeded. Re-entry mid-walk is permitted (and expected when
	// the post-drain walk + lingering worktree changes push the queue
	// back to cap); the contract we care about is "clear was emitted".
	if sumD.WalkedFiles == 0 {
		t.Fatalf("Pass D should have walked after backpressure cleared; summary=%+v", sumD)
	}
}

// TestCapture_PendingDepthCap_Disabled verifies cap=0 short-circuits all
// counting + bookkeeping work. With ACD_MAX_PENDING_EVENTS=0 a flood of
// captures should land in capture_events without any drops or watermark.
func TestCapture_PendingDepthCap_Disabled(t *testing.T) {
	t.Setenv(EnvMaxPendingEvents, "0")
	resetPendingCapWarnForTest(t, 1)

	f := newCaptureFixture(t)
	for i := 0; i < 12; i++ {
		name := fmt.Sprintf("file-%02d.txt", i)
		if err := os.WriteFile(filepath.Join(f.dir, name), []byte("x"), 0o644); err != nil {
			t.Fatalf("write %s: %v", name, err)
		}
	}
	sum, err := Capture(context.Background(), f.dir, f.db, f.cctx, CaptureOpts{
		IgnoreChecker:    f.ig,
		SensitiveMatcher: f.matcher,
	})
	if err != nil {
		t.Fatalf("Capture: %v", err)
	}
	if sum.EventsDropped != 0 {
		t.Fatalf("disabled cap should not drop; summary=%+v", sum)
	}
	if sum.EventsAppended < 12 {
		t.Fatalf("EventsAppended=%d, want >=12; summary=%+v", sum.EventsAppended, sum)
	}
	if sum.PendingDepth != 0 || sum.PendingHighWater != 0 {
		t.Fatalf("disabled cap should leave depth/high_water at 0; summary=%+v", sum)
	}
	if _, ok, err := state.MetaGet(context.Background(), f.db, MetaKeyPendingHighWater); err != nil {
		t.Fatalf("MetaGet: %v", err)
	} else if ok {
		t.Fatalf("MetaKeyPendingHighWater should be unset when cap is disabled")
	}
}

// TestCapture_PendingDepthCap_RateLimited ensures we don't spam slog.Warn
// across multiple saturated passes. With a 60-second interval, two
// saturated passes back-to-back must produce exactly one warn record.
// Under the new contract the pre-seeded row puts capture into the
// pre-walk gate, so neither pass walks the worktree.
func TestCapture_PendingDepthCap_RateLimited(t *testing.T) {
	t.Setenv(EnvMaxPendingEvents, "1")
	resetPendingCapWarnForTest(t, 60)

	prevDefault := slog.Default()
	t.Cleanup(func() { slog.SetDefault(prevDefault) })
	var logBuf bytes.Buffer
	slog.SetDefault(slog.New(slog.NewJSONHandler(&logBuf, &slog.HandlerOptions{Level: slog.LevelWarn})))

	f := newCaptureFixture(t)
	// Pre-seed one pending row to put the cap immediately in effect.
	if _, err := state.AppendCaptureEvent(context.Background(), f.db, state.CaptureEvent{
		BranchRef: f.cctx.BranchRef, BranchGeneration: f.cctx.BranchGeneration,
		BaseHead: f.cctx.BaseHead, Operation: "create", Path: "seed.txt",
		Fidelity: "exact", State: state.EventStatePending,
	}, []state.CaptureOp{{Op: "create", Path: "seed.txt", Fidelity: "exact"}}); err != nil {
		t.Fatalf("seed pending: %v", err)
	}

	// Stub the seed into shadow_paths so it isn't reclassified as a delete.
	if _, err := f.db.SQL().ExecContext(context.Background(), `
INSERT INTO shadow_paths(branch_ref, branch_generation, path, operation, mode, oid, base_head, fidelity, updated_ts)
VALUES (?, ?, 'seed.txt', 'create', '100644', '0000000000000000000000000000000000000000', ?, 'exact', 0)`,
		f.cctx.BranchRef, f.cctx.BranchGeneration, f.cctx.BaseHead); err != nil {
		t.Fatalf("seed shadow: %v", err)
	}
	if err := os.WriteFile(filepath.Join(f.dir, "seed.txt"), []byte("seed"), 0o644); err != nil {
		t.Fatalf("write seed: %v", err)
	}
	for i := 0; i < 5; i++ {
		name := fmt.Sprintf("drop-%02d.txt", i)
		if err := os.WriteFile(filepath.Join(f.dir, name), []byte("y"), 0o644); err != nil {
			t.Fatalf("write %s: %v", name, err)
		}
	}

	// Two saturated passes: each must early-return BEFORE walkLive (the
	// new pre-walk gate). The rate limiter must fold their warns into one.
	for i := 0; i < 2; i++ {
		sum, err := Capture(context.Background(), f.dir, f.db, f.cctx, CaptureOpts{
			IgnoreChecker:    f.ig,
			SensitiveMatcher: f.matcher,
		})
		if err != nil {
			t.Fatalf("Capture pass %d: %v", i, err)
		}
		if sum.WalkedFiles != 0 {
			t.Fatalf("pass %d WalkedFiles=%d, want 0 (saturated pass must skip walk); summary=%+v",
				i, sum.WalkedFiles, sum)
		}
		if !sum.BackpressurePaused {
			t.Fatalf("pass %d BackpressurePaused=false; want true; summary=%+v", i, sum)
		}
		if sum.EventsDropped < 1 {
			t.Fatalf("pass %d EventsDropped=%d, want >=1; summary=%+v", i, sum.EventsDropped, sum)
		}
	}

	count := strings.Count(logBuf.String(), "capture pending depth at cap")
	if count != 1 {
		t.Fatalf("expected exactly 1 warn record under rate limit, got %d:\n%s", count, logBuf.String())
	}
}

// TestCapture_HonorsManualPauseDirectInvocation: Capture must consult the
// daemon pause gate when a direct caller (test, future CLI wrapper) invokes
// it without going through the run loop. Otherwise the live worktree state
// during a manual pause window would still be enqueued, defeating the
// "pause replay" guarantee — the next replay drain would resurrect work the
// operator intentionally rewound.
//
// Symmetric counterpart to TestReplay_SkipsDrainWhenManualMarkerPresent.
func TestCapture_HonorsManualPauseDirectInvocation(t *testing.T) {
	f := newCaptureFixture(t)
	ctx := context.Background()

	// Bootstrap the shadow so a fresh write is later classified as
	// `create` rather than getting absorbed into the baseline.
	f.firstCapture(t)

	// Drop a file that would, absent the pause, become a captured event.
	if err := os.WriteFile(filepath.Join(f.dir, "rewound.txt"),
		[]byte("would-be-resurrected\n"), 0o644); err != nil {
		t.Fatalf("write rewound: %v", err)
	}

	// Activate a manual pause marker — the same artifact `acd pause` writes.
	if _, err := pausepkg.Write(pausepkg.Path(f.gitDir), pausepkg.Marker{
		Reason: "operator surgery",
		SetAt:  time.Now().UTC().Format(time.RFC3339),
		SetBy:  "test",
	}, false); err != nil {
		t.Fatalf("write pause marker: %v", err)
	}
	t.Cleanup(func() { _ = os.Remove(pausepkg.Path(f.gitDir)) })

	beforeCount := captureEventsTotal(t, ctx, f.db)
	trace := &memoryTraceLogger{}

	sum, err := Capture(ctx, f.dir, f.db, f.cctx, CaptureOpts{
		IgnoreChecker:    f.ig,
		SensitiveMatcher: f.matcher,
		GitDir:           f.gitDir,
		Trace:            trace,
	})
	if err != nil {
		t.Fatalf("Capture: %v", err)
	}
	if !sum.Skipped {
		t.Fatalf("expected Skipped=true, got %+v", sum)
	}
	if sum.SkipReason == "" {
		t.Fatalf("expected non-empty SkipReason, got %+v", sum)
	}
	if sum.EventsAppended != 0 {
		t.Fatalf("expected EventsAppended=0 under manual pause, got %d", sum.EventsAppended)
	}

	// No new capture_events row may have been minted while the pause was
	// active — that's the whole point of the gate.
	if got := captureEventsTotal(t, ctx, f.db); got != beforeCount {
		t.Fatalf("capture_events grew while paused: before=%d after=%d", beforeCount, got)
	}

	// Trace symmetry: the run loop emits "capture.pause" for paused
	// captures via the same helper. Direct callers must produce the same
	// trace shape so operators see one consistent event class.
	events := traceEventsByClass(trace.Events(), "capture.pause")
	if len(events) != 1 {
		t.Fatalf("capture.pause trace events=%d want 1; events=%+v", len(events), trace.Events())
	}
	output, ok := events[0].Output.(map[string]any)
	if !ok {
		t.Fatalf("trace output type=%T want map[string]any", events[0].Output)
	}
	if events[0].Reason != "capture_paused" || output["source"] != "manual" {
		t.Fatalf("unexpected trace event: %+v", events[0])
	}
}

// TestCapture_RunLoopSkipPauseCheckOptOut: SkipPauseCheck=true bypasses the
// pause gate inside Capture — the run loop relies on this to avoid a double
// trace event (the run loop already emits capture.pause before deciding
// whether to invoke Capture at all). With SkipPauseCheck=true the walk runs
// even though a manual marker is present.
func TestCapture_RunLoopSkipPauseCheckOptOut(t *testing.T) {
	f := newCaptureFixture(t)
	ctx := context.Background()
	f.firstCapture(t)

	if err := os.WriteFile(filepath.Join(f.dir, "skip-opt.txt"),
		[]byte("captured-anyway\n"), 0o644); err != nil {
		t.Fatalf("write skip-opt: %v", err)
	}

	if _, err := pausepkg.Write(pausepkg.Path(f.gitDir), pausepkg.Marker{
		Reason: "run loop already gated",
		SetAt:  time.Now().UTC().Format(time.RFC3339),
		SetBy:  "test",
	}, false); err != nil {
		t.Fatalf("write pause marker: %v", err)
	}
	t.Cleanup(func() { _ = os.Remove(pausepkg.Path(f.gitDir)) })

	sum, err := Capture(ctx, f.dir, f.db, f.cctx, CaptureOpts{
		IgnoreChecker:    f.ig,
		SensitiveMatcher: f.matcher,
		GitDir:           f.gitDir,
		SkipPauseCheck:   true,
	})
	if err != nil {
		t.Fatalf("Capture: %v", err)
	}
	if sum.Skipped {
		t.Fatalf("SkipPauseCheck must bypass the gate; got Skipped=true %+v", sum)
	}
	if sum.EventsAppended == 0 {
		t.Fatalf("expected at least one event appended under SkipPauseCheck=true, got %+v", sum)
	}
}

// TestCapture_LargeIgnoredTree is the P0 regression for the AI-Assistant
// build-tree wedge: a top-level gitignored directory must be pruned at the
// directory layer rather than being walked file-by-file. Before the BFS +
// per-layer ignore-classify rewrite, walkLive's filepath.WalkDir DFS would
// readdir every entry inside the ignored subtree and only filter after the
// fact, causing IgnoreChecker batches to balloon to 100k+ paths on real
// repos.
//
// The test creates a small but non-trivial ignored subtree (build/ with a
// fan-out under it) plus a single tracked file at the root. After capture,
// WalkedFiles must reflect only the tracked file — none of the children
// under build/ should have been walked, hashed, or classified.
func TestCapture_LargeIgnoredTree(t *testing.T) {
	f := newCaptureFixture(t)

	// Add `build/` to .gitignore so the entire top-level subtree is
	// classified ignored. We also keep the seed `ignored.txt` rule so the
	// fixture's pinned shadow assumption stays intact.
	gitignore := filepath.Join(f.dir, ".gitignore")
	if err := os.WriteFile(gitignore, []byte("ignored.txt\nbuild/\n"), 0o644); err != nil {
		t.Fatalf("rewrite .gitignore: %v", err)
	}

	// Tracked file at the worktree root that MUST be walked + captured.
	tracked := filepath.Join(f.dir, "kept.txt")
	if err := os.WriteFile(tracked, []byte("keep me\n"), 0o644); err != nil {
		t.Fatalf("write kept.txt: %v", err)
	}

	// Fan out an ignored subtree. Numbers are big enough to make the bug
	// visible (DFS would walk every leaf) but small enough to keep the
	// test fast on slow CI: 16 dirs * 16 files = 256 ignored leaves +
	// 16 dirs themselves.
	const fanout = 16
	buildRoot := filepath.Join(f.dir, "build")
	if err := os.MkdirAll(buildRoot, 0o755); err != nil {
		t.Fatalf("mkdir build: %v", err)
	}
	for i := 0; i < fanout; i++ {
		sub := filepath.Join(buildRoot, fmt.Sprintf("sub-%02d", i))
		if err := os.MkdirAll(sub, 0o755); err != nil {
			t.Fatalf("mkdir sub: %v", err)
		}
		for j := 0; j < fanout; j++ {
			leaf := filepath.Join(sub, fmt.Sprintf("leaf-%02d.bin", j))
			if err := os.WriteFile(leaf, []byte("noise"), 0o644); err != nil {
				t.Fatalf("write leaf: %v", err)
			}
		}
	}

	sum, err := Capture(context.Background(), f.dir, f.db, f.cctx, CaptureOpts{
		IgnoreChecker:    f.ig,
		SensitiveMatcher: f.matcher,
	})
	if err != nil {
		t.Fatalf("Capture: %v", err)
	}

	// The tracked file plus the seed .gitignore are the only paths that
	// should reach the hashing stage. The fanout of 256 ignored leaves
	// MUST NOT count toward WalkedFiles.
	if sum.WalkedFiles >= int64(fanout*fanout) {
		t.Fatalf("WalkedFiles=%d looks like ignored tree was walked anyway (fanout^2=%d)",
			sum.WalkedFiles, fanout*fanout)
	}
	// Acceptance threshold from the task: walkLive should return with
	// WalkedFiles well under 1000.
	if sum.WalkedFiles >= 1000 {
		t.Fatalf("WalkedFiles=%d exceeds 1000 cap; ignored subtree must be pruned at the directory layer", sum.WalkedFiles)
	}

	// The capture event for the tracked file must exist, and no event
	// should have been emitted for any path under build/.
	ops := pendingOps(t, f.db)
	var sawKept bool
	for _, op := range ops {
		if strings.HasPrefix(op.Path, "build/") || op.Path == "build" {
			t.Fatalf("captured event under ignored subtree: %+v", op)
		}
		if op.Path == "kept.txt" {
			sawKept = true
		}
	}
	if !sawKept {
		t.Fatalf("expected create event for kept.txt, got ops=%+v", ops)
	}
}

// TestCapture_ClassifyIgnoredBatched_RespectsCap verifies the helper
// slices large path lists into chunks of at most ignoreCheckBatchSize. We
// stub the underlying IgnoreChecker via a fake .gitignore that doesn't
// actually match anything; what we're asserting is the batching contract,
// not the classification result.
func TestCapture_ClassifyIgnoredBatched_RespectsCap(t *testing.T) {
	f := newCaptureFixture(t)

	// Build a slice well above the cap so the helper must do >=3 round
	// trips internally.
	const total = ignoreCheckBatchSize*2 + 7
	paths := make([]string, total)
	for i := range paths {
		paths[i] = fmt.Sprintf("synthetic/path-%05d.txt", i)
	}

	results, err := classifyIgnoredBatched(context.Background(), f.ig, paths, ignoreCheckBatchSize)
	if err != nil {
		t.Fatalf("classifyIgnoredBatched: %v", err)
	}
	if len(results) != len(paths) {
		t.Fatalf("len(results)=%d, len(paths)=%d — must be 1:1", len(results), len(paths))
	}
}

// TestCapture_AcdTopLevelPathNotPruned is the P0 regression for the
// worktree walker pruning a literal top-level "acd/" directory. The
// daemon's state subdir lives at <gitDir>/acd, never at <worktree>/acd,
// so a user repo that contains a real "acd/" directory at its root must
// be walked and its files captured. The previous walker pruned by name
// match, silently dropping every file under "acd/" — classify then
// emitted phantom delete events and replay deleted real user files.
func TestCapture_AcdTopLevelPathNotPruned(t *testing.T) {
	f := newCaptureFixture(t)

	// Create a worktree-rooted "acd/" directory with a real file inside,
	// plus a nested file deeper in the tree to exercise descent.
	acdDir := filepath.Join(f.dir, "acd")
	if err := os.MkdirAll(filepath.Join(acdDir, "nested"), 0o755); err != nil {
		t.Fatalf("mkdir acd/nested: %v", err)
	}
	if err := os.WriteFile(filepath.Join(acdDir, "foo.txt"), []byte("user file 1"), 0o644); err != nil {
		t.Fatalf("write acd/foo.txt: %v", err)
	}
	if err := os.WriteFile(filepath.Join(acdDir, "nested", "bar.txt"), []byte("user file 2"), 0o644); err != nil {
		t.Fatalf("write acd/nested/bar.txt: %v", err)
	}

	f.firstCapture(t)

	ops := pendingOps(t, f.db)
	wantPaths := map[string]bool{
		"acd/foo.txt":        false,
		"acd/nested/bar.txt": false,
	}
	for _, op := range ops {
		if _, ok := wantPaths[op.Path]; ok {
			if op.Op != "create" {
				t.Fatalf("path %q: got op %q, want %q (all=%+v)", op.Path, op.Op, "create", ops)
			}
			wantPaths[op.Path] = true
		}
	}
	for p, seen := range wantPaths {
		if !seen {
			t.Fatalf("expected create event for %q, not pruned by stale stateSubdir match (all=%+v)", p, ops)
		}
	}
}

// TestCapture_NTPStepBackwardDoesNotSilenceWarn proves that an NTP backward
// step (or any wall-clock rewind) does NOT permanently silence the
// pending-cap warn. Pre-fix the gate compared `now-last < interval` with
// signed int64 arithmetic; if `last` was set when the clock was ahead and
// then NTP stepped back, every subsequent comparison evaluated true and
// suppressed the warn forever.
//
// Setup: stamp `last` 5 minutes in the future (simulating a clock rewind),
// then call shouldEmitPendingCapWarn with `now` representing the corrected
// time. The gate must fire (clamp + emit) instead of staying suppressed.
func TestCapture_NTPStepBackwardDoesNotSilenceWarn(t *testing.T) {
	resetPendingCapWarnForTest(t, 60)
	defer resetPendingCapWarnForTest(t, 0)

	now := time.Date(2026, 5, 2, 12, 0, 0, 0, time.UTC)
	future := now.Add(5 * time.Minute)

	// Stamp `last` 5 minutes ahead — simulates the situation where the wall
	// clock was running fast, we recorded a warn, and then NTP stepped the
	// clock backward.
	pendingCapWarnLastUnix.Store(future.Unix())

	// Override the time source for shouldEmitPendingCapWarn to return `now`.
	fn := func() time.Time { return now }
	wrapped := fn
	pendingCapNowFn.Store(&wrapped)
	defer pendingCapNowFn.Store(nil)

	if !shouldEmitPendingCapWarn() {
		t.Fatal("NTP backward step silenced pending-cap warn forever; clamp missing")
	}
	// The clamp should have rewritten last to `now` so a follow-up immediately
	// after the same now is suppressed (interval throttling still in force).
	if shouldEmitPendingCapWarn() {
		t.Fatal("interval throttling broken after clamp")
	}
}

// TestCapture_SortByPathOrdersSeqLexicographically confirms that with
// SortByPath=true, the capture_events.seq order matches Path ascending —
// even when Classify groups ops by category (renames, live-order
// create/modify/mode, then deletes) and would otherwise yield a non-lex
// order across categories. The setup mixes one delete (early-letter path)
// with one create (late-letter path); without sort the create appears
// first because Classify walks live paths before plain deletes.
func TestCapture_SortByPathOrdersSeqLexicographically(t *testing.T) {
	f := newCaptureFixture(t)

	// Seed shadow with aaa.txt so the next pass can produce a delete for it.
	if err := os.WriteFile(filepath.Join(f.dir, "aaa.txt"), []byte("seed"), 0o644); err != nil {
		t.Fatalf("write aaa.txt: %v", err)
	}
	f.firstCapture(t)
	firstCount := len(pendingOps(t, f.db))

	// Mutate: delete aaa.txt, create zzz.txt. Classify emits creates before
	// deletes within its own category passes, so unsorted order is
	// [zzz create, aaa delete].
	if err := os.Remove(filepath.Join(f.dir, "aaa.txt")); err != nil {
		t.Fatalf("remove aaa.txt: %v", err)
	}
	if err := os.WriteFile(filepath.Join(f.dir, "zzz.txt"), []byte("new"), 0o644); err != nil {
		t.Fatalf("write zzz.txt: %v", err)
	}

	if _, err := Capture(context.Background(), f.dir, f.db, f.cctx, CaptureOpts{
		IgnoreChecker:    f.ig,
		SensitiveMatcher: f.matcher,
		SortByPath:       true,
	}); err != nil {
		t.Fatalf("Capture SortByPath=true: %v", err)
	}

	all := pendingOps(t, f.db)
	pass := all[firstCount:]
	if len(pass) != 2 {
		t.Fatalf("expected 2 ops in second pass, got %d (%+v)", len(pass), pass)
	}
	// With SortByPath=true, lexicographic Path order: aaa.txt before zzz.txt.
	if pass[0].Path != "aaa.txt" || pass[0].Op != "delete" {
		t.Fatalf("op[0] = %+v, want {delete aaa.txt}", pass[0])
	}
	if pass[1].Path != "zzz.txt" || pass[1].Op != "create" {
		t.Fatalf("op[1] = %+v, want {create zzz.txt}", pass[1])
	}
	// Defensive: the slice is non-decreasing by Path.
	for i := 1; i < len(pass); i++ {
		if pass[i-1].Path > pass[i].Path {
			t.Fatalf("seq order not lexicographic: %+v", pass)
		}
	}
}

// TestCapture_SortByPathDefaultPreservesClassifyOrder confirms that with
// SortByPath=false (the daemon run-loop default) the seq order matches
// Classify's native ordering: live-order create/modify/mode first, then
// plain deletes. Same fixture as the sorted variant, so the only
// difference between the two tests is the SortByPath flag.
func TestCapture_SortByPathDefaultPreservesClassifyOrder(t *testing.T) {
	f := newCaptureFixture(t)

	if err := os.WriteFile(filepath.Join(f.dir, "aaa.txt"), []byte("seed"), 0o644); err != nil {
		t.Fatalf("write aaa.txt: %v", err)
	}
	f.firstCapture(t)
	firstCount := len(pendingOps(t, f.db))

	if err := os.Remove(filepath.Join(f.dir, "aaa.txt")); err != nil {
		t.Fatalf("remove aaa.txt: %v", err)
	}
	if err := os.WriteFile(filepath.Join(f.dir, "zzz.txt"), []byte("new"), 0o644); err != nil {
		t.Fatalf("write zzz.txt: %v", err)
	}

	if _, err := Capture(context.Background(), f.dir, f.db, f.cctx, CaptureOpts{
		IgnoreChecker:    f.ig,
		SensitiveMatcher: f.matcher,
		// SortByPath omitted — defaults to false.
	}); err != nil {
		t.Fatalf("Capture default: %v", err)
	}

	all := pendingOps(t, f.db)
	pass := all[firstCount:]
	if len(pass) != 2 {
		t.Fatalf("expected 2 ops in second pass, got %d (%+v)", len(pass), pass)
	}
	// Classify order: creates from the live walk pass come BEFORE plain
	// deletes from the third pass, so seq is [zzz create, aaa delete].
	if pass[0].Path != "zzz.txt" || pass[0].Op != "create" {
		t.Fatalf("op[0] = %+v, want {create zzz.txt}; SortByPath=false must preserve Classify order", pass[0])
	}
	if pass[1].Path != "aaa.txt" || pass[1].Op != "delete" {
		t.Fatalf("op[1] = %+v, want {delete aaa.txt}; SortByPath=false must preserve Classify order", pass[1])
	}
}

// TestCapture_SortByPathShuffledMultiDirOrdering confirms SortByPath=true
// produces strict lexicographic seq order even when the live tree spans
// multiple sibling-cluster directories whose walk order would otherwise
// interleave with Classify's create/delete category passes. The fixture
// touches 6+ paths across 3 directories, mixing creates and a delete so
// Classify's natural order is provably non-lex.
func TestCapture_SortByPathShuffledMultiDirOrdering(t *testing.T) {
	f := newCaptureFixture(t)

	// Seed shadow with one file we'll later delete (a/zzz.txt) so the
	// second pass yields a delete in directory "a/" alongside creates
	// in "b/" and "c/".
	if err := os.MkdirAll(filepath.Join(f.dir, "a"), 0o755); err != nil {
		t.Fatalf("mkdir a: %v", err)
	}
	if err := os.WriteFile(filepath.Join(f.dir, "a", "zzz.txt"), []byte("seed"), 0o644); err != nil {
		t.Fatalf("write a/zzz.txt: %v", err)
	}
	f.firstCapture(t)
	firstCount := len(pendingOps(t, f.db))

	// Mutate: delete a/zzz.txt and create a shuffled set of files spanning
	// 3 directories. We deliberately interleave directories and use names
	// whose lex order differs from creation order.
	if err := os.Remove(filepath.Join(f.dir, "a", "zzz.txt")); err != nil {
		t.Fatalf("remove a/zzz.txt: %v", err)
	}
	for _, dir := range []string{"b", "c"} {
		if err := os.MkdirAll(filepath.Join(f.dir, dir), 0o755); err != nil {
			t.Fatalf("mkdir %s: %v", dir, err)
		}
	}
	// Creation order is intentionally NOT lex order:
	// c/2.txt, a/1.txt, b/3.txt, c/1.txt, a/2.txt, b/1.txt
	creates := []string{
		"c/2.txt",
		"a/1.txt",
		"b/3.txt",
		"c/1.txt",
		"a/2.txt",
		"b/1.txt",
	}
	for _, p := range creates {
		if err := os.WriteFile(filepath.Join(f.dir, p), []byte("x"), 0o644); err != nil {
			t.Fatalf("write %s: %v", p, err)
		}
	}

	if _, err := Capture(context.Background(), f.dir, f.db, f.cctx, CaptureOpts{
		IgnoreChecker:    f.ig,
		SensitiveMatcher: f.matcher,
		SortByPath:       true,
	}); err != nil {
		t.Fatalf("Capture SortByPath=true: %v", err)
	}

	all := pendingOps(t, f.db)
	pass := all[firstCount:]
	if len(pass) != 7 {
		t.Fatalf("expected 7 ops in second pass (1 delete + 6 creates), got %d (%+v)", len(pass), pass)
	}

	// Strict lex order over all 7 paths. With SortByPath=true the delete
	// for a/zzz.txt should land AFTER a/2.txt but BEFORE b/* despite
	// Classify normally putting plain deletes after live-pass creates.
	want := []string{
		"a/1.txt",
		"a/2.txt",
		"a/zzz.txt",
		"b/1.txt",
		"b/3.txt",
		"c/1.txt",
		"c/2.txt",
	}
	for i, w := range want {
		if pass[i].Path != w {
			t.Fatalf("seq[%d].Path = %q, want %q (full pass: %+v)", i, pass[i].Path, w, pass)
		}
	}
	for i := 1; i < len(pass); i++ {
		if pass[i-1].Path > pass[i].Path {
			t.Fatalf("seq order not strictly lexicographic at i=%d: %+v", i, pass)
		}
	}

	// And the delete must still classify as a delete, not as a phantom
	// create — sort must not mutate Op.
	for _, op := range pass {
		if op.Path == "a/zzz.txt" && op.Op != "delete" {
			t.Fatalf("a/zzz.txt op = %q, want delete", op.Op)
		}
	}
}

// TestCapture_DisablePendingCapOptOverride confirms that
// CaptureOpts.DisablePendingCap=true skips the pending-depth cap for that
// call only, even when EnvMaxPendingEvents would otherwise constrain it.
// This is the typed plumb that `acd commit-all` uses in place of the
// removed os.Setenv mutation; the daemon run loop leaves the field false
// so the documented invariant still holds for production.
func TestCapture_DisablePendingCapOptOverride(t *testing.T) {
	t.Setenv(EnvMaxPendingEvents, "5")
	resetPendingCapWarnForTest(t, 1)
	f := newCaptureFixture(t)
	for i := 0; i < 12; i++ {
		name := fmt.Sprintf("file-%02d.txt", i)
		if err := os.WriteFile(filepath.Join(f.dir, name), []byte("hello"), 0o644); err != nil {
			t.Fatalf("write %s: %v", name, err)
		}
	}

	sum, err := Capture(context.Background(), f.dir, f.db, f.cctx, CaptureOpts{
		IgnoreChecker:     f.ig,
		SensitiveMatcher:  f.matcher,
		SortByPath:        true,
		DisablePendingCap: true,
	})
	if err != nil {
		t.Fatalf("Capture: %v", err)
	}
	if sum.EventsDropped != 0 {
		t.Fatalf("EventsDropped=%d with DisablePendingCap=true; want 0; summary=%+v", sum.EventsDropped, sum)
	}
	if sum.BackpressurePaused {
		t.Fatalf("BackpressurePaused=true with DisablePendingCap=true; want false; summary=%+v", sum)
	}
	if sum.EventsAppended < 12 {
		t.Fatalf("EventsAppended=%d, want >=12 with DisablePendingCap=true; summary=%+v", sum.EventsAppended, sum)
	}
}

func TestCaptureCheckpointProtectsBeforeCappedEventsBecomePublishable(t *testing.T) {
	t.Setenv(EnvMaxPendingEvents, "1")
	f := newCaptureFixture(t)
	for _, name := range []string{"one.txt", "two.txt", "three.txt"} {
		if err := os.WriteFile(filepath.Join(f.dir, name), []byte(name+"\n"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	store := checkpointpkg.Store{DB: f.db}
	opts := CaptureOpts{
		IgnoreChecker:    f.ig,
		SensitiveMatcher: f.matcher,
		CheckpointStore:  &store,
		WorktreeID:       checkpointpkg.WorktreeID(f.dir),
		ObservationEpoch: 11,
		CheckpointReason: state.CheckpointReasonPoll,
	}
	summary, err := Capture(context.Background(), f.dir, f.db, f.cctx, opts)
	if err != nil {
		t.Fatal(err)
	}
	if !summary.Protected || summary.CheckpointID == "" {
		t.Fatalf("summary=%+v", summary)
	}
	if summary.EventsDropped != 0 || summary.BackpressurePaused {
		t.Fatalf("checkpoint capture dropped deferred events: %+v", summary)
	}
	if summary.EventsAppended != 1 || summary.PendingDepth != 1 {
		t.Fatalf("checkpoint capture ignored pending cap: %+v", summary)
	}
	projection, err := state.ReadCheckpointProjection(context.Background(), f.db.Path(), 10)
	if err != nil {
		t.Fatal(err)
	}
	if projection.Completed != 1 || projection.Prepared != 0 || projection.Latest == nil {
		t.Fatalf("projection=%+v", projection)
	}
	publishable, err := state.PublishableEvents(context.Background(), f.db, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(publishable) != 1 {
		t.Fatalf("publishable=%d appended=%d", len(publishable), summary.EventsAppended)
	}
	entries, err := git.LsTree(context.Background(), f.dir,
		projection.Latest.CommitOID, true)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]bool{"one.txt": false, "two.txt": false, "three.txt": false}
	for _, entry := range entries {
		if _, ok := want[entry.Path]; ok {
			want[entry.Path] = true
		}
	}
	for path, found := range want {
		if !found {
			t.Fatalf("checkpoint tree missing %s: %+v", path, entries)
		}
	}

	if err := os.WriteFile(filepath.Join(f.dir, "four.txt"), []byte("four"), 0o644); err != nil {
		t.Fatal(err)
	}
	opts.ObservationEpoch = 12
	second, err := Capture(context.Background(), f.dir, f.db, f.cctx, opts)
	if err != nil {
		t.Fatal(err)
	}
	if !second.Protected || second.CheckpointID == "" || second.EventsAppended != 0 {
		t.Fatalf("saturated checkpoint did not remain protection-only: %+v", second)
	}
	latest, err := state.ReadCheckpointProjection(context.Background(), f.db.Path(), 1)
	if err != nil {
		t.Fatal(err)
	}
	if latest.Latest == nil || latest.Latest.ID != second.CheckpointID || len(latest.Latest.EventSeqs) != 0 {
		t.Fatalf("latest saturated checkpoint=%+v", latest.Latest)
	}

	opts.ObservationEpoch = 13
	third, err := Capture(context.Background(), f.dir, f.db, f.cctx, opts)
	if err != nil {
		t.Fatal(err)
	}
	if third.CheckpointID != second.CheckpointID || third.EventsAppended != 0 {
		t.Fatalf("unchanged saturated pass churned checkpoint: second=%+v third=%+v", second, third)
	}

	if err := RequireProtectionCheckpoint(
		context.Background(), f.db, opts.WorktreeID, 14); err != nil {
		t.Fatal(err)
	}
	opts.ObservationEpoch = 14
	barrier, err := Capture(context.Background(), f.dir, f.db, f.cctx, opts)
	if err != nil {
		t.Fatal(err)
	}
	if barrier.CheckpointID == third.CheckpointID || barrier.EventsAppended != 0 {
		t.Fatalf("required barrier reused checkpoint: third=%+v barrier=%+v", third, barrier)
	}
	if _, ok, err := state.MetaGet(context.Background(), f.db,
		requiredProtectionCheckpointEpochKey(opts.WorktreeID)); err != nil || ok {
		t.Fatalf("required checkpoint marker remains: ok=%t err=%v", ok, err)
	}
}

func TestProtectWorktreeCheckpointsDetachedStateWithoutPublicationEvents(t *testing.T) {
	f := newCaptureFixture(t)
	store := checkpointpkg.Store{DB: f.db}
	detached := CaptureContext{BaseHead: f.cctx.BaseHead}
	opts := CaptureOpts{
		IgnoreChecker:    f.ig,
		SensitiveMatcher: f.matcher,
		CheckpointStore:  &store,
		WorktreeID:       checkpointpkg.WorktreeID(f.dir),
		CheckpointReason: state.CheckpointReasonPoll,
		ObservationEpoch: 1,
	}
	first, err := ProtectWorktree(context.Background(), f.dir, f.db, detached, opts)
	if err != nil {
		t.Fatal(err)
	}
	if !first.Protected || first.CheckpointID == "" {
		t.Fatalf("first=%+v", first)
	}
	if err := os.WriteFile(filepath.Join(f.dir, "detached-edit.txt"), []byte("safe\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	opts.ObservationEpoch = 2
	second, err := ProtectWorktree(context.Background(), f.dir, f.db, detached, opts)
	if err != nil {
		t.Fatal(err)
	}
	if !second.Protected || second.CheckpointID == first.CheckpointID {
		t.Fatalf("second=%+v first=%+v", second, first)
	}
	var events int
	if err := f.db.ReadSQL().QueryRowContext(context.Background(),
		`SELECT COUNT(*) FROM capture_events`).Scan(&events); err != nil {
		t.Fatal(err)
	}
	if events != 0 {
		t.Fatalf("protection-only pass created %d publication events", events)
	}
	projection, err := state.ReadCheckpointProjection(context.Background(), f.db.Path(), 10)
	if err != nil {
		t.Fatal(err)
	}
	if projection.Completed != 2 || projection.Latest == nil || projection.Latest.ObservedRef != "" {
		t.Fatalf("projection=%+v", projection)
	}
}

// TestCapture_PathQuiescenceTrackerStampsLastWriteTimes verifies that
// running Capture stamps the per-path quiescence tracker. The capture row
// itself is always durable — the tracker is a separate hint consulted by
// the planner-offer gate — so we assert both: the event is appended AND
// the tracker now reports the path as recently-written.
func TestCapture_PathQuiescenceTrackerStampsLastWriteTimes(t *testing.T) {
	ResetPathQuiescenceForTest(t)
	// Enable the gate; the hot-path RecordPathWrite short-circuits when
	// the gate is off, so a test that asserts a stamp landed must opt in
	// to the gate first.
	t.Setenv(EnvPathQuiescenceSeconds, "30")
	_ = resolvePathQuiescenceSeconds()
	now := time.Date(2026, 5, 15, 12, 0, 0, 0, time.UTC)
	SetPathQuiescenceClockForTest(t, func() time.Time { return now })
	t.Cleanup(func() { SetPathQuiescenceClockForTest(t, nil) })

	f := newCaptureFixture(t)
	if err := os.WriteFile(filepath.Join(f.dir, "tracked.txt"), []byte("v1"), 0o644); err != nil {
		t.Fatalf("write tracked: %v", err)
	}
	sum, err := Capture(context.Background(), f.dir, f.db, f.cctx, CaptureOpts{
		IgnoreChecker:    f.ig,
		SensitiveMatcher: f.matcher,
	})
	if err != nil {
		t.Fatalf("Capture: %v", err)
	}
	if sum.EventsAppended < 1 {
		t.Fatalf("EventsAppended=%d want >=1", sum.EventsAppended)
	}
	got, ok := PathLastWrite("tracked.txt")
	if !ok {
		t.Fatalf("PathLastWrite missing for tracked.txt after capture")
	}
	if !got.Equal(now) {
		t.Fatalf("PathLastWrite=%v want %v", got, now)
	}
	if IsPathQuiescent("tracked.txt", 30*time.Second, now) {
		t.Fatalf("IsPathQuiescent unexpectedly true immediately after write (zero elapsed < 30s)")
	}
	if !IsPathQuiescent("tracked.txt", 30*time.Second, now.Add(31*time.Second)) {
		t.Fatalf("IsPathQuiescent unexpectedly false 31s after write (>=30s elapsed)")
	}
}

// TestCapture_PathQuiescenceDisabledShortCircuits verifies the gate is OFF
// at quiescence == 0 (the default). Even an immediate read after a fresh
// write must report the path as eligible.
func TestCapture_PathQuiescenceDisabledShortCircuits(t *testing.T) {
	ResetPathQuiescenceForTest(t)
	now := time.Date(2026, 5, 15, 12, 0, 0, 0, time.UTC)
	SetPathQuiescenceClockForTest(t, func() time.Time { return now })
	t.Cleanup(func() { SetPathQuiescenceClockForTest(t, nil) })

	RecordPathWrite("recent.go", now)
	if !IsPathQuiescent("recent.go", 0, now) {
		t.Fatalf("zero quiescence must short-circuit to true")
	}
	if !IsPathQuiescent("never-recorded.go", 30*time.Second, now) {
		t.Fatalf("never-recorded path must be treated as quiescent (gate cannot strand stale rows)")
	}
}

// TestCapture_PathQuiescenceEnvParsesSeconds verifies the env knob honors
// integer-second values, defaults to zero on unset/garbage, and clamps
// negatives to zero.
func TestCapture_PathQuiescenceEnvParsesSeconds(t *testing.T) {
	cases := []struct {
		raw  string
		want time.Duration
	}{
		{"", 0},
		{"0", 0},
		{"30", 30 * time.Second},
		{"600", 600 * time.Second},
		{"-5", 0},
		{"garbage", 0},
	}
	for _, tc := range cases {
		t.Run(tc.raw, func(t *testing.T) {
			t.Setenv(EnvPathQuiescenceSeconds, tc.raw)
			got := resolvePathQuiescenceSeconds()
			if got != tc.want {
				t.Fatalf("resolvePathQuiescenceSeconds(%q)=%s want %s", tc.raw, got, tc.want)
			}
		})
	}
}

// TestCapture_MaxPendingEventsOverrideTakesPrecedence confirms that a
// strictly-positive MaxPendingEventsOverride overrides the env value but
// still enforces a cap (unlike DisablePendingCap which removes it).
func TestCapture_MaxPendingEventsOverrideTakesPrecedence(t *testing.T) {
	t.Setenv(EnvMaxPendingEvents, "5")
	resetPendingCapWarnForTest(t, 1)
	f := newCaptureFixture(t)
	for i := 0; i < 12; i++ {
		name := fmt.Sprintf("file-%02d.txt", i)
		if err := os.WriteFile(filepath.Join(f.dir, name), []byte("hello"), 0o644); err != nil {
			t.Fatalf("write %s: %v", name, err)
		}
	}

	sum, err := Capture(context.Background(), f.dir, f.db, f.cctx, CaptureOpts{
		IgnoreChecker:            f.ig,
		SensitiveMatcher:         f.matcher,
		SortByPath:               true,
		MaxPendingEventsOverride: 8,
	})
	if err != nil {
		t.Fatalf("Capture: %v", err)
	}
	if sum.EventsAppended != 8 {
		t.Fatalf("EventsAppended=%d with override=8; want 8; summary=%+v", sum.EventsAppended, sum)
	}
}

func TestBeginProtectionObservationInvalidatesPriorCoverage(t *testing.T) {
	ctx := context.Background()
	f := newCaptureFixture(t)
	if err := state.MetaSetMany(ctx, f.db, map[string]string{
		MetaKeyProtectionObservationEpoch: "7",
		MetaKeyProtectionCoveredEpoch:     "7",
		MetaKeyProtectionComplete:         "true",
	}); err != nil {
		t.Fatal(err)
	}
	epoch, err := BeginProtectionObservation(ctx, f.db)
	if err != nil {
		t.Fatal(err)
	}
	if epoch != 8 {
		t.Fatalf("epoch=%d want=8", epoch)
	}
	complete, _, err := state.MetaGet(ctx, f.db, MetaKeyProtectionComplete)
	if err != nil || complete != "false" {
		t.Fatalf("protection complete=(%q,%v), want false", complete, err)
	}
}

func TestBeginProtectionObservationJoinsConcurrentHints(t *testing.T) {
	ctx := context.Background()
	f := newCaptureFixture(t)
	if err := state.MetaSetMany(ctx, f.db, map[string]string{
		MetaKeyProtectionObservationEpoch: "11",
		MetaKeyProtectionComplete:         "true",
	}); err != nil {
		t.Fatal(err)
	}
	const callers = 16
	epochs := make(chan int64, callers)
	errs := make(chan error, callers)
	var group sync.WaitGroup
	for range callers {
		group.Add(1)
		go func() {
			defer group.Done()
			epoch, err := BeginProtectionObservation(ctx, f.db)
			epochs <- epoch
			errs <- err
		}()
	}
	group.Wait()
	close(epochs)
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	for epoch := range epochs {
		if epoch != 12 {
			t.Fatalf("epoch=%d want=12", epoch)
		}
	}
}
