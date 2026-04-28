package daemon

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
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
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	dir := t.TempDir()
	if err := git.Init(ctx, dir); err != nil {
		t.Fatalf("git init: %v", err)
	}
	// Configure identity so commit-tree works on hosts without git config.
	for _, kv := range [][]string{
		{"user.email", "acd-test@example.com"},
		{"user.name", "ACD Test"},
		{"commit.gpgsign", "false"},
	} {
		if _, err := git.Run(ctx, git.RunOpts{Dir: dir}, "config", kv[0], kv[1]); err != nil {
			t.Fatalf("git config %s: %v", kv[0], err)
		}
	}
	// Initial commit so HEAD resolves to a real OID.
	seedFile := filepath.Join(dir, ".gitignore")
	if err := os.WriteFile(seedFile, []byte("ignored.txt\n"), 0o644); err != nil {
		t.Fatalf("seed gitignore: %v", err)
	}
	if _, err := git.Run(ctx, git.RunOpts{Dir: dir}, "add", ".gitignore"); err != nil {
		t.Fatalf("git add: %v", err)
	}
	if _, err := git.Run(ctx, git.RunOpts{Dir: dir}, "commit", "-q", "-m", "seed"); err != nil {
		t.Fatalf("git commit: %v", err)
	}

	gitDir, err := git.AbsoluteGitDir(ctx, dir)
	if err != nil {
		t.Fatalf("AbsoluteGitDir: %v", err)
	}
	dbPath := state.DBPathFromGitDir(gitDir)
	db, err := state.Open(ctx, dbPath)
	if err != nil {
		t.Fatalf("state.Open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	head, err := git.RevParse(ctx, dir, "HEAD")
	if err != nil {
		t.Fatalf("rev-parse HEAD: %v", err)
	}

	ig := git.NewIgnoreChecker(dir)
	t.Cleanup(func() { _ = ig.Close() })

	return &captureFixture{
		dir:    dir,
		gitDir: gitDir,
		db:     db,
		cctx: CaptureContext{
			BranchRef:        "refs/heads/main",
			BranchGeneration: 1,
			BaseHead:         head,
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
