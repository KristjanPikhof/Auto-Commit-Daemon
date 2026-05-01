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
	"testing"
	"time"

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
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	dir := t.TempDir()
	if err := git.Init(ctx, dir); err != nil {
		t.Fatalf("git init: %v", err)
	}
	// Force HEAD onto refs/heads/main regardless of host's init.defaultBranch
	// (CI runners default to master; the fixture's CaptureContext is pinned
	// to refs/heads/main).
	if _, err := git.Run(ctx, git.RunOpts{Dir: dir}, "symbolic-ref", "HEAD", "refs/heads/main"); err != nil {
		t.Fatalf("symbolic-ref HEAD: %v", err)
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

	// Pass C: simulate replay drain. Mark 3 of the 10 pending rows as
	// published (depth 7), still above the high-water mark of 8 (10*0.8),
	// so the gate stays active.
	if _, err := f.db.SQL().ExecContext(context.Background(),
		`UPDATE capture_events SET state = 'published' WHERE seq IN (
			SELECT seq FROM capture_events WHERE state = 'pending' ORDER BY seq ASC LIMIT 3
		)`); err != nil {
		t.Fatalf("simulate drain (3 rows): %v", err)
	}
	sumC, err := Capture(context.Background(), f.dir, f.db, f.cctx, CaptureOpts{
		IgnoreChecker:    f.ig,
		SensitiveMatcher: f.matcher,
	})
	if err != nil {
		t.Fatalf("Capture pass C: %v", err)
	}
	if !sumC.BackpressurePaused {
		t.Fatalf("expected backpressure to still be active above high-water (depth=7, high_water=8); summary=%+v", sumC)
	}
	if sumC.WalkedFiles != 0 {
		t.Fatalf("WalkedFiles=%d, want 0 above high-water; summary=%+v", sumC.WalkedFiles, sumC)
	}

	// Pass D: drain further so depth drops below the high-water mark.
	// Mark one more row published (depth 6), backpressure must clear.
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
	if sumD.BackpressurePaused {
		t.Fatalf("BackpressurePaused=true after clear transition; summary=%+v", sumD)
	}
	if _, ok, _ := state.MetaGet(context.Background(), f.db, MetaKeyCaptureBackpressurePausedAt); ok {
		t.Fatalf("MetaKeyCaptureBackpressurePausedAt should be deleted after clear")
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

// TestCapture_PendingDepthCap_RateLimited ensures we don't spam slog.Warn on
// every dropped event in a single burst. With a 60-second interval and 5
// drops, we expect exactly one warn record on the buffer.
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
	sum, err := Capture(context.Background(), f.dir, f.db, f.cctx, CaptureOpts{
		IgnoreChecker:    f.ig,
		SensitiveMatcher: f.matcher,
	})
	if err != nil {
		t.Fatalf("Capture: %v", err)
	}
	if sum.EventsDropped < 5 {
		t.Fatalf("expected at least 5 drops, got summary=%+v", sum)
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
