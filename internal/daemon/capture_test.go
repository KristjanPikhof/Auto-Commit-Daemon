package daemon

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"

	checkpointpkg "github.com/KristjanPikhof/Auto-Commit-Daemon/internal/checkpoint"
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

func TestFinishRemovalScanReusesExactIndexedOversizeBlob(t *testing.T) {
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

func TestFinishRemovalScanStreamsDirtyIndexedLargeBlob(t *testing.T) {
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

	indexBefore, err := os.ReadFile(filepath.Join(f.gitDir, "index"))
	if err != nil {
		t.Fatal(err)
	}
	entries, _, summary, err := ScanProtectedEntries(context.Background(), f.dir, CaptureOpts{IgnoreChecker: f.ig, MaxFileBytes: 2048})
	if err != nil || summary.Oversize != 0 {
		t.Fatalf("scan=%+v err=%v", summary, err)
	}
	want, err := git.HashObjectStdinReadOnly(context.Background(), f.dir, bytes.Repeat([]byte("b"), 4096))
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, entry := range entries {
		if entry.Path == path {
			found = entry.OID == want
		}
	}
	if !found {
		t.Fatalf("dirty blob missing: %+v", entries)
	}
	indexAfter, err := os.ReadFile(filepath.Join(f.gitDir, "index"))
	if err != nil || !bytes.Equal(indexBefore, indexAfter) {
		t.Fatalf("read-only scan changed index: %v", err)
	}

}

func TestFinishRemovalProtectWorktreeCheckpointsDetachedStateWithoutPublicationEvents(t *testing.T) {
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
	legacy, err := state.LegacyTableExists(t.Context(), f.db.ReadSQL(), "capture_events")
	if err != nil || legacy {
		t.Fatalf("protection-only pass created legacy publication state: %t, %v", legacy, err)
	}
	projection, err := state.ReadCheckpointProjection(context.Background(), f.db.Path(), 10)
	if err != nil {
		t.Fatal(err)
	}
	if projection.Completed != 2 || projection.Latest == nil || projection.Latest.ObservedRef != "" {
		t.Fatalf("projection=%+v", projection)
	}
}

func TestFinishRemovalBeginProtectionObservationInvalidatesPriorCoverage(t *testing.T) {
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

func TestFinishRemovalBeginProtectionObservationJoinsConcurrentHints(t *testing.T) {
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

// pendingOps returns the (op, path) pairs from capture_events for assertions.
func pendingOps(t *testing.T, db *state.DB) []struct{ Op, Path string } {
	t.Helper()
	exists, err := state.LegacyTableExists(t.Context(), db.ReadSQL(), "capture_events")
	if err != nil {
		t.Fatal(err)
	}
	if !exists {
		return nil
	}
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

func TestFinishRemovalScanSymlinkDirectoryAndPrivacyExclusions(t *testing.T) {
	f := newCaptureFixture(t)
	external := t.TempDir()
	finishWrite(t, external, "private.txt", "outside repository")
	if err := os.Symlink(external, filepath.Join(f.dir, "linked")); err != nil {
		t.Fatal(err)
	}
	finishWrite(t, f.dir, "node_modules/generated.txt", "generated")
	finishWrite(t, f.dir, ".env", "SECRET=value")
	finishWrite(t, f.dir, ".gitignore", "ignored.txt\n")
	finishWrite(t, f.dir, "ignored.txt", "ignored")
	finishWrite(t, f.dir, "ordinary.txt", "eligible")
	entries, exclusions, summary, err := ScanProtectedEntries(context.Background(), f.dir, CaptureOpts{IgnoreChecker: f.ig, SensitiveMatcher: f.matcher, SafeIgnoreMatcher: state.NewSafeIgnoreMatcherFromValues("true", "")})
	if err != nil || summary.Errors != 0 {
		t.Fatalf("scan=%+v: %v", summary, err)
	}
	seen := map[string]checkpointpkg.Entry{}
	for _, e := range entries {
		seen[e.Path] = e
	}
	link, ok := seen["linked"]
	if !ok || link.Mode != git.SymlinkMode {
		t.Fatalf("link=%+v present=%t", link, ok)
	}
	oid, _, err := git.HashSymlinkBlob(context.Background(), f.dir, external)
	if err != nil || link.OID != oid {
		t.Fatalf("link target oid=%s expected=%s: %v", link.OID, oid, err)
	}
	for _, path := range []string{"linked/private.txt", "node_modules/generated.txt", ".env", "ignored.txt"} {
		if _, ok := seen[path]; ok {
			t.Fatalf("excluded path protected: %s", path)
		}
	}
	if _, ok := seen["ordinary.txt"]; !ok {
		t.Fatal("ordinary file excluded")
	}
	if len(exclusions) == 0 {
		t.Fatal("missing privacy exclusion evidence")
	}
}

func TestFinishRemovalScanRejectsFileIdentitySwap(t *testing.T) {
	f := newCaptureFixture(t)
	path := filepath.Join(f.dir, "swapped.txt")
	finishWrite(t, f.dir, "swapped.txt", "observed bytes")
	before, err := os.Lstat(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(path, path+".old"); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(path+".old", path); err != nil {
		t.Fatal(err)
	}
	entry, ok, reason, err := hashCandidate(context.Background(), f.dir, candidateLike{rel: "swapped.txt", fi: before}, walkOpts{maxBytes: DefaultMaxFileBytes, hashOnly: true})
	if ok || entry.OID != "" || (err == nil && reason != "unstable") {
		t.Fatalf("unsafe swap accepted: entry=%+v ok=%t reason=%q err=%v", entry, ok, reason, err)
	}
}
