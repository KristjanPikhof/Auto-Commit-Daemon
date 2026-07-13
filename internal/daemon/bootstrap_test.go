package daemon

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

// countShadowRows returns shadow_paths rows for (branchRef, gen).
func countShadowRows(t *testing.T, db *state.DB, branchRef string, gen int64) int {
	t.Helper()
	var n int
	if err := db.SQL().QueryRowContext(context.Background(),
		`SELECT COUNT(*) FROM shadow_paths WHERE branch_ref = ? AND branch_generation = ?`,
		branchRef, gen,
	).Scan(&n); err != nil {
		t.Fatalf("count shadow_paths: %v", err)
	}
	return n
}

// addLargeBootstrapFiles commits a nested tree with count entries and returns
// the resulting HEAD OID. Git plumbing avoids materializing thousands of files
// in the worktree; the bootstrap path under test reads the committed tree.
func addLargeBootstrapFiles(t *testing.T, dir string, count int) string {
	t.Helper()
	ctx := context.Background()
	parent, err := git.RevParse(ctx, dir, "HEAD")
	if err != nil {
		t.Fatalf("rev-parse HEAD: %v", err)
	}
	seedOID, err := git.RevParse(ctx, dir, "HEAD:.gitignore")
	if err != nil {
		t.Fatalf("rev-parse .gitignore: %v", err)
	}
	payloadOID, err := git.HashObjectStdin(ctx, dir, []byte("bootstrap payload\n"))
	if err != nil {
		t.Fatalf("hash bootstrap payload: %v", err)
	}

	entries := make([]git.MktreeEntry, 0, count)
	for i := 0; i < count; i++ {
		entries = append(entries, git.MktreeEntry{
			Mode: git.RegularFileMode,
			Type: "blob",
			OID:  payloadOID,
			Path: fmt.Sprintf("f%05d.txt", i),
		})
	}
	dataTree, err := git.Mktree(ctx, dir, entries)
	if err != nil {
		t.Fatalf("mktree data: %v", err)
	}
	rootTree, err := git.Mktree(ctx, dir, []git.MktreeEntry{
		{Mode: git.RegularFileMode, Type: "blob", OID: seedOID, Path: ".gitignore"},
		{Mode: "040000", Type: "tree", OID: dataTree, Path: "data"},
	})
	if err != nil {
		t.Fatalf("mktree root: %v", err)
	}
	head, err := git.CommitTree(ctx, dir, rootTree, "bulk-seed\n", parent)
	if err != nil {
		t.Fatalf("commit-tree: %v", err)
	}
	if err := git.UpdateRef(ctx, dir, "HEAD", head, parent); err != nil {
		t.Fatalf("update-ref HEAD: %v", err)
	}
	return head
}

// TestBootstrapShadow_AtomicSeedOnFailure verifies that an error mid-seed
// (a) leaves no partial rows in shadow_paths for the active generation, and
// (b) does not write the completion marker.
func TestBootstrapShadow_AtomicSeedOnFailure(t *testing.T) {
	f := newDaemonFixture(t)
	ctx := context.Background()

	// Seed the worktree with > shadowBootstrapChunkSize files so we know
	// at least the second chunk will be observable mid-bootstrap.
	totalFiles := shadowBootstrapChunkSize + 100
	head := addLargeBootstrapFiles(t, f.dir, totalFiles)

	cctx := CaptureContext{
		BranchRef:        "refs/heads/main",
		BranchGeneration: 1,
		BaseHead:         head,
	}

	// Inject an error on the second AppendShadowBatch call so the first
	// chunk lands but the seed aborts before the marker is set.
	original := appendShadowBatchFn
	t.Cleanup(func() { appendShadowBatchFn = original })

	calls := 0
	injectedErr := errors.New("injected: simulated DB busy mid-seed")
	appendShadowBatchFn = func(ctx context.Context, d *state.DB, rows []state.ShadowPath) error {
		calls++
		if calls == 1 {
			// Let the first chunk persist so we can verify cleanup
			// actually removes rows (not "nothing to remove").
			return state.AppendShadowBatch(ctx, d, rows)
		}
		return injectedErr
	}

	seeded, err := BootstrapShadow(ctx, f.dir, f.db, cctx)
	if err == nil {
		t.Fatalf("BootstrapShadow expected injected error, got nil (seeded=%d)", seeded)
	}
	if !errors.Is(err, injectedErr) {
		t.Fatalf("BootstrapShadow err = %v; want wrap of %v", err, injectedErr)
	}

	// (a) No partial rows for the active generation.
	if got := countShadowRows(t, f.db, cctx.BranchRef, cctx.BranchGeneration); got != 0 {
		t.Fatalf("expected 0 shadow rows after partial-seed failure; got %d", got)
	}

	// (b) Completion marker must not be set.
	bootstrapped, err := IsShadowBootstrapped(ctx, f.db, cctx.BranchRef, cctx.BranchGeneration)
	if err != nil {
		t.Fatalf("IsShadowBootstrapped: %v", err)
	}
	if bootstrapped {
		t.Fatalf("completion marker unexpectedly set after partial-seed failure")
	}

	// Sanity: the test seam fired more than once (we want to be sure the
	// failure happened on a *later* chunk, not on the first call).
	if calls < 2 {
		t.Fatalf("appendShadowBatchFn calls = %d; want >= 2 (first chunk should land before the injected failure)", calls)
	}
}

// TestBootstrapShadow_LargeRepoBatched verifies that a 5000+ file seed
// completes via chunked AppendShadowBatch, persists every row, and writes
// exactly one completion marker.
func TestBootstrapShadow_LargeRepoBatched(t *testing.T) {
	f := newDaemonFixture(t)
	ctx := context.Background()

	totalFiles := shadowBootstrapChunkSize + 250
	head := addLargeBootstrapFiles(t, f.dir, totalFiles)

	cctx := CaptureContext{
		BranchRef:        "refs/heads/main",
		BranchGeneration: 1,
		BaseHead:         head,
	}

	// Wrap the production seam so we can count chunks. Use the real
	// AppendShadowBatch underneath.
	original := appendShadowBatchFn
	t.Cleanup(func() { appendShadowBatchFn = original })
	chunks := 0
	maxChunkSize := 0
	appendShadowBatchFn = func(ctx context.Context, d *state.DB, rows []state.ShadowPath) error {
		chunks++
		if len(rows) > maxChunkSize {
			maxChunkSize = len(rows)
		}
		return state.AppendShadowBatch(ctx, d, rows)
	}

	seeded, err := BootstrapShadow(ctx, f.dir, f.db, cctx)
	if err != nil {
		t.Fatalf("BootstrapShadow: %v", err)
	}
	// We expect exactly totalFiles + 1 (the seed .gitignore from the
	// fixture) entries to be seeded, all blobs.
	if seeded < totalFiles {
		t.Fatalf("seeded %d rows; want at least %d", seeded, totalFiles)
	}

	got := countShadowRows(t, f.db, cctx.BranchRef, cctx.BranchGeneration)
	if got != seeded {
		t.Fatalf("shadow_paths COUNT = %d; want %d", got, seeded)
	}

	// At least 2 chunks (totalFiles > chunk size). Each chunk must respect
	// the cap.
	if chunks < 2 {
		t.Fatalf("chunks = %d; want >= 2 with %d files at chunk size %d", chunks, totalFiles, shadowBootstrapChunkSize)
	}
	if maxChunkSize > shadowBootstrapChunkSize {
		t.Fatalf("max chunk size %d exceeded cap %d", maxChunkSize, shadowBootstrapChunkSize)
	}

	// Completion marker present and exactly one.
	bootstrapped, err := IsShadowBootstrapped(ctx, f.db, cctx.BranchRef, cctx.BranchGeneration)
	if err != nil {
		t.Fatalf("IsShadowBootstrapped: %v", err)
	}
	if !bootstrapped {
		t.Fatalf("completion marker missing after successful seed")
	}

	// Direct meta lookup also works and the value is "1".
	v, ok, err := state.MetaGet(ctx, f.db, ShadowBootstrappedKey(cctx.BranchRef, cctx.BranchGeneration))
	if err != nil {
		t.Fatalf("MetaGet shadow marker: %v", err)
	}
	if !ok || v != "1" {
		t.Fatalf("shadow marker value = %q (ok=%v); want %q", v, ok, "1")
	}

	// Only one bootstrap key with the expected prefix should exist.
	rows, err := f.db.SQL().QueryContext(ctx,
		`SELECT key FROM daemon_meta WHERE key LIKE ?`,
		MetaKeyShadowBootstrappedPrefix+"%",
	)
	if err != nil {
		t.Fatalf("query daemon_meta: %v", err)
	}
	defer rows.Close()
	keys := 0
	for rows.Next() {
		keys++
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows.Err: %v", err)
	}
	if keys != 1 {
		t.Fatalf("daemon_meta keys with prefix %q = %d; want 1", MetaKeyShadowBootstrappedPrefix, keys)
	}
}

// TestBootstrapShadow_IdempotentByMarker verifies that a second call with the
// completion marker present is a no-op: it does not invoke
// AppendShadowBatch and does not re-walk HEAD's tree.
func TestBootstrapShadow_IdempotentByMarker(t *testing.T) {
	f := newDaemonFixture(t)
	ctx := context.Background()

	// Modest seed — we just need the marker, not chunking behaviour.
	head := addLargeBootstrapFiles(t, f.dir, 50)
	cctx := CaptureContext{
		BranchRef:        "refs/heads/main",
		BranchGeneration: 1,
		BaseHead:         head,
	}

	original := appendShadowBatchFn
	t.Cleanup(func() { appendShadowBatchFn = original })

	firstCalls := 0
	appendShadowBatchFn = func(ctx context.Context, d *state.DB, rows []state.ShadowPath) error {
		firstCalls++
		return state.AppendShadowBatch(ctx, d, rows)
	}

	seeded1, err := BootstrapShadow(ctx, f.dir, f.db, cctx)
	if err != nil {
		t.Fatalf("BootstrapShadow first call: %v", err)
	}
	if seeded1 == 0 {
		t.Fatalf("first BootstrapShadow seeded 0 rows; expected >0")
	}
	if firstCalls == 0 {
		t.Fatalf("first BootstrapShadow did not invoke appendShadowBatchFn")
	}

	// Swap in a seam that fails loudly if it gets called — the second
	// invocation must short-circuit on the marker check.
	appendShadowBatchFn = func(ctx context.Context, d *state.DB, rows []state.ShadowPath) error {
		t.Errorf("appendShadowBatchFn called on idempotent re-bootstrap (rows=%d)", len(rows))
		return nil
	}

	seeded2, err := BootstrapShadow(ctx, f.dir, f.db, cctx)
	if err != nil {
		t.Fatalf("BootstrapShadow second call: %v", err)
	}
	if seeded2 != 0 {
		t.Fatalf("second BootstrapShadow seeded %d rows; want 0 (idempotent)", seeded2)
	}

	// Row count should still equal the first-call result.
	got := countShadowRows(t, f.db, cctx.BranchRef, cctx.BranchGeneration)
	if got != seeded1 {
		t.Fatalf("shadow_paths COUNT changed across idempotent re-bootstrap: was %d, now %d", seeded1, got)
	}
}

// TestBootstrapShadow_OrphanBranchSetsMarker verifies that an empty BaseHead
// (orphan branch) is a no-op for shadow_paths but still writes the marker
// so capture/replay can proceed without being permanently gated.
func TestBootstrapShadow_OrphanBranchSetsMarker(t *testing.T) {
	f := newDaemonFixture(t)
	ctx := context.Background()
	cctx := CaptureContext{
		BranchRef:        "refs/heads/main",
		BranchGeneration: 1,
		BaseHead:         "",
	}

	seeded, err := BootstrapShadow(ctx, f.dir, f.db, cctx)
	if err != nil {
		t.Fatalf("BootstrapShadow orphan: %v", err)
	}
	if seeded != 0 {
		t.Fatalf("orphan BootstrapShadow seeded %d rows; want 0", seeded)
	}

	if got := countShadowRows(t, f.db, cctx.BranchRef, cctx.BranchGeneration); got != 0 {
		t.Fatalf("orphan shadow_paths COUNT = %d; want 0", got)
	}

	bootstrapped, err := IsShadowBootstrapped(ctx, f.db, cctx.BranchRef, cctx.BranchGeneration)
	if err != nil {
		t.Fatalf("IsShadowBootstrapped: %v", err)
	}
	if !bootstrapped {
		t.Fatalf("orphan branch did not get a completion marker")
	}
}

// TestShadowBootstrappedKey_Format pins the on-disk key format so an
// accidental rename of the constant or formatting helper would fail loud.
func TestShadowBootstrappedKey_Format(t *testing.T) {
	got := ShadowBootstrappedKey("refs/heads/main", 7)
	want := "shadow.bootstrapped:refs/heads/main:7"
	if got != want {
		t.Fatalf("ShadowBootstrappedKey = %q; want %q", got, want)
	}
}
