package git

import (
	"context"
	"errors"
	"strings"
	"testing"
)

// hashLargeBlob writes a deterministic blob of size bytes and returns its
// OID via hash-object.
func hashLargeBlob(t *testing.T, dir string, size int) string {
	t.Helper()
	payload := make([]byte, size)
	for i := range payload {
		payload[i] = byte('A' + (i % 26))
	}
	oid, err := HashObjectStdin(context.Background(), dir, payload)
	if err != nil {
		t.Fatalf("hash-object: %v", err)
	}
	return oid
}

func TestCatFileBlob_TruncatesAtLimit(t *testing.T) {
	dir := initRepo(t)
	ctx := context.Background()
	const size = 64 * 1024
	oid := hashLargeBlob(t, dir, size)

	// Cap below the blob size — must return ErrStdoutOverflow with a
	// prefix sized to the cap (not the full blob).
	const cap = 4096
	out, err := CatFileBlobLimited(ctx, dir, oid, cap)
	if !errors.Is(err, ErrStdoutOverflow) {
		t.Fatalf("expected ErrStdoutOverflow, got err=%v len=%d", err, len(out))
	}
	if int64(len(out)) > cap {
		t.Fatalf("prefix exceeded cap: len=%d cap=%d", len(out), cap)
	}
	if len(out) == 0 {
		t.Fatalf("expected non-empty prefix on overflow")
	}

	// Cap above blob size — must succeed and return the full payload.
	full, err := CatFileBlobLimited(ctx, dir, oid, size*2)
	if err != nil {
		t.Fatalf("CatFileBlobLimited within cap: %v", err)
	}
	if len(full) != size {
		t.Fatalf("expected %d bytes, got %d", size, len(full))
	}

	// Default-cap helper preserves backwards compat for small blobs.
	small := hashLargeBlob(t, dir, 32)
	got, err := CatFileBlob(ctx, dir, small)
	if err != nil {
		t.Fatalf("CatFileBlob default cap: %v", err)
	}
	if len(got) != 32 {
		t.Fatalf("expected 32 bytes, got %d", len(got))
	}
}

func TestDiffBlobs_BoundedOutput(t *testing.T) {
	dir := initRepo(t)
	ctx := context.Background()
	const size = 64 * 1024
	oidA := hashLargeBlob(t, dir, size)
	// Mutate the second blob so the diff is also large.
	payload := make([]byte, size)
	for i := range payload {
		payload[i] = byte('a' + ((i + 7) % 26))
	}
	oidB, err := HashObjectStdin(ctx, dir, payload)
	if err != nil {
		t.Fatalf("hash-object B: %v", err)
	}

	const cap = 4096
	out, err := DiffBlobsLimited(ctx, dir, oidA, oidB, cap)
	if !errors.Is(err, ErrStdoutOverflow) {
		t.Fatalf("expected ErrStdoutOverflow, got err=%v len=%d", err, len(out))
	}
	if int64(len(out)) > cap {
		t.Fatalf("diff prefix exceeded cap: len=%d cap=%d", len(out), cap)
	}
	if !strings.Contains(out, "diff") && !strings.Contains(out, "---") && !strings.Contains(out, "@@") {
		// The cap is generous enough to capture at least the diff header
		// or hunk marker; if not, the prefix is still bounded but flag a
		// regression-friendly hint.
		t.Logf("diff prefix did not include header markers (len=%d): %q", len(out), out[:min(len(out), 200)])
	}

	// Within-cap call must succeed end-to-end.
	wide, err := DiffBlobsLimited(ctx, dir, oidA, oidB, size*8)
	if err != nil {
		t.Fatalf("DiffBlobsLimited within cap: %v", err)
	}
	if wide == "" {
		t.Fatalf("expected non-empty diff body")
	}
}
