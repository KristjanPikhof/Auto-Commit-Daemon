package globalops

import (
	"os"
	"path/filepath"
	"testing"
)

func TestStorePreservesURICharactersInPath(t *testing.T) {
	ctx := t.Context()
	path := filepath.Join(t.TempDir(), "data?#%23", "operations.db")
	store, err := Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Prepare(ctx, Operation{ID: "saved", Kind: "setup", Phase: "planned", PlanDigest: "digest"}, nil); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatal(err)
	}
	store, err = OpenReadOnly(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if operations, err := store.OperationsByPhase(ctx, "planned"); err != nil || len(operations) != 1 || operations[0].ID != "saved" {
		t.Fatalf("operations=%+v err=%v", operations, err)
	}
}
