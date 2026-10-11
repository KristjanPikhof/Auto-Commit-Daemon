package central

import (
	"os"
	"path/filepath"
	"testing"
)

func TestStatsDBPreservesURICharactersInPath(t *testing.T) {
	ctx := t.Context()
	path := filepath.Join(t.TempDir(), "data?#%23", "stats.db")
	db, err := OpenAt(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.MetaSet(ctx, "sentinel", "correct-path", 1); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatal(err)
	}
	db, err = OpenAt(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if value, ok, err := db.MetaGet(ctx, "sentinel"); err != nil || !ok || value != "correct-path" {
		t.Fatalf("sentinel=%q ok=%t err=%v", value, ok, err)
	}
}
