package credentials

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/paths"
)

func testStore(t *testing.T) Store {
	t.Helper()
	root := t.TempDir()
	return NewStore(paths.Roots{Config: filepath.Join(root, "config", "acd")})
}

func TestStoreSetReadResolveAndRemove(t *testing.T) {
	t.Parallel()
	store := testStore(t)
	if err := store.Set("sk-protected-value"); err != nil {
		t.Fatal(err)
	}
	assertMode(t, filepath.Dir(store.Path()), 0o700)
	assertMode(t, store.Path(), 0o600)
	key, err := store.Read()
	if err != nil || key != "sk-protected-value" {
		t.Fatalf("Read key set=%v err=%v", key != "", err)
	}
	status, err := store.Status()
	if err != nil || !status.ProtectedFileSet || status.Path != store.Path() {
		t.Fatalf("Status = %+v err=%v", status, err)
	}
	key, source, err := Resolve(store, func(string) (string, bool) {
		return "sk-environment-value", true
	})
	if err != nil || key != "sk-environment-value" || source != SourceEnvironment {
		t.Fatalf("Resolve source=%q key set=%v err=%v", source, key != "", err)
	}
	removed, err := store.Remove()
	if err != nil || !removed {
		t.Fatalf("Remove = %v, %v", removed, err)
	}
	if _, err := store.Read(); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Read after remove = %v", err)
	}
}

func TestResolveRefusesInvalidEnvironmentCredentialWithoutFileFallback(t *testing.T) {
	store := testStore(t)
	if err := store.Set("sk-protected-value"); err != nil {
		t.Fatal(err)
	}
	key, source, err := Resolve(store, func(string) (string, bool) { return "bad\nvalue", true })
	if err == nil || key != "" || source != SourceEnvironment || strings.Contains(err.Error(), "sk-protected-value") {
		t.Fatalf("Resolve invalid environment = source %q key set %v err %v", source, key != "", err)
	}
}

func TestStoreRejectsInsecureFilesystemObjects(t *testing.T) {
	t.Parallel()
	t.Run("directory mode", func(t *testing.T) {
		store := testStore(t)
		if err := os.MkdirAll(filepath.Dir(store.Path()), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := store.Set("sk-value"); err == nil || !strings.Contains(err.Error(), "0700") {
			t.Fatalf("Set error = %v", err)
		}
	})
	t.Run("file mode", func(t *testing.T) {
		store := testStore(t)
		if err := os.MkdirAll(filepath.Dir(store.Path()), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(store.Path(), []byte(`{"version":1,"openai_compat_api_key":"sk-value"}`), 0o644); err != nil {
			t.Fatal(err)
		}
		if _, err := store.Read(); err == nil || !strings.Contains(err.Error(), "0600") {
			t.Fatalf("Read error = %v", err)
		}
	})
	t.Run("file symlink", func(t *testing.T) {
		store := testStore(t)
		if err := os.MkdirAll(filepath.Dir(store.Path()), 0o700); err != nil {
			t.Fatal(err)
		}
		target := filepath.Join(t.TempDir(), "target")
		if err := os.WriteFile(target, []byte(`{"version":1,"openai_compat_api_key":"sk-value"}`), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(target, store.Path()); err != nil {
			t.Fatal(err)
		}
		if _, err := store.Read(); err == nil || !strings.Contains(err.Error(), "regular file") {
			t.Fatalf("Read error = %v", err)
		}
	})
}

func TestStoreRejectsMalformedOrUnsupportedDocumentsWithoutLeak(t *testing.T) {
	t.Parallel()
	tests := map[string]string{
		"malformed": `{"version":1`,
		"multiple":  `{"version":1,"openai_compat_api_key":"sk-secret"} {}`,
		"future":    `{"version":2,"openai_compat_api_key":"sk-secret"}`,
		"unknown":   `{"version":1,"openai_compat_api_key":"sk-secret","extra":true}`,
		"empty":     `{"version":1,"openai_compat_api_key":""}`,
	}
	for name, body := range tests {
		name, body := name, body
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			store := testStore(t)
			if err := os.MkdirAll(filepath.Dir(store.Path()), 0o700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(store.Path(), []byte(body), 0o600); err != nil {
				t.Fatal(err)
			}
			_, err := store.Read()
			if err == nil || strings.Contains(err.Error(), "sk-secret") {
				t.Fatalf("Read error = %v", err)
			}
		})
	}
}

func TestStoreAtomicRewriteLeavesNoTemporaryFile(t *testing.T) {
	t.Parallel()
	store := testStore(t)
	if err := store.Set("sk-first"); err != nil {
		t.Fatal(err)
	}
	if err := store.Set("sk-second"); err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(filepath.Dir(store.Path()))
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Name() != filepath.Base(store.Path()) {
		t.Fatalf("directory entries = %v", entries)
	}
	key, err := store.Read()
	if err != nil || key != "sk-second" {
		t.Fatalf("rewritten key set=%v err=%v", key != "", err)
	}
}

func TestCredentialReplacementCommitRollbackAndCrashRecovery(t *testing.T) {
	store := testStore(t)
	if err := store.Set("sk-before"); err != nil {
		t.Fatal(err)
	}
	tx, err := store.BeginReplacement("sk-after")
	if err != nil {
		t.Fatal(err)
	}
	if got, _ := store.Read(); got != "sk-after" {
		t.Fatalf("replacement = %q", got)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	if got, _ := store.Read(); got != "sk-before" {
		t.Fatalf("rollback = %q", got)
	}

	if _, err := store.BeginReplacement("sk-interrupted"); err != nil {
		t.Fatal(err)
	}
	if err := store.RecoverPendingReplacement(); err != nil {
		t.Fatal(err)
	}
	if got, _ := store.Read(); got != "sk-before" {
		t.Fatalf("crash recovery = %q", got)
	}

	tx, err = store.BeginReplacement("sk-committed")
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if got, _ := store.Read(); got != "sk-committed" {
		t.Fatalf("commit = %q", got)
	}
	entries, err := os.ReadDir(filepath.Dir(store.Path()))
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Name() != filepath.Base(store.Path()) {
		t.Fatalf("credential directory entries = %v", entries)
	}
}

func assertMode(t *testing.T, path string, want os.FileMode) {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if got := info.Mode().Perm(); got != want {
		t.Fatalf("%s mode = %#o, want %#o", path, got, want)
	}
}
