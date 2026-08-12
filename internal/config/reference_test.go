package config

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

func TestGeneratedReferenceIsCurrent(t *testing.T) {
	path := filepath.Join("..", "..", "docs", "configuration-reference.md")
	current, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if generated := RenderReference(); !bytes.Equal(current, generated) {
		t.Fatalf("%s is stale; run go run ./internal/config/configdoc --write", path)
	}
}

func TestGeneratedReferenceDoesNotExposeSensitiveDefaults(t *testing.T) {
	reference := RenderReference()
	for _, field := range Catalog() {
		if !field.Sensitive {
			continue
		}
		rowPrefix := []byte("| `" + field.Name + "` |")
		start := bytes.Index(reference, rowPrefix)
		if start < 0 {
			t.Fatalf("missing row for %s", field.Name)
		}
		end := bytes.IndexByte(reference[start:], '\n')
		if end < 0 {
			end = len(reference) - start
		}
		row := reference[start : start+end]
		if !bytes.Contains(row, []byte("| not shown |")) {
			t.Fatalf("sensitive default for %s was not redacted: %s", field.Name, row)
		}
	}
}

func TestGeneratedReferenceIncludesEveryCatalogFieldOnce(t *testing.T) {
	reference := RenderReference()
	if got, want := bytes.Count(reference, []byte("\n| `")), len(Catalog()); got != want {
		t.Fatalf("generated rows=%d, want catalog rows=%d", got, want)
	}
	for _, field := range Catalog() {
		prefix := []byte("| `" + field.Name + "` | `" + field.Environment + "` |")
		if got := bytes.Count(reference, prefix); got != 1 {
			t.Fatalf("field %s rows=%d, want 1", field.Name, got)
		}
	}
}

func TestGeneratedReferenceDoesNotResolveEnvironmentValues(t *testing.T) {
	const sentinel = "effective-value-must-not-appear"
	for _, field := range Catalog() {
		t.Setenv(field.Environment, sentinel)
	}
	if reference := RenderReference(); bytes.Contains(reference, []byte(sentinel)) {
		t.Fatal("generated reference exposed an effective environment value")
	}
}
