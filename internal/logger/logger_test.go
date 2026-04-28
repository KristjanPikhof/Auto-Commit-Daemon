package logger

import (
	"bufio"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestNew_RejectsRelativePath(t *testing.T) {
	t.Parallel()
	if _, _, err := New(Options{Path: "relative/log"}); err == nil {
		t.Fatalf("New accepted relative path")
	}
}

func TestNew_RejectsEmptyPath(t *testing.T) {
	t.Parallel()
	if _, _, err := New(Options{}); err == nil {
		t.Fatalf("New accepted empty path")
	}
}

func TestNew_WritesJSONL(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "daemon.log")
	logger, closer, err := New(Options{Path: path})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	logger.Info("hello", "k", "v", "n", 7)
	if err := closer.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	line := strings.TrimRight(string(data), "\n")
	var rec map[string]any
	if err := json.Unmarshal([]byte(line), &rec); err != nil {
		t.Fatalf("log line is not JSON: %v\n%s", err, line)
	}
	if rec["msg"] != "hello" {
		t.Errorf("msg field = %v; want hello", rec["msg"])
	}
	if rec["k"] != "v" {
		t.Errorf("k field = %v", rec["k"])
	}
}

func TestRotation_TriggersOnSize(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "daemon.log")
	// Tiny size so a few writes force a rotation.
	w, err := newRotatingWriter(path, 200, 3, 14)
	if err != nil {
		t.Fatalf("newRotatingWriter: %v", err)
	}
	defer w.Close()

	payload := []byte(strings.Repeat("x", 80) + "\n")
	for i := 0; i < 10; i++ {
		if _, err := w.Write(payload); err != nil {
			t.Fatalf("Write %d: %v", i, err)
		}
	}
	backups, err := listBackups(path)
	if err != nil {
		t.Fatalf("listBackups: %v", err)
	}
	if len(backups) == 0 {
		t.Fatalf("expected at least one rotated archive; got none")
	}
	for _, b := range backups {
		if !strings.HasSuffix(b, ".gz") {
			t.Errorf("backup is not gz-compressed: %s", b)
		}
		// Sanity-check that the gz parses and yields non-empty content.
		f, err := os.Open(b)
		if err != nil {
			t.Fatalf("open backup: %v", err)
		}
		gz, err := gzip.NewReader(f)
		if err != nil {
			f.Close()
			t.Fatalf("gzip.NewReader: %v", err)
		}
		body, err := io.ReadAll(gz)
		gz.Close()
		f.Close()
		if err != nil {
			t.Fatalf("read gz: %v", err)
		}
		if len(body) == 0 {
			t.Errorf("backup %s decompressed to empty", b)
		}
	}
}

func TestRotation_RetainsAtMostMaxBackups(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "daemon.log")
	w, err := newRotatingWriter(path, 100, 2, 14) // max 2 backups
	if err != nil {
		t.Fatalf("newRotatingWriter: %v", err)
	}
	defer w.Close()
	payload := []byte(strings.Repeat("x", 60) + "\n")
	for i := 0; i < 30; i++ {
		if _, err := w.Write(payload); err != nil {
			t.Fatalf("Write: %v", err)
		}
	}
	backups, err := listBackups(path)
	if err != nil {
		t.Fatalf("listBackups: %v", err)
	}
	if len(backups) > 2 {
		t.Fatalf("expected ≤2 backups; got %d (%v)", len(backups), backups)
	}
}

func TestRotation_PrunesByAge(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "daemon.log")

	// Lay down a fake "ancient" backup before constructing the writer.
	old := path + ".5.gz"
	if err := os.WriteFile(old, []byte("old"), 0o600); err != nil {
		t.Fatalf("seed old backup: %v", err)
	}
	ancient := time.Now().Add(-30 * 24 * time.Hour)
	if err := os.Chtimes(old, ancient, ancient); err != nil {
		t.Fatalf("chtimes: %v", err)
	}

	w, err := newRotatingWriter(path, 1024, 5, 14)
	if err != nil {
		t.Fatalf("newRotatingWriter: %v", err)
	}
	defer w.Close()

	if _, err := os.Stat(old); !os.IsNotExist(err) {
		t.Fatalf("ancient backup not pruned: stat err = %v", err)
	}
}

func TestRotation_ConcurrentWritesNoCorruption(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "daemon.log")
	// Sized large enough that *no* records are evicted past retention:
	// the goal here is to prove records aren't *corrupted* under
	// concurrency. Rotation churn is exercised by the smaller-size
	// tests above.
	const (
		goroutines   = 8
		perGoroutine = 200
		maxBackups   = 50
	)
	logger, closer, err := New(Options{
		Path:         path,
		MaxSizeBytes: 4096,
		MaxBackups:   maxBackups,
		MaxAgeDays:   14,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < perGoroutine; i++ {
				logger.Info("hello",
					slog.Int("g", id),
					slog.Int("i", i),
					slog.String("pad", strings.Repeat("z", 40)),
				)
			}
		}(g)
	}
	wg.Wait()
	if err := closer.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Every line in the active log + every line in every gz backup must
	// parse as JSON. Concurrent rotation must not corrupt records.
	totalLines := countJSONLines(t, path)
	backups, err := listBackups(path)
	if err != nil {
		t.Fatalf("listBackups: %v", err)
	}
	for _, b := range backups {
		totalLines += countGzipJSONLines(t, b)
	}
	want := goroutines * perGoroutine
	if totalLines != want {
		t.Fatalf("recovered %d JSON lines across active+backups; want %d (lost or corrupted records)", totalLines, want)
	}
}

func countJSONLines(t *testing.T, path string) int {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open %s: %v", path, err)
	}
	defer f.Close()
	return countJSONFromReader(t, f, path)
}

func countGzipJSONLines(t *testing.T, path string) int {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open gz %s: %v", path, err)
	}
	defer f.Close()
	gz, err := gzip.NewReader(f)
	if err != nil {
		t.Fatalf("gzip.NewReader %s: %v", path, err)
	}
	defer gz.Close()
	return countJSONFromReader(t, gz, path)
}

func countJSONFromReader(t *testing.T, r io.Reader, label string) int {
	t.Helper()
	scan := bufio.NewScanner(r)
	scan.Buffer(make([]byte, 64*1024), 1<<20)
	var n int
	for scan.Scan() {
		line := scan.Bytes()
		if len(line) == 0 {
			continue
		}
		var rec map[string]any
		if err := json.Unmarshal(line, &rec); err != nil {
			t.Fatalf("corrupt JSON in %s line %d: %v\n%s", label, n+1, err, string(line))
		}
		n++
	}
	if err := scan.Err(); err != nil {
		t.Fatalf("scan %s: %v", label, err)
	}
	return n
}

func TestRotatingWriter_ClosedRejectsWrites(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "daemon.log")
	w, err := newRotatingWriter(path, 1024, 3, 14)
	if err != nil {
		t.Fatalf("newRotatingWriter: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, err := w.Write([]byte("after-close\n")); err == nil {
		t.Fatalf("Write after Close returned nil error")
	}
}

func TestShiftBackups_OldestDropped(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "daemon.log")
	// Seed the directory with 3 fake archives at the max-backups limit.
	for i := 1; i <= 3; i++ {
		if err := os.WriteFile(fmt.Sprintf("%s.%d.gz", path, i), []byte(fmt.Sprintf("old%d", i)), 0o600); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}
	w, err := newRotatingWriter(path, 100, 3, 14)
	if err != nil {
		t.Fatalf("newRotatingWriter: %v", err)
	}
	defer w.Close()
	if err := w.shiftBackups(); err != nil {
		t.Fatalf("shiftBackups: %v", err)
	}
	// .3.gz should have been dropped (was at max-backups boundary).
	if _, err := os.Stat(fmt.Sprintf("%s.3.gz", path)); !os.IsNotExist(err) {
		t.Errorf("oldest backup not dropped at max-backups boundary")
	}
	// .1 should now be at .2; .2 at .3.
	if _, err := os.Stat(fmt.Sprintf("%s.2.gz", path)); err != nil {
		t.Errorf(".2.gz missing after shift: %v", err)
	}
	if _, err := os.Stat(fmt.Sprintf("%s.3.gz", path)); err != nil {
		t.Errorf(".3.gz missing after shift: %v", err)
	}
}
