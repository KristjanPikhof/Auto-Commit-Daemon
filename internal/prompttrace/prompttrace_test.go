package prompttrace

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestWithNoopDoesNotInjectContext(t *testing.T) {
	ctx := With(context.Background(), Noop{}, Metadata{Seq: 7})
	if _, _, ok := From(ctx); ok {
		t.Fatal("Noop logger injected prompt trace context")
	}
}

func TestNewFromEnvDisabledReturnsNilLogger(t *testing.T) {
	t.Setenv(EnvTrace, "")

	logger, err := NewFromEnv("/repo", t.TempDir())
	if err != nil {
		t.Fatalf("NewFromEnv disabled: %v", err)
	}
	if logger != nil {
		t.Fatalf("logger=%T, want nil", logger)
	}
}

func TestNewFromEnvEnabledReturnsInitError(t *testing.T) {
	t.Setenv(EnvTrace, "1")
	gitDir := filepath.Join(t.TempDir(), "not-dir")
	if err := os.WriteFile(gitDir, []byte("file"), 0o600); err != nil {
		t.Fatalf("write gitDir fixture: %v", err)
	}

	logger, err := NewFromEnv("/repo", gitDir)
	if err == nil {
		t.Fatal("NewFromEnv enabled with invalid git dir returned nil error")
	}
	if logger != nil {
		t.Fatalf("logger=%T, want nil on init error", logger)
	}
}

func TestNewRejectsSymlinkTraceDir(t *testing.T) {
	root := t.TempDir()
	target := filepath.Join(root, "target")
	if err := os.Mkdir(target, 0o700); err != nil {
		t.Fatalf("mkdir target: %v", err)
	}
	link := filepath.Join(root, "trace")
	if err := os.Symlink(target, link); err != nil {
		t.Fatalf("symlink trace dir: %v", err)
	}

	if _, err := New(Options{Dir: link}); err == nil {
		t.Fatal("New accepted symlink trace dir")
	}
}

func TestWriterRejectsSymlinkTraceFile(t *testing.T) {
	dir := t.TempDir()
	now := time.Date(2026, 5, 4, 12, 0, 0, 0, time.UTC)
	if err := os.Symlink(filepath.Join(dir, "target.jsonl"), filepath.Join(dir, "2026-05-04.jsonl")); err != nil {
		t.Fatalf("symlink trace file: %v", err)
	}
	w, err := New(Options{Dir: dir, Now: func() time.Time { return now }})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	w.Record(Record{Stage: "request"})
	err = w.Close()
	if err == nil {
		t.Fatal("Close returned nil error for symlink trace file")
	}
	if !strings.Contains(err.Error(), "symlink") {
		t.Fatalf("Close err=%v, want symlink error", err)
	}
}

func TestWalkSkipsMalformedLinesWithValidRecords(t *testing.T) {
	dir := t.TempDir()
	first := mustMarshalRecord(t, Record{
		TS:    time.Date(2026, 5, 4, 10, 0, 0, 0, time.UTC),
		Stage: "request",
		Seq:   1,
	})
	second := mustMarshalRecord(t, Record{
		TS:    time.Date(2026, 5, 4, 10, 1, 0, 0, time.UTC),
		Stage: "response",
		Seq:   2,
	})
	body := string(first) + "\n" +
		`{"ts":"not-a-time","stage":"request"}` + "\n" +
		`{"ts":"2026-05-04T10:02:00Z","stage":` + "\n" +
		string(second) + "\n"
	if err := os.WriteFile(filepath.Join(dir, "2026-05-04.jsonl"), []byte(body), 0o600); err != nil {
		t.Fatalf("write trace: %v", err)
	}

	records, err := Read(context.Background(), ReadOptions{Dir: dir})
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if len(records) != 2 {
		t.Fatalf("len(records)=%d, want 2", len(records))
	}
	if records[0].Seq != 1 || records[1].Seq != 2 {
		t.Fatalf("records seqs=%d,%d; want 1,2", records[0].Seq, records[1].Seq)
	}
}

func TestWalkSkipsSymlinkTraceFiles(t *testing.T) {
	dir := t.TempDir()
	line := mustMarshalRecord(t, Record{
		TS:    time.Date(2026, 5, 4, 10, 0, 0, 0, time.UTC),
		Stage: "request",
		Seq:   1,
	})
	if err := os.WriteFile(filepath.Join(dir, "2026-05-04.jsonl"), append(line, '\n'), 0o600); err != nil {
		t.Fatalf("write real trace: %v", err)
	}
	if err := os.Symlink(filepath.Join(dir, "2026-05-04.jsonl"), filepath.Join(dir, "2026-05-05.jsonl")); err != nil {
		t.Fatalf("symlink trace file: %v", err)
	}

	records, err := Read(context.Background(), ReadOptions{Dir: dir})
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if len(records) != 1 || records[0].Seq != 1 {
		t.Fatalf("records=%v, want only real trace record", records)
	}
}

func mustMarshalRecord(t *testing.T, rec Record) []byte {
	t.Helper()
	line, err := marshalRecord(rec)
	if err != nil {
		t.Fatalf("marshalRecord: %v", err)
	}
	return line
}
