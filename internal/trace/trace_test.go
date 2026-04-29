package trace

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestGoldenJSONL(t *testing.T) {
	now := time.Date(2026, 4, 29, 12, 34, 56, 789, time.UTC)
	dir := t.TempDir()
	w, err := New(Options{
		Repo: "/repo/acd",
		Dir:  dir,
		Now:  func() time.Time { return now },
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	events := []Event{
		{
			BranchRef:  "refs/heads/main",
			HeadSHA:    "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			EventClass: "capture.classify",
			Decision:   "capture",
			Reason:     "tracked file changed",
			Input:      map[string]any{"path": "alpha.txt", "operation": "modify"},
			Output:     map[string]any{"ops": 1},
			Seq:        1,
			Generation: 7,
		},
		{
			BranchRef:  "refs/heads/main",
			HeadSHA:    "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			EventClass: "capture.sensitive_skip",
			Decision:   "skip",
			Reason:     "sensitive glob matched",
			Input:      map[string]any{"path": ".env"},
			Output:     map[string]any{"matched": true},
			Seq:        2,
			Generation: 7,
		},
		{
			BranchRef:  "refs/heads/main",
			HeadSHA:    "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
			EventClass: "replay.conflict",
			Decision:   "blocked_conflict",
			Reason:     "before oid mismatch",
			Input:      map[string]any{"path": "alpha.txt", "expected": "old"},
			Output:     map[string]any{"actual": "new"},
			Error:      "scratch index disagreed with event before-state",
			Seq:        3,
			Generation: 7,
		},
		{
			BranchRef:  "refs/heads/main",
			HeadSHA:    "cccccccccccccccccccccccccccccccccccccccc",
			EventClass: "replay.commit",
			Decision:   "published",
			Reason:     "commit-tree and update-ref succeeded",
			Input:      map[string]any{"seq": 4},
			Output:     map[string]any{"commit": "dddddddddddddddddddddddddddddddddddddddd"},
			Seq:        4,
			Generation: 7,
		},
		{
			BranchRef:  "refs/heads/main",
			HeadSHA:    "dddddddddddddddddddddddddddddddddddddddd",
			EventClass: "branch_token.transition",
			Decision:   "fast-forward",
			Reason:     "new head descends from previous head",
			Input:      map[string]any{"previous": "rev:cccc refs/heads/main"},
			Output:     map[string]any{"current": "rev:dddd refs/heads/main"},
			Seq:        0,
			Generation: 7,
		},
		{
			BranchRef:  "refs/heads/main",
			HeadSHA:    "dddddddddddddddddddddddddddddddddddddddd",
			EventClass: "bootstrap_shadow.reseed",
			Decision:   "skip",
			Reason:     "generation already seeded",
			Input:      map[string]any{"generation": 7},
			Output:     map[string]any{"rows": 42},
			Seq:        0,
			Generation: 7,
		},
	}
	for _, ev := range events {
		w.Record(ev)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	got, err := os.ReadFile(filepath.Join(dir, "2026-04-29.jsonl"))
	if err != nil {
		t.Fatalf("read trace: %v", err)
	}
	want, err := os.ReadFile(filepath.Join("testdata", "trace_golden.jsonl"))
	if err != nil {
		t.Fatalf("read golden: %v", err)
	}
	if string(got) != string(want) {
		t.Fatalf("golden mismatch\n--- got ---\n%s\n--- want ---\n%s", got, want)
	}
	assertJSONLines(t, string(got), 6)
}

func TestFullBufferDropsOldest(t *testing.T) {
	now := time.Date(2026, 4, 29, 0, 0, 0, 0, time.UTC)
	w, err := New(Options{
		Dir:        t.TempDir(),
		Capacity:   3,
		Now:        func() time.Time { return now },
		skipWorker: true,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = w.Close() })

	for i := 1; i <= 5; i++ {
		w.Record(Event{Seq: int64(i), EventClass: "test"})
	}
	if got := w.Dropped(); got != 2 {
		t.Fatalf("Dropped=%d want 2", got)
	}

	var got []int64
	for len(w.ch) > 0 {
		got = append(got, (<-w.ch).Seq)
	}
	want := []int64{3, 4, 5}
	if len(got) != len(want) {
		t.Fatalf("buffer len=%d want %d (%v)", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("buffer=%v want %v", got, want)
		}
	}
}

func TestFromEnvDisabledIsNoop(t *testing.T) {
	t.Setenv(EnvTrace, "")
	t.Setenv(EnvTraceDir, "")
	gitDir := filepath.Join(t.TempDir(), ".git")

	l := FromEnv("/repo/acd", gitDir)
	if _, ok := l.(Noop); !ok {
		t.Fatalf("FromEnv disabled returned %T, want Noop", l)
	}
	l.Record(Event{EventClass: "ignored"})
	if err := l.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, err := os.Stat(filepath.Join(gitDir, "acd", "trace")); !os.IsNotExist(err) {
		t.Fatalf("disabled trace dir stat err=%v, want not exist", err)
	}
}

func TestFromEnvTraceDirOverride(t *testing.T) {
	t.Setenv(EnvTrace, "yes")
	dir := t.TempDir()
	t.Setenv(EnvTraceDir, dir)

	l := FromEnv("/repo/acd", filepath.Join(t.TempDir(), ".git"))
	l.Record(Event{EventClass: "env.enabled"})
	if err := l.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	matches, err := filepath.Glob(filepath.Join(dir, "*.jsonl"))
	if err != nil {
		t.Fatalf("glob: %v", err)
	}
	if len(matches) != 1 {
		t.Fatalf("trace files=%v, want one in override dir", matches)
	}
	got, err := os.ReadFile(matches[0])
	if err != nil {
		t.Fatalf("read trace: %v", err)
	}
	assertJSONLines(t, string(got), 1)
}

func TestEnabledFromEnvTruthies(t *testing.T) {
	for _, value := range []string{"1", "true", "TRUE", " yes "} {
		t.Setenv(EnvTrace, value)
		if !EnabledFromEnv() {
			t.Fatalf("EnabledFromEnv(%q)=false, want true", value)
		}
	}
	for _, value := range []string{"", "0", "false", "no", "enabled"} {
		t.Setenv(EnvTrace, value)
		if EnabledFromEnv() {
			t.Fatalf("EnabledFromEnv(%q)=true, want false", value)
		}
	}
}

func assertJSONLines(t *testing.T, data string, wantLines int) {
	t.Helper()
	lines := strings.Split(strings.TrimSpace(data), "\n")
	if len(lines) != wantLines {
		t.Fatalf("line count=%d want %d", len(lines), wantLines)
	}
	for i, line := range lines {
		var decoded map[string]any
		if err := json.Unmarshal([]byte(line), &decoded); err != nil {
			t.Fatalf("line %d is not JSON: %v\n%s", i+1, err, line)
		}
		for _, key := range []string{
			"ts", "repo", "branch_ref", "head_sha", "event_class", "decision",
			"reason", "input", "output", "error", "seq", "generation",
		} {
			if _, ok := decoded[key]; !ok {
				t.Fatalf("line %d missing key %q: %s", i+1, key, line)
			}
		}
	}
}
