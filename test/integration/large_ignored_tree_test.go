//go:build integration
// +build integration

// large_ignored_tree_test.go is the integration-level mirror of the unit-test
// regression in internal/daemon (TestCapture_LargeIgnoredTree). It drives the
// real `acd` binary against a worktree containing tens of thousands of empty
// files under a top-level gitignored directory and asserts the boot+capture
// path completes in seconds, not minutes.
//
// The pre-fix walkLive code performed a DFS readdir on the entire tree and
// only filtered ignored entries after the fact, which made startup latency
// linear in the size of the ignored subtree. After the BFS + per-layer
// ignore-classify rewrite, the entire ignored directory is pruned at the
// directory layer and never read.
package integration_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

// TestCapture_LargeIgnoredTree wires a 50k-file ignored subtree behind the
// integration boundary. Acceptance:
//
//   - `acd start` reaches mode=running within 10s of invocation.
//   - One capture pass completes (capture.classify trace event observed)
//     within the 10s budget; the trace's WalkedFiles output stays well
//     below 1000 (we accept <1000 per the task description, though
//     production code should report ~1 — only the tracked file).
//   - No capture_events row exists for any path under build/.
func TestCapture_LargeIgnoredTree(t *testing.T) {
	requireSQLite(t)
	if testing.Short() {
		t.Skip("large-ignored-tree fixture writes 50k files; skipped under -short")
	}

	repo := tempRepo(t)
	env := withIsolatedHome(t)
	t.Cleanup(func() { stopSessionForce(t, env, repo) })

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	// Gitignore the entire build/ tree. We commit the .gitignore so the
	// shadow seed sees it as tracked and the daemon's capture loop never
	// has to classify it on every pass.
	gitignore := filepath.Join(repo, ".gitignore")
	if err := os.WriteFile(gitignore, []byte("build/\n"), 0o644); err != nil {
		t.Fatalf("write .gitignore: %v", err)
	}
	runGitOK(t, repo, "add", ".gitignore")
	runGitOK(t, repo, "commit", "-q", "-m", "ignore build/")

	// One tracked file at the worktree root that MUST be walked + captured.
	tracked := filepath.Join(repo, "kept.txt")
	if err := os.WriteFile(tracked, []byte("keep me\n"), 0o644); err != nil {
		t.Fatalf("write kept.txt: %v", err)
	}

	// Fan out the ignored subtree. 50,000 files spread across 100 dirs
	// matches the AI-Assistant build-tree wedge size. Empty bodies keep the
	// fixture cheap on slow disks.
	const totalIgnored = 50_000
	const perDir = 500
	if err := writeIgnoredFanout(filepath.Join(repo, "build"), totalIgnored, perDir); err != nil {
		t.Fatalf("write ignored fanout: %v", err)
	}

	// Phase 2 — start the daemon with tracing on so we can introspect
	// walked_files without reaching into the daemon process.
	traceDir := filepath.Join(repo, ".git", "acd", "trace-test")
	if err := os.MkdirAll(traceDir, 0o755); err != nil {
		t.Fatalf("mkdir trace dir: %v", err)
	}
	traceEnv := envWith(env, "ACD_TRACE=1", "ACD_TRACE_DIR="+traceDir)

	startWall := time.Now()
	startSession(t, ctx, traceEnv, repo, "ignored-1", "shell",
		"ACD_TRACE=1", "ACD_TRACE_DIR="+traceDir)
	waitFor(t, "daemon mode=running within 10s for 50k ignored tree", 10*time.Second, func() bool {
		return readDaemonStateMode(repo) == "running"
	})
	if elapsed := time.Since(startWall); elapsed > 12*time.Second {
		t.Fatalf("startup budget overshot with 50k ignored tree: %v", elapsed)
	}

	// Drive a capture pass through wake. The classify trace will land
	// inside the 10s wall-clock budget defined by the task.
	wakeSession(t, ctx, traceEnv, repo, "ignored-1")

	walkBudget := 10 * time.Second
	classifyDeadline := time.Now().Add(walkBudget)
	var walked int64 = -1
	for time.Now().Before(classifyDeadline) {
		if got, ok := readMaxWalkedFiles(t, traceDir); ok {
			walked = got
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if walked < 0 {
		t.Fatalf("no capture.classify trace event emitted within %v", walkBudget)
	}
	if walked >= 1000 {
		t.Fatalf("WalkedFiles=%d >= 1000; the 50k ignored subtree leaked into the walk", walked)
	}

	// Sanity: the tracked file must still be captured. Wait briefly for the
	// kept.txt commit to land — proves the post-fix path is not just
	// silently skipping everything.
	waitForCommitContaining(t, repo, "kept.txt", 8*time.Second)

	// And no capture_events row may reference build/.
	dbPath := filepath.Join(repo, ".git", "acd", "state.db")
	leaked := sqliteScalar(t, dbPath,
		"SELECT COUNT(*) FROM capture_events WHERE path LIKE 'build/%' OR path = 'build'")
	if leaked != "0" {
		dump := sqliteScalar(t, dbPath,
			"SELECT group_concat(seq || ':' || path, char(10)) FROM capture_events WHERE path LIKE 'build/%' OR path = 'build'")
		t.Fatalf("capture_events leaked into ignored subtree: count=%s\n%s", leaked, dump)
	}
}

// writeIgnoredFanout writes `total` empty files spread across multiple
// subdirectories (≤ perDir files each). Returns the first I/O error.
func writeIgnoredFanout(root string, total, perDir int) error {
	if perDir <= 0 {
		perDir = 500
	}
	if err := os.MkdirAll(root, 0o755); err != nil {
		return fmt.Errorf("mkdir %s: %w", root, err)
	}
	for i := 0; i < total; i++ {
		dirIdx := i / perDir
		sub := filepath.Join(root, fmt.Sprintf("d%05d", dirIdx))
		if i%perDir == 0 {
			if err := os.MkdirAll(sub, 0o755); err != nil {
				return fmt.Errorf("mkdir %s: %w", sub, err)
			}
		}
		leaf := filepath.Join(sub, fmt.Sprintf("f%06d.bin", i))
		f, err := os.Create(leaf)
		if err != nil {
			return fmt.Errorf("create %s: %w", leaf, err)
		}
		if err := f.Close(); err != nil {
			return fmt.Errorf("close %s: %w", leaf, err)
		}
	}
	return nil
}

// readMaxWalkedFiles scans every JSONL trace file in dir for capture.classify
// records and returns MAX(output.walked_files) seen. Returns ok=false when no
// classify record has landed yet.
func readMaxWalkedFiles(t *testing.T, traceDir string) (int64, bool) {
	t.Helper()
	entries, err := os.ReadDir(traceDir)
	if err != nil {
		return 0, false
	}
	var maxWalked int64 = -1
	for _, ent := range entries {
		if ent.IsDir() || !strings.HasSuffix(ent.Name(), ".jsonl") {
			continue
		}
		data, err := os.ReadFile(filepath.Join(traceDir, ent.Name()))
		if err != nil {
			continue
		}
		for _, line := range strings.Split(string(data), "\n") {
			line = strings.TrimSpace(line)
			if line == "" {
				continue
			}
			n, ok := classifyWalkedFiles(line)
			if !ok {
				continue
			}
			if n > maxWalked {
				maxWalked = n
			}
		}
	}
	if maxWalked < 0 {
		return 0, false
	}
	return maxWalked, true
}

// classifyWalkedFiles parses a single JSONL trace line and returns the
// walked_files output if the record is a capture.classify event. Falls back
// to a best-effort substring scan when the JSON shape varies — trace records
// nest output as `{"walked_files":N,...}` so we accept either path.
func classifyWalkedFiles(line string) (int64, bool) {
	if !strings.Contains(line, `"event_class":"capture.classify"`) {
		return 0, false
	}
	const tag = `"walked_files":`
	idx := strings.Index(line, tag)
	if idx < 0 {
		return 0, false
	}
	tail := line[idx+len(tag):]
	end := 0
	for end < len(tail) {
		c := tail[end]
		if (c >= '0' && c <= '9') || c == '-' {
			end++
			continue
		}
		break
	}
	if end == 0 {
		return 0, false
	}
	n, err := strconv.ParseInt(tail[:end], 10, 64)
	if err != nil {
		return 0, false
	}
	return n, true
}

