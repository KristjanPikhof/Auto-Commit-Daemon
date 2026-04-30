//go:build integration
// +build integration

// Package integration_test composes the production stack end-to-end with
// real subprocesses (`acd` binary) and real git worktrees. The build tag
// keeps the package out of the default test run; invoke with
//
//	go test ./test/integration/... -tags=integration -race -count=1
//
// (per §14.3). Helpers live here; one *_test.go file per scenario family.
package integration_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"
)

// acdBinaryPath is the per-process build cache. We compile the `acd` binary
// once and reuse it across every integration scenario. The directory is
// registered for cleanup in TestMain so /tmp does not accumulate stale
// builds across `go test` runs.
var (
	acdBinaryOnce sync.Once
	acdBinary     string
	acdBinaryDir  string
	acdBinaryErr  error
)

// TestMain owns process-wide setup/teardown — currently just removing the
// build cache directory after the suite completes so /tmp stays clean.
func TestMain(m *testing.M) {
	code := m.Run()
	if acdBinaryDir != "" {
		_ = os.RemoveAll(acdBinaryDir)
	}
	os.Exit(code)
}

// buildAcdBinary builds (or returns the cached path of) the production `acd`
// binary. Subsequent calls within the same `go test` process reuse the
// existing binary; we never invalidate the cache because go's test driver
// gives us a fresh process per package.
//
// The build runs with the same flags as the Makefile (CGO_ENABLED=0,
// netgo+osusergo) so the binary is identical to a release build for the
// purpose of integration scenarios.
func buildAcdBinary(t *testing.T) string {
	t.Helper()
	acdBinaryOnce.Do(func() {
		// Resolve the repo root by climbing up from this file. test/integration
		// sits two directories below the module root.
		_, here, _, ok := runtime.Caller(0)
		if !ok {
			acdBinaryErr = errors.New("integration: cannot resolve test source path")
			return
		}
		repoRoot := filepath.Clean(filepath.Join(filepath.Dir(here), "..", ".."))

		outDir, err := os.MkdirTemp("", "acd-integration-bin-*")
		if err != nil {
			acdBinaryErr = fmt.Errorf("mkdtemp: %w", err)
			return
		}
		acdBinaryDir = outDir
		bin := filepath.Join(outDir, "acd")
		if runtime.GOOS == "windows" {
			bin += ".exe"
		}
		cmd := exec.Command("go", "build",
			"-tags=netgo,osusergo",
			"-trimpath",
			"-o", bin,
			"./cmd/acd",
		)
		cmd.Dir = repoRoot
		cmd.Env = append(os.Environ(), "CGO_ENABLED=0")
		out, err := cmd.CombinedOutput()
		if err != nil {
			acdBinaryErr = fmt.Errorf("go build: %w\n%s", err, out)
			return
		}
		acdBinary = bin
	})
	if acdBinaryErr != nil {
		t.Fatalf("buildAcdBinary: %v", acdBinaryErr)
	}
	return acdBinary
}

// tempRepo creates a fresh git repo with one seed commit so HEAD resolves
// for capture+replay. Returns the absolute repo dir; the caller owns no
// cleanup beyond t.TempDir's automatic teardown.
func tempRepo(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	gitInit(t, dir)
	// Configure a user so commits succeed without global config.
	for _, kv := range [][]string{
		{"user.email", "acd-integration@example.com"},
		{"user.name", "ACD Integration"},
		{"commit.gpgsign", "false"},
	} {
		runGitOK(t, dir, "config", kv[0], kv[1])
	}
	// Seed commit so HEAD exists.
	if err := os.WriteFile(filepath.Join(dir, ".gitignore"), []byte("# acd integration seed\n"), 0o644); err != nil {
		t.Fatalf("seed: %v", err)
	}
	runGitOK(t, dir, "add", ".gitignore")
	runGitOK(t, dir, "commit", "-q", "-m", "seed")
	return dir
}

// gitInit runs `git init -q dir`.
func gitInit(t *testing.T, dir string) {
	t.Helper()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", dir, err)
	}
	if out, err := exec.Command("git", "init", "-q", dir).CombinedOutput(); err != nil {
		t.Fatalf("git init: %v\n%s", err, out)
	}
	// Force branch to main regardless of host's init.defaultBranch.
	if out, err := exec.Command("git", "-C", dir, "symbolic-ref", "HEAD", "refs/heads/main").CombinedOutput(); err != nil {
		t.Fatalf("symbolic-ref HEAD: %v\n%s", err, out)
	}
}

// runGitOK runs `git -C dir args...` and fails the test on non-zero exit.
func runGitOK(t *testing.T, dir string, args ...string) string {
	t.Helper()
	out, err := runGit(dir, args...)
	if err != nil {
		t.Fatalf("git %s: %v\n%s", strings.Join(args, " "), err, out)
	}
	return out
}

// runGit runs `git -C dir args...` and returns stdout (or stdout+stderr on
// failure). No t pointer so it's safe in goroutines / waitFor predicates.
func runGit(dir string, args ...string) (string, error) {
	full := append([]string{"-C", dir}, args...)
	cmd := exec.Command("git", full...)
	out, err := cmd.CombinedOutput()
	return string(out), err
}

// ExecResult captures the output of an `acd` invocation.
type ExecResult struct {
	Stdout   string
	Stderr   string
	ExitCode int
}

// runAcd execs the integration-built binary with `args` and returns its
// stdout/stderr/exit-code. Inherits HOME from the test process; callers that
// need an isolated XDG layout should set ACD_TEST_HOME via env when
// appropriate (we don't reach for this in v1).
func runAcd(t *testing.T, ctx context.Context, env []string, args ...string) ExecResult {
	t.Helper()
	bin := buildAcdBinary(t)
	cmd := exec.CommandContext(ctx, bin, args...)
	if env != nil {
		cmd.Env = env
	} else {
		cmd.Env = os.Environ()
	}
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()
	exit := 0
	if err != nil {
		var ee *exec.ExitError
		if errors.As(err, &ee) {
			exit = ee.ExitCode()
		} else {
			// non-ExitError (e.g. binary missing) — propagate via Stderr so
			// the caller can decide.
			stderr.WriteString("\n[runAcd]: " + err.Error())
			exit = -1
		}
	}
	return ExecResult{
		Stdout:   stdout.String(),
		Stderr:   stderr.String(),
		ExitCode: exit,
	}
}

// waitFor polls pred at ~50ms intervals until it returns true or the
// timeout elapses. Fails the test on timeout with `name` in the message.
func waitFor(t *testing.T, name string, timeout time.Duration, pred func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if pred() {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("waitFor: %s did not become true within %v", name, timeout)
}

// withIsolatedHome returns the env slice for runAcd that points HOME at a
// per-test tmpdir so the central registry/stats live in isolation.
func withIsolatedHome(t *testing.T) []string {
	t.Helper()
	home := t.TempDir()
	env := os.Environ()
	for i, kv := range env {
		if strings.HasPrefix(kv, "HOME=") || strings.HasPrefix(kv, "XDG_STATE_HOME=") ||
			strings.HasPrefix(kv, "XDG_DATA_HOME=") || strings.HasPrefix(kv, "XDG_CONFIG_HOME=") ||
			strings.HasPrefix(kv, "ACD_AI_") {
			env[i] = ""
		}
	}
	out := make([]string, 0, len(env)+4)
	for _, kv := range env {
		if kv != "" {
			out = append(out, kv)
		}
	}
	out = append(out,
		"HOME="+home,
		"XDG_STATE_HOME=",
		"XDG_DATA_HOME=",
		"XDG_CONFIG_HOME=",
	)
	return out
}

// envWith appends extra KEY=VALUE pairs to a base env (typically
// withIsolatedHome's return value).
func envWith(base []string, kvs ...string) []string {
	out := make([]string, 0, len(base)+len(kvs))
	out = append(out, base...)
	out = append(out, kvs...)
	return out
}

// readDaemonStateMode reads daemon_state.mode from <repo>/.git/acd/state.db
// using the sqlite3 binary. Falls back to "" if anything goes wrong (caller
// is expected to use waitFor + retry).
func readDaemonStateMode(repoDir string) string {
	dbPath := filepath.Join(repoDir, ".git", "acd", "state.db")
	if _, err := os.Stat(dbPath); err != nil {
		return ""
	}
	out, err := exec.Command("sqlite3", dbPath, "SELECT mode FROM daemon_state WHERE id = 1").CombinedOutput()
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(out))
}

// readDaemonStatePID returns daemon_state.pid (or 0).
func readDaemonStatePID(repoDir string) int {
	dbPath := filepath.Join(repoDir, ".git", "acd", "state.db")
	if _, err := os.Stat(dbPath); err != nil {
		return 0
	}
	out, err := exec.Command("sqlite3", dbPath, "SELECT pid FROM daemon_state WHERE id = 1").CombinedOutput()
	if err != nil {
		return 0
	}
	v := strings.TrimSpace(string(out))
	pid := 0
	fmt.Sscanf(v, "%d", &pid)
	return pid
}

// writeFile is shorthand for os.WriteFile + t.Fatalf.
func writeFile(t *testing.T, path, body string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("mkdir for %s: %v", path, err)
	}
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

func gitCommitAll(t *testing.T, repo, message string, paths ...string) string {
	t.Helper()
	if len(paths) == 0 {
		paths = []string{"."}
	}
	runGitOK(t, repo, append([]string{"add"}, paths...)...)
	runGitOK(t, repo, "commit", "-q", "-m", message)
	return strings.TrimSpace(runGitOK(t, repo, "rev-parse", "HEAD"))
}

// waitForMetaCleared polls daemon_meta until the given key is absent or empty,
// with a 50ms tick and the supplied timeout. Fails the test if the key is still
// set when the deadline expires.
func waitForMetaCleared(t *testing.T, dbPath, key string, timeout time.Duration) {
	t.Helper()
	waitFor(t, fmt.Sprintf("daemon_meta[%s] cleared", key), timeout, func() bool {
		out, err := exec.Command("sqlite3", dbPath,
			fmt.Sprintf("SELECT value FROM daemon_meta WHERE key = %s", sqliteLiteral(key))).
			CombinedOutput()
		if err != nil {
			return false
		}
		return strings.TrimSpace(string(out)) == ""
	})
}

// sqliteLiteral returns a single-quoted SQLite string literal for key.
func sqliteLiteral(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}
