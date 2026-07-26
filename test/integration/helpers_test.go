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
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/ai"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/config"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

// activateIntentV2Runtime installs an explicit, already-applied Intent Fast v2
// revision before daemon startup. Integration tests may still use environment
// variables to customize provider and timing fields, but never rely on the
// unsupported env-only v1 startup path.
func activateIntentV2Runtime(t *testing.T, repo string, extra ...string) []string {
	t.Helper()
	values := make(map[string]string, len(extra))
	cleaned := make([]string, 0, len(extra))
	for _, item := range extra {
		name, value, ok := strings.Cut(item, "=")
		if !ok {
			cleaned = append(cleaned, item)
			continue
		}
		values[name] = value
		if name != ai.EnvCommitStrategy && name != "ACD_COMMIT_PRESET" {
			cleaned = append(cleaned, item)
		}
	}
	lookup := func(name string) (string, bool) {
		value, ok := values[name]
		return value, ok
	}
	overrides := config.Overrides{}
	overrides[config.FieldCommitStrategy], _ = json.Marshal("intent")
	overrides[config.FieldCommitPreset], _ = json.Marshal("fast")
	if _, configured := values[ai.EnvProvider]; !configured {
		overrides[config.FieldProvider], _ =
			json.Marshal("subprocess:missing-integration")
	}
	resolved, preset, err := config.ResolveAll(config.ResolveInput{
		Repository: overrides, LookupEnv: lookup,
	}, overrides)
	if err != nil {
		t.Fatalf("resolve Intent v2 integration revision: %v", err)
	}
	snapshot := map[string]any{
		"preset_id": preset.ID(), "preset_version": preset.Version(),
		"customized": preset.Customized,
	}
	for _, field := range config.Catalog() {
		if field.Boundary == config.ApplyHot && field.Persistable &&
			!field.Sensitive {
			snapshot[field.Name] = resolved[field.Name].EffectiveValue()
		}
	}
	provider := resolved[config.FieldProvider].EffectiveValue()
	confirmations := []string{string(ai.ConfirmationDiffEgress)}
	if strings.HasPrefix(provider, "subprocess:") {
		confirmations = append(confirmations,
			string(ai.ConfirmationSubprocessExecution))
	}
	if provider == "openai-compat" &&
		strings.TrimRight(resolved[config.FieldBaseURL].EffectiveValue(), "/") !=
			strings.TrimRight(ai.DefaultOpenAIBaseURL, "/") {
		confirmations = append(confirmations,
			string(ai.ConfirmationEndpointCredentials))
	}
	snapshot["confirmations"] = confirmations
	body, err := json.Marshal(snapshot)
	if err != nil {
		t.Fatalf("encode Intent v2 integration revision: %v", err)
	}
	dbPath := filepath.Join(repo, ".git", "acd", "state.db")
	db, err := state.Open(context.Background(), dbPath)
	if err != nil {
		t.Fatalf("open Intent v2 integration state: %v", err)
	}
	defer db.Close()
	revision, err := state.InsertConfigRevision(context.Background(), db,
		state.ConfigRevisionInput{
			Snapshot: body, Profile: "integration", Scope: "repository",
			SourceGeneration: 0, Reason: "integration Intent v2 fixture",
		})
	if err != nil {
		t.Fatalf("insert Intent v2 integration revision: %v", err)
	}
	request, ok, err := state.RequestConfigActivation(
		context.Background(), db, revision.ID, sql.NullInt64{})
	if err != nil || !ok {
		t.Fatalf("request Intent v2 integration revision: ok=%v err=%v",
			ok, err)
	}
	if ok, err := state.AcknowledgeConfigActivation(
		context.Background(), db, request.ID, revision.ID); err != nil || !ok {
		t.Fatalf("acknowledge Intent v2 integration revision: ok=%v err=%v",
			ok, err)
	}
	if ok, err := state.ApplyConfigActivation(
		context.Background(), db, request.ID, revision.ID); err != nil || !ok {
		t.Fatalf("apply Intent v2 integration revision: ok=%v err=%v",
			ok, err)
	}
	return cleaned
}

func assertIntentV2RuntimeActive(t *testing.T, repo string) {
	t.Helper()
	dbPath := filepath.Join(repo, ".git", "acd", "state.db")
	got := sqliteScalar(t, dbPath, `
SELECT COALESCE((SELECT value FROM daemon_meta
                 WHERE key='intent.v2.migration_state'), '') || '|' ||
       COALESCE((SELECT value FROM daemon_meta
                 WHERE key='intent.v2.preset_id'), '') || '|' ||
       COALESCE((SELECT value FROM daemon_meta
                 WHERE key='intent.v2.needs_attention'), '')`)
	if !strings.HasPrefix(got, "active|intent.fast|") ||
		strings.TrimPrefix(got, "active|intent.fast|") != "" {
		t.Fatalf("Intent v2 runtime is not active: %q", got)
	}
}

// acdBinaryPath is the per-process build cache. We compile the `acd` binary
// once and reuse it across every integration scenario. The directory is
// registered for cleanup in TestMain so /tmp does not accumulate stale
// builds across `go test` runs.
var (
	acdBinaryOnce sync.Once
	acdBinary     string
	acdBinaryDir  string
	acdBinaryErr  error

	repoTemplateOnce sync.Once
	repoTemplateDir  string
	repoTemplateErr  error
)

// TestMain removes package-scoped binary and repository fixtures after the
// suite completes so /tmp stays clean.
func TestMain(m *testing.M) {
	code := m.Run()
	if acdBinaryDir != "" {
		_ = os.RemoveAll(acdBinaryDir)
	}
	if repoTemplateDir != "" {
		_ = os.RemoveAll(repoTemplateDir)
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
	repoTemplateOnce.Do(initRepoTemplate)
	if repoTemplateErr != nil {
		t.Fatalf("initialize integration repo template: %v", repoTemplateErr)
	}

	dir := t.TempDir()
	if err := copyRepoTemplate(repoTemplateDir, dir); err != nil {
		t.Fatalf("materialize integration repo template: %v", err)
	}
	return dir
}

func initRepoTemplate() {
	dir, err := os.MkdirTemp("", "acd-integration-repo-template-*")
	if err != nil {
		repoTemplateErr = err
		return
	}
	repoTemplateDir = dir

	if out, err := exec.Command("git", "init", "-q", dir).CombinedOutput(); err != nil {
		repoTemplateErr = fmt.Errorf("git init: %w\n%s", err, out)
		return
	}
	for _, args := range [][]string{
		{"symbolic-ref", "HEAD", "refs/heads/main"},
		{"config", "user.email", "acd-integration@example.com"},
		{"config", "user.name", "ACD Integration"},
		{"config", "commit.gpgsign", "false"},
	} {
		if out, err := runGit(dir, args...); err != nil {
			repoTemplateErr = fmt.Errorf("git %s: %w\n%s", strings.Join(args, " "), err, out)
			return
		}
	}
	if err := os.WriteFile(filepath.Join(dir, ".gitignore"), []byte("# acd integration seed\n"), 0o644); err != nil {
		repoTemplateErr = fmt.Errorf("write seed: %w", err)
		return
	}
	for _, args := range [][]string{{"add", ".gitignore"}, {"commit", "-q", "-m", "seed"}} {
		if out, err := runGit(dir, args...); err != nil {
			repoTemplateErr = fmt.Errorf("git %s: %w\n%s", strings.Join(args, " "), err, out)
			return
		}
	}
}

func copyRepoTemplate(src, dst string) error {
	return filepath.WalkDir(src, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		rel, err := filepath.Rel(src, path)
		if err != nil {
			return err
		}
		if rel == "." {
			return nil
		}
		target := filepath.Join(dst, rel)
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return os.MkdirAll(target, info.Mode().Perm())
		}
		if entry.Type()&os.ModeSymlink != 0 {
			linkTarget, err := os.Readlink(path)
			if err != nil {
				return err
			}
			return os.Symlink(linkTarget, target)
		}
		if !entry.Type().IsRegular() {
			return fmt.Errorf("unsupported fixture entry %s with mode %s", rel, info.Mode())
		}
		in, err := os.Open(path)
		if err != nil {
			return err
		}
		out, err := os.OpenFile(target, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, info.Mode().Perm())
		if err != nil {
			_ = in.Close()
			return err
		}
		_, copyErr := io.Copy(out, in)
		return errors.Join(copyErr, out.Close(), in.Close())
	})
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

type synchronizedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *synchronizedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *synchronizedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func (b *synchronizedBuffer) contains(value string) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return bytes.Contains(b.buf.Bytes(), []byte(value))
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

// runPTYCommand executes a command behind the host's script(1) PTY. The
// wrapper sets a real kernel window size before exec and can resize it during
// the session, which exercises Bubble Tea's SIGWINCH path rather than merely
// setting COLUMNS/LINES. Inputs are written after startup so full-screen
// initialization is observable before keyboard-only interaction begins.
func runPTYCommand(t *testing.T, ctx context.Context, env []string, cols, rows int, resizeCols, resizeRows int, input string, args ...string) ExecResult {
	t.Helper()
	if runtime.GOOS != "darwin" && runtime.GOOS != "linux" {
		t.Skip("PTY integration is supported on macOS and Linux")
	}
	if _, err := exec.LookPath("script"); err != nil {
		t.Skip("script(1) PTY utility required")
	}
	body := "stty cols " + strconv.Itoa(cols) + " rows " + strconv.Itoa(rows)
	minInputDelay := 850 * time.Millisecond
	if resizeCols > 0 && resizeRows > 0 {
		body += "; (sleep 0.9; stty cols " + strconv.Itoa(resizeCols) + " rows " + strconv.Itoa(resizeRows) + " < /dev/tty; kill -WINCH $$) & exec \"$@\""
		minInputDelay = 1400 * time.Millisecond
	} else {
		body += "; exec \"$@\""
	}
	commandArgs := append([]string{"/bin/sh", "-c", body, "sh"}, args...)
	var scriptArgs []string
	if runtime.GOOS == "darwin" {
		scriptArgs = append([]string{"-q", "/dev/null"}, commandArgs...)
	} else {
		scriptArgs = []string{"-q", "-c", shellJoin(commandArgs), "/dev/null"}
	}
	cmd := exec.CommandContext(ctx, "script", scriptArgs...)
	cmd.Env = env
	stdin, err := cmd.StdinPipe()
	if err != nil {
		t.Fatalf("PTY stdin: %v", err)
	}
	var output synchronizedBuffer
	cmd.Stdout = &output
	cmd.Stderr = &output
	startedAt := time.Now()
	if err := cmd.Start(); err != nil {
		t.Fatalf("start PTY command: %v", err)
	}
	readyDeadline := time.Now().Add(5 * time.Second)
	for !output.contains("ACD SETTINGS") && time.Now().Before(readyDeadline) && ctx.Err() == nil {
		time.Sleep(10 * time.Millisecond)
	}
	if remaining := minInputDelay - time.Since(startedAt); remaining > 0 {
		timer := time.NewTimer(remaining)
		select {
		case <-timer.C:
		case <-ctx.Done():
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
		}
	}
	if input != "" {
		for i, chunk := range strings.Split(input, "\x00") {
			if i > 0 {
				time.Sleep(700 * time.Millisecond)
			}
			_, _ = io.WriteString(stdin, chunk)
		}
	}
	_ = stdin.Close()
	err = cmd.Wait()
	exit := 0
	if err != nil {
		var ee *exec.ExitError
		if errors.As(err, &ee) {
			exit = ee.ExitCode()
		} else {
			exit = -1
		}
	}
	return ExecResult{Stdout: output.String(), ExitCode: exit}
}

func shellJoin(args []string) string {
	quoted := make([]string, len(args))
	for i, arg := range args {
		quoted[i] = "'" + strings.ReplaceAll(arg, "'", "'\\''") + "'"
	}
	return strings.Join(quoted, " ")
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

// SeedFlushRequests inserts n flush_requests rows in 'pending' status using a
// single batched INSERT. Used by populated-state startup tests to simulate a
// real-world AI-Assistant repo where the daemon was killed mid-burst.
func SeedFlushRequests(t *testing.T, dbPath string, n int) {
	t.Helper()
	if n <= 0 {
		return
	}
	var sb strings.Builder
	sb.WriteString("BEGIN; ")
	now := nowFloatSeconds()
	const chunk = 500
	for start := 0; start < n; start += chunk {
		end := start + chunk
		if end > n {
			end = n
		}
		sb.WriteString("INSERT INTO flush_requests(command, non_blocking, requested_ts, status) VALUES ")
		for i := start; i < end; i++ {
			if i > start {
				sb.WriteString(", ")
			}
			fmt.Fprintf(&sb, "('wake', 0, %f, 'pending')", now+float64(i)*1e-6)
		}
		sb.WriteString("; ")
	}
	sb.WriteString("COMMIT;")
	if out, err := exec.Command("sqlite3", dbPath, sb.String()).CombinedOutput(); err != nil {
		t.Fatalf("seed flush_requests: %v\n%s", err, out)
	}
}

// SeedDaemonClients inserts n stale daemon_clients rows whose watch_pid
// targets PIDs that are extremely unlikely to be alive. The fingerprints are
// distinct so the daemon's GC sweep evicts them on its next pass.
func SeedDaemonClients(t *testing.T, dbPath string, n int) {
	t.Helper()
	if n <= 0 {
		return
	}
	now := nowFloatSeconds()
	var sb strings.Builder
	sb.WriteString("BEGIN; ")
	for i := 0; i < n; i++ {
		// Use very high PIDs that are vanishingly unlikely to be live so the
		// GC sweep classifies the row as stale immediately.
		pid := 2000000 + i
		fmt.Fprintf(&sb,
			"INSERT OR REPLACE INTO daemon_clients(session_id, harness, watch_pid, watch_fp, registered_ts, last_seen_ts) "+
				"VALUES ('stale-client-%04d', 'shell', %d, 'stale|fp|%d', %f, %f); ",
			i, pid, i, now-3600, now-3600)
	}
	sb.WriteString("COMMIT;")
	if out, err := exec.Command("sqlite3", dbPath, sb.String()).CombinedOutput(); err != nil {
		t.Fatalf("seed daemon_clients: %v\n%s", err, out)
	}
}

// SeedShadowGenerations writes `generations` distinct (branch_ref, branch_generation)
// shadow rows of `rowsPerGen` size each. The newest generation matches the
// caller's intent for the active branch generation; older ones are present to
// stress retention/pruning and bootstrap idempotency. Paths are deterministic
// so the seeded state is reproducible across runs.
func SeedShadowGenerations(t *testing.T, dbPath, branchRef string, generations, rowsPerGen int) {
	t.Helper()
	if generations <= 0 || rowsPerGen <= 0 {
		return
	}
	now := nowFloatSeconds()
	const chunk = 400
	for gen := 1; gen <= generations; gen++ {
		// Every (gen,path) needs a unique 40-char OID. Use sha-shaped padding.
		baseHead := fmt.Sprintf("%040x", gen)
		for start := 0; start < rowsPerGen; start += chunk {
			end := start + chunk
			if end > rowsPerGen {
				end = rowsPerGen
			}
			var sb strings.Builder
			sb.WriteString("BEGIN; INSERT INTO shadow_paths(branch_ref, branch_generation, path, operation, mode, oid, base_head, fidelity, updated_ts) VALUES ")
			for i := start; i < end; i++ {
				if i > start {
					sb.WriteString(", ")
				}
				oid := fmt.Sprintf("%040x", (gen<<24)|(i+1))
				path := fmt.Sprintf("seed/g%d/p%05d.txt", gen, i)
				fmt.Fprintf(&sb, "(%s, %d, %s, 'create', '100644', '%s', '%s', 'rescan', %f)",
					sqliteLiteral(branchRef), gen, sqliteLiteral(path), oid, baseHead, now)
			}
			sb.WriteString("; COMMIT;")
			if out, err := exec.Command("sqlite3", dbPath, sb.String()).CombinedOutput(); err != nil {
				t.Fatalf("seed shadow_paths gen=%d: %v\n%s", gen, err, out)
			}
		}
	}
}

// readHeartbeatTs reads daemon_state.heartbeat_ts (or 0).
func readHeartbeatTs(repoDir string) float64 {
	dbPath := filepath.Join(repoDir, ".git", "acd", "state.db")
	if _, err := os.Stat(dbPath); err != nil {
		return 0
	}
	out, err := exec.Command("sqlite3", dbPath, "SELECT heartbeat_ts FROM daemon_state WHERE id = 1").CombinedOutput()
	if err != nil {
		return 0
	}
	v := strings.TrimSpace(string(out))
	var f float64
	fmt.Sscanf(v, "%f", &f)
	return f
}

// initStateDBSchema brings <repo>/.git/acd/state.db into existence with the
// canonical schema applied. The integration suite cannot import the internal
// state package, so we use the production `acd` binary itself: a brief
// `acd start` + `acd stop` cycle migrates the schema, after which we are
// free to seed arbitrary rows for the populated-state scenarios.
//
// Returns the absolute path to the state.db.
func initStateDBSchema(t *testing.T, ctx context.Context, env []string, repo, sessionID string) string {
	t.Helper()
	startSession(t, ctx, env, repo, sessionID, "shell")
	waitMode(t, repo, "running", 5*time.Second)
	stopSessionForce(t, env, repo)
	waitMode(t, repo, "stopped", 5*time.Second)
	dbPath := filepath.Join(repo, ".git", "acd", "state.db")
	if _, err := os.Stat(dbPath); err != nil {
		t.Fatalf("state.db not created by start/stop bootstrap: %v", err)
	}
	return dbPath
}
