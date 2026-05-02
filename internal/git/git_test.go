package git

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// requireGit is the gate every test funnels through: skip cleanly when the
// system git binary is missing, so the test suite stays usable on stripped
// CI images.
func requireGit(t *testing.T) {
	t.Helper()
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git binary not on PATH; skipping")
	}
}

// initRepo runs `git init` in a fresh tmpdir and configures the minimum
// identity bits commit-tree needs. Returns the repo root.
func initRepo(t *testing.T) string {
	t.Helper()
	requireGit(t)
	dir := t.TempDir()
	ctx := context.Background()
	if err := Init(ctx, dir); err != nil {
		t.Fatalf("git init: %v", err)
	}
	// commit-tree refuses to run without an author; configure repo-local
	// values so we don't rely on the host's ~/.gitconfig.
	for _, kv := range [][2]string{
		{"user.email", "acd-test@example.com"},
		{"user.name", "ACD Test"},
		{"commit.gpgsign", "false"},
	} {
		if _, err := Run(ctx, RunOpts{Dir: dir}, "config", kv[0], kv[1]); err != nil {
			t.Fatalf("git config %s: %v", kv[0], err)
		}
	}
	return dir
}

func TestRunReturnsTypedErrorOnNonZeroExit(t *testing.T) {
	requireGit(t)
	dir := t.TempDir()
	_, err := Run(context.Background(), RunOpts{Dir: dir}, "rev-parse", "HEAD")
	if err == nil {
		t.Fatalf("expected error for rev-parse outside a repo")
	}
	gerr, ok := err.(*Error)
	if !ok {
		t.Fatalf("expected *git.Error, got %T: %v", err, err)
	}
	if gerr.ExitCode == 0 {
		t.Fatalf("expected non-zero exit, got %d", gerr.ExitCode)
	}
	if gerr.Stderr == "" {
		t.Fatalf("expected stderr captured, got empty")
	}
}

func TestRunRespectsContextCancellation(t *testing.T) {
	requireGit(t)
	dir := initRepo(t)
	// `git wait-for-pack` does not exist; a long-running invocation we can
	// kill is `git --version` after a sleep — instead we cancel a hung
	// stdin reader by using `hash-object --stdin` with a context that
	// expires before we send any input.
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("pipe: %v", err)
	}
	t.Cleanup(func() { _ = r.Close(); _ = w.Close() })

	start := time.Now()
	_, err = Run(ctx, RunOpts{Dir: dir, Stdin: r}, "hash-object", "-w", "--stdin")
	elapsed := time.Since(start)
	if err == nil {
		t.Fatalf("expected error from cancelled context")
	}
	if elapsed > 2*time.Second {
		t.Fatalf("ctx cancel did not kill child quickly (took %s)", elapsed)
	}
}

func TestRun_TimeoutKillsHungGit(t *testing.T) {
	requireGit(t)
	dir := initRepo(t)

	// hash-object --stdin blocks until stdin is closed; pipe with no
	// writer-close + a never-cancelled background ctx exercises the
	// per-call RunOpts.Timeout path specifically.
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("pipe: %v", err)
	}
	t.Cleanup(func() { _ = r.Close(); _ = w.Close() })

	const budget = 250 * time.Millisecond
	start := time.Now()
	_, err = Run(context.Background(), RunOpts{
		Dir:     dir,
		Stdin:   r,
		Timeout: budget,
	}, "hash-object", "-w", "--stdin")
	elapsed := time.Since(start)
	if err == nil {
		t.Fatalf("expected error from per-call timeout")
	}
	// Allow generous jitter for slow CI runners but assert we did not
	// wait anywhere near forever.
	if elapsed > budget+5*time.Second {
		t.Fatalf("timeout did not kill child quickly (took %s, budget %s)", elapsed, budget)
	}
}

func TestRun_ZeroTimeoutDoesNotImposeDeadline(t *testing.T) {
	requireGit(t)
	dir := initRepo(t)
	// A trivial command must complete normally with Timeout=0 (default).
	out, err := Run(context.Background(), RunOpts{Dir: dir}, "rev-parse", "--is-inside-work-tree")
	if err != nil {
		t.Fatalf("rev-parse: %v", err)
	}
	if strings.TrimSpace(string(out)) != "true" {
		t.Fatalf("expected 'true', got %q", string(out))
	}
}

func TestScrubEnvKeepsLCAllC(t *testing.T) {
	requireGit(t)
	// Even when the host overrides LC_ALL, the scrubbed env must force C
	// so we can parse git output deterministically.
	t.Setenv("LC_ALL", "fr_FR.UTF-8")
	env := scrubEnv(nil)
	joined := strings.Join(env, "\x00")
	if !strings.Contains(joined, "LC_ALL=C") {
		t.Fatalf("expected LC_ALL=C in scrubbed env, got %v", env)
	}
	if strings.Contains(joined, "LC_ALL=fr_FR.UTF-8") {
		t.Fatalf("host LC_ALL leaked: %v", env)
	}
	// Only one LC_ALL entry should appear (scrub appends our forced
	// value, not the host one).
	count := 0
	for _, kv := range env {
		if strings.HasPrefix(kv, "LC_ALL=") {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("expected exactly one LC_ALL entry, got %d: %v", count, env)
	}
}

func TestScrubEnvDropsGitVarsButKeepsAllowlist(t *testing.T) {
	requireGit(t)
	// Set a sentinel GIT_* var on the host; if the scrub fails, the child
	// process will see it and we'll observe it via `git --exec-path` env
	// inspection. We use `git env--helper` if available; fall back to a
	// portable check via printing PATH from a printenv-ish probe.
	t.Setenv("GIT_AUTHOR_NAME", "leaked")
	t.Setenv("GIT_DIR", "/should/not/leak")
	t.Setenv("PATH", os.Getenv("PATH"))

	env := scrubEnv(nil)
	joined := strings.Join(env, "\x00")
	if strings.Contains(joined, "GIT_AUTHOR_NAME=leaked") {
		t.Fatalf("GIT_AUTHOR_NAME leaked into scrubbed env: %v", env)
	}
	if strings.Contains(joined, "GIT_DIR=/should/not/leak") {
		t.Fatalf("GIT_DIR leaked into scrubbed env: %v", env)
	}
	if !strings.Contains(joined, "GIT_TERMINAL_PROMPT=0") {
		t.Fatalf("expected GIT_TERMINAL_PROMPT=0 in scrubbed env, got %v", env)
	}
	if !strings.Contains(joined, "GIT_OPTIONAL_LOCKS=0") {
		t.Fatalf("expected GIT_OPTIONAL_LOCKS=0 in scrubbed env, got %v", env)
	}
	if !strings.Contains(joined, "LC_ALL=C") {
		t.Fatalf("expected LC_ALL=C in scrubbed env, got %v", env)
	}
	if !strings.Contains(joined, "PATH=") {
		t.Fatalf("expected PATH preserved in scrubbed env, got %v", env)
	}
}

func TestScrubEnvSurvivesEndToEnd(t *testing.T) {
	requireGit(t)
	dir := initRepo(t)
	// If the scrub failed and GIT_DIR=/bogus leaked through, every git
	// call below would fail with "Not a git repository". Set the var on
	// the test process and confirm git still resolves the test repo.
	t.Setenv("GIT_DIR", filepath.Join(t.TempDir(), "bogus"))
	out, err := Run(context.Background(), RunOpts{Dir: dir}, "rev-parse", "--is-inside-work-tree")
	if err != nil {
		t.Fatalf("rev-parse with GIT_DIR set on host: %v", err)
	}
	if strings.TrimSpace(string(out)) != "true" {
		t.Fatalf("expected 'true' inside work tree, got %q", string(out))
	}
}
