//go:build integration
// +build integration

package integration_test

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
	"testing"
	"time"
)

// commitAllFixture seeds a tempRepo with a deterministic dirty worktree:
// many uncommitted files spread across multiple directories with sibling
// clusters, so the planner sees coherent windows. Returns the repo dir and
// the lexicographically sorted list of expected paths.
func commitAllFixture(t *testing.T) (string, []string) {
	t.Helper()
	repo := tempRepo(t)
	files := []string{
		"cmd/main.go",
		"docs/a.md",
		"docs/b.md",
		"pkg/a/x.go",
		"pkg/a/y.go",
		"pkg/a/z.go",
		"pkg/b/x.go",
		"pkg/b/y.go",
		"pkg/b/z.go",
	}
	// Defensive: lex-sort regardless of source ordering.
	sort.Strings(files)
	for i, rel := range files {
		writeFile(t, filepath.Join(repo, rel), "// "+rel+"\n// content "+itoa(i)+"\n")
	}
	return repo, files
}

func itoa(i int) string {
	const digits = "0123456789"
	if i == 0 {
		return "0"
	}
	negative := i < 0
	if negative {
		i = -i
	}
	var buf [20]byte
	pos := len(buf)
	for i > 0 {
		pos--
		buf[pos] = digits[i%10]
		i /= 10
	}
	if negative {
		pos--
		buf[pos] = '-'
	}
	return string(buf[pos:])
}

// commitsTouchingPath returns commit OIDs (oldest-first) that touched path.
// Walks `git log --reverse --name-only HEAD` so we observe the on-disk order
// commit-all produced.
func commitsTouchingPath(t *testing.T, repo, path string) []string {
	t.Helper()
	out := runGitOK(t, repo, "log", "--reverse", "--name-only", "--pretty=format:COMMIT %H", "HEAD")
	var hits []string
	current := ""
	for _, line := range strings.Split(out, "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "COMMIT ") {
			current = strings.TrimPrefix(line, "COMMIT ")
			continue
		}
		if line == path {
			hits = append(hits, current)
		}
	}
	return hits
}

// allCommitFiles returns, oldest-first, the set of (commit_oid -> [paths])
// pairs and the flat ordered list of paths as they appear across the history
// past the seed commit.
func allCommitFiles(t *testing.T, repo string) (orderedCommits []string, perCommit map[string][]string) {
	t.Helper()
	out := runGitOK(t, repo, "log", "--reverse", "--name-only", "--pretty=format:COMMIT %H", "HEAD")
	perCommit = map[string][]string{}
	current := ""
	for _, line := range strings.Split(out, "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		if strings.HasPrefix(line, "COMMIT ") {
			current = strings.TrimPrefix(line, "COMMIT ")
			orderedCommits = append(orderedCommits, current)
			continue
		}
		perCommit[current] = append(perCommit[current], line)
	}
	return orderedCommits, perCommit
}

// commitAllEnv returns withIsolatedHome augmented with the requested commit
// strategy / provider knobs. We always pin ACD_AI_PROVIDER explicitly so
// host-level env (or absence thereof) cannot perturb the test outcome.
func commitAllEnv(t *testing.T, strategy, provider string) []string {
	t.Helper()
	base := withIsolatedHome(t)
	extras := []string{
		"ACD_COMMIT_STRATEGY=" + strategy,
		"ACD_AI_PROVIDER=" + provider,
	}
	return envWith(base, extras...)
}

// TestCommitAllEventStrategyOrdersByPath: with strategy=event, every dirty
// file becomes its own commit and commit ordering follows lex(path).
func TestCommitAllEventStrategyOrdersByPath(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git binary required")
	}
	repo, files := commitAllFixture(t)
	env := commitAllEnv(t, "event", "deterministic")

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	seedCommitCount := commitCount(t, repo)

	res := runAcd(t, ctx, env, "commit-all", "--repo", repo, "--yes")
	if res.ExitCode != 0 {
		t.Fatalf("commit-all exit=%d\nstdout=%s\nstderr=%s", res.ExitCode, res.Stdout, res.Stderr)
	}

	// Worktree must be clean.
	if dirty := strings.TrimSpace(runGitOK(t, repo, "status", "--porcelain")); dirty != "" {
		t.Fatalf("worktree still dirty after commit-all:\n%s", dirty)
	}

	finalCount := commitCount(t, repo)
	wantCount := seedCommitCount + len(files)
	if finalCount != wantCount {
		t.Fatalf("event-strategy commit count=%d want=%d (seed=%d files=%d)",
			finalCount, wantCount, seedCommitCount, len(files))
	}

	orderedCommits, perCommit := allCommitFiles(t, repo)
	// Skip the seed commit (first); inspect only the post-seed history.
	if len(orderedCommits) < seedCommitCount {
		t.Fatalf("history shorter than seedCount=%d: got %d commits", seedCommitCount, len(orderedCommits))
	}
	postSeed := orderedCommits[seedCommitCount:]
	if len(postSeed) != len(files) {
		t.Fatalf("post-seed commit count=%d want=%d", len(postSeed), len(files))
	}

	// Each post-seed commit must touch exactly one of our fixture paths,
	// and the order must be lex(path).
	wantOrder := append([]string(nil), files...)
	sort.Strings(wantOrder)
	gotOrder := make([]string, 0, len(postSeed))
	for _, c := range postSeed {
		paths := perCommit[c]
		if len(paths) != 1 {
			t.Fatalf("event-strategy commit %s touched %d files, want 1: %v", c, len(paths), paths)
		}
		gotOrder = append(gotOrder, paths[0])
	}
	for i, want := range wantOrder {
		if gotOrder[i] != want {
			t.Fatalf("event-strategy commit ordering mismatch at idx=%d:\nwant=%v\ngot =%v", i, wantOrder, gotOrder)
		}
	}
}

// TestCommitAllIntentStrategyDeterministic: with strategy=intent and the
// deterministic AI provider, every dirty file ends up committed and no file
// is left dirty. The deterministic planner currently emits one-event plans,
// but commit-all must still sweep every offered seq to terminal published.
func TestCommitAllIntentStrategyDeterministic(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git binary required")
	}
	repo, files := commitAllFixture(t)
	env := commitAllEnv(t, "intent", "deterministic")

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	seedCommitCount := commitCount(t, repo)

	res := runAcd(t, ctx, env, "commit-all", "--repo", repo, "--yes")
	if res.ExitCode != 0 {
		t.Fatalf("commit-all (intent) exit=%d\nstdout=%s\nstderr=%s", res.ExitCode, res.Stdout, res.Stderr)
	}

	if dirty := strings.TrimSpace(runGitOK(t, repo, "status", "--porcelain")); dirty != "" {
		t.Fatalf("worktree still dirty after commit-all (intent):\n%s", dirty)
	}

	// All fixture files must be present in the post-seed history. Grouped
	// commits are allowed; we only assert that no file was dropped.
	orderedCommits, perCommit := allCommitFiles(t, repo)
	if len(orderedCommits) <= seedCommitCount {
		t.Fatalf("intent strategy produced no new commits beyond seed (seed=%d, total=%d)",
			seedCommitCount, len(orderedCommits))
	}
	committed := map[string]bool{}
	for _, c := range orderedCommits[seedCommitCount:] {
		for _, p := range perCommit[c] {
			committed[p] = true
		}
	}
	for _, want := range files {
		if !committed[want] {
			t.Fatalf("intent strategy did not commit %q; committed=%v", want, sortedKeys(committed))
		}
	}

	// At least as many commits as planner-style passes — bounded above by
	// number of files. We don't constrain the exact count because grouping
	// is provider-defined.
	postSeed := len(orderedCommits) - seedCommitCount
	if postSeed < 1 || postSeed > len(files) {
		t.Fatalf("intent post-seed commit count=%d out of [1, %d]", postSeed, len(files))
	}
}

func sortedKeys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// TestCommitAllRefusesWhenDaemonAlive: simulate the live daemon by holding
// <gitDir>/acd/daemon.lock with LOCK_EX|LOCK_NB. commit-all must refuse
// with a non-zero exit and a stderr message that names the daemon lock.
func TestCommitAllRefusesWhenDaemonAlive(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git binary required")
	}
	repo, _ := commitAllFixture(t)
	env := commitAllEnv(t, "event", "deterministic")

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Hold the daemon lock ourselves. That mirrors the live-daemon refusal
	// path without requiring a full session boot.
	lockDir := filepath.Join(repo, ".git", "acd")
	if err := os.MkdirAll(lockDir, 0o700); err != nil {
		t.Fatalf("mkdir lock dir: %v", err)
	}
	lockPath := filepath.Join(lockDir, "daemon.lock")
	f, err := os.OpenFile(lockPath, os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		t.Fatalf("open daemon.lock: %v", err)
	}
	defer f.Close()
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		t.Fatalf("flock daemon.lock: %v", err)
	}
	defer func() { _ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN) }()

	res := runAcd(t, ctx, env, "commit-all", "--repo", repo, "--yes")
	if res.ExitCode == 0 {
		t.Fatalf("commit-all should have refused while daemon.lock is held; stdout=%s\nstderr=%s",
			res.Stdout, res.Stderr)
	}
	combined := res.Stdout + res.Stderr
	if !strings.Contains(strings.ToLower(combined), "daemon") {
		t.Fatalf("expected stderr/stdout to mention daemon lock; got:\nstdout=%s\nstderr=%s",
			res.Stdout, res.Stderr)
	}
	// And the worktree must be entirely untouched.
	dirtyLines := strings.Split(strings.TrimSpace(runGitOK(t, repo, "status", "--porcelain")), "\n")
	if len(dirtyLines) == 0 || (len(dirtyLines) == 1 && dirtyLines[0] == "") {
		t.Fatalf("expected fixture files still dirty after refusal; got clean worktree")
	}
}

// TestCommitAllRefusesOnDetachedHEAD: detach HEAD, then assert commit-all
// refuses and mentions the detached state.
func TestCommitAllRefusesOnDetachedHEAD(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git binary required")
	}
	repo, _ := commitAllFixture(t)
	env := commitAllEnv(t, "event", "deterministic")

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Detach HEAD onto the seed commit's SHA.
	headSHA := strings.TrimSpace(runGitOK(t, repo, "rev-parse", "HEAD"))
	runGitOK(t, repo, "checkout", "--quiet", "--detach", headSHA)

	res := runAcd(t, ctx, env, "commit-all", "--repo", repo, "--yes")
	if res.ExitCode == 0 {
		t.Fatalf("commit-all should refuse on detached HEAD; stdout=%s\nstderr=%s",
			res.Stdout, res.Stderr)
	}
	combined := strings.ToLower(res.Stdout + res.Stderr)
	if !strings.Contains(combined, "detached") {
		t.Fatalf("expected refusal message to mention detached HEAD; got:\nstdout=%s\nstderr=%s",
			res.Stdout, res.Stderr)
	}
}
