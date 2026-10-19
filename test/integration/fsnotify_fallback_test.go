//go:build integration

package integration_test

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func readDaemonMetaKey(repo, key string) string {
	out, err := exec.Command("sqlite3", filepath.Join(repo, ".git", "acd", "state.db"), "SELECT value FROM daemon_meta WHERE key="+sqliteLiteral(key)).CombinedOutput()
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(out))
}
func waitMetaValue(t *testing.T, repo, key, want string, timeout time.Duration) {
	t.Helper()
	waitFor(t, fmt.Sprintf("%s=%s", key, want), timeout, func() bool { return readDaemonMetaKey(repo, key) == want })
}
func finishWaitProtectedFile(t *testing.T, f *finishFixture, path, body string, timeout time.Duration) {
	t.Helper()
	waitFor(t, "protected bytes for "+path, timeout, func() bool {
		ref := readDaemonStateScalar(f.Repo, "SELECT checkpoint_ref FROM checkpoints WHERE phase='completed' AND coverage_complete=1 AND retained=1 ORDER BY seq DESC LIMIT 1")
		if ref == "" {
			return false
		}
		got, err := runGit(f.Repo, "show", ref+":"+path)
		return err == nil && got == body
	})
}
func TestFinishFaultFsnotifyBudgetFallback(t *testing.T) {
	for _, runtimeBudget := range []bool{false, true} {
		t.Run(fmt.Sprintf("runtime=%t", runtimeBudget), func(t *testing.T) {
			t.Parallel()
			f := newFinishFixture(t, "ACD_MAX_INOTIFY_WATCHES=3")
			if runtimeBudget {
				waitMetaValue(t, f.Repo, "fsnotify.mode", "fsnotify", 5*time.Second)
			}
			for i := 0; i < 8; i++ {
				writeFile(t, filepath.Join(f.Repo, fmt.Sprintf("fanout/d%d/file.txt", i)), "new tree\n")
			}
			if !runtimeBudget {
				f.Restart(t, t.Context())
			}
			waitMetaValue(t, f.Repo, "fsnotify.mode", "poll", 10*time.Second)
			if got := readDaemonMetaKey(f.Repo, "fsnotify.fallback_reason"); got != "watch_budget_exceeded" {
				t.Fatalf("fallback reason %q", got)
			}
			writeFile(t, filepath.Join(f.Repo, "protected.txt"), "fallback work\n")
			f.Barrier(t, t.Context())
			finishWaitProtectedFile(t, f, "protected.txt", "fallback work\n", 5*time.Second)
			if strings.TrimSpace(runGitOK(t, f.Repo, "rev-parse", "HEAD")) != f.BaseHead {
				t.Fatal("fallback published work")
			}
		})
	}
}
func TestFinishFaultFsnotifyDisabledFallback(t *testing.T) {
	t.Parallel()
	f := newFinishFixture(t, "ACD_DISABLE_FSNOTIFY=1")
	waitMetaValue(t, f.Repo, "fsnotify.mode", "poll", 5*time.Second)
	if got := readDaemonMetaKey(f.Repo, "fsnotify.fallback_reason"); got != "disabled_by_env" {
		t.Fatalf("fallback reason %q", got)
	}
	writeFile(t, filepath.Join(f.Repo, "protected.txt"), "poll protection\n")
	f.Barrier(t, t.Context())
	finishWaitProtectedFile(t, f, "protected.txt", "poll protection\n", 5*time.Second)
}
func TestFinishFaultFsnotifyProtectsWithoutHint(t *testing.T) {
	t.Parallel()
	f := newFinishFixture(t)
	waitMetaValue(t, f.Repo, "fsnotify.mode", "fsnotify", 5*time.Second)
	budget := 4 * time.Second
	if os.Getenv("ACD_PERF_TEST") == "1" {
		budget = 1500 * time.Millisecond
	}
	writeFile(t, filepath.Join(f.Repo, "fast.txt"), "event-driven protection\n")
	finishWaitProtectedFile(t, f, "fast.txt", "event-driven protection\n", budget)
	if strings.TrimSpace(runGitOK(t, f.Repo, "rev-parse", "HEAD")) != f.BaseHead {
		t.Fatal("watch wake published work")
	}
}
