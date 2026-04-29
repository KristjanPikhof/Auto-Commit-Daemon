package cli

import (
	"bytes"
	"os"
	"strings"
	"testing"
)

// TestBuildDaemonRunOptions_WiresCentralStats pins the regression where
// runDaemon failed to populate Options.CentralStatsDBPath /
// Options.RepoHash. With either field empty the daemon's rollup pass
// silently skips central.PushRollupsToCentral, leaving stats.db empty
// and `acd stats` reporting zero across all repos forever.
//
// We exercise the helper directly so the test stays fast and does not
// have to spin the daemon loop. The DB handle can be nil here — the
// helper does not dereference it.
func TestBuildDaemonRunOptions_WiresCentralStats(t *testing.T) {
	roots := withIsolatedHome(t)
	repoDir, _, db := makeRepoStateDB(t)

	var errBuf bytes.Buffer
	opts := buildDaemonRunOptions(repoDir, repoDir+"/.git", db, &errBuf)

	if opts.RepoPath != repoDir {
		t.Fatalf("RepoPath = %q, want %q", opts.RepoPath, repoDir)
	}
	if opts.GitDir == "" {
		t.Fatalf("GitDir empty")
	}
	if opts.DB != db {
		t.Fatalf("DB handle not propagated")
	}
	if opts.RepoHash == "" {
		t.Fatalf("RepoHash empty — `acd stats` push skipped (errOut=%q)", errBuf.String())
	}
	if opts.CentralStatsDBPath == "" {
		t.Fatalf("CentralStatsDBPath empty — `acd stats` push skipped (errOut=%q)", errBuf.String())
	}
	if want := roots.StatsDBPath(); opts.CentralStatsDBPath != want {
		t.Fatalf("CentralStatsDBPath = %q, want %q", opts.CentralStatsDBPath, want)
	}
	if errBuf.Len() != 0 {
		t.Fatalf("expected no errOut for healthy resolution, got %q", errBuf.String())
	}
}

// TestBuildDaemonRunOptions_FsnotifyEnvToggle confirms the env gate is
// read into the FsnotifyEnabled field — kept alongside the stats wiring
// test so both `daemon run` Options invariants live in one place.
func TestBuildDaemonRunOptions_FsnotifyEnvToggle(t *testing.T) {
	repoDir, _, db := makeRepoStateDB(t)

	for _, tc := range []struct {
		env  string
		want bool
	}{
		{env: "", want: false},
		{env: "0", want: false},
		{env: "false", want: false},
		{env: "1", want: true},
		{env: "true", want: true},
		{env: "yes", want: true},
	} {
		t.Run("env="+tc.env, func(t *testing.T) {
			withIsolatedHome(t)
			t.Setenv("ACD_FSNOTIFY_ENABLED", tc.env)
			opts := buildDaemonRunOptions(repoDir, repoDir+"/.git", db, os.Stderr)
			if opts.FsnotifyEnabled != tc.want {
				t.Fatalf("FsnotifyEnabled = %v, want %v (env=%q)", opts.FsnotifyEnabled, tc.want, tc.env)
			}
		})
	}
}

// TestBuildDaemonRunOptions_BadRepoLogsAndDegrades verifies that a
// non-existent repo path causes RepoHash to be empty (since
// paths.RepoHash hashes a real path) but the helper does NOT panic and
// records a diagnostic on errOut so the daemon can still boot. Stats
// will be silently disabled — the run loop tolerates this.
func TestBuildDaemonRunOptions_BadRepoLogsAndDegrades(t *testing.T) {
	withIsolatedHome(t)
	_, _, db := makeRepoStateDB(t)

	var errBuf bytes.Buffer
	// /nonexistent guarantees repo-hash failure on every supported
	// platform; resolve-paths still succeeds because it depends only
	// on $HOME (which withIsolatedHome stamps).
	opts := buildDaemonRunOptions("/nonexistent/path/for/test", "/nonexistent/path/for/test/.git", db, &errBuf)

	if opts.CentralStatsDBPath == "" {
		t.Fatalf("CentralStatsDBPath should still resolve from isolated home, got empty (errOut=%q)", errBuf.String())
	}
	if opts.RepoHash != "" {
		t.Fatalf("RepoHash should be empty when repo path resolution fails, got %q", opts.RepoHash)
	}
	if !strings.Contains(errBuf.String(), "repo hash") {
		t.Fatalf("expected errOut to mention repo hash failure, got %q", errBuf.String())
	}
}
