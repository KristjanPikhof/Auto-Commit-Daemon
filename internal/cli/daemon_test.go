package cli

import (
	"bytes"
	"os"
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

// (paths.RepoHash is a pure path hash and does not stat the filesystem,
// so a "bad repo path" degrade test isn't meaningful here. RepoHash
// failure paths are exercised in internal/paths.)
