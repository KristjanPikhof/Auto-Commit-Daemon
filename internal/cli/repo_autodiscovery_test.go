package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/central"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/paths"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

func disableRepoAutodiscovery(t *testing.T) paths.Roots {
	t.Helper()
	roots := withIsolatedHome(t)
	t.Setenv("ACD_REPO_AUTODISCOVERY", "0")
	return roots
}

func assertNoRepoStateOrRegistry(t *testing.T, roots paths.Roots, repoDir string) {
	t.Helper()
	if _, err := os.Stat(filepath.Join(repoDir, ".git", "acd")); !os.IsNotExist(err) {
		t.Fatalf(".git/acd stat err=%v, want not exist", err)
	}
	if _, err := os.Stat(roots.RegistryPath()); !os.IsNotExist(err) {
		t.Fatalf("registry stat err=%v, want not exist", err)
	}
}

func assertNoRepoState(t *testing.T, repoDir string) {
	t.Helper()
	if _, err := os.Stat(filepath.Join(repoDir, ".git", "acd")); !os.IsNotExist(err) {
		t.Fatalf(".git/acd stat err=%v, want not exist", err)
	}
}

func registerDisabledRepo(t *testing.T, roots paths.Roots, repoDir string) {
	t.Helper()
	stateDB := state.DBPathFromGitDir(filepath.Join(repoDir, ".git"))
	if err := central.WithLock(roots, func(reg *central.Registry) error {
		reg.UpsertRepo(repoDir, "disabled-hash", stateDB, "codex", 10)
		reg.DisableRepo(central.RepoRemovalTarget{Path: repoDir}, 20)
		return nil
	}); err != nil {
		t.Fatalf("register disabled repo: %v", err)
	}
}

func TestWakeTouchFlush_DisabledRepoSkipsWithoutState(t *testing.T) {
	for _, tc := range []struct {
		name       string
		run        func(context.Context, *bytes.Buffer, string) error
		wantReason string
	}{
		{
			name: "wake",
			run: func(ctx context.Context, out *bytes.Buffer, repoDir string) error {
				return runWake(ctx, out, repoDir, "session-wake", true)
			},
			wantReason: repoAutodiscoverySkipRepoDisabled,
		},
		{
			name: "touch",
			run: func(ctx context.Context, out *bytes.Buffer, repoDir string) error {
				return runTouch(ctx, out, repoDir, "session-touch", true)
			},
			wantReason: repoAutodiscoverySkipRepoDisabled,
		},
		{
			name: "flush-heartbeat",
			run: func(ctx context.Context, out *bytes.Buffer, repoDir string) error {
				return runFlush(ctx, out, repoDir, "session-flush", false, true)
			},
			wantReason: repoAutodiscoverySkipRepoDisabled,
		},
		{
			name: "flush-logical",
			run: func(ctx context.Context, out *bytes.Buffer, repoDir string) error {
				return runFlush(ctx, out, repoDir, "session-flush", true, true)
			},
			wantReason: repoAutodiscoverySkipRepoDisabled,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			roots := withIsolatedHome(t)
			repoDir := makeUnregisteredStartRepo(t)
			registerDisabledRepo(t, roots, repoDir)
			var out bytes.Buffer
			if err := tc.run(context.Background(), &out, repoDir); err != nil {
				t.Fatalf("%s: %v", tc.name, err)
			}
			if !strings.Contains(out.String(), `"skipped": true`) || !strings.Contains(out.String(), tc.wantReason) {
				t.Fatalf("output=%s, want skipped %s", out.String(), tc.wantReason)
			}
			assertNoRepoState(t, repoDir)
		})
	}
}

func TestWakeTouchFlush_AutodiscoveryDisabledUnregisteredSkipsWithoutState(t *testing.T) {
	for _, tc := range []struct {
		name string
		run  func(context.Context, *bytes.Buffer, string) error
	}{
		{
			name: "wake",
			run: func(ctx context.Context, out *bytes.Buffer, repoDir string) error {
				return runWake(ctx, out, repoDir, "session-wake", true)
			},
		},
		{
			name: "touch",
			run: func(ctx context.Context, out *bytes.Buffer, repoDir string) error {
				return runTouch(ctx, out, repoDir, "session-touch", true)
			},
		},
		{
			name: "flush-heartbeat",
			run: func(ctx context.Context, out *bytes.Buffer, repoDir string) error {
				return runFlush(ctx, out, repoDir, "session-flush", false, true)
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			roots := disableRepoAutodiscovery(t)
			repoDir := makeUnregisteredStartRepo(t)
			var out bytes.Buffer
			if err := tc.run(context.Background(), &out, repoDir); err != nil {
				t.Fatalf("%s: %v", tc.name, err)
			}
			if !strings.Contains(out.String(), `"skipped": true`) || !strings.Contains(out.String(), repoAutodiscoverySkipDisabled) {
				t.Fatalf("output=%s, want skipped autodiscovery_disabled", out.String())
			}
			assertNoRepoStateOrRegistry(t, roots, repoDir)
		})
	}
}

func TestFlushLogical_AutodiscoveryDisabledUnregisteredRefusesUnknownSessionWithoutState(t *testing.T) {
	roots := disableRepoAutodiscovery(t)
	repoDir := makeUnregisteredStartRepo(t)

	var out bytes.Buffer
	if err := runFlush(context.Background(), &out, repoDir, "missing-session", true, true); err != nil {
		t.Fatalf("runFlush logical: %v", err)
	}
	var got flushResult
	if err := json.Unmarshal(out.Bytes(), &got); err != nil {
		t.Fatalf("unmarshal: %v\n%s", err, out.String())
	}
	if !got.OK || !got.Logical || !got.Skipped || got.RefusedReason != "unknown_session" || got.SkippedReason != "unknown_session" {
		t.Fatalf("unexpected logical flush result: %+v", got)
	}
	assertNoRepoStateOrRegistry(t, roots, repoDir)
}
