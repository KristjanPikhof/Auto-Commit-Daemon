package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/daemon"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	pausepkg "github.com/KristjanPikhof/Auto-Commit-Daemon/internal/pause"
)

// TestCommitAll_FlagsRegistered ensures the command surfaces all required
// flags. Full behavior coverage lives in the t-unit task.
func TestCommitAll_FlagsRegistered(t *testing.T) {
	cmd := newCommitAllCmd()
	for _, name := range []string{"yes", "dry-run"} {
		if cmd.Flags().Lookup(name) == nil {
			t.Fatalf("commit-all: missing flag --%s", name)
		}
	}
}

// TestCommitAll_HelpExposesCommand verifies the root command tree wires the
// command in and the help text mentions it.
func TestCommitAll_HelpExposesCommand(t *testing.T) {
	root := newRootCmd()
	var out bytes.Buffer
	root.SetOut(&out)
	root.SetErr(&bytes.Buffer{})
	root.SetArgs([]string{"--help"})
	if err := root.ExecuteContext(context.Background()); err != nil {
		t.Fatalf("root help: %v", err)
	}
	if !strings.Contains(out.String(), "acd commit-all") {
		t.Fatalf("root help missing commit-all entry:\n%s", out.String())
	}
}

// TestCommitAll_RefusesDetachedHEAD pins the detached-HEAD refusal guard.
func TestCommitAll_RefusesDetachedHEAD(t *testing.T) {
	repo, _, _ := makeRegisteredGitRepoStateDB(t)
	ctx := context.Background()
	head, err := git.RevParse(ctx, repo, "HEAD")
	if err != nil {
		t.Fatalf("rev-parse HEAD: %v", err)
	}
	if _, err := git.Run(ctx, git.RunOpts{Dir: repo}, "checkout", "--detach", head); err != nil {
		t.Fatalf("git checkout --detach: %v", err)
	}

	var out bytes.Buffer
	err = runCommitAll(ctx, &out, nil, repo, true, false, true)
	if err == nil {
		t.Fatalf("runCommitAll on detached HEAD returned nil; want refusal error")
	}
	if !strings.Contains(err.Error(), "detached HEAD") {
		t.Fatalf("expected detached HEAD message, got: %v", err)
	}
}

// TestCommitAll_RefusesGitOperationInProgress pins the rebase/merge marker
// refusal guard.
func TestCommitAll_RefusesGitOperationInProgress(t *testing.T) {
	repo, _, _ := makeRegisteredGitRepoStateDB(t)
	ctx := context.Background()
	gitDir := filepath.Join(repo, ".git")
	if err := os.WriteFile(filepath.Join(gitDir, "MERGE_HEAD"), []byte("deadbeef\n"), 0o644); err != nil {
		t.Fatalf("write MERGE_HEAD: %v", err)
	}

	var out bytes.Buffer
	err := runCommitAll(ctx, &out, nil, repo, true, false, true)
	if err == nil {
		t.Fatalf("expected refusal while MERGE_HEAD is present")
	}
	if !strings.Contains(err.Error(), "git operation") {
		t.Fatalf("expected git operation refusal, got: %v", err)
	}
}

// TestCommitAll_RefusesManualPauseMarker pins the pause-marker refusal guard.
func TestCommitAll_RefusesManualPauseMarker(t *testing.T) {
	repo, _, _ := makeRegisteredGitRepoStateDB(t)
	ctx := context.Background()
	gitDir := filepath.Join(repo, ".git")
	markerPath := pausepkg.Path(gitDir)
	if err := os.MkdirAll(filepath.Dir(markerPath), 0o700); err != nil {
		t.Fatalf("mkdir marker parent: %v", err)
	}
	if err := os.WriteFile(markerPath, []byte(`{"reason":"test","set_at":"now","set_by":"test"}`), 0o600); err != nil {
		t.Fatalf("write pause marker: %v", err)
	}

	var out bytes.Buffer
	err := runCommitAll(ctx, &out, nil, repo, true, false, true)
	if err == nil {
		t.Fatalf("expected refusal while manual pause marker is present")
	}
	if !strings.Contains(err.Error(), "pause") {
		t.Fatalf("expected pause refusal, got: %v", err)
	}
}

// TestCommitAll_RefusesWhileDaemonLockHeld pins the daemon-alive refusal
// guard. Holding daemon.lock simulates a live daemon.
func TestCommitAll_RefusesWhileDaemonLockHeld(t *testing.T) {
	repo, _, _ := makeRegisteredGitRepoStateDB(t)
	ctx := context.Background()
	held, err := daemon.AcquireDaemonLock(filepath.Join(repo, ".git"))
	if err != nil {
		t.Fatalf("pre-acquire daemon.lock: %v", err)
	}
	defer func() { _ = held.Release() }()

	var out bytes.Buffer
	err = runCommitAll(ctx, &out, nil, repo, true, false, true)
	if err == nil {
		t.Fatalf("expected refusal while daemon.lock is held")
	}
	if !strings.Contains(err.Error(), "daemon") {
		t.Fatalf("expected daemon-alive refusal, got: %v", err)
	}
}

// TestCommitAll_CleanWorktreeNoOp covers the success path on a clean worktree:
// capture finds no events, command exits zero with PendingBefore=0.
func TestCommitAll_CleanWorktreeNoOp(t *testing.T) {
	repo, _, db := makeRegisteredGitRepoStateDB(t)
	_ = db.Close()
	ctx := context.Background()

	var out bytes.Buffer
	if err := runCommitAll(ctx, &out, nil, repo, true, false, true); err != nil {
		t.Fatalf("runCommitAll clean worktree: %v", err)
	}
	var got commitAllResult
	if err := json.Unmarshal(out.Bytes(), &got); err != nil {
		t.Fatalf("unmarshal: %v\n%s", err, out.String())
	}
	if !got.OK || got.PendingBefore != 0 {
		t.Fatalf("clean worktree result: %+v", got)
	}
	if got.Strategy == "" {
		t.Fatalf("strategy must be reported even on no-op, got: %+v", got)
	}
}

// TestCommitAll_DryRunNeverCommits pins that --dry-run leaves HEAD unchanged
// even with a dirty worktree.
func TestCommitAll_DryRunNeverCommits(t *testing.T) {
	repo, _, db := makeRegisteredGitRepoStateDB(t)
	_ = db.Close()
	ctx := context.Background()
	if err := os.WriteFile(filepath.Join(repo, "new.txt"), []byte("dirty\n"), 0o644); err != nil {
		t.Fatalf("write dirty file: %v", err)
	}
	headBefore, err := git.RevParse(ctx, repo, "HEAD")
	if err != nil {
		t.Fatalf("rev-parse HEAD: %v", err)
	}

	var out bytes.Buffer
	if err := runCommitAll(ctx, &out, nil, repo, true, true, true); err != nil {
		t.Fatalf("runCommitAll dry-run: %v", err)
	}
	var got commitAllResult
	if err := json.Unmarshal(out.Bytes(), &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if !got.DryRun {
		t.Fatalf("expected DryRun=true, got %+v", got)
	}
	headAfter, err := git.RevParse(ctx, repo, "HEAD")
	if err != nil {
		t.Fatalf("rev-parse after: %v", err)
	}
	if headAfter != headBefore {
		t.Fatalf("dry-run mutated HEAD: before=%s after=%s", headBefore, headAfter)
	}
}

// TestCommitAll_JSONRequiresYesWhenInteractive pins that --json without --yes
// refuses because there is no interactive prompt available.
func TestCommitAll_JSONRequiresYesWhenInteractive(t *testing.T) {
	repo, _, db := makeRegisteredGitRepoStateDB(t)
	_ = db.Close()
	ctx := context.Background()
	if err := os.WriteFile(filepath.Join(repo, "new.txt"), []byte("dirty\n"), 0o644); err != nil {
		t.Fatalf("write dirty file: %v", err)
	}

	var out bytes.Buffer
	err := runCommitAll(ctx, &out, nil, repo, false, false, true)
	if err == nil {
		t.Fatalf("expected --json without --yes to refuse")
	}
	if !strings.Contains(err.Error(), "--yes") {
		t.Fatalf("expected --yes prompt error, got: %v", err)
	}
}
