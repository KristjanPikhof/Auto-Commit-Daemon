package verification

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	gitpkg "github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
)

func TestClassifyResourceUnavailable(t *testing.T) {
	gitNoSpace := &gitpkg.Error{
		Args: []string{"worktree", "add"}, ExitCode: 128,
		Stderr: strings.Repeat("diagnostic context\n", 600) +
			"fatal: cannot create directory: No space left on device\n",
		Err: errors.New("exit status 128"),
	}
	gitQuota := &gitpkg.Error{
		Args: []string{"worktree", "remove"}, ExitCode: 128,
		Stderr: "fatal: could not remove worktree: Disk quota exceeded\n",
		Err:    errors.New("exit status 128"),
	}
	tests := []struct {
		name      string
		err       error
		want      bool
		wantCause error
		wantSame  bool
	}{
		{
			name: "wrapped filesystem no space",
			err: &os.PathError{
				Op: "mkdir", Path: "verification-worktree", Err: syscall.ENOSPC,
			},
			want: true, wantCause: syscall.ENOSPC,
		},
		{
			name: "wrapped filesystem quota",
			err: &os.PathError{
				Op: "write", Path: "marker.json", Err: syscall.EDQUOT,
			},
			want: true, wantCause: syscall.EDQUOT,
		},
		{name: "bounded git no space diagnostic", err: gitNoSpace, want: true},
		{
			name: "joined git quota diagnostic",
			err:  errors.Join(errors.New("cleanup failed"), gitQuota), want: true,
		},
		{
			name: "untyped matching text",
			err:  errors.New("no space left on device"), wantSame: true,
		},
		{
			name: "ordinary git failure",
			err: &gitpkg.Error{
				Args: []string{"worktree", "add"}, ExitCode: 128,
				Stderr: "fatal: invalid reference\n", Err: errors.New("exit status 128"),
			},
			wantSame: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			classified := ClassifyResourceUnavailable(tc.err)
			if got := errors.Is(classified, ErrResourceUnavailable); got != tc.want {
				t.Fatalf("resource classification=%v want=%v err=%v", got, tc.want, classified)
			}
			if tc.wantCause != nil && !errors.Is(classified, tc.wantCause) {
				t.Fatalf("classified error lost cause %v: %v", tc.wantCause, classified)
			}
			if tc.wantSame && classified != tc.err {
				t.Fatalf("ordinary error was replaced: before=%p after=%p", tc.err, classified)
			}
			if tc.want {
				again := ClassifyResourceUnavailable(classified)
				if again != classified {
					t.Fatal("resource classification is not idempotent")
				}
			}
		})
	}
}

func TestRunnerCommandNoSpaceDiagnosticRemainsVerificationFailure(t *testing.T) {
	repo := initVerificationRepo(t)
	commit := runGit(t, repo, "rev-parse", "HEAD")
	approved := mustApprove(
		t, repo, `printf 'No space left on device\n' >&2; exit 9`,
		5*time.Second,
	)
	result, err := (Runner{
		WorkspaceRoot: filepath.Join(t.TempDir(), "verification"),
	}).Run(context.Background(), Request{
		RepoPath: repo, CandidateID: "candidate-command-no-space",
		CommitOID: commit, Command: approved,
	})
	if err != nil {
		t.Fatalf("Run returned infrastructure error for command exit: %v", err)
	}
	if result.Status != StatusFailed || !result.NeedsAttention ||
		result.ExitCode != 9 {
		t.Fatalf("result=%+v", result)
	}
}

func TestRunnerRunExactCandidateTreePreservesLiveState(t *testing.T) {
	repo := initVerificationRepo(t)
	ctx := context.Background()
	candidateCommit := candidateCommitWithFile(t, repo, "tracked.txt", "candidate\n")

	if err := os.WriteFile(filepath.Join(repo, "tracked.txt"), []byte("live dirty\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(repo, "staged.txt"), []byte("staged\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	runGit(t, repo, "add", "staged.txt")
	if err := os.WriteFile(filepath.Join(repo, "live-only.txt"), []byte("live\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	headBefore := runGit(t, repo, "rev-parse", "HEAD")
	statusBefore := runGitRaw(t, repo, "status", "--porcelain=v1", "-z")
	indexPath := filepath.Join(repo, ".git", "index")
	indexBefore, err := os.ReadFile(indexPath)
	if err != nil {
		t.Fatal(err)
	}
	trackedBefore, err := os.ReadFile(filepath.Join(repo, "tracked.txt"))
	if err != nil {
		t.Fatal(err)
	}

	approved := mustApprove(t, repo,
		`test "$(cat tracked.txt)" = candidate && test ! -e live-only.txt && printf 'exact tree\n'`,
		5*time.Second)
	runner := Runner{}
	resolvedRepo, err := gitpkg.ResolveWorktree(ctx, repo)
	if err != nil {
		t.Fatal(err)
	}
	workspaceRoot := filepath.Join(resolvedRepo.GitDir, "acd", workspaceDirName)
	result, err := runner.Run(ctx, Request{
		RepoPath:    repo,
		CandidateID: "candidate-exact",
		CommitOID:   candidateCommit,
		Command:     approved,
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if result.Status != StatusPassed || result.NeedsAttention {
		t.Fatalf("result=%+v", result)
	}
	if result.Output != "exact tree\n" {
		t.Fatalf("output=%q", result.Output)
	}
	if got := runGit(t, repo, "rev-parse", "HEAD"); got != headBefore {
		t.Fatalf("HEAD changed: got %s want %s", got, headBefore)
	}
	if got := runGitRaw(t, repo, "status", "--porcelain=v1", "-z"); got != statusBefore {
		t.Fatalf("live status changed:\ngot  %q\nwant %q", got, statusBefore)
	}
	indexAfter, err := os.ReadFile(indexPath)
	if err != nil {
		t.Fatal(err)
	}
	if string(indexAfter) != string(indexBefore) {
		t.Fatal("live index bytes changed")
	}
	trackedAfter, err := os.ReadFile(filepath.Join(repo, "tracked.txt"))
	if err != nil {
		t.Fatal(err)
	}
	if string(trackedAfter) != string(trackedBefore) {
		t.Fatal("live worktree bytes changed")
	}
	if strings.Contains(runGit(t, repo, "worktree", "list", "--porcelain"), workspaceRoot) {
		t.Fatal("verification worktree remains registered")
	}
	assertWorkspaceRootEmpty(t, workspaceRoot)
}

func TestRunnerStructuralCheckMaterializesWithoutLiveChanges(t *testing.T) {
	repo := initVerificationRepo(t)
	commit := candidateCommitWithFile(
		t, repo, "candidate-only.txt", "candidate\n",
	)
	if err := os.WriteFile(
		filepath.Join(repo, "live-only.txt"), []byte("live\n"), 0o644,
	); err != nil {
		t.Fatal(err)
	}
	before := runGitRaw(t, repo, "status", "--porcelain=v1", "-z")
	runner := Runner{
		WorkspaceRoot: filepath.Join(t.TempDir(), "verification"),
	}
	result, err := runner.CheckStructural(
		context.Background(),
		StructuralRequest{
			RepoPath: repo, CandidateID: "candidate-structural",
			CommitOID: commit,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != StatusPassed || result.Mode != ModeStructural ||
		result.NeedsAttention || result.ExitCode != 0 {
		t.Fatalf("result=%+v", result)
	}
	if after := runGitRaw(
		t, repo, "status", "--porcelain=v1", "-z",
	); after != before {
		t.Fatalf("live status changed: got %q want %q", after, before)
	}
	assertWorkspaceRootEmpty(t, runner.WorkspaceRoot)
}

func TestRunnerFailureRetainsSanitizedFinal64KiB(t *testing.T) {
	repo := initVerificationRepo(t)
	commit := runGit(t, repo, "rev-parse", "HEAD")
	t.Setenv("ACD_AI_API_KEY", "environment-secret")
	t.Setenv("BUILD_TOKEN", "build-secret")
	command := `head -c 70000 /dev/zero | tr '\000' x; ` +
		`printf '\nenv=%s|%s\n' "$ACD_AI_API_KEY" "$BUILD_TOKEN"; ` +
		`printf 'api_key=supersecret\nAuthorization: Bearer bearer-secret\n'; exit 7`
	approved := mustApprove(t, repo, command, 5*time.Second)
	runner := Runner{WorkspaceRoot: filepath.Join(t.TempDir(), "verification")}
	result, err := runner.Run(context.Background(), Request{
		RepoPath:    repo,
		CandidateID: "candidate-failure",
		CommitOID:   commit,
		Command:     approved,
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if result.Status != StatusFailed || !result.NeedsAttention || result.ExitCode != 7 {
		t.Fatalf("result=%+v", result)
	}
	if len(result.Output) > OutputLimit {
		t.Fatalf("output bytes=%d want <=%d", len(result.Output), OutputLimit)
	}
	for _, secret := range []string{"environment-secret", "build-secret", "supersecret", "bearer-secret"} {
		if strings.Contains(result.Output, secret) {
			t.Fatalf("output leaked %q", secret)
		}
	}
	if !strings.Contains(result.Output, "env=|") {
		t.Fatalf("filtered environment evidence missing from output")
	}
	if !strings.Contains(result.Output, "api_key=[redacted]") ||
		!strings.Contains(result.Output, "Authorization: Bearer [redacted]") {
		t.Fatalf("sanitized output missing redaction: %q", result.Output[len(result.Output)-200:])
	}
	assertWorkspaceRootEmpty(t, runner.WorkspaceRoot)
}

func TestRunnerTimeoutKillsCommandGroupAndCleansWorkspace(t *testing.T) {
	repo := initVerificationRepo(t)
	commit := runGit(t, repo, "rev-parse", "HEAD")
	approved := mustApprove(t, repo, `printf 'started\n'; sleep 30`, 50*time.Millisecond)
	runner := Runner{WorkspaceRoot: filepath.Join(t.TempDir(), "verification")}
	started := time.Now()
	result, err := runner.Run(context.Background(), Request{
		RepoPath:    repo,
		CandidateID: "candidate-timeout",
		CommitOID:   commit,
		Command:     approved,
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if elapsed := time.Since(started); elapsed > 3*time.Second {
		t.Fatalf("timeout took %v", elapsed)
	}
	if result.Status != StatusTimedOut || !result.TimedOut || !result.NeedsAttention {
		t.Fatalf("result=%+v", result)
	}
	if !strings.Contains(result.Output, "started") {
		t.Fatalf("output=%q", result.Output)
	}
	assertWorkspaceRootEmpty(t, runner.WorkspaceRoot)
}

func TestRunnerKillsBackgroundDescendantsAfterSuccessfulShell(t *testing.T) {
	repo := initVerificationRepo(t)
	commit := runGit(t, repo, "rev-parse", "HEAD")
	delayedWrite := filepath.Join(t.TempDir(), "background-write")
	command := `(sleep 0.3; printf leaked > ` + shellQuote(delayedWrite) + `) >/dev/null 2>&1 & exit 0`
	approved := mustApprove(t, repo, command, 5*time.Second)
	result, err := (Runner{
		WorkspaceRoot: filepath.Join(t.TempDir(), "verification"),
	}).Run(context.Background(), Request{
		RepoPath:    repo,
		CandidateID: "candidate-background-child",
		CommitOID:   commit,
		Command:     approved,
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if result.Status != StatusPassed {
		t.Fatalf("result=%+v", result)
	}
	time.Sleep(600 * time.Millisecond)
	if _, err := os.Stat(delayedWrite); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("background descendant survived Run: %v", err)
	}
}

func TestRunnerUsesPrivateHomeAndCapabilityFreeEnvironment(t *testing.T) {
	repo := initVerificationRepo(t)
	commit := runGit(t, repo, "rev-parse", "HEAD")
	externalConfig := filepath.Join(t.TempDir(), "config")
	if err := os.MkdirAll(filepath.Join(externalConfig, "acd"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(
		filepath.Join(externalConfig, "acd", "credentials.json"),
		[]byte("external-credential-secret"),
		0o600,
	); err != nil {
		t.Fatal(err)
	}
	t.Setenv("XDG_CONFIG_HOME", externalConfig)
	t.Setenv("SSH_AUTH_SOCK", filepath.Join(t.TempDir(), "agent.sock"))
	t.Setenv("AWS_ACCESS_KEY_ID", "cloud-capability")
	root := filepath.Join(t.TempDir(), "verification")
	command := `test ! -e "$XDG_CONFIG_HOME/acd/credentials.json"; ` +
		`probe="$(mktemp -d "$TMPDIR/probe.XXXXXX")"; ` +
		`if git -C "$probe" rev-parse --show-toplevel >/dev/null 2>&1; then exit 17; fi; ` +
		`printf 'home=%s\nconfig=%s\ntmp=%s\nprobe=%s\nssh=%s\naws=%s\n' ` +
		`"$HOME" "$XDG_CONFIG_HOME" "$TMPDIR" "$probe" "$SSH_AUTH_SOCK" "$AWS_ACCESS_KEY_ID"`
	approved := mustApprove(t, repo, command, 5*time.Second)
	result, err := (Runner{WorkspaceRoot: root}).Run(context.Background(), Request{
		RepoPath:    repo,
		CandidateID: "candidate-private-environment",
		CommitOID:   commit,
		Command:     approved,
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if result.Status != StatusPassed {
		t.Fatalf("result=%+v", result)
	}
	home := outputValue(result.Output, "home")
	config := outputValue(result.Output, "config")
	temp := outputValue(result.Output, "tmp")
	probe := outputValue(result.Output, "probe")
	if home == "" || config == "" || temp == "" || probe == "" ||
		!strings.Contains(result.Output, "ssh=\n") ||
		!strings.Contains(result.Output, "aws=\n") {
		t.Fatalf("environment was not isolated: %q", result.Output)
	}
	resolvedRepo, err := gitpkg.ResolveWorktree(context.Background(), repo)
	if err != nil {
		t.Fatal(err)
	}
	for name, path := range map[string]string{
		"home": home, "config": config, "tmp": temp, "probe": probe,
	} {
		if pathWithin(repo, path) ||
			pathWithin(resolvedRepo.GitDir, path) ||
			pathWithin(root, path) {
			t.Fatalf("%s path %q is inside repository verification ancestry", name, path)
		}
		if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("%s path was not cleaned: %v; output=%q", name, err, result.Output)
		}
	}
	if strings.Contains(result.Output, externalConfig) ||
		strings.Contains(result.Output, "external-credential-secret") ||
		strings.Contains(result.Output, "cloud-capability") {
		t.Fatalf("external capability leaked: %q", result.Output)
	}
}

func TestRunnerCleansRegistrationWhenCommandDeletesOwnTree(t *testing.T) {
	repo := initVerificationRepo(t)
	commit := runGit(t, repo, "rev-parse", "HEAD")
	approved := mustApprove(t, repo, `rm -rf "$PWD"`, 5*time.Second)
	runner := Runner{WorkspaceRoot: filepath.Join(t.TempDir(), "verification")}
	result, err := runner.Run(context.Background(), Request{
		RepoPath:    repo,
		CandidateID: "candidate-removes-tree",
		CommitOID:   commit,
		Command:     approved,
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if result.Status != StatusPassed {
		t.Fatalf("result=%+v", result)
	}
	if strings.Contains(runGit(t, repo, "worktree", "list", "--porcelain"), runner.WorkspaceRoot) {
		t.Fatal("deleted verification worktree remains registered")
	}
	assertWorkspaceRootEmpty(t, runner.WorkspaceRoot)
}

func TestRunnerCancelledBeforeStartCreatesNoWorkspace(t *testing.T) {
	repo := initVerificationRepo(t)
	approved := mustApprove(t, repo, "true", time.Second)
	root := filepath.Join(t.TempDir(), "verification")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := (Runner{WorkspaceRoot: root}).Run(ctx, Request{
		RepoPath:    repo,
		CandidateID: "candidate-cancelled",
		CommitOID:   runGit(t, repo, "rev-parse", "HEAD"),
		Command:     approved,
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error=%v", err)
	}
	assertWorkspaceRootAbsentOrEmpty(t, root)
}

func TestRunnerCleanupStaleRemovesDeadAndPreservesActive(t *testing.T) {
	repoPath := initVerificationRepo(t)
	ctx := context.Background()
	repo, err := gitpkg.ResolveWorktree(ctx, repoPath)
	if err != nil {
		t.Fatal(err)
	}
	commit := runGit(t, repoPath, "rev-parse", "HEAD")
	runner := Runner{WorkspaceRoot: filepath.Join(t.TempDir(), "verification")}
	root, err := runner.workspaceRoot(repo)
	if err != nil {
		t.Fatal(err)
	}
	dead, err := prepareWorkspace(ctx, root, repo, "candidate-dead", commit)
	if err != nil {
		t.Fatal(err)
	}
	deadMarker := readMarkerForTest(t, dead.sessionPath)
	deadMarker.PID = 99999999
	rewriteMarkerForTest(t, dead.sessionPath, deadMarker)
	rewriteEnvironmentOwnerForTest(t, dead, func(owner *environmentOwnerMarker) {
		owner.PID = deadMarker.PID
	})

	active, err := prepareWorkspace(ctx, root, repo, "candidate-active", commit)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = cleanupWorkspace(context.Background(), repoPath, active)
	})

	result, err := runner.CleanupStale(ctx, repoPath)
	if err != nil {
		t.Fatalf("CleanupStale: %v", err)
	}
	if result.Removed != 1 || result.Active != 1 || result.Skipped != 0 {
		t.Fatalf("result=%+v", result)
	}
	if _, err := os.Stat(dead.sessionPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("dead workspace remains: %v", err)
	}
	if _, err := os.Stat(active.treePath); err != nil {
		t.Fatalf("active workspace removed: %v", err)
	}
	worktrees := runGit(t, repoPath, "worktree", "list", "--porcelain")
	if strings.Contains(worktrees, dead.treePath) {
		t.Fatal("dead worktree remains registered")
	}
	if !strings.Contains(worktrees, active.treePath) {
		t.Fatal("active worktree registration missing")
	}
}

func TestRunnerCleanupStaleRetriesCleanupRequiredFromLiveProcess(t *testing.T) {
	repoPath := initVerificationRepo(t)
	ctx := context.Background()
	repo, err := gitpkg.ResolveWorktree(ctx, repoPath)
	if err != nil {
		t.Fatal(err)
	}
	commit := runGit(t, repoPath, "rev-parse", "HEAD")
	runner := Runner{WorkspaceRoot: filepath.Join(t.TempDir(), "verification")}
	root, err := runner.workspaceRoot(repo)
	if err != nil {
		t.Fatal(err)
	}
	stale, err := prepareWorkspace(ctx, root, repo, "candidate-cleanup-required", commit)
	if err != nil {
		t.Fatal(err)
	}
	marker := stale.marker
	marker.State = workspaceCleanupRequired
	if err := writeWorkspaceMarker(stale.sessionPath, marker); err != nil {
		t.Fatal(err)
	}

	result, err := runner.CleanupStale(ctx, repoPath)
	if err != nil {
		t.Fatalf("CleanupStale: %v", err)
	}
	if result.Removed != 1 || result.Active != 0 {
		t.Fatalf("result=%+v", result)
	}
	if _, err := os.Stat(stale.sessionPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("cleanup-required workspace remains: %v", err)
	}
}

func TestCleanupRejectsExternalEnvironmentMarkerMismatch(t *testing.T) {
	repoPath := initVerificationRepo(t)
	ctx := context.Background()
	repo, err := gitpkg.ResolveWorktree(ctx, repoPath)
	if err != nil {
		t.Fatal(err)
	}
	runner := Runner{WorkspaceRoot: filepath.Join(t.TempDir(), "verification")}
	root, err := runner.workspaceRoot(repo)
	if err != nil {
		t.Fatal(err)
	}
	ws, err := prepareWorkspace(
		ctx,
		root,
		repo,
		"candidate-owner-mismatch",
		runGit(t, repoPath, "rev-parse", "HEAD"),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = os.RemoveAll(ws.environmentPath)
		_ = os.RemoveAll(ws.sessionPath)
	})
	ownerPath := filepath.Join(ws.environmentPath, environmentMarker)
	owner, err := readEnvironmentOwner(ownerPath)
	if err != nil {
		t.Fatal(err)
	}
	owner.EnvironmentToken = "different-token"
	if err := os.Remove(ownerPath); err != nil {
		t.Fatal(err)
	}
	if err := writeJSONFile(ownerPath, owner); err != nil {
		t.Fatal(err)
	}

	if err := cleanupWorkspace(ctx, repoPath, ws); err == nil ||
		!strings.Contains(err.Error(), "ownership mismatch") {
		t.Fatalf("cleanup error=%v", err)
	}
	if _, err := os.Stat(ws.environmentPath); err != nil {
		t.Fatalf("mismatched external environment was removed: %v", err)
	}
}

func TestExternalEnvironmentAvoidsUnsafeHostTempDirectory(t *testing.T) {
	repoPath := initVerificationRepo(t)
	repo, err := gitpkg.ResolveWorktree(context.Background(), repoPath)
	if err != nil {
		t.Fatal(err)
	}
	unsafeTemp := filepath.Join(repo.GitDir, "acd", "host-temp")
	if err := os.MkdirAll(unsafeTemp, 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("TMPDIR", unsafeTemp)

	path, err := externalEnvironmentPath(repo, "0123456789abcdef")
	if err != nil {
		t.Fatal(err)
	}
	if pathWithin(repo.Root, path) || pathWithin(repo.GitDir, path) {
		t.Fatalf("external environment remained inside Git ancestry: %s", path)
	}
}

func TestRunnerCleanupStaleRemovesOldIncompleteAndOversizedMarkers(t *testing.T) {
	repo := initVerificationRepo(t)
	runner := Runner{WorkspaceRoot: filepath.Join(t.TempDir(), "verification")}
	if err := ensurePrivateDirectory(runner.WorkspaceRoot); err != nil {
		t.Fatal(err)
	}
	old := time.Now().Add(-incompleteGrace - time.Minute)

	incomplete := filepath.Join(runner.WorkspaceRoot, workspacePrefix+"incomplete")
	if err := os.Mkdir(incomplete, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(incomplete, old, old); err != nil {
		t.Fatal(err)
	}

	oversized := filepath.Join(runner.WorkspaceRoot, workspacePrefix+"oversized")
	if err := os.Mkdir(oversized, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(
		filepath.Join(oversized, workspaceMarkerName),
		[]byte(strings.Repeat("x", maxMarkerBytes+1)),
		0o600,
	); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(oversized, old, old); err != nil {
		t.Fatal(err)
	}

	result, err := runner.CleanupStale(context.Background(), repo)
	if err != nil {
		t.Fatalf("CleanupStale: %v", err)
	}
	if result.Removed != 2 || result.Skipped != 0 {
		t.Fatalf("result=%+v", result)
	}
	assertWorkspaceRootEmpty(t, runner.WorkspaceRoot)
}

func TestRunnerCleanupStaleLeavesMalformedAndSymlinkedEntries(t *testing.T) {
	repo := initVerificationRepo(t)
	runner := Runner{WorkspaceRoot: filepath.Join(t.TempDir(), "verification")}
	if err := ensurePrivateDirectory(runner.WorkspaceRoot); err != nil {
		t.Fatal(err)
	}
	malformed := filepath.Join(runner.WorkspaceRoot, workspacePrefix+"malformed")
	if err := os.Mkdir(malformed, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(malformed, workspaceMarkerName), []byte(`{"version":1} {}`), 0o600); err != nil {
		t.Fatal(err)
	}
	target := t.TempDir()
	symlink := filepath.Join(runner.WorkspaceRoot, workspacePrefix+"symlink")
	if err := os.Symlink(target, symlink); err != nil {
		t.Fatal(err)
	}
	result, err := runner.CleanupStale(context.Background(), repo)
	if err == nil {
		t.Fatal("expected malformed-marker diagnostic")
	}
	if result.Removed != 0 || result.Skipped != 2 {
		t.Fatalf("result=%+v", result)
	}
	if _, err := os.Lstat(malformed); err != nil {
		t.Fatalf("malformed workspace was removed: %v", err)
	}
	if _, err := os.Lstat(symlink); err != nil {
		t.Fatalf("symlink workspace was removed: %v", err)
	}
}

func TestRunnerRejectsWrongRepositoryAndSymbolicCommitBeforeMaterialization(t *testing.T) {
	repo := initVerificationRepo(t)
	other := initVerificationRepo(t)
	root := filepath.Join(t.TempDir(), "verification")
	runner := Runner{WorkspaceRoot: root}
	approved := mustApprove(t, other, "true", time.Second)
	_, err := runner.Run(context.Background(), Request{
		RepoPath:    repo,
		CandidateID: "candidate-mismatch",
		CommitOID:   runGit(t, repo, "rev-parse", "HEAD"),
		Command:     approved,
	})
	if err == nil || !strings.Contains(err.Error(), "does not match") {
		t.Fatalf("error=%v", err)
	}
	assertWorkspaceRootAbsentOrEmpty(t, root)

	approved = mustApprove(t, repo, "true", time.Second)
	_, err = runner.Run(context.Background(), Request{
		RepoPath:    repo,
		CandidateID: "candidate-symbolic",
		CommitOID:   "HEAD",
		Command:     approved,
	})
	if err == nil || !strings.Contains(err.Error(), "symbolic or abbreviated") {
		t.Fatalf("error=%v", err)
	}
	assertWorkspaceRootAbsentOrEmpty(t, root)
}

func TestNewApprovedCommandValidatesScopeAndBounds(t *testing.T) {
	repo := initVerificationRepo(t)
	tests := []struct {
		name       string
		approvalID string
		mode       Mode
		command    string
		timeout    time.Duration
	}{
		{name: "missing id", mode: ModeFast, command: "true", timeout: time.Second},
		{name: "bad mode", approvalID: "revision:1", mode: "none", command: "true", timeout: time.Second},
		{name: "missing command", approvalID: "revision:1", mode: ModeFast, timeout: time.Second},
		{name: "nul command", approvalID: "revision:1", mode: ModeFast, command: "true\x00", timeout: time.Second},
		{name: "missing timeout", approvalID: "revision:1", mode: ModeFast, command: "true"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := NewApprovedCommand(repo, test.approvalID, test.mode, test.command, test.timeout); err == nil {
				t.Fatal("expected validation error")
			}
		})
	}
}

func initVerificationRepo(t *testing.T) string {
	t.Helper()
	repo := t.TempDir()
	if err := gitpkg.Init(context.Background(), repo); err != nil {
		t.Fatal(err)
	}
	runGit(t, repo, "symbolic-ref", "HEAD", "refs/heads/main")
	runGit(t, repo, "config", "user.name", "ACD Test")
	runGit(t, repo, "config", "user.email", "acd@example.invalid")
	if err := os.WriteFile(filepath.Join(repo, "tracked.txt"), []byte("base\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	runGit(t, repo, "add", "tracked.txt")
	runGit(t, repo, "commit", "-q", "-m", "base")
	return repo
}

func candidateCommitWithFile(t *testing.T, repo, path, content string) string {
	t.Helper()
	ctx := context.Background()
	index := filepath.Join(t.TempDir(), "candidate.index")
	if err := gitpkg.ReadTree(ctx, repo, index, "HEAD"); err != nil {
		t.Fatal(err)
	}
	oid, err := gitpkg.HashObjectStdin(ctx, repo, []byte(content))
	if err != nil {
		t.Fatal(err)
	}
	if err := gitpkg.UpdateIndexInfo(ctx, repo, index,
		[]string{"100644 " + oid + "\t" + path}); err != nil {
		t.Fatal(err)
	}
	tree, err := gitpkg.WriteTree(ctx, repo, index)
	if err != nil {
		t.Fatal(err)
	}
	parent := runGit(t, repo, "rev-parse", "HEAD")
	commit, err := gitpkg.CommitTree(ctx, repo, tree, "candidate\n", parent)
	if err != nil {
		t.Fatal(err)
	}
	return commit
}

func mustApprove(t *testing.T, repo, command string, timeout time.Duration) ApprovedCommand {
	t.Helper()
	approved, err := NewApprovedCommand(repo, "revision:1", ModeFast, command, timeout)
	if err != nil {
		t.Fatal(err)
	}
	return approved
}

func runGit(t *testing.T, repo string, args ...string) string {
	t.Helper()
	output, err := gitpkg.Run(context.Background(), gitpkg.RunOpts{
		Dir:     repo,
		Timeout: gitpkg.DefaultWriteTimeout,
	}, args...)
	if err != nil {
		t.Fatalf("git %s: %v", strings.Join(args, " "), err)
	}
	return strings.TrimSpace(string(output))
}

func runGitRaw(t *testing.T, repo string, args ...string) string {
	t.Helper()
	output, err := gitpkg.Run(context.Background(), gitpkg.RunOpts{
		Dir:     repo,
		Timeout: gitpkg.DefaultWriteTimeout,
	}, args...)
	if err != nil {
		t.Fatalf("git %s: %v", strings.Join(args, " "), err)
	}
	return string(output)
}

func assertWorkspaceRootEmpty(t *testing.T, root string) {
	t.Helper()
	entries, err := os.ReadDir(root)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("workspace root has %d entries", len(entries))
	}
}

func assertWorkspaceRootAbsentOrEmpty(t *testing.T, root string) {
	t.Helper()
	entries, err := os.ReadDir(root)
	if errors.Is(err, os.ErrNotExist) {
		return
	}
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("workspace root has %d entries", len(entries))
	}
}

func readMarkerForTest(t *testing.T, sessionPath string) workspaceMarker {
	t.Helper()
	marker, err := readWorkspaceMarker(filepath.Join(sessionPath, workspaceMarkerName))
	if err != nil {
		t.Fatal(err)
	}
	return marker
}

func rewriteMarkerForTest(t *testing.T, sessionPath string, marker workspaceMarker) {
	t.Helper()
	path := filepath.Join(sessionPath, workspaceMarkerName)
	raw, err := jsonMarshal(marker)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatal(err)
	}
}

func rewriteEnvironmentOwnerForTest(
	t *testing.T,
	ws workspace,
	update func(*environmentOwnerMarker),
) {
	t.Helper()
	path := filepath.Join(ws.environmentPath, environmentMarker)
	owner, err := readEnvironmentOwner(path)
	if err != nil {
		t.Fatal(err)
	}
	update(&owner)
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if err := writeJSONFile(path, owner); err != nil {
		t.Fatal(err)
	}
}

func jsonMarshal(value any) ([]byte, error) {
	var builder strings.Builder
	encoder := json.NewEncoder(&builder)
	if err := encoder.Encode(value); err != nil {
		return nil, err
	}
	return []byte(builder.String()), nil
}

func shellQuote(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "'\"'\"'") + "'"
}

func outputValue(output, key string) string {
	prefix := key + "="
	for _, line := range strings.Split(output, "\n") {
		if strings.HasPrefix(line, prefix) {
			return strings.TrimPrefix(line, prefix)
		}
	}
	return ""
}
