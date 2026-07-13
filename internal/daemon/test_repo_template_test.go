package daemon

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

type daemonTestRepoTemplate struct {
	dir  string
	head string
}

type daemonTestRepo struct {
	dir    string
	gitDir string
	head   string
	db     *state.DB
}

var (
	captureRepoTemplate daemonTestRepoTemplate
	daemonRepoTemplate  daemonTestRepoTemplate
)

// setupDaemonTestRepoTemplates pays the Git initialization and state schema
// bootstrap cost once per package test process. Each fixture gets a private
// filesystem copy, so tests remain free to mutate refs, objects, and state.
func setupDaemonTestRepoTemplates() (string, error) {
	root, err := os.MkdirTemp("", "acd-daemon-templates-*")
	if err != nil {
		return "", fmt.Errorf("create template root: %w", err)
	}

	cleanupOnError := func(err error) (string, error) {
		_ = os.RemoveAll(root)
		return "", err
	}

	captureRepoTemplate, err = buildDaemonTestRepoTemplate(
		root,
		"capture",
		[]byte("ignored.txt\n"),
	)
	if err != nil {
		return cleanupOnError(err)
	}
	daemonRepoTemplate, err = buildDaemonTestRepoTemplate(
		root,
		"daemon",
		[]byte("# acd test seed\n"),
	)
	if err != nil {
		return cleanupOnError(err)
	}
	return root, nil
}

func buildDaemonTestRepoTemplate(root, name string, gitignore []byte) (daemonTestRepoTemplate, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	dir := filepath.Join(root, name)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return daemonTestRepoTemplate{}, fmt.Errorf("create %s template: %w", name, err)
	}
	if err := git.Init(ctx, dir); err != nil {
		return daemonTestRepoTemplate{}, fmt.Errorf("git init %s template: %w", name, err)
	}
	if _, err := git.Run(ctx, git.RunOpts{Dir: dir}, "symbolic-ref", "HEAD", "refs/heads/main"); err != nil {
		return daemonTestRepoTemplate{}, fmt.Errorf("set %s template HEAD: %w", name, err)
	}
	for _, kv := range [][]string{
		{"user.email", "acd-test@example.com"},
		{"user.name", "ACD Test"},
		{"commit.gpgsign", "false"},
	} {
		if _, err := git.Run(ctx, git.RunOpts{Dir: dir}, "config", kv[0], kv[1]); err != nil {
			return daemonTestRepoTemplate{}, fmt.Errorf("configure %s template %s: %w", name, kv[0], err)
		}
	}
	if err := os.WriteFile(filepath.Join(dir, ".gitignore"), gitignore, 0o644); err != nil {
		return daemonTestRepoTemplate{}, fmt.Errorf("write %s template seed: %w", name, err)
	}
	if _, err := git.Run(ctx, git.RunOpts{Dir: dir}, "add", ".gitignore"); err != nil {
		return daemonTestRepoTemplate{}, fmt.Errorf("stage %s template seed: %w", name, err)
	}
	if _, err := git.Run(ctx, git.RunOpts{Dir: dir}, "commit", "-q", "-m", "seed"); err != nil {
		return daemonTestRepoTemplate{}, fmt.Errorf("commit %s template seed: %w", name, err)
	}

	head, err := git.RevParse(ctx, dir, "HEAD")
	if err != nil {
		return daemonTestRepoTemplate{}, fmt.Errorf("resolve %s template HEAD: %w", name, err)
	}
	db, err := state.Open(ctx, state.DBPathFromGitDir(filepath.Join(dir, ".git")))
	if err != nil {
		return daemonTestRepoTemplate{}, fmt.Errorf("bootstrap %s template state: %w", name, err)
	}
	if _, err := db.SQL().ExecContext(ctx, `PRAGMA wal_checkpoint(TRUNCATE)`); err != nil {
		_ = db.Close()
		return daemonTestRepoTemplate{}, fmt.Errorf("checkpoint %s template state: %w", name, err)
	}
	if err := db.Close(); err != nil {
		return daemonTestRepoTemplate{}, fmt.Errorf("close %s template state: %w", name, err)
	}

	return daemonTestRepoTemplate{dir: dir, head: head}, nil
}

func cloneDaemonTestRepo(t *testing.T, template daemonTestRepoTemplate) *daemonTestRepo {
	t.Helper()
	if template.dir == "" || template.head == "" {
		t.Fatal("daemon test repository template was not initialized")
	}

	dir, err := os.MkdirTemp("", "acd-daemon-test-*")
	if err != nil {
		t.Fatalf("create test repository: %v", err)
	}
	t.Cleanup(func() { removeAllWithRetry(t, dir) })
	if err := os.CopyFS(dir, os.DirFS(template.dir)); err != nil {
		t.Fatalf("clone test repository template: %v", err)
	}

	gitDir := filepath.Join(dir, ".git")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	db, err := state.Open(ctx, state.DBPathFromGitDir(gitDir))
	if err != nil {
		t.Fatalf("open cloned test state: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	return &daemonTestRepo{dir: dir, gitDir: gitDir, head: template.head, db: db}
}
