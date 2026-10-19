package git

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func commitWorktreePath(t *testing.T, ctx context.Context, repo, path, body, msg string) string {
	t.Helper()
	if err := os.WriteFile(filepath.Join(repo, path), []byte(body), 0o644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
	if _, err := Run(ctx, RunOpts{Dir: repo, Timeout: DefaultWriteTimeout}, "add", path); err != nil {
		t.Fatalf("git add %s: %v", path, err)
	}
	if _, err := Run(ctx, RunOpts{Dir: repo, Timeout: DefaultWriteTimeout}, "commit", "-q", "-m", msg); err != nil {
		t.Fatalf("git commit %s: %v", path, err)
	}
	oid, err := RevParse(ctx, repo, "HEAD")
	if err != nil {
		t.Fatalf("rev-parse HEAD: %v", err)
	}
	return oid
}
