package daemon

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

func TestFinishWorkerRecordsCompletionBeforeUnlockingIndex(t *testing.T) {
	f, w := finishWorkerFixture(t)
	realGit, err := exec.LookPath("git")
	if err != nil {
		t.Fatal(err)
	}
	shim := t.TempDir()
	acquired := filepath.Join(shim, "writer-acquired-lock")
	t.Setenv("PATH", shim+string(os.PathListSeparator)+os.Getenv("PATH"))
	for build := 174; build <= 175; build++ {
		finishWrite(t, f.dir, "project.pbxproj", fmt.Sprintf("CURRENT_PROJECT_VERSION = %d;\n", build))
		r := startFinishWorker(t, w, FinishOptions{Purpose: "Update release build number"})
		// A competing Git writer takes the index lock at the first Git check
		// after publication and index reconciliation, if ACD has released it.
		script := fmt.Sprintf(`#!/bin/sh
if [ "$1" = symbolic-ref ] && [ "$4" = refs/heads/main ] &&
   [ "$(%q rev-parse 'HEAD^{tree}')" = %q ] &&
   %q diff --cached --quiet; then
  if (set -C; printf 'competing Git writer\n' > %q) 2>/dev/null; then
    touch %q
  fi
fi
exec %q "$@"
`, realGit, r.Target.TargetTree, realGit, filepath.Join(f.dir, ".git", "index.lock"), acquired, realGit)
		if err := os.WriteFile(filepath.Join(shim, "git"), []byte(script), 0700); err != nil {
			t.Fatal(err)
		}
		completed, err := w.Execute(context.Background(), r.ID)
		if err != nil || completed.State != state.FinishCompleted {
			t.Fatalf("build %d finish state=%s result=%s: %v", build, completed.State, completed.Result, err)
		}
		var result state.FinishPublicationResult
		if err := json.Unmarshal(completed.Result, &result); err != nil || !result.Published || !result.IndexReconciled {
			t.Fatalf("build %d result=%+v: %v", build, result, err)
		}
		// Stop the competing writer before inspecting the completed operation.
		if err := os.Remove(filepath.Join(shim, "git")); err != nil {
			t.Fatal(err)
		}
		if _, err := os.Stat(acquired); !os.IsNotExist(err) {
			t.Fatalf("index lock released before completion: %v", err)
		}
		for _, name := range []string{"index.lock", "HEAD.lock"} {
			if _, err := os.Stat(filepath.Join(f.dir, ".git", name)); !os.IsNotExist(err) {
				t.Fatalf("build %d left %s: %v", build, name, err)
			}
		}
		if dirty := finishWorkerGit(t, f.dir, "status", "--porcelain"); dirty != "" {
			t.Fatalf("build %d left changes: %s", build, dirty)
		}
	}
}
