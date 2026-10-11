package daemon

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/checkpoint"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

// The real IgnoreChecker pipe supplies a deterministic race boundary between
// enumeration and descent, without adding a scanner hook or timing sleeps.
func TestFinishProtectionRejectsAncestorSymlinkSwap(t *testing.T) {
	for _, targetKind := range []string{"external", "sensitive-in-root"} {
		t.Run(targetKind, func(t *testing.T) {
			f := newCaptureFixture(t)
			finishWrite(t, f.dir, "public/original.txt", "original eligible content")
			finishWrite(t, f.dir, "ordinary.txt", "ordinary sibling remains protected")
			target := t.TempDir()
			if targetKind == "sensitive-in-root" {
				target = filepath.Join(f.dir, "vault")
			}
			secret := "UNIQUE PRIVATE ANCESTOR CONTENT " + targetKind + " " + f.dir
			finishWrite(t, target, "private.txt", secret)
			secretOID, err := git.HashObjectStdinReadOnly(t.Context(), f.dir, []byte(secret))
			if err != nil {
				t.Fatal(err)
			}
			indexBefore, err := os.ReadFile(filepath.Join(f.gitDir, "index"))
			if err != nil {
				t.Fatal(err)
			}
			realGit, err := exec.LookPath("git")
			if err != nil {
				t.Fatal(err)
			}
			shim := t.TempDir()
			marker := filepath.Join(shim, "swapped")
			quote := func(s string) string { return "'" + strings.ReplaceAll(s, "'", "'\\''") + "'" }
			script := fmt.Sprintf(`#!/bin/bash
if [[ "$1" == check-ignore ]]; then
  while IFS= read -r -d '' path; do
    if [[ "$path" == public && ! -e %s ]]; then
      mv public .public-before
      ln -s %s public
      touch %s
    fi
    printf '\0\0\0%%s\0' "$path"
  done
else
  exec %s "$@"
fi
`, quote(marker), quote(target), quote(marker), quote(realGit))
			if err := os.WriteFile(filepath.Join(shim, "git"), []byte(script), 0o755); err != nil {
				t.Fatal(err)
			}
			t.Setenv("PATH", shim+string(os.PathListSeparator)+os.Getenv("PATH"))
			store := checkpoint.Store{DB: f.db}
			summary, err := ProtectWorktree(t.Context(), f.dir, f.db, f.cctx, CaptureOpts{
				IgnoreChecker: f.ig, SensitiveMatcher: state.NewSensitiveMatcherFromValue("vault"),
				CheckpointStore: &store, WorktreeID: checkpoint.WorktreeID(f.dir), ObservationEpoch: 1,
				CheckpointReason: state.CheckpointReasonPoll,
			})
			if err == nil || !summary.Partial || summary.Protected || summary.CheckpointID == "" {
				t.Fatalf("swapped ancestor must retain a partial checkpoint: summary=%+v err=%v", summary, err)
			}
			if _, err := os.Stat(marker); err != nil {
				t.Fatalf("ignore boundary never swapped directory: %v", err)
			}
			projection, err := state.ReadCheckpointProjection(t.Context(), f.db.Path(), 1)
			if err != nil || projection.Latest == nil || !projection.Latest.Partial {
				t.Fatalf("partial checkpoint projection=%+v err=%v", projection, err)
			}
			cp := projection.Latest
			issue := false
			for _, item := range cp.CaptureIssues {
				issue = issue || (item.Path == "public" && item.Reason == "unreadable")
			}
			if !issue {
				t.Fatalf("missing ancestor coverage issue: %+v", cp.CaptureIssues)
			}
			complete, _, err := state.MetaGet(t.Context(), f.db, MetaKeyProtectionComplete)
			if err != nil || complete != "false" {
				t.Fatalf("coverage=%q err=%v", complete, err)
			}
			entries, err := git.LsTree(t.Context(), f.dir, cp.CommitOID, true)
			if err != nil {
				t.Fatal(err)
			}
			tree := make(map[string]git.TreeEntry, len(entries))
			for _, entry := range entries {
				tree[entry.Path] = entry
			}
			ordinary, ok := tree["ordinary.txt"]
			if !ok {
				t.Fatal("readable ordinary sibling was not saved")
			}
			body, err := git.Run(t.Context(), git.RunOpts{Dir: f.dir}, "cat-file", "blob", ordinary.OID)
			if err != nil || string(body) != "ordinary sibling remains protected" {
				t.Fatalf("ordinary blob=%q err=%v", body, err)
			}
			for name, entry := range tree {
				if name == "public/private.txt" || strings.HasPrefix(name, "vault/") || entry.OID == secretOID {
					t.Fatalf("private ancestor content entered checkpoint: %s %+v", name, entry)
				}
			}
			if _, err := git.Run(t.Context(), git.RunOpts{Dir: f.dir}, "cat-file", "-e", secretOID); err == nil {
				t.Fatal("private bytes entered the Git object store")
			}
			indexAfter, err := os.ReadFile(filepath.Join(f.gitDir, "index"))
			if err != nil || !bytes.Equal(indexBefore, indexAfter) {
				t.Fatalf("scan changed native staging: %v", err)
			}
			for filename, want := range map[string]string{
				filepath.Join(f.dir, "ordinary.txt"):                "ordinary sibling remains protected",
				filepath.Join(f.dir, ".public-before/original.txt"): "original eligible content",
				filepath.Join(target, "private.txt"):                secret,
			} {
				body, err := os.ReadFile(filename)
				if err != nil || string(body) != want {
					t.Fatalf("live content changed at %s: %q, %v", filename, body, err)
				}
			}
			if got, err := os.Readlink(filepath.Join(f.dir, "public")); err != nil || got != target {
				t.Fatalf("scan changed swapped link: %q, %v", got, err)
			}
			if got, err := git.RevParse(t.Context(), f.dir, "HEAD"); err != nil || got != f.cctx.BaseHead {
				t.Fatalf("scan moved HEAD: %q, %v", got, err)
			}
		})
	}
}
