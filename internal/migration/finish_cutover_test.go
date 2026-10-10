package migration

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/checkpoint"
	gitpkg "github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

func TestFinishLegacyDurablePrepareResume(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	repo := filepath.Join(root, "repo")
	if _, err := gitpkg.Run(ctx, gitpkg.RunOpts{Dir: root}, "init", "-q", repo); err != nil {
		t.Fatal(err)
	}
	if _, err := gitpkg.Run(ctx, gitpkg.RunOpts{Dir: repo}, "symbolic-ref", "HEAD", "refs/heads/main"); err != nil {
		t.Fatal(err)
	}
	wt, err := gitpkg.ResolveWorktree(ctx, repo)
	if err != nil {
		t.Fatal(err)
	}
	db, err := state.Open(ctx, state.DBPathFromGitDir(wt.GitDir))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	blob, err := gitpkg.HashObjectStdinDurable(ctx, repo, []byte("older captured version\n"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = db.SQL().ExecContext(ctx, `INSERT INTO capture_events(seq,branch_ref,branch_generation,base_head,operation,path,fidelity,captured_ts) VALUES(1,'refs/heads/main',1,'','create','file.txt','exact',1);
 INSERT INTO capture_ops(event_seq,ord,op,path,after_oid,after_mode,fidelity) VALUES(1,0,'create','file.txt',?,'100644','exact')`, blob); err != nil {
		t.Fatal(err)
	}
	m, err := state.InventoryFinishLegacy(ctx, db)
	if err != nil {
		t.Fatal(err)
	}
	ref, oid, pins, err := pinFinishLegacy(ctx, wt.Root, checkpoint.WorktreeID(wt.Root), m)
	if err != nil {
		t.Fatal(err)
	}
	// Crash after private objects/refs are durable, before the SQLite pointer.
	repeatRef, repeatOID, _, err := pinFinishLegacy(ctx, wt.Root, checkpoint.WorktreeID(wt.Root), m)
	if err != nil || repeatRef != ref || repeatOID != oid {
		t.Fatalf("orphan ref reconnect %s %s %v", repeatRef, repeatOID, err)
	}
	saved, err := state.PrepareFinishLegacyCutover(ctx, db, checkpoint.WorktreeID(wt.Root), m, ref, oid, pins)
	if err != nil {
		t.Fatal(err)
	}
	// Crash after durable preparation. Later work must remain outside its proof.
	later := []byte("newer live bytes\n")
	if err = os.WriteFile(filepath.Join(repo, "file.txt"), later, 0600); err != nil {
		t.Fatal(err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err = ActivateFinish(canceled, repo, db); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled activation: %v", err)
	}
	result, err := ActivateFinish(ctx, repo, db)
	if err != nil || result.State != "completed" || result.ManifestDigest != saved.ManifestDigest || result.Unpublished != 1 {
		t.Fatalf("resume %+v %v", result, err)
	}
	bytes, err := os.ReadFile(filepath.Join(repo, "file.txt"))
	if err != nil || string(bytes) != string(later) {
		t.Fatal("resume overwrote later work")
	}
	if _, err = db.SQL().ExecContext(ctx, "DELETE FROM capture_events WHERE seq=1"); err == nil {
		t.Fatal("archival capture pruned")
	}
	if _, err = db.SQL().ExecContext(ctx, "UPDATE capture_ops SET after_oid='wrong' WHERE event_seq=1"); err == nil {
		t.Fatal("archival version rewritten")
	}
}
