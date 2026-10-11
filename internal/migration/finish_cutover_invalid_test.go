package migration

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	gitpkg "github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/testutil"
)

func TestFinishLegacyMalformedIdentityPreservesValidObjectsThroughGC(t *testing.T) {
	for _, malformed := range []string{"nonhex", "uppercase"} {
		t.Run(malformed, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
			defer cancel()
			repo, wt := migrationRepo(t, ctx)
			run := func(args ...string) string {
				t.Helper()
				out, err := gitpkg.Run(ctx, gitpkg.RunOpts{Dir: repo}, args...)
				if err != nil {
					t.Fatalf("git %v: %v", args, err)
				}
				return strings.TrimSpace(string(out))
			}
			tree, err := gitpkg.WriteTreeDurable(ctx, repo, filepath.Join(t.TempDir(), "empty.index"), nil)
			if err != nil {
				t.Fatal(err)
			}
			head, err := gitpkg.CommitTreeDurable(ctx, repo, tree, "Current branch", "Current Author", "current@example.com")
			if err != nil {
				t.Fatal(err)
			}
			run("update-ref", "refs/heads/main", head)
			run("read-tree", head)
			staged, err := gitpkg.HashObjectStdinDurable(ctx, repo, []byte("staged bytes\n"))
			if err != nil {
				t.Fatal(err)
			}
			run("update-index", "--add", "--cacheinfo", "100644", staged, "live.txt")
			later := []byte("later worktree bytes\n")
			if err = os.WriteFile(filepath.Join(repo, "live.txt"), later, 0600); err != nil {
				t.Fatal(err)
			}
			index, err := os.ReadFile(filepath.Join(wt.GitDir, "index"))
			if err != nil {
				t.Fatal(err)
			}

			// Neither original object has a Git ref or index entry. Only legacy
			// SQLite provenance knows about them before activation.
			blob, err := gitpkg.HashObjectStdinDurable(ctx, repo, []byte("older sensitive captured content\n"))
			if err != nil {
				t.Fatal(err)
			}
			commit, err := gitpkg.CommitTreeDurable(ctx, repo, tree, "Original captured base", "Original Author", "original@example.com")
			if err != nil {
				t.Fatal(err)
			}
			reachable := run("rev-list", "--objects", "--all")
			if strings.Contains(reachable, blob) || strings.Contains(reachable, commit) {
				t.Fatal("fixture original objects already reachable")
			}
			db, err := state.Open(ctx, state.DBPathFromGitDir(wt.GitDir))
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			bad := "!malformed-object"
			if malformed == "uppercase" {
				bad = strings.ToUpper(blob)
			}
			seq, err := testutil.SeedLegacyCapture(ctx, db, state.CaptureEvent{BranchRef: "refs/heads/main", BranchGeneration: 3, BaseHead: commit, Operation: "modify", Path: ".env", Fidelity: "exact"}, []state.CaptureOp{{Op: "modify", Path: ".env", BeforeOID: sql.NullString{String: bad, Valid: true}, BeforeMode: sql.NullString{String: "100644", Valid: true}, AfterOID: sql.NullString{String: blob, Valid: true}, AfterMode: sql.NullString{String: "100644", Valid: true}, Fidelity: "exact"}})
			if err != nil {
				t.Fatal(err)
			}
			// Missing well-formed identities still aggregate with malformed ones.
			if _, err = testutil.SeedLegacyCapture(ctx, db, state.CaptureEvent{BranchRef: "refs/heads/main", BranchGeneration: 3, BaseHead: commit, Operation: "create", Path: "missing.txt", Fidelity: "exact"}, []state.CaptureOp{{Op: "create", Path: "missing.txt", AfterOID: sql.NullString{String: strings.Repeat("f", 40), Valid: true}, AfterMode: sql.NullString{String: "100644", Valid: true}, Fidelity: "exact"}}); err != nil {
				t.Fatal(err)
			}
			originalRows := finishLegacyCaptureRows(t, ctx, db)
			failed, err := ActivateFinish(ctx, repo, db)
			if !errors.Is(err, ErrFinishLegacyAttention) || !errors.Is(err, state.ErrFinishIdentity) || !errors.Is(err, errFinishLegacyObjectsMissing) || failed.State != "needs_attention" {
				t.Fatalf("malformed inventory %+v: %v", failed, err)
			}
			if failed.ReasonCode != "legacy_inventory_invalid" || failed.SnapshotRef == "" || failed.SnapshotOID == "" {
				t.Errorf("available snapshot not durably recorded: %+v", failed)
			}
			run("gc", "--prune=now")
			if got := run("cat-file", "blob", blob); got != "older sensitive captured content" {
				t.Fatalf("valid captured blob lost: %q", got)
			}
			run("cat-file", "-e", commit+"^{commit}")
			stored, err := state.LoadFinishLegacyCutover(ctx, db)
			if err != nil || stored.State != "needs_attention" || stored.SnapshotRef != failed.SnapshotRef || stored.SnapshotOID != failed.SnapshotOID {
				t.Fatalf("durable attention %+v: %v", stored, err)
			}
			if run("rev-parse", stored.SnapshotRef) != stored.SnapshotOID {
				t.Fatal("saved snapshot proof changed")
			}
			retried, err := ActivateFinish(ctx, repo, db)
			if !errors.Is(err, ErrFinishLegacyAttention) || retried.State != "needs_attention" || retried.SnapshotRef != stored.SnapshotRef || retried.SnapshotOID != stored.SnapshotOID {
				t.Fatalf("malformed retry completed or changed proof: %+v %v", retried, err)
			}
			for _, table := range []string{"finish_legacy_cutover", "finish_legacy_retirements"} {
				var count int
				if err = db.ReadSQL().QueryRowContext(ctx, "SELECT COUNT(*) FROM "+table).Scan(&count); err != nil || count != 0 {
					t.Fatalf("malformed inventory advanced %s: %d %v", table, count, err)
				}
			}
			if _, err = db.SQL().ExecContext(ctx, "UPDATE capture_ops SET after_oid='changed' WHERE event_seq=?", seq); err == nil {
				t.Fatal("failed activation left capture provenance mutable")
			}
			if got := finishLegacyCaptureRows(t, ctx, db); got != originalRows {
				t.Fatal("activation changed original capture rows")
			}
			afterIndex, err := os.ReadFile(filepath.Join(wt.GitDir, "index"))
			if err != nil || !bytes.Equal(afterIndex, index) || run("rev-parse", "HEAD") != head || run("symbolic-ref", "HEAD") != "refs/heads/main" {
				t.Fatalf("activation changed HEAD or staged index: %v", err)
			}
			body, err := os.ReadFile(filepath.Join(repo, "live.txt"))
			if err != nil || !bytes.Equal(body, later) {
				t.Fatal("activation replaced later worktree bytes")
			}
		})
	}
}

func finishLegacyCaptureRows(t *testing.T, ctx context.Context, db *state.DB) string {
	t.Helper()
	queries := []string{
		`SELECT json_group_array(json_array(seq,branch_ref,branch_generation,base_head,operation,path,old_path,fidelity,captured_ts,published_ts,state,commit_oid,error,message)) FROM (SELECT * FROM capture_events ORDER BY seq)`,
		`SELECT json_group_array(json_array(event_seq,ord,op,path,old_path,before_oid,before_mode,after_oid,after_mode,fidelity)) FROM (SELECT * FROM capture_ops ORDER BY event_seq,ord)`,
	}
	var result []string
	for _, query := range queries {
		var rows string
		if err := db.ReadSQL().QueryRowContext(ctx, query).Scan(&rows); err != nil {
			t.Fatal(err)
		}
		result = append(result, rows)
	}
	return strings.Join(result, "\n")
}

func TestFinishLegacyCompletedActivationRechecksBlobAvailability(t *testing.T) {
	ctx := t.Context()
	repo, wt := migrationRepo(t, ctx)
	db, err := state.Open(ctx, state.DBPathFromGitDir(wt.GitDir))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	blob, err := gitpkg.HashObjectStdinDurable(ctx, repo, []byte("original captured bytes\n"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := testutil.SeedLegacyCapture(ctx, db, state.CaptureEvent{BranchRef: "refs/heads/main", BranchGeneration: 1, Operation: "create", Path: "old.txt", Fidelity: "exact"}, []state.CaptureOp{{Op: "create", Path: "old.txt", AfterOID: sql.NullString{String: blob, Valid: true}, AfterMode: sql.NullString{String: "100644", Valid: true}, Fidelity: "exact"}}); err != nil {
		t.Fatal(err)
	}
	originalRows := finishLegacyCaptureRows(t, ctx, db)
	completed, err := ActivateFinish(ctx, repo, db)
	if err != nil || completed.State != "completed" {
		t.Fatalf("initial activation %+v %v", completed, err)
	}
	objectPath := filepath.Join(wt.CommonDir, "objects", blob[:2], blob[2:])
	original, err := os.ReadFile(objectPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(objectPath); err != nil {
		t.Fatal(err)
	}
	missing, err := ActivateFinish(ctx, repo, db)
	if !errors.Is(err, ErrFinishLegacyAttention) || missing.State != "needs_attention" || missing.ReasonCode != "legacy_object_missing" {
		t.Fatalf("missing retained blob accepted %+v %v", missing, err)
	}
	if missing.SnapshotRef != completed.SnapshotRef || missing.SnapshotOID != completed.SnapshotOID || finishLegacyCaptureRows(t, ctx, db) != originalRows {
		t.Fatal("missing blob changed immutable proof or capture provenance")
	}
	if got, err := gitpkg.RevParse(ctx, repo, completed.SnapshotRef); err != nil || got != completed.SnapshotOID {
		t.Fatal("missing blob refusal moved snapshot")
	}
	if err := os.WriteFile(objectPath, original, 0444); err != nil {
		t.Fatal(err)
	}
	recovered, err := ActivateFinish(ctx, repo, db)
	if err != nil || recovered.State != "completed" || recovered.SnapshotRef != completed.SnapshotRef || recovered.SnapshotOID != completed.SnapshotOID {
		t.Fatalf("exact restored blob cannot retry %+v %v", recovered, err)
	}
}
