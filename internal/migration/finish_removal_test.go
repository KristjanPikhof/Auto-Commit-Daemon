package migration

import (
	"bytes"
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/central"
	gitpkg "github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/testutil"
)

// External history and interrupted old repairs must never prevent fresh protection.
func TestFinishRemovalLegacyAmbiguityEnablesProtectionWithoutAdoption(t *testing.T) {
	for _, scenario := range []string{"external-rewrite", "interrupted-repair", "missing-old-blob"} {
		t.Run(scenario, func(t *testing.T) {
			ctx := context.Background()
			repo, wt := migrationRepo(t, ctx)
			run := func(args ...string) []byte {
				t.Helper()
				out, err := gitpkg.Run(ctx, gitpkg.RunOpts{Dir: repo}, args...)
				if err != nil {
					t.Fatal(err)
				}
				return out
			}
			oldBlob, err := gitpkg.HashObjectStdinDurable(ctx, repo, []byte("original captured bytes\n"))
			if err != nil {
				t.Fatal(err)
			}
			newBlob, err := gitpkg.HashObjectStdinDurable(ctx, repo, []byte("captured correction\n"))
			if err != nil {
				t.Fatal(err)
			}
			tree, err := gitpkg.WriteTreeDurable(ctx, repo, filepath.Join(t.TempDir(), "private.index"), []gitpkg.IndexEntry{{Mode: "100644", OID: oldBlob, Path: "file.txt"}})
			if err != nil {
				t.Fatal(err)
			}
			original, err := gitpkg.CommitTreeDurable(ctx, repo, tree, "Original history", "Original Author", "original@example.com")
			if err != nil {
				t.Fatal(err)
			}
			replacement, err := gitpkg.CommitTreeDurable(ctx, repo, tree, "External rewritten history", "Rewrite Author", "rewrite@example.com")
			if err != nil {
				t.Fatal(err)
			}
			run("update-ref", "refs/heads/main", replacement)
			run("read-tree", replacement)
			later := []byte("later live work stays untouched\n")
			if err = os.WriteFile(filepath.Join(repo, "file.txt"), later, 0600); err != nil {
				t.Fatal(err)
			}
			run("update-index", "--cacheinfo", "100644", newBlob, "file.txt")
			rawIndex, err := os.ReadFile(filepath.Join(wt.GitDir, "index"))
			if err != nil {
				t.Fatal(err)
			}
			dbPath := state.DBPathFromGitDir(wt.GitDir)
			db, err := state.Open(ctx, dbPath)
			if err != nil {
				t.Fatal(err)
			}
			beforeOID := oldBlob
			if scenario == "missing-old-blob" {
				beforeOID = strings.Repeat("f", 40)
			}
			seq, err := testutil.SeedLegacyCapture(ctx, db, state.CaptureEvent{BranchRef: "refs/heads/main", BranchGeneration: 3, BaseHead: original, Operation: "modify", Path: "file.txt", Fidelity: "exact"}, []state.CaptureOp{{Op: "modify", Path: "file.txt", BeforeOID: sql.NullString{String: beforeOID, Valid: true}, BeforeMode: sql.NullString{String: "100644", Valid: true}, AfterOID: sql.NullString{String: newBlob, Valid: true}, AfterMode: sql.NullString{String: "100644", Valid: true}, Fidelity: "exact"}})
			if err != nil {
				t.Fatal(err)
			}
			if scenario == "interrupted-repair" {
				if _, err = db.SQL().ExecContext(ctx, `INSERT INTO intent_repairs(id,branch_ref,branch_generation,status,expected_head,plan_digest,old_head,new_head,created_ts,updated_ts) VALUES('old-repair','refs/heads/main',3,'git_applied',?,'saved-plan',?,?,1,1)`, original, original, replacement); err != nil {
					t.Fatal(err)
				}
			}
			if _, err = db.SQL().ExecContext(ctx, "PRAGMA user_version=32"); err != nil {
				t.Fatal(err)
			}
			digest, err := state.ReadLegacyEvidenceDigest(ctx, dbPath)
			if err != nil {
				t.Fatal(err)
			}
			if err = db.Close(); err != nil {
				t.Fatal(err)
			}
			record := central.RepoRecord{Path: wt.Root, StateDB: dbPath, CommonDir: wt.CommonDir, RepositoryID: central.CanonicalID(wt.CommonDir), WorktreeID: central.CanonicalID(wt.Root)}
			backup := filepath.Join(t.TempDir(), "original-v32.db")
			plan, err := Preflight(ctx, record, backup)
			if err != nil {
				t.Fatalf("old chain blocked preview: %v", err)
			}
			results, err := ApplyAll(ctx, []RepositoryPlan{plan})
			if err != nil || len(results) != 1 || results[0].CheckpointID == "" {
				t.Fatalf("protection activation: %+v %v", results, err)
			}
			expected := "completed"
			if scenario != "external-rewrite" {
				expected = "needs_attention"
			}
			if results[0].Legacy.State != expected || results[0].Legacy.SnapshotRef == "" {
				t.Fatalf("legacy outcome %+v", results[0].Legacy)
			}
			backupDigest, err := state.ReadLegacyEvidenceDigest(ctx, backup)
			if err != nil || backupDigest != digest {
				t.Fatalf("backup changed originals %s %s %v", digest, backupDigest, err)
			}
			db, err = state.Open(ctx, dbPath)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			var stateName, savedOID string
			if err = db.ReadSQL().QueryRowContext(ctx, `SELECT e.state,o.before_oid FROM capture_events e JOIN capture_ops o ON o.event_seq=e.seq WHERE e.seq=?`, seq).Scan(&stateName, &savedOID); err != nil || stateName != "pending" || savedOID != beforeOID {
				t.Fatalf("original rewritten %s %s %v", stateName, savedOID, err)
			}
			if _, err = db.SQL().ExecContext(ctx, "UPDATE capture_ops SET before_oid='wrong' WHERE event_seq=?", seq); err == nil {
				t.Fatal("old evidence not sealed")
			}
			if head := strings.TrimSpace(string(run("rev-parse", "HEAD"))); head != replacement {
				t.Fatal("activation changed branch")
			}
			afterIndex, err := os.ReadFile(filepath.Join(wt.GitDir, "index"))
			if err != nil || !bytes.Equal(afterIndex, rawIndex) {
				t.Fatal("activation changed approved/later staging")
			}
			live, err := os.ReadFile(filepath.Join(repo, "file.txt"))
			if err != nil || !bytes.Equal(live, later) {
				t.Fatal("activation overwrote later work")
			}
			run("gc", "--prune=now")
			for _, oid := range []string{original, replacement, oldBlob, newBlob} {
				run("cat-file", "-e", oid)
			}
			if author := strings.TrimSpace(string(run("show", "-s", "--format=%an <%ae>", original))); author != "Original Author <original@example.com>" {
				t.Fatal("original author changed")
			}
		})
	}
}
