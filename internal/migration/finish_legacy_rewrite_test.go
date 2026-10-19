package migration

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	gitpkg "github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/testutil"
)

func TestFinishLegacyRewriteMemberActivationRetainsOriginals(t *testing.T) {
	for _, invalid := range []bool{false, true} {
		t.Run(fmt.Sprintf("malformed_member=%t", invalid), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
			defer cancel()
			repo, wt := migrationRepo(t, ctx)
			db, err := state.Open(ctx, state.DBPathFromGitDir(wt.GitDir))
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			if err := testutil.InitLegacy(ctx, db); err != nil {
				t.Fatal(err)
			}
			tree, err := gitpkg.WriteTreeDurable(ctx, repo, filepath.Join(t.TempDir(), "empty.index"), nil)
			if err != nil {
				t.Fatal(err)
			}
			commit := func(message string) string {
				t.Helper()
				oid, err := gitpkg.CommitTreeDurable(ctx, repo, tree, message, "Original author", "original@example.com")
				if err != nil {
					t.Fatal(err)
				}
				return oid
			}
			head, old := commit("Current head"), commit("Archived original")
			if err := gitpkg.UpdateRef(ctx, repo, "refs/heads/main", head, ""); err != nil {
				t.Fatal(err)
			}
			if _, err := gitpkg.Run(ctx, gitpkg.RunOpts{Dir: repo}, "read-tree", head); err != nil {
				t.Fatal(err)
			}
			index, err := os.ReadFile(filepath.Join(wt.GitDir, "index"))
			if err != nil {
				t.Fatal(err)
			}
			live := []byte("Later work stays live\n")
			if err := os.WriteFile(filepath.Join(repo, "later.txt"), live, 0600); err != nil {
				t.Fatal(err)
			}
			raw := fmt.Sprintf(`[ {"old_oid":%q,"original_message":"Archived original"} ]`, old)
			if invalid {
				raw = fmt.Sprintf(`[{"old_oid":"not-an-oid"},{"old_oid":%q,"original_message":"Archived original"}]`, old)
			}
			if _, err := db.SQL().ExecContext(ctx, `INSERT INTO rewrite_plans(id,created_ts,updated_ts,revision,branch_ref,expected_head,validation_status) VALUES('legacy',1,1,1,'refs/heads/main',?,'valid')`, head); err != nil {
				t.Fatal(err)
			}
			if _, err := db.SQL().ExecContext(ctx, `INSERT INTO rewrite_plan_groups(plan_id,ord,members_json,proposed_message,grouping_reason) VALUES('legacy',0,?,'Reviewed proposal','Saved boundary')`, raw); err != nil {
				t.Fatal(err)
			}
			if invalid {
				// A corrupt earlier group must not prevent reading the later group.
				if _, err := db.SQL().ExecContext(ctx, `INSERT INTO rewrite_plan_groups(plan_id,ord,members_json,proposed_message,grouping_reason) VALUES('legacy',-1,'[{','Bad','')`); err != nil {
					t.Fatal(err)
				}
			}
			saved, err := ActivateFinish(ctx, repo, db)
			if invalid {
				if !errors.Is(err, ErrFinishLegacyAttention) || !errors.Is(err, state.ErrFinishIdentity) || saved.State != "needs_attention" || saved.SnapshotRef == "" {
					t.Fatalf("malformed activation: %+v error=%v", saved, err)
				}
			} else if err != nil || saved.State != "completed" {
				t.Fatalf("canonical old member rejected: %+v error=%v", saved, err)
			}
			if _, err := gitpkg.Run(ctx, gitpkg.RunOpts{Dir: repo}, "gc", "--prune=now"); err != nil {
				t.Fatal(err)
			}
			if out, err := gitpkg.Run(ctx, gitpkg.RunOpts{Dir: repo}, "show", "-s", "--format=%an%n%ae%n%B", old); err != nil || strings.TrimSpace(string(out)) != "Original author\noriginal@example.com\nArchived original" {
				t.Fatalf("archived original lost or changed: %q error=%v", out, err)
			}
			var original, applyStatus string
			if err := db.ReadSQL().QueryRowContext(ctx, "SELECT members_json FROM rewrite_plan_groups WHERE ord=0").Scan(&original); err != nil || original != raw {
				t.Fatalf("raw metadata changed: %q error=%v", original, err)
			}
			if err := db.ReadSQL().QueryRowContext(ctx, "SELECT apply_status FROM rewrite_plans WHERE id='legacy'").Scan(&applyStatus); err != nil || applyStatus != "pending" {
				t.Fatalf("archived proposal was applied: %q error=%v", applyStatus, err)
			}
			var cutovers int
			if err := db.ReadSQL().QueryRowContext(ctx, "SELECT COUNT(*) FROM finish_legacy_cutover").Scan(&cutovers); err != nil || (invalid && cutovers != 0) || (!invalid && cutovers != 1) {
				t.Fatalf("cutover count=%d error=%v", cutovers, err)
			}
			again, err := ActivateFinish(ctx, repo, db)
			if again.State != saved.State || (invalid && !errors.Is(err, ErrFinishLegacyAttention)) || (!invalid && err != nil) {
				t.Fatalf("reconnect: %+v error=%v", again, err)
			}
			afterIndex, err := os.ReadFile(filepath.Join(wt.GitDir, "index"))
			if err != nil || !bytes.Equal(index, afterIndex) {
				t.Fatal("activation changed native index")
			}
			if actual, err := gitpkg.RevParse(ctx, repo, "HEAD"); err != nil || actual != head {
				t.Fatal("activation changed branch HEAD")
			}
			if body, err := os.ReadFile(filepath.Join(repo, "later.txt")); err != nil || !bytes.Equal(body, live) {
				t.Fatal("activation changed live work")
			}
		})
	}
}
