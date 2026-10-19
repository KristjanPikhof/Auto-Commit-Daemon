//go:build integration

package integration_test

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/migration"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

func TestFinishMigrationLosslessRealLegacyFixtures(t *testing.T) {
	for _, scenario := range []string{"protected-only", "pending-drain", "failed-drain", "prepared", "git-applied", "history-repair", "staged", "linked-worktree", "partial-checkpoint", "older-schema"} {
		t.Run(scenario, func(t *testing.T) {
			ctx := context.Background()
			f := newFinishLegacyFixture(t, scenario)
			before := f.manifest(t)
			projected, err := state.ReadFinishLegacyCutover(ctx, f.DBPath)
			if err != nil || projected.Available {
				t.Fatalf("readonly old projection: %+v %v", projected, err)
			}
			if got := f.manifest(t); !reflect.DeepEqual(before, got) {
				t.Fatal("readonly legacy inspection mutated fixture")
			}
			db, err := state.Open(ctx, f.DBPath)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			cutover, err := migration.ActivateFinish(ctx, f.Repo, db)
			attention := scenario == "history-repair" || scenario == "older-schema"
			if attention {
				if !errors.Is(err, migration.ErrFinishLegacyAttention) || cutover.State != "needs_attention" {
					t.Fatalf("repair ambiguity: %+v %v", cutover, err)
				}
			} else if err != nil || cutover.State != "completed" {
				t.Fatalf("activate: %+v %v", cutover, err)
			}
			after := f.manifest(t)
			if !reflect.DeepEqual(before.Worktrees, after.Worktrees) || !reflect.DeepEqual(before.Config, after.Config) {
				t.Fatal("live files, index, branch, or config changed")
			}
			for _, line := range strings.Split(strings.TrimSpace(before.Refs), "\n") {
				if !strings.Contains(after.Refs, line) {
					t.Fatalf("old ref altered: %s", line)
				}
			}
			for oid, data := range before.Objects {
				if !bytes.Equal(after.Objects[oid], data) {
					t.Fatalf("immutable object changed: %s", oid)
				}
			}
			allowed := map[string]bool{}
			if scenario == "prepared" || scenario == "git-applied" {
				for _, table := range []string{"self_publications", "operations", "operation_steps", "decision_records", "checkpoint_publications", "publish_state", "daemon_meta"} {
					allowed[table] = true
				}
			}
			if scenario == "git-applied" {
				allowed["capture_events"] = true
			}
			for table, rows := range before.Tables {
				if !allowed[table] && !bytes.Equal(after.Tables[table], rows) {
					t.Fatalf("legacy rows changed in %s", table)
				}
			}
			// Old versions have no live provider/replay path. Their only new refs pin proof.
			if !strings.Contains(after.Refs, cutover.SnapshotRef) {
				t.Fatal("cutover snapshot was not retained")
			}
			if scenario == "git-applied" && cutover.Unpublished != 0 {
				t.Fatalf("proven old branch publication remained pending: %+v", cutover)
			}
			if scenario != "git-applied" && cutover.Unpublished != 2 {
				t.Fatalf("retained work falsely marked published: %+v", cutover)
			}
			stable := f.manifest(t)
			again, againErr := migration.ActivateFinish(ctx, f.Repo, db)
			if (againErr != nil) != attention || again.ManifestDigest != cutover.ManifestDigest {
				t.Fatalf("repeat activation: %+v %v", again, againErr)
			}
			repeated := f.manifest(t)
			if !reflect.DeepEqual(stable.Worktrees, repeated.Worktrees) || stable.Refs != repeated.Refs {
				t.Fatal("repeat activation changed Git ownership")
			}
			// Pruning dangling loose objects must retain both older captures and an
			// unapplied, authored publication target that is only in the old journal.
			f.git(t, f.Repo, "reflog", "expire", "--expire=now", "--all")
			f.git(t, f.Repo, "gc", "--prune=now")
			if got := f.git(t, f.Repo, "cat-file", "blob", f.CapturedOnlyBlob); !bytes.Equal(got, []byte("earlier captured bytes\n")) {
				t.Fatal("capture-only version was pruned")
			}
			if scenario == "prepared" {
				if got := f.git(t, f.Repo, "cat-file", "commit", f.TargetHead); !bytes.Equal(got, before.Objects[f.TargetHead]) {
					t.Fatal("unapplied authored target was pruned")
				}
			}
		})
	}
}

func TestFinishMigrationPreparedResumeAndMovedBranchAttention(t *testing.T) {
	ctx := context.Background()
	f := newFinishLegacyFixture(t, "prepared")
	db, err := state.Open(ctx, f.DBPath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	// Moving the branch is a real crash-recovery ambiguity, not permission to
	// replay its old target. The earlier captured blob must still be retained.
	f.git(t, f.Repo, "update-ref", "refs/heads/main", f.CheckpointCommit, f.SourceHead)
	before := f.manifest(t)
	result, err := migration.ActivateFinish(ctx, f.Repo, db)
	if !errors.Is(err, migration.ErrFinishLegacyAttention) || result.ReasonCode != "legacy_publication_ambiguous" {
		t.Fatalf("moved branch %+v %v", result, err)
	}
	after := f.manifest(t)
	if !reflect.DeepEqual(before.Worktrees, after.Worktrees) {
		t.Fatal("ambiguous migration changed worktree/index/branch")
	}
	// Restore exact known ownership in this isolated fixture, then explicitly
	// retry activation. It settles the saved journal without a provider call.
	f.git(t, f.Repo, "update-ref", "refs/heads/main", f.SourceHead, f.CheckpointCommit)
	result, err = migration.ActivateFinish(ctx, f.Repo, db)
	if err != nil || result.State != "completed" {
		t.Fatalf("exact retry %+v %v", result, err)
	}
}

func TestFinishMigrationMissingJournalObjectKeepsAvailableCapturedVersions(t *testing.T) {
	ctx := context.Background()
	f := newFinishLegacyFixture(t, "prepared")
	objectPath := filepath.Join(f.Repo, ".git", "objects", f.TargetHead[:2], f.TargetHead[2:])
	targetBytes, err := os.ReadFile(objectPath)
	if err != nil {
		t.Fatal(err)
	}
	if err = os.Remove(objectPath); err != nil {
		t.Fatal(err)
	}
	db, err := state.Open(ctx, f.DBPath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	result, err := migration.ActivateFinish(ctx, f.Repo, db)
	if !errors.Is(err, migration.ErrFinishLegacyAttention) || result.ReasonCode != "legacy_object_missing" || result.SnapshotRef == "" {
		t.Fatalf("missing proof: %+v %v", result, err)
	}
	projected, err := state.ReadFinishLegacyCutover(ctx, f.DBPath)
	if err != nil || projected.ReasonCode != "legacy_object_missing" {
		t.Fatalf("read-only attention: %+v %v", projected, err)
	}
	f.git(t, f.Repo, "reflog", "expire", "--expire=now", "--all")
	f.git(t, f.Repo, "gc", "--prune=now")
	if got := f.git(t, f.Repo, "cat-file", "blob", f.CapturedOnlyBlob); !bytes.Equal(got, []byte("earlier captured bytes\n")) {
		t.Fatal("unrelated missing journal pruned retained capture")
	}
	if _, err = db.SQL().ExecContext(ctx, "DELETE FROM capture_events WHERE seq=1"); err == nil {
		t.Fatal("failure allowed archival capture pruning")
	}
	if err = os.MkdirAll(filepath.Dir(objectPath), 0700); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(objectPath, targetBytes, 0444); err != nil {
		t.Fatal(err)
	}
	result, err = migration.ActivateFinish(ctx, f.Repo, db)
	if err != nil || result.State != "completed" {
		t.Fatalf("restored immutable proof: %+v %v", result, err)
	}
}
