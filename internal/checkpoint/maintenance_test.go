package checkpoint

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	gitpkg "github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

func TestMaintenanceLicenseFailureRecoversAfterRestart(t *testing.T) {
	ctx := context.Background()
	repo, db := checkpointFixture(t, ctx)
	store := Store{DB: db}
	cp, err := store.Create(ctx, Request{RepoRoot: repo, WorktreeID: WorktreeID(repo),
		Reason: state.CheckpointReasonManualBarrier, ObservationEpoch: 1, CoverageEpoch: 1})
	if err != nil {
		t.Fatal(err)
	}
	gitPath, err := exec.LookPath("git")
	if err != nil {
		t.Fatal(err)
	}
	shim := t.TempDir()
	flag := filepath.Join(shim, "unaccepted")
	if err := os.WriteFile(flag, nil, 0600); err != nil {
		t.Fatal(err)
	}
	// An actual failing Git subprocess reproduces the reported macOS error.
	script := fmt.Sprintf("#!/bin/sh\nif [ -e '%s' ] && [ \"$1\" = rev-list ]; then\n echo \"You have not agreed to the Xcode license agreements. Please run 'sudo xcodebuild -license'.\" >&2\n exit 69\nfi\nexec '%s' \"$@\"\n", flag, gitPath)
	if err := os.WriteFile(filepath.Join(shim, "git"), []byte(script), 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", shim+string(os.PathListSeparator)+os.Getenv("PATH"))
	now := time.Now().Truncate(time.Second)
	var previous MaintenanceStatus
	for i, delay := range []time.Duration{time.Minute, 2 * time.Minute, 5 * time.Minute, 15 * time.Minute, 15 * time.Minute} {
		result, err := store.Maintain(ctx, repo, WorktreeID(repo), now, previous)
		if err != nil {
			t.Fatal(err)
		}
		if result.State != "prerequisite" || result.OverBudget || result.NextAttemptTS != now.Add(delay).Unix() || !strings.Contains(result.Error, "Xcode license") {
			t.Fatalf("attempt %d: %+v", i, result)
		}
		// Reopen the database as a restarted worker would, retaining its deadline.
		reopened, err := state.Open(ctx, db.Path())
		if err != nil {
			t.Fatal(err)
		}
		restarted := Store{DB: reopened}
		saved, err := restarted.LoadMaintenance(ctx)
		if err != nil {
			t.Fatal(err)
		}
		unchanged, err := restarted.Maintain(ctx, repo, WorktreeID(repo), now.Add(delay-time.Second), saved)
		if err != nil || unchanged != result {
			t.Fatalf("restart ignored deadline: %+v %v", unchanged, err)
		}
		if err := reopened.Close(); err != nil {
			t.Fatal(err)
		}
		previous = result
		now = now.Add(delay)
	}
	if err := os.Remove(flag); err != nil {
		t.Fatal(err)
	}
	result, err := store.Maintain(ctx, repo, WorktreeID(repo), now, previous)
	if err != nil {
		t.Fatal(err)
	}
	if result.State != "healthy" || result.Error != "" || result.Failures != 0 || result.LastSuccessTS != now.Unix() || result.NextAttemptTS != now.Add(time.Hour).Unix() {
		t.Fatalf("recovery: %+v", result)
	}
	if oid, err := gitpkg.RevParse(ctx, repo, cp.Checkpoint.Ref); err != nil || oid != cp.Checkpoint.CommitOID {
		t.Fatalf("protected checkpoint changed: %s %v", oid, err)
	}
	legacy, _, _ := state.MetaGet(ctx, db, "protection.retention_over_budget")
	if legacy != "false" {
		t.Fatalf("legacy warning remained: %s", legacy)
	}
}

func TestMaintenanceDistinguishesBudgetAndFailedMeasurement(t *testing.T) {
	ctx := context.Background()
	repo, db := checkpointFixture(t, ctx)
	store := Store{DB: db}
	_, err := store.Create(ctx, Request{RepoRoot: repo, WorktreeID: WorktreeID(repo), Reason: state.CheckpointReasonManualBarrier, ObservationEpoch: 1, CoverageEpoch: 1})
	if err != nil {
		t.Fatal(err)
	}
	store.retentionInventory = func(context.Context, string, []string) (map[string]int64, error) {
		return map[string]int64{"object": DefaultContentBudget + 1}, nil
	}
	now := time.Now()
	over, err := store.Maintain(ctx, repo, WorktreeID(repo), now, MaintenanceStatus{})
	if err != nil || over.State != "over_budget" || !over.OverBudget {
		t.Fatalf("budget: %+v %v", over, err)
	}
	store.retentionInventory = func(context.Context, string, []string) (map[string]int64, error) {
		return nil, errors.New("temporary inventory timeout")
	}
	failed, err := store.Maintain(ctx, repo, WorktreeID(repo), now.Add(time.Hour), over)
	if err != nil || failed.State != "retrying" || failed.NeedsAction() || failed.LastSuccessTS != over.LastSuccessTS || failed.ContentBytes != over.ContentBytes {
		t.Fatalf("failure: %+v %v", failed, err)
	}
	if strings.Contains(failed.Summary(), "exceed") || strings.Contains(failed.NextAction(), "repo gc") {
		t.Fatal("failed measurement presented as excess storage")
	}
}

func TestMaintenanceLegacyFailureIsNotBudget(t *testing.T) {
	result := DecodeMaintenance("", "needs_action")
	if result.State != "retrying" || result.OverBudget || result.NeedsAction() || result.NextAttemptTS != 0 {
		t.Fatalf("legacy: %+v", result)
	}
}

func TestMaintenanceRefMovementRemainsNeedsAction(t *testing.T) {
	ctx := context.Background()
	repo, db := checkpointFixture(t, ctx)
	store := Store{DB: db}
	cp, err := store.Create(ctx, Request{RepoRoot: repo, WorktreeID: WorktreeID(repo), Reason: state.CheckpointReasonManualBarrier, ObservationEpoch: 1, CoverageEpoch: 1})
	if err != nil {
		t.Fatal(err)
	}
	items, err := state.RetentionCheckpoints(ctx, db, WorktreeID(repo))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := state.PrepareCheckpointPrune(ctx, db, items[0], "sha256:"+strings.Repeat("0", 64)); err != nil {
		t.Fatal(err)
	}
	other, err := gitpkg.CommitTreeDurable(ctx, repo, cp.Checkpoint.TreeOID, "different checkpoint\n", IdentityName, IdentityEmail)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := gitpkg.Run(ctx, gitpkg.RunOpts{Dir: repo}, "update-ref", cp.Checkpoint.Ref, other); err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	result, err := store.Maintain(ctx, repo, WorktreeID(repo), now, MaintenanceStatus{})
	if err != nil || result.State != "needs_action" {
		t.Fatalf("first: %+v %v", result, err)
	}
	result, err = store.Maintain(ctx, repo, WorktreeID(repo), time.Unix(result.NextAttemptTS, 0), result)
	if err != nil || result.State != "needs_action" {
		t.Fatalf("unsafe retry cleared warning: %+v %v", result, err)
	}
	if oid, err := gitpkg.RevParse(ctx, repo, cp.Checkpoint.Ref); err != nil || oid != other {
		t.Fatalf("moved ref was deleted: %s %v", oid, err)
	}
	// Restoring the exact recorded ref makes recovery provable again.
	if _, err := gitpkg.Run(ctx, gitpkg.RunOpts{Dir: repo}, "update-ref", cp.Checkpoint.Ref, cp.Checkpoint.CommitOID); err != nil {
		t.Fatal(err)
	}
	result, err = store.Maintain(ctx, repo, WorktreeID(repo), time.Unix(result.NextAttemptTS, 0), result)
	if err != nil || result.State != "healthy" {
		t.Fatalf("proven recovery: %+v %v", result, err)
	}
	pending, err := state.PreparedCheckpointPrunes(ctx, db)
	if err != nil || len(pending) != 0 {
		t.Fatalf("recovered prune still pending: %+v %v", pending, err)
	}

}
