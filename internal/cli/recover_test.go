package cli

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	pausepkg "github.com/KristjanPikhof/Auto-Commit-Daemon/internal/pause"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

func makeRegisteredGitRepoStateDB(t *testing.T) (repoDir, stateDB string, db *state.DB) {
	t.Helper()
	roots := withIsolatedHome(t)
	repoDir, stateDB, db = makeSeededRepoStateDB(t)
	registerRepo(t, roots, repoDir, stateDB, "test")
	return repoDir, stateDB, db
}

func TestRecover_RequiresLegacySafetyFlags(t *testing.T) {
	repo, _, _ := makeRegisteredGitRepoStateDB(t)
	var out bytes.Buffer
	if err := runRecover(context.Background(), &out, repo, false, false, false, true, false); err == nil || !strings.Contains(err.Error(), "pass --auto") {
		t.Fatalf("runRecover err=%v want --auto refusal", err)
	}
	if err := runRecover(context.Background(), &out, repo, true, false, false, true, false); err == nil || !strings.Contains(err.Error(), "without --yes") {
		t.Fatalf("runRecover err=%v want --yes refusal", err)
	}
}

func TestRecover_DryRunDelegatesToReadOnlyFixPlan(t *testing.T) {
	repo, stateDB, db := makeRegisteredGitRepoStateDB(t)
	stageRecoverableBarrierPair(t, context.Background(), repo, db, "refs/heads/main", 1)
	before, err := fileSHA256(stateDB)
	if err != nil {
		t.Fatalf("checksum before: %v", err)
	}

	var out bytes.Buffer
	if err := runRecover(context.Background(), &out, repo, true, true, false, true, false); err != nil {
		t.Fatalf("runRecover dry-run: %v\n%s", err, out.String())
	}
	var plan fixPlan
	if err := json.Unmarshal(out.Bytes(), &plan); err != nil {
		t.Fatalf("unmarshal delegated fix plan: %v\n%s", err, out.String())
	}
	if !plan.DryRun || findFixAction(plan, fixActionReconcileUnpublishedChain) == nil {
		t.Fatalf("recover did not delegate to safe fix plan: %+v", plan)
	}
	after, err := fileSHA256(stateDB)
	if err != nil {
		t.Fatalf("checksum after: %v", err)
	}
	if before != after {
		t.Fatalf("recover dry-run mutated state.db: before=%s after=%s", before, after)
	}
}

func TestRecover_ApplyPreservesStalePairInsteadOfRetargeting(t *testing.T) {
	repo, _, db := makeRegisteredGitRepoStateDB(t)
	ctx := context.Background()
	first, _ := stageRecoverableBarrierPair(t, ctx, repo, db, "refs/heads/stale", 9)

	var out bytes.Buffer
	if err := runRecover(ctx, &out, repo, true, false, true, true, false); err != nil {
		t.Fatalf("runRecover apply: %v\n%s", err, out.String())
	}
	var branchRef, eventState string
	var generation int64
	if err := db.SQL().QueryRowContext(ctx,
		`SELECT branch_ref, branch_generation, state FROM capture_events WHERE seq = ?`, first,
	).Scan(&branchRef, &generation, &eventState); err != nil {
		t.Fatalf("query event: %v", err)
	}
	if branchRef != "refs/heads/stale" || generation != 9 || eventState != state.EventStateRecovered {
		t.Fatalf("recover rewrote provenance: %s/g%d state=%s", branchRef, generation, eventState)
	}
}

func TestRecover_ClearPauseDelegatesExplicitRemoval(t *testing.T) {
	repo, _, _ := makeRegisteredGitRepoStateDB(t)
	markerPath := filepath.Join(repo, ".git", "acd", "paused")
	if _, err := pausepkg.Write(markerPath, pausepkg.Marker{
		Reason: "maintenance", SetAt: time.Now().UTC().Format(time.RFC3339), SetBy: "test",
	}, true); err != nil {
		t.Fatalf("write marker: %v", err)
	}

	var out bytes.Buffer
	if err := runRecover(context.Background(), &out, repo, true, false, true, true, true); err != nil {
		t.Fatalf("runRecover --clear-pause: %v\n%s", err, out.String())
	}
	if _, err := os.Stat(markerPath); !os.IsNotExist(err) {
		t.Fatalf("pause marker remains: %v", err)
	}
}

func TestRecover_RefusesWithDaemonAlive(t *testing.T) {
	repo, _, db := makeRegisteredGitRepoStateDB(t)
	if err := state.SaveDaemonState(context.Background(), db, state.DaemonState{
		PID: os.Getpid(), Mode: "running",
		BranchRef:        sql.NullString{String: "refs/heads/main", Valid: true},
		BranchGeneration: sql.NullInt64{Int64: 1, Valid: true},
	}); err != nil {
		t.Fatalf("SaveDaemonState: %v", err)
	}

	var out bytes.Buffer
	err := runRecover(context.Background(), &out, repo, true, false, true, true, false)
	if err == nil || !strings.Contains(err.Error(), "unsafe conditions") {
		t.Fatalf("runRecover err=%v want live-daemon refusal", err)
	}
}

func TestRecover_DryRunDoesNotBootstrapSchema(t *testing.T) {
	repo, stateDB, db := makeRegisteredGitRepoStateDB(t)
	if _, err := db.SQL().ExecContext(context.Background(), `DROP TABLE recovery_snapshots`); err != nil {
		t.Fatalf("drop recovery_snapshots: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}
	before, err := fileSHA256(stateDB)
	if err != nil {
		t.Fatalf("checksum before: %v", err)
	}

	var out bytes.Buffer
	if err := runRecover(context.Background(), &out, repo, true, true, false, true, false); err != nil {
		t.Fatalf("runRecover dry-run: %v", err)
	}
	after, err := fileSHA256(stateDB)
	if err != nil {
		t.Fatalf("checksum after: %v", err)
	}
	if before != after {
		t.Fatalf("dry-run bootstrapped schema: before=%s after=%s", before, after)
	}

	conn, err := openStateDBReadOnly(context.Background(), stateDB)
	if err != nil {
		t.Fatalf("open read-only: %v", err)
	}
	defer conn.Close()
	if exists, err := sqliteTableExists(context.Background(), conn, "recovery_snapshots"); err != nil || exists {
		t.Fatalf("recovery_snapshots exists=%v err=%v", exists, err)
	}
}

func TestRecover_DoesNotMutateHEADOrIndex(t *testing.T) {
	repo, _, db := makeRegisteredGitRepoStateDB(t)
	ctx := context.Background()
	stageRecoverableBarrierPair(t, ctx, repo, db, "refs/heads/main", 1)
	headBefore, err := git.RevParse(ctx, repo, "HEAD")
	if err != nil {
		t.Fatalf("HEAD before: %v", err)
	}
	indexBefore, err := git.Run(ctx, git.RunOpts{Dir: repo}, "diff", "--cached", "--name-status")
	if err != nil {
		t.Fatalf("index before: %v", err)
	}

	var out bytes.Buffer
	if err := runRecover(ctx, &out, repo, true, false, true, true, false); err != nil {
		t.Fatalf("runRecover: %v", err)
	}
	headAfter, _ := git.RevParse(ctx, repo, "HEAD")
	indexAfter, _ := git.Run(ctx, git.RunOpts{Dir: repo}, "diff", "--cached", "--name-status")
	if headAfter != headBefore || string(indexAfter) != string(indexBefore) {
		t.Fatalf("recover mutated Git state: HEAD %s->%s index %q->%q", headBefore, headAfter, indexBefore, indexAfter)
	}
}
