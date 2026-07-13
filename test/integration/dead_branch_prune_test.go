//go:build integration
// +build integration

// dead_branch_prune_test.go drives the dead-branch recovery surface
// end-to-end against the production `acd` binary:
//
//   - (a) Diverged transition with the prior branch deleted archives the exact
//     blocked_conflict / failed pair and retains its rows as recovered
//     provenance.
//   - (b) Diverged transition with the prior branch still alive preserves the
//     terminal rows.
//   - (c) Daemon startup sweep archives pre-seeded terminals for refs that
//     have since been deleted.
//   - (d) ACD_KEEP_DEAD_BRANCH_BARRIERS=1 disables the startup sweep. Runtime
//     branch transitions still reconcile the exact prior pair before accepting
//     a new token; this is transition safety, not the optional sweep.
//   - (e) RefExists transient-error fail-open path. Covered by the unit-level
//     TestDeadBranchSweep_RefExistsErrorPreservesRows in
//     internal/daemon/dead_branch_sweep_test.go (the integration-level
//     simulation requires corrupting a packed-refs blob in a way that
//     reliably makes `git show-ref` exit non-1 across hosts; that has proven
//     fragile so we lean on the unit test for this case and document it
//     here).
//   - Diagnose-meta surface assertion: `acd diagnose --json` includes all
//     three legacy dead_branch_prune_* fields on the recovery path; on the
//     no-recovery path the two int fields render as `0` (always-emit
//     contract — zero
//     is the documented "never ran" sentinel) and the refs slice is
//     omitted (omitempty + nil).
//
// Capture rows and their immutable operations are seeded through the sqlite3
// binary against the real state.db, exactly as the existing populated-state
// and explainable-UX
// integration tests do — the integration package cannot import the internal
// state package, and the daemon's own SQL drivers are ABI-compatible with
// raw inserts via `sqlite3` since the schema is materialized by the binary
// itself before we seed.
package integration_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// seedTerminalCaptureEvent inserts one capture event plus its immutable create
// operation in the requested terminal state for the given exact branch pair.
// The blob is written to the real object database so recovery has the same
// complete provenance it receives from production capture.
func seedTerminalCaptureEvent(t *testing.T, dbPath, branchRef string, generation int, baseHead, path, eventState string) {
	t.Helper()
	repo := filepath.Dir(filepath.Dir(filepath.Dir(dbPath)))
	afterOID := gitHashObjectStdin(t, repo, "dead branch recovery payload: "+path+"\n")
	now := nowFloatSeconds()
	stmt := fmt.Sprintf(
		`PRAGMA busy_timeout=5000;
BEGIN;
INSERT INTO capture_events(
    branch_ref, branch_generation, base_head, operation, path,
    fidelity, captured_ts, state
) VALUES (%s, %d, %s, 'create', %s, 'exact', %f, %s);
INSERT INTO capture_ops(
    event_seq, ord, op, path, after_oid, after_mode, fidelity
) VALUES (last_insert_rowid(), 0, 'create', %s, %s, '100644', 'exact');
COMMIT;`,
		sqliteLiteral(branchRef), generation, sqliteLiteral(baseHead),
		sqliteLiteral(path), now, sqliteLiteral(eventState),
		sqliteLiteral(path), sqliteLiteral(afterOID))
	if out, err := exec.Command("sqlite3", dbPath, stmt).CombinedOutput(); err != nil {
		t.Fatalf("seed capture_events ref=%s state=%s: %v\n%s", branchRef, eventState, err, out)
	}
}

// countTerminalsForRef returns the count of capture_events rows whose
// branch_ref matches the given ref and whose state is in {blocked_conflict,
// failed}. A recovered pair has zero terminals while retaining every original
// capture_events row.
func countTerminalsForRef(t *testing.T, dbPath, branchRef string) int {
	t.Helper()
	q := fmt.Sprintf(
		"SELECT COUNT(*) FROM capture_events WHERE branch_ref = %s AND state IN ('blocked_conflict','failed');",
		sqliteLiteral(branchRef))
	out := sqliteScalar(t, dbPath, q)
	n := 0
	fmt.Sscanf(out, "%d", &n)
	return n
}

// assertRecoveredPair verifies the no-loss dead-branch contract: all original
// rows retain their exact provenance, every row transitions to recovered, and
// one durable snapshot/ref protects the reconstructed tree.
func assertRecoveredPair(
	t *testing.T,
	dbPath, repo, branchRef string,
	generation int,
	baseHead string,
	wantEvents int,
) {
	t.Helper()
	waitFor(t, "dead branch pair recovered", 10*time.Second, func() bool {
		q := fmt.Sprintf(`
SELECT COUNT(*) FROM capture_events
WHERE branch_ref = %s AND branch_generation = %d AND base_head = %s
  AND state = 'recovered'`,
			sqliteLiteral(branchRef), generation, sqliteLiteral(baseHead))
		return sqliteScalar(t, dbPath, q) == fmt.Sprintf("%d", wantEvents)
	})

	if got := countTerminalsForRef(t, dbPath, branchRef); got != 0 {
		t.Fatalf("dead-ref %s terminal rows=%d want 0 after recovery", branchRef, got)
	}
	provenanceCount := sqliteScalar(t, dbPath, fmt.Sprintf(`
SELECT COUNT(*) FROM capture_events
WHERE branch_ref = %s AND branch_generation = %d AND base_head = %s`,
		sqliteLiteral(branchRef), generation, sqliteLiteral(baseHead)))
	if provenanceCount != fmt.Sprintf("%d", wantEvents) {
		t.Fatalf("dead-ref %s retained provenance=%s want %d", branchRef, provenanceCount, wantEvents)
	}

	snapshotID := sqliteScalar(t, dbPath, fmt.Sprintf(`
SELECT id FROM recovery_snapshots
WHERE branch_ref = %s AND branch_generation = %d
  AND outcome = 'recovered' AND event_count = %d`,
		sqliteLiteral(branchRef), generation, wantEvents))
	if snapshotID == "" {
		t.Fatalf("dead-ref %s recovery snapshot missing", branchRef)
	}
	if got := sqliteScalar(t, dbPath, fmt.Sprintf(`
SELECT COUNT(*) FROM recovery_snapshot_events
WHERE snapshot_id = %s`, snapshotID)); got != fmt.Sprintf("%d", wantEvents) {
		t.Fatalf("dead-ref %s recovery membership=%s want %d", branchRef, got, wantEvents)
	}
	recoveryRef := sqliteScalar(t, dbPath, fmt.Sprintf(
		"SELECT recovery_ref FROM recovery_snapshots WHERE id = %s", snapshotID))
	recoveryCommit := sqliteScalar(t, dbPath, fmt.Sprintf(
		"SELECT commit_oid FROM recovery_snapshots WHERE id = %s", snapshotID))
	if !strings.HasPrefix(recoveryRef, "refs/acd/recovery/") {
		t.Fatalf("dead-ref %s recovery_ref=%q", branchRef, recoveryRef)
	}
	if got := strings.TrimSpace(runGitOK(t, repo, "show-ref", "--hash", "--verify", recoveryRef)); got != recoveryCommit {
		t.Fatalf("dead-ref %s recovery ref resolves to %q want %q", branchRef, got, recoveryCommit)
	}
}

func currentBranchGeneration(t *testing.T, dbPath string) int {
	t.Helper()
	genRaw := sqliteScalar(t, dbPath, "SELECT value FROM daemon_meta WHERE key = 'branch.generation'")
	var gen int
	if _, err := fmt.Sscanf(genRaw, "%d", &gen); err != nil || gen <= 0 {
		t.Fatalf("branch.generation=%q is not a positive integer", genRaw)
	}
	return gen
}

// TestDeadBranchPrune_RuntimeDivergedDeletedBranchRecoversRows covers scenario
// (a): with the daemon still running, switching away from a branch and deleting
// that prior ref drives processBranchTokenChange through the runtime Diverged
// recovery path.
func TestDeadBranchPrune_RuntimeDivergedDeletedBranchRecoversRows(t *testing.T) {
	requireSQLite(t)

	repo := tempRepo(t)
	env := withIsolatedHome(t)
	t.Cleanup(func() { stopSessionForce(t, env, repo) })

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	const branchName = "runtime-dead"
	const deadRef = "refs/heads/" + branchName
	runGitOK(t, repo, "checkout", "-b", branchName)
	headOID := strings.TrimSpace(runGitOK(t, repo, "rev-parse", "HEAD"))

	startSession(t, ctx, env, repo, "dbp-runtime-prune", "shell")
	waitMode(t, repo, "running", 5*time.Second)

	dbPath := filepath.Join(repo, ".git", "acd", "state.db")
	wantToken := "rev:" + headOID + " " + deadRef
	waitFor(t, "daemon observed runtime-dead branch token", 5*time.Second, func() bool {
		return sqliteScalar(t, dbPath, "SELECT value FROM daemon_meta WHERE key = 'branch_token'") == wantToken
	})
	// Let the startup no-op sweep finish before we seed rows intended for the
	// runtime branch-transition path.
	time.Sleep(750 * time.Millisecond)
	gen := currentBranchGeneration(t, dbPath)

	seedTerminalCaptureEvent(t, dbPath, deadRef, gen, headOID, "runtime-blocked.txt", "blocked_conflict")
	seedTerminalCaptureEvent(t, dbPath, deadRef, gen, headOID, "runtime-failed.txt", "failed")
	if got := countTerminalsForRef(t, dbPath, deadRef); got != 2 {
		t.Fatalf("seeded runtime terminals for %s: got=%d want=2", deadRef, got)
	}

	runGitOK(t, repo, "checkout", "main")
	runGitOK(t, repo, "update-ref", "-d", deadRef)
	wakeSession(t, ctx, env, repo, "dbp-runtime-prune")

	assertRecoveredPair(t, dbPath, repo, deadRef, gen, headOID, 2)
	lastRefs := sqliteScalar(t, dbPath, "SELECT value FROM daemon_meta WHERE key = 'dead_branch_prune.last_refs'")
	if !strings.Contains(lastRefs, deadRef) {
		t.Fatalf("dead_branch_prune.last_refs=%q missing %q", lastRefs, deadRef)
	}
}

// TestDeadBranchPrune_DivergedDeletedBranchRecoversRows covers the restart shape:
// a branch exists when terminal rows land, is deleted while the daemon is
// stopped, then startup sweep recovers the rows on the next run.
//
// Strategy notes:
//
//   - A live `acd start` populates the state.db schema and stamps
//     branch.generation. We then stop it, seed a dead-ref row, delete the dead
//     ref, and restart the daemon so the *startup* sweep observes the dead ref.
//   - Scenario (c) — pure startup sweep with no prior daemon session — uses
//     a separate test below. This test is specifically the "ref was alive
//     when terminals landed, then operator merged + deleted" shape.
func TestDeadBranchPrune_DivergedDeletedBranchRecoversRows(t *testing.T) {
	requireSQLite(t)

	repo := tempRepo(t)
	env := withIsolatedHome(t)
	t.Cleanup(func() { stopSessionForce(t, env, repo) })

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Bring up the daemon once so .git/acd/state.db materializes with the
	// canonical schema. Then stop so we can seed deterministically.
	startSession(t, ctx, env, repo, "dbp-a-init", "shell")
	waitMode(t, repo, "running", 5*time.Second)
	stopSessionForce(t, env, repo)
	waitMode(t, repo, "stopped", 5*time.Second)

	dbPath := filepath.Join(repo, ".git", "acd", "state.db")

	// Create refs/heads/foo on top of HEAD so the seeded rows reference an
	// initially-real branch. The integration repo is on refs/heads/main.
	headOID := strings.TrimSpace(runGitOK(t, repo, "rev-parse", "HEAD"))
	runGitOK(t, repo, "update-ref", "refs/heads/foo", headOID)

	// Seed a blocked_conflict row tied to refs/heads/foo (generation 1).
	const deadRef = "refs/heads/foo"
	seedTerminalCaptureEvent(t, dbPath, deadRef, 1, headOID, "foo-blocked.txt", "blocked_conflict")
	seedTerminalCaptureEvent(t, dbPath, deadRef, 1, headOID, "foo-failed.txt", "failed")
	if got := countTerminalsForRef(t, dbPath, deadRef); got != 2 {
		t.Fatalf("seeded terminals for %s: got=%d want=2", deadRef, got)
	}

	// Delete refs/heads/foo to simulate "merged + branch deleted".
	runGitOK(t, repo, "update-ref", "-d", deadRef)

	// Start the daemon again — startup sweep + Diverged path will observe the
	// dead ref. The daemon's startup sweep archives the exact pair and retains
	// each capture_events row as recovered provenance.
	startSession(t, ctx, env, repo, "dbp-a-prune", "shell")
	waitMode(t, repo, "running", 10*time.Second)

	assertRecoveredPair(t, dbPath, repo, deadRef, 1, headOID, 2)
}

// TestDeadBranchPrune_LiveBranchPreservesRows covers scenario (b): when the
// prior branch ref is still alive in the repo, the sweep must leave its
// terminal rows alone.
func TestDeadBranchPrune_LiveBranchPreservesRows(t *testing.T) {
	requireSQLite(t)

	repo := tempRepo(t)
	env := withIsolatedHome(t)
	t.Cleanup(func() { stopSessionForce(t, env, repo) })

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	startSession(t, ctx, env, repo, "dbp-b-init", "shell")
	waitMode(t, repo, "running", 5*time.Second)
	stopSessionForce(t, env, repo)
	waitMode(t, repo, "stopped", 5*time.Second)

	dbPath := filepath.Join(repo, ".git", "acd", "state.db")

	headOID := strings.TrimSpace(runGitOK(t, repo, "rev-parse", "HEAD"))
	const liveRef = "refs/heads/keep"
	runGitOK(t, repo, "update-ref", liveRef, headOID)

	// Seed the same terminal rows but DO NOT delete the ref.
	seedTerminalCaptureEvent(t, dbPath, liveRef, 1, headOID, "keep-blocked.txt", "blocked_conflict")
	seedTerminalCaptureEvent(t, dbPath, liveRef, 1, headOID, "keep-failed.txt", "failed")
	if got := countTerminalsForRef(t, dbPath, liveRef); got != 2 {
		t.Fatalf("seed rows for %s: got=%d want 2", liveRef, got)
	}

	// Restart the daemon — sweep should NOT touch live-ref rows.
	startSession(t, ctx, env, repo, "dbp-b-keep", "shell")
	waitMode(t, repo, "running", 10*time.Second)

	// Allow time for any sweep activity to complete; assert preservation.
	time.Sleep(500 * time.Millisecond)
	if got := countTerminalsForRef(t, dbPath, liveRef); got != 2 {
		t.Fatalf("live-ref %s terminals=%d want 2 (sweep must preserve live refs)", liveRef, got)
	}
}

// TestDeadBranchPrune_StartupSweepRecoversPreSeededTerminals covers scenario
// (c): seed terminal rows for refs that never existed, then start the daemon.
// runStartupDeadBranchSweep observes the dead refs and archives both pairs.
func TestDeadBranchPrune_StartupSweepRecoversPreSeededTerminals(t *testing.T) {
	requireSQLite(t)

	repo := tempRepo(t)
	env := withIsolatedHome(t)
	t.Cleanup(func() { stopSessionForce(t, env, repo) })

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Materialize the schema, then stop and seed.
	startSession(t, ctx, env, repo, "dbp-c-init", "shell")
	waitMode(t, repo, "running", 5*time.Second)
	stopSessionForce(t, env, repo)
	waitMode(t, repo, "stopped", 5*time.Second)

	dbPath := filepath.Join(repo, ".git", "acd", "state.db")
	headOID := strings.TrimSpace(runGitOK(t, repo, "rev-parse", "HEAD"))

	const deadRef1 = "refs/heads/dead-1"
	const deadRef2 = "refs/heads/dead-2"
	// Neither ref ever exists in the repo. Sweep must observe RefExists=false
	// for both and recover their terminals without deleting provenance.
	seedTerminalCaptureEvent(t, dbPath, deadRef1, 1, headOID, "dead1-blocked.txt", "blocked_conflict")
	seedTerminalCaptureEvent(t, dbPath, deadRef1, 1, headOID, "dead1-failed.txt", "failed")
	seedTerminalCaptureEvent(t, dbPath, deadRef2, 1, headOID, "dead2-blocked.txt", "blocked_conflict")

	if got := countTerminalsForRef(t, dbPath, deadRef1); got != 2 {
		t.Fatalf("seed deadRef1: got=%d want 2", got)
	}
	if got := countTerminalsForRef(t, dbPath, deadRef2); got != 1 {
		t.Fatalf("seed deadRef2: got=%d want 1", got)
	}

	// Restart — startup sweep should archive both exact pairs.
	startSession(t, ctx, env, repo, "dbp-c-prune", "shell")
	waitMode(t, repo, "running", 10*time.Second)

	assertRecoveredPair(t, dbPath, repo, deadRef1, 1, headOID, 2)
	assertRecoveredPair(t, dbPath, repo, deadRef2, 1, headOID, 1)
}

// TestDeadBranchPrune_OptOutPreservesRows covers scenario (d) for startup
// sweep: with ACD_KEEP_DEAD_BRANCH_BARRIERS=1, the sweep preserves rows even
// when refs are dead.
func TestDeadBranchPrune_OptOutPreservesRows(t *testing.T) {
	requireSQLite(t)

	repo := tempRepo(t)
	env := withIsolatedHome(t)
	t.Cleanup(func() { stopSessionForce(t, env, repo) })

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Boot once to create state.db schema.
	startSession(t, ctx, env, repo, "dbp-d-init", "shell")
	waitMode(t, repo, "running", 5*time.Second)
	stopSessionForce(t, env, repo)
	waitMode(t, repo, "stopped", 5*time.Second)

	dbPath := filepath.Join(repo, ".git", "acd", "state.db")
	headOID := strings.TrimSpace(runGitOK(t, repo, "rev-parse", "HEAD"))

	const deadRef = "refs/heads/keep-dead-rows"
	seedTerminalCaptureEvent(t, dbPath, deadRef, 1, headOID, "keep-blocked.txt", "blocked_conflict")
	seedTerminalCaptureEvent(t, dbPath, deadRef, 1, headOID, "keep-failed.txt", "failed")
	if got := countTerminalsForRef(t, dbPath, deadRef); got != 2 {
		t.Fatalf("seed rows for %s: got=%d want 2", deadRef, got)
	}

	// Restart with the opt-out env. The daemon's startup sweep must skip
	// pruning entirely — leaving both terminals intact.
	startSession(t, ctx, env, repo, "dbp-d-keep", "shell",
		"ACD_KEEP_DEAD_BRANCH_BARRIERS=1")
	waitMode(t, repo, "running", 10*time.Second)

	// Settle window for any background work; rows must remain.
	time.Sleep(750 * time.Millisecond)
	if got := countTerminalsForRef(t, dbPath, deadRef); got != 2 {
		t.Fatalf("opt-out: dead-ref %s terminals=%d want 2 (sweep must skip)", deadRef, got)
	}
}

// TestDeadBranchPrune_RuntimeTransitionRecoversWithSweepOptOut proves the env
// knob disables the optional sweep, not mandatory transition reconciliation.
// Accepting a new token while leaving the old pair unpublished would allow the
// new worktree to be captured against stale shadow state.
func TestDeadBranchPrune_RuntimeTransitionRecoversWithSweepOptOut(t *testing.T) {
	requireSQLite(t)

	repo := tempRepo(t)
	env := withIsolatedHome(t)
	t.Cleanup(func() { stopSessionForce(t, env, repo) })

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	const branchName = "runtime-keep-dead"
	const deadRef = "refs/heads/" + branchName
	runGitOK(t, repo, "checkout", "-b", branchName)
	headOID := strings.TrimSpace(runGitOK(t, repo, "rev-parse", "HEAD"))

	startSession(t, ctx, env, repo, "dbp-runtime-keep", "shell",
		"ACD_KEEP_DEAD_BRANCH_BARRIERS=1")
	waitMode(t, repo, "running", 5*time.Second)

	dbPath := filepath.Join(repo, ".git", "acd", "state.db")
	wantToken := "rev:" + headOID + " " + deadRef
	waitFor(t, "daemon observed runtime opt-out branch token", 5*time.Second, func() bool {
		return sqliteScalar(t, dbPath, "SELECT value FROM daemon_meta WHERE key = 'branch_token'") == wantToken
	})
	time.Sleep(750 * time.Millisecond)
	gen := currentBranchGeneration(t, dbPath)

	seedTerminalCaptureEvent(t, dbPath, deadRef, gen, headOID, "runtime-keep-blocked.txt", "blocked_conflict")
	seedTerminalCaptureEvent(t, dbPath, deadRef, gen, headOID, "runtime-keep-failed.txt", "failed")
	if got := countTerminalsForRef(t, dbPath, deadRef); got != 2 {
		t.Fatalf("seeded runtime opt-out terminals for %s: got=%d want=2", deadRef, got)
	}

	runGitOK(t, repo, "checkout", "main")
	runGitOK(t, repo, "update-ref", "-d", deadRef)
	wakeSession(t, ctx, env, repo, "dbp-runtime-keep")

	assertRecoveredPair(t, dbPath, repo, deadRef, gen, headOID, 2)
	lastRefs := sqliteScalar(t, dbPath, "SELECT value FROM daemon_meta WHERE key = 'dead_branch_prune.last_refs'")
	if !strings.Contains(lastRefs, deadRef) {
		t.Fatalf("runtime transition recovery refs=%q missing %q", lastRefs, deadRef)
	}
}

// TestDeadBranchPrune_DiagnoseMetaSurfacesAfterRecovery asserts the three
// dead_branch_prune_* JSON fields surface from `acd diagnose --json` after a
// successful sweep. Counterpart to the unit-level meta-write tests in
// internal/daemon/dead_branch_sweep_test.go: this proves the full pipeline
// (daemon writes meta -> CLI diagnose reads meta -> JSON fields populate).
func TestDeadBranchPrune_DiagnoseMetaSurfacesAfterRecovery(t *testing.T) {
	requireSQLite(t)

	repo := tempRepo(t)
	env := withIsolatedHome(t)
	t.Cleanup(func() { stopSessionForce(t, env, repo) })

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	// Boot to materialize schema, then stop and seed.
	startSession(t, ctx, env, repo, "dbp-meta-init", "shell")
	waitMode(t, repo, "running", 5*time.Second)
	stopSessionForce(t, env, repo)
	waitMode(t, repo, "stopped", 5*time.Second)

	dbPath := filepath.Join(repo, ".git", "acd", "state.db")
	headOID := strings.TrimSpace(runGitOK(t, repo, "rev-parse", "HEAD"))

	const deadRef = "refs/heads/diagnose-dead"
	seedTerminalCaptureEvent(t, dbPath, deadRef, 1, headOID, "diag-blocked.txt", "blocked_conflict")

	beforeTS := time.Now().Unix() - 1 // tolerate clock skew

	// Restart — the startup sweep recovers the dead-ref terminal and stamps
	// the three legacy dead_branch_prune.* meta keys.
	startSession(t, ctx, env, repo, "dbp-meta-prune", "shell")
	waitMode(t, repo, "running", 10*time.Second)

	assertRecoveredPair(t, dbPath, repo, deadRef, 1, headOID, 1)
	// Wait until daemon has stamped the meta keys (the best-effort sweep runs
	// slightly after the rows transition to recovered).
	waitFor(t, "dead_branch_prune.last_run_ts present", 5*time.Second, func() bool {
		return sqliteScalar(t, dbPath, "SELECT value FROM daemon_meta WHERE key = 'dead_branch_prune.last_run_ts'") != ""
	})

	res := runAcd(t, ctx, env, "diagnose", "--repo", repo, "--json")
	if res.ExitCode != 0 {
		t.Fatalf("acd diagnose exit=%d\nstdout=%s\nstderr=%s", res.ExitCode, res.Stdout, res.Stderr)
	}
	var report struct {
		DeadBranchPruneLastRunTS int64    `json:"dead_branch_prune_last_run_ts"`
		DeadBranchPruneLastCount int      `json:"dead_branch_prune_last_count"`
		DeadBranchPruneLastRefs  []string `json:"dead_branch_prune_last_refs"`
	}
	if err := json.Unmarshal([]byte(res.Stdout), &report); err != nil {
		t.Fatalf("decode diagnose json: %v\nstdout=%s", err, res.Stdout)
	}

	afterTS := time.Now().Unix() + 1
	if report.DeadBranchPruneLastRunTS < beforeTS || report.DeadBranchPruneLastRunTS > afterTS {
		t.Fatalf("dead_branch_prune_last_run_ts=%d outside [%d, %d]\njson=%s",
			report.DeadBranchPruneLastRunTS, beforeTS, afterTS, res.Stdout)
	}
	if report.DeadBranchPruneLastCount < 1 {
		t.Fatalf("dead_branch_prune_last_count=%d want >= 1\njson=%s",
			report.DeadBranchPruneLastCount, res.Stdout)
	}
	foundDeadRef := false
	for _, r := range report.DeadBranchPruneLastRefs {
		if r == deadRef {
			foundDeadRef = true
			break
		}
	}
	if !foundDeadRef {
		t.Fatalf("dead_branch_prune_last_refs=%v missing %q\njson=%s",
			report.DeadBranchPruneLastRefs, deadRef, res.Stdout)
	}
}

// TestDeadBranchPrune_DiagnoseMetaAbsentBeforeAnyPrune asserts the
// dead-branch prune JSON shape on a fresh repo with no recorded prune. The
// two int fields (`dead_branch_prune_last_run_ts`, `_last_count`) are
// always present in the JSON — `0` is the documented "never ran" sentinel
// (omitempty was dropped on those fields deliberately so dashboards can
// distinguish "field missing" from "value present and zero"). The slice
// (`_last_refs`) keeps `omitempty` and is therefore absent when nil. This
// is the sister test to the "after prune" assertion above; together they
// pin the contract.
func TestDeadBranchPrune_DiagnoseMetaAbsentBeforeAnyPrune(t *testing.T) {
	requireSQLite(t)

	repo := tempRepo(t)
	env := withIsolatedHome(t)
	t.Cleanup(func() { stopSessionForce(t, env, repo) })

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Bring the daemon up but do NOT seed any dead-ref terminals — the sweep
	// is a no-op and the meta keys must remain absent.
	startSession(t, ctx, env, repo, "dbp-noprune", "shell")
	waitMode(t, repo, "running", 5*time.Second)

	res := runAcd(t, ctx, env, "diagnose", "--repo", repo, "--json")
	if res.ExitCode != 0 {
		t.Fatalf("acd diagnose exit=%d\nstdout=%s\nstderr=%s", res.ExitCode, res.Stdout, res.Stderr)
	}
	// Decode into a map so we can inspect the JSON shape: the two int
	// fields must be present with value 0 (always-emit contract — zero is
	// the "never ran" sentinel); the refs slice must be absent (omitempty
	// + nil).
	var raw map[string]any
	if err := json.Unmarshal([]byte(res.Stdout), &raw); err != nil {
		t.Fatalf("decode diagnose json: %v\nstdout=%s", err, res.Stdout)
	}
	for _, key := range []string{
		"dead_branch_prune_last_run_ts",
		"dead_branch_prune_last_count",
	} {
		v, ok := raw[key]
		if !ok {
			t.Fatalf("expected JSON field %q present (always-emit contract) on no-prune boot\nstdout=%s",
				key, res.Stdout)
		}
		// JSON numbers decode as float64 in any-typed maps.
		if num, isNum := v.(float64); !isNum || num != 0 {
			t.Fatalf("expected JSON field %q == 0 on no-prune boot; got %v (%T)\nstdout=%s",
				key, v, v, res.Stdout)
		}
	}
	if _, ok := raw["dead_branch_prune_last_refs"]; ok {
		t.Fatalf("expected JSON field 'dead_branch_prune_last_refs' absent (omitempty + nil) on no-prune boot; got value=%v\nstdout=%s",
			raw["dead_branch_prune_last_refs"], res.Stdout)
	}

	// Belt-and-suspenders: also verify the meta keys are unset in state.db.
	dbPath := filepath.Join(repo, ".git", "acd", "state.db")
	for _, key := range []string{
		"dead_branch_prune.last_run_ts",
		"dead_branch_prune.last_count",
		"dead_branch_prune.last_refs",
	} {
		v := sqliteScalar(t, dbPath, fmt.Sprintf("SELECT value FROM daemon_meta WHERE key = %s", sqliteLiteral(key)))
		if v != "" {
			t.Fatalf("meta %q=%q expected unset on no-prune boot", key, v)
		}
	}
}
