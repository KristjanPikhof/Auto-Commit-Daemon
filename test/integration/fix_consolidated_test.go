//go:build integration
// +build integration

package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/paths"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/supervisor"
)

func TestControlOnRecoversDuplicateCaptureWithLinkedWorktreeWorkerLive(t *testing.T) {
	requireSQLite(t)
	repo := tempRepo(t)
	linked := filepath.Join(t.TempDir(), "linked")
	runGitOK(t, repo, "worktree", "add", "-q", "-b", "linked-recovery", linked)
	t.Cleanup(func() { _, _ = runGit(repo, "worktree", "remove", "--force", linked) })
	env := withIsolatedHome(t)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	mainDB := initStateDBSchema(t, ctx, env, repo, "linked-recovery-main")
	roots, repositoryID := prepareCheckpointRegistration(t, env, linked)
	linkedRepositoryID := repositoryID
	_, mainRepositoryID := prepareCheckpointRegistration(t, env, repo)
	if linkedRepositoryID != mainRepositoryID {
		t.Fatalf("linked worktrees have distinct repository ids: main=%s linked=%s", mainRepositoryID, linkedRepositoryID)
	}

	head := strings.TrimSpace(runGitOK(t, repo, "rev-parse", "HEAD"))
	beforeOID := strings.TrimSpace(runGitOK(t, repo, "rev-parse", "HEAD:.gitignore"))
	afterOID := gitHashObjectStdin(t, repo, "duplicate captured state\n")
	now := nowFloatSeconds()
	seed := fmt.Sprintf(`
INSERT INTO capture_events(branch_ref,branch_generation,base_head,operation,path,fidelity,captured_ts,state,error)
VALUES ('refs/heads/main',1,'%s','modify','.gitignore','rescan',%f,'blocked_conflict','history moved');
INSERT INTO capture_ops(event_seq,ord,op,path,before_oid,before_mode,after_oid,after_mode,fidelity)
VALUES (last_insert_rowid(),0,'modify','.gitignore','%s','100644','%s','100644','rescan');
INSERT INTO capture_events(branch_ref,branch_generation,base_head,operation,path,fidelity,captured_ts,state)
VALUES ('refs/heads/main',1,'%s','modify','.gitignore','rescan',%f,'pending');
INSERT INTO capture_ops(event_seq,ord,op,path,before_oid,before_mode,after_oid,after_mode,fidelity)
VALUES (last_insert_rowid(),0,'modify','.gitignore','%s','100644','%s','100644','rescan');
`, head, now, beforeOID, afterOID, head, now+0.001, beforeOID, afterOID)
	ensureCheckpointRuntime(t, env, repo, buildAcdBinary(t))
	before := mustIntegrationWorkerStatus(t, ctx, roots, repositoryID)
	off := runAcd(t, ctx, env, "off", "--repo", repo, "--json")
	if off.ExitCode != 0 {
		t.Fatalf("off exit=%d\nstdout=%s\nstderr=%s", off.ExitCode, off.Stdout, off.Stderr)
	}
	sqliteExec(t, mainDB, seed)

	on := runAcd(t, ctx, env, "on", "--repo", repo, "--json")
	if on.ExitCode != 0 || strings.Contains(on.Stdout, `"action_required": true`) {
		t.Fatalf("on exit=%d\nstdout=%s\nstderr=%s", on.ExitCode, on.Stdout, on.Stderr)
	}
	if got := sqliteScalar(t, mainDB, "SELECT group_concat(state, ',') FROM (SELECT state FROM capture_events ORDER BY seq)"); got != "recovered,recovered" {
		t.Fatalf("duplicate capture states=%q", got)
	}
	after := mustIntegrationWorkerStatus(t, ctx, roots, repositoryID)
	if before.PID <= 0 || after.PID <= 0 {
		t.Fatalf("shared worker missing: before=%+v after=%+v", before, after)
	}
	if before.PID != after.PID {
		if err := syscall.Kill(before.PID, 0); !errors.Is(err, syscall.ESRCH) {
			t.Fatalf("old shared worker pid %d survived recovery: %v", before.PID, err)
		}
	}
	status := runAcd(t, ctx, env, "status", "--repo", repo, "--json")
	if status.ExitCode != 0 || !strings.Contains(status.Stdout, `"pending_events": 0`) ||
		!strings.Contains(status.Stdout, `"blocked_events": 0`) || strings.Contains(status.Stdout, `"action_required": true`) {
		t.Fatalf("post-recovery status exit=%d\nstdout=%s\nstderr=%s", status.ExitCode, status.Stdout, status.Stderr)
	}
}

func mustIntegrationWorkerStatus(t *testing.T, ctx context.Context, roots paths.Roots, repositoryID string) supervisor.WorkerStatus {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		status, err := checkpointRuntimeStatus(ctx, roots)
		if err == nil {
			for _, worker := range status.Workers {
				if worker.RepositoryID == repositoryID && worker.PID > 0 && worker.State == "running" {
					return worker
				}
			}
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("worker %s did not become ready", repositoryID)
	return supervisor.WorkerStatus{}
}

// TestFix_ReconcilesWholeExactPairs pins the immutable recovery contract:
// exact HEAD matches publish the full pair, while explicit --force archives a
// non-matching pair without deleting or retargeting its captured rows.
func TestFix_ReconcilesWholeExactPairs(t *testing.T) {
	t.Parallel()
	requireSQLite(t)

	repo := tempRepo(t)
	env := withIsolatedHome(t)
	t.Cleanup(func() { stopSessionForce(t, env, repo) })

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	baseHead := strings.TrimSpace(runGitOK(t, repo, "rev-parse", "HEAD"))
	writeFile(t, filepath.Join(repo, "published-a.txt"), "published a\n")
	writeFile(t, filepath.Join(repo, "published-b.txt"), "published b\n")
	runGitOK(t, repo, "add", "published-a.txt", "published-b.txt")
	runGitOK(t, repo, "commit", "-q", "-m", "publish captured pair externally")
	head := strings.TrimSpace(runGitOK(t, repo, "rev-parse", "HEAD"))
	publishedAOID := strings.TrimSpace(runGitOK(t, repo, "rev-parse", "HEAD:published-a.txt"))
	publishedBOID := strings.TrimSpace(runGitOK(t, repo, "rev-parse", "HEAD:published-b.txt"))

	dbPath := initStateDBSchema(t, ctx, env, repo, "fix-barrier-init")
	gen := sqliteScalar(t, dbPath, "SELECT value FROM daemon_meta WHERE key = 'branch.generation'")
	if gen == "" {
		gen = "1"
	}

	publishedPair := seedCreateRecoveryPair(t, dbPath, "refs/heads/main", gen, baseHead,
		"published-a.txt", publishedAOID, "published-b.txt", publishedBOID)
	apply := runAcd(t, ctx, env, "fix", "--repo", repo, "--yes", "--json")
	if apply.ExitCode != 0 {
		t.Fatalf("fix --yes exit=%d\nstdout=%s\nstderr=%s", apply.ExitCode, apply.Stdout, apply.Stderr)
	}
	publishedPlan := decodeFixPlan(t, apply.Stdout)
	publishedAction := findFixActionForSeq(publishedPlan.Actions, "reconcile_unpublished_chain", publishedPair.FirstSeq)
	if publishedAction == nil || !publishedAction.Applied || publishedAction.State != "published" || publishedAction.RecoveryRef == "" {
		t.Fatalf("normal fix did not publish exact HEAD pair: action=%+v\n%s", publishedAction, apply.Stdout)
	}
	if got := sqliteScalar(t, dbPath, fmt.Sprintf(
		"SELECT group_concat(value, ',') FROM (SELECT state || '|' || commit_oid || '|' || branch_ref || '|' || branch_generation AS value FROM capture_events WHERE seq IN (%s,%s) ORDER BY seq)",
		publishedPair.FirstSeq, publishedPair.SecondSeq)); got != fmt.Sprintf("published|%s|refs/heads/main|%s,published|%s|refs/heads/main|%s", head, gen, head, gen) {
		t.Fatalf("published pair provenance/state=%q", got)
	}
	if rows := sqliteScalar(t, dbPath, fmt.Sprintf(
		"SELECT COUNT(*) FROM capture_events WHERE seq IN (%s,%s)", publishedPair.FirstSeq, publishedPair.SecondSeq)); rows != "2" {
		t.Fatalf("normal fix deleted captured rows: %s", rows)
	}
	runGitOK(t, repo, "show-ref", "--verify", publishedAction.RecoveryRef)

	archiveAOID := gitHashObjectStdin(t, repo, "archive a\n")
	archiveBOID := gitHashObjectStdin(t, repo, "archive b\n")
	archivePair := seedCreateRecoveryPair(t, dbPath, "refs/heads/main", gen, head,
		"archive-a.txt", archiveAOID, "archive-b.txt", archiveBOID)
	refsBefore := recoveryRefList(t, repo)
	forceDry := runAcd(t, ctx, env, "fix", "--repo", repo, "--force", "--dry-run", "--json")
	if forceDry.ExitCode != 0 {
		t.Fatalf("fix --force --dry-run exit=%d\nstdout=%s\nstderr=%s", forceDry.ExitCode, forceDry.Stdout, forceDry.Stderr)
	}
	forcePlan := decodeFixPlan(t, forceDry.Stdout)
	if !forcePlan.DryRun {
		t.Fatalf("--force --dry-run did not flag dry_run=true\n%s", forceDry.Stdout)
	}
	forceAction := findFixActionForSeq(forcePlan.Actions, "reconcile_unpublished_chain", archivePair.FirstSeq)
	if forceAction == nil || !forceAction.ArchiveOnly || !forceAction.RequiresForce || forceAction.Applied {
		t.Fatalf("--force --dry-run action=%+v\n%s", forceAction, forceDry.Stdout)
	}
	if got := exactPairStates(t, dbPath, archivePair); got != "blocked_conflict,pending" {
		t.Fatalf("--force --dry-run mutated pair states to %q", got)
	}
	if refsAfter := recoveryRefList(t, repo); refsAfter != refsBefore {
		t.Fatalf("--force --dry-run mutated recovery refs:\nbefore=%s\nafter=%s", refsBefore, refsAfter)
	}

	forceApply := runAcd(t, ctx, env, "fix", "--repo", repo, "--force", "--yes", "--json")
	if forceApply.ExitCode != 0 {
		t.Fatalf("fix --force --yes exit=%d\nstdout=%s\nstderr=%s",
			forceApply.ExitCode, forceApply.Stdout, forceApply.Stderr)
	}
	appliedPlan := decodeFixPlan(t, forceApply.Stdout)
	appliedAction := findFixActionForSeq(appliedPlan.Actions, "reconcile_unpublished_chain", archivePair.FirstSeq)
	if appliedAction == nil || !appliedAction.Applied || appliedAction.State != "recovered" ||
		appliedAction.RowsChanged != 2 || !strings.HasPrefix(appliedAction.RecoveryRef, "refs/acd/recovery/") {
		t.Fatalf("--force --yes archive action=%+v\n%s", appliedAction, forceApply.Stdout)
	}
	if got := exactPairStates(t, dbPath, archivePair); got != "recovered,recovered" {
		t.Fatalf("--force --yes pair states=%q", got)
	}
	if rows := sqliteScalar(t, dbPath, fmt.Sprintf(
		"SELECT COUNT(*) FROM capture_events WHERE seq IN (%s,%s)", archivePair.FirstSeq, archivePair.SecondSeq)); rows != "2" {
		t.Fatalf("--force --yes deleted captured rows: %s", rows)
	}
	if ops := sqliteScalar(t, dbPath, fmt.Sprintf(
		"SELECT COUNT(*) FROM capture_ops WHERE event_seq IN (%s,%s)", archivePair.FirstSeq, archivePair.SecondSeq)); ops != "2" {
		t.Fatalf("--force --yes deleted captured ops: %s", ops)
	}
	runGitOK(t, repo, "show-ref", "--verify", appliedAction.RecoveryRef)
}

// TestRecoverAndPurgeDeprecationWarnings asserts the legacy `acd recover`
// and `acd purge-events` entrypoints retain their safe compatibility contract
// while emitting the documented deprecation stderr line. Recover delegates to
// immutable whole-pair recovery; purge refuses ambiguous selectors and requires
// explicit --all before preserving every planned pair.
func TestRecoverAndPurgeDeprecationWarnings(t *testing.T) {
	t.Parallel()
	requireSQLite(t)

	repo := tempRepo(t)
	env := withIsolatedHome(t)
	t.Cleanup(func() { stopSessionForce(t, env, repo) })

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Schema bootstrap + central registry entry.
	dbPath := initStateDBSchema(t, ctx, env, repo, "deprec-init")

	head := strings.TrimSpace(runGitOK(t, repo, "rev-parse", "HEAD"))
	gen := sqliteScalar(t, dbPath, "SELECT value FROM daemon_meta WHERE key = 'branch.generation'")
	if gen == "" {
		gen = "1"
	}

	staleAOID := gitHashObjectStdin(t, repo, "stale a\n")
	staleBOID := gitHashObjectStdin(t, repo, "stale b\n")
	stalePair := seedCreateRecoveryPair(t, dbPath, "refs/heads/stale", "99", head,
		"stale-a.txt", staleAOID, "stale-b.txt", staleBOID)

	// Deprecated path: acd recover --auto --yes.
	recover := runAcd(t, ctx, env, "recover", "--repo", repo, "--auto", "--yes", "--json")
	if recover.ExitCode != 0 {
		t.Fatalf("acd recover --auto --yes exit=%d\nstdout=%s\nstderr=%s",
			recover.ExitCode, recover.Stdout, recover.Stderr)
	}
	const wantRecoverDeprec = "warning: acd recover is a compatibility alias; use acd support recover"
	if !strings.Contains(recover.Stderr, wantRecoverDeprec) {
		t.Fatalf("recover stderr missing deprecation banner\nwant: %q\nstderr: %q",
			wantRecoverDeprec, recover.Stderr)
	}
	recoverPlan := decodeFixPlan(t, recover.Stdout)
	recoverAction := findFixActionForSeq(recoverPlan.Actions, "reconcile_unpublished_chain", stalePair.FirstSeq)
	if recoverAction == nil || !recoverAction.Applied || recoverAction.State != "recovered" || recoverAction.RecoveryRef == "" {
		t.Fatalf("recover alias action=%+v\n%s", recoverAction, recover.Stdout)
	}
	if got := sqliteScalar(t, dbPath, fmt.Sprintf(
		"SELECT group_concat(value, ',') FROM (SELECT branch_ref || '|' || branch_generation || '|' || state AS value FROM capture_events WHERE seq IN (%s,%s) ORDER BY seq)",
		stalePair.FirstSeq, stalePair.SecondSeq)); got != "refs/heads/stale|99|recovered,refs/heads/stale|99|recovered" {
		t.Fatalf("recover alias retargeted or split stale pair: %q", got)
	}
	if rows := sqliteScalar(t, dbPath, fmt.Sprintf(
		"SELECT COUNT(*) FROM capture_events WHERE seq IN (%s,%s)", stalePair.FirstSeq, stalePair.SecondSeq)); rows != "2" {
		t.Fatalf("recover alias deleted captured rows: %s", rows)
	}
	runGitOK(t, repo, "show-ref", "--verify", recoverAction.RecoveryRef)

	purgeAOID := gitHashObjectStdin(t, repo, "purge alias a\n")
	purgeBOID := gitHashObjectStdin(t, repo, "purge alias b\n")
	purgePair := seedCreateRecoveryPair(t, dbPath, "refs/heads/main", gen, head,
		"purge-a.txt", purgeAOID, "purge-b.txt", purgeBOID)

	// The old selective spelling is now fail-closed because delegating it to a
	// whole-repository fix could preserve unrelated failed or stale pairs.
	snapshotsBefore := sqliteScalar(t, dbPath, "SELECT COUNT(*) FROM recovery_snapshots")
	purge := runAcd(t, ctx, env, "purge-events", "--repo", repo, "--blocked", "--yes", "--json")
	if purge.ExitCode == 0 {
		t.Fatalf("acd purge-events --blocked --yes unexpectedly succeeded\nstdout=%s\nstderr=%s",
			purge.Stdout, purge.Stderr)
	}
	if !strings.Contains(purge.Stdout+purge.Stderr, "selective --blocked/--pending/--failed recovery is no longer supported") {
		t.Fatalf("purge-events selective refusal missing\nstdout=%s\nstderr=%s",
			purge.Stdout, purge.Stderr)
	}
	if got := exactPairStates(t, dbPath, purgePair); got != "blocked_conflict,pending" {
		t.Fatalf("refused purge alias changed pair states=%q", got)
	}
	if got := sqliteScalar(t, dbPath, "SELECT COUNT(*) FROM recovery_snapshots"); got != snapshotsBefore {
		t.Fatalf("refused purge alias snapshots=%s want unchanged %s", got, snapshotsBefore)
	}

	// Explicit --all retains the deprecated safe alias for one release.
	purge = runAcd(t, ctx, env, "purge-events", "--repo", repo, "--all", "--yes", "--json")
	if purge.ExitCode != 0 {
		t.Fatalf("acd purge-events --all --yes exit=%d\nstdout=%s\nstderr=%s",
			purge.ExitCode, purge.Stdout, purge.Stderr)
	}
	const wantPurgeDeprec = "acd purge-events is deprecated; use acd fix --force [--yes]. See acd fix --help."
	if !strings.Contains(purge.Stderr, wantPurgeDeprec) {
		t.Fatalf("purge-events stderr missing deprecation banner\nwant: %q\nstderr: %q",
			wantPurgeDeprec, purge.Stderr)
	}
	purgePlan := decodeFixPlan(t, purge.Stdout)
	purgeAction := findFixActionForSeq(purgePlan.Actions, "reconcile_unpublished_chain", purgePair.FirstSeq)
	if purgeAction == nil || !purgeAction.Applied || purgeAction.State != "recovered" ||
		!purgeAction.ArchiveOnly || purgeAction.RecoveryRef == "" {
		t.Fatalf("purge alias action=%+v\n%s", purgeAction, purge.Stdout)
	}
	if got := exactPairStates(t, dbPath, purgePair); got != "recovered,recovered" {
		t.Fatalf("purge alias pair states=%q", got)
	}
	if rows := sqliteScalar(t, dbPath, fmt.Sprintf(
		"SELECT COUNT(*) FROM capture_events WHERE seq IN (%s,%s)", purgePair.FirstSeq, purgePair.SecondSeq)); rows != "2" {
		t.Fatalf("purge alias deleted captured rows: %s", rows)
	}
	runGitOK(t, repo, "show-ref", "--verify", purgeAction.RecoveryRef)
}

func TestFix_GeneratedPendingCleanupKeepsGitManual(t *testing.T) {
	t.Parallel()
	requireSQLite(t)

	repo := tempRepo(t)
	env := withIsolatedHome(t)
	t.Cleanup(func() { stopSessionForce(t, env, repo) })

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	for _, rel := range []string{
		".derivedData-provider-core/Index.noindex/a.db",
		".derivedData-provider-core/Index.noindex/b.db",
	} {
		writeFile(t, filepath.Join(repo, filepath.FromSlash(rel)), "generated\n")
	}
	runGitOK(t, repo, "add", "-f",
		".derivedData-provider-core/Index.noindex/a.db",
		".derivedData-provider-core/Index.noindex/b.db")
	runGitOK(t, repo, "commit", "-q", "-m", "track generated cache files")
	aOID := strings.TrimSpace(runGitOK(t, repo, "rev-parse", "HEAD:.derivedData-provider-core/Index.noindex/a.db"))
	bOID := strings.TrimSpace(runGitOK(t, repo, "rev-parse", "HEAD:.derivedData-provider-core/Index.noindex/b.db"))
	if err := os.RemoveAll(filepath.Join(repo, ".derivedData-provider-core")); err != nil {
		t.Fatalf("remove generated root: %v", err)
	}
	beforeStatus := runGitOK(t, repo, "status", "--short", "--", ".derivedData-provider-core")

	dbPath := initStateDBSchema(t, ctx, env, repo, "fix-generated-init")
	head := strings.TrimSpace(runGitOK(t, repo, "rev-parse", "HEAD"))
	gen := sqliteScalar(t, dbPath, "SELECT value FROM daemon_meta WHERE key = 'branch.generation'")
	if gen == "" {
		gen = "1"
	}
	now := nowFloatSeconds()
	seedSQL := fmt.Sprintf(`
INSERT INTO capture_events(branch_ref, branch_generation, base_head, operation, path, fidelity, captured_ts, state)
VALUES ('refs/heads/main', %s, '%s', 'delete', '.derivedData-provider-core/Index.noindex/a.db', 'rescan', %f, 'pending');
INSERT INTO capture_ops(event_seq, ord, op, path, before_oid, before_mode, fidelity)
VALUES (last_insert_rowid(), 0, 'delete', '.derivedData-provider-core/Index.noindex/a.db', '%s', '100644', 'rescan');
INSERT INTO planner_state(event_seq, defer_count, last_planned_ts)
VALUES ((SELECT seq FROM capture_events WHERE path = '.derivedData-provider-core/Index.noindex/a.db' ORDER BY seq DESC LIMIT 1), 0, %f);
INSERT INTO capture_events(branch_ref, branch_generation, base_head, operation, path, fidelity, captured_ts, state)
VALUES ('refs/heads/main', %s, '%s', 'delete', '.derivedData-provider-core/Index.noindex/b.db', 'rescan', %f, 'pending');
INSERT INTO capture_ops(event_seq, ord, op, path, before_oid, before_mode, fidelity)
VALUES (last_insert_rowid(), 0, 'delete', '.derivedData-provider-core/Index.noindex/b.db', '%s', '100644', 'rescan');
INSERT INTO planner_state(event_seq, defer_count, last_planned_ts)
VALUES ((SELECT seq FROM capture_events WHERE path = '.derivedData-provider-core/Index.noindex/b.db' ORDER BY seq DESC LIMIT 1), 0, %f);
INSERT INTO capture_events(branch_ref, branch_generation, base_head, operation, path, fidelity, captured_ts, state)
VALUES ('refs/heads/main', %s, '%s', 'delete', 'build/output.js', 'rescan', %f, 'pending');
`, gen, head, now, aOID, now,
		gen, head, now+0.001, bOID, now+0.001,
		gen, head, now+0.002)
	if out, err := exec.Command("sqlite3", dbPath, seedSQL).CombinedOutput(); err != nil {
		t.Fatalf("seed generated pending rows: %v\n%s", err, out)
	}
	prepareCheckpointRegistration(t, env, repo)

	diagnose := runAcd(t, ctx, env, "diagnose", "--repo", repo, "--json")
	if diagnose.ExitCode != 0 {
		t.Fatalf("diagnose exit=%d\nstdout=%s\nstderr=%s", diagnose.ExitCode, diagnose.Stdout, diagnose.Stderr)
	}
	if !strings.Contains(diagnose.Stdout, `"generated_pending"`) ||
		!strings.Contains(diagnose.Stdout, `"tracked_count": 2`) {
		t.Fatalf("diagnose did not surface generated pending tracked count:\n%s", diagnose.Stdout)
	}

	dryRun := runAcd(t, ctx, env, "fix", "--repo", repo, "--dry-run", "--json")
	if dryRun.ExitCode != 0 {
		t.Fatalf("fix dry-run exit=%d\nstdout=%s\nstderr=%s", dryRun.ExitCode, dryRun.Stdout, dryRun.Stderr)
	}
	plan := decodeFixPlan(t, dryRun.Stdout)
	if !hasFixActionKind(plan.Actions, "drop_generated_pending") {
		t.Fatalf("fix dry-run missing drop_generated_pending:\n%s", dryRun.Stdout)
	}

	apply := runAcd(t, ctx, env, "fix", "--repo", repo, "--yes", "--json")
	if apply.ExitCode != 0 {
		t.Fatalf("fix --yes exit=%d\nstdout=%s\nstderr=%s", apply.ExitCode, apply.Stdout, apply.Stderr)
	}
	if got := sqliteScalar(t, dbPath,
		"SELECT COUNT(*) FROM capture_events WHERE path LIKE '.derivedData-provider-core/%'"); got != "0" {
		t.Fatalf("generated pending rows remain after fix --yes: %s\n%s", got, apply.Stdout)
	}
	if got := sqliteScalar(t, dbPath,
		"SELECT COUNT(*) FROM capture_ops WHERE path LIKE '.derivedData-provider-core/%'"); got != "0" {
		t.Fatalf("generated capture_ops remain after fix --yes: %s", got)
	}
	if got := sqliteScalar(t, dbPath,
		"SELECT COUNT(*) FROM planner_state"); got != "0" {
		t.Fatalf("generated planner_state rows remain after fix --yes: %s", got)
	}
	if got := sqliteScalar(t, dbPath,
		"SELECT COUNT(*) FROM capture_events WHERE path = 'build/output.js' AND state = 'pending'"); got != "1" {
		t.Fatalf("unrelated pending row count=%s want 1", got)
	}
	afterStatus := runGitOK(t, repo, "status", "--short", "--", ".derivedData-provider-core")
	if afterStatus != beforeStatus {
		t.Fatalf("fix mutated Git status:\nbefore:\n%s\nafter:\n%s", beforeStatus, afterStatus)
	}
}

// fixPlanProbe is the JSON subset this file inspects. We re-declare it (rather
// than importing internal/cli) because integration tests live in a separate
// package; the keys are stable per the SPEC LOCK.
type fixPlanProbe struct {
	DryRun      bool             `json:"dry_run"`
	Force       bool             `json:"force"`
	Actions     []fixActionProbe `json:"actions"`
	Unsafe      []string         `json:"unsafe"`
	Suggestions []string         `json:"suggestions"`
}

type fixActionProbe struct {
	Kind             string `json:"kind"`
	Seq              int64  `json:"seq"`
	Path             string `json:"path"`
	BranchRef        string `json:"branch_ref"`
	BranchGeneration int64  `json:"branch_generation"`
	PendingCount     int    `json:"pending_count"`
	RequiresForce    bool   `json:"requires_force"`
	ArchiveOnly      bool   `json:"archive_only"`
	Applied          bool   `json:"applied"`
	RowsChanged      int64  `json:"rows_changed"`
	State            string `json:"state"`
	RecoveryRef      string `json:"recovery_ref"`
}

func decodeFixPlan(t *testing.T, body string) fixPlanProbe {
	t.Helper()
	var plan fixPlanProbe
	if strings.TrimSpace(body) == "" {
		return plan
	}
	decodeProductEnvelopeData(t, body, &plan)
	return plan
}

func decodeProductEnvelopeData(t *testing.T, body string, target any) {
	t.Helper()
	var envelope struct {
		OK   *bool           `json:"ok"`
		Data json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal([]byte(body), &envelope); err != nil {
		t.Fatalf("decode command json: %v\nbody=%s", err, body)
	}
	payload := []byte(body)
	if envelope.OK != nil {
		if !*envelope.OK {
			t.Fatalf("command returned an error envelope: %s", body)
		}
		if len(envelope.Data) == 0 || string(envelope.Data) == "null" {
			t.Fatalf("command envelope omitted data: %s", body)
		}
		payload = envelope.Data
	}
	if err := json.Unmarshal(payload, target); err != nil {
		t.Fatalf("decode command data: %v\nbody=%s", err, body)
	}
}

func hasFixActionKind(actions []fixActionProbe, kind string) bool {
	for _, a := range actions {
		if a.Kind == kind {
			return true
		}
	}
	return false
}

func findFixActionForSeq(actions []fixActionProbe, kind, seq string) *fixActionProbe {
	for i := range actions {
		if actions[i].Kind == kind && fmt.Sprintf("%d", actions[i].Seq) == seq {
			return &actions[i]
		}
	}
	return nil
}

type seededRecoveryPair struct {
	FirstSeq  string
	SecondSeq string
}

func seedCreateRecoveryPair(
	t *testing.T,
	dbPath, branchRef, generation, baseHead, firstPath, firstOID, secondPath, secondOID string,
) seededRecoveryPair {
	t.Helper()
	now := nowFloatSeconds()
	seedSQL := fmt.Sprintf(`
INSERT INTO capture_events(branch_ref, branch_generation, base_head, operation, path, fidelity, captured_ts, state, error)
VALUES ('%s', %s, '%s', 'create', '%s', 'exact', %f, 'blocked_conflict', 'integration recovery barrier');
INSERT INTO capture_ops(event_seq, ord, op, path, after_oid, after_mode, fidelity)
VALUES (last_insert_rowid(), 0, 'create', '%s', '%s', '100644', 'exact');
INSERT INTO capture_events(branch_ref, branch_generation, base_head, operation, path, fidelity, captured_ts, state)
VALUES ('%s', %s, '%s', 'create', '%s', 'exact', %f, 'pending');
INSERT INTO capture_ops(event_seq, ord, op, path, after_oid, after_mode, fidelity)
VALUES (last_insert_rowid(), 0, 'create', '%s', '%s', '100644', 'exact');
`, branchRef, generation, baseHead, firstPath, now, firstPath, firstOID,
		branchRef, generation, baseHead, secondPath, now+0.001, secondPath, secondOID)
	if out, err := exec.Command("sqlite3", dbPath, seedSQL).CombinedOutput(); err != nil {
		t.Fatalf("seed exact recovery pair: %v\n%s", err, out)
	}
	pair := seededRecoveryPair{
		FirstSeq: sqliteScalar(t, dbPath, fmt.Sprintf(
			"SELECT seq FROM capture_events WHERE path = '%s' ORDER BY seq DESC LIMIT 1", firstPath)),
		SecondSeq: sqliteScalar(t, dbPath, fmt.Sprintf(
			"SELECT seq FROM capture_events WHERE path = '%s' ORDER BY seq DESC LIMIT 1", secondPath)),
	}
	if pair.FirstSeq == "" || pair.SecondSeq == "" {
		t.Fatalf("seeded recovery pair missing seqs: %+v", pair)
	}
	return pair
}

func exactPairStates(t *testing.T, dbPath string, pair seededRecoveryPair) string {
	t.Helper()
	return sqliteScalar(t, dbPath, fmt.Sprintf(
		"SELECT group_concat(state, ',') FROM (SELECT state FROM capture_events WHERE seq IN (%s,%s) ORDER BY seq)",
		pair.FirstSeq, pair.SecondSeq))
}

func recoveryRefList(t *testing.T, repo string) string {
	t.Helper()
	return runGitOK(t, repo, "for-each-ref", "--format=%(refname):%(objectname)", "refs/acd/recovery/")
}
