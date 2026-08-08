//go:build integration
// +build integration

package integration_test

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestExplainableUX_ProtectedTrackedSensitiveFileIsNotDeleted(t *testing.T) {
	if _, err := exec.LookPath("sqlite3"); err != nil {
		t.Skip("sqlite3 binary required for decision assertions")
	}

	repo := tempRepo(t)
	env := withIsolatedHome(t)
	t.Cleanup(func() { stopSessionForce(t, env, repo) })

	writeFile(t, filepath.Join(repo, ".env"), "SECRET=tracked\n")
	gitCommitAll(t, repo, "track sensitive env", ".env")

	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	startSession(t, ctx, env, repo, "ux-sensitive-1", "shell")
	waitMode(t, repo, "running", 5*time.Second)

	writeFile(t, filepath.Join(repo, "harmless.txt"), "commit me\n")
	wakeSession(t, ctx, env, repo, "ux-sensitive-1")
	waitForCommitContaining(t, repo, "harmless.txt", 10*time.Second)

	if out, err := runGit(repo, "cat-file", "-e", "HEAD:.env"); err != nil {
		t.Fatalf("tracked sensitive .env disappeared from HEAD: %v\n%s", err, out)
	}

	dbPath := filepath.Join(repo, ".git", "acd", "state.db")
	waitFor(t, "protected decision for tracked .env", 8*time.Second, func() bool {
		return sqliteScalar(t, dbPath,
			"SELECT COUNT(*) FROM decision_records WHERE path = '.env' AND kind = 'protected' AND action_taken = 'no_delete_generated'") != "0"
	})

	events := runAcd(t, ctx, env, "events", "--repo", repo, "--path", ".env", "--json")
	if events.ExitCode != 0 {
		t.Fatalf("acd events exit=%d\nstdout=%s\nstderr=%s", events.ExitCode, events.Stdout, events.Stderr)
	}
	if !strings.Contains(events.Stdout, `"kind": "protected"`) || !strings.Contains(events.Stdout, `"action_taken": "no_delete_generated"`) {
		t.Fatalf("events output missing protected/no_delete_generated decision:\n%s", events.Stdout)
	}

	explain := runAcd(t, ctx, env, "explain", "--repo", repo, "--path", ".env")
	if explain.ExitCode != 0 {
		t.Fatalf("acd explain exit=%d\nstdout=%s\nstderr=%s", explain.ExitCode, explain.Stdout, explain.Stderr)
	}
	for _, want := range []string{"Explanation:", "Skipped protected path .env", "Next: No action is needed"} {
		if !strings.Contains(explain.Stdout, want) {
			t.Fatalf("explain output missing %q:\n%s", want, explain.Stdout)
		}
	}
}

func TestExplainableUX_DecisionLedgerDrivesEventsExplainAndFix(t *testing.T) {
	if _, err := exec.LookPath("sqlite3"); err != nil {
		t.Skip("sqlite3 binary required for seeded state")
	}

	repo := tempRepo(t)
	env := withIsolatedHome(t)
	t.Cleanup(func() { stopSessionForce(t, env, repo) })

	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	startSession(t, ctx, env, repo, "ux-ledger-1", "shell")
	waitMode(t, repo, "running", 5*time.Second)
	off := runAcd(t, ctx, env, "off", "--repo", repo, "--force", "--json")
	if off.ExitCode != 0 {
		t.Fatalf("acd off exit=%d\nstdout=%s\nstderr=%s", off.ExitCode, off.Stdout, off.Stderr)
	}
	if !waitStopped(repo, 5*time.Second) {
		t.Fatal("repository worker did not stop after acd off")
	}

	writeFile(t, filepath.Join(repo, "manual.txt"), "landed outside acd\n")
	manualHead := gitCommitAll(t, repo, "manual external commit", "manual.txt")
	manualOID := strings.Fields(runGitOK(t, repo, "ls-tree", manualHead, "manual.txt"))[2]
	revertedAfterOID := gitHashObjectStdin(t, repo, "queued work that was later reverted\n")
	obsoleteAfterOID := gitHashObjectStdin(t, repo, "obsolete blocked work\n")
	dbPath := filepath.Join(repo, ".git", "acd", "state.db")
	gen := sqliteScalar(t, dbPath, "SELECT value FROM daemon_meta WHERE key = 'branch.generation'")
	if gen == "" {
		gen = "1"
	}
	now := nowFloatSeconds()

	seedSQL := fmt.Sprintf(`
INSERT INTO capture_events(branch_ref, branch_generation, base_head, operation, path, fidelity, captured_ts, state, error)
VALUES ('refs/heads/main', %s, '%s', 'create', 'manual.txt', 'exact', %f, 'blocked_conflict', 'before-state mismatch');
INSERT INTO capture_ops(event_seq, ord, op, path, after_oid, after_mode, fidelity)
VALUES (last_insert_rowid(), 0, 'create', 'manual.txt', '%s', '100644', 'exact');
INSERT INTO decision_records(decision_ts, kind, path, reason, event_seq, commit_oid, branch_ref, branch_generation, action_taken, user_message)
VALUES (%f, 'handled_external', 'manual.txt', 'already_published_by_external_committer', (SELECT seq FROM capture_events WHERE path = 'manual.txt' ORDER BY seq DESC LIMIT 1), '%s', 'refs/heads/main', %s, 'marked_published', 'Manual commit already contains this change.');
INSERT INTO capture_events(branch_ref, branch_generation, base_head, operation, path, fidelity, captured_ts, state, error)
VALUES ('refs/heads/main', %s, '%s', 'create', 'reverted.txt', 'exact', %f, 'pending', NULL);
INSERT INTO capture_ops(event_seq, ord, op, path, after_oid, after_mode, fidelity)
VALUES (last_insert_rowid(), 0, 'create', 'reverted.txt', '%s', '100644', 'exact');
INSERT INTO decision_records(decision_ts, kind, path, reason, event_seq, commit_oid, branch_ref, branch_generation, action_taken, user_message)
VALUES (%f, 'superseded_external', 'reverted.txt', 'superseded_external_current_head_matches_captured_before_state', (SELECT seq FROM capture_events WHERE path = 'reverted.txt' ORDER BY seq DESC LIMIT 1), '%s', 'refs/heads/main', %s, 'marked_published', 'Manual revert superseded queued ACD work.');
INSERT INTO capture_events(branch_ref, branch_generation, base_head, operation, path, fidelity, captured_ts, state, error)
VALUES ('refs/heads/main', %s, '%s', 'create', 'obsolete-blocker.txt', 'exact', %f, 'blocked_conflict', 'before-state mismatch');
INSERT INTO capture_ops(event_seq, ord, op, path, after_oid, after_mode, fidelity)
VALUES (last_insert_rowid(), 0, 'create', 'obsolete-blocker.txt', '%s', '100644', 'exact');
`, gen, manualHead, now, manualOID, now+0.001, manualHead, gen,
		gen, manualHead, now+0.002, revertedAfterOID, now+0.003, manualHead, gen,
		gen, manualHead, now+0.004, obsoleteAfterOID)
	if out, err := exec.Command("sqlite3", dbPath, seedSQL).CombinedOutput(); err != nil {
		t.Fatalf("seed decision ledger: %v\n%s", err, out)
	}
	manualSeq := sqliteScalar(t, dbPath, "SELECT seq FROM capture_events WHERE path = 'manual.txt' ORDER BY seq DESC LIMIT 1")
	var manualSeqNumber int64
	if _, err := fmt.Sscan(manualSeq, &manualSeqNumber); err != nil {
		t.Fatalf("parse manual event seq %q: %v", manualSeq, err)
	}

	events := runAcd(t, ctx, env, "events", "--repo", repo, "--json")
	if events.ExitCode != 0 {
		t.Fatalf("acd events exit=%d\nstdout=%s\nstderr=%s", events.ExitCode, events.Stdout, events.Stderr)
	}
	if !strings.Contains(events.Stdout, `"kind": "handled_external"`) ||
		!strings.Contains(events.Stdout, `"kind": "superseded_external"`) {
		t.Fatalf("events output missing external decisions:\n%s", events.Stdout)
	}

	explainPath := runAcd(t, ctx, env, "explain", "--repo", repo, "--path", "reverted.txt")
	if explainPath.ExitCode != 0 {
		t.Fatalf("acd explain path exit=%d\nstdout=%s\nstderr=%s", explainPath.ExitCode, explainPath.Stdout, explainPath.Stderr)
	}
	if !strings.Contains(explainPath.Stdout, "Manual revert superseded queued ACD work.") {
		t.Fatalf("path explain did not surface superseded decision:\n%s", explainPath.Stdout)
	}

	explainCommit := runAcd(t, ctx, env, "explain", "--repo", repo, "--commit", manualHead, "--json")
	if explainCommit.ExitCode != 0 {
		t.Fatalf("acd explain commit exit=%d\nstdout=%s\nstderr=%s", explainCommit.ExitCode, explainCommit.Stdout, explainCommit.Stderr)
	}
	if !strings.Contains(explainCommit.Stdout, `"mode": "commit"`) ||
		!strings.Contains(explainCommit.Stdout, `"kind": "handled_external"`) {
		t.Fatalf("commit explain did not link external decision:\n%s", explainCommit.Stdout)
	}

	fixDryRun := runAcd(t, ctx, env, "fix", "--repo", repo, "--dry-run", "--json")
	if fixDryRun.ExitCode != 0 {
		t.Fatalf("acd fix dry-run exit=%d\nstdout=%s\nstderr=%s", fixDryRun.ExitCode, fixDryRun.Stdout, fixDryRun.Stderr)
	}
	var plan struct {
		DryRun  bool `json:"dry_run"`
		Actions []struct {
			Kind         string `json:"kind"`
			Seq          int64  `json:"seq"`
			PendingCount int    `json:"pending_count"`
		} `json:"actions"`
	}
	if err := json.Unmarshal([]byte(fixDryRun.Stdout), &plan); err != nil {
		t.Fatalf("decode fix dry-run: %v\n%s", err, fixDryRun.Stdout)
	}
	if !plan.DryRun || len(plan.Actions) != 1 ||
		plan.Actions[0].Kind != "reconcile_unpublished_chain" ||
		plan.Actions[0].Seq != manualSeqNumber ||
		plan.Actions[0].PendingCount != 3 {
		t.Fatalf("fix dry-run did not plan expected safe actions: %+v\n%s", plan, fixDryRun.Stdout)
	}

	fixApply := runAcd(t, ctx, env, "fix", "--repo", repo, "--yes", "--json")
	if fixApply.ExitCode != 0 {
		t.Fatalf("acd fix apply exit=%d\nstdout=%s\nstderr=%s", fixApply.ExitCode, fixApply.Stdout, fixApply.Stderr)
	}
	if states := sqliteScalar(t, dbPath, "SELECT group_concat(state, ',') FROM (SELECT state FROM capture_events ORDER BY seq)"); states != "recovered,recovered,recovered" {
		t.Fatalf("whole-chain states=%q want all recovered\ndry-run=%s\napply=%s", states, fixDryRun.Stdout, fixApply.Stdout)
	}
	if members := sqliteScalar(t, dbPath, "SELECT COUNT(*) FROM recovery_snapshot_events"); members != "3" {
		t.Fatalf("recovery snapshot members=%q want 3\napply=%s", members, fixApply.Stdout)
	}
}

func TestExplainableUX_DaemonRecordsHandledExternalDecision(t *testing.T) {
	requireSQLite(t)
	if _, err := exec.LookPath("bash"); err != nil {
		t.Skip("bash not available; slow subprocess provider requires bash")
	}

	repo := tempRepo(t)
	env := withIsolatedHome(t)
	t.Cleanup(func() { stopSessionForce(t, env, repo) })

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	target := filepath.Join(repo, "external-handled.txt")
	writeFile(t, target, "before\n")
	baseHead := gitCommitAll(t, repo, "baseline handled external", "external-handled.txt")
	providerStarted := filepath.Join(t.TempDir(), "provider-started")
	plugDir := writePluginScript(t, "slow-handled", fmt.Sprintf(`#!/usr/bin/env bash
while IFS= read -r line; do
  printf 'started\n' > %q
  sleep 2
  printf '{"version":1,"subject":"slow handled race","body":"","error":""}\n'
done
`, providerStarted))
	slowEnv := envWith(env,
		"ACD_AI_PROVIDER=subprocess:slow-handled",
		"ACD_AI_TIMEOUT=10s",
		pathPrepended(plugDir),
	)

	startSession(t, ctx, slowEnv, repo, "ux-handled-daemon", "shell")
	waitMode(t, repo, "running", 5*time.Second)

	dbPath := filepath.Join(repo, ".git", "acd", "state.db")
	writeFile(t, target, "same change\n")
	wakeSession(t, ctx, slowEnv, repo, "ux-handled-daemon")
	waitFor(t, "provider entered after replay conflict probe", 8*time.Second, func() bool {
		_, err := os.Stat(providerStarted)
		return err == nil
	})
	externalHead := gitCommitAll(t, repo, "external handled commit", "external-handled.txt")
	if externalHead == baseHead {
		t.Fatalf("external commit did not advance HEAD")
	}

	waitForEventState(t, dbPath, "external-handled.txt", "published", 15*time.Second)
	waitForDecision(t, dbPath, "external-handled.txt", "handled_external", "already_published_after_cas_exhaustion", 15*time.Second)

	if got := sqliteScalar(t, dbPath, "SELECT commit_oid FROM capture_events WHERE path = 'external-handled.txt' ORDER BY seq DESC LIMIT 1"); got != externalHead {
		t.Fatalf("published commit_oid=%q want external HEAD %s", got, externalHead)
	}
	if count := strings.TrimSpace(runGitOK(t, repo, "rev-list", "--count", "HEAD")); count != "3" {
		log := runGitOK(t, repo, "log", "--oneline", "--decorate")
		t.Fatalf("commit count=%s want 3 (seed + baseline + external only)\nlog:\n%s", count, log)
	}

	events := runAcd(t, ctx, env, "events", "--repo", repo, "--path", "external-handled.txt", "--json")
	if events.ExitCode != 0 {
		t.Fatalf("acd events exit=%d\nstdout=%s\nstderr=%s", events.ExitCode, events.Stdout, events.Stderr)
	}
	if !strings.Contains(events.Stdout, `"kind": "handled_external"`) {
		t.Fatalf("events output missing daemon-recorded handled_external:\n%s", events.Stdout)
	}
}

func TestExplainableUX_DaemonRecordsSupersededExternalDecision(t *testing.T) {
	requireSQLite(t)
	if _, err := exec.LookPath("bash"); err != nil {
		t.Skip("bash not available; slow subprocess provider requires bash")
	}

	providerStarted := filepath.Join(t.TempDir(), "provider-started")
	plugDir := writePluginScript(t, "slow", fmt.Sprintf(`#!/usr/bin/env bash
while IFS= read -r line; do
	printf 'started\n' > %q
  sleep 10
  printf '{"version":1,"subject":"slow provider","body":"","error":""}\n'
done
`, providerStarted))
	repo := tempRepo(t)
	env := withIsolatedHome(t)
	t.Cleanup(func() { stopSessionForce(t, env, repo) })

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	paths := []string{"external-race-a.txt", "external-race-b.txt"}
	for _, path := range paths {
		writeFile(t, filepath.Join(repo, path), "before\n")
	}
	baseHead := gitCommitAll(t, repo, "baseline reverted external", paths...)

	slowEnv := envWith(env,
		"ACD_AI_PROVIDER=subprocess:slow",
		"ACD_AI_TIMEOUT=30s",
		pathPrepended(plugDir),
	)
	startSession(t, ctx, slowEnv, repo, "ux-superseded-daemon", "shell",
		"ACD_AI_PROVIDER=subprocess:slow",
		"ACD_AI_TIMEOUT=30s",
		pathPrepended(plugDir),
	)
	waitMode(t, repo, "running", 5*time.Second)

	dbPath := filepath.Join(repo, ".git", "acd", "state.db")
	for _, path := range paths {
		writeFile(t, filepath.Join(repo, path), "after\n")
	}
	wakeSession(t, ctx, slowEnv, repo, "ux-superseded-daemon")
	waitFor(t, "daemon checkpointed both external-race captures", 10*time.Second, func() bool {
		return sqliteScalar(t, dbPath,
			`SELECT COUNT(DISTINCT e.path)
FROM capture_events e
JOIN checkpoint_events ce ON ce.event_seq = e.seq
JOIN checkpoints c ON c.id = ce.checkpoint_id
WHERE e.path IN ('external-race-a.txt', 'external-race-b.txt')
  AND e.state = 'pending'
  AND c.phase = 'completed'`) == "2"
	})
	ordered := strings.Split(sqliteScalar(t, dbPath,
		`SELECT group_concat(path, ',') FROM (
  SELECT path FROM capture_events
  WHERE path IN ('external-race-a.txt', 'external-race-b.txt')
  ORDER BY seq
)`), ",")
	if len(ordered) != 2 {
		t.Fatalf("captured path order=%q want two paths", ordered)
	}
	handledPath, supersededPath := ordered[0], ordered[1]
	waitFor(t, "provider entered after completed checkpoint", 8*time.Second, func() bool {
		_, err := os.Stat(providerStarted)
		return err == nil
	})

	externalAfter := gitCommitAll(t, repo, "external after", paths...)
	writeFile(t, filepath.Join(repo, supersededPath), "before\n")
	externalRevert := gitCommitAll(t, repo, "external revert", supersededPath)
	if externalAfter == baseHead || externalRevert == externalAfter {
		t.Fatalf("external revert history did not advance as expected: base=%s after=%s revert=%s", baseHead, externalAfter, externalRevert)
	}

	if !eventStateBecomes(dbPath, handledPath, "published", 30*time.Second) {
		rows := sqliteScalar(t, dbPath,
			"SELECT group_concat(seq || ':' || path || ':' || state || ':' || COALESCE(error, ''), char(10)) FROM capture_events ORDER BY seq")
		decisions := sqliteScalar(t, dbPath,
			"SELECT group_concat(kind || ':' || COALESCE(path, '') || ':' || COALESCE(reason, ''), char(10)) FROM decision_records ORDER BY id")
		journals := sqliteScalar(t, dbPath,
			"SELECT group_concat(id || ':' || phase || ':' || source_head || ':' || target_commit_oid, char(10)) FROM self_publications ORDER BY id")
		checkpoints := sqliteScalar(t, dbPath,
			"SELECT group_concat(id || ':' || phase || ':' || reason, char(10)) FROM checkpoints ORDER BY seq")
		t.Fatalf("%s did not publish\nrows:\n%s\ndecisions:\n%s\njournals:\n%s\ncheckpoints:\n%s",
			handledPath, rows, decisions, journals, checkpoints)
	}
	if !eventStateBecomes(dbPath, supersededPath, "published", 20*time.Second) {
		dump := sqliteScalar(t, dbPath,
			fmt.Sprintf("SELECT group_concat(seq || ':' || state || ':' || COALESCE(error, ''), char(10)) FROM capture_events WHERE path = %s ORDER BY seq", sqliteQuote(supersededPath)))
		decisions := sqliteScalar(t, dbPath,
			fmt.Sprintf("SELECT group_concat(kind || ':' || COALESCE(reason, ''), char(10)) FROM decision_records WHERE path = %s ORDER BY id", sqliteQuote(supersededPath)))
		t.Fatalf("%s did not publish after restart\nrows:\n%s\ndecisions:\n%s", supersededPath, dump, decisions)
	}
	waitForDecision(t, dbPath, handledPath, "handled_external", "already_published_after_cas_exhaustion", 8*time.Second)
	waitForDecision(t, dbPath, supersededPath, "superseded_external", "superseded_external_current_head_matches_captured_before_state", 8*time.Second)

	if out, err := runGit(repo, "cat-file", "-e", "HEAD:"+supersededPath); err != nil {
		t.Fatalf("target missing after superseded replay: %v\n%s", err, out)
	}
	if got := runGitOK(t, repo, "show", "HEAD:"+supersededPath); got != "before\n" {
		t.Fatalf("target content=%q want before-state", got)
	}
	if count := strings.TrimSpace(runGitOK(t, repo, "rev-list", "--count", "HEAD")); count != "4" {
		log := runGitOK(t, repo, "log", "--oneline", "--decorate")
		t.Fatalf("commit count=%s want 4 (seed + baseline + external after/revert only)\nlog:\n%s", count, log)
	}

	events := runAcd(t, ctx, env, "events", "--repo", repo, "--path", supersededPath, "--json")
	if events.ExitCode != 0 {
		t.Fatalf("acd events exit=%d\nstdout=%s\nstderr=%s", events.ExitCode, events.Stdout, events.Stderr)
	}
	if !strings.Contains(events.Stdout, `"kind": "superseded_external"`) {
		t.Fatalf("events output missing daemon-recorded superseded_external:\n%s", events.Stdout)
	}
}

func TestExplainableUX_ReadOnlyCommandsDoNotMigratePreDecisionLedgerDB(t *testing.T) {
	requireSQLite(t)

	repo := tempRepo(t)
	env := withIsolatedHome(t)
	t.Cleanup(func() { stopSessionForce(t, env, repo) })

	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	dbPath := initStateDBSchema(t, ctx, env, repo, "ux-readonly-bootstrap")
	if out, err := exec.Command("sqlite3", dbPath, `
DROP TABLE IF EXISTS decision_records;
PRAGMA user_version = 4;
PRAGMA wal_checkpoint(TRUNCATE);
`).CombinedOutput(); err != nil {
		t.Fatalf("prepare pre-decision-ledger db: %v\n%s", err, out)
	}
	before := fileSHA256(t, dbPath)
	versionBefore := sqliteScalar(t, dbPath, "PRAGMA user_version")

	events := runAcd(t, ctx, env, "events", "--repo", repo, "--json")
	if events.ExitCode != 0 {
		t.Fatalf("acd events exit=%d\nstdout=%s\nstderr=%s", events.ExitCode, events.Stdout, events.Stderr)
	}
	if !strings.Contains(events.Stdout, "Decision ledger is not available") {
		t.Fatalf("events did not report missing decision ledger:\n%s", events.Stdout)
	}
	explain := runAcd(t, ctx, env, "explain", "--repo", repo, "--json")
	if explain.ExitCode != 0 {
		t.Fatalf("acd explain exit=%d\nstdout=%s\nstderr=%s", explain.ExitCode, explain.Stdout, explain.Stderr)
	}
	if !strings.Contains(explain.Stdout, "Decision ledger is not available") {
		t.Fatalf("explain did not report missing decision ledger:\n%s", explain.Stdout)
	}

	if after := fileSHA256(t, dbPath); after != before {
		t.Fatalf("read-only commands changed state.db checksum: before=%s after=%s", before, after)
	}
	if got := sqliteScalar(t, dbPath, "PRAGMA user_version"); got != versionBefore {
		t.Fatalf("user_version changed: before=%s after=%s", versionBefore, got)
	}
}

func TestExplainableUX_EventsWatchStreamsAppendedDecisions(t *testing.T) {
	if _, err := exec.LookPath("sqlite3"); err != nil {
		t.Skip("sqlite3 binary required for seeded state")
	}

	repo := tempRepo(t)
	env := withIsolatedHome(t)
	t.Cleanup(func() { stopSessionForce(t, env, repo) })

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	startSession(t, ctx, env, repo, "ux-watch-1", "shell")
	waitMode(t, repo, "running", 5*time.Second)
	stopSessionForce(t, env, repo)

	dbPath := filepath.Join(repo, ".git", "acd", "state.db")
	cursor := sqliteScalar(t, dbPath, "SELECT COALESCE(MAX(id), 0) FROM decision_records")

	watchCtx, stopWatch := context.WithCancel(ctx)
	defer stopWatch()
	cmd := exec.CommandContext(watchCtx, buildAcdBinary(t),
		"events", "--repo", repo, "--watch", "--interval", "50ms", "--since", cursor, "--json")
	cmd.Env = env
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatalf("events watch stdout pipe: %v", err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatalf("events watch start: %v", err)
	}

	lines := make(chan string, 8)
	scanDone := make(chan struct{})
	go func() {
		defer close(scanDone)
		scanner := bufio.NewScanner(stdout)
		for scanner.Scan() {
			lines <- scanner.Text()
		}
	}()

	time.Sleep(150 * time.Millisecond)
	if out, err := exec.Command("sqlite3", dbPath, fmt.Sprintf(`
INSERT INTO decision_records(decision_ts, kind, path, reason, action_taken, user_message)
VALUES (%f, 'blocked', 'watch.txt', 'before-state mismatch', 'blocked_conflict', 'watch observed this decision');
`, nowFloatSeconds())).CombinedOutput(); err != nil {
		t.Fatalf("insert watched decision: %v\n%s", err, out)
	}

	deadline := time.After(5 * time.Second)
	for {
		select {
		case line := <-lines:
			if strings.Contains(line, `"kind":"blocked"`) && strings.Contains(line, `"path":"watch.txt"`) {
				stopWatch()
				_ = cmd.Wait()
				<-scanDone
				return
			}
		case <-deadline:
			stopWatch()
			_ = cmd.Wait()
			t.Fatalf("events --watch did not stream appended decision")
		}
	}
}

func waitForDecision(t *testing.T, dbPath, path, kind, reason string, timeout time.Duration) {
	t.Helper()
	query := fmt.Sprintf(
		"SELECT COUNT(*) FROM decision_records WHERE path = %s AND kind = %s AND reason = %s",
		sqliteQuote(path),
		sqliteQuote(kind),
		sqliteQuote(reason),
	)
	waitFor(t, fmt.Sprintf("decision %s for %s", kind, path), timeout, func() bool {
		return sqliteScalar(t, dbPath, query) != "0"
	})
}

func eventStateBecomes(dbPath, path, want string, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	query := fmt.Sprintf("SELECT state FROM capture_events WHERE path = %s ORDER BY seq DESC LIMIT 1", sqliteQuote(path))
	for time.Now().Before(deadline) {
		out, err := exec.Command("sqlite3", dbPath, query).CombinedOutput()
		if err == nil && strings.TrimSpace(string(out)) == want {
			return true
		}
		time.Sleep(75 * time.Millisecond)
	}
	return false
}

func fileSHA256(t *testing.T, path string) string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return fmt.Sprintf("%x", sha256.Sum256(data))
}
