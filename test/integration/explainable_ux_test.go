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
	"syscall"
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
	for _, want := range []string{"Explanation:", "Skipped present protected path .env", "Next: No action is needed"} {
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
	stopSessionForce(t, env, repo)

	writeFile(t, filepath.Join(repo, "manual.txt"), "landed outside acd\n")
	manualHead := gitCommitAll(t, repo, "manual external commit", "manual.txt")
	manualOID := strings.Fields(runGitOK(t, repo, "ls-tree", manualHead, "manual.txt"))[2]
	revertedAfterOID := gitHashObjectStdin(t, repo, "queued work that was later reverted\n")
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
VALUES ('refs/heads/main', %s, '%s', 'modify', 'obsolete-blocker.txt', 'rescan', %f, 'blocked_conflict', 'before-state mismatch');
`, gen, manualHead, now, manualOID, now+0.001, manualHead, gen,
		gen, manualHead, now+0.002, revertedAfterOID, now+0.003, manualHead, gen,
		gen, manualHead, now+0.004)
	if out, err := exec.Command("sqlite3", dbPath, seedSQL).CombinedOutput(); err != nil {
		t.Fatalf("seed decision ledger: %v\n%s", err, out)
	}
	manualSeq := sqliteScalar(t, dbPath, "SELECT seq FROM capture_events WHERE path = 'manual.txt' ORDER BY seq DESC LIMIT 1")

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
			Kind string `json:"kind"`
			Seq  int64  `json:"seq"`
		} `json:"actions"`
	}
	if err := json.Unmarshal([]byte(fixDryRun.Stdout), &plan); err != nil {
		t.Fatalf("decode fix dry-run: %v\n%s", err, fixDryRun.Stdout)
	}
	if !plan.DryRun || !hasIntegrationFixAction(plan.Actions, "mark_external_published") ||
		!hasIntegrationFixAction(plan.Actions, "delete_obsolete_barrier") {
		t.Fatalf("fix dry-run did not plan expected safe actions: %+v\n%s", plan, fixDryRun.Stdout)
	}

	fixApply := runAcd(t, ctx, env, "fix", "--repo", repo, "--yes", "--json")
	if fixApply.ExitCode != 0 {
		t.Fatalf("acd fix apply exit=%d\nstdout=%s\nstderr=%s", fixApply.ExitCode, fixApply.Stdout, fixApply.Stderr)
	}
	if state := sqliteScalar(t, dbPath, fmt.Sprintf("SELECT state FROM capture_events WHERE seq = %s", manualSeq)); state != "published" {
		t.Fatalf("manual external event state=%q want published\ndry-run=%s\napply=%s", state, fixDryRun.Stdout, fixApply.Stdout)
	}
}

func TestExplainableUX_DaemonRecordsHandledExternalDecision(t *testing.T) {
	requireSQLite(t)

	repo := tempRepo(t)
	env := withIsolatedHome(t)
	t.Cleanup(func() { stopSessionForce(t, env, repo) })

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	target := filepath.Join(repo, "external-handled.txt")
	writeFile(t, target, "before\n")
	baseHead := gitCommitAll(t, repo, "baseline handled external", "external-handled.txt")

	startSession(t, ctx, env, repo, "ux-handled-daemon", "shell")
	waitMode(t, repo, "running", 5*time.Second)

	dbPath := filepath.Join(repo, ".git", "acd", "state.db")
	pauseReplay(t, ctx, env, repo, "handled external integration")
	writeFile(t, target, "same change\n")
	externalHead := gitCommitAll(t, repo, "external handled commit", "external-handled.txt")
	if externalHead == baseHead {
		t.Fatalf("external commit did not advance HEAD")
	}

	resumeReplay(t, ctx, env, repo)
	wakeSession(t, ctx, env, repo, "ux-handled-daemon")
	waitForEventState(t, dbPath, "external-handled.txt", "published", 8*time.Second)
	waitForDecision(t, dbPath, "external-handled.txt", "handled_external", "already_published_by_external_committer", 8*time.Second)

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

	plugDir := writePluginScript(t, "slow", `#!/usr/bin/env bash
while IFS= read -r line; do
  sleep 10
  printf '{"version":1,"subject":"slow provider","body":"","error":""}\n'
done
`)
	repo := tempRepo(t)
	env := withIsolatedHome(t)
	t.Cleanup(func() { stopSessionForce(t, env, repo) })

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	target := filepath.Join(repo, "zzz-reverted.txt")
	writeFile(t, target, "before\n")
	baseHead := gitCommitAll(t, repo, "baseline reverted external", "zzz-reverted.txt")

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
	writeFile(t, filepath.Join(repo, "aaa-slow.txt"), "first queued event\n")
	writeFile(t, target, "after\n")
	wakeSession(t, ctx, slowEnv, repo, "ux-superseded-daemon")
	waitFor(t, "daemon captured superseded target before replay", 5*time.Second, func() bool {
		return sqliteScalar(t, dbPath,
			"SELECT COUNT(*) FROM capture_events WHERE path = 'zzz-reverted.txt' AND state = 'pending'") != "0"
	})

	pid := readDaemonStatePID(repo)
	if pid <= 0 {
		t.Fatalf("missing daemon pid before forced interruption")
	}
	_ = syscall.Kill(pid, syscall.SIGKILL)
	waitFor(t, "slow daemon killed before replay drained target", 5*time.Second, func() bool {
		return !processAlive(pid)
	})

	externalAfter := gitCommitAll(t, repo, "external after", "zzz-reverted.txt")
	writeFile(t, target, "before\n")
	externalRevert := gitCommitAll(t, repo, "external revert", "zzz-reverted.txt")
	if externalAfter == baseHead || externalRevert == externalAfter {
		t.Fatalf("external revert history did not advance as expected: base=%s after=%s revert=%s", baseHead, externalAfter, externalRevert)
	}

	startSession(t, ctx, env, repo, "ux-superseded-resume", "shell")
	waitMode(t, repo, "running", 5*time.Second)
	wakeSession(t, ctx, env, repo, "ux-superseded-resume")
	waitForEventState(t, dbPath, "aaa-slow.txt", "published", 12*time.Second)
	waitForEventState(t, dbPath, "zzz-reverted.txt", "published", 12*time.Second)
	waitForDecision(t, dbPath, "zzz-reverted.txt", "superseded_external", "superseded_external_current_head_matches_captured_before_state", 8*time.Second)

	if out, err := runGit(repo, "cat-file", "-e", "HEAD:zzz-reverted.txt"); err != nil {
		t.Fatalf("target missing after superseded replay: %v\n%s", err, out)
	}
	if got := runGitOK(t, repo, "show", "HEAD:zzz-reverted.txt"); got != "before\n" {
		t.Fatalf("target content=%q want before-state", got)
	}
	if count := strings.TrimSpace(runGitOK(t, repo, "rev-list", "--count", "HEAD")); count != "5" {
		log := runGitOK(t, repo, "log", "--oneline", "--decorate")
		t.Fatalf("commit count=%s want 5 (seed + baseline + external after/revert + aaa replay)\nlog:\n%s", count, log)
	}

	events := runAcd(t, ctx, env, "events", "--repo", repo, "--path", "zzz-reverted.txt", "--json")
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

func hasIntegrationFixAction(actions []struct {
	Kind string `json:"kind"`
	Seq  int64  `json:"seq"`
}, kind string) bool {
	for _, action := range actions {
		if action.Kind == kind {
			return true
		}
	}
	return false
}
