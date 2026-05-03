//go:build integration
// +build integration

package integration_test

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
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
	for _, want := range []string{"Explanation:", "ACD skipped .env", "Next: No action is needed"} {
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
	dbPath := filepath.Join(repo, ".git", "acd", "state.db")
	gen := sqliteScalar(t, dbPath, "SELECT value FROM daemon_meta WHERE key = 'branch.generation'")
	if gen == "" {
		gen = "1"
	}
	now := nowFloatSeconds()

	seedSQL := fmt.Sprintf(`
INSERT INTO capture_events(branch_ref, branch_generation, base_head, operation, path, fidelity, captured_ts, state, error)
VALUES ('refs/heads/main', %s, '%s', 'modify', 'manual.txt', 'exact', %f, 'blocked_conflict', 'before-state mismatch');
INSERT INTO decision_records(decision_ts, kind, path, reason, event_seq, commit_oid, branch_ref, branch_generation, action_taken, user_message)
VALUES (%f, 'handled_external', 'manual.txt', 'already_published_by_external_committer', last_insert_rowid(), '%s', 'refs/heads/main', %s, 'marked_published', 'Manual commit already contains this change.');
INSERT INTO capture_events(branch_ref, branch_generation, base_head, operation, path, fidelity, captured_ts, state, error)
VALUES ('refs/heads/main', %s, '%s', 'modify', 'reverted.txt', 'exact', %f, 'pending', NULL);
INSERT INTO decision_records(decision_ts, kind, path, reason, event_seq, commit_oid, branch_ref, branch_generation, action_taken, user_message)
VALUES (%f, 'superseded_external', 'reverted.txt', 'superseded_external_current_head_matches_captured_before_state', last_insert_rowid(), '%s', 'refs/heads/main', %s, 'marked_published', 'Manual revert superseded queued ACD work.');
INSERT INTO capture_events(branch_ref, branch_generation, base_head, operation, path, fidelity, captured_ts, state, error)
VALUES ('refs/heads/main', %s, '%s', 'modify', 'obsolete-blocker.txt', 'rescan', %f, 'blocked_conflict', 'before-state mismatch');
`, gen, manualHead, now, now+0.001, manualHead, gen,
		gen, manualHead, now+0.002, now+0.003, manualHead, gen,
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
