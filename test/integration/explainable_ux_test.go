//go:build integration

package integration_test

import (
	"bufio"
	"context"
	"crypto/sha256"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestExplainableUX_ProtectedTrackedSensitiveFileIsNotDeleted(t *testing.T) {
	f := newFinishFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if out := f.Run(t, ctx, "off", "--json"); out.ExitCode != 0 {
		t.Fatalf("off %+v", out)
	}
	writeFile(t, filepath.Join(f.Repo, ".env"), "SECRET=tracked\n")
	gitCommitAll(t, f.Repo, "Track original sensitive entry", ".env")
	writeFile(t, filepath.Join(f.Repo, ".env"), "SECRET=later-private\n")
	on := f.Run(t, ctx, "on", "--json")
	if on.ExitCode != 0 {
		t.Fatalf("on %+v", on)
	}
	writeFile(t, filepath.Join(f.Repo, "harmless.txt"), "complete goal\n")
	f.Barrier(t, ctx)
	done := f.Run(t, ctx, "finish", "Complete harmless goal", "--json")
	if done.ExitCode != 0 {
		t.Fatalf("finish %+v", done)
	}
	if got := runGitOK(t, f.Repo, "show", "HEAD:.env"); got != "SECRET=tracked\n" {
		t.Fatalf("sensitive tracked entry changed: %q", got)
	}
	if got := readOptionalFile(t, filepath.Join(f.Repo, ".env")); got != "SECRET=later-private\n" {
		t.Fatal("later private work overwritten")
	}
	if got := runGitOK(t, f.Repo, "show", "HEAD:harmless.txt"); got != "complete goal\n" {
		t.Fatal("eligible work missing")
	}
}

func TestExplainableUX_ArchivedLedgerRemainsReadOnly(t *testing.T) {
	f := newFinishFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if out := f.Run(t, ctx, "off", "--json"); out.ExitCode != 0 {
		t.Fatalf("off %+v", out)
	}
	dbPath := f.DBPath
	seed := fmt.Sprintf(`INSERT INTO decision_records(decision_ts,kind,path,reason,commit_oid,action_taken,user_message) VALUES(1,'handled_external','manual.txt','already_published_by_external_committer','%s','marked_published','Manual commit already contains this change.'),(2,'superseded_external','reverted.txt','superseded_external_current_head_matches_captured_before_state','%s','marked_published','Manual revert superseded queued ACD work.'); PRAGMA wal_checkpoint(TRUNCATE);`, f.BaseHead, f.BaseHead)
	if out, err := exec.Command("sqlite3", dbPath, seed).CombinedOutput(); err != nil {
		t.Fatalf("seed %v %s", err, out)
	}
	before := fileSHA256(t, dbPath)
	activity := f.Run(t, ctx, "history", "activity", "--json")
	if activity.ExitCode != 0 || !strings.Contains(activity.Stdout, `"handled_external"`) || !strings.Contains(activity.Stdout, `"superseded_external"`) {
		t.Fatalf("activity %+v", activity)
	}
	path := f.Run(t, ctx, "history", "explain", "--path", "reverted.txt")
	if path.ExitCode != 0 || !strings.Contains(path.Stdout, "Manual revert superseded queued ACD work.") {
		t.Fatalf("path %+v", path)
	}
	commit := f.Run(t, ctx, "history", "explain", "--commit", f.BaseHead, "--json")
	if commit.ExitCode != 0 || !strings.Contains(commit.Stdout, `"handled_external"`) {
		t.Fatalf("commit %+v", commit)
	}
	if after := fileSHA256(t, dbPath); after != before {
		t.Fatal("archive inspection changed state")
	}
}

func TestExplainableUX_ReadOnlyCommandsDoNotMigratePreDecisionLedgerDB(t *testing.T) {
	requireSQLite(t)

	f := newFinishFixture(t)
	repo, env := f.Repo, f.Env

	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	if out := f.Run(t, ctx, "off", "--json"); out.ExitCode != 0 {
		t.Fatalf("off: %+v", out)
	}
	dbPath := f.DBPath
	if out, err := exec.Command("sqlite3", dbPath, `
DROP TABLE IF EXISTS decision_records;
PRAGMA user_version = 4;
PRAGMA wal_checkpoint(TRUNCATE);
`).CombinedOutput(); err != nil {
		t.Fatalf("prepare pre-decision-ledger db: %v\n%s", err, out)
	}
	before := fileSHA256(t, dbPath)
	versionBefore := sqliteScalar(t, dbPath, "PRAGMA user_version")

	events := runAcd(t, ctx, env, "history", "activity", "--repo", repo, "--json")
	if events.ExitCode != 0 {
		t.Fatalf("acd events exit=%d\nstdout=%s\nstderr=%s", events.ExitCode, events.Stdout, events.Stderr)
	}
	if !strings.Contains(events.Stdout, "Decision ledger is not available") {
		t.Fatalf("events did not report missing decision ledger:\n%s", events.Stdout)
	}
	explain := runAcd(t, ctx, env, "history", "explain", "--repo", repo, "--json")
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

	f := newFinishFixture(t)
	repo, env := f.Repo, f.Env

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if out := f.Run(t, ctx, "off", "--json"); out.ExitCode != 0 {
		t.Fatalf("off: %+v", out)
	}

	dbPath := filepath.Join(repo, ".git", "acd", "state.db")
	sqliteExec(t, dbPath, fmt.Sprintf("INSERT INTO decision_records(decision_ts,kind,path) VALUES (%f,'protected','earlier.txt')", nowFloatSeconds()))
	cursor := sqliteScalar(t, dbPath, "SELECT COALESCE(MAX(id), 0) FROM decision_records")
	rejected := runAcd(t, ctx, env,
		"history", "activity", "--repo", repo, "--watch", "--since", cursor, "--json")
	if rejected.ExitCode != 2 || !strings.Contains(rejected.Stdout, "--watch does not support --json") {
		t.Fatalf("events --watch --json exit=%d, want invalid input\nstdout=%s\nstderr=%s",
			rejected.ExitCode, rejected.Stdout, rejected.Stderr)
	}

	watchCtx, stopWatch := context.WithCancel(ctx)
	defer stopWatch()
	cmd := exec.CommandContext(watchCtx, buildAcdBinary(t),
		"history", "activity", "--repo", repo, "--watch", "--interval", "50ms", "--since", cursor)
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

	if out, err := exec.Command("sqlite3", dbPath, fmt.Sprintf(`
INSERT INTO decision_records(decision_ts, kind, path, reason, action_taken, user_message)
VALUES (%f, 'blocked', 'watch.txt', 'before-state mismatch', 'blocked_conflict', 'watch observed this decision');
`, nowFloatSeconds())).CombinedOutput(); err != nil {
		t.Fatalf("insert watched decision: %v\n%s", err, out)
	}

	var watched strings.Builder
	deadline := time.After(5 * time.Second)
	for {
		select {
		case line := <-lines:
			watched.WriteString(line + "\n")
			if strings.Contains(watched.String(), `Path: "watch.txt"`) && strings.Contains(watched.String(), "Action: blocked conflict") && strings.Contains(watched.String(), "watch observed this decision") {
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

func fileSHA256(t *testing.T, path string) string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return fmt.Sprintf("%x", sha256.Sum256(data))
}
