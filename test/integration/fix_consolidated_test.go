//go:build integration
// +build integration

package integration_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// TestFix_BarrierWithSuccessorsRequiresForce pins the SPEC LOCK rule that
// purge_barrier_with_successors is gated behind --force. Without --force,
// even with --yes, the planner must NOT include the purge action and the
// CLI must refuse to delete a blocked barrier that has pending successors.
// With --force the dry-run plan must list the action; with --force --yes
// the row is deleted and publish_state cleared.
//
// Scenario shape mirrors the real Trekoon incident: blocked_conflict row at
// seq=N hides one or more pending captures at seq>N for the same anchor.
// HEAD does NOT match the captured after_oid, so resolve_already_landed_barrier
// is intentionally OUT of the plan.
func TestFix_BarrierWithSuccessorsRequiresForce(t *testing.T) {
	requireSQLite(t)

	repo := tempRepo(t)
	env := withIsolatedHome(t)
	t.Cleanup(func() { stopSessionForce(t, env, repo) })

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Bring schema up via a start/stop cycle so seed SQL has all tables and
	// register the repo in the central registry (acd fix looks it up there).
	dbPath := initStateDBSchema(t, ctx, env, repo, "fix-barrier-init")

	head := strings.TrimSpace(runGitOK(t, repo, "rev-parse", "HEAD"))
	gen := sqliteScalar(t, dbPath, "SELECT value FROM daemon_meta WHERE key = 'branch.generation'")
	if gen == "" {
		gen = "1"
	}

	// Captured after_oid is a content hash that does NOT exist anywhere in
	// HEAD's tree — guarantees alreadyPublishedAtHEAD returns false and so
	// resolve_already_landed_barrier is not eligible. The blocker is keyed
	// by an error string that does NOT classify as before_state_mismatch,
	// so the daemon's self-heal probe stays out of the picture even if a
	// daemon happened to be alive.
	bogusBefore := "1111111111111111111111111111111111111111"
	bogusAfter := "2222222222222222222222222222222222222222"
	successorAfter := gitHashObjectStdin(t, repo, "successor body\n")
	now := nowFloatSeconds()
	seedSQL := fmt.Sprintf(`
INSERT INTO capture_events(branch_ref, branch_generation, base_head, operation, path, fidelity, captured_ts, state, error)
VALUES ('refs/heads/main', %s, '%s', 'modify', 'barrier.txt', 'rescan', %f, 'blocked_conflict', 'cas_fail: ref moved during update');
INSERT INTO capture_ops(event_seq, ord, op, path, before_oid, before_mode, after_oid, after_mode, fidelity)
VALUES (last_insert_rowid(), 0, 'modify', 'barrier.txt', '%s', '100644', '%s', '100644', 'rescan');
INSERT INTO capture_events(branch_ref, branch_generation, base_head, operation, path, fidelity, captured_ts, state)
VALUES ('refs/heads/main', %s, '%s', 'create', 'successor.txt', 'exact', %f, 'pending');
INSERT INTO capture_ops(event_seq, ord, op, path, after_oid, after_mode, fidelity)
VALUES (last_insert_rowid(), 0, 'create', 'successor.txt', '%s', '100644', 'exact');
INSERT INTO publish_state(id, event_seq, branch_ref, branch_generation, source_head, target_commit_oid, status, error, updated_ts)
VALUES (1, NULL, 'refs/heads/main', %s, '%s', NULL, 'blocked_conflict', 'cas_fail: ref moved', %f);
`, gen, head, now, bogusBefore, bogusAfter,
		gen, head, now+0.001, successorAfter,
		gen, head, now+0.002)
	if out, err := exec.Command("sqlite3", dbPath, seedSQL).CombinedOutput(); err != nil {
		t.Fatalf("seed barrier rows: %v\n%s", err, out)
	}
	barrierSeq := sqliteScalar(t, dbPath,
		"SELECT seq FROM capture_events WHERE path = 'barrier.txt' ORDER BY seq DESC LIMIT 1")
	if barrierSeq == "" {
		t.Fatalf("seeded barrier seq missing")
	}

	// --dry-run WITHOUT --force: plan must NOT include purge_barrier_with_successors
	// and the JSON suggestions block must nudge the operator toward --force.
	dryRun := runAcd(t, ctx, env, "fix", "--repo", repo, "--dry-run", "--json")
	if dryRun.ExitCode != 0 {
		t.Fatalf("fix --dry-run exit=%d\nstdout=%s\nstderr=%s", dryRun.ExitCode, dryRun.Stdout, dryRun.Stderr)
	}
	plan := decodeFixPlan(t, dryRun.Stdout)
	if !plan.DryRun {
		t.Fatalf("plan.dry_run=false on --dry-run invocation\n%s", dryRun.Stdout)
	}
	if hasFixActionKind(plan.Actions, "purge_barrier_with_successors") {
		t.Fatalf("--dry-run without --force planned purge_barrier_with_successors\n%s", dryRun.Stdout)
	}
	if hasFixActionKind(plan.Actions, "resolve_already_landed_barrier") {
		t.Fatalf("--dry-run planned resolve_already_landed_barrier; HEAD must not match captured after_oid\n%s", dryRun.Stdout)
	}
	if !suggestionsMentionForce(plan.Suggestions) {
		t.Fatalf("--dry-run suggestions did not nudge operator toward --force:\n%v", plan.Suggestions)
	}

	// --yes WITHOUT --force: must refuse to mint the destructive purge.
	// The plan rendered must still omit purge_barrier_with_successors and
	// the seeded blocked row must survive.
	yesNoForce := runAcd(t, ctx, env, "fix", "--repo", repo, "--yes", "--json")
	// `acd fix --yes` may exit 0 (no qualifying safe actions) or non-zero
	// (refused due to a daemon-alive unsafe reason). Either way, no
	// destructive purge has been applied and the blocked row must still be
	// present at its seeded seq.
	if remaining := sqliteScalar(t, dbPath,
		fmt.Sprintf("SELECT state FROM capture_events WHERE seq = %s", barrierSeq)); remaining != "blocked_conflict" {
		t.Fatalf("--yes without --force mutated blocked row to state=%q (seq=%s)\nstdout=%s\nstderr=%s",
			remaining, barrierSeq, yesNoForce.Stdout, yesNoForce.Stderr)
	}
	yesPlan := decodeFixPlan(t, yesNoForce.Stdout)
	if hasFixActionKind(yesPlan.Actions, "purge_barrier_with_successors") {
		// Even if Applied=false, the kind being in the plan would indicate
		// the planner ignored the --force gate.
		t.Fatalf("--yes without --force planned purge_barrier_with_successors:\n%s", yesNoForce.Stdout)
	}

	// --force --dry-run: plan now includes the purge action keyed at the
	// blocked seq. Still no mutation.
	forceDry := runAcd(t, ctx, env, "fix", "--repo", repo, "--force", "--dry-run", "--json")
	if forceDry.ExitCode != 0 {
		t.Fatalf("fix --force --dry-run exit=%d\nstdout=%s\nstderr=%s", forceDry.ExitCode, forceDry.Stdout, forceDry.Stderr)
	}
	forcePlan := decodeFixPlan(t, forceDry.Stdout)
	if !forcePlan.DryRun {
		t.Fatalf("--force --dry-run did not flag dry_run=true\n%s", forceDry.Stdout)
	}
	if !hasFixActionKindForSeq(forcePlan.Actions, "purge_barrier_with_successors", barrierSeq) {
		t.Fatalf("--force --dry-run plan missing purge_barrier_with_successors for seq=%s\n%s",
			barrierSeq, forceDry.Stdout)
	}
	if rem := sqliteScalar(t, dbPath,
		fmt.Sprintf("SELECT state FROM capture_events WHERE seq = %s", barrierSeq)); rem != "blocked_conflict" {
		t.Fatalf("--force --dry-run mutated state to %q", rem)
	}

	// --force --yes: row is deleted, publish_state singleton flips to ok.
	forceApply := runAcd(t, ctx, env, "fix", "--repo", repo, "--force", "--yes", "--json")
	if forceApply.ExitCode != 0 {
		t.Fatalf("fix --force --yes exit=%d\nstdout=%s\nstderr=%s",
			forceApply.ExitCode, forceApply.Stdout, forceApply.Stderr)
	}
	if rem := sqliteScalar(t, dbPath,
		fmt.Sprintf("SELECT COUNT(*) FROM capture_events WHERE seq = %s", barrierSeq)); rem != "0" {
		t.Fatalf("blocked seq %s still present after --force --yes\n%s", barrierSeq, forceApply.Stdout)
	}
	if pubStatus := sqliteScalar(t, dbPath, "SELECT status FROM publish_state WHERE id = 1"); pubStatus != "ok" {
		dump, _ := exec.Command("sqlite3", dbPath,
			"SELECT id,status,event_seq,error FROM publish_state").CombinedOutput()
		t.Fatalf("publish_state.status=%q want ok after purge\nrows:\n%s", pubStatus, dump)
	}
	// Successor must remain pending — purge only removes the blocked row.
	if rem := sqliteScalar(t, dbPath,
		"SELECT state FROM capture_events WHERE path = 'successor.txt' ORDER BY seq DESC LIMIT 1"); rem != "pending" {
		t.Fatalf("successor row state=%q want pending", rem)
	}
}

// TestRecoverAndPurgeDeprecationWarnings asserts the legacy `acd recover`
// and `acd purge-events` entrypoints still work for one release while
// emitting the documented deprecation stderr line. Both must forward to the
// new acd fix paths so existing scripts keep functioning.
func TestRecoverAndPurgeDeprecationWarnings(t *testing.T) {
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

	// Seed a stale-anchor row: branch_ref/generation differs from current
	// HEAD so acd recover --auto retargets it back onto refs/heads/main.
	staleAfter := gitHashObjectStdin(t, repo, "stale anchor body\n")
	now := nowFloatSeconds()
	staleSQL := fmt.Sprintf(`
INSERT INTO capture_events(branch_ref, branch_generation, base_head, operation, path, fidelity, captured_ts, state, error)
VALUES ('refs/heads/stale', 99, '%s', 'create', 'stale.txt', 'exact', %f, 'blocked_conflict', 'old anchor');
INSERT INTO capture_ops(event_seq, ord, op, path, after_oid, after_mode, fidelity)
VALUES (last_insert_rowid(), 0, 'create', 'stale.txt', '%s', '100644', 'exact');
`, head, now, staleAfter)
	if out, err := exec.Command("sqlite3", dbPath, staleSQL).CombinedOutput(); err != nil {
		t.Fatalf("seed stale anchor: %v\n%s", err, out)
	}
	staleSeq := sqliteScalar(t, dbPath,
		"SELECT seq FROM capture_events WHERE path = 'stale.txt' ORDER BY seq DESC LIMIT 1")

	// Deprecated path: acd recover --auto --yes.
	recover := runAcd(t, ctx, env, "recover", "--repo", repo, "--auto", "--yes", "--json")
	if recover.ExitCode != 0 {
		t.Fatalf("acd recover --auto --yes exit=%d\nstdout=%s\nstderr=%s",
			recover.ExitCode, recover.Stdout, recover.Stderr)
	}
	const wantRecoverDeprec = "acd recover is deprecated; use acd fix [--clear-pause]. See acd fix --help."
	if !strings.Contains(recover.Stderr, wantRecoverDeprec) {
		t.Fatalf("recover stderr missing deprecation banner\nwant: %q\nstderr: %q",
			wantRecoverDeprec, recover.Stderr)
	}
	// Retarget must still complete — branch_ref/generation flip back to
	// the current main/gen anchor on the stale row.
	got := sqliteScalar(t, dbPath,
		fmt.Sprintf("SELECT branch_ref || '|' || branch_generation || '|' || state FROM capture_events WHERE seq = %s", staleSeq))
	wantPrefix := "refs/heads/main|"
	if !strings.HasPrefix(got, wantPrefix) {
		t.Fatalf("stale row after recover=%q want prefix %q", got, wantPrefix)
	}
	if !strings.HasSuffix(got, "|pending") {
		t.Fatalf("stale row after recover=%q want suffix |pending (blocked rows reset)", got)
	}

	// Seed a fresh blocked row so the purge-events alias has work to do.
	blockedSeed := fmt.Sprintf(`
INSERT INTO capture_events(branch_ref, branch_generation, base_head, operation, path, fidelity, captured_ts, state, error)
VALUES ('refs/heads/main', %s, '%s', 'modify', 'purge-me.txt', 'rescan', %f, 'blocked_conflict', 'cas_fail');
INSERT INTO capture_ops(event_seq, ord, op, path, before_oid, before_mode, after_oid, after_mode, fidelity)
VALUES (last_insert_rowid(), 0, 'modify', 'purge-me.txt', '1111111111111111111111111111111111111111', '100644', '2222222222222222222222222222222222222222', '100644', 'rescan');
`, gen, head, now+1)
	if out, err := exec.Command("sqlite3", dbPath, blockedSeed).CombinedOutput(); err != nil {
		t.Fatalf("seed purge target: %v\n%s", err, out)
	}
	purgeSeq := sqliteScalar(t, dbPath,
		"SELECT seq FROM capture_events WHERE path = 'purge-me.txt' ORDER BY seq DESC LIMIT 1")
	if purgeSeq == "" {
		t.Fatalf("purge target seq missing")
	}

	// Deprecated path: acd purge-events --blocked --yes.
	purge := runAcd(t, ctx, env, "purge-events", "--repo", repo, "--blocked", "--yes", "--json")
	if purge.ExitCode != 0 {
		t.Fatalf("acd purge-events --blocked --yes exit=%d\nstdout=%s\nstderr=%s",
			purge.ExitCode, purge.Stdout, purge.Stderr)
	}
	const wantPurgeDeprec = "acd purge-events is deprecated; use acd fix --force [--yes]. See acd fix --help."
	if !strings.Contains(purge.Stderr, wantPurgeDeprec) {
		t.Fatalf("purge-events stderr missing deprecation banner\nwant: %q\nstderr: %q",
			wantPurgeDeprec, purge.Stderr)
	}
	// Purge must still complete — the seeded blocked row is gone.
	if cnt := sqliteScalar(t, dbPath,
		fmt.Sprintf("SELECT COUNT(*) FROM capture_events WHERE seq = %s", purgeSeq)); cnt != "0" {
		t.Fatalf("purge-events left blocked seq %s in DB (count=%s)", purgeSeq, cnt)
	}
}

func TestFix_GeneratedPendingCleanupKeepsGitManual(t *testing.T) {
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
VALUES (last_insert_rowid(), 0, 'delete', '.derivedData-provider-core/Index.noindex/a.db', '1111111111111111111111111111111111111111', '100644', 'rescan');
INSERT INTO planner_state(event_seq, defer_count, last_planned_ts)
VALUES ((SELECT seq FROM capture_events WHERE path = '.derivedData-provider-core/Index.noindex/a.db' ORDER BY seq DESC LIMIT 1), 0, %f);
INSERT INTO capture_events(branch_ref, branch_generation, base_head, operation, path, fidelity, captured_ts, state)
VALUES ('refs/heads/main', %s, '%s', 'delete', '.derivedData-provider-core/Index.noindex/b.db', 'rescan', %f, 'pending');
INSERT INTO capture_ops(event_seq, ord, op, path, before_oid, before_mode, fidelity)
VALUES (last_insert_rowid(), 0, 'delete', '.derivedData-provider-core/Index.noindex/b.db', '2222222222222222222222222222222222222222', '100644', 'rescan');
INSERT INTO planner_state(event_seq, defer_count, last_planned_ts)
VALUES ((SELECT seq FROM capture_events WHERE path = '.derivedData-provider-core/Index.noindex/b.db' ORDER BY seq DESC LIMIT 1), 0, %f);
INSERT INTO capture_events(branch_ref, branch_generation, base_head, operation, path, fidelity, captured_ts, state)
VALUES ('refs/heads/main', %s, '%s', 'delete', 'build/output.js', 'rescan', %f, 'pending');
`, gen, head, now, now,
		gen, head, now+0.001, now+0.001,
		gen, head, now+0.002)
	if out, err := exec.Command("sqlite3", dbPath, seedSQL).CombinedOutput(); err != nil {
		t.Fatalf("seed generated pending rows: %v\n%s", err, out)
	}

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
	Kind          string `json:"kind"`
	Seq           int64  `json:"seq"`
	Path          string `json:"path"`
	RequiresForce bool   `json:"requires_force"`
}

func decodeFixPlan(t *testing.T, body string) fixPlanProbe {
	t.Helper()
	var plan fixPlanProbe
	if strings.TrimSpace(body) == "" {
		return plan
	}
	if err := json.Unmarshal([]byte(body), &plan); err != nil {
		t.Fatalf("decode fix plan json: %v\nbody=%s", err, body)
	}
	return plan
}

func hasFixActionKind(actions []fixActionProbe, kind string) bool {
	for _, a := range actions {
		if a.Kind == kind {
			return true
		}
	}
	return false
}

func hasFixActionKindForSeq(actions []fixActionProbe, kind, seq string) bool {
	for _, a := range actions {
		if a.Kind != kind {
			continue
		}
		if fmt.Sprintf("%d", a.Seq) == seq {
			return true
		}
	}
	return false
}

func suggestionsMentionForce(suggestions []string) bool {
	for _, s := range suggestions {
		if strings.Contains(s, "--force") {
			return true
		}
	}
	return false
}
