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

// TestFix_ReconcilesWholeExactPairs pins the immutable recovery contract:
// exact HEAD matches publish the full pair, while explicit --force archives a
// non-matching pair without deleting or retargeting its captured rows.
func TestFix_ReconcilesWholeExactPairs(t *testing.T) {
	t.Parallel()
	requireSQLite(t)

	repo := tempRepo(t)
	env := finishLaunchctlEnv(t, withIsolatedHome(t))

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

	dbPath := initLegacySupportDB(t, ctx, env, repo)
	gen := sqliteScalar(t, dbPath, "SELECT value FROM daemon_meta WHERE key = 'branch.generation'")
	if gen == "" {
		gen = "1"
	}

	publishedPair := seedCreateRecoveryPair(t, dbPath, "refs/heads/main", gen, baseHead,
		"published-a.txt", publishedAOID, "published-b.txt", publishedBOID)
	apply := runAcd(t, ctx, env, "support", "recover", "--repo", repo, "--yes", "--json")
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
	forceDry := runAcd(t, ctx, env, "support", "recover", "--repo", repo, "--force", "--dry-run", "--json")
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

	forceApply := runAcd(t, ctx, env, "support", "recover", "--repo", repo, "--force", "--yes", "--json")
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

func TestFix_GeneratedPendingCleanupKeepsGitManual(t *testing.T) {
	t.Parallel()
	requireSQLite(t)

	repo := tempRepo(t)
	env := finishLaunchctlEnv(t, withIsolatedHome(t))

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

	dbPath := initLegacySupportDB(t, ctx, env, repo)
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

	diagnose := runAcd(t, ctx, env, "support", "diagnose", "--repo", repo, "--json")
	if diagnose.ExitCode != 0 {
		t.Fatalf("diagnose exit=%d\nstdout=%s\nstderr=%s", diagnose.ExitCode, diagnose.Stdout, diagnose.Stderr)
	}
	if !strings.Contains(diagnose.Stdout, `"state_db_checksum_verified": true`) || !strings.Contains(diagnose.Stdout, `"reason_code": "legacy_activation_required"`) {
		t.Fatalf("diagnose lost read-only retained legacy state: %s", diagnose.Stdout)
	}

	dryRun := runAcd(t, ctx, env, "support", "recover", "--repo", repo, "--dry-run", "--json")
	if dryRun.ExitCode != 0 {
		t.Fatalf("fix dry-run exit=%d\nstdout=%s\nstderr=%s", dryRun.ExitCode, dryRun.Stdout, dryRun.Stderr)
	}
	plan := decodeFixPlan(t, dryRun.Stdout)
	foundGenerated := false
	for _, action := range plan.Actions {
		if action.Kind == "drop_generated_pending" {
			foundGenerated = true
			if action.TrackedCount != 2 || action.PendingCount != 2 {
				t.Fatalf("generated preview lost native tracked paths or archived membership: %+v", action)
			}
		}
	}
	if !foundGenerated {
		t.Fatalf("recovery dry-run missing generated cleanup: %s", dryRun.Stdout)
	}

	apply := runAcd(t, ctx, env, "support", "recover", "--repo", repo, "--yes", "--json")
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
	TrackedCount     int    `json:"tracked_count"`
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
