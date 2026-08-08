//go:build integration
// +build integration

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

// TestSquashBacklogRecovery_PreservesSixtyCapturesAndControls proves the
// original failure shape at production boundaries: a feature branch leaves a
// blocked 60-event queue, its work is squash-merged to main, the feature ref is
// deleted, and a fresh daemon must reconcile the whole immutable pair before
// accepting main. Lifecycle off/on must not disturb the proof snapshot or ref.
func TestSquashBacklogRecovery_PreservesSixtyCapturesAndControls(t *testing.T) {
	requireSQLite(t)
	t.Parallel()

	repo := tempRepo(t)
	env := envWith(withIsolatedHome(t),
		"ACD_COMMIT_STRATEGY=event",
		"ACD_AI_PROVIDER=deterministic",
		"ACD_REWIND_GRACE_SECONDS=0",
	)
	t.Cleanup(func() { stopSessionForce(t, env, repo) })

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	const featureName = "feature/recovery-backlog"
	const featureRef = "refs/heads/" + featureName
	runGitOK(t, repo, "checkout", "-q", "-b", featureName)
	baseHead := strings.TrimSpace(runGitOK(t, repo, "rev-parse", "HEAD"))
	dbPath := initStateDBSchema(t, ctx, env, repo, "squash-backlog-bootstrap")
	generation := sqliteScalar(t, dbPath,
		"SELECT value FROM daemon_meta WHERE key = 'branch.generation'")
	if generation == "" {
		generation = "1"
	}

	type capturedFile struct {
		path string
		oid  string
	}
	paths := make([]string, 0, 60)
	var seed strings.Builder
	seed.WriteString("BEGIN;\n")
	for i := 0; i < 60; i++ {
		rel := fmt.Sprintf("backlog/file-%02d.txt", i)
		body := fmt.Sprintf("squash backlog payload %02d\n", i)
		writeFile(t, filepath.Join(repo, rel), body)
		paths = append(paths, rel)
	}
	oids := strings.Fields(runGitOK(t, repo,
		append([]string{"hash-object", "-w", "--"}, paths...)...))
	if len(oids) != len(paths) {
		t.Fatalf("batched hash-object returned %d oids want %d", len(oids), len(paths))
	}
	files := make([]capturedFile, 0, len(paths))
	for i, rel := range paths {
		oid := oids[i]
		files = append(files, capturedFile{path: rel, oid: oid})
		eventState := "pending"
		errorValue := "NULL"
		if i == 0 {
			eventState = "blocked_conflict"
			errorValue = "'integration squash backlog barrier'"
		}
		fmt.Fprintf(&seed, `
INSERT INTO capture_events(
    branch_ref, branch_generation, base_head, operation, path,
    fidelity, captured_ts, state, error
) VALUES ('%s', %s, '%s', 'create', '%s', 'exact', %.6f, '%s', %s);
INSERT INTO capture_ops(event_seq, ord, op, path, after_oid, after_mode, fidelity)
VALUES (last_insert_rowid(), 0, 'create', '%s', '%s', '100644', 'exact');
`, featureRef, generation, baseHead, rel,
			nowFloatSeconds()+float64(i)/1000, eventState, errorValue,
			rel, oid)
	}
	seed.WriteString("COMMIT;\n")
	if out := sqliteExec(t, dbPath, seed.String()); strings.TrimSpace(out) != "" {
		t.Fatalf("seed 60-event feature backlog returned output: %s", out)
	}
	firstSeq := sqliteScalar(t, dbPath,
		"SELECT MIN(seq) FROM capture_events WHERE branch_ref = '"+featureRef+"' AND branch_generation = "+generation)
	lastSeq := sqliteScalar(t, dbPath,
		"SELECT MAX(seq) FROM capture_events WHERE branch_ref = '"+featureRef+"' AND branch_generation = "+generation)
	if firstSeq == "" || lastSeq == "" {
		t.Fatal("seeded backlog seq range is empty")
	}

	runGitOK(t, repo, "add", "backlog")
	runGitOK(t, repo, "commit", "-q", "-m", "build feature backlog")
	runGitOK(t, repo, "checkout", "-q", "main")
	runGitOK(t, repo, "merge", "--squash", featureName)
	runGitOK(t, repo, "commit", "-q", "-m", "squash feature backlog")
	runGitOK(t, repo, "branch", "-D", featureName)
	if dirty := strings.TrimSpace(runGitOK(t, repo, "status", "--porcelain=v1")); dirty != "" {
		t.Fatalf("worktree dirty before daemon startup:\n%s", dirty)
	}

	startSession(t, ctx, env, repo, "squash-backlog-recovery", "shell")
	waitMode(t, repo, "running", 5*time.Second)
	waitFor(t, "60-event squash backlog reconciliation", 30*time.Second, func() bool {
		wakeSession(t, ctx, env, repo, "squash-backlog-recovery")
		got := sqliteScalar(t, dbPath, fmt.Sprintf(`
SELECT
  (SELECT COUNT(*) FROM capture_events
   WHERE seq BETWEEN %s AND %s AND state IN ('published','recovered')) || '|' ||
  (SELECT COUNT(*) FROM capture_events
   WHERE seq BETWEEN %s AND %s AND state IN ('pending','blocked_conflict','failed')) || '|' ||
  (SELECT COUNT(*) FROM recovery_snapshots
   WHERE first_event_seq = %s AND last_event_seq = %s AND event_count = 60)`,
			firstSeq, lastSeq, firstSeq, lastSeq, firstSeq, lastSeq))
		return got == "60|0|1"
	})

	snapshotID := sqliteScalar(t, dbPath, fmt.Sprintf(`
SELECT id FROM recovery_snapshots
WHERE first_event_seq = %s AND last_event_seq = %s AND event_count = 60`, firstSeq, lastSeq))
	if snapshotID == "" {
		t.Fatal("60-event recovery snapshot is missing")
	}
	snapshotSig := sqliteScalar(t, dbPath, fmt.Sprintf(`
SELECT outcome || '|' || branch_ref || '|' || branch_generation || '|' ||
       first_event_seq || '|' || last_event_seq || '|' || event_count || '|' ||
       commit_oid || '|' || recovery_ref
FROM recovery_snapshots WHERE id = %s`, snapshotID))
	recoveryRef := sqliteScalar(t, dbPath,
		fmt.Sprintf("SELECT recovery_ref FROM recovery_snapshots WHERE id = %s", snapshotID))
	recoveryCommit := sqliteScalar(t, dbPath,
		fmt.Sprintf("SELECT commit_oid FROM recovery_snapshots WHERE id = %s", snapshotID))
	if !strings.HasPrefix(recoveryRef, "refs/acd/recovery/") {
		t.Fatalf("recovery ref=%q", recoveryRef)
	}
	if got := strings.TrimSpace(runGitOK(t, repo, "show-ref", "--hash", "--verify", recoveryRef)); got != recoveryCommit {
		t.Fatalf("recovery ref resolves to %q want %q", got, recoveryCommit)
	}
	if got := sqliteScalar(t, dbPath, fmt.Sprintf(`
SELECT COUNT(*) FROM recovery_snapshot_events WHERE snapshot_id = %s`, snapshotID)); got != "60" {
		t.Fatalf("snapshot membership=%s want 60", got)
	}
	if got := sqliteScalar(t, dbPath, fmt.Sprintf(`
SELECT COUNT(*) FROM capture_events
WHERE seq BETWEEN %s AND %s
  AND branch_ref = '%s' AND branch_generation = %s AND base_head = '%s'`,
		firstSeq, lastSeq, featureRef, generation, baseHead)); got != "60" {
		t.Fatalf("immutable feature provenance rows=%s want 60", got)
	}
	headOIDs := treeBlobOIDs(t, repo, "HEAD", "backlog")
	recoveryOIDs := treeBlobOIDs(t, repo, recoveryRef, "backlog")
	for _, file := range files {
		headOID := headOIDs[file.path]
		if headOID == file.oid {
			continue
		}
		archiveOID := recoveryOIDs[file.path]
		if archiveOID != file.oid {
			t.Fatalf("captured blob for %s unreachable: HEAD=%s recovery=%s want=%s",
				file.path, headOID, archiveOID, file.oid)
		}
	}
	assertSquashBacklogControlHealth(t, ctx, env, repo)

	off := runAcd(t, ctx, env, "off", "--repo", repo, "--json")
	if off.ExitCode != 0 {
		t.Fatalf("acd off exit=%d\nstdout=%s\nstderr=%s", off.ExitCode, off.Stdout, off.Stderr)
	}
	if got := sqliteScalar(t, dbPath, fmt.Sprintf(`
SELECT outcome || '|' || branch_ref || '|' || branch_generation || '|' ||
       first_event_seq || '|' || last_event_seq || '|' || event_count || '|' ||
       commit_oid || '|' || recovery_ref
FROM recovery_snapshots WHERE id = %s`, snapshotID)); got != snapshotSig {
		t.Fatalf("snapshot changed across acd off:\nbefore=%s\nafter=%s", snapshotSig, got)
	}
	if got := strings.TrimSpace(runGitOK(t, repo, "show-ref", "--hash", "--verify", recoveryRef)); got != recoveryCommit {
		t.Fatalf("recovery ref changed across acd off: %q", got)
	}

	on := runAcd(t, ctx, env, "on", "--repo", repo, "--json")
	if on.ExitCode != 0 {
		t.Fatalf("acd on exit=%d\nstdout=%s\nstderr=%s", on.ExitCode, on.Stdout, on.Stderr)
	}
	waitMode(t, repo, "running", 10*time.Second)
	assertSquashBacklogControlHealth(t, ctx, env, repo)
	if got := sqliteScalar(t, dbPath, fmt.Sprintf(`
SELECT outcome || '|' || branch_ref || '|' || branch_generation || '|' ||
       first_event_seq || '|' || last_event_seq || '|' || event_count || '|' ||
       commit_oid || '|' || recovery_ref
FROM recovery_snapshots WHERE id = %s`, snapshotID)); got != snapshotSig {
		t.Fatalf("snapshot changed across acd on:\nbefore=%s\nafter=%s", snapshotSig, got)
	}
}

func treeBlobOIDs(t *testing.T, repo, tree, path string) map[string]string {
	t.Helper()
	out := runGitOK(t, repo, "ls-tree", "-r", "--full-tree", tree, "--", path)
	oids := make(map[string]string)
	for _, line := range strings.Split(strings.TrimSpace(out), "\n") {
		if line == "" {
			continue
		}
		meta, name, ok := strings.Cut(line, "\t")
		if !ok {
			t.Fatalf("parse ls-tree line %q", line)
		}
		fields := strings.Fields(meta)
		if len(fields) != 3 || fields[1] != "blob" {
			t.Fatalf("unexpected ls-tree entry %q", line)
		}
		oids[name] = fields[2]
	}
	return oids
}

func assertSquashBacklogControlHealth(t *testing.T, ctx context.Context, env []string, repo string) {
	t.Helper()
	type statusEnvelope struct {
		State string `json:"state"`
		Data  struct {
			Enabled       bool   `json:"enabled"`
			Protected     bool   `json:"protected"`
			Worker        string `json:"worker"`
			PendingEvents int    `json:"pending_events"`
			BlockedEvents int    `json:"blocked_events"`
		} `json:"data"`
	}
	var last ExecResult
	var health statusEnvelope
	waitFor(t, "protected squash backlog control health", 10*time.Second, func() bool {
		last = runAcd(t, ctx, env, "--repo", repo, "--json")
		if last.ExitCode != 0 || json.Unmarshal([]byte(last.Stdout), &health) != nil {
			return false
		}
		return health.Data.Enabled && health.Data.Protected && health.Data.Worker == "running" &&
			health.Data.PendingEvents == 0 && health.Data.BlockedEvents == 0
	})
	if health.State != "protected" {
		t.Fatalf("bare acd state=%q want protected\nstdout=%s\nstderr=%s",
			health.State, last.Stdout, last.Stderr)
	}
}

func sqliteExec(t *testing.T, dbPath, statement string) string {
	t.Helper()
	out, err := exec.Command("sqlite3", dbPath, statement).CombinedOutput()
	if err != nil {
		t.Fatalf("sqlite exec: %v\n%s", err, out)
	}
	return string(out)
}
