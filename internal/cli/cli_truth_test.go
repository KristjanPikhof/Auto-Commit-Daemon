package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/testutil"
)

func TestStatusPublicationTruthSeparatesGitAndACD(t *testing.T) {
	rec, db, _ := finishStatusFixture(t)
	finishStatusWrite(t, rec.Path, "recovered.txt", "saved recovery")
	cp := finishStatusCheckpoint(t, rec, db)
	seq, err := testutil.SeedLegacyCapture(t.Context(), db, state.CaptureEvent{BranchRef: "refs/heads/main", BaseHead: finishStatusGit(t, rec.Path, "rev-parse", "HEAD"), Operation: "create", Path: "recovered.txt", State: state.EventStateRecovered, Fidelity: "exact"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.SQL().ExecContext(t.Context(), "INSERT INTO checkpoint_events(checkpoint_id,ord,event_seq) VALUES(?,0,?)", cp.CheckpointID, seq); err != nil {
		t.Fatal(err)
	}
	report := finishStatusRead(t, rec, db)
	if !report.Protected || !report.CheckpointPublishedByACD || report.PublicationOutcome.BranchCommitted == nil || *report.PublicationOutcome.BranchCommitted || report.PublicationOutcome.RecoveredChanges != 1 {
		t.Fatalf("saved recovery confused with branch commit: %+v", report.PublicationOutcome)
	}
	finishStatusAssertViews(t, rec, db, report)
	var shared, product bytes.Buffer
	renderFinishOutcomeHuman(&shared, report.PublicationOutcome)
	control := controlResult{Enabled: true}
	applyControlStatusWithDaemonAlive(&control, report, true)
	if err := renderProductEnvelope(&product, envelopeFromControl(control), false); err != nil {
		t.Fatal(err)
	}
	for name, body := range map[string]string{"shared": shared.String(), "status": product.String()} {
		if !strings.Contains(body, report.PublicationOutcome.Legacy.Reason) || !strings.Contains(body, "acd setup") || strings.Contains(body, "Legacy archive: 0") {
			t.Fatalf("%s hid unknown-count legacy attention: %s", name, body)
		}
	}
	finishStatusWrite(t, rec.Path, "later.txt", "later edit")
	report = finishStatusRead(t, rec, db)
	if report.WorktreeClean || report.AllChangesCommittedInGit || report.PublicationOutcome.WorktreeChanges != 2 || report.PublicationOutcome.PendingChanges != 1 || !report.CheckpointPublishedByACD {
		t.Fatalf("later work lost immutable checkpoint evidence: %+v", report)
	}
}

func TestStatusDiagnoseDoctorControlReadsStayReadOnly(t *testing.T) {
	roots := withIsolatedHome(t)
	ctx := context.Background()
	repo, dbPath, d := makeRepoStateDB(t)
	registerRepo(t, roots, repo, dbPath, "codex")
	if err := state.SaveDaemonState(ctx, d, state.DaemonState{
		PID: os.Getpid(), Mode: "running", HeartbeatTS: nowFloat(),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := d.SQL().ExecContext(ctx, `PRAGMA wal_checkpoint(TRUNCATE)`); err != nil {
		t.Fatal(err)
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	before, err := fileSHA256(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	rejectPath := filepath.Join(filepath.Dir(dbPath), "logs", "intent_planner_rejects.jsonl")
	for name, read := range map[string]func() error{
		"status":   func() error { return writeStatusProjectionFixture(ctx, &bytes.Buffer{}, repo, true) },
		"diagnose": func() error { return runDiagnose(ctx, &bytes.Buffer{}, repo, true) },
		"doctor":   func() error { return runDoctor(ctx, &bytes.Buffer{}, false, repo, true) },
		"control":  func() error { _, err := inspectControl(ctx, repo); return err },
	} {
		t.Run(name, func(t *testing.T) {
			if err := read(); err != nil {
				t.Fatal(err)
			}
		})
	}
	after, err := fileSHA256(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	if before != after {
		t.Fatalf("state.db checksum changed: %s != %s", before, after)
	}
	if _, err := os.Stat(rejectPath); !os.IsNotExist(err) {
		t.Fatalf("read path created reject log %s: %v", rejectPath, err)
	}
}

func TestStatusTruthJSONFieldsAreAdditive(t *testing.T) {
	body, err := json.Marshal(statusReport{
		OperationalState: "healthy_idle", WorktreeClean: true,
		AllChangesCommittedInGit: true, CheckpointPublishedByACD: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{
		`"operational_state":"healthy_idle"`, `"worktree_clean":true`,
		`"all_changes_committed_in_git":true`, `"checkpoint_published_by_acd":true`,
	} {
		if !bytes.Contains(body, []byte(field)) {
			t.Fatalf("JSON missing %s: %s", field, body)
		}
	}
}
