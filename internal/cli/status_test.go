package cli

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/daemon"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/testutil"
)

func TestStatus_StaleHeartbeatOverlay(t *testing.T) {
	roots := withIsolatedHome(t)
	ctx := context.Background()

	repo, db, d := makeRepoStateDB(t)
	registerRepo(t, roots, repo, db, "claude-code")

	stale := float64(time.Now().Add(-2 * time.Hour).Unix())
	if err := state.SaveDaemonState(ctx, d, state.DaemonState{
		PID: 1, Mode: "running", HeartbeatTS: stale,
	}); err != nil {
		t.Fatalf("save: %v", err)
	}

	var out bytes.Buffer
	if err := writeStatusProjectionFixture(ctx, &out, repo, true); err != nil {
		t.Fatal(err)
	}
	var report statusReport
	if err := json.Unmarshal(out.Bytes(), &report); err != nil {
		t.Fatal(err)
	}
	if !report.Stale || report.HeartbeatAgeSeconds < 2*60*60 {
		t.Fatalf("status report=%+v", report)
	}
}

func TestStatus_UnregisteredRepoErrors(t *testing.T) {
	_ = withIsolatedHome(t)
	ctx := context.Background()

	stranger := initCLIResolverRepo(t)
	var out bytes.Buffer
	err := writeStatusProjectionFixture(ctx, &out, stranger, false)
	if err == nil {
		t.Fatal("expected error for unregistered repo")
	}
	if !strings.Contains(err.Error(), "not registered") {
		t.Fatalf("error should mention 'not registered': %v", err)
	}
}

func TestStatus_SkipsDecisionSummaryForPreV5DB(t *testing.T) {
	roots := withIsolatedHome(t)
	ctx := context.Background()

	repo, dbPath, d := makeRepoStateDB(t)
	registerRepo(t, roots, repo, dbPath, "codex")
	if err := state.SaveDaemonState(ctx, d, state.DaemonState{
		PID: os.Getpid(), Mode: "running", HeartbeatTS: nowFloat(),
	}); err != nil {
		t.Fatalf("save state: %v", err)
	}
	if _, err := d.SQL().ExecContext(ctx, `DROP TABLE decision_records`); err != nil {
		t.Fatalf("drop decision_records: %v", err)
	}

	var out bytes.Buffer
	if err := writeStatusProjectionFixture(ctx, &out, repo, true); err != nil {
		t.Fatalf("writeStatusProjectionFixture should tolerate missing decision_records: %v\n%s", err, out.String())
	}
	var report statusReport
	if err := json.Unmarshal(out.Bytes(), &report); err != nil {
		t.Fatal(err)
	}
	if len(report.DecisionCounts) != 0 || len(report.RecentDecisions) != 0 {
		t.Fatalf("pre-v5 status unexpectedly has decisions: %+v", report)
	}
}

func TestStatus_JSONShape(t *testing.T) {
	roots := withIsolatedHome(t)
	ctx := context.Background()

	repo, db, d := makeRepoStateDB(t)
	registerRepo(t, roots, repo, db, "claude-code")
	if err := state.SaveDaemonState(ctx, d, state.DaemonState{
		PID: 7, Mode: "running", HeartbeatTS: nowFloat(),
	}); err != nil {
		t.Fatalf("save: %v", err)
	}

	var out bytes.Buffer
	if err := writeStatusProjectionFixture(ctx, &out, repo, true); err != nil {
		t.Fatalf("writeStatusProjectionFixture json: %v", err)
	}
	var rep statusReport
	if err := json.Unmarshal(out.Bytes(), &rep); err != nil {
		t.Fatalf("unmarshal: %v\n%s", err, out.String())
	}
	if rep.Repo != repo {
		t.Fatalf("repo = %q, want %q", rep.Repo, repo)
	}
	if rep.PID != 7 {
		t.Fatalf("pid = %d, want 7", rep.PID)
	}
	if rep.Daemon != "running" {
		t.Fatalf("daemon = %q, want running", rep.Daemon)
	}
}

func appendIntentPendingEvent(t *testing.T, ctx context.Context, d *state.DB, path string, capturedTS float64) int64 {
	t.Helper()
	seq, err := testutil.SeedLegacyCapture(ctx, d, state.CaptureEvent{
		BranchRef: "refs/heads/main", BranchGeneration: 1,
		BaseHead: "deadbeef", Operation: "modify", Path: path,
		Fidelity: "exact", CapturedTS: capturedTS,
	}, []state.CaptureOp{{Op: "modify", Path: path, Fidelity: "exact"}})
	if err != nil {
		t.Fatalf("AppendCaptureEvent: %v", err)
	}
	return seq
}

func TestFinishStatusHistoricalClientsCannotOverridePublication(t *testing.T) {
	rec, db, w := finishStatusFixture(t)
	finishStatusWrite(t, rec.Path, "goal.txt", "finished goal")
	request, _, err := w.Start(t.Context(), daemon.FinishOptions{Purpose: "Complete goal"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Execute(t.Context(), request.ID); err != nil {
		t.Fatal(err)
	}
	head := finishStatusGit(t, rec.Path, "rev-parse", "HEAD")
	now := nowFloat()
	for i, client := range []testutil.LegacyClient{{SessionID: "old-a", Harness: "claude-code", LastSeenTS: now}, {SessionID: "old-b", Harness: "pi", LastSeenTS: now - 14}} {
		if err := testutil.SeedLegacyClient(t.Context(), db, client); err != nil {
			t.Fatalf("client%d: %v", i, err)
		}
	}
	seq, err := testutil.SeedLegacyCapture(t.Context(), db, state.CaptureEvent{BranchRef: "refs/heads/old", BaseHead: "old", Operation: "modify", Path: "old.txt", State: state.EventStatePublished, Fidelity: "exact"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := testutil.SeedLegacyCaptureOutcome(t.Context(), db, seq, state.EventStatePublished, sqlNullStr("obsolete-oid"), sql.NullString{}, sqlNullStr("Old publication"), now); err != nil {
		t.Fatal(err)
	}
	report := finishStatusRead(t, rec, db)
	if len(report.Clients) != 2 || report.Clients[0].SessionID != "old-a" || report.Clients[1].Harness != "pi" || report.LastCommitOID != head || report.PublicationOutcome.Finish == nil || !report.PublicationOutcome.Finish.Published {
		t.Fatalf("archive overrode exact publication: %+v", report)
	}
	finishStatusAssertViews(t, rec, db, report)
}
