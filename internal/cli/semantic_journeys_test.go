package cli

import (
	"bytes"
	"strings"
	"testing"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/daemon"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/migration"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/testutil"
)

func TestRecoveryHistoryDoesNotHideCurrentPublicationActivity(t *testing.T) {
	record, db, worker := finishStatusFixture(t)
	finishStatusWrite(t, record.Path, "recovered.txt", "preserved work")
	checkpoint := finishStatusCheckpoint(t, record, db)
	seq, err := testutil.SeedLegacyCapture(t.Context(), db, state.CaptureEvent{
		BranchRef: "refs/heads/main", BaseHead: finishStatusGit(t, record.Path, "rev-parse", "HEAD"),
		Operation: "create", Path: "recovered.txt", State: state.EventStateRecovered, Fidelity: "exact",
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.SQL().ExecContext(t.Context(), "INSERT INTO checkpoint_events(checkpoint_id,ord,event_seq) VALUES(?,0,?)", checkpoint.CheckpointID, seq); err != nil {
		t.Fatal(err)
	}
	if _, err := migration.ActivateFinish(t.Context(), record.Path, db); err != nil {
		t.Fatal(err)
	}
	report := finishStatusRead(t, record, db)
	var preserved bytes.Buffer
	renderFinishOutcomeHuman(&preserved, report.PublicationOutcome)
	if report.PublicationOutcome.RecoveredChanges != 1 || !strings.Contains(preserved.String(), "Recovery: 1 legacy changes saved separately") {
		t.Fatalf("recovery preservation was hidden: %+v %s", report.PublicationOutcome, &preserved)
	}

	finishStatusWrite(t, record.Path, "goal.txt", "complete current goal")
	request, created, err := worker.Start(t.Context(), daemon.FinishOptions{Purpose: "Complete current goal"})
	if err != nil || !created {
		t.Fatalf("start finish: created=%v err=%v", created, err)
	}
	if _, err := state.UpdateFinishRequest(t.Context(), db, request.ID, request.Attempt, state.FinishRunning, state.FinishUpdate{
		State: state.FinishRunning, Stage: "checking",
	}); err != nil {
		t.Fatal(err)
	}
	report = finishStatusRead(t, record, db)
	entry := productListEntry{PublicationOutcome: report.PublicationOutcome, PublicationProgress: report.PublicationProgress, OperationalState: report.OperationalState}
	if !report.Busy || productListPhase(entry) != "checking" {
		t.Fatalf("recovery hid active verification: %+v phase=%s", entry, productListPhase(entry))
	}
	finishStatusAssertViews(t, record, db, report)
	if _, err := worker.Execute(t.Context(), request.ID); err != nil {
		t.Fatal(err)
	}
	report = finishStatusRead(t, record, db)
	entry.PublicationOutcome, entry.PublicationProgress, entry.OperationalState = report.PublicationOutcome, report.PublicationProgress, report.OperationalState
	if report.Busy || report.PublicationOutcome.Finish == nil || !report.PublicationOutcome.Finish.Published ||
		report.PublicationOutcome.Finish.NewHead != finishStatusGit(t, record.Path, "rev-parse", "HEAD") || productListPhase(entry) != "finish-completed" {
		t.Fatalf("historical recovery hid exact current publication: %+v phase=%s", entry, productListPhase(entry))
	}
	history, err := loadCheckpointHistory(t.Context(), record.Path)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, item := range history {
		if item.ID == checkpoint.CheckpointID {
			found = item.Outcome == "recovered" && item.PublicationOutcome.RecoveredChanges == 1
		}
	}
	if !found {
		t.Fatal("current publication lost the retained recovery checkpoint")
	}
	finishStatusAssertViews(t, record, db, report)
}

func TestRestoreWithoutIDDoesNotApplyOutsideTerminal(t *testing.T) {
	_, repo, dbPath := registeredProductMutationRepo(t)
	before := fileDigest(t, dbPath)
	cmd := newRootCmd()
	cmd.SetIn(strings.NewReader("1\ny\n"))
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{"restore", "--repo", repo})
	err := cmd.Execute()
	if err == nil || !strings.Contains(err.Error(), "acd history") {
		t.Fatalf("bare nonterminal restore: %v", err)
	}
	if fileDigest(t, dbPath) != before {
		t.Fatal("nonterminal restore changed state")
	}
}

func TestCompactStatusSeparatesAIWaitAndRecovery(t *testing.T) {
	committed := false
	var out bytes.Buffer
	err := renderProductEnvelope(&out, productEnvelope{State: productStateWaiting, Data: productStatusData{
		Enabled: true, Protected: true,
		PublicationOutcome: publicationOutcome{BranchCommitted: &committed, WaitingChanges: 3, RecoveredChanges: 2, ReasonCode: "provider_wait"},
	}}, false)
	if err != nil {
		t.Fatal(err)
	}
	for _, line := range []string{"Protection: on", "Last observed work saved: yes", "Branch commits: 3 changes waiting for AI", "Recovery: 2 changes saved separately", "Next: No action needed."} {
		if !strings.Contains(out.String(), line) {
			t.Fatalf("missing %q: %s", line, &out)
		}
	}
	if strings.Contains(out.String(), "Worker liveness") || strings.Contains(out.String(), "Published to Git") {
		t.Fatalf("default status contains obsolete/detail output: %s", &out)
	}
}
