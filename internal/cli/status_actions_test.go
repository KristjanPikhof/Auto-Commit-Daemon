package cli

import (
	"bytes"
	"strings"
	"testing"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/daemon"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

func TestStatusOldFailedFinishDoesNotRequireRetryAfterCommit(t *testing.T) {
	for _, scanning := range []bool{false, true} {
		t.Run(map[bool]string{false: "saved", true: "scanning"}[scanning], func(t *testing.T) {
			rec, db, worker := finishStatusFixture(t)
			finishStatusWrite(t, rec.Path, "version.txt", "174\n")
			request, _, err := worker.Start(t.Context(), daemon.FinishOptions{Purpose: "Update release version"})
			if err != nil {
				t.Fatal(err)
			}
			_, err = state.UpdateFinishRequest(t.Context(), db, request.ID, request.Attempt, state.FinishRunning, state.FinishUpdate{
				State: state.FinishFailed, ReasonCode: "invalid_provider_plan", Reason: "invalid_provider_plan: split boundary rejected",
			})
			if err != nil {
				t.Fatal(err)
			}
			// The user commits a newer version instead of retrying the old target.
			finishStatusWrite(t, rec.Path, "version.txt", "175\n")
			finishStatusGit(t, rec.Path, "add", "version.txt")
			finishStatusGit(t, rec.Path, "commit", "-m", "Update release version")
			finishStatusCheckpoint(t, rec, db)
			if scanning {
				if _, err := daemon.BeginProtectionObservation(t.Context(), db); err != nil {
					t.Fatal(err)
				}
			}
			report := finishStatusRead(t, rec, db)
			control := controlResult{Enabled: true}
			applyControlStatusWithDaemonAlive(&control, report, true)
			envelope := envelopeFromControl(control)
			if envelope.NextAction != nil || envelope.State == productStateNeedsAction || report.PublicationOutcome.LaterChanges != 0 {
				t.Fatalf("obsolete finish still actionable: %+v; outcome=%+v", envelope, report.PublicationOutcome)
			}
			if report.PublicationOutcome.Finish == nil || report.PublicationOutcome.Finish.RequestID != request.ID {
				t.Fatal("diagnostic history was discarded")
			}
			var out bytes.Buffer
			if err := renderProductEnvelope(&out, envelope, false); err != nil {
				t.Fatal(err)
			}
			for _, unwanted := range []string{"retry", request.ID, "invalid_provider_plan", "Later changes:", "Failed:"} {
				if strings.Contains(out.String(), unwanted) {
					t.Fatalf("normal output includes %q:\n%s", unwanted, out.String())
				}
			}
			if !strings.Contains(out.String(), "No action needed.") || !strings.Contains(out.String(), "committed") {
				t.Fatalf("current state missing:\n%s", out.String())
			}
			if lines := strings.Count(out.String(), "\n"); lines > 5 {
				t.Fatalf("clean status needs %d lines:\n%s", lines, out.String())
			}
			entry := productListEntry{State: envelope.State, PublicationOutcome: report.PublicationOutcome, OperationalState: report.OperationalState}
			if strings.Contains(productListStatus(entry), "failed") {
				t.Fatal("list presents a historical failure as current")
			}
			finishStatusAssertViews(t, rec, db, report)
		})
	}
}

func TestStatusDistinguishesOptionalAndRequiredActions(t *testing.T) {
	for _, required := range []bool{false, true} {
		t.Run(map[bool]string{false: "optional", true: "required"}[required], func(t *testing.T) {
			health := controlHealthHealthy
			if required {
				health = controlHealthNeedsAttention
			}
			var out bytes.Buffer
			err := renderProductEnvelope(&out, envelopeFromControl(controlResult{
				Enabled: true, Health: health, NextAction: "Run `acd doctor`.", Summary: "Some files could not be saved.",
			}), false)
			if err != nil {
				t.Fatal(err)
			}
			want, unwanted := "Optional:", "Action required:"
			if required {
				want, unwanted = unwanted, want
			}
			if !strings.Contains(out.String(), want) || strings.Contains(out.String(), unwanted) {
				t.Fatalf("wrong action priority:\n%s", out.String())
			}
		})
	}
}

func TestStatusFailedRequestKeepsLaterWorkInDiagnostics(t *testing.T) {
	rec, db, worker := finishStatusFixture(t)
	finishStatusWrite(t, rec.Path, "version.txt", "174\n")
	request, _, err := worker.Start(t.Context(), daemon.FinishOptions{Purpose: "Update release version"})
	if err != nil {
		t.Fatal(err)
	}
	_, err = state.UpdateFinishRequest(t.Context(), db, request.ID, request.Attempt, state.FinishRunning, state.FinishUpdate{
		State: state.FinishFailed, ReasonCode: "invalid_provider_plan", Reason: "Plan rejected",
	})
	if err != nil {
		t.Fatal(err)
	}
	finishStatusWrite(t, rec.Path, "later.txt", "new work\n")
	finishStatusCheckpoint(t, rec, db)
	report := finishStatusRead(t, rec, db)
	if report.PublicationOutcome.LaterChanges != 1 || report.PublicationOutcome.PendingChanges != 2 {
		t.Fatalf("lost saved or later changes: %+v", report.PublicationOutcome)
	}
	var control controlResult
	applyFinishControlStatus(&control, report, true)
	if !strings.Contains(control.NextAction, "acd finish") || strings.Contains(control.NextAction, request.ID) || control.Health == controlHealthNeedsAttention {
		t.Fatalf("current work should offer an optional fresh finish: %+v", control)
	}
}

func TestStatusAutomaticRetryNeedsNoCommand(t *testing.T) {
	report := statusReport{Daemon: "running", CaptureHealth: state.CaptureHealth{State: "retrying", Error: "temporary read failure"}}
	var control controlResult
	applyFinishControlStatus(&control, report, true)
	envelope := envelopeFromControl(control)
	if envelope.NextAction != nil || envelope.State == productStateNeedsAction || !strings.Contains(control.Summary, "automatically") {
		t.Fatalf("automatic retry became a user task: %+v", envelope)
	}
}

func TestStatusCommittedWorkDoesNotHideInterruptedPublication(t *testing.T) {
	committed := true
	report := statusReport{Daemon: "running", Protected: true, PublicationOutcome: publicationOutcome{
		CheckpointAvailable: true, CountsKnown: true, BranchCommitted: &committed,
		Finish: &finishStatusReport{State: state.FinishNeedsAttention, Published: true, RequestID: "interrupted", Reason: "index reconciliation needs review"},
	}}
	var control controlResult
	applyFinishControlStatus(&control, report, true)
	envelope := envelopeFromControl(control)
	if envelope.State != productStateNeedsAction || envelope.NextAction == nil || !strings.Contains(*envelope.NextAction, "recover --force --dry-run") {
		t.Fatalf("real blocker hidden: %+v", envelope)
	}
}

func TestStatusFailedFinishDoesNotHideHistoryAction(t *testing.T) {
	report := statusReport{Daemon: "running", Protected: true, PublicationOutcome: publicationOutcome{
		CountsKnown: true,
		Finish:      &finishStatusReport{State: state.FinishFailed, RequestID: "old-failure"},
		History:     &finishHistoryStatusReport{State: "needs_attention", PlanID: "current-rewrite"},
	}}
	var control controlResult
	applyFinishControlStatus(&control, report, true)
	envelope := envelopeFromControl(control)
	if envelope.State != productStateNeedsAction || envelope.NextAction == nil || !strings.Contains(*envelope.NextAction, "history rewrite") {
		t.Fatalf("historical finish hid current history action: %+v", envelope)
	}
}
