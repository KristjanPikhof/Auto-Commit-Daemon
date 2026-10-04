package cli

import (
	"bytes"
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/central"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

func TestCaptureHealthTruthWithResponsiveWorkerAndEmptyQueue(t *testing.T) {
	ctx := context.Background()
	repo, dbPath, db := makeSeededRepoStateDB(t)
	now := time.Now()
	if err := state.SaveDaemonState(ctx, db, state.DaemonState{PID: os.Getpid(), Mode: "running", HeartbeatTS: float64(now.Unix()), UpdatedTS: float64(now.Unix())}); err != nil {
		t.Fatal(err)
	}
	if err := state.MetaSetMany(ctx, db, map[string]string{"last_capture_error": "eligible file unreadable", "protection.complete": "false"}); err != nil {
		t.Fatal(err)
	}
	rec := central.RepoRecord{Path: repo, StateDB: dbPath, Enabled: true}
	status, err := buildStatusReport(ctx, rec, now)
	if err != nil {
		t.Fatal(err)
	}
	if status.CaptureErrors < 1 || status.CaptureHealth.State != "blocked" {
		t.Fatalf("lost capture error: %+v", status.CaptureHealth)
	}
	control := controlResult{OK: true, Enabled: true, Registered: true}
	applyControlStatusWithDaemonAlive(&control, status, true)
	if control.OK || control.Protected || control.Health != controlHealthNeedsAttention || !strings.Contains(control.Summary, "incomplete") {
		t.Fatalf("status masks failure: %+v", control)
	}
	if envelope := envelopeFromControl(control); envelope.State != productStateNeedsAction {
		t.Fatalf("status envelope=%+v", envelope)
	}
	overview, err := readProductListRepo(ctx, rec, now)
	if err != nil {
		t.Fatal(err)
	}
	if overview.report.CaptureHealth.State != "blocked" {
		t.Fatalf("list lost capture health: %+v", overview.report.CaptureHealth)
	}
	plan, err := buildFixPlan(ctx, repo, dbPath, true, false, false)
	if err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	if err := renderFix(&output, plan, false); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(output.String(), "state is healthy") || strings.Contains(output.String(), "stop the shared runtime") || !strings.Contains(output.String(), "protection is incomplete") {
		t.Fatalf("recovery masks failure: %s", output.String())
	}
	output.Reset()
	if err := renderProductDiagnoseReport(&output, diagnoseReport{CaptureHealth: status.CaptureHealth}); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(output.String(), `"state": "needs_action"`) {
		t.Fatalf("diagnose masks failure: %s", output.String())
	}
}
