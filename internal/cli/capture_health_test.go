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
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/supervisor"
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
	rec := central.RepoRecord{Path: repo, StateDB: dbPath}
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

func TestCaptureHealthCheckpointStallWithoutPendingEvents(t *testing.T) {
	t.Setenv("ACD_AI_TIMEOUT", "1m")
	report := statusReport{Daemon: "running", PID: os.Getpid(), Busy: true, CheckpointProtectionAvailable: true, FullPollTS: 100}
	progress, err := buildPublicationProgressReport(context.Background(), nil, report, time.Unix(1000, 0))
	if err != nil || progress.Phase != "stalled" || progress.QueuePending != 0 {
		t.Fatalf("empty queue stall=%+v err=%v", progress, err)
	}
	report.PublicationProgress = progress
	control := controlResult{OK: true, Enabled: true}
	applyControlStatusWithDaemonAlive(&control, report, true)
	if control.Health != controlHealthNeedsAttention || control.Protected || strings.Contains(control.Summary, "remains protected") {
		t.Fatalf("capture stall=%+v", control)
	}
	entry := productListEntryFromOverview(central.RepoRecord{RepositoryID: "repository", WorktreeID: "worktree"}, supervisor.WorkerStatus{}, productListRepoOverview{report: report}, nil)
	if entry.State != productStateNeedsAction || entry.PublicationProgress.Phase != "stalled" {
		t.Fatalf("list masked stalled checkpoint: %+v", entry)
	}
}

func TestCaptureHealthRecoveryFailsWhenWorkerUnavailable(t *testing.T) {
	roots := withIsolatedHome(t)
	repo, dbPath, db := makeSeededRepoStateDB(t)
	registerRepo(t, roots, repo, dbPath, "shell")
	if err := state.MetaSet(context.Background(), db, "last_capture_error", "eligible file unreadable"); err != nil {
		t.Fatal(err)
	}
	plan, err := executeFix(context.Background(), repo, false, true, false, false)
	if err == nil || plan == nil || !plan.Incomplete || plan.CaptureHealth.Error == "" || plan.RuntimeQuiescence {
		t.Fatalf("recovery=%+v err=%v", plan, err)
	}
}
