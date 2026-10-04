package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/central"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/daemon"
	pausepkg "github.com/KristjanPikhof/Auto-Commit-Daemon/internal/pause"
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
	entry := productListEntry{State: productStateNeedsAction, CaptureHealth: status.CaptureHealth, PublicationProgress: status.PublicationProgress,
		PublicationOutcome: publicationOutcome{RecoveredChanges: 1, BranchCommitted: &control.Published}}
	if productListPhase(entry) != "capture-blocked" || productListStatus(entry) != "needs action" || !strings.Contains(publicationProgressPhaseLabel(status.PublicationProgress), "incomplete capture") {
		t.Fatalf("human views mask capture failure: phase=%q status=%q", productListPhase(entry), productListStatus(entry))
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
	for _, pending := range []bool{false, true} {
		for _, age := range []time.Duration{time.Minute, time.Hour} {
			t.Run(strconv.FormatBool(pending)+"/"+age.String(), func(t *testing.T) {
				ctx := context.Background()
				repo, dbPath, db := makeSeededRepoStateDB(t)
				now := time.Now().Truncate(time.Second)
				lastPoll := now.Add(-age)
				if err := state.SaveDaemonState(ctx, db, state.DaemonState{PID: os.Getpid(), Mode: "running", HeartbeatTS: float64(now.Unix()), UpdatedTS: float64(now.Unix())}); err != nil {
					t.Fatal(err)
				}
				if err := state.MetaSetMany(ctx, db, map[string]string{
					daemon.MetaKeyProtectionFullPollTS:       strconv.FormatInt(lastPoll.Unix(), 10),
					daemon.MetaKeyProtectionObservationEpoch: "2",
					daemon.MetaKeyProtectionCoveredEpoch:     "1",
					daemon.MetaKeyProtectionComplete:         "false",
				}); err != nil {
					t.Fatal(err)
				}
				if pending {
					if _, err := state.AppendCaptureEvent(ctx, db, state.CaptureEvent{
						BranchRef: "refs/heads/main", BranchGeneration: 1, BaseHead: "head",
						Operation: "modify", Path: "main.go", Fidelity: "exact", CapturedTS: float64(lastPoll.Unix()),
					}, nil); err != nil {
						t.Fatal(err)
					}
				}
				rec := central.RepoRecord{Path: repo, StateDB: dbPath, RepositoryID: "repository", WorktreeID: "worktree"}
				report, err := buildStatusReport(ctx, rec, now)
				if err != nil {
					t.Fatal(err)
				}
				overview, err := readProductListRepo(ctx, rec, now)
				if err != nil {
					t.Fatal(err)
				}
				wantPhase := "checkpointing"
				if age == time.Hour {
					wantPhase = "stalled"
				}
				for _, got := range []publicationProgressReport{report.PublicationProgress, overview.report.PublicationProgress} {
					if got.Phase != wantPhase || got.LastProgressTS != float64(lastPoll.Unix()) {
						t.Fatalf("checkpoint phase=%+v want=%s baseline=%d", got, wantPhase, lastPoll.Unix())
					}
					if !pending && got.QueuePending != 0 {
						t.Fatalf("empty queue progress=%+v", got)
					}
				}
				if wantPhase == "stalled" {
					control := controlResult{OK: true, Enabled: true}
					applyControlStatusWithDaemonAlive(&control, report, true)
					if control.Health != controlHealthNeedsAttention || control.Protected || strings.Contains(control.Summary, "remains protected") {
						t.Fatalf("capture stall=%+v", control)
					}
					entry := productListEntryFromOverview(rec, supervisor.WorkerStatus{}, overview, nil)
					if entry.State != productStateNeedsAction || entry.PublicationProgress.Phase != "stalled" {
						t.Fatalf("list masked stalled checkpoint: %+v", entry)
					}
				}
			})
		}
	}
}

func TestCaptureHealthPreservesIndependentAttention(t *testing.T) {
	for _, captureState := range []string{"retrying", "blocked"} {
		for _, tc := range []struct {
			name, remedy string
			mutate       func(*statusReport)
		}{
			{"manual pause", "pause reason", func(s *statusReport) { s.Paused = true; s.Pause = &pauseInfo{Source: "manual"} }},
			{"backpressure", "clearing backpressure", func(s *statusReport) { s.BackpressurePaused = true }},
			{"configuration", "config edit", func(s *statusReport) { s.Configuration.Configuration = "needs_attention" }},
			{"blocked drain", "support diagnose", func(s *statusReport) { s.PublicationDrain.Phase = state.PublicationDrainNeedsAction }},
			{"replay", "support recover", func(s *statusReport) { s.Replay.State = "needs_attention" }},
			{"terminal captures", "support recover", func(s *statusReport) { s.ActiveTerminalEvents = 1 }},
			{"barrier", "support recover", func(s *statusReport) { s.ActiveBarriers = 1 }},
			{"Intent recovery", intentRecoveryVerificationAttentionNext, func(s *statusReport) {
				s.PublicationProgress = publicationProgressReport{Origin: "intent_recovery", Phase: "needs_action"}
			}},
		} {
			t.Run(captureState+"/"+tc.name, func(t *testing.T) {
				report := statusReport{Daemon: "running", PID: os.Getpid(), CaptureHealth: state.CaptureHealth{State: captureState, Error: "unstable file"}}
				tc.mutate(&report)
				control := controlResult{OK: true, Enabled: true}
				applyControlStatusWithDaemonAlive(&control, report, true)
				if control.OK || control.Health != controlHealthNeedsAttention || !strings.Contains(control.NextAction, tc.remedy) || !strings.Contains(control.Summary, "incomplete") {
					t.Fatalf("capture hides attention: %+v", control)
				}
				envelope := envelopeFromControl(control)
				if envelope.State != productStateNeedsAction || envelope.Data.(productStatusData).CaptureHealth.Error != report.CaptureHealth.Error {
					t.Fatalf("status lost required action or capture health: %+v", envelope)
				}
				entry := productListEntryFromOverview(central.RepoRecord{RepositoryID: "repository", WorktreeID: "worktree"}, supervisor.WorkerStatus{}, productListRepoOverview{report: report}, nil)
				if entry.State != productStateNeedsAction || entry.CaptureHealth.Error != report.CaptureHealth.Error {
					t.Fatalf("list lost required action or capture health: %+v", entry)
				}
			})
		}
	}
}

func TestCaptureHealthDiagnosePreservesIndependentAttention(t *testing.T) {
	for _, operational := range []bool{false, true} {
		report := diagnoseReport{CaptureHealth: state.CaptureHealth{State: "retrying", Error: "unstable file"}}
		if operational {
			report.OperationalState = "needs_attention"
		} else {
			report.PublicationDrain.Phase = state.PublicationDrainNeedsAction
		}
		var output bytes.Buffer
		if err := renderProductDiagnoseReport(&output, report); err != nil {
			t.Fatal(err)
		}
		var envelope struct {
			State productState   `json:"state"`
			Data  diagnoseReport `json:"data"`
		}
		if err := json.Unmarshal(output.Bytes(), &envelope); err != nil {
			t.Fatal(err)
		}
		if envelope.State != productStateNeedsAction || envelope.Data.CaptureHealth.Error != report.CaptureHealth.Error {
			t.Fatalf("diagnose lost required action or capture health: %s", output.String())
		}
	}
}

func TestCaptureHealthPausedProtectionThroughProductionReaders(t *testing.T) {
	for _, manual := range []bool{false, true} {
		t.Run(strconv.FormatBool(manual), func(t *testing.T) {
			withIsolatedHome(t)
			ctx := context.Background()
			repo, dbPath, db := makeSeededRepoStateDB(t)
			now := time.Now()
			if err := state.SaveDaemonState(ctx, db, state.DaemonState{PID: os.Getpid(), Mode: "running", HeartbeatTS: float64(now.Unix()), UpdatedTS: float64(now.Unix())}); err != nil {
				t.Fatal(err)
			}
			health := state.CaptureHealth{State: "retrying", Error: "unstable file"}
			if err := state.MetaSetJSON(ctx, db, "capture.health", health); err != nil {
				t.Fatal(err)
			}
			if err := state.MetaSet(ctx, db, "last_capture_error", health.Error); err != nil {
				t.Fatal(err)
			}
			wantPhase, wantOperational := "needs_action", "needs_attention"
			if manual {
				writePauseMarkerForStateDB(t, dbPath, pausepkg.Marker{Reason: "inspect changes", SetAt: now.UTC().Format(time.RFC3339), SetBy: "test"})
				wantPhase, wantOperational = "paused", "paused"
			} else if err := state.MetaSet(ctx, db, daemon.MetaKeyCaptureBackpressurePausedAt, now.UTC().Format(time.RFC3339)); err != nil {
				t.Fatal(err)
			}
			rec := central.RepoRecord{Path: repo, StateDB: dbPath, RepositoryID: "repository", WorktreeID: "worktree"}
			status, err := buildStatusReport(ctx, rec, now)
			if err != nil {
				t.Fatal(err)
			}
			overview, err := readProductListRepo(ctx, rec, now)
			if err != nil {
				t.Fatal(err)
			}
			for _, report := range []statusReport{status, overview.report} {
				if report.PublicationProgress.Phase != wantPhase || report.OperationalState != wantOperational || report.CaptureHealth.State != "retrying" {
					t.Fatalf("pause hidden: %+v", report)
				}
			}
			entry := productListEntryFromOverview(rec, supervisor.WorkerStatus{}, overview, nil)
			wantLabel := "blocked"
			if manual {
				wantLabel = "paused"
			}
			if entry.State != productStateNeedsAction || productListPhase(entry) != wantLabel {
				t.Fatalf("list hides pause: %+v", entry)
			}
			diagnose, err := buildDiagnoseReport(ctx, rec)
			if err != nil {
				t.Fatal(err)
			}
			var output bytes.Buffer
			if err := renderProductDiagnoseReport(&output, diagnose); err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(output.String(), `"state": "needs_action"`) || !strings.Contains(output.String(), `"error": "unstable file"`) {
				t.Fatalf("diagnose hides pause or capture health: %s", output.String())
			}
		})
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
