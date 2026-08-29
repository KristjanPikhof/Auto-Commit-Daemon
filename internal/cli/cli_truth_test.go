package cli

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/central"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/daemon"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

func TestIntentObservabilityNewestPlannerErrorWins(t *testing.T) {
	_, _, d := makeRepoStateDB(t)
	ctx := context.Background()
	if err := state.MetaSet(ctx, d, "commit.strategy", "intent"); err != nil {
		t.Fatal(err)
	}
	for _, rec := range []state.DecisionRecord{
		{DecisionTS: 20, Kind: state.DecisionKindIntentPlannerError,
			EventSeq: sql.NullInt64{Int64: 22, Valid: true},
			Path:     sql.NullString{String: "new.go", Valid: true},
			Reason:   sql.NullString{String: "newest validation failure", Valid: true}},
		{DecisionTS: 10, Kind: state.DecisionKindIntentPlannerError,
			EventSeq: sql.NullInt64{Int64: 11, Valid: true},
			Path:     sql.NullString{String: "stale.go", Valid: true},
			Reason:   sql.NullString{String: "stale appended later", Valid: true}},
	} {
		if _, err := state.AppendDecision(ctx, d, rec); err != nil {
			t.Fatal(err)
		}
	}
	report, err := loadIntentStrategyReport(ctx, d.SQL())
	if err != nil {
		t.Fatal(err)
	}
	if report.LastPlannerErrorEventSeq != 22 ||
		report.LastPlannerErrorPath != "new.go" ||
		report.LastPlannerError != "newest validation failure" ||
		report.LastPlannerErrorTS != 20 {
		t.Fatalf("planner error=%+v", report)
	}
}

func TestDiagnosePlannerErrorRemediationTracksCurrentHealth(t *testing.T) {
	healthy := diagnoseReport{
		PendingDepth: 1,
		IntentStrategy: intentStrategyReport{
			EffectiveProvider:        "openai-compat",
			EffectiveModel:           "gpt-test",
			LastPlannerError:         "stale validation failure",
			LastPlannerErrorEventSeq: 22,
			LastPlannerErrorTS:       100,
			PlannerHealth: &daemon.IntentPlannerHealthSnapshot{
				State: daemon.IntentPlannerCircuitClosed,
			},
			LastPlannerWindow: &intentPlannerWindowSummary{
				PlannedTS:      101,
				Provider:       "openai-compat",
				Model:          "gpt-test",
				ResolutionMode: "provider",
			},
		},
	}

	tests := []struct {
		name       string
		report     diagnoseReport
		wantWarn   bool
		wantPhrase string
	}{
		{name: "later healthy provider window", report: healthy},
		{name: "drained historical error", report: func() diagnoseReport {
			r := healthy
			r.PendingDepth = 0
			return r
		}()},
		{name: "pending error not yet recovered", report: func() diagnoseReport {
			r := healthy
			window := *r.IntentStrategy.LastPlannerWindow
			window.PlannedTS = 99
			r.IntentStrategy.LastPlannerWindow = &window
			return r
		}(), wantWarn: true, wantPhrase: "evidence-based local grouping"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			found := strings.Join(diagnoseRemediation(tc.report), "\n")
			warned := strings.Contains(found, "last failed validation")
			if warned != tc.wantWarn {
				t.Fatalf("remediation=%q warned=%t want %t", found, warned, tc.wantWarn)
			}
			if tc.wantPhrase != "" && !strings.Contains(found, tc.wantPhrase) {
				t.Fatalf("remediation=%q missing %q", found, tc.wantPhrase)
			}
			if strings.Contains(found, "deterministic fallback") {
				t.Fatalf("remediation uses obsolete fallback wording: %q", found)
			}
		})
	}
}

func TestDiagnoseUsesExactWorktreeRejectPath(t *testing.T) {
	want := filepath.Join(t.TempDir(), "linked", ".git", "worktrees", "topic",
		"acd", "planner-rejects.jsonl")
	report := diagnoseReport{IntentStrategy: intentStrategyReport{
		RejectLogPath: want,
		PlannerHealth: &daemon.IntentPlannerHealthSnapshot{
			State: daemon.IntentPlannerCircuitOpen,
		},
	}}
	found := strings.Join(diagnoseRemediation(report), "\n")
	if !strings.Contains(found, want) || strings.Contains(found, ".git/acd/"+want) {
		t.Fatalf("remediation=%q want exact path %q", found, want)
	}
}

func TestIntentObservabilityReportsEffectiveCorrectionMaximum(t *testing.T) {
	for _, tc := range []struct {
		raw        string
		configured int
		effective  int
	}{
		{raw: `{"intent.retry_on_invalid":"9"}`, configured: 9, effective: 2},
		{raw: `{"intent.retry_on_invalid":"0"}`, configured: 0, effective: 0},
	} {
		var report intentV2Report
		decodeIntentV2Snapshot(tc.raw, &report)
		if report.ConfiguredRetryOnInvalid != tc.configured ||
			report.EffectiveCorrectionMax != tc.effective {
			t.Fatalf("snapshot=%s report=%+v", tc.raw, report)
		}
	}
}

func TestControlTruthStateMatrix(t *testing.T) {
	healthOpen := &daemon.IntentPlannerHealthSnapshot{
		State: daemon.IntentPlannerCircuitOpen,
	}
	for _, tc := range []struct {
		name       string
		mutate     func(*statusReport)
		wantState  string
		wantHealth string
		wantText   string
	}{
		{name: "retrying", mutate: func(s *statusReport) {
			s.Replay = replayObservabilityReport{State: "degraded", ErrorRepeatCount: 3}
		}, wantState: "retrying", wantHealth: controlHealthDegraded, wantText: "3 consecutive"},
		{name: "durable block", mutate: func(s *statusReport) {
			s.Replay = replayObservabilityReport{State: "needs_attention"}
		}, wantState: "needs_attention", wantHealth: controlHealthNeedsAttention, wantText: "safety block"},
		{name: "circuit fallback", mutate: func(s *statusReport) {
			s.IntentStrategy.PlannerHealth = healthOpen
			s.PendingEvents = 1
		}, wantState: "fallback", wantHealth: controlHealthDegraded, wantText: "semantic commit messages are waiting"},
		{name: "batch wait", mutate: func(s *statusReport) {
			s.PendingEvents = 2
			s.IntentStrategy = intentStrategyReport{Active: true, BatchWaitActive: true}
		}, wantState: "waiting", wantHealth: controlHealthWaiting, wantText: "waiting to group"},
		{name: "busy", mutate: func(s *statusReport) {
			s.PendingEvents = 2
			s.Busy = true
		}, wantState: "busy", wantHealth: controlHealthHealthy, wantText: "publishing"},
		{name: "healthy idle", mutate: func(*statusReport) {},
			wantState: "healthy_idle", wantHealth: controlHealthHealthy, wantText: "normally"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			status := statusReport{Daemon: "running", PID: os.Getpid()}
			tc.mutate(&status)
			status.OperationalState = statusOperationalState(status)
			if status.OperationalState != tc.wantState {
				t.Fatalf("state=%q want=%q status=%+v", status.OperationalState, tc.wantState, status)
			}
			result := controlResult{OK: true, Health: controlHealthHealthy}
			applyControlStatus(&result, status)
			if result.Health != tc.wantHealth || !strings.Contains(result.Summary, tc.wantText) {
				t.Fatalf("control=%+v", result)
			}
		})
	}
}

func TestOperationalStatePublicationDrainSelfHealMatrix(t *testing.T) {
	for _, tc := range []struct {
		phase        string
		fallbackMode string
		want         string
	}{
		{state.PublicationDrainCheckpointing, "", "self_healing"},
		{state.PublicationDrainSemantic, "", "planning"},
		{state.PublicationDrainNormalizing, "", "self_healing"},
		{state.PublicationDrainEventFallback, "", "event_fallback"},
		{state.PublicationDrainEventFallback, "semantic_replan", "planning"},
		{state.PublicationDrainNeedsAction, "", "needs_attention"},
		{state.PublicationDrainCompleted, "", "healthy_idle"},
	} {
		report := statusReport{
			Daemon: "running", PID: os.Getpid(),
			PublicationDrain: publicationDrainReport{
				Available: true, ID: "drain", Phase: tc.phase,
				FallbackMode: tc.fallbackMode,
			},
		}
		if got := statusOperationalState(report); got != tc.want {
			t.Fatalf("phase=%s state=%s want=%s", tc.phase, got, tc.want)
		}
	}
}

func TestPublicationDrainStatusDiagnoseDoctorReadOnlyTruth(t *testing.T) {
	ctx := context.Background()
	repo, dbPath, db := makeRepoStateDB(t)
	seqs := insertCompletedCheckpoint(t, db, "cp-truth-drain", "0123456789abcdef",
		[]checkpointMemberFixture{{State: state.EventStatePending}})
	drain := state.PublicationDrain{
		ID: "drain-cp-truth", CheckpointID: "cp-truth-drain",
		WorktreeID: "0123456789abcdef", BranchRef: "refs/heads/main",
		BranchGeneration: 7, CommitStrategy: "event", CommitFormat: "imperative",
		Provider: "deterministic", Phase: state.PublicationDrainEventFallback,
		TargetEventCount: 1, SemanticRebuildAttempts: 1,
		EventFallbackCount: 1, FallbackMode: "atomic_dependency_components",
		LastError: "sanitized planner failure", StagedConsent: true,
		StagedConsumed: true, CreatedTS: 1, UpdatedTS: 2, LastProgressTS: 2,
		EventSeqs: seqs,
	}
	if created, err := state.PreparePublicationDrain(
		ctx, db, drain); err != nil || !created {
		t.Fatalf("prepare drain=(%t,%v)", created, err)
	}
	if _, err := db.SQL().ExecContext(ctx, `
INSERT INTO operations(id,kind,worktree_id,phase,status,created_ts,updated_ts)
VALUES('op-other-drain','checkpoint','0123456789abcdef','completed','completed',3,3);
INSERT INTO checkpoints(
 id,seq,operation_id,worktree_id,reason,observation_epoch,coverage_epoch,
 observed_head,observed_ref,tree_oid,commit_oid,checkpoint_ref,phase,created_ts,completed_ts
) VALUES(
 'cp-other-drain',2,'op-other-drain','0123456789abcdef','manual_barrier',1,1,
 'head','refs/heads/other','tree','commit','refs/acd/checkpoints/cp-other-drain','completed',3,3
)`); err != nil {
		t.Fatal(err)
	}
	otherDrain := state.PublicationDrain{
		ID: "drain-other-newer", CheckpointID: "cp-other-drain",
		WorktreeID: "0123456789abcdef", BranchRef: "refs/heads/other",
		BranchGeneration: 8, CommitStrategy: "event", CommitFormat: "imperative",
		Provider: "deterministic", Phase: state.PublicationDrainSemantic,
		CreatedTS: 3, UpdatedTS: 3, LastProgressTS: 3,
	}
	if created, err := state.PreparePublicationDrain(
		ctx, db, otherDrain); err != nil || !created {
		t.Fatalf("prepare other drain=(%t,%v)", created, err)
	}
	if _, err := db.SQL().ExecContext(ctx, `
INSERT INTO daemon_state(
 id,pid,mode,heartbeat_ts,branch_ref,branch_generation,updated_ts
)
VALUES(1,?,'running',?,'refs/heads/main',7,?)
ON CONFLICT(id) DO UPDATE SET pid=excluded.pid,mode=excluded.mode,
	heartbeat_ts=excluded.heartbeat_ts,branch_ref=excluded.branch_ref,
	branch_generation=excluded.branch_generation,updated_ts=excluded.updated_ts`,
		os.Getpid(), time.Now().Unix(), time.Now().Unix()); err != nil {
		t.Fatal(err)
	}
	if _, err := db.SQL().ExecContext(ctx, "PRAGMA wal_checkpoint(TRUNCATE)"); err != nil {
		t.Fatal(err)
	}
	before, err := fileSHA256(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	rec := central.RepoRecord{
		Path: repo, StateDB: dbPath, RepoHash: "0123456789abcdef",
	}
	status, err := buildStatusReport(ctx, rec, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if status.OperationalState != "event_fallback" ||
		status.PublicationDrain.ID != drain.ID ||
		status.PublicationDrain.RemainingEvents != 1 ||
		!status.PublicationDrain.StagedConsumed {
		t.Fatalf("status drain=%+v state=%s",
			status.PublicationDrain, status.OperationalState)
	}
	overview, err := readProductListRepo(ctx, rec, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if overview.report.PublicationDrain.ID != drain.ID {
		t.Fatalf("list drain=%+v, newer other-pair drain=%s",
			overview.report.PublicationDrain, otherDrain.ID)
	}
	diagnose := diagnoseReport{}
	diagnose.OperationalState = status.OperationalState
	diagnose.PublicationDrain = status.PublicationDrain
	doctor := doctorRepoReport{}
	doctor.OperationalState = status.OperationalState
	doctor.PublicationDrain = status.PublicationDrain
	if diagnose.PublicationDrain != doctor.PublicationDrain ||
		diagnose.OperationalState != doctor.OperationalState {
		t.Fatalf("cross-command diagnose=%+v doctor=%+v", diagnose, doctor)
	}
	after, err := fileSHA256(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	if before != after {
		t.Fatalf("read-only status changed DB: before=%s after=%s", before, after)
	}
}

func TestStatusPublicationTruthSeparatesGitAndACD(t *testing.T) {
	roots := withIsolatedHome(t)
	ctx := context.Background()
	repo, dbPath, d := makeRepoStateDB(t)
	seqs := insertCompletedCheckpoint(t, d, "cp-recovered-status",
		"0123456789abcdef", []checkpointMemberFixture{{
			State: state.EventStateRecovered, CommitOID: "recovery-commit",
		}})
	registerRepo(t, roots, repo, dbPath, "codex")
	if err := state.SaveDaemonState(ctx, d, state.DaemonState{
		PID: os.Getpid(), Mode: "running", HeartbeatTS: nowFloat(),
	}); err != nil {
		t.Fatal(err)
	}
	if err := state.MetaSetMany(ctx, d, map[string]string{
		daemon.MetaKeyProtectionObservationEpoch: "7",
		daemon.MetaKeyProtectionCoveredEpoch:     "7",
		daemon.MetaKeyProtectionCheckpointID:     "checkpoint-7",
		daemon.MetaKeyProtectionComplete:         "true",
	}); err != nil {
		t.Fatal(err)
	}
	rec := central.RepoRecord{Path: repo, StateDB: dbPath}
	report, err := buildStatusReport(ctx, rec, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if !report.WorktreeClean || !report.AllChangesCommittedInGit ||
		!report.CheckpointPublishedByACD || report.UnpublishedCheckpoints != 0 {
		t.Fatalf("clean truth=%+v", report)
	}
	listReport := statusReport{}
	if err := readProductListProtection(ctx, d.SQL(), &listReport); err != nil {
		t.Fatal(err)
	}
	if !listReport.Protected || listReport.UnpublishedCheckpoints != 0 {
		t.Fatalf("recovered list truth=%+v", listReport)
	}
	if err := os.WriteFile(filepath.Join(repo, "dirty.txt"), []byte("dirty\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	report, err = buildStatusReport(ctx, rec, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if report.WorktreeClean || report.AllChangesCommittedInGit ||
		!report.CheckpointPublishedByACD {
		t.Fatalf("dirty truth=%+v", report)
	}
	for _, unresolvedState := range []string{
		state.EventStateFailed,
		state.EventStateBlockedConflict,
	} {
		t.Run(unresolvedState, func(t *testing.T) {
			if _, err := d.SQL().ExecContext(ctx,
				`UPDATE capture_events SET state=? WHERE seq=?`,
				unresolvedState, seqs[0]); err != nil {
				t.Fatal(err)
			}
			report, err := buildStatusReport(ctx, rec, time.Now())
			if err != nil {
				t.Fatal(err)
			}
			if report.CheckpointPublishedByACD || report.UnpublishedCheckpoints != 1 {
				t.Fatalf("%s checkpoint truth=%+v", unresolvedState, report)
			}
			listReport := statusReport{}
			if err := readProductListProtection(ctx, d.SQL(), &listReport); err != nil {
				t.Fatal(err)
			}
			if listReport.UnpublishedCheckpoints != 1 {
				t.Fatalf("%s list truth=%+v", unresolvedState, listReport)
			}
		})
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
	rejectPath := plannerRejectLogPath(dbPath)
	for name, read := range map[string]func() error{
		"status":   func() error { return runStatus(ctx, &bytes.Buffer{}, repo, true) },
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
