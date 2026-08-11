package cli

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/central"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/daemon"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/paths"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

func makeProtectedControlRepoStateDB(t *testing.T) (repoDir, stateDB string, db *state.DB) {
	t.Helper()
	repoDir, stateDB, db = makeRepoStateDB(t)
	if err := state.MetaSetMany(context.Background(), db, map[string]string{
		daemon.MetaKeyProtectionObservationEpoch: "1",
		daemon.MetaKeyProtectionCoveredEpoch:     "1",
		daemon.MetaKeyProtectionCheckpointID:     "cp-1700000000000-0123456789abcdef",
		daemon.MetaKeyProtectionComplete:         "true",
	}); err != nil {
		t.Fatalf("seed protected control projection: %v", err)
	}
	return repoDir, stateDB, db
}

func registerProtectedControlRepo(t *testing.T, roots paths.Roots, repoDir string) {
	t.Helper()
	wt, err := git.ResolveWorktree(context.Background(), repoDir)
	if err != nil {
		t.Fatalf("resolve protected control worktree: %v", err)
	}
	if err := central.WithLock(roots, func(registry *central.Registry) error {
		_, registerErr := registry.RegisterResolvedRepo(wt, "", time.Now().Unix())
		return registerErr
	}); err != nil {
		t.Fatalf("register protected control worktree: %v", err)
	}
}

func TestControlBareUnregisteredIsReadOnly(t *testing.T) {
	_ = withIsolatedHome(t)
	repo := makeStartRepo(t)
	stateDB := state.DBPathFromGitDir(filepath.Join(repo, ".git"))

	var out bytes.Buffer
	if err := runControlStatus(context.Background(), &out, repo, true); err != nil {
		t.Fatalf("runControlStatus: %v", err)
	}
	got := decodeControlResult(t, out.Bytes())
	if got.Health != controlHealthOff || got.Registered || got.Enabled || !got.OK {
		t.Fatalf("unexpected unregistered health: %+v", got)
	}
	if got.NextAction != "Run `acd on` to enable it." {
		t.Fatalf("next_action=%q", got.NextAction)
	}
	if _, err := os.Stat(stateDB); !os.IsNotExist(err) {
		t.Fatalf("bare acd created state DB %s: err=%v", stateDB, err)
	}
}

func TestControlBareUncutDisabledRepositoryRecommendsSetup(t *testing.T) {
	roots := withIsolatedHome(t)
	repo, stateDB, db := makeRepoStateDB(t)
	if err := central.WithLock(roots, func(registry *central.Registry) error {
		hash, err := paths.RepoHash(repo)
		if err != nil {
			return err
		}
		registry.UpsertRepo(repo, hash, stateDB, "", time.Now().Unix())
		result := registry.DisableRepo(central.RepoRemovalTarget{Path: repo, StateDB: stateDB}, time.Now().Unix())
		if result.NotFound {
			return errors.New("registered repository was not found")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	var out bytes.Buffer
	if err := runControlStatus(context.Background(), &out, repo, true); err != nil {
		t.Fatalf("runControlStatus: %v", err)
	}
	got := decodeControlResult(t, out.Bytes())
	if got.Health != controlHealthOff || got.NextAction != "Run `acd setup` to upgrade and enable protection." {
		t.Fatalf("uncut disabled status=%+v", got)
	}
}

func TestControlBareNonRepoReturnsClassification(t *testing.T) {
	_ = withIsolatedHome(t)
	nonRepo := t.TempDir()

	var out bytes.Buffer
	if err := runControlStatus(context.Background(), &out, nonRepo, true); ExitCode(err) != ExitActionRequired {
		t.Fatalf("runControlStatus exit=%d err=%v", ExitCode(err), err)
	}
	got := decodeControlResult(t, out.Bytes())
	if got.OK || got.Health != controlHealthNotRepo || got.NextAction == "" {
		t.Fatalf("unexpected non-repo health: %+v", got)
	}
}

func TestControlBareFreshHeartbeatWithDeadPIDNeedsAttention(t *testing.T) {
	roots := withIsolatedHome(t)
	ctx := context.Background()
	repo, _, db := makeProtectedControlRepoStateDB(t)
	registerProtectedControlRepo(t, roots, repo)
	if err := state.SaveDaemonState(ctx, db, state.DaemonState{
		PID:         999_999_999,
		Mode:        "running",
		HeartbeatTS: nowFloat(),
		UpdatedTS:   nowFloat(),
	}); err != nil {
		t.Fatalf("save dead daemon state: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close state DB: %v", err)
	}

	var out bytes.Buffer
	if err := runControlStatus(ctx, &out, repo, true); ExitCode(err) != ExitActionRequired {
		t.Fatalf("runControlStatus exit=%d err=%v", ExitCode(err), err)
	}
	got := decodeControlResult(t, out.Bytes())
	if got.OK || got.Health != controlHealthNeedsAttention || got.NextAction != "Run `acd on` to start it." {
		t.Fatalf("unexpected dead-pid health: %+v", got)
	}
}

func TestApplyControlStatusDurableReplayBlockNeedsAttention(t *testing.T) {
	status := statusReport{
		Daemon: "running", PID: os.Getpid(),
		Replay: replayObservabilityReport{
			State: "needs_attention", ErrorRepeatCount: 3,
		},
	}
	result := controlResult{OK: true, Health: controlHealthHealthy}
	applyControlStatus(&result, status)
	if result.OK || result.Health != controlHealthNeedsAttention ||
		!strings.Contains(result.Summary, "durable block") {
		t.Fatalf("control result=%+v", result)
	}
	if !result.RecoveryRequired {
		t.Fatal("durable replay block did not request automatic recovery")
	}
	if !strings.Contains(result.NextAction, "acd support recover --dry-run") ||
		!strings.Contains(result.NextAction, "acd support recover --yes") ||
		strings.Contains(result.NextAction, "acd doctor") {
		t.Fatalf("next action=%q", result.NextAction)
	}
}

func TestApplyControlStatusIncompleteCheckpointNeedsAttention(t *testing.T) {
	status := statusReport{
		Daemon:                        "running",
		PID:                           os.Getpid(),
		CheckpointProtectionAvailable: true,
		ObservationEpoch:              7,
		CoveredEpoch:                  6,
	}
	result := controlResult{OK: true, Health: controlHealthHealthy}

	applyControlStatus(&result, status)

	if result.OK || result.Health != controlHealthNeedsAttention || result.Protected {
		t.Fatalf("incomplete checkpoint result=%+v", result)
	}
	if !strings.Contains(result.NextAction, "acd doctor") {
		t.Fatalf("next action=%q", result.NextAction)
	}
}

func TestApplyControlStatusBusyCheckpointCompletesAutomatically(t *testing.T) {
	status := statusReport{
		Daemon:                        "running",
		PID:                           os.Getpid(),
		Busy:                          true,
		CheckpointProtectionAvailable: true,
		ObservationEpoch:              7,
		CoveredEpoch:                  6,
	}
	result := controlResult{OK: true, Health: controlHealthHealthy}

	applyControlStatus(&result, status)

	if !result.OK || result.Health != controlHealthWaiting || result.Protected {
		t.Fatalf("busy checkpoint result=%+v", result)
	}
	if !strings.Contains(result.NextAction, "completes automatically") || strings.Contains(result.NextAction, "acd doctor") {
		t.Fatalf("busy checkpoint next action=%q", result.NextAction)
	}
}

func TestApplyControlStatusManualPauseOutranksIncompleteCheckpoint(t *testing.T) {
	status := statusReport{
		Daemon:                        "running",
		PID:                           os.Getpid(),
		Paused:                        true,
		Pause:                         &pauseInfo{Source: "manual", Reason: "repo surgery"},
		CheckpointProtectionAvailable: true,
		ObservationEpoch:              7,
		CoveredEpoch:                  6,
	}
	result := controlResult{OK: true, Health: controlHealthHealthy}

	applyControlStatus(&result, status)

	if result.OK || result.Health != controlHealthNeedsAttention ||
		!strings.Contains(result.Summary, "paused") ||
		!strings.Contains(result.NextAction, "acd status") {
		t.Fatalf("paused incomplete-checkpoint result=%+v", result)
	}
}

func TestEnvelopeFromControlPreservesWorker(t *testing.T) {
	want := controlResult{Daemon: "running", Health: controlHealthHealthy}

	envelope := envelopeFromControl(want)
	data, ok := envelope.Data.(productStatusData)
	if !ok {
		t.Fatalf("status data type=%T, want productStatusData", envelope.Data)
	}
	if data.Worker != want.Daemon {
		t.Fatalf("worker=%q, want %q", data.Worker, want.Daemon)
	}
}

func TestControlBareIgnoresHistoricalInactiveTerminalEvents(t *testing.T) {
	roots := withIsolatedHome(t)
	ctx := context.Background()
	repo, _, db := makeProtectedControlRepoStateDB(t)
	registerProtectedControlRepo(t, roots, repo)
	if err := state.SaveDaemonState(ctx, db, state.DaemonState{
		PID:              os.Getpid(),
		Mode:             "running",
		HeartbeatTS:      nowFloat(),
		BranchRef:        sqlNullStr("refs/heads/main"),
		BranchGeneration: sql.NullInt64{Int64: 1, Valid: true},
		UpdatedTS:        nowFloat(),
	}); err != nil {
		t.Fatalf("save daemon state: %v", err)
	}
	seq, err := state.AppendCaptureEvent(ctx, db, state.CaptureEvent{
		BranchRef:        "refs/heads/old-topic",
		BranchGeneration: 7,
		BaseHead:         "deadbeef",
		Operation:        "modify",
		Path:             "old.go",
		Fidelity:         "exact",
	}, []state.CaptureOp{{Op: "modify", Path: "old.go", Fidelity: "exact"}})
	if err != nil {
		t.Fatalf("append historical conflict: %v", err)
	}
	if err := state.MarkEventBlocked(ctx, db, seq, "before-state mismatch", nowFloat(),
		sqlNullStr("refs/heads/old-topic"), sql.NullInt64{Int64: 7, Valid: true}, sqlNullStr("deadbeef")); err != nil {
		t.Fatalf("mark historical conflict: %v", err)
	}
	failedSeq, err := state.AppendCaptureEvent(ctx, db, state.CaptureEvent{
		BranchRef:        "refs/heads/old-topic",
		BranchGeneration: 7,
		BaseHead:         "deadbeef",
		Operation:        "modify",
		Path:             "failed.go",
		Fidelity:         "exact",
	}, []state.CaptureOp{{Op: "modify", Path: "failed.go", Fidelity: "exact"}})
	if err != nil {
		t.Fatalf("append historical failure: %v", err)
	}
	if err := state.MarkEventPublished(ctx, db, failedSeq, state.EventStateFailed,
		sql.NullString{}, sqlNullStr("provider failed"), sql.NullString{}, nowFloat()); err != nil {
		t.Fatalf("mark historical failure: %v", err)
	}
	if _, err := state.AppendCaptureEvent(ctx, db, state.CaptureEvent{
		BranchRef:        "refs/heads/old-topic",
		BranchGeneration: 7,
		BaseHead:         "deadbeef",
		Operation:        "modify",
		Path:             "historical-pending.go",
		Fidelity:         "exact",
	}, []state.CaptureOp{{Op: "modify", Path: "historical-pending.go", Fidelity: "exact"}}); err != nil {
		t.Fatalf("append historical pending event: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close state DB: %v", err)
	}

	var out bytes.Buffer
	if err := runControlStatus(ctx, &out, repo, true); err != nil {
		t.Fatalf("runControlStatus: %v", err)
	}
	got := decodeControlResult(t, out.Bytes())
	if !got.OK || got.Health == controlHealthNeedsAttention || got.BlockedEvents != 1 {
		t.Fatalf("historical conflict affected active health: %+v", got)
	}
}

func TestControlBareNeedsAttentionForActiveTailTerminalEvent(t *testing.T) {
	for _, terminalState := range []string{state.EventStateBlockedConflict, state.EventStateFailed} {
		t.Run(terminalState, func(t *testing.T) {
			roots := withIsolatedHome(t)
			ctx := context.Background()
			repo, _, db := makeProtectedControlRepoStateDB(t)
			registerProtectedControlRepo(t, roots, repo)
			if err := state.SaveDaemonState(ctx, db, state.DaemonState{
				PID:              os.Getpid(),
				Mode:             "running",
				HeartbeatTS:      nowFloat(),
				BranchRef:        sqlNullStr("refs/heads/main"),
				BranchGeneration: sql.NullInt64{Int64: 1, Valid: true},
				UpdatedTS:        nowFloat(),
			}); err != nil {
				t.Fatalf("save daemon state: %v", err)
			}
			seq, err := state.AppendCaptureEvent(ctx, db, state.CaptureEvent{
				BranchRef: "refs/heads/main", BranchGeneration: 1,
				BaseHead: "deadbeef", Operation: "modify", Path: "stuck.go",
				Fidelity: "exact",
			}, []state.CaptureOp{{Op: "modify", Path: "stuck.go", Fidelity: "exact"}})
			if err != nil {
				t.Fatalf("append terminal event: %v", err)
			}
			switch terminalState {
			case state.EventStateBlockedConflict:
				err = state.MarkEventBlocked(ctx, db, seq, "before-state mismatch", nowFloat(),
					sqlNullStr("refs/heads/main"), sql.NullInt64{Int64: 1, Valid: true}, sqlNullStr("deadbeef"))
			case state.EventStateFailed:
				err = state.MarkEventPublished(ctx, db, seq, state.EventStateFailed,
					sql.NullString{}, sqlNullStr("provider failed"), sql.NullString{}, nowFloat())
			}
			if err != nil {
				t.Fatalf("mark terminal event: %v", err)
			}
			if err := db.Close(); err != nil {
				t.Fatalf("close state DB: %v", err)
			}

			var out bytes.Buffer
			if err := runControlStatus(ctx, &out, repo, true); ExitCode(err) != ExitActionRequired {
				t.Fatalf("runControlStatus exit=%d err=%v", ExitCode(err), err)
			}
			got := decodeControlResult(t, out.Bytes())
			if got.OK || got.Health != controlHealthNeedsAttention || !strings.Contains(got.Summary, "blocked publication") {
				t.Fatalf("active tail terminal event was not surfaced: %+v", got)
			}
			if !strings.Contains(got.NextAction, "acd support recover --dry-run") ||
				!strings.Contains(got.NextAction, "acd support recover --yes") {
				t.Fatalf("active tail recovery action=%q", got.NextAction)
			}
		})
	}
}

func TestControlOnReturnsErrorAfterRenderingUnhealthyResult(t *testing.T) {
	roots := withIsolatedHome(t)
	ctx := context.Background()
	repo, stateDB, db := makeProtectedControlRepoStateDB(t)
	registerRepo(t, roots, repo, stateDB, "")
	if err := state.SaveDaemonState(ctx, db, state.DaemonState{
		PID:              os.Getpid(),
		Mode:             "running",
		HeartbeatTS:      nowFloat(),
		BranchRef:        sqlNullStr("refs/heads/main"),
		BranchGeneration: sql.NullInt64{Int64: 1, Valid: true},
		UpdatedTS:        nowFloat(),
	}); err != nil {
		t.Fatalf("save daemon state: %v", err)
	}
	seq, err := state.AppendCaptureEvent(ctx, db, state.CaptureEvent{
		BranchRef: "refs/heads/main", BranchGeneration: 1,
		BaseHead: "deadbeef", Operation: "modify", Path: "stuck.go", Fidelity: "exact",
	}, []state.CaptureOp{{Op: "modify", Path: "stuck.go", Fidelity: "exact"}})
	if err != nil {
		t.Fatalf("append terminal event: %v", err)
	}
	if err := state.MarkEventBlocked(ctx, db, seq, "before-state mismatch", nowFloat(),
		sqlNullStr("refs/heads/main"), sql.NullInt64{Int64: 1, Valid: true}, sqlNullStr("deadbeef")); err != nil {
		t.Fatalf("mark terminal event: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close state DB: %v", err)
	}
	previousSpawn := spawnDaemon
	spawnCount := 0
	spawnDaemon = func(ctx context.Context, repoAbs string) (int, error) {
		spawnCount++
		spawnDB, err := state.Open(
			ctx, state.DBPathFromGitDir(filepath.Join(repoAbs, ".git")))
		if err != nil {
			return 0, err
		}
		defer spawnDB.Close()
		if err := state.SaveDaemonState(ctx, spawnDB, state.DaemonState{
			PID:              os.Getpid(),
			Mode:             "running",
			HeartbeatTS:      nowFloat(),
			BranchRef:        sqlNullStr("refs/heads/main"),
			BranchGeneration: sql.NullInt64{Int64: 1, Valid: true},
			UpdatedTS:        nowFloat(),
		}); err != nil {
			return 0, err
		}
		return os.Getpid(), nil
	}
	t.Cleanup(func() { spawnDaemon = previousSpawn })

	var out bytes.Buffer
	err = runControlOn(ctx, &out, repo, true)
	if ExitCode(err) != ExitActionRequired || !strings.Contains(err.Error(), "setup") {
		t.Fatalf("runControlOn exit=%d err=%v want setup-required result", ExitCode(err), err)
	}
	if out.Len() != 0 || spawnCount != 0 {
		t.Fatalf("v19 on mutated before cutover: output=%q spawn_count=%d", out.String(), spawnCount)
	}
}

func TestApplyControlStatusPlannerCircuitDegraded(t *testing.T) {
	for _, tc := range []struct {
		state    daemon.IntentPlannerCircuitState
		nextText string
	}{
		{state: daemon.IntentPlannerCircuitOpen, nextText: "probe the provider automatically after cooldown"},
		{state: daemon.IntentPlannerCircuitHalfOpen, nextText: "running the automatic provider probe"},
	} {
		t.Run(string(tc.state), func(t *testing.T) {
			status := statusReport{
				Daemon:        "running",
				PID:           os.Getpid(),
				CaptureErrors: 2,
				IntentStrategy: intentStrategyReport{PlannerHealth: &daemon.IntentPlannerHealthSnapshot{
					State: tc.state,
				}},
			}
			res := controlResult{OK: true}
			applyControlStatus(&res, status)
			if res.Health != controlHealthDegraded ||
				!strings.Contains(res.Summary, "deterministic fallback") ||
				!strings.Contains(res.NextAction, tc.nextText) {
				t.Fatalf("control result=%+v", res)
			}
		})
	}
}

func TestApplyControlStatusRewindGraceIsWaiting(t *testing.T) {
	status := statusReport{
		Daemon: "running",
		PID:    os.Getpid(),
		Paused: true,
		Pause: &pauseInfo{
			Source:           "rewind_grace",
			Reason:           "rewind grace",
			ExpiresAt:        time.Now().UTC().Add(45 * time.Second).Format(time.RFC3339),
			RemainingSeconds: 45,
		},
	}
	res := controlResult{OK: true}

	applyControlStatus(&res, status)

	if !res.OK || res.Health != controlHealthWaiting {
		t.Fatalf("rewind grace should be a healthy wait: %+v", res)
	}
	if !strings.Contains(res.Summary, "45s") || !strings.Contains(res.NextAction, "resume automatically") {
		t.Fatalf("rewind grace guidance is not actionable: %+v", res)
	}
}

func TestApplyControlStatusRewindGraceDoesNotHideRecoveryBlockers(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*statusReport)
		want   string
	}{
		{
			name: "backpressure",
			mutate: func(status *statusReport) {
				status.BackpressurePaused = true
			},
			want: "durable storage",
		},
		{
			name: "terminal barrier",
			mutate: func(status *statusReport) {
				status.ActiveBarriers = 1
			},
			want: "blocked publication",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			status := statusReport{
				Daemon: "running",
				PID:    os.Getpid(),
				Paused: true,
				Pause: &pauseInfo{
					Source:           "rewind_grace",
					RemainingSeconds: 45,
				},
			}
			tc.mutate(&status)
			result := controlResult{OK: true}
			applyControlStatus(&result, status)
			if result.OK || result.Health != controlHealthNeedsAttention ||
				!strings.Contains(result.Summary, tc.want) {
				t.Fatalf("result=%+v", result)
			}
		})
	}
}

func TestApplyControlStatusManualPauseStillNeedsAttention(t *testing.T) {
	status := statusReport{
		Daemon: "running",
		PID:    os.Getpid(),
		Paused: true,
		Pause: &pauseInfo{
			Source: "manual",
			Reason: "repo surgery",
		},
	}
	res := controlResult{OK: true}

	applyControlStatus(&res, status)

	if res.OK || res.Health != controlHealthNeedsAttention {
		t.Fatalf("manual pause should still need attention: %+v", res)
	}
}

func TestApplyControlStatusPlannerCircuitRespectsBlockerPrecedence(t *testing.T) {
	health := &daemon.IntentPlannerHealthSnapshot{State: daemon.IntentPlannerCircuitOpen}
	for _, tc := range []struct {
		name   string
		mutate func(*statusReport)
		want   string
	}{
		{name: "pause", mutate: func(s *statusReport) { s.Paused = true }, want: "paused"},
		{name: "backpressure", mutate: func(s *statusReport) { s.BackpressurePaused = true }, want: "durable storage"},
		{name: "barrier", mutate: func(s *statusReport) { s.ActiveBarriers = 1 }, want: "blocked publication"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			status := statusReport{
				Daemon:         "running",
				PID:            os.Getpid(),
				IntentStrategy: intentStrategyReport{PlannerHealth: health},
			}
			tc.mutate(&status)
			res := controlResult{OK: true}
			applyControlStatus(&res, status)
			if res.Health != controlHealthNeedsAttention || !strings.Contains(strings.ToLower(res.Summary), tc.want) {
				t.Fatalf("control result=%+v", res)
			}
		})
	}
}

func TestControlOnRegistersStartsAndIsIdempotent(t *testing.T) {
	withIsolatedHome(t)
	repo := makeStartRepo(t)
	var setupRequired bytes.Buffer
	if err := runControlOn(context.Background(), &setupRequired, repo, true); ExitCode(err) != ExitActionRequired || !strings.Contains(err.Error(), "setup") {
		t.Fatalf("uncut repository on exit=%d err=%v", ExitCode(err), err)
	}
	if setupRequired.Len() != 0 {
		t.Fatalf("uncut repository on emitted success output: %q", setupRequired.String())
	}
}

func TestControlOnTreatsRewindGraceAsWaiting(t *testing.T) {
	withIsolatedHome(t)
	repo := makeStartRepo(t)
	if err := runControlOn(context.Background(), io.Discard, repo, true); ExitCode(err) != ExitActionRequired {
		t.Fatalf("uncut repository on exit=%d err=%v", ExitCode(err), err)
	}
}

func TestControlOffDisablesStopsPreservesAndIsIdempotent(t *testing.T) {
	withIsolatedHome(t)
	repo := makeStartRepo(t)
	var offOut bytes.Buffer
	if err := runControlOff(context.Background(), &offOut, repo, true); err != nil {
		t.Fatalf("off unconfigured repository: %v", err)
	}
	off := decodeControlResult(t, offOut.Bytes())
	if off.Health != controlHealthOff || off.Changed || len(off.Actions) != 0 {
		t.Fatalf("unconfigured off was not idempotent: %+v", off)
	}
}

func TestControlOnRestartsFreshLookingDeadDaemon(t *testing.T) {
	roots := withIsolatedHome(t)
	ctx := context.Background()
	repo, stateDB, db := makeProtectedControlRepoStateDB(t)
	registerRepo(t, roots, repo, stateDB, "")
	if err := state.SaveDaemonState(ctx, db, state.DaemonState{
		PID:         999_999_999,
		Mode:        "running",
		HeartbeatTS: nowFloat(),
		UpdatedTS:   nowFloat(),
	}); err != nil {
		t.Fatalf("save dead daemon state: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close state DB: %v", err)
	}
	var setupRequired bytes.Buffer
	if err := runControlOn(ctx, &setupRequired, repo, true); ExitCode(err) != ExitActionRequired || !strings.Contains(err.Error(), "setup") {
		t.Fatalf("v19 repository on exit=%d err=%v", ExitCode(err), err)
	}
}

func TestControlOffUnknownRepoRecordsDurableOptOut(t *testing.T) {
	roots := withIsolatedHome(t)
	repo := makeStartRepo(t)

	var out bytes.Buffer
	if err := runControlOff(context.Background(), &out, repo, true); err != nil {
		t.Fatalf("runControlOff: %v", err)
	}
	got := decodeControlResult(t, out.Bytes())
	if got.Changed || len(got.Actions) != 0 || got.Health != controlHealthOff {
		t.Fatalf("unexpected idempotent off result: %+v", got)
	}
	reg, err := central.Load(roots)
	if err != nil {
		t.Fatalf("load registry: %v", err)
	}
	rec, ok := reg.FindRepo(repo, state.DBPathFromGitDir(filepath.Join(repo, ".git")))
	if ok {
		t.Fatalf("off registered an unknown repository: rec=%+v", rec)
	}
}

func TestControlOffUnknownDetachedRepoRecordsDurableOptOut(t *testing.T) {
	roots := withIsolatedHome(t)
	ctx := context.Background()
	repo := makeStartRepo(t)
	commitStartRepoSeed(t, repo)
	if _, err := git.Run(ctx, git.RunOpts{Dir: repo}, "checkout", "--detach", "--quiet"); err != nil {
		t.Fatalf("detach HEAD: %v", err)
	}

	var out bytes.Buffer
	if err := runControlOff(ctx, &out, repo, true); err != nil {
		t.Fatalf("runControlOff detached: %v", err)
	}
	got := decodeControlResult(t, out.Bytes())
	if got.Health != controlHealthOff || got.Changed || len(got.Actions) != 0 {
		t.Fatalf("unexpected detached off result: %+v", got)
	}
	reg, err := central.Load(roots)
	if err != nil {
		t.Fatalf("load registry: %v", err)
	}
	rec, ok := reg.FindRepo(repo, state.DBPathFromGitDir(filepath.Join(repo, ".git")))
	if ok {
		t.Fatalf("detached off registered an unknown repository: rec=%+v", rec)
	}
	if _, err := os.Stat(state.DBPathFromGitDir(filepath.Join(repo, ".git"))); !os.IsNotExist(err) {
		t.Fatalf("detached off should not initialize state DB: err=%v", err)
	}
}

func TestControlBareHumanAnswersProtectionQuestions(t *testing.T) {
	_ = withIsolatedHome(t)
	repo := makeStartRepo(t)

	var out bytes.Buffer
	if err := runControlStatus(context.Background(), &out, repo, false); err != nil {
		t.Fatalf("runControlStatus: %v", err)
	}
	for _, line := range []string{"Enabled:", "Protected:", "Published to Git:", "Action required:", "Next:"} {
		if got := strings.Count(out.String(), line); got != 1 {
			t.Fatalf("%s line count=%d\n%s", line, got, out.String())
		}
	}
}

func TestControlRootBareUsesReadOnlyHealth(t *testing.T) {
	_ = withIsolatedHome(t)
	repo := makeStartRepo(t)
	root := newRootCmd()
	var out, errOut bytes.Buffer
	root.SetOut(&out)
	root.SetErr(&errOut)
	root.SetArgs([]string{"--repo", repo, "--json"})
	if err := root.ExecuteContext(context.Background()); err != nil {
		t.Fatalf("bare root: %v\nstderr=%s", err, errOut.String())
	}
	got := decodeControlResult(t, out.Bytes())
	if got.Command != "status" || got.Health != controlHealthOff || got.Registered {
		t.Fatalf("unexpected bare root result: %+v", got)
	}
}

func decodeControlResult(t *testing.T, body []byte) controlResult {
	t.Helper()
	var envelope struct {
		OK         bool              `json:"ok"`
		State      productState      `json:"state"`
		Changed    bool              `json:"changed"`
		Actions    []productAction   `json:"actions"`
		NextAction *string           `json:"next_action"`
		Data       productStatusData `json:"data"`
	}
	if err := json.Unmarshal(body, &envelope); err != nil {
		t.Fatalf("decode control result: %v\n%s", err, body)
	}
	if envelope.Actions == nil {
		t.Fatalf("actions must encode as [] rather than null: %s", body)
	}
	health := string(envelope.State)
	if envelope.State == productStateProtected {
		health = controlHealthHealthy
	} else if envelope.State == productStateNeedsAction {
		health = controlHealthNeedsAttention
	}
	if envelope.Data.Summary == "The current directory is not inside a Git worktree." {
		health = controlHealthNotRepo
	}
	actions := make([]string, 0, len(envelope.Actions))
	for _, action := range envelope.Actions {
		actions = append(actions, action.Kind)
	}
	next := "No action needed."
	if envelope.NextAction != nil {
		next = *envelope.NextAction
	}
	got := controlResult{
		OK:      envelope.State != productStateNeedsAction && health != controlHealthNotRepo,
		Command: envelope.Data.Command, Repo: envelope.Data.Repo,
		Health: health, Summary: envelope.Data.Summary, NextAction: next,
		Registered: envelope.Data.Registered, Enabled: envelope.Data.Enabled,
		PendingEvents: envelope.Data.PendingEvents, BlockedEvents: envelope.Data.BlockedEvents,
		Changed: envelope.Changed, Actions: actions,
		StatePreserved: envelope.Data.StatePreserved,
		Protected:      envelope.Data.Protected, Published: envelope.Data.Published,
		CheckpointID: envelope.Data.CheckpointID,
	}
	return got
}
