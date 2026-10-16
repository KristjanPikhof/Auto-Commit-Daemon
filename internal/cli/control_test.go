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
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/checkpoint"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/installer"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/paths"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/supervisor"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/testutil"
)

func makeProtectedControlRepoStateDB(t *testing.T) (repoDir, stateDB string, db *state.DB) {
	t.Helper()
	repoDir, stateDB, db = makeSeededRepoStateDB(t)
	rec := central.RepoRecord{Path: repoDir, StateDB: stateDB, WorktreeID: checkpoint.WorktreeID(repoDir)}
	sum := finishStatusCheckpoint(t, rec, db)
	if !sum.Protected || sum.Partial {
		t.Fatal("fixture protection incomplete")
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
	repo := makeUnregisteredStartRepo(t)
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

func TestControlBareDisabledRepositoryReportsGitTruth(t *testing.T) {
	roots := withIsolatedHome(t)
	ctx := context.Background()
	repo, stateDB, db := makeProtectedControlRepoStateDB(t)
	registerProtectedControlRepo(t, roots, repo)
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if err := central.WithLock(roots, func(registry *central.Registry) error {
		result := registry.DisableRepo(central.RepoRemovalTarget{
			Path: repo, StateDB: stateDB,
		}, time.Now().Unix())
		if result.NotFound {
			return errors.New("registered repository was not found")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	var cleanOut bytes.Buffer
	if err := runControlStatus(ctx, &cleanOut, repo, true); err != nil {
		t.Fatalf("clean disabled status: %v", err)
	}
	clean := decodeControlResult(t, cleanOut.Bytes())
	if !clean.WorktreeClean || !clean.AllChangesCommittedInGit || !clean.Published {
		t.Fatalf("clean disabled status lost Git truth: %+v", clean)
	}

	if err := os.WriteFile(filepath.Join(repo, "uncommitted.txt"), []byte("dirty\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	var dirtyOut bytes.Buffer
	if err := runControlStatus(ctx, &dirtyOut, repo, true); err != nil {
		t.Fatalf("dirty disabled status: %v", err)
	}
	dirty := decodeControlResult(t, dirtyOut.Bytes())
	if dirty.WorktreeClean || dirty.AllChangesCommittedInGit || !dirty.Published || dirty.PublicationOutcome.WorktreeChanges != 1 {
		t.Fatalf("dirty disabled status hid Git changes: %+v", dirty)
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
	if got.OK || got.Health != controlHealthNeedsAttention || got.NextAction != "Run `acd on` to start protection." {
		t.Fatalf("unexpected dead-pid health: %+v", got)
	}
}

func TestApplyControlStatusNewObservationWaitsForProtection(t *testing.T) {
	status := statusReport{
		Daemon:                        "running",
		PID:                           os.Getpid(),
		CheckpointProtectionAvailable: true,
		ObservationEpoch:              7,
		CoveredEpoch:                  6,
	}
	result := controlResult{OK: true, Health: controlHealthHealthy}

	applyControlStatus(&result, status)

	if !result.OK || result.Health != controlHealthWaiting || result.Protected {
		t.Fatalf("new observation incorrectly claimed protected: %+v", result)
	}
	if result.NextAction != "No action needed." {
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
	if result.NextAction != "No action needed." {
		t.Fatalf("busy checkpoint next action=%q", result.NextAction)
	}
}

func TestApplyControlStatusRetiredPauseDoesNotStopSaving(t *testing.T) {
	report := statusReport{Daemon: "running", PID: os.Getpid(), Paused: true, Pause: &pauseInfo{Source: "manual"}, ObservationEpoch: 7, CoveredEpoch: 6}
	result := controlResult{}
	applyControlStatus(&result, report)
	if !result.OK || result.Health != controlHealthWaiting || result.Protected || !strings.Contains(result.Summary, "Saving") {
		t.Fatalf("legacy publication pause blocked protection: %+v", result)
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
	seq, err := testutil.SeedLegacyCapture(ctx, db, state.CaptureEvent{
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
	if err := seedLegacyBlockedCapture(ctx, db, seq, "before-state mismatch", nowFloat(),
		sqlNullStr("refs/heads/old-topic"), sql.NullInt64{Int64: 7, Valid: true}, sqlNullStr("deadbeef")); err != nil {
		t.Fatalf("mark historical conflict: %v", err)
	}
	failedSeq, err := testutil.SeedLegacyCapture(ctx, db, state.CaptureEvent{
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
	if err := testutil.SeedLegacyCaptureOutcome(ctx, db, failedSeq, state.EventStateFailed,
		sql.NullString{}, sqlNullStr("provider failed"), sql.NullString{}, nowFloat()); err != nil {
		t.Fatalf("mark historical failure: %v", err)
	}
	if _, err := testutil.SeedLegacyCapture(ctx, db, state.CaptureEvent{
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
	if !got.OK || !got.Protected || got.BlockedEvents != 0 || got.PublicationOutcome.Legacy.State != "needs_attention" || got.NextAction != "No action needed." {
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
			seq, err := testutil.SeedLegacyCapture(ctx, db, state.CaptureEvent{
				BranchRef: "refs/heads/main", BranchGeneration: 1,
				BaseHead: "deadbeef", Operation: "modify", Path: "stuck.go",
				Fidelity: "exact",
			}, []state.CaptureOp{{Op: "modify", Path: "stuck.go", Fidelity: "exact"}})
			if err != nil {
				t.Fatalf("append terminal event: %v", err)
			}
			switch terminalState {
			case state.EventStateBlockedConflict:
				err = seedLegacyBlockedCapture(ctx, db, seq, "before-state mismatch", nowFloat(),
					sqlNullStr("refs/heads/main"), sql.NullInt64{Int64: 1, Valid: true}, sqlNullStr("deadbeef"))
			case state.EventStateFailed:
				err = testutil.SeedLegacyCaptureOutcome(ctx, db, seq, state.EventStateFailed,
					sql.NullString{}, sqlNullStr("provider failed"), sql.NullString{}, nowFloat())
			}
			if err != nil {
				t.Fatalf("mark terminal event: %v", err)
			}
			if err := db.Close(); err != nil {
				t.Fatalf("close state DB: %v", err)
			}

			var out bytes.Buffer
			if err := runControlStatus(ctx, &out, repo, true); err != nil {
				t.Fatalf("legacy attention blocked checkpoint protection: %v", err)
			}
			got := decodeControlResult(t, out.Bytes())
			if !got.OK || !got.Protected || got.PublicationOutcome.Legacy.State != "needs_attention" || got.NextAction != "No action needed." {
				t.Fatalf("legacy evidence or protection hidden: %+v", got)
			}
			var stored string
			conn, err := openStateDBReadOnly(ctx, state.DBPathFromGitDir(filepath.Join(repo, ".git")))
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			if err := conn.QueryRowContext(ctx, "SELECT state FROM capture_events WHERE seq=?", seq).Scan(&stored); err != nil || stored != terminalState {
				t.Fatalf("inspection changed legacy state %q: %v", stored, err)
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
	seq, err := testutil.SeedLegacyCapture(ctx, db, state.CaptureEvent{
		BranchRef: "refs/heads/main", BranchGeneration: 1,
		BaseHead: "deadbeef", Operation: "modify", Path: "stuck.go", Fidelity: "exact",
	}, []state.CaptureOp{{Op: "modify", Path: "stuck.go", Fidelity: "exact"}})
	if err != nil {
		t.Fatalf("append terminal event: %v", err)
	}
	if err := seedLegacyBlockedCapture(ctx, db, seq, "before-state mismatch", nowFloat(),
		sqlNullStr("refs/heads/main"), sql.NullInt64{Int64: 1, Valid: true}, sqlNullStr("deadbeef")); err != nil {
		t.Fatalf("mark terminal event: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close state DB: %v", err)
	}

	var out bytes.Buffer
	err = runControlOn(ctx, &out, repo, true)
	if ExitCode(err) != ExitActionRequired || !strings.Contains(err.Error(), "setup") {
		t.Fatalf("runControlOn exit=%d err=%v want setup-required result", ExitCode(err), err)
	}
	if out.Len() != 0 {
		t.Fatalf("v19 on mutated before cutover: output=%q", out.String())
	}
}

func TestApplyControlStatusStorageWarningDoesNotGateCurrentWorkOnLegacyArchive(t *testing.T) {
	report := statusReport{Daemon: "running", PID: os.Getpid(), Protected: true, CheckpointRetentionOverBudget: true, PublicationOutcome: publicationOutcome{Legacy: state.FinishLegacyCutover{State: "needs_attention", Reason: "Saved legacy proof needs inspection"}}}
	result := controlResult{}
	applyControlStatus(&result, report)
	if !result.OK || !result.Protected || result.Health != controlHealthNeedsAttention || !strings.Contains(result.NextAction, "acd doctor") || !strings.Contains(result.Summary, "Legacy work remains preserved") || strings.Count(result.Summary, "exceed the storage budget") != 1 {
		t.Fatalf("maintenance hid remediation or duplicated output: %+v", result)
	}
}

func TestControlOnFreshSetupFailurePreservesRepository(t *testing.T) {
	roots := withIsolatedHome(t)
	repo := makeUnregisteredStartRepo(t)
	lookup := controlRepoLookup{Roots: roots}
	lookup.Worktree.Root = repo
	err := bootstrapControlSetupWith(t.Context(), lookup,
		func(context.Context, paths.Roots, installer.Options) (installer.Plan, error) {
			return installer.Plan{FreshDefaults: true}, nil
		},
		func(context.Context, paths.Roots, installer.Plan, installer.ApplyOptions) (installer.Result, error) {
			return installer.Result{}, errors.New("owned installation unavailable")
		})
	if err == nil || !strings.Contains(err.Error(), "repository was not enabled") {
		t.Fatalf("failed bootstrap: %v", err)
	}
	for _, path := range []string{state.DBPathFromGitDir(filepath.Join(repo, ".git")), roots.RegistryPath()} {
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Fatalf("failed bootstrap created %s: %v", path, err)
		}
	}
}

func TestControlOnMismatchedRuntimeRefusesBeforeActivation(t *testing.T) {
	roots := withIsolatedHome(t)
	repo := makeUnregisteredStartRepo(t)
	if err := os.MkdirAll(filepath.Dir(roots.ManagedBinaryPath()), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(roots.ManagedBinaryPath(), []byte("older-acd"), 0o755); err != nil {
		t.Fatal(err)
	}
	err := runControlOn(context.Background(), io.Discard, repo, true)
	if ExitCode(err) != ExitActionRequired || !strings.Contains(err.Error(), "acd setup") {
		t.Fatalf("mismatched runtime exit=%d err=%v", ExitCode(err), err)
	}
	if _, err := os.Stat(state.DBPathFromGitDir(filepath.Join(repo, ".git"))); !os.IsNotExist(err) {
		t.Fatalf("acd on created repository state before runtime validation: %v", err)
	}
	if _, err := os.Stat(roots.RegistryPath()); !os.IsNotExist(err) {
		t.Fatalf("acd on created registry before runtime validation: %v", err)
	}
}

func TestControlOffDisablesStopsPreservesAndIsIdempotent(t *testing.T) {
	withIsolatedHome(t)
	repo := makeUnregisteredStartRepo(t)
	var offOut bytes.Buffer
	if err := runControlOffWithForce(context.Background(), &offOut, repo, true, false); err != nil {
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
	repo := makeUnregisteredStartRepo(t)

	var out bytes.Buffer
	if err := runControlOffWithForce(context.Background(), &out, repo, true, false); err != nil {
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
	repo := makeUnregisteredStartRepo(t)
	commitStartRepoSeed(t, repo)
	if _, err := git.Run(ctx, git.RunOpts{Dir: repo}, "checkout", "--detach", "--quiet"); err != nil {
		t.Fatalf("detach HEAD: %v", err)
	}

	var out bytes.Buffer
	if err := runControlOffWithForce(ctx, &out, repo, true, false); err != nil {
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

func TestApplySupervisorWorkerFailureProvidesRepairThatMatchesCause(t *testing.T) {
	tests := []struct {
		name    string
		error   string
		summary string
		next    string
	}{
		{
			name:    "safe schema upgrade",
			error:   "acd worker: open /repo: state: checkpoint-first setup required: state database schema is v20, want v21",
			summary: "saved data needs an upgrade",
			next:    "Run acd setup",
		},
		{
			name:    "old owner",
			error:   "acd worker: acquire repository ownership: already owned",
			summary: "older ACD process",
			next:    "Run acd on",
		},
		{
			name:    "unknown startup failure",
			error:   "acd worker: provider configuration is invalid",
			summary: "could not restart protection",
			next:    "acd support logs --lines 100",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result := controlResult{OK: true, Health: controlHealthHealthy}
			applySupervisorWorkerFailure(&result, supervisor.WorkerStatus{
				State: "needs_action", Restarts: 5, LastError: test.error,
			})
			if result.OK || result.Health != controlHealthNeedsAttention ||
				!strings.Contains(result.Summary, test.summary) ||
				!strings.Contains(result.NextAction, test.next) {
				t.Fatalf("result=%+v", result)
			}
		})
	}
}

func TestRenderProductDoctorWorkerShowsCauseAndFix(t *testing.T) {
	var out bytes.Buffer
	renderProductDoctorWorker(&out, controlResult{
		SupervisorWorkerState: "needs_action", SupervisorWorkerRestarts: 5,
		SupervisorWorkerError: "schema is v20, want v21",
		NextAction:            "Run `acd on` to repair it.",
	})
	for _, want := range []string{
		"State: needs action after 5 restart attempt(s)",
		"Cause: schema is v20, want v21",
		"Fix: Run `acd on` to repair it.",
	} {
		if !strings.Contains(out.String(), want) {
			t.Fatalf("doctor output missing %q:\n%s", want, out.String())
		}
	}
}

func TestControlBareHumanAnswersProtectionQuestions(t *testing.T) {
	_ = withIsolatedHome(t)
	repo := makeUnregisteredStartRepo(t)

	var out bytes.Buffer
	if err := runControlStatus(context.Background(), &out, repo, false); err != nil {
		t.Fatalf("runControlStatus: %v", err)
	}
	for _, line := range []string{"Protection:", "Git commits:", "Optional:"} {
		if got := strings.Count(out.String(), line); got != 1 {
			t.Fatalf("%s line count=%d\n%s", line, got, out.String())
		}
	}
}

func TestControlRootBareUsesReadOnlyHealth(t *testing.T) {
	_ = withIsolatedHome(t)
	repo := makeUnregisteredStartRepo(t)
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
	if envelope.Data.Summary == "This directory is not inside a Git repository." {
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
		StatePreserved:           envelope.Data.StatePreserved,
		Protected:                envelope.Data.Protected,
		PublicationOutcome:       envelope.Data.PublicationOutcome,
		CaptureHealth:            envelope.Data.CaptureHealth,
		PublicationProgress:      envelope.Data.PublicationProgress,
		Daemon:                   envelope.Data.Worker,
		Published:                envelope.Data.Published,
		WorktreeClean:            envelope.Data.WorktreeClean,
		AllChangesCommittedInGit: envelope.Data.AllChangesCommittedInGit,
		CheckpointPublishedByACD: envelope.Data.CheckpointPublishedByACD,
		CheckpointID:             envelope.Data.CheckpointID,
	}
	return got
}

func TestControlDisabledMissingDatabaseKeepsReadOnlyGitTruth(t *testing.T) {
	roots := withIsolatedHome(t)
	repo, path, db := makeProtectedControlRepoStateDB(t)
	registerProtectedControlRepo(t, roots, repo)
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if err := central.WithLock(roots, func(reg *central.Registry) error {
		reg.DisableRepo(central.RepoRemovalTarget{Path: repo, StateDB: path}, time.Now().Unix())
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	for _, dirty := range []bool{false, true} {
		if dirty {
			if err := os.WriteFile(filepath.Join(repo, "later.txt"), []byte("later"), 0600); err != nil {
				t.Fatal(err)
			}
		}
		res, err := inspectControl(t.Context(), repo)
		if err != nil || !res.OK || res.Health != controlHealthOff || res.WorktreeClean == dirty || res.AllChangesCommittedInGit == dirty || res.Protected || res.Published || res.PublicationOutcome.BranchCommitted != nil {
			t.Fatalf("missing DB invented protection or lost Git truth: %+v %v", res, err)
		}
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Fatalf("status recreated missing DB: %v", err)
		}
	}
}
