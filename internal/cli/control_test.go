package cli

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/central"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/daemon"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

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

func TestControlBareNonRepoReturnsClassification(t *testing.T) {
	_ = withIsolatedHome(t)
	nonRepo := t.TempDir()

	var out bytes.Buffer
	if err := runControlStatus(context.Background(), &out, nonRepo, true); err != nil {
		t.Fatalf("runControlStatus: %v", err)
	}
	got := decodeControlResult(t, out.Bytes())
	if got.OK || got.Health != controlHealthNotRepo || got.NextAction == "" {
		t.Fatalf("unexpected non-repo health: %+v", got)
	}
}

func TestControlBareFreshHeartbeatWithDeadPIDNeedsAttention(t *testing.T) {
	roots := withIsolatedHome(t)
	ctx := context.Background()
	repo, stateDB, db := makeRepoStateDB(t)
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

	var out bytes.Buffer
	if err := runControlStatus(ctx, &out, repo, true); err != nil {
		t.Fatalf("runControlStatus: %v", err)
	}
	got := decodeControlResult(t, out.Bytes())
	if got.OK || got.Health != controlHealthNeedsAttention || got.NextAction != "Run `acd on` to start it." {
		t.Fatalf("unexpected dead-pid health: %+v", got)
	}
}

func TestControlBareIgnoresHistoricalInactiveTerminalEvents(t *testing.T) {
	roots := withIsolatedHome(t)
	ctx := context.Background()
	repo, stateDB, db := makeRepoStateDB(t)
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
			repo, stateDB, db := makeRepoStateDB(t)
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
			if err := runControlStatus(ctx, &out, repo, true); err != nil {
				t.Fatalf("runControlStatus: %v", err)
			}
			got := decodeControlResult(t, out.Bytes())
			if got.OK || got.Health != controlHealthNeedsAttention || !strings.Contains(got.Summary, "terminal replay") {
				t.Fatalf("active tail terminal event was not surfaced: %+v", got)
			}
		})
	}
}

func TestControlOnReturnsErrorAfterRenderingUnhealthyResult(t *testing.T) {
	roots := withIsolatedHome(t)
	ctx := context.Background()
	repo, stateDB, db := makeRepoStateDB(t)
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

	var out bytes.Buffer
	err = runControlOn(ctx, &out, repo, true)
	if err == nil || !strings.Contains(err.Error(), "repository remains unhealthy") {
		t.Fatalf("runControlOn err=%v want unhealthy result", err)
	}
	got := decodeControlResult(t, out.Bytes())
	if got.OK || got.Command != "on" || got.Health != controlHealthNeedsAttention {
		t.Fatalf("unhealthy diagnostic was not rendered: %+v", got)
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

func TestApplyControlStatusPlannerCircuitRespectsBlockerPrecedence(t *testing.T) {
	health := &daemon.IntentPlannerHealthSnapshot{State: daemon.IntentPlannerCircuitOpen}
	for _, tc := range []struct {
		name   string
		mutate func(*statusReport)
		want   string
	}{
		{name: "pause", mutate: func(s *statusReport) { s.Paused = true }, want: "paused"},
		{name: "backpressure", mutate: func(s *statusReport) { s.BackpressurePaused = true }, want: "backpressure"},
		{name: "barrier", mutate: func(s *statusReport) { s.ActiveBarriers = 1 }, want: "terminal replay"},
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
	roots := withIsolatedHome(t)
	repo := makeStartRepo(t)
	spawnCount, restoreSpawn := installFakeSpawn(t, os.Getpid())
	t.Cleanup(restoreSpawn)

	var firstOut bytes.Buffer
	if err := runControlOn(context.Background(), &firstOut, repo, true); err != nil {
		t.Fatalf("first runControlOn: %v", err)
	}
	first := decodeControlResult(t, firstOut.Bytes())
	if !first.OK || first.Health != controlHealthHealthy || !first.Registered || !first.Enabled || !first.Changed {
		t.Fatalf("unexpected first on result: %+v", first)
	}
	if want := []string{"registered", "started"}; !reflect.DeepEqual(first.Actions, want) {
		t.Fatalf("actions=%v want=%v", first.Actions, want)
	}
	if !first.StatePreserved || spawnCount.Load() != 1 {
		t.Fatalf("state_preserved=%v spawn_count=%d", first.StatePreserved, spawnCount.Load())
	}
	reg, err := central.Load(roots)
	if err != nil {
		t.Fatalf("load registry: %v", err)
	}
	if rec, ok := reg.FindRepo(repo, state.DBPathFromGitDir(filepath.Join(repo, ".git"))); !ok || rec.LifecycleDisabled() {
		t.Fatalf("on did not leave enabled registry row: ok=%v rec=%+v", ok, rec)
	}

	var secondOut bytes.Buffer
	if err := runControlOn(context.Background(), &secondOut, repo, true); err != nil {
		t.Fatalf("second runControlOn: %v", err)
	}
	second := decodeControlResult(t, secondOut.Bytes())
	if second.Changed || len(second.Actions) != 0 || second.Health != controlHealthHealthy {
		t.Fatalf("second on was not idempotent: %+v", second)
	}
	if spawnCount.Load() != 1 {
		t.Fatalf("spawn_count=%d want=1", spawnCount.Load())
	}

	var thirdOut bytes.Buffer
	if err := runControlOn(context.Background(), &thirdOut, repo, true); err != nil {
		t.Fatalf("third runControlOn: %v", err)
	}
	if secondOut.String() != thirdOut.String() {
		t.Fatalf("idempotent JSON changed\nsecond=%s\nthird=%s", secondOut.String(), thirdOut.String())
	}
}

func TestControlOffDisablesStopsPreservesAndIsIdempotent(t *testing.T) {
	roots := withIsolatedHome(t)
	repo := makeStartRepo(t)
	spawnCount, restoreSpawn := installFakeSpawn(t, os.Getpid())
	t.Cleanup(restoreSpawn)

	if err := runControlOn(context.Background(), io.Discard, repo, true); err != nil {
		t.Fatalf("seed on: %v", err)
	}

	previousStop := repoDisableStopOneRepo
	repoDisableStopOneRepo = func(ctx context.Context, repoPath, sessionID string, force bool) (stopRepoResult, error) {
		db, err := state.Open(ctx, state.DBPathFromGitDir(filepath.Join(repoPath, ".git")))
		if err != nil {
			return stopRepoResult{}, err
		}
		defer db.Close()
		if err := state.SaveDaemonState(ctx, db, state.DaemonState{Mode: "stopped", UpdatedTS: nowFloat()}); err != nil {
			return stopRepoResult{}, err
		}
		return stopRepoResult{Repo: repoPath, Stopped: true, Force: force, DaemonPID: os.Getpid()}, nil
	}
	t.Cleanup(func() { repoDisableStopOneRepo = previousStop })

	var firstOut bytes.Buffer
	if err := runControlOff(context.Background(), &firstOut, repo, true); err != nil {
		t.Fatalf("first runControlOff: %v", err)
	}
	first := decodeControlResult(t, firstOut.Bytes())
	if !first.OK || first.Health != controlHealthOff || first.Enabled || !first.Registered || !first.Changed || !first.StatePreserved {
		t.Fatalf("unexpected first off result: %+v", first)
	}
	if want := []string{"disabled", "stopped"}; !reflect.DeepEqual(first.Actions, want) {
		t.Fatalf("actions=%v want=%v", first.Actions, want)
	}
	if _, err := os.Stat(state.DBPathFromGitDir(filepath.Join(repo, ".git"))); err != nil {
		t.Fatalf("off did not preserve state DB: %v", err)
	}
	reg, err := central.Load(roots)
	if err != nil {
		t.Fatalf("load registry: %v", err)
	}
	rec, ok := reg.FindRepo(repo, state.DBPathFromGitDir(filepath.Join(repo, ".git")))
	if !ok || !rec.LifecycleDisabled() {
		t.Fatalf("off did not leave disabled registry row: ok=%v rec=%+v", ok, rec)
	}

	var secondOut bytes.Buffer
	if err := runControlOff(context.Background(), &secondOut, repo, true); err != nil {
		t.Fatalf("second runControlOff: %v", err)
	}
	second := decodeControlResult(t, secondOut.Bytes())
	if second.Changed || len(second.Actions) != 0 || second.Health != controlHealthOff {
		t.Fatalf("second off was not idempotent: %+v", second)
	}

	var onOut bytes.Buffer
	if err := runControlOn(context.Background(), &onOut, repo, true); err != nil {
		t.Fatalf("runControlOn after off: %v", err)
	}
	on := decodeControlResult(t, onOut.Bytes())
	if want := []string{"enabled", "started"}; !reflect.DeepEqual(on.Actions, want) {
		t.Fatalf("on actions=%v want=%v", on.Actions, want)
	}
	if !on.Enabled || on.Health != controlHealthHealthy || spawnCount.Load() != 2 {
		t.Fatalf("unexpected re-enabled result=%+v spawn_count=%d", on, spawnCount.Load())
	}
}

func TestControlOnRestartsFreshLookingDeadDaemon(t *testing.T) {
	roots := withIsolatedHome(t)
	ctx := context.Background()
	repo, stateDB, db := makeRepoStateDB(t)
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
	spawnCount, restoreSpawn := installFakeSpawn(t, os.Getpid())
	t.Cleanup(restoreSpawn)

	var out bytes.Buffer
	if err := runControlOn(ctx, &out, repo, true); err != nil {
		t.Fatalf("runControlOn: %v", err)
	}
	got := decodeControlResult(t, out.Bytes())
	if want := []string{"started"}; !reflect.DeepEqual(got.Actions, want) {
		t.Fatalf("actions=%v want=%v", got.Actions, want)
	}
	if !got.Changed || got.Health != controlHealthHealthy || spawnCount.Load() != 1 {
		t.Fatalf("unexpected restart result=%+v spawn_count=%d", got, spawnCount.Load())
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
	if want := []string{"registered", "disabled"}; !reflect.DeepEqual(got.Actions, want) {
		t.Fatalf("actions=%v want=%v", got.Actions, want)
	}
	reg, err := central.Load(roots)
	if err != nil {
		t.Fatalf("load registry: %v", err)
	}
	rec, ok := reg.FindRepo(repo, state.DBPathFromGitDir(filepath.Join(repo, ".git")))
	if !ok || !rec.LifecycleDisabled() {
		t.Fatalf("unknown off was not durable: ok=%v rec=%+v", ok, rec)
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
	if got.Health != controlHealthOff || !reflect.DeepEqual(got.Actions, []string{"registered", "disabled"}) {
		t.Fatalf("unexpected detached off result: %+v", got)
	}
	reg, err := central.Load(roots)
	if err != nil {
		t.Fatalf("load registry: %v", err)
	}
	rec, ok := reg.FindRepo(repo, state.DBPathFromGitDir(filepath.Join(repo, ".git")))
	if !ok || !rec.LifecycleDisabled() {
		t.Fatalf("detached off was not durable: ok=%v rec=%+v", ok, rec)
	}
	if _, err := os.Stat(state.DBPathFromGitDir(filepath.Join(repo, ".git"))); !os.IsNotExist(err) {
		t.Fatalf("detached off should not initialize state DB: err=%v", err)
	}
}

func TestControlBareHumanHasOneHealthAndNextLine(t *testing.T) {
	_ = withIsolatedHome(t)
	repo := makeStartRepo(t)

	var out bytes.Buffer
	if err := runControlStatus(context.Background(), &out, repo, false); err != nil {
		t.Fatalf("runControlStatus: %v", err)
	}
	if got := strings.Count(out.String(), "Health:"); got != 1 {
		t.Fatalf("Health line count=%d\n%s", got, out.String())
	}
	if got := strings.Count(out.String(), "Next:"); got != 1 {
		t.Fatalf("Next line count=%d\n%s", got, out.String())
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
	var got controlResult
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatalf("decode control result: %v\n%s", err, body)
	}
	if got.Actions == nil {
		t.Fatalf("actions must encode as [] rather than null: %s", body)
	}
	return got
}
