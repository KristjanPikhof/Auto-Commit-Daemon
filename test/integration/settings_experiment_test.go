//go:build integration
// +build integration

package integration_test

import (
	"context"
	"database/sql"
	"fmt"
	"net/http"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

func TestSettingsExperimentExactlyTenWindowsRestartAndRevert(t *testing.T) {
	repo := tempRepo(t)
	env := withIsolatedHome(t)
	var hits atomic.Int32
	server, trustEnv := newOpenAITestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeSettingsPlannerRequest(t, r)
		seqs := offeredIntentSeqs(t, req.intentChatRequest)
		hits.Add(1)
		writeSettingsPlannerResponse(t, w, req.Model, seqs)
	}))
	defer server.Close()
	extra := settingsRuntimeEnv(server.URL, trustEnv)
	fullEnv := envWith(env, extra...)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	startSession(t, ctx, env, repo, "settings-exp-ten-a", "shell", extra...)
	t.Cleanup(func() { stopSessionForce(t, fullEnv, repo) })
	waitMode(t, repo, "running", 5*time.Second)
	db := openSettingsRuntimeDB(t, repo)

	baseline, _ := queueSettingsRevision(t, db, sql.NullInt64{}, "baseline-model", 1, nil)
	wakeSession(t, ctx, fullEnv, repo, "settings-exp-ten-a")
	waitSettingsApplied(t, db, baseline.ID, 8*time.Second)
	candidate, _ := queueSettingsRevision(t, db, sql.NullInt64{Int64: baseline.ID, Valid: true}, "candidate-model", 2, nil)
	experiment, err := state.CreateConfigExperiment(ctx, db, state.ConfigExperimentInput{BaselineRevisionID: baseline.ID, CandidateRevisionID: candidate.ID, WindowBudget: 10, FailurePolicy: "continue"})
	if err != nil {
		t.Fatalf("create ten-window experiment: %v", err)
	}
	wakeSession(t, ctx, fullEnv, repo, "settings-exp-ten-a")
	waitSettingsApplied(t, db, candidate.ID, 8*time.Second)
	startCommits := commitCount(t, repo)
	dbPath := filepath.Join(repo, ".git", "acd", "state.db")

	sessionID := "settings-exp-ten-a"
	for window := 1; window <= 10; window++ {
		name := fmt.Sprintf("experiment-%02d.txt", window)
		writeFile(t, filepath.Join(repo, name), fmt.Sprintf("window %d\n", window))
		wakeSession(t, ctx, fullEnv, repo, sessionID)
		waitForEventState(t, dbPath, name, "published", 12*time.Second)
		waitExperimentWindows(t, db, experiment.ID, window, 8*time.Second)
		if window == 4 {
			stopSessionForce(t, fullEnv, repo)
			waitMode(t, repo, "stopped", 5*time.Second)
			sessionID = "settings-exp-ten-b"
			startSession(t, ctx, env, repo, sessionID, "shell", extra...)
			waitMode(t, repo, "running", 5*time.Second)
			waitSettingsApplied(t, db, candidate.ID, 8*time.Second)
		}
	}

	waitExperimentStatus(t, db, experiment.ID, state.ExperimentCompleted, 8*time.Second)
	waitFor(t, "baseline-derived revert applied", 12*time.Second, func() bool {
		projection, err := state.RuntimeConfigActivationState(ctx, db)
		return err == nil && projection.AppliedRevisionID.Valid && projection.AppliedRevisionID.Int64 > candidate.ID && projection.DesiredRevisionID.Int64 == projection.AppliedRevisionID.Int64
	})
	projection, _ := state.RuntimeConfigActivationState(ctx, db)
	revert, err := state.ConfigRevisionByID(ctx, db, projection.AppliedRevisionID.Int64)
	if err != nil {
		t.Fatal(err)
	}
	if revert.ID == baseline.ID || revert.ID == candidate.ID || revert.SnapshotJSON != baseline.SnapshotJSON {
		t.Fatalf("revert=%+v baseline=%+v", revert, baseline)
	}
	if got := sqliteScalar(t, dbPath, fmt.Sprintf("SELECT completed_windows || '|' || window_budget || '|' || status FROM config_experiments WHERE id=%d", experiment.ID)); got != "10|10|completed" {
		t.Fatalf("experiment row=%s", got)
	}
	if got := sqliteScalar(t, dbPath, fmt.Sprintf("SELECT COUNT(*) FROM intent_planner_windows WHERE experiment_id=%d AND experiment_consumed=1", experiment.ID)); got != "10" {
		t.Fatalf("consumed planner windows=%s want 10", got)
	}
	if got := commitCount(t, repo); got != startCommits+10 {
		t.Fatalf("commit count=%d want %d", got, startCommits+10)
	}
	for window := 1; window <= 10; window++ {
		name := fmt.Sprintf("experiment-%02d.txt", window)
		if history, err := runGit(repo, "log", "--format=%H", "--", name); err != nil || strings.TrimSpace(history) == "" {
			t.Fatalf("experiment-created commit missing for %s", name)
		}
	}
	if hits.Load() != 10 {
		t.Fatalf("HTTPS mock hits=%d want exactly 10", hits.Load())
	}
}

func TestSettingsExperimentExpiryWithNoPendingWork(t *testing.T) {
	repo := tempRepo(t)
	env := withIsolatedHome(t)
	server, trustEnv := newOpenAITestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeSettingsPlannerRequest(t, r)
		writeSettingsPlannerResponse(t, w, req.Model, offeredIntentSeqs(t, req.intentChatRequest))
	}))
	defer server.Close()
	extra := settingsRuntimeEnv(server.URL, trustEnv)
	fullEnv := envWith(env, extra...)
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	startSession(t, ctx, env, repo, "settings-exp-expiry", "shell", extra...)
	t.Cleanup(func() { stopSessionForce(t, fullEnv, repo) })
	waitMode(t, repo, "running", 5*time.Second)
	db := openSettingsRuntimeDB(t, repo)
	baseline, _ := queueSettingsRevision(t, db, sql.NullInt64{}, "expiry-baseline", 1, nil)
	wakeSession(t, ctx, fullEnv, repo, "settings-exp-expiry")
	waitSettingsApplied(t, db, baseline.ID, 8*time.Second)
	candidate, _ := queueSettingsRevision(t, db, sql.NullInt64{Int64: baseline.ID, Valid: true}, "expiry-candidate", 2, nil)
	experiment, err := state.CreateConfigExperiment(ctx, db, state.ConfigExperimentInput{BaselineRevisionID: baseline.ID, CandidateRevisionID: candidate.ID, WindowBudget: 10, ExpiresTS: sql.NullFloat64{Float64: float64(time.Now().Add(1200*time.Millisecond).UnixNano()) / 1e9, Valid: true}, FailurePolicy: "continue"})
	if err != nil {
		t.Fatal(err)
	}
	wakeSession(t, ctx, fullEnv, repo, "settings-exp-expiry")
	waitSettingsApplied(t, db, candidate.ID, 8*time.Second)
	commits := commitCount(t, repo)
	waitExperimentStatus(t, db, experiment.ID, state.ExperimentExpired, 12*time.Second)
	waitFor(t, "expired baseline revert applied", 12*time.Second, func() bool {
		projection, err := state.RuntimeConfigActivationState(ctx, db)
		return err == nil && projection.AppliedRevisionID.Valid && projection.AppliedRevisionID.Int64 > candidate.ID
	})
	if got := commitCount(t, repo); got != commits {
		t.Fatalf("expiry changed commits: before=%d after=%d", commits, got)
	}
	if got := sqliteScalar(t, filepath.Join(repo, ".git", "acd", "state.db"), fmt.Sprintf("SELECT completed_windows FROM config_experiments WHERE id=%d", experiment.ID)); got != "0" {
		t.Fatalf("idle expiry consumed windows=%s", got)
	}
}

func TestSettingsExperimentProviderErrorPolicyRevertsAndKeepsCommit(t *testing.T) {
	repo := tempRepo(t)
	env := withIsolatedHome(t)
	server, trustEnv := newOpenAITestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeSettingsPlannerRequest(t, r)
		if writeIntentMessageRewriteResponse(t, w, req.intentChatRequest) {
			return
		}
		if req.Model == "error-candidate" {
			http.Error(w, `{"error":{"message":"synthetic failure"}}`, http.StatusInternalServerError)
			return
		}
		writeSettingsPlannerResponse(t, w, req.Model, offeredIntentSeqs(t, req.intentChatRequest))
	}))
	defer server.Close()
	extra := settingsRuntimeEnv(server.URL, trustEnv)
	fullEnv := envWith(env, extra...)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Second)
	defer cancel()
	startSession(t, ctx, env, repo, "settings-exp-error", "shell", extra...)
	t.Cleanup(func() { stopSessionForce(t, fullEnv, repo) })
	waitMode(t, repo, "running", 5*time.Second)
	db := openSettingsRuntimeDB(t, repo)
	baseline, _ := queueSettingsRevision(t, db, sql.NullInt64{}, "error-baseline", 1, nil)
	wakeSession(t, ctx, fullEnv, repo, "settings-exp-error")
	waitSettingsApplied(t, db, baseline.ID, 8*time.Second)
	candidate, _ := queueSettingsRevision(t, db, sql.NullInt64{Int64: baseline.ID, Valid: true}, "error-candidate", 2, nil)
	experiment, err := state.CreateConfigExperiment(ctx, db, state.ConfigExperimentInput{BaselineRevisionID: baseline.ID, CandidateRevisionID: candidate.ID, WindowBudget: 10, FailurePolicy: "revert"})
	if err != nil {
		t.Fatal(err)
	}
	wakeSession(t, ctx, fullEnv, repo, "settings-exp-error")
	waitSettingsApplied(t, db, candidate.ID, 8*time.Second)
	writeFile(t, filepath.Join(repo, "experiment-error.txt"), "fallback commit remains\n")
	wakeSession(t, ctx, fullEnv, repo, "settings-exp-error")
	waitForEventState(t, filepath.Join(repo, ".git", "acd", "state.db"), "experiment-error.txt", "published", 12*time.Second)
	waitExperimentStatus(t, db, experiment.ID, state.ExperimentFailed, 8*time.Second)
	waitFor(t, "provider-error baseline revert applied", 12*time.Second, func() bool {
		projection, err := state.RuntimeConfigActivationState(ctx, db)
		return err == nil && projection.AppliedRevisionID.Valid && projection.AppliedRevisionID.Int64 > candidate.ID
	})
	if strings.TrimSpace(runGitOK(t, repo, "log", "--format=%H", "--", "experiment-error.txt")) == "" {
		t.Fatal("provider-error fallback commit was removed by experiment revert")
	}
}

func waitExperimentWindows(t *testing.T, db *state.DB, id int64, windows int, timeout time.Duration) {
	t.Helper()
	waitFor(t, fmt.Sprintf("experiment %d windows=%d", id, windows), timeout, func() bool {
		experiment, err := state.ConfigExperimentByID(context.Background(), db, id)
		return err == nil && experiment.CompletedWindows == windows
	})
}

func waitExperimentStatus(t *testing.T, db *state.DB, id int64, status string, timeout time.Duration) {
	t.Helper()
	waitFor(t, fmt.Sprintf("experiment %d status=%s", id, status), timeout, func() bool {
		experiment, err := state.ConfigExperimentByID(context.Background(), db, id)
		return err == nil && experiment.Status == status
	})
}
