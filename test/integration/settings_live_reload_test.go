//go:build integration
// +build integration

package integration_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

type settingsPlannerHit struct {
	Model string
	Seqs  []int64
}

func TestSettingsLiveReloadInFlightAThenNextB(t *testing.T) {
	repo := tempRepo(t)
	env := withIsolatedHome(t)
	var mu sync.Mutex
	var hits []settingsPlannerHit
	firstA := make(chan struct{})
	releaseA := make(chan struct{})
	server, trustEnv := newOpenAITestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeSettingsPlannerRequest(t, r)
		seqs := offeredIntentSeqs(t, req.intentChatRequest)
		mu.Lock()
		hits = append(hits, settingsPlannerHit{Model: req.Model, Seqs: append([]int64(nil), seqs...)})
		index := len(hits)
		mu.Unlock()
		if req.Model == "model-a" && index == 1 {
			close(firstA)
			<-releaseA
		}
		writeSettingsPlannerResponse(t, w, req.Model, seqs)
	}))
	defer server.Close()

	extra := settingsRuntimeEnv(server.URL, trustEnv)
	extra = activateIntentV2Runtime(t, repo, extra...)
	fullEnv := envWith(env, extra...)
	ctx, cancel := context.WithTimeout(context.Background(), 75*time.Second)
	defer cancel()
	startSession(t, ctx, env, repo, "settings-live-ab", "shell", extra...)
	t.Cleanup(func() { stopSessionForce(t, fullEnv, repo) })
	waitMode(t, repo, "running", 5*time.Second)
	db := openSettingsRuntimeDB(t, repo)
	a, aReq := queueSettingsRevision(t, db, sql.NullInt64{}, "model-a", 1, nil)
	wakeSession(t, ctx, fullEnv, repo, "settings-live-ab")
	waitSettingsApplied(t, db, a.ID, 8*time.Second)

	writeFile(t, filepath.Join(repo, "revision-a.txt"), "revision A\n")
	wakeSession(t, ctx, fullEnv, repo, "settings-live-ab")
	select {
	case <-firstA:
	case <-time.After(10 * time.Second):
		t.Fatal("model A planner request never entered HTTPS mock")
	}
	b, bReq := queueSettingsRevision(t, db, sql.NullInt64{Int64: a.ID, Valid: true}, "model-b", 2, nil)
	wakeSession(t, ctx, fullEnv, repo, "settings-live-ab")
	close(releaseA)
	waitForEventState(t, filepath.Join(repo, ".git", "acd", "state.db"), "revision-a.txt", "published", 12*time.Second)
	waitSettingsApplied(t, db, b.ID, 12*time.Second)

	writeFile(t, filepath.Join(repo, "revision-b.txt"), "revision B\n")
	wakeSession(t, ctx, fullEnv, repo, "settings-live-ab")
	waitForEventState(t, filepath.Join(repo, ".git", "acd", "state.db"), "revision-b.txt", "published", 12*time.Second)

	mu.Lock()
	models := make([]string, len(hits))
	for i := range hits {
		models[i] = hits[i].Model
	}
	mu.Unlock()
	if len(models) < 2 || models[0] != "model-a" || models[1] != "model-b" {
		t.Fatalf("planner model sequence=%v want [model-a model-b]", models)
	}
	dbPath := filepath.Join(repo, ".git", "acd", "state.db")
	if got := sqliteScalar(t, dbPath, "SELECT group_concat(config_revision_id, ',') FROM (SELECT config_revision_id FROM intent_planner_windows ORDER BY id LIMIT 2)"); got != fmt.Sprintf("%d,%d", a.ID, b.ID) {
		t.Fatalf("planner revision sequence=%s want %d,%d", got, a.ID, b.ID)
	}
	if got := sqliteScalar(t, dbPath, "SELECT desired_revision_id || '|' || applied_revision_id || '|' || last_known_good_revision_id FROM runtime_config_state WHERE id=1"); got != fmt.Sprintf("%d|%d|%d", b.ID, b.ID, b.ID) {
		t.Fatalf("runtime projection=%s", got)
	}
	for _, req := range []state.ConfigActivationRequest{aReq, bReq} {
		got, err := state.ActivationRequestByID(ctx, db, req.ID)
		if err != nil || got.Status != state.ActivationApplied {
			t.Fatalf("activation request %d=%+v err=%v", req.ID, got, err)
		}
	}
	status := runAcd(t, ctx, fullEnv, "status", "--repo", repo, "--json")
	var statusPayload struct {
		RuntimeConfig struct {
			Desired int64 `json:"desired_revision"`
			Applied int64 `json:"applied_revision"`
		} `json:"runtime_config"`
	}
	decodeErr := json.Unmarshal([]byte(status.Stdout), &statusPayload)
	if status.ExitCode != 0 || decodeErr != nil || statusPayload.RuntimeConfig.Desired != b.ID || statusPayload.RuntimeConfig.Applied != b.ID {
		t.Fatalf("status/runtime mismatch exit=%d\n%s\n%s", status.ExitCode, status.Stdout, status.Stderr)
	}
}

func TestRuntimeConfigInvalidRetentionRapidABCAndRepositoryGates(t *testing.T) {
	repo := tempRepo(t)
	env := withIsolatedHome(t)
	ctx, cancel := context.WithTimeout(context.Background(), 75*time.Second)
	defer cancel()
	startSession(t, ctx, env, repo, "settings-gates", "shell")
	t.Cleanup(func() { stopSessionForce(t, env, repo) })
	waitMode(t, repo, "running", 5*time.Second)
	db := openSettingsRuntimeDB(t, repo)
	a, _ := queueSettingsRevision(t, db, sql.NullInt64{}, "", 1, map[string]any{"ai.provider": "deterministic", "commit.strategy": "event"})
	wakeSession(t, ctx, env, repo, "settings-gates")
	waitSettingsApplied(t, db, a.ID, 8*time.Second)

	bad, badReq := queueSettingsRevision(t, db, sql.NullInt64{Int64: a.ID, Valid: true}, "", 2, map[string]any{"capture.max_file_bytes": 1234})
	wakeSession(t, ctx, env, repo, "settings-gates")
	waitSettingsRequest(t, db, badReq.ID, state.ActivationRejected, 8*time.Second)
	projection, _ := state.RuntimeConfigActivationState(ctx, db)
	if projection.AppliedRevisionID.Int64 != a.ID || projection.LastKnownGoodRevisionID.Int64 != a.ID {
		t.Fatalf("invalid revision %d displaced A: %+v", bad.ID, projection)
	}

	b, _ := queueSettingsRevision(t, db, sql.NullInt64{Int64: bad.ID, Valid: true}, "", 3, map[string]any{"ai.provider": "deterministic", "commit.strategy": "event", "commit.format": "imperative"})
	c, _ := queueSettingsRevision(t, db, sql.NullInt64{Int64: b.ID, Valid: true}, "", 4, map[string]any{"ai.provider": "deterministic", "commit.strategy": "event", "commit.format": "conventional"})
	wakeSession(t, ctx, env, repo, "settings-gates")
	waitSettingsApplied(t, db, c.ID, 8*time.Second)
	projection, _ = state.RuntimeConfigActivationState(ctx, db)
	if projection.DesiredRevisionID.Int64 != c.ID || projection.AppliedRevisionID.Int64 != c.ID {
		t.Fatalf("A-B-C did not converge atomically to C: %+v", projection)
	}

	gates := []struct {
		name  string
		enter func()
		exit  func()
	}{
		{name: "manual-pause", enter: func() {
			res := runAcd(t, ctx, env, "pause", "--repo", repo, "--reason", "settings gate", "--yes", "--json")
			if res.ExitCode != 0 {
				t.Fatalf("pause: %s", res.Stderr)
			}
		}, exit: func() {
			res := runAcd(t, ctx, env, "resume", "--repo", repo, "--yes", "--json")
			if res.ExitCode != 0 {
				t.Fatalf("resume: %s", res.Stderr)
			}
		}},
		{name: "detached-head", enter: func() { runGitOK(t, repo, "checkout", "--detach", "-q") }, exit: func() { runGitOK(t, repo, "checkout", "main", "-q") }},
		{name: "git-operation", enter: func() { writeFile(t, filepath.Join(repo, ".git", "MERGE_HEAD"), strings.Repeat("0", 40)+"\n") }, exit: func() { _ = os.Remove(filepath.Join(repo, ".git", "MERGE_HEAD")) }},
	}
	expected := c.ID
	for i, gate := range gates {
		t.Run(gate.name, func(t *testing.T) {
			gate.enter()
			defer gate.exit()
			next, req := queueSettingsRevision(t, db, sql.NullInt64{Int64: expected, Valid: true}, "", int64(10+i), map[string]any{"ai.provider": "deterministic", "commit.strategy": "event", "intent.window": 10 + i})
			wakeSession(t, ctx, env, repo, "settings-gates")
			waitSettingsApplied(t, db, next.ID, 8*time.Second)
			waitSettingsRequest(t, db, req.ID, state.ActivationApplied, 2*time.Second)
			projection, _ := state.RuntimeConfigActivationState(ctx, db)
			if projection.DesiredRevisionID.Int64 != next.ID || projection.AppliedRevisionID.Int64 != next.ID {
				t.Fatalf("gate exposed half-applied state: %+v", projection)
			}
			expected = next.ID
		})
	}
}

func TestRuntimeConfigShutdownAndCrashBoundaryRecovery(t *testing.T) {
	for _, tc := range []struct {
		name        string
		acknowledge bool
	}{
		{name: "crash-before-ack"},
		{name: "crash-after-ack", acknowledge: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			repo := tempRepo(t)
			env := withIsolatedHome(t)
			ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
			defer cancel()
			// Initialize the real schema without starting a daemon, then leave a
			// pending or acknowledged activation as crash residue.
			db := openSettingsRuntimeDB(t, repo)
			revision, request := queueSettingsRevision(t, db, sql.NullInt64{}, "", 1, map[string]any{"ai.provider": "deterministic", "commit.strategy": "event"})
			if tc.acknowledge {
				if ok, err := state.AcknowledgeConfigActivation(ctx, db, request.ID, revision.ID); err != nil || !ok {
					t.Fatalf("seed acknowledged crash row: ok=%v err=%v", ok, err)
				}
			}
			startSession(t, ctx, env, repo, "settings-recover-"+tc.name, "shell")
			t.Cleanup(func() { stopSessionForce(t, env, repo) })
			waitMode(t, repo, "running", 5*time.Second)
			wakeSession(t, ctx, env, repo, "settings-recover-"+tc.name)
			waitSettingsApplied(t, db, revision.ID, 8*time.Second)
			waitSettingsRequest(t, db, request.ID, state.ActivationApplied, 2*time.Second)

			next, _ := queueSettingsRevision(t, db, sql.NullInt64{Int64: revision.ID, Valid: true}, "", 2, map[string]any{"ai.provider": "deterministic", "commit.strategy": "event", "intent.window": 12})
			// A stop racing the activation may leave it pending, acknowledged, or
			// applied; restart must converge boundedly in every case.
			stopSessionForce(t, env, repo)
			waitMode(t, repo, "stopped", 5*time.Second)
			startSession(t, ctx, env, repo, "settings-recover2-"+tc.name, "shell")
			wakeSession(t, ctx, env, repo, "settings-recover2-"+tc.name)
			waitSettingsApplied(t, db, next.ID, 8*time.Second)
		})
	}
}

type settingsIntentRequest struct {
	intentChatRequest
	Model string `json:"model"`
}

func decodeSettingsPlannerRequest(t *testing.T, r *http.Request) settingsIntentRequest {
	t.Helper()
	defer r.Body.Close()
	var request settingsIntentRequest
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		t.Fatalf("decode settings planner request: %v", err)
	}
	return request
}

func writeSettingsPlannerResponse(t *testing.T, w http.ResponseWriter, model string, seqs []int64) {
	t.Helper()
	plan := map[string]any{"selected_seqs": seqs, "deferred_seqs": []int64{}, "subject": "Apply " + model + " settings", "body": "- Prove one complete runtime revision", "grouping_reason": "settings integration", "deferred_reasons": []map[string]any{}}
	writeIntentPlanResponse(t, w, "call-settings", plan)
}

func settingsRuntimeEnv(url, trustEnv string) []string {
	return []string{"ACD_COMMIT_STRATEGY=intent", "ACD_INTENT_WINDOW=1", "ACD_INTENT_MIN_PENDING=1", "ACD_INTENT_SETTLE_WINDOW=0s", "ACD_INTENT_MAX_PENDING_AGE=30s", "ACD_AI_PROVIDER=openai-compat", "ACD_AI_BASE_URL=" + url, "ACD_AI_API_KEY=settings-test-key", "ACD_AI_MODEL=bootstrap", trustEnv}
}

func openSettingsRuntimeDB(t *testing.T, repo string) *state.DB {
	t.Helper()
	db, err := state.Open(context.Background(), filepath.Join(repo, ".git", "acd", "state.db"))
	if err != nil {
		t.Fatalf("open runtime state: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func queueSettingsRevision(t *testing.T, db *state.DB, expected sql.NullInt64, model string, generation int64, values map[string]any) (state.ConfigRevision, state.ConfigActivationRequest) {
	t.Helper()
	if values == nil {
		values = map[string]any{
			"ai.provider": "openai-compat", "ai.model": model,
			"ai.diff_egress": "true", "commit.strategy": "intent",
			"commit.preset": "fast", "commit.format": "imperative",
			"intent.window": "1", "intent.min_pending": "1",
			"intent.settle_window": "0s", "intent.max_pending_age": "30s",
			"intent.verification": "none", "intent.repair.enabled": "false",
			"preset_id": "intent.fast", "preset_version": 2,
			"customized": true,
			"confirmations": []string{
				"endpoint_credentials", "diff_egress",
			},
		}
	}
	strategy, _ := values["commit.strategy"].(string)
	if strategy == "" {
		strategy = "event"
		values["commit.strategy"] = strategy
	}
	if _, ok := values["preset_id"]; !ok {
		values["commit.preset"] = "fast"
		values["preset_id"] = strategy + ".fast"
		values["preset_version"] = 2
		values["customized"] = true
	}
	body, err := json.Marshal(values)
	if err != nil {
		t.Fatal(err)
	}
	revision, err := state.InsertConfigRevision(context.Background(), db, state.ConfigRevisionInput{Snapshot: body, Profile: "integration", Scope: "repository", SourceGeneration: generation, Reason: "integration fixture"})
	if err != nil {
		t.Fatalf("insert settings revision: %v", err)
	}
	if !expected.Valid {
		projection, projectionErr := state.RuntimeConfigActivationState(
			context.Background(), db)
		if projectionErr != nil {
			t.Fatalf("load current settings revision: %v", projectionErr)
		}
		expected = projection.DesiredRevisionID
	}
	request, ok, err := state.RequestConfigActivation(context.Background(), db, revision.ID, expected)
	if err != nil || !ok {
		t.Fatalf("request settings revision %d expected=%v: ok=%v err=%v", revision.ID, expected, ok, err)
	}
	return revision, request
}

func waitSettingsApplied(t *testing.T, db *state.DB, revisionID int64, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		projection, err := state.RuntimeConfigActivationState(context.Background(), db)
		if err == nil && projection.DesiredRevisionID.Valid && projection.DesiredRevisionID.Int64 == revisionID && projection.AppliedRevisionID.Valid && projection.AppliedRevisionID.Int64 == revisionID {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	projection, err := state.RuntimeConfigActivationState(context.Background(), db)
	var status, activationError string
	_ = db.ReadSQL().QueryRow(`SELECT status, COALESCE(error, '') FROM config_activation_requests WHERE revision_id=? ORDER BY id DESC LIMIT 1`, revisionID).Scan(&status, &activationError)
	t.Fatalf("runtime revision %s not applied within %v: projection=%+v err=%v request_status=%q request_error=%q", strconv.FormatInt(revisionID, 10), timeout, projection, err, status, activationError)
}

func waitSettingsRequest(t *testing.T, db *state.DB, requestID int64, status string, timeout time.Duration) {
	t.Helper()
	waitFor(t, fmt.Sprintf("activation request %d=%s", requestID, status), timeout, func() bool {
		request, err := state.ActivationRequestByID(context.Background(), db, requestID)
		return err == nil && request.Status == status
	})
}
