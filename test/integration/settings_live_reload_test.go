//go:build integration

package integration_test

import (
	"context"
	"encoding/json"
	"net/http"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/ai"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/settings"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

func TestSettingsLiveReloadKeepsFrozenFinishAndUsesNewApprovalNext(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Second)
	defer cancel()
	var mu sync.Mutex
	var models []string
	entered := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })
	server, trustEnv := newOpenAITestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := decodeFinishChatRequest(t, r)
		mu.Lock()
		models = append(models, req.Model)
		count := len(models)
		mu.Unlock()
		if req.Model == "model-a" && count == 1 {
			close(entered)
			select {
			case <-release:
			case <-r.Context().Done():
				return
			}
		}
		writeFinishChatResponse(t, w, req)
	}))
	defer server.Close()
	extra := []string{trustEnv, "ACD_AI_API_KEY=settings-test-key"}
	f := newFinishFixture(t, extra...)
	approval := []ai.ConfirmationRequirement{ai.ConfirmationEndpointCredentials, ai.ConfirmationDiffEgress}
	values := map[string]string{"commit.strategy": "intent", "commit.format": "imperative", "ai.provider": "openai-compat", "ai.base_url": server.URL + "/v1", "ai.model": "model-a", "ai.diff_egress": "true", "intent.verification": "structural"}
	a := f.Apply(t, ctx, values, approval...)
	writeFile(t, filepath.Join(f.Repo, "revision-a.txt"), "approved A\n")
	f.Barrier(t, ctx)
	finished := make(chan ExecResult, 1)
	go func() { finished <- f.Run(t, ctx, "finish", "Apply model-a settings", "--json") }()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("A provider never entered")
	}
	values["ai.model"] = "model-b"
	b := f.Apply(t, ctx, values, approval...)
	releaseOnce.Do(func() { close(release) })
	if result := <-finished; result.ExitCode != 0 {
		t.Fatalf("frozen A %+v", result)
	}
	if subject := runGitOK(t, f.Repo, "log", "-1", "--format=%s"); strings.TrimSpace(subject) != "Apply model-a settings" {
		t.Fatalf("frozen message %q", subject)
	}
	writeFile(t, filepath.Join(f.Repo, "revision-b.txt"), "approved B\n")
	f.Barrier(t, ctx)
	if result := f.Run(t, ctx, "finish", "Apply model-b settings", "--json"); result.ExitCode != 0 {
		t.Fatalf("new B %+v", result)
	}
	mu.Lock()
	got := append([]string(nil), models...)
	mu.Unlock()
	if len(got) != 2 || got[0] != "model-a" || got[1] != "model-b" {
		t.Fatalf("provider sequence %v", got)
	}
	db := f.OpenDB(t)
	var digests []string
	rows, err := db.ReadSQL().QueryContext(ctx, "SELECT target_json FROM finish_requests WHERE state='completed' ORDER BY created_ts")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var body string
		if err := rows.Scan(&body); err != nil {
			t.Fatal(err)
		}
		var target state.FinishTarget
		if err := json.Unmarshal([]byte(body), &target); err != nil {
			t.Fatal(err)
		}
		digests = append(digests, target.ConfigDigest)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if len(digests) != 2 || digests[0] == "" || digests[0] == digests[1] || a.RevisionID == b.RevisionID {
		t.Fatalf("distinct frozen configuration missing: %v", digests)
	}
}

func TestRuntimeConfigInvalidApplyRetainsApprovedRevisionAndPrivacy(t *testing.T) {
	f := newFinishFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	a := f.Apply(t, ctx, map[string]string{"commit.strategy": "local", "commit.format": "imperative"})
	lookup := func(key string) (string, bool) { value := envValue(f.Env, key); return value, value != "" }
	service, err := settings.NewService(ctx, settings.Options{Roots: f.Roots, RepoPath: f.Repo, LookupEnv: lookup, CredentialLookup: lookup})
	if err != nil {
		t.Fatal(err)
	}
	defer service.Close()
	snapshot, err := service.Snapshot(ctx, settings.ScopeRepository, "")
	if err != nil {
		t.Fatal(err)
	}
	_, err = service.Apply(ctx, settings.ApplyRequest{Values: map[string]string{"commit.strategy": "intent", "ai.provider": "deterministic"}, ExpectedGeneration: snapshot.SavedGeneration, ExpectedDesiredRevision: snapshot.DesiredRevisionID})
	if err == nil {
		t.Fatal("invalid Intent transport displaced approval")
	}
	db := f.OpenDB(t)
	projection, err := state.RuntimeConfigActivationState(ctx, db)
	if err != nil || projection.AppliedRevisionID.Int64 != a.RevisionID {
		t.Fatalf("invalid apply displaced A: %+v %v", projection, err)
	}
	b := f.Apply(t, ctx, map[string]string{"commit.format": "conventional"})
	c := f.Apply(t, ctx, map[string]string{"commit.format": "imperative"})
	if b.RevisionID == c.RevisionID || c.RevisionID == a.RevisionID {
		t.Fatal("distinct approvals were not retained")
	}
	f.Restart(t, ctx)
	projection, err = state.RuntimeConfigActivationState(ctx, db)
	if err != nil || projection.AppliedRevisionID.Int64 != c.RevisionID || projection.LastKnownGoodRevisionID.Int64 != c.RevisionID {
		t.Fatalf("approved restart %+v %v", projection, err)
	}
	if got := strings.TrimSpace(runGitOK(t, f.Repo, "rev-parse", "HEAD")); got != f.BaseHead {
		t.Fatal("settings applied branch commits")
	}
	var forbidden int
	if err := db.ReadSQL().QueryRowContext(ctx, "SELECT COUNT(*) FROM config_revisions WHERE snapshot_json LIKE '%settings-test-key%'").Scan(&forbidden); err != nil || forbidden != 0 {
		t.Fatal("credentials entered approval ledger")
	}
}
