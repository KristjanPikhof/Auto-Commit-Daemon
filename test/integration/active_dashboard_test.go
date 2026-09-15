//go:build integration

package integration_test

import (
	"context"
	"encoding/json"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/checkpoint"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

func TestActiveDashboardWorksOutsideRepositories(t *testing.T) {
	ctx := context.Background()
	env := withIsolatedHome(t)
	active, idle := tempRepo(t), tempRepo(t)
	for _, repo := range []string{active, idle} {
		prepareCheckpointRegistration(t, env, repo)
		db, err := state.Open(ctx, filepath.Join(repo, ".git", "acd", "state.db"))
		if err != nil {
			t.Fatal(err)
		}
		at := time.Now()
		if repo == idle {
			at = at.Add(-2 * time.Hour)
		}
		state.RecordActivity(ctx, db, at)
		if err := state.MetaSetJSON(ctx, db, checkpoint.MaintenanceMetaKey, checkpoint.MaintenanceStatus{State: "prerequisite", Error: "Xcode license agreements"}); err != nil {
			t.Fatal(err)
		}
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
	}
	outside := t.TempDir()
	compact := runAcdFromDir(t, ctx, env, outside, "list", "--once", "--verbose")
	if compact.ExitCode != 3 || !strings.Contains(compact.Stdout, active) || strings.Contains(compact.Stdout, idle) || !strings.Contains(compact.Stdout, "Xcode license") {
		t.Fatalf("compact: %+v", compact)
	}
	all := runAcdFromDir(t, ctx, env, outside, "list", "--json")
	var envelope struct {
		Data struct {
			Repos []struct {
				Repo string `json:"repo"`
			} `json:"repos"`
		} `json:"data"`
	}
	if err := json.Unmarshal([]byte(all.Stdout), &envelope); err != nil {
		t.Fatal(err)
	}
	if all.ExitCode != 3 || len(envelope.Data.Repos) != 2 {
		t.Fatalf("exhaustive: %+v", all)
	}
}

func assertRecentDashboardActivity(t *testing.T, repo string) {
	t.Helper()
	ctx := context.Background()
	db, err := state.Open(ctx, filepath.Join(repo, ".git", "acd", "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	raw, ok, err := state.MetaGet(ctx, db, state.ActivityMetaKey)
	if err != nil || !ok {
		t.Fatalf("activity missing: %q %v", raw, err)
	}
	ts, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || time.Since(time.Unix(ts, 0)) > time.Minute {
		t.Fatalf("activity not recent: %q %v", raw, err)
	}
	active, _, err := state.MetaGet(ctx, db, state.RewritePIDMetaKey)
	if err != nil || active != "" {
		t.Fatalf("completed operation still active: %q %v", active, err)
	}
}
