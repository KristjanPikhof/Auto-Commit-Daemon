package cli

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/central"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/checkpoint"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/daemon"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

func TestProductListWatchKeepsRowsInPlace(t *testing.T) {
	var order []string
	for _, tc := range []struct{ input, want []string }{
		{[]string{"a", "b"}, []string{"a", "b"}},
		{[]string{"c", "b", "a"}, []string{"a", "b", "c"}},
		{[]string{"c", "b"}, []string{"b", "c"}},
		{[]string{"a", "c", "b"}, []string{"b", "c", "a"}},
	} {
		entries := make([]productListEntry, len(tc.input))
		for i, repo := range tc.input {
			entries[i] = productListEntry{Repo: repo}
		}
		_, order = orderProductListFrame(entries, order)
		if !reflect.DeepEqual(order, tc.want) {
			t.Fatalf("order=%v want=%v", order, tc.want)
		}
	}
}

func TestProductListHiddenWarningDoesNotFailCompactSnapshot(t *testing.T) {
	original := productListCollect
	t.Cleanup(func() { productListCollect = original })
	productListCollect = func(context.Context) (productListData, productState, error) {
		return productListData{Repos: []productListEntry{{Repo: "/idle-warning", ActionRequired: true, State: productStateNeedsAction}}}, productStateNeedsAction, nil
	}
	for _, tc := range []struct {
		name      string
		json, all bool
		exit      int
	}{
		{"compact", false, false, 0}, {"all", false, true, ExitActionRequired}, {"json", true, false, ExitActionRequired},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var out bytes.Buffer
			err := runProductListOnceView(context.Background(), &out, tc.json, false, tc.all)
			if ExitCode(err) != tc.exit {
				t.Fatalf("exit=%d want=%d: %v", ExitCode(err), tc.exit, err)
			}
			if tc.name == "compact" && (!strings.Contains(out.String(), "No active repositories") || strings.Contains(out.String(), "idle-warning")) {
				t.Fatal(out.String())
			}
		})
	}
}

func TestProductListActivitySurvivesSessionClosure(t *testing.T) {
	ctx := context.Background()
	repo := materializeTestRepo(t, false)
	db, err := state.Open(ctx, filepath.Join(repo, ".git", "acd", "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	now := time.Now().Truncate(time.Second)
	hookAt := now.Add(-10 * time.Minute)
	if err := state.RegisterClient(ctx, db, state.Client{SessionID: "agent", Harness: "codex", LastSeenTS: float64(hookAt.Unix())}); err != nil {
		t.Fatal(err)
	}
	if _, err := state.DeregisterClient(ctx, db, "agent"); err != nil {
		t.Fatal(err)
	}
	if err := state.SaveDaemonState(ctx, db, state.DaemonState{PID: os.Getpid(), Mode: "running", HeartbeatTS: float64(now.Unix()), UpdatedTS: float64(now.Unix())}); err != nil {
		t.Fatal(err)
	}
	record := central.RepoRecord{Path: repo, StateDB: db.Path(), LastSeenTS: now.Unix(), RepositoryID: "repo", WorktreeID: "wt"}
	overview, err := readProductListRepo(ctx, record, now)
	if err != nil {
		t.Fatal(err)
	}
	if !overview.lastActivity.Equal(hookAt) || overview.clients != 0 || overview.unfinished {
		t.Fatalf("closed session: %+v", overview)
	}
	// Housekeeping changes must not renew the activity window.
	if err := state.MetaSet(ctx, db, daemon.MetaKeyProtectionFullPollTS, fmt.Sprint(now.Unix())); err != nil {
		t.Fatal(err)
	}
	if err := state.MetaSet(ctx, db, daemon.MetaKeyProtectionRetentionOverBudget, "needs_action"); err != nil {
		t.Fatal(err)
	}
	overview, err = readProductListRepo(ctx, record, now)
	if err != nil {
		t.Fatal(err)
	}
	if !overview.lastActivity.Equal(hookAt) || overview.unfinished || overview.report.CheckpointRetentionOverBudget {
		t.Fatalf("maintenance counted as activity: %+v", overview)
	}
	// Existing branch observation covers rewrites performed by other tools.
	if err := state.MetaSet(ctx, db, daemon.MetaKeyBranchTokenChangedAt, fmt.Sprint(now.Unix())); err != nil {
		t.Fatal(err)
	}
	overview, err = readProductListRepo(ctx, record, now)
	if err != nil {
		t.Fatal(err)
	}
	if !overview.lastActivity.Equal(now) {
		t.Fatalf("branch activity=%s", overview.lastActivity)
	}
}

func TestProductListMaintenanceDoesNotHideProtectionFailure(t *testing.T) {
	for _, m := range []checkpoint.MaintenanceStatus{
		{State: "retrying", Error: "inventory timeout"},
		{State: "prerequisite", Error: "Xcode license agreements"},
		{State: "over_budget", OverBudget: true},
	} {
		report := statusReport{Daemon: "running", PID: os.Getpid(), ActiveBarriers: 1, CheckpointMaintenance: m}
		result := controlResult{OK: true}
		applyControlStatus(&result, report)
		if result.Health != controlHealthNeedsAttention || !strings.HasPrefix(result.Summary, "A blocked publication") || !strings.Contains(result.NextAction, "support recover") {
			t.Fatalf("maintenance hid blocker: %+v", result)
		}
	}
}

func TestProductListReadOnlyMaintenanceAgreesWithStatus(t *testing.T) {
	ctx := context.Background()
	repo := materializeTestRepo(t, false)
	db, err := state.Open(ctx, filepath.Join(repo, ".git", "acd", "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	want := checkpoint.MaintenanceStatus{State: "prerequisite", Error: "Xcode license agreements", NextAttemptTS: time.Now().Add(time.Minute).Unix()}
	if err := state.MetaSetJSON(ctx, db, checkpoint.MaintenanceMetaKey, want); err != nil {
		t.Fatal(err)
	}
	record := central.RepoRecord{Path: repo, StateDB: db.Path(), RepositoryID: "repo", WorktreeID: "wt"}
	before, err := fileSHA256(db.Path())
	if err != nil {
		t.Fatal(err)
	}
	overview, err := readProductListRepo(ctx, record, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	status, err := buildStatusReport(ctx, record, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	diagnose, err := buildDiagnoseReport(ctx, record)
	if err != nil {
		t.Fatal(err)
	}
	if overview.report.CheckpointMaintenance != want || status.CheckpointMaintenance != want || diagnose.CheckpointMaintenance != want {
		t.Fatal("readers disagree about maintenance")
	}
	after, err := fileSHA256(db.Path())
	if err != nil {
		t.Fatal(err)
	}
	if before != after {
		t.Fatal("read-only reports modified database")
	}
}
