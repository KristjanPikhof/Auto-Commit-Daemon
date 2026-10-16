package cli

import (
	"bufio"
	"bytes"
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/central"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/checkpoint"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/daemon"
	gitpkg "github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/supervisor"
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

func TestProductListIdleWarningRemainsVisibleInCompactSnapshot(t *testing.T) {
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
		{"compact", false, false, ExitActionRequired}, {"all", false, true, ExitActionRequired}, {"json", true, false, ExitActionRequired},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var out bytes.Buffer
			err := runProductListOnceView(context.Background(), &out, tc.json, false, tc.all)
			if ExitCode(err) != tc.exit {
				t.Fatalf("exit=%d want=%d: %v", ExitCode(err), tc.exit, err)
			}
			if tc.name == "compact" && (!strings.Contains(out.String(), "idle-warning") || !strings.Contains(out.String(), "needs action")) {
				t.Fatal(out.String())
			}
		})
	}
}

func TestProductListActivitySurvivesClientExit(t *testing.T) {
	lookup, runtime, _ := freshProtectionBarrierFixture(t)
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	repo, db := lookup.Worktree.Root, runtime.db
	// The editor writes a real file and stays alive until its stdin closes.
	// Stdout is the observable save milestone; no session row is synthesized.
	editor := exec.CommandContext(ctx, "sh", "-c", `printf 'unfinished goal\n' > "$1"; printf 'saved\n'; read reply || exit 0`, "editor", filepath.Join(repo, "goal.txt"))
	stdout, err := editor.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	stdin, err := editor.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := editor.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if editor.ProcessState == nil {
			_ = editor.Process.Kill()
			_ = editor.Wait()
		}
	})
	if ready, err := bufio.NewReader(stdout).ReadString('\n'); err != nil || ready != "saved\n" {
		t.Fatalf("editor save=%q: %v", ready, err)
	}
	head, _ := gitpkg.RevParse(ctx, repo, "HEAD")
	index, _ := gitpkg.IndexContentDigest(ctx, repo)
	protected := protectBarrierFixture(t, ctx, lookup, runtime)
	if err := state.RecordCaptureHealth(ctx, db, "", protected.CheckpointID, time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := state.SaveDaemonState(ctx, db, state.DaemonState{PID: os.Getpid(), Mode: "running", HeartbeatTS: nowFloat(), UpdatedTS: nowFloat()}); err != nil {
		t.Fatal(err)
	}
	record := lookup.Record
	record.Path, record.StateDB = repo, db.Path()
	before, err := readProductListRepo(ctx, record, time.Now())
	if err != nil || before.lastActivity.IsZero() || !before.unfinished || !before.report.Protected {
		t.Fatalf("saved work %+v: %v", before, err)
	}
	if err := stdin.Close(); err != nil {
		t.Fatal(err)
	}
	if err := editor.Wait(); err != nil || editor.ProcessState == nil || !editor.ProcessState.Success() {
		t.Fatalf("editor exit: %v", err)
	}
	overview, err := readProductListRepo(ctx, record, time.Now())
	if err != nil || !overview.lastActivity.Equal(before.lastActivity) || overview.clients != 0 || !overview.unfinished || !overview.report.Protected || overview.report.LatestCheckpointID != protected.CheckpointID {
		t.Fatalf("client exit lost protected activity %+v: %v", overview, err)
	}
	entry := productListEntryFromOverview(record, supervisor.WorkerStatus{}, overview, nil)
	visible, hidden := selectProductListEntriesAt([]productListEntry{entry}, false, before.lastActivity.Add(2*time.Hour))
	if hidden != 0 || len(visible) != 1 || !visible[0].Protected || !visible[0].UnfinishedWork || visible[0].LastActivityAt == "" {
		t.Fatalf("unfinished work hidden after client exit: hidden=%d rows=%+v", hidden, visible)
	}
	var out bytes.Buffer
	if err := renderProductListSelection(&out, visible, hidden, true, false); err != nil || !strings.Contains(out.String(), filepath.Base(repo)) {
		t.Fatalf("protected row not visible: %v\n%s", err, out.String())
	}
	// Poll/retention housekeeping does not manufacture newer user activity.
	if err := state.MetaSet(ctx, db, daemon.MetaKeyProtectionFullPollTS, "9999999999"); err != nil {
		t.Fatal(err)
	}
	if err := state.MetaSetJSON(ctx, db, checkpoint.MaintenanceMetaKey, checkpoint.MaintenanceStatus{State: "over_budget", OverBudget: true}); err != nil {
		t.Fatal(err)
	}
	overview, err = readProductListRepo(ctx, record, time.Now())
	if err != nil || !overview.lastActivity.Equal(before.lastActivity) || !overview.unfinished || !overview.report.CheckpointRetentionOverBudget {
		t.Fatalf("maintenance changed durable work/activity %+v: %v", overview, err)
	}
	// A real branch observation creates new checkpoint activity, rather than
	// trusting a retired branch-token or hook-session timestamp.
	if _, err := gitpkg.Run(ctx, gitpkg.RunOpts{Dir: repo}, "switch", "-q", "-c", "feature"); err != nil {
		t.Fatal(err)
	}
	next := protectBarrierFixture(t, ctx, lookup, runtime)
	if err := state.RecordCaptureHealth(ctx, db, "", next.CheckpointID, time.Now()); err != nil {
		t.Fatal(err)
	}
	overview, err = readProductListRepo(ctx, record, time.Now())
	if err != nil || !overview.lastActivity.After(before.lastActivity) || !overview.unfinished || overview.report.LatestCheckpointID != next.CheckpointID {
		t.Fatalf("new checkpoint activity missing %+v: %v", overview, err)
	}
	assertBarrierNeverPublishes(t, lookup, runtime, head, index)
}

func TestProductListMaintenanceDoesNotHideProtectionFailure(t *testing.T) {
	for _, m := range []checkpoint.MaintenanceStatus{
		{State: "retrying", Error: "inventory timeout"},
		{State: "prerequisite", Error: "Xcode license agreements"},
		{State: "over_budget", OverBudget: true},
	} {
		report := statusReport{Daemon: "running", PID: os.Getpid(), CaptureHealth: state.CaptureHealth{State: "blocked", Error: "eligible file unreadable"}, CheckpointMaintenance: m}
		result := controlResult{OK: true}
		applyControlStatus(&result, report)
		if result.Health != controlHealthNeedsAttention || !strings.HasPrefix(result.Summary, "Some changes could not be saved: eligible file unreadable") || !strings.Contains(result.NextAction, "acd doctor") {
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

func TestProductListDistinguishesRewriteExecutionFromProposal(t *testing.T) {
	ctx := context.Background()
	repo := materializeTestRepo(t, false)
	db, err := state.Open(ctx, filepath.Join(repo, ".git", "acd", "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	plan, err := state.SaveIntentHistoryPlan(ctx, db, state.IntentHistoryPlan{SourceBranchRef: "refs/heads/main", TargetBranchRef: "refs/heads/main", ExpectedHead: strings.Repeat("a", 40),
		Goals: []state.IntentHistoryGoal{{ID: "wording", Purpose: "Improve wording", Message: "Improve wording", OldOIDs: []string{strings.Repeat("a", 40)}}}})
	if err != nil {
		t.Fatal(err)
	}
	record := central.RepoRecord{Path: repo, StateDB: db.Path(), RepositoryID: "repo", WorktreeID: "wt"}
	overview, err := readProductListRepo(ctx, record, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if overview.unfinished || !overview.lastActivity.IsZero() {
		t.Fatalf("saved proposal counted as execution: %+v", overview)
	}
	request, created, err := state.StartFinishHistoryRequest(ctx, db, state.FinishHistoryRequest{PlanID: plan.ID, WorktreeID: checkpoint.WorktreeID(repo), ConfigDigest: "approved", IndexDigest: "frozen", DeadlineTS: float64(time.Now().Add(time.Minute).Unix())})
	if err != nil || !created {
		t.Fatalf("explicit history request: created=%v err=%v", created, err)
	}
	overview, err = readProductListRepo(ctx, record, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if !overview.unfinished || !overview.report.Busy || overview.report.PublicationProgress.Phase != "history_running" {
		t.Fatalf("active history execution not visible: %+v", overview)
	}
	request.Status, request.Error = "failed", "worker interrupted"
	if err := state.UpdateFinishHistoryRequest(ctx, db, request, "running"); err != nil {
		t.Fatal(err)
	}
	overview, err = readProductListRepo(ctx, record, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if overview.unfinished || overview.report.Busy {
		t.Fatalf("terminal history request pinned repository: %+v", overview)
	}
}
