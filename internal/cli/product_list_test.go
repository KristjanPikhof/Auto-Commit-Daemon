package cli

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/central"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/daemon"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/paths"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/supervisor"
)

func TestProductListOnceNeedsActionHumanRendersThenExitsThree(t *testing.T) {
	registerProductListNeedsActionRepo(t)

	var out bytes.Buffer
	err := runProductListOnce(context.Background(), &out, false, false)
	if ExitCode(err) != ExitActionRequired || !ErrorRendered(err) {
		t.Fatalf("exit=%d rendered=%v err=%v, want rendered exit %d", ExitCode(err), ErrorRendered(err), err, ExitActionRequired)
	}
	for _, want := range []string{"REPO", "SAFE", "needs action", "no"} {
		if !strings.Contains(out.String(), want) {
			t.Fatalf("human output missing %q:\n%s", want, out.String())
		}
	}
}

func TestProductListOnceNeedsActionJSONRendersThenExitsThree(t *testing.T) {
	repo := registerProductListNeedsActionRepo(t)

	var out bytes.Buffer
	err := runProductListOnce(context.Background(), &out, true, false)
	if ExitCode(err) != ExitActionRequired || !ErrorRendered(err) {
		t.Fatalf("exit=%d rendered=%v err=%v, want rendered exit %d", ExitCode(err), ErrorRendered(err), err, ExitActionRequired)
	}
	var got struct {
		OK    bool            `json:"ok"`
		State productState    `json:"state"`
		Data  productListData `json:"data"`
		Error *productError   `json:"error"`
	}
	if err := json.Unmarshal(out.Bytes(), &got); err != nil {
		t.Fatalf("unmarshal JSON: %v\n%s", err, out.String())
	}
	if !got.OK || got.State != productStateNeedsAction || got.Error != nil {
		t.Fatalf("unexpected envelope: %+v", got)
	}
	if len(got.Data.Repos) != 1 {
		t.Fatalf("repos=%d, want 1: %+v", len(got.Data.Repos), got.Data.Repos)
	}
	entry := got.Data.Repos[0]
	if entry.Repo != repo || !entry.Enabled || !entry.ActionRequired || entry.State != productStateNeedsAction {
		t.Fatalf("unexpected repository aggregate: %+v", entry)
	}
	raw := out.String()
	for _, field := range []string{`"worker_state"`, `"operational_state"`, `"blocked_events"`, `"last_activity_at"`, `"publication_drain"`} {
		if !strings.Contains(raw, field) {
			t.Fatalf("JSON output missing additive field %s:\n%s", field, raw)
		}
	}
}

func TestProductListOnceOffRepositoryRequiresAction(t *testing.T) {
	roots := withIsolatedHome(t)
	repo := registerProductListOffRepo(t, roots, materializeTestRepo(t, false))

	var out bytes.Buffer
	err := runProductListOnce(context.Background(), &out, true, false)
	if ExitCode(err) != ExitActionRequired || !ErrorRendered(err) {
		t.Fatalf("exit=%d rendered=%v err=%v, want rendered exit %d", ExitCode(err), ErrorRendered(err), err, ExitActionRequired)
	}
	var got struct {
		State productState    `json:"state"`
		Data  productListData `json:"data"`
	}
	if err := json.Unmarshal(out.Bytes(), &got); err != nil {
		t.Fatalf("unmarshal JSON: %v\n%s", err, out.String())
	}
	if got.State != productStateNeedsAction || len(got.Data.Repos) != 1 {
		t.Fatalf("unexpected aggregate: %+v", got)
	}
	entry := got.Data.Repos[0]
	if entry.Repo != repo || entry.Enabled || !entry.ActionRequired || entry.State != productStateNeedsAction {
		t.Fatalf("off repository was not classified as action-required: %+v", entry)
	}
}

func TestProductListNeedsActionDominatesMixedStates(t *testing.T) {
	roots := withIsolatedHome(t)
	registerProductListOffRepo(t, roots, materializeTestRepo(t, false))
	registerProductListNeedsActionRepoWithRoots(t, roots)

	data, stateName, err := collectProductList(context.Background())
	if err != nil {
		t.Fatalf("collect product list: %v", err)
	}
	if stateName != productStateNeedsAction || len(data.Repos) != 2 {
		t.Fatalf("state=%q repos=%d, want needs_action with two repos", stateName, len(data.Repos))
	}
	for _, entry := range data.Repos {
		if !entry.ActionRequired || entry.State != productStateNeedsAction {
			t.Fatalf("entry did not retain needs_action classification: %+v", entry)
		}
	}
}

func TestProductListDrainAndPendingLabels(t *testing.T) {
	if got := productListMode(productListEntry{PublicationProgress: publicationProgressReport{
		Strategy: "intent", TemporaryLocalFallback: true,
	}}); got != "intent" {
		t.Fatalf("configured Intent mode rendered as %q during local recovery", got)
	}
	entries := []productListEntry{
		{Repo: "/active", Enabled: true, Protected: true,
			State: productStatePublishing, PendingEvents: 22,
			PublicationDrain:    publicationDrainReport{ID: "drain", Phase: "semantic", PublishedEvents: 7, TargetEvents: 12, RemainingEvents: 5},
			PublicationProgress: publicationProgressReport{Strategy: "intent", Origin: "commit_all", Phase: "intent_planning", QueuePending: 22, TargetRemaining: 5, TargetTotal: 12, LastProgressTS: 1, LastProgressAgeSeconds: 90}},
		{Repo: "/pending", Enabled: true, Protected: true,
			State: productStateWaiting, PendingEvents: 3,
			PublicationProgress: publicationProgressReport{Strategy: "intent", Phase: "intent_wait", QueuePending: 3, WaitRemainingSeconds: 42, LastProgressTS: 1, LastProgressAgeSeconds: 8}},
		{Repo: "/recovering", Enabled: true, Protected: true,
			State: productStatePublishing, PendingEvents: 7,
			PublicationProgress: publicationProgressReport{Strategy: "intent", Origin: "intent_recovery", Phase: "intent_replanning", QueuePending: 7, TargetRemaining: 4, TargetTotal: 6, LastProgressTS: 1, LastProgressAgeSeconds: 12}},
		{Repo: "/blocked", Enabled: true, Protected: true, ActionRequired: true,
			State: productStateNeedsAction, PendingEvents: 7,
			PublicationProgress: publicationProgressReport{Strategy: "intent", Origin: "intent_recovery", Phase: "needs_action", NeedsAttention: true, QueuePending: 7, TargetRemaining: 4, TargetTotal: 6, LastProgressTS: 1, LastProgressAgeSeconds: 12}},
		{Repo: "/completed", Enabled: true, Protected: true,
			State:               productStateProtected,
			PublicationDrain:    publicationDrainReport{ID: "drain", Phase: "completed", PublishedEvents: 12, TargetEvents: 12},
			PublicationProgress: publicationProgressReport{Strategy: "intent", Phase: "idle"}},
	}

	var out bytes.Buffer
	if err := renderProductListTable(&out, entries, false); err != nil {
		t.Fatalf("render table: %v", err)
	}
	for _, row := range []struct {
		repo, mode, queue, target, phase string
	}{
		{"active", "intent", "22", "commit-all:5/12", "intent-plan"},
		{"pending", "intent", "3", "-", "wait:42s"},
		{"recovering", "intent", "7", "recover:4/6", "intent-replan"},
		{"blocked", "intent", "7", "recover:4/6", "blocked"},
		{"completed", "intent", "0", "-", "idle"},
	} {
		line := productListLineForRepo(t, out.String(), row.repo)
		fields := strings.Fields(line)
		if len(fields) < 8 || fields[2] != row.mode || fields[3] != row.queue ||
			fields[4] != row.target || fields[6] != row.phase {
			t.Fatalf("row %q progress=%v, want mode=%q queue=%q target=%q phase=%q:\n%s",
				row.repo, fields, row.mode, row.queue, row.target, row.phase, line)
		}
	}
	if line := productListLineForRepo(t, out.String(), "completed"); !strings.Contains(line, "healthy") {
		t.Fatalf("completed drain did not return to healthy: %s", line)
	}
	if line := productListLineForRepo(t, out.String(), "blocked"); !strings.Contains(line, "needs action") {
		t.Fatalf("exhausted recovery did not require action: %s", line)
	}
}

func TestProductListDoesNotMaskActiveIntentRecoveryAsCheckpointing(t *testing.T) {
	record := central.RepoRecord{
		Path: "/repo", RepositoryID: "repo-id", WorktreeID: "worktree-id",
	}
	overview := productListRepoOverview{report: statusReport{
		Daemon: "running", PID: os.Getpid(), PendingEvents: 7,
		CheckpointProtectionAvailable: true, Protected: false, Busy: true,
		OperationalState: "busy",
		IntentStrategy:   intentStrategyReport{Strategy: "intent", Active: true},
		PublicationProgress: publicationProgressReport{
			Strategy: "intent", Origin: "intent_recovery",
			Phase: "intent_replanning", QueuePending: 7,
			TargetRemaining: 4, TargetTotal: 6,
		},
	}}
	entry := productListEntryFromOverview(record, supervisor.WorkerStatus{
		RepositoryID: record.RepositoryID, State: "running",
	}, overview, nil)
	if !strings.Contains(entry.Summary, "automatically rebuilding semantic") ||
		strings.Contains(entry.Summary, "checkpointing") {
		t.Fatalf("active recovery entry=%+v", entry)
	}

	overview.report.PublicationProgress.Phase = "needs_action"
	overview.report.PublicationProgress.NeedsAttention = true
	overview.report.PublicationProgress.AttentionReason =
		"complete semantic prefix failed verification"
	entry = productListEntryFromOverview(record, supervisor.WorkerStatus{
		RepositoryID: record.RepositoryID, State: "running",
	}, overview, nil)
	if !entry.ActionRequired || entry.State != productStateNeedsAction ||
		productListStatus(entry) != "needs action" ||
		productListPhase(entry) != "blocked" ||
		productListTarget(entry) != "recover:4/6" ||
		entry.Summary != intentRecoveryVerificationAttentionSummary ||
		!strings.Contains(entry.NextAction, "acd doctor") {
		t.Fatalf("exhausted recovery entry=%+v", entry)
	}
}

func TestProductListLoadsCurrentProviderWait(t *testing.T) {
	t.Setenv("ACD_AI_TIMEOUT", "1m")
	ctx := context.Background()
	repo, dbPath, db := makeRepoStateDB(t)
	now := time.Now()
	branchRef := "refs/heads/main"
	if err := state.SaveDaemonState(ctx, db, state.DaemonState{
		PID: os.Getpid(), Mode: "running", HeartbeatTS: float64(now.Unix()),
		BranchRef: sql.NullString{String: branchRef, Valid: true},
		BranchGeneration: sql.NullInt64{
			Int64: 1, Valid: true,
		},
	}); err != nil {
		t.Fatal(err)
	}
	if err := state.MetaSetMany(ctx, db, map[string]string{
		daemon.MetaKeyProtectionObservationEpoch: "1",
		daemon.MetaKeyProtectionCoveredEpoch:     "1",
		daemon.MetaKeyProtectionCheckpointID:     "checkpoint",
		daemon.MetaKeyProtectionComplete:         "true",
		"commit.strategy":                        "intent",
		"ai.provider":                            "openai-compat",
		"ai.model":                               "gpt-test",
	}); err != nil {
		t.Fatal(err)
	}
	seq, err := state.AppendCaptureEvent(ctx, db, state.CaptureEvent{
		BranchRef: branchRef, BranchGeneration: 1, BaseHead: "head",
		Operation: "modify", Path: "main.go", Fidelity: "exact",
		CapturedTS: float64(now.Add(-10 * time.Minute).Unix()),
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := state.AppendIntentPlannerWindow(ctx, db, state.IntentPlannerWindow{
		PlannedTS: float64(now.Add(-time.Minute).Unix()),
		Provider:  sql.NullString{String: "openai-compat", Valid: true},
		Model:     sql.NullString{String: "gpt-test", Valid: true},
		BranchRef: branchRef, BranchGeneration: 1,
		OfferedSeqs: []int64{seq}, VisibleOriginalSeqs: []int64{seq},
	}); err != nil {
		t.Fatal(err)
	}
	run, err := state.EnsureIntentPlanRun(ctx, db, state.IntentPlanRun{
		Fingerprint: "sha256:provider-wait", BranchRef: branchRef,
		BranchGeneration: 1, AttemptLimit: 3,
		Provider: sql.NullString{String: "openai-compat", Valid: true},
		Model:    sql.NullString{String: "gpt-test", Valid: true},
	})
	if err != nil {
		t.Fatal(err)
	}
	run.UnresolvedSeqs = []int64{seq}
	run.ProgressState = sql.NullString{String: "waiting_message_rewrite", Valid: true}
	run.ResolutionMode = sql.NullString{String: "waiting_message_rewrite", Valid: true}
	if err := state.UpdateIntentPlanRun(ctx, db, run); err != nil {
		t.Fatal(err)
	}
	// A newer window on another branch must not replace the active branch's
	// locked-message state in the repository overview.
	if _, err := state.AppendIntentPlannerWindow(ctx, db, state.IntentPlannerWindow{
		PlannedTS: float64(now.Unix()), BranchRef: "refs/heads/other",
		BranchGeneration: 1, OfferedSeqs: []int64{seq + 1},
		VisibleOriginalSeqs: []int64{seq + 1},
	}); err != nil {
		t.Fatal(err)
	}
	health := daemon.IntentPlannerHealthSnapshot{
		State:               daemon.IntentPlannerCircuitOpen,
		ProviderFingerprint: testPlannerHealthFingerprint(),
		ConsecutiveFailures: 1,
	}
	if err := state.MetaSetJSON(ctx, db, daemon.MetaKeyIntentPlannerHealth, struct {
		Version int `json:"version"`
		daemon.IntentPlannerHealthSnapshot
	}{Version: 1, IntentPlannerHealthSnapshot: health}); err != nil {
		t.Fatal(err)
	}

	record := central.RepoRecord{
		Path: repo, StateDB: dbPath, RepositoryID: "repository-id",
		WorktreeID: "worktree-id",
	}
	overview, err := readProductListRepo(ctx, record, now)
	if err != nil {
		t.Fatal(err)
	}
	progress := overview.report.PublicationProgress
	if progress.Phase != "provider_wait" || progress.PlannerProvider != "openai-compat" ||
		overview.report.IntentStrategy.LastPlannerWindow == nil ||
		overview.report.IntentStrategy.LastPlannerWindow.BranchRef != branchRef ||
		overview.report.IntentStrategy.ResolutionMode != "waiting_message_rewrite" {
		t.Fatalf("provider wait overview=%+v strategy=%+v",
			progress, overview.report.IntentStrategy)
	}
	entry := productListEntryFromOverview(record, supervisor.WorkerStatus{
		RepositoryID: record.RepositoryID, State: "running",
	}, overview, nil)
	if got := productListStatus(entry); got != "waiting" {
		t.Fatalf("provider wait status=%q entry=%+v", got, entry)
	}
}

func TestProductListNextActionTargetsQuotedRepository(t *testing.T) {
	roots := withIsolatedHome(t)
	repo := materializeProductListRepoWithSpaces(t)
	registerProductListOffRepo(t, roots, repo)

	data, _, err := collectProductList(context.Background())
	if err != nil {
		t.Fatalf("collect product list: %v", err)
	}
	if len(data.Repos) != 1 {
		t.Fatalf("repos=%d, want 1", len(data.Repos))
	}
	want := "--repo " + productListShellQuote(repo)
	if !strings.Contains(data.Repos[0].NextAction, want) {
		t.Fatalf("next action=%q, want targeted quoted repository %q", data.Repos[0].NextAction, want)
	}
	if got := productListTargetAction("Run `acd doctor`.", "/tmp/repo with ' quote"); got != "Run `acd doctor --repo '/tmp/repo with '\"'\"' quote'`." {
		t.Fatalf("single-quote escaping drifted: %q", got)
	}
}

func TestProductListWatchNeedsActionContinuesRefreshing(t *testing.T) {
	registerProductListNeedsActionRepo(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	out := &productListFrameWriter{cancel: cancel, want: 2}

	if err := runProductListWatch(ctx, out, time.Millisecond, false); err != nil {
		t.Fatalf("watch: %v", err)
	}
	if frames := out.frameCount(); frames < 2 {
		t.Fatalf("frames=%d, want at least 2", frames)
	}
	if !strings.Contains(out.String(), "needs action") {
		t.Fatalf("watch output missing needs action:\n%s", out.String())
	}
}

func TestProductListPersistentFlagsAreHandled(t *testing.T) {
	t.Run("repo rejected", func(t *testing.T) {
		withIsolatedHome(t)
		cmd := newRootCmd()
		cmd.SetArgs([]string{"list", "--once", "--repo", t.TempDir()})
		if err := cmd.Execute(); ExitCode(err) != ExitInvalid {
			t.Fatalf("exit=%d err=%v, want %d", ExitCode(err), err, ExitInvalid)
		}
	})

	t.Run("json honored", func(t *testing.T) {
		withIsolatedHome(t)
		cmd := newRootCmd()
		var out bytes.Buffer
		cmd.SetOut(&out)
		cmd.SetArgs([]string{"list", "--json"})
		if err := cmd.Execute(); err != nil {
			t.Fatalf("list --json: %v", err)
		}
		var envelope productEnvelope
		if err := json.Unmarshal(out.Bytes(), &envelope); err != nil {
			t.Fatalf("JSON output: %v\n%s", err, out.String())
		}
	})

	t.Run("quiet preserves final result", func(t *testing.T) {
		withIsolatedHome(t)
		cmd := newRootCmd()
		var out bytes.Buffer
		cmd.SetOut(&out)
		cmd.SetArgs([]string{"list", "--once", "--quiet"})
		if err := cmd.Execute(); err != nil {
			t.Fatalf("list --quiet: %v", err)
		}
		if !strings.Contains(out.String(), "REPO") {
			t.Fatalf("quiet suppressed the final result:\n%s", out.String())
		}
	})

	t.Run("interactive all rejected", func(t *testing.T) {
		withIsolatedHome(t)
		cmd := newRootCmd()
		cmd.SetArgs([]string{"list", "--interactive", "--all"})
		if err := cmd.Execute(); ExitCode(err) != ExitInvalid {
			t.Fatalf("exit=%d err=%v, want %d", ExitCode(err), err, ExitInvalid)
		}
	})

	t.Run("log level validated", func(t *testing.T) {
		withIsolatedHome(t)
		cmd := newRootCmd()
		cmd.SetArgs([]string{"list", "--once", "--log-level", "loud"})
		if err := cmd.Execute(); ExitCode(err) != ExitInvalid {
			t.Fatalf("exit=%d err=%v, want %d", ExitCode(err), err, ExitInvalid)
		}
	})
}

func TestProductListSelectionKeepsRelevantAndFillsFive(t *testing.T) {
	base := time.Date(2026, 8, 15, 10, 0, 0, 0, time.UTC)
	entries := []productListEntry{
		{Repo: "/broken", ActionRequired: true, State: productStateNeedsAction, lastActivity: base.Add(1 * time.Minute)},
		{Repo: "/working", State: productStatePublishing, PendingEvents: 2, lastActivity: base.Add(2 * time.Minute)},
		{Repo: "/waiting", State: productStateWaiting, OperationalState: "waiting", lastActivity: base.Add(3 * time.Minute)},
	}
	for index := 0; index < 6; index++ {
		entries = append(entries, productListEntry{
			Repo: fmt.Sprintf("/healthy-%d", index), State: productStateProtected,
			Protected: true, lastActivity: base.Add(time.Duration(index) * time.Minute),
		})
	}
	sortProductListEntries(entries)

	visible, hidden := selectProductListEntries(entries, false)
	if len(visible) != productListDefaultRows || hidden != 4 {
		t.Fatalf("visible=%d hidden=%d, want 5 and 4", len(visible), hidden)
	}
	for _, repo := range []string{"/broken", "/working", "/waiting"} {
		if !productListContainsRepo(visible, repo) {
			t.Fatalf("mandatory repository %s was hidden: %+v", repo, visible)
		}
	}
	for _, repo := range []string{"/healthy-5", "/healthy-4"} {
		if !productListContainsRepo(visible, repo) {
			t.Fatalf("recent repository %s did not fill the view: %+v", repo, visible)
		}
	}
	all, hidden := selectProductListEntries(entries, true)
	if len(all) != len(entries) || hidden != 0 {
		t.Fatalf("--all selected %d hidden %d, want %d and 0", len(all), hidden, len(entries))
	}
}

func TestProductListSelectionPrefersRecentWorkOverPaused(t *testing.T) {
	base := time.Date(2026, 8, 15, 10, 0, 0, 0, time.UTC)
	entries := []productListEntry{
		{Repo: "/paused-new", State: productStateNeedsAction, OperationalState: "paused", lastActivity: base.Add(10 * time.Minute)},
		{Repo: "/healthy-new", State: productStateProtected, Protected: true, lastActivity: base.Add(9 * time.Minute)},
		{Repo: "/waiting", State: productStateWaiting, OperationalState: "waiting", lastActivity: base.Add(8 * time.Minute)},
		{Repo: "/healthy-old", State: productStateProtected, Protected: true, lastActivity: base.Add(7 * time.Minute)},
		{Repo: "/working", State: productStatePublishing, PendingEvents: 1, lastActivity: base},
		{Repo: "/broken", State: productStateNeedsAction, ActionRequired: true, lastActivity: base},
	}
	sortProductListEntries(entries)

	visible, hidden := selectProductListEntries(entries, false)
	want := []string{"/broken", "/waiting", "/working", "/healthy-new", "/healthy-old"}
	if len(visible) != len(want) || hidden != 1 {
		t.Fatalf("visible=%d hidden=%d, want %d and 1: %+v", len(visible), hidden, len(want), visible)
	}
	for index, repo := range want {
		if visible[index].Repo != repo {
			t.Fatalf("visible[%d]=%s, want %s: %+v", index, visible[index].Repo, repo, visible)
		}
	}
}

func TestProductListSelectionKeepsPausedActionRequired(t *testing.T) {
	base := time.Date(2026, 8, 15, 10, 0, 0, 0, time.UTC)
	entries := []productListEntry{{
		Repo: "/blocked-drain", State: productStateNeedsAction,
		OperationalState: "paused", ActionRequired: true,
		lastActivity: base,
	}}
	for index := 0; index < productListDefaultRows+2; index++ {
		entries = append(entries, productListEntry{
			Repo:  fmt.Sprintf("/healthy-%d", index),
			State: productStateProtected, Protected: true,
			lastActivity: base.Add(time.Duration(index+1) * time.Minute),
		})
	}
	sortProductListEntries(entries)

	visible, hidden := selectProductListEntries(entries, false)
	if !productListContainsRepo(visible, "/blocked-drain") {
		t.Fatalf("paused action-required drain was hidden: %+v", visible)
	}
	if len(visible) != productListDefaultRows || hidden != 3 {
		t.Fatalf("visible=%d hidden=%d, want %d and 3",
			len(visible), hidden, productListDefaultRows)
	}
}

func TestProductListSelectionUsesPausedAsFallback(t *testing.T) {
	entries := []productListEntry{
		{Repo: "/working", State: productStatePublishing, PendingEvents: 1},
		{Repo: "/recent", State: productStateProtected, Protected: true},
		{Repo: "/paused-a", State: productStateNeedsAction, OperationalState: "paused"},
		{Repo: "/paused-b", State: productStateNeedsAction, OperationalState: "paused"},
	}
	sortProductListEntries(entries)
	visible, hidden := selectProductListEntries(entries, false)
	if len(visible) != len(entries) || hidden != 0 ||
		visible[2].Repo != "/paused-a" || visible[3].Repo != "/paused-b" {
		t.Fatalf("paused repositories were not used as the final fallback: %+v", visible)
	}
}

func TestProductListRelevantRowsAreNeverCapped(t *testing.T) {
	entries := make([]productListEntry, 0, 8)
	for index := 0; index < 8; index++ {
		entries = append(entries, productListEntry{
			Repo: fmt.Sprintf("/working-%d", index), State: productStatePublishing,
			PendingEvents: 1,
		})
	}
	visible, hidden := selectProductListEntries(entries, false)
	if len(visible) != len(entries) || hidden != 0 {
		t.Fatalf("visible=%d hidden=%d, want all %d active rows", len(visible), hidden, len(entries))
	}
}

func TestProductListAllControlsOnlyHumanFiltering(t *testing.T) {
	entries := make([]productListEntry, 0, 7)
	for index := 0; index < 7; index++ {
		entries = append(entries, productListEntry{
			Repo: fmt.Sprintf("/repo-%d", index), Protected: true,
			State: productStateProtected,
		})
	}
	original := productListCollect
	productListCollect = func(context.Context) (productListData, productState, error) {
		return productListData{UpdatedAt: time.Now().UTC().Format(time.RFC3339), Repos: entries}, productStateProtected, nil
	}
	t.Cleanup(func() { productListCollect = original })

	var compact bytes.Buffer
	if err := runProductListOnceView(context.Background(), &compact, false, false, false); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(compact.String(), "2 repositories hidden; use acd list --all") || strings.Contains(compact.String(), "repo-6") {
		t.Fatalf("default view did not stay compact:\n%s", compact.String())
	}

	var all bytes.Buffer
	if err := runProductListOnceView(context.Background(), &all, false, false, true); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(all.String(), "repositories hidden") || !strings.Contains(all.String(), "repo-6") {
		t.Fatalf("--all did not render every enabled repository:\n%s", all.String())
	}

	var jsonOut bytes.Buffer
	if err := runProductListOnceView(context.Background(), &jsonOut, true, false, false); err != nil {
		t.Fatal(err)
	}
	var envelope struct {
		Data productListData `json:"data"`
	}
	if err := json.Unmarshal(jsonOut.Bytes(), &envelope); err != nil {
		t.Fatal(err)
	}
	if len(envelope.Data.Repos) != len(entries) {
		t.Fatalf("JSON repos=%d, want exhaustive %d", len(envelope.Data.Repos), len(entries))
	}
}

func TestProductListWatchWaitsAfterEachFrame(t *testing.T) {
	original := productListCollect
	var started []time.Time
	productListCollect = func(context.Context) (productListData, productState, error) {
		started = append(started, time.Now())
		time.Sleep(30 * time.Millisecond)
		return productListData{UpdatedAt: time.Now().UTC().Format(time.RFC3339)}, productStateProtected, nil
	}
	t.Cleanup(func() { productListCollect = original })

	ctx, cancel := context.WithCancel(context.Background())
	out := &productListFrameWriter{cancel: cancel, want: 2}
	if err := runProductListWatchView(ctx, out, 20*time.Millisecond, false, false); err != nil {
		t.Fatal(err)
	}
	if len(started) < 2 {
		t.Fatalf("collections=%d, want at least 2", len(started))
	}
	if gap := started[1].Sub(started[0]); gap < 45*time.Millisecond {
		t.Fatalf("collection gap=%s, want collection time plus refresh interval", gap)
	}
}

func TestProductListWatchCancellationDoesNotRenderPartialFrame(t *testing.T) {
	original := productListCollect
	productListCollect = func(ctx context.Context) (productListData, productState, error) {
		<-ctx.Done()
		return productListData{Repos: []productListEntry{{Repo: "/partial", ActionRequired: true}}}, productStateNeedsAction, nil
	}
	t.Cleanup(func() { productListCollect = original })

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var out bytes.Buffer
	if err := runProductListWatchView(ctx, &out, time.Second, false, false); err != nil {
		t.Fatal(err)
	}
	if out.Len() != 0 {
		t.Fatalf("canceled collection rendered a partial frame:\n%s", out.String())
	}
}

func TestProductListTerminalWatchUsesAlternateScreen(t *testing.T) {
	original := productListCollect
	ctx, cancel := context.WithCancel(context.Background())
	frames := 0
	productListCollect = func(context.Context) (productListData, productState, error) {
		frames++
		if frames == 2 {
			cancel()
		}
		return productListData{UpdatedAt: time.Now().UTC().Format(time.RFC3339)}, productStateProtected, nil
	}
	t.Cleanup(func() { productListCollect = original })

	var out bytes.Buffer
	if err := runProductListWatchDisplay(ctx, &out, time.Millisecond, false, false, true); err != nil {
		t.Fatal(err)
	}
	got := out.String()
	if strings.Count(got, "\033[?1049h") != 1 || strings.Count(got, "\033[?1049l") != 1 {
		t.Fatalf("alternate screen was not entered and restored exactly once: %q", got)
	}
	if strings.Count(got, "\033[?25l") != 1 || strings.Count(got, "\033[?25h") != 1 {
		t.Fatalf("cursor visibility was not restored: %q", got)
	}
}

func TestProductListWatchKeepsKnownAttentionAcrossUnknownRead(t *testing.T) {
	original := productListCollect
	ctx, cancel := context.WithCancel(context.Background())
	frames := 0
	productListCollect = func(context.Context) (productListData, productState, error) {
		frames++
		entry := productListEntry{
			Repo: "/repo", State: productStateNeedsAction, ActionRequired: true,
			Protected: false,
		}
		if frames == 2 {
			entry = productListEntry{
				Repo: "/repo", State: productStateProtected,
				OperationalState: "healthy_idle", ProtectionUnknown: true,
			}
		}
		return productListData{UpdatedAt: time.Now().UTC().Format(time.RFC3339), Repos: []productListEntry{entry}}, productStateNeedsAction, nil
	}
	t.Cleanup(func() { productListCollect = original })

	out := &productListFrameWriter{cancel: cancel, want: 2}
	if err := runProductListWatchView(ctx, out, time.Millisecond, false, false); err != nil {
		t.Fatal(err)
	}
	if count := strings.Count(out.String(), "needs action"); count != 2 {
		t.Fatalf("known attention was not retained across unknown read, count=%d:\n%s", count, out.String())
	}
}

func TestProductListReadFailuresDoNotMigrate(t *testing.T) {
	t.Run("pre-checkpoint schema", func(t *testing.T) {
		dbPath := filepath.Join(t.TempDir(), "state.db")
		db, err := sql.Open("sqlite", dbPath)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := db.Exec(`PRAGMA user_version=19`); err != nil {
			t.Fatal(err)
		}
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
		record := central.RepoRecord{Path: t.TempDir(), StateDB: dbPath, RepositoryID: "repository-id", WorktreeID: "worktree-id"}
		if _, err := readProductListRepo(context.Background(), record, time.Now()); err == nil {
			t.Fatal("pre-checkpoint database unexpectedly produced a healthy overview")
		}
		assertProductListSchemaUnchanged(t, dbPath, 19)
	})

	t.Run("future schema", func(t *testing.T) {
		dbPath := filepath.Join(t.TempDir(), "state.db")
		db, err := sql.Open("sqlite", dbPath)
		if err != nil {
			t.Fatal(err)
		}
		future := state.SchemaVersion + 1
		if _, err := db.Exec(fmt.Sprintf(`PRAGMA user_version=%d`, future)); err != nil {
			t.Fatal(err)
		}
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
		record := central.RepoRecord{Path: t.TempDir(), StateDB: dbPath, RepositoryID: "repository-id", WorktreeID: "worktree-id"}
		if _, err := readProductListRepo(context.Background(), record, time.Now()); err == nil || !strings.Contains(err.Error(), "newer than supported") {
			t.Fatalf("future schema error=%v", err)
		}
		assertProductListSchemaUnchanged(t, dbPath, future)
	})

	t.Run("corrupt database", func(t *testing.T) {
		dbPath := filepath.Join(t.TempDir(), "state.db")
		if err := os.WriteFile(dbPath, []byte("not sqlite"), 0o600); err != nil {
			t.Fatal(err)
		}
		record := central.RepoRecord{Path: t.TempDir(), StateDB: dbPath, RepositoryID: "repository-id", WorktreeID: "worktree-id"}
		if _, err := readProductListRepo(context.Background(), record, time.Now()); err == nil {
			t.Fatal("corrupt database unexpectedly produced an overview")
		}
		contents, err := os.ReadFile(dbPath)
		if err != nil {
			t.Fatal(err)
		}
		if string(contents) != "not sqlite" {
			t.Fatalf("corrupt database was modified: %q", contents)
		}
	})
}

func assertProductListSchemaUnchanged(t *testing.T, dbPath string, want int) {
	t.Helper()
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var version int
	if err := db.QueryRow(`PRAGMA user_version`).Scan(&version); err != nil {
		t.Fatal(err)
	}
	if version != want {
		t.Fatalf("schema version=%d, want unchanged %d", version, want)
	}
}

func TestProductListSortingUsesSeverityThenActivity(t *testing.T) {
	base := time.Date(2026, 8, 15, 10, 0, 0, 0, time.UTC)
	entries := []productListEntry{
		{Repo: "/healthy", State: productStateProtected, lastActivity: base.Add(4 * time.Minute)},
		{Repo: "/waiting", State: productStateWaiting, lastActivity: base.Add(3 * time.Minute)},
		{Repo: "/working-old", State: productStatePublishing, lastActivity: base},
		{Repo: "/broken", ActionRequired: true, State: productStateNeedsAction, lastActivity: base},
		{Repo: "/working-new", State: productStatePublishing, lastActivity: base.Add(2 * time.Minute)},
		{Repo: "/paused", State: productStateNeedsAction, OperationalState: "paused", lastActivity: base.Add(5 * time.Minute)},
	}
	sortProductListEntries(entries)
	want := []string{"/broken", "/waiting", "/working-new", "/working-old", "/healthy", "/paused"}
	for index := range want {
		if entries[index].Repo != want[index] {
			t.Fatalf("order[%d]=%s, want %s: %+v", index, entries[index].Repo, want[index], entries)
		}
	}
}

func TestProductListHeartbeatsDoNotChangeRecentWork(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "state.db")
	db, err := state.Open(ctx, dbPath)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	if err := state.SaveDaemonState(ctx, db, state.DaemonState{
		PID: os.Getpid(), Mode: "running", HeartbeatTS: float64(now.Unix()), UpdatedTS: float64(now.Unix()),
	}); err != nil {
		t.Fatal(err)
	}
	if err := state.RegisterClient(ctx, db, state.Client{
		SessionID: "session", Harness: "codex", LastSeenTS: float64(now.Unix()),
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	record := central.RepoRecord{
		Path: t.TempDir(), StateDB: dbPath, LastSeenTS: 100,
		RepositoryID: "repository-id", WorktreeID: "worktree-id",
	}
	overview, err := readProductListRepo(ctx, record, now)
	if err != nil {
		t.Fatal(err)
	}
	want := time.Unix(record.LastSeenTS, 0)
	if !overview.lastActivity.Equal(want) {
		t.Fatalf("heartbeat changed recent work from %s to %s", want, overview.lastActivity)
	}
}

func TestProductListCompactLabelsDisambiguateCollisions(t *testing.T) {
	entries := []productListEntry{
		{Repo: "/work/team/project", RepoHash: "abcdef123", State: productStateProtected},
		{Repo: "/personal/project", RepoHash: "123456789", State: productStateProtected},
		{Repo: "/work/unique", State: productStateProtected},
	}
	labels := productListLabels(entries)
	want := []string{"project [abcdef]", "project [123456]", "unique"}
	for index := range want {
		if labels[index] != want[index] {
			t.Fatalf("label[%d]=%q, want %q", index, labels[index], want[index])
		}
	}
}

func TestProductListStatusMapsOperationalWork(t *testing.T) {
	tests := []struct {
		name  string
		entry productListEntry
		want  string
	}{
		{name: "validation", entry: productListEntry{State: productStateWaiting, OperationalState: "validating"}, want: "working"},
		{name: "checkpoint", entry: productListEntry{State: productStateWaiting, OperationalState: "busy"}, want: "working"},
		{name: "intent wait", entry: productListEntry{State: productStateWaiting, OperationalState: "waiting", PendingEvents: 2}, want: "waiting"},
		{name: "manual pause", entry: productListEntry{State: productStateNeedsAction, OperationalState: "paused"}, want: "paused"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := productListStatus(test.entry); got != test.want {
				t.Fatalf("status=%q, want %q", got, test.want)
			}
		})
	}
}

func TestProductListStartingWorkerIsWorking(t *testing.T) {
	record := central.RepoRecord{Path: "/repo", RepoHash: "abcdef", RepositoryID: "repository-id", WorktreeID: "worktree-id"}
	overview := productListRepoOverview{
		report: statusReport{Repo: record.Path, Daemon: "stopped", OperationalState: "stopped"},
	}
	entry := productListEntryFromOverview(record, supervisor.WorkerStatus{
		RepositoryID: record.RepositoryID, State: "backoff", LastError: "retrying",
	}, overview, nil)
	if entry.ActionRequired || productListStatus(entry) != "working" {
		t.Fatalf("retrying worker entry=%+v status=%q", entry, productListStatus(entry))
	}

	overview.report.PublicationDrain.Phase = state.PublicationDrainNeedsAction
	entry = productListEntryFromOverview(record, supervisor.WorkerStatus{
		RepositoryID: record.RepositoryID, State: "backoff", LastError: "retrying",
	}, overview, nil)
	if !entry.ActionRequired || productListStatus(entry) != "needs action" {
		t.Fatalf("drain failure was hidden by worker retry: %+v status=%q", entry, productListStatus(entry))
	}
}

func TestProductListIncompleteCheckpointIsWorking(t *testing.T) {
	record := central.RepoRecord{
		Path: "/repo", RepoHash: "abcdef", RepositoryID: "repository-id", WorktreeID: "worktree-id",
	}
	overview := productListRepoOverview{report: statusReport{
		Repo: record.Path, Daemon: "running", PID: os.Getpid(),
		CheckpointProtectionAvailable: true, Protected: false,
		OperationalState: "waiting",
	}}
	entry := productListEntryFromOverview(record, supervisor.WorkerStatus{
		RepositoryID: record.RepositoryID, State: "running",
	}, overview, nil)
	if entry.Protected || entry.ActionRequired || productListStatus(entry) != "working" {
		t.Fatalf("active checkpoint entry=%+v status=%q", entry, productListStatus(entry))
	}

	overview.report.checkpointNeedsAction = true
	entry = productListEntryFromOverview(record, supervisor.WorkerStatus{
		RepositoryID: record.RepositoryID, State: "running",
	}, overview, nil)
	if !entry.ActionRequired || productListStatus(entry) != "needs action" {
		t.Fatalf("checkpoint failure was hidden: %+v status=%q", entry, productListStatus(entry))
	}
}

func TestProductListTransientReadFailureIsNotNeedsAction(t *testing.T) {
	record := central.RepoRecord{
		Path: "/repo", RepoHash: "abcdef", LastSeenTS: time.Now().Unix(),
		RepositoryID: "repository-id", WorktreeID: "worktree-id",
	}
	entry := productListEntryFromOverview(record, supervisor.WorkerStatus{
		RepositoryID: record.RepositoryID, State: "running",
	}, productListRepoOverview{}, context.DeadlineExceeded)
	if entry.ActionRequired || entry.State != productStateProtected ||
		productListStatus(entry) != "healthy" || !entry.ProtectionUnknown {
		t.Fatalf("transient read failure became an alert: %+v", entry)
	}
	var out bytes.Buffer
	if err := renderProductListDashboard(&out, []productListEntry{entry}, false, false); err != nil {
		t.Fatal(err)
	}
	if line := productListLineForRepo(t, out.String(), "repo"); !strings.Contains(line, "-     -") {
		t.Fatalf("unknown protection was not rendered distinctly: %s", line)
	}

	entry = productListEntryFromOverview(record, supervisor.WorkerStatus{
		RepositoryID: record.RepositoryID, State: "starting",
	}, productListRepoOverview{}, errors.New("database is locked (SQLITE_BUSY)"))
	if entry.ActionRequired || productListStatus(entry) != "working" || !entry.ProtectionUnknown {
		t.Fatalf("starting worker contention became an alert: %+v", entry)
	}
}

func TestProductListVerboseIncludesOperationalDetails(t *testing.T) {
	entry := productListEntry{
		Repo: "/repo", WorkerState: "running", Clients: 2, Protected: true,
		State: productStateNeedsAction, ActionRequired: true, BlockedEvents: 1,
		LastCommitOID: "1234567890abcdef", Summary: "Publication is blocked.",
		NextAction: "Run `acd doctor --repo '/repo'`.",
	}
	var out bytes.Buffer
	if err := renderProductListDashboard(&out, []productListEntry{entry}, true, false); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"REPOSITORY", "WORKER", "LIVE TOOLS", "BLOCKED", "LAST COMMIT", "needs action", "acd doctor --repo '/repo'"} {
		if !strings.Contains(out.String(), want) {
			t.Fatalf("verbose output missing %q:\n%s", want, out.String())
		}
	}
}

func productListContainsRepo(entries []productListEntry, repo string) bool {
	for _, entry := range entries {
		if entry.Repo == repo {
			return true
		}
	}
	return false
}

func TestProductListCollectionIsBoundedAndReadsSupervisorOnce(t *testing.T) {
	roots := withIsolatedHome(t)
	if err := central.WithLock(roots, func(registry *central.Registry) error {
		for index := 0; index < 50; index++ {
			path := filepath.Join(t.TempDir(), fmt.Sprintf("repo-%02d", index))
			registry.UpsertRepo(path, fmt.Sprintf("%016x", index+1), filepath.Join(path, "state.db"), "codex", int64(index+1))
			record := &registry.Repos[len(registry.Repos)-1]
			record.RepositoryID = fmt.Sprintf("%016x", index+1)
			record.WorktreeID = fmt.Sprintf("%016x", index+101)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	originalSupervisor, originalReader := productListReadSupervisor, productListReadRepo
	t.Cleanup(func() {
		productListReadSupervisor = originalSupervisor
		productListReadRepo = originalReader
	})
	var supervisorCalls atomic.Int32
	productListReadSupervisor = func(context.Context, paths.Roots) (supervisor.Status, bool) {
		supervisorCalls.Add(1)
		return supervisor.Status{}, true
	}
	var active, peak atomic.Int32
	productListReadRepo = func(ctx context.Context, record central.RepoRecord, now time.Time) (productListRepoOverview, error) {
		current := active.Add(1)
		defer active.Add(-1)
		for {
			observed := peak.Load()
			if current <= observed || peak.CompareAndSwap(observed, current) {
				break
			}
		}
		select {
		case <-ctx.Done():
			return productListRepoOverview{}, ctx.Err()
		case <-time.After(10 * time.Millisecond):
		}
		return healthyProductListOverview(record, now), nil
	}

	data, _, err := collectProductList(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(data.Repos) != 50 {
		t.Fatalf("repos=%d, want 50", len(data.Repos))
	}
	if supervisorCalls.Load() != 1 {
		t.Fatalf("supervisor calls=%d, want 1", supervisorCalls.Load())
	}
	if peak.Load() < 2 || peak.Load() > productListReadLimit {
		t.Fatalf("peak concurrency=%d, want 2..%d", peak.Load(), productListReadLimit)
	}
}

func TestProductListSlowRepositoryDoesNotBlockOtherRows(t *testing.T) {
	roots := withIsolatedHome(t)
	if err := central.WithLock(roots, func(registry *central.Registry) error {
		for index := 0; index < 10; index++ {
			path := filepath.Join(t.TempDir(), fmt.Sprintf("repo-%02d", index))
			registry.UpsertRepo(path, fmt.Sprintf("%016x", index+1), filepath.Join(path, "state.db"), "codex", int64(index+1))
			record := &registry.Repos[len(registry.Repos)-1]
			record.RepositoryID = fmt.Sprintf("%016x", index+1)
			record.WorktreeID = fmt.Sprintf("%016x", index+101)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	originalReader := productListReadRepo
	t.Cleanup(func() { productListReadRepo = originalReader })
	productListReadRepo = func(ctx context.Context, record central.RepoRecord, now time.Time) (productListRepoOverview, error) {
		if strings.HasSuffix(record.Path, "repo-00") {
			<-ctx.Done()
			return productListRepoOverview{}, ctx.Err()
		}
		return healthyProductListOverview(record, now), nil
	}

	started := time.Now()
	data, stateName, err := collectProductList(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(started); elapsed < productListReadTimeout || elapsed > time.Second {
		t.Fatalf("elapsed=%s, want one bounded repository timeout", elapsed)
	}
	if len(data.Repos) != 10 || stateName != productStateProtected {
		t.Fatalf("unexpected bounded result: state=%s repos=%+v", stateName, data.Repos)
	}
	var slow productListEntry
	for _, entry := range data.Repos {
		if strings.HasSuffix(entry.Repo, "repo-00") {
			slow = entry
			break
		}
	}
	if slow.Repo == "" || slow.ActionRequired || !slow.ProtectionUnknown {
		t.Fatalf("slow repository was not retained as a transient unknown: %+v", slow)
	}
}

func healthyProductListOverview(record central.RepoRecord, now time.Time) productListRepoOverview {
	return productListRepoOverview{
		report: statusReport{
			Repo: record.Path, RepoHash: record.RepoHash, Daemon: "running", PID: os.Getpid(),
			CheckpointProtectionAvailable: true, Protected: true,
			LatestCheckpointID: "cp-test", OperationalState: "healthy_idle",
		},
		lastActivity: now,
	}
}

func registerProductListNeedsActionRepo(t *testing.T) string {
	t.Helper()
	roots := withIsolatedHome(t)
	return registerProductListNeedsActionRepoWithRoots(t, roots)
}

func registerProductListNeedsActionRepoWithRoots(t *testing.T, roots paths.Roots) string {
	t.Helper()
	repo := materializeTestRepo(t, false)
	missingStateDB := filepath.Join(repo, ".git", "acd", "missing.db")
	registerRepo(t, roots, repo, missingStateDB, "codex")
	if err := central.WithLock(roots, func(registry *central.Registry) error {
		for i := range registry.Repos {
			if registry.Repos[i].Path == repo {
				registry.Repos[i].RepositoryID = "repository-id"
				registry.Repos[i].WorktreeID = "worktree-id"
				return nil
			}
		}
		return fmt.Errorf("registered repository %s not found", repo)
	}); err != nil {
		t.Fatalf("update registry identity: %v", err)
	}
	return repo
}

func registerProductListOffRepo(t *testing.T, roots paths.Roots, repo string) string {
	t.Helper()
	registerRepo(t, roots, repo, filepath.Join(repo, ".git", "acd", "missing.db"), "codex")
	return repo
}

func materializeProductListRepoWithSpaces(t *testing.T) string {
	t.Helper()
	source := materializeTestRepo(t, false)
	target := filepath.Join(t.TempDir(), "repo with spaces")
	if err := os.Rename(source, target); err != nil {
		t.Fatalf("move repository to spaced path: %v", err)
	}
	realTarget, err := filepath.EvalSymlinks(target)
	if err != nil {
		t.Fatalf("canonicalize spaced repository path: %v", err)
	}
	return realTarget
}

func productListLineForRepo(t *testing.T, output, repo string) string {
	t.Helper()
	for _, line := range strings.Split(output, "\n") {
		if strings.HasPrefix(line, repo+" ") {
			return line
		}
	}
	t.Fatalf("repository row %q missing:\n%s", repo, output)
	return ""
}

type productListFrameWriter struct {
	mu     sync.Mutex
	buf    bytes.Buffer
	cancel context.CancelFunc
	frames int
	want   int
}

func (w *productListFrameWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	n, err := w.buf.Write(p)
	w.frames += strings.Count(string(p), "Updated:")
	if w.frames >= w.want {
		w.cancel()
	}
	return n, err
}

func (w *productListFrameWriter) String() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.buf.String()
}

func (w *productListFrameWriter) frameCount() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.frames
}
