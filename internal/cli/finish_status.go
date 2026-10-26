package cli

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/central"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/checkpoint"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/daemon"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/identity"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/paths"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

var errFinishScopeUnknown = errors.New("saved exclusion scope is unavailable")

type finishHistoryStatusReport struct {
	PlanID  string `json:"plan_id"`
	Attempt int    `json:"attempt"`
	State   string `json:"state"`
	DryRun  bool   `json:"dry_run"`
	Reason  string `json:"reason,omitempty"`
}

type finishStatusReport struct {
	EvidenceCalls  int                  `json:"evidence_calls,omitempty"`
	RequestID      string               `json:"request_id"`
	Attempt        int                  `json:"attempt"`
	State          string               `json:"state"`
	Stage          string               `json:"stage,omitempty"`
	Mode           string               `json:"mode"`
	Style          string               `json:"style"`
	CheckpointID   string               `json:"checkpoint_id"`
	TargetTree     string               `json:"target_tree"`
	BranchRef      string               `json:"branch_ref"`
	DryRun         bool                 `json:"dry_run"`
	Published      bool                 `json:"published"`
	NoChanges      bool                 `json:"no_changes"`
	CommitsCreated int                  `json:"commits_created"`
	NewHead        string               `json:"new_head,omitempty"`
	ReasonCode     string               `json:"reason_code,omitempty"`
	Reason         string               `json:"reason,omitempty"`
	Verification   []daemon.FinishCheck `json:"verification"`
	UpdatedTS      float64              `json:"updated_ts"`
	sourceTree     string
}

func readFinishStatus(ctx context.Context, db *sql.DB, worktree string) (*finishStatusReport, error) {
	return readFinishStatusForCheckpoint(ctx, db, worktree, "")
}

func readFinishStatusForCheckpoint(ctx context.Context, db *sql.DB, worktree, checkpointID string) (*finishStatusReport, error) {
	var exists int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='finish_requests'`).Scan(&exists); err != nil {
		return nil, err
	}
	if exists == 0 {
		return nil, nil
	}
	var r state.FinishRequest
	var target, plan, result string
	err := db.QueryRowContext(ctx, `SELECT request_id,attempt,target_json,state,stage,dry_run,plan_json,result_json,reason_code,reason,updated_ts FROM finish_requests WHERE worktree_id=? AND (?='' OR checkpoint_id=?) ORDER BY CASE WHEN state IN ('running','prepared','needs_attention') THEN 0 ELSE 1 END,created_ts DESC,attempt DESC LIMIT 1`, worktree, checkpointID, checkpointID).Scan(&r.ID, &r.Attempt, &target, &r.State, &r.Stage, &r.DryRun, &plan, &result, &r.ReasonCode, &r.Reason, &r.UpdatedTS)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal([]byte(target), &r.Target); err != nil {
		return nil, fmt.Errorf("ACD could not read the saved finish details.\nNext: run acd doctor.\nDetails: finish target: %w", err)
	}
	report := &finishStatusReport{
		RequestID: r.ID, Attempt: r.Attempt, State: r.State, Stage: r.Stage,
		Mode: r.Target.Mode, Style: r.Target.Style, CheckpointID: r.Target.CheckpointID,
		TargetTree: r.Target.TargetTree, BranchRef: r.Target.BranchRef, DryRun: r.DryRun,
		ReasonCode: r.ReasonCode, Reason: r.Reason, UpdatedTS: r.UpdatedTS,
		Verification: []daemon.FinishCheck{}, sourceTree: r.Target.CheckpointTree,
	}
	var progress struct {
		EvidenceCalls int `json:"evidence_calls"`
	}
	if json.Unmarshal([]byte(result), &progress) == nil {
		report.EvidenceCalls = progress.EvidenceCalls
	}
	var savedReview struct {
		Disclosure struct {
			EvidenceCalls int `json:"evidence_calls"`
		} `json:"disclosure"`
	}
	if json.Unmarshal([]byte(plan), &savedReview) == nil {
		report.EvidenceCalls = max(report.EvidenceCalls, savedReview.Disclosure.EvidenceCalls)
	}
	if result != "" {
		var saved daemon.FinishResult
		if err := json.Unmarshal([]byte(result), &saved); err != nil {
			return nil, fmt.Errorf("ACD could not read the saved finish details.\nNext: run acd doctor.\nDetails: finish result: %w", err)
		}
		report.Published = saved.Published
		report.NoChanges = saved.NoChanges
		report.CommitsCreated = saved.CommitsCreated
		report.NewHead = saved.NewHead
	}
	if plan != "" {
		var saved daemon.FinishSavedPlan
		if err := json.Unmarshal([]byte(plan), &saved); err != nil {
			return nil, fmt.Errorf("ACD could not read the saved finish details.\nNext: run acd doctor.\nDetails: finish checks: %w", err)
		}
		for _, step := range saved.Steps {
			if step.Check.TreeOID != "" {
				report.Verification = append(report.Verification, step.Check)
			}
		}
	}
	return report, nil
}

func readFinishHistoryStatus(ctx context.Context, db *sql.DB, worktree string) (*finishHistoryStatusReport, error) {
	var exists int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='finish_history_requests'").Scan(&exists); err != nil {
		return nil, err
	}
	if exists == 0 {
		return nil, nil
	}
	var r finishHistoryStatusReport
	var body string
	err := db.QueryRowContext(ctx, "SELECT plan_id,attempt,status,request_json,error FROM finish_history_requests WHERE worktree_id=? AND status IN ('running','prepared','needs_attention') LIMIT 1", worktree).Scan(&r.PlanID, &r.Attempt, &r.State, &body, &r.Reason)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var request state.FinishHistoryRequest
	if err := json.Unmarshal([]byte(body), &request); err != nil {
		return nil, err
	}
	r.DryRun = request.DryRun
	return &r, nil
}

// readFinishStatusReport shares one read-only projection across all views.
// Protection describes the last completed observation, not unscanned live bytes.
func readFinishStatusReport(ctx context.Context, rec central.RepoRecord, now time.Time) (statusReport, error) {
	report := statusReport{Repo: rec.Path, RepoHash: rec.RepoHash, Daemon: "stopped", Clients: []statusClient{}}
	db, err := openStateDBReadOnly(ctx, rec.StateDB)
	if err != nil {
		return report, err
	}
	defer db.Close()
	var schema int
	if err := db.QueryRowContext(ctx, "PRAGMA user_version").Scan(&schema); err != nil {
		return report, err
	}
	if schema > state.SchemaVersion {
		return report, fmt.Errorf("ACD could not check protection.\nNext: run acd doctor.\nDetails: state schema %d is newer than supported %d", schema, state.SchemaVersion)
	}
	if schema < 20 {
		return report, nil
	}
	report.CheckpointProtectionAvailable = true
	report.PublicationOutcome.CheckpointAvailable = true
	get := func(key string) string { v, _, _ := metaLookup(ctx, db, key); return v }
	report.LatestCheckpointID = get(daemon.MetaKeyProtectionCheckpointID)
	fmt.Sscan(get(daemon.MetaKeyProtectionObservationEpoch), &report.ObservationEpoch)
	fmt.Sscan(get(daemon.MetaKeyProtectionCoveredEpoch), &report.CoveredEpoch)
	fmt.Sscan(get(daemon.MetaKeyProtectionFullPollTS), &report.FullPollTS)
	report.Protected = get(daemon.MetaKeyProtectionComplete) == "true" && report.LatestCheckpointID != "" && report.ObservationEpoch > 0 && report.ObservationEpoch == report.CoveredEpoch
	report.CaptureHealth, err = state.ReadCaptureHealth(ctx, db)
	if err != nil {
		return report, err
	}
	if report.CaptureHealth.Error != "" {
		report.Protected = false
		report.CaptureErrors = max(1, report.CaptureHealth.IssueCount)
	}
	report.CheckpointMaintenance, err = readCheckpointMaintenance(ctx, db)
	if err != nil {
		return report, err
	}
	report.CheckpointRetentionOverBudget = report.CheckpointMaintenance.OverBudget
	var heartbeat float64
	err = db.QueryRowContext(ctx, `SELECT pid,mode,heartbeat_ts FROM daemon_state WHERE id=1`).Scan(&report.PID, &report.Daemon, &heartbeat)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return report, err
	}
	report.HeartbeatTS = int64(heartbeat)
	report.HeartbeatAgeSeconds = max(0, int64(now.Unix())-int64(heartbeat))
	responsive := report.Daemon == "running" && report.PID > 0 && identity.Alive(report.PID) && heartbeat > 0 && now.Sub(time.Unix(0, int64(heartbeat*1e9))) <= 3*time.Second
	report.Stale = report.Daemon == "running" && !responsive
	hasClients, err := state.LegacyTableExists(ctx, db, "daemon_clients")
	if err != nil {
		return report, err
	}
	if hasClients {
		rows, err := db.QueryContext(ctx, `SELECT session_id,harness,COALESCE(watch_pid,0),last_seen_ts FROM daemon_clients ORDER BY last_seen_ts DESC`)
		if err != nil {
			return report, err
		}
		for rows.Next() {
			var client statusClient
			var seen float64
			if err := rows.Scan(&client.SessionID, &client.Harness, &client.WatchPID, &seen); err != nil {
				rows.Close()
				return report, err
			}
			client.LastSeenTS = int64(seen)
			client.LastSeenAgeS = max(0, now.Unix()-int64(seen))
			report.Clients = append(report.Clients, client)
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return report, err
		}
		if err := rows.Close(); err != nil {
			return report, err
		}
	}

	report.PublicationProgress = publicationProgressReport{Phase: "saved", WorkerResponsive: responsive, HeartbeatAgeSeconds: report.HeartbeatAgeSeconds, LastProgressTS: report.CaptureHealth.LastProgressTS}
	report.PublicationProgress.LastProgressAgeSeconds = max(0, now.Unix()-int64(report.CaptureHealth.LastProgressTS))
	liveCount, err := productWorktreeChangeCount(ctx, rec.Path)
	if err != nil {
		return report, err
	}
	report.WorktreeClean = liveCount == 0
	report.AllChangesCommittedInGit = report.WorktreeClean
	outcome := &report.PublicationOutcome
	outcome.Legacy, err = state.ReadFinishLegacyCutover(ctx, rec.StateDB)
	if err != nil {
		if ctx.Err() != nil {
			return report, ctx.Err()
		}
		outcome.Legacy = state.FinishLegacyCutover{State: "needs_attention", ReasonCode: "legacy_archive_unreadable", Reason: "Legacy archive could not be read: " + err.Error()}
	}
	outcome.WorktreeChanges = liveCount
	outcome.Protected = report.Protected
	outcome.Finish, err = readFinishStatus(ctx, db, checkpoint.WorktreeID(rec.Path))
	if err != nil {
		return report, err
	}
	outcome.History, err = readFinishHistoryStatus(ctx, db, checkpoint.WorktreeID(rec.Path))
	if err != nil {
		return report, err
	}
	scope, scopeKnown := readFinishProtectionScope(get(daemon.MetaKeyProtectionScope), report.LatestCheckpointID)
	head, headErr := git.RevParse(ctx, rec.Path, "HEAD")
	if headErr != nil && !errors.Is(headErr, git.ErrRefNotFound) {
		return report, headErr
	}
	branch, err := git.Run(ctx, git.RunOpts{Dir: rec.Path}, "symbolic-ref", "--quiet", "--no-recurse", "HEAD")
	if err == nil {
		report.BranchRef = strings.TrimSpace(string(branch))
	}
	if report.LatestCheckpointID != "" {
		cp, err := readFinishCheckpoint(ctx, rec.StateDB, report.LatestCheckpointID)
		if err != nil {
			return report, err
		}
		if proofErr := verifyFinishCheckpointRef(ctx, rec.Path, cp); proofErr != nil {
			if ctx.Err() != nil {
				return report, ctx.Err()
			}
			report.Protected = false
			outcome.Protected = false
			report.CaptureErrors = max(1, report.CaptureErrors)
			report.CaptureHealth.State = "blocked"
			report.CaptureHealth.Error = "ACD could not verify the saved checkpoint: " + proofErr.Error()
			report.CaptureHealth.CheckpointID = cp.ID
			report.CaptureHealth.IssueCount = max(1, report.CaptureHealth.IssueCount)
		} else {
			if cp.Partial || cp.Phase != state.CheckpointCompleted {
				report.Protected = false
				outcome.Protected = false
			}
			if cp.Partial && report.CaptureHealth.Error == "" {
				report.CaptureHealth = state.CaptureHealth{State: "blocked", Error: "ACD could not save some files included in protection.", CheckpointID: cp.ID, IssueCount: len(cp.CaptureIssues), Issues: cp.CaptureIssues}
			}
			count, err := finishSavedChangeCount(ctx, scope, rec.Path, head, cp.TreeOID)
			if errors.Is(err, errFinishScopeUnknown) {
				scopeKnown = false
				err = nil
			}
			if err != nil {
				return report, err
			}
			outcome.PendingChanges = count
			outcome.WaitingChanges = count
			outcome.CountsKnown = scopeKnown
			committed := count == 0
			outcome.BranchCommitted = &committed
			if outcome.Finish != nil && outcome.Finish.sourceTree != "" {
				outcome.LaterChanges, err = finishSavedChangeCount(ctx, scope, rec.Path, outcome.Finish.sourceTree, cp.TreeOID)
				if errors.Is(err, errFinishScopeUnknown) {
					scopeKnown = false
					outcome.CountsKnown = false
					err = nil
				}
				if err != nil {
					return report, err
				}
			}
			var events, published, recovered int
			hasCaptures, err := state.LegacyTableExists(ctx, db, "capture_events")
			if err != nil {
				return report, err
			}
			if hasCaptures {
				err = db.QueryRowContext(ctx, `SELECT COUNT(*),COALESCE(SUM(e.state='published'),0),COALESCE(SUM(e.state='recovered'),0) FROM checkpoint_events ce JOIN capture_events e ON e.seq=ce.event_seq WHERE ce.checkpoint_id=?`, cp.ID).Scan(&events, &published, &recovered)
			} else {
				// Missing legacy evidence cannot resolve existing archived membership.
				err = db.QueryRowContext(ctx, `SELECT COUNT(*) FROM checkpoint_events WHERE checkpoint_id=?`, cp.ID).Scan(&events)
			}
			if err != nil {
				return report, err
			}
			outcome.RecoveredChanges = recovered
			// Deprecated booleans retain resolved-work semantics for legacy membership.
			outcome.Resolved = (events > 0 && events == published+recovered) || (events == 0 && scopeKnown && committed)
			if cp.Partial || !scopeKnown {
				outcome.CountsKnown = false
				outcome.BranchCommitted = nil
				outcome.PendingChanges = 0
				outcome.WaitingChanges = 0
				outcome.LaterChanges = 0
				if cp.Partial {
					outcome.Resolved = false
				}
			}
		}
	}
	if f := outcome.Finish; f != nil {
		report.PublicationProgress.Strategy = f.Mode
		report.PublicationProgress.Phase = f.State
		if f.Stage != "" {
			report.PublicationProgress.Phase = f.Stage
		}
		report.Busy = f.State == state.FinishRunning || f.State == state.FinishPrepared
		if f.Published {
			outcome.LaterChanges = liveCount
			report.LastCommitOID = f.NewHead
			report.LastCommitTS = int64(f.UpdatedTS)
		}
		if f.State == state.FinishFailed {
			if outcome.CountsKnown && outcome.PendingChanges == 0 && outcome.WorktreeChanges == 0 {
				outcome.LaterChanges = 0
			}
			report.PublicationProgress.Phase = "saved"
		}
	}
	if h := outcome.History; h != nil && (h.State == "running" || h.State == "prepared") && !finishStatusTakesPriority(outcome.Finish) {
		report.Busy = true
		report.PublicationProgress.Phase = "history_" + h.State
	}
	report.CheckpointPublishedByACD = report.Protected && outcome.Resolved
	report.OperationalState = "saved"
	if !responsive {
		report.OperationalState = "stopped"
	} else if report.CaptureHealth.Error != "" {
		report.OperationalState = "protection_incomplete"
		if !report.Busy {
			report.PublicationProgress.Phase = "protection_incomplete"
		}
	} else if report.Busy {
		report.OperationalState = "finishing"
	} else if !report.Protected {
		report.OperationalState = "checkpointing"
		if !finishStatusTakesPriority(outcome.Finish) {
			report.PublicationProgress.Phase = "checkpointing"
		}
	} else if outcome.CountsKnown && outcome.PendingChanges == 0 && liveCount == 0 {
		report.OperationalState = "up_to_date"
	}
	return report, nil
}

// Status inspects coverage without asking the restore resolver to approve it.
func readFinishCheckpoint(ctx context.Context, path, id string) (state.Checkpoint, error) {
	projection, err := state.ReadCheckpointProjection(ctx, path, 1)
	if err != nil {
		return state.Checkpoint{}, err
	}
	if projection.Latest != nil && projection.Latest.ID == id {
		return *projection.Latest, nil
	}
	return state.ResolveCheckpoint(ctx, path, id)
}

// Observe the literal pin without acquiring ref locks or repairing it. SQLite
// coverage alone cannot prove that Git still retains the checkpoint.
func verifyFinishCheckpointRef(ctx context.Context, repo string, cp state.Checkpoint) error {
	if cp.Ref == "" || cp.CommitOID == "" {
		return errors.New("checkpoint ref identity is missing")
	}
	body, err := git.RunWithLimit(ctx, git.RunOpts{Dir: repo, Timeout: git.DefaultReadTimeout}, 1<<20, "for-each-ref", "--format=%(refname)%00%(objectname)%00%(symref)", cp.Ref)
	if err != nil {
		return err
	}
	expected := cp.Ref + "\x00" + cp.CommitOID + "\x00"
	if strings.TrimSuffix(string(body), "\n") != expected {
		return errors.New("checkpoint ref is missing, moved, or symbolic")
	}
	return nil
}

// Count only eligible saved paths. Missing excluded entries in a checkpoint
// never imply that sensitive files or Gitlinks should be deleted from a branch.
func finishSavedChangeCount(ctx context.Context, scope daemon.ProtectionScope, repo, base, target string) (int, error) {
	if target == "" {
		return 0, nil
	}
	var before []git.TreeEntry
	var err error
	if base != "" {
		before, err = git.LsTree(ctx, repo, base, true)
		if err != nil {
			return 0, err
		}
	}
	after, err := git.LsTree(ctx, repo, target, true)
	if err != nil {
		return 0, err
	}
	sensitive := state.NewSensitiveMatcherFromPatterns(scope.Sensitive, scope.SensitiveAllow)
	safe := state.NewSafeIgnoreMatcherFromPatterns(scope.SafeIgnore)
	excluded := map[string]bool{}
	for _, path := range scope.Submodules {
		excluded[path] = true
	}
	for _, entry := range before {
		if entry.Mode == "160000" {
			excluded[entry.Path] = true
		}
	}
	eligible := func(path string) bool {
		if excluded[path] || sensitive.Match(path) || safe.MatchFile(path) {
			return false
		}
		for parent := path; strings.Contains(parent, "/"); {
			parent = parent[:strings.LastIndex(parent, "/")]
			if excluded[parent] || sensitive.MatchDirectory(parent) || safe.MatchDirectory(parent) {
				return false
			}
		}
		return true
	}
	old, newEntries := map[string]git.TreeEntry{}, map[string]git.TreeEntry{}
	for _, entry := range before {
		if eligible(entry.Path) {
			old[entry.Path] = entry
		}
	}
	for _, entry := range after {
		if eligible(entry.Path) {
			newEntries[entry.Path] = entry
		}
	}
	count := 0
	var absent []string
	for path, entry := range old {
		next, exists := newEntries[path]
		if !exists {
			absent = append(absent, path)
		} else if next.Mode != entry.Mode || next.OID != entry.OID {
			count++
		}
	}
	for path := range newEntries {
		if _, exists := old[path]; !exists {
			count++
		}
	}
	// A saved entry already proves eligibility. Later ignore edits cannot hide it.
	// For omitted base entries, a live ignore match cannot distinguish exclusion
	// from deletion, so do not invent an exact count from it.
	if len(absent) > 0 {
		// Match finish eligibility: skip-worktree entries are deliberately
		// omitted by sparse checkout, rather than saved deletions.
		flags, err := git.Run(ctx, git.RunOpts{Dir: repo, Timeout: git.DefaultReadTimeout}, "ls-files", "-v", "-z")
		if err != nil {
			return 0, err
		}
		skipWorktree := map[string]bool{}
		for _, entry := range strings.Split(string(flags), "\x00") {
			if len(entry) >= 3 && (entry[0] == 'S' || entry[0] == 's') {
				skipWorktree[entry[2:]] = true
			}
		}
		omitted := absent[:0]
		for _, path := range absent {
			if !skipWorktree[path] {
				omitted = append(omitted, path)
			}
		}
		checker := git.NewIgnoreChecker(repo)
		defer checker.Close()
		ignored, err := checker.Check(ctx, omitted)
		if err != nil {
			return 0, err
		}
		for _, excluded := range ignored {
			if excluded {
				return 0, errFinishScopeUnknown
			}
			count++
		}
	}
	return count, nil
}

func finishStatusSummary(f *finishStatusReport) string {
	if f == nil {
		return "No finish requested."
	}
	switch f.State {
	case state.FinishCompleted:
		if f.DryRun {
			return "Preview checked; branch unchanged."
		}
		if f.NoChanges {
			return "No changes to commit."
		}
		if f.Published {
			return commitCountLabel(f.CommitsCreated)
		}
	case state.FinishFailed:
		if f.ReasonCode == "finish_preserved" {
			return "Interrupted finish preserved. Review current changes before a new finish."
		}
		return "Failed: " + consoleCause(f.Reason) + " The requested changes remain saved."
	case state.FinishNeedsAttention:
		return "Needs attention: " + consoleCause(f.Reason) + " The requested changes remain saved."
	}
	stage := f.Stage
	if stage == "" {
		stage = f.State
	}
	return finishStageLabel(stage)
}

func finishStatusTakesPriority(f *finishStatusReport) bool {
	return f != nil && (f.State == state.FinishRunning || f.State == state.FinishPrepared || f.State == state.FinishNeedsAttention)
}

func applyFinishControlStatus(res *controlResult, report statusReport, alive bool) {
	res.OK = true
	res.Health = controlHealthHealthy
	res.Daemon = report.Daemon
	res.DaemonPID = report.PID
	res.PublicationOutcome = report.PublicationOutcome
	res.Protected = report.Protected
	res.Published = checkpointPublishedByACD(report)
	res.CheckpointPublishedByACD = res.Published
	res.Busy = report.Busy
	res.OperationalState = report.OperationalState
	res.WorktreeClean = report.WorktreeClean
	res.AllChangesCommittedInGit = report.AllChangesCommittedInGit
	res.CheckpointID = report.LatestCheckpointID
	res.CaptureHealth = report.CaptureHealth
	res.PublicationProgress = report.PublicationProgress
	res.NextAction = "No action needed."
	res.Summary = "The changes ACD last checked are saved."
	switch {
	case report.Stale || report.Daemon != "running" || !alive:
		res.OK = false
		res.Health = controlHealthNeedsAttention
		res.Summary = "Background protection is not running."
		res.NextAction = "Run `acd on` to start protection."
	case report.CaptureHealth.Error != "":
		res.OK = false
		res.Health = controlHealthNeedsAttention
		res.Summary = "Some changes could not be saved: " + report.CaptureHealth.Error
		res.NextAction = "Run acd doctor to see which files could not be saved. Other readable changes remain saved."
		if report.CaptureHealth.State == "retrying" {
			res.OK = true
			res.Health = controlHealthWaiting
			res.Summary = "ACD is retrying automatically. Other readable changes remain saved."
			res.NextAction = "No action needed."
		}
	case !report.Protected:
		res.Health = controlHealthWaiting
		res.Summary = "Saving the latest observed work."
	case report.Busy:
		res.Health = controlHealthPublishing
		res.Summary = finishStatusSummary(report.PublicationOutcome.Finish)
	default:
		if f := report.PublicationOutcome.Finish; finishStatusTakesPriority(f) {
			res.Summary = finishStatusSummary(f)
		}
		if res.NextAction == "No action needed." && (report.PublicationOutcome.PendingChanges > 0 || report.PublicationOutcome.WorktreeChanges > 0 || !report.PublicationOutcome.CountsKnown) {
			res.NextAction = "Run `acd finish` when this work is complete."
		}
	}
	if legacy := report.PublicationOutcome.Legacy; legacy.State == "needs_attention" && res.OK {
		res.Summary += " Older saved work is retained separately."
	}
	if h := report.PublicationOutcome.History; h != nil && res.OK && !finishStatusTakesPriority(report.PublicationOutcome.Finish) && (h.State == "running" || h.State == "prepared" || h.State == "needs_attention") {
		res.Summary = "History plan " + h.PlanID + ": " + h.State + ". Checkpoint protection continues."
		if h.State == "needs_attention" {
			res.OK = false
			res.Health = controlHealthNeedsAttention
			res.NextAction = "Run acd doctor to review the interrupted history rewrite."
		}
	}
	applyMaintenanceStatus(res, report)
	// Saving new edits and a blocked finish are independent. Neither a pending
	// scan nor a maintenance warning may hide the request that blocks finish.
	if f := report.PublicationOutcome.Finish; f != nil {
		if f.State == state.FinishNeedsAttention {
			res.OK = false
			res.Health = controlHealthNeedsAttention
			res.NextAction = "Review the interrupted finish with `acd support recover --force --dry-run`. Recovery needs your approval."
		}
	}
}

func renderFinishStatusChecks(out io.Writer, f *finishStatusReport) {
	for _, check := range f.Verification {
		fmt.Fprintln(out, finishCheckLabel(check))
	}
}

func renderFinishUncommitted(out io.Writer, outcome publicationOutcome) {
	if outcome.CountsKnown {
		fmt.Fprintf(out, "Changes outside Git commits: %d saved, %d currently in your files\n", outcome.PendingChanges, outcome.WorktreeChanges)
		return
	}
	fmt.Fprintf(out, "Changes outside Git commits: saved count unavailable; %d currently in your files\n", outcome.WorktreeChanges)
}

func renderFinishOutcomeHuman(out io.Writer, outcome publicationOutcome) {
	fmt.Fprintf(out, "Last checked changes saved: %s\n", yesNo(outcome.Protected))
	renderFinishUncommitted(out, outcome)
	if f := outcome.Finish; f != nil {
		fmt.Fprintf(out, "Finish: %s\n", consoleProse(finishStatusSummary(f)))
		renderFinishStatusChecks(out, f)
	}
	if h := outcome.History; h != nil {
		fmt.Fprintf(out, "History rewrite: %s\nPlan: %s\n", h.PlanID, h.State)
	}
	if legacy := outcome.Legacy; legacy.State == "needs_attention" {
		renderLegacyAttention(out, legacy)
	}
	if outcome.Legacy.Unpublished > 0 {
		fmt.Fprintf(out, "Older saved versions: %d retained\n", outcome.Legacy.Unpublished)
	}
	if outcome.RecoveredChanges > 0 {
		fmt.Fprintf(out, "Recovery: %d older changes saved separately from the branch\n", outcome.RecoveredChanges)
	}
	if outcome.LaterChanges > 0 {
		fmt.Fprintf(out, "Later changes: %d changed items for the next finish\n", outcome.LaterChanges)
	}
}

func readFinishProtectionScope(body, id string) (daemon.ProtectionScope, bool) {
	var scope daemon.ProtectionScope
	if json.Unmarshal([]byte(body), &scope) != nil || scope.Version != 1 || scope.CheckpointID != id || id == "" {
		return daemon.ProtectionScope{}, false
	}
	return scope, true
}

func finishReadOnlyConfigValues(ctx context.Context, roots paths.Roots, repo string) (map[string]string, string, error) {
	lookup, err := loadControlRepo(ctx, repo)
	if err != nil {
		return nil, "", err
	}
	if lookup.Registered && fileExists(lookup.Record.StateDB) {
		db, err := state.OpenReadOnly(ctx, lookup.Record.StateDB)
		if err != nil {
			return nil, "", err
		}
		revision, found, err := finishApprovedRevision(ctx, db)
		_ = db.Close()
		if err != nil {
			return nil, "", err
		}
		if found {
			values, _, err := finishSnapshotValues(revision.SnapshotJSON)
			return values, fmt.Sprintf("approved revision %d", revision.ID), err
		}
	}
	values, _, err := finishSavedValues(roots, repo)
	return values, "saved settings", err
}
