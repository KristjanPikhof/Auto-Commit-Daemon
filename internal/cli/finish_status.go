package cli

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
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

type finishStatusReport struct {
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
	var exists int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='finish_requests'`).Scan(&exists); err != nil {
		return nil, err
	}
	if exists == 0 {
		return nil, nil
	}
	var r state.FinishRequest
	var target, plan, result string
	err := db.QueryRowContext(ctx, `SELECT request_id,attempt,target_json,state,stage,dry_run,plan_json,result_json,reason_code,reason,updated_ts FROM finish_requests WHERE worktree_id=? ORDER BY CASE WHEN state IN ('running','prepared','needs_attention') THEN 0 ELSE 1 END,created_ts DESC,attempt DESC LIMIT 1`, worktree).Scan(&r.ID, &r.Attempt, &target, &r.State, &r.Stage, &r.DryRun, &plan, &result, &r.ReasonCode, &r.Reason, &r.UpdatedTS)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal([]byte(target), &r.Target); err != nil {
		return nil, fmt.Errorf("finish target: %w", err)
	}
	report := &finishStatusReport{RequestID: r.ID, Attempt: r.Attempt, State: r.State, Stage: r.Stage, Mode: r.Target.Mode, Style: r.Target.Style, CheckpointID: r.Target.CheckpointID, TargetTree: r.Target.TargetTree, BranchRef: r.Target.BranchRef, DryRun: r.DryRun, ReasonCode: r.ReasonCode, Reason: r.Reason, UpdatedTS: r.UpdatedTS, Verification: []daemon.FinishCheck{}, sourceTree: r.Target.CheckpointTree}
	if result != "" {
		var saved daemon.FinishResult
		if err := json.Unmarshal([]byte(result), &saved); err != nil {
			return nil, fmt.Errorf("finish result: %w", err)
		}
		report.Published = saved.Published
		report.NoChanges = saved.NoChanges
		report.CommitsCreated = saved.CommitsCreated
		report.NewHead = saved.NewHead
	}
	if plan != "" {
		var saved daemon.FinishSavedPlan
		if err := json.Unmarshal([]byte(plan), &saved); err != nil {
			return nil, fmt.Errorf("finish checks: %w", err)
		}
		for _, step := range saved.Steps {
			if step.Check.TreeOID != "" {
				report.Verification = append(report.Verification, step.Check)
			}
		}
	}
	return report, nil
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
	report.Protected = get(daemon.MetaKeyProtectionComplete) == "true" && report.LatestCheckpointID != "" && report.ObservationEpoch == report.CoveredEpoch
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
	if err := rows.Close(); err != nil {
		return report, err
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
	outcome.WorktreeChanges = liveCount
	outcome.Protected = report.Protected
	outcome.Finish, err = readFinishStatus(ctx, db, checkpoint.WorktreeID(rec.Path))
	if err != nil {
		return report, err
	}
	head, headErr := git.RevParse(ctx, rec.Path, "HEAD")
	if headErr != nil && !errors.Is(headErr, git.ErrRefNotFound) {
		return report, headErr
	}
	branch, err := git.Run(ctx, git.RunOpts{Dir: rec.Path}, "symbolic-ref", "--quiet", "--no-recurse", "HEAD")
	if err == nil {
		report.BranchRef = strings.TrimSpace(string(branch))
	}
	if report.LatestCheckpointID != "" {
		cp, err := state.ResolveCheckpoint(ctx, rec.StateDB, report.LatestCheckpointID)
		if err != nil {
			return report, err
		}
		if cp.Partial || cp.Phase != state.CheckpointCompleted {
			report.Protected = false
			outcome.Protected = false
		}
		count, err := finishSavedChangeCount(ctx, rec.Path, head, cp.TreeOID)
		if err != nil {
			return report, err
		}
		outcome.PendingChanges = count
		outcome.WaitingChanges = count
		outcome.CountsKnown = true
		committed := count == 0
		outcome.BranchCommitted = &committed
		if outcome.Finish != nil && outcome.Finish.sourceTree != "" {
			outcome.LaterChanges, err = finishSavedChangeCount(ctx, rec.Path, outcome.Finish.sourceTree, cp.TreeOID)
			if err != nil {
				return report, err
			}
		}
		var events, published, recovered int
		err = db.QueryRowContext(ctx, `SELECT COUNT(*),COALESCE(SUM(e.state='published'),0),COALESCE(SUM(e.state='recovered'),0) FROM checkpoint_events ce JOIN capture_events e ON e.seq=ce.event_seq WHERE ce.checkpoint_id=?`, cp.ID).Scan(&events, &published, &recovered)
		if err != nil {
			return report, err
		}
		outcome.RecoveredChanges = recovered
		// Deprecated booleans retain resolved-work semantics for legacy membership.
		outcome.Resolved = (events > 0 && events == published+recovered) || (events == 0 && committed)
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
	}
	report.CheckpointPublishedByACD = report.Protected && outcome.Resolved
	report.OperationalState = "saved"
	if !responsive {
		report.OperationalState = "stopped"
	} else if report.CaptureHealth.Error != "" {
		report.OperationalState = "protection_incomplete"
	} else if report.Busy {
		report.OperationalState = "finishing"
	} else if outcome.PendingChanges == 0 {
		report.OperationalState = "up_to_date"
	}
	return report, nil
}

// Count only eligible saved paths. Missing excluded entries in a checkpoint
// never imply that sensitive files or Gitlinks should be deleted from a branch.
func finishSavedChangeCount(ctx context.Context, repo, base, target string) (int, error) {
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
	sensitive, safe := state.NewSensitiveMatcher(), state.NewSafeIgnoreMatcher()
	if roots, err := paths.Resolve(); err == nil {
		if values, _, err := finishSavedValues(roots, repo); err == nil {
			sensitive = state.NewSensitiveMatcherFromValue(values["capture.sensitive_globs"])
		}
	}
	excluded := map[string]bool{}
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
	paths := make([]string, 0)
	for path, entry := range old {
		next, exists := newEntries[path]
		if !exists || next.Mode != entry.Mode || next.OID != entry.OID {
			paths = append(paths, path)
		}
	}
	for path := range newEntries {
		if _, exists := old[path]; !exists {
			paths = append(paths, path)
		}
	}
	if len(paths) == 0 {
		return 0, nil
	}
	checker := git.NewIgnoreChecker(repo)
	defer checker.Close()
	ignored, err := checker.Check(ctx, paths)
	if err != nil {
		return 0, err
	}
	count := 0
	for index := range paths {
		if !ignored[index] {
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
			return fmt.Sprintf("Completed: %d commit(s) on the branch.", f.CommitsCreated)
		}
	case state.FinishFailed:
		return "Failed: " + f.Reason + " Work remains saved."
	case state.FinishNeedsAttention:
		return "Publication needs attention: " + f.Reason + " Work remains saved."
	}
	stage := f.Stage
	if stage == "" {
		stage = f.State
	}
	return "Running: " + stage + "; work remains saved."
}
