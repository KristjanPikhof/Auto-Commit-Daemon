package cli

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/daemon"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	pausepkg "github.com/KristjanPikhof/Auto-Commit-Daemon/internal/pause"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

func TestFix_DefaultsToDryRunWhenNoFlagsPassed(t *testing.T) {
	// Without --yes (and without --force) `acd fix` must be a pure dry-run.
	// The old "refuse without --yes" guard has been replaced by silent
	// dry-run default per SPEC LOCK so casual operators see the plan first.
	repo, stateDB, _ := makeRegisteredGitRepoStateDB(t)
	before, err := fileSHA256(stateDB)
	if err != nil {
		t.Fatalf("checksum before: %v", err)
	}
	var out bytes.Buffer
	if err := runFix(context.Background(), &out, repo, false /*dryRun*/, false /*yes*/, false /*force*/, false /*clearPause*/, true /*jsonOut*/); err != nil {
		t.Fatalf("runFix without --yes must dry-run silently, got err=%v\n%s", err, out.String())
	}
	var plan fixPlan
	if err := json.Unmarshal(out.Bytes(), &plan); err != nil {
		t.Fatalf("unmarshal: %v\n%s", err, out.String())
	}
	if !plan.DryRun {
		t.Fatalf("plan.DryRun=false in no-flag default: %+v", plan)
	}
	after, err := fileSHA256(stateDB)
	if err != nil {
		t.Fatalf("checksum after: %v", err)
	}
	if before != after {
		t.Fatalf("no-flag default mutated state.db: before=%s after=%s", before, after)
	}
}

func TestFix_DryRunPlansSafeActionsWithoutMutation(t *testing.T) {
	repo, stateDB, db := makeRegisteredGitRepoStateDB(t)
	seedPurgeFixtureRows(t, db)
	before, err := fileSHA256(stateDB)
	if err != nil {
		t.Fatalf("checksum before: %v", err)
	}

	var out bytes.Buffer
	if err := runFix(context.Background(), &out, repo, true, false, false, false, true); err != nil {
		t.Fatalf("runFix dry-run: %v\n%s", err, out.String())
	}
	var plan fixPlan
	if err := json.Unmarshal(out.Bytes(), &plan); err != nil {
		t.Fatalf("unmarshal fix plan: %v\n%s", err, out.String())
	}
	if !plan.DryRun {
		t.Fatalf("plan.DryRun=false: %+v", plan)
	}
	if plan.BackupPath != "" {
		t.Fatalf("dry-run wrote backup %q", plan.BackupPath)
	}
	if !hasFixAction(plan, fixActionDeleteObsoleteBarrier) {
		t.Fatalf("fix plan lacks obsolete barrier cleanup: %+v", plan.Actions)
	}
	after, err := fileSHA256(stateDB)
	if err != nil {
		t.Fatalf("checksum after: %v", err)
	}
	if before != after {
		t.Fatalf("dry-run mutated state.db: before=%s after=%s", before, after)
	}
}

func TestFix_DryRunToleratesPreV5DB(t *testing.T) {
	repo, _, db := makeRegisteredGitRepoStateDB(t)
	seedPurgeFixtureRows(t, db)
	if _, err := db.SQL().ExecContext(context.Background(), `DROP TABLE decision_records`); err != nil {
		t.Fatalf("drop decision_records: %v", err)
	}

	var out bytes.Buffer
	if err := runFix(context.Background(), &out, repo, true, false, false, false, true); err != nil {
		t.Fatalf("runFix dry-run should tolerate missing decision_records: %v\n%s", err, out.String())
	}
	var plan fixPlan
	if err := json.Unmarshal(out.Bytes(), &plan); err != nil {
		t.Fatalf("unmarshal fix plan: %v\n%s", err, out.String())
	}
	if !hasFixAction(plan, fixActionDeleteObsoleteBarrier) {
		t.Fatalf("pre-v5 fix plan lacks obsolete barrier cleanup: %+v", plan.Actions)
	}
}

func TestFix_ApplyClearsExpiredPauseAndDrainedBackpressure(t *testing.T) {
	repo, _, db := makeRegisteredGitRepoStateDB(t)
	ctx := context.Background()
	if err := state.MetaSet(ctx, db, daemon.MetaKeyCaptureBackpressurePausedAt, time.Now().UTC().Add(-time.Hour).Format(time.RFC3339)); err != nil {
		t.Fatalf("seed backpressure: %v", err)
	}
	markerPath := filepath.Join(repo, ".git", "acd", "paused")
	expiredAt := time.Now().UTC().Add(-time.Minute).Format(time.RFC3339)
	if _, err := pausepkg.Write(markerPath, pausepkg.Marker{
		Reason:    "old maintenance",
		SetAt:     time.Now().UTC().Add(-time.Hour).Format(time.RFC3339),
		SetBy:     "test",
		ExpiresAt: &expiredAt,
	}, true); err != nil {
		t.Fatalf("write marker: %v", err)
	}

	var out bytes.Buffer
	if err := runFix(ctx, &out, repo, false, true, false, false, true); err != nil {
		t.Fatalf("runFix apply: %v\n%s", err, out.String())
	}
	var plan fixPlan
	if err := json.Unmarshal(out.Bytes(), &plan); err != nil {
		t.Fatalf("unmarshal fix result: %v\n%s", err, out.String())
	}
	if plan.BackupPath == "" {
		t.Fatalf("apply did not create backup: %+v", plan)
	}
	if !plan.ManualPauseRemoved {
		t.Fatalf("manual pause not marked removed: %+v", plan)
	}
	if _, err := os.Stat(markerPath); !os.IsNotExist(err) {
		t.Fatalf("manual pause marker still exists: %v", err)
	}
	if _, ok, err := state.MetaGet(ctx, db, daemon.MetaKeyCaptureBackpressurePausedAt); err != nil {
		t.Fatalf("MetaGet backpressure: %v", err)
	} else if ok {
		t.Fatalf("backpressure meta still present")
	}
	if _, ok, err := state.MetaGet(ctx, db, "capture.backpressure_overridden_at"); err != nil {
		t.Fatalf("MetaGet override: %v", err)
	} else if !ok {
		t.Fatalf("backpressure override stamp missing")
	}
	if _, ok, err := state.MetaGet(ctx, db, daemon.MetaKeyManualPauseResumedAt); err != nil {
		t.Fatalf("MetaGet manual resume: %v", err)
	} else if !ok {
		t.Fatalf("manual pause resume stamp missing")
	}
}

func TestFix_ApplyDeletesObsoleteBarrierAndLeavesPending(t *testing.T) {
	repo, _, db := makeRegisteredGitRepoStateDB(t)
	seedPurgeFixtureRows(t, db)

	var out bytes.Buffer
	if err := runFix(context.Background(), &out, repo, false, true, false, false, true); err != nil {
		t.Fatalf("runFix apply: %v\n%s", err, out.String())
	}
	got := countCaptureRowsByState(t, db)
	if got[state.EventStateBlockedConflict] != 0 {
		t.Fatalf("blocked rows remain: %v", got)
	}
	if got[state.EventStateFailed] != 0 {
		t.Fatalf("failed rows remain: %v", got)
	}
	if got[state.EventStatePending] != 2 {
		t.Fatalf("pending rows changed unexpectedly: %v", got)
	}
	var status string
	if err := db.SQL().QueryRowContext(context.Background(),
		`SELECT status FROM publish_state WHERE id = 1`).Scan(&status); err != nil {
		t.Fatalf("read publish_state: %v", err)
	}
	if status == state.EventStateBlockedConflict {
		t.Fatalf("publish_state.status still blocked_conflict")
	}
}

func TestFix_ApplyRefusesWhenPlanHasUnsafeReasons(t *testing.T) {
	repo, _, db := makeRegisteredGitRepoStateDB(t)
	ctx := context.Background()
	head, err := git.RevParse(ctx, repo, "HEAD")
	if err != nil {
		t.Fatalf("rev-parse: %v", err)
	}
	seq, err := state.AppendCaptureEvent(ctx, db, state.CaptureEvent{
		BranchRef:        "refs/heads/main",
		BranchGeneration: 1,
		BaseHead:         head,
		Operation:        "modify",
		Path:             "unsafe.txt",
		Fidelity:         "exact",
		State:            state.EventStateBlockedConflict,
		Error:            sql.NullString{String: "before-state mismatch", Valid: true},
	}, nil)
	if err != nil {
		t.Fatalf("AppendCaptureEvent: %v", err)
	}
	if _, err := state.AppendDecision(ctx, db, state.DecisionRecord{
		Kind:             state.DecisionKindHandledExternal,
		Path:             sql.NullString{String: "unsafe.txt", Valid: true},
		Reason:           sql.NullString{String: "already_published_by_external_committer", Valid: true},
		EventSeq:         sql.NullInt64{Int64: seq, Valid: true},
		CommitOID:        sql.NullString{String: "1111111111111111111111111111111111111111", Valid: true},
		BranchRef:        sql.NullString{String: "refs/heads/main", Valid: true},
		BranchGeneration: sql.NullInt64{Int64: 1, Valid: true},
	}); err != nil {
		t.Fatalf("AppendDecision: %v", err)
	}

	var out bytes.Buffer
	err = runFix(ctx, &out, repo, false, true, false, false, true)
	if err == nil {
		t.Fatalf("runFix apply succeeded despite unsafe plan:\n%s", out.String())
	}
	if !strings.Contains(err.Error(), "unsafe conditions") {
		t.Fatalf("error=%v want unsafe refusal", err)
	}
	var stateName string
	if err := db.SQL().QueryRowContext(ctx, `SELECT state FROM capture_events WHERE seq = ?`, seq).Scan(&stateName); err != nil {
		t.Fatalf("query event: %v", err)
	}
	if stateName != state.EventStateBlockedConflict {
		t.Fatalf("event state=%q want blocked_conflict", stateName)
	}
}

func TestFix_ApplyMarksDecisionLedExternalRowPublished(t *testing.T) {
	repo, _, db := makeRegisteredGitRepoStateDB(t)
	ctx := context.Background()
	if err := os.WriteFile(filepath.Join(repo, "external.txt"), []byte("landed outside acd\n"), 0o644); err != nil {
		t.Fatalf("write external: %v", err)
	}
	if _, err := git.Run(ctx, git.RunOpts{Dir: repo}, "add", "external.txt"); err != nil {
		t.Fatalf("git add external: %v", err)
	}
	if _, err := git.Run(ctx, git.RunOpts{Dir: repo}, "-c", "user.name=ACD Test", "-c", "user.email=acd@example.invalid", "commit", "-m", "external"); err != nil {
		t.Fatalf("git commit external: %v", err)
	}
	head, err := git.RevParse(ctx, repo, "HEAD")
	if err != nil {
		t.Fatalf("rev-parse: %v", err)
	}
	afterOID, err := git.LsTreeBlobOID(ctx, repo, head, "external.txt")
	if err != nil {
		t.Fatalf("ls-tree external: %v", err)
	}
	seq, err := state.AppendCaptureEvent(ctx, db, state.CaptureEvent{
		BranchRef:        "refs/heads/main",
		BranchGeneration: 1,
		BaseHead:         head,
		Operation:        "create",
		Path:             "external.txt",
		Fidelity:         "exact",
		State:            state.EventStateBlockedConflict,
		Error:            sql.NullString{String: "before-state mismatch", Valid: true},
	}, []state.CaptureOp{{
		Op:        "create",
		Path:      "external.txt",
		AfterOID:  sql.NullString{String: afterOID, Valid: true},
		AfterMode: sql.NullString{String: "100644", Valid: true},
		Fidelity:  "exact",
	}})
	if err != nil {
		t.Fatalf("AppendCaptureEvent: %v", err)
	}
	if _, err := state.AppendDecision(ctx, db, state.DecisionRecord{
		Kind:             state.DecisionKindHandledExternal,
		Path:             sql.NullString{String: "external.txt", Valid: true},
		Reason:           sql.NullString{String: "already_published_by_external_committer", Valid: true},
		EventSeq:         sql.NullInt64{Int64: seq, Valid: true},
		CommitOID:        sql.NullString{String: head, Valid: true},
		BranchRef:        sql.NullString{String: "refs/heads/main", Valid: true},
		BranchGeneration: sql.NullInt64{Int64: 1, Valid: true},
	}); err != nil {
		t.Fatalf("AppendDecision: %v", err)
	}

	var out bytes.Buffer
	if err := runFix(ctx, &out, repo, false, true, false, false, true); err != nil {
		t.Fatalf("runFix apply: %v\n%s", err, out.String())
	}
	var stateName, commitOID string
	var errMsg sql.NullString
	if err := db.SQL().QueryRowContext(ctx,
		`SELECT state, commit_oid, error FROM capture_events WHERE seq = ?`, seq,
	).Scan(&stateName, &commitOID, &errMsg); err != nil {
		t.Fatalf("query event: %v", err)
	}
	if stateName != state.EventStatePublished || commitOID != head || errMsg.Valid {
		t.Fatalf("event state=%q commit=%q err=%v, want published %s nil", stateName, commitOID, errMsg, head)
	}
}

func hasFixAction(plan fixPlan, kind string) bool {
	for _, action := range plan.Actions {
		if action.Kind == kind {
			return true
		}
	}
	return false
}
