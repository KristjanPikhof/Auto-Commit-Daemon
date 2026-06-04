package cli

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
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

func TestFix_DryRunPlansGeneratedPendingCleanup(t *testing.T) {
	repo, stateDB, db := makeRegisteredGitRepoStateDB(t)
	ctx := context.Background()
	generatedSeqs := seedGeneratedPendingFixFixture(t, ctx, repo, db)
	before, err := fileSHA256(stateDB)
	if err != nil {
		t.Fatalf("checksum before: %v", err)
	}

	var out bytes.Buffer
	if err := runFix(ctx, &out, repo, true, false, false, false, true); err != nil {
		t.Fatalf("runFix generated dry-run: %v\n%s", err, out.String())
	}
	var plan fixPlan
	if err := json.Unmarshal(out.Bytes(), &plan); err != nil {
		t.Fatalf("unmarshal: %v\n%s", err, out.String())
	}
	var generated *fixAction
	for i := range plan.Actions {
		if plan.Actions[i].Kind == fixActionDropGeneratedPending {
			generated = &plan.Actions[i]
			break
		}
	}
	if generated == nil {
		t.Fatalf("plan missing generated cleanup action: %+v", plan.Actions)
	}
	if generated.GeneratedRoot != ".derivedData-provider-core" ||
		generated.SafeIgnorePattern != ".derivedData*/" ||
		generated.PendingCount != 2 ||
		generated.TrackedCount != 2 ||
		!reflect.DeepEqual(generated.EventSeqs, generatedSeqs) {
		t.Fatalf("generated action=%+v, seqs=%v", *generated, generatedSeqs)
	}
	after, err := fileSHA256(stateDB)
	if err != nil {
		t.Fatalf("checksum after: %v", err)
	}
	if before != after {
		t.Fatalf("dry-run mutated state.db: before=%s after=%s", before, after)
	}

	out.Reset()
	if err := runFix(ctx, &out, repo, true, false, false, false, false); err != nil {
		t.Fatalf("runFix generated human dry-run: %v\n%s", err, out.String())
	}
	human := out.String()
	for _, want := range []string{
		"drop protected generated pending deletes",
		"root=.derivedData-provider-core pending=2 tracked=2",
		"git add -u -- .derivedData-provider-core",
	} {
		if !strings.Contains(human, want) {
			t.Fatalf("human output missing %q:\n%s", want, human)
		}
	}
}

func TestFix_ApplyDropsGeneratedPendingOnly(t *testing.T) {
	repo, _, db := makeRegisteredGitRepoStateDB(t)
	ctx := context.Background()
	generatedSeqs := seedGeneratedPendingFixFixture(t, ctx, repo, db)
	beforeIndex := gitCachedNameStatus(t, ctx, repo)

	var out bytes.Buffer
	if err := runFix(ctx, &out, repo, false, true, false, false, true); err != nil {
		t.Fatalf("runFix generated apply: %v\n%s", err, out.String())
	}
	var plan fixPlan
	if err := json.Unmarshal(out.Bytes(), &plan); err != nil {
		t.Fatalf("unmarshal: %v\n%s", err, out.String())
	}
	var generated *fixAction
	for i := range plan.Actions {
		if plan.Actions[i].Kind == fixActionDropGeneratedPending {
			generated = &plan.Actions[i]
			break
		}
	}
	if generated == nil || !generated.Applied || generated.RowsChanged != 2 {
		t.Fatalf("generated action not applied as expected: %+v actions=%+v", generated, plan.Actions)
	}
	if plan.BackupPath == "" {
		t.Fatalf("apply did not create backup: %+v", plan)
	}
	for _, seq := range generatedSeqs {
		if got := countRowsWhere(t, db, "capture_events", "seq = ?", seq); got != 0 {
			t.Fatalf("generated capture_event seq=%d remains: %d", seq, got)
		}
		if got := countRowsWhere(t, db, "capture_ops", "event_seq = ?", seq); got != 0 {
			t.Fatalf("generated capture_ops seq=%d remains: %d", seq, got)
		}
		if got := countRowsWhere(t, db, "planner_state", "event_seq = ?", seq); got != 0 {
			t.Fatalf("generated planner_state seq=%d remains: %d", seq, got)
		}
	}
	for _, path := range []string{"build/output.js", "src/ordinary.txt"} {
		if got := countRowsWhere(t, db, "capture_events", "path = ? AND state = ?", path, state.EventStatePending); got != 1 {
			t.Fatalf("unrelated pending path %s count=%d want 1", path, got)
		}
	}
	if afterIndex := gitCachedNameStatus(t, ctx, repo); afterIndex != beforeIndex {
		t.Fatalf("fix mutated git index:\nbefore:\n%s\nafter:\n%s", beforeIndex, afterIndex)
	}
}

// TestFix_DryRunListsResolveAlreadyLandedBarrier exercises the Wave 3a
// resolve_already_landed_barrier action: when an external committer already
// landed the captured modify at HEAD and the corresponding capture_events
// row is blocked_conflict with a before-state-mismatch error, the planner
// must list it for auto-resolution under --yes alone.
func TestFix_DryRunListsResolveAlreadyLandedBarrier(t *testing.T) {
	repo, _, db := makeRegisteredGitRepoStateDB(t)
	ctx := context.Background()

	// Land the "external" commit modifying seed.txt so HEAD blob matches what
	// the captured op claims as after_oid.
	seedPath := filepath.Join(repo, "seed.txt")
	if err := os.WriteFile(seedPath, []byte("seed\nlanded externally\n"), 0o644); err != nil {
		t.Fatalf("write seed.txt: %v", err)
	}
	if _, err := git.Run(ctx, git.RunOpts{Dir: repo}, "add", "seed.txt"); err != nil {
		t.Fatalf("git add: %v", err)
	}
	if _, err := git.Run(ctx, git.RunOpts{Dir: repo}, "-c", "user.name=ACD Test", "-c", "user.email=acd@example.invalid", "commit", "-m", "external modify"); err != nil {
		t.Fatalf("git commit external: %v", err)
	}
	head, err := git.RevParse(ctx, repo, "HEAD")
	if err != nil {
		t.Fatalf("rev-parse HEAD: %v", err)
	}
	afterOID, err := git.LsTreeBlobOID(ctx, repo, head, "seed.txt")
	if err != nil {
		t.Fatalf("ls-tree HEAD seed.txt: %v", err)
	}
	parent, err := git.RevParse(ctx, repo, "HEAD~1")
	if err != nil {
		t.Fatalf("rev-parse HEAD~1: %v", err)
	}
	beforeOID, err := git.LsTreeBlobOID(ctx, repo, parent, "seed.txt")
	if err != nil {
		t.Fatalf("ls-tree HEAD~1 seed.txt: %v", err)
	}

	seq, err := state.AppendCaptureEvent(ctx, db, state.CaptureEvent{
		BranchRef:        "refs/heads/main",
		BranchGeneration: 1,
		BaseHead:         parent,
		Operation:        "modify",
		Path:             "seed.txt",
		Fidelity:         "exact",
		State:            state.EventStateBlockedConflict,
		Error:            sql.NullString{String: "modify before-state mismatch", Valid: true},
	}, []state.CaptureOp{{
		Op:         "modify",
		Path:       "seed.txt",
		BeforeOID:  sql.NullString{String: beforeOID, Valid: true},
		BeforeMode: sql.NullString{String: "100644", Valid: true},
		AfterOID:   sql.NullString{String: afterOID, Valid: true},
		AfterMode:  sql.NullString{String: "100644", Valid: true},
		Fidelity:   "exact",
	}})
	if err != nil {
		t.Fatalf("AppendCaptureEvent: %v", err)
	}

	var out bytes.Buffer
	if err := runFix(ctx, &out, repo, true, false, false, false, true); err != nil {
		t.Fatalf("runFix dry-run: %v\n%s", err, out.String())
	}
	var plan fixPlan
	if err := json.Unmarshal(out.Bytes(), &plan); err != nil {
		t.Fatalf("unmarshal: %v\n%s", err, out.String())
	}
	if !hasFixAction(plan, fixActionResolveAlreadyLandedBarrier) {
		t.Fatalf("plan lacks resolve_already_landed_barrier: %+v", plan.Actions)
	}
	// Find the action and confirm it carries the expected commit OID + seq.
	var found *fixAction
	for i := range plan.Actions {
		if plan.Actions[i].Kind == fixActionResolveAlreadyLandedBarrier && plan.Actions[i].Seq == seq {
			found = &plan.Actions[i]
			break
		}
	}
	if found == nil {
		t.Fatalf("no resolve action for seq=%d: %+v", seq, plan.Actions)
	}
	if found.CommitOID != head {
		t.Fatalf("resolve action commit_oid=%q want %q", found.CommitOID, head)
	}
}

// TestFix_ApplyYesPromotesAlreadyLandedBarrier confirms --yes alone (no
// --force) is sufficient to promote the blocked_conflict row to published
// and append a handled_external_after_block decision row.
func TestFix_ApplyYesPromotesAlreadyLandedBarrier(t *testing.T) {
	repo, _, db := makeRegisteredGitRepoStateDB(t)
	ctx := context.Background()
	if err := os.WriteFile(filepath.Join(repo, "seed.txt"), []byte("seed\nlanded externally\n"), 0o644); err != nil {
		t.Fatalf("write seed.txt: %v", err)
	}
	if _, err := git.Run(ctx, git.RunOpts{Dir: repo}, "add", "seed.txt"); err != nil {
		t.Fatalf("git add: %v", err)
	}
	if _, err := git.Run(ctx, git.RunOpts{Dir: repo}, "-c", "user.name=ACD Test", "-c", "user.email=acd@example.invalid", "commit", "-m", "external modify"); err != nil {
		t.Fatalf("git commit external: %v", err)
	}
	head, err := git.RevParse(ctx, repo, "HEAD")
	if err != nil {
		t.Fatalf("rev-parse: %v", err)
	}
	afterOID, err := git.LsTreeBlobOID(ctx, repo, head, "seed.txt")
	if err != nil {
		t.Fatalf("ls-tree: %v", err)
	}
	parent, err := git.RevParse(ctx, repo, "HEAD~1")
	if err != nil {
		t.Fatalf("rev-parse HEAD~1: %v", err)
	}
	beforeOID, err := git.LsTreeBlobOID(ctx, repo, parent, "seed.txt")
	if err != nil {
		t.Fatalf("ls-tree HEAD~1: %v", err)
	}
	seq, err := state.AppendCaptureEvent(ctx, db, state.CaptureEvent{
		BranchRef:        "refs/heads/main",
		BranchGeneration: 1,
		BaseHead:         parent,
		Operation:        "modify",
		Path:             "seed.txt",
		Fidelity:         "exact",
		State:            state.EventStateBlockedConflict,
		Error:            sql.NullString{String: "modify before-state mismatch", Valid: true},
	}, []state.CaptureOp{{
		Op:         "modify",
		Path:       "seed.txt",
		BeforeOID:  sql.NullString{String: beforeOID, Valid: true},
		BeforeMode: sql.NullString{String: "100644", Valid: true},
		AfterOID:   sql.NullString{String: afterOID, Valid: true},
		AfterMode:  sql.NullString{String: "100644", Valid: true},
		Fidelity:   "exact",
	}})
	if err != nil {
		t.Fatalf("AppendCaptureEvent: %v", err)
	}

	var out bytes.Buffer
	if err := runFix(ctx, &out, repo, false, true, false, false, true); err != nil {
		t.Fatalf("runFix apply: %v\n%s", err, out.String())
	}
	var stateName, commitOID string
	if err := db.SQL().QueryRowContext(ctx,
		`SELECT state, commit_oid FROM capture_events WHERE seq = ?`, seq,
	).Scan(&stateName, &commitOID); err != nil {
		t.Fatalf("query (post-apply event):\n%s\n%v", out.String(), err)
	}
	if stateName != state.EventStatePublished || commitOID != head {
		t.Fatalf("event after fix state=%q commit=%q want published+%s\n%s", stateName, commitOID, head, out.String())
	}
	var kind string
	if err := db.SQL().QueryRowContext(ctx,
		`SELECT kind FROM decision_records WHERE event_seq = ? AND kind = ?`,
		seq, state.DecisionKindHandledExternalAfterBlock,
	).Scan(&kind); err != nil {
		t.Fatalf("query decision (out=%s): %v", out.String(), err)
	}
}

// TestFix_YesAloneRefusesToIncludePurge pins the SPEC LOCK constraint that
// --yes WITHOUT --force never plans purge_barrier_with_successors, even when
// a barrier-with-successors row exists.
func TestFix_YesAloneRefusesToIncludePurge(t *testing.T) {
	repo, _, db := makeRegisteredGitRepoStateDB(t)
	ctx := context.Background()
	// Stage a blocked row at seq=N and a pending row at seq=N+1 on the
	// current branch+head so the barrier-with-successors predicate matches
	// without also tripping retarget_stale_anchor.
	head, err := git.RevParse(ctx, repo, "HEAD")
	if err != nil {
		t.Fatalf("rev-parse: %v", err)
	}
	if _, err := state.AppendCaptureEvent(ctx, db, state.CaptureEvent{
		BranchRef:        "refs/heads/main",
		BranchGeneration: 1,
		BaseHead:         head,
		Operation:        "modify",
		Path:             "barrier.txt",
		Fidelity:         "exact",
		State:            state.EventStateBlockedConflict,
		Error:            sql.NullString{String: "old conflict", Valid: true},
	}, nil); err != nil {
		t.Fatalf("seed blocked: %v", err)
	}
	if _, err := state.AppendCaptureEvent(ctx, db, state.CaptureEvent{
		BranchRef:        "refs/heads/main",
		BranchGeneration: 1,
		BaseHead:         head,
		Operation:        "modify",
		Path:             "successor.txt",
		Fidelity:         "exact",
		State:            state.EventStatePending,
	}, nil); err != nil {
		t.Fatalf("seed pending successor: %v", err)
	}

	// Dry-run plan only — apply-mode here would invoke clearPublishBarrierIfSafe
	// and other unrelated mutations. The contract we care about is the
	// presence/absence of purge in the planned action set.
	var out bytes.Buffer
	if err := runFix(ctx, &out, repo, true /*dryRun*/, false /*yes*/, false /*force*/, false /*clearPause*/, true); err != nil {
		t.Fatalf("runFix dry-run (no force): %v\n%s", err, out.String())
	}
	var plan fixPlan
	if err := json.Unmarshal(out.Bytes(), &plan); err != nil {
		t.Fatalf("unmarshal: %v\n%s", err, out.String())
	}
	if hasFixAction(plan, fixActionPurgeBarrierWithSuccessors) {
		t.Fatalf("--force not set: plan must NOT include purge: %+v", plan.Actions)
	}
	// Operator nudge: when a barrier-with-successors exists we still surface
	// the --force suggestion so the path is discoverable.
	foundNudge := false
	for _, s := range plan.Suggestions {
		if strings.Contains(s, "--force") {
			foundNudge = true
			break
		}
	}
	if !foundNudge {
		t.Fatalf("plan must surface --force nudge in Suggestions: %+v", plan.Suggestions)
	}
	if !plan.ForceRequired {
		t.Fatalf("plan.force_required=false with active force-only barrier: %+v", plan)
	}
	got := countCaptureRowsByState(t, db)
	if got[state.EventStateBlockedConflict] != 1 || got[state.EventStatePending] != 1 {
		t.Fatalf("normal fix must not destructively purge barrier rows: %v", got)
	}
}

func TestFix_SafePlanPrintsExactForceCommandWithRepoPath(t *testing.T) {
	repo, _, db := makeRegisteredGitRepoStateDB(t)
	ctx := context.Background()
	stageBarrierWithSuccessors(t, ctx, repo, db)

	var out bytes.Buffer
	if err := runFix(ctx, &out, repo, true, false, false, false, false); err != nil {
		t.Fatalf("runFix dry-run human: %v\n%s", err, out.String())
	}
	want := "acd fix --repo " + repo + " --force --yes"
	if !strings.Contains(out.String(), want) {
		t.Fatalf("human output missing exact force command %q:\n%s", want, out.String())
	}
}

// TestFix_ForceDryRunListsPurge confirms --force without --yes still plans
// purge_barrier_with_successors (dry-run) so operators can preview the
// destructive action before opting in.
func TestFix_ForceDryRunListsPurge(t *testing.T) {
	repo, stateDB, db := makeRegisteredGitRepoStateDB(t)
	ctx := context.Background()
	stageBarrierWithSuccessors(t, ctx, repo, db)
	before, err := fileSHA256(stateDB)
	if err != nil {
		t.Fatalf("checksum before: %v", err)
	}

	var out bytes.Buffer
	if err := runFix(ctx, &out, repo, false, false /*yes*/, true /*force*/, false, true); err != nil {
		t.Fatalf("runFix --force --dry-run: %v\n%s", err, out.String())
	}
	var plan fixPlan
	if err := json.Unmarshal(out.Bytes(), &plan); err != nil {
		t.Fatalf("unmarshal: %v\n%s", err, out.String())
	}
	if !plan.DryRun {
		t.Fatalf("--force without --yes must dry-run: %+v", plan)
	}
	if !hasFixAction(plan, fixActionPurgeBarrierWithSuccessors) {
		t.Fatalf("--force dry-run did not plan purge: %+v", plan.Actions)
	}
	after, err := fileSHA256(stateDB)
	if err != nil {
		t.Fatalf("checksum after: %v", err)
	}
	if before != after {
		t.Fatalf("--force --dry-run mutated state.db: before=%s after=%s", before, after)
	}
}

// TestFix_ForceYesAppliesPurge exercises the full destructive path:
// --force --yes deletes the blocked barrier row and clears the matching
// publish_state breadcrumb. Pending successors are left untouched.
func TestFix_ForceYesAppliesPurge(t *testing.T) {
	repo, _, db := makeRegisteredGitRepoStateDB(t)
	ctx := context.Background()
	stageBarrierWithSuccessors(t, ctx, repo, db)

	var out bytes.Buffer
	if err := runFix(ctx, &out, repo, false, true /*yes*/, true /*force*/, false, true); err != nil {
		t.Fatalf("runFix --force --yes: %v\n%s", err, out.String())
	}
	got := countCaptureRowsByState(t, db)
	if got[state.EventStateBlockedConflict] != 0 {
		t.Fatalf("blocked rows remain after --force --yes: %v", got)
	}
	if got[state.EventStatePending] == 0 {
		t.Fatalf("pending successors should remain: %v", got)
	}
}

func TestFix_ForceYesPurgesBeforeRetargetResetsBlockedRows(t *testing.T) {
	repo, _, db := makeRegisteredGitRepoStateDB(t)
	ctx := context.Background()
	stageBarrierWithSuccessors(t, ctx, repo, db)
	if err := state.MetaSet(ctx, db, "last_replay_error", "stale breadcrumb"); err != nil {
		t.Fatalf("MetaSet: %v", err)
	}

	var out bytes.Buffer
	if err := runFix(ctx, &out, repo, false, true, true, false, true); err != nil {
		t.Fatalf("runFix --force --yes: %v\n%s", err, out.String())
	}
	var plan fixPlan
	if err := json.Unmarshal(out.Bytes(), &plan); err != nil {
		t.Fatalf("unmarshal: %v\n%s", err, out.String())
	}
	purgeIdx, retargetIdx := -1, -1
	for i, action := range plan.Actions {
		switch action.Kind {
		case fixActionPurgeBarrierWithSuccessors:
			purgeIdx = i
			if action.RowsChanged != 1 {
				t.Fatalf("purge rows=%d want 1: %+v", action.RowsChanged, plan.Actions)
			}
		case fixActionRetargetStaleAnchor:
			retargetIdx = i
		}
	}
	if purgeIdx < 0 || retargetIdx < 0 || purgeIdx > retargetIdx {
		t.Fatalf("purge must be planned/applied before retarget: purge=%d retarget=%d actions=%+v", purgeIdx, retargetIdx, plan.Actions)
	}
	got := countCaptureRowsByState(t, db)
	if got[state.EventStateBlockedConflict] != 0 {
		t.Fatalf("blocked rows remain after order-safe force fix: %v", got)
	}
	if got[state.EventStatePending] != 1 {
		t.Fatalf("barrier must be deleted, not reset to pending; row counts: %v", got)
	}
	var barrierRows int
	if err := db.SQL().QueryRowContext(ctx,
		`SELECT COUNT(*) FROM capture_events WHERE path = 'barrier.txt'`,
	).Scan(&barrierRows); err != nil {
		t.Fatalf("query barrier rows: %v", err)
	}
	if barrierRows != 0 {
		t.Fatalf("force purge must delete barrier row, got %d barrier rows", barrierRows)
	}
}

func TestFix_ForceYesPurgesFailedBarrierWithSuccessors(t *testing.T) {
	repo, _, db := makeRegisteredGitRepoStateDB(t)
	ctx := context.Background()
	head, err := git.RevParse(ctx, repo, "HEAD")
	if err != nil {
		t.Fatalf("rev-parse: %v", err)
	}
	if _, err := state.AppendCaptureEvent(ctx, db, state.CaptureEvent{
		BranchRef:        "refs/heads/main",
		BranchGeneration: 1,
		BaseHead:         head,
		Operation:        "modify",
		Path:             "failed-barrier.txt",
		Fidelity:         "exact",
		State:            state.EventStateFailed,
		Error:            sql.NullString{String: "old failure", Valid: true},
	}, nil); err != nil {
		t.Fatalf("seed failed barrier: %v", err)
	}
	if _, err := state.AppendCaptureEvent(ctx, db, state.CaptureEvent{
		BranchRef:        "refs/heads/main",
		BranchGeneration: 1,
		BaseHead:         head,
		Operation:        "modify",
		Path:             "pending-behind-failed.txt",
		Fidelity:         "exact",
		State:            state.EventStatePending,
	}, nil); err != nil {
		t.Fatalf("seed pending successor: %v", err)
	}

	var out bytes.Buffer
	if err := runFix(ctx, &out, repo, false, true, true, false, true); err != nil {
		t.Fatalf("runFix --force --yes: %v\n%s", err, out.String())
	}
	var plan fixPlan
	if err := json.Unmarshal(out.Bytes(), &plan); err != nil {
		t.Fatalf("unmarshal: %v\n%s", err, out.String())
	}
	var purge *fixAction
	for i := range plan.Actions {
		if plan.Actions[i].Kind == fixActionPurgeBarrierWithSuccessors {
			purge = &plan.Actions[i]
			break
		}
	}
	if purge == nil {
		t.Fatalf("plan did not purge failed barrier: %+v", plan.Actions)
	}
	if purge.State != state.EventStateFailed || purge.RowsChanged != 1 {
		t.Fatalf("purge action = %+v, want failed rows=1", *purge)
	}
	got := countCaptureRowsByState(t, db)
	if got[state.EventStateFailed] != 0 {
		t.Fatalf("failed rows remain after --force --yes: %v", got)
	}
	if got[state.EventStatePending] == 0 {
		t.Fatalf("pending successors should remain: %v", got)
	}
}

func TestFix_ForceYesReportsIncompleteWhenPlannedPurgeChangesZeroRows(t *testing.T) {
	repo, stateDB, _ := makeRegisteredGitRepoStateDB(t)
	ctx := context.Background()
	head, err := git.RevParse(ctx, repo, "HEAD")
	if err != nil {
		t.Fatalf("rev-parse: %v", err)
	}
	plan := fixPlan{
		Repo:             repo,
		StateDB:          stateDB,
		GitDir:           filepath.Join(repo, ".git"),
		CurrentBranchRef: "refs/heads/main",
		CurrentHead:      head,
		Generation:       1,
		Force:            true,
		Actions: []fixAction{{
			ID:               fixActionPurgeBarrierWithSuccessors + ":9999",
			Kind:             fixActionPurgeBarrierWithSuccessors,
			Seq:              9999,
			BranchRef:        "refs/heads/main",
			BranchGeneration: 1,
			RequiresForce:    true,
		}},
	}
	if err := applyFixPlan(ctx, stateDB, &plan); err == nil {
		t.Fatalf("applyFixPlan succeeded despite zero-row planned purge: %+v", plan)
	}
	if !plan.Incomplete || len(plan.VerifyErrors) == 0 {
		t.Fatalf("plan missing zero-row incomplete verification: %+v", plan)
	}
}

// TestFix_RetargetActionLandsWhenStaleAnchorPresent mirrors the recover_test
// fixture but drives the new action via acd fix. The fixture stages a stale
// pending row (not blocked, so the older delete_obsolete_barrier path leaves
// it alone); after --yes the row must be retargeted onto refs/heads/main.
func TestFix_RetargetActionLandsWhenStaleAnchorPresent(t *testing.T) {
	repo, _, db := makeRegisteredGitRepoStateDB(t)
	ctx := context.Background()
	head, err := git.RevParse(ctx, repo, "HEAD")
	if err != nil {
		t.Fatalf("rev-parse: %v", err)
	}
	if err := state.SaveDaemonState(ctx, db, state.DaemonState{
		PID:              999999,
		Mode:             "stopped",
		BranchRef:        sql.NullString{String: "refs/heads/stale", Valid: true},
		BranchGeneration: sql.NullInt64{Int64: 2, Valid: true},
	}); err != nil {
		t.Fatalf("SaveDaemonState: %v", err)
	}
	if err := state.MetaSet(ctx, db, "branch.generation", "2"); err != nil {
		t.Fatalf("MetaSet: %v", err)
	}
	seq, err := state.AppendCaptureEvent(ctx, db, state.CaptureEvent{
		BranchRef:        "refs/heads/stale",
		BranchGeneration: 2,
		BaseHead:         head,
		Operation:        "create",
		Path:             "stale.txt",
		Fidelity:         "full",
		State:            state.EventStatePending,
	}, []state.CaptureOp{{
		Op:        "create",
		Path:      "stale.txt",
		Fidelity:  "full",
		AfterMode: sql.NullString{String: "100644", Valid: true},
		AfterOID:  sql.NullString{String: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", Valid: true},
	}})
	if err != nil {
		t.Fatalf("AppendCaptureEvent: %v", err)
	}

	var out bytes.Buffer
	if err := runFix(ctx, &out, repo, false /*dryRun*/, true /*yes*/, false, false, true); err != nil {
		t.Fatalf("runFix --yes: %v\n%s", err, out.String())
	}
	var plan fixPlan
	if err := json.Unmarshal(out.Bytes(), &plan); err != nil {
		t.Fatalf("unmarshal: %v\n%s", err, out.String())
	}
	if !hasFixAction(plan, fixActionRetargetStaleAnchor) {
		t.Fatalf("plan missing retarget_stale_anchor: %+v", plan.Actions)
	}
	var branchRef, eventState string
	var gen int64
	if err := db.SQL().QueryRowContext(ctx,
		`SELECT branch_ref, branch_generation, state FROM capture_events WHERE seq = ?`, seq,
	).Scan(&branchRef, &gen, &eventState); err != nil {
		t.Fatalf("query event: %v", err)
	}
	if branchRef != "refs/heads/main" || gen != 2 || eventState != state.EventStatePending {
		t.Fatalf("retarget did not land: branch=%q gen=%d state=%q want main/2/pending",
			branchRef, gen, eventState)
	}
}

// stageBarrierWithSuccessors seeds: 1 blocked_conflict + 1 pending at higher
// seq on same (refs/heads/main, generation=1) anchored at the current HEAD so
// the planner sees a barrier with successors WITHOUT also tripping the
// retarget_stale_anchor predicate. Used by the --force tests.
func stageBarrierWithSuccessors(t *testing.T, ctx context.Context, repo string, db *state.DB) {
	t.Helper()
	head, err := git.RevParse(ctx, repo, "HEAD")
	if err != nil {
		t.Fatalf("rev-parse: %v", err)
	}
	if _, err := state.AppendCaptureEvent(ctx, db, state.CaptureEvent{
		BranchRef:        "refs/heads/main",
		BranchGeneration: 1,
		BaseHead:         head,
		Operation:        "modify",
		Path:             "barrier.txt",
		Fidelity:         "exact",
		State:            state.EventStateBlockedConflict,
		Error:            sql.NullString{String: "old conflict", Valid: true},
	}, nil); err != nil {
		t.Fatalf("seed blocked: %v", err)
	}
	if _, err := state.AppendCaptureEvent(ctx, db, state.CaptureEvent{
		BranchRef:        "refs/heads/main",
		BranchGeneration: 1,
		BaseHead:         head,
		Operation:        "modify",
		Path:             "successor.txt",
		Fidelity:         "exact",
		State:            state.EventStatePending,
	}, nil); err != nil {
		t.Fatalf("seed pending successor: %v", err)
	}
	// Mirror publish_state singleton so the breadcrumb-clear path is exercised.
	if _, err := db.SQL().ExecContext(ctx, `
INSERT INTO publish_state(id, event_seq, branch_ref, branch_generation, source_head, status, error, updated_ts)
VALUES (1, 1, 'refs/heads/main', 1, ?, 'blocked_conflict', 'modify before-state mismatch', 1.0)
ON CONFLICT(id) DO UPDATE SET status=excluded.status, error=excluded.error`, head); err != nil {
		t.Fatalf("seed publish_state: %v", err)
	}
}

func seedGeneratedPendingFixFixture(t *testing.T, ctx context.Context, repo string, db *state.DB) []int64 {
	t.Helper()
	write := func(rel, body string) {
		t.Helper()
		full := filepath.Join(repo, filepath.FromSlash(rel))
		if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
			t.Fatalf("mkdir %s: %v", rel, err)
		}
		if err := os.WriteFile(full, []byte(body), 0o644); err != nil {
			t.Fatalf("write %s: %v", rel, err)
		}
	}
	write(".derivedData-provider-core/Index.noindex/a.db", "a")
	write(".derivedData-provider-core/Index.noindex/b.db", "b")
	write("build/output.js", "ignored but not safe-ignore")
	if _, err := git.Run(ctx, git.RunOpts{Dir: repo}, "add", "-f",
		".derivedData-provider-core/Index.noindex/a.db",
		".derivedData-provider-core/Index.noindex/b.db"); err != nil {
		t.Fatalf("git add forced generated files: %v", err)
	}
	head, err := git.RevParse(ctx, repo, "HEAD")
	if err != nil {
		t.Fatalf("rev-parse: %v", err)
	}
	seed := func(path, op string) int64 {
		t.Helper()
		seq, err := state.AppendCaptureEvent(ctx, db, state.CaptureEvent{
			BranchRef:        "refs/heads/main",
			BranchGeneration: 1,
			BaseHead:         head,
			Operation:        op,
			Path:             path,
			Fidelity:         "full",
			State:            state.EventStatePending,
		}, []state.CaptureOp{{
			Op:         op,
			Path:       path,
			Fidelity:   "full",
			BeforeOID:  sql.NullString{String: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", Valid: op == "delete"},
			BeforeMode: sql.NullString{String: git.RegularFileMode, Valid: op == "delete"},
			AfterOID:   sql.NullString{String: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", Valid: op != "delete"},
			AfterMode:  sql.NullString{String: git.RegularFileMode, Valid: op != "delete"},
		}})
		if err != nil {
			t.Fatalf("AppendCaptureEvent(%s,%s): %v", op, path, err)
		}
		if err := state.RecordPlannerOffer(ctx, db, seq, 123); err != nil {
			t.Fatalf("RecordPlannerOffer(%d): %v", seq, err)
		}
		return seq
	}
	seqs := []int64{
		seed(".derivedData-provider-core/Index.noindex/a.db", "delete"),
		seed(".derivedData-provider-core/Index.noindex/b.db", "delete"),
	}
	seed("build/output.js", "delete")
	seed("src/ordinary.txt", "modify")
	return seqs
}

func countRowsWhere(t *testing.T, db *state.DB, table, where string, args ...any) int {
	t.Helper()
	var n int
	q := fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE %s", table, where)
	if err := db.SQL().QueryRowContext(context.Background(), q, args...).Scan(&n); err != nil {
		t.Fatalf("count %s where %s: %v", table, where, err)
	}
	return n
}

func gitCachedNameStatus(t *testing.T, ctx context.Context, repo string) string {
	t.Helper()
	out, err := git.Run(ctx, git.RunOpts{Dir: repo}, "diff", "--cached", "--name-status")
	if err != nil {
		t.Fatalf("git diff --cached --name-status: %v", err)
	}
	return string(out)
}
