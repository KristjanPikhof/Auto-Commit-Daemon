package cli

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/daemon"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/paths"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

func TestDiagnose_AnchorMismatchDetected(t *testing.T) {
	roots := withIsolatedHome(t)
	ctx := context.Background()
	repo, dbPath, d := makeDiagnoseRepo(t, roots)

	if err := state.SaveDaemonState(ctx, d, state.DaemonState{
		PID: 7, Mode: "running", BranchRef: sql.NullString{String: "refs/heads/feature", Valid: true},
		BranchGeneration: sql.NullInt64{Int64: 3, Valid: true},
	}); err != nil {
		t.Fatalf("save daemon_state: %v", err)
	}
	if err := state.MetaSet(ctx, d, "branch_token", "rev:abc refs/heads/feature"); err != nil {
		t.Fatalf("set branch token: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}
	checksumBefore := mustSHA256(t, dbPath)

	var out bytes.Buffer
	if err := runDiagnose(ctx, &out, repo, false); err != nil {
		t.Fatalf("runDiagnose: %v", err)
	}
	got := out.String()
	for _, want := range []string{
		"Anchor: git HEAD=refs/heads/main daemon=refs/heads/feature generation=3 MISMATCH",
		"Branch token: rev:abc refs/heads/feature",
		"Read-only: verified",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("diagnose output missing %q in:\n%s", want, got)
		}
	}
	if checksumAfter := mustSHA256(t, dbPath); checksumBefore != checksumAfter {
		t.Fatalf("state.db checksum changed: before=%s after=%s", checksumBefore, checksumAfter)
	}
}

func TestDiagnose_AnchorFallsBackToBranchToken(t *testing.T) {
	roots := withIsolatedHome(t)
	ctx := context.Background()
	repo, _, d := makeDiagnoseRepo(t, roots)

	if err := state.SaveDaemonState(ctx, d, state.DaemonState{
		PID: 7, Mode: "running",
	}); err != nil {
		t.Fatalf("save daemon_state: %v", err)
	}
	if err := state.MetaSet(ctx, d, "branch_token", "rev:abc refs/heads/main"); err != nil {
		t.Fatalf("set branch token: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}

	var out bytes.Buffer
	if err := runDiagnose(ctx, &out, repo, true); err != nil {
		t.Fatalf("runDiagnose: %v", err)
	}
	var rep diagnoseReport
	if err := json.Unmarshal(out.Bytes(), &rep); err != nil {
		t.Fatalf("unmarshal diagnose: %v\n%s", err, out.String())
	}
	if rep.Anchor.Mismatch {
		t.Fatalf("anchor mismatch should be false when branch_token matches HEAD: %+v", rep.Anchor)
	}
	if rep.Anchor.DaemonBranchRef != "refs/heads/main" {
		t.Fatalf("daemon branch fallback=%q want refs/heads/main", rep.Anchor.DaemonBranchRef)
	}
}

func TestDiagnose_BlockedHistogram(t *testing.T) {
	roots := withIsolatedHome(t)
	ctx := context.Background()
	repo, _, d := makeDiagnoseRepo(t, roots)

	if err := state.SaveDaemonState(ctx, d, state.DaemonState{
		PID: 7, Mode: "running", BranchRef: sql.NullString{String: "refs/heads/main", Valid: true},
		BranchGeneration: sql.NullInt64{Int64: 1, Valid: true},
	}); err != nil {
		t.Fatalf("save daemon_state: %v", err)
	}
	blockDiagnoseEvent(t, ctx, d, "a.go", "before-state mismatch: missing-in-index")
	blockDiagnoseEvent(t, ctx, d, "b.go", "commit-tree failed")
	seq := blockDiagnoseEvent(t, ctx, d, "c.go", "plain replay conflict")
	if err := state.MetaSet(ctx, d, "last_replay_conflict",
		`{"seq":`+itoa64(seq)+`,"error_class":"cas_fail","message":"structured"}`); err != nil {
		t.Fatalf("set last_replay_conflict: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}

	var out bytes.Buffer
	if err := runDiagnose(ctx, &out, repo, true); err != nil {
		t.Fatalf("runDiagnose: %v", err)
	}
	var rep diagnoseReport
	if err := json.Unmarshal(out.Bytes(), &rep); err != nil {
		t.Fatalf("unmarshal diagnose: %v\n%s", err, out.String())
	}
	want := map[string]int{
		"before_state_mismatch": 1,
		"commit_build_failure":  1,
		"cas_fail":              1,
	}
	got := map[string]int{}
	for _, bucket := range rep.BlockedHistogram {
		got[bucket.ErrorClass] = bucket.Count
	}
	for cls, count := range want {
		if got[cls] != count {
			t.Fatalf("histogram[%s]=%d, want %d; full=%v", cls, got[cls], count, rep.BlockedHistogram)
		}
	}
	if len(rep.RecentBlocked) != 3 {
		t.Fatalf("recent blocked len=%d, want 3", len(rep.RecentBlocked))
	}
	if rep.RecentBlocked[0].Seq != seq || rep.RecentBlocked[0].ErrorClass != "cas_fail" {
		t.Fatalf("recent[0]=%+v, want newest structured cas_fail seq %d", rep.RecentBlocked[0], seq)
	}
}

func TestDiagnose_FailedBarrierGuidance(t *testing.T) {
	roots := withIsolatedHome(t)
	ctx := context.Background()
	repo, _, d := makeDiagnoseRepo(t, roots)

	if err := state.SaveDaemonState(ctx, d, state.DaemonState{
		PID: 7, Mode: "running", BranchRef: sql.NullString{String: "refs/heads/main", Valid: true},
		BranchGeneration: sql.NullInt64{Int64: 1, Valid: true},
	}); err != nil {
		t.Fatalf("save daemon_state: %v", err)
	}
	seq, err := state.AppendCaptureEvent(ctx, d, state.CaptureEvent{
		BranchRef: "refs/heads/main", BranchGeneration: 1,
		BaseHead: "deadbeef", Operation: "modify", Path: "bad.go",
		Fidelity: "exact",
	}, []state.CaptureOp{{Op: "modify", Path: "bad.go", Fidelity: "exact"}})
	if err != nil {
		t.Fatalf("append failed event: %v", err)
	}
	if err := state.MarkEventPublished(ctx, d, seq, state.EventStateFailed,
		sql.NullString{}, sql.NullString{String: "commit-tree failed", Valid: true},
		sql.NullString{}, nowFloat()); err != nil {
		t.Fatalf("mark failed: %v", err)
	}
	if _, err := state.AppendCaptureEvent(ctx, d, state.CaptureEvent{
		BranchRef: "refs/heads/main", BranchGeneration: 1,
		BaseHead: "deadbeef", Operation: "modify", Path: "later.go",
		Fidelity: "exact",
	}, []state.CaptureOp{{Op: "modify", Path: "later.go", Fidelity: "exact"}}); err != nil {
		t.Fatalf("append pending successor: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}

	var out bytes.Buffer
	if err := runDiagnose(ctx, &out, repo, true); err != nil {
		t.Fatalf("runDiagnose json: %v", err)
	}
	var rep diagnoseReport
	if err := json.Unmarshal(out.Bytes(), &rep); err != nil {
		t.Fatalf("unmarshal diagnose: %v\n%s", err, out.String())
	}
	if rep.FailedEvents != 1 || rep.FailedBlockingPending != 1 {
		t.Fatalf("failed fields = events %d blocking %d, want 1/1", rep.FailedEvents, rep.FailedBlockingPending)
	}
	if len(rep.RecentBlocked) == 0 || rep.RecentBlocked[0].State != state.EventStateFailed {
		t.Fatalf("recent barriers missing state: %+v", rep.RecentBlocked)
	}
	if !containsStringWith(rep.Remediation, "acd fix --dry-run") {
		t.Fatalf("remediation missing fix dry-run: %v", rep.Remediation)
	}

	out.Reset()
	if err := runDiagnose(ctx, &out, repo, false); err != nil {
		t.Fatalf("runDiagnose human: %v", err)
	}
	for _, want := range []string{"Failed terminal events: 1", "blocking pending replay", "acd fix --dry-run", "failed modify bad.go"} {
		if !strings.Contains(out.String(), want) {
			t.Fatalf("diagnose human missing %q in:\n%s", want, out.String())
		}
	}
}

func containsStringWith(items []string, needle string) bool {
	for _, item := range items {
		if strings.Contains(item, needle) {
			return true
		}
	}
	return false
}

func TestDiagnose_LegacyReplayConflictMetadataFallsBackToRowError(t *testing.T) {
	roots := withIsolatedHome(t)
	ctx := context.Background()
	repo, _, d := makeDiagnoseRepo(t, roots)

	if err := state.SaveDaemonState(ctx, d, state.DaemonState{
		PID: 7, Mode: "running", BranchRef: sql.NullString{String: "refs/heads/main", Valid: true},
		BranchGeneration: sql.NullInt64{Int64: 1, Valid: true},
	}); err != nil {
		t.Fatalf("save daemon_state: %v", err)
	}
	seq := blockDiagnoseEvent(t, ctx, d, "legacy.go", "before-state mismatch: expected abc actual def")
	if err := state.MetaSet(ctx, d, "last_replay_conflict", "seq="+itoa64(seq)+": update-ref CAS failed"); err != nil {
		t.Fatalf("set legacy last_replay_conflict: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}

	var out bytes.Buffer
	if err := runDiagnose(ctx, &out, repo, true); err != nil {
		t.Fatalf("runDiagnose: %v", err)
	}
	var rep diagnoseReport
	if err := json.Unmarshal(out.Bytes(), &rep); err != nil {
		t.Fatalf("unmarshal diagnose: %v\n%s", err, out.String())
	}
	if len(rep.RecentBlocked) != 1 {
		t.Fatalf("recent blocked len=%d, want 1", len(rep.RecentBlocked))
	}
	if got := rep.RecentBlocked[0].ErrorClass; got != "before_state_mismatch" {
		t.Fatalf("recent blocked error_class=%q, want before_state_mismatch", got)
	}
}

func TestDiagnose_JSONOutput(t *testing.T) {
	roots := withIsolatedHome(t)
	ctx := context.Background()
	repo, _, d := makeDiagnoseRepo(t, roots)
	if err := state.SaveDaemonState(ctx, d, state.DaemonState{
		PID: 7, Mode: "running", BranchRef: sql.NullString{String: "refs/heads/main", Valid: true},
		BranchGeneration: sql.NullInt64{Int64: 1, Valid: true},
	}); err != nil {
		t.Fatalf("save daemon_state: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}

	root := newRootCmd()
	var out, errOut bytes.Buffer
	root.SetOut(&out)
	root.SetErr(&errOut)
	root.SetArgs([]string{"diagnose", "--repo", repo, "--json"})
	if err := root.ExecuteContext(ctx); err != nil {
		t.Fatalf("execute diagnose: %v\nstderr:\n%s", err, errOut.String())
	}

	var rep diagnoseReport
	if err := json.Unmarshal(out.Bytes(), &rep); err != nil {
		t.Fatalf("unmarshal diagnose JSON: %v\n%s", err, out.String())
	}
	if rep.Repo != repo {
		t.Fatalf("repo=%q, want %q", rep.Repo, repo)
	}
	if rep.Anchor.Mismatch {
		t.Fatalf("anchor mismatch = true, want false: %+v", rep.Anchor)
	}
	if !rep.StateDBChecksumVerified || rep.StateDBChecksumBefore == "" || rep.StateDBChecksumAfter == "" {
		t.Fatalf("checksum fields not verified: before=%q after=%q verified=%v",
			rep.StateDBChecksumBefore, rep.StateDBChecksumAfter, rep.StateDBChecksumVerified)
	}
	if len(rep.Remediation) != 1 || !strings.Contains(rep.Remediation[0], "No anchor mismatch") {
		t.Fatalf("unexpected remediation: %v", rep.Remediation)
	}
}

// TestDiagnose_BackpressureSurfaced asserts the new
// capture.backpressure_paused_at + capture.events_dropped_total meta keys
// are surfaced via `acd diagnose --json`. Operators rely on these to
// distinguish "saturated and refusing new events" from "all is well".
func TestDiagnose_BackpressureSurfaced(t *testing.T) {
	roots := withIsolatedHome(t)
	ctx := context.Background()
	repo, _, d := makeDiagnoseRepo(t, roots)

	stamp := time.Now().UTC().Format(time.RFC3339)
	if err := state.MetaSet(ctx, d, daemon.MetaKeyCaptureBackpressurePausedAt, stamp); err != nil {
		t.Fatalf("seed backpressure meta: %v", err)
	}
	if err := state.MetaSet(ctx, d, daemon.MetaKeyCaptureEventsDroppedTotal, "42"); err != nil {
		t.Fatalf("seed dropped total: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}

	root := newRootCmd()
	var out, errOut bytes.Buffer
	root.SetOut(&out)
	root.SetErr(&errOut)
	root.SetArgs([]string{"diagnose", "--repo", repo, "--json"})
	if err := root.ExecuteContext(ctx); err != nil {
		t.Fatalf("execute diagnose: %v\nstderr:\n%s", err, errOut.String())
	}
	var rep diagnoseReport
	if err := json.Unmarshal(out.Bytes(), &rep); err != nil {
		t.Fatalf("unmarshal: %v\n%s", err, out.String())
	}
	if !rep.BackpressurePaused {
		t.Fatalf("BackpressurePaused=false; want true")
	}
	if rep.BackpressurePausedAt != stamp {
		t.Fatalf("BackpressurePausedAt=%q, want %q", rep.BackpressurePausedAt, stamp)
	}
	if rep.EventsDroppedTotal != 42 {
		t.Fatalf("EventsDroppedTotal=%d, want 42", rep.EventsDroppedTotal)
	}
	// The remediation array must include a backpressure-specific hint so
	// operators see the recovery path without grepping logs.
	var sawHint bool
	for _, r := range rep.Remediation {
		if strings.Contains(r, "backpressure") {
			sawHint = true
			break
		}
	}
	if !sawHint {
		t.Fatalf("remediation lacks backpressure hint: %v", rep.Remediation)
	}
}

// TestStatus_BackpressureSurfaced mirrors TestDiagnose_BackpressureSurfaced
// for the `acd status --json` surface.
func TestStatus_BackpressureSurfaced(t *testing.T) {
	roots := withIsolatedHome(t)
	ctx := context.Background()
	repo, _, d := makeDiagnoseRepo(t, roots)

	stamp := time.Now().UTC().Format(time.RFC3339)
	if err := state.MetaSet(ctx, d, daemon.MetaKeyCaptureBackpressurePausedAt, stamp); err != nil {
		t.Fatalf("seed backpressure meta: %v", err)
	}
	if err := state.MetaSet(ctx, d, daemon.MetaKeyCaptureEventsDroppedTotal, "9"); err != nil {
		t.Fatalf("seed dropped total: %v", err)
	}

	var out bytes.Buffer
	if err := runStatus(ctx, &out, repo, true); err != nil {
		t.Fatalf("runStatus: %v", err)
	}
	var rep statusReport
	if err := json.Unmarshal(out.Bytes(), &rep); err != nil {
		t.Fatalf("unmarshal: %v\n%s", err, out.String())
	}
	if !rep.BackpressurePaused {
		t.Fatalf("BackpressurePaused=false; want true")
	}
	if rep.BackpressurePausedAt != stamp {
		t.Fatalf("BackpressurePausedAt=%q, want %q", rep.BackpressurePausedAt, stamp)
	}
	if rep.EventsDroppedTotal != 9 {
		t.Fatalf("EventsDroppedTotal=%d, want 9", rep.EventsDroppedTotal)
	}
}

// seedDiagnoseCommit writes one file and commits it so that git rev-parse HEAD
// resolves to a real SHA. Returns the HEAD SHA.
func seedDiagnoseCommit(t *testing.T, repoDir string) string {
	t.Helper()
	ctx := context.Background()
	for _, kv := range [][]string{
		{"user.email", "acd-test@example.com"},
		{"user.name", "ACD Test"},
		{"commit.gpgsign", "false"},
	} {
		if _, err := git.Run(ctx, git.RunOpts{Dir: repoDir}, "config", kv[0], kv[1]); err != nil {
			t.Fatalf("git config %s: %v", kv[0], err)
		}
	}
	if err := os.WriteFile(filepath.Join(repoDir, "seed.txt"), []byte("seed\n"), 0o644); err != nil {
		t.Fatalf("write seed: %v", err)
	}
	if _, err := git.Run(ctx, git.RunOpts{Dir: repoDir}, "add", "seed.txt"); err != nil {
		t.Fatalf("git add seed: %v", err)
	}
	if _, err := git.Run(ctx, git.RunOpts{Dir: repoDir}, "commit", "-q", "-m", "seed"); err != nil {
		t.Fatalf("git commit seed: %v", err)
	}
	head, err := git.RevParse(ctx, repoDir, "HEAD")
	if err != nil {
		t.Fatalf("rev-parse HEAD: %v", err)
	}
	return head
}

// seedOperationMarker writes the three meta keys that diagnoseOperationMarker
// reads: the operation name, the set_at timestamp (oldEnough seconds ago), and
// the HEAD SHA at marker onset.
func seedOperationMarker(t *testing.T, ctx context.Context, d *state.DB, op, headAt string, oldEnough time.Duration) {
	t.Helper()
	setAt := time.Now().Add(-oldEnough)
	stamp := strconv.FormatFloat(float64(setAt.UnixNano())/1e9, 'f', -1, 64)
	if err := state.MetaSet(ctx, d, daemon.MetaKeyOperationInProgress, op); err != nil {
		t.Fatalf("seed operation_in_progress: %v", err)
	}
	if err := state.MetaSet(ctx, d, daemon.MetaKeyOperationInProgressSetAt, stamp); err != nil {
		t.Fatalf("seed operation_in_progress.set_at: %v", err)
	}
	if err := state.MetaSet(ctx, d, daemon.MetaKeyOperationInProgressHead, headAt); err != nil {
		t.Fatalf("seed operation_in_progress.head_at: %v", err)
	}
}

// TestDiagnose_StaleOperationMarker_HeadAdvanced_NotStale verifies that when
// an operation_in_progress marker has been present beyond the staleness
// threshold but HEAD has advanced since the marker was first recorded, the
// report does NOT flag it as stale. A long-running interactive rebase still
// making progress must not produce a false stale_operation_marker.
func TestDiagnose_StaleOperationMarker_HeadAdvanced_NotStale(t *testing.T) {
	roots := withIsolatedHome(t)
	ctx := context.Background()
	repo, _, d := makeDiagnoseRepo(t, roots)

	// Create a real commit so HEAD resolves to a known SHA.
	currentHead := seedDiagnoseCommit(t, repo)

	// Seed the marker with a *different* head_at SHA — simulates HEAD having
	// advanced since the marker was first recorded.
	const staleSHA = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	seedOperationMarker(t, ctx, d, "rebase-merge", staleSHA, 20*time.Minute)
	if err := d.Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}

	var out bytes.Buffer
	if err := runDiagnose(ctx, &out, repo, true); err != nil {
		t.Fatalf("runDiagnose: %v", err)
	}
	var rep diagnoseReport
	if err := json.Unmarshal(out.Bytes(), &rep); err != nil {
		t.Fatalf("unmarshal diagnose: %v\n%s", err, out.String())
	}
	if rep.StaleOperationMarker {
		t.Fatalf("StaleOperationMarker=true but HEAD advanced from %s to %s; expected false",
			staleSHA, currentHead)
	}
	if rep.OperationInProgress != "rebase-merge" {
		t.Fatalf("OperationInProgress=%q, want rebase-merge", rep.OperationInProgress)
	}
}

// TestDiagnose_StaleOperationMarker_HeadMotionless_IsStale verifies that when
// an operation_in_progress marker has been present beyond the staleness
// threshold and HEAD has NOT moved since the marker was first recorded, the
// report flags it as stale. This matches the abandoned-rebase scenario the
// remediation hint is written for.
func TestDiagnose_StaleOperationMarker_HeadMotionless_IsStale(t *testing.T) {
	roots := withIsolatedHome(t)
	ctx := context.Background()
	repo, _, d := makeDiagnoseRepo(t, roots)

	// Create a real commit so HEAD resolves.
	currentHead := seedDiagnoseCommit(t, repo)

	// Seed the marker with head_at == current HEAD — HEAD has not moved.
	seedOperationMarker(t, ctx, d, "merge", currentHead, 20*time.Minute)
	if err := d.Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}

	var out bytes.Buffer
	if err := runDiagnose(ctx, &out, repo, true); err != nil {
		t.Fatalf("runDiagnose: %v", err)
	}
	var rep diagnoseReport
	if err := json.Unmarshal(out.Bytes(), &rep); err != nil {
		t.Fatalf("unmarshal diagnose: %v\n%s", err, out.String())
	}
	if !rep.StaleOperationMarker {
		t.Fatalf("StaleOperationMarker=false but elapsed > threshold and HEAD unchanged at %s; expected true",
			currentHead)
	}
	// Remediation must mention the operation name and HEAD-motionless context.
	var sawHint bool
	for _, r := range rep.Remediation {
		if strings.Contains(r, "operation_in_progress=merge") && strings.Contains(r, "no HEAD movement") {
			sawHint = true
			break
		}
	}
	if !sawHint {
		t.Fatalf("remediation lacks stale-marker hint: %v", rep.Remediation)
	}
}

// TestDiagnose_CapacityRemediation_FiresOnDepthAlone verifies that the capacity
// remediation hint fires when PendingDepth > 0 even when PendingHighWater is
// zero (unset, as on a fresh repo that has never hit the high-water mark).
func TestDiagnose_CapacityRemediation_FiresOnDepthAlone(t *testing.T) {
	roots := withIsolatedHome(t)
	ctx := context.Background()
	repo, _, d := makeDiagnoseRepo(t, roots)

	if err := state.SaveDaemonState(ctx, d, state.DaemonState{
		PID: 7, Mode: "running", BranchRef: sql.NullString{String: "refs/heads/main", Valid: true},
		BranchGeneration: sql.NullInt64{Int64: 1, Valid: true},
	}); err != nil {
		t.Fatalf("save daemon_state: %v", err)
	}
	// Append one pending event to make PendingDepth = 1. PendingHighWater is
	// intentionally left at 0 (never set) to exercise the relaxed guard.
	if _, err := state.AppendCaptureEvent(ctx, d, state.CaptureEvent{
		BranchRef: "refs/heads/main", BranchGeneration: 1,
		BaseHead: "deadbeef", Operation: "modify", Path: "a.go",
		Fidelity: "exact", CapturedTS: nowFloat(),
	}, []state.CaptureOp{{Op: "modify", Path: "a.go", Fidelity: "exact"}}); err != nil {
		t.Fatalf("append capture event: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}

	var out bytes.Buffer
	if err := runDiagnose(ctx, &out, repo, true); err != nil {
		t.Fatalf("runDiagnose: %v", err)
	}
	var rep diagnoseReport
	if err := json.Unmarshal(out.Bytes(), &rep); err != nil {
		t.Fatalf("unmarshal diagnose: %v\n%s", err, out.String())
	}
	if rep.PendingDepth != 1 {
		t.Fatalf("PendingDepth=%d, want 1", rep.PendingDepth)
	}
	if rep.PendingHighWater != 0 {
		t.Fatalf("PendingHighWater=%d, want 0 (test pre-condition)", rep.PendingHighWater)
	}
	var sawHint bool
	for _, r := range rep.Remediation {
		if strings.Contains(r, "pending depth is non-zero") {
			sawHint = true
			break
		}
	}
	if !sawHint {
		t.Fatalf("remediation lacks capacity hint even with PendingDepth=1 and PendingHighWater=0: %v",
			rep.Remediation)
	}
}

func TestDiagnose_IntentBatchWaitUsesDefaultsWithoutNewMetadata(t *testing.T) {
	roots := withIsolatedHome(t)
	ctx := context.Background()
	repo, _, d := makeDiagnoseRepo(t, roots)

	if err := state.SaveDaemonState(ctx, d, state.DaemonState{
		PID: 7, Mode: "running", BranchRef: sql.NullString{String: "refs/heads/main", Valid: true},
		BranchGeneration: sql.NullInt64{Int64: 1, Valid: true},
	}); err != nil {
		t.Fatalf("save daemon_state: %v", err)
	}
	if err := state.MetaSet(ctx, d, "commit.strategy", "intent"); err != nil {
		t.Fatalf("set commit.strategy: %v", err)
	}
	appendIntentPendingEvent(t, ctx, d, "sparse.go", nowFloat()-30)
	if err := d.Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}

	var out bytes.Buffer
	if err := runDiagnose(ctx, &out, repo, true); err != nil {
		t.Fatalf("runDiagnose: %v", err)
	}
	var rep diagnoseReport
	if err := json.Unmarshal(out.Bytes(), &rep); err != nil {
		t.Fatalf("unmarshal diagnose: %v\n%s", err, out.String())
	}
	if !rep.IntentStrategy.BatchWaitActive ||
		rep.IntentStrategy.MinPending != 10 ||
		rep.IntentStrategy.MaxPendingAgeSeconds != 300 ||
		rep.IntentStrategy.VisiblePendingEvents != 1 {
		t.Fatalf("intent strategy = %+v, want defaulted active batch wait", rep.IntentStrategy)
	}
	var sawHint bool
	for _, item := range rep.Remediation {
		if strings.Contains(item, "intent replay is waiting") &&
			strings.Contains(item, "ACD_INTENT_MIN_PENDING") &&
			strings.Contains(item, "ACD_COMMIT_STRATEGY=event") {
			sawHint = true
			break
		}
	}
	if !sawHint {
		t.Fatalf("remediation lacks intent batch wait hint: %v", rep.Remediation)
	}

	var humanOut bytes.Buffer
	if err := runDiagnose(ctx, &humanOut, repo, false); err != nil {
		t.Fatalf("runDiagnose human: %v", err)
	}
	if !strings.Contains(humanOut.String(), "Intent batch wait: pending=1 min_pending=10") {
		t.Fatalf("diagnose human missing batch wait line:\n%s", humanOut.String())
	}
}

func makeDiagnoseRepo(t *testing.T, roots paths.Roots) (repoDir, dbPath string, d *state.DB) {
	t.Helper()
	ctx := context.Background()
	repoDir = t.TempDir()
	if err := git.Init(ctx, repoDir); err != nil {
		t.Fatalf("git init: %v", err)
	}
	if _, err := git.Run(ctx, git.RunOpts{Dir: repoDir}, "symbolic-ref", "HEAD", "refs/heads/main"); err != nil {
		t.Fatalf("symbolic-ref HEAD: %v", err)
	}
	dbPath = state.DBPathFromGitDir(filepath.Join(repoDir, ".git"))
	var err error
	d, err = state.Open(ctx, dbPath)
	if err != nil {
		t.Fatalf("state.Open: %v", err)
	}
	registerRepo(t, roots, repoDir, dbPath, "codex")
	return repoDir, dbPath, d
}

func blockDiagnoseEvent(t *testing.T, ctx context.Context, d *state.DB, path, message string) int64 {
	t.Helper()
	seq, err := state.AppendCaptureEvent(ctx, d, state.CaptureEvent{
		BranchRef: "refs/heads/main", BranchGeneration: 1,
		BaseHead: "deadbeef", Operation: "modify", Path: path,
		Fidelity: "exact", CapturedTS: nowFloat(),
	}, []state.CaptureOp{{Op: "modify", Path: path, Fidelity: "exact"}})
	if err != nil {
		t.Fatalf("append event: %v", err)
	}
	if err := state.MarkEventBlocked(ctx, d, seq, message, nowFloat(),
		sql.NullString{String: "refs/heads/main", Valid: true},
		sql.NullInt64{Int64: 1, Valid: true},
		sql.NullString{String: "deadbeef", Valid: true},
	); err != nil {
		t.Fatalf("mark blocked: %v", err)
	}
	return seq
}

func mustSHA256(t *testing.T, path string) string {
	t.Helper()
	got, err := fileSHA256(path)
	if err != nil {
		t.Fatalf("sha256 %s: %v", path, err)
	}
	return got
}

func itoa64(v int64) string {
	return strconv.FormatInt(v, 10)
}

// TestDiagnose_DeadBranchPrune_Populated seeds the three daemon_meta keys
// stamped by daemon.recordDeadBranchPruneMeta and asserts diagnose surfaces
// them on the JSON report with the agreed snake_case field names. This is
// the operator-visible signal that stale-branch hygiene ran and what it
// pruned.
func TestDiagnose_DeadBranchPrune_Populated(t *testing.T) {
	roots := withIsolatedHome(t)
	ctx := context.Background()
	repo, _, d := makeDiagnoseRepo(t, roots)

	const wantTS int64 = 1_700_000_000
	const wantCount = 7
	wantRefs := []string{"refs/heads/old", "refs/heads/feature-x"}
	refsJSON, err := json.Marshal(wantRefs)
	if err != nil {
		t.Fatalf("marshal refs: %v", err)
	}
	if err := state.MetaSet(ctx, d, daemon.MetaKeyDeadBranchPruneLastRunTS, strconv.FormatInt(wantTS, 10)); err != nil {
		t.Fatalf("seed last_run_ts: %v", err)
	}
	if err := state.MetaSet(ctx, d, daemon.MetaKeyDeadBranchPruneLastCount, strconv.Itoa(wantCount)); err != nil {
		t.Fatalf("seed last_count: %v", err)
	}
	if err := state.MetaSet(ctx, d, daemon.MetaKeyDeadBranchPruneLastRefs, string(refsJSON)); err != nil {
		t.Fatalf("seed last_refs: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}

	var out bytes.Buffer
	if err := runDiagnose(ctx, &out, repo, true); err != nil {
		t.Fatalf("runDiagnose: %v", err)
	}
	var rep diagnoseReport
	if err := json.Unmarshal(out.Bytes(), &rep); err != nil {
		t.Fatalf("unmarshal diagnose: %v\n%s", err, out.String())
	}
	if rep.DeadBranchPruneLastRunTS != wantTS {
		t.Fatalf("DeadBranchPruneLastRunTS=%d want %d", rep.DeadBranchPruneLastRunTS, wantTS)
	}
	if rep.DeadBranchPruneLastCount != wantCount {
		t.Fatalf("DeadBranchPruneLastCount=%d want %d", rep.DeadBranchPruneLastCount, wantCount)
	}
	if len(rep.DeadBranchPruneLastRefs) != len(wantRefs) {
		t.Fatalf("DeadBranchPruneLastRefs len=%d want %d", len(rep.DeadBranchPruneLastRefs), len(wantRefs))
	}
	for i, r := range wantRefs {
		if rep.DeadBranchPruneLastRefs[i] != r {
			t.Fatalf("DeadBranchPruneLastRefs[%d]=%q want %q", i, rep.DeadBranchPruneLastRefs[i], r)
		}
	}
	// JSON keys must match the agreed snake_case names so dashboards and
	// scripted operators are not regressed by a struct-tag drift.
	for _, want := range []string{
		`"dead_branch_prune_last_run_ts": 1700000000`,
		`"dead_branch_prune_last_count": 7`,
		`"dead_branch_prune_last_refs"`,
	} {
		if !strings.Contains(out.String(), want) {
			t.Fatalf("diagnose JSON missing %q in:\n%s", want, out.String())
		}
	}
}

// TestDiagnose_DeadBranchPrune_Absent asserts that when no meta keys are
// present (fresh repo / never-pruned), all three fields default to their
// zero values. The two int fields render as 0 in the JSON (zero is the
// documented "never ran" sentinel — distinguishable absence is no longer
// needed because zero IS the meaningful value). The slice keeps `omitempty`
// and renders as absent when nil.
func TestDiagnose_DeadBranchPrune_Absent(t *testing.T) {
	roots := withIsolatedHome(t)
	ctx := context.Background()
	repo, _, d := makeDiagnoseRepo(t, roots)
	if err := d.Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}

	var out bytes.Buffer
	if err := runDiagnose(ctx, &out, repo, true); err != nil {
		t.Fatalf("runDiagnose: %v", err)
	}
	var rep diagnoseReport
	if err := json.Unmarshal(out.Bytes(), &rep); err != nil {
		t.Fatalf("unmarshal diagnose: %v\n%s", err, out.String())
	}
	if rep.DeadBranchPruneLastRunTS != 0 {
		t.Fatalf("DeadBranchPruneLastRunTS=%d want 0", rep.DeadBranchPruneLastRunTS)
	}
	if rep.DeadBranchPruneLastCount != 0 {
		t.Fatalf("DeadBranchPruneLastCount=%d want 0", rep.DeadBranchPruneLastCount)
	}
	if rep.DeadBranchPruneLastRefs != nil {
		t.Fatalf("DeadBranchPruneLastRefs=%v want nil", rep.DeadBranchPruneLastRefs)
	}
	// The two int fields must be present (always-emit contract) with their
	// zero values. The slice (omitempty) must be absent when nil.
	for _, want := range []string{
		`"dead_branch_prune_last_run_ts": 0`,
		`"dead_branch_prune_last_count": 0`,
	} {
		if !strings.Contains(out.String(), want) {
			t.Fatalf("diagnose JSON missing %q in:\n%s", want, out.String())
		}
	}
	if strings.Contains(out.String(), "dead_branch_prune_last_refs") {
		t.Fatalf("diagnose JSON unexpectedly contains 'dead_branch_prune_last_refs' (slice omitempty broken):\n%s", out.String())
	}
}

// TestDiagnose_RenderHumanIncludesDeadBranchPrune asserts the human renderer
// surfaces the dead-branch prune surface when the meta keys are populated.
// A zero last_run_ts must NOT render the line (no clutter on fresh repos).
func TestDiagnose_RenderHumanIncludesDeadBranchPrune(t *testing.T) {
	roots := withIsolatedHome(t)
	ctx := context.Background()
	repo, _, d := makeDiagnoseRepo(t, roots)

	const wantTS int64 = 1_700_000_000
	const wantCount = 5
	wantRefs := []string{"refs/heads/old-feature"}
	refsJSON, err := json.Marshal(wantRefs)
	if err != nil {
		t.Fatalf("marshal refs: %v", err)
	}
	if err := state.MetaSet(ctx, d, daemon.MetaKeyDeadBranchPruneLastRunTS, strconv.FormatInt(wantTS, 10)); err != nil {
		t.Fatalf("seed last_run_ts: %v", err)
	}
	if err := state.MetaSet(ctx, d, daemon.MetaKeyDeadBranchPruneLastCount, strconv.Itoa(wantCount)); err != nil {
		t.Fatalf("seed last_count: %v", err)
	}
	if err := state.MetaSet(ctx, d, daemon.MetaKeyDeadBranchPruneLastRefs, string(refsJSON)); err != nil {
		t.Fatalf("seed last_refs: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}

	var out bytes.Buffer
	if err := runDiagnose(ctx, &out, repo, false); err != nil {
		t.Fatalf("runDiagnose human: %v", err)
	}
	want := "Dead-branch prune: 5 row(s) pruned at " + time.Unix(wantTS, 0).Format(time.RFC3339)
	if !strings.Contains(out.String(), want) {
		t.Fatalf("human renderer missing dead-branch prune line %q in:\n%s", want, out.String())
	}
	if !strings.Contains(out.String(), "refs/heads/old-feature") {
		t.Fatalf("human renderer missing pruned ref name in:\n%s", out.String())
	}
}

// TestDiagnose_RenderHumanOmitsDeadBranchPruneWhenZero asserts the renderer
// suppresses the dead-branch prune line entirely when last_run_ts == 0
// (never-ran sentinel). Avoids cluttering output on fresh repos.
func TestDiagnose_RenderHumanOmitsDeadBranchPruneWhenZero(t *testing.T) {
	roots := withIsolatedHome(t)
	ctx := context.Background()
	repo, _, d := makeDiagnoseRepo(t, roots)
	if err := d.Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}

	var out bytes.Buffer
	if err := runDiagnose(ctx, &out, repo, false); err != nil {
		t.Fatalf("runDiagnose human: %v", err)
	}
	if strings.Contains(out.String(), "Dead-branch prune") {
		t.Fatalf("human renderer included dead-branch prune line on never-ran repo:\n%s", out.String())
	}
}

// TestDiagnose_AutoResolvableBlocked seeds a blocked_conflict row whose
// after_oid already matches the HEAD blob (modify op, before_state_mismatch
// class). Expects auto_resolvable_blocked_count == 1 and the "Daemon will
// auto-resolve" remediation hint.
func TestDiagnose_AutoResolvableBlocked(t *testing.T) {
	roots := withIsolatedHome(t)
	ctx := context.Background()
	repo, _, d := makeDiagnoseRepo(t, roots)

	// Create a real commit so HEAD resolves and has a blob we can reference.
	headSHA := seedDiagnoseCommit(t, repo)

	// Get the blob OID of the committed file from HEAD.
	blobOID, err := git.RevParse(ctx, repo, "HEAD:seed.txt")
	if err != nil {
		t.Fatalf("rev-parse HEAD:seed.txt: %v", err)
	}

	if err := state.SaveDaemonState(ctx, d, state.DaemonState{
		PID: 7, Mode: "running", BranchRef: sql.NullString{String: "refs/heads/main", Valid: true},
		BranchGeneration: sql.NullInt64{Int64: 1, Valid: true},
	}); err != nil {
		t.Fatalf("save daemon_state: %v", err)
	}

	// Append a blocked_conflict event with modify op + after_oid = blob at HEAD.
	// base_head = headSHA so ancestry check passes (headSHA == HEAD => ancestor trivially).
	seq, err := state.AppendCaptureEvent(ctx, d, state.CaptureEvent{
		BranchRef: "refs/heads/main", BranchGeneration: 1,
		BaseHead: headSHA, Operation: "modify", Path: "seed.txt",
		Fidelity: "exact", CapturedTS: nowFloat(),
	}, []state.CaptureOp{{
		Op: "modify", Path: "seed.txt", Fidelity: "exact",
		AfterOID: sql.NullString{String: blobOID, Valid: true},
	}})
	if err != nil {
		t.Fatalf("append event: %v", err)
	}
	// Mark blocked with a before_state_mismatch-class error.
	if err := state.MarkEventBlocked(ctx, d, seq,
		"before-state mismatch: expected abc actual def", nowFloat(),
		sql.NullString{String: "refs/heads/main", Valid: true},
		sql.NullInt64{Int64: 1, Valid: true},
		sql.NullString{String: headSHA, Valid: true},
	); err != nil {
		t.Fatalf("mark blocked: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}

	var out bytes.Buffer
	if err := runDiagnose(ctx, &out, repo, true); err != nil {
		t.Fatalf("runDiagnose: %v", err)
	}
	var rep diagnoseReport
	if err := json.Unmarshal(out.Bytes(), &rep); err != nil {
		t.Fatalf("unmarshal diagnose: %v\n%s", err, out.String())
	}
	if rep.AutoResolvableBlockedCount != 1 {
		t.Fatalf("AutoResolvableBlockedCount=%d, want 1", rep.AutoResolvableBlockedCount)
	}
	var sawHint bool
	for _, r := range rep.Remediation {
		if strings.Contains(r, "Daemon will auto-resolve 1 blocked row") {
			sawHint = true
			break
		}
	}
	if !sawHint {
		t.Fatalf("remediation lacks auto-resolve hint: %v", rep.Remediation)
	}
}

// TestDiagnose_BarrierWithSuccessorsNoMatch seeds a blocked_conflict row with
// a pending successor but where HEAD does NOT match the after_oid. Expects
// barrier_with_successors_count > 0 and the acd fix --force --dry-run hint.
func TestDiagnose_BarrierWithSuccessorsNoMatch(t *testing.T) {
	roots := withIsolatedHome(t)
	ctx := context.Background()
	repo, _, d := makeDiagnoseRepo(t, roots)

	if err := state.SaveDaemonState(ctx, d, state.DaemonState{
		PID: 7, Mode: "running", BranchRef: sql.NullString{String: "refs/heads/main", Valid: true},
		BranchGeneration: sql.NullInt64{Int64: 1, Valid: true},
	}); err != nil {
		t.Fatalf("save daemon_state: %v", err)
	}

	// Blocked row with a non-matching after_oid (no real commit, HEAD is empty).
	seq, err := state.AppendCaptureEvent(ctx, d, state.CaptureEvent{
		BranchRef: "refs/heads/main", BranchGeneration: 1,
		BaseHead: "deadbeef", Operation: "modify", Path: "barrier.go",
		Fidelity: "exact", CapturedTS: nowFloat(),
	}, []state.CaptureOp{{
		Op: "modify", Path: "barrier.go", Fidelity: "exact",
		AfterOID: sql.NullString{String: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", Valid: true},
	}})
	if err != nil {
		t.Fatalf("append blocked event: %v", err)
	}
	if err := state.MarkEventBlocked(ctx, d, seq,
		"before-state mismatch: missing-in-index", nowFloat(),
		sql.NullString{String: "refs/heads/main", Valid: true},
		sql.NullInt64{Int64: 1, Valid: true},
		sql.NullString{String: "deadbeef", Valid: true},
	); err != nil {
		t.Fatalf("mark blocked: %v", err)
	}

	// Pending successor at a higher seq in the same (branch_ref, generation).
	if _, err := state.AppendCaptureEvent(ctx, d, state.CaptureEvent{
		BranchRef: "refs/heads/main", BranchGeneration: 1,
		BaseHead: "deadbeef", Operation: "modify", Path: "successor.go",
		Fidelity: "exact", CapturedTS: nowFloat(),
	}, []state.CaptureOp{{Op: "modify", Path: "successor.go", Fidelity: "exact"}}); err != nil {
		t.Fatalf("append successor event: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}

	var out bytes.Buffer
	if err := runDiagnose(ctx, &out, repo, true); err != nil {
		t.Fatalf("runDiagnose: %v", err)
	}
	var rep diagnoseReport
	if err := json.Unmarshal(out.Bytes(), &rep); err != nil {
		t.Fatalf("unmarshal diagnose: %v\n%s", err, out.String())
	}
	if rep.BarrierWithSuccessorsCount == 0 {
		t.Fatalf("BarrierWithSuccessorsCount=0, want > 0")
	}
	// AutoResolvableBlockedCount must be 0 (HEAD is empty, after_oid won't match).
	// So the condition BarrierWithSuccessorsCount > AutoResolvableBlockedCount fires.
	var sawHint bool
	for _, r := range rep.Remediation {
		if strings.Contains(r, "acd fix --force --dry-run") {
			sawHint = true
			break
		}
	}
	if !sawHint {
		t.Fatalf("remediation lacks force dry-run hint: %v", rep.Remediation)
	}
}

// TestDiagnose_DeadBranchPrune_MalformedJSON asserts that a corrupt
// last_refs blob does NOT panic or abort diagnose: ts/count still parse,
// refs becomes nil, and the report is otherwise well-formed.
func TestDiagnose_DeadBranchPrune_MalformedJSON(t *testing.T) {
	roots := withIsolatedHome(t)
	ctx := context.Background()
	repo, _, d := makeDiagnoseRepo(t, roots)

	if err := state.MetaSet(ctx, d, daemon.MetaKeyDeadBranchPruneLastRunTS, "1700000000"); err != nil {
		t.Fatalf("seed last_run_ts: %v", err)
	}
	if err := state.MetaSet(ctx, d, daemon.MetaKeyDeadBranchPruneLastCount, "3"); err != nil {
		t.Fatalf("seed last_count: %v", err)
	}
	// "{" is invalid JSON — Unmarshal returns an error and the helper must
	// fall back to nil refs without aborting diagnose.
	if err := state.MetaSet(ctx, d, daemon.MetaKeyDeadBranchPruneLastRefs, "{"); err != nil {
		t.Fatalf("seed last_refs: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}

	var out bytes.Buffer
	if err := runDiagnose(ctx, &out, repo, true); err != nil {
		t.Fatalf("runDiagnose: %v", err)
	}
	var rep diagnoseReport
	if err := json.Unmarshal(out.Bytes(), &rep); err != nil {
		t.Fatalf("unmarshal diagnose: %v\n%s", err, out.String())
	}
	if rep.DeadBranchPruneLastRunTS != 1_700_000_000 {
		t.Fatalf("DeadBranchPruneLastRunTS=%d want 1700000000", rep.DeadBranchPruneLastRunTS)
	}
	if rep.DeadBranchPruneLastCount != 3 {
		t.Fatalf("DeadBranchPruneLastCount=%d want 3", rep.DeadBranchPruneLastCount)
	}
	if rep.DeadBranchPruneLastRefs != nil {
		t.Fatalf("DeadBranchPruneLastRefs=%v want nil after malformed JSON", rep.DeadBranchPruneLastRefs)
	}
}
