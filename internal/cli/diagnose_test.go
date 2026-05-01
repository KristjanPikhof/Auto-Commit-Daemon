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
