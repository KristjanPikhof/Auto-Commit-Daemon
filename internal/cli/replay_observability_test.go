package cli

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

func TestReplayObservabilityRetryMatrix(t *testing.T) {
	for _, tc := range []struct {
		name      string
		rawCount  string
		wantCount int
	}{
		{name: "first", rawCount: "1", wantCount: 1},
		{name: "second", rawCount: "2", wantCount: 2},
		{name: "third", rawCount: "3", wantCount: 3},
		{name: "capped", rawCount: "2000000", wantCount: replayRepeatCountCap},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, _, d := makeRepoStateDB(t)
			ctx := context.Background()
			if err := state.MetaSetMany(ctx, d, map[string]string{
				"last_replay_error":         "retryable capture 42 failed",
				"replay.error_repeat_count": tc.rawCount,
				"replay.error_last_seen_ts": "1786464200",
			}); err != nil {
				t.Fatal(err)
			}

			report, err := loadReplayObservabilityReport(ctx, d.SQL())
			if err != nil {
				t.Fatal(err)
			}
			if report.State != "degraded" ||
				report.ErrorRepeatCount != tc.wantCount ||
				report.ErrorLastSeenTS != 1786464200 ||
				report.BlockedSeq != 42 ||
				report.LastError != "retryable capture 42 failed" {
				t.Fatalf("retry projection=%+v", report)
			}
		})
	}
}

func TestReplayObservabilityDurableAttentionMatrix(t *testing.T) {
	for _, tc := range []struct {
		name string
		seed func(*testing.T, *state.DB) int64
	}{
		{
			name: "explicit runtime or invariant block",
			seed: func(t *testing.T, d *state.DB) int64 {
				t.Helper()
				if err := state.MetaSet(context.Background(), d,
					"intent.v2.needs_attention",
					"runtime verification is unavailable"); err != nil {
					t.Fatal(err)
				}
				return 0
			},
		},
		{
			name: "candidate verification attention",
			seed: func(t *testing.T, d *state.DB) int64 {
				t.Helper()
				ctx := context.Background()
				seq := appendReplayHealthEvent(t, d, "candidate.go")
				seedCurrentReplayPair(t, d, "refs/heads/main", 1)
				if err := state.SaveDaemonState(ctx, d, state.DaemonState{
					PID: 1234, Mode: "running",
				}); err != nil {
					t.Fatal(err)
				}
				assertDaemonReplayAnchorNull(t, d)
				if err := state.SaveIntentCandidate(ctx, d, state.IntentCandidate{
					ID: "candidate-attention", BranchRef: "refs/heads/main",
					BranchGeneration: 1, Status: state.IntentCandidateWaiting,
					Readiness: state.IntentReadinessWait,
					VerificationStatus: sql.NullString{
						String: "failed", Valid: true,
					},
					Events: []state.IntentCandidateEvent{{
						EventSeq: seq, EventRole: "code",
					}},
				}); err != nil {
					t.Fatal(err)
				}
				return seq
			},
		},
		{
			name: "terminal publication barrier with production heartbeat shape",
			seed: func(t *testing.T, d *state.DB) int64 {
				t.Helper()
				ctx := context.Background()
				seq := appendReplayHealthEvent(t, d, "blocked.go")
				if err := state.MarkEventPublished(ctx, d, seq,
					state.EventStateFailed, sql.NullString{},
					sql.NullString{String: "invariant failed", Valid: true},
					sql.NullString{}, nowFloat()); err != nil {
					t.Fatal(err)
				}
				seedCurrentReplayPair(t, d, "refs/heads/main", 1)
				if err := state.SaveDaemonState(ctx, d, state.DaemonState{
					PID: 1234, Mode: "running",
				}); err != nil {
					t.Fatal(err)
				}
				assertDaemonReplayAnchorNull(t, d)
				return seq
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, _, d := makeRepoStateDB(t)
			wantSeq := tc.seed(t, d)
			report, err := loadReplayObservabilityReport(
				context.Background(), d.SQL())
			if err != nil {
				t.Fatal(err)
			}
			if report.State != "needs_attention" ||
				report.BlockedSeq != wantSeq {
				t.Fatalf("durable projection=%+v want_seq=%d", report, wantSeq)
			}
		})
	}
}

func TestCurrentReplayPairLegacyDaemonStateFallback(t *testing.T) {
	_, _, d := makeRepoStateDB(t)
	ctx := context.Background()
	if err := state.SaveDaemonState(ctx, d, state.DaemonState{
		PID:              1234,
		Mode:             "running",
		BranchRef:        sql.NullString{String: "refs/heads/legacy", Valid: true},
		BranchGeneration: sql.NullInt64{Int64: 7, Valid: true},
	}); err != nil {
		t.Fatal(err)
	}

	branchRef, generation, ok, err := currentReplayPair(ctx, d.SQL())
	if err != nil {
		t.Fatal(err)
	}
	if !ok || branchRef != "refs/heads/legacy" || generation != 7 {
		t.Fatalf("legacy pair=(%q,%d,%t), want refs/heads/legacy,7,true",
			branchRef, generation, ok)
	}
}

func TestCurrentReplayPairMetadataSnapshotWinsBranchSwitchMismatch(t *testing.T) {
	_, _, d := makeRepoStateDB(t)
	ctx := context.Background()
	if err := state.SaveDaemonState(ctx, d, state.DaemonState{
		PID:              1234,
		Mode:             "running",
		BranchRef:        sql.NullString{String: "refs/heads/old", Valid: true},
		BranchGeneration: sql.NullInt64{Int64: 1, Valid: true},
	}); err != nil {
		t.Fatal(err)
	}
	seedCurrentReplayPair(t, d, "refs/heads/new", 2)

	branchRef, generation, ok, err := currentReplayPair(ctx, d.SQL())
	if err != nil {
		t.Fatal(err)
	}
	if !ok || branchRef != "refs/heads/new" || generation != 2 {
		t.Fatalf("metadata pair=(%q,%d,%t), want refs/heads/new,2,true",
			branchRef, generation, ok)
	}
}

func TestCurrentReplayPairDetachedTokenSuppressesStaleLegacy(t *testing.T) {
	_, _, d := makeRepoStateDB(t)
	ctx := context.Background()
	seedLegacyReplayPair(t, d)
	if err := state.MetaSetMany(ctx, d, map[string]string{
		"branch_token":      "rev:" + strings.Repeat("b", 40),
		"branch.generation": "8",
	}); err != nil {
		t.Fatal(err)
	}

	branchRef, generation, ok, err := currentReplayPair(ctx, d.SQL())
	if err != nil {
		t.Fatal(err)
	}
	if ok || branchRef != "" || generation != 0 {
		t.Fatalf("detached pair=(%q,%d,%t), want no attached pair",
			branchRef, generation, ok)
	}
}

func TestCurrentReplayPairIncompleteGenerationSuppressesStaleLegacy(t *testing.T) {
	for _, tc := range []struct {
		name       string
		generation *string
	}{
		{name: "missing"},
		{name: "malformed", generation: stringPointer("not-a-generation")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, _, d := makeRepoStateDB(t)
			ctx := context.Background()
			seedLegacyReplayPair(t, d)
			if err := state.MetaSet(ctx, d, "branch_token",
				"rev:"+strings.Repeat("c", 40)+" refs/heads/current"); err != nil {
				t.Fatal(err)
			}
			if tc.generation != nil {
				if err := state.MetaSet(ctx, d, "branch.generation", *tc.generation); err != nil {
					t.Fatal(err)
				}
			}

			branchRef, generation, ok, err := currentReplayPair(ctx, d.SQL())
			if err != nil {
				t.Fatal(err)
			}
			if ok || branchRef != "" || generation != 0 {
				t.Fatalf("incomplete pair=(%q,%d,%t), want no attached pair",
					branchRef, generation, ok)
			}
		})
	}
}

func TestReplayObservabilityMalformedMetadataIsBounded(t *testing.T) {
	_, _, d := makeRepoStateDB(t)
	ctx := context.Background()
	raw := "\x1b[31mtoken=private-value\x00 capture 7 " + strings.Repeat("x", 2048)
	if err := state.MetaSetMany(ctx, d, map[string]string{
		"last_replay_error":         raw,
		"replay.error_repeat_count": "not-a-number",
		"replay.error_last_seen_ts": "not-a-time",
	}); err != nil {
		t.Fatal(err)
	}
	report, err := loadReplayObservabilityReport(ctx, d.SQL())
	if err != nil {
		t.Fatal(err)
	}
	if report.State != "degraded" || report.ErrorRepeatCount != 0 ||
		report.ErrorLastSeenTS != 0 || report.BlockedSeq != 7 ||
		len(report.LastError) > 1024 ||
		strings.Contains(report.LastError, "private-value") ||
		strings.Contains(report.LastError, "\x1b") ||
		strings.ContainsRune(report.LastError, '\x00') {
		t.Fatalf("malformed projection=%+v", report)
	}
}

func TestReplayObservabilityPreSchemaReadIsReadOnly(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "state.db")
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `
CREATE TABLE legacy_state(id INTEGER PRIMARY KEY);
PRAGMA user_version=4;`); err != nil {
		db.Close()
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	before, err := fileSHA256(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	conn, err := openStateDBReadOnly(ctx, dbPath)
	if err != nil {
		t.Fatal(err)
	}
	report, loadErr := loadReplayObservabilityReport(ctx, conn)
	closeErr := conn.Close()
	if loadErr != nil || closeErr != nil {
		t.Fatalf("load=%v close=%v", loadErr, closeErr)
	}
	after, err := fileSHA256(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	if report.State != "active" || report.LastError != "" || before != after {
		t.Fatalf("pre-schema report=%+v checksum=%s/%s", report, before, after)
	}
}

func appendReplayHealthEvent(t *testing.T, d *state.DB, path string) int64 {
	t.Helper()
	seq, err := state.AppendCaptureEvent(context.Background(), d,
		state.CaptureEvent{
			BranchRef: "refs/heads/main", BranchGeneration: 1,
			BaseHead: strings.Repeat("a", 40), Operation: "modify",
			Path: path, Fidelity: "exact", CapturedTS: nowFloat(),
		}, nil)
	if err != nil {
		t.Fatalf("append %s: %v", path, err)
	}
	return seq
}

func seedCurrentReplayPair(t *testing.T, d *state.DB, branchRef string, generation int64) {
	t.Helper()
	if err := state.MetaSetMany(context.Background(), d, map[string]string{
		"branch_token":      "rev:" + strings.Repeat("a", 40) + " " + branchRef,
		"branch.generation": fmt.Sprintf("%d", generation),
	}); err != nil {
		t.Fatalf("seed current replay pair: %v", err)
	}
}

func seedLegacyReplayPair(t *testing.T, d *state.DB) {
	t.Helper()
	if err := state.SaveDaemonState(context.Background(), d, state.DaemonState{
		PID:              1234,
		Mode:             "running",
		BranchRef:        sql.NullString{String: "refs/heads/stale", Valid: true},
		BranchGeneration: sql.NullInt64{Int64: 7, Valid: true},
	}); err != nil {
		t.Fatalf("seed legacy replay pair: %v", err)
	}
}

func stringPointer(value string) *string {
	return &value
}

func assertDaemonReplayAnchorNull(t *testing.T, d *state.DB) {
	t.Helper()
	var branchRef sql.NullString
	var generation sql.NullInt64
	if err := d.SQL().QueryRow(`
SELECT branch_ref, branch_generation FROM daemon_state WHERE id=1`,
	).Scan(&branchRef, &generation); err != nil {
		t.Fatalf("read production heartbeat shape: %v", err)
	}
	if branchRef.Valid || generation.Valid {
		t.Fatalf("heartbeat replay anchor=(%v,%v), want NULL,NULL",
			branchRef, generation)
	}
}

func TestReplayObservabilityHumanShowsRetryEvidence(t *testing.T) {
	report := replayObservabilityReport{
		State: "degraded", LastError: "retryable capture 9 failed",
		ErrorRepeatCount: 3, ErrorLastSeenTS: 1786464200, BlockedSeq: 9,
	}
	var out strings.Builder
	renderReplayObservabilityHuman(&out, report)
	for _, want := range []string{
		"Replay: degraded", "repeats=3", "last_seen=1786464200",
		"blocked_seq=9", fmt.Sprintf("Last replay error: %s", report.LastError),
	} {
		if !strings.Contains(out.String(), want) {
			t.Fatalf("human output missing %q: %s", want, out.String())
		}
	}
}
