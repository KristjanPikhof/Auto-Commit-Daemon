package daemon

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

func TestReplayErrorObservabilityCountsAndClearsRecovery(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	db := openTestDB(t)
	now := time.Unix(100, 0)
	raw := errors.New("candidate failed\nsecret sk-1234567890abcdef")

	first, count, err := recordReplayErrorObservability(ctx, db, raw, now)
	if err != nil {
		t.Fatal(err)
	}
	if count != 1 || strings.Contains(first, "\n") ||
		strings.Contains(first, "sk-1234567890abcdef") {
		t.Fatalf("first error=%q count=%d", first, count)
	}
	second, count, err := recordReplayErrorObservability(
		ctx, db, raw, now.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if second != first || count != 2 {
		t.Fatalf("second error=%q count=%d want %q/2", second, count, first)
	}

	previous, repeats, err := clearReplayErrorObservability(ctx, db)
	if err != nil {
		t.Fatal(err)
	}
	if previous != first || repeats != 2 {
		t.Fatalf("recovery previous=%q repeats=%d", previous, repeats)
	}
	if value, ok, err := state.MetaGet(ctx, db, metaLastReplayError); err != nil || !ok || value != "" {
		t.Fatalf("cleared last error=%q ok=%v err=%v", value, ok, err)
	}
	if value, ok, err := state.MetaGet(ctx, db, metaReplayErrorRepeat); err != nil || !ok || value != "0" {
		t.Fatalf("cleared repeat count=%q ok=%v err=%v", value, ok, err)
	}
	if value, ok, err := state.MetaGet(ctx, db, metaReplayErrorLastSeenTS); err != nil || !ok || value != "" {
		t.Fatalf("cleared last-seen timestamp=%q ok=%v err=%v",
			value, ok, err)
	}
}

func TestReplayErrorObservabilityMarksCompletedTransitionProofAttention(
	t *testing.T,
) {
	t.Parallel()
	ctx := context.Background()
	db := openTestDB(t)
	replayErr := fmt.Errorf("canonicalize repaired event base: %w",
		state.ErrCompletedBranchTransitionProof)

	if _, _, err := recordReplayErrorObservability(
		ctx, db, replayErr, time.Unix(100, 0)); err != nil {
		t.Fatal(err)
	}
	attention, ok, err := state.MetaGet(
		ctx, db, MetaKeyBranchTransitionNeedsAttention)
	if err != nil || !ok || !strings.Contains(attention, "canonicalize") {
		t.Fatalf("transition attention=(%q,%t,%v)", attention, ok, err)
	}
}

func TestReplayErrorLogLimiterEmitsFirstPeriodicAndRecovery(t *testing.T) {
	t.Parallel()
	var limiter replayErrorLogLimiter
	start := time.Unix(100, 0)
	if emit, suppressed := limiter.observe("same", start); !emit || suppressed != 0 {
		t.Fatalf("first emit=%v suppressed=%d", emit, suppressed)
	}
	if emit, _ := limiter.observe("same", start.Add(time.Second)); emit {
		t.Fatal("identical error was not rate limited")
	}
	if emit, suppressed := limiter.observe(
		"same", start.Add(replayErrorLogInterval),
	); !emit || suppressed != 1 {
		t.Fatalf("periodic emit=%v suppressed=%d", emit, suppressed)
	}
	if emit, _ := limiter.observe("changed", start.Add(replayErrorLogInterval+time.Second)); !emit {
		t.Fatal("changed error did not emit immediately")
	}
	if value, _ := limiter.recover(); value != "changed" {
		t.Fatalf("recovery value=%q", value)
	}
	if emit, _ := limiter.observe("same", start.Add(2*replayErrorLogInterval)); !emit {
		t.Fatal("first occurrence after recovery did not emit")
	}
}

func replayErrorMetaTestKeys() []string {
	return []string{
		"last_replay_conflict",
		"last_replay_conflict_legacy",
		metaLastReplayError,
		metaReplayErrorRepeat,
		metaReplayErrorLastSeenTS,
	}
}
