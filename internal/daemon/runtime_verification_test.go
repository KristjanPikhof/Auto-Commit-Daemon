package daemon

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/ai"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/verification"
)

func TestRuntimeIntentVerifierAdvancesTopologicalCandidateParent(t *testing.T) {
	f := newCaptureFixture(t)
	ctx := context.Background()
	startHead := f.cctx.BaseHead

	if err := os.WriteFile(filepath.Join(f.dir, "a.txt"),
		[]byte("a\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := Capture(ctx, f.dir, f.db, f.cctx, CaptureOpts{
		IgnoreChecker: f.ig, SensitiveMatcher: f.matcher,
	}); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(f.dir, "b.txt"),
		[]byte("b\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := Capture(ctx, f.dir, f.db, f.cctx, CaptureOpts{
		IgnoreChecker: f.ig, SensitiveMatcher: f.matcher,
	}); err != nil {
		t.Fatal(err)
	}
	pending, err := state.PendingEvents(ctx, f.db, 0)
	if err != nil {
		t.Fatal(err)
	}
	captures := make(map[string]IntentCandidateCapture)
	for _, event := range pending {
		ops, loadErr := state.LoadCaptureOps(ctx, f.db, event.Seq)
		if loadErr != nil {
			t.Fatal(loadErr)
		}
		captures[event.Path] = IntentCandidateCapture{Event: event, Ops: ops}
	}
	if _, ok := captures["a.txt"]; !ok {
		t.Fatalf("missing a.txt capture: %v", captures)
	}
	if _, ok := captures["b.txt"]; !ok {
		t.Fatalf("captures=%v", captures)
	}
	command, err := verification.NewApprovedCommand(
		f.dir, "topological-verification", verification.ModeFast,
		`[ ! -f b.txt ] || [ -f a.txt ]`, 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	verify := runtimeIntentCandidateVerifier(
		f.dir, f.gitDir, startHead, 7, command)
	first := ai.IntentCandidateAssignment{
		CandidateID:  "candidate-a",
		SelectedSeqs: []int64{captures["a.txt"].Event.Seq},
	}
	if result, err := verify(
		ctx, first, []IntentCandidateCapture{captures["a.txt"]},
	); err != nil || result.Status != string(verification.StatusPassed) {
		t.Fatalf("first verification=%+v err=%v", result, err)
	}
	second := ai.IntentCandidateAssignment{
		CandidateID:         "candidate-b",
		SelectedSeqs:        []int64{captures["b.txt"].Event.Seq},
		DependsOnCandidates: []string{"candidate-a"},
	}
	if result, err := verify(
		ctx, second, []IntentCandidateCapture{captures["b.txt"]},
	); err != nil || result.Status != string(verification.StatusPassed) {
		t.Fatalf("dependent verification=%+v err=%v", result, err)
	}
	head, err := git.RevParse(ctx, f.dir, "HEAD")
	if err != nil {
		t.Fatal(err)
	}
	if head != startHead {
		t.Fatalf("verification changed live HEAD: %s -> %s", startHead, head)
	}
}

func TestRuntimeIntentRepairVerifierChecksExactCommitWithoutRefChanges(
	t *testing.T,
) {
	f := newCaptureFixture(t)
	ctx := context.Background()
	startHead := f.cctx.BaseHead
	command, err := verification.NewApprovedCommand(
		f.dir,
		"repair-verification",
		verification.ModeFast,
		`test "$(git rev-parse HEAD)" = "`+startHead+`"`,
		10*time.Second,
	)
	if err != nil {
		t.Fatal(err)
	}
	verify := runtimeIntentRepairCommitVerifier(
		f.dir, 11, command)
	if err := verify(ctx, startHead, 0); err != nil {
		t.Fatalf("verify exact repair commit: %v", err)
	}
	head, err := git.RevParse(ctx, f.dir, "HEAD")
	if err != nil || head != startHead {
		t.Fatalf("HEAD=%s err=%v want %s", head, err, startHead)
	}
}

func TestRuntimeIntentRepairVerifierKeepsCancellationRetryable(
	t *testing.T,
) {
	f := newCaptureFixture(t)
	command, err := verification.NewApprovedCommand(
		f.dir,
		"repair-cancellation",
		verification.ModeFast,
		"true",
		10*time.Second,
	)
	if err != nil {
		t.Fatal(err)
	}
	verify := runtimeIntentRepairCommitVerifier(f.dir, 12, command)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err = verify(ctx, f.cctx.BaseHead, 0)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error=%v want context cancellation", err)
	}
	if errors.Is(err, git.ErrIntentRepairVerification) {
		t.Fatalf("cancellation became durable verification failure: %v", err)
	}
}
