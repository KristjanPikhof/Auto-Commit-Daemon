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

func TestRuntimeVerificationCacheSurvivesRestartAndRequiresExactInputs(t *testing.T) {
	f := newCaptureFixture(t)
	base := context.Background()
	if err := os.WriteFile(filepath.Join(f.dir, "feature.txt"), []byte("feature\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	f.firstCapture(t)
	events, err := state.PendingEvents(base, f.db, 0)
	if err != nil || len(events) == 0 {
		t.Fatalf("events=%v err=%v", events, err)
	}
	var event state.CaptureEvent
	for _, candidate := range events {
		if candidate.Path == "feature.txt" {
			event = candidate
		}
	}
	ops, err := state.LoadCaptureOps(base, f.db, event.Seq)
	if err != nil {
		t.Fatal(err)
	}
	captures := []IntentCandidateCapture{{Event: event, Ops: ops}}
	assignment := ai.IntentCandidateAssignment{CandidateID: "feature", SelectedSeqs: []int64{event.Seq}}
	countPath := filepath.Join(t.TempDir(), "runs")
	command, err := verification.NewApprovedCommand(f.dir, "cache-proof", verification.ModeFast,
		"printf 'run\\n' >> '"+countPath+"'; test -f feature.txt", 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	cachePath := filepath.Join(t.TempDir(), "cache.db")
	run := func(revision int64, corrupt bool) IntentCandidateVerification {
		t.Helper()
		db, err := state.Open(base, cachePath)
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		if corrupt {
			if err := state.MetaSet(base, db, runtimeVerificationCacheMeta, "invalid json"); err != nil {
				t.Fatal(err)
			}
		}
		ctx, cancel := context.WithCancel(base)
		defer cancel()
		ctx = context.WithValue(ctx, publicationEvaluationKey{}, &publicationEvaluation{db: db, cancel: cancel,
			identity: func(context.Context) (string, error) { return "frozen", nil }, protect: func(context.Context) error { return nil }})
		result, err := runtimeIntentCandidateVerifier(f.dir, f.gitDir, f.cctx.BaseHead, revision, command)(ctx, assignment, captures)
		if err != nil || result.Status != string(verification.StatusPassed) {
			t.Fatalf("result=%+v err=%v", result, err)
		}
		return result
	}
	first := run(7, false)
	second := run(7, false)
	if first.CheckedTS != second.CheckedTS {
		t.Fatal("restart repeated an unchanged successful check")
	}
	assertRuns := func(want string) {
		t.Helper()
		got, err := os.ReadFile(countPath)
		if err != nil || string(got) != want {
			t.Fatalf("runs=%q err=%v want=%q", got, err, want)
		}
	}
	assertRuns("run\n")
	run(8, false)
	assertRuns("run\nrun\n")
	run(8, true)
	assertRuns("run\nrun\nrun\n")
}
