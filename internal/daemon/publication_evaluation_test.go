package daemon

import (
	"context"
	"errors"
	"fmt"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/ai"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/config"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/verification"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

func TestRunCheckpointDuringBlockedMessage(t *testing.T) {
	f := newDaemonFixture(t)
	registerLiveClient(t, f.db)
	entered, release := make(chan struct{}), make(chan struct{})
	wake := make(chan struct{}, 1)
	var once sync.Once
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	gate := &sync.RWMutex{}
	go func() {
		done <- Run(ctx, Options{
			RepoPath: f.dir, GitDir: f.gitDir, DB: f.db,
			Scheduler: fastScheduler(), BootGrace: 30 * time.Second,
			WakeCh: wake, ShutdownCh: make(chan struct{}), SkipSignals: true,
			OperationGate: gate,
			MessageFn: func(callCtx context.Context, ec EventContext) (string, error) {
				once.Do(func() { close(entered) })
				select {
				case <-release:
					return DeterministicMessage(callCtx, ec)
				case <-callCtx.Done():
					return "", callCtx.Err()
				}
			},
		})
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("worker did not join evaluation")
		}
	})
	waitForDaemonMode(t, f.db, "running", 3*time.Second)
	if err := os.WriteFile(filepath.Join(f.dir, "first.txt"), []byte("first\n"), 0600); err != nil {
		t.Fatal(err)
	}
	wake <- struct{}{}
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("message provider was not called")
	}
	head, err := git.RevParse(ctx, f.dir, "HEAD")
	if err != nil {
		t.Fatal(err)
	}
	// A restore/setup writer can acquire its gate while the provider waits.
	locked := make(chan struct{})
	go func() { gate.Lock(); gate.Unlock(); close(locked) }()
	select {
	case <-locked:
	case <-time.After(time.Second):
		t.Fatal("provider holds operation gate")
	}
	if err := os.WriteFile(filepath.Join(f.dir, "later.txt"), []byte("later\n"), 0600); err != nil {
		t.Fatal(err)
	}
	wake <- struct{}{}
	var checkpointID string
	waitFor(t, 3*time.Second, "later edit checkpoint while provider blocked", func() bool {
		id, ok, err := state.MetaGet(ctx, f.db, MetaKeyProtectionCheckpointID)
		if err != nil || !ok {
			return false
		}
		checkpoint, err := state.ResolveCheckpoint(ctx, f.db.Path(), id)
		if err != nil {
			return false
		}
		_, err = git.Run(ctx, git.RunOpts{Dir: f.dir}, "cat-file", "-e", checkpoint.CommitOID+":later.txt")
		if err == nil {
			checkpointID = id
		}
		return err == nil
	})
	if checkpointID == "" {
		t.Fatal("later checkpoint missing")
	}
	pendingClassification, _, err := state.MetaGet(ctx, f.db, MetaKeyProtectionClassificationPending)
	if err != nil || pendingClassification != "true" {
		t.Fatalf("protected-only edit must wait for classification: %q %v", pendingClassification, err)
	}
	unchanged, err := git.RevParse(ctx, f.dir, "HEAD")
	if err != nil || unchanged != head {
		t.Fatalf("publication advanced before provider result: %s %v", unchanged, err)
	}
	// Protected later bytes must not silently enter the suspended target.
	var laterEvents int
	if err := f.db.ReadSQL().QueryRowContext(ctx, "SELECT COUNT(*) FROM capture_events WHERE path='later.txt'").Scan(&laterEvents); err != nil || laterEvents != 0 {
		t.Fatalf("later edit entered frozen replay: %d %v", laterEvents, err)
	}
	close(release)
	waitForCommit(t, f.dir, head, 5*time.Second)
}

func TestPublicationEvaluationRejectsStaleResultAndJoins(t *testing.T) {
	for _, scenario := range []string{"stale", "shutdown"} {
		t.Run(scenario, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			wake, shutdown := make(chan struct{}, 1), make(chan struct{})
			started, finished := make(chan struct{}), make(chan struct{})
			gate := &sync.RWMutex{}
			stale := false
			evaluation := &publicationEvaluation{gate: gate, cancel: cancel, wake: wake, shutdown: shutdown,
				identity: func(context.Context) (string, error) {
					if stale {
						return "new", nil
					}
					return "original", nil
				},
				protect: func(context.Context) error { return nil }, interval: time.Hour}
			result := make(chan error, 1)
			go func() {
				gate.RLock()
				defer gate.RUnlock()
				_, err := evaluatePublication(context.WithValue(ctx, publicationEvaluationKey{}, evaluation), func(jobCtx context.Context) (string, error) {
					close(started)
					<-jobCtx.Done()
					close(finished)
					return "must not apply", nil
				})
				result <- err
			}()
			<-started
			if scenario == "shutdown" {
				close(shutdown)
			} else {
				gate.Lock()
				stale = true
				gate.Unlock()
				wake <- struct{}{}
			}
			select {
			case err := <-result:
				if scenario == "stale" && !errors.Is(err, errPublicationEvaluationStale) {
					t.Fatalf("expected stale rejection: %v", err)
				}
				if scenario == "shutdown" && !errors.Is(err, context.Canceled) {
					t.Fatalf("expected cancellation: %v", err)
				}
			case <-time.After(time.Second):
				t.Fatal("evaluation did not cancel")
			}
			select {
			case <-finished:
			default:
				t.Fatal("evaluation returned without joining")
			}
		})
	}
}

func TestRunCheckpointDuringProjectVerification(t *testing.T) {
	f := newDaemonFixture(t)
	registerLiveClient(t, f.db)
	control := t.TempDir()
	marker, fifo := filepath.Join(control, "started"), filepath.Join(control, "release")
	if err := syscall.Mkfifo(fifo, 0600); err != nil {
		t.Fatal(err)
	}
	command, err := verification.NewApprovedCommand(f.dir, "blocked-check", verification.ModeFast,
		"touch '"+marker+"'; read value < '"+fifo+"'", 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	wake := make(chan struct{}, 1)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- Run(ctx, Options{
			RepoPath: f.dir, GitDir: f.gitDir, DB: f.db, Scheduler: fastScheduler(), BootGrace: 30 * time.Second,
			WakeCh: wake, ShutdownCh: make(chan struct{}), SkipSignals: true,
			replay: func(passCtx context.Context, repo string, db *state.DB, cctx CaptureContext, opts ReplayOpts) (ReplaySummary, error) {
				opts.CommitStrategy = ai.CommitStrategyIntent
				opts.IntentPreset = config.PresetBalanced
				opts.IntentPlanner = ai.DeterministicProvider{}
				opts.IntentHealth = nil
				opts.IntentSettleWindow = -1
				opts.IntentBypassBatchWait = true
				opts.IntentVerificationMode = "fast"
				opts.IntentCandidateVerify = runtimeIntentCandidateVerifier(repo, f.gitDir, cctx.BaseHead, 1, command)
				return Replay(passCtx, repo, db, cctx, opts)
			},
		})
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("worker did not join project verifier")
		}
	})
	waitForDaemonMode(t, f.db, "running", 3*time.Second)
	if err := os.WriteFile(filepath.Join(f.dir, "verified.txt"), []byte("verify\n"), 0600); err != nil {
		t.Fatal(err)
	}
	wake <- struct{}{}
	waitFor(t, 5*time.Second, "project command started", func() bool { _, err := os.Stat(marker); return err == nil })
	if err := os.WriteFile(filepath.Join(f.dir, "during-check.txt"), []byte("protected during test\n"), 0600); err != nil {
		t.Fatal(err)
	}
	wake <- struct{}{}
	waitFor(t, 3*time.Second, "completed checkpoint during project check", func() bool {
		id, ok, err := state.MetaGet(ctx, f.db, MetaKeyProtectionCheckpointID)
		if err != nil || !ok {
			return false
		}
		checkpoint, err := state.ResolveCheckpoint(ctx, f.db.Path(), id)
		if err != nil {
			return false
		}
		_, err = git.Run(ctx, git.RunOpts{Dir: f.dir}, "cat-file", "-e", checkpoint.CommitOID+":during-check.txt")
		return err == nil
	})
	// Cancel while the actual subprocess is blocked; its isolated worktree and
	// runtime lease must remain owned until the process has exited.
	cancel()
}

func TestEventPublicationWaitsForSelectedProviderAndRecovers(t *testing.T) {
	f := newCaptureFixture(t)
	ctx := context.Background()
	if _, err := BootstrapShadow(ctx, f.dir, f.db, f.cctx); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(f.dir, "semantic.txt"), []byte("preserved\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := Capture(ctx, f.dir, f.db, f.cctx, CaptureOpts{IgnoreChecker: f.ig, SensitiveMatcher: f.matcher}); err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	health := NewIntentPlannerHealth(ctx, f.db, IntentPlannerHealthOptions{Provider: IntentPlannerProviderIdentity{Provider: "test-ai"}, Now: func() time.Time { return now }})
	calls := 0
	unavailable := true
	opts := ReplayOpts{GitDir: f.gitDir, CommitStrategy: ai.CommitStrategyEvent, IntentHealth: health, MessageFn: func(context.Context, EventContext) (string, error) {
		calls++
		if unavailable {
			return "", &ai.ProviderHTTPError{StatusCode: 503, Detail: "temporarily unavailable"}
		}
		return "Preserve semantic behavior\n\n- Keep the requested behavior consistent", nil
	}}
	first, err := Replay(ctx, f.dir, f.db, f.cctx, opts)
	if !isIntentPlannerCircuitWait(err) || first.Failed != 0 || first.Published != 0 || first.Disposition != ReplayDispositionTransientWait {
		t.Fatalf("outage=%+v err=%v", first, err)
	}
	pending, err := state.PendingEvents(ctx, f.db, 0)
	if err != nil || len(pending) != 1 {
		t.Fatalf("outage lost pending capture: %v %v", pending, err)
	}
	_, _ = Replay(ctx, f.dir, f.db, f.cctx, opts)
	if calls != 1 {
		t.Fatal("event retry bypassed provider cooldown")
	}
	unavailable = false
	now = now.Add(31 * time.Second)
	recovered, err := Replay(ctx, f.dir, f.db, f.cctx, opts)
	if err != nil || recovered.Published != 1 || recovered.Failed != 0 {
		t.Fatalf("recovery=%+v err=%v", recovered, err)
	}
	out, err := git.Run(ctx, git.RunOpts{Dir: f.dir}, "log", "-1", "--format=%s")
	if err != nil || strings.TrimSpace(string(out)) != "Preserve semantic behavior" {
		t.Fatalf("message=%s err=%v", out, err)
	}
}

func TestPublicationProviderConfigurationRequiresAction(t *testing.T) {
	for _, status := range []int{401, 403, 404} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			ctx := context.Background()
			db := openIntentCandidateTestDB(t)
			failure := &ai.ProviderHTTPError{StatusCode: status, Detail: "check provider settings"}
			health := NewIntentPlannerHealth(ctx, db, IntentPlannerHealthOptions{Provider: IntentPlannerProviderIdentity{Provider: "configured-ai"}})
			_, err := generatePublicationMessage(ctx, func(context.Context, EventContext) (string, error) { return "", failure }, EventContext{}, health)
			if !ai.ProviderNeedsConfiguration(err) || isIntentPlannerCircuitWait(err) {
				t.Fatalf("configuration classified as temporary wait: %v", err)
			}
			summary := ReplaySummary{}
			classifyReplayDisposition(&summary, err)
			if summary.Disposition != ReplayDispositionNeedsAttention || summary.DispositionReason != "provider_configuration_required" {
				t.Fatalf("configuration outcome=%+v", summary)
			}
			capture := appendIntentCandidateCapture(t, db, "feature.go", "create", "", "feature")
			result, err := EvaluateIntentCandidates(ctx, db, IntentCandidateEvaluation{BranchRef: "refs/heads/main", BranchGeneration: 1, Captures: []IntentCandidateCapture{capture}, Planner: &intentCandidatePlannerStub{err: failure}, Health: health, Preset: config.PresetBalanced})
			if !ai.ProviderNeedsConfiguration(err) || len(result.Decisions) != 0 || result.PlanAttempt != 0 {
				t.Fatalf("configuration planner=%+v err=%v", result, err)
			}
		})
	}
}
