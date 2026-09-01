package daemon

import (
	"context"
	"database/sql"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/ai"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/prompttrace"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

type intentHealthPromptRecorder struct {
	mu      sync.Mutex
	records []prompttrace.Record
}

type intentHealthRunProvider struct {
	mu       sync.Mutex
	plans    int
	requests []ai.IntentPlanRequest
}

func (p *intentHealthRunProvider) Name() string    { return "openai-compat" }
func (p *intentHealthRunProvider) NeedsDiff() bool { return true }

func (p *intentHealthRunProvider) Generate(context.Context, ai.CommitContext) (ai.Result, error) {
	return ai.Result{Subject: "Generate should not serve intent planning", Source: p.Name()}, nil
}

func (p *intentHealthRunProvider) PlanIntent(ctx context.Context, req ai.IntentPlanRequest) (ai.IntentPlan, error) {
	plan, err := (ai.DeterministicProvider{}).PlanIntent(ctx, req)
	if err != nil {
		return ai.IntentPlan{}, err
	}
	plan.Subject = "Reuse run-scoped planner"
	plan.GroupingReason = "run-scoped provider selected the pending capture"
	plan.Source = p.Name()
	p.mu.Lock()
	p.plans++
	p.requests = append(p.requests, req)
	p.mu.Unlock()
	return plan, nil
}

func (p *intentHealthRunProvider) snapshot() (int, []ai.IntentPlanRequest) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.plans, append([]ai.IntentPlanRequest(nil), p.requests...)
}

type intentHealthRunCloser struct{ calls atomic.Int32 }

func (c *intentHealthRunCloser) Close() error {
	c.calls.Add(1)
	return nil
}

func (r *intentHealthPromptRecorder) Record(record prompttrace.Record) {
	r.mu.Lock()
	r.records = append(r.records, record)
	r.mu.Unlock()
}

func (r *intentHealthPromptRecorder) Close() error    { return nil }
func (r *intentHealthPromptRecorder) Dropped() uint64 { return 0 }

func (r *intentHealthPromptRecorder) Records() []prompttrace.Record {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]prompttrace.Record(nil), r.records...)
}

func countIntentPlannerErrorDecisions(t *testing.T, ctx context.Context, db *state.DB, seqs []int64) int {
	t.Helper()
	total := 0
	for _, seq := range seqs {
		rows, err := state.DecisionsForEvent(ctx, db, seq, 100)
		if err != nil {
			t.Fatalf("DecisionsForEvent(%d): %v", seq, err)
		}
		for _, row := range rows {
			if row.Kind == state.DecisionKindIntentPlannerError {
				total++
			}
		}
	}
	return total
}

func TestReplay_IntentHealthSingletonOutageUsesCircuitFallback(t *testing.T) {
	runBoundedParallel(t)

	f := newCaptureFixture(t)
	ctx := context.Background()
	if _, err := BootstrapShadow(ctx, f.dir, f.db, f.cctx); err != nil {
		t.Fatalf("BootstrapShadow: %v", err)
	}
	captureOnePendingFile(t, ctx, f, "singleton-outage.txt", "offline\n")

	planner := &recordingIntentPlanner{name: "openai-compat", err: errors.New("remote unavailable")}
	health := NewIntentPlannerHealth(ctx, f.db, IntentPlannerHealthOptions{
		Provider: openAIIntentHealthIdentity("https://planner.example/v1"),
	})
	messageCalls := 0
	sum, err := Replay(ctx, f.dir, f.db, f.cctx, ReplayOpts{
		GitDir:                f.gitDir,
		MessageFn:             func(context.Context, EventContext) (string, error) { messageCalls++; return "must not run", nil },
		CommitStrategy:        ai.CommitStrategyIntent,
		IntentPlanner:         planner,
		IntentHealth:          health,
		IntentPlannerProvider: "openai-compat",
		IntentPlannerModel:    "planner-model",
		IntentWindow:          10,
		IntentMinPending:      1,
		IntentBypassBatchWait: true,
	})
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if sum.Published != 1 || planner.calls != 1 || messageCalls != 0 {
		t.Fatalf("summary=%+v planner_calls=%d message_calls=%d", sum, planner.calls, messageCalls)
	}
	if snap := health.Snapshot(); snap.State != IntentPlannerCircuitOpen || snap.LastFailureClass != IntentPlannerFailureTransport {
		t.Fatalf("health=%+v want transport-open", snap)
	} else if !sum.PlannerCircuitOpen ||
		sum.PlannerProviderFingerprint != snap.ProviderFingerprint ||
		sum.PlannerCircuitFailureCount != snap.ConsecutiveFailures ||
		sum.PlannerCircuitLastFailureTS != snap.LastFailureTS {
		t.Fatalf("summary health=%+v want post-replay snapshot=%+v", sum, snap)
	}
}

func TestReplay_IntentHealthProviderShapeFailureCountsAsValidation(t *testing.T) {
	runBoundedParallel(t)

	f := newCaptureFixture(t)
	ctx := context.Background()
	if _, err := BootstrapShadow(ctx, f.dir, f.db, f.cctx); err != nil {
		t.Fatalf("BootstrapShadow: %v", err)
	}
	captureOnePendingFile(t, ctx, f, "provider-shape.txt", "invalid\n")

	planner := &recordingIntentPlanner{name: "openai-compat", err: &ai.IntentPlanValidationError{
		Code:    ai.IntentPlanValidationShape,
		Message: "openai-compat: no choices in response",
	}}
	health := NewIntentPlannerHealth(ctx, f.db, IntentPlannerHealthOptions{
		Provider: openAIIntentHealthIdentity("https://planner.example/v1"),
	})
	sum, err := Replay(ctx, f.dir, f.db, f.cctx, ReplayOpts{
		GitDir:                f.gitDir,
		CommitStrategy:        ai.CommitStrategyIntent,
		IntentPlanner:         planner,
		IntentHealth:          health,
		IntentPlannerProvider: "openai-compat",
		IntentWindow:          10,
		IntentMinPending:      1,
		IntentBypassBatchWait: true,
	})
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if sum.Published != 1 || planner.calls != 1 {
		t.Fatalf("summary=%+v planner_calls=%d", sum, planner.calls)
	}
	if snap := health.Snapshot(); snap.State != IntentPlannerCircuitClosed ||
		snap.ConsecutiveFailures != 1 || snap.LastFailureClass != IntentPlannerFailureValidation {
		t.Fatalf("health=%+v want one closed validation failure", snap)
	}
}

func TestReplay_IntentFailureSanitizesDurableObservability(t *testing.T) {
	runBoundedParallel(t)

	f := newCaptureFixture(t)
	ctx := context.Background()
	if _, err := BootstrapShadow(ctx, f.dir, f.db, f.cctx); err != nil {
		t.Fatalf("BootstrapShadow: %v", err)
	}
	captureOnePendingFile(t, ctx, f, "secret-error.txt", "change\n")
	pending, err := state.PendingEvents(ctx, f.db, 0)
	if err != nil || len(pending) != 1 {
		t.Fatalf("pending=%d err=%v", len(pending), err)
	}
	const secret = "planner-response-secret"
	planner := &recordingIntentPlanner{
		name: "openai-compat",
		err:  errors.New(`openai-compat: http 400: {"token":"` + secret + `"}`),
	}
	health := NewIntentPlannerHealth(ctx, f.db, IntentPlannerHealthOptions{
		Provider: openAIIntentHealthIdentity("https://planner.example/v1"),
	})
	prompts := &intentHealthPromptRecorder{}
	traces := &memoryTraceLogger{}
	if _, err := Replay(ctx, f.dir, f.db, f.cctx, ReplayOpts{
		GitDir:                f.gitDir,
		Trace:                 traces,
		PromptTrace:           prompts,
		CommitStrategy:        ai.CommitStrategyIntent,
		IntentPlanner:         planner,
		IntentHealth:          health,
		IntentPlannerProvider: "openai-compat",
		IntentWindow:          10,
		IntentMinPending:      1,
		IntentBypassBatchWait: true,
	}); err != nil {
		t.Fatalf("Replay: %v", err)
	}
	assertNoSecret := func(label, value string) {
		t.Helper()
		if strings.Contains(value, secret) || !strings.Contains(value, "[REDACTED]") {
			t.Fatalf("%s=%q", label, value)
		}
	}
	assertNoSecret("health last error", health.Snapshot().LastError)
	decisions, err := state.DecisionsForEvent(ctx, f.db, pending[0].Seq, 20)
	if err != nil {
		t.Fatalf("DecisionsForEvent: %v", err)
	}
	foundDecision := false
	for _, decision := range decisions {
		if decision.Kind == state.DecisionKindIntentPlannerError {
			foundDecision = true
			assertNoSecret("decision reason", decision.Reason.String)
		}
	}
	if !foundDecision {
		t.Fatal("planner error decision missing")
	}
	windows, err := state.RecentIntentPlannerWindows(ctx, f.db, 1)
	if err != nil || len(windows) != 1 {
		t.Fatalf("windows=%d err=%v", len(windows), err)
	}
	assertNoSecret("window validation failure", windows[0].ValidationFailure.String)
	foundPrompt := false
	for _, record := range prompts.Records() {
		if record.Stage == "fallback" && record.Response != nil {
			foundPrompt = true
			assertNoSecret("prompt fallback", record.Response.FallbackReason)
		}
	}
	if !foundPrompt {
		t.Fatal("prompt fallback record missing")
	}
	foundTrace := false
	for _, event := range traces.Events() {
		if event.EventClass == "intent.planner.validation_failed" {
			foundTrace = true
			assertNoSecret("trace error", event.Error)
		}
	}
	if !foundTrace {
		t.Fatal("planner validation trace missing")
	}
}

func TestReplay_IntentHealthOpenBypassDoesNotRepeatPlannerErrors(t *testing.T) {
	runBoundedParallel(t)

	f := newCaptureFixture(t)
	ctx := context.Background()
	if _, err := BootstrapShadow(ctx, f.dir, f.db, f.cctx); err != nil {
		t.Fatalf("BootstrapShadow: %v", err)
	}
	captureOnePendingFile(t, ctx, f, "first.txt", "first\n")
	captureOnePendingFile(t, ctx, f, "second.txt", "second\n")
	pending, err := state.PendingEvents(ctx, f.db, 0)
	if err != nil {
		t.Fatalf("PendingEvents: %v", err)
	}
	seqs := []int64{pending[0].Seq, pending[1].Seq}

	planner := &recordingIntentPlanner{name: "openai-compat", err: errors.New("gateway timeout")}
	health := NewIntentPlannerHealth(ctx, f.db, IntentPlannerHealthOptions{
		Provider: openAIIntentHealthIdentity("https://planner.example/v1"),
	})
	prompts := &intentHealthPromptRecorder{}
	opts := ReplayOpts{
		GitDir:                f.gitDir,
		PromptTrace:           prompts,
		CommitStrategy:        ai.CommitStrategyIntent,
		IntentPlanner:         planner,
		IntentHealth:          health,
		IntentPlannerProvider: "openai-compat",
		IntentPlannerModel:    "planner-model",
		IntentIncludeDiffs:    true,
		IntentWindow:          10,
		IntentMinPending:      1,
		IntentBypassBatchWait: true,
	}
	first, err := Replay(ctx, f.dir, f.db, f.cctx, opts)
	if err != nil {
		t.Fatalf("first Replay: %v", err)
	}
	if first.Published != 1 || planner.calls != 1 {
		t.Fatalf("first summary=%+v planner_calls=%d", first, planner.calls)
	}
	firstErrorCount := countIntentPlannerErrorDecisions(t, ctx, f.db, seqs)
	if firstErrorCount != 2 {
		t.Fatalf("first planner-error decisions=%d want 2", firstErrorCount)
	}

	f.cctx.BaseHead = first.BaseHead
	second, err := Replay(ctx, f.dir, f.db, f.cctx, opts)
	if err != nil {
		t.Fatalf("second Replay: %v", err)
	}
	if second.Published != 1 || planner.calls != 1 {
		t.Fatalf("second summary=%+v planner_calls=%d want circuit bypass", second, planner.calls)
	}
	if got := countIntentPlannerErrorDecisions(t, ctx, f.db, seqs); got != firstErrorCount {
		t.Fatalf("planner-error decisions grew on bypass: before=%d after=%d", firstErrorCount, got)
	}
	if snap := health.Snapshot(); snap.State != IntentPlannerCircuitOpen || snap.BypassCount != 1 {
		t.Fatalf("health=%+v want open with one bypass", snap)
	}

	windows, err := state.RecentIntentPlannerWindows(ctx, f.db, 2)
	if err != nil {
		t.Fatalf("RecentIntentPlannerWindows: %v", err)
	}
	if len(windows) != 2 {
		t.Fatalf("windows=%d want 2", len(windows))
	}
	if windows[0].Provider.String != "openai-compat" || windows[0].Model.String != "planner-model" || windows[0].ValidationFailure.Valid {
		t.Fatalf("bypass window metadata=%+v", windows[0])
	}
	if !windows[1].ValidationFailure.Valid || windows[1].ValidationFailure.String == "" {
		t.Fatalf("first failure window missing validation failure: %+v", windows[1])
	}

	records := prompts.Records()
	foundCircuitFallback := false
	for _, record := range records {
		if record.Stage != "fallback" || record.Response == nil ||
			!strings.Contains(record.Response.FallbackReason, "circuit bypass") {
			continue
		}
		foundCircuitFallback = true
		if record.Provider != "openai-compat" || record.Model != "planner-model" || !record.DiffIncluded {
			t.Fatalf("circuit fallback metadata=%+v", record)
		}
	}
	if !foundCircuitFallback {
		t.Fatalf("prompt records missing circuit fallback: %+v", records)
	}
}

func TestReplay_IntentHealthSelectionSafetyFailureIsValidation(t *testing.T) {
	runBoundedParallel(t)

	f := newCaptureFixture(t)
	ctx := context.Background()
	if _, err := BootstrapShadow(ctx, f.dir, f.db, f.cctx); err != nil {
		t.Fatalf("BootstrapShadow: %v", err)
	}
	path := filepath.Join(f.dir, "same.txt")
	if err := os.WriteFile(path, []byte("first\n"), 0o644); err != nil {
		t.Fatalf("write first: %v", err)
	}
	if _, err := Capture(ctx, f.dir, f.db, f.cctx, CaptureOpts{IgnoreChecker: f.ig, SensitiveMatcher: f.matcher}); err != nil {
		t.Fatalf("capture first: %v", err)
	}
	if err := os.WriteFile(path, []byte("second\n"), 0o644); err != nil {
		t.Fatalf("write second: %v", err)
	}
	if _, err := Capture(ctx, f.dir, f.db, f.cctx, CaptureOpts{IgnoreChecker: f.ig, SensitiveMatcher: f.matcher}); err != nil {
		t.Fatalf("capture second: %v", err)
	}
	pending, err := state.PendingEvents(ctx, f.db, 0)
	if err != nil || len(pending) != 2 {
		t.Fatalf("pending=%d err=%v want 2", len(pending), err)
	}
	planner := &recordingIntentPlanner{name: "openai-compat", plan: ai.IntentPlan{
		SelectedSeqs:    []int64{pending[1].Seq},
		DeferredSeqs:    []int64{pending[0].Seq},
		DeferredReasons: []ai.DeferredReason{{Seq: pending[0].Seq, Reason: "wait"}},
		Subject:         "Publish dependent edit",
		GroupingReason:  "incorrectly skipped the prior same-path capture",
	}}
	health := NewIntentPlannerHealth(ctx, f.db, IntentPlannerHealthOptions{Provider: openAIIntentHealthIdentity("https://planner.example/v1")})
	sum, err := Replay(ctx, f.dir, f.db, f.cctx, ReplayOpts{
		GitDir:                f.gitDir,
		CommitStrategy:        ai.CommitStrategyIntent,
		IntentPlanner:         planner,
		IntentHealth:          health,
		IntentWindow:          10,
		IntentMinPending:      1,
		IntentBypassBatchWait: true,
	})
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if sum.Published != 1 {
		t.Fatalf("summary=%+v want deterministic fallback publish", sum)
	}
	if snap := health.Snapshot(); snap.State != IntentPlannerCircuitClosed ||
		snap.ConsecutiveFailures != 1 || snap.LastFailureClass != IntentPlannerFailureValidation {
		t.Fatalf("health=%+v want one closed validation failure", snap)
	}
}

func TestRun_IntentV2HealthReusesRevisionedProvider(t *testing.T) {
	t.Setenv(ai.EnvCommitStrategy, string(ai.CommitStrategyIntent))
	t.Setenv(ai.EnvIntentMinPending, "1")
	t.Setenv(ai.EnvIntentSettleWindow, "0")
	t.Setenv("ACD_AI_DIFF_EGRESS", "1")
	t.Setenv("XDG_CONFIG_HOME", filepath.Join(t.TempDir(), "config"))
	t.Setenv("XDG_STATE_HOME", filepath.Join(t.TempDir(), "state"))
	t.Setenv("XDG_DATA_HOME", filepath.Join(t.TempDir(), "share"))

	f := newDaemonFixture(t)
	registerLiveClient(t, f.db)
	revision := runtimeRevision(t, f.db, "health-reuse", 1,
		map[string]any{
			"ai.provider":          "subprocess:runtime-health",
			"commit.strategy":      "intent",
			"intent.min_pending":   1,
			"intent.settle_window": "0s",
			"confirmations": []string{
				string(ai.ConfirmationSubprocessExecution),
				string(ai.ConfirmationDiffEgress),
			},
		})
	if _, ok, err := state.RequestConfigActivation(context.Background(), f.db,
		revision.ID, sql.NullInt64{}); err != nil || !ok {
		t.Fatalf("request runtime activation: ok=%v err=%v", ok, err)
	}
	if err := os.WriteFile(filepath.Join(f.dir, "provider-reuse.txt"), []byte("reuse\n"), 0o644); err != nil {
		t.Fatalf("write provider-reuse.txt: %v", err)
	}
	provider := &intentHealthRunProvider{}
	closer := &intentHealthRunCloser{}
	wakeCh := make(chan struct{}, 4)
	shutdownCh := make(chan struct{}, 1)
	runCtx, cancel := context.WithCancel(context.Background())
	manual := Scheduler{Base: time.Hour, IdleCeiling: time.Hour, ErrorCeiling: time.Hour}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = Run(runCtx, Options{
			RepoPath:    f.dir,
			GitDir:      f.gitDir,
			DB:          f.db,
			Scheduler:   manual,
			BootGrace:   30 * time.Second,
			WakeCh:      wakeCh,
			ShutdownCh:  shutdownCh,
			SkipSignals: true,
			runtimeBuildProvider: func(ai.ProviderConfig) (ai.Provider, io.Closer, error) {
				return provider, closer, nil
			},
		})
	}()
	t.Cleanup(func() {
		cancel()
		wg.Wait()
	})
	waitFor(t, 5*time.Second, "run-scoped intent planner call", func() bool {
		calls, _ := provider.snapshot()
		return calls >= 1
	})
	waitFor(t, 5*time.Second, "provider-reuse capture published", func() bool {
		var count int
		if err := f.db.ReadSQL().QueryRowContext(context.Background(),
			`SELECT COUNT(*) FROM capture_events WHERE state = ?`,
			state.EventStatePublished).Scan(&count); err != nil {
			return false
		}
		return count >= 1
	})
	if err := os.WriteFile(filepath.Join(f.dir, "provider-reuse-second.txt"),
		[]byte("reuse again\n"), 0o644); err != nil {
		t.Fatalf("write provider-reuse-second.txt: %v", err)
	}
	wakeCh <- struct{}{}
	waitFor(t, 5*time.Second, "second run-scoped intent planner call", func() bool {
		calls, _ := provider.snapshot()
		return calls >= 2
	})
	waitFor(t, 5*time.Second, "second provider-reuse capture published", func() bool {
		var count int
		if err := f.db.ReadSQL().QueryRowContext(context.Background(),
			`SELECT COUNT(*) FROM capture_events WHERE state = ?`,
			state.EventStatePublished).Scan(&count); err != nil {
			return false
		}
		return count >= 2
	})
	waitFor(t, 5*time.Second, "second Intent v2 candidate settled", func() bool {
		var count int
		if err := f.db.ReadSQL().QueryRowContext(context.Background(),
			`SELECT COUNT(*) FROM intent_candidates WHERE status = 'published'`,
		).Scan(&count); err != nil {
			return false
		}
		return count >= 2
	})
	cancel()
	wg.Wait()

	calls, requests := provider.snapshot()
	if calls != 2 || len(requests) != 2 {
		t.Fatalf("revisioned provider calls=%d requests=%d want 2/2",
			calls, len(requests))
	}
	for i, request := range requests {
		if len(request.OfferedCaptures) == 0 ||
			request.OfferedCaptures[0].CapturedDiff == "" {
			t.Fatalf("planner request %d=%+v want diff-bearing request",
				i, request)
		}
	}
	if got := closer.calls.Load(); got != 1 {
		t.Fatalf("provider Close calls=%d want 1", got)
	}
	var persisted intentPlannerHealthRecord
	if ok, err := state.MetaGetJSON(context.Background(), f.db,
		MetaKeyIntentPlannerHealth, &persisted); err != nil || !ok {
		t.Fatalf("load persisted intent health ok=%v err=%v", ok, err)
	}
	if persisted.State != IntentPlannerCircuitClosed ||
		persisted.ProviderFingerprint == "" {
		t.Fatalf("persisted health=%+v", persisted)
	}
}
