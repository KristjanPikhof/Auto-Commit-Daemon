package daemon

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unicode"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/ai"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

type intentHealthClock struct {
	mu  sync.Mutex
	now time.Time
}

func newIntentHealthClock() *intentHealthClock {
	return &intentHealthClock{now: time.Date(2026, 7, 13, 0, 0, 0, 0, time.UTC)}
}

func (c *intentHealthClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *intentHealthClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	c.mu.Unlock()
}

func openAIIntentHealthIdentity(endpoint string) IntentPlannerProviderIdentity {
	return IntentPlannerProviderIdentity{
		Provider: "openai-compat",
		Model:    "planner-model",
		Endpoint: endpoint,
	}
}

func newIntentHealthTestDB(t *testing.T) *state.DB {
	t.Helper()
	db, err := state.Open(context.Background(), filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatalf("state.Open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func readIntentHealthRecord(t *testing.T, db *state.DB) intentPlannerHealthRecord {
	t.Helper()
	var record intentPlannerHealthRecord
	ok, err := state.MetaGetJSON(context.Background(), db, MetaKeyIntentPlannerHealth, &record)
	if err != nil {
		t.Fatalf("MetaGetJSON: %v", err)
	}
	if !ok {
		t.Fatal("intent planner health record missing")
	}
	return record
}

func TestIntentPlannerCircuitWaitClassificationIsStructural(t *testing.T) {
	waitErr := &IntentPlannerCircuitOpenError{
		RetryAt: time.Unix(100, 0).UTC(),
	}
	if !isIntentPlannerCircuitWait(waitErr) ||
		!isIntentPlannerCircuitWait(fmt.Errorf("semantic planning: %w", waitErr)) {
		t.Fatal("typed circuit wait was not classified as transient")
	}
	if isIntentPlannerCircuitWait(errors.New(waitErr.Error())) {
		t.Fatal("matching error text was accepted without the typed cause")
	}
	if isIntentPlannerCircuitWait(errors.Join(waitErr, errors.New("database failed"))) {
		t.Fatal("joined non-wait failure was suppressed as a circuit wait")
	}
}

func TestIntentPlannerHealthTransportOpensImmediatelyAndBacksOff(t *testing.T) {
	db := newIntentHealthTestDB(t)
	clock := newIntentHealthClock()
	health := NewIntentPlannerHealth(context.Background(), db, IntentPlannerHealthOptions{
		Provider: openAIIntentHealthIdentity("https://planner.example/v1"),
		Now:      clock.Now,
	})

	permit, err := health.Acquire(context.Background())
	if err != nil {
		t.Fatalf("Acquire closed: %v", err)
	}
	if err := health.Complete(context.Background(), permit, &IntentPlannerTransportFailure{
		Err: errors.New("connection refused"),
	}); err != nil {
		t.Fatalf("Complete transport: %v", err)
	}

	snap := health.Snapshot()
	if snap.State != IntentPlannerCircuitOpen || snap.BackoffLevel != 0 {
		t.Fatalf("snapshot=%+v want open level 0", snap)
	}
	if got, want := snap.NextProbeTS, intentPlannerHealthTimestamp(clock.Now().Add(30*time.Second)); got != want {
		t.Fatalf("next_probe_ts=%f want %f", got, want)
	}
	if _, err := health.Acquire(context.Background()); err == nil {
		t.Fatal("Acquire during cooldown succeeded")
	} else {
		var openErr *IntentPlannerCircuitOpenError
		if !errors.As(err, &openErr) || openErr.HalfOpen {
			t.Fatalf("Acquire error=%T %v want cooldown open error", err, err)
		}
	}
	if got := health.Snapshot().BypassCount; got != 1 {
		t.Fatalf("bypass_count=%d want 1", got)
	}

	for step, tc := range []struct {
		advance          time.Duration
		failure          error
		level            int
		backoff          time.Duration
		maxProbeFailures int
	}{
		{30 * time.Second, &IntentPlannerTransportFailure{Err: errors.New("timeout")}, 1, 2 * time.Minute, 0},
		{2 * time.Minute, &IntentPlannerValidationFailure{Err: errors.New("invalid selection")}, 2, 10 * time.Minute, 0},
		{10 * time.Minute, &IntentPlannerTransportFailure{Err: errors.New("still unavailable")}, 2, 10 * time.Minute, 1},
	} {
		clock.Advance(tc.advance)
		probe, err := health.Acquire(context.Background())
		if err != nil {
			t.Fatalf("step %d Acquire probe: %v", step, err)
		}
		if !probe.halfOpenProbe {
			t.Fatalf("step %d permit=%+v want half-open probe", step, probe)
		}
		if err := health.Complete(context.Background(), probe, tc.failure); err != nil {
			t.Fatalf("step %d Complete: %v", step, err)
		}
		snap = health.Snapshot()
		if snap.State != IntentPlannerCircuitOpen || snap.BackoffLevel != tc.level {
			t.Fatalf("step %d snapshot=%+v want open level %d", step, snap, tc.level)
		}
		if snap.MaxBackoffProbeFailures != tc.maxProbeFailures {
			t.Fatalf("step %d max probes=%d want %d", step,
				snap.MaxBackoffProbeFailures, tc.maxProbeFailures)
		}
		if got, want := snap.NextProbeTS, intentPlannerHealthTimestamp(clock.Now().Add(tc.backoff)); got != want {
			t.Fatalf("step %d next_probe_ts=%f want %f", step, got, want)
		}
	}

	persisted := readIntentHealthRecord(t, db)
	if persisted.State != IntentPlannerCircuitOpen || persisted.BackoffLevel != 2 ||
		persisted.MaxBackoffProbeFailures != 1 || persisted.BypassCount != 1 {
		t.Fatalf("persisted=%+v", persisted)
	}
}

func TestIntentPlannerHealthValidationCountsOnlyCompletedMaxProbe(t *testing.T) {
	clock := newIntentHealthClock()
	health := NewIntentPlannerHealth(context.Background(), nil,
		IntentPlannerHealthOptions{
			Provider: openAIIntentHealthIdentity("https://planner.example/v1"),
			Now:      clock.Now,
		})

	for attempt := 1; attempt <= 3; attempt++ {
		permit, err := health.Acquire(context.Background())
		if err != nil {
			t.Fatalf("validation %d acquire: %v", attempt, err)
		}
		if err := health.Complete(context.Background(), permit,
			&IntentPlannerValidationFailure{Err: errors.New("invalid plan")}); err != nil {
			t.Fatalf("validation %d complete: %v", attempt, err)
		}
	}
	for attempt, advance := range []time.Duration{
		30 * time.Second, 2 * time.Minute, 10 * time.Minute,
	} {
		clock.Advance(advance)
		permit, err := health.Acquire(context.Background())
		if err != nil {
			t.Fatalf("probe %d acquire: %v", attempt+1, err)
		}
		if err := health.Complete(context.Background(), permit,
			&IntentPlannerValidationFailure{Err: errors.New("still invalid")}); err != nil {
			t.Fatalf("probe %d complete: %v", attempt+1, err)
		}
		want := 0
		if attempt == 2 {
			want = 1
		}
		if got := health.Snapshot().MaxBackoffProbeFailures; got != want {
			t.Fatalf("probe %d max failures=%d want %d", attempt+1, got, want)
		}
	}
}

func TestIntentPlannerHealthOpensAfterThreeValidationFailures(t *testing.T) {
	clock := newIntentHealthClock()
	health := NewIntentPlannerHealth(context.Background(), nil, IntentPlannerHealthOptions{
		Provider: openAIIntentHealthIdentity("https://planner.example/v1"),
		Now:      clock.Now,
	})

	for attempt := 1; attempt <= 3; attempt++ {
		permit, err := health.Acquire(context.Background())
		if err != nil {
			t.Fatalf("attempt %d Acquire: %v", attempt, err)
		}
		err = health.Complete(context.Background(), permit, &IntentPlannerValidationFailure{
			Err: fmt.Errorf("validation %d", attempt),
		})
		if err != nil {
			t.Fatalf("attempt %d Complete: %v", attempt, err)
		}
		snap := health.Snapshot()
		if snap.ConsecutiveFailures != attempt {
			t.Fatalf("attempt %d failures=%d", attempt, snap.ConsecutiveFailures)
		}
		wantState := IntentPlannerCircuitClosed
		if attempt == 3 {
			wantState = IntentPlannerCircuitOpen
		}
		if snap.State != wantState {
			t.Fatalf("attempt %d state=%s want %s", attempt, snap.State, wantState)
		}
	}
}

func TestIntentPlannerHealthSuccessResetsValidationAndClosesProbe(t *testing.T) {
	clock := newIntentHealthClock()
	health := NewIntentPlannerHealth(context.Background(), nil, IntentPlannerHealthOptions{
		Provider: openAIIntentHealthIdentity("https://planner.example/v1"),
		Now:      clock.Now,
	})

	permit, _ := health.Acquire(context.Background())
	_ = health.Complete(context.Background(), permit, &IntentPlannerValidationFailure{Err: errors.New("bad plan")})
	permit, _ = health.Acquire(context.Background())
	if err := health.Complete(context.Background(), permit, nil); err != nil {
		t.Fatalf("Complete success: %v", err)
	}
	if snap := health.Snapshot(); snap.State != IntentPlannerCircuitClosed ||
		snap.ConsecutiveFailures != 0 || snap.MaxBackoffProbeFailures != 0 ||
		snap.LastError != "" {
		t.Fatalf("after success snapshot=%+v", snap)
	}

	permit, _ = health.Acquire(context.Background())
	_ = health.Complete(context.Background(), permit, &IntentPlannerTransportFailure{Err: errors.New("down")})
	clock.Advance(30 * time.Second)
	probe, err := health.Acquire(context.Background())
	if err != nil {
		t.Fatalf("Acquire half-open: %v", err)
	}
	if err := health.Complete(context.Background(), probe, nil); err != nil {
		t.Fatalf("Complete probe success: %v", err)
	}
	if snap := health.Snapshot(); snap.State != IntentPlannerCircuitClosed || snap.BackoffLevel != 0 || snap.NextProbeTS != 0 {
		t.Fatalf("after probe success snapshot=%+v", snap)
	}
}

func TestIntentPlannerHealthExactlyOneHalfOpenLease(t *testing.T) {
	clock := newIntentHealthClock()
	health := NewIntentPlannerHealth(context.Background(), nil, IntentPlannerHealthOptions{
		Provider: openAIIntentHealthIdentity("https://planner.example/v1"),
		Now:      clock.Now,
	})
	permit, _ := health.Acquire(context.Background())
	_ = health.Complete(context.Background(), permit, &IntentPlannerTransportFailure{Err: errors.New("down")})
	clock.Advance(30 * time.Second)

	const callers = 32
	start := make(chan struct{})
	var allowed atomic.Int32
	var halfOpenRejected atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_, err := health.Acquire(context.Background())
			if err == nil {
				allowed.Add(1)
				return
			}
			var openErr *IntentPlannerCircuitOpenError
			if errors.As(err, &openErr) && openErr.HalfOpen {
				halfOpenRejected.Add(1)
			}
		}()
	}
	close(start)
	wg.Wait()
	if got := allowed.Load(); got != 1 {
		t.Fatalf("allowed=%d want 1", got)
	}
	if got := halfOpenRejected.Load(); got != callers-1 {
		t.Fatalf("half-open rejected=%d want %d", got, callers-1)
	}
	if got := health.Snapshot().BypassCount; got != callers-1 {
		t.Fatalf("bypass_count=%d want %d", got, callers-1)
	}
}

func TestIntentPlannerHealthProviderFingerprintReset(t *testing.T) {
	db := newIntentHealthTestDB(t)
	clock := newIntentHealthClock()
	providerA := openAIIntentHealthIdentity("https://planner-a.example/v1")
	providerB := openAIIntentHealthIdentity("https://planner-b.example/v1")
	health := NewIntentPlannerHealth(context.Background(), db, IntentPlannerHealthOptions{Provider: providerA, Now: clock.Now})
	permit, _ := health.Acquire(context.Background())
	_ = health.Complete(context.Background(), permit, &IntentPlannerTransportFailure{Err: errors.New("down")})

	reset := NewIntentPlannerHealth(context.Background(), db, IntentPlannerHealthOptions{Provider: providerB, Now: clock.Now})
	if snap := reset.Snapshot(); snap.State != IntentPlannerCircuitClosed ||
		snap.ProviderFingerprint != IntentPlannerProviderFingerprint(providerB) {
		t.Fatalf("reset snapshot=%+v", snap)
	}
	persisted := readIntentHealthRecord(t, db)
	if persisted.State != IntentPlannerCircuitClosed || persisted.ProviderFingerprint != IntentPlannerProviderFingerprint(providerB) {
		t.Fatalf("persisted reset=%+v", persisted)
	}
}

func TestIntentPlannerHealthDeterministicBypassPersistsProviderReset(t *testing.T) {
	db := newIntentHealthTestDB(t)
	clock := newIntentHealthClock()
	remote := openAIIntentHealthIdentity("https://planner.example/v1")
	health := NewIntentPlannerHealth(context.Background(), db, IntentPlannerHealthOptions{Provider: remote, Now: clock.Now})
	permit, _ := health.Acquire(context.Background())
	_ = health.Complete(context.Background(), permit, &IntentPlannerTransportFailure{Err: errors.New("down")})
	deterministic := NewIntentPlannerHealth(context.Background(), db, IntentPlannerHealthOptions{
		// Keep the provider label deliberately identical: changing only the
		// provider mode must still reset yesterday's remote outage.
		Provider: IntentPlannerProviderIdentity{Provider: "openai-compat", Deterministic: true},
		Now:      clock.Now,
	})
	permit, err := deterministic.Acquire(context.Background())
	if err != nil {
		t.Fatalf("deterministic Acquire: %v", err)
	}
	if err := deterministic.Complete(context.Background(), permit, &IntentPlannerTransportFailure{Err: errors.New("ignored")}); err != nil {
		t.Fatalf("deterministic Complete: %v", err)
	}
	after := readIntentHealthRecord(t, db)
	if after.State != IntentPlannerCircuitClosed || after.ConsecutiveFailures != 0 || after.LastError != "" {
		t.Fatalf("deterministic reset persisted=%+v", after)
	}
	if after.ProviderFingerprint != IntentPlannerProviderFingerprint(IntentPlannerProviderIdentity{
		Provider: "openai-compat", Deterministic: true,
	}) {
		t.Fatalf("deterministic fingerprint=%q", after.ProviderFingerprint)
	}
}

func TestIntentPlannerHealthRestartNormalizesHalfOpenLease(t *testing.T) {
	db := newIntentHealthTestDB(t)
	clock := newIntentHealthClock()
	provider := openAIIntentHealthIdentity("https://planner.example/v1")
	fingerprint := IntentPlannerProviderFingerprint(provider)
	if err := state.MetaSetJSON(context.Background(), db, MetaKeyIntentPlannerHealth, intentPlannerHealthRecord{
		Version: intentPlannerHealthVersion,
		IntentPlannerHealthSnapshot: IntentPlannerHealthSnapshot{
			State:               IntentPlannerCircuitHalfOpen,
			ProviderFingerprint: fingerprint,
			BackoffLevel:        1,
			NextProbeTS:         intentPlannerHealthTimestamp(clock.Now().Add(-time.Minute)),
		},
	}); err != nil {
		t.Fatalf("seed half-open: %v", err)
	}

	restarted := NewIntentPlannerHealth(context.Background(), db, IntentPlannerHealthOptions{Provider: provider, Now: clock.Now})
	snap := restarted.Snapshot()
	if snap.State != IntentPlannerCircuitOpen || snap.NextProbeTS != intentPlannerHealthTimestamp(clock.Now()) {
		t.Fatalf("restart snapshot=%+v", snap)
	}
	first, err := restarted.Acquire(context.Background())
	if err != nil || !first.halfOpenProbe {
		t.Fatalf("first Acquire permit=%+v err=%v", first, err)
	}
	if _, err := restarted.Acquire(context.Background()); err == nil {
		t.Fatal("second Acquire unexpectedly obtained restart probe")
	} else {
		var openErr *IntentPlannerCircuitOpenError
		if !errors.As(err, &openErr) || !openErr.HalfOpen {
			t.Fatalf("second Acquire error=%T %v", err, err)
		}
	}
	if persisted := readIntentHealthRecord(t, db); persisted.State != IntentPlannerCircuitHalfOpen {
		t.Fatalf("persisted state=%s want half_open after claim", persisted.State)
	}
}

func TestIntentPlannerHealthCancellationDoesNotMutate(t *testing.T) {
	clock := newIntentHealthClock()
	health := NewIntentPlannerHealth(context.Background(), nil, IntentPlannerHealthOptions{
		Provider: openAIIntentHealthIdentity("https://planner.example/v1"),
		Now:      clock.Now,
	})
	permit, _ := health.Acquire(context.Background())
	before := health.Snapshot()

	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if err := health.Complete(canceled, permit, context.Canceled); !errors.Is(err, context.Canceled) {
		t.Fatalf("Complete error=%v want context.Canceled", err)
	}
	if after := health.Snapshot(); !reflect.DeepEqual(before, after) {
		t.Fatalf("canceled Complete mutated state\nbefore=%+v\nafter=%+v", before, after)
	}
	if err := health.Complete(context.Background(), permit, context.Canceled); err != nil {
		t.Fatalf("Complete provider-internal context.Canceled: %v", err)
	}
	if after := health.Snapshot(); after.State != IntentPlannerCircuitOpen || after.LastFailureClass != IntentPlannerFailureTransport {
		t.Fatalf("provider-internal cancellation snapshot=%+v want transport-open", after)
	}

	clock.Advance(30 * time.Second)
	openBefore := health.Snapshot()
	if _, err := health.Acquire(canceled); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled Acquire error=%v", err)
	}
	if after := health.Snapshot(); !reflect.DeepEqual(openBefore, after) {
		t.Fatalf("canceled Acquire mutated state\nbefore=%+v\nafter=%+v", openBefore, after)
	}
}

func TestIntentPlannerHealthClassifiesRewriteOutcomes(t *testing.T) {
	transportCause := errors.New("rewrite endpoint unavailable")
	transportErr := &ai.MessageQualityError{
		Provider: "openai-compat",
		Cause:    transportCause,
	}
	classified := classifyIntentPlannerHealthFailure(transportErr, true)
	var transport *IntentPlannerTransportFailure
	if !errors.As(classified, &transport) || !errors.Is(classified, transportCause) {
		t.Fatalf("transport classification=%T %v", classified, classified)
	}
	transportHealth := NewIntentPlannerHealth(context.Background(), nil, IntentPlannerHealthOptions{
		Provider: openAIIntentHealthIdentity("https://planner.example/v1"),
	})
	permit, err := transportHealth.Acquire(context.Background())
	if err != nil {
		t.Fatalf("transport Acquire: %v", err)
	}
	if err := transportHealth.Complete(context.Background(), permit, classified); err != nil {
		t.Fatalf("transport Complete: %v", err)
	}
	if snapshot := transportHealth.Snapshot(); snapshot.State != IntentPlannerCircuitOpen ||
		snapshot.LastFailureClass != IntentPlannerFailureTransport {
		t.Fatalf("transport snapshot=%+v want immediate open", snapshot)
	}

	malformedErr := &ai.MessageQualityError{
		Provider: "openai-compat",
		Cause: &ai.IntentMessageRewriteValidationError{
			Err: errors.New("malformed rewrite response"),
		},
	}
	classified = classifyIntentPlannerHealthFailure(malformedErr, true)
	var validation *IntentPlannerValidationFailure
	if !errors.As(classified, &validation) {
		t.Fatalf("malformed classification=%T %v want validation", classified, classified)
	}
	malformedHealth := NewIntentPlannerHealth(context.Background(), nil, IntentPlannerHealthOptions{
		Provider: openAIIntentHealthIdentity("https://planner.example/v1"),
	})
	permit, err = malformedHealth.Acquire(context.Background())
	if err != nil {
		t.Fatalf("malformed Acquire: %v", err)
	}
	if err := malformedHealth.Complete(context.Background(), permit, classified); err != nil {
		t.Fatalf("malformed Complete: %v", err)
	}
	if snapshot := malformedHealth.Snapshot(); snapshot.State != IntentPlannerCircuitClosed ||
		snapshot.ConsecutiveFailures != 1 || snapshot.LastFailureClass != IntentPlannerFailureValidation {
		t.Fatalf("malformed snapshot=%+v want one validation failure", snapshot)
	}

	qualityErr := &ai.MessageQualityError{
		Provider: "openai-compat",
		Report: ai.MessageQualityReport{
			Action: ai.MessageQualityRewrite,
		},
	}
	classified = classifyIntentPlannerHealthFailure(qualityErr, true)
	validation = nil
	if !errors.As(classified, &validation) {
		t.Fatalf("quality classification=%T %v want validation", classified, classified)
	}
	qualityHealth := NewIntentPlannerHealth(context.Background(), nil, IntentPlannerHealthOptions{
		Provider: openAIIntentHealthIdentity("https://planner.example/v1"),
	})
	permit, err = qualityHealth.Acquire(context.Background())
	if err != nil {
		t.Fatalf("quality Acquire: %v", err)
	}
	if err := qualityHealth.Complete(context.Background(), permit, classified); err != nil {
		t.Fatalf("quality Complete: %v", err)
	}
	if snapshot := qualityHealth.Snapshot(); snapshot.State != IntentPlannerCircuitClosed ||
		snapshot.ConsecutiveFailures != 1 || snapshot.LastFailureClass != IntentPlannerFailureValidation {
		t.Fatalf("quality snapshot=%+v want one validation failure", snapshot)
	}
}

func TestIntentPlannerHealthWrappedCallerCancellationDoesNotMutate(t *testing.T) {
	health := NewIntentPlannerHealth(context.Background(), nil, IntentPlannerHealthOptions{
		Provider: openAIIntentHealthIdentity("https://planner.example/v1"),
	})
	permit, err := health.Acquire(context.Background())
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	before := health.Snapshot()
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	rewriteErr := &ai.MessageQualityError{
		Provider: "openai-compat",
		Cause:    context.Canceled,
	}
	classified := classifyIntentPlannerHealthFailure(rewriteErr, true)
	if err := health.Complete(canceled, permit, classified); !errors.Is(err, context.Canceled) {
		t.Fatalf("Complete error=%v want context.Canceled", err)
	}
	if after := health.Snapshot(); !reflect.DeepEqual(before, after) {
		t.Fatalf("caller cancellation mutated health\nbefore=%+v\nafter=%+v", before, after)
	}
}

func TestIntentPlannerHealthCallerCancellationWinsUnrelatedFailureWithoutMutation(t *testing.T) {
	health := NewIntentPlannerHealth(context.Background(), nil, IntentPlannerHealthOptions{
		Provider: openAIIntentHealthIdentity("https://planner.example/v1"),
	})
	permit, err := health.Acquire(context.Background())
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	before := health.Snapshot()
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	failure := &IntentPlannerTransportFailure{Err: errors.New("provider failed independently")}
	if err := health.Complete(canceled, permit, failure); !errors.Is(err, context.Canceled) {
		t.Fatalf("Complete error=%v want context.Canceled", err)
	}
	if after := health.Snapshot(); !reflect.DeepEqual(before, after) {
		t.Fatalf("caller cancellation mutated health\nbefore=%+v\nafter=%+v", before, after)
	}
}

func TestIntentPlannerHealthCanceledHalfOpenProbeReturnsToOpen(t *testing.T) {
	clock := newIntentHealthClock()
	db := newIntentHealthTestDB(t)
	health := NewIntentPlannerHealth(context.Background(), db, IntentPlannerHealthOptions{
		Provider: openAIIntentHealthIdentity("https://planner.example/v1"),
		Now:      clock.Now,
	})
	permit, err := health.Acquire(context.Background())
	if err != nil {
		t.Fatalf("initial Acquire: %v", err)
	}
	if err := health.Complete(context.Background(), permit, &IntentPlannerTransportFailure{Err: errors.New("offline")}); err != nil {
		t.Fatalf("open circuit: %v", err)
	}
	before := health.Snapshot()
	clock.Advance(30 * time.Second)
	probe, err := health.Acquire(context.Background())
	if err != nil || !probe.halfOpenProbe {
		t.Fatalf("half-open Acquire permit=%+v err=%v", probe, err)
	}

	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	failure := &IntentPlannerTransportFailure{Err: errors.New("provider failed independently")}
	if err := health.Complete(canceled, probe, failure); !errors.Is(err, context.Canceled) {
		t.Fatalf("Complete error=%v want context.Canceled", err)
	}
	after := health.Snapshot()
	if after.State != IntentPlannerCircuitOpen ||
		after.ConsecutiveFailures != before.ConsecutiveFailures ||
		after.BackoffLevel != before.BackoffLevel ||
		after.LastFailureClass != before.LastFailureClass ||
		after.LastError != before.LastError ||
		after.NextProbeTS != intentPlannerHealthTimestamp(clock.Now()) {
		t.Fatalf("canceled half-open snapshot=%+v before=%+v", after, before)
	}
	if persisted := readIntentHealthRecord(t, db); persisted.State != IntentPlannerCircuitOpen || persisted.NextProbeTS != after.NextProbeTS {
		t.Fatalf("persisted canceled probe=%+v want open at next probe", persisted)
	}
	next, err := health.Acquire(context.Background())
	if err != nil || !next.halfOpenProbe {
		t.Fatalf("next Acquire permit=%+v err=%v want new probe", next, err)
	}
}

func TestIntentPlannerHealthRejectsUntypedFailureWithoutMutation(t *testing.T) {
	health := NewIntentPlannerHealth(context.Background(), nil, IntentPlannerHealthOptions{
		Provider: openAIIntentHealthIdentity("https://planner.example/v1"),
	})
	permit, _ := health.Acquire(context.Background())
	before := health.Snapshot()
	err := health.Complete(context.Background(), permit, errors.New("ambiguous"))
	var typed *IntentPlannerFailureTypeError
	if !errors.As(err, &typed) {
		t.Fatalf("Complete error=%T %v want IntentPlannerFailureTypeError", err, err)
	}
	if after := health.Snapshot(); !reflect.DeepEqual(before, after) {
		t.Fatalf("untyped failure mutated state\nbefore=%+v\nafter=%+v", before, after)
	}
}

func TestIntentPlannerHealthPersistenceIsBestEffort(t *testing.T) {
	db := newIntentHealthTestDB(t)
	health := NewIntentPlannerHealth(context.Background(), db, IntentPlannerHealthOptions{
		Provider: openAIIntentHealthIdentity("https://planner.example/v1"),
	})
	permit, _ := health.Acquire(context.Background())
	var logs bytes.Buffer
	previousLogger := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, nil)))
	t.Cleanup(func() { slog.SetDefault(previousLogger) })
	if err := db.Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}
	if err := health.Complete(context.Background(), permit, &IntentPlannerTransportFailure{Err: errors.New("down")}); err != nil {
		t.Fatalf("Complete with unavailable persistence: %v", err)
	}
	if snap := health.Snapshot(); snap.State != IntentPlannerCircuitOpen {
		t.Fatalf("snapshot=%+v want in-memory open", snap)
	}
	if !strings.Contains(logs.String(), "intent planner health persistence failed") {
		t.Fatalf("persistence warning missing: %q", logs.String())
	}
}

func TestIntentPlannerHealthFingerprintAndErrorRedaction(t *testing.T) {
	const errorCap = 512
	first := IntentPlannerProviderIdentity{
		Provider: "openai-compat",
		Model:    "planner-model",
		Endpoint: "https://alice:password-one@EXAMPLE.com/v1?api_key=query-one#fragment-one",
	}
	second := IntentPlannerProviderIdentity{
		Provider: "openai-compat",
		Model:    "planner-model",
		Endpoint: "https://bob:password-two@example.com/v1?token=query-two#fragment-two",
	}
	fingerprint := IntentPlannerProviderFingerprint(first)
	if fingerprint != IntentPlannerProviderFingerprint(second) {
		t.Fatalf("fingerprints differ after secret-only endpoint changes: %q vs %q", fingerprint, IntentPlannerProviderFingerprint(second))
	}
	for _, secret := range []string{"alice", "password-one", "query-one", "api_key"} {
		if strings.Contains(fingerprint, secret) {
			t.Fatalf("fingerprint contains secret %q: %q", secret, fingerprint)
		}
	}
	if fingerprint == IntentPlannerProviderFingerprint(openAIIntentHealthIdentity("https://other.example/v1")) {
		t.Fatal("different endpoint identity produced identical fingerprint")
	}

	raw := "request https://alice:password-one@example.com/v1?api_key=query-one#fragment " +
		"Authorization: Bearer bearer-secret token=token-secret api_key=key-secret " +
		"sk-1234567890abcdef\x00\n" + strings.Repeat("x", errorCap+100)
	clean := ai.SanitizePlannerError(raw)
	for _, secret := range []string{
		"alice", "password-one", "query-one", "bearer-secret", "token-secret",
		"key-secret", "sk-1234567890abcdef",
	} {
		if strings.Contains(clean, secret) {
			t.Fatalf("sanitized error contains %q: %q", secret, clean)
		}
	}
	if got := len([]rune(clean)); got > errorCap {
		t.Fatalf("sanitized error runes=%d want <=%d", got, errorCap)
	}
	for _, r := range clean {
		if unicode.IsControl(r) {
			t.Fatalf("sanitized error contains control rune %U", r)
		}
	}

	db := newIntentHealthTestDB(t)
	health := NewIntentPlannerHealth(context.Background(), db, IntentPlannerHealthOptions{Provider: first})
	permit, _ := health.Acquire(context.Background())
	if err := health.Complete(context.Background(), permit, &IntentPlannerTransportFailure{Err: errors.New(raw)}); err != nil {
		t.Fatalf("Complete: %v", err)
	}
	persisted := readIntentHealthRecord(t, db)
	if persisted.ProviderFingerprint != fingerprint || persisted.LastError != clean {
		t.Fatalf("persisted fingerprint/error mismatch: %+v", persisted)
	}
	payload, err := json.Marshal(persisted)
	if err != nil {
		t.Fatalf("marshal persisted health: %v", err)
	}
	for _, field := range []string{
		`"next_probe_ts"`, `"consecutive_failures"`, `"last_failure_class"`, `"bypass_count"`,
	} {
		if !bytes.Contains(payload, []byte(field)) {
			t.Fatalf("persisted JSON missing %s: %s", field, payload)
		}
	}
	for _, stale := range []string{`"retry_at"`, `"consecutive_validation_failures"`, `"last_failure_kind"`} {
		if bytes.Contains(payload, []byte(stale)) {
			t.Fatalf("persisted JSON contains stale field %s: %s", stale, payload)
		}
	}
}

func TestDecodeIntentPlannerHealthSnapshot(t *testing.T) {
	fingerprint := IntentPlannerProviderFingerprint(openAIIntentHealthIdentity("https://planner.example/v1"))
	raw, err := json.Marshal(intentPlannerHealthRecord{
		Version: intentPlannerHealthVersion,
		IntentPlannerHealthSnapshot: IntentPlannerHealthSnapshot{
			State:               IntentPlannerCircuitOpen,
			ProviderFingerprint: fingerprint,
			ConsecutiveFailures: 1,
			BackoffLevel:        1,
			LastFailureClass:    IntentPlannerFailureTransport,
			LastError:           "Authorization: Bearer secret-value",
		},
	})
	if err != nil {
		t.Fatalf("marshal health: %v", err)
	}
	snapshot, err := DecodeIntentPlannerHealthSnapshot(string(raw))
	if err != nil {
		t.Fatalf("DecodeIntentPlannerHealthSnapshot: %v", err)
	}
	if snapshot.State != IntentPlannerCircuitOpen || snapshot.ProviderFingerprint != fingerprint {
		t.Fatalf("snapshot=%+v", snapshot)
	}
	if strings.Contains(snapshot.LastError, "secret-value") {
		t.Fatalf("decoded snapshot leaked secret: %q", snapshot.LastError)
	}

	for _, tc := range []struct {
		name string
		raw  string
		want error
	}{
		{name: "malformed", raw: `{"version":1,"state":`, want: ErrIntentPlannerHealthInvalidRecord},
		{name: "unsupported", raw: `{"version":99,"state":"open"}`, want: ErrIntentPlannerHealthUnsupportedVersion},
		{name: "unsafe fingerprint", raw: `{"version":1,"state":"open","provider_fingerprint":"api-key-value"}`, want: ErrIntentPlannerHealthInvalidRecord},
		{name: "unknown state", raw: `{"version":1,"state":"confused","provider_fingerprint":"` + fingerprint + `"}`, want: ErrIntentPlannerHealthInvalidRecord},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := DecodeIntentPlannerHealthSnapshot(tc.raw); !errors.Is(err, tc.want) {
				t.Fatalf("error=%v want %v", err, tc.want)
			}
		})
	}
}
