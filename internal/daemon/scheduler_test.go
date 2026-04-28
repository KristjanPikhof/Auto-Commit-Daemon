package daemon

import (
	"testing"
	"time"
)

// TestScheduler_IdleDoublingCaps: NextIdle doubles the input until it hits
// IdleCeiling, then clamps.
func TestScheduler_IdleDoublingCaps(t *testing.T) {
	s := Scheduler{
		Base:         100 * time.Millisecond,
		IdleCeiling:  1 * time.Second,
		ErrorCeiling: 5 * time.Second,
	}
	got := s.Reset()
	if got != 100*time.Millisecond {
		t.Fatalf("Reset=%v want 100ms", got)
	}
	want := []time.Duration{
		200 * time.Millisecond,
		400 * time.Millisecond,
		800 * time.Millisecond,
		1 * time.Second, // capped
		1 * time.Second, // stays capped
	}
	for _, w := range want {
		got = s.NextIdle(got)
		if got != w {
			t.Fatalf("NextIdle: got %v want %v", got, w)
		}
	}
}

// TestScheduler_ErrorDoublingCaps: NextError caps at ErrorCeiling.
func TestScheduler_ErrorDoublingCaps(t *testing.T) {
	s := Scheduler{
		Base:         100 * time.Millisecond,
		IdleCeiling:  1 * time.Second,
		ErrorCeiling: 500 * time.Millisecond,
	}
	got := s.Reset()
	want := []time.Duration{
		200 * time.Millisecond,
		400 * time.Millisecond,
		500 * time.Millisecond, // capped
		500 * time.Millisecond,
	}
	for _, w := range want {
		got = s.NextError(got)
		if got != w {
			t.Fatalf("NextError: got %v want %v", got, w)
		}
	}
}

// TestScheduler_ResetReturnsBase: Reset always returns Base regardless of
// how deep the loop had backed off.
func TestScheduler_ResetReturnsBase(t *testing.T) {
	s := Scheduler{
		Base:         50 * time.Millisecond,
		IdleCeiling:  10 * time.Second,
		ErrorCeiling: 10 * time.Second,
	}
	if got := s.Reset(); got != 50*time.Millisecond {
		t.Fatalf("Reset=%v want 50ms", got)
	}
	// After many idle ticks, Reset still drops back to Base.
	d := s.Reset()
	for i := 0; i < 20; i++ {
		d = s.NextIdle(d)
	}
	if d == 50*time.Millisecond {
		t.Fatalf("expected backoff to grow, got Base")
	}
	if got := s.Reset(); got != 50*time.Millisecond {
		t.Fatalf("Reset after backoff=%v want 50ms", got)
	}
}

// TestScheduler_ZeroFieldsUseDefaults: empty Scheduler{} returns the
// production defaults.
func TestScheduler_ZeroFieldsUseDefaults(t *testing.T) {
	s := Scheduler{}
	if got := s.Reset(); got != DefaultSchedulerBase {
		t.Fatalf("default Reset=%v want %v", got, DefaultSchedulerBase)
	}
	d := s.Reset()
	for i := 0; i < 50; i++ {
		d = s.NextIdle(d)
	}
	if d != DefaultSchedulerIdleCeiling {
		t.Fatalf("idle ceiling=%v want %v", d, DefaultSchedulerIdleCeiling)
	}
}
