// poll_scheduler.go implements the run-loop's idle/error backoff per §8.6.
//
// Pure function: no goroutine ownership; the run loop owns timers. The
// scheduler simply tells the loop "given this current delay, what should
// the next delay be on idle/error/success?".
//
// Defaults:
//   - Base:          750ms
//   - IdleCeiling:   30s
//   - ErrorCeiling:  60s
//
// Tests can construct a Scheduler with smaller bases/ceilings to keep the
// suite fast (Base = 10ms etc.).
package daemon

import "time"

// Default scheduler knobs per §8.6.
const (
	DefaultSchedulerBase         = 750 * time.Millisecond
	DefaultSchedulerIdleCeiling  = 30 * time.Second
	DefaultSchedulerErrorCeiling = 60 * time.Second
)

// Scheduler is the run-loop's pluggable backoff helper. Zero-valued fields
// fall back to the production defaults so production callers can leave the
// struct empty.
type Scheduler struct {
	Base         time.Duration
	IdleCeiling  time.Duration
	ErrorCeiling time.Duration
}

// resolved returns the effective Base/IdleCeiling/ErrorCeiling triple,
// substituting defaults for zero-valued fields. Negative fields are also
// treated as "use default" — callers should never see a sub-zero delay.
func (s Scheduler) resolved() (time.Duration, time.Duration, time.Duration) {
	base := s.Base
	if base <= 0 {
		base = DefaultSchedulerBase
	}
	idle := s.IdleCeiling
	if idle <= 0 {
		idle = DefaultSchedulerIdleCeiling
	}
	errC := s.ErrorCeiling
	if errC <= 0 {
		errC = DefaultSchedulerErrorCeiling
	}
	return base, idle, errC
}

// Reset returns the base interval. Used after a successful loop body or a
// wake — the next sleep starts fresh from Base regardless of how much we
// had backed off previously.
func (s Scheduler) Reset() time.Duration {
	base, _, _ := s.resolved()
	return base
}

// NextIdle doubles the current delay, capped at IdleCeiling. A current
// delay <= 0 starts from Base.
func (s Scheduler) NextIdle(current time.Duration) time.Duration {
	base, idle, _ := s.resolved()
	if current <= 0 {
		return base
	}
	next := current * 2
	if next > idle {
		next = idle
	}
	if next < base {
		next = base
	}
	return next
}

// NextError doubles the current delay, capped at ErrorCeiling. A current
// delay <= 0 starts from Base.
func (s Scheduler) NextError(current time.Duration) time.Duration {
	base, _, errC := s.resolved()
	if current <= 0 {
		return base
	}
	next := current * 2
	if next > errC {
		next = errC
	}
	if next < base {
		next = base
	}
	return next
}
