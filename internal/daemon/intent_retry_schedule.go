package daemon

import "time"

// Filesystem wakeups remain immediate. Idle polling must not sleep past a
// scheduled provider probe; an already due probe uses normal backoff.
func intentProviderRetryDelay(delay time.Duration, health IntentPlannerHealthSnapshot, now time.Time) time.Duration {
	if health.State != IntentPlannerCircuitOpen || health.NextProbeTS <= 0 {
		return delay
	}
	remaining := time.Unix(0, int64(health.NextProbeTS*float64(time.Second))).Sub(now)
	if remaining > 0 && remaining < delay {
		return remaining
	}
	return delay
}
