package daemon

import (
	"testing"
	"time"
)

func TestIntentProviderRetrySchedulingHonorsDueProbeWithoutBusyLoop(t *testing.T) {
	now := time.Unix(1000, 0)
	health := IntentPlannerHealthSnapshot{State: IntentPlannerCircuitOpen, NextProbeTS: 1015}
	if delay := intentProviderRetryDelay(2*time.Minute, health, now); delay != 15*time.Second {
		t.Fatalf("missed due probe: %v", delay)
	}
	if delay := intentProviderRetryDelay(time.Second, health, now); delay != time.Second {
		t.Fatalf("delayed capture polling: %v", delay)
	}
	if delay := intentProviderRetryDelay(2*time.Minute, health, now.Add(time.Minute)); delay != 2*time.Minute {
		t.Fatalf("busy loop after deadline: %v", delay)
	}
	health.State = IntentPlannerCircuitClosed
	if delay := intentProviderRetryDelay(time.Minute, health, now); delay != time.Minute {
		t.Fatalf("stale deadline woke recovered provider: %v", delay)
	}
}
