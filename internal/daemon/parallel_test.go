package daemon

import "testing"

const maxParallelDaemonTests = 4

var parallelDaemonTestSlots = make(chan struct{}, maxParallelDaemonTests)

// runBoundedParallel keeps the daemon package's Git-heavy top-level tests
// parallel without allowing them to exhaust process resources under -race.
// Acquire after t.Parallel so sequential tests are never blocked by a slot.
func runBoundedParallel(t *testing.T) {
	t.Helper()
	t.Parallel()

	parallelDaemonTestSlots <- struct{}{}
	t.Cleanup(func() {
		<-parallelDaemonTestSlots
	})
}
