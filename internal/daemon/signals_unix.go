//go:build unix

// signals_unix.go installs SIGTERM/SIGINT/SIGUSR1 handlers and exposes the
// channels the run loop uses to coalesce wakes and observe shutdown.
//
// SIGUSR1 wakes are coalesced — the run loop only needs to know "wake at
// least once", so a buffered channel of capacity 1 is sufficient and we
// drop duplicate sends when the buffer is full. Mirrors the legacy
// snapshot-daemon's wake_event Event semantics.
//
// SIGTERM/SIGINT both flow through the same Shutdown channel; the run loop
// treats them identically (graceful drain + return).
package daemon

import (
	"context"
	"os"
	"os/signal"
	"syscall"
)

// Signals carries the channels the run loop selects on.
//
// Wake fires (at least once) on each SIGUSR1 received. Shutdown fires once
// on the first SIGTERM or SIGINT. Both channels are read-only from the
// run-loop's perspective.
type Signals struct {
	Wake     <-chan struct{}
	Shutdown <-chan struct{}
}

// InstallSignalHandlers registers signal.Notify on SIGTERM, SIGINT, and
// SIGUSR1, returning Signals (the receive channels) plus a cleanup func
// that detaches the handlers and stops the dispatcher goroutine.
//
// Honors ctx: when ctx.Done fires, the dispatcher exits.
func InstallSignalHandlers(ctx context.Context) (*Signals, func()) {
	wakeOut := make(chan struct{}, 1)
	shutdownOut := make(chan struct{}, 1)

	rawSig := make(chan os.Signal, 8)
	signal.Notify(rawSig, syscall.SIGTERM, syscall.SIGINT, syscall.SIGUSR1)

	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			select {
			case <-ctx.Done():
				return
			case sig, ok := <-rawSig:
				if !ok {
					return
				}
				switch sig {
				case syscall.SIGUSR1:
					// Coalesce: drop duplicate wakes when the
					// buffer is already full.
					select {
					case wakeOut <- struct{}{}:
					default:
					}
				case syscall.SIGTERM, syscall.SIGINT:
					// Coalesce shutdown too — once is enough.
					select {
					case shutdownOut <- struct{}{}:
					default:
					}
					// Also wake the loop so it exits the
					// idle sleep immediately.
					select {
					case wakeOut <- struct{}{}:
					default:
					}
				}
			}
		}
	}()

	cleanup := func() {
		signal.Stop(rawSig)
		close(rawSig)
		<-done
	}
	return &Signals{Wake: wakeOut, Shutdown: shutdownOut}, cleanup
}
