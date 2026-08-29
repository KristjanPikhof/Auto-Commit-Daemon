package cli

import (
	"context"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/daemon"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/supervisor"
)

func TestWorkerActivityHintCoalescerPreservesLeadingAndTrailingWakes(t *testing.T) {
	wakes := make(chan string, 8)
	coalescer := newWorkerActivityHintCoalescer(
		40*time.Millisecond, 200*time.Millisecond,
		func(worktreeID string) { wakes <- worktreeID },
	)
	t.Cleanup(coalescer.Close)

	coalescer.Hint("worktree")
	assertWorkerWake(t, wakes, "worktree", 100*time.Millisecond)
	coalescer.Hint("worktree")
	coalescer.Hint("worktree")
	assertNoWorkerWake(t, wakes, 15*time.Millisecond)
	assertWorkerWake(t, wakes, "worktree", 250*time.Millisecond)
	assertNoWorkerWake(t, wakes, 80*time.Millisecond)
}

func TestWorkerActivityHintCoalescerClampsTrailingWake(t *testing.T) {
	wakes := make(chan string, 2)
	coalescer := newWorkerActivityHintCoalescer(
		500*time.Millisecond, 40*time.Millisecond,
		func(worktreeID string) { wakes <- worktreeID },
	)
	t.Cleanup(coalescer.Close)

	coalescer.Hint("worktree")
	assertWorkerWake(t, wakes, "worktree", 100*time.Millisecond)
	assertWorkerWake(t, wakes, "worktree", 200*time.Millisecond)
}

func TestWorkerActivityHintCoalescerCloseCancelsTail(t *testing.T) {
	wakes := make(chan string, 2)
	coalescer := newWorkerActivityHintCoalescer(
		30*time.Millisecond, 60*time.Millisecond,
		func(worktreeID string) { wakes <- worktreeID },
	)
	coalescer.Hint("worktree")
	assertWorkerWake(t, wakes, "worktree", 100*time.Millisecond)
	coalescer.Close()
	coalescer.Hint("worktree")
	assertNoWorkerWake(t, wakes, 100*time.Millisecond)
}

func TestWorkerActivityHintCoalescerConcurrentBurst(t *testing.T) {
	var wakes atomic.Int64
	coalescer := newWorkerActivityHintCoalescer(
		5*time.Second, 10*time.Second,
		func(string) { wakes.Add(1) },
	)
	start := make(chan struct{})
	var calls sync.WaitGroup
	for range 64 {
		calls.Add(1)
		go func() {
			defer calls.Done()
			<-start
			coalescer.Hint("worktree")
		}()
	}
	close(start)
	calls.Wait()
	coalescer.Close()
	if got := wakes.Load(); got != 1 {
		t.Fatalf("concurrent burst wakes=%d want one leading wake", got)
	}
}

func TestWorkerHintBeginsEveryObservationBeforeCoalescedWake(t *testing.T) {
	ctx := context.Background()
	db, err := state.Open(ctx, filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	wakes := make(chan string, 4)
	coalescer := newWorkerActivityHintCoalescer(
		100*time.Millisecond, 500*time.Millisecond,
		func(worktreeID string) { wakes <- worktreeID },
	)
	t.Cleanup(coalescer.Close)
	handler := repositoryWorkerHandler{
		runtimes:      map[string]*workerRuntime{"worktree": {db: db}},
		wake:          func(worktreeID string) { wakes <- worktreeID },
		activityHints: coalescer,
	}

	if _, protocolErr := handler.HandleWorkerRequest(ctx, supervisor.Request{
		Method: "hint", WorktreeID: "worktree",
	}); protocolErr != nil {
		t.Fatal(protocolErr)
	}
	assertWorkerWake(t, wakes, "worktree", 100*time.Millisecond)
	firstEpoch := protectionObservationEpoch(t, ctx, db)
	if err := state.MetaSet(ctx, db, daemon.MetaKeyProtectionCoveredEpoch, strconv.FormatInt(firstEpoch, 10)); err != nil {
		t.Fatal(err)
	}
	if err := state.MetaSet(ctx, db, daemon.MetaKeyProtectionComplete, "true"); err != nil {
		t.Fatal(err)
	}

	if _, protocolErr := handler.HandleWorkerRequest(ctx, supervisor.Request{
		Method: "hint", WorktreeID: "worktree",
	}); protocolErr != nil {
		t.Fatal(protocolErr)
	}
	if secondEpoch := protectionObservationEpoch(t, ctx, db); secondEpoch != firstEpoch+1 {
		t.Fatalf("second accepted observation epoch=%d want %d", secondEpoch, firstEpoch+1)
	}
	if complete, ok, err := state.MetaGet(ctx, db, daemon.MetaKeyProtectionComplete); err != nil || !ok || complete != "false" {
		t.Fatalf("second accepted observation complete=%q ok=%t err=%v", complete, ok, err)
	}
	assertNoWorkerWake(t, wakes, 20*time.Millisecond)
	assertWorkerWake(t, wakes, "worktree", 250*time.Millisecond)
}

func protectionObservationEpoch(t *testing.T, ctx context.Context, db *state.DB) int64 {
	t.Helper()
	raw, ok, err := state.MetaGet(ctx, db, daemon.MetaKeyProtectionObservationEpoch)
	if err != nil || !ok {
		t.Fatalf("protection observation epoch=%q ok=%t err=%v", raw, ok, err)
	}
	epoch, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		t.Fatal(err)
	}
	return epoch
}

func assertWorkerWake(t *testing.T, wakes <-chan string, want string, timeout time.Duration) {
	t.Helper()
	select {
	case got := <-wakes:
		if got != want {
			t.Fatalf("wake target=%q want %q", got, want)
		}
	case <-time.After(timeout):
		t.Fatalf("worker wake did not arrive within %s", timeout)
	}
}

func assertNoWorkerWake(t *testing.T, wakes <-chan string, duration time.Duration) {
	t.Helper()
	select {
	case got := <-wakes:
		t.Fatalf("unexpected worker wake for %q", got)
	case <-time.After(duration):
	}
}
