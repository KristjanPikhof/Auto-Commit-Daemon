package daemon

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	checkpointpkg "github.com/KristjanPikhof/Auto-Commit-Daemon/internal/checkpoint"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

func finishProtectionWait(t *testing.T, ready func() bool) {
	t.Helper()
	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		if ready() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("checkpoint milestone not reached")
}
func finishProtectionLatest(t *testing.T, f *captureFixture) *state.Checkpoint {
	t.Helper()
	p, err := state.ReadCheckpointProjection(context.Background(), f.db.Path(), 10)
	if err != nil {
		t.Fatal(err)
	}
	return p.Latest
}
func finishProtectionHas(t *testing.T, f *captureFixture, path, body string) bool {
	t.Helper()
	cp := finishProtectionLatest(t, f)
	if cp == nil {
		return false
	}
	return strings.TrimSpace(finishWorkerGit(t, f.dir, "show", cp.CommitOID+":"+path)) == strings.TrimSpace(body)
}
func finishProtectionRun(t *testing.T, f *captureFixture, change func(*Options)) (chan struct{}, func()) {
	t.Helper()
	wake := make(chan struct{}, 1)
	shutdown := make(chan struct{})
	result := make(chan error, 1)
	opts := Options{RepoPath: f.dir, GitDir: f.gitDir, DB: f.db, SkipSignals: true, WakeCh: wake, ShutdownCh: shutdown, Logger: slog.New(slog.NewTextHandler(io.Discard, nil)), BootGrace: time.Hour, Scheduler: Scheduler{Base: 15 * time.Millisecond, IdleCeiling: time.Hour, ErrorCeiling: 100 * time.Millisecond}}
	if change != nil {
		change(&opts)
	}
	go func() { result <- Run(context.Background(), opts) }()
	finishProtectionWait(t, func() bool { return finishProtectionLatest(t, f) != nil })
	var once sync.Once
	stop := func() {
		t.Helper()
		once.Do(func() {
			close(shutdown)
			select {
			case err := <-result:
				if err != nil {
					t.Errorf("stop: %v", err)
				}
			case <-time.After(12 * time.Second):
				t.Error("worker failed to stop")
			}
		})
	}
	t.Cleanup(stop)
	return wake, stop
}
func TestFinishProtectionSavesIdleAndStopNeverPublish(t *testing.T) {
	f := newCaptureFixture(t)
	head := finishWorkerGit(t, f.dir, "rev-parse", "HEAD")
	index := finishWorkerGit(t, f.dir, "ls-files", "--stage")
	var calls atomic.Int32
	wake, stop := finishProtectionRun(t, f, func(o *Options) {
		o.MessageFn = func(context.Context, EventContext) (string, error) { calls.Add(1); return "unexpected", nil }
	})
	for i := 0; i < 6; i++ {
		body := fmt.Sprintf("save %d", i)
		finishWrite(t, f.dir, "work.txt", body)
		wake <- struct{}{}
		finishProtectionWait(t, func() bool {
			cp := finishProtectionLatest(t, f)
			if cp == nil {
				return false
			}
			out := finishWorkerGit(t, f.dir, "ls-tree", "--name-only", cp.CommitOID)
			return strings.Contains(out, "work.txt") && finishProtectionHas(t, f, "work.txt", body)
		})
	}
	before := finishProtectionLatest(t, f).ID
	wake <- struct{}{}
	finishProtectionWait(t, func() bool {
		raw, _, _ := state.MetaGet(context.Background(), f.db, MetaKeyProtectionComplete)
		return raw == "true"
	})
	if finishProtectionLatest(t, f).ID != before {
		t.Fatal("unchanged wake churned checkpoint")
	}
	finishWrite(t, f.dir, "work.txt", "final stop bytes")
	stop()
	if !finishProtectionHas(t, f, "work.txt", "final stop bytes") {
		t.Fatal("stop did not protect last edit")
	}
	if got := finishWorkerGit(t, f.dir, "rev-parse", "HEAD"); got != head {
		t.Fatal("automatic branch publication", got)
	}
	if got := finishWorkerGit(t, f.dir, "ls-files", "--stage"); got != index {
		t.Fatal("automatic staging", got)
	}
	if calls.Load() != 0 {
		t.Fatal("automatic message/provider work")
	}
	for _, table := range []string{"capture_events", "shadow_paths", "finish_requests"} {
		var count int
		if err := f.db.ReadSQL().QueryRow("SELECT COUNT(*) FROM " + table).Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count != 0 {
			t.Fatalf("%s has %d rows", table, count)
		}
	}
	if raw, ok, _ := state.MetaGet(context.Background(), f.db, MetaKeyProtectionClassificationPending); ok && raw == "true" {
		t.Fatal("obsolete classification queue pending")
	}
	stamp, ok, err := state.LoadDaemonState(context.Background(), f.db)
	if err != nil || !ok || stamp.Mode != "stopped" {
		t.Fatalf("stop stamp %+v %v", stamp, err)
	}
}
func TestFinishProtectionUnsafeGitAndHeadTransitions(t *testing.T) {
	f := newCaptureFixture(t)
	wake, stop := finishProtectionRun(t, f, nil)
	finishWorkerGit(t, f.dir, "checkout", "--detach", "-q")
	finishWrite(t, f.dir, "detached.txt", "protected")
	wake <- struct{}{}
	finishProtectionWait(t, func() bool { cp := finishProtectionLatest(t, f); return cp != nil && cp.ObservedRef == "" })
	if !finishProtectionHas(t, f, "detached.txt", "protected") {
		t.Fatal("detached edit missing")
	}
	finishWorkerGit(t, f.dir, "switch", "-c", "next", "-q")
	if err := os.WriteFile(filepath.Join(f.gitDir, "MERGE_HEAD"), []byte(f.cctx.BaseHead+"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	finishWrite(t, f.dir, "conflicted.txt", "still protected")
	wake <- struct{}{}
	finishProtectionWait(t, func() bool {
		cp := finishProtectionLatest(t, f)
		return cp != nil && cp.ObservedRef == "refs/heads/next"
	})
	if !finishProtectionHas(t, f, "conflicted.txt", "still protected") {
		t.Fatal("unsafe Git stopped protection")
	}
	stop()
}
func TestFinishProtectionRestartReuseAndRequiredBarrier(t *testing.T) {
	f := newCaptureFixture(t)
	_, stop := finishProtectionRun(t, f, nil)
	stop()
	before := finishProtectionLatest(t, f).ID
	wake, stopAgain := finishProtectionRun(t, f, func(o *Options) { o.FinishRecoveryDone = true })
	if finishProtectionLatest(t, f).ID != before {
		t.Fatal("restart churned unchanged checkpoint")
	}
	if err := RequireProtectionCheckpoint(context.Background(), f.db, checkpointpkg.WorktreeID(f.dir), 1); err != nil {
		t.Fatal(err)
	}
	wake <- struct{}{}
	finishProtectionWait(t, func() bool { return finishProtectionLatest(t, f).ID != before })
	stopAgain()
}
func TestFinishProtectionStreamsLargeFilesAndExcludesPrivateFiles(t *testing.T) {
	f := newCaptureFixture(t)
	finishWrite(t, f.dir, "large.txt", strings.Repeat("large content\n", 10000))
	finishWrite(t, f.dir, ".env", "SECRET=value")
	finishWrite(t, f.dir, ".gitignore", "ignored.txt\n")
	finishWrite(t, f.dir, "ignored.txt", "ignored")
	_, stop := finishProtectionRun(t, f, func(o *Options) { o.MaxFileBytes = 8 })
	stop()
	cp := finishProtectionLatest(t, f)
	names := finishWorkerGit(t, f.dir, "ls-tree", "--name-only", cp.CommitOID)
	if !strings.Contains(names, "large.txt") || strings.Contains(names, ".env\n") || strings.Contains(names, "ignored.txt") {
		t.Fatalf("checkpoint scope %s", names)
	}
	if !finishProtectionHas(t, f, "large.txt", strings.Repeat("large content\n", 10000)) {
		t.Fatal("large file truncated")
	}
	if cp.Partial {
		t.Fatal("privacy exclusions treated as partial")
	}
}
func TestFinishProtectionWatcherWakeAndPollingFallback(t *testing.T) {
	for _, fallback := range []bool{false, true} {
		t.Run(fmt.Sprint(fallback), func(t *testing.T) {
			if fallback {
				t.Setenv(EnvDisableFsnotify, "1")
			}
			f := newCaptureFixture(t)
			_, stop := finishProtectionRun(t, f, func(o *Options) {
				o.FsnotifyEnabled = true
				o.FsnotifyDebounce = 10 * time.Millisecond
				if fallback {
					o.Scheduler.IdleCeiling = 80 * time.Millisecond
				}
			})
			finishProtectionWait(t, func() bool {
				mode, _, _ := state.MetaGet(context.Background(), f.db, "fsnotify.mode")
				return mode == map[bool]string{false: "fsnotify", true: "poll"}[fallback]
			})
			finishWrite(t, f.dir, "watch.txt", "wake without explicit signal")
			finishProtectionWait(t, func() bool {
				cp := finishProtectionLatest(t, f)
				if cp == nil {
					return false
				}
				names := finishWorkerGit(t, f.dir, "ls-tree", "--name-only", cp.CommitOID)
				return strings.Contains(names, "watch.txt")
			})
			if fallback {
				reason, _, _ := state.MetaGet(context.Background(), f.db, "fsnotify.fallback_reason")
				if reason != FallbackDisabled {
					t.Fatalf("fallback reason %q", reason)
				}
			}
			stop()
		})
	}
}
func TestFinishProtectionCanonicalOwnershipAndSharedScanGate(t *testing.T) {
	f := newCaptureFixture(t)
	lock, err := AcquireDaemonLock(f.gitDir)
	if err != nil {
		t.Fatal(err)
	}
	err = Run(context.Background(), Options{RepoPath: f.dir, GitDir: f.gitDir, DB: f.db, SkipSignals: true})
	if err == nil {
		t.Fatal("second owner accepted")
	}
	lock.Release()
	gate := &sync.Mutex{}
	wake, stop := finishProtectionRun(t, f, func(o *Options) { o.ProtectionGate = gate; o.progressHeartbeatEvery = 20 * time.Millisecond })
	gate.Lock()
	before := finishProtectionLatest(t, f).ID
	stamp, _, _ := state.LoadDaemonState(context.Background(), f.db)
	finishWrite(t, f.dir, "serialized.txt", "new")
	wake <- struct{}{}
	finishProtectionWait(t, func() bool {
		current, _, _ := state.LoadDaemonState(context.Background(), f.db)
		return current.HeartbeatTS > stamp.HeartbeatTS
	})
	if finishProtectionLatest(t, f).ID != before {
		t.Fatal("scan ignored protection gate")
	}
	gate.Unlock()
	finishProtectionWait(t, func() bool { return finishProtectionLatest(t, f).ID != before })
	stop()
}

func TestFinishProtectionPartialCheckpointRecoversWithoutQueue(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("unreadable-file scenario requires ordinary user permissions")
	}
	f := newCaptureFixture(t)
	finishWrite(t, f.dir, "readable.txt", "durable")
	finishWrite(t, f.dir, "unreadable.txt", "recover after permissions fixed")
	path := filepath.Join(f.dir, "unreadable.txt")
	if err := os.Chmod(path, 0); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(path, 0600) })
	wake, stop := finishProtectionRun(t, f, nil)
	finishProtectionWait(t, func() bool { cp := finishProtectionLatest(t, f); return cp != nil && cp.Partial })
	cp := finishProtectionLatest(t, f)
	if len(cp.CaptureIssues) == 0 {
		t.Fatal("partial checkpoint omitted issues")
	}
	complete, _, _ := state.MetaGet(context.Background(), f.db, MetaKeyProtectionComplete)
	if complete != "false" {
		t.Fatal("partial checkpoint reported complete")
	}
	if !finishProtectionHas(t, f, "readable.txt", "durable") {
		t.Fatal("readable bytes not protected")
	}
	if err := os.Chmod(path, 0600); err != nil {
		t.Fatal(err)
	}
	wake <- struct{}{}
	finishProtectionWait(t, func() bool { cp := finishProtectionLatest(t, f); return cp != nil && !cp.Partial })
	if !finishProtectionHas(t, f, "unreadable.txt", "recover after permissions fixed") {
		t.Fatal("coverage did not recover")
	}
	stop()
	if len(pendingOps(t, f.db)) != 0 {
		t.Fatal("partial recovery created publication queue")
	}
}
