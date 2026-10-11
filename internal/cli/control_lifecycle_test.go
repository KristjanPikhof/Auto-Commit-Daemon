package cli

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/paths"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/supervisor"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/version"
)

type controlTestRPC func(context.Context, supervisor.Request) (any, *supervisor.ProtocolError)

func (f controlTestRPC) HandleWorkerRequest(ctx context.Context, request supervisor.Request) (any, *supervisor.ProtocolError) {
	return f(ctx, request)
}

func serveControlTestRPC(t *testing.T, socket string, handler controlTestRPC) {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- supervisor.ServeWorker(ctx, socket, handler) }()
	t.Cleanup(func() {
		cancel()
		if err := <-done; err != nil {
			t.Errorf("isolated RPC server: %v", err)
		}
	})
	readyCtx, stop := context.WithTimeout(t.Context(), time.Second)
	defer stop()
	for {
		if _, err := os.Stat(socket); err == nil {
			return
		}
		select {
		case <-readyCtx.Done():
			t.Fatal("isolated RPC socket did not appear")
		case <-time.After(time.Millisecond):
		}
	}
}

func controlLifecycleFixture(t *testing.T) (controlRepoLookup, *workerRuntime, *repositoryWorkerHandler) {
	t.Helper()
	_ = withIsolatedHome(t)
	sockets, err := os.MkdirTemp("/tmp", "acd-on-test-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(sockets) })
	t.Setenv("XDG_STATE_HOME", sockets)
	roots, err := paths.Resolve()
	if err != nil {
		t.Fatal(err)
	}
	repo := makeUnregisteredStartRepo(t)
	commitStartRepoSeed(t, repo)
	registerProtectedControlRepo(t, roots, repo)
	lookup, err := loadControlRepo(t.Context(), repo)
	if err != nil {
		t.Fatal(err)
	}
	db, err := state.Open(t.Context(), lookup.Record.StateDB)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	runtime := &workerRuntime{record: lookup.Record, worktree: lookup.Worktree, db: db, roots: roots, gate: &sync.RWMutex{}, protectionGate: &sync.Mutex{}}
	handler := &repositoryWorkerHandler{repositoryID: lookup.Record.RepositoryID, runtimes: map[string]*workerRuntime{lookup.Record.WorktreeID: runtime}}
	handler.wake = func(string) { protectBarrierFixture(t, t.Context(), lookup, runtime) }
	protectBarrierFixture(t, t.Context(), lookup, runtime)
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Dir(roots.ManagedBinaryPath()), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(executable, roots.ManagedBinaryPath()); err != nil {
		t.Fatal(err)
	}
	return lookup, runtime, handler
}

func TestFinishControlOnReusesWorkerAndWaitsForStartup(t *testing.T) {
	for _, scenario := range []string{"healthy", "delayed-startup", "legacy-attention", "missing-worktree"} {
		t.Run(scenario, func(t *testing.T) {
			lookup, runtime, handler := controlLifecycleFixture(t)
			ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
			defer cancel()
			if scenario == "legacy-attention" {
				if _, err := state.RecordFinishLegacyActivationFailure(ctx, runtime.db, "legacy_activation_required", "Retry explicit activation", "", ""); err != nil {
					t.Fatal(err)
				}
			}
			head, _ := git.RevParse(ctx, lookup.Worktree.Root, "HEAD")
			index, _ := git.IndexContentDigest(ctx, lookup.Worktree.Root)
			if err := os.WriteFile(filepath.Join(lookup.Worktree.Root, "current-work.txt"), []byte("new current work\n"), 0600); err != nil {
				t.Fatal(err)
			}
			var restarting, checkpointed atomic.Int32
			var ready, worktreeLoaded atomic.Bool
			ready.Store(scenario != "delayed-startup")
			worktreeLoaded.Store(scenario != "missing-worktree")
			startupDone := make(chan struct{})
			if scenario == "delayed-startup" {
				// Model startup taking longer than the former ten-second CLI limit.
				go func() {
					defer close(startupDone)
					timer := time.NewTimer(11 * time.Second)
					defer timer.Stop()
					select {
					case <-ctx.Done():
					case <-timer.C:
						ready.Store(true)
					}
				}()
				t.Cleanup(func() { cancel(); <-startupDone })
			}
			serveControlTestRPC(t, supervisor.WorkerSocketPath(lookup.Roots, lookup.Record.RepositoryID), func(ctx context.Context, request supervisor.Request) (any, *supervisor.ProtocolError) {
				if request.Method == "status" && request.WorktreeID == "" {
					return supervisor.WorkerReadiness{RepositoryID: lookup.Record.RepositoryID, PID: os.Getpid(), Ready: ready.Load()}, nil
				}
				if !worktreeLoaded.Load() {
					return nil, &supervisor.ProtocolError{Code: "worktree_not_found", Message: "Worktree has not been loaded"}
				}
				return handler.HandleWorkerRequest(ctx, request)
			})
			executable, _ := os.Executable()
			digest, err := version.FileDigest(executable)
			if err != nil {
				t.Fatal(err)
			}
			serveControlTestRPC(t, lookup.Roots.SupervisorSocketPath(), func(ctx context.Context, request supervisor.Request) (any, *supervisor.ProtocolError) {
				switch request.Method {
				case "status":
					return supervisor.Status{Version: version.String(), BinaryDigest: digest, Ownership: fmt.Sprintf("user:%d", os.Getuid()), Compatibility: runtimeCompatibility(), Workers: []supervisor.WorkerStatus{{RepositoryID: lookup.Record.RepositoryID, PID: os.Getpid(), State: "running"}}}, nil
				case "enable_repository":
					return map[string]any{"enabled": true}, nil
				case "restart_repository":
					restarting.Add(1)
					worktreeLoaded.Store(true)
					return map[string]any{"released": true}, nil
				case "checkpoint_barrier":
					checkpointed.Add(1)
					if err := state.SaveDaemonState(ctx, runtime.db, state.DaemonState{PID: os.Getpid(), Mode: "running", HeartbeatTS: nowFloat(), UpdatedTS: nowFloat()}); err != nil {
						return nil, &supervisor.ProtocolError{Code: "heartbeat_failed", Message: err.Error()}
					}
					return handler.HandleWorkerRequest(ctx, request)
				default:
					return nil, &supervisor.ProtocolError{Code: "unexpected_method", Message: request.Method}
				}
			})
			var out bytes.Buffer
			if err := runControlOn(ctx, &out, lookup.Worktree.Root, true); err != nil {
				t.Fatalf("on: %v\n%s", err, out.String())
			}
			result := decodeControlResult(t, out.Bytes())
			wantRestarts := int32(0)
			if scenario == "missing-worktree" {
				wantRestarts = 1
			}
			if restarting.Load() != wantRestarts || checkpointed.Load() != 1 || !result.OK || !result.Protected {
				t.Fatalf("restarts=%d barriers=%d result=%+v", restarting.Load(), checkpointed.Load(), result)
			}
			inspection, err := inspectControl(ctx, lookup.Worktree.Root)
			if err != nil || inspection.DaemonPID != os.Getpid() {
				t.Fatalf("owned worker after on: pid=%d: %v", inspection.DaemonPID, err)
			}
			assertBarrierNeverPublishes(t, lookup, runtime, head, index)
			projection, err := state.ReadCheckpointProjection(ctx, lookup.Record.StateDB, 1)
			if err != nil || projection.Latest == nil {
				t.Fatalf("current checkpoint: %v", err)
			}
			contents, err := git.Run(ctx, git.RunOpts{Dir: lookup.Worktree.Root}, "show", projection.Latest.CommitOID+":current-work.txt")
			if err != nil || string(contents) != "new current work\n" {
				t.Fatalf("protected current bytes=%q: %v", contents, err)
			}
			if scenario == "legacy-attention" {
				legacy, err := state.LoadFinishLegacyCutover(ctx, runtime.db)
				if err != nil || legacy.State != "needs_attention" {
					t.Fatalf("archived diagnostic changed: state=%s: %v", legacy.State, err)
				}
			}
		})
	}
}

func TestFinishControlReadinessRequiresOwnedIdentityAndHonorsCancellation(t *testing.T) {
	lookup, _, _ := controlLifecycleFixture(t)
	worker := supervisor.WorkerStatus{RepositoryID: lookup.Record.RepositoryID, PID: os.Getpid(), State: "running"}
	var reply atomic.Pointer[supervisor.WorkerReadiness]
	reply.Store(&supervisor.WorkerReadiness{RepositoryID: worker.RepositoryID, PID: worker.PID, Ready: true})
	serveControlTestRPC(t, supervisor.WorkerSocketPath(lookup.Roots, worker.RepositoryID), func(context.Context, supervisor.Request) (any, *supervisor.ProtocolError) { return *reply.Load(), nil })
	for _, wrong := range []supervisor.WorkerReadiness{
		{RepositoryID: "another-repository", PID: worker.PID, Ready: true},
		{RepositoryID: worker.RepositoryID, PID: worker.PID + 1, Ready: true},
		{RepositoryID: worker.RepositoryID, PID: worker.PID, Ready: false},
	} {
		reply.Store(&wrong)
		if controlWorkerReady(t.Context(), lookup, worker) {
			t.Fatalf("accepted wrong worker identity: %+v", wrong)
		}
	}
	reply.Store(&supervisor.WorkerReadiness{RepositoryID: worker.RepositoryID, PID: worker.PID, Ready: true})
	if !controlWorkerReady(t.Context(), lookup, worker) {
		t.Fatal("exact owned readiness rejected")
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := waitControlWorkerReady(ctx, lookup, supervisor.CheckpointBarrierTimeout, io.Discard); !errors.Is(err, context.Canceled) {
		t.Fatalf("startup cancellation: %v", err)
	}
}

func TestProductListTerminalInterruptKeepsCompletedSnapshot(t *testing.T) {
	original := productListCollect
	t.Cleanup(func() { productListCollect = original })
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	frames := 0
	productListCollect = func(context.Context) (productListData, productState, error) {
		frames++
		if frames == 2 {
			cancel()
			return productListData{UpdatedAt: "partial refresh"}, productStateProtected, ctx.Err()
		}
		return productListData{UpdatedAt: "completed snapshot"}, productStateProtected, nil
	}
	var out bytes.Buffer
	if err := runProductListWatchDisplay(ctx, &out, time.Millisecond, false, false, true); err != nil {
		t.Fatal(err)
	}
	restored := strings.Split(out.String(), "\033[?1049l")
	if len(restored) != 2 || !strings.Contains(restored[1], "completed snapshot") || !strings.Contains(restored[1], "No active repositories") || strings.Contains(out.String(), "partial refresh") {
		t.Fatalf("final terminal snapshot: %q", out.String())
	}
}
