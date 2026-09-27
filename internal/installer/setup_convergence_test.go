package installer

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/central"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/paths"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/supervisor"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/version"
)

func convergenceRoots(t *testing.T) paths.Roots {
	t.Helper()
	root, err := os.MkdirTemp("/tmp", "acd-converge-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	return paths.Roots{State: filepath.Join(root, "state"), Share: filepath.Join(root, "share"), Config: filepath.Join(root, "config")}
}

func TestCompatibleSetupNoopStillChecksProtection(t *testing.T) {
	if runtime.GOOS != "darwin" {
		t.Skip("compatible upgrades use macOS session ownership")
	}
	for _, unhealthy := range []bool{false, true} {
		name := "healthy"
		if unhealthy {
			name = "unhealthy"
		}
		t.Run(name, func(t *testing.T) {
			roots := convergenceRoots(t)
			executable := filepath.Join(roots.State, "source")
			if err := os.MkdirAll(roots.State, 0700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(executable, []byte("same binary"), 0700); err != nil {
				t.Fatal(err)
			}
			digest, err := version.FileDigest(executable)
			if err != nil {
				t.Fatal(err)
			}
			serveRuntimeStatus(t, roots, supervisor.Status{PID: os.Getpid(), Version: version.String(), BinaryDigest: digest, Compatibility: RuntimeCompatibility()})
			plan := Plan{Mode: "compatible_upgrade", OperationID: "noop", SourceExecutable: executable, Registry: central.NewRegistry()}
			plan.Digest = digestPlan(plan)
			sentinel := errors.New("checkpoint could not be confirmed")
			calls := 0
			result, err := Apply(context.Background(), roots, plan, ApplyOptions{Ready: func(_ context.Context, _ paths.Roots, got *central.Registry) error {
				calls++
				if got != plan.Registry {
					t.Fatal("readiness used a different registry")
				}
				if unhealthy {
					return sentinel
				}
				return nil
			}})
			if calls != 1 || result.Changed {
				t.Fatalf("readiness calls=%d result=%+v", calls, result)
			}
			if unhealthy && !errors.Is(err, sentinel) || !unhealthy && err != nil {
				t.Fatalf("Apply err=%v", err)
			}
		})
	}
}

func TestSetupReadinessVerifiesVersionAndCheckpoint(t *testing.T) {
	for _, scenario := range []struct {
		name, version string
		protected     bool
		want          string
	}{
		{"ready", version.String(), true, ""},
		{"wrong version", "outdated", true, "does not match"},
		{"unprotected", version.String(), false, "did not confirm checkpoint coverage"},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			roots := convergenceRoots(t)
			if err := os.MkdirAll(filepath.Dir(roots.ManagedBinaryPath()), 0700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(roots.ManagedBinaryPath(), []byte("installed"), 0700); err != nil {
				t.Fatal(err)
			}
			digest, err := version.FileDigest(roots.ManagedBinaryPath())
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			record := central.RepoRecord{Path: "/isolated-test-repo", RepositoryID: "1111111111111111", WorktreeID: "2222222222222222", StateDB: filepath.Join(roots.State, "fixture.db")}
			db, err := state.Open(ctx, record.StateDB)
			if err != nil {
				t.Fatal(err)
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			registry := central.NewRegistry()
			registry.Repos = []central.RepoRecord{record}
			var barriers atomic.Int32
			startReadinessServer(t, ctx, roots.SupervisorSocketPath(), func(request supervisor.Request) supervisor.Response {
				response := supervisor.Response{Version: supervisor.ProtocolVersion, ID: request.ID, OK: true}
				if request.Method == "checkpoint_barrier" {
					barriers.Add(1)
					response.Data = map[string]bool{"protected": scenario.protected}
					return response
				}
				response.Data = supervisor.Status{Version: scenario.version, BinaryDigest: digest, Compatibility: RuntimeCompatibility(), Workers: []supervisor.WorkerStatus{{RepositoryID: record.RepositoryID, PID: os.Getpid(), State: "running"}}}
				return response
			})
			startReadinessServer(t, ctx, supervisor.WorkerSocketPath(roots, record.RepositoryID), readyWorkerResponse)
			err = verifySetupReadiness(ctx, roots, registry, ApplyOptions{}, "workers")
			if scenario.want == "" && err != nil || scenario.want != "" && (err == nil || !strings.Contains(err.Error(), scenario.want)) {
				t.Fatalf("readiness err=%v want=%q", err, scenario.want)
			}
			if scenario.version == version.String() && barriers.Load() != 1 {
				t.Fatalf("checkpoint requests=%d want1", barriers.Load())
			}
		})
	}
}
