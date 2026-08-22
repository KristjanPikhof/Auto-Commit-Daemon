package cli

import (
	"context"
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/central"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/paths"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/supervisor"
)

func TestPrepareControlRepositoryActivatesOnlyTarget(t *testing.T) {
	_ = withIsolatedHome(t)
	shortRoot, err := os.MkdirTemp("/tmp", "acd-repo-on-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(shortRoot) })
	t.Setenv("XDG_STATE_HOME", filepath.Join(shortRoot, "state"))
	t.Setenv("XDG_DATA_HOME", filepath.Join(shortRoot, "share"))
	t.Setenv("XDG_CONFIG_HOME", filepath.Join(shortRoot, "config"))
	roots, err := paths.Resolve()
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	target := makeUnregisteredStartRepo(t)
	deferred := makeUnregisteredStartRepo(t)
	deferredWT, err := git.ResolveWorktree(ctx, deferred)
	if err != nil {
		t.Fatal(err)
	}
	if err := central.WithLock(roots, func(registry *central.Registry) error {
		registration, err := registry.RegisterResolvedRepo(deferredWT, "", time.Now().Unix())
		if err != nil {
			return err
		}
		registry.DisableRepo(central.RepoRemovalTarget{
			Path: registration.Record.Path, StateDB: registration.Record.StateDB,
		}, time.Now().Unix())
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	requests := serveRepositoryMaintenance(t, roots.SupervisorSocketPath())

	lookup, err := loadControlRepo(ctx, target)
	if err != nil {
		t.Fatal(err)
	}
	prepared, actions, err := prepareControlRepository(ctx, lookup)
	if err != nil {
		t.Fatal(err)
	}
	if !prepared.Registered || prepared.Record.LifecycleDisabled() ||
		prepared.Record.RepositoryID == "" || prepared.Record.WorktreeID == "" {
		t.Fatalf("prepared repository=%+v", prepared)
	}
	if !containsStrings(actions, "migrated", "registered", "enabled") {
		t.Fatalf("actions=%v", actions)
	}
	if version, err := state.ReadUserVersion(ctx, prepared.Record.StateDB); err != nil || version != state.SchemaVersion {
		t.Fatalf("target schema=(%d,%v)", version, err)
	}

	registry, err := central.Load(roots)
	if err != nil {
		t.Fatal(err)
	}
	deferredRecord, ok := registry.FindRepo(deferredWT.Root, state.DBPathFromGitDir(deferredWT.GitDir))
	if !ok || !deferredRecord.LifecycleDisabled() {
		t.Fatalf("deferred repository changed: %+v ok=%v", deferredRecord, ok)
	}
	if _, err := os.Stat(deferredRecord.StateDB); !os.IsNotExist(err) {
		t.Fatalf("disabled repository state was created: %v", err)
	}
	if got := <-requests; len(got) != 2 || got[0] != "maintenance_begin" || got[1] != "maintenance_end" {
		t.Fatalf("maintenance requests=%v", got)
	}
}

func serveRepositoryMaintenance(t *testing.T, socketPath string) <-chan []string {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(socketPath), 0o700); err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("unix", socketPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	done := make(chan []string, 1)
	go func() {
		methods := make([]string, 0, 2)
		for len(methods) < 2 {
			conn, err := listener.Accept()
			if err != nil {
				done <- methods
				return
			}
			var request supervisor.Request
			if err := json.NewDecoder(conn).Decode(&request); err != nil {
				_ = conn.Close()
				done <- methods
				return
			}
			methods = append(methods, request.Method)
			response := supervisor.Response{
				Version: supervisor.ProtocolVersion, ID: request.ID, OK: true,
			}
			if request.Method == "maintenance_begin" {
				response.Data = supervisor.MaintenanceLease{
					RepositoryID: request.RepositoryID,
					Token:        "test-maintenance-token",
					ExpiresAt:    time.Now().Add(time.Minute),
				}
			} else {
				response.Data = supervisor.MaintenanceStatus{Released: true}
			}
			_ = json.NewEncoder(conn).Encode(response)
			_ = conn.Close()
		}
		done <- methods
	}()
	return done
}

func containsStrings(values []string, wanted ...string) bool {
	for _, want := range wanted {
		found := false
		for _, value := range values {
			if value == want {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}
