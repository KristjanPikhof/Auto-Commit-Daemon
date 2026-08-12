package installer

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/paths"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/supervisor"
)

func stopSupervisorForUninstall(
	ctx context.Context,
	roots paths.Roots,
	client supervisor.Client,
	timeout time.Duration,
) error {
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	lifecycleLock, err := supervisor.AcquireSessionLifecycleLock(ctx, roots)
	if err != nil {
		return fmt.Errorf("uninstall: lock supervisor session lifecycle: %w", err)
	}
	defer lifecycleLock.Release()
	statusResponse, err := client.Do(ctx, supervisor.Request{
		Version: supervisor.ProtocolVersion, ID: "uninstall-status", Method: "status",
	})
	if err != nil {
		if socketAbsent(roots.SupervisorSocketPath()) && allWorkerSocketsAbsent(roots.SupervisorSocketPath()) {
			return nil
		}
		return fmt.Errorf("uninstall: prove supervisor owner before shutdown: %w", err)
	}
	if statusResponse.Error != nil {
		return fmt.Errorf("uninstall: prove supervisor owner before shutdown: %s", statusResponse.Error.Message)
	}
	status, err := decode[supervisor.Status](statusResponse.Data)
	if err != nil || status.PID <= 0 {
		return errors.New("uninstall: supervisor status did not identify its process")
	}
	workers := make([]processProof, 0, len(status.Workers))
	for _, worker := range status.Workers {
		workers = append(workers, processProof{
			socketPath: supervisor.WorkerSocketPath(roots, worker.RepositoryID), pid: worker.PID,
		})
	}

	shutdownResponse, shutdownErr := client.Do(ctx, supervisor.Request{
		Version: supervisor.ProtocolVersion, ID: "uninstall-shutdown", Method: "shutdown",
		DeadlineMS: time.Now().Add(timeout).UnixMilli(),
	})
	if shutdownErr == nil && shutdownResponse.Error == nil {
		shutdown, decodeErr := decode[supervisor.ShutdownStatus](shutdownResponse.Data)
		if decodeErr != nil {
			shutdownErr = decodeErr
		} else if !shutdown.Stopped {
			shutdownErr = errors.New("supervisor did not confirm stopped state")
		}
	} else if shutdownErr == nil {
		shutdownErr = errors.New(shutdownResponse.Error.Message)
	}

	waitCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	if err := waitForStoppedSupervisor(waitCtx, roots.SupervisorSocketPath(), status.PID, workers); err != nil {
		return errors.Join(fmt.Errorf("uninstall: supervisor shutdown not proved: %w", err), shutdownErr)
	}
	return nil
}

type processProof struct {
	socketPath string
	pid        int
}

func waitForStoppedSupervisor(ctx context.Context, socketPath string, pid int, workers []processProof) error {
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()
	for {
		if socketAbsent(socketPath) && !processAlive(pid) && processesStopped(workers) && allWorkerSocketsAbsent(socketPath) {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func allWorkerSocketsAbsent(supervisorSocket string) bool {
	matches, err := filepath.Glob(filepath.Join(filepath.Dir(supervisorSocket), "worker-*.sock"))
	return err == nil && len(matches) == 0
}

func processesStopped(processes []processProof) bool {
	for _, process := range processes {
		if process.socketPath == "" || !socketAbsent(process.socketPath) || processAlive(process.pid) {
			return false
		}
	}
	return true
}

func socketAbsent(path string) bool {
	_, err := os.Lstat(path)
	return errors.Is(err, os.ErrNotExist)
}

func processAlive(pid int) bool {
	if pid <= 0 {
		return false
	}
	err := syscall.Kill(pid, 0)
	return err == nil || errors.Is(err, syscall.EPERM)
}
