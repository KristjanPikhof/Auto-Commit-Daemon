package cli

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/central"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/paths"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/supervisor"
)

func withQuiescedRepositoryRuntime(
	ctx context.Context,
	roots paths.Roots,
	repositoryID string,
	operation func(context.Context) error,
) error {
	if operation == nil {
		return errors.New("acd fix: nil recovery operation")
	}
	registry, err := central.Load(roots)
	if err != nil {
		return fmt.Errorf("acd fix: load registry for runtime maintenance: %w", err)
	}
	enabled := make([]central.RepoRecord, 0)
	for _, record := range registry.Repos {
		if record.RepositoryID == repositoryID && !record.LifecycleDisabled() {
			enabled = append(enabled, record)
		}
	}
	client := supervisor.Client{SocketPath: roots.SupervisorSocketPath(), Timeout: supervisor.CheckpointBarrierTimeout}
	for _, record := range enabled {
		request := supervisor.Request{
			Version: supervisor.ProtocolVersion,
			ID:      fmt.Sprintf("fix-maintenance-checkpoint-%s-%d", record.WorktreeID, time.Now().UnixNano()),
			Method:  "checkpoint_barrier", RepositoryID: repositoryID, WorktreeID: record.WorktreeID,
			DeadlineMS: time.Now().Add(supervisor.CheckpointBarrierTimeout).UnixMilli(),
		}
		response, callErr := client.Do(ctx, request)
		if callErr != nil {
			return fmt.Errorf("acd fix: protect worktree %s before recovery: %w", record.Path, callErr)
		}
		if response.Error != nil {
			return fmt.Errorf("acd fix: protect worktree %s before recovery: %s", record.Path, response.Error.Message)
		}
	}

	beginResponse, err := client.Do(ctx, supervisor.Request{
		Version: supervisor.ProtocolVersion, ID: fmt.Sprintf("fix-maintenance-begin-%d", time.Now().UnixNano()),
		Method: "maintenance_begin", RepositoryID: repositoryID,
		DeadlineMS: time.Now().Add(30 * time.Second).UnixMilli(),
	})
	if err != nil {
		return fmt.Errorf("acd fix: stop shared repository worker: %w", err)
	}
	if beginResponse.Error != nil {
		return fmt.Errorf("acd fix: stop shared repository worker: %s", beginResponse.Error.Message)
	}
	lease, err := decodeProductData[supervisor.MaintenanceLease](beginResponse.Data)
	if err != nil {
		return fmt.Errorf("acd fix: decode repository maintenance lease: %w", err)
	}
	if lease.Token == "" {
		return errors.New("acd fix: repository maintenance lease has no token")
	}

	operationCtx, cancelOperation := context.WithCancel(ctx)
	renewDone := make(chan error, 1)
	go renewRepositoryMaintenance(operationCtx, client, repositoryID, lease.Token, cancelOperation, renewDone)
	operationErr := operation(operationCtx)
	cancelOperation()
	renewErr := <-renewDone

	endCtx, endCancel := context.WithTimeout(context.Background(), 30*time.Second)
	params, _ := json.Marshal(map[string]string{"token": lease.Token})
	endResponse, endErr := client.Do(endCtx, supervisor.Request{
		Version: supervisor.ProtocolVersion, ID: fmt.Sprintf("fix-maintenance-end-%d", time.Now().UnixNano()),
		Method: "maintenance_end", RepositoryID: repositoryID, Params: params,
		DeadlineMS: time.Now().Add(30 * time.Second).UnixMilli(),
	})
	if endErr == nil && endResponse.Error != nil {
		endErr = errors.New(endResponse.Error.Message)
	}
	if endErr == nil && len(enabled) > 0 {
		endErr = waitRepositoryWorkerProtected(endCtx, roots, repositoryID)
	}
	endCancel()
	if endErr != nil {
		endErr = fmt.Errorf("acd fix: restore shared repository worker: %w", endErr)
	}
	return errors.Join(operationErr, renewErr, endErr)
}

func renewRepositoryMaintenance(
	ctx context.Context,
	client supervisor.Client,
	repositoryID, token string,
	cancel context.CancelFunc,
	done chan<- error,
) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	params, _ := json.Marshal(map[string]string{"token": token})
	for {
		select {
		case <-ctx.Done():
			done <- nil
			return
		case <-ticker.C:
			response, err := client.Do(ctx, supervisor.Request{
				Version: supervisor.ProtocolVersion, ID: fmt.Sprintf("fix-maintenance-renew-%d", time.Now().UnixNano()),
				Method: "maintenance_renew", RepositoryID: repositoryID, Params: params,
				DeadlineMS: time.Now().Add(10 * time.Second).UnixMilli(),
			})
			if err == nil && response.Error != nil {
				err = errors.New(response.Error.Message)
			}
			if err != nil {
				if ctx.Err() != nil {
					done <- nil
					return
				}
				cancel()
				done <- fmt.Errorf("acd fix: renew repository maintenance lease: %w", err)
				return
			}
		}
	}
}
