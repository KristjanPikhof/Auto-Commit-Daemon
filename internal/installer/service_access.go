package installer

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/central"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/paths"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/supervisor"
)

// ServiceAccessError means macOS allowed setup itself to read a repository but
// denied the same read to the launchd-managed binary. The user must grant the
// managed binary Full Disk Access before background protection can be honest.
type ServiceAccessError struct {
	Target        string
	ManagedBinary string
	Cause         error
}

func (e *ServiceAccessError) Error() string {
	return fmt.Sprintf(
		"macOS blocked background access to %s; grant Full Disk Access to %s in System Settings > Privacy & Security > Full Disk Access, then rerun `acd setup`",
		e.Target, e.ManagedBinary,
	)
}

func (e *ServiceAccessError) Unwrap() error { return e.Cause }

func verifyMacOSServiceAccess(
	ctx context.Context,
	roots paths.Roots,
	plan Plan,
	executor Executor,
) (returnErr error) {
	targets := enabledRepositoryPaths(plan.Registry)
	if len(targets) == 0 {
		return errors.New("setup: no enabled repository is available for the service access check")
	}
	resultPath := filepath.Join(plan.BackupRoot, "service-access-result.json")
	servicePath := filepath.Join(plan.BackupRoot, "service-access.plist")
	definition, err := supervisor.RenderServiceAccessCheck(
		plan.ManagedBinary, roots.SupervisorLogPath(), plan.OperationID,
		resultPath, targets,
	)
	if err != nil {
		return err
	}
	definition.Path = servicePath
	if err := writeAtomic(servicePath, definition.Content, 0o600); err != nil {
		return err
	}
	loaded := false
	defer func() {
		var cleanupErr error
		if loaded {
			cleanupErr = unloadService(context.Background(), executor, definition)
		}
		for _, target := range []string{resultPath, servicePath} {
			if err := os.Remove(target); err != nil && !errors.Is(err, os.ErrNotExist) {
				cleanupErr = errors.Join(cleanupErr, err)
			}
		}
		returnErr = errors.Join(returnErr, cleanupErr)
	}()
	if err := loadService(ctx, executor, definition); err != nil {
		return fmt.Errorf("setup: start macOS background access check: %w", err)
	}
	loaded = true

	deadline := time.NewTimer(15 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	lastTarget := targets[0]
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-deadline.C:
			return &ServiceAccessError{
				Target: lastTarget, ManagedBinary: plan.ManagedBinary,
				Cause: errors.New("background repository read timed out"),
			}
		case <-ticker.C:
			status, readErr := supervisor.ReadServiceAccessStatus(resultPath)
			if errors.Is(readErr, os.ErrNotExist) {
				continue
			}
			if readErr != nil {
				return readErr
			}
			if status.Target != "" {
				lastTarget = status.Target
			}
			switch status.State {
			case "checking":
				continue
			case "completed":
				return nil
			case "failed":
				return &ServiceAccessError{
					Target: lastTarget, ManagedBinary: plan.ManagedBinary,
					Cause: errors.New(status.Error),
				}
			default:
				return fmt.Errorf("setup: invalid macOS background access state %q", status.State)
			}
		}
	}
}

func enabledRepositoryPaths(registry *central.Registry) []string {
	if registry == nil {
		return nil
	}
	unique := make(map[string]struct{})
	for _, record := range registry.Repos {
		if record.LifecycleDisabled() || record.Path == "" {
			continue
		}
		unique[record.Path] = struct{}{}
	}
	result := make([]string, 0, len(unique))
	for target := range unique {
		result = append(result, target)
	}
	sort.Strings(result)
	return result
}
