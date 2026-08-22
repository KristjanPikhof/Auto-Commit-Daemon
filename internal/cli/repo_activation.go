package cli

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/central"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/globalops"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/migration"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

func prepareControlRepository(ctx context.Context, lookup controlRepoLookup) (controlRepoLookup, []string, error) {
	needsPreparation, err := controlRepositoryNeedsPreparation(ctx, lookup)
	if err != nil {
		return lookup, nil, err
	}
	if !needsPreparation {
		return lookup, nil, nil
	}
	if err := ensureAttachedHEAD(ctx, lookup.Worktree.Root); err != nil {
		return lookup, nil, err
	}
	if err := git.DurabilitySupport(ctx, lookup.Worktree.Root); err != nil {
		return lookup, nil, fmt.Errorf("acd on: Git durability support: %w", err)
	}

	lifecycleLock, err := globalops.AcquireUserLock(ctx, lookup.Roots.OperationsDBPath())
	if err != nil {
		return lookup, nil, fmt.Errorf("acd on: acquire user lifecycle lock: %w", err)
	}
	defer lifecycleLock.Release()

	current, err := loadControlRepo(ctx, lookup.Worktree.Root)
	if err != nil {
		return lookup, nil, err
	}
	needsPreparation, err = controlRepositoryNeedsPreparation(ctx, current)
	if err != nil {
		return current, nil, err
	}
	if !needsPreparation {
		return current, nil, nil
	}

	registry, err := central.Load(current.Roots)
	if err != nil {
		return current, nil, fmt.Errorf("acd on: load registry: %w", err)
	}
	if registry.Version != central.RegistryVersion {
		return current, nil, fmt.Errorf("%w: repository registry is v%d, want v%d; run `acd setup`",
			state.ErrSetupRequired, registry.Version, central.RegistryVersion)
	}
	registration, err := registry.RegisterResolvedRepo(current.Worktree, "", time.Now().Unix())
	if err != nil {
		return current, nil, fmt.Errorf("acd on: prepare repository registration: %w", err)
	}
	record := registration.Record
	version, versionErr := state.ReadUserVersion(ctx, record.StateDB)
	needsMigration := errors.Is(versionErr, os.ErrNotExist) || version != state.SchemaVersion
	if versionErr != nil && !errors.Is(versionErr, os.ErrNotExist) {
		return current, nil, fmt.Errorf("acd on: inspect repository protection data: %w", versionErr)
	}
	if version > state.SchemaVersion {
		return current, nil, fmt.Errorf("acd on: repository protection data uses future schema v%d", version)
	}
	if !needsMigration {
		if err := state.QuickCheck(ctx, record.StateDB); err != nil {
			return current, nil, fmt.Errorf("acd on: check repository protection data: %w", err)
		}
	}

	actions := make([]string, 0, 3)
	operation := func(operationCtx context.Context) error {
		var migrationPlan *migration.RepositoryPlan
		if needsMigration {
			backupRoot := filepath.Join(current.Roots.SetupRoot(),
				fmt.Sprintf("repo-on-%d-%s", time.Now().UnixNano(), record.WorktreeID))
			if err := os.MkdirAll(backupRoot, 0o700); err != nil {
				return fmt.Errorf("acd on: create repository migration backup: %w", err)
			}
			plan, err := migration.Preflight(operationCtx, record,
				filepath.Join(backupRoot, "state.db"))
			if err != nil {
				return fmt.Errorf("acd on: prepare repository protection data: %w", err)
			}
			if _, err := migration.ApplyAll(operationCtx, []migration.RepositoryPlan{plan}); err != nil {
				return fmt.Errorf("acd on: upgrade repository protection data: %w", err)
			}
			migrationPlan = &plan
		}

		var inserted, enabled bool
		registryErr := central.WithLock(current.Roots, func(locked *central.Registry) error {
			registered, err := locked.RegisterResolvedRepo(current.Worktree, "", time.Now().Unix())
			if err != nil {
				return err
			}
			inserted = registered.Inserted
			enablement := locked.EnableRepo(central.RepoRemovalTarget{
				Path: registered.Record.Path, StateDB: registered.Record.StateDB,
			}, time.Now().Unix())
			if enablement.NotFound {
				return errors.New("registered repository disappeared before enablement")
			}
			enabled = enablement.Updated || current.Record.LifecycleDisabled() || !current.Registered
			return nil
		})
		if registryErr != nil {
			if migrationPlan != nil {
				registryErr = errors.Join(registryErr, migration.Rollback([]migration.RepositoryPlan{*migrationPlan}))
			}
			return fmt.Errorf("acd on: persist repository enablement: %w", registryErr)
		}
		if migrationPlan != nil {
			actions = append(actions, "migrated")
		}
		if inserted {
			actions = append(actions, "registered")
		}
		if enabled {
			actions = append(actions, "enabled")
		}
		return nil
	}
	if err := withQuiescedRepositoryRuntimeForCommand(ctx, current.Roots,
		record.RepositoryID, "acd on", operation); err != nil {
		return current, nil, err
	}

	prepared, err := loadControlRepo(ctx, current.Worktree.Root)
	if err != nil {
		return current, nil, err
	}
	if !prepared.Registered || prepared.Record.LifecycleDisabled() {
		return prepared, nil, errors.New("acd on: repository preparation did not persist enablement")
	}
	return prepared, actions, nil
}

func controlRepositoryNeedsPreparation(ctx context.Context, lookup controlRepoLookup) (bool, error) {
	if !lookup.Registered || lookup.Record.RepositoryID == "" || lookup.Record.WorktreeID == "" ||
		lookup.Record.LifecycleDisabled() || !fileExists(lookup.Record.StateDB) {
		return true, nil
	}
	version, err := state.ReadUserVersion(ctx, lookup.Record.StateDB)
	if err != nil {
		return false, fmt.Errorf("acd on: inspect repository protection data: %w", err)
	}
	return version != state.SchemaVersion, nil
}
