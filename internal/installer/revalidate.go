package installer

import (
	"context"
	"errors"
	"strings"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/paths"
)

func revalidateSetupPlan(ctx context.Context, roots paths.Roots, reviewed Plan, ignoreServiceState bool) error {
	selection := "none"
	if len(reviewed.Integrations) > 0 {
		selection = strings.Join(reviewed.Integrations, ",")
	}
	current, err := BuildPlan(ctx, roots, Options{
		Repo: reviewed.Repo, Integrations: selection, Executable: reviewed.SourceExecutable,
		SkipServiceCheck: reviewed.ServiceCheckSkipped,
	})
	if err != nil {
		return err
	}
	if ignoreServiceState {
		copyRuntimeServiceState(&current.PriorService, reviewed.PriorService)
		current.Digest = digestPlan(current)
	}
	if current.Digest != reviewed.Digest {
		return errors.New("setup: live installation state changed after preview; preview again")
	}
	return nil
}

func revalidateUninstallPlan(ctx context.Context, roots paths.Roots, reviewed UninstallPlan, ignoreServiceState bool) error {
	current, err := BuildUninstallPlan(ctx, roots, reviewed.PurgeData)
	if err != nil {
		return err
	}
	if ignoreServiceState {
		copyRuntimeServiceState(&current.PriorService, reviewed.PriorService)
		current.Digest = digestUninstallPlan(current)
	}
	if current.Digest != reviewed.Digest {
		return errors.New("uninstall: live installation state changed after preview; preview again")
	}
	return nil
}

func copyRuntimeServiceState(target *ServiceState, reviewed ServiceState) {
	target.Loaded = reviewed.Loaded
	target.Enabled = reviewed.Enabled
	target.LegacyLoaded = reviewed.LegacyLoaded
	target.SessionLoaded = reviewed.SessionLoaded
}
