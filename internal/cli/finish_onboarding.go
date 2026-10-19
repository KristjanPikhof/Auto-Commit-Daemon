package cli

import (
	"context"
	"fmt"
	"os"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/installer"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/paths"
)

// On authorizes initial local protection. Existing installations always keep
// the explicit setup upgrade boundary and their reviewed preferences.
func bootstrapControlSetup(ctx context.Context, lookup controlRepoLookup) error {
	return bootstrapControlSetupWith(ctx, lookup, installer.BuildPlan, installer.Apply)
}

func bootstrapControlSetupWith(ctx context.Context, lookup controlRepoLookup,
	build func(context.Context, paths.Roots, installer.Options) (installer.Plan, error),
	apply func(context.Context, paths.Roots, installer.Plan, installer.ApplyOptions) (installer.Result, error)) error {
	if _, err := os.Lstat(lookup.Roots.ManagedBinaryPath()); err == nil {
		return nil
	} else if !os.IsNotExist(err) {
		return fmt.Errorf("acd on: inspect runtime: %w", err)
	}
	plan, err := build(ctx, lookup.Roots, installer.Options{Repo: lookup.Worktree.Root, Integrations: "none", NonInteractive: true})
	if err != nil {
		return fmt.Errorf("acd on: prepare initial setup: %w", err)
	}
	if plan.ExistingInstall || !plan.FreshDefaults || len(plan.Repositories) != 0 {
		return actionRequiredError("setup_required", "acd on: existing user setup requires an explicit upgrade; run `acd setup`")
	}
	if _, err := apply(ctx, lookup.Roots, plan, installer.ApplyOptions{}); err != nil {
		return fmt.Errorf("acd on: initial setup failed; repository was not enabled: %w", err)
	}
	return nil
}
