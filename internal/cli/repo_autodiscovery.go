package cli

import (
	"context"
	"errors"
	"fmt"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/central"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/config"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/paths"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

const (
	repoAutodiscoverySkipDisabled = "autodiscovery_disabled"
	repoAutodiscoverySkipRegistry = "autodiscovery_registry_error"
)

type repoAutodiscoveryPolicy struct {
	Worktree   git.Worktree
	Roots      paths.Roots
	Decision   config.AutodiscoveryDecision
	Requested  string
	Registered bool
	Record     central.RepoRecord
}

func startAutodiscoveryCaller(harness string) config.PolicyCaller {
	if harness != "" {
		return config.PolicyCallerHook
	}
	return config.PolicyCallerManual
}

func hookAutodiscoveryCaller() config.PolicyCaller {
	return config.PolicyCallerHook
}

func isManualAutodiscoveryCaller(caller config.PolicyCaller) bool {
	return caller == config.PolicyCallerManual
}

func evaluateRepoAutodiscoveryPolicy(ctx context.Context, command, repoFlag string, caller config.PolicyCaller) (repoAutodiscoveryPolicy, error) {
	wt, err := git.ResolveWorktree(ctx, repoFlag)
	if err != nil {
		if errors.Is(err, git.ErrNotWorktree) {
			return repoAutodiscoveryPolicy{}, fmt.Errorf("cli: repo %q is not inside a Git worktree: %w", repoFlag, err)
		}
		return repoAutodiscoveryPolicy{}, err
	}
	roots, err := paths.Resolve()
	if err != nil {
		return repoAutodiscoveryPolicy{}, fmt.Errorf("acd %s: resolve paths: %w", command, err)
	}
	decision, err := config.ImplicitRepoRegistrationAllowedFromRoots(caller, roots)
	if err != nil {
		return repoAutodiscoveryPolicy{}, err
	}
	policy := repoAutodiscoveryPolicy{
		Worktree:  wt,
		Roots:     roots,
		Decision:  decision,
		Requested: repoFlag,
	}
	if policy.Requested == "" {
		policy.Requested = wt.Root
	}
	if decision.Allowed {
		return policy, nil
	}
	reg, err := central.Load(roots)
	if err != nil {
		if caller == config.PolicyCallerHook {
			policy.Decision.SkippedReason = repoAutodiscoverySkipRegistry
			return policy, nil
		}
		return repoAutodiscoveryPolicy{}, fmt.Errorf("acd %s: load registry: %w", command, err)
	}
	rec, ok := reg.FindRepo(wt.Root, state.DBPathFromGitDir(wt.GitDir))
	if ok {
		policy.Registered = true
		policy.Record = rec
	}
	return policy, nil
}

func (p repoAutodiscoveryPolicy) allowsImplicitState() bool {
	return p.Decision.Allowed || p.Registered
}

func (p repoAutodiscoveryPolicy) skipReason() string {
	if p.Decision.SkippedReason != "" {
		return p.Decision.SkippedReason
	}
	return repoAutodiscoverySkipDisabled
}

func repoInitRequiredError(command string, p repoAutodiscoveryPolicy) error {
	source := p.Decision.Source
	if source == "" {
		source = config.DecisionSourceDefault
	}
	return fmt.Errorf("acd %s: repo init required for %s: repo autodiscovery is disabled (source: %s); run `acd repo init --repo %s` or enable repo_lifecycle.autodiscovery in %s",
		command, p.Worktree.Root, source, p.Requested, p.Decision.ConfigPath)
}
