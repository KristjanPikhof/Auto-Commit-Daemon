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
	repoAutodiscoverySkipDisabled     = "autodiscovery_disabled"
	repoAutodiscoverySkipRegistry     = "autodiscovery_registry_error"
	repoAutodiscoverySkipRepoDisabled = "repo_disabled"
)

type repoAutodiscoveryPolicy struct {
	Worktree   git.Worktree
	Roots      paths.Roots
	Decision   config.AutodiscoveryDecision
	Requested  string
	Registered bool
	Disabled   bool
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
	decision := config.AutodiscoveryDecision{Allowed: false, Source: "explicit_repository_enablement"}
	policy := repoAutodiscoveryPolicy{
		Worktree:  wt,
		Roots:     roots,
		Decision:  decision,
		Requested: repoFlag,
	}
	if policy.Requested == "" {
		policy.Requested = wt.Root
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
		policy.Disabled = rec.LifecycleDisabled()
	}
	return policy, nil
}

func (p repoAutodiscoveryPolicy) allowsImplicitState() bool {
	return p.Registered && !p.Disabled
}

func (p repoAutodiscoveryPolicy) skipReason() string {
	if p.Disabled {
		return repoAutodiscoverySkipRepoDisabled
	}
	if p.Decision.SkippedReason != "" {
		return p.Decision.SkippedReason
	}
	return repoAutodiscoverySkipDisabled
}

func repoDisabledError(command string, p repoAutodiscoveryPolicy) error {
	return fmt.Errorf("acd %s: repo %s is disabled; run `acd on --repo %s` to enable protection again",
		command, p.Worktree.Root, p.Requested)
}

func repoDisabledAfterControlLock(p repoAutodiscoveryPolicy) bool {
	reg, err := central.Load(p.Roots)
	if err != nil {
		return false
	}
	rec, ok := reg.FindRepo(p.Worktree.Root, state.DBPathFromGitDir(p.Worktree.GitDir))
	return ok && rec.LifecycleDisabled()
}

func repoInitRequiredError(command string, p repoAutodiscoveryPolicy) error {
	return fmt.Errorf("acd %s: repository protection is off for %s; run `acd on --repo %s` to enable it",
		command, p.Worktree.Root, p.Requested)
}
