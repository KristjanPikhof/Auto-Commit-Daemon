package cli

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/central"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/config"
	gitpkg "github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/paths"
)

type priorEnvironmentValue struct {
	value string
	set   bool
}

func loadRestartEnvironment(repo string) (map[string]string, error) {
	roots, err := paths.Resolve()
	if err != nil {
		return nil, fmt.Errorf("resolve config paths: %w", err)
	}
	wt, err := gitpkg.ResolveWorktree(context.Background(), repo)
	if err != nil {
		return nil, fmt.Errorf("resolve repository identity: %w", err)
	}
	repoHash := central.CanonicalID(wt.Root)
	doc, err := config.NewStore(roots).Load()
	if err != nil {
		return nil, err
	}
	return config.ResolveRestartEnvironment(doc, repoHash, os.LookupEnv)
}

// applyRestartEnvironment overlays saved restart-bound values for the lifetime
// of one daemon/start operation. The restore closure keeps in-process tests and
// embedded callers isolated; a spawned daemon inherits the resolved overlay.
func applyRestartEnvironment(repo string) (func(), error) {
	values, err := loadRestartEnvironment(repo)
	if err != nil {
		return nil, err
	}
	prior := make(map[string]priorEnvironmentValue, len(values))
	for name, value := range values {
		old, set := os.LookupEnv(name)
		prior[name] = priorEnvironmentValue{value: old, set: set}
		if err := os.Setenv(name, value); err != nil {
			restoreEnvironment(prior)
			return nil, fmt.Errorf("set %s: %w", name, err)
		}
	}
	return func() { restoreEnvironment(prior) }, nil
}

func restoreEnvironment(prior map[string]priorEnvironmentValue) {
	for name, value := range prior {
		if value.set {
			_ = os.Setenv(name, value.value)
		} else {
			_ = os.Unsetenv(name)
		}
	}
}

func clientTTLForRepo(repo string) time.Duration {
	values, err := loadRestartEnvironment(repo)
	if err != nil {
		return clientTTL()
	}
	raw := values["ACD_CLIENT_TTL_SECONDS"]
	seconds, err := strconv.ParseFloat(raw, 64)
	if err != nil || seconds <= 0 {
		return clientTTL()
	}
	return time.Duration(seconds * float64(time.Second))
}
