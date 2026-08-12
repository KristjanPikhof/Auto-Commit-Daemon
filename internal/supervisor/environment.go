package supervisor

import (
	"path/filepath"
	"sort"
	"strings"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/paths"
)

const supervisorOwnershipEnv = "ACD_INTERNAL_SUPERVISOR_OWNERSHIP"

// WorkerEnvironment captures only ACD runtime overrides plus PATH. Workers
// receive the validated values through the owner-only supervisor socket so
// restarts use one stable runtime view without persisting environment values.
func WorkerEnvironment(environ []string) map[string]string {
	values := make(map[string]string)
	for _, item := range environ {
		name, value, ok := strings.Cut(item, "=")
		if !ok || name == "" {
			continue
		}
		if name == "PATH" || strings.HasPrefix(name, "ACD_") && name != supervisorOwnershipEnv {
			values[name] = value
		}
	}
	return values
}

func sessionProcessEnvironment(roots paths.Roots, environ []string, ownerID string) []string {
	return append(ProcessEnvironment(roots, environ), supervisorOwnershipEnv+"=session:"+ownerID)
}

func ValidWorkerEnvironment(values map[string]string) bool {
	for name := range values {
		if name == supervisorOwnershipEnv || name != "PATH" && !strings.HasPrefix(name, "ACD_") {
			return false
		}
		if strings.ContainsRune(name, '=') || strings.ContainsRune(name, 0) {
			return false
		}
	}
	return true
}

// ProcessEnvironment keeps long-lived supervisors and workers from retaining
// unrelated secrets inherited from the invoking terminal.
func ProcessEnvironment(roots paths.Roots, environ []string) []string {
	values := WorkerEnvironment(environ)
	for _, item := range environ {
		name, value, ok := strings.Cut(item, "=")
		if ok && name == "HOME" {
			values[name] = value
			break
		}
	}
	values["XDG_STATE_HOME"] = filepath.Dir(roots.State)
	values["XDG_DATA_HOME"] = filepath.Dir(roots.Share)
	values["XDG_CONFIG_HOME"] = filepath.Dir(roots.Config)
	names := make([]string, 0, len(values))
	for name := range values {
		names = append(names, name)
	}
	sort.Strings(names)
	result := make([]string, 0, len(names))
	for _, name := range names {
		result = append(result, name+"="+values[name])
	}
	return result
}
