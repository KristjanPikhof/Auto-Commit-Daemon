package supervisor

import "strings"

// WorkerEnvironment captures only ACD runtime overrides plus PATH. macOS
// workers are launched as independent launchd jobs, so they cannot inherit the
// supervisor process environment directly. The values are transferred over
// the owner-only supervisor socket and are never written to a plist or log.
func WorkerEnvironment(environ []string) map[string]string {
	values := make(map[string]string)
	for _, item := range environ {
		name, value, ok := strings.Cut(item, "=")
		if !ok || name == "" {
			continue
		}
		if name == "PATH" || strings.HasPrefix(name, "ACD_") {
			values[name] = value
		}
	}
	return values
}

func ValidWorkerEnvironment(values map[string]string) bool {
	for name := range values {
		if name != "PATH" && !strings.HasPrefix(name, "ACD_") {
			return false
		}
		if strings.ContainsRune(name, '=') || strings.ContainsRune(name, 0) {
			return false
		}
	}
	return true
}
