package ai

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

func writePluginScript(t *testing.T, dir, name, body string) string {
	t.Helper()
	bin := filepath.Join(dir, "acd-provider-"+name)
	header := "#!/usr/bin/env bash\nset -u\n"
	if err := os.WriteFile(bin, []byte(header+body), 0o755); err != nil {
		t.Fatalf("write plugin script: %v", err)
	}
	return bin
}

// fixedLookPath returns a LookPathFunc that resolves only the supplied
// binary; everything else returns ErrNotFound. Keeps tests insulated from
// the real $PATH.
func fixedLookPath(name, path string) LookPathFunc {
	return func(want string) (string, error) {
		if want == name {
			return path, nil
		}
		return "", fmt.Errorf("not found: %s", want)
	}
}

func skipIfWindows(t *testing.T) {
	t.Helper()
	if runtime.GOOS == "windows" {
		t.Skip("subprocess plugin tests require a POSIX shell")
	}
}
