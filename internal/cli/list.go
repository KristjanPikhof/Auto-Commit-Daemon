package cli

import (
	"github.com/mattn/go-isatty"
	"os"
	"time"

	_ "modernc.org/sqlite"
)

const defaultListWatchInterval = 2 * time.Second

// listUseWatchMode reports whether acd list should run the live dashboard.
// Explicit --once disables watch; explicit --watch enables it; otherwise TTY
// stdout defaults to watch.
func listUseWatchMode(stdout *os.File, once, watchExplicit bool) bool {
	if once {
		return false
	}
	if watchExplicit {
		return true
	}
	if stdout == nil {
		return false
	}
	return isatty.IsTerminal(stdout.Fd()) || isatty.IsCygwinTerminal(stdout.Fd())
}
