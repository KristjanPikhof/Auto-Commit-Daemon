package cli

import (
	"os"
	"time"

	"github.com/mattn/go-isatty"
	_ "modernc.org/sqlite"
)

const defaultListWatchInterval = 2 * time.Second

func listOutputIsTerminal(stdout *os.File) bool {
	if stdout == nil {
		return false
	}
	return isatty.IsTerminal(stdout.Fd()) || isatty.IsCygwinTerminal(stdout.Fd())
}
