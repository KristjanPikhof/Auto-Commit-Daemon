package main

import (
	"fmt"
	"os"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/cli"
)

func main() {
	if err := cli.Execute(); err != nil {
		if !cli.ErrorRendered(err) {
			fmt.Fprintln(os.Stderr, "acd:", err)
		}
		os.Exit(cli.ExitCode(err))
	}
}
