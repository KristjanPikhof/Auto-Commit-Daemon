package cli

import (
	"errors"
	"fmt"

	"github.com/spf13/cobra"
)

// errNotImplemented is the sentinel returned by every stubbed subcommand
// during Phase 0 scaffolding. Each subcommand will replace its RunE in its
// dedicated phase.
var errNotImplemented = errors.New("not implemented yet (Phase 0 stub)")

func stubRun(name string) func(*cobra.Command, []string) error {
	return func(cmd *cobra.Command, args []string) error {
		fmt.Fprintf(cmd.ErrOrStderr(), "acd %s: %s\n", name, errNotImplemented)
		return errNotImplemented
	}
}
