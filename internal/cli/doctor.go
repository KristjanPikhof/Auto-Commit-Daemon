package cli

import "github.com/spf13/cobra"

func newDoctorCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "doctor",
		Short: "Run install + runtime diagnostics; optionally bundle as zip",
		RunE:  stubRun("doctor"),
	}
	cmd.Flags().Bool("bundle", false, "Write a doctor zip to ~/Downloads")
	return cmd
}
