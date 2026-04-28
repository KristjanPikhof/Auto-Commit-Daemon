package cli

import "github.com/spf13/cobra"

func newStatsCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "stats",
		Short: "Aggregated commits/events/bytes across all repos",
		RunE:  stubRun("stats"),
	}
	cmd.Flags().String("since", "7d", "Lookback window (e.g. 7d, 30d, 1y)")
	return cmd
}
