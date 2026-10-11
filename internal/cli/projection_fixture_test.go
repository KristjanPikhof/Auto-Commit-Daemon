package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"time"
)

// Projection tests inspect the read-only report separately from the product
// envelope. Human assertions use the production detail renderers.
func renderStatusProjectionFixture(out io.Writer, report statusReport) error {
	renderFinishOutcomeHuman(out, report.PublicationOutcome)
	return nil
}

func writeStatusProjectionFixture(ctx context.Context, out io.Writer, repo string, jsonOut bool) error {
	if ctx == nil {
		ctx = context.Background()
	}
	rec, _, _, err := lookupRegisteredRepo("status", repo)
	if err != nil {
		return err
	}

	report, err := buildStatusReport(ctx, rec, time.Now())
	if err != nil {
		return fmt.Errorf("acd status: %w", err)
	}

	if jsonOut {
		enc := json.NewEncoder(out)
		enc.SetIndent("", "  ")
		return enc.Encode(report)
	}
	return renderStatusProjectionFixture(out, report)
}
