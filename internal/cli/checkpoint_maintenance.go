package cli

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/checkpoint"
)

func readCheckpointMaintenance(ctx context.Context, conn *sql.DB) (checkpoint.MaintenanceStatus, error) {
	raw, _, err := metaLookup(ctx, conn, checkpoint.MaintenanceMetaKey)
	if err != nil {
		return checkpoint.MaintenanceStatus{}, err
	}
	legacy, _, err := metaLookup(ctx, conn, "protection.retention_over_budget")
	return checkpoint.DecodeMaintenance(raw, legacy), err
}

func maintenanceDetails(m checkpoint.MaintenanceStatus) string {
	if m.Summary() == "" {
		return ""
	}
	parts := []string{m.Summary()}
	if m.Error != "" {
		parts = append(parts, m.Error)
	}
	if m.LastSuccessTS > 0 {
		parts = append(parts, fmt.Sprintf("Last measured: %d bytes retained, %d protected; budget %d bytes.",
			m.ContentBytes, m.ProtectedBytes, checkpoint.DefaultContentBudget))
	}
	if m.NextAttemptTS > 0 {
		parts = append(parts, "Next check: "+time.Unix(m.NextAttemptTS, 0).UTC().Format(time.RFC3339)+".")
	}
	return strings.Join(parts, " ")
}

func applyMaintenanceStatus(res *controlResult, report statusReport) {
	m := report.CheckpointMaintenance
	// Compatibility for callers constructing a report without the new details.
	if m.State == "" && report.CheckpointRetentionOverBudget {
		m = checkpoint.DecodeMaintenance("", "true")
	}
	res.CheckpointMaintenance = m
	if m.Summary() == "" {
		return
	}
	if res.Health == controlHealthNeedsAttention {
		// Preserve the more urgent protection/publication failure and its remedy.
		res.Summary += " " + m.Summary()
		return
	}
	if res.Health == controlHealthHealthy && report.PendingEvents == 0 {
		res.Summary = maintenanceDetails(m)
	} else {
		res.Summary += " " + maintenanceDetails(m)
	}
	if m.NeedsAction() {
		res.Health = controlHealthNeedsAttention
		res.NextAction = m.NextAction()
	}
}
