package state

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
)

// ReadLegacyEvidenceDigest freezes original recorded evidence for setup's
// preview/revalidation boundary. It never upgrades or resolves legacy work.
func ReadLegacyEvidenceDigest(ctx context.Context, path string) (string, error) {
	db, err := OpenReadOnly(ctx, path)
	if err != nil {
		return "", err
	}
	defer db.Close()
	tx, err := db.ReadSQL().BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return "", err
	}
	defer tx.Rollback()
	// Current databases share these checkpoint tables with older schemas, but
	// ordinary protection is not archived evidence. Match the legacy inventory's
	// requirement for an older publication consumer before hashing checkpoints.
	var legacyCheckpoints bool
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name IN ('capture_events','self_publications','intent_repairs'))`).Scan(&legacyCheckpoints); err != nil {
		return "", err
	}
	hash := sha256.New()
	tables := []string{"capture_events", "capture_ops", "shadow_paths", "checkpoints", "checkpoint_events", "recovery_snapshots", "recovery_snapshot_events", "self_publications", "self_publication_members", "intent_candidates", "intent_candidate_events", "intent_candidate_lineage", "intent_repairs", "intent_repair_commits", "intent_repair_commit_lineage", "intent_repair_members", "intent_repair_member_seals", "publication_drains", "publication_drain_events", "planner_state", "intent_plan_runs", "flush_requests", "rewrite_plans", "rewrite_plan_commits", "rewrite_plan_groups"}
	for _, table := range tables {
		if !legacyCheckpoints && (table == "checkpoints" || table == "checkpoint_events") {
			continue
		}
		exists, err := legacyTableExists(ctx, tx, table)
		if err != nil {
			return "", err
		}
		if !exists {
			continue
		}
		rows, err := tx.QueryContext(ctx, "SELECT * FROM "+table+" LIMIT 0")
		if err != nil {
			return "", err
		}
		cols, err := rows.Columns()
		rows.Close()
		if err != nil {
			return "", err
		}
		order := make([]string, len(cols))
		for i := range cols {
			order[i] = fmt.Sprint(i + 1)
		}
		rows, err = tx.QueryContext(ctx, "SELECT * FROM "+table+" ORDER BY "+strings.Join(order, ","))
		if err != nil {
			return "", err
		}
		fmt.Fprintln(hash, table, cols)
		for rows.Next() {
			values := make([]any, len(cols))
			dest := make([]any, len(cols))
			for i := range values {
				dest[i] = &values[i]
			}
			if err = rows.Scan(dest...); err != nil {
				rows.Close()
				return "", err
			}
			body, err := json.Marshal(values)
			if err != nil {
				rows.Close()
				return "", err
			}
			hash.Write(body)
			hash.Write([]byte{'\n'})
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return "", err
		}
	}
	rows, err := tx.QueryContext(ctx, "SELECT key,value FROM daemon_meta WHERE key LIKE 'intent.history.plan.%' ORDER BY key")
	if err != nil {
		return "", err
	}
	for rows.Next() {
		var key, value string
		if err = rows.Scan(&key, &value); err != nil {
			rows.Close()
			return "", err
		}
		body, _ := json.Marshal([]string{key, value})
		hash.Write(body)
		hash.Write([]byte{'\n'})
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("sha256:%x", hash.Sum(nil)), nil
}
