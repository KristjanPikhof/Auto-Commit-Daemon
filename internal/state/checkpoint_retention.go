package state

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

type RetentionCheckpoint struct {
	ID, WorktreeID, Reason, Ref, CommitOID string
	Sequence                               int64
	CreatedTS                              float64
	Retained, Published, Unresolved        bool
	LatestComplete                         bool
	Superseded                             bool
}

const CheckpointRetentionBatchLimit = 100

func RetentionCheckpoints(ctx context.Context, db *DB, worktreeID string) ([]RetentionCheckpoint, error) {
	if db == nil {
		return nil, errors.New("state: retention requires a database")
	}
	version, err := db.UserVersion(ctx)
	if err != nil {
		return nil, err
	}
	finishReferences := ""
	superseded := "0"
	if version >= 31 {
		finishReferences = ` OR EXISTS (SELECT 1 FROM finish_requests f WHERE f.checkpoint_id=cp.id)`
	}
	if version >= 32 {
		finishReferences += ` OR EXISTS(SELECT 1 FROM finish_legacy_object_pins p WHERE p.object_oid=cp.commit_oid)`
		superseded = `cp.coverage_complete=1 AND cp.reason IN ('poll','watch','hint','manual_barrier')
	 AND NOT EXISTS(SELECT 1 FROM checkpoint_events ce WHERE ce.checkpoint_id=cp.id)
	 AND EXISTS(SELECT 1 FROM checkpoints newer WHERE newer.worktree_id=cp.worktree_id AND newer.seq>cp.seq AND newer.phase='completed' AND newer.retained=1 AND newer.coverage_complete=1)`
	}
	hasCaptures, err := legacyTableExists(ctx, db.ReadSQL(), "capture_events")
	if err != nil {
		return nil, err
	}
	published := "0"
	if hasCaptures {
		published = `EXISTS(SELECT 1 FROM checkpoint_events ce WHERE ce.checkpoint_id=cp.id) AND NOT EXISTS(SELECT 1 FROM checkpoint_events ce JOIN capture_events e ON e.seq=ce.event_seq WHERE ce.checkpoint_id=cp.id AND e.state<>'published')`
	}
	rows, err := db.readSQL().QueryContext(ctx, `
SELECT cp.id,cp.worktree_id,cp.reason,cp.checkpoint_ref,cp.commit_oid,
       cp.seq,cp.created_ts,cp.retained,
       COALESCE(cp.coverage_complete=1 AND cp.seq=(SELECT MAX(complete_cp.seq) FROM checkpoints complete_cp WHERE complete_cp.worktree_id=cp.worktree_id AND complete_cp.phase='completed' AND complete_cp.retained=1 AND complete_cp.coverage_complete=1),0),
       `+published+` AS published,
       EXISTS (
         SELECT 1 FROM operations o
         WHERE o.id=cp.operation_id AND o.status IN ('prepared','applying','active','needs_action')
       ) OR EXISTS (
         SELECT 1 FROM restore_operations r
         WHERE r.phase NOT IN ('completed','rolled_back')
           AND cp.id IN (r.target_checkpoint_id,
                         r.pre_restore_checkpoint_id,
                         r.post_restore_checkpoint_id)
       )`+finishReferences+` AS unresolved,`+superseded+` AS superseded
FROM checkpoints cp
WHERE cp.worktree_id=? AND cp.phase='completed'
ORDER BY cp.seq DESC`, worktreeID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var checkpoints []RetentionCheckpoint
	for rows.Next() {
		var item RetentionCheckpoint
		if err := rows.Scan(&item.ID, &item.WorktreeID, &item.Reason, &item.Ref,
			&item.CommitOID, &item.Sequence, &item.CreatedTS, &item.Retained,
			&item.LatestComplete, &item.Published, &item.Unresolved, &item.Superseded); err != nil {
			return nil, err
		}
		checkpoints = append(checkpoints, item)
	}
	return checkpoints, rows.Err()
}

// CheckRetentionLegacyProof blocks pruning until old provenance has a durable
// inventory. A fully pinned cutover may remain in attention for branch recovery.
func CheckRetentionLegacyProof(ctx context.Context, db *DB) error {
	version, err := db.UserVersion(ctx)
	if err != nil {
		return err
	}
	if version < 32 {
		return nil
	}
	cutover, err := LoadFinishLegacyCutover(ctx, db)
	if err != nil {
		return err
	}
	if cutover.WorktreeID != "" {
		return nil
	}
	queries := []legacySelect{
		{"capture_events", "SELECT 1 FROM capture_events"}, {"capture_ops", "SELECT 1 FROM capture_ops"},
		{"shadow_paths", "SELECT 1 FROM shadow_paths"}, {"rewrite_plans", "SELECT 1 FROM rewrite_plans"},
		{"intent_repairs", "SELECT 1 FROM intent_repairs"}, {"self_publications", "SELECT 1 FROM self_publications"},
		{"daemon_meta", "SELECT 1 FROM daemon_meta WHERE key LIKE 'intent.history.plan.%'"},
	}
	query, err := legacyUnion(ctx, db.ReadSQL(), queries)
	if err != nil {
		return err
	}
	var legacy bool
	if query != "" {
		if err = db.ReadSQL().QueryRowContext(ctx, "SELECT EXISTS("+query+")").Scan(&legacy); err != nil {
			return err
		}
	}

	if legacy || cutover.State == "needs_attention" {
		return errors.New("state: legacy retention inventory is not durable; activate protection to retain original versions before cleanup")
	}
	return nil
}

// RetentionObjectPins includes every explicit finish attempt and saved
// history selection. Backup refs are optional until their branch CAS occurs.
type RetentionObjectPin struct {
	Ref, CommitOID string
	Optional       bool
}

func RetentionObjectPins(ctx context.Context, db *DB, worktree string) ([]RetentionObjectPin, error) {
	version, err := db.UserVersion(ctx)
	if err != nil {
		return nil, err
	}
	var pins []RetentionObjectPin
	var legacy []FinishLegacyPin
	if version >= 31 {
		rows, err := db.ReadSQL().QueryContext(ctx, finishSelect+" WHERE worktree_id=?", worktree)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			r, err := scanFinish(rows)
			if err != nil {
				rows.Close()
				return nil, err
			}
			if r.Target.TargetRef != "" {
				pins = append(pins, RetentionObjectPin{Ref: r.Target.TargetRef, CommitOID: r.Target.TargetCommitOID})
			}
			if len(r.Publication) > 0 {
				var p struct {
					Ref string `json:"object_ref"`
					OID string `json:"object_oid"`
				}
				if err = json.Unmarshal(r.Publication, &p); err != nil {
					rows.Close()
					return nil, err
				}
				if p.Ref != "" {
					pins = append(pins, RetentionObjectPin{Ref: p.Ref, CommitOID: p.OID})
				}
			}
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return nil, err
		}

	}
	if version >= 32 {
		legacy, err = FinishLegacyObjectPins(ctx, db)
		if err != nil {
			return nil, err
		}
		for _, p := range legacy {
			pins = append(pins, RetentionObjectPin{Ref: p.PinRef, CommitOID: p.PinOID})
		}
		cutover, err := LoadFinishLegacyCutover(ctx, db)
		if err != nil {
			return nil, err
		}
		if cutover.SnapshotRef != "" {
			pins = append(pins, RetentionObjectPin{Ref: cutover.SnapshotRef, CommitOID: cutover.SnapshotOID})
		}
		rows, err := db.ReadSQL().QueryContext(ctx, "SELECT proof_json,status,request_json FROM finish_history_requests WHERE worktree_id=? AND proof_json<>''", worktree)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var raw, status, request string
			if err = rows.Scan(&raw, &status, &request); err != nil {
				rows.Close()
				return nil, err
			}
			var p FinishHistoryProof
			var r FinishHistoryRequest
			if err = json.Unmarshal([]byte(raw), &p); err != nil {
				rows.Close()
				return nil, err
			}
			if err = json.Unmarshal([]byte(request), &r); err != nil {
				rows.Close()
				return nil, err
			}
			pins = append(pins, RetentionObjectPin{Ref: p.SourceRef, CommitOID: p.ExpectedHead}, RetentionObjectPin{Ref: p.ObjectRef, CommitOID: p.NewHead}, RetentionObjectPin{Ref: p.BackupRef, CommitOID: p.ExpectedHead, Optional: status != "completed" || r.DryRun})
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return nil, err
		}
	}
	plans, err := SavedIntentHistoryPlans(ctx, db)
	if err != nil {
		return nil, err
	}
	proved := make(map[string]bool, len(legacy))
	for _, pin := range legacy {
		proved[pin.ObjectOID] = true
	}
	for _, p := range plans {
		if p.SourceRef != "" {
			pins = append(pins, RetentionObjectPin{Ref: p.SourceRef, CommitOID: p.ExpectedHead})
			continue
		}
		for _, oid := range append(append([]string{p.ExpectedHead}, p.SourceChain...), p.RecreateUnchanged...) {
			if !proved[oid] {
				return nil, fmt.Errorf("state: legacy history source %s lacks a retained private pin", oid)
			}
		}
	}
	seen := map[string]int{}
	unique := make([]RetentionObjectPin, 0, len(pins))
	for _, p := range pins {
		if !validFinishRef(p.Ref, "refs/acd/") || !validFinishOID(p.CommitOID) {
			return nil, ErrFinishIdentity
		}
		if index, exists := seen[p.Ref]; exists {
			if unique[index].CommitOID != p.CommitOID {
				return nil, ErrFinishIdentity
			}
			unique[index].Optional = unique[index].Optional && p.Optional
			continue
		}
		seen[p.Ref] = len(unique)
		unique = append(unique, p)
	}
	return unique, nil
}

// CompactPrunedCheckpoints removes only completed, unreferenced ordinary
// protection metadata after its private ref was deleted by the GC journal.
func CompactPrunedCheckpoints(ctx context.Context, db *DB, worktree string, limit int) (int, error) {
	if limit <= 0 || limit > CheckpointRetentionBatchLimit {
		limit = CheckpointRetentionBatchLimit
	}
	version, err := db.UserVersion(ctx)
	if err != nil {
		return 0, err
	}
	if version < 32 {
		return 0, nil
	}
	tx, err := db.SQL().BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	legacyBlockers := ""
	for _, blocker := range []legacySelect{
		{"publication_drains", " AND NOT EXISTS(SELECT 1 FROM publication_drains d WHERE d.checkpoint_id=cp.id)"},
		{"self_publications", " AND NOT EXISTS(SELECT 1 FROM self_publications p WHERE p.operation_id IN(cp.operation_id,'gc-'||cp.id))"},
	} {
		exists, err := legacyTableExists(ctx, tx, blocker.table)
		if err != nil {
			return 0, err
		}
		if exists {
			legacyBlockers += blocker.query
		}
	}
	rows, err := tx.QueryContext(ctx, `SELECT cp.id,cp.operation_id FROM checkpoints cp
 JOIN operations source ON source.id=cp.operation_id AND source.kind='checkpoint' AND source.status='completed'
 JOIN operations gc ON gc.id='gc-'||cp.id AND gc.kind='checkpoint_gc' AND gc.status='completed'
 JOIN operation_steps gcstep ON gcstep.operation_id=gc.id AND gcstep.ord=1 AND gcstep.kind='delete_checkpoint_ref' AND gcstep.phase='completed' AND gcstep.proof_id=cp.id AND gcstep.target=cp.checkpoint_ref AND gcstep.before_digest=cp.commit_oid
 WHERE cp.worktree_id=? AND cp.retained=0 AND cp.pruned_ts IS NOT NULL AND cp.phase='completed'
 AND cp.coverage_complete=1 AND cp.reason IN ('poll','watch','hint','manual_barrier')
 AND NOT EXISTS(SELECT 1 FROM checkpoint_events ce WHERE ce.checkpoint_id=cp.id)
 AND NOT EXISTS(SELECT 1 FROM finish_requests f WHERE f.checkpoint_id=cp.id)
 AND NOT EXISTS(SELECT 1 FROM restore_operations r WHERE cp.id IN(r.target_checkpoint_id,r.pre_restore_checkpoint_id,r.post_restore_checkpoint_id))
 AND NOT EXISTS(SELECT 1 FROM finish_legacy_object_pins p WHERE p.object_oid=cp.commit_oid)
 AND NOT EXISTS(SELECT 1 FROM finish_legacy_activation_failure WHERE NOT EXISTS(SELECT 1 FROM finish_legacy_cutover))
 AND NOT EXISTS(SELECT 1 FROM operation_steps s WHERE s.proof_id=cp.id AND s.operation_id NOT IN(cp.operation_id,'gc-'||cp.id))
 AND NOT EXISTS(SELECT 1 FROM checkpoints shared WHERE shared.id<>cp.id AND shared.operation_id IN(cp.operation_id,'gc-'||cp.id))
 AND NOT EXISTS(SELECT 1 FROM restore_operations r WHERE r.operation_id IN(cp.operation_id,'gc-'||cp.id))
 `+legacyBlockers+` ORDER BY cp.seq LIMIT ?`, worktree, limit)
	if err != nil {
		return 0, err
	}
	type item struct{ id, operation string }
	var items []item
	for rows.Next() {
		var i item
		if err = rows.Scan(&i.id, &i.operation); err != nil {
			rows.Close()
			return 0, err
		}
		items = append(items, i)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return 0, err
	}
	for _, i := range items {
		if _, err = tx.ExecContext(ctx, "DELETE FROM checkpoints WHERE id=?", i.id); err != nil {
			return 0, err
		}
		if _, err = tx.ExecContext(ctx, "DELETE FROM operations WHERE id IN(?,?)", i.operation, "gc-"+i.id); err != nil {
			return 0, err
		}
	}
	if err = tx.Commit(); err != nil {
		return 0, err
	}
	return len(items), nil
}

func PrepareCheckpointPrune(ctx context.Context, db *DB, checkpoint RetentionCheckpoint, planDigest string) (string, error) {
	operationID := "gc-" + checkpoint.ID
	err := PrepareOperation(ctx, db, Operation{ID: operationID, Kind: "checkpoint_gc",
		WorktreeID: checkpoint.WorktreeID, PlanDigest: planDigest}, []OperationStep{{
		Kind: "delete_checkpoint_ref", Target: checkpoint.Ref,
		BeforeDigest: checkpoint.CommitOID, ProofID: checkpoint.ID,
	}})
	if err != nil {
		var count int
		queryErr := db.readSQL().QueryRowContext(ctx, `SELECT COUNT(*) FROM operations WHERE id=? AND status='prepared'`, operationID).Scan(&count)
		if queryErr == nil && count == 1 {
			return operationID, nil
		}
		return "", err
	}
	return operationID, nil
}

func CompleteCheckpointPrune(ctx context.Context, db *DB, operationID, checkpointID, ref, expectedCommit string) error {
	now := float64(time.Now().UnixNano()) / float64(time.Second)
	tx, err := db.conn.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	result, err := tx.ExecContext(ctx, `UPDATE checkpoints SET retained=0,pruned_ts=?
WHERE id=? AND checkpoint_ref=? AND commit_oid=? AND retained=1`, now, checkpointID, ref, expectedCommit)
	if err != nil {
		return err
	}
	if changed, _ := result.RowsAffected(); changed != 1 {
		var retained bool
		if err := tx.QueryRowContext(ctx, `SELECT retained FROM checkpoints WHERE id=?`, checkpointID).Scan(&retained); err != nil || retained {
			return fmt.Errorf("state: checkpoint prune identity changed")
		}
	}
	if _, err := tx.ExecContext(ctx,
		`DELETE FROM checkpoint_events WHERE checkpoint_id=?`, checkpointID); err != nil {
		return fmt.Errorf("state: clear checkpoint event membership: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE operation_steps SET phase='completed',completed_ts=? WHERE operation_id=? AND ord=1`, now, operationID); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE operations SET phase='completed',status='completed',error='',updated_ts=?,completed_ts=? WHERE id=? AND status IN ('prepared','needs_action')`, now, now, operationID); err != nil {
		return err
	}
	return tx.Commit()
}

type PreparedCheckpointPrune struct {
	OperationID, CheckpointID, Ref, CommitOID string
}

func PreparedCheckpointPrunes(ctx context.Context, db *DB) ([]PreparedCheckpointPrune, error) {
	rows, err := db.readSQL().QueryContext(ctx, `
SELECT o.id,s.proof_id,s.target,s.before_digest
FROM operations o JOIN operation_steps s ON s.operation_id=o.id
WHERE o.kind='checkpoint_gc' AND o.status IN ('prepared','needs_action') AND s.ord=1
ORDER BY o.created_ts,o.id LIMIT 100`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []PreparedCheckpointPrune
	for rows.Next() {
		var item PreparedCheckpointPrune
		if err := rows.Scan(&item.OperationID, &item.CheckpointID, &item.Ref, &item.CommitOID); err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, rows.Err()
}
