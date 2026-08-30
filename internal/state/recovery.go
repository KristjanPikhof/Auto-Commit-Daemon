package state

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
)

// RecoverySnapshot is the durable record of one all-or-none unpublished-chain
// transition. Outcome is EventStatePublished when a stable external commit was
// proven, or EventStateRecovered when a composed tree was archived. RecoveryRef
// protects CommitOID in both outcomes so a cross-store HEAD race cannot make
// the evidence unreachable. Exact ordered membership lives in
// recovery_snapshot_events.
type RecoverySnapshot struct {
	ID               int64
	CreatedTS        float64
	Outcome          string
	BranchRef        string
	BranchGeneration int64
	FirstEventSeq    int64
	LastEventSeq     int64
	EventCount       int
	CommitOID        string
	RecoveryRef      sql.NullString
	Reason           sql.NullString
}

// RecoverySnapshotEvent is one exact ordered member of a recovery snapshot.
// EventSeq intentionally has no capture_events foreign key so membership
// survives later terminal-row pruning, like decision_records.event_seq.
type RecoverySnapshotEvent struct {
	SnapshotID int64
	Ord        int
	EventSeq   int64
}

// RecoveryChainEvent pairs one immutable capture event with its immutable,
// ordered operations. Callers pass the result back to TransitionRecoveryChain
// as the expected chain; the transition rechecks both layers before writing.
type RecoveryChainEvent struct {
	Event CaptureEvent
	Ops   []CaptureOp
}

// RecoveryChainTransition describes the only two safe terminal outcomes for a
// complete unpublished chain. Expected must be the exact same-anchor suffix
// returned by LoadUnpublishedRecoveryChain.
type RecoveryChainTransition struct {
	Expected []RecoveryChainEvent
	// ExpectedLater seals every unpublished row that remains after an exact
	// frozen prefix. Branch adoption rechecks Expected || ExpectedLater in one
	// writer transaction so a newly appended capture aborts the whole attempt.
	ExpectedLater []RecoveryChainEvent
	TargetState   string
	CommitOID     string
	RecoveryRef   string
	Reason        string
	ActionTaken   string
	UserMessage   string
	// DecisionKind preserves a narrower legacy classification when the
	// caller has already proven it. Empty selects the outcome default.
	DecisionKind string
	TransitionTS float64
	// InvalidateShadow atomically removes the transitioned pair's shadow
	// rows and bootstrap marker. Active replay sets this for recovered chains
	// so the run loop reseeds HEAD and captures the still-dirty worktree again.
	// Branch-transition callers leave it false and own their generation reseed.
	InvalidateShadow bool
	// AllowLaterUnpublished permits Expected to be an exact leading prefix of
	// the current unpublished suffix. Frozen publication recovery uses this to
	// settle only its immutable target while preserving captures made later.
	AllowLaterUnpublished bool
	// AdoptedBranchHead makes prefix settlement, the complete replacement
	// shadow, and branch-token metadata one SQLite commit. The caller must hold
	// the literal branch ref at this exact OID for the duration of the call.
	AdoptedBranchHead string
	ShadowRows        []ShadowPath
}

// ErrRecoveryChainChanged means the unpublished suffix no longer exactly
// matches the caller's expected seq/state/provenance/ops. No snapshot,
// lifecycle update, planner deletion, breadcrumb cleanup, or decision is
// committed when this error is returned.
var ErrRecoveryChainChanged = errors.New("state: recovery chain changed")

var errPublishedRecoveryContextLimit = errors.New("state: published recovery context limit exceeded")

type recoveryQueryer interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}

const maxPublishedRecoveryContextEvents = 4096

// LoadUnpublishedRecoveryChain returns the complete unpublished suffix for one
// (branch_ref, branch_generation), starting at firstSeq and ordered by seq.
// Only pending, blocked_conflict, and failed are unpublished; recovered is a
// terminal non-barrier state and is deliberately excluded.
func LoadUnpublishedRecoveryChain(ctx context.Context, d *DB, branchRef string, branchGeneration, firstSeq int64) ([]RecoveryChainEvent, error) {
	if d == nil {
		return nil, fmt.Errorf("state: LoadUnpublishedRecoveryChain: nil db")
	}
	return loadUnpublishedRecoveryChain(ctx, d.readSQL(), branchRef, branchGeneration, firstSeq)
}

// LoadUnpublishedRecoveryChainBounded returns one complete unpublished suffix
// while capping both event and operation evidence. It uses one event query and
// one bulk operation query so proof work does not grow into one query per
// capture. A suffix beyond either bound fails closed with
// ErrCompletedBranchTransitionProof.
func LoadUnpublishedRecoveryChainBounded(
	ctx context.Context,
	d *DB,
	branchRef string,
	branchGeneration int64,
	firstSeq int64,
	limit int,
) ([]RecoveryChainEvent, error) {
	if d == nil {
		return nil, fmt.Errorf(
			"state: LoadUnpublishedRecoveryChainBounded: nil db")
	}
	return loadUnpublishedRecoveryChainBounded(
		ctx, d.readSQL(), branchRef, branchGeneration, firstSeq, limit)
}

// LoadPublishedRecoveryContext returns published candidates that may be needed
// to materialize an unpublished suffix. It includes earlier rows in the exact
// branch/generation pair that touch a path in the selected unpublished suffix
// because a previously captured event can be published after HEAD advances
// beyond the suffix's first base. The daemon narrows candidates by seed state
// and commit ancestry before materialization. TransitionRecoveryChain never
// changes these rows.
func LoadPublishedRecoveryContext(
	ctx context.Context,
	d *DB,
	branchRef string,
	branchGeneration int64,
	firstSeq int64,
	lastSeq int64,
) ([]RecoveryChainEvent, error) {
	if d == nil {
		return nil, fmt.Errorf("state: LoadPublishedRecoveryContext: nil db")
	}
	if branchRef == "" || branchGeneration < 1 || firstSeq < 1 || lastSeq < firstSeq {
		return nil, fmt.Errorf("state: LoadPublishedRecoveryContext: invalid selector")
	}
	selected, err := loadPublishedRecoveryContextSeqs(
		ctx, d.readSQL(), branchRef, branchGeneration, firstSeq, lastSeq,
	)
	if err != nil {
		return nil, err
	}
	if len(selected) == 0 {
		return nil, nil
	}

	rows, err := d.readSQL().QueryContext(ctx, `
SELECT seq, branch_ref, branch_generation, base_head, operation, path, old_path,
       fidelity, captured_ts, published_ts, state, commit_oid, error, message
FROM capture_events
WHERE branch_ref = ?
  AND branch_generation = ?
  AND state = ?
  AND seq < ?
ORDER BY seq ASC`, branchRef, branchGeneration, EventStatePublished, lastSeq)
	if err != nil {
		return nil, fmt.Errorf("state: load published recovery context: %w", err)
	}
	defer rows.Close()
	var recoveryContext []RecoveryChainEvent
	for rows.Next() {
		var ev CaptureEvent
		if err := rows.Scan(&ev.Seq, &ev.BranchRef, &ev.BranchGeneration, &ev.BaseHead,
			&ev.Operation, &ev.Path, &ev.OldPath, &ev.Fidelity,
			&ev.CapturedTS, &ev.PublishedTS, &ev.State, &ev.CommitOID,
			&ev.Error, &ev.Message); err != nil {
			return nil, fmt.Errorf("state: scan published recovery context: %w", err)
		}
		if _, needed := selected[ev.Seq]; needed {
			recoveryContext = append(recoveryContext, RecoveryChainEvent{Event: ev})
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("state: iterate published recovery context: %w", err)
	}
	if err := rows.Close(); err != nil {
		return nil, fmt.Errorf("state: close published recovery context: %w", err)
	}
	bySeq := make(map[int64]int, len(recoveryContext))
	for i := range recoveryContext {
		bySeq[recoveryContext[i].Event.Seq] = i
	}
	opRows, err := d.readSQL().QueryContext(ctx, `
SELECT op.event_seq, op.ord, op.op, op.path, op.old_path,
       op.before_oid, op.before_mode, op.after_oid, op.after_mode, op.fidelity
FROM capture_ops op
JOIN capture_events event ON event.seq = op.event_seq
WHERE event.branch_ref = ?
  AND event.branch_generation = ?
  AND event.state = ?
  AND event.seq < ?
ORDER BY op.event_seq ASC, op.ord ASC`,
		branchRef, branchGeneration, EventStatePublished, lastSeq)
	if err != nil {
		return nil, fmt.Errorf("state: load published recovery context ops: %w", err)
	}
	defer opRows.Close()
	for opRows.Next() {
		var op CaptureOp
		if err := opRows.Scan(&op.EventSeq, &op.Ord, &op.Op, &op.Path, &op.OldPath,
			&op.BeforeOID, &op.BeforeMode, &op.AfterOID, &op.AfterMode,
			&op.Fidelity); err != nil {
			return nil, fmt.Errorf("state: scan published recovery context op: %w", err)
		}
		if i, needed := bySeq[op.EventSeq]; needed {
			recoveryContext[i].Ops = append(recoveryContext[i].Ops, op)
		}
	}
	if err := opRows.Err(); err != nil {
		return nil, fmt.Errorf("state: iterate published recovery context ops: %w", err)
	}
	return recoveryContext, nil
}

type recoveryPathCandidate struct {
	seq   int64
	paths []string
}

func loadPublishedRecoveryContextSeqs(
	ctx context.Context,
	q recoveryQueryer,
	branchRef string,
	branchGeneration int64,
	firstSeq int64,
	lastSeq int64,
) (map[int64]struct{}, error) {
	required := make(map[string]struct{})
	rows, err := q.QueryContext(ctx, `
SELECT op.path, op.old_path
FROM capture_ops op
JOIN capture_events event ON event.seq = op.event_seq
WHERE event.branch_ref = ?
  AND event.branch_generation = ?
  AND event.seq >= ?
  AND event.seq <= ?
  AND event.state IN (?, ?, ?)`,
		branchRef, branchGeneration, firstSeq, lastSeq,
		EventStatePending, EventStateBlockedConflict, EventStateFailed)
	if err != nil {
		return nil, fmt.Errorf("state: load unpublished recovery paths: %w", err)
	}
	for rows.Next() {
		var path string
		var oldPath sql.NullString
		if err := rows.Scan(&path, &oldPath); err != nil {
			_ = rows.Close()
			return nil, fmt.Errorf("state: scan unpublished recovery path: %w", err)
		}
		addRecoveryRequiredPath(required, path)
		if oldPath.Valid {
			addRecoveryRequiredPath(required, oldPath.String)
		}
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return nil, fmt.Errorf("state: iterate unpublished recovery paths: %w", err)
	}
	if err := rows.Close(); err != nil {
		return nil, fmt.Errorf("state: close unpublished recovery paths: %w", err)
	}
	if len(required) == 0 {
		return nil, nil
	}

	rows, err = q.QueryContext(ctx, `
SELECT event.seq, op.path, op.old_path
FROM capture_events event
JOIN capture_ops op ON op.event_seq = event.seq
WHERE event.branch_ref = ?
  AND event.branch_generation = ?
  AND event.state = ?
  AND event.seq < ?
ORDER BY event.seq DESC, op.ord ASC`,
		branchRef, branchGeneration, EventStatePublished, lastSeq)
	if err != nil {
		return nil, fmt.Errorf("state: load published recovery path candidates: %w", err)
	}
	var candidates []recoveryPathCandidate
	for rows.Next() {
		var seq int64
		var path string
		var oldPath sql.NullString
		if err := rows.Scan(&seq, &path, &oldPath); err != nil {
			_ = rows.Close()
			return nil, fmt.Errorf("state: scan published recovery path candidate: %w", err)
		}
		if len(candidates) == 0 || candidates[len(candidates)-1].seq != seq {
			candidates = append(candidates, recoveryPathCandidate{seq: seq})
		}
		candidate := &candidates[len(candidates)-1]
		candidate.paths = append(candidate.paths, path)
		if oldPath.Valid && oldPath.String != "" {
			candidate.paths = append(candidate.paths, oldPath.String)
		}
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return nil, fmt.Errorf("state: iterate published recovery path candidates: %w", err)
	}
	if err := rows.Close(); err != nil {
		return nil, fmt.Errorf("state: close published recovery path candidates: %w", err)
	}

	selected := make(map[int64]struct{})
	for _, candidate := range candidates {
		if !recoveryPathsIntersect(candidate.paths, required) {
			continue
		}
		selected[candidate.seq] = struct{}{}
		if len(selected) > maxPublishedRecoveryContextEvents {
			return nil, fmt.Errorf("%w: bounded limit is %d events",
				errPublishedRecoveryContextLimit, maxPublishedRecoveryContextEvents)
		}
		for _, path := range candidate.paths {
			addRecoveryRequiredPath(required, path)
		}
	}
	return selected, nil
}

func addRecoveryRequiredPath(required map[string]struct{}, path string) {
	if path != "" {
		required[path] = struct{}{}
	}
}

func recoveryPathsIntersect(paths []string, required map[string]struct{}) bool {
	for _, path := range paths {
		if _, ok := required[path]; ok {
			return true
		}
	}
	return false
}

func loadUnpublishedRecoveryChain(ctx context.Context, q recoveryQueryer, branchRef string, branchGeneration, firstSeq int64) ([]RecoveryChainEvent, error) {
	if branchRef == "" {
		return nil, fmt.Errorf("state: LoadUnpublishedRecoveryChain: empty branch_ref")
	}
	if branchGeneration < 1 {
		return nil, fmt.Errorf("state: LoadUnpublishedRecoveryChain: invalid branch_generation %d", branchGeneration)
	}
	if firstSeq < 1 {
		return nil, fmt.Errorf("state: LoadUnpublishedRecoveryChain: invalid first_seq %d", firstSeq)
	}

	rows, err := q.QueryContext(ctx, `
SELECT seq, branch_ref, branch_generation, base_head, operation, path, old_path,
       fidelity, captured_ts, published_ts, state, commit_oid, error, message
FROM capture_events
WHERE branch_ref = ?
  AND branch_generation = ?
  AND seq >= ?
  AND state IN (?, ?, ?)
ORDER BY seq ASC`,
		branchRef, branchGeneration, firstSeq,
		EventStatePending, EventStateBlockedConflict, EventStateFailed)
	if err != nil {
		return nil, fmt.Errorf("state: load unpublished recovery chain: %w", err)
	}
	defer rows.Close()

	var chain []RecoveryChainEvent
	for rows.Next() {
		var ev CaptureEvent
		if err := rows.Scan(&ev.Seq, &ev.BranchRef, &ev.BranchGeneration, &ev.BaseHead,
			&ev.Operation, &ev.Path, &ev.OldPath, &ev.Fidelity,
			&ev.CapturedTS, &ev.PublishedTS, &ev.State, &ev.CommitOID,
			&ev.Error, &ev.Message); err != nil {
			return nil, fmt.Errorf("state: scan unpublished recovery event: %w", err)
		}
		chain = append(chain, RecoveryChainEvent{Event: ev})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("state: iterate unpublished recovery chain: %w", err)
	}
	if err := rows.Close(); err != nil {
		return nil, fmt.Errorf("state: close unpublished recovery chain: %w", err)
	}

	for i := range chain {
		ops, err := loadCaptureOpsWith(ctx, q, chain[i].Event.Seq)
		if err != nil {
			return nil, err
		}
		chain[i].Ops = ops
	}
	return chain, nil
}

func loadUnpublishedRecoveryChainBounded(
	ctx context.Context,
	q recoveryQueryer,
	branchRef string,
	branchGeneration int64,
	firstSeq int64,
	limit int,
) ([]RecoveryChainEvent, error) {
	if branchRef == "" {
		return nil, fmt.Errorf(
			"state: LoadUnpublishedRecoveryChainBounded: empty branch_ref")
	}
	if branchGeneration < 1 {
		return nil, fmt.Errorf(
			"state: LoadUnpublishedRecoveryChainBounded: invalid branch_generation %d",
			branchGeneration)
	}
	if firstSeq < 1 {
		return nil, fmt.Errorf(
			"state: LoadUnpublishedRecoveryChainBounded: invalid first_seq %d",
			firstSeq)
	}
	if limit < 1 || limit > CompletedBranchTransitionProofLimit {
		return nil, fmt.Errorf(
			"state: LoadUnpublishedRecoveryChainBounded: invalid limit %d", limit)
	}

	rows, err := q.QueryContext(ctx, `
SELECT seq, branch_ref, branch_generation, base_head, operation, path, old_path,
       fidelity, captured_ts, published_ts, state, commit_oid, error, message
FROM capture_events
WHERE branch_ref = ?
  AND branch_generation = ?
  AND seq >= ?
  AND state IN (?, ?, ?)
ORDER BY seq ASC
LIMIT ?`,
		branchRef, branchGeneration, firstSeq,
		EventStatePending, EventStateBlockedConflict, EventStateFailed,
		limit+1)
	if err != nil {
		return nil, fmt.Errorf(
			"state: load bounded unpublished recovery events: %w", err)
	}
	var chain []RecoveryChainEvent
	for rows.Next() {
		var ev CaptureEvent
		if err := rows.Scan(
			&ev.Seq, &ev.BranchRef, &ev.BranchGeneration, &ev.BaseHead,
			&ev.Operation, &ev.Path, &ev.OldPath, &ev.Fidelity,
			&ev.CapturedTS, &ev.PublishedTS, &ev.State, &ev.CommitOID,
			&ev.Error, &ev.Message); err != nil {
			_ = rows.Close()
			return nil, fmt.Errorf(
				"state: scan bounded unpublished recovery event: %w", err)
		}
		chain = append(chain, RecoveryChainEvent{Event: ev})
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return nil, fmt.Errorf(
			"state: iterate bounded unpublished recovery events: %w", err)
	}
	if err := rows.Close(); err != nil {
		return nil, fmt.Errorf(
			"state: close bounded unpublished recovery events: %w", err)
	}
	if len(chain) > limit {
		return nil, fmt.Errorf(
			"%w: unpublished recovery suffix exceeds %d events",
			ErrCompletedBranchTransitionProof, limit)
	}
	if len(chain) == 0 {
		return nil, nil
	}

	bySeq := make(map[int64]int, len(chain))
	for i := range chain {
		bySeq[chain[i].Event.Seq] = i
	}
	lastSeq := chain[len(chain)-1].Event.Seq
	opRows, err := q.QueryContext(ctx, `
SELECT op.event_seq, op.ord, op.op, op.path, op.old_path,
       op.before_oid, op.before_mode, op.after_oid, op.after_mode, op.fidelity
FROM capture_ops op
JOIN capture_events event ON event.seq = op.event_seq
WHERE event.branch_ref = ?
  AND event.branch_generation = ?
  AND event.seq >= ?
  AND event.seq <= ?
  AND event.state IN (?, ?, ?)
ORDER BY op.event_seq ASC, op.ord ASC
LIMIT ?`,
		branchRef, branchGeneration, firstSeq, lastSeq,
		EventStatePending, EventStateBlockedConflict, EventStateFailed,
		limit+1)
	if err != nil {
		return nil, fmt.Errorf(
			"state: load bounded unpublished recovery operations: %w", err)
	}
	opCount := 0
	for opRows.Next() {
		var op CaptureOp
		if err := opRows.Scan(
			&op.EventSeq, &op.Ord, &op.Op, &op.Path, &op.OldPath,
			&op.BeforeOID, &op.BeforeMode, &op.AfterOID, &op.AfterMode,
			&op.Fidelity); err != nil {
			_ = opRows.Close()
			return nil, fmt.Errorf(
				"state: scan bounded unpublished recovery operation: %w", err)
		}
		opCount++
		if opCount > limit {
			_ = opRows.Close()
			return nil, fmt.Errorf(
				"%w: unpublished recovery suffix exceeds %d operations",
				ErrCompletedBranchTransitionProof, limit)
		}
		i, ok := bySeq[op.EventSeq]
		if !ok {
			_ = opRows.Close()
			return nil, fmt.Errorf(
				"%w: bounded recovery operation references unselected event %d",
				ErrCompletedBranchTransitionProof, op.EventSeq)
		}
		chain[i].Ops = append(chain[i].Ops, op)
	}
	if err := opRows.Err(); err != nil {
		_ = opRows.Close()
		return nil, fmt.Errorf(
			"state: iterate bounded unpublished recovery operations: %w", err)
	}
	if err := opRows.Close(); err != nil {
		return nil, fmt.Errorf(
			"state: close bounded unpublished recovery operations: %w", err)
	}
	return chain, nil
}

func loadCaptureOpsWith(ctx context.Context, q recoveryQueryer, seq int64) ([]CaptureOp, error) {
	rows, err := q.QueryContext(ctx, `
SELECT event_seq, ord, op, path, old_path,
       before_oid, before_mode, after_oid, after_mode, fidelity
FROM capture_ops WHERE event_seq = ? ORDER BY ord ASC`, seq)
	if err != nil {
		return nil, fmt.Errorf("state: load recovery capture ops seq=%d: %w", seq, err)
	}
	defer rows.Close()
	var ops []CaptureOp
	for rows.Next() {
		var op CaptureOp
		if err := rows.Scan(&op.EventSeq, &op.Ord, &op.Op, &op.Path, &op.OldPath,
			&op.BeforeOID, &op.BeforeMode, &op.AfterOID, &op.AfterMode,
			&op.Fidelity); err != nil {
			return nil, fmt.Errorf("state: scan recovery capture op seq=%d: %w", seq, err)
		}
		ops = append(ops, op)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("state: iterate recovery capture ops seq=%d: %w", seq, err)
	}
	return ops, nil
}

// TransitionRecoveryChain atomically moves one exact unpublished suffix to
// published or recovered. It never changes branch_ref, branch_generation,
// base_head, operation, path, old_path, fidelity, or capture_ops.
func TransitionRecoveryChain(ctx context.Context, d *DB, req RecoveryChainTransition) (RecoverySnapshot, error) {
	var zero RecoverySnapshot
	if d == nil {
		return zero, fmt.Errorf("state: TransitionRecoveryChain: nil db")
	}
	if err := validateRecoveryTransition(req); err != nil {
		return zero, err
	}
	if req.TransitionTS == 0 {
		req.TransitionTS = nowSeconds()
	}

	tx, err := d.conn.BeginTx(ctx, nil)
	if err != nil {
		return zero, fmt.Errorf("state: begin recovery transition: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	// Promote the deferred transaction to a writer before the exact-chain
	// reread. The write handle has one connection, and this no-op UPDATE also
	// reserves SQLite's writer slot against other processes for the remainder
	// of the compare-and-transition sequence.
	if _, err := tx.ExecContext(ctx,
		`UPDATE daemon_meta SET updated_ts = updated_ts WHERE key = '__acd_recovery_tx_guard__'`); err != nil {
		return zero, fmt.Errorf("state: lock recovery transition: %w", err)
	}

	first := req.Expected[0].Event
	var actual []RecoveryChainEvent
	if req.AdoptedBranchHead != "" {
		actual, err = loadUnpublishedRecoveryChainBounded(
			ctx, tx, first.BranchRef, first.BranchGeneration, first.Seq,
			CompletedBranchTransitionProofLimit)
	} else {
		actual, err = loadUnpublishedRecoveryChain(
			ctx, tx, first.BranchRef, first.BranchGeneration, first.Seq)
	}
	if err != nil {
		return zero, err
	}
	matches := sameRecoveryChain(actual, req.Expected)
	if req.AdoptedBranchHead != "" {
		expectedAll := make([]RecoveryChainEvent, 0,
			len(req.Expected)+len(req.ExpectedLater))
		expectedAll = append(expectedAll, req.Expected...)
		expectedAll = append(expectedAll, req.ExpectedLater...)
		matches = sameRecoveryChain(actual, expectedAll)
	} else if req.AllowLaterUnpublished {
		matches = sameRecoveryChainPrefix(actual, req.Expected)
	}
	if !matches {
		return zero, ErrRecoveryChainChanged
	}

	snapshot := RecoverySnapshot{
		CreatedTS:        req.TransitionTS,
		Outcome:          req.TargetState,
		BranchRef:        first.BranchRef,
		BranchGeneration: first.BranchGeneration,
		FirstEventSeq:    first.Seq,
		LastEventSeq:     req.Expected[len(req.Expected)-1].Event.Seq,
		EventCount:       len(req.Expected),
		CommitOID:        req.CommitOID,
		RecoveryRef:      sql.NullString{String: req.RecoveryRef, Valid: req.RecoveryRef != ""},
		Reason:           sql.NullString{String: req.Reason, Valid: req.Reason != ""},
	}
	res, err := tx.ExecContext(ctx, `
INSERT INTO recovery_snapshots(
    created_ts, outcome, branch_ref, branch_generation,
    first_event_seq, last_event_seq, event_count,
    commit_oid, recovery_ref, reason
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		snapshot.CreatedTS, snapshot.Outcome, snapshot.BranchRef,
		snapshot.BranchGeneration, snapshot.FirstEventSeq,
		snapshot.LastEventSeq, snapshot.EventCount, snapshot.CommitOID,
		snapshot.RecoveryRef, snapshot.Reason)
	if err != nil {
		return zero, fmt.Errorf("state: insert recovery snapshot: %w", err)
	}
	snapshot.ID, err = res.LastInsertId()
	if err != nil {
		return zero, fmt.Errorf("state: recovery snapshot id: %w", err)
	}

	decisionKind := req.DecisionKind
	if decisionKind == "" {
		decisionKind = DecisionKindRecoveryPublished
	}
	action := req.ActionTaken
	message := req.UserMessage
	if req.TargetState == EventStateRecovered {
		if req.DecisionKind == "" {
			decisionKind = DecisionKindRecoveryArchived
		}
		if action == "" {
			action = "archived unpublished chain to recovery ref"
		}
		if message == "" {
			message = fmt.Sprintf("Captured work was preserved at %s.", req.RecoveryRef)
		}
	} else {
		if action == "" {
			action = "reconciled unpublished chain as externally published"
		}
		if message == "" {
			message = "An external commit already contains this captured work."
		}
	}

	members := make(map[int64]struct{}, len(req.Expected))
	for ord, expected := range req.Expected {
		ev := expected.Event
		if _, err := tx.ExecContext(ctx, `
INSERT INTO recovery_snapshot_events(snapshot_id, ord, event_seq)
VALUES (?, ?, ?)`, snapshot.ID, ord, ev.Seq); err != nil {
			return zero, fmt.Errorf("state: insert recovery snapshot member seq=%d: %w", ev.Seq, err)
		}

		res, err := tx.ExecContext(ctx, `
UPDATE capture_events
SET state = ?, commit_oid = ?, error = NULL, published_ts = ?
WHERE seq = ?
  AND state = ?
  AND branch_ref = ?
  AND branch_generation = ?
  AND base_head = ?
  AND operation = ?
  AND path = ?
  AND old_path IS ?
  AND fidelity = ?`,
			req.TargetState, req.CommitOID, req.TransitionTS,
			ev.Seq, ev.State, ev.BranchRef, ev.BranchGeneration,
			ev.BaseHead, ev.Operation, ev.Path, ev.OldPath, ev.Fidelity)
		if err != nil {
			return zero, fmt.Errorf("state: transition recovery event seq=%d: %w", ev.Seq, err)
		}
		n, err := res.RowsAffected()
		if err != nil {
			return zero, fmt.Errorf("state: recovery transition rows seq=%d: %w", ev.Seq, err)
		}
		if n != 1 {
			return zero, ErrRecoveryChainChanged
		}
		if _, err := tx.ExecContext(ctx,
			`DELETE FROM planner_state WHERE event_seq = ?`, ev.Seq); err != nil {
			return zero, fmt.Errorf("state: clear recovery planner state seq=%d: %w", ev.Seq, err)
		}
		members[ev.Seq] = struct{}{}

		if _, err := appendDecision(ctx, tx, DecisionRecord{
			DecisionTS:       req.TransitionTS,
			Kind:             decisionKind,
			Path:             sql.NullString{String: ev.Path, Valid: ev.Path != ""},
			Reason:           sql.NullString{String: req.Reason, Valid: req.Reason != ""},
			EventSeq:         sql.NullInt64{Int64: ev.Seq, Valid: true},
			HeadSHA:          sql.NullString{String: req.CommitOID, Valid: true},
			CommitOID:        sql.NullString{String: req.CommitOID, Valid: true},
			BranchRef:        sql.NullString{String: ev.BranchRef, Valid: true},
			BranchGeneration: sql.NullInt64{Int64: ev.BranchGeneration, Valid: true},
			ActionTaken:      sql.NullString{String: action, Valid: true},
			UserMessage:      sql.NullString{String: message, Valid: true},
		}); err != nil {
			return zero, fmt.Errorf("state: append recovery decision seq=%d: %w", ev.Seq, err)
		}
	}

	if err := supersedeRecoveryIntentCandidates(
		ctx, tx, snapshot.ID, first.BranchRef, first.BranchGeneration,
		req.TransitionTS,
	); err != nil {
		return zero, err
	}

	if req.InvalidateShadow {
		if _, err := tx.ExecContext(ctx, `
DELETE FROM shadow_paths
WHERE branch_ref = ? AND branch_generation = ?`,
			first.BranchRef, first.BranchGeneration); err != nil {
			return zero, fmt.Errorf("state: invalidate recovery shadow rows: %w", err)
		}
		marker := fmt.Sprintf("shadow.bootstrapped:%s:%d", first.BranchRef, first.BranchGeneration)
		if _, err := tx.ExecContext(ctx,
			`DELETE FROM daemon_meta WHERE key = ?`, marker); err != nil {
			return zero, fmt.Errorf("state: invalidate recovery shadow marker: %w", err)
		}
	}
	if req.AdoptedBranchHead != "" {
		marker := fmt.Sprintf("shadow.bootstrapped:%s:%d",
			first.BranchRef, first.BranchGeneration)
		if err := replaceShadowGenerationTx(
			ctx, tx, first.BranchRef, first.BranchGeneration,
			marker, req.ShadowRows); err != nil {
			return zero, fmt.Errorf(
				"state: adopt recovery shadow: %w", err)
		}
		const upsertMeta = `
INSERT INTO daemon_meta(key, value, updated_ts) VALUES (?, ?, ?)
ON CONFLICT(key) DO UPDATE SET value = excluded.value,
                               updated_ts = excluded.updated_ts`
		branchToken := "rev:" + req.AdoptedBranchHead + " " + first.BranchRef
		meta := map[string]string{
			selfPublicationMetaBranchGeneration: strconv.FormatInt(
				first.BranchGeneration, 10),
			selfPublicationMetaBranchHead:  req.AdoptedBranchHead,
			selfPublicationMetaBranchToken: branchToken,
			"branch_token_changed_at": strconv.FormatFloat(
				req.TransitionTS, 'f', -1, 64),
			"branch.transition.needs_attention": "",
		}
		for key, value := range meta {
			if _, err := tx.ExecContext(
				ctx, upsertMeta, key, value, req.TransitionTS); err != nil {
				return zero, fmt.Errorf(
					"state: adopt recovery branch meta %s: %w", key, err)
			}
		}
		if _, err := tx.ExecContext(ctx,
			`DELETE FROM daemon_meta WHERE key = 'manual_pause.resumed_at'`); err != nil {
			return zero, fmt.Errorf(
				"state: clear manual resume after recovery adoption: %w", err)
		}
	}

	clearedBreadcrumb, err := clearMatchingRecoveryBreadcrumb(ctx, tx, members,
		first.BranchRef, first.BranchGeneration, req.TargetState,
		req.CommitOID, req.TransitionTS)
	if err != nil {
		return zero, err
	}
	if clearedBreadcrumb {
		for _, key := range []string{
			"last_replay_conflict",
			"last_replay_conflict_legacy",
			"last_replay_error",
			"replay.error_repeat_count",
			"replay.error_last_seen_ts",
		} {
			if _, err := tx.ExecContext(ctx, `DELETE FROM daemon_meta WHERE key = ?`, key); err != nil {
				return zero, fmt.Errorf("state: clear recovery meta %s: %w", key, err)
			}
		}
	}

	if err := tx.Commit(); err != nil {
		return zero, fmt.Errorf("state: commit recovery transition: %w", err)
	}
	return snapshot, nil
}

// supersedeRecoveryIntentCandidates releases every live candidate whose
// active membership intersects the settled recovery snapshot. Recovery can
// move only part of a candidate's membership out of pending state, so all of
// that candidate's active memberships must be released for the remaining
// pending captures to be planned again without stale recovered members.
func supersedeRecoveryIntentCandidates(
	ctx context.Context,
	tx *sql.Tx,
	snapshotID int64,
	branchRef string,
	branchGeneration int64,
	transitionTS float64,
) error {
	rows, err := tx.QueryContext(ctx, `
SELECT DISTINCT candidate.id
FROM intent_candidates candidate
JOIN intent_candidate_events member
  ON member.candidate_id=candidate.id
JOIN recovery_snapshot_events recovered
  ON recovered.event_seq=member.event_seq
WHERE recovered.snapshot_id=?
  AND member.membership_state='active'
  AND candidate.branch_ref=?
  AND candidate.branch_generation=?
  AND candidate.status IN ('open','waiting','ready','soft_published','blocked')
ORDER BY candidate.id`, snapshotID, branchRef, branchGeneration)
	if err != nil {
		return fmt.Errorf("state: inspect recovery intent candidates: %w", err)
	}
	var candidateIDs []string
	for rows.Next() {
		var candidateID string
		if err := rows.Scan(&candidateID); err != nil {
			_ = rows.Close()
			return fmt.Errorf("state: scan recovery intent candidate: %w", err)
		}
		candidateIDs = append(candidateIDs, candidateID)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return fmt.Errorf("state: iterate recovery intent candidates: %w", err)
	}
	if err := rows.Close(); err != nil {
		return fmt.Errorf("state: close recovery intent candidates: %w", err)
	}

	for _, candidateID := range candidateIDs {
		if _, err := tx.ExecContext(ctx, `
UPDATE intent_candidate_events
SET membership_state='superseded'
WHERE candidate_id=? AND membership_state='active'`, candidateID); err != nil {
			return fmt.Errorf(
				"state: supersede recovery candidate %s membership: %w",
				candidateID, err)
		}
		res, err := tx.ExecContext(ctx, `
UPDATE intent_candidates
SET status='superseded', readiness='wait',
    soft_publication_deadline=NULL, updated_ts=?
WHERE id=?
  AND branch_ref=?
  AND branch_generation=?
  AND status IN ('open','waiting','ready','soft_published','blocked')`,
			transitionTS, candidateID, branchRef, branchGeneration)
		if err != nil {
			return fmt.Errorf(
				"state: supersede recovery candidate %s: %w", candidateID, err)
		}
		affected, err := res.RowsAffected()
		if err != nil {
			return fmt.Errorf(
				"state: count superseded recovery candidate %s: %w",
				candidateID, err)
		}
		if affected != 1 {
			return ErrRecoveryChainChanged
		}
	}
	return nil
}

func validateRecoveryTransition(req RecoveryChainTransition) error {
	if len(req.Expected) == 0 {
		return fmt.Errorf("state: TransitionRecoveryChain: empty expected chain")
	}
	if req.TargetState != EventStatePublished && req.TargetState != EventStateRecovered {
		return fmt.Errorf("state: TransitionRecoveryChain: invalid target state %q", req.TargetState)
	}
	if req.CommitOID == "" {
		return fmt.Errorf("state: TransitionRecoveryChain: empty commit oid")
	}
	if req.RecoveryRef == "" {
		return fmt.Errorf("state: TransitionRecoveryChain: protected outcome requires recovery ref")
	}
	if req.InvalidateShadow && req.TargetState != EventStateRecovered {
		return fmt.Errorf("state: TransitionRecoveryChain: shadow invalidation requires recovered outcome")
	}
	if req.AllowLaterUnpublished && req.TargetState != EventStatePublished {
		return fmt.Errorf(
			"state: TransitionRecoveryChain: later unpublished rows require published outcome")
	}
	if req.AllowLaterUnpublished && req.InvalidateShadow {
		return fmt.Errorf(
			"state: TransitionRecoveryChain: later unpublished rows forbid shadow invalidation")
	}
	if len(req.ExpectedLater) > 0 && req.AdoptedBranchHead == "" {
		return fmt.Errorf(
			"state: TransitionRecoveryChain: sealed later rows require branch adoption")
	}
	if req.AdoptedBranchHead != "" {
		if req.TargetState != EventStatePublished || req.InvalidateShadow {
			return fmt.Errorf(
				"state: TransitionRecoveryChain: branch adoption requires published outcome")
		}
		if req.AllowLaterUnpublished != (len(req.ExpectedLater) > 0) {
			return fmt.Errorf(
				"state: TransitionRecoveryChain: later-row adoption identity is inconsistent")
		}
	}

	expectedAll := make([]RecoveryChainEvent, 0,
		len(req.Expected)+len(req.ExpectedLater))
	expectedAll = append(expectedAll, req.Expected...)
	expectedAll = append(expectedAll, req.ExpectedLater...)
	first := expectedAll[0].Event
	if first.BranchRef == "" || first.BranchGeneration < 1 || first.Seq < 1 {
		return fmt.Errorf("state: TransitionRecoveryChain: invalid first event provenance")
	}
	prevSeq := int64(0)
	for i, expected := range expectedAll {
		ev := expected.Event
		if ev.Seq <= prevSeq {
			return fmt.Errorf("state: TransitionRecoveryChain: event seqs not strictly increasing at index %d", i)
		}
		if ev.BranchRef != first.BranchRef || ev.BranchGeneration != first.BranchGeneration {
			return fmt.Errorf("state: TransitionRecoveryChain: mixed branch provenance at seq %d", ev.Seq)
		}
		if ev.BaseHead == "" || ev.Operation == "" || ev.Path == "" || ev.Fidelity == "" {
			return fmt.Errorf("state: TransitionRecoveryChain: incomplete provenance at seq %d", ev.Seq)
		}
		switch ev.State {
		case EventStatePending, EventStateBlockedConflict, EventStateFailed:
		default:
			return fmt.Errorf("state: TransitionRecoveryChain: event %d is not unpublished: %q", ev.Seq, ev.State)
		}
		for ord, op := range expected.Ops {
			if op.EventSeq != ev.Seq || op.Ord != ord || op.Op == "" || op.Path == "" || op.Fidelity == "" {
				return fmt.Errorf("state: TransitionRecoveryChain: invalid op provenance seq=%d ord=%d", ev.Seq, ord)
			}
		}
		prevSeq = ev.Seq
	}
	if req.AdoptedBranchHead != "" {
		marker := fmt.Sprintf("shadow.bootstrapped:%s:%d",
			first.BranchRef, first.BranchGeneration)
		if err := validateShadowReplacement(
			first.BranchRef, first.BranchGeneration,
			marker, req.ShadowRows); err != nil {
			return err
		}
		for i, row := range req.ShadowRows {
			if row.BaseHead != req.AdoptedBranchHead {
				return fmt.Errorf(
					"state: TransitionRecoveryChain: shadow row %d has stale base", i)
			}
		}
	}
	return nil
}

func sameRecoveryChain(actual, expected []RecoveryChainEvent) bool {
	if len(actual) != len(expected) {
		return false
	}
	for i := range actual {
		if actual[i].Event != expected[i].Event || len(actual[i].Ops) != len(expected[i].Ops) {
			return false
		}
		for j := range actual[i].Ops {
			if actual[i].Ops[j] != expected[i].Ops[j] {
				return false
			}
		}
	}
	return true
}

func sameRecoveryChainPrefix(actual, expected []RecoveryChainEvent) bool {
	return len(actual) >= len(expected) &&
		sameRecoveryChain(actual[:len(expected)], expected)
}

func clearMatchingRecoveryBreadcrumb(
	ctx context.Context,
	tx *sql.Tx,
	members map[int64]struct{},
	branchRef string,
	branchGeneration int64,
	targetState string,
	commitOID string,
	transitionTS float64,
) (bool, error) {
	var eventSeq sql.NullInt64
	var pubBranchRef sql.NullString
	var pubGeneration sql.NullInt64
	err := tx.QueryRowContext(ctx, `
SELECT event_seq, branch_ref, branch_generation
FROM publish_state
WHERE id = 1 AND status = 'blocked_conflict'`).Scan(
		&eventSeq, &pubBranchRef, &pubGeneration)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("state: load recovery publish breadcrumb: %w", err)
	}
	if !eventSeq.Valid || !pubBranchRef.Valid || pubBranchRef.String != branchRef ||
		!pubGeneration.Valid || pubGeneration.Int64 != branchGeneration {
		return false, nil
	}
	if _, ok := members[eventSeq.Int64]; !ok {
		return false, nil
	}
	status := "ok"
	targetCommit := sql.NullString{}
	if targetState == EventStatePublished {
		status = EventStatePublished
		targetCommit = sql.NullString{String: commitOID, Valid: true}
	}
	res, err := tx.ExecContext(ctx, `
UPDATE publish_state
SET event_seq = NULL,
	 target_commit_oid = ?,
	 status = ?,
    error = NULL,
    updated_ts = ?
WHERE id = 1
  AND status = 'blocked_conflict'
  AND event_seq = ?
  AND branch_ref = ?
  AND branch_generation = ?`, targetCommit, status, transitionTS,
		eventSeq.Int64, branchRef, branchGeneration)
	if err != nil {
		return false, fmt.Errorf("state: clear recovery publish breadcrumb: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("state: clear recovery publish breadcrumb rows: %w", err)
	}
	return n == 1, nil
}

// RecoverySnapshotByID loads one durable snapshot by primary key.
func RecoverySnapshotByID(ctx context.Context, d *DB, id int64) (RecoverySnapshot, bool, error) {
	if d == nil {
		return RecoverySnapshot{}, false, fmt.Errorf("state: RecoverySnapshotByID: nil db")
	}
	if id < 1 {
		return RecoverySnapshot{}, false, fmt.Errorf("state: RecoverySnapshotByID: invalid id %d", id)
	}
	return queryRecoverySnapshot(ctx, d, `WHERE id = ?`, id)
}

// RecoverySnapshotByRef loads a snapshot by its hidden proof/recovery Git ref.
func RecoverySnapshotByRef(ctx context.Context, d *DB, recoveryRef string) (RecoverySnapshot, bool, error) {
	if d == nil {
		return RecoverySnapshot{}, false, fmt.Errorf("state: RecoverySnapshotByRef: nil db")
	}
	if recoveryRef == "" {
		return RecoverySnapshot{}, false, fmt.Errorf("state: RecoverySnapshotByRef: empty recovery ref")
	}
	return queryRecoverySnapshot(ctx, d, `WHERE recovery_ref = ?`, recoveryRef)
}

func queryRecoverySnapshot(ctx context.Context, d *DB, where string, arg any) (RecoverySnapshot, bool, error) {
	var snapshot RecoverySnapshot
	err := d.readSQL().QueryRowContext(ctx, `
SELECT id, created_ts, outcome, branch_ref, branch_generation,
       first_event_seq, last_event_seq, event_count,
       commit_oid, recovery_ref, reason
FROM recovery_snapshots `+where, arg).Scan(
		&snapshot.ID, &snapshot.CreatedTS, &snapshot.Outcome,
		&snapshot.BranchRef, &snapshot.BranchGeneration,
		&snapshot.FirstEventSeq, &snapshot.LastEventSeq, &snapshot.EventCount,
		&snapshot.CommitOID, &snapshot.RecoveryRef, &snapshot.Reason)
	if errors.Is(err, sql.ErrNoRows) {
		return RecoverySnapshot{}, false, nil
	}
	if err != nil {
		return RecoverySnapshot{}, false, fmt.Errorf("state: query recovery snapshot: %w", err)
	}
	return snapshot, true, nil
}

// RecoverySnapshotEvents returns exact membership in original chain order.
func RecoverySnapshotEvents(ctx context.Context, d *DB, snapshotID int64) ([]RecoverySnapshotEvent, error) {
	if d == nil {
		return nil, fmt.Errorf("state: RecoverySnapshotEvents: nil db")
	}
	if snapshotID < 1 {
		return nil, fmt.Errorf("state: RecoverySnapshotEvents: invalid snapshot id %d", snapshotID)
	}
	rows, err := d.readSQL().QueryContext(ctx, `
SELECT snapshot_id, ord, event_seq
FROM recovery_snapshot_events
WHERE snapshot_id = ?
ORDER BY ord ASC`, snapshotID)
	if err != nil {
		return nil, fmt.Errorf("state: query recovery snapshot events: %w", err)
	}
	defer rows.Close()
	var events []RecoverySnapshotEvent
	for rows.Next() {
		var event RecoverySnapshotEvent
		if err := rows.Scan(&event.SnapshotID, &event.Ord, &event.EventSeq); err != nil {
			return nil, fmt.Errorf("state: scan recovery snapshot event: %w", err)
		}
		events = append(events, event)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("state: iterate recovery snapshot events: %w", err)
	}
	return events, nil
}
