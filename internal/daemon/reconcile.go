package daemon

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
	acdtrace "github.com/KristjanPikhof/Auto-Commit-Daemon/internal/trace"
)

const (
	recoveryCommitIdentityName          = "Auto Commit Daemon"
	recoveryCommitIdentityEmail         = "acd-recovery@localhost"
	maxPublishedRecoveryContextCommits  = 64
	maxPublishedRecoveryAncestryCommits = 4096
)

// RecoveryReconcileOptions identifies one immutable unpublished suffix. The
// generic entrypoint deliberately accepts a pending first row: branch changes
// and dead-branch cleanup must preserve pending-only chains before changing
// generation metadata or reseeding the shadow. Replay uses the narrower
// reconcileActiveBarrierChain wrapper below.
type RecoveryReconcileOptions struct {
	GitDir           string
	BranchRef        string
	BranchGeneration int64
	FirstSeq         int64
	Trigger          string
	Trace            acdtrace.Logger
	// ArchiveOnly is required when a branch/dead-ref transition is already
	// known to have invalidated external-publish proof. It still snapshots and
	// rechecks the exact live branch token, but never compares the chain to the
	// current HEAD. It is also the only mode allowed to archive while HEAD is
	// attached to a missing ref.
	ArchiveOnly bool
	// InvalidateShadow requests crash-safe active-pair shadow invalidation when
	// the outcome is recovered. It is ignored for externally published chains.
	InvalidateShadow bool
	// ExpectedMissingRef is set only by dead-ref sweeps. Reconciliation
	// rechecks it before the final proof checks, then verifies and locks its
	// absence in the same Git transaction that protects the recovery ref while
	// the SQLite state transition runs.
	ExpectedMissingRef string

	// Test-only synchronization points. They are per-call instead of package
	// globals so race-enabled parallel tests cannot interfere with one another.
	afterInitialLiveToken  func()
	beforeLiveTokenRecheck func()
	beforeFinalHeadCheck   func()
	beforeStateTransition  func()
	afterRecoveryRefLocked func()
	decisionKind           string
}

// RecoveryChainResult describes a completed all-or-none chain transition.
// Handled is false when FirstSeq no longer identifies an unpublished row.
type RecoveryChainResult struct {
	Handled     bool
	Outcome     string
	SnapshotID  int64
	RecoveryRef string
	CommitOID   string
	FirstSeq    int64
	LastSeq     int64
	EventCount  int
}

type recoveryPathState struct {
	Path    string
	Present bool
	Mode    string
	OID     string
}

type recoveryLiveState struct {
	token   string
	head    string
	hasHead bool
}

// ReconcileUnpublishedChain proves or preserves the exact unpublished suffix
// beginning at opts.FirstSeq. It never mutates HEAD, the live index, or the
// worktree. The recovery tree is reconstructed from the first event's
// immutable base_head in a private index; live HEAD is used only for the
// external-publish proof and branch-token stability guards. Both published
// and archived outcomes create a deterministic hidden ref before the atomic
// DB transition, keeping the evidence reachable across the Git/SQLite gap.
func ReconcileUnpublishedChain(
	ctx context.Context,
	repoRoot string,
	db *state.DB,
	opts RecoveryReconcileOptions,
) (RecoveryChainResult, error) {
	var result RecoveryChainResult
	if repoRoot == "" || db == nil {
		return result, fmt.Errorf("daemon: reconcile recovery chain: repoRoot and db required")
	}
	if opts.BranchRef == "" || opts.BranchGeneration < 1 || opts.FirstSeq < 1 {
		return result, fmt.Errorf("daemon: reconcile recovery chain: invalid chain selector")
	}
	if err := ctx.Err(); err != nil {
		return result, err
	}

	chain, err := state.LoadUnpublishedRecoveryChain(
		ctx, db, opts.BranchRef, opts.BranchGeneration, opts.FirstSeq,
	)
	if err != nil {
		return result, fmt.Errorf("daemon: reconcile recovery chain: load suffix: %w", err)
	}
	// LoadUnpublishedRecoveryChain intentionally returns seq >= FirstSeq. An
	// exact mismatch means the selected row moved concurrently; never consume
	// a later suffix by accident.
	if len(chain) == 0 || chain[0].Event.Seq != opts.FirstSeq {
		return result, nil
	}
	first := chain[0].Event
	last := chain[len(chain)-1].Event
	recoveryContext, err := state.LoadPublishedRecoveryContext(
		ctx, db, opts.BranchRef, opts.BranchGeneration,
		first.Seq, last.Seq,
	)
	if err != nil {
		return result, fmt.Errorf("daemon: reconcile recovery chain: load published context: %w", err)
	}
	recoveryContext, err = excludeSupersededPublishedContext(ctx, db, recoveryContext)
	if err != nil {
		return result, err
	}
	recoveryContext, err = excludeSeedRepresentedPublishedContext(
		ctx, repoRoot, first.BaseHead, recoveryContext,
	)
	if err != nil {
		return result, err
	}
	recoveryContext, err = descendantPublishedContext(ctx, repoRoot, first.BaseHead, recoveryContext)
	if err != nil {
		return result, err
	}
	recoveryContext, err = representedPublishedContext(ctx, repoRoot, recoveryContext)
	if err != nil {
		return result, err
	}

	gitDir := opts.GitDir
	if gitDir == "" {
		gitDir, err = git.AbsoluteGitDir(ctx, repoRoot)
		if err != nil {
			return result, fmt.Errorf("daemon: reconcile recovery chain: resolve git dir: %w", err)
		}
	}

	live, err := currentRecoveryLiveState(ctx, repoRoot,
		opts.afterInitialLiveToken, opts.beforeLiveTokenRecheck)
	if err != nil {
		return result, err
	}
	if !live.hasHead && !opts.ArchiveOnly {
		return result, fmt.Errorf("daemon: reconcile recovery chain: HEAD is missing; archive-only mode required")
	}
	if err := requireRecoveryRefMissing(ctx, repoRoot, opts.ExpectedMissingRef); err != nil {
		return result, err
	}

	baseHead := chain[0].Event.BaseHead
	materialization := make([]state.RecoveryChainEvent, 0, len(recoveryContext)+len(chain))
	materialization = append(materialization, recoveryContext...)
	materialization = append(materialization, chain...)
	if err := validateRecoveryObjects(ctx, repoRoot, materialization); err != nil {
		return result, err
	}
	treeOID, finalState, err := materializeRecoveryTree(ctx, repoRoot, gitDir, baseHead, recoveryContext, chain)
	if err != nil {
		return result, err
	}
	if err := requireStableRecoveryLiveState(ctx, repoRoot, live); err != nil {
		return result, err
	}
	if err := requireRecoveryRefMissing(ctx, repoRoot, opts.ExpectedMissingRef); err != nil {
		return result, err
	}

	matched := false
	if live.hasHead && !opts.ArchiveOnly {
		matched, err = recoveryChainMatchesHEAD(ctx, repoRoot, live.head, chain, finalState)
		if err != nil {
			return result, err
		}
	}

	trigger := strings.TrimSpace(opts.Trigger)
	if trigger == "" {
		trigger = "automatic_chain_reconciliation"
	}
	transition := state.RecoveryChainTransition{
		Expected: chain,
		Reason:   trigger,
	}

	if matched {
		ref := recoveryProofRefName(opts.BranchRef, opts.BranchGeneration, first.Seq, last.Seq, live.head)
		commitOID, err := ensurePublishedProofRef(ctx, repoRoot, ref, live.head, chain, finalState)
		if err != nil {
			return result, err
		}
		transition.TargetState = state.EventStatePublished
		transition.CommitOID = commitOID
		transition.RecoveryRef = ref
		transition.DecisionKind = opts.decisionKind
	} else {
		ref := recoveryRefName(opts.BranchRef, opts.BranchGeneration, first.Seq, last.Seq, baseHead, treeOID)
		commitOID, err := ensureRecoveryCommit(ctx, repoRoot, ref, treeOID, baseHead, chain)
		if err != nil {
			return result, err
		}
		transition.TargetState = state.EventStateRecovered
		transition.CommitOID = commitOID
		transition.RecoveryRef = ref
		transition.InvalidateShadow = opts.InvalidateShadow
	}

	if opts.beforeFinalHeadCheck != nil {
		opts.beforeFinalHeadCheck()
	}
	if err := requireStableRecoveryLiveState(ctx, repoRoot, live); err != nil {
		return result, err
	}
	if err := requireRecoveryRefMissing(ctx, repoRoot, opts.ExpectedMissingRef); err != nil {
		return result, err
	}
	if opts.beforeStateTransition != nil {
		opts.beforeStateTransition()
	}
	if err := requireStableRecoveryLiveState(ctx, repoRoot, live); err != nil {
		return result, err
	}
	if err := requireRecoveryRefMissing(ctx, repoRoot, opts.ExpectedMissingRef); err != nil {
		return result, err
	}
	var snapshot state.RecoverySnapshot
	protectedTransition := func() error {
		if opts.afterRecoveryRefLocked != nil {
			opts.afterRecoveryRefLocked()
		}
		var transitionErr error
		snapshot, transitionErr = state.TransitionRecoveryChain(ctx, db, transition)
		return transitionErr
	}
	if opts.ExpectedMissingRef == "" {
		err = git.WithLockedRecoveryRef(
			ctx, repoRoot, transition.RecoveryRef, transition.CommitOID, protectedTransition,
		)
	} else {
		err = git.WithLockedRecoveryRefAndAbsentRef(
			ctx, repoRoot, transition.RecoveryRef, transition.CommitOID,
			opts.ExpectedMissingRef, protectedTransition,
		)
	}
	if err != nil {
		return result, fmt.Errorf("daemon: reconcile recovery chain: protected transition: %w", err)
	}

	result = RecoveryChainResult{
		Handled:     true,
		Outcome:     snapshot.Outcome,
		SnapshotID:  snapshot.ID,
		RecoveryRef: snapshot.RecoveryRef.String,
		CommitOID:   snapshot.CommitOID,
		FirstSeq:    snapshot.FirstEventSeq,
		LastSeq:     snapshot.LastEventSeq,
		EventCount:  snapshot.EventCount,
	}
	traceReplay(opts.Trace, repoRoot, CaptureContext{
		BranchRef:        opts.BranchRef,
		BranchGeneration: opts.BranchGeneration,
		BaseHead:         live.head,
	}, first, "replay.chain_reconcile", snapshot.Outcome, trigger, map[string]any{
		"snapshot_id":  snapshot.ID,
		"outcome":      snapshot.Outcome,
		"commit":       snapshot.CommitOID,
		"recovery_ref": snapshot.RecoveryRef.String,
		"first_seq":    snapshot.FirstEventSeq,
		"last_seq":     snapshot.LastEventSeq,
		"event_count":  snapshot.EventCount,
		"source_head":  baseHead,
	})
	return result, nil
}

// reconcileActiveBarrierChain uses the existence of any terminal row as its
// gate, then delegates the active pair's entire unpublished chain from the
// earliest pending/blocked/failed row. This keeps whole-pair shadow
// invalidation aligned with the exact rows transitioned out of replay.
func reconcileActiveBarrierChain(
	ctx context.Context,
	repoRoot string,
	db *state.DB,
	cctx CaptureContext,
	opts ReplayOpts,
) (RecoveryChainResult, error) {
	var result RecoveryChainResult
	_, _, ok, err := firstRecoveryBarrier(ctx, db, cctx.BranchRef, cctx.BranchGeneration)
	if err != nil || !ok {
		return result, err
	}
	seq, ok, err := firstUnpublishedRecoverySeq(ctx, db, cctx.BranchRef, cctx.BranchGeneration)
	if err != nil || !ok {
		return result, err
	}
	return ReconcileUnpublishedChain(ctx, repoRoot, db, RecoveryReconcileOptions{
		GitDir:           opts.GitDir,
		BranchRef:        cctx.BranchRef,
		BranchGeneration: cctx.BranchGeneration,
		FirstSeq:         seq,
		Trigger:          "replay_terminal_barrier",
		Trace:            opts.Trace,
		InvalidateShadow: true,
	})
}

// reconcileTransitionPair protects every unpublished row for one exact branch
// token before callers change generation metadata or reseed shadow state. A
// valid live HEAD always gets the normal exact proof; archiveOnly is reserved
// for a missing/unresolvable live HEAD. A missing pair is an idempotent no-op.
// Legacy bare-rev tokens have no branch ref, so they reconcile every exact
// unpublished pair at that generation instead of falling back to unsafe
// generation-wide cleanup.
func reconcileTransitionPair(
	ctx context.Context,
	repoRoot string,
	gitDir string,
	db *state.DB,
	branchRef string,
	branchGeneration int64,
	archiveOnly bool,
	expectedMissingRef string,
	trigger string,
	trace acdtrace.Logger,
) (RecoveryChainResult, error) {
	var result RecoveryChainResult
	if branchRef == "" && branchGeneration > 0 {
		rows, err := db.ReadSQL().QueryContext(ctx, `
SELECT DISTINCT branch_ref
FROM capture_events
WHERE branch_generation = ?
  AND branch_ref != ''
  AND state IN (?, ?, ?)
ORDER BY branch_ref`, branchGeneration,
			state.EventStatePending, state.EventStateBlockedConflict, state.EventStateFailed)
		if err != nil {
			return result, fmt.Errorf("daemon: reconcile recovery generation: list exact pairs: %w", err)
		}
		var refs []string
		for rows.Next() {
			var ref string
			if err := rows.Scan(&ref); err != nil {
				_ = rows.Close()
				return result, fmt.Errorf("daemon: reconcile recovery generation: scan branch ref: %w", err)
			}
			refs = append(refs, ref)
		}
		if err := rows.Err(); err != nil {
			_ = rows.Close()
			return result, fmt.Errorf("daemon: reconcile recovery generation: iterate branch refs: %w", err)
		}
		if err := rows.Close(); err != nil {
			return result, fmt.Errorf("daemon: reconcile recovery generation: close branch refs: %w", err)
		}
		for _, ref := range refs {
			pair, err := reconcileTransitionPair(ctx, repoRoot, gitDir, db, ref,
				branchGeneration, archiveOnly, expectedMissingRef, trigger, trace)
			if err != nil {
				return result, err
			}
			if !pair.Handled {
				continue
			}
			if !result.Handled {
				result = pair
				continue
			}
			result.EventCount += pair.EventCount
			result.LastSeq = pair.LastSeq
			if result.Outcome != pair.Outcome {
				result.Outcome = "mixed"
			}
			result.RecoveryRef = pair.RecoveryRef
		}
		return result, nil
	}
	seq, ok, err := firstUnpublishedRecoverySeq(ctx, db, branchRef, branchGeneration)
	if err != nil || !ok {
		return result, err
	}
	return ReconcileUnpublishedChain(ctx, repoRoot, db, RecoveryReconcileOptions{
		GitDir:             gitDir,
		BranchRef:          branchRef,
		BranchGeneration:   branchGeneration,
		FirstSeq:           seq,
		Trigger:            trigger,
		Trace:              trace,
		ArchiveOnly:        archiveOnly,
		ExpectedMissingRef: expectedMissingRef,
	})
}

func firstUnpublishedRecoverySeq(
	ctx context.Context,
	db *state.DB,
	branchRef string,
	branchGeneration int64,
) (seq int64, ok bool, err error) {
	if db == nil || branchRef == "" || branchGeneration < 1 {
		return 0, false, nil
	}
	err = db.ReadSQL().QueryRowContext(ctx, `
SELECT seq
FROM capture_events
WHERE branch_ref = ?
  AND branch_generation = ?
  AND state IN (?, ?, ?)
ORDER BY seq ASC
LIMIT 1`, branchRef, branchGeneration,
		state.EventStatePending, state.EventStateBlockedConflict, state.EventStateFailed,
	).Scan(&seq)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, fmt.Errorf("daemon: reconcile recovery chain: find first unpublished row: %w", err)
	}
	return seq, true, nil
}

func firstRecoveryBarrier(
	ctx context.Context,
	db *state.DB,
	branchRef string,
	branchGeneration int64,
) (seq int64, eventState string, ok bool, err error) {
	if db == nil || branchRef == "" || branchGeneration < 1 {
		return 0, "", false, nil
	}
	err = db.ReadSQL().QueryRowContext(ctx, `
SELECT seq, state
FROM capture_events
WHERE branch_ref = ?
  AND branch_generation = ?
  AND state IN (?, ?)
ORDER BY seq ASC
LIMIT 1`, branchRef, branchGeneration,
		state.EventStateBlockedConflict, state.EventStateFailed,
	).Scan(&seq, &eventState)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, "", false, nil
	}
	if err != nil {
		return 0, "", false, fmt.Errorf("daemon: reconcile recovery chain: find barrier: %w", err)
	}
	return seq, eventState, true, nil
}

func representedPublishedContext(
	ctx context.Context,
	repoRoot string,
	recoveryContext []state.RecoveryChainEvent,
) ([]state.RecoveryChainEvent, error) {
	groups, err := groupPublishedContextByCommit(recoveryContext)
	if err != nil {
		return nil, err
	}

	represented := make([]state.RecoveryChainEvent, 0, len(recoveryContext))
	for _, group := range groups {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		commitOID := group[0].Event.CommitOID.String
		components := splitPublishedContextByConnectedPaths(group)
		requiredPaths := make(map[string]struct{})
		for _, component := range components {
			initialState, cumulativeState := recoveryGroupBoundaryStates(component)
			for _, pathState := range append(initialState, cumulativeState...) {
				requiredPaths[pathState.Path] = struct{}{}
			}
		}
		paths := make([]string, 0, len(requiredPaths))
		for path := range requiredPaths {
			paths = append(paths, path)
		}
		sort.Strings(paths)
		entries, err := git.LsTree(ctx, repoRoot, commitOID, false, git.LiteralPathspecs(paths)...)
		if err != nil {
			return nil, fmt.Errorf("daemon: reconcile recovery chain: read published context commit %s: %w",
				commitOID, err)
		}
		commitEntries := make(map[string]git.TreeEntry, len(entries))
		for _, entry := range entries {
			commitEntries[entry.Path] = entry
		}
		for _, component := range components {
			initialState, cumulativeState := recoveryGroupBoundaryStates(component)
			afterMatch, err := recoveryStatesMatchEntries(ctx, commitEntries, cumulativeState)
			if err != nil {
				return nil, fmt.Errorf("daemon: reconcile recovery chain: prove published context group %d-%d cumulative after-state: %w",
					component[0].Event.Seq, component[len(component)-1].Event.Seq, err)
			}
			if afterMatch {
				represented = append(represented, component...)
				continue
			}
			beforeMatch, err := recoveryStatesMatchEntries(ctx, commitEntries, initialState)
			if err != nil {
				return nil, fmt.Errorf("daemon: reconcile recovery chain: prove published context group %d-%d initial before-state: %w",
					component[0].Event.Seq, component[len(component)-1].Event.Seq, err)
			}
			if !beforeMatch {
				return nil, fmt.Errorf("daemon: reconcile recovery chain: published context group %d-%d commit represents neither initial before nor cumulative after state",
					component[0].Event.Seq, component[len(component)-1].Event.Seq)
			}
			// The commit still contains the component's initial before-state,
			// so an external change superseded every member without an
			// explicit superseded decision. Skip it to avoid resurrection.
		}
	}
	sort.Slice(represented, func(i, j int) bool {
		return represented[i].Event.Seq < represented[j].Event.Seq
	})
	return represented, nil
}

func excludeSupersededPublishedContext(
	ctx context.Context,
	db *state.DB,
	recoveryContext []state.RecoveryChainEvent,
) ([]state.RecoveryChainEvent, error) {
	filtered := make([]state.RecoveryChainEvent, 0, len(recoveryContext))
	for _, item := range recoveryContext {
		decisions, err := state.DecisionsForEvent(ctx, db, item.Event.Seq, 1000)
		if err != nil {
			return nil, fmt.Errorf("daemon: reconcile recovery chain: load published context decision seq=%d: %w", item.Event.Seq, err)
		}
		superseded := false
		for _, decision := range decisions {
			if decision.Kind == state.DecisionKindSupersededExternal {
				superseded = true
				break
			}
		}
		if !superseded {
			filtered = append(filtered, item)
		}
	}
	return filtered, nil
}

func groupPublishedContextByCommit(
	recoveryContext []state.RecoveryChainEvent,
) ([][]state.RecoveryChainEvent, error) {
	groups, err := collectPublishedContextByCommit(recoveryContext)
	if err != nil {
		return nil, err
	}
	if len(groups) > maxPublishedRecoveryContextCommits {
		return nil, fmt.Errorf("daemon: reconcile recovery chain: published context exceeds bounded limit of %d commits",
			maxPublishedRecoveryContextCommits)
	}
	return groups, nil
}

func collectPublishedContextByCommit(
	recoveryContext []state.RecoveryChainEvent,
) ([][]state.RecoveryChainEvent, error) {
	byCommit := make(map[string][]state.RecoveryChainEvent)
	commitOrder := make([]string, 0)
	for _, item := range recoveryContext {
		if !item.Event.CommitOID.Valid || item.Event.CommitOID.String == "" {
			return nil, fmt.Errorf("daemon: reconcile recovery chain: published context seq=%d has no commit", item.Event.Seq)
		}
		commitOID := item.Event.CommitOID.String
		if _, exists := byCommit[commitOID]; !exists {
			commitOrder = append(commitOrder, commitOID)
		}
		byCommit[commitOID] = append(byCommit[commitOID], item)
	}
	groups := make([][]state.RecoveryChainEvent, 0, len(commitOrder))
	for _, commitOID := range commitOrder {
		groups = append(groups, byCommit[commitOID])
	}
	return groups, nil
}

func excludeSeedRepresentedPublishedContext(
	ctx context.Context,
	repoRoot string,
	baseHead string,
	recoveryContext []state.RecoveryChainEvent,
) ([]state.RecoveryChainEvent, error) {
	requiredPaths := make(map[string]struct{})
	for _, item := range recoveryContext {
		for _, path := range touchedPaths(item.Ops) {
			requiredPaths[path] = struct{}{}
		}
	}
	if len(requiredPaths) == 0 {
		return nil, nil
	}
	paths := make([]string, 0, len(requiredPaths))
	for path := range requiredPaths {
		paths = append(paths, path)
	}
	sort.Strings(paths)
	entries, err := git.LsTree(ctx, repoRoot, baseHead, false, git.LiteralPathspecs(paths)...)
	if err != nil {
		return nil, fmt.Errorf("daemon: reconcile recovery chain: read published context seed %s: %w",
			baseHead, err)
	}
	seedEntries := make(map[string]git.IndexEntry, len(entries))
	for _, entry := range entries {
		seedEntries[entry.Path] = git.IndexEntry{
			Mode: entry.Mode,
			OID:  entry.OID,
			Path: entry.Path,
		}
	}

	keep := make([][]bool, len(recoveryContext))
	for i := len(recoveryContext) - 1; i >= 0; i-- {
		item := recoveryContext[i]
		keep[i] = make([]bool, len(item.Ops))
		for opIndex := len(item.Ops) - 1; opIndex >= 0; opIndex-- {
			op := item.Ops[opIndex]
			afterState := recoveryOpStates([]state.CaptureOp{op}, true)
			if recoveryStatesMatchIndex(seedEntries, afterState) {
				setRecoveryIndexStates(seedEntries, recoveryOpStates([]state.CaptureOp{op}, false))
				continue
			}
			keep[i][opIndex] = true
		}
	}

	filtered := make([]state.RecoveryChainEvent, 0, len(recoveryContext))
	for i, item := range recoveryContext {
		ops := make([]state.CaptureOp, 0, len(item.Ops))
		for opIndex, op := range item.Ops {
			if keep[i][opIndex] {
				ops = append(ops, op)
			}
		}
		if len(ops) > 0 {
			item.Ops = ops
			filtered = append(filtered, item)
		}
	}
	return filtered, nil
}

func recoveryStatesMatchIndex(
	entries map[string]git.IndexEntry,
	states []recoveryPathState,
) bool {
	for _, want := range states {
		entry, present := entries[want.Path]
		if !want.Present {
			if present {
				return false
			}
			continue
		}
		if !present || entry.Mode != want.Mode || entry.OID != want.OID {
			return false
		}
	}
	return len(states) > 0
}

func setRecoveryIndexStates(
	entries map[string]git.IndexEntry,
	states []recoveryPathState,
) {
	for _, state := range states {
		if !state.Present {
			delete(entries, state.Path)
			continue
		}
		entries[state.Path] = git.IndexEntry{
			Mode: state.Mode,
			OID:  state.OID,
			Path: state.Path,
		}
	}
}

func splitPublishedContextByConnectedPaths(
	group []state.RecoveryChainEvent,
) [][]state.RecoveryChainEvent {
	parent := make(map[string]string)
	var find func(string) string
	find = func(path string) string {
		root, ok := parent[path]
		if !ok {
			parent[path] = path
			return path
		}
		if root != path {
			parent[path] = find(root)
		}
		return parent[path]
	}
	union := func(paths []string) {
		if len(paths) == 0 {
			return
		}
		root := find(paths[0])
		for _, path := range paths[1:] {
			other := find(path)
			if other != root {
				parent[other] = root
			}
		}
	}
	for _, item := range group {
		for _, op := range item.Ops {
			union(touchedPaths([]state.CaptureOp{op}))
		}
	}

	componentOrder := make([]string, 0)
	seenComponent := make(map[string]struct{})
	byComponent := make(map[string][]state.RecoveryChainEvent)
	for _, item := range group {
		eventOps := make(map[string][]state.CaptureOp)
		eventOrder := make([]string, 0)
		for _, op := range item.Ops {
			paths := touchedPaths([]state.CaptureOp{op})
			if len(paths) == 0 {
				continue
			}
			root := find(paths[0])
			if _, seen := seenComponent[root]; !seen {
				seenComponent[root] = struct{}{}
				componentOrder = append(componentOrder, root)
			}
			if _, seen := eventOps[root]; !seen {
				eventOrder = append(eventOrder, root)
			}
			eventOps[root] = append(eventOps[root], op)
		}
		for _, root := range eventOrder {
			member := item
			member.Ops = eventOps[root]
			byComponent[root] = append(byComponent[root], member)
		}
	}
	components := make([][]state.RecoveryChainEvent, 0, len(componentOrder))
	for _, root := range componentOrder {
		components = append(components, byComponent[root])
	}
	return components
}

func descendantPublishedContext(
	ctx context.Context,
	repoRoot string,
	baseHead string,
	recoveryContext []state.RecoveryChainEvent,
) ([]state.RecoveryChainEvent, error) {
	groups, err := collectPublishedContextByCommit(recoveryContext)
	if err != nil {
		return nil, err
	}
	if len(groups) == 0 {
		return nil, nil
	}

	// Ask one bounded rev-list process which retained commits actually descend
	// from the recovery seed. Applying the proof cap before this filter would
	// let a long, already-ancestral published history block recovery even
	// though none of those commits needs materialization proof.
	if !validRecoveryCommitOID(baseHead) {
		return nil, fmt.Errorf("daemon: reconcile recovery chain: invalid base commit %q", baseHead)
	}
	args := []string{
		"rev-list",
		"--ancestry-path",
		fmt.Sprintf("--max-count=%d", maxPublishedRecoveryAncestryCommits+1),
	}
	descendantCandidates := make(map[string]struct{}, len(groups))
	for i, group := range groups {
		commitOID := group[0].Event.CommitOID.String
		if !validRecoveryCommitOID(commitOID) {
			return nil, fmt.Errorf("daemon: reconcile recovery chain: invalid published context commit %q", commitOID)
		}
		descendantCandidates[commitOID] = struct{}{}
		if i == 0 {
			args = append(args, baseHead+".."+commitOID)
		} else {
			args = append(args, commitOID)
		}
	}
	maxOutput := int64((maxPublishedRecoveryAncestryCommits + 1) * (64 + 1))
	out, err := git.RunWithLimit(ctx, git.RunOpts{
		Dir:     repoRoot,
		Timeout: git.DefaultReadTimeout,
	}, maxOutput, args...)
	if err != nil {
		return nil, fmt.Errorf("daemon: reconcile recovery chain: batch-filter published context descendants of base %s: %w",
			baseHead, err)
	}
	traversedCommits := strings.Fields(string(out))
	if len(traversedCommits) > maxPublishedRecoveryAncestryCommits {
		return nil, fmt.Errorf("daemon: reconcile recovery chain: published context ancestry exceeds bounded limit of %d commits",
			maxPublishedRecoveryAncestryCommits)
	}
	descendantCommits := make(map[string]struct{}, len(groups))
	for _, commitOID := range traversedCommits {
		if _, expected := descendantCandidates[commitOID]; expected {
			descendantCommits[commitOID] = struct{}{}
		}
	}
	if len(descendantCommits) > maxPublishedRecoveryContextCommits {
		return nil, fmt.Errorf("daemon: reconcile recovery chain: published context exceeds bounded limit of %d descendant commits",
			maxPublishedRecoveryContextCommits)
	}

	filtered := make([]state.RecoveryChainEvent, 0, len(recoveryContext))
	for _, group := range groups {
		commitOID := group[0].Event.CommitOID.String
		if _, descends := descendantCommits[commitOID]; descends {
			filtered = append(filtered, group...)
		}
	}
	sort.Slice(filtered, func(i, j int) bool {
		return filtered[i].Event.Seq < filtered[j].Event.Seq
	})
	return filtered, nil
}

func validRecoveryCommitOID(oid string) bool {
	if len(oid) != 40 && len(oid) != 64 {
		return false
	}
	_, err := hex.DecodeString(oid)
	return err == nil
}

func recoveryGroupBoundaryStates(group []state.RecoveryChainEvent) ([]recoveryPathState, []recoveryPathState) {
	initialByPath := make(map[string]recoveryPathState)
	cumulativeByPath := make(map[string]recoveryPathState)
	for _, item := range group {
		for _, op := range item.Ops {
			for _, pathState := range recoveryOpStates([]state.CaptureOp{op}, false) {
				if _, seen := initialByPath[pathState.Path]; !seen {
					initialByPath[pathState.Path] = pathState
				}
			}
			for _, pathState := range recoveryOpStates([]state.CaptureOp{op}, true) {
				cumulativeByPath[pathState.Path] = pathState
			}
		}
	}
	return sortedRecoveryPathStates(initialByPath), sortedRecoveryPathStates(cumulativeByPath)
}

func sortedRecoveryPathStates(byPath map[string]recoveryPathState) []recoveryPathState {
	paths := make([]string, 0, len(byPath))
	for path := range byPath {
		paths = append(paths, path)
	}
	sort.Strings(paths)
	states := make([]recoveryPathState, 0, len(paths))
	for _, path := range paths {
		states = append(states, byPath[path])
	}
	return states
}

func recoveryOpStates(ops []state.CaptureOp, after bool) []recoveryPathState {
	byPath := make(map[string]recoveryPathState)
	for _, op := range ops {
		if after {
			switch op.Op {
			case "create", "modify", "mode":
				byPath[op.Path] = recoveryPathState{Path: op.Path, Present: true, Mode: op.AfterMode.String, OID: op.AfterOID.String}
			case "delete":
				byPath[op.Path] = recoveryPathState{Path: op.Path}
			case "rename":
				byPath[op.OldPath.String] = recoveryPathState{Path: op.OldPath.String}
				byPath[op.Path] = recoveryPathState{Path: op.Path, Present: true, Mode: op.AfterMode.String, OID: op.AfterOID.String}
			}
			continue
		}
		switch op.Op {
		case "create":
			byPath[op.Path] = recoveryPathState{Path: op.Path}
		case "modify", "mode", "delete":
			byPath[op.Path] = recoveryPathState{Path: op.Path, Present: true, Mode: op.BeforeMode.String, OID: op.BeforeOID.String}
		case "rename":
			byPath[op.OldPath.String] = recoveryPathState{Path: op.OldPath.String, Present: true, Mode: op.BeforeMode.String, OID: op.BeforeOID.String}
			byPath[op.Path] = recoveryPathState{Path: op.Path}
		}
	}
	return sortedRecoveryPathStates(byPath)
}

func validateRecoveryObjects(ctx context.Context, repoRoot string, chain []state.RecoveryChainEvent) error {
	required := make(map[string]string)
	for _, item := range chain {
		if err := ctx.Err(); err != nil {
			return err
		}
		ev := item.Event
		if err := addRecoveryObjectRequirement(required, ev.BaseHead, "commit"); err != nil {
			return fmt.Errorf("daemon: reconcile recovery chain: base commit seq=%d: %w", ev.Seq, err)
		}
		if problem := validateOps(item.Ops); problem != "" {
			return fmt.Errorf("daemon: reconcile recovery chain: invalid ops seq=%d: %s", ev.Seq, problem)
		}
		if len(item.Ops) == 0 {
			return fmt.Errorf("daemon: reconcile recovery chain: event %d has no operations", ev.Seq)
		}
		for _, op := range item.Ops {
			if op.Op == "rename" && (!op.BeforeOID.Valid || !op.BeforeMode.Valid) {
				return fmt.Errorf("daemon: reconcile recovery chain: rename %s lacks immutable before-state", op.Path)
			}
			for _, object := range recoveryOpObjects(op) {
				kind, err := recoveryObjectKind(object.mode)
				if err != nil {
					return fmt.Errorf("daemon: reconcile recovery chain: seq=%d path=%s: %w", ev.Seq, op.Path, err)
				}
				if err := addRecoveryObjectRequirement(required, object.oid, kind); err != nil {
					return fmt.Errorf("daemon: reconcile recovery chain: seq=%d path=%s: %w", ev.Seq, op.Path, err)
				}
			}
		}
	}
	oids := make([]string, 0, len(required))
	for oid := range required {
		oids = append(oids, oid)
	}
	sort.Strings(oids)
	var input strings.Builder
	for _, oid := range oids {
		input.WriteString(oid)
		input.WriteByte('\n')
	}
	out, err := git.Run(ctx, git.RunOpts{Dir: repoRoot, Stdin: strings.NewReader(input.String())},
		"cat-file", "--batch-check=%(objectname) %(objecttype)")
	if err != nil {
		return fmt.Errorf("daemon: reconcile recovery chain: validate objects: %w", err)
	}
	lines := strings.Split(strings.TrimSpace(string(out)), "\n")
	if len(lines) != len(oids) {
		return fmt.Errorf("daemon: reconcile recovery chain: object validation returned %d rows, want %d", len(lines), len(oids))
	}
	for i, line := range lines {
		fields := strings.Fields(line)
		if len(fields) == 2 && fields[0] == oids[i] && fields[1] == "missing" {
			return fmt.Errorf("daemon: reconcile recovery chain: missing %s object %s", required[oids[i]], oids[i])
		}
		if len(fields) != 2 || fields[0] != oids[i] || fields[1] != required[oids[i]] {
			return fmt.Errorf("daemon: reconcile recovery chain: object %s is %q, want %s", oids[i], line, required[oids[i]])
		}
	}
	return nil
}

type recoveryObject struct {
	mode string
	oid  string
}

func recoveryOpObjects(op state.CaptureOp) []recoveryObject {
	objects := make([]recoveryObject, 0, 2)
	if op.BeforeOID.Valid && op.BeforeMode.Valid {
		objects = append(objects, recoveryObject{mode: op.BeforeMode.String, oid: op.BeforeOID.String})
	}
	if op.AfterOID.Valid && op.AfterMode.Valid {
		objects = append(objects, recoveryObject{mode: op.AfterMode.String, oid: op.AfterOID.String})
	}
	return objects
}

func recoveryObjectKind(mode string) (string, error) {
	switch mode {
	case git.RegularFileMode, "100755", git.SymlinkMode:
		return "blob", nil
	case "160000":
		return "commit", nil
	default:
		return "", fmt.Errorf("unsupported git mode %q", mode)
	}
}

func addRecoveryObjectRequirement(required map[string]string, oid, kind string) error {
	if oid == "" {
		return fmt.Errorf("empty %s object oid", kind)
	}
	if previous, ok := required[oid]; ok && previous != kind {
		return fmt.Errorf("object %s required as both %s and %s", oid, previous, kind)
	}
	required[oid] = kind
	return nil
}

func materializeRecoveryTree(
	ctx context.Context,
	repoRoot string,
	gitDir string,
	baseHead string,
	recoveryContext []state.RecoveryChainEvent,
	chain []state.RecoveryChainEvent,
) (string, []recoveryPathState, error) {
	indexParent := filepath.Join(gitDir, "acd")
	if err := os.MkdirAll(indexParent, 0o700); err != nil {
		return "", nil, fmt.Errorf("daemon: reconcile recovery chain: mkdir index parent: %w", err)
	}
	tmpDir, err := os.MkdirTemp(indexParent, "recovery-")
	if err != nil {
		return "", nil, fmt.Errorf("daemon: reconcile recovery chain: mkdir temp index: %w", err)
	}
	defer os.RemoveAll(tmpDir)
	if err := os.Chmod(tmpDir, 0o700); err != nil {
		return "", nil, fmt.Errorf("daemon: reconcile recovery chain: chmod temp index: %w", err)
	}
	indexFile := filepath.Join(tmpDir, "idx")
	if err := git.ReadTree(ctx, repoRoot, indexFile, baseHead); err != nil {
		return "", nil, fmt.Errorf("daemon: reconcile recovery chain: seed base tree %s: %w", baseHead, err)
	}

	ordered := make([]state.RecoveryChainEvent, 0, len(recoveryContext)+len(chain))
	ordered = append(ordered, recoveryContext...)
	ordered = append(ordered, chain...)
	sort.Slice(ordered, func(i, j int) bool {
		return ordered[i].Event.Seq < ordered[j].Event.Seq
	})
	chainSeqs := make(map[int64]struct{}, len(chain))
	for _, item := range chain {
		chainSeqs[item.Event.Seq] = struct{}{}
	}
	allTouched := make(map[string]struct{})
	chainTouched := make(map[string]struct{})
	for _, item := range ordered {
		for _, path := range touchedPaths(item.Ops) {
			allTouched[path] = struct{}{}
			if _, unpublished := chainSeqs[item.Event.Seq]; unpublished {
				chainTouched[path] = struct{}{}
			}
		}
	}
	if len(chainTouched) == 0 {
		return "", nil, fmt.Errorf("daemon: reconcile recovery chain: empty materialized chain")
	}
	paths := make([]string, 0, len(allTouched))
	for path := range allTouched {
		paths = append(paths, path)
	}
	sort.Strings(paths)
	entries, err := git.LsFilesIndex(ctx, repoRoot, indexFile, git.LiteralPathspecs(paths)...)
	if err != nil {
		return "", nil, fmt.Errorf("daemon: reconcile recovery chain: read final index: %w", err)
	}
	byPath := make(map[string]git.IndexEntry, len(entries))
	for _, entry := range entries {
		if entry.Stage != 0 {
			return "", nil, fmt.Errorf("daemon: reconcile recovery chain: unmerged final index path %s", entry.Path)
		}
		if _, exists := byPath[entry.Path]; exists {
			return "", nil, fmt.Errorf("daemon: reconcile recovery chain: duplicate final index path %s", entry.Path)
		}
		byPath[entry.Path] = entry
	}
	for _, item := range ordered {
		if err := ctx.Err(); err != nil {
			return "", nil, err
		}
		if conflict := applyRecoveryOpsInMemory(byPath, item.Ops); conflict != "" {
			return "", nil, fmt.Errorf("daemon: reconcile recovery chain: provenance mismatch seq=%d: %s", item.Event.Seq, conflict)
		}
	}
	const zeroOID = "0000000000000000000000000000000000000000"
	lines := make([]string, 0, len(paths))
	for _, path := range paths {
		if _, present := byPath[path]; !present {
			lines = append(lines, fmt.Sprintf("0 %s\t%s", zeroOID, path))
		}
	}
	for _, path := range paths {
		if entry, present := byPath[path]; present {
			lines = append(lines, fmt.Sprintf("%s %s\t%s", entry.Mode, entry.OID, path))
		}
	}
	if err := git.UpdateIndexInfo(ctx, repoRoot, indexFile, lines); err != nil {
		return "", nil, fmt.Errorf("daemon: reconcile recovery chain: apply final index: %w", err)
	}
	treeOID, err := git.WriteTree(ctx, repoRoot, indexFile)
	if err != nil {
		return "", nil, fmt.Errorf("daemon: reconcile recovery chain: write recovery tree: %w", err)
	}
	chainPaths := make([]string, 0, len(chainTouched))
	for path := range chainTouched {
		chainPaths = append(chainPaths, path)
	}
	sort.Strings(chainPaths)
	finalState := make([]recoveryPathState, 0, len(chainPaths))
	for _, path := range chainPaths {
		entry, present := byPath[path]
		finalState = append(finalState, recoveryPathState{
			Path: path, Present: present, Mode: entry.Mode, OID: entry.OID,
		})
	}
	if matches, err := recoveryStatesMatchTree(ctx, repoRoot, treeOID, finalState); err != nil {
		return "", nil, fmt.Errorf("daemon: reconcile recovery chain: verify materialized tree: %w", err)
	} else if !matches {
		return "", nil, fmt.Errorf("daemon: reconcile recovery chain: materialized tree failed final-state verification")
	}
	return treeOID, finalState, nil
}

func applyRecoveryOpsInMemory(index map[string]git.IndexEntry, ops []state.CaptureOp) string {
	for _, op := range ops {
		beforeMatches := func(path string) bool {
			entry, ok := index[path]
			return ok && entry.Mode == op.BeforeMode.String && entry.OID == op.BeforeOID.String
		}
		after := git.IndexEntry{Mode: op.AfterMode.String, OID: op.AfterOID.String, Path: op.Path}
		switch op.Op {
		case "create":
			if existing, ok := index[op.Path]; ok && (existing.Mode != after.Mode || existing.OID != after.OID) {
				return fmt.Sprintf("create conflict for %s", op.Path)
			}
			index[op.Path] = after
		case "modify", "mode":
			if !beforeMatches(op.Path) {
				return fmt.Sprintf("%s before-state mismatch for %s", op.Op, op.Path)
			}
			index[op.Path] = after
		case "delete":
			if !beforeMatches(op.Path) {
				return fmt.Sprintf("delete before-state mismatch for %s", op.Path)
			}
			delete(index, op.Path)
		case "rename":
			oldPath := op.OldPath.String
			if !beforeMatches(oldPath) {
				return fmt.Sprintf("rename source mismatch for %s", oldPath)
			}
			if _, exists := index[op.Path]; exists {
				return fmt.Sprintf("rename target already exists for %s", op.Path)
			}
			delete(index, oldPath)
			index[op.Path] = after
		default:
			return fmt.Sprintf("unknown op %q", op.Op)
		}
	}
	return ""
}

func recoveryChainMatchesHEAD(
	ctx context.Context,
	repoRoot string,
	head string,
	chain []state.RecoveryChainEvent,
	finalState []recoveryPathState,
) (bool, error) {
	matched := true
	seen := make(map[string]struct{})
	for _, item := range chain {
		if err := ctx.Err(); err != nil {
			return false, err
		}
		base := item.Event.BaseHead
		if _, ok := seen[base]; ok {
			continue
		}
		ancestor, err := git.IsAncestor(ctx, repoRoot, base, head)
		if err != nil {
			return false, fmt.Errorf("daemon: reconcile recovery chain: prove ancestry %s..%s: %w", base, head, err)
		}
		if !ancestor {
			matched = false
		}
		seen[base] = struct{}{}
	}
	statesMatch, err := recoveryStatesMatchTree(ctx, repoRoot, head, finalState)
	if err != nil {
		return false, fmt.Errorf("daemon: reconcile recovery chain: prove HEAD paths: %w", err)
	}
	return matched && statesMatch, nil
}

func recoveryStatesMatchTree(
	ctx context.Context,
	repoRoot string,
	rev string,
	states []recoveryPathState,
) (bool, error) {
	if len(states) == 0 {
		return false, fmt.Errorf("empty recovery state proof")
	}
	paths := make([]string, 0, len(states))
	for _, state := range states {
		paths = append(paths, state.Path)
	}
	entries, err := git.LsTree(ctx, repoRoot, rev, false, git.LiteralPathspecs(paths)...)
	if err != nil {
		return false, err
	}
	byPath := make(map[string]git.TreeEntry, len(entries))
	for _, entry := range entries {
		byPath[entry.Path] = entry
	}
	return recoveryStatesMatchEntries(ctx, byPath, states)
}

func recoveryStatesMatchEntries(
	ctx context.Context,
	byPath map[string]git.TreeEntry,
	states []recoveryPathState,
) (bool, error) {
	if len(states) == 0 {
		return false, fmt.Errorf("empty recovery state proof")
	}
	matched := true
	for _, want := range states {
		if err := ctx.Err(); err != nil {
			return false, err
		}
		entry, present := byPath[want.Path]
		if !want.Present {
			if present {
				matched = false
			}
			continue
		}
		wantType := "blob"
		if want.Mode == "160000" {
			wantType = "commit"
		}
		if !present || entry.Mode != want.Mode || entry.OID != want.OID || entry.Type != wantType {
			matched = false
		}
	}
	return matched, nil
}

func ensureRecoveryCommit(
	ctx context.Context,
	repoRoot string,
	ref string,
	treeOID string,
	baseHead string,
	chain []state.RecoveryChainEvent,
) (string, error) {
	if existing, err := reusableRecoveryCommit(ctx, repoRoot, ref, treeOID, baseHead); err == nil && existing != "" {
		return existing, nil
	} else if err != nil && !errors.Is(err, git.ErrRefNotFound) {
		return "", err
	}

	first := chain[0].Event
	last := chain[len(chain)-1].Event
	message := fmt.Sprintf(
		"Preserve ACD recovery chain\n\nBranch: %s\nGeneration: %d\nEvents: %d-%d\n",
		first.BranchRef, first.BranchGeneration, first.Seq, last.Seq,
	)
	commitOID, err := git.CommitTreeWithIdentity(ctx, repoRoot, treeOID, message,
		recoveryCommitIdentityName, recoveryCommitIdentityEmail, baseHead)
	if err != nil {
		return "", fmt.Errorf("daemon: reconcile recovery chain: create recovery commit: %w", err)
	}
	if _, err := git.EnsureRecoveryRef(ctx, repoRoot, ref, commitOID); err == nil {
		return commitOID, nil
	} else if !errors.Is(err, git.ErrRecoveryRefCollision) {
		return "", fmt.Errorf("daemon: reconcile recovery chain: protect recovery commit: %w", err)
	}

	// commit-tree timestamps make equivalent retries produce different commit
	// OIDs. A racing winner is reusable only when both its tree and immutable
	// provenance parent match this chain.
	existing, reuseErr := reusableRecoveryCommit(ctx, repoRoot, ref, treeOID, baseHead)
	if reuseErr != nil {
		return "", reuseErr
	}
	return existing, nil
}

func ensurePublishedProofRef(
	ctx context.Context,
	repoRoot string,
	ref string,
	liveHead string,
	chain []state.RecoveryChainEvent,
	finalState []recoveryPathState,
) (string, error) {
	existing, err := git.RevParse(ctx, repoRoot, ref)
	if err == nil {
		matches, proofErr := recoveryChainMatchesHEAD(ctx, repoRoot, existing, chain, finalState)
		if proofErr != nil {
			return "", fmt.Errorf("daemon: reconcile recovery chain: verify existing published proof: %w", proofErr)
		}
		if !matches {
			return "", fmt.Errorf("%w: published proof ref %s points at non-matching commit %s",
				git.ErrRecoveryRefCollision, ref, existing)
		}
		return existing, nil
	}
	if !errors.Is(err, git.ErrRefNotFound) {
		return "", fmt.Errorf("daemon: reconcile recovery chain: resolve published proof ref: %w", err)
	}
	if _, err := git.EnsureRecoveryRef(ctx, repoRoot, ref, liveHead); err == nil {
		return liveHead, nil
	} else if !errors.Is(err, git.ErrRecoveryRefCollision) {
		return "", fmt.Errorf("daemon: reconcile recovery chain: protect published proof: %w", err)
	}
	// A racing writer may have protected an equivalent external commit.
	existing, err = git.RevParse(ctx, repoRoot, ref)
	if err != nil {
		return "", fmt.Errorf("daemon: reconcile recovery chain: re-read published proof: %w", err)
	}
	matches, err := recoveryChainMatchesHEAD(ctx, repoRoot, existing, chain, finalState)
	if err != nil {
		return "", fmt.Errorf("daemon: reconcile recovery chain: verify raced published proof: %w", err)
	}
	if !matches {
		return "", fmt.Errorf("%w: raced published proof ref %s points at non-matching commit %s",
			git.ErrRecoveryRefCollision, ref, existing)
	}
	return existing, nil
}

func reusableRecoveryCommit(ctx context.Context, repoRoot, ref, treeOID, baseHead string) (string, error) {
	commitOID, err := git.RevParse(ctx, repoRoot, ref)
	if err != nil {
		return "", err
	}
	existingTree, err := git.RevParse(ctx, repoRoot, ref+"^{tree}")
	if err != nil {
		return "", fmt.Errorf("%w: recovery ref %s has no commit tree", git.ErrRecoveryRefCollision, ref)
	}
	parent, err := git.RevParse(ctx, repoRoot, ref+"^")
	if err != nil {
		return "", fmt.Errorf("%w: recovery ref %s has no provenance parent", git.ErrRecoveryRefCollision, ref)
	}
	if existingTree != treeOID || parent != baseHead {
		return "", fmt.Errorf(
			"%w: recovery ref %s has tree %s parent %s, want tree %s parent %s",
			git.ErrRecoveryRefCollision, ref, existingTree, parent, treeOID, baseHead,
		)
	}
	return commitOID, nil
}

func recoveryRefName(branchRef string, generation, firstSeq, lastSeq int64, baseHead, treeOID string) string {
	token := sha256.Sum256([]byte(fmt.Sprintf("%s\x00%d", branchRef, generation)))
	target := sha256.Sum256([]byte(baseHead + "\x00" + treeOID))
	return fmt.Sprintf("%s%x/g%d/%d-%d-%x/archive",
		git.RecoveryRefPrefix, token[:6], generation, firstSeq, lastSeq, target[:12])
}

func recoveryProofRefName(branchRef string, generation, firstSeq, lastSeq int64, commitOID string) string {
	token := sha256.Sum256([]byte(fmt.Sprintf("%s\x00%d", branchRef, generation)))
	target := sha256.Sum256([]byte(commitOID))
	return fmt.Sprintf("%s%x/g%d/%d-%d-%x/published",
		git.RecoveryRefPrefix, token[:6], generation, firstSeq, lastSeq, target[:12])
}

func currentRecoveryLiveState(
	ctx context.Context,
	repoRoot string,
	afterInitialToken func(),
	beforeTokenRecheck func(),
) (recoveryLiveState, error) {
	var live recoveryLiveState
	tokenBefore, err := BranchGenerationToken(ctx, repoRoot)
	if err != nil {
		return live, fmt.Errorf("daemon: reconcile recovery chain: resolve branch token: %w", err)
	}
	if afterInitialToken != nil {
		afterInitialToken()
	}
	// The token already contains the HEAD observed by BranchGenerationToken.
	// Deriving the SHA from that same sample avoids an ABA race where a separate
	// RevParse reads transient B while the token reads A both before and after.
	head := tokenSHA(tokenBefore)
	hasHead := head != ""
	if beforeTokenRecheck != nil {
		beforeTokenRecheck()
	}
	tokenAfter, err := BranchGenerationToken(ctx, repoRoot)
	if err != nil {
		return live, fmt.Errorf("daemon: reconcile recovery chain: re-read branch token: %w", err)
	}
	if tokenAfter != tokenBefore {
		return live, fmt.Errorf("daemon: reconcile recovery chain: branch token moved from %q to %q", tokenBefore, tokenAfter)
	}
	return recoveryLiveState{
		token:   tokenBefore,
		head:    head,
		hasHead: hasHead,
	}, nil
}

func requireStableRecoveryLiveState(ctx context.Context, repoRoot string, expected recoveryLiveState) error {
	actual, err := BranchGenerationToken(ctx, repoRoot)
	if err != nil {
		return fmt.Errorf("daemon: reconcile recovery chain: re-read branch token: %w", err)
	}
	if actual != expected.token {
		return fmt.Errorf("daemon: reconcile recovery chain: branch token moved from %q to %q", expected.token, actual)
	}
	return nil
}

func requireRecoveryRefMissing(ctx context.Context, repoRoot, ref string) error {
	if ref == "" {
		return nil
	}
	exists, err := git.RefExists(ctx, repoRoot, ref)
	if err != nil {
		return fmt.Errorf("daemon: reconcile recovery chain: recheck missing ref %s: %w", ref, err)
	}
	if exists {
		return fmt.Errorf("daemon: reconcile recovery chain: expected ref %s to remain missing", ref)
	}
	return nil
}
