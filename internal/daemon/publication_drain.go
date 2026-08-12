package daemon

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/ai"
	gitpkg "github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

const publicationDrainHeadChangedPrefix = "publication drain HEAD changed after checkpoint:"
const publicationDrainRecoveredTargetError = "frozen publication target contains a terminal event"

// ActivePublicationDrainForPair returns the one durable drain owned by the
// active branch generation. Multiple active drains for one pair fail closed.
func ActivePublicationDrainForPair(
	ctx context.Context,
	db *state.DB,
	branchRef string,
	generation int64,
) (*state.PublicationDrain, error) {
	drains, err := state.ActivePublicationDrains(ctx, db)
	if err != nil {
		return nil, err
	}
	var active *state.PublicationDrain
	for i := range drains {
		drain := &drains[i]
		if drain.BranchRef != branchRef || drain.BranchGeneration != generation {
			continue
		}
		if active != nil {
			return nil, errors.New(
				"daemon: multiple active publication drains for branch generation")
		}
		copy := *drain
		loaded, err := state.PublicationDrainByID(ctx, db, copy.ID)
		if err != nil {
			return nil, err
		}
		active = &loaded
	}
	return active, nil
}

// RestartablePublicationDrainForPair returns the latest HEAD-change block
// that a new consented commit-all invocation may re-prove. Ordinary daemon
// replay must not use this lookup because needs-action drains remain terminal
// until the user retries the operation.
func RestartablePublicationDrainForPair(
	ctx context.Context,
	db *state.DB,
	branchRef string,
	generation int64,
) (*state.PublicationDrain, error) {
	rows, err := db.ReadSQL().QueryContext(ctx, `
SELECT id FROM publication_drains
WHERE branch_ref=? AND branch_generation=? AND phase='needs_action'
  AND (last_error LIKE ? OR last_error=? COLLATE BINARY)
ORDER BY created_ts DESC,id DESC LIMIT 2`,
		branchRef, generation, publicationDrainHeadChangedPrefix+"%",
		publicationDrainRecoveredTargetError)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(ids) == 0 {
		return nil, nil
	}
	if len(ids) > 1 {
		return nil, errors.New(
			"daemon: multiple restartable publication drains for branch generation")
	}
	drain, err := state.PublicationDrainByID(ctx, db, ids[0])
	if err != nil {
		return nil, err
	}
	return &drain, nil
}

// ResumePublicationDrainCheckpointing completes the only mutable Git step
// authorized by commit-all --yes. It rechecks branch, HEAD, Git operations,
// and conflicts before resetting the index, so restart recovery remains
// automatic without guessing through external repository movement.
func ResumePublicationDrainCheckpointing(
	ctx context.Context,
	repoRoot string,
	db *state.DB,
	drain state.PublicationDrain,
	now time.Time,
) (state.PublicationDrain, error) {
	recheckingHeadAdvance := drain.Phase == state.PublicationDrainNeedsAction &&
		strings.HasPrefix(drain.LastError, publicationDrainHeadChangedPrefix)
	recheckingRecoveredTarget := drain.Phase == state.PublicationDrainNeedsAction &&
		drain.LastError == publicationDrainRecoveredTargetError
	if drain.Phase != state.PublicationDrainCheckpointing &&
		!recheckingHeadAdvance && !recheckingRecoveredTarget {
		return drain, nil
	}
	fail := func(reason error) (state.PublicationDrain, error) {
		if recheckingHeadAdvance || recheckingRecoveredTarget {
			return drain, nil
		}
		nowTS := float64(now.UnixNano()) / 1e9
		update := PublicationDrainUpdateFrom(drain, nowTS, drain.LastProgressTS)
		update.Phase = state.PublicationDrainNeedsAction
		update.LastError = reason.Error()
		blocked, err := state.AdvancePublicationDrain(ctx, db, drain.ID, update)
		if err != nil {
			return drain, errors.Join(reason, err)
		}
		return blocked, nil
	}
	branchRef, err := gitpkg.RunBranchRef(ctx, repoRoot)
	if err != nil || branchRef == "" || branchRef != drain.BranchRef {
		if err == nil {
			err = errors.New("publication drain branch is detached or changed")
		}
		return fail(err)
	}
	var observedHead string
	if err := db.ReadSQL().QueryRowContext(ctx,
		`SELECT observed_head FROM checkpoints WHERE id=?`, drain.CheckpointID).
		Scan(&observedHead); err != nil {
		return fail(err)
	}
	currentHead, err := gitpkg.Run(ctx, gitpkg.RunOpts{Dir: repoRoot},
		"rev-parse", "--verify", "HEAD")
	currentHeadText := strings.TrimSpace(string(currentHead))
	if err == nil && currentHeadText != observedHead {
		var safe bool
		safe, err = publicationDrainOwnsHeadAdvance(
			ctx, repoRoot, db, drain, observedHead, currentHeadText)
		if err == nil && !safe {
			err = fmt.Errorf(
				publicationDrainHeadChangedPrefix+" observed=%s current=%s",
				observedHead, currentHeadText)
		}
	}
	if err != nil {
		return fail(err)
	}
	if recheckingRecoveredTarget {
		counts, countErr := publicationDrainCountsForTarget(ctx, db, drain.EventSeqs)
		if countErr != nil {
			return drain, countErr
		}
		if counts.recovered == 0 || counts.terminal != 0 {
			return drain, nil
		}
	}
	worktree, err := gitpkg.ResolveWorktree(ctx, repoRoot)
	if err != nil {
		return fail(err)
	}
	for _, marker := range []string{
		"rebase-merge", "rebase-apply", "MERGE_HEAD", "CHERRY_PICK_HEAD", "BISECT_LOG",
	} {
		if _, statErr := os.Lstat(filepath.Join(worktree.GitDir, marker)); statErr == nil {
			return fail(errors.New("publication drain encountered an active Git operation"))
		} else if !errors.Is(statErr, os.ErrNotExist) {
			return fail(statErr)
		}
	}
	unmerged, err := gitpkg.Run(ctx, gitpkg.RunOpts{Dir: repoRoot},
		"diff", "--name-only", "--diff-filter=U")
	if err != nil || strings.TrimSpace(string(unmerged)) != "" {
		if err == nil {
			err = errors.New("publication drain encountered unresolved conflicts")
		}
		return fail(err)
	}
	if recheckingHeadAdvance || recheckingRecoveredTarget {
		nowTS := float64(now.UnixNano()) / 1e9
		drain, err = state.ReopenPublicationDrainCheckpointing(
			ctx, db, drain.ID, drain.LastError, nowTS)
		if err != nil {
			return drain, err
		}
		recheckingHeadAdvance = false
		recheckingRecoveredTarget = false
	}
	if drain.StagedConsent && !drain.StagedConsumed {
		if _, err := gitpkg.Run(ctx, gitpkg.RunOpts{Dir: repoRoot},
			"reset", "--mixed", "--quiet", "HEAD", "--"); err != nil {
			return fail(err)
		}
	}
	nowTS := float64(now.UnixNano()) / 1e9
	update := PublicationDrainUpdateFrom(drain, nowTS, nowTS)
	update.Phase = state.PublicationDrainSemantic
	update.StagedConsumed = drain.StagedConsent
	return state.AdvancePublicationDrain(ctx, db, drain.ID, update)
}

func publicationDrainOwnsHeadAdvance(
	ctx context.Context,
	repoRoot string,
	db *state.DB,
	drain state.PublicationDrain,
	observedHead string,
	currentHead string,
) (bool, error) {
	if observedHead == "" || currentHead == "" {
		return false, nil
	}
	if _, err := gitpkg.Run(ctx, gitpkg.RunOpts{Dir: repoRoot},
		"merge-base", "--is-ancestor", observedHead, currentHead); err != nil {
		return false, nil
	}
	output, err := gitpkg.Run(ctx, gitpkg.RunOpts{Dir: repoRoot},
		"rev-list", "--first-parent", "--reverse", "--max-count=4097",
		observedHead+".."+currentHead)
	if err != nil {
		return false, err
	}
	commits := strings.Fields(string(output))
	if len(commits) == 0 || len(commits) > 4096 {
		return false, nil
	}
	sources := make(map[string]string, len(commits))
	const batchSize = 400
	for start := 0; start < len(commits); start += batchSize {
		end := min(start+batchSize, len(commits))
		placeholders := make([]string, 0, end-start)
		args := make([]any, 0, end-start+2)
		args = append(args, drain.BranchRef, drain.BranchGeneration)
		for _, commit := range commits[start:end] {
			placeholders = append(placeholders, "?")
			args = append(args, commit)
		}
		rows, err := db.ReadSQL().QueryContext(ctx, `
SELECT target_commit_oid,source_head FROM self_publications
WHERE branch_ref=? AND branch_generation=? AND phase='completed'
  AND target_commit_oid IN (`+strings.Join(placeholders, ",")+`)`, args...)
		if err != nil {
			return false, err
		}
		for rows.Next() {
			var target, source string
			if err := rows.Scan(&target, &source); err != nil {
				rows.Close()
				return false, err
			}
			sources[target] = source
		}
		if err := rows.Close(); err != nil {
			return false, err
		}
	}
	expectedSource := observedHead
	for _, commit := range commits {
		if sources[commit] != expectedSource {
			return false, nil
		}
		expectedSource = commit
	}
	return true, nil
}

// ResumePublicationDrainNormalization records the bounded semantic rebuild as
// exhausted before replay enters the local atomic fallback. Keeping this as a
// separate durable transition makes a crash between the two phases resumable
// without calling the provider again.
func ResumePublicationDrainNormalization(
	ctx context.Context,
	db *state.DB,
	drain state.PublicationDrain,
	now time.Time,
) (state.PublicationDrain, error) {
	if drain.Phase != state.PublicationDrainNormalizing {
		return drain, nil
	}
	nowTS := float64(now.UnixNano()) / 1e9
	if nowTS < drain.UpdatedTS {
		nowTS = drain.UpdatedTS
	}
	update := PublicationDrainUpdateFrom(drain, nowTS, drain.LastProgressTS)
	update.Phase = state.PublicationDrainEventFallback
	update.EventFallbackCount++
	update.FallbackMode = "atomic_dependency_components"
	return state.AdvancePublicationDrain(ctx, db, drain.ID, update)
}

// publicationDrainAtomicFallbackPlanner keeps every hard dependency component
// in one commit. It is local and deterministic, so the final fallback neither
// calls a provider nor degrades Intent publication to per-event commits.
type publicationDrainAtomicFallbackPlanner struct {
	commitFormat ai.CommitFormat
}

// publicationDrainAtomicFallbackWindow returns the complete hard-dependency
// component containing the first pending event. A component that exceeds the
// candidate protocol cap fails closed instead of being split across commits.
func publicationDrainAtomicFallbackWindow(
	ctx context.Context,
	db *state.DB,
	cctx CaptureContext,
	pending []state.CaptureEvent,
) ([]state.CaptureEvent, error) {
	if len(pending) == 0 {
		return nil, nil
	}
	bySeq := make(map[int64]struct{}, len(pending))
	for _, event := range pending {
		bySeq[event.Seq] = struct{}{}
	}
	captures := make([]IntentCandidateCapture, 0, len(pending))
	for _, event := range pending {
		ops, err := state.LoadCaptureOps(ctx, db, event.Seq)
		if err != nil {
			return nil, fmt.Errorf(
				"daemon: load atomic fallback capture %d: %w", event.Seq, err)
		}
		captures = append(captures, IntentCandidateCapture{Event: event, Ops: ops})
	}
	derived, err := BuildIntentCandidateDependencies(
		cctx.BranchRef, cctx.BranchGeneration, captures, nil, time.Now().UTC())
	if err != nil {
		return nil, fmt.Errorf("daemon: build atomic fallback dependencies: %w", err)
	}
	persisted, err := state.IntentCaptureDependenciesForPair(
		ctx, db, cctx.BranchRef, cctx.BranchGeneration)
	if err != nil {
		return nil, fmt.Errorf("daemon: load atomic fallback dependencies: %w", err)
	}
	dependencies, err := mergeIntentDependencies(persisted, derived)
	if err != nil {
		return nil, fmt.Errorf("daemon: merge atomic fallback dependencies: %w", err)
	}
	adjacent := make(map[int64][]int64)
	for _, dependency := range dependencies {
		if dependency.Strength != string(ai.IntentDependencyHard) {
			continue
		}
		if _, ok := bySeq[dependency.PrerequisiteSeq]; !ok {
			continue
		}
		if _, ok := bySeq[dependency.DependentSeq]; !ok {
			continue
		}
		adjacent[dependency.PrerequisiteSeq] = append(
			adjacent[dependency.PrerequisiteSeq], dependency.DependentSeq)
		adjacent[dependency.DependentSeq] = append(
			adjacent[dependency.DependentSeq], dependency.PrerequisiteSeq)
	}
	component := map[int64]struct{}{pending[0].Seq: {}}
	queue := []int64{pending[0].Seq}
	for len(queue) > 0 {
		seq := queue[0]
		queue = queue[1:]
		for _, neighbor := range adjacent[seq] {
			if _, seen := component[neighbor]; seen {
				continue
			}
			component[neighbor] = struct{}{}
			if len(component) > ai.IntentCandidateCaptureCap {
				return nil, fmt.Errorf(
					"daemon: atomic fallback dependency component exceeds %d captures",
					ai.IntentCandidateCaptureCap)
			}
			queue = append(queue, neighbor)
		}
	}
	window := make([]state.CaptureEvent, 0, len(component))
	for _, event := range pending {
		if _, ok := component[event.Seq]; ok {
			window = append(window, event)
		}
	}
	return window, nil
}

func (publicationDrainAtomicFallbackPlanner) Name() string {
	return "publication-drain-atomic-fallback"
}

func (publicationDrainAtomicFallbackPlanner) PlanIntent(
	context.Context,
	ai.IntentPlanRequest,
) (ai.IntentPlan, error) {
	return ai.IntentPlan{}, errors.New("publication drain requires Intent v2")
}

func (p publicationDrainAtomicFallbackPlanner) PlanIntentV2(
	ctx context.Context,
	req ai.IntentPlanRequestV2,
) (ai.IntentPlanV2, error) {
	if err := ctx.Err(); err != nil {
		return ai.IntentPlanV2{}, err
	}
	plan := deterministicIntentCandidatePlan(req, false, true)
	provider := ai.DeterministicProvider{CommitFormat: p.commitFormat}
	for index := range plan.Candidates {
		if plan.Candidates[index].Readiness != ai.IntentCandidateReady {
			continue
		}
		plan.Candidates[index].Subject = provider.FormatSubjectForOps(
			plan.Candidates[index].Subject, nil)
	}
	return plan, nil
}

func publicationDrainPendingEvents(
	ctx context.Context,
	db *state.DB,
	cctx CaptureContext,
	drain state.PublicationDrain,
	pending []state.CaptureEvent,
) ([]state.CaptureEvent, error) {
	if drain.BranchRef != cctx.BranchRef ||
		drain.BranchGeneration != cctx.BranchGeneration {
		return nil, errors.New("daemon: publication drain branch identity changed")
	}
	target := make(map[int64]struct{}, len(drain.EventSeqs))
	for _, seq := range drain.EventSeqs {
		target[seq] = struct{}{}
	}
	filtered := make([]state.CaptureEvent, 0, len(drain.EventSeqs))
	for _, event := range pending {
		if _, ok := target[event.Seq]; !ok {
			continue
		}
		if event.BranchRef != drain.BranchRef ||
			event.BranchGeneration != drain.BranchGeneration {
			return nil, errors.New(
				"daemon: publication drain membership changed branch generation")
		}
		filtered = append(filtered, event)
	}
	if drain.Phase != state.PublicationDrainEventFallback || len(filtered) < 2 {
		return filtered, nil
	}
	dependencies, err := state.IntentCaptureDependenciesForPair(
		ctx, db, drain.BranchRef, drain.BranchGeneration)
	if err != nil {
		return nil, fmt.Errorf("daemon: load publication drain dependencies: %w", err)
	}
	return topologicalPublicationDrainEvents(filtered, dependencies)
}

func topologicalPublicationDrainEvents(
	events []state.CaptureEvent,
	dependencies []state.IntentCaptureDependency,
) ([]state.CaptureEvent, error) {
	indexBySeq := make(map[int64]int, len(events))
	for i := range events {
		indexBySeq[events[i].Seq] = i
	}
	indegree := make([]int, len(events))
	dependents := make([][]int, len(events))
	seen := make(map[[2]int64]struct{})
	for _, dependency := range dependencies {
		if dependency.Strength != string(ai.IntentDependencyHard) {
			continue
		}
		from, fromOK := indexBySeq[dependency.PrerequisiteSeq]
		to, toOK := indexBySeq[dependency.DependentSeq]
		if !fromOK || !toOK || from == to {
			continue
		}
		key := [2]int64{dependency.PrerequisiteSeq, dependency.DependentSeq}
		if _, duplicate := seen[key]; duplicate {
			continue
		}
		seen[key] = struct{}{}
		indegree[to]++
		dependents[from] = append(dependents[from], to)
	}
	ready := make([]int, 0, len(events))
	for i := range events {
		if indegree[i] == 0 {
			ready = append(ready, i)
		}
	}
	sort.Slice(ready, func(i, j int) bool {
		return events[ready[i]].Seq < events[ready[j]].Seq
	})
	ordered := make([]state.CaptureEvent, 0, len(events))
	for len(ready) > 0 {
		current := ready[0]
		ready = ready[1:]
		ordered = append(ordered, events[current])
		for _, dependent := range dependents[current] {
			indegree[dependent]--
			if indegree[dependent] == 0 {
				ready = append(ready, dependent)
			}
		}
		sort.Slice(ready, func(i, j int) bool {
			return events[ready[i]].Seq < events[ready[j]].Seq
		})
	}
	if len(ordered) != len(events) {
		return nil, errors.New(
			"daemon: publication drain hard dependency graph contains a cycle")
	}
	return ordered, nil
}

type publicationDrainCounts struct {
	published int64
	recovered int64
	terminal  int64
	commits   int64
}

// UpdatePublicationDrainAfterReplay persists pass progress and bounded
// semantic escalation. It never changes the configured commit strategy.
func UpdatePublicationDrainAfterReplay(
	ctx context.Context,
	db *state.DB,
	drain state.PublicationDrain,
	summary ReplaySummary,
	replayErr error,
	now time.Time,
) (state.PublicationDrain, error) {
	counts, err := publicationDrainCountsForTarget(ctx, db, drain.EventSeqs)
	if err != nil {
		return drain, err
	}
	resolved := counts.published + counts.recovered
	if counts.terminal == 0 && resolved < drain.TargetEventCount {
		blocked, blockErr := publicationDrainHasEarlierTerminal(ctx, db, drain)
		if blockErr != nil {
			return drain, blockErr
		}
		if blocked {
			counts.terminal = 1
		}
	}
	nowTS := float64(now.UnixNano()) / 1e9
	if nowTS < drain.UpdatedTS {
		nowTS = drain.UpdatedTS
	}
	progressTS := drain.LastProgressTS
	if resolved > drain.PublishedEventCount ||
		counts.commits > drain.CommitCount {
		progressTS = nowTS
	}
	update := PublicationDrainUpdateFrom(drain, nowTS, progressTS)
	update.PublishedEventCount = resolved
	update.CommitCount = counts.commits
	if summary.PlannerFailure != "" {
		update.LastError = summary.PlannerFailure
	}
	if counts.terminal > 0 {
		update.Phase = state.PublicationDrainNeedsAction
		update.LastError = publicationDrainRecoveredTargetError
		return state.AdvancePublicationDrain(ctx, db, drain.ID, update)
	}
	if resolved == drain.TargetEventCount {
		update.Phase = state.PublicationDrainCompleted
		update.CompletedTS = sql.NullFloat64{Float64: nowTS, Valid: true}
		return state.AdvancePublicationDrain(ctx, db, drain.ID, update)
	}
	if replayErr != nil {
		update.LastError = replayErr.Error()
		var exhausted *IntentSemanticFallbackRequiredError
		if errors.As(replayErr, &exhausted) {
			if drain.Phase == state.PublicationDrainSemantic {
				update.Phase = state.PublicationDrainNormalizing
				update.SemanticRebuildAttempts++
			} else {
				update.Phase = state.PublicationDrainEventFallback
				update.EventFallbackCount++
				update.FallbackMode = "atomic_dependency_components"
			}
			return state.AdvancePublicationDrain(ctx, db, drain.ID, update)
		}
		update.Phase = state.PublicationDrainNeedsAction
		return state.AdvancePublicationDrain(ctx, db, drain.ID, update)
	}
	if resolved == drain.PublishedEventCount && !summary.HasMore {
		switch drain.Phase {
		case state.PublicationDrainSemantic:
			update.Phase = state.PublicationDrainNormalizing
			update.SemanticRebuildAttempts++
			update.FallbackMode = "deterministic_semantic"
		case state.PublicationDrainNormalizing:
			update.Phase = state.PublicationDrainEventFallback
			update.EventFallbackCount++
			update.FallbackMode = "atomic_dependency_components"
		}
	}
	return state.AdvancePublicationDrain(ctx, db, drain.ID, update)
}

func publicationDrainHasEarlierTerminal(
	ctx context.Context,
	db *state.DB,
	drain state.PublicationDrain,
) (bool, error) {
	var maxSeq int64
	for _, seq := range drain.EventSeqs {
		maxSeq = max(maxSeq, seq)
	}
	if maxSeq == 0 {
		return false, nil
	}
	var count int64
	if err := db.ReadSQL().QueryRowContext(ctx, `
SELECT COUNT(*) FROM capture_events
WHERE branch_ref=? AND branch_generation=? AND seq<=?
  AND state IN ('failed','blocked_conflict')`,
		drain.BranchRef, drain.BranchGeneration, maxSeq).Scan(&count); err != nil {
		return false, err
	}
	return count > 0, nil
}

func PublicationDrainUpdateFrom(
	drain state.PublicationDrain,
	updatedTS float64,
	progressTS float64,
) state.PublicationDrainUpdate {
	return state.PublicationDrainUpdate{
		ExpectedPhase: drain.Phase, Phase: drain.Phase,
		PublishedEventCount:     drain.PublishedEventCount,
		SemanticRebuildAttempts: drain.SemanticRebuildAttempts,
		EventFallbackCount:      drain.EventFallbackCount,
		CommitCount:             drain.CommitCount, FallbackMode: drain.FallbackMode,
		LastError: drain.LastError, StagedConsent: drain.StagedConsent,
		StagedConsumed: drain.StagedConsumed,
		UpdatedTS:      updatedTS, LastProgressTS: progressTS,
		CompletedTS: drain.CompletedTS,
	}
}

func publicationDrainCountsForTarget(
	ctx context.Context,
	db *state.DB,
	eventSeqs []int64,
) (publicationDrainCounts, error) {
	var counts publicationDrainCounts
	commits := make(map[string]struct{})
	const batchSize = 500
	for start := 0; start < len(eventSeqs); start += batchSize {
		end := min(start+batchSize, len(eventSeqs))
		placeholders := make([]string, 0, end-start)
		args := make([]any, 0, end-start)
		for _, seq := range eventSeqs[start:end] {
			placeholders = append(placeholders, "?")
			args = append(args, seq)
		}
		rows, err := db.ReadSQL().QueryContext(ctx, `
SELECT state,COALESCE(commit_oid,'') FROM capture_events
WHERE seq IN (`+strings.Join(placeholders, ",")+")", args...)
		if err != nil {
			return counts, err
		}
		for rows.Next() {
			var eventState, commitOID string
			if err := rows.Scan(&eventState, &commitOID); err != nil {
				rows.Close()
				return counts, err
			}
			switch eventState {
			case state.EventStatePublished:
				counts.published++
				if commitOID != "" {
					commits[commitOID] = struct{}{}
				}
			case state.EventStateRecovered:
				counts.recovered++
			case state.EventStateFailed, state.EventStateBlockedConflict:
				counts.terminal++
			}
		}
		if err := rows.Close(); err != nil {
			return counts, err
		}
	}
	counts.commits = int64(len(commits))
	return counts, nil
}
