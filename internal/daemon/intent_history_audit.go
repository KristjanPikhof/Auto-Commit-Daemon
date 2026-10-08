package daemon

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/ai"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

const metaIntentHistoryAudit = "intent.history.audit"

type intentHistoryAuditRecord struct {
	Fingerprint   string  `json:"fingerprint"`
	NextAttemptTS float64 `json:"next_attempt_ts,omitempty"`
	Attempts      int     `json:"attempts,omitempty"`
	Outcome       string  `json:"outcome"`
	Reason        string  `json:"reason,omitempty"`
}

// MaybeRepairIntentHistory audits a bounded private suffix while publication is
// idle. Only objective message defects trigger provider planning. The configured
// ownership, sharing, horizon, verification and staging fences still apply.
// Unchanged evidence is inspected once; provider waits remain retryable.
func MaybeRepairIntentHistory(ctx context.Context, repoRoot, gitDir string, db *state.DB, cctx CaptureContext, opts ReplayOpts) (IntentRepairResult, error) {
	skipped := func(reason string) IntentRepairResult {
		return IntentRepairResult{Status: state.IntentRepairSkipped, Reason: reason}
	}
	if !opts.IntentRepairEnabled {
		return skipped("quality_audit_disabled"), nil
	}
	if reason, err := intentRepairMutationBarrier(ctx, gitDir, db); err != nil {
		return IntentRepairResult{}, err
	} else if reason != "" {
		return skipped(reason), nil
	}
	var pending int
	if err := db.ReadSQL().QueryRowContext(ctx, `SELECT COUNT(*) FROM capture_events WHERE branch_ref=? AND branch_generation=? AND state='pending'`, cctx.BranchRef, cctx.BranchGeneration).Scan(&pending); err != nil {
		return IntentRepairResult{}, err
	}
	if pending != 0 {
		return skipped("quality_audit_waits_for_publication"), nil
	}
	head, err := git.RevParse(ctx, repoRoot, "HEAD")
	if err != nil {
		return IntentRepairResult{}, err
	}
	fingerprint := fmt.Sprintf("sha256:%x", sha256.Sum256([]byte(fmt.Sprintf("%s\x00%d\x00%s\x00%s\x00%s\x00%s\x00%t\x00%d\x00%s\x00%s", cctx.BranchRef, cctx.BranchGeneration, head, opts.IntentPlannerProvider, opts.IntentPlannerModel, opts.CommitFormat, opts.IntentIncludeDiffs, opts.IntentRepairMaxCommits, opts.IntentVerificationMode, opts.IntentHealth.Snapshot().ProviderFingerprint))))
	var record intentHistoryAuditRecord
	ok, err := state.MetaGetJSON(ctx, db, metaIntentHistoryAudit, &record)
	if err != nil {
		return IntentRepairResult{}, err
	}
	now := float64(time.Now().UnixNano()) / 1e9
	if ok && record.Fingerprint == fingerprint && (record.NextAttemptTS == 0 || now < record.NextAttemptTS) {
		return skipped("quality_audit_unchanged"), nil
	}
	if record.Fingerprint != fingerprint {
		record = intentHistoryAuditRecord{Fingerprint: fingerprint}
	}
	finish := func(reason string) (IntentRepairResult, error) {
		record.Outcome = "skipped"
		record.Reason = reason
		record.NextAttemptTS = 0
		if reason == git.IntentRepairReasonStagedOverlap || reason == git.IntentRepairReasonAlternateRef || reason == "repair_verification_unavailable" {
			// These fences may change without a new commit. Recheck slowly,
			// while unchanged successful plans still make no provider calls.
			record.NextAttemptTS = now + 300
		}
		return skipped(reason), state.MetaSetJSON(ctx, db, metaIntentHistoryAudit, record)
	}
	limit := opts.IntentRepairMaxCommits
	if limit <= 0 || limit > git.MaxIntentRepairCommits {
		limit = git.MaxIntentRepairCommits
	}
	out, err := git.Run(ctx, git.RunOpts{Dir: repoRoot, Timeout: git.DefaultReadTimeout}, "rev-list", "--first-parent", fmt.Sprintf("--max-count=%d", limit), head)
	if err != nil {
		return IntentRepairResult{}, err
	}
	var chain []string
	var owned []git.IntentRepairOwnedCommit
	for _, oid := range strings.Fields(string(out)) {
		var candidateID string
		// A published capture and its active candidate are the ownership proof.
		// Subject or author identity alone never authorizes automatic rewriting.
		err := db.ReadSQL().QueryRowContext(ctx, `
SELECT candidate.id FROM intent_candidates candidate
JOIN intent_candidate_events membership ON membership.candidate_id=candidate.id AND membership.membership_state='active'
JOIN capture_events event ON event.seq=membership.event_seq AND event.state='published' AND event.commit_oid=?
WHERE candidate.branch_ref=? AND candidate.branch_generation=?
  AND candidate.status='soft_published' AND candidate.soft_publication_deadline>?
ORDER BY candidate.id LIMIT 1`, oid, cctx.BranchRef, cctx.BranchGeneration, now).Scan(&candidateID)
		if err == sql.ErrNoRows {
			break
		}
		if err != nil {
			return IntentRepairResult{}, err
		}
		chain = append(chain, oid)
		owned = append(owned, git.IntentRepairOwnedCommit{OID: oid, CandidateID: candidateID})
	}
	if len(chain) == 0 {
		return finish("quality_audit_no_private_owned_suffix")
	}
	for left, right := 0, len(chain)-1; left < right; left, right = left+1, right-1 {
		chain[left], chain[right] = chain[right], chain[left]
		owned[left], owned[right] = owned[right], owned[left]
	}
	units, err := git.ReadIntentHistoryUnits(ctx, repoRoot, chain)
	if err != nil {
		return IntentRepairResult{}, err
	}
	if len(units) == 0 || len(units) > state.IntentCandidateMaxCaptures {
		return finish("quality_audit_unit_limit")
	}
	paths := map[string]struct{}{}
	for _, unit := range units {
		paths[unit.Path] = struct{}{}
	}
	pathList := make([]string, 0, len(paths))
	for path := range paths {
		pathList = append(pathList, path)
	}
	sort.Strings(pathList)
	eligible, err := git.CheckIntentRepairEligibility(ctx, repoRoot, git.IntentRepairEligibilityOptions{BranchRef: cctx.BranchRef, ExpectedHead: head, Commits: owned, Paths: pathList, MaxCommits: limit})
	if err != nil {
		return IntentRepairResult{}, err
	}
	if !eligible.Eligible {
		return finish(eligible.Reason)
	}
	debt := false
	for _, oid := range chain {
		message, err := git.RunWithLimit(ctx, git.RunOpts{Dir: repoRoot, Timeout: git.DefaultReadTimeout}, intentRepairMessageReadCap, "show", "-s", "--format=%B", oid)
		if err != nil {
			return IntentRepairResult{}, err
		}
		parts := strings.SplitN(strings.TrimSpace(string(message)), "\n\n", 2)
		body := ""
		if len(parts) == 2 {
			body = parts[1]
		}
		req := ai.IntentPlanRequest{CommitFormat: opts.CommitFormat}
		plan := ai.IntentPlan{Subject: parts[0], Body: body}
		for i, unit := range units {
			if unit.OldOID == oid {
				seq := int64(i + 1)
				req.OfferedCaptures = append(req.OfferedCaptures, ai.OfferedCapture{Seq: seq, Path: unit.Path, Op: "modify", Fidelity: "full"})
				plan.SelectedSeqs = append(plan.SelectedSeqs, seq)
			}
		}
		quality := ai.EvaluateIntentPlanMessageQuality(req, plan)
		debt = debt || quality.HasReason(ai.MessageQualityReasonGenericSubject) || quality.HasReason(ai.MessageQualityReasonFilenameOnly) || quality.HasReason(ai.MessageQualityReasonMalformedSubject)
	}
	if !debt {
		return finish("quality_audit_no_quality_debt")
	}
	if (opts.IntentVerificationMode == "fast" || opts.IntentVerificationMode == "full") && opts.IntentRepairCommitVerify == nil {
		return finish("repair_verification_unavailable")
	}
	cfg, closePlanner, err := resolveIntentReplayConfig(opts)
	if err != nil {
		return IntentRepairResult{}, err
	}
	if closePlanner != nil {
		defer closePlanner()
	}
	permit, err := cfg.health.Acquire(ctx)
	if err != nil {
		record.Outcome = "provider_wait"
		record.Reason = "quality_audit_provider_wait"
		record.NextAttemptTS = now + 300
		if wait, ok := err.(*IntentPlannerCircuitOpenError); ok && !wait.RetryAt.IsZero() {
			record.NextAttemptTS = float64(wait.RetryAt.UnixNano()) / 1e9
		}
		return skipped(record.Reason), state.MetaSetJSON(ctx, db, metaIntentHistoryAudit, record)
	}
	record.Attempts++
	record.NextAttemptTS = now + 300
	record.Outcome = "planning"
	record.Reason = ""
	if err := state.MetaSetJSON(ctx, db, metaIntentHistoryAudit, record); err != nil {
		return IntentRepairResult{}, err
	}
	history, planErr := evaluatePublication(ctx, func(jobCtx context.Context) (state.IntentHistoryPlan, error) {
		return PlanIntentHistory(jobCtx, repoRoot, cctx.BranchRef, chain, cfg.planner, opts.CommitFormat, cfg.includeDiffs)
	})
	if err := cfg.health.Complete(ctx, permit, classifyIntentHistoryAuditFailure(planErr)); err != nil {
		return IntentRepairResult{}, err
	}
	if ctx.Err() != nil {
		return IntentRepairResult{}, ctx.Err()
	}
	if planErr != nil {
		record.Outcome = "provider_wait"
		record.Reason = "quality_audit_replan_wait"
		if snapshot := cfg.health.Snapshot(); snapshot.NextProbeTS > 0 {
			record.NextAttemptTS = snapshot.NextProbeTS
		}
		return skipped(record.Reason), state.MetaSetJSON(ctx, db, metaIntentHistoryAudit, record)
	}
	if len(history.Goals) == 0 || len(history.Goals) > limit {
		return finish("quality_audit_goal_limit")
	}
	candidates, repairPlan, err := intentHistoryAuditRepairPlan(ctx, db, cctx, history, fingerprint, opts, pathList)
	if err != nil {
		return finish("quality_audit_capture_evidence_incomplete")
	}
	if err := state.SaveIntentRepairCandidates(ctx, db, candidates); err != nil {
		return IntentRepairResult{}, err
	}
	result, err := ApplyIntentRepairTransaction(ctx, repoRoot, gitDir, db, cctx, repairPlan)
	if err != nil {
		record.Outcome = "failed"
		record.Reason = "quality_audit_verification_or_state_failure"
	} else {
		record.Outcome = result.Status
		record.Reason = result.Reason
	}
	record.NextAttemptTS = 0
	if saveErr := state.MetaSetJSON(ctx, db, metaIntentHistoryAudit, record); saveErr != nil && err == nil {
		err = saveErr
	}
	return result, err
}

func classifyIntentHistoryAuditFailure(err error) error {
	if err == nil {
		return nil
	}
	return classifyIntentPlannerHealthFailure(err, true)
}

func intentHistoryAuditRepairPlan(ctx context.Context, db *state.DB, cctx CaptureContext, history state.IntentHistoryPlan, fingerprint string, opts ReplayOpts, paths []string) ([]state.IntentCandidate, IntentRepairPlan, error) {
	plan := IntentRepairPlan{ID: "audit-" + strings.TrimPrefix(fingerprint, "sha256:")[:24], BranchRef: cctx.BranchRef, BranchGeneration: cctx.BranchGeneration, ExpectedHead: history.ExpectedHead, OldChain: history.SourceChain, Paths: paths, AllowCaptureRepartition: true, ExpectedFinalTree: history.Goals[len(history.Goals)-1].TreeOID, MaxCommits: opts.IntentRepairMaxCommits, VerifyCommit: opts.IntentRepairCommitVerify}
	unitGoal := map[string]int{}
	for goalIndex, goal := range history.Goals {
		for _, i := range goal.Units {
			if i < 0 || i >= len(history.Units) {
				return nil, plan, fmt.Errorf("invalid history unit")
			}
			unit := history.Units[i]
			key := unit.OldOID + "\x00" + unit.Path
			if _, exists := unitGoal[key]; exists {
				return nil, plan, fmt.Errorf("ambiguous history unit")
			}
			unitGoal[key] = goalIndex
		}
	}
	goalEvents := make([][]state.IntentCandidateEvent, len(history.Goals))
	for _, oid := range history.SourceChain {
		rows, err := db.ReadSQL().QueryContext(ctx, `SELECT seq FROM capture_events WHERE branch_ref=? AND branch_generation=? AND state='published' AND commit_oid=? ORDER BY seq LIMIT ?`, cctx.BranchRef, cctx.BranchGeneration, oid, state.IntentRepairMaxMembers+1)
		if err != nil {
			return nil, plan, err
		}
		var seqs []int64
		for rows.Next() {
			var seq int64
			if err := rows.Scan(&seq); err != nil {
				rows.Close()
				return nil, plan, err
			}
			seqs = append(seqs, seq)
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return nil, plan, err
		}
		rows.Close()
		if len(seqs) == 0 || len(seqs) > state.IntentRepairMaxMembers {
			return nil, plan, fmt.Errorf("capture evidence missing")
		}
		for _, seq := range seqs {
			ops, err := state.LoadCaptureOpsBounded(ctx, db, seq, state.IntentCandidateMaxCaptures)
			if err != nil {
				return nil, plan, err
			}
			owner := -1
			for _, op := range ops {
				for _, path := range []string{op.Path, op.OldPath.String} {
					if path == "" {
						continue
					}
					index, ok := unitGoal[oid+"\x00"+path]
					if !ok || owner >= 0 && owner != index {
						return nil, plan, fmt.Errorf("capture cannot be safely partitioned")
					}
					owner = index
				}
			}
			if owner < 0 {
				return nil, plan, fmt.Errorf("capture has no recorded path transition")
			}
			goalEvents[owner] = append(goalEvents[owner], state.IntentCandidateEvent{EventSeq: seq, EventRole: "history_reconstruction"})
		}
	}
	var candidates []state.IntentCandidate
	for index, goal := range history.Goals {
		if len(goalEvents[index]) == 0 || len(goalEvents[index]) > state.IntentCandidateMaxCaptures {
			return nil, plan, fmt.Errorf("invalid goal membership")
		}
		id := plan.ID + fmt.Sprintf("-%d", index+1)
		candidates = append(candidates, state.IntentCandidate{ID: id, BranchRef: cctx.BranchRef, BranchGeneration: cctx.BranchGeneration, Status: state.IntentCandidateReady, Purpose: goal.Purpose, Readiness: state.IntentReadinessReady, Events: goalEvents[index], PlannerProtocol: sql.NullString{String: ai.IntentPlannerProtocolV2, Valid: true}})
		replacement := IntentRepairCandidatePlan{CandidateID: id, TreeOID: goal.TreeOID, Message: goal.Message}
		old := map[string]struct{}{}
		for _, i := range goal.Units {
			old[history.Units[i].OldOID] = struct{}{}
		}
		for _, oid := range history.SourceChain {
			if _, ok := old[oid]; ok {
				replacement.Replaces = append(replacement.Replaces, oid)
			}
		}
		for _, event := range goalEvents[index] {
			replacement.EventSeqs = append(replacement.EventSeqs, event.EventSeq)
		}
		plan.Candidates = append(plan.Candidates, replacement)
	}
	return candidates, plan, nil
}
