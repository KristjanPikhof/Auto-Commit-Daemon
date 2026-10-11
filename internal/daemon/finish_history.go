package daemon

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/ai"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/checkpoint"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/finish"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/verification"
)

type FinishHistoryOptions struct {
	Mode, Style, Purpose, VerificationConfigDigest string
	MessagesOnly, DiffEgress                       bool
	Deadline                                       time.Time
	BeforeCall                                     func(context.Context) error
}

func finishHistoryEvidence(ctx context.Context, repo string, chain []string) (finish.Evidence, []git.IntentHistoryUnit, string, error) {
	var evidence finish.Evidence
	units, err := git.ReadIntentHistoryUnits(ctx, repo, chain)
	if err != nil {
		return evidence, nil, "", err
	}
	base, err := git.IntentHistoryBaseTree(ctx, repo, chain[0])
	if err != nil {
		return evidence, nil, "", err
	}
	evidence.Base, err = readFinishTree(ctx, repo, base)
	if err != nil {
		return evidence, nil, "", err
	}
	evidence.Target, err = readFinishTree(ctx, repo, chain[len(chain)-1])
	if err != nil {
		return evidence, nil, "", err
	}
	authors, err := git.IntentHistoryAuthors(ctx, repo, units)
	if err != nil {
		return evidence, nil, "", err
	}
	ids := map[string]string{}
	for index, unit := range units {
		id := strconv.Itoa(index + 1)
		ids[unit.OldOID+"\x00"+unit.Path] = id
		evidence.Changes = append(evidence.Changes, finish.Change{ID: id, Path: unit.Path, Before: finish.Entry{Mode: unit.Before.Mode, OID: unit.Before.OID}, After: finish.Entry{Mode: unit.After.Mode, OID: unit.After.OID}, Author: strconv.Itoa(authors[unit.OldOID]), Provenance: unit.OldOID})
	}
	renames, err := git.ReadIntentHistoryRenamePairs(ctx, repo, chain)
	if err != nil {
		return evidence, nil, "", err
	}
	groups := map[string]string{}
	for index, pair := range renames {
		group := "rename-" + strconv.Itoa(index+1)
		groups[ids[pair.OldOID+"\x00"+pair.BeforePath]] = group
		groups[ids[pair.OldOID+"\x00"+pair.AfterPath]] = group
	}
	for index := range evidence.Changes {
		evidence.Changes[index].AtomicGroup = groups[evidence.Changes[index].ID]
	}
	return evidence, units, base, finish.ValidateEvidence(evidence)
}

func finishHistoryDiffs(ctx context.Context, repo string, evidence finish.Evidence) ([]ai.FinishDiff, error) {
	var result []ai.FinishDiff
	total := 0
	for _, change := range evidence.Changes {
		if change.Before.Mode == "160000" || change.After.Mode == "160000" {
			result = append(result, ai.FinishDiff{ChangeID: change.ID, Binary: true})
			continue
		}
		base, err := git.IntentHistoryBaseTree(ctx, repo, change.Provenance)
		if err != nil {
			return nil, err
		}
		diffs, err := finishProviderDiffs(ctx, repo, base, change.Provenance, []finish.Change{change}, true)
		if err != nil {
			return nil, err
		}
		total += len(diffs[0].Text)
		if total > ai.FinishHistoryDiffByteCap {
			return nil, &ai.FinishPlanningError{Code: "provider_input_too_large", Err: fmt.Errorf("exact history diffs total %d bytes; limit %d bytes; use local mode or a smaller explicit history range", total, ai.FinishHistoryDiffByteCap)}
		}
		result = append(result, diffs[0])
	}
	return result, nil
}

func PlanFinishHistory(ctx context.Context, repo, branch string, chain, suffix []string, transport ai.FinishTransport, options FinishHistoryOptions) (state.IntentHistoryPlan, error) {
	var plan state.IntentHistoryPlan
	if options.Mode != "local" && options.Mode != "intent" {
		return plan, errors.New("history rewrite: unsupported mode")
	}
	if err := finish.ValidatePurpose(options.Purpose); err != nil {
		return plan, err
	}
	if options.Style == "" {
		options.Style = finish.StyleImperative
	}
	if options.Mode == "intent" {
		if options.Deadline.IsZero() {
			return plan, errors.New("history rewrite: fixed planning deadline is required")
		}
		var cancel context.CancelFunc
		ctx, cancel = context.WithDeadline(ctx, options.Deadline)
		defer cancel()
	}
	evidence, units, base, err := finishHistoryEvidence(ctx, repo, chain)
	if err != nil {
		return plan, err
	}
	full := append(slices.Clone(chain), suffix...)
	if _, err := git.ReadIntentHistoryUnits(ctx, repo, full); err != nil {
		return plan, err
	}
	plan, err = state.PrepareIntentHistoryPlan(state.IntentHistoryPlan{SourceBranchRef: branch, TargetBranchRef: branch, ExpectedHead: full[len(full)-1], BaseTree: base, SourceChain: slices.Clone(chain), RecreateUnchanged: slices.Clone(suffix), CommitFormat: options.Style, Mode: options.Mode, MessagesOnly: options.MessagesOnly, VerificationConfigDigest: options.VerificationConfigDigest, PlanningDeadlineTS: finishTimestamp(options.Deadline)})
	if err != nil {
		return plan, err
	}
	for _, unit := range units {
		plan.Units = append(plan.Units, stateHistoryUnit(unit))
	}
	wt, err := git.ResolveWorktree(ctx, repo)
	if err != nil {
		return plan, err
	}
	plan.SourceRef = git.FinishRefPrefix + checkpoint.WorktreeID(wt.Root) + "/history/" + plan.ID + "/source"
	if _, err := git.EnsurePrivateRefDurable(ctx, repo, git.FinishRefPrefix, plan.SourceRef, plan.ExpectedHead); err != nil {
		return plan, err
	}
	var diffs []ai.FinishDiff
	if options.Mode == "intent" && options.DiffEgress {
		diffs, err = finishHistoryDiffs(ctx, repo, evidence)
		if err != nil {
			return plan, err
		}
	}
	if options.Mode == "local" || options.MessagesOnly {
		authorUnits := make([]git.IntentHistoryUnit, len(chain))
		for index, oid := range chain {
			authorUnits[index].OldOID = oid
		}
		commitAuthors, err := git.IntentHistoryAuthors(ctx, repo, authorUnits)
		if err != nil {
			return plan, err
		}
		var commits []ai.HistoryMessageCommit
		for _, oid := range chain {
			message, err := git.Run(ctx, git.RunOpts{Dir: repo}, "show", "-s", "--format=%B", oid)
			if err != nil {
				return plan, err
			}
			tree, err := git.RevParse(ctx, repo, oid+"^{tree}")
			if err != nil {
				return plan, err
			}
			goal := state.IntentHistoryGoal{ID: oid, Purpose: "Preserve original commit grouping", Reason: "Keep recorded order and membership", TreeOID: tree, AuthorOID: oid, OldOIDs: []string{oid}, Message: strings.TrimRight(string(message), "\n")}
			commit := ai.HistoryMessageCommit{ID: oid, TreeOID: tree, OriginalMessage: goal.Message, Author: strconv.Itoa(commitAuthors[oid])}
			for index, unit := range units {
				if unit.OldOID == oid {
					goal.Units = append(goal.Units, index)
					commit.ChangeIDs = append(commit.ChangeIDs, evidence.Changes[index].ID)
					commit.Author = evidence.Changes[index].Author
				}
			}
			if options.Mode == "local" && finish.ValidateMessage(options.Style, goal.Message) != nil {
				before, err := git.IntentHistoryBaseTree(ctx, repo, oid)
				if err != nil {
					return plan, err
				}
				baseTree, err := readFinishTree(ctx, repo, before)
				if err != nil {
					return plan, err
				}
				target, err := readFinishTree(ctx, repo, oid)
				if err != nil {
					return plan, err
				}
				local, err := finish.Local(finish.Evidence{Base: baseTree, Target: target, Changes: finish.NetChanges(baseTree, target, "recorded")}, finish.LocalOptions{Style: options.Style})
				if err != nil {
					return plan, err
				}
				goal.Message = "Preserve recorded empty commit"
				if options.Style == finish.StyleConventional {
					goal.Message = "chore: preserve recorded empty commit"
				}
				if len(local.Steps) > 0 {
					goal.Message = local.Steps[0].Message
				}
			}
			plan.Goals = append(plan.Goals, goal)
			commits = append(commits, commit)
		}
		if options.Mode == "intent" {
			goals, result, err := ai.PlanHistoryMessages(ctx, transport, evidence, commits, ai.FinishOptions{Deadline: options.Deadline, Style: options.Style, Purpose: options.Purpose, Diffs: diffs, DiffEgress: options.DiffEgress, BeforeCall: options.BeforeCall})
			if err != nil {
				return plan, err
			}
			plan.Disclosure, _ = json.Marshal(result.Disclosure)
			plan.PlanningCalls = result.Calls
			for index, goal := range goals {
				plan.Goals[index].Purpose, plan.Goals[index].Reason, plan.Goals[index].Message = goal.Purpose, goal.Boundary, goal.Message
			}
		}
	} else {
		result, err := ai.PlanFinish(ctx, transport, evidence, ai.FinishOptions{Deadline: options.Deadline, Style: options.Style, Purpose: options.Purpose, Diffs: diffs, DiffEgress: options.DiffEgress, BeforeCall: options.BeforeCall})
		if err != nil {
			return plan, err
		}
		plan.SharedPlan = &result.Plan
		plan.Disclosure, _ = json.Marshal(result.Disclosure)
		plan.PlanningCalls = result.Calls
		for index, step := range result.Plan.Steps {
			tree, err := finishHistoryTree(ctx, repo, step.Tree)
			if err != nil {
				return plan, err
			}
			if err := git.PinFinishHistoryTree(ctx, repo, strings.TrimSuffix(plan.SourceRef, "/source")+"/goal-"+strconv.Itoa(index+1), tree); err != nil {
				return plan, err
			}
			goal := state.IntentHistoryGoal{ID: "goal-" + strconv.Itoa(index+1), Purpose: step.Purpose, Reason: step.Boundary, Message: step.Message, TreeOID: tree}
			for _, id := range step.ChangeIDs {
				unitIndex, _ := strconv.Atoi(id)
				unitIndex--
				goal.Units = append(goal.Units, unitIndex)
				oid := units[unitIndex].OldOID
				if !slices.Contains(goal.OldOIDs, oid) {
					goal.OldOIDs = append(goal.OldOIDs, oid)
				}
				if goal.AuthorOID == "" {
					goal.AuthorOID = oid
				}
			}
			plan.Goals = append(plan.Goals, goal)
		}
	}
	err = ValidateIntentHistoryPlan(ctx, repo, plan)
	return plan, err
}

func finishHistoryTree(ctx context.Context, repo string, tree finish.Tree) (string, error) {
	dir, err := os.MkdirTemp("", "acd-history-index-")
	if err != nil {
		return "", err
	}
	defer os.RemoveAll(dir)
	var entries []git.IndexEntry
	for path, entry := range tree {
		entries = append(entries, git.IndexEntry{Path: path, Mode: entry.Mode, OID: entry.OID})
	}
	slices.SortFunc(entries, func(a, b git.IndexEntry) int { return strings.Compare(a.Path, b.Path) })
	return git.WriteTreeDurable(ctx, repo, filepath.Join(dir, "index"), entries)
}

// ValidateIntentHistoryPlan validates exact recorded membership or a shared
// purpose plan. Version-one plans remain inspectable, never silently upgraded.
func ValidateIntentHistoryPlan(ctx context.Context, repo string, plan state.IntentHistoryPlan) error {
	if plan.Version != state.IntentHistoryPlanVersion || len(plan.SourceChain) == 0 || (plan.Mode != "local" && plan.Mode != "intent") {
		return errors.New("history rewrite: legacy/invalid plan; create a new plan with the current CLI")
	}
	if len(plan.ID) > 128 || strings.ContainsAny(plan.ID, "/\\ \t\r\n\x00") || plan.ID == "" {
		return state.ErrFinishIdentity
	}
	wt, err := git.ResolveWorktree(ctx, repo)
	if err != nil {
		return err
	}
	expectedRef := git.FinishRefPrefix + checkpoint.WorktreeID(wt.Root) + "/history/" + plan.ID + "/source"
	if plan.SourceRef != expectedRef {
		return state.ErrFinishIdentity
	}
	pinned, err := git.RevParse(ctx, repo, plan.SourceRef)
	if err != nil || pinned != plan.ExpectedHead {
		return errors.Join(err, state.ErrFinishIdentity)
	}
	evidence, units, base, err := finishHistoryEvidence(ctx, repo, plan.SourceChain)
	if err != nil {
		return err
	}
	var recorded []state.IntentHistoryUnit
	for _, unit := range units {
		recorded = append(recorded, stateHistoryUnit(unit))
	}
	if !reflect.DeepEqual(recorded, plan.Units) || base != plan.BaseTree {
		return errors.New("history rewrite: recorded provenance differs from saved plan")
	}
	full := append(slices.Clone(plan.SourceChain), plan.RecreateUnchanged...)
	if _, err := git.ReadIntentHistoryUnits(ctx, repo, full); err != nil {
		return err
	}
	if full[len(full)-1] != plan.ExpectedHead {
		return errors.New("history rewrite: invalid frozen source head")
	}
	if plan.Mode == "local" || plan.MessagesOnly {
		if len(plan.Goals) != len(plan.SourceChain) || plan.SharedPlan != nil {
			return errors.New("history rewrite: preserved membership differs")
		}
		for index, goal := range plan.Goals {
			oid := plan.SourceChain[index]
			tree, err := git.RevParse(ctx, repo, oid+"^{tree}")
			if err != nil {
				return err
			}
			var indexes []int
			for unitIndex, unit := range units {
				if unit.OldOID == oid {
					indexes = append(indexes, unitIndex)
				}
			}
			if goal.TreeOID != tree || goal.AuthorOID != oid || !slices.Equal(goal.OldOIDs, []string{oid}) || !slices.Equal(goal.Units, indexes) {
				return errors.New("history rewrite: commit order, tree, author or membership changed")
			}
		}
	} else {
		if plan.Mode != "intent" || plan.SharedPlan == nil {
			return errors.New("history rewrite: saved shared plan is required")
		}
		if err := finish.Validate(evidence, *plan.SharedPlan); err != nil {
			return err
		}
		if len(plan.Goals) != len(plan.SharedPlan.Steps) {
			return errors.New("history rewrite: goal count differs from shared plan")
		}
		for index, step := range plan.SharedPlan.Steps {
			goal := plan.Goals[index]
			pinned, err := git.RevParse(ctx, repo, strings.TrimSuffix(plan.SourceRef, "/source")+"/goal-"+strconv.Itoa(index+1)+"^{tree}")
			if err != nil || pinned != goal.TreeOID {
				return errors.Join(err, state.ErrFinishIdentity)
			}
			tree, err := readFinishTree(ctx, repo, goal.TreeOID)
			if err != nil {
				return err
			}
			var indexes []int
			var oldOIDs []string
			for _, id := range step.ChangeIDs {
				n, _ := strconv.Atoi(id)
				n--
				if n < 0 || n >= len(units) {
					return errors.New("history rewrite: unknown unit")
				}
				indexes = append(indexes, n)
				if !slices.Contains(oldOIDs, units[n].OldOID) {
					oldOIDs = append(oldOIDs, units[n].OldOID)
				}
			}
			if !reflect.DeepEqual(tree, step.Tree) || goal.Message != step.Message || goal.Purpose != step.Purpose || goal.Reason != step.Boundary || !slices.Equal(indexes, goal.Units) || !slices.Equal(oldOIDs, goal.OldOIDs) || len(oldOIDs) == 0 || goal.AuthorOID != oldOIDs[0] {
				return errors.New("history rewrite: saved goal differs from shared proof")
			}
		}
	}
	for _, goal := range plan.Goals {
		if plan.Mode == "intent" {
			if len(plan.Goals) > 1 && strings.TrimSpace(goal.Reason) == "" {
				return errors.New("history rewrite: split goals require review or revert boundaries")
			}
			quality := finish.Goal{Purpose: goal.Purpose, Boundary: goal.Reason, Message: goal.Message}
			for _, index := range goal.Units {
				quality.ChangeIDs = append(quality.ChangeIDs, evidence.Changes[index].ID)
			}
			if err := ai.ValidateHistoryGoalQuality(quality, evidence); err != nil {
				return err
			}
		}
		if err := finish.ValidatePurpose(goal.Purpose); err != nil {
			return err
		}
		if err := finish.ValidateMessage(plan.CommitFormat, goal.Message); err != nil {
			return err
		}
	}
	return nil
}

// ExecuteFinishHistoryRequest has no planning loop. Saved plans contain all
// messages; only exact tree verification and publication run on the worker.
func (w *FinishWorker) ExecuteFinishHistoryRequest(ctx context.Context, plan state.IntentHistoryPlan, r state.FinishHistoryRequest) (state.FinishHistoryRequest, error) {
	if len(r.Proof) > 0 {
		var proof state.FinishHistoryProof
		if err := json.Unmarshal(r.Proof, &proof); err != nil {
			return r, err
		}
		if proof.PlanID != r.PlanID || proof.WorktreeID != r.WorktreeID || proof.IndexDigest != r.IndexDigest {
			return r, state.ErrFinishIdentity
		}
		return w.publishFinishHistory(ctx, r, proof, true)
	}
	fail := func(cause error) (state.FinishHistoryRequest, error) {
		recordCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		r.Status, r.Error = "failed", ai.SanitizePlannerError(cause.Error())
		return r, errors.Join(cause, state.UpdateFinishHistoryRequest(recordCtx, w.DB, r, "running"))
	}
	if err := ValidateIntentHistoryPlan(ctx, w.RepoPath, plan); err != nil {
		return fail(err)
	}
	wt, cfg, digest, err := w.resolved(ctx)
	if err != nil {
		return fail(err)
	}
	if r.ConfigDigest != cfg.ConfigDigest || plan.VerificationConfigDigest != cfg.ConfigDigest {
		return fail(state.ErrFinishIdentity)
	}
	index, err := git.IndexContentDigest(ctx, wt.Root)
	if err != nil {
		return fail(err)
	}
	if index != r.IndexDigest {
		return fail(state.ErrFinishIdentity)
	}
	evalCtx, cancel := w.evaluationContext(ctx, wt, cfg, digest, state.FinishRequest{ID: plan.ID, Attempt: r.Attempt, Target: state.FinishTarget{BranchRef: plan.SourceBranchRef, ExpectedHead: plan.ExpectedHead, IndexDigest: index}})
	defer cancel()
	verifyCtx, verifyCancel := context.WithDeadline(evalCtx, time.Unix(0, int64(r.DeadlineTS*1e9)))
	defer verifyCancel()
	var checks []FinishCheck
	var goals []git.FinishHistoryGoal
	for _, goal := range plan.Goals {
		goals = append(goals, git.FinishHistoryGoal{TreeOID: goal.TreeOID, Message: goal.Message, AuthorOID: goal.AuthorOID, OldOIDs: goal.OldOIDs})
	}
	proof, err := git.BuildFinishHistoryChain(verifyCtx, wt.Root, git.FinishHistoryBuildOptions{PlanID: plan.ID, WorktreeID: r.WorktreeID, SourceBranchRef: plan.SourceBranchRef, TargetBranchRef: plan.TargetBranchRef, ExpectedHead: plan.ExpectedHead, IndexDigest: index, SourceRef: plan.SourceRef, LockToken: plan.ID + "-" + strconv.Itoa(r.Attempt), SourceChain: plan.SourceChain, RecreateUnchanged: plan.RecreateUnchanged, Goals: goals, Verify: func(callCtx context.Context, oid, tree string, index int) error {
		result, err := evaluatePublication(callCtx, func(jobCtx context.Context) (verification.Result, error) {
			if cfg.VerificationMode == "fast" || cfg.VerificationMode == "full" {
				return w.Runner.Run(jobCtx, verification.Request{RepoPath: wt.Root, CandidateID: plan.ID + "-" + strconv.Itoa(index), CommitOID: oid, Command: cfg.Command})
			}
			return w.Runner.CheckStructural(jobCtx, verification.StructuralRequest{RepoPath: wt.Root, CandidateID: plan.ID + "-" + strconv.Itoa(index), CommitOID: oid})
		})
		checks = append(checks, FinishCheck{TreeOID: tree, Policy: cfg.VerificationMode, Executed: result.Executed, Result: result})
		r.Checks, _ = json.Marshal(checks)
		if err == nil && result.NeedsAttention {
			err = errors.New("history rewrite: exact candidate verification failed")
		}
		return err
	}})
	if err != nil {
		return fail(err)
	}
	proof.Checks, _ = json.Marshal(checks)
	r.Proof, _ = json.Marshal(proof)
	if r.DryRun {
		r.Status = "completed"
		return r, state.UpdateFinishHistoryRequest(ctx, w.DB, r, "running")
	}
	r.Status = "prepared"
	if err := state.UpdateFinishHistoryRequest(ctx, w.DB, r, "running"); err != nil {
		return r, err
	}
	return w.publishFinishHistory(ctx, r, proof, false)
}

func (w *FinishWorker) publishFinishHistory(ctx context.Context, r state.FinishHistoryRequest, proof state.FinishHistoryProof, resume bool) (state.FinishHistoryRequest, error) {
	if w.OperationGate != nil {
		w.OperationGate.RUnlock()
		w.OperationGate.Lock()
		defer func() { w.OperationGate.Unlock(); w.OperationGate.RLock() }()
	}
	from := r.Status
	err := git.ApplyFinishHistoryProof(ctx, w.RepoPath, proof, resume, func() error {
		r.Status, r.Error = "completed", ""
		r.Result, _ = json.Marshal(proof)
		return state.UpdateFinishHistoryRequest(ctx, w.DB, r, from)
	})
	if err != nil {
		recordCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		r.Status, r.Error = "needs_attention", ai.SanitizePlannerError(err.Error())
		err = errors.Join(err, state.UpdateFinishHistoryRequest(recordCtx, w.DB, r, from))
	}
	return r, err
}

func stateHistoryUnit(unit git.IntentHistoryUnit) state.IntentHistoryUnit {
	return state.IntentHistoryUnit{OldOID: unit.OldOID, Path: unit.Path, Before: state.IntentHistoryVersion{OID: unit.Before.OID, Mode: unit.Before.Mode}, After: state.IntentHistoryVersion{OID: unit.After.OID, Mode: unit.After.Mode}}
}
