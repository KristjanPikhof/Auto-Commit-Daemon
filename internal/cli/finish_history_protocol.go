package cli

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/daemon"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/supervisor"
)

type finishHistoryParams struct {
	Plan   state.IntentHistoryPlan `json:"plan"`
	PlanID string                  `json:"plan_id"`
	DryRun bool                    `json:"dry_run"`
}

func (h *repositoryWorkerHandler) handleFinishHistory(ctx context.Context, runtime *workerRuntime, request supervisor.Request) (any, *supervisor.ProtocolError) {
	if request.RepositoryID != h.repositoryID || request.WorktreeID != runtime.record.WorktreeID {
		return nil, finishProtocolError(state.ErrFinishIdentity)
	}
	var params finishHistoryParams
	if err := decodeFinishParams(request.Params, &params); err != nil {
		return nil, protocolFailure("invalid_history", err, false)
	}
	if request.Method == "history_status" {
		r, ok, err := state.LoadFinishHistoryRequest(ctx, runtime.db, params.PlanID)
		if err == nil && (!ok || r.WorktreeID != request.WorktreeID) {
			err = state.ErrFinishNotFound
		}
		if err != nil {
			return nil, finishProtocolError(err)
		}
		return r, nil
	}
	runtime.finishMu.Lock()
	defer runtime.finishMu.Unlock()
	if runtime.workerCtx == nil || runtime.workerCtx.Err() != nil {
		return nil, protocolFailure("worker_unavailable", errors.New("history rewrite: protection worker is stopping"), true)
	}
	if workerPublicationHeld(runtime.roots.SetupPublicationHoldPath(), runtime.restoreHeld) {
		return nil, finishProtocolError(state.ErrFinishBusy)
	}
	if _, err := state.ActiveFinishRequest(ctx, runtime.db, request.WorktreeID); err == nil {
		return nil, finishProtocolError(state.ErrFinishBusy)
	} else if !errors.Is(err, state.ErrFinishNotFound) {
		return nil, finishProtocolError(err)
	}
	active, exists, err := state.ActiveFinishHistoryRequest(ctx, runtime.db, request.WorktreeID)
	if err != nil {
		return nil, finishProtocolError(err)
	}
	plan := params.Plan
	if saved, ok, err := state.LoadIntentHistoryPlan(ctx, runtime.db, plan.ID); err != nil {
		return nil, finishProtocolError(err)
	} else if ok && !reflect.DeepEqual(saved, plan) {
		return nil, finishProtocolError(state.ErrFinishIdentity)
	}
	if exists {
		if active.PlanID != plan.ID || active.DryRun != params.DryRun {
			return nil, finishProtocolError(state.ErrFinishBusy)
		}
		if active.Status == "running" {
			return active, nil
		}
		// Prepared evidence is self-contained; reconnect does not load credentials or current settings.
		worker := &daemon.FinishWorker{RepoPath: runtime.worktree.Root, DB: runtime.db, OperationGate: runtime.gate}
		h.launchFinishHistory(runtime, worker, plan, active)
		return active, nil
	}
	old, ok, err := state.LoadFinishHistoryRequest(ctx, runtime.db, plan.ID)
	if err != nil {
		return nil, finishProtocolError(err)
	}
	if ok && old.WorktreeID != request.WorktreeID {
		return nil, finishProtocolError(state.ErrFinishIdentity)
	}
	if ok && old.Status == "completed" && !old.DryRun {
		var proof state.FinishHistoryProof
		if err := json.Unmarshal(old.Proof, &proof); err != nil {
			return nil, finishProtocolError(err)
		}
		if err := git.VerifyPublishedFinishHistoryProof(ctx, runtime.worktree.Root, proof); err != nil {
			return nil, finishProtocolError(err)
		}
		return old, nil
	}
	cfg, err := resolveFinishWorkerConfigInputs(ctx, runtime.roots, runtime, false)
	if err != nil {
		return nil, finishProtocolError(err)
	}
	if plan.VerificationConfigDigest != cfg.ConfigDigest || plan.Mode != cfg.Mode || plan.CommitFormat != cfg.Style {
		return nil, finishProtocolError(state.ErrFinishIdentity)
	}
	runtime.gate.RLock()
	defer runtime.gate.RUnlock()
	if err := daemon.ValidateIntentHistoryPlan(ctx, runtime.worktree.Root, plan); err != nil {
		return nil, finishProtocolError(err)
	}
	if request.Method == "history_save" {
		plan, err = state.SaveIntentHistoryPlan(ctx, runtime.db, plan)
		if err != nil {
			return nil, finishProtocolError(err)
		}
		return state.FinishHistoryRequest{PlanID: plan.ID, WorktreeID: request.WorktreeID, Status: "saved"}, nil
	}
	index, err := git.IndexContentDigest(ctx, runtime.worktree.Root)
	if err != nil {
		return nil, finishProtocolError(err)
	}
	if err := git.CheckFinishHistoryStart(ctx, runtime.worktree.Root, plan.SourceBranchRef, plan.ExpectedHead, index); err != nil {
		return nil, finishProtocolError(err)
	}
	plan, err = state.SaveIntentHistoryPlan(ctx, runtime.db, plan)
	if err != nil {
		return nil, finishProtocolError(err)
	}
	timeout := cfg.VerificationTimeout
	if timeout <= 0 {
		timeout = 5 * time.Minute
	}
	r, created, err := state.StartFinishHistoryRequest(ctx, runtime.db, state.FinishHistoryRequest{PlanID: plan.ID, WorktreeID: request.WorktreeID, ConfigDigest: cfg.ConfigDigest, IndexDigest: index, DryRun: params.DryRun, DeadlineTS: float64(time.Now().Add(timeout).UnixNano()) / 1e9})
	if err != nil {
		return nil, finishProtocolError(err)
	}
	if created {
		worker := &daemon.FinishWorker{RepoPath: runtime.worktree.Root, RepositoryID: h.repositoryID, DB: runtime.db, Config: cfg, OperationGate: runtime.gate, ProtectionGate: runtime.protectionGate}
		h.launchFinishHistory(runtime, worker, plan, r)
	}
	return r, nil
}

func (h *repositoryWorkerHandler) launchFinishHistory(runtime *workerRuntime, worker *daemon.FinishWorker, plan state.IntentHistoryPlan, r state.FinishHistoryRequest) {
	if runtime.historyJobs == nil {
		runtime.historyJobs = map[string]bool{}
	}
	if runtime.historyJobs[r.PlanID] {
		return
	}
	runtime.historyJobs[r.PlanID] = true
	runtime.finishJobs.Add(1)
	runtime.activeFinishJobs.Add(1)
	go func() {
		defer runtime.finishJobs.Done()
		defer runtime.activeFinishJobs.Add(-1)
		defer func() { runtime.finishMu.Lock(); delete(runtime.historyJobs, r.PlanID); runtime.finishMu.Unlock() }()
		runtime.gate.RLock()
		defer runtime.gate.RUnlock()
		_, _ = worker.ExecuteFinishHistoryRequest(runtime.workerCtx, plan, r)
	}()
}

func historyRequest(ctx context.Context, lookup controlRepoLookup, method string, params finishHistoryParams) (state.FinishHistoryRequest, error) {
	raw, err := json.Marshal(params)
	if err != nil {
		return state.FinishHistoryRequest{}, err
	}
	timeout := 30 * time.Second
	response, err := supervisor.DoWorker(ctx, supervisor.WorkerSocketPath(lookup.Roots, lookup.Record.RepositoryID), supervisor.Request{Version: supervisor.ProtocolVersion, ID: "history-" + params.Plan.ID + params.PlanID, Method: method, RepositoryID: lookup.Record.RepositoryID, WorktreeID: lookup.Record.WorktreeID, Params: raw, DeadlineMS: time.Now().Add(timeout).UnixMilli()}, timeout)
	if err != nil {
		return state.FinishHistoryRequest{}, unavailableError("history rewrite: protection worker unavailable; run `acd on` or update with `acd setup`")
	}
	if response.Error != nil {
		return state.FinishHistoryRequest{}, &CommandError{Code: response.Error.Code, Message: response.Error.Message, Exit: ExitActionRequired, Retryable: response.Error.Retryable}
	}
	return decodeProductData[state.FinishHistoryRequest](response.Data)
}
