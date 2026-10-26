package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/ai"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/daemon"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/supervisor"
)

type finishStartParams struct {
	Purpose       string        `json:"purpose"`
	Message       string        `json:"message"`
	DryRun        bool          `json:"dry_run"`
	IncludeStaged bool          `json:"include_staged"`
	RetryID       string        `json:"retry_id"`
	Timeout       time.Duration `json:"timeout,omitempty"`
}

func decodeFinishParams(raw json.RawMessage, dst any) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(dst); err != nil {
		return err
	}
	if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
		return errors.New("finish: expected one parameters object")
	}
	return nil
}

func (h *repositoryWorkerHandler) handleFinish(ctx context.Context, runtime *workerRuntime, request supervisor.Request) (any, *supervisor.ProtocolError) {
	if request.RepositoryID != h.repositoryID || request.WorktreeID == "" || request.WorktreeID != runtime.record.WorktreeID {
		return nil, protocolFailure("worker_identity_mismatch", state.ErrFinishIdentity, false)
	}
	if request.Method == "finish_status" {
		var params struct {
			ID string `json:"request_id"`
		}
		if err := decodeFinishParams(request.Params, &params); err != nil {
			return nil, protocolFailure("invalid_finish", err, false)
		}
		r, err := state.LoadFinishRequest(ctx, runtime.db, params.ID)
		if err == nil && r.Target.WorktreeID != request.WorktreeID {
			err = state.ErrFinishIdentity
		}
		if err != nil {
			return nil, finishProtocolError(err)
		}
		return r, nil
	}
	var params finishStartParams
	if err := decodeFinishParams(request.Params, &params); err != nil {
		return nil, protocolFailure("invalid_finish", err, false)
	}
	if err := daemon.ValidateFinishTimeout(params.Timeout); err != nil {
		return nil, protocolFailure("invalid_finish", err, false)
	}
	runtime.finishMu.Lock()
	defer runtime.finishMu.Unlock()
	if runtime.workerCtx == nil || runtime.workerCtx.Err() != nil {
		return nil, protocolFailure("worker_unavailable", errors.New("finish: protection worker is stopping; run `acd on`"), true)
	}
	if workerPublicationHeld(runtime.roots.SetupPublicationHoldPath(), runtime.restoreHeld) {
		return nil, protocolFailure("finish_busy", errors.New("finish: setup or restore holds publication; complete that operation first"), false)
	}
	// Reconnection uses the frozen attempt. A removed credential or a saved
	// settings edit cannot prevent a client from observing its running job.
	var active state.FinishRequest
	var err error
	if params.RetryID != "" {
		if params.Purpose != "" || params.Message != "" || params.IncludeStaged {
			return nil, protocolFailure("invalid_finish", errors.New("finish: retry cannot change purpose, message or staged consent"), false)
		}
		active, err = state.LoadFinishRequest(ctx, runtime.db, params.RetryID)
		if err == nil && active.Target.WorktreeID != request.WorktreeID {
			err = state.ErrFinishIdentity
		}
	} else {
		active, err = state.ActiveFinishRequest(ctx, runtime.db, request.WorktreeID)
	}
	if err != nil && !errors.Is(err, state.ErrFinishNotFound) {
		return nil, finishProtocolError(err)
	}
	if params.RetryID != "" && errors.Is(err, state.ErrFinishNotFound) {
		return nil, finishProtocolError(err)
	}
	if err == nil && (active.State == state.FinishRunning || active.State == state.FinishPrepared || active.State == state.FinishNeedsAttention) {
		matches := active.DryRun == params.DryRun
		if params.RetryID == "" {
			matches = matches && active.Target.Purpose == params.Purpose && active.Target.Message == params.Message && active.Target.IncludeStaged == params.IncludeStaged
		}
		if !matches {
			return nil, finishProtocolError(state.ErrFinishBusy)
		}
		return active, nil
	}
	if _, exists, err := state.ActiveFinishHistoryRequest(ctx, runtime.db, request.WorktreeID); err != nil {
		return nil, finishProtocolError(err)
	} else if exists {
		return nil, finishProtocolError(state.ErrFinishBusy)
	}
	cfg, err := resolveFinishWorkerConfig(ctx, runtime.roots, runtime)
	if err != nil {
		return nil, finishProtocolError(err)
	}
	worker := &daemon.FinishWorker{RepoPath: runtime.worktree.Root, RepositoryID: h.repositoryID, DB: runtime.db, Config: cfg, OperationGate: runtime.gate, ProtectionGate: runtime.protectionGate}
	runtime.gate.RLock()
	r, created, err := worker.Start(ctx, daemon.FinishOptions{Purpose: params.Purpose, Message: params.Message, DryRun: params.DryRun, IncludeStaged: params.IncludeStaged, RetryID: params.RetryID, Timeout: params.Timeout})
	runtime.gate.RUnlock()
	if err != nil {
		return nil, finishProtocolError(err)
	}
	if created && r.State == state.FinishRunning {
		runtime.finishJobs.Add(1)
		runtime.activeFinishJobs.Add(1)
		go func() {
			defer runtime.finishJobs.Done()
			defer runtime.activeFinishJobs.Add(-1)
			runtime.gate.RLock()
			defer runtime.gate.RUnlock()
			_, _ = worker.Execute(runtime.workerCtx, r.ID)
		}()
	}
	return r, nil
}

func finishProtocolError(err error) *supervisor.ProtocolError {
	code := "finish_rejected"
	switch {
	case errors.Is(err, state.ErrFinishNotFound):
		code = "finish_not_found"
	case errors.Is(err, state.ErrFinishIdentity):
		code = "finish_identity_mismatch"
	case errors.Is(err, state.ErrFinishBusy):
		code = "finish_busy"
	case errors.Is(err, context.DeadlineExceeded):
		code = "finish_timeout"
	}
	return &supervisor.ProtocolError{Code: code, Message: ai.SanitizePlannerError(err.Error()), Retryable: errors.Is(err, context.DeadlineExceeded)}
}

func finishRequest(ctx context.Context, lookup controlRepoLookup, method string, params any) (state.FinishRequest, error) {
	raw, err := json.Marshal(params)
	if err != nil {
		return state.FinishRequest{}, err
	}
	timeout := 30 * time.Second
	if method == "finish_start" {
		timeout = supervisor.CheckpointBarrierTimeout
	}
	response, err := supervisor.DoWorker(ctx, supervisor.WorkerSocketPath(lookup.Roots, lookup.Record.RepositoryID), supervisor.Request{Version: supervisor.ProtocolVersion, ID: fmt.Sprintf("finish-%d", time.Now().UnixNano()), Method: method, RepositoryID: lookup.Record.RepositoryID, WorktreeID: lookup.Record.WorktreeID, Params: raw, DeadlineMS: time.Now().Add(timeout).UnixMilli()}, timeout)
	if err != nil {
		return state.FinishRequest{}, unavailableError("acd finish: protection worker unavailable; run `acd on` or `acd setup` to update the runtime")
	}
	if response.Error != nil {
		if response.Error.Code == "invalid_request" {
			return state.FinishRequest{}, actionRequiredError("runtime_upgrade_required", "acd finish: runtime does not support finish; run `acd setup`")
		}
		if response.Error.Code == "finish_not_found" || response.Error.Code == "finish_identity_mismatch" {
			return state.FinishRequest{}, &CommandError{Code: response.Error.Code, Message: response.Error.Message, Exit: ExitInvalid}
		}
		return state.FinishRequest{}, &CommandError{Code: response.Error.Code, Message: response.Error.Message, Exit: ExitActionRequired, Retryable: response.Error.Retryable}
	}
	return decodeProductData[state.FinishRequest](response.Data)
}
