package daemon

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"time"

	acdconfig "github.com/KristjanPikhof/Auto-Commit-Daemon/internal/config"
	gitpkg "github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/identity"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/verification"
)

const configValidationPollInterval = 500 * time.Millisecond

func (m *RuntimeBundleManager) StartValidationWorker(
	parent context.Context,
	wake chan<- struct{},
) {
	if m == nil || m.db == nil {
		return
	}
	m.validationStartOnce.Do(func() {
		ctx, cancel := context.WithCancel(parent)
		m.mu.Lock()
		m.validationCancel = cancel
		m.mu.Unlock()
		m.validationWG.Add(1)
		go func() {
			defer m.validationWG.Done()
			m.runValidationWorker(ctx, wake)
		}()
	})
}

func (m *RuntimeBundleManager) runValidationWorker(
	ctx context.Context,
	wake chan<- struct{},
) {
	m.recoverAbandonedValidations(ctx)
	if _, err := (verification.Runner{}).CleanupStale(ctx, m.builder.RepoRoot); err != nil {
		m.logger.Warn("clean stale setup verification",
			"err", err.Error())
	}
	timer := time.NewTimer(0)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		}
		processed, err := m.processNextValidation(ctx)
		if err != nil && !errors.Is(err, context.Canceled) {
			m.logger.Warn("process setup validation", "err", err.Error())
		}
		if processed {
			notifyValidationWake(wake)
			timer.Reset(0)
		} else {
			timer.Reset(configValidationPollInterval)
		}
	}
}

func (m *RuntimeBundleManager) recoverAbandonedValidations(ctx context.Context) {
	runs, err := state.RunningConfigValidations(ctx, m.db)
	if err != nil {
		m.logger.Warn("inspect running setup validations", "err", err.Error())
		return
	}
	for _, run := range runs {
		if !run.OwnerPID.Valid || run.OwnerPID.Int64 <= 0 ||
			identity.Alive(int(run.OwnerPID.Int64)) {
			continue
		}
		if ok, err := state.RequeueConfigValidation(
			ctx, m.db, run.ID, int(run.OwnerPID.Int64),
		); err != nil {
			m.logger.Warn("requeue abandoned setup validation",
				"validation_id", run.ID, "err", err.Error())
		} else if ok {
			m.logger.Info("requeued abandoned setup validation",
				"validation_id", run.ID, "attempt", run.Attempt)
		}
	}
}

func (m *RuntimeBundleManager) processNextValidation(
	ctx context.Context,
) (bool, error) {
	projection, err := state.RuntimeConfigActivationState(ctx, m.db)
	if err != nil {
		return false, err
	}
	if !projection.DesiredRequestID.Valid {
		return false, nil
	}
	requestID := projection.DesiredRequestID.Int64
	if _, err := state.CancelSupersededConfigValidations(
		ctx, m.db, requestID, "superseded by a newer configuration",
	); err != nil {
		return false, err
	}
	run, ok, err := state.ClaimNextConfigValidation(ctx, m.db, os.Getpid())
	if err != nil || !ok {
		return false, err
	}
	runCtx, cancelRun := context.WithCancel(ctx)
	superseded := make(chan struct{}, 1)
	monitorDone := make(chan struct{})
	go m.cancelSupersededValidation(
		runCtx, run.ActivationRequestID, cancelRun, superseded, monitorDone,
	)
	result, runErr := m.executeConfigValidation(runCtx, run)
	cancelRun()
	<-monitorDone
	if errors.Is(ctx.Err(), context.Canceled) {
		_, requeueErr := state.RequeueConfigValidation(
			context.Background(), m.db, run.ID, os.Getpid(),
		)
		return true, errors.Join(ctx.Err(), requeueErr)
	}
	select {
	case <-superseded:
		projection, projectionErr :=
			state.RuntimeConfigActivationState(ctx, m.db)
		if projectionErr != nil {
			return true, projectionErr
		}
		if projection.DesiredRequestID.Valid {
			_, err = state.CancelSupersededConfigValidations(
				ctx, m.db, projection.DesiredRequestID.Int64,
				"superseded by a newer configuration",
			)
			return true, err
		}
		_, err = state.CompleteConfigValidation(
			ctx, m.db, run.ID, os.Getpid(),
			state.ConfigValidationCancelled, sql.NullInt64{},
			"", "desired configuration was cleared",
		)
		return true, err
	default:
	}
	status := state.ConfigValidationFailed
	exitCode := sql.NullInt64{}
	output := result.Output
	boundedError := ""
	switch result.Status {
	case verification.StatusPassed:
		status = state.ConfigValidationPassed
		exitCode = sql.NullInt64{Int64: 0, Valid: true}
	case verification.StatusTimedOut:
		status = state.ConfigValidationTimedOut
	case verification.StatusFailed:
		status = state.ConfigValidationFailed
		exitCode = sql.NullInt64{
			Int64: int64(result.ExitCode), Valid: result.ExitCode >= 0,
		}
	}
	if runErr != nil {
		boundedError = runErr.Error()
	}
	completed, completeErr := state.CompleteConfigValidation(
		ctx, m.db, run.ID, os.Getpid(), status,
		exitCode, output, boundedError,
	)
	if completeErr != nil {
		return true, completeErr
	}
	if !completed {
		return true, errors.New("runtime config: setup validation ownership changed")
	}
	if status == state.ConfigValidationPassed {
		m.logger.Info("setup validation passed",
			"validation_id", run.ID, "attempt", run.Attempt)
	} else {
		m.logger.Warn("setup validation needs attention",
			"validation_id", run.ID, "attempt", run.Attempt,
			"status", status, "exit_code", result.ExitCode)
	}
	return true, nil
}

func (m *RuntimeBundleManager) executeConfigValidation(
	ctx context.Context,
	run state.ConfigValidationRun,
) (verification.Result, error) {
	result := verification.Result{
		Status: verification.StatusNeedsAttention, NeedsAttention: true,
		ExitCode: -1, CommitOID: run.ExpectedHead,
	}
	revision, err := state.ConfigRevisionByID(ctx, m.db, run.RevisionID)
	if err != nil {
		return result, err
	}
	values, _, _, err := decodeRuntimeSnapshot(revision.SnapshotJSON)
	if err != nil {
		return result, err
	}
	commandKey := acdconfig.FieldVerificationFastCommand
	timeoutKey := acdconfig.FieldVerificationFastTimeout
	mode := verification.ModeFast
	if run.Mode == "structural" {
		commandKey = ""
		timeoutKey = ""
	} else if run.Mode == "full" {
		commandKey = acdconfig.FieldVerificationFullCommand
		timeoutKey = acdconfig.FieldVerificationFullTimeout
		mode = verification.ModeFull
	}
	command := ""
	if commandKey != "" {
		command = values[commandKey]
	}
	sum := sha256.Sum256([]byte(command))
	if (run.Mode != "structural" && command == "") ||
		hex.EncodeToString(sum[:]) != run.CommandDigest {
		return result, errors.New(
			"runtime config: reviewed validation command no longer matches the immutable revision",
		)
	}
	branch, err := gitpkg.RunBranchRef(ctx, m.builder.RepoRoot)
	if err != nil || branch != run.BranchRef {
		return result, fmt.Errorf(
			"runtime config: validation branch changed; run `acd configure` to review the current branch",
		)
	}
	head, err := gitpkg.RevParse(ctx, m.builder.RepoRoot, run.BranchRef+"^{commit}")
	if err != nil || head != run.ExpectedHead {
		return result, fmt.Errorf(
			"runtime config: validation HEAD changed; run `acd configure` to review the current commit",
		)
	}
	resolved, err := gitpkg.RevParse(
		ctx, m.builder.RepoRoot, run.ExpectedHead+"^{commit}",
	)
	if err != nil || resolved != run.ExpectedHead {
		return result, fmt.Errorf(
			"runtime config: recorded validation commit is missing; restore it or run `acd configure`",
		)
	}
	generation, err := LoadBranchGeneration(ctx, m.db)
	if err != nil || generation != run.BranchGeneration {
		return result, fmt.Errorf(
			"runtime config: validation branch generation changed; run `acd configure`",
		)
	}

	m.mu.Lock()
	builder := m.builder
	current := m.active.bundle
	m.mu.Unlock()
	candidate, err := builder.BuildRevision(ctx, revision, current)
	if err != nil {
		return result, err
	}
	defer m.closeBundle(candidate)
	if candidate.ReplayBlockedReason != "" {
		return result, errors.New(candidate.ReplayBlockedReason)
	}
	if run.Mode == "structural" {
		return (verification.Runner{}).CheckStructural(
			ctx, verification.StructuralRequest{
				RepoPath: m.builder.RepoRoot,
				CandidateID: fmt.Sprintf(
					"config-validation-%d-attempt-%d",
					run.ID, run.Attempt,
				),
				CommitOID: run.ExpectedHead,
			},
		)
	}
	timeout := runtimeDuration(values, timeoutKey, 0)
	approved, err := verification.NewApprovedCommand(
		m.builder.RepoRoot, run.ApprovalID, mode, command, timeout,
	)
	if err != nil {
		return result, err
	}
	return (verification.Runner{}).Run(ctx, verification.Request{
		RepoPath: m.builder.RepoRoot,
		CandidateID: fmt.Sprintf(
			"config-validation-%d-attempt-%d", run.ID, run.Attempt,
		),
		CommitOID: run.ExpectedHead,
		Command:   approved,
	})
}

func (m *RuntimeBundleManager) cancelSupersededValidation(
	ctx context.Context,
	requestID int64,
	cancel context.CancelFunc,
	superseded chan<- struct{},
	done chan<- struct{},
) {
	defer close(done)
	ticker := time.NewTicker(configValidationPollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		projection, err := state.RuntimeConfigActivationState(ctx, m.db)
		if err != nil {
			m.logger.Warn(
				"inspect desired setup validation",
				"validation_request_id", requestID,
				"err", err.Error(),
			)
			continue
		}
		if projection.DesiredRequestID.Valid &&
			projection.DesiredRequestID.Int64 == requestID {
			continue
		}
		select {
		case superseded <- struct{}{}:
		default:
		}
		cancel()
		return
	}
}

func notifyValidationWake(wake chan<- struct{}) {
	if wake == nil {
		return
	}
	select {
	case wake <- struct{}{}:
	default:
	}
}
