package daemon

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/ai"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

// publicationEvaluation keeps the canonical worker as the sole state writer.
// Only an immutable provider/scratch/verification call runs in the background;
// the worker protects new bytes while that call is outstanding. The enclosing
// replay retains its runtime lease and frozen inputs until this returns.
type publicationEvaluation struct {
	gate                           *sync.RWMutex
	db                             *state.DB
	cancel                         context.CancelFunc
	identity                       func(context.Context) (string, error)
	protect                        func(context.Context) error
	wake, files, changes, shutdown <-chan struct{}
	onShutdown                     func()
	interval                       time.Duration
}

type publicationEvaluationKey struct{}

var errPublicationEvaluationStale = errors.New("publication evaluation inputs changed")

func evaluatePublication[T any](ctx context.Context, run func(context.Context) (T, error)) (T, error) {
	evaluation, _ := ctx.Value(publicationEvaluationKey{}).(*publicationEvaluation)
	if evaluation == nil {
		return run(ctx)
	}
	var zero T
	identity, err := evaluation.identity(ctx)
	if err != nil {
		evaluation.cancel()
		return zero, err
	}
	type result struct {
		value T
		err   error
	}
	done := make(chan result, 1)
	jobCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	// Inputs belong to the suspended worker stack, and are never accessed by
	// protection. A buffered result lets cancellation always join the job.
	go func() { value, err := run(jobCtx); done <- result{value, err} }()
	if evaluation.gate != nil {
		evaluation.gate.RUnlock()
	}
	locked := false
	lock := func() {
		if !locked && evaluation.gate != nil {
			evaluation.gate.RLock()
		}
		locked = true
	}
	unlock := func() {
		if locked && evaluation.gate != nil {
			evaluation.gate.RUnlock()
		}
		locked = false
	}
	defer lock() // restore the caller's operation-gate ownership on every exit
	interval := evaluation.interval
	if interval <= 0 {
		interval = 750 * time.Millisecond
	}
	delay := interval
	timer := time.NewTimer(delay)
	defer timer.Stop()
	abort := func(cause error) (T, error) {
		evaluation.cancel()
		cancel()
		<-done // resources cannot close while a provider or verifier still uses them
		return zero, cause
	}
	validate := func() error {
		current, err := evaluation.identity(ctx)
		if err != nil {
			return err
		}
		if current != identity {
			return errPublicationEvaluationStale
		}
		return nil
	}
	for {
		activity := false
		select {
		case completed := <-done:
			lock()
			if err := validate(); err != nil {
				evaluation.cancel()
				return zero, err
			}
			return completed.value, completed.err
		case <-ctx.Done():
			return abort(ctx.Err())
		case <-evaluation.shutdown:
			if evaluation.onShutdown != nil {
				evaluation.onShutdown()
			}
			return abort(context.Canceled)
		case <-evaluation.wake:
			activity = true
		case <-evaluation.files:
			activity = true
		case <-evaluation.changes:
			activity = true
		case <-timer.C:
		}
		lock()
		if err := validate(); err != nil {
			unlock()
			return abort(err)
		}
		if err := evaluation.protect(ctx); err != nil {
			unlock()
			return abort(err)
		}
		unlock()
		if activity {
			delay = interval
		} else {
			delay = min(delay*2, 2*time.Minute)
		}
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		timer.Reset(delay)
	}
}

// Candidate membership, desired configuration and restore journals can change
// without moving HEAD. Include them in the apply proof; protection-only
// checkpoints intentionally do not affect it.
func publicationEvaluationIdentity(ctx context.Context, repoRoot, gitDir string, db *state.DB, cctx CaptureContext) (string, error) {
	token, err := BranchGenerationToken(ctx, repoRoot)
	if err != nil {
		return "", err
	}
	if _, active := gitOperationInProgress(gitDir); active {
		return "", errPublicationEvaluationStale
	}
	pause, err := daemonPauseState(ctx, gitDir, db)
	if err != nil {
		return "", err
	}
	if pause.Active {
		return "", errPublicationEvaluationStale
	}
	runtime, err := state.RuntimeConfigActivationState(ctx, db)
	if err != nil {
		return "", err
	}
	candidates, err := state.IntentCandidatesForPair(ctx, db, cctx.BranchRef, cctx.BranchGeneration, state.IntentCandidateMaxOpenPerPair)
	if err != nil {
		return "", err
	}
	var restores int64
	var restoreTS float64
	err = db.ReadSQL().QueryRowContext(ctx, "SELECT COUNT(*), COALESCE(MAX(updated_ts),0) FROM restore_operations").Scan(&restores, &restoreTS)
	if err != nil {
		return "", err
	}
	data, err := json.Marshal([]any{token, runtime.DesiredRevisionID, runtime.AppliedRevisionID, candidates, restores, restoreTS})
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%x", sha256.Sum256(data)), nil
}

// Health bookkeeping runs on the worker, around the immutable message call.
func generatePublicationMessage(ctx context.Context, fn MessageFn, event EventContext, health *IntentPlannerHealth) (string, error) {
	var permit IntentPlannerHealthPermit
	if health != nil {
		var err error
		permit, err = health.Acquire(ctx)
		if err != nil {
			return "", err
		}
	}
	message, err := evaluatePublication(ctx, func(jobCtx context.Context) (string, error) { return fn(jobCtx, event) })
	if err == nil && message == "" {
		err = errors.New("selected provider returned an empty message")
	}
	if ai.ProviderNeedsConfiguration(err) {
		if health != nil {
			_ = health.Complete(ctx, permit, nil)
		}
		return "", err
	}
	if health != nil {
		var failure error
		if err != nil {
			failure = &IntentPlannerTransportFailure{Err: err}
		}
		if healthErr := health.Complete(ctx, permit, failure); healthErr != nil {
			return "", healthErr
		}
	}
	if err != nil {
		if ctx.Err() != nil {
			return "", ctx.Err()
		}
		wait := &IntentPlannerCircuitOpenError{}
		if health != nil {
			wait.RetryAt = time.Unix(0, int64(health.Snapshot().NextProbeTS*1e9))
		}
		return "", wait
	}
	return message, nil
}
