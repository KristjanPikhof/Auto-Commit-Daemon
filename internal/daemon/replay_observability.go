package daemon

import (
	"context"
	"strconv"
	"strings"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/ai"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

const (
	metaLastReplayError       = "last_replay_error"
	metaReplayErrorRepeat     = "replay.error_repeat_count"
	metaReplayErrorLastSeenTS = "replay.error_last_seen_ts"

	replayErrorLogInterval = 30 * time.Second
	replayErrorRepeatCap   = 1_000_000
)

// replayErrorLogLimiter emits the first occurrence of an error, then at most
// one identical line per interval. A changed error always emits immediately.
type replayErrorLogLimiter struct {
	value      string
	lastLogged time.Time
	suppressed int
}

func (l *replayErrorLogLimiter) observe(
	value string,
	now time.Time,
) (emit bool, suppressed int) {
	if value == "" {
		return false, 0
	}
	if value != l.value {
		l.value = value
		l.lastLogged = now
		l.suppressed = 0
		return true, 0
	}
	if l.lastLogged.IsZero() || now.Sub(l.lastLogged) >= replayErrorLogInterval {
		suppressed = l.suppressed
		l.lastLogged = now
		l.suppressed = 0
		return true, suppressed
	}
	l.suppressed++
	return false, 0
}

func (l *replayErrorLogLimiter) recover() (value string, suppressed int) {
	value, suppressed = l.value, l.suppressed
	*l = replayErrorLogLimiter{}
	return value, suppressed
}

// reconcileReplayErrorObservability records terminal/retryable replay errors,
// but treats a planner circuit cooldown as healthy waiting. A wait also clears
// stale metadata written by an older worker that misclassified the same typed
// condition as a replay failure.
func reconcileReplayErrorObservability(
	ctx context.Context,
	db *state.DB,
	replayErr error,
	now time.Time,
) (value string, count int, providerWait bool, err error) {
	if isIntentPlannerCircuitWait(replayErr) {
		value, count, err = clearReplayErrorObservability(ctx, db)
		return value, count, true, err
	}
	value, count, err = recordReplayErrorObservability(
		ctx, db, replayErr, now)
	return value, count, false, err
}

// recordReplayErrorObservability stores one bounded error and the number of
// consecutive identical replay failures. It intentionally uses daemon_meta so
// pre-existing schemas gain the projection without migration.
func recordReplayErrorObservability(
	ctx context.Context,
	db *state.DB,
	replayErr error,
	now time.Time,
) (string, int, error) {
	if db == nil || replayErr == nil {
		return "", 0, nil
	}
	clean := strings.TrimSpace(ai.SanitizePlannerError(replayErr.Error()))
	if clean == "" {
		clean = "replay failed"
	}
	count := 1
	if previous, ok, err := state.MetaGet(ctx, db, metaLastReplayError); err != nil {
		return clean, count, err
	} else if ok && previous == clean {
		if raw, exists, getErr := state.MetaGet(
			ctx, db, metaReplayErrorRepeat,
		); getErr != nil {
			return clean, count, getErr
		} else if exists {
			if parsed, parseErr := strconv.Atoi(strings.TrimSpace(raw)); parseErr == nil &&
				parsed > 0 && parsed < replayErrorRepeatCap {
				count = parsed + 1
			} else if parsed >= replayErrorRepeatCap {
				count = replayErrorRepeatCap
			}
		}
	}
	pairs := map[string]string{
		metaLastReplayError:       clean,
		metaReplayErrorRepeat:     strconv.Itoa(count),
		metaReplayErrorLastSeenTS: strconv.FormatInt(now.Unix(), 10),
	}
	if err := state.MetaSetMany(ctx, db, pairs); err != nil {
		return clean, count, err
	}
	if err := recordCompletedTransitionProofAttention(
		ctx, db, replayErr); err != nil {
		return clean, count, err
	}
	return clean, count, nil
}

// clearReplayErrorObservability clears the active error while returning the
// prior bounded value/count so the caller can emit an explicit recovery line.
func clearReplayErrorObservability(
	ctx context.Context,
	db *state.DB,
) (string, int, error) {
	if db == nil {
		return "", 0, nil
	}
	previous, _, err := state.MetaGet(ctx, db, metaLastReplayError)
	if err != nil {
		return "", 0, err
	}
	previous = strings.TrimSpace(ai.SanitizePlannerError(previous))
	count := 0
	if raw, ok, getErr := state.MetaGet(ctx, db, metaReplayErrorRepeat); getErr != nil {
		return previous, count, getErr
	} else if ok {
		count, _ = strconv.Atoi(strings.TrimSpace(raw))
	}
	if previous == "" && count == 0 {
		return "", 0, nil
	}
	if err := state.MetaSetMany(ctx, db, map[string]string{
		metaLastReplayError:       "",
		metaReplayErrorRepeat:     "0",
		metaReplayErrorLastSeenTS: "",
	}); err != nil {
		return previous, count, err
	}
	return previous, count, nil
}
