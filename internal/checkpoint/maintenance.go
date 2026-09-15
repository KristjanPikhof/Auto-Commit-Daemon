package checkpoint

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

const MaintenanceMetaKey = "protection.maintenance"

// MaintenanceStatus separates a failed measurement from measured storage use.
// NextAttemptTS survives restarts; successful checks return to an hourly cadence.
type MaintenanceStatus struct {
	State         string `json:"state"`
	Error         string `json:"error,omitempty"`
	Failures      int    `json:"failures,omitempty"`
	NextAttemptTS int64  `json:"next_attempt_ts,omitempty"`
	LastSuccessTS int64  `json:"last_success_ts,omitempty"`
	ContentBytes  int64  `json:"content_bytes"`
	ProtectedBytes int64 `json:"protected_bytes"`
	OverBudget    bool   `json:"over_budget"`
}

func DecodeMaintenance(raw, legacy string) MaintenanceStatus {
	var result MaintenanceStatus
	if raw != "" {
		if err := json.Unmarshal([]byte(raw), &result); err == nil {
			return result
		}
		return MaintenanceStatus{State: "retrying", Error: "Saved checkpoint maintenance result could not be read."}
	}
	switch legacy {
	case "needs_action":
		return MaintenanceStatus{State: "retrying", Error: "An earlier checkpoint maintenance check failed; awaiting recheck."}
	case "true":
		return MaintenanceStatus{State: "over_budget", OverBudget: true}
	default:
		return result
	}
}

func (s Store) LoadMaintenance(ctx context.Context) (MaintenanceStatus, error) {
	raw, _, err := state.MetaGet(ctx, s.DB, MaintenanceMetaKey)
	if err != nil {
		return MaintenanceStatus{}, err
	}
	legacy, _, err := state.MetaGet(ctx, s.DB, "protection.retention_over_budget")
	return DecodeMaintenance(raw, legacy), err
}

func (m MaintenanceStatus) NeedsAction() bool {
	return m.State == "prerequisite" || m.State == "needs_action" || m.State == "over_budget"
}

func (m MaintenanceStatus) Summary() string {
	switch m.State {
	case "retrying":
		return "Checkpoint maintenance failed; ACD will retry automatically."
	case "prerequisite":
		return "Checkpoint maintenance is waiting for the Xcode license agreement."
	case "needs_action":
		return "Checkpoint maintenance stopped at a safety check. Protected data was retained."
	case "over_budget":
		return "Retained checkpoints exceed the storage budget. Protected data was retained."
	default:
		return ""
	}
}

func (m MaintenanceStatus) NextAction() string {
	switch m.State {
	case "prerequisite":
		return "Review the Xcode license with `sudo xcodebuild -license` in a terminal. ACD will recheck automatically."
	case "needs_action", "over_budget":
		return "Run `acd doctor` to review checkpoint maintenance details."
	default:
		return "No action needed. ACD will retry checkpoint maintenance automatically."
	}
}

// retentionSafetyError identifies failures after a prune has entered its
// durable recovery protocol. Retrying still uses that protocol's ref proofs.
type retentionSafetyError struct{ error }

func (e retentionSafetyError) Unwrap() error { return e.error }

func maintenanceFailure(err error) string {
	if strings.Contains(err.Error(), "license agreements") || strings.Contains(err.Error(), "xcodebuild -license") {
		return "prerequisite"
	}
	var safety retentionSafetyError
	if errors.As(err, &safety) {
		return "needs_action"
	}
	return "retrying"
}

func maintenanceRetry(failures int) time.Duration {
	delays := [...]time.Duration{time.Minute, 2 * time.Minute, 5 * time.Minute, 15 * time.Minute}
	return delays[min(max(failures-1, 0), len(delays)-1)]
}

// Maintain runs only when due. Errors from retention are durable outcomes;
// the returned error means the outcome itself could not be persisted.
func (s Store) Maintain(ctx context.Context, repo, worktree string, now time.Time, previous MaintenanceStatus) (MaintenanceStatus, error) {
	if now.Unix() < previous.NextAttemptTS {
		return previous, nil
	}
	result := previous
	summary, retentionErr := s.ApplyRetention(ctx, repo, worktree, now)
	if retentionErr != nil {
		result.State = maintenanceFailure(retentionErr)
		result.Error = strings.Join(strings.Fields(retentionErr.Error()), " ")
		if len(result.Error) > 2048 {
			result.Error = result.Error[:2048]
		}
		result.Failures = min(previous.Failures+1, 4)
		result.NextAttemptTS = now.Add(maintenanceRetry(result.Failures)).Unix()
	} else {
		result = MaintenanceStatus{
			State: "healthy", NextAttemptTS: now.Add(time.Hour).Unix(), LastSuccessTS: now.Unix(),
			ContentBytes: summary.ContentBytes, ProtectedBytes: summary.ProtectedBytes, OverBudget: summary.OverBudget,
		}
		if summary.OverBudget {
			result.State = "over_budget"
		}
	}
	raw, err := json.Marshal(result)
	if err != nil {
		return previous, err
	}
	// The legacy field remains a measurement, never an error sentinel.
	if err := state.MetaSetMany(ctx, s.DB, map[string]string{
		MaintenanceMetaKey: string(raw), "protection.retention_over_budget": fmt.Sprint(result.OverBudget),
	}); err != nil {
		return previous, err
	}
	return result, nil
}
