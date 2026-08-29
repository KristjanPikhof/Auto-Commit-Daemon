package daemon

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/ai"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

const MetaKeyIntentVerificationActivity = "intent.v2.active_verification"

const (
	intentVerificationIdentityMax = 128
	intentVerificationBranchMax   = 1024
	intentVerificationCleanupMax  = 5 * time.Second
)

// IntentVerificationActivity is the durable, privacy-safe proof that the
// canonical replay writer is synchronously verifying one semantic group.
type IntentVerificationActivity struct {
	BranchRef           string  `json:"branch_ref"`
	BranchGeneration    int64   `json:"branch_generation"`
	CandidateID         string  `json:"candidate_id"`
	PlanFingerprint     string  `json:"plan_fingerprint"`
	RecoveryCandidateID string  `json:"recovery_candidate_id,omitempty"`
	StartedTS           float64 `json:"started_ts"`
}

func DecodeIntentVerificationActivity(raw string) (IntentVerificationActivity, error) {
	var activity IntentVerificationActivity
	if err := json.Unmarshal([]byte(raw), &activity); err != nil {
		return activity, fmt.Errorf(
			"daemon: decode intent verification activity: %w", err)
	}
	if err := validateIntentVerificationActivity(activity); err != nil {
		return activity, err
	}
	return activity, nil
}

func validateIntentVerificationActivity(activity IntentVerificationActivity) error {
	if err := validateIntentVerificationText(
		"branch ref", activity.BranchRef, intentVerificationBranchMax, true); err != nil {
		return err
	}
	if activity.BranchGeneration < 0 {
		return errors.New(
			"daemon: intent verification activity requires a branch generation")
	}
	for _, identity := range []struct {
		name     string
		value    string
		required bool
	}{
		{name: "candidate id", value: activity.CandidateID, required: true},
		{name: "plan fingerprint", value: activity.PlanFingerprint, required: true},
		{name: "recovery candidate id", value: activity.RecoveryCandidateID},
	} {
		if err := validateIntentVerificationText(
			identity.name, identity.value, intentVerificationIdentityMax,
			identity.required); err != nil {
			return err
		}
	}
	if activity.StartedTS <= 0 || math.IsNaN(activity.StartedTS) ||
		math.IsInf(activity.StartedTS, 0) {
		return errors.New(
			"daemon: intent verification activity requires a valid start time")
	}
	return nil
}

func validateIntentVerificationText(
	name string,
	value string,
	limit int,
	required bool,
) error {
	if value == "" && !required {
		return nil
	}
	if strings.TrimSpace(value) == "" || len(value) > limit ||
		!utf8.ValidString(value) {
		return fmt.Errorf(
			"daemon: intent verification activity has an invalid %s", name)
	}
	for _, r := range value {
		if !unicode.IsPrint(r) || r == '\u007f' {
			return fmt.Errorf(
				"daemon: intent verification activity has an invalid %s", name)
		}
	}
	return nil
}

func runIntentCandidateVerificationWithActivity(
	ctx context.Context,
	db *state.DB,
	activity IntentVerificationActivity,
	verify IntentCandidateVerifier,
	assignment ai.IntentCandidateAssignment,
	captures []IntentCandidateCapture,
) (result IntentCandidateVerification, err error) {
	if db == nil || verify == nil {
		return result, errors.New(
			"daemon: intent verification activity requires state and verifier")
	}
	if activity.StartedTS <= 0 {
		activity.StartedTS = float64(time.Now().UnixNano()) / 1e9
	}
	if err := validateIntentVerificationActivity(activity); err != nil {
		return result, err
	}
	if err := state.MetaSetJSON(
		ctx, db, MetaKeyIntentVerificationActivity, activity); err != nil {
		return result, fmt.Errorf(
			"daemon: persist intent verification activity: %w", err)
	}
	defer func() {
		cleanupCtx, cancel := context.WithTimeout(
			context.WithoutCancel(ctx), intentVerificationCleanupMax)
		defer cancel()
		_, cleanupErr := state.MetaDelete(
			cleanupCtx, db, MetaKeyIntentVerificationActivity)
		if cleanupErr != nil {
			err = errors.Join(err,
				fmt.Errorf(
					"daemon: clear intent verification activity: %w", cleanupErr))
		}
	}()
	return verify(ctx, assignment, captures)
}

func clearStaleIntentVerificationActivity(
	ctx context.Context,
	db *state.DB,
) error {
	if db == nil {
		return nil
	}
	if _, ok, err := state.MetaGet(
		ctx, db, MetaKeyIntentVerificationActivity); err != nil || !ok {
		return err
	}
	_, err := state.MetaDelete(ctx, db, MetaKeyIntentVerificationActivity)
	return err
}
