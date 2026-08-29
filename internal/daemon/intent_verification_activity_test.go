package daemon

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/ai"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

func TestIntentVerificationActivityBoundsIdentities(t *testing.T) {
	base := IntentVerificationActivity{
		BranchRef:        "refs/heads/main",
		BranchGeneration: 7,
		CandidateID:      "intent-candidate",
		PlanFingerprint:  "sha256:plan",
		StartedTS:        123,
	}
	oversized := strings.Repeat("x", intentVerificationIdentityMax+1)
	for _, test := range []struct {
		name   string
		mutate func(*IntentVerificationActivity)
	}{
		{name: "candidate", mutate: func(activity *IntentVerificationActivity) {
			activity.CandidateID = oversized
		}},
		{name: "plan", mutate: func(activity *IntentVerificationActivity) {
			activity.PlanFingerprint = oversized
		}},
		{name: "recovery", mutate: func(activity *IntentVerificationActivity) {
			activity.RecoveryCandidateID = oversized
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			activity := base
			test.mutate(&activity)
			if err := validateIntentVerificationActivity(activity); err == nil {
				t.Fatalf("oversized %s identity was accepted", test.name)
			}
		})
	}
}

func TestIntentVerificationActivityWrapsSynchronousVerifier(t *testing.T) {
	ctx := context.Background()
	db := openIntentCandidateTestDB(t)
	activity := IntentVerificationActivity{
		BranchRef:           "refs/heads/main",
		BranchGeneration:    7,
		CandidateID:         "intent-candidate",
		PlanFingerprint:     "sha256:plan",
		RecoveryCandidateID: "failed-candidate",
	}
	witnessed := false
	want := IntentCandidateVerification{
		Status: "passed", Output: "ok", CheckedTS: 123,
	}
	got, err := runIntentCandidateVerificationWithActivity(
		ctx, db, activity,
		func(
			context.Context,
			ai.IntentCandidateAssignment,
			[]IntentCandidateCapture,
		) (IntentCandidateVerification, error) {
			var persisted IntentVerificationActivity
			ok, err := state.MetaGetJSON(
				ctx, db, MetaKeyIntentVerificationActivity, &persisted)
			if err != nil {
				t.Fatal(err)
			}
			if !ok || persisted.BranchRef != activity.BranchRef ||
				persisted.BranchGeneration != activity.BranchGeneration ||
				persisted.CandidateID != activity.CandidateID ||
				persisted.PlanFingerprint != activity.PlanFingerprint ||
				persisted.RecoveryCandidateID != activity.RecoveryCandidateID ||
				persisted.StartedTS <= 0 {
				t.Fatalf("persisted verification activity=%+v", persisted)
			}
			witnessed = true
			return want, nil
		}, ai.IntentCandidateAssignment{CandidateID: activity.CandidateID}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !witnessed || got != want {
		t.Fatalf("verification result=%+v witnessed=%v", got, witnessed)
	}
	assertIntentVerificationActivityAbsent(t, ctx, db)
}

func TestIntentVerificationActivityCleanupIgnoresCallerCancellation(t *testing.T) {
	db := openIntentCandidateTestDB(t)
	ctx, cancel := context.WithCancel(context.Background())
	activity := IntentVerificationActivity{
		BranchRef:        "refs/heads/main",
		BranchGeneration: 7,
		CandidateID:      "intent-candidate",
		PlanFingerprint:  "sha256:plan",
	}
	_, err := runIntentCandidateVerificationWithActivity(
		ctx, db, activity,
		func(
			context.Context,
			ai.IntentCandidateAssignment,
			[]IntentCandidateCapture,
		) (IntentCandidateVerification, error) {
			cancel()
			return IntentCandidateVerification{}, context.Canceled
		}, ai.IntentCandidateAssignment{CandidateID: activity.CandidateID}, nil)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("verification error=%v, want context cancellation", err)
	}
	assertIntentVerificationActivityAbsent(t, context.Background(), db)
}

func TestClearStaleIntentVerificationActivity(t *testing.T) {
	ctx := context.Background()
	db := openIntentCandidateTestDB(t)
	if err := state.MetaSetJSON(ctx, db, MetaKeyIntentVerificationActivity,
		IntentVerificationActivity{
			BranchRef:        "refs/heads/main",
			BranchGeneration: 7,
			CandidateID:      "intent-candidate",
			PlanFingerprint:  "sha256:plan",
			StartedTS:        123,
		}); err != nil {
		t.Fatal(err)
	}
	if err := clearStaleIntentVerificationActivity(ctx, db); err != nil {
		t.Fatal(err)
	}
	assertIntentVerificationActivityAbsent(t, ctx, db)
}

func assertIntentVerificationActivityAbsent(
	t *testing.T,
	ctx context.Context,
	db *state.DB,
) {
	t.Helper()
	if _, ok, err := state.MetaGet(
		ctx, db, MetaKeyIntentVerificationActivity); err != nil {
		t.Fatal(err)
	} else if ok {
		t.Fatal("verification activity marker was not cleared")
	}
}
