package daemon

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/ai"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/config"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

func TestSetupValidationBlocksActivationUntilPass(t *testing.T) {
	f := newCaptureFixture(t)
	ctx := context.Background()
	if err := SaveBranchGeneration(
		ctx, f.db, f.cctx.BranchGeneration, f.cctx.BaseHead,
	); err != nil {
		t.Fatal(err)
	}
	command := "test -f .gitignore"
	revision, request, validation, ok, err :=
		queueDaemonSetupValidation(t, f, command, "5s")
	if err != nil || !ok {
		t.Fatalf("validated activation=(%+v,%+v,%+v,%v,%v)",
			revision, request, validation, ok, err)
	}
	closers := map[string]*runtimeTestCloser{}
	builder := runtimeBuilder(f.db, closers)
	builder.RepoRoot = f.dir
	manager := NewRuntimeBundleManager(
		&RuntimeBundle{
			Provider:       &runtimeTestProvider{name: "deterministic"},
			MessageFn:      DeterministicMessage,
			CommitStrategy: ai.CommitStrategyEvent,
		},
		builder,
		time.Second,
	)
	defer manager.Close()

	if err := manager.ActivateDesired(ctx); err != nil {
		t.Fatal(err)
	}
	if manager.Current().RevisionID != 0 {
		t.Fatal("desired revision activated before setup validation")
	}
	processed, err := manager.processNextValidation(ctx)
	if err != nil || !processed {
		t.Fatalf("process validation=(%v,%v)", processed, err)
	}
	latest, found, err := state.LatestConfigValidationForRequest(
		ctx, f.db, request.ID,
	)
	if err != nil || !found || latest.Status != state.ConfigValidationPassed {
		t.Fatalf("validation=(%+v,%v,%v)", latest, found, err)
	}
	if err := manager.ActivateDesired(ctx); err != nil {
		t.Fatal(err)
	}
	if manager.Current().RevisionID != revision.ID {
		t.Fatalf("active revision=%d want=%d",
			manager.Current().RevisionID, revision.ID)
	}
}

func TestSetupValidationCancelsWhenSuperseded(t *testing.T) {
	f := newCaptureFixture(t)
	ctx := context.Background()
	if err := SaveBranchGeneration(
		ctx, f.db, f.cctx.BranchGeneration, f.cctx.BaseHead,
	); err != nil {
		t.Fatal(err)
	}
	revision, request, validation, ok, err :=
		queueDaemonSetupValidation(t, f, "sleep 30", "1m")
	if err != nil || !ok {
		t.Fatalf("validated activation=(%+v,%+v,%+v,%v,%v)",
			revision, request, validation, ok, err)
	}
	builder := runtimeBuilder(f.db, map[string]*runtimeTestCloser{})
	builder.RepoRoot = f.dir
	manager := NewRuntimeBundleManager(
		&RuntimeBundle{
			Provider:       &runtimeTestProvider{name: "deterministic"},
			MessageFn:      DeterministicMessage,
			CommitStrategy: ai.CommitStrategyEvent,
		},
		builder,
		time.Second,
	)
	defer manager.Close()

	done := make(chan error, 1)
	go func() {
		processed, processErr := manager.processNextValidation(ctx)
		if !processed && processErr == nil {
			processErr = errors.New("validation was not processed")
		}
		done <- processErr
	}()
	deadline := time.Now().Add(5 * time.Second)
	for {
		current, currentErr := state.ConfigValidationByID(
			ctx, f.db, validation.ID,
		)
		if currentErr != nil {
			t.Fatal(currentErr)
		}
		if current.Status == state.ConfigValidationRunning {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("validation did not enter running state")
		}
		time.Sleep(10 * time.Millisecond)
	}
	newer := runtimeRevision(
		t, f.db, "newer", 2,
		map[string]any{
			config.FieldProvider:       "deterministic",
			config.FieldCommitStrategy: "event",
		},
	)
	if _, requested, requestErr := state.RequestConfigActivation(
		ctx, f.db, newer.ID,
		sql.NullInt64{Int64: revision.ID, Valid: true},
	); requestErr != nil || !requested {
		t.Fatalf("superseding activation=(%v,%v)", requested, requestErr)
	}
	select {
	case processErr := <-done:
		if processErr != nil {
			t.Fatal(processErr)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("superseded validation command was not cancelled")
	}
	latest, found, err := state.LatestConfigValidationForRequest(
		ctx, f.db, request.ID,
	)
	if err != nil || !found ||
		latest.Status != state.ConfigValidationCancelled {
		t.Fatalf("latest=(%+v,%v,%v)", latest, found, err)
	}
}

func queueDaemonSetupValidation(
	t *testing.T,
	f *captureFixture,
	command string,
	timeout string,
) (
	state.ConfigRevision,
	state.ConfigActivationRequest,
	state.ConfigValidationRun,
	bool,
	error,
) {
	t.Helper()
	digest := sha256.Sum256([]byte(command))
	snapshot, err := json.Marshal(map[string]any{
		config.FieldProvider:                "subprocess:runtime-test",
		config.FieldCommitStrategy:          "intent",
		config.FieldCommitPreset:            "balanced",
		config.FieldCommitFormat:            "imperative",
		config.FieldDiffEgress:              true,
		config.FieldIntentVerification:      "fast",
		config.FieldVerificationFastCommand: command,
		config.FieldVerificationFastTimeout: timeout,
		config.FieldIntentRepairEnabled:     true,
		"preset_id":                         "intent.balanced",
		"preset_version":                    config.PresetCatalogVersion,
		"customized":                        false,
		"confirmations": []string{
			string(ai.ConfirmationSubprocessExecution),
			string(ai.ConfirmationDiffEgress),
			string(ai.ConfirmationVerificationCommand),
			string(ai.ConfirmationIntentRepair),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	return state.CreateConfigActivationWithValidation(
		context.Background(),
		f.db,
		state.ConfigRevisionInput{
			Snapshot: snapshot, Profile: "default", Scope: "repository",
		},
		sql.NullInt64{},
		state.ConfigValidationSpec{
			BranchRef:        f.cctx.BranchRef,
			BranchGeneration: f.cctx.BranchGeneration,
			ExpectedHead:     f.cctx.BaseHead,
			Mode:             "fast",
			CommandSource:    "test fixture",
			CommandDigest:    hex.EncodeToString(digest[:]),
			ApprovalID:       "configure:fingerprint",
		},
	)
}
