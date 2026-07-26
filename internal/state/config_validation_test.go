package state

import (
	"context"
	"database/sql"
	"strings"
	"testing"
)

func validationSpec() ConfigValidationSpec {
	return ConfigValidationSpec{
		BranchRef:        "refs/heads/main",
		BranchGeneration: 4,
		ExpectedHead:     strings.Repeat("a", 40),
		Mode:             "fast",
		CommandSource:    "Go language default",
		CommandDigest:    strings.Repeat("b", 64),
		ApprovalID:       "configure:fingerprint",
	}
}

func validationRevision() ConfigRevisionInput {
	return ConfigRevisionInput{
		Snapshot:         []byte(`{"commit":{"strategy":"intent","preset":"balanced"}}`),
		Profile:          "default",
		Scope:            "repo",
		SourceGeneration: 4,
		Reason:           "configure everyday",
	}
}

func TestValidatedActivationIsAtomicAndClaimable(t *testing.T) {
	d, _ := openTestDB(t)
	ctx := context.Background()
	revision, request, queued, ok, err := CreateConfigActivationWithValidation(
		ctx, d, validationRevision(), sql.NullInt64{}, validationSpec(),
	)
	if err != nil || !ok {
		t.Fatalf("CreateConfigActivationWithValidation=(%+v,%+v,%+v,%v,%v)",
			revision, request, queued, ok, err)
	}
	if queued.Status != ConfigValidationQueued ||
		queued.RevisionID != revision.ID ||
		queued.ActivationRequestID != request.ID ||
		queued.Attempt != 1 {
		t.Fatalf("queued=%+v", queued)
	}
	claimed, ok, err := ClaimNextConfigValidation(ctx, d, 4242)
	if err != nil || !ok || claimed.ID != queued.ID ||
		claimed.Status != ConfigValidationRunning ||
		claimed.OwnerPID.Int64 != 4242 {
		t.Fatalf("claim=(%+v,%v,%v)", claimed, ok, err)
	}
	completed, err := CompleteConfigValidation(
		ctx, d, claimed.ID, 4242, ConfigValidationPassed,
		sql.NullInt64{Int64: 0, Valid: true}, "passed", "",
	)
	if err != nil || !completed {
		t.Fatalf("CompleteConfigValidation=(%v,%v)", completed, err)
	}
	latest, found, err := LatestConfigValidationForRequest(ctx, d, request.ID)
	if err != nil || !found || latest.Status != ConfigValidationPassed ||
		latest.SanitizedOutput != "passed" {
		t.Fatalf("latest=(%+v,%v,%v)", latest, found, err)
	}
}

func TestValidatedActivationStaleCASRollsBackRevision(t *testing.T) {
	d, _ := openTestDB(t)
	ctx := context.Background()
	baseline := testConfigRevision(t, d, "baseline", 1)
	if _, ok, err := RequestConfigActivation(
		ctx, d, baseline.ID, sql.NullInt64{},
	); err != nil || !ok {
		t.Fatalf("RequestConfigActivation=(%v,%v)", ok, err)
	}
	var before int
	if err := d.SQL().QueryRowContext(ctx, `SELECT COUNT(*) FROM config_revisions`).Scan(&before); err != nil {
		t.Fatal(err)
	}
	_, _, _, ok, err := CreateConfigActivationWithValidation(
		ctx, d, validationRevision(), sql.NullInt64{}, validationSpec(),
	)
	if err != nil || ok {
		t.Fatalf("stale validated activation=(%v,%v)", ok, err)
	}
	var after, validations int
	if err := d.SQL().QueryRowContext(ctx, `SELECT COUNT(*) FROM config_revisions`).Scan(&after); err != nil {
		t.Fatal(err)
	}
	if err := d.SQL().QueryRowContext(ctx, `SELECT COUNT(*) FROM config_validation_runs`).Scan(&validations); err != nil {
		t.Fatal(err)
	}
	if after != before || validations != 0 {
		t.Fatalf("atomic rollback revisions=%d->%d validations=%d", before, after, validations)
	}
}

func TestConfigValidationRetryAndSupersession(t *testing.T) {
	d, _ := openTestDB(t)
	ctx := context.Background()
	revision, request, queued, ok, err := CreateConfigActivationWithValidation(
		ctx, d, validationRevision(), sql.NullInt64{}, validationSpec(),
	)
	if err != nil || !ok {
		t.Fatal(err)
	}
	claimed, ok, err := ClaimNextConfigValidation(ctx, d, 5151)
	if err != nil || !ok {
		t.Fatalf("claim=(%v,%v)", ok, err)
	}
	longOutput := strings.Repeat("x", configValidationOutputLimit+100)
	if done, err := CompleteConfigValidation(
		ctx, d, claimed.ID, 5151, ConfigValidationFailed,
		sql.NullInt64{Int64: 2, Valid: true}, longOutput, "test failed",
	); err != nil || !done {
		t.Fatalf("complete=(%v,%v)", done, err)
	}
	retry, ok, err := RetryConfigValidation(ctx, d, request.ID)
	if err != nil || !ok || retry.Attempt != 2 ||
		retry.Status != ConfigValidationQueued {
		t.Fatalf("retry=(%+v,%v,%v)", retry, ok, err)
	}
	first, err := ConfigValidationByID(ctx, d, queued.ID)
	if err != nil || len(first.SanitizedOutput) != configValidationOutputLimit {
		t.Fatalf("bounded output=%d err=%v", len(first.SanitizedOutput), err)
	}

	newRevision := testConfigRevision(t, d, "newer", 5)
	newRequest, ok, err := RequestConfigActivation(
		ctx, d, newRevision.ID, sql.NullInt64{Int64: revision.ID, Valid: true},
	)
	if err != nil || !ok {
		t.Fatalf("new request=(%+v,%v,%v)", newRequest, ok, err)
	}
	cancelled, err := CancelSupersededConfigValidations(
		ctx, d, newRequest.ID, "new desired revision",
	)
	if err != nil || cancelled != 1 {
		t.Fatalf("cancel superseded=(%d,%v)", cancelled, err)
	}
	latest, found, err := LatestConfigValidationForRequest(ctx, d, request.ID)
	if err != nil || !found || latest.Status != ConfigValidationCancelled {
		t.Fatalf("latest=(%+v,%v,%v)", latest, found, err)
	}
}

func TestRequeueRunningConfigValidationUsesOwnerCAS(t *testing.T) {
	d, _ := openTestDB(t)
	ctx := context.Background()
	_, _, _, ok, err := CreateConfigActivationWithValidation(
		ctx, d, validationRevision(), sql.NullInt64{}, validationSpec(),
	)
	if err != nil || !ok {
		t.Fatal(err)
	}
	claimed, ok, err := ClaimNextConfigValidation(ctx, d, 6161)
	if err != nil || !ok {
		t.Fatal(err)
	}
	if requeued, err := RequeueConfigValidation(ctx, d, claimed.ID, 9999); err != nil || requeued {
		t.Fatalf("wrong owner requeue=(%v,%v)", requeued, err)
	}
	if requeued, err := RequeueConfigValidation(ctx, d, claimed.ID, 6161); err != nil || !requeued {
		t.Fatalf("owner requeue=(%v,%v)", requeued, err)
	}
	running, err := RunningConfigValidations(ctx, d)
	if err != nil || len(running) != 0 {
		t.Fatalf("running=%+v err=%v", running, err)
	}
}
