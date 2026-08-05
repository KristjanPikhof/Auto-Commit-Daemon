package cli

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/config"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

func queueCLIConfigValidation(
	t *testing.T,
	d *state.DB,
) state.ConfigValidationRun {
	t.Helper()
	snapshot, err := json.Marshal(map[string]string{
		config.FieldCommitStrategy:          "intent",
		config.FieldCommitPreset:            "balanced",
		config.FieldIntentVerification:      "fast",
		config.FieldVerificationFastCommand: "go test ./... -run '^$'",
	})
	if err != nil {
		t.Fatal(err)
	}
	_, _, run, ok, err := state.CreateConfigActivationWithValidation(
		context.Background(),
		d,
		state.ConfigRevisionInput{
			Snapshot: snapshot, Profile: "default", Scope: "repo",
			SourceGeneration: 1, Reason: "configure Everyday",
		},
		sql.NullInt64{},
		state.ConfigValidationSpec{
			BranchRef: "refs/heads/main", BranchGeneration: 1,
			ExpectedHead: strings.Repeat("a", 40), Mode: "fast",
			CommandSource: "Go language default",
			CommandDigest: strings.Repeat("b", 64),
			ApprovalID:    "reviewed-fingerprint",
		},
	)
	if err != nil || !ok {
		t.Fatalf("queue validation=(%+v,%v,%v)", run, ok, err)
	}
	return run
}

func TestConfigurationReadinessSurfacesAndFailureRecovery(t *testing.T) {
	roots := withIsolatedHome(t)
	ctx := context.Background()
	repo, dbPath, d := makeRepoStateDB(t)
	registerRepo(t, roots, repo, dbPath, "codex")
	queued := queueCLIConfigValidation(t, d)

	var statusOut bytes.Buffer
	if err := runStatus(ctx, &statusOut, repo, true); err != nil {
		t.Fatal(err)
	}
	var status statusReport
	if err := json.Unmarshal(statusOut.Bytes(), &status); err != nil {
		t.Fatal(err)
	}
	if status.Configuration.Configuration != "validating" ||
		status.Configuration.Experience != "Everyday" ||
		status.Configuration.Command != "go test ./... -run '^$'" ||
		status.Configuration.Source != "Go language default" {
		t.Fatalf("status readiness=%+v", status.Configuration)
	}

	var diagnoseOut bytes.Buffer
	if err := runDiagnose(ctx, &diagnoseOut, repo, true); err != nil {
		t.Fatal(err)
	}
	var diagnose diagnoseReport
	if err := json.Unmarshal(diagnoseOut.Bytes(), &diagnose); err != nil {
		t.Fatal(err)
	}
	if diagnose.Configuration.Validation != state.ConfigValidationQueued {
		t.Fatalf("diagnose readiness=%+v", diagnose.Configuration)
	}

	var eventsOut bytes.Buffer
	if err := runEvents(
		ctx, &eventsOut, repo, "", 0, 10, false,
		time.Millisecond, true,
	); err != nil {
		t.Fatal(err)
	}
	var events eventsReport
	if err := json.Unmarshal(eventsOut.Bytes(), &events); err != nil {
		t.Fatal(err)
	}
	if events.Configuration.Configuration != "validating" {
		t.Fatalf("events readiness=%+v", events.Configuration)
	}

	var doctorOut bytes.Buffer
	if err := runDoctor(ctx, &doctorOut, false, "", true); err != nil {
		t.Fatal(err)
	}
	var doctor doctorReport
	if err := json.Unmarshal(doctorOut.Bytes(), &doctor); err != nil {
		t.Fatal(err)
	}
	if len(doctor.Repos) != 1 ||
		doctor.Repos[0].Configuration.Configuration != "validating" {
		t.Fatalf("doctor readiness=%+v", doctor.Repos)
	}

	claimed, ok, err := state.ClaimNextConfigValidation(ctx, d, 4242)
	if err != nil || !ok || claimed.ID != queued.ID {
		t.Fatalf("claim validation=(%+v,%v,%v)", claimed, ok, err)
	}
	exitCode := sql.NullInt64{Int64: 2, Valid: true}
	if completed, err := state.CompleteConfigValidation(
		ctx, d, claimed.ID, 4242, state.ConfigValidationFailed,
		exitCode, "FAIL package check", "full check failed",
	); err != nil || !completed {
		t.Fatalf("complete validation=(%v,%v)", completed, err)
	}
	statusOut.Reset()
	if err := runStatus(ctx, &statusOut, repo, false); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"Configuration: needs_attention (Everyday)",
		"Validation: failed attempt=1",
		"FAIL package check",
	} {
		if !strings.Contains(statusOut.String(), want) {
			t.Fatalf("status output missing %q:\n%s", want, statusOut.String())
		}
	}
}

func TestReplayObservabilitySurfaces(t *testing.T) {
	roots := withIsolatedHome(t)
	ctx := context.Background()
	repo, dbPath, d := makeRepoStateDB(t)
	registerRepo(t, roots, repo, dbPath, "codex")
	if err := state.MetaSetMany(ctx, d, map[string]string{
		"last_replay_error":         "candidate planning failed",
		"replay.error_repeat_count": "4",
	}); err != nil {
		t.Fatal(err)
	}

	var statusOut bytes.Buffer
	if err := runStatus(ctx, &statusOut, repo, true); err != nil {
		t.Fatal(err)
	}
	var status statusReport
	if err := json.Unmarshal(statusOut.Bytes(), &status); err != nil {
		t.Fatal(err)
	}

	var diagnoseOut bytes.Buffer
	if err := runDiagnose(ctx, &diagnoseOut, repo, true); err != nil {
		t.Fatal(err)
	}
	var diagnose diagnoseReport
	if err := json.Unmarshal(diagnoseOut.Bytes(), &diagnose); err != nil {
		t.Fatal(err)
	}

	var eventsOut bytes.Buffer
	if err := runEvents(
		ctx, &eventsOut, repo, "", 0, 10, false,
		time.Millisecond, true,
	); err != nil {
		t.Fatal(err)
	}
	var events eventsReport
	if err := json.Unmarshal(eventsOut.Bytes(), &events); err != nil {
		t.Fatal(err)
	}

	var doctorOut bytes.Buffer
	if err := runDoctor(ctx, &doctorOut, false, "", true); err != nil {
		t.Fatal(err)
	}
	var doctor doctorReport
	if err := json.Unmarshal(doctorOut.Bytes(), &doctor); err != nil {
		t.Fatal(err)
	}
	if len(doctor.Repos) != 1 {
		t.Fatalf("doctor repos=%+v", doctor.Repos)
	}
	for name, replay := range map[string]replayObservabilityReport{
		"status":   status.Replay,
		"diagnose": diagnose.Replay,
		"events":   events.Replay,
		"doctor":   doctor.Repos[0].Replay,
	} {
		if replay.State != "needs_attention" ||
			replay.ErrorRepeatCount != 4 ||
			replay.LastError != "candidate planning failed" {
			t.Fatalf("%s replay=%+v", name, replay)
		}
	}
}
