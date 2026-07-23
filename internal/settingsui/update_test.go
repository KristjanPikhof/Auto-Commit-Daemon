package settingsui

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/ai"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/config"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/settings"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

type adapterService struct {
	snapshot            settings.Snapshot
	saved               settings.SaveRequest
	applied             settings.ApplyRequest
	applyCalls          int
	revertCalls         int
	validate            settings.Validation
	test                settings.ProviderTestResult
	err                 error
	startedWindows      int
	cancelledExperiment int64
	required            []ai.ConfirmationRequirement
	testRequired        []ai.ConfirmationRequirement
	validatedWith       []ai.ConfirmationRequirement
	testedWith          []ai.ConfirmationRequirement
}

func (a *adapterService) Snapshot(context.Context, settings.Scope, string) (settings.Snapshot, error) {
	return a.snapshot, a.err
}
func (a *adapterService) Save(_ context.Context, r settings.SaveRequest) (settings.SaveResult, error) {
	a.saved = r
	return settings.SaveResult{Generation: r.ExpectedGeneration + 1, Scope: r.Scope}, a.err
}
func (a *adapterService) Validate(_ context.Context, _ map[string]string, confirmed []ai.ConfirmationRequirement) (settings.Validation, error) {
	a.validatedWith = append([]ai.ConfirmationRequirement(nil), confirmed...)
	validation := a.validate
	confirmedSet := map[ai.ConfirmationRequirement]bool{}
	for _, value := range confirmed {
		confirmedSet[value] = true
	}
	validation.Missing = nil
	for _, value := range a.required {
		if !confirmedSet[value] {
			validation.Missing = append(validation.Missing, value)
		}
	}
	return validation, a.err
}

func (a *adapterService) TestProvider(_ context.Context, _ map[string]string, confirmed []ai.ConfirmationRequirement) (settings.ProviderTestResult, error) {
	a.testedWith = append([]ai.ConfirmationRequirement(nil), confirmed...)
	confirmedSet := map[ai.ConfirmationRequirement]bool{}
	for _, value := range confirmed {
		confirmedSet[value] = true
	}
	var missing []ai.ConfirmationRequirement
	for _, value := range a.testRequired {
		if !confirmedSet[value] {
			missing = append(missing, value)
		}
	}
	if len(missing) > 0 {
		return settings.ProviderTestResult{}, &settings.ConfirmationRequiredError{Missing: missing}
	}
	return a.test, a.err
}
func (a *adapterService) Apply(_ context.Context, r settings.ApplyRequest) (settings.ApplyResult, error) {
	a.applyCalls++
	a.applied = r
	return settings.ApplyResult{RevisionID: 8, Queued: true}, a.err
}
func (a *adapterService) Revert(context.Context, settings.RevertRequest) (settings.ApplyResult, error) {
	a.revertCalls++
	return settings.ApplyResult{RevisionID: 9, Queued: true}, a.err
}
func (a *adapterService) StartExperiment(_ context.Context, req settings.ExperimentRequest) (settings.ExperimentResult, error) {
	a.startedWindows = req.WindowBudget
	return settings.ExperimentResult{Experiment: settings.ExperimentSnapshot{ID: 4, WindowBudget: req.WindowBudget, Status: "active"}, Candidate: settings.ApplyResult{RevisionID: 8, Queued: true}}, a.err
}
func (a *adapterService) ExperimentProgress(context.Context, int64) (settings.ExperimentSnapshot, error) {
	return settings.ExperimentSnapshot{ID: 4, WindowBudget: 10, CompletedWindows: 3, Status: "active"}, a.err
}
func (a *adapterService) CancelExperiment(_ context.Context, id int64) (settings.ExperimentResult, error) {
	a.cancelledExperiment = id
	return settings.ExperimentResult{Revert: settings.ApplyResult{RevisionID: 9, Queued: true}}, a.err
}
func (a *adapterService) RevertExperiment(ctx context.Context, id int64) (settings.ExperimentResult, error) {
	return a.CancelExperiment(ctx, id)
}
func (a *adapterService) Compare(context.Context, ...int64) (settings.Comparison, error) {
	return settings.Comparison{Interpretation: settings.ComparisonInterpretation}, a.err
}

func adapterFixture() *adapterService {
	return &adapterService{snapshot: settings.Snapshot{Scope: settings.ScopeRepository, Profile: "fast", SavedGeneration: 2, DesiredRevisionID: 7, AppliedRevisionID: 7, LastKnownGoodRevisionID: 6, DaemonRunning: true, Fields: []settings.FieldSnapshot{{Name: config.FieldModel, DraftValue: "old", ActiveValue: "active", Source: config.SourceProfile, ShadowedEnvironment: "env-model", EnvironmentSet: true, Persistable: true, Boundary: config.ApplyHot}, {Name: config.FieldAPIKey, DraftValue: "set", Sensitive: true, EnvironmentSet: true}, {Name: "capture.max_file_bytes", DraftValue: "10", ActiveValue: "10", Source: config.SourceDefault, Persistable: true, Boundary: config.ApplyRestart}}}, validate: settings.Validation{Fingerprint: "service-fp"}, test: settings.ProviderTestResult{Fingerprint: "service-fp", Provider: "openai-compat", Success: true}}
}

func TestSettingsRuntimeRailUsesRealTestAndApplyAcknowledgements(t *testing.T) {
	svc := adapterFixture()
	b := NewServiceBackend(svc, BackendAdapterOptions{Scope: settings.ScopeRepository})
	snap, err := b.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if snap.DesiredRevision != 7 || snap.AppliedRevision != 7 {
		t.Fatalf("snapshot=%+v", snap)
	}
	draft := map[string]string{config.FieldModel: "new"}
	tested, err := b.Test(context.Background(), draft)
	if err != nil || !tested.OK || tested.Fingerprint != "service-fp" {
		t.Fatalf("test=%+v err=%v", tested, err)
	}
	applied, err := b.Apply(context.Background(), draft, "ignored-ui-fingerprint")
	if err != nil {
		t.Fatal(err)
	}
	if !applied.Queued || applied.DesiredRevision != 8 || applied.AppliedRevision != 7 || svc.applied.TestedFingerprint != "service-fp" {
		t.Fatalf("result=%+v request=%+v", applied, svc.applied)
	}
}

func TestSettingsRuntimeRailStaleDraftAndRejectionDoNotApply(t *testing.T) {
	svc := adapterFixture()
	b := NewServiceBackend(svc, BackendAdapterOptions{Scope: settings.ScopeRepository})
	_, _ = b.Snapshot(context.Background())
	_, _ = b.Test(context.Background(), map[string]string{config.FieldModel: "a"})
	if _, err := b.Apply(context.Background(), map[string]string{config.FieldModel: "b"}, ""); err == nil || svc.applyCalls != 0 {
		t.Fatalf("stale err=%v calls=%d", err, svc.applyCalls)
	}
	svc.err = errors.New("rejected\x1b[2J\x00secret-free")
	if _, err := b.Test(context.Background(), map[string]string{}); err == nil || err.Error() != "rejected secret-free" {
		t.Fatalf("sanitized err=%q", err)
	}
}

func TestSettingsConfirmationDiffBeforeAnyApplyMutation(t *testing.T) {
	svc := adapterFixture()
	svc.required = []ai.ConfirmationRequirement{ai.ConfirmationDiffEgress}
	b := NewServiceBackend(svc, BackendAdapterOptions{Scope: settings.ScopeRepository})
	_, _ = b.Snapshot(context.Background())
	draft := map[string]string{config.FieldModel: "candidate"}
	if _, err := b.Test(context.Background(), draft); err != nil {
		t.Fatal(err)
	}
	_, err := b.Apply(context.Background(), draft, "")
	var confirmationErr *ConfirmationRequiredError
	if !errors.As(err, &confirmationErr) || len(confirmationErr.Missing) != 1 || confirmationErr.Missing[0].ID != string(ai.ConfirmationDiffEgress) {
		t.Fatalf("confirmation error=%v", err)
	}
	if svc.saved.Values != nil || svc.applyCalls != 0 {
		t.Fatalf("pre-confirmation mutation: save=%+v applyCalls=%d", svc.saved, svc.applyCalls)
	}
	b.Confirm(confirmationErr.Missing)
	if _, err := b.Apply(context.Background(), draft, ""); err != nil {
		t.Fatal(err)
	}
	if svc.applyCalls != 1 || !reflect.DeepEqual(svc.applied.Confirmations, []ai.ConfirmationRequirement{ai.ConfirmationDiffEgress}) {
		t.Fatalf("apply calls=%d confirmations=%v", svc.applyCalls, svc.applied.Confirmations)
	}
}

func TestSettingsInteractiveConfirmationIsBoundToDraft(t *testing.T) {
	svc := adapterFixture()
	svc.testRequired = []ai.ConfirmationRequirement{ai.ConfirmationEndpointCredentials}
	b := NewServiceBackend(svc, BackendAdapterOptions{Scope: settings.ScopeRepository})
	draftA := map[string]string{config.FieldBaseURL: "https://a.example/v1"}
	draftB := map[string]string{config.FieldBaseURL: "https://b.example/v1"}

	_, err := b.Test(context.Background(), draftA)
	var confirmationErr *ConfirmationRequiredError
	if !errors.As(err, &confirmationErr) {
		t.Fatalf("draft A confirmation error=%v", err)
	}
	b.Confirm(confirmationErr.Missing)
	if _, err := b.Test(context.Background(), draftA); err != nil {
		t.Fatalf("confirmed draft A: %v", err)
	}
	if !reflect.DeepEqual(svc.testedWith, []ai.ConfirmationRequirement{ai.ConfirmationEndpointCredentials}) {
		t.Fatalf("draft A confirmations=%v", svc.testedWith)
	}

	_, err = b.Test(context.Background(), draftB)
	if !errors.As(err, &confirmationErr) {
		t.Fatalf("changed draft reused confirmation: %v", err)
	}
	if len(svc.testedWith) != 0 {
		t.Fatalf("changed draft confirmations=%v", svc.testedWith)
	}
}

func TestSettingsLaunchConfirmationRemainsDraftIndependent(t *testing.T) {
	svc := adapterFixture()
	svc.testRequired = []ai.ConfirmationRequirement{ai.ConfirmationEndpointCredentials}
	b := NewServiceBackend(svc, BackendAdapterOptions{Scope: settings.ScopeRepository,
		Confirmations: []ai.ConfirmationRequirement{ai.ConfirmationEndpointCredentials}})
	for _, endpoint := range []string{"https://a.example/v1", "https://b.example/v1"} {
		if _, err := b.Test(context.Background(), map[string]string{config.FieldBaseURL: endpoint}); err != nil {
			t.Fatalf("pre-authorized %s: %v", endpoint, err)
		}
	}
}

func TestSettingsSourceShadowClearSecretAndRestartProjection(t *testing.T) {
	svc := adapterFixture()
	b := NewServiceBackend(svc, BackendAdapterOptions{Scope: settings.ScopeRepository})
	snap, err := b.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if snap.Fields[0].Source != "profile" || snap.Fields[0].Shadowed != "environment: env-model" || snap.Fields[0].ActiveValue != "active" {
		t.Fatalf("field=%+v", snap.Fields[0])
	}
	if snap.Fields[1].Value != "" || !snap.Fields[1].SensitiveSet || snap.Fields[1].Shadowed != "environment" {
		t.Fatalf("secret=%+v", snap.Fields[1])
	}
	if !snap.Fields[2].Restart {
		t.Fatal("restart label missing")
	}
	_, _ = b.Test(context.Background(), map[string]string{config.FieldModel: ""})
	_, err = b.Apply(context.Background(), map[string]string{config.FieldModel: ""}, "")
	if err != nil {
		t.Fatal(err)
	}
	if value, ok := svc.saved.Values[config.FieldModel]; !ok || value != nil {
		t.Fatalf("clear=%v present=%v", value, ok)
	}
}

func TestSettingsProfileAndGlobalSaveNeverQueueActivation(t *testing.T) {
	for _, scope := range []settings.Scope{settings.ScopeProfile, settings.ScopeGlobal} {
		t.Run(string(scope), func(t *testing.T) {
			svc := adapterFixture()
			svc.required = []ai.ConfirmationRequirement{ai.ConfirmationDiffEgress}
			b := NewServiceBackend(svc, BackendAdapterOptions{Scope: scope, Profile: "fast"})
			_, _ = b.Snapshot(context.Background())
			draft := map[string]string{config.FieldModel: "new"}
			_, _ = b.Test(context.Background(), draft)
			got, err := b.Apply(context.Background(), draft, "")
			if err != nil {
				t.Fatal(err)
			}
			if got.Queued || svc.applyCalls != 0 {
				t.Fatalf("fanout result=%+v calls=%d", got, svc.applyCalls)
			}
			if len(svc.validatedWith) != 0 {
				t.Fatalf("non-repository save checked activation confirmations: %v", svc.validatedWith)
			}
			if scope == settings.ScopeGlobal && got.Summary != "global defaults saved; running repositories were not changed" {
				t.Fatalf("summary=%q", got.Summary)
			}
		})
	}
}

func TestSettingsProfileExperimentStartProgressCancel(t *testing.T) {
	svc := adapterFixture()
	b := NewServiceBackend(svc, BackendAdapterOptions{Scope: settings.ScopeRepository})
	_, _ = b.Snapshot(context.Background())
	_, _ = b.Test(context.Background(), map[string]string{config.FieldModel: "candidate"})
	got, err := b.StartExperiment(context.Background(), map[string]string{config.FieldModel: "candidate"}, ExperimentOptions{WindowBudget: 10})
	if err != nil || got.ID != 4 || svc.startedWindows != 10 {
		t.Fatalf("start=%+v err=%v", got, err)
	}
	cancel, err := b.CancelExperiment(context.Background(), got.ID)
	if err != nil || !cancel.Queued || svc.cancelledExperiment != 4 {
		t.Fatalf("cancel=%+v err=%v", cancel, err)
	}
	if _, err := b.StartExperiment(context.Background(), nil, ExperimentOptions{}); err == nil {
		t.Fatal("unbounded experiment accepted")
	}
}

func TestSettingsTerminalExperimentDoesNotHijackKnownGoodRevert(t *testing.T) {
	svc := adapterFixture()
	svc.snapshot.Experiment = &settings.ExperimentSnapshot{ID: 4, Status: state.ExperimentCompleted}
	b := NewServiceBackend(svc, BackendAdapterOptions{Scope: settings.ScopeRepository})
	_, _ = b.Snapshot(context.Background())

	result, err := b.Revert(context.Background(), 0)
	if err != nil {
		t.Fatal(err)
	}
	if svc.revertCalls != 1 || svc.cancelledExperiment != 0 || result.DesiredRevision != 9 {
		t.Fatalf("revert=%+v normalCalls=%d experimentID=%d", result, svc.revertCalls, svc.cancelledExperiment)
	}
}
