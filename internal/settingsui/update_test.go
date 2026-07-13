package settingsui

import (
	"context"
	"errors"
	"testing"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/ai"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/config"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/settings"
)

type adapterService struct {
	snapshot            settings.Snapshot
	saved               settings.SaveRequest
	applied             settings.ApplyRequest
	applyCalls          int
	validate            settings.Validation
	test                settings.ProviderTestResult
	err                 error
	startedWindows      int
	cancelledExperiment int64
}

func (a *adapterService) Snapshot(context.Context, settings.Scope, string) (settings.Snapshot, error) {
	return a.snapshot, a.err
}
func (a *adapterService) Save(_ context.Context, r settings.SaveRequest) (settings.SaveResult, error) {
	a.saved = r
	return settings.SaveResult{Generation: r.ExpectedGeneration + 1, Scope: r.Scope}, a.err
}
func (a *adapterService) Validate(context.Context, map[string]string, []ai.ConfirmationRequirement) (settings.Validation, error) {
	return a.validate, a.err
}
func (a *adapterService) TestProvider(context.Context, map[string]string, []ai.ConfirmationRequirement) (settings.ProviderTestResult, error) {
	return a.test, a.err
}
func (a *adapterService) Apply(_ context.Context, r settings.ApplyRequest) (settings.ApplyResult, error) {
	a.applyCalls++
	a.applied = r
	return settings.ApplyResult{RevisionID: 8, Queued: true}, a.err
}
func (a *adapterService) Revert(context.Context, settings.RevertRequest) (settings.ApplyResult, error) {
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
	if _, err := b.Test(context.Background(), map[string]string{}); err == nil || err.Error() != "rejectedsecret-free" {
		t.Fatalf("sanitized err=%q", err)
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
	got, err := b.StartExperiment(context.Background(), map[string]string{config.FieldModel: "candidate"}, 10)
	if err != nil || got.ID != 4 || svc.startedWindows != 10 {
		t.Fatalf("start=%+v err=%v", got, err)
	}
	cancel, err := b.CancelExperiment(context.Background(), got.ID)
	if err != nil || !cancel.Queued || svc.cancelledExperiment != 4 {
		t.Fatalf("cancel=%+v err=%v", cancel, err)
	}
	if _, err := b.StartExperiment(context.Background(), nil, 0); err == nil {
		t.Fatal("unbounded experiment accepted")
	}
}
