package daemon

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/ai"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/config"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

type runtimeTestProvider struct{ name string }

func (p *runtimeTestProvider) Name() string { return p.name }
func (p *runtimeTestProvider) Generate(context.Context, ai.CommitContext) (ai.Result, error) {
	return ai.Result{Subject: "Test runtime bundle"}, nil
}
func (p *runtimeTestProvider) PlanIntent(_ context.Context, req ai.IntentPlanRequest) (ai.IntentPlan, error) {
	if len(req.OfferedCaptures) == 0 {
		return ai.IntentPlan{}, errors.New("no captures")
	}
	return ai.IntentPlan{SelectedSeqs: []int64{req.OfferedCaptures[0].Seq}, Subject: "Test runtime bundle", GroupingReason: "synthetic", Source: p.name}, nil
}

type runtimeTestCloser struct {
	calls atomic.Int32
	block <-chan struct{}
}

func (c *runtimeTestCloser) Close() error {
	c.calls.Add(1)
	if c.block != nil {
		<-c.block
	}
	return nil
}

func runtimeRevision(t *testing.T, db *state.DB, profile string, generation int64, values map[string]any) state.ConfigRevision {
	t.Helper()
	strategy, _ := values[config.FieldCommitStrategy].(string)
	if strategy == "" {
		strategy = string(ai.CommitStrategyEvent)
		values[config.FieldCommitStrategy] = strategy
	}
	preset := config.PresetFast
	presetID := "event.fast"
	if strategy == string(ai.CommitStrategyIntent) {
		presetID = "intent.fast"
		values[config.FieldCommitPreset] = string(preset)
		values[config.FieldDiffEgress] = true
		provider, _ := values[config.FieldProvider].(string)
		confirmations := []string{string(ai.ConfirmationDiffEgress)}
		if provider == "" || provider == "deterministic" {
			values[config.FieldProvider] = "subprocess:runtime-test"
			confirmations = append(confirmations,
				string(ai.ConfirmationSubprocessExecution))
		}
		if _, exists := values["confirmations"]; !exists {
			values["confirmations"] = confirmations
		}
	}
	values["preset_id"] = presetID
	values["preset_version"] = config.PresetCatalogVersion
	values["customized"] = true
	body, err := json.Marshal(values)
	if err != nil {
		t.Fatal(err)
	}
	revision, err := state.InsertConfigRevision(context.Background(), db, state.ConfigRevisionInput{Snapshot: body, Profile: profile, Scope: "repo", SourceGeneration: generation})
	if err != nil {
		t.Fatal(err)
	}
	return revision
}

func runtimeBuilder(db *state.DB, closers map[string]*runtimeTestCloser) RuntimeBundleBuilder {
	return RuntimeBundleBuilder{DB: db, BuildProvider: func(cfg ai.ProviderConfig) (ai.Provider, io.Closer, error) {
		name := cfg.Mode
		if name == "openai-compat" {
			name = "openai-compat"
		}
		closer := &runtimeTestCloser{}
		closers[cfg.Model] = closer
		return &runtimeTestProvider{name: name}, closer, nil
	}}
}

func TestRuntimeConfigAllowsDeterministicIntentFastWithoutCredentials(t *testing.T) {
	db := openTestDB(t)
	values := map[string]any{
		config.FieldProvider:            "deterministic",
		config.FieldDiffEgress:          false,
		config.FieldCommitStrategy:      "intent",
		config.FieldCommitPreset:        "fast",
		config.FieldIntentVerification:  "structural",
		config.FieldIntentRepairEnabled: false,
		"preset_id":                     "intent.fast",
		"preset_version":                config.PresetCatalogVersion,
		"customized":                    true,
		"confirmations":                 []string{},
	}
	body, err := json.Marshal(values)
	if err != nil {
		t.Fatal(err)
	}
	revision, err := state.InsertConfigRevision(context.Background(), db,
		state.ConfigRevisionInput{Snapshot: body, Profile: "default", Scope: "repository"})
	if err != nil {
		t.Fatal(err)
	}
	bundle, err := (RuntimeBundleBuilder{DB: db, RepoRoot: t.TempDir()}).
		BuildRevision(context.Background(), revision, nil)
	if err != nil {
		t.Fatal(err)
	}
	if bundle.ReplayBlockedReason != "" || bundle.IntentPlanner == nil ||
		bundle.PresetID != "intent.fast" || bundle.IntentPreset != config.PresetFast {
		t.Fatalf("deterministic Intent/Fast bundle=%+v", bundle)
	}
}

func TestRuntimeConfigAllowsDeterministicEverydayButBlocksQuality(t *testing.T) {
	db := openTestDB(t)
	build := func(t *testing.T, preset string) *RuntimeBundle {
		t.Helper()
		values := map[string]any{
			config.FieldProvider:            "deterministic",
			config.FieldDiffEgress:          false,
			config.FieldCommitStrategy:      "intent",
			config.FieldCommitPreset:        preset,
			config.FieldIntentVerification:  "structural",
			config.FieldIntentRepairEnabled: preset == "balanced",
			"preset_id":                     "intent." + preset,
			"preset_version":                config.PresetCatalogVersion,
			"customized":                    true,
			"confirmations":                 []string{"intent_repair"},
		}
		body, err := json.Marshal(values)
		if err != nil {
			t.Fatal(err)
		}
		revision, err := state.InsertConfigRevision(context.Background(), db,
			state.ConfigRevisionInput{Snapshot: body, Profile: "default", Scope: "repository"})
		if err != nil {
			t.Fatal(err)
		}
		bundle, err := (RuntimeBundleBuilder{DB: db, RepoRoot: t.TempDir()}).
			BuildRevision(context.Background(), revision, nil)
		if err != nil {
			t.Fatal(err)
		}
		return bundle
	}

	everyday := build(t, "balanced")
	if everyday.ReplayBlockedReason != "" || everyday.IntentPlanner == nil ||
		everyday.PresetID != "intent.balanced" {
		t.Fatalf("deterministic Everyday bundle = %+v", everyday)
	}
	quality := build(t, "quality")
	if quality.ReplayBlockedReason == "" {
		t.Fatalf("deterministic Quality unexpectedly active = %+v", quality)
	}
}

func TestRuntimeBundleLeaseKeepsOneImmutableRevision(t *testing.T) {
	t.Setenv(ai.EnvAPIKey, "test-only-key")
	t.Setenv(ai.EnvBaseURL, ai.DefaultOpenAIBaseURL)
	t.Setenv(ai.EnvDiffEgress, "false")
	db := openTestDB(t)
	closers := map[string]*runtimeTestCloser{}
	a := runtimeRevision(t, db, "work", 1, map[string]any{
		"ai.provider": "openai-compat", "ai.model": "model-a",
		"commit.strategy": "intent", "commit.format": "imperative",
		"intent.window": 3, "intent.min_pending": 2,
	})
	b := runtimeRevision(t, db, "work", 2, map[string]any{
		"ai.provider": "openai-compat", "ai.model": "model-a",
		"commit.strategy": "intent", "commit.format": "conventional",
		"intent.window": 9, "intent.min_pending": 4,
		"intent.retry_on_invalid": 1,
	})
	c := runtimeRevision(t, db, "work", 3, map[string]any{
		"ai.provider": "openai-compat", "ai.model": "model-c",
		"commit.strategy": "intent", "intent.window": 11,
	})
	builder := runtimeBuilder(db, closers)
	bundleA, err := builder.BuildRevision(context.Background(), a, nil)
	if err != nil {
		t.Fatal(err)
	}
	bundleB, err := builder.BuildRevision(context.Background(), b, bundleA)
	if err != nil {
		t.Fatal(err)
	}
	if bundleA.IntentHealth != bundleB.IntentHealth {
		t.Fatal("intent-only/format tuning reset circuit health")
	}
	if bundleB.IntentRetryLimit != 1 {
		t.Fatalf("retry limit=%d", bundleB.IntentRetryLimit)
	}
	bundleC, err := builder.BuildRevision(context.Background(), c, bundleB)
	if err != nil {
		t.Fatal(err)
	}
	if bundleC.IntentHealth == bundleB.IntentHealth || bundleC.HealthFingerprint == bundleB.HealthFingerprint {
		t.Fatal("provider identity change preserved circuit health")
	}
	withOtherTrust := bundleC.HealthIdentity
	withOtherTrust.TrustFingerprint = "sha256:other"
	if IntentPlannerProviderFingerprint(withOtherTrust) == bundleC.HealthFingerprint {
		t.Fatal("CA trust change preserved circuit identity")
	}

	manager := NewRuntimeBundleManager(bundleA, builder, time.Second)
	closerA := bundleA.ProviderCloser.(*runtimeTestCloser)
	lease := manager.Lease()
	manager.swap(bundleB)
	if closerA.calls.Load() != 0 {
		t.Fatal("retired leased provider closed during pass")
	}
	leased := lease.Bundle()
	if leased.RevisionID != a.ID || leased.CommitFormat != ai.CommitFormatImperative || leased.IntentWindow != 3 || leased.IntentMinPending != 2 {
		t.Fatalf("mixed leased bundle: %+v", leased)
	}
	lease.Release()
	if closerA.calls.Load() != 1 {
		t.Fatalf("old close count=%d", closerA.calls.Load())
	}
	manager.Close()
}

func TestRuntimeBundleUsesFrozenDrainRevisionThenReturnsToDesired(t *testing.T) {
	t.Setenv(ai.EnvAPIKey, "test-only-key")
	t.Setenv(ai.EnvBaseURL, ai.DefaultOpenAIBaseURL)
	t.Setenv(ai.EnvDiffEgress, "false")
	ctx := context.Background()
	db := openTestDB(t)
	builder := runtimeBuilder(db, map[string]*runtimeTestCloser{})
	intentRevision := runtimeRevision(t, db, "intent", 1, map[string]any{
		config.FieldProvider:       "openai-compat",
		config.FieldModel:          "semantic-model",
		config.FieldCommitStrategy: "intent",
		config.FieldCommitFormat:   "imperative",
	})
	eventRevision := runtimeRevision(t, db, "event", 2, map[string]any{
		config.FieldProvider:       "deterministic",
		config.FieldCommitStrategy: "event",
		config.FieldCommitFormat:   "imperative",
	})
	activate := func(revisionID int64, expected sql.NullInt64) {
		t.Helper()
		request, ok, err := state.RequestConfigActivation(
			ctx, db, revisionID, expected)
		if err != nil || !ok {
			t.Fatalf("request revision %d=(%t,%v)", revisionID, ok, err)
		}
		if ok, err := state.AcknowledgeConfigActivation(
			ctx, db, request.ID, revisionID); err != nil || !ok {
			t.Fatalf("ack revision %d=(%t,%v)", revisionID, ok, err)
		}
		if ok, err := state.ApplyConfigActivation(
			ctx, db, request.ID, revisionID); err != nil || !ok {
			t.Fatalf("apply revision %d=(%t,%v)", revisionID, ok, err)
		}
	}
	activate(intentRevision.ID, sql.NullInt64{})
	activate(eventRevision.ID, sql.NullInt64{
		Int64: intentRevision.ID, Valid: true,
	})
	intentBundle, err := builder.BuildRevision(ctx, intentRevision, nil)
	if err != nil {
		t.Fatal(err)
	}
	eventBundle, err := builder.BuildRevision(ctx, eventRevision, intentBundle)
	if err != nil {
		t.Fatal(err)
	}
	drain := state.PublicationDrain{
		ID: "frozen-runtime", CommitStrategy: string(intentBundle.CommitStrategy),
		CommitFormat:        string(intentBundle.CommitFormat),
		ConfigRevisionID:    intentBundle.RevisionID,
		Provider:            intentBundle.HealthIdentity.Provider,
		ProviderModel:       intentBundle.Model,
		ProviderFingerprint: intentBundle.HealthFingerprint,
	}
	manager := NewRuntimeBundleManager(eventBundle, builder, time.Second)
	defer manager.Close()
	if err := manager.ActivatePublicationDrainRevision(ctx, drain); err != nil {
		t.Fatal(err)
	}
	if current := manager.Current(); current.RevisionID != intentRevision.ID ||
		current.CommitStrategy != ai.CommitStrategyIntent ||
		publicationDrainRuntimeBlock(drain, current) != "" {
		t.Fatalf("frozen drain bundle=%+v", current)
	}
	projection, err := state.RuntimeConfigActivationState(ctx, db)
	if err != nil || projection.AppliedRevisionID.Int64 != eventRevision.ID {
		t.Fatalf("desired projection changed=%+v err=%v", projection, err)
	}
	// Reconciliation removed the frozen drain before this pass leased its
	// runtime. The refresh must restore desired Event semantics immediately.
	if err := refreshPublicationDrainRuntimeAfterReconcile(
		ctx, db, manager, "refs/heads/main", 7, drain.ID,
	); err != nil {
		t.Fatal(err)
	}
	if current := manager.Current(); current.RevisionID != eventRevision.ID ||
		current.CommitStrategy != ai.CommitStrategyEvent {
		t.Fatalf("post-drain desired bundle=%+v", current)
	}
}

func TestRuntimeBundleRejectsFrozenRevisionMismatchBeforeProviderBuild(
	t *testing.T,
) {
	ctx := context.Background()
	db := openTestDB(t)
	revision := runtimeRevision(t, db, "intent", 1, map[string]any{
		config.FieldProvider:       "openai-compat",
		config.FieldModel:          "semantic-model",
		config.FieldCommitStrategy: "intent",
		config.FieldCommitFormat:   "imperative",
	})
	var builds atomic.Int32
	builder := RuntimeBundleBuilder{DB: db, BuildProvider: func(
		ai.ProviderConfig,
	) (ai.Provider, io.Closer, error) {
		builds.Add(1)
		return &runtimeTestProvider{name: "openai-compat"}, nil, nil
	}}
	initial := &RuntimeBundle{
		RevisionID: 99, CommitStrategy: ai.CommitStrategyEvent,
		CommitFormat: ai.CommitFormatImperative,
		Provider:     &runtimeTestProvider{name: "deterministic"},
	}
	manager := NewRuntimeBundleManager(initial, builder, time.Second)
	defer manager.Close()
	drain := state.PublicationDrain{
		ConfigRevisionID: revision.ID, CommitStrategy: "intent",
		CommitFormat: "imperative", Provider: "openai-compat",
		ProviderModel: "different-model",
	}
	for i := 0; i < 2; i++ {
		if err := manager.ActivatePublicationDrainRevision(ctx, drain); err == nil {
			t.Fatal("mismatched frozen contract unexpectedly activated")
		}
	}
	if builds.Load() != 0 || manager.Current() != initial {
		t.Fatalf("provider builds=%d current=%+v", builds.Load(), manager.Current())
	}
}

func TestRuntimeBundleDoesNotRebuildMatchingBlockedDrainProvider(t *testing.T) {
	db := openTestDB(t)
	var builds atomic.Int32
	bundle := &RuntimeBundle{
		RevisionID: 7, CommitStrategy: ai.CommitStrategyIntent,
		CommitFormat: ai.CommitFormatImperative,
		Provider:     &runtimeTestProvider{name: "openai-compat"},
		HealthIdentity: IntentPlannerProviderIdentity{
			Provider: "openai-compat", Model: "semantic-model",
		},
		Model: "semantic-model", HealthFingerprint: "sha256:" +
			strings.Repeat("b", 64),
		ReplayBlockedReason: "provider temporarily unavailable",
	}
	drain := state.PublicationDrain{
		ConfigRevisionID: bundle.RevisionID, CommitStrategy: "intent",
		CommitFormat: "imperative", Provider: "openai-compat",
		ProviderModel:       bundle.Model,
		ProviderFingerprint: bundle.HealthFingerprint,
	}
	manager := NewRuntimeBundleManager(bundle, RuntimeBundleBuilder{
		DB: db, BuildProvider: func(ai.ProviderConfig) (
			ai.Provider, io.Closer, error,
		) {
			builds.Add(1)
			return &runtimeTestProvider{name: "openai-compat"}, nil, nil
		},
	}, time.Second)
	defer manager.Close()
	for i := 0; i < 2; i++ {
		if err := manager.ActivatePublicationDrainRevision(
			context.Background(), drain); err != nil {
			t.Fatal(err)
		}
	}
	if builds.Load() != 0 || manager.Current() != bundle {
		t.Fatalf("provider builds=%d current=%+v", builds.Load(), manager.Current())
	}
}

func TestRuntimeStatusMetaUsesActiveBundle(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)
	if err := state.MetaSetMany(ctx, db, map[string]string{
		"ai.provider":            "deterministic",
		"ai.model":               "stale-model",
		"ai.timeout":             "5m0s",
		"commit.strategy":        "event",
		"commit.format":          "conventional",
		"intent.window":          "10",
		"intent.min_pending":     "10",
		"intent.settle_window":   "10s",
		"intent.max_pending_age": "5m0s",
		"intent.recent_commits":  "2",
		"intent.defer_limit":     "1",
		"intent.diff_egress":     "false",
	}); err != nil {
		t.Fatal(err)
	}
	bundle := &RuntimeBundle{
		Provider:            &runtimeTestProvider{name: "composed-provider"},
		HealthIdentity:      IntentPlannerProviderIdentity{Provider: "openai-compat"},
		Model:               "gpt-test",
		ProviderTimeout:     45 * time.Second,
		CommitStrategy:      ai.CommitStrategyIntent,
		CommitFormat:        ai.CommitFormatImperative,
		IntentWindow:        20,
		IntentMinPending:    18,
		IntentSettleWindow:  30 * time.Second,
		IntentMaxPendingAge: 3 * time.Minute,
		IntentRecentCommits: 5,
		IntentDeferLimit:    2,
		DiffEgress:          true,
	}
	if err := stampRuntimeStatusMeta(ctx, db, bundle); err != nil {
		t.Fatal(err)
	}
	want := map[string]string{
		"ai.provider":            "openai-compat",
		"ai.model":               "gpt-test",
		"ai.timeout":             "45s",
		"commit.strategy":        "intent",
		"commit.format":          "imperative",
		"intent.window":          "20",
		"intent.min_pending":     "18",
		"intent.settle_window":   "30s",
		"intent.max_pending_age": "3m0s",
		"intent.recent_commits":  "5",
		"intent.defer_limit":     "2",
		"intent.diff_egress":     "true",
	}
	for key, value := range want {
		got, ok, err := state.MetaGet(ctx, db, key)
		if err != nil || !ok || got != value {
			t.Fatalf("%s=%q ok=%t err=%v want %q", key, got, ok, err, value)
		}
	}
	blocked := *bundle
	blocked.RevisionID = 2
	blocked.HealthIdentity.Provider = "deterministic"
	blocked.Provider = &runtimeTestProvider{name: "deterministic"}
	blocked.Model = ""
	blocked.CommitStrategy = ai.CommitStrategyEvent
	blocked.ReplayBlockedReason = "provider unavailable"
	if err := stampRuntimeStatusMeta(ctx, db, &blocked); err != nil {
		t.Fatal(err)
	}
	ready, ok, err := state.MetaGet(ctx, db, "publication.runtime.ready")
	if err != nil || !ok || ready != "false" {
		t.Fatalf("blocked publication runtime ready=%q ok=%t err=%v", ready, ok, err)
	}
	provider, ok, err := state.MetaGet(ctx, db, "publication.runtime.provider")
	if err != nil || !ok || provider != "openai-compat" {
		t.Fatalf("immutable prior tuple provider=%q ok=%t err=%v", provider, ok, err)
	}
}

func TestRuntimeBundleAllowsApprovedLocalSubprocessDiffContext(t *testing.T) {
	t.Setenv(ai.EnvDiffEgress, "false")
	db := openTestDB(t)
	body, err := json.Marshal(map[string]any{
		config.FieldProvider:       "subprocess:local-planner",
		config.FieldCommitStrategy: "intent",
		config.FieldCommitPreset:   "fast",
		config.FieldDiffEgress:     false,
		"preset_id":                "intent.fast",
		"preset_version":           config.PresetCatalogVersion,
		"customized":               true,
		"confirmations": []string{
			string(ai.ConfirmationSubprocessExecution),
			string(ai.ConfirmationDiffEgress),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	revision, err := state.InsertConfigRevision(context.Background(), db,
		state.ConfigRevisionInput{
			Snapshot: body, Profile: "local", Scope: "repo",
			SourceGeneration: 1,
		})
	if err != nil {
		t.Fatal(err)
	}
	bundle, err := runtimeBuilder(db, map[string]*runtimeTestCloser{}).
		BuildRevision(context.Background(), revision, nil)
	if err != nil {
		t.Fatal(err)
	}
	if bundle.ReplayBlockedReason != "" || !bundle.IntentIncludeDiffs ||
		bundle.DiffEgress {
		t.Fatalf("local subprocess diff policy=%+v", bundle)
	}
}

func TestRuntimeBundleProviderFailureUsesPresetPolicy(t *testing.T) {
	db := openTestDB(t)
	for _, tc := range []struct {
		name             string
		preset           config.PresetName
		verificationMode string
		commandField     string
		wantBlocked      bool
		wantVerification bool
	}{
		{
			name:   "fast falls back to hard dependency planning",
			preset: config.PresetFast, verificationMode: "none",
		},
		{
			name:   "balanced falls back through approved verification",
			preset: config.PresetBalanced, verificationMode: "fast",
			commandField:     config.FieldVerificationFastCommand,
			wantVerification: true,
		},
		{
			name:   "quality remains fail closed",
			preset: config.PresetQuality, verificationMode: "full",
			commandField: config.FieldVerificationFullCommand,
			wantBlocked:  true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			values := map[string]any{
				config.FieldProvider:            "subprocess:unavailable",
				config.FieldCommitStrategy:      "intent",
				config.FieldCommitPreset:        string(tc.preset),
				config.FieldDiffEgress:          false,
				config.FieldIntentVerification:  tc.verificationMode,
				config.FieldIntentRepairEnabled: false,
				"preset_id":                     "intent." + string(tc.preset),
				"preset_version":                config.PresetCatalogVersion,
				"customized":                    true,
				"confirmations": []string{
					string(ai.ConfirmationSubprocessExecution),
					string(ai.ConfirmationDiffEgress),
				},
			}
			if tc.commandField != "" {
				values[tc.commandField] = "true"
				if tc.verificationMode == "full" {
					values[config.FieldVerificationFullTimeout] = "10m"
				} else {
					values[config.FieldVerificationFastTimeout] = "2m"
				}
				values["confirmations"] = append(
					values["confirmations"].([]string),
					string(ai.ConfirmationVerificationCommand))
			}
			body, err := json.Marshal(values)
			if err != nil {
				t.Fatal(err)
			}
			revision, err := state.InsertConfigRevision(context.Background(), db,
				state.ConfigRevisionInput{
					Snapshot: body, Profile: string(tc.preset), Scope: "repo",
					SourceGeneration: 1,
				})
			if err != nil {
				t.Fatal(err)
			}
			builder := RuntimeBundleBuilder{
				DB: db, RepoRoot: t.TempDir(),
				BuildProvider: func(ai.ProviderConfig) (ai.Provider, io.Closer, error) {
					return nil, nil, errors.New("provider unavailable")
				},
			}
			bundle, err := builder.BuildRevision(
				context.Background(), revision, nil)
			if err != nil {
				t.Fatal(err)
			}
			if (bundle.ReplayBlockedReason != "") != tc.wantBlocked {
				t.Fatalf("blocked=%q wantBlocked=%t",
					bundle.ReplayBlockedReason, tc.wantBlocked)
			}
			if _, ok := bundle.IntentPlanner.(unavailableIntentPlanner); !ok {
				t.Fatalf("planner=%T, want semantic provider wait",
					bundle.IntentPlanner)
			}
			if bundle.HealthIdentity.Provider != "subprocess:unavailable" ||
				bundle.HealthIdentity.Deterministic {
				t.Fatalf("provider identity=%+v", bundle.HealthIdentity)
			}
			if bundle.IntentVerificationReady != tc.wantVerification {
				t.Fatalf("verificationReady=%t want=%t",
					bundle.IntentVerificationReady, tc.wantVerification)
			}
			if tc.wantBlocked &&
				!strings.Contains(bundle.ReplayBlockedReason, "provider is unavailable") {
				t.Fatalf("blocked reason=%q", bundle.ReplayBlockedReason)
			}
		})
	}
}

func TestRuntimeConfigProviderReloadConvergesABCAndRetainsKnownGood(t *testing.T) {
	t.Setenv(ai.EnvAPIKey, "test-only-key")
	t.Setenv(ai.EnvBaseURL, ai.DefaultOpenAIBaseURL)
	t.Setenv(ai.EnvDiffEgress, "false")
	db := openTestDB(t)
	closers := map[string]*runtimeTestCloser{}
	builder := runtimeBuilder(db, closers)
	initialCloser := &runtimeTestCloser{}
	initial := &RuntimeBundle{Provider: &runtimeTestProvider{name: "deterministic"}, ProviderCloser: initialCloser, MessageFn: DeterministicMessage}
	manager := NewRuntimeBundleManager(initial, builder, time.Second)
	defer manager.Close()
	var expected sql.NullInt64
	var latest state.ConfigRevision
	for i, model := range []string{"model-a", "model-b", "model-c"} {
		revision := runtimeRevision(t, db, "work", int64(i+1), map[string]any{"ai.provider": "openai-compat", "ai.model": model, "commit.strategy": "intent", "intent.window": i + 3})
		request, ok, err := state.RequestConfigActivation(context.Background(), db, revision.ID, expected)
		if err != nil || !ok {
			t.Fatalf("request %s: %+v %v", model, request, err)
		}
		expected = sql.NullInt64{Int64: revision.ID, Valid: true}
		latest = revision
	}
	if err := manager.ActivateDesired(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := manager.Current(); got.RevisionID != latest.ID || got.Model != "model-c" || got.IntentWindow != 5 {
		t.Fatalf("active=%+v", got)
	}
	projection, err := state.RuntimeConfigActivationState(context.Background(), db)
	if err != nil || projection.AppliedRevisionID.Int64 != latest.ID || projection.LastKnownGoodRevisionID.Int64 != latest.ID {
		t.Fatalf("projection=%+v err=%v", projection, err)
	}
	if initialCloser.calls.Load() != 1 {
		t.Fatalf("initial close count=%d", initialCloser.calls.Load())
	}

	bad := runtimeRevision(t, db, "work", 4, map[string]any{"capture.max_file_bytes": 1234})
	request, ok, err := state.RequestConfigActivation(context.Background(), db, bad.ID, expected)
	if err != nil || !ok {
		t.Fatal(err)
	}
	if err := manager.ActivateDesired(context.Background()); err == nil {
		t.Fatal("restart-only candidate applied")
	}
	projection, _ = state.RuntimeConfigActivationState(context.Background(), db)
	if projection.AppliedRevisionID.Int64 != latest.ID || projection.LastKnownGoodRevisionID.Int64 != latest.ID {
		t.Fatalf("known-good changed: %+v", projection)
	}
	gotRequest, _ := state.ActivationRequestByID(context.Background(), db, request.ID)
	if gotRequest.Status != state.ActivationRejected {
		t.Fatalf("request=%+v", gotRequest)
	}

	restarted := NewRuntimeBundleManager(
		&RuntimeBundle{Provider: &runtimeTestProvider{name: "deterministic"}, MessageFn: DeterministicMessage},
		builder,
		time.Second,
	)
	defer restarted.Close()
	if err := restarted.ActivateDesired(context.Background()); err != nil {
		t.Fatalf("restart after rejected desired: %v", err)
	}
	if got := restarted.Current(); got.RevisionID != latest.ID || got.Model != "model-c" {
		t.Fatalf("restart bundle=%+v want last-known-good revision %d", got, latest.ID)
	}
}

func TestFirstEventRuntimeActivationFailureKeepsReplayBlocked(t *testing.T) {
	t.Setenv(ai.EnvAPIKey, "test-only-key")
	t.Setenv(ai.EnvDiffEgress, "false")
	db := openTestDB(t)
	revision := runtimeRevision(t, db, "global", 1, map[string]any{
		config.FieldProvider:       "openai-compat",
		config.FieldBaseURL:        "https://repo-override.example/v1",
		config.FieldModel:          "repo-model",
		config.FieldCommitStrategy: "event",
	})
	request, ok, err := state.RequestConfigActivation(
		context.Background(), db, revision.ID, sql.NullInt64{})
	if err != nil || !ok {
		t.Fatalf("request activation: ok=%v err=%v", ok, err)
	}
	blockedReason := configuredRuntimeReplayBlock(context.Background(), db)
	if blockedReason == "" {
		t.Fatal("persisted desired revision did not block startup replay")
	}
	initial := &RuntimeBundle{
		Provider:            &runtimeTestProvider{name: "deterministic"},
		MessageFn:           DeterministicMessage,
		CommitStrategy:      ai.CommitStrategyEvent,
		PresetID:            "event.fast",
		PresetVersion:       config.PresetCatalogVersion,
		ReplayBlockedReason: blockedReason,
	}
	manager := NewRuntimeBundleManager(
		initial, runtimeBuilder(db, map[string]*runtimeTestCloser{}),
		time.Second,
	)
	defer manager.Close()
	if err := manager.ActivateDesired(context.Background()); err == nil {
		t.Fatal("drifted Event activation unexpectedly succeeded")
	}
	current := manager.Current()
	if current.RevisionID != 0 || current.ReplayBlockedReason == "" {
		t.Fatalf("unsafe startup fallback became publishable: %+v", current)
	}
	gotRequest, err := state.ActivationRequestByID(
		context.Background(), db, request.ID)
	if err != nil {
		t.Fatal(err)
	}
	if gotRequest.Status != state.ActivationRejected {
		t.Fatalf("activation request status=%q", gotRequest.Status)
	}
}

func TestRuntimeConfigRejectsFailedExperimentAndQueuesBaseline(t *testing.T) {
	t.Setenv(ai.EnvAPIKey, "test-only-key")
	t.Setenv(ai.EnvBaseURL, ai.DefaultOpenAIBaseURL)
	t.Setenv(ai.EnvDiffEgress, "false")
	db := openTestDB(t)
	closers := map[string]*runtimeTestCloser{}
	builder := runtimeBuilder(db, closers)
	baseline := runtimeRevision(t, db, "baseline", 1, map[string]any{
		"ai.provider": "deterministic", "commit.strategy": "intent",
	})
	baselineRequest, ok, err := state.RequestConfigActivation(
		context.Background(), db, baseline.ID, sql.NullInt64{},
	)
	if err != nil || !ok {
		t.Fatalf("baseline request: ok=%v err=%v", ok, err)
	}
	_, _ = state.AcknowledgeConfigActivation(context.Background(), db, baselineRequest.ID, baseline.ID)
	_, _ = state.ApplyConfigActivation(context.Background(), db, baselineRequest.ID, baseline.ID)
	baselineBundle, err := builder.BuildRevision(context.Background(), baseline, nil)
	if err != nil {
		t.Fatal(err)
	}
	candidate := runtimeRevision(t, db, "candidate", 2, map[string]any{
		"capture.max_file_bytes": 1234,
	})
	experiment, request, ok, err := state.RequestConfigExperimentActivation(
		context.Background(),
		db,
		state.ConfigExperimentInput{
			BaselineRevisionID:  baseline.ID,
			CandidateRevisionID: candidate.ID,
			WindowBudget:        10,
			FailurePolicy:       "revert",
		},
		sql.NullInt64{Int64: baseline.ID, Valid: true},
	)
	if err != nil || !ok {
		t.Fatalf("experiment request: ok=%v err=%v", ok, err)
	}
	manager := NewRuntimeBundleManager(baselineBundle, builder, time.Second)
	defer manager.Close()
	if err := manager.ActivateDesired(context.Background()); err == nil {
		t.Fatal("invalid experiment candidate activated")
	}
	gotExperiment, err := state.ConfigExperimentByID(context.Background(), db, experiment.ID)
	if err != nil || gotExperiment.Status != state.ExperimentFailed {
		t.Fatalf("experiment=%+v err=%v", gotExperiment, err)
	}
	gotRequest, err := state.ActivationRequestByID(context.Background(), db, request.ID)
	if err != nil || gotRequest.Status != state.ActivationRejected {
		t.Fatalf("request=%+v err=%v", gotRequest, err)
	}
	projection, err := state.RuntimeConfigActivationState(context.Background(), db)
	if err != nil || !projection.DesiredRevisionID.Valid ||
		projection.DesiredRevisionID.Int64 == candidate.ID {
		t.Fatalf("baseline revert was not queued: projection=%+v err=%v", projection, err)
	}
	if err := manager.ActivateDesired(context.Background()); err != nil {
		t.Fatalf("activate queued baseline revert: %v", err)
	}
	if manager.Current().RevisionID != projection.DesiredRevisionID.Int64 {
		t.Fatalf("bundle=%+v projection=%+v", manager.Current(), projection)
	}
}

func TestRuntimeConfigExpiresExperimentWithoutCurrentCandidateBundle(t *testing.T) {
	db := openTestDB(t)
	baseline := runtimeRevision(t, db, "baseline", 1, map[string]any{
		"ai.provider": "deterministic", "commit.strategy": "intent",
	})
	request, ok, err := state.RequestConfigActivation(
		context.Background(), db, baseline.ID, sql.NullInt64{},
	)
	if err != nil || !ok {
		t.Fatal(err)
	}
	_, _ = state.AcknowledgeConfigActivation(context.Background(), db, request.ID, baseline.ID)
	_, _ = state.ApplyConfigActivation(context.Background(), db, request.ID, baseline.ID)
	candidate := runtimeRevision(t, db, "candidate", 2, map[string]any{
		"ai.provider": "deterministic", "commit.strategy": "intent",
	})
	_, _, ok, err = state.RequestConfigExperimentActivation(
		context.Background(),
		db,
		state.ConfigExperimentInput{
			BaselineRevisionID:  baseline.ID,
			CandidateRevisionID: candidate.ID,
			WindowBudget:        10,
			ExpiresTS:           sql.NullFloat64{Float64: 10, Valid: true},
		},
		sql.NullInt64{Int64: baseline.ID, Valid: true},
	)
	if err != nil || !ok {
		t.Fatalf("experiment request: ok=%v err=%v", ok, err)
	}
	manager := NewRuntimeBundleManager(
		&RuntimeBundle{RevisionID: baseline.ID, Provider: &runtimeTestProvider{name: "deterministic"}},
		runtimeBuilder(db, map[string]*runtimeTestCloser{}),
		time.Second,
	)
	defer manager.Close()
	queued, err := manager.QueueExperimentRevert(context.Background(), time.Unix(20, 0))
	if err != nil || !queued {
		t.Fatalf("queued=%v err=%v", queued, err)
	}
}

func TestRuntimeConfigQueuesFailedExperimentWithoutCurrentCandidateBundle(t *testing.T) {
	db := openTestDB(t)
	baseline := runtimeRevision(t, db, "baseline", 1, map[string]any{
		"ai.provider": "deterministic", "commit.strategy": "intent",
	})
	request, ok, err := state.RequestConfigActivation(
		context.Background(), db, baseline.ID, sql.NullInt64{},
	)
	if err != nil || !ok {
		t.Fatal(err)
	}
	_, _ = state.AcknowledgeConfigActivation(context.Background(), db, request.ID, baseline.ID)
	_, _ = state.ApplyConfigActivation(context.Background(), db, request.ID, baseline.ID)
	candidate := runtimeRevision(t, db, "candidate", 2, map[string]any{
		"ai.provider": "deterministic", "commit.strategy": "intent",
	})
	experiment, _, ok, err := state.RequestConfigExperimentActivation(
		context.Background(),
		db,
		state.ConfigExperimentInput{
			BaselineRevisionID:  baseline.ID,
			CandidateRevisionID: candidate.ID,
			WindowBudget:        10,
		},
		sql.NullInt64{Int64: baseline.ID, Valid: true},
	)
	if err != nil || !ok {
		t.Fatalf("experiment request: ok=%v err=%v", ok, err)
	}
	if updated, err := state.FinishConfigExperiment(
		context.Background(), db, experiment.ID, state.ExperimentFailed, "provider failed",
	); err != nil || !updated {
		t.Fatalf("finish experiment: updated=%v err=%v", updated, err)
	}
	manager := NewRuntimeBundleManager(
		&RuntimeBundle{RevisionID: baseline.ID, Provider: &runtimeTestProvider{name: "deterministic"}},
		runtimeBuilder(db, map[string]*runtimeTestCloser{}),
		time.Second,
	)
	defer manager.Close()
	queued, err := manager.QueueExperimentRevert(context.Background(), time.Unix(20, 0))
	if err != nil || !queued {
		t.Fatalf("queued=%v err=%v", queued, err)
	}
}

func TestRuntimeConfigRestartRecoversAcknowledgedDesired(t *testing.T) {
	t.Setenv(ai.EnvAPIKey, "test-only-key")
	t.Setenv(ai.EnvBaseURL, ai.DefaultOpenAIBaseURL)
	t.Setenv(ai.EnvDiffEgress, "false")
	db := openTestDB(t)
	closers := map[string]*runtimeTestCloser{}
	revision := runtimeRevision(t, db, "restart", 1, map[string]any{"ai.provider": "openai-compat", "ai.model": "restart-model", "commit.strategy": "intent"})
	request, ok, err := state.RequestConfigActivation(context.Background(), db, revision.ID, sql.NullInt64{})
	if err != nil || !ok {
		t.Fatal(err)
	}
	if ok, err := state.AcknowledgeConfigActivation(context.Background(), db, request.ID, revision.ID); err != nil || !ok {
		t.Fatalf("ack=%v err=%v", ok, err)
	}
	manager := NewRuntimeBundleManager(&RuntimeBundle{Provider: &runtimeTestProvider{name: "deterministic"}, MessageFn: DeterministicMessage}, runtimeBuilder(db, closers), time.Second)
	defer manager.Close()
	if err := manager.ActivateDesired(context.Background()); err != nil {
		t.Fatal(err)
	}
	projection, _ := state.RuntimeConfigActivationState(context.Background(), db)
	if manager.Current().RevisionID != revision.ID || projection.AppliedRevisionID.Int64 != revision.ID {
		t.Fatalf("bundle=%+v projection=%+v", manager.Current(), projection)
	}
}

func TestRuntimeConfigFinalizesRequestWhenBundleAlreadyMatches(t *testing.T) {
	t.Setenv(ai.EnvAPIKey, "test-only-key")
	db := openTestDB(t)
	closers := map[string]*runtimeTestCloser{}
	revision := runtimeRevision(t, db, "current", 1, map[string]any{
		"ai.provider": "deterministic", "commit.strategy": "event",
	})
	request, ok, err := state.RequestConfigActivation(context.Background(), db, revision.ID, sql.NullInt64{})
	if err != nil || !ok {
		t.Fatalf("request=%+v ok=%v err=%v", request, ok, err)
	}
	if ok, err := state.AcknowledgeConfigActivation(context.Background(), db, request.ID, revision.ID); err != nil || !ok {
		t.Fatalf("ack=%v err=%v", ok, err)
	}
	manager := NewRuntimeBundleManager(&RuntimeBundle{
		RevisionID: revision.ID, Provider: &runtimeTestProvider{name: "deterministic"}, MessageFn: DeterministicMessage,
	}, runtimeBuilder(db, closers), time.Second)
	defer manager.Close()

	if err := manager.ActivateDesired(context.Background()); err != nil {
		t.Fatal(err)
	}
	projection, err := state.RuntimeConfigActivationState(context.Background(), db)
	if err != nil || projection.AppliedRevisionID.Int64 != revision.ID {
		t.Fatalf("projection=%+v err=%v", projection, err)
	}
	got, err := state.ActivationRequestByID(context.Background(), db, request.ID)
	if err != nil || got.Status != state.ActivationApplied {
		t.Fatalf("request=%+v err=%v", got, err)
	}
}

func TestRuntimeConfigRequiresPrivacyConfirmations(t *testing.T) {
	t.Setenv(ai.EnvAPIKey, "test-only-key")
	db := openTestDB(t)
	closers := map[string]*runtimeTestCloser{}
	revision := runtimeRevision(t, db, "privacy", 1, map[string]any{"ai.provider": "openai-compat", "ai.base_url": "https://gateway.example/v1", "ai.model": "model", "ai.diff_egress": true})
	_, err := runtimeBuilder(db, closers).BuildRevision(context.Background(), revision, nil)
	if err == nil {
		t.Fatal("unconfirmed privacy effects accepted")
	}
	confirmed := runtimeRevision(t, db, "privacy", 2, map[string]any{"ai.provider": "openai-compat", "ai.base_url": "https://gateway.example/v1", "ai.model": "model", "ai.diff_egress": true, "confirmations": []string{"endpoint_credentials", "diff_egress"}})
	if _, err := runtimeBuilder(db, closers).BuildRevision(context.Background(), confirmed, nil); err != nil {
		t.Fatal(err)
	}
}

func TestRuntimeBundleRetiredProviderCloseIsBounded(t *testing.T) {
	block := make(chan struct{})
	closer := &runtimeTestCloser{block: block}
	manager := NewRuntimeBundleManager(&RuntimeBundle{Provider: &runtimeTestProvider{name: "a"}, ProviderCloser: closer}, RuntimeBundleBuilder{}, 20*time.Millisecond)
	started := time.Now()
	manager.swap(&RuntimeBundle{Provider: &runtimeTestProvider{name: "b"}})
	if time.Since(started) > time.Second {
		t.Fatalf("bounded close took %v", time.Since(started))
	}
	if closer.calls.Load() != 1 {
		t.Fatalf("close count=%d", closer.calls.Load())
	}
	close(block)
	manager.Close()
}

func TestConfigRevisionStampsDecisionsAndPlannerWindows(t *testing.T) {
	db := openTestDB(t)
	revision := runtimeRevision(t, db, "candidate", 1, map[string]any{"ai.provider": "deterministic"})
	ctx := withRuntimeTelemetry(context.Background(), &RuntimeBundle{
		RevisionID:    revision.ID,
		Profile:       revision.Profile,
		PresetID:      "intent.balanced",
		PresetVersion: config.PresetCatalogVersion,
	})
	telemetry := runtimeTelemetryFromContext(ctx)
	if telemetry.presetID != "intent.balanced" ||
		telemetry.presetVersion != config.PresetCatalogVersion {
		t.Fatalf("runtime preset telemetry=%+v", telemetry)
	}
	cctx := CaptureContext{BranchRef: "refs/heads/main", BranchGeneration: 3, BaseHead: "abc123"}
	ev := state.CaptureEvent{
		BranchRef:        cctx.BranchRef,
		BranchGeneration: cctx.BranchGeneration,
		BaseHead:         cctx.BaseHead,
		Operation:        "modify",
		Path:             "src/app.go",
		Fidelity:         "exact",
	}
	seq, err := state.AppendCaptureEvent(ctx, db, ev, []state.CaptureOp{{Op: "modify", Path: ev.Path, Fidelity: ev.Fidelity}})
	if err != nil {
		t.Fatal(err)
	}
	ev.Seq = seq
	recordCapturedDecision(ctx, db, cctx, seq, ClassifiedOp{Op: ev.Operation, Path: ev.Path, Fidelity: ev.Fidelity})
	recordReplayDecision(ctx, db, ev, cctx, float64(time.Now().UnixNano())/float64(time.Second), state.DecisionKindCommitted, "event", "deadbeef")

	decisions, err := state.DecisionsForEvent(ctx, db, seq, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(decisions) != 2 {
		t.Fatalf("decisions=%+v", decisions)
	}
	for _, decision := range decisions {
		if !decision.ConfigRevisionID.Valid || decision.ConfigRevisionID.Int64 != revision.ID ||
			!decision.ConfigProfile.Valid || decision.ConfigProfile.String != "candidate" {
			t.Fatalf("unstamped decision: %+v", decision)
		}
	}

	req := ai.IntentPlanRequest{
		OfferedCaptures: []ai.OfferedCapture{{Seq: seq, Path: ev.Path, Op: ev.Operation, Fidelity: ev.Fidelity}},
		CommitFormat:    ai.CommitFormatImperative,
	}
	plan := ai.IntentPlan{SelectedSeqs: []int64{seq}, Subject: "Update app behavior", GroupingReason: "one change", Source: "openai-compat"}
	cfg := intentReplayConfig{plannerProvider: "openai-compat", plannerModel: "synthetic", retryCount: 2}
	if err := recordIntentPlannerWindow(ctx, db, cfg, req, plan, []intentReplayItem{{event: ev}}, cctx,
		float64(time.Now().Add(-5*time.Millisecond).UnixNano())/float64(time.Second), false, ""); err != nil {
		t.Fatal(err)
	}
	windows, err := state.RecentIntentPlannerWindows(ctx, db, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(windows) != 1 {
		t.Fatalf("windows=%+v", windows)
	}
	window := windows[0]
	if !window.ConfigRevisionID.Valid || window.ConfigRevisionID.Int64 != revision.ID ||
		!window.ConfigProfile.Valid || window.ConfigProfile.String != "candidate" ||
		!window.DurationMS.Valid || window.DurationMS.Int64 < 0 || window.RetryCount != 2 ||
		window.FallbackUsed || !window.Outcome.Valid || window.Outcome.String != "selected" {
		t.Fatalf("planner telemetry=%+v", window)
	}
}

func TestConfigRevisionPlannerFallbackTelemetryIsDescriptive(t *testing.T) {
	db := openTestDB(t)
	revision := runtimeRevision(t, db, "fallback", 1, map[string]any{"ai.provider": "deterministic"})
	ctx := withRuntimeTelemetry(context.Background(), &RuntimeBundle{RevisionID: revision.ID, Profile: revision.Profile})
	cctx := CaptureContext{BranchRef: "refs/heads/main", BranchGeneration: 1}
	ev := state.CaptureEvent{Seq: 41, BranchRef: cctx.BranchRef, BranchGeneration: 1, Operation: "modify", Path: "safe.txt", Fidelity: "exact"}
	req := ai.IntentPlanRequest{OfferedCaptures: []ai.OfferedCapture{{Seq: ev.Seq, Path: ev.Path, Op: ev.Operation}}, CommitFormat: ai.CommitFormatImperative}
	plan := ai.IntentPlan{SelectedSeqs: []int64{ev.Seq}, Subject: "Update safe behavior", GroupingReason: "fallback", Source: "deterministic"}
	cfg := intentReplayConfig{plannerProvider: "openai-compat", plannerModel: "synthetic", retryCount: 3}
	if err := recordIntentPlannerWindow(ctx, db, cfg, req, plan, []intentReplayItem{{event: ev}}, cctx,
		float64(time.Now().UnixNano())/float64(time.Second), false, "provider validation failed"); err != nil {
		t.Fatal(err)
	}
	windows, err := state.RecentIntentPlannerWindows(ctx, db, 1)
	if err != nil || len(windows) != 1 {
		t.Fatalf("windows=%+v err=%v", windows, err)
	}
	window := windows[0]
	if !window.FallbackUsed || window.RetryCount != 3 || !window.ValidationFailure.Valid ||
		window.Outcome.String != "provider_error_fallback_selected" {
		t.Fatalf("fallback telemetry=%+v", window)
	}
	if window.ValidationFailure.String == "" || window.ConfigRevisionID.Int64 != revision.ID {
		t.Fatalf("missing safe fallback provenance: %+v", window)
	}
}

func TestCandidateWaitingPlannerWindowRecordsDeferredFallback(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()
	cctx := CaptureContext{
		BranchRef: "refs/heads/main", BranchGeneration: 1, BaseHead: "abc123",
	}
	ev := state.CaptureEvent{
		BranchRef: cctx.BranchRef, BranchGeneration: cctx.BranchGeneration,
		BaseHead: cctx.BaseHead, Operation: "modify", Path: "pending.go",
		Fidelity: "exact",
	}
	seq, err := state.AppendCaptureEvent(ctx, db, ev, []state.CaptureOp{{
		Op: "modify", Path: ev.Path, Fidelity: ev.Fidelity,
	}})
	if err != nil {
		t.Fatal(err)
	}
	ev.Seq = seq
	req := ai.IntentPlanRequest{
		OfferedCaptures: []ai.OfferedCapture{{
			Seq: seq, Path: ev.Path, Op: ev.Operation, Fidelity: ev.Fidelity,
		}},
		CommitFormat: ai.CommitFormatImperative,
	}
	plan := ai.IntentPlan{
		DeferredSeqs: []int64{seq},
		DeferredReasons: []ai.DeferredReason{{
			Seq: seq, Reason: "quality candidate requires attention",
		}},
		Source: ai.IntentPlannerProtocolV2,
	}
	cfg := intentReplayConfig{
		plannerProvider: "openai-compat", plannerModel: "synthetic",
		fallbackUsed: true,
	}
	if err := recordIntentPlannerWindow(
		ctx, db, cfg, req, plan, []intentReplayItem{{event: ev}}, cctx,
		float64(time.Now().UnixNano())/float64(time.Second), false, "",
	); err != nil {
		t.Fatal(err)
	}
	windows, err := state.RecentIntentPlannerWindows(ctx, db, 1)
	if err != nil || len(windows) != 1 {
		t.Fatalf("windows=%+v err=%v", windows, err)
	}
	window := windows[0]
	if !window.FallbackUsed || window.Outcome.String != "deferred" ||
		len(window.SelectedGroups) != 0 ||
		len(window.DeferredSeqs) != 1 || window.DeferredSeqs[0] != seq {
		t.Fatalf("waiting fallback telemetry=%+v", window)
	}
}
