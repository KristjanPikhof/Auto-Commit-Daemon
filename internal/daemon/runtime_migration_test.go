package daemon

import (
	"context"
	"database/sql"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/ai"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/central"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/checkpoint"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/config"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/credentials"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/paths"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

func TestIntentV2CutoverMigratesLegacyIntentOnceAndBlocksUnsafeReplay(t *testing.T) {
	ctx := context.Background()
	repo, dbPath := intentV2MigrationRepo(t)
	db, err := state.Open(ctx, dbPath)
	if err != nil {
		t.Fatal(err)
	}
	legacy := runtimeRevision(t, db, "legacy", 7, map[string]any{
		config.FieldProvider:           "openai-compat",
		config.FieldModel:              "legacy-model",
		config.FieldCommitStrategy:     "intent",
		config.FieldCommitFormat:       "conventional",
		config.FieldIntentWindow:       17,
		config.FieldIntentMinPending:   6,
		config.FieldIntentDeferLimit:   1,
		config.FieldIntentVerification: "none",
	})
	// Strip the helper-added v2 metadata to model a v14 runtime revision.
	var legacyValues map[string]any
	if err := json.Unmarshal([]byte(legacy.SnapshotJSON), &legacyValues); err != nil {
		t.Fatal(err)
	}
	delete(legacyValues, "preset_id")
	delete(legacyValues, "preset_version")
	delete(legacyValues, "customized")
	delete(legacyValues, config.FieldCommitPreset)
	for _, field := range []string{
		config.FieldIntentRepairEnabled,
		config.FieldIntentRepairHorizon,
		config.FieldIntentRepairMaxCommits,
		config.FieldIntentVerification,
		config.FieldVerificationFastCommand,
		config.FieldVerificationFastTimeout,
		config.FieldVerificationFullCommand,
		config.FieldVerificationFullTimeout,
	} {
		delete(legacyValues, field)
	}
	legacyValues[config.FieldDiffEgress] = false
	legacyValues["confirmations"] = []string{}
	body, _ := json.Marshal(legacyValues)
	legacy, err = state.InsertConfigRevision(ctx, db, state.ConfigRevisionInput{
		Snapshot: body, Profile: "legacy", Scope: "repository",
		SourceGeneration: 7, Reason: "legacy v14",
	})
	if err != nil {
		t.Fatal(err)
	}
	request, ok, err := state.RequestConfigActivation(
		ctx, db, legacy.ID, sql.NullInt64{})
	if err != nil || !ok {
		t.Fatalf("request legacy: ok=%v err=%v", ok, err)
	}
	_, _ = state.AcknowledgeConfigActivation(ctx, db, request.ID, legacy.ID)
	_, _ = state.ApplyConfigActivation(ctx, db, request.ID, legacy.ID)
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	downgradeStateVersion(t, dbPath, 14)
	db, err = state.Open(ctx, dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	roots := intentV2MigrationRoots(t)
	result, err := EnsureIntentV2RuntimeCutover(
		ctx, db, repo, roots, func(name string) (string, bool) {
			if name == ai.EnvAPIKey {
				return "test-key", true
			}
			return "", false
		})
	if err != nil {
		t.Fatal(err)
	}
	if !result.Required || !result.Migrated ||
		result.PresetID != "intent.balanced" ||
		result.PresetVersion != config.PresetCatalogVersion {
		t.Fatalf("cutover=%+v", result)
	}
	revision, err := state.ConfigRevisionByID(ctx, db, result.RevisionID)
	if err != nil {
		t.Fatal(err)
	}
	values, confirmations, metadata, err :=
		decodeRuntimeSnapshot(revision.SnapshotJSON)
	if err != nil {
		t.Fatal(err)
	}
	if metadata.PresetID != "intent.balanced" ||
		values[config.FieldIntentWindow] != "17" ||
		values[config.FieldCommitFormat] != "conventional" ||
		values[config.FieldIntentVerification] != "structural" ||
		values[config.FieldIntentRepairEnabled] != "true" {
		t.Fatalf("migrated values=%+v metadata=%+v", values, metadata)
	}
	if _, ok := confirmations[string(ai.ConfirmationIntentRepair)]; !ok {
		t.Fatalf("repair confirmation not materialized: %+v", confirmations)
	}
	if _, ok := confirmations[string(ai.ConfirmationDiffEgress)]; ok {
		t.Fatalf("diff consent was inferred: %+v", confirmations)
	}
	bundle, err := runtimeBuilder(db, map[string]*runtimeTestCloser{}).
		BuildRevision(ctx, revision, nil)
	if err != nil {
		t.Fatal(err)
	}
	if bundle.ReplayBlockedReason == "" ||
		!strings.Contains(bundle.ReplayBlockedReason, "acd configure") ||
		bundle.IntentPlanner == nil {
		t.Fatalf("unsafe migrated bundle=%+v", bundle)
	}
	if _, ok := bundle.IntentPlanner.(unavailableIntentPlanner); !ok {
		t.Fatalf("unsafe migration planner=%T; want unavailable semantic planner",
			bundle.IntentPlanner)
	}

	var before int
	if err := db.ReadSQL().QueryRow(
		`SELECT COUNT(*) FROM config_revisions WHERE reason='Intent v2 cutover'`,
	).Scan(&before); err != nil {
		t.Fatal(err)
	}
	again, err := EnsureIntentV2RuntimeCutover(
		ctx, db, repo, roots, nil)
	if err != nil {
		t.Fatal(err)
	}
	var after int
	_ = db.ReadSQL().QueryRow(
		`SELECT COUNT(*) FROM config_revisions WHERE reason='Intent v2 cutover'`,
	).Scan(&after)
	if before != 1 || after != before || again.Migrated {
		t.Fatalf("non-idempotent cutover before=%d after=%d again=%+v",
			before, after, again)
	}
}

func TestIntentV2CutoverDoesNotFanOutGlobalApprovalToLegacyRuntime(t *testing.T) {
	ctx := context.Background()
	repo, dbPath := intentV2MigrationRepo(t)
	db, err := state.Open(ctx, dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	legacy := runtimeRevision(t, db, "legacy", 7, map[string]any{
		config.FieldProvider:       "openai-compat",
		config.FieldBaseURL:        ai.DefaultOpenAIBaseURL,
		config.FieldModel:          "legacy-model",
		config.FieldCommitStrategy: "intent",
		config.FieldDiffEgress:     true,
		"confirmations": []string{
			string(ai.ConfirmationDiffEgress),
		},
	})
	var legacySnapshot map[string]any
	if err := json.Unmarshal(
		[]byte(legacy.SnapshotJSON), &legacySnapshot,
	); err != nil {
		t.Fatal(err)
	}
	legacySnapshot["preset_version"] = config.PresetCatalogVersion - 1
	legacyBody, err := json.Marshal(legacySnapshot)
	if err != nil {
		t.Fatal(err)
	}
	legacy, err = state.InsertConfigRevision(
		ctx, db, state.ConfigRevisionInput{
			Snapshot: legacyBody, Profile: "legacy", Scope: "repository",
			SourceGeneration: 7, Reason: "legacy preset revision",
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	request, ok, err := state.RequestConfigActivation(
		ctx, db, legacy.ID, sql.NullInt64{})
	if err != nil || !ok {
		t.Fatalf("request legacy activation: ok=%v err=%v", ok, err)
	}
	if _, err := state.AcknowledgeConfigActivation(
		ctx, db, request.ID, legacy.ID,
	); err != nil {
		t.Fatal(err)
	}
	if _, err := state.ApplyConfigActivation(
		ctx, db, request.ID, legacy.ID,
	); err != nil {
		t.Fatal(err)
	}

	roots := intentV2MigrationRoots(t)
	raw := func(value string) json.RawMessage {
		body, marshalErr := json.Marshal(value)
		if marshalErr != nil {
			t.Fatal(marshalErr)
		}
		return body
	}
	global := config.Overrides{
		config.FieldCommitStrategy:      raw("intent"),
		config.FieldCommitPreset:        raw("balanced"),
		config.FieldProvider:            raw("openai-compat"),
		config.FieldBaseURL:             raw("https://global.example/v1"),
		config.FieldModel:               raw("global-model"),
		config.FieldDiffEgress:          raw("true"),
		config.FieldIntentVerification:  raw("structural"),
		config.FieldIntentRepairEnabled: raw("true"),
	}
	resolved, preset, err := config.ResolveAll(
		config.ResolveInput{Global: global}, nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	effective := make(map[string]string, len(resolved))
	for name, field := range resolved {
		effective[name] = field.EffectiveValue()
	}
	fingerprint, err := config.SettingsFingerprint(effective, preset)
	if err != nil {
		t.Fatal(err)
	}
	if err := config.NewStore(roots).Update(func(doc *config.Document) error {
		doc.Settings.Global = global
		doc.Settings.GlobalSetupApproval = &config.GlobalSetupApproval{
			Generation: 1, Fingerprint: fingerprint,
			Confirmations: []string{
				string(ai.ConfirmationDiffEgress),
				string(ai.ConfirmationEndpointCredentials),
				string(ai.ConfirmationIntentRepair),
			},
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	result, err := EnsureIntentV2RuntimeCutover(
		ctx, db, repo, roots, func(name string) (string, bool) {
			if name == ai.EnvAPIKey {
				return "test-key", true
			}
			return "", false
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if !result.Migrated ||
		result.PresetVersion != config.PresetCatalogVersion {
		t.Fatalf("legacy migration=%+v", result)
	}
	revision, err := state.ConfigRevisionByID(ctx, db, result.RevisionID)
	if err != nil {
		t.Fatal(err)
	}
	values, confirmations, _, err :=
		decodeRuntimeSnapshot(revision.SnapshotJSON)
	if err != nil {
		t.Fatal(err)
	}
	if values[config.FieldBaseURL] != ai.DefaultOpenAIBaseURL ||
		values[config.FieldModel] != "legacy-model" {
		t.Fatalf("legacy runtime values were replaced: %+v", values)
	}
	if _, ok := confirmations[string(ai.ConfirmationDiffEgress)]; !ok {
		t.Fatalf("legacy confirmation was lost: %+v", confirmations)
	}
	if _, ok := confirmations[string(ai.ConfirmationEndpointCredentials)]; ok {
		t.Fatalf("global endpoint consent fanned out: %+v", confirmations)
	}
}

func TestIntentV2CutoverPreservesMetaOnlyLegacyIntent(t *testing.T) {
	ctx := context.Background()
	repo, dbPath := intentV2MigrationRepo(t)
	db, err := state.Open(ctx, dbPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := state.MetaSet(ctx, db, "commit.strategy", "intent"); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	downgradeStateVersion(t, dbPath, 14)
	db, err = state.Open(ctx, dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	result, err := EnsureIntentV2RuntimeCutover(
		ctx, db, repo, intentV2MigrationRoots(t),
		func(name string) (string, bool) {
			if name == ai.EnvCommitStrategy {
				return "event", true
			}
			return "", false
		})
	if err != nil {
		t.Fatal(err)
	}
	if !result.Required || !result.Migrated ||
		result.PresetID != "intent.balanced" {
		t.Fatalf("meta-only legacy cutover=%+v", result)
	}
	revision, err := state.ConfigRevisionByID(ctx, db, result.RevisionID)
	if err != nil {
		t.Fatal(err)
	}
	values, _, _, err := decodeRuntimeSnapshot(revision.SnapshotJSON)
	if err != nil {
		t.Fatal(err)
	}
	if values[config.FieldCommitStrategy] != "intent" {
		t.Fatalf("legacy strategy lost: values=%+v", values)
	}
}

func TestSetRuntimeMetaIfChangedDoesNotRewriteEqualValues(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)
	if err := state.MetaSet(ctx, db, "intent.v2.preset_id",
		"intent.balanced"); err != nil {
		t.Fatal(err)
	}
	var before float64
	if err := db.SQL().QueryRowContext(ctx,
		`SELECT updated_ts FROM daemon_meta WHERE key='intent.v2.preset_id'`,
	).Scan(&before); err != nil {
		t.Fatal(err)
	}
	if err := setRuntimeMetaIfChanged(ctx, db, map[string]string{
		"intent.v2.preset_id": "intent.balanced",
	}); err != nil {
		t.Fatal(err)
	}
	var after float64
	if err := db.SQL().QueryRowContext(ctx,
		`SELECT updated_ts FROM daemon_meta WHERE key='intent.v2.preset_id'`,
	).Scan(&after); err != nil {
		t.Fatal(err)
	}
	if before != after {
		t.Fatalf("equal runtime metadata was rewritten: %v -> %v",
			before, after)
	}
}

func TestIntentV2EvaluationMetaPreservesUnresolvedVerificationAttention(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)
	capture := appendIntentCandidateCapture(
		t, db, "verify.go", "create", "", "after")
	candidate := state.IntentCandidate{
		ID: "verification-attention", BranchRef: "refs/heads/main",
		BranchGeneration: 1, Status: state.IntentCandidateWaiting,
		Readiness: state.IntentReadinessWait,
		VerificationStatus: sql.NullString{
			String: "failed", Valid: true,
		},
		Events: []state.IntentCandidateEvent{{
			EventSeq: capture.Event.Seq, EventRole: "code",
		}},
	}
	if err := state.SaveIntentCandidate(ctx, db, candidate); err != nil {
		t.Fatal(err)
	}
	bundle := &RuntimeBundle{
		PresetID: "intent.balanced", PresetVersion: config.PresetCatalogVersion,
	}
	if err := updateIntentV2EvaluationMeta(ctx, db, bundle,
		ReplaySummary{Skipped: true, SkippedReason: "intent_batch_wait"},
		nil); err != nil {
		t.Fatal(err)
	}
	attention, ok, err := state.MetaGet(
		ctx, db, "intent.v2.needs_attention")
	if err != nil || !ok || attention == "" {
		t.Fatalf("verification attention=%q ok=%v err=%v",
			attention, ok, err)
	}
	migration, _, _ := state.MetaGet(ctx, db, metaIntentV2MigrationState)
	if migration != "active" {
		t.Fatalf("candidate attention changed migration state to %q",
			migration)
	}

	replacement := candidate
	replacement.ID = "verification-resolved"
	replacement.VerificationStatus = sql.NullString{
		String: "passed", Valid: true,
	}
	if err := state.SaveIntentCandidate(ctx, db, replacement); err != nil {
		t.Fatal(err)
	}
	if err := updateIntentV2EvaluationMeta(ctx, db, bundle,
		ReplaySummary{Skipped: true, SkippedReason: "intent_batch_wait"},
		nil); err != nil {
		t.Fatal(err)
	}
	attention, ok, err = state.MetaGet(
		ctx, db, "intent.v2.needs_attention")
	if err != nil || !ok || attention != "" {
		t.Fatalf("resolved verification attention=%q ok=%v err=%v",
			attention, ok, err)
	}
}

func TestIntentV2CutoverUsesAuthoredIntentWithoutLegacyDBEvidence(t *testing.T) {
	ctx := context.Background()
	repo, dbPath := intentV2MigrationRepo(t)
	db, err := state.Open(ctx, dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	roots := intentV2MigrationRoots(t)
	repoHash := central.CanonicalID(repo)
	if err := config.NewStore(roots).Update(func(doc *config.Document) error {
		settings := doc.Settings.Repositories[repoHash]
		settings.Fields = config.Overrides{}
		settings.Fields[config.FieldCommitStrategy], _ =
			json.Marshal("intent")
		settings.Fields[config.FieldProvider], _ =
			json.Marshal("openai-compat")
		doc.Settings.Repositories[repoHash] = settings
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	result, err := EnsureIntentV2RuntimeCutover(
		ctx, db, repo, roots, func(string) (string, bool) {
			return "", false
		})
	if err != nil {
		t.Fatal(err)
	}
	if !result.Required || !result.Migrated ||
		result.PresetID != "intent.balanced" {
		t.Fatalf("authored cutover=%+v", result)
	}
}

func TestIntentV2CutoverResolvesIntentAcrossAuthoringLayers(t *testing.T) {
	for _, layer := range []string{"repository", "profile", "global", "environment"} {
		t.Run(layer, func(t *testing.T) {
			ctx := context.Background()
			repo, dbPath := intentV2MigrationRepo(t)
			db, err := state.Open(ctx, dbPath)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			roots := intentV2MigrationRoots(t)
			repoHash := central.CanonicalID(repo)
			if layer != "environment" {
				err = config.NewStore(roots).Update(func(doc *config.Document) error {
					value, _ := json.Marshal("intent")
					switch layer {
					case "repository":
						settings := doc.Settings.Repositories[repoHash]
						settings.Fields = config.Overrides{
							config.FieldCommitStrategy: value,
						}
						doc.Settings.Repositories[repoHash] = settings
					case "profile":
						doc.Settings.Profiles["team"] = config.Profile{
							Fields: config.Overrides{
								config.FieldCommitStrategy: value,
							},
						}
						settings := doc.Settings.Repositories[repoHash]
						settings.Profile = "team"
						doc.Settings.Repositories[repoHash] = settings
					case "global":
						doc.Settings.Global[config.FieldCommitStrategy] = value
					}
					return nil
				})
				if err != nil {
					t.Fatal(err)
				}
			}
			lookup := func(name string) (string, bool) {
				if layer == "environment" &&
					name == "ACD_COMMIT_STRATEGY" {
					return "intent", true
				}
				return "", false
			}
			result, err := EnsureIntentV2RuntimeCutover(
				ctx, db, repo, roots, lookup)
			if err != nil {
				t.Fatal(err)
			}
			if !result.Required || result.PresetID != "intent.balanced" {
				t.Fatalf("%s cutover=%+v", layer, result)
			}
		})
	}
}

func TestIntentV2CutoverInheritsMatchingGlobalSetupApproval(t *testing.T) {
	ctx := context.Background()
	repo, dbPath := intentV2MigrationRepo(t)
	db, err := state.Open(ctx, dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	roots := intentV2MigrationRoots(t)
	raw := func(value string) json.RawMessage {
		body, marshalErr := json.Marshal(value)
		if marshalErr != nil {
			t.Fatal(marshalErr)
		}
		return body
	}
	global := config.Overrides{
		config.FieldCommitStrategy:      raw("intent"),
		config.FieldCommitPreset:        raw("balanced"),
		config.FieldProvider:            raw("openai-compat"),
		config.FieldBaseURL:             raw("https://gateway.example/v1"),
		config.FieldModel:               raw("gateway-model"),
		config.FieldDiffEgress:          raw("true"),
		config.FieldIntentVerification:  raw("structural"),
		config.FieldIntentRepairEnabled: raw("true"),
	}
	resolved, preset, err := config.ResolveAll(
		config.ResolveInput{Global: global}, nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	effective := make(map[string]string, len(resolved))
	for name, field := range resolved {
		effective[name] = field.EffectiveValue()
	}
	fingerprint, err := config.SettingsFingerprint(effective, preset)
	if err != nil {
		t.Fatal(err)
	}
	if err := config.NewStore(roots).Update(func(doc *config.Document) error {
		doc.Settings.Global = global
		doc.Settings.GlobalSetupApproval = &config.GlobalSetupApproval{
			Generation:  1,
			Fingerprint: fingerprint,
			Confirmations: []string{
				string(ai.ConfirmationDiffEgress),
				string(ai.ConfirmationEndpointCredentials),
				string(ai.ConfirmationIntentRepair),
			},
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := config.NewStore(roots).Update(func(doc *config.Document) error {
		doc.Settings.Repositories["unrelated-repository"] =
			config.RepositorySettings{Fields: config.Overrides{
				config.FieldCommitFormat: raw("conventional"),
			}}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	result, err := EnsureIntentV2RuntimeCutover(
		ctx, db, repo, roots, func(name string) (string, bool) {
			if name == ai.EnvAPIKey {
				return "test-key", true
			}
			return "", false
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	revision, err := state.ConfigRevisionByID(ctx, db, result.RevisionID)
	if err != nil {
		t.Fatal(err)
	}
	values, confirmations, _, err := decodeRuntimeSnapshot(revision.SnapshotJSON)
	if err != nil {
		t.Fatal(err)
	}
	if values[config.FieldIntentVerification] != "structural" {
		t.Fatalf("verification=%q", values[config.FieldIntentVerification])
	}
	for _, confirmation := range []ai.ConfirmationRequirement{
		ai.ConfirmationDiffEgress,
		ai.ConfirmationEndpointCredentials,
		ai.ConfirmationIntentRepair,
	} {
		if _, ok := confirmations[string(confirmation)]; !ok {
			t.Fatalf("missing inherited confirmation %q: %+v",
				confirmation, confirmations)
		}
	}

	driftRepo, driftDBPath := intentV2MigrationRepo(t)
	driftDB, err := state.Open(ctx, driftDBPath)
	if err != nil {
		t.Fatal(err)
	}
	defer driftDB.Close()
	driftHash := central.CanonicalID(driftRepo)
	if err := config.NewStore(roots).Update(func(doc *config.Document) error {
		doc.Settings.Repositories[driftHash] = config.RepositorySettings{
			Fields: config.Overrides{
				config.FieldBaseURL: raw("https://other.example/v1"),
			},
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	driftResult, err := EnsureIntentV2RuntimeCutover(
		ctx, driftDB, driftRepo, roots, func(name string) (string, bool) {
			if name == ai.EnvAPIKey {
				return "test-key", true
			}
			return "", false
		},
	)
	if err == nil ||
		!strings.Contains(err.Error(), "do not match the tested global setup") {
		t.Fatalf("drifted repository result=%+v err=%v", driftResult, err)
	}
	if !driftResult.Required || driftResult.RevisionID != 0 {
		t.Fatalf("drifted repository cutover=%+v", driftResult)
	}
}

func TestRuntimeCutoverMaterializesApprovedGlobalEventDefaults(t *testing.T) {
	ctx := context.Background()
	repo, dbPath := intentV2MigrationRepo(t)
	db, err := state.Open(ctx, dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	roots := intentV2MigrationRoots(t)
	raw := func(value string) json.RawMessage {
		body, marshalErr := json.Marshal(value)
		if marshalErr != nil {
			t.Fatal(marshalErr)
		}
		return body
	}
	global := config.Overrides{
		config.FieldCommitStrategy:     raw("event"),
		config.FieldCommitPreset:       raw("fast"),
		config.FieldCommitFormat:       raw("imperative"),
		config.FieldProvider:           raw("deterministic"),
		config.FieldDiffEgress:         raw("false"),
		config.FieldIntentVerification: raw("none"),
	}
	resolved, preset, err := config.ResolveAll(
		config.ResolveInput{Global: global}, nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	effective := make(map[string]string, len(resolved))
	for name, field := range resolved {
		effective[name] = field.EffectiveValue()
	}
	fingerprint, err := config.SettingsFingerprint(effective, preset)
	if err != nil {
		t.Fatal(err)
	}
	if err := config.NewStore(roots).Update(func(doc *config.Document) error {
		doc.Settings.Global = global
		doc.Settings.GlobalSetupApproval = &config.GlobalSetupApproval{
			Generation: 1, Fingerprint: fingerprint,
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	result, err := EnsureIntentV2RuntimeCutover(ctx, db, repo, roots, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !result.Required || !result.Migrated ||
		result.PresetID != "event.fast" {
		t.Fatalf("global Event cutover=%+v", result)
	}
	revision, err := state.ConfigRevisionByID(ctx, db, result.RevisionID)
	if err != nil {
		t.Fatal(err)
	}
	values, confirmations, _, err := decodeRuntimeSnapshot(revision.SnapshotJSON)
	if err != nil {
		t.Fatal(err)
	}
	if values[config.FieldCommitStrategy] != "event" ||
		values[config.FieldProvider] != "deterministic" ||
		values[config.FieldIntentVerification] != "none" ||
		len(confirmations) != 0 {
		t.Fatalf("global Event values=%+v confirmations=%+v",
			values, confirmations)
	}
}

func TestRuntimeCutoverHonorsAuthoredDeterministicIntentFast(t *testing.T) {
	ctx := context.Background()
	repo, dbPath := intentV2MigrationRepo(t)
	db, err := state.Open(ctx, dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	roots := intentV2MigrationRoots(t)
	raw := func(value any) json.RawMessage {
		body, marshalErr := json.Marshal(value)
		if marshalErr != nil {
			t.Fatal(marshalErr)
		}
		return body
	}
	if err := config.NewStore(roots).Update(func(doc *config.Document) error {
		doc.Settings.Repositories[checkpoint.WorktreeID(repo)] = config.RepositorySettings{
			Fields: config.Overrides{
				config.FieldProvider:            raw("deterministic"),
				config.FieldCommitStrategy:      raw("intent"),
				config.FieldCommitPreset:        raw("fast"),
				config.FieldDiffEgress:          raw(false),
				config.FieldIntentVerification:  raw("structural"),
				config.FieldIntentRepairEnabled: raw(false),
			},
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	result, err := EnsureIntentV2RuntimeCutover(ctx, db, repo, roots, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !result.Required || !result.Migrated || result.PresetID != "intent.fast" {
		t.Fatalf("cutover=%+v", result)
	}
	revision, err := state.ConfigRevisionByID(ctx, db, result.RevisionID)
	if err != nil {
		t.Fatal(err)
	}
	bundle, err := (RuntimeBundleBuilder{DB: db, RepoRoot: repo}).BuildRevision(ctx, revision, nil)
	if err != nil {
		t.Fatal(err)
	}
	if bundle.ReplayBlockedReason != "" || bundle.IntentPlanner == nil {
		t.Fatalf("bundle=%+v", bundle)
	}
}

func TestRuntimeCutoverRejectsUntestedGlobalEventModelDrift(t *testing.T) {
	ctx := context.Background()
	repo, dbPath := intentV2MigrationRepo(t)
	db, err := state.Open(ctx, dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	roots := intentV2MigrationRoots(t)
	raw := func(value string) json.RawMessage {
		body, marshalErr := json.Marshal(value)
		if marshalErr != nil {
			t.Fatal(marshalErr)
		}
		return body
	}
	global := config.Overrides{
		config.FieldCommitStrategy:     raw("event"),
		config.FieldCommitPreset:       raw("fast"),
		config.FieldCommitFormat:       raw("imperative"),
		config.FieldProvider:           raw("openai-compat"),
		config.FieldBaseURL:            raw(ai.DefaultOpenAIBaseURL),
		config.FieldModel:              raw("tested-model"),
		config.FieldDiffEgress:         raw("false"),
		config.FieldIntentVerification: raw("none"),
	}
	resolved, preset, err := config.ResolveAll(
		config.ResolveInput{Global: global}, nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	effective := make(map[string]string, len(resolved))
	for name, field := range resolved {
		effective[name] = field.EffectiveValue()
	}
	fingerprint, err := config.SettingsFingerprint(effective, preset)
	if err != nil {
		t.Fatal(err)
	}
	repoHash := central.CanonicalID(repo)
	if err := config.NewStore(roots).Update(func(doc *config.Document) error {
		doc.Settings.Global = global
		doc.Settings.GlobalSetupApproval = &config.GlobalSetupApproval{
			Generation: 1, Fingerprint: fingerprint,
		}
		doc.Settings.Repositories[repoHash] = config.RepositorySettings{
			Fields: config.Overrides{
				config.FieldModel: raw("untested-repository-model"),
			},
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	result, err := EnsureIntentV2RuntimeCutover(
		ctx, db, repo, roots, func(name string) (string, bool) {
			if name == ai.EnvAPIKey {
				return "test-key", true
			}
			return "", false
		},
	)
	if err == nil ||
		!strings.Contains(err.Error(), "do not match the tested global setup") {
		t.Fatalf("model drift result=%+v err=%v", result, err)
	}
	if !result.Required {
		t.Fatalf("model drift did not require capture-only cutover: %+v", result)
	}
	var revisions int
	if err := db.ReadSQL().QueryRowContext(
		ctx, `SELECT COUNT(*) FROM config_revisions`,
	).Scan(&revisions); err != nil {
		t.Fatal(err)
	}
	if revisions != 0 {
		t.Fatalf("untested model created %d runtime revisions", revisions)
	}
}

func TestIntentV2CutoverInfersEventFastAndPreservesProviderFields(t *testing.T) {
	t.Setenv(ai.EnvBaseURL, ai.DefaultOpenAIBaseURL)
	t.Setenv(ai.EnvDiffEgress, "false")
	ctx := context.Background()
	repo, dbPath := intentV2MigrationRepo(t)
	db, err := state.Open(ctx, dbPath)
	if err != nil {
		t.Fatal(err)
	}
	legacy, err := state.InsertConfigRevision(ctx, db, state.ConfigRevisionInput{
		Snapshot: []byte(`{
			"ai.provider":"deterministic",
			"ai.model":"legacy-event-model",
			"ai.timeout":"41s",
			"commit.strategy":"event",
			"commit.format":"conventional",
			"intent.window":"13",
			"confirmations":[]
		}`),
		Profile: "legacy", Scope: "repository", SourceGeneration: 4,
		Reason: "legacy event",
	})
	if err != nil {
		t.Fatal(err)
	}
	request, ok, err := state.RequestConfigActivation(
		ctx, db, legacy.ID, sql.NullInt64{})
	if err != nil || !ok {
		t.Fatal(err)
	}
	_, _ = state.AcknowledgeConfigActivation(ctx, db, request.ID, legacy.ID)
	_, _ = state.ApplyConfigActivation(ctx, db, request.ID, legacy.ID)
	_ = db.Close()
	downgradeStateVersion(t, dbPath, 14)
	db, err = state.Open(ctx, dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	result, err := EnsureIntentV2RuntimeCutover(
		ctx, db, repo, intentV2MigrationRoots(t), nil)
	if err != nil {
		t.Fatal(err)
	}
	if result.PresetID != "event.fast" {
		t.Fatalf("event cutover=%+v", result)
	}
	revision, err := state.ConfigRevisionByID(ctx, db, result.RevisionID)
	if err != nil {
		t.Fatal(err)
	}
	values, _, _, err := decodeRuntimeSnapshot(revision.SnapshotJSON)
	if err != nil {
		t.Fatal(err)
	}
	if values[config.FieldProvider] != "deterministic" ||
		values[config.FieldModel] != "legacy-event-model" ||
		values[config.FieldTimeout] != "41s" ||
		values[config.FieldCommitFormat] != "conventional" ||
		values[config.FieldIntentWindow] != "13" {
		t.Fatalf("event migration lost overrides: %+v", values)
	}
	bundle, err := runtimeBuilder(db, map[string]*runtimeTestCloser{}).
		BuildRevision(ctx, revision, nil)
	if err != nil {
		t.Fatal(err)
	}
	if bundle.ReplayBlockedReason != "" ||
		bundle.PresetID != "event.fast" ||
		bundle.PresetVersion != config.PresetCatalogVersion {
		t.Fatalf("event bundle=%+v", bundle)
	}
}

func TestRuntimeBundleCredentialsUseEnvironmentThenProtectedFile(t *testing.T) {
	t.Setenv(ai.EnvBaseURL, ai.DefaultOpenAIBaseURL)
	t.Setenv(ai.EnvDiffEgress, "false")
	ctx := context.Background()
	repo, dbPath := intentV2MigrationRepo(t)
	db, err := state.Open(ctx, dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	roots := intentV2MigrationRoots(t)
	store := credentials.NewStore(roots)
	if err := store.Set("protected-secret"); err != nil {
		t.Fatal(err)
	}
	revision := runtimeRevision(t, db, "credentials", 1, map[string]any{
		config.FieldProvider:       "openai-compat",
		config.FieldCommitStrategy: "intent",
		config.FieldCommitPreset:   "fast",
	})
	var received string
	builder := RuntimeBundleBuilder{
		DB: db, RepoRoot: repo, CredentialStore: &store,
		LookupEnv: func(string) (string, bool) { return "", false },
		BuildProvider: func(cfg ai.ProviderConfig) (ai.Provider, io.Closer, error) {
			received = cfg.APIKey
			return &runtimeTestProvider{name: "openai-compat"}, nil, nil
		},
	}
	bundle, err := builder.BuildRevision(ctx, revision, nil)
	if err != nil {
		t.Fatal(err)
	}
	if received != "protected-secret" || bundle.ReplayBlockedReason != "" {
		t.Fatalf("protected credential was not used: received=%q bundle=%+v",
			received, bundle)
	}
	builder.LookupEnv = func(name string) (string, bool) {
		if name == ai.EnvAPIKey {
			return "environment-secret", true
		}
		return "", false
	}
	if _, err := builder.BuildRevision(ctx, revision, bundle); err != nil {
		t.Fatal(err)
	}
	if received != "environment-secret" {
		t.Fatalf("environment credential did not win: %q", received)
	}
	if strings.Contains(bundle.ReplayBlockedReason, "protected-secret") ||
		strings.Contains(bundle.HealthFingerprint, "protected-secret") {
		t.Fatal("credential leaked into bundle metadata")
	}
}

func intentV2MigrationRepo(t *testing.T) (string, string) {
	t.Helper()
	repo := filepath.Join(t.TempDir(), "repo")
	if err := os.MkdirAll(repo, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := git.Init(context.Background(), repo); err != nil {
		t.Fatal(err)
	}
	if _, err := git.Run(context.Background(), git.RunOpts{Dir: repo},
		"symbolic-ref", "HEAD", "refs/heads/main"); err != nil {
		t.Fatal(err)
	}
	return repo, filepath.Join(repo, ".git", "acd", "state.db")
}

func intentV2MigrationRoots(t *testing.T) paths.Roots {
	t.Helper()
	root := t.TempDir()
	return paths.Roots{
		Config: filepath.Join(root, "config"),
		State:  filepath.Join(root, "state"),
		Share:  filepath.Join(root, "share"),
	}
}

func downgradeStateVersion(t *testing.T, dbPath string, version int) {
	t.Helper()
	raw, err := sql.Open("sqlite", "file:"+dbPath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := raw.Exec("PRAGMA user_version = " +
		strconv.Itoa(version)); err != nil {
		raw.Close()
		t.Fatal(err)
	}
	if err := raw.Close(); err != nil {
		t.Fatal(err)
	}
}
