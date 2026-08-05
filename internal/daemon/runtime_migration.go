package daemon

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/ai"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/config"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/paths"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
)

const (
	metaIntentV2CutoverRequired = "intent.v2.cutover_required"
	metaIntentV2MigrationState  = "intent.v2.migration_state"
)

type IntentV2CutoverResult struct {
	Required      bool
	Migrated      bool
	RevisionID    int64
	PresetID      string
	PresetVersion int
	Customized    bool
}

// EnsureIntentV2RuntimeCutover materializes one immutable v2 runtime revision
// before an upgraded repository can lease replay settings. It does not touch
// capture events: missing provider, diff-context, or verification
// prerequisites are represented by the revision and later block replay while
// capture continues.
func EnsureIntentV2RuntimeCutover(
	ctx context.Context,
	db *state.DB,
	repoRoot string,
	roots paths.Roots,
	lookupEnv func(string) (string, bool),
) (IntentV2CutoverResult, error) {
	var result IntentV2CutoverResult
	if db == nil {
		return result, errors.New("daemon: Intent v2 cutover: state db is required")
	}
	required, markerPresent, err := state.MetaGet(ctx, db, metaIntentV2CutoverRequired)
	if err != nil {
		return result, err
	}
	if lookupEnv == nil {
		lookupEnv = os.LookupEnv
	}

	repoHash, err := paths.RepoHash(repoRoot)
	if err != nil {
		return result, fmt.Errorf("daemon: Intent v2 cutover: repository identity: %w", err)
	}
	doc, err := config.NewStore(roots).Load()
	if err != nil {
		return result, fmt.Errorf("daemon: Intent v2 cutover: load authoring settings: %w", err)
	}
	repoSettings := doc.Settings.Repositories[repoHash]
	profile := doc.Settings.Profiles[repoSettings.Profile]
	input := config.ResolveInput{
		Repository: repoSettings.Fields,
		Profile:    profile.Fields,
		Global:     doc.Settings.Global,
		LookupEnv:  lookupEnv,
	}
	base, _, err := config.ResolveAll(input, nil)
	if err != nil {
		return result, fmt.Errorf("daemon: Intent v2 cutover: resolve authoring settings: %w", err)
	}
	authoredIntent := base[config.FieldCommitStrategy].EffectiveValue() ==
		string(config.StrategyIntent)
	_, globalSetupApproved := config.ActiveGlobalSetupApproval(doc)
	result.Required = markerPresent && parseRuntimeBool(required) ||
		authoredIntent || globalSetupApproved
	if !result.Required {
		return result, nil
	}

	runtimeState, err := state.RuntimeConfigActivationState(ctx, db)
	if err != nil {
		return result, err
	}
	sourceRevisionID := firstRuntimeRevisionID(runtimeState)
	values := make(map[string]string)
	confirmations := make(map[string]struct{})
	sourceGeneration := int64(doc.Generation)
	profileName := repoSettings.Profile
	if profileName == "" {
		profileName = "default"
	}
	if sourceRevisionID == 0 {
		if legacyStrategy, ok, err := state.MetaGet(
			ctx, db, "commit.strategy"); err != nil {
			return result, err
		} else if ok {
			legacyStrategy = strings.TrimSpace(legacyStrategy)
			if legacyStrategy == string(config.StrategyEvent) ||
				legacyStrategy == string(config.StrategyIntent) {
				values[config.FieldCommitStrategy] = legacyStrategy
			}
		}
	}
	if sourceRevisionID > 0 {
		revision, err := state.ConfigRevisionByID(ctx, db, sourceRevisionID)
		if err != nil {
			return result, err
		}
		sourceGeneration = revision.SourceGeneration
		profileName = revision.Profile
		decoded, priorConfirmations, metadata, err :=
			decodeRuntimeSnapshot(revision.SnapshotJSON)
		if err != nil {
			return result, err
		}
		if metadata.PresetVersion == config.PresetCatalogVersion &&
			metadata.PresetID != "" {
			if err := setRuntimeMetaIfChanged(ctx, db, map[string]string{
				metaIntentV2MigrationState: "migrated",
				"intent.v2.preset_id":      metadata.PresetID,
				"intent.v2.preset_version": strconv.Itoa(metadata.PresetVersion),
			}); err != nil {
				return result, err
			}
			if _, err := state.MetaDelete(
				ctx, db, metaIntentV2CutoverRequired); err != nil {
				return result, err
			}
			result.RevisionID = revision.ID
			result.PresetID = metadata.PresetID
			result.PresetVersion = metadata.PresetVersion
			result.Customized = metadata.Customized
			return result, nil
		}
		values = decoded
		confirmations = priorConfirmations
	}

	strategy := config.CommitStrategy(base[config.FieldCommitStrategy].EffectiveValue())
	if value := strings.TrimSpace(values[config.FieldCommitStrategy]); value != "" {
		strategy = config.CommitStrategy(value)
	}
	presetName := config.PresetFast
	if strategy == config.StrategyIntent {
		presetName = config.PresetBalanced
	}
	if explicit := config.PresetName(values[config.FieldCommitPreset]); explicit != "" {
		if _, exists := config.LookupPreset(strategy, explicit); exists {
			presetName = explicit
		}
	}

	overrides := make(config.Overrides, len(values)+2)
	for name, value := range values {
		field, exists := config.LookupField(name)
		if !exists || field.Boundary != config.ApplyHot ||
			field.Sensitive || !field.Persistable {
			continue
		}
		overrides[name], _ = json.Marshal(value)
	}
	overrides[config.FieldCommitStrategy], _ = json.Marshal(string(strategy))
	overrides[config.FieldCommitPreset], _ = json.Marshal(string(presetName))
	input.Experiment = overrides
	resolved, preset, err := config.ResolveAll(input, overrides)
	if err != nil {
		return result, fmt.Errorf("daemon: Intent v2 cutover: materialize preset: %w", err)
	}
	if approval, ok := config.ActiveGlobalSetupApproval(doc); ok &&
		sourceRevisionID == 0 {
		effective := make(map[string]string, len(resolved))
		for name, field := range resolved {
			effective[name] = field.EffectiveValue()
		}
		fingerprint, fingerprintErr := config.SettingsFingerprint(effective, preset)
		if fingerprintErr != nil {
			return result, fmt.Errorf(
				"daemon: Intent v2 cutover: fingerprint global setup: %w",
				fingerprintErr,
			)
		}
		if fingerprint != approval.Fingerprint {
			return result, errors.New(
				"daemon: Intent v2 cutover: effective settings do not match the tested global setup; run acd configure --repo .",
			)
		}
		for _, confirmation := range approval.Confirmations {
			confirmations[confirmation] = struct{}{}
		}
	}
	if strategy == config.StrategyIntent &&
		resolved[config.FieldIntentRepairEnabled].EffectiveValue() == "true" {
		// The v2 cutover contract explicitly activates strict automatic repair
		// for migrated Balanced/Quality repositories. Diff context and exact
		// verification commands remain unapproved unless a matching reviewed
		// setup supplied their separate confirmations.
		confirmations[string(ai.ConfirmationIntentRepair)] = struct{}{}
	}
	snapshot := make(map[string]any)
	for _, field := range config.Catalog() {
		if field.Boundary != config.ApplyHot || field.Sensitive ||
			!field.Persistable {
			continue
		}
		snapshot[field.Name] = resolved[field.Name].EffectiveValue()
	}
	snapshot["preset_id"] = preset.ID()
	snapshot["preset_version"] = preset.Version()
	snapshot["customized"] = preset.Customized
	confirmationList := make([]string, 0, len(confirmations))
	for confirmation := range confirmations {
		if strings.TrimSpace(confirmation) != "" {
			confirmationList = append(confirmationList, confirmation)
		}
	}
	sort.Strings(confirmationList)
	snapshot["confirmations"] = confirmationList
	body, err := json.Marshal(snapshot)
	if err != nil {
		return result, fmt.Errorf("daemon: Intent v2 cutover: encode revision: %w", err)
	}
	revision, reused, err := reusableIntentV2CutoverRevision(
		ctx, db, string(body))
	if err != nil {
		return result, err
	}
	if !reused {
		revision, err = state.InsertConfigRevision(ctx, db, state.ConfigRevisionInput{
			Snapshot: body, Profile: profileName, Scope: "repository",
			SourceGeneration: sourceGeneration, Reason: "Intent v2 cutover",
		})
		if err != nil {
			return result, err
		}
	}
	_, activated, err := state.RequestConfigActivation(
		ctx, db, revision.ID, runtimeState.DesiredRevisionID)
	if err != nil {
		return result, err
	}
	if !activated {
		return result, errors.New("daemon: Intent v2 cutover: desired revision changed")
	}
	if err := setRuntimeMetaIfChanged(ctx, db, map[string]string{
		metaIntentV2MigrationState: "migrated",
		"intent.v2.preset_id":      preset.ID(),
		"intent.v2.preset_version": strconv.Itoa(preset.Version()),
	}); err != nil {
		return result, err
	}
	if _, err := state.MetaDelete(ctx, db, metaIntentV2CutoverRequired); err != nil {
		return result, err
	}
	result.Migrated = true
	result.RevisionID = revision.ID
	result.PresetID = preset.ID()
	result.PresetVersion = preset.Version()
	result.Customized = preset.Customized
	return result, nil
}

func reusableIntentV2CutoverRevision(
	ctx context.Context,
	db *state.DB,
	snapshot string,
) (state.ConfigRevision, bool, error) {
	var revision state.ConfigRevision
	err := db.ReadSQL().QueryRowContext(ctx, `
SELECT id, created_ts, profile, scope, snapshot_json, snapshot_hash,
       source_generation, reason
FROM config_revisions
WHERE reason='Intent v2 cutover' AND snapshot_json=?
ORDER BY id DESC LIMIT 1`, snapshot).Scan(
		&revision.ID, &revision.CreatedTS, &revision.Profile, &revision.Scope,
		&revision.SnapshotJSON, &revision.SnapshotHash,
		&revision.SourceGeneration, &revision.Reason)
	if errors.Is(err, sql.ErrNoRows) {
		return state.ConfigRevision{}, false, nil
	}
	if err != nil {
		return state.ConfigRevision{}, false,
			fmt.Errorf("daemon: Intent v2 cutover: reuse revision: %w", err)
	}
	return revision, true, nil
}

func firstRuntimeRevisionID(runtime state.RuntimeConfigState) int64 {
	for _, value := range []sql.NullInt64{
		runtime.DesiredRevisionID,
		runtime.AppliedRevisionID,
		runtime.LastKnownGoodRevisionID,
	} {
		if value.Valid && value.Int64 > 0 {
			return value.Int64
		}
	}
	return 0
}

func setRuntimeMetaIfChanged(
	ctx context.Context,
	db *state.DB,
	pairs map[string]string,
) error {
	changed := make(map[string]string)
	for key, value := range pairs {
		current, exists, err := state.MetaGet(ctx, db, key)
		if err != nil {
			return err
		}
		if !exists || current != value {
			changed[key] = value
		}
	}
	return state.MetaSetMany(ctx, db, changed)
}

func hasUnresolvedIntentV2CandidateAttention(
	ctx context.Context,
	db *state.DB,
) (bool, error) {
	if db == nil {
		return false, errors.New("daemon: Intent v2 attention: state db is required")
	}
	var attention int
	err := db.SQL().QueryRowContext(ctx, `
SELECT EXISTS(
    SELECT 1
    FROM intent_candidates c
    JOIN intent_candidate_events ce
      ON ce.candidate_id=c.id AND ce.membership_state='active'
    JOIN capture_events e
      ON e.seq=ce.event_seq AND e.state='pending'
    WHERE c.status='blocked'
       OR c.verification_status IN ('failed','timed_out','needs_attention')
)`).Scan(&attention)
	if err != nil {
		return false, err
	}
	return attention != 0, nil
}

func updateIntentV2EvaluationMeta(
	ctx context.Context,
	db *state.DB,
	bundle *RuntimeBundle,
	summary ReplaySummary,
	replayErr error,
) error {
	if replayErr != nil || bundle == nil {
		return nil
	}
	unresolved, err := hasUnresolvedIntentV2CandidateAttention(ctx, db)
	if err != nil {
		return err
	}
	pairs := map[string]string{
		metaIntentV2MigrationState:  "active",
		"intent.v2.preset_id":       bundle.PresetID,
		"intent.v2.preset_version":  strconv.Itoa(bundle.PresetVersion),
		"intent.v2.needs_attention": "",
	}
	if summary.SkippedReason == "intent_v2_needs_attention" || unresolved {
		pairs["intent.v2.needs_attention"] = runtimeConfigureReason(
			"candidate planning or verification needs attention")
	}
	return setRuntimeMetaIfChanged(ctx, db, pairs)
}
