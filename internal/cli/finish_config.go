package cli

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"slices"
	"strings"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/ai"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/central"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/config"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/credentials"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/daemon"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/paths"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/verification"
)

// resolveFinishWorkerConfig reads approved inputs only. It does not activate
// authoring changes, build a provider, or run a project check.
func resolveFinishWorkerConfig(ctx context.Context, roots paths.Roots, runtime *workerRuntime) (daemon.FinishWorkerConfig, error) {
	return resolveFinishWorkerConfigInputs(ctx, roots, runtime, true)
}

func resolveFinishWorkerConfigInputs(ctx context.Context, roots paths.Roots, runtime *workerRuntime, withCredentials bool) (daemon.FinishWorkerConfig, error) {
	var out daemon.FinishWorkerConfig
	revision, found, err := finishApprovedRevision(ctx, runtime.db)
	if err != nil {
		return out, err
	}
	var values map[string]string
	var approved []ai.ConfirmationRequirement
	if found {
		values, approved, err = finishSnapshotValues(revision.SnapshotJSON)
		out.ConfigDigest = fmt.Sprintf("revision:%d:%s", revision.ID, revision.SnapshotHash)
	} else {
		values, approved, err = finishSavedValues(roots, runtime.worktree.Root)
		if err == nil {
			out.ConfigDigest, err = config.SettingsFingerprint(values)
		}
	}
	if err != nil {
		return out, err
	}
	out.Mode = values[config.FieldCommitStrategy]
	out.Style = values[config.FieldCommitFormat]
	out.Approved = approved
	out.Capture = daemon.CaptureOpts{MaxFileBytes: runtime.policy.MaxFileBytes, SensitiveMatcher: runtime.policy.Sensitive, SafeIgnoreMatcher: runtime.policy.SafeIgnore}
	out.Provider = ai.ProviderConfig{Mode: values[config.FieldProvider], BaseURL: values[config.FieldBaseURL], Model: values[config.FieldModel], CAFile: values[config.FieldCAFile], DiffEgress: values[config.FieldDiffEgress] == "true", CommitStrategy: ai.CommitStrategy(out.Mode), CommitFormat: ai.CommitFormat(out.Style)}
	out.Provider.Timeout, err = time.ParseDuration(values[config.FieldTimeout])
	if err != nil {
		return out, err
	}
	if out.Mode == "local" {
		out.Provider.Mode, out.Provider.DiffEgress = "deterministic", false
	} else if withCredentials && out.Provider.Mode == "openai-compat" {
		out.Provider.APIKey, _, err = credentials.Resolve(credentials.NewStore(roots), os.LookupEnv)
		if err != nil {
			return out, err
		}
	}
	out.VerificationMode = values[config.FieldIntentVerification]
	if err := finishRequireApprovals(out); err != nil {
		return out, err
	}
	out.VerificationTimeout = 5 * time.Minute
	if out.VerificationMode == "fast" || out.VerificationMode == "full" {
		commandField, timeoutField := config.FieldVerificationFastCommand, config.FieldVerificationFastTimeout
		if out.VerificationMode == "full" {
			commandField, timeoutField = config.FieldVerificationFullCommand, config.FieldVerificationFullTimeout
		}
		out.VerificationTimeout, err = time.ParseDuration(values[timeoutField])
		if err == nil {
			out.Command, err = verification.NewApprovedCommand(runtime.worktree.Root, out.ConfigDigest, verification.Mode(out.VerificationMode), values[commandField], out.VerificationTimeout)
		}
		if err != nil {
			return out, err
		}
	}
	out.AuthorName, out.AuthorEmail, err = finishGitAuthor(ctx, runtime.worktree.Root)
	return out, err
}

func finishApprovedRevision(ctx context.Context, db *state.DB) (state.ConfigRevision, bool, error) {
	if db == nil {
		return state.ConfigRevision{}, false, nil
	}
	active, err := state.RuntimeConfigActivationState(ctx, db)
	if err != nil {
		return state.ConfigRevision{}, false, err
	}
	for _, id := range []int64{active.AppliedRevisionID.Int64, active.LastKnownGoodRevisionID.Int64} {
		if id > 0 {
			revision, err := state.ConfigRevisionByID(ctx, db, id)
			return revision, err == nil, err
		}
	}
	if active.DesiredRevisionID.Valid || active.DesiredRequestID.Valid {
		return state.ConfigRevision{}, false, errors.New("finish: configuration is not approved for use; approve its settings with acd config")
	}
	return state.ConfigRevision{}, false, nil
}

func finishSnapshotValues(body string) (map[string]string, []ai.ConfirmationRequirement, error) {
	var raw map[string]json.RawMessage
	if err := json.Unmarshal([]byte(body), &raw); err != nil || raw == nil {
		return nil, nil, errors.New("finish: approved configuration snapshot is unreadable")
	}
	var confirmations []ai.ConfirmationRequirement
	if encoded, exists := raw["confirmations"]; exists {
		if err := json.Unmarshal(encoded, &confirmations); err != nil {
			return nil, nil, errors.New("finish: approved configuration confirmations are unreadable")
		}
	}
	values, err := finishResolveValues(config.ResolveInput{Global: finishTranslateEvent(raw)})
	return values, confirmations, err
}

func finishSavedValues(roots paths.Roots, repo string) (map[string]string, []ai.ConfirmationRequirement, error) {
	doc, err := config.NewStore(roots).Load()
	if err != nil {
		return nil, nil, err
	}
	id := central.CanonicalID(repo)
	repository := doc.Settings.Repositories[id]
	values, err := finishResolveValues(config.ResolveInput{Repository: repository.Fields, Profile: doc.Settings.Profiles[repository.Profile].Fields, Global: doc.Settings.Global, LookupEnv: os.LookupEnv, LegacyDefaults: config.LegacyFinishDefaults(doc, []string{"repository:" + id, "profile:" + repository.Profile, "global"}, os.LookupEnv)})
	if err != nil {
		return nil, nil, err
	}
	fingerprint, err := config.SettingsFingerprint(values)
	if err != nil {
		return nil, nil, err
	}
	var approved []ai.ConfirmationRequirement
	if approval, exists := config.ActiveGlobalSetupApproval(doc); exists && approval.Fingerprint == fingerprint {
		for _, scope := range approval.Confirmations {
			approved = append(approved, ai.ConfirmationRequirement(scope))
		}
	}
	return values, approved, nil
}

func finishResolveValues(input config.ResolveInput) (map[string]string, error) {
	// Snapshots from older releases may include removed controls. Resolve only
	// the supported hot catalog, and never evaluate secret or restart inputs.
	values := map[string]string{}
	for _, field := range config.Catalog() {
		if field.Boundary != config.ApplyHot || !field.Persistable || field.Sensitive {
			continue
		}
		resolved, err := config.ResolveField(field.Name, input)
		if err != nil {
			return nil, err
		}
		values[field.Name] = resolved.EffectiveValue()
	}
	return values, nil
}

func finishTranslateEvent(values config.Overrides) config.Overrides {
	var mode string
	if json.Unmarshal(values[config.FieldCommitStrategy], &mode) != nil || !strings.EqualFold(strings.TrimSpace(mode), "event") {
		return values
	}
	out := make(config.Overrides, len(values))
	for key, value := range values {
		out[key] = value
	}
	out[config.FieldCommitStrategy] = json.RawMessage(`"local"`)
	return out
}

func finishRequireApprovals(cfg daemon.FinishWorkerConfig) error {
	var required []ai.ConfirmationRequirement
	if cfg.Mode == "intent" {
		switch {
		case cfg.Provider.Mode == "openai-compat":
			endpoint := strings.TrimRight(strings.TrimSpace(cfg.Provider.BaseURL), "/")
			if endpoint != ai.DefaultOpenAIBaseURL {
				required = append(required, ai.ConfirmationEndpointCredentials)
			}
			if strings.HasPrefix(strings.ToLower(endpoint), "http://") {
				required = append(required, ai.ConfirmationInsecureEndpointCredentials)
			}
		case strings.HasPrefix(cfg.Provider.Mode, "subprocess:"):
			required = append(required, ai.ConfirmationSubprocessExecution)
		}
		if cfg.Provider.DiffEgress {
			required = append(required, ai.ConfirmationDiffEgress)
		}
	}
	if cfg.VerificationMode == "fast" || cfg.VerificationMode == "full" {
		required = append(required, ai.ConfirmationVerificationCommand)
	}
	for _, scope := range required {
		if !slices.Contains(cfg.Approved, scope) {
			return fmt.Errorf("finish: %s requires approval for the exact configuration; run acd config", scope)
		}
	}
	return nil
}

func finishGitAuthor(ctx context.Context, repo string) (string, string, error) {
	out, err := git.Run(ctx, git.RunOpts{Dir: repo}, "var", "GIT_AUTHOR_IDENT")
	if err != nil {
		// An empty finish does not need an author. The worker rejects a
		// nonempty target if this identity is absent.
		return "", "", ctx.Err()
	}
	identity := strings.TrimSpace(string(out))
	start, end := strings.LastIndex(identity, " <"), strings.LastIndex(identity, ">")
	if start <= 0 || end <= start+2 {
		return "", "", errors.New("finish: Git author identity is unreadable")
	}
	return identity[:start], identity[start+2 : end], nil
}
