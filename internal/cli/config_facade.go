package cli

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/spf13/cobra"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/central"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/config"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/credentials"
	gitpkg "github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/paths"
)

type configScopeOptions struct {
	Scope   string
	Profile string
}

type configValue struct {
	Name   string        `json:"name"`
	Value  string        `json:"value"`
	Source config.Source `json:"source"`
}

func newConfigGetCmd() *cobra.Command {
	var options configScopeOptions
	cmd := &cobra.Command{
		Use: "get [KEY]", Short: "Read resolved configuration", Args: cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			repo, _ := cmd.Flags().GetString("repo")
			jsonOut, _ := cmd.Flags().GetBool("json")
			key := ""
			if len(args) == 1 {
				key = args[0]
			}
			return runConfigGet(cmd.Context(), cmd.OutOrStdout(), repo, options, key, jsonOut)
		},
	}
	addConfigScopeFlags(cmd, &options)
	return cmd
}

func newConfigSetCmd() *cobra.Command {
	var options configScopeOptions
	cmd := &cobra.Command{
		Use: "set KEY VALUE", Short: "Save one configuration value", Args: cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			repo, _ := cmd.Flags().GetString("repo")
			jsonOut, _ := cmd.Flags().GetBool("json")
			return runConfigSet(cmd.Context(), cmd.OutOrStdout(), repo, options, args[0], args[1], jsonOut)
		},
	}
	addConfigScopeFlags(cmd, &options)
	return cmd
}

func newConfigResetCmd() *cobra.Command {
	var options configScopeOptions
	cmd := &cobra.Command{
		Use: "reset [KEY]", Short: "Reset one value or an entire configuration scope", Args: cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			repo, _ := cmd.Flags().GetString("repo")
			jsonOut, _ := cmd.Flags().GetBool("json")
			key := ""
			if len(args) == 1 {
				key = args[0]
			}
			return runConfigReset(cmd.Context(), cmd.OutOrStdout(), repo, options, key, jsonOut)
		},
	}
	addConfigScopeFlags(cmd, &options)
	return cmd
}

func addConfigScopeFlags(cmd *cobra.Command, options *configScopeOptions) {
	cmd.Flags().StringVar(&options.Scope, "scope", "", "repo, profile, or global (default: repo inside a worktree; global outside)")
	cmd.Flags().StringVar(&options.Profile, "profile", "", "Profile name when --scope=profile")
}

type resolvedConfigTarget struct {
	Roots      paths.Roots
	Store      *config.Store
	Document   *config.Document
	Scope      string
	Profile    string
	WorktreeID string
	Repo       string
}

func resolveConfigTarget(ctx context.Context, repo string, options configScopeOptions) (resolvedConfigTarget, error) {
	roots, err := paths.Resolve()
	if err != nil {
		return resolvedConfigTarget{}, err
	}
	target := resolvedConfigTarget{Roots: roots, Store: config.NewStore(roots)}
	target.Document, err = target.Store.Load()
	if err != nil {
		return resolvedConfigTarget{}, err
	}
	wt, worktreeErr := gitpkg.ResolveWorktree(ctx, repo)
	scope := strings.ToLower(strings.TrimSpace(options.Scope))
	if scope == "" {
		if worktreeErr == nil {
			scope = "repo"
		} else if repo == "" && errors.Is(worktreeErr, gitpkg.ErrNotWorktree) {
			scope = "global"
		} else {
			return resolvedConfigTarget{}, worktreeErr
		}
	}
	if scope != "repo" && scope != "profile" && scope != "global" {
		return resolvedConfigTarget{}, invalidCommandError("acd config: invalid --scope %q", options.Scope)
	}
	if scope == "global" && repo != "" {
		return resolvedConfigTarget{}, invalidCommandError("acd config: --repo is not valid with --scope=global")
	}
	if scope == "profile" && strings.TrimSpace(options.Profile) == "" {
		return resolvedConfigTarget{}, invalidCommandError("acd config: --profile is required with --scope=profile")
	}
	if scope != "profile" && options.Profile != "" {
		return resolvedConfigTarget{}, invalidCommandError("acd config: --profile requires --scope=profile")
	}
	if scope == "repo" {
		if worktreeErr != nil {
			return resolvedConfigTarget{}, invalidCommandError("acd config: repository scope requires a Git worktree")
		}
		target.Repo = wt.Root
		target.WorktreeID = central.CanonicalID(wt.Root)
	}
	target.Scope, target.Profile = scope, strings.TrimSpace(options.Profile)
	return target, nil
}

func runConfigGet(ctx context.Context, out io.Writer, repo string, options configScopeOptions, key string, jsonOut bool) error {
	target, err := resolveConfigTarget(ctx, repo, options)
	if err != nil {
		return err
	}
	input, selected := configInput(target)
	resolved, _, err := config.ResolveAll(input, selected)
	if err != nil {
		return fmt.Errorf("acd config get: %w", err)
	}
	values := make([]configValue, 0, len(resolved))
	for _, definition := range config.Catalog() {
		if key != "" && definition.Name != key {
			continue
		}
		field := resolved[definition.Name]
		value := field.Value
		if definition.Sensitive {
			value = "redacted"
		}
		values = append(values, configValue{Name: definition.Name, Value: value, Source: field.Source})
	}
	if key != "" && len(values) == 0 {
		return invalidCommandError("acd config get: unknown field %q", key)
	}
	if jsonOut {
		return renderJSONEnvelope(out, productEnvelope{OK: true, State: configProductState(target), Actions: []productAction{}, Data: map[string]any{"scope": target.Scope, "profile": target.Profile, "values": values}})
	}
	for _, value := range values {
		fmt.Fprintf(out, "%s=%s\t%s\n", value.Name, value.Value, value.Source)
	}
	return nil
}

func runConfigSet(ctx context.Context, out io.Writer, repo string, options configScopeOptions, key, value string, jsonOut bool) error {
	target, err := resolveConfigTarget(ctx, repo, options)
	if err != nil {
		return err
	}
	definition, ok := config.LookupField(key)
	if !ok || !definition.Persistable || definition.Sensitive {
		return invalidCommandError("acd config set: field %q cannot be persisted; use `acd config credentials` for secrets", key)
	}
	raw, err := normalizedConfigRaw(definition, value)
	if err != nil {
		return invalidCommandError("acd config set: %v", err)
	}
	err = target.Store.UpdateExpected(target.Document.Generation, func(document *config.Document) error {
		overrides := configOverrides(document, target, true)
		overrides[key] = raw
		return saveConfigOverrides(document, target, overrides)
	})
	if err != nil {
		return fmt.Errorf("acd config set: %w", err)
	}
	next := "Run `acd config edit` to test and activate this saved draft."
	if definition.Boundary == config.ApplyRestart {
		next = "Run `acd off`, then `acd on`, to activate this restart-required setting."
	}
	return renderConfigMutation(out, target, jsonOut, "config_set", key, next)
}

func runConfigReset(ctx context.Context, out io.Writer, repo string, options configScopeOptions, key string, jsonOut bool) error {
	target, err := resolveConfigTarget(ctx, repo, options)
	if err != nil {
		return err
	}
	if key != "" {
		if _, ok := config.LookupField(key); !ok {
			return invalidCommandError("acd config reset: unknown field %q", key)
		}
	}
	err = target.Store.UpdateExpected(target.Document.Generation, func(document *config.Document) error {
		overrides := configOverrides(document, target, true)
		if key == "" {
			for name := range overrides {
				delete(overrides, name)
			}
		} else {
			delete(overrides, key)
		}
		return saveConfigOverrides(document, target, overrides)
	})
	if err != nil {
		return fmt.Errorf("acd config reset: %w", err)
	}
	targetName := key
	if targetName == "" {
		targetName = target.Scope
	}
	return renderConfigMutation(out, target, jsonOut, "config_reset", targetName,
		"Run `acd config edit` to review and activate the resolved settings.")
}

func configInput(target resolvedConfigTarget) (config.ResolveInput, config.Overrides) {
	input := config.ResolveInput{Global: target.Document.Settings.Global, LookupEnv: os.LookupEnv}
	switch target.Scope {
	case "repo":
		repository := target.Document.Settings.Repositories[target.WorktreeID]
		input.Repository = repository.Fields
		input.Profile = target.Document.Settings.Profiles[repository.Profile].Fields
		return input, repository.Fields
	case "profile":
		profile := target.Document.Settings.Profiles[target.Profile]
		input.Profile = profile.Fields
		return input, profile.Fields
	default:
		return input, target.Document.Settings.Global
	}
}

func configOverrides(document *config.Document, target resolvedConfigTarget, create bool) config.Overrides {
	var overrides config.Overrides
	switch target.Scope {
	case "repo":
		overrides = document.Settings.Repositories[target.WorktreeID].Fields
	case "profile":
		overrides = document.Settings.Profiles[target.Profile].Fields
	default:
		overrides = document.Settings.Global
	}
	if overrides == nil && create {
		overrides = config.Overrides{}
	}
	return overrides
}

func saveConfigOverrides(document *config.Document, target resolvedConfigTarget, overrides config.Overrides) error {
	switch target.Scope {
	case "repo":
		repository := document.Settings.Repositories[target.WorktreeID]
		repository.Fields = overrides
		document.Settings.Repositories[target.WorktreeID] = repository
	case "profile":
		profile := document.Settings.Profiles[target.Profile]
		profile.Fields = overrides
		document.Settings.Profiles[target.Profile] = profile
	default:
		document.Settings.Global = overrides
	}
	return nil
}

func normalizedConfigRaw(definition config.FieldDefinition, value string) (json.RawMessage, error) {
	testDocument := config.NewDocument()
	raw, _ := json.Marshal(value)
	testDocument.Settings.Global[definition.Name] = raw
	if err := config.ValidateDocument(testDocument); err != nil {
		return nil, err
	}
	return raw, nil
}

func renderConfigMutation(out io.Writer, target resolvedConfigTarget, jsonOut bool, kind, name, next string) error {
	stateName := configProductState(target)
	if target.Scope == "repo" {
		stateName = productStateWaiting
	}
	if jsonOut {
		return renderJSONEnvelope(out, productEnvelope{OK: true, State: stateName, Changed: true,
			Actions: []productAction{{Kind: kind, Status: "completed", Target: name}}, NextAction: &next,
			Data: map[string]any{"scope": target.Scope, "profile": target.Profile, "generation": target.Document.Generation + 1}})
	}
	fmt.Fprintf(out, "Saved %s configuration for %s.\n", target.Scope, name)
	fmt.Fprintf(out, "Next: %s\n", next)
	return nil
}

func runProductCredentialStatus(out io.Writer, jsonOut bool) error {
	roots, err := paths.Resolve()
	if err != nil {
		return err
	}
	store := credentials.NewStore(roots)
	status, err := store.Status()
	if err != nil {
		return fmt.Errorf("acd config credentials: %w", err)
	}
	_, source, err := credentials.Resolve(store, os.LookupEnv)
	if err != nil {
		return fmt.Errorf("acd config credentials: %w", err)
	}
	data := map[string]any{"source": source, "environment_set": source == credentials.SourceEnvironment,
		"protected_file_set": status.ProtectedFileSet, "path": status.Path}
	if jsonOut {
		return renderJSONEnvelope(out, productEnvelope{OK: true, State: productStateOff,
			Actions: []productAction{}, Data: data})
	}
	fmt.Fprintf(out, "Credential source: %s\n", source)
	fmt.Fprintf(out, "Protected file configured: %t\n", status.ProtectedFileSet)
	fmt.Fprintf(out, "Protected file: %s\n", status.Path)
	return nil
}

func configProductState(target resolvedConfigTarget) productState {
	if target.Scope == "repo" {
		return productStateWaiting
	}
	return productStateOff
}
