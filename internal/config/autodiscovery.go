// Package config loads global operator configuration.
package config

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/paths"
)

const (
	// EnvRepoAutodiscovery overrides the durable config policy when set to a
	// non-empty true/false value.
	EnvRepoAutodiscovery = "ACD_REPO_AUTODISCOVERY"

	DecisionSourceDefault = "default"
	DecisionSourceConfig  = "config"
	DecisionSourceEnv     = "env"

	SkipReasonPolicyError = "autodiscovery_policy_error"
)

// PolicyCaller identifies whether the caller can safely surface config errors
// to a human or must remain non-blocking for harness hooks.
type PolicyCaller string

const (
	PolicyCallerManual PolicyCaller = "manual"
	PolicyCallerHook   PolicyCaller = "hook"
)

// AutodiscoveryDecision tells CLI callers whether they may implicitly create
// repo state and a central registry row.
type AutodiscoveryDecision struct {
	Allowed       bool
	Source        string
	ConfigPath    string
	SkippedReason string
}

type fileConfig struct {
	RepoLifecycle *repoLifecycleConfig `json:"repo_lifecycle"`
}

type repoLifecycleConfig struct {
	Autodiscovery *bool `json:"autodiscovery"`
}

// ImplicitRepoRegistrationAllowed loads the global autodiscovery policy using
// the user's XDG config location.
func ImplicitRepoRegistrationAllowed(caller PolicyCaller) (AutodiscoveryDecision, error) {
	roots, err := paths.Resolve()
	if err != nil {
		return handlePolicyError(caller, AutodiscoveryDecision{
			Allowed: false,
			Source:  DecisionSourceDefault,
		}, fmt.Errorf("acd config: resolve paths: %w", err))
	}
	return ImplicitRepoRegistrationAllowedFromRoots(caller, roots)
}

// ImplicitRepoRegistrationAllowedFromRoots is the testable form of
// ImplicitRepoRegistrationAllowed.
func ImplicitRepoRegistrationAllowedFromRoots(caller PolicyCaller, roots paths.Roots) (AutodiscoveryDecision, error) {
	decision := AutodiscoveryDecision{
		Allowed:    true,
		Source:     DecisionSourceDefault,
		ConfigPath: roots.ConfigPath(),
	}

	if cfg, ok, err := readAutodiscoveryConfig(decision.ConfigPath); err != nil {
		return handlePolicyError(caller, decision, err)
	} else if ok {
		decision.Allowed = cfg
		decision.Source = DecisionSourceConfig
	}

	if raw := strings.TrimSpace(os.Getenv(EnvRepoAutodiscovery)); raw != "" {
		enabled, err := parseAutodiscoveryBool(raw)
		if err != nil {
			return handlePolicyError(caller, decision, fmt.Errorf("acd config: %s: %w", EnvRepoAutodiscovery, err))
		}
		decision.Allowed = enabled
		decision.Source = DecisionSourceEnv
	}

	return decision, nil
}

func readAutodiscoveryConfig(path string) (bool, bool, error) {
	body, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return false, false, nil
		}
		return false, false, fmt.Errorf("acd config: read %s: %w", path, err)
	}
	var cfg fileConfig
	dec := json.NewDecoder(bytes.NewReader(body))
	if err := dec.Decode(&cfg); err != nil {
		return false, false, fmt.Errorf("acd config: parse %s: %w", path, err)
	}
	if err := dec.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return false, false, fmt.Errorf("acd config: parse %s: multiple JSON values", path)
	}
	if cfg.RepoLifecycle == nil || cfg.RepoLifecycle.Autodiscovery == nil {
		return false, false, nil
	}
	return *cfg.RepoLifecycle.Autodiscovery, true, nil
}

func parseAutodiscoveryBool(raw string) (bool, error) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "1", "true", "yes", "on", "enable", "enabled":
		return true, nil
	case "0", "false", "no", "off", "disable", "disabled":
		return false, nil
	default:
		return false, fmt.Errorf("invalid boolean %q", raw)
	}
}

func handlePolicyError(caller PolicyCaller, decision AutodiscoveryDecision, err error) (AutodiscoveryDecision, error) {
	if caller == PolicyCallerHook {
		decision.Allowed = false
		decision.SkippedReason = SkipReasonPolicyError
		return decision, nil
	}
	return decision, err
}
