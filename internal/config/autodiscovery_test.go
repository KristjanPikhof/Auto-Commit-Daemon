package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/paths"
)

func TestImplicitRepoRegistrationAllowed_DefaultEnabledWhenConfigMissing(t *testing.T) {
	roots := testRoots(t)

	decision, err := ImplicitRepoRegistrationAllowedFromRoots(PolicyCallerManual, roots)
	if err != nil {
		t.Fatalf("ImplicitRepoRegistrationAllowedFromRoots: %v", err)
	}
	if !decision.Allowed {
		t.Fatalf("Allowed = false, want true")
	}
	if decision.Source != DecisionSourceDefault {
		t.Fatalf("Source = %q, want %q", decision.Source, DecisionSourceDefault)
	}
	if decision.ConfigPath != filepath.Join(roots.Config, "config.json") {
		t.Fatalf("ConfigPath = %q", decision.ConfigPath)
	}
}

func TestImplicitRepoRegistrationAllowed_ConfigFalse(t *testing.T) {
	roots := testRoots(t)
	writeConfig(t, roots, `{"repo_lifecycle":{"autodiscovery":false}}`)

	decision, err := ImplicitRepoRegistrationAllowedFromRoots(PolicyCallerManual, roots)
	if err != nil {
		t.Fatalf("ImplicitRepoRegistrationAllowedFromRoots: %v", err)
	}
	if decision.Allowed {
		t.Fatalf("Allowed = true, want false")
	}
	if decision.Source != DecisionSourceConfig {
		t.Fatalf("Source = %q, want %q", decision.Source, DecisionSourceConfig)
	}
}

func TestImplicitRepoRegistrationAllowed_EnvOverrideWins(t *testing.T) {
	roots := testRoots(t)
	writeConfig(t, roots, `{"repo_lifecycle":{"autodiscovery":false}}`)
	t.Setenv(EnvRepoAutodiscovery, "enabled")

	decision, err := ImplicitRepoRegistrationAllowedFromRoots(PolicyCallerManual, roots)
	if err != nil {
		t.Fatalf("ImplicitRepoRegistrationAllowedFromRoots: %v", err)
	}
	if !decision.Allowed {
		t.Fatalf("Allowed = false, want true")
	}
	if decision.Source != DecisionSourceEnv {
		t.Fatalf("Source = %q, want %q", decision.Source, DecisionSourceEnv)
	}
}

func TestImplicitRepoRegistrationAllowed_EnvFalseDisablesWithoutConfig(t *testing.T) {
	roots := testRoots(t)
	t.Setenv(EnvRepoAutodiscovery, "off")

	decision, err := ImplicitRepoRegistrationAllowedFromRoots(PolicyCallerManual, roots)
	if err != nil {
		t.Fatalf("ImplicitRepoRegistrationAllowedFromRoots: %v", err)
	}
	if decision.Allowed {
		t.Fatalf("Allowed = true, want false")
	}
	if decision.Source != DecisionSourceEnv {
		t.Fatalf("Source = %q, want %q", decision.Source, DecisionSourceEnv)
	}
}

func TestImplicitRepoRegistrationAllowed_ManualCallerReturnsMalformedConfig(t *testing.T) {
	roots := testRoots(t)
	writeConfig(t, roots, `{"repo_lifecycle":`)

	_, err := ImplicitRepoRegistrationAllowedFromRoots(PolicyCallerManual, roots)
	if err == nil {
		t.Fatalf("expected malformed config error")
	}
	if !strings.Contains(err.Error(), roots.ConfigPath()) {
		t.Fatalf("error %q does not mention config path %q", err, roots.ConfigPath())
	}
}

func TestImplicitRepoRegistrationAllowed_HookCallerSkipsMalformedConfig(t *testing.T) {
	roots := testRoots(t)
	writeConfig(t, roots, `{"repo_lifecycle":`)

	decision, err := ImplicitRepoRegistrationAllowedFromRoots(PolicyCallerHook, roots)
	if err != nil {
		t.Fatalf("ImplicitRepoRegistrationAllowedFromRoots: %v", err)
	}
	if decision.Allowed {
		t.Fatalf("Allowed = true, want hook-safe skip")
	}
	if decision.SkippedReason != SkipReasonPolicyError {
		t.Fatalf("SkippedReason = %q, want %q", decision.SkippedReason, SkipReasonPolicyError)
	}
}

func TestImplicitRepoRegistrationAllowed_MalformedEnv(t *testing.T) {
	roots := testRoots(t)
	t.Setenv(EnvRepoAutodiscovery, "maybe")

	if _, err := ImplicitRepoRegistrationAllowedFromRoots(PolicyCallerManual, roots); err == nil {
		t.Fatalf("expected malformed env error")
	}

	decision, err := ImplicitRepoRegistrationAllowedFromRoots(PolicyCallerHook, roots)
	if err != nil {
		t.Fatalf("hook caller should skip malformed env without error: %v", err)
	}
	if decision.Allowed || decision.SkippedReason != SkipReasonPolicyError {
		t.Fatalf("decision = %+v, want hook-safe policy error skip", decision)
	}
}

func testRoots(t *testing.T) paths.Roots {
	t.Helper()
	t.Setenv(EnvRepoAutodiscovery, "")
	base := t.TempDir()
	return paths.Roots{
		State:  filepath.Join(base, "state"),
		Share:  filepath.Join(base, "share"),
		Config: filepath.Join(base, "config"),
	}
}

func writeConfig(t *testing.T, roots paths.Roots, body string) {
	t.Helper()
	if err := os.MkdirAll(roots.Config, 0o700); err != nil {
		t.Fatalf("mkdir config: %v", err)
	}
	if err := os.WriteFile(roots.ConfigPath(), []byte(body), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
}
