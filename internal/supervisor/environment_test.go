package supervisor

import (
	"reflect"
	"slices"
	"testing"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/paths"
)

func TestWorkerEnvironmentKeepsRuntimeOverridesInMemory(t *testing.T) {
	got := WorkerEnvironment([]string{
		"HOME=/private/home",
		"PATH=/test/bin:/usr/bin",
		"ACD_AI_PROVIDER=openai-compat",
		"ACD_AI_API_KEY=secret-test-value",
		"UNRELATED=value",
		"MALFORMED",
	})
	want := map[string]string{
		"PATH":            "/test/bin:/usr/bin",
		"ACD_AI_PROVIDER": "openai-compat",
		"ACD_AI_API_KEY":  "secret-test-value",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("environment=%v want %v", got, want)
	}
	if !ValidWorkerEnvironment(got) {
		t.Fatalf("filtered environment rejected: %v", got)
	}
	if ValidWorkerEnvironment(map[string]string{"HOME": "/private/home"}) {
		t.Fatal("unapproved environment key accepted")
	}
	if ValidWorkerEnvironment(map[string]string{supervisorOwnershipEnv: "session"}) {
		t.Fatal("internal ownership marker accepted as worker configuration")
	}
}

func TestSessionProcessEnvironmentDropsUnrelatedSecrets(t *testing.T) {
	roots := paths.Roots{State: "/tmp/state/acd", Share: "/tmp/share/acd", Config: "/tmp/config/acd"}
	got := ProcessEnvironment(roots, []string{
		"HOME=/Users/test", "PATH=/usr/bin", "ACD_AI_PROVIDER=deterministic",
		"AWS_SECRET_ACCESS_KEY=do-not-inherit", "SSH_AUTH_SOCK=/tmp/agent.sock",
	})
	for _, want := range []string{
		"HOME=/Users/test", "PATH=/usr/bin", "ACD_AI_PROVIDER=deterministic",
		"XDG_STATE_HOME=/tmp/state", "XDG_DATA_HOME=/tmp/share", "XDG_CONFIG_HOME=/tmp/config",
	} {
		if !slices.Contains(got, want) {
			t.Fatalf("process environment missing %q: %v", want, got)
		}
	}
	for _, unexpected := range []string{"AWS_SECRET_ACCESS_KEY=do-not-inherit", "SSH_AUTH_SOCK=/tmp/agent.sock"} {
		if slices.Contains(got, unexpected) {
			t.Fatalf("process environment retained %q", unexpected)
		}
	}
	if got := sessionProcessEnvironment(roots, []string{"HOME=/Users/test"}, "user:501"); !slices.Contains(got, supervisorOwnershipEnv+"=user:501") {
		t.Fatalf("session environment missing ownership attestation: %v", got)
	}
}
