package supervisor

import (
	"reflect"
	"testing"
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
}
