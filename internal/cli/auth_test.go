package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/credentials"
)

func executeAuth(t *testing.T, input string, args ...string) (string, string, error) {
	t.Helper()
	root := newRootCmd()
	var out, errOut bytes.Buffer
	root.SetOut(&out)
	root.SetErr(&errOut)
	root.SetIn(strings.NewReader(input))
	root.SetArgs(args)
	err := root.ExecuteContext(context.Background())
	return out.String(), errOut.String(), err
}

func TestAuthSetStatusAndRemove(t *testing.T) {
	roots := withIsolatedHome(t)
	t.Setenv("ACD_AI_API_KEY", "")
	out, errOut, err := executeAuth(t, "sk-cli-secret\n", "auth", "set", "--stdin")
	if err != nil || errOut != "" || strings.Contains(out, "sk-cli-secret") {
		t.Fatalf("auth set out=%q errOut=%q err=%v", out, errOut, err)
	}
	store := credentials.NewStore(roots)
	key, err := store.Read()
	if err != nil || key != "sk-cli-secret" {
		t.Fatalf("stored key set=%v err=%v", key != "", err)
	}

	out, _, err = executeAuth(t, "", "auth", "status", "--json")
	if err != nil || strings.Contains(out, "sk-cli-secret") {
		t.Fatalf("auth status out=%q err=%v", out, err)
	}
	var envelope struct {
		Data authStatusReport `json:"data"`
	}
	if err := json.Unmarshal([]byte(out), &envelope); err != nil {
		t.Fatal(err)
	}
	status := envelope.Data
	if status.Source != credentials.SourceFile || !status.ProtectedFileSet || status.EnvironmentSet {
		t.Fatalf("status = %+v", status)
	}

	if _, _, err := executeAuth(t, "", "auth", "remove"); err == nil {
		t.Fatal("auth remove succeeded without --yes")
	}
	out, _, err = executeAuth(t, "", "auth", "remove", "--yes")
	if err != nil || !strings.Contains(out, "Removed") {
		t.Fatalf("auth remove out=%q err=%v", out, err)
	}
}

func TestAuthStatusEnvironmentPriorityIsSecretFree(t *testing.T) {
	roots := withIsolatedHome(t)
	if err := credentials.NewStore(roots).Set("sk-file-secret"); err != nil {
		t.Fatal(err)
	}
	t.Setenv("ACD_AI_API_KEY", "sk-env-secret")
	out, _, err := executeAuth(t, "", "--json", "auth", "status")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(out, "sk-file-secret") || strings.Contains(out, "sk-env-secret") {
		t.Fatalf("status leaked secret: %s", out)
	}
	var envelope struct {
		Data authStatusReport `json:"data"`
	}
	if err := json.Unmarshal([]byte(out), &envelope); err != nil {
		t.Fatal(err)
	}
	status := envelope.Data
	if status.Source != credentials.SourceEnvironment || !status.EnvironmentSet || !status.ProtectedFileSet {
		t.Fatalf("status = %+v", status)
	}
}

func TestAuthSetRejectsLiteralAndUnmaskedNonTerminalInput(t *testing.T) {
	withIsolatedHome(t)
	if _, _, err := executeAuth(t, "", "auth", "set", "sk-literal"); err == nil {
		t.Fatal("literal positional secret accepted")
	}
	if _, _, err := executeAuth(t, "sk-unmasked\n", "auth", "set"); err == nil ||
		!strings.Contains(err.Error(), "--stdin") {
		t.Fatalf("unmasked non-terminal error = %v", err)
	}
	root := newRootCmd()
	auth, _, err := root.Find([]string{"auth", "set"})
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"key", "token", "secret", "api-key"} {
		if auth.Flags().Lookup(name) != nil {
			t.Fatalf("literal secret flag --%s exists", name)
		}
	}
}

func TestAuthStatusRejectsInsecureFileWithoutLeakingBody(t *testing.T) {
	roots := withIsolatedHome(t)
	t.Setenv("ACD_AI_API_KEY", "")
	if err := os.MkdirAll(roots.Config, 0o700); err != nil {
		t.Fatal(err)
	}
	body := []byte(`{"version":1,"openai_compat_api_key":"sk-never-print"}`)
	if err := os.WriteFile(credentials.NewStore(roots).Path(), body, 0o644); err != nil {
		t.Fatal(err)
	}
	out, errOut, err := executeAuth(t, "", "auth", "status", "--json")
	if err == nil || strings.Contains(out+errOut+err.Error(), "sk-never-print") {
		t.Fatalf("status out=%q errOut=%q err=%v", out, errOut, err)
	}
}
