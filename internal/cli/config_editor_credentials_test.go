package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/ai"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/config"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/credentials"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/settingsui"
)

type configKeyInput struct{ reader *strings.Reader }

func (r *configKeyInput) Read(p []byte) (int, error) {
	if len(p) > 1 {
		p = p[:1]
	}
	return r.reader.Read(p)
}
func configKeyRaw(value string) json.RawMessage {
	body, _ := json.Marshal(value)
	return body
}

func TestConfigEditorIntentPromptsForMissingCredentialBeforeSave(t *testing.T) {
	for _, tc := range []struct {
		name, afterPrompt string
		approve           bool
	}{
		{"save", "y\n", true},
		{"cancel", "n\n9\ny\n", false},
		{"change to local provider", "n\n6\n1\n1\n1\n2\n8\ny\n", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			approve := tc.approve
			e := editorFixture(t)
			e.probe = func(context.Context, ai.ProviderConfig) (ai.ProviderProbeResult, error) {
				t.Fatal("saving settings must not probe the provider")
				return ai.ProviderProbeResult{}, nil
			}
			editorSeed(t, e, func(doc *config.Document) { doc.Settings.Global[config.FieldProvider] = configKeyRaw("openai-compat") })
			input := "6\n2\n8\nsetup-secret\n" + tc.afterPrompt
			var out bytes.Buffer
			err := settingsui.RunEditor(t.Context(), e, settingsui.EditorOptions{Scope: "global", Accessible: true, NoColor: true, Input: &configKeyInput{reader: strings.NewReader(input)}, Output: &out})
			if err != nil {
				t.Fatalf("editor: %v\n%s", err, out.String())
			}
			if !strings.Contains(out.String(), "requires an API key") || strings.Contains(out.String(), "setup-secret") {
				t.Fatalf("missing required prompt or secret leaked: %s", out.String())
			}
			key, err := credentials.NewStore(e.roots).Read()
			if err != nil && (approve || !errors.Is(err, credentials.ErrNotFound)) {
				t.Fatal(err)
			}
			if (key != "") != approve || (approve && key != "setup-secret") {
				t.Fatalf("credential presence=%v want=%v", key != "", approve)
			}
			snapshot, err := e.Load(t.Context(), "global", nil)
			if err != nil {
				t.Fatal(err)
			}
			for _, field := range snapshot.Fields {
				if field.Key == config.FieldDiffEgress && approve && field.Value != "true" {
					t.Fatal("Intent selection did not save approved code sharing")
				}
				if field.Key == config.FieldProvider && tc.name == "change to local provider" && field.Value != "deterministic" {
					t.Fatalf("provider=%s want=deterministic", field.Value)
				}
				if field.Key == config.FieldCommitStrategy {
					want := "local"
					if approve {
						want = "intent"
					}
					if field.Value != want {
						t.Fatalf("mode=%s want=%s", field.Value, want)
					}
				}
			}
		})
	}
}

func TestConfigEditorCredentialRequirementFollowsModeAndProvider(t *testing.T) {
	for _, tc := range []struct {
		name, mode, provider, source string
		want                         bool
	}{
		{"missing API key", "intent", "openai-compat", "", true},
		{"saved API key", "intent", "openai-compat", "file", false},
		{"environment API key", "intent", "openai-compat", "environment", false},
		{"local", "local", "openai-compat", "", false},
		{"subprocess", "intent", "subprocess:test", "", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := editorFixture(t)
			editorSeed(t, e, func(doc *config.Document) {
				doc.Settings.Global[config.FieldProvider] = configKeyRaw(tc.provider)
				doc.Settings.Global[config.FieldCommitStrategy] = configKeyRaw(tc.mode)
			})
			if tc.source == "file" {
				if err := credentials.NewStore(e.roots).Set("saved-secret"); err != nil {
					t.Fatal(err)
				}
			}
			if tc.source == "environment" {
				e.lookup = func(name string) (string, bool) {
					if name == ai.EnvAPIKey {
						return "environment-secret", true
					}
					return "", false
				}
			}
			snapshot, err := e.Load(t.Context(), "global", nil)
			if err != nil {
				t.Fatal(err)
			}
			if snapshot.CredentialMissing != tc.want {
				t.Fatalf("missing=%v want=%v", snapshot.CredentialMissing, tc.want)
			}
		})
	}
}
