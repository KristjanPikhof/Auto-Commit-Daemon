package cli

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/ai"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/config"
)

func TestConfigEditorExistingIntentReviewsRequiredSharing(t *testing.T) {
	for _, scope := range []string{"global", "repo"} {
		for _, explicitOff := range []bool{false, true} {
			name := scope + "/propose sharing"
			if explicitOff {
				name = scope + "/keep explicit refusal"
			}
			t.Run(name, func(t *testing.T) {
				e := editorFixture(t)
				if scope == "repo" {
					e.repo = materializeTestRepo(t, false)
				}
				e.lookup = func(key string) (string, bool) {
					if key == ai.EnvAPIKey {
						return "sharing-test-key", true
					}
					return "", false
				}
				e.probe = func(context.Context, ai.ProviderConfig) (ai.ProviderProbeResult, error) {
					t.Fatal("reviewing and saving sharing must not contact the provider")
					return ai.ProviderProbeResult{}, nil
				}
				editorSeed(t, e, func(doc *config.Document) {
					doc.Settings.Global[config.FieldCommitStrategy] = json.RawMessage(`"intent"`)
					doc.Settings.Global[config.FieldProvider] = json.RawMessage(`"openai-compat"`)
					doc.Settings.Global[config.FieldDiffEgress] = json.RawMessage(`false`)
				})
				changes := map[string]*string{}
				if explicitOff {
					changes[config.FieldDiffEgress] = editorString("false")
				}
				draft := editorDraft(t, e, scope, changes)
				review, err := e.Review(t.Context(), draft)
				if explicitOff {
					if err == nil || !strings.Contains(err.Error(), "Intent requires redacted code diffs") || *changes[config.FieldDiffEgress] != "false" {
						t.Fatalf("explicit sharing refusal was overridden: changes=%v err=%v", changes, err)
					}
					return
				}
				if err != nil || !strings.Contains(review.Text, "Allow sending redacted code diffs") {
					t.Fatalf("existing Intent cannot review required sharing: err=%v review=%s", err, review.Text)
				}
				if len(changes) != 0 {
					t.Fatal("review mutated the editor draft before approval")
				}
				doc, err := config.NewStore(e.roots).Load()
				if err != nil || string(doc.Settings.Global[config.FieldDiffEgress]) != "false" {
					t.Fatalf("review changed saved sharing: %v", err)
				}
				if _, err := review.Save(t.Context()); err != nil {
					t.Fatal(err)
				}
				projection, err := e.project(scope, nil)
				if err != nil || projection.values[config.FieldDiffEgress] != "true" {
					t.Fatalf("approved sharing was not saved: %v", err)
				}
			})
		}
	}
}
