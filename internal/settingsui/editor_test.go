package settingsui

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/config"
)

type bytewiseReader struct{ r io.Reader }

func (r *bytewiseReader) Read(p []byte) (int, error) {
	if len(p) > 1 {
		p = p[:1]
	}
	return r.r.Read(p)
}

type editorFake struct {
	drafts      []EditorDraft
	saved       int
	failOnce    bool
	reviewError error
}

func (b *editorFake) Load(_ context.Context, scope string, changes map[string]*string) (EditorSnapshot, error) {
	snapshot := EditorSnapshot{Generation: 7, Credential: "Configured"}
	for _, definition := range config.Catalog() {
		if definition.Sensitive || !definition.Persistable {
			continue
		}
		field := EditorField{Key: definition.Name, Value: definition.Default, Source: "global", Inherited: "inherited-value"}
		if scope == "global" {
			field.Inherited = definition.Default
		}
		if field.Key == config.FieldModel {
			field.Value = "old-model"
			field.Overridden = true
		}
		if value, found := changes[field.Key]; found {
			if value == nil {
				field.Value = field.Inherited
				field.Overridden = false
			} else {
				field.Value = *value
				field.Overridden = true
			}
		}
		snapshot.Fields = append(snapshot.Fields, field)
	}
	return snapshot, nil
}
func (b *editorFake) Review(_ context.Context, draft EditorDraft) (EditorReview, error) {
	if b.reviewError != nil {
		return EditorReview{}, b.reviewError
	}
	b.drafts = append(b.drafts, draft)
	return EditorReview{Text: "Scope: " + draft.Scope + "\nAPI key: masked", Save: func(context.Context) (string, error) {
		b.saved++
		if b.failOnce {
			b.failOnce = false
			return "", errors.New("Connection rejected; correct the endpoint")
		}
		return "Settings saved. Waiting to apply.", nil
	}}, nil
}
func runEditorInput(t *testing.T, backend *editorFake, input, repo string) (string, error) {
	t.Helper()
	var out bytes.Buffer
	err := RunEditor(context.Background(), backend, EditorOptions{Input: &bytewiseReader{strings.NewReader(input)}, Output: &out, Accessible: true, NoColor: true, Repo: repo})
	return out.String(), err
}

func TestEditorModelIsEditableAndSaveIsOneReviewedAction(t *testing.T) {
	b := &editorFake{}
	out, err := runEditorInput(t, b, "2\n1\nnew-model\n8\ny\n", "")
	if err != nil {
		t.Fatalf("%v\n%s", err, out)
	}
	if b.saved != 1 || len(b.drafts) != 1 || *b.drafts[0].Changes[config.FieldModel] != "new-model" {
		t.Fatalf("saved=%d drafts=%+v", b.saved, b.drafts)
	}
	for _, want := range []string{"Global defaults", "Model", "Review changes", "Waiting to apply"} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %s: %s", want, out)
		}
	}
	if strings.Contains(out, "\x1b[") {
		t.Fatal("accessible editor emitted control sequences")
	}
}
func TestEditorDeclinedReviewAndDirtyCancelNeverSave(t *testing.T) {
	b := &editorFake{}
	out, err := runEditorInput(t, b, "2\n1\nnew-model\n8\nn\n9\ny\n", "")
	if err != nil || b.saved != 0 {
		t.Fatalf("err=%v saved=%d\n%s", err, b.saved, out)
	}
	if !strings.Contains(out, "Discard unsaved changes") {
		t.Fatal(out)
	}
}
func TestEditorFailedSaveKeepsCredentialAndEditsForRetry(t *testing.T) {
	b := &editorFake{failOnce: true}
	out, err := runEditorInput(t, b, "4\nsecret-test-token\n2\n1\nnew-model\n8\ny\n8\ny\n", "")
	if err != nil || b.saved != 2 || len(b.drafts) != 2 {
		t.Fatalf("err=%v saved=%d\n%s", err, b.saved, out)
	}
	if b.drafts[1].Credential != "secret-test-token" || *b.drafts[1].Changes[config.FieldModel] != "new-model" {
		t.Fatal("retry lost draft")
	}
	if strings.Contains(out, "secret-test-token") || !strings.Contains(out, "Connection rejected") {
		t.Fatalf("credential or error handling: %s", out)
	}
}
func TestEditorSwitchScopeAndResetOneOverride(t *testing.T) {
	b := &editorFake{}
	// Select repository scope, choose Model, use inheritance, then save.
	out, err := runEditorInput(t, b, "1\n2\n3\n2\n9\ny\n", "/test/repo")
	if err != nil || b.saved != 1 {
		t.Fatalf("err=%v\n%s", err, out)
	}
	draft := b.drafts[0]
	value, found := draft.Changes[config.FieldModel]
	if draft.Scope != "repo" || !found || value != nil || len(draft.Changes) != 1 {
		t.Fatalf("draft=%+v", draft)
	}
}
func TestEditorScopeChangeCanKeepUnsavedDraft(t *testing.T) {
	b := &editorFake{}
	out, err := runEditorInput(t, b, "3\n1\nnew-model\n1\n2\nn\n9\ny\n", "/test/repo")
	if err != nil || b.saved != 1 {
		t.Fatalf("err=%v\n%s", err, out)
	}
	if b.drafts[0].Scope != "global" || *b.drafts[0].Changes[config.FieldModel] != "new-model" {
		t.Fatal("scope change discarded draft without approval")
	}
}
func TestEditorSafeTextRemovesTerminalControls(t *testing.T) {
	clean := safeText("ok\x1b[31m\n\r\tbad\x1b[0m")
	if strings.ContainsAny(clean, "\x1b\n\r\t") || !strings.Contains(clean, "ok") {
		t.Fatal(clean)
	}
}

func TestEditorAccessibleReviewErrorIsVisible(t *testing.T) {
	b := &editorFake{reviewError: errors.New("API key is missing; select API key to enter it")}
	out, err := runEditorInput(t, b, "8\n9\n", "")
	if err != nil || b.saved != 0 || !strings.Contains(out, "API key is missing") {
		t.Fatalf("err=%v saved=%d\n%s", err, b.saved, out)
	}
}

func TestEditorGlobalModelResetShowsCurrentDefault(t *testing.T) {
	b := &editorFake{}
	out, err := runEditorInput(t, b, "2\n2\n8\ny\n", "")
	if err != nil {
		t.Fatalf("%v\n%s", err, out)
	}
	if !strings.Contains(out, "Reset to default: gpt-6-luna") {
		t.Fatal(out)
	}
	if b.saved != 1 || len(b.drafts) != 1 {
		t.Fatalf("saved=%d drafts=%+v", b.saved, b.drafts)
	}
	value, found := b.drafts[0].Changes[config.FieldModel]
	if !found || value != nil {
		t.Fatal("reset did not remove the model override")
	}
}
