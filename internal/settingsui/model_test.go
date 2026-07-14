package settingsui

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	tea "charm.land/bubbletea/v2"
)

type fakeBackend struct {
	snapshot  Snapshot
	tested    map[string]string
	applied   map[string]string
	blocked   chan struct{}
	missing   []ConfirmationRequirement
	confirmed []ConfirmationRequirement
	testCalls int
}

func (f *fakeBackend) Snapshot(context.Context) (Snapshot, error) { return f.snapshot, nil }
func (f *fakeBackend) Save(_ context.Context, v map[string]string) (ApplyResult, error) {
	f.applied = v
	return ApplyResult{SavedOnly: true, Summary: "draft saved"}, nil
}
func (f *fakeBackend) Test(ctx context.Context, v map[string]string) (TestResult, error) {
	f.testCalls++
	f.tested = v
	if len(f.missing) > 0 {
		return TestResult{}, &ConfirmationRequiredError{Missing: append([]ConfirmationRequirement(nil), f.missing...)}
	}
	if f.blocked != nil {
		select {
		case <-f.blocked:
		case <-ctx.Done():
			return TestResult{}, ctx.Err()
		}
	}
	return TestResult{OK: true, Summary: "synthetic request passed"}, nil
}
func (f *fakeBackend) Confirm(requirements []ConfirmationRequirement) {
	f.confirmed = append(f.confirmed, requirements...)
	f.missing = nil
}
func (f *fakeBackend) Apply(_ context.Context, v map[string]string, _ string) (ApplyResult, error) {
	f.applied = v
	return ApplyResult{DesiredRevision: 8, AppliedRevision: 7, Queued: true, Summary: "next safe boundary"}, nil
}
func (f *fakeBackend) Revert(context.Context, int64) (ApplyResult, error) {
	return ApplyResult{DesiredRevision: 9, AppliedRevision: 7, Queued: true, Summary: "revert queued"}, nil
}
func (f *fakeBackend) StartExperiment(context.Context, map[string]string, ExperimentOptions) (Experiment, error) {
	return Experiment{ID: 3, Active: true, TotalWindows: 10}, nil
}
func (f *fakeBackend) CancelExperiment(context.Context, int64) (ApplyResult, error) {
	return ApplyResult{DesiredRevision: 10, Queued: true, Summary: "experiment cancellation queued"}, nil
}
func (f *fakeBackend) SelectProfile(_ context.Context, profile string) (ApplyResult, error) {
	f.snapshot.Profile = profile
	return ApplyResult{Summary: "repository profile selected"}, nil
}

func baseSnapshot() Snapshot {
	return Snapshot{ActiveRevision: 7, DesiredRevision: 7, AppliedRevision: 7, LastKnownGood: 6, DaemonRunning: true, Profile: "daily", Fields: []FieldValue{{Key: "ai.provider", Value: "openai-compat", ActiveValue: "deterministic", Source: "environment"}, {Key: "ai.model", Value: "old-model", ActiveValue: "active-model", Source: "profile", Shadowed: "environment: env-model"}, {Key: "ai.api_key", Value: "top-secret", Source: "environment", SensitiveSet: true}}}
}

func keyMsg(s string) tea.KeyPressMsg {
	if s == "enter" {
		return tea.KeyPressMsg{Code: tea.KeyEnter}
	}
	if s == "esc" {
		return tea.KeyPressMsg{Code: tea.KeyEscape}
	}
	return tea.KeyPressMsg{Code: []rune(s)[0], Text: s}
}

func updated(t *testing.T, m Model, msg tea.Msg) (Model, tea.Cmd) {
	t.Helper()
	next, cmd := m.Update(msg)
	got, ok := next.(Model)
	if !ok {
		t.Fatalf("model type %T", next)
	}
	return got, cmd
}
func runCmd(t *testing.T, cmd tea.Cmd) tea.Msg {
	t.Helper()
	if cmd == nil {
		t.Fatal("expected command")
	}
	return cmd()
}

func TestModelUpdateLifecycle(t *testing.T) {
	b := &fakeBackend{snapshot: baseSnapshot()}
	m := New(b)
	var cmd tea.Cmd
	m, cmd = updated(t, m, snapshotMsg{Snapshot: b.snapshot})
	if cmd != nil {
		t.Fatal("snapshot should not block or schedule work")
	}
	m, cmd = updated(t, m, keyMsg("enter"))
	if m.Mode != ModeEdit {
		t.Fatalf("mode=%v", m.Mode)
	}
	m.input.SetValue("new-provider")
	m, _ = updated(t, m, keyMsg("enter"))
	if !m.Dirty["ai.provider"] || m.TestFingerprint != "" {
		t.Fatal("edit must dirty and invalidate test")
	}
	m, cmd = updated(t, m, keyMsg("t"))
	if m.Operation == nil || m.Operation.Name != "test" {
		t.Fatal("test operation missing")
	}
	focus := m.Focus
	m, _ = updated(t, m, runCmd(t, cmd))
	if !m.Test.OK || m.Focus != focus || m.TestFingerprint != m.Fingerprint() {
		t.Fatal("test result or focus mismatch")
	}
	m, _ = updated(t, m, keyMsg("a"))
	if m.Mode != ModeConfirmApply {
		t.Fatalf("apply mode=%v", m.Mode)
	}
	m, cmd = updated(t, m, keyMsg("y"))
	m, _ = updated(t, m, runCmd(t, cmd))
	if m.PendingRevision != 8 || m.AppliedRevision != 7 || m.Focus != focus {
		t.Fatalf("queued state=%+v", m)
	}
	if b.applied["ai.provider"] != "new-provider" {
		t.Fatalf("apply draft=%v", b.applied)
	}
	if _, ok := b.applied["ai.api_key"]; ok {
		t.Fatal("sensitive value reached backend")
	}
}

func TestModelSavedDraftRemainsAvailableForActivation(t *testing.T) {
	b := &fakeBackend{snapshot: baseSnapshot()}
	m := New(b)
	m, _ = updated(t, m, snapshotMsg{Snapshot: b.snapshot})
	m.Dirty["ai.model"] = true

	m, cmd := updated(t, m, keyMsg("s"))
	m, _ = updated(t, m, runCmd(t, cmd))
	if m.IsDirty() || !strings.HasPrefix(m.Status, "SAVED:") {
		t.Fatalf("saved state=%+v", m)
	}

	m, cmd = updated(t, m, keyMsg("t"))
	m, _ = updated(t, m, runCmd(t, cmd))
	m, _ = updated(t, m, keyMsg("a"))
	if m.Mode != ModeConfirmApply {
		t.Fatalf("saved draft cannot be activated: mode=%v status=%q", m.Mode, m.Status)
	}
}

func TestUpdatePollingPreservesFocusAndDirtyDraft(t *testing.T) {
	b := &fakeBackend{snapshot: baseSnapshot()}
	m := New(b)
	m, _ = updated(t, m, snapshotMsg{Snapshot: b.snapshot})
	m.Focus = 1
	m.Draft["ai.model"] = "draft-model"
	m.Dirty["ai.model"] = true
	b.snapshot.DesiredRevision = 8
	m, cmd := updated(t, m, PollMsg{})
	m, _ = updated(t, m, runCmd(t, cmd))
	if m.Focus != 1 || m.Draft["ai.model"] != "draft-model" || m.PendingRevision != 8 {
		t.Fatalf("poll moved local state: %+v", m)
	}
}

func TestUpdateRejectRevertExperimentAndCancellation(t *testing.T) {
	b := &fakeBackend{snapshot: baseSnapshot()}
	m := New(b)
	m, _ = updated(t, m, snapshotMsg{Snapshot: b.snapshot})
	m, _ = updated(t, m, operationMsg{id: 999, err: errors.New("stale")})
	if m.Err != "" {
		t.Fatal("stale result applied")
	}
	m, cmd := updated(t, m, keyMsg("r"))
	m, _ = updated(t, m, runCmd(t, cmd))
	if m.PendingRevision != 9 {
		t.Fatal("revert not queued")
	}
	m, cmd = updated(t, m, keyMsg("x"))
	m, _ = updated(t, m, runCmd(t, cmd))
	if !m.Experiment.Active || m.Experiment.TotalWindows != 10 {
		t.Fatal("experiment not started")
	}
	b.blocked = make(chan struct{})
	m, cmd = updated(t, m, keyMsg("t"))
	if cmd == nil {
		t.Fatal("missing async command")
	}
	m, _ = updated(t, m, keyMsg("esc"))
	if m.Operation != nil || m.Status != "CANCELLED: test" {
		t.Fatalf("cancel=%+v", m)
	}
}

func TestKeymapKeyboardOnlyDirtyQuit(t *testing.T) {
	m := New(&fakeBackend{})
	m.Dirty["ai.model"] = true
	m, _ = updated(t, m, keyMsg("q"))
	if m.Mode != ModeConfirmQuit {
		t.Fatal("dirty quit did not confirm")
	}
	m, cmd := updated(t, m, keyMsg("esc"))
	if cmd != nil || m.Mode != ModeBrowse {
		t.Fatal("escape did not resume")
	}
	m, _ = updated(t, m, keyMsg("q"))
	_, cmd = updated(t, m, keyMsg("d"))
	if cmd == nil {
		t.Fatal("discard quit missing")
	}
}

func TestModelSnapshotSanitizesTerminalContent(t *testing.T) {
	m := New(nil)
	s := baseSnapshot()
	s.PendingError = "bad\x1b[31m red\x1b[0m\x00"
	s.Profile = "x\x1b]0;owned\x07"
	m, _ = updated(t, m, snapshotMsg{Snapshot: s})
	if m.Snapshot.PendingError != "bad red" || m.Snapshot.Profile != "x" {
		t.Fatalf("unsanitized: %#v %#v", m.Snapshot.PendingError, m.Snapshot.Profile)
	}
	if m.Draft["ai.api_key"] != "" {
		t.Fatal("secret copied into draft")
	}
}

func TestModelOperationErrorSanitized(t *testing.T) {
	m := New(&fakeBackend{})
	m.Operation = &Operation{ID: 1, Name: "test"}
	m, _ = updated(t, m, operationMsg{id: 1, err: errors.New("oops\x1b[2J\x00")})
	if m.Err != "oops" {
		t.Fatalf("error=%q", m.Err)
	}
}

func TestModelConfirmationRetriesOnceAndClearsFailure(t *testing.T) {
	requirement := ConfirmationRequirement{ID: "endpoint_credentials", Label: "send credentials to a non-default endpoint"}
	b := &fakeBackend{snapshot: baseSnapshot(), missing: []ConfirmationRequirement{requirement}}
	m := New(b)
	m, _ = updated(t, m, snapshotMsg{Snapshot: b.snapshot})
	m.Err = "old failure"
	m.Status = "FAILED: old failure"

	m, cmd := updated(t, m, keyMsg("T"))
	if m.Err != "" || m.Operation == nil {
		t.Fatalf("operation did not clear stale error: %+v", m)
	}
	m, _ = updated(t, m, runCmd(t, cmd))
	if m.Mode != ModeConfirmRisk || m.Confirmation == nil || b.testCalls != 1 || m.Err != "" {
		t.Fatalf("confirmation state=%+v calls=%d", m, b.testCalls)
	}
	if got := m.Render(); !strings.Contains(got, requirement.Label) || strings.Contains(got, "old failure") {
		t.Fatalf("confirmation render=%q", got)
	}
	m, cmd = updated(t, m, keyMsg("y"))
	if cmd == nil || len(b.confirmed) != 1 {
		t.Fatalf("confirmation not recorded: %+v", b.confirmed)
	}
	m, _ = updated(t, m, runCmd(t, cmd))
	if b.testCalls != 2 || !m.Test.OK || m.Err != "" || m.Mode != ModeBrowse || lifecycle(m) != "TESTED" {
		t.Fatalf("retry state=%+v calls=%d", m, b.testCalls)
	}
}

func TestModelConfirmationCancelDoesNotRetry(t *testing.T) {
	b := &fakeBackend{snapshot: baseSnapshot(), missing: []ConfirmationRequirement{{ID: "subprocess", Label: "execute provider subprocess"}}}
	m := New(b)
	m, _ = updated(t, m, snapshotMsg{Snapshot: b.snapshot})
	m, cmd := updated(t, m, keyMsg("t"))
	m, _ = updated(t, m, runCmd(t, cmd))
	m, cmd = updated(t, m, keyMsg("esc"))
	if cmd != nil || b.testCalls != 1 || len(b.confirmed) != 0 || m.Mode != ModeBrowse || m.Status != "CANCELLED: risk confirmation" {
		t.Fatalf("cancel state=%+v calls=%d confirmed=%v", m, b.testCalls, b.confirmed)
	}
}

func TestLifecycleRejectsFreshAndRejectedFalseLabels(t *testing.T) {
	fresh := New(nil)
	if got := lifecycle(fresh); got != "DRAFT" {
		t.Fatalf("fresh lifecycle=%q", got)
	}
	rejected := New(nil)
	rejected.PendingRevision, rejected.AppliedRevision = 2, 1
	rejected.Snapshot.PendingStatus = "rejected"
	rejected.Snapshot.PendingError = "provider rejected"
	if got := lifecycle(rejected); got != "REJECTED" {
		t.Fatalf("rejected lifecycle=%q", got)
	}
	active := New(nil)
	active.PendingRevision, active.AppliedRevision = 1, 1
	if got := lifecycle(active); got != "ACTIVE" {
		t.Fatalf("active lifecycle=%q", got)
	}
}

func TestSafeTextCollapsesLayoutControls(t *testing.T) {
	if got := safeText("row1\n\trow2\x1b[2J\x00"); got != "row1 row2" {
		t.Fatalf("safeText=%q", got)
	}
}

var _ = time.Second
