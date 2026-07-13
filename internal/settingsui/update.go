package settingsui

import (
	"context"
	"errors"
	"strings"

	"charm.land/bubbles/v2/key"
	tea "charm.land/bubbletea/v2"
)

type snapshotMsg struct {
	Snapshot Snapshot
	err      error
}
type operationMsg struct {
	id     uint64
	name   string
	result any
	err    error
}
type PollMsg struct{}

func (m Model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.Width, m.Height = msg.Width, msg.Height
		m.input.SetWidth(max(12, min(48, msg.Width-12)))
		return m, nil
	case snapshotMsg:
		if msg.err != nil {
			m.Err = safeText(msg.err.Error())
			return m, nil
		}
		m.Snapshot = sanitizeSnapshot(msg.Snapshot)
		m.PendingRevision, m.AppliedRevision, m.Experiment = m.Snapshot.DesiredRevision, m.Snapshot.AppliedRevision, m.Snapshot.Experiment
		if len(m.Draft) == 0 {
			for _, f := range m.Snapshot.Fields {
				if !descriptor(f.Key).Sensitive {
					m.Draft[f.Key] = f.Value
				}
			}
		}
		return m, nil
	case PollMsg:
		return m, m.snapshotCmd()
	case operationMsg:
		if m.Operation == nil || msg.id != m.Operation.ID {
			return m, nil
		}
		m.Operation = nil
		if msg.err != nil {
			m.Err = safeText(msg.err.Error())
			m.Status = "FAILED: " + m.Err
			return m, nil
		}
		switch result := msg.result.(type) {
		case TestResult:
			m.Test = sanitizeTest(result)
			m.TestFingerprint = m.Fingerprint()
			if m.Test.OK {
				m.Status = "TESTED: " + m.Test.Summary
			} else {
				m.Status = "FAILED: " + m.Test.Summary
			}
		case ApplyResult:
			m.PendingRevision = result.DesiredRevision
			m.AppliedRevision = result.AppliedRevision
			if result.Queued {
				m.Status = "QUEUED: " + safeText(result.Summary)
			} else {
				m.Status = "ACTIVE: " + safeText(result.Summary)
				m.Dirty = map[string]bool{}
			}
		case Experiment:
			m.Experiment = result
			m.Status = "EXPERIMENT: bounded run started"
		}
		return m, m.snapshotCmd()
	case tea.KeyPressMsg:
		return m.updateKey(msg)
	}
	return m, nil
}

func (m Model) updateKey(msg tea.KeyPressMsg) (tea.Model, tea.Cmd) {
	if m.Mode == ModeEdit || m.Mode == ModeSearch {
		if key.Matches(msg, m.keys.Cancel) {
			m.Mode = ModeBrowse
			m.input.Blur()
			return m, nil
		}
		if msg.String() == "enter" {
			if m.Mode == ModeSearch {
				m.Search = safeText(m.input.Value())
				m.Focus = 0
			} else if f := m.ActiveField(); f.Key != "" && !f.Sensitive {
				value := safeText(m.input.Value())
				m.Draft[f.Key] = value
				m.Dirty[f.Key] = true
				m.TestFingerprint = ""
				m.Test = TestResult{}
			}
			m.Mode = ModeBrowse
			m.input.Blur()
			return m, nil
		}
		var cmd tea.Cmd
		m.input, cmd = m.input.Update(msg)
		return m, cmd
	}
	if m.Mode == ModeConfirmQuit {
		if key.Matches(msg, m.keys.Discard) || key.Matches(msg, m.keys.Confirm) {
			m.quitting = true
			return m, tea.Quit
		}
		if key.Matches(msg, m.keys.Cancel) {
			m.Mode = ModeBrowse
		}
		return m, nil
	}
	if m.Mode == ModeConfirmApply {
		if key.Matches(msg, m.keys.Confirm) {
			m.Mode = ModeBrowse
			return m, m.start("apply", func(ctx context.Context) (any, error) {
				return m.backend.Apply(ctx, sanitizedDraft(m.Draft), m.Fingerprint())
			})
		}
		if key.Matches(msg, m.keys.Cancel) {
			m.Mode = ModeBrowse
		}
		return m, nil
	}
	if key.Matches(msg, m.keys.Quit) {
		if m.IsDirty() {
			m.Mode = ModeConfirmQuit
			return m, nil
		}
		m.quitting = true
		return m, tea.Quit
	}
	if key.Matches(msg, m.keys.Cancel) && m.Operation != nil {
		m.Operation.Cancel()
		m.Status = "CANCELLED: " + m.Operation.Name
		m.Operation = nil
		return m, nil
	}
	if m.Operation != nil {
		return m, nil
	}
	fields := visibleFields(m.Search)
	switch {
	case key.Matches(msg, m.keys.Up):
		if m.Focus > 0 {
			m.Focus--
		}
	case key.Matches(msg, m.keys.Down):
		if m.Focus+1 < len(fields) {
			m.Focus++
		}
	case key.Matches(msg, m.keys.Search):
		m.Mode = ModeSearch
		m.input.SetValue(m.Search)
		return m, m.input.Focus()
	case key.Matches(msg, m.keys.Edit):
		f := m.ActiveField()
		if f.Key != "" && !f.Sensitive {
			m.Mode = ModeEdit
			m.input.SetValue(m.Draft[f.Key])
			return m, m.input.Focus()
		}
	case key.Matches(msg, m.keys.Test):
		return m, m.start("test", func(ctx context.Context) (any, error) { return m.backend.Test(ctx, sanitizedDraft(m.Draft)) })
	case key.Matches(msg, m.keys.Apply):
		if !m.IsDirty() {
			m.Status = "DRAFT: no changes"
		} else if !m.Test.OK || m.TestFingerprint != m.Fingerprint() {
			m.Status = "DRAFT: test current changes before apply"
		} else {
			m.Mode = ModeConfirmApply
		}
	case key.Matches(msg, m.keys.Revert):
		return m, m.start("revert", func(ctx context.Context) (any, error) { return m.backend.Revert(ctx, m.Snapshot.LastKnownGood) })
	case key.Matches(msg, m.keys.Experiment):
		return m, m.start("experiment", func(ctx context.Context) (any, error) {
			return m.backend.StartExperiment(ctx, sanitizedDraft(m.Draft), 10)
		})
	}
	return m, nil
}

func (m *Model) start(name string, fn func(context.Context) (any, error)) tea.Cmd {
	if m.backend == nil {
		return func() tea.Msg { return operationMsg{err: errors.New("settings backend unavailable")} }
	}
	m.nextOperation++
	id := m.nextOperation
	ctx, cancel := context.WithCancel(context.Background())
	m.Operation = &Operation{ID: id, Name: name, Cancel: cancel}
	m.Status = "WORKING: " + strings.ToUpper(name)
	return func() tea.Msg {
		result, err := fn(ctx)
		return operationMsg{id: id, name: name, result: result, err: err}
	}
}

func sanitizeSnapshot(s Snapshot) Snapshot {
	s.PendingError = safeText(s.PendingError)
	s.Profile = safeText(s.Profile)
	for i := range s.Fields {
		s.Fields[i].Key = safeText(s.Fields[i].Key)
		s.Fields[i].Source = safeText(s.Fields[i].Source)
		s.Fields[i].Shadowed = safeText(s.Fields[i].Shadowed)
		if s.Fields[i].SensitiveSet || descriptor(s.Fields[i].Key).Sensitive {
			s.Fields[i].Value = ""
		} else {
			s.Fields[i].Value = safeText(s.Fields[i].Value)
		}
	}
	return s
}
func sanitizeTest(r TestResult) TestResult {
	r.Fingerprint = safeText(r.Fingerprint)
	r.Summary = safeText(r.Summary)
	return r
}
