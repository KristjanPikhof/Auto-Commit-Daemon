package settingsui

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"charm.land/bubbles/v2/textinput"
	tea "charm.land/bubbletea/v2"
)

type Mode int

const (
	ModeBrowse Mode = iota
	ModeEdit
	ModeSearch
	ModeConfirmQuit
	ModeConfirmApply
	ModeConfirmRisk
)

type Operation struct {
	ID     uint64
	Name   string
	Cancel context.CancelFunc
	Run    func(context.Context) (any, error)
}

type ConfirmationPrompt struct {
	Operation    string
	Requirements []ConfirmationRequirement
	Retry        func(context.Context) (any, error)
}

type Model struct {
	backend Backend
	keys    KeyMap

	Snapshot         Snapshot
	Draft            map[string]string
	Dirty            map[string]bool
	Test             TestResult
	TestFingerprint  string
	PendingRevision  int64
	AppliedRevision  int64
	Operation        *Operation
	Confirmation     *ConfirmationPrompt
	Experiment       Experiment
	Focus            int
	Mode             Mode
	Width, Height    int
	Search           string
	Status           string
	Err              string
	ExperimentBudget int
	ExperimentExpiry int
	ExperimentPolicy string
	ProfileIndex     int

	input         textinput.Model
	nextOperation uint64
	quitting      bool
	accessible    bool
	noColor       bool
}

func New(backend Backend) Model {
	in := textinput.New()
	in.CharLimit = 512
	in.SetWidth(42)
	return Model{backend: backend, keys: DefaultKeyMap(), Draft: map[string]string{}, Dirty: map[string]bool{}, input: in, Width: 80, Height: 24,
		ExperimentBudget: 10, ExperimentPolicy: "continue"}
}

func (m Model) Init() tea.Cmd { return m.snapshotCmd() }

func (m Model) ActiveField() FieldDescriptor {
	fields := visibleFields(m.Search)
	if len(fields) == 0 {
		return FieldDescriptor{}
	}
	if m.Focus < 0 {
		m.Focus = 0
	}
	if m.Focus >= len(fields) {
		m.Focus = len(fields) - 1
	}
	return fields[m.Focus]
}

func (m Model) IsDirty() bool { return len(m.Dirty) > 0 }

func (m Model) Fingerprint() string {
	keys := make([]string, 0, len(m.Draft))
	for key := range m.Draft {
		if !descriptor(key).Sensitive {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	var b strings.Builder
	for _, key := range keys {
		fmt.Fprintf(&b, "%s=%s\n", key, m.Draft[key])
	}
	return b.String()
}

func (m Model) snapshotCmd() tea.Cmd {
	if m.backend == nil {
		return nil
	}
	return func() tea.Msg {
		s, err := m.backend.Snapshot(context.Background())
		return snapshotMsg{Snapshot: s, err: err}
	}
}
