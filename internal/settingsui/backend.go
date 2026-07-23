package settingsui

import (
	"context"
	"time"
)

// Backend is deliberately UI-shaped. Production services can adapt to it without
// leaking config, state, provider, or daemon types into the state machine.
type Backend interface {
	Snapshot(context.Context) (Snapshot, error)
	Save(context.Context, map[string]string) (ApplyResult, error)
	Test(context.Context, map[string]string) (TestResult, error)
	Apply(context.Context, map[string]string, string) (ApplyResult, error)
	Revert(context.Context, int64) (ApplyResult, error)
	StartExperiment(context.Context, map[string]string, ExperimentOptions) (Experiment, error)
	CancelExperiment(context.Context, int64) (ApplyResult, error)
	SelectProfile(context.Context, string) (ApplyResult, error)
}

type ExperimentOptions struct {
	WindowBudget  int
	ExpiresAfter  time.Duration
	FailurePolicy string
}

type Snapshot struct {
	Fields          []FieldValue
	ActiveRevision  int64
	DesiredRevision int64
	AppliedRevision int64
	LastKnownGood   int64
	PendingSince    time.Time
	PendingError    string
	PendingStatus   string
	DaemonRunning   bool
	Experiment      Experiment
	SavedGeneration uint64
	Profile         string
	Comparison      string
	Profiles        []string
}

type FieldValue struct {
	Key          string
	Value        string
	ActiveValue  string
	Source       string
	Shadowed     string
	Restart      bool
	SensitiveSet bool
}

type TestResult struct {
	Fingerprint string
	Summary     string
	OK          bool
}

type ApplyResult struct {
	DesiredRevision int64
	AppliedRevision int64
	Queued          bool
	Summary         string
}

type Experiment struct {
	ID               int64
	Profile          string
	CompletedWindows int
	TotalWindows     int
	ExpiresAt        time.Time
	Active           bool
	FailurePolicy    string
	Status           string
}
