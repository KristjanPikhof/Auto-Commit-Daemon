package settingsui

import "charm.land/bubbles/v2/key"

type KeyMap struct {
	Up, Down, Edit, Search, Save, Test, Apply, Revert, Experiment, CancelExperiment key.Binding
	Mode, Profile, Budget, Expiry, Policy, Cancel, Quit, Confirm, Discard           key.Binding
}

func DefaultKeyMap() KeyMap {
	return KeyMap{
		Up:               key.NewBinding(key.WithKeys("up", "k"), key.WithHelp("up/k", "previous")),
		Down:             key.NewBinding(key.WithKeys("down", "j"), key.WithHelp("down/j", "next")),
		Edit:             key.NewBinding(key.WithKeys("enter", "e"), key.WithHelp("enter/e", "edit")),
		Search:           key.NewBinding(key.WithKeys("/"), key.WithHelp("/", "search")),
		Save:             key.NewBinding(key.WithKeys("s"), key.WithHelp("s", "save")),
		Test:             key.NewBinding(key.WithKeys("t", "T"), key.WithHelp("t/T", "test")),
		Apply:            key.NewBinding(key.WithKeys("a"), key.WithHelp("a", "apply")),
		Revert:           key.NewBinding(key.WithKeys("r"), key.WithHelp("r", "revert")),
		Mode:             key.NewBinding(key.WithKeys("m"), key.WithHelp("m", "strategy/preset")),
		Experiment:       key.NewBinding(key.WithKeys("x"), key.WithHelp("x", "experiment")),
		CancelExperiment: key.NewBinding(key.WithKeys("X"), key.WithHelp("X", "cancel experiment")),
		Profile:          key.NewBinding(key.WithKeys("p"), key.WithHelp("p", "select profile")),
		Budget:           key.NewBinding(key.WithKeys("b"), key.WithHelp("b", "experiment budget")),
		Expiry:           key.NewBinding(key.WithKeys("z"), key.WithHelp("z", "experiment expiry")),
		Policy:           key.NewBinding(key.WithKeys("f"), key.WithHelp("f", "failure policy")),
		Cancel:           key.NewBinding(key.WithKeys("esc", "ctrl+c"), key.WithHelp("esc", "cancel")),
		Quit:             key.NewBinding(key.WithKeys("q"), key.WithHelp("q", "quit")),
		Confirm:          key.NewBinding(key.WithKeys("y"), key.WithHelp("y", "confirm")),
		Discard:          key.NewBinding(key.WithKeys("d"), key.WithHelp("d", "discard")),
	}
}
