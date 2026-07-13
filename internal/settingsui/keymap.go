package settingsui

import "charm.land/bubbles/v2/key"

type KeyMap struct {
	Up, Down, Edit, Search, Test, Apply, Revert, Experiment, Cancel, Quit, Confirm, Discard key.Binding
}

func DefaultKeyMap() KeyMap {
	return KeyMap{
		Up:         key.NewBinding(key.WithKeys("up", "k"), key.WithHelp("up/k", "previous")),
		Down:       key.NewBinding(key.WithKeys("down", "j"), key.WithHelp("down/j", "next")),
		Edit:       key.NewBinding(key.WithKeys("enter", "e"), key.WithHelp("enter/e", "edit")),
		Search:     key.NewBinding(key.WithKeys("/"), key.WithHelp("/", "search")),
		Test:       key.NewBinding(key.WithKeys("t"), key.WithHelp("t", "test")),
		Apply:      key.NewBinding(key.WithKeys("a"), key.WithHelp("a", "apply")),
		Revert:     key.NewBinding(key.WithKeys("r"), key.WithHelp("r", "revert")),
		Experiment: key.NewBinding(key.WithKeys("x"), key.WithHelp("x", "experiment")),
		Cancel:     key.NewBinding(key.WithKeys("esc", "ctrl+c"), key.WithHelp("esc", "cancel")),
		Quit:       key.NewBinding(key.WithKeys("q"), key.WithHelp("q", "quit")),
		Confirm:    key.NewBinding(key.WithKeys("y"), key.WithHelp("y", "confirm")),
		Discard:    key.NewBinding(key.WithKeys("d"), key.WithHelp("d", "discard")),
	}
}
