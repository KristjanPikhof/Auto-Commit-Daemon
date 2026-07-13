package settingsui

import (
	"os"
	"strings"

	"charm.land/lipgloss/v2"
)

type Theme struct {
	Enabled                            bool
	Title, Focus, Dim, Good, Warn, Bad lipgloss.Style
}

func NewTheme(noColor bool) Theme {
	return NewThemeForBackground(noColor, true)
}

func NewThemeForBackground(noColor, dark bool) Theme {
	enabled := !noColor && os.Getenv("NO_COLOR") == "" && strings.ToLower(os.Getenv("TERM")) != "dumb"
	t := Theme{Enabled: enabled}
	if enabled {
		accent, focus, dim := "63", "39", "245"
		if !dark {
			accent, focus, dim = "55", "25", "240"
		}
		t.Title = lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color(accent))
		t.Focus = lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color(focus))
		t.Dim = lipgloss.NewStyle().Foreground(lipgloss.Color(dim))
		t.Good = lipgloss.NewStyle().Foreground(lipgloss.Color("42"))
		t.Warn = lipgloss.NewStyle().Foreground(lipgloss.Color("214"))
		t.Bad = lipgloss.NewStyle().Foreground(lipgloss.Color("196"))
	}
	return t
}

func (t Theme) render(style lipgloss.Style, s string) string {
	if !t.Enabled {
		return s
	}
	return style.Render(s)
}
