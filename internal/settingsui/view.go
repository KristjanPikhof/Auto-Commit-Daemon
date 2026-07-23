package settingsui

import (
	"fmt"
	"os"
	"strings"

	tea "charm.land/bubbletea/v2"
	"github.com/charmbracelet/x/ansi"
)

func (m Model) View() tea.View {
	v := tea.NewView(m.Render())
	v.AltScreen = !m.accessible
	return v
}

func (m Model) Render() string {
	t := NewTheme(m.noColor || m.accessible)
	fields := visibleFields(m.Search)
	if m.Focus >= len(fields) && len(fields) > 0 {
		m.Focus = len(fields) - 1
	}
	header := t.render(t.Title, "ACD SETTINGS") + "  " + lifecycle(m) + fmt.Sprintf("  profile: %s", fallback(m.Snapshot.Profile, "default"))
	draftRail := fmt.Sprintf("DRAFT %s > TESTED %s", mark(m.IsDirty()), mark(m.Test.OK && m.TestFingerprint == m.Fingerprint()))
	applyRail := fmt.Sprintf("QUEUED r%d > ACTIVE r%d", m.PendingRevision, m.AppliedRevision)
	left := []string{"FIELDS"}
	displayFields := fields
	if m.Width >= 70 {
		limit := max(1, m.Height-10)
		if len(displayFields) > limit {
			start := max(0, min(m.Focus-limit/2, len(displayFields)-limit))
			displayFields = displayFields[start : start+limit]
		}
	}
	for _, f := range displayFields {
		prefix := "  "
		if f.Key == m.ActiveField().Key {
			prefix = "> "
		}
		value := m.Draft[f.Key]
		if f.Sensitive {
			value = secretState(m.Snapshot.Fields, f.Key)
		}
		dirty := ""
		if m.Dirty[f.Key] {
			dirty = " *"
		}
		line := fmt.Sprintf("%s%s: %s%s", prefix, f.Label, fallback(value, "inherit"), dirty)
		if f.Key == m.ActiveField().Key {
			line = t.render(t.Focus, line)
		}
		left = append(left, line)
	}
	active := m.ActiveField()
	detail := []string{"DETAILS"}
	if active.Key != "" {
		detail = append(detail, active.Label, active.Description, "Source: "+fieldSource(m.Snapshot.Fields, active.Key), "Changed: "+changed(m, active.Key), "Apply: "+active.Apply)
		detail = append(detail, "Active: "+fieldActive(m.Snapshot.Fields, active.Key), "Draft: "+fieldDraft(m, active))
		if active.Sensitive {
			detail = append(detail, "Value: [set/unset only; never displayed]")
		}
	}
	state := []string{"REVISION", draftRail, applyRail, fmt.Sprintf("Desired: r%d", m.PendingRevision), fmt.Sprintf("Applied: r%d", m.AppliedRevision)}
	expiry := []string{"none", "15m", "1h"}[m.ExperimentExpiry]
	state = append(state, fmt.Sprintf("Experiment setup: %d windows, expiry %s, policy %s", m.ExperimentBudget, expiry, m.ExperimentPolicy))
	if m.Snapshot.PendingError != "" {
		state = append(state, "Failure: "+m.Snapshot.PendingError)
	}
	if m.Experiment.Active {
		state = append(state, fmt.Sprintf("Experiment: %d/%d windows (descriptive)", m.Experiment.CompletedWindows, m.Experiment.TotalWindows))
	}
	if m.Snapshot.Comparison != "" {
		state = append(state, "Comparison: "+m.Snapshot.Comparison)
	}
	if m.Status != "" {
		state = append(state, m.Status)
	}
	body := ""
	switch {
	case m.Width >= 100:
		body = columns(m.Width, strings.Join(left, "\n"), strings.Join(detail, "\n"), strings.Join(state, "\n"))
	case m.Width >= 70:
		combined := append(append(detail, ""), state...)
		body = columns(m.Width, strings.Join(left, "\n"), strings.Join(combined, "\n"))
	default:
		fieldLine := "No matching fields"
		if active.Key != "" {
			value := m.Draft[active.Key]
			if active.Sensitive {
				value = secretState(m.Snapshot.Fields, active.Key)
			}
			fieldLine = "> " + active.Label + ": " + fallback(value, "inherit")
		}
		compact := []string{
			"FIELD " + fmt.Sprintf("%d/%d", min(m.Focus+1, len(fields)), len(fields)),
			fieldLine,
			"Source: " + fieldSource(m.Snapshot.Fields, active.Key),
			"Changed: " + changed(m, active.Key),
			"Apply: " + fallback(active.Apply, "next safe boundary"),
			"Active: " + fieldActive(m.Snapshot.Fields, active.Key),
			"Draft: " + fieldDraft(m, active),
			"REVISION", draftRail, applyRail,
		}
		if m.Experiment.Active {
			compact = append(compact, fmt.Sprintf("Experiment: %d/%d windows (descriptive)", m.Experiment.CompletedWindows, m.Experiment.TotalWindows))
		}
		if m.Snapshot.Comparison != "" {
			compact = append(compact, "Comparison: "+m.Snapshot.Comparison)
		}
		if m.Status != "" {
			compact = append(compact, m.Status)
		}
		body = strings.Join(compact, "\n")
	}
	footer := "[q] quit [up/down] navigate [enter] edit [/] search [s] save [t] test [a] apply [r] revert [p] profile [x/X] start/cancel experiment [b/z/f] budget/expiry/policy"
	if m.Width < 70 {
		footer = "[q] quit [j/k] nav [e] edit [s] save [t] test [a] apply [x/X] experiment"
	} else if m.Width < 100 {
		footer = "[q] quit [j/k] nav [enter] edit [/] find [s] save [t] test [a] apply [r] revert [p] profile [x/X] experiment"
	}
	if m.Mode == ModeConfirmQuit {
		footer = "Unsaved DRAFT. [d/y] discard and quit  [esc] continue editing"
	}
	if m.Mode == ModeConfirmApply {
		footer = "Apply TESTED draft at the next safe boundary? [y] confirm  [esc] cancel"
	}
	if m.Mode == ModeEdit {
		footer = "Edit " + active.Label + ": " + m.input.View() + "  [enter] accept  [esc] cancel"
	}
	if m.Mode == ModeSearch {
		footer = "Search: " + m.input.View() + "  [enter] filter  [esc] cancel"
	}
	out := header + "\n" + strings.Repeat("-", max(1, min(m.Width, 120))) + "\n" + body + "\n" + footer
	return normalizeRender(out, m.Width, m.Height, m.noColor || m.accessible || strings.ToLower(os.Getenv("TERM")) == "dumb")
}

func columns(width int, parts ...string) string {
	gap := " | "
	col := (width - len(gap)*(len(parts)-1)) / len(parts)
	rows := make([][]string, len(parts))
	maxRows := 0
	for i, p := range parts {
		rows[i] = strings.Split(p, "\n")
		if len(rows[i]) > maxRows {
			maxRows = len(rows[i])
		}
	}
	var out []string
	for r := 0; r < maxRows; r++ {
		var cols []string
		for _, lines := range rows {
			s := ""
			if r < len(lines) {
				s = ansi.Truncate(lines[r], col, "")
			}
			cols = append(cols, s+strings.Repeat(" ", max(0, col-ansi.StringWidth(s))))
		}
		out = append(out, strings.Join(cols, gap))
	}
	return strings.Join(out, "\n")
}
func normalizeRender(s string, width, height int, ascii bool) string {
	var out []string
	for _, line := range strings.Split(s, "\n") {
		line = strings.TrimRight(line, " ")
		if ansi.StringWidth(line) > width {
			suffix := "…"
			if ascii {
				suffix = "..."
			}
			line = ansi.Truncate(line, width-len(suffix), suffix)
		}
		out = append(out, line)
		if height > 0 && len(out) >= height {
			break
		}
	}
	return strings.Join(out, "\n")
}
func lifecycle(m Model) string {
	if m.Snapshot.PendingStatus == "rejected" || m.Snapshot.PendingError != "" {
		return "REJECTED"
	}
	if m.Err != "" || strings.HasPrefix(m.Status, "FAILED") {
		return "FAILED"
	}
	if m.PendingRevision > 0 && m.PendingRevision != m.AppliedRevision {
		return "QUEUED"
	}
	if m.Test.OK && m.TestFingerprint == m.Fingerprint() {
		return "TESTED"
	}
	if m.IsDirty() {
		return "DRAFT"
	}
	if m.AppliedRevision > 0 && m.PendingRevision == m.AppliedRevision {
		return "ACTIVE"
	}
	return "DRAFT"
}
func mark(v bool) string {
	if v {
		return "[yes]"
	}
	return "[no]"
}
func fallback(v, d string) string {
	if strings.TrimSpace(v) == "" {
		return d
	}
	return safeText(v)
}
func changed(m Model, key string) string {
	if m.Dirty[key] {
		return "yes (draft shadows active value)"
	}
	return "no"
}
func fieldSource(values []FieldValue, key string) string {
	for _, v := range values {
		if v.Key == key {
			if v.Shadowed != "" {
				return fallback(v.Source, "default") + "; shadows " + v.Shadowed
			}
			return fallback(v.Source, "default")
		}
	}
	return "default"
}
func fieldActive(values []FieldValue, key string) string {
	for _, value := range values {
		if value.Key == key {
			if descriptor(key).Sensitive {
				return secretState(values, key)
			}
			return fallback(value.ActiveValue, "not active")
		}
	}
	return "not active"
}
func fieldDraft(m Model, field FieldDescriptor) string {
	if field.Sensitive {
		return secretState(m.Snapshot.Fields, field.Key)
	}
	return fallback(m.Draft[field.Key], "inherit")
}
func secretState(values []FieldValue, key string) string {
	for _, v := range values {
		if v.Key == key && v.SensitiveSet {
			return "[set]"
		}
	}
	return "[unset]"
}
