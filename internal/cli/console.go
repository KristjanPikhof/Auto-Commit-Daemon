package cli

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/daemon"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/verification"
	"github.com/charmbracelet/x/term"
)

// consoleProse wraps prose while leaving command lines intact for copying.
func consoleProse(text string) string {
	width := 80
	if columns, _, err := term.GetSize(os.Stdout.Fd()); err == nil && columns > 0 {
		width = columns
	}
	var lines []string
	for _, line := range strings.Split(text, "\n") {
		line = strings.Map(func(r rune) rune {
			if unicode.IsControl(r) {
				return ' '
			}
			return r
		}, line)
		if strings.Contains(line, "acd ") || strings.Contains(line, "`") {
			lines = append(lines, line)
			continue
		}
		current := ""
		for _, word := range strings.Fields(line) {
			if current != "" && utf8.RuneCountInString(current)+1+utf8.RuneCountInString(word) > width {
				lines = append(lines, current)
				current = ""
			}
			if current != "" {
				current += " "
			}
			current += word
		}
		lines = append(lines, current)
	}
	return strings.Join(lines, "\n")
}

func finishStageLabel(stage string) string {
	switch stage {
	case "planning", "preparing":
		return "Preparing your commits."
	case "verifying":
		return "Checking the proposed commits."
	case "publishing", "prepared":
		return "Adding the prepared commits to your branch."
	default:
		return "Finish is in progress."
	}
}

func finishCheckLabel(check daemon.FinishCheck) string {
	if check.Policy == "none" {
		return "Project checks: skipped (none configured)."
	}
	if check.Policy == "structural" || check.Result.Mode == verification.ModeStructural {
		return "Project checks: not run; only structural checks were configured."
	}
	if check.Reused {
		return "Project checks: " + string(check.Result.Status) + " earlier; saved result reused."
	}
	if check.Executed {
		return "Project checks: " + string(check.Result.Status) + "."
	}
	return "Project checks: not run."
}

// ConsoleError keeps diagnostic causes available without repeating the CLI prefix.
func ConsoleError(err error) string {
	message := strings.TrimPrefix(consoleCause(err.Error()), "acd: ")
	var commandErr *CommandError
	if !errors.As(err, &commandErr) || commandErr.Exit != ExitInvalid {
		summary := ""
		for _, entry := range []struct{ prefix, summary string }{
			{"acd on:", "ACD could not start protection."},
			{"acd off:", "ACD could not stop protection safely."},
			{"acd setup:", "ACD could not complete setup."},
			{"acd finish:", "ACD could not complete the finish request."},
			{"history rewrite:", "ACD could not change the selected history."},
			{"acd restore:", "ACD could not restore the selected snapshot."},
			{"acd config:", "ACD could not complete the settings change."},
			{"acd uninstall:", "ACD could not complete uninstall."},
		} {
			if strings.HasPrefix(message, entry.prefix) {
				summary = entry.summary
				break
			}
		}
		if summary != "" && !strings.Contains(message, "\n") {
			message = summary + "\nDetails: " + message
		}
	}
	return "acd: " + consoleProse(message)
}

func consoleCause(message string) string {
	for _, pair := range [][2]string{
		{"state: another finish request owns this worktree", "An earlier finish request must be reviewed before starting another. Run acd doctor."},
		{"git: finish publication ownership or Git state changed", "The branch or Git state changed. ACD could not safely complete this commit."},
		{"git: staged changes require explicit --include-staged consent", "Git has staged changes. Review them, then use acd finish --include-staged to include that exact selection."},
		{"git: finish cannot prove ownership of the existing Git lock", "Another Git operation may be using this repository. ACD could not safely complete this commit."},
		{"finish: protection checkpoint is incomplete", "ACD could not save all eligible changes, so finish did not continue. Run acd doctor to see which items need attention."},
	} {
		message = strings.ReplaceAll(message, pair[0], pair[1])
	}
	return message
}

func restoreNextAction(refusal string) string {
	switch {
	case strings.Contains(refusal, "in progress"):
		return "Finish or cancel the Git operation, then preview the restore again."
	case strings.Contains(refusal, "conflicts"):
		return "Resolve the Git conflicts, then preview the restore again."
	case strings.Contains(refusal, "staged"):
		return "Review the listed staged items before restoring. Keep staging intact until you decide how to handle the overlap."
	case strings.Contains(refusal, "excluded"):
		return "Review ACD exclusion settings and the chosen snapshot."
	default:
		return "Run `acd doctor` to review the missing safety evidence."
	}
}

func commitCountLabel(count int) string {
	if count == 1 {
		return "Created 1 local Git commit."
	}
	return fmt.Sprintf("Created %d local Git commits.", count)
}
