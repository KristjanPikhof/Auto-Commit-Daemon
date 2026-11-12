package ai

import (
	"errors"
	"fmt"
	"regexp"
	"strings"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/finish"
)

// FinishReviewLineTarget asks for smaller complete steps, with a visible
// explanation when exact evidence or necessary coupling requires a larger one.
const FinishReviewLineTarget = 200

// CommitSizeFailure contains only locally measured numbers, never source or
// command output. It can share the existing planning correction safely.
type CommitSizeFailure struct {
	Step, ChangedLines, ChangedFiles, Limit int
}

func (failure *CommitSizeFailure) Error() string {
	return fmt.Sprintf("step %d changes %d lines across %d files, above the %d-line review target: split independent complete steps, or supply a concrete large_change_reason explaining the necessary coupling or indivisible evidence and include it in the commit body; do not separate required callers, imports or tests merely to meet the target", failure.Step, failure.ChangedLines, failure.ChangedFiles, failure.Limit)
}

type finishBoundariesError struct{ Reason string }

func (failure *finishBoundariesError) Error() string {
	return "cannot plan purposeful commits: " + failure.Reason
}

// All errors in a collected result must be approved correction feedback.
// A compiler rejection cannot hide an operational failure in the same result.
func correctableFinishCheck(err error) bool {
	switch failure := err.(type) {
	case interface{ Unwrap() []error }:
		causes := failure.Unwrap()
		for _, cause := range causes {
			if !correctableFinishCheck(cause) {
				return false
			}
		}
		return len(causes) > 0
	case interface{ Unwrap() error }:
		return correctableFinishCheck(failure.Unwrap())
	case *SourceCheckFailure, *CommitSizeFailure:
		return true
	default:
		return false
	}
}

var vagueFinishReason = regexp.MustCompile(`(?i)(?:^|[.;:])\s*(?:fix|address|handle|improve)\s+(?:both|it|this|that|these|them|the issues|these issues|the problems)[.!]?\s*$`)

func validateReviewExplanation(goal finish.Goal) error {
	if vagueFinishReason.MatchString(goal.Reason) {
		return errors.New("explain the concrete mechanism and its effect instead of a placeholder such as 'fix both'")
	}
	reason := strings.Join(strings.Fields(goal.LargeChangeReason), " ")
	if reason == "" {
		return nil // Actual candidate size determines whether an exception is needed.
	}
	if len(strings.Fields(reason)) < 6 || isGenericSubject(reason) || vagueFinishReason.MatchString(reason) {
		return errors.New("large_change_reason must identify the concrete dependency or indivisible evidence that prevents smaller complete steps")
	}
	if !finishMessageContainsExplanation(goal.Message, reason) {
		return errors.New("include large_change_reason verbatim in the published body so reviewers can judge why this larger commit stays together")
	}
	return nil
}

func finishMessageContainsExplanation(message, explanation string) bool {
	_, body, _ := strings.Cut(message, "\n\n")
	var lines []string
	for _, line := range strings.Split(body, "\n") {
		lines = append(lines, strings.TrimPrefix(strings.TrimSpace(line), "- "))
	}
	return strings.Contains(strings.Join(strings.Fields(strings.Join(lines, " ")), " "), strings.Join(strings.Fields(explanation), " "))
}

const finishSmallStepsPrompt = `
review_line_target is the preferred maximum additions plus deletions in each actual proposed commit. Plan the smallest complete, useful steps first; an overall feature or task can contain several such steps. ACD measures each candidate against its preceding tree, not the accumulated save history. File counts are context, not a split quota. Keep the implementation, callers, imports and regression tests needed for each step together.
For every goal return large_change_reason, normally an empty string. If a step must exceed review_line_target, explain the specific dependency, indivisible recorded version, large deletion or required generated companion that prevents a smaller useful step. Include this explanation verbatim in the published message body. Sharing a task, feature or directory alone is not an exception. Prefer splitting independent behavior over explaining an avoidably large goal. Do not manufacture partial features or cosmetic commits to reduce the count.
Messages should explain the concrete change, the previous problem or constraint, and how the chosen mechanism helps. Replace placeholders such as 'fix both' or 'address these issues' with the actual mechanism. On correction, consider measured size and project-check failures together, preserving independent purposes and valid intermediate trees. Exact supplied messages and messages-only history rewrites keep their existing boundaries.`
