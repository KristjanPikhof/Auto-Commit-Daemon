package ai

import (
	"fmt"
	"strings"
)

const imperativeCommitMessageFormatInstructions = "Commit message format: " +
	"Line 1: <imperative verb> <what changed>, max 50 characters, no trailing period. " +
	"Line 2: blank. " +
	"Line 3+: bullet list for why/context; each bullet starts with '- '; max 72 characters per line; wrapped continuation lines must not start with '- '. " +
	"Line 1 must start with an imperative verb such as Add, Fix, Refactor, Remove, Rename, Simplify, Update, or Document. " +
	"Describe the semantic change, not just the filename. " +
	"Prefer a concise subject and practical body focused on what changed and why. " +
	"The body should explain why, intent, impact, or context, not restate the diff. " +
	"Avoid generic messages such as Update file, WIP, or changes. " +
	"Do not mention filenames in line 1 unless the change is specifically about that file itself."

var conventionalCommitTypes = []string{
	"feat",
	"fix",
	"docs",
	"refactor",
	"test",
	"build",
	"ci",
	"chore",
	"perf",
	"style",
	"revert",
}

const conventionalCommitMessageFormatInstructions = "Commit message format: " +
	"Line 1: <type>: <description>, max 50 characters, no trailing period. " +
	"Line 2: blank. " +
	"Line 3+: bullet list for why/context; each bullet starts with '- '; max 72 characters per line; wrapped continuation lines must not start with '- '. " +
	"Line 1 must use a scope-less Conventional Commit type followed by ': ' and a non-empty description. " +
	"Allowed types are feat, fix, docs, refactor, test, build, ci, chore, perf, style, and revert. " +
	"Do not include scopes such as feat(api): or fix(cli):. " +
	"Prefer a concise subject and practical body focused on what changed and why. " +
	"The body should explain why, intent, impact, or context, not restate the diff. " +
	"Example subjects include `feat: add commit format selection` and `fix: reject scoped conventional subjects`."

// CommitMessageFormatInstructions returns the provider-facing message
// contract for the selected format. Unknown values intentionally use the
// imperative contract so default behaviour remains stable.
func CommitMessageFormatInstructions(format CommitFormat) string {
	switch format {
	case CommitFormatConventional:
		return conventionalCommitMessageFormatInstructions
	default:
		return imperativeCommitMessageFormatInstructions
	}
}

// ConventionalCommitTypes returns the scope-less Conventional Commit types
// accepted by ACD in conventional mode.
func ConventionalCommitTypes() []string {
	return append([]string(nil), conventionalCommitTypes...)
}

func isConventionalCommitType(raw string) bool {
	raw = strings.TrimSpace(raw)
	for _, typ := range conventionalCommitTypes {
		if raw == typ {
			return true
		}
	}
	return false
}

func effectiveCommitFormat(format CommitFormat) CommitFormat {
	switch format {
	case CommitFormatConventional:
		return CommitFormatConventional
	default:
		return CommitFormatImperative
	}
}

func firstCommitFormat(formats ...CommitFormat) CommitFormat {
	for _, format := range formats {
		if format != "" {
			return effectiveCommitFormat(format)
		}
	}
	return CommitFormatImperative
}

func commitFormatValidationError(prefix string, reasons []MessageQualityReason) error {
	if len(reasons) == 0 {
		return nil
	}
	parts := make([]string, 0, len(reasons))
	for _, reason := range reasons {
		parts = append(parts, string(reason.Code))
	}
	return fmt.Errorf("%s: message format validation failed: %s", prefix, strings.Join(parts, ","))
}

func validateCommitMessageFormat(format CommitFormat, subject, body string) []MessageQualityReason {
	var reasons []MessageQualityReason
	if strings.TrimSpace(body) != "" && !wellFormedBulletBody(strings.TrimSpace(body)) {
		reasons = append(reasons, MessageQualityReason{
			Code:    MessageQualityReasonMalformedBody,
			Message: "body must contain only '- ' bullets with indented continuations",
		})
	}
	if effectiveCommitFormat(format) != CommitFormatConventional {
		return reasons
	}
	subject = strings.TrimSpace(subject)
	switch {
	case subject == "":
		reasons = append(reasons, MessageQualityReason{
			Code:    MessageQualityReasonEmptySubject,
			Message: "subject is empty",
		})
	case len([]rune(subject)) > SubjectCap:
		reasons = append(reasons, MessageQualityReason{
			Code:    MessageQualityReasonMalformedSubject,
			Message: "conventional subject exceeds 50 characters",
		})
	case strings.HasSuffix(subject, "."):
		reasons = append(reasons, MessageQualityReason{
			Code:    MessageQualityReasonMalformedSubject,
			Message: "conventional subject must not end with a period",
		})
	default:
		reasons = append(reasons, validateConventionalSubject(subject)...)
	}
	return reasons
}

func validateConventionalSubject(subject string) []MessageQualityReason {
	idx := strings.Index(subject, ": ")
	if idx <= 0 {
		return []MessageQualityReason{{
			Code:    MessageQualityReasonMalformedSubject,
			Message: "conventional subject must use '<type>: <description>'",
		}}
	}
	typ := subject[:idx]
	desc := strings.TrimSpace(subject[idx+2:])
	switch {
	case strings.ContainsAny(typ, "()!"):
		return []MessageQualityReason{{
			Code:    MessageQualityReasonMalformedSubject,
			Message: "conventional subject must not include a scope or breaking marker",
		}}
	case !isConventionalCommitType(typ):
		return []MessageQualityReason{{
			Code:    MessageQualityReasonUnknownCommitType,
			Message: "conventional subject uses an unknown type",
		}}
	case desc == "":
		return []MessageQualityReason{{
			Code:    MessageQualityReasonMalformedSubject,
			Message: "conventional subject description must be non-empty",
		}}
	}
	return nil
}
