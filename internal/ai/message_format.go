package ai

import "strings"

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
	"Example subject: feat: add commit format selection. " +
	"Example subject: fix: reject scoped conventional subjects."

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
