package ai

import (
	"path/filepath"
	"regexp"
	"sort"
	"strings"
)

// MessageQualityAction describes what the intent path should do with the
// planner's commit message after normal sanitation and quality checks.
type MessageQualityAction string

const (
	MessageQualityClean          MessageQualityAction = "clean"
	MessageQualitySanitizeAccept MessageQualityAction = "sanitize_accept"
	MessageQualityRewrite        MessageQualityAction = "rewrite"
	MessageQualityFallback       MessageQualityAction = "fallback"
)

// MessageQualityReasonCode names one policy finding. Codes are stable because
// later diagnostics and trace records persist them for operators.
type MessageQualityReasonCode string

const (
	MessageQualityReasonEmptySubject       MessageQualityReasonCode = "empty_subject"
	MessageQualityReasonGenericSubject     MessageQualityReasonCode = "generic_subject"
	MessageQualityReasonFilenameOnly       MessageQualityReasonCode = "filename_only_subject"
	MessageQualityReasonTokenOnly          MessageQualityReasonCode = "token_only_subject"
	MessageQualityReasonBodyRequired       MessageQualityReasonCode = "body_required"
	MessageQualityReasonMalformedBody      MessageQualityReasonCode = "malformed_body"
	MessageQualityReasonMalformedSubject   MessageQualityReasonCode = "malformed_subject"
	MessageQualityReasonUnknownCommitType  MessageQualityReasonCode = "unknown_commit_type"
	MessageQualityReasonSanitizedSubject   MessageQualityReasonCode = "sanitized_subject"
	MessageQualityReasonSanitizedBody      MessageQualityReasonCode = "sanitized_body"
	MessageQualityReasonMissingSelection   MessageQualityReasonCode = "missing_selected_capture"
	MessageQualityReasonUnknownSelection   MessageQualityReasonCode = "unknown_selected_capture"
	MessageQualityReasonHighImpactChange   MessageQualityReasonCode = "high_impact_change"
	MessageQualityReasonMixedChangeClasses MessageQualityReasonCode = "mixed_change_classes"
)

// MessageQualityReason is one actionable classifier finding.
type MessageQualityReason struct {
	Code    MessageQualityReasonCode
	Message string
}

// MessageQualityReport is the pure classifier result used by planner, rewrite,
// fallback, and diagnostics code.
type MessageQualityReport struct {
	Action           MessageQualityAction
	Reasons          []MessageQualityReason
	SanitizedSubject string
	SanitizedBody    string
	BodyRequired     bool
	SelectedCaptures int
	SelectedPaths    int
	DiffBytes        int
	HighImpact       bool
	ChangeClasses    []string
}

// EvaluateIntentPlanMessageQuality classifies whether a planner message can be
// accepted as-is, accepted after SanitizeMessage normalization, rewritten by the
// provider, or rejected to deterministic fallback.
func EvaluateIntentPlanMessageQuality(req IntentPlanRequest, plan IntentPlan) MessageQualityReport {
	ctx := messageQualityContext(req, plan.SelectedSeqs)
	subject, body := sanitizeSubjectBody(plan.Subject, plan.Body)
	report := MessageQualityReport{
		Action:           MessageQualityClean,
		SanitizedSubject: subject,
		SanitizedBody:    body,
		BodyRequired:     ctx.bodyRequired,
		SelectedCaptures: len(ctx.captures),
		SelectedPaths:    len(ctx.paths),
		DiffBytes:        ctx.diffBytes,
		HighImpact:       ctx.highImpact,
		ChangeClasses:    ctx.sortedClasses(),
	}

	if len(plan.SelectedSeqs) == 0 {
		report.add(MessageQualityReasonMissingSelection, "selected captures are required before message quality can be evaluated")
	}
	if ctx.unknownSelection {
		report.add(MessageQualityReasonUnknownSelection, "selected_seqs references captures outside the offered window")
	}
	if strings.TrimSpace(plan.Subject) == "" {
		report.add(MessageQualityReasonEmptySubject, "subject is empty")
	}
	if subject != strings.TrimSpace(plan.Subject) {
		report.add(MessageQualityReasonSanitizedSubject, "subject changes after sanitation")
	}
	if body != strings.TrimSpace(plan.Body) {
		report.add(MessageQualityReasonSanitizedBody, "body changes after sanitation")
	}
	if ctx.highImpact {
		report.add(MessageQualityReasonHighImpactChange, "selected captures touch CLI, config, migration, recovery, public API, or release-sensitive paths")
	}
	if ctx.mixedClasses {
		report.add(MessageQualityReasonMixedChangeClasses, "selected captures mix code, tests, docs, config, or migration changes")
	}
	if body == "" && ctx.bodyRequired {
		report.add(MessageQualityReasonBodyRequired, "selected captures require body bullets")
	}
	if strings.TrimSpace(plan.Body) != "" && !wellFormedBulletBody(strings.TrimSpace(plan.Body)) {
		report.add(MessageQualityReasonMalformedBody, "body must contain only '- ' bullets with indented continuations")
	}
	for _, reason := range validateCommitMessageFormat(req.CommitFormat, subject, body) {
		report.add(reason.Code, reason.Message)
	}
	if isGenericSubject(subject) {
		report.add(MessageQualityReasonGenericSubject, "subject is generic and does not describe the semantic change")
	}
	if isFilenameOnlySubject(subject, ctx.paths) {
		report.add(MessageQualityReasonFilenameOnly, "subject only names a file or path")
	}
	if isTokenOnlySubject(subject) {
		report.add(MessageQualityReasonTokenOnly, "subject only names a parsed token or symbol")
	}

	report.Action = report.decide(plan)
	return report
}

func (r *MessageQualityReport) add(code MessageQualityReasonCode, message string) {
	r.Reasons = append(r.Reasons, MessageQualityReason{Code: code, Message: message})
}

func (r MessageQualityReport) HasReason(code MessageQualityReasonCode) bool {
	for _, reason := range r.Reasons {
		if reason.Code == code {
			return true
		}
	}
	return false
}

func (r MessageQualityReport) decide(plan IntentPlan) MessageQualityAction {
	if r.HasReason(MessageQualityReasonMissingSelection) ||
		r.HasReason(MessageQualityReasonUnknownSelection) ||
		r.HasReason(MessageQualityReasonEmptySubject) {
		return MessageQualityFallback
	}
	if r.HasReason(MessageQualityReasonGenericSubject) ||
		r.HasReason(MessageQualityReasonFilenameOnly) ||
		r.HasReason(MessageQualityReasonTokenOnly) ||
		r.HasReason(MessageQualityReasonBodyRequired) ||
		r.HasReason(MessageQualityReasonMalformedSubject) ||
		r.HasReason(MessageQualityReasonUnknownCommitType) ||
		r.HasReason(MessageQualityReasonMalformedBody) {
		return MessageQualityRewrite
	}
	if r.SanitizedSubject != strings.TrimSpace(plan.Subject) ||
		r.SanitizedBody != strings.TrimSpace(plan.Body) {
		return MessageQualitySanitizeAccept
	}
	return MessageQualityClean
}

type messageQualityContextResult struct {
	captures         []OfferedCapture
	paths            map[string]struct{}
	diffBytes        int
	highImpact       bool
	mixedClasses     bool
	bodyRequired     bool
	unknownSelection bool
	classes          map[string]struct{}
}

func messageQualityContext(req IntentPlanRequest, selectedSeqs []int64) messageQualityContextResult {
	offered := make(map[int64]OfferedCapture, len(req.OfferedCaptures))
	for _, capture := range req.OfferedCaptures {
		offered[capture.Seq] = capture
	}
	ctx := messageQualityContextResult{
		paths:   make(map[string]struct{}),
		classes: make(map[string]struct{}),
	}
	for _, seq := range selectedSeqs {
		capture, ok := offered[seq]
		if !ok {
			ctx.unknownSelection = true
			continue
		}
		ctx.captures = append(ctx.captures, capture)
		if capture.Path != "" {
			ctx.paths[capture.Path] = struct{}{}
		}
		ctx.diffBytes += len(capture.CapturedDiff)
		if isHighImpactPath(capture.Path) {
			ctx.highImpact = true
		}
		class := changeClass(capture.Path)
		if class != "" {
			ctx.classes[class] = struct{}{}
		}
	}
	ctx.mixedClasses = len(ctx.classes) > 1
	ctx.bodyRequired = len(ctx.captures) > 1 ||
		len(ctx.paths) > 1 ||
		ctx.diffBytes > 1200 ||
		ctx.highImpact ||
		ctx.mixedClasses
	return ctx
}

func (ctx messageQualityContextResult) sortedClasses() []string {
	if len(ctx.classes) == 0 {
		return nil
	}
	out := make([]string, 0, len(ctx.classes))
	for class := range ctx.classes {
		out = append(out, class)
	}
	sort.Strings(out)
	return out
}

func sanitizeSubjectBody(subject, body string) (string, string) {
	cleaned := SanitizeMessage(subject + "\n\n" + body)
	parts := strings.SplitN(cleaned, "\n\n", 2)
	if len(parts) == 1 {
		return parts[0], ""
	}
	return parts[0], parts[1]
}

func wellFormedBulletBody(body string) bool {
	body = strings.TrimSpace(body)
	if body == "" {
		return true
	}
	for _, line := range strings.Split(body, "\n") {
		if strings.TrimSpace(line) == "" {
			return false
		}
		if strings.HasPrefix(line, "- ") {
			continue
		}
		if strings.HasPrefix(line, "  ") && !strings.HasPrefix(strings.TrimLeft(line, " "), "- ") {
			continue
		}
		return false
	}
	return true
}

var genericSubjectPatterns = []*regexp.Regexp{
	regexp.MustCompile(`(?i)^(update|change|modify|fix|adjust|improve|refactor)\s+(file|files|stuff|things|changes|code|content|logic|data)$`),
	regexp.MustCompile(`(?i)^(wip|changes|misc|miscellaneous|updates?)$`),
	regexp.MustCompile(`(?i)^update\s+\d+\s+files?(?:\s+in\s+.+)?$`),
}

func isGenericSubject(subject string) bool {
	subject = strings.TrimSpace(subject)
	for _, re := range genericSubjectPatterns {
		if re.MatchString(subject) {
			return true
		}
	}
	return false
}

func isFilenameOnlySubject(subject string, paths map[string]struct{}) bool {
	subject = strings.TrimSpace(subject)
	if subject == "" {
		return false
	}
	tail := subjectTail(subject)
	if looksLikePathToken(tail) {
		return true
	}
	for p := range paths {
		if tail == filepath.Base(p) || tail == strings.Trim(p, "/") {
			return true
		}
	}
	return false
}

func subjectTail(subject string) string {
	fields := strings.Fields(subject)
	if len(fields) <= 1 {
		return subject
	}
	switch strings.ToLower(fields[0]) {
	case "add", "fix", "refactor", "remove", "rename", "simplify", "update", "document", "change", "modify", "adjust", "improve":
		return strings.TrimSpace(strings.TrimPrefix(subject, fields[0]))
	default:
		return subject
	}
}

func looksLikePathToken(s string) bool {
	s = strings.Trim(s, "`'\" ")
	if s == "" || strings.Contains(s, " ") {
		return false
	}
	if strings.Contains(s, "/") {
		return true
	}
	ext := filepath.Ext(s)
	return len(ext) > 1 && len(strings.TrimSuffix(s, ext)) > 0
}

func isTokenOnlySubject(subject string) bool {
	tail := strings.Trim(subjectTail(subject), "`'\" ")
	if tail == "" || strings.Contains(tail, " ") || looksLikePathToken(tail) {
		return false
	}
	if strings.Contains(tail, "_") || strings.Contains(tail, "-") {
		return true
	}
	if hasInternalUpper(tail) && tail[0] >= 'a' && tail[0] <= 'z' {
		return true
	}
	switch strings.ToLower(tail) {
	case "parsed", "total", "value", "data", "state", "result", "item", "items", "helper", "logic", "flow":
		return true
	}
	return false
}

func hasInternalUpper(s string) bool {
	for i := 1; i < len(s); i++ {
		if s[i] >= 'A' && s[i] <= 'Z' {
			return true
		}
	}
	return false
}

func isHighImpactPath(path string) bool {
	clean := strings.Trim(path, "/")
	if clean == "" {
		return false
	}
	switch {
	case strings.HasPrefix(clean, "cmd/acd/"):
		return true
	case strings.HasPrefix(clean, "internal/cli/"):
		return true
	case strings.HasPrefix(clean, "internal/state/"):
		return true
	case strings.Contains(clean, "migration"):
		return true
	case strings.Contains(clean, "recover") || strings.Contains(clean, "recovery"):
		return true
	case strings.HasPrefix(clean, "templates/"):
		return true
	case strings.HasPrefix(clean, ".github/workflows/"):
		return true
	case strings.HasPrefix(clean, "scripts/install"):
		return true
	case clean == "go.mod" || clean == "go.sum" || clean == ".goreleaser.yaml":
		return true
	case clean == "README.md" || strings.HasPrefix(clean, "docs/"):
		return true
	default:
		return false
	}
}

func changeClass(path string) string {
	clean := strings.Trim(path, "/")
	ext := strings.ToLower(filepath.Ext(clean))
	switch {
	case clean == "":
		return ""
	case strings.HasPrefix(clean, "docs/") || ext == ".md" || ext == ".markdown":
		return "docs"
	case strings.Contains(clean, "test/") || strings.Contains(clean, "tests/") ||
		strings.HasSuffix(clean, "_test.go") || strings.HasSuffix(clean, ".test.ts") ||
		strings.HasSuffix(clean, ".test.tsx") || strings.HasSuffix(clean, ".spec.ts") ||
		strings.HasSuffix(clean, ".spec.tsx"):
		return "tests"
	case strings.HasPrefix(clean, ".github/") || strings.HasPrefix(clean, "templates/") ||
		ext == ".json" || ext == ".yaml" || ext == ".yml" || ext == ".toml":
		return "config"
	case strings.Contains(clean, "migration"):
		return "migration"
	case ext == ".go" || ext == ".ts" || ext == ".tsx" || ext == ".js" ||
		ext == ".jsx" || ext == ".py" || ext == ".sh":
		return "code"
	default:
		return "other"
	}
}
