package ai

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"unicode/utf8"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/finish"
)

type HistoryMessageCommit struct {
	ID              string   `json:"id"`
	TreeOID         string   `json:"tree_oid"`
	Author          string   `json:"author"`
	OriginalMessage string   `json:"original_message"`
	ChangeIDs       []string `json:"change_ids"`
}

func finishPrompt(request FinishPlanRequest) string {
	if len(request.HistoryCommits) == 0 {
		return finishSystemPrompt
	}
	return finishSystemPrompt + `
This request is an explicit messages-only history rewrite. Keep every history_commits record in its original order, including empty and net-neutral commits. Return exactly one goal for each original commit. Its change_ids must contain ONLY that original commit's id. Changes and recorded diffs provide context, not permission to regroup membership. Preserve authors and tree boundaries; only purpose, boundary and message are proposed. Explain the recorded change without inventing tests, verification, motivation, or file content.`
}

// PlanHistoryMessages preserves commit membership, unlike a purpose plan that
// may remove net-neutral changes. It uses one exchange and one combined correction.
func PlanHistoryMessages(ctx context.Context, transport FinishTransport, evidence finish.Evidence, commits []HistoryMessageCommit, options FinishOptions) ([]finish.Goal, FinishPlanResult, error) {
	var result FinishPlanResult
	if options.Style == "" {
		options.Style = finish.StyleImperative
	}
	if options.Style != finish.StyleImperative && options.Style != finish.StyleConventional {
		return nil, result, errors.New("history messages: unsupported message style")
	}
	if err := finish.ValidatePurpose(options.Purpose); err != nil {
		return nil, result, err
	}
	if err := finish.ValidateEvidence(evidence); err != nil {
		return nil, result, err
	}
	if len(commits) == 0 || len(commits) > 256 || transport == nil {
		return nil, result, errors.New("history messages: valid recorded commits and configured transport are required")
	}
	if options.Deadline.IsZero() {
		return nil, result, finishFailure("provider_deadline", errors.New("persisted planning deadline is required"))
	}
	ctx, cancel := context.WithDeadline(ctx, options.Deadline)
	defer cancel()
	request, disclosure, err := prepareFinishRequest(evidence, options)
	if err != nil {
		return nil, result, err
	}
	for _, commit := range commits {
		if !utf8.ValidString(commit.ID + commit.TreeOID + commit.Author + commit.OriginalMessage) {
			return nil, result, errors.New("history messages: recorded evidence is not UTF-8")
		}
		redacted := finishRedactText(commit.OriginalMessage)
		disclosure.Redacted = disclosure.Redacted || redacted != commit.OriginalMessage
		commit.OriginalMessage = redacted
		request.HistoryCommits = append(request.HistoryCommits, commit)
	}
	for _, diffs := range [][]FinishDiff{options.Diffs, request.Diffs} {
		total := 0
		for _, diff := range diffs {
			total += len(diff.Text)
		}
		if total > HistoryRewriteTotalDiffCap {
			return nil, result, finishInputLimit(total, HistoryRewriteTotalDiffCap)
		}
	}
	disclosure.Provider = transport.Name()
	for call := 0; call < 2; call++ {
		body, err := transport.EncodeFinishRequest(request)
		if err != nil {
			return nil, result, err
		}
		if len(body) > HistoryRewriteRequestByteCap {
			return nil, result, finishInputLimit(len(body), HistoryRewriteRequestByteCap)
		}
		if err := ctx.Err(); err != nil {
			return nil, result, finishFailure("provider_deadline", err)
		}
		if options.BeforeCall != nil {
			if err := options.BeforeCall(ctx); err != nil {
				return nil, result, err
			}
		}
		if err := ctx.Err(); err != nil {
			return nil, result, finishFailure("provider_deadline", err)
		}
		if call == 0 {
			result.Disclosure = disclosure
		}
		result.Calls++
		result.Disclosure.RequestBytes = append(result.Disclosure.RequestBytes, len(body))
		raw, exchangeErr := transport.ExchangeFinish(ctx, body)
		var outputError *finishOutputError
		if exchangeErr != nil && !errors.As(exchangeErr, &outputError) {
			return nil, result, finishFailure("provider_failure", exchangeErr)
		}
		if outputError != nil {
			raw = outputError.Raw
		}
		if err := ctx.Err(); err != nil {
			return nil, result, finishFailure("provider_deadline", err)
		}
		goals, err := validateHistoryMessages(raw, evidence, commits, options)
		if err == nil {
			return goals, result, nil
		}
		if call == 1 {
			return nil, result, finishFailure("invalid_plan", err)
		}
		request.Correction = &FinishCorrection{PreviousOutput: finishRedactText(string(raw)), ValidationError: finishCorrectionError(err)}
	}
	return nil, result, errors.New("history messages: bounded attempts exhausted")
}

func validateHistoryMessages(raw []byte, evidence finish.Evidence, commits []HistoryMessageCommit, options FinishOptions) ([]finish.Goal, error) {
	if len(raw) > subprocessResponseByteCap || !utf8.Valid(raw) {
		return nil, errors.New("history messages: response is too large or invalid UTF-8")
	}
	var output struct {
		Goals []finish.Goal `json:"goals"`
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&output); err != nil {
		return nil, err
	}
	if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
		return nil, errors.New("history messages: expected one response object")
	}
	if len(output.Goals) != len(commits) {
		return nil, errors.New("history messages: preserve every original commit exactly once")
	}
	for index, goal := range output.Goals {
		if len(goal.ChangeIDs) != 1 || goal.ChangeIDs[0] != commits[index].ID {
			return nil, errors.New("history messages: original commit order and membership must remain unchanged")
		}
		if err := finish.ValidatePurpose(goal.Purpose); err != nil {
			return nil, err
		}
		if err := finish.ValidateMessage(options.Style, goal.Message); err != nil {
			return nil, err
		}
		quality := goal
		quality.ChangeIDs = commits[index].ChangeIDs
		if err := validateFinishGoalQuality(quality, evidence, options); err != nil {
			return nil, err
		}
	}
	return output.Goals, nil
}

// ValidateHistoryGoalQuality rechecks saved Intent messages against their
// recorded membership without opening a provider or exposing any source text.
func ValidateHistoryGoalQuality(goal finish.Goal, evidence finish.Evidence) error {
	return validateFinishGoalQuality(goal, evidence, FinishOptions{})
}
