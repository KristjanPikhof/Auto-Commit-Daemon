package ai

import (
	"encoding/json"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/prompttrace"
)

func promptTraceMetadata(meta prompttrace.Metadata, provider, model string) prompttrace.Metadata {
	meta.Provider = provider
	if meta.Model == "" {
		meta.Model = model
	}
	return meta
}

func promptTransformMetadata(input, redacted, output string) prompttrace.TransformMetadata {
	return prompttrace.TransformMetadata{
		RedactionApplied: input != redacted,
		Truncated:        redacted != output,
		InputBytes:       len(input),
		RedactedBytes:    len(redacted),
		OutputBytes:      len(output),
	}
}

func offeredSeqs(req IntentPlanRequest) []int64 {
	if len(req.OfferedCaptures) == 0 {
		return nil
	}
	seqs := make([]int64, 0, len(req.OfferedCaptures))
	for _, capture := range req.OfferedCaptures {
		seqs = append(seqs, capture.Seq)
	}
	return seqs
}

func intentDiffIncluded(req IntentPlanRequest) bool {
	for _, capture := range req.OfferedCaptures {
		if capture.CapturedDiff != "" {
			return true
		}
	}
	return false
}

func offeredSeqsV2(req IntentPlanRequestV2) []int64 {
	if len(req.OfferedCaptures) == 0 {
		return nil
	}
	seqs := make([]int64, 0, len(req.OfferedCaptures))
	for _, capture := range req.OfferedCaptures {
		seqs = append(seqs, capture.Seq)
	}
	return seqs
}

func intentDiffIncludedV2(req IntentPlanRequestV2) bool {
	for _, capture := range req.OfferedCaptures {
		if capture.CapturedDiff != "" {
			return true
		}
	}
	return false
}

func candidatePlanSeqs(plan IntentPlanV2) []int64 {
	var seqs []int64
	for _, candidate := range plan.Candidates {
		seqs = append(seqs, candidate.SelectedSeqs...)
	}
	return seqs
}

func openAITraceRequest(body []byte, transform prompttrace.TransformMetadata) (prompttrace.Record, error) {
	var decoded struct {
		Messages []struct {
			Role    string `json:"role"`
			Content string `json:"content"`
		} `json:"messages"`
		Tools []struct {
			Function struct {
				Parameters any `json:"parameters"`
			} `json:"function"`
		} `json:"tools"`
	}
	if err := json.Unmarshal(body, &decoded); err != nil {
		return prompttrace.Record{}, err
	}
	rec := prompttrace.Record{
		Stage:     "request",
		Request:   append([]byte(nil), body...),
		Transform: transform,
	}
	for _, msg := range decoded.Messages {
		switch msg.Role {
		case "system":
			rec.SystemMessage = msg.Content
		case "user":
			rec.UserMessage = msg.Content
		}
	}
	if len(decoded.Tools) > 0 {
		rec.ToolSchema = decoded.Tools[0].Function.Parameters
	}
	return rec, nil
}
