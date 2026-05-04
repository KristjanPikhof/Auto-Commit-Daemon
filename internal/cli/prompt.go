package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/central"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/paths"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/prompttrace"
)

type promptReport struct {
	Repo     string      `json:"repo"`
	TraceDir string      `json:"trace_dir"`
	Query    promptQuery `json:"query"`
	Found    bool        `json:"found"`
	Message  string      `json:"message,omitempty"`
	Trace    *promptView `json:"trace,omitempty"`
}

type promptQuery struct {
	Last bool  `json:"last,omitempty"`
	Seq  int64 `json:"seq,omitempty"`
}

type promptView struct {
	Timestamp          string                        `json:"timestamp"`
	Strategy           string                        `json:"strategy,omitempty"`
	Provider           string                        `json:"provider,omitempty"`
	Model              string                        `json:"model,omitempty"`
	Seq                int64                         `json:"seq,omitempty"`
	OfferedSeqs        []int64                       `json:"offered_seqs,omitempty"`
	BranchRef          string                        `json:"branch_ref,omitempty"`
	Generation         int64                         `json:"generation,omitempty"`
	DiffIncluded       bool                          `json:"diff_included"`
	DiffCap            int                           `json:"diff_cap,omitempty"`
	Transform          prompttrace.TransformMetadata `json:"transform,omitempty"`
	SystemPrompt       string                        `json:"system_prompt,omitempty"`
	UserPrompt         string                        `json:"user_prompt,omitempty"`
	ToolSchema         any                           `json:"tool_schema,omitempty"`
	Request            json.RawMessage               `json:"request,omitempty"`
	SubprocessEnvelope json.RawMessage               `json:"subprocess_envelope,omitempty"`
	Response           *prompttrace.Response         `json:"response,omitempty"`
	Fallback           *prompttrace.Response         `json:"fallback,omitempty"`
	ValidationState    string                        `json:"validation_state"`
	Stages             []string                      `json:"stages"`
	Errors             []string                      `json:"errors,omitempty"`
}

type promptGroup struct {
	records  []prompttrace.Record
	request  *prompttrace.Record
	response *prompttrace.Record
	fallback *prompttrace.Record
}

const maxPromptGroupsInMemory = 1024

func newPromptCmd() *cobra.Command {
	var (
		last bool
		seq  int64
	)

	cmd := &cobra.Command{
		Use:   "prompt",
		Short: "Inspect the last recorded AI prompt request",
		Long: `Inspect locally recorded AI prompt requests for the current repo.

Prompt traces are written only when the daemon runs with ACD_AI_PROMPT_TRACE
enabled. This command reads <git-dir>/acd/prompt-trace JSONL files without
opening or migrating state.db. With no selector, --last is implied.`,
		Example: `  acd prompt
  acd prompt --last
  acd prompt --seq 42
  acd prompt --seq 42 --json`,
		RunE: func(cmd *cobra.Command, args []string) error {
			repo, _ := cmd.Flags().GetString("repo")
			jsonOut, _ := cmd.Flags().GetBool("json")
			return runPrompt(cmd.Context(), cmd.OutOrStdout(), repo, last, seq, jsonOut)
		},
	}
	cmd.Flags().BoolVar(&last, "last", false, "Show the most recent prompt trace")
	cmd.Flags().Int64Var(&seq, "seq", 0, "Show the newest trace for a captured event or offered intent seq")
	return cmd
}

func runPrompt(ctx context.Context, out io.Writer, repo string, last bool, seq int64, jsonOut bool) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if seq < 0 {
		return fmt.Errorf("acd prompt: --seq must be non-negative")
	}
	if last && seq > 0 {
		return fmt.Errorf("acd prompt: choose only one of --last or --seq")
	}
	if !last && seq == 0 {
		last = true
	}

	rec, err := promptRepoRecord(repo)
	if err != nil {
		return err
	}
	gitDir := gitDirFromStateDB(rec.StateDB)
	traceDir := prompttrace.Dir(gitDir)
	group, hasTraces, err := selectPromptGroup(ctx, traceDir, last, seq)
	if err != nil {
		return fmt.Errorf("acd prompt: read prompt trace: %w", err)
	}
	report := buildPromptReport(rec.Path, traceDir, group, hasTraces, last, seq)
	if jsonOut {
		enc := json.NewEncoder(out)
		enc.SetIndent("", "  ")
		return enc.Encode(report)
	}
	return renderPromptHuman(out, report)
}

func promptRepoRecord(repo string) (repoRecord, error) {
	abs, err := resolveRepo(repo)
	if err != nil {
		return repoRecord{}, err
	}
	roots, err := paths.Resolve()
	if err != nil {
		return repoRecord{}, fmt.Errorf("acd prompt: resolve paths: %w", err)
	}
	reg, err := central.Load(roots)
	if err != nil {
		return repoRecord{}, fmt.Errorf("acd prompt: load registry: %w", err)
	}
	rec, ok := findRepo(reg, abs)
	if !ok {
		return repoRecord{}, fmt.Errorf("acd prompt: repo %s is not registered (try `acd start --repo %s`)", abs, abs)
	}
	return repoRecord{Path: rec.Path, StateDB: rec.StateDB}, nil
}

type repoRecord struct {
	Path    string
	StateDB string
}

func buildPromptReport(repo, traceDir string, group *promptGroup, hasTraces bool, last bool, seq int64) promptReport {
	report := promptReport{
		Repo:     repo,
		TraceDir: traceDir,
		Query:    promptQuery{Last: last, Seq: seq},
	}
	if !hasTraces {
		report.Message = fmt.Sprintf("No prompt traces found in %s. Start acd with ACD_AI_PROMPT_TRACE=1 to record AI requests.", traceDir)
		return report
	}
	if group == nil {
		report.Message = fmt.Sprintf("No prompt trace found for seq %d in %s.", seq, traceDir)
		return report
	}
	report.Found = true
	view := promptGroupView(group)
	report.Trace = &view
	return report
}

func selectPromptGroup(ctx context.Context, traceDir string, last bool, seq int64) (*promptGroup, bool, error) {
	selector := newPromptGroupSelector(last, seq)
	err := prompttrace.Walk(ctx, prompttrace.ReadOptions{Dir: traceDir}, func(rec prompttrace.Record) error {
		selector.add(rec)
		return nil
	})
	if err != nil {
		return nil, false, err
	}
	return selector.selected(), selector.hasTraces, nil
}

type promptGroupSelector struct {
	last      bool
	seq       int64
	hasTraces bool
	latest    *promptGroup
	matching  *promptGroup
	recent    []*promptGroup
}

func newPromptGroupSelector(last bool, seq int64) *promptGroupSelector {
	return &promptGroupSelector{last: last, seq: seq}
}

func (s *promptGroupSelector) add(rec prompttrace.Record) {
	s.hasTraces = true
	if s.last {
		group := s.addRecent(rec)
		if s.latest == nil || groupLatestTS(group).After(groupLatestTS(s.latest)) {
			s.latest = group
		}
		return
	}
	if rec.Stage == "request" {
		if recordContainsSeq(rec, s.seq) {
			s.matching = newPromptGroup(rec)
		}
		return
	}
	if !recordContainsSeq(rec, s.seq) && (s.matching == nil || !samePromptKeyFromGroup(s.matching, rec)) {
		return
	}
	if s.matching == nil || !samePromptKeyFromGroup(s.matching, rec) {
		s.matching = newPromptGroup(rec)
		return
	}
	addPromptRecord(s.matching, rec)
}

func (s *promptGroupSelector) addRecent(rec prompttrace.Record) *promptGroup {
	if rec.Stage == "request" {
		group := newPromptGroup(rec)
		s.recent = append(s.recent, group)
		if len(s.recent) > maxPromptGroupsInMemory {
			s.recent = s.recent[1:]
		}
		return group
	}
	for i := len(s.recent) - 1; i >= 0; i-- {
		if samePromptKeyFromGroup(s.recent[i], rec) {
			addPromptRecord(s.recent[i], rec)
			return s.recent[i]
		}
	}
	group := newPromptGroup(rec)
	s.recent = append(s.recent, group)
	if len(s.recent) > maxPromptGroupsInMemory {
		s.recent = s.recent[1:]
	}
	return group
}

func (s *promptGroupSelector) selected() *promptGroup {
	if s.last {
		return s.latest
	}
	return s.matching
}

func newPromptGroup(rec prompttrace.Record) *promptGroup {
	group := &promptGroup{}
	addPromptRecord(group, rec)
	return group
}

func addPromptRecord(group *promptGroup, rec prompttrace.Record) {
	group.records = append(group.records, rec)
	recCopy := rec
	switch rec.Stage {
	case "request":
		group.request = &recCopy
	case "response":
		group.response = &recCopy
	case "fallback":
		group.fallback = &recCopy
	}
}

func samePromptKeyFromGroup(group *promptGroup, rec prompttrace.Record) bool {
	if group.request != nil {
		return samePromptKey(*group.request, rec)
	}
	for _, existing := range group.records {
		if samePromptKey(existing, rec) {
			return true
		}
	}
	return false
}

func samePromptKey(a, b prompttrace.Record) bool {
	return a.Strategy == b.Strategy &&
		promptProviderCompatible(a.Provider, b.Provider) &&
		promptFieldCompatible(a.Model, b.Model) &&
		a.Seq == b.Seq &&
		int64SlicesEqual(a.OfferedSeqs, b.OfferedSeqs) &&
		a.BranchRef == b.BranchRef &&
		a.Generation == b.Generation
}

func promptFieldCompatible(a, b string) bool {
	return a == b || a == "" || b == ""
}

func promptProviderCompatible(a, b string) bool {
	return promptFieldCompatible(a, b) ||
		strings.HasPrefix(a, b+"+") ||
		strings.HasPrefix(b, a+"+")
}

func recordContainsSeq(rec prompttrace.Record, seq int64) bool {
	if rec.Seq == seq {
		return true
	}
	for _, offered := range rec.OfferedSeqs {
		if offered == seq {
			return true
		}
	}
	return false
}

func groupLatestTS(group *promptGroup) time.Time {
	var ts time.Time
	for _, rec := range group.records {
		if rec.TS.After(ts) {
			ts = rec.TS
		}
	}
	return ts
}

func promptGroupView(group *promptGroup) promptView {
	base := group.records[0]
	if group.request != nil {
		base = *group.request
	}
	view := promptView{
		Timestamp:          groupLatestTS(group).UTC().Format(time.RFC3339Nano),
		Strategy:           base.Strategy,
		Provider:           base.Provider,
		Model:              base.Model,
		Seq:                base.Seq,
		OfferedSeqs:        append([]int64(nil), base.OfferedSeqs...),
		BranchRef:          base.BranchRef,
		Generation:         base.Generation,
		DiffIncluded:       base.DiffIncluded,
		DiffCap:            base.DiffCap,
		Transform:          base.Transform,
		SystemPrompt:       base.SystemMessage,
		UserPrompt:         base.UserMessage,
		ToolSchema:         base.ToolSchema,
		Request:            append(json.RawMessage(nil), base.Request...),
		SubprocessEnvelope: append(json.RawMessage(nil), base.SubprocessEnvelope...),
		ValidationState:    "ok",
	}
	for _, rec := range group.records {
		view.Stages = append(view.Stages, rec.Stage)
		if rec.Error != "" {
			view.Errors = append(view.Errors, rec.Error)
		}
	}
	if group.response != nil {
		view.Response = group.response.Response
	}
	if group.fallback != nil {
		view.Fallback = group.fallback.Response
	}
	view.ValidationState = promptValidationState(view)
	return view
}

func promptValidationState(view promptView) string {
	if len(view.Errors) > 0 {
		return "error"
	}
	if view.Response != nil {
		if view.Response.ValidationError != "" {
			return "validation_error"
		}
		if view.Response.Error != "" {
			return "error"
		}
	}
	if view.Fallback != nil && view.Fallback.FallbackReason != "" {
		return "fallback"
	}
	return "ok"
}

func renderPromptHuman(out io.Writer, report promptReport) error {
	fmt.Fprintf(out, "Repo: %s\n", report.Repo)
	fmt.Fprintf(out, "Trace dir: %s\n", report.TraceDir)
	if report.Query.Seq > 0 {
		fmt.Fprintf(out, "Query: seq %d\n\n", report.Query.Seq)
	} else {
		fmt.Fprintln(out, "Query: last")
		fmt.Fprintln(out)
	}
	if !report.Found || report.Trace == nil {
		_, err := fmt.Fprintln(out, report.Message)
		return err
	}
	trace := report.Trace
	fmt.Fprintf(out, "Timestamp: %s\n", trace.Timestamp)
	fmt.Fprintf(out, "Strategy: %s\n", fallback(trace.Strategy, "-"))
	fmt.Fprintf(out, "Provider: %s\n", fallback(trace.Provider, "-"))
	fmt.Fprintf(out, "Model: %s\n", fallback(trace.Model, "-"))
	fmt.Fprintf(out, "Seq: %s\n", formatPromptSeq(trace.Seq))
	fmt.Fprintf(out, "Offered seqs: %s\n", formatPromptSeqs(trace.OfferedSeqs))
	if trace.BranchRef != "" || trace.Generation != 0 {
		fmt.Fprintf(out, "Branch: %s generation %d\n", fallback(trace.BranchRef, "-"), trace.Generation)
	}
	fmt.Fprintf(out, "Diff: included=%t cap=%d redaction_applied=%t truncated=%t bytes=%d/%d/%d\n",
		trace.DiffIncluded,
		trace.DiffCap,
		trace.Transform.RedactionApplied,
		trace.Transform.Truncated,
		trace.Transform.InputBytes,
		trace.Transform.RedactedBytes,
		trace.Transform.OutputBytes,
	)
	fmt.Fprintf(out, "Validation/fallback: %s", trace.ValidationState)
	if trace.Response != nil && trace.Response.ValidationError != "" {
		fmt.Fprintf(out, " validation_error=%q", trace.Response.ValidationError)
	}
	if trace.Fallback != nil && trace.Fallback.FallbackProvider != "" {
		fmt.Fprintf(out, " fallback_provider=%s", trace.Fallback.FallbackProvider)
	}
	if trace.Fallback != nil && trace.Fallback.FallbackReason != "" {
		fmt.Fprintf(out, " fallback_reason=%q", trace.Fallback.FallbackReason)
	}
	fmt.Fprintln(out)
	fmt.Fprintf(out, "Stages: %s\n\n", strings.Join(trace.Stages, ", "))
	printPromptBlock(out, "System prompt", trace.SystemPrompt)
	printPromptBlock(out, "User prompt", trace.UserPrompt)
	if trace.ToolSchema != nil {
		printPromptBlock(out, "Tool schema", mustPrettyJSON(trace.ToolSchema))
	}
	if len(trace.Request) > 0 {
		printPromptBlock(out, "Request envelope", prettyRawJSON(trace.Request))
	}
	if len(trace.SubprocessEnvelope) > 0 {
		printPromptBlock(out, "Subprocess envelope", prettyRawJSON(trace.SubprocessEnvelope))
	}
	if trace.Response != nil {
		printPromptBlock(out, "Response", mustPrettyJSON(trace.Response))
	}
	if trace.Fallback != nil {
		printPromptBlock(out, "Fallback", mustPrettyJSON(trace.Fallback))
	}
	return nil
}

func printPromptBlock(out io.Writer, label, body string) {
	fmt.Fprintf(out, "%s:\n", label)
	if body == "" {
		fmt.Fprintln(out, "  (empty)")
		fmt.Fprintln(out)
		return
	}
	for _, line := range strings.Split(strings.TrimRight(body, "\n"), "\n") {
		fmt.Fprintf(out, "  %s\n", line)
	}
	fmt.Fprintln(out)
}

func prettyRawJSON(raw json.RawMessage) string {
	var buf bytes.Buffer
	if err := json.Indent(&buf, raw, "", "  "); err != nil {
		return string(raw)
	}
	return buf.String()
}

func mustPrettyJSON(v any) string {
	raw, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return fmt.Sprintf("%v", v)
	}
	return string(raw)
}

func formatPromptSeq(seq int64) string {
	if seq == 0 {
		return "-"
	}
	return strconv.FormatInt(seq, 10)
}

func formatPromptSeqs(seqs []int64) string {
	if len(seqs) == 0 {
		return "-"
	}
	out := make([]string, 0, len(seqs))
	for _, seq := range seqs {
		out = append(out, strconv.FormatInt(seq, 10))
	}
	return strings.Join(out, ", ")
}

func int64SlicesEqual(a, b []int64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
