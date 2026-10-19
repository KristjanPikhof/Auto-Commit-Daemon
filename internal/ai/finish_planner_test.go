package ai

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/finish"
)

type finishFakeTransport struct {
	requests []FinishPlanRequest
	exchange func(context.Context, FinishPlanRequest) ([]byte, error)
	padding  int
}

func (*finishFakeTransport) Name() string { return "fake" }
func (p *finishFakeTransport) EncodeFinishRequest(request FinishPlanRequest) ([]byte, error) {
	body, err := json.Marshal(request)
	return append(body, strings.Repeat(" ", p.padding)...), err
}
func (p *finishFakeTransport) ExchangeFinish(ctx context.Context, body []byte) ([]byte, error) {
	var request FinishPlanRequest
	if err := json.Unmarshal(body, &request); err != nil {
		return nil, err
	}
	p.requests = append(p.requests, request)
	return p.exchange(ctx, request)
}
func finishTestEntry(char string) finish.Entry {
	return finish.Entry{Mode: "100644", OID: strings.Repeat(char, 40)}
}
func finishTestEvidence() finish.Evidence {
	base := finish.Tree{"settings.go": finishTestEntry("a")}
	target := finish.Tree{"settings.go": finishTestEntry("b")}
	return finish.Evidence{Base: base, Target: target, Changes: finish.NetChanges(base, target, "author-a")}
}
func finishTestOptions(evidence finish.Evidence) FinishOptions {
	options := FinishOptions{Deadline: time.Now().Add(time.Minute), Style: finish.StyleImperative, Purpose: "Keep authored settings ahead of defaults", DiffEgress: true, RequireCompletionContext: true}
	for _, change := range evidence.Changes {
		options.Diffs = append(options.Diffs, FinishDiff{ChangeID: change.ID, Text: "- previous recorded value\n+ next recorded value\n"})
	}
	return options
}
func finishTestOutput(goals ...finish.Goal) []byte {
	body, err := json.Marshal(struct {
		Goals []finish.Goal `json:"goals"`
	}{goals})
	if err != nil {
		panic(err)
	}
	return body
}
func finishTestGoal(evidence finish.Evidence) finish.Goal {
	ids := make([]string, 0, len(evidence.Changes))
	for _, change := range evidence.Changes {
		ids = append(ids, change.ID)
	}
	return finish.Goal{Purpose: "Preserve explicit settings", Message: "Preserve explicit settings\n\n- Keep authored values ahead of defaults.", ChangeIDs: ids}
}
func finishAssertError(t *testing.T, err error, code string) {
	t.Helper()
	var failure *FinishPlanningError
	if !errors.As(err, &failure) || failure.Code != code || !failure.Retryable() {
		t.Fatalf("error = %v, want retryable %s", err, code)
	}
}

func TestFinishIntentCompletedGoals(t *testing.T) {
	for _, style := range []string{finish.StyleImperative, finish.StyleConventional} {
		t.Run(style, func(t *testing.T) {
			// A reusable parser prerequisite precedes its feature consumer. The
			// broader local default, caller, tests and docs complete one goal.
			evidence := finish.Evidence{Base: finish.Tree{}, Target: finish.Tree{}}
			paths := []string{"parser.go", "parser_test.go", "finish.go", "finish_test.go", "settings.go", "cli.go", "settings_test.go", "docs/settings.md"}
			for index, path := range paths {
				entry := finishTestEntry(fmt.Sprintf("%x", index+1))
				evidence.Target[path] = entry
				change := finish.Change{ID: path, Path: path, After: entry, Author: "a"}
				if index >= 2 {
					change.DependsOn = []string{"parser.go"}
				}
				if index >= 4 {
					change.DependsOn = append(change.DependsOn, "finish.go")
				}
				evidence.Changes = append(evidence.Changes, change)
			}
			// A later unpublished correction of the same settings file stays
			// with its completed goal, not in a cosmetic follow-up commit.
			last := finishTestEntry("f")
			evidence.Changes = append(evidence.Changes, finish.Change{ID: "settings-correction", Path: "settings.go", Before: evidence.Target["settings.go"], After: last, Author: "a", DependsOn: []string{"settings.go"}})
			evidence.Target["settings.go"] = last
			goals := []finish.Goal{
				{Purpose: "Extract the reusable settings parser", Boundary: "Review parser independently of the new default", Message: "Extract reusable settings parser\n\n- Share parsing before changing setup behavior.", ChangeIDs: paths[:2]},
				{Purpose: "Add explicit offline finish", Boundary: "Review batch completion before changing setup defaults", Message: "Add explicit offline finish\n\n- Save a complete protected batch on request.", ChangeIDs: paths[2:4]},
				{Purpose: "Default new setups to offline finish", Boundary: "Revert default behavior while keeping explicit finish", Message: "Default new setups to offline finish\n\n- Keep initial protection independent of credentials.", ChangeIDs: append(slices.Clone(paths[4:]), "settings-correction")},
			}
			if style == finish.StyleConventional {
				goals[0].Message = "refactor: extract settings parser\n\n- Share parsing before changing setup behavior."
				goals[1].Message = "feat: add explicit offline finish\n\n- Save a complete protected batch on request."
				goals[2].Message = "feat: default new setups to offline finish\n\n- Keep initial protection independent of credentials."
			}
			provider := &finishFakeTransport{exchange: func(_ context.Context, request FinishPlanRequest) ([]byte, error) {
				if request.Style != style || request.Purpose != "New users should protect work without credentials" {
					t.Fatalf("request = %+v", request)
				}
				return finishTestOutput(goals[2], goals[1], goals[0]), nil
			}}
			options := finishTestOptions(evidence)
			options.Style = style
			options.Purpose = "New users should protect work without credentials"
			result, err := PlanFinish(context.Background(), provider, evidence, options)
			if err != nil {
				t.Fatal(err)
			}
			if result.Calls != 1 || len(result.Plan.Steps) != 3 || !maps.Equal(result.Plan.Steps[2].Tree, evidence.Target) {
				t.Fatalf("result = %+v", result)
			}
			if len(result.Plan.Steps[0].Tree) != 2 || len(result.Plan.Steps[1].Tree) != 4 || result.Plan.Steps[2].Purpose != goals[2].Purpose {
				t.Fatal("prerequisite or broader goal lost")
			}
		})
	}
}

func TestFinishIntentCombinedCorrection(t *testing.T) {
	evidence := finishTestEvidence()
	for _, bad := range []string{
		`{"goals":[{"purpose":"Fabricate","message":"Preserve explicit settings\n\n- Keep authored values ahead of defaults.","change_ids":["imagined"]}]}`,
		`{"goals":[{"purpose":"Incomplete","message":"fix(settings): change settings","change_ids":["settings.go"]}]}`,
		`{"goals":[],"tree":{"settings.go":"fabricated"}}`,
		`not json`,
	} {
		t.Run(bad[:min(25, len(bad))], func(t *testing.T) {
			calls := 0
			options := finishTestOptions(evidence)
			var deadlines []time.Time
			options.BeforeCall = func(ctx context.Context) error {
				calls++
				deadline, _ := ctx.Deadline()
				deadlines = append(deadlines, deadline)
				return nil
			}
			provider := &finishFakeTransport{exchange: func(_ context.Context, request FinishPlanRequest) ([]byte, error) {
				if request.Correction == nil {
					return []byte(bad), nil
				}
				_, expected := validateFinishOutput([]byte(bad), evidence, options)
				if request.Correction.ValidationError != finishCorrectionError(expected) || request.Correction.PreviousOutput != bad {
					t.Fatalf("correction = %+v", request.Correction)
				}
				return finishTestOutput(finishTestGoal(evidence)), nil
			}}
			result, err := PlanFinish(context.Background(), provider, evidence, options)
			if err != nil || result.Calls != 2 || calls != 2 || deadlines[0] != deadlines[1] || deadlines[0] != options.Deadline {
				t.Fatalf("result=%+v err=%v calls=%d deadlines=%v", result, err, calls, deadlines)
			}
		})
	}
	provider := &finishFakeTransport{exchange: func(context.Context, FinishPlanRequest) ([]byte, error) { return []byte(`{"goals":[]}`), nil }}
	result, err := PlanFinish(context.Background(), provider, evidence, finishTestOptions(evidence))
	finishAssertError(t, err, "invalid_provider_plan")
	if result.Calls != 2 {
		t.Fatalf("calls=%d", result.Calls)
	}
}

func TestFinishIntentSemanticQualityCorrection(t *testing.T) {
	evidence := finishTestEvidence()
	for _, style := range []string{finish.StyleImperative, finish.StyleConventional} {
		for _, scenario := range []string{"generic", "filename", "purpose", "boundary", "test claim", "default impact", "missing reason"} {
			t.Run(style+"/"+scenario, func(t *testing.T) {
				options := finishTestOptions(evidence)
				options.Style = style
				goal := finishTestGoal(evidence)
				if style == finish.StyleConventional {
					goal.Message = "fix: preserve explicit settings\n\n- Keep authored values ahead of defaults."
				}
				bad := goal
				switch scenario {
				case "missing reason":
					bad.Message = strings.Split(goal.Message, "\n")[0]
				case "generic":
					bad.Message = "Update files"
				case "filename":
					bad.Message = "Update settings.go"
				case "purpose":
					bad.Purpose = "file1"
				case "boundary":
					bad.Boundary = "file1"
				case "test claim":
					bad.Message += "\n- Tests passed."
				case "default impact":
					bad.Purpose = "Default protection to offline mode"
					bad.Message = "Default protection to offline mode"
					options.DiffEgress = true
					options.Purpose = "Default protection should not require credentials"
					options.Diffs = []FinishDiff{{ChangeID: "settings.go", Text: "- defaultStrategy = intent\n+ defaultStrategy = local\n"}}
				}
				if style == finish.StyleConventional && slices.Contains([]string{"generic", "filename", "default impact"}, scenario) {
					bad.Message = "fix: " + strings.ToLower(bad.Message[:1]) + bad.Message[1:]
				}
				provider := &finishFakeTransport{exchange: func(_ context.Context, request FinishPlanRequest) ([]byte, error) {
					if request.Correction == nil {
						return finishTestOutput(bad), nil
					}
					if request.Correction.ValidationError == "" {
						t.Fatal("quality correction missing exact reason")
					}
					return finishTestOutput(goal), nil
				}}
				result, err := PlanFinish(context.Background(), provider, evidence, options)
				if err != nil || result.Calls != 2 || result.Plan.Steps[0].Message != goal.Message {
					t.Fatalf("result=%+v err=%v", result, err)
				}
			})
		}
	}
}

func TestFinishIntentPrivacyAndSuppliedMessage(t *testing.T) {
	evidence := finishTestEvidence()
	evidence.Changes[0].Provenance = "local immutable provenance must stay local"
	options := finishTestOptions(evidence)
	options.Purpose = "Rotate password=abcdefgh12345678"
	options.Message = "Rotate service credentials\n\n- Replace password=abcdefgh12345678."
	options.Diffs = []FinishDiff{{ChangeID: "settings.go", Text: "+password=abcdefgh12345678\n", BeforeSize: 12, AfterSize: 20}}
	for _, allowed := range []bool{false, true} {
		t.Run(fmt.Sprint(allowed), func(t *testing.T) {
			options.DiffEgress = allowed
			provider := &finishFakeTransport{exchange: func(_ context.Context, request FinishPlanRequest) ([]byte, error) {
				body, _ := json.Marshal(request)
				if strings.Contains(string(body), "abcdefgh12345678") || request.Changes[0].Provenance != "" {
					t.Fatalf("private text sent: %s", body)
				}
				if (request.Diffs[0].Text != "") != allowed {
					t.Fatalf("diff sent without consent: %+v", request)
				}
				goal := finishTestGoal(evidence)
				goal.Message = request.Message
				return finishTestOutput(goal), nil
			}}
			result, err := PlanFinish(context.Background(), provider, evidence, options)
			if !allowed {
				var failure *FinishPlanningError
				if !errors.As(err, &failure) || failure.Code != "provider_diffs_required" || failure.Retryable() || result.Calls != 0 || len(provider.requests) != 0 {
					t.Fatalf("unapproved code sharing reached provider: %+v %v", result, err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if result.Plan.Steps[0].Message != options.Message || !result.Disclosure.Redacted || len(result.Disclosure.TextChangeIDs) != map[bool]int{false: 0, true: 1}[allowed] {
				t.Fatalf("result=%+v", result)
			}
		})
	}
	options.DiffEgress = true
	options.Diffs = []FinishDiff{{ChangeID: "settings.go", Binary: true, BeforeSize: 128, AfterSize: 256}}
	provider := &finishFakeTransport{exchange: func(_ context.Context, request FinishPlanRequest) ([]byte, error) {
		goal := finishTestGoal(evidence)
		goal.Message = request.Message
		return finishTestOutput(goal), nil
	}}
	result, err := PlanFinish(context.Background(), provider, evidence, options)
	if err != nil || len(result.Disclosure.BinaryChangeIDs) != 1 || len(result.Disclosure.TextChangeIDs) != 0 {
		t.Fatalf("binary result=%+v err=%v", result, err)
	}
}

func TestFinishIntentBoundsAndNoFallback(t *testing.T) {
	evidence := finishTestEvidence()
	for _, scenario := range []string{"transport failure", "budget", "expired", "missing deadline", "missing diff", "binary text", "per-path cap", "redaction cap", "aggregate cap", "serialized cap", "invalid purpose", "invalid identifiers", "no-op"} {
		t.Run(scenario, func(t *testing.T) {
			options := finishTestOptions(evidence)
			input := evidence
			code := ""
			paid := 0
			options.BeforeCall = func(context.Context) error { paid++; return nil }
			provider := &finishFakeTransport{exchange: func(context.Context, FinishPlanRequest) ([]byte, error) {
				return nil, errors.New("provider unavailable password=abcdefgh12345678")
			}}
			switch scenario {
			case "transport failure":
				code = "provider_failure"
			case "budget":
				code = "provider_budget"
				options.BeforeCall = func(context.Context) error { return errors.New("budget exhausted") }
			case "expired":
				code = "provider_deadline"
				options.Deadline = time.Now().Add(-time.Second)
			case "missing deadline":
				code = "invalid_options"
				options.Deadline = time.Time{}
			case "missing diff":
				code = "invalid_evidence"
				options.Diffs = nil
				options.DiffEgress = true
			case "binary text":
				code = "invalid_evidence"
				options.Diffs = []FinishDiff{{ChangeID: "settings.go", Binary: true, Text: "bytes"}}
			case "per-path cap":
				code = "provider_input_too_large"
				options.DiffEgress = true
				options.Diffs = []FinishDiff{{ChangeID: "settings.go", Text: strings.Repeat("x", FinishDiffByteCap+1)}}
			case "redaction cap":
				code = "provider_input_too_large"
				options.DiffEgress = true
				options.Diffs = []FinishDiff{{ChangeID: "settings.go", Text: strings.Repeat("token=12345678\n", FinishDiffByteCap/len("token=12345678\n"))}}
			case "aggregate cap":
				options.Diffs = nil
				code = "provider_input_too_large"
				options.DiffEgress = true
				input = finish.Evidence{Base: finish.Tree{}, Target: finish.Tree{}}
				for i := 0; i <= FinishEvidenceDiffByteCap/FinishDiffByteCap; i++ {
					path := fmt.Sprintf("file-%d.go", i)
					input.Target[path] = finishTestEntry("b")
					options.Diffs = append(options.Diffs, FinishDiff{ChangeID: path, Text: strings.Repeat("x", FinishDiffByteCap)})
				}
				input.Changes = finish.NetChanges(input.Base, input.Target, "author-a")
			case "serialized cap":
				code = "provider_input_too_large"
				provider.padding = FinishRequestByteCap
			case "invalid purpose":
				code = "invalid_options"
				options.Purpose = "\x1b[31m unsafe"
			case "invalid identifiers":
				code = "invalid_evidence"
				input.Changes = slices.Clone(evidence.Changes)
				input.Changes[0].ID = "bad\xff"
			case "no-op":
				input = finish.Evidence{Base: evidence.Base, Target: evidence.Base}
				options.Deadline = time.Time{}
			}
			result, err := PlanFinish(context.Background(), provider, input, options)
			if code == "" {
				if err != nil || len(result.Plan.Steps) != 0 {
					t.Fatalf("no-op result=%+v err=%v", result, err)
				}
			} else {
				finishAssertError(t, err, code)
				if strings.Contains(err.Error(), "abcdefgh12345678") {
					t.Fatal("error leaked credential")
				}
			}
			want := 0
			if scenario == "transport failure" {
				want = 1
			}
			if result.Calls != want || len(provider.requests) != want || paid != want {
				t.Fatalf("result=%+v requests=%d paid=%d", result, len(provider.requests), paid)
			}
		})
	}
	options := finishTestOptions(evidence)
	options.Deadline = time.Now().Add(25 * time.Millisecond)
	provider := &finishFakeTransport{exchange: func(ctx context.Context, _ FinishPlanRequest) ([]byte, error) { <-ctx.Done(); return nil, ctx.Err() }}
	result, err := PlanFinish(context.Background(), provider, evidence, options)
	if !errors.Is(err, context.DeadlineExceeded) || result.Calls != 1 {
		t.Fatalf("timeout result=%+v err=%v", result, err)
	}
}

func TestFinishIntentExactSerializedCapAndCorrectionCap(t *testing.T) {
	evidence := finishTestEvidence()
	options := finishTestOptions(evidence)
	provider := &finishFakeTransport{exchange: func(context.Context, FinishPlanRequest) ([]byte, error) {
		return finishTestOutput(finishTestGoal(evidence)), nil
	}}
	request, _, err := prepareFinishRequest(evidence, options)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := provider.EncodeFinishRequest(request)
	provider.padding = FinishRequestByteCap - len(body)
	result, err := PlanFinish(context.Background(), provider, evidence, options)
	if err != nil || result.Disclosure.RequestBytes[0] != FinishRequestByteCap {
		t.Fatalf("exact cap result=%+v err=%v", result, err)
	}
	provider.requests = nil
	provider.padding = 0
	provider.exchange = func(context.Context, FinishPlanRequest) ([]byte, error) {
		return []byte(strings.Repeat("x", FinishRequestByteCap)), nil
	}
	result, err = PlanFinish(context.Background(), provider, evidence, options)
	finishAssertError(t, err, "provider_input_too_large")
	if result.Calls != 1 || len(provider.requests) != 1 {
		t.Fatalf("oversized correction sent: %+v", result)
	}
}

func TestFinishIntentCorrectionConsumesBudget(t *testing.T) {
	evidence := finishTestEvidence()
	options := finishTestOptions(evidence)
	paid := 0
	options.BeforeCall = func(context.Context) error {
		paid++
		if paid > 1 {
			return errors.New("persisted planning budget exhausted")
		}
		return nil
	}
	provider := &finishFakeTransport{exchange: func(context.Context, FinishPlanRequest) ([]byte, error) { return []byte("invalid output"), nil }}
	result, err := PlanFinish(context.Background(), provider, evidence, options)
	finishAssertError(t, err, "provider_budget")
	if result.Calls != 1 || len(provider.requests) != 1 || paid != 2 {
		t.Fatalf("result=%+v requests=%d paid=%d", result, len(provider.requests), paid)
	}
}

func TestFinishIntentOpenAIRejectsFailureAndRedirect(t *testing.T) {
	for _, status := range []int{http.StatusServiceUnavailable, http.StatusTemporaryRedirect} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			var requests atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requests.Add(1)
				w.Header().Set("Location", "/redirected")
				w.WriteHeader(status)
			}))
			defer server.Close()
			provider := &OpenAIProvider{BaseURL: server.URL, APIKey: "test-key", HTTP: server.Client()}
			result, err := PlanFinish(context.Background(), provider, finishTestEvidence(), finishTestOptions(finishTestEvidence()))
			finishAssertError(t, err, "provider_failure")
			if result.Calls != 1 || requests.Load() != 1 {
				t.Fatalf("hidden HTTP calls: result=%+v requests=%d", result, requests.Load())
			}
		})
	}
}

func TestFinishIntentOpenAITransport(t *testing.T) {
	evidence := finishTestEvidence()
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Error(err)
		}
		if !strings.Contains(string(body), "Plan completed goals before assigning") || strings.Contains(string(body), "selected_seqs") || r.Header.Get("Authorization") != "Bearer test-key" {
			t.Errorf("bad request: %s", body)
		}
		arguments := string(finishTestOutput(finishTestGoal(evidence)))
		_ = json.NewEncoder(w).Encode(map[string]any{"choices": []any{map[string]any{"message": map[string]any{"tool_calls": []any{map[string]any{"function": map[string]any{"name": "finish_plan", "arguments": arguments}}}}}}})
	}))
	defer server.Close()
	cfg := ProviderConfig{Mode: "openai-compat", BaseURL: server.URL, APIKey: "test-key", Model: DefaultOpenAIModel, CommitStrategy: CommitStrategyIntent, DiffEgress: true}
	if _, _, err := BuildFinishTransport(cfg, nil); err == nil {
		t.Fatal("unapproved endpoint accepted")
	}
	transport, closer, err := BuildFinishTransport(cfg, []ConfirmationRequirement{ConfirmationEndpointCredentials, ConfirmationInsecureEndpointCredentials, ConfirmationDiffEgress})
	if err != nil {
		t.Fatal(err)
	}
	if closer != nil {
		defer closer.Close()
	}
	if requests.Load() != 0 {
		t.Fatal("provider construction made a request")
	}
	result, err := PlanFinish(context.Background(), transport, evidence, finishTestOptions(evidence))
	if err != nil || result.Calls != 1 || requests.Load() != 1 {
		t.Fatalf("result=%+v err=%v requests=%d", result, err, requests.Load())
	}
	for _, mode := range []string{"deterministic", "unknown", "subprocess:missing"} {
		cfg.Mode = mode
		if _, _, err := BuildFinishTransport(cfg, nil); err == nil {
			t.Fatalf("%s accepted without configured AI approval", mode)
		}
	}
}

func TestFinishIntentSubprocessOneExchange(t *testing.T) {
	skipIfWindows(t)
	for _, crash := range []bool{false, true} {
		t.Run(fmt.Sprint(crash), func(t *testing.T) {
			dir := t.TempDir()
			calls := filepath.Join(dir, "calls")
			response := `printf '%s\n' '{"version":1,"finish_plan":{"goals":[{"purpose":"Preserve settings","message":"Preserve explicit settings\n\n- Keep authored values ahead of defaults.","change_ids":["settings.go"]}]}}'`
			if crash {
				response = "exit 1"
			}
			bin := writePluginScript(t, dir, "finish", fmt.Sprintf("while IFS= read -r line; do\n  printf 'call\\n' >> '%s'\n  case \"$line\" in *'\"request_type\":\"finish_plan\"'*) ;; *) exit 2 ;; esac\n  %s\ndone\n", calls, response))
			provider := NewSubprocessProvider("finish", SubprocessOptions{LookPath: fixedLookPath("acd-provider-finish", bin), Stderr: io.Discard, Timeout: 10 * time.Second})
			defer provider.Close()
			result, err := PlanFinish(context.Background(), provider, finishTestEvidence(), finishTestOptions(finishTestEvidence()))
			if crash {
				finishAssertError(t, err, "provider_failure")
			} else if err != nil {
				t.Fatal(err)
			}
			body, readErr := os.ReadFile(calls)
			if readErr != nil {
				t.Fatal(readErr)
			}
			if string(body) != "call\n" || result.Calls != 1 {
				t.Fatalf("hidden retries: calls=%q result=%+v", body, result)
			}
		})
	}
}
