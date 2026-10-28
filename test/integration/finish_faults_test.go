//go:build integration

package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/ai"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/config"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/finish"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/state"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/supervisor"
)

type finishChatRequest struct {
	Model string
	Plan  ai.FinishPlanRequest
}

func decodeFinishChatRequest(t *testing.T, r *http.Request) finishChatRequest {
	t.Helper()
	var wire struct {
		Model    string `json:"model"`
		Messages []struct {
			Role    string `json:"role"`
			Content string `json:"content"`
		} `json:"messages"`
	}
	if err := json.NewDecoder(r.Body).Decode(&wire); err != nil {
		t.Errorf("decode finish HTTP request: %v", err)
		return finishChatRequest{}
	}
	for _, message := range wire.Messages {
		if message.Role == "user" {
			var plan ai.FinishPlanRequest
			if err := json.Unmarshal([]byte(message.Content), &plan); err != nil {
				t.Errorf("decode finish evidence: %v", err)
			}
			return finishChatRequest{Model: wire.Model, Plan: plan}
		}
	}
	t.Error("finish request has no user evidence")
	return finishChatRequest{}
}

func writeFinishChatResponse(t *testing.T, w http.ResponseWriter, r finishChatRequest) {
	t.Helper()
	ids := make([]string, 0, len(r.Plan.Changes))
	for _, change := range r.Plan.Changes {
		ids = append(ids, change.ID)
	}
	message := r.Plan.Message
	if message == "" {
		message = r.Plan.Purpose
	}
	if message == "" {
		message = "Complete requested work"
	}
	if r.Plan.Style == "conventional" && r.Plan.Message == "" {
		message = "chore: " + message
	}
	if r.Plan.Message == "" {
		message += "\n\n- Keep the completed request separate from later edits."
	}
	arguments, err := json.Marshal(map[string]any{"goals": []finish.Goal{{Purpose: r.Plan.Purpose, Boundary: "Keep the supplied complete request together", Message: message, ChangeIDs: ids}}})
	if err != nil {
		t.Error(err)
		return
	}
	writeFinishChatArguments(t, w, string(arguments))
}

func writeFinishChatArguments(t *testing.T, w http.ResponseWriter, arguments string) {
	t.Helper()
	w.Header().Set("Content-Type", "application/json")
	err := json.NewEncoder(w).Encode(map[string]any{"choices": []any{map[string]any{"message": map[string]any{"tool_calls": []any{map[string]any{"type": "function", "function": map[string]any{"name": "finish_plan", "arguments": arguments}}}}}}})
	if err != nil {
		t.Error(err)
	}
}

func finishIntentFixture(t *testing.T, handler http.HandlerFunc, timeout string) *finishFixture {
	t.Helper()
	return finishIntentFixtureWithSettings(t, handler, timeout, nil)
}

func finishIntentFixtureWithSettings(t *testing.T, handler http.HandlerFunc, timeout string, extra map[string]string, confirmed ...ai.ConfirmationRequirement) *finishFixture {
	t.Helper()
	server, trust := newOpenAITestServer(t, handler)
	t.Cleanup(server.Close)
	f := newFinishFixture(t, "ACD_AI_API_KEY=fixture-key")
	draft := map[string]string{config.FieldCommitStrategy: "intent", config.FieldDiffEgress: "true", config.FieldProvider: "openai-compat", config.FieldBaseURL: server.URL, config.FieldCAFile: strings.TrimPrefix(trust, "ACD_AI_CA_FILE="), config.FieldModel: "finish-fixture", config.FieldTimeout: timeout}
	for key, value := range extra {
		draft[key] = value
	}
	f.Apply(t, t.Context(), draft, append(confirmed, ai.ConfirmationEndpointCredentials, ai.ConfirmationDiffEgress)...)
	return f
}

type finishCommandResult struct {
	OK         bool   `json:"ok"`
	NextAction string `json:"next_action"`
	Data       struct {
		state.FinishRequest
		Published      bool   `json:"published"`
		NoChanges      bool   `json:"no_changes"`
		CommitsCreated int    `json:"commits_created"`
		NewHead        string `json:"new_head"`
		StagedConsumed bool   `json:"staged_consumed"`
	} `json:"data"`
}

func finishRunJSON(t *testing.T, f *finishFixture, args ...string) finishCommandResult {
	t.Helper()
	result := f.Run(t, t.Context(), append(args, "--json")...)
	var decoded finishCommandResult
	if err := json.Unmarshal([]byte(result.Stdout), &decoded); err != nil {
		t.Fatalf("finish JSON exit=%d stdout=%s stderr=%s: %v", result.ExitCode, result.Stdout, result.Stderr, err)
	}
	if result.ExitCode == 0 != decoded.OK {
		t.Fatalf("finish exit/JSON disagree: exit=%d %+v", result.ExitCode, decoded)
	}
	return decoded
}

func finishStartRPC(t *testing.T, f *finishFixture, params map[string]any) state.FinishRequest {
	t.Helper()
	raw, err := json.Marshal(params)
	if err != nil {
		t.Fatal(err)
	}
	response, err := supervisor.DoWorker(t.Context(), supervisor.WorkerSocketPath(f.Roots, f.RepositoryID), supervisor.Request{Version: supervisor.ProtocolVersion, ID: "fixture-finish-start", Method: "finish_start", RepositoryID: f.RepositoryID, WorktreeID: f.WorktreeID, Params: raw}, 10*time.Second)
	if err != nil || !response.OK {
		t.Fatalf("finish RPC: %+v %v", response, err)
	}
	data, err := json.Marshal(response.Data)
	if err != nil {
		t.Fatal(err)
	}
	var r state.FinishRequest
	if err := json.Unmarshal(data, &r); err != nil {
		t.Fatal(err)
	}
	return r
}

func finishWaitRequest(t *testing.T, f *finishFixture, id string) state.FinishRequest {
	t.Helper()
	db := f.OpenDB(t)
	var r state.FinishRequest
	waitFor(t, "finish terminal request", 15*time.Second, func() bool {
		var err error
		r, err = state.LoadFinishRequest(t.Context(), db, id)
		return err == nil && r.State != state.FinishRunning && r.State != state.FinishPrepared
	})
	return r
}

func finishAssertSavedViews(t *testing.T, f *finishFixture, pending int) {
	t.Helper()
	var status struct {
		Data struct {
			Protected bool `json:"protected"`
			Outcome   struct {
				Pending   int   `json:"pending_changes"`
				Known     bool  `json:"counts_known"`
				Committed *bool `json:"branch_committed"`
			} `json:"publication_outcome"`
		} `json:"data"`
	}
	var result ExecResult
	waitFor(t, "complete saved status", 15*time.Second, func() bool {
		result = f.Run(t, t.Context(), "status", "--json")
		err := json.Unmarshal([]byte(result.Stdout), &status)
		return err == nil && status.Data.Protected && status.Data.Outcome.Known && status.Data.Outcome.Pending == pending && status.Data.Outcome.Committed != nil && !*status.Data.Outcome.Committed
	})
	// The list must agree even when a failed finish gives status a nonzero exit.
	var list ExecResult
	waitFor(t, "complete saved list", 15*time.Second, func() bool {
		list = runAcd(t, t.Context(), f.Env, "list", "--json")
		var rows struct {
			Data struct {
				Repositories []struct {
					Repo      string `json:"repo"`
					Protected bool   `json:"protected"`
					Outcome   struct {
						Pending   int   `json:"pending_changes"`
						Known     bool  `json:"counts_known"`
						Committed *bool `json:"branch_committed"`
					} `json:"publication_outcome"`
				} `json:"repos"`
			} `json:"data"`
		}
		if err := json.Unmarshal([]byte(list.Stdout), &rows); err != nil {
			return false
		}
		for _, row := range rows.Data.Repositories {
			if row.Repo == f.Repo {
				return row.Protected && row.Outcome.Known && row.Outcome.Pending == pending && row.Outcome.Committed != nil && !*row.Outcome.Committed
			}
		}
		return false
	})
}

func TestFinishFaultOneCombinedCorrection(t *testing.T) {
	t.Parallel()
	var calls atomic.Int32
	f := finishIntentFixture(t, func(w http.ResponseWriter, r *http.Request) {
		request := decodeFinishChatRequest(t, r)
		call := calls.Add(1)
		if call == 1 {
			if request.Plan.Correction != nil {
				t.Error("first call carried correction")
			}
			writeFinishChatArguments(t, w, `{"goals":[]}`)
			return
		}
		if request.Plan.Correction == nil {
			t.Error("second call lacks combined correction")
		}
		writeFinishChatResponse(t, w, request)
	}, "10s")
	writeFile(t, filepath.Join(f.Repo, "parser.go"), "package parser\n")
	writeFile(t, filepath.Join(f.Repo, "parser_test.go"), "package parser\n")
	f.Barrier(t, t.Context())
	if calls.Load() != 0 {
		t.Fatal("checkpoint called provider")
	}
	result := finishRunJSON(t, f, "finish", "Complete parser goal", "--message", "Complete parser goal")
	if !result.OK || !result.Data.Published || result.Data.CommitsCreated != 1 || result.Data.PlanningCalls != 2 || calls.Load() != 2 {
		t.Fatalf("bounded correction result %+v calls=%d", result, calls.Load())
	}
	if headSubject(t, f.Repo) != "Complete parser goal" || commitCount(t, f.Repo) != 2 {
		t.Fatal("complete batch message/count differs")
	}
}

func TestFinishFaultProviderFailureRetryKeepsFrozenTarget(t *testing.T) {
	for _, kind := range []string{"invalid", "timeout"} {
		t.Run(kind, func(t *testing.T) {
			t.Parallel()
			var calls atomic.Int32
			var succeeds atomic.Bool
			f := finishIntentFixture(t, func(w http.ResponseWriter, r *http.Request) {
				request := decodeFinishChatRequest(t, r)
				calls.Add(1)
				if succeeds.Load() {
					writeFinishChatResponse(t, w, request)
					return
				}
				if kind == "timeout" {
					<-r.Context().Done()
					return
				}
				writeFinishChatArguments(t, w, `{"goals":[{"purpose":"Invent unrelated work","message":"Complete unrelated work","change_ids":["not-recorded"]}]}`)
			}, "10s")
			writeFile(t, filepath.Join(f.Repo, "goal.txt"), "selected\n")
			failed := finishRunJSON(t, f, "finish", "Complete selected goal", "--message", "Complete selected goal")
			wantCalls := int32(2)
			if kind == "timeout" {
				wantCalls = 1
				if failed.Data.ReasonCode != "provider_deadline" || !strings.Contains(failed.NextAction, "--timeout 15m0s") {
					t.Fatalf("planning deadline lost its cause or longer retry: %+v", failed)
				}
			}
			if failed.OK || failed.Data.State != state.FinishFailed || calls.Load() != wantCalls || failed.Data.Published {
				t.Fatalf("failure %+v calls=%d", failed, calls.Load())
			}
			if strings.TrimSpace(runGitOK(t, f.Repo, "rev-parse", "HEAD")) != f.BaseHead {
				t.Fatal("failed provider moved branch")
			}
			writeFile(t, filepath.Join(f.Repo, "goal.txt"), "later edit\n")
			writeFile(t, filepath.Join(f.Repo, "later.txt"), "later file\n")
			f.Barrier(t, t.Context())
			finishAssertSavedViews(t, f, 2)
			succeeds.Store(true)
			retry := finishRunJSON(t, f, "finish", "--retry", failed.Data.ID)
			if !retry.OK || !retry.Data.Published || retry.Data.Attempt != 2 || !reflect.DeepEqual(retry.Data.Target, failed.Data.Target) || calls.Load() != wantCalls+1 {
				t.Fatalf("frozen retry %+v calls=%d", retry, calls.Load())
			}
			if got := runGitOK(t, f.Repo, "show", "HEAD:goal.txt"); got != "selected\n" {
				t.Fatalf("later bytes entered target: %q", got)
			}
			if _, err := runGit(f.Repo, "cat-file", "-e", "HEAD:later.txt"); err == nil {
				t.Fatal("later file entered frozen target")
			}
			if got, err := os.ReadFile(filepath.Join(f.Repo, "goal.txt")); err != nil || string(got) != "later edit\n" {
				t.Fatalf("live later edit changed: %q %v", got, err)
			}
			f.Barrier(t, t.Context())
			finishAssertSavedViews(t, f, 2)
		})
	}
}

func TestFinishFaultDisconnectedClientsReconnectToOneRequest(t *testing.T) {
	t.Parallel()
	entered := make(chan struct{}, 1)
	release := make(chan struct{}, 1)
	defer close(release)
	var calls atomic.Int32
	f := finishIntentFixture(t, func(w http.ResponseWriter, r *http.Request) {
		request := decodeFinishChatRequest(t, r)
		calls.Add(1)
		entered <- struct{}{}
		select {
		case <-release:
			writeFinishChatResponse(t, w, request)
		case <-r.Context().Done():
		}
	}, "30s")
	writeFile(t, filepath.Join(f.Repo, "goal.txt"), "selected\n")
	ctx, cancel := context.WithCancel(t.Context())
	resultCh := make(chan ExecResult, 1)
	go func() {
		resultCh <- f.Run(t, ctx, "finish", "Complete selected goal", "--message", "Complete selected goal", "--json")
	}()
	select {
	case <-entered:
	case <-time.After(15 * time.Second):
		t.Fatal("provider did not receive request")
	}
	cancel()
	<-resultCh
	same := finishStartRPC(t, f, map[string]any{"purpose": "Complete selected goal", "message": "Complete selected goal"})
	if same.State != state.FinishRunning || calls.Load() != 1 {
		t.Fatalf("disconnected job %+v calls=%d", same, calls.Load())
	}
	writeFile(t, filepath.Join(f.Repo, "goal.txt"), "later\n")
	f.Barrier(t, t.Context())
	// Buffered signal releases exactly the one running provider without leaving
	// an unbounded peer goroutine after an assertion fails.
	release <- struct{}{}
	completed := finishWaitRequest(t, f, same.ID)
	if completed.State != state.FinishCompleted || calls.Load() != 1 {
		t.Fatalf("detached request %+v calls=%d", completed, calls.Load())
	}
	raw, _ := json.Marshal(map[string]string{"request_id": same.ID})
	response, err := supervisor.DoWorker(t.Context(), supervisor.WorkerSocketPath(f.Roots, f.RepositoryID), supervisor.Request{Version: supervisor.ProtocolVersion, ID: "reconnected-status", Method: "finish_status", RepositoryID: f.RepositoryID, WorktreeID: f.WorktreeID, Params: raw}, 10*time.Second)
	var reconnected state.FinishRequest
	encoded, _ := json.Marshal(response.Data)
	decodeErr := json.Unmarshal(encoded, &reconnected)
	if err != nil || !response.OK || decodeErr != nil || reconnected.ID != same.ID || reconnected.Attempt != 1 || reconnected.State != state.FinishCompleted || calls.Load() != 1 || commitCount(t, f.Repo) != 2 {
		t.Fatalf("completed reconnect: %+v %+v %v %v", response, reconnected, err, decodeErr)
	}
	if runGitOK(t, f.Repo, "show", "HEAD:goal.txt") != "selected\n" {
		t.Fatal("frozen target changed")
	}
	f.Barrier(t, t.Context())
	finishAssertSavedViews(t, f, 1)
}

func TestFinishFaultVerificationFailureKeepsCompleteBatch(t *testing.T) {
	t.Parallel()
	var calls atomic.Int32
	countPath := filepath.Join(t.TempDir(), "checks")
	allowPath := filepath.Join(t.TempDir(), "allow")
	command := fmt.Sprintf("printf 'check\\n' >> %s; test -f %s", shellQuote(countPath), shellQuote(allowPath))
	f := finishIntentFixtureWithSettings(t, func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		writeFinishChatResponse(t, w, decodeFinishChatRequest(t, r))
	}, "10s", map[string]string{config.FieldIntentVerification: "full", config.FieldVerificationFullCommand: command, config.FieldVerificationFullTimeout: "5s"}, ai.ConfirmationVerificationCommand)
	writeFile(t, filepath.Join(f.Repo, "goal.txt"), "selected\n")
	failed := finishRunJSON(t, f, "finish", "Complete checked goal", "--message", "Complete checked goal")
	if failed.OK || failed.Data.State != state.FinishFailed || calls.Load() != 1 || strings.TrimSpace(runGitOK(t, f.Repo, "rev-parse", "HEAD")) != f.BaseHead {
		t.Fatalf("check failure %+v calls=%d", failed, calls.Load())
	}
	writeFile(t, filepath.Join(f.Repo, "later.txt"), "later\n")
	f.Barrier(t, t.Context())
	finishAssertSavedViews(t, f, 2)
	writeFile(t, allowPath, "approved check now succeeds\n")
	retry := finishRunJSON(t, f, "finish", "--retry", failed.Data.ID)
	raw, err := os.ReadFile(countPath)
	if !retry.OK || !retry.Data.Published || calls.Load() != 2 || retry.Data.Attempt != 2 || retry.Data.PlanningCalls != 1 || err != nil || strings.Count(string(raw), "check") != 2 {
		t.Fatalf("explicit retry lost frozen plan/check budget: %+v calls=%d checks=%q %v", retry, calls.Load(), raw, err)
	}
	if _, err := runGit(f.Repo, "cat-file", "-e", "HEAD:later.txt"); err == nil {
		t.Fatal("verification retry included later work")
	}
}

func TestFinishFaultSubprocessFailureDoesNotFallback(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	spawns := filepath.Join(dir, "spawns")
	script := fmt.Sprintf("#!/bin/sh\nprintf 'spawn\\n' >> %s\nIFS= read -r request || exit 1\nexit 7\n", shellQuote(spawns))
	writeFile(t, filepath.Join(dir, "acd-provider-fail"), script)
	if err := os.Chmod(filepath.Join(dir, "acd-provider-fail"), 0755); err != nil {
		t.Fatal(err)
	}
	f := newFinishFixture(t, "PATH="+dir+string(os.PathListSeparator)+os.Getenv("PATH"))
	f.Apply(t, t.Context(), map[string]string{config.FieldCommitStrategy: "intent", config.FieldDiffEgress: "true", config.FieldProvider: "subprocess:fail", config.FieldTimeout: "2s"}, ai.ConfirmationSubprocessExecution, ai.ConfirmationDiffEgress)
	writeFile(t, filepath.Join(f.Repo, "goal.txt"), "protected\n")
	failed := finishRunJSON(t, f, "finish", "Complete protected goal", "--message", "Complete protected goal")
	raw, err := os.ReadFile(spawns)
	if failed.OK || failed.Data.State != state.FinishFailed || failed.Data.PlanningCalls != 1 || err != nil || strings.Count(string(raw), "spawn") != 1 || commitCount(t, f.Repo) != 1 {
		t.Fatalf("subprocess failure fell back %+v spawns=%q %v", failed, raw, err)
	}
	f.Barrier(t, t.Context())
	finishAssertSavedViews(t, f, 1)
}

func TestFinishFaultLargeAssetsStayProtectedDuringProviderOutage(t *testing.T) {
	t.Parallel()
	var calls atomic.Int32
	f := finishIntentFixture(t, func(w http.ResponseWriter, r *http.Request) {
		request := decodeFinishChatRequest(t, r)
		calls.Add(1)
		paths := make(map[string]string)
		for _, change := range request.Plan.Changes {
			paths[change.ID] = change.Path
		}
		binaries, texts := 0, 0
		for _, diff := range request.Plan.Diffs {
			if strings.HasPrefix(paths[diff.ChangeID], "assets/") {
				binaries++
				if !diff.Binary || diff.Text != "" {
					t.Error("binary contents reached provider")
				}
			} else {
				texts++
				if diff.Binary || diff.Text == "" {
					t.Error("approved code evidence did not reach provider")
				}
			}
		}
		if !request.Plan.DiffIncluded || binaries != 4 || texts != 4 {
			t.Errorf("incomplete mixed evidence: shared=%t binaries=%d texts=%d", request.Plan.DiffIncluded, binaries, texts)
		}
		http.Error(w, "unavailable", http.StatusServiceUnavailable)
	}, "10s")
	names := []string{"eng_autocomplete.bin", "est_autocomplete.bin", "rus_autocomplete.bin", "spa_autocomplete.bin"}
	sizes := []int{14832809, 21685601, 18343302, 15617454}
	for i, name := range names {
		body := bytes.Repeat([]byte{byte(i + 1)}, sizes[i])
		body[0] = 0 // Git detects binary content from a NUL, not the .bin suffix.
		writeFile(t, filepath.Join(f.Repo, "assets", name), string(body))
	}
	writeFile(t, filepath.Join(f.Repo, "autocomplete.go"), "package autocomplete\n")
	writeFile(t, filepath.Join(f.Repo, "autocomplete_test.go"), "package autocomplete\n")
	writeFile(t, filepath.Join(f.Repo, "autocomplete.md"), "Use the bundled dictionaries.\n")
	writeFile(t, filepath.Join(f.Repo, "settings.py"), "def save(value): return value\n")
	runGitOK(t, f.Repo, "add", "settings.py")
	before := runGitOK(t, f.Repo, "ls-files", "--stage")
	failed := finishRunJSON(t, f, "finish", "Complete bundled autocomplete", "--include-staged", "--message", "Complete bundled autocomplete")
	if failed.OK || failed.Data.State != state.FinishFailed || calls.Load() != 1 || runGitOK(t, f.Repo, "ls-files", "--stage") != before || commitCount(t, f.Repo) != 1 {
		t.Fatalf("outage altered native state %+v calls=%d", failed, calls.Load())
	}
	db := f.OpenDB(t)
	projection, err := state.ReadCheckpointProjection(t.Context(), db.Path(), 1)
	if err != nil || projection.Latest == nil {
		t.Fatalf("checkpoint: %+v %v", projection, err)
	}
	for _, name := range names {
		path := "assets/" + name
		want := strings.TrimSpace(runGitOK(t, f.Repo, "hash-object", path))
		got := strings.TrimSpace(runGitOK(t, f.Repo, "rev-parse", projection.Latest.CommitOID+":"+path))
		if want != got {
			t.Fatalf("large asset not exactly protected: %s", path)
		}
	}
	finishAssertSavedViews(t, f, 8)
}

func TestFinishFaultRunningRequestRestartRequiresFrozenRetry(t *testing.T) {
	t.Parallel()
	entered := make(chan struct{}, 1)
	var calls atomic.Int32
	var retryAllowed atomic.Bool
	f := finishIntentFixture(t, func(w http.ResponseWriter, r *http.Request) {
		request := decodeFinishChatRequest(t, r)
		calls.Add(1)
		if !retryAllowed.Load() {
			entered <- struct{}{}
			<-r.Context().Done()
			return
		}
		writeFinishChatResponse(t, w, request)
	}, "30s")
	writeFile(t, filepath.Join(f.Repo, "goal.txt"), "frozen before restart\n")
	request := finishStartRPC(t, f, map[string]any{"purpose": "Complete restart goal", "message": "Complete restart goal"})
	select {
	case <-entered:
	case <-time.After(15 * time.Second):
		t.Fatal("provider did not start")
	}
	oldPID := readDaemonStatePID(f.Repo)
	f.Restart(t, t.Context())
	interrupted := finishWaitRequest(t, f, request.ID)
	if interrupted.State != state.FinishFailed || !reflect.DeepEqual(interrupted.Target, request.Target) || interrupted.Attempt != 1 {
		t.Fatalf("restart changed request: %+v", interrupted)
	}
	if readDaemonStatePID(f.Repo) == oldPID || calls.Load() != 1 || strings.TrimSpace(runGitOK(t, f.Repo, "rev-parse", "HEAD")) != f.BaseHead {
		t.Fatal("restart silently retried or published")
	}
	writeFile(t, filepath.Join(f.Repo, "goal.txt"), "later after restart\n")
	writeFile(t, filepath.Join(f.Repo, "later.txt"), "later goal\n")
	f.Barrier(t, t.Context())
	finishAssertSavedViews(t, f, 2)
	retryAllowed.Store(true)
	retried := finishRunJSON(t, f, "finish", "--retry", request.ID)
	if !retried.OK || retried.Data.ID != request.ID || retried.Data.Attempt != 2 || !reflect.DeepEqual(retried.Data.Target, request.Target) || calls.Load() != 2 {
		t.Fatalf("explicit frozen retry: %+v calls=%d", retried, calls.Load())
	}
	if runGitOK(t, f.Repo, "show", "HEAD:goal.txt") != "frozen before restart\n" {
		t.Fatal("retry selected later work")
	}
	if _, err := runGit(f.Repo, "show", "HEAD:later.txt"); err == nil {
		t.Fatal("later file entered frozen retry")
	}
	body, err := os.ReadFile(filepath.Join(f.Repo, "goal.txt"))
	if err != nil || string(body) != "later after restart\n" {
		t.Fatal("retry rewrote live bytes")
	}
	f.Barrier(t, t.Context())
	finishAssertSavedViews(t, f, 2)
}

func TestFinishLargeTextReviewsThenPublishesFrozenBatch(t *testing.T) {
	var reviews, plans, corrections atomic.Int32
	var f *finishFixture
	richSummary := strings.Repeat("Preserve the complete recorded library and its consumer. ", 20)
	large := "first recorded line\n" + strings.Repeat("complete library entry\n", 70000) + "last recorded line\n"
	f = finishIntentFixture(t, func(w http.ResponseWriter, r *http.Request) {
		if r.ContentLength > ai.FinishContextByteCap {
			t.Error("oversized context reached real HTTP provider")
			http.Error(w, "context window exceeded", http.StatusBadRequest)
			return
		}
		request := decodeFinishChatRequest(t, r)
		if request.Plan.Review != nil {
			first := reviews.Add(1) == 1
			if first {
				writeFile(t, filepath.Join(f.Repo, "library.txt"), "later edit during evidence review\n")
				f.Barrier(t, t.Context())
			}
			var observations []map[string]string
			for _, part := range request.Plan.Review.Parts {
				observations = append(observations, map[string]string{"id": part.ID, "summary": richSummary})
			}
			if first {
				observations[0]["summary"] = strings.Repeat("overlong factual summary ", 200)
			}
			if request.Plan.Correction != nil {
				corrections.Add(1)
				if !strings.Contains(request.Plan.Correction.ValidationError, "maximum is 4096") {
					t.Error("review correction omitted specific feedback")
				}
			}
			arguments, _ := json.Marshal(map[string]any{"observations": observations})
			_ = json.NewEncoder(w).Encode(map[string]any{"choices": []any{map[string]any{"message": map[string]any{"tool_calls": []any{map[string]any{"function": map[string]any{"name": "finish_evidence", "arguments": string(arguments)}}}}}}})
			return
		}
		plans.Add(1)
		if len(request.Plan.Reviews) < 2 || len(request.Plan.Changes) != 2 {
			t.Error("global plan lacks complete review context or membership")
		}
		for _, observation := range request.Plan.Reviews {
			if observation.Summary != richSummary {
				t.Error("global plan lost details from a rich review summary")
			}
		}
		writeFile(t, filepath.Join(f.Repo, "library.txt"), "later edit stays outside this finish\n")
		writeFinishChatResponse(t, w, request)
	}, "60s")
	writeFile(t, filepath.Join(f.Repo, "library.txt"), large)
	writeFile(t, filepath.Join(f.Repo, "consumer.txt"), "Use the recorded library\n")
	result := finishRunJSON(t, f, "finish", "Preserve completed library and consumer")
	if !result.OK || !result.Data.Published || result.Data.PlanningCalls != 1 || result.Data.EvidenceCalls != int(reviews.Load()) || reviews.Load() < 3 || plans.Load() != 1 || corrections.Load() != 1 {
		t.Fatalf("large real finish: %+v reviews=%d plans=%d", result, reviews.Load(), plans.Load())
	}
	if got := runGitOK(t, f.Repo, "show", "HEAD:library.txt"); got != large {
		t.Fatal("published library lost full contents")
	}
	if tree := strings.TrimSpace(runGitOK(t, f.Repo, "rev-parse", "HEAD^{tree}")); tree != result.Data.Target.TargetTree {
		t.Fatal("published tree differs from frozen target")
	}
	if got, err := os.ReadFile(filepath.Join(f.Repo, "library.txt")); err != nil || string(got) != "later edit stays outside this finish\n" {
		t.Fatal("later work was overwritten", err)
	}
	f.Barrier(t, t.Context())
	finishAssertSavedViews(t, f, 1)
}

func TestFinishReviewRetryResumesAfterWorkerRestart(t *testing.T) {
	var reviews, plans atomic.Int32
	var retry atomic.Bool
	var mu sync.Mutex
	accepted := map[string]bool{}
	f := finishIntentFixture(t, func(w http.ResponseWriter, r *http.Request) {
		// A rejected peer cancels in-flight HTTP uploads. Those partial bodies
		// are expected transport failures, not malformed provider requests.
		body, err := io.ReadAll(r.Body)
		if err != nil {
			return
		}
		r.Body = io.NopCloser(bytes.NewReader(body))
		request := decodeFinishChatRequest(t, r)
		if request.Plan.Review != nil {
			call := reviews.Add(1)
			if !retry.Load() && call > 4 {
				w.WriteHeader(http.StatusBadRequest)
				_, _ = w.Write([]byte(`{"error":{"message":"review interrupted"}}`))
				return
			}
			var observations []map[string]string
			mu.Lock()
			for _, part := range request.Plan.Review.Parts {
				if retry.Load() && accepted[part.ID] {
					t.Error("accepted fragment sent again after restart")
				}
				if !retry.Load() {
					accepted[part.ID] = true
				}
				observations = append(observations, map[string]string{"id": part.ID, "summary": "Preserve complete library entries and their consumers."})
			}
			mu.Unlock()
			arguments, _ := json.Marshal(map[string]any{"observations": observations})
			_ = json.NewEncoder(w).Encode(map[string]any{"choices": []any{map[string]any{"message": map[string]any{"tool_calls": []any{map[string]any{"function": map[string]any{"name": "finish_evidence", "arguments": string(arguments)}}}}}}})
			return
		}
		plans.Add(1)
		writeFinishChatResponse(t, w, request)
	}, "60s")
	for i := 0; i < 156; i++ {
		writeFile(t, filepath.Join(f.Repo, fmt.Sprintf("library-%03d.txt", i)), "Complete recorded library value\n")
	}
	writeFile(t, filepath.Join(f.Repo, "large.txt"), strings.Repeat("Complete readable library entry\n", 70000))
	failed := finishRunJSON(t, f, "finish", "Preserve completed library and consumers", "--timeout", "90s")
	if failed.OK || failed.Data.State != state.FinishFailed || failed.Data.ReviewedFragments == 0 || failed.Data.ReviewedFragments >= failed.Data.ReviewFragments || failed.Data.PlanningCalls != 0 || plans.Load() != 0 {
		t.Fatalf("partial reviews not durable: %+v", failed)
	}
	f.Restart(t, t.Context())
	writeFile(t, filepath.Join(f.Repo, "later.txt"), "Later work belongs to the next finish\n")
	f.Barrier(t, t.Context())
	retry.Store(true)
	completed := finishRunJSON(t, f, "finish", "--retry", failed.Data.ID, "--timeout", "120s")
	if !completed.OK || !completed.Data.Published || completed.Data.Attempt != 2 || completed.Data.ReusedFragments != failed.Data.ReviewedFragments || completed.Data.ReviewedFragments != completed.Data.ReviewFragments || completed.Data.PlanningCalls != 1 || plans.Load() != 1 || !reflect.DeepEqual(completed.Data.Target, failed.Data.Target) {
		t.Fatalf("retry lost frozen proof or review progress: %+v", completed)
	}
	if completed.Data.DeadlineTS-completed.Data.CreatedTS < 119 {
		t.Fatal("explicit retry timeout ignored")
	}
	if tree := strings.TrimSpace(runGitOK(t, f.Repo, "rev-parse", "HEAD^{tree}")); tree != failed.Data.Target.TargetTree {
		t.Fatal("retry published later work")
	}
	if body, err := os.ReadFile(filepath.Join(f.Repo, "later.txt")); err != nil || string(body) != "Later work belongs to the next finish\n" {
		t.Fatal("later edits overwritten", err)
	}
	f.Barrier(t, t.Context())
	finishAssertSavedViews(t, f, 1)
}
