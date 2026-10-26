package ai

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

const finishPluginReply = `{"version":1,"finish_plan":{"goals":[{"purpose":"Confirm provider connection","message":"Confirm provider connection","change_ids":["probe"]}]}}`

func TestFinishRemovalSubprocessCloseKillsDescendants(t *testing.T) {
	skipIfWindows(t)
	for _, ignoresEOF := range []bool{false, true} {
		t.Run(fmt.Sprintf("ignores EOF=%t", ignoresEOF), func(t *testing.T) {
			dir := t.TempDir()
			pidFile := filepath.Join(dir, "child.pid")
			script := fmt.Sprintf("sleep 30 >&2 &\nprintf '%%s' \"$!\" > '%s'\nIFS= read -r line || exit 1\nprintf '%%s\\n' '%s'\n", pidFile, finishPluginReply)
			if ignoresEOF {
				script += "sleep 30\n"
			} else {
				script += "while IFS= read -r line; do :; done\n"
			}
			bin := writePluginScript(t, dir, "descendants", script)
			p := NewSubprocessProvider("descendants", SubprocessOptions{LookPath: fixedLookPath("acd-provider-descendants", bin), Stderr: io.Discard, Timeout: 5 * time.Second})
			t.Cleanup(func() { _ = p.Close() })
			if _, err := p.ExchangeFinish(t.Context(), []byte(`{}`)); err != nil {
				t.Fatal(err)
			}
			raw, err := os.ReadFile(pidFile)
			if err != nil {
				t.Fatal(err)
			}
			pid, err := strconv.Atoi(string(raw))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = syscall.Kill(pid, syscall.SIGKILL) })
			session := p.plugin
			started := time.Now()
			if err := p.Close(); err != nil && !errors.Is(err, exec.ErrWaitDelay) {
				t.Fatal(err)
			}
			if elapsed := time.Since(started); elapsed > 7*time.Second {
				t.Fatalf("Close exceeded its cleanup budget: %s", elapsed)
			}
			if session.cmd.ProcessState == nil {
				t.Fatal("Close did not reap the plugin")
			}
			select {
			case <-session.done:
			default:
				t.Fatal("Close did not join the response reader")
			}
			deadline := time.Now().Add(time.Second)
			for {
				status, _ := exec.Command("ps", "-o", "stat=", "-p", strconv.Itoa(pid)).Output()
				state := strings.TrimSpace(string(status))
				if state == "" || strings.HasPrefix(state, "Z") {
					break
				}
				if time.Now().After(deadline) {
					t.Fatalf("plugin descendant survived Close: pid=%d state=%s", pid, state)
				}
				time.Sleep(10 * time.Millisecond)
			}
		})
	}
}

func TestFinishRemovalSubprocessLifecycle(t *testing.T) {
	skipIfWindows(t)
	t.Run("concurrent exchanges and cleanup", func(t *testing.T) {
		dir := t.TempDir()
		bin := writePluginScript(t, dir, "concurrent", "while IFS= read -r line; do printf '%s\\n' '"+finishPluginReply+"'; done\n")
		p := NewSubprocessProvider("concurrent", SubprocessOptions{LookPath: fixedLookPath("acd-provider-concurrent", bin), Stderr: io.Discard, Timeout: time.Second})
		t.Cleanup(func() { _ = p.Close() })
		body, err := p.EncodeFinishRequest(FinishPlanRequest{Style: "imperative"})
		if err != nil {
			t.Fatal(err)
		}
		const calls = 8
		var group sync.WaitGroup
		outcomes := make(chan error, calls)
		for range calls {
			group.Add(1)
			go func() {
				defer group.Done()
				out, err := p.ExchangeFinish(t.Context(), body)
				if err == nil && !strings.Contains(string(out), "Confirm provider connection") {
					err = errors.New("response lost")
				}
				outcomes <- err
			}()
		}
		group.Wait()
		close(outcomes)
		for err := range outcomes {
			if err != nil {
				t.Fatal(err)
			}
		}
		if err := p.Close(); err != nil {
			t.Fatal(err)
		}
		if _, err := p.ExchangeFinish(t.Context(), body); err == nil {
			t.Fatal("closed transport accepted exchange")
		}
		if err := p.Close(); err != nil {
			t.Fatal("second close", err)
		}
	})
	t.Run("crash has no retry and next explicit call respawns", func(t *testing.T) {
		dir := t.TempDir()
		marker := filepath.Join(dir, "spawned")
		script := fmt.Sprintf("printf 'spawn\\n' >> '%s'\nIFS= read -r line || exit 1\nif [ ! -f '%s.first' ]; then touch '%s.first'; exit 7; fi\nprintf '%%s\\n' '%s'\n", marker, marker, marker, finishPluginReply)
		bin := writePluginScript(t, dir, "crash", script)
		p := NewSubprocessProvider("crash", SubprocessOptions{LookPath: fixedLookPath("acd-provider-crash", bin), Stderr: io.Discard, Timeout: time.Second})
		defer p.Close()
		if _, err := p.ExchangeFinish(t.Context(), []byte(`{}`)); err == nil {
			t.Fatal("crash succeeded")
		}
		raw, err := os.ReadFile(marker)
		if err != nil || strings.Count(string(raw), "spawn") != 1 {
			t.Fatalf("implicit retry: %s %v", raw, err)
		}
		if _, err := p.ExchangeFinish(t.Context(), []byte(`{}`)); err != nil {
			t.Fatal("explicit call", err)
		}
		raw, err = os.ReadFile(marker)
		if err != nil || strings.Count(string(raw), "spawn") != 2 {
			t.Fatalf("explicit respawn: %s %v", raw, err)
		}
	})
	t.Run("deadline kills stalled exchange", func(t *testing.T) {
		dir := t.TempDir()
		bin := writePluginScript(t, dir, "timeout", "IFS= read -r line || exit 1\nsleep 10\n")
		p := NewSubprocessProvider("timeout", SubprocessOptions{LookPath: fixedLookPath("acd-provider-timeout", bin), Stderr: io.Discard, Timeout: 30 * time.Millisecond})
		defer p.Close()
		start := time.Now()
		_, err := p.ExchangeFinish(t.Context(), []byte(`{}`))
		if !errors.Is(err, context.DeadlineExceeded) || time.Since(start) > time.Second {
			t.Fatalf("unbounded deadline: %v %s", err, time.Since(start))
		}
	})
	t.Run("soft error keeps process and stderr remains local", func(t *testing.T) {
		home := t.TempDir()
		t.Setenv("HOME", home)
		dir := t.TempDir()
		script := "printf 'local diagnostic\\n' >&2\nn=0\nwhile IFS= read -r line; do n=$((n+1)); if [ \"$n\" -eq 1 ]; then printf '%s\\n' '{\"version\":1,\"error\":\"upstream unavailable\"}'; else printf '%s\\n' '" + finishPluginReply + "'; fi; done\n"
		bin := writePluginScript(t, dir, "soft", script)
		p := NewSubprocessProvider("soft", SubprocessOptions{LookPath: fixedLookPath("acd-provider-soft", bin), Timeout: time.Second})
		defer p.Close()
		if _, err := p.ExchangeFinish(t.Context(), []byte(`{}`)); err == nil {
			t.Fatal("soft failure succeeded")
		}
		if _, err := p.ExchangeFinish(t.Context(), []byte(`{}`)); err != nil {
			t.Fatal("soft error restarted or wedged process", err)
		}
		if err := p.Close(); err != nil {
			t.Fatal(err)
		}
		raw, err := os.ReadFile(filepath.Join(home, ".local", "state", "acd", "plugin-soft.log"))
		if err != nil || !strings.Contains(string(raw), "local diagnostic") {
			t.Fatalf("stderr lost: %s %v", raw, err)
		}
	})
}

func TestFinishRemovalOpenAITransportLimits(t *testing.T) {
	for _, scenario := range []string{"default model and auth", "deadline", "response cap", "wrong tool"} {
		t.Run(scenario, func(t *testing.T) {
			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				if scenario == "deadline" {
					select {
					case <-r.Context().Done():
					case <-time.After(200 * time.Millisecond):
					}
					return
				}
				if scenario == "response cap" {
					_, _ = io.WriteString(w, strings.Repeat("x", subprocessResponseByteCap+1))
					return
				}
				var request openAIToolRequest
				if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
					t.Error(err)
				}
				if request.Model != DefaultOpenAIModel || r.Header.Get("Authorization") != "Bearer test-key" {
					t.Errorf("model/auth mismatch: %+v", request)
				}
				name := "finish_plan"
				if scenario == "wrong tool" {
					name = "commit_message"
				}
				_ = json.NewEncoder(w).Encode(map[string]any{"choices": []any{map[string]any{"message": map[string]any{"tool_calls": []any{map[string]any{"function": map[string]any{"name": name, "arguments": "{\"goals\":[]}"}}}}}}})
			}))
			defer server.Close()
			p := &OpenAIProvider{BaseURL: server.URL, APIKey: "test-key", HTTP: server.Client()}
			body, err := p.EncodeFinishRequest(FinishPlanRequest{Style: "imperative"})
			if err != nil {
				t.Fatal(err)
			}
			ctx := t.Context()
			if scenario == "deadline" {
				var cancel context.CancelFunc
				ctx, cancel = context.WithTimeout(ctx, 30*time.Millisecond)
				defer cancel()
			}
			_, err = p.ExchangeFinish(ctx, body)
			if scenario == "default model and auth" && err != nil {
				t.Fatal(err)
			}
			if scenario != "default model and auth" && err == nil {
				t.Fatal("invalid transport success")
			}
			if calls.Load() != 1 {
				t.Fatalf("actual exchanges=%d", calls.Load())
			}
		})
	}
}

func TestFinishOpenAIRejectionExplainsCauseWithoutSecrets(t *testing.T) {
	for _, body := range []string{
		`{"error":{"message":"Maximum context length exceeded. API key: test-key; password=abcdefgh12345678"},"request_body":"private echoed code"}`,
		`{"error":{"message":"Use a smaller context window","code":400}}`,
		`{"error":{"message":"` + strings.Repeat("x", 2000) + `"}}`,
		`{"error":{"message":""},"secret":"private echoed code"}`,
		`not JSON with private echoed code`,
	} {
		t.Run(body[:min(32, len(body))], func(t *testing.T) {
			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				w.WriteHeader(http.StatusBadRequest)
				_, _ = io.WriteString(w, body)
			}))
			defer server.Close()
			p := &OpenAIProvider{BaseURL: server.URL, APIKey: "test-key", HTTP: server.Client()}
			_, err := p.ExchangeFinish(t.Context(), []byte(`{}`))
			var rejection *ProviderHTTPError
			if !errors.As(err, &rejection) || rejection.StatusCode != 400 || calls.Load() != 1 {
				t.Fatalf("rejection lost or retried: %v calls=%d", err, calls.Load())
			}
			for _, secret := range []string{"test-key", "abcdefgh12345678", "private echoed code"} {
				if strings.Contains(err.Error(), secret) {
					t.Fatalf("provider rejection leaked %q", secret)
				}
			}
			if len([]rune(rejection.Detail)) > plannerErrorCap {
				t.Fatal("unbounded rejection detail")
			}
			if strings.Contains(body, "Maximum context") && !strings.Contains(rejection.Detail, "Maximum context length exceeded") {
				t.Fatal("actionable cause was discarded")
			}
			if strings.Contains(body, "smaller context") && rejection.Detail != "Use a smaller context window" {
				t.Fatal("numeric provider code hid the message")
			}
		})
	}
}

func TestFinishEvidenceToolMustMatchRequest(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"choices": []any{map[string]any{"message": map[string]any{"tool_calls": []any{map[string]any{"function": map[string]any{"name": "finish_plan", "arguments": `{"observations":[]}`}}}}}}})
	}))
	defer server.Close()
	p := &OpenAIProvider{BaseURL: server.URL, APIKey: "test-key", HTTP: server.Client()}
	body, err := p.EncodeFinishRequest(FinishPlanRequest{Review: &FinishReviewTask{}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := p.ExchangeFinish(t.Context(), body); err == nil {
		t.Fatal("wrong evidence tool accepted")
	}
}

func TestFinishSubprocessEvidenceProtocol(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	dir := t.TempDir()
	bin := writePluginScript(t, dir, "review", "while IFS= read -r line; do printf '%s\\n' '{\"version\":1,\"finish_evidence\":{\"observations\":[{\"id\":\"part-1\",\"summary\":\"Preserve the recorded settings\"}]}}'; done\n")
	p := NewSubprocessProvider("review", SubprocessOptions{LookPath: fixedLookPath("acd-provider-review", bin), Timeout: time.Second})
	defer p.Close()
	body, err := p.EncodeFinishRequest(FinishPlanRequest{Review: &FinishReviewTask{Parts: []FinishReviewPart{{ID: "part-1", ChangeID: "settings.go", Part: 1, Parts: 1, Text: "+ authored settings\n"}}}})
	if err != nil || !strings.Contains(string(body), `"request_type":"finish_evidence"`) {
		t.Fatalf("review protocol: %s %v", body, err)
	}
	raw, err := p.ExchangeFinish(t.Context(), body)
	if err != nil || !strings.Contains(string(raw), "Preserve the recorded settings") {
		t.Fatalf("evidence reply %s %v", raw, err)
	}
}
