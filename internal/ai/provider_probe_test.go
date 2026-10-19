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
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/finish"
	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/prompttrace"
)

type probeCaptureProvider struct {
	called  atomic.Int32
	ctx     context.Context
	request FinishPlanRequest
	err     error
	wait    bool
	output  []byte
}

func (*probeCaptureProvider) Name() string { return "probe-capture" }

func (*probeCaptureProvider) EncodeFinishRequest(request FinishPlanRequest) ([]byte, error) {
	return json.Marshal(request)
}
func (p *probeCaptureProvider) ExchangeFinish(ctx context.Context, body []byte) ([]byte, error) {
	p.called.Add(1)
	p.ctx = ctx
	if err := json.Unmarshal(body, &p.request); err != nil {
		return nil, err
	}
	if p.wait {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	if p.err != nil {
		return nil, p.err
	}
	if p.output != nil {
		return p.output, nil
	}
	message := "Confirm provider connection"
	if p.request.Style == "conventional" {
		message = "chore: confirm provider connection"
	}
	return json.Marshal(map[string]any{"goals": []finish.Goal{{Purpose: "Confirm provider connection", Message: message + "\n\n- Confirm planning works with synthetic input.", ChangeIDs: []string{"probe"}}}})
}

func TestFinishRemovalProviderProbeBothStylesAndNoCorrection(t *testing.T) {
	for _, style := range []CommitFormat{CommitFormatImperative, CommitFormatConventional} {
		t.Run(string(style), func(t *testing.T) {
			provider := &probeCaptureProvider{}
			if result, err := ProbeProvider(t.Context(), provider, time.Second, style); err != nil || !result.Success || provider.request.Style != string(style) || provider.called.Load() != 1 {
				t.Fatalf("style probe: %+v %v", result, err)
			}
			provider = &probeCaptureProvider{output: []byte(`{"goals":[]}`)}
			if result, err := ProbeProvider(t.Context(), provider, time.Second, style); err == nil || result.Success || provider.called.Load() != 1 {
				t.Fatalf("malformed probe retried: %+v %v calls=%d", result, err, provider.called.Load())
			}
			ctx, cancel := context.WithCancel(t.Context())
			cancel()
			provider = &probeCaptureProvider{}
			if _, err := ProbeProvider(ctx, provider, time.Second, style); !errors.Is(err, context.Canceled) || provider.called.Load() != 0 {
				t.Fatalf("cancelled probe called transport: %v", err)
			}
		})
	}
}

func TestFinishRemovalProviderProbeUsesOneSyntheticSecretSafeRequest(t *testing.T) {
	provider := &probeCaptureProvider{}
	traceCtx := prompttrace.With(context.Background(), nil, prompttrace.Metadata{
		BranchRef: "refs/heads/private",
		Seq:       42,
	})
	result, err := ProbeProvider(traceCtx, provider, time.Second, CommitFormatImperative)
	if err != nil {
		t.Fatal(err)
	}
	if provider.called.Load() != 1 {
		t.Fatalf("calls=%d", provider.called.Load())
	}
	if _, _, ok := prompttrace.From(provider.ctx); ok {
		t.Fatal("probe propagated prompt-trace metadata")
	}
	if len(provider.request.Changes) != 1 || provider.request.Changes[0].Path != "acd-settings-probe.txt" || len(provider.request.Diffs) != 0 || provider.request.DiffIncluded || provider.request.Correction != nil || len(provider.request.HistoryCommits) != 0 {
		t.Fatalf("probe leaked nonsynthetic metadata: %+v", provider.request)
	}
	if !result.Success || result.Error != "" || result.Provider != "probe-capture" {
		t.Fatalf("result=%+v", result)
	}
}

func TestFinishRemovalProviderProbeCancellationIsBoundedAndSanitized(t *testing.T) {
	provider := &probeCaptureProvider{wait: true}
	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(20*time.Millisecond, cancel)
	started := time.Now()
	result, err := ProbeProvider(ctx, provider, 5*time.Second, CommitFormatImperative)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error=%v", err)
	}
	if time.Since(started) > time.Second {
		t.Fatalf("cancellation took %v", time.Since(started))
	}
	if result.Success || result.Latency <= 0 || result.Error != context.Canceled.Error() {
		t.Fatalf("result=%+v", result)
	}
}

func TestFinishRemovalProviderProbeRedactsProviderResponseErrors(t *testing.T) {
	secret := "sk-super-secret-value"
	provider := &probeCaptureProvider{err: errors.New("request https://user:pass@example.test/private?token=x failed Authorization: Bearer " + secret)}
	result, err := ProbeProvider(context.Background(), provider, time.Second, CommitFormatImperative)
	if err == nil {
		t.Fatal("expected error")
	}
	for _, exposed := range []string{secret, "user:pass", "/private", "token=x"} {
		if strings.Contains(err.Error(), exposed) || strings.Contains(result.Error, exposed) {
			t.Fatalf("probe error exposed %q: result=%+v err=%v", exposed, result, err)
		}
	}
	if !strings.Contains(result.Error, "[REDACTED]") {
		t.Fatalf("result error=%q", result.Error)
	}
}

func TestFinishRemovalProviderProbeSanitizesWrappedDeadlineAndPreservesClassification(t *testing.T) {
	provider := &probeCaptureProvider{err: fmt.Errorf(
		"request https://user:pass@example.test/private?token=x: %w",
		context.DeadlineExceeded,
	)}
	result, err := ProbeProvider(context.Background(), provider, time.Second, CommitFormatImperative)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("error=%v", err)
	}
	for _, exposed := range []string{"user:pass", "/private", "token=x"} {
		if strings.Contains(err.Error(), exposed) || strings.Contains(result.Error, exposed) {
			t.Fatalf("wrapped timeout exposed %q: result=%+v err=%v", exposed, result, err)
		}
	}
}

func TestFinishRemovalStrictTransportReturnsPrimaryResponseFailure(t *testing.T) {
	secret := "sk-primary-response-secret"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "upstream rejected "+secret, http.StatusBadGateway)
	}))
	defer server.Close()
	provider, closer, err := buildFinishTransport(ProviderConfig{
		Mode: "openai-compat", BaseURL: server.URL, APIKey: secret,
		Model: "test-model",
	})
	if err != nil {
		t.Fatal(err)
	}
	if closer != nil {
		defer closer.Close()
	}
	body, err := provider.EncodeFinishRequest(FinishPlanRequest{Style: "imperative", Purpose: "Confirm provider connection"})
	if err != nil {
		t.Fatal(err)
	}
	_, err = provider.ExchangeFinish(context.Background(), body)
	if err == nil {
		t.Fatal("strict transport unexpectedly succeeded")
	}
	// Raw provider errors are sanitized at the probe boundary used by the UI.
	result, probeErr := ProbeProvider(context.Background(), provider, time.Second, CommitFormatImperative)
	if probeErr == nil || result.Success || strings.Contains(result.Error, secret) {
		t.Fatalf("result=%+v err=%v", result, probeErr)
	}
}

func TestFinishRemovalProviderProbeConfigClosesOwnedSubprocess(t *testing.T) {
	dir := t.TempDir()
	marker := filepath.Join(dir, "closed")
	script := filepath.Join(dir, "acd-provider-probe")
	contents := "#!/bin/sh\n" +
		"IFS= read -r request || exit 1\n" +
		"printf '%s\\n' '{\"version\":1,\"finish_plan\":{\"goals\":[{\"purpose\":\"Confirm provider connection\",\"message\":\"Confirm provider connection\\n\\n- Confirm planning works with synthetic input.\",\"change_ids\":[\"probe\"]}]}}'\n" +
		"IFS= read -r ignored || printf closed > '" + marker + "'\n"
	if err := os.WriteFile(script, []byte(contents), 0o700); err != nil {
		t.Fatal(err)
	}
	result, err := ProbeProviderConfig(context.Background(), ProviderConfig{
		Mode: "subprocess:probe",
		subprocessLookPath: func(name string) (string, error) {
			if name != "acd-provider-probe" {
				return "", errors.New("unexpected binary")
			}
			return script, nil
		},
		subprocessStderr: io.Discard,
	})
	if err != nil || !result.Success {
		t.Fatalf("result=%+v err=%v", result, err)
	}
	if _, err := os.Stat(marker); err != nil {
		t.Fatalf("subprocess close marker: %v", err)
	}
}
