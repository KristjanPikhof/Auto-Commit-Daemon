package ai

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/prompttrace"
)

type probeCaptureProvider struct {
	called atomic.Int32
	ctx    context.Context
	cc     CommitContext
	err    error
	wait   bool
}

func (*probeCaptureProvider) Name() string { return "probe-capture" }

func (p *probeCaptureProvider) Generate(ctx context.Context, cc CommitContext) (Result, error) {
	p.called.Add(1)
	p.ctx = ctx
	p.cc = cc
	if p.wait {
		<-ctx.Done()
		return Result{}, ctx.Err()
	}
	if p.err != nil {
		return Result{}, p.err
	}
	return Result{Subject: "Probe provider", Body: "- hidden response text"}, nil
}

func TestProviderProbeUsesOneSyntheticSecretSafeRequest(t *testing.T) {
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
	if provider.cc.Path != "acd-settings-probe.txt" || provider.cc.Op != "modify" {
		t.Fatalf("synthetic inventory path=%q op=%q", provider.cc.Path, provider.cc.Op)
	}
	if provider.cc.RepoRoot != "" || provider.cc.Branch != "" ||
		provider.cc.DiffText != "" || len(provider.cc.Commits) != 0 ||
		len(provider.cc.MultiOp) != 0 || provider.cc.OldPath != "" ||
		!provider.cc.Now.IsZero() {
		t.Fatalf("probe leaked non-synthetic metadata: %+v", provider.cc)
	}
	if !result.Success || result.Error != "" || result.Provider != "probe-capture" {
		t.Fatalf("result=%+v", result)
	}
}

func TestProviderProbeCancellationIsBoundedAndSanitized(t *testing.T) {
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

func TestProviderProbeRedactsProviderResponseErrors(t *testing.T) {
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
