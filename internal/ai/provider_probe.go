package ai

import (
	"context"
	"errors"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/finish"
)

// ProviderProbeResult reports a synthetic connection test without provider text.
type ProviderProbeResult struct {
	Provider string
	Latency  time.Duration
	Success  bool
	Error    string
}

const maximumProviderProbeTimeout = 30 * time.Second

// ProbeProviderConfig strictly constructs, probes, and closes one provider.
// The probe uses fixed synthetic metadata and a fresh context with no prompt
// trace or repository-scoped values. The caller's cancellation still
// propagates, and the request is capped at 30 seconds.
func ProbeProviderConfig(ctx context.Context, cfg ProviderConfig) (ProviderProbeResult, error) {
	if err := ctx.Err(); err != nil {
		return failedProviderProbe(normalizeMode(cfg.Mode), 0, err)
	}
	provider, closer, err := buildFinishTransport(cfg)
	if err != nil {
		return failedProviderProbe(normalizeMode(cfg.Mode), 0, err)
	}
	if closer != nil {
		defer closer.Close()
	}
	return ProbeProvider(ctx, provider, cfg.Timeout, cfg.CommitFormat)
}

// ProbeProvider sends exactly one fixed synthetic Finish request. It does
// not compose fallback, mutate planner health, start an experiment, write a
// commit, or expose the returned subject/body.
func ProbeProvider(ctx context.Context, provider FinishTransport, timeout time.Duration, format CommitFormat) (ProviderProbeResult, error) {
	if provider == nil {
		return failedProviderProbe("", 0, errors.New("ai: provider probe requires a provider"))
	}
	if err := ctx.Err(); err != nil {
		return failedProviderProbe(provider.Name(), 0, err)
	}
	if timeout <= 0 || timeout > maximumProviderProbeTimeout {
		timeout = maximumProviderProbeTimeout
	}
	probeCtx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	stop := context.AfterFunc(ctx, cancel)
	defer stop()

	started := time.Now()
	entry := finish.Entry{OID: "1111111111111111111111111111111111111111", Mode: "100644"}
	evidence := finish.Evidence{
		Base:   finish.Tree{},
		Target: finish.Tree{"acd-settings-probe.txt": entry},
		Changes: []finish.Change{{
			ID: "probe", Path: "acd-settings-probe.txt", After: entry, Author: "synthetic",
		}},
	}
	options := FinishOptions{Style: string(normalizeCommitFormat(string(format))), Purpose: "Confirm provider connection"}
	// The fixed connection probe has no repository content or planning consent.
	request := FinishPlanRequest{Style: options.Style, Purpose: options.Purpose, Changes: evidence.Changes}
	body, err := provider.EncodeFinishRequest(request)
	if err == nil {
		err = ctx.Err()
		if err == nil {
			err = probeCtx.Err()
		}
	}
	if err == nil {
		var output []byte
		output, err = provider.ExchangeFinish(probeCtx, body)
		if err == nil {
			_, err = validateFinishOutput(output, evidence, options)
		}
	}
	latency := time.Since(started)
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			err = ctxErr
		}
		return failedProviderProbe(provider.Name(), latency, err)
	}
	return ProviderProbeResult{
		Provider: sanitizeProviderLabel(provider.Name()),
		Latency:  latency,
		Success:  true,
	}, nil
}

func failedProviderProbe(provider string, latency time.Duration, err error) (ProviderProbeResult, error) {
	clean := SanitizePlannerError(err.Error())
	cleanErr := error(errors.New(clean))
	if errors.Is(err, context.Canceled) {
		cleanErr = providerProbeContextError{message: clean, cause: context.Canceled}
	} else if errors.Is(err, context.DeadlineExceeded) {
		cleanErr = providerProbeContextError{message: clean, cause: context.DeadlineExceeded}
	}
	return ProviderProbeResult{
		Provider: sanitizeProviderLabel(provider),
		Latency:  latency,
		Error:    clean,
	}, cleanErr
}

type providerProbeContextError struct {
	message string
	cause   error
}

func (e providerProbeContextError) Error() string { return e.message }
func (e providerProbeContextError) Unwrap() error { return e.cause }
