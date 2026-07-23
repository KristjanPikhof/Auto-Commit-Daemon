package ai

import (
	"context"
	"errors"
	"io"
	"time"
)

// ProviderProbeResult is deliberately smaller than a provider Result. It
// confirms only that the selected provider returned a valid commit-message
// shape; model/plugin text is never returned to the settings surface.
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
	provider, closer, err := BuildStrictProvider(cfg)
	if err != nil {
		return failedProviderProbe(normalizeMode(cfg.Mode), 0, err)
	}
	if closer != nil {
		defer func(closer io.Closer) { _ = closer.Close() }(closer)
	}
	return ProbeProvider(ctx, provider, cfg.Timeout, cfg.CommitFormat)
}

// ProbeProvider sends exactly one fixed synthetic event to provider. It does
// not compose fallback, mutate planner health, start an experiment, write a
// commit, or expose the returned subject/body.
func ProbeProvider(ctx context.Context, provider Provider, timeout time.Duration, format CommitFormat) (ProviderProbeResult, error) {
	if provider == nil {
		return failedProviderProbe("", 0, errors.New("ai: provider probe requires a provider"))
	}
	if timeout <= 0 || timeout > maximumProviderProbeTimeout {
		timeout = maximumProviderProbeTimeout
	}
	probeCtx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	stop := context.AfterFunc(ctx, cancel)
	defer stop()

	started := time.Now()
	_, err := provider.Generate(probeCtx, CommitContext{
		Path:         "acd-settings-probe.txt",
		Op:           "modify",
		CommitFormat: effectiveCommitFormat(format),
	})
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
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		cleanErr = err
	}
	return ProviderProbeResult{
		Provider: sanitizeProviderLabel(provider),
		Latency:  latency,
		Error:    clean,
	}, cleanErr
}
