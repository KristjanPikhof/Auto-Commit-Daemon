package ai

import (
	"errors"
	"fmt"
	"os/exec"
)

// ProviderHTTPError preserves response status for retry and configuration
// decisions. Explanatory text is never parsed to choose a recovery path.
type ProviderHTTPError struct {
	StatusCode int
	Detail     string
}

func (e *ProviderHTTPError) Error() string {
	return fmt.Sprintf("openai-compat: http %d: %s", e.StatusCode, e.Detail)
}

type ProviderConfigurationError struct{ Err error }

func (e *ProviderConfigurationError) Error() string { return e.Err.Error() }
func (e *ProviderConfigurationError) Unwrap() error { return e.Err }

func ProviderNeedsConfiguration(err error) bool {
	var configuration *ProviderConfigurationError
	if errors.As(err, &configuration) {
		return true
	}
	var response *ProviderHTTPError
	if errors.As(err, &response) {
		return response.StatusCode == 401 || response.StatusCode == 403 || response.StatusCode == 404
	}
	return errors.Is(err, exec.ErrNotFound)
}
