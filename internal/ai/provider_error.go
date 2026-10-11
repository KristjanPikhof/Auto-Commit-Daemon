package ai

import (
	"fmt"
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
