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
	return fmt.Sprintf("The AI provider rejected the request (HTTP %d).\nDetails: %s\nNext: Review the provider settings and account before retrying.", e.StatusCode, e.Detail)
}
