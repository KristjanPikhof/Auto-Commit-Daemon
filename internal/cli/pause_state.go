package cli

type pauseInfo struct {
	Source           string `json:"source"`
	Reason           string `json:"reason,omitempty"`
	SetAt            string `json:"set_at,omitempty"`
	ExpiresAt        string `json:"expires_at,omitempty"`
	RemainingSeconds int64  `json:"remaining_seconds,omitempty"`
}
