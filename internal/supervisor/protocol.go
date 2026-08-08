// Package supervisor owns the user-level process that reconciles enabled ACD
// repositories into one isolated worker process per Git common directory.
package supervisor

import (
	"encoding/json"
	"fmt"
)

const ProtocolVersion = 1

type Request struct {
	Version      int             `json:"version"`
	ID           string          `json:"id"`
	Method       string          `json:"method"`
	RepositoryID string          `json:"repository_id,omitempty"`
	WorktreeID   string          `json:"worktree_id,omitempty"`
	DeadlineMS   int64           `json:"deadline_ms,omitempty"`
	Params       json.RawMessage `json:"params,omitempty"`
}

type Response struct {
	Version int            `json:"version"`
	ID      string         `json:"id"`
	OK      bool           `json:"ok"`
	Data    any            `json:"data,omitempty"`
	Error   *ProtocolError `json:"error,omitempty"`
}

type ProtocolError struct {
	Code      string `json:"code"`
	Message   string `json:"message"`
	Retryable bool   `json:"retryable"`
}

var validMethods = map[string]struct{}{
	"status": {}, "enable_repository": {}, "disable_repository": {},
	"checkpoint_barrier": {}, "hint": {}, "history": {},
	"restore_plan": {}, "restore_apply": {}, "repair": {}, "shutdown": {},
	"worker_environment": {},
}

func (r Request) Validate() error {
	if r.Version != ProtocolVersion {
		return fmt.Errorf("unsupported protocol version %d", r.Version)
	}
	if r.ID == "" {
		return fmt.Errorf("request id is required")
	}
	if _, ok := validMethods[r.Method]; !ok {
		return fmt.Errorf("unknown method %q", r.Method)
	}
	return nil
}
