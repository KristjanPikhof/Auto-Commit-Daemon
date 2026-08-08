package cli

import (
	"errors"
	"fmt"
)

const (
	ExitOK             = 0
	ExitInternal       = 1
	ExitInvalid        = 2
	ExitActionRequired = 3
	ExitUnavailable    = 4
)

// CommandError carries the stable process-exit and JSON error contracts.
// Message must already be safe for terminal and JSON output.
type CommandError struct {
	Code      string
	Message   string
	Exit      int
	Retryable bool
	Details   map[string]any
	rendered  bool
}

func (e *CommandError) Error() string { return e.Message }

func (e *CommandError) Unwrap() error { return nil }

func commandError(err error, code string, exit int, retryable bool) error {
	if err == nil {
		return nil
	}
	var existing *CommandError
	if errors.As(err, &existing) {
		return err
	}
	return &CommandError{
		Code: code, Message: err.Error(), Exit: exit, Retryable: retryable,
	}
}

func invalidCommandError(format string, args ...any) error {
	return &CommandError{Code: "invalid_argument", Message: fmt.Sprintf(format, args...), Exit: ExitInvalid}
}

func actionRequiredError(code, message string) error {
	return &CommandError{Code: code, Message: message, Exit: ExitActionRequired}
}

func unavailableError(message string) error {
	return &CommandError{Code: "supervisor_unavailable", Message: message, Exit: ExitUnavailable, Retryable: true}
}

// ExitCode returns the public process exit code for an Execute error.
func ExitCode(err error) int {
	if err == nil {
		return ExitOK
	}
	var commandErr *CommandError
	if errors.As(err, &commandErr) && commandErr.Exit >= ExitInternal && commandErr.Exit <= ExitUnavailable {
		return commandErr.Exit
	}
	return ExitInternal
}

// ErrorRendered reports whether Execute already emitted the error as the one
// JSON response. Human-mode errors are intentionally left for main to print.
func ErrorRendered(err error) bool {
	var commandErr *CommandError
	return errors.As(err, &commandErr) && commandErr.rendered
}
