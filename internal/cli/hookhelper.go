package cli

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/spf13/cobra"
)

func newHookStdinExtractCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:    "hook-stdin-extract FIELD [FIELD...]",
		Short:  "Extract one or more top-level JSON fields from hook stdin",
		Hidden: true,
		Args:   cobra.MinimumNArgs(1),
		RunE: func(c *cobra.Command, args []string) error {
			return runHookStdinExtract(c.InOrStdin(), c.OutOrStdout(), args...)
		},
	}
	return cmd
}

// runHookStdinExtract decodes a JSON object from stdin and prints each
// requested top-level field as a newline-terminated scalar in argument order.
// A field name ending in ? is optional; missing optional fields emit an empty
// line so hook bodies can keep their positional reads while falling back.
// Required fields are emitted as soon as they are produced, so a missing or
// non-scalar field at position N still leaves fields 1..N-1 on stdout.
func runHookStdinExtract(in io.Reader, out io.Writer, fields ...string) error {
	if len(fields) == 0 {
		return errors.New("acd hook-stdin-extract: at least one field is required")
	}
	for i, f := range fields {
		key := strings.TrimSuffix(strings.TrimSpace(f), "?")
		if key == "" {
			return fmt.Errorf("acd hook-stdin-extract: field at position %d is empty", i)
		}
	}
	var payload map[string]any
	dec := json.NewDecoder(io.LimitReader(in, 1024*1024))
	dec.UseNumber()
	if err := dec.Decode(&payload); err != nil {
		return fmt.Errorf("acd hook-stdin-extract: decode stdin JSON: %w", err)
	}
	for _, field := range fields {
		raw := strings.TrimSpace(field)
		optional := strings.HasSuffix(raw, "?")
		key := strings.TrimSuffix(raw, "?")
		v, ok := payload[key]
		if !ok || v == nil {
			if optional {
				if _, err := fmt.Fprintln(out); err != nil {
					return err
				}
				continue
			}
			return fmt.Errorf("acd hook-stdin-extract: field %q not found", key)
		}
		var s string
		switch tv := v.(type) {
		case string:
			s = tv
		case json.Number:
			s = tv.String()
		case bool:
			s = fmt.Sprintf("%t", tv)
		default:
			return fmt.Errorf("acd hook-stdin-extract: field %q is not a scalar", key)
		}
		if _, err := fmt.Fprintln(out, s); err != nil {
			return err
		}
	}
	return nil
}
