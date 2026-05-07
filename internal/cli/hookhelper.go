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

func runHookStdinExtract(in io.Reader, out io.Writer, fields ...string) error {
	if len(fields) == 0 {
		return errors.New("acd hook-stdin-extract: at least one field is required")
	}
	for i, f := range fields {
		if strings.TrimSpace(f) == "" {
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
		key := strings.TrimSpace(field)
		v, ok := payload[key]
		if !ok || v == nil {
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
