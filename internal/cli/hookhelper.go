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
		Use:    "hook-stdin-extract FIELD",
		Short:  "Extract a top-level JSON field from hook stdin",
		Hidden: true,
		Args:   cobra.ExactArgs(1),
		RunE: func(c *cobra.Command, args []string) error {
			return runHookStdinExtract(c.InOrStdin(), c.OutOrStdout(), args[0])
		},
	}
	return cmd
}

func runHookStdinExtract(in io.Reader, out io.Writer, field string) error {
	field = strings.TrimSpace(field)
	if field == "" {
		return errors.New("acd hook-stdin-extract: field is required")
	}
	var payload map[string]any
	dec := json.NewDecoder(io.LimitReader(in, 1024*1024))
	dec.UseNumber()
	if err := dec.Decode(&payload); err != nil {
		return fmt.Errorf("acd hook-stdin-extract: decode stdin JSON: %w", err)
	}
	v, ok := payload[field]
	if !ok || v == nil {
		return fmt.Errorf("acd hook-stdin-extract: field %q not found", field)
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
		return fmt.Errorf("acd hook-stdin-extract: field %q is not a scalar", field)
	}
	_, err := fmt.Fprintln(out, s)
	return err
}
