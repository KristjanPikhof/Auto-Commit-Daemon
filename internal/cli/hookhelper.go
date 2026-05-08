package cli

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/spf13/cobra"
)

// hookStdinLimit caps stdin payloads at 1 MiB. Hook payloads from harnesses are
// expected to be tiny JSON objects; this guards against runaway inputs.
const hookStdinLimit = 1024 * 1024

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
//
// All required-field output is buffered and only flushed once every required
// field resolves successfully. A missing or non-scalar required field at any
// position therefore leaves stdout completely untouched, so hook bodies that
// trail the call with `|| exit 0` short-circuit cleanly without partial reads.
//
// Empty-string scalars on required fields are treated identically to a missing
// field: they return the same field-not-found error so the same short-circuit
// applies. Optional empty-string scalars emit a blank line as before.
//
// Every emitted scalar is validated to contain no carriage return, line feed,
// or NUL byte before any output occurs; injection attempts surface as a
// descriptive error and never reach stdout.
//
// Stdin is bounded by hookStdinLimit (1 MiB). Payloads larger than that limit
// surface as a distinct truncation error rather than a generic JSON decode
// failure so hook logs show the real cause.
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

	// Read stdin into a bounded buffer so we can distinguish "JSON was
	// malformed" from "payload exceeded 1 MiB". We read up to limit+1 bytes;
	// anything past the limit means truncation.
	limited := io.LimitReader(in, hookStdinLimit+1)
	raw, err := io.ReadAll(limited)
	if err != nil {
		return fmt.Errorf("acd hook-stdin-extract: read stdin: %w", err)
	}
	if len(raw) > hookStdinLimit {
		return fmt.Errorf("acd hook-stdin-extract: stdin exceeded %d byte limit (1 MiB); refusing to decode truncated JSON", hookStdinLimit)
	}

	var payload map[string]any
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	if err := dec.Decode(&payload); err != nil {
		return fmt.Errorf("acd hook-stdin-extract: decode stdin JSON: %w", err)
	}

	var buf bytes.Buffer
	for _, field := range fields {
		raw := strings.TrimSpace(field)
		optional := strings.HasSuffix(raw, "?")
		key := strings.TrimSuffix(raw, "?")

		v, ok := payload[key]
		if !ok || v == nil {
			if optional {
				buf.WriteByte('\n')
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

		// Empty-string scalars on required fields collapse to the same
		// field-not-found error so hook bodies short-circuit identically
		// regardless of whether the harness omitted the key or sent "".
		if s == "" && !optional {
			return fmt.Errorf("acd hook-stdin-extract: field %q not found", key)
		}

		// Reject any control byte that would corrupt the bash `read`
		// pipeline driving this helper: CR, LF, or NUL would either
		// inject extra fields or terminate cwd/session_id mid-string.
		if idx := strings.IndexAny(s, "\r\n\x00"); idx >= 0 {
			return fmt.Errorf("acd hook-stdin-extract: field %q contains forbidden byte 0x%02x at offset %d", key, s[idx], idx)
		}

		buf.WriteString(s)
		buf.WriteByte('\n')
	}

	// Flush the staged output only after every required field resolved.
	if _, err := out.Write(buf.Bytes()); err != nil {
		return err
	}
	return nil
}
