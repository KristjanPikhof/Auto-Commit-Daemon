package prompttrace

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// Dir returns the default prompt trace directory for a repo git dir.
func Dir(gitDir string) string {
	return filepath.Join(gitDir, "acd", "prompt-trace")
}

type ReadOptions struct {
	GitDir string
	Dir    string
}

// Walk streams prompt trace JSONL records without accumulating them in memory.
// Missing trace directories are treated as empty traces.
func Walk(ctx context.Context, opts ReadOptions, fn func(Record) error) error {
	if fn == nil {
		return fmt.Errorf("prompttrace: nil walk function")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	dir := opts.Dir
	if dir == "" {
		if opts.GitDir == "" {
			return fmt.Errorf("prompttrace: Dir or GitDir required")
		}
		dir = Dir(opts.GitDir)
	}
	info, err := os.Lstat(dir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return fmt.Errorf("prompttrace: stat dir: %w", err)
	}
	if info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("prompttrace: trace dir must not be a symlink: %s", dir)
	}
	if !info.IsDir() {
		return fmt.Errorf("prompttrace: trace dir is not a directory: %s", dir)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return fmt.Errorf("prompttrace: read dir: %w", err)
	}
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].Name() < entries[j].Name()
	})

	for _, entry := range entries {
		if err := ctx.Err(); err != nil {
			return err
		}
		if entry.Type()&os.ModeSymlink != 0 || entry.IsDir() || !strings.HasSuffix(entry.Name(), ".jsonl") {
			continue
		}
		if err := walkFile(ctx, filepath.Join(dir, entry.Name()), fn); err != nil {
			return err
		}
	}
	return nil
}

// Read reads prompt trace JSONL records from disk without creating files or
// mutating repo state. Missing trace directories are treated as empty traces.
func Read(ctx context.Context, opts ReadOptions) ([]Record, error) {
	var records []Record
	if err := Walk(ctx, opts, func(rec Record) error {
		records = append(records, rec)
		return nil
	}); err != nil {
		return nil, err
	}
	return records, nil
}

func walkFile(ctx context.Context, path string, fn func(Record) error) error {
	f, err := openRegularNoFollow(path, os.O_RDONLY, 0)
	if err != nil {
		return fmt.Errorf("prompttrace: open %s: %w", path, err)
	}
	defer f.Close()

	reader := bufio.NewReader(f)
	lineNo := 0
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		line, err := reader.ReadBytes('\n')
		if len(line) > 0 {
			lineNo++
			line = bytes.TrimSpace(line)
			if len(line) > 0 {
				rec, derr := decodeRecord(line)
				if derr != nil {
					continue
				}
				if err := fn(rec); err != nil {
					return err
				}
			}
		}
		if err == nil {
			continue
		}
		if errors.Is(err, io.EOF) {
			return nil
		}
		return fmt.Errorf("prompttrace: read %s: %w", path, err)
	}
}

func decodeRecord(line []byte) (Record, error) {
	var decoded jsonRecord
	if err := json.Unmarshal(line, &decoded); err != nil {
		return Record{}, err
	}
	ts, err := time.Parse(time.RFC3339Nano, decoded.TS)
	if err != nil {
		return Record{}, fmt.Errorf("timestamp: %w", err)
	}
	return Record{
		TS:                 ts.UTC(),
		Repo:               decoded.Repo,
		Stage:              decoded.Stage,
		Strategy:           decoded.Strategy,
		Provider:           decoded.Provider,
		Model:              decoded.Model,
		Seq:                decoded.Seq,
		OfferedSeqs:        append([]int64(nil), decoded.OfferedSeqs...),
		BranchRef:          decoded.BranchRef,
		Generation:         decoded.Generation,
		DiffIncluded:       decoded.DiffIncluded,
		DiffCap:            decoded.DiffCap,
		Transform:          decoded.Transform,
		SystemMessage:      decoded.SystemMessage,
		UserMessage:        decoded.UserMessage,
		ToolSchema:         decoded.ToolSchema,
		Request:            append(json.RawMessage(nil), decoded.Request...),
		SubprocessEnvelope: append(json.RawMessage(nil), decoded.SubprocessEnvelope...),
		Response:           decoded.Response,
		Error:              decoded.Error,
	}, nil
}
