package prompttrace

import (
	"bufio"
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

// Read reads prompt trace JSONL records from disk without creating files or
// mutating repo state. Missing trace directories are treated as empty traces.
func Read(ctx context.Context, opts ReadOptions) ([]Record, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	dir := opts.Dir
	if dir == "" {
		if opts.GitDir == "" {
			return nil, fmt.Errorf("prompttrace: Dir or GitDir required")
		}
		dir = Dir(opts.GitDir)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, fmt.Errorf("prompttrace: read dir: %w", err)
	}
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].Name() < entries[j].Name()
	})

	var records []Record
	for _, entry := range entries {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".jsonl") {
			continue
		}
		path := filepath.Join(dir, entry.Name())
		fileRecords, err := readFile(ctx, path)
		if err != nil {
			return nil, err
		}
		records = append(records, fileRecords...)
	}
	return records, nil
}

func readFile(ctx context.Context, path string) ([]Record, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("prompttrace: open %s: %w", path, err)
	}
	defer f.Close()

	var records []Record
	reader := bufio.NewReader(f)
	lineNo := 0
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		line, err := reader.ReadBytes('\n')
		if len(line) > 0 {
			lineNo++
			line = []byte(strings.TrimSpace(string(line)))
			if len(line) > 0 {
				rec, derr := decodeRecord(line)
				if derr != nil {
					return nil, fmt.Errorf("prompttrace: decode %s:%d: %w", path, lineNo, derr)
				}
				records = append(records, rec)
			}
		}
		if err == nil {
			continue
		}
		if errors.Is(err, io.EOF) {
			return records, nil
		}
		return nil, fmt.Errorf("prompttrace: read %s: %w", path, err)
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
