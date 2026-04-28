// Package logger constructs the daemon's slog JSONL handler with size+age
// based file rotation. Per D19, all daemon output is structured JSON
// (one event per line) so operators can grep/jq it without bespoke
// parsers. Per §13.1/§13.2 the active file is rotated to gz once it
// exceeds the size threshold and old archives are pruned by age.
package logger

import (
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
)

// Options configures New().
type Options struct {
	// Path is the absolute path to the active daemon.log file. Required.
	// Parent directory is created (mode 0o700) if missing.
	Path string

	// Level is the minimum slog level to emit. Default: slog.LevelInfo.
	Level slog.Level

	// IncludeSource adds source-file/line annotations to every record.
	// Useful for development; default off because it bloats production
	// logs and leaks build paths.
	IncludeSource bool

	// MaxSizeBytes is the size threshold above which the active log is
	// rotated. Zero falls back to DefaultMaxSize (10 MiB per §13.2).
	MaxSizeBytes int64

	// MaxAge is the maximum age (in days) for retained gz archives.
	// Older archives are pruned at startup. Zero falls back to
	// DefaultMaxAgeDays (14 per §13.2).
	MaxAgeDays int

	// MaxBackups is the number of gz archives to retain. Zero falls back
	// to DefaultMaxBackups (5 per §13.2).
	MaxBackups int
}

// Defaults locked by §13.2.
const (
	DefaultMaxSize    int64 = 10 * 1024 * 1024
	DefaultMaxAgeDays       = 14
	DefaultMaxBackups       = 5
)

// New returns a JSONL slog.Logger that writes to opts.Path, rotating the
// file in-process on size/age limits. The returned io.Closer flushes and
// releases the underlying file handle; daemons should defer it before
// exit so the final lines survive a clean shutdown.
//
// New also performs the startup age sweep described by §13.2: any
// archive older than MaxAgeDays is unlinked before the first record is
// written.
func New(opts Options) (*slog.Logger, io.Closer, error) {
	if opts.Path == "" {
		return nil, nil, errors.New("logger: Options.Path is required")
	}
	if !filepath.IsAbs(opts.Path) {
		return nil, nil, errors.New("logger: Options.Path must be absolute")
	}
	if opts.MaxSizeBytes == 0 {
		opts.MaxSizeBytes = DefaultMaxSize
	}
	if opts.MaxAgeDays == 0 {
		opts.MaxAgeDays = DefaultMaxAgeDays
	}
	if opts.MaxBackups == 0 {
		opts.MaxBackups = DefaultMaxBackups
	}

	dir := filepath.Dir(opts.Path)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, nil, err
	}

	w, err := newRotatingWriter(opts.Path, opts.MaxSizeBytes, opts.MaxBackups, opts.MaxAgeDays)
	if err != nil {
		return nil, nil, err
	}

	hopts := &slog.HandlerOptions{
		Level:     opts.Level,
		AddSource: opts.IncludeSource,
	}
	logger := slog.New(slog.NewJSONHandler(w, hopts))
	return logger, w, nil
}
