package logger

import (
	"compress/gzip"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// gzipCloseWait bounds how long Close blocks waiting for in-flight
// background gzip goroutines to finish. The actual gzip work is bounded
// by file size and disk throughput; on a clean shutdown we want the
// archive emitted but we will not wedge the daemon if disk I/O is stuck.
const gzipCloseWait = 5 * time.Second

// gzipFileFn is a test seam: tests substitute a slow implementation to
// prove that rotateLocked does not block writers while gzip is in flight.
var gzipFileFn = gzipFile

// rotatingWriter is an io.Writer + io.Closer that owns a single active
// log file and rotates it in place when it exceeds maxSize. Rotation:
//
//  1. close active file
//  2. shift backups: .N.gz → .(N+1).gz, oldest beyond maxBackups dropped
//  3. gzip the just-closed active file to .1.gz
//  4. open a fresh active file
//
// All writes are serialized through a mutex, so concurrent goroutines
// never interleave bytes mid-record. A single Write call corresponds to
// one slog record (slog flushes the JSON line in one Write), so
// per-record atomicity is preserved across rotations.
//
// The age-based prune (drop archives older than maxAgeDays) runs once at
// construction; we don't re-run it on every Write because slog runs in
// hot paths and stat'ing every backup would be wasteful. Day-granularity
// pruning at boot is sufficient per §13.2.
type rotatingWriter struct {
	mu         sync.Mutex
	path       string
	file       *os.File
	size       int64
	maxSize    int64
	maxBackups int
	maxAgeDays int
	closed     bool
	// gzipWG tracks background gzip goroutines spawned by rotateLocked so
	// Close can wait briefly for them to finish; the bound is gzipCloseWait
	// so a wedged disk does not stall daemon shutdown.
	gzipWG sync.WaitGroup
}

// newRotatingWriter opens path in append mode, primes the in-memory size
// counter from the existing file (if any), and prunes age-expired
// backups.
func newRotatingWriter(path string, maxSize int64, maxBackups, maxAgeDays int) (*rotatingWriter, error) {
	w := &rotatingWriter{
		path:       path,
		maxSize:    maxSize,
		maxBackups: maxBackups,
		maxAgeDays: maxAgeDays,
	}
	if err := w.openFile(); err != nil {
		return nil, err
	}
	w.pruneByAge()
	return w, nil
}

// Write appends p to the active log, rotating beforehand if the file
// would cross maxSize. The two-step "rotate-if-needed, then write" order
// guarantees no record is split across files: even a single oversize
// record lands wholly in the *new* file.
func (w *rotatingWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return 0, os.ErrClosed
	}
	if w.size+int64(len(p)) > w.maxSize && w.size > 0 {
		if err := w.rotateLocked(); err != nil {
			return 0, err
		}
	}
	n, err := w.file.Write(p)
	w.size += int64(n)
	return n, err
}

// Close releases the active file. After Close the writer rejects further
// Writes with os.ErrClosed. Close briefly waits (bounded by
// gzipCloseWait) for any in-flight background gzip goroutines so the
// archive on disk reflects the final rotated content; if the wait
// expires (e.g. wedged disk I/O) Close proceeds anyway and the
// goroutines are leaked — they finish whenever the kernel unblocks
// them.
func (w *rotatingWriter) Close() error {
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		return nil
	}
	w.closed = true
	var fileErr error
	if w.file != nil {
		fileErr = w.file.Close()
		w.file = nil
	}
	w.mu.Unlock()

	// Wait for background gzip goroutines off-mutex so a writer that
	// races Close cannot deadlock behind us. Bound the wait so a wedged
	// disk does not stall daemon shutdown.
	done := make(chan struct{})
	go func() {
		w.gzipWG.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(gzipCloseWait):
		slog.Warn("logger: in-flight gzip did not finish; proceeding with close",
			"timeout", gzipCloseWait.String())
	}
	return fileErr
}

// openFile (re)opens the active log in append mode. Append mode means
// concurrent writers in the same process — and any out-of-process
// observers like tail(1) — see writes land at the current end.
func (w *rotatingWriter) openFile() error {
	f, err := os.OpenFile(w.path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	st, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return err
	}
	w.file = f
	w.size = st.Size()
	return nil
}

// rotateLocked performs the synchronous portion of rotation: close the
// active file, shift existing .N.gz backups outward, rename the just-
// closed active file to a unique temp name, and open a fresh active
// file. Gzipping the renamed temp file to .1.gz happens in a background
// goroutine so concurrent slog Writes never block on compression of
// multi-MB log files. Caller must hold w.mu.
//
// Concurrency model: when rotation N+1 starts while rotation N's gzip is
// still in flight, the per-rotation temp filename keeps the source file
// distinct, but both rotations target `.1.gz`. To avoid collision we
// serialize the gzip + shift step inside the goroutine via the
// gzipWG.Wait barrier at the head of rotateLocked. This means a *second*
// rotation does block writers while waiting for the *prior* gzip — but
// that is rare in practice (rotations only fire on size threshold) and
// strictly bounded by disk throughput, not by the active file's size.
func (w *rotatingWriter) rotateLocked() error {
	// Drain any prior in-flight gzip before we shift backups. Without
	// this, a back-to-back rotation could try to rename .1.gz → .2.gz
	// while the previous rotation's goroutine is still writing .1.gz.
	w.gzipWG.Wait()

	if err := w.file.Close(); err != nil {
		return err
	}
	w.file = nil

	// Shift existing .N.gz files outward. Walk highest → lowest so we
	// don't clobber rotated files mid-shift.
	if err := w.shiftBackups(); err != nil {
		return err
	}

	// Rename the just-closed active file to a unique temp name. Unique
	// per rotation so an in-flight gzip on a prior temp cannot collide.
	// A crash mid-gzip leaves the .rotating file readable rather than a
	// half-written .gz.
	tmp := fmt.Sprintf("%s.rotating-%d", w.path, time.Now().UnixNano())
	dst := w.path + ".1.gz"
	renamed := true
	if err := os.Rename(w.path, tmp); err != nil {
		if !os.IsNotExist(err) {
			return err
		}
		renamed = false
	}

	// Open the fresh active file BEFORE returning so the next Write can
	// proceed immediately.
	if err := w.openFile(); err != nil {
		// Best-effort cleanup of the orphan temp; the caller will see
		// the openFile error.
		if renamed {
			_ = os.Remove(tmp)
		}
		return err
	}

	if renamed {
		w.gzipWG.Add(1)
		go func(src, dst string) {
			defer w.gzipWG.Done()
			if err := gzipFileFn(src, dst); err != nil {
				slog.Warn("logger: gzip rotated log failed",
					"src", src, "dst", dst, "err", err.Error())
			}
			_ = os.Remove(src)
		}(tmp, dst)
	}

	w.pruneByAge()
	return nil
}

// shiftBackups renames foo.log.N.gz → foo.log.(N+1).gz, dropping anything
// past maxBackups so the on-disk fan-out is bounded.
func (w *rotatingWriter) shiftBackups() error {
	for i := w.maxBackups; i >= 1; i-- {
		src := fmt.Sprintf("%s.%d.gz", w.path, i)
		dst := fmt.Sprintf("%s.%d.gz", w.path, i+1)
		if _, err := os.Stat(src); err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return err
		}
		if i == w.maxBackups {
			// Past retention: drop instead of shifting.
			if err := os.Remove(src); err != nil && !os.IsNotExist(err) {
				return err
			}
			continue
		}
		if err := os.Rename(src, dst); err != nil {
			return err
		}
	}
	return nil
}

// gzipFile reads src in chunks and writes a gzipped copy to dst. The
// streaming approach keeps memory bounded for arbitrarily large rotations.
func gzipFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.OpenFile(dst, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	gz := gzip.NewWriter(out)
	if _, err := io.Copy(gz, in); err != nil {
		_ = gz.Close()
		_ = out.Close()
		return err
	}
	if err := gz.Close(); err != nil {
		_ = out.Close()
		return err
	}
	return out.Close()
}

// pruneByAge unlinks any rotated archive whose mtime is older than
// maxAgeDays. Errors are swallowed (best-effort cleanup): a stale file
// we can't delete should not crash the daemon.
func (w *rotatingWriter) pruneByAge() {
	if w.maxAgeDays <= 0 {
		return
	}
	cutoff := time.Now().Add(-time.Duration(w.maxAgeDays) * 24 * time.Hour)
	prefix := filepath.Base(w.path) + "."
	dir := filepath.Dir(w.path)
	entries, err := os.ReadDir(dir)
	if err != nil {
		return
	}
	for _, e := range entries {
		name := e.Name()
		if !strings.HasPrefix(name, prefix) || !strings.HasSuffix(name, ".gz") {
			continue
		}
		full := filepath.Join(dir, name)
		info, err := e.Info()
		if err != nil {
			continue
		}
		if info.ModTime().Before(cutoff) {
			_ = os.Remove(full)
		}
	}
}

// listBackups returns the rotated archive paths matching `<base>.N.gz`,
// sorted by N ascending. Used in tests + diagnostics.
func listBackups(activePath string) ([]string, error) {
	dir := filepath.Dir(activePath)
	prefix := filepath.Base(activePath) + "."
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	type indexed struct {
		idx  int
		name string
	}
	var found []indexed
	for _, e := range entries {
		name := e.Name()
		if !strings.HasPrefix(name, prefix) || !strings.HasSuffix(name, ".gz") {
			continue
		}
		mid := strings.TrimSuffix(strings.TrimPrefix(name, prefix), ".gz")
		idx, err := strconv.Atoi(mid)
		if err != nil {
			continue
		}
		found = append(found, indexed{idx, name})
	}
	sort.Slice(found, func(i, j int) bool { return found[i].idx < found[j].idx })
	out := make([]string, len(found))
	for i, f := range found {
		out[i] = filepath.Join(dir, f.name)
	}
	return out, nil
}
