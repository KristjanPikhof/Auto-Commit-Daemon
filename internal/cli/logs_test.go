package cli

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/KristjanPikhof/Auto-Commit-Daemon/internal/paths"
)

func TestLogsTailPrintsLastLinesRaw(t *testing.T) {
	roots := withIsolatedHome(t)
	repo, stateDB, d := makeRepoStateDB(t)
	registerRepo(t, roots, repo, stateDB, "codex")
	if err := d.Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}
	logPath := writeRepoLog(t, roots, repo, strings.Join([]string{
		`{"level":"info","msg":"one"}`,
		`{"level":"info","msg":"two"}`,
		`{"level":"warn","msg":"three"}`,
	}, "\n")+"\n")

	var out bytes.Buffer
	if err := runLogs(context.Background(), &out, repo, 2, false); err != nil {
		t.Fatalf("runLogs: %v", err)
	}
	want := `{"level":"info","msg":"two"}` + "\n" +
		`{"level":"warn","msg":"three"}` + "\n"
	if out.String() != want {
		t.Fatalf("tail output mismatch for %s:\ngot:\n%s\nwant:\n%s", logPath, out.String(), want)
	}
}

func TestReadLastLogLinesReturnsEOFOffset(t *testing.T) {
	path := filepath.Join(t.TempDir(), "daemon.log")
	body := strings.Join([]string{
		`{"msg":"one"}`,
		`{"msg":"two"}`,
		`{"msg":"three"}`,
	}, "\n") + "\n"
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatalf("write log: %v", err)
	}

	lines, offset, err := readLastLogLines(path, 2)
	if err != nil {
		t.Fatalf("readLastLogLines: %v", err)
	}
	if offset != int64(len(body)) {
		t.Fatalf("offset=%d want EOF %d", offset, len(body))
	}
	if got, want := strings.Join(lines, "\n"), `{"msg":"two"}`+"\n"+`{"msg":"three"}`; got != want {
		t.Fatalf("lines=%q want %q", got, want)
	}
}

func TestLogsMissingLogReturnsActionableError(t *testing.T) {
	roots := withIsolatedHome(t)
	repo, stateDB, d := makeRepoStateDB(t)
	registerRepo(t, roots, repo, stateDB, "codex")
	if err := d.Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}

	var out bytes.Buffer
	err := runLogs(context.Background(), &out, repo, defaultLogLines, false)
	if err == nil {
		t.Fatal("runLogs returned nil error for missing log")
	}
	msg := err.Error()
	if !strings.Contains(msg, "daemon log missing") || !strings.Contains(msg, "acd start --repo") || !strings.Contains(msg, "acd doctor") {
		t.Fatalf("missing log error is not actionable: %v", err)
	}
}

func TestLogsUnregisteredRepoReturnsActionableError(t *testing.T) {
	withIsolatedHome(t)
	repo := t.TempDir()

	var out bytes.Buffer
	err := runLogs(context.Background(), &out, repo, defaultLogLines, false)
	if err == nil {
		t.Fatal("runLogs returned nil error for unregistered repo")
	}
	if !strings.Contains(err.Error(), "is not registered") || !strings.Contains(err.Error(), "acd start --repo") {
		t.Fatalf("unregistered repo error is not actionable: %v", err)
	}
}

func TestLogsFollowStreamsAppendedLines(t *testing.T) {
	roots := withIsolatedHome(t)
	repo, stateDB, d := makeRepoStateDB(t)
	registerRepo(t, roots, repo, stateDB, "codex")
	if err := d.Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}
	logPath := writeRepoLog(t, roots, repo, `{"msg":"initial"}`+"\n")

	oldInterval := logFollowPollInterval
	logFollowPollInterval = 10 * time.Millisecond
	t.Cleanup(func() { logFollowPollInterval = oldInterval })

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	writer := newSignalWriter(`{"msg":"appended"}`)
	errCh := make(chan error, 1)
	go func() {
		errCh <- runLogs(ctx, writer, repo, 0, true)
	}()

	appendDone := make(chan struct{})
	go func() {
		defer close(appendDone)
		ticker := time.NewTicker(20 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				f, err := os.OpenFile(logPath, os.O_APPEND|os.O_WRONLY, 0)
				if err != nil {
					return
				}
				_, _ = f.WriteString(`{"msg":"appended"}` + "\n")
				_ = f.Close()
			}
		}
	}()

	select {
	case <-writer.seen:
	case <-time.After(2 * time.Second):
		cancel()
		t.Fatalf("timed out waiting for followed line; output:\n%s", writer.String())
	}
	cancel()
	<-appendDone
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("runLogs follow returned error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("runLogs follow did not exit after context cancellation")
	}
}

func TestLogsCommandRegisteredWithHelp(t *testing.T) {
	root := newRootCmd()
	var out, errOut bytes.Buffer
	root.SetOut(&out)
	root.SetErr(&errOut)
	root.SetArgs([]string{"logs", "--help"})
	if err := root.ExecuteContext(context.Background()); err != nil {
		t.Fatalf("execute help: %v\nstderr:\n%s", err, errOut.String())
	}
	help := out.String()
	for _, want := range []string{"Print the current repo daemon log tail", "--lines", "--follow"} {
		if !strings.Contains(help, want) {
			t.Fatalf("logs help missing %q:\n%s", want, help)
		}
	}
}

func writeRepoLog(t *testing.T, roots paths.Roots, repo, body string) string {
	t.Helper()
	hash, err := paths.RepoHash(repo)
	if err != nil {
		t.Fatalf("repo hash: %v", err)
	}
	logPath := roots.RepoLogPath(hash)
	if err := os.MkdirAll(filepath.Dir(logPath), 0o700); err != nil {
		t.Fatalf("mkdir log dir: %v", err)
	}
	if err := os.WriteFile(logPath, []byte(body), 0o600); err != nil {
		t.Fatalf("write log: %v", err)
	}
	return logPath
}

type signalWriter struct {
	mu     sync.Mutex
	buf    bytes.Buffer
	needle string
	seen   chan struct{}
	once   sync.Once
}

func newSignalWriter(needle string) *signalWriter {
	return &signalWriter{
		needle: needle,
		seen:   make(chan struct{}),
	}
}

func (w *signalWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	_, _ = w.buf.Write(p)
	seen := strings.Contains(w.buf.String(), w.needle)
	w.mu.Unlock()
	if seen {
		w.once.Do(func() { close(w.seen) })
	}
	return len(p), nil
}

func (w *signalWriter) String() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.buf.String()
}
