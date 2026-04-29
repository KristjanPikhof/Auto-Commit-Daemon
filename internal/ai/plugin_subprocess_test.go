// plugin_subprocess_test.go — exercises the subprocess plugin runner end
// to end. Each test installs a bash script as `acd-provider-test` in a
// temp directory and points the provider at it via a custom LookPath
// hook, so the harness never depends on the developer's $PATH.
//
// Skipped on Windows (the project does not support Windows in v1, see
// project plan §11.3) because the mock plugin is a bash script.
package ai

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// writePluginScript drops a bash script into dir under the canonical
// `acd-provider-<name>` filename and marks it executable. Returns the
// resolved absolute path so tests can plumb it through LookPath.
func writePluginScript(t *testing.T, dir, name, body string) string {
	t.Helper()
	bin := filepath.Join(dir, "acd-provider-"+name)
	header := "#!/usr/bin/env bash\nset -u\n"
	if err := os.WriteFile(bin, []byte(header+body), 0o755); err != nil {
		t.Fatalf("write plugin script: %v", err)
	}
	return bin
}

// fixedLookPath returns a LookPathFunc that resolves only the supplied
// binary; everything else returns ErrNotFound. Keeps tests insulated from
// the real $PATH.
func fixedLookPath(name, path string) LookPathFunc {
	return func(want string) (string, error) {
		if want == name {
			return path, nil
		}
		return "", fmt.Errorf("not found: %s", want)
	}
}

func skipIfWindows(t *testing.T) {
	t.Helper()
	if runtime.GOOS == "windows" {
		t.Skip("subprocess plugin tests require a POSIX shell")
	}
}

// TestSubprocess_HappyPath exercises the canonical request/response path.
// The mock plugin reads each JSONL line from stdin, extracts the path
// field with a tiny shell expression, and emits a Result whose subject
// embeds that path so we can verify the correlation.
func TestSubprocess_HappyPath(t *testing.T) {
	skipIfWindows(t)
	dir := t.TempDir()
	bin := writePluginScript(t, dir, "test", `
while IFS= read -r line; do
  # crude path extractor: pull "path":"VAL" from the JSONL line.
  path=$(printf '%s' "$line" | sed -E 's/.*"path":"([^"]*)".*/\1/')
  printf '{"version":1,"subject":"Touched %s","body":"- modify %s","error":""}\n' "$path" "$path"
done
`)
	p := NewSubprocessProvider("test", SubprocessOptions{
		LookPath: fixedLookPath("acd-provider-test", bin),
		Timeout:  5 * time.Second,
		Stderr:   io.Discard,
	})
	t.Cleanup(func() { _ = p.Close() })

	r, err := p.Generate(context.Background(), CommitContext{Path: "src/main.go", Op: "modify"})
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	if r.Subject != "Touched src/main.go" {
		t.Errorf("subject: got %q want %q", r.Subject, "Touched src/main.go")
	}
	if r.Source != "subprocess:test" {
		t.Errorf("source: got %q want %q", r.Source, "subprocess:test")
	}
	if !strings.Contains(r.Body, "modify src/main.go") {
		t.Errorf("body: got %q", r.Body)
	}
}

func TestSubprocess_RedactsDiffBeforeSend(t *testing.T) {
	skipIfWindows(t)
	dir := t.TempDir()
	bin := writePluginScript(t, dir, "test", `
while IFS= read -r line; do
  case "$line" in
    *AKIAIOSFODNN7EXAMPLE*)
      printf '{"version":1,"subject":"","body":"","error":"secret leaked"}\n'
      ;;
    *REDACTED_SECRET*)
      printf '{"version":1,"subject":"Redacted diff","body":"","error":""}\n'
      ;;
    *)
      printf '{"version":1,"subject":"","body":"","error":"missing redaction"}\n'
      ;;
  esac
done
`)
	p := NewSubprocessProvider("test", SubprocessOptions{
		LookPath: fixedLookPath("acd-provider-test", bin),
		Timeout:  5 * time.Second,
		Stderr:   io.Discard,
	})
	t.Cleanup(func() { _ = p.Close() })

	r, err := p.Generate(context.Background(), CommitContext{
		Path:     "config/prod.yaml",
		Op:       "modify",
		DiffText: "+aws_access_key_id: AKIAIOSFODNN7EXAMPLE\n",
	})
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	if r.Subject != "Redacted diff" {
		t.Fatalf("subject=%q", r.Subject)
	}
}

// TestSubprocess_Concurrency hammers Generate from many goroutines. The
// owner-goroutine serialisation contract guarantees no garbled stdout
// reads; this test would deadlock or panic under -race if the runner ever
// let two requests share the pipe.
func TestSubprocess_Concurrency(t *testing.T) {
	skipIfWindows(t)
	dir := t.TempDir()
	bin := writePluginScript(t, dir, "test", `
while IFS= read -r line; do
  path=$(printf '%s' "$line" | sed -E 's/.*"path":"([^"]*)".*/\1/')
  printf '{"version":1,"subject":"S %s","body":"","error":""}\n' "$path"
done
`)
	p := NewSubprocessProvider("test", SubprocessOptions{
		LookPath: fixedLookPath("acd-provider-test", bin),
		Timeout:  5 * time.Second,
		Stderr:   io.Discard,
	})
	t.Cleanup(func() { _ = p.Close() })

	const goroutines = 10
	const perGoroutine = 5
	var wg sync.WaitGroup
	var failures atomic.Int64
	for g := 0; g < goroutines; g++ {
		g := g
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < perGoroutine; i++ {
				path := fmt.Sprintf("g%d/i%d.go", g, i)
				r, err := p.Generate(context.Background(), CommitContext{Path: path, Op: "modify"})
				if err != nil {
					failures.Add(1)
					t.Errorf("Generate(%s): %v", path, err)
					return
				}
				want := "S " + path
				if r.Subject != want {
					failures.Add(1)
					t.Errorf("subject mismatch: got %q want %q", r.Subject, want)
					return
				}
			}
		}()
	}
	wg.Wait()
	if failures.Load() > 0 {
		t.Fatalf("%d failures", failures.Load())
	}
}

// TestSubprocess_SoftError verifies a non-empty error field surfaces as a
// Generate error while leaving the plugin process running. A second call
// against the same provider succeeds, demonstrating the soft-fail path
// does not trip the crash/respawn machinery.
func TestSubprocess_SoftError(t *testing.T) {
	skipIfWindows(t)
	dir := t.TempDir()
	bin := writePluginScript(t, dir, "test", `
i=0
while IFS= read -r line; do
  i=$((i+1))
  if [ "$i" = "1" ]; then
    printf '{"version":1,"subject":"","body":"","error":"boom"}\n'
  else
    printf '{"version":1,"subject":"OK","body":"","error":""}\n'
  fi
done
`)
	p := NewSubprocessProvider("test", SubprocessOptions{
		LookPath: fixedLookPath("acd-provider-test", bin),
		Timeout:  5 * time.Second,
		Stderr:   io.Discard,
	})
	t.Cleanup(func() { _ = p.Close() })

	if _, err := p.Generate(context.Background(), CommitContext{Path: "x", Op: "modify"}); err == nil {
		t.Fatal("expected soft error on first call, got nil")
	} else if !strings.Contains(err.Error(), "boom") {
		t.Errorf("error did not surface plugin message: %v", err)
	}

	r, err := p.Generate(context.Background(), CommitContext{Path: "y", Op: "modify"})
	if err != nil {
		t.Fatalf("second Generate: %v", err)
	}
	if r.Subject != "OK" {
		t.Errorf("subject: got %q want OK", r.Subject)
	}

	// Compose-with-deterministic should fall back cleanly on the soft
	// error rather than surfacing it.
	det := DeterministicProvider{}
	composed := Compose(NewSubprocessProvider("test", SubprocessOptions{
		LookPath: fixedLookPath("acd-provider-test", bin),
		Timeout:  5 * time.Second,
		Stderr:   io.Discard,
	}), det)
	r, err = composed.Generate(context.Background(), CommitContext{Path: "z.go", Op: "modify"})
	if err != nil {
		t.Fatalf("composed Generate: %v", err)
	}
	// First call to the new provider hits the soft error; Compose then
	// calls deterministic which yields "Update z.go".
	if r.Subject != "Update z.go" {
		t.Errorf("composed fallback subject: got %q want %q", r.Subject, "Update z.go")
	}
	if r.Source != "deterministic" {
		t.Errorf("composed fallback source: got %q want deterministic", r.Source)
	}
}

// TestSubprocess_Timeout makes the plugin sleep longer than the timeout.
// The runner must kill the plugin on timeout and respawn on the next
// Generate. We point the second invocation at a *different* script (via
// a fresh provider sharing the same temp dir prefix) to keep the test
// straightforward.
func TestSubprocess_Timeout(t *testing.T) {
	skipIfWindows(t)
	dir := t.TempDir()
	slowBin := writePluginScript(t, dir, "slow", `
while IFS= read -r line; do
  sleep 60
  printf '{"version":1,"subject":"never","body":"","error":""}\n'
done
`)
	p := NewSubprocessProvider("slow", SubprocessOptions{
		LookPath: fixedLookPath("acd-provider-slow", slowBin),
		Timeout:  200 * time.Millisecond,
		Stderr:   io.Discard,
	})
	t.Cleanup(func() { _ = p.Close() })

	start := time.Now()
	_, err := p.Generate(context.Background(), CommitContext{Path: "a", Op: "modify"})
	elapsed := time.Since(start)
	if err == nil {
		t.Fatal("expected timeout error")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("expected DeadlineExceeded, got %v", err)
	}
	if elapsed > 5*time.Second {
		t.Errorf("timeout fired far too late: %v", elapsed)
	}

	// After timeout the provider should be ready to respawn. Swap the
	// plugin to a fast one and retry — the new process is a fresh pid.
	fastBin := writePluginScript(t, dir, "slow", `
while IFS= read -r line; do
  printf '{"version":1,"subject":"fast","body":"","error":""}\n'
done
`)
	_ = fastBin // same path as slowBin, overwritten in place
	r, err := p.Generate(context.Background(), CommitContext{Path: "a", Op: "modify"})
	if err != nil {
		t.Fatalf("respawn Generate: %v", err)
	}
	if r.Subject != "fast" {
		t.Errorf("respawn subject: got %q want fast", r.Subject)
	}
}

// TestSubprocess_CrashRespawn forces the plugin to exit after one
// response. The next Generate must respawn the binary cleanly and
// continue to function.
func TestSubprocess_CrashRespawn(t *testing.T) {
	skipIfWindows(t)
	dir := t.TempDir()
	// State file persists across both spawns. Iteration 1 exits after
	// the first response; iteration 2 stays alive and responds normally.
	stateFile := filepath.Join(dir, "iter")
	bin := writePluginScript(t, dir, "test", fmt.Sprintf(`
state=%q
n=$(cat "$state" 2>/dev/null || echo 0)
n=$((n+1))
echo $n > "$state"
if [ "$n" = "1" ]; then
  IFS= read -r line
  printf '{"version":1,"subject":"first","body":"","error":""}\n'
  exit 0
else
  while IFS= read -r line; do
    printf '{"version":1,"subject":"second","body":"","error":""}\n'
  done
fi
`, stateFile))
	p := NewSubprocessProvider("test", SubprocessOptions{
		LookPath: fixedLookPath("acd-provider-test", bin),
		Timeout:  5 * time.Second,
		Stderr:   io.Discard,
	})
	t.Cleanup(func() { _ = p.Close() })

	r, err := p.Generate(context.Background(), CommitContext{Path: "a", Op: "modify"})
	if err != nil {
		t.Fatalf("first Generate: %v", err)
	}
	if r.Subject != "first" {
		t.Errorf("first subject: got %q", r.Subject)
	}

	// Plugin has exited. Next Generate respawns; subsequent requests
	// run against the long-lived second process.
	r, err = p.Generate(context.Background(), CommitContext{Path: "b", Op: "modify"})
	if err != nil {
		t.Fatalf("second Generate (respawn): %v", err)
	}
	if r.Subject != "second" {
		t.Errorf("second subject: got %q want second", r.Subject)
	}
	r, err = p.Generate(context.Background(), CommitContext{Path: "c", Op: "modify"})
	if err != nil {
		t.Fatalf("third Generate: %v", err)
	}
	if r.Subject != "second" {
		t.Errorf("third subject: got %q want second", r.Subject)
	}
}

// TestSubprocess_MissingBinary checks the constructor records the lookup
// error rather than panicking, and Generate surfaces it on the first call.
// Compose() with deterministic falls back cleanly so the daemon keeps
// running with no plugin present.
func TestSubprocess_MissingBinary(t *testing.T) {
	skipIfWindows(t)
	missing := func(string) (string, error) {
		return "", fmt.Errorf("not found")
	}
	p := NewSubprocessProvider("test", SubprocessOptions{
		LookPath: missing,
	})
	if p.resolveErr == nil {
		t.Fatal("expected resolveErr when binary missing")
	}
	if _, err := p.Generate(context.Background(), CommitContext{Path: "x", Op: "modify"}); err == nil {
		t.Fatal("Generate must surface lookup error")
	}

	composed := Compose(p, DeterministicProvider{})
	r, err := composed.Generate(context.Background(), CommitContext{Path: "x.go", Op: "modify"})
	if err != nil {
		t.Fatalf("composed Generate: %v", err)
	}
	if r.Subject != "Update x.go" {
		t.Errorf("fallback subject: got %q", r.Subject)
	}
	if r.Source != "deterministic" {
		t.Errorf("fallback source: got %q", r.Source)
	}
}

// TestSubprocess_CloseCleanup verifies Close terminates the plugin
// promptly and subsequent Generate calls error.
func TestSubprocess_CloseCleanup(t *testing.T) {
	skipIfWindows(t)
	dir := t.TempDir()
	bin := writePluginScript(t, dir, "test", `
while IFS= read -r line; do
  printf '{"version":1,"subject":"OK","body":"","error":""}\n'
done
`)
	p := NewSubprocessProvider("test", SubprocessOptions{
		LookPath: fixedLookPath("acd-provider-test", bin),
		Timeout:  5 * time.Second,
		Stderr:   io.Discard,
	})

	// Spawn the plugin by issuing one request.
	if _, err := p.Generate(context.Background(), CommitContext{Path: "a", Op: "modify"}); err != nil {
		t.Fatalf("priming Generate: %v", err)
	}

	closeStart := time.Now()
	if err := p.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if elapsed := time.Since(closeStart); elapsed > subprocessShutdownGrace+time.Second {
		t.Errorf("Close took too long: %v", elapsed)
	}

	if _, err := p.Generate(context.Background(), CommitContext{Path: "b", Op: "modify"}); err == nil {
		t.Fatal("Generate after Close should error")
	}

	// Idempotent.
	if err := p.Close(); err != nil {
		t.Errorf("second Close: %v", err)
	}
}
