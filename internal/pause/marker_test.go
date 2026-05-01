package pause

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"golang.org/x/sys/unix"
)

func sampleMarker() Marker {
	return Marker{
		Reason: "test",
		SetAt:  "2026-01-01T00:00:00Z",
		SetBy:  "host:user",
	}
}

func readBody(t *testing.T, path string) []byte {
	t.Helper()
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read marker: %v", err)
	}
	return body
}

func TestPauseWrite_FreshCreate_OverwroteFalse(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "paused")
	overwrote, err := Write(path, sampleMarker(), false)
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	if overwrote {
		t.Fatalf("overwrote=true on fresh create; want false")
	}
	var got Marker
	if err := json.Unmarshal(readBody(t, path), &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got.Reason != "test" {
		t.Fatalf("reason=%q want test", got.Reason)
	}
}

func TestPauseWrite_OverwriteRegular_OverwroteTrue(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "paused")
	if _, err := Write(path, sampleMarker(), false); err != nil {
		t.Fatalf("seed write: %v", err)
	}
	updated := sampleMarker()
	updated.Reason = "updated"
	overwrote, err := Write(path, updated, true)
	if err != nil {
		t.Fatalf("Write overwrite: %v", err)
	}
	if !overwrote {
		t.Fatalf("overwrote=false on existing regular marker; want true")
	}
	var got Marker
	if err := json.Unmarshal(readBody(t, path), &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got.Reason != "updated" {
		t.Fatalf("reason=%q want updated", got.Reason)
	}
}

func TestPauseWrite_RejectsExistingWithoutOverwrite(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "paused")
	if _, err := Write(path, sampleMarker(), false); err != nil {
		t.Fatalf("seed write: %v", err)
	}
	overwrote, err := Write(path, sampleMarker(), false)
	if err == nil {
		t.Fatalf("expected error when overwrite=false on existing marker")
	}
	if !errors.Is(err, os.ErrExist) {
		t.Fatalf("want os.ErrExist; got %v", err)
	}
	if overwrote {
		t.Fatalf("overwrote=true on rejected write; want false")
	}
}

func TestPauseWrite_RejectsSymlinkDestination(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("symlink semantics are not exercised on windows")
	}
	dir := t.TempDir()
	victim := filepath.Join(dir, "victim")
	if err := os.WriteFile(victim, []byte("DO NOT TOUCH"), 0o600); err != nil {
		t.Fatalf("seed victim: %v", err)
	}
	path := filepath.Join(dir, "paused")
	if err := os.Symlink(victim, path); err != nil {
		t.Fatalf("symlink: %v", err)
	}

	overwrote, err := Write(path, sampleMarker(), true)
	if err == nil {
		t.Fatalf("expected error when destination is symlink")
	}
	if !errors.Is(err, ErrNonRegularTarget) {
		t.Fatalf("want ErrNonRegularTarget; got %v", err)
	}
	if overwrote {
		t.Fatalf("overwrote=true on rejected symlink; want false")
	}
	// Symlink itself must remain untouched.
	info, err := os.Lstat(path)
	if err != nil {
		t.Fatalf("lstat after rejection: %v", err)
	}
	if info.Mode()&os.ModeSymlink == 0 {
		t.Fatalf("symlink no longer present after rejection")
	}
	// Victim must remain untouched.
	body, err := os.ReadFile(victim)
	if err != nil {
		t.Fatalf("read victim: %v", err)
	}
	if string(body) != "DO NOT TOUCH" {
		t.Fatalf("victim body=%q want unchanged", string(body))
	}
}

func TestPauseWrite_RejectsDirectoryDestination(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "paused")
	if err := os.Mkdir(path, 0o700); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	overwrote, err := Write(path, sampleMarker(), true)
	if err == nil {
		t.Fatalf("expected error when destination is directory")
	}
	if !errors.Is(err, ErrNonRegularTarget) {
		t.Fatalf("want ErrNonRegularTarget; got %v", err)
	}
	if overwrote {
		t.Fatalf("overwrote=true on rejected directory; want false")
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if !info.IsDir() {
		t.Fatalf("destination is no longer a directory after rejection")
	}
}

func TestPauseWrite_AtomicNoPartialBody(t *testing.T) {
	// We can not easily kill the process mid-write inside a unit test, but
	// we can prove the postcondition that drove the design: the destination
	// path always contains a fully-formed JSON document, never a truncated
	// prefix. This invariant follows from temp+rename: the live file is
	// either the previous version or the new version, never partial bytes.
	dir := t.TempDir()
	path := filepath.Join(dir, "paused")
	if _, err := Write(path, sampleMarker(), false); err != nil {
		t.Fatalf("first write: %v", err)
	}
	// Confirm only one file landed in the dir (no orphaned paused.tmp.*).
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("readdir: %v", err)
	}
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), "paused.tmp.") {
			t.Fatalf("orphaned temp file after success: %s", e.Name())
		}
	}
	updated := sampleMarker()
	updated.Reason = "second"
	if _, err := Write(path, updated, true); err != nil {
		t.Fatalf("second write: %v", err)
	}
	body := readBody(t, path)
	var got Marker
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatalf("unmarshal after overwrite: %v body=%q", err, string(body))
	}
	if got.Reason != "second" {
		t.Fatalf("reason=%q want second", got.Reason)
	}
}

// gitDirFor returns a temp dir that doubles as a "gitDir" for the pause
// marker layout: <gitDir>/acd/paused. The acd subdir is created on demand
// so individual tests can stage any state they need before calling Read.
func gitDirFor(t *testing.T) string {
	t.Helper()
	gitDir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(gitDir, "acd"), 0o700); err != nil {
		t.Fatalf("mkdir acd: %v", err)
	}
	return gitDir
}

func TestPauseRead_MissingFile(t *testing.T) {
	gitDir := gitDirFor(t)
	marker, ok, err := Read(gitDir)
	if err != nil {
		t.Fatalf("Read on missing file: %v", err)
	}
	if ok {
		t.Fatalf("ok=true on missing marker; want false")
	}
	if marker != (Marker{}) {
		t.Fatalf("marker=%+v on missing file; want zero value", marker)
	}
}

func TestPauseRead_MalformedJSON(t *testing.T) {
	gitDir := gitDirFor(t)
	if err := os.WriteFile(Path(gitDir), []byte("{this is not json"), 0o600); err != nil {
		t.Fatalf("seed malformed: %v", err)
	}
	_, _, err := Read(gitDir)
	if !errors.Is(err, ErrMalformed) {
		t.Fatalf("want ErrMalformed; got %v", err)
	}
}

func TestPauseRead_EmptyFile(t *testing.T) {
	gitDir := gitDirFor(t)
	if err := os.WriteFile(Path(gitDir), nil, 0o600); err != nil {
		t.Fatalf("seed empty: %v", err)
	}
	_, _, err := Read(gitDir)
	if !errors.Is(err, ErrMalformed) {
		t.Fatalf("empty file should be ErrMalformed; got %v", err)
	}
}

func TestPauseRead_Roundtrip(t *testing.T) {
	gitDir := gitDirFor(t)
	expires := "2026-12-31T23:59:59Z"
	original := Marker{
		Reason:    "operator paused",
		SetAt:     "2026-05-01T12:00:00Z",
		SetBy:     "host:user",
		ExpiresAt: &expires,
		Version:   0,
	}
	if _, err := Write(Path(gitDir), original, false); err != nil {
		t.Fatalf("Write: %v", err)
	}
	got, ok, err := Read(gitDir)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if !ok {
		t.Fatalf("ok=false after Write; want true")
	}
	if got.Reason != original.Reason || got.SetAt != original.SetAt || got.SetBy != original.SetBy {
		t.Fatalf("roundtrip mismatch: got=%+v want=%+v", got, original)
	}
	if got.ExpiresAt == nil || *got.ExpiresAt != expires {
		t.Fatalf("ExpiresAt mismatch: got=%v want=%s", got.ExpiresAt, expires)
	}
	if got.Version != 0 {
		t.Fatalf("Version=%d on default-write; want 0", got.Version)
	}
}

func TestPauseRead_NonRegularFile(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("FIFO not supported on windows")
	}
	gitDir := gitDirFor(t)
	path := Path(gitDir)
	if err := unix.Mkfifo(path, 0o600); err != nil {
		t.Skipf("mkfifo unsupported on this platform: %v", err)
	}
	_, _, err := Read(gitDir)
	if !errors.Is(err, ErrNonRegularSource) {
		t.Fatalf("want ErrNonRegularSource on FIFO; got %v", err)
	}
}

func TestPauseRead_RejectsSymlink(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("symlink semantics not exercised on windows")
	}
	gitDir := gitDirFor(t)
	victim := filepath.Join(gitDir, "victim")
	if err := os.WriteFile(victim, []byte(`{"reason":"attacker","set_at":"x","set_by":"y"}`), 0o600); err != nil {
		t.Fatalf("seed victim: %v", err)
	}
	if err := os.Symlink(victim, Path(gitDir)); err != nil {
		t.Fatalf("symlink: %v", err)
	}
	_, _, err := Read(gitDir)
	if err == nil {
		t.Fatalf("expected error reading through symlink; got nil")
	}
	// Either ErrNonRegularSource (if the symlink resolved through O_NOFOLLOW
	// produced ELOOP) or just ELOOP-wrapped is acceptable; we MUST not
	// silently follow the symlink and return the victim payload.
	if !errors.Is(err, ErrNonRegularSource) && !errors.Is(err, unix.ELOOP) {
		t.Fatalf("want ErrNonRegularSource or ELOOP; got %v", err)
	}
}

func TestPauseRead_RejectsOversize(t *testing.T) {
	gitDir := gitDirFor(t)
	// Build a syntactically-valid JSON object that exceeds maxMarkerSize so
	// we test the size cap, not the JSON parser.
	huge := make([]byte, maxMarkerSize+128)
	for i := range huge {
		huge[i] = 'x'
	}
	body := []byte(`{"reason":"`)
	body = append(body, huge...)
	body = append(body, []byte(`","set_at":"x","set_by":"y"}`)...)
	if err := os.WriteFile(Path(gitDir), body, 0o600); err != nil {
		t.Fatalf("seed huge: %v", err)
	}
	_, _, err := Read(gitDir)
	if !errors.Is(err, ErrMalformed) {
		t.Fatalf("want ErrMalformed on oversize; got %v", err)
	}
}

func TestPauseWrite_ConcurrentNoOverwrite_ExactlyOneWinner(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "paused")

	const writers = 16
	var (
		wg        sync.WaitGroup
		successes int64
		exists    int64
		other     int64
		otherErr  error
		otherMu   sync.Mutex
	)
	start := make(chan struct{})
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_, err := Write(path, sampleMarker(), false)
			switch {
			case err == nil:
				atomic.AddInt64(&successes, 1)
			case errors.Is(err, os.ErrExist):
				atomic.AddInt64(&exists, 1)
			default:
				atomic.AddInt64(&other, 1)
				otherMu.Lock()
				if otherErr == nil {
					otherErr = err
				}
				otherMu.Unlock()
			}
		}()
	}
	close(start)
	wg.Wait()

	if successes != 1 {
		t.Fatalf("successes=%d want 1 (race in overwrite=false path)", successes)
	}
	if exists != writers-1 {
		t.Fatalf("os.ErrExist count=%d want %d (other=%d, firstOtherErr=%v)", exists, writers-1, other, otherErr)
	}
	if other != 0 {
		t.Fatalf("unexpected non-ErrExist failures=%d firstErr=%v", other, otherErr)
	}

	// And the published marker must be a valid, fully-formed document.
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read final marker: %v", err)
	}
	var got Marker
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatalf("unmarshal final: %v body=%q", err, string(body))
	}
	if got.Reason != "test" {
		t.Fatalf("reason=%q want test", got.Reason)
	}
}

func TestPauseWrite_VersionFieldOmittedWhenZero(t *testing.T) {
	// The Version stub must not bleed schema noise into v0 markers. With
	// the `omitempty` tag, a default-constructed marker should produce
	// JSON that does not contain the "version" key.
	dir := t.TempDir()
	path := filepath.Join(dir, "paused")
	if _, err := Write(path, sampleMarker(), false); err != nil {
		t.Fatalf("Write: %v", err)
	}
	body := readBody(t, path)
	if strings.Contains(string(body), `"version"`) {
		t.Fatalf("default marker leaked version field: %s", body)
	}
}

func TestPauseWrite_TempFileCleanedUpOnLstatRejection(t *testing.T) {
	// When Lstat-before rejects (symlink/dir/non-regular), no temp file
	// should be created at all — the rejection happens before we touch the
	// filesystem with our own writes.
	dir := t.TempDir()
	path := filepath.Join(dir, "paused")
	if err := os.Mkdir(path, 0o700); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if _, err := Write(path, sampleMarker(), true); err == nil {
		t.Fatalf("expected error on directory destination")
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("readdir: %v", err)
	}
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), "paused.tmp.") {
			t.Fatalf("temp file leaked after Lstat rejection: %s", e.Name())
		}
	}
}
