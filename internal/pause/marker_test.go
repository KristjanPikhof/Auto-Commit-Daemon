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
