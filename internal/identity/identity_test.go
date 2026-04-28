package identity

import (
	"os"
	"os/exec"
	"runtime"
	"strings"
	"testing"
)

func TestAlive_SelfPid(t *testing.T) {
	t.Parallel()
	if !Alive(os.Getpid()) {
		t.Fatalf("Alive(self) = false; want true")
	}
}

func TestAlive_ZeroAndNegative(t *testing.T) {
	t.Parallel()
	if Alive(0) {
		t.Errorf("Alive(0) = true; want false (signals process group)")
	}
	if Alive(-1) {
		t.Errorf("Alive(-1) = true; want false")
	}
}

func TestAlive_DeadPid(t *testing.T) {
	t.Parallel()
	// Spawn `true`, wait for it to exit, then assert Alive returns false.
	// We use os/exec rather than os.StartProcess so Wait() handles the
	// reaping for us — without it the pid would still be a defunct entry
	// in the process table on Linux.
	cmd := exec.Command(trueBinary())
	if err := cmd.Start(); err != nil {
		t.Fatalf("start short-lived child: %v", err)
	}
	pid := cmd.Process.Pid
	if err := cmd.Wait(); err != nil {
		t.Fatalf("wait child: %v", err)
	}
	if Alive(pid) {
		t.Fatalf("Alive(%d) = true after exit+wait; want false", pid)
	}
}

func TestHashArgv_Deterministic(t *testing.T) {
	t.Parallel()
	a := HashArgv([]string{"acd", "daemon", "run"})
	b := HashArgv([]string{"acd", "daemon", "run"})
	if a != b {
		t.Fatalf("HashArgv non-deterministic: %s vs %s", a, b)
	}
	if len(a) != 64 { // sha256 hex
		t.Fatalf("HashArgv returned %d chars; want 64", len(a))
	}
}

func TestHashArgv_NULSeparation(t *testing.T) {
	t.Parallel()
	// The NUL separator must distinguish ["ab","c"] from ["a","bc"]; if
	// we joined with the empty string both would hash identically.
	if HashArgv([]string{"ab", "c"}) == HashArgv([]string{"a", "bc"}) {
		t.Fatalf("HashArgv collides on no-separator boundary")
	}
}

func TestHashArgv_OrderSensitive(t *testing.T) {
	t.Parallel()
	if HashArgv([]string{"a", "b"}) == HashArgv([]string{"b", "a"}) {
		t.Fatalf("HashArgv must be order-sensitive")
	}
}

func TestMatch_EmptyNeverMatches(t *testing.T) {
	t.Parallel()
	empty := Fingerprint{}
	full := Fingerprint{StartTime: "Mon Apr 28 14:22:13 2026", ArgvHash: "abc"}
	if Match(empty, empty) {
		t.Errorf("empty/empty matched; expected refusal")
	}
	if Match(empty, full) || Match(full, empty) {
		t.Errorf("empty/full matched; expected refusal")
	}
}

func TestMatch_DifferentArgvDiffers(t *testing.T) {
	t.Parallel()
	a := Fingerprint{StartTime: "Mon Apr 28 14:22:13 2026", ArgvHash: HashArgv([]string{"acd", "v1"})}
	b := Fingerprint{StartTime: "Mon Apr 28 14:22:13 2026", ArgvHash: HashArgv([]string{"acd", "v2"})}
	if Match(a, b) {
		t.Fatalf("different argv hashes matched")
	}
}

func TestMatch_DifferentStartTimeDiffers(t *testing.T) {
	t.Parallel()
	hash := HashArgv([]string{"acd"})
	a := Fingerprint{StartTime: "Mon Apr 28 14:22:13 2026", ArgvHash: hash}
	b := Fingerprint{StartTime: "Mon Apr 28 14:22:14 2026", ArgvHash: hash}
	if Match(a, b) {
		t.Fatalf("different start times matched")
	}
}

func TestMatch_IdenticalMatches(t *testing.T) {
	t.Parallel()
	fp := Fingerprint{StartTime: "Mon Apr 28 14:22:13 2026", ArgvHash: HashArgv([]string{"acd"})}
	if !Match(fp, fp) {
		t.Fatalf("identical fingerprints did not match")
	}
}

func TestCapture_Self(t *testing.T) {
	if runtime.GOOS != "darwin" && runtime.GOOS != "linux" {
		t.Skipf("ps fingerprint only validated on darwin/linux; running on %s", runtime.GOOS)
	}
	fp, err := Capture(os.Getpid())
	if err != nil {
		t.Fatalf("Capture(self): %v", err)
	}
	if fp.StartTime == "" || fp.ArgvHash == "" {
		t.Fatalf("Capture(self) returned partial fingerprint: %+v", fp)
	}
	// Year sanity-check: lstart includes a 4-digit year. We don't pin
	// the format more tightly because the test fixture is the live
	// system clock.
	if !strings.ContainsAny(fp.StartTime, "0123456789") {
		t.Errorf("StartTime missing digits: %q", fp.StartTime)
	}
}

func TestCapture_DeadPidErrors(t *testing.T) {
	if runtime.GOOS != "darwin" && runtime.GOOS != "linux" {
		t.Skip("ps fingerprint only validated on darwin/linux")
	}
	cmd := exec.Command(trueBinary())
	if err := cmd.Start(); err != nil {
		t.Fatalf("start child: %v", err)
	}
	pid := cmd.Process.Pid
	_ = cmd.Wait()
	if _, err := Capture(pid); err == nil {
		t.Fatalf("Capture(dead pid) returned nil error")
	}
}

func TestCapture_RejectsBadPid(t *testing.T) {
	t.Parallel()
	if _, err := Capture(0); err == nil {
		t.Errorf("Capture(0) returned nil error")
	}
	if _, err := Capture(-1); err == nil {
		t.Errorf("Capture(-1) returned nil error")
	}
}

func TestCaptureSelf_UsesInProcessArgv(t *testing.T) {
	if runtime.GOOS != "darwin" && runtime.GOOS != "linux" {
		t.Skip("ps fingerprint only validated on darwin/linux")
	}
	fp, err := CaptureSelf()
	if err != nil {
		t.Fatalf("CaptureSelf: %v", err)
	}
	if fp.ArgvHash != HashArgv(os.Args) {
		t.Fatalf("CaptureSelf argv hash %s; want %s (HashArgv(os.Args))", fp.ArgvHash, HashArgv(os.Args))
	}
}

// trueBinary returns the path to a no-op executable. We prefer /bin/true
// over /usr/bin/true for darwin compatibility but fall back if missing.
func trueBinary() string {
	for _, p := range []string{"/bin/true", "/usr/bin/true"} {
		if _, err := os.Stat(p); err == nil {
			return p
		}
	}
	return "/bin/true"
}
