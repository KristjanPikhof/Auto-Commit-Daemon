//go:build darwin

package identity

// psBinary is the absolute path to `ps`. We hardcode it to defend against a
// $PATH-prepended shim that could forge fingerprints (start time + argv hash)
// for arbitrary pids — e.g. an attacker who can write into a writable
// directory earlier in PATH and convince the daemon to capture a fingerprint
// from there. macOS ships `ps` at /bin/ps as part of the base system; the
// path is stable across releases.
const psBinary = "/bin/ps"
