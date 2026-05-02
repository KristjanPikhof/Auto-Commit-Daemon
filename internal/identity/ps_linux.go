//go:build linux

package identity

// psBinary is the absolute path to `ps`. We hardcode it to defend against a
// $PATH-prepended shim that could forge fingerprints. Linux distributions
// place ps at /usr/bin/ps via the procps-ng package; this is stable across
// Debian, Ubuntu, RHEL/CentOS, Fedora, Alpine, and Arch.
const psBinary = "/usr/bin/ps"
