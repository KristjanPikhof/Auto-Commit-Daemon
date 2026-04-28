// Package paths resolves XDG base directories for acd state, share, config,
// and per-repo log roots. See §13.1, §13.2.
//
// Resolution rules (per the XDG base-directory spec):
//
//	XDG_STATE_HOME  → $HOME/.local/state   (per-repo state, log dir)
//	XDG_DATA_HOME   → $HOME/.local/share   (central registry, stats.db)
//	XDG_CONFIG_HOME → $HOME/.config        (operator config)
//
// All `acd ...` subdirectories are appended afterward. Per the spec, a
// non-empty XDG_* override is honored only when it is an absolute path;
// any other value (including a relative path) is rejected by the spec
// and the implementation falls back to the default.
package paths

import (
	"errors"
	"os"
	"path/filepath"
)

// appName is the per-tool subdirectory used under each XDG root.
const appName = "acd"

// Roots bundles the three XDG roots after acd's subdirectory has been
// appended. State, share, and config are each guaranteed absolute.
type Roots struct {
	// State is the per-tool state root. Each repo gets a subdir keyed by
	// its short hash beneath this. JSONL log files live alongside.
	State string
	// Share is the per-tool central directory. registry.json + stats.db
	// live here.
	Share string
	// Config is the per-tool config directory. Reserved for operator
	// preferences (no v1 consumer yet).
	Config string
}

// Resolve returns the XDG roots for the current user, honoring the
// XDG_*_HOME environment variables when they're set to absolute paths.
//
// $HOME is required for the fallback path; if it's missing we return an
// error rather than silently writing to "/.local/...".
func Resolve() (Roots, error) {
	home, err := homeDir()
	if err != nil {
		return Roots{}, err
	}
	state := xdgRoot("XDG_STATE_HOME", filepath.Join(home, ".local", "state"))
	share := xdgRoot("XDG_DATA_HOME", filepath.Join(home, ".local", "share"))
	config := xdgRoot("XDG_CONFIG_HOME", filepath.Join(home, ".config"))
	return Roots{
		State:  filepath.Join(state, appName),
		Share:  filepath.Join(share, appName),
		Config: filepath.Join(config, appName),
	}, nil
}

// RepoStateDir returns the per-repo state directory under State, keyed by
// the repo's short hash. The directory is *not* created here; callers
// (logger, daemon) are responsible for `os.MkdirAll(..., 0o700)`.
func (r Roots) RepoStateDir(repoHash string) string {
	return filepath.Join(r.State, repoHash)
}

// RepoLogPath is the canonical daemon.log location for a repo.
func (r Roots) RepoLogPath(repoHash string) string {
	return filepath.Join(r.RepoStateDir(repoHash), "daemon.log")
}

// RegistryPath returns ~/.local/share/acd/registry.json.
func (r Roots) RegistryPath() string {
	return filepath.Join(r.Share, "registry.json")
}

// RegistryLockPath returns ~/.local/share/acd/registry.lock.
func (r Roots) RegistryLockPath() string {
	return filepath.Join(r.Share, "registry.lock")
}

// StatsDBPath returns ~/.local/share/acd/stats.db.
func (r Roots) StatsDBPath() string {
	return filepath.Join(r.Share, "stats.db")
}

// xdgRoot reads an XDG_*_HOME env var. If unset, empty, or not absolute,
// it falls back to the supplied default. Per the XDG spec, "If $XDG_...
// is either not set or empty, a default ... should be used. ... All paths
// set in these environment variables must be absolute."
func xdgRoot(envName, fallback string) string {
	if v := os.Getenv(envName); v != "" && filepath.IsAbs(v) {
		return v
	}
	return fallback
}

// homeDir returns $HOME, or os.UserHomeDir() as a fallback. We require
// the result to be absolute to avoid silent writes into the working
// directory if the env is busted.
func homeDir() (string, error) {
	if h := os.Getenv("HOME"); h != "" && filepath.IsAbs(h) {
		return h, nil
	}
	h, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}
	if !filepath.IsAbs(h) {
		return "", errors.New("paths: home directory is not absolute")
	}
	return h, nil
}
