package daemon

// LiveEntry is one path in the live worktree snapshot. Mirrors the legacy
// daemon's `live[rel] = {path, mode, oid}` shape.
type LiveEntry struct {
	Path string
	Mode string // git mode bits ("100644", "100755", "120000")
	OID  string // blob OID
}
