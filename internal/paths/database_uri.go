package paths

import (
	"net/url"
	"path/filepath"
)

// DatabaseURI keeps filesystem path bytes separate from SQLite URI options.
func DatabaseURI(dbPath string, options url.Values) string {
	path := (&url.URL{Path: filepath.ToSlash(dbPath)}).EscapedPath()
	return "file:" + path + "?" + options.Encode()
}
