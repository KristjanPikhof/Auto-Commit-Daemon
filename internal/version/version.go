package version

// These are populated by -ldflags at build time.
var (
	Version = "dev"
	GitSHA  = "unknown"
)

// String returns a single human-readable version line.
func String() string {
	return Version + " (" + GitSHA + ")"
}
